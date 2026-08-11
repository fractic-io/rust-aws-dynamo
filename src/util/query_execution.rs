use std::{cmp::Ordering, collections::HashMap};

use fractic_server_error::ServerError;

use crate::{
    errors::DynamoCalloutError,
    schema::{
        identifiers::RawIdPath, item_deserialization::parse_dynamo_map, pk_sk::id_fields_from_map,
        DynamoObject, PkSk, AUTO_FIELDS_SORT,
    },
};

use super::{
    collapse_helpers::collapse_partitioned_items,
    consistency_overlay::OverlayMutation,
    expand_helpers::expand_batched_items,
    id_relations::{child_query_prefix, validate_parent_for},
    query::QueryExpression,
    DynamoGenericQuery, DynamoMap, DynamoQuery, DynamoUtil, QueryAllOptions,
};

// Public interface.
// ----------------------------------------------------------------------------

impl DynamoUtil {
    /// Executes a typed key query and deserializes matching objects.
    pub async fn query<T: DynamoObject>(
        &self,
        query: DynamoQuery<T>,
    ) -> Result<Vec<T>, ServerError> {
        self.query_generic(query.into())
            .await?
            .into_iter()
            .filter_map(|item| {
                let (_, sk) =
                    id_fields_from_map(&item).expect("query result item did not have pk/sk.");
                (RawIdPath::new(sk).object_label().ok()? == T::id_label())
                    .then(|| parse_dynamo_map::<T>(&item))
            })
            .collect::<Result<Vec<T>, ServerError>>()
    }

    /// Efficiently queries all children of type `T` belonging to `parent_id`.
    pub async fn query_all<T: DynamoObject>(
        &self,
        parent_id: &PkSk,
    ) -> Result<Vec<T>, ServerError> {
        self.query_all_opt(parent_id, QueryAllOptions::default())
            .await
    }

    pub async fn query_all_opt<T: DynamoObject>(
        &self,
        parent_id: &PkSk,
        options: QueryAllOptions,
    ) -> Result<Vec<T>, ServerError> {
        validate_parent_for::<T>(parent_id)?;
        let prefix = child_query_prefix::<T>(parent_id);
        let query = DynamoQuery::pk(prefix.pk).sk_begins_with(prefix.sk);
        self.query::<T>(if options.consistent_read {
            query.consistent_read()
        } else {
            query
        })
        .await
    }

    /// Executes a generic key query and returns raw Dynamo maps.
    pub async fn query_generic(
        &self,
        query: DynamoGenericQuery,
    ) -> Result<Vec<DynamoMap>, ServerError> {
        let expression = query.into_expression();
        // Snapshot before the network call so writes which were visible when
        // the query began cannot expire while DynamoDB is responding.
        let overlay = expression
            .needs_overlay()
            .then(|| self.consistency_overlay.snapshot(&self.table));
        let response = self
            .backend
            .query(
                self.table.clone(),
                expression.index_name.clone(),
                expression.condition.clone(),
                expression.attribute_values.clone(),
                None,
                expression.uses_native_consistency(),
            )
            .await
            .map_err(|error| DynamoCalloutError::with_debug(&error))?;

        let mut raw_items = response
            .into_iter()
            .flat_map(|page| page.items.unwrap_or_default().into_iter())
            .collect::<Vec<_>>();
        if let Some(overlay) = overlay {
            reconcile_gsi_results(&expression, &mut raw_items, overlay)?;
        }

        let items = collapse_partitioned_items(expand_batched_items(raw_items))?;
        Ok(sort_by_custom_order(items))
    }
}

// Helpers.
// ----------------------------------------------------------------------------

fn reconcile_gsi_results(
    query: &QueryExpression,
    items: &mut Vec<DynamoMap>,
    mutations: Vec<OverlayMutation>,
) -> Result<(), ServerError> {
    let mut by_id = items
        .drain(..)
        .map(|item| PkSk::from_map(&item).map(|id| (id, item)))
        .collect::<Result<HashMap<_, _>, _>>()?;

    for mutation in mutations {
        match mutation {
            OverlayMutation::Put { id, item } => {
                by_id.remove(&id);
                if query.matches_item(&item) {
                    by_id.insert(id, (*item).clone());
                }
            }
            OverlayMutation::Delete(id) => {
                by_id.remove(&id);
            }
        }
    }

    items.extend(by_id.into_values());
    if let Some(index) = query.index {
        items.sort_by(|left, right| {
            string_attribute(left, index.config.sort_field)
                .cmp(&string_attribute(right, index.config.sort_field))
                .then_with(|| item_id_order(left, right))
        });
    }
    Ok(())
}

fn string_attribute<'a>(item: &'a DynamoMap, field: &str) -> Option<&'a str> {
    item.get(field)
        .and_then(|value| value.as_s().ok())
        .map(String::as_str)
}

fn item_id_order(left: &DynamoMap, right: &DynamoMap) -> Ordering {
    let left = id_fields_from_map(left).expect("query result did not contain valid pk/sk");
    let right = id_fields_from_map(right).expect("query result did not contain valid pk/sk");
    left.cmp(&right)
}

fn sort_by_custom_order(items: Vec<DynamoMap>) -> Vec<DynamoMap> {
    let mut items = items
        .into_iter()
        .map(|item| {
            let sort = item
                .get(AUTO_FIELDS_SORT)
                .and_then(|value| value.as_n().ok())
                .and_then(|value| value.parse::<f64>().ok());
            (item, sort)
        })
        .collect::<Vec<_>>();
    items.sort_by(|(_, left), (_, right)| match (left, right) {
        (Some(left), Some(right)) => left.total_cmp(right),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        _ => Ordering::Equal,
    });
    items.into_iter().map(|(item, _)| item).collect()
}

// Tests.
// ----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::result_large_err)]

    use std::{sync::Arc, time::Duration};

    use aws_sdk_dynamodb::{operation::query::QueryOutput, types::AttributeValue};
    use fractic_core::collection;

    use super::*;
    use crate::util::{
        backend::MockDynamoBackend,
        consistency_overlay::{DynamoConsistencyOverlay, InMemoryDynamoConsistencyOverlay},
        IndexConfig,
    };

    const INDEX: IndexConfig = IndexConfig {
        name: "by_group",
        partition_field: "group",
        sort_field: "rank",
    };

    fn item(id: &str, group: &str, rank: &str) -> DynamoMap {
        collection! {
            "pk".to_string() => AttributeValue::S("ROOT".into()),
            "sk".to_string() => AttributeValue::S(id.into()),
            "group".to_string() => AttributeValue::S(group.into()),
            "rank".to_string() => AttributeValue::S(rank.into()),
        }
    }

    fn util(
        backend: MockDynamoBackend,
        overlay: Arc<InMemoryDynamoConsistencyOverlay>,
    ) -> DynamoUtil {
        DynamoUtil {
            backend: Arc::new(backend),
            consistency_overlay: overlay,
            table: "table".into(),
        }
    }

    #[tokio::test]
    async fn consistent_table_query_uses_native_consistency() {
        let mut backend = MockDynamoBackend::new();
        backend
            .expect_query()
            .withf(|table, index, condition, values, projection, consistent| {
                table == "table"
                    && index.is_none()
                    && condition == "pk = :pk_val"
                    && values.get(":pk_val") == Some(&AttributeValue::S("ROOT".into()))
                    && projection.is_none()
                    && *consistent
            })
            .once()
            .returning(|_, _, _, _, _, _| Ok(vec![QueryOutput::builder().build()]));

        let util = util(
            backend,
            Arc::new(InMemoryDynamoConsistencyOverlay::new(Duration::from_secs(
                60,
            ))),
        );
        let result = util
            .query_generic(DynamoGenericQuery::pk("ROOT").all().consistent_read())
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn consistent_lsi_query_uses_native_consistency_without_overlay() {
        let overlay = Arc::new(InMemoryDynamoConsistencyOverlay::new(Duration::from_secs(
            60,
        )));
        overlay.record_put("table", item("ITEM#LOCAL", "TARGET", "1"));
        let mut backend = MockDynamoBackend::new();
        backend
            .expect_query()
            .withf(|table, index, condition, values, projection, consistent| {
                table == "table"
                    && index.as_deref() == Some("by_group")
                    && condition == "group = :pk_val"
                    && values.get(":pk_val") == Some(&AttributeValue::S("TARGET".into()))
                    && projection.is_none()
                    && *consistent
            })
            .once()
            .returning(|_, _, _, _, _, _| Ok(vec![QueryOutput::builder().build()]));

        let result = util(backend, overlay)
            .query_generic(
                DynamoGenericQuery::lsi(INDEX)
                    .pk("TARGET")
                    .all()
                    .consistent_read(),
            )
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn consistent_gsi_query_reconciles_puts_moves_and_deletes() {
        let overlay = Arc::new(InMemoryDynamoConsistencyOverlay::new(Duration::from_secs(
            60,
        )));
        overlay.record_put("table", item("ITEM#A", "OTHER", "0"));
        overlay.record_delete(
            "table",
            PkSk {
                pk: "ROOT".into(),
                sk: "ITEM#B".into(),
            },
        );
        overlay.record_put("table", item("ITEM#D", "TARGET", "1"));
        let mut sparse = item("ITEM#SPARSE", "TARGET", "unused");
        sparse.remove("rank");
        overlay.record_put("table", sparse);

        let mut backend = MockDynamoBackend::new();
        backend.expect_query().once().returning(|_, _, _, _, _, _| {
            Ok(vec![QueryOutput::builder()
                .set_items(Some(vec![
                    item("ITEM#A", "TARGET", "0"),
                    item("ITEM#B", "TARGET", "1"),
                    item("ITEM#C", "TARGET", "2"),
                ]))
                .build()])
        });

        let result = util(backend, overlay)
            .query_generic(
                DynamoGenericQuery::gsi(INDEX)
                    .pk("TARGET")
                    .all()
                    .consistent_read(),
            )
            .await
            .unwrap();

        let ids = result
            .iter()
            .map(|item| PkSk::from_map(item).unwrap().sk)
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["ITEM#D", "ITEM#C"]);
    }

    #[tokio::test]
    async fn ordinary_gsi_query_does_not_apply_the_overlay() {
        let overlay = Arc::new(InMemoryDynamoConsistencyOverlay::new(Duration::from_secs(
            60,
        )));
        overlay.record_put("table", item("ITEM#LOCAL", "TARGET", "1"));
        let mut backend = MockDynamoBackend::new();
        backend
            .expect_query()
            .once()
            .returning(|_, _, _, _, _, _| Ok(vec![QueryOutput::builder().build()]));

        let result = util(backend, overlay)
            .query_generic(DynamoGenericQuery::index(INDEX).pk("TARGET").all())
            .await
            .unwrap();
        assert!(result.is_empty());
    }
}
