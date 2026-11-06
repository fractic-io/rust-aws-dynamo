use std::collections::BTreeMap;

use aws_sdk_dynamodb::types::AttributeValue;
use fractic_core::collection;
use fractic_server_error::ServerError;

use crate::{errors::DynamoCalloutError, schema::PkSk};

use super::{DynamoMap, DynamoUtil};

// Public interface.
// ----------------------------------------------------------------------------

/// Atomic counters: an item whose numeric attributes are incremented in place,
/// without reading them first. The item is not a `DynamoObject`; it is read
/// and written raw and takes part in no query, bundle or overlay.
impl DynamoUtil {
    /// Adds one to each of the named numeric attributes of the item `id` in
    /// one atomic `UpdateItem`; the item and any absent attribute are created.
    ///
    /// Performs no schema checks and notifies no write observer: this is the
    /// primitive an observer itself writes with.
    pub async fn raw_increment_counters(
        &self,
        id: &PkSk,
        names: &[&str],
    ) -> Result<(), ServerError> {
        if names.is_empty() {
            return Ok(());
        }
        let aliases = (0..names.len())
            .map(|index| format!("#c{index}"))
            .collect::<Vec<_>>();
        let update = format!(
            "ADD {}",
            aliases
                .iter()
                .map(|alias| format!("{alias} :one"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let attribute_names = aliases
            .into_iter()
            .zip(names)
            .map(|(alias, name)| (alias, name.to_string()))
            .collect();
        self.backend
            .update_item(
                self.table.clone(),
                key_of(id),
                update,
                collection! {
                    ":one".to_string() => AttributeValue::N("1".to_string()),
                },
                attribute_names,
                None,
            )
            .await
            .map_err(|e| DynamoCalloutError::with_debug(&e))?;
        Ok(())
    }

    /// Reads every numeric attribute of the item `id` as a counter, with a
    /// consistent read. An absent item has no counters.
    pub async fn raw_read_counters(&self, id: &PkSk) -> Result<BTreeMap<String, u64>, ServerError> {
        let output = self
            .backend
            .get_item(self.table.clone(), key_of(id), None, true)
            .await
            .map_err(|e| DynamoCalloutError::with_debug(&e))?;
        Ok(output
            .item
            .unwrap_or_default()
            .into_iter()
            .filter_map(|(name, value)| match value {
                AttributeValue::N(number) => number.parse().ok().map(|count| (name, count)),
                _ => None,
            })
            .collect())
    }
}

// Helpers.
// ----------------------------------------------------------------------------

fn key_of(id: &PkSk) -> DynamoMap {
    let mut key = DynamoMap::new();
    id.write_to_map(&mut key);
    key
}

// Tests.
// ----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::result_large_err)]

    use std::{collections::HashMap, sync::Arc};

    use aws_sdk_dynamodb::{
        operation::{get_item::GetItemOutput, update_item::UpdateItemOutput},
        types::AttributeValue,
    };
    use fractic_core::collection;

    use crate::{
        context::test_ctx::TestCtx,
        schema::PkSk,
        util::{backend::MockDynamoBackend, DynamoUtil},
    };

    async fn build_util(backend: MockDynamoBackend) -> DynamoUtil {
        let ctx = TestCtx::init_test("mock-region".to_string());
        ctx.override_dynamo_backend(Arc::new(backend)).await;
        DynamoUtil::new(&*ctx, "my_table").await.unwrap()
    }

    fn counters_id() -> PkSk {
        PkSk {
            pk: "KEY#cache-generation".to_string(),
            sk: "@COUNTERS".to_string(),
        }
    }

    #[tokio::test]
    async fn increment_is_one_add_expression_with_aliased_names() {
        let mut backend = MockDynamoBackend::new();
        backend
            .expect_update_item()
            .withf(|table, key, update, values, names, condition| {
                table == "my_table"
                    && key.get("pk") == Some(&AttributeValue::S("KEY#cache-generation".into()))
                    && key.get("sk") == Some(&AttributeValue::S("@COUNTERS".into()))
                    && update == "ADD #c0 :one, #c1 :one"
                    && values
                        == &collection! {
                            ":one".to_string() => AttributeValue::N("1".to_string()),
                        }
                    && names
                        == &collection! {
                            "#c0".to_string() => "STORY".to_string(),
                            "#c1".to_string() => "TEMPLATE".to_string(),
                        }
                    && condition.is_none()
            })
            .times(1)
            .returning(|_, _, _, _, _, _| Ok(UpdateItemOutput::builder().build()));
        let util = build_util(backend).await;
        util.raw_increment_counters(&counters_id(), &["STORY", "TEMPLATE"])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn increment_of_nothing_writes_nothing() {
        let backend = MockDynamoBackend::new();
        let util = build_util(backend).await;
        util.raw_increment_counters(&counters_id(), &[])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn read_returns_numeric_attributes_only_and_is_consistent() {
        let mut backend = MockDynamoBackend::new();
        backend
            .expect_get_item()
            .withf(|table, key, projection, consistent| {
                table == "my_table"
                    && key.get("sk") == Some(&AttributeValue::S("@COUNTERS".into()))
                    && projection.is_none()
                    && *consistent
            })
            .returning(|_, _, _, _| {
                Ok(GetItemOutput::builder()
                    .set_item(Some(collection! {
                        "pk".to_string() => AttributeValue::S("KEY#cache-generation".into()),
                        "sk".to_string() => AttributeValue::S("@COUNTERS".into()),
                        "STORY".to_string() => AttributeValue::N("3".into()),
                        "TEMPLATE".to_string() => AttributeValue::N("12".into()),
                    }))
                    .build())
            });
        let util = build_util(backend).await;
        let counters = util.raw_read_counters(&counters_id()).await.unwrap();
        assert_eq!(
            counters.into_iter().collect::<HashMap<_, _>>(),
            collection! {
                "STORY".to_string() => 3,
                "TEMPLATE".to_string() => 12,
            }
        );
    }

    #[tokio::test]
    async fn read_of_an_absent_item_is_empty() {
        let mut backend = MockDynamoBackend::new();
        backend
            .expect_get_item()
            .returning(|_, _, _, _| Ok(GetItemOutput::builder().build()));
        let util = build_util(backend).await;
        assert!(util
            .raw_read_counters(&counters_id())
            .await
            .unwrap()
            .is_empty());
    }
}
