use fractic_generic_server_error::GenericServerError;
use ordered_float::NotNan;

use crate::{
    errors::DynamoInvalidOperationError,
    schema::{DynamoObject, PkSk},
};

use super::{backend::DynamoBackendImpl, DynamoInsertPosition, DynamoQueryMatchType, DynamoUtil};

#[derive(Debug, PartialEq, Eq)]
struct OrderedItem<'a> {
    id: &'a PkSk,
    sort: NotNan<f64>,
}
impl PartialOrd for OrderedItem<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.sort.partial_cmp(&other.sort)
    }
}
impl Ord for OrderedItem<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.sort.cmp(&other.sort)
    }
}

pub(crate) async fn calculate_sort_values<T: DynamoObject, B: DynamoBackendImpl>(
    util: &DynamoUtil<B>,
    parent_id: PkSk,
    object: &T,
    insert_position: DynamoInsertPosition,
    num: usize,
) -> Result<Vec<f64>, GenericServerError> {
    let dbg_cxt = "generate_ordered_custom_ids";

    // Special 'sort' field is used to order elements. Use f64 so we can always
    // insert in between any two elements.
    let sort_value_init = NotNan::new(1.0).unwrap();
    let sort_value_default_gap = NotNan::new(1.0).unwrap();

    // Search for all IDs for existing items of this type.
    let search_id = PkSk {
        pk: object.generate_pk(&parent_id.pk, &parent_id.sk, ""),
        sk: object.generate_sk(&parent_id.pk, &parent_id.sk, ""),
    };
    let query = util
        .query::<T>(None, search_id, DynamoQueryMatchType::BeginsWith)
        .await?;
    let existing_vals = {
        let mut v = query
            .iter()
            .filter_map(|item| {
                if let Some(Ok(sort)) = item.sort().map(NotNan::new) {
                    Some(OrderedItem {
                        id: item.id_or_critical().unwrap(),
                        sort,
                    })
                } else {
                    None
                }
            })
            .collect::<Vec<OrderedItem>>();
        v.sort();
        v
    };

    Ok(match &insert_position {
        DynamoInsertPosition::First => {
            let min_val = existing_vals
                .first()
                .map(|item| item.sort)
                .unwrap_or(sort_value_init);
            (0..num)
                .map(|i| min_val - sort_value_default_gap * (i as f64 + 1.0))
                .map(f64::from)
                .rev()
                .collect()
        }
        DynamoInsertPosition::Last => {
            let max_val = existing_vals
                .last()
                .map(|item| item.sort)
                .unwrap_or(sort_value_init);
            (0..num)
                .map(|i| max_val + sort_value_default_gap * (i as f64 + 1.0))
                .map(f64::from)
                .collect()
        }
        DynamoInsertPosition::After(id) => {
            let insert_after_index = existing_vals
                .iter()
                .position(|item| item.id == id)
                .ok_or(DynamoInvalidOperationError::new(
                    dbg_cxt,
                    "The ID provided in DynamoInsertPosition::After(id) does not exist as a sorted item of type T in the database.",
                ))?;
            let insert_after = existing_vals.get(insert_after_index).unwrap();
            let insert_before = existing_vals.get(insert_after_index + 1);
            match insert_before {
                // Insert in between two items by calculating evenly spaced
                // values in between insert_before and insert_after.
                Some(insert_before) => {
                    let gap = (insert_before.sort - insert_after.sort) / (num as f64 + 1.0);
                    (0..num)
                        .map(|i| insert_after.sort + gap * (i as f64 + 1.0))
                        .map(f64::from)
                        .collect()
                }
                // No items after, simple insert same as ::Last.
                None => (0..num)
                    .map(|i| insert_after.sort + sort_value_default_gap * (i as f64 + 1.0))
                    .map(f64::from)
                    .collect(),
            }
        }
    })
}

// Tests.
// --------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::{
        impl_dynamo_object,
        schema::{AutoFields, NestingType},
        util::{backend::MockDynamoBackendImpl, DynamoUtil},
    };
    use aws_sdk_dynamodb::{operation::query::QueryOutput, types::AttributeValue};
    use fractic_core::collection;
    use mockall::predicate::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq)]
    struct TestDynamoObject {
        id: Option<PkSk>,
        #[serde(flatten)]
        auto_fields: AutoFields,
        data: Option<String>,
    }
    impl_dynamo_object!(TestDynamoObject, "TEST", NestingType::TopLevelChild);

    fn build_test_item(pk: &str, sk: &str, sort: Option<f64>) -> TestDynamoObject {
        TestDynamoObject {
            id: Some(PkSk {
                pk: pk.to_string(),
                sk: sk.to_string(),
            }),
            auto_fields: AutoFields {
                sort,
                ..Default::default()
            },
            data: None,
        }
    }

    fn build_dynamo_item(pk: &str, sk: &str, sort: Option<f64>) -> HashMap<String, AttributeValue> {
        let mut item: HashMap<String, AttributeValue> = collection! {
            "pk".to_string() => AttributeValue::S(pk.to_string()),
            "sk".to_string() => AttributeValue::S(sk.to_string()),
        };
        if let Some(sort_val) = sort {
            item.insert("sort".to_string(), AttributeValue::N(sort_val.to_string()));
        }
        item
    }

    #[tokio::test]
    async fn test_calculate_sort_values_first() {
        let mut backend = MockDynamoBackendImpl::new();
        backend
            .expect_query()
            .withf(|_, _, _, _| true)
            .returning(|_, _, _, _| {
                Ok(QueryOutput::builder()
                    .set_items(Some(vec![
                        build_dynamo_item("ROOT", "GROUP#123#TEST#1", Some(0.5)),
                        build_dynamo_item("ROOT", "GROUP#123#TEST#2", Some(1.5)),
                    ]))
                    .build())
            });

        let util = DynamoUtil {
            backend,
            table: "my_table".to_string(),
        };

        let parent_id = PkSk {
            pk: "ROOT".to_string(),
            sk: "GROUP#123".to_string(),
        };

        let object = build_test_item("ROOT", "GROUP#123#TEST#3", None);

        let sort_values =
            calculate_sort_values(&util, parent_id, &object, DynamoInsertPosition::First, 2)
                .await
                .unwrap();

        assert_eq!(sort_values.len(), 2);
        assert!(sort_values[0] < 0.5);
        assert!(sort_values[1] < 0.5);
        assert!(sort_values[0] < sort_values[1]);
    }

    #[tokio::test]
    async fn test_calculate_sort_values_last() {
        let mut backend = MockDynamoBackendImpl::new();
        backend
            .expect_query()
            .withf(|_, _, _, _| true)
            .returning(|_, _, _, _| {
                Ok(QueryOutput::builder()
                    .set_items(Some(vec![
                        build_dynamo_item("ROOT", "GROUP#123#TEST#1", Some(0.5)),
                        build_dynamo_item("ROOT", "GROUP#123#TEST#2", Some(1.5)),
                    ]))
                    .build())
            });

        let util = DynamoUtil {
            backend,
            table: "my_table".to_string(),
        };

        let parent_id = PkSk {
            pk: "ROOT".to_string(),
            sk: "GROUP#123".to_string(),
        };

        let object = build_test_item("ROOT", "GROUP#123#TEST#3", None);

        let sort_values =
            calculate_sort_values(&util, parent_id, &object, DynamoInsertPosition::Last, 2)
                .await
                .unwrap();

        assert_eq!(sort_values.len(), 2);
        assert!(sort_values[0] > 1.5);
        assert!(sort_values[1] > 1.5);
        assert!(sort_values[0] < sort_values[1]);
    }

    #[tokio::test]
    async fn test_calculate_sort_values_after() {
        let mut backend = MockDynamoBackendImpl::new();
        backend
            .expect_query()
            .withf(|_, _, _, _| true)
            .returning(|_, _, _, _| {
                Ok(QueryOutput::builder()
                    .set_items(Some(vec![
                        build_dynamo_item("ROOT", "GROUP#123#TEST#1", Some(0.5)),
                        build_dynamo_item("ROOT", "GROUP#123#TEST#2", Some(1.5)),
                    ]))
                    .build())
            });

        let util = DynamoUtil {
            backend,
            table: "my_table".to_string(),
        };

        let parent_id = PkSk {
            pk: "ROOT".to_string(),
            sk: "GROUP#123".to_string(),
        };

        let object = build_test_item("ROOT", "GROUP#123#TEST#3", None);

        let after_id = PkSk {
            pk: "ROOT".to_string(),
            sk: "GROUP#123#TEST#1".to_string(),
        };

        let sort_values = calculate_sort_values(
            &util,
            parent_id,
            &object,
            DynamoInsertPosition::After(after_id),
            2,
        )
        .await
        .unwrap();

        assert_eq!(sort_values.len(), 2);
        assert!(sort_values[0] > 0.5 && sort_values[0] < 1.5);
        assert!(sort_values[1] > 0.5 && sort_values[1] < 1.5);
        assert!(sort_values[0] < sort_values[1]);
    }

    #[tokio::test]
    async fn test_calculate_sort_values_after_last_item() {
        let mut backend = MockDynamoBackendImpl::new();
        backend
            .expect_query()
            .withf(|_, _, _, _| true)
            .returning(|_, _, _, _| {
                Ok(QueryOutput::builder()
                    .set_items(Some(vec![
                        build_dynamo_item("ROOT", "GROUP#123#TEST#1", Some(0.5)),
                        build_dynamo_item("ROOT", "GROUP#123#TEST#2", Some(1.5)),
                    ]))
                    .build())
            });

        let util = DynamoUtil {
            backend,
            table: "my_table".to_string(),
        };

        let parent_id = PkSk {
            pk: "ROOT".to_string(),
            sk: "GROUP#123".to_string(),
        };

        let object = build_test_item("ROOT", "GROUP#123#TEST#3", None);

        let after_id = PkSk {
            pk: "ROOT".to_string(),
            sk: "GROUP#123#TEST#2".to_string(),
        };

        let sort_values = calculate_sort_values(
            &util,
            parent_id,
            &object,
            DynamoInsertPosition::After(after_id),
            2,
        )
        .await
        .unwrap();

        assert_eq!(sort_values.len(), 2);
        assert!(sort_values[0] > 1.5);
        assert!(sort_values[1] > 1.5);
        assert!(sort_values[0] < sort_values[1]);
    }

    #[tokio::test]
    async fn test_calculate_sort_values_empty_existing_items() {
        let mut backend = MockDynamoBackendImpl::new();
        backend
            .expect_query()
            .withf(|_, _, _, _| true)
            .returning(|_, _, _, _| Ok(QueryOutput::builder().set_items(Some(vec![])).build()));

        let util = DynamoUtil {
            backend,
            table: "my_table".to_string(),
        };

        let parent_id = PkSk {
            pk: "ROOT".to_string(),
            sk: "GROUP#123".to_string(),
        };

        let object = build_test_item("ROOT", "GROUP#123#TEST#3", None);

        let sort_values =
            calculate_sort_values(&util, parent_id, &object, DynamoInsertPosition::First, 2)
                .await
                .unwrap();

        assert_eq!(sort_values.len(), 2);
        assert!(sort_values[0] < sort_values[1]);
    }
}
