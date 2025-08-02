use fractic_server_error::ServerError;
use ordered_float::NotNan;

use crate::{
    errors::{DynamoInvalidId, DynamoInvalidOperation},
    schema::{id_calculations::generate_pk_sk, DynamoObject, IdLogic, PkSk},
};

use super::{DynamoInsertPosition, DynamoQueryMatchType, DynamoUtil};

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

// Strip final UUID or timestamp from a DynamoDB ID.
fn _sk_strip_uuid<T: DynamoObject>(
    id_logic: IdLogic<T::Data>,
    sk: String,
) -> Result<String, ServerError> {
    Ok(match id_logic {
        // For Singleton, no ID to strip.
        IdLogic::Singleton => sk,
        // For SingletonFamily, strip the key.
        IdLogic::SingletonFamily(_) => sk.split('[').next().unwrap().to_string(),
        // For Uuid and Timestamp, take ID until last '#' character.
        IdLogic::Uuid | IdLogic::Timestamp => sk[..sk.rfind('#').ok_or_else(|| {
            DynamoInvalidId::with_debug(
                "can't strip Uuid/Timestamp since ID didn't contain '#'",
                &sk,
            )
        })?]
            .to_string(),
    })
}

pub(crate) async fn calculate_sort_values<T: DynamoObject>(
    util: &DynamoUtil,
    parent_id: PkSk,
    data: &T::Data,
    insert_position: DynamoInsertPosition,
    num: usize,
) -> Result<Vec<f64>, ServerError> {
    // Special 'sort' field is used to order elements. Use f64 so we can always
    // insert in between any two elements.
    let sort_value_init = NotNan::new(1.0).unwrap();
    let sort_value_default_gap = NotNan::new(1.0).unwrap();

    // Search for all IDs for existing items of this type by creating an example
    // ID and stripping the ID UUID / timestamp off the end.
    let (example_pk, example_sk) = generate_pk_sk::<T>(data, &parent_id.pk, &parent_id.sk)?;
    let search_id = PkSk {
        pk: example_pk,
        sk: _sk_strip_uuid::<T>(T::id_logic(), example_sk)?,
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
                        id: item.id(),
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
                .ok_or(DynamoInvalidOperation::new(
                    "the ID provided in DynamoInsertPosition::After(id) does not exist as a sorted item of type T in the database",
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
    use std::{collections::HashMap, sync::Arc};

    use super::*;
    use crate::context::test_ctx::TestCtx;
    use crate::{
        dynamo_object,
        schema::{AutoFields, NestingLogic},
        util::{backend::MockDynamoBackend, DynamoUtil},
    };
    use aws_sdk_dynamodb::{operation::query::QueryOutput, types::AttributeValue};
    use fractic_core::collection;
    use mockall::predicate::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq)]
    pub struct TestDynamoObjectData {
        data: Option<String>,
    }
    dynamo_object!(
        TestDynamoObject,
        TestDynamoObjectData,
        "TEST",
        IdLogic::Uuid,
        NestingLogic::TopLevelChildOfAny
    );

    async fn build_util(mock_backend: MockDynamoBackend) -> DynamoUtil {
        let ctx = TestCtx::init_test("mock-region".to_string());
        ctx.override_dynamo_backend(Arc::new(mock_backend)).await;
        DynamoUtil::new(&*ctx, "my_table").await.unwrap()
    }

    fn build_test_item(pk: &str, sk: &str, sort: Option<f64>) -> TestDynamoObject {
        TestDynamoObject {
            id: PkSk {
                pk: pk.to_string(),
                sk: sk.to_string(),
            },
            auto_fields: AutoFields {
                sort,
                ..Default::default()
            },
            data: TestDynamoObjectData::default(),
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
        let mut backend = MockDynamoBackend::new();
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

        let util = build_util(backend).await;

        let parent_id = PkSk {
            pk: "ROOT".to_string(),
            sk: "GROUP#123".to_string(),
        };

        let object = build_test_item("ROOT", "GROUP#123#TEST#3", None);

        let sort_values = calculate_sort_values::<TestDynamoObject>(
            &util,
            parent_id,
            &object.data,
            DynamoInsertPosition::First,
            2,
        )
        .await
        .unwrap();

        assert_eq!(sort_values.len(), 2);
        assert!(sort_values[0] < 0.5);
        assert!(sort_values[1] < 0.5);
        assert!(sort_values[0] < sort_values[1]);
    }

    #[tokio::test]
    async fn test_calculate_sort_values_last() {
        let mut backend = MockDynamoBackend::new();
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

        let util = build_util(backend).await;

        let parent_id = PkSk {
            pk: "ROOT".to_string(),
            sk: "GROUP#123".to_string(),
        };

        let object = build_test_item("ROOT", "GROUP#123#TEST#3", None);

        let sort_values = calculate_sort_values::<TestDynamoObject>(
            &util,
            parent_id,
            &object.data,
            DynamoInsertPosition::Last,
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
    async fn test_calculate_sort_values_after() {
        let mut backend = MockDynamoBackend::new();
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

        let util = build_util(backend).await;

        let parent_id = PkSk {
            pk: "ROOT".to_string(),
            sk: "GROUP#123".to_string(),
        };

        let object = build_test_item("ROOT", "GROUP#123#TEST#3", None);

        let after_id = PkSk {
            pk: "ROOT".to_string(),
            sk: "GROUP#123#TEST#1".to_string(),
        };

        let sort_values = calculate_sort_values::<TestDynamoObject>(
            &util,
            parent_id,
            &object.data,
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
        let mut backend = MockDynamoBackend::new();
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

        let util = build_util(backend).await;

        let parent_id = PkSk {
            pk: "ROOT".to_string(),
            sk: "GROUP#123".to_string(),
        };

        let object = build_test_item("ROOT", "GROUP#123#TEST#3", None);

        let after_id = PkSk {
            pk: "ROOT".to_string(),
            sk: "GROUP#123#TEST#2".to_string(),
        };

        let sort_values = calculate_sort_values::<TestDynamoObject>(
            &util,
            parent_id,
            &object.data,
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
        let mut backend = MockDynamoBackend::new();
        backend
            .expect_query()
            .withf(|_, _, _, _| true)
            .returning(|_, _, _, _| Ok(QueryOutput::builder().set_items(Some(vec![])).build()));

        let util = build_util(backend).await;

        let parent_id = PkSk {
            pk: "ROOT".to_string(),
            sk: "GROUP#123".to_string(),
        };

        let object = build_test_item("ROOT", "GROUP#123#TEST#3", None);

        let sort_values = calculate_sort_values::<TestDynamoObject>(
            &util,
            parent_id,
            &object.data,
            DynamoInsertPosition::First,
            2,
        )
        .await
        .unwrap();

        assert_eq!(sort_values.len(), 2);
        assert!(sort_values[0] < sort_values[1]);
    }

    #[test]
    fn test_sk_strip_uuid() {
        // We just use TestDynamoObject for all these, even though technically
        // this means the T::id_logic() isn't correct. But it's okay because the
        // function doesn't use it and we just want to test the function logic
        // here.
        assert_eq!(
            _sk_strip_uuid::<TestDynamoObject>(
                IdLogic::<TestDynamoObjectData>::Uuid,
                "GROUP#123#TEST#123".to_string()
            )
            .unwrap(),
            "GROUP#123#TEST"
        );
        assert_eq!(
            _sk_strip_uuid::<TestDynamoObject>(
                IdLogic::<TestDynamoObjectData>::Timestamp,
                "GROUP#123#TEST2#0005416".to_string()
            )
            .unwrap(),
            "GROUP#123#TEST2"
        );
        assert_eq!(
            _sk_strip_uuid::<TestDynamoObject>(
                IdLogic::<TestDynamoObjectData>::Singleton,
                "@SINGLETON".to_string()
            )
            .unwrap(),
            "@SINGLETON"
        );
        assert_eq!(
            _sk_strip_uuid::<TestDynamoObject>(
                IdLogic::<TestDynamoObjectData>::SingletonFamily(Box::new(
                    |_| "samplekey".to_string()
                )),
                "@SINGLETONFAM[samplekey]".to_string()
            )
            .unwrap(),
            "@SINGLETONFAM"
        );
    }
}
