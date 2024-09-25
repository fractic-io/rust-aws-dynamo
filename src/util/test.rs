#[cfg(test)]
mod tests {
    use crate::util::DynamoObject;
    use crate::{
        impl_dynamo_object,
        schema::{AutoFields, IdLogic, PkSk},
        util::{
            backend::MockDynamoBackendImpl, DynamoQueryMatchType, DynamoUtil,
            AUTO_FIELDS_CREATED_AT, AUTO_FIELDS_SORT, AUTO_FIELDS_UPDATED_AT,
        },
    };

    use aws_sdk_dynamodb::{
        operation::{
            batch_write_item::BatchWriteItemOutput, delete_item::DeleteItemOutput,
            get_item::GetItemOutput, put_item::PutItemOutput, query::QueryOutput,
            update_item::UpdateItemOutput,
        },
        types::AttributeValue,
    };
    use fractic_core::collection;
    use mockall::predicate::*;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    #[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq)]
    struct TestDynamoObject {
        id: Option<PkSk>,
        #[serde(flatten)]
        auto_fields: AutoFields,
        data: Option<String>,
    }
    impl_dynamo_object!(TestDynamoObject, "TEST", IdLogic::TopLevelChild);

    fn build_item_no_data() -> (TestDynamoObject, HashMap<String, AttributeValue>) {
        (
            TestDynamoObject {
                id: Some(PkSk {
                    pk: "ROOT".to_string(),
                    sk: "GROUP#123#TEST#1".to_string(),
                }),
                auto_fields: Default::default(),
                data: None,
            },
            collection! {
                "pk".to_string() => AttributeValue::S("ROOT".to_string()),
                "sk".to_string() => AttributeValue::S("GROUP#123#TEST#1".to_string()),
            },
        )
    }

    fn build_item_high_sort() -> (TestDynamoObject, HashMap<String, AttributeValue>) {
        (
            TestDynamoObject {
                id: Some(PkSk {
                    pk: "ROOT".to_string(),
                    sk: "GROUP#123#TEST#2".to_string(),
                }),
                auto_fields: AutoFields {
                    sort: Some(0.75),
                    ..Default::default()
                },
                data: Some("high_sort".to_string()),
            },
            collection! {
                "pk".to_string() => AttributeValue::S("ROOT".to_string()),
                "sk".to_string() => AttributeValue::S("GROUP#123#TEST#2".to_string()),
                "sort".to_string() => AttributeValue::N("0.75".to_string()),
                "data".to_string() => AttributeValue::S("high_sort".to_string()),
            },
        )
    }

    fn build_item_low_sort() -> (TestDynamoObject, HashMap<String, AttributeValue>) {
        (
            TestDynamoObject {
                id: Some(PkSk {
                    pk: "ROOT".to_string(),
                    sk: "GROUP#123#TEST#3".to_string(),
                }),
                auto_fields: AutoFields {
                    sort: Some(0.10001),
                    ..Default::default()
                },
                data: Some("low_sort".to_string()),
            },
            collection! {
                "pk".to_string() => AttributeValue::S("ROOT".to_string()),
                "sk".to_string() => AttributeValue::S("GROUP#123#TEST#3".to_string()),
                "sort".to_string() => AttributeValue::N("0.10001".to_string()),
                "data".to_string() => AttributeValue::S("low_sort".to_string()),
            },
        )
    }

    #[tokio::test]
    async fn test_query() {
        let mut backend = MockDynamoBackendImpl::new();
        backend
            .expect_query()
            .with(
                eq("my_table".to_string()),
                eq(None),
                eq("pk = :pk_val AND begins_with(sk, :sk_val)".to_string()),
                eq::<HashMap<String, AttributeValue>>(collection! {
                    ":pk_val".to_string() => AttributeValue::S("ROOT".to_string()),
                    ":sk_val".to_string() => AttributeValue::S("GROUP#123".to_string())
                }),
            )
            .returning(|_, _, _, _| {
                Ok(QueryOutput::builder()
                    .set_items(Some(vec![
                        build_item_high_sort().1,
                        build_item_no_data().1,
                        build_item_low_sort().1,
                        collection!(
                            "pk".to_string() => AttributeValue::S("ROOT".to_string()),
                            "sk".to_string() => AttributeValue::S("GROUP#123#OTHEROBJECT#1".to_string())
                        )
                    ]))
                    .build())
            });

        let util = DynamoUtil {
            backend,
            table: "my_table".to_string(),
        };
        let result = util
            .query::<TestDynamoObject>(
                None,
                PkSk {
                    pk: "ROOT".to_string(),
                    sk: "GROUP#123".to_string(),
                },
                DynamoQueryMatchType::BeginsWith,
            )
            .await
            .unwrap();

        // Should have ignored object of label "OTHEROBJECT", since we are
        // are querying only TestDynamoObject, which has label "TEST".
        assert_eq!(result.len(), 3);

        // Lower sort value item first.
        assert_eq!(result[0], build_item_low_sort().0);
        assert_eq!(result[1], build_item_high_sort().0);
        assert_eq!(result[2], build_item_no_data().0);
    }

    #[tokio::test]
    async fn test_query_generic() {
        let mut backend = MockDynamoBackendImpl::new();
        backend
            .expect_query()
            .with(
                eq("my_table".to_string()),
                eq(None),
                eq("pk = :pk_val AND begins_with(sk, :sk_val)".to_string()),
                eq::<HashMap<String, AttributeValue>>(collection! {
                    ":pk_val".to_string() => AttributeValue::S("ROOT".to_string()),
                    ":sk_val".to_string() => AttributeValue::S("GROUP#123#TEST".to_string())
                }),
            )
            .returning(|_, _, _, _| {
                Ok(QueryOutput::builder()
                    .set_items(Some(vec![
                        build_item_high_sort().1,
                        build_item_low_sort().1,
                    ]))
                    .build())
            });

        let util = DynamoUtil {
            backend,
            table: "my_table".to_string(),
        };

        let result = util
            .query_generic(
                None,
                PkSk {
                    pk: "ROOT".to_string(),
                    sk: "GROUP#123#TEST".to_string(),
                },
                DynamoQueryMatchType::BeginsWith,
            )
            .await
            .unwrap();

        assert_eq!(result.len(), 2);

        // Lower sort value item first.
        assert_eq!(result[0], build_item_low_sort().1);
        assert_eq!(result[1], build_item_high_sort().1);
    }

    #[tokio::test]
    async fn test_get_item() {
        let mut backend = MockDynamoBackendImpl::new();
        backend
            .expect_get_item()
            .with(
                eq("my_table".to_string()),
                eq::<HashMap<String, AttributeValue>>(collection! {
                    "pk".to_string() => AttributeValue::S("ROOT".to_string()),
                    "sk".to_string() => AttributeValue::S("GROUP#123#TEST#2".to_string())
                }),
            )
            .returning(|_, _| {
                Ok(GetItemOutput::builder()
                    .set_item(Some(build_item_high_sort().1))
                    .build())
            });

        let util = DynamoUtil {
            backend,
            table: "my_table".to_string(),
        };

        let result = util
            .get_item::<TestDynamoObject>(PkSk {
                pk: "ROOT".to_string(),
                sk: "GROUP#123#TEST#2".to_string(),
            })
            .await
            .unwrap();

        assert!(result.is_some());
        let item = result.unwrap();
        assert_eq!(item.pk(), Some("ROOT"));
        assert_eq!(item.sk(), Some("GROUP#123#TEST#2"));
        assert_eq!(item.data, Some("high_sort".to_string()));
    }

    #[tokio::test]
    async fn test_create_item() {
        let mut backend = MockDynamoBackendImpl::new();
        backend
            .expect_put_item()
            .withf(|_, item| {
                item.get(AUTO_FIELDS_CREATED_AT).is_some()
                    && item.get(AUTO_FIELDS_UPDATED_AT).is_some()
                    && item.get(AUTO_FIELDS_SORT).is_some()
                    && item.get("data").is_some()
            })
            .returning(|_, _| Ok(PutItemOutput::builder().build()));

        let util = DynamoUtil {
            backend,
            table: "my_table".to_string(),
        };

        let new_item = build_item_high_sort().0;

        let result = util
            .create_item(
                PkSk {
                    pk: "ROOT".to_string(),
                    sk: "GROUP#123".to_string(),
                },
                &new_item,
                Some(0.75),
            )
            .await
            .unwrap();

        assert_eq!(result.pk, "GROUP#123".to_string());
    }

    #[tokio::test]
    async fn test_batch_create_item() {
        let mut backend = MockDynamoBackendImpl::new();
        backend
            .expect_batch_put_item()
            .withf(|_, items| {
                items.len() == 2
                    && items.iter().all(|item| {
                        item.get(AUTO_FIELDS_CREATED_AT).is_some()
                            && item.get(AUTO_FIELDS_UPDATED_AT).is_some()
                            && item.get(AUTO_FIELDS_SORT).is_some()
                            && item.get("data").is_some()
                    })
            })
            .returning(|_, _| Ok(BatchWriteItemOutput::builder().build()));

        let util = DynamoUtil {
            backend,
            table: "my_table".to_string(),
        };

        let item1 = build_item_no_data().0;
        let item2 = build_item_no_data().0;
        let items = vec![(&item1, Some(0.1200000)), (&item2, Some(12.0))];

        let result = util
            .batch_create_item(
                PkSk {
                    pk: "ROOT".to_string(),
                    sk: "GROUP#123".to_string(),
                },
                items,
            )
            .await
            .unwrap();

        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn test_update_item() {
        let mut backend = MockDynamoBackendImpl::new();
        backend
            .expect_update_item()
            .withf(|_, id, update_expr, values, keys| {
                id.get("pk").unwrap().as_s().unwrap() == "pk_to_update"
                    && id.get("sk").unwrap().as_s().unwrap() == "sk_to_update"
                    && update_expr == "SET #k1 = :v1, #k2 = :v2"
                    && values.get(":v1").is_some()
                    && values.get(":v2").is_some()
                    && {
                        let mut v = vec![keys.get("#k1").unwrap(), keys.get("#k2").unwrap()];
                        v.sort();
                        v
                    } == vec![&"data".to_string(), &"updated_at".to_string()]
            })
            .returning(|_, _, _, _, _| Ok(UpdateItemOutput::builder().build()));

        let util = DynamoUtil {
            backend,
            table: "my_table".to_string(),
        };

        let update_item = TestDynamoObject {
            id: Some(PkSk {
                pk: "pk_to_update".to_string(),
                sk: "sk_to_update".to_string(),
            }),
            auto_fields: Default::default(),
            data: Some("new_data".to_string()),
        };

        let result = util.update_item(&update_item).await.unwrap();
        assert_eq!(result, ());
    }

    #[tokio::test]
    async fn test_delete_item() {
        let mut backend = MockDynamoBackendImpl::new();
        backend
            .expect_delete_item()
            .with(
                eq("my_table".to_string()),
                eq::<HashMap<String, AttributeValue>>(collection! {
                    "pk".to_string() => AttributeValue::S("GROUP#123".to_string()),
                    "sk".to_string() => AttributeValue::S("LIST#123#ITEM#456".to_string())
                }),
            )
            .returning(|_, _| Ok(DeleteItemOutput::builder().build()));

        let util = DynamoUtil {
            backend,
            table: "my_table".to_string(),
        };

        let result = util
            .delete_item(PkSk {
                pk: "GROUP#123".to_string(),
                sk: "LIST#123#ITEM#456".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(result, ());
    }

    #[tokio::test]
    async fn test_batch_delete_item() {
        let mut backend = MockDynamoBackendImpl::new();
        backend
            .expect_batch_delete_item()
            .with(
                eq("my_table".to_string()),
                eq(vec![
                    collection! {
                        "pk".to_string() => AttributeValue::S("pk_to_delete_1".to_string()),
                        "sk".to_string() => AttributeValue::S("sk_to_delete_1".to_string()),
                    },
                    collection! {
                        "pk".to_string() => AttributeValue::S("pk_to_delete_2".to_string()),
                        "sk".to_string() => AttributeValue::S("sk_to_delete_2".to_string()),
                    },
                ]),
            )
            .returning(|_, _| Ok(BatchWriteItemOutput::builder().build()));

        let util = DynamoUtil {
            backend,
            table: "my_table".to_string(),
        };

        let keys = vec![
            PkSk {
                pk: "pk_to_delete_1".to_string(),
                sk: "sk_to_delete_1".to_string(),
            },
            PkSk {
                pk: "pk_to_delete_2".to_string(),
                sk: "sk_to_delete_2".to_string(),
            },
        ];

        let result = util.batch_delete_item(keys).await.unwrap();
        assert_eq!(result, ());
    }
}
