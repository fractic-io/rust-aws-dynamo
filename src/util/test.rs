#[cfg(test)]
mod tests {
    use crate::context::test_ctx::TestCtx;
    use crate::errors::DynamoNotFound;
    use crate::schema::{IdLogic, Timestamp};
    use crate::util::{CreateOptions, TtlConfig, AUTO_FIELDS_TTL, CHUNK_RESERVED_KEY};
    use crate::{
        dynamo_object,
        schema::{AutoFields, DynamoObject, NestingLogic, PkSk},
        util::{
            backend::MockDynamoBackend, DynamoQueryMatchType, DynamoUtil, AUTO_FIELDS_CREATED_AT,
            AUTO_FIELDS_SORT, AUTO_FIELDS_UPDATED_AT,
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
    use chrono::{DateTime, Utc};
    use core::panic;
    use fractic_core::collection;
    use mockall::predicate::*;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq)]
    pub struct TestDynamoObjectData {
        val_non_null: String,
        val_nullable: Option<String>,
    }
    dynamo_object!(
        TestDynamoObject,
        TestDynamoObjectData,
        "TEST",
        IdLogic::Uuid,
        NestingLogic::InlineChildOfAny
    );

    async fn build_util(mock_backend: MockDynamoBackend) -> DynamoUtil {
        let ctx = TestCtx::init_test("mock-region".to_string());
        ctx.override_dynamo_backend(Arc::new(mock_backend)).await;
        DynamoUtil::new(&*ctx, "my_table").await.unwrap()
    }

    fn build_item_no_data() -> (TestDynamoObject, HashMap<String, AttributeValue>) {
        (
            TestDynamoObject {
                id: PkSk {
                    pk: "ROOT".to_string(),
                    sk: "GROUP#123#TEST#1".to_string(),
                },
                auto_fields: Default::default(),
                data: TestDynamoObjectData::default(),
            },
            collection! {
                "pk".to_string() => AttributeValue::S("ROOT".to_string()),
                "sk".to_string() => AttributeValue::S("GROUP#123#TEST#1".to_string()),
                "val_non_null".to_string() => AttributeValue::S("".to_string()),
                // "val_nullable" should not be present, since keys with null
                // values are skipped.
            },
        )
    }

    fn build_item_high_sort() -> (TestDynamoObject, HashMap<String, AttributeValue>) {
        (
            TestDynamoObject {
                id: PkSk {
                    pk: "ROOT".to_string(),
                    sk: "GROUP#123#TEST#2".to_string(),
                },
                auto_fields: AutoFields {
                    sort: Some(0.75),
                    ..Default::default()
                },
                data: TestDynamoObjectData {
                    val_non_null: "high_sort".to_string(),
                    val_nullable: None,
                },
            },
            collection! {
                "pk".to_string() => AttributeValue::S("ROOT".to_string()),
                "sk".to_string() => AttributeValue::S("GROUP#123#TEST#2".to_string()),
                "sort".to_string() => AttributeValue::N("0.75".to_string()),
                "val_non_null".to_string() => AttributeValue::S("high_sort".to_string()),
                // "val_nullable" should not be present, since keys with null
                // values are skipped.
            },
        )
    }

    fn build_item_low_sort() -> (TestDynamoObject, HashMap<String, AttributeValue>) {
        (
            TestDynamoObject {
                id: PkSk {
                    pk: "ROOT".to_string(),
                    sk: "GROUP#123#TEST#3".to_string(),
                },
                auto_fields: AutoFields {
                    sort: Some(0.10001),
                    ..Default::default()
                },
                data: TestDynamoObjectData {
                    val_non_null: "low_sort".to_string(),
                    val_nullable: None,
                },
            },
            collection! {
                "pk".to_string() => AttributeValue::S("ROOT".to_string()),
                "sk".to_string() => AttributeValue::S("GROUP#123#TEST#3".to_string()),
                "sort".to_string() => AttributeValue::N("0.10001".to_string()),
                "val_non_null".to_string() => AttributeValue::S("low_sort".to_string()),
                // "val_nullable" should not be present, since keys with null
                // values are skipped.
            },
        )
    }

    #[tokio::test]
    async fn test_query() {
        let mut backend = MockDynamoBackend::new();
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

        let util = build_util(backend).await;
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
        assert_eq!(result[0].id(), build_item_low_sort().0.id());
        assert_eq!(result[0].data(), build_item_low_sort().0.data());
        assert_eq!(result[1].id(), build_item_high_sort().0.id());
        assert_eq!(result[1].data(), build_item_high_sort().0.data());
        assert_eq!(result[2].id(), build_item_no_data().0.id());
        assert_eq!(result[2].data(), build_item_no_data().0.data());
    }

    #[tokio::test]
    async fn test_query_with_chunk() {
        let sample_timestamp = Timestamp::now();

        let sample_chunk_id_1 = PkSk {
            pk: "GROUP#456".to_string(),
            sk: "TEST#0001".to_string(),
        };
        let sample_chunk_id_2 = PkSk {
            pk: "GROUP#456".to_string(),
            sk: "TEST#0002".to_string(),
        };
        let non_chunk_id = build_item_no_data().0.id;

        let mut backend = MockDynamoBackend::new();
        let cid1 = sample_chunk_id_1.clone();
        let cid2 = sample_chunk_id_2.clone();
        let tm_str = serde_json::to_string(&sample_timestamp)
            .unwrap()
            .strip_prefix("\"")
            .unwrap()
            .strip_suffix("\"")
            .unwrap()
            .to_string();
        backend
            .expect_query()
            .with(
                eq("my_table".to_string()),
                eq(None),
                eq("pk = :pk_val AND begins_with(sk, :sk_val)".to_string()),
                eq::<HashMap<String, AttributeValue>>(collection! {
                    ":pk_val".to_string() => AttributeValue::S("GROUP#456".to_string()),
                    ":sk_val".to_string() => AttributeValue::S("TEST#".to_string()),
                }),
            )
            .returning(move |_, _, _, _| {
                Ok(QueryOutput::builder()
                    .set_items(Some(vec![
                        // Non-chunk item:
                        build_item_no_data().1,
                        // Chunk item without sort:
                        collection! {
                            "pk".to_string() => AttributeValue::S(cid1.pk.clone()),
                            "sk".to_string() => AttributeValue::S(cid1.sk.clone()),
                            CHUNK_RESERVED_KEY.to_string() => AttributeValue::L(vec![
                                AttributeValue::M(build_item_high_sort().1),
                                AttributeValue::M(build_item_low_sort().1),
                            ]),
                        },
                        // Mismatching type (should be ignored):
                        collection! {
                            "pk".to_string() => AttributeValue::S("GROUP#456".to_string()),
                            "sk".to_string() => AttributeValue::S("OTHER#5555".to_string()),
                        },
                        // Chunk item with sort:
                        collection! {
                            "pk".to_string() => AttributeValue::S(cid2.pk.clone()),
                            "sk".to_string() => AttributeValue::S(cid2.sk.clone()),
                            AUTO_FIELDS_CREATED_AT.to_string() => AttributeValue::S(tm_str.clone()),
                            AUTO_FIELDS_UPDATED_AT.to_string() => AttributeValue::S(tm_str.clone()),
                            CHUNK_RESERVED_KEY.to_string() => AttributeValue::L(vec![
                                AttributeValue::M(build_item_high_sort().1),
                                AttributeValue::M(build_item_low_sort().1),
                            ]),
                            AUTO_FIELDS_SORT.to_string() => AttributeValue::N("0.5".to_string()),
                        },
                    ]))
                    .build())
            });

        let util = build_util(backend).await;
        let result = util
            .query::<TestDynamoObject>(
                None,
                PkSk {
                    pk: "GROUP#456".to_string(),
                    sk: "TEST#".to_string(),
                },
                DynamoQueryMatchType::BeginsWith,
            )
            .await
            .unwrap();

        // Should have expanded chunk into individual items.
        assert_eq!(result.len(), 5);
        let (item_1, item_2, item_3, item_4, item_5) = {
            let mut iter = result.into_iter();
            (
                iter.next().unwrap(),
                iter.next().unwrap(),
                iter.next().unwrap(),
                iter.next().unwrap(),
                iter.next().unwrap(),
            )
        };

        // Within a chunk, should be sorted by chunk order. But, at the
        // top-level it should be sorted by sort.
        assert_eq!(item_1.data.val_non_null, "high_sort");
        assert_eq!(item_1.auto_fields.sort, Some(0.5)); // Should buble up to top.
        assert_eq!(item_2.data.val_non_null, "low_sort");
        assert_eq!(item_2.auto_fields.sort, Some(0.5)); // Also, but not above item 1.
        assert_eq!(item_3.data.val_non_null, "");
        assert_eq!(item_3.auto_fields.sort, None);
        assert_eq!(item_4.data.val_non_null, "high_sort");
        assert_eq!(item_4.auto_fields.sort, None);
        assert_eq!(item_5.data.val_non_null, "low_sort");
        assert_eq!(item_5.auto_fields.sort, None);

        // And chunk items should have (share) the chunk's ID:
        assert_eq!(*item_1.id(), sample_chunk_id_2);
        assert_eq!(*item_2.id(), sample_chunk_id_2);
        assert_eq!(*item_3.id(), non_chunk_id);
        assert_eq!(*item_4.id(), sample_chunk_id_1);
        assert_eq!(*item_5.id(), sample_chunk_id_1);

        // And the chunk's metadata:
        assert_eq!(
            item_1.auto_fields.created_at,
            Some(sample_timestamp.clone())
        );
        assert_eq!(
            item_1.auto_fields.updated_at,
            Some(sample_timestamp.clone())
        );
        assert_eq!(
            item_2.auto_fields.created_at,
            Some(sample_timestamp.clone())
        );
        assert_eq!(
            item_2.auto_fields.updated_at,
            Some(sample_timestamp.clone())
        );
        assert_eq!(item_3.auto_fields.created_at, None);
        assert_eq!(item_3.auto_fields.updated_at, None);
        assert_eq!(item_4.auto_fields.created_at, None);
        assert_eq!(item_4.auto_fields.updated_at, None);
        assert_eq!(item_5.auto_fields.created_at, None);
        assert_eq!(item_5.auto_fields.updated_at, None);
    }

    #[tokio::test]
    async fn test_query_generic() {
        let mut backend = MockDynamoBackend::new();
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

        let util = build_util(backend).await;

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
        let mut backend = MockDynamoBackend::new();
        backend
            .expect_get_item()
            .with(
                eq("my_table".to_string()),
                eq::<HashMap<String, AttributeValue>>(collection! {
                    "pk".to_string() => AttributeValue::S("ROOT".to_string()),
                    "sk".to_string() => AttributeValue::S("GROUP#123#TEST#2".to_string())
                }),
                eq(None),
            )
            .returning(|_, _, _| {
                Ok(GetItemOutput::builder()
                    .set_item(Some(build_item_high_sort().1))
                    .build())
            });

        let util = build_util(backend).await;

        let result = util
            .get_item::<TestDynamoObject>(PkSk {
                pk: "ROOT".to_string(),
                sk: "GROUP#123#TEST#2".to_string(),
            })
            .await
            .unwrap();

        assert!(result.is_some());
        let item = result.unwrap();
        assert_eq!(item.pk(), "ROOT");
        assert_eq!(item.sk(), "GROUP#123#TEST#2");
        assert_eq!(item.data.val_non_null, "high_sort".to_string());
        assert_eq!(item.data.val_nullable, None);
    }

    #[tokio::test]
    async fn test_item_exists() {
        let mut backend = MockDynamoBackend::new();
        backend
            .expect_get_item()
            .with(
                eq("my_table".to_string()),
                eq::<HashMap<String, AttributeValue>>(collection! {
                    "pk".to_string() => AttributeValue::S("ROOT".to_string()),
                    "sk".to_string() => AttributeValue::S("GROUP#123#TEST#2".to_string())
                }),
                eq(Some("pk".to_string())),
            )
            .returning(|_, _, _| {
                Ok(GetItemOutput::builder()
                    .set_item(Some(collection! {
                        "pk".to_string() => AttributeValue::S("ROOT".to_string()),
                    }))
                    .build())
            });
        backend
            .expect_get_item()
            .with(
                eq("my_table".to_string()),
                eq::<HashMap<String, AttributeValue>>(collection! {
                    "pk".to_string() => AttributeValue::S("ROOT".to_string()),
                    "sk".to_string() => AttributeValue::S("NOT_EXISTS#456".to_string())
                }),
                eq(Some("pk".to_string())),
            )
            .returning(|_, _, _| Ok(GetItemOutput::builder().set_item(None).build()));

        let util = build_util(backend).await;

        let expect_exists = util
            .item_exists(PkSk {
                pk: "ROOT".to_string(),
                sk: "GROUP#123#TEST#2".to_string(),
            })
            .await;
        let expect_not_exists = util
            .item_exists(PkSk {
                pk: "ROOT".to_string(),
                sk: "NOT_EXISTS#456".to_string(),
            })
            .await;

        assert!(expect_exists.is_ok());
        assert!(expect_not_exists.is_ok());
        assert!(expect_exists.unwrap());
        assert!(!expect_not_exists.unwrap());
    }

    #[tokio::test]
    async fn test_create_item() {
        let mut backend = MockDynamoBackend::new();
        backend
            .expect_put_item()
            .withf(|_, item| {
                item.get(AUTO_FIELDS_CREATED_AT).is_some()
                    && item.get(AUTO_FIELDS_UPDATED_AT).is_some()
                    && item.get(AUTO_FIELDS_SORT).is_some()
                    && item.get(AUTO_FIELDS_SORT).unwrap().as_n().unwrap() == "0.75"
                    && item.get(AUTO_FIELDS_TTL).is_none()
                    && item.get("val_non_null").is_some()
                    && item.get("val_nullable").is_none()
            })
            .returning(|_, _| Ok(PutItemOutput::builder().build()));

        let util = build_util(backend).await;

        let new_item = build_item_high_sort().0;

        let result = util
            .create_item::<TestDynamoObject>(
                PkSk {
                    pk: "ROOT".to_string(),
                    sk: "GROUP#123".to_string(),
                },
                new_item.data,
                Some(CreateOptions {
                    custom_sort: Some(0.75),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();

        assert_eq!(result.pk(), "ROOT".to_string());
        assert!(result.sk().starts_with("GROUP#123#TEST#"));
        assert_eq!(result.sk().len(), 31);
    }

    #[tokio::test]
    async fn test_create_item_with_ttl() {
        let mut backend = MockDynamoBackend::new();
        backend
            .expect_put_item()
            .withf(|_, item| {
                item.get(AUTO_FIELDS_CREATED_AT).is_some()
                    && item.get(AUTO_FIELDS_UPDATED_AT).is_some()
                    && item.get(AUTO_FIELDS_SORT).is_none()
                    && item.get(AUTO_FIELDS_TTL).is_some()
                    && DateTime::from_timestamp(
                        item.get(AUTO_FIELDS_TTL)
                            .unwrap()
                            .as_n()
                            .unwrap()
                            .parse()
                            .unwrap(),
                        0,
                    )
                    .unwrap()
                    .format("%Y-%m-%d")
                    .to_string()
                        == (Utc::now() + chrono::Duration::days(365))
                            .format("%Y-%m-%d")
                            .to_string()
                    && item.get("val_non_null").is_some()
                    && item.get("val_nullable").is_none()
            })
            .returning(|_, _| Ok(PutItemOutput::builder().build()));

        let util = build_util(backend).await;

        let new_item = build_item_high_sort().0;

        let result = util
            .create_item::<TestDynamoObject>(
                PkSk {
                    pk: "ROOT".to_string(),
                    sk: "GROUP#123".to_string(),
                },
                new_item.data,
                Some(CreateOptions {
                    ttl: Some(TtlConfig::OneYear),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();

        assert_eq!(result.pk(), "ROOT".to_string());
        assert!(result.sk().starts_with("GROUP#123#TEST#"));
        assert_eq!(result.sk().len(), 31);
    }

    #[tokio::test]
    async fn test_batch_create_item() {
        let mut backend = MockDynamoBackend::new();
        backend
            .expect_batch_put_item()
            .withf(|_, items| {
                items.len() == 2
                    && items.iter().all(|item| {
                        item.get(AUTO_FIELDS_CREATED_AT).is_some()
                            && item.get(AUTO_FIELDS_UPDATED_AT).is_some()
                            && item.get(AUTO_FIELDS_SORT).is_some()
                            && item.get(AUTO_FIELDS_TTL).is_none()
                            && item.get("val_non_null").is_some()
                            && item.get("val_nullable").is_none()
                    })
            })
            .returning(|_, _| Ok(BatchWriteItemOutput::builder().build()));

        let util = build_util(backend).await;

        let item1 = build_item_no_data().0;
        let item2 = build_item_no_data().0;
        let items = vec![
            (
                item1.data,
                Some(CreateOptions {
                    custom_sort: Some(0.12),
                    ..Default::default()
                }),
            ),
            (
                item2.data,
                Some(CreateOptions {
                    custom_sort: Some(12.0),
                    ..Default::default()
                }),
            ),
        ];

        let result = util
            .batch_create_item::<TestDynamoObject>(
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
    async fn test_update_item_with_null() {
        let mut backend = MockDynamoBackend::new();
        backend
            .expect_update_item()
            .withf(|_, id, update_expr, values, keys, condition| {
                id.get("pk").unwrap().as_s().unwrap() == "ABC#123"
                    && id.get("sk").unwrap().as_s().unwrap() == "TEST#321"
                    && update_expr.trim() == "SET #k1 = :v1, #k2 = :v2 REMOVE #rmk1"
                    && values.get(":v1").is_some()
                    && values.get(":v2").is_some()
                    && values.get(":v3").is_none()
                    && {
                        let mut v = vec![keys.get("#k1").unwrap(), keys.get("#k2").unwrap()];
                        v.sort();
                        v
                    } == vec![&"updated_at".to_string(), &"val_non_null".to_string()]
                    && keys.get("#rmk1").unwrap() == "val_nullable"
                    && matches!(condition, Some(c) if c == "attribute_exists(pk)")
            })
            .returning(|_, _, _, _, _, _| Ok(UpdateItemOutput::builder().build()));

        let util = build_util(backend).await;

        let update_item = TestDynamoObject {
            id: PkSk {
                pk: "ABC#123".to_string(),
                sk: "TEST#321".to_string(),
            },
            auto_fields: Default::default(),
            data: TestDynamoObjectData {
                val_non_null: "new_data".into(),
                val_nullable: None,
            },
        };

        let result = util.update_item(&update_item).await.unwrap();
        assert_eq!(result, ());
    }

    #[tokio::test]
    async fn test_update_item_non_null() {
        let mut backend = MockDynamoBackend::new();
        backend
            .expect_update_item()
            .withf(|_, id, update_expr, values, keys, condition| {
                id.get("pk").unwrap().as_s().unwrap() == "ABC#123"
                    && id.get("sk").unwrap().as_s().unwrap() == "TEST#321"
                    && update_expr.trim() == "SET #k1 = :v1, #k2 = :v2, #k3 = :v3"
                    && values.get(":v1").is_some()
                    && values.get(":v2").is_some()
                    && values.get(":v3").is_some()
                    && {
                        let mut v = vec![
                            keys.get("#k1").unwrap(),
                            keys.get("#k2").unwrap(),
                            keys.get("#k3").unwrap(),
                        ];
                        v.sort();
                        v
                    } == vec![
                        &"updated_at".to_string(),
                        &"val_non_null".to_string(),
                        &"val_nullable".to_string(),
                    ]
                    && keys.get("#rmk1").is_none()
                    && matches!(condition, Some(c) if c == "attribute_exists(pk)")
            })
            .returning(|_, _, _, _, _, _| Ok(UpdateItemOutput::builder().build()));

        let util = build_util(backend).await;

        let update_item = TestDynamoObject {
            id: PkSk {
                pk: "ABC#123".to_string(),
                sk: "TEST#321".to_string(),
            },
            auto_fields: Default::default(),
            data: TestDynamoObjectData {
                val_non_null: "new_data".into(),
                val_nullable: Some("non_null".into()),
            },
        };

        let result = util.update_item(&update_item).await.unwrap();
        assert_eq!(result, ());
    }

    #[tokio::test]
    async fn test_update_item_transaction_existing() {
        let mut backend = MockDynamoBackend::new();
        backend
            .expect_get_item()
            .with(
                eq("my_table".to_string()),
                eq::<HashMap<String, AttributeValue>>(collection! {
                    "pk".to_string() => AttributeValue::S("ABC#123".to_string()),
                    "sk".to_string() => AttributeValue::S("TEST#321".to_string())
                }),
                eq(None),
            )
            .returning(|_, _, _| {
                Ok(GetItemOutput::builder()
                    .set_item(Some(collection! {
                        // ID & auto fields should /not/ be included in the
                        // condition expression of the transaction update:
                        "pk".to_string() => AttributeValue::S("ABC#123".to_string()),
                        "sk".to_string() => AttributeValue::S("TEST#321".to_string()),
                        "sort".to_string() => AttributeValue::N("0.75".to_string()),

                        // Non-null data fields /should/ be checked for changes
                        // in the transaction update condition expression:
                        "val_non_null".to_string() => AttributeValue::S("old_data".to_string()),
                    }))
                    .build())
            });
        backend
            .expect_update_item()
            .withf(|_, id, update_expr, values, keys, condition| {
                id.get("pk").unwrap().as_s().unwrap() == "ABC#123"
                    && id.get("sk").unwrap().as_s().unwrap() == "TEST#321"
                    && update_expr.trim() == "SET #k1 = :v1, #k2 = :v2, #k3 = :v3"
                    && values.get(":v1").is_some()
                    && values.get(":v2").is_some()
                    && values.get(":v3").is_some()
                    && {
                        let mut v = vec![
                            keys.get("#k1").unwrap(),
                            keys.get("#k2").unwrap(),
                            keys.get("#k3").unwrap(),
                        ];
                        v.sort();
                        v
                    } == vec![
                        &"updated_at".to_string(),
                        &"val_non_null".to_string(),
                        &"val_nullable".to_string(),
                    ]
                    && keys.get("#rmk1").is_none()
                    && *condition == Some("attribute_exists(pk) AND #c1 = :cv1".to_string())
                    && keys.get("#c1").unwrap() == "val_non_null"
                    && values.get(":cv1").unwrap().as_s().unwrap() == "old_data"
            })
            .returning(|_, _, _, _, _, _| Ok(UpdateItemOutput::builder().build()));

        let util = build_util(backend).await;

        let result = util
            .update_item_transaction::<TestDynamoObject>(
                PkSk {
                    pk: "ABC#123".to_string(),
                    sk: "TEST#321".to_string(),
                },
                |item| {
                    let Some(mut item) = item else {
                        return Err(DynamoNotFound::new());
                    };
                    item.val_non_null = "new_data".into();
                    item.val_nullable = Some("non_null".into());
                    Ok(item)
                },
            )
            .await;
        assert!(result.is_ok());

        let object_after = result.unwrap();
        assert_eq!(object_after.data.val_non_null, "new_data");
        assert_eq!(object_after.data.val_nullable, Some("non_null".into()));
    }

    #[tokio::test]
    async fn test_update_item_transaction_new() {
        let mut backend = MockDynamoBackend::new();
        backend
            .expect_get_item()
            .withf(|table, _key, _projection| table == "my_table")
            .returning(|_, _, _| Ok(GetItemOutput::builder().set_item(None).build()));
        backend
            .expect_update_item()
            .withf(|_, id, update_expr, values, keys, condition| {
                id.get("pk").unwrap().as_s().unwrap() == "ABC#123"
                    && id.get("sk").unwrap().as_s().unwrap() == "TEST#321"
                    && update_expr.trim() == "SET #k1 = :v1, #k2 = :v2, #k3 = :v3"
                    && values.get(":v1").is_some()
                    && values.get(":v2").is_some()
                    && values.get(":v3").is_some()
                    && {
                        let mut v = vec![
                            keys.get("#k1").unwrap(),
                            keys.get("#k2").unwrap(),
                            keys.get("#k3").unwrap(),
                        ];
                        v.sort();
                        v
                    } == vec![
                        &"updated_at".to_string(),
                        &"val_non_null".to_string(),
                        &"val_nullable".to_string(),
                    ]
                    && keys.get("#rmk1").is_none()
                    && *condition == Some("attribute_not_exists(pk)".to_string())
                    && keys.get("#c1").is_none()
                    && values.get(":cv1").is_none()
            })
            .returning(|_, _, _, _, _, _| Ok(UpdateItemOutput::builder().build()));

        let util = build_util(backend).await;

        let result = util
            .update_item_transaction::<TestDynamoObject>(
                PkSk {
                    pk: "ABC#123".to_string(),
                    sk: "TEST#321".to_string(),
                },
                |item| {
                    if item.is_some() {
                        panic!("Item should not exist");
                    }
                    Ok(TestDynamoObjectData {
                        val_non_null: "new_data".into(),
                        val_nullable: Some("non_null".into()),
                    })
                },
            )
            .await;
        assert!(result.is_ok());

        let object_after = result.unwrap();
        assert_eq!(object_after.data.val_non_null, "new_data");
        assert_eq!(object_after.data.val_nullable, Some("non_null".into()));
    }

    #[tokio::test]
    async fn test_delete_item() {
        let mut backend = MockDynamoBackend::new();
        backend
            .expect_delete_item()
            .with(
                eq("my_table".to_string()),
                eq::<HashMap<String, AttributeValue>>(collection! {
                    "pk".to_string() => AttributeValue::S("GROUP#123".to_string()),
                    "sk".to_string() => AttributeValue::S("LIST#123#TEST#456".to_string())
                }),
            )
            .returning(|_, _| Ok(DeleteItemOutput::builder().build()));

        let util = build_util(backend).await;

        let result = util
            .delete_item::<TestDynamoObject>(PkSk {
                pk: "GROUP#123".to_string(),
                sk: "LIST#123#TEST#456".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(result, ());
    }

    #[tokio::test]
    async fn test_delete_item_invalid_type() {
        let mut backend = MockDynamoBackend::new();
        backend
            .expect_delete_item()
            .with(
                eq("my_table".to_string()),
                eq::<HashMap<String, AttributeValue>>(collection! {
                    "pk".to_string() => AttributeValue::S("GROUP#123".to_string()),
                    "sk".to_string() => AttributeValue::S("LIST#123#WRONGTYPE#456".to_string())
                }),
            )
            .returning(|_, _| Ok(DeleteItemOutput::builder().build()));

        let util = build_util(backend).await;

        let result = util
            .delete_item::<TestDynamoObject>(PkSk {
                pk: "GROUP#123".to_string(),
                sk: "LIST#123#WRONGTYPE#456".to_string(),
            })
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_batch_delete_item() {
        let mut backend = MockDynamoBackend::new();
        backend
            .expect_batch_delete_item()
            .with(
                eq("my_table".to_string()),
                eq(vec![
                    collection! {
                        "pk".to_string() => AttributeValue::S("ABC#123".to_string()),
                        "sk".to_string() => AttributeValue::S("TEST#321".to_string()),
                    },
                    collection! {
                        "pk".to_string() => AttributeValue::S("DEF#456".to_string()),
                        "sk".to_string() => AttributeValue::S("TEST#654".to_string()),
                    },
                ]),
            )
            .returning(|_, _| Ok(BatchWriteItemOutput::builder().build()));

        let util = build_util(backend).await;

        let keys = vec![
            PkSk {
                pk: "ABC#123".to_string(),
                sk: "TEST#321".to_string(),
            },
            PkSk {
                pk: "DEF#456".to_string(),
                sk: "TEST#654".to_string(),
            },
        ];

        let result = util
            .batch_delete_item::<TestDynamoObject>(keys)
            .await
            .unwrap();
        assert_eq!(result, ());
    }

    #[tokio::test]
    async fn test_query_all() {
        let mut backend = MockDynamoBackend::new();
        backend
            .expect_query()
            .with(
                eq("my_table".to_string()),
                eq(None),
                eq("pk = :pk_val AND begins_with(sk, :sk_val)".to_string()),
                eq::<HashMap<String, AttributeValue>>(collection! {
                    ":pk_val".to_string() => AttributeValue::S("ROOT".to_string()),
                    ":sk_val".to_string() => AttributeValue::S("GROUP#123#TEST#".to_string()),
                }),
            )
            .returning(|_, _, _, _| {
                Ok(QueryOutput::builder()
                    .set_items(Some(vec![
                        build_item_high_sort().1,
                        build_item_no_data().1,
                        build_item_low_sort().1,
                    ]))
                    .build())
            });

        let util = build_util(backend).await;
        let result = util
            .query_all::<TestDynamoObject>(PkSk {
                pk: "ROOT".to_string(),
                sk: "GROUP#123".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(result.len(), 3);

        // Sorted by sort: low_sort, high_sort, no_data
        assert_eq!(result[0].id(), build_item_low_sort().0.id());
        assert_eq!(result[1].id(), build_item_high_sort().0.id());
        assert_eq!(result[2].id(), build_item_no_data().0.id());
    }

    #[tokio::test]
    async fn test_batch_delete_all() {
        let mut backend = MockDynamoBackend::new();

        // Expect query_all, return one child.
        backend
            .expect_query()
            .with(
                eq("my_table".to_string()),
                eq(None),
                eq("pk = :pk_val AND begins_with(sk, :sk_val)".to_string()),
                eq::<HashMap<String, AttributeValue>>(collection! {
                    ":pk_val".to_string() => AttributeValue::S("ROOT".to_string()),
                    ":sk_val".to_string() => AttributeValue::S("GROUP#123#TEST#".to_string()),
                }),
            )
            .returning(|_, _, _, _| {
                Ok(QueryOutput::builder()
                    .set_items(Some(vec![build_item_no_data().1]))
                    .build())
            });

        // Expect batch_delete_item, return success.
        backend
            .expect_batch_delete_item()
            .with(
                eq("my_table".to_string()),
                eq(vec![collection! {
                    "pk".to_string() => AttributeValue::S("ROOT".to_string()),
                    "sk".to_string() => AttributeValue::S("GROUP#123#TEST#1".to_string()),
                }]),
            )
            .returning(|_, _| Ok(BatchWriteItemOutput::builder().build()));

        let util = build_util(backend).await;
        let result = util
            .batch_delete_all::<TestDynamoObject>(PkSk {
                pk: "ROOT".to_string(),
                sk: "GROUP#123".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(result, ());
    }
}
