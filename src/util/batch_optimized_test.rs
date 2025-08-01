
#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::test_ctx::TestCtx;
    use crate::errors::DynamoInvalidOperation;
    use crate::schema::{IdLogic, NestingLogic, PkSk};
    use crate::util::{backend::MockDynamoBackend, DynamoUtil};
    use aws_sdk_dynamodb::operation::batch_write_item::BatchWriteItemOutput;
    use aws_sdk_dynamodb::operation::query::QueryOutput;
    use aws_sdk_dynamodb::types::AttributeValue;
    use fractic_core::collection;
    use mockall::predicate::*;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    #[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq)]
    pub struct BatchData {
        val: String,
    }

    dynamo_object!(
        BatchObject,
        BatchData,
        "BATCH",
        IdLogic::BatchOptimized(2), // chunk_size = 2
        NestingLogic::TopLevelChildOfAny
    );

    // Helper to build util with provided mock backend
    async fn build_util(mock_backend: MockDynamoBackend) -> DynamoUtil {
        let ctx = TestCtx::init_test("mock-region".to_string());
        ctx.override_dynamo_backend(Arc::new(mock_backend)).await;
        DynamoUtil::new(&*ctx, "my_table").await.unwrap()
    }

    // ---------------------------------------------------------------------
    // batch_replace_all_ordered – initial insert
    // ---------------------------------------------------------------------
    #[tokio::test]
    async fn test_batch_replace_all_ordered_insert() {
        let mut backend = MockDynamoBackend::new();

        // 1) _delete_all_batch_optimized_chunks issues a query – return empty.
        backend
            .expect_query()
            .withf(|table, _idx, cond, _| {
                table == "my_table" && cond.contains("begins_with(sk")
            })
            .returning(|_, _, _, _| Ok(QueryOutput::builder().set_items(Some(vec![])).build()));

        // 2) raw_batch_delete_ids will not be called (no items), but raw_batch_put_item will.
        backend
            .expect_batch_put_item()
            .withf(|table, items| {
                // Expect two chunk rows for 3 logical items with chunk_size 2.
                if table != "my_table" { return false; }
                items.len() == 2 &&
                items.iter().all(|m| m.contains_key("_") && m.get("sk").unwrap().as_s().unwrap().contains("__CHUNK"))
            })
            .returning(|_, _| Ok(BatchWriteItemOutput::builder().build()));

        let util = build_util(backend).await;
        let parent_id = PkSk { pk: "ROOT".to_string(), sk: "GROUP#ABC".to_string() };

        let data = vec![
            BatchData { val: "one".to_string() },
            BatchData { val: "two".to_string() },
            BatchData { val: "three".to_string() },
        ];

        let created = util
            .batch_replace_all_ordered::<BatchObject>(parent_id.clone(), data.clone())
            .await
            .unwrap();

        // Expect same length as input.
        assert_eq!(created.len(), 3);
        // IDs should increment starting from 1 with minimal padding.
        assert_eq!(created[0].sk(), "BATCH#1");
        assert_eq!(created[1].sk(), "BATCH#2");
        assert_eq!(created[2].sk(), "BATCH#3");
        // pk should be parent's sk.
        assert_eq!(created[0].pk(), "GROUP#ABC");
    }

    // ---------------------------------------------------------------------
    // batch_replace_all_ordered – deletes obsolete chunks
    // ---------------------------------------------------------------------
    #[tokio::test]
    async fn test_batch_replace_all_ordered_replaces_existing() {
        let mut backend = MockDynamoBackend::new();

        // Existing two chunk rows will be queried and then deleted
        let existing_items = vec![
            collection! {
                "pk".to_string() => AttributeValue::S("GROUP#ABC".to_string()),
                "sk".to_string() => AttributeValue::S("BATCH#__CHUNK#000000".to_string()),
            },
            collection! {
                "pk".to_string() => AttributeValue::S("GROUP#ABC".to_string()),
                "sk".to_string() => AttributeValue::S("BATCH#__CHUNK#000001".to_string()),
            },
        ];

        backend
            .expect_query()
            .returning(move |_, _, _, _| {
                Ok(QueryOutput::builder().set_items(Some(existing_items.clone())).build())
            });

        backend
            .expect_batch_delete_item()
            .withf(|table, keys| {
                table == "my_table" && keys.len() == 2
            })
            .returning(|_, _| Ok(BatchWriteItemOutput::builder().build()));

        // New put call – can be any size, accept.
        backend
            .expect_batch_put_item()
            .returning(|_, _| Ok(BatchWriteItemOutput::builder().build()));

        let util = build_util(backend).await;
        let parent_id = PkSk { pk: "ROOT".to_string(), sk: "GROUP#ABC".to_string() };
        let data = vec![BatchData { val: "only".to_string() }];
        let res = util.batch_replace_all_ordered::<BatchObject>(parent_id, data).await;
        assert!(res.is_ok());
    }

    // ---------------------------------------------------------------------
    // query – unchunking behaviour
    // ---------------------------------------------------------------------
    #[tokio::test]
    async fn test_query_unchunks_batch_items() {
        let mut backend = MockDynamoBackend::new();

        // Build child items inside chunk row
        let child1 = collection! {
            "pk".to_string() => AttributeValue::S("GROUP#ABC".to_string()),
            "sk".to_string() => AttributeValue::S("BATCH#001".to_string()),
            "val".to_string() => AttributeValue::S("one".to_string()),
        };
        let child2 = collection! {
            "pk".to_string() => AttributeValue::S("GROUP#ABC".to_string()),
            "sk".to_string() => AttributeValue::S("BATCH#002".to_string()),
            "val".to_string() => AttributeValue::S("two".to_string()),
        };

        let chunk_row = collection! {
            "pk".to_string() => AttributeValue::S("GROUP#ABC".to_string()),
            "sk".to_string() => AttributeValue::S("BATCH#__CHUNK#000000".to_string()),
            "_".to_string() => AttributeValue::L(vec![AttributeValue::M(child1.clone()), AttributeValue::M(child2.clone())])
        };

        backend
            .expect_query()
            .returning(move |_, _, _, _| {
                Ok(QueryOutput::builder().set_items(Some(vec![chunk_row.clone()])).build())
            });

        let util = build_util(backend).await;
        let parent_id = PkSk { pk: "GROUP#ABC".to_string(), sk: "".to_string() };
        let items = util
            .query::<BatchObject>(None, parent_id, DynamoQueryMatchType::BeginsWith)
            .await
            .unwrap();

        assert_eq!(items.len(), 2);
        assert_eq!(items[0].data.val, "one");
        assert_eq!(items[1].data.val, "two");
    }

    // ---------------------------------------------------------------------
    // unsupported operations
    // ---------------------------------------------------------------------
    #[tokio::test]
    async fn test_unsupported_create_item() {
        let backend = MockDynamoBackend::new();
        let util = build_util(backend).await;
        let parent_id = PkSk { pk: "ROOT".to_string(), sk: "GROUP#ABC".to_string() };
        let result = util
            .create_item::<BatchObject>(parent_id, BatchData { val: "x".into() }, None)
            .await;
        assert!(matches!(result, Err(e) if e.is::<DynamoInvalidOperation>()));
    }
}