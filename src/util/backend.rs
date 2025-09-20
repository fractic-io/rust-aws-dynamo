use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_dynamodb::{
    error::SdkError,
    operation::{
        batch_write_item::{BatchWriteItemError, BatchWriteItemOutput},
        delete_item::{DeleteItemError, DeleteItemOutput},
        get_item::{GetItemError, GetItemOutput},
        put_item::{PutItemError, PutItemOutput},
        query::{QueryError, QueryOutput},
        update_item::{UpdateItemError, UpdateItemOutput},
    },
    types::{AttributeValue, DeleteRequest, PutRequest, WriteRequest},
};
use fractic_context::register_ctx_singleton;
use fractic_core::collection;
use mockall::automock;

use crate::DynamoCtxView;

// Underlying backend, which performs the actual AWS operations. Kept generic so
// that it can be swapped with a mock backend for testing.
//
// Should be kept as minimal and close as possible to the real
// aws_sdk_dynamodb::Client, to minimize untestable code.
#[automock]
#[async_trait]
pub trait DynamoBackend: Send + Sync {
    async fn query(
        &self,
        table_name: String,
        index: Option<String>,
        condition: String,
        attribute_values: HashMap<String, AttributeValue>,
    ) -> Result<Vec<QueryOutput>, SdkError<QueryError>>;

    async fn get_item(
        &self,
        table_name: String,
        key: HashMap<String, AttributeValue>,
        projection_expression: Option<String>,
    ) -> Result<GetItemOutput, SdkError<GetItemError>>;

    async fn put_item(
        &self,
        table_name: String,
        item: HashMap<String, AttributeValue>,
    ) -> Result<PutItemOutput, SdkError<PutItemError>>;

    async fn batch_put_item(
        &self,
        table_name: String,
        items: Vec<HashMap<String, AttributeValue>>,
    ) -> Result<BatchWriteItemOutput, SdkError<BatchWriteItemError>>;

    async fn update_item(
        &self,
        table_name: String,
        key: HashMap<String, AttributeValue>,
        update_expression: String,
        expression_attribute_values: HashMap<String, AttributeValue>,
        expression_attribute_names: HashMap<String, String>,
        condition_expression: Option<String>,
    ) -> Result<UpdateItemOutput, SdkError<UpdateItemError>>;

    async fn delete_item(
        &self,
        table_name: String,
        key: HashMap<String, AttributeValue>,
    ) -> Result<DeleteItemOutput, SdkError<DeleteItemError>>;

    async fn batch_delete_item(
        &self,
        table_name: String,
        keys: Vec<HashMap<String, AttributeValue>>,
    ) -> Result<BatchWriteItemOutput, SdkError<BatchWriteItemError>>;
}

// Real implementation,
// making actual calls to AWS.
// --------------------------------------------------

#[async_trait]
impl DynamoBackend for aws_sdk_dynamodb::Client {
    async fn query(
        &self,
        table_name: String,
        index: Option<String>,
        condition: String,
        attribute_values: HashMap<String, AttributeValue>,
    ) -> Result<Vec<QueryOutput>, SdkError<QueryError>> {
        self.query()
            .set_table_name(Some(table_name))
            .set_index_name(index)
            .set_key_condition_expression(Some(condition))
            .set_expression_attribute_values(Some(attribute_values))
            .into_paginator()
            .send()
            .try_collect()
            .await
    }

    async fn get_item(
        &self,
        table_name: String,
        key: HashMap<String, AttributeValue>,
        projection_expression: Option<String>,
    ) -> Result<GetItemOutput, SdkError<GetItemError>> {
        self.get_item()
            .set_table_name(Some(table_name))
            .set_key(Some(key))
            .set_projection_expression(projection_expression)
            .send()
            .await
    }

    async fn put_item(
        &self,
        table_name: String,
        item: HashMap<String, AttributeValue>,
    ) -> Result<PutItemOutput, SdkError<PutItemError>> {
        self.put_item()
            .set_table_name(Some(table_name))
            .set_item(Some(item))
            .send()
            .await
    }

    async fn batch_put_item(
        &self,
        table_name: String,
        items: Vec<HashMap<String, AttributeValue>>,
    ) -> Result<BatchWriteItemOutput, SdkError<BatchWriteItemError>> {
        self.batch_write_item()
            .set_request_items(Some(collection!(
                table_name => items
                    .into_iter()
                    .map(|item|
                        WriteRequest::builder()
                            .put_request(PutRequest::builder()
                            .set_item(Some(item))
                            .build()
                            .expect("Invalid PutRequest"))
                            .build()
                    )
                    .collect()
            )))
            .send()
            .await
    }

    async fn update_item(
        &self,
        table_name: String,
        key: HashMap<String, AttributeValue>,
        update_expression: String,
        expression_attribute_values: HashMap<String, AttributeValue>,
        expression_attribute_names: HashMap<String, String>,
        condition_expression: Option<String>,
    ) -> Result<UpdateItemOutput, SdkError<UpdateItemError>> {
        self.update_item()
            .set_table_name(Some(table_name))
            .set_key(Some(key))
            .set_update_expression(Some(update_expression))
            .set_expression_attribute_values(Some(expression_attribute_values))
            .set_expression_attribute_names(Some(expression_attribute_names))
            .set_condition_expression(condition_expression)
            .send()
            .await
    }

    async fn delete_item(
        &self,
        table_name: String,
        key: HashMap<String, AttributeValue>,
    ) -> Result<DeleteItemOutput, SdkError<DeleteItemError>> {
        self.delete_item()
            .set_table_name(Some(table_name))
            .set_key(Some(key))
            .send()
            .await
    }

    async fn batch_delete_item(
        &self,
        table_name: String,
        keys: Vec<HashMap<String, AttributeValue>>,
    ) -> Result<BatchWriteItemOutput, SdkError<BatchWriteItemError>> {
        self.batch_write_item()
            .set_request_items(Some(collection!(
                table_name => keys
                    .into_iter()
                    .map(|key|
                        WriteRequest::builder()
                            .delete_request(DeleteRequest::builder()
                            .set_key(Some(key))
                            .build()
                            .expect("Invalid DeleteRequest"))
                            .build()
                    )
                    .collect()
            )))
            .send()
            .await
    }
}

// Register dependency, default to real AWS backend.
// --------------------------------------------------

register_ctx_singleton!(dyn DynamoCtxView, dyn DynamoBackend, |ctx: Arc<
    dyn DynamoCtxView,
>| async move {
    let region = Region::new(ctx.dynamo_region().clone());
    let shared_config = aws_config::defaults(BehaviorVersion::v2025_08_07())
        .region(region)
        .load()
        .await;
    Ok(aws_sdk_dynamodb::Client::new(&shared_config))
});
