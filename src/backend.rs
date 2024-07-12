use std::collections::HashMap;

use async_trait::async_trait;
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
    types::AttributeValue,
};

pub mod mock;
pub mod real;

// Underlying backend, which performs the actual AWS operations. Kept generic so
// that it can be swapped with a mock backend for testing.
//
// Should be kept as minimal and close as possible to the real
// aws_sdk_dynamodb::Client, to minimize untestable code.
#[async_trait]
pub trait DynamoBackendImpl {
    async fn query(
        &self,
        table_name: String,
        index: Option<String>,
        condition: String,
        attribute_values: HashMap<String, AttributeValue>,
    ) -> Result<QueryOutput, SdkError<QueryError>>;

    async fn get_item(
        &self,
        table_name: String,
        key: HashMap<String, AttributeValue>,
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
