use std::collections::HashMap;

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_cognitoidentityprovider::{config::Region, error::SdkError};
use aws_sdk_dynamodb::{
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
use fractic_core::collection;
use fractic_env_config::EnvVariables;
use fractic_generic_server_error::GenericServerError;

use crate::{env::DynamoEnvConfig, util::DynamoUtil};

use super::DynamoBackendImpl;

// Real implementation,
// making actual calls to AWS.
// --------------------------------------------------

impl DynamoUtil<aws_sdk_dynamodb::Client> {
    pub async fn new(
        env: EnvVariables<DynamoEnvConfig>,
        table: impl Into<String>,
    ) -> Result<Self, GenericServerError> {
        let region_str = env.get(&DynamoEnvConfig::DynamoRegion)?;
        let region = Region::new(region_str.clone());
        let shared_config = aws_config::defaults(BehaviorVersion::v2024_03_28())
            .region(region)
            .load()
            .await;
        let client = aws_sdk_dynamodb::Client::new(&shared_config);
        Ok(Self {
            backend: client,
            table: table.into(),
        })
    }
}

#[async_trait]
impl DynamoBackendImpl for aws_sdk_dynamodb::Client {
    async fn query(
        &self,
        table_name: String,
        index: Option<String>,
        condition: String,
        attribute_values: HashMap<String, AttributeValue>,
    ) -> Result<QueryOutput, SdkError<QueryError>> {
        self.query()
            .set_table_name(Some(table_name))
            .set_index_name(index)
            .set_key_condition_expression(Some(condition))
            .set_expression_attribute_values(Some(attribute_values))
            .send()
            .await
    }

    async fn get_item(
        &self,
        table_name: String,
        key: HashMap<String, AttributeValue>,
    ) -> Result<GetItemOutput, SdkError<GetItemError>> {
        self.get_item()
            .set_table_name(Some(table_name))
            .set_key(Some(key))
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
    ) -> Result<UpdateItemOutput, SdkError<UpdateItemError>> {
        self.update_item()
            .set_table_name(Some(table_name))
            .set_key(Some(key))
            .set_update_expression(Some(update_expression))
            .set_expression_attribute_values(Some(expression_attribute_values))
            .set_expression_attribute_names(Some(expression_attribute_names))
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