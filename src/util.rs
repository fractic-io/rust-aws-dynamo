use std::collections::HashMap;

use async_trait::async_trait;
use aws_sdk_cognitoidentityprovider::error::SdkError;
use aws_sdk_dynamodb::{
    operation::{
        delete_item::{DeleteItemError, DeleteItemOutput},
        get_item::{GetItemError, GetItemOutput},
        put_item::{PutItemError, PutItemOutput},
        query::{QueryError, QueryOutput},
        update_item::{UpdateItemError, UpdateItemOutput},
    },
    types::AttributeValue,
};
use fractic_core::collection;
use fractic_generic_server_error::{common::CriticalError, GenericServerError};
use serde::{de::DeserializeOwned, Serialize};

use crate::errors::{DynamoConnectionError, DynamoInvalidOperationError, DynamoNotFoundError};

use super::{
    id_calculations::generate_id,
    parsing::{build_dynamo_map, parse_dynamo_map, IdKeys},
};

// AWS Dynamo utils.
// --------------------------------------------------

pub trait DynamoObject: Serialize + DeserializeOwned + std::fmt::Debug {
    fn pk(&self) -> Option<&str>;
    fn pk_or_critical(&self) -> Result<&str, GenericServerError> {
        let dbg_cxt: &'static str = "pk_or_critical";
        Ok(self.pk().ok_or_else(|| {
            CriticalError::with_debug(
                dbg_cxt,
                "DynamoObject did not have pk!",
                Self::id_label().to_string(),
            )
        })?)
    }
    fn sk(&self) -> Option<&str>;
    fn sk_or_critical(&self) -> Result<&str, GenericServerError> {
        let dbg_cxt: &'static str = "sk_or_critical";
        Ok(self.sk().ok_or_else(|| {
            CriticalError::with_debug(
                dbg_cxt,
                "DynamoObject did not have sk!",
                Self::id_label().to_string(),
            )
        })?)
    }
    fn id_label() -> &'static str;
    fn generate_pk(&self, parent_pk: &str, parent_sk: &str, new_id: &str) -> String;
    fn generate_sk(&self, parent_pk: &str, parent_sk: &str, new_id: &str) -> String;
}

pub type DynamoMap = HashMap<String, AttributeValue>;

pub enum DynamoQueryMatchType {
    BeginsWith,
    Equals,
}

// Main class, to be used by clients.
pub struct DynamoUtil<C: DynamoClientImpl> {
    pub client: C,
}
impl<C: DynamoClientImpl> DynamoUtil<C> {
    pub async fn query<T: DynamoObject>(
        &self,
        table: String,
        index: Option<String>,
        pk: String,
        sk: String,
        match_type: DynamoQueryMatchType,
    ) -> Result<Vec<T>, GenericServerError> {
        let dbg_cxt: &'static str = "query";
        let condition = match match_type {
            DynamoQueryMatchType::BeginsWith => "pk = :pk_val AND begins_with(sk, :sk_val)",
            DynamoQueryMatchType::Equals => "pk = :pk_val AND sk = :sk_val",
        }
        .to_string();
        let attribute_values = collection! {
            ":pk_val".to_string() => AttributeValue::S(pk),
            ":sk_val".to_string() => AttributeValue::S(sk),
        };
        let response = self
            .client
            .query(table, index, condition, attribute_values)
            .await
            .map_err(|e| DynamoConnectionError::with_debug(dbg_cxt, "", format!("{:#?}", e)))?;
        let items = response.items();
        items
            .into_iter()
            .map(|item| parse_dynamo_map::<T>(&item))
            .collect::<Result<Vec<T>, GenericServerError>>()
    }

    pub async fn query_generic(
        &self,
        table: String,
        index: Option<String>,
        pk: String,
        sk: String,
        match_type: DynamoQueryMatchType,
    ) -> Result<Vec<DynamoMap>, GenericServerError> {
        let dbg_cxt: &'static str = "query_generic";
        let condition = match match_type {
            DynamoQueryMatchType::BeginsWith => "pk = :pk_val AND begins_with(sk, :sk_val)",
            DynamoQueryMatchType::Equals => "pk = :pk_val AND sk = :sk_val",
        }
        .to_string();
        let attribute_values = collection! {
            ":pk_val".to_string() => AttributeValue::S(pk),
            ":sk_val".to_string() => AttributeValue::S(sk),
        };
        let response = self
            .client
            .query(table, index, condition, attribute_values)
            .await
            .map_err(|e| DynamoConnectionError::with_debug(dbg_cxt, "", format!("{:#?}", e)))?;
        let items = response.items();
        Ok(items.to_vec())
    }

    pub async fn get_item<T: DynamoObject>(
        &self,
        table: String,
        pk: String,
        sk: String,
    ) -> Result<Option<T>, GenericServerError> {
        let dbg_cxt: &'static str = "get_item";
        let key = collection! {
            "pk".to_string() => AttributeValue::S(pk),
            "sk".to_string() => AttributeValue::S(sk),
        };
        let response = self
            .client
            .get_item(table, key)
            .await
            .map_err(|e| DynamoConnectionError::with_debug(dbg_cxt, "", format!("{:#?}", e)))?;
        response
            .item
            .map(|item| parse_dynamo_map::<T>(&item))
            .transpose()
    }

    pub async fn create_item<T: DynamoObject>(
        &self,
        table: String,
        parent_pk: String,
        parent_sk: String,
        object: &T,
        custom_id: Option<String>,
    ) -> Result<(String, String), GenericServerError> {
        let dbg_cxt: &'static str = "create_item";
        let new_id = custom_id.unwrap_or(generate_id());
        let new_pk = object.generate_pk(&parent_pk, &parent_sk, &new_id);
        let new_sk = object.generate_sk(&parent_pk, &parent_sk, &new_id);
        let map = build_dynamo_map::<T>(&object, IdKeys::Override(new_pk.clone(), new_sk.clone()))?;
        self.client
            .put_item(table, map)
            .await
            .map_err(|e| DynamoConnectionError::with_debug(dbg_cxt, "", format!("{:#?}", e)))?;
        Ok((new_pk, new_sk))
    }

    pub async fn update_item<T: DynamoObject>(
        &self,
        table: String,
        object: &T,
    ) -> Result<(), GenericServerError> {
        let dbg_cxt: &'static str = "update_item";
        let key = collection! {
            "pk".to_string() => AttributeValue::S(object.pk().ok_or(
                DynamoInvalidOperationError::new(
                    dbg_cxt, "pk required"))?.to_string()),
            "sk".to_string() => AttributeValue::S(object.sk().ok_or(
                DynamoInvalidOperationError::new(
                    dbg_cxt, "sk required"))?.to_string()),
        };
        let map = build_dynamo_map::<T>(&object, IdKeys::None)?;
        let mut expression_attribute_names = HashMap::new();
        let mut expression_attribute_values = HashMap::new();
        let update_expression = "SET ".to_string()
            + &map
                .iter()
                .enumerate()
                .map(|(idx, (key, _))| {
                    let key_placeholder = format!("#k{}", idx + 1);
                    let value_placeholder = format!(":v{}", idx + 1);
                    expression_attribute_names.insert(key_placeholder.clone(), key.to_string());
                    expression_attribute_values.insert(value_placeholder.clone(), map[key].clone());
                    format!("{} = {}", key_placeholder, value_placeholder)
                })
                .collect::<Vec<String>>()
                .join(", ");
        self.client
            .update_item(
                table,
                key,
                update_expression,
                expression_attribute_values,
                expression_attribute_names,
            )
            .await
            .map_err(|e| match e.into_service_error() {
                UpdateItemError::ResourceNotFoundException(_) => DynamoNotFoundError::default(),
                other => DynamoConnectionError::with_debug(dbg_cxt, "", format!("{:#?}", other)),
            })?;
        Ok(())
    }

    pub async fn delete_item(
        &self,
        table: String,
        pk: String,
        sk: String,
    ) -> Result<(), GenericServerError> {
        let dbg_cxt: &'static str = "delete_item";
        let key = collection! {
            "pk".to_string() => AttributeValue::S(pk),
            "sk".to_string() => AttributeValue::S(sk),
        };
        self.client
            .delete_item(table, key)
            .await
            .map_err(|e| match e.into_service_error() {
                DeleteItemError::ResourceNotFoundException(_) => DynamoNotFoundError::default(),
                other => DynamoConnectionError::with_debug(dbg_cxt, "", format!("{:#?}", other)),
            })?;
        Ok(())
    }
}

// Underlying client, which performs the actual AWS operations.
// Kept generic so that it can be swapped with a mock client for testing.
//
// Should be kept as minimal and close as possible to the real
// aws_sdk_dynamodb::Client, to minimize untestable code.
#[async_trait]
pub trait DynamoClientImpl {
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
}

// Tests.
// --------------------------------------------------

// TODO
