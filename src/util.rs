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
use fractic_generic_server_error::GenericServerError;

use crate::{
    errors::{DynamoConnectionError, DynamoInvalidOperationError, DynamoNotFoundError},
    id_calculations::{ORDERED_IDS_DEFAULT_GAP, ORDERED_IDS_DIGITS, ORDERED_IDS_INIT},
    schema::DynamoObject,
};

use super::{
    id_calculations::generate_id,
    parsing::{build_dynamo_map, parse_dynamo_map, IdKeys},
};

// AWS Dynamo utils.
// --------------------------------------------------

pub type DynamoMap = HashMap<String, AttributeValue>;

pub enum DynamoQueryMatchType {
    BeginsWith,
    Equals,
}

pub enum DynamoInsertPosition {
    First,
    Last,
    After { pk: String, sk: String },
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
        let new_id = custom_id.unwrap_or_else(generate_id);
        let new_pk = object.generate_pk(&parent_pk, &parent_sk, &new_id);
        let new_sk = object.generate_sk(&parent_pk, &parent_sk, &new_id);
        let map = build_dynamo_map::<T>(&object, IdKeys::Override(new_pk.clone(), new_sk.clone()))?;
        self.client
            .put_item(table, map)
            .await
            .map_err(|e| DynamoConnectionError::with_debug(dbg_cxt, "", format!("{:#?}", e)))?;
        Ok((new_pk, new_sk))
    }

    pub async fn create_item_ordered<T: DynamoObject>(
        &self,
        table: String,
        parent_pk: String,
        parent_sk: String,
        object: &T,
        insert_position: DynamoInsertPosition,
    ) -> Result<(String, String), GenericServerError> {
        let dbg_cxt: &'static str = "create_item_ordered";
        let search_pk = object.generate_pk(&parent_pk, &parent_sk, "");
        let search_sk = object.generate_sk(&parent_pk, &parent_sk, "");
        let query = self
            .query::<T>(
                table.clone(),
                None,
                search_pk,
                search_sk,
                DynamoQueryMatchType::BeginsWith,
            )
            .await?;
        let existing_number_ids = query
            .iter()
            .filter_map(|item| item.sk())
            .filter_map(|sk| sk.split('#').last())
            .filter_map(|id_part| match id_part.parse::<u32>() {
                Ok(id) => Some(id),
                Err(_) => None,
            })
            .collect::<Vec<_>>();
        let min_id = existing_number_ids
            .iter()
            .min()
            .unwrap_or(&ORDERED_IDS_INIT);
        let max_id = existing_number_ids
            .iter()
            .max()
            .unwrap_or(&ORDERED_IDS_INIT);
        let new_id = match insert_position {
            DynamoInsertPosition::First => min_id - ORDERED_IDS_DEFAULT_GAP,
            DynamoInsertPosition::Last => max_id + ORDERED_IDS_DEFAULT_GAP,
            DynamoInsertPosition::After { sk, .. } => {
                let mut last_id_components = sk.split('#').rev();
                let (Some(last_id_part), Some(last_id_label)) =
                    (last_id_components.next(), last_id_components.next())
                else {
                    return Err(DynamoInvalidOperationError::new(
                        dbg_cxt,
                        "'sk' of insert position does not have valid ordered ID structure (doesn't end in '[...#]label#ID')",
                    ));
                };
                if last_id_label != T::id_label() {
                    return Err(DynamoInvalidOperationError::with_debug(
                        dbg_cxt,
                        "'sk' of insert position does not have valid ordered ID structure (label does not match object type)",
                        "expected: ".to_string() + T::id_label() + ", got: " + last_id_label,
                    ));
                };
                let Ok(id_as_number) = last_id_part.parse::<u32>() else {
                    return Err(DynamoInvalidOperationError::new(
                        dbg_cxt,
                        "'sk' of insert position does not have valid ordered ID structure (ID was not a number)",
                    ));
                };
                let position_index = existing_number_ids
                    .iter()
                    .position(|id| id == &id_as_number)
                    .ok_or(DynamoInvalidOperationError::new(
                        dbg_cxt,
                        "'sk' of insert position does not have valid ordered ID structure (ID was not found in existing items)",
                    ))?;
                match existing_number_ids.get(position_index + 1) {
                    Some(next_id) => id_as_number + (next_id - id_as_number) / 2,
                    None => id_as_number + ORDERED_IDS_DEFAULT_GAP,
                }
            }
        };
        // Since sorting based on strings, number of digits is important.
        let new_id = format!("{:0width$}", new_id, width = ORDERED_IDS_DIGITS);
        self.create_item(table, parent_pk, parent_sk, object, Some(new_id))
            .await
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
