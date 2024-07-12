use std::collections::{BinaryHeap, HashMap};

use aws_sdk_dynamodb::{
    operation::{
        batch_write_item::BatchWriteItemError, delete_item::DeleteItemError,
        update_item::UpdateItemError,
    },
    types::AttributeValue,
};
use fractic_core::collection;
use fractic_generic_server_error::GenericServerError;

use crate::{
    backend::DynamoBackendImpl,
    errors::{DynamoConnectionError, DynamoInvalidOperationError, DynamoNotFoundError},
    schema::{
        id_calculations::{
            generate_id, get_last_id_label, get_pk_sk_from_map, ORDERED_IDS_DEFAULT_GAP,
            ORDERED_IDS_DIGITS, ORDERED_IDS_INIT,
        },
        parsing::{build_dynamo_map, parse_dynamo_map, IdKeys},
        DynamoObject,
    },
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
pub struct DynamoUtil<C: DynamoBackendImpl> {
    pub client: C,
}
impl<C: DynamoBackendImpl> DynamoUtil<C> {
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
            .filter_map(|item| {
                let sk = get_pk_sk_from_map(&item)
                    .expect("Query result item did not have pk/sk.")
                    .1;
                if get_last_id_label(sk) == T::id_label() {
                    // Item is of type T.
                    Some(parse_dynamo_map::<T>(&item))
                } else {
                    // Item is not of type T, but instead an inline child (of a
                    // different type), which will be skipped. Use query_dynamic
                    // to access objects of type T and their inline children.
                    None
                }
            })
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

    pub async fn batch_create_item<T: DynamoObject>(
        &self,
        table: String,
        parent_pk: String,
        parent_sk: String,
        objects_and_custom_ids: Vec<(&T, Option<String>)>,
    ) -> Result<Vec<(String, String)>, GenericServerError> {
        let dbg_cxt: &'static str = "batch_create_item";
        if objects_and_custom_ids.is_empty() {
            return Ok(Vec::new());
        }
        let (items, ids) = objects_and_custom_ids
            .into_iter()
            .map(|(object, custom_id)| {
                let new_id = custom_id.unwrap_or_else(generate_id);
                let new_pk = object.generate_pk(&parent_pk, &parent_sk, &new_id);
                let new_sk = object.generate_sk(&parent_pk, &parent_sk, &new_id);
                Ok((
                    build_dynamo_map::<T>(
                        &object,
                        IdKeys::Override(new_pk.clone(), new_sk.clone()),
                    )?,
                    (new_pk, new_sk),
                ))
            })
            .collect::<Result<Vec<(DynamoMap, (String, String))>, GenericServerError>>()?
            .into_iter()
            .unzip();
        self.client
            .batch_put_item(table, items)
            .await
            .map_err(|e| DynamoConnectionError::with_debug(dbg_cxt, "", format!("{:#?}", e)))?;
        Ok(ids)
    }

    async fn generate_ordered_custom_ids<T: DynamoObject>(
        &self,
        table: String,
        parent_pk: String,
        parent_sk: String,
        object: &T,
        insert_position: DynamoInsertPosition,
        num: usize,
    ) -> Result<Vec<String>, GenericServerError> {
        let dbg_cxt = "generate_ordered_custom_ids";

        // Search for all IDs for existing items of this type.
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
        let mut existing_ids_heap = query
            .iter()
            .filter_map(|item| item.sk())
            .filter_map(|sk| sk.split('#').last())
            .filter_map(|id_part| match id_part.parse::<u32>() {
                Ok(id) => Some(id),
                Err(_) => None,
            })
            .collect::<BinaryHeap<_>>();

        // Determine the new IDs based on insertion position.
        let mut results = Vec::new();
        let mut last_inserted_id = None;
        for _ in 0..num {
            let existing_ids_sorted_vec = existing_ids_heap.clone().into_sorted_vec();
            let min_id = existing_ids_sorted_vec.first().unwrap_or(&ORDERED_IDS_INIT);
            let max_id = existing_ids_sorted_vec.last().unwrap_or(&ORDERED_IDS_INIT);

            let new_id = match &insert_position {
                DynamoInsertPosition::First => min_id - ORDERED_IDS_DEFAULT_GAP,
                DynamoInsertPosition::Last => max_id + ORDERED_IDS_DEFAULT_GAP,
                DynamoInsertPosition::After {
                    sk: insert_position_sk,
                    ..
                } => {
                    let insert_after_id = if let Some(last_inserted_id) = last_inserted_id {
                        // Since we're generating multiple IDs, only the first
                        // one should follow the sk from the
                        // DynamoInsertPosition. All the rest should follow the
                        // last item.
                        last_inserted_id
                    } else {
                        let mut last_id_components = insert_position_sk.split('#').rev();
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
                                "expected: ".to_string()
                                    + T::id_label()
                                    + ", got: "
                                    + last_id_label,
                            ));
                        };
                        let Ok(id_as_number) = last_id_part.parse::<u32>() else {
                            return Err(DynamoInvalidOperationError::new(
                                dbg_cxt,
                                "'sk' of insert position does not have valid ordered ID structure (ID was not a number)",
                            ));
                        };
                        id_as_number
                    };
                    let position_index = existing_ids_sorted_vec
                    .iter()
                    .position(|id| id == &insert_after_id)
                    .ok_or(DynamoInvalidOperationError::new(
                        dbg_cxt,
                        "'sk' of insert position does not have valid ordered ID structure (ID was not found in existing items)",
                    ))?;
                    match existing_ids_sorted_vec.get(position_index + 1) {
                        Some(next_id) => insert_after_id + (next_id - insert_after_id) / 2,
                        None => insert_after_id + ORDERED_IDS_DEFAULT_GAP,
                    }
                }
            };
            // Since sorting based on strings, number of digits is important.
            results.push(format!("{:0width$}", new_id, width = ORDERED_IDS_DIGITS));

            last_inserted_id = Some(new_id);
            existing_ids_heap.push(new_id);
        }
        Ok(results)
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
        let new_id = self
            .generate_ordered_custom_ids(
                table.clone(),
                parent_pk.clone(),
                parent_sk.clone(),
                object,
                insert_position,
                1,
            )
            .await?
            .pop()
            .ok_or(DynamoInvalidOperationError::new(
                dbg_cxt,
                "failed to generate new ordered ID",
            ))?;
        self.create_item(table, parent_pk, parent_sk, object, Some(new_id))
            .await
    }

    pub async fn batch_create_item_ordered<T: DynamoObject>(
        &self,
        table: String,
        parent_pk: String,
        parent_sk: String,
        objects: Vec<&T>,
        insert_position: DynamoInsertPosition,
    ) -> Result<Vec<(String, String)>, GenericServerError> {
        if objects.is_empty() {
            return Ok(Vec::new());
        }
        let new_ids = self
            .generate_ordered_custom_ids(
                table.clone(),
                parent_pk.clone(),
                parent_sk.clone(),
                *objects.first().unwrap(),
                insert_position,
                objects.len(),
            )
            .await?;
        self.batch_create_item(
            table,
            parent_pk,
            parent_sk,
            objects
                .into_iter()
                .zip(new_ids.into_iter())
                .map(|(object, new_id)| (object, Some(new_id)))
                .collect(),
        )
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

    pub async fn batch_delete_item(
        &self,
        table: String,
        keys: Vec<(String, String)>,
    ) -> Result<(), GenericServerError> {
        let dbg_cxt: &'static str = "batch_delete_item";
        if keys.is_empty() {
            return Ok(());
        }
        let items = keys
            .into_iter()
            .map(|(pk, sk)| {
                collection! {
                    "pk".to_string() => AttributeValue::S(pk),
                    "sk".to_string() => AttributeValue::S(sk),
                }
            })
            .collect::<Vec<_>>();
        self.client
            .batch_delete_item(table, items)
            .await
            .map_err(|e| match e.into_service_error() {
                BatchWriteItemError::ResourceNotFoundException(_) => DynamoNotFoundError::default(),
                other => DynamoConnectionError::with_debug(dbg_cxt, "", format!("{:#?}", other)),
            })?;
        Ok(())
    }
}

// Tests.
// --------------------------------------------------

// TODO
