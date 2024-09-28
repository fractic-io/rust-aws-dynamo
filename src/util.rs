use std::collections::HashMap;

use aws_sdk_dynamodb::{
    operation::{
        batch_write_item::BatchWriteItemError, delete_item::DeleteItemError,
        update_item::UpdateItemError,
    },
    types::AttributeValue,
};
use backend::DynamoBackendImpl;
use calculate_sort::calculate_sort_values;
use fractic_core::collection;
use fractic_generic_server_error::GenericServerError;

use crate::{
    errors::{DynamoConnectionError, DynamoInvalidOperation, DynamoNotFound},
    schema::{
        id_calculations::{generate_pk_sk, get_object_type, get_pk_sk_from_map},
        parsing::{
            build_dynamo_map_for_existing_obj, build_dynamo_map_for_new_obj, parse_dynamo_map,
            IdKeys,
        },
        DynamoObject, IdLogic, PkSk, Timestamp,
    },
};

pub mod backend;
mod calculate_sort;
mod test;

pub type DynamoMap = HashMap<String, AttributeValue>;
pub const AUTO_FIELDS_CREATED_AT: &str = "created_at";
pub const AUTO_FIELDS_UPDATED_AT: &str = "updated_at";
pub const AUTO_FIELDS_SORT: &str = "sort";

pub enum DynamoQueryMatchType {
    BeginsWith,
    Equals,
}

pub enum DynamoInsertPosition {
    First,
    Last,
    After(PkSk),
}

fn _validate_id<T: DynamoObject>(
    dbg_cxt: &'static str,
    id: &PkSk,
) -> Result<(), GenericServerError> {
    if id.object_type()? != T::id_label() {
        return Err(DynamoInvalidOperation::with_debug(
            dbg_cxt,
            "ID does not match object type.",
            format!("Expected object type: {}, got ID: {}", T::id_label(), id),
        ));
    }
    Ok(())
}

pub struct DynamoUtil<B: DynamoBackendImpl> {
    pub backend: B,
    pub table: String,
}
impl<C: DynamoBackendImpl> DynamoUtil<C> {
    pub async fn query<T: DynamoObject>(
        &self,
        index: Option<String>,
        id: PkSk,
        match_type: DynamoQueryMatchType,
    ) -> Result<Vec<T>, GenericServerError> {
        self.query_generic(index, id, match_type)
            .await?
            .into_iter()
            .filter_map(|item| {
                let (pk, sk) =
                    get_pk_sk_from_map(&item).expect("Query result item did not have pk/sk.");
                match get_object_type(pk, sk) {
                    Ok(label) if label == T::id_label() => {
                        // Item is of type T.
                        Some(parse_dynamo_map::<T>(&item))
                    }
                    _ => {
                        // Item is not of type T, but instead an inline child (of a
                        // different type), which will be skipped. Use query_dynamic
                        // to access objects of type T and their inline children.
                        None
                    }
                }
            })
            .collect::<Result<Vec<T>, GenericServerError>>()
    }

    pub async fn query_generic(
        &self,
        index: Option<String>,
        id: PkSk,
        match_type: DynamoQueryMatchType,
    ) -> Result<Vec<DynamoMap>, GenericServerError> {
        let dbg_cxt: &'static str = "query_generic";
        let condition = match match_type {
            DynamoQueryMatchType::BeginsWith if id.sk.is_empty() => "pk = :pk_val",
            DynamoQueryMatchType::BeginsWith => "pk = :pk_val AND begins_with(sk, :sk_val)",
            DynamoQueryMatchType::Equals => "pk = :pk_val AND sk = :sk_val",
        }
        .to_string();
        let mut attribute_values = HashMap::new();
        attribute_values.insert(":pk_val".to_string(), AttributeValue::S(id.pk));
        if !id.sk.is_empty() {
            attribute_values.insert(":sk_val".to_string(), AttributeValue::S(id.sk));
        }
        let response = self
            .backend
            .query(self.table.clone(), index, condition, attribute_values)
            .await
            .map_err(|e| DynamoConnectionError::with_debug(dbg_cxt, "", format!("{:#?}", e)))?;
        let mut items = response.items().to_vec();
        items.sort_by(|a, b| {
            let a_sort = a
                .get(AUTO_FIELDS_SORT)
                .and_then(|v| v.as_n().ok().map(|n| n.parse::<f64>().ok()))
                .flatten();
            let b_sort = b
                .get(AUTO_FIELDS_SORT)
                .and_then(|v| v.as_n().ok().map(|n| n.parse::<f64>().ok()))
                .flatten();
            match (a_sort, b_sort) {
                (Some(a), Some(b)) => a.partial_cmp(&b).unwrap(),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                _ => std::cmp::Ordering::Equal,
            }
        });
        Ok(items.to_vec())
    }

    pub async fn get_item<T: DynamoObject>(
        &self,
        id: PkSk,
    ) -> Result<Option<T>, GenericServerError> {
        let dbg_cxt: &'static str = "get_item";
        _validate_id::<T>(dbg_cxt, &id)?;
        let key = collection! {
            "pk".to_string() => AttributeValue::S(id.pk),
            "sk".to_string() => AttributeValue::S(id.sk),
        };
        let response = self
            .backend
            .get_item(self.table.clone(), key)
            .await
            .map_err(|e| DynamoConnectionError::with_debug(dbg_cxt, "", format!("{:#?}", e)))?;
        response
            .item
            .map(|item| parse_dynamo_map::<T>(&item))
            .transpose()
    }

    pub async fn create_item<T: DynamoObject>(
        &self,
        parent_id: PkSk,
        data: T::Data,
        custom_sort: Option<f64>,
    ) -> Result<T, GenericServerError> {
        let dbg_cxt: &'static str = "create_item";
        let (new_pk, new_sk) = generate_pk_sk::<T>(&data, &parent_id.pk, &parent_id.sk)?;
        let map = build_dynamo_map_for_new_obj::<T>(
            &data,
            new_pk.clone(),
            new_sk.clone(),
            Some(vec![
                (AUTO_FIELDS_CREATED_AT, Box::new(Timestamp::now())),
                (AUTO_FIELDS_UPDATED_AT, Box::new(Timestamp::now())),
                (AUTO_FIELDS_SORT, Box::new(custom_sort)),
            ]),
        )?;
        self.backend
            .put_item(self.table.clone(), map)
            .await
            .map_err(|e| DynamoConnectionError::with_debug(dbg_cxt, "", format!("{:#?}", e)))?;
        Ok(T::new(
            PkSk {
                pk: new_pk,
                sk: new_sk,
            },
            data,
        ))
    }

    pub async fn batch_create_item<T: DynamoObject>(
        &self,
        parent_id: PkSk,
        data_and_custom_sorts: Vec<(T::Data, Option<f64>)>,
    ) -> Result<Vec<T>, GenericServerError> {
        let dbg_cxt: &'static str = "batch_create_item";
        if matches!(T::id_logic(), IdLogic::Timestamp) {
            return Err(DynamoInvalidOperation::new(
                dbg_cxt,
                "batch_create_item is not allowed with timestamp-based IDs, since all items would get the same ID and only one item would be written.",
            ));
        }
        if data_and_custom_sorts.is_empty() {
            return Ok(Vec::new());
        }
        let (items, ids): (Vec<DynamoMap>, Vec<PkSk>) = data_and_custom_sorts
            .iter()
            .map(|(data, custom_sort)| {
                let (new_pk, new_sk) = generate_pk_sk::<T>(data, &parent_id.pk, &parent_id.sk)?;
                Ok((
                    build_dynamo_map_for_new_obj::<T>(
                        data,
                        new_pk.clone(),
                        new_sk.clone(),
                        Some(vec![
                            (AUTO_FIELDS_CREATED_AT, Box::new(Timestamp::now())),
                            (AUTO_FIELDS_UPDATED_AT, Box::new(Timestamp::now())),
                            (AUTO_FIELDS_SORT, Box::new(custom_sort.clone())),
                        ]),
                    )?,
                    PkSk {
                        pk: new_pk,
                        sk: new_sk,
                    },
                ))
            })
            .collect::<Result<Vec<(DynamoMap, PkSk)>, GenericServerError>>()?
            .into_iter()
            .unzip();
        // Split into 25-item chunks (max supported by DynamoDB).
        for chunk in items.chunks(25) {
            self.backend
                .batch_put_item(self.table.clone(), chunk.to_vec())
                .await
                .map_err(|e| DynamoConnectionError::with_debug(dbg_cxt, "", format!("{:#?}", e)))?;
        }
        Ok(ids
            .into_iter()
            .zip(data_and_custom_sorts.into_iter())
            .map(|(id, (data, _))| T::new(id, data))
            .collect())
    }

    // Use when complex ordering is required
    // (for simple ordering, consider using timestamp-based IDs).
    //
    // Complex ordering works based on a 'sort' field, a floating point value
    // such that a new item can always always be place in between any two
    // existing items. Query functions in this util take this 'sort' field into
    // account, always sorting the query results by it before returning the
    // data.
    //
    // If this ordered insertion is used together with regular insertion, some
    // items will have a 'sort' value while others will not. In this case, the
    // ordered items will be placed before unordered items in query results.
    //
    // WARNING: This function requires checking all existing sort values to
    // place the new item appropriately, which can be expensive. If inserting
    // many ordered items, use batch_create_item_ordered (which only fetches
    // sort values once), or consider using timestamp-based IDs instead.
    pub async fn create_item_ordered<T: DynamoObject>(
        &self,
        parent_id: PkSk,
        data: T::Data,
        insert_position: DynamoInsertPosition,
    ) -> Result<T, GenericServerError> {
        let dbg_cxt: &'static str = "create_item_ordered";
        let sort_val =
            calculate_sort_values::<T, _>(self, parent_id.clone(), &data, insert_position, 1)
                .await?
                .pop()
                .ok_or(DynamoInvalidOperation::new(
                    dbg_cxt,
                    "failed to generate new ordered ID",
                ))?;
        self.create_item::<T>(parent_id, data, Some(sort_val)).await
    }

    pub async fn batch_create_item_ordered<T: DynamoObject>(
        &self,
        parent_id: PkSk,
        data: Vec<T::Data>,
        insert_position: DynamoInsertPosition,
    ) -> Result<Vec<T>, GenericServerError> {
        if data.is_empty() {
            return Ok(Vec::new());
        }
        let new_ids = calculate_sort_values::<T, _>(
            self,
            parent_id.clone(),
            data.first().unwrap(),
            insert_position,
            data.len(),
        )
        .await?;
        self.batch_create_item::<T>(
            parent_id,
            data.into_iter()
                .zip(new_ids.into_iter())
                .map(|(d, sort_val)| (d, Some(sort_val)))
                .collect(),
        )
        .await
    }

    // Updates fields of an existing item. Since this logic internally uses
    // update_item instead of put_item, unrecognized fields unaffected. If the
    // item does not exist, an error is returned.
    pub async fn update_item<T: DynamoObject>(&self, object: &T) -> Result<(), GenericServerError> {
        let dbg_cxt: &'static str = "update_item";
        _validate_id::<T>(dbg_cxt, object.id())?;
        let key = collection! {
            "pk".to_string() => AttributeValue::S(object.pk().to_string()),
            "sk".to_string() => AttributeValue::S(object.sk().to_string()),
        };
        let map = build_dynamo_map_for_existing_obj::<T>(
            &object,
            IdKeys::None,
            Some(vec![(AUTO_FIELDS_UPDATED_AT, Box::new(Timestamp::now()))]),
        )?;
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
        self.backend
            .update_item(
                self.table.clone(),
                key,
                update_expression,
                expression_attribute_values,
                expression_attribute_names,
                // Ensure item exists:
                Some("attribute_exists(pk)".to_string()),
            )
            .await
            .map_err(|e| match e.into_service_error() {
                UpdateItemError::ResourceNotFoundException(_) => DynamoNotFound::default(),
                other => DynamoConnectionError::with_debug(dbg_cxt, "", format!("{:#?}", other)),
            })?;
        Ok(())
    }

    pub async fn delete_item<T: DynamoObject>(&self, id: PkSk) -> Result<(), GenericServerError> {
        let dbg_cxt: &'static str = "delete_item";
        _validate_id::<T>(dbg_cxt, &id)?;
        let key = collection! {
            "pk".to_string() => AttributeValue::S(id.pk),
            "sk".to_string() => AttributeValue::S(id.sk),
        };
        self.backend
            .delete_item(self.table.clone(), key)
            .await
            .map_err(|e| match e.into_service_error() {
                DeleteItemError::ResourceNotFoundException(_) => DynamoNotFound::default(),
                other => DynamoConnectionError::with_debug(dbg_cxt, "", format!("{:#?}", other)),
            })?;
        Ok(())
    }

    pub async fn batch_delete_item<T: DynamoObject>(
        &self,
        keys: Vec<PkSk>,
    ) -> Result<(), GenericServerError> {
        let dbg_cxt: &'static str = "batch_delete_item";
        for key in &keys {
            _validate_id::<T>(dbg_cxt, key)?;
        }
        self.raw_batch_delete_ids(keys).await
    }

    // Performs no checks and directly deletes the given IDs from the database.
    pub async fn raw_batch_delete_ids(&self, keys: Vec<PkSk>) -> Result<(), GenericServerError> {
        let dbg_cxt: &'static str = "raw_batch_delete_ids";
        if keys.is_empty() {
            return Ok(());
        }
        let items = keys
            .into_iter()
            .map(|id| {
                collection! {
                    "pk".to_string() => AttributeValue::S(id.pk),
                    "sk".to_string() => AttributeValue::S(id.sk),
                }
            })
            .collect::<Vec<_>>();
        // Split into 25-item chunks (max supported by DynamoDB).
        for chunk in items.chunks(25) {
            self.backend
                .batch_delete_item(self.table.clone(), chunk.to_vec())
                .await
                .map_err(|e| match e.into_service_error() {
                    BatchWriteItemError::ResourceNotFoundException(_) => DynamoNotFound::default(),
                    other => {
                        DynamoConnectionError::with_debug(dbg_cxt, "", format!("{:#?}", other))
                    }
                })?;
        }
        Ok(())
    }

    // Performs no checks and directly writes the given DynamoMaps to the database.
    // If the item exists, it is updated. If it does not exist, it is created.
    //
    // This does not check or update auto fields (updated_at, sort, etc.). The map values
    // are just directly written.
    //
    // Should only be used internally for efficient low-level DB actions.
    pub async fn raw_batch_put_item(
        &self,
        items: Vec<DynamoMap>,
    ) -> Result<(), GenericServerError> {
        let dbg_cxt: &'static str = "raw_batch_put_item";
        if items.is_empty() {
            return Ok(());
        }
        // Split into 25-item chunks (max supported by DynamoDB).
        for chunk in items.chunks(25) {
            self.backend
                .batch_put_item(self.table.clone(), chunk.to_vec())
                .await
                .map_err(|e| DynamoConnectionError::with_debug(dbg_cxt, "", format!("{:#?}", e)))?;
        }
        Ok(())
    }
}
