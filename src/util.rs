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
use chrono::{DateTime, Duration, Utc};
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
pub const AUTO_FIELDS_TTL: &str = "ttl";

#[derive(Debug, PartialEq)]
pub enum DynamoQueryMatchType {
    BeginsWith,
    Equals,
    GreaterThan,
    GreaterThanOrEquals,
    LessThan,
    LessThanOrEquals,
    SuffixGreaterThanOrEquals(char),
    SuffixLessThanOrEquals(char),
}

#[derive(Debug)]
pub enum DynamoInsertPosition {
    First,
    Last,
    After(PkSk),
}

#[derive(Debug)]
pub enum TtlConfig {
    OneWeek,
    OneMonth,
    OneYear,
    CustomDuration(Duration),
    CustomDate(DateTime<Utc>),
}

#[derive(Debug, Clone, Copy)]
pub struct IndexConfig {
    pub name: &'static str,
    pub partition_field: &'static str,
    pub sort_field: &'static str,
}

#[derive(Debug, Default)]
pub struct CreateOptions {
    pub custom_sort: Option<f64>,
    /// If provided, the given item is automatically deleted by Dynamo after the
    /// expiry time, usually within a day or two.
    ///
    /// IMPORTANT: This requires TTL to be enabled on the table, using attribute
    /// name 'ttl'.
    pub ttl: Option<TtlConfig>,
}

fn validate_id<T: DynamoObject>(
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

impl TtlConfig {
    fn compute_timestamp(&self) -> i64 {
        match self {
            TtlConfig::OneWeek => (Utc::now() + Duration::weeks(1)).timestamp(),
            TtlConfig::OneMonth => (Utc::now() + Duration::days(30)).timestamp(),
            TtlConfig::OneYear => (Utc::now() + Duration::days(365)).timestamp(),
            TtlConfig::CustomDuration(duration) => (Utc::now() + *duration).timestamp(),
            TtlConfig::CustomDate(date) => date.timestamp(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DynamoUtil<B: DynamoBackendImpl> {
    pub backend: B,
    pub table: String,
}
impl<C: DynamoBackendImpl> DynamoUtil<C> {
    const ITEM_EXISTS_CONDITION: &'static str = "attribute_exists(pk)";
    const ITEM_DOES_NOT_EXIST_CONDITION: &'static str = "attribute_not_exists(pk)";

    pub async fn query<T: DynamoObject>(
        &self,
        index: Option<IndexConfig>,
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
        index: Option<IndexConfig>,
        id: PkSk,
        match_type: DynamoQueryMatchType,
    ) -> Result<Vec<DynamoMap>, GenericServerError> {
        let dbg_cxt: &'static str = "query_generic";
        let (index_name, partition_field, sort_field) = match index {
            Some(index) => (
                Some(index.name.to_string()),
                index.partition_field,
                index.sort_field,
            ),
            None => (None, "pk", "sk"),
        };
        let condition = match match_type {
            DynamoQueryMatchType::BeginsWith if id.sk.is_empty() => {
                format!("{} = :pk_val", partition_field)
            }
            DynamoQueryMatchType::BeginsWith => format!(
                "{} = :pk_val AND begins_with({}, :sk_val)",
                partition_field, sort_field
            ),
            DynamoQueryMatchType::Equals => {
                format!("{} = :pk_val AND {} = :sk_val", partition_field, sort_field)
            }
            DynamoQueryMatchType::GreaterThan => {
                format!("{} = :pk_val AND {} > :sk_val", partition_field, sort_field)
            }
            DynamoQueryMatchType::GreaterThanOrEquals => {
                format!(
                    "{} = :pk_val AND {} >= :sk_val",
                    partition_field, sort_field
                )
            }
            DynamoQueryMatchType::LessThan => {
                format!("{} = :pk_val AND {} < :sk_val", partition_field, sort_field)
            }
            DynamoQueryMatchType::LessThanOrEquals => {
                format!(
                    "{} = :pk_val AND {} <= :sk_val",
                    partition_field, sort_field
                )
            }
            DynamoQueryMatchType::SuffixGreaterThanOrEquals(_) => {
                format!(
                    "{} = :pk_val AND {} BETWEEN :sk_val AND :sk_max",
                    partition_field, sort_field
                )
            }
            DynamoQueryMatchType::SuffixLessThanOrEquals(_) => {
                format!(
                    "{} = :pk_val AND {} BETWEEN :sk_min AND :sk_val",
                    partition_field, sort_field
                )
            }
        }
        .to_string();
        let mut attribute_values = HashMap::new();
        attribute_values.insert(":pk_val".to_string(), AttributeValue::S(id.pk));
        match match_type {
            DynamoQueryMatchType::SuffixGreaterThanOrEquals(delim) => {
                // '~' is the last ASCII character, so we can use it as an upper
                // bound to limit the query to a given prefix (similar to using
                // begins_with). Since queries can only have one condition per
                // key, this allows us to effectively do >= and begins_with at
                // the same time, by using a BETWEEN condition.
                attribute_values.insert(
                    ":sk_max".to_string(),
                    AttributeValue::S(format!(
                        "{}~",
                        id.sk
                            .rsplit_once(delim)
                            .ok_or_else(|| {
                                DynamoInvalidOperation::with_debug(
                                    dbg_cxt,
                                    "Sort field filter did not contain the delimiter char, so could not extract the prefix for matching.",
                                    id.sk.clone(),
                                )
                            })?
                            .0
                    )),
                );
            }
            DynamoQueryMatchType::SuffixLessThanOrEquals(delim) => {
                attribute_values.insert(
                    ":sk_min".to_string(),
                    AttributeValue::S(format!(
                        "{}",
                        id.sk
                            .rsplit_once(delim)
                            .ok_or_else(|| {
                                DynamoInvalidOperation::with_debug(
                                    dbg_cxt,
                                    "Sort field filter did not contain the delimiter char, so could not extract the prefix for matching.",
                                    id.sk.clone(),
                                )
                            })?
                            .0
                    )),
                );
            }
            _ => {}
        }
        if !id.sk.is_empty() {
            attribute_values.insert(":sk_val".to_string(), AttributeValue::S(id.sk));
        }
        let response = self
            .backend
            .query(self.table.clone(), index_name, condition, attribute_values)
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
        validate_id::<T>(dbg_cxt, &id)?;
        let key = collection! {
            "pk".to_string() => AttributeValue::S(id.pk),
            "sk".to_string() => AttributeValue::S(id.sk),
        };
        let response = self
            .backend
            .get_item(self.table.clone(), key, None)
            .await
            .map_err(|e| DynamoConnectionError::with_debug(dbg_cxt, "", format!("{:#?}", e)))?;
        response
            .item
            .map(|item| parse_dynamo_map::<T>(&item))
            .transpose()
    }

    /// Efficiently checks if an item exists, without fetching item data.
    pub async fn item_exists(&self, id: PkSk) -> Result<bool, GenericServerError> {
        let dbg_cxt: &'static str = "item_exists";
        let key = collection! {
            "pk".to_string() => AttributeValue::S(id.pk),
            "sk".to_string() => AttributeValue::S(id.sk),
        };
        let response = self
            .backend
            .get_item(self.table.clone(), key, Some("pk".to_string()))
            .await
            .map_err(|e| DynamoConnectionError::with_debug(dbg_cxt, "", format!("{:#?}", e)))?;
        Ok(response.item.is_some())
    }

    pub async fn create_item<T: DynamoObject>(
        &self,
        parent_id: PkSk,
        data: T::Data,
        options: Option<CreateOptions>,
    ) -> Result<T, GenericServerError> {
        let dbg_cxt: &'static str = "create_item";
        let (new_pk, new_sk) = generate_pk_sk::<T>(&data, &parent_id.pk, &parent_id.sk)?;
        let sort: Option<f64> = options.as_ref().and_then(|o| o.custom_sort);
        let ttl: Option<i64> = options
            .as_ref()
            .and_then(|o| o.ttl.as_ref())
            .map(|ttl| ttl.compute_timestamp());
        let map = build_dynamo_map_for_new_obj::<T>(
            &data,
            new_pk.clone(),
            new_sk.clone(),
            Some(vec![
                (AUTO_FIELDS_CREATED_AT, Box::new(Timestamp::now())),
                (AUTO_FIELDS_UPDATED_AT, Box::new(Timestamp::now())),
                (AUTO_FIELDS_SORT, Box::new(sort)),
                (AUTO_FIELDS_TTL, Box::new(ttl)),
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
        data_and_options: Vec<(T::Data, Option<CreateOptions>)>,
    ) -> Result<Vec<T>, GenericServerError> {
        let dbg_cxt: &'static str = "batch_create_item";
        if matches!(T::id_logic(), IdLogic::Timestamp) {
            return Err(DynamoInvalidOperation::new(
                dbg_cxt,
                "batch_create_item is not allowed with timestamp-based IDs, since all items would get the same ID and only one item would be written.",
            ));
        }
        if data_and_options.is_empty() {
            return Ok(Vec::new());
        }
        let (items, ids): (Vec<DynamoMap>, Vec<PkSk>) = data_and_options
            .iter()
            .map(|(data, options)| {
                let (new_pk, new_sk) = generate_pk_sk::<T>(data, &parent_id.pk, &parent_id.sk)?;
                let sort: Option<f64> = options.as_ref().and_then(|o| o.custom_sort);
                let ttl: Option<i64> = options
                    .as_ref()
                    .and_then(|o| o.ttl.as_ref())
                    .map(|ttl| ttl.compute_timestamp());
                Ok((
                    build_dynamo_map_for_new_obj::<T>(
                        data,
                        new_pk.clone(),
                        new_sk.clone(),
                        Some(vec![
                            (AUTO_FIELDS_CREATED_AT, Box::new(Timestamp::now())),
                            (AUTO_FIELDS_UPDATED_AT, Box::new(Timestamp::now())),
                            (AUTO_FIELDS_SORT, Box::new(sort)),
                            (AUTO_FIELDS_TTL, Box::new(ttl)),
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
            .zip(data_and_options.into_iter())
            .map(|(id, (data, _))| T::new(id, data))
            .collect())
    }

    /// Use when complex ordering is required (for simple ordering, consider
    /// using timestamp-based IDs).
    ///
    /// Complex ordering works based on a 'sort' field, a floating point value
    /// such that a new item can always always be place in between any two
    /// existing items. Query functions in this util take this 'sort' field into
    /// account, always sorting the query results by it before returning the
    /// data.
    ///
    /// If this ordered insertion is used together with regular insertion, some
    /// items will have a 'sort' value while others will not. In this case, the
    /// ordered items will be placed before unordered items in query results.
    ///
    /// WARNING: This function requires checking all existing sort values to
    /// place the new item appropriately, which can be expensive. If inserting
    /// many ordered items, use batch_create_item_ordered (which only fetches
    /// sort values once), or consider using timestamp-based IDs instead.
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
        self.create_item::<T>(
            parent_id,
            data,
            Some(CreateOptions {
                custom_sort: Some(sort_val),
                ..Default::default()
            }),
        )
        .await
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
                .map(|(d, sort_val)| {
                    (
                        d,
                        Some(CreateOptions {
                            custom_sort: Some(sort_val),
                            ..Default::default()
                        }),
                    )
                })
                .collect(),
        )
        .await
    }

    /// Updates fields of an existing item. Since this logic internally uses
    /// update_item instead of put_item, unrecognized fields unaffected. If the
    /// item does not exist, an error is returned. Fields with null values are
    /// removed from the item.
    pub async fn update_item<T: DynamoObject>(&self, object: &T) -> Result<(), GenericServerError> {
        self.update_item_with_conditions(
            object,
            HashMap::default(),
            vec![Self::ITEM_EXISTS_CONDITION.to_string()],
        )
        .await
    }

    /// Updates an object in an all-or-nothing transaction. If the object has
    /// changed since it was fetched, the update is aborted and returns an
    /// error. If 'op' returns an error, the transaction is also aborted. If the
    /// object does not exist, the result of 'op' will be created as a new
    /// object, and the transaction condition will ensure another object with
    /// the same ID wasn't created in the meantime.
    pub async fn update_item_transaction<T: DynamoObject>(
        &self,
        id: PkSk,
        op: impl FnOnce(Option<T::Data>) -> Result<T::Data, GenericServerError>,
    ) -> Result<T, GenericServerError> {
        let object_before = self.get_item::<T>(id.clone()).await?;
        let (map_before, existance_condition) = match object_before {
            Some(ref o) => (
                build_dynamo_map_for_existing_obj::<T>(o, IdKeys::None, None)?.0,
                Self::ITEM_EXISTS_CONDITION.to_string(),
            ),
            None => (
                HashMap::default(),
                Self::ITEM_DOES_NOT_EXIST_CONDITION.to_string(),
            ),
        };
        let object_after = T::new(id, op(object_before.map(|o| o.into_data()))?);
        self.update_item_with_conditions::<T>(&object_after, map_before, vec![existance_condition])
            .await?;
        Ok(object_after)
    }

    async fn update_item_with_conditions<T: DynamoObject>(
        &self,
        object: &T,
        attribute_conditions: HashMap<String, AttributeValue>,
        custom_conditions: Vec<String>,
    ) -> Result<(), GenericServerError> {
        let dbg_cxt: &'static str = "update_item_with_conditions";
        validate_id::<T>(dbg_cxt, object.id())?;
        let key = collection! {
            "pk".to_string() => AttributeValue::S(object.pk().to_string()),
            "sk".to_string() => AttributeValue::S(object.sk().to_string()),
        };
        let (map, null_keys) = build_dynamo_map_for_existing_obj::<T>(
            &object,
            IdKeys::None,
            Some(vec![(AUTO_FIELDS_UPDATED_AT, Box::new(Timestamp::now()))]),
        )?;

        // Build update expression:
        let mut expression_attribute_names = HashMap::new();
        let mut expression_attribute_values = HashMap::new();
        let set_expression = match map.is_empty() {
            true => "".to_string(),
            false => {
                "SET ".to_string()
                    + &map
                        .into_iter()
                        .enumerate()
                        .map(|(idx, (key, value))| {
                            let key_placeholder = format!("#k{}", idx + 1);
                            let value_placeholder = format!(":v{}", idx + 1);
                            expression_attribute_names.insert(key_placeholder.clone(), key);
                            expression_attribute_values.insert(value_placeholder.clone(), value);
                            format!("{} = {}", key_placeholder, value_placeholder)
                        })
                        .collect::<Vec<String>>()
                        .join(", ")
            }
        };
        let remove_expression = match null_keys.is_empty() {
            true => "".to_string(),
            false => {
                "REMOVE ".to_string()
                    + &null_keys
                        .into_iter()
                        .enumerate()
                        .map(|(idx, key)| {
                            let key_placeholder = format!("#rmk{}", idx + 1);
                            expression_attribute_names.insert(key_placeholder.clone(), key);
                            key_placeholder
                        })
                        .collect::<Vec<String>>()
                        .join(", ")
            }
        };
        let update_expression = format!("{} {}", set_expression, remove_expression);

        // Ensure item exists, and any additional custom conditions:
        let condition_expression = custom_conditions
            .into_iter()
            .chain(
                attribute_conditions
                    .into_iter()
                    .enumerate()
                    .map(|(idx, (key, value))| {
                        let key_placeholder = format!("#c{}", idx + 1);
                        let value_placeholder = format!(":cv{}", idx + 1);
                        expression_attribute_names.insert(key_placeholder.clone(), key);
                        expression_attribute_values.insert(value_placeholder.clone(), value);
                        format!("{} = {}", key_placeholder, value_placeholder)
                    }),
            )
            .collect::<Vec<String>>()
            .join(" AND ");

        self.backend
            .update_item(
                self.table.clone(),
                key,
                update_expression,
                expression_attribute_values,
                expression_attribute_names,
                Some(condition_expression),
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
        validate_id::<T>(dbg_cxt, &id)?;
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
            validate_id::<T>(dbg_cxt, key)?;
        }
        self.raw_batch_delete_ids(keys).await
    }

    /// Performs no checks and directly deletes the given IDs from the database.
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

    /// Performs no checks and directly writes the given DynamoMaps to the
    /// database. If the item exists, it is updated. If it does not exist, it is
    /// created.
    ///
    /// This does not check or update auto fields (updated_at, sort, etc.). The
    /// map values are just directly written.
    ///
    /// Should only be used internally for efficient low-level DB actions.
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
