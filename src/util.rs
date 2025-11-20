use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use aws_sdk_dynamodb::{
    operation::{
        batch_write_item::BatchWriteItemError, delete_item::DeleteItemError,
        update_item::UpdateItemError,
    },
    types::AttributeValue,
};
use backend::DynamoBackend;
use calculate_sort::calculate_sort_values;
use chrono::{DateTime, Duration, Utc};
use fractic_core::{collection, req_not_none};
use fractic_server_error::{CriticalError, ServerError};
use serde::Serialize;

use crate::{
    errors::{
        DynamoCalloutError, DynamoInvalidBatchOptimizedIdUsage, DynamoInvalidOperation,
        DynamoNotFound,
    },
    schema::{
        id_calculations::{generate_pk_sk, get_object_type, get_pk_sk_from_map},
        parsing::{
            build_dynamo_map_for_existing_obj, build_dynamo_map_for_new_obj,
            build_dynamo_map_internal, parse_dynamo_map, IdKeys,
        },
        DynamoObject, IdLogic, NestingLogic, PkSk, Timestamp,
    },
    util::{
        chunk_helpers::WithMetadataFrom as _,
        id_helpers::{child_search_prefix, validate_id, validate_parent_id},
    },
    DynamoCtxView,
};

pub mod backend;
mod calculate_sort;
mod chunk_helpers;
mod id_helpers;
mod test;

/// Comparison operators for numeric conditions.
#[derive(Debug, Clone, Copy)]
pub enum NumericOp {
    GreaterThan,
    GreaterThanOrEquals,
    LessThan,
    LessThanOrEquals,
}

/// Conditions that can be applied during update operations.
///
/// - Eq: checks equality for all non-null fields present in the provided data.
/// - Compare: applies the numeric operator to all numeric fields present in the provided data.
/// - IsNull: checks that the listed attributes are either not set or explicitly null.
/// - IsSet: checks that the listed attributes exist and are not null.
#[derive(Debug, Clone)]
pub enum UpdateCondition<T: DynamoObject> {
    Eq { data: T::Data },
    Compare { data: T::Data, op: NumericOp },
    IsNull { fields: Vec<String> },
    IsSet { fields: Vec<String> },
}

pub type DynamoMap = HashMap<String, AttributeValue>;
pub const AUTO_FIELDS_CREATED_AT: &str = "created_at";
pub const AUTO_FIELDS_UPDATED_AT: &str = "updated_at";
pub const AUTO_FIELDS_SORT: &str = "sort";
pub const AUTO_FIELDS_TTL: &str = "ttl";

/// In case of chunked items, the '..' key is used to store the children (which
/// should be flattened before returning to the caller).
pub const FLATTEN_RESERVED_KEY: &str = ".."; // WARNING: Also hardcoded
                                             // in batch_replace_all_ordered.

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

#[derive(Clone)]
pub struct DynamoUtil {
    pub backend: Arc<dyn DynamoBackend>,
    pub table: String,
}
impl DynamoUtil {
    const ITEM_EXISTS_CONDITION: &'static str = "attribute_exists(pk)";
    const ITEM_DOES_NOT_EXIST_CONDITION: &'static str = "attribute_not_exists(pk)";

    pub async fn new(
        ctx: &dyn DynamoCtxView,
        table: impl Into<String>,
    ) -> Result<Self, ServerError> {
        Ok(Self {
            backend: ctx.dynamo_backend().await?,
            table: table.into(),
        })
    }

    pub async fn query<T: DynamoObject>(
        &self,
        index: Option<IndexConfig>,
        id: PkSk,
        match_type: DynamoQueryMatchType,
    ) -> Result<Vec<T>, ServerError> {
        self.query_generic(index, id, match_type)
            .await?
            .into_iter()
            .filter_map(|item| {
                let (pk, sk) =
                    get_pk_sk_from_map(&item).expect("query result item did not have pk/sk.");
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
            .collect::<Result<Vec<T>, ServerError>>()
    }

    /// Efficiently queries all children of type `T` belonging to the given
    /// `parent_id`. Handles all nesting logic types.
    pub async fn query_all<T: DynamoObject>(&self, parent_id: PkSk) -> Result<Vec<T>, ServerError> {
        validate_parent_id::<T>(&parent_id)?;
        self.query::<T>(
            None,
            child_search_prefix::<T>(parent_id),
            DynamoQueryMatchType::BeginsWith,
        )
        .await
    }

    pub async fn query_generic(
        &self,
        index: Option<IndexConfig>,
        id: PkSk,
        match_type: DynamoQueryMatchType,
    ) -> Result<Vec<DynamoMap>, ServerError> {
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
                                    "sort field filter did not contain the delimiter char, so \
                                     could not extract the prefix for matching",
                                    &id.sk,
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
                                    "sort field filter did not contain the delimiter char, so \
                                     could not extract the prefix for matching",
                                    &id.sk,
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
            .map_err(|e| DynamoCalloutError::with_debug(&e))?;

        // Expand chunked items.
        let mut items: Vec<DynamoMap> = response
            .into_iter()
            .flat_map(|page| page.items.unwrap_or_default().into_iter())
            .flat_map(|mut item| match item.remove(FLATTEN_RESERVED_KEY) {
                Some(AttributeValue::L(children)) => children
                    .into_iter()
                    .filter_map(|child| {
                        if let AttributeValue::M(inner_map) = child {
                            Some(inner_map.with_metadata_from(&item))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>(),
                _ => vec![item],
            })
            .collect();

        // Sort by sort key. Since 'sort_by' is stable, ID-based ordering is
        // preserved for items without a sort key.
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
        Ok(items)
    }

    pub async fn get_item<T: DynamoObject>(&self, id: PkSk) -> Result<Option<T>, ServerError> {
        if matches!(T::id_logic(), IdLogic::BatchOptimized { .. }) {
            return Err(DynamoInvalidBatchOptimizedIdUsage::new());
        }
        validate_id::<T>(&id)?;
        let key = collection! {
            "pk".to_string() => AttributeValue::S(id.pk),
            "sk".to_string() => AttributeValue::S(id.sk),
        };
        let response = self
            .backend
            .get_item(self.table.clone(), key, None)
            .await
            .map_err(|e| DynamoCalloutError::with_debug(&e))?;
        response
            .item
            .map(|item| parse_dynamo_map::<T>(&item))
            .transpose()
    }

    /// Efficiently checks if an item exists, without fetching item data.
    pub async fn item_exists(&self, id: PkSk) -> Result<bool, ServerError> {
        let key = collection! {
            "pk".to_string() => AttributeValue::S(id.pk),
            "sk".to_string() => AttributeValue::S(id.sk),
        };
        let response = self
            .backend
            .get_item(self.table.clone(), key, Some("pk".to_string()))
            .await
            .map_err(|e| DynamoCalloutError::with_debug(&e))?;
        Ok(response.item.is_some())
    }

    pub async fn create_item<T: DynamoObject>(
        &self,
        parent_id: PkSk,
        data: T::Data,
        options: Option<CreateOptions>,
    ) -> Result<T, ServerError> {
        if matches!(T::id_logic(), IdLogic::BatchOptimized { .. }) {
            return Err(DynamoInvalidBatchOptimizedIdUsage::new());
        }
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
            .map_err(|e| DynamoCalloutError::with_debug(&e))?;
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
    ) -> Result<Vec<T>, ServerError> {
        if matches!(T::id_logic(), IdLogic::BatchOptimized { .. }) {
            return Err(DynamoInvalidBatchOptimizedIdUsage::new());
        }
        if matches!(T::id_logic(), IdLogic::Timestamp) {
            return Err(DynamoInvalidOperation::new(
                "batch_create_item is not allowed with timestamp-based IDs, since all items would \
                 get the same ID and only one item would be written",
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
            .collect::<Result<Vec<(DynamoMap, PkSk)>, ServerError>>()?
            .into_iter()
            .unzip();
        // Split into 25-item chunks (max supported by DynamoDB).
        for chunk in items.chunks(25) {
            self.backend
                .batch_put_item(self.table.clone(), chunk.to_vec())
                .await
                .map_err(|e| DynamoCalloutError::with_debug(&e))?;
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
    ) -> Result<T, ServerError> {
        if matches!(T::id_logic(), IdLogic::BatchOptimized { .. }) {
            return Err(DynamoInvalidBatchOptimizedIdUsage::new());
        }
        let sort_val =
            calculate_sort_values::<T>(self, parent_id.clone(), &data, insert_position, 1)
                .await?
                .pop()
                .ok_or(DynamoInvalidOperation::new(
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
    ) -> Result<Vec<T>, ServerError> {
        if matches!(T::id_logic(), IdLogic::BatchOptimized { .. }) {
            return Err(DynamoInvalidBatchOptimizedIdUsage::new());
        }
        if data.is_empty() {
            return Ok(Vec::new());
        }
        let new_ids = calculate_sort_values::<T>(
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
    pub async fn update_item<T: DynamoObject>(&self, object: &T) -> Result<(), ServerError> {
        if matches!(T::id_logic(), IdLogic::BatchOptimized { .. }) {
            return Err(DynamoInvalidBatchOptimizedIdUsage::new());
        }
        self.update_item_internal(
            object,
            HashMap::default(),
            vec![Self::ITEM_EXISTS_CONDITION.to_string()],
            HashMap::new(),
            HashMap::new(),
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
        op: impl FnOnce(Option<T::Data>) -> Result<T::Data, ServerError>,
    ) -> Result<T, ServerError> {
        if matches!(T::id_logic(), IdLogic::BatchOptimized { .. }) {
            return Err(DynamoInvalidBatchOptimizedIdUsage::new());
        }
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
        self.update_item_internal::<T>(
            &object_after,
            map_before,
            vec![existance_condition],
            HashMap::new(),
            HashMap::new(),
        )
        .await?;
        Ok(object_after)
    }

    /// Updates fields of an existing item with additional, flexible conditions.
    ///
    /// In addition to equality checks using a partially-filled `T::Data`, this supports:
    /// - Numeric comparisons (>, >=, <, <=) for any numeric fields set in `data`
    /// - Attribute presence checks (null/non-set vs set/non-null)
    ///
    /// This automatically adds an existence check for the item being updated.
    /// Fields with null values in `object` are removed (same as `update_item`).
    pub async fn update_item_with_conditions<T: DynamoObject>(
        &self,
        object: &T,
        conditions: Vec<UpdateCondition<T>>,
    ) -> Result<(), ServerError> {
        if matches!(T::id_logic(), IdLogic::BatchOptimized { .. }) {
            return Err(DynamoInvalidBatchOptimizedIdUsage::new());
        }
        // Equality conditions: attribute -> value
        let mut eq_attribute_conditions: HashMap<String, AttributeValue> = HashMap::new();
        // Custom condition expressions requiring additional placeholders
        let mut custom_conditions: Vec<String> = vec![Self::ITEM_EXISTS_CONDITION.to_string()];
        // Additional placeholders for names/values used by custom conditions
        let mut extra_expression_attribute_names: HashMap<String, String> = HashMap::new();
        let mut extra_expression_attribute_values: HashMap<String, AttributeValue> = HashMap::new();

        for cond in conditions {
            match cond {
                UpdateCondition::Eq { data } => {
                    // Convert partial data into a map; nulls are skipped by serializer.
                    let (data_map, _skipped_nulls) =
                        build_dynamo_map_internal(&data, None, None, None)?;
                    for (k, v) in data_map {
                        // Avoid id/auto fields (shouldn't be present for Data anyway)
                        if (k == "pk") || (k == "sk") {
                            continue;
                        }
                        eq_attribute_conditions.insert(k, v);
                    }
                }
                UpdateCondition::Compare { data, op } => {
                    let (data_map, _skipped_nulls) =
                        build_dynamo_map_internal(&data, None, None, None)?;
                    for (k, v) in data_map {
                        if (k == "pk") || (k == "sk") {
                            continue;
                        }
                        // Ensure numeric value
                        let AttributeValue::N(_) = v else {
                            return Err(DynamoInvalidOperation::new(&format!(
                                "non-numeric value provided for numeric comparison on '{}'",
                                k
                            )));
                        };
                        let suffix = k
                            .chars()
                            .map(|c| if c.is_alphanumeric() { c } else { '_' })
                            .collect::<String>();
                        let name_placeholder = format!("#cond_{}", suffix);
                        let value_placeholder = format!(":cv_{}", suffix);
                        extra_expression_attribute_names
                            .insert(name_placeholder.clone(), k.clone());
                        // Last-wins if duplicate fields in multiple conditions; acceptable
                        extra_expression_attribute_values.insert(value_placeholder.clone(), v);
                        let op_str = match op {
                            NumericOp::GreaterThan => ">",
                            NumericOp::GreaterThanOrEquals => ">=",
                            NumericOp::LessThan => "<",
                            NumericOp::LessThanOrEquals => "<=",
                        };
                        custom_conditions.push(format!(
                            "{} {} {}",
                            name_placeholder, op_str, value_placeholder
                        ));
                    }
                }
                UpdateCondition::IsNull { fields } => {
                    for f in fields {
                        let suffix = f
                            .chars()
                            .map(|c| if c.is_alphanumeric() { c } else { '_' })
                            .collect::<String>();
                        let name_placeholder = format!("#cond_{}", suffix);
                        extra_expression_attribute_names
                            .insert(name_placeholder.clone(), f.to_string());
                        let expr = format!(
                            "(attribute_not_exists({name}) OR attribute_type({name}, 'NULL'))",
                            name = name_placeholder
                        );
                        custom_conditions.push(expr);
                    }
                }
                UpdateCondition::IsSet { fields } => {
                    for f in fields {
                        let suffix = f
                            .chars()
                            .map(|c| if c.is_alphanumeric() { c } else { '_' })
                            .collect::<String>();
                        let name_placeholder = format!("#cond_{}", suffix);
                        extra_expression_attribute_names
                            .insert(name_placeholder.clone(), f.to_string());
                        let expr = format!(
                            "(attribute_exists({name}) AND NOT attribute_type({name}, 'NULL'))",
                            name = name_placeholder
                        );
                        custom_conditions.push(expr);
                    }
                }
            }
        }

        self.update_item_internal::<T>(
            object,
            eq_attribute_conditions,
            custom_conditions,
            extra_expression_attribute_names,
            extra_expression_attribute_values,
        )
        .await
    }

    /// Lower-level update that allows injecting additional expression attribute
    /// names/values for custom condition placeholders.
    async fn update_item_internal<T: DynamoObject>(
        &self,
        object: &T,
        attribute_conditions: HashMap<String, AttributeValue>,
        custom_conditions: Vec<String>,
        extra_expression_attribute_names: HashMap<String, String>,
        extra_expression_attribute_values: HashMap<String, AttributeValue>,
    ) -> Result<(), ServerError> {
        validate_id::<T>(object.id())?;
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
        let mut expression_attribute_names = extra_expression_attribute_names;
        let mut expression_attribute_values = extra_expression_attribute_values;
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

        // Custom conditions + attribute equality conditions:
        let mut all_conditions: Vec<String> = Vec::new();
        // Start with any provided custom conditions
        all_conditions.extend(custom_conditions.into_iter());
        // Append equality conditions, using keyed placeholders to avoid ambiguity
        for (key, value) in attribute_conditions.into_iter() {
            let suffix = key
                .chars()
                .map(|c| if c.is_alphanumeric() { c } else { '_' })
                .collect::<String>();
            let key_placeholder = format!("#c_{}", suffix);
            let value_placeholder = format!(":cv_{}", suffix);
            expression_attribute_names.insert(key_placeholder.clone(), key);
            expression_attribute_values.insert(value_placeholder.clone(), value);
            all_conditions.push(format!("{} = {}", key_placeholder, value_placeholder));
        }
        let condition_expression = all_conditions.join(" AND ");

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
                UpdateItemError::ResourceNotFoundException(_) => DynamoNotFound::new(),
                other => DynamoCalloutError::with_debug(&other),
            })?;
        Ok(())
    }

    pub async fn delete_item<T: DynamoObject>(&self, id: PkSk) -> Result<(), ServerError> {
        if matches!(T::id_logic(), IdLogic::BatchOptimized { .. }) {
            return Err(DynamoInvalidBatchOptimizedIdUsage::new());
        }
        validate_id::<T>(&id)?;
        let key = collection! {
            "pk".to_string() => AttributeValue::S(id.pk),
            "sk".to_string() => AttributeValue::S(id.sk),
        };
        self.backend
            .delete_item(self.table.clone(), key)
            .await
            .map_err(|e| match e.into_service_error() {
                DeleteItemError::ResourceNotFoundException(_) => DynamoNotFound::new(),
                other => DynamoCalloutError::with_debug(&other),
            })?;
        Ok(())
    }

    pub async fn batch_delete_item<T: DynamoObject>(
        &self,
        keys: Vec<PkSk>,
    ) -> Result<(), ServerError> {
        if matches!(T::id_logic(), IdLogic::BatchOptimized { .. }) {
            return Err(DynamoInvalidBatchOptimizedIdUsage::new());
        }
        for key in &keys {
            validate_id::<T>(key)?;
        }
        self.raw_batch_delete_ids(keys).await
    }

    /// Deletes *all* children of type T on the parent.
    pub async fn batch_delete_all<T: DynamoObject>(
        &self,
        parent_id: PkSk,
    ) -> Result<(), ServerError> {
        // Ensure we dedup IDs, since query logic expands chunks internally.
        let children_ids: HashSet<PkSk> = self
            .query_all::<T>(parent_id)
            .await?
            .into_iter()
            .map(|c| c.id().clone())
            .collect::<HashSet<_>>();
        self.raw_batch_delete_ids(children_ids.into_iter().collect())
            .await
    }

    /// Replaces *all* children of type T on the parent.
    ///
    /// For BatchOptimized item types, this function internally stores the data
    /// in large chunks (which get flattened by query logic).
    pub async fn batch_replace_all_ordered<T: DynamoObject>(
        &self,
        parent_id: PkSk,
        data: Vec<T::Data>,
    ) -> Result<(), ServerError> {
        // Validations.
        validate_parent_id::<T>(&parent_id)?;
        let chunk_size = match T::id_logic() {
            IdLogic::BatchOptimized { chunk_size } => {
                if chunk_size == 0 {
                    return Err(DynamoInvalidOperation::new(
                        "invalid IdLogic::BatchOptimized usage; chunk_size must be greater than 0",
                    ));
                }
                Some(chunk_size)
            }
            _ => None,
        };

        self.batch_delete_all::<T>(parent_id.clone()).await?;
        if data.is_empty() {
            return Ok(());
        }

        // For ordinary ID logic, simply create the new items in order.
        // -------------------------------------------------------------------
        if !matches!(T::id_logic(), IdLogic::BatchOptimized { .. }) {
            self.batch_create_item_ordered::<T>(parent_id, data, DynamoInsertPosition::Last)
                .await?;
            return Ok(());
        }

        // For batch-optimized ID logic, batch into flattenable chunks.
        // -------------------------------------------------------------------
        req_not_none!(chunk_size, CriticalError);
        let num_chunks = (data.len() + chunk_size - 1) / chunk_size; // rounds up for partial chunks

        // When building the UUID component of IDs for flattenable items, use
        // the chunk index (1, 2, 3, ...), padded with 0s to the minimum length
        // to represent all chunks (ex. if 456 chunks: 001, 002 ... 455).
        // Padding is important since Dynamo sorts lexicographically.
        let index_digits = match num_chunks {
            0 => unreachable!("data.is_empty() checked above"),
            1 => 0, // Special case: replaced with '-' later.
            n => (n - 1).ilog10() as usize + 1,
        };

        // Build 'flattenable' items, which wrap the data in chunks. These get
        // automatically unwrapped by query_generic.
        #[derive(Serialize)]
        struct Flattenable<T: DynamoObject> {
            #[serde(rename = "..")]
            l: Vec<T::Data>,
        }
        let maps: Vec<DynamoMap> = data
            .chunks(chunk_size)
            .enumerate()
            .map(|(i, chunk)| {
                let new_obj_id = if index_digits == 0 {
                    // If there's only a single chunk, indicate with '-' instead
                    // of index, to make it more human-readable (i.e. make it
                    // clear there's only a single chunk).
                    format!("{}#-", T::id_label())
                } else {
                    format!("{}#{}", T::id_label(), format!("{:0index_digits$}", i))
                };
                let (pk, sk) = match T::nesting_logic() {
                    NestingLogic::Root => ("ROOT".to_string(), new_obj_id),
                    NestingLogic::TopLevelChildOf(_) | NestingLogic::TopLevelChildOfAny => {
                        (parent_id.sk.clone(), new_obj_id)
                    }
                    NestingLogic::InlineChildOf(_) | NestingLogic::InlineChildOfAny => (
                        parent_id.pk.clone(),
                        format!("{}#{}", parent_id.sk.clone(), new_obj_id),
                    ),
                };

                let flattenable = Flattenable::<T> { l: chunk.to_vec() };
                build_dynamo_map_internal::<Flattenable<T>>(
                    &flattenable,
                    Some(pk),
                    Some(sk),
                    Some(vec![
                        (AUTO_FIELDS_CREATED_AT, Box::new(Timestamp::now())),
                        (AUTO_FIELDS_UPDATED_AT, Box::new(Timestamp::now())),
                        (AUTO_FIELDS_SORT, Box::new(None::<f64>)),
                        (AUTO_FIELDS_TTL, Box::new(None::<i64>)),
                    ]),
                )
                .map(|res| res.0)
            })
            .collect::<Result<_, _>>()?;

        self.raw_batch_put_item(maps).await
    }

    /// Performs a full table scan and returns the raw Dynamo items as-is. No
    /// sorting, chunk expansion, filtering, or other processing.
    ///
    /// In other words, forwarding the items returned from this function
    /// directly into `raw_batch_put_item` would be a no-op.
    pub async fn raw_full_table_scan(&self) -> Result<Vec<DynamoMap>, ServerError> {
        let response = self
            .backend
            .scan(self.table.clone())
            .await
            .map_err(|e| DynamoCalloutError::with_debug(&e))?;
        Ok(response
            .into_iter()
            .flat_map(|page| page.items.unwrap_or_default().into_iter())
            .collect())
    }

    /// Performs no checks and directly deletes the given IDs from the database.
    pub async fn raw_batch_delete_ids(&self, keys: Vec<PkSk>) -> Result<(), ServerError> {
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
                    BatchWriteItemError::ResourceNotFoundException(_) => DynamoNotFound::new(),
                    other => DynamoCalloutError::with_debug(&other),
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
    pub async fn raw_batch_put_item(&self, items: Vec<DynamoMap>) -> Result<(), ServerError> {
        if items.is_empty() {
            return Ok(());
        }
        // Split into 25-item chunks (max supported by DynamoDB).
        for chunk in items.chunks(25) {
            self.backend
                .batch_put_item(self.table.clone(), chunk.to_vec())
                .await
                .map_err(|e| DynamoCalloutError::with_debug(&e))?;
        }
        Ok(())
    }
}
