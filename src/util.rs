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

use crate::{
    errors::{
        DynamoCalloutError, DynamoInvalidBatchOptimizedIdUsage, DynamoInvalidExtIdUsage,
        DynamoInvalidOperation, DynamoNotFound, DynamoUnexpectedItemCount,
    },
    schema::{
        id_calculations::{generate_pk_sk, get_object_type, get_pk_sk_from_map},
        parsing::{
            build_dynamo_map_for_existing_obj, build_dynamo_map_for_new_obj,
            build_dynamo_map_internal, parse_dynamo_map, IdKeys,
        },
        DynamoObject, IdLogic, PkSk, Timestamp,
    },
    util::{
        collapse_helpers::{
            build_partition_write_plan, collapse_partitioned_items, expand_partition_delete_ids,
            ext_base_id, is_partitioned_id_logic,
        },
        expand_helpers::{build_expandable_batch_maps, expand_query_items},
        id_helpers::{child_search_prefix, validate_id, validate_parent_id},
        update_helpers::CmpOp,
    },
    DynamoCtxView,
};

pub mod backend;
mod calculate_sort;
mod collapse_helpers;
mod expand_helpers;
mod id_helpers;
mod metadata_helpers;
mod test;
mod update_helpers;

pub type DynamoMap = HashMap<String, AttributeValue>;
pub const AUTO_FIELDS_CREATED_AT: &str = "created_at";
pub const AUTO_FIELDS_UPDATED_AT: &str = "updated_at";
pub const AUTO_FIELDS_SORT: &str = "sort";
pub const AUTO_FIELDS_TTL: &str = "ttl";

/// In case of batch items, the '..' key is used to store the batch items array,
/// which should be expanded before returning to the caller.
pub const EXPAND_RESERVED_KEY: &str = ".."; // WARNING: Also hardcoded
                                            // in `expand_helpers.rs`.

/// In case of partition items, the '##' key is used to store the object
/// partition string, which should be collapsed with other partitions before
/// returning to the caller.
pub const COLLAPSE_RESERVED_KEY: &str = "##"; // WARNING: Also hardcoded
                                              // in `collapse_helpers.rs`.

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

#[derive(Debug)]
pub struct CreateToken<T: DynamoObject> {
    // NOTE: It's important that this struct does not implement Clone, provides
    // no constructor, and has private fields. This way tokens can't be used
    // more than once, since they can't be duplicated and must be moved when
    // building `CreateOptions`.
    id: PkSk,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: DynamoObject> CreateToken<T> {
    pub fn id(&self) -> &PkSk {
        &self.id
    }
}

#[derive(Debug)]
pub struct CreateOptions<T: DynamoObject> {
    pub custom_sort: Option<f64>,
    /// If provided, the given item is automatically deleted by Dynamo after the
    /// expiry time, usually within a day or two.
    ///
    /// IMPORTANT: This requires TTL to be enabled on the table, using attribute
    /// name 'ttl'.
    pub ttl: Option<TtlConfig>,
    pub token: Option<CreateToken<T>>,
}

impl<T: DynamoObject> Default for CreateOptions<T> {
    fn default() -> Self {
        Self {
            custom_sort: None,
            ttl: None,
            token: None,
        }
    }
}

/// Comparison operators for numeric conditions.
#[derive(Debug, Clone, Copy)]
pub enum NumericOp {
    GreaterThan,
    GreaterThanOrEquals,
    LessThan,
    LessThanOrEquals,
}

/// Conditions that can be applied during update operations.
#[derive(Debug, Clone)]
pub enum UpdateCondition<T: DynamoObject> {
    /// Checks equality for all fields set to non-null values.
    PartialEq(T::Data),
    /// Applies the numeric operator to all fields set to non-null values. Only
    /// numerically comparable fields must be set.
    NumericCompare { partial: T::Data, map_op: NumericOp },
    /// Checks that the field is either not set or explicitly null. Supports
    /// nested fields (ex. "details.address.city").
    FieldIsNone(String),
    /// Checks that the field exists and is not null. Supports nested fields
    /// (ex. "details.address.city").
    FieldIsSome(String),
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
    pub async fn query_all<T: DynamoObject>(
        &self,
        parent_id: &PkSk,
    ) -> Result<Vec<T>, ServerError> {
        validate_parent_id::<T>(parent_id)?;
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

        let mut items = expand_query_items(
            response
                .into_iter()
                .flat_map(|page| page.items.unwrap_or_default().into_iter())
                .collect(),
        );
        items = collapse_partitioned_items(items)?;

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
        if is_partitioned_id_logic::<T>() {
            let items = self
                .query::<T>(None, ext_base_id(&id), DynamoQueryMatchType::BeginsWith)
                .await?;
            return match items.len() {
                0 => Ok(None),
                1 => Ok(items.into_iter().next()),
                n => Err(DynamoUnexpectedItemCount::new(&format!(
                    "expected at most one logical item for '{}', got {}",
                    id, n
                ))),
            };
        }
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

    pub fn create_token<T: DynamoObject>(
        &self,
        parent_id: &PkSk,
        data: &T::Data,
    ) -> Result<CreateToken<T>, ServerError> {
        let (pk, sk) = generate_pk_sk::<T>(data, &parent_id.pk, &parent_id.sk)?;
        Ok(CreateToken {
            id: PkSk { pk, sk },
            _phantom: std::marker::PhantomData,
        })
    }

    pub async fn create_item<T: DynamoObject>(
        &self,
        parent_id: &PkSk,
        data: T::Data,
    ) -> Result<T, ServerError> {
        self.create_item_opt(parent_id, data, CreateOptions::default())
            .await
    }

    pub async fn create_item_opt<T: DynamoObject>(
        &self,
        parent_id: &PkSk,
        data: T::Data,
        options: CreateOptions<T>,
    ) -> Result<T, ServerError> {
        if matches!(T::id_logic(), IdLogic::BatchOptimized { .. }) {
            return Err(DynamoInvalidBatchOptimizedIdUsage::new());
        }
        let PkSk { pk, sk } = options
            .token
            .map_or_else(|| self.create_token(parent_id, &data), |t| Ok(t))?
            .id;
        let sort: Option<f64> = options.custom_sort;
        let ttl: Option<i64> = options.ttl.map(|ttl| ttl.compute_timestamp());
        if is_partitioned_id_logic::<T>() {
            let logical_id = ext_base_id(&PkSk { pk, sk });
            let plan = build_partition_write_plan::<T>(self, &logical_id, &data, sort, ttl).await?;
            self.raw_batch_delete_ids(plan.stale_delete_ids).await?;
            self.raw_batch_put_item(plan.put_items).await?;
            return Ok(T::new(logical_id, data));
        }
        let now = Timestamp::now();
        let map = build_dynamo_map_for_new_obj::<T>(
            &data,
            pk.clone(),
            sk.clone(),
            Some(vec![
                (AUTO_FIELDS_CREATED_AT, Box::new(now.clone())),
                (AUTO_FIELDS_UPDATED_AT, Box::new(now)),
                (AUTO_FIELDS_SORT, Box::new(sort)),
                (AUTO_FIELDS_TTL, Box::new(ttl)),
            ]),
        )?;
        self.backend
            .put_item(self.table.clone(), map)
            .await
            .map_err(|e| DynamoCalloutError::with_debug(&e))?;
        Ok(T::new(PkSk { pk, sk }, data))
    }

    pub async fn batch_create_item<T: DynamoObject>(
        &self,
        parent_id: &PkSk,
        data: Vec<T::Data>,
    ) -> Result<Vec<T>, ServerError> {
        self.batch_create_item_opt(
            parent_id,
            data.into_iter()
                .map(|d| (d, CreateOptions::default()))
                .collect(),
        )
        .await
    }

    pub async fn batch_create_item_opt<T: DynamoObject>(
        &self,
        parent_id: &PkSk,
        data_and_options: Vec<(T::Data, CreateOptions<T>)>,
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
        if is_partitioned_id_logic::<T>() {
            let mut items = Vec::new();
            let mut stale_delete_ids = Vec::new();
            let mut created = Vec::new();
            for (data, options) in data_and_options.into_iter() {
                let PkSk { pk, sk } = options
                    .token
                    .map_or_else(|| self.create_token::<T>(parent_id, &data), |t| Ok(t))?
                    .id;
                let logical_id = ext_base_id(&PkSk { pk, sk });
                let plan = build_partition_write_plan::<T>(
                    self,
                    &logical_id,
                    &data,
                    options.custom_sort,
                    options.ttl.map(|ttl| ttl.compute_timestamp()),
                )
                .await?;
                items.extend(plan.put_items);
                stale_delete_ids.extend(plan.stale_delete_ids);
                created.push(T::new(logical_id, data));
            }
            self.raw_batch_delete_ids(stale_delete_ids).await?;
            self.raw_batch_put_item(items).await?;
            return Ok(created);
        }
        let (items, ids): (Vec<DynamoMap>, Vec<PkSk>) = data_and_options
            .iter()
            .map(|(data, options)| {
                let PkSk { pk, sk } = options.token.as_ref().map_or_else(
                    || Ok(self.create_token::<T>(parent_id, data)?.id),
                    |t| Ok(t.id.clone()),
                )?;
                let sort: Option<f64> = options.custom_sort;
                let ttl: Option<i64> = options.ttl.as_ref().map(|ttl| ttl.compute_timestamp());
                let now = Timestamp::now();
                Ok((
                    build_dynamo_map_for_new_obj::<T>(
                        data,
                        pk.clone(),
                        sk.clone(),
                        Some(vec![
                            (AUTO_FIELDS_CREATED_AT, Box::new(now.clone())),
                            (AUTO_FIELDS_UPDATED_AT, Box::new(now)),
                            (AUTO_FIELDS_SORT, Box::new(sort)),
                            (AUTO_FIELDS_TTL, Box::new(ttl)),
                        ]),
                    )?,
                    PkSk { pk, sk },
                ))
            })
            .collect::<Result<Vec<(DynamoMap, PkSk)>, ServerError>>()?
            .into_iter()
            .unzip();
        // Split into 25-item batches (max supported by DynamoDB).
        for batch in items.chunks(25) {
            self.backend
                .batch_put_item(self.table.clone(), batch.to_vec())
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
        parent_id: &PkSk,
        data: T::Data,
        insert_position: DynamoInsertPosition,
    ) -> Result<T, ServerError> {
        if matches!(T::id_logic(), IdLogic::BatchOptimized { .. }) {
            return Err(DynamoInvalidBatchOptimizedIdUsage::new());
        }
        let sort_val = calculate_sort_values::<T>(self, parent_id, &data, insert_position, 1)
            .await?
            .pop()
            .ok_or(DynamoInvalidOperation::new(
                "failed to generate new ordered ID",
            ))?;
        self.create_item_opt::<T>(
            parent_id,
            data,
            CreateOptions {
                custom_sort: Some(sort_val),
                ..Default::default()
            },
        )
        .await
    }

    pub async fn batch_create_item_ordered<T: DynamoObject>(
        &self,
        parent_id: &PkSk,
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
            parent_id,
            data.first().unwrap(),
            insert_position,
            data.len(),
        )
        .await?;
        self.batch_create_item_opt::<T>(
            parent_id,
            data.into_iter()
                .zip(new_ids.into_iter())
                .map(|(d, sort_val)| {
                    (
                        d,
                        CreateOptions {
                            custom_sort: Some(sort_val),
                            ..Default::default()
                        },
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
        if is_partitioned_id_logic::<T>() {
            return Err(DynamoInvalidExtIdUsage::new());
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
    ///
    /// This is very efficient, as it uses fetch + conditional update, rather
    /// than some kind of blocking or locking call.
    pub async fn update_item_transaction<T: DynamoObject>(
        &self,
        id: PkSk,
        op: impl FnOnce(Option<T::Data>) -> Result<T::Data, ServerError>,
    ) -> Result<T, ServerError> {
        if matches!(T::id_logic(), IdLogic::BatchOptimized { .. }) {
            return Err(DynamoInvalidBatchOptimizedIdUsage::new());
        }
        if is_partitioned_id_logic::<T>() {
            return Err(DynamoInvalidExtIdUsage::new());
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
            map_before
                .into_iter()
                .map(|(k, v)| (k, (v, CmpOp::Eq)))
                .collect(),
            vec![existance_condition],
            HashMap::new(),
            HashMap::new(),
        )
        .await?;
        Ok(object_after)
    }

    /// Similar to `update_item`, but aborts the update if the provided
    /// conditions are not met (returning an error if aborted).
    ///
    /// Update conditions are directly supported by Dynamo, so this is very
    /// efficient.
    pub async fn update_item_with_conditions<T: DynamoObject>(
        &self,
        object: &T,
        conditions: Vec<UpdateCondition<T>>,
    ) -> Result<(), ServerError> {
        if matches!(T::id_logic(), IdLogic::BatchOptimized { .. }) {
            return Err(DynamoInvalidBatchOptimizedIdUsage::new());
        }
        if is_partitioned_id_logic::<T>() {
            return Err(DynamoInvalidExtIdUsage::new());
        }

        // Attribute comparison conditions.
        let mut cmp_attribute_conditions: HashMap<String, (AttributeValue, CmpOp)> = HashMap::new();

        // Custom condition expressions (which may reference expression
        // attribute names/values).
        let mut custom_conditions: Vec<String> = vec![Self::ITEM_EXISTS_CONDITION.to_string()];
        let mut expression_attribute_names: HashMap<String, String> = HashMap::new();
        let mut expression_attribute_values: HashMap<String, AttributeValue> = HashMap::new();

        for (idx, cond) in conditions.into_iter().enumerate() {
            match cond {
                UpdateCondition::PartialEq(data) => {
                    // Convert partial data into a map; nulls are skipped by serializer.
                    let (data_map, _skipped_nulls) =
                        build_dynamo_map_internal(&data, None, None, None)?;
                    cmp_attribute_conditions.extend(
                        data_map
                            .into_iter()
                            .filter(|(k, _)| (k != "pk") && (k != "sk"))
                            .map(|(k, v)| (k, (v, CmpOp::Eq))),
                    );
                }
                UpdateCondition::NumericCompare {
                    partial: data,
                    map_op: op,
                } => {
                    // Convert partial data into a map, and check values are numeric.
                    let (data_map, _skipped_nulls) =
                        build_dynamo_map_internal(&data, None, None, None)?;
                    if let Some((bad_k, _)) = data_map
                        .iter()
                        .find(|(_, v)| !matches!(v, AttributeValue::N(_)))
                    {
                        return Err(DynamoInvalidOperation::new(&format!(
                            "non-numeric value provided for numeric comparison on '{}'",
                            bad_k
                        )));
                    }
                    cmp_attribute_conditions.extend(
                        data_map
                            .into_iter()
                            .filter(|(k, _)| (k != "pk") && (k != "sk"))
                            .map(|(k, v)| (k, (v, op.into()))),
                    );
                }
                UpdateCondition::FieldIsNone(field) => {
                    let path = field
                        .split('.')
                        .enumerate()
                        .map(|(j, field_part)| {
                            let placeholder = format!("#u{}p{}", idx + 1, j + 1);
                            expression_attribute_names
                                .insert(placeholder.clone(), field_part.to_string());
                            placeholder
                        })
                        .collect::<Vec<_>>()
                        .join(".");
                    let null_type_placeholder = format!(":u{}n", idx + 1);
                    expression_attribute_values.insert(
                        null_type_placeholder.clone(),
                        AttributeValue::S("NULL".to_string()),
                    );
                    let condition = format!(
                        "(attribute_not_exists({p}) OR attribute_type({p}, {t}))",
                        p = path,
                        t = null_type_placeholder,
                    );
                    custom_conditions.push(condition);
                }
                UpdateCondition::FieldIsSome(field) => {
                    let path = field
                        .split('.')
                        .enumerate()
                        .map(|(j, field_part)| {
                            let placeholder = format!("#u{}p{}", idx + 1, j + 1);
                            expression_attribute_names
                                .insert(placeholder.clone(), field_part.to_string());
                            placeholder
                        })
                        .collect::<Vec<_>>()
                        .join(".");
                    let null_type_placeholder = format!(":u{}n", idx + 1);
                    expression_attribute_values.insert(
                        null_type_placeholder.clone(),
                        AttributeValue::S("NULL".to_string()),
                    );
                    let condition = format!(
                        "(attribute_exists({p}) AND NOT attribute_type({p}, {t}))",
                        p = path,
                        t = null_type_placeholder,
                    );
                    custom_conditions.push(condition);
                }
            }
        }

        self.update_item_internal::<T>(
            object,
            cmp_attribute_conditions,
            custom_conditions,
            expression_attribute_names,
            expression_attribute_values,
        )
        .await
    }

    async fn update_item_internal<T: DynamoObject>(
        &self,
        object: &T,
        attribute_conditions: HashMap<String, (AttributeValue, CmpOp)>,
        custom_conditions: Vec<String>,
        mut expression_attribute_names: HashMap<String, String>,
        mut expression_attribute_values: HashMap<String, AttributeValue>,
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

        // Build update expression.
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

        // Build custom conditions & attribute equality conditions.
        let condition_expression = custom_conditions
            .into_iter()
            .chain(
                // Append comparison conditions, using keyed placeholders to avoid
                // ambiguity.
                attribute_conditions
                    .into_iter()
                    .enumerate()
                    .map(|(idx, (key, (value, op)))| {
                        let key_placeholder = format!("#c{}", idx + 1);
                        let value_placeholder = format!(":cv{}", idx + 1);
                        let condition = format!("{} {} {}", key_placeholder, op, value_placeholder);
                        expression_attribute_names.insert(key_placeholder, key);
                        expression_attribute_values.insert(value_placeholder, value);
                        condition
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
        if is_partitioned_id_logic::<T>() {
            let ids = expand_partition_delete_ids::<T>(self, vec![id]).await?;
            return self.raw_batch_delete_ids(ids).await;
        }
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
        self.raw_batch_delete_ids(expand_partition_delete_ids::<T>(self, keys).await?)
            .await
    }

    /// Deletes *all* children of type T on the parent.
    pub async fn batch_delete_all<T: DynamoObject>(
        &self,
        parent_id: &PkSk,
    ) -> Result<(), ServerError> {
        // Ensure we dedup IDs, since query logic expands batches internally.
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
    /// in large batches (which get expanded by query logic).
    pub async fn batch_replace_all_ordered<T: DynamoObject>(
        &self,
        parent_id: &PkSk,
        data: Vec<T::Data>,
    ) -> Result<(), ServerError> {
        // Validations.
        validate_parent_id::<T>(parent_id)?;
        let batch_size = match T::id_logic() {
            IdLogic::BatchOptimized { batch_size } => {
                if batch_size == 0 {
                    return Err(DynamoInvalidOperation::new(
                        "invalid IdLogic::BatchOptimized usage; batch_size must be greater than 0",
                    ));
                }
                Some(batch_size)
            }
            _ => None,
        };

        self.batch_delete_all::<T>(parent_id).await?;
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

        // For batch-optimized ID logic, group the data into expandable batches.
        // -------------------------------------------------------------------
        req_not_none!(batch_size, CriticalError);
        let maps = build_expandable_batch_maps::<T>(parent_id, data, batch_size)?;

        self.raw_batch_put_item(maps).await
    }

    /// Performs a full table scan and returns the raw Dynamo items as-is. No
    /// sorting, batch expansion, filtering, or other processing.
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

    /// Performs no checks and directly fetches the given IDs from the database.
    pub async fn raw_batch_get_ids(
        &self,
        keys: Vec<PkSk>,
        projection_expression: Option<String>,
    ) -> Result<Vec<DynamoMap>, ServerError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut items = Vec::new();

        // Split into 100-item batches (max supported by DynamoDB for reads).
        for batch in keys.chunks(100) {
            let mut pending_keys = batch
                .iter()
                .map(|id| {
                    collection! {
                        "pk".to_string() => AttributeValue::S(id.pk.clone()),
                        "sk".to_string() => AttributeValue::S(id.sk.clone()),
                    }
                })
                .collect::<Vec<_>>();
            while !pending_keys.is_empty() {
                let response = self
                    .backend
                    .batch_get_item(
                        self.table.clone(),
                        pending_keys,
                        projection_expression.clone(),
                    )
                    .await
                    .map_err(|e| DynamoCalloutError::with_debug(&e))?;
                if let Some(found) = response
                    .responses()
                    .and_then(|responses| responses.get(&self.table))
                {
                    items.extend(found.iter().cloned());
                }
                pending_keys = response
                    .unprocessed_keys()
                    .and_then(|keys| keys.get(&self.table))
                    .map(|keys| keys.keys().to_vec())
                    .unwrap_or_default();
            }
        }
        Ok(items)
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

        // Split into 25-item batches (max supported by DynamoDB).
        for batch in items.chunks(25) {
            self.backend
                .batch_delete_item(self.table.clone(), batch.to_vec())
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

        // Split into 25-item batches (max supported by DynamoDB).
        for batch in items.chunks(25) {
            self.backend
                .batch_put_item(self.table.clone(), batch.to_vec())
                .await
                .map_err(|e| DynamoCalloutError::with_debug(&e))?;
        }
        Ok(())
    }
}
