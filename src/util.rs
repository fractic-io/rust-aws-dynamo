use std::{collections::HashSet, sync::Arc};

use aws_sdk_dynamodb::{
    operation::{
        batch_write_item::BatchWriteItemError, delete_item::DeleteItemError,
        update_item::UpdateItemError,
    },
    types::AttributeValue,
};
use backend::DynamoBackend;
pub(crate) use calculate_sort::calculate_sort_values;
use chrono::{DateTime, Duration, Utc};
use fractic_core::{collection, req_not_none};
use fractic_server_error::{CriticalError, ServerError};
use raw_batch_helpers::{unprocessed_delete_keys, unprocessed_put_items, wait_before_batch_retry};

use crate::{
    errors::{
        DynamoBatchReadRetriesExhausted, DynamoBatchWriteRetriesExhausted, DynamoCalloutError,
        DynamoInvalidBatchOptimizedIdUsage, DynamoInvalidExtIdUsage, DynamoInvalidOperation,
        DynamoInvalidPhantomObjectUsage, DynamoNotFound, DynamoUnexpectedItemCount,
    },
    schema::{
        identifiers::{generate_id, RawIdPath},
        item_deserialization::parse_dynamo_map,
        item_serialization::{
            build_canonical_data_map, build_dynamo_map_for_existing_obj,
            build_dynamo_map_for_new_obj, build_materialized_write_plan, IdKeys,
        },
        materialization::validate_materialized_storage,
        pk_sk::id_fields_from_map,
        DynamoObject, IdLogic, PkSk, Timestamp,
    },
    util::{
        collapse_helpers::{
            build_partition_write_plan, expand_partition_delete_ids, ext_base_id,
            fetch_num_partitions, fetch_num_partitions_batch, is_partitioned_id_logic,
        },
        expand_helpers::build_expandable_batch_maps,
        id_relations::{child_query_prefix, validate_object_id, validate_parent_for},
        rename_cleanup::add_legacy_field_removals,
        update_plan::{AttributeUpdatePlan, CmpOp},
    },
    DynamoCtxView,
};

// Modules.
// ----------------------------------------------------------------------------

pub mod backend;
mod calculate_sort;
pub(crate) mod collapse_helpers;
pub mod consistency_overlay;
mod expand_helpers;
mod id_relations;
mod metadata_helpers;
mod query;
mod query_execution;
mod raw_batch_helpers;
mod rename_cleanup;
mod test;
mod update_plan;

// Constants.
// ----------------------------------------------------------------------------

pub use crate::schema::{
    DynamoMap, AUTO_FIELDS_CREATED_AT, AUTO_FIELDS_SORT, AUTO_FIELDS_TTL, AUTO_FIELDS_UPDATED_AT,
    COLLAPSE_DATA_RESERVED_KEY, COLLAPSE_PLACEHOLDER_RESERVED_KEY, EXPAND_DATA_RESERVED_KEY,
};

use raw_batch_helpers::{MAX_BATCH_READ_RETRIES, MAX_BATCH_WRITE_RETRIES};

// Definitions.
// ----------------------------------------------------------------------------

pub use query::{DynamoGenericQuery, DynamoQuery, IndexConfig, IndexKind};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Debug)]
pub struct CreateToken<T: DynamoObject> {
    // NOTE: It's important that this struct does not implement Clone, provides
    // no constructor, and has private fields. This way tokens can't be used
    // more than once, since they can't be duplicated and must be moved when
    // building `CreateOptions`.
    id: PkSk,
    _phantom: std::marker::PhantomData<T>,
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

/// Controls how a primary-key item read is performed.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GetOptions {
    /// Uses DynamoDB's strongly consistent read mode when enabled.
    pub consistent_read: bool,
}

/// Comparison operators for numeric conditions.
#[derive(Debug, Clone, Copy)]
pub enum NumericOp {
    GreaterThan,
    GreaterThanOrEquals,
    LessThan,
    LessThanOrEquals,
}

/// An immutable persistence snapshot captured by
/// [`UpdateCondition::unchanged_since`].
#[derive(Debug, Clone)]
pub struct DynamoObjectSnapshot<T: DynamoObject> {
    updated_at: Option<Timestamp>,
    fallback_data: T::Data,
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
    /// Checks that an object has not changed since the captured snapshot.
    UnchangedSince(DynamoObjectSnapshot<T>),
}

/// Summary of physical rows removed by a partition delete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchDeletePartitionResult {
    /// Number of physical rows found in the partition.
    pub row_count: usize,
    /// Distinct object labels recognized in the deleted row keys.
    pub object_labels: HashSet<String>,
}

// Impls.
// ----------------------------------------------------------------------------

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

impl<T: DynamoObject> UpdateCondition<T> {
    /// Captures the object's current persistence state for optimistic
    /// concurrency control.
    ///
    /// Call this before mutating `object`, then pass the returned condition to
    /// [`DynamoUtil::update_item_with_conditions`]:
    ///
    /// ```ignore
    /// let unchanged = UpdateCondition::unchanged_since(&object);
    /// object.data.name = "new name".to_string();
    /// util.update_item_with_conditions(&object, vec![unchanged]).await?;
    /// ```
    ///
    /// The condition compares `updated_at` when the snapshot contains it,
    /// otherwise it falls back to comparing all captured canonical data.
    pub fn unchanged_since(object: &T) -> Self {
        Self::UnchangedSince(DynamoObjectSnapshot {
            updated_at: object.updated_at().cloned(),
            fallback_data: object.data().clone(),
        })
    }
}

impl<T: DynamoObject> CreateToken<T> {
    pub fn id(&self) -> &PkSk {
        &self.id
    }
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

// Public interface.
// ----------------------------------------------------------------------------

#[derive(Clone)]
pub struct DynamoUtil {
    pub backend: Arc<dyn DynamoBackend>,
    pub consistency_overlay: Arc<dyn consistency_overlay::DynamoConsistencyOverlay>,
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
            consistency_overlay: ctx.dynamo_consistency_overlay().await?,
            table: table.into(),
        })
    }

    pub async fn get_item<T: DynamoObject>(&self, id: PkSk) -> Result<Option<T>, ServerError> {
        self.get_item_opt(id, GetOptions::default()).await
    }

    /// Fetches an item by primary key with caller-selected read options.
    pub async fn get_item_opt<T: DynamoObject>(
        &self,
        id: PkSk,
        options: GetOptions,
    ) -> Result<Option<T>, ServerError> {
        reject_batch_optimized_ids::<T>()?;
        validate_object_id::<T>(&id)?;
        if is_partitioned_id_logic::<T>() {
            let prefix = ext_base_id(&id);
            let query = DynamoQuery::pk(prefix.pk).sk_begins_with(prefix.sk);
            let query = if options.consistent_read {
                query.consistent_read()
            } else {
                query
            };
            let items = self.query::<T>(query).await?;
            match items.len() {
                0 => Ok(None),
                1 => Ok(items.into_iter().next()),
                n => Err(DynamoUnexpectedItemCount::new(&format!(
                    "expected at most one logical item for '{}', got {}",
                    id, n
                ))),
            }
        } else {
            let key = collection! {
                "pk".to_string() => AttributeValue::S(id.pk),
                "sk".to_string() => AttributeValue::S(id.sk),
            };
            let response = self
                .backend
                .get_item(self.table.clone(), key, None, options.consistent_read)
                .await
                .map_err(|e| DynamoCalloutError::with_debug(&e))?;
            response
                .item
                .map(|item| parse_dynamo_map::<T>(&item))
                .transpose()
        }
    }

    /// Efficiently checks if an item exists, without fetching item data.
    pub async fn item_exists(&self, id: PkSk) -> Result<bool, ServerError> {
        self.item_exists_opt(id, GetOptions::default()).await
    }

    /// Efficiently checks if an item exists with caller-selected consistency.
    pub async fn item_exists_opt(
        &self,
        id: PkSk,
        options: GetOptions,
    ) -> Result<bool, ServerError> {
        let key = collection! {
            "pk".to_string() => AttributeValue::S(id.pk),
            "sk".to_string() => AttributeValue::S(id.sk),
        };
        let projection = Some("pk".to_string());
        let response = self
            .backend
            .get_item(self.table.clone(), key, projection, options.consistent_read)
            .await
            .map_err(|e| DynamoCalloutError::with_debug(&e))?;
        Ok(response.item.is_some())
    }

    pub fn create_token<T: DynamoObject>(
        &self,
        parent_id: &PkSk,
        data: &T::Data,
    ) -> Result<CreateToken<T>, ServerError> {
        reject_phantom_objects::<T>()?;
        reject_batch_optimized_ids::<T>()?;
        Ok(CreateToken {
            id: generate_id::<T>(data, parent_id)?,
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
        reject_phantom_objects::<T>()?;
        reject_batch_optimized_ids::<T>()?;
        validate_materialized_storage::<T>()?;
        let PkSk { pk, sk } = options
            .token
            .map_or_else(|| self.create_token(parent_id, &data), Ok)?
            .id;
        let sort = options.custom_sort;
        let ttl = options.ttl.map(|ttl| ttl.compute_timestamp());
        if is_partitioned_id_logic::<T>() {
            let logical_id = ext_base_id(&PkSk { pk, sk });
            let num_existing_partitions = fetch_num_partitions(self, &logical_id).await?;
            let plan = build_partition_write_plan::<T>(
                &logical_id,
                &data,
                sort,
                ttl,
                num_existing_partitions,
            )?;
            self.raw_batch_delete_ids(plan.stale_delete_ids).await?;
            self.raw_batch_put_item(plan.put_items).await?;
            Ok(T::new(logical_id, data))
        } else {
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
                .put_item(self.table.clone(), map.clone())
                .await
                .map_err(|e| DynamoCalloutError::with_debug(&e))?;
            self.consistency_overlay.record_put(&self.table, map);
            Ok(T::new(PkSk { pk, sk }, data))
        }
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
        reject_phantom_objects::<T>()?;
        reject_batch_optimized_ids::<T>()?;
        validate_materialized_storage::<T>()?;
        if data_and_options.is_empty() {
            return Ok(Vec::new());
        }
        let create_batch_id = |data: &T::Data, token: Option<&CreateToken<T>>| {
            token.map_or_else(
                || {
                    self.create_token::<T>(parent_id, data)
                        .map(|token| token.id)
                },
                |token| Ok(token.id.clone()),
            )
        };
        if is_partitioned_id_logic::<T>() {
            let pending_writes = data_and_options
                .into_iter()
                .map(|(data, options)| {
                    let PkSk { pk, sk } = create_batch_id(&data, options.token.as_ref())?;
                    Ok((
                        ext_base_id(&PkSk { pk, sk }),
                        data,
                        options.custom_sort,
                        options.ttl.map(|ttl| ttl.compute_timestamp()),
                    ))
                })
                .collect::<Result<Vec<_>, ServerError>>()?;

            let mut seen_logical_ids = HashSet::new();
            let unique_logical_ids = pending_writes
                .iter()
                .filter(|(logical_id, _, _, _)| seen_logical_ids.insert(logical_id))
                .map(|(logical_id, _, _, _)| logical_id.clone())
                .collect::<Vec<_>>();
            let existing_partition_counts =
                fetch_num_partitions_batch(self, &unique_logical_ids).await?;

            let mut items = Vec::new();
            let mut stale_delete_ids = Vec::new();
            let mut created = Vec::new();
            for (logical_id, data, sort, ttl) in pending_writes {
                let plan = build_partition_write_plan::<T>(
                    &logical_id,
                    &data,
                    sort,
                    ttl,
                    existing_partition_counts.get(&logical_id).copied(),
                )?;
                items.extend(plan.put_items);
                stale_delete_ids.extend(plan.stale_delete_ids);
                created.push(T::new(logical_id, data));
            }
            self.raw_batch_delete_ids(stale_delete_ids).await?;
            self.raw_batch_put_item(items).await?;
            Ok(created)
        } else {
            let (items, ids): (Vec<DynamoMap>, Vec<PkSk>) = data_and_options
                .iter()
                .map(|(data, options)| {
                    let PkSk { pk, sk } = create_batch_id(data, options.token.as_ref())?;
                    let sort = options.custom_sort;
                    let ttl = options.ttl.as_ref().map(TtlConfig::compute_timestamp);
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

            self.raw_batch_put_item(items).await?;
            Ok(ids
                .into_iter()
                .zip(data_and_options)
                .map(|(id, (data, _))| T::new(id, data))
                .collect())
        }
    }

    /// Use when complex ordering is required (for simple ordering, consider
    /// using UUID-v7 IDs).
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
    /// sort values once), or consider using UUID-v7 IDs instead.
    pub async fn create_item_ordered<T: DynamoObject>(
        &self,
        parent_id: &PkSk,
        data: T::Data,
        insert_position: DynamoInsertPosition,
    ) -> Result<T, ServerError> {
        self.create_item_ordered_opt(parent_id, data, insert_position, CreateOptions::default())
            .await
    }

    /// Creates an ordered item using caller-provided creation options.
    pub async fn create_item_ordered_opt<T: DynamoObject>(
        &self,
        parent_id: &PkSk,
        data: T::Data,
        insert_position: DynamoInsertPosition,
        mut options: CreateOptions<T>,
    ) -> Result<T, ServerError> {
        reject_phantom_objects::<T>()?;
        reject_batch_optimized_ids::<T>()?;
        let sort_val = calculate_sort_values::<T>(self, parent_id, &data, insert_position, 1)
            .await?
            .pop()
            .ok_or(DynamoInvalidOperation::new(
                "failed to generate new ordered ID",
            ))?;
        options.custom_sort = Some(sort_val);
        self.create_item_opt::<T>(parent_id, data, options).await
    }

    pub async fn batch_create_item_ordered<T: DynamoObject>(
        &self,
        parent_id: &PkSk,
        data: Vec<T::Data>,
        insert_position: DynamoInsertPosition,
    ) -> Result<Vec<T>, ServerError> {
        reject_phantom_objects::<T>()?;
        reject_batch_optimized_ids::<T>()?;
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
                .zip(new_ids)
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
        reject_phantom_objects::<T>()?;
        reject_batch_optimized_ids::<T>()?;
        if is_partitioned_id_logic::<T>() {
            return Err(DynamoInvalidExtIdUsage::new());
        }
        self.update_item_internal(
            object,
            AttributeUpdatePlan::with_condition(Self::ITEM_EXISTS_CONDITION),
        )
        .await
    }

    /// Recomputes and atomically refreshes only an object's materialized
    /// attributes, without changing its canonical data or automatic timestamps.
    ///
    /// The update is conditioned on the canonical data still matching
    /// `object`, preventing a backfill based on a stale read from overwriting
    /// materialized values produced by a concurrent domain update.
    pub async fn refresh_materialized_attributes<T: DynamoObject>(
        &self,
        object: &T,
    ) -> Result<(), ServerError> {
        reject_phantom_objects::<T>()?;
        validate_object_id::<T>(object.id())?;
        if T::materialized_attribute_names().is_empty() {
            return Ok(());
        }

        let materialized = build_materialized_write_plan::<T>(object.id(), object.data())?;
        let mut update = AttributeUpdatePlan::with_condition(Self::ITEM_EXISTS_CONDITION);
        update.set = materialized.set;
        update.remove = materialized.remove;
        add_legacy_field_removals::<T>(&update.set, &mut update.remove);
        update.add_unchanged_condition::<T>(object.updated_at(), object.data())?;

        self.execute_update_plan::<T>(object.id(), update).await
    }

    /// Updates an object in an all-or-nothing transaction. If the object has
    /// changed since it was fetched, the update is aborted and returns an
    /// error. If 'op' returns an error, the transaction is also aborted. If the
    /// object does not exist, the result of 'op' will be created as a new
    /// object, and the transaction condition will ensure another object with
    /// the same ID wasn't created in the meantime.
    ///
    /// This is very efficient, as it uses fetch + conditional update, rather
    /// than some kind of blocking or locking call. It uses the same
    /// optimistic-concurrency semantics as [`UpdateCondition::unchanged_since`].
    pub async fn update_item_transaction<T: DynamoObject>(
        &self,
        id: PkSk,
        op: impl FnOnce(Option<T::Data>) -> Result<T::Data, ServerError>,
    ) -> Result<T, ServerError> {
        reject_phantom_objects::<T>()?;
        reject_batch_optimized_ids::<T>()?;
        if is_partitioned_id_logic::<T>() {
            return Err(DynamoInvalidExtIdUsage::new());
        }
        let object_before = self.get_item::<T>(id.clone()).await?;
        let update = match object_before.as_ref() {
            Some(object) => {
                let mut update = AttributeUpdatePlan::with_condition(Self::ITEM_EXISTS_CONDITION);
                update.add_unchanged_condition::<T>(object.updated_at(), object.data())?;
                update
            }
            None => AttributeUpdatePlan::with_condition(Self::ITEM_DOES_NOT_EXIST_CONDITION),
        };
        let object_after = T::new(id, op(object_before.map(DynamoObject::into_data))?);
        self.update_item_internal::<T>(&object_after, update)
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
        reject_phantom_objects::<T>()?;
        reject_batch_optimized_ids::<T>()?;
        if is_partitioned_id_logic::<T>() {
            return Err(DynamoInvalidExtIdUsage::new());
        }

        let mut update = AttributeUpdatePlan::with_condition(Self::ITEM_EXISTS_CONDITION);

        for condition in conditions {
            match condition {
                UpdateCondition::PartialEq(data) => {
                    // Convert partial data into a map; nulls are skipped by serializer.
                    let (data_map, _skipped_nulls) = build_canonical_data_map::<T>(&data)?;
                    update
                        .comparisons
                        .extend(data_map.into_iter().map(|(k, v)| (k, (v, CmpOp::Eq))));
                }
                UpdateCondition::NumericCompare {
                    partial: data,
                    map_op: op,
                } => {
                    // Convert partial data into a map, and check values are numeric.
                    let (data_map, _skipped_nulls) = build_canonical_data_map::<T>(&data)?;
                    if let Some((bad_k, _)) = data_map
                        .iter()
                        .find(|(_, v)| !matches!(v, AttributeValue::N(_)))
                    {
                        return Err(DynamoInvalidOperation::new(&format!(
                            "non-numeric value provided for numeric comparison on '{bad_k}'"
                        )));
                    }
                    update
                        .comparisons
                        .extend(data_map.into_iter().map(|(k, v)| (k, (v, op.into()))));
                }
                UpdateCondition::FieldIsNone(field) => {
                    update.add_presence_condition::<T>(&field, false);
                }
                UpdateCondition::FieldIsSome(field) => {
                    update.add_presence_condition::<T>(&field, true);
                }
                UpdateCondition::UnchangedSince(snapshot) => {
                    update.add_unchanged_condition::<T>(
                        snapshot.updated_at.as_ref(),
                        &snapshot.fallback_data,
                    )?;
                }
            }
        }

        self.update_item_internal::<T>(object, update).await
    }

    async fn update_item_internal<T: DynamoObject>(
        &self,
        object: &T,
        mut update: AttributeUpdatePlan,
    ) -> Result<(), ServerError> {
        validate_object_id::<T>(object.id())?;
        let (map, mut null_keys) = build_dynamo_map_for_existing_obj::<T>(
            object,
            IdKeys::None,
            Some(vec![(AUTO_FIELDS_UPDATED_AT, Box::new(Timestamp::now()))]),
        )?;
        add_legacy_field_removals::<T>(&map, &mut null_keys);

        update.set = map;
        update.remove = null_keys;
        self.execute_update_plan::<T>(object.id(), update).await
    }

    async fn execute_update_plan<T: DynamoObject>(
        &self,
        id: &PkSk,
        update: AttributeUpdatePlan,
    ) -> Result<(), ServerError> {
        let key = collection! {
            "pk".to_string() => AttributeValue::S(id.pk.clone()),
            "sk".to_string() => AttributeValue::S(id.sk.clone()),
        };
        let expression = update.into_expression::<T>();

        let response = self
            .backend
            .update_item(
                self.table.clone(),
                key,
                expression.update,
                expression.values,
                expression.names,
                Some(expression.condition),
            )
            .await
            .map_err(|e| match e.into_service_error() {
                UpdateItemError::ResourceNotFoundException(_) => DynamoNotFound::new(),
                other => DynamoCalloutError::with_debug(&other),
            })?;
        if let Some(item) = response.attributes {
            self.consistency_overlay.record_put(&self.table, item);
        }
        Ok(())
    }

    pub async fn delete_item<T: DynamoObject>(&self, id: PkSk) -> Result<(), ServerError> {
        reject_phantom_objects::<T>()?;
        reject_batch_optimized_ids::<T>()?;
        validate_object_id::<T>(&id)?;
        if is_partitioned_id_logic::<T>() {
            let ids = expand_partition_delete_ids::<T>(self, vec![id]).await?;
            self.raw_batch_delete_ids(ids).await
        } else {
            let deleted_id = id.clone();
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
            self.consistency_overlay
                .record_delete(&self.table, deleted_id);
            Ok(())
        }
    }

    pub async fn batch_delete_item<T: DynamoObject>(
        &self,
        keys: Vec<PkSk>,
    ) -> Result<(), ServerError> {
        reject_phantom_objects::<T>()?;
        reject_batch_optimized_ids::<T>()?;
        for key in &keys {
            validate_object_id::<T>(key)?;
        }
        self.raw_batch_delete_ids(expand_partition_delete_ids::<T>(self, keys).await?)
            .await
    }

    /// Deletes *all* children of type T on the parent.
    pub async fn batch_delete_all<T: DynamoObject>(
        &self,
        parent_id: &PkSk,
    ) -> Result<(), ServerError> {
        reject_phantom_objects::<T>()?;
        validate_parent_for::<T>(parent_id)?;
        let search_prefix = child_query_prefix::<T>(parent_id);
        let response = self
            .backend
            .query(
                self.table.clone(),
                None,
                "pk = :pk_val AND begins_with(sk, :sk_val)".to_string(),
                collection! {
                    ":pk_val".to_string() => AttributeValue::S(search_prefix.pk),
                    ":sk_val".to_string() => AttributeValue::S(search_prefix.sk),
                },
                Some("pk, sk".to_string()),
                false,
            )
            .await
            .map_err(|e| DynamoCalloutError::with_debug(&e))?;
        let ids: HashSet<PkSk> = response
            .into_iter()
            .flat_map(|page| page.items.unwrap_or_default().into_iter())
            .filter_map(|item| {
                let (_, sk) =
                    id_fields_from_map(&item).expect("query result item did not have pk/sk.");
                match RawIdPath::new(sk).object_label() {
                    Ok(label) if label == T::id_label() => Some(PkSk::from_map(&item)),
                    _ => None,
                }
            })
            .collect::<Result<HashSet<_>, ServerError>>()?;
        self.raw_batch_delete_ids(ids.into_iter().collect()).await
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
        reject_phantom_objects::<T>()?;
        validate_materialized_storage::<T>()?;
        // Validations.
        validate_parent_for::<T>(parent_id)?;
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
        let maps = build_expandable_batch_maps::<T>(parent_id, &data, batch_size)?;

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
            for attempt in 0..=MAX_BATCH_READ_RETRIES {
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
                if pending_keys.is_empty() {
                    break;
                }
                if attempt == MAX_BATCH_READ_RETRIES {
                    return Err(DynamoBatchReadRetriesExhausted::new(
                        pending_keys.len(),
                        MAX_BATCH_READ_RETRIES,
                    ));
                }
                wait_before_batch_retry(attempt).await;
            }
        }
        Ok(items)
    }

    /// Performs no checks and directly deletes the given IDs from the database
    /// (ignores duplicates).
    pub async fn raw_batch_delete_ids(&self, keys: Vec<PkSk>) -> Result<(), ServerError> {
        if keys.is_empty() {
            return Ok(());
        }

        // Deduplicate IDs: Track borrowed keys so deduplication neither clones
        // nor changes the order in which callers supplied the first occurrence.
        let keep = {
            let mut seen = HashSet::with_capacity(keys.len());
            keys.iter().map(|key| seen.insert(key)).collect::<Vec<_>>()
        };
        let mut unique_keys = keys
            .into_iter()
            .zip(keep)
            .filter_map(|(key, keep)| keep.then_some(key));

        // Split into 25-item batches (max supported by DynamoDB).
        loop {
            let mut pending = unique_keys
                .by_ref()
                .take(25)
                .map(|id| {
                    collection! {
                        "pk".to_string() => AttributeValue::S(id.pk),
                        "sk".to_string() => AttributeValue::S(id.sk),
                    }
                })
                .collect::<Vec<_>>();
            if pending.is_empty() {
                break;
            }

            for attempt in 0..=MAX_BATCH_WRITE_RETRIES {
                let requested = pending;
                let response = self
                    .backend
                    .batch_delete_item(self.table.clone(), requested.clone())
                    .await
                    .map_err(|e| match e.into_service_error() {
                        BatchWriteItemError::ResourceNotFoundException(_) => DynamoNotFound::new(),
                        other => DynamoCalloutError::with_debug(&other),
                    })?;
                pending = unprocessed_delete_keys(&response, &self.table)?;
                let pending_ids = pending
                    .iter()
                    .map(PkSk::from_map)
                    .collect::<Result<HashSet<_>, _>>()?;
                for id in requested
                    .iter()
                    .map(PkSk::from_map)
                    .collect::<Result<Vec<_>, _>>()?
                {
                    if !pending_ids.contains(&id) {
                        self.consistency_overlay.record_delete(&self.table, id);
                    }
                }
                if pending.is_empty() {
                    break;
                }
                if attempt == MAX_BATCH_WRITE_RETRIES {
                    return Err(DynamoBatchWriteRetriesExhausted::new(
                        "delete",
                        pending.len(),
                        MAX_BATCH_WRITE_RETRIES,
                    ));
                }
                wait_before_batch_retry(attempt).await;
            }
        }
        Ok(())
    }

    /// Performs no checks and directly deletes an entire DynamoDB partition.
    pub async fn raw_batch_delete_partition(
        &self,
        partition_key: String,
    ) -> Result<BatchDeletePartitionResult, ServerError> {
        // Query directly to bypass logical item expansion and collapse.
        let response = self
            .backend
            .query(
                self.table.clone(),
                None,
                "pk = :pk_val".to_string(),
                collection! {
                    ":pk_val".to_string() => AttributeValue::S(partition_key),
                },
                Some("pk, sk".to_string()),
                false,
            )
            .await
            .map_err(|e| DynamoCalloutError::with_debug(&e))?;
        let keys = response
            .into_iter()
            .flat_map(|page| page.items.unwrap_or_default())
            .map(|item| PkSk::from_map(&item))
            .collect::<Result<Vec<_>, _>>()?;
        let mut object_labels = HashSet::new();
        for key in &keys {
            if let Ok(label) = key.object_type() {
                if !object_labels.contains(label) {
                    object_labels.insert(label.to_owned());
                }
            }
        }
        let result = BatchDeletePartitionResult {
            row_count: keys.len(),
            object_labels,
        };
        self.raw_batch_delete_ids(keys).await?;
        Ok(result)
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
            let mut pending = batch.to_vec();
            for attempt in 0..=MAX_BATCH_WRITE_RETRIES {
                let requested = pending;
                let response = self
                    .backend
                    .batch_put_item(self.table.clone(), requested.clone())
                    .await
                    .map_err(|e| DynamoCalloutError::with_debug(&e))?;
                pending = unprocessed_put_items(&response, &self.table)?;
                let pending_ids = pending
                    .iter()
                    .map(PkSk::from_map)
                    .collect::<Result<HashSet<_>, _>>()?;
                for item in requested {
                    if !pending_ids.contains(&PkSk::from_map(&item)?) {
                        self.consistency_overlay.record_put(&self.table, item);
                    }
                }
                if pending.is_empty() {
                    break;
                }
                if attempt == MAX_BATCH_WRITE_RETRIES {
                    return Err(DynamoBatchWriteRetriesExhausted::new(
                        "put",
                        pending.len(),
                        MAX_BATCH_WRITE_RETRIES,
                    ));
                }
                wait_before_batch_retry(attempt).await;
            }
        }
        Ok(())
    }
}

// Helpers.
// ----------------------------------------------------------------------------

fn reject_phantom_objects<T: DynamoObject>() -> Result<(), ServerError> {
    if matches!(T::id_logic(), IdLogic::Phantom) {
        return Err(DynamoInvalidPhantomObjectUsage::new());
    }
    Ok(())
}

fn reject_batch_optimized_ids<T: DynamoObject>() -> Result<(), ServerError> {
    if matches!(T::id_logic(), IdLogic::BatchOptimized { .. }) {
        return Err(DynamoInvalidBatchOptimizedIdUsage::new());
    }
    Ok(())
}
