use std::collections::{HashMap, HashSet};

use aws_sdk_dynamodb::types::AttributeValue;
use fractic_server_error::ServerError;
use serde::Serialize;

use crate::{
    errors::{DynamoCalloutError, DynamoInvalidOperation, DynamoInvalidPartitioning},
    schema::{
        id_calculations::{get_ext_index, strip_ext_suffix},
        parsing::{build_dynamo_map_internal, deserialize_dynamo_map_partitions},
        DynamoObject, IdLogic, PkSk, Timestamp,
    },
    util::{
        metadata_helpers::WithMetadataFrom as _, DynamoMap, DynamoUtil, AUTO_FIELDS_CREATED_AT,
        AUTO_FIELDS_SORT, AUTO_FIELDS_TTL, AUTO_FIELDS_UPDATED_AT, COLLAPSE_COUNT_RESERVED_KEY,
        COLLAPSE_RESERVED_KEY,
    },
};

/// Technical limit is 400KB, so use 300KB to leave a buffer for auto-fields.
const MAX_PARTITION_BYTES: usize = 300 * 1024;

// Core structs.
// ----------------------------------------------------------------------------

/// Placeholder stored at logical item's ID to indicate object data is spread
/// across multiple partitions.
///
/// Being able to efficiently check the number of partitions without needing to
/// fetch them all is important for efficient deletes.
#[derive(Serialize)]
struct PartitionedItemPlaceholder {
    #[serde(rename = "#!")]
    num_partitions: usize,
}

/// Partition data. Collapsing the data of all partitions results in the
/// original logical item.
#[derive(Serialize)]
struct CollapsablePartition {
    #[serde(rename = "##")]
    partition: String,
}

// ID logic.
// ----------------------------------------------------------------------------

pub(crate) fn is_partitioned_id_logic<T: DynamoObject>() -> bool {
    matches!(
        T::id_logic(),
        IdLogic::SingletonExt | IdLogic::IndexedSingletonExt(_)
    )
}

/// Logical ID of a partitioned item (i.e. with sk '...+N' suffix).
pub(crate) fn ext_base_id(id: &PkSk) -> PkSk {
    PkSk {
        pk: id.pk.clone(),
        sk: strip_ext_suffix(&id.sk).to_string(),
    }
}

// Read logic.
// ----------------------------------------------------------------------------

/// Fetches the partition placeholder item to check partition count.
async fn fetch_num_partitions(
    util: &DynamoUtil,
    base_id: &PkSk,
) -> Result<Option<usize>, ServerError> {
    let key = HashMap::from([
        ("pk".to_string(), AttributeValue::S(base_id.pk.clone())),
        ("sk".to_string(), AttributeValue::S(base_id.sk.clone())),
    ]);
    let response = util
        .backend
        .get_item(
            util.table.clone(),
            key,
            Some(format!("pk, {}", COLLAPSE_COUNT_RESERVED_KEY)),
        )
        .await
        .map_err(|e| DynamoCalloutError::with_debug(&e))?;

    Ok(response.item.and_then(|item| {
        item.get(COLLAPSE_COUNT_RESERVED_KEY)
            .and_then(|v| v.as_n().ok())
            .and_then(|v| v.parse::<usize>().ok())
    }))
}

/// Batch version of `fetch_num_partitions`.
async fn fetch_num_partitions_batch(
    util: &DynamoUtil,
    base_ids: &[PkSk],
) -> Result<HashMap<PkSk, usize>, ServerError> {
    let items = util
        .raw_batch_get_ids(
            base_ids.to_vec(),
            Some(format!("pk, sk, {}", COLLAPSE_COUNT_RESERVED_KEY)),
        )
        .await?;
    let mut counts = HashMap::new();
    for item in items {
        let id = PkSk::from_map(&item)?;
        let Some(count) = item
            .get(COLLAPSE_COUNT_RESERVED_KEY)
            .and_then(|v| v.as_n().ok())
            .and_then(|v| v.parse::<usize>().ok())
        else {
            continue;
        };
        counts.insert(id, count);
    }
    Ok(counts)
}

/// Collapses queried placeholder and partition rows back into their logical
/// items while preserving the original query order.
///
/// Returns an error when a partitioned item's placeholder or partitions are
/// incomplete or malformed in a way that should be impossible for valid data.
pub(crate) fn collapse_partitioned_items(
    items: Vec<DynamoMap>,
) -> Result<Vec<DynamoMap>, ServerError> {
    enum OrderedEntry {
        Item(DynamoMap),
        Partitioned(PkSk),
    }

    let mut ordered_entries = Vec::new();
    let mut partition_groups: HashMap<PkSk, Vec<DynamoMap>> = HashMap::new();
    let mut placeholder_groups: HashMap<PkSk, DynamoMap> = HashMap::new();
    let mut seen_partition_groups = HashSet::new();

    for item in items {
        let is_partition = item.contains_key(COLLAPSE_RESERVED_KEY);
        let is_placeholder = item.contains_key(COLLAPSE_COUNT_RESERVED_KEY);
        if !is_partition && !is_placeholder {
            ordered_entries.push(OrderedEntry::Item(item));
            continue;
        }

        let id = PkSk::from_map(&item)?;
        let base_id = ext_base_id(&id);
        if seen_partition_groups.insert(base_id.clone()) {
            ordered_entries.push(OrderedEntry::Partitioned(base_id.clone()));
        }
        if is_partition {
            partition_groups.entry(base_id).or_default().push(item);
        } else {
            placeholder_groups.insert(base_id, item);
        }
    }

    let mut collapsed = Vec::new();
    for entry in ordered_entries {
        match entry {
            OrderedEntry::Item(item) => collapsed.push(item),
            OrderedEntry::Partitioned(base_id) => {
                let placeholder = placeholder_groups.remove(&base_id).ok_or_else(|| {
                    DynamoInvalidPartitioning::new(
                        "partitioned item is missing its ext placeholder row",
                    )
                })?;
                let mut partitions = partition_groups.remove(&base_id).ok_or_else(|| {
                    DynamoInvalidPartitioning::new(
                        "partitioned item is missing one or more ext partitions",
                    )
                })?;
                partitions.sort_by_key(|item| {
                    PkSk::from_map(item)
                        .ok()
                        .and_then(|id| get_ext_index(&id.sk))
                        .unwrap_or(usize::MAX)
                });

                let expected_total = placeholder
                    .get(COLLAPSE_COUNT_RESERVED_KEY)
                    .and_then(|v| v.as_n().ok())
                    .ok_or_else(|| {
                        DynamoInvalidPartitioning::new(
                            "ext placeholder row was missing partition count",
                        )
                    })?
                    .parse::<usize>()
                    .map_err(|e| {
                        DynamoInvalidPartitioning::with_debug("failed to parse partition count", &e)
                    })?;
                if partitions.len() != expected_total {
                    return Err(DynamoInvalidPartitioning::new(&format!(
                        "partitioned item '{}' expected {} partitions but query returned {}",
                        base_id,
                        expected_total,
                        partitions.len()
                    )));
                }

                let serialized_partitions = partitions
                    .iter()
                    .map(|item| {
                        item.get(COLLAPSE_RESERVED_KEY)
                            .and_then(|v| v.as_s().ok())
                            .map(|v| v.to_string())
                            .ok_or_else(|| {
                                DynamoInvalidPartitioning::new(
                                    "ext partition row was missing its data field",
                                )
                            })
                    })
                    .collect::<Result<Vec<_>, ServerError>>()?;
                let mut logical_item = deserialize_dynamo_map_partitions(serialized_partitions)?
                    .with_metadata_from(&placeholder);
                base_id.write_to_map(&mut logical_item);
                collapsed.push(logical_item);
            }
        }
    }

    if !partition_groups.is_empty() || !placeholder_groups.is_empty() {
        return Err(DynamoInvalidPartitioning::new(
            "partitioned items could not be fully collapsed from query results",
        ));
    }

    Ok(collapsed)
}

// Write logic.
// ----------------------------------------------------------------------------

pub(crate) struct PartitionWritePlan {
    /// New items to write (includes partition placeholder and all partitions).
    pub put_items: Vec<DynamoMap>,

    /// List of lingering items that will not be cleanly overwritten by our new
    /// put items. These should be deleted to perform a clean overwrite.
    pub stale_delete_ids: Vec<PkSk>,
}

fn split_json_partitions(serialized: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut part_start = 0;
    let mut part_bytes = 0;

    for (idx, ch) in serialized.char_indices() {
        let char_bytes = ch.len_utf8();
        if part_bytes > 0 && part_bytes + char_bytes > MAX_PARTITION_BYTES {
            parts.push(serialized[part_start..idx].to_string());
            part_start = idx;
            part_bytes = 0;
        }
        part_bytes += char_bytes;
    }

    parts.push(serialized[part_start..].to_string());
    parts
}

/// Auto-field overrides written to partition placeholder and all partition
/// items. That way fields like TTL will still take effect consistently.
fn common_overrides(
    now: &Timestamp,
    sort: Option<f64>,
    ttl: Option<i64>,
) -> Vec<(&'static str, Box<dyn erased_serde::Serialize>)> {
    vec![
        (AUTO_FIELDS_CREATED_AT, Box::new(now.clone())),
        (AUTO_FIELDS_UPDATED_AT, Box::new(now.clone())),
        (AUTO_FIELDS_SORT, Box::new(sort)),
        (AUTO_FIELDS_TTL, Box::new(ttl)),
    ]
}

/// Uses the minimum number of digits to represent all partition indices.
fn build_partition_ids(base_id: &PkSk, total_partitions: usize) -> Vec<PkSk> {
    fn partition_digits(total_partitions: usize) -> usize {
        match total_partitions {
            0 => 1,
            1 => 1,
            n => (n - 1).ilog10() as usize + 1,
        }
    }

    fn partition_id(base_id: &PkSk, idx: usize, total_partitions: usize) -> PkSk {
        let digits = partition_digits(total_partitions);
        PkSk {
            pk: base_id.pk.clone(),
            sk: format!("{}+{:0digits$}", base_id.sk, idx),
        }
    }

    (0..total_partitions)
        .map(|idx| partition_id(base_id, idx, total_partitions))
        .collect()
}

/// NOTE: Internally runs a DB fetch to check existing partition count (if any).
pub(crate) async fn build_partition_write_plan<T: DynamoObject>(
    util: &DynamoUtil,
    logical_id: &PkSk,
    data: &T::Data,
    sort: Option<f64>,
    ttl: Option<i64>,
) -> Result<PartitionWritePlan, ServerError> {
    let base_id = ext_base_id(logical_id);
    let serialized = serde_json::to_string(data).map_err(|e| {
        DynamoInvalidOperation::with_debug("failed to serialize partitioned item", &e)
    })?;
    let partition_strings = split_json_partitions(&serialized);
    let total_partitions = partition_strings.len();
    let new_partition_ids = build_partition_ids(&base_id, total_partitions);
    let old_partition_total = fetch_num_partitions(util, &base_id).await?;
    let stale_delete_ids = old_partition_total
        .map(|old_total| {
            let new_partition_sks = new_partition_ids
                .iter()
                .map(|id| id.sk.as_str())
                .collect::<HashSet<_>>();
            build_partition_ids(&base_id, old_total)
                .into_iter()
                .filter(|id| !new_partition_sks.contains(id.sk.as_str()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    // Build partition placeholder.
    let now = Timestamp::now();
    let mut put_items = Vec::with_capacity(total_partitions + 1);
    put_items.push(
        build_dynamo_map_internal(
            &PartitionedItemPlaceholder {
                num_partitions: total_partitions,
            },
            Some(base_id.pk.clone()),
            Some(base_id.sk.clone()),
            Some(common_overrides(&now, sort, ttl)),
        )?
        .0,
    );

    // Build partition items.
    for (partition_id, partition) in new_partition_ids
        .into_iter()
        .zip(partition_strings.into_iter())
    {
        put_items.push(
            build_dynamo_map_internal(
                &CollapsablePartition { partition },
                Some(base_id.pk.clone()),
                Some(partition_id.sk),
                Some(common_overrides(&now, sort, ttl)),
            )?
            .0,
        );
    }

    Ok(PartitionWritePlan {
        put_items,
        stale_delete_ids,
    })
}

// Delete logic.
// ----------------------------------------------------------------------------

/// Expands each logical ID into the placeholder row plus any partition rows so
/// deleting a partitioned item removes the entire stored representation.
pub(crate) async fn expand_partition_delete_ids<T: DynamoObject>(
    util: &DynamoUtil,
    ids: Vec<PkSk>,
) -> Result<Vec<PkSk>, ServerError> {
    if !is_partitioned_id_logic::<T>() {
        return Ok(ids);
    }

    let mut base_ids = Vec::new();
    let mut seen = HashSet::new();
    for id in ids {
        let base_id = ext_base_id(&id);
        if !seen.insert(base_id.clone()) {
            continue;
        }
        base_ids.push(base_id);
    }

    let partition_counts = fetch_num_partitions_batch(util, &base_ids).await?;
    let mut out = Vec::new();
    for base_id in base_ids {
        out.push(base_id.clone());
        if let Some(count) = partition_counts.get(&base_id).copied() {
            out.extend(build_partition_ids(&base_id, count));
        }
    }
    Ok(out)
}
