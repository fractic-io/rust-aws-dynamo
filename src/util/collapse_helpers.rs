use std::collections::{HashMap, HashSet};

use aws_sdk_dynamodb::types::AttributeValue;
use fractic_server_error::ServerError;
use serde::Serialize;

use crate::{
    errors::{DynamoCalloutError, DynamoInvalidOperation},
    schema::{
        id_calculations::{get_ext_partition_index, strip_ext_partition_suffix},
        parsing::{build_dynamo_map_internal, parse_json_object_to_dynamo_map},
        DynamoObject, IdLogic, PkSk, Timestamp,
    },
    util::{
        expand_helpers::WithMetadataFrom as _, DynamoMap, DynamoUtil, COLLAPSE_RESERVED_KEY,
        AUTO_FIELDS_CREATED_AT, AUTO_FIELDS_SORT, AUTO_FIELDS_TTL, AUTO_FIELDS_UPDATED_AT,
    },
};

pub(crate) const PARTITION_COUNT_KEY: &str = "ext_total_partitions";
const MAX_PARTITION_BYTES: usize = 300 * 1024;

#[derive(Serialize)]
struct CollapsablePartition {
    #[serde(rename = "##")]
    partition: String,
}

#[derive(Serialize)]
struct PartitionPlaceholder {
    ext_total_partitions: usize,
}

pub(crate) struct PartitionWritePlan {
    pub put_items: Vec<DynamoMap>,
    pub stale_delete_ids: Vec<PkSk>,
}

pub(crate) fn is_partitioned_id_logic<T: DynamoObject>() -> bool {
    matches!(
        T::id_logic(),
        IdLogic::SingletonExt | IdLogic::IndexedSingletonExt(_)
    )
}

pub(crate) fn ext_base_id(id: &PkSk) -> PkSk {
    PkSk {
        pk: id.pk.clone(),
        sk: strip_ext_partition_suffix(&id.sk).to_string(),
    }
}

pub(crate) fn build_partition_ids(base_id: &PkSk, total_partitions: usize) -> Vec<PkSk> {
    (0..total_partitions)
        .map(|idx| partition_id(base_id, idx, total_partitions))
        .collect()
}

pub(crate) async fn build_partition_write_plan<T: DynamoObject>(
    util: &DynamoUtil,
    logical_id: &PkSk,
    data: &T::Data,
    sort: Option<f64>,
    ttl: Option<i64>,
) -> Result<PartitionWritePlan, ServerError> {
    let base_id = ext_base_id(logical_id);
    let serialized = serde_json::to_string(data)
        .map_err(|e| DynamoInvalidOperation::with_debug("failed to serialize partitioned item", &e))?;
    let partition_strings = split_json_partitions(&serialized);
    let total_partitions = partition_strings.len();
    let new_partition_ids = build_partition_ids(&base_id, total_partitions);
    let old_partition_total = fetch_existing_partition_total(util, &base_id).await?;
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

    let now = Timestamp::now();
    let mut put_items = Vec::with_capacity(total_partitions + 1);
    put_items.push(build_dynamo_map_internal(
        &PartitionPlaceholder {
            ext_total_partitions: total_partitions,
        },
        Some(base_id.pk.clone()),
        Some(base_id.sk.clone()),
        Some(common_overrides(&now, sort, ttl)),
    )?
    .0);
    for (partition_id, partition) in new_partition_ids.into_iter().zip(partition_strings.into_iter()) {
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

pub(crate) async fn expand_partition_delete_ids<T: DynamoObject>(
    util: &DynamoUtil,
    ids: Vec<PkSk>,
) -> Result<Vec<PkSk>, ServerError> {
    if !is_partitioned_id_logic::<T>() {
        return Ok(ids);
    }

    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for id in ids {
        let base_id = ext_base_id(&id);
        if !seen.insert(base_id.clone()) {
            continue;
        }
        let count = fetch_existing_partition_total(util, &base_id).await?;
        out.push(base_id.clone());
        if let Some(count) = count {
            out.extend(build_partition_ids(&base_id, count));
        }
    }
    Ok(out)
}

pub(crate) fn collapse_partitioned_items(items: Vec<DynamoMap>) -> Result<Vec<DynamoMap>, ServerError> {
    enum OrderedEntry {
        Item(DynamoMap),
        Partitioned(PkSk),
    }

    let mut ordered_entries = Vec::new();
    let mut partition_groups: HashMap<PkSk, Vec<DynamoMap>> = HashMap::new();
    let mut placeholder_groups: HashMap<PkSk, DynamoMap> = HashMap::new();
    let mut seen_partition_groups = HashSet::new();

    for item in items {
        let has_partition = item.contains_key(COLLAPSE_RESERVED_KEY);
        let has_placeholder = item.contains_key(PARTITION_COUNT_KEY);
        if !has_partition && !has_placeholder {
            ordered_entries.push(OrderedEntry::Item(item));
            continue;
        }

        let id = PkSk::from_map(&item)?;
        let base_id = ext_base_id(&id);
        if seen_partition_groups.insert(base_id.clone()) {
            ordered_entries.push(OrderedEntry::Partitioned(base_id.clone()));
        }
        if has_partition {
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
                    DynamoInvalidOperation::new(
                        "partitioned item is missing its ext placeholder row",
                    )
                })?;
                let mut partitions = partition_groups.remove(&base_id).ok_or_else(|| {
                    DynamoInvalidOperation::new(
                        "partitioned item is missing one or more ext partitions",
                    )
                })?;
                partitions.sort_by_key(|item| {
                    PkSk::from_map(item)
                        .ok()
                        .and_then(|id| get_ext_partition_index(&id.sk))
                        .unwrap_or(usize::MAX)
                });

                let expected_total = placeholder
                    .get(PARTITION_COUNT_KEY)
                    .and_then(|v| v.as_n().ok())
                    .ok_or_else(|| {
                        DynamoInvalidOperation::new(
                            "ext placeholder row was missing ext_total_partitions",
                        )
                    })?
                    .parse::<usize>()
                    .map_err(|e| {
                        DynamoInvalidOperation::with_debug(
                            "failed to parse ext_total_partitions",
                            &e,
                        )
                    })?;
                if partitions.len() != expected_total {
                    return Err(DynamoInvalidOperation::new(&format!(
                        "partitioned item '{}' expected {} partitions but query returned {}",
                        base_id, expected_total, partitions.len()
                    )));
                }

                let serialized = partitions
                    .iter()
                    .map(|item| {
                        item.get(COLLAPSE_RESERVED_KEY)
                            .and_then(|v| v.as_s().ok())
                            .map(|v| v.to_string())
                            .ok_or_else(|| {
                                DynamoInvalidOperation::new(
                                    "ext partition row was missing its ## field",
                                )
                            })
                    })
                    .collect::<Result<Vec<_>, ServerError>>()?
                    .join("");
                let mut logical_item = parse_json_object_to_dynamo_map(&serialized)?
                    .with_metadata_from(&placeholder);
                base_id.write_to_map(&mut logical_item);
                collapsed.push(logical_item);
            }
        }
    }

    if !partition_groups.is_empty() || !placeholder_groups.is_empty() {
        return Err(DynamoInvalidOperation::new(
            "partitioned items could not be fully collapsed from query results",
        ));
    }

    Ok(collapsed)
}

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

fn partition_id(base_id: &PkSk, idx: usize, total_partitions: usize) -> PkSk {
    let digits = partition_digits(total_partitions);
    PkSk {
        pk: base_id.pk.clone(),
        sk: format!("{}+{:0digits$}", base_id.sk, idx),
    }
}

fn partition_digits(total_partitions: usize) -> usize {
    match total_partitions {
        0 => 1,
        1 => 1,
        n => (n - 1).ilog10() as usize + 1,
    }
}

async fn fetch_existing_partition_total(
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
            Some(format!("pk, {}", PARTITION_COUNT_KEY)),
        )
        .await
        .map_err(|e| DynamoCalloutError::with_debug(&e))?;

    Ok(response.item.and_then(|item| {
        item.get(PARTITION_COUNT_KEY)
            .and_then(|v| v.as_n().ok())
            .and_then(|v| v.parse::<usize>().ok())
    }))
}
