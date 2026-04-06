use std::collections::{HashMap, HashSet};

use aws_sdk_dynamodb::types::AttributeValue;
use fractic_core::collection;
use fractic_server_error::{CriticalError, ServerError};
use serde_json::Value as JsonValue;

use crate::{
    errors::{DynamoCalloutError, DynamoInvalidOperation},
    schema::{
        id_calculations::{extdata_base_sk, extdata_index_from_sk, get_pk_sk_from_map},
        parsing::{
            build_dynamo_map_for_new_obj, build_dynamo_map_internal, deserialize_dynamo_payload_map,
            serialize_dynamo_payload_map,
        },
        DynamoObject, PkSk, Timestamp,
    },
};

use super::{
    DynamoMap, DynamoQueryMatchType, DynamoUtil, NumericOp, UpdateCondition,
    AUTO_FIELDS_CREATED_AT, AUTO_FIELDS_SORT, AUTO_FIELDS_TTL, AUTO_FIELDS_UPDATED_AT,
    FLATTEN_RESERVED_KEY,
};

pub(crate) const EXTDATA_RESERVED_KEY: &str = "++";
const EXTDATA_CHUNK_BYTES_BUDGET: usize = 350 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ShardGroupKey {
    pk: String,
    base_sk: String,
}

#[derive(Debug, Clone)]
pub(crate) struct ExtDataMetadata {
    pub created_at: Option<Timestamp>,
    pub updated_at: Option<Timestamp>,
    pub sort: Option<f64>,
    pub ttl: Option<i64>,
}

impl ExtDataMetadata {
    pub(crate) fn new(
        created_at: Option<Timestamp>,
        updated_at: Option<Timestamp>,
        sort: Option<f64>,
        ttl: Option<i64>,
    ) -> Self {
        Self {
            created_at,
            updated_at,
            sort,
            ttl,
        }
    }

    fn into_overrides(self) -> Vec<(&'static str, Box<dyn erased_serde::Serialize>)> {
        vec![
            (AUTO_FIELDS_CREATED_AT, Box::new(self.created_at)),
            (AUTO_FIELDS_UPDATED_AT, Box::new(self.updated_at)),
            (AUTO_FIELDS_SORT, Box::new(self.sort)),
            (AUTO_FIELDS_TTL, Box::new(self.ttl)),
        ]
    }
}

pub(crate) struct ExtDataStore<'a> {
    util: &'a DynamoUtil,
}

impl<'a> ExtDataStore<'a> {
    pub(crate) fn new(util: &'a DynamoUtil) -> Self {
        Self { util }
    }

    pub(crate) fn canonical_id(id: &PkSk) -> Result<PkSk, ServerError> {
        let prefix = Self::prefix_id(id)?;
        Ok(PkSk {
            pk: prefix.pk,
            sk: format!("{}0", prefix.sk),
        })
    }

    pub(crate) fn normalize_query(
        id: PkSk,
        match_type: DynamoQueryMatchType,
    ) -> Result<(PkSk, DynamoQueryMatchType), ServerError> {
        if extdata_base_sk(&id.sk).is_some()
            && matches!(
                match_type,
                DynamoQueryMatchType::BeginsWith | DynamoQueryMatchType::Equals
            )
        {
            Ok((Self::prefix_id(&id)?, DynamoQueryMatchType::BeginsWith))
        } else {
            Ok((id, match_type))
        }
    }

    pub(crate) async fn get_logical_map(&self, id: &PkSk) -> Result<Option<DynamoMap>, ServerError> {
        let raw_items = self.raw_items_for_id(id).await?;
        if raw_items.is_empty() {
            return Ok(None);
        }

        let mut logical_items = collapse_query_results(raw_items)?;
        match logical_items.len() {
            1 => Ok(logical_items.pop()),
            _ => Err(DynamoInvalidOperation::new(
                "unexpected ExtData query result; expected exactly one logical object",
            )),
        }
    }

    pub(crate) async fn item_exists(&self, id: &PkSk) -> Result<bool, ServerError> {
        Ok(!self.raw_items_for_id(id).await?.is_empty())
    }

    pub(crate) async fn replace_object<T: DynamoObject>(
        &self,
        id: &PkSk,
        data: &T::Data,
        metadata: ExtDataMetadata,
    ) -> Result<(), ServerError> {
        let storage_items = encode_object::<T>(data, id, metadata)?;
        let existing_ids = self.raw_ids_for_id(id).await?;
        self.util.raw_batch_delete_ids(existing_ids).await?;
        self.util.raw_batch_put_item(storage_items).await
    }

    pub(crate) async fn delete_object(&self, id: &PkSk) -> Result<(), ServerError> {
        let existing_ids = self.raw_ids_for_id(id).await?;
        self.util.raw_batch_delete_ids(existing_ids).await
    }

    pub(crate) async fn raw_ids_for_many(&self, ids: Vec<PkSk>) -> Result<Vec<PkSk>, ServerError> {
        let mut unique = HashSet::new();
        for id in ids {
            unique.extend(self.raw_ids_for_id(&id).await?);
        }
        Ok(unique.into_iter().collect())
    }

    async fn raw_ids_for_id(&self, id: &PkSk) -> Result<Vec<PkSk>, ServerError> {
        self.raw_items_for_id(id)
            .await?
            .into_iter()
            .map(|item| PkSk::from_map(&item))
            .collect()
    }

    async fn raw_items_for_id(&self, id: &PkSk) -> Result<Vec<DynamoMap>, ServerError> {
        let prefix = Self::prefix_id(id)?;
        self.query_raw_prefix(&prefix).await
    }

    async fn query_raw_prefix(&self, id: &PkSk) -> Result<Vec<DynamoMap>, ServerError> {
        let response = self
            .util
            .backend
            .query(
                self.util.table.clone(),
                None,
                "pk = :pk_val AND begins_with(sk, :sk_val)".to_string(),
                collection! {
                    ":pk_val".to_string() => AttributeValue::S(id.pk.clone()),
                    ":sk_val".to_string() => AttributeValue::S(id.sk.clone()),
                },
            )
            .await
            .map_err(|e| DynamoCalloutError::with_debug(&e))?;

        Ok(response
            .into_iter()
            .flat_map(|page| page.items.unwrap_or_default().into_iter())
            .collect())
    }

    fn prefix_id(id: &PkSk) -> Result<PkSk, ServerError> {
        let base_sk = extdata_base_sk(&id.sk).ok_or_else(|| {
            DynamoInvalidOperation::with_debug("ID is not an ExtData object ID", &id.to_string())
        })?;
        Ok(PkSk {
            pk: id.pk.clone(),
            sk: format!("{}+", base_sk),
        })
    }
}

pub(crate) fn collapse_query_results(items: Vec<DynamoMap>) -> Result<Vec<DynamoMap>, ServerError> {
    let mut passthrough = Vec::new();
    let mut shard_groups: HashMap<ShardGroupKey, Vec<(usize, String)>> = HashMap::new();

    for item in items {
        match decode_shard(&item)? {
            Some((group_key, shard_index, payload_piece)) => {
                shard_groups
                    .entry(group_key)
                    .or_default()
                    .push((shard_index, payload_piece));
            }
            None => passthrough.push(item),
        }
    }

    for (group_key, mut shards) in shard_groups {
        shards.sort_by_key(|(index, _)| *index);
        for (expected_index, (actual_index, _)) in shards.iter().enumerate() {
            if *actual_index != expected_index {
                return Err(DynamoInvalidOperation::new(&format!(
                    "malformed ExtData shard set for '{}|{}'; expected shard +{}, got +{}",
                    group_key.pk, group_key.base_sk, expected_index, actual_index
                )));
            }
        }

        let payload = shards
            .into_iter()
            .map(|(_, piece)| piece)
            .collect::<Vec<_>>()
            .join("");
        let mut logical_map = deserialize_dynamo_payload_map(&payload)?;
        validate_payload_map(&logical_map)?;
        logical_map.insert("pk".to_string(), AttributeValue::S(group_key.pk));
        logical_map.insert(
            "sk".to_string(),
            AttributeValue::S(format!("{}+0", group_key.base_sk)),
        );
        passthrough.push(logical_map);
    }

    Ok(passthrough)
}

pub(crate) fn matches_conditions<T: DynamoObject>(
    current: &T::Data,
    conditions: &[UpdateCondition<T>],
) -> Result<bool, ServerError> {
    let current_map = build_dynamo_map_internal(current, None, None, None)?.0;
    let current_json = serde_json::to_value(current)
        .map_err(|e| CriticalError::with_debug("failed to serialize current ExtData object", &e))?;

    for condition in conditions {
        let satisfied = match condition {
            UpdateCondition::PartialEq(partial) => {
                let partial_map = build_dynamo_map_internal(partial, None, None, None)?.0;
                partial_map
                    .into_iter()
                    .all(|(key, value)| current_map.get(&key) == Some(&value))
            }
            UpdateCondition::NumericCompare { partial, map_op } => {
                let partial_map = build_dynamo_map_internal(partial, None, None, None)?.0;
                partial_map.into_iter().all(|(key, value)| {
                    let Some(current_value) = current_map.get(&key) else {
                        return false;
                    };
                    let Ok(current_num) = current_value
                        .as_n()
                        .ok()
                        .and_then(|n| n.parse::<f64>().ok())
                        .ok_or(())
                    else {
                        return false;
                    };
                    let Ok(compare_num) = value
                        .as_n()
                        .ok()
                        .and_then(|n| n.parse::<f64>().ok())
                        .ok_or(())
                    else {
                        return false;
                    };
                    match map_op {
                        NumericOp::GreaterThan => current_num > compare_num,
                        NumericOp::GreaterThanOrEquals => current_num >= compare_num,
                        NumericOp::LessThan => current_num < compare_num,
                        NumericOp::LessThanOrEquals => current_num <= compare_num,
                    }
                })
            }
            UpdateCondition::FieldIsNone(path) => json_at_path(&current_json, path)
                .map(|value| value.is_null())
                .unwrap_or(true),
            UpdateCondition::FieldIsSome(path) => json_at_path(&current_json, path)
                .map(|value| !value.is_null())
                .unwrap_or(false),
        };

        if !satisfied {
            return Ok(false);
        }
    }

    Ok(true)
}

fn encode_object<T: DynamoObject>(
    data: &T::Data,
    id: &PkSk,
    metadata: ExtDataMetadata,
) -> Result<Vec<DynamoMap>, ServerError> {
    let logical_id = ExtDataStore::canonical_id(id)?;
    let logical_map = build_dynamo_map_for_new_obj::<T>(
        data,
        logical_id.pk.clone(),
        logical_id.sk.clone(),
        Some(metadata.into_overrides()),
    )?;
    validate_payload_map(&logical_map)?;
    let payload = serialize_dynamo_payload_map(&logical_map)?;
    let prefix = ExtDataStore::prefix_id(&logical_id)?;

    Ok(split_payload(&payload)
        .into_iter()
        .enumerate()
        .map(|(index, piece)| {
            collection! {
                "pk".to_string() => AttributeValue::S(logical_id.pk.clone()),
                "sk".to_string() => AttributeValue::S(format!("{}{}", prefix.sk, index)),
                EXTDATA_RESERVED_KEY.to_string() => AttributeValue::S(piece),
            }
        })
        .collect())
}

fn decode_shard(item: &DynamoMap) -> Result<Option<(ShardGroupKey, usize, String)>, ServerError> {
    let Some(piece_value) = item.get(EXTDATA_RESERVED_KEY) else {
        return Ok(None);
    };

    if item
        .keys()
        .any(|key| (key != "pk") && (key != "sk") && (key != EXTDATA_RESERVED_KEY))
    {
        return Err(DynamoInvalidOperation::new(
            "malformed ExtData shard contained unexpected fields",
        ));
    }

    let (pk, sk) = get_pk_sk_from_map(item)?;
    let base_sk = extdata_base_sk(sk).ok_or_else(|| {
        DynamoInvalidOperation::with_debug("malformed ExtData shard ID", &sk.to_string())
    })?;
    let index = extdata_index_from_sk(sk).ok_or_else(|| {
        DynamoInvalidOperation::with_debug("malformed ExtData shard index", &sk.to_string())
    })?;
    let piece = piece_value.as_s().map_err(|_| {
        DynamoInvalidOperation::new("malformed ExtData shard payload; expected a string")
    })?;

    Ok(Some((
        ShardGroupKey {
            pk: pk.to_string(),
            base_sk: base_sk.to_string(),
        },
        index,
        piece.to_string(),
    )))
}

fn validate_payload_map(map: &DynamoMap) -> Result<(), ServerError> {
    for reserved_key in [FLATTEN_RESERVED_KEY, EXTDATA_RESERVED_KEY] {
        if map.contains_key(reserved_key) {
            return Err(DynamoInvalidOperation::new(&format!(
                "reserved key '{}' is not allowed in ExtData payloads",
                reserved_key
            )));
        }
    }
    Ok(())
}

fn split_payload(payload: &str) -> Vec<String> {
    if payload.is_empty() {
        return vec![String::new()];
    }

    let mut parts = Vec::new();
    let mut start = 0;
    while start < payload.len() {
        let mut end = usize::min(start + EXTDATA_CHUNK_BYTES_BUDGET, payload.len());
        while !payload.is_char_boundary(end) {
            end -= 1;
        }
        parts.push(payload[start..end].to_string());
        start = end;
    }
    parts
}

fn json_at_path<'a>(value: &'a JsonValue, path: &str) -> Option<&'a JsonValue> {
    let mut current = value;
    for segment in path.split('.') {
        let JsonValue::Object(map) = current else {
            return None;
        };
        current = map.get(segment)?;
    }
    Some(current)
}
