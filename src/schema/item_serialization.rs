use aws_sdk_dynamodb::types::AttributeValue;
use fractic_server_error::ServerError;
use serde::Serialize;

use super::{
    attribute_value::{serde_value_to_attribute_value, DynamoMap},
    materialization::{build_materialized_write_plan_against, MaterializedWritePlan},
    DynamoObject, PkSk,
};
use crate::errors::DynamoItemParsingError;

// Public interface.
// ----------------------------------------------------------------------------

pub enum IdKeys {
    CopyFromObject,
    Override(String, String),
    None,
}

pub fn build_dynamo_map_for_new_obj<T: DynamoObject>(
    data: &T::Data,
    pk: String,
    sk: String,
    overrides: Option<Vec<(&str, Box<dyn erased_serde::Serialize>)>>,
) -> Result<DynamoMap, ServerError> {
    let id = PkSk {
        pk: pk.clone(),
        sk: sk.clone(),
    };
    let (mut map, mut nulls) = build_dynamo_map_internal(data, Some(pk), Some(sk), overrides)?;
    build_materialized_write_plan_against::<T>(&id, data, &map, &nulls)?
        .apply_to(&mut map, &mut nulls);
    Ok(map)
}

pub fn build_dynamo_map_for_existing_obj<T: DynamoObject>(
    object: &T,
    id_keys: IdKeys,
    overrides: Option<Vec<(&str, Box<dyn erased_serde::Serialize>)>>,
) -> Result<(DynamoMap, Vec<String>), ServerError> {
    let (pk, sk) = match id_keys {
        IdKeys::Override(pk, sk) => (Some(pk), Some(sk)),
        IdKeys::CopyFromObject => (Some(object.id().pk.clone()), Some(object.id().sk.clone())),
        IdKeys::None => (None, None),
    };
    let (mut map, mut nulls) = build_dynamo_map_internal(object, pk, sk, overrides)?;
    build_materialized_write_plan_against::<T>(object.id(), object.data(), &map, &nulls)?
        .apply_to(&mut map, &mut nulls);
    Ok((map, nulls))
}

// Crate-internal.
// ----------------------------------------------------------------------------

/// Canonical top-level data as persisted, without IDs, automatic metadata, or
/// materialized attributes.
pub(crate) fn build_canonical_data_map<T: DynamoObject>(
    data: &T::Data,
) -> Result<(DynamoMap, Vec<String>), ServerError> {
    build_dynamo_map_internal(data, None, None, None)
}

pub(crate) fn build_materialized_write_plan<T: DynamoObject>(
    id: &PkSk,
    data: &T::Data,
) -> Result<MaterializedWritePlan, ServerError> {
    let (serialized, nulls) = build_canonical_data_map::<T>(data)?;
    build_materialized_write_plan_against::<T>(id, data, &serialized, &nulls)
}

pub(crate) fn build_dynamo_map_internal<T: Serialize>(
    object: &T,
    pk: Option<String>,
    sk: Option<String>,
    overrides: Option<Vec<(&str, Box<dyn erased_serde::Serialize>)>>,
) -> Result<(DynamoMap, Vec<String>), ServerError> {
    let mut skipped_null_keys = Vec::new();
    let json_value = serde_json::to_value(object)
        .map_err(|e| DynamoItemParsingError::with_debug("failed to serialize object", &e))?;

    let mut attribute_values = DynamoMap::new();
    match json_value {
        serde_json::Value::Object(map) => {
            for (key, value) in map {
                if key == "id" {
                    continue;
                }
                if let Some(value) = serde_value_to_attribute_value(value)? {
                    attribute_values.insert(key, value);
                } else {
                    skipped_null_keys.push(key);
                }
            }
        }
        unsupported => {
            return Err(DynamoItemParsingError::new(&format!(
                "can't build DynamoMap from type '{unsupported:?}'"
            )))
        }
    }

    if let Some(pk) = pk {
        attribute_values.insert("pk".to_string(), AttributeValue::S(pk));
    }
    if let Some(sk) = sk {
        attribute_values.insert("sk".to_string(), AttributeValue::S(sk));
    }

    if let Some(overrides) = overrides {
        for (key, value) in overrides {
            let json_value = serde_json::to_value(&value).map_err(|e| {
                DynamoItemParsingError::with_debug("failed to serialize override object", &e)
            })?;
            if let Some(value) = serde_value_to_attribute_value(json_value)? {
                attribute_values.insert(key.into(), value);
            } else {
                skipped_null_keys.push(key.into());
            }
        }
    }

    Ok((attribute_values, skipped_null_keys))
}
