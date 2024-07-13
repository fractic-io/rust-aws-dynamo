use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use fractic_generic_server_error::{common::CriticalError, GenericServerError};

use crate::{errors::DynamoItemParsingError, schema::DynamoObject, util::DynamoMap};

// Converting between DynamoMap and DynamoObject.
// --------------------------------------------------

pub enum IdKeys {
    CopyFromObject,
    Override(String, String),
    None,
}

pub fn build_dynamo_map<T: DynamoObject>(
    object: &T,
    id_keys: IdKeys,
    overrides: Option<Vec<(impl Into<String>, Box<dyn erased_serde::Serialize>)>>,
) -> Result<DynamoMap, GenericServerError> {
    let dbg_cxt: &'static str = "build_dynamo_map";

    // DynamoObject -> Serde value.
    let json_value = serde_json::to_value(&object).map_err(|e| {
        DynamoItemParsingError::with_debug(dbg_cxt, "failed to serialize object", e.to_string())
    })?;

    // Serde value -> DynamoMap.
    let mut attribute_values: HashMap<String, AttributeValue> = HashMap::new();
    match json_value {
        serde_json::Value::Object(map) => {
            for (key, value) in map.into_iter() {
                if key == "id" {
                    // ID key is handled explicitly to avoid accidental issues,
                    // and properly set pk/sk separately.
                    continue;
                }
                attribute_values.insert(key, serde_value_to_attribute_value(value)?);
            }
        }
        unsupported => {
            return Err(DynamoItemParsingError::with_debug(
                dbg_cxt,
                "can't build DynamoMap from object",
                format!("{:?}", unsupported),
            ))
        }
    }

    // Set ID keys.
    match id_keys {
        IdKeys::Override(pk, sk) => {
            attribute_values.insert("pk".to_string(), AttributeValue::S(pk));
            attribute_values.insert("sk".to_string(), AttributeValue::S(sk));
        }
        IdKeys::CopyFromObject => {
            attribute_values.insert(
                "pk".to_string(),
                AttributeValue::S(
                    object
                        .pk()
                        .ok_or(DynamoItemParsingError::new(dbg_cxt, "object missing pk"))?
                        .to_string(),
                ),
            );
            attribute_values.insert(
                "sk".to_string(),
                AttributeValue::S(
                    object
                        .sk()
                        .ok_or(DynamoItemParsingError::new(dbg_cxt, "object missing sk"))?
                        .to_string(),
                ),
            );
        }
        IdKeys::None => {}
    }

    // Set overrides.
    if let Some(overrides) = overrides {
        for (key, value) in overrides.into_iter() {
            let json_value = serde_json::to_value(&value).map_err(|e| {
                DynamoItemParsingError::with_debug(
                    dbg_cxt,
                    "failed to serialize override object",
                    e.to_string(),
                )
            })?;
            attribute_values.insert(key.into(), serde_value_to_attribute_value(json_value)?);
        }
    }

    Ok(attribute_values)
}

pub fn parse_dynamo_map<T: DynamoObject>(map: &DynamoMap) -> Result<T, GenericServerError> {
    let dbg_cxt: &'static str = "parse_dynamo_map";

    // DynamoMap -> Serde value.
    let mut serde_map: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
    for (key, value) in map.iter() {
        if (key == "pk") || (key == "sk") {
            // ID keys are handled explicitly to avoid accidental issues, and
            // properly pk/sk into id field.
            continue;
        }
        serde_map.insert(key.clone(), attribute_value_to_serde_value(value.clone())?);
    }

    // Set ID key from pk/sk.
    serde_map.insert(
        "id".to_string(),
        match (map.get("pk"), map.get("sk")) {
            (Some(pk), Some(sk)) => serde_json::Value::String(format!(
                "{}|{}",
                pk.as_s()
                    .map_err(|_| CriticalError::new(dbg_cxt, "pk was not string"))?,
                sk.as_s()
                    .map_err(|_| CriticalError::new(dbg_cxt, "sk was not string"))?,
            )),
            _ => serde_json::Value::Null,
        },
    );

    // Serde value -> DynamoObject.
    serde_json::from_value(serde_json::Value::Object(serde_map)).map_err(|e| {
        DynamoItemParsingError::with_debug(
            dbg_cxt,
            "failed to convert from Serde value",
            e.to_string(),
        )
    })
}

// Inner recursive functions.
// --------------------------------------------------

fn serde_value_to_attribute_value(
    value: serde_json::Value,
) -> Result<AttributeValue, GenericServerError> {
    let dbg_cxt: &'static str = "serde_value_to_attribute_value";
    match value {
        serde_json::Value::Null => Ok(AttributeValue::Null(true)),
        serde_json::Value::String(s) => Ok(AttributeValue::S(s)),
        serde_json::Value::Number(n) => Ok(AttributeValue::N(n.to_string())),
        serde_json::Value::Object(map) => {
            let mut attribute_map: HashMap<String, AttributeValue> = HashMap::new();
            for (key, value) in map.into_iter() {
                attribute_map.insert(key, serde_value_to_attribute_value(value)?);
            }
            Ok(AttributeValue::M(attribute_map)) // DynamoDB M type is for Map
        }
        serde_json::Value::Array(array) => {
            let mut attribute_array: Vec<AttributeValue> = Vec::new();
            for value in array.into_iter() {
                attribute_array.push(serde_value_to_attribute_value(value)?);
            }
            Ok(AttributeValue::L(attribute_array)) // DynamoDB L type is for List
        }
        unsupported => Err(DynamoItemParsingError::with_debug(
            dbg_cxt,
            "unsupported serde_json::Value type",
            format!("{:?}", unsupported),
        )),
    }
}

fn attribute_value_to_serde_value(
    value: AttributeValue,
) -> Result<serde_json::Value, GenericServerError> {
    let dbg_cxt: &'static str = "attribute_value_to_serde_value";
    match value {
        AttributeValue::Null(_) => Ok(serde_json::Value::Null),
        AttributeValue::S(s) => Ok(serde_json::Value::String(s)),
        AttributeValue::N(n) => Ok(serde_json::Value::Number(n.parse().map_err(|e| {
            DynamoItemParsingError::with_debug(dbg_cxt, "failed to parse number", format!("{}", e))
        })?)),
        AttributeValue::M(map) => {
            let mut serde_map: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
            for (key, value) in map.into_iter() {
                serde_map.insert(key, attribute_value_to_serde_value(value)?);
            }
            Ok(serde_json::Value::Object(serde_map))
        }
        AttributeValue::L(array) => {
            let mut serde_array: Vec<serde_json::Value> = Vec::new();
            for value in array.into_iter() {
                serde_array.push(attribute_value_to_serde_value(value)?);
            }
            Ok(serde_json::Value::Array(serde_array))
        }
        unsupported => Err(DynamoItemParsingError::with_debug(
            dbg_cxt,
            "unsupported AttributeValue type",
            format!("{:?}", unsupported),
        )),
    }
}

// Tests.
// --------------------------------------------------

// TODO
