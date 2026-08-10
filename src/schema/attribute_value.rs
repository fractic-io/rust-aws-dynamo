use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use fractic_server_error::ServerError;
use serde::Serialize;
use serde_json::{Map, Value};

use crate::errors::DynamoItemParsingError;

pub type DynamoMap = HashMap<String, AttributeValue>;

/// Converts an object-shaped Serde value into a Dynamo map.
///
/// Null object fields are omitted, matching normal Serde `Option::None`
/// persistence. Explicit nulls inside arrays are retained.
pub(crate) fn serde_value_into_dynamo_map(value: Value) -> Result<DynamoMap, ServerError> {
    let Value::Object(map) = value else {
        return Err(DynamoItemParsingError::new(&format!(
            "can't build DynamoMap from type '{value:?}'"
        )));
    };
    map.into_iter()
        .map(|(key, value)| {
            serde_value_to_attribute_value(value).map(|value| value.map(|value| (key, value)))
        })
        .filter_map(Result::transpose)
        .collect()
}

/// Converts a Dynamo map into an object-shaped Serde value.
///
/// Dynamo null map fields are omitted and null list entries are retained. Set
/// and binary values are rejected because they have no unambiguous JSON form.
pub(crate) fn dynamo_map_to_serde_value(map: &DynamoMap) -> Result<Value, ServerError> {
    Ok(Value::Object(
        map.iter()
            .map(|(key, value)| {
                attribute_value_to_serde_value(value)
                    .map(|value| value.map(|value| (key.clone(), value)))
            })
            .filter_map(Result::transpose)
            .collect::<Result<Map<_, _>, ServerError>>()?,
    ))
}

pub(crate) fn serialize_attribute_value<T: Serialize>(
    value: T,
) -> Result<Option<AttributeValue>, ServerError> {
    let value = serde_json::to_value(value).map_err(|error| {
        DynamoItemParsingError::with_debug("failed to serialize DynamoDB attribute", &error)
    })?;
    serde_value_to_attribute_value(value)
}

pub(crate) fn serde_value_to_attribute_value(
    value: Value,
) -> Result<Option<AttributeValue>, ServerError> {
    match value {
        Value::Null => Ok(None),
        Value::String(s) => Ok(Some(AttributeValue::S(s))),
        Value::Number(n) => Ok(Some(AttributeValue::N(n.to_string()))),
        Value::Bool(b) => Ok(Some(AttributeValue::Bool(b))),
        Value::Object(map) => Ok(Some(AttributeValue::M(
            map.into_iter()
                .map(|(key, value)| {
                    serde_value_to_attribute_value(value)
                        .map(|value| value.map(|value| (key, value)))
                })
                .filter_map(Result::transpose)
                .collect::<Result<HashMap<_, _>, ServerError>>()?,
        ))),
        Value::Array(array) => Ok(Some(AttributeValue::L(
            array
                .into_iter()
                .map(|value| {
                    serde_value_to_attribute_value(value)
                        .map(|value| value.unwrap_or(AttributeValue::Null(true)))
                })
                .collect::<Result<Vec<_>, ServerError>>()?,
        ))),
    }
}

pub(crate) fn attribute_value_to_serde_value(
    value: &AttributeValue,
) -> Result<Option<Value>, ServerError> {
    match value {
        AttributeValue::Null(_) => Ok(None),
        AttributeValue::S(s) => Ok(Some(Value::String(s.clone()))),
        AttributeValue::N(n) => {
            Ok(Some(Value::Number(n.parse().map_err(|e| {
                DynamoItemParsingError::with_debug("failed to parse number", &e)
            })?)))
        }
        AttributeValue::Bool(b) => Ok(Some(Value::Bool(*b))),
        AttributeValue::M(map) => Ok(Some(Value::Object(
            map.iter()
                .map(|(key, value)| {
                    attribute_value_to_serde_value(value)
                        .map(|value| value.map(|value| (key.clone(), value)))
                })
                .filter_map(Result::transpose)
                .collect::<Result<Map<_, _>, ServerError>>()?,
        ))),
        AttributeValue::L(array) => Ok(Some(Value::Array(
            array
                .iter()
                .map(|value| {
                    attribute_value_to_serde_value(value).map(|value| value.unwrap_or(Value::Null))
                })
                .collect::<Result<Vec<_>, ServerError>>()?,
        ))),
        unsupported => Err(DynamoItemParsingError::new(&format!(
            "unsupported AttributeValue type '{unsupported:?}'"
        ))),
    }
}
