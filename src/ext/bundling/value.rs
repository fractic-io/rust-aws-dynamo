use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use fractic_server_error::ServerError;
use serde_json::{Map, Number, Value};

use crate::errors::DynamoInvalidOperation;

use super::{BundleValuePath, BundleValuePathSegment};

pub(crate) fn map_to_value(map: &HashMap<String, AttributeValue>) -> Result<Value, ServerError> {
    Ok(Value::Object(
        map.iter()
            .filter(|(key, _)| key.as_str() != "pk" && key.as_str() != "sk")
            .filter_map(|(key, value)| {
                attribute_to_value(value)
                    .transpose()
                    .map(|value| value.map(|value| (key.clone(), value)))
            })
            .collect::<Result<Map<_, _>, ServerError>>()?,
    ))
}

pub(crate) fn value_to_map(value: &Value) -> Result<HashMap<String, AttributeValue>, ServerError> {
    let Value::Object(map) = value else {
        return Err(invalid_value("bundle item data was not an object"));
    };
    map.iter()
        .filter_map(|(key, value)| {
            value_to_attribute(value)
                .transpose()
                .map(|value| value.map(|value| (key.clone(), value)))
        })
        .collect()
}

fn attribute_to_value(value: &AttributeValue) -> Result<Option<Value>, ServerError> {
    Ok(Some(match value {
        AttributeValue::Bool(value) => Value::Bool(*value),
        AttributeValue::L(values) => Value::Array(
            values
                .iter()
                .map(|value| Ok(attribute_to_value(value)?.unwrap_or(Value::Null)))
                .collect::<Result<_, _>>()?,
        ),
        AttributeValue::M(values) => Value::Object(
            values
                .iter()
                .filter_map(|(key, value)| {
                    attribute_to_value(value)
                        .transpose()
                        .map(|value| value.map(|value| (key.clone(), value)))
                })
                .collect::<Result<_, ServerError>>()?,
        ),
        AttributeValue::N(value) => Value::Number(
            value
                .parse::<Number>()
                .map_err(|_| invalid_value("Dynamo number was not valid JSON"))?,
        ),
        AttributeValue::Null(_) => return Ok(None),
        AttributeValue::S(value) => Value::String(value.clone()),
        _ => {
            return Err(invalid_value(
                "binary and set Dynamo values are not supported by bundles",
            ))
        }
    }))
}

fn value_to_attribute(value: &Value) -> Result<Option<AttributeValue>, ServerError> {
    Ok(Some(match value {
        Value::Null => return Ok(None),
        Value::Bool(value) => AttributeValue::Bool(*value),
        Value::Number(value) => AttributeValue::N(value.to_string()),
        Value::String(value) => AttributeValue::S(value.clone()),
        Value::Array(values) => AttributeValue::L(
            values
                .iter()
                .map(|value| Ok(value_to_attribute(value)?.unwrap_or(AttributeValue::Null(true))))
                .collect::<Result<_, _>>()?,
        ),
        Value::Object(values) => AttributeValue::M(
            values
                .iter()
                .filter_map(|(key, value)| {
                    value_to_attribute(value)
                        .transpose()
                        .map(|value| value.map(|value| (key.clone(), value)))
                })
                .collect::<Result<_, ServerError>>()?,
        ),
    }))
}

pub(crate) fn value_at_path<'a>(root: &'a Value, path: &BundleValuePath) -> Option<&'a Value> {
    let mut value = root;
    for segment in &path.0 {
        value = match (segment, value) {
            (BundleValuePathSegment::Field(field), Value::Object(map)) => map.get(field)?,
            (BundleValuePathSegment::Index(index), Value::Array(list)) => list.get(*index)?,
            _ => return None,
        };
    }
    Some(value)
}

pub(crate) fn set_value_at_path(
    root: &mut Value,
    path: &BundleValuePath,
    replacement: Value,
) -> Result<(), ServerError> {
    fn descend(
        value: &mut Value,
        path: &[BundleValuePathSegment],
        replacement: Value,
    ) -> Result<(), ServerError> {
        let Some((segment, rest)) = path.split_first() else {
            *value = replacement;
            return Ok(());
        };
        let next = match (segment, value) {
            (BundleValuePathSegment::Field(field), Value::Object(map)) => map.get_mut(field),
            (BundleValuePathSegment::Index(index), Value::Array(list)) => list.get_mut(*index),
            _ => None,
        }
        .ok_or_else(|| invalid_value("reference path did not match bundled data"))?;
        descend(next, rest, replacement)
    }

    descend(root, &path.0, replacement)
}

fn invalid_value(details: &str) -> ServerError {
    DynamoInvalidOperation::new(&format!("invalid Dynamo bundle value: {details}"))
}
