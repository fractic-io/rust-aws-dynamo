use fractic_server_error::ServerError;
use serde_json::Value;

use crate::errors::DynamoInvalidOperation;

use super::{BundleValuePath, BundleValuePathSegment};

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
