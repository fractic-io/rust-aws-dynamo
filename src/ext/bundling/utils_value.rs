use fractic_server_error::ServerError;
use serde_json::Value;

use crate::errors::DynamoInvalidBundleValue;

use super::{BundleDataPath, BundleDataPathSegment};

// Private interface.
// ----------------------------------------------------------------------------

pub(crate) fn value_at_path<'a>(root: &'a Value, path: &BundleDataPath) -> Option<&'a Value> {
    path.segments()
        .iter()
        .try_fold(root, |value, segment| match (segment, value) {
            (BundleDataPathSegment::Field(field), Value::Object(map)) => map.get(field),
            (BundleDataPathSegment::Index(index), Value::Array(list)) => list.get(*index),
            _ => None,
        })
}

pub(crate) fn set_value_at_path(
    root: &mut Value,
    path: &BundleDataPath,
    replacement: Value,
) -> Result<(), ServerError> {
    fn descend(
        value: &mut Value,
        path: &[BundleDataPathSegment],
        replacement: Value,
    ) -> Result<(), ServerError> {
        let Some((segment, rest)) = path.split_first() else {
            *value = replacement;
            return Ok(());
        };
        if rest.is_empty() {
            if let BundleDataPathSegment::Field(field) = segment {
                if let Value::Object(map) = value {
                    map.insert(field.clone(), replacement);
                    return Ok(());
                }
            }
        }
        let next = match (segment, value) {
            (BundleDataPathSegment::Field(field), Value::Object(map)) => map.get_mut(field),
            (BundleDataPathSegment::Index(index), Value::Array(list)) => list.get_mut(*index),
            _ => None,
        }
        .ok_or_else(|| {
            DynamoInvalidBundleValue::new("reference path did not match bundled data")
        })?;
        descend(next, rest, replacement)
    }

    descend(root, path.segments(), replacement)
}
