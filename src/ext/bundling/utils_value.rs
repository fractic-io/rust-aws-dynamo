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
    *value_at_segments_mut(root, path.segments())? = replacement;
    Ok(())
}

pub(crate) fn upsert_value_at_path(
    root: &mut Value,
    path: &BundleDataPath,
    replacement: Value,
) -> Result<(), ServerError> {
    if let Some((BundleDataPathSegment::Field(field), parent_path)) = path.segments().split_last() {
        if let Value::Object(parent) = value_at_segments_mut(root, parent_path)? {
            parent.insert(field.clone(), replacement);
            return Ok(());
        }
    }
    set_value_at_path(root, path, replacement)
}

fn value_at_segments_mut<'a>(
    root: &'a mut Value,
    path: &[BundleDataPathSegment],
) -> Result<&'a mut Value, ServerError> {
    path.iter()
        .try_fold(root, |value, segment| match (segment, value) {
            (BundleDataPathSegment::Field(field), Value::Object(map)) => map.get_mut(field),
            (BundleDataPathSegment::Index(index), Value::Array(list)) => list.get_mut(*index),
            _ => None,
        })
        .ok_or_else(|| DynamoInvalidBundleValue::new("reference path did not match bundled data"))
}
