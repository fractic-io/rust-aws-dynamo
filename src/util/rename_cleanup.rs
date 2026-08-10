use std::collections::{HashMap, HashSet};

use aws_sdk_dynamodb::types::AttributeValue;

use crate::schema::DynamoObject;

/// Adds legacy renamed attributes to the remove list when their canonical
/// attribute is being written or removed.
///
/// This lets an update both write the new attribute name and clean up the old
/// persisted name in the same DynamoDB update expression.
pub fn add_legacy_field_removals<T: DynamoObject>(
    map: &HashMap<String, AttributeValue>,
    null_keys: &mut Vec<String>,
) {
    let mut remove_keys = null_keys.iter().cloned().collect::<HashSet<_>>();
    for renamed in T::renamed_fields() {
        if renamed.is_noop() {
            continue;
        }

        let canonical_is_updated = map.contains_key(renamed.to) || remove_keys.contains(renamed.to);
        if canonical_is_updated
            && !map.contains_key(renamed.from)
            && remove_keys.insert(renamed.from.to_string())
        {
            null_keys.push(renamed.from.to_string());
        }
    }
}
