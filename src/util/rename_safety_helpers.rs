use std::collections::{HashMap, HashSet};

use aws_sdk_dynamodb::types::AttributeValue;

use crate::{
    schema::DynamoObject,
    util::{
        add_condition_attribute, field_is_none_condition, field_is_some_condition,
        update_helpers::CmpOp,
    },
};

/// Adds legacy renamed attributes to the remove list when their canonical
/// attribute is being written or removed.
///
/// This lets an update both write the new attribute name and clean up the old
/// persisted name in the same DynamoDB update expression.
pub fn add_renamed_field_removals<T: DynamoObject>(
    map: &HashMap<String, AttributeValue>,
    null_keys: &mut Vec<String>,
) {
    let mut remove_keys = null_keys.iter().cloned().collect::<HashSet<_>>();
    for renamed in T::renamed_fields() {
        if renamed.from.is_empty() || renamed.to.is_empty() || renamed.from == renamed.to {
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

/// Builds a comparison condition for an update-time attribute check.
///
/// For regular fields this returns a direct comparison against the canonical
/// attribute. For renamed fields, the generated expression checks the canonical
/// attribute when present and falls back to the legacy attribute only when the
/// canonical attribute is absent or explicitly null.
pub fn renamed_field_attribute_condition<T: DynamoObject>(
    idx: usize,
    key: String,
    value: AttributeValue,
    op: CmpOp,
    expression_attribute_names: &mut HashMap<String, String>,
    expression_attribute_values: &mut HashMap<String, AttributeValue>,
) -> String {
    let key_placeholder = format!("#c{}", idx + 1);
    let value_placeholder = format!(":cv{}", idx + 1);
    expression_attribute_names.insert(key_placeholder.clone(), key.clone());
    expression_attribute_values.insert(value_placeholder.clone(), value);

    let Some((legacy_key, _)) = renamed_field_for_canonical::<T>(&key) else {
        return format!("{} {} {}", key_placeholder, op, value_placeholder);
    };

    let legacy_key_placeholder = format!("#c{}r", idx + 1);
    let null_type_placeholder = format!(":cn{}", idx + 1);
    expression_attribute_names.insert(legacy_key_placeholder.clone(), legacy_key.to_string());
    expression_attribute_values.insert(
        null_type_placeholder.clone(),
        AttributeValue::S("NULL".to_string()),
    );

    let canonical_some = field_is_some_condition(&key_placeholder, &null_type_placeholder);
    let canonical_none = field_is_none_condition(&key_placeholder, &null_type_placeholder);
    format!(
        "(({canonical_some} AND {key_placeholder} {op} {value_placeholder}) OR ({canonical_none} \
         AND {legacy_key_placeholder} {op} {value_placeholder}))",
    )
}

/// Builds a presence condition for a canonical field that may have a legacy
/// persisted name.
///
/// The field may be a top-level attribute or a dotted nested path. Renames are
/// applied only to the top-level path segment because `DynamoFieldRename`
/// represents persisted top-level attribute renames.
pub fn renamed_field_presence_condition<T: DynamoObject>(
    field: &str,
    idx: usize,
    expect_some: bool,
    null_type_placeholder: &str,
    expression_attribute_names: &mut HashMap<String, String>,
) -> Option<String> {
    let (legacy_key, canonical_key) = renamed_field_path_for_canonical::<T>(field)?;
    let canonical_path = add_condition_attribute(
        &canonical_key,
        &format!("u{}p", idx + 1),
        expression_attribute_names,
    );
    let legacy_path = add_condition_attribute(
        &legacy_key,
        &format!("u{}rp", idx + 1),
        expression_attribute_names,
    );
    let canonical_some = field_is_some_condition(&canonical_path, null_type_placeholder);
    let canonical_none = field_is_none_condition(&canonical_path, null_type_placeholder);
    let legacy_some = field_is_some_condition(&legacy_path, null_type_placeholder);
    let legacy_none = field_is_none_condition(&legacy_path, null_type_placeholder);

    Some(match expect_some {
        true => format!("({canonical_some} OR ({canonical_none} AND {legacy_some}))"),
        false => format!("({canonical_none} AND {legacy_none})"),
    })
}

fn renamed_field_for_canonical<T: DynamoObject>(
    canonical_key: &str,
) -> Option<(&'static str, &'static str)> {
    T::renamed_fields()
        .iter()
        .find(|renamed| {
            !renamed.from.is_empty()
                && !renamed.to.is_empty()
                && renamed.from != renamed.to
                && renamed.to == canonical_key
        })
        .map(|renamed| (renamed.from, renamed.to))
}

fn renamed_field_path_for_canonical<T: DynamoObject>(
    canonical_field: &str,
) -> Option<(String, String)> {
    let (first_segment, suffix) = canonical_field
        .split_once('.')
        .map_or((canonical_field, None), |(first_segment, suffix)| {
            (first_segment, Some(suffix))
        });
    let (legacy_key, canonical_key) = renamed_field_for_canonical::<T>(first_segment)?;

    Some(match suffix {
        Some(suffix) => (
            format!("{legacy_key}.{suffix}"),
            format!("{canonical_key}.{suffix}"),
        ),
        None => (legacy_key.to_string(), canonical_key.to_string()),
    })
}
