use std::collections::{HashMap, HashSet};

use aws_sdk_dynamodb::types::AttributeValue;

use crate::{
    schema::DynamoObject,
    util::{
        add_condition_attribute, field_is_none_condition, field_is_some_condition,
        update_helpers::CmpOp,
    },
};

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

pub fn renamed_field_presence_condition<T: DynamoObject>(
    field: &str,
    idx: usize,
    expect_some: bool,
    null_type_placeholder: &str,
    expression_attribute_names: &mut HashMap<String, String>,
) -> Option<String> {
    let (legacy_key, canonical_key) = renamed_field_for_canonical::<T>(field)?;
    let canonical_path = add_condition_attribute(
        canonical_key,
        &format!("u{}p", idx + 1),
        expression_attribute_names,
    );
    let legacy_path = add_condition_attribute(
        legacy_key,
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
