use std::{collections::HashMap, fmt};

use aws_sdk_dynamodb::types::AttributeValue;
use fractic_server_error::{CriticalError, ServerError};

use crate::{
    errors::DynamoInvalidOperation,
    schema::{
        attribute_value::serialize_attribute_value, persistence::build_canonical_data_map,
        DynamoMap, DynamoObject, Timestamp, AUTO_FIELDS_UPDATED_AT,
    },
    util::NumericOp,
};

#[derive(Default)]
pub(super) struct AttributeUpdatePlan {
    pub(super) set: DynamoMap,
    pub(super) remove: Vec<String>,
    pub(super) comparisons: HashMap<String, (AttributeValue, CmpOp)>,
    conditions: Vec<String>,
    expression_names: HashMap<String, String>,
    expression_values: HashMap<String, AttributeValue>,
    next_presence_index: usize,
    has_unchanged_condition: bool,
}

pub(super) struct DynamoUpdateExpression {
    pub(super) update: String,
    pub(super) condition: String,
    pub(super) names: HashMap<String, String>,
    pub(super) values: HashMap<String, AttributeValue>,
}

impl AttributeUpdatePlan {
    pub(super) fn with_condition(condition: impl Into<String>) -> Self {
        Self {
            conditions: vec![condition.into()],
            ..Default::default()
        }
    }

    pub(super) fn add_presence_condition<T: DynamoObject>(
        &mut self,
        field: &str,
        expect_some: bool,
    ) {
        let idx = self.next_presence_index;
        self.next_presence_index += 1;
        let null_type_placeholder = format!(":u{}n", idx + 1);
        self.expression_values.insert(
            null_type_placeholder.clone(),
            AttributeValue::S("NULL".to_string()),
        );
        self.conditions.push(rename_aware_presence_condition::<T>(
            field,
            idx,
            expect_some,
            &null_type_placeholder,
            &mut self.expression_names,
        ));
    }

    pub(super) fn add_unchanged_condition<T: DynamoObject>(
        &mut self,
        updated_at: Option<&Timestamp>,
        fallback_data: &T::Data,
    ) -> Result<(), ServerError> {
        if self.has_unchanged_condition {
            return Err(DynamoInvalidOperation::new(
                "only one unchanged-since condition may be used per update",
            ));
        }
        self.has_unchanged_condition = true;

        if let Some(updated_at) = updated_at {
            let string_value = serialize_attribute_value(updated_at)?.ok_or_else(|| {
                CriticalError::new("updated_at serialized to an absent DynamoDB attribute")
            })?;
            let map_value = AttributeValue::M(HashMap::from([
                (
                    "seconds".to_string(),
                    AttributeValue::N(updated_at.seconds.to_string()),
                ),
                (
                    "nanos".to_string(),
                    AttributeValue::N(updated_at.nanos.to_string()),
                ),
            ]));
            self.expression_names.insert(
                "#unchanged_updated_at".to_string(),
                AUTO_FIELDS_UPDATED_AT.to_string(),
            );
            self.expression_values
                .insert(":unchanged_updated_at_string".to_string(), string_value);
            self.expression_values
                .insert(":unchanged_updated_at_map".to_string(), map_value);
            self.conditions.push(
                "(#unchanged_updated_at = :unchanged_updated_at_string OR \
                 #unchanged_updated_at = :unchanged_updated_at_map)"
                    .to_string(),
            );
        } else {
            let (source_values, source_nulls) = build_canonical_data_map::<T>(fallback_data)?;
            self.comparisons.extend(
                source_values
                    .into_iter()
                    .map(|(key, value)| (key, (value, CmpOp::Eq))),
            );
            for field in source_nulls {
                self.add_presence_condition::<T>(&field, false);
            }
        }
        Ok(())
    }

    pub(super) fn into_expression<T: DynamoObject>(self) -> DynamoUpdateExpression {
        let Self {
            set,
            remove,
            comparisons,
            conditions,
            mut expression_names,
            mut expression_values,
            ..
        } = self;

        let set_expression = if set.is_empty() {
            String::new()
        } else {
            "SET ".to_string()
                + &set
                    .into_iter()
                    .enumerate()
                    .map(|(idx, (key, value))| {
                        let key_placeholder = format!("#k{}", idx + 1);
                        let value_placeholder = format!(":v{}", idx + 1);
                        expression_names.insert(key_placeholder.clone(), key);
                        expression_values.insert(value_placeholder.clone(), value);
                        format!("{key_placeholder} = {value_placeholder}")
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
        };
        let remove_expression = if remove.is_empty() {
            String::new()
        } else {
            "REMOVE ".to_string()
                + &remove
                    .into_iter()
                    .enumerate()
                    .map(|(idx, key)| {
                        let key_placeholder = format!("#rmk{}", idx + 1);
                        expression_names.insert(key_placeholder.clone(), key);
                        key_placeholder
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
        };
        let condition = conditions
            .into_iter()
            .chain(
                comparisons
                    .into_iter()
                    .enumerate()
                    .map(|(idx, (key, (value, op)))| {
                        rename_aware_comparison_condition::<T>(
                            idx,
                            key,
                            value,
                            op,
                            &mut expression_names,
                            &mut expression_values,
                        )
                    }),
            )
            .collect::<Vec<_>>()
            .join(" AND ");

        DynamoUpdateExpression {
            update: format!("{set_expression} {remove_expression}"),
            condition,
            names: expression_names,
            values: expression_values,
        }
    }
}

/// Comparison operator for update condition expressions.
#[derive(Debug, Clone, Copy)]
pub enum CmpOp {
    Eq,
    Gt,
    Lt,
    Gte,
    Lte,
}

impl From<NumericOp> for CmpOp {
    fn from(op: NumericOp) -> Self {
        match op {
            NumericOp::GreaterThan => CmpOp::Gt,
            NumericOp::GreaterThanOrEquals => CmpOp::Gte,
            NumericOp::LessThan => CmpOp::Lt,
            NumericOp::LessThanOrEquals => CmpOp::Lte,
        }
    }
}

impl fmt::Display for CmpOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CmpOp::Eq => write!(f, "="),
            CmpOp::Gt => write!(f, ">"),
            CmpOp::Lt => write!(f, "<"),
            CmpOp::Gte => write!(f, ">="),
            CmpOp::Lte => write!(f, "<="),
        }
    }
}

/// Registers expression attribute-name placeholders for a condition path.
///
/// DynamoDB condition expressions need attribute names to be represented as
/// placeholders when names are reserved words or contain special characters.
/// This helper turns a dotted field path such as `details.address.city` into a
/// placeholder path like `#u1p1.#u1p2.#u1p3` and fills
/// `expression_attribute_names` with the segment mappings.
pub fn add_condition_attribute_path(
    field: &str,
    placeholder_prefix: &str,
    expression_attribute_names: &mut HashMap<String, String>,
) -> String {
    field
        .split('.')
        .enumerate()
        .map(|(j, field_part)| {
            let placeholder = format!("#{placeholder_prefix}{}", j + 1);
            expression_attribute_names.insert(placeholder.clone(), field_part.to_string());
            placeholder
        })
        .collect::<Vec<_>>()
        .join(".")
}

/// Builds a condition that treats a missing attribute or DynamoDB `NULL` value
/// as none.
pub fn field_is_none_condition(path: &str, null_type_placeholder: &str) -> String {
    format!("(attribute_not_exists({path}) OR attribute_type({path}, {null_type_placeholder}))")
}

/// Builds a condition that requires an attribute to exist and not be DynamoDB
/// `NULL`.
pub fn field_is_some_condition(path: &str, null_type_placeholder: &str) -> String {
    format!("(attribute_exists({path}) AND NOT attribute_type({path}, {null_type_placeholder}))")
}

fn rename_aware_comparison_condition<T: DynamoObject>(
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

    let Some(legacy_key) = legacy_field_for_canonical::<T>(&key) else {
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

fn rename_aware_presence_condition<T: DynamoObject>(
    field: &str,
    idx: usize,
    expect_some: bool,
    null_type_placeholder: &str,
    expression_attribute_names: &mut HashMap<String, String>,
) -> String {
    let Some((legacy_key, canonical_key)) = legacy_and_canonical_paths_for_field::<T>(field) else {
        let path = add_condition_attribute_path(
            field,
            &format!("u{}p", idx + 1),
            expression_attribute_names,
        );
        return if expect_some {
            field_is_some_condition(&path, null_type_placeholder)
        } else {
            field_is_none_condition(&path, null_type_placeholder)
        };
    };

    let canonical_path = add_condition_attribute_path(
        &canonical_key,
        &format!("u{}p", idx + 1),
        expression_attribute_names,
    );
    let legacy_path = add_condition_attribute_path(
        &legacy_key,
        &format!("u{}rp", idx + 1),
        expression_attribute_names,
    );
    let canonical_some = field_is_some_condition(&canonical_path, null_type_placeholder);
    let canonical_none = field_is_none_condition(&canonical_path, null_type_placeholder);
    let legacy_some = field_is_some_condition(&legacy_path, null_type_placeholder);
    let legacy_none = field_is_none_condition(&legacy_path, null_type_placeholder);

    if expect_some {
        format!("({canonical_some} OR ({canonical_none} AND {legacy_some}))")
    } else {
        format!("({canonical_none} AND {legacy_none})")
    }
}

fn legacy_field_for_canonical<T: DynamoObject>(canonical_key: &str) -> Option<&'static str> {
    T::renamed_fields()
        .iter()
        .find(|renamed| !renamed.is_noop() && renamed.to == canonical_key)
        .map(|renamed| renamed.from)
}

fn legacy_and_canonical_paths_for_field<T: DynamoObject>(
    canonical_field: &str,
) -> Option<(String, String)> {
    let (canonical_key, suffix) = canonical_field
        .split_once('.')
        .map_or((canonical_field, None), |(key, suffix)| (key, Some(suffix)));
    let legacy_key = legacy_field_for_canonical::<T>(canonical_key)?;
    let with_suffix =
        |key: &str| suffix.map_or_else(|| key.to_string(), |suffix| format!("{key}.{suffix}"));
    Some((with_suffix(legacy_key), with_suffix(canonical_key)))
}
