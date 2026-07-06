use std::{collections::HashMap, fmt};

use crate::util::NumericOp;

/// Comparison operator for update condition expressions.
#[derive(Debug, Clone, Copy)]
pub enum CmpOp {
    Eq,
    Gt,
    Lt,
    Gte,
    Lte,
}

impl Into<CmpOp> for NumericOp {
    fn into(self) -> CmpOp {
        match self {
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
pub fn add_condition_attribute(
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
