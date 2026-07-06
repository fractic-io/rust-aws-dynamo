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

pub fn expression_attribute_path(
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

pub fn field_is_none_condition(path: &str, null_type_placeholder: &str) -> String {
    format!("(attribute_not_exists({path}) OR attribute_type({path}, {null_type_placeholder}))")
}

pub fn field_is_some_condition(path: &str, null_type_placeholder: &str) -> String {
    format!("(attribute_exists({path}) AND NOT attribute_type({path}, {null_type_placeholder}))")
}
