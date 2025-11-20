use std::fmt;

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
