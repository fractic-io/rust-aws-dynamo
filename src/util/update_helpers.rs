use std::fmt::{self, Write as _};

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

/// Trait to sanitize a string for use as Dynamo update expression attribute
/// name (placeholder).
pub trait Sanitize {
    fn sanitized(&self) -> Sanitized<'_>;
}

impl Sanitize for str {
    fn sanitized(&self) -> Sanitized<'_> {
        Sanitized {
            inner: self.chars(),
        }
    }
}

impl Sanitize for String {
    fn sanitized(&self) -> Sanitized<'_> {
        Sanitized {
            inner: self.chars(),
        }
    }
}

pub struct Sanitized<'a> {
    inner: std::str::Chars<'a>,
}

impl<'a> fmt::Display for Sanitized<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut chars = self.inner.clone(); // Clone iterator to avoid borrowing issues
        while let Some(c) = chars.next() {
            if c.is_alphanumeric() {
                f.write_char(c)?;
            } else {
                f.write_char('_')?;
            }
        }
        Ok(())
    }
}
