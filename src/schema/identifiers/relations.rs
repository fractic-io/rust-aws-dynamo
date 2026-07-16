use std::fmt;

use crate::schema::{DynamoObject, NestingLogic, PkSk};

use super::sort_key::SortKey;

/// Describes why a candidate parent cannot contain an object type.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ParentRelationError {
    RootRequired,
    SingletonCannotHaveChildren,
    WrongObjectType {
        expected: &'static str,
        actual: String,
    },
    InvalidParentId(String),
}

impl fmt::Display for ParentRelationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RootRequired => formatter
                .write_str("parent ID must be ROOT|ROOT for an object with NestingLogic::Root"),
            Self::SingletonCannotHaveChildren => {
                formatter.write_str("singleton objects cannot have children")
            }
            Self::WrongObjectType { expected, actual } => write!(
                formatter,
                "parent object type must be '{expected}', but was '{actual}'"
            ),
            Self::InvalidParentId(details) => {
                write!(formatter, "parent ID could not be parsed: {details}")
            }
        }
    }
}

/// Checks only the parent/child relationship, without choosing an API-specific
/// error category.
pub(crate) fn validate_parent_relation<T: DynamoObject>(
    parent: &PkSk,
) -> Result<(), ParentRelationError> {
    let nesting = T::nesting_logic();
    if nesting == NestingLogic::Root {
        return if parent == PkSk::root() {
            Ok(())
        } else {
            Err(ParentRelationError::RootRequired)
        };
    }
    if SortKey::new(&parent.sk).is_singleton() {
        return Err(ParentRelationError::SingletonCannotHaveChildren);
    }

    let expected = match nesting {
        NestingLogic::TopLevelChildOf(expected) | NestingLogic::InlineChildOf(expected) => expected,
        NestingLogic::TopLevelChildOfAny | NestingLogic::InlineChildOfAny => return Ok(()),
        NestingLogic::Root => unreachable!("handled above"),
    };
    let actual = SortKey::new(&parent.sk)
        .parse()
        .map_err(|error| ParentRelationError::InvalidParentId(error.to_string()))?
        .object_label();
    if actual == expected {
        Ok(())
    } else {
        Err(ParentRelationError::WrongObjectType {
            expected,
            actual: actual.to_string(),
        })
    }
}
