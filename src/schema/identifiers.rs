//! Internal model and rules for DynamoDB identifiers.
//!
//! The crate uses two terminal sort-key forms:
//!
//! | ID logic | Terminal sort-key component |
//! | --- | --- |
//! | UUID, timestamp, batch | `LABEL#value` |
//! | Singleton | `@LABEL` |
//! | Indexed singleton | `@LABEL[key]` |
//!
//! Inline children append that terminal component to the parent's sort key,
//! while top-level children use the parent's sort key as their partition key.
//! Ext-partition rows add a final `+N`, which [`SortKey::logical`] removes.
//!
//! Most code should work with [`PkSk`] and [`SortKey`] rather than assembling or
//! dissecting these strings itself.

use std::fmt;

use fractic_server_error::{CriticalError, ServerError};

use crate::errors::{DynamoInvalidId, DynamoInvalidParent};

use super::{DynamoObject, IdLogic, NestingLogic, PkSk};

pub(crate) const ROOT_KEY: &str = "ROOT";

const ID_VALUE_WIDTH: usize = 16;
const MAX_TIMESTAMP_ID: i64 = 9_999_999_999_999_999;
const BASE62_ALPHABET: &[u8; 62] =
    b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

// Sort-key grammar.
// -----------------------------------------------------------------------------

/// A borrowed DynamoDB sort key with operations that understand this crate's
/// ID grammar.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SortKey<'a> {
    raw: &'a str,
}

impl<'a> SortKey<'a> {
    pub(crate) fn new(raw: &'a str) -> Self {
        Self { raw }
    }

    /// Returns the logical object sort key, without a physical ext-partition
    /// suffix such as `+0` or `+12`.
    pub(crate) fn logical(self) -> &'a str {
        split_ext_suffix(self.raw).0
    }

    /// Returns the physical ext-partition index, when the key ends in a valid
    /// `+N` suffix representable as a `usize`.
    pub(crate) fn ext_partition_index(self) -> Option<usize> {
        split_ext_suffix(self.raw).1
    }

    /// Whether the terminal object uses singleton syntax.
    pub(crate) fn is_singleton(self) -> bool {
        self.logical().contains('@')
    }

    /// Returns the terminal syntax used by this sort key.
    pub(crate) fn terminal_kind(self) -> Result<TerminalKind, ServerError> {
        let sk = self.logical();
        let Some(singleton_start) = sk.find('@') else {
            self.object_label()?;
            return Ok(TerminalKind::Regular);
        };
        let singleton = &sk[singleton_start + 1..];
        singleton_label(singleton, sk)?;
        if singleton.contains('[') {
            Ok(TerminalKind::IndexedSingleton)
        } else {
            Ok(TerminalKind::Singleton)
        }
    }

    /// Returns the label of the terminal object represented by this sort key.
    pub(crate) fn object_label(self) -> Result<&'a str, ServerError> {
        let sk = self.logical();
        if let Some(singleton_start) = sk.find('@') {
            return singleton_label(&sk[singleton_start + 1..], sk);
        }

        let Some((prefix, value)) = sk.rsplit_once('#') else {
            return Err(invalid_sort_key("expected `LABEL#value`", sk));
        };
        if value.is_empty() {
            return Err(invalid_sort_key("terminal ID value was empty", sk));
        }

        let label = prefix.rsplit_once('#').map_or(prefix, |(_, label)| label);
        validate_parsed_label(label, sk)?;
        Ok(label)
    }

    /// Returns the terminal object's self-contained sort-key component.
    ///
    /// For example, `PARENT#1#CHILD#2` becomes `CHILD#2`, while
    /// `PARENT#1@SETTINGS[key]` becomes `@SETTINGS[key]`.
    pub(crate) fn terminal_component(self, expected_label: &str) -> Result<&'a str, ServerError> {
        let sk = self.logical();
        if self.object_label()? != expected_label {
            return Err(DynamoInvalidId::with_debug(
                "sort-key label did not match the expected object label",
                &sk,
            ));
        }
        if let Some(singleton_start) = sk.find('@') {
            return Ok(&sk[singleton_start..]);
        }

        let (prefix, _) = sk
            .rsplit_once('#')
            .ok_or_else(|| invalid_sort_key("expected `LABEL#value`", sk))?;
        Ok(prefix
            .rsplit_once('#')
            .map_or(sk, |(ancestor, _)| &sk[ancestor.len() + 1..]))
    }

    /// Returns the minimal value stored by [`super::ForeignRef`].
    ///
    /// This intentionally remains tolerant of legacy malformed values because
    /// foreign references have historically been normalized during reads.
    pub(crate) fn foreign_ref_value(self) -> &'a str {
        let sk = self.logical();
        if let Some(singleton_start) = sk.find('@') {
            let singleton = &sk[singleton_start + 1..];
            if let (Some(open), Some(close)) = (singleton.find('['), singleton.rfind(']')) {
                if open < close {
                    return &singleton[open + 1..close];
                }
            }
            return "";
        }
        sk.rsplit_once('#').map_or(sk, |(_, value)| value)
    }

    /// Returns this child's logical sort-key path relative to its parent.
    pub(crate) fn relative_to(self, parent: SortKey<'_>) -> Result<&'a str, ServerError> {
        self.logical()
            .strip_prefix(parent.logical())
            .filter(|relative| relative.starts_with('#') || relative.starts_with('@'))
            .ok_or_else(|| {
                DynamoInvalidId::with_debug(
                    "child sort key did not start with its parent sort key",
                    &self.raw,
                )
            })
    }

    /// Replaces a non-singleton terminal ID value while retaining its label and
    /// ancestor path. Singleton IDs are stable and are returned unchanged.
    fn with_terminal_value(self, terminal_value: &str) -> Result<String, ServerError> {
        let sk = self.logical();
        self.object_label()?;
        if self.is_singleton() {
            return Ok(sk.to_string());
        }
        let (prefix, _) = sk
            .rsplit_once('#')
            .ok_or_else(|| invalid_sort_key("expected `LABEL#value`", sk))?;
        Ok(format!("{prefix}#{terminal_value}"))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TerminalKind {
    Regular,
    Singleton,
    IndexedSingleton,
}

// ID generation.
// -----------------------------------------------------------------------------

/// Controls deterministic ID generation used by batch operations and tests.
#[derive(Default)]
pub(crate) struct IdGenerationOptions {
    pub timestamp_millis: Option<i64>,
}

/// Generates a logical ID for a new object.
pub(crate) fn generate_id<T: DynamoObject>(
    data: &T::Data,
    parent: &PkSk,
) -> Result<PkSk, ServerError> {
    generate_id_with_options::<T>(data, parent, IdGenerationOptions::default())
}

/// Generates a logical ID, optionally using a caller-supplied timestamp.
pub(crate) fn generate_id_with_options<T: DynamoObject>(
    data: &T::Data,
    parent: &PkSk,
    options: IdGenerationOptions,
) -> Result<PkSk, ServerError> {
    validate_parent_relation::<T>(parent)
        .map_err(|error| DynamoInvalidParent::new(&error.to_string()))?;
    validate_configured_label(T::id_label())?;

    let object_sk =
        match T::id_logic() {
            IdLogic::Phantom => return Err(CriticalError::new(
                "IDs for IdLogic::Phantom must be constructed manually; phantom objects cannot be \
                 persisted",
            )),
            IdLogic::Uuid => format!("{}#{}", T::id_label(), new_uuid_value()),
            IdLogic::Timestamp => {
                let timestamp = options
                    .timestamp_millis
                    .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
                format!("{}#{}", T::id_label(), timestamp_value(timestamp)?)
            }
            IdLogic::Singleton | IdLogic::SingletonExt => format!("@{}", T::id_label()),
            IdLogic::IndexedSingleton(key) | IdLogic::IndexedSingletonExt(key) => {
                format!("@{}[{}]", T::id_label(), key(data))
            }
            IdLogic::BatchOptimized { .. } => {
                return Err(CriticalError::new(
                    "IDs for IdLogic::BatchOptimized are generated by \
                 DynamoUtil::batch_replace_all_ordered; regular ID generation was unexpectedly \
                 requested",
                ))
            }
        };

    Ok(place_for::<T>(parent, &object_sk))
}

// Placement.
// -----------------------------------------------------------------------------

/// The three ways an object's terminal sort key can be placed in DynamoDB.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IdPlacement {
    Root,
    TopLevel,
    Inline,
}

/// Places an object's sort-key component using `T`'s declared nesting logic.
pub(crate) fn place_for<T: DynamoObject>(parent: &PkSk, object_sk: &str) -> PkSk {
    place_object(parent, object_sk, placement_for(T::nesting_logic()))
}

/// Places a logical terminal or relative sort key according to the requested
/// storage relationship.
pub(crate) fn place_object(parent: &PkSk, object_sk: &str, placement: IdPlacement) -> PkSk {
    let object_sk = SortKey::new(object_sk).logical();
    match placement {
        IdPlacement::Root => PkSk {
            pk: ROOT_KEY.to_string(),
            sk: object_sk.to_string(),
        },
        IdPlacement::TopLevel => PkSk {
            pk: parent.sk.clone(),
            sk: object_sk.to_string(),
        },
        IdPlacement::Inline => {
            let sk = if object_sk.starts_with('#') || object_sk.starts_with('@') {
                format!("{}{object_sk}", parent.sk)
            } else {
                join_inline_child(&parent.sk, object_sk)
            };
            PkSk {
                pk: parent.pk.clone(),
                sk,
            }
        }
    }
}

/// Appends a complete child component to an inline parent's sort key.
pub(crate) fn join_inline_child(parent_sk: &str, child_component: &str) -> String {
    if child_component.starts_with('@') {
        format!("{parent_sk}{child_component}")
    } else {
        format!("{parent_sk}#{child_component}")
    }
}

/// Regenerates the terminal value of a UUID ID.
pub(crate) fn regenerate_uuid(sk: &str) -> Result<String, ServerError> {
    SortKey::new(sk).with_terminal_value(&new_uuid_value())
}

/// Regenerates the terminal value of a timestamp ID.
pub(crate) fn regenerate_timestamp(sk: &str, timestamp_millis: i64) -> Result<String, ServerError> {
    SortKey::new(sk).with_terminal_value(&timestamp_value(timestamp_millis)?)
}

// Parent/child relationships.
// -----------------------------------------------------------------------------

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
        .object_label()
        .map_err(|error| ParentRelationError::InvalidParentId(error.to_string()))?;
    if actual == expected {
        Ok(())
    } else {
        Err(ParentRelationError::WrongObjectType {
            expected,
            actual: actual.to_string(),
        })
    }
}

fn placement_for(nesting: NestingLogic) -> IdPlacement {
    match nesting {
        NestingLogic::Root => IdPlacement::Root,
        NestingLogic::TopLevelChildOf(_) | NestingLogic::TopLevelChildOfAny => {
            IdPlacement::TopLevel
        }
        NestingLogic::InlineChildOf(_) | NestingLogic::InlineChildOfAny => IdPlacement::Inline,
    }
}

// Grammar internals.
// -----------------------------------------------------------------------------

fn singleton_label<'a>(singleton: &'a str, full_sk: &str) -> Result<&'a str, ServerError> {
    let label = if let Some(open) = singleton.find('[') {
        if !singleton.ends_with(']') {
            return Err(invalid_sort_key(
                "indexed singleton was missing its closing `]`",
                full_sk,
            ));
        }
        &singleton[..open]
    } else {
        if singleton.contains(']') {
            return Err(invalid_sort_key(
                "singleton contained an unmatched `]`",
                full_sk,
            ));
        }
        singleton
    };
    validate_parsed_label(label, full_sk)?;
    Ok(label)
}

fn validate_parsed_label(label: &str, full_sk: &str) -> Result<(), ServerError> {
    if label.is_empty() {
        return Err(invalid_sort_key("object label was empty", full_sk));
    }
    if label.contains(['#', '@', '[', ']', '+']) {
        return Err(invalid_sort_key(
            "object label contained a reserved ID character",
            full_sk,
        ));
    }
    Ok(())
}

fn validate_configured_label(label: &str) -> Result<(), ServerError> {
    if label.is_empty() || label.contains(['#', '@', '[', ']', '+']) {
        return Err(CriticalError::with_debug(
            "DynamoObject::id_label() must be non-empty and cannot contain `#`, `@`, `[`, `]`, or \
             `+`",
            &label,
        ));
    }
    Ok(())
}

fn invalid_sort_key(message: &str, sk: &str) -> ServerError {
    DynamoInvalidId::with_debug(message, &sk)
}

fn split_ext_suffix(sk: &str) -> (&str, Option<usize>) {
    let Some(digits_start) = sk
        .char_indices()
        .rev()
        .take_while(|(_, character)| character.is_ascii_digit())
        .last()
        .map(|(index, _)| index)
    else {
        return (sk, None);
    };
    if digits_start == 0 || sk.as_bytes().get(digits_start - 1) != Some(&b'+') {
        return (sk, None);
    }
    let Ok(index) = sk[digits_start..].parse::<usize>() else {
        return (sk, None);
    };
    (&sk[..digits_start - 1], Some(index))
}

fn base62_encode(mut value: u128, width: usize) -> String {
    let mut result = vec!['0'; width];
    for position in (0..width).rev() {
        result[position] = BASE62_ALPHABET[(value % 62) as usize] as char;
        value /= 62;
    }
    result.into_iter().collect()
}

fn new_uuid_value() -> String {
    base62_encode(uuid::Uuid::new_v4().as_u128(), ID_VALUE_WIDTH)
}

fn timestamp_value(timestamp_millis: i64) -> Result<String, ServerError> {
    if !(0..=MAX_TIMESTAMP_ID).contains(&timestamp_millis) {
        return Err(CriticalError::with_debug(
            "timestamp ID must be a non-negative value that fits in 16 decimal digits",
            &timestamp_millis,
        ));
    }
    Ok(format!("{timestamp_millis:0ID_VALUE_WIDTH$}"))
}

// Tests.
// ----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use serde::{Deserialize, Serialize};

    use crate::{dynamo_object, schema::PkSk};

    use super::*;

    #[test]
    fn test_base62_encode_16_chars() {
        let encoded = base62_encode(1234567890, 16);
        assert_eq!(encoded.len(), 16);
        // As the length is fixed, padding may occur; ensure the encoded string meets this condition.
        assert!(encoded
            .chars()
            .all(|c| BASE62_ALPHABET.contains(&(c as u8))));
    }

    #[test]
    fn test_base62_encode_20_chars() {
        let encoded = base62_encode(1234567890, 20);
        assert_eq!(encoded.len(), 20);
        // As the length is fixed, padding may occur; ensure the encoded string meets this condition.
        assert!(encoded
            .chars()
            .all(|c| BASE62_ALPHABET.contains(&(c as u8))));
    }

    #[test]
    fn test_generate_uuid() {
        let uuid = new_uuid_value();
        assert_eq!(uuid.len(), 16);
        // Ensure the UUID is base62 encoded.
        assert!(uuid.chars().all(|c| BASE62_ALPHABET.contains(&(c as u8))));
    }

    #[test]
    fn test_generate_timestamp() {
        let timestamp_1 = timestamp_value(chrono::Utc::now().timestamp_millis()).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let timestamp_2 = timestamp_value(chrono::Utc::now().timestamp_millis()).unwrap();
        assert_eq!(timestamp_1.len(), 16);
        assert_eq!(timestamp_2.len(), 16);
        assert!(timestamp_2 > timestamp_1);
    }

    #[test]
    fn test_is_singleton() {
        assert!(!SortKey::new("ORDER#456#ITEM#789").is_singleton());
        assert!(SortKey::new("@SINGLTN").is_singleton());
        assert!(SortKey::new("@SINGLTN+0").is_singleton());
        assert!(SortKey::new("@SINGLTN[KEY]").is_singleton());
        assert!(SortKey::new("@SINGLTN[KEY]+12").is_singleton());
        assert!(SortKey::new("ORDER#56#ITEM#1@SIGNATURE").is_singleton());
    }

    #[test]
    fn test_object_label() {
        assert!(SortKey::new("INVALID").object_label().is_err());
        assert_eq!(SortKey::new("ORDER#456").object_label().unwrap(), "ORDER");
        assert_eq!(
            SortKey::new("ORDER#456#ITEM#789").object_label().unwrap(),
            "ITEM"
        );
        assert_eq!(SortKey::new("ITEM#789").object_label().unwrap(), "ITEM");

        // Singletons:
        assert_eq!(SortKey::new("@SINGLTN").object_label().unwrap(), "SINGLTN");
        assert_eq!(
            SortKey::new("@PREF[user@example.com]")
                .object_label()
                .unwrap(),
            "PREF"
        );
        assert_eq!(
            SortKey::new("ORDER#56#ITEM#1@POST").object_label().unwrap(),
            "POST"
        );
        assert_eq!(
            SortKey::new("ORDER#56#ITEM#1@POST[key]")
                .object_label()
                .unwrap(),
            "POST"
        );
        assert_eq!(
            SortKey::new("@SINGLTN+0").object_label().unwrap(),
            "SINGLTN"
        );
        assert_eq!(
            SortKey::new("@PREF[key]+12").object_label().unwrap(),
            "PREF"
        );

        assert!(SortKey::new("LABEL#").object_label().is_err());
        assert!(SortKey::new("@").object_label().is_err());
        assert!(SortKey::new("@FAMILY[key").object_label().is_err());
        assert!(SortKey::new("@FAMILY]").object_label().is_err());
    }

    #[test]
    fn test_logical_sort_key() {
        assert_eq!(SortKey::new("@SINGLTN").logical(), "@SINGLTN");
        assert_eq!(SortKey::new("@SINGLTN+0").logical(), "@SINGLTN");
        assert_eq!(
            SortKey::new("PARENT#1@FAMILY[key]+12").logical(),
            "PARENT#1@FAMILY[key]"
        );
        let overflowing = format!("@SINGLTN+{}0", usize::MAX);
        assert_eq!(SortKey::new(&overflowing).logical(), overflowing);
    }

    #[test]
    fn test_ext_partition_index() {
        assert_eq!(SortKey::new("@SINGLTN").ext_partition_index(), None);
        assert_eq!(SortKey::new("@SINGLTN+0").ext_partition_index(), Some(0));
        assert_eq!(
            SortKey::new("PARENT#1@FAMILY[key]+12").ext_partition_index(),
            Some(12)
        );
    }

    #[test]
    fn test_join_inline_child() {
        assert_eq!(
            join_inline_child("ORDER#56#ITEM#1", "POST#abc123"),
            "ORDER#56#ITEM#1#POST#abc123"
        );
        assert_eq!(
            join_inline_child("ORDER#56#ITEM#1", "@POST"),
            "ORDER#56#ITEM#1@POST"
        );
    }

    #[test]
    fn test_relocate_stored_ids() {
        assert_eq!(
            SortKey::new("PARENT#old#CHILD#old")
                .terminal_component("CHILD")
                .unwrap(),
            "CHILD#old"
        );
        assert_eq!(
            SortKey::new("PARENT#old@SETTINGS[key]")
                .terminal_component("SETTINGS")
                .unwrap(),
            "@SETTINGS[key]"
        );
        assert_eq!(
            SortKey::new("PARENT#old@SETTINGS[user@example.com]")
                .terminal_component("SETTINGS")
                .unwrap(),
            "@SETTINGS[user@example.com]"
        );
        assert_eq!(
            SortKey::new("PARENT#old#CHILD#old")
                .relative_to(SortKey::new("PARENT#old"))
                .unwrap(),
            "#CHILD#old"
        );

        let parent = PkSk {
            pk: "ROOT".into(),
            sk: "PARENT#new".into(),
        };
        let top = place_object(
            &parent,
            &regenerate_uuid("CHILD#old").unwrap(),
            IdPlacement::TopLevel,
        );
        assert_eq!(top.pk, "PARENT#new");
        assert!(top.sk.starts_with("CHILD#"));
        assert_ne!(top.sk, "CHILD#old");

        let inline = place_object(
            &parent,
            &regenerate_uuid("#CHILD#old").unwrap(),
            IdPlacement::Inline,
        );
        assert_eq!(inline.pk, "ROOT");
        assert!(inline.sk.starts_with("PARENT#new#CHILD#"));
        assert_ne!(inline.sk, "PARENT#new#CHILD#old");

        assert_eq!(regenerate_uuid("@SETTINGS[key]").unwrap(), "@SETTINGS[key]");
        assert_eq!(
            place_object(PkSk::root(), "ROOTOBJ#old", IdPlacement::Root),
            PkSk {
                pk: "ROOT".into(),
                sk: "ROOTOBJ#old".into(),
            }
        );
    }

    // ID generation logic.
    // --------------------------------------------------

    // Test case 1: NestingLogic::Root with IdLogic::Uuid
    #[derive(Debug, Serialize, Deserialize, Default, Clone)]
    pub struct TestObjectRootUuidData {}
    dynamo_object!(
        TestObjectRootUuid,
        TestObjectRootUuidData,
        "TEST",
        IdLogic::Uuid,
        NestingLogic::Root
    );

    #[test]
    fn test_generate_root_uuid() {
        let result =
            generate_id::<TestObjectRootUuid>(&TestObjectRootUuidData {}, PkSk::root()).unwrap();
        assert_eq!(result.pk, "ROOT");
        assert!(result.sk.starts_with("TEST#"));
        assert_eq!(result.sk.len(), "TEST#".len() + 16);
    }

    // Test case 2: NestingLogic::Root with IdLogic::Timestamp
    #[derive(Debug, Serialize, Deserialize, Default, Clone)]
    pub struct TestObjectRootTimestampData {}
    dynamo_object!(
        TestObjectRootTimestamp,
        TestObjectRootTimestampData,
        "TEST",
        IdLogic::Timestamp,
        NestingLogic::Root
    );

    #[test]
    fn test_generate_root_timestamp() {
        let result = generate_id_with_options::<TestObjectRootTimestamp>(
            &TestObjectRootTimestampData {},
            PkSk::root(),
            IdGenerationOptions {
                timestamp_millis: Some(1_234),
            },
        )
        .unwrap();
        assert_eq!(result.pk, "ROOT");
        assert_eq!(result.sk, "TEST#0000000000001234");
    }

    // Test case 3: NestingLogic::TopLevelChildOfAny with IdLogic::Uuid
    #[derive(Debug, Serialize, Deserialize, Default, Clone)]
    pub struct TestObjectTopLevelChildUuidData {}
    dynamo_object!(
        TestObjectTopLevelChildUuid,
        TestObjectTopLevelChildUuidData,
        "TEST",
        IdLogic::Uuid,
        NestingLogic::TopLevelChildOfAny
    );

    #[test]
    fn test_generate_top_level_child_uuid() {
        let parent = PkSk {
            pk: "parent_pk".into(),
            sk: "PARENT#1".into(),
        };
        let result = generate_id::<TestObjectTopLevelChildUuid>(
            &TestObjectTopLevelChildUuidData {},
            &parent,
        )
        .unwrap();
        assert_eq!(result.pk, parent.sk);
        assert!(result.sk.starts_with("TEST#"));
        assert_eq!(result.sk.len(), "TEST#".len() + 16);
    }

    // Test case 4: NestingLogic::InlineChildOfAny with IdLogic::Uuid
    #[derive(Debug, Serialize, Deserialize, Default, Clone)]
    pub struct TestObjectInlineChildUuidData {}
    dynamo_object!(
        TestObjectInlineChildUuid,
        TestObjectInlineChildUuidData,
        "TEST",
        IdLogic::Uuid,
        NestingLogic::InlineChildOfAny
    );

    #[test]
    fn test_generate_inline_child_uuid() {
        let parent = PkSk {
            pk: "parent_pk".into(),
            sk: "PARENT#1".into(),
        };
        let result =
            generate_id::<TestObjectInlineChildUuid>(&TestObjectInlineChildUuidData {}, &parent)
                .unwrap();
        assert_eq!(result.pk, parent.pk);
        assert!(result.sk.starts_with("PARENT#1#TEST#"));
        assert_eq!(result.sk.len(), "PARENT#1#".len() + "TEST#".len() + 16);
    }

    #[test]
    fn test_place_for_uses_child_nesting_logic() {
        let parent = PkSk {
            pk: "parent_partition".into(),
            sk: "PARENT#1".into(),
        };
        assert_eq!(
            place_for::<TestObjectTopLevelChildUuid>(&parent, "@CHILD"),
            PkSk {
                pk: "PARENT#1".into(),
                sk: "@CHILD".into(),
            }
        );
        assert_eq!(
            place_for::<TestObjectInlineChildUuid>(&parent, "@CHILD"),
            PkSk {
                pk: "parent_partition".into(),
                sk: "PARENT#1@CHILD".into(),
            }
        );
    }

    // Test case 5: Singleton parent cannot have children
    #[test]
    fn test_generate_rejects_singleton_parent() {
        let parent = PkSk {
            pk: "ROOT".into(),
            sk: "@PARENT".into(),
        };
        let error = generate_id::<TestObjectTopLevelChildUuid>(
            &TestObjectTopLevelChildUuidData {},
            &parent,
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("singleton objects cannot have children"));
    }

    // Test case 6: NestingLogic::TopLevelChildOf("PARENT") with matching parent type
    #[derive(Debug, Serialize, Deserialize, Default, Clone)]
    pub struct TestObjectTopLevelChildOfParentData {}
    dynamo_object!(
        TestObjectTopLevelChildOfParent,
        TestObjectTopLevelChildOfParentData,
        "CHILD",
        IdLogic::Uuid,
        NestingLogic::TopLevelChildOf("PARENT")
    );

    #[test]
    fn test_generate_top_level_child_of_required_parent() {
        let parent = PkSk {
            pk: "ROOT".into(),
            sk: "PARENT#1234567890123456".into(),
        };
        let result = generate_id::<TestObjectTopLevelChildOfParent>(
            &TestObjectTopLevelChildOfParentData {},
            &parent,
        )
        .unwrap();
        assert_eq!(result.pk, parent.sk);
        assert!(result.sk.starts_with("CHILD#"));
        assert_eq!(result.sk.len(), "CHILD#".len() + 16);
    }

    // Test case 7: NestingLogic::TopLevelChildOf("PARENT") with non-matching parent type
    #[test]
    fn test_generate_rejects_wrong_parent_type() {
        let parent = PkSk {
            pk: "ROOT".into(),
            sk: "NOTPARENT#1234567890123456".into(),
        };
        let error = generate_id::<TestObjectTopLevelChildOfParent>(
            &TestObjectTopLevelChildOfParentData {},
            &parent,
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("parent object type must be 'PARENT', but was 'NOTPARENT'"));
    }

    // Test case 8: IdLogic::Singleton
    #[derive(Debug, Serialize, Deserialize, Default, Clone)]
    pub struct TestObjectSingletonData {}
    dynamo_object!(
        TestObjectSingleton,
        TestObjectSingletonData,
        "SINGLETON",
        IdLogic::Singleton,
        NestingLogic::Root
    );

    #[test]
    fn test_generate_singleton() {
        let result =
            generate_id::<TestObjectSingleton>(&TestObjectSingletonData {}, PkSk::root()).unwrap();
        assert_eq!(result.pk, "ROOT");
        assert_eq!(result.sk, "@SINGLETON");
    }

    #[derive(Debug, Serialize, Deserialize, Default, Clone)]
    pub struct TestObjectSingletonExtData {}
    dynamo_object!(
        TestObjectSingletonExt,
        TestObjectSingletonExtData,
        "SINGLETONEXT",
        IdLogic::SingletonExt,
        NestingLogic::Root
    );

    #[test]
    fn test_generate_singleton_ext() {
        let result =
            generate_id::<TestObjectSingletonExt>(&TestObjectSingletonExtData {}, PkSk::root())
                .unwrap();
        assert_eq!(result.pk, "ROOT");
        assert_eq!(result.sk, "@SINGLETONEXT");
    }

    // Test case 9: IdLogic::IndexedSingleton
    #[derive(Debug, Serialize, Deserialize, Default, Clone)]
    pub struct TestObjectIndexedSingletonData {
        key_field: String,
    }
    dynamo_object!(
        TestObjectIndexedSingleton,
        TestObjectIndexedSingletonData,
        "FAMILY",
        IdLogic::IndexedSingleton(Box::new(|obj: &TestObjectIndexedSingletonData| {
            Cow::Borrowed(&obj.key_field)
        })),
        NestingLogic::Root
    );

    #[test]
    fn test_generate_indexed_singleton() {
        let result = generate_id::<TestObjectIndexedSingleton>(
            &TestObjectIndexedSingletonData {
                key_field: "key123".to_string(),
            },
            PkSk::root(),
        )
        .unwrap();
        assert_eq!(result.pk, "ROOT");
        assert_eq!(result.sk, "@FAMILY[key123]");
    }

    #[derive(Debug, Serialize, Deserialize, Default, Clone)]
    pub struct TestObjectIndexedSingletonExtData {
        key_field: String,
    }
    dynamo_object!(
        TestObjectIndexedSingletonExt,
        TestObjectIndexedSingletonExtData,
        "FAMILYEXT",
        IdLogic::IndexedSingletonExt(Box::new(|obj: &TestObjectIndexedSingletonExtData| {
            Cow::Borrowed(&obj.key_field)
        })),
        NestingLogic::Root
    );

    #[test]
    fn test_generate_indexed_singleton_ext() {
        let result = generate_id::<TestObjectIndexedSingletonExt>(
            &TestObjectIndexedSingletonExtData {
                key_field: "key123".to_string(),
            },
            PkSk::root(),
        )
        .unwrap();
        assert_eq!(result.pk, "ROOT");
        assert_eq!(result.sk, "@FAMILYEXT[key123]");
    }

    #[derive(Debug, Serialize, Deserialize, Default, Clone)]
    pub struct TestObjectInlineSingletonData {}
    dynamo_object!(
        TestObjectInlineSingleton,
        TestObjectInlineSingletonData,
        "INLSINGLETON",
        IdLogic::Singleton,
        NestingLogic::InlineChildOfAny
    );

    #[test]
    fn test_generate_inline_singleton() {
        let parent = PkSk {
            pk: "USER#123".into(),
            sk: "ORDER#56#ITEM#1".into(),
        };
        let result =
            generate_id::<TestObjectInlineSingleton>(&TestObjectInlineSingletonData {}, &parent)
                .unwrap();
        assert_eq!(result.pk, parent.pk);
        assert_eq!(result.sk, "ORDER#56#ITEM#1@INLSINGLETON");
    }

    #[derive(Debug, Serialize, Deserialize, Default, Clone)]
    pub struct TestObjectInlineIndexedSingletonData {
        key_field: String,
    }
    dynamo_object!(
        TestObjectInlineIndexedSingleton,
        TestObjectInlineIndexedSingletonData,
        "INLFAMILY",
        IdLogic::IndexedSingleton(Box::new(|obj: &TestObjectInlineIndexedSingletonData| {
            Cow::Borrowed(&obj.key_field)
        })),
        NestingLogic::InlineChildOfAny
    );

    #[test]
    fn test_generate_inline_indexed_singleton() {
        let parent = PkSk {
            pk: "USER#123".into(),
            sk: "ORDER#56#ITEM#1".into(),
        };
        let result = generate_id::<TestObjectInlineIndexedSingleton>(
            &TestObjectInlineIndexedSingletonData {
                key_field: "key123".to_string(),
            },
            &parent,
        )
        .unwrap();
        assert_eq!(result.pk, parent.pk);
        assert_eq!(result.sk, "ORDER#56#ITEM#1@INLFAMILY[key123]");
    }

    // Test case 10: Invalid parent_sk format
    #[test]
    fn test_generate_rejects_invalid_parent_id() {
        let parent = PkSk {
            pk: "ROOT".into(),
            sk: "INVALIDFORMAT".into(),
        };
        let error = generate_id::<TestObjectTopLevelChildOfParent>(
            &TestObjectTopLevelChildOfParentData {},
            &parent,
        )
        .unwrap_err();
        assert!(error.to_string().contains("parent ID could not be parsed"));
    }

    #[test]
    fn test_generate_rejects_non_root_parent_for_root_object() {
        let parent = PkSk {
            pk: "ROOT".into(),
            sk: "OTHER#1".into(),
        };
        assert!(generate_id::<TestObjectRootUuid>(&TestObjectRootUuidData {}, &parent).is_err());
    }

    #[test]
    fn test_timestamp_value_rejects_out_of_range_values() {
        assert!(timestamp_value(-1).is_err());
        assert!(timestamp_value(MAX_TIMESTAMP_ID + 1).is_err());
    }
}
