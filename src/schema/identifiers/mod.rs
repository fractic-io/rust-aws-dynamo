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
//! Ext-partition rows add a final `+N`.
//!
//! [`SortKey`] is intentionally a raw/tolerant view. Strict identifier logic
//! should call [`SortKey::parse`] once and retain the returned parsed value.

mod generation;
mod placement;
mod relations;
mod sort_key;

#[cfg(test)]
mod tests;

pub(crate) use generation::{
    generate_id, generate_id_with_options, regenerate_timestamp, regenerate_uuid,
    IdGenerationOptions,
};
pub(crate) use placement::{place_for, place_object, IdPlacement, ROOT_KEY};
pub(crate) use relations::validate_parent_relation;
pub(crate) use sort_key::{SortKey, TerminalKind};
