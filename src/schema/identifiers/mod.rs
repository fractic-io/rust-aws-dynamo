//! Internal model and rules for DynamoDB identifiers.
//!
//! The crate uses three terminal-segment forms:
//!
//! | ID logic | Terminal segment |
//! | --- | --- |
//! | UUID, timestamp, batch | `LABEL#value` |
//! | Singleton | `@LABEL` |
//! | Indexed singleton | `@LABEL[key]` |
//!
//! Inline children append that terminal segment to the parent's ID path,
//! while top-level children use the parent's sort key as their partition key.
//! Ext-partition rows add a final `+N`.
//!
//! [`RawIdPath`] is intentionally a tolerant, unvalidated view. Strict
//! identifier logic should call [`RawIdPath::parse`] once and retain the
//! returned [`ParsedIdPath`].

mod generation;
mod id_path;
mod placement;
mod relations;

#[cfg(test)]
mod tests;

pub(crate) use generation::{
    generate_id, regenerate_timestamp, regenerate_uuid, timestamp_lower_bound,
    timestamp_upper_bound,
};
pub(crate) use id_path::{ParsedIdPath, RawIdPath, TerminalSegmentKind};
pub(crate) use placement::{
    place_terminal_segment, place_terminal_segment_with, IdPlacement, ROOT_KEY,
};
pub(crate) use relations::validate_parent_relation;
