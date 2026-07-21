use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    schema::{DynamoObject, IdLogic, NestingLogic, PkSk},
    util::DynamoInsertPosition,
};

// Definitions.
// ----------------------------------------------------------------------------

/// A stable, database-independent identifier for an item in a bundle.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BundleId {
    pub value: u64,
    pub label: String,
    pub original_sk: String,
}

/// Portable representation of every `IdLogic` variant.
///
/// Keep `from_object` exhaustive: adding an `IdLogic` variant must require an
/// explicit bundling decision at compile time.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum BundleIdLogic {
    #[default]
    UuidV4,
    UuidV7,
    Singleton,
    IndexedSingleton,
    BatchOptimized,
    SingletonExt,
    IndexedSingletonExt,
    Phantom,
}

/// A portable snapshot of one Dynamo object and its stored descendants.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DynamoBundle {
    pub version: u32,
    /// Original logical database placement of the bundle root.
    pub source_root: PkSk,
    pub root: BundleId,
    /// Omitted descendant labels keyed by the owning object label.
    #[serde(default)]
    pub omitted_descendants: BTreeMap<String, BTreeSet<String>>,
    pub items: Vec<DynamoBundleItem>,
}

/// One logical Dynamo object. Batch-optimized records remain opaque values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DynamoBundleItem {
    pub id: BundleId,
    /// ID generation behavior needed when duplicating this object.
    pub id_logic: BundleIdLogic,
    pub parent: Option<BundleId>,
    pub nesting: BundleNesting,
    #[serde(default)]
    pub storage: DynamoBundleStorage,
    /// Object-shaped Serde data without `pk` or `sk`.
    pub data: Value,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DynamoBundleStorage {
    #[default]
    Standard,
    /// A logical item that must be repartitioned on import.
    ExtPartitioned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BundleNesting {
    Root,
    TopLevel,
    Inline,
}

/// A location within the serialized data of a [`DynamoBundleItem`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BundleDataPath(Vec<BundleDataPathSegment>);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImportMode {
    /// Upserts the bundle at its original source placement.
    Merge,
    /// Reconciles the stored subtree at its original source placement.
    Replace,
    /// Always creates a distinct root identity, allowing a different parent.
    New {
        /// Optional destination ordering. Ordered items should always provide
        /// an explicit position; unordered wrappers should leave this unset.
        position: Option<DynamoInsertPosition>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DynamoImportResult {
    pub root_id: PkSk,
    /// Number of logical bundle objects written by the import.
    pub written_objects: usize,
    /// Number of logical stale roots recursively removed by Replace.
    pub deleted_subtree_roots: usize,
    /// Whether the import created a distinct root identity.
    pub created_new: bool,
    pub warnings: Vec<DynamoImportWarning>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DynamoImportWarning {
    ZeroedInTableReference,
    ZeroedOutOfTableReference,
}

// Public interface.
// ----------------------------------------------------------------------------

impl Default for ImportMode {
    fn default() -> Self {
        Self::New { position: None }
    }
}

impl BundleIdLogic {
    pub fn from_object<O: DynamoObject>() -> Self {
        match O::id_logic() {
            IdLogic::UuidV4 => Self::UuidV4,
            IdLogic::UuidV7 => Self::UuidV7,
            IdLogic::Singleton => Self::Singleton,
            IdLogic::IndexedSingleton(_) => Self::IndexedSingleton,
            IdLogic::BatchOptimized { .. } => Self::BatchOptimized,
            IdLogic::SingletonExt => Self::SingletonExt,
            IdLogic::IndexedSingletonExt(_) => Self::IndexedSingletonExt,
            IdLogic::Phantom => Self::Phantom,
        }
    }
}

impl From<NestingLogic> for BundleNesting {
    fn from(nesting: NestingLogic) -> Self {
        match nesting {
            NestingLogic::Root => Self::Root,
            NestingLogic::TopLevelChildOf(_) | NestingLogic::TopLevelChildOfAny => Self::TopLevel,
            NestingLogic::InlineChildOf(_) | NestingLogic::InlineChildOfAny => Self::Inline,
        }
    }
}

impl DynamoBundle {
    /// Version of the first public Dynamo bundle format.
    pub const VERSION: u32 = 1;
}

impl DynamoBundleItem {
    /// Reads a value selected by a bundle reference path.
    pub fn value_at(&self, path: &BundleDataPath) -> Option<&Value> {
        super::utils_value::value_at_path(&self.data, path)
    }
}

impl BundleDataPath {
    /// Selects fields using a dot-separated path such as
    /// `transformation.prompt_template`.
    pub fn dotted(path: impl AsRef<str>) -> Self {
        Self(
            path.as_ref()
                .split('.')
                .map(|field| {
                    assert!(
                        !field.is_empty(),
                        "bundle data paths must contain non-empty dot-separated fields"
                    );
                    BundleDataPathSegment::Field(field.to_owned())
                })
                .collect(),
        )
    }

    pub fn field(field: impl Into<String>) -> Self {
        Self(vec![BundleDataPathSegment::Field(field.into())])
    }

    pub fn then_field(mut self, field: impl Into<String>) -> Self {
        self.0.push(BundleDataPathSegment::Field(field.into()));
        self
    }

    pub fn then_index(mut self, index: usize) -> Self {
        self.0.push(BundleDataPathSegment::Index(index));
        self
    }
}

impl fmt::Display for BundleDataPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for segment in &self.0 {
            match segment {
                BundleDataPathSegment::Field(field) => write!(f, ".{field}")?,
                BundleDataPathSegment::Index(index) => write!(f, "[{index}]")?,
            }
        }
        Ok(())
    }
}

// Private interface.
// ----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BundleDataPathSegment {
    Field(String),
    Index(usize),
}

impl BundleDataPath {
    pub(crate) fn segments(&self) -> &[BundleDataPathSegment] {
        &self.0
    }

    pub(crate) fn is_prefix_of(&self, other: &Self) -> bool {
        other.0.starts_with(&self.0)
    }
}
