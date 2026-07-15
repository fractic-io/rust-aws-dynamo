use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};

use fractic_server_error::ServerError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::schema::{DynamoObject, IdLogic, PkSk};

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
    Uuid,
    Timestamp,
    Singleton,
    IndexedSingleton,
    BatchOptimized,
    SingletonExt,
    IndexedSingletonExt,
    Phantom,
}

impl BundleIdLogic {
    pub fn from_object<O: DynamoObject>() -> Self {
        match O::id_logic() {
            IdLogic::Uuid => Self::Uuid,
            IdLogic::Timestamp => Self::Timestamp,
            IdLogic::Singleton => Self::Singleton,
            IdLogic::IndexedSingleton(_) => Self::IndexedSingleton,
            IdLogic::BatchOptimized { .. } => Self::BatchOptimized,
            IdLogic::SingletonExt => Self::SingletonExt,
            IdLogic::IndexedSingletonExt(_) => Self::IndexedSingletonExt,
            IdLogic::Phantom => Self::Phantom,
        }
    }
}

/// A portable snapshot of one Dynamo object and its stored descendants.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DynamoBundle {
    pub version: u32,
    pub root: BundleId,
    #[serde(default)]
    pub recursive: bool,
    /// Export omissions keyed by the label whose descendants they apply to.
    #[serde(default)]
    pub exclusions: BTreeMap<String, BTreeSet<String>>,
    pub items: Vec<DynamoBundleItem>,
    #[serde(default)]
    pub references: Vec<DynamoBundleReference>,
}

impl DynamoBundle {
    /// Version of the first public Dynamo bundle format.
    pub const VERSION: u32 = 1;
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

impl DynamoBundleItem {
    /// Reads a value selected by a bundle reference path.
    pub fn value_at(&self, path: &BundleValuePath) -> Option<&Value> {
        super::value::value_at_path(&self.data, path)
    }
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BundleValuePath(pub Vec<BundleValuePathSegment>);

impl BundleValuePath {
    pub fn field(field: impl Into<String>) -> Self {
        Self(vec![BundleValuePathSegment::Field(field.into())])
    }

    pub fn then_field(mut self, field: impl Into<String>) -> Self {
        self.0.push(BundleValuePathSegment::Field(field.into()));
        self
    }

    pub fn then_index(mut self, index: usize) -> Self {
        self.0.push(BundleValuePathSegment::Index(index));
        self
    }
}

impl fmt::Display for BundleValuePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for segment in &self.0 {
            match segment {
                BundleValuePathSegment::Field(field) => write!(f, ".{field}")?,
                BundleValuePathSegment::Index(index) => write!(f, "[{index}]")?,
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BundleValuePathSegment {
    Field(String),
    Index(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DynamoBundleReferenceEncoding {
    PkSk,
    ForeignRef,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DynamoBundleReference {
    pub source: BundleId,
    pub path: BundleValuePath,
    pub target: DynamoBundleReferenceTarget,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DynamoBundleReferenceTarget {
    Internal {
        id: BundleId,
        encoding: DynamoBundleReferenceEncoding,
    },
    /// External references are supported only within the importing table.
    External {
        lookup_id: PkSk,
        /// The reference path itself for scalar options, or a containing path
        /// when one missing member must clear a compound optional value.
        clear_path: BundleValuePath,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IfExisting {
    Merge,
    Replace,
    #[default]
    Duplicate,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DynamoImportResult {
    pub root_id: PkSk,
    /// Number of logical bundle objects written by the import.
    pub written_objects: usize,
    /// Number of logical stale roots recursively removed by Replace.
    pub deleted_subtree_roots: usize,
    pub duplicated: bool,
    pub warnings: Vec<DynamoImportWarning>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DynamoImportWarning {
    MissingExternalReference,
}

/// Per-label configuration carried by `DynamoBundlePolicy::Include`.
pub struct DynamoBundleSpec {
    id_logic: BundleIdLogic,
    exclude_subtrees: BTreeSet<String>,
    reference_rules: Vec<DynamoBundleReferenceRule>,
}

/// Explicit application policy for bundling a persisted object label.
///
/// Every label encountered by export or import must have a deliberate policy.
/// `ExcludeSubtree` omits an encountered non-root object and its descendants
/// during export, while `Reject` rejects the entire operation. Bundle roots and
/// every object already present in an imported bundle must be `Include`.
#[derive(Default)]
pub enum DynamoBundlePolicy {
    Include(DynamoBundleSpec),
    ExcludeSubtree,
    #[default]
    Reject,
}

impl From<DynamoBundleSpec> for DynamoBundlePolicy {
    fn from(spec: DynamoBundleSpec) -> Self {
        Self::Include(spec)
    }
}

impl DynamoBundlePolicy {
    /// Includes an object type using its exact ID behavior and no custom
    /// exclusions or reference rules.
    pub fn include<O: DynamoObject>() -> Self {
        DynamoBundleSpec::for_object::<O>().into()
    }
}

impl DynamoBundleSpec {
    /// Creates a spec with explicit ID behavior and no exclusions or reference
    /// rules.
    pub(crate) fn new(id_logic: BundleIdLogic) -> Self {
        Self {
            id_logic,
            exclude_subtrees: BTreeSet::new(),
            reference_rules: Vec::new(),
        }
    }

    /// Creates a spec carrying the object's exact ID behavior.
    ///
    /// Applications should use this for every included type so duplicate
    /// imports can regenerate or preserve IDs correctly.
    pub fn for_object<O: DynamoObject>() -> Self {
        Self::new(BundleIdLogic::from_object::<O>())
    }

    pub fn id_logic(&self) -> BundleIdLogic {
        self.id_logic
    }

    pub fn excluded_subtrees(&self) -> &BTreeSet<String> {
        &self.exclude_subtrees
    }

    pub fn reference_rules(&self) -> &[DynamoBundleReferenceRule] {
        &self.reference_rules
    }

    pub fn excluding(mut self, label: impl Into<String>) -> Self {
        self.exclude_subtrees.insert(label.into());
        self
    }

    pub fn excluding_all(mut self, labels: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.exclude_subtrees
            .extend(labels.into_iter().map(Into::into));
        self
    }

    pub fn with_reference(mut self, rule: DynamoBundleReferenceRule) -> Self {
        self.reference_rules.push(rule);
        self
    }
}

pub struct DynamoBundleReferenceRule {
    pub(crate) selector: Arc<BundleReferenceSelector>,
}

type BundleReferenceSelector =
    dyn Fn(&DynamoBundleItem) -> Result<Vec<DynamoBundleReferenceMatch>, ServerError> + Send + Sync;

impl DynamoBundleReferenceRule {
    pub fn new(
        selector: impl Fn(&DynamoBundleItem) -> Result<Vec<DynamoBundleReferenceMatch>, ServerError>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        Self {
            selector: Arc::new(selector),
        }
    }

    pub fn at_path(path: BundleValuePath, target: DynamoBundleReferenceMatchTarget) -> Self {
        Self::new(move |item| {
            Ok(super::value::value_at_path(&item.data, &path)
                .is_some()
                .then(|| DynamoBundleReferenceMatch {
                    path: path.clone(),
                    target: target.clone(),
                })
                .into_iter()
                .collect())
        })
    }
}

#[derive(Debug, Clone)]
pub struct DynamoBundleReferenceMatch {
    pub path: BundleValuePath,
    pub target: DynamoBundleReferenceMatchTarget,
}

impl DynamoBundleReferenceMatch {
    pub fn internal(
        path: BundleValuePath,
        encoding: DynamoBundleReferenceEncoding,
        target_label: impl Into<Option<String>>,
    ) -> Self {
        Self {
            path,
            target: DynamoBundleReferenceMatchTarget::Internal {
                target_label: target_label.into(),
                encoding,
            },
        }
    }

    pub fn external(path: BundleValuePath, lookup_id: PkSk) -> Self {
        Self::external_clearing(path.clone(), lookup_id, path)
    }

    /// Marks an external reference whose absence clears a containing optional
    /// value, such as an optional tuple holding more than one reference.
    pub fn external_clearing(
        path: BundleValuePath,
        lookup_id: PkSk,
        clear_path: BundleValuePath,
    ) -> Self {
        Self {
            path,
            target: DynamoBundleReferenceMatchTarget::External {
                lookup_id,
                clear_path: Some(clear_path),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub enum DynamoBundleReferenceMatchTarget {
    Internal {
        target_label: Option<String>,
        encoding: DynamoBundleReferenceEncoding,
    },
    External {
        lookup_id: PkSk,
        /// `None` means the matched reference path itself.
        clear_path: Option<BundleValuePath>,
    },
}

impl DynamoBundleReferenceMatchTarget {
    pub fn internal(
        encoding: DynamoBundleReferenceEncoding,
        target_label: impl Into<Option<String>>,
    ) -> Self {
        Self::Internal {
            target_label: target_label.into(),
            encoding,
        }
    }

    pub fn external(lookup_id: PkSk) -> Self {
        Self::External {
            lookup_id,
            clear_path: None,
        }
    }
}
