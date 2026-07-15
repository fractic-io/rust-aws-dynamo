use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};

use fractic_server_error::ServerError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::schema::PkSk;

/// A stable, database-independent identifier for an item in a bundle.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BundleId {
    pub value: u64,
    pub label: String,
    pub original_sk: String,
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

/// One logical Dynamo object. Batch-optimized records remain opaque values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DynamoBundleItem {
    pub id: BundleId,
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
    External(PkSk),
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
    /// Number of logical bundle items written by Merge.
    pub merged: usize,
    /// Number of logical stale roots recursively removed by Replace.
    pub deleted: usize,
    pub duplicated: bool,
    pub warnings: Vec<DynamoImportWarning>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DynamoImportWarning {
    MissingExternalReference,
}

/// Per-label configuration returned by `DynamoCrudAlgorithms::bundle_spec`.
#[derive(Default)]
pub struct DynamoBundleSpec {
    pub exclude_subtrees: BTreeSet<String>,
    pub reference_rules: Vec<DynamoBundleReferenceRule>,
}

impl DynamoBundleSpec {
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
        Self {
            path,
            target: DynamoBundleReferenceMatchTarget::External { lookup_id },
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
        Self::External { lookup_id }
    }
}
