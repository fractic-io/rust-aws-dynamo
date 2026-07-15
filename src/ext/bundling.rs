//! Portable export and import of complete Dynamo object trees.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt,
    sync::Arc,
};

use aws_sdk_dynamodb::{primitives::Blob, types::AttributeValue};
use fractic_core::collection;
use fractic_server_error::ServerError;
use futures_util::{stream, StreamExt as _, TryStreamExt as _};
use serde::{Deserialize, Serialize};

use crate::{
    errors::{DynamoCalloutError, DynamoInvalidOperation, DynamoNotFound},
    ext::crud::DynamoCrudAlgorithms,
    schema::{
        id_calculations::{get_object_type, strip_ext_suffix},
        DynamoObject, NestingLogic, PkSk,
    },
    util::DynamoUtil,
};

const BUNDLE_VERSION: u32 = 1;
const QUERY_CONCURRENCY: usize = 16;

// Definitions.
// ----------------------------------------------------------------------------

/// A stable, database-independent identifier for an item in a bundle.
///
/// `original_sk` deliberately retains the source sort-key information. The
/// numeric value is only an unambiguous handle within this bundle.
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
    /// Whether the export intentionally includes the root's stored tree.
    #[serde(default)]
    pub recursive: bool,
    /// Subtree labels intentionally omitted by exporter-side bundle specs.
    #[serde(default)]
    pub excluded_subtrees: HashSet<String>,
    pub items: Vec<DynamoBundleItem>,
    #[serde(default)]
    pub references: Vec<DynamoBundleReference>,
}

/// One logical object. A logical object may contain multiple physical rows for
/// ext-partitioned storage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DynamoBundleItem {
    pub id: BundleId,
    pub parent: Option<BundleId>,
    pub nesting: BundleNesting,
    pub rows: Vec<DynamoBundleRow>,
}

/// Placement of an object relative to its parent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BundleNesting {
    Root,
    TopLevel,
    Inline,
}

/// One physical Dynamo row belonging to a logical bundled object.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DynamoBundleRow {
    /// Suffix after the logical object's base SK, such as `+0` for an ext row.
    pub sk_suffix: String,
    pub data: BTreeMap<String, DynamoBundleValue>,
}

/// Lossless, Serde-compatible representation of a DynamoDB attribute value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum DynamoBundleValue {
    Binary(Vec<u8>),
    Bool(bool),
    BinarySet(Vec<Vec<u8>>),
    List(Vec<DynamoBundleValue>),
    Map(BTreeMap<String, DynamoBundleValue>),
    Number(String),
    NumberSet(Vec<String>),
    Null(bool),
    String(String),
    StringSet(Vec<String>),
}

/// A concrete path into a bundled row.
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

/// How a reference is encoded in the persisted attribute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DynamoBundleReferenceEncoding {
    /// A `PkSk`, serialized as `pk|sk`.
    PkSk,
    /// A minimal `ForeignRef`, serialized as its terminal SK value.
    ForeignRef,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DynamoBundleReference {
    pub source: BundleId,
    pub row: usize,
    pub path: BundleValuePath,
    pub encoding: DynamoBundleReferenceEncoding,
    pub original: DynamoBundleValue,
    pub target: DynamoBundleReferenceTarget,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DynamoBundleReferenceTarget {
    Internal(BundleId),
    External(DynamoBundleExternalRef),
}

/// Serializable information needed to validate an external reference on
/// import. Applications can use `namespace` and `value` for cross-table refs;
/// `lookup_id` enables the built-in same-table existence check.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DynamoBundleExternalRef {
    pub namespace: Option<String>,
    pub value: DynamoBundleValue,
    pub lookup_id: Option<PkSk>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IfExisting {
    Overwrite,
    #[default]
    Duplicate,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DynamoImportResult {
    pub root_id: PkSk,
    pub created: usize,
    pub updated: usize,
    pub deleted: usize,
    pub duplicated: bool,
    pub warnings: Vec<DynamoImportWarning>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DynamoImportWarning {
    MissingExternalReference {
        source: BundleId,
        path: BundleValuePath,
        reference: DynamoBundleExternalRef,
    },
}

/// Per-label configuration returned by `DynamoCrudAlgorithms::bundle_spec`.
#[derive(Default)]
pub struct DynamoBundleSpec {
    /// Labels whose complete subtrees should be omitted below this object.
    pub exclude_subtrees: HashSet<String>,
    pub reference_rules: Vec<DynamoBundleReferenceRule>,
}

impl DynamoBundleSpec {
    pub fn excluding(mut self, label: impl Into<String>) -> Self {
        self.exclude_subtrees.insert(label.into());
        self
    }

    pub fn with_reference(mut self, rule: DynamoBundleReferenceRule) -> Self {
        self.reference_rules.push(rule);
        self
    }

    pub fn excluding_all(mut self, labels: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.exclude_subtrees
            .extend(labels.into_iter().map(Into::into));
        self
    }
}

/// A rule locates references in one item after the complete bundle ID mapping
/// is known. This callback form supports tagged enums, paired refs, and other
/// domain-specific layouts without putting schema knowledge in the bundler.
pub struct DynamoBundleReferenceRule {
    selector: Arc<BundleReferenceSelector>,
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

    /// Convenience for a single fixed path in every physical row where it is
    /// present. More involved layouts can use `new` directly.
    pub fn at_path(
        path: BundleValuePath,
        encoding: DynamoBundleReferenceEncoding,
        target: DynamoBundleReferenceMatchTarget,
    ) -> Self {
        Self::new(move |item| {
            Ok(item
                .rows
                .iter()
                .enumerate()
                .filter(|(_, row)| value_at_path(&row.data, &path).is_some())
                .map(|(row, _)| DynamoBundleReferenceMatch {
                    row,
                    path: path.clone(),
                    encoding,
                    target: target.clone(),
                })
                .collect())
        })
    }
}

#[derive(Debug, Clone)]
pub struct DynamoBundleReferenceMatch {
    pub row: usize,
    pub path: BundleValuePath,
    pub encoding: DynamoBundleReferenceEncoding,
    pub target: DynamoBundleReferenceMatchTarget,
}

impl DynamoBundleReferenceMatch {
    pub fn internal(
        row: usize,
        path: BundleValuePath,
        encoding: DynamoBundleReferenceEncoding,
        target_label: impl Into<Option<String>>,
    ) -> Self {
        Self {
            row,
            path,
            encoding,
            target: DynamoBundleReferenceMatchTarget::Internal {
                target_label: target_label.into(),
            },
        }
    }

    pub fn external(
        row: usize,
        path: BundleValuePath,
        encoding: DynamoBundleReferenceEncoding,
        namespace: Option<String>,
        lookup_id: Option<PkSk>,
    ) -> Self {
        Self {
            row,
            path,
            encoding,
            target: DynamoBundleReferenceMatchTarget::External {
                namespace,
                lookup_id,
            },
        }
    }
}

#[derive(Debug, Clone)]
pub enum DynamoBundleReferenceMatchTarget {
    /// Must resolve to an object included in this bundle.
    Internal { target_label: Option<String> },
    /// Retained only when the referenced object exists at import time.
    External {
        namespace: Option<String>,
        lookup_id: Option<PkSk>,
    },
}

// Public interface.
// ----------------------------------------------------------------------------

pub async fn export<O: DynamoObject>(
    util: &DynamoUtil,
    algorithms: &dyn DynamoCrudAlgorithms,
    item: O,
) -> Result<DynamoBundle, ServerError> {
    export_inner(
        util,
        algorithms,
        item.id().clone(),
        root_nesting::<O>(),
        false,
    )
    .await
}

pub async fn export_recursive<O: DynamoObject>(
    util: &DynamoUtil,
    algorithms: &dyn DynamoCrudAlgorithms,
    item: O,
) -> Result<DynamoBundle, ServerError> {
    export_inner(
        util,
        algorithms,
        item.id().clone(),
        root_nesting::<O>(),
        true,
    )
    .await
}

pub async fn import<O: DynamoObject>(
    util: &DynamoUtil,
    algorithms: &dyn DynamoCrudAlgorithms,
    parent: Option<&PkSk>,
    mut bundle: DynamoBundle,
    if_existing: IfExisting,
) -> Result<DynamoImportResult, ServerError> {
    validate_bundle(&bundle)?;
    let root_item = bundle
        .items
        .iter()
        .find(|item| item.id == bundle.root)
        .ok_or_else(|| invalid_bundle("bundle root item was missing"))?;
    if root_item.id.label != O::id_label() || root_item.nesting != root_nesting::<O>() {
        return Err(invalid_bundle(
            "bundle root type or nesting did not match the importing CRUD wrapper",
        ));
    }

    let preserved_ids = build_id_map(&bundle, parent, false)?;
    let preserved_rows = materialize_rows(&bundle, &preserved_ids)?;
    let conflicts = util
        .raw_batch_get_ids(
            preserved_rows.keys().cloned().collect(),
            Some("pk, sk".into()),
        )
        .await?;
    let duplicated = !conflicts.is_empty() && matches!(if_existing, IfExisting::Duplicate);
    let id_map = if duplicated {
        build_id_map(&bundle, parent, true)?
    } else {
        preserved_ids
    };
    if duplicated {
        let duplicate_rows = materialize_rows(&bundle, &id_map)?;
        if !util
            .raw_batch_get_ids(
                duplicate_rows.keys().cloned().collect(),
                Some("pk, sk".into()),
            )
            .await?
            .is_empty()
        {
            return Err(DynamoInvalidOperation::new(
                "bundle cannot be duplicated at this placement because one or more fixed \
                 singleton IDs still conflict",
            ));
        }
    }

    let warnings = resolve_references(util, algorithms, &mut bundle, &id_map).await?;
    let desired = materialize_rows(&bundle, &id_map)?;
    let root_id = id_map
        .get(&bundle.root)
        .cloned()
        .ok_or_else(|| invalid_bundle("bundle root was not present in ID map"))?;

    let existing = if conflicts.is_empty() || duplicated {
        HashMap::new()
    } else {
        collect_existing_rows(
            util,
            algorithms,
            &root_id,
            bundle_root_nesting(&bundle),
            bundle.recursive,
            &bundle.excluded_subtrees,
        )
        .await?
    };

    let puts = desired
        .iter()
        .filter(|(id, map)| existing.get(*id) != Some(*map))
        .map(|(_, map)| map.clone())
        .collect::<Vec<_>>();
    let deletes = existing
        .keys()
        .filter(|id| !desired.contains_key(*id))
        .cloned()
        .collect::<Vec<_>>();
    let updated = desired
        .keys()
        .filter(|id| existing.contains_key(*id))
        .filter(|id| existing.get(*id) != desired.get(*id))
        .count();
    let created = desired.len()
        - desired
            .keys()
            .filter(|id| existing.contains_key(*id))
            .count();

    // Delete stale rows before putting replacements. This is important for
    // shrinking ext-partitioned items, whose lingering partitions must not be
    // observable after import completes.
    util.raw_batch_delete_ids(deletes.clone()).await?;
    util.raw_batch_put_item(puts).await?;

    Ok(DynamoImportResult {
        root_id,
        created,
        updated,
        deleted: deletes.len(),
        duplicated,
        warnings,
    })
}

// Internal: export traversal.
// ----------------------------------------------------------------------------

#[derive(Clone)]
struct CollectedItem {
    id: PkSk,
    parent: Option<PkSk>,
    nesting: BundleNesting,
    rows: Vec<crate::util::DynamoMap>,
    /// Exclusions inherited from ancestors. The object's own spec is added
    /// when its child partition is queried.
    excludes: HashSet<String>,
}

async fn export_inner(
    util: &DynamoUtil,
    algorithms: &dyn DynamoCrudAlgorithms,
    root: PkSk,
    root_nesting: BundleNesting,
    recursive: bool,
) -> Result<DynamoBundle, ServerError> {
    let collected = collect_items(
        util,
        algorithms,
        &root,
        root_nesting,
        recursive,
        &HashSet::new(),
    )
    .await?;
    if collected.is_empty() {
        return Err(DynamoNotFound::new());
    }

    let mut by_pk_sk = HashMap::new();
    let mut bundle_items = Vec::with_capacity(collected.len());
    for (index, item) in collected.iter().enumerate() {
        let label = get_object_type(&item.id.pk, &item.id.sk)?.to_string();
        let id = BundleId {
            value: index as u64,
            label,
            original_sk: item.id.sk.clone(),
        };
        by_pk_sk.insert(item.id.clone(), id.clone());
        bundle_items.push(DynamoBundleItem {
            id,
            parent: None,
            nesting: item.nesting,
            rows: item
                .rows
                .iter()
                .map(|row| row_to_bundle_row(&item.id, row))
                .collect::<Result<_, _>>()?,
        });
    }
    for (bundle_item, collected_item) in bundle_items.iter_mut().zip(&collected) {
        bundle_item.parent = collected_item
            .parent
            .as_ref()
            .and_then(|id| by_pk_sk.get(id))
            .cloned();
    }

    let root_id = by_pk_sk
        .get(&logical_base_id(&root))
        .cloned()
        .ok_or_else(|| invalid_bundle("source root was not returned by DynamoDB"))?;
    let excluded_subtrees = collected
        .iter()
        .filter_map(|item| get_object_type(&item.id.pk, &item.id.sk).ok())
        .filter_map(|label| algorithms.bundle_spec(label))
        .flat_map(|spec| spec.exclude_subtrees)
        .collect();
    let mut bundle = DynamoBundle {
        version: BUNDLE_VERSION,
        root: root_id,
        recursive,
        excluded_subtrees,
        items: bundle_items,
        references: Vec::new(),
    };
    bundle.references = collect_references(algorithms, &bundle)?;
    Ok(bundle)
}

async fn collect_items(
    util: &DynamoUtil,
    algorithms: &dyn DynamoCrudAlgorithms,
    root: &PkSk,
    root_nesting: BundleNesting,
    recursive: bool,
    inherited_excludes: &HashSet<String>,
) -> Result<Vec<CollectedItem>, ServerError> {
    let root = logical_base_id(root);
    let initial_rows = raw_query(util, &root.pk, Some(&root.sk)).await?;
    let mut initial_groups = group_logical_rows(initial_rows)?;
    let root_rows = initial_groups
        .remove(&root)
        .ok_or_else(|| DynamoNotFound::new())?;
    let mut root_excludes = inherited_excludes.clone();
    root_excludes.extend(
        algorithms
            .bundle_spec(get_object_type(&root.pk, &root.sk)?)
            .map(|spec| spec.exclude_subtrees)
            .unwrap_or_default(),
    );
    let mut collected = vec![CollectedItem {
        id: root.clone(),
        parent: None,
        nesting: root_nesting,
        rows: root_rows,
        excludes: root_excludes.clone(),
    }];
    if !recursive {
        return Ok(collected);
    }

    initial_groups.retain(|id, _| is_inline_descendant(&id.sk, &root.sk));
    append_partition_groups(&mut collected, &root, initial_groups, &root_excludes)?;

    let mut queried = HashSet::new();
    let mut frontier = collected
        .iter()
        .map(|item| item.id.clone())
        .collect::<Vec<_>>();
    while !frontier.is_empty() {
        let owners = frontier
            .drain(..)
            .filter(|id| queried.insert(id.clone()))
            .collect::<Vec<_>>();
        let mut results = stream::iter(owners)
            .map(|owner| async move {
                let rows = raw_query(util, &owner.sk, None).await?;
                Ok::<_, ServerError>((owner, group_logical_rows(rows)?))
            })
            .buffer_unordered(QUERY_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        results.sort_by(|(a, _), (b, _)| a.pk.cmp(&b.pk).then_with(|| a.sk.cmp(&b.sk)));

        for (owner, groups) in results {
            let before = collected.len();
            let mut excludes = collected
                .iter()
                .find(|item| item.id == owner)
                .map(|item| item.excludes.clone())
                .unwrap_or_default();
            if let Some(spec) = algorithms.bundle_spec(get_object_type(&owner.pk, &owner.sk)?) {
                excludes.extend(spec.exclude_subtrees);
            }
            append_partition_groups(&mut collected, &owner, groups, &excludes)?;
            frontier.extend(collected[before..].iter().map(|item| item.id.clone()));
        }
    }
    Ok(collected)
}

fn append_partition_groups(
    collected: &mut Vec<CollectedItem>,
    owner: &PkSk,
    groups: HashMap<PkSk, Vec<crate::util::DynamoMap>>,
    excludes: &HashSet<String>,
) -> Result<(), ServerError> {
    let mut ids = groups.keys().cloned().collect::<Vec<_>>();
    ids.sort_by(|a, b| a.sk.len().cmp(&b.sk.len()).then_with(|| a.sk.cmp(&b.sk)));
    // Including the owner lets the same helper correctly classify the root's
    // inline-prefix query as well as regular child-partition queries.
    let mut accepted = vec![owner.clone()];
    let mut excluded = Vec::<PkSk>::new();
    for id in ids {
        let label = get_object_type(&id.pk, &id.sk)?;
        if excludes.contains(label)
            || excluded
                .iter()
                .any(|ancestor| is_inline_descendant(&id.sk, &ancestor.sk))
        {
            excluded.push(id);
            continue;
        }
        let inline_parent = accepted
            .iter()
            .filter(|candidate| is_inline_descendant(&id.sk, &candidate.sk))
            .max_by_key(|candidate| candidate.sk.len())
            .cloned();
        let (parent, nesting) = match inline_parent {
            Some(parent) => (parent, BundleNesting::Inline),
            None => (owner.clone(), BundleNesting::TopLevel),
        };
        let rows = groups
            .get(&id)
            .cloned()
            .ok_or_else(|| invalid_bundle("grouped Dynamo rows disappeared"))?;
        collected.push(CollectedItem {
            id: id.clone(),
            parent: Some(parent),
            nesting,
            rows,
            excludes: excludes.clone(),
        });
        accepted.push(id);
    }
    Ok(())
}

fn group_logical_rows(
    rows: Vec<crate::util::DynamoMap>,
) -> Result<HashMap<PkSk, Vec<crate::util::DynamoMap>>, ServerError> {
    let mut groups = HashMap::new();
    for row in rows {
        let id = PkSk::from_map(&row)?;
        groups
            .entry(logical_base_id(&id))
            .or_insert_with(Vec::new)
            .push(row);
    }
    for rows in groups.values_mut() {
        rows.sort_by_key(|row| PkSk::from_map(row).map(|id| id.sk).unwrap_or_default());
    }
    Ok(groups)
}

async fn raw_query(
    util: &DynamoUtil,
    pk: &str,
    sk_prefix: Option<&str>,
) -> Result<Vec<crate::util::DynamoMap>, ServerError> {
    let mut values: HashMap<String, AttributeValue> = collection! {
        ":pk".to_string() => AttributeValue::S(pk.to_string()),
    };
    let condition = if let Some(prefix) = sk_prefix {
        values.insert(":sk".to_string(), AttributeValue::S(prefix.to_string()));
        "pk = :pk AND begins_with(sk, :sk)"
    } else {
        "pk = :pk"
    };
    let pages = util
        .backend
        .query(
            util.table.clone(),
            None,
            condition.to_string(),
            values,
            None,
        )
        .await
        .map_err(|error| DynamoCalloutError::with_debug(&error))?;
    Ok(pages
        .into_iter()
        .flat_map(|page| page.items.unwrap_or_default())
        .collect())
}

// Internal: references.
// ----------------------------------------------------------------------------

fn collect_references(
    algorithms: &dyn DynamoCrudAlgorithms,
    bundle: &DynamoBundle,
) -> Result<Vec<DynamoBundleReference>, ServerError> {
    let mut references = Vec::new();
    for item in &bundle.items {
        let Some(spec) = algorithms.bundle_spec(&item.id.label) else {
            continue;
        };
        for rule in spec.reference_rules {
            for matched in (rule.selector)(item)? {
                let original = item
                    .rows
                    .get(matched.row)
                    .and_then(|row| value_at_path(&row.data, &matched.path))
                    .cloned()
                    .ok_or_else(|| {
                        invalid_bundle("reference rule returned a missing value path")
                    })?;
                let target = match matched.target {
                    DynamoBundleReferenceMatchTarget::Internal { target_label } => {
                        let target = find_internal_target(
                            bundle,
                            &original,
                            matched.encoding,
                            target_label.as_deref(),
                        )?;
                        DynamoBundleReferenceTarget::Internal(target)
                    }
                    DynamoBundleReferenceMatchTarget::External {
                        namespace,
                        lookup_id,
                    } => DynamoBundleReferenceTarget::External(DynamoBundleExternalRef {
                        namespace,
                        value: original.clone(),
                        lookup_id,
                    }),
                };
                references.push(DynamoBundleReference {
                    source: item.id.clone(),
                    row: matched.row,
                    path: matched.path,
                    encoding: matched.encoding,
                    original,
                    target,
                });
            }
        }
    }
    Ok(references)
}

fn find_internal_target(
    bundle: &DynamoBundle,
    value: &DynamoBundleValue,
    encoding: DynamoBundleReferenceEncoding,
    target_label: Option<&str>,
) -> Result<BundleId, ServerError> {
    let raw = value_string(value)?;
    let matches = bundle
        .items
        .iter()
        .filter(|item| target_label.is_none_or(|label| item.id.label == label))
        .filter(|item| match encoding {
            DynamoBundleReferenceEncoding::PkSk => raw
                .split_once('|')
                .map(|(_, sk)| sk == item.id.original_sk)
                .unwrap_or(false),
            DynamoBundleReferenceEncoding::ForeignRef => terminal_ref(&item.id.original_sk) == raw,
        })
        .map(|item| item.id.clone())
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [target] => Ok(target.clone()),
        [] => Err(invalid_bundle(
            "local reference target was not included in bundle",
        )),
        _ => Err(invalid_bundle("local reference target was ambiguous")),
    }
}

async fn resolve_references(
    util: &DynamoUtil,
    algorithms: &dyn DynamoCrudAlgorithms,
    bundle: &mut DynamoBundle,
    id_map: &HashMap<BundleId, PkSk>,
) -> Result<Vec<DynamoImportWarning>, ServerError> {
    let mut warnings = Vec::new();
    for reference in bundle.references.clone() {
        let replacement = match &reference.target {
            DynamoBundleReferenceTarget::Internal(target) => {
                let target = id_map
                    .get(target)
                    .ok_or_else(|| invalid_bundle("reference target was absent from ID map"))?;
                DynamoBundleValue::String(match reference.encoding {
                    DynamoBundleReferenceEncoding::PkSk => target.to_string(),
                    DynamoBundleReferenceEncoding::ForeignRef => {
                        terminal_ref(&target.sk).to_string()
                    }
                })
            }
            DynamoBundleReferenceTarget::External(external) => {
                let exists = match algorithms.bundle_external_ref_exists(external).await? {
                    Some(exists) => Some(exists),
                    None => match &external.lookup_id {
                        Some(id) => Some(util.item_exists(id.clone()).await?),
                        None => None,
                    },
                };
                if exists == Some(false) {
                    warnings.push(DynamoImportWarning::MissingExternalReference {
                        source: reference.source.clone(),
                        path: reference.path.clone(),
                        reference: external.clone(),
                    });
                    DynamoBundleValue::Null(true)
                } else {
                    reference.original.clone()
                }
            }
        };
        let item = bundle
            .items
            .iter_mut()
            .find(|item| item.id == reference.source)
            .ok_or_else(|| invalid_bundle("reference source was absent from bundle"))?;
        let row = item
            .rows
            .get_mut(reference.row)
            .ok_or_else(|| invalid_bundle("reference row was absent from bundle item"))?;
        set_value_at_path(&mut row.data, &reference.path, replacement)?;
    }
    Ok(warnings)
}

// Internal: import mapping and diff.
// ----------------------------------------------------------------------------

fn build_id_map(
    bundle: &DynamoBundle,
    parent: Option<&PkSk>,
    duplicate: bool,
) -> Result<HashMap<BundleId, PkSk>, ServerError> {
    let mut result = HashMap::new();
    let mut pending = bundle.items.iter().collect::<Vec<_>>();
    while !pending.is_empty() {
        let mut next = Vec::new();
        let mut mapped_any = false;
        for item in pending {
            let parent_id = item.parent.as_ref().and_then(|id| result.get(id));
            if item.parent.is_some() && parent_id.is_none() {
                next.push(item);
                continue;
            }
            let mapped = if item.id == bundle.root {
                place_root(item, parent, duplicate)?
            } else {
                place_child(
                    item,
                    parent_id.expect("non-root bundle item has parent"),
                    duplicate,
                )?
            };
            result.insert(item.id.clone(), mapped);
            mapped_any = true;
        }
        if !mapped_any {
            return Err(invalid_bundle(
                "bundle item parents contain a cycle or missing ID",
            ));
        }
        pending = next;
    }
    Ok(result)
}

fn place_root(
    item: &DynamoBundleItem,
    parent: Option<&PkSk>,
    duplicate: bool,
) -> Result<PkSk, ServerError> {
    match item.nesting {
        BundleNesting::Root => {
            if parent.is_some() {
                return Err(invalid_bundle(
                    "root object bundle cannot be imported below a parent",
                ));
            }
            Ok(PkSk {
                pk: "ROOT".to_string(),
                sk: maybe_freshen(&item.id.original_sk, duplicate),
            })
        }
        BundleNesting::TopLevel => {
            let parent = parent.ok_or_else(|| invalid_bundle("child bundle requires a parent"))?;
            Ok(PkSk {
                pk: parent.sk.clone(),
                sk: maybe_freshen(&item.id.original_sk, duplicate),
            })
        }
        BundleNesting::Inline => {
            let parent = parent.ok_or_else(|| invalid_bundle("child bundle requires a parent"))?;
            let component = object_sk_component(&item.id.original_sk, &item.id.label)?;
            Ok(PkSk {
                pk: parent.pk.clone(),
                sk: append_relative_sk(&parent.sk, &maybe_freshen(component, duplicate)),
            })
        }
    }
}

fn place_child(
    item: &DynamoBundleItem,
    parent: &PkSk,
    duplicate: bool,
) -> Result<PkSk, ServerError> {
    match item.nesting {
        BundleNesting::TopLevel => Ok(PkSk {
            pk: parent.sk.clone(),
            sk: maybe_freshen(&item.id.original_sk, duplicate),
        }),
        BundleNesting::Inline => {
            let original_parent = item
                .parent
                .as_ref()
                .ok_or_else(|| invalid_bundle("inline child did not have a bundle parent"))?;
            let relative = item
                .id
                .original_sk
                .strip_prefix(&original_parent.original_sk)
                .ok_or_else(|| invalid_bundle("inline child SK did not start with parent SK"))?;
            Ok(PkSk {
                pk: parent.pk.clone(),
                sk: format!("{}{}", parent.sk, maybe_freshen(relative, duplicate)),
            })
        }
        BundleNesting::Root => Err(invalid_bundle("non-root item had root nesting")),
    }
}

fn materialize_rows(
    bundle: &DynamoBundle,
    ids: &HashMap<BundleId, PkSk>,
) -> Result<HashMap<PkSk, crate::util::DynamoMap>, ServerError> {
    let mut output = HashMap::new();
    for item in &bundle.items {
        let base = ids
            .get(&item.id)
            .ok_or_else(|| invalid_bundle("bundle item had no mapped ID"))?;
        for row in &item.rows {
            let id = PkSk {
                pk: base.pk.clone(),
                sk: format!("{}{}", base.sk, row.sk_suffix),
            };
            let mut map = row
                .data
                .iter()
                .map(|(key, value)| Ok((key.clone(), value_to_attribute(value)?)))
                .collect::<Result<crate::util::DynamoMap, ServerError>>()?;
            id.write_to_map(&mut map);
            if output.insert(id, map).is_some() {
                return Err(invalid_bundle(
                    "multiple bundle rows mapped to the same Dynamo ID",
                ));
            }
        }
    }
    Ok(output)
}

async fn collect_existing_rows(
    util: &DynamoUtil,
    algorithms: &dyn DynamoCrudAlgorithms,
    root: &PkSk,
    root_nesting: BundleNesting,
    recursive: bool,
    excluded_subtrees: &HashSet<String>,
) -> Result<HashMap<PkSk, crate::util::DynamoMap>, ServerError> {
    collect_items(
        util,
        algorithms,
        root,
        root_nesting,
        recursive,
        excluded_subtrees,
    )
    .await?
    .into_iter()
    .flat_map(|item| item.rows)
    .map(|row| Ok((PkSk::from_map(&row)?, row)))
    .collect::<Result<_, ServerError>>()
}

// Helpers.
// ----------------------------------------------------------------------------

fn root_nesting<O: DynamoObject>() -> BundleNesting {
    match O::nesting_logic() {
        NestingLogic::Root => BundleNesting::Root,
        NestingLogic::TopLevelChildOf(_) | NestingLogic::TopLevelChildOfAny => {
            BundleNesting::TopLevel
        }
        NestingLogic::InlineChildOf(_) | NestingLogic::InlineChildOfAny => BundleNesting::Inline,
    }
}

fn bundle_root_nesting(bundle: &DynamoBundle) -> BundleNesting {
    bundle
        .items
        .iter()
        .find(|item| item.id == bundle.root)
        .map(|item| item.nesting)
        .unwrap_or(BundleNesting::Root)
}

fn validate_bundle(bundle: &DynamoBundle) -> Result<(), ServerError> {
    if bundle.version != BUNDLE_VERSION {
        return Err(invalid_bundle("unsupported bundle version"));
    }
    let ids = bundle
        .items
        .iter()
        .map(|item| &item.id)
        .collect::<HashSet<_>>();
    let values = bundle
        .items
        .iter()
        .map(|item| item.id.value)
        .collect::<HashSet<_>>();
    if ids.len() != bundle.items.len()
        || values.len() != bundle.items.len()
        || !ids.contains(&bundle.root)
    {
        return Err(invalid_bundle(
            "bundle item IDs were duplicated or root was missing",
        ));
    }
    if bundle.items.iter().any(|item| {
        (item.id == bundle.root) != item.parent.is_none()
            || item.rows.is_empty()
            || item
                .rows
                .iter()
                .map(|row| &row.sk_suffix)
                .collect::<HashSet<_>>()
                .len()
                != item.rows.len()
    }) {
        return Err(invalid_bundle(
            "bundle root/parent shape or physical rows were invalid",
        ));
    }
    if bundle.items.iter().any(|item| {
        item.parent
            .as_ref()
            .is_some_and(|parent| !ids.contains(parent))
    }) {
        return Err(invalid_bundle("bundle item referred to a missing parent"));
    }
    Ok(())
}

fn logical_base_id(id: &PkSk) -> PkSk {
    PkSk {
        pk: id.pk.clone(),
        sk: strip_ext_suffix(&id.sk).to_string(),
    }
}

fn is_inline_descendant(sk: &str, parent_sk: &str) -> bool {
    sk.strip_prefix(parent_sk)
        .is_some_and(|suffix| suffix.starts_with('#') || suffix.starts_with('@'))
}

fn append_relative_sk(parent_sk: &str, child_sk: &str) -> String {
    if child_sk.starts_with('@') || child_sk.starts_with('#') {
        format!("{parent_sk}{child_sk}")
    } else {
        format!("{parent_sk}#{child_sk}")
    }
}

fn object_sk_component<'a>(sk: &'a str, label: &str) -> Result<&'a str, ServerError> {
    if let Some(at) = sk.rfind('@') {
        return Ok(&sk[at..]);
    }
    let marker = format!("#{label}#");
    if let Some(index) = sk.rfind(&marker) {
        return Ok(&sk[index + 1..]);
    }
    if sk.starts_with(&format!("{label}#")) {
        return Ok(sk);
    }
    Err(invalid_bundle(
        "could not extract object's own SK component",
    ))
}

fn maybe_freshen(sk: &str, duplicate: bool) -> String {
    if !duplicate || sk.contains('@') {
        return sk.to_string();
    }
    let Some((prefix, _)) = sk.rsplit_once('#') else {
        return sk.to_string();
    };
    let fresh = uuid::Uuid::new_v4().simple().to_string();
    format!("{prefix}#{}", &fresh[..16])
}

fn terminal_ref(sk: &str) -> &str {
    let sk = strip_ext_suffix(sk);
    if let Some(at) = sk.rfind('@') {
        let singleton = &sk[at + 1..];
        if let (Some(open), Some(close)) = (singleton.find('['), singleton.find(']')) {
            return &singleton[open + 1..close];
        }
        return "";
    }
    sk.rsplit_once('#').map(|(_, value)| value).unwrap_or(sk)
}

fn row_to_bundle_row(
    base: &PkSk,
    row: &crate::util::DynamoMap,
) -> Result<DynamoBundleRow, ServerError> {
    let id = PkSk::from_map(row)?;
    let sk_suffix = id
        .sk
        .strip_prefix(&base.sk)
        .ok_or_else(|| invalid_bundle("physical row SK did not start with logical base SK"))?
        .to_string();
    let data = row
        .iter()
        .filter(|(key, _)| key.as_str() != "pk" && key.as_str() != "sk")
        .map(|(key, value)| Ok((key.clone(), attribute_to_value(value)?)))
        .collect::<Result<_, ServerError>>()?;
    Ok(DynamoBundleRow { sk_suffix, data })
}

fn attribute_to_value(value: &AttributeValue) -> Result<DynamoBundleValue, ServerError> {
    Ok(match value {
        AttributeValue::B(value) => DynamoBundleValue::Binary(value.as_ref().to_vec()),
        AttributeValue::Bool(value) => DynamoBundleValue::Bool(*value),
        AttributeValue::Bs(values) => DynamoBundleValue::BinarySet(
            values.iter().map(|value| value.as_ref().to_vec()).collect(),
        ),
        AttributeValue::L(values) => DynamoBundleValue::List(
            values
                .iter()
                .map(attribute_to_value)
                .collect::<Result<_, _>>()?,
        ),
        AttributeValue::M(values) => DynamoBundleValue::Map(
            values
                .iter()
                .map(|(key, value)| Ok((key.clone(), attribute_to_value(value)?)))
                .collect::<Result<_, ServerError>>()?,
        ),
        AttributeValue::N(value) => DynamoBundleValue::Number(value.clone()),
        AttributeValue::Ns(values) => DynamoBundleValue::NumberSet(values.clone()),
        AttributeValue::Null(value) => DynamoBundleValue::Null(*value),
        AttributeValue::S(value) => DynamoBundleValue::String(value.clone()),
        AttributeValue::Ss(values) => DynamoBundleValue::StringSet(values.clone()),
        _ => return Err(invalid_bundle("unsupported unknown Dynamo AttributeValue")),
    })
}

fn value_to_attribute(value: &DynamoBundleValue) -> Result<AttributeValue, ServerError> {
    Ok(match value {
        DynamoBundleValue::Binary(value) => AttributeValue::B(Blob::new(value.clone())),
        DynamoBundleValue::Bool(value) => AttributeValue::Bool(*value),
        DynamoBundleValue::BinarySet(values) => {
            AttributeValue::Bs(values.iter().cloned().map(Blob::new).collect())
        }
        DynamoBundleValue::List(values) => AttributeValue::L(
            values
                .iter()
                .map(value_to_attribute)
                .collect::<Result<_, _>>()?,
        ),
        DynamoBundleValue::Map(values) => AttributeValue::M(
            values
                .iter()
                .map(|(key, value)| Ok((key.clone(), value_to_attribute(value)?)))
                .collect::<Result<_, ServerError>>()?,
        ),
        DynamoBundleValue::Number(value) => AttributeValue::N(value.clone()),
        DynamoBundleValue::NumberSet(values) => AttributeValue::Ns(values.clone()),
        DynamoBundleValue::Null(value) => AttributeValue::Null(*value),
        DynamoBundleValue::String(value) => AttributeValue::S(value.clone()),
        DynamoBundleValue::StringSet(values) => AttributeValue::Ss(values.clone()),
    })
}

fn value_at_path<'a>(
    root: &'a BTreeMap<String, DynamoBundleValue>,
    path: &BundleValuePath,
) -> Option<&'a DynamoBundleValue> {
    let (first, rest) = path.0.split_first()?;
    let BundleValuePathSegment::Field(field) = first else {
        return None;
    };
    let mut value = root.get(field)?;
    for segment in rest {
        value = match (segment, value) {
            (BundleValuePathSegment::Field(field), DynamoBundleValue::Map(map)) => {
                map.get(field)?
            }
            (BundleValuePathSegment::Index(index), DynamoBundleValue::List(list)) => {
                list.get(*index)?
            }
            _ => return None,
        };
    }
    Some(value)
}

fn set_value_at_path(
    root: &mut BTreeMap<String, DynamoBundleValue>,
    path: &BundleValuePath,
    replacement: DynamoBundleValue,
) -> Result<(), ServerError> {
    fn descend(
        value: &mut DynamoBundleValue,
        path: &[BundleValuePathSegment],
        replacement: DynamoBundleValue,
    ) -> Result<(), ServerError> {
        let Some((first, rest)) = path.split_first() else {
            *value = replacement;
            return Ok(());
        };
        let next = match (first, value) {
            (BundleValuePathSegment::Field(field), DynamoBundleValue::Map(map)) => {
                map.get_mut(field)
            }
            (BundleValuePathSegment::Index(index), DynamoBundleValue::List(list)) => {
                list.get_mut(*index)
            }
            _ => None,
        }
        .ok_or_else(|| invalid_bundle("reference path did not match bundled value"))?;
        descend(next, rest, replacement)
    }

    let (first, rest) = path
        .0
        .split_first()
        .ok_or_else(|| invalid_bundle("reference path was empty"))?;
    let BundleValuePathSegment::Field(field) = first else {
        return Err(invalid_bundle("reference path must start with a field"));
    };
    let value = root
        .get_mut(field)
        .ok_or_else(|| invalid_bundle("reference path field was absent"))?;
    descend(value, rest, replacement)
}

fn value_string(value: &DynamoBundleValue) -> Result<&str, ServerError> {
    match value {
        DynamoBundleValue::String(value) => Ok(value),
        _ => Err(invalid_bundle("reference value was not a string")),
    }
}

fn invalid_bundle(details: &str) -> ServerError {
    DynamoInvalidOperation::new(&format!("invalid Dynamo bundle: {details}"))
}

// Tests.
// ----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::backend::MockDynamoBackend;
    use async_trait::async_trait;
    use aws_sdk_dynamodb::operation::{
        batch_get_item::BatchGetItemOutput, batch_write_item::BatchWriteItemOutput,
        query::QueryOutput,
    };

    #[derive(Debug, Default, Clone, Serialize, Deserialize)]
    pub struct TestRootData {}
    crate::dynamo_object!(
        TestRoot,
        TestRootData,
        "ROOTOBJ",
        crate::schema::IdLogic::Uuid,
        NestingLogic::Root
    );

    struct TestAlgorithms;

    #[async_trait]
    impl DynamoCrudAlgorithms for TestAlgorithms {
        async fn recursive_delete(&self, _id: PkSk) -> Result<(), ServerError> {
            Ok(())
        }

        fn bundle_spec(&self, id_label: &str) -> Option<DynamoBundleSpec> {
            match id_label {
                "ROOTOBJ" => Some(DynamoBundleSpec::default().excluding("RECALC")),
                "CHILD" => Some(DynamoBundleSpec::default().excluding("GRAND")),
                _ => None,
            }
        }
    }

    struct MissingExternalAlgorithms;

    #[async_trait]
    impl DynamoCrudAlgorithms for MissingExternalAlgorithms {
        async fn recursive_delete(&self, _id: PkSk) -> Result<(), ServerError> {
            Ok(())
        }

        async fn bundle_external_ref_exists(
            &self,
            _reference: &DynamoBundleExternalRef,
        ) -> Result<Option<bool>, ServerError> {
            Ok(Some(false))
        }
    }

    fn id(value: u64, label: &str, sk: &str) -> BundleId {
        BundleId {
            value,
            label: label.into(),
            original_sk: sk.into(),
        }
    }

    fn row(pk: &str, sk: &str) -> crate::util::DynamoMap {
        HashMap::from([
            ("pk".into(), AttributeValue::S(pk.into())),
            ("sk".into(), AttributeValue::S(sk.into())),
            ("value".into(), AttributeValue::S(format!("data:{sk}"))),
        ])
    }

    #[tokio::test]
    #[allow(clippy::result_large_err)]
    async fn recursive_export_walks_top_level_and_inline_storage_and_preserves_ext_rows() {
        let root_sk = "ROOTOBJ#root";
        let mut backend = MockDynamoBackend::new();
        backend
            .expect_query()
            .times(7)
            .returning(move |_, _, _, values, _| {
                let pk = values.get(":pk").unwrap().as_s().unwrap();
                let rows = match pk.as_str() {
                    "ROOT" => vec![
                        row("ROOT", root_sk),
                        row("ROOT", "ROOTOBJ#root#ROOTINLINE#one"),
                        // Dynamo begins_with also returns this, but it is not a
                        // structural inline descendant of the requested root.
                        row("ROOT", "ROOTOBJ#root2"),
                    ],
                    "ROOTOBJ#root" => vec![
                        row(root_sk, "CHILD#one"),
                        row(root_sk, "CHILD#one#INLINE#one"),
                        row(root_sk, "@BIG"),
                        row(root_sk, "@BIG+0"),
                        row(root_sk, "RECALC#ignored"),
                    ],
                    "CHILD#one" => vec![
                        row("CHILD#one", "GRAND#ignored"),
                        row("CHILD#one", "KEEP#one"),
                    ],
                    "ROOTOBJ#root#ROOTINLINE#one"
                    | "CHILD#one#INLINE#one"
                    | "@BIG"
                    | "KEEP#one" => vec![],
                    unexpected => panic!("unexpected partition query: {unexpected}"),
                };
                Ok(vec![QueryOutput::builder().set_items(Some(rows)).build()])
            });
        let util = DynamoUtil {
            backend: Arc::new(backend),
            table: "table".into(),
        };

        let bundle = export_inner(
            &util,
            &TestAlgorithms,
            PkSk {
                pk: "ROOT".into(),
                sk: root_sk.into(),
            },
            BundleNesting::Root,
            true,
        )
        .await
        .unwrap();

        assert!(bundle.recursive);
        assert_eq!(bundle.items.len(), 6);
        assert!(bundle.excluded_subtrees.contains("RECALC"));
        assert!(bundle.excluded_subtrees.contains("GRAND"));
        assert!(!bundle
            .items
            .iter()
            .any(|item| item.id.original_sk == "ROOTOBJ#root2"));
        assert!(!bundle.items.iter().any(|item| item.id.label == "RECALC"));
        assert!(!bundle.items.iter().any(|item| item.id.label == "GRAND"));
        let big = bundle
            .items
            .iter()
            .find(|item| item.id.label == "BIG")
            .unwrap();
        assert_eq!(big.rows.len(), 2);
        assert_eq!(big.rows[0].sk_suffix, "");
        assert_eq!(big.rows[1].sk_suffix, "+0");
        let inline = bundle
            .items
            .iter()
            .find(|item| item.id.label == "INLINE")
            .unwrap();
        assert_eq!(inline.nesting, BundleNesting::Inline);
        assert_eq!(inline.parent.as_ref().unwrap().label, "CHILD");
        let root_inline = bundle
            .items
            .iter()
            .find(|item| item.id.label == "ROOTINLINE")
            .unwrap();
        assert_eq!(root_inline.nesting, BundleNesting::Inline);
        assert_eq!(root_inline.parent.as_ref().unwrap().label, "ROOTOBJ");
        let kept = bundle
            .items
            .iter()
            .find(|item| item.id.label == "KEEP")
            .unwrap();
        assert_eq!(kept.nesting, BundleNesting::TopLevel);
        assert_eq!(kept.parent.as_ref().unwrap().label, "CHILD");
    }

    #[test]
    fn duplicate_mapping_reparents_inline_and_top_level_children() {
        let root = id(0, "ROOTOBJ", "ROOTOBJ#old");
        let top = id(1, "TOP", "TOP#old");
        let inline = id(2, "INLINE", "TOP#old#INLINE#old");
        let bundle = DynamoBundle {
            version: BUNDLE_VERSION,
            root: root.clone(),
            recursive: true,
            excluded_subtrees: HashSet::new(),
            items: vec![
                DynamoBundleItem {
                    id: root.clone(),
                    parent: None,
                    nesting: BundleNesting::Root,
                    rows: vec![],
                },
                DynamoBundleItem {
                    id: top.clone(),
                    parent: Some(root.clone()),
                    nesting: BundleNesting::TopLevel,
                    rows: vec![],
                },
                DynamoBundleItem {
                    id: inline.clone(),
                    parent: Some(top.clone()),
                    nesting: BundleNesting::Inline,
                    rows: vec![],
                },
            ],
            references: vec![],
        };

        let mapped = build_id_map(&bundle, None, true).unwrap();
        assert_eq!(mapped[&root].pk, "ROOT");
        assert_ne!(mapped[&root].sk, root.original_sk);
        assert_eq!(mapped[&top].pk, mapped[&root].sk);
        assert_eq!(mapped[&inline].pk, mapped[&top].pk);
        assert!(mapped[&inline]
            .sk
            .starts_with(&format!("{}#INLINE#", mapped[&top].sk)));
    }

    #[test]
    fn attribute_values_round_trip_losslessly() {
        let value = AttributeValue::M(HashMap::from([
            ("n".into(), AttributeValue::N("1.2300".into())),
            ("null".into(), AttributeValue::Null(true)),
            (
                "bytes".into(),
                AttributeValue::B(Blob::new(vec![0, 1, 255])),
            ),
            (
                "set".into(),
                AttributeValue::Ss(vec!["a".into(), "b".into()]),
            ),
        ]));
        assert_eq!(
            value_to_attribute(&attribute_to_value(&value).unwrap()).unwrap(),
            value
        );
    }

    #[test]
    fn internal_foreign_ref_resolves_by_label_and_terminal_id() {
        let target = id(1, "PIPELINE", "PIPELINE#abc");
        let bundle = DynamoBundle {
            version: BUNDLE_VERSION,
            root: id(0, "STORY", "STORY#root"),
            recursive: true,
            excluded_subtrees: HashSet::new(),
            items: vec![DynamoBundleItem {
                id: target.clone(),
                parent: None,
                nesting: BundleNesting::TopLevel,
                rows: vec![],
            }],
            references: vec![],
        };
        assert_eq!(
            find_internal_target(
                &bundle,
                &DynamoBundleValue::String("abc".into()),
                DynamoBundleReferenceEncoding::ForeignRef,
                Some("PIPELINE"),
            )
            .unwrap(),
            target
        );
    }

    #[tokio::test]
    async fn import_remaps_internal_refs_and_warns_and_nulls_missing_external_refs() {
        let source = id(0, "SOURCE", "SOURCE#old");
        let target = id(1, "TARGET", "TARGET#old");
        let external = DynamoBundleExternalRef {
            namespace: Some("other_table".into()),
            value: DynamoBundleValue::String("ROOT|EXTERNAL#old".into()),
            lookup_id: None,
        };
        let mut bundle = DynamoBundle {
            version: BUNDLE_VERSION,
            root: source.clone(),
            recursive: false,
            excluded_subtrees: HashSet::new(),
            items: vec![
                DynamoBundleItem {
                    id: source.clone(),
                    parent: None,
                    nesting: BundleNesting::Root,
                    rows: vec![DynamoBundleRow {
                        sk_suffix: String::new(),
                        data: BTreeMap::from([
                            ("local".into(), DynamoBundleValue::String("old".into())),
                            (
                                "external".into(),
                                DynamoBundleValue::String("ROOT|EXTERNAL#old".into()),
                            ),
                        ]),
                    }],
                },
                DynamoBundleItem {
                    id: target.clone(),
                    parent: Some(source.clone()),
                    nesting: BundleNesting::TopLevel,
                    rows: vec![],
                },
            ],
            references: vec![
                DynamoBundleReference {
                    source: source.clone(),
                    row: 0,
                    path: BundleValuePath::field("local"),
                    encoding: DynamoBundleReferenceEncoding::ForeignRef,
                    original: DynamoBundleValue::String("old".into()),
                    target: DynamoBundleReferenceTarget::Internal(target.clone()),
                },
                DynamoBundleReference {
                    source: source.clone(),
                    row: 0,
                    path: BundleValuePath::field("external"),
                    encoding: DynamoBundleReferenceEncoding::PkSk,
                    original: external.value.clone(),
                    target: DynamoBundleReferenceTarget::External(external.clone()),
                },
            ],
        };
        let new_target = PkSk {
            pk: "SOURCE#new".into(),
            sk: "TARGET#new".into(),
        };
        let ids = HashMap::from([
            (
                source.clone(),
                PkSk {
                    pk: "ROOT".into(),
                    sk: "SOURCE#new".into(),
                },
            ),
            (target, new_target),
        ]);
        let util = DynamoUtil {
            backend: Arc::new(MockDynamoBackend::new()),
            table: "table".into(),
        };

        let warnings = resolve_references(&util, &MissingExternalAlgorithms, &mut bundle, &ids)
            .await
            .unwrap();

        assert_eq!(warnings.len(), 1);
        assert_eq!(
            bundle.items[0].rows[0].data["local"],
            DynamoBundleValue::String("new".into())
        );
        assert_eq!(
            bundle.items[0].rows[0].data["external"],
            DynamoBundleValue::Null(true)
        );
    }

    #[tokio::test]
    #[allow(clippy::result_large_err)]
    async fn overwrite_import_diffs_and_puts_only_changed_physical_rows() {
        let root_id = id(0, "ROOTOBJ", "ROOTOBJ#root");
        let bundle = DynamoBundle {
            version: BUNDLE_VERSION,
            root: root_id.clone(),
            recursive: false,
            excluded_subtrees: HashSet::new(),
            items: vec![DynamoBundleItem {
                id: root_id,
                parent: None,
                nesting: BundleNesting::Root,
                rows: vec![DynamoBundleRow {
                    sk_suffix: String::new(),
                    data: BTreeMap::from([(
                        "value".into(),
                        DynamoBundleValue::String("new".into()),
                    )]),
                }],
            }],
            references: vec![],
        };
        let old = HashMap::from([
            ("pk".into(), AttributeValue::S("ROOT".into())),
            ("sk".into(), AttributeValue::S("ROOTOBJ#root".into())),
            ("value".into(), AttributeValue::S("old".into())),
        ]);
        let mut backend = MockDynamoBackend::new();
        let conflict = old.clone();
        backend
            .expect_batch_get_item()
            .times(1)
            .returning(move |table, _, projection| {
                assert_eq!(projection.as_deref(), Some("pk, sk"));
                Ok(BatchGetItemOutput::builder()
                    .set_responses(Some(HashMap::from([(table, vec![conflict.clone()])])))
                    .build())
            });
        backend
            .expect_query()
            .times(1)
            .returning(move |_, _, _, _, _| {
                Ok(vec![QueryOutput::builder()
                    .set_items(Some(vec![old.clone()]))
                    .build()])
            });
        backend
            .expect_batch_put_item()
            .times(1)
            .returning(|_, items| {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0]["value"], AttributeValue::S("new".into()));
                Ok(BatchWriteItemOutput::builder().build())
            });
        let util = DynamoUtil {
            backend: Arc::new(backend),
            table: "table".into(),
        };

        let result =
            import::<TestRoot>(&util, &TestAlgorithms, None, bundle, IfExisting::Overwrite)
                .await
                .unwrap();

        assert_eq!(result.root_id.pk, "ROOT");
        assert_eq!(result.root_id.sk, "ROOTOBJ#root");
        assert_eq!(result.created, 0);
        assert_eq!(result.updated, 1);
        assert_eq!(result.deleted, 0);
        assert!(!result.duplicated);
    }
}
