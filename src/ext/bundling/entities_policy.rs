use std::{
    collections::{hash_map::Entry, BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use fractic_server_error::ServerError;
use serde_json::Value;

use crate::{
    errors::DynamoInvalidOperation,
    ext::crud::DynamoCrudAlgorithms,
    schema::{DynamoFieldRename, DynamoObject, IdLogic, NestingLogic, PkSk},
    util::{AUTO_FIELDS_SORT, AUTO_FIELDS_TTL, EXPAND_DATA_RESERVED_KEY},
};

use super::{
    invalid_bundle, BundleDataPath, BundleId, BundleIdLogic, BundleNesting, DynamoBundle,
    DynamoBundleItem, DynamoBundleReferenceEncoding,
};

// Definitions.
// ----------------------------------------------------------------------------

/// Declarative bundle configuration for one DynamoDB table.
///
/// Objects are denied by default. Register every bundleable object with
/// [`include`](Self::include), then describe omitted descendant subtrees and
/// references on the returned object configuration.
#[derive(Default)]
pub struct DynamoBundlePolicy {
    objects: HashMap<&'static str, DynamoBundleObjectPolicy>,
}

/// Bundle behavior for one included Dynamo object type.
pub struct DynamoBundleObjectPolicy {
    id_logic: BundleIdLogic,
    renamed_fields: &'static [DynamoFieldRename],
    schema_variants: Vec<DynamoBundleSchemaVariant>,
    omitted_descendants: BTreeSet<String>,
    reference_rules: Vec<DynamoBundleReferenceRule>,
}

struct DynamoBundleSchemaVariant {
    type_name: &'static str,
    topology: DynamoBundleTopology,
    validate_data: Arc<BundleDataValidator>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DynamoBundleTopology {
    nesting: BundleNesting,
    parent: DynamoBundleParentRequirement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DynamoBundleParentRequirement {
    None,
    Any,
    Label(&'static str),
}

/// An advanced reference selector for shapes not covered by the declarative
/// reference helpers on [`DynamoBundleObjectConfig`].
pub struct DynamoBundleReferenceRule {
    pub(crate) selector: Arc<BundleReferenceSelector>,
}

type BundleReferenceSelector =
    dyn Fn(&DynamoBundleItem) -> Result<Vec<DynamoBundleReferenceMatch>, ServerError> + Send + Sync;

type BundleDataValidator = dyn Fn(&DynamoBundleItem) -> Result<(), String> + Send + Sync;

/// A reference discovered by a custom bundle reference rule.
#[derive(Debug, Clone)]
pub struct DynamoBundleReferenceMatch {
    pub path: BundleDataPath,
    pub(crate) target: DynamoBundleReferenceMatchTarget,
}

// Public interface.
// ----------------------------------------------------------------------------

impl DynamoBundlePolicy {
    pub fn new() -> Self {
        Self::default()
    }

    /// Includes an object type and returns its configuration builder.
    ///
    /// Types sharing a persisted label share one configuration and must have
    /// identical ID and rename behavior. Each type contributes its own allowed
    /// topology and data shape for import validation.
    pub fn include<O: DynamoObject>(&mut self) -> &mut DynamoBundleObjectPolicy {
        let object = self.include_label(
            O::id_label(),
            BundleIdLogic::from_object::<O>(),
            O::renamed_fields(),
        );
        object.register_schema::<O>();
        object
    }

    pub fn object<O: DynamoObject>(&self) -> Option<&DynamoBundleObjectPolicy> {
        self.objects.get(O::id_label())
    }

    pub fn contains<O: DynamoObject>(&self) -> bool {
        self.object::<O>().is_some()
    }

    pub fn contains_label(&self, label: &str) -> bool {
        self.objects.contains_key(label)
    }
}

impl DynamoBundleObjectPolicy {
    pub fn id_logic(&self) -> BundleIdLogic {
        self.id_logic
    }

    pub fn omitted_descendants(&self) -> &BTreeSet<String> {
        &self.omitted_descendants
    }

    pub fn reference_rule_count(&self) -> usize {
        self.reference_rules.len()
    }

    /// Omits every subtree with the given object label beneath this object.
    /// The omitted object can still be included elsewhere or exported as a
    /// root when it is registered separately.
    pub fn omit_descendants<O: DynamoObject>(&mut self) -> &mut Self {
        self.omit_descendant_label(O::id_label())
    }

    /// Remaps a `ForeignRef` whose target must be present in the bundle.
    pub fn bundled_foreign_ref<O: DynamoObject>(&mut self, path: &'static str) -> &mut Self {
        self.bundled_reference::<O>(path, DynamoBundleReferenceEncoding::ForeignRef)
    }

    /// Remaps a `PkSk` whose target must be present in the bundle.
    pub fn bundled_pksk<O: DynamoObject>(&mut self, path: &'static str) -> &mut Self {
        self.bundled_reference::<O>(path, DynamoBundleReferenceEncoding::PkSk)
    }

    /// Remaps every `PkSk` in an array. Every target must be in the bundle.
    pub fn bundled_pksk_each<O: DynamoObject>(&mut self, path: &'static str) -> &mut Self {
        let list_path = BundleDataPath::dotted(path);
        let target_label = O::id_label().to_owned();
        self.reference_rules
            .push(DynamoBundleReferenceRule::custom(move |item| {
                let Some(values) = item.value_at(&list_path).and_then(Value::as_array) else {
                    return Ok(Vec::new());
                };
                Ok(values
                    .iter()
                    .enumerate()
                    .filter(|(_, value)| value.is_string())
                    .map(|(index, _)| {
                        DynamoBundleReferenceMatch::bundled_label(
                            list_path.clone().then_index(index),
                            DynamoBundleReferenceEncoding::PkSk,
                            target_label.clone(),
                        )
                    })
                    .collect())
            }));
        self
    }

    /// Preserves an out-of-bundle, same-table `PkSk` when it exists at the
    /// destination and clears it with a warning when it does not.
    pub fn in_table_pksk(&mut self, path: &'static str) -> &mut Self {
        let path = BundleDataPath::dotted(path);
        self.reference_rules
            .push(DynamoBundleReferenceRule::custom(move |item| {
                let Some(raw) = item.value_at(&path).and_then(Value::as_str) else {
                    return Ok(Vec::new());
                };
                Ok(vec![DynamoBundleReferenceMatch::in_table(
                    path.clone(),
                    PkSk::from_string(raw)?,
                )])
            }));
        self
    }

    /// Preserves an out-of-table `PkSk` on Merge and Replace without checking
    /// it, and clears it with a warning when importing as New.
    pub fn out_of_table_pksk(&mut self, path: &'static str) -> &mut Self {
        let path = BundleDataPath::dotted(path);
        self.reference_rules
            .push(DynamoBundleReferenceRule::custom(move |item| {
                let Some(raw) = item.value_at(&path).and_then(Value::as_str) else {
                    return Ok(Vec::new());
                };
                Ok(vec![DynamoBundleReferenceMatch::out_of_table(
                    path.clone(),
                    PkSk::from_string(raw)?,
                )])
            }));
        self
    }

    /// Adds an advanced reference selector for a compound or derived value.
    pub fn custom_references(&mut self, rule: DynamoBundleReferenceRule) -> &mut Self {
        self.reference_rules.push(rule);
        self
    }
}

impl DynamoBundleReferenceRule {
    pub fn custom(
        selector: impl Fn(&DynamoBundleItem) -> Result<Vec<DynamoBundleReferenceMatch>, ServerError>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        Self {
            selector: Arc::new(selector),
        }
    }
}

impl DynamoBundleReferenceMatch {
    pub fn bundled<O: DynamoObject>(
        path: BundleDataPath,
        encoding: DynamoBundleReferenceEncoding,
    ) -> Self {
        Self::bundled_label(path, encoding, O::id_label().to_owned())
    }

    pub fn in_table(path: BundleDataPath, lookup_id: PkSk) -> Self {
        Self::in_table_clearing(path.clone(), lookup_id, path)
    }

    /// Marks an in-table reference whose absence clears a containing optional
    /// value, such as an optional tuple holding more than one reference.
    pub fn in_table_clearing(
        path: BundleDataPath,
        lookup_id: PkSk,
        clear_path: BundleDataPath,
    ) -> Self {
        Self {
            path,
            target: DynamoBundleReferenceMatchTarget::InTable {
                lookup_id,
                clear_path,
            },
        }
    }

    pub fn out_of_table(path: BundleDataPath, lookup_id: PkSk) -> Self {
        Self::out_of_table_clearing(path.clone(), lookup_id, path)
    }

    /// Marks an out-of-table reference whose containing optional value is
    /// cleared when importing as New.
    pub fn out_of_table_clearing(
        path: BundleDataPath,
        lookup_id: PkSk,
        clear_path: BundleDataPath,
    ) -> Self {
        Self {
            path,
            target: DynamoBundleReferenceMatchTarget::OutOfTable {
                lookup_id,
                clear_path,
            },
        }
    }
}

// Private interface.
// ----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) enum DynamoBundleReferenceMatchTarget {
    Bundled {
        target_label: String,
        encoding: DynamoBundleReferenceEncoding,
    },
    InTable {
        lookup_id: PkSk,
        clear_path: BundleDataPath,
    },
    OutOfTable {
        lookup_id: PkSk,
        clear_path: BundleDataPath,
    },
}

impl DynamoBundlePolicy {
    pub(crate) fn include_label(
        &mut self,
        label: &'static str,
        id_logic: BundleIdLogic,
        renamed_fields: &'static [DynamoFieldRename],
    ) -> &mut DynamoBundleObjectPolicy {
        match self.objects.entry(label) {
            Entry::Vacant(entry) => entry.insert(DynamoBundleObjectPolicy {
                id_logic,
                renamed_fields,
                schema_variants: Vec::new(),
                omitted_descendants: BTreeSet::new(),
                reference_rules: Vec::new(),
            }),
            Entry::Occupied(entry) => {
                assert_eq!(
                    entry.get().id_logic,
                    id_logic,
                    "bundle types sharing label `{label}` used different ID behavior"
                );
                assert_eq!(
                    entry.get().renamed_fields,
                    renamed_fields,
                    "bundle types sharing label `{label}` used different field renames"
                );
                entry.into_mut()
            }
        }
    }

    pub(crate) fn get(&self, label: &str) -> Option<&DynamoBundleObjectPolicy> {
        self.objects.get(label)
    }

    pub(crate) fn require(&self, label: &str) -> Result<&DynamoBundleObjectPolicy, ServerError> {
        self.get(label).ok_or_else(|| {
            DynamoInvalidOperation::new(&format!(
                "Dynamo object label `{label}` is not allowed in bundles"
            ))
        })
    }
}

impl DynamoBundleObjectPolicy {
    fn register_schema<O: DynamoObject>(&mut self) {
        let type_name = std::any::type_name::<O>();
        if self
            .schema_variants
            .iter()
            .any(|variant| variant.type_name == type_name)
        {
            return;
        }
        let topology = DynamoBundleTopology::from_nesting(O::nesting_logic());
        let validate_data = Arc::new(|item: &DynamoBundleItem| validate_item_data::<O>(item));
        self.schema_variants.push(DynamoBundleSchemaVariant {
            type_name,
            topology,
            validate_data,
        });
    }

    pub(crate) fn omit_descendant_label(&mut self, label: &str) -> &mut Self {
        self.omitted_descendants.insert(label.to_owned());
        self
    }

    pub(crate) fn reference_rules(&self) -> &[DynamoBundleReferenceRule] {
        &self.reference_rules
    }

    pub(crate) fn normalize_renamed_fields(&self, data: &mut Value) {
        normalize_renamed_fields(data, self.renamed_fields);
    }

    fn validate_schema(
        &self,
        item: &DynamoBundleItem,
        parent: BundleItemParent<'_>,
    ) -> Result<(), ServerError> {
        if self.schema_variants.is_empty() {
            return Ok(());
        }
        let matching = self
            .schema_variants
            .iter()
            .filter(|variant| variant.topology.matches(item.nesting, parent))
            .collect::<Vec<_>>();
        if matching.is_empty() {
            return Err(invalid_bundle(&format!(
                "item label `{}` used {:?} nesting below {}, which does not match any local schema",
                item.id.label,
                item.nesting,
                parent.description(),
            )));
        }

        let mut errors = Vec::new();
        for variant in matching {
            match (variant.validate_data)(item) {
                Ok(()) => return Ok(()),
                Err(error) => errors.push(format!("{}: {error}", variant.type_name)),
            }
        }
        Err(invalid_bundle(&format!(
            "item label `{}` data did not match its local schema: {}",
            item.id.label,
            errors.join("; "),
        )))
    }
}

impl DynamoBundleTopology {
    fn from_nesting(nesting: NestingLogic) -> Self {
        match nesting {
            NestingLogic::Root => Self {
                nesting: BundleNesting::Root,
                parent: DynamoBundleParentRequirement::None,
            },
            NestingLogic::TopLevelChildOf(label) => Self {
                nesting: BundleNesting::TopLevel,
                parent: DynamoBundleParentRequirement::Label(label),
            },
            NestingLogic::TopLevelChildOfAny => Self {
                nesting: BundleNesting::TopLevel,
                parent: DynamoBundleParentRequirement::Any,
            },
            NestingLogic::InlineChildOf(label) => Self {
                nesting: BundleNesting::Inline,
                parent: DynamoBundleParentRequirement::Label(label),
            },
            NestingLogic::InlineChildOfAny => Self {
                nesting: BundleNesting::Inline,
                parent: DynamoBundleParentRequirement::Any,
            },
        }
    }

    fn matches(&self, nesting: BundleNesting, parent: BundleItemParent<'_>) -> bool {
        self.nesting == nesting && self.parent.matches(parent)
    }
}

impl DynamoBundleParentRequirement {
    fn matches(self, parent: BundleItemParent<'_>) -> bool {
        match self {
            Self::None => matches!(parent, BundleItemParent::None),
            Self::Any => !matches!(parent, BundleItemParent::None),
            Self::Label(expected) => parent.label().is_some_and(|actual| actual == expected),
        }
    }
}

#[derive(Clone, Copy)]
enum BundleItemParent<'a> {
    None,
    Bundled(&'a str),
    External(&'a PkSk),
}

impl<'a> BundleItemParent<'a> {
    fn label(self) -> Option<&'a str> {
        match self {
            Self::None => None,
            Self::Bundled(label) => Some(label),
            Self::External(id) => id.object_type().ok(),
        }
    }

    fn description(self) -> String {
        match self {
            Self::None => "no parent".to_owned(),
            Self::Bundled(label) => format!("bundled parent label `{label}`"),
            Self::External(id) => format!("destination parent `{id}`"),
        }
    }
}

fn validate_item_data<O: DynamoObject>(item: &DynamoBundleItem) -> Result<(), String> {
    if matches!(O::id_logic(), IdLogic::BatchOptimized { .. }) {
        let values = item
            .data
            .get(EXPAND_DATA_RESERVED_KEY)
            .and_then(Value::as_array)
            .ok_or_else(|| {
                format!(
                    "batch-optimized data did not contain an `{EXPAND_DATA_RESERVED_KEY}` array"
                )
            })?;
        for (index, value) in values.iter().enumerate() {
            validate_data_value::<O>(value)
                .map_err(|error| format!("batch member {index} was invalid: {error}"))?;
        }
        return Ok(());
    }
    validate_data_value::<O>(&item.data)
}

fn validate_data_value<O: DynamoObject>(data: &Value) -> Result<(), String> {
    let mut data = data.clone();
    normalize_renamed_fields(&mut data, O::renamed_fields());
    if let Value::Object(data) = &mut data {
        data.remove(AUTO_FIELDS_SORT);
        data.remove(AUTO_FIELDS_TTL);
    }
    serde_json::from_value::<O::Data>(data)
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn normalize_renamed_fields(data: &mut Value, renamed_fields: &[DynamoFieldRename]) {
    let Value::Object(data) = data else {
        return;
    };
    for renamed in renamed_fields {
        if renamed.is_noop() {
            continue;
        }
        if data.contains_key(renamed.to) {
            data.remove(renamed.from);
        } else if let Some(value) = data.remove(renamed.from) {
            data.insert(renamed.to.to_owned(), value);
        }
    }
}

pub(crate) fn configured_bundle_policy(
    algorithms: &dyn DynamoCrudAlgorithms,
) -> DynamoBundlePolicy {
    let mut policy = DynamoBundlePolicy::new();
    algorithms.bundle_policy(&mut policy);
    policy
}

/// Validates portable ID behavior against the importing application and
/// combines both sources' omission policies. The union is intentionally
/// stricter: neither source can cause a subtree protected by the other source
/// to be managed by Replace.
pub(crate) fn validate_import_policy(
    bundle: &DynamoBundle,
    policy: &DynamoBundlePolicy,
    root_id_logic: BundleIdLogic,
    destination_parent: Option<&PkSk>,
) -> Result<BTreeMap<String, BTreeSet<String>>, ServerError> {
    let labels = bundle
        .items
        .iter()
        .map(|item| item.id.label.as_str())
        .collect::<BTreeSet<_>>();
    let mut effective = bundle.omitted_descendants.clone();

    for (owner, omissions) in &bundle.omitted_descendants {
        if !labels.contains(owner.as_str()) {
            return Err(invalid_bundle(
                "omission policy referenced an owner label absent from the bundle",
            ));
        }
        if owner.is_empty() || omissions.iter().any(String::is_empty) {
            return Err(invalid_bundle("omission policy contained an empty label"));
        }
    }

    for label in labels {
        let local = policy.require(label)?;
        if !local.omitted_descendants().is_empty() {
            effective
                .entry(label.to_owned())
                .or_default()
                .extend(local.omitted_descendants().iter().cloned());
        }
    }

    let items = bundle
        .items
        .iter()
        .map(|item| (&item.id, item))
        .collect::<HashMap<&BundleId, &DynamoBundleItem>>();
    for item in &bundle.items {
        let local = policy.require(&item.id.label)?;
        let local_id_logic = local.id_logic();
        let expected = if item.id == bundle.root {
            if local_id_logic != root_id_logic {
                return Err(invalid_bundle(&format!(
                    "root ID logic {root_id_logic:?} did not match local policy {local_id_logic:?}"
                )));
            }
            root_id_logic
        } else {
            local_id_logic
        };
        if item.id_logic != expected {
            return Err(invalid_bundle(&format!(
                "item label `{}` used ID logic {:?}, but the local policy requires {expected:?}",
                item.id.label, item.id_logic
            )));
        }
        let parent = if item.id == bundle.root {
            destination_parent.map_or(BundleItemParent::None, BundleItemParent::External)
        } else {
            let parent = item
                .parent
                .as_ref()
                .and_then(|parent| items.get(parent))
                .ok_or_else(|| invalid_bundle("bundle item parent was missing"))?;
            BundleItemParent::Bundled(&parent.id.label)
        };
        local.validate_schema(item, parent)?;
    }
    Ok(effective)
}

impl DynamoBundleReferenceMatch {
    pub(crate) fn bundled_label(
        path: BundleDataPath,
        encoding: DynamoBundleReferenceEncoding,
        target_label: impl Into<String>,
    ) -> Self {
        Self {
            path,
            target: DynamoBundleReferenceMatchTarget::Bundled {
                target_label: target_label.into(),
                encoding,
            },
        }
    }
}

// Helpers.
// ----------------------------------------------------------------------------

impl DynamoBundleObjectPolicy {
    fn bundled_reference<O: DynamoObject>(
        &mut self,
        path: &'static str,
        encoding: DynamoBundleReferenceEncoding,
    ) -> &mut Self {
        let path = BundleDataPath::dotted(path);
        let target_label = O::id_label().to_owned();
        self.reference_rules
            .push(DynamoBundleReferenceRule::at_bundled_path(
                path,
                encoding,
                target_label,
            ));
        self
    }
}

impl DynamoBundleReferenceRule {
    fn at_bundled_path(
        path: BundleDataPath,
        encoding: DynamoBundleReferenceEncoding,
        target_label: String,
    ) -> Self {
        Self::custom(move |item| {
            Ok(item.value_at(&path).map_or_else(Vec::new, |_| {
                vec![DynamoBundleReferenceMatch::bundled_label(
                    path.clone(),
                    encoding,
                    target_label.clone(),
                )]
            }))
        })
    }
}
