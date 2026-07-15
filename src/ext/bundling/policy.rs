use std::{
    collections::{hash_map::Entry, BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use fractic_server_error::ServerError;
use serde_json::Value;

use crate::{
    errors::DynamoInvalidOperation,
    ext::crud::DynamoCrudAlgorithms,
    schema::{DynamoFieldRename, DynamoObject, PkSk},
};

use super::{
    invalid_bundle, BundleDataPath, BundleIdLogic, DynamoBundle, DynamoBundleItem,
    DynamoBundleReferenceEncoding,
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
    omitted_descendants: BTreeSet<String>,
    reference_rules: Vec<DynamoBundleReferenceRule>,
}

/// An advanced reference selector for shapes not covered by the declarative
/// reference helpers on [`DynamoBundleObjectConfig`].
pub struct DynamoBundleReferenceRule {
    pub(crate) selector: Arc<BundleReferenceSelector>,
}

type BundleReferenceSelector =
    dyn Fn(&DynamoBundleItem) -> Result<Vec<DynamoBundleReferenceMatch>, ServerError> + Send + Sync;

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
    /// identical ID and rename behavior.
    pub fn include<O: DynamoObject>(&mut self) -> &mut DynamoBundleObjectPolicy {
        self.include_label(
            O::id_label(),
            BundleIdLogic::from_object::<O>(),
            O::renamed_fields(),
        )
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

    /// Preserves a same-table `PkSk` when it exists at the destination and
    /// clears it with a warning when it does not.
    pub fn external_pksk(&mut self, path: &'static str) -> &mut Self {
        let path = BundleDataPath::dotted(path);
        self.reference_rules
            .push(DynamoBundleReferenceRule::custom(move |item| {
                let Some(raw) = item.value_at(&path).and_then(Value::as_str) else {
                    return Ok(Vec::new());
                };
                Ok(vec![DynamoBundleReferenceMatch::external(
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

    pub fn external(path: BundleDataPath, lookup_id: PkSk) -> Self {
        Self::external_clearing(path.clone(), lookup_id, path)
    }

    /// Marks an external reference whose absence clears a containing optional
    /// value, such as an optional tuple holding more than one reference.
    pub fn external_clearing(
        path: BundleDataPath,
        lookup_id: PkSk,
        clear_path: BundleDataPath,
    ) -> Self {
        Self {
            path,
            target: DynamoBundleReferenceMatchTarget::External {
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
    External {
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
    pub(crate) fn omit_descendant_label(&mut self, label: &str) -> &mut Self {
        self.omitted_descendants.insert(label.to_owned());
        self
    }

    pub(crate) fn reference_rules(&self) -> &[DynamoBundleReferenceRule] {
        &self.reference_rules
    }

    pub(crate) fn normalize_renamed_fields(&self, data: &mut Value) {
        let Value::Object(data) = data else {
            return;
        };
        for renamed in self.renamed_fields {
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

    for item in &bundle.items {
        let local_id_logic = policy.require(&item.id.label)?.id_logic();
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
