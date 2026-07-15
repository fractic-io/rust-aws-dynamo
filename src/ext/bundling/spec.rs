use std::collections::{BTreeMap, BTreeSet, HashMap};

use fractic_server_error::ServerError;

use crate::{errors::DynamoInvalidOperation, ext::crud::DynamoCrudAlgorithms};

use super::{DynamoBundle, DynamoBundleSpec};

/// Loads each label's application configuration at most once per operation.
pub(crate) struct BundleSpecCache<'a> {
    algorithms: &'a dyn DynamoCrudAlgorithms,
    specs: HashMap<String, Option<DynamoBundleSpec>>,
}

impl<'a> BundleSpecCache<'a> {
    pub(crate) fn new(algorithms: &'a dyn DynamoCrudAlgorithms) -> Self {
        Self {
            algorithms,
            specs: HashMap::new(),
        }
    }

    pub(crate) fn get(&mut self, label: &str) -> Option<&DynamoBundleSpec> {
        if !self.specs.contains_key(label) {
            self.specs
                .insert(label.to_string(), self.algorithms.bundle_spec(label));
        }
        self.specs.get(label).and_then(Option::as_ref)
    }

    pub(crate) fn exclusions(&mut self, label: &str) -> BTreeSet<String> {
        self.get(label)
            .map(|spec| spec.exclude_subtrees.clone())
            .unwrap_or_default()
    }
}

/// Combines the portable policy with the current application's policy. The
/// union is intentionally the stricter policy: neither source can cause a
/// subtree protected by the other source to be managed by Replace.
pub(crate) fn effective_import_exclusions(
    bundle: &DynamoBundle,
    specs: &mut BundleSpecCache<'_>,
) -> Result<BTreeMap<String, BTreeSet<String>>, ServerError> {
    let labels = bundle
        .items
        .iter()
        .map(|item| item.id.label.as_str())
        .collect::<BTreeSet<_>>();
    let mut effective = bundle.exclusions.clone();

    for (owner, exclusions) in &bundle.exclusions {
        if !labels.contains(owner.as_str()) {
            return Err(invalid_bundle(
                "exclusion policy referenced an owner label absent from the bundle",
            ));
        }
        if owner.is_empty() || exclusions.iter().any(String::is_empty) {
            return Err(invalid_bundle("exclusion policy contained an empty label"));
        }
    }

    for label in labels {
        let local = specs.exclusions(label);
        if !local.is_empty() {
            effective
                .entry(label.to_string())
                .or_default()
                .extend(local);
        }
    }
    Ok(effective)
}

fn invalid_bundle(details: &str) -> ServerError {
    DynamoInvalidOperation::new(&format!("invalid Dynamo bundle: {details}"))
}
