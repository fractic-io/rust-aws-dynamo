use std::collections::{HashMap, HashSet};

use fractic_server_error::ServerError;
use serde_json::Value;

use crate::{
    errors::DynamoInvalidBundle,
    schema::{identifiers::RawIdPath, ForeignRef, PkSk},
};

use super::{
    entities_policy::{DynamoBundlePolicy, DynamoBundleReferenceMatchTarget},
    BundleDataPath, BundleId, DynamoBundle, DynamoBundleItem, DynamoBundleReferenceEncoding,
};

// Definitions.
// ----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BundleReference {
    pub source: BundleId,
    pub path: BundleDataPath,
    pub target: BundleReferenceTarget,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum BundleReferenceTarget {
    Bundled {
        id: BundleId,
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

// Internal interface.
// ----------------------------------------------------------------------------

pub(crate) fn derive_reference_manifest(
    policy: &DynamoBundlePolicy,
    bundle: &DynamoBundle,
    original_ids: &HashMap<PkSk, BundleId>,
) -> Result<Vec<BundleReference>, ServerError> {
    let mut references = Vec::new();
    for item in &bundle.items {
        let object = policy.require(&item.id.label)?;
        for rule in object.reference_rules() {
            for matched in (rule.selector)(item)? {
                let original = item
                    .value_at(&matched.path)
                    .ok_or_else(|| DynamoInvalidBundle::new("reference path was missing"))?;
                let target = match matched.target {
                    DynamoBundleReferenceMatchTarget::Bundled {
                        target_label,
                        encoding,
                    } => BundleReferenceTarget::Bundled {
                        id: find_bundled_target(
                            bundle,
                            original_ids,
                            item,
                            &matched.path,
                            original,
                            encoding,
                            Some(&target_label),
                        )?,
                        encoding,
                    },
                    DynamoBundleReferenceMatchTarget::InTable {
                        lookup_id,
                        clear_path,
                    } => BundleReferenceTarget::InTable {
                        lookup_id,
                        clear_path,
                    },
                    DynamoBundleReferenceMatchTarget::OutOfTable {
                        lookup_id,
                        clear_path,
                    } => BundleReferenceTarget::OutOfTable {
                        lookup_id,
                        clear_path,
                    },
                };
                references.push(BundleReference {
                    source: item.id.clone(),
                    path: matched.path,
                    target,
                });
            }
        }
    }
    validate_reference_manifest(bundle, &references)?;
    Ok(references)
}

pub(crate) fn derive_out_of_table_references(
    policy: &DynamoBundlePolicy,
    bundle: &DynamoBundle,
) -> Result<Vec<BundleReference>, ServerError> {
    let mut references = Vec::new();
    for item in &bundle.items {
        let object = policy.require(&item.id.label)?;
        for rule in object.reference_rules() {
            for matched in (rule.selector)(item)? {
                let DynamoBundleReferenceMatchTarget::OutOfTable {
                    lookup_id,
                    clear_path,
                } = matched.target
                else {
                    continue;
                };
                references.push(BundleReference {
                    source: item.id.clone(),
                    path: matched.path,
                    target: BundleReferenceTarget::OutOfTable {
                        lookup_id,
                        clear_path,
                    },
                });
            }
        }
    }
    validate_reference_manifest(bundle, &references)?;
    Ok(references)
}

// Helpers.
// ----------------------------------------------------------------------------

fn validate_reference_manifest(
    bundle: &DynamoBundle,
    references: &[BundleReference],
) -> Result<(), ServerError> {
    let items = bundle
        .items
        .iter()
        .map(|item| (&item.id, item))
        .collect::<HashMap<_, _>>();
    let mut reference_paths = HashSet::new();
    for reference in references {
        let source = items
            .get(&reference.source)
            .ok_or_else(|| DynamoInvalidBundle::new("reference source was invalid"))?;
        if !reference_paths.insert((&reference.source, &reference.path)) {
            return Err(DynamoInvalidBundle::new(
                "bundle policy produced multiple references at the same path",
            ));
        }
        if !source
            .value_at(&reference.path)
            .is_some_and(Value::is_string)
        {
            return Err(DynamoInvalidBundle::new(
                "reference path was invalid or not a string",
            ));
        }
        if matches!(
            &reference.target,
            BundleReferenceTarget::Bundled { id: target, .. } if !items.contains_key(target)
        ) {
            return Err(DynamoInvalidBundle::new("reference target was invalid"));
        }
        let clear_path = match &reference.target {
            BundleReferenceTarget::InTable { clear_path, .. }
            | BundleReferenceTarget::OutOfTable { clear_path, .. } => Some(clear_path),
            BundleReferenceTarget::Bundled { .. } => None,
        };
        if clear_path.is_some_and(|clear_path| {
            !clear_path.is_prefix_of(&reference.path) || source.value_at(clear_path).is_none()
        }) {
            return Err(DynamoInvalidBundle::new(
                "external reference clear path was not a containing value",
            ));
        }
    }
    Ok(())
}

fn find_bundled_target(
    bundle: &DynamoBundle,
    original_ids: &HashMap<PkSk, BundleId>,
    source: &DynamoBundleItem,
    path: &BundleDataPath,
    value: &Value,
    encoding: DynamoBundleReferenceEncoding,
    target_label: Option<&str>,
) -> Result<BundleId, ServerError> {
    let raw = value
        .as_str()
        .ok_or_else(|| DynamoInvalidBundle::new("reference value was not a string"))?;
    if encoding == DynamoBundleReferenceEncoding::PkSk {
        let id = PkSk::from_string(raw)
            .map_err(|_| DynamoInvalidBundle::new("pk/sk reference was invalid"))?;
        let target = original_ids
            .get(&id)
            .filter(|target| target_label.is_none_or(|label| target.label == label))
            .ok_or_else(|| missing_internal_target(bundle, source, path, raw, target_label))?;
        return Ok(target.clone());
    }

    if let Some(target) = unique_foreign_target(bundle, raw, target_label)? {
        return Ok(target.id.clone());
    }
    let reference = serde_json::from_value::<ForeignRef<'static>>(Value::String(raw.to_owned()))
        .map_err(|_| DynamoInvalidBundle::new("foreign reference was invalid"))?;
    unique_foreign_target(bundle, reference.raw(), target_label)?
        .map(|target| target.id.clone())
        .ok_or_else(|| missing_internal_target(bundle, source, path, raw, target_label))
}

fn unique_foreign_target<'a>(
    bundle: &'a DynamoBundle,
    reference: &str,
    target_label: Option<&str>,
) -> Result<Option<&'a DynamoBundleItem>, ServerError> {
    let mut matches = bundle.items.iter().filter(|item| {
        target_label.is_none_or(|label| item.id.label == label)
            && RawIdPath::new(&item.id.original_sk).foreign_ref_value() == reference
    });
    let target = matches.next();
    if target.is_some() && matches.next().is_some() {
        return Err(DynamoInvalidBundle::new(
            "bundled reference target was ambiguous",
        ));
    }
    Ok(target)
}

fn missing_internal_target(
    bundle: &DynamoBundle,
    source: &DynamoBundleItem,
    path: &BundleDataPath,
    raw_target: &str,
    target_label: Option<&str>,
) -> ServerError {
    DynamoInvalidBundle::new(&format!(
        "portable export rooted at `{}` was not closed: `{}` item `{}` path `{path}` requires \
         internal {}target `{raw_target}`, but that target was outside the exported scope",
        bundle.source_root,
        source.id.label,
        source.id.original_sk,
        target_label.map_or_else(String::new, |label| format!("`{label}` ")),
    ))
}
