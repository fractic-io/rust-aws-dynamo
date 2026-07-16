use std::collections::{HashMap, HashSet};

use fractic_server_error::ServerError;
use serde_json::Value;

use crate::{
    schema::id_calculations::{get_object_type, strip_ext_suffix},
    util::{AUTO_FIELDS_CREATED_AT, AUTO_FIELDS_UPDATED_AT},
};

use super::{invalid_bundle, BundleNesting, DynamoBundle, DynamoBundleReferenceTarget};

// Private interface.
// ----------------------------------------------------------------------------

pub(crate) fn validate_bundle(bundle: &DynamoBundle) -> Result<(), ServerError> {
    if bundle.version != DynamoBundle::VERSION {
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
        return Err(invalid_bundle("bundle IDs were invalid"));
    }
    for item in &bundle.items {
        let Value::Object(data) = &item.data else {
            return Err(invalid_bundle("bundle item data was not an object"));
        };
        if (item.id == bundle.root) != item.parent.is_none()
            || (item.id != bundle.root && item.nesting == BundleNesting::Root)
            || item
                .parent
                .as_ref()
                .is_some_and(|parent| !ids.contains(parent))
        {
            return Err(invalid_bundle("bundle item shape was invalid"));
        }
        if strip_ext_suffix(&item.id.original_sk) != item.id.original_sk
            || get_object_type("", &item.id.original_sk)? != item.id.label
        {
            return Err(invalid_bundle("bundle item ID metadata was invalid"));
        }
        if [
            "pk",
            "sk",
            "id",
            AUTO_FIELDS_CREATED_AT,
            AUTO_FIELDS_UPDATED_AT,
        ]
        .iter()
        .any(|reserved| data.contains_key(*reserved))
        {
            return Err(invalid_bundle(
                "bundle item data contained a reserved or regenerated field",
            ));
        }
    }
    let items = bundle
        .items
        .iter()
        .map(|item| (&item.id, item))
        .collect::<HashMap<_, _>>();
    let root = items
        .get(&bundle.root)
        .ok_or_else(|| invalid_bundle("bundle root item was missing"))?;
    if strip_ext_suffix(&bundle.source_root.sk) != bundle.source_root.sk
        || bundle.source_root.sk != root.id.original_sk
        || get_object_type(&bundle.source_root.pk, &bundle.source_root.sk)? != root.id.label
        || (root.nesting == BundleNesting::Root && bundle.source_root.pk != "ROOT")
    {
        return Err(invalid_bundle("bundle source root metadata was invalid"));
    }
    let mut reference_paths = HashSet::new();
    for reference in &bundle.references {
        let Some(source) = items.get(&reference.source) else {
            return Err(invalid_bundle("bundle reference source was invalid"));
        };
        if !reference_paths.insert((&reference.source, &reference.path)) {
            return Err(invalid_bundle(
                "bundle contained multiple references at the same path",
            ));
        }
        if !source
            .value_at(&reference.path)
            .is_some_and(Value::is_string)
        {
            return Err(invalid_bundle(
                "bundle reference path was invalid or not a string",
            ));
        }
        if matches!(
            &reference.target,
            DynamoBundleReferenceTarget::Bundled { id: target, .. } if !ids.contains(target)
        ) {
            return Err(invalid_bundle("bundle reference target was invalid"));
        }
        let clear_path = match &reference.target {
            DynamoBundleReferenceTarget::InTable { clear_path, .. }
            | DynamoBundleReferenceTarget::OutOfTable { clear_path, .. } => Some(clear_path),
            DynamoBundleReferenceTarget::Bundled { .. } => None,
        };
        if clear_path.is_some_and(|clear_path| {
            !clear_path.is_prefix_of(&reference.path) || source.value_at(clear_path).is_none()
        }) {
            return Err(invalid_bundle(
                "external reference clear path was not a containing value",
            ));
        }
    }
    Ok(())
}
