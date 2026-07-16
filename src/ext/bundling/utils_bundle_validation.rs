use std::collections::{HashMap, HashSet};

use fractic_server_error::ServerError;
use serde_json::Value;

use crate::{
    errors::DynamoInvalidBundle,
    schema::identifiers::{SortKey, ROOT_KEY},
    util::{AUTO_FIELDS_CREATED_AT, AUTO_FIELDS_UPDATED_AT},
};

use super::{BundleNesting, DynamoBundle, DynamoBundleReferenceTarget};

// Private interface.
// ----------------------------------------------------------------------------

pub(crate) fn validate_bundle(bundle: &DynamoBundle) -> Result<(), ServerError> {
    if bundle.version != DynamoBundle::VERSION {
        return Err(DynamoInvalidBundle::new("unsupported bundle version"));
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
        return Err(DynamoInvalidBundle::new("bundle IDs were invalid"));
    }
    for item in &bundle.items {
        let Value::Object(data) = &item.data else {
            return Err(DynamoInvalidBundle::new(
                "bundle item data was not an object",
            ));
        };
        if (item.id == bundle.root) != item.parent.is_none()
            || (item.id != bundle.root && item.nesting == BundleNesting::Root)
            || item
                .parent
                .as_ref()
                .is_some_and(|parent| !ids.contains(parent))
        {
            return Err(DynamoInvalidBundle::new("bundle item shape was invalid"));
        }
        let sort_key = SortKey::new(&item.id.original_sk);
        if sort_key.logical() != item.id.original_sk || sort_key.object_label()? != item.id.label {
            return Err(DynamoInvalidBundle::new(
                "bundle item ID metadata was invalid",
            ));
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
            return Err(DynamoInvalidBundle::new(
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
        .ok_or_else(|| DynamoInvalidBundle::new("bundle root item was missing"))?;
    let root_sort_key = SortKey::new(&bundle.source_root.sk);
    if root_sort_key.logical() != bundle.source_root.sk
        || bundle.source_root.sk != root.id.original_sk
        || root_sort_key.object_label()? != root.id.label
        || (root.nesting == BundleNesting::Root && bundle.source_root.pk != ROOT_KEY)
    {
        return Err(DynamoInvalidBundle::new(
            "bundle source root metadata was invalid",
        ));
    }
    let mut reference_paths = HashSet::new();
    for reference in &bundle.references {
        let Some(source) = items.get(&reference.source) else {
            return Err(DynamoInvalidBundle::new(
                "bundle reference source was invalid",
            ));
        };
        if !reference_paths.insert((&reference.source, &reference.path)) {
            return Err(DynamoInvalidBundle::new(
                "bundle contained multiple references at the same path",
            ));
        }
        if !source
            .value_at(&reference.path)
            .is_some_and(Value::is_string)
        {
            return Err(DynamoInvalidBundle::new(
                "bundle reference path was invalid or not a string",
            ));
        }
        if matches!(
            &reference.target,
            DynamoBundleReferenceTarget::Bundled { id: target, .. } if !ids.contains(target)
        ) {
            return Err(DynamoInvalidBundle::new(
                "bundle reference target was invalid",
            ));
        }
        let clear_path = match &reference.target {
            DynamoBundleReferenceTarget::InTable { clear_path, .. }
            | DynamoBundleReferenceTarget::OutOfTable { clear_path, .. } => Some(clear_path),
            DynamoBundleReferenceTarget::Bundled { .. } => None,
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
