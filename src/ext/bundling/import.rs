use std::collections::{BTreeMap, HashMap, HashSet};

use fractic_server_error::ServerError;
use futures_util::{stream, StreamExt as _, TryStreamExt as _};
use serde_json::Value;

use crate::{
    errors::DynamoInvalidOperation,
    ext::crud::DynamoCrudAlgorithms,
    schema::{
        id_calculations::{
            get_object_type, is_singleton, object_sk_component, place_inline_id, place_root_id,
            place_top_level_id, relative_child_sk, strip_ext_suffix,
        },
        parsing::build_dynamo_map_internal,
        DynamoObject, PkSk, Timestamp,
    },
    util::{
        collapse_helpers::{
            build_partition_write_plan_from_serialized_at, ext_partition_ids_for_count,
        },
        DynamoMap, DynamoUtil, AUTO_FIELDS_CREATED_AT, AUTO_FIELDS_SORT, AUTO_FIELDS_TTL,
        AUTO_FIELDS_UPDATED_AT, COLLAPSE_PLACEHOLDER_RESERVED_KEY,
    },
};

use super::{
    export::{collect_items, export_with_policy, terminal_ref},
    root_nesting,
    spec::{effective_import_exclusions, BundleSpecCache},
    value::{set_value_at_path, value_at_path},
    BundleId, BundleNesting, DynamoBundle, DynamoBundleItem, DynamoBundleReferenceEncoding,
    DynamoBundleReferenceTarget, DynamoBundleStorage, DynamoImportResult, DynamoImportWarning,
    IfExisting,
};

const DELETE_CONCURRENCY: usize = 16;

struct MergePlan {
    puts: Vec<DynamoMap>,
    maintenance_deletes: Vec<PkSk>,
}

struct ExistingState {
    conflicts: HashSet<PkSk>,
    ext_partition_counts: HashMap<PkSk, usize>,
}

pub(crate) async fn import_bundle<O: DynamoObject>(
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
    let effective_exclusions =
        effective_import_exclusions(&bundle, &mut BundleSpecCache::new(algorithms))?;

    let preserved_ids = build_id_map(&bundle, parent, false)?;
    let existing = find_existing(util, &preserved_ids).await?;
    let duplicated = !existing.conflicts.is_empty() && matches!(if_existing, IfExisting::Duplicate);
    if duplicated && is_singleton("", &bundle.root.original_sk) {
        return Err(DynamoInvalidOperation::new(
            "a singleton bundle root cannot be duplicated below the same parent",
        ));
    }
    let id_map = if duplicated {
        build_id_map(&bundle, parent, true)?
    } else {
        preserved_ids
    };
    if id_map.values().collect::<HashSet<_>>().len() != id_map.len() {
        return Err(invalid_bundle(
            "multiple bundle items mapped to the same destination ID",
        ));
    }
    let root_id = id_map
        .get(&bundle.root)
        .cloned()
        .ok_or_else(|| invalid_bundle("bundle root had no destination ID"))?;

    let old = if !existing.conflicts.is_empty() && matches!(if_existing, IfExisting::Replace) {
        Some(
            export_with_policy(
                util,
                algorithms,
                root_id.clone(),
                root_nesting::<O>(),
                bundle.recursive,
                &effective_exclusions,
            )
            .await?,
        )
    } else {
        None
    };

    let warnings = resolve_references(util, &mut bundle, &id_map).await?;
    let existing_partition_counts = if duplicated {
        HashMap::new()
    } else {
        existing.ext_partition_counts
    };
    let merge_plan = build_merge_plan(&bundle, &id_map, &existing_partition_counts)?;
    util.raw_batch_delete_ids(merge_plan.maintenance_deletes)
        .await?;
    util.raw_batch_put_item(merge_plan.puts).await?;

    let deleted_subtree_roots = match old {
        Some(old) => replace_stale(util, algorithms, parent, &old, &id_map).await?,
        None => 0,
    };

    Ok(DynamoImportResult {
        root_id,
        written_objects: bundle.items.len(),
        deleted_subtree_roots,
        duplicated,
        warnings,
    })
}

async fn find_existing(
    util: &DynamoUtil,
    ids: &HashMap<BundleId, PkSk>,
) -> Result<ExistingState, ServerError> {
    let rows = util
        .raw_batch_get_ids(ids.values().cloned().collect(), None)
        .await?;
    let mut conflicts = HashSet::new();
    let mut ext_partition_counts = HashMap::new();
    for row in rows {
        let id = PkSk::from_map(&row)?;
        if let Some(count) = row
            .get(COLLAPSE_PLACEHOLDER_RESERVED_KEY)
            .and_then(|value| value.as_n().ok())
            .and_then(|value| value.parse().ok())
        {
            ext_partition_counts.insert(id.clone(), count);
        }
        conflicts.insert(id);
    }
    Ok(ExistingState {
        conflicts,
        ext_partition_counts,
    })
}

async fn resolve_references(
    util: &DynamoUtil,
    bundle: &mut DynamoBundle,
    id_map: &HashMap<BundleId, PkSk>,
) -> Result<Vec<DynamoImportWarning>, ServerError> {
    let external_ids = bundle
        .references
        .iter()
        .filter_map(|reference| match &reference.target {
            DynamoBundleReferenceTarget::External { lookup_id, .. } => Some(lookup_id.clone()),
            DynamoBundleReferenceTarget::Internal { .. } => None,
        })
        .collect::<HashSet<_>>();
    let existing_external = util
        .raw_batch_get_ids(external_ids.into_iter().collect(), Some("pk, sk".into()))
        .await?
        .iter()
        .map(PkSk::from_map)
        .collect::<Result<HashSet<_>, _>>()?;

    let mut warnings = Vec::new();
    let mut missing_clears = Vec::new();
    for reference in bundle.references.clone() {
        let replacement = match &reference.target {
            DynamoBundleReferenceTarget::Internal {
                id: target,
                encoding,
            } => {
                let target = id_map
                    .get(target)
                    .ok_or_else(|| invalid_bundle("reference target had no destination ID"))?;
                Value::String(match encoding {
                    DynamoBundleReferenceEncoding::PkSk => target.to_string(),
                    DynamoBundleReferenceEncoding::ForeignRef => {
                        terminal_ref(&target.sk).to_string()
                    }
                })
            }
            DynamoBundleReferenceTarget::External { lookup_id, .. }
                if existing_external.contains(lookup_id) =>
            {
                continue;
            }
            DynamoBundleReferenceTarget::External { clear_path, .. } => {
                warnings.push(DynamoImportWarning::MissingExternalReference);
                missing_clears.push((reference.source, clear_path.clone()));
                continue;
            }
        };
        let item = bundle
            .items
            .iter_mut()
            .find(|item| item.id == reference.source)
            .ok_or_else(|| invalid_bundle("reference source was absent"))?;
        set_value_at_path(&mut item.data, &reference.path, replacement)?;
    }
    // Clear missing compound references after all remapping so clearing a
    // containing value cannot invalidate another nested reference path.
    for (source, clear_path) in missing_clears {
        let item = bundle
            .items
            .iter_mut()
            .find(|item| item.id == source)
            .ok_or_else(|| invalid_bundle("reference source was absent"))?;
        set_value_at_path(&mut item.data, &clear_path, Value::Null)?;
    }
    Ok(warnings)
}

fn build_merge_plan(
    bundle: &DynamoBundle,
    ids: &HashMap<BundleId, PkSk>,
    existing_partition_counts: &HashMap<PkSk, usize>,
) -> Result<MergePlan, ServerError> {
    let mut puts = Vec::new();
    let mut maintenance_deletes = Vec::new();
    let imported_at = Timestamp::now();
    for item in &bundle.items {
        let id = ids
            .get(&item.id)
            .ok_or_else(|| invalid_bundle("bundle item had no destination ID"))?;
        match item.storage {
            DynamoBundleStorage::Standard => {
                let map = build_dynamo_map_internal(
                    &item.data,
                    Some(id.pk.clone()),
                    Some(id.sk.clone()),
                    Some(vec![
                        (AUTO_FIELDS_CREATED_AT, Box::new(imported_at.clone())),
                        (AUTO_FIELDS_UPDATED_AT, Box::new(imported_at.clone())),
                    ]),
                )?
                .0;
                puts.push(map);
                if let Some(count) = existing_partition_counts.get(id) {
                    maintenance_deletes.extend(ext_partition_ids_for_count(id, *count));
                }
            }
            DynamoBundleStorage::ExtPartitioned => {
                let (serialized, sort, ttl) = partition_payload(&item.data)?;
                let plan = build_partition_write_plan_from_serialized_at(
                    id,
                    &serialized,
                    sort,
                    ttl,
                    existing_partition_counts.get(id).copied(),
                    &imported_at,
                )?;
                puts.extend(plan.put_items);
                maintenance_deletes.extend(plan.stale_delete_ids);
            }
        }
    }
    Ok(MergePlan {
        puts,
        maintenance_deletes,
    })
}

fn partition_payload(data: &Value) -> Result<(String, Option<f64>, Option<i64>), ServerError> {
    let Value::Object(mut object) = data.clone() else {
        return Err(invalid_bundle("partitioned item data was not an object"));
    };
    let sort = object
        .remove(AUTO_FIELDS_SORT)
        .and_then(|value| value.as_f64());
    let ttl = object
        .remove(AUTO_FIELDS_TTL)
        .and_then(|value| value.as_i64());
    object.remove(AUTO_FIELDS_CREATED_AT);
    object.remove(AUTO_FIELDS_UPDATED_AT);
    Ok((
        serde_json::to_string(&Value::Object(object)).map_err(|error| {
            DynamoInvalidOperation::with_debug(
                "failed to serialize partitioned bundle item",
                &error,
            )
        })?,
        sort,
        ttl,
    ))
}

async fn replace_stale(
    util: &DynamoUtil,
    algorithms: &dyn DynamoCrudAlgorithms,
    parent: Option<&PkSk>,
    old: &DynamoBundle,
    new_ids: &HashMap<BundleId, PkSk>,
) -> Result<usize, ServerError> {
    let old_ids = build_id_map(old, parent, false)?;
    let desired = new_ids.values().cloned().collect::<HashSet<_>>();
    let stale = old
        .items
        .iter()
        .filter(|item| {
            old_ids
                .get(&item.id)
                .is_some_and(|id| !desired.contains(id))
        })
        .map(|item| item.id.clone())
        .collect::<HashSet<_>>();
    let stale_roots = old
        .items
        .iter()
        .filter(|item| stale.contains(&item.id))
        .filter(|item| {
            item.parent
                .as_ref()
                .is_none_or(|parent| !stale.contains(parent))
        })
        .map(|item| {
            Ok((
                old_ids
                    .get(&item.id)
                    .cloned()
                    .ok_or_else(|| invalid_bundle("stale item had no destination ID"))?,
                item.nesting,
            ))
        })
        .collect::<Result<Vec<_>, ServerError>>()?;

    let delete_groups = stream::iter(stale_roots.iter().cloned())
        .map(|(id, nesting)| async move {
            let no_exclusions = BTreeMap::new();
            let (items, _) =
                collect_items(util, algorithms, &id, nesting, true, Some(&no_exclusions)).await?;
            items
                .into_iter()
                .flat_map(|item| item.rows)
                .map(|row| PkSk::from_map(&row))
                .collect::<Result<Vec<_>, ServerError>>()
        })
        .buffer_unordered(DELETE_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
    util.raw_batch_delete_ids(delete_groups.into_iter().flatten().collect())
        .await?;
    Ok(stale_roots.len())
}

pub(crate) fn build_id_map(
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
            return Err(invalid_bundle("bundle item parent graph was invalid"));
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
                    "root object cannot be imported below a parent",
                ));
            }
            Ok(place_root_id(&item.id.original_sk, duplicate))
        }
        BundleNesting::TopLevel => {
            let parent = parent.ok_or_else(|| invalid_bundle("child bundle requires a parent"))?;
            Ok(place_top_level_id(parent, &item.id.original_sk, duplicate))
        }
        BundleNesting::Inline => {
            let parent = parent.ok_or_else(|| invalid_bundle("child bundle requires a parent"))?;
            let component = object_sk_component(&item.id.original_sk, &item.id.label)?;
            Ok(place_inline_id(parent, component, duplicate))
        }
    }
}

fn place_child(
    item: &DynamoBundleItem,
    parent: &PkSk,
    duplicate: bool,
) -> Result<PkSk, ServerError> {
    match item.nesting {
        BundleNesting::TopLevel => Ok(place_top_level_id(parent, &item.id.original_sk, duplicate)),
        BundleNesting::Inline => {
            let original_parent = item
                .parent
                .as_ref()
                .ok_or_else(|| invalid_bundle("inline child had no parent"))?;
            let relative = relative_child_sk(&item.id.original_sk, &original_parent.original_sk)?;
            Ok(place_inline_id(parent, relative, duplicate))
        }
        BundleNesting::Root => Err(invalid_bundle("non-root item had root nesting")),
    }
}

fn validate_bundle(bundle: &DynamoBundle) -> Result<(), ServerError> {
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
        if !value_at_path(&source.data, &reference.path).is_some_and(Value::is_string) {
            return Err(invalid_bundle(
                "bundle reference path was invalid or not a string",
            ));
        }
        if matches!(
            &reference.target,
            DynamoBundleReferenceTarget::Internal { id: target, .. } if !ids.contains(target)
        ) {
            return Err(invalid_bundle("bundle reference target was invalid"));
        }
        if let DynamoBundleReferenceTarget::External { clear_path, .. } = &reference.target {
            let is_containing_path = clear_path.0.len() <= reference.path.0.len()
                && reference.path.0.starts_with(&clear_path.0);
            if !is_containing_path || value_at_path(&source.data, clear_path).is_none() {
                return Err(invalid_bundle(
                    "external reference clear path was not a containing value",
                ));
            }
        }
    }
    Ok(())
}

fn invalid_bundle(details: &str) -> ServerError {
    DynamoInvalidOperation::new(&format!("invalid Dynamo bundle: {details}"))
}
