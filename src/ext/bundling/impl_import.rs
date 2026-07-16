use std::collections::{BTreeMap, HashMap, HashSet};

use fractic_server_error::{CriticalError, ServerError};
use futures_util::{stream, StreamExt as _, TryStreamExt as _};
use serde_json::Value;

use crate::{
    errors::{DynamoInvalidBundle, DynamoInvalidOperation},
    ext::crud::DynamoCrudAlgorithms,
    schema::{parsing::build_dynamo_map_internal, DynamoObject, PkSk, Timestamp},
    util::{
        calculate_sort_values,
        collapse_helpers::{
            build_partition_write_plan_from_serialized_at, ext_partition_ids_for_count,
        },
        DynamoInsertPosition, DynamoMap, DynamoUtil, AUTO_FIELDS_CREATED_AT, AUTO_FIELDS_SORT,
        AUTO_FIELDS_TTL, AUTO_FIELDS_UPDATED_AT, COLLAPSE_PLACEHOLDER_RESERVED_KEY,
    },
};

use super::{
    entities_policy::{configured_bundle_policy, validate_import_policy},
    impl_export::{collect_bundle_items, export_with_omissions},
    impl_mapping::build_id_map,
    impl_utils::set_value_at_path,
    impl_validation::validate_bundle,
    root_nesting, BundleId, BundleIdLogic, DynamoBundle, DynamoBundlePolicy,
    DynamoBundleReferenceEncoding, DynamoBundleReferenceTarget, DynamoBundleStorage,
    DynamoImportResult, DynamoImportWarning, ImportMode,
};

const DELETE_CONCURRENCY: usize = 16;

// Definitions.
// ----------------------------------------------------------------------------

struct ImportWritePlan {
    puts: Vec<DynamoMap>,
    maintenance_deletes: Vec<PkSk>,
}

struct ExistingState {
    has_conflicts: bool,
    ext_partition_counts: HashMap<PkSk, usize>,
}

struct ReplacePlan {
    delete_ids: HashSet<PkSk>,
    stale_rows: Vec<DynamoMap>,
    stale_root_count: usize,
}

// Private interface.
// ----------------------------------------------------------------------------

pub(crate) async fn import_bundle<O: DynamoObject>(
    util: &DynamoUtil,
    algorithms: &dyn DynamoCrudAlgorithms,
    parent: Option<&PkSk>,
    mut bundle: DynamoBundle,
    mode: ImportMode,
) -> Result<DynamoImportResult, ServerError> {
    validate_bundle(&bundle)?;
    let root_item = bundle
        .items
        .iter()
        .find(|item| item.id == bundle.root)
        .ok_or_else(|| DynamoInvalidBundle::new("bundle root item was missing"))?;
    if root_item.id.label != O::id_label() || root_item.nesting != root_nesting::<O>() {
        return Err(DynamoInvalidBundle::new(
            "bundle root type or nesting did not match the importing CRUD wrapper",
        ));
    }
    let root_id_logic = BundleIdLogic::from_object::<O>();
    let policy = configured_bundle_policy(algorithms);
    let effective_omissions = validate_import_policy(&bundle, &policy, root_id_logic, parent)?;
    let replacing = matches!(&mode, ImportMode::Replace);
    let (id_map, existing, created_new, new_position) = match mode {
        ImportMode::New { position } => {
            let ids = build_id_map(&bundle, parent, true, root_id_logic)?;
            let destination_root = ids
                .get(&bundle.root)
                .ok_or_else(|| DynamoInvalidBundle::new("bundle root had no New destination ID"))?;
            if destination_root == &bundle.source_root {
                return Err(DynamoInvalidOperation::new(
                    "bundle root has a fixed identity at this placement and cannot be created as \
                     New; use Merge or Replace",
                ));
            }
            let existing = find_existing(util, &ids).await?;
            if existing.has_conflicts {
                return Err(DynamoInvalidOperation::new(
                    "one or more generated New destination IDs already exist",
                ));
            }
            (ids, existing, true, position)
        }
        ImportMode::Merge | ImportMode::Replace => {
            let preserved_ids = build_id_map(&bundle, parent, false, root_id_logic)?;
            let preserved_root_id = preserved_ids.get(&bundle.root).ok_or_else(|| {
                DynamoInvalidBundle::new("bundle root had no preserved destination ID")
            })?;
            if preserved_root_id != &bundle.source_root {
                return Err(DynamoInvalidOperation::new(
                    "bundle reparenting is not supported for Merge or Replace; use New to \
                     import below a different parent",
                ));
            }
            let existing = find_existing(util, &preserved_ids).await?;
            (preserved_ids, existing, false, None)
        }
    };
    if id_map.values().collect::<HashSet<_>>().len() != id_map.len() {
        return Err(DynamoInvalidBundle::new(
            "multiple bundle items mapped to the same destination ID",
        ));
    }
    let root_id = id_map
        .get(&bundle.root)
        .cloned()
        .ok_or_else(|| DynamoInvalidBundle::new("bundle root had no destination ID"))?;

    let old = if existing.has_conflicts && replacing {
        Some(
            export_with_omissions(
                util,
                &policy,
                &root_id,
                root_nesting::<O>(),
                root_id_logic,
                &effective_omissions,
            )
            .await?,
        )
    } else {
        None
    };
    let replace_plan = match &old {
        Some(old) => Some(build_replace_plan(util, &policy, parent, old, &id_map).await?),
        None => None,
    };

    let warnings = resolve_references(
        util,
        &mut bundle,
        &id_map,
        created_new,
        replace_plan.as_ref().map(|plan| &plan.delete_ids),
    )
    .await?;
    if created_new {
        prepare_new_root_data::<O>(util, parent, &mut bundle, new_position).await?;
    }
    let existing_partition_counts = if created_new {
        HashMap::new()
    } else {
        existing.ext_partition_counts
    };
    let write_plan = build_write_plan(&bundle, &id_map, &existing_partition_counts)?;
    util.raw_batch_delete_ids(write_plan.maintenance_deletes)
        .await?;
    util.raw_batch_put_item(write_plan.puts).await?;

    let deleted_subtree_roots = match replace_plan {
        Some(plan) => {
            if !plan.stale_rows.is_empty() {
                algorithms
                    .bundle_import_outoftable_cleanup(&plan.stale_rows)
                    .await?;
            }
            util.raw_batch_delete_ids(plan.delete_ids.into_iter().collect())
                .await?;
            plan.stale_root_count
        }
        None => 0,
    };

    Ok(DynamoImportResult {
        root_id,
        written_objects: bundle.items.len(),
        deleted_subtree_roots,
        created_new,
        warnings,
    })
}

// Internal.
// ----------------------------------------------------------------------------

async fn find_existing(
    util: &DynamoUtil,
    ids: &HashMap<BundleId, PkSk>,
) -> Result<ExistingState, ServerError> {
    let rows = util
        .raw_batch_get_ids(ids.values().cloned().collect(), None)
        .await?;
    let has_conflicts = !rows.is_empty();
    let mut ext_partition_counts = HashMap::new();
    for row in rows {
        let id = PkSk::from_map(&row)?;
        if let Some(count) = row
            .get(COLLAPSE_PLACEHOLDER_RESERVED_KEY)
            .and_then(|value| value.as_n().ok())
            .and_then(|value| value.parse().ok())
        {
            ext_partition_counts.insert(id, count);
        }
    }
    Ok(ExistingState {
        has_conflicts,
        ext_partition_counts,
    })
}

async fn resolve_references(
    util: &DynamoUtil,
    bundle: &mut DynamoBundle,
    id_map: &HashMap<BundleId, PkSk>,
    created_new: bool,
    pending_deletes: Option<&HashSet<PkSk>>,
) -> Result<Vec<DynamoImportWarning>, ServerError> {
    let in_table_ids = bundle
        .references
        .iter()
        .filter_map(|reference| match &reference.target {
            DynamoBundleReferenceTarget::InTable { lookup_id, .. } => Some(lookup_id.clone()),
            DynamoBundleReferenceTarget::Bundled { .. }
            | DynamoBundleReferenceTarget::OutOfTable { .. } => None,
        })
        .collect::<HashSet<_>>();
    let mut existing_in_table = if in_table_ids.is_empty() {
        HashSet::new()
    } else {
        util.raw_batch_get_ids(in_table_ids.into_iter().collect(), Some("pk, sk".into()))
            .await?
            .iter()
            .map(PkSk::from_map)
            .collect::<Result<HashSet<_>, _>>()?
    };
    if let Some(pending_deletes) = pending_deletes {
        existing_in_table.retain(|id| !pending_deletes.contains(id));
    }
    existing_in_table.extend(id_map.values().cloned());

    let source_indexes = bundle
        .items
        .iter()
        .enumerate()
        .map(|(index, item)| (item.id.value, index))
        .collect::<HashMap<_, _>>();
    let mut warnings = Vec::new();
    let mut missing_clears = Vec::new();
    for reference in &bundle.references {
        let source_index = source_indexes
            .get(&reference.source.value)
            .copied()
            .ok_or_else(|| DynamoInvalidBundle::new("reference source was absent"))?;
        let replacement = match &reference.target {
            DynamoBundleReferenceTarget::Bundled {
                id: target,
                encoding,
            } => {
                let target = id_map.get(target).ok_or_else(|| {
                    DynamoInvalidBundle::new("reference target had no destination ID")
                })?;
                match encoding {
                    DynamoBundleReferenceEncoding::PkSk => Value::String(target.to_string()),
                    DynamoBundleReferenceEncoding::ForeignRef => {
                        serde_json::to_value(target.build_ref()).map_err(|error| {
                            DynamoInvalidOperation::with_debug(
                                "failed to serialize remapped bundle foreign reference",
                                &error,
                            )
                        })?
                    }
                }
            }
            DynamoBundleReferenceTarget::InTable { lookup_id, .. }
                if existing_in_table.contains(lookup_id) =>
            {
                continue;
            }
            DynamoBundleReferenceTarget::InTable { clear_path, .. } => {
                warnings.push(DynamoImportWarning::ZeroedInTableReference);
                missing_clears.push((source_index, clear_path));
                continue;
            }
            DynamoBundleReferenceTarget::OutOfTable { .. } if !created_new => continue,
            DynamoBundleReferenceTarget::OutOfTable { clear_path, .. } => {
                warnings.push(DynamoImportWarning::ZeroedOutOfTableReference);
                missing_clears.push((source_index, clear_path));
                continue;
            }
        };
        set_value_at_path(
            &mut bundle.items[source_index].data,
            &reference.path,
            replacement,
        )?;
    }
    // Clear missing compound references after all remapping so clearing a
    // containing value cannot invalidate another nested reference path.
    for (source_index, clear_path) in missing_clears {
        set_value_at_path(
            &mut bundle.items[source_index].data,
            clear_path,
            Value::Null,
        )?;
    }
    Ok(warnings)
}

fn build_write_plan(
    bundle: &DynamoBundle,
    ids: &HashMap<BundleId, PkSk>,
    existing_partition_counts: &HashMap<PkSk, usize>,
) -> Result<ImportWritePlan, ServerError> {
    let mut puts = Vec::new();
    let mut maintenance_deletes = Vec::new();
    let imported_at = Timestamp::now();
    for item in &bundle.items {
        let id = ids
            .get(&item.id)
            .ok_or_else(|| DynamoInvalidBundle::new("bundle item had no destination ID"))?;
        match item.storage {
            DynamoBundleStorage::Standard => {
                puts.push(
                    build_dynamo_map_internal(
                        &item.data,
                        Some(id.pk.clone()),
                        Some(id.sk.clone()),
                        Some(vec![
                            (AUTO_FIELDS_CREATED_AT, Box::new(imported_at.clone())),
                            (AUTO_FIELDS_UPDATED_AT, Box::new(imported_at.clone())),
                        ]),
                    )?
                    .0,
                );
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
    Ok(ImportWritePlan {
        puts,
        maintenance_deletes,
    })
}

fn partition_payload(data: &Value) -> Result<(String, Option<f64>, Option<i64>), ServerError> {
    let Value::Object(mut object) = data.clone() else {
        return Err(DynamoInvalidBundle::new(
            "partitioned item data was not an object",
        ));
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

async fn prepare_new_root_data<O: DynamoObject>(
    util: &DynamoUtil,
    parent: Option<&PkSk>,
    bundle: &mut DynamoBundle,
    position: Option<DynamoInsertPosition>,
) -> Result<(), ServerError> {
    let root = bundle
        .items
        .iter_mut()
        .find(|item| item.id == bundle.root)
        .ok_or_else(|| DynamoInvalidBundle::new("bundle root item was missing"))?;
    match &mut root.data {
        Value::Object(root_data) => {
            root_data.remove(AUTO_FIELDS_SORT);
        }
        _ => {
            return Err(DynamoInvalidBundle::new(
                "bundle root data was not an object",
            ))
        }
    }
    let Some(position) = position else {
        return Ok(());
    };
    let parent = parent.ok_or_else(|| {
        DynamoInvalidBundle::new(
            "ordered bundle root required a destination parent for sort placement",
        )
    })?;
    let data = serde_json::from_value::<O::Data>(root.data.clone()).map_err(|error| {
        DynamoInvalidOperation::with_debug(
            "ordered bundle root data could not be read for destination sort placement",
            &error,
        )
    })?;
    let sort = calculate_sort_values::<O>(util, parent, &data, position, 1)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| {
            CriticalError::new("ordered bundle root sort calculation returned no value")
        })?;
    let Value::Object(root_data) = &mut root.data else {
        return Err(DynamoInvalidBundle::new(
            "bundle root data was not an object",
        ));
    };
    root_data.insert(
        AUTO_FIELDS_SORT.to_owned(),
        Value::Number(
            serde_json::Number::from_f64(sort)
                .ok_or_else(|| CriticalError::new("ordered bundle root sort was not finite"))?,
        ),
    );
    Ok(())
}

async fn build_replace_plan(
    util: &DynamoUtil,
    bundles: &DynamoBundlePolicy,
    parent: Option<&PkSk>,
    old: &DynamoBundle,
    new_ids: &HashMap<BundleId, PkSk>,
) -> Result<ReplacePlan, ServerError> {
    let old_root_id_logic = old
        .items
        .iter()
        .find(|item| item.id == old.root)
        .ok_or_else(|| DynamoInvalidBundle::new("root item was missing"))?
        .id_logic;
    let old_ids = build_id_map(old, parent, false, old_root_id_logic)?;
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
    let stale_roots =
        old.items
            .iter()
            .filter(|item| {
                stale.contains(&item.id)
                    && item
                        .parent
                        .as_ref()
                        .is_none_or(|parent| !stale.contains(parent))
            })
            .map(|item| {
                Ok((
                    old_ids.get(&item.id).cloned().ok_or_else(|| {
                        DynamoInvalidBundle::new("stale item had no destination ID")
                    })?,
                    item.nesting,
                ))
            })
            .collect::<Result<Vec<_>, ServerError>>()?;

    let delete_groups = stream::iter(stale_roots.iter().cloned())
        .map(|(id, nesting)| async move {
            let no_omissions = BTreeMap::new();
            let (items, _) =
                collect_bundle_items(util, bundles, &id, nesting, Some(&no_omissions)).await?;
            items
                .into_iter()
                .flat_map(|item| item.rows)
                .map(|row| Ok((PkSk::from_map(&row)?, row)))
                .collect::<Result<Vec<_>, ServerError>>()
        })
        .buffer_unordered(DELETE_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
    let mut delete_rows = HashMap::new();
    for (id, row) in delete_groups.into_iter().flatten() {
        if !desired.contains(&id) {
            delete_rows.insert(id, row);
        }
    }
    let mut delete_rows = delete_rows.into_iter().collect::<Vec<_>>();
    delete_rows.sort_by(|(a, _), (b, _)| a.pk.cmp(&b.pk).then_with(|| a.sk.cmp(&b.sk)));
    let delete_ids = delete_rows
        .iter()
        .map(|(id, _)| id.clone())
        .collect::<HashSet<_>>();
    let stale_rows = delete_rows.into_iter().map(|(_, row)| row).collect();
    Ok(ReplacePlan {
        delete_ids,
        stale_rows,
        stale_root_count: stale_roots.len(),
    })
}
