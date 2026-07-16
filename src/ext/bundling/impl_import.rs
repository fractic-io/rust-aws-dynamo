use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use chrono::Utc;
use fractic_server_error::{CriticalError, ServerError};
use futures_util::{stream, StreamExt as _, TryStreamExt as _};
use serde_json::Value;

use crate::{
    errors::DynamoInvalidOperation,
    ext::crud::DynamoCrudAlgorithms,
    schema::{
        id_calculations::{
            freshen_object_sk, freshen_timestamp_object_sk, get_object_type, object_sk_component,
            place_inline_id, place_root_id, place_top_level_id, relative_child_sk,
            strip_ext_suffix,
        },
        parsing::build_dynamo_map_internal,
        DynamoObject, PkSk, Timestamp,
    },
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
    impl_utils::set_value_at_path,
    invalid_bundle, root_nesting, BundleId, BundleIdLogic, BundleNesting, DynamoBundle,
    DynamoBundleItem, DynamoBundlePolicy, DynamoBundleReferenceEncoding,
    DynamoBundleReferenceTarget, DynamoBundleStorage, DynamoImportResult, DynamoImportWarning,
    ImportMode,
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
        .ok_or_else(|| invalid_bundle("bundle root item was missing"))?;
    if root_item.id.label != O::id_label() || root_item.nesting != root_nesting::<O>() {
        return Err(invalid_bundle(
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
                .ok_or_else(|| invalid_bundle("bundle root had no New destination ID"))?;
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
            let preserved_root_id = preserved_ids
                .get(&bundle.root)
                .ok_or_else(|| invalid_bundle("bundle root had no preserved destination ID"))?;
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
        return Err(invalid_bundle(
            "multiple bundle items mapped to the same destination ID",
        ));
    }
    let root_id = id_map
        .get(&bundle.root)
        .cloned()
        .ok_or_else(|| invalid_bundle("bundle root had no destination ID"))?;

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

pub(crate) fn build_id_map(
    bundle: &DynamoBundle,
    parent: Option<&PkSk>,
    duplicate: bool,
    root_id_logic: BundleIdLogic,
) -> Result<HashMap<BundleId, PkSk>, ServerError> {
    let root = bundle
        .items
        .iter()
        .find(|item| item.id == bundle.root)
        .ok_or_else(|| invalid_bundle("bundle root item was missing"))?;
    let mut children = HashMap::<&BundleId, Vec<&DynamoBundleItem>>::new();
    for item in &bundle.items {
        if let Some(parent) = &item.parent {
            children.entry(parent).or_default().push(item);
        }
    }

    let mut result = HashMap::with_capacity(bundle.items.len());
    let mut duplicate_ids = DuplicateIdGenerator::new();
    result.insert(
        root.id.clone(),
        place_root(root, parent, duplicate, root_id_logic, &mut duplicate_ids)?,
    );
    let mut frontier = VecDeque::from([root]);
    while let Some(mapped_parent) = frontier.pop_front() {
        if let Some(child_items) = children.remove(&mapped_parent.id) {
            let parent_id = result
                .get(&mapped_parent.id)
                .cloned()
                .ok_or_else(|| invalid_bundle("mapped bundle parent was missing"))?;
            for item in child_items {
                let mapped = place_child(
                    item,
                    &parent_id,
                    duplicate,
                    item.id_logic,
                    &mut duplicate_ids,
                )?;
                if result.insert(item.id.clone(), mapped).is_some() {
                    return Err(invalid_bundle("bundle IDs were invalid"));
                }
                frontier.push_back(item);
            }
        }
    }
    if result.len() != bundle.items.len() {
        return Err(invalid_bundle("bundle item parent graph was invalid"));
    }
    Ok(result)
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
            .ok_or_else(|| invalid_bundle("reference source was absent"))?;
        let replacement = match &reference.target {
            DynamoBundleReferenceTarget::Bundled {
                id: target,
                encoding,
            } => {
                let target = id_map
                    .get(target)
                    .ok_or_else(|| invalid_bundle("reference target had no destination ID"))?;
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
            .ok_or_else(|| invalid_bundle("bundle item had no destination ID"))?;
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
        .ok_or_else(|| invalid_bundle("bundle root item was missing"))?;
    match &mut root.data {
        Value::Object(root_data) => {
            root_data.remove(AUTO_FIELDS_SORT);
        }
        _ => return Err(invalid_bundle("bundle root data was not an object")),
    }
    let Some(position) = position else {
        return Ok(());
    };
    let parent = parent.ok_or_else(|| {
        invalid_bundle("ordered bundle root required a destination parent for sort placement")
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
        return Err(invalid_bundle("bundle root data was not an object"));
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
        .ok_or_else(|| invalid_bundle("root item was missing"))?
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
    let stale_roots = old
        .items
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
            let no_omissions = BTreeMap::new();
            let (items, _) =
                collect_bundle_items(util, bundles, &id, nesting, Some(&no_omissions)).await?;
            items
                .into_iter()
                .flat_map(|item| item.rows)
                .map(|row| PkSk::from_map(&row))
                .collect::<Result<Vec<_>, ServerError>>()
        })
        .buffer_unordered(DELETE_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
    let delete_ids = delete_groups
        .into_iter()
        .flatten()
        .filter(|id| !desired.contains(id))
        .collect::<HashSet<_>>();
    Ok(ReplacePlan {
        delete_ids,
        stale_root_count: stale_roots.len(),
    })
}

// Helpers.
// ----------------------------------------------------------------------------

fn place_root(
    item: &DynamoBundleItem,
    parent: Option<&PkSk>,
    duplicate: bool,
    id_logic: BundleIdLogic,
    duplicate_ids: &mut DuplicateIdGenerator,
) -> Result<PkSk, ServerError> {
    match item.nesting {
        BundleNesting::Root => {
            if parent.is_some() {
                return Err(invalid_bundle(
                    "root object cannot be imported below a parent",
                ));
            }
            let object_sk =
                destination_object_sk(&item.id.original_sk, duplicate, id_logic, duplicate_ids)?;
            Ok(place_root_id(&object_sk))
        }
        BundleNesting::TopLevel => {
            let parent = parent.ok_or_else(|| invalid_bundle("child bundle requires a parent"))?;
            let object_sk =
                destination_object_sk(&item.id.original_sk, duplicate, id_logic, duplicate_ids)?;
            Ok(place_top_level_id(parent, &object_sk))
        }
        BundleNesting::Inline => {
            let parent = parent.ok_or_else(|| invalid_bundle("child bundle requires a parent"))?;
            let component = object_sk_component(&item.id.original_sk, &item.id.label)?;
            let component = destination_object_sk(component, duplicate, id_logic, duplicate_ids)?;
            Ok(place_inline_id(parent, &component))
        }
    }
}

fn place_child(
    item: &DynamoBundleItem,
    parent: &PkSk,
    duplicate: bool,
    id_logic: BundleIdLogic,
    duplicate_ids: &mut DuplicateIdGenerator,
) -> Result<PkSk, ServerError> {
    match item.nesting {
        BundleNesting::TopLevel => {
            let object_sk =
                destination_object_sk(&item.id.original_sk, duplicate, id_logic, duplicate_ids)?;
            Ok(place_top_level_id(parent, &object_sk))
        }
        BundleNesting::Inline => {
            let original_parent = item
                .parent
                .as_ref()
                .ok_or_else(|| invalid_bundle("inline child had no parent"))?;
            let relative = relative_child_sk(&item.id.original_sk, &original_parent.original_sk)?;
            let relative = destination_object_sk(relative, duplicate, id_logic, duplicate_ids)?;
            Ok(place_inline_id(parent, &relative))
        }
        BundleNesting::Root => Err(invalid_bundle("non-root item had root nesting")),
    }
}

struct DuplicateIdGenerator {
    timestamp_seed: i64,
    timestamp_count: usize,
}

impl DuplicateIdGenerator {
    fn new() -> Self {
        Self {
            timestamp_seed: Utc::now().timestamp_millis(),
            timestamp_count: 0,
        }
    }

    fn next_timestamp(&mut self) -> Result<i64, ServerError> {
        let offset = i64::try_from(self.timestamp_count)
            .map_err(|_| CriticalError::new("bundle timestamp item index overflowed i64"))?;
        let timestamp = self
            .timestamp_seed
            .checked_add(offset)
            .ok_or_else(|| CriticalError::new("bundle timestamp ID overflowed i64"))?;
        self.timestamp_count = self
            .timestamp_count
            .checked_add(1)
            .ok_or_else(|| CriticalError::new("bundle timestamp item count overflowed usize"))?;
        Ok(timestamp)
    }
}

fn destination_object_sk(
    original_sk: &str,
    duplicate: bool,
    id_logic: BundleIdLogic,
    duplicate_ids: &mut DuplicateIdGenerator,
) -> Result<String, ServerError> {
    if !duplicate {
        return Ok(strip_ext_suffix(original_sk).to_string());
    }
    match id_logic {
        BundleIdLogic::Uuid => Ok(freshen_object_sk(original_sk)),
        BundleIdLogic::Timestamp => Ok(freshen_timestamp_object_sk(
            original_sk,
            duplicate_ids.next_timestamp()?,
        )),
        BundleIdLogic::Singleton
        | BundleIdLogic::IndexedSingleton
        | BundleIdLogic::BatchOptimized
        | BundleIdLogic::SingletonExt
        | BundleIdLogic::IndexedSingletonExt => Ok(strip_ext_suffix(original_sk).to_string()),
        BundleIdLogic::Phantom => Err(invalid_bundle(
            "persisted bundle item used phantom ID logic",
        )),
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
