use std::collections::{HashMap, VecDeque};

use chrono::Utc;
use fractic_server_error::{CriticalError, ServerError};

use crate::errors::DynamoInvalidBundle;
use crate::schema::{
    id_calculations::{
        freshen_object_sk, freshen_timestamp_object_sk, object_sk_component, place_inline_id,
        place_root_id, place_top_level_id, relative_child_sk, strip_ext_suffix,
    },
    PkSk,
};

use super::{BundleId, BundleIdLogic, BundleNesting, DynamoBundle, DynamoBundleItem};

// Private interface.
// ----------------------------------------------------------------------------

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
        .ok_or_else(|| DynamoInvalidBundle::new("bundle root item was missing"))?;
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
                .ok_or_else(|| DynamoInvalidBundle::new("mapped bundle parent was missing"))?;
            for item in child_items {
                let mapped = place_child(
                    item,
                    &parent_id,
                    duplicate,
                    item.id_logic,
                    &mut duplicate_ids,
                )?;
                if result.insert(item.id.clone(), mapped).is_some() {
                    return Err(DynamoInvalidBundle::new("bundle IDs were invalid"));
                }
                frontier.push_back(item);
            }
        }
    }
    if result.len() != bundle.items.len() {
        return Err(DynamoInvalidBundle::new(
            "bundle item parent graph was invalid",
        ));
    }
    Ok(result)
}

// Internal.
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
                return Err(DynamoInvalidBundle::new(
                    "root object cannot be imported below a parent",
                ));
            }
            let object_sk =
                destination_object_sk(&item.id.original_sk, duplicate, id_logic, duplicate_ids)?;
            Ok(place_root_id(&object_sk))
        }
        BundleNesting::TopLevel => {
            let parent =
                parent.ok_or_else(|| DynamoInvalidBundle::new("child bundle requires a parent"))?;
            let object_sk =
                destination_object_sk(&item.id.original_sk, duplicate, id_logic, duplicate_ids)?;
            Ok(place_top_level_id(parent, &object_sk))
        }
        BundleNesting::Inline => {
            let parent =
                parent.ok_or_else(|| DynamoInvalidBundle::new("child bundle requires a parent"))?;
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
                .ok_or_else(|| DynamoInvalidBundle::new("inline child had no parent"))?;
            let relative = relative_child_sk(&item.id.original_sk, &original_parent.original_sk)?;
            let relative = destination_object_sk(relative, duplicate, id_logic, duplicate_ids)?;
            Ok(place_inline_id(parent, &relative))
        }
        BundleNesting::Root => Err(DynamoInvalidBundle::new("non-root item had root nesting")),
    }
}

// Helpers.
// ----------------------------------------------------------------------------

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
        BundleIdLogic::Phantom => Err(DynamoInvalidBundle::new(
            "persisted bundle item used phantom ID logic",
        )),
    }
}
