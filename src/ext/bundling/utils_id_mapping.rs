use std::collections::{HashMap, VecDeque};

use chrono::Utc;
use fractic_server_error::{CriticalError, ServerError};

use crate::errors::DynamoInvalidBundle;
use crate::schema::{
    identifiers::{
        place_terminal_segment_with, regenerate_timestamp, regenerate_uuid, IdPlacement, RawIdPath,
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
    let mut duplicate_ids = DuplicateIdGenerator::new();
    let root_id = place_root(root, parent, duplicate, root_id_logic, &mut duplicate_ids)?;
    map_bundle_tree(bundle, root, root_id, |item, parent_id| {
        place_child(
            item,
            parent_id,
            duplicate,
            item.id_logic,
            &mut duplicate_ids,
        )
    })
}

/// Reconstructs the IDs represented by the bundle before any New remapping.
pub(crate) fn build_source_id_map(
    bundle: &DynamoBundle,
) -> Result<HashMap<BundleId, PkSk>, ServerError> {
    let root = bundle
        .items
        .iter()
        .find(|item| item.id == bundle.root)
        .ok_or_else(|| DynamoInvalidBundle::new("bundle root item was missing"))?;
    map_bundle_tree(bundle, root, bundle.source_root.clone(), place_source_child)
}

// Internal.
// ----------------------------------------------------------------------------

fn map_bundle_tree(
    bundle: &DynamoBundle,
    root: &DynamoBundleItem,
    root_id: PkSk,
    mut map_child: impl FnMut(&DynamoBundleItem, &PkSk) -> Result<PkSk, ServerError>,
) -> Result<HashMap<BundleId, PkSk>, ServerError> {
    let mut children = HashMap::<&BundleId, Vec<&DynamoBundleItem>>::new();
    for item in &bundle.items {
        if let Some(parent) = &item.parent {
            children.entry(parent).or_default().push(item);
        }
    }
    let mut result = HashMap::from([(root.id.clone(), root_id)]);
    let mut frontier = VecDeque::from([root]);
    while let Some(mapped_parent) = frontier.pop_front() {
        if let Some(child_items) = children.remove(&mapped_parent.id) {
            let parent_id = result
                .get(&mapped_parent.id)
                .cloned()
                .ok_or_else(|| DynamoInvalidBundle::new("mapped bundle parent was missing"))?;
            for item in child_items {
                if result
                    .insert(item.id.clone(), map_child(item, &parent_id)?)
                    .is_some()
                {
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

fn place_source_child(item: &DynamoBundleItem, parent: &PkSk) -> Result<PkSk, ServerError> {
    let sk = RawIdPath::new(&item.id.original_sk)
        .logical_path()
        .to_owned();
    match item.nesting {
        BundleNesting::TopLevel => Ok(PkSk {
            pk: parent.sk.clone(),
            sk,
        }),
        BundleNesting::Inline => Ok(PkSk {
            pk: parent.pk.clone(),
            sk,
        }),
        BundleNesting::Root => Err(DynamoInvalidBundle::new("non-root item had root nesting")),
    }
}

fn place_root(
    item: &DynamoBundleItem,
    parent: Option<&PkSk>,
    duplicate: bool,
    id_logic: BundleIdLogic,
    duplicate_ids: &mut DuplicateIdGenerator,
) -> Result<PkSk, ServerError> {
    let (parent, original_sk, placement) = match item.nesting {
        BundleNesting::Root => {
            if parent.is_some() {
                return Err(DynamoInvalidBundle::new(
                    "root object cannot be imported below a parent",
                ));
            }
            (
                PkSk::root(),
                item.id.original_sk.as_str(),
                IdPlacement::Root,
            )
        }
        BundleNesting::TopLevel => (
            parent.ok_or_else(|| DynamoInvalidBundle::new("child bundle requires a parent"))?,
            item.id.original_sk.as_str(),
            IdPlacement::TopLevel,
        ),
        BundleNesting::Inline => (
            parent.ok_or_else(|| DynamoInvalidBundle::new("child bundle requires a parent"))?,
            RawIdPath::new(&item.id.original_sk).terminal_segment(&item.id.label)?,
            IdPlacement::Inline,
        ),
    };
    Ok(place_terminal_segment_with(
        parent,
        &destination_object_sk(original_sk, duplicate, id_logic, duplicate_ids)?,
        placement,
    ))
}

fn place_child(
    item: &DynamoBundleItem,
    parent: &PkSk,
    duplicate: bool,
    id_logic: BundleIdLogic,
    duplicate_ids: &mut DuplicateIdGenerator,
) -> Result<PkSk, ServerError> {
    let (original_sk, placement) = match item.nesting {
        BundleNesting::TopLevel => (item.id.original_sk.as_str(), IdPlacement::TopLevel),
        BundleNesting::Inline => {
            let original_parent = item
                .parent
                .as_ref()
                .ok_or_else(|| DynamoInvalidBundle::new("inline child had no parent"))?;
            (
                RawIdPath::new(&item.id.original_sk)
                    .relative_to(RawIdPath::new(&original_parent.original_sk))?,
                IdPlacement::Inline,
            )
        }
        BundleNesting::Root => {
            return Err(DynamoInvalidBundle::new("non-root item had root nesting"))
        }
    };
    Ok(place_terminal_segment_with(
        parent,
        &destination_object_sk(original_sk, duplicate, id_logic, duplicate_ids)?,
        placement,
    ))
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
        return Ok(RawIdPath::new(original_sk).logical_path().to_string());
    }
    match id_logic {
        BundleIdLogic::Uuid => regenerate_uuid(original_sk),
        BundleIdLogic::Timestamp => {
            regenerate_timestamp(original_sk, duplicate_ids.next_timestamp()?)
        }
        BundleIdLogic::Singleton
        | BundleIdLogic::IndexedSingleton
        | BundleIdLogic::BatchOptimized
        | BundleIdLogic::SingletonExt
        | BundleIdLogic::IndexedSingletonExt => {
            Ok(RawIdPath::new(original_sk).logical_path().to_string())
        }
        BundleIdLogic::Phantom => Err(DynamoInvalidBundle::new(
            "persisted bundle item used phantom ID logic",
        )),
    }
}
