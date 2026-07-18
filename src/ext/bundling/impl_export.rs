use std::collections::{BTreeMap, BTreeSet, HashMap};

use aws_sdk_dynamodb::types::AttributeValue;
use fractic_core::collection;
use fractic_server_error::ServerError;
use futures_util::{stream, StreamExt as _, TryStreamExt as _};
use serde_json::Value;

use crate::{
    errors::DynamoInvalidBundle,
    errors::{DynamoCalloutError, DynamoNotFound},
    ext::crud::DynamoCrudAlgorithms,
    schema::{
        identifiers::RawIdPath, parsing::dynamo_map_to_serde_value, pk_sk::id_fields_from_map, PkSk,
    },
    util::{
        collapse_helpers::{collapse_partitioned_items, ext_base_id},
        DynamoMap, DynamoUtil, AUTO_FIELDS_CREATED_AT, AUTO_FIELDS_UPDATED_AT,
        COLLAPSE_DATA_RESERVED_KEY, COLLAPSE_PLACEHOLDER_RESERVED_KEY,
    },
};

use super::{
    entities_policy::{configured_bundle_policy, DynamoBundleObjectPolicy},
    reference_manifest::derive_reference_manifest,
    BundleId, BundleIdLogic, BundleNesting, DynamoBundle, DynamoBundleItem, DynamoBundlePolicy,
    DynamoBundleStorage,
};

const QUERY_CONCURRENCY: usize = 16;

// Definitions.
// ----------------------------------------------------------------------------

pub(crate) struct CollectedItem {
    pub id: PkSk,
    pub parent: Option<PkSk>,
    pub nesting: BundleNesting,
    pub rows: Vec<DynamoMap>,
    omitted_descendants: BTreeSet<String>,
}

struct ExportOptions<'a> {
    root_nesting: BundleNesting,
    root_id_logic: BundleIdLogic,
    fixed_omissions: Option<&'a BTreeMap<String, BTreeSet<String>>>,
    validate_reference_closure: bool,
}

// Private interface.
// ----------------------------------------------------------------------------

pub(crate) async fn export_from_config(
    util: &DynamoUtil,
    algorithms: &dyn DynamoCrudAlgorithms,
    root: PkSk,
    root_nesting: BundleNesting,
    root_id_logic: BundleIdLogic,
) -> Result<DynamoBundle, ServerError> {
    let policy = configured_bundle_policy(algorithms);
    export_bundle(
        util,
        &policy,
        &root,
        ExportOptions {
            root_nesting,
            root_id_logic,
            fixed_omissions: None,
            validate_reference_closure: true,
        },
    )
    .await
}

/// Exports a destination snapshot using the incoming bundle's omission policy.
pub(crate) async fn export_with_omissions(
    util: &DynamoUtil,
    policy: &DynamoBundlePolicy,
    root: &PkSk,
    root_nesting: BundleNesting,
    root_id_logic: BundleIdLogic,
    omissions: &BTreeMap<String, BTreeSet<String>>,
) -> Result<DynamoBundle, ServerError> {
    export_bundle(
        util,
        policy,
        root,
        ExportOptions {
            root_nesting,
            root_id_logic,
            fixed_omissions: Some(omissions),
            validate_reference_closure: false,
        },
    )
    .await
}

async fn export_bundle(
    util: &DynamoUtil,
    policy: &DynamoBundlePolicy,
    root: &PkSk,
    options: ExportOptions<'_>,
) -> Result<DynamoBundle, ServerError> {
    let (collected, omitted_descendants) = collect_bundle_items(
        util,
        policy,
        root,
        options.root_nesting,
        options.fixed_omissions,
    )
    .await?;

    let root_id = ext_base_id(root);
    let mut by_pk_sk = HashMap::new();
    let mut items = Vec::with_capacity(collected.len());
    for (index, item) in collected.into_iter().enumerate() {
        let label = RawIdPath::new(&item.id.sk).object_label()?.to_string();
        let (storage, mut data) = normalize_rows(item.rows)?;
        let object = policy.require(&label)?;
        object.normalize_data(&mut data);
        let id_logic = if item.id == root_id {
            options.root_id_logic
        } else {
            object.id_logic()
        };
        let id = BundleId {
            value: index as u64,
            label,
            original_sk: item.id.sk.clone(),
        };
        let parent = item.parent.as_ref().map_or(Ok(None), |parent| {
            by_pk_sk
                .get(parent)
                .cloned()
                .map(Some)
                .ok_or_else(|| DynamoInvalidBundle::new("bundle item preceded its parent"))
        })?;
        by_pk_sk.insert(item.id, id.clone());
        items.push(DynamoBundleItem {
            id,
            id_logic,
            parent,
            nesting: item.nesting,
            storage,
            data,
        });
    }

    let root = by_pk_sk
        .get(&root_id)
        .cloned()
        .ok_or_else(|| DynamoInvalidBundle::new("source root was not returned by DynamoDB"))?;
    let bundle = DynamoBundle {
        version: DynamoBundle::VERSION,
        source_root: root_id,
        root,
        omitted_descendants,
        items,
    };
    if options.validate_reference_closure {
        derive_reference_manifest(policy, &bundle, &by_pk_sk)?;
    }
    Ok(bundle)
}

// Item collection.
// ----------------------------------------------------------------------------

pub(crate) async fn collect_bundle_items(
    util: &DynamoUtil,
    policy: &DynamoBundlePolicy,
    root: &PkSk,
    root_nesting: BundleNesting,
    fixed_omissions: Option<&BTreeMap<String, BTreeSet<String>>>,
) -> Result<(Vec<CollectedItem>, BTreeMap<String, BTreeSet<String>>), ServerError> {
    let root = ext_base_id(root);
    let root_label = RawIdPath::new(&root.sk).object_label()?;
    let root_config = policy.require(root_label)?;
    let mut recorded = fixed_omissions.cloned().unwrap_or_default();
    let root_omissions = omissions_for(root_config, fixed_omissions, &mut recorded, root_label);
    let initial_rows = raw_query(util, &root.pk, Some(&root.sk)).await?;
    let mut initial_groups = group_logical_rows(initial_rows)?;
    let root_rows = initial_groups
        .remove(&root)
        .ok_or_else(DynamoNotFound::new)?;
    let mut collected = vec![CollectedItem {
        id: root.clone(),
        parent: None,
        nesting: root_nesting,
        rows: root_rows,
        omitted_descendants: root_omissions,
    }];

    initial_groups.retain(|id, _| is_inline_descendant(&id.sk, &root.sk));
    append_partition_groups(
        &mut collected,
        0,
        initial_groups,
        policy,
        fixed_omissions,
        &mut recorded,
    )?;

    let mut frontier = (0..collected.len()).collect::<Vec<_>>();
    while !frontier.is_empty() {
        let collected_ref = &collected;
        let mut results = stream::iter(std::mem::take(&mut frontier))
            .map(move |owner_index| async move {
                let rows = raw_query(util, &collected_ref[owner_index].id.sk, None).await?;
                Ok::<_, ServerError>((owner_index, group_logical_rows(rows)?))
            })
            .buffer_unordered(QUERY_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        results.sort_by(|(a, _), (b, _)| {
            let a = &collected[*a].id;
            let b = &collected[*b].id;
            a.pk.cmp(&b.pk).then_with(|| a.sk.cmp(&b.sk))
        });

        for (owner_index, groups) in results {
            let before = collected.len();
            append_partition_groups(
                &mut collected,
                owner_index,
                groups,
                policy,
                fixed_omissions,
                &mut recorded,
            )?;
            frontier.extend(before..collected.len());
        }
    }
    Ok((collected, recorded))
}

// Helpers.
// ----------------------------------------------------------------------------

fn omissions_for(
    object: &DynamoBundleObjectPolicy,
    fixed: Option<&BTreeMap<String, BTreeSet<String>>>,
    recorded: &mut BTreeMap<String, BTreeSet<String>>,
    label: &str,
) -> BTreeSet<String> {
    if let Some(fixed) = fixed {
        return fixed.get(label).cloned().unwrap_or_default();
    }
    let omissions = object.omitted_descendants();
    if !omissions.is_empty() {
        recorded.insert(label.to_owned(), omissions.clone());
    }
    omissions.clone()
}

fn append_partition_groups(
    collected: &mut Vec<CollectedItem>,
    owner_index: usize,
    groups: HashMap<PkSk, Vec<DynamoMap>>,
    policy: &DynamoBundlePolicy,
    fixed_omissions: Option<&BTreeMap<String, BTreeSet<String>>>,
    recorded: &mut BTreeMap<String, BTreeSet<String>>,
) -> Result<(), ServerError> {
    let mut groups = groups.into_iter().collect::<Vec<_>>();
    groups.sort_by(|(a, _), (b, _)| a.sk.len().cmp(&b.sk.len()).then_with(|| a.sk.cmp(&b.sk)));
    let mut accepted = vec![owner_index];
    let mut omitted = Vec::<PkSk>::new();
    for (id, rows) in groups {
        let label = RawIdPath::new(&id.sk).object_label()?.to_string();
        let inline_parent = accepted
            .iter()
            .copied()
            .filter(|index| is_inline_descendant(&id.sk, &collected[*index].id.sk))
            .max_by_key(|index| collected[*index].id.sk.len());
        let inherited_omissions =
            &collected[inline_parent.unwrap_or(owner_index)].omitted_descendants;
        if inherited_omissions.contains(&label)
            || omitted
                .iter()
                .any(|ancestor| is_inline_descendant(&id.sk, &ancestor.sk))
        {
            omitted.push(id);
            continue;
        }
        let (parent, nesting) = match inline_parent {
            Some(index) => (collected[index].id.clone(), BundleNesting::Inline),
            None => (collected[owner_index].id.clone(), BundleNesting::TopLevel),
        };
        let object = policy.require(&label)?;
        let mut descendant_omissions = inherited_omissions.clone();
        descendant_omissions.extend(omissions_for(object, fixed_omissions, recorded, &label));
        collected.push(CollectedItem {
            id,
            parent: Some(parent),
            nesting,
            rows,
            omitted_descendants: descendant_omissions,
        });
        accepted.push(collected.len() - 1);
    }
    Ok(())
}

fn normalize_rows(rows: Vec<DynamoMap>) -> Result<(DynamoBundleStorage, Value), ServerError> {
    let partitioned = rows.iter().any(|row| {
        row.contains_key(COLLAPSE_PLACEHOLDER_RESERVED_KEY)
            || row.contains_key(COLLAPSE_DATA_RESERVED_KEY)
    });
    if partitioned {
        let collapsed = collapse_partitioned_items(rows)?;
        let [map] = collapsed.as_slice() else {
            return Err(DynamoInvalidBundle::new(
                "partitioned logical object did not collapse to one item",
            ));
        };
        return Ok((DynamoBundleStorage::ExtPartitioned, normalized_data(map)?));
    }
    let [map] = rows.as_slice() else {
        return Err(DynamoInvalidBundle::new(
            "non-partitioned logical object had multiple physical rows",
        ));
    };
    Ok((DynamoBundleStorage::Standard, normalized_data(map)?))
}

fn normalized_data(map: &DynamoMap) -> Result<Value, ServerError> {
    let Value::Object(mut data) = dynamo_map_to_serde_value(map)? else {
        unreachable!("Dynamo map conversion always returns an object")
    };
    data.remove("pk");
    data.remove("sk");
    // Bundle imports represent new writes, so temporal metadata is regenerated
    // consistently for standard, ext-partitioned, and opaque batch rows.
    data.remove(AUTO_FIELDS_CREATED_AT);
    data.remove(AUTO_FIELDS_UPDATED_AT);
    Ok(Value::Object(data))
}

fn group_logical_rows(rows: Vec<DynamoMap>) -> Result<HashMap<PkSk, Vec<DynamoMap>>, ServerError> {
    let mut groups: HashMap<PkSk, Vec<DynamoMap>> = HashMap::new();
    for row in rows {
        let id = PkSk::from_map(&row)?;
        groups.entry(ext_base_id(&id)).or_default().push(row);
    }
    for rows in groups.values_mut() {
        rows.sort_unstable_by(|left, right| {
            let (_, left_sk) =
                id_fields_from_map(left).expect("grouped DynamoDB row must retain its pk/sk");
            let (_, right_sk) =
                id_fields_from_map(right).expect("grouped DynamoDB row must retain its pk/sk");
            left_sk.cmp(right_sk)
        });
    }
    Ok(groups)
}

async fn raw_query(
    util: &DynamoUtil,
    pk: &str,
    sk_prefix: Option<&str>,
) -> Result<Vec<DynamoMap>, ServerError> {
    let mut values: HashMap<String, AttributeValue> = collection! {
        ":pk".to_string() => AttributeValue::S(pk.to_string()),
    };
    let condition = if let Some(prefix) = sk_prefix {
        values.insert(":sk".to_string(), AttributeValue::S(prefix.to_string()));
        "pk = :pk AND begins_with(sk, :sk)"
    } else {
        "pk = :pk"
    };
    let pages = util
        .backend
        .query(
            util.table.clone(),
            None,
            condition.to_string(),
            values,
            None,
        )
        .await
        .map_err(|error| DynamoCalloutError::with_debug(&error))?;
    Ok(pages
        .into_iter()
        .flat_map(|page| page.items.unwrap_or_default())
        .collect())
}

fn is_inline_descendant(sk: &str, parent_sk: &str) -> bool {
    sk.strip_prefix(parent_sk)
        .is_some_and(|suffix| suffix.starts_with(['#', '@']))
}
