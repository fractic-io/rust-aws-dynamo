use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use aws_sdk_dynamodb::types::AttributeValue;
use fractic_core::collection;
use fractic_server_error::ServerError;
use futures_util::{stream, StreamExt as _, TryStreamExt as _};
use serde_json::Value;

use crate::{
    errors::{DynamoCalloutError, DynamoInvalidOperation, DynamoNotFound},
    ext::crud::DynamoCrudAlgorithms,
    schema::{
        id_calculations::{get_object_type, strip_ext_suffix},
        parsing::dynamo_map_to_serde_value,
        ForeignRef, PkSk,
    },
    util::{
        collapse_helpers::collapse_partitioned_items, DynamoMap, DynamoUtil,
        AUTO_FIELDS_CREATED_AT, AUTO_FIELDS_UPDATED_AT, COLLAPSE_DATA_RESERVED_KEY,
        COLLAPSE_PLACEHOLDER_RESERVED_KEY,
    },
};

use super::{
    spec::BundlePolicyCache, value::value_at_path, BundleId, BundleIdLogic, BundleNesting,
    DynamoBundle, DynamoBundleItem, DynamoBundlePolicy, DynamoBundleReference,
    DynamoBundleReferenceEncoding, DynamoBundleReferenceMatchTarget, DynamoBundleReferenceTarget,
    DynamoBundleSpec, DynamoBundleStorage,
};

const QUERY_CONCURRENCY: usize = 16;

#[derive(Clone)]
pub(crate) struct CollectedItem {
    pub id: PkSk,
    pub parent: Option<PkSk>,
    pub nesting: BundleNesting,
    pub rows: Vec<DynamoMap>,
    descendant_exclusions: BTreeSet<String>,
}

pub(crate) async fn export_from_specs(
    util: &DynamoUtil,
    algorithms: &dyn DynamoCrudAlgorithms,
    root: PkSk,
    root_nesting: BundleNesting,
    root_id_logic: BundleIdLogic,
    recursive: bool,
) -> Result<DynamoBundle, ServerError> {
    export_inner(
        util,
        algorithms,
        root,
        root_nesting,
        root_id_logic,
        recursive,
        None,
        true,
    )
    .await
}

/// Exports a destination snapshot using the incoming bundle's omission policy.
pub(crate) async fn export_with_policy(
    util: &DynamoUtil,
    algorithms: &dyn DynamoCrudAlgorithms,
    root: PkSk,
    root_nesting: BundleNesting,
    root_id_logic: BundleIdLogic,
    recursive: bool,
    exclusions: &BTreeMap<String, BTreeSet<String>>,
) -> Result<DynamoBundle, ServerError> {
    export_inner(
        util,
        algorithms,
        root,
        root_nesting,
        root_id_logic,
        recursive,
        Some(exclusions),
        false,
    )
    .await
}

async fn export_inner(
    util: &DynamoUtil,
    algorithms: &dyn DynamoCrudAlgorithms,
    root: PkSk,
    root_nesting: BundleNesting,
    root_id_logic: BundleIdLogic,
    recursive: bool,
    fixed_exclusions: Option<&BTreeMap<String, BTreeSet<String>>>,
    include_references: bool,
) -> Result<DynamoBundle, ServerError> {
    let mut specs = BundlePolicyCache::new(algorithms);
    let (collected, exclusions) = collect_items_with_specs(
        util,
        &mut specs,
        &root,
        root_nesting,
        recursive,
        fixed_exclusions,
    )
    .await?;

    let root_id = logical_base_id(&root);
    let mut by_pk_sk = HashMap::new();
    let mut items = Vec::with_capacity(collected.len());
    for (index, item) in collected.iter().enumerate() {
        let label = get_object_type(&item.id.pk, &item.id.sk)?.to_string();
        let (storage, data) = normalize_rows(item.rows.clone())?;
        let id_logic = if item.id == root_id {
            root_id_logic
        } else {
            specs.require_included(&label)?.id_logic
        };
        let id = BundleId {
            value: index as u64,
            label,
            original_sk: item.id.sk.clone(),
        };
        by_pk_sk.insert(item.id.clone(), id.clone());
        items.push(DynamoBundleItem {
            id,
            id_logic,
            parent: None,
            nesting: item.nesting,
            storage,
            data,
        });
    }
    for (item, collected) in items.iter_mut().zip(&collected) {
        item.parent = collected
            .parent
            .as_ref()
            .and_then(|id| by_pk_sk.get(id))
            .cloned();
    }

    let root = by_pk_sk
        .get(&logical_base_id(&root))
        .cloned()
        .ok_or_else(|| invalid_bundle("source root was not returned by DynamoDB"))?;
    let mut bundle = DynamoBundle {
        version: DynamoBundle::VERSION,
        root,
        recursive,
        exclusions,
        items,
        references: Vec::new(),
    };
    if include_references {
        bundle.references = collect_references(&mut specs, &bundle, &by_pk_sk)?;
    }
    Ok(bundle)
}

pub(crate) async fn collect_items(
    util: &DynamoUtil,
    algorithms: &dyn DynamoCrudAlgorithms,
    root: &PkSk,
    root_nesting: BundleNesting,
    recursive: bool,
    fixed_exclusions: Option<&BTreeMap<String, BTreeSet<String>>>,
) -> Result<(Vec<CollectedItem>, BTreeMap<String, BTreeSet<String>>), ServerError> {
    let mut specs = BundlePolicyCache::new(algorithms);
    collect_items_with_specs(
        util,
        &mut specs,
        root,
        root_nesting,
        recursive,
        fixed_exclusions,
    )
    .await
}

async fn collect_items_with_specs(
    util: &DynamoUtil,
    specs: &mut BundlePolicyCache<'_>,
    root: &PkSk,
    root_nesting: BundleNesting,
    recursive: bool,
    fixed_exclusions: Option<&BTreeMap<String, BTreeSet<String>>>,
) -> Result<(Vec<CollectedItem>, BTreeMap<String, BTreeSet<String>>), ServerError> {
    let root = logical_base_id(root);
    let root_label = get_object_type(&root.pk, &root.sk)?;
    let root_spec = specs.require_included(root_label)?;
    let mut recorded = fixed_exclusions.cloned().unwrap_or_default();
    let root_exclusions = exclusions_for(root_spec, fixed_exclusions, &mut recorded, root_label);
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
        descendant_exclusions: root_exclusions.clone(),
    }];
    if !recursive {
        return Ok((collected, recorded));
    }

    initial_groups.retain(|id, _| is_inline_descendant(&id.sk, &root.sk));
    append_partition_groups(
        &mut collected,
        &root,
        initial_groups,
        &root_exclusions,
        specs,
        fixed_exclusions,
        &mut recorded,
    )?;

    let mut queried = HashSet::new();
    let mut frontier = collected
        .iter()
        .map(|item| item.id.clone())
        .collect::<Vec<_>>();
    while !frontier.is_empty() {
        let owners = frontier
            .drain(..)
            .filter(|id| queried.insert(id.clone()))
            .collect::<Vec<_>>();
        let mut results = stream::iter(owners)
            .map(|owner| async move {
                let rows = raw_query(util, &owner.sk, None).await?;
                Ok::<_, ServerError>((owner, group_logical_rows(rows)?))
            })
            .buffer_unordered(QUERY_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        results.sort_by(|(a, _), (b, _)| a.pk.cmp(&b.pk).then_with(|| a.sk.cmp(&b.sk)));

        for (owner, groups) in results {
            let before = collected.len();
            let exclusions = collected
                .iter()
                .find(|item| item.id == owner)
                .map(|item| item.descendant_exclusions.clone())
                .unwrap_or_default();
            append_partition_groups(
                &mut collected,
                &owner,
                groups,
                &exclusions,
                specs,
                fixed_exclusions,
                &mut recorded,
            )?;
            frontier.extend(collected[before..].iter().map(|item| item.id.clone()));
        }
    }
    Ok((collected, recorded))
}

fn exclusions_for(
    spec: &DynamoBundleSpec,
    fixed: Option<&BTreeMap<String, BTreeSet<String>>>,
    recorded: &mut BTreeMap<String, BTreeSet<String>>,
    label: &str,
) -> BTreeSet<String> {
    if let Some(fixed) = fixed {
        return fixed.get(label).cloned().unwrap_or_default();
    }
    let exclusions = spec.exclude_subtrees.clone();
    if !exclusions.is_empty() {
        recorded.insert(label.to_string(), exclusions.clone());
    }
    exclusions
}

fn append_partition_groups(
    collected: &mut Vec<CollectedItem>,
    owner: &PkSk,
    groups: HashMap<PkSk, Vec<DynamoMap>>,
    owner_exclusions: &BTreeSet<String>,
    specs: &mut BundlePolicyCache<'_>,
    fixed_exclusions: Option<&BTreeMap<String, BTreeSet<String>>>,
    recorded: &mut BTreeMap<String, BTreeSet<String>>,
) -> Result<(), ServerError> {
    let mut ids = groups.keys().cloned().collect::<Vec<_>>();
    ids.sort_by(|a, b| a.sk.len().cmp(&b.sk.len()).then_with(|| a.sk.cmp(&b.sk)));
    let owner_label = get_object_type(&owner.pk, &owner.sk)?.to_string();
    let mut accepted = vec![(owner.clone(), owner_exclusions.clone(), owner_label.clone())];
    let mut excluded = Vec::<PkSk>::new();
    for id in ids {
        let label = get_object_type(&id.pk, &id.sk)?.to_string();
        let inline_parent = accepted
            .iter()
            .filter(|(candidate, _, _)| is_inline_descendant(&id.sk, &candidate.sk))
            .max_by_key(|(candidate, _, _)| candidate.sk.len());
        let inherited_exclusions = inline_parent
            .map(|(_, exclusions, _)| exclusions)
            .unwrap_or(owner_exclusions);
        if inherited_exclusions.contains(&label)
            || excluded
                .iter()
                .any(|ancestor| is_inline_descendant(&id.sk, &ancestor.sk))
        {
            excluded.push(id);
            continue;
        }
        let (parent, parent_label, nesting) = match inline_parent {
            Some((parent, _, parent_label)) => {
                (parent.clone(), parent_label.clone(), BundleNesting::Inline)
            }
            None => (owner.clone(), owner_label.clone(), BundleNesting::TopLevel),
        };
        let spec = match specs.get(&label) {
            DynamoBundlePolicy::Include(spec) => spec,
            DynamoBundlePolicy::ExcludeSubtree => {
                recorded
                    .entry(parent_label)
                    .or_default()
                    .insert(label.clone());
                excluded.push(id);
                continue;
            }
            DynamoBundlePolicy::Reject => {
                return Err(DynamoInvalidOperation::new(&format!(
                    "Dynamo object label `{label}` is not allowed in bundles"
                )))
            }
        };
        let mut descendant_exclusions = inherited_exclusions.clone();
        descendant_exclusions.extend(exclusions_for(spec, fixed_exclusions, recorded, &label));
        collected.push(CollectedItem {
            id: id.clone(),
            parent: Some(parent),
            nesting,
            rows: groups
                .get(&id)
                .cloned()
                .ok_or_else(|| invalid_bundle("grouped rows disappeared"))?,
            descendant_exclusions: descendant_exclusions.clone(),
        });
        accepted.push((id, descendant_exclusions, label));
    }
    Ok(())
}

fn normalize_rows(mut rows: Vec<DynamoMap>) -> Result<(DynamoBundleStorage, Value), ServerError> {
    let partitioned = rows.iter().any(|row| {
        row.contains_key(COLLAPSE_PLACEHOLDER_RESERVED_KEY)
            || row.contains_key(COLLAPSE_DATA_RESERVED_KEY)
    });
    if partitioned {
        let mut collapsed = collapse_partitioned_items(rows)?;
        if collapsed.len() != 1 {
            return Err(invalid_bundle(
                "partitioned logical object did not collapse to one item",
            ));
        }
        return Ok((
            DynamoBundleStorage::ExtPartitioned,
            normalized_data(&collapsed.remove(0))?,
        ));
    }
    if rows.len() != 1 {
        return Err(invalid_bundle(
            "non-partitioned logical object had multiple physical rows",
        ));
    }
    Ok((
        DynamoBundleStorage::Standard,
        normalized_data(&rows.remove(0))?,
    ))
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

fn collect_references(
    specs: &mut BundlePolicyCache<'_>,
    bundle: &DynamoBundle,
    original_ids: &HashMap<PkSk, BundleId>,
) -> Result<Vec<DynamoBundleReference>, ServerError> {
    let mut references = Vec::new();
    for item in &bundle.items {
        let spec = specs.require_included(&item.id.label)?;
        for rule in &spec.reference_rules {
            for matched in (rule.selector)(item)? {
                let original = value_at_path(&item.data, &matched.path)
                    .cloned()
                    .ok_or_else(|| invalid_bundle("reference path was missing"))?;
                let target = match matched.target {
                    DynamoBundleReferenceMatchTarget::Internal {
                        target_label,
                        encoding,
                    } => {
                        let id = find_internal_target(
                            bundle,
                            original_ids,
                            &original,
                            encoding,
                            target_label.as_deref(),
                        )?;
                        DynamoBundleReferenceTarget::Internal { id, encoding }
                    }
                    DynamoBundleReferenceMatchTarget::External {
                        lookup_id,
                        clear_path,
                    } => DynamoBundleReferenceTarget::External {
                        lookup_id,
                        clear_path: clear_path.unwrap_or_else(|| matched.path.clone()),
                    },
                };
                references.push(DynamoBundleReference {
                    source: item.id.clone(),
                    path: matched.path,
                    target,
                });
            }
        }
    }
    Ok(references)
}

fn find_internal_target(
    bundle: &DynamoBundle,
    original_ids: &HashMap<PkSk, BundleId>,
    value: &Value,
    encoding: DynamoBundleReferenceEncoding,
    target_label: Option<&str>,
) -> Result<BundleId, ServerError> {
    let raw = value
        .as_str()
        .ok_or_else(|| invalid_bundle("reference value was not a string"))?;
    let foreign_ref = if encoding == DynamoBundleReferenceEncoding::ForeignRef {
        Some(
            serde_json::from_value::<ForeignRef<'static>>(Value::String(raw.to_owned()))
                .map_err(|_| invalid_bundle("foreign reference was invalid"))?,
        )
    } else {
        None
    };
    let matches = bundle
        .items
        .iter()
        .filter(|item| target_label.is_none_or(|label| item.id.label == label))
        .filter(|item| match encoding {
            DynamoBundleReferenceEncoding::PkSk => PkSk::from_string(raw)
                .ok()
                .and_then(|id| original_ids.get(&id))
                .is_some_and(|id| id == &item.id),
            DynamoBundleReferenceEncoding::ForeignRef => foreign_ref
                .as_ref()
                .is_some_and(|reference| terminal_ref(&item.id.original_sk) == reference.raw()),
        })
        .map(|item| item.id.clone())
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [target] => Ok(target.clone()),
        [] => Err(invalid_bundle("local reference target was not bundled")),
        _ => Err(invalid_bundle("local reference target was ambiguous")),
    }
}

fn group_logical_rows(rows: Vec<DynamoMap>) -> Result<HashMap<PkSk, Vec<DynamoMap>>, ServerError> {
    let mut groups = HashMap::new();
    for row in rows {
        let id = PkSk::from_map(&row)?;
        groups
            .entry(logical_base_id(&id))
            .or_insert_with(Vec::new)
            .push(row);
    }
    for rows in groups.values_mut() {
        rows.sort_by_key(|row| PkSk::from_map(row).map(|id| id.sk).unwrap_or_default());
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

pub(crate) fn logical_base_id(id: &PkSk) -> PkSk {
    PkSk {
        pk: id.pk.clone(),
        sk: strip_ext_suffix(&id.sk).to_string(),
    }
}

fn is_inline_descendant(sk: &str, parent_sk: &str) -> bool {
    sk.strip_prefix(parent_sk)
        .is_some_and(|suffix| suffix.starts_with('#') || suffix.starts_with('@'))
}

pub(crate) fn terminal_ref(sk: &str) -> &str {
    let sk = strip_ext_suffix(sk);
    if let Some(at) = sk.rfind('@') {
        let singleton = &sk[at + 1..];
        if let (Some(open), Some(close)) = (singleton.find('['), singleton.find(']')) {
            return &singleton[open + 1..close];
        }
        return "";
    }
    sk.rsplit_once('#').map(|(_, value)| value).unwrap_or(sk)
}

fn invalid_bundle(details: &str) -> ServerError {
    DynamoInvalidOperation::new(&format!("invalid Dynamo bundle: {details}"))
}
