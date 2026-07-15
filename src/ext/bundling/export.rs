use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use aws_sdk_dynamodb::types::AttributeValue;
use fractic_core::collection;
use fractic_server_error::ServerError;
use futures_util::{stream, StreamExt as _, TryStreamExt as _};
use serde_json::Value;

use crate::{
    errors::{DynamoCalloutError, DynamoNotFound},
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
    invalid_bundle,
    policy::{
        configured_bundle_policy, DynamoBundleObjectPolicy, DynamoBundleReferenceMatchTarget,
    },
    BundleId, BundleIdLogic, BundleNesting, DynamoBundle, DynamoBundleItem, DynamoBundlePolicy,
    DynamoBundleReference, DynamoBundleReferenceEncoding, DynamoBundleReferenceTarget,
    DynamoBundleStorage,
};

const QUERY_CONCURRENCY: usize = 16;

// Definitions.
// ----------------------------------------------------------------------------

#[derive(Clone)]
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
    recursive: bool,
    fixed_omissions: Option<&'a BTreeMap<String, BTreeSet<String>>>,
    include_references: bool,
}

// Private interface.
// ----------------------------------------------------------------------------

pub(crate) async fn export_from_config(
    util: &DynamoUtil,
    algorithms: &dyn DynamoCrudAlgorithms,
    root: PkSk,
    root_nesting: BundleNesting,
    root_id_logic: BundleIdLogic,
    recursive: bool,
) -> Result<DynamoBundle, ServerError> {
    let policy = configured_bundle_policy(algorithms);
    export_bundle(
        util,
        &policy,
        &root,
        ExportOptions {
            root_nesting,
            root_id_logic,
            recursive,
            fixed_omissions: None,
            include_references: true,
        },
    )
    .await
}

/// Exports a destination snapshot using the incoming bundle's omission policy.
pub(crate) async fn export_with_omissions(
    util: &DynamoUtil,
    bundles: &DynamoBundlePolicy,
    root: &PkSk,
    root_nesting: BundleNesting,
    root_id_logic: BundleIdLogic,
    recursive: bool,
    omissions: &BTreeMap<String, BTreeSet<String>>,
) -> Result<DynamoBundle, ServerError> {
    export_bundle(
        util,
        bundles,
        root,
        ExportOptions {
            root_nesting,
            root_id_logic,
            recursive,
            fixed_omissions: Some(omissions),
            include_references: false,
        },
    )
    .await
}

async fn export_bundle(
    util: &DynamoUtil,
    bundles: &DynamoBundlePolicy,
    root: &PkSk,
    options: ExportOptions<'_>,
) -> Result<DynamoBundle, ServerError> {
    let (collected, omitted_descendants) = collect_bundle_items(
        util,
        bundles,
        root,
        options.root_nesting,
        options.recursive,
        options.fixed_omissions,
    )
    .await?;

    let root_id = logical_base_id(root);
    let mut by_pk_sk = HashMap::new();
    let mut items = Vec::with_capacity(collected.len());
    for (index, item) in collected.into_iter().enumerate() {
        let label = get_object_type(&item.id.pk, &item.id.sk)?.to_string();
        let (storage, mut data) = normalize_rows(item.rows)?;
        let object = bundles.require(&label)?;
        object.normalize_renamed_fields(&mut data);
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
        let parent = item
            .parent
            .as_ref()
            .map(|parent| {
                by_pk_sk
                    .get(parent)
                    .cloned()
                    .ok_or_else(|| invalid_bundle("bundle item preceded its parent"))
            })
            .transpose()?;
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
        .ok_or_else(|| invalid_bundle("source root was not returned by DynamoDB"))?;
    let mut bundle = DynamoBundle {
        version: DynamoBundle::VERSION,
        root,
        recursive: options.recursive,
        omitted_descendants,
        items,
        references: Vec::new(),
    };
    if options.include_references {
        bundle.references = collect_references(bundles, &bundle, &by_pk_sk)?;
    }
    Ok(bundle)
}

pub(crate) async fn collect_bundle_items(
    util: &DynamoUtil,
    bundles: &DynamoBundlePolicy,
    root: &PkSk,
    root_nesting: BundleNesting,
    recursive: bool,
    fixed_omissions: Option<&BTreeMap<String, BTreeSet<String>>>,
) -> Result<(Vec<CollectedItem>, BTreeMap<String, BTreeSet<String>>), ServerError> {
    let root = logical_base_id(root);
    let root_label = get_object_type(&root.pk, &root.sk)?;
    let root_config = bundles.require(root_label)?;
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
        omitted_descendants: root_omissions.clone(),
    }];
    if !recursive {
        return Ok((collected, recorded));
    }

    initial_groups.retain(|id, _| is_inline_descendant(&id.sk, &root.sk));
    append_partition_groups(
        &mut collected,
        &root,
        initial_groups,
        &root_omissions,
        bundles,
        fixed_omissions,
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
            let omissions = collected
                .iter()
                .find(|item| item.id == owner)
                .map(|item| item.omitted_descendants.clone())
                .unwrap_or_default();
            append_partition_groups(
                &mut collected,
                &owner,
                groups,
                &omissions,
                bundles,
                fixed_omissions,
                &mut recorded,
            )?;
            frontier.extend(collected[before..].iter().map(|item| item.id.clone()));
        }
    }
    Ok((collected, recorded))
}

pub(crate) fn logical_base_id(id: &PkSk) -> PkSk {
    PkSk {
        pk: id.pk.clone(),
        sk: strip_ext_suffix(&id.sk).to_string(),
    }
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
    sk.rsplit_once('#').map_or(sk, |(_, value)| value)
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
    let omissions = object.omitted_descendants().clone();
    if !omissions.is_empty() {
        recorded.insert(label.to_string(), omissions.clone());
    }
    omissions
}

fn append_partition_groups(
    collected: &mut Vec<CollectedItem>,
    owner: &PkSk,
    groups: HashMap<PkSk, Vec<DynamoMap>>,
    owner_omissions: &BTreeSet<String>,
    bundles: &DynamoBundlePolicy,
    fixed_omissions: Option<&BTreeMap<String, BTreeSet<String>>>,
    recorded: &mut BTreeMap<String, BTreeSet<String>>,
) -> Result<(), ServerError> {
    let mut groups = groups.into_iter().collect::<Vec<_>>();
    groups.sort_by(|(a, _), (b, _)| a.sk.len().cmp(&b.sk.len()).then_with(|| a.sk.cmp(&b.sk)));
    let mut accepted = vec![(owner.clone(), owner_omissions.clone())];
    let mut omitted = Vec::<PkSk>::new();
    for (id, rows) in groups {
        let label = get_object_type(&id.pk, &id.sk)?.to_string();
        let inline_parent = accepted
            .iter()
            .filter(|(candidate, _)| is_inline_descendant(&id.sk, &candidate.sk))
            .max_by_key(|(candidate, _)| candidate.sk.len());
        let inherited_omissions = inline_parent.map_or(owner_omissions, |(_, omissions)| omissions);
        if inherited_omissions.contains(&label)
            || omitted
                .iter()
                .any(|ancestor| is_inline_descendant(&id.sk, &ancestor.sk))
        {
            omitted.push(id);
            continue;
        }
        let (parent, nesting) = match inline_parent {
            Some((parent, _)) => (parent.clone(), BundleNesting::Inline),
            None => (owner.clone(), BundleNesting::TopLevel),
        };
        let object = bundles.require(&label)?;
        let mut descendant_omissions = inherited_omissions.clone();
        descendant_omissions.extend(omissions_for(object, fixed_omissions, recorded, &label));
        collected.push(CollectedItem {
            id: id.clone(),
            parent: Some(parent),
            nesting,
            rows,
            omitted_descendants: descendant_omissions.clone(),
        });
        accepted.push((id, descendant_omissions));
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
            return Err(invalid_bundle(
                "partitioned logical object did not collapse to one item",
            ));
        };
        return Ok((DynamoBundleStorage::ExtPartitioned, normalized_data(map)?));
    }
    let [map] = rows.as_slice() else {
        return Err(invalid_bundle(
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

fn collect_references(
    bundles: &DynamoBundlePolicy,
    bundle: &DynamoBundle,
    original_ids: &HashMap<PkSk, BundleId>,
) -> Result<Vec<DynamoBundleReference>, ServerError> {
    let mut references = Vec::new();
    for item in &bundle.items {
        let object = bundles.require(&item.id.label)?;
        for rule in object.reference_rules() {
            for matched in (rule.selector)(item)? {
                let original = item
                    .value_at(&matched.path)
                    .cloned()
                    .ok_or_else(|| invalid_bundle("reference path was missing"))?;
                let target = match matched.target {
                    DynamoBundleReferenceMatchTarget::Bundled {
                        target_label,
                        encoding,
                    } => {
                        let id = find_bundled_target(
                            bundle,
                            original_ids,
                            &original,
                            encoding,
                            Some(&target_label),
                        )?;
                        DynamoBundleReferenceTarget::Bundled { id, encoding }
                    }
                    DynamoBundleReferenceMatchTarget::External {
                        lookup_id,
                        clear_path,
                    } => DynamoBundleReferenceTarget::External {
                        lookup_id,
                        clear_path,
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

fn find_bundled_target(
    bundle: &DynamoBundle,
    original_ids: &HashMap<PkSk, BundleId>,
    value: &Value,
    encoding: DynamoBundleReferenceEncoding,
    target_label: Option<&str>,
) -> Result<BundleId, ServerError> {
    let raw = value
        .as_str()
        .ok_or_else(|| invalid_bundle("reference value was not a string"))?;
    if encoding == DynamoBundleReferenceEncoding::PkSk {
        let id =
            PkSk::from_string(raw).map_err(|_| invalid_bundle("pk/sk reference was invalid"))?;
        let target = original_ids
            .get(&id)
            .filter(|target| target_label.is_none_or(|label| target.label == label))
            .ok_or_else(|| invalid_bundle("bundled reference target was absent"))?;
        return Ok(target.clone());
    }

    let reference = serde_json::from_value::<ForeignRef<'static>>(Value::String(raw.to_owned()))
        .map_err(|_| invalid_bundle("foreign reference was invalid"))?;
    let mut matches = bundle.items.iter().filter(|item| {
        target_label.is_none_or(|label| item.id.label == label)
            && terminal_ref(&item.id.original_sk) == reference.raw()
    });
    let target = matches
        .next()
        .ok_or_else(|| invalid_bundle("bundled reference target was absent"))?;
    if matches.next().is_some() {
        return Err(invalid_bundle("bundled reference target was ambiguous"));
    }
    Ok(target.id.clone())
}

fn group_logical_rows(rows: Vec<DynamoMap>) -> Result<HashMap<PkSk, Vec<DynamoMap>>, ServerError> {
    let mut groups: HashMap<PkSk, Vec<DynamoMap>> = HashMap::new();
    for row in rows {
        let id = PkSk::from_map(&row)?;
        groups.entry(logical_base_id(&id)).or_default().push(row);
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

fn is_inline_descendant(sk: &str, parent_sk: &str) -> bool {
    sk.strip_prefix(parent_sk)
        .is_some_and(|suffix| suffix.starts_with('#') || suffix.starts_with('@'))
}
