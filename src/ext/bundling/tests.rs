use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use aws_sdk_dynamodb::{
    operation::{
        batch_get_item::BatchGetItemOutput, batch_write_item::BatchWriteItemOutput,
        query::QueryOutput,
    },
    primitives::Blob,
    types::AttributeValue,
};
use fractic_server_error::ServerError;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    ext::crud::DynamoCrudAlgorithms,
    schema::{
        parsing::{dynamo_map_to_serde_value, serde_value_to_dynamo_map},
        IdLogic, NestingLogic, PkSk,
    },
    util::{
        backend::MockDynamoBackend, DynamoMap, DynamoUtil, AUTO_FIELDS_CREATED_AT,
        AUTO_FIELDS_UPDATED_AT, COLLAPSE_DATA_RESERVED_KEY, COLLAPSE_PLACEHOLDER_RESERVED_KEY,
    },
};

use super::{
    export::export_from_specs,
    import::{build_id_map, import_bundle},
    spec::{effective_import_exclusions, BundleSpecCache},
    BundleId, BundleNesting, BundleValuePath, DynamoBundle, DynamoBundleItem,
    DynamoBundleReference, DynamoBundleReferenceEncoding, DynamoBundleReferenceTarget,
    DynamoBundleSpec, DynamoBundleStorage, DynamoImportWarning, IfExisting, BUNDLE_VERSION,
};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct TestRootData {}
crate::dynamo_object!(
    TestRoot,
    TestRootData,
    "ROOTOBJ",
    IdLogic::Uuid,
    NestingLogic::Root
);

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct TestSingletonData {}
crate::dynamo_object!(
    TestSingleton,
    TestSingletonData,
    "SETTINGS",
    IdLogic::Singleton,
    NestingLogic::Root
);

struct TestAlgorithms;

#[async_trait]
impl DynamoCrudAlgorithms for TestAlgorithms {
    async fn recursive_delete(&self, _id: PkSk) -> Result<(), ServerError> {
        Ok(())
    }

    fn bundle_spec(&self, id_label: &str) -> Option<DynamoBundleSpec> {
        match id_label {
            "ROOTOBJ" => Some(DynamoBundleSpec::default().excluding("RECALC")),
            "CHILD" => Some(DynamoBundleSpec::default().excluding("GRAND")),
            _ => None,
        }
    }
}

struct CountingAlgorithms(AtomicUsize);

#[async_trait]
impl DynamoCrudAlgorithms for CountingAlgorithms {
    async fn recursive_delete(&self, _id: PkSk) -> Result<(), ServerError> {
        Ok(())
    }

    fn bundle_spec(&self, _id_label: &str) -> Option<DynamoBundleSpec> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Some(DynamoBundleSpec::default())
    }
}

fn id(value: u64, label: &str, sk: &str) -> BundleId {
    BundleId {
        value,
        label: label.into(),
        original_sk: sk.into(),
    }
}

fn row(pk: &str, sk: &str) -> DynamoMap {
    HashMap::from([
        ("pk".into(), AttributeValue::S(pk.into())),
        ("sk".into(), AttributeValue::S(sk.into())),
        ("value".into(), AttributeValue::S(format!("data:{sk}"))),
    ])
}

fn bundle_item(
    id: BundleId,
    parent: Option<BundleId>,
    nesting: BundleNesting,
    data: Value,
) -> DynamoBundleItem {
    DynamoBundleItem {
        id,
        parent,
        nesting,
        storage: DynamoBundleStorage::Standard,
        data,
    }
}

fn util(backend: MockDynamoBackend) -> DynamoUtil {
    DynamoUtil {
        backend: Arc::new(backend),
        table: "table".into(),
    }
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn recursive_export_scopes_exclusions_and_normalizes_singleton_ext() {
    let root_sk = "ROOTOBJ#root";
    let mut backend = MockDynamoBackend::new();
    backend
        .expect_query()
        .times(7)
        .returning(move |_, _, _, values, _| {
            let pk = values.get(":pk").unwrap().as_s().unwrap();
            let rows = match pk.as_str() {
                "ROOT" => {
                    let mut root = row("ROOT", root_sk);
                    root.insert(
                        AUTO_FIELDS_CREATED_AT.into(),
                        AttributeValue::S("old-created".into()),
                    );
                    root.insert(
                        AUTO_FIELDS_UPDATED_AT.into(),
                        AttributeValue::S("old-updated".into()),
                    );
                    vec![
                        root,
                        row("ROOT", "ROOTOBJ#root#ROOTINLINE#one"),
                        // `begins_with` returns this too, but it is not structural.
                        row("ROOT", "ROOTOBJ#root2"),
                    ]
                }
                "ROOTOBJ#root" => {
                    let mut placeholder = row(root_sk, "@BIG");
                    placeholder.remove("value");
                    placeholder.insert(
                        COLLAPSE_PLACEHOLDER_RESERVED_KEY.into(),
                        AttributeValue::N("1".into()),
                    );
                    let mut partition = row(root_sk, "@BIG+0");
                    partition.remove("value");
                    partition.insert(
                        COLLAPSE_DATA_RESERVED_KEY.into(),
                        AttributeValue::S(r#"{"large":"yes"}"#.into()),
                    );
                    let mut batch = row(root_sk, "BATCH#-");
                    batch.insert(
                        "..".into(),
                        AttributeValue::L(vec![AttributeValue::M(HashMap::from([(
                            "name".into(),
                            AttributeValue::S("opaque".into()),
                        )]))]),
                    );
                    vec![
                        row(root_sk, "CHILD#one"),
                        // CHILD's own policy must apply while processing the
                        // same query that first returned CHILD.
                        row(root_sk, "CHILD#one#GRAND#inline-ignored"),
                        placeholder,
                        partition,
                        batch,
                        row(root_sk, "RECALC#ignored"),
                    ]
                }
                "CHILD#one" => vec![
                    row("CHILD#one", "GRAND#ignored"),
                    row("CHILD#one", "KEEP#one"),
                ],
                "ROOTOBJ#root#ROOTINLINE#one" | "@BIG" | "BATCH#-" | "KEEP#one" => vec![],
                unexpected => panic!("unexpected partition query: {unexpected}"),
            };
            Ok(vec![QueryOutput::builder().set_items(Some(rows)).build()])
        });

    let bundle = export_from_specs(
        &util(backend),
        &TestAlgorithms,
        PkSk {
            pk: "ROOT".into(),
            sk: root_sk.into(),
        },
        BundleNesting::Root,
        true,
    )
    .await
    .unwrap();

    assert_eq!(bundle.items.len(), 6);
    assert_eq!(
        bundle.exclusions,
        BTreeMap::from([
            ("CHILD".into(), BTreeSet::from(["GRAND".into()])),
            ("ROOTOBJ".into(), BTreeSet::from(["RECALC".into()])),
        ])
    );
    assert!(!bundle
        .items
        .iter()
        .any(|item| matches!(item.id.label.as_str(), "RECALC" | "GRAND")));
    assert!(!bundle
        .items
        .iter()
        .any(|item| item.id.original_sk == "ROOTOBJ#root2"));
    let root = bundle
        .items
        .iter()
        .find(|item| item.id.label == "ROOTOBJ")
        .unwrap();
    assert!(root.data.get(AUTO_FIELDS_CREATED_AT).is_none());
    assert!(root.data.get(AUTO_FIELDS_UPDATED_AT).is_none());

    let big = bundle
        .items
        .iter()
        .find(|item| item.id.label == "BIG")
        .unwrap();
    assert_eq!(big.storage, DynamoBundleStorage::ExtPartitioned);
    assert_eq!(big.data["large"], "yes");

    let batch = bundle
        .items
        .iter()
        .find(|item| item.id.label == "BATCH")
        .unwrap();
    assert_eq!(batch.storage, DynamoBundleStorage::Standard);
    assert_eq!(batch.data[".."][0]["name"], "opaque");

    let inline = bundle
        .items
        .iter()
        .find(|item| item.id.label == "ROOTINLINE")
        .unwrap();
    assert_eq!(inline.nesting, BundleNesting::Inline);
    assert_eq!(inline.parent.as_ref().unwrap().label, "ROOTOBJ");
    serde_json::from_str::<DynamoBundle>(&serde_json::to_string(&bundle).unwrap()).unwrap();
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn export_loads_each_label_spec_only_once() {
    let algorithms = CountingAlgorithms(AtomicUsize::new(0));
    let mut backend = MockDynamoBackend::new();
    backend.expect_query().times(1).returning(|_, _, _, _, _| {
        Ok(vec![QueryOutput::builder()
            .set_items(Some(vec![row("ROOT", "ROOTOBJ#root")]))
            .build()])
    });

    export_from_specs(
        &util(backend),
        &algorithms,
        PkSk {
            pk: "ROOT".into(),
            sk: "ROOTOBJ#root".into(),
        },
        BundleNesting::Root,
        false,
    )
    .await
    .unwrap();

    assert_eq!(algorithms.0.load(Ordering::Relaxed), 1);
}

#[test]
fn import_exclusions_are_the_strict_union_and_require_present_owner_labels() {
    let root = id(0, "ROOTOBJ", "ROOTOBJ#root");
    let mut bundle = DynamoBundle {
        version: BUNDLE_VERSION,
        root: root.clone(),
        recursive: true,
        exclusions: BTreeMap::from([("ROOTOBJ".into(), BTreeSet::from(["BUNDLE_ONLY".into()]))]),
        items: vec![bundle_item(root, None, BundleNesting::Root, json!({}))],
        references: vec![],
    };
    let effective =
        effective_import_exclusions(&bundle, &mut BundleSpecCache::new(&TestAlgorithms)).unwrap();
    assert_eq!(
        effective["ROOTOBJ"],
        BTreeSet::from(["BUNDLE_ONLY".into(), "RECALC".into()])
    );

    bundle.exclusions = BTreeMap::from([("ABSENT_OWNER".into(), BTreeSet::from(["CHILD".into()]))]);
    assert!(
        effective_import_exclusions(&bundle, &mut BundleSpecCache::new(&TestAlgorithms),).is_err()
    );
}

#[test]
fn ext_partitioned_storage_accepts_the_previous_serialized_name() {
    assert_eq!(
        serde_json::from_value::<DynamoBundleStorage>(json!("singleton_ext")).unwrap(),
        DynamoBundleStorage::ExtPartitioned
    );
}

#[test]
fn serde_values_omit_null_object_fields_and_reject_dynamo_only_values() {
    let map = HashMap::from([
        ("text".into(), AttributeValue::S("hello".into())),
        ("none".into(), AttributeValue::Null(true)),
        (
            "list".into(),
            AttributeValue::L(vec![AttributeValue::Null(true)]),
        ),
    ]);
    let value = dynamo_map_to_serde_value(&map).unwrap();
    assert_eq!(value, json!({"text": "hello", "list": [null]}));
    assert_eq!(
        serde_value_to_dynamo_map(&value).unwrap(),
        map_without_null_field(map)
    );

    let unsupported =
        HashMap::from([("binary".into(), AttributeValue::B(Blob::new(vec![1, 2, 3])))]);
    assert!(dynamo_map_to_serde_value(&unsupported).is_err());
}

fn map_without_null_field(mut map: DynamoMap) -> DynamoMap {
    map.remove("none");
    map
}

#[test]
fn duplicate_mapping_reparents_inline_and_top_level_children() {
    let root = id(0, "ROOTOBJ", "ROOTOBJ#old");
    let top = id(1, "TOP", "TOP#old");
    let inline = id(2, "INLINE", "TOP#old#INLINE#old");
    let bundle = DynamoBundle {
        version: BUNDLE_VERSION,
        root: root.clone(),
        recursive: true,
        exclusions: BTreeMap::new(),
        items: vec![
            bundle_item(root.clone(), None, BundleNesting::Root, json!({})),
            bundle_item(
                top.clone(),
                Some(root.clone()),
                BundleNesting::TopLevel,
                json!({}),
            ),
            bundle_item(
                inline.clone(),
                Some(top.clone()),
                BundleNesting::Inline,
                json!({}),
            ),
        ],
        references: vec![],
    };

    let mapped = build_id_map(&bundle, None, true).unwrap();
    assert_eq!(mapped[&root].pk, "ROOT");
    assert_ne!(mapped[&root].sk, root.original_sk);
    assert_eq!(mapped[&top].pk, mapped[&root].sk);
    assert_eq!(mapped[&inline].pk, mapped[&top].pk);
    assert!(mapped[&inline]
        .sk
        .starts_with(&format!("{}#INLINE#", mapped[&top].sk)));
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn duplicate_remaps_internal_refs_and_clears_missing_same_table_refs() {
    let root = id(0, "ROOTOBJ", "ROOTOBJ#root");
    let target = id(1, "TARGET", "TARGET#old");
    let existing_external = PkSk {
        pk: "ROOT".into(),
        sk: "EXTERNAL#existing".into(),
    };
    let missing_external = PkSk {
        pk: "ROOT".into(),
        sk: "EXTERNAL#missing".into(),
    };
    let bundle = DynamoBundle {
        version: BUNDLE_VERSION,
        root: root.clone(),
        recursive: true,
        exclusions: BTreeMap::new(),
        items: vec![
            bundle_item(
                root.clone(),
                None,
                BundleNesting::Root,
                json!({
                    "local": "old",
                    "kept": existing_external.to_string(),
                    "missing": missing_external.to_string()
                }),
            ),
            bundle_item(
                target.clone(),
                Some(root.clone()),
                BundleNesting::TopLevel,
                json!({}),
            ),
        ],
        references: vec![
            DynamoBundleReference {
                source: root.clone(),
                path: BundleValuePath::field("local"),
                target: DynamoBundleReferenceTarget::Internal {
                    id: target,
                    encoding: DynamoBundleReferenceEncoding::ForeignRef,
                },
            },
            DynamoBundleReference {
                source: root.clone(),
                path: BundleValuePath::field("kept"),
                target: DynamoBundleReferenceTarget::External(existing_external.clone()),
            },
            DynamoBundleReference {
                source: root,
                path: BundleValuePath::field("missing"),
                target: DynamoBundleReferenceTarget::External(missing_external),
            },
        ],
    };
    let mut backend = MockDynamoBackend::new();
    backend
        .expect_batch_get_item()
        .times(2)
        .returning(move |table, keys, projection| {
            let is_conflict_check = keys.iter().any(|key| {
                key.get("sk")
                    .and_then(|value| value.as_s().ok())
                    .is_some_and(|sk| sk == "ROOTOBJ#root")
            });
            let rows = if is_conflict_check {
                vec![row("ROOT", "ROOTOBJ#root")]
            } else if projection.as_deref() == Some("pk, sk") && keys.len() == 2 {
                vec![row("ROOT", "EXTERNAL#existing")]
            } else {
                vec![]
            };
            Ok(BatchGetItemOutput::builder()
                .set_responses(Some(HashMap::from([(table, rows)])))
                .build())
        });
    backend
        .expect_batch_put_item()
        .times(1)
        .returning(|_, items| {
            assert_eq!(items.len(), 2);
            let root = items
                .iter()
                .find(|item| item["pk"].as_s().unwrap() == "ROOT")
                .unwrap();
            assert_ne!(root["local"], AttributeValue::S("old".into()));
            assert_eq!(
                root["kept"],
                AttributeValue::S("ROOT|EXTERNAL#existing".into())
            );
            assert!(!root.contains_key("missing"));
            Ok(BatchWriteItemOutput::builder().build())
        });

    let result = import_bundle::<TestRoot>(
        &util(backend),
        &TestAlgorithms,
        None,
        bundle.clone(),
        IfExisting::Duplicate,
    )
    .await
    .unwrap();
    // `import_bundle` owns and mutates the bundle; this assertion keeps the
    // original fixture useful and verifies the result contract instead.
    assert_eq!(bundle.items.len(), result.merged);
    assert_eq!(
        result.warnings,
        vec![DynamoImportWarning::MissingExternalReference]
    );
    assert!(result.duplicated);
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn merge_upserts_preserved_ids_and_removes_old_ext_partitions() {
    let root = id(0, "ROOTOBJ", "ROOTOBJ#root");
    let bundle = DynamoBundle {
        version: BUNDLE_VERSION,
        root: root.clone(),
        recursive: false,
        exclusions: BTreeMap::new(),
        items: vec![bundle_item(
            root,
            None,
            BundleNesting::Root,
            json!({"value": "ordinary"}),
        )],
        references: vec![],
    };
    let mut backend = MockDynamoBackend::new();
    backend
        .expect_batch_get_item()
        .times(1)
        .returning(|table, _, projection| {
            assert_eq!(projection, None);
            let mut placeholder = row("ROOT", "ROOTOBJ#root");
            placeholder.insert(
                COLLAPSE_PLACEHOLDER_RESERVED_KEY.into(),
                AttributeValue::N("2".into()),
            );
            Ok(BatchGetItemOutput::builder()
                .set_responses(Some(HashMap::from([(table, vec![placeholder])])))
                .build())
        });
    backend
        .expect_batch_delete_item()
        .times(1)
        .returning(|_, keys| {
            let mut ids = keys
                .iter()
                .map(PkSk::from_map)
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            ids.sort_by(|a, b| a.sk.cmp(&b.sk));
            assert_eq!(
                ids,
                vec![
                    PkSk {
                        pk: "ROOT".into(),
                        sk: "ROOTOBJ#root+0".into(),
                    },
                    PkSk {
                        pk: "ROOT".into(),
                        sk: "ROOTOBJ#root+1".into(),
                    },
                ]
            );
            Ok(BatchWriteItemOutput::builder().build())
        });
    backend
        .expect_batch_put_item()
        .times(1)
        .returning(|_, items| {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0]["sk"], AttributeValue::S("ROOTOBJ#root".into()));
            assert_eq!(items[0]["value"], AttributeValue::S("ordinary".into()));
            assert!(items[0].contains_key(AUTO_FIELDS_CREATED_AT));
            assert!(items[0].contains_key(AUTO_FIELDS_UPDATED_AT));
            Ok(BatchWriteItemOutput::builder().build())
        });

    let result = import_bundle::<TestRoot>(
        &util(backend),
        &TestAlgorithms,
        None,
        bundle,
        IfExisting::Merge,
    )
    .await
    .unwrap();
    assert_eq!(result.root_id.sk, "ROOTOBJ#root");
    assert_eq!(result.merged, 1);
    assert!(!result.duplicated);
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn replace_deletes_excluded_descendants_when_their_managed_parent_is_removed() {
    let root_id = id(0, "ROOTOBJ", "ROOTOBJ#root");
    let bundle = DynamoBundle {
        version: BUNDLE_VERSION,
        root: root_id.clone(),
        recursive: true,
        exclusions: BTreeMap::from([("ROOTOBJ".into(), BTreeSet::from(["RECALC".into()]))]),
        items: vec![bundle_item(
            root_id,
            None,
            BundleNesting::Root,
            json!({"value": "new"}),
        )],
        references: vec![],
    };

    let mut backend = MockDynamoBackend::new();
    backend
        .expect_batch_get_item()
        .times(1)
        .returning(|table, _, projection| {
            assert_eq!(projection, None);
            Ok(BatchGetItemOutput::builder()
                .set_responses(Some(HashMap::from([(
                    table,
                    vec![row("ROOT", "ROOTOBJ#root")],
                )])))
                .build())
        });
    backend
        .expect_query()
        .times(6)
        .returning(|_, _, _, values, _| {
            let pk = values.get(":pk").unwrap().as_s().unwrap();
            let rows = match pk.as_str() {
                "ROOT" => vec![row("ROOT", "ROOTOBJ#root")],
                "ROOTOBJ#root" => vec![row("ROOTOBJ#root", "STEP#old")],
                "STEP#old" => vec![row("STEP#old", "RECALC#history")],
                "RECALC#history" => vec![],
                unexpected => panic!("unexpected partition query: {unexpected}"),
            };
            Ok(vec![QueryOutput::builder().set_items(Some(rows)).build()])
        });
    backend
        .expect_batch_put_item()
        .times(1)
        .returning(|_, items| {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0]["value"], AttributeValue::S("new".into()));
            Ok(BatchWriteItemOutput::builder().build())
        });
    backend
        .expect_batch_delete_item()
        .times(1)
        .returning(|_, keys| {
            let ids = keys
                .iter()
                .map(PkSk::from_map)
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert!(ids.contains(&PkSk {
                pk: "ROOTOBJ#root".into(),
                sk: "STEP#old".into(),
            }));
            assert!(ids.contains(&PkSk {
                pk: "STEP#old".into(),
                sk: "RECALC#history".into(),
            }));
            Ok(BatchWriteItemOutput::builder().build())
        });

    let result = import_bundle::<TestRoot>(
        &util(backend),
        &TestAlgorithms,
        None,
        bundle,
        IfExisting::Replace,
    )
    .await
    .unwrap();

    assert_eq!(result.deleted, 1);
    assert!(!result.duplicated);
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn duplicate_rejects_a_conflicting_singleton_root_before_writing() {
    let root = id(0, "SETTINGS", "@SETTINGS");
    let bundle = DynamoBundle {
        version: BUNDLE_VERSION,
        root: root.clone(),
        recursive: false,
        exclusions: BTreeMap::new(),
        items: vec![bundle_item(
            root,
            None,
            BundleNesting::Root,
            json!({"value": "settings"}),
        )],
        references: vec![],
    };
    let mut backend = MockDynamoBackend::new();
    backend
        .expect_batch_get_item()
        .times(1)
        .returning(|table, _, _| {
            Ok(BatchGetItemOutput::builder()
                .set_responses(Some(HashMap::from([(
                    table,
                    vec![row("ROOT", "@SETTINGS")],
                )])))
                .build())
        });

    assert!(import_bundle::<TestSingleton>(
        &util(backend),
        &TestAlgorithms,
        None,
        bundle,
        IfExisting::Duplicate,
    )
    .await
    .is_err());
}

#[tokio::test]
async fn import_rejects_reference_paths_that_are_not_present() {
    let root = id(0, "ROOTOBJ", "ROOTOBJ#root");
    let bundle = DynamoBundle {
        version: BUNDLE_VERSION,
        root: root.clone(),
        recursive: false,
        exclusions: BTreeMap::new(),
        items: vec![bundle_item(
            root.clone(),
            None,
            BundleNesting::Root,
            json!({"present": "value"}),
        )],
        references: vec![DynamoBundleReference {
            source: root,
            path: BundleValuePath::field("missing"),
            target: DynamoBundleReferenceTarget::External(PkSk {
                pk: "ROOT".into(),
                sk: "TARGET#one".into(),
            }),
        }],
    };

    assert!(import_bundle::<TestRoot>(
        &util(MockDynamoBackend::new()),
        &TestAlgorithms,
        None,
        bundle,
        IfExisting::Merge,
    )
    .await
    .is_err());
}
