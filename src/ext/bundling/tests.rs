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
    policy::{validate_import_policy, BundlePolicyCache},
    BundleId, BundleIdLogic, BundleNesting, BundleValuePath, DynamoBundle, DynamoBundleItem,
    DynamoBundlePolicy, DynamoBundleReference, DynamoBundleReferenceEncoding,
    DynamoBundleReferenceTarget, DynamoBundleSpec, DynamoBundleStorage, DynamoImportWarning,
    IfExisting,
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

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct TestBatchData {}
crate::dynamo_object!(
    TestBatch,
    TestBatchData,
    "BATCH",
    IdLogic::BatchOptimized { batch_size: 10 },
    NestingLogic::TopLevelChildOfAny
);

struct TestAlgorithms;

#[async_trait]
impl DynamoCrudAlgorithms for TestAlgorithms {
    async fn recursive_delete(&self, _id: PkSk) -> Result<(), ServerError> {
        Ok(())
    }

    fn bundle_policy(&self, id_label: &str) -> DynamoBundlePolicy {
        match id_label {
            "ROOTOBJ" => DynamoBundleSpec::new(BundleIdLogic::Uuid)
                .excluding("RECALC")
                .into(),
            "CHILD" => DynamoBundleSpec::new(BundleIdLogic::Uuid)
                .excluding("GRAND")
                .into(),
            "BIG" => DynamoBundleSpec::new(BundleIdLogic::SingletonExt).into(),
            "BATCH" => DynamoBundleSpec::new(BundleIdLogic::BatchOptimized).into(),
            "SETTINGS" => DynamoBundleSpec::new(BundleIdLogic::Singleton).into(),
            "EXCLUDED" => DynamoBundlePolicy::ExcludeSubtree,
            "DENIED" => DynamoBundlePolicy::Reject,
            _ => DynamoBundleSpec::new(BundleIdLogic::Uuid).into(),
        }
    }
}

struct CountingAlgorithms(AtomicUsize);

#[async_trait]
impl DynamoCrudAlgorithms for CountingAlgorithms {
    async fn recursive_delete(&self, _id: PkSk) -> Result<(), ServerError> {
        Ok(())
    }

    fn bundle_policy(&self, _id_label: &str) -> DynamoBundlePolicy {
        self.0.fetch_add(1, Ordering::Relaxed);
        DynamoBundleSpec::new(BundleIdLogic::Uuid).into()
    }
}

struct DefaultAlgorithms;

#[async_trait]
impl DynamoCrudAlgorithms for DefaultAlgorithms {
    async fn recursive_delete(&self, _id: PkSk) -> Result<(), ServerError> {
        Ok(())
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
        id_logic: BundleIdLogic::Uuid,
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
async fn recursive_export_scopes_exclusions_and_normalizes_ext_partitioning() {
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
        BundleIdLogic::Uuid,
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
    assert_eq!(big.id_logic, BundleIdLogic::SingletonExt);
    assert_eq!(big.data["large"], "yes");

    let batch = bundle
        .items
        .iter()
        .find(|item| item.id.label == "BATCH")
        .unwrap();
    assert_eq!(batch.storage, DynamoBundleStorage::Standard);
    assert_eq!(batch.id_logic, BundleIdLogic::BatchOptimized);
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
async fn recursive_export_omits_excluded_subtrees_and_records_the_omission() {
    let root_sk = "ROOTOBJ#root";
    let mut backend = MockDynamoBackend::new();
    backend
        .expect_query()
        .times(2)
        .returning(move |_, _, _, values, _| {
            let pk = values.get(":pk").unwrap().as_s().unwrap();
            let rows = match pk.as_str() {
                "ROOT" => vec![row("ROOT", root_sk)],
                "ROOTOBJ#root" => vec![
                    row(root_sk, "EXCLUDED#one"),
                    row(root_sk, "EXCLUDED#one#CHILD#also-excluded"),
                ],
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
        BundleIdLogic::Uuid,
        true,
    )
    .await
    .unwrap();

    assert_eq!(bundle.items.len(), 1);
    assert_eq!(
        bundle.exclusions["ROOTOBJ"],
        BTreeSet::from(["EXCLUDED".into(), "RECALC".into()])
    );
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn recursive_export_rejects_denied_descendants() {
    let root_sk = "ROOTOBJ#root";
    let mut backend = MockDynamoBackend::new();
    backend
        .expect_query()
        .times(2)
        .returning(move |_, _, _, values, _| {
            let pk = values.get(":pk").unwrap().as_s().unwrap();
            let rows = match pk.as_str() {
                "ROOT" => vec![row("ROOT", root_sk)],
                "ROOTOBJ#root" => vec![row(root_sk, "DENIED#one")],
                unexpected => panic!("unexpected partition query: {unexpected}"),
            };
            Ok(vec![QueryOutput::builder().set_items(Some(rows)).build()])
        });

    let error = export_from_specs(
        &util(backend),
        &TestAlgorithms,
        PkSk {
            pk: "ROOT".into(),
            sk: root_sk.into(),
        },
        BundleNesting::Root,
        BundleIdLogic::Uuid,
        true,
    )
    .await
    .unwrap_err();

    assert!(error.to_string().contains("DENIED"));
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
        BundleIdLogic::Uuid,
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
        version: DynamoBundle::VERSION,
        root: root.clone(),
        recursive: true,
        exclusions: BTreeMap::from([("ROOTOBJ".into(), BTreeSet::from(["BUNDLE_ONLY".into()]))]),
        items: vec![bundle_item(root, None, BundleNesting::Root, json!({}))],
        references: vec![],
    };
    let effective = validate_import_policy(
        &bundle,
        &mut BundlePolicyCache::new(&TestAlgorithms),
        BundleIdLogic::Uuid,
    )
    .unwrap();
    assert_eq!(
        effective["ROOTOBJ"],
        BTreeSet::from(["BUNDLE_ONLY".into(), "RECALC".into()])
    );

    bundle.exclusions = BTreeMap::from([("ABSENT_OWNER".into(), BTreeSet::from(["CHILD".into()]))]);
    assert!(validate_import_policy(
        &bundle,
        &mut BundlePolicyCache::new(&TestAlgorithms),
        BundleIdLogic::Uuid,
    )
    .is_err());
}

#[test]
fn import_rejects_excluded_and_denied_bundle_items() {
    for label in ["EXCLUDED", "DENIED"] {
        let root = id(0, label, &format!("{label}#root"));
        let bundle = DynamoBundle {
            version: DynamoBundle::VERSION,
            root: root.clone(),
            recursive: false,
            exclusions: BTreeMap::new(),
            items: vec![bundle_item(root, None, BundleNesting::Root, json!({}))],
            references: vec![],
        };

        assert!(validate_import_policy(
            &bundle,
            &mut BundlePolicyCache::new(&TestAlgorithms),
            BundleIdLogic::Uuid,
        )
        .is_err());
    }
}

#[test]
fn import_rejects_id_logic_metadata_that_disagrees_with_local_policy() {
    let root = id(0, "ROOTOBJ", "ROOTOBJ#root");
    let child = id(1, "CHILD", "CHILD#child");
    let mut child_item = bundle_item(
        child,
        Some(root.clone()),
        BundleNesting::TopLevel,
        json!({}),
    );
    child_item.id_logic = BundleIdLogic::Timestamp;
    let bundle = DynamoBundle {
        version: DynamoBundle::VERSION,
        root: root.clone(),
        recursive: true,
        exclusions: BTreeMap::new(),
        items: vec![
            bundle_item(root, None, BundleNesting::Root, json!({})),
            child_item,
        ],
        references: vec![],
    };

    assert!(validate_import_policy(
        &bundle,
        &mut BundlePolicyCache::new(&TestAlgorithms),
        BundleIdLogic::Uuid,
    )
    .is_err());
}

#[test]
fn import_rejects_root_policy_that_disagrees_with_crud_type() {
    let root = id(0, "ROOTOBJ", "ROOTOBJ#root");
    let bundle = DynamoBundle {
        version: DynamoBundle::VERSION,
        root: root.clone(),
        recursive: false,
        exclusions: BTreeMap::new(),
        items: vec![bundle_item(root, None, BundleNesting::Root, json!({}))],
        references: vec![],
    };

    assert!(validate_import_policy(
        &bundle,
        &mut BundlePolicyCache::new(&TestAlgorithms),
        BundleIdLogic::Timestamp,
    )
    .is_err());
}

#[test]
fn bundling_is_denied_by_default() {
    assert!(matches!(
        DefaultAlgorithms.bundle_policy("UNREGISTERED"),
        DynamoBundlePolicy::Reject
    ));
    assert!(BundlePolicyCache::new(&DefaultAlgorithms)
        .require_included("UNREGISTERED")
        .is_err());
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
        version: DynamoBundle::VERSION,
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

    let mapped = build_id_map(&bundle, None, true, BundleIdLogic::Uuid).unwrap();
    assert_eq!(mapped[&root].pk, "ROOT");
    assert_ne!(mapped[&root].sk, root.original_sk);
    assert_eq!(mapped[&top].pk, mapped[&root].sk);
    assert_eq!(mapped[&inline].pk, mapped[&top].pk);
    assert!(mapped[&inline]
        .sk
        .starts_with(&format!("{}#INLINE#", mapped[&top].sk)));
}

#[test]
fn duplicate_mapping_preserves_batch_ids_and_increments_timestamp_millis() {
    let root = id(0, "ROOTOBJ", "ROOTOBJ#old");
    let first_timestamp = id(1, "EVENT", "EVENT#0000000000000001");
    let second_timestamp = id(2, "EVENT", "EVENT#0000000000000002");
    let batch = id(3, "BATCH", "BATCH#0");
    let mut first_timestamp_item = bundle_item(
        first_timestamp.clone(),
        Some(root.clone()),
        BundleNesting::TopLevel,
        json!({}),
    );
    first_timestamp_item.id_logic = BundleIdLogic::Timestamp;
    let mut second_timestamp_item = bundle_item(
        second_timestamp.clone(),
        Some(root.clone()),
        BundleNesting::TopLevel,
        json!({}),
    );
    second_timestamp_item.id_logic = BundleIdLogic::Timestamp;
    let mut batch_item = bundle_item(
        batch.clone(),
        Some(root.clone()),
        BundleNesting::TopLevel,
        json!({"..": [{"value": 1}]}),
    );
    batch_item.id_logic = BundleIdLogic::BatchOptimized;
    let bundle = DynamoBundle {
        version: DynamoBundle::VERSION,
        root: root.clone(),
        recursive: true,
        exclusions: BTreeMap::new(),
        items: vec![
            bundle_item(root.clone(), None, BundleNesting::Root, json!({})),
            first_timestamp_item,
            second_timestamp_item,
            batch_item,
        ],
        references: vec![],
    };

    let mapped = build_id_map(&bundle, None, true, BundleIdLogic::Uuid).unwrap();
    let first_millis = mapped[&first_timestamp]
        .sk
        .rsplit_once('#')
        .unwrap()
        .1
        .parse::<i64>()
        .unwrap();
    let second_millis = mapped[&second_timestamp]
        .sk
        .rsplit_once('#')
        .unwrap()
        .1
        .parse::<i64>()
        .unwrap();

    assert_eq!(second_millis, first_millis + 1);
    assert_eq!(mapped[&batch].sk, "BATCH#0");
    assert_eq!(mapped[&batch].pk, mapped[&root].sk);
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
        version: DynamoBundle::VERSION,
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
                    "missing": missing_external.to_string(),
                    "compound": [missing_external.to_string(), "kept member"]
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
                target: DynamoBundleReferenceTarget::External {
                    lookup_id: existing_external.clone(),
                    clear_path: BundleValuePath::field("kept"),
                },
            },
            DynamoBundleReference {
                source: root.clone(),
                path: BundleValuePath::field("missing"),
                target: DynamoBundleReferenceTarget::External {
                    lookup_id: missing_external.clone(),
                    clear_path: BundleValuePath::field("missing"),
                },
            },
            DynamoBundleReference {
                source: root,
                path: BundleValuePath::field("compound").then_index(0),
                target: DynamoBundleReferenceTarget::External {
                    lookup_id: missing_external,
                    clear_path: BundleValuePath::field("compound"),
                },
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
            assert!(!root.contains_key("compound"));
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
    assert_eq!(bundle.items.len(), result.written_objects);
    assert_eq!(
        result.warnings,
        vec![
            DynamoImportWarning::MissingExternalReference,
            DynamoImportWarning::MissingExternalReference,
        ]
    );
    assert!(result.duplicated);
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn external_reference_to_an_incoming_id_is_not_cleared() {
    let root = id(0, "ROOTOBJ", "ROOTOBJ#root");
    let target = id(1, "TARGET", "TARGET#target");
    let target_id = PkSk {
        pk: root.original_sk.clone(),
        sk: target.original_sk.clone(),
    };
    let bundle = DynamoBundle {
        version: DynamoBundle::VERSION,
        root: root.clone(),
        recursive: true,
        exclusions: BTreeMap::new(),
        items: vec![
            bundle_item(
                root.clone(),
                None,
                BundleNesting::Root,
                json!({"external": target_id.to_string()}),
            ),
            bundle_item(
                target.clone(),
                Some(root.clone()),
                BundleNesting::TopLevel,
                json!({}),
            ),
        ],
        references: vec![DynamoBundleReference {
            source: root,
            path: BundleValuePath::field("external"),
            target: DynamoBundleReferenceTarget::External {
                lookup_id: target_id.clone(),
                clear_path: BundleValuePath::field("external"),
            },
        }],
    };
    let mut backend = MockDynamoBackend::new();
    backend
        .expect_batch_get_item()
        .times(2)
        .returning(|table, _, _| {
            Ok(BatchGetItemOutput::builder()
                .set_responses(Some(HashMap::from([(table, vec![])])))
                .build())
        });
    backend
        .expect_batch_put_item()
        .times(1)
        .returning(move |_, items| {
            let imported_root = items
                .iter()
                .find(|item| item["pk"].as_s().unwrap() == "ROOT")
                .unwrap();
            assert_eq!(
                imported_root["external"],
                AttributeValue::S(target_id.to_string())
            );
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

    assert!(result.warnings.is_empty());
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn merge_upserts_preserved_ids_and_removes_old_ext_partitions() {
    let root = id(0, "ROOTOBJ", "ROOTOBJ#root");
    let bundle = DynamoBundle {
        version: DynamoBundle::VERSION,
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
    assert_eq!(result.written_objects, 1);
    assert!(!result.duplicated);
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn replace_deletes_excluded_descendants_when_their_managed_parent_is_removed() {
    let root_id = id(0, "ROOTOBJ", "ROOTOBJ#root");
    let bundle = DynamoBundle {
        version: DynamoBundle::VERSION,
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

    assert_eq!(result.deleted_subtree_roots, 1);
    assert!(!result.duplicated);
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn duplicate_rejects_a_conflicting_singleton_root_before_writing() {
    let root = id(0, "SETTINGS", "@SETTINGS");
    let mut root_item = bundle_item(
        root.clone(),
        None,
        BundleNesting::Root,
        json!({"value": "settings"}),
    );
    root_item.id_logic = BundleIdLogic::Singleton;
    let bundle = DynamoBundle {
        version: DynamoBundle::VERSION,
        root: root.clone(),
        recursive: false,
        exclusions: BTreeMap::new(),
        items: vec![root_item],
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
#[allow(clippy::result_large_err)]
async fn duplicate_rejects_a_conflicting_batch_optimized_root_before_writing() {
    let parent = PkSk {
        pk: "ROOT".into(),
        sk: "ROOTOBJ#parent".into(),
    };
    let root = id(0, "BATCH", "BATCH#0");
    let mut root_item = bundle_item(
        root.clone(),
        None,
        BundleNesting::TopLevel,
        json!({"..": [{"value": "batched"}]}),
    );
    root_item.id_logic = BundleIdLogic::BatchOptimized;
    let bundle = DynamoBundle {
        version: DynamoBundle::VERSION,
        root,
        recursive: false,
        exclusions: BTreeMap::new(),
        items: vec![root_item],
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
                    vec![row("ROOTOBJ#parent", "BATCH#0")],
                )])))
                .build())
        });

    let error = import_bundle::<TestBatch>(
        &util(backend),
        &TestAlgorithms,
        Some(&parent),
        bundle,
        IfExisting::Duplicate,
    )
    .await
    .unwrap_err();

    assert!(error.to_string().contains("batch-optimized bundle root"));
}

#[tokio::test]
async fn import_rejects_reference_paths_that_are_not_present() {
    let root = id(0, "ROOTOBJ", "ROOTOBJ#root");
    let bundle = DynamoBundle {
        version: DynamoBundle::VERSION,
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
            target: DynamoBundleReferenceTarget::External {
                lookup_id: PkSk {
                    pk: "ROOT".into(),
                    sk: "TARGET#one".into(),
                },
                clear_path: BundleValuePath::field("missing"),
            },
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
