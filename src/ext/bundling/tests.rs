use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
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
        DynamoObject, IdLogic, NestingLogic, PkSk,
    },
    util::{
        backend::MockDynamoBackend, DynamoInsertPosition, DynamoMap, DynamoUtil,
        AUTO_FIELDS_CREATED_AT, AUTO_FIELDS_UPDATED_AT, COLLAPSE_DATA_RESERVED_KEY,
        COLLAPSE_PLACEHOLDER_RESERVED_KEY,
    },
};

use super::{
    entities_policy::{
        configured_bundle_policy, validate_import_policy, DynamoBundleReferenceMatchTarget,
    },
    impl_export::export_from_config,
    impl_import::{import_bundle, reconcile_replace_out_of_table_references},
    utils_id_mapping::build_id_map,
    utils_reference_manifest::{BundleReference, BundleReferenceTarget},
    BundleDataPath, BundleId, BundleIdLogic, BundleNesting, DynamoBundle, DynamoBundleItem,
    DynamoBundlePolicy, DynamoBundleReferenceEncoding, DynamoBundleReferenceMatch,
    DynamoBundleReferenceRule, DynamoBundleStorage, DynamoImportWarning, ImportMode,
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
pub struct TestOtherRootData {}
crate::dynamo_object!(
    TestOtherRoot,
    TestOtherRootData,
    "OTHERROOT",
    IdLogic::Uuid,
    NestingLogic::Root
);

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct TestRenamedRootData {
    pub canonical_ref: Option<PkSk>,
}
crate::dynamo_object!(
    TestRenamedRoot,
    TestRenamedRootData,
    "RENAMEDROOT",
    IdLogic::Uuid,
    NestingLogic::Root,
    renamed = ["legacy_ref" => "canonical_ref"]
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

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct TestRenamedBatchData {
    pub canonical_name: Option<String>,
}
crate::dynamo_object!(
    TestRenamedBatch,
    TestRenamedBatchData,
    "RENAMEDBATCH",
    IdLogic::BatchOptimized { batch_size: 10 },
    NestingLogic::TopLevelChildOfAny,
    renamed = ["legacy_name" => "canonical_name"]
);

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct TestOrderedData {
    pub name: Option<String>,
}
crate::dynamo_object!(
    TestOrdered,
    TestOrderedData,
    "ORDERED",
    IdLogic::Uuid,
    NestingLogic::TopLevelChildOfAny
);

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TestStrictChildData {
    pub required_name: String,
}
crate::dynamo_object!(
    TestStrictChild,
    TestStrictChildData,
    "STRICTCHILD",
    IdLogic::Uuid,
    NestingLogic::TopLevelChildOf("ROOTOBJ")
);

crate::dynamo_object!(
    TestSharedChildOfRoot,
    TestStrictChildData,
    "SHAREDCHILD",
    IdLogic::Uuid,
    NestingLogic::TopLevelChildOf("ROOTOBJ")
);
crate::dynamo_object!(
    TestSharedChildOfOtherRoot,
    TestStrictChildData,
    "SHAREDCHILD",
    IdLogic::Uuid,
    NestingLogic::TopLevelChildOf("OTHERROOT")
);

struct TestAlgorithms;

#[async_trait]
impl DynamoCrudAlgorithms for TestAlgorithms {
    async fn recursive_delete(&self, _id: PkSk) -> Result<(), ServerError> {
        Ok(())
    }

    fn bundle_policy(&self, bundles: &mut DynamoBundlePolicy) {
        for label in [
            "RECALC",
            "GRAND",
            "KEEP",
            "ROOTINLINE",
            "TARGET",
            "TOP",
            "INLINE",
            "EVENT",
            "EXTERNAL",
            "STEP",
        ] {
            bundles.include_label(label, BundleIdLogic::Uuid, &[]);
        }
        bundles
            .include_label("ROOTOBJ", BundleIdLogic::Uuid, &[])
            .omit_descendant_label("RECALC")
            .omit_descendant_label("EXCLUDED")
            .custom_references(test_references());
        bundles
            .include_label("CHILD", BundleIdLogic::Uuid, &[])
            .omit_descendant_label("GRAND");
        bundles.include_label("BIG", BundleIdLogic::SingletonExt, &[]);
        bundles.include_label("BATCH", BundleIdLogic::BatchOptimized, &[]);
        bundles.include_label("SETTINGS", BundleIdLogic::Singleton, &[]);
        bundles.include::<TestOrdered>();
    }
}

struct RequiredReferenceAlgorithms;

#[async_trait]
impl DynamoCrudAlgorithms for RequiredReferenceAlgorithms {
    async fn recursive_delete(&self, _id: PkSk) -> Result<(), ServerError> {
        Ok(())
    }

    fn bundle_policy(&self, bundles: &mut DynamoBundlePolicy) {
        bundles
            .include::<TestRoot>()
            .bundled_pksk::<TestOrdered>("required_target")
            .omit_descendants::<TestOrdered>();
        bundles.include::<TestOrdered>();
    }
}

struct RenamedReferenceAlgorithms;

#[async_trait]
impl DynamoCrudAlgorithms for RenamedReferenceAlgorithms {
    async fn recursive_delete(&self, _id: PkSk) -> Result<(), ServerError> {
        Ok(())
    }

    fn bundle_policy(&self, bundles: &mut DynamoBundlePolicy) {
        bundles
            .include::<TestRenamedRoot>()
            .bundled_pksk::<TestOrdered>("canonical_ref");
        bundles.include::<TestOrdered>();
    }
}

struct OutOfTableAlgorithms;

#[async_trait]
impl DynamoCrudAlgorithms for OutOfTableAlgorithms {
    async fn recursive_delete(&self, _id: PkSk) -> Result<(), ServerError> {
        Ok(())
    }

    fn bundle_policy(&self, bundles: &mut DynamoBundlePolicy) {
        bundles
            .include::<TestRoot>()
            .out_of_table_pksk("out_of_table")
            .out_of_table_pksk("route");
    }
}

struct InvalidReferenceAlgorithms;

#[async_trait]
impl DynamoCrudAlgorithms for InvalidReferenceAlgorithms {
    async fn recursive_delete(&self, _id: PkSk) -> Result<(), ServerError> {
        Ok(())
    }

    fn bundle_policy(&self, bundles: &mut DynamoBundlePolicy) {
        bundles
            .include::<TestRoot>()
            .custom_references(DynamoBundleReferenceRule::custom(|_| {
                Ok(vec![DynamoBundleReferenceMatch::in_table(
                    BundleDataPath::field("missing"),
                    PkSk {
                        pk: "ROOT".into(),
                        sk: "TARGET#one".into(),
                    },
                )])
            }));
    }
}

struct CleanupAlgorithms {
    stale_ids: Arc<Mutex<Vec<PkSk>>>,
}

#[async_trait]
impl DynamoCrudAlgorithms for CleanupAlgorithms {
    async fn recursive_delete(&self, _id: PkSk) -> Result<(), ServerError> {
        Ok(())
    }

    async fn bundle_external_data_cleanup(
        &self,
        stale_rows: &[DynamoMap],
    ) -> Result<(), ServerError> {
        let mut stale_ids = self.stale_ids.lock().unwrap();
        for row in stale_rows {
            stale_ids.push(PkSk::from_map(row)?);
        }
        Ok(())
    }

    fn bundle_policy(&self, bundles: &mut DynamoBundlePolicy) {
        TestAlgorithms.bundle_policy(bundles);
    }
}

fn test_references() -> DynamoBundleReferenceRule {
    DynamoBundleReferenceRule::custom(|item| {
        let mut references = Vec::new();
        let local_path = BundleDataPath::field("local");
        if item.value_at(&local_path).and_then(Value::as_str).is_some() {
            references.push(DynamoBundleReferenceMatch::bundled_label(
                local_path,
                DynamoBundleReferenceEncoding::ForeignRef,
                "TARGET",
            ));
        }
        for field in ["kept", "missing", "external", "stale_ref"] {
            let path = BundleDataPath::field(field);
            if let Some(raw) = item.value_at(&path).and_then(Value::as_str) {
                references.push(DynamoBundleReferenceMatch::in_table(
                    path,
                    PkSk::from_string(raw)?,
                ));
            }
        }
        let compound_path = BundleDataPath::field("compound");
        let compound_reference_path = compound_path.clone().then_index(0);
        if let Some(raw) = item
            .value_at(&compound_reference_path)
            .and_then(Value::as_str)
        {
            references.push(DynamoBundleReferenceMatch::in_table_clearing(
                compound_reference_path,
                PkSk::from_string(raw)?,
                compound_path,
            ));
        }
        for field in ["out_of_table", "route", "out_of_table_ref"] {
            let path = BundleDataPath::field(field);
            if let Some(raw) = item.value_at(&path).and_then(Value::as_str) {
                references.push(DynamoBundleReferenceMatch::out_of_table(
                    path,
                    PkSk::from_string(raw)?,
                ));
            }
        }
        Ok(references)
    })
}

struct CountingAlgorithms(AtomicUsize);

#[async_trait]
impl DynamoCrudAlgorithms for CountingAlgorithms {
    async fn recursive_delete(&self, _id: PkSk) -> Result<(), ServerError> {
        Ok(())
    }

    fn bundle_policy(&self, bundles: &mut DynamoBundlePolicy) {
        self.0.fetch_add(1, Ordering::Relaxed);
        bundles.include_label("ROOTOBJ", BundleIdLogic::Uuid, &[]);
    }
}

struct DefaultAlgorithms;

#[async_trait]
impl DynamoCrudAlgorithms for DefaultAlgorithms {
    async fn recursive_delete(&self, _id: PkSk) -> Result<(), ServerError> {
        Ok(())
    }
}

struct SchemaValidationAlgorithms;

#[async_trait]
impl DynamoCrudAlgorithms for SchemaValidationAlgorithms {
    async fn recursive_delete(&self, _id: PkSk) -> Result<(), ServerError> {
        Ok(())
    }

    fn bundle_policy(&self, bundles: &mut DynamoBundlePolicy) {
        bundles.include::<TestRoot>();
        bundles.include::<TestOtherRoot>();
        bundles.include::<TestStrictChild>();
        bundles.include::<TestSharedChildOfRoot>();
        bundles.include::<TestSharedChildOfOtherRoot>();
        bundles.include::<TestBatch>();
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
async fn recursive_export_scopes_omissions_and_normalizes_ext_partitioning() {
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

    let bundle = export_from_config(
        &util(backend),
        &TestAlgorithms,
        PkSk {
            pk: "ROOT".into(),
            sk: root_sk.into(),
        },
        BundleNesting::Root,
        BundleIdLogic::Uuid,
    )
    .await
    .unwrap();

    assert_eq!(bundle.items.len(), 6);
    assert_eq!(
        bundle.source_root,
        PkSk {
            pk: "ROOT".into(),
            sk: root_sk.into(),
        }
    );
    assert_eq!(
        bundle.omitted_descendants,
        BTreeMap::from([
            ("CHILD".into(), BTreeSet::from(["GRAND".into()])),
            (
                "ROOTOBJ".into(),
                BTreeSet::from(["EXCLUDED".into(), "RECALC".into()]),
            ),
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
async fn recursive_export_omits_configured_subtrees_and_records_the_omission() {
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

    let bundle = export_from_config(
        &util(backend),
        &TestAlgorithms,
        PkSk {
            pk: "ROOT".into(),
            sk: root_sk.into(),
        },
        BundleNesting::Root,
        BundleIdLogic::Uuid,
    )
    .await
    .unwrap();

    assert_eq!(bundle.items.len(), 1);
    assert_eq!(
        bundle.omitted_descendants["ROOTOBJ"],
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

    let error = export_from_config(
        &util(backend),
        &TestAlgorithms,
        PkSk {
            pk: "ROOT".into(),
            sk: root_sk.into(),
        },
        BundleNesting::Root,
        BundleIdLogic::Uuid,
    )
    .await
    .unwrap_err();

    assert!(error.to_string().contains("DENIED"));
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn export_reports_required_internal_targets_outside_the_scope() {
    let root_sk = "ROOTOBJ#root";
    let target = PkSk {
        pk: root_sk.into(),
        sk: "ORDERED#outside".into(),
    };
    let mut backend = MockDynamoBackend::new();
    backend
        .expect_query()
        .times(2)
        .returning(move |_, _, _, values, _| {
            let pk = values.get(":pk").unwrap().as_s().unwrap();
            let rows = match pk.as_str() {
                "ROOT" => {
                    let mut root = row("ROOT", root_sk);
                    root.insert(
                        "required_target".into(),
                        AttributeValue::S(target.to_string()),
                    );
                    vec![root]
                }
                "ROOTOBJ#root" => vec![row(root_sk, "ORDERED#outside")],
                unexpected => panic!("unexpected partition query: {unexpected}"),
            };
            Ok(vec![QueryOutput::builder().set_items(Some(rows)).build()])
        });

    let error = export_from_config(
        &util(backend),
        &RequiredReferenceAlgorithms,
        PkSk {
            pk: "ROOT".into(),
            sk: root_sk.into(),
        },
        BundleNesting::Root,
        BundleIdLogic::Uuid,
    )
    .await
    .unwrap_err();
    let message = error.to_string();

    assert!(message.contains("portable export rooted at `ROOT|ROOTOBJ#root` was not closed"));
    assert!(message.contains("`ROOTOBJ` item `ROOTOBJ#root`"));
    assert!(message.contains("path `.required_target`"));
    assert!(message.contains("`ORDERED` target `ROOTOBJ#root|ORDERED#outside`"));
    assert!(message.contains("outside the exported scope"));
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn export_loads_bundle_configuration_once() {
    let algorithms = CountingAlgorithms(AtomicUsize::new(0));
    let mut backend = MockDynamoBackend::new();
    backend
        .expect_query()
        .times(2)
        .returning(|_, _, _, values, _| {
            let pk = values.get(":pk").unwrap().as_s().unwrap();
            let rows = match pk.as_str() {
                "ROOT" => vec![row("ROOT", "ROOTOBJ#root")],
                "ROOTOBJ#root" => vec![],
                unexpected => panic!("unexpected partition query: {unexpected}"),
            };
            Ok(vec![QueryOutput::builder().set_items(Some(rows)).build()])
        });

    export_from_config(
        &util(backend),
        &algorithms,
        PkSk {
            pk: "ROOT".into(),
            sk: "ROOTOBJ#root".into(),
        },
        BundleNesting::Root,
        BundleIdLogic::Uuid,
    )
    .await
    .unwrap();

    assert_eq!(algorithms.0.load(Ordering::Relaxed), 1);
}

#[test]
fn import_omissions_are_the_strict_union_and_require_present_owner_labels() {
    let root = id(0, "ROOTOBJ", "ROOTOBJ#root");
    let mut bundle = DynamoBundle {
        version: DynamoBundle::VERSION,
        source_root: PkSk {
            pk: "ROOT".into(),
            sk: root.original_sk.clone(),
        },
        root: root.clone(),
        omitted_descendants: BTreeMap::from([(
            "ROOTOBJ".into(),
            BTreeSet::from(["BUNDLE_ONLY".into()]),
        )]),
        items: vec![bundle_item(root, None, BundleNesting::Root, json!({}))],
    };
    let effective = validate_import_policy(
        &bundle,
        &configured_bundle_policy(&TestAlgorithms),
        BundleIdLogic::Uuid,
        None,
    )
    .unwrap();
    assert_eq!(
        effective["ROOTOBJ"],
        BTreeSet::from(["BUNDLE_ONLY".into(), "EXCLUDED".into(), "RECALC".into(),])
    );

    bundle.omitted_descendants =
        BTreeMap::from([("ABSENT_OWNER".into(), BTreeSet::from(["CHILD".into()]))]);
    assert!(validate_import_policy(
        &bundle,
        &configured_bundle_policy(&TestAlgorithms),
        BundleIdLogic::Uuid,
        None,
    )
    .is_err());
}

#[test]
fn import_rejects_unconfigured_bundle_items() {
    for label in ["UNCONFIGURED", "DENIED"] {
        let root = id(0, label, &format!("{label}#root"));
        let bundle = DynamoBundle {
            version: DynamoBundle::VERSION,
            source_root: PkSk {
                pk: "ROOT".into(),
                sk: root.original_sk.clone(),
            },
            root: root.clone(),
            omitted_descendants: BTreeMap::new(),
            items: vec![bundle_item(root, None, BundleNesting::Root, json!({}))],
        };

        assert!(validate_import_policy(
            &bundle,
            &configured_bundle_policy(&TestAlgorithms),
            BundleIdLogic::Uuid,
            None,
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
        source_root: PkSk {
            pk: "ROOT".into(),
            sk: root.original_sk.clone(),
        },
        root: root.clone(),
        omitted_descendants: BTreeMap::new(),
        items: vec![
            bundle_item(root, None, BundleNesting::Root, json!({})),
            child_item,
        ],
    };

    assert!(validate_import_policy(
        &bundle,
        &configured_bundle_policy(&TestAlgorithms),
        BundleIdLogic::Uuid,
        None,
    )
    .is_err());
}

#[test]
fn import_rejects_root_policy_that_disagrees_with_crud_type() {
    let root = id(0, "ROOTOBJ", "ROOTOBJ#root");
    let bundle = DynamoBundle {
        version: DynamoBundle::VERSION,
        source_root: PkSk {
            pk: "ROOT".into(),
            sk: root.original_sk.clone(),
        },
        root: root.clone(),
        omitted_descendants: BTreeMap::new(),
        items: vec![bundle_item(root, None, BundleNesting::Root, json!({}))],
    };

    assert!(validate_import_policy(
        &bundle,
        &configured_bundle_policy(&TestAlgorithms),
        BundleIdLogic::Timestamp,
        None,
    )
    .is_err());
}

#[test]
fn import_validates_local_topology_and_data_shape() {
    let root = id(0, "ROOTOBJ", "ROOTOBJ#root");
    let child = id(1, "STRICTCHILD", "STRICTCHILD#child");
    let mut bundle = DynamoBundle {
        version: DynamoBundle::VERSION,
        source_root: PkSk {
            pk: "ROOT".into(),
            sk: root.original_sk.clone(),
        },
        root: root.clone(),
        omitted_descendants: BTreeMap::new(),
        items: vec![
            bundle_item(root.clone(), None, BundleNesting::Root, json!({})),
            bundle_item(
                child.clone(),
                Some(root.clone()),
                BundleNesting::TopLevel,
                json!({"required_name": "valid", "sort": 4.0}),
            ),
        ],
    };
    let policy = configured_bundle_policy(&SchemaValidationAlgorithms);

    validate_import_policy(&bundle, &policy, BundleIdLogic::Uuid, None).unwrap();

    bundle.items[1].nesting = BundleNesting::Inline;
    let topology_error =
        validate_import_policy(&bundle, &policy, BundleIdLogic::Uuid, None).unwrap_err();
    assert!(topology_error.to_string().contains("local schema"));

    bundle.items[1].nesting = BundleNesting::TopLevel;
    bundle.items[1].data = json!({"required_name": 4});
    let data_error =
        validate_import_policy(&bundle, &policy, BundleIdLogic::Uuid, None).unwrap_err();
    assert!(data_error.to_string().contains("data did not match"));

    bundle.items[1].data = json!({"required_name": "valid", "sort": 4.0});
    bundle.items[0] = bundle_item(
        id(0, "OTHERROOT", "OTHERROOT#root"),
        None,
        BundleNesting::Root,
        json!({}),
    );
    bundle.root = bundle.items[0].id.clone();
    bundle.items[1].parent = Some(bundle.root.clone());
    let parent_error =
        validate_import_policy(&bundle, &policy, BundleIdLogic::Uuid, None).unwrap_err();
    assert!(parent_error
        .to_string()
        .contains("bundled parent label `OTHERROOT`"));
}

#[test]
fn import_validates_batch_optimized_payload_members() {
    let root = id(0, "BATCH", "BATCH#-");
    let mut root_item = bundle_item(
        root.clone(),
        None,
        BundleNesting::TopLevel,
        json!({"..": [{}]}),
    );
    root_item.id_logic = BundleIdLogic::BatchOptimized;
    let mut bundle = DynamoBundle {
        version: DynamoBundle::VERSION,
        source_root: PkSk {
            pk: "ROOTOBJ#parent".into(),
            sk: root.original_sk.clone(),
        },
        root,
        omitted_descendants: BTreeMap::new(),
        items: vec![root_item],
    };
    let parent = PkSk {
        pk: "ROOT".into(),
        sk: "ROOTOBJ#parent".into(),
    };
    let policy = configured_bundle_policy(&SchemaValidationAlgorithms);

    validate_import_policy(
        &bundle,
        &policy,
        BundleIdLogic::BatchOptimized,
        Some(&parent),
    )
    .unwrap();

    bundle.items[0].data = json!({});
    let error = validate_import_policy(
        &bundle,
        &policy,
        BundleIdLogic::BatchOptimized,
        Some(&parent),
    )
    .unwrap_err();
    assert!(error.to_string().contains("batch-optimized data"));
}

#[test]
fn import_accepts_registered_schema_variants_sharing_a_label() {
    let root = id(0, "OTHERROOT", "OTHERROOT#root");
    let child = id(1, "SHAREDCHILD", "SHAREDCHILD#child");
    let bundle = DynamoBundle {
        version: DynamoBundle::VERSION,
        source_root: PkSk {
            pk: "ROOT".into(),
            sk: root.original_sk.clone(),
        },
        root: root.clone(),
        omitted_descendants: BTreeMap::new(),
        items: vec![
            bundle_item(root.clone(), None, BundleNesting::Root, json!({})),
            bundle_item(
                child,
                Some(root),
                BundleNesting::TopLevel,
                json!({"required_name": "valid"}),
            ),
        ],
    };

    validate_import_policy(
        &bundle,
        &configured_bundle_policy(&SchemaValidationAlgorithms),
        BundleIdLogic::Uuid,
        None,
    )
    .unwrap();
}

#[test]
fn bundling_is_denied_by_default() {
    let bundles = configured_bundle_policy(&DefaultAlgorithms);
    assert!(!bundles.contains_label("UNREGISTERED"));
    assert!(bundles.require("UNREGISTERED").is_err());
}

#[test]
fn bundle_config_normalizes_top_level_renames_before_selecting_references() {
    let mut bundles = DynamoBundlePolicy::new();
    bundles
        .include::<TestRenamedRoot>()
        .in_table_pksk("canonical_ref")
        .out_of_table_pksk("archive_ref");
    let object = bundles.object::<TestRenamedRoot>().unwrap();
    let mut item = bundle_item(
        id(0, "RENAMEDROOT", "RENAMEDROOT#root"),
        None,
        BundleNesting::Root,
        json!({
            "legacy_ref": "ROOT|TARGET#one",
            "archive_ref": "ROOT|TARGET#archive"
        }),
    );

    object.normalize_data(&mut item.data);

    assert_eq!(
        item.data,
        json!({
            "canonical_ref": "ROOT|TARGET#one",
            "archive_ref": "ROOT|TARGET#archive"
        })
    );
    let in_table = (object.reference_rules()[0].selector)(&item).unwrap();
    assert_eq!(in_table.len(), 1);
    assert_eq!(in_table[0].path, BundleDataPath::dotted("canonical_ref"));
    assert!(matches!(
        &in_table[0].target,
        DynamoBundleReferenceMatchTarget::InTable { .. }
    ));
    let out_of_table = (object.reference_rules()[1].selector)(&item).unwrap();
    assert_eq!(out_of_table.len(), 1);
    assert_eq!(out_of_table[0].path, BundleDataPath::dotted("archive_ref"));
    assert!(matches!(
        &out_of_table[0].target,
        DynamoBundleReferenceMatchTarget::OutOfTable { .. }
    ));
}

#[test]
fn bundle_policy_normalizes_batch_members_and_prefers_canonical_fields() {
    let root = id(0, "RENAMEDROOT", "RENAMEDROOT#root");
    let batch = id(1, "RENAMEDBATCH", "RENAMEDBATCH#0");
    let mut batch_item = bundle_item(
        batch,
        Some(root.clone()),
        BundleNesting::TopLevel,
        json!({
            "..": [
                {"legacy_name": "legacy"},
                {"legacy_name": "discarded", "canonical_name": "canonical"}
            ]
        }),
    );
    batch_item.id_logic = BundleIdLogic::BatchOptimized;
    let mut bundle = DynamoBundle {
        version: DynamoBundle::VERSION,
        source_root: PkSk {
            pk: "ROOT".into(),
            sk: root.original_sk.clone(),
        },
        root: root.clone(),
        omitted_descendants: BTreeMap::new(),
        items: vec![
            bundle_item(
                root,
                None,
                BundleNesting::Root,
                json!({
                    "legacy_ref": "ROOT|TARGET#legacy",
                    "canonical_ref": "ROOT|TARGET#canonical"
                }),
            ),
            batch_item,
        ],
    };
    let mut policy = DynamoBundlePolicy::new();
    policy.include::<TestRenamedRoot>();
    policy.include::<TestRenamedBatch>();

    policy.normalize_bundle_data(&mut bundle).unwrap();

    assert_eq!(
        bundle.items[0].data,
        json!({"canonical_ref": "ROOT|TARGET#canonical"})
    );
    assert_eq!(
        bundle.items[1].data,
        json!({
            "..": [
                {"canonical_name": "legacy"},
                {"canonical_name": "canonical"}
            ]
        })
    );
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn import_normalizes_legacy_reference_fields_before_remapping_and_writing() {
    let root = id(0, "RENAMEDROOT", "RENAMEDROOT#source");
    let target = id(1, "ORDERED", "ORDERED#source");
    let original_target = PkSk {
        pk: root.original_sk.clone(),
        sk: target.original_sk.clone(),
    };
    let bundle = DynamoBundle {
        version: DynamoBundle::VERSION,
        source_root: PkSk {
            pk: "ROOT".into(),
            sk: root.original_sk.clone(),
        },
        root: root.clone(),
        omitted_descendants: BTreeMap::new(),
        items: vec![
            bundle_item(
                root.clone(),
                None,
                BundleNesting::Root,
                json!({"legacy_ref": original_target.to_string()}),
            ),
            bundle_item(
                target,
                Some(root),
                BundleNesting::TopLevel,
                json!({"name": "target"}),
            ),
        ],
    };
    let mut backend = MockDynamoBackend::new();
    backend
        .expect_batch_get_item()
        .times(1)
        .returning(|table, _, _| {
            Ok(BatchGetItemOutput::builder()
                .set_responses(Some(HashMap::from([(table, Vec::new())])))
                .build())
        });
    backend
        .expect_batch_put_item()
        .times(1)
        .returning(move |_, items| {
            let root = items
                .iter()
                .find(|item| item["pk"].as_s().is_ok_and(|pk| pk == "ROOT"))
                .unwrap();
            let target = items
                .iter()
                .find(|item| item["pk"].as_s().is_ok_and(|pk| pk != "ROOT"))
                .unwrap();
            let remapped = PkSk::from_string(root["canonical_ref"].as_s().unwrap()).unwrap();

            assert_eq!(remapped, PkSk::from_map(target).unwrap());
            assert_ne!(remapped, original_target);
            assert!(!root.contains_key("legacy_ref"));
            Ok(BatchWriteItemOutput::builder().build())
        });

    let result = import_bundle::<TestRenamedRoot>(
        &util(backend),
        &RenamedReferenceAlgorithms,
        None,
        bundle,
        ImportMode::New { position: None },
        None,
    )
    .await
    .unwrap();

    assert!(result.created_new);
    assert!(result.warnings.is_empty());
}

#[test]
fn bundled_pksk_each_selects_only_string_array_members() {
    let mut bundles = DynamoBundlePolicy::new();
    bundles
        .include::<TestRoot>()
        .bundled_pksk_each::<TestBatch>("targets");
    let object = bundles.object::<TestRoot>().unwrap();
    let item = bundle_item(
        id(0, "ROOTOBJ", "ROOTOBJ#root"),
        None,
        BundleNesting::Root,
        json!({"targets": ["ROOTOBJ#root|BATCH#0", null, 4]}),
    );

    let matches = (object.reference_rules()[0].selector)(&item).unwrap();

    assert_eq!(matches.len(), 1);
    assert_eq!(
        matches[0].path,
        BundleDataPath::field("targets").then_index(0)
    );
    assert!(matches!(
        &matches[0].target,
        DynamoBundleReferenceMatchTarget::Bundled {
            target_label,
            encoding: DynamoBundleReferenceEncoding::PkSk,
        } if target_label == TestBatch::id_label()
    ));
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

#[test]
fn indexed_singleton_reference_values_allow_at_signs_in_keys() {
    assert_eq!(
        crate::schema::identifiers::RawIdPath::new("PARENT#old@SETTINGS[user@example.com]")
            .foreign_ref_value(),
        "user@example.com"
    );
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
        source_root: PkSk {
            pk: "ROOT".into(),
            sk: root.original_sk.clone(),
        },
        root: root.clone(),
        omitted_descendants: BTreeMap::new(),
        items: vec![
            bundle_item(
                inline.clone(),
                Some(top.clone()),
                BundleNesting::Inline,
                json!({}),
            ),
            bundle_item(
                top.clone(),
                Some(root.clone()),
                BundleNesting::TopLevel,
                json!({}),
            ),
            bundle_item(root.clone(), None, BundleNesting::Root, json!({})),
        ],
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
fn duplicate_mapping_preserves_batch_ids_and_regenerates_ordered_timestamp_uuids() {
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
        source_root: PkSk {
            pk: "ROOT".into(),
            sk: root.original_sk.clone(),
        },
        root: root.clone(),
        omitted_descendants: BTreeMap::new(),
        items: vec![
            bundle_item(root.clone(), None, BundleNesting::Root, json!({})),
            first_timestamp_item,
            second_timestamp_item,
            batch_item,
        ],
    };

    let mapped = build_id_map(&bundle, None, true, BundleIdLogic::Uuid).unwrap();
    let first_value = mapped[&first_timestamp]
        .sk
        .rsplit_once('#')
        .unwrap()
        .1
        .to_string();
    let second_value = mapped[&second_timestamp]
        .sk
        .rsplit_once('#')
        .unwrap()
        .1
        .to_string();

    assert_eq!(first_value.len(), 22);
    assert_eq!(second_value.len(), 22);
    assert!(first_value < second_value);
    assert_eq!(mapped[&batch].sk, "BATCH#0");
    assert_eq!(mapped[&batch].pk, mapped[&root].sk);
}

#[tokio::test]
async fn merge_and_replace_reject_reparenting_before_database_access() {
    let source_parent = PkSk {
        pk: "ROOT".into(),
        sk: "ROOTOBJ#source-parent".into(),
    };
    let destination_parent = PkSk {
        pk: "ROOT".into(),
        sk: "ROOTOBJ#destination-parent".into(),
    };
    let root = id(0, "ORDERED", "ORDERED#same");
    let bundle = DynamoBundle {
        version: DynamoBundle::VERSION,
        source_root: PkSk {
            pk: source_parent.sk,
            sk: root.original_sk.clone(),
        },
        root: root.clone(),
        omitted_descendants: BTreeMap::new(),
        items: vec![bundle_item(
            root,
            None,
            BundleNesting::TopLevel,
            json!({"name": "item"}),
        )],
    };

    for mode in [ImportMode::Merge, ImportMode::Replace] {
        let error = import_bundle::<TestOrdered>(
            &util(MockDynamoBackend::new()),
            &TestAlgorithms,
            Some(&destination_parent),
            bundle.clone(),
            mode,
            None,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("reparenting is not supported"));
        assert!(error.to_string().contains("use New"));
    }
}

#[tokio::test]
async fn import_rejects_singleton_destination_parents_before_database_access() {
    let root = id(0, "ORDERED", "ORDERED#source");
    let bundle = DynamoBundle {
        version: DynamoBundle::VERSION,
        source_root: PkSk {
            pk: "ROOTOBJ#source-parent".into(),
            sk: root.original_sk.clone(),
        },
        root: root.clone(),
        omitted_descendants: BTreeMap::new(),
        items: vec![bundle_item(
            root,
            None,
            BundleNesting::TopLevel,
            json!({"name": "item"}),
        )],
    };

    let error = import_bundle::<TestOrdered>(
        &util(MockDynamoBackend::new()),
        &TestAlgorithms,
        Some(&PkSk {
            pk: "ROOT".into(),
            sk: "@SETTINGS".into(),
        }),
        bundle,
        ImportMode::New { position: None },
        None,
    )
    .await
    .unwrap_err();

    assert!(error
        .to_string()
        .contains("singleton objects cannot have children"));
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn ordered_new_gets_a_fresh_id_and_is_placed_last() {
    let parent = PkSk {
        pk: "ROOT".into(),
        sk: "ROOTOBJ#parent".into(),
    };
    let root = id(0, "ORDERED", "ORDERED#source");
    let bundle = DynamoBundle {
        version: DynamoBundle::VERSION,
        source_root: PkSk {
            pk: parent.sk.clone(),
            sk: root.original_sk.clone(),
        },
        root: root.clone(),
        omitted_descendants: BTreeMap::new(),
        items: vec![bundle_item(
            root,
            None,
            BundleNesting::TopLevel,
            json!({"name": "duplicate", "sort": 3.0}),
        )],
    };
    let mut backend = MockDynamoBackend::new();
    backend
        .expect_batch_get_item()
        .times(1)
        .returning(|table, _, _| {
            Ok(BatchGetItemOutput::builder()
                .set_responses(Some(HashMap::from([(table, vec![])])))
                .build())
        });
    backend.expect_query().times(1).returning(|_, _, _, _, _| {
        let mut existing = row("ROOTOBJ#parent", "ORDERED#existing");
        existing.insert("sort".into(), AttributeValue::N("7".into()));
        Ok(vec![QueryOutput::builder()
            .set_items(Some(vec![existing]))
            .build()])
    });
    backend
        .expect_batch_put_item()
        .times(1)
        .returning(|_, items| {
            assert_eq!(items.len(), 1);
            assert_ne!(items[0]["sk"], AttributeValue::S("ORDERED#source".into()));
            assert_eq!(items[0]["sort"], AttributeValue::N("8.0".into()));
            Ok(BatchWriteItemOutput::builder().build())
        });

    let result = import_bundle::<TestOrdered>(
        &util(backend),
        &TestAlgorithms,
        Some(&parent),
        bundle,
        ImportMode::New {
            position: Some(DynamoInsertPosition::Last),
        },
        None,
    )
    .await
    .unwrap();

    assert!(result.created_new);
    assert_ne!(result.root_id.sk, "ORDERED#source");
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn new_without_an_insertion_position_clears_the_source_sort() {
    let parent = PkSk {
        pk: "ROOT".into(),
        sk: "ROOTOBJ#parent".into(),
    };
    let root = id(0, "ORDERED", "ORDERED#source");
    let bundle = DynamoBundle {
        version: DynamoBundle::VERSION,
        source_root: PkSk {
            pk: parent.sk.clone(),
            sk: root.original_sk.clone(),
        },
        root: root.clone(),
        omitted_descendants: BTreeMap::new(),
        items: vec![bundle_item(
            root,
            None,
            BundleNesting::TopLevel,
            json!({"name": "new", "sort": 3.0}),
        )],
    };
    let mut backend = MockDynamoBackend::new();
    backend
        .expect_batch_get_item()
        .times(1)
        .returning(|table, _, _| {
            Ok(BatchGetItemOutput::builder()
                .set_responses(Some(HashMap::from([(table, vec![])])))
                .build())
        });
    backend
        .expect_batch_put_item()
        .times(1)
        .returning(|_, items| {
            assert_eq!(items.len(), 1);
            assert!(!items[0].contains_key("sort"));
            Ok(BatchWriteItemOutput::builder().build())
        });

    import_bundle::<TestOrdered>(
        &util(backend),
        &TestAlgorithms,
        Some(&parent),
        bundle,
        ImportMode::New { position: None },
        None,
    )
    .await
    .unwrap();
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn new_remaps_bundled_refs_and_clears_zeroed_external_refs() {
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
    let out_of_table = PkSk {
        pk: "ROOT".into(),
        sk: "ARCHIVE#outside".into(),
    };
    let bundle = DynamoBundle {
        version: DynamoBundle::VERSION,
        source_root: PkSk {
            pk: "ROOT".into(),
            sk: root.original_sk.clone(),
        },
        root: root.clone(),
        omitted_descendants: BTreeMap::new(),
        items: vec![
            bundle_item(
                root.clone(),
                None,
                BundleNesting::Root,
                json!({
                    "local": "old",
                    "kept": existing_external.to_string(),
                    "missing": missing_external.to_string(),
                    "compound": [missing_external.to_string(), "kept member"],
                    "out_of_table": out_of_table.to_string()
                }),
            ),
            bundle_item(
                target.clone(),
                Some(root.clone()),
                BundleNesting::TopLevel,
                json!({}),
            ),
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
            assert!(!root.contains_key("out_of_table"));
            Ok(BatchWriteItemOutput::builder().build())
        });

    let result = import_bundle::<TestRoot>(
        &util(backend),
        &TestAlgorithms,
        None,
        bundle.clone(),
        ImportMode::New { position: None },
        None,
    )
    .await
    .unwrap();
    // `import_bundle` owns and mutates the bundle; this assertion keeps the
    // original fixture useful and verifies the result contract instead.
    assert_eq!(bundle.items.len(), result.written_objects);
    assert_eq!(
        result.warnings,
        vec![
            DynamoImportWarning::ZeroedInTableReference,
            DynamoImportWarning::ZeroedInTableReference,
            DynamoImportWarning::ZeroedOutOfTableReference,
        ]
    );
    assert!(result.created_new);
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn new_preserves_valid_out_of_table_references() {
    let root = id(0, "ROOTOBJ", "ROOTOBJ#root");
    let out_of_table = PkSk {
        pk: "ROOT".into(),
        sk: "ROUTE#outside".into(),
    };
    let bundle = DynamoBundle {
        version: DynamoBundle::VERSION,
        source_root: PkSk {
            pk: "ROOT".into(),
            sk: root.original_sk.clone(),
        },
        root: root.clone(),
        omitted_descendants: BTreeMap::new(),
        items: vec![bundle_item(
            root.clone(),
            None,
            BundleNesting::Root,
            json!({"route": out_of_table.to_string()}),
        )],
    };
    let mut backend = MockDynamoBackend::new();
    backend
        .expect_batch_get_item()
        .times(1)
        .returning(|table, _, _| {
            Ok(BatchGetItemOutput::builder()
                .set_responses(Some(HashMap::from([(table, vec![])])))
                .build())
        });
    let expected = out_of_table.clone();
    backend
        .expect_batch_put_item()
        .times(1)
        .returning(move |_, items| {
            assert_eq!(items[0]["route"], AttributeValue::S(expected.to_string()));
            Ok(BatchWriteItemOutput::builder().build())
        });

    let result = import_bundle::<TestRoot>(
        &util(backend),
        &OutOfTableAlgorithms,
        None,
        bundle,
        ImportMode::New { position: None },
        Some(&HashSet::from([out_of_table])),
    )
    .await
    .unwrap();

    assert!(result.warnings.is_empty());
    assert!(result.created_new);
}

#[test]
fn replace_preserves_local_out_of_table_associations_and_clears_without_local_state() {
    let root = id(0, "ROOTOBJ", "ROOTOBJ#root");
    let root_id = PkSk {
        pk: "ROOT".into(),
        sk: root.original_sk.clone(),
    };
    let local = PkSk {
        pk: "ROOT".into(),
        sk: "ARCHIVE#local".into(),
    };
    let incoming = PkSk {
        pk: "ROOT".into(),
        sk: "ARCHIVE#incoming".into(),
    };
    let policy = configured_bundle_policy(&OutOfTableAlgorithms);
    let old = DynamoBundle {
        version: DynamoBundle::VERSION,
        source_root: root_id.clone(),
        root: root.clone(),
        omitted_descendants: BTreeMap::new(),
        items: vec![bundle_item(
            root.clone(),
            None,
            BundleNesting::Root,
            json!({"out_of_table": local.to_string()}),
        )],
    };
    let mut bundle = DynamoBundle {
        version: DynamoBundle::VERSION,
        source_root: root_id.clone(),
        root: root.clone(),
        omitted_descendants: BTreeMap::new(),
        items: vec![bundle_item(
            root.clone(),
            None,
            BundleNesting::Root,
            json!({}),
        )],
    };
    let destination_ids = HashMap::from([(root.clone(), root_id)]);

    let warnings = reconcile_replace_out_of_table_references(
        &mut bundle,
        &[],
        &destination_ids,
        Some(&old),
        &policy,
        None,
    )
    .unwrap();

    assert!(warnings.is_empty());
    assert_eq!(
        bundle.items[0].data["out_of_table"],
        Value::String(local.to_string())
    );

    bundle.items[0].data = json!({"out_of_table": incoming.to_string()});
    let references = vec![BundleReference {
        source: root,
        path: BundleDataPath::field("out_of_table"),
        target: BundleReferenceTarget::OutOfTable {
            lookup_id: incoming.clone(),
            clear_path: BundleDataPath::field("out_of_table"),
        },
    }];
    let warnings = reconcile_replace_out_of_table_references(
        &mut bundle,
        &references,
        &destination_ids,
        None,
        &policy,
        None,
    )
    .unwrap();

    assert_eq!(
        warnings,
        vec![DynamoImportWarning::ZeroedOutOfTableReference]
    );
    assert!(bundle.items[0].data["out_of_table"].is_null());

    bundle.items[0].data = json!({"out_of_table": incoming.to_string()});
    let references = vec![BundleReference {
        source: bundle.root.clone(),
        path: BundleDataPath::field("out_of_table"),
        target: BundleReferenceTarget::OutOfTable {
            lookup_id: incoming.clone(),
            clear_path: BundleDataPath::field("out_of_table"),
        },
    }];
    let warnings = reconcile_replace_out_of_table_references(
        &mut bundle,
        &references,
        &destination_ids,
        Some(&old),
        &policy,
        Some(&HashSet::from([incoming.clone()])),
    )
    .unwrap();

    assert!(warnings.is_empty());
    assert_eq!(
        bundle.items[0].data["out_of_table"],
        Value::String(incoming.to_string())
    );
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
        source_root: PkSk {
            pk: "ROOT".into(),
            sk: root.original_sk.clone(),
        },
        root: root.clone(),
        omitted_descendants: BTreeMap::new(),
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
        ImportMode::Merge,
        None,
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
        source_root: PkSk {
            pk: "ROOT".into(),
            sk: root.original_sk.clone(),
        },
        root: root.clone(),
        omitted_descendants: BTreeMap::new(),
        items: vec![bundle_item(
            root,
            None,
            BundleNesting::Root,
            json!({"value": "ordinary"}),
        )],
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
        ImportMode::Merge,
        None,
    )
    .await
    .unwrap();
    assert_eq!(result.root_id.sk, "ROOTOBJ#root");
    assert_eq!(result.written_objects, 1);
    assert!(!result.created_new);
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn replace_deletes_omitted_descendants_when_their_managed_parent_is_removed() {
    let stale_ids_seen = Arc::new(Mutex::new(Vec::new()));
    let algorithms = CleanupAlgorithms {
        stale_ids: stale_ids_seen.clone(),
    };
    let root_id = id(0, "ROOTOBJ", "ROOTOBJ#root");
    let stale_id = PkSk {
        pk: "ROOTOBJ#root".into(),
        sk: "STEP#old".into(),
    };
    let out_of_table_id = PkSk {
        pk: "ROOT".into(),
        sk: "ARCHIVE#pipeline".into(),
    };
    let local_out_of_table_id = PkSk {
        pk: "ROOT".into(),
        sk: "ARCHIVE#local".into(),
    };
    let bundle = DynamoBundle {
        version: DynamoBundle::VERSION,
        source_root: PkSk {
            pk: "ROOT".into(),
            sk: root_id.original_sk.clone(),
        },
        root: root_id.clone(),
        omitted_descendants: BTreeMap::from([(
            "ROOTOBJ".into(),
            BTreeSet::from(["RECALC".into()]),
        )]),
        items: vec![bundle_item(
            root_id.clone(),
            None,
            BundleNesting::Root,
            json!({
                "value": "new",
                "stale_ref": stale_id.to_string(),
                "out_of_table_ref": out_of_table_id.to_string()
            }),
        )],
    };

    let mut backend = MockDynamoBackend::new();
    backend
        .expect_batch_get_item()
        .times(2)
        .returning(|table, _, projection| {
            let rows = if projection.is_none() {
                vec![row("ROOT", "ROOTOBJ#root")]
            } else {
                vec![row("ROOTOBJ#root", "STEP#old")]
            };
            Ok(BatchGetItemOutput::builder()
                .set_responses(Some(HashMap::from([(table, rows)])))
                .build())
        });
    backend.expect_query().times(6).returning({
        let local_out_of_table_id = local_out_of_table_id.clone();
        move |_, _, _, values, _| {
            let pk = values.get(":pk").unwrap().as_s().unwrap();
            let rows = match pk.as_str() {
                "ROOT" => {
                    let mut root = row("ROOT", "ROOTOBJ#root");
                    root.insert(
                        "out_of_table_ref".into(),
                        AttributeValue::S(local_out_of_table_id.to_string()),
                    );
                    vec![root]
                }
                "ROOTOBJ#root" => vec![row("ROOTOBJ#root", "STEP#old")],
                "STEP#old" => vec![row("STEP#old", "RECALC#history")],
                "RECALC#history" => vec![],
                unexpected => panic!("unexpected partition query: {unexpected}"),
            };
            Ok(vec![QueryOutput::builder().set_items(Some(rows)).build()])
        }
    });
    let expected_local_out_of_table_id = local_out_of_table_id.clone();
    backend
        .expect_batch_put_item()
        .times(1)
        .returning(move |_, items| {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0]["value"], AttributeValue::S("new".into()));
            assert!(!items[0].contains_key("stale_ref"));
            assert_eq!(
                items[0]["out_of_table_ref"],
                AttributeValue::S(expected_local_out_of_table_id.to_string())
            );
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
        &algorithms,
        None,
        bundle,
        ImportMode::Replace,
        None,
    )
    .await
    .unwrap();

    assert_eq!(result.deleted_subtree_roots, 1);
    assert!(!result.created_new);
    assert_eq!(
        result.warnings,
        vec![DynamoImportWarning::ZeroedInTableReference]
    );
    let stale_ids_seen = stale_ids_seen.lock().unwrap();
    assert!(stale_ids_seen.contains(&PkSk {
        pk: "ROOTOBJ#root".into(),
        sk: "STEP#old".into(),
    }));
    assert!(stale_ids_seen.contains(&PkSk {
        pk: "STEP#old".into(),
        sk: "RECALC#history".into(),
    }));
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn new_rejects_a_fixed_singleton_root_at_its_source_placement() {
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
        source_root: PkSk {
            pk: "ROOT".into(),
            sk: root.original_sk.clone(),
        },
        root: root.clone(),
        omitted_descendants: BTreeMap::new(),
        items: vec![root_item],
    };
    assert!(import_bundle::<TestSingleton>(
        &util(MockDynamoBackend::new()),
        &TestAlgorithms,
        None,
        bundle,
        ImportMode::New { position: None },
        None,
    )
    .await
    .is_err());
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn new_rejects_a_fixed_batch_root_at_its_source_placement() {
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
        source_root: PkSk {
            pk: parent.sk.clone(),
            sk: root.original_sk.clone(),
        },
        root,
        omitted_descendants: BTreeMap::new(),
        items: vec![root_item],
    };
    let error = import_bundle::<TestBatch>(
        &util(MockDynamoBackend::new()),
        &TestAlgorithms,
        Some(&parent),
        bundle,
        ImportMode::New { position: None },
        None,
    )
    .await
    .unwrap_err();

    assert!(error.to_string().contains("fixed identity"));
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn new_allows_a_fixed_batch_root_below_a_different_parent() {
    let source_parent = PkSk {
        pk: "ROOT".into(),
        sk: "ROOTOBJ#source-parent".into(),
    };
    let destination_parent = PkSk {
        pk: "ROOT".into(),
        sk: "ROOTOBJ#destination-parent".into(),
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
        source_root: PkSk {
            pk: source_parent.sk,
            sk: root.original_sk.clone(),
        },
        root,
        omitted_descendants: BTreeMap::new(),
        items: vec![root_item],
    };
    let mut backend = MockDynamoBackend::new();
    backend
        .expect_batch_get_item()
        .times(1)
        .returning(|table, _, _| {
            Ok(BatchGetItemOutput::builder()
                .set_responses(Some(HashMap::from([(table, vec![])])))
                .build())
        });
    backend
        .expect_batch_put_item()
        .times(1)
        .returning(move |_, items| {
            assert_eq!(items.len(), 1);
            assert_eq!(
                PkSk::from_map(&items[0]).unwrap(),
                PkSk {
                    pk: destination_parent.sk.clone(),
                    sk: "BATCH#0".into(),
                }
            );
            Ok(BatchWriteItemOutput::builder().build())
        });

    let result = import_bundle::<TestBatch>(
        &util(backend),
        &TestAlgorithms,
        Some(&PkSk {
            pk: "ROOT".into(),
            sk: "ROOTOBJ#destination-parent".into(),
        }),
        bundle,
        ImportMode::New { position: None },
        None,
    )
    .await
    .unwrap();

    assert!(result.created_new);
    assert_eq!(result.root_id.sk, "BATCH#0");
}

#[tokio::test]
async fn import_rejects_reference_paths_that_are_not_present() {
    let root = id(0, "ROOTOBJ", "ROOTOBJ#root");
    let bundle = DynamoBundle {
        version: DynamoBundle::VERSION,
        source_root: PkSk {
            pk: "ROOT".into(),
            sk: root.original_sk.clone(),
        },
        root: root.clone(),
        omitted_descendants: BTreeMap::new(),
        items: vec![bundle_item(
            root,
            None,
            BundleNesting::Root,
            json!({"present": "value"}),
        )],
    };

    assert!(import_bundle::<TestRoot>(
        &util(MockDynamoBackend::new()),
        &InvalidReferenceAlgorithms,
        None,
        bundle,
        ImportMode::Merge,
        None,
    )
    .await
    .is_err());
}
