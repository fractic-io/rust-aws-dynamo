use std::borrow::Cow;

use serde::{Deserialize, Serialize};

use super::*;
use crate::{
    dynamo_object,
    schema::{IdLogic, NestingLogic, PkSk},
};

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct RootUuidData {}
dynamo_object!(
    RootUuid,
    RootUuidData,
    "ROOTOBJ",
    IdLogic::Uuid,
    NestingLogic::Root
);

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct RootTimestampData {}
dynamo_object!(
    RootTimestamp,
    RootTimestampData,
    "EVENT",
    IdLogic::Timestamp,
    NestingLogic::Root
);

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct TopLevelChildData {}
dynamo_object!(
    TopLevelChild,
    TopLevelChildData,
    "TOP",
    IdLogic::Uuid,
    NestingLogic::TopLevelChildOfAny
);

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct InlineChildData {}
dynamo_object!(
    InlineChild,
    InlineChildData,
    "INLINE",
    IdLogic::Uuid,
    NestingLogic::InlineChildOfAny
);

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct StrictChildData {}
dynamo_object!(
    StrictChild,
    StrictChildData,
    "STRICT",
    IdLogic::Uuid,
    NestingLogic::TopLevelChildOf("PARENT")
);

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct SingletonData {}
dynamo_object!(
    Singleton,
    SingletonData,
    "SETTINGS",
    IdLogic::Singleton,
    NestingLogic::Root
);

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct IndexedSingletonData {
    key: String,
}
dynamo_object!(
    IndexedSingleton,
    IndexedSingletonData,
    "LOOKUP",
    IdLogic::IndexedSingleton(Box::new(|data: &IndexedSingletonData| {
        Cow::Borrowed(&data.key)
    })),
    NestingLogic::Root
);

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct InvalidLabelData {}
dynamo_object!(
    InvalidLabel,
    InvalidLabelData,
    "BAD#LABEL",
    IdLogic::Uuid,
    NestingLogic::Root
);

#[test]
fn generates_each_terminal_segment_shape() {
    let uuid = generate_id::<RootUuid>(&RootUuidData {}, PkSk::root()).unwrap();
    assert_eq!(uuid.pk, ROOT_KEY);
    assert!(uuid.sk.starts_with("ROOTOBJ#"));
    assert_eq!(uuid.sk.len(), "ROOTOBJ#".len() + 22);

    let timestamp = generate_id::<RootTimestamp>(&RootTimestampData {}, PkSk::root()).unwrap();
    assert!(timestamp.sk.starts_with("EVENT#"));
    assert_eq!(timestamp.sk.len(), "EVENT#".len() + 22);

    let singleton = generate_id::<Singleton>(&SingletonData {}, PkSk::root()).unwrap();
    assert_eq!(singleton.sk, "@SETTINGS");

    let indexed = generate_id::<IndexedSingleton>(
        &IndexedSingletonData {
            key: "user@example.com".into(),
        },
        PkSk::root(),
    )
    .unwrap();
    assert_eq!(indexed.sk, "@LOOKUP[user@example.com]");
}

#[test]
fn generation_and_direct_placement_use_the_child_nesting_logic() {
    let parent = PkSk {
        pk: "PARENT_PARTITION".into(),
        sk: "PARENT#1".into(),
    };

    let top = generate_id::<TopLevelChild>(&TopLevelChildData {}, &parent).unwrap();
    assert_eq!(top.pk, parent.sk);
    assert!(top.sk.starts_with("TOP#"));

    let inline = generate_id::<InlineChild>(&InlineChildData {}, &parent).unwrap();
    assert_eq!(inline.pk, parent.pk);
    assert!(inline.sk.starts_with("PARENT#1#INLINE#"));

    assert_eq!(
        place_terminal_segment::<TopLevelChild>(&parent, "@CHILD"),
        PkSk {
            pk: "PARENT#1".into(),
            sk: "@CHILD".into(),
        }
    );
    assert_eq!(
        place_terminal_segment::<InlineChild>(&parent, "@CHILD"),
        PkSk {
            pk: "PARENT_PARTITION".into(),
            sk: "PARENT#1@CHILD".into(),
        }
    );
}

#[test]
fn generation_enforces_parent_relationships() {
    let matching = PkSk {
        pk: ROOT_KEY.into(),
        sk: "PARENT#1".into(),
    };
    assert!(generate_id::<StrictChild>(&StrictChildData {}, &matching).is_ok());

    let wrong_type = PkSk {
        pk: ROOT_KEY.into(),
        sk: "OTHER#1".into(),
    };
    assert!(generate_id::<StrictChild>(&StrictChildData {}, &wrong_type).is_err());

    let singleton_parent = PkSk {
        pk: ROOT_KEY.into(),
        sk: "@PARENT".into(),
    };
    assert!(generate_id::<TopLevelChild>(&TopLevelChildData {}, &singleton_parent).is_err());

    assert!(generate_id::<RootUuid>(&RootUuidData {}, &matching).is_err());
}

#[test]
fn regenerates_only_non_singleton_segment_values() {
    let regenerated = regenerate_uuid("PARENT#old#CHILD#old").unwrap();
    assert!(regenerated.starts_with("PARENT#old#CHILD#"));
    assert_ne!(regenerated, "PARENT#old#CHILD#old");

    assert_eq!(
        regenerate_timestamp("#CHILD#old").unwrap().len(),
        "#CHILD#".len() + 22
    );
    assert_eq!(regenerate_uuid("@SETTINGS[key]").unwrap(), "@SETTINGS[key]");
}

#[test]
fn constructs_timestamp_query_bounds_using_object_placement() {
    let lower = timestamp_lower_bound::<RootTimestamp>(PkSk::root(), 1_234).unwrap();
    let upper = timestamp_upper_bound::<RootTimestamp>(PkSk::root(), 1_234).unwrap();

    assert_eq!(lower.pk, ROOT_KEY);
    assert!(lower.sk.starts_with("EVENT#"));
    assert!(lower.sk < upper.sk);
    assert!(timestamp_lower_bound::<RootUuid>(PkSk::root(), 1_234).is_err());
}

#[test]
fn rejects_invalid_schema_labels() {
    assert!(generate_id::<InvalidLabel>(&InvalidLabelData {}, PkSk::root()).is_err());
}
