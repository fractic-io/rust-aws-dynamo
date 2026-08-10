use fractic_server_error::{CriticalError, ServerError};
use serde_json::{Map, Value};

use crate::{
    errors::DynamoItemParsingError,
    schema::{
        materialization::validate_materialized_attribute_names, DynamoFieldRename, DynamoMap,
        DynamoObject,
    },
};

pub(crate) use super::attribute_value::dynamo_map_to_serde_value;

// Public interface.
// ----------------------------------------------------------------------------

pub fn parse_dynamo_map<T: DynamoObject>(map: &DynamoMap) -> Result<T, ServerError> {
    validate_materialized_attribute_names(T::materialized_attribute_names(), T::renamed_fields())?;

    // DynamoMap -> Serde value.
    let Value::Object(mut serde_map) = dynamo_map_to_serde_value(map)? else {
        unreachable!("DynamoMap conversion always returns an object")
    };
    // ID keys are handled explicitly to avoid accidental issues, and to
    // properly combine pk/sk into the object's `id` field.
    serde_map.remove("pk");
    serde_map.remove("sk");

    // Set ID key from pk/sk.
    serde_map.insert(
        "id".to_string(),
        match (map.get("pk"), map.get("sk")) {
            (Some(pk), Some(sk)) => serde_json::Value::String(format!(
                "{}|{}",
                pk.as_s()
                    .map_err(|_| CriticalError::new("pk was not string"))?,
                sk.as_s()
                    .map_err(|_| CriticalError::new("sk was not string"))?,
            )),
            _ => serde_json::Value::Null,
        },
    );

    normalize_renamed_fields(&mut serde_map, T::renamed_fields());
    for name in T::materialized_attribute_names() {
        serde_map.remove(*name);
    }

    // Serde value -> DynamoObject.
    serde_json::from_value(serde_json::Value::Object(serde_map))
        .map_err(|e| DynamoItemParsingError::with_debug("failed to convert from Serde value", &e))
}

// Crate-internal.
// ----------------------------------------------------------------------------

pub(crate) fn deserialize_dynamo_map_partitions<I, S>(
    partitions: I,
) -> Result<DynamoMap, ServerError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut json = String::new();
    for partition in partitions {
        json.push_str(partition.as_ref());
    }

    let value: serde_json::Value = serde_json::from_str(&json)
        .map_err(|e| DynamoItemParsingError::with_debug("failed to parse partition json", &e))?;
    super::attribute_value::serde_value_into_dynamo_map(value)
}

/// Converts an object-shaped Serde value into a Dynamo map.
///
/// Null object fields are omitted, matching normal Serde `Option::None`
/// persistence. Explicit nulls inside arrays are retained.
#[cfg(test)]
pub(crate) fn serde_value_to_dynamo_map(value: &Value) -> Result<DynamoMap, ServerError> {
    super::attribute_value::serde_value_into_dynamo_map(value.clone())
}

// Helpers.
// ----------------------------------------------------------------------------

fn normalize_renamed_fields(map: &mut Map<String, Value>, renamed_fields: &[DynamoFieldRename]) {
    for renamed in renamed_fields {
        if renamed.is_noop() {
            continue;
        }

        if map.contains_key(renamed.to) {
            map.remove(renamed.from);
        } else if let Some(value) = map.remove(renamed.from) {
            map.insert(renamed.to.to_string(), value);
        }
    }
}

// Tests.
// ----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        dynamo_object,
        schema::item_serialization::{
            build_dynamo_map_for_existing_obj, build_dynamo_map_for_new_obj, IdKeys,
        },
        schema::{AutoFields, IdLogic, NestingLogic, PkSk, Timestamp},
        util::{AUTO_FIELDS_CREATED_AT, AUTO_FIELDS_SORT, AUTO_FIELDS_TTL, AUTO_FIELDS_UPDATED_AT},
    };
    use aws_sdk_dynamodb::types::AttributeValue;
    use fractic_core::collection;
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use std::collections::HashMap;

    #[derive(Serialize, Deserialize, Debug, PartialEq, Default, Clone)]
    pub struct TestDynamoObjectData {
        name: String,
        name_nullable: Option<String>,
        null: Option<String>,
        num: u32,
        float: f64,
        nested_map: HashMap<String, String>,
        nested_map_with_null: HashMap<String, Option<String>>,
        nested_vec: Vec<String>,
        nested_vec_with_null: Vec<Option<String>>,
    }

    dynamo_object!(
        TestDynamoObject,
        TestDynamoObjectData,
        "TEST",
        IdLogic::UuidV4,
        NestingLogic::Root
    );

    #[derive(Serialize, Deserialize, Debug, PartialEq, Default, Clone)]
    pub struct TestRenamedObjectData {
        name: Option<String>,
    }

    dynamo_object!(
        TestRenamedObject,
        TestRenamedObjectData,
        "TESTRENAMED",
        IdLogic::UuidV4,
        NestingLogic::Root,
        renamed = ["old_name" -> "name"]
    );

    #[derive(Serialize, Deserialize, Debug, PartialEq, Default, Clone)]
    pub struct TestMaterializedObjectData {
        email: String,
        active: bool,
    }

    dynamo_object!(
        TestMaterializedObject,
        TestMaterializedObjectData,
        "TESTMATERIALIZED",
        IdLogic::UuidV4,
        NestingLogic::Root,
        materialized = |id, data| {
            "email_normalized" => data.email.trim().to_lowercase(),
            "active_lookup" => data.active.then(|| format!("{}|{}", id.pk, id.sk)),
        },
        renamed = ["old_email_normalized" => "email_normalized"],
    );

    #[derive(Serialize, Deserialize, Debug, PartialEq, Default, Clone)]
    pub struct TestMaterializedCollisionData {
        email: Option<String>,
    }

    dynamo_object!(
        TestMaterializedCollision,
        TestMaterializedCollisionData,
        "TESTMATERIALIZEDCOLLISION",
        IdLogic::UuidV4,
        NestingLogic::Root,
        materialized = |data| {
            "email" => data.email.clone(),
        },
    );

    #[derive(Serialize, Deserialize, Debug, PartialEq, Default, Clone)]
    pub struct TestInvalidMaterializedData {
        value: String,
    }

    dynamo_object!(
        TestDuplicateMaterialized,
        TestInvalidMaterializedData,
        "TESTDUPLICATEMATERIALIZED",
        IdLogic::UuidV4,
        NestingLogic::Root,
        materialized = |data| {
            "lookup" => data.value.clone(),
            "lookup" => data.value.clone(),
        },
    );

    dynamo_object!(
        TestReservedMaterialized,
        TestInvalidMaterializedData,
        "TESTRESERVEDMATERIALIZED",
        IdLogic::UuidV4,
        NestingLogic::Root,
        materialized = |data| {
            "pk" => data.value.clone(),
        },
    );

    #[derive(Serialize, Deserialize, Debug, PartialEq, Default, Clone)]
    pub struct TestLegacyRenameMaterializedCollisionData {
        email: Option<String>,
    }

    dynamo_object!(
        TestLegacyRenameMaterializedCollision,
        TestLegacyRenameMaterializedCollisionData,
        "TESTLEGACYRENAMEMATERIALIZEDCOLLISION",
        IdLogic::UuidV4,
        NestingLogic::Root,
        renamed = ["old_email" => "email"],
        materialized = |data| {
            "old_email" => data.email.as_ref().map(|email| email.to_lowercase()),
        },
    );

    #[derive(Serialize, Deserialize, Debug, PartialEq, Default, Clone)]
    pub struct TestUnsupportedMaterializedData {
        value: String,
    }

    dynamo_object!(
        TestUnsupportedMaterialized,
        TestUnsupportedMaterializedData,
        "TESTUNSUPPORTEDMATERIALIZED",
        IdLogic::SingletonExt,
        NestingLogic::Root,
        materialized = |data| {
            "value_lookup" => data.value.clone(),
        },
    );

    #[test]
    fn test_build_dynamo_map_adds_materialized_attributes() {
        let data = TestMaterializedObjectData {
            email: "  User@Example.COM ".to_string(),
            active: true,
        };
        let map = build_dynamo_map_for_new_obj::<TestMaterializedObject>(
            &data,
            "ROOT".to_string(),
            "TESTMATERIALIZED#1".to_string(),
            None,
        )
        .unwrap();

        assert_eq!(
            map.get("email_normalized").unwrap().as_s().unwrap(),
            "user@example.com"
        );
        assert_eq!(
            map.get("active_lookup").unwrap().as_s().unwrap(),
            "ROOT|TESTMATERIALIZED#1"
        );
    }

    #[test]
    fn test_build_dynamo_map_removes_null_materialized_attributes_on_update() {
        let object = TestMaterializedObject::new(
            PkSk {
                pk: "ROOT".to_string(),
                sk: "TESTMATERIALIZED#1".to_string(),
            },
            TestMaterializedObjectData {
                email: "User@Example.COM".to_string(),
                active: false,
            },
        );

        let (map, remove) = build_dynamo_map_for_existing_obj::<TestMaterializedObject>(
            &object,
            IdKeys::None,
            None,
        )
        .unwrap();

        assert_eq!(
            map.get("email_normalized").unwrap().as_s().unwrap(),
            "user@example.com"
        );
        assert!(!map.contains_key("active_lookup"));
        assert!(remove.contains(&"active_lookup".to_string()));
    }

    #[test]
    fn test_materialized_attribute_collisions_are_rejected_even_when_null() {
        let result = build_dynamo_map_for_new_obj::<TestMaterializedCollision>(
            &TestMaterializedCollisionData { email: None },
            "ROOT".to_string(),
            "TESTMATERIALIZEDCOLLISION#1".to_string(),
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_duplicate_materialized_attribute_names_are_rejected() {
        let result = build_dynamo_map_for_new_obj::<TestDuplicateMaterialized>(
            &TestInvalidMaterializedData {
                value: "value".to_string(),
            },
            "ROOT".to_string(),
            "TESTDUPLICATEMATERIALIZED#1".to_string(),
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_reserved_materialized_attribute_names_are_rejected() {
        let result = build_dynamo_map_for_new_obj::<TestReservedMaterialized>(
            &TestInvalidMaterializedData {
                value: "value".to_string(),
            },
            "ROOT".to_string(),
            "TESTRESERVEDMATERIALIZED#1".to_string(),
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_materialized_attribute_names_cannot_be_legacy_rename_sources() {
        let data = TestLegacyRenameMaterializedCollisionData { email: None };
        let write = build_dynamo_map_for_new_obj::<TestLegacyRenameMaterializedCollision>(
            &data,
            "ROOT".to_string(),
            "TESTLEGACYRENAMEMATERIALIZEDCOLLISION#1".to_string(),
            None,
        );
        assert!(write.is_err());

        let persisted = collection!(
            "pk".to_string() => AttributeValue::S("ROOT".to_string()),
            "sk".to_string() => AttributeValue::S(
                "TESTLEGACYRENAMEMATERIALIZEDCOLLISION#1".to_string()
            ),
            "old_email".to_string() => AttributeValue::S("derived@example.com".to_string()),
        );
        let read = parse_dynamo_map::<TestLegacyRenameMaterializedCollision>(&persisted);
        assert!(read.is_err());
    }

    #[test]
    fn test_materialized_attributes_reject_partitioned_storage() {
        let result = build_dynamo_map_for_new_obj::<TestUnsupportedMaterialized>(
            &TestUnsupportedMaterializedData {
                value: "value".to_string(),
            },
            "ROOT".to_string(),
            "@TESTUNSUPPORTEDMATERIALIZED".to_string(),
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_build_dynamo_map_for_new_obj() {
        let input = TestDynamoObject {
            id: PkSk {
                pk: "123".to_string(),
                sk: "456".to_string(),
            },
            auto_fields: AutoFields::default(),
            data: TestDynamoObjectData {
                name: "Test".to_string(),
                name_nullable: Some("TestNonNull".to_string()),
                null: None,
                num: 42,
                float: 2.72,
                nested_map: [("key".to_string(), "value".to_string())]
                    .iter()
                    .cloned()
                    .collect(),
                nested_map_with_null: collection!(
                    "null".to_string() => None,
                    "non-null".to_string() => Some("value".to_string())
                ),
                nested_vec: vec!["elem1".to_string(), "elem2".to_string()],
                nested_vec_with_null: vec![Some("elem1".to_string()), None],
            },
        };

        let output = build_dynamo_map_for_new_obj::<TestDynamoObject>(
            &input.data,
            "pk_override".to_string(),
            "sk_override".to_string(),
            None,
        )
        .unwrap();

        let expected_output = collection!(
            "pk".to_string() => AttributeValue::S("pk_override".to_string()),
            "sk".to_string() => AttributeValue::S("sk_override".to_string()),
            "name".to_string() => AttributeValue::S("Test".to_string()),
            "name_nullable".to_string() => AttributeValue::S("TestNonNull".to_string()),
            "num".to_string() => AttributeValue::N("42".to_string()),
            "float".to_string() => AttributeValue::N("2.72".to_string()),
            "nested_map".to_string() => AttributeValue::M(collection!(
                "key".to_string() => AttributeValue::S("value".to_string())
            )),
            "nested_map_with_null".to_string() => AttributeValue::M(collection!(
                "non-null".to_string() => AttributeValue::S("value".to_string())
            )),
            "nested_vec".to_string() => AttributeValue::L(vec![
                AttributeValue::S("elem1".to_string()),
                AttributeValue::S("elem2".to_string()),
            ]),
            "nested_vec_with_null".to_string() => AttributeValue::L(vec![
                AttributeValue::S("elem1".to_string()),
                AttributeValue::Null(true),
            ]),
        );
        assert_eq!(output, expected_output);
    }

    #[test]
    fn test_build_dynamo_map_copy_id_from_object() {
        let input = TestDynamoObject {
            id: PkSk {
                pk: "123".to_string(),
                sk: "456".to_string(),
            },
            auto_fields: AutoFields::default(),
            data: TestDynamoObjectData {
                name: "Test".to_string(),
                num: 42,
                float: 2.72,
                ..Default::default()
            },
        };

        let (output, skipped_null_keys) =
            build_dynamo_map_for_existing_obj(&input, IdKeys::CopyFromObject, None).unwrap();

        let expected_output = collection!(
            "pk".to_string() => AttributeValue::S("123".to_string()),
            "sk".to_string() => AttributeValue::S("456".to_string()),
            "name".to_string() => AttributeValue::S("Test".to_string()),
            "num".to_string() => AttributeValue::N("42".to_string()),
            "float".to_string() => AttributeValue::N("2.72".to_string()),
            "nested_map".to_string() => AttributeValue::M(collection!()),
            "nested_map_with_null".to_string() => AttributeValue::M(collection!()),
            "nested_vec".to_string() => AttributeValue::L(vec![]),
            "nested_vec_with_null".to_string() => AttributeValue::L(vec![]),
        );
        let expected_skipped_null_keys = vec!["name_nullable".to_string(), "null".to_string()];

        assert_eq!(output, expected_output);
        assert_eq!(skipped_null_keys, expected_skipped_null_keys);
    }

    #[test]
    fn test_build_dynamo_map_override_id() {
        let input = TestDynamoObject {
            id: PkSk {
                pk: "123".to_string(),
                sk: "456".to_string(),
            },
            auto_fields: AutoFields::default(),
            data: TestDynamoObjectData {
                name: "Test".to_string(),
                name_nullable: Some("TestNonNull".to_string()),
                null: None,
                num: 42,
                float: 2.72,
                nested_map: [("key".to_string(), "value".to_string())]
                    .iter()
                    .cloned()
                    .collect(),
                nested_map_with_null: collection!(
                    "null".to_string() => None,
                    "non-null".to_string() => Some("value".to_string())
                ),
                nested_vec: vec!["elem1".to_string(), "elem2".to_string()],
                nested_vec_with_null: vec![None, Some("elem1".to_string()), None],
            },
        };

        let (output, skipped_null_keys) = build_dynamo_map_for_existing_obj(
            &input,
            IdKeys::Override("pk_override".to_string(), "sk_override".to_string()),
            None,
        )
        .unwrap();

        let expected_output = collection!(
            "pk".to_string() => AttributeValue::S("pk_override".to_string()),
            "sk".to_string() => AttributeValue::S("sk_override".to_string()),
            "name".to_string() => AttributeValue::S("Test".to_string()),
            "name_nullable".to_string() => AttributeValue::S("TestNonNull".to_string()),
            "num".to_string() => AttributeValue::N("42".to_string()),
            "float".to_string() => AttributeValue::N("2.72".to_string()),
            "nested_map".to_string() => AttributeValue::M(collection!(
                "key".to_string() => AttributeValue::S("value".to_string())
            )),
            "nested_map_with_null".to_string() => AttributeValue::M(collection!(
                "non-null".to_string() => AttributeValue::S("value".to_string())
            )),
            "nested_vec".to_string() => AttributeValue::L(vec![
                AttributeValue::S("elem1".to_string()),
                AttributeValue::S("elem2".to_string()),
            ]),
            "nested_vec_with_null".to_string() => AttributeValue::L(vec![
                AttributeValue::Null(true),
                AttributeValue::S("elem1".to_string()),
                AttributeValue::Null(true),
            ]),
        );
        let expected_skipped_null_keys = vec!["null".to_string()];

        assert_eq!(output, expected_output);
        assert_eq!(skipped_null_keys, expected_skipped_null_keys);
    }

    #[test]
    fn test_build_dynamo_map_no_ids() {
        let input = TestDynamoObject {
            id: PkSk {
                pk: "123".to_string(),
                sk: "456".to_string(),
            },
            auto_fields: AutoFields::default(),
            data: TestDynamoObjectData {
                name: "Test".to_string(),
                num: 42,
                float: 2.72,
                ..Default::default()
            },
        };

        let (output, skipped_null_keys) =
            build_dynamo_map_for_existing_obj(&input, IdKeys::None, None).unwrap();

        let expected_output = collection!(
            // No id fields.
            "name".to_string() => AttributeValue::S("Test".to_string()),
            "num".to_string() => AttributeValue::N("42".to_string()),
            "float".to_string() => AttributeValue::N("2.72".to_string()),
            "nested_map".to_string() => AttributeValue::M(collection!()),
            "nested_map_with_null".to_string() => AttributeValue::M(collection!()),
            "nested_vec".to_string() => AttributeValue::L(vec![]),
            "nested_vec_with_null".to_string() => AttributeValue::L(vec![]),
        );
        let expected_skipped_null_keys = vec!["name_nullable".to_string(), "null".to_string()];

        assert_eq!(output, expected_output);
        assert_eq!(skipped_null_keys, expected_skipped_null_keys);
    }

    #[test]
    fn test_build_dynamo_map_auto_fields_skipped() {
        let sample_timestamp = Timestamp::now();

        let input = TestDynamoObject {
            id: PkSk {
                pk: "123".to_string(),
                sk: "456".to_string(),
            },
            // These fields should always be skipped in serialization,
            // to make them effectively read-only. They should be manually
            // overrided by DynamoUtil logic.
            auto_fields: AutoFields {
                created_at: Some(sample_timestamp.clone()),
                updated_at: Some(sample_timestamp.clone()),
                sort: Some(0.65),
                ttl: Some(1234567890),
                unknown_fields: collection!(
                    "unknown_field".to_string() => Value::String("unknown_value".to_string())
                ),
            },
            data: TestDynamoObjectData {
                name: "Test".to_string(),
                num: 42,
                float: 2.72,
                ..Default::default()
            },
        };

        let (output, skipped_null_keys) =
            build_dynamo_map_for_existing_obj(&input, IdKeys::None, None).unwrap();

        let expected_output = collection!(
            // - No id fields.
            // - No auto fields.
            "name".to_string() => AttributeValue::S("Test".to_string()),
            "num".to_string() => AttributeValue::N("42".to_string()),
            "float".to_string() => AttributeValue::N("2.72".to_string()),
            "nested_map".to_string() => AttributeValue::M(collection!()),
            "nested_map_with_null".to_string() => AttributeValue::M(collection!()),
            "nested_vec".to_string() => AttributeValue::L(vec![]),
            "nested_vec_with_null".to_string() => AttributeValue::L(vec![]),
        );
        let expected_skipped_null_keys = vec!["name_nullable".to_string(), "null".to_string()];

        assert_eq!(output, expected_output);
        assert_eq!(skipped_null_keys, expected_skipped_null_keys);
    }

    #[test]
    fn test_build_dynamo_map_with_overrides() {
        let sample_timestamp_1 = Timestamp::now();
        let sample_timestamp_2 = Timestamp::now();

        let input = TestDynamoObject {
            id: PkSk {
                pk: "123".to_string(),
                sk: "456".to_string(),
            },
            auto_fields: AutoFields::default(),
            data: TestDynamoObjectData {
                name: "Test".to_string(),
                num: 42,
                float: 2.72,
                ..Default::default()
            },
        };

        let (output, skipped_null_keys) = build_dynamo_map_for_existing_obj(
            &input,
            IdKeys::None,
            Some(vec![
                (AUTO_FIELDS_CREATED_AT, Box::new(sample_timestamp_1.clone())),
                (AUTO_FIELDS_UPDATED_AT, Box::new(sample_timestamp_2.clone())),
                (AUTO_FIELDS_SORT, Box::new(1.2345)),
            ]),
        )
        .unwrap();

        let expected_output = collection!(
            "name".to_string() => AttributeValue::S("Test".to_string()),
            "num".to_string() => AttributeValue::N("42".to_string()),
            "float".to_string() => AttributeValue::N("2.72".to_string()),
            "nested_map".to_string() => AttributeValue::M(collection!()),
            "nested_map_with_null".to_string() => AttributeValue::M(collection!()),
            "nested_vec".to_string() => AttributeValue::L(vec![]),
            "nested_vec_with_null".to_string() => AttributeValue::L(vec![]),
            AUTO_FIELDS_CREATED_AT.to_string() => AttributeValue::S(format!(
                "{:011}.{:09}",
                sample_timestamp_1.seconds, sample_timestamp_1.nanos
            )),
            AUTO_FIELDS_UPDATED_AT.to_string() => AttributeValue::S(format!(
                "{:011}.{:09}",
                sample_timestamp_2.seconds, sample_timestamp_2.nanos
            )),
            AUTO_FIELDS_SORT.to_string() => AttributeValue::N("1.2345".to_string())
        );
        let expected_skipped_null_keys = vec!["name_nullable".to_string(), "null".to_string()];

        assert_eq!(output, expected_output);
        assert_eq!(skipped_null_keys, expected_skipped_null_keys);
    }

    #[test]
    fn test_parse_dynamo_map() {
        let sample_timestamp_1 = Timestamp::now();
        let sample_timestamp_2 = Timestamp::now();

        let input = collection!(
            "pk".to_string() => AttributeValue::S("123".to_string()),
            "sk".to_string() => AttributeValue::S("456".to_string()),
            "name".to_string() => AttributeValue::S("Test".to_string()),
            "num".to_string() => AttributeValue::N("42".to_string()),
            "float".to_string() => AttributeValue::N("2.72".to_string()),
            "nested_map".to_string() => AttributeValue::M(collection!(
                "key".to_string() => AttributeValue::S("value".to_string())
            )),
            "nested_map_with_null".to_string() => AttributeValue::M(collection!(
                "non-null".to_string() => AttributeValue::S("value".to_string()),
            )),
            "nested_vec".to_string() => AttributeValue::L(vec![
                AttributeValue::S("elem1".to_string()),
                AttributeValue::S("elem2".to_string()),
            ]),
            "nested_vec_with_null".to_string() => AttributeValue::L(vec![
                AttributeValue::Null(true),
                AttributeValue::S("elem1".to_string()),
                AttributeValue::Null(true),
            ]),
            // Test both string and map formats for storing Timestamp:
            AUTO_FIELDS_CREATED_AT.to_string() => AttributeValue::M(collection!(
                "seconds".to_string() => AttributeValue::N(sample_timestamp_1.seconds.to_string()),
                "nanos".to_string() => AttributeValue::N(sample_timestamp_1.nanos.to_string())
            )),
            AUTO_FIELDS_UPDATED_AT.to_string() => AttributeValue::S(format!(
                "{:011}.{:09}",
                sample_timestamp_2.seconds, sample_timestamp_2.nanos
            )),
            AUTO_FIELDS_SORT.to_string() => AttributeValue::N("1.2345".to_string()),
            AUTO_FIELDS_TTL.to_string() => AttributeValue::N("1234567890".to_string()),
            "unknown_field".to_string() => AttributeValue::S("unknown_value".to_string()),
        );

        let output: TestDynamoObject = parse_dynamo_map(&input).unwrap();

        let expected_output = TestDynamoObject {
            id: PkSk {
                pk: "123".to_string(),
                sk: "456".to_string(),
            },
            auto_fields: AutoFields {
                created_at: Some(sample_timestamp_1.clone()),
                updated_at: Some(sample_timestamp_2.clone()),
                sort: Some(1.2345),
                ttl: Some(1234567890),
                unknown_fields: collection!(
                    "unknown_field".to_string() => Value::String("unknown_value".to_string())
                ),
            },
            data: TestDynamoObjectData {
                name: "Test".to_string(),
                name_nullable: None,
                null: None,
                num: 42,
                float: 2.72,
                nested_map: collection!("key".to_string() => "value".to_string()),
                nested_map_with_null: collection!(
                    "non-null".to_string() => Some("value".to_string()),
                ),
                nested_vec: vec!["elem1".to_string(), "elem2".to_string()],
                nested_vec_with_null: vec![None, Some("elem1".to_string()), None],
            },
        };
        assert_eq!(output.id, expected_output.id);
        assert_eq!(output.auto_fields, expected_output.auto_fields);
        assert_eq!(output.data, expected_output.data);
    }

    // Null-values shouldn't really be encountered since they are skipped in
    // serialization. They should still work, however, so test it here.
    #[test]
    fn test_parse_dynamo_map_with_null_values() {
        let sample_timestamp_1 = Timestamp::now();
        let sample_timestamp_2 = Timestamp::now();

        let input = collection!(
            "pk".to_string() => AttributeValue::S("123".to_string()),
            "sk".to_string() => AttributeValue::S("456".to_string()),
            "name".to_string() => AttributeValue::S("Test".to_string()),
            "name_nullable".to_string() => AttributeValue::Null(true),
            "null".to_string() => AttributeValue::Null(true),
            "num".to_string() => AttributeValue::N("42".to_string()),
            "float".to_string() => AttributeValue::N("2.72".to_string()),
            "nested_map".to_string() => AttributeValue::M(collection!(
                "key".to_string() => AttributeValue::S("value".to_string())
            )),
            "nested_map_with_null".to_string() => AttributeValue::M(collection!(
                "non-null".to_string() => AttributeValue::S("value".to_string()),
                "null".to_string() => AttributeValue::Null(true)
            )),
            "nested_vec".to_string() => AttributeValue::L(vec![
                AttributeValue::S("elem1".to_string()),
                AttributeValue::S("elem2".to_string()),
            ]),
            "nested_vec_with_null".to_string() => AttributeValue::L(vec![
                AttributeValue::Null(true),
                AttributeValue::S("elem1".to_string()),
                AttributeValue::Null(true),
            ]),
            AUTO_FIELDS_CREATED_AT.to_string() => AttributeValue::M(collection!(
                "seconds".to_string() => AttributeValue::N(sample_timestamp_1.seconds.to_string()),
                "nanos".to_string() => AttributeValue::N(sample_timestamp_1.nanos.to_string())
            )),
            AUTO_FIELDS_UPDATED_AT.to_string() => AttributeValue::M(collection!(
                "seconds".to_string() => AttributeValue::N(sample_timestamp_2.seconds.to_string()),
                "nanos".to_string() => AttributeValue::N(sample_timestamp_2.nanos.to_string())
            )),
            AUTO_FIELDS_SORT.to_string() => AttributeValue::N("1.2345".to_string()),
            AUTO_FIELDS_TTL.to_string() => AttributeValue::N("1234567890".to_string()),
            "unknown_field".to_string() => AttributeValue::S("unknown_value".to_string()),
        );

        let output: TestDynamoObject = parse_dynamo_map(&input).unwrap();

        let expected_output = TestDynamoObject {
            id: PkSk {
                pk: "123".to_string(),
                sk: "456".to_string(),
            },
            auto_fields: AutoFields {
                created_at: Some(sample_timestamp_1.clone()),
                updated_at: Some(sample_timestamp_2.clone()),
                sort: Some(1.2345),
                ttl: Some(1234567890),
                unknown_fields: collection!(
                    "unknown_field".to_string() => Value::String("unknown_value".to_string())
                ),
            },
            data: TestDynamoObjectData {
                name: "Test".to_string(),
                name_nullable: None,
                null: None,
                num: 42,
                float: 2.72,
                nested_map: collection!("key".to_string() => "value".to_string()),
                nested_map_with_null: collection!(
                    "non-null".to_string() => Some("value".to_string()),
                ),
                nested_vec: vec!["elem1".to_string(), "elem2".to_string()],
                nested_vec_with_null: vec![None, Some("elem1".to_string()), None],
            },
        };
        assert_eq!(output.id, expected_output.id);
        assert_eq!(output.auto_fields, expected_output.auto_fields);
        assert_eq!(output.data, expected_output.data);
    }

    #[test]
    fn test_parse_dynamo_map_with_renamed_fields() {
        let input = collection!(
            "pk".to_string() => AttributeValue::S("123".to_string()),
            "sk".to_string() => AttributeValue::S("456".to_string()),
            "old_name".to_string() => AttributeValue::S("old".to_string()),
        );

        let output: TestRenamedObject = parse_dynamo_map(&input).unwrap();

        assert_eq!(output.data.name, Some("old".to_string()));
    }

    #[test]
    fn test_parse_dynamo_map_prefers_canonical_renamed_fields() {
        let input = collection!(
            "pk".to_string() => AttributeValue::S("123".to_string()),
            "sk".to_string() => AttributeValue::S("456".to_string()),
            "old_name".to_string() => AttributeValue::S("old".to_string()),
            "name".to_string() => AttributeValue::S("new".to_string()),
        );

        let output: TestRenamedObject = parse_dynamo_map(&input).unwrap();

        assert_eq!(output.data.name, Some("new".to_string()));
        assert!(output.auto_fields.unknown_fields.is_empty());
    }

    #[test]
    fn test_parse_dynamo_map_strips_materialized_attributes() {
        let input = collection!(
            "pk".to_string() => AttributeValue::S("ROOT".to_string()),
            "sk".to_string() => AttributeValue::S("TESTMATERIALIZED#1".to_string()),
            "email".to_string() => AttributeValue::S("User@Example.COM".to_string()),
            "active".to_string() => AttributeValue::Bool(true),
            "email_normalized".to_string() => AttributeValue::S("stale-value".to_string()),
            "active_lookup".to_string() => AttributeValue::S("stale-value".to_string()),
            "unrecognized".to_string() => AttributeValue::S("preserved".to_string()),
        );

        let output: TestMaterializedObject = parse_dynamo_map(&input).unwrap();

        assert_eq!(output.data.email, "User@Example.COM");
        assert!(output.data.active);
        assert_eq!(
            output.unknown_field_keys(),
            vec![&"unrecognized".to_string()]
        );
    }

    #[test]
    fn test_parse_dynamo_map_applies_rename_before_stripping_materialized_attribute() {
        let input = collection!(
            "pk".to_string() => AttributeValue::S("ROOT".to_string()),
            "sk".to_string() => AttributeValue::S("TESTMATERIALIZED#1".to_string()),
            "email".to_string() => AttributeValue::S("User@Example.COM".to_string()),
            "active".to_string() => AttributeValue::Bool(true),
            "old_email_normalized".to_string() => AttributeValue::S("legacy".to_string()),
        );

        let output: TestMaterializedObject = parse_dynamo_map(&input).unwrap();

        assert!(output.auto_fields.unknown_fields.is_empty());
    }

    #[test]
    fn test_parse_dynamo_map_partitions() {
        let output =
            deserialize_dynamo_map_partitions([r#"{"name":"Tes"#, r#"t","nested":{"value":1}}"#])
                .unwrap();

        let expected_output = collection!(
            "name".to_string() => AttributeValue::S("Test".to_string()),
            "nested".to_string() => AttributeValue::M(collection!(
                "value".to_string() => AttributeValue::N("1".to_string())
            ))
        );

        assert_eq!(output, expected_output);
    }
}
