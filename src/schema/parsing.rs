use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use fractic_generic_server_error::{common::CriticalError, GenericServerError};
use serde::Serialize;

use crate::{errors::DynamoItemParsingError, schema::DynamoObject, util::DynamoMap};

// Converting between DynamoMap and DynamoObject.
// --------------------------------------------------

pub enum IdKeys {
    CopyFromObject,
    Override(String, String),
    None,
}

pub fn build_dynamo_map_for_new_obj<T: DynamoObject>(
    data: &T::Data,
    pk: String,
    sk: String,
    overrides: Option<Vec<(&str, Box<dyn erased_serde::Serialize>)>>,
) -> Result<DynamoMap, GenericServerError> {
    // For new objects, skipped null keys are not important.
    let (dynamo_map, _skipped_null_keys) =
        build_dynamo_map_internal(data, Some(pk), Some(sk), overrides)?;
    Ok(dynamo_map)
}

// IMPORTANT:
//   In addition to the resulting dynamo map, this function also returns a list
//   of keys that were skipped because they were null. If updating an existing
//   item in the database, these keys should be included in the update query as
//   REMOVE operations, to avoid existing non-null values being left untouched.
pub fn build_dynamo_map_for_existing_obj<T: DynamoObject>(
    object: &T,
    id_keys: IdKeys,
    overrides: Option<Vec<(&str, Box<dyn erased_serde::Serialize>)>>,
) -> Result<(DynamoMap, Vec<String>), GenericServerError> {
    let (pk, sk) = match id_keys {
        IdKeys::Override(pk, sk) => (Some(pk), Some(sk)),
        IdKeys::CopyFromObject => (Some(object.id().pk.clone()), Some(object.id().sk.clone())),
        IdKeys::None => (None, None),
    };
    build_dynamo_map_internal(object, pk, sk, overrides)
}

fn build_dynamo_map_internal<T: Serialize>(
    object: &T,
    pk: Option<String>,
    sk: Option<String>,
    overrides: Option<Vec<(&str, Box<dyn erased_serde::Serialize>)>>,
) -> Result<(DynamoMap, Vec<String>), GenericServerError> {
    let dbg_cxt: &'static str = "build_dynamo_map";

    // Keep track of skipped null values, as they may be important to the caller.
    let mut skipped_null_keys: Vec<String> = Vec::new();

    // DynamoObject -> Serde value.
    let json_value = serde_json::to_value(&object).map_err(|e| {
        DynamoItemParsingError::with_debug(dbg_cxt, "failed to serialize object", e.to_string())
    })?;

    // Serde value -> DynamoMap.
    let mut attribute_values: HashMap<String, AttributeValue> = HashMap::new();
    match json_value {
        serde_json::Value::Object(map) => {
            for (key, value) in map.into_iter() {
                if key == "id" {
                    // ID key is handled explicitly to avoid accidental issues,
                    // and properly set pk/sk separately.
                    continue;
                }
                if let Some(v) = serde_value_to_attribute_value(value)? {
                    attribute_values.insert(key, v);
                } else {
                    skipped_null_keys.push(key);
                }
            }
        }
        unsupported => {
            return Err(DynamoItemParsingError::with_debug(
                dbg_cxt,
                "can't build DynamoMap from object",
                format!("{:?}", unsupported),
            ))
        }
    }

    // Set ID keys.
    if let Some(pk) = pk {
        attribute_values.insert("pk".to_string(), AttributeValue::S(pk));
    }
    if let Some(sk) = sk {
        attribute_values.insert("sk".to_string(), AttributeValue::S(sk));
    }

    // Set overrides.
    if let Some(overrides) = overrides {
        for (key, value) in overrides.into_iter() {
            let json_value = serde_json::to_value(&value).map_err(|e| {
                DynamoItemParsingError::with_debug(
                    dbg_cxt,
                    "failed to serialize override object",
                    e.to_string(),
                )
            })?;
            if let Some(v) = serde_value_to_attribute_value(json_value)? {
                attribute_values.insert(key.into(), v);
            } else {
                skipped_null_keys.push(key.into());
            }
        }
    }

    Ok((attribute_values, skipped_null_keys))
}

pub fn parse_dynamo_map<T: DynamoObject>(map: &DynamoMap) -> Result<T, GenericServerError> {
    let dbg_cxt: &'static str = "parse_dynamo_map";

    // DynamoMap -> Serde value.
    let mut serde_map: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
    for (key, value) in map.iter() {
        if (key == "pk") || (key == "sk") {
            // ID keys are handled explicitly to avoid accidental issues, and
            // properly pk/sk into id field.
            continue;
        }
        if let Some(v) = attribute_value_to_serde_value(value.clone())? {
            serde_map.insert(key.clone(), v);
        }
    }

    // Set ID key from pk/sk.
    serde_map.insert(
        "id".to_string(),
        match (map.get("pk"), map.get("sk")) {
            (Some(pk), Some(sk)) => serde_json::Value::String(format!(
                "{}|{}",
                pk.as_s()
                    .map_err(|_| CriticalError::new(dbg_cxt, "pk was not string"))?,
                sk.as_s()
                    .map_err(|_| CriticalError::new(dbg_cxt, "sk was not string"))?,
            )),
            _ => serde_json::Value::Null,
        },
    );

    // Serde value -> DynamoObject.
    serde_json::from_value(serde_json::Value::Object(serde_map)).map_err(|e| {
        DynamoItemParsingError::with_debug(
            dbg_cxt,
            "failed to convert from Serde value",
            e.to_string(),
        )
    })
}

// Inner recursive functions.
// --------------------------------------------------

fn serde_value_to_attribute_value(
    value: serde_json::Value,
) -> Result<Option<AttributeValue>, GenericServerError> {
    match value {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(s) => Ok(Some(AttributeValue::S(s))),
        serde_json::Value::Number(n) => Ok(Some(AttributeValue::N(n.to_string()))),
        serde_json::Value::Bool(b) => Ok(Some(AttributeValue::Bool(b))),
        serde_json::Value::Object(map) => Ok(Some(AttributeValue::M(
            map.into_iter()
                // Convert SerdeValue to AttributeValue for each key-value pair,
                // filtering all pairs where value is None.
                .filter_map(|(k, v)| Some((k, serde_value_to_attribute_value(v).transpose()?)))
                // Catch any conversion errors.
                .map(|(k, v)| Ok((k, v?)))
                .collect::<Result<HashMap<String, AttributeValue>, GenericServerError>>()?,
        ))),
        serde_json::Value::Array(array) => {
            Ok(Some(AttributeValue::L(
                array
                    .into_iter()
                    .map(|v| {
                        serde_value_to_attribute_value(v).map(|v| match v {
                            Some(v) => v,
                            // For arrays, we want to keep explicit nulls:
                            None => AttributeValue::Null(true),
                        })
                    })
                    .collect::<Result<Vec<_>, GenericServerError>>()?,
            )))
        }
    }
}

fn attribute_value_to_serde_value(
    value: AttributeValue,
) -> Result<Option<serde_json::Value>, GenericServerError> {
    let dbg_cxt: &'static str = "attribute_value_to_serde_value";
    match value {
        AttributeValue::Null(_) => Ok(None),
        AttributeValue::S(s) => Ok(Some(serde_json::Value::String(s))),
        AttributeValue::N(n) => Ok(Some(serde_json::Value::Number(n.parse().map_err(|e| {
            DynamoItemParsingError::with_debug(dbg_cxt, "failed to parse number", format!("{}", e))
        })?))),
        AttributeValue::Bool(b) => Ok(Some(serde_json::Value::Bool(b))),
        AttributeValue::M(map) => Ok(Some(serde_json::Value::Object(
            map.into_iter()
                // Convert AttributeValue to SerdeValue for each key-value pair,
                // filtering all pairs where value is None.
                .filter_map(|(k, v)| Some((k, attribute_value_to_serde_value(v).transpose()?)))
                // Catch any conversion errors.
                .map(|(k, v)| Ok((k, v?)))
                .collect::<Result<serde_json::Map<String, serde_json::Value>, GenericServerError>>(
                )?,
        ))),
        AttributeValue::L(array) => Ok(Some(serde_json::Value::Array(
            array
                .into_iter()
                .map(|v| {
                    attribute_value_to_serde_value(v).map(|v| match v {
                        Some(v) => v,
                        // For arrays, we want to keep explicit nulls:
                        None => serde_json::Value::Null,
                    })
                })
                .collect::<Result<Vec<_>, GenericServerError>>()?,
        ))),
        unsupported => Err(DynamoItemParsingError::with_debug(
            dbg_cxt,
            "unsupported AttributeValue type",
            format!("{:?}", unsupported),
        )),
    }
}

// Tests.
// --------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        dynamo_object,
        schema::{
            AutoFields, DynamoObject, DynamoObjectData, IdLogic, NestingLogic, PkSk, Timestamp,
        },
        util::{AUTO_FIELDS_CREATED_AT, AUTO_FIELDS_SORT, AUTO_FIELDS_UPDATED_AT},
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
        IdLogic::Uuid,
        NestingLogic::Root
    );

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
                float: 3.14,
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
            "float".to_string() => AttributeValue::N("3.14".to_string()),
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
                float: 3.14,
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
            "float".to_string() => AttributeValue::N("3.14".to_string()),
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
                float: 3.14,
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
            "float".to_string() => AttributeValue::N("3.14".to_string()),
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
                float: 3.14,
                ..Default::default()
            },
        };

        let (output, skipped_null_keys) =
            build_dynamo_map_for_existing_obj(&input, IdKeys::None, None).unwrap();

        let expected_output = collection!(
            // No id fields.
            "name".to_string() => AttributeValue::S("Test".to_string()),
            "num".to_string() => AttributeValue::N("42".to_string()),
            "float".to_string() => AttributeValue::N("3.14".to_string()),
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
                unknown_fields: collection!(
                    "unknown_field".to_string() => Value::String("unknown_value".to_string())
                ),
            },
            data: TestDynamoObjectData {
                name: "Test".to_string(),
                num: 42,
                float: 3.14,
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
            "float".to_string() => AttributeValue::N("3.14".to_string()),
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
                float: 3.14,
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
            "float".to_string() => AttributeValue::N("3.14".to_string()),
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
            "float".to_string() => AttributeValue::N("3.14".to_string()),
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
                unknown_fields: collection!(
                    "unknown_field".to_string() => Value::String("unknown_value".to_string())
                ),
            },
            data: TestDynamoObjectData {
                name: "Test".to_string(),
                name_nullable: None,
                null: None,
                num: 42,
                float: 3.14,
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
            "float".to_string() => AttributeValue::N("3.14".to_string()),
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
                unknown_fields: collection!(
                    "unknown_field".to_string() => Value::String("unknown_value".to_string())
                ),
            },
            data: TestDynamoObjectData {
                name: "Test".to_string(),
                name_nullable: None,
                null: None,
                num: 42,
                float: 3.14,
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
}
