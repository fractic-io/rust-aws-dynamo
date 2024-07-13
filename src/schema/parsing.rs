use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use fractic_generic_server_error::{common::CriticalError, GenericServerError};

use crate::{errors::DynamoItemParsingError, schema::DynamoObject, util::DynamoMap};

// Converting between DynamoMap and DynamoObject.
// --------------------------------------------------

pub enum IdKeys {
    CopyFromObject,
    Override(String, String),
    None,
}

pub fn build_dynamo_map<T: DynamoObject>(
    object: &T,
    id_keys: IdKeys,
    overrides: Option<Vec<(&str, Box<dyn erased_serde::Serialize>)>>,
) -> Result<DynamoMap, GenericServerError> {
    let dbg_cxt: &'static str = "build_dynamo_map";

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
                attribute_values.insert(key, serde_value_to_attribute_value(value)?);
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
    match id_keys {
        IdKeys::Override(pk, sk) => {
            attribute_values.insert("pk".to_string(), AttributeValue::S(pk));
            attribute_values.insert("sk".to_string(), AttributeValue::S(sk));
        }
        IdKeys::CopyFromObject => {
            attribute_values.insert(
                "pk".to_string(),
                AttributeValue::S(
                    object
                        .pk()
                        .ok_or(DynamoItemParsingError::new(dbg_cxt, "object missing pk"))?
                        .to_string(),
                ),
            );
            attribute_values.insert(
                "sk".to_string(),
                AttributeValue::S(
                    object
                        .sk()
                        .ok_or(DynamoItemParsingError::new(dbg_cxt, "object missing sk"))?
                        .to_string(),
                ),
            );
        }
        IdKeys::None => {}
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
            attribute_values.insert(key.into(), serde_value_to_attribute_value(json_value)?);
        }
    }

    Ok(attribute_values)
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
        serde_map.insert(key.clone(), attribute_value_to_serde_value(value.clone())?);
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
) -> Result<AttributeValue, GenericServerError> {
    let dbg_cxt: &'static str = "serde_value_to_attribute_value";
    match value {
        serde_json::Value::Null => Ok(AttributeValue::Null(true)),
        serde_json::Value::String(s) => Ok(AttributeValue::S(s)),
        serde_json::Value::Number(n) => Ok(AttributeValue::N(n.to_string())),
        serde_json::Value::Object(map) => {
            let mut attribute_map: HashMap<String, AttributeValue> = HashMap::new();
            for (key, value) in map.into_iter() {
                attribute_map.insert(key, serde_value_to_attribute_value(value)?);
            }
            Ok(AttributeValue::M(attribute_map)) // DynamoDB M type is for Map
        }
        serde_json::Value::Array(array) => {
            let mut attribute_array: Vec<AttributeValue> = Vec::new();
            for value in array.into_iter() {
                attribute_array.push(serde_value_to_attribute_value(value)?);
            }
            Ok(AttributeValue::L(attribute_array)) // DynamoDB L type is for List
        }
        unsupported => Err(DynamoItemParsingError::with_debug(
            dbg_cxt,
            "unsupported serde_json::Value type",
            format!("{:?}", unsupported),
        )),
    }
}

fn attribute_value_to_serde_value(
    value: AttributeValue,
) -> Result<serde_json::Value, GenericServerError> {
    let dbg_cxt: &'static str = "attribute_value_to_serde_value";
    match value {
        AttributeValue::Null(_) => Ok(serde_json::Value::Null),
        AttributeValue::S(s) => Ok(serde_json::Value::String(s)),
        AttributeValue::N(n) => Ok(serde_json::Value::Number(n.parse().map_err(|e| {
            DynamoItemParsingError::with_debug(dbg_cxt, "failed to parse number", format!("{}", e))
        })?)),
        AttributeValue::M(map) => {
            let mut serde_map: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
            for (key, value) in map.into_iter() {
                serde_map.insert(key, attribute_value_to_serde_value(value)?);
            }
            Ok(serde_json::Value::Object(serde_map))
        }
        AttributeValue::L(array) => {
            let mut serde_array: Vec<serde_json::Value> = Vec::new();
            for value in array.into_iter() {
                serde_array.push(attribute_value_to_serde_value(value)?);
            }
            Ok(serde_json::Value::Array(serde_array))
        }
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
        impl_dynamo_object,
        schema::{AutoFields, NestingType, PkSk, Timestamp},
        util::{AUTO_FIELDS_CREATED_AT, AUTO_FIELDS_SORT, AUTO_FIELDS_UPDATED_AT},
    };
    use aws_sdk_dynamodb::types::AttributeValue;
    use fractic_core::collection;
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use std::collections::HashMap;

    #[derive(Serialize, Deserialize, Debug, PartialEq, Default)]
    struct TestDynamoObject {
        id: Option<PkSk>,

        #[serde(flatten)]
        auto_fields: AutoFields,

        name: String,
        name_nullable: Option<String>,
        num: u32,
        float: f64,
        nested_map: HashMap<String, String>,
        nested_vec: Vec<String>,
    }

    impl_dynamo_object!(TestDynamoObject, "TEST", NestingType::Root);

    #[test]
    fn test_build_dynamo_map_copy_id_from_object() {
        let input = TestDynamoObject {
            id: Some(PkSk {
                pk: "123".to_string(),
                sk: "456".to_string(),
            }),
            auto_fields: AutoFields::default(),
            name: "Test".to_string(),
            name_nullable: None,
            num: 42,
            float: 3.14,
            nested_map: collection!(),
            nested_vec: vec![],
        };

        let output = build_dynamo_map(&input, IdKeys::CopyFromObject, None).unwrap();

        let expected_output = collection!(
            "pk".to_string() => AttributeValue::S("123".to_string()),
            "sk".to_string() => AttributeValue::S("456".to_string()),
            "name".to_string() => AttributeValue::S("Test".to_string()),
            "name_nullable".to_string() => AttributeValue::Null(true),
            "num".to_string() => AttributeValue::N("42".to_string()),
            "float".to_string() => AttributeValue::N("3.14".to_string()),
            "nested_map".to_string() => AttributeValue::M(collection!()),
            "nested_vec".to_string() => AttributeValue::L(vec![]),
        );
        assert_eq!(output, expected_output);
    }

    #[test]
    fn test_build_dynamo_map_override_id() {
        let input = TestDynamoObject {
            id: Some(PkSk {
                pk: "123".to_string(),
                sk: "456".to_string(),
            }),
            auto_fields: AutoFields::default(),
            name: "Test".to_string(),
            name_nullable: Some("TestNonNull".to_string()),
            num: 42,
            float: 3.14,
            nested_map: [("key".to_string(), "value".to_string())]
                .iter()
                .cloned()
                .collect(),
            nested_vec: vec!["elem1".to_string(), "elem2".to_string()],
        };

        let output = build_dynamo_map(
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
            "nested_vec".to_string() => AttributeValue::L(vec![
                AttributeValue::S("elem1".to_string()),
                AttributeValue::S("elem2".to_string()),
            ]),
        );
        assert_eq!(output, expected_output);
    }

    #[test]
    fn test_build_dynamo_map_no_ids() {
        let input = TestDynamoObject {
            id: Some(PkSk {
                pk: "123".to_string(),
                sk: "456".to_string(),
            }),
            name: "Test".to_string(),
            num: 42,
            float: 3.14,
            ..Default::default()
        };

        let output = build_dynamo_map(&input, IdKeys::None, None).unwrap();

        let expected_output = collection!(
            // No id fields.
            "name".to_string() => AttributeValue::S("Test".to_string()),
            "name_nullable".to_string() => AttributeValue::Null(true),
            "num".to_string() => AttributeValue::N("42".to_string()),
            "float".to_string() => AttributeValue::N("3.14".to_string()),
            "nested_map".to_string() => AttributeValue::M(collection!()),
            "nested_vec".to_string() => AttributeValue::L(vec![]),
        );
        assert_eq!(output, expected_output);
    }

    #[test]
    fn test_build_dynamo_map_auto_fields_skipped() {
        let sample_timestamp = Timestamp::now();

        let input = TestDynamoObject {
            id: Some(PkSk {
                pk: "123".to_string(),
                sk: "456".to_string(),
            }),
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
            name: "Test".to_string(),
            name_nullable: None,
            num: 42,
            float: 3.14,
            nested_map: collection!(),
            nested_vec: vec![],
        };

        let output = build_dynamo_map(&input, IdKeys::None, None).unwrap();

        let expected_output = collection!(
            // - No id fields.
            // - No auto fields.
            "name".to_string() => AttributeValue::S("Test".to_string()),
            "name_nullable".to_string() => AttributeValue::Null(true),
            "num".to_string() => AttributeValue::N("42".to_string()),
            "float".to_string() => AttributeValue::N("3.14".to_string()),
            "nested_map".to_string() => AttributeValue::M(collection!()),
            "nested_vec".to_string() => AttributeValue::L(vec![]),
        );
        assert_eq!(output, expected_output);
    }

    #[test]
    fn test_build_dynamo_map_with_overrides() {
        let sample_timestamp_1 = Timestamp::now();
        let sample_timestamp_2 = Timestamp::now();

        let input = TestDynamoObject {
            id: Some(PkSk {
                pk: "123".to_string(),
                sk: "456".to_string(),
            }),
            name: "Test".to_string(),
            num: 42,
            float: 3.14,
            ..Default::default()
        };

        let output = build_dynamo_map(
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
            "name_nullable".to_string() => AttributeValue::Null(true),
            "num".to_string() => AttributeValue::N("42".to_string()),
            "float".to_string() => AttributeValue::N("3.14".to_string()),
            "nested_map".to_string() => AttributeValue::M(collection!()),
            "nested_vec".to_string() => AttributeValue::L(vec![]),
            AUTO_FIELDS_CREATED_AT.to_string() => AttributeValue::M(collection!(
                "seconds".to_string() => AttributeValue::N(sample_timestamp_1.seconds.to_string()),
                "nanos".to_string() => AttributeValue::N(sample_timestamp_1.nanos.to_string())
            )),
            AUTO_FIELDS_UPDATED_AT.to_string() => AttributeValue::M(collection!(
                "seconds".to_string() => AttributeValue::N(sample_timestamp_2.seconds.to_string()),
                "nanos".to_string() => AttributeValue::N(sample_timestamp_2.nanos.to_string())
            )),
            AUTO_FIELDS_SORT.to_string() => AttributeValue::N("1.2345".to_string())
        );
        assert_eq!(output, expected_output);
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
            "nested_vec".to_string() => AttributeValue::L(vec![
                AttributeValue::S("elem1".to_string()),
                AttributeValue::S("elem2".to_string()),
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
            id: Some(PkSk {
                pk: "123".to_string(),
                sk: "456".to_string(),
            }),
            auto_fields: AutoFields {
                created_at: Some(sample_timestamp_1.clone()),
                updated_at: Some(sample_timestamp_2.clone()),
                sort: Some(1.2345),
                unknown_fields: collection!(
                    "unknown_field".to_string() => Value::String("unknown_value".to_string())
                ),
            },
            name: "Test".to_string(),
            name_nullable: None,
            num: 42,
            float: 3.14,
            nested_map: [("key".to_string(), "value".to_string())]
                .iter()
                .cloned()
                .collect(),
            nested_vec: vec!["elem1".to_string(), "elem2".to_string()],
        };
        assert_eq!(output, expected_output);
    }
}
