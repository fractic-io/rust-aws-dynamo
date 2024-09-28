// Internal ID calculations. Clients should use PkSk instead.

use aws_sdk_dynamodb::types::AttributeValue;
use fractic_generic_server_error::{common::CriticalError, GenericServerError};

use crate::{
    errors::{DynamoInvalidId, DynamoInvalidParent},
    util::DynamoMap,
};

use super::{DynamoObject, IdLogic, NestingLogic};

const ALPHABET: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

fn _base62_encode(mut n: u128, num_chars: usize) -> String {
    let mut result = vec![' '; num_chars];

    for i in 0..num_chars {
        result[num_chars - 1 - i] = ALPHABET[(n % 62) as usize] as char;
        n /= 62;
    }

    result.into_iter().collect()
}

fn _uuid_16_chars() -> String {
    let uuid = uuid::Uuid::new_v4();
    _base62_encode(uuid.as_u128(), 16)
}

fn _epoch_timestamp_16_chars() -> String {
    let timestamp = chrono::Utc::now().timestamp();
    format!("{:016}", timestamp)
}

pub(crate) fn generate_pk_sk<T: DynamoObject>(
    data: &T::Data,
    parent_pk: &str,
    parent_sk: &str,
) -> Result<(String, String), GenericServerError> {
    let dbg_cxt: &'static str = "generate_pk_sk";
    // Validate parent ID:
    if is_singleton(parent_pk, parent_sk) {
        return Err(DynamoInvalidParent::new(
            dbg_cxt,
            "Singletons cannot have children.",
        ));
    }
    match T::nesting_logic() {
        NestingLogic::InlineChildOf(ptype_req) | NestingLogic::TopLevelChildOf(ptype_req) => {
            let ptype = get_object_type(parent_pk, parent_sk)?;
            if ptype != ptype_req {
                return Err(DynamoInvalidParent::with_debug(
                    dbg_cxt,
                    "",
                    format!("{} != {}", ptype, ptype_req),
                ));
            }
        }
        _ => {}
    }
    // Build pk / sk:
    let new_obj_id = match T::id_logic() {
        IdLogic::Uuid => format!("{}#{}", T::id_label(), _uuid_16_chars()),
        IdLogic::Timestamp => format!("{}#{}", T::id_label(), _epoch_timestamp_16_chars()),
        IdLogic::Singleton => format!("@{}", T::id_label()),
        IdLogic::SingletonFamily(key) => format!("@{}[{}]", T::id_label(), key(data)),
    };
    match T::nesting_logic() {
        NestingLogic::Root => Ok(("ROOT".to_string(), new_obj_id)),
        NestingLogic::TopLevelChildOf(_) | NestingLogic::TopLevelChildOfAny => {
            Ok((parent_sk.to_string(), new_obj_id))
        }
        NestingLogic::InlineChildOf(_) | NestingLogic::InlineChildOfAny => Ok((
            parent_pk.to_string(),
            format!("{}#{}", parent_sk, new_obj_id),
        )),
    }
}

pub(crate) fn is_singleton(_pk: &str, sk: &str) -> bool {
    sk.contains('@')
}

pub(crate) fn get_object_type<'a>(
    _pk: &'a str,
    sk: &'a str,
) -> Result<&'a str, GenericServerError> {
    let dbg_cxt: &'static str = "get_object_type";
    if let Some(pos) = sk.find('@') {
        // '@' indicates object is a singleton. In this case, the only label
        // that matters is the @LABEL, which can by extracted by getting the
        // text between '@' and '[' (in case of SingletonFamily) or EOL (regular
        // Singleton).
        let after_excl = &sk[pos + 1..];
        let end_pos = after_excl.find(|c| c == '[').unwrap_or(after_excl.len());
        Ok(&after_excl[..end_pos])
    } else {
        // Otherwise, object type is decided by the last label in the
        // PARENT#uuid#CHILD#uuid#... format.
        let split: Vec<&str> = sk.split('#').collect();
        if split.len() < 2 {
            Err(DynamoInvalidId::new(dbg_cxt, "sk not in LABEL#uuid format"))
        } else {
            Ok(split[split.len() - 2])
        }
    }
}

// Helper function to grab the pk/sk from a "pk|sk" string.
pub(crate) fn get_pk_sk_from_string(id: &str) -> Result<(&str, &str), GenericServerError> {
    let dbg_cxt: &'static str = "get_pk_sk_from_string";
    let split: Vec<&str> = id.split('|').collect();
    if split.len() != 2 {
        Err(DynamoInvalidId::new(dbg_cxt, "not in format pk|sk"))
    } else {
        Ok((split[0], split[1]))
    }
}

// Helper function to grab or update the pk/sk from a DynamoMap.
pub(crate) fn get_pk_sk_from_map(map: &DynamoMap) -> Result<(&str, &str), GenericServerError> {
    let dbg_cxt: &'static str = "get_pk_sk_from_map";
    let gen_err = || CriticalError::new(dbg_cxt, "DynamoMap did not contain pk/sk fields!");
    Ok((
        map.get("pk")
            .ok_or_else(|| gen_err())?
            .as_s()
            .map_err(|_| gen_err())?,
        map.get("sk")
            .ok_or_else(|| gen_err())?
            .as_s()
            .map_err(|_| gen_err())?,
    ))
}
pub(crate) fn set_pk_sk_in_map(map: &mut DynamoMap, pk: String, sk: String) {
    map.insert("pk".to_string(), AttributeValue::S(pk));
    map.insert("sk".to_string(), AttributeValue::S(sk));
}

#[cfg(test)]
mod tests {
    use aws_sdk_dynamodb::types::AttributeValue;
    use fractic_core::collection;
    use serde::{Deserialize, Serialize};

    use crate::{
        dynamo_object,
        schema::{AutoFields, DynamoObject, DynamoObjectData, PkSk},
    };

    use super::*;

    #[test]
    fn test_base62_encode_16_chars() {
        let encoded = _base62_encode(1234567890, 16);
        assert_eq!(encoded.len(), 16);
        // As the length is fixed, padding may occur; ensure the encoded string meets this condition.
        assert!(encoded.chars().all(|c| ALPHABET.contains(&(c as u8))));
    }

    #[test]
    fn test_base62_encode_20_chars() {
        let encoded = _base62_encode(1234567890, 20);
        assert_eq!(encoded.len(), 20);
        // As the length is fixed, padding may occur; ensure the encoded string meets this condition.
        assert!(encoded.chars().all(|c| ALPHABET.contains(&(c as u8))));
    }

    #[test]
    fn test_generate_uuid() {
        let uuid = _uuid_16_chars();
        assert_eq!(uuid.len(), 16);
        // Ensure the UUID is base62 encoded.
        assert!(uuid.chars().all(|c| ALPHABET.contains(&(c as u8))));
    }

    #[test]
    fn test_generate_timestamp() {
        let timestamp_1 = _epoch_timestamp_16_chars();
        std::thread::sleep(std::time::Duration::from_secs(2));
        let timestamp_2 = _epoch_timestamp_16_chars();
        assert_eq!(timestamp_1.len(), 16);
        assert_eq!(timestamp_2.len(), 16);
        assert!(timestamp_2 > timestamp_1);
    }

    #[test]
    fn test_is_singleton() {
        assert!(!is_singleton("USER#123", "ORDER#456#ITEM#789"));
        assert!(is_singleton("USER#123", "@SINGLTN"));
        assert!(is_singleton("ROOT", "@SINGLTN"));
        assert!(is_singleton("ROOT", "@SINGLTN[KEY]"));
        assert!(is_singleton("USER#123", "ORDER#56#ITEM#1#@SIGNATURE"));
    }

    #[test]
    fn test_get_object_type() {
        assert!(get_object_type("USER#123", "INVALID").is_err());
        assert_eq!(get_object_type("ROOT", "ORDER#456").unwrap(), "ORDER");
        assert_eq!(
            get_object_type("ROOT", "ORDER#456#ITEM#789").unwrap(),
            "ITEM"
        );
        assert_eq!(get_object_type("ORDER#123", "ITEM#789").unwrap(), "ITEM");
        assert_eq!(
            get_object_type("USER#123", "ORDER#456#ITEM#910").unwrap(),
            "ITEM"
        );

        // Singletons:
        assert_eq!(get_object_type("USER#123", "@SINGLTN").unwrap(), "SINGLTN");
        assert_eq!(get_object_type("ROOT", "@SINGLTN").unwrap(), "SINGLTN");
        assert_eq!(
            get_object_type("USER#456", "@PREF[ORDER#46#ITEM#7]").unwrap(),
            "PREF"
        );
        assert_eq!(
            get_object_type("USER#123", "ORDER#56#ITEM#1#@POST").unwrap(),
            "POST"
        );
        assert_eq!(
            get_object_type("USER#123", "ORDER#56#ITEM#1#@POST[key]").unwrap(),
            "POST"
        );
    }

    #[test]
    fn test_get_pk_sk_from_string() {
        // Valid:
        match get_pk_sk_from_string("USER#123#CHILD#456|ORDER#456#ITEM#abc1234DEF") {
            Ok((pk, sk)) => {
                assert_eq!(pk, "USER#123#CHILD#456");
                assert_eq!(sk, "ORDER#456#ITEM#abc1234DEF");
            }
            Err(e) => panic!("Expected Ok, got Err: {}", e),
        }

        // Invalid:
        match get_pk_sk_from_string("invalid_format_string") {
            Ok(_) => panic!("Expected Err, got Ok"),
            Err(e) => assert!(e.to_string().contains("not in format pk|sk")),
        }
    }

    #[test]
    fn test_get_pk_sk_from_map() {
        // With required fields:
        let map = collection!(
            "pk".to_string() => AttributeValue::S("USER#123#CHILD#456".to_string()),
            "sk".to_string() => AttributeValue::S("ORDER#456#ITEM#abc1234DEF".to_string())
        );
        match get_pk_sk_from_map(&map) {
            Ok((pk, sk)) => {
                assert_eq!(pk, "USER#123#CHILD#456");
                assert_eq!(sk, "ORDER#456#ITEM#abc1234DEF");
            }
            Err(e) => panic!("Expected Ok, got Err: {}", e),
        }

        // Without required fields:
        let invalid_map = collection!(
            "pk".to_string() => AttributeValue::S("USER#123#CHILD#456".to_string()),
        );
        match get_pk_sk_from_map(&invalid_map) {
            Ok(_) => panic!("Expected Err, got Ok"),
            Err(e) => assert!(e.to_string().contains("did not contain pk/sk fields")),
        }
    }

    // ID generation logic.
    // --------------------------------------------------

    // Test case 1: NestingLogic::Root with IdLogic::Uuid
    #[derive(Debug, Serialize, Deserialize, Default)]
    pub struct TestObjectRootUuidData {}
    dynamo_object!(
        TestObjectRootUuid,
        TestObjectRootUuidData,
        "TEST",
        IdLogic::Uuid,
        NestingLogic::Root
    );

    #[test]
    fn test_generate_pk_sk_root_uuid() {
        let obj = TestObjectRootUuid {
            id: PkSk::root(),
            auto_fields: AutoFields::default(),
            data: TestObjectRootUuidData::default(),
        };
        let parent_pk = "any_pk";
        let parent_sk = "any_sk";
        let result = generate_pk_sk::<TestObjectRootUuid>(&obj.data, parent_pk, parent_sk).unwrap();
        assert_eq!(result.0, "ROOT");
        assert!(result.1.starts_with("TEST#"));
        assert_eq!(result.1.len(), "TEST#".len() + 16);
    }

    // Test case 2: NestingLogic::Root with IdLogic::Timestamp
    #[derive(Debug, Serialize, Deserialize, Default)]
    pub struct TestObjectRootTimestampData {}
    dynamo_object!(
        TestObjectRootTimestamp,
        TestObjectRootTimestampData,
        "TEST",
        IdLogic::Timestamp,
        NestingLogic::Root
    );

    #[test]
    fn test_generate_pk_sk_root_timestamp() {
        let obj = TestObjectRootTimestamp {
            id: PkSk::root(),
            auto_fields: AutoFields::default(),
            data: TestObjectRootTimestampData::default(),
        };
        let parent_pk = "any_pk";
        let parent_sk = "any_sk";
        let result =
            generate_pk_sk::<TestObjectRootTimestamp>(&obj.data, parent_pk, parent_sk).unwrap();
        assert_eq!(result.0, "ROOT");
        assert!(result.1.starts_with("TEST#"));
        assert_eq!(result.1.len(), "TEST#".len() + 16);
    }

    // Test case 3: NestingLogic::TopLevelChildOfAny with IdLogic::Uuid
    #[derive(Debug, Serialize, Deserialize, Default)]
    pub struct TestObjectTopLevelChildUuidData {}
    dynamo_object!(
        TestObjectTopLevelChildUuid,
        TestObjectTopLevelChildUuidData,
        "TEST",
        IdLogic::Uuid,
        NestingLogic::TopLevelChildOfAny
    );

    #[test]
    fn test_generate_pk_sk_top_level_child_uuid() {
        let obj = TestObjectTopLevelChildUuid {
            id: PkSk::root(),
            auto_fields: AutoFields::default(),
            data: TestObjectTopLevelChildUuidData::default(),
        };
        let parent_pk = "parent_pk";
        let parent_sk = "parent_sk";
        let result =
            generate_pk_sk::<TestObjectTopLevelChildUuid>(&obj.data, parent_pk, parent_sk).unwrap();
        assert_eq!(result.0, parent_sk);
        assert!(result.1.starts_with("TEST#"));
        assert_eq!(result.1.len(), "TEST#".len() + 16);
    }

    // Test case 4: NestingLogic::InlineChildOfAny with IdLogic::Uuid
    #[derive(Debug, Serialize, Deserialize, Default)]
    pub struct TestObjectInlineChildUuidData {}
    dynamo_object!(
        TestObjectInlineChildUuid,
        TestObjectInlineChildUuidData,
        "TEST",
        IdLogic::Uuid,
        NestingLogic::InlineChildOfAny
    );

    #[test]
    fn test_generate_pk_sk_inline_child_uuid() {
        let obj = TestObjectInlineChildUuid {
            id: PkSk::root(),
            auto_fields: AutoFields::default(),
            data: TestObjectInlineChildUuidData::default(),
        };
        let parent_pk = "parent_pk";
        let parent_sk = "parent_sk";
        let result =
            generate_pk_sk::<TestObjectInlineChildUuid>(&obj.data, parent_pk, parent_sk).unwrap();
        assert_eq!(result.0, parent_pk);
        let expected_sk_prefix = format!("{}#TEST#", parent_sk);
        assert!(result.1.starts_with(&expected_sk_prefix));
        let expected_length = parent_sk.len() + 1 + "TEST#".len() + 16;
        assert_eq!(result.1.len(), expected_length);
    }

    // Test case 5: Singleton parent cannot have children
    #[test]
    fn test_generate_pk_sk_singleton_parent_error() {
        let obj = TestObjectTopLevelChildUuid {
            id: PkSk::root(),
            auto_fields: AutoFields::default(),
            data: TestObjectTopLevelChildUuidData::default(),
        };
        let parent_pk = "any_pk";
        let parent_sk = "@PARENT";
        let result = generate_pk_sk::<TestObjectTopLevelChildUuid>(&obj.data, parent_pk, parent_sk);
        assert!(result.is_err());
        if let Err(err) = result {
            let err_msg = err.to_string();
            assert!(err_msg.contains("Singletons cannot have children."));
        } else {
            panic!("Expected error but got Ok");
        }
    }

    // Test case 6: NestingLogic::TopLevelChildOf("PARENT") with matching parent type
    #[derive(Debug, Serialize, Deserialize, Default)]
    pub struct TestObjectTopLevelChildOfParentData {}
    dynamo_object!(
        TestObjectTopLevelChildOfParent,
        TestObjectTopLevelChildOfParentData,
        "CHILD",
        IdLogic::Uuid,
        NestingLogic::TopLevelChildOf("PARENT")
    );

    #[test]
    fn test_generate_pk_sk_top_level_child_of_parent() {
        let obj = TestObjectTopLevelChildOfParent {
            id: PkSk::root(),
            auto_fields: AutoFields::default(),
            data: TestObjectTopLevelChildOfParentData::default(),
        };
        let parent_pk = "any_pk";
        let parent_sk = "PARENT#1234567890123456";
        let result =
            generate_pk_sk::<TestObjectTopLevelChildOfParent>(&obj.data, parent_pk, parent_sk)
                .unwrap();
        assert_eq!(result.0, parent_sk);
        assert!(result.1.starts_with("CHILD#"));
        assert_eq!(result.1.len(), "CHILD#".len() + 16);
    }

    // Test case 7: NestingLogic::TopLevelChildOf("PARENT") with non-matching parent type
    #[test]
    fn test_generate_pk_sk_top_level_child_of_parent_error() {
        let obj = TestObjectTopLevelChildOfParent {
            id: PkSk::root(),
            auto_fields: AutoFields::default(),
            data: TestObjectTopLevelChildOfParentData::default(),
        };
        let parent_pk = "any_pk";
        let parent_sk = "NOTPARENT#1234567890123456";
        let result =
            generate_pk_sk::<TestObjectTopLevelChildOfParent>(&obj.data, parent_pk, parent_sk);
        assert!(result.is_err());
        if let Err(err) = result {
            let err_msg = err.to_string();
            assert!(err_msg.contains("NOTPARENT != PARENT"));
        } else {
            panic!("Expected error but got Ok");
        }
    }

    // Test case 8: IdLogic::Singleton
    #[derive(Debug, Serialize, Deserialize, Default)]
    pub struct TestObjectSingletonData {}
    dynamo_object!(
        TestObjectSingleton,
        TestObjectSingletonData,
        "SINGLETON",
        IdLogic::Singleton,
        NestingLogic::Root
    );

    #[test]
    fn test_generate_pk_sk_singleton() {
        let obj = TestObjectSingleton {
            id: PkSk::root(),
            auto_fields: AutoFields::default(),
            data: TestObjectSingletonData::default(),
        };
        let parent_pk = "any_pk";
        let parent_sk = "any_sk";
        let result =
            generate_pk_sk::<TestObjectSingleton>(&obj.data, parent_pk, parent_sk).unwrap();
        assert_eq!(result.0, "ROOT");
        assert_eq!(result.1, "@SINGLETON");
    }

    // Test case 9: IdLogic::SingletonFamily
    #[derive(Debug, Serialize, Deserialize, Default)]
    pub struct TestObjectSingletonFamilyData {
        key_field: String,
    }
    dynamo_object!(
        TestObjectSingletonFamily,
        TestObjectSingletonFamilyData,
        "FAMILY",
        IdLogic::SingletonFamily(Box::new(|obj: &TestObjectSingletonFamilyData| obj
            .key_field
            .clone())),
        NestingLogic::Root
    );

    #[test]
    fn test_generate_pk_sk_singleton_family() {
        let obj = TestObjectSingletonFamily {
            id: PkSk::root(),
            auto_fields: AutoFields::default(),
            data: TestObjectSingletonFamilyData {
                key_field: "key123".to_string(),
            },
        };
        let parent_pk = "any_pk";
        let parent_sk = "any_sk";
        let result =
            generate_pk_sk::<TestObjectSingletonFamily>(&obj.data, parent_pk, parent_sk).unwrap();
        assert_eq!(result.0, "ROOT");
        assert_eq!(result.1, "@FAMILY[key123]");
    }

    // Test case 10: Invalid parent_sk format
    #[test]
    fn test_generate_pk_sk_invalid_parent_sk_format() {
        let obj = TestObjectTopLevelChildOfParent {
            id: PkSk::root(),
            auto_fields: AutoFields::default(),
            data: TestObjectTopLevelChildOfParentData::default(),
        };
        let parent_pk = "any_pk";
        let parent_sk = "INVALIDFORMAT";
        let result =
            generate_pk_sk::<TestObjectTopLevelChildOfParent>(&obj.data, parent_pk, parent_sk);
        assert!(result.is_err());
        if let Err(err) = result {
            let err_msg = err.to_string();
            assert!(err_msg.contains("sk not in LABEL#uuid format"));
        } else {
            panic!("Expected error but got Ok");
        }
    }
}
