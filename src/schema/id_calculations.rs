use aws_sdk_dynamodb::types::AttributeValue;
use fractic_generic_server_error::{common::CriticalError, GenericServerError};

use crate::{errors::DynamoInvalidIdError, util::DynamoMap};

const ALPHABET: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
const NUM_CHARACTERS_TO_GENERATE: usize = 16;

fn _base62_encode(mut n: u128) -> String {
    let mut result = vec![' '; NUM_CHARACTERS_TO_GENERATE];

    for i in 0..NUM_CHARACTERS_TO_GENERATE {
        result[NUM_CHARACTERS_TO_GENERATE - 1 - i] = ALPHABET[(n % 62) as usize] as char;
        n /= 62;
    }

    result.into_iter().collect()
}

pub fn generate_uuid() -> String {
    let uuid = uuid::Uuid::new_v4();
    _base62_encode(uuid.as_u128())
}

pub fn get_object_type<'a>(pk: &'a str, sk: &'a str) -> &'a str {
    if get_last_id_label(sk) == "ROOT" {
        get_last_id_label(pk)
    } else {
        get_last_id_label(sk)
    }
}

pub fn get_last_id_label(id: &str) -> &str {
    let split: Vec<&str> = id.split('#').collect();
    if split.len() < 2 {
        "ROOT"
    } else {
        split[split.len() - 2]
    }
}

// Helper function to grab the pk/sk from a "pk|sk" string.
pub fn get_pk_sk_from_string(id: &str) -> Result<(&str, &str), GenericServerError> {
    let dbg_cxt: &'static str = "get_pk_sk_from_string";
    let split: Vec<&str> = id.split('|').collect();
    if split.len() != 2 {
        Err(DynamoInvalidIdError::new(dbg_cxt, "not in format pk|sk"))
    } else {
        Ok((split[0], split[1]))
    }
}

// Helper function to grab or update the pk/sk from a DynamoMap.
pub fn get_pk_sk_from_map(map: &DynamoMap) -> Result<(&str, &str), GenericServerError> {
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
pub fn set_pk_sk_in_map(map: &mut DynamoMap, pk: &str, sk: &str) {
    map.insert("pk".to_string(), AttributeValue::S(pk.to_string()));
    map.insert("sk".to_string(), AttributeValue::S(sk.to_string()));
}

#[cfg(test)]
mod tests {
    use aws_sdk_dynamodb::types::AttributeValue;
    use fractic_core::collection;

    use super::*;

    #[test]
    fn test_base62_encode() {
        let encoded = _base62_encode(1234567890);
        assert_eq!(encoded.len(), NUM_CHARACTERS_TO_GENERATE);
        // As the length is fixed, padding may occur; ensure the encoded string meets this condition.
        assert!(encoded.chars().all(|c| ALPHABET.contains(&(c as u8))));
    }

    #[test]
    fn test_generate_uuid() {
        let uuid = generate_uuid();
        assert_eq!(uuid.len(), NUM_CHARACTERS_TO_GENERATE);
        // Ensure the UUID is base62 encoded.
        assert!(uuid.chars().all(|c| ALPHABET.contains(&(c as u8))));
    }

    #[test]
    fn test_get_object_type() {
        assert_eq!(get_object_type("USER#123", "ROOT"), "USER");
        assert_eq!(get_object_type("ROOT", "ORDER#456"), "ORDER");
        assert_eq!(get_object_type("ROOT", "ORDER#456#ITEM#789"), "ITEM");
        assert_eq!(get_object_type("ORDER#123", "ITEM#789"), "ITEM");
        assert_eq!(get_object_type("USER#123", "ORDER#456#ITEM#910"), "ITEM");
    }

    #[test]
    fn test_get_last_id_label() {
        assert_eq!(get_last_id_label("USER#123"), "USER");
        assert_eq!(get_last_id_label("USER#123#ORDER#456"), "ORDER");
        assert_eq!(get_last_id_label("ROOT"), "ROOT");
        assert_eq!(get_last_id_label("USER#123#ORDER#456#ITEM#789"), "ITEM");
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
}
