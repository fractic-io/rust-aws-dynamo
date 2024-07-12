use fractic_generic_server_error::{common::CriticalError, GenericServerError};

use crate::{errors::DynamoInvalidIdError, util::DynamoMap};

const ALPHABET: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
const NUM_CHARACTERS_TO_GENERATE: usize = 16;

// Ordered IDs.
//
// Ordering of items with 'ordered IDs' is based on lexicographical sort, so the
// amount of digits is important.
//
// - If we add to the end, we add the 'gap' to the current largest ID.
// - If we add to the beginning, we subtract the 'gap' from the current smallest ID.
// - If we add in the middle, we take the average of the two IDs.
//
// Choose a middle gap such that, if we keep adding items at the end, we have
// plenty of space to grow, yet large enough to be able to insert items in the
// middle without running out of possible values.
pub(crate) const ORDERED_IDS_DIGITS: usize = 9;
pub(crate) const ORDERED_IDS_INIT: u32 = 2000000000;
pub(crate) const ORDERED_IDS_DEFAULT_GAP: u32 = 0000010000;

fn _base62_encode(mut n: u128) -> String {
    let mut result = vec![' '; NUM_CHARACTERS_TO_GENERATE];

    for i in 0..NUM_CHARACTERS_TO_GENERATE {
        result[NUM_CHARACTERS_TO_GENERATE - 1 - i] = ALPHABET[(n % 62) as usize] as char;
        n /= 62;
    }

    result.into_iter().collect()
}

pub fn generate_id() -> String {
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

// Helper function to grab the pk/sk from a DynamoMap.
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
