use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

use super::id_calculations::get_pk_sk_from_string;

// Custom struct to hold 'pk' and 'sk', which gets serialized and deserialized
// as "pk|sk" in communication with downstream clients, but are separate
// properties in the underlying data store (primary_key and sort_key).
#[derive(Debug)]
pub struct PkSk {
    pub pk: String,
    pub sk: String,
}

impl Serialize for PkSk {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        format!("{}|{}", self.pk, self.sk).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PkSk {
    fn deserialize<D>(deserializer: D) -> Result<PkSk, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let (pk, sk) = get_pk_sk_from_string(&s).map_err(|e| de::Error::custom(e))?;
        Ok(PkSk {
            pk: pk.to_string(),
            sk: sk.to_string(),
        })
    }
}
