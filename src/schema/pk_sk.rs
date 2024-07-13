use std::fmt;

use fractic_generic_server_error::GenericServerError;
use serde::{
    de::{self},
    Deserialize, Deserializer, Serialize, Serializer,
};

use crate::errors::DynamoInvalidIdError;

use super::{id_calculations::get_pk_sk_from_string, PkSk};

impl PkSk {
    pub fn root() -> PkSk {
        PkSk {
            pk: "ROOT".to_string(),
            sk: "ROOT".to_string(),
        }
    }
    pub fn from_string(s: &str) -> Result<PkSk, GenericServerError> {
        let dbg_cxt = "PkSk::from_string";
        serde_json::from_str(s).map_err(|e| {
            DynamoInvalidIdError::with_debug(dbg_cxt, "Invalid PkSk string.", e.to_string())
        })
    }
}

impl fmt::Display for PkSk {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}|{}", self.pk, self.sk)
    }
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
