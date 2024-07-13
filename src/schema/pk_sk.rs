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

// Tests.
// --------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_root() {
        let pksk = PkSk::root();
        assert_eq!(pksk.pk, "ROOT");
        assert_eq!(pksk.sk, "ROOT");
    }

    #[test]
    fn test_from_string_valid() {
        let json_str = r#""test_pk|test_sk""#;
        // Using from_string:
        let pksk = PkSk::from_string(json_str).unwrap();
        assert_eq!(pksk.pk, "test_pk");
        assert_eq!(pksk.sk, "test_sk");
        // Using Deserialize:
        let pksk: PkSk = serde_json::from_str(json_str).unwrap();
        assert_eq!(pksk.pk, "test_pk");
        assert_eq!(pksk.sk, "test_sk");
    }

    #[test]
    fn test_from_string_invalid() {
        // Using from_string:
        let json_str = r#"invalid_format"#;
        assert!(PkSk::from_string(json_str).is_err());
        // Using Deserialize:
        let json_str = r#""invalid_format""#;
        assert!(serde_json::from_str::<PkSk>(json_str).is_err());
    }

    #[test]
    fn test_to_string() {
        let pksk = PkSk {
            pk: "test_pk".to_string(),
            sk: "test_sk".to_string(),
        };
        // Using display:
        assert_eq!(format!("{}", pksk), "test_pk|test_sk");
        // Using Serialize:
        let serialized = serde_json::to_string(&pksk).unwrap();
        assert_eq!(serialized, r#""test_pk|test_sk""#);
    }
}
