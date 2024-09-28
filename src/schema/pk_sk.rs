use std::fmt;

use fractic_generic_server_error::GenericServerError;
use serde::{
    de::{self},
    Deserialize, Deserializer, Serialize, Serializer,
};

use crate::{
    errors::DynamoInvalidId, schema::id_calculations::get_pk_sk_from_map, util::DynamoMap,
};

use super::{
    id_calculations::{
        generate_pk_sk, get_object_type, get_pk_sk_from_string, is_singleton, set_pk_sk_in_map,
    },
    DynamoObject, PkSk,
};

impl PkSk {
    pub fn root() -> PkSk {
        PkSk {
            pk: "ROOT".to_string(),
            sk: "ROOT".to_string(),
        }
    }

    pub fn generate<T: DynamoObject>(
        data: &T::Data,
        parent_id: &PkSk,
    ) -> Result<PkSk, GenericServerError> {
        let (pk, sk) = generate_pk_sk::<T>(data, &parent_id.pk, &parent_id.sk)?;
        Ok(PkSk { pk, sk })
    }

    pub fn from_string(s: &str) -> Result<PkSk, GenericServerError> {
        let dbg_cxt = "PkSk::from_string";
        serde_json::from_str(format!("\"{}\"", s).as_str()).map_err(|e| {
            DynamoInvalidId::with_debug(dbg_cxt, "Invalid PkSk string.", e.to_string())
        })
    }

    pub fn from_map(map: &DynamoMap) -> Result<PkSk, GenericServerError> {
        let (pk, sk) = get_pk_sk_from_map(map)?;
        Ok(PkSk {
            pk: pk.to_string(),
            sk: sk.to_string(),
        })
    }

    pub fn write_to_map(&self, map: &mut DynamoMap) {
        set_pk_sk_in_map(map, self.pk.to_string(), self.sk.to_string());
    }

    pub fn object_type(&self) -> Result<&str, GenericServerError> {
        get_object_type(&self.pk, &self.sk)
    }

    pub fn is_singleton(&self) -> bool {
        is_singleton(&self.pk, &self.sk)
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
        // Using from_string:
        let json_str = r#"test_pk|test_sk"#;
        let pksk = PkSk::from_string(json_str).unwrap();
        assert_eq!(pksk.pk, "test_pk");
        assert_eq!(pksk.sk, "test_sk");
        // Using Deserialize:
        let json_str = r#""test_pk|test_sk""#; // Extra quotes.
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
        let json_str = r#""invalid_format""#; // Extra quotes.
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
        assert_eq!(serialized, r#""test_pk|test_sk""#); // Extra quotes.
    }

    #[test]
    fn test_object_type() {
        let pksk = PkSk {
            pk: "PARENT_OJBECT#123abc".to_string(),
            sk: "CHILD_OBJECT#456def#NESTED_CHILD_OBJECT#789hij".to_string(),
        };
        assert_eq!(pksk.object_type().unwrap(), "NESTED_CHILD_OBJECT");
        assert!(!pksk.is_singleton());
    }

    #[test]
    fn test_object_type_singleton() {
        let pksk = PkSk {
            pk: "PARENT_OJBECT#123abc".to_string(),
            sk: "@SINGLETON[CHILD_OBJECT#456def#NESTED_CHILD_OBJECT#789hij]".to_string(),
        };
        assert_eq!(pksk.object_type().unwrap(), "SINGLETON");
        assert!(pksk.is_singleton());
    }
}
