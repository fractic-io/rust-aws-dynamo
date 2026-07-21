use std::{fmt, sync::LazyLock};

use aws_sdk_dynamodb::types::AttributeValue;
use fractic_server_error::ServerError;
use serde::{
    de::{self},
    Deserialize, Deserializer, Serialize, Serializer,
};

use crate::{errors::DynamoInvalidId, util::DynamoMap};

use super::{
    identifiers::{generate_id, uuid_v7_lower_bound, uuid_v7_upper_bound, RawIdPath, ROOT_KEY},
    DynamoObject, ForeignRef, PkSk,
};

static ROOT: LazyLock<PkSk> = LazyLock::new(|| PkSk {
    pk: ROOT_KEY.to_string(),
    sk: ROOT_KEY.to_string(),
});

impl PkSk {
    /// The canonical placement handle for root-level objects.
    pub fn root() -> &'static PkSk {
        &ROOT
    }

    /// Generates a new logical ID using `T`'s configured ID and nesting rules.
    pub fn generate<T: DynamoObject>(
        data: &T::Data,
        parent_id: &PkSk,
    ) -> Result<PkSk, ServerError> {
        generate_id::<T>(data, parent_id)
    }

    /// Constructs the first possible ID for a UUID-v7 object at the given Unix
    /// epoch millisecond.
    ///
    /// This can be used as the lower key in a UUID-v7 timestamp range query.
    pub fn uuid_v7_lower_bound<T: DynamoObject>(
        parent_id: &PkSk,
        timestamp_millis: i64,
    ) -> Result<PkSk, ServerError> {
        uuid_v7_lower_bound::<T>(parent_id, timestamp_millis)
    }

    /// Constructs the last possible ID for a UUID-v7 object at the given Unix
    /// epoch millisecond.
    ///
    /// This can be used as the inclusive upper key in a UUID-v7 timestamp
    /// range query.
    pub fn uuid_v7_upper_bound<T: DynamoObject>(
        parent_id: &PkSk,
        timestamp_millis: i64,
    ) -> Result<PkSk, ServerError> {
        uuid_v7_upper_bound::<T>(parent_id, timestamp_millis)
    }

    /// Parses the client-facing `pk|sk` representation.
    pub fn from_string(s: &str) -> Result<PkSk, ServerError> {
        let (pk, sk) = split_serialized_id(s)?;
        Ok(PkSk {
            pk: pk.to_string(),
            sk: sk.to_string(),
        })
    }

    pub fn from_map(map: &DynamoMap) -> Result<PkSk, ServerError> {
        let (pk, sk) = id_fields_from_map(map)?;
        Ok(PkSk {
            pk: pk.to_string(),
            sk: sk.to_string(),
        })
    }

    pub fn write_to_map(&self, map: &mut DynamoMap) {
        map.insert("pk".to_string(), AttributeValue::S(self.pk.clone()));
        map.insert("sk".to_string(), AttributeValue::S(self.sk.clone()));
    }

    /// Returns the terminal object's configured ID label.
    pub fn object_type(&self) -> Result<&str, ServerError> {
        RawIdPath::new(&self.sk).object_label()
    }

    /// Whether the terminal object uses singleton ID syntax.
    pub fn is_singleton(&self) -> bool {
        RawIdPath::new(&self.sk).is_singleton()
    }

    pub fn build_ref<'a>(&'a self) -> ForeignRef<'a> {
        ForeignRef::from(self)
    }

    pub fn into_ref(self) -> ForeignRef<'static> {
        ForeignRef::from(self)
    }
}

pub(crate) fn id_fields_from_map(map: &DynamoMap) -> Result<(&str, &str), ServerError> {
    let field = |name| {
        map.get(name)
            .and_then(|value| value.as_s().ok())
            .map(String::as_str)
            .ok_or_else(|| {
                DynamoInvalidId::with_debug(
                    "DynamoDB item did not contain both string `pk` and `sk` fields",
                    &name,
                )
            })
    };
    Ok((field("pk")?, field("sk")?))
}

fn split_serialized_id(id: &str) -> Result<(&str, &str), ServerError> {
    let (pk, sk) = id
        .split_once('|')
        .ok_or_else(|| DynamoInvalidId::with_debug("ID was not in `pk|sk` format", &id))?;
    if pk.is_empty() || sk.is_empty() {
        return Err(DynamoInvalidId::with_debug(
            "ID contained an empty partition key or sort key",
            &id,
        ));
    }
    Ok((pk, sk))
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
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for PkSk {
    fn deserialize<D>(deserializer: D) -> Result<PkSk, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut pk = String::deserialize(deserializer)?;
        let separator = pk.find('|').ok_or_else(|| {
            de::Error::custom(DynamoInvalidId::with_debug(
                "ID was not in `pk|sk` format",
                &pk,
            ))
        })?;
        let sk = pk.split_off(separator + 1);
        pk.truncate(separator);
        if pk.is_empty() || sk.is_empty() {
            return Err(de::Error::custom(DynamoInvalidId::with_debug(
                "ID contained an empty partition key or sort key",
                &format!("{pk}|{sk}"),
            )));
        }
        Ok(PkSk { pk, sk })
    }
}

// Tests.
// --------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

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
        let json_str = r"test_pk|test_sk";
        let pksk = PkSk::from_string(json_str).unwrap();
        assert_eq!(pksk.pk, "test_pk");
        assert_eq!(pksk.sk, "test_sk");
        // Using Deserialize:
        let json_str = r#""test_pk|test_sk""#; // Extra quotes.
        let pksk: PkSk = serde_json::from_str(json_str).unwrap();
        assert_eq!(pksk.pk, "test_pk");
        assert_eq!(pksk.sk, "test_sk");

        let pksk = PkSk::from_string(r#"test_pk|sort|"key"#).unwrap();
        assert_eq!(pksk.pk, "test_pk");
        assert_eq!(pksk.sk, r#"sort|"key"#);
    }

    #[test]
    fn test_from_string_invalid() {
        // Using from_string:
        let json_str = r"invalid_format";
        assert!(PkSk::from_string(json_str).is_err());
        // Using Deserialize:
        let json_str = r#""invalid_format""#; // Extra quotes.
        assert!(serde_json::from_str::<PkSk>(json_str).is_err());
        assert!(PkSk::from_string("|test_sk").is_err());
        assert!(PkSk::from_string("test_pk|").is_err());
    }

    #[test]
    fn test_map_round_trip_and_validation() {
        let id = PkSk {
            pk: "test_pk".into(),
            sk: "TEST#1".into(),
        };
        let mut map = HashMap::new();
        id.write_to_map(&mut map);
        assert_eq!(PkSk::from_map(&map).unwrap(), id);

        map.insert("sk".into(), AttributeValue::N("1".into()));
        assert!(PkSk::from_map(&map).is_err());
    }

    #[test]
    fn test_to_string() {
        let pksk = PkSk {
            pk: "test_pk".to_string(),
            sk: "test_sk".to_string(),
        };
        // Using display:
        assert_eq!(format!("{pksk}"), "test_pk|test_sk");
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
            sk: "@SINGLETON[user@example.com]".to_string(),
        };
        assert_eq!(pksk.object_type().unwrap(), "SINGLETON");
        assert!(pksk.is_singleton());
    }
}
