use std::collections::HashMap;

use fractic_generic_server_error::GenericServerError;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub mod id_calculations;
pub mod parsing;
pub mod pk_sk;
pub mod timestamp;

#[derive(Debug, PartialEq)]
pub enum NestingType {
    Root,
    InlineChild,
    TopLevelChild,
}

pub trait DynamoObject: Serialize + DeserializeOwned + std::fmt::Debug {
    // ID calculations:
    fn pk(&self) -> Option<&str>;
    fn sk(&self) -> Option<&str>;
    fn id_label() -> &'static str;
    fn nesting_type() -> NestingType;
    fn generate_pk(&self, parent_pk: &str, parent_sk: &str, uuid: &str) -> String;
    fn generate_sk(&self, parent_pk: &str, parent_sk: &str, uuid: &str) -> String;

    // Shorthand ID accessors:
    fn id_or_critical(&self) -> Result<&PkSk, GenericServerError>;
    fn pk_or_critical(&self) -> Result<&str, GenericServerError> {
        self.id_or_critical().map(|id| id.pk.as_str())
    }
    fn sk_or_critical(&self) -> Result<&str, GenericServerError> {
        self.id_or_critical().map(|id| id.sk.as_str())
    }

    // Auto-fields accessors:
    fn auto_fields(&self) -> &AutoFields;
    fn created_at(&self) -> Option<&Timestamp> {
        self.auto_fields().created_at.as_ref()
    }
    fn updated_at(&self) -> Option<&Timestamp> {
        self.auto_fields().updated_at.as_ref()
    }
    fn sort(&self) -> Option<f64> {
        self.auto_fields().sort
    }
    fn has_unknown_fields(&self) -> bool {
        !self.auto_fields().unknown_fields.is_empty()
    }
    fn unknown_field_keys(&self) -> Vec<&String> {
        self.auto_fields().unknown_fields.keys().collect()
    }
}

#[macro_export]
macro_rules! impl_dynamo_object {
    ($type:ident, $id_label:expr, $nesting_type:expr) => {
        impl DynamoObject for $type {
            fn pk(&self) -> Option<&str> {
                self.id.as_ref().map(|pk_sk| pk_sk.pk.as_str())
            }
            fn sk(&self) -> Option<&str> {
                self.id.as_ref().map(|pk_sk| pk_sk.sk.as_str())
            }
            fn id_label() -> &'static str {
                $id_label
            }
            fn nesting_type() -> NestingType {
                $nesting_type
            }
            fn generate_pk(&self, parent_pk: &str, parent_sk: &str, _uuid: &str) -> String {
                match $nesting_type {
                    NestingType::Root => format!("ROOT"),
                    NestingType::TopLevelChild => format!("{parent_sk}"),
                    NestingType::InlineChild => format!("{parent_pk}"),
                }
            }
            fn generate_sk(&self, _parent_pk: &str, parent_sk: &str, uuid: &str) -> String {
                match $nesting_type {
                    NestingType::Root => format!("{}#{uuid}", $id_label),
                    NestingType::TopLevelChild => format!("{}#{uuid}", $id_label),
                    NestingType::InlineChild => format!("{parent_sk}#{}#{uuid}", $id_label),
                }
            }

            fn id_or_critical(
                &self,
            ) -> Result<&PkSk, fractic_generic_server_error::GenericServerError> {
                let dbg_cxt: &'static str = "id_or_critical";
                Ok(self.id.as_ref().ok_or_else(|| {
                    fractic_generic_server_error::common::CriticalError::with_debug(
                        dbg_cxt,
                        "DynamoObject did not have id.",
                        Self::id_label().to_string(),
                    )
                })?)
            }

            fn auto_fields(&self) -> &AutoFields {
                // All DynamoObjects should add a flattened auto_fields field:
                //
                // #[serde(flatten)]
                // pub auto_fields: AutoFields,
                &self.auto_fields
            }
        }
    };
}

// Standard sub-structs:
// ---------------------------

// Custom struct to hold 'pk' and 'sk', which gets serialized and deserialized
// as "pk|sk" in communication with downstream clients, but are separate
// properties in the underlying data store (primary_key and sort_key).
#[derive(Debug, PartialEq, Clone, Hash, Eq)]
pub struct PkSk {
    pub pk: String,
    pub sk: String,
}

// Fields automatically populated by DynamoUtil. This struct should be included
// in all DynamoObjects as a flattened field:
//
// #[serde(flatten)]
// pub auto_fields: AutoFields,
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq)]
pub struct AutoFields {
    // Since these are manually handled by DynamoUtil, they should be read-only.
    // This is can be accomplished by skipping serialization, since it will
    // cause these fields to be skipped when converting to the DynomoMap. This
    // also means they will not be included when an object is converted to JSON
    // (ex. when this object is sent to a client by an API), so if they are
    // needed they should be manually read and included using the accessors.
    #[serde(skip_serializing)] // Read-only.
    pub created_at: Option<Timestamp>,
    #[serde(skip_serializing)] // Read-only.
    pub updated_at: Option<Timestamp>,
    #[serde(skip_serializing)] // Read-only.
    pub sort: Option<f64>,
    #[serde(flatten, skip_serializing)] // Read-only.
    pub unknown_fields: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Timestamp {
    pub seconds: i64,
    pub nanos: u32,
}

// Tests.
// ---------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    #[derive(Debug, Serialize, Deserialize, Clone, Default)]
    struct TestMain {
        id: Option<PkSk>,
        #[serde(flatten)]
        auto_fields: AutoFields,
    }

    #[derive(Debug, Serialize, Deserialize, Clone, Default)]
    struct TestInlineChild {
        id: Option<PkSk>,
        #[serde(flatten)]
        auto_fields: AutoFields,
    }

    #[derive(Debug, Serialize, Deserialize, Clone, Default)]
    struct TestTopLevelChild {
        id: Option<PkSk>,
        #[serde(flatten)]
        auto_fields: AutoFields,
    }

    impl_dynamo_object!(TestMain, "TEST", NestingType::Root);
    impl_dynamo_object!(TestInlineChild, "INLINECHILD", NestingType::InlineChild);
    impl_dynamo_object!(
        TestTopLevelChild,
        "TOPLEVELCHILD",
        NestingType::TopLevelChild
    );

    #[test]
    fn test_auto_fields_default() {
        let obj = TestMain::default();
        assert!(obj.auto_fields.created_at.is_none());
        assert!(obj.auto_fields.updated_at.is_none());
        assert!(obj.auto_fields.sort.is_none());
        assert!(obj.auto_fields.unknown_fields.is_empty());
    }

    #[test]
    fn test_id_accessors() {
        let obj_with_id = TestMain {
            id: Some(PkSk {
                pk: String::from("PK"),
                sk: String::from("SK"),
            }),
            auto_fields: AutoFields::default(),
        };
        let obj_without_id = TestMain {
            id: None,
            auto_fields: AutoFields::default(),
        };

        assert!(obj_with_id.id_or_critical().is_ok());
        assert!(obj_without_id.id_or_critical().is_err());
        assert_eq!(obj_with_id.pk_or_critical().unwrap(), "PK");
        assert!(obj_without_id.pk_or_critical().is_err());
        assert_eq!(obj_with_id.sk_or_critical().unwrap(), "SK");
        assert!(obj_without_id.sk_or_critical().is_err());
    }

    #[test]
    fn test_auto_fields_accessors() {
        let mut unknown_fields = HashMap::new();
        unknown_fields.insert(String::from("key"), json!("value"));
        let auto_fields = AutoFields {
            created_at: Some(Timestamp {
                seconds: 1625247600,
                nanos: 0,
            }),
            updated_at: Some(Timestamp {
                seconds: 1625247601,
                nanos: 0,
            }),
            sort: Some(1.0),
            unknown_fields,
        };

        let obj = TestMain {
            id: None,
            auto_fields: auto_fields.clone(),
        };

        assert_eq!(obj.created_at().unwrap().seconds, 1625247600);
        assert_eq!(obj.updated_at().unwrap().seconds, 1625247601);
        assert_eq!(obj.sort().unwrap(), 1.0);
        assert!(obj.has_unknown_fields());
        assert_eq!(obj.unknown_field_keys(), vec![&String::from("key")]);
    }

    #[test]
    fn test_generate_id_main() {
        let obj = TestMain {
            id: Some(PkSk {
                pk: String::from("PK"),
                sk: String::from("SK"),
            }),
            auto_fields: AutoFields::default(),
        };
        let pk = obj.generate_pk("parent_pk", "parent_sk", "uuid");
        assert_eq!(pk, "ROOT");
        let sk = obj.generate_sk("parent_pk", "parent_sk", "uuid");
        assert_eq!(sk, "TEST#uuid");
    }

    #[test]
    fn test_generate_id_inline_child() {
        let obj = TestInlineChild {
            id: Some(PkSk {
                pk: String::from("PK"),
                sk: String::from("SK"),
            }),
            auto_fields: AutoFields::default(),
        };
        let pk = obj.generate_pk("parent_pk", "parent_sk", "uuid");
        assert_eq!(pk, "parent_pk");
        let sk = obj.generate_sk("parent_pk", "parent_sk", "uuid");
        assert_eq!(sk, "parent_sk#INLINECHILD#uuid");
    }

    #[test]
    fn test_generate_id_top_level_child() {
        let obj = TestTopLevelChild {
            id: Some(PkSk {
                pk: String::from("PK"),
                sk: String::from("SK"),
            }),
            auto_fields: AutoFields::default(),
        };
        let pk = obj.generate_pk("parent_pk", "parent_sk", "uuid");
        assert_eq!(pk, "parent_sk");
        let sk = obj.generate_sk("parent_pk", "parent_sk", "uuid");
        assert_eq!(sk, "TOPLEVELCHILD#uuid");
    }
}
