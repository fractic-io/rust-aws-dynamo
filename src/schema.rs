use std::collections::HashMap;

use fractic_generic_server_error::GenericServerError;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub mod id_calculations;
pub mod parsing;
pub mod pk_sk;
pub mod timestamp;

#[derive(Debug, PartialEq)]
pub enum IdLogic {
    // Warning:
    //   All RootChild & RootSingleton objects are stored under the 'ROOT' pk,
    //   so this key will likely get very hot (and therefore throttled) if this
    //   data frequently used. If clients need frequent access, consider ways to
    //   move the data down into more specific pk's using the other types or, if
    //   the data is fairly static, consider putting it behind a cache or using
    //   a separate access pattern outside of DynamoDB (such as S3).

    // Note:
    //   In all of the below options, the parent pk/sk may indicate a real
    //   parent object, but they don't have to. The concept of the 'parent' is
    //   used kind of like a placement indicator for the ID-generation logic, to
    //   place the new objects in a logically reasonable yet efficient location
    //   in the database.

    // Root:
    //
    // Objects are placed under 'ROOT' partition (regardless of parent IDs
    // provided).
    // ------

    // New unique object created everytime:
    //   pk: ROOT
    //   sk: LABEL#uuid
    RootChild,
    // Fixed-ID singleton object created or overwritten:
    //   pk: ROOT
    //   sk: !LABEL
    RootSingleton,

    // TopLevel:
    //
    // Objects are placed under separate partition based on parent's sk. As
    // such, the child objects require a separate query to fetch.
    // ------

    // New unique object created everytime:
    //   pk: parent.sk
    //   sk: LABEL#uuid
    TopLevelChild,
    // Fixed-ID singleton object created or overwritten:
    //   pk: parent.sk
    //   sk: !LABEL
    TopLevelSingleton,

    // Inline:
    //
    // Objects are placed under the same partition as the parent object. This
    // way the child objects can often be directly inlined into the search
    // results for the parent (by querying sk prefix).
    // ------

    // New unique object created everytime:
    //   pk: parent.pk
    //   sk: parent.sk#LABEL#uuid
    InlineChild,
    // Fixed-ID singleton object created or overwritten:
    //   pk: parent.pk
    //   sk: !LABEL:parent.sk
    InlinePrefixSingleton,
    // Fixed-ID singleton object created or overwritten:
    //   pk: parent.pk
    //   sk: parent.sk:!LABEL
    InlinePostfixSingleton,
}

pub trait DynamoObject: Serialize + DeserializeOwned + std::fmt::Debug {
    // ID calculations:
    fn pk(&self) -> Option<&str>;
    fn sk(&self) -> Option<&str>;
    fn id_label() -> &'static str;
    fn id_logic() -> IdLogic;
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
    ($type:ident, $id_label:expr, $id_logic:expr) => {
        impl DynamoObject for $type {
            fn pk(&self) -> Option<&str> {
                // All DynamoObjects should add an id field:
                //
                // id: Option<PkSk>,
                //
                self.id.as_ref().map(|pk_sk| pk_sk.pk.as_str())
            }
            fn sk(&self) -> Option<&str> {
                // All DynamoObjects should add an id field:
                //
                // id: Option<PkSk>,
                //
                self.id.as_ref().map(|pk_sk| pk_sk.sk.as_str())
            }
            fn id_label() -> &'static str {
                $id_label
            }
            fn id_logic() -> IdLogic {
                $id_logic
            }
            fn generate_pk(&self, parent_pk: &str, parent_sk: &str, _uuid: &str) -> String {
                match $id_logic {
                    IdLogic::RootChild => format!("ROOT"),
                    IdLogic::RootSingleton => format!("ROOT"),
                    IdLogic::TopLevelChild => format!("{parent_sk}"),
                    IdLogic::TopLevelSingleton => format!("{parent_sk}"),
                    IdLogic::InlineChild => format!("{parent_pk}"),
                    IdLogic::InlinePrefixSingleton => format!("{parent_pk}"),
                    IdLogic::InlinePostfixSingleton => format!("{parent_pk}"),
                }
            }
            fn generate_sk(&self, _parent_pk: &str, parent_sk: &str, uuid: &str) -> String {
                match $id_logic {
                    IdLogic::RootChild => format!("{}#{uuid}", $id_label),
                    IdLogic::RootSingleton => format!("!{}", $id_label),
                    IdLogic::TopLevelChild => format!("{}#{uuid}", $id_label),
                    IdLogic::TopLevelSingleton => format!("!{}", $id_label),
                    IdLogic::InlineChild => format!("{parent_sk}#{}#{uuid}", $id_label),
                    IdLogic::InlinePrefixSingleton => format!("!{}:{parent_sk}", $id_label),
                    IdLogic::InlinePostfixSingleton => format!("{parent_sk}:!{}", $id_label),
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
                //
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
    struct TestRootChild {
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

    #[derive(Debug, Serialize, Deserialize, Clone, Default)]
    struct TestRootSingleton {
        id: Option<PkSk>,
        #[serde(flatten)]
        auto_fields: AutoFields,
    }

    #[derive(Debug, Serialize, Deserialize, Clone, Default)]
    struct TestTopLevelSingleton {
        id: Option<PkSk>,
        #[serde(flatten)]
        auto_fields: AutoFields,
    }

    #[derive(Debug, Serialize, Deserialize, Clone, Default)]
    struct TestInlinePrefixSingleton {
        id: Option<PkSk>,
        #[serde(flatten)]
        auto_fields: AutoFields,
    }

    #[derive(Debug, Serialize, Deserialize, Clone, Default)]
    struct TestInlinePostfixSingleton {
        id: Option<PkSk>,
        #[serde(flatten)]
        auto_fields: AutoFields,
    }

    impl_dynamo_object!(TestRootChild, "ROOTCHILD", IdLogic::RootChild);
    impl_dynamo_object!(TestInlineChild, "INLINECHILD", IdLogic::InlineChild);
    impl_dynamo_object!(TestTopLevelChild, "TOPLEVELCHILD", IdLogic::TopLevelChild);
    impl_dynamo_object!(TestRootSingleton, "ROOTSINGLETON", IdLogic::RootSingleton);
    impl_dynamo_object!(
        TestTopLevelSingleton,
        "TOPLEVELSINGLETON",
        IdLogic::TopLevelSingleton
    );
    impl_dynamo_object!(
        TestInlinePrefixSingleton,
        "INLINEPREFIXSINGLETON",
        IdLogic::InlinePrefixSingleton
    );
    impl_dynamo_object!(
        TestInlinePostfixSingleton,
        "INLINEPOSTFIXSINGLETON",
        IdLogic::InlinePostfixSingleton
    );

    #[test]
    fn test_auto_fields_default() {
        let obj = TestRootChild::default();
        assert!(obj.auto_fields.created_at.is_none());
        assert!(obj.auto_fields.updated_at.is_none());
        assert!(obj.auto_fields.sort.is_none());
        assert!(obj.auto_fields.unknown_fields.is_empty());
    }

    #[test]
    fn test_id_accessors() {
        let obj_with_id = TestRootChild {
            id: Some(PkSk {
                pk: String::from("PK"),
                sk: String::from("SK"),
            }),
            auto_fields: AutoFields::default(),
        };
        let obj_without_id = TestRootChild {
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

        let obj = TestRootChild {
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
    fn test_generate_id_root_child() {
        let obj = TestRootChild {
            id: Some(PkSk {
                pk: String::from("PK"),
                sk: String::from("SK"),
            }),
            auto_fields: AutoFields::default(),
        };
        let pk = obj.generate_pk("parent_pk", "parent_sk", "uuid");
        assert_eq!(pk, "ROOT");
        let sk = obj.generate_sk("parent_pk", "parent_sk", "uuid");
        assert_eq!(sk, "ROOTCHILD#uuid");
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

    #[test]
    fn test_generate_id_root_singleton() {
        let obj = TestRootSingleton {
            id: Some(PkSk {
                pk: String::from("PK"),
                sk: String::from("SK"),
            }),
            auto_fields: AutoFields::default(),
        };
        let pk = obj.generate_pk("parent_pk", "parent_sk", "uuid");
        assert_eq!(pk, "ROOT");
        let sk = obj.generate_sk("parent_pk", "parent_sk", "uuid");
        assert_eq!(sk, "!ROOTSINGLETON");
    }

    #[test]
    fn test_generate_id_top_level_singleton() {
        let obj = TestTopLevelSingleton {
            id: Some(PkSk {
                pk: String::from("PK"),
                sk: String::from("SK"),
            }),
            auto_fields: AutoFields::default(),
        };
        let pk = obj.generate_pk("parent_pk", "parent_sk", "uuid");
        assert_eq!(pk, "parent_sk");
        let sk = obj.generate_sk("parent_pk", "parent_sk", "uuid");
        assert_eq!(sk, "!TOPLEVELSINGLETON");
    }

    #[test]
    fn test_generate_id_inline_prefix_singleton() {
        let obj = TestInlinePrefixSingleton {
            id: Some(PkSk {
                pk: String::from("PK"),
                sk: String::from("SK"),
            }),
            auto_fields: AutoFields::default(),
        };
        let pk = obj.generate_pk("parent_pk", "parent_sk", "uuid");
        assert_eq!(pk, "parent_pk");
        let sk = obj.generate_sk("parent_pk", "parent_sk", "uuid");
        assert_eq!(sk, "!INLINEPREFIXSINGLETON:parent_sk");
    }

    #[test]
    fn test_generate_id_inline_postfix_singleton() {
        let obj = TestInlinePostfixSingleton {
            id: Some(PkSk {
                pk: String::from("PK"),
                sk: String::from("SK"),
            }),
            auto_fields: AutoFields::default(),
        };
        let pk = obj.generate_pk("parent_pk", "parent_sk", "uuid");
        assert_eq!(pk, "parent_pk");
        let sk = obj.generate_sk("parent_pk", "parent_sk", "uuid");
        assert_eq!(sk, "parent_sk:!INLINEPOSTFIXSINGLETON");
    }
}
