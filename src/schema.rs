use std::collections::HashMap;

use fractic_generic_server_error::GenericServerError;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub mod id_calculations;
pub mod parsing;
pub mod pk_sk;
pub mod timestamp;
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
            fn generate_pk(&self, parent_pk: &str, parent_sk: &str, uuid: &str) -> String {
                match $nesting_type {
                    NestingType::Root => format!("ROOT"),
                    NestingType::TopLevelChild => format!("{parent_sk}"),
                    NestingType::InlineChild => format!("{parent_pk}"),
                }
            }
            fn generate_sk(&self, parent_pk: &str, parent_sk: &str, uuid: &str) -> String {
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
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
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
    #[serde(flatten, skip_serializing)] // Read-only.
    pub unknown_fields: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Timestamp {
    pub seconds: i64,
    pub nanos: u32,
}
