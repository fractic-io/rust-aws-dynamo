use fractic_generic_server_error::{common::CriticalError, GenericServerError};
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
    fn pk(&self) -> Option<&str>;
    fn pk_or_critical(&self) -> Result<&str, GenericServerError> {
        let dbg_cxt: &'static str = "pk_or_critical";
        Ok(self.pk().ok_or_else(|| {
            CriticalError::with_debug(
                dbg_cxt,
                "DynamoObject did not have pk!",
                Self::id_label().to_string(),
            )
        })?)
    }
    fn sk(&self) -> Option<&str>;
    fn sk_or_critical(&self) -> Result<&str, GenericServerError> {
        let dbg_cxt: &'static str = "sk_or_critical";
        Ok(self.sk().ok_or_else(|| {
            CriticalError::with_debug(
                dbg_cxt,
                "DynamoObject did not have sk!",
                Self::id_label().to_string(),
            )
        })?)
    }
    fn id_label() -> &'static str;
    fn generate_pk(&self, parent_pk: &str, parent_sk: &str, new_id: &str) -> String;
    fn generate_sk(&self, parent_pk: &str, parent_sk: &str, new_id: &str) -> String;
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
            fn generate_pk(&self, parent_pk: &str, parent_sk: &str, new_id: &str) -> String {
                match $nesting_type {
                    NestingType::Root => format!("ROOT"),
                    NestingType::TopLevelChild => format!("{parent_sk}"),
                    NestingType::InlineChild => format!("{parent_pk}"),
                }
            }
            fn generate_sk(&self, parent_pk: &str, parent_sk: &str, new_id: &str) -> String {
                match $nesting_type {
                    NestingType::Root => format!("{}#{new_id}", $id_label),
                    NestingType::TopLevelChild => format!("{}#{new_id}", $id_label),
                    NestingType::InlineChild => format!("{parent_sk}#{}#{new_id}", $id_label),
                }
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

// Used for automatic created / modified timestamps.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Timestamp {
    pub seconds: i64,
    pub nanos: u32,
}
