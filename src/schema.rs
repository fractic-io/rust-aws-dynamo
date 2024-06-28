use fractic_generic_server_error::{common::CriticalError, GenericServerError};
use serde::{de::DeserializeOwned, Serialize};

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
