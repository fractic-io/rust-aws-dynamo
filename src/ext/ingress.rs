use fractic_server_error::ServerError;
use serde::de::{self, Deserialize, DeserializeOwned, Deserializer};
use serde_json::Value as JsonValue;

use crate::{
    errors::DynamoNotFound,
    schema::{DynamoObject, PkSk},
    util::DynamoUtil,
};

/// Allows callers to either pass an already-fetched object `O`, or a `PkSk` id
/// to be fetched lazily.
///
/// Deserialization is untagged (i.e. the client's desired interface is
/// inferred).
#[derive(Debug, Clone)]
pub enum PassOrFetch<O>
where
    O: DynamoObject + DeserializeOwned,
{
    Pass(O),
    Fetch(PkSk),
}

impl<O> From<O> for PassOrFetch<O>
where
    O: DynamoObject + DeserializeOwned,
{
    fn from(value: O) -> Self {
        Self::Pass(value)
    }
}

impl<O> From<PkSk> for PassOrFetch<O>
where
    O: DynamoObject + DeserializeOwned,
{
    fn from(value: PkSk) -> Self {
        Self::Fetch(value)
    }
}

impl<'de, O> Deserialize<'de> for PassOrFetch<O>
where
    O: DynamoObject + DeserializeOwned,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = JsonValue::deserialize(deserializer)?;

        // 1) Try as PkSk first (string form).
        if let Ok(id) = serde_json::from_value::<PkSk>(v.clone()) {
            return Ok(PassOrFetch::Fetch(id));
        }

        // 2) Otherwise, try as full object O.
        let obj = serde_json::from_value::<O>(v).map_err(|e| de::Error::custom(e))?;
        Ok(PassOrFetch::Pass(obj))
    }
}

impl<O> PassOrFetch<O>
where
    O: DynamoObject + DeserializeOwned,
{
    /// Resolves into a concrete `O` by either passing through the provided
    /// object or fetching it via `DynamoUtil`.
    pub async fn resolve(self, dynamo_util: &DynamoUtil) -> Result<O, ServerError> {
        match self {
            PassOrFetch::Pass(obj) => Ok(obj),
            PassOrFetch::Fetch(id) => match dynamo_util.get_item::<O>(id).await? {
                Some(obj) => Ok(obj),
                None => Err(DynamoNotFound::new()),
            },
        }
    }
}

/// Allows callers to pass, fetch or create an object.
///
/// - Pass: provide a full `O`
/// - Fetch: provide a `PkSk` id (`"pk|sk"`)
/// - Create: provide `{ parent_id: "pk|sk", data: O::Data }`
///
/// Like `PassOrFetch`, deserialization is untagged (i.e. the client's desired
/// interface is inferred).
#[derive(Debug, Clone)]
pub enum PassFetchOrCreate<O>
where
    O: DynamoObject + DeserializeOwned,
    O::Data: DeserializeOwned,
{
    Pass(O),
    Fetch(PkSk),
    Create { parent_id: PkSk, data: O::Data },
}

impl<O> From<O> for PassFetchOrCreate<O>
where
    O: DynamoObject + DeserializeOwned,
    O::Data: DeserializeOwned,
{
    fn from(value: O) -> Self {
        Self::Pass(value)
    }
}

impl<'de, O> Deserialize<'de> for PassFetchOrCreate<O>
where
    O: DynamoObject + DeserializeOwned,
    O::Data: DeserializeOwned,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = JsonValue::deserialize(deserializer)?;

        // 1) Try as PkSk (string id).
        if let Ok(id) = serde_json::from_value::<PkSk>(v.clone()) {
            return Ok(PassFetchOrCreate::Fetch(id));
        }

        // 2) Try as Create { parent_id, data }.
        if let JsonValue::Object(map) = &v {
            if map.contains_key("parent_id") && map.contains_key("data") {
                let parent_id =
                    serde_json::from_value::<PkSk>(map.get("parent_id").cloned().ok_or_else(
                        || de::Error::custom("missing parent_id for create variant"),
                    )?)
                    .map_err(|e| de::Error::custom(e))?;
                let data = serde_json::from_value::<O::Data>(
                    map.get("data")
                        .cloned()
                        .ok_or_else(|| de::Error::custom("missing data for create variant"))?,
                )
                .map_err(|e| de::Error::custom(e))?;
                return Ok(PassFetchOrCreate::Create { parent_id, data });
            }
        }

        // 3) Otherwise, try as full object O (Pass).
        let obj = serde_json::from_value::<O>(v).map_err(|e| de::Error::custom(e))?;
        Ok(PassFetchOrCreate::Pass(obj))
    }
}

impl<O> From<PkSk> for PassFetchOrCreate<O>
where
    O: DynamoObject + DeserializeOwned,
    O::Data: DeserializeOwned,
{
    fn from(value: PkSk) -> Self {
        Self::Fetch(value)
    }
}

impl<O> PassFetchOrCreate<O>
where
    O: DynamoObject + DeserializeOwned,
    O::Data: DeserializeOwned,
{
    /// Resolves into a concrete `O` by either passing through, fetching, or
    /// creating the object via `DynamoUtil`.
    pub async fn resolve(self, dynamo_util: &DynamoUtil) -> Result<O, ServerError> {
        match self {
            PassFetchOrCreate::Pass(obj) => Ok(obj),
            PassFetchOrCreate::Fetch(id) => match dynamo_util.get_item::<O>(id).await? {
                Some(obj) => Ok(obj),
                None => Err(DynamoNotFound::new()),
            },
            PassFetchOrCreate::Create { parent_id, data } => {
                dynamo_util.create_item::<O>(parent_id, data, None).await
            }
        }
    }
}
