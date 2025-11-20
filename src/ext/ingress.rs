use fractic_server_error::ServerError;
use serde::{
    de::{self, DeserializeOwned, Deserializer},
    Deserialize,
};
use serde_json::Value as JsonValue;

use crate::{
    errors::DynamoNotFound,
    schema::{DynamoObject, PkSk},
    util::{DynamoInsertPosition, DynamoUtil},
};

/// Allows callers to either pass an already-fetched object `O`, or a `PkSk` id
/// to be fetched lazily.
///
/// Deserialization is untagged (i.e. the client's desired interface is
/// inferred).
#[derive(Debug, Clone)]
pub enum PassOrFetch<O>
where
    O: DynamoObject,
{
    Pass(O),
    Fetch(PkSk),
}

impl<O> From<O> for PassOrFetch<O>
where
    O: DynamoObject,
{
    fn from(value: O) -> Self {
        Self::Pass(value)
    }
}

impl<O> From<PkSk> for PassOrFetch<O>
where
    O: DynamoObject,
{
    fn from(value: PkSk) -> Self {
        Self::Fetch(value)
    }
}

impl<'de, O> Deserialize<'de> for PassOrFetch<O>
where
    O: DynamoObject,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = JsonValue::deserialize(deserializer)?;

        // 1) Try as PkSk.
        if let Some(Ok(id)) = v.as_str().map(|s| PkSk::from_string(&s)) {
            return Ok(PassOrFetch::Fetch(id));
        }

        // 2) Otherwise, try as full object O.
        let obj = serde_json::from_value::<O>(v).map_err(|e| de::Error::custom(e))?;
        Ok(PassOrFetch::Pass(obj))
    }
}

impl<O> PassOrFetch<O>
where
    O: DynamoObject,
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
/// - Pass: provide a full `O`.
/// - Fetch: provide a `PkSk` id (`"pk|sk"`).
/// - Create: provide arguments `C` (see `CreateArgs` types).
#[derive(Debug, Clone)]
pub enum PassFetchOrCreate<O, C>
where
    O: DynamoObject,
    C: CreateArgs<O>,
{
    Pass(O),
    Fetch(PkSk),
    Create(C),
}

pub type PassFetchOrCreateUnordered<O> = PassFetchOrCreate<O, UnorderedCreate<O>>;
pub type PassFetchOrCreateOrdered<O> = PassFetchOrCreate<O, OrderedCreate<O>>;
pub type PassFetchOrCreateUnorderedWithParent<O> =
    PassFetchOrCreate<O, UnorderedCreateWithParent<O>>;
pub type PassFetchOrCreateOrderedWithParent<O> = PassFetchOrCreate<O, OrderedCreateWithParent<O>>;

impl<O, C> From<O> for PassFetchOrCreate<O, C>
where
    O: DynamoObject,
    C: CreateArgs<O>,
{
    fn from(value: O) -> Self {
        Self::Pass(value)
    }
}

impl<O, C> From<PkSk> for PassFetchOrCreate<O, C>
where
    O: DynamoObject,
    C: CreateArgs<O>,
{
    fn from(value: PkSk) -> Self {
        Self::Fetch(value)
    }
}

pub trait CreateArgs<O>: DeserializeOwned
where
    O: DynamoObject,
{
}

/// Unordered create.
#[derive(Debug, Clone, Deserialize)]
pub struct UnorderedCreate<O: DynamoObject> {
    pub create: O::Data,
}

impl<O: DynamoObject> CreateArgs<O> for UnorderedCreate<O> {}

/// Ordered create (with optional `after` argument).
#[derive(Debug, Clone, Deserialize)]
pub struct OrderedCreate<O: DynamoObject> {
    pub create: O::Data,
    pub after: Option<PkSk>,
}

impl<O: DynamoObject> CreateArgs<O> for OrderedCreate<O> {}

/// Unordered create with parent passed in by caller.
#[derive(Debug, Clone, Deserialize)]
pub struct UnorderedCreateWithParent<O: DynamoObject> {
    pub parent_id: PkSk,
    pub create: O::Data,
}

impl<O: DynamoObject> CreateArgs<O> for UnorderedCreateWithParent<O> {}

/// Ordered create with parent passed in by caller.
#[derive(Debug, Clone, Deserialize)]
pub struct OrderedCreateWithParent<O: DynamoObject> {
    pub parent_id: PkSk,
    pub create: O::Data,
    pub after: Option<PkSk>,
}

impl<O: DynamoObject> CreateArgs<O> for OrderedCreateWithParent<O> {}

impl<O> From<UnorderedCreate<O>> for PassFetchOrCreate<O, UnorderedCreate<O>>
where
    O: DynamoObject,
{
    fn from(value: UnorderedCreate<O>) -> Self {
        Self::Create(value)
    }
}

impl<O> From<OrderedCreate<O>> for PassFetchOrCreate<O, OrderedCreate<O>>
where
    O: DynamoObject,
{
    fn from(value: OrderedCreate<O>) -> Self {
        Self::Create(value)
    }
}

impl<O> From<UnorderedCreateWithParent<O>> for PassFetchOrCreate<O, UnorderedCreateWithParent<O>>
where
    O: DynamoObject,
{
    fn from(value: UnorderedCreateWithParent<O>) -> Self {
        Self::Create(value)
    }
}

impl<O> From<OrderedCreateWithParent<O>> for PassFetchOrCreate<O, OrderedCreateWithParent<O>>
where
    O: DynamoObject,
{
    fn from(value: OrderedCreateWithParent<O>) -> Self {
        Self::Create(value)
    }
}

impl<'de, O, C> Deserialize<'de> for PassFetchOrCreate<O, C>
where
    O: DynamoObject,
    C: CreateArgs<O>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = JsonValue::deserialize(deserializer)?;

        // 1) Try as PkSk.
        if let Some(Ok(id)) = v.as_str().map(|s| PkSk::from_string(&s)) {
            return Ok(PassFetchOrCreate::<O, C>::Fetch(id));
        }

        // 2) Try as full object O.
        if let Ok(object) = serde_json::from_value::<O>(v.clone()) {
            return Ok(PassFetchOrCreate::<O, C>::Pass(object));
        }

        // 3) Otherwise, try as create arguments C.
        let create_args = serde_json::from_value::<C>(v).map_err(|e| de::Error::custom(e))?;
        Ok(PassFetchOrCreate::<O, C>::Create(create_args))
    }
}

impl<O> PassFetchOrCreate<O, UnorderedCreate<O>>
where
    O: DynamoObject,
{
    pub async fn resolve(
        self,
        dynamo_util: &DynamoUtil,
        parent_id: Option<PkSk>,
    ) -> Result<O, ServerError> {
        match self {
            PassFetchOrCreate::Pass(obj) => Ok(obj),
            PassFetchOrCreate::Fetch(id) => match dynamo_util.get_item::<O>(id).await? {
                Some(obj) => Ok(obj),
                None => Err(DynamoNotFound::new()),
            },
            PassFetchOrCreate::Create(UnorderedCreate { create: data }) => {
                let parent_id = parent_id.unwrap_or_else(PkSk::root);
                dynamo_util.create_item::<O>(parent_id, data, None).await
            }
        }
    }
}

impl<O> PassFetchOrCreate<O, OrderedCreate<O>>
where
    O: DynamoObject,
{
    pub async fn resolve(
        self,
        dynamo_util: &DynamoUtil,
        parent_id: Option<PkSk>,
    ) -> Result<O, ServerError> {
        match self {
            PassFetchOrCreate::Pass(obj) => Ok(obj),
            PassFetchOrCreate::Fetch(id) => match dynamo_util.get_item::<O>(id).await? {
                Some(obj) => Ok(obj),
                None => Err(DynamoNotFound::new()),
            },
            PassFetchOrCreate::Create(OrderedCreate {
                create: data,
                after,
            }) => {
                let parent_id = parent_id.unwrap_or_else(PkSk::root);
                let insert_position = match after {
                    Some(a) => DynamoInsertPosition::After(a),
                    None => DynamoInsertPosition::Last,
                };
                dynamo_util
                    .create_item_ordered::<O>(parent_id, data, insert_position)
                    .await
            }
        }
    }
}

impl<O> PassFetchOrCreate<O, UnorderedCreateWithParent<O>>
where
    O: DynamoObject,
{
    pub async fn resolve(self, dynamo_util: &DynamoUtil) -> Result<O, ServerError> {
        match self {
            PassFetchOrCreate::Pass(obj) => Ok(obj),
            PassFetchOrCreate::Fetch(id) => match dynamo_util.get_item::<O>(id).await? {
                Some(obj) => Ok(obj),
                None => Err(DynamoNotFound::new()),
            },
            PassFetchOrCreate::Create(UnorderedCreateWithParent {
                parent_id: parent,
                create: data,
            }) => dynamo_util.create_item::<O>(parent, data, None).await,
        }
    }
}

impl<O> PassFetchOrCreate<O, OrderedCreateWithParent<O>>
where
    O: DynamoObject,
{
    pub async fn resolve(self, dynamo_util: &DynamoUtil) -> Result<O, ServerError> {
        match self {
            PassFetchOrCreate::Pass(obj) => Ok(obj),
            PassFetchOrCreate::Fetch(id) => match dynamo_util.get_item::<O>(id).await? {
                Some(obj) => Ok(obj),
                None => Err(DynamoNotFound::new()),
            },
            PassFetchOrCreate::Create(OrderedCreateWithParent {
                parent_id: parent,
                create: data,
                after,
            }) => {
                let insert_position = match after {
                    Some(a) => DynamoInsertPosition::After(a),
                    None => DynamoInsertPosition::Last,
                };
                dynamo_util
                    .create_item_ordered::<O>(parent, data, insert_position)
                    .await
            }
        }
    }
}
