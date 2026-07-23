use std::sync::{Arc, Mutex};

use fractic_server_error::ServerError;
use serde::{
    de::{self, DeserializeOwned, Deserializer},
    Deserialize,
};
use serde_json::Value as JsonValue;

use crate::{
    errors::DynamoInvalidOperation,
    errors::DynamoNotFound,
    schema::{DynamoObject, PkSk},
    util::{CreateOptions, CreateToken, DynamoInsertPosition, DynamoUtil},
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
        if let Some(Ok(id)) = v.as_str().map(PkSk::from_string) {
            return Ok(PassOrFetch::Fetch(id));
        }

        // 2) Otherwise, try as full object O.
        let obj = serde_json::from_value::<O>(v).map_err(de::Error::custom)?;
        Ok(PassOrFetch::Pass(obj))
    }
}

impl<O> PassOrFetch<O>
where
    O: DynamoObject,
{
    /// Returns the ID of the passed object or the ID to fetch.
    pub fn id(&self) -> &PkSk {
        match self {
            PassOrFetch::Pass(obj) => obj.id(),
            PassOrFetch::Fetch(id) => id,
        }
    }

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
    Create {
        args: C,
        token: Option<CachedCreateToken<O>>,
    },
}

pub type PassFetchOrCreateUnordered<O> = PassFetchOrCreate<O, UnorderedCreate<O>>;
pub type PassFetchOrCreateOrdered<O> = PassFetchOrCreate<O, OrderedCreate<O>>;
pub type PassFetchOrCreateUnorderedWithParent<O> =
    PassFetchOrCreate<O, UnorderedCreateWithParent<O>>;
pub type PassFetchOrCreateOrderedWithParent<O> = PassFetchOrCreate<O, OrderedCreateWithParent<O>>;

/// A generated create token shared by cloned ingress values.
///
/// The ID can be read by every clone, while the token itself can only be
/// consumed by one creation attempt.
#[derive(Debug)]
pub struct CachedCreateToken<O>
where
    O: DynamoObject,
{
    id: PkSk,
    token: Arc<Mutex<Option<CreateToken<O>>>>,
}

impl<O> CachedCreateToken<O>
where
    O: DynamoObject,
{
    fn new(token: CreateToken<O>) -> Self {
        Self {
            id: token.id().clone(),
            token: Arc::new(Mutex::new(Some(token))),
        }
    }

    fn take(&self) -> Result<CreateToken<O>, ServerError> {
        self.token
            .lock()
            .map_err(|_| DynamoInvalidOperation::new("Cached create token lock was poisoned"))?
            .take()
            .ok_or_else(|| DynamoInvalidOperation::new("Cached create token was already consumed"))
    }
}

impl<O> Clone for CachedCreateToken<O>
where
    O: DynamoObject,
{
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            token: Arc::clone(&self.token),
        }
    }
}

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
    /// Returns the object data supplied for creation.
    fn data(&self) -> &O::Data;

    /// Returns the explicit parent ID, or the caller-provided parent for create
    /// arguments that do not contain one.
    fn parent_id<'a>(&'a self, parent_id: Option<&'a PkSk>) -> &'a PkSk;
}

/// Unordered create.
#[derive(Debug, Clone, Deserialize)]
pub struct UnorderedCreate<O: DynamoObject> {
    #[serde(rename = "create")]
    pub data: O::Data,
}

impl<O: DynamoObject> CreateArgs<O> for UnorderedCreate<O> {
    fn data(&self) -> &O::Data {
        &self.data
    }

    fn parent_id<'a>(&'a self, parent_id: Option<&'a PkSk>) -> &'a PkSk {
        parent_id.unwrap_or(PkSk::root())
    }
}

/// Ordered create (with optional `after` argument).
#[derive(Debug, Clone, Deserialize)]
pub struct OrderedCreate<O: DynamoObject> {
    #[serde(rename = "create")]
    pub data: O::Data,
    pub after: Option<PkSk>,
}

impl<O: DynamoObject> CreateArgs<O> for OrderedCreate<O> {
    fn data(&self) -> &O::Data {
        &self.data
    }

    fn parent_id<'a>(&'a self, parent_id: Option<&'a PkSk>) -> &'a PkSk {
        parent_id.unwrap_or(PkSk::root())
    }
}

/// Unordered create with parent passed in by caller.
#[derive(Debug, Clone, Deserialize)]
pub struct UnorderedCreateWithParent<O: DynamoObject> {
    pub parent_id: PkSk,
    #[serde(rename = "create")]
    pub data: O::Data,
}

impl<O: DynamoObject> CreateArgs<O> for UnorderedCreateWithParent<O> {
    fn data(&self) -> &O::Data {
        &self.data
    }

    fn parent_id<'a>(&'a self, _parent_id: Option<&'a PkSk>) -> &'a PkSk {
        &self.parent_id
    }
}

/// Ordered create with parent passed in by caller.
#[derive(Debug, Clone, Deserialize)]
pub struct OrderedCreateWithParent<O: DynamoObject> {
    pub parent_id: PkSk,
    #[serde(rename = "create")]
    pub data: O::Data,
    pub after: Option<PkSk>,
}

impl<O: DynamoObject> CreateArgs<O> for OrderedCreateWithParent<O> {
    fn data(&self) -> &O::Data {
        &self.data
    }

    fn parent_id<'a>(&'a self, _parent_id: Option<&'a PkSk>) -> &'a PkSk {
        &self.parent_id
    }
}

impl<O> From<UnorderedCreate<O>> for PassFetchOrCreate<O, UnorderedCreate<O>>
where
    O: DynamoObject,
{
    fn from(value: UnorderedCreate<O>) -> Self {
        Self::Create {
            args: value,
            token: None,
        }
    }
}

impl<O> From<OrderedCreate<O>> for PassFetchOrCreate<O, OrderedCreate<O>>
where
    O: DynamoObject,
{
    fn from(value: OrderedCreate<O>) -> Self {
        Self::Create {
            args: value,
            token: None,
        }
    }
}

impl<O> From<UnorderedCreateWithParent<O>> for PassFetchOrCreate<O, UnorderedCreateWithParent<O>>
where
    O: DynamoObject,
{
    fn from(value: UnorderedCreateWithParent<O>) -> Self {
        Self::Create {
            args: value,
            token: None,
        }
    }
}

impl<O> From<OrderedCreateWithParent<O>> for PassFetchOrCreate<O, OrderedCreateWithParent<O>>
where
    O: DynamoObject,
{
    fn from(value: OrderedCreateWithParent<O>) -> Self {
        Self::Create {
            args: value,
            token: None,
        }
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
        if let Some(Ok(id)) = v.as_str().map(PkSk::from_string) {
            return Ok(PassFetchOrCreate::<O, C>::Fetch(id));
        }

        // 2) Try as full object O.
        if let Ok(object) = serde_json::from_value::<O>(v.clone()) {
            return Ok(PassFetchOrCreate::<O, C>::Pass(object));
        }

        // 3) Otherwise, try as create arguments C.
        let create_args = serde_json::from_value::<C>(v).map_err(de::Error::custom)?;
        Ok(PassFetchOrCreate::<O, C>::Create {
            args: create_args,
            token: None,
        })
    }
}

impl<O, C> PassFetchOrCreate<O, C>
where
    O: DynamoObject,
    C: CreateArgs<O>,
{
    /// Returns whether this ingress value contains creation arguments.
    pub fn is_create(&self) -> bool {
        matches!(self, PassFetchOrCreate::Create { .. })
    }

    /// Returns the existing ID, or creates and caches a token for a pending
    /// creation and returns its ID.
    pub fn id_or_create_token(
        &mut self,
        dynamo_util: &DynamoUtil,
        parent_id: Option<&PkSk>,
    ) -> Result<&PkSk, ServerError> {
        match self {
            PassFetchOrCreate::Pass(obj) => Ok(obj.id()),
            PassFetchOrCreate::Fetch(id) => Ok(id),
            PassFetchOrCreate::Create { args, token } => {
                if token.is_none() {
                    *token = Some(CachedCreateToken::new(
                        dynamo_util.create_token::<O>(args.parent_id(parent_id), args.data())?,
                    ));
                }
                Ok(&token
                    .as_ref()
                    .expect("create token was initialized above")
                    .id)
            }
        }
    }

    /// Returns mutable creation arguments, or `None` when passing or fetching.
    pub fn create_args_mut(&mut self) -> Option<&mut C> {
        match self {
            PassFetchOrCreate::Create { args, .. } => Some(args),
            PassFetchOrCreate::Pass(_) | PassFetchOrCreate::Fetch(_) => None,
        }
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
            PassFetchOrCreate::Create {
                args: UnorderedCreate { data },
                token,
            } => {
                let parent_id = parent_id.as_ref().unwrap_or(PkSk::root());
                dynamo_util
                    .create_item_opt::<O>(
                        parent_id,
                        data,
                        CreateOptions {
                            token: token.map(|token| token.take()).transpose()?,
                            ..Default::default()
                        },
                    )
                    .await
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
            PassFetchOrCreate::Create {
                args: OrderedCreate { data, after },
                token,
            } => {
                let parent_id = parent_id.as_ref().unwrap_or(PkSk::root());
                let insert_position = match after {
                    Some(a) => DynamoInsertPosition::After(a),
                    None => DynamoInsertPosition::Last,
                };
                dynamo_util
                    .create_item_ordered_opt::<O>(
                        parent_id,
                        data,
                        insert_position,
                        CreateOptions {
                            token: token.map(|token| token.take()).transpose()?,
                            ..Default::default()
                        },
                    )
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
            PassFetchOrCreate::Create {
                args: UnorderedCreateWithParent { parent_id, data },
                token,
            } => {
                dynamo_util
                    .create_item_opt::<O>(
                        &parent_id,
                        data,
                        CreateOptions {
                            token: token.map(|token| token.take()).transpose()?,
                            ..Default::default()
                        },
                    )
                    .await
            }
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
            PassFetchOrCreate::Create {
                args:
                    OrderedCreateWithParent {
                        parent_id,
                        data,
                        after,
                    },
                token,
            } => {
                let insert_position = match after {
                    Some(a) => DynamoInsertPosition::After(a),
                    None => DynamoInsertPosition::Last,
                };
                dynamo_util
                    .create_item_ordered_opt::<O>(
                        &parent_id,
                        data,
                        insert_position,
                        CreateOptions {
                            token: token.map(|token| token.take()).transpose()?,
                            ..Default::default()
                        },
                    )
                    .await
            }
        }
    }
}

/// Allows callers to either pass a reference to an already-fetched object `&O`,
/// or an id `PkSk` to be fetched lazily (or, for compatibility with serde, it
/// can directly take ownership of an `O` built from deserialization). The
/// resolved reference is valid for the lifetime of the `RefOrFetch` itself.
#[derive(Debug, Clone)]
pub enum RefOrFetch<'a, O>
where
    O: DynamoObject,
{
    Ref(&'a O),
    Owned(O),
    Fetch { id: PkSk, cached: Option<O> },
}

impl<'a, O> From<&'a O> for RefOrFetch<'a, O>
where
    O: DynamoObject,
{
    fn from(value: &'a O) -> Self {
        Self::Ref(value)
    }
}

impl<'a, O> From<PkSk> for RefOrFetch<'a, O>
where
    O: DynamoObject,
{
    fn from(value: PkSk) -> Self {
        Self::Fetch {
            id: value,
            cached: None,
        }
    }
}

// NOTE:
//   No From<..> implementation for O -> RefOrFetch::Owned(O), since that's not
//   an intended use-case.

impl<'a, O> RefOrFetch<'a, O>
where
    O: DynamoObject,
{
    pub async fn resolve<'s>(&'s mut self, dynamo_util: &DynamoUtil) -> Result<&'s O, ServerError> {
        match self {
            RefOrFetch::Ref(reference) => Ok(*reference),
            RefOrFetch::Owned(object) => Ok(object),
            RefOrFetch::Fetch { id, cached } => {
                if cached.is_none() {
                    *cached = dynamo_util.get_item::<O>(id.clone()).await?;
                }
                cached.as_ref().ok_or_else(DynamoNotFound::new)
            }
        }
    }
}

impl<'de, 'a, O> Deserialize<'de> for RefOrFetch<'a, O>
where
    O: DynamoObject + DeserializeOwned,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = JsonValue::deserialize(deserializer)?;

        // 1) Try as PkSk.
        if let Some(Ok(id)) = v.as_str().map(PkSk::from_string) {
            return Ok(RefOrFetch::Fetch { id, cached: None });
        }

        // 2) Otherwise, deserialize the full object and store it owned.
        let obj = serde_json::from_value::<O>(v).map_err(de::Error::custom)?;
        Ok(RefOrFetch::Owned(obj))
    }
}

/// Allows callers to either pass a reference to an already-fetched object `&O`,
/// fetch by ID, or create an item (or, for compatibility with serde, it can
/// directly take ownership of an `O` built from deserialization). The resolved
/// reference is valid for the lifetime of the `RefFetchOrCreate` itself.
#[derive(Debug, Clone)]
pub enum RefFetchOrCreate<'a, O, C>
where
    O: DynamoObject,
    C: CreateArgs<O>,
{
    Ref(&'a O),
    Owned(O),
    Fetch { id: PkSk, cached: Option<O> },
    Create { args: Option<C>, cached: Option<O> },
}

pub type RefFetchOrCreateUnordered<'a, O> = RefFetchOrCreate<'a, O, UnorderedCreate<O>>;
pub type RefFetchOrCreateOrdered<'a, O> = RefFetchOrCreate<'a, O, OrderedCreate<O>>;
pub type RefFetchOrCreateUnorderedWithParent<'a, O> =
    RefFetchOrCreate<'a, O, UnorderedCreateWithParent<O>>;
pub type RefFetchOrCreateOrderedWithParent<'a, O> =
    RefFetchOrCreate<'a, O, OrderedCreateWithParent<O>>;

impl<'a, O, C> From<&'a O> for RefFetchOrCreate<'a, O, C>
where
    O: DynamoObject,
    C: CreateArgs<O>,
{
    fn from(value: &'a O) -> Self {
        Self::Ref(value)
    }
}

impl<'a, O, C> From<PkSk> for RefFetchOrCreate<'a, O, C>
where
    O: DynamoObject,
    C: CreateArgs<O>,
{
    fn from(value: PkSk) -> Self {
        Self::Fetch {
            id: value,
            cached: None,
        }
    }
}

impl<'a, O> From<UnorderedCreate<O>> for RefFetchOrCreate<'a, O, UnorderedCreate<O>>
where
    O: DynamoObject,
{
    fn from(value: UnorderedCreate<O>) -> Self {
        Self::Create {
            args: Some(value),
            cached: None,
        }
    }
}

impl<'a, O> From<OrderedCreate<O>> for RefFetchOrCreate<'a, O, OrderedCreate<O>>
where
    O: DynamoObject,
{
    fn from(value: OrderedCreate<O>) -> Self {
        Self::Create {
            args: Some(value),
            cached: None,
        }
    }
}

impl<'a, O> From<UnorderedCreateWithParent<O>>
    for RefFetchOrCreate<'a, O, UnorderedCreateWithParent<O>>
where
    O: DynamoObject,
{
    fn from(value: UnorderedCreateWithParent<O>) -> Self {
        Self::Create {
            args: Some(value),
            cached: None,
        }
    }
}

impl<'a, O> From<OrderedCreateWithParent<O>> for RefFetchOrCreate<'a, O, OrderedCreateWithParent<O>>
where
    O: DynamoObject,
{
    fn from(value: OrderedCreateWithParent<O>) -> Self {
        Self::Create {
            args: Some(value),
            cached: None,
        }
    }
}

// NOTE:
//   No From<..> implementation for O -> RefFetchOrCreate::Owned(O), since
//   that's not an intended use-case.

impl<'de, 'a, O, C> Deserialize<'de> for RefFetchOrCreate<'a, O, C>
where
    O: DynamoObject + DeserializeOwned,
    C: CreateArgs<O>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = JsonValue::deserialize(deserializer)?;

        // 1) Try as PkSk.
        if let Some(Ok(id)) = v.as_str().map(PkSk::from_string) {
            return Ok(RefFetchOrCreate::Fetch { id, cached: None });
        }

        // 2) Try as full object O.
        if let Ok(object) = serde_json::from_value::<O>(v.clone()) {
            return Ok(RefFetchOrCreate::Owned(object));
        }

        // 3) Otherwise, try as create arguments C.
        let create_args = serde_json::from_value::<C>(v).map_err(de::Error::custom)?;
        Ok(RefFetchOrCreate::Create {
            args: Some(create_args),
            cached: None,
        })
    }
}

impl<'a, O> RefFetchOrCreate<'a, O, UnorderedCreate<O>>
where
    O: DynamoObject,
{
    pub async fn resolve<'s>(
        &'s mut self,
        dynamo_util: &DynamoUtil,
        parent_id: Option<PkSk>,
    ) -> Result<&'s O, ServerError> {
        match self {
            RefFetchOrCreate::Ref(reference) => Ok(*reference),
            RefFetchOrCreate::Owned(object) => Ok(object),
            RefFetchOrCreate::Fetch { id, cached } => {
                if cached.is_none() {
                    *cached = dynamo_util.get_item::<O>(id.clone()).await?;
                }
                cached.as_ref().ok_or_else(DynamoNotFound::new)
            }
            RefFetchOrCreate::Create { args, cached } => {
                if cached.is_none() {
                    let UnorderedCreate { data } = args.take().ok_or_else(|| {
                        DynamoInvalidOperation::new(
                            "RefFetchOrCreate::Create already attempted and cannot be retried",
                        )
                    })?;
                    let parent_id = parent_id.as_ref().unwrap_or(PkSk::root());
                    *cached = Some(dynamo_util.create_item::<O>(parent_id, data).await?);
                }
                cached.as_ref().ok_or_else(DynamoNotFound::new)
            }
        }
    }
}

impl<'a, O> RefFetchOrCreate<'a, O, OrderedCreate<O>>
where
    O: DynamoObject,
{
    pub async fn resolve<'s>(
        &'s mut self,
        dynamo_util: &DynamoUtil,
        parent_id: Option<PkSk>,
    ) -> Result<&'s O, ServerError> {
        match self {
            RefFetchOrCreate::Ref(reference) => Ok(*reference),
            RefFetchOrCreate::Owned(object) => Ok(object),
            RefFetchOrCreate::Fetch { id, cached } => {
                if cached.is_none() {
                    *cached = dynamo_util.get_item::<O>(id.clone()).await?;
                }
                cached.as_ref().ok_or_else(DynamoNotFound::new)
            }
            RefFetchOrCreate::Create { args, cached } => {
                if cached.is_none() {
                    let OrderedCreate { data, after } = args.take().ok_or_else(|| {
                        DynamoInvalidOperation::new(
                            "RefFetchOrCreate::Create already attempted and cannot be retried",
                        )
                    })?;
                    let parent_id = parent_id.as_ref().unwrap_or(PkSk::root());
                    let insert_position = match after {
                        Some(a) => DynamoInsertPosition::After(a),
                        None => DynamoInsertPosition::Last,
                    };
                    *cached = Some(
                        dynamo_util
                            .create_item_ordered::<O>(parent_id, data, insert_position)
                            .await?,
                    );
                }
                cached.as_ref().ok_or_else(DynamoNotFound::new)
            }
        }
    }
}

impl<'a, O> RefFetchOrCreate<'a, O, UnorderedCreateWithParent<O>>
where
    O: DynamoObject,
{
    pub async fn resolve<'s>(&'s mut self, dynamo_util: &DynamoUtil) -> Result<&'s O, ServerError> {
        match self {
            RefFetchOrCreate::Ref(reference) => Ok(*reference),
            RefFetchOrCreate::Owned(object) => Ok(object),
            RefFetchOrCreate::Fetch { id, cached } => {
                if cached.is_none() {
                    *cached = dynamo_util.get_item::<O>(id.clone()).await?;
                }
                cached.as_ref().ok_or_else(DynamoNotFound::new)
            }
            RefFetchOrCreate::Create { args, cached } => {
                if cached.is_none() {
                    let UnorderedCreateWithParent { parent_id, data } =
                        args.take().ok_or_else(|| {
                            DynamoInvalidOperation::new(
                                "RefFetchOrCreate::Create already attempted and cannot be retried",
                            )
                        })?;
                    *cached = Some(dynamo_util.create_item::<O>(&parent_id, data).await?);
                }
                cached.as_ref().ok_or_else(DynamoNotFound::new)
            }
        }
    }
}

impl<'a, O> RefFetchOrCreate<'a, O, OrderedCreateWithParent<O>>
where
    O: DynamoObject,
{
    pub async fn resolve<'s>(&'s mut self, dynamo_util: &DynamoUtil) -> Result<&'s O, ServerError> {
        match self {
            RefFetchOrCreate::Ref(reference) => Ok(*reference),
            RefFetchOrCreate::Owned(object) => Ok(object),
            RefFetchOrCreate::Fetch { id, cached } => {
                if cached.is_none() {
                    *cached = dynamo_util.get_item::<O>(id.clone()).await?;
                }
                cached.as_ref().ok_or_else(DynamoNotFound::new)
            }
            RefFetchOrCreate::Create { args, cached } => {
                if cached.is_none() {
                    let OrderedCreateWithParent {
                        parent_id,
                        data,
                        after,
                    } = args.take().ok_or_else(|| {
                        DynamoInvalidOperation::new(
                            "RefFetchOrCreate::Create already attempted and cannot be retried",
                        )
                    })?;
                    let insert_position = match after {
                        Some(a) => DynamoInsertPosition::After(a),
                        None => DynamoInsertPosition::Last,
                    };
                    *cached = Some(
                        dynamo_util
                            .create_item_ordered::<O>(&parent_id, data, insert_position)
                            .await?,
                    );
                }
                cached.as_ref().ok_or_else(DynamoNotFound::new)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::{
        context::test_ctx::TestCtx,
        dynamo_object,
        schema::{IdLogic, NestingLogic},
        util::backend::MockDynamoBackend,
    };

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    pub struct TestObjectData {
        value: String,
    }

    dynamo_object!(
        TestObject,
        TestObjectData,
        "INGRESS_TEST",
        IdLogic::UuidV4,
        NestingLogic::Root
    );

    async fn build_util() -> DynamoUtil {
        let ctx = TestCtx::init_test("mock-region".to_string());
        ctx.override_dynamo_backend(Arc::new(MockDynamoBackend::new()))
            .await;
        DynamoUtil::new(&*ctx, "ingress_test").await.unwrap()
    }

    #[test]
    fn pass_or_fetch_exposes_id() {
        let id = PkSk {
            pk: "ROOT".into(),
            sk: "INGRESS_TEST#object".into(),
        };
        let passed = PassOrFetch::from(TestObject::new(id.clone(), TestObjectData::default()));
        let fetched = PassOrFetch::<TestObject>::from(id.clone());

        assert_eq!(passed.id(), &id);
        assert_eq!(fetched.id(), &id);
    }

    #[tokio::test]
    async fn id_or_create_token_caches_and_shares_one_token() {
        let dynamo_util = build_util().await;
        let mut ingress = PassFetchOrCreateUnordered::<TestObject>::from(UnorderedCreate {
            data: TestObjectData::default(),
        });
        let first_id = ingress
            .id_or_create_token(&dynamo_util, None)
            .unwrap()
            .clone();
        let second_id = ingress
            .id_or_create_token(&dynamo_util, None)
            .unwrap()
            .clone();
        let cloned = ingress.clone();

        assert_eq!(first_id, second_id);
        let PassFetchOrCreate::Create {
            token: Some(token), ..
        } = ingress
        else {
            panic!("expected prepared create ingress");
        };
        let PassFetchOrCreate::Create {
            token: Some(cloned_token),
            ..
        } = cloned
        else {
            panic!("expected cloned prepared create ingress");
        };
        assert_eq!(token.id, cloned_token.id);
        assert!(token.take().is_ok());
        assert!(cloned_token.take().is_err());
    }
}
