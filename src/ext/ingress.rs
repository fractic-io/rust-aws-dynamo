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
///
/// Cloning duplicates the unresolved request intent. Each cloned create
/// request produces an independent creation plan when prepared; whether their
/// IDs differ depends on the object's ID logic. Preparation consumes the
/// ingress value and produces a non-cloneable [`CreatePlan`].
#[derive(Debug, Clone)]
pub struct PassFetchOrCreate<O, C>
where
    O: DynamoObject,
    C: CreateArgs<O>,
{
    inner: PassFetchOrCreateInner<O, C>,
}

#[derive(Debug, Clone)]
enum PassFetchOrCreateInner<O, C>
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

/// A resolved existing object or a uniquely owned creation plan.
#[derive(Debug)]
pub enum PreparedIngress<O>
where
    O: DynamoObject,
{
    /// An object that was passed directly or fetched by ID.
    Existing(O),
    /// A new object with a reserved ID that has not yet been written.
    Create(CreatePlan<O>),
}

/// A uniquely owned object creation with a reserved ID.
#[derive(Debug)]
pub struct CreatePlan<O>
where
    O: DynamoObject,
{
    data: O::Data,
    parent_id: PkSk,
    mode: CreateMode,
    token: CreateToken<O>,
}

#[derive(Debug)]
enum CreateMode {
    Unordered,
    Ordered(DynamoInsertPosition),
}

impl<O> CreatePlan<O>
where
    O: DynamoObject,
{
    /// Returns the ID reserved for this creation.
    pub fn id(&self) -> &PkSk {
        self.token.id()
    }

    /// Returns the object data that will be supplied for creation.
    ///
    /// The reserved ID is not recalculated after mutation. Callers must not
    /// change fields used by the object's ID logic.
    pub fn data_mut(&mut self) -> &mut O::Data {
        &mut self.data
    }

    /// Creates the planned object using its reserved ID.
    pub async fn create(self, dynamo_util: &DynamoUtil) -> Result<O, ServerError> {
        let options = CreateOptions {
            token: Some(self.token),
            ..Default::default()
        };
        match self.mode {
            CreateMode::Ordered(insert_position) => {
                dynamo_util
                    .create_item_ordered_opt::<O>(
                        &self.parent_id,
                        self.data,
                        insert_position,
                        options,
                    )
                    .await
            }
            CreateMode::Unordered => {
                dynamo_util
                    .create_item_opt::<O>(&self.parent_id, self.data, options)
                    .await
            }
        }
    }
}

impl<O, C> From<O> for PassFetchOrCreate<O, C>
where
    O: DynamoObject,
    C: CreateArgs<O>,
{
    fn from(value: O) -> Self {
        Self {
            inner: PassFetchOrCreateInner::Pass(value),
        }
    }
}

impl<O, C> From<PkSk> for PassFetchOrCreate<O, C>
where
    O: DynamoObject,
    C: CreateArgs<O>,
{
    fn from(value: PkSk) -> Self {
        Self {
            inner: PassFetchOrCreateInner::Fetch(value),
        }
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
    fn parent_id<'a>(&'a self, parent_id: &'a PkSk) -> &'a PkSk;
}

trait PrepareCreateArgs<O>: CreateArgs<O>
where
    O: DynamoObject,
{
    fn into_prepared_parts(self) -> (O::Data, CreateMode);
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

    fn parent_id<'a>(&'a self, parent_id: &'a PkSk) -> &'a PkSk {
        parent_id
    }
}

impl<O: DynamoObject> PrepareCreateArgs<O> for UnorderedCreate<O> {
    fn into_prepared_parts(self) -> (O::Data, CreateMode) {
        (self.data, CreateMode::Unordered)
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

    fn parent_id<'a>(&'a self, parent_id: &'a PkSk) -> &'a PkSk {
        parent_id
    }
}

impl<O: DynamoObject> PrepareCreateArgs<O> for OrderedCreate<O> {
    fn into_prepared_parts(self) -> (O::Data, CreateMode) {
        (
            self.data,
            CreateMode::Ordered(
                self.after
                    .map_or(DynamoInsertPosition::Last, DynamoInsertPosition::After),
            ),
        )
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

    fn parent_id<'a>(&'a self, _parent_id: &'a PkSk) -> &'a PkSk {
        &self.parent_id
    }
}

impl<O: DynamoObject> PrepareCreateArgs<O> for UnorderedCreateWithParent<O> {
    fn into_prepared_parts(self) -> (O::Data, CreateMode) {
        (self.data, CreateMode::Unordered)
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

    fn parent_id<'a>(&'a self, _parent_id: &'a PkSk) -> &'a PkSk {
        &self.parent_id
    }
}

impl<O: DynamoObject> PrepareCreateArgs<O> for OrderedCreateWithParent<O> {
    fn into_prepared_parts(self) -> (O::Data, CreateMode) {
        (
            self.data,
            CreateMode::Ordered(
                self.after
                    .map_or(DynamoInsertPosition::Last, DynamoInsertPosition::After),
            ),
        )
    }
}

impl<O> From<UnorderedCreate<O>> for PassFetchOrCreate<O, UnorderedCreate<O>>
where
    O: DynamoObject,
{
    fn from(value: UnorderedCreate<O>) -> Self {
        Self {
            inner: PassFetchOrCreateInner::Create(value),
        }
    }
}

impl<O> From<OrderedCreate<O>> for PassFetchOrCreate<O, OrderedCreate<O>>
where
    O: DynamoObject,
{
    fn from(value: OrderedCreate<O>) -> Self {
        Self {
            inner: PassFetchOrCreateInner::Create(value),
        }
    }
}

impl<O> From<UnorderedCreateWithParent<O>> for PassFetchOrCreate<O, UnorderedCreateWithParent<O>>
where
    O: DynamoObject,
{
    fn from(value: UnorderedCreateWithParent<O>) -> Self {
        Self {
            inner: PassFetchOrCreateInner::Create(value),
        }
    }
}

impl<O> From<OrderedCreateWithParent<O>> for PassFetchOrCreate<O, OrderedCreateWithParent<O>>
where
    O: DynamoObject,
{
    fn from(value: OrderedCreateWithParent<O>) -> Self {
        Self {
            inner: PassFetchOrCreateInner::Create(value),
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
            return Ok(PassFetchOrCreate {
                inner: PassFetchOrCreateInner::Fetch(id),
            });
        }

        // 2) Try as full object O.
        if let Ok(object) = serde_json::from_value::<O>(v.clone()) {
            return Ok(PassFetchOrCreate {
                inner: PassFetchOrCreateInner::Pass(object),
            });
        }

        // 3) Otherwise, try as create arguments C.
        let create_args = serde_json::from_value::<C>(v).map_err(de::Error::custom)?;
        Ok(PassFetchOrCreate {
            inner: PassFetchOrCreateInner::Create(create_args),
        })
    }
}

async fn prepare_pass_fetch_or_create<O, C>(
    ingress: PassFetchOrCreate<O, C>,
    dynamo_util: &DynamoUtil,
    parent_id: &PkSk,
) -> Result<PreparedIngress<O>, ServerError>
where
    O: DynamoObject,
    C: PrepareCreateArgs<O>,
{
    match ingress.inner {
        PassFetchOrCreateInner::Pass(obj) => Ok(PreparedIngress::Existing(obj)),
        PassFetchOrCreateInner::Fetch(id) => {
            let obj = dynamo_util
                .get_item::<O>(id)
                .await?
                .ok_or_else(DynamoNotFound::new)?;
            Ok(PreparedIngress::Existing(obj))
        }
        PassFetchOrCreateInner::Create(args) => {
            let parent_id = args.parent_id(parent_id).clone();
            let token = dynamo_util.create_token::<O>(&parent_id, args.data())?;
            let (data, mode) = args.into_prepared_parts();
            Ok(PreparedIngress::Create(CreatePlan {
                data,
                parent_id,
                mode,
                token,
            }))
        }
    }
}

async fn resolve_pass_fetch_or_create<O, C>(
    ingress: PassFetchOrCreate<O, C>,
    dynamo_util: &DynamoUtil,
    parent_id: &PkSk,
) -> Result<O, ServerError>
where
    O: DynamoObject,
    C: PrepareCreateArgs<O>,
{
    match prepare_pass_fetch_or_create(ingress, dynamo_util, parent_id).await? {
        PreparedIngress::Existing(obj) => Ok(obj),
        PreparedIngress::Create(plan) => plan.create(dynamo_util).await,
    }
}

impl<O> PassFetchOrCreate<O, UnorderedCreate<O>>
where
    O: DynamoObject,
{
    /// Resolves a passed, fetched, or newly created object.
    pub async fn resolve(
        self,
        dynamo_util: &DynamoUtil,
        parent_id: &PkSk,
    ) -> Result<O, ServerError> {
        resolve_pass_fetch_or_create(self, dynamo_util, parent_id).await
    }

    /// Resolves passed/fetched input or prepares a uniquely owned creation.
    pub async fn prepare(
        self,
        dynamo_util: &DynamoUtil,
        parent_id: &PkSk,
    ) -> Result<PreparedIngress<O>, ServerError> {
        prepare_pass_fetch_or_create(self, dynamo_util, parent_id).await
    }
}

impl<O> PassFetchOrCreate<O, OrderedCreate<O>>
where
    O: DynamoObject,
{
    /// Resolves a passed, fetched, or newly created object.
    pub async fn resolve(
        self,
        dynamo_util: &DynamoUtil,
        parent_id: &PkSk,
    ) -> Result<O, ServerError> {
        resolve_pass_fetch_or_create(self, dynamo_util, parent_id).await
    }

    /// Resolves passed/fetched input or prepares a uniquely owned creation.
    pub async fn prepare(
        self,
        dynamo_util: &DynamoUtil,
        parent_id: &PkSk,
    ) -> Result<PreparedIngress<O>, ServerError> {
        prepare_pass_fetch_or_create(self, dynamo_util, parent_id).await
    }
}

impl<O> PassFetchOrCreate<O, UnorderedCreateWithParent<O>>
where
    O: DynamoObject,
{
    /// Resolves a passed, fetched, or newly created object.
    pub async fn resolve(self, dynamo_util: &DynamoUtil) -> Result<O, ServerError> {
        resolve_pass_fetch_or_create(self, dynamo_util, PkSk::root()).await
    }

    /// Resolves passed/fetched input or prepares a uniquely owned creation.
    pub async fn prepare(
        self,
        dynamo_util: &DynamoUtil,
    ) -> Result<PreparedIngress<O>, ServerError> {
        prepare_pass_fetch_or_create(self, dynamo_util, PkSk::root()).await
    }
}

impl<O> PassFetchOrCreate<O, OrderedCreateWithParent<O>>
where
    O: DynamoObject,
{
    /// Resolves a passed, fetched, or newly created object.
    pub async fn resolve(self, dynamo_util: &DynamoUtil) -> Result<O, ServerError> {
        resolve_pass_fetch_or_create(self, dynamo_util, PkSk::root()).await
    }

    /// Resolves passed/fetched input or prepares a uniquely owned creation.
    pub async fn prepare(
        self,
        dynamo_util: &DynamoUtil,
    ) -> Result<PreparedIngress<O>, ServerError> {
        prepare_pass_fetch_or_create(self, dynamo_util, PkSk::root()).await
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
    #![allow(clippy::result_large_err)]

    use std::sync::Arc;

    use aws_sdk_dynamodb::operation::put_item::PutItemOutput;
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

    async fn build_util_with_backend(backend: MockDynamoBackend) -> DynamoUtil {
        let ctx = TestCtx::init_test("mock-region".to_string());
        ctx.override_dynamo_backend(Arc::new(backend)).await;
        DynamoUtil::new(&*ctx, "ingress_test").await.unwrap()
    }

    async fn build_util() -> DynamoUtil {
        build_util_with_backend(MockDynamoBackend::new()).await
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
    async fn cloned_ingress_prepares_independent_create_plans() {
        let dynamo_util = build_util().await;
        let ingress = PassFetchOrCreateUnordered::<TestObject>::from(UnorderedCreate {
            data: TestObjectData::default(),
        });
        let cloned = ingress.clone();
        let PreparedIngress::Create(mut first) =
            ingress.prepare(&dynamo_util, PkSk::root()).await.unwrap()
        else {
            panic!("expected create plan");
        };
        let PreparedIngress::Create(second) =
            cloned.prepare(&dynamo_util, PkSk::root()).await.unwrap()
        else {
            panic!("expected create plan");
        };

        assert_ne!(first.id(), second.id());
        let first_id = first.id().clone();
        first.data_mut().value = "updated after reserving ID".into();
        assert_eq!(first.id(), &first_id);
    }

    #[tokio::test]
    async fn simple_resolve_passes_through_existing_object() {
        let dynamo_util = build_util().await;
        let id = PkSk {
            pk: "ROOT".into(),
            sk: "INGRESS_TEST#object".into(),
        };
        let ingress = PassFetchOrCreateUnordered::<TestObject>::from(TestObject::new(
            id.clone(),
            TestObjectData::default(),
        ));

        let object = ingress.resolve(&dynamo_util, PkSk::root()).await.unwrap();

        assert_eq!(object.id(), &id);
    }

    #[tokio::test]
    async fn simple_resolve_creates_with_a_prepared_token() {
        let mut backend = MockDynamoBackend::new();
        backend
            .expect_put_item()
            .returning(|_, _| Ok(PutItemOutput::builder().build()));
        let dynamo_util = build_util_with_backend(backend).await;
        let ingress = PassFetchOrCreateUnordered::<TestObject>::from(UnorderedCreate {
            data: TestObjectData {
                value: "created".into(),
            },
        });

        let object = ingress.resolve(&dynamo_util, PkSk::root()).await.unwrap();

        assert_eq!(object.data.value, "created");
    }
}
