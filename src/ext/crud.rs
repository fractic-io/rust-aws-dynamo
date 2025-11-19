//! Series of type-safe CRUD operation wrappers.

use std::{marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use fractic_server_error::ServerError;

use crate::{
    errors::DynamoNotFound,
    schema::{DynamoObject, PkSk},
    util::{DynamoInsertPosition, DynamoUtil},
};

// Interface.
// ===========================================================================

/// Trait that must be implemented to use type-safe CRUD operation wrappers.
#[async_trait]
pub trait DynamoCrudAlgorithms: Send + Sync {
    async fn recursive_delete(&self, id: PkSk) -> Result<(), ServerError>;
}

/// Marker trait used to validate whether a given type is a valid parent of `O`.
///
/// Implement this for all parent object types that are allowed to contain `O`.
/// For example:
///   impl ParentOf<PipelineStep> for Pipeline {}
/// or group with a custom marker trait:
///   trait PipelineParent: DynamoObject {}
///   impl PipelineParent for Story {}
///   impl PipelineParent for Journey {}
///   impl<T: PipelineParent> ParentOf<Pipeline> for T {}
pub trait ParentOf<O: DynamoObject>: DynamoObject {}

/// Type-safe accessor for CRUD operations on a root object.
#[async_trait]
pub trait ManageRoot<O>: Send + Sync
where
    O: DynamoObject,
{
    // Item operations:
    async fn get(&self, id: PkSk) -> Result<O, ServerError>;
    async fn add(&self, data: O::Data) -> Result<O, ServerError>;
    async fn batch_add(&self, data: Vec<O::Data>) -> Result<Vec<O>, ServerError>;
    async fn update(&self, item: &O) -> Result<(), ServerError>;
    async fn delete(&self, item: O) -> Result<O::Data, ServerError>;
    async fn batch_delete(&self, items: Vec<O>) -> Result<Vec<O::Data>, ServerError>;

    // Global operations:
    async fn query_all(&self) -> Result<Vec<O>, ServerError>;
    async fn batch_delete_all(&self) -> Result<(), ServerError>;
}

/// Type-safe accessor for CRUD operations on a root object with children.
#[async_trait]
pub trait ManageRootWithChildren<O>: Send + Sync
where
    O: DynamoObject,
{
    // Item operations:
    async fn get(&self, id: PkSk) -> Result<O, ServerError>;
    async fn add(&self, data: O::Data) -> Result<O, ServerError>;
    async fn batch_add(&self, data: Vec<O::Data>) -> Result<Vec<O>, ServerError>;
    async fn update(&self, item: &O) -> Result<(), ServerError>;
    async fn delete_recursive(&self, item: O) -> Result<O::Data, ServerError>;
    async fn delete_non_recursive(&self, item: O) -> Result<O::Data, ServerError>;
    async fn batch_delete_non_recursive(&self, items: Vec<O>) -> Result<Vec<O::Data>, ServerError>;

    // Global operations:
    async fn query_all(&self) -> Result<Vec<O>, ServerError>;
    async fn batch_delete_all_non_recursive(&self) -> Result<(), ServerError>;
}

/// Type-safe accessor for CRUD operations on an ordered child object (i.e. has
/// a parent, and whose `add` and `batch_add` operations should use more costly
/// sort-key-based insertion).
#[async_trait]
pub trait ManageOrderedChild<O>: Send + Sync
where
    O: DynamoObject,
{
    // Item operations:
    async fn get(&self, id: PkSk) -> Result<O, ServerError>;
    async fn add<P>(&self, parent: &P, data: O::Data, after: Option<&O>) -> Result<O, ServerError>
    where
        Self: Sized,
        P: DynamoObject + ParentOf<O>;
    async fn batch_add<P>(
        &self,
        parent: &P,
        data: Vec<O::Data>,
        after: Option<&O>,
    ) -> Result<Vec<O>, ServerError>
    where
        Self: Sized,
        P: DynamoObject + ParentOf<O>;
    async fn update(&self, item: &O) -> Result<(), ServerError>;
    async fn delete(&self, item: O) -> Result<O::Data, ServerError>;
    async fn batch_delete(&self, items: Vec<O>) -> Result<Vec<O::Data>, ServerError>;

    // Global operations:
    async fn query_all<P>(&self, parent: &P) -> Result<Vec<O>, ServerError>
    where
        Self: Sized,
        P: DynamoObject + ParentOf<O>;
    async fn batch_delete_all<P>(&self, parent: &P) -> Result<(), ServerError>
    where
        Self: Sized,
        P: DynamoObject + ParentOf<O>;
}

/// Type-safe accessor for CRUD operations on an ordered child object, which
/// itself also has children.
#[async_trait]
pub trait ManageOrderedChildWithChildren<O>: Send + Sync
where
    O: DynamoObject,
{
    // Item operations:
    async fn get(&self, id: PkSk) -> Result<O, ServerError>;
    async fn add<P>(&self, parent: &P, data: O::Data, after: Option<&O>) -> Result<O, ServerError>
    where
        Self: Sized,
        P: DynamoObject + ParentOf<O>;
    async fn batch_add<P>(
        &self,
        parent: &P,
        data: Vec<O::Data>,
        after: Option<&O>,
    ) -> Result<Vec<O>, ServerError>
    where
        Self: Sized,
        P: DynamoObject + ParentOf<O>;
    async fn update(&self, item: &O) -> Result<(), ServerError>;
    async fn delete_recursive(&self, item: O) -> Result<O::Data, ServerError>;
    async fn delete_non_recursive(&self, item: O) -> Result<O::Data, ServerError>;
    async fn batch_delete_non_recursive(&self, items: Vec<O>) -> Result<Vec<O::Data>, ServerError>;

    // Global operations:
    async fn query_all<P>(&self, parent: &P) -> Result<Vec<O>, ServerError>
    where
        Self: Sized,
        P: DynamoObject + ParentOf<O>;
    async fn batch_delete_all_non_recursive<P>(&self, parent: &P) -> Result<(), ServerError>
    where
        Self: Sized,
        P: DynamoObject + ParentOf<O>;
}

/// Type-safe accessor for CRUD operations on an unordered child object (i.e.
/// has a parent, and whose order should be based directly on the ID logic,
/// avoiding costly sort-key determination).
#[async_trait]
pub trait ManageUnorderedChild<O>: Send + Sync
where
    O: DynamoObject,
{
    // Item operations:
    async fn get(&self, id: PkSk) -> Result<O, ServerError>;
    async fn add<P>(&self, parent: &P, data: O::Data) -> Result<O, ServerError>
    where
        Self: Sized,
        P: DynamoObject + ParentOf<O>;
    async fn batch_add<P>(&self, parent: &P, data: Vec<O::Data>) -> Result<Vec<O>, ServerError>
    where
        Self: Sized,
        P: DynamoObject + ParentOf<O>;
    async fn update(&self, item: &O) -> Result<(), ServerError>;
    async fn delete(&self, item: O) -> Result<O::Data, ServerError>;
    async fn batch_delete(&self, items: Vec<O>) -> Result<Vec<O::Data>, ServerError>;

    // Global operations:
    async fn query_all<P>(&self, parent: &P) -> Result<Vec<O>, ServerError>
    where
        Self: Sized,
        P: DynamoObject + ParentOf<O>;
    async fn batch_delete_all<P>(&self, parent: &P) -> Result<(), ServerError>
    where
        Self: Sized,
        P: DynamoObject + ParentOf<O>;
}

/// Type-safe accessor for CRUD operations on an unordered child object, which
/// itself also has children.
#[async_trait]
pub trait ManageUnorderedChildWithChildren<O>: Send + Sync
where
    O: DynamoObject,
{
    // Item operations:
    async fn get(&self, id: PkSk) -> Result<O, ServerError>;
    async fn add<P>(&self, parent: &P, data: O::Data) -> Result<O, ServerError>
    where
        Self: Sized,
        P: DynamoObject + ParentOf<O>;
    async fn batch_add<P>(&self, parent: &P, data: Vec<O::Data>) -> Result<Vec<O>, ServerError>
    where
        Self: Sized,
        P: DynamoObject + ParentOf<O>;
    async fn update(&self, item: &O) -> Result<(), ServerError>;
    async fn delete_recursive(&self, item: O) -> Result<O::Data, ServerError>;
    async fn delete_non_recursive(&self, item: O) -> Result<O::Data, ServerError>;
    async fn batch_delete_non_recursive(&self, items: Vec<O>) -> Result<Vec<O::Data>, ServerError>;

    // Global operations:
    async fn query_all<P>(&self, parent: &P) -> Result<Vec<O>, ServerError>
    where
        Self: Sized,
        P: DynamoObject + ParentOf<O>;
    async fn batch_delete_all_non_recursive<P>(&self, parent: &P) -> Result<(), ServerError>
    where
        Self: Sized,
        P: DynamoObject + ParentOf<O>;
}

/// Type-safe accessor for CRUD operations on a child with
/// IdLogic::BatchOptimized.
#[async_trait]
pub trait ManageBatchChild<O>: Send + Sync
where
    O: DynamoObject,
{
    // Batch operations:
    async fn query_all<P>(&self, parent: &P) -> Result<Vec<O>, ServerError>
    where
        Self: Sized,
        P: DynamoObject + ParentOf<O>;
    async fn batch_delete_all<P>(&self, parent: &P) -> Result<(), ServerError>
    where
        Self: Sized,
        P: DynamoObject + ParentOf<O>;
    async fn batch_replace_all_ordered<P>(
        &self,
        parent: &P,
        data: Vec<O::Data>,
    ) -> Result<(), ServerError>
    where
        Self: Sized,
        P: DynamoObject + ParentOf<O>;
}

// Implementation.
// ===========================================================================

pub struct ManageRootImpl<O: DynamoObject> {
    dynamo_util: Arc<DynamoUtil>,
    _crud_algorithms: Arc<dyn DynamoCrudAlgorithms>,
    _phantom: PhantomData<O>,
}

impl<O: DynamoObject> ManageRootImpl<O> {
    pub fn new(
        dynamo_util: Arc<DynamoUtil>,
        crud_algorithms: Arc<dyn DynamoCrudAlgorithms>,
    ) -> Self {
        Self {
            dynamo_util,
            _crud_algorithms: crud_algorithms,
            _phantom: PhantomData::<O>,
        }
    }
}

#[async_trait]
impl<O> ManageRoot<O> for ManageRootImpl<O>
where
    O: DynamoObject,
{
    // Item operations:
    // -----------------------------------------------------------------------

    async fn get(&self, id: PkSk) -> Result<O, ServerError> {
        self.dynamo_util
            .get_item::<O>(id)
            .await?
            .ok_or_else(|| DynamoNotFound::new())
    }

    async fn add(&self, data: O::Data) -> Result<O, ServerError> {
        self.dynamo_util
            .create_item::<O>(PkSk::root(), data, None)
            .await
    }

    async fn batch_add(&self, data: Vec<O::Data>) -> Result<Vec<O>, ServerError> {
        self.dynamo_util
            .batch_create_item::<O>(PkSk::root(), data.into_iter().map(|d| (d, None)).collect())
            .await
    }

    async fn update(&self, item: &O) -> Result<(), ServerError> {
        self.dynamo_util.update_item(item).await
    }

    async fn delete(&self, item: O) -> Result<O::Data, ServerError> {
        self.dynamo_util.delete_item::<O>(item.id().clone()).await?;
        Ok(item.into_data())
    }

    async fn batch_delete(&self, items: Vec<O>) -> Result<Vec<O::Data>, ServerError> {
        self.dynamo_util
            .batch_delete_item::<O>(items.iter().map(|i| i.id().clone()).collect())
            .await?;
        Ok(items.into_iter().map(DynamoObject::into_data).collect())
    }

    // Global operations:
    // -----------------------------------------------------------------------

    async fn query_all(&self) -> Result<Vec<O>, ServerError> {
        self.dynamo_util.query_all::<O>(PkSk::root()).await
    }

    async fn batch_delete_all(&self) -> Result<(), ServerError> {
        self.dynamo_util.batch_delete_all::<O>(PkSk::root()).await
    }
}

pub struct ManageRootWithChildrenImpl<O: DynamoObject> {
    dynamo_util: Arc<DynamoUtil>,
    crud_algorithms: Arc<dyn DynamoCrudAlgorithms>,
    _phantom: PhantomData<O>,
}

impl<O: DynamoObject> ManageRootWithChildrenImpl<O> {
    pub fn new(
        dynamo_util: Arc<DynamoUtil>,
        crud_algorithms: Arc<dyn DynamoCrudAlgorithms>,
    ) -> Self {
        Self {
            dynamo_util,
            crud_algorithms,
            _phantom: PhantomData::<O>,
        }
    }
}

#[async_trait]
impl<O> ManageRootWithChildren<O> for ManageRootWithChildrenImpl<O>
where
    O: DynamoObject,
{
    // Item operations:
    // -----------------------------------------------------------------------

    async fn get(&self, id: PkSk) -> Result<O, ServerError> {
        self.dynamo_util
            .get_item::<O>(id)
            .await?
            .ok_or_else(|| DynamoNotFound::new())
    }

    async fn add(&self, data: O::Data) -> Result<O, ServerError> {
        self.dynamo_util
            .create_item::<O>(PkSk::root(), data, None)
            .await
    }

    async fn batch_add(&self, data: Vec<O::Data>) -> Result<Vec<O>, ServerError> {
        self.dynamo_util
            .batch_create_item::<O>(PkSk::root(), data.into_iter().map(|d| (d, None)).collect())
            .await
    }

    async fn update(&self, item: &O) -> Result<(), ServerError> {
        self.dynamo_util.update_item(item).await
    }

    async fn delete_recursive(&self, item: O) -> Result<O::Data, ServerError> {
        self.crud_algorithms
            .recursive_delete(item.id().clone())
            .await?;
        Ok(item.into_data())
    }

    async fn delete_non_recursive(&self, item: O) -> Result<O::Data, ServerError> {
        self.dynamo_util.delete_item::<O>(item.id().clone()).await?;
        Ok(item.into_data())
    }

    async fn batch_delete_non_recursive(&self, items: Vec<O>) -> Result<Vec<O::Data>, ServerError> {
        self.dynamo_util
            .batch_delete_item::<O>(items.iter().map(|i| i.id().clone()).collect())
            .await?;
        Ok(items.into_iter().map(DynamoObject::into_data).collect())
    }

    // Global operations:
    // -----------------------------------------------------------------------

    async fn query_all(&self) -> Result<Vec<O>, ServerError> {
        self.dynamo_util.query_all::<O>(PkSk::root()).await
    }

    async fn batch_delete_all_non_recursive(&self) -> Result<(), ServerError> {
        self.dynamo_util.batch_delete_all::<O>(PkSk::root()).await
    }
}

pub struct ManageOrderedChildImpl<O: DynamoObject> {
    dynamo_util: Arc<DynamoUtil>,
    _crud_algorithms: Arc<dyn DynamoCrudAlgorithms>,
    _phantom: PhantomData<O>,
}

impl<O: DynamoObject> ManageOrderedChildImpl<O> {
    pub fn new(
        dynamo_util: Arc<DynamoUtil>,
        crud_algorithms: Arc<dyn DynamoCrudAlgorithms>,
    ) -> Self {
        Self {
            dynamo_util,
            _crud_algorithms: crud_algorithms,
            _phantom: PhantomData::<O>,
        }
    }
}

#[async_trait]
impl<O> ManageOrderedChild<O> for ManageOrderedChildImpl<O>
where
    O: DynamoObject,
{
    // Item operations:
    // -----------------------------------------------------------------------

    async fn get(&self, id: PkSk) -> Result<O, ServerError> {
        self.dynamo_util
            .get_item::<O>(id)
            .await?
            .ok_or_else(|| DynamoNotFound::new())
    }

    async fn add<P>(&self, parent: &P, data: O::Data, after: Option<&O>) -> Result<O, ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        let insert_position = match after {
            Some(a) => DynamoInsertPosition::After(a.id().clone()),
            None => DynamoInsertPosition::Last,
        };
        self.dynamo_util
            .create_item_ordered::<O>(parent.id().clone(), data, insert_position)
            .await
    }

    async fn batch_add<P>(
        &self,
        parent: &P,
        data: Vec<O::Data>,
        after: Option<&O>,
    ) -> Result<Vec<O>, ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        let insert_position = match after {
            Some(a) => DynamoInsertPosition::After(a.id().clone()),
            None => DynamoInsertPosition::Last,
        };
        self.dynamo_util
            .batch_create_item_ordered::<O>(parent.id().clone(), data, insert_position)
            .await
    }

    async fn update(&self, item: &O) -> Result<(), ServerError> {
        self.dynamo_util.update_item(item).await
    }

    async fn delete(&self, item: O) -> Result<O::Data, ServerError> {
        self.dynamo_util.delete_item::<O>(item.id().clone()).await?;
        Ok(item.into_data())
    }

    async fn batch_delete(&self, items: Vec<O>) -> Result<Vec<O::Data>, ServerError> {
        self.dynamo_util
            .batch_delete_item::<O>(items.iter().map(|i| i.id().clone()).collect())
            .await?;
        Ok(items.into_iter().map(DynamoObject::into_data).collect())
    }

    // Global operations:
    // -----------------------------------------------------------------------

    async fn query_all<P>(&self, parent: &P) -> Result<Vec<O>, ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        self.dynamo_util.query_all::<O>(parent.id().clone()).await
    }

    async fn batch_delete_all<P>(&self, parent: &P) -> Result<(), ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        self.dynamo_util
            .batch_delete_all::<O>(parent.id().clone())
            .await
    }
}

pub struct ManageOrderedChildWithChildrenImpl<O: DynamoObject> {
    dynamo_util: Arc<DynamoUtil>,
    crud_algorithms: Arc<dyn DynamoCrudAlgorithms>,
    _phantom: PhantomData<O>,
}

impl<O: DynamoObject> ManageOrderedChildWithChildrenImpl<O> {
    pub fn new(
        dynamo_util: Arc<DynamoUtil>,
        crud_algorithms: Arc<dyn DynamoCrudAlgorithms>,
    ) -> Self {
        Self {
            dynamo_util,
            crud_algorithms: crud_algorithms,
            _phantom: PhantomData::<O>,
        }
    }
}

#[async_trait]
impl<O> ManageOrderedChildWithChildren<O> for ManageOrderedChildWithChildrenImpl<O>
where
    O: DynamoObject,
{
    // Item operations:
    // -----------------------------------------------------------------------

    async fn get(&self, id: PkSk) -> Result<O, ServerError> {
        self.dynamo_util
            .get_item::<O>(id)
            .await?
            .ok_or_else(|| DynamoNotFound::new())
    }

    async fn add<P>(&self, parent: &P, data: O::Data, after: Option<&O>) -> Result<O, ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        let insert_position = match after {
            Some(a) => DynamoInsertPosition::After(a.id().clone()),
            None => DynamoInsertPosition::Last,
        };
        self.dynamo_util
            .create_item_ordered::<O>(parent.id().clone(), data, insert_position)
            .await
    }

    async fn batch_add<P>(
        &self,
        parent: &P,
        data: Vec<O::Data>,
        after: Option<&O>,
    ) -> Result<Vec<O>, ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        let insert_position = match after {
            Some(a) => DynamoInsertPosition::After(a.id().clone()),
            None => DynamoInsertPosition::Last,
        };
        self.dynamo_util
            .batch_create_item_ordered::<O>(parent.id().clone(), data, insert_position)
            .await
    }

    async fn update(&self, item: &O) -> Result<(), ServerError> {
        self.dynamo_util.update_item(item).await
    }

    async fn delete_recursive(&self, item: O) -> Result<O::Data, ServerError> {
        self.crud_algorithms
            .recursive_delete(item.id().clone())
            .await?;
        Ok(item.into_data())
    }

    async fn delete_non_recursive(&self, item: O) -> Result<O::Data, ServerError> {
        self.dynamo_util.delete_item::<O>(item.id().clone()).await?;
        Ok(item.into_data())
    }

    async fn batch_delete_non_recursive(&self, items: Vec<O>) -> Result<Vec<O::Data>, ServerError> {
        self.dynamo_util
            .batch_delete_item::<O>(items.iter().map(|i| i.id().clone()).collect())
            .await?;
        Ok(items.into_iter().map(DynamoObject::into_data).collect())
    }

    // Global operations:
    // -----------------------------------------------------------------------

    async fn query_all<P>(&self, parent: &P) -> Result<Vec<O>, ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        self.dynamo_util.query_all::<O>(parent.id().clone()).await
    }

    async fn batch_delete_all_non_recursive<P>(&self, parent: &P) -> Result<(), ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        self.dynamo_util
            .batch_delete_all::<O>(parent.id().clone())
            .await
    }
}

pub struct ManageUnorderedChildImpl<O: DynamoObject> {
    dynamo_util: Arc<DynamoUtil>,
    _crud_algorithms: Arc<dyn DynamoCrudAlgorithms>,
    _phantom: PhantomData<O>,
}

impl<O: DynamoObject> ManageUnorderedChildImpl<O> {
    pub fn new(
        dynamo_util: Arc<DynamoUtil>,
        crud_algorithms: Arc<dyn DynamoCrudAlgorithms>,
    ) -> Self {
        Self {
            dynamo_util,
            _crud_algorithms: crud_algorithms,
            _phantom: PhantomData::<O>,
        }
    }
}

#[async_trait]
impl<O> ManageUnorderedChild<O> for ManageUnorderedChildImpl<O>
where
    O: DynamoObject,
{
    // Item operations:
    // -----------------------------------------------------------------------

    async fn get(&self, id: PkSk) -> Result<O, ServerError> {
        self.dynamo_util
            .get_item::<O>(id)
            .await?
            .ok_or_else(|| DynamoNotFound::new())
    }

    async fn add<P>(&self, parent: &P, data: O::Data) -> Result<O, ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        self.dynamo_util
            .create_item::<O>(parent.id().clone(), data, None)
            .await
    }

    async fn batch_add<P>(&self, parent: &P, data: Vec<O::Data>) -> Result<Vec<O>, ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        self.dynamo_util
            .batch_create_item::<O>(
                parent.id().clone(),
                data.into_iter().map(|d| (d, None)).collect(),
            )
            .await
    }

    async fn update(&self, item: &O) -> Result<(), ServerError> {
        self.dynamo_util.update_item(item).await
    }

    async fn delete(&self, item: O) -> Result<O::Data, ServerError> {
        self.dynamo_util.delete_item::<O>(item.id().clone()).await?;
        Ok(item.into_data())
    }

    async fn batch_delete(&self, items: Vec<O>) -> Result<Vec<O::Data>, ServerError> {
        self.dynamo_util
            .batch_delete_item::<O>(items.iter().map(|i| i.id().clone()).collect())
            .await?;
        Ok(items.into_iter().map(DynamoObject::into_data).collect())
    }

    // Global operations:
    // -----------------------------------------------------------------------

    async fn query_all<P>(&self, parent: &P) -> Result<Vec<O>, ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        self.dynamo_util.query_all::<O>(parent.id().clone()).await
    }

    async fn batch_delete_all<P>(&self, parent: &P) -> Result<(), ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        self.dynamo_util
            .batch_delete_all::<O>(parent.id().clone())
            .await
    }
}

pub struct ManageUnorderedChildWithChildrenImpl<O: DynamoObject> {
    dynamo_util: Arc<DynamoUtil>,
    crud_algorithms: Arc<dyn DynamoCrudAlgorithms>,
    _phantom: PhantomData<O>,
}

impl<O: DynamoObject> ManageUnorderedChildWithChildrenImpl<O> {
    pub fn new(
        dynamo_util: Arc<DynamoUtil>,
        crud_algorithms: Arc<dyn DynamoCrudAlgorithms>,
    ) -> Self {
        Self {
            dynamo_util,
            crud_algorithms: crud_algorithms,
            _phantom: PhantomData::<O>,
        }
    }
}

#[async_trait]
impl<O> ManageUnorderedChildWithChildren<O> for ManageUnorderedChildWithChildrenImpl<O>
where
    O: DynamoObject,
{
    // Item operations:
    // -----------------------------------------------------------------------

    async fn get(&self, id: PkSk) -> Result<O, ServerError> {
        self.dynamo_util
            .get_item::<O>(id)
            .await?
            .ok_or_else(|| DynamoNotFound::new())
    }

    async fn add<P>(&self, parent: &P, data: O::Data) -> Result<O, ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        self.dynamo_util
            .create_item::<O>(parent.id().clone(), data, None)
            .await
    }

    async fn batch_add<P>(&self, parent: &P, data: Vec<O::Data>) -> Result<Vec<O>, ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        self.dynamo_util
            .batch_create_item::<O>(
                parent.id().clone(),
                data.into_iter().map(|d| (d, None)).collect(),
            )
            .await
    }

    async fn update(&self, item: &O) -> Result<(), ServerError> {
        self.dynamo_util.update_item(item).await
    }

    async fn delete_recursive(&self, item: O) -> Result<O::Data, ServerError> {
        self.crud_algorithms
            .recursive_delete(item.id().clone())
            .await?;
        Ok(item.into_data())
    }

    async fn delete_non_recursive(&self, item: O) -> Result<O::Data, ServerError> {
        self.dynamo_util.delete_item::<O>(item.id().clone()).await?;
        Ok(item.into_data())
    }

    async fn batch_delete_non_recursive(&self, items: Vec<O>) -> Result<Vec<O::Data>, ServerError> {
        self.dynamo_util
            .batch_delete_item::<O>(items.iter().map(|i| i.id().clone()).collect())
            .await?;
        Ok(items.into_iter().map(DynamoObject::into_data).collect())
    }

    // Global operations:
    // -----------------------------------------------------------------------

    async fn query_all<P>(&self, parent: &P) -> Result<Vec<O>, ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        self.dynamo_util.query_all::<O>(parent.id().clone()).await
    }

    async fn batch_delete_all_non_recursive<P>(&self, parent: &P) -> Result<(), ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        self.dynamo_util
            .batch_delete_all::<O>(parent.id().clone())
            .await
    }
}

pub struct ManageBatchChildImpl<O: DynamoObject> {
    dynamo_util: Arc<DynamoUtil>,
    _crud_algorithms: Arc<dyn DynamoCrudAlgorithms>,
    _phantom: PhantomData<O>,
}

impl<O: DynamoObject> ManageBatchChildImpl<O> {
    pub fn new(
        dynamo_util: Arc<DynamoUtil>,
        crud_algorithms: Arc<dyn DynamoCrudAlgorithms>,
    ) -> Self {
        Self {
            dynamo_util,
            _crud_algorithms: crud_algorithms,
            _phantom: PhantomData::<O>,
        }
    }
}

#[async_trait]
impl<O> ManageBatchChild<O> for ManageBatchChildImpl<O>
where
    O: DynamoObject,
{
    // Batch operations:
    // -----------------------------------------------------------------------

    async fn query_all<P>(&self, parent: &P) -> Result<Vec<O>, ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        self.dynamo_util.query_all::<O>(parent.id().clone()).await
    }

    async fn batch_delete_all<P>(&self, parent: &P) -> Result<(), ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        self.dynamo_util
            .batch_delete_all::<O>(parent.id().clone())
            .await
    }

    async fn batch_replace_all_ordered<P>(
        &self,
        parent: &P,
        data: Vec<O::Data>,
    ) -> Result<(), ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        self.dynamo_util
            .batch_replace_all_ordered::<O>(parent.id().clone(), data)
            .await
    }
}
