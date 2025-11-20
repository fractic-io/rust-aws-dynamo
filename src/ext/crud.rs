//! Series of type-safe CRUD operation wrappers.

use std::{marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use fractic_server_error::ServerError;

use crate::{
    errors::DynamoNotFound,
    schema::{DynamoObject, PkSk},
    util::{DynamoInsertPosition, DynamoUtil},
};

/// Trait that must be implemented to use type-safe CRUD operation wrappers.
#[async_trait]
pub trait DynamoCrudAlgorithms: Send + Sync {
    async fn recursive_delete(&self, id: PkSk) -> Result<(), ServerError>;
}

/// Marker trait used to validate whether a given type is a valid parent of `O`.
/// Multiple valid parents are supported.
///
/// Example:
/// ```rust,ignore
/// struct Parent1;
/// struct Parent2;
/// struct Child;
///
/// impl ParentOf<Child> for Parent1 {}
/// impl ParentOf<Child> for Parent2 {}
/// ```
pub trait ParentOf<O: DynamoObject>: DynamoObject {}

/// Type-safe accessor for CRUD operations on a root object.
pub struct ManageRoot<O: DynamoObject> {
    dynamo_util: Arc<DynamoUtil>,
    _crud_algorithms: Arc<dyn DynamoCrudAlgorithms>,
    _phantom: PhantomData<O>,
}

impl<O: DynamoObject> ManageRoot<O> {
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

    // Item operations:
    // -----------------------------------------------------------------------

    pub async fn get(&self, id: PkSk) -> Result<O, ServerError> {
        self.dynamo_util
            .get_item::<O>(id)
            .await?
            .ok_or_else(|| DynamoNotFound::new())
    }

    pub async fn add(&self, data: O::Data) -> Result<O, ServerError> {
        self.dynamo_util.create_item::<O>(PkSk::root(), data).await
    }

    pub async fn batch_add(&self, data: Vec<O::Data>) -> Result<Vec<O>, ServerError> {
        self.dynamo_util
            .batch_create_item::<O>(PkSk::root(), data)
            .await
    }

    pub async fn update(&self, item: &O) -> Result<(), ServerError> {
        self.dynamo_util.update_item(item).await
    }

    pub async fn delete(&self, item: O) -> Result<O::Data, ServerError> {
        self.dynamo_util.delete_item::<O>(item.id().clone()).await?;
        Ok(item.into_data())
    }

    pub async fn batch_delete(&self, items: Vec<O>) -> Result<Vec<O::Data>, ServerError> {
        self.dynamo_util
            .batch_delete_item::<O>(items.iter().map(|i| i.id().clone()).collect())
            .await?;
        Ok(items.into_iter().map(DynamoObject::into_data).collect())
    }

    // Global operations:
    // -----------------------------------------------------------------------

    pub async fn query_all(&self) -> Result<Vec<O>, ServerError> {
        self.dynamo_util.query_all::<O>(PkSk::root()).await
    }

    pub async fn batch_delete_all(&self) -> Result<(), ServerError> {
        self.dynamo_util.batch_delete_all::<O>(PkSk::root()).await
    }
}

/// Type-safe accessor for CRUD operations on a root object with children.
pub struct ManageRootWithChildren<O: DynamoObject> {
    dynamo_util: Arc<DynamoUtil>,
    crud_algorithms: Arc<dyn DynamoCrudAlgorithms>,
    _phantom: PhantomData<O>,
}

impl<O: DynamoObject> ManageRootWithChildren<O> {
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

    // Item operations:
    // -----------------------------------------------------------------------

    pub async fn get(&self, id: PkSk) -> Result<O, ServerError> {
        self.dynamo_util
            .get_item::<O>(id)
            .await?
            .ok_or_else(|| DynamoNotFound::new())
    }

    pub async fn add(&self, data: O::Data) -> Result<O, ServerError> {
        self.dynamo_util.create_item::<O>(PkSk::root(), data).await
    }

    pub async fn batch_add(&self, data: Vec<O::Data>) -> Result<Vec<O>, ServerError> {
        self.dynamo_util
            .batch_create_item::<O>(PkSk::root(), data)
            .await
    }

    pub async fn update(&self, item: &O) -> Result<(), ServerError> {
        self.dynamo_util.update_item(item).await
    }

    pub async fn delete_recursive(&self, item: O) -> Result<O::Data, ServerError> {
        self.crud_algorithms
            .recursive_delete(item.id().clone())
            .await?;
        Ok(item.into_data())
    }

    pub async fn delete_non_recursive(&self, item: O) -> Result<O::Data, ServerError> {
        self.dynamo_util.delete_item::<O>(item.id().clone()).await?;
        Ok(item.into_data())
    }

    pub async fn batch_delete_non_recursive(
        &self,
        items: Vec<O>,
    ) -> Result<Vec<O::Data>, ServerError> {
        self.dynamo_util
            .batch_delete_item::<O>(items.iter().map(|i| i.id().clone()).collect())
            .await?;
        Ok(items.into_iter().map(DynamoObject::into_data).collect())
    }

    // Global operations:
    // -----------------------------------------------------------------------

    pub async fn query_all(&self) -> Result<Vec<O>, ServerError> {
        self.dynamo_util.query_all::<O>(PkSk::root()).await
    }

    pub async fn batch_delete_all_non_recursive(&self) -> Result<(), ServerError> {
        self.dynamo_util.batch_delete_all::<O>(PkSk::root()).await
    }
}

/// Type-safe accessor for CRUD operations on an ordered child object (i.e. has
/// a parent, and whose `add` and `batch_add` operations should use more costly
/// sort-key-based insertion).
pub struct ManageOrderedChild<O: DynamoObject> {
    dynamo_util: Arc<DynamoUtil>,
    _crud_algorithms: Arc<dyn DynamoCrudAlgorithms>,
    _phantom: PhantomData<O>,
}

impl<O: DynamoObject> ManageOrderedChild<O> {
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

    // Item operations:
    // -----------------------------------------------------------------------

    pub async fn get(&self, id: PkSk) -> Result<O, ServerError> {
        self.dynamo_util
            .get_item::<O>(id)
            .await?
            .ok_or_else(|| DynamoNotFound::new())
    }

    pub async fn add<P>(
        &self,
        parent: &P,
        data: O::Data,
        after: Option<&O>,
    ) -> Result<O, ServerError>
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

    pub async fn batch_add<P>(
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

    pub async fn update(&self, item: &O) -> Result<(), ServerError> {
        self.dynamo_util.update_item(item).await
    }

    pub async fn delete(&self, item: O) -> Result<O::Data, ServerError> {
        self.dynamo_util.delete_item::<O>(item.id().clone()).await?;
        Ok(item.into_data())
    }

    pub async fn batch_delete(&self, items: Vec<O>) -> Result<Vec<O::Data>, ServerError> {
        self.dynamo_util
            .batch_delete_item::<O>(items.iter().map(|i| i.id().clone()).collect())
            .await?;
        Ok(items.into_iter().map(DynamoObject::into_data).collect())
    }

    // Global operations:
    // -----------------------------------------------------------------------

    pub async fn query_all<P>(&self, parent: &P) -> Result<Vec<O>, ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        self.dynamo_util.query_all::<O>(parent.id().clone()).await
    }

    pub async fn batch_delete_all<P>(&self, parent: &P) -> Result<(), ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        self.dynamo_util
            .batch_delete_all::<O>(parent.id().clone())
            .await
    }
}

/// Type-safe accessor for CRUD operations on an ordered child object, which
/// itself also has children.
pub struct ManageOrderedChildWithChildren<O: DynamoObject> {
    dynamo_util: Arc<DynamoUtil>,
    crud_algorithms: Arc<dyn DynamoCrudAlgorithms>,
    _phantom: PhantomData<O>,
}

impl<O: DynamoObject> ManageOrderedChildWithChildren<O> {
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

    // Item operations:
    // -----------------------------------------------------------------------

    pub async fn get(&self, id: PkSk) -> Result<O, ServerError> {
        self.dynamo_util
            .get_item::<O>(id)
            .await?
            .ok_or_else(|| DynamoNotFound::new())
    }

    pub async fn add<P>(
        &self,
        parent: &P,
        data: O::Data,
        after: Option<&O>,
    ) -> Result<O, ServerError>
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

    pub async fn batch_add<P>(
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

    pub async fn update(&self, item: &O) -> Result<(), ServerError> {
        self.dynamo_util.update_item(item).await
    }

    pub async fn delete_recursive(&self, item: O) -> Result<O::Data, ServerError> {
        self.crud_algorithms
            .recursive_delete(item.id().clone())
            .await?;
        Ok(item.into_data())
    }

    pub async fn delete_non_recursive(&self, item: O) -> Result<O::Data, ServerError> {
        self.dynamo_util.delete_item::<O>(item.id().clone()).await?;
        Ok(item.into_data())
    }

    pub async fn batch_delete_non_recursive(
        &self,
        items: Vec<O>,
    ) -> Result<Vec<O::Data>, ServerError> {
        self.dynamo_util
            .batch_delete_item::<O>(items.iter().map(|i| i.id().clone()).collect())
            .await?;
        Ok(items.into_iter().map(DynamoObject::into_data).collect())
    }

    // Global operations:
    // -----------------------------------------------------------------------

    pub async fn query_all<P>(&self, parent: &P) -> Result<Vec<O>, ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        self.dynamo_util.query_all::<O>(parent.id().clone()).await
    }

    pub async fn batch_delete_all_non_recursive<P>(&self, parent: &P) -> Result<(), ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        self.dynamo_util
            .batch_delete_all::<O>(parent.id().clone())
            .await
    }
}

/// Type-safe accessor for CRUD operations on an unordered child object (i.e.
/// has a parent, and whose order should be based directly on the ID logic,
/// avoiding costly sort-key determination).
pub struct ManageUnorderedChild<O: DynamoObject> {
    dynamo_util: Arc<DynamoUtil>,
    _crud_algorithms: Arc<dyn DynamoCrudAlgorithms>,
    _phantom: PhantomData<O>,
}

impl<O: DynamoObject> ManageUnorderedChild<O> {
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

    // Item operations:
    // -----------------------------------------------------------------------

    pub async fn get(&self, id: PkSk) -> Result<O, ServerError> {
        self.dynamo_util
            .get_item::<O>(id)
            .await?
            .ok_or_else(|| DynamoNotFound::new())
    }

    pub async fn add<P>(&self, parent: &P, data: O::Data) -> Result<O, ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        self.dynamo_util
            .create_item::<O>(parent.id().clone(), data)
            .await
    }

    pub async fn batch_add<P>(&self, parent: &P, data: Vec<O::Data>) -> Result<Vec<O>, ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        self.dynamo_util
            .batch_create_item::<O>(parent.id().clone(), data)
            .await
    }

    pub async fn update(&self, item: &O) -> Result<(), ServerError> {
        self.dynamo_util.update_item(item).await
    }

    pub async fn delete(&self, item: O) -> Result<O::Data, ServerError> {
        self.dynamo_util.delete_item::<O>(item.id().clone()).await?;
        Ok(item.into_data())
    }

    pub async fn batch_delete(&self, items: Vec<O>) -> Result<Vec<O::Data>, ServerError> {
        self.dynamo_util
            .batch_delete_item::<O>(items.iter().map(|i| i.id().clone()).collect())
            .await?;
        Ok(items.into_iter().map(DynamoObject::into_data).collect())
    }

    // Global operations:
    // -----------------------------------------------------------------------

    pub async fn query_all<P>(&self, parent: &P) -> Result<Vec<O>, ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        self.dynamo_util.query_all::<O>(parent.id().clone()).await
    }

    pub async fn batch_delete_all<P>(&self, parent: &P) -> Result<(), ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        self.dynamo_util
            .batch_delete_all::<O>(parent.id().clone())
            .await
    }
}

/// Type-safe accessor for CRUD operations on an unordered child object, which
/// itself also has children.
pub struct ManageUnorderedChildWithChildren<O: DynamoObject> {
    dynamo_util: Arc<DynamoUtil>,
    crud_algorithms: Arc<dyn DynamoCrudAlgorithms>,
    _phantom: PhantomData<O>,
}

impl<O: DynamoObject> ManageUnorderedChildWithChildren<O> {
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

    // Item operations:
    // -----------------------------------------------------------------------

    pub async fn get(&self, id: PkSk) -> Result<O, ServerError> {
        self.dynamo_util
            .get_item::<O>(id)
            .await?
            .ok_or_else(|| DynamoNotFound::new())
    }

    pub async fn add<P>(&self, parent: &P, data: O::Data) -> Result<O, ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        self.dynamo_util
            .create_item::<O>(parent.id().clone(), data)
            .await
    }

    pub async fn batch_add<P>(&self, parent: &P, data: Vec<O::Data>) -> Result<Vec<O>, ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        self.dynamo_util
            .batch_create_item::<O>(parent.id().clone(), data)
            .await
    }

    pub async fn update(&self, item: &O) -> Result<(), ServerError> {
        self.dynamo_util.update_item(item).await
    }

    pub async fn delete_recursive(&self, item: O) -> Result<O::Data, ServerError> {
        self.crud_algorithms
            .recursive_delete(item.id().clone())
            .await?;
        Ok(item.into_data())
    }

    pub async fn delete_non_recursive(&self, item: O) -> Result<O::Data, ServerError> {
        self.dynamo_util.delete_item::<O>(item.id().clone()).await?;
        Ok(item.into_data())
    }

    pub async fn batch_delete_non_recursive(
        &self,
        items: Vec<O>,
    ) -> Result<Vec<O::Data>, ServerError> {
        self.dynamo_util
            .batch_delete_item::<O>(items.iter().map(|i| i.id().clone()).collect())
            .await?;
        Ok(items.into_iter().map(DynamoObject::into_data).collect())
    }

    // Global operations:
    // -----------------------------------------------------------------------

    pub async fn query_all<P>(&self, parent: &P) -> Result<Vec<O>, ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        self.dynamo_util.query_all::<O>(parent.id().clone()).await
    }

    pub async fn batch_delete_all_non_recursive<P>(&self, parent: &P) -> Result<(), ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        self.dynamo_util
            .batch_delete_all::<O>(parent.id().clone())
            .await
    }
}

/// Type-safe accessor for CRUD operations on a child with
/// IdLogic::BatchOptimized.
pub struct ManageBatchChild<O: DynamoObject> {
    dynamo_util: Arc<DynamoUtil>,
    _crud_algorithms: Arc<dyn DynamoCrudAlgorithms>,
    _phantom: PhantomData<O>,
}

impl<O: DynamoObject> ManageBatchChild<O> {
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

    // Batch operations:
    // -----------------------------------------------------------------------

    pub async fn query_all<P>(&self, parent: &P) -> Result<Vec<O>, ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        self.dynamo_util.query_all::<O>(parent.id().clone()).await
    }

    pub async fn batch_delete_all<P>(&self, parent: &P) -> Result<(), ServerError>
    where
        P: DynamoObject + ParentOf<O>,
    {
        self.dynamo_util
            .batch_delete_all::<O>(parent.id().clone())
            .await
    }

    pub async fn batch_replace_all_ordered<P>(
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
