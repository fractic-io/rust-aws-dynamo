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
pub trait DynamoCrudOperations: Send + Sync {
    async fn recursive_delete(&self, id: PkSk) -> Result<(), ServerError>;
}

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

/// Type-safe accessor for CRUD operations on a child object (i.e. has a
/// parent).
#[async_trait]
pub trait ManageChild<O>: Send + Sync
where
    O: DynamoObject,
{
    type Parent: DynamoObject;

    // Item operations:
    async fn get(&self, id: PkSk) -> Result<O, ServerError>;
    async fn add(
        &self,
        parent: &Self::Parent,
        data: O::Data,
        after: Option<&O>,
    ) -> Result<O, ServerError>;
    async fn batch_add(
        &self,
        parent: &Self::Parent,
        data: Vec<O::Data>,
        after: Option<&O>,
    ) -> Result<Vec<O>, ServerError>;
    async fn update(&self, item: &O) -> Result<(), ServerError>;
    async fn delete(&self, item: O) -> Result<O::Data, ServerError>;
    async fn batch_delete(&self, items: Vec<O>) -> Result<Vec<O::Data>, ServerError>;

    // Global operations:
    async fn query_all(&self, parent: &Self::Parent) -> Result<Vec<O>, ServerError>;
    async fn batch_delete_all(&self, parent: &Self::Parent) -> Result<(), ServerError>;
}

/// Type-safe accessor for CRUD operations on a child object, which itself also
/// has children.
#[async_trait]
pub trait ManageChildWithChildren<O>: Send + Sync
where
    O: DynamoObject,
{
    type Parent: DynamoObject;

    // Item operations:
    async fn get(&self, id: PkSk) -> Result<O, ServerError>;
    async fn add(
        &self,
        parent: &Self::Parent,
        data: O::Data,
        after: Option<&O>,
    ) -> Result<O, ServerError>;
    async fn batch_add(
        &self,
        parent: &Self::Parent,
        data: Vec<O::Data>,
        after: Option<&O>,
    ) -> Result<Vec<O>, ServerError>;
    async fn update(&self, item: &O) -> Result<(), ServerError>;
    async fn delete_recursive(&self, item: O) -> Result<O::Data, ServerError>;
    async fn delete_non_recursive(&self, item: O) -> Result<O::Data, ServerError>;
    async fn batch_delete_non_recursive(&self, items: Vec<O>) -> Result<Vec<O::Data>, ServerError>;

    // Global operations:
    async fn query_all(&self, parent: &Self::Parent) -> Result<Vec<O>, ServerError>;
    async fn batch_delete_all_non_recursive(
        &self,
        parent: &Self::Parent,
    ) -> Result<(), ServerError>;
}

/// Type-safe accessor for CRUD operations on a child with
/// IdLogic::BatchOptimized.
#[async_trait]
pub trait ManageBatchChild<O>: Send + Sync
where
    O: DynamoObject,
{
    type Parent: DynamoObject;

    // Batch operations:
    async fn query_all(&self, parent: &Self::Parent) -> Result<Vec<O>, ServerError>;
    async fn batch_delete_all(&self, parent: &Self::Parent) -> Result<(), ServerError>;
    async fn batch_replace_all_ordered(
        &self,
        parent: &Self::Parent,
        data: Vec<O::Data>,
    ) -> Result<(), ServerError>;
}

// Implementation.
// ===========================================================================

pub struct ManageRootImpl<O: DynamoObject> {
    dynamo_util: Arc<DynamoUtil>,
    _crud_operations: Arc<dyn DynamoCrudOperations>,
    _phantom: PhantomData<O>,
}

impl<O: DynamoObject> ManageRootImpl<O> {
    pub fn new(
        dynamo_util: Arc<DynamoUtil>,
        crud_operations: Arc<dyn DynamoCrudOperations>,
    ) -> Self {
        Self {
            dynamo_util,
            _crud_operations: crud_operations,
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
    crud_operations: Arc<dyn DynamoCrudOperations>,
    _phantom: PhantomData<O>,
}

impl<O: DynamoObject> ManageRootWithChildrenImpl<O> {
    pub fn new(
        dynamo_util: Arc<DynamoUtil>,
        crud_operations: Arc<dyn DynamoCrudOperations>,
    ) -> Self {
        Self {
            dynamo_util,
            crud_operations,
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
        self.crud_operations
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

pub struct ManageChildImpl<O: DynamoObject, P: DynamoObject> {
    dynamo_util: Arc<DynamoUtil>,
    _crud_operations: Arc<dyn DynamoCrudOperations>,
    _phantom: PhantomData<(O, P)>,
}

impl<O: DynamoObject, P: DynamoObject> ManageChildImpl<O, P> {
    pub fn new(
        dynamo_util: Arc<DynamoUtil>,
        crud_operations: Arc<dyn DynamoCrudOperations>,
    ) -> Self {
        Self {
            dynamo_util,
            _crud_operations: crud_operations,
            _phantom: PhantomData::<(O, P)>,
        }
    }
}

#[async_trait]
impl<O, P> ManageChild<O> for ManageChildImpl<O, P>
where
    O: DynamoObject,
    P: DynamoObject,
{
    type Parent = P;

    // Item operations:
    // -----------------------------------------------------------------------

    async fn get(&self, id: PkSk) -> Result<O, ServerError> {
        self.dynamo_util
            .get_item::<O>(id)
            .await?
            .ok_or_else(|| DynamoNotFound::new())
    }

    async fn add(&self, parent: &P, data: O::Data, after: Option<&O>) -> Result<O, ServerError> {
        let insert_position = match after {
            Some(a) => DynamoInsertPosition::After(a.id().clone()),
            None => DynamoInsertPosition::Last,
        };
        self.dynamo_util
            .create_item_ordered::<O>(parent.id().clone(), data, insert_position)
            .await
    }

    async fn batch_add(
        &self,
        parent: &P,
        data: Vec<O::Data>,
        after: Option<&O>,
    ) -> Result<Vec<O>, ServerError> {
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

    async fn query_all(&self, parent: &P) -> Result<Vec<O>, ServerError> {
        self.dynamo_util.query_all::<O>(parent.id().clone()).await
    }

    async fn batch_delete_all(&self, parent: &P) -> Result<(), ServerError> {
        self.dynamo_util
            .batch_delete_all::<O>(parent.id().clone())
            .await
    }
}

pub struct ManageChildWithChildrenImpl<O: DynamoObject, P: DynamoObject> {
    dynamo_util: Arc<DynamoUtil>,
    crud_operations: Arc<dyn DynamoCrudOperations>,
    _phantom: PhantomData<(O, P)>,
}

impl<O: DynamoObject, P: DynamoObject> ManageChildWithChildrenImpl<O, P> {
    pub fn new(
        dynamo_util: Arc<DynamoUtil>,
        crud_operations: Arc<dyn DynamoCrudOperations>,
    ) -> Self {
        Self {
            dynamo_util,
            crud_operations,
            _phantom: PhantomData::<(O, P)>,
        }
    }
}

#[async_trait]
impl<O, P> ManageChildWithChildren<O> for ManageChildWithChildrenImpl<O, P>
where
    O: DynamoObject,
    P: DynamoObject,
{
    type Parent = P;

    // Item operations:
    // -----------------------------------------------------------------------

    async fn get(&self, id: PkSk) -> Result<O, ServerError> {
        self.dynamo_util
            .get_item::<O>(id)
            .await?
            .ok_or_else(|| DynamoNotFound::new())
    }

    async fn add(&self, parent: &P, data: O::Data, after: Option<&O>) -> Result<O, ServerError> {
        let insert_position = match after {
            Some(a) => DynamoInsertPosition::After(a.id().clone()),
            None => DynamoInsertPosition::Last,
        };
        self.dynamo_util
            .create_item_ordered::<O>(parent.id().clone(), data, insert_position)
            .await
    }

    async fn batch_add(
        &self,
        parent: &P,
        data: Vec<O::Data>,
        after: Option<&O>,
    ) -> Result<Vec<O>, ServerError> {
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
        self.crud_operations
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

    async fn query_all(&self, parent: &P) -> Result<Vec<O>, ServerError> {
        self.dynamo_util.query_all::<O>(parent.id().clone()).await
    }

    async fn batch_delete_all_non_recursive(&self, parent: &P) -> Result<(), ServerError> {
        self.dynamo_util
            .batch_delete_all::<O>(parent.id().clone())
            .await
    }
}

pub struct ManageBatchChildImpl<O: DynamoObject, P: DynamoObject> {
    dynamo_util: Arc<DynamoUtil>,
    _crud_operations: Arc<dyn DynamoCrudOperations>,
    _phantom: PhantomData<(O, P)>,
}

impl<O: DynamoObject, P: DynamoObject> ManageBatchChildImpl<O, P> {
    pub fn new(
        dynamo_util: Arc<DynamoUtil>,
        crud_operations: Arc<dyn DynamoCrudOperations>,
    ) -> Self {
        Self {
            dynamo_util,
            _crud_operations: crud_operations,
            _phantom: PhantomData::<(O, P)>,
        }
    }
}

#[async_trait]
impl<O, P> ManageBatchChild<O> for ManageBatchChildImpl<O, P>
where
    O: DynamoObject,
    P: DynamoObject,
{
    type Parent = P;

    // Batch operations:
    // -----------------------------------------------------------------------

    async fn query_all(&self, parent: &P) -> Result<Vec<O>, ServerError> {
        self.dynamo_util.query_all::<O>(parent.id().clone()).await
    }

    async fn batch_delete_all(&self, parent: &P) -> Result<(), ServerError> {
        self.dynamo_util
            .batch_delete_all::<O>(parent.id().clone())
            .await
    }

    async fn batch_replace_all_ordered(
        &self,
        parent: &P,
        data: Vec<O::Data>,
    ) -> Result<(), ServerError> {
        self.dynamo_util
            .batch_replace_all_ordered::<O>(parent.id().clone(), data)
            .await
    }
}
