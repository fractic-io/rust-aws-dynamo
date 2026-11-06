use std::sync::{Arc, PoisonError, RwLock};

use async_trait::async_trait;
use fractic_context::register_ctx_singleton;
use fractic_server_error::ServerError;

use crate::{schema::PkSk, DynamoCtxView};

// Public interface.
// ----------------------------------------------------------------------------

/// Told about every write `DynamoUtil` completes, once it has succeeded.
///
/// `ids` are the items the operation created, replaced, updated or deleted in
/// `table`; a batch reports all of its items in one call, a failed batch the
/// items that landed before it failed. An error from the observer fails the
/// operation, although the write itself is already in the table.
#[async_trait]
pub trait DynamoWriteObserver: Send + Sync {
    async fn after_write(&self, table: &str, ids: &[PkSk]) -> Result<(), ServerError>;
}

/// The write observers of one context, notified in subscription order.
///
/// The registered default has none. A consumer that wants to see writes
/// subscribes its observer once, at start-up, before the first write; the
/// library cannot be told to do so through the context, because view-level
/// dependencies have no override outside tests.
#[derive(Default)]
pub struct DynamoWriteObservers {
    observers: RwLock<Vec<Arc<dyn DynamoWriteObserver>>>,
}

impl DynamoWriteObservers {
    pub fn subscribe(&self, observer: Arc<dyn DynamoWriteObserver>) {
        self.observers
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .push(observer);
    }

    pub(crate) async fn notify(&self, table: &str, ids: &[PkSk]) -> Result<(), ServerError> {
        if ids.is_empty() {
            return Ok(());
        }
        let observers = self
            .observers
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        for observer in observers {
            observer.after_write(table, ids).await?;
        }
        Ok(())
    }
}

// Dependency registration.
// ----------------------------------------------------------------------------

register_ctx_singleton!(dyn DynamoCtxView, DynamoWriteObservers, |_ctx: Arc<
    dyn DynamoCtxView,
>| async move {
    Ok(DynamoWriteObservers::default())
});

// Tests.
// ----------------------------------------------------------------------------

#[cfg(test)]
pub(crate) mod tests {
    #![allow(clippy::result_large_err)]

    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use aws_sdk_dynamodb::{
        operation::{
            batch_write_item::BatchWriteItemOutput, delete_item::DeleteItemOutput,
            put_item::PutItemOutput, update_item::UpdateItemOutput,
        },
        types::AttributeValue,
    };
    use fractic_server_error::{CriticalError, ServerError};
    use serde::{Deserialize, Serialize};

    use crate::{
        context::test_ctx::TestCtx,
        dynamo_object,
        schema::{DynamoObject, IdLogic, NestingLogic, PkSk},
        util::{backend::MockDynamoBackend, DynamoUtil},
    };

    use super::{DynamoWriteObserver, DynamoWriteObservers};

    #[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq)]
    pub struct ObservedData {
        value: String,
    }
    dynamo_object!(
        Observed,
        ObservedData,
        "OBSERVED",
        IdLogic::UuidV4,
        NestingLogic::Root
    );

    /// Records every notification as `(table, ids)`.
    #[derive(Default)]
    pub(crate) struct RecordingObserver {
        pub writes: Mutex<Vec<(String, Vec<PkSk>)>>,
    }

    #[async_trait]
    impl DynamoWriteObserver for RecordingObserver {
        async fn after_write(&self, table: &str, ids: &[PkSk]) -> Result<(), ServerError> {
            self.writes
                .lock()
                .unwrap()
                .push((table.to_string(), ids.to_vec()));
            Ok(())
        }
    }

    struct FailingObserver;

    #[async_trait]
    impl DynamoWriteObserver for FailingObserver {
        async fn after_write(&self, _table: &str, _ids: &[PkSk]) -> Result<(), ServerError> {
            Err(CriticalError::new("observer failed"))
        }
    }

    async fn util_with(
        backend: MockDynamoBackend,
        observer: Arc<dyn DynamoWriteObserver>,
    ) -> DynamoUtil {
        let ctx = TestCtx::init_test("mock-region".to_string());
        ctx.override_dynamo_backend(Arc::new(backend)).await;
        let observers = DynamoWriteObservers::default();
        observers.subscribe(observer);
        ctx.override_dynamo_write_observers(Arc::new(observers))
            .await;
        DynamoUtil::new(&*ctx, "my_table").await.unwrap()
    }

    fn root_id(sk: &str) -> PkSk {
        PkSk {
            pk: "ROOT".to_string(),
            sk: sk.to_string(),
        }
    }

    #[tokio::test]
    async fn create_update_delete_notify_with_the_item_id() {
        let mut backend = MockDynamoBackend::new();
        backend
            .expect_put_item()
            .returning(|_, _| Ok(PutItemOutput::builder().build()));
        backend
            .expect_update_item()
            .returning(|_, _, _, _, _, _| Ok(UpdateItemOutput::builder().build()));
        backend
            .expect_delete_item()
            .returning(|_, _| Ok(DeleteItemOutput::builder().build()));
        let observer = Arc::new(RecordingObserver::default());
        let util = util_with(backend, observer.clone()).await;

        let created = util
            .create_item::<Observed>(PkSk::root(), ObservedData::default())
            .await
            .unwrap();
        util.update_item(&created).await.unwrap();
        util.delete_item::<Observed>(created.id().clone())
            .await
            .unwrap();

        let writes = observer.writes.lock().unwrap();
        assert_eq!(writes.len(), 3);
        for (table, ids) in writes.iter() {
            assert_eq!(table, "my_table");
            assert_eq!(ids, &vec![created.id().clone()]);
        }
    }

    #[tokio::test]
    async fn batches_notify_once_with_every_id() {
        let mut backend = MockDynamoBackend::new();
        backend
            .expect_batch_put_item()
            .returning(|_, _| Ok(BatchWriteItemOutput::builder().build()));
        backend
            .expect_batch_delete_item()
            .returning(|_, _| Ok(BatchWriteItemOutput::builder().build()));
        let observer = Arc::new(RecordingObserver::default());
        let util = util_with(backend, observer.clone()).await;

        let put_ids = (0..30).map(|i| root_id(&format!("OBSERVED#{i}")));
        let items = put_ids
            .clone()
            .map(|id| {
                let mut map = crate::util::DynamoMap::new();
                id.write_to_map(&mut map);
                map.insert("value".to_string(), AttributeValue::S("v".to_string()));
                map
            })
            .collect();
        util.raw_batch_put_item(items).await.unwrap();
        util.raw_batch_delete_ids(put_ids.clone().collect())
            .await
            .unwrap();

        let writes = observer.writes.lock().unwrap();
        assert_eq!(writes.len(), 2);
        assert_eq!(writes[0].1, put_ids.clone().collect::<Vec<_>>());
        assert_eq!(writes[1].1, put_ids.collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn observer_error_fails_the_write_operation() {
        let mut backend = MockDynamoBackend::new();
        backend
            .expect_put_item()
            .returning(|_, _| Ok(PutItemOutput::builder().build()));
        let util = util_with(backend, Arc::new(FailingObserver)).await;

        let result = util
            .create_item::<Observed>(PkSk::root(), ObservedData::default())
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn no_observers_and_no_ids_are_free() {
        let observers = DynamoWriteObservers::default();
        observers.notify("t", &[]).await.unwrap();
        observers
            .notify("t", &[root_id("OBSERVED#1")])
            .await
            .unwrap();
    }
}
