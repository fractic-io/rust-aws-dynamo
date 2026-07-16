use aws_sdk_dynamodb::operation::batch_write_item::BatchWriteItemOutput;
use fractic_server_error::{CriticalError, ServerError};

use super::DynamoMap;

pub(super) const MAX_BATCH_WRITE_RETRIES: usize = 3;
pub(super) const MAX_BATCH_READ_RETRIES: usize = 3;
const BATCH_RETRY_BASE_DELAY_MS: u64 = 25;

pub(super) fn unprocessed_delete_keys(
    response: &BatchWriteItemOutput,
    table: &str,
) -> Result<Vec<DynamoMap>, ServerError> {
    response
        .unprocessed_items()
        .and_then(|items| items.get(table))
        .into_iter()
        .flatten()
        .map(|request| {
            request
                .delete_request()
                .map(|delete| delete.key().clone())
                .ok_or_else(|| {
                    CriticalError::new(
                        "DynamoDB returned a non-delete request for an unprocessed batch delete",
                    )
                })
        })
        .collect()
}

pub(super) fn unprocessed_put_items(
    response: &BatchWriteItemOutput,
    table: &str,
) -> Result<Vec<DynamoMap>, ServerError> {
    response
        .unprocessed_items()
        .and_then(|items| items.get(table))
        .into_iter()
        .flatten()
        .map(|request| {
            request
                .put_request()
                .map(|put| put.item().clone())
                .ok_or_else(|| {
                    CriticalError::new(
                        "DynamoDB returned a non-put request for an unprocessed batch put",
                    )
                })
        })
        .collect()
}

pub(super) async fn wait_before_batch_retry(attempt: usize) {
    tokio::time::sleep(std::time::Duration::from_millis(
        BATCH_RETRY_BASE_DELAY_MS * (1_u64 << attempt),
    ))
    .await;
}
