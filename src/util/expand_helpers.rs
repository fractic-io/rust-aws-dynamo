use fractic_server_error::{CriticalError, ServerError};
use serde::Serialize;

use crate::{
    schema::{
        identifiers::place_terminal_segment, parsing::build_dynamo_map_internal, DynamoObject,
        PkSk, Timestamp,
    },
    util::{
        metadata_helpers::WithMetadataFrom as _, DynamoMap, AUTO_FIELDS_CREATED_AT,
        AUTO_FIELDS_SORT, AUTO_FIELDS_TTL, AUTO_FIELDS_UPDATED_AT, EXPAND_DATA_RESERVED_KEY,
    },
};

// Core structs.
// ----------------------------------------------------------------------------

/// Wrapper struct used to store a batch of items that should be expanded on
/// query.
#[derive(Serialize)]
struct ExpandableBatch<T: DynamoObject> {
    #[serde(rename = "..")]
    items: Vec<T::Data>,
}

// Read logic.
// ----------------------------------------------------------------------------

pub(crate) fn expand_batched_items(items: Vec<DynamoMap>) -> Vec<DynamoMap> {
    items
        .into_iter()
        .flat_map(|mut item| match item.remove(EXPAND_DATA_RESERVED_KEY) {
            Some(aws_sdk_dynamodb::types::AttributeValue::L(children)) => children
                .into_iter()
                .filter_map(|child| match child {
                    aws_sdk_dynamodb::types::AttributeValue::M(inner_map) => {
                        Some(inner_map.with_metadata_from(&item))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>(),
            _ => vec![item],
        })
        .collect()
}

// Write logic.
// ----------------------------------------------------------------------------

pub(crate) fn build_expandable_batch_maps<T: DynamoObject>(
    parent_id: &PkSk,
    data: Vec<T::Data>,
    batch_size: usize,
) -> Result<Vec<DynamoMap>, ServerError> {
    let num_batches = data.len().div_ceil(batch_size);

    let index_digits = match num_batches {
        0 => unreachable!("data.is_empty() should be checked by caller"),
        1 => 0,
        n => (n - 1).ilog10() as usize + 1,
    };

    data.chunks(batch_size)
        .enumerate()
        .map(|(i, batch)| {
            let new_obj_id = if index_digits == 0 {
                format!("{}#-", T::id_label())
            } else {
                format!("{}#{i:0index_digits$}", T::id_label())
            };
            let PkSk { pk, sk } = place_terminal_segment::<T>(parent_id, &new_obj_id);

            let expandable_batch = ExpandableBatch::<T> {
                items: batch.to_vec(),
            };
            let now = Timestamp::now();
            build_dynamo_map_internal::<ExpandableBatch<T>>(
                &expandable_batch,
                Some(pk),
                Some(sk),
                Some(vec![
                    (AUTO_FIELDS_CREATED_AT, Box::new(now.clone())),
                    (AUTO_FIELDS_UPDATED_AT, Box::new(now)),
                    (AUTO_FIELDS_SORT, Box::new(None::<f64>)),
                    (AUTO_FIELDS_TTL, Box::new(None::<i64>)),
                ]),
            )
            .map(|res| res.0)
        })
        .collect::<Result<_, _>>()
        .map_err(|e| {
            CriticalError::with_debug(
                "failed to build expandable batch maps for batch_replace_all_ordered",
                &e,
            )
        })
}
