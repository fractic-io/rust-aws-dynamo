use fractic_generic_server_error::GenericServerError;
use ordered_float::NotNan;

use crate::{
    errors::DynamoInvalidOperationError,
    schema::{DynamoObject, PkSk},
};

use super::{backend::DynamoBackendImpl, DynamoInsertPosition, DynamoQueryMatchType, DynamoUtil};

#[derive(Debug, PartialEq, Eq)]
struct OrderedItem<'a> {
    id: &'a PkSk,
    sort: NotNan<f64>,
}
impl PartialOrd for OrderedItem<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.sort.partial_cmp(&other.sort)
    }
}
impl Ord for OrderedItem<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.sort.cmp(&other.sort)
    }
}

pub(crate) async fn calculate_sort_values<T: DynamoObject, B: DynamoBackendImpl>(
    util: &DynamoUtil<B>,
    parent_id: PkSk,
    object: &T,
    insert_position: DynamoInsertPosition,
    num: usize,
) -> Result<Vec<f64>, GenericServerError> {
    let dbg_cxt = "generate_ordered_custom_ids";

    // Special 'sort' field is used to order elements. Use f64 so we can always
    // insert in between any two elements.
    let sort_value_init = NotNan::new(1.0).unwrap();
    let sort_value_default_gap = NotNan::new(1.0).unwrap();

    // Search for all IDs for existing items of this type.
    let search_id = PkSk {
        pk: object.generate_pk(&parent_id.pk, &parent_id.sk, ""),
        sk: object.generate_sk(&parent_id.pk, &parent_id.sk, ""),
    };
    let query = util
        .query::<T>(None, search_id, DynamoQueryMatchType::BeginsWith)
        .await?;
    let existing_vals = {
        let mut v = query
            .iter()
            .filter_map(|item| {
                if let Some(Ok(sort)) = item.sort().map(NotNan::new) {
                    Some(OrderedItem {
                        id: item.id_or_critical().unwrap(),
                        sort,
                    })
                } else {
                    None
                }
            })
            .collect::<Vec<OrderedItem>>();
        v.sort();
        v
    };

    Ok(match &insert_position {
        DynamoInsertPosition::First => {
            let min_val = existing_vals
                .first()
                .map(|item| item.sort)
                .unwrap_or(sort_value_init);
            (0..num)
                .map(|i| min_val - sort_value_default_gap * (i as f64 + 1.0))
                .map(f64::from)
                .rev()
                .collect()
        }
        DynamoInsertPosition::Last => {
            let max_val = existing_vals
                .last()
                .map(|item| item.sort)
                .unwrap_or(sort_value_init);
            (0..num)
                .map(|i| max_val + sort_value_default_gap * (i as f64 + 1.0))
                .map(f64::from)
                .collect()
        }
        DynamoInsertPosition::After(id) => {
            let insert_after_index = existing_vals
                .iter()
                .position(|item| item.id == id)
                .ok_or(DynamoInvalidOperationError::new(
                    dbg_cxt,
                    "The ID provided in DynamoInsertPosition::After(id) does not exist as a sorted item of type T in the database.",
                ))?;
            let insert_after = existing_vals.get(insert_after_index).unwrap();
            let insert_before = existing_vals.get(insert_after_index + 1);
            match insert_before {
                // Insert in between two items by calculating evenly spaced
                // values in between insert_before and insert_after.
                Some(insert_before) => {
                    let gap = (insert_before.sort - insert_after.sort) / (num as f64 + 1.0);
                    (0..num)
                        .map(|i| insert_after.sort + gap * (i as f64 + 1.0))
                        .map(f64::from)
                        .collect()
                }
                // No items after, simple insert same as ::Last.
                None => (0..num)
                    .map(|i| insert_after.sort + sort_value_default_gap * (i as f64 + 1.0))
                    .map(f64::from)
                    .collect(),
            }
        }
    })
}
