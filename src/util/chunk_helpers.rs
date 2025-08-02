use std::collections::HashMap;

use crate::util::{
    DynamoMap, AUTO_FIELDS_CREATED_AT, AUTO_FIELDS_SORT, AUTO_FIELDS_TTL, AUTO_FIELDS_UPDATED_AT,
};

pub trait WithMetadataFrom {
    fn with_metadata_from(self, other: &DynamoMap) -> DynamoMap;
}

impl WithMetadataFrom for DynamoMap {
    fn with_metadata_from(self, other: &DynamoMap) -> DynamoMap {
        // All metadata keys whose values should come from `other`.
        const META_KEYS: &[&str] = &[
            "pk",
            "sk",
            AUTO_FIELDS_CREATED_AT,
            AUTO_FIELDS_UPDATED_AT,
            AUTO_FIELDS_SORT,
            AUTO_FIELDS_TTL,
        ];

        // 1. Keep everything from `self` **except** metadata keys.
        // 2. Append metadata keys from `other` if they exist.
        self.into_iter()
            .filter(|(k, _)| !META_KEYS.contains(&k.as_str()))
            .chain(
                META_KEYS
                    .iter()
                    .filter_map(|k| other.get(*k).map(|v| ((*k).to_string(), v.clone()))),
            )
            .collect::<HashMap<_, _>>()
    }
}
