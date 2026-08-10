pub const AUTO_FIELDS_CREATED_AT: &str = "created_at";
pub const AUTO_FIELDS_UPDATED_AT: &str = "updated_at";
pub const AUTO_FIELDS_SORT: &str = "sort";
pub const AUTO_FIELDS_TTL: &str = "ttl";

/// Storage key for batch-optimized item data.
pub const EXPAND_DATA_RESERVED_KEY: &str = "..";

/// Storage keys for externally partitioned items.
pub const COLLAPSE_PLACEHOLDER_RESERVED_KEY: &str = "#!";
pub const COLLAPSE_DATA_RESERVED_KEY: &str = "##";

pub(crate) fn is_reserved_attribute_name(name: &str) -> bool {
    [
        "id",
        "pk",
        "sk",
        AUTO_FIELDS_CREATED_AT,
        AUTO_FIELDS_UPDATED_AT,
        AUTO_FIELDS_SORT,
        AUTO_FIELDS_TTL,
        EXPAND_DATA_RESERVED_KEY,
        COLLAPSE_PLACEHOLDER_RESERVED_KEY,
        COLLAPSE_DATA_RESERVED_KEY,
    ]
    .contains(&name)
}
