use std::{collections::HashMap, marker::PhantomData};

use aws_sdk_dynamodb::types::AttributeValue;
use fractic_server_error::ServerError;

use crate::{
    errors::DynamoInvalidOperation,
    schema::{
        identifiers::{uuid_v7_value_lower_bound, uuid_v7_value_upper_bound},
        DynamoObject, IdLogic,
    },
};

/// A complete key-query specification whose results deserialize as `T`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DynamoQuery<T> {
    inner: DynamoGenericQuery,
    schema: PhantomData<fn() -> T>,
}

/// A complete key-query specification that returns raw DynamoDB maps.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DynamoGenericQuery {
    index: Option<IndexConfig>,
    partition: String,
    sort: SortKeyCondition,
}

/// Builds a typed query after its primary or secondary index is selected.
pub struct DynamoQueryPartition<T, P> {
    index: Option<IndexConfig>,
    partition: P,
    schema: PhantomData<fn() -> T>,
}

/// Builds a generic query after its primary or secondary index is selected.
pub struct DynamoGenericQueryPartition<P> {
    index: Option<IndexConfig>,
    partition: P,
}

/// Selects the partition queried through a secondary index.
pub struct DynamoQueryIndex<T> {
    index: IndexConfig,
    schema: PhantomData<fn() -> T>,
}

/// Selects the partition queried through a secondary index for generic results.
pub struct DynamoGenericQueryIndex {
    index: IndexConfig,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexConfig {
    pub name: &'static str,
    pub partition_field: &'static str,
    pub sort_field: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SortKeyCondition {
    Any,
    Equals(String),
    BeginsWith(String),
    GreaterThan(String),
    GreaterThanOrEqual(String),
    LessThan(String),
    LessThanOrEqual(String),
    BetweenInclusive { lower: String, upper: String },
}

pub(super) struct QueryExpression {
    pub index_name: Option<String>,
    pub condition: String,
    pub attribute_values: HashMap<String, AttributeValue>,
}

impl<T> DynamoQuery<T> {
    /// Starts a query against the primary table index.
    pub fn pk<P>(partition: P) -> DynamoQueryPartition<T, P> {
        DynamoQueryPartition {
            index: None,
            partition,
            schema: PhantomData,
        }
    }

    /// Starts a query against `index`.
    pub fn index(index: IndexConfig) -> DynamoQueryIndex<T> {
        DynamoQueryIndex {
            index,
            schema: PhantomData,
        }
    }
}

impl DynamoGenericQuery {
    /// Starts a query against the primary table index.
    pub fn pk<P>(partition: P) -> DynamoGenericQueryPartition<P> {
        DynamoGenericQueryPartition {
            index: None,
            partition,
        }
    }

    /// Starts a query against `index`.
    pub fn index(index: IndexConfig) -> DynamoGenericQueryIndex {
        DynamoGenericQueryIndex { index }
    }

    fn new(
        partition: impl Into<String>,
        sort: SortKeyCondition,
        index: Option<IndexConfig>,
    ) -> Self {
        Self {
            index,
            partition: partition.into(),
            sort,
        }
    }

    fn uuidv7_range<T: DynamoObject>(
        partition: impl Into<String>,
        sk_prefix: impl Into<String>,
        start_millis: i64,
        end_millis: i64,
        index: Option<IndexConfig>,
    ) -> Result<Self, ServerError> {
        if start_millis > end_millis {
            return Err(DynamoInvalidOperation::with_debug(
                "UUID-v7 query start must not exceed its end",
                &(start_millis, end_millis),
            ));
        }
        if !matches!(T::id_logic(), IdLogic::UuidV7) {
            return Err(DynamoInvalidOperation::new(&format!(
                "UUID-v7 ranges require IdLogic::UuidV7, but object type '{}' uses different ID logic",
                T::id_label()
            )));
        }
        let sk_prefix = sk_prefix.into();
        let expected_suffix = format!("{}#", T::id_label());
        if !sk_prefix.ends_with(&expected_suffix) {
            return Err(DynamoInvalidOperation::with_debug(
                "UUID-v7 sort-key prefix must end with the queried object's `LABEL#` prefix",
                &(sk_prefix, expected_suffix),
            ));
        }
        let lower = format!("{sk_prefix}{}", uuid_v7_value_lower_bound(start_millis)?);
        let upper = format!("{sk_prefix}{}", uuid_v7_value_upper_bound(end_millis)?);
        Self::between_inclusive(partition, lower, upper, index)
    }

    fn between_inclusive(
        partition: impl Into<String>,
        lower: impl Into<String>,
        upper: impl Into<String>,
        index: Option<IndexConfig>,
    ) -> Result<Self, ServerError> {
        let lower = lower.into();
        let upper = upper.into();
        if lower > upper {
            return Err(DynamoInvalidOperation::with_debug(
                "DynamoDB query lower sort-key bound must not exceed its upper bound",
                &(lower, upper),
            ));
        }
        Ok(Self::new(
            partition,
            SortKeyCondition::BetweenInclusive { lower, upper },
            index,
        ))
    }

    fn same_prefix_at_or_after(
        partition: impl Into<String>,
        sort_key: impl Into<String>,
        delimiter: char,
        index: Option<IndexConfig>,
    ) -> Result<Self, ServerError> {
        let sort_key = sort_key.into();
        let (prefix, _) = sort_key.rsplit_once(delimiter).ok_or_else(|| {
            DynamoInvalidOperation::with_debug(
                "sort-key filter did not contain its delimiter",
                &sort_key,
            )
        })?;
        let upper = format!("{prefix}{delimiter}~");
        Self::between_inclusive(partition, sort_key, upper, index)
    }

    fn same_prefix_at_or_before(
        partition: impl Into<String>,
        sort_key: impl Into<String>,
        delimiter: char,
        index: Option<IndexConfig>,
    ) -> Result<Self, ServerError> {
        let sort_key = sort_key.into();
        let (prefix, _) = sort_key.rsplit_once(delimiter).ok_or_else(|| {
            DynamoInvalidOperation::with_debug(
                "sort-key filter did not contain its delimiter",
                &sort_key,
            )
        })?;
        Self::between_inclusive(partition, format!("{prefix}{delimiter}"), sort_key, index)
    }

    pub(super) fn into_expression(self) -> QueryExpression {
        let (index_name, partition_field, sort_field) = match self.index {
            Some(index) => (
                Some(index.name.to_owned()),
                index.partition_field,
                index.sort_field,
            ),
            None => (None, "pk", "sk"),
        };
        let mut attribute_values =
            HashMap::from([(":pk_val".to_string(), AttributeValue::S(self.partition))]);
        let condition = match self.sort {
            SortKeyCondition::Any => format!("{partition_field} = :pk_val"),
            SortKeyCondition::Equals(value) => {
                attribute_values.insert(":sk_val".to_string(), AttributeValue::S(value));
                format!("{partition_field} = :pk_val AND {sort_field} = :sk_val")
            }
            SortKeyCondition::BeginsWith(value) => {
                attribute_values.insert(":sk_val".to_string(), AttributeValue::S(value));
                format!("{partition_field} = :pk_val AND begins_with({sort_field}, :sk_val)")
            }
            SortKeyCondition::GreaterThan(value) => {
                attribute_values.insert(":sk_val".to_string(), AttributeValue::S(value));
                format!("{partition_field} = :pk_val AND {sort_field} > :sk_val")
            }
            SortKeyCondition::GreaterThanOrEqual(value) => {
                attribute_values.insert(":sk_val".to_string(), AttributeValue::S(value));
                format!("{partition_field} = :pk_val AND {sort_field} >= :sk_val")
            }
            SortKeyCondition::LessThan(value) => {
                attribute_values.insert(":sk_val".to_string(), AttributeValue::S(value));
                format!("{partition_field} = :pk_val AND {sort_field} < :sk_val")
            }
            SortKeyCondition::LessThanOrEqual(value) => {
                attribute_values.insert(":sk_val".to_string(), AttributeValue::S(value));
                format!("{partition_field} = :pk_val AND {sort_field} <= :sk_val")
            }
            SortKeyCondition::BetweenInclusive { lower, upper } => {
                attribute_values.insert(":sk_val".to_string(), AttributeValue::S(lower));
                attribute_values.insert(":sk_max".to_string(), AttributeValue::S(upper));
                format!("{partition_field} = :pk_val AND {sort_field} BETWEEN :sk_val AND :sk_max")
            }
        };
        QueryExpression {
            index_name,
            condition,
            attribute_values,
        }
    }
}

impl<T> From<DynamoQuery<T>> for DynamoGenericQuery {
    fn from(query: DynamoQuery<T>) -> Self {
        query.inner
    }
}

impl<T> DynamoQueryIndex<T> {
    /// Selects the partition-key value for this index query.
    pub fn pk<P>(self, partition: P) -> DynamoQueryPartition<T, P> {
        DynamoQueryPartition {
            index: Some(self.index),
            partition,
            schema: PhantomData,
        }
    }
}

impl DynamoGenericQueryIndex {
    /// Selects the partition-key value for this index query.
    pub fn pk<P>(self, partition: P) -> DynamoGenericQueryPartition<P> {
        DynamoGenericQueryPartition {
            index: Some(self.index),
            partition,
        }
    }
}

macro_rules! partition_terminals {
    ($builder:ident $(<$schema:ident>)?, $output:ty, $wrap:expr) => {
        impl<$($schema,)? P: Into<String>> $builder<$($schema,)? P> {
            /// Matches every item in the selected partition.
            pub fn all(self) -> $output {
                $wrap(DynamoGenericQuery::new(
                    self.partition,
                    SortKeyCondition::Any,
                    self.index,
                ))
            }

            /// Matches items whose sort key exactly equals `sort_key`.
            pub fn sk_equals(self, sort_key: impl Into<String>) -> $output {
                $wrap(DynamoGenericQuery::new(
                    self.partition,
                    SortKeyCondition::Equals(sort_key.into()),
                    self.index,
                ))
            }

            /// Matches items whose sort key starts with `prefix`.
            pub fn sk_begins_with(self, prefix: impl Into<String>) -> $output {
                let prefix = prefix.into();
                if prefix.is_empty() {
                    self.all()
                } else {
                    $wrap(DynamoGenericQuery::new(
                        self.partition,
                        SortKeyCondition::BeginsWith(prefix),
                        self.index,
                    ))
                }
            }

            /// Matches items whose sort key is greater than `sort_key`.
            pub fn sk_greater_than(self, sort_key: impl Into<String>) -> $output {
                $wrap(DynamoGenericQuery::new(
                    self.partition,
                    SortKeyCondition::GreaterThan(sort_key.into()),
                    self.index,
                ))
            }

            /// Matches items whose sort key is greater than or equal to `sort_key`.
            pub fn sk_greater_than_or_equal(self, sort_key: impl Into<String>) -> $output {
                $wrap(DynamoGenericQuery::new(
                    self.partition,
                    SortKeyCondition::GreaterThanOrEqual(sort_key.into()),
                    self.index,
                ))
            }

            /// Matches items whose sort key is less than `sort_key`.
            pub fn sk_less_than(self, sort_key: impl Into<String>) -> $output {
                $wrap(DynamoGenericQuery::new(
                    self.partition,
                    SortKeyCondition::LessThan(sort_key.into()),
                    self.index,
                ))
            }

            /// Matches items whose sort key is less than or equal to `sort_key`.
            pub fn sk_less_than_or_equal(self, sort_key: impl Into<String>) -> $output {
                $wrap(DynamoGenericQuery::new(
                    self.partition,
                    SortKeyCondition::LessThanOrEqual(sort_key.into()),
                    self.index,
                ))
            }

            /// Matches items whose sort key is within the inclusive range.
            pub fn sk_between_inclusive(
                self,
                lower: impl Into<String>,
                upper: impl Into<String>,
            ) -> Result<$output, ServerError> {
                DynamoGenericQuery::between_inclusive(self.partition, lower, upper, self.index)
                    .map($wrap)
            }

            /// Matches the same delimited key family at or above `sort_key`.
            pub fn sk_same_prefix_at_or_after(
                self,
                sort_key: impl Into<String>,
                delimiter: char,
            ) -> Result<$output, ServerError> {
                DynamoGenericQuery::same_prefix_at_or_after(
                    self.partition,
                    sort_key,
                    delimiter,
                    self.index,
                )
                .map($wrap)
            }

            /// Matches the same delimited key family at or below `sort_key`.
            pub fn sk_same_prefix_at_or_before(
                self,
                sort_key: impl Into<String>,
                delimiter: char,
            ) -> Result<$output, ServerError> {
                DynamoGenericQuery::same_prefix_at_or_before(
                    self.partition,
                    sort_key,
                    delimiter,
                    self.index,
                )
                .map($wrap)
            }
        }
    };
}

partition_terminals!(DynamoQueryPartition<T>, DynamoQuery<T>, |inner| {
    DynamoQuery {
        inner,
        schema: PhantomData,
    }
});
partition_terminals!(DynamoGenericQueryPartition, DynamoGenericQuery, |inner| {
    inner
});

impl<T: DynamoObject, P: Into<String>> DynamoQueryPartition<T, P> {
    /// Matches UUID-v7 sort keys within the inclusive millisecond range.
    ///
    /// `sk_prefix` must be the complete sort-key prefix immediately before the
    /// UUID value and must end in `T`'s `LABEL#` segment.
    pub fn sk_uuidv7_range(
        self,
        sk_prefix: impl Into<String>,
        start_millis: i64,
        end_millis: i64,
    ) -> Result<DynamoQuery<T>, ServerError> {
        DynamoGenericQuery::uuidv7_range::<T>(
            self.partition,
            sk_prefix,
            start_millis,
            end_millis,
            self.index,
        )
        .map(|inner| DynamoQuery {
            inner,
            schema: PhantomData,
        })
    }
}

impl<P: Into<String>> DynamoGenericQueryPartition<P> {
    /// Matches UUID-v7 sort keys of `T` within the inclusive millisecond range.
    ///
    /// `sk_prefix` must be the complete sort-key prefix immediately before the
    /// UUID value and must end in `T`'s `LABEL#` segment.
    pub fn sk_uuidv7_range<T: DynamoObject>(
        self,
        sk_prefix: impl Into<String>,
        start_millis: i64,
        end_millis: i64,
    ) -> Result<DynamoGenericQuery, ServerError> {
        DynamoGenericQuery::uuidv7_range::<T>(
            self.partition,
            sk_prefix,
            start_millis,
            end_millis,
            self.index,
        )
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::{
        dynamo_object,
        schema::{IdLogic, NestingLogic},
    };

    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    pub struct EventData {}
    dynamo_object!(
        Event,
        EventData,
        "EVENT",
        IdLogic::UuidV7,
        NestingLogic::Root
    );

    dynamo_object!(
        RandomEvent,
        EventData,
        "RANDOM",
        IdLogic::UuidV4,
        NestingLogic::Root
    );

    #[test]
    fn builders_compile_expected_conditions() {
        let expression = DynamoGenericQuery::pk("P")
            .sk_begins_with("PREFIX#")
            .into_expression();
        assert_eq!(
            expression.condition,
            "pk = :pk_val AND begins_with(sk, :sk_val)"
        );

        let expression = DynamoGenericQuery::pk("P")
            .sk_between_inclusive("A", "Z")
            .unwrap()
            .into_expression();
        assert_eq!(
            expression.condition,
            "pk = :pk_val AND sk BETWEEN :sk_val AND :sk_max"
        );

        let expression = DynamoGenericQuery::index(IndexConfig {
            name: "by_owner",
            partition_field: "owner_pk",
            sort_field: "owner_sk",
        })
        .pk("OWNER")
        .sk_equals("EVENT#1")
        .into_expression();
        assert_eq!(expression.index_name.as_deref(), Some("by_owner"));
        assert_eq!(
            expression.condition,
            "owner_pk = :pk_val AND owner_sk = :sk_val"
        );

        let expression = DynamoGenericQuery::pk("P")
            .sk_same_prefix_at_or_after("EVENT#050", '#')
            .unwrap()
            .into_expression();
        assert_eq!(
            expression.attribute_values[":sk_val"],
            AttributeValue::S("EVENT#050".into())
        );
        assert_eq!(
            expression.attribute_values[":sk_max"],
            AttributeValue::S("EVENT#~".into())
        );

        let expression = DynamoGenericQuery::pk("P")
            .sk_same_prefix_at_or_before("EVENT#050", '#')
            .unwrap()
            .into_expression();
        assert_eq!(
            expression.attribute_values[":sk_val"],
            AttributeValue::S("EVENT#".into())
        );
        assert_eq!(
            expression.attribute_values[":sk_max"],
            AttributeValue::S("EVENT#050".into())
        );
    }

    #[test]
    fn uuid_v7_ranges_are_typed_primary_queries() {
        let query: DynamoQuery<Event> = DynamoQuery::pk("ROOT")
            .sk_uuidv7_range("EVENT#", 1_000, 2_000)
            .unwrap();
        let query: DynamoGenericQuery = query.into();
        let _ = query.into_expression();
    }

    #[test]
    fn uuid_v7_ranges_validate_id_logic_and_sort_key_prefix() {
        assert!(DynamoGenericQuery::pk("ROOT")
            .sk_uuidv7_range::<Event>("OTHER#", 1_000, 2_000)
            .is_err());
        assert!(DynamoGenericQuery::pk("ROOT")
            .sk_uuidv7_range::<RandomEvent>("RANDOM#", 1_000, 2_000)
            .is_err());
    }

    #[test]
    fn typed_queries_explicitly_convert_to_generic_queries() {
        let typed: DynamoQuery<Event> = DynamoQuery::pk("ROOT").all();
        let generic: DynamoGenericQuery = typed.into();
        let _ = generic.into_expression();
    }
}
