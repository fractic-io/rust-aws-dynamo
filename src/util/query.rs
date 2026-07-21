use std::{borrow::Borrow, collections::HashMap, marker::PhantomData};

use aws_sdk_dynamodb::types::AttributeValue;
use fractic_server_error::ServerError;

use crate::{
    errors::DynamoInvalidOperation,
    schema::{DynamoObject, PkSk},
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

/// Builds a typed query against the primary table index.
pub struct DynamoQueryPartition<T, P> {
    partition: P,
    schema: PhantomData<fn() -> T>,
}

/// Builds a generic query against the primary table index.
pub struct DynamoGenericQueryPartition<P> {
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

/// Builds a typed query against a secondary index.
pub struct DynamoQueryIndexPartition<T, P> {
    index: IndexConfig,
    partition: P,
    schema: PhantomData<fn() -> T>,
}

/// Builds a generic query against a secondary index.
pub struct DynamoGenericQueryIndexPartition<P> {
    index: IndexConfig,
    partition: P,
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
    pub fn partition<P>(partition: P) -> DynamoQueryPartition<T, P> {
        DynamoQueryPartition {
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

    /// Erases the result schema while preserving the validated query.
    pub fn into_generic(self) -> DynamoGenericQuery {
        self.inner
    }
}

impl DynamoGenericQuery {
    /// Starts a query against the primary table index.
    pub fn partition<P>(partition: P) -> DynamoGenericQueryPartition<P> {
        DynamoGenericQueryPartition { partition }
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

    fn uuid_v7_range<T: DynamoObject>(
        parent: &PkSk,
        start_millis: i64,
        end_millis: i64,
    ) -> Result<Self, ServerError> {
        if start_millis > end_millis {
            return Err(DynamoInvalidOperation::with_debug(
                "UUID-v7 query start must not exceed its end",
                &(start_millis, end_millis),
            ));
        }
        let lower = PkSk::uuid_v7_lower_bound::<T>(parent, start_millis)?;
        let upper = PkSk::uuid_v7_upper_bound::<T>(parent, end_millis)?;
        if lower.pk != upper.pk {
            return Err(DynamoInvalidOperation::new(
                "UUID-v7 query bounds resolved to different partitions",
            ));
        }
        Self::between_inclusive(lower.pk, lower.sk, upper.sk, None)
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
        let prefix = sort_key.rsplit_once(delimiter).ok_or_else(|| {
            DynamoInvalidOperation::with_debug(
                "sort-key filter did not contain its delimiter",
                &sort_key,
            )
        })?;
        let upper = format!("{}{delimiter}~", prefix.0);
        Self::between_inclusive(partition, sort_key, upper, index)
    }

    fn same_prefix_at_or_before(
        partition: impl Into<String>,
        sort_key: impl Into<String>,
        delimiter: char,
        index: Option<IndexConfig>,
    ) -> Result<Self, ServerError> {
        let sort_key = sort_key.into();
        let lower = sort_key
            .rsplit_once(delimiter)
            .ok_or_else(|| {
                DynamoInvalidOperation::with_debug(
                    "sort-key filter did not contain its delimiter",
                    &sort_key,
                )
            })?
            .0;
        let lower = format!("{lower}{delimiter}");
        Self::between_inclusive(partition, lower, sort_key, index)
    }

    pub(super) fn into_expression(self) -> Result<QueryExpression, ServerError> {
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
        Ok(QueryExpression {
            index_name,
            condition,
            attribute_values,
        })
    }
}

impl<T> From<DynamoQuery<T>> for DynamoGenericQuery {
    fn from(query: DynamoQuery<T>) -> Self {
        query.into_generic()
    }
}

impl<T> DynamoQueryIndex<T> {
    /// Selects the partition-key value for this index query.
    pub fn partition<P>(self, partition: P) -> DynamoQueryIndexPartition<T, P> {
        DynamoQueryIndexPartition {
            index: self.index,
            partition,
            schema: PhantomData,
        }
    }
}

impl DynamoGenericQueryIndex {
    /// Selects the partition-key value for this index query.
    pub fn partition<P>(self, partition: P) -> DynamoGenericQueryIndexPartition<P> {
        DynamoGenericQueryIndexPartition {
            index: self.index,
            partition,
        }
    }
}

macro_rules! ordinary_partition_terminals {
    ($builder:ident $(<$schema:ident>)?, $output:ty, $wrap:expr) => {
        impl<$($schema,)? P: Into<String>> $builder<$($schema,)? P> {
            /// Matches every item in the selected partition.
            pub fn all(self) -> $output {
                $wrap(DynamoGenericQuery::new(
                    self.partition,
                    SortKeyCondition::Any,
                    None,
                ))
            }

            /// Matches items whose sort key exactly equals `sort_key`.
            pub fn equals(self, sort_key: impl Into<String>) -> $output {
                $wrap(DynamoGenericQuery::new(
                    self.partition,
                    SortKeyCondition::Equals(sort_key.into()),
                    None,
                ))
            }

            /// Matches items whose sort key starts with `prefix`.
            pub fn begins_with(self, prefix: impl Into<String>) -> $output {
                let prefix = prefix.into();
                if prefix.is_empty() {
                    self.all()
                } else {
                    $wrap(DynamoGenericQuery::new(
                        self.partition,
                        SortKeyCondition::BeginsWith(prefix),
                        None,
                    ))
                }
            }

            /// Matches items whose sort key is greater than `sort_key`.
            pub fn greater_than(self, sort_key: impl Into<String>) -> $output {
                $wrap(DynamoGenericQuery::new(
                    self.partition,
                    SortKeyCondition::GreaterThan(sort_key.into()),
                    None,
                ))
            }

            /// Matches items whose sort key is greater than or equal to `sort_key`.
            pub fn greater_than_or_equal(self, sort_key: impl Into<String>) -> $output {
                $wrap(DynamoGenericQuery::new(
                    self.partition,
                    SortKeyCondition::GreaterThanOrEqual(sort_key.into()),
                    None,
                ))
            }

            /// Matches items whose sort key is less than `sort_key`.
            pub fn less_than(self, sort_key: impl Into<String>) -> $output {
                $wrap(DynamoGenericQuery::new(
                    self.partition,
                    SortKeyCondition::LessThan(sort_key.into()),
                    None,
                ))
            }

            /// Matches items whose sort key is less than or equal to `sort_key`.
            pub fn less_than_or_equal(self, sort_key: impl Into<String>) -> $output {
                $wrap(DynamoGenericQuery::new(
                    self.partition,
                    SortKeyCondition::LessThanOrEqual(sort_key.into()),
                    None,
                ))
            }

            /// Matches items whose sort key is within the inclusive range.
            pub fn between_inclusive(
                self,
                lower: impl Into<String>,
                upper: impl Into<String>,
            ) -> Result<$output, ServerError> {
                DynamoGenericQuery::between_inclusive(self.partition, lower, upper, None)
                    .map($wrap)
            }

            /// Matches the same delimited key family at or above `sort_key`.
            pub fn same_prefix_at_or_after(
                self,
                sort_key: impl Into<String>,
                delimiter: char,
            ) -> Result<$output, ServerError> {
                DynamoGenericQuery::same_prefix_at_or_after(
                    self.partition,
                    sort_key,
                    delimiter,
                    None,
                )
                .map($wrap)
            }

            /// Matches the same delimited key family at or below `sort_key`.
            pub fn same_prefix_at_or_before(
                self,
                sort_key: impl Into<String>,
                delimiter: char,
            ) -> Result<$output, ServerError> {
                DynamoGenericQuery::same_prefix_at_or_before(
                    self.partition,
                    sort_key,
                    delimiter,
                    None,
                )
                .map($wrap)
            }
        }
    };
}

ordinary_partition_terminals!(DynamoQueryPartition<T>, DynamoQuery<T>, |inner| {
    DynamoQuery {
        inner,
        schema: PhantomData,
    }
});
ordinary_partition_terminals!(DynamoGenericQueryPartition, DynamoGenericQuery, |inner| {
    inner
});

macro_rules! index_partition_terminals {
    ($builder:ident $(<$schema:ident>)?, $output:ty, $wrap:expr) => {
        impl<$($schema,)? P: Into<String>> $builder<$($schema,)? P> {
            /// Matches every item in the selected index partition.
            pub fn all(self) -> $output {
                $wrap(DynamoGenericQuery::new(
                    self.partition,
                    SortKeyCondition::Any,
                    Some(self.index),
                ))
            }

            /// Matches items whose index sort key exactly equals `sort_key`.
            pub fn equals(self, sort_key: impl Into<String>) -> $output {
                $wrap(DynamoGenericQuery::new(
                    self.partition,
                    SortKeyCondition::Equals(sort_key.into()),
                    Some(self.index),
                ))
            }

            /// Matches items whose index sort key starts with `prefix`.
            pub fn begins_with(self, prefix: impl Into<String>) -> $output {
                let prefix = prefix.into();
                if prefix.is_empty() {
                    self.all()
                } else {
                    $wrap(DynamoGenericQuery::new(
                        self.partition,
                        SortKeyCondition::BeginsWith(prefix),
                        Some(self.index),
                    ))
                }
            }

            /// Matches items whose index sort key is greater than `sort_key`.
            pub fn greater_than(self, sort_key: impl Into<String>) -> $output {
                $wrap(DynamoGenericQuery::new(
                    self.partition,
                    SortKeyCondition::GreaterThan(sort_key.into()),
                    Some(self.index),
                ))
            }

            /// Matches items whose index sort key is greater than or equal to `sort_key`.
            pub fn greater_than_or_equal(self, sort_key: impl Into<String>) -> $output {
                $wrap(DynamoGenericQuery::new(
                    self.partition,
                    SortKeyCondition::GreaterThanOrEqual(sort_key.into()),
                    Some(self.index),
                ))
            }

            /// Matches items whose index sort key is less than `sort_key`.
            pub fn less_than(self, sort_key: impl Into<String>) -> $output {
                $wrap(DynamoGenericQuery::new(
                    self.partition,
                    SortKeyCondition::LessThan(sort_key.into()),
                    Some(self.index),
                ))
            }

            /// Matches items whose index sort key is less than or equal to `sort_key`.
            pub fn less_than_or_equal(self, sort_key: impl Into<String>) -> $output {
                $wrap(DynamoGenericQuery::new(
                    self.partition,
                    SortKeyCondition::LessThanOrEqual(sort_key.into()),
                    Some(self.index),
                ))
            }

            /// Matches items whose index sort key is within the inclusive range.
            pub fn between_inclusive(
                self,
                lower: impl Into<String>,
                upper: impl Into<String>,
            ) -> Result<$output, ServerError> {
                DynamoGenericQuery::between_inclusive(
                    self.partition,
                    lower,
                    upper,
                    Some(self.index),
                )
                .map($wrap)
            }

            /// Matches the same delimited index-key family at or above `sort_key`.
            pub fn same_prefix_at_or_after(
                self,
                sort_key: impl Into<String>,
                delimiter: char,
            ) -> Result<$output, ServerError> {
                DynamoGenericQuery::same_prefix_at_or_after(
                    self.partition,
                    sort_key,
                    delimiter,
                    Some(self.index),
                )
                .map($wrap)
            }

            /// Matches the same delimited index-key family at or below `sort_key`.
            pub fn same_prefix_at_or_before(
                self,
                sort_key: impl Into<String>,
                delimiter: char,
            ) -> Result<$output, ServerError> {
                DynamoGenericQuery::same_prefix_at_or_before(
                    self.partition,
                    sort_key,
                    delimiter,
                    Some(self.index),
                )
                .map($wrap)
            }
        }
    };
}

index_partition_terminals!(DynamoQueryIndexPartition<T>, DynamoQuery<T>, |inner| {
    DynamoQuery {
        inner,
        schema: PhantomData,
    }
});
index_partition_terminals!(
    DynamoGenericQueryIndexPartition,
    DynamoGenericQuery,
    |inner| inner
);

impl<T: DynamoObject, P: Borrow<PkSk>> DynamoQueryPartition<T, P> {
    /// Matches UUID-v7 children created within the inclusive millisecond range.
    pub fn uuid_v7_range(
        self,
        start_millis: i64,
        end_millis: i64,
    ) -> Result<DynamoQuery<T>, ServerError> {
        DynamoGenericQuery::uuid_v7_range::<T>(self.partition.borrow(), start_millis, end_millis)
            .map(|inner| DynamoQuery {
                inner,
                schema: PhantomData,
            })
    }
}

impl<P: Borrow<PkSk>> DynamoGenericQueryPartition<P> {
    /// Matches UUID-v7 children of `T` created within the inclusive millisecond range.
    pub fn uuid_v7_range<T: DynamoObject>(
        self,
        start_millis: i64,
        end_millis: i64,
    ) -> Result<DynamoGenericQuery, ServerError> {
        DynamoGenericQuery::uuid_v7_range::<T>(self.partition.borrow(), start_millis, end_millis)
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

    #[test]
    fn builders_compile_expected_conditions() {
        let expression = DynamoGenericQuery::partition("P")
            .begins_with("PREFIX#")
            .into_expression()
            .unwrap();
        assert_eq!(
            expression.condition,
            "pk = :pk_val AND begins_with(sk, :sk_val)"
        );

        let expression = DynamoGenericQuery::partition("P")
            .between_inclusive("A", "Z")
            .unwrap()
            .into_expression()
            .unwrap();
        assert_eq!(
            expression.condition,
            "pk = :pk_val AND sk BETWEEN :sk_val AND :sk_max"
        );

        let expression = DynamoGenericQuery::partition("P")
            .same_prefix_at_or_after("EVENT#050", '#')
            .unwrap()
            .into_expression()
            .unwrap();
        assert_eq!(
            expression.attribute_values[":sk_val"],
            AttributeValue::S("EVENT#050".into())
        );
        assert_eq!(
            expression.attribute_values[":sk_max"],
            AttributeValue::S("EVENT#~".into())
        );

        let expression = DynamoGenericQuery::partition("P")
            .same_prefix_at_or_before("EVENT#050", '#')
            .unwrap()
            .into_expression()
            .unwrap();
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
        let query: DynamoQuery<Event> = DynamoQuery::partition(PkSk::root())
            .uuid_v7_range(1_000, 2_000)
            .unwrap();
        assert!(query.into_generic().into_expression().is_ok());
    }

    #[test]
    fn typed_queries_explicitly_convert_to_generic_queries() {
        let typed: DynamoQuery<Event> = DynamoQuery::partition("ROOT").all();
        let generic: DynamoGenericQuery = typed.into();
        assert!(generic.into_expression().is_ok());
    }
}
