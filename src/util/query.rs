use std::{collections::HashMap, marker::PhantomData};

use aws_sdk_dynamodb::types::AttributeValue;
use fractic_server_error::ServerError;

use crate::{
    errors::DynamoInvalidOperation,
    schema::{DynamoObject, PkSk},
};

/// Marker for a query that was constructed without object-schema context.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NoSchema {}

/// A complete DynamoDB key-query specification.
///
/// `S` records the optional object schema used to construct the query. Typed
/// queries deserialize results as `S`; generic queries can execute a query with
/// any schema marker and return raw DynamoDB maps.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DynamoQuery<S = NoSchema> {
    index: Option<IndexConfig>,
    partition: String,
    sort: SortKeyCondition,
    primary_only: bool,
    schema: PhantomData<fn() -> S>,
}

/// An explicitly schema-free query specification.
pub type RawDynamoQuery = DynamoQuery<NoSchema>;

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
    GreaterThanOrEquals(String),
    LessThan(String),
    LessThanOrEquals(String),
    BetweenInclusive { lower: String, upper: String },
}

pub(super) struct QueryExpression {
    pub index_name: Option<String>,
    pub condition: String,
    pub attribute_values: HashMap<String, AttributeValue>,
}

impl<S> DynamoQuery<S> {
    /// Matches every item in a partition.
    pub fn all(partition: impl Into<String>) -> Self {
        Self::new(partition, SortKeyCondition::Any)
    }

    /// Matches items whose sort key exactly equals `sort_key`.
    pub fn equals(partition: impl Into<String>, sort_key: impl Into<String>) -> Self {
        Self::new(partition, SortKeyCondition::Equals(sort_key.into()))
    }

    /// Matches items whose sort key starts with `prefix`.
    pub fn begins_with(partition: impl Into<String>, prefix: impl Into<String>) -> Self {
        let prefix = prefix.into();
        if prefix.is_empty() {
            Self::all(partition)
        } else {
            Self::new(partition, SortKeyCondition::BeginsWith(prefix))
        }
    }

    /// Matches items whose sort key is greater than `sort_key`.
    pub fn greater_than(partition: impl Into<String>, sort_key: impl Into<String>) -> Self {
        Self::new(partition, SortKeyCondition::GreaterThan(sort_key.into()))
    }

    /// Matches items whose sort key is greater than or equal to `sort_key`.
    pub fn greater_than_or_equals(
        partition: impl Into<String>,
        sort_key: impl Into<String>,
    ) -> Self {
        Self::new(
            partition,
            SortKeyCondition::GreaterThanOrEquals(sort_key.into()),
        )
    }

    /// Matches items whose sort key is less than `sort_key`.
    pub fn less_than(partition: impl Into<String>, sort_key: impl Into<String>) -> Self {
        Self::new(partition, SortKeyCondition::LessThan(sort_key.into()))
    }

    /// Matches items whose sort key is less than or equal to `sort_key`.
    pub fn less_than_or_equals(partition: impl Into<String>, sort_key: impl Into<String>) -> Self {
        Self::new(
            partition,
            SortKeyCondition::LessThanOrEquals(sort_key.into()),
        )
    }

    /// Matches items whose sort key is within the inclusive range.
    pub fn between_inclusive(
        partition: impl Into<String>,
        lower: impl Into<String>,
        upper: impl Into<String>,
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
        ))
    }

    /// Matches a delimited sort-key family at or above `sort_key`.
    pub fn suffix_greater_than_or_equals(
        partition: impl Into<String>,
        sort_key: impl Into<String>,
        delimiter: char,
    ) -> Result<Self, ServerError> {
        let sort_key = sort_key.into();
        let prefix = sort_key.rsplit_once(delimiter).ok_or_else(|| {
            DynamoInvalidOperation::with_debug(
                "sort-key filter did not contain its delimiter",
                &sort_key,
            )
        })?;
        let upper = format!("{}{delimiter}~", prefix.0);
        Self::between_inclusive(partition, sort_key, upper)
    }

    /// Matches a delimited sort-key family at or below `sort_key`.
    pub fn suffix_less_than_or_equals(
        partition: impl Into<String>,
        sort_key: impl Into<String>,
        delimiter: char,
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
        Self::between_inclusive(partition, lower, sort_key)
    }

    /// Executes this query against the configured DynamoDB index.
    pub fn on_index(mut self, index: IndexConfig) -> Self {
        self.index = Some(index);
        self
    }

    /// Erases the schema marker while preserving the validated query.
    pub fn into_generic(self) -> RawDynamoQuery {
        DynamoQuery {
            index: self.index,
            partition: self.partition,
            sort: self.sort,
            primary_only: self.primary_only,
            schema: PhantomData,
        }
    }

    fn new(partition: impl Into<String>, sort: SortKeyCondition) -> Self {
        Self {
            index: None,
            partition: partition.into(),
            sort,
            primary_only: false,
            schema: PhantomData,
        }
    }

    pub(super) fn into_expression(self) -> Result<QueryExpression, ServerError> {
        if self.primary_only && self.index.is_some() {
            return Err(DynamoInvalidOperation::new(
                "parent-based UUID-v7 range queries can only use the primary table index",
            ));
        }
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
            SortKeyCondition::GreaterThanOrEquals(value) => {
                attribute_values.insert(":sk_val".to_string(), AttributeValue::S(value));
                format!("{partition_field} = :pk_val AND {sort_field} >= :sk_val")
            }
            SortKeyCondition::LessThan(value) => {
                attribute_values.insert(":sk_val".to_string(), AttributeValue::S(value));
                format!("{partition_field} = :pk_val AND {sort_field} < :sk_val")
            }
            SortKeyCondition::LessThanOrEquals(value) => {
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

impl<S: DynamoObject> DynamoQuery<S> {
    /// Matches UUID-v7 objects created within the inclusive millisecond range.
    pub fn uuid_v7_range(
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
        let lower = PkSk::uuid_v7_lower_bound::<S>(parent, start_millis)?;
        let upper = PkSk::uuid_v7_upper_bound::<S>(parent, end_millis)?;
        if lower.pk != upper.pk {
            return Err(DynamoInvalidOperation::new(
                "UUID-v7 query bounds resolved to different partitions",
            ));
        }
        let mut query = Self::between_inclusive(lower.pk, lower.sk, upper.sk)?;
        query.primary_only = true;
        Ok(query)
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
    fn constructors_compile_expected_conditions() {
        let expression = RawDynamoQuery::begins_with("P", "PREFIX#")
            .into_expression()
            .unwrap();
        assert_eq!(
            expression.condition,
            "pk = :pk_val AND begins_with(sk, :sk_val)"
        );

        let expression = RawDynamoQuery::between_inclusive("P", "A", "Z")
            .unwrap()
            .into_expression()
            .unwrap();
        assert_eq!(
            expression.condition,
            "pk = :pk_val AND sk BETWEEN :sk_val AND :sk_max"
        );

        let expression = RawDynamoQuery::suffix_greater_than_or_equals("P", "EVENT#050", '#')
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

        let expression = RawDynamoQuery::suffix_less_than_or_equals("P", "EVENT#050", '#')
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
    fn uuid_v7_ranges_are_typed_and_primary_only() {
        let query = DynamoQuery::<Event>::uuid_v7_range(PkSk::root(), 1_000, 2_000).unwrap();
        assert!(query.clone().into_expression().is_ok());
        assert!(query
            .on_index(IndexConfig {
                name: "gsi",
                partition_field: "gpk",
                sort_field: "gsk",
            })
            .into_expression()
            .is_err());
    }
}
