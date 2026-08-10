use std::collections::HashSet;

use fractic_server_error::ServerError;
use serde::Serialize;

use super::{
    attribute_names::is_reserved_attribute_name,
    attribute_value::{serialize_attribute_value, DynamoMap},
    DynamoFieldRename, DynamoObject, IdLogic, PkSk,
};
use crate::errors::DynamoInvalidOperation;

/// A set of top-level attributes computed from a [`DynamoObject`]'s ID and
/// canonical data and persisted with the object.
///
/// This type is normally constructed by `dynamo_object!`'s `materialized`
/// option rather than directly.
#[derive(Debug, Default)]
pub struct MaterializedAttributes {
    pub(crate) entries: Vec<(
        &'static str,
        Option<aws_sdk_dynamodb::types::AttributeValue>,
    )>,
}

#[doc(hidden)]
pub type MaterializedAttributesResult = Result<MaterializedAttributes, ServerError>;

impl MaterializedAttributes {
    #[doc(hidden)]
    pub fn insert<T: Serialize>(
        &mut self,
        name: &'static str,
        value: T,
    ) -> Result<(), ServerError> {
        self.entries.push((name, serialize_attribute_value(value)?));
        Ok(())
    }
}

/// Persistence changes owned by a [`DynamoObject`]'s materialization logic.
///
/// Keeping this independent of a complete Dynamo item allows typed CRUD,
/// maintenance backfills, and bundle import to share one calculation.
#[derive(Debug, Default)]
pub(crate) struct MaterializedWritePlan {
    pub(crate) set: DynamoMap,
    pub(crate) remove: Vec<String>,
}

impl MaterializedWritePlan {
    pub(crate) fn apply_to_update(self, set: &mut DynamoMap, remove: &mut Vec<String>) {
        for (name, value) in self.set {
            remove.retain(|remove_name| remove_name != &name);
            set.insert(name, value);
        }
        for name in self.remove {
            set.remove(&name);
            if !remove.contains(&name) {
                remove.push(name);
            }
        }
    }

    pub(crate) fn apply_to_put(self, item: &mut DynamoMap) {
        for (name, value) in self.set {
            item.insert(name, value);
        }
        for name in self.remove {
            item.remove(&name);
        }
    }
}

pub(crate) fn validate_materialized_storage<T: DynamoObject>() -> Result<(), ServerError> {
    if T::materialized_attribute_names().is_empty()
        || matches!(
            T::id_logic(),
            IdLogic::UuidV4 | IdLogic::UuidV7 | IdLogic::Singleton | IdLogic::IndexedSingleton(_)
        )
    {
        return Ok(());
    }
    Err(DynamoInvalidOperation::new(&format!(
        "materialized attributes are only supported for ordinary, non-partitioned objects; object \
         type '{}' uses an incompatible ID logic",
        T::id_label()
    )))
}

pub(crate) fn validate_materialized_attribute_names(
    names: &[&str],
    renamed_fields: &[DynamoFieldRename],
) -> Result<(), ServerError> {
    let mut declared = HashSet::with_capacity(names.len());
    for &name in names {
        if name.is_empty() {
            return Err(DynamoInvalidOperation::new(
                "materialized attribute names cannot be empty",
            ));
        }
        if is_reserved_attribute_name(name) {
            return Err(DynamoInvalidOperation::new(&format!(
                "materialized attribute '{name}' is reserved"
            )));
        }
        if !declared.insert(name) {
            return Err(DynamoInvalidOperation::new(&format!(
                "materialized attribute '{name}' is declared more than once"
            )));
        }
        if renamed_fields
            .iter()
            .any(|renamed| !renamed.is_noop() && renamed.from == name)
        {
            return Err(DynamoInvalidOperation::new(&format!(
                "materialized attribute '{name}' collides with a legacy renamed field"
            )));
        }
    }
    Ok(())
}

pub(crate) fn build_materialized_write_plan_against<T: DynamoObject>(
    id: &PkSk,
    data: &T::Data,
    serialized: &DynamoMap,
    serialized_nulls: &[String],
) -> Result<MaterializedWritePlan, ServerError> {
    let names = T::materialized_attribute_names();
    if names.is_empty() {
        return Ok(MaterializedWritePlan::default());
    }

    validate_materialized_storage::<T>()?;
    validate_materialized_attribute_names(names, T::renamed_fields())?;

    for &name in names {
        if serialized.contains_key(name)
            || serialized_nulls
                .iter()
                .any(|serialized_name| serialized_name == name)
        {
            return Err(DynamoInvalidOperation::new(&format!(
                "materialized attribute '{name}' collides with a serialized object attribute"
            )));
        }
    }

    let materialized = T::materialized_attributes(id, data)?;
    let mut plan = MaterializedWritePlan::default();
    let declared = names.iter().copied().collect::<HashSet<_>>();
    let mut produced = HashSet::with_capacity(materialized.entries.len());
    for (name, value) in materialized.entries {
        if !declared.contains(name) {
            return Err(DynamoInvalidOperation::new(&format!(
                "materialization produced undeclared attribute '{name}'"
            )));
        }
        if !produced.insert(name) {
            return Err(DynamoInvalidOperation::new(&format!(
                "materialization produced attribute '{name}' more than once"
            )));
        }
        match value {
            Some(value) => {
                plan.set.insert(name.to_string(), value);
            }
            None => plan.remove.push(name.to_string()),
        }
    }
    if let Some(missing) = names.iter().find(|name| !produced.contains(**name)) {
        return Err(DynamoInvalidOperation::new(&format!(
            "materialization did not produce declared attribute '{missing}'"
        )));
    }
    Ok(plan)
}
