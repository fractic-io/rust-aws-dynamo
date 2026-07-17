//! Portable export and import of complete Dynamo object trees.

mod entities_bundle;
mod entities_policy;
mod impl_export;
mod impl_import;
mod utils_bundle_validation;
mod utils_id_mapping;
mod utils_value;

pub use entities_bundle::*;
pub use entities_policy::{
    DynamoBundlePolicy, DynamoBundleReferenceMatch, DynamoBundleReferenceRule,
};

use std::collections::HashSet;

use fractic_server_error::ServerError;

use crate::{
    ext::crud::DynamoCrudAlgorithms,
    schema::{DynamoObject, PkSk},
    util::DynamoUtil,
};

// Public interface.
// ----------------------------------------------------------------------------

/// Bundling operations for a DynamoDB table.
pub struct Bundler<'a> {
    dynamo_util: &'a DynamoUtil,
    crud_algorithms: &'a dyn DynamoCrudAlgorithms,
}

impl<'a> Bundler<'a> {
    pub fn new(dynamo_util: &'a DynamoUtil, crud_algorithms: &'a dyn DynamoCrudAlgorithms) -> Self {
        Self {
            dynamo_util,
            crud_algorithms,
        }
    }

    pub async fn export<O: DynamoObject>(&self, item: O) -> Result<DynamoBundle, ServerError> {
        impl_export::export_from_config(
            self.dynamo_util,
            self.crud_algorithms,
            item.id().clone(),
            O::nesting_logic().into(),
            BundleIdLogic::from_object::<O>(),
        )
        .await
    }

    /// Imports a bundle, optionally preserving New-mode out-of-table
    /// references whose targets the caller has verified in their owning data
    /// stores. `None` clears all such references in New mode.
    pub async fn import<O: DynamoObject>(
        &self,
        parent: Option<&PkSk>,
        bundle: DynamoBundle,
        mode: ImportMode,
        valid_out_of_table_refs: Option<&HashSet<PkSk>>,
    ) -> Result<DynamoImportResult, ServerError> {
        impl_import::import_bundle::<O>(
            self.dynamo_util,
            self.crud_algorithms,
            parent,
            bundle,
            mode,
            valid_out_of_table_refs,
        )
        .await
    }
}

impl DynamoBundle {
    /// Validates the portable bundle's version, topology, IDs, data shape, and
    /// reference paths without performing database access.
    pub fn validate(&self) -> Result<(), ServerError> {
        utils_bundle_validation::validate_bundle(self)
    }
}

// Tests.
// ----------------------------------------------------------------------------

#[cfg(test)]
mod tests;
