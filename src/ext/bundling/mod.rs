//! Portable export and import of complete Dynamo object trees.

mod entities_bundle;
mod entities_policy;
mod impl_export;
mod impl_import;
mod impl_mapping;
mod impl_utils;
mod impl_validation;

pub use entities_bundle::*;
pub use entities_policy::{
    DynamoBundlePolicy, DynamoBundleReferenceMatch, DynamoBundleReferenceRule,
};

use fractic_server_error::ServerError;

use crate::{
    ext::crud::DynamoCrudAlgorithms,
    schema::{DynamoObject, NestingLogic, PkSk},
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
            root_nesting::<O>(),
            BundleIdLogic::from_object::<O>(),
        )
        .await
    }

    pub async fn import<O: DynamoObject>(
        &self,
        parent: Option<&PkSk>,
        bundle: DynamoBundle,
        mode: ImportMode,
    ) -> Result<DynamoImportResult, ServerError> {
        impl_import::import_bundle::<O>(
            self.dynamo_util,
            self.crud_algorithms,
            parent,
            bundle,
            mode,
        )
        .await
    }
}

// Helpers.
// ----------------------------------------------------------------------------

pub(crate) fn root_nesting<O: DynamoObject>() -> BundleNesting {
    match O::nesting_logic() {
        NestingLogic::Root => BundleNesting::Root,
        NestingLogic::TopLevelChildOf(_) | NestingLogic::TopLevelChildOfAny => {
            BundleNesting::TopLevel
        }
        NestingLogic::InlineChildOf(_) | NestingLogic::InlineChildOfAny => BundleNesting::Inline,
    }
}

// Tests.
// ----------------------------------------------------------------------------

#[cfg(test)]
mod tests;
