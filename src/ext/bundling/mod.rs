//! Portable export and import of complete Dynamo object trees.

mod export;
mod import;
mod model;
mod policy;
mod value;

pub use model::*;
pub use policy::{DynamoBundlePolicy, DynamoBundleReferenceMatch, DynamoBundleReferenceRule};

use fractic_server_error::ServerError;

use crate::{
    errors::DynamoInvalidOperation,
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
        export::export_from_config(
            self.dynamo_util,
            self.crud_algorithms,
            item.id().clone(),
            root_nesting::<O>(),
            BundleIdLogic::from_object::<O>(),
            false,
        )
        .await
    }

    pub async fn export_deep<O: DynamoObject>(&self, item: O) -> Result<DynamoBundle, ServerError> {
        export::export_from_config(
            self.dynamo_util,
            self.crud_algorithms,
            item.id().clone(),
            root_nesting::<O>(),
            BundleIdLogic::from_object::<O>(),
            true,
        )
        .await
    }

    pub async fn import<O: DynamoObject>(
        &self,
        parent: Option<&PkSk>,
        bundle: DynamoBundle,
        if_existing: IfExisting,
    ) -> Result<DynamoImportResult, ServerError> {
        import::import_bundle::<O>(
            self.dynamo_util,
            self.crud_algorithms,
            parent,
            bundle,
            if_existing,
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

pub(crate) fn invalid_bundle(details: &str) -> ServerError {
    DynamoInvalidOperation::new(&format!("invalid Dynamo bundle: {details}"))
}

// Tests.
// ----------------------------------------------------------------------------

#[cfg(test)]
mod tests;
