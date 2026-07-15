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
    ext::crud::DynamoCrudAlgorithms,
    schema::{DynamoObject, NestingLogic, PkSk},
    util::DynamoUtil,
};

pub async fn export<O: DynamoObject>(
    util: &DynamoUtil,
    algorithms: &dyn DynamoCrudAlgorithms,
    item: O,
) -> Result<DynamoBundle, ServerError> {
    export::export_from_config(
        util,
        algorithms,
        item.id().clone(),
        root_nesting::<O>(),
        BundleIdLogic::from_object::<O>(),
        false,
    )
    .await
}

pub async fn export_recursive<O: DynamoObject>(
    util: &DynamoUtil,
    algorithms: &dyn DynamoCrudAlgorithms,
    item: O,
) -> Result<DynamoBundle, ServerError> {
    export::export_from_config(
        util,
        algorithms,
        item.id().clone(),
        root_nesting::<O>(),
        BundleIdLogic::from_object::<O>(),
        true,
    )
    .await
}

pub async fn import<O: DynamoObject>(
    util: &DynamoUtil,
    algorithms: &dyn DynamoCrudAlgorithms,
    parent: Option<&PkSk>,
    bundle: DynamoBundle,
    if_existing: IfExisting,
) -> Result<DynamoImportResult, ServerError> {
    import::import_bundle::<O>(util, algorithms, parent, bundle, if_existing).await
}

pub(crate) fn root_nesting<O: DynamoObject>() -> BundleNesting {
    match O::nesting_logic() {
        NestingLogic::Root => BundleNesting::Root,
        NestingLogic::TopLevelChildOf(_) | NestingLogic::TopLevelChildOfAny => {
            BundleNesting::TopLevel
        }
        NestingLogic::InlineChildOf(_) | NestingLogic::InlineChildOfAny => BundleNesting::Inline,
    }
}

#[cfg(test)]
mod tests;
