use fractic_server_error::{define_client_error, define_internal_error, define_user_error};

define_user_error!(DynamoNotFound, "Requested item does not exist.");
define_internal_error!(DynamoCalloutError, "Generic DynamoDB error.");
define_internal_error!(
    DynamoItemParsingError,
    "DynamoDB item parsing error: {details}.",
    { details: &str }
);
define_client_error!(
    DynamoInvalidId,
    "DynamoDB invalid ID: {details}.",
    { details: &str }
);
define_client_error!(
    DynamoInvalidOperation,
    "Invalid DynamoDB operation: {details}.",
    { details: &str }
);
define_client_error!(
    DynamoInvalidBatchOptimizedIdUsage,
    "Item-level operations are not supported for items with IdLogic::BatchOptimized. Use batch_replace_all_ordered instead."
);
define_client_error!(
    DynamoInvalidParent,
    "Invalid parent object type: {details}.",
    { details: &str }
);
