use fractic_generic_server_error::{
    define_internal_error_type, define_user_visible_error_type, GenericServerError,
    GenericServerErrorTrait,
};

define_user_visible_error_type!(DynamoNotFound, "Requested item does not exist.");
define_internal_error_type!(DynamoConnectionError, "Generic DynamoDB error.");
define_internal_error_type!(DynamoItemParsingError, "DynamoDB item parsing error.");
define_internal_error_type!(DynamoInvalidOperation, "Invalid DynamoDB operation.");
define_internal_error_type!(DynamoInvalidId, "DynamoDB invalid ID.");
define_internal_error_type!(
    DynamoInvalidParent,
    "Invalid DynamoDB operation: invalid parent object type."
);
