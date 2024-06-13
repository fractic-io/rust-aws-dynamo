use std::error::Error as StdError;
use std::fmt;

use fractic_generic_server_error::{
    define_internal_error_type, define_user_visible_error_type, GenericServerError,
    GenericServerErrorTrait,
};

define_user_visible_error_type!(DynamoNotFoundError, "Requested item does not exist.");
define_internal_error_type!(DynamoConnectionError, "DynamoDB error.");
define_internal_error_type!(DynamoItemParsingError, "DynamoDB item parsing error.");
define_internal_error_type!(
    DynamoInvalidOperationError,
    "DynamoDB invalid operation error."
);
define_internal_error_type!(DynamoInvalidIdError, "DynamoDB invalid id error.");
