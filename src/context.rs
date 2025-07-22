use fractic_context::define_ctx_view;

define_ctx_view!(
    name: DynamoCtxView,
    env {
        DYNAMO_REGION: String,
    },
    secrets {},
    deps {},
    req_impl {}
);
