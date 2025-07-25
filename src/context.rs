use fractic_context::define_ctx_view;

define_ctx_view!(
    name: DynamoCtxView,
    env {
        DYNAMO_REGION: String,
    },
    secrets {},
    deps_overlay {
        dyn crate::util::backend::DynamoBackend,
    },
    req_impl {}
);

#[cfg(test)]
pub(crate) mod test_ctx {
    use fractic_context::define_ctx;

    define_ctx!(
        name: TestCtx,
        env {
            DYNAMO_REGION: String,
        },
        secrets_fetch_region: DUMMY,
        secrets_fetch_id: DUMMY,
        secrets {},
        deps {},
        views {
            crate::DynamoCtxView,
        }
    );
}
