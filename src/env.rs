use fractic_env_config::{define_env_config, define_env_variable, EnvConfigEnum};

define_env_variable!(DYNAMO_REGION);

define_env_config!(
    DynamoEnvConfig,
    DynamoRegion => DYNAMO_REGION,
);
