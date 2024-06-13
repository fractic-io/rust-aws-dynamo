#[macro_export]
macro_rules! impl_dynamo_object {
    ($type:ident, $id_label:expr, $nesting_type:expr) => {
        impl DynamoObject for $type {
            fn pk(&self) -> Option<&str> {
                self.id.as_ref().map(|pk_sk| pk_sk.pk.as_str())
            }
            fn sk(&self) -> Option<&str> {
                self.id.as_ref().map(|pk_sk| pk_sk.sk.as_str())
            }
            fn id_label() -> &'static str {
                $id_label
            }
            fn generate_pk(&self, parent_pk: &str, parent_sk: &str, new_id: &str) -> String {
                match $nesting_type {
                    NestingType::Root => format!("ROOT"),
                    NestingType::TopLevelChild => format!("{parent_sk}"),
                    NestingType::InlineChild => format!("{parent_pk}"),
                }
            }
            fn generate_sk(&self, parent_pk: &str, parent_sk: &str, new_id: &str) -> String {
                match $nesting_type {
                    NestingType::Root => format!("{}#{new_id}", $id_label),
                    NestingType::TopLevelChild => format!("{}#{new_id}", $id_label),
                    NestingType::InlineChild => format!("{parent_sk}#{}#{new_id}", $id_label),
                }
            }
        }
    };
}
