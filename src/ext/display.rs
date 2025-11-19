// Optional add-on to implement fmt::Display for DynamoObject or
// MaybeCommittedDynamoObject types.
//
// Requires T::Data implements fmt::Display.
// ---------------------------------------------------------------------------

#[macro_export]
macro_rules! with_impl_display {
    ($type:ident) => {
        impl std::fmt::Display for $type {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                use $crate::schema::DynamoObject as _;
                use $crate::schema::MaybeCommittedDynamoObject as _;
                write!(f, "{}", self.data())
            }
        }
    };
}
