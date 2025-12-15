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

// Optional add-on to build struct that can dynamically store either written
// (containing ID) or unwritten (only containing data) versions of the
// DynamoObject.
// ---------------------------------------------------------------------------

#[macro_export]
macro_rules! with_maybe_committed_scaffolding {
    ($type:ident, $objecttype:ident, $datatype:ident) => {
        #[derive(Debug, Serialize, Deserialize, Clone)]
        #[serde(untagged)]
        pub enum $type {
            Committed($objecttype),
            Uncommitted($datatype),
        }

        impl $crate::schema::MaybeCommittedDynamoObject<$objecttype> for $type {
            fn id(&self) -> Option<&$crate::schema::PkSk> {
                match self {
                    $type::Committed(object) => Some(&object.id),
                    $type::Uncommitted(_) => None,
                }
            }

            fn data(&self) -> &$datatype {
                match self {
                    $type::Committed(object) => &object.data,
                    $type::Uncommitted(data) => data,
                }
            }

            fn data_mut(&mut self) -> &mut $datatype {
                match self {
                    $type::Committed(object) => &mut object.data,
                    $type::Uncommitted(data) => data,
                }
            }

            fn into_data(self) -> $datatype {
                match self {
                    $type::Committed(object) => object.data,
                    $type::Uncommitted(data) => data,
                }
            }
        }
    };
}
