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
        }
    };
}
