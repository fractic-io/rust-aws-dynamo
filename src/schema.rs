use std::collections::HashMap;

use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub mod add_ons;
pub(crate) mod id_calculations;
pub mod parsing;
pub mod pk_sk;
pub mod timestamp;

pub enum IdLogic<T: DynamoObjectData> {
    // New IDs are generated based on UUID v4. This option should be used in
    // almost all cases.
    //
    // <new-obj-id>: LABEL#<uuid>
    Uuid,

    // New IDs are generated based on epoch timestamp. Can be used for efficient
    // date-based ordering and filtering, since the date range can be directly
    // filtered in the query.
    //
    // However, a couple important things to consider:
    // - Object creation date is leaked to users by object ID.
    // - IDs are "guessable", which could be a security concern.
    // - If multiple children for the same parent are written in the same
    // second, they will have the same ID, and the second write will overwrite
    // the first.
    // - Changing ID logic later can be very risky / complex, so should consider
    // all future use-cases from the beginning.
    //
    // An alternative strategy would be to use a UUID-based ID with ordered
    // insertion (flexible but inneficient) or a GSI based on a separate
    // timestamp field (efficient but requires extra storage).
    //
    // <new-obj-id>: LABEL#<timestamp>
    Timestamp,

    // Only one version of this object exists for a given parent, prefixed with
    // a '@'. Subsequent writes always overwrite the existing object.
    //
    // <new-obj-id>: @LABEL
    Singleton,

    // Kind of like an indexable Singleton map, where the key (determined by a
    // given field in the object) is used as a key to the label that can be
    // efficiently queried.
    //
    // <new-obj-id>: @LABEL[<key>]
    SingletonFamily(Box<dyn Fn(&T) -> String>),
}

#[derive(Debug, PartialEq)]
pub enum NestingLogic {
    // Warning:
    //   All Root objects are stored under the 'ROOT' pk, so this key will
    //   likely get very hot (and therefore throttled) if this data is
    //   frequently used. If clients need frequent access, consider ways to move
    //   the data down into more specific pk's using the other types, using
    //   phantom parents, or -- if the data is fairly static -- consider using a
    //   separate access pattern outside of DynamoDB (such as S3 behind a cached
    //   API function).

    // Note:
    //   In all of the below options, the parent pk/sk may indicate a real
    //   parent object, but they don't have to. The concept of the 'parent' is
    //   used kind of like a placement indicator for the ID-generation logic,
    //   but may be a phantom object (i.e. not actually written to database).

    // Objects are placed under 'ROOT' partition (regardless of parent IDs
    // provided).
    //   pk: ROOT
    //   sk: <new-obj-id>
    Root,

    // Objects are placed under separate partition based on parent's sk. As
    // such, the child objects require a separate query to fetch.
    //   pk: parent.sk
    //   sk: <new-obj-id>
    TopLevelChildOfAny,
    TopLevelChildOf(&'static str), // Validates parent's object type.

    // Objects are placed under the same partition as the parent object. This
    // way the child objects can often be directly inlined into the search
    // results for the parent (by querying sk prefix).
    //   pk: parent.pk
    //   sk: parent.sk#<new-obj-id>
    InlineChildOfAny,
    InlineChildOf(&'static str), // Validates parent's object type.
}

pub trait DynamoObject: Serialize + DeserializeOwned + std::fmt::Debug {
    type Data: DynamoObjectData;

    fn new(id: PkSk, data: Self::Data) -> Self;

    // ID calculations:
    fn id(&self) -> &PkSk;
    fn pk(&self) -> &str {
        self.id().pk.as_str()
    }
    fn sk(&self) -> &str {
        self.id().sk.as_str()
    }
    fn id_label() -> &'static str;
    fn id_logic() -> IdLogic<Self::Data>;
    fn nesting_logic() -> NestingLogic;

    // Data:
    fn data(&self) -> &Self::Data;

    // Auto-fields accessors:
    fn auto_fields(&self) -> &AutoFields;
    fn created_at(&self) -> Option<&Timestamp> {
        self.auto_fields().created_at.as_ref()
    }
    fn updated_at(&self) -> Option<&Timestamp> {
        self.auto_fields().updated_at.as_ref()
    }
    fn sort(&self) -> Option<f64> {
        self.auto_fields().sort
    }
    fn has_unknown_fields(&self) -> bool {
        !self.auto_fields().unknown_fields.is_empty()
    }
    fn unknown_field_keys(&self) -> Vec<&String> {
        self.auto_fields().unknown_fields.keys().collect()
    }
}

pub trait DynamoObjectData:
    Serialize + DeserializeOwned + std::fmt::Debug + Default + Clone
{
}

#[macro_export]
macro_rules! dynamo_object {
    ($type:ident, $datatype:ident, $id_label:expr, $id_logic:expr, $nesting_logic:expr) => {
        #[derive(Debug, Serialize, Deserialize, Clone)]
        pub struct $type {
            pub id: PkSk,

            #[serde(flatten)]
            pub data: $datatype,

            // Must be last to capture unknown fields.
            #[serde(flatten)]
            pub auto_fields: AutoFields,
        }

        impl DynamoObjectData for $datatype {}

        impl DynamoObject for $type {
            type Data = $datatype;

            fn new(id: PkSk, data: Self::Data) -> Self {
                Self {
                    id,
                    data,
                    auto_fields: AutoFields::default(),
                }
            }

            fn id(&self) -> &PkSk {
                &self.id
            }
            fn data(&self) -> &Self::Data {
                &self.data
            }
            fn auto_fields(&self) -> &AutoFields {
                &self.auto_fields
            }

            fn id_label() -> &'static str {
                $id_label
            }
            fn id_logic() -> IdLogic<$datatype> {
                $id_logic
            }
            fn nesting_logic() -> NestingLogic {
                $nesting_logic
            }
        }
    };
}

// Dynamic trait to hold either committed (with ID) or uncommitted (only data)
// versions of a DynamoObject. See 'with_maybe_committed_scaffolding!' add-on.
pub trait MaybeCommittedDynamoObject<T: DynamoObject> {
    fn id(&self) -> Option<&PkSk>;
    fn data(&self) -> &T::Data;
    fn data_mut(&mut self) -> &mut T::Data;
}

// Standard sub-structs:
// ---------------------------

// Custom struct to hold 'pk' and 'sk', which gets serialized and deserialized
// as "pk|sk" in communication with downstream clients, but are separate
// properties in the underlying data store (primary_key and sort_key).
#[derive(Debug, PartialEq, Clone, Hash, Eq)]
pub struct PkSk {
    pub pk: String,
    pub sk: String,
}

// Fields automatically populated by DynamoUtil. This struct is automatically
// included in all DynamoObjects as a flattened field:
//
// #[serde(flatten)]
// pub auto_fields: AutoFields,
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq)]
pub struct AutoFields {
    // Since these are manually handled by DynamoUtil, they should be read-only.
    // This is can be accomplished by skipping serialization, since it will
    // cause these fields to be skipped when converting to the DynomoMap. This
    // also means they will not be included when an object is converted to JSON
    // (ex. when this object is sent to a client by an API), so if they are
    // needed they should be manually read and included using the accessors.
    #[serde(skip_serializing)] // Read-only.
    pub created_at: Option<Timestamp>,
    #[serde(skip_serializing)] // Read-only.
    pub updated_at: Option<Timestamp>,
    #[serde(skip_serializing)] // Read-only.
    pub sort: Option<f64>,
    #[serde(flatten, skip_serializing)] // Read-only.
    pub unknown_fields: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Timestamp {
    pub seconds: i64,
    pub nanos: u32,
}

// Tests.
// ---------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    #[derive(Debug, Serialize, Deserialize, Clone, Default)]
    pub struct Test1Data {}
    dynamo_object!(Test1, Test1Data, "TEST1", IdLogic::Uuid, NestingLogic::Root);

    #[derive(Debug, Serialize, Deserialize, Clone, Default)]
    pub struct Test2Data {}
    dynamo_object!(
        Test2,
        Test2Data,
        "TEST2",
        IdLogic::Timestamp,
        NestingLogic::InlineChildOfAny
    );

    #[derive(Debug, Serialize, Deserialize, Clone, Default)]
    pub struct Test3Data {}
    dynamo_object!(
        Test3,
        Test3Data,
        "TEST3",
        IdLogic::Singleton,
        NestingLogic::TopLevelChildOf("TEST2")
    );

    #[derive(Debug, Serialize, Deserialize, Clone, Default)]
    pub struct Test4Data {
        key: String,
    }
    dynamo_object!(
        Test4,
        Test4Data,
        "TEST4",
        IdLogic::SingletonFamily(Box::new(|obj: &Test4Data| obj.key.clone())),
        NestingLogic::InlineChildOf("TEST3")
    );

    #[test]
    fn test_auto_fields_default() {
        let obj = Test1 {
            id: PkSk::root(),
            auto_fields: AutoFields::default(),
            data: Test1Data::default(),
        };
        assert!(obj.auto_fields.created_at.is_none());
        assert!(obj.auto_fields.updated_at.is_none());
        assert!(obj.auto_fields.sort.is_none());
        assert!(obj.auto_fields.unknown_fields.is_empty());
    }

    #[test]
    fn test_id_accessors() {
        let obj = Test1 {
            id: PkSk {
                pk: String::from("PK"),
                sk: String::from("SK"),
            },
            auto_fields: AutoFields::default(),
            data: Test1Data::default(),
        };

        assert_eq!(obj.pk(), "PK");
        assert_eq!(obj.id().pk, "PK");
        assert_eq!(obj.sk(), "SK");
        assert_eq!(obj.id().sk, "SK");
    }

    #[test]
    fn test_auto_fields_accessors() {
        let mut unknown_fields = HashMap::new();
        unknown_fields.insert(String::from("key"), json!("value"));
        let auto_fields = AutoFields {
            created_at: Some(Timestamp {
                seconds: 1625247600,
                nanos: 0,
            }),
            updated_at: Some(Timestamp {
                seconds: 1625247601,
                nanos: 0,
            }),
            sort: Some(1.0),
            unknown_fields,
        };

        let obj = Test1 {
            id: PkSk::root(),
            auto_fields: auto_fields.clone(),
            data: Test1Data::default(),
        };

        assert_eq!(obj.created_at().unwrap().seconds, 1625247600);
        assert_eq!(obj.updated_at().unwrap().seconds, 1625247601);
        assert_eq!(obj.sort().unwrap(), 1.0);
        assert!(obj.has_unknown_fields());
        assert_eq!(obj.unknown_field_keys(), vec![&String::from("key")]);
    }

    #[test]
    fn test_id_logic_accessor() {
        assert!(matches!(Test1::id_logic(), IdLogic::Uuid));
        assert!(matches!(Test2::id_logic(), IdLogic::Timestamp));
        assert!(matches!(Test3::id_logic(), IdLogic::Singleton));
        assert!(matches!(Test4::id_logic(), IdLogic::SingletonFamily(_)));
    }

    #[test]
    fn test_nesting_logic_accessor() {
        assert!(matches!(Test1::nesting_logic(), NestingLogic::Root));
        assert!(matches!(
            Test2::nesting_logic(),
            NestingLogic::InlineChildOfAny
        ));
        assert!(matches!(
            Test3::nesting_logic(),
            NestingLogic::TopLevelChildOf("TEST2")
        ));
        assert!(matches!(
            Test4::nesting_logic(),
            NestingLogic::InlineChildOf("TEST3")
        ));
    }

    #[test]
    fn test_singleton_family_key_fn() {
        let obj = Test4 {
            id: PkSk::root(),
            auto_fields: AutoFields::default(),
            data: Test4Data {
                key: String::from("KEY"),
            },
        };
        let IdLogic::SingletonFamily(key_fn) = Test4::id_logic() else {
            panic!("Expected SingletonFamily.");
        };
        assert_eq!(key_fn(&obj.data), "KEY");
    }
}
