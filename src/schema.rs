use std::{borrow::Cow, collections::HashMap};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub mod add_ons;
pub mod coordinate;
pub mod display;
pub mod foreign_ref;
pub(crate) mod identifiers;
pub mod parsing;
pub mod pk_sk;
pub mod timestamp;

type IdKeyFn<T> = dyn for<'a> Fn(&'a T) -> Cow<'a, str>;

pub enum IdLogic<T: DynamoObjectData> {
    /// New IDs are generated as UUID v4 values, encoded as 22 base62 characters
    /// (lossless, 122 bits of entropy). This option should be used in almost
    /// all cases.
    ///
    /// Format: `LABEL#<base62-uuid-v4>`
    UuidV4,

    /// New IDs are generated as UUID v7 values, encoded as 22 base62 characters
    /// (lossless, 74 bits of entropy / ms). UUID v7 places a Unix epoch
    /// timestamp in the high bits, so DynamoDB's lexicographical sort order
    /// provides efficient date-based ordering and filtering while retaining
    /// strong random uniqueness within each millisecond.
    ///
    /// This is a solid option for simple ordering and efficient filtering.
    /// Since UUID-v7 is monotonic within-process, even batch-add operations
    /// (that may generate many IDs within the same millisecond) result in
    /// stable lexicographical ID ordering automatically.
    ///
    /// Note, however:
    /// - Object creation date is leaked to users by object ID.
    /// - Re-ordering is not possible.
    /// - Changing ID logic later can be very risky / complex, so should
    ///   consider all future use-cases from the beginning.
    ///
    /// For complex ordering, re-ordering, and stronger order guarantees, use
    /// UuidV4 with ordered insertion (flexible but inneficient) or a GSI based
    /// on a separate timestamp field (efficient but requires extra storage).
    ///
    /// Use [`PkSk::uuid_v7_lower_bound`] and [`PkSk::uuid_v7_upper_bound`] to
    /// construct keys for UUID-v7 comparisons, or UUID-v7-specific
    /// [`DynamoQuery::pk(...).sk_uuidv7_range(...)`] queries for efficient
    /// filtering.
    ///
    /// Format: `LABEL#<base62-uuid-v7>`
    UuidV7,

    /// Only one version of this object exists for a given parent, prefixed with
    /// a '@'. Subsequent writes always overwrite the existing object.
    ///
    /// <new-obj-id>: @LABEL
    Singleton,

    /// Acts as a kind of map, where the key (determined by a given field in the
    /// object) is included in the ID so it can be efficiently queried.
    /// Subsequent writes for objects with the same key overwrite the existing
    /// object.
    ///
    /// <new-obj-id>: @LABEL[<key>]
    IndexedSingleton(Box<IdKeyFn<T>>),

    /// Efficient batch-only access.
    ///
    /// WARNING: When using this ID logic, each item contains only a single '..'
    /// key with the batch's items as an array. As such, no individual item
    /// CRUD, GSIs, or property-based Dynamo operations are possible.
    ///
    /// With BatchOptimized, the only supported actions are:
    /// - query::<T>(...) (and variants)
    /// - batch_replace_all_ordered::<T>(...)
    /// - batch_delete_all::<T>(...)
    /// - item_exists(...) (caveat: only confirms batch existence)
    ///
    /// Items are stored in batches, with batch_size items per batch. IDs are
    /// sequential numbers, zero-padded to the minimum number of digits required
    /// to maintain sortability.
    ///
    /// <new-obj-id>: LABEL#<batch-index>
    BatchOptimized {
        /// Should be chosen such that each batch approaches, but never exceeds,
        /// Dynamo's 400KB max item size. Must be greater than 0.
        batch_size: usize,
    },

    /// A variant of `Singleton` that supports large objects via partitioning.
    ///
    /// Auto-splits the Singleton into partitions, allowing storage of objects
    /// exceeding the max Dynamo item size. Splitting and re-assembling is
    /// handled behind the scenes, and the client-facing interface is the same
    /// as for regular Singletons.
    ///
    /// WARNING 1: When using this ID logic, each item contains only a single
    /// '##' key with the object partition as a string. As such, no GSIs or
    /// property-based Dynamo operations are possible.
    ///
    /// WARNING 2: Since a single write operation internally must touch multiple
    /// rows (and DynamoDB doesn't support transactions), there is a risk that
    /// reads performed during this time may see an invalid intermediate state.
    ///
    /// It is safe to switch an existing `Singleton` to `SingletonExt`, but not
    /// in reverse.
    ///
    /// <placeholder>: @LABEL
    /// <partition-ids>: @LABEL+N
    SingletonExt,

    /// A variant of `IndexedSingleton` that supports large objects via
    /// partitioning.
    ///
    /// Like `SingletonExt`, splitting and re-assembling is handled behind the
    /// scenes, and the client-facing interface is the same as for regular
    /// IndexedSingletons.
    ///
    /// WARNING 1: When using this ID logic, each item contains only a single
    /// '##' key with the object partitions as a string. As such, no GSIs or
    /// property-based Dynamo operations are possible.
    ///
    /// WARNING 2: Since a single write operation internally must touch multiple
    /// rows (and DynamoDB doesn't support transactions), there is a risk that
    /// reads performed during this time may see an invalid intermediate state.
    ///
    /// It is safe to switch an existing `IndexedSingleton` to
    /// `IndexedSingletonExt`, but not in reverse.
    ///
    /// <placeholder>: @LABEL[<key>]
    /// <partition-ids>: @LABEL[<key>]+N
    IndexedSingletonExt(Box<IdKeyFn<T>>),

    /// A *non-persisted* placement handle that can act as a typed parent for
    /// other objects.
    ///
    /// Phantom objects are not to be actually written to the database, and the
    /// DynamoUtil will reject any create, update or delete requests on phantom
    /// objects themselves.
    ///
    /// <new-obj-id>: None
    Phantom,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NestingLogic {
    /// Warning:
    ///   All Root objects are stored under the 'ROOT' pk, so this key will
    ///   likely get very hot (and therefore throttled) if this data is
    ///   frequently used. If clients need frequent access, consider ways to
    ///   move the data down into more specific pk's using the other types,
    ///   using phantom parents, or -- if the data is fairly static -- consider
    ///   using a separate access pattern outside of DynamoDB (such as S3 behind
    ///   a cached API function).
    ///
    /// Note:
    ///   In all of the below options, the parent pk/sk may indicate a real
    ///   parent object, but they don't have to. The concept of the 'parent' is
    ///   used kind of like a placement indicator for the ID-generation logic,
    ///   but may be a phantom object (i.e. not actually written to database).
    ///
    /// Objects are placed under 'ROOT' partition.
    ///   pk: ROOT
    ///   sk: <new-obj-id>
    Root,

    /// Objects are placed under separate partition based on parent's sk. As
    /// such, the child objects require a separate query to fetch.
    ///   pk: parent.sk
    ///   sk: <new-obj-id>
    TopLevelChildOf(&'static str), // Validates parent's object type.
    TopLevelChildOfAny,

    /// Objects are placed under the same partition as the parent object. This
    /// way the child objects can often be directly inlined into the search
    /// results for the parent (by querying sk prefix).
    ///   pk: parent.pk
    ///   sk: parent.sk#<new-obj-id>
    InlineChildOf(&'static str), // Validates parent's object type.
    InlineChildOfAny,
}

pub trait DynamoObject:
    Serialize + DeserializeOwned + std::fmt::Debug + Send + Sync + 'static
{
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
    fn renamed_fields() -> &'static [DynamoFieldRename] {
        &[]
    }
    /// Names of top-level attributes derived from this object's canonical data
    /// and persisted alongside it.
    fn materialized_attribute_names() -> &'static [&'static str] {
        &[]
    }
    /// Computes the persisted materialized attributes for this object.
    ///
    /// Implementations must be deterministic functions of `id` and `data`.
    fn materialized_attributes(_id: &PkSk, _data: &Self::Data) -> MaterializedAttributesResult {
        Ok(MaterializedAttributes::default())
    }

    // Data:
    fn data(&self) -> &Self::Data;
    fn data_mut(&mut self) -> &mut Self::Data;
    fn into_data(self) -> Self::Data;

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
    fn ttl(&self) -> Option<i64> {
        self.auto_fields().ttl
    }
    fn has_unknown_fields(&self) -> bool {
        !self.auto_fields().unknown_fields.is_empty()
    }
    fn unknown_field_keys(&self) -> Vec<&String> {
        self.auto_fields().unknown_fields.keys().collect()
    }
}

/// The reason we require Default is to be maximally tolerant during
/// deserialization. This way, for example, if we are querying a GSI which only
/// projects some of the keys, we are still guaranteed to successfully
/// deserialize the resulting objects.
///
/// Strict data ownership / lifetime requirements are to allow DynamoObjects to
/// easily be passed between threads.
pub trait DynamoObjectData:
    Serialize + DeserializeOwned + std::fmt::Debug + Default + Clone + Send + Sync + 'static
{
}

impl<T> DynamoObjectData for T where
    T: Serialize + DeserializeOwned + std::fmt::Debug + Default + Clone + Send + Sync + 'static
{
}

/// A persisted top-level field rename accepted during Dynamo deserialization.
///
/// `from` is the legacy attribute name and `to` is the current canonical name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DynamoFieldRename {
    pub from: &'static str,
    pub to: &'static str,
}

impl DynamoFieldRename {
    pub fn is_noop(&self) -> bool {
        self.from.is_empty() || self.to.is_empty() || self.from == self.to
    }
}

/// A set of top-level attributes computed from a [`DynamoObject`]'s ID and
/// canonical data and persisted with the object.
///
/// This type is normally constructed by `dynamo_object!`'s `materialized`
/// option rather than directly.
#[derive(Debug, Default)]
pub struct MaterializedAttributes {
    pub(crate) entries: Vec<(
        &'static str,
        Option<aws_sdk_dynamodb::types::AttributeValue>,
    )>,
}

#[doc(hidden)]
pub type MaterializedAttributesResult =
    Result<MaterializedAttributes, fractic_server_error::ServerError>;

impl MaterializedAttributes {
    #[doc(hidden)]
    pub fn insert<T: Serialize>(
        &mut self,
        name: &'static str,
        value: T,
    ) -> Result<(), fractic_server_error::ServerError> {
        let value = serde_json::to_value(value).map_err(|error| {
            crate::errors::DynamoItemParsingError::with_debug(
                "failed to serialize materialized attribute",
                &error,
            )
        })?;
        self.entries
            .push((name, parsing::serde_value_to_attribute_value(value)?));
        Ok(())
    }
}

/// Defines a typed DynamoDB object and its persistence behavior.
///
/// The optional `materialized` section declares top-level attributes that are
/// recomputed on every typed create or update. Each value may be any Serde
/// serializable expression; an `Option::None` value removes the attribute.
/// Materialization may depend on canonical data alone or on both the generated
/// ID and canonical data:
///
/// ```ignore
/// dynamo_object!(
///     User,
///     UserData,
///     "USER",
///     IdLogic::UuidV4,
///     NestingLogic::Root,
///     renamed = ["old_email" => "email"],
///     materialized = |id, data| {
///         "email_normalized" => data.email.trim().to_lowercase(),
///         "lookup_key" => data.active.then(|| format!("{}|{}", id.pk, id.sk)),
///     },
/// );
/// ```
///
/// Materialized attributes are supported only by ordinary, non-partitioned ID
/// logic—not `BatchOptimized`, `SingletonExt`, or `IndexedSingletonExt`.
#[macro_export]
macro_rules! dynamo_object {
    (
        $type:ident,
        $datatype:ident,
        $id_label:expr,
        $id_logic:expr,
        $nesting_logic:expr
        $(, $($options:tt)*)?
    ) => {
        $crate::dynamo_object!(
            @options
            [$type, $datatype, $id_label, $id_logic, $nesting_logic]
            [renamed: (unset)]
            [materialized: (unset)]
            $($($options)*)?
        );
    };

    (
        @options
        [$type:ident, $datatype:ident, $id_label:expr, $id_logic:expr, $nesting_logic:expr]
        [renamed: $renamed_state:tt]
        [materialized: $materialized_state:tt]
    ) => {
        $crate::dynamo_object!(
            @impl
            $type,
            $datatype,
            $id_label,
            $id_logic,
            $nesting_logic,
            $renamed_state,
            $materialized_state
        );
    };

    (
        @options
        [$type:ident, $datatype:ident, $id_label:expr, $id_logic:expr, $nesting_logic:expr]
        [renamed: (unset)]
        [materialized: $materialized_state:tt]
        renamed = [$($from:literal => $to:literal),* $(,)?]
        $(, $($rest:tt)*)?
    ) => {
        $crate::dynamo_object!(
            @options
            [$type, $datatype, $id_label, $id_logic, $nesting_logic]
            [renamed: (set [$($from => $to),*])]
            [materialized: $materialized_state]
            $($($rest)*)?
        );
    };

    (
        @options
        [$type:ident, $datatype:ident, $id_label:expr, $id_logic:expr, $nesting_logic:expr]
        [renamed: (unset)]
        [materialized: $materialized_state:tt]
        renamed = [$($from:literal -> $to:literal),* $(,)?]
        $(, $($rest:tt)*)?
    ) => {
        $crate::dynamo_object!(
            @options
            [$type, $datatype, $id_label, $id_logic, $nesting_logic]
            [renamed: (set [$($from => $to),*])]
            [materialized: $materialized_state]
            $($($rest)*)?
        );
    };

    (
        @options
        [$type:ident, $datatype:ident, $id_label:expr, $id_logic:expr, $nesting_logic:expr]
        [renamed: $renamed_state:tt]
        [materialized: (unset)]
        materialized = |$data:ident| {
            $($name:literal => $value:expr),* $(,)?
        }
        $(, $($rest:tt)*)?
    ) => {
        $crate::dynamo_object!(
            @options
            [$type, $datatype, $id_label, $id_logic, $nesting_logic]
            [renamed: $renamed_state]
            [materialized: (one [$data] [$($name => $value),*])]
            $($($rest)*)?
        );
    };

    (
        @options
        [$type:ident, $datatype:ident, $id_label:expr, $id_logic:expr, $nesting_logic:expr]
        [renamed: $renamed_state:tt]
        [materialized: (unset)]
        materialized = |$id:ident, $data:ident| {
            $($name:literal => $value:expr),* $(,)?
        }
        $(, $($rest:tt)*)?
    ) => {
        $crate::dynamo_object!(
            @options
            [$type, $datatype, $id_label, $id_logic, $nesting_logic]
            [renamed: $renamed_state]
            [materialized: (two [$id, $data] [$($name => $value),*])]
            $($($rest)*)?
        );
    };

    (
        @options
        [$($base:tt)*]
        [renamed: (set $renamed:tt)]
        [materialized: $materialized:tt]
        renamed = $duplicate:tt
        $($rest:tt)*
    ) => {
        compile_error!("duplicate `renamed` option in dynamo_object!");
    };

    (
        @options
        [$($base:tt)*]
        [renamed: $renamed:tt]
        [materialized: $materialized:tt]
        materialized = $duplicate:tt
        $($rest:tt)*
    ) => {
        compile_error!("duplicate `materialized` option in dynamo_object!");
    };

    (
        @options
        [$($base:tt)*]
        [renamed: $renamed:tt]
        [materialized: $materialized:tt]
        $($invalid:tt)+
    ) => {
        compile_error!("invalid option syntax in dynamo_object!");
    };

    (@renamed_impl (unset)) => {
        fn renamed_fields() -> &'static [$crate::schema::DynamoFieldRename] {
            &[]
        }
    };

    (@renamed_impl (set [$($from:literal => $to:literal),*])) => {
        fn renamed_fields() -> &'static [$crate::schema::DynamoFieldRename] {
            &[
                $(
                    $crate::schema::DynamoFieldRename {
                        from: $from,
                        to: $to,
                    }
                ),*
            ]
        }
    };

    (@materialized_impl (unset)) => {
        fn materialized_attribute_names() -> &'static [&'static str] {
            &[]
        }
    };

    (@materialized_impl (one [$data:ident] [$($name:literal => $value:expr),*])) => {
        fn materialized_attribute_names() -> &'static [&'static str] {
            &[$($name),*]
        }

        fn materialized_attributes(
            _id: &$crate::schema::PkSk,
            data: &Self::Data,
        ) -> $crate::schema::MaterializedAttributesResult {
            let $data = data;
            let mut attributes = $crate::schema::MaterializedAttributes::default();
            $(attributes.insert($name, $value)?;)*
            Ok(attributes)
        }
    };

    (@materialized_impl (two [$id:ident, $data:ident] [$($name:literal => $value:expr),*])) => {
        fn materialized_attribute_names() -> &'static [&'static str] {
            &[$($name),*]
        }

        fn materialized_attributes(
            id: &$crate::schema::PkSk,
            data: &Self::Data,
        ) -> $crate::schema::MaterializedAttributesResult {
            let $id = id;
            let $data = data;
            let mut attributes = $crate::schema::MaterializedAttributes::default();
            $(attributes.insert($name, $value)?;)*
            Ok(attributes)
        }
    };

    (
        @impl
        $type:ident,
        $datatype:ident,
        $id_label:expr,
        $id_logic:expr,
        $nesting_logic:expr,
        $renamed:tt,
        $materialized:tt
    ) => {
        #[derive(Debug, Serialize, Deserialize, Clone)]
        pub struct $type {
            pub id: $crate::schema::PkSk,

            #[serde(flatten)]
            pub data: $datatype,

            // Must be last to capture unknown fields.
            #[serde(flatten)]
            pub auto_fields: $crate::schema::AutoFields,
        }

        impl $crate::schema::DynamoObject for $type {
            type Data = $datatype;

            fn new(id: $crate::schema::PkSk, data: Self::Data) -> Self {
                Self {
                    id,
                    data,
                    auto_fields: $crate::schema::AutoFields::default(),
                }
            }

            fn id(&self) -> &$crate::schema::PkSk {
                &self.id
            }
            fn data(&self) -> &Self::Data {
                &self.data
            }
            fn data_mut(&mut self) -> &mut Self::Data {
                &mut self.data
            }
            fn into_data(self) -> Self::Data {
                self.data
            }
            fn auto_fields(&self) -> &$crate::schema::AutoFields {
                &self.auto_fields
            }

            fn id_label() -> &'static str {
                $id_label
            }
            fn id_logic() -> $crate::schema::IdLogic<$datatype> {
                $id_logic
            }
            fn nesting_logic() -> $crate::schema::NestingLogic {
                $nesting_logic
            }
            $crate::dynamo_object!(@renamed_impl $renamed);
            $crate::dynamo_object!(@materialized_impl $materialized);
        }
    };
}

#[macro_export]
macro_rules! phantom_object {
    ($type:ident, $id_label:expr, |$($arg:ident : $arg_ty:ty),* $(,)?| $id:expr) => {
        $crate::phantom_object!(
            @impl $type,
            $id_label,
            ($($arg : $arg_ty),*),
            $id
        );
    };

    ($type:ident, $id_label:expr, $id:expr) => {
        $crate::phantom_object!(@impl $type, $id_label, (), $id);
    };

    (@impl $type:ident, $id_label:expr, ($($arg:ident : $arg_ty:ty),*), $id:expr) => {
        #[derive(Debug, Serialize, Deserialize, Clone)]
        pub struct $type {
            pub id: $crate::schema::PkSk,

            #[serde(flatten)]
            pub data: $crate::schema::PhantomObjectData,

            // Must be last to capture unknown fields.
            #[serde(flatten)]
            pub auto_fields: $crate::schema::AutoFields,
        }

        impl $type {
            pub fn of($($arg: $arg_ty),*) -> Self {
                <$type as $crate::schema::DynamoObject>::new(
                    $id,
                    $crate::schema::PhantomObjectData::default(),
                )
            }
        }

        impl $crate::schema::DynamoObject for $type {
            type Data = $crate::schema::PhantomObjectData;

            fn new(id: $crate::schema::PkSk, data: Self::Data) -> Self {
                Self {
                    id,
                    data,
                    auto_fields: $crate::schema::AutoFields::default(),
                }
            }

            fn id(&self) -> &$crate::schema::PkSk {
                &self.id
            }
            fn data(&self) -> &Self::Data {
                &self.data
            }
            fn data_mut(&mut self) -> &mut Self::Data {
                &mut self.data
            }
            fn into_data(self) -> Self::Data {
                self.data
            }
            fn auto_fields(&self) -> &$crate::schema::AutoFields {
                &self.auto_fields
            }

            fn id_label() -> &'static str {
                $id_label
            }
            fn id_logic() -> $crate::schema::IdLogic<Self::Data> {
                $crate::schema::IdLogic::Phantom
            }
            fn nesting_logic() -> $crate::schema::NestingLogic {
                $crate::schema::NestingLogic::Root
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
    fn into_data(self) -> T::Data;
}

// Standard sub-structs:
// ---------------------------

/// Custom struct to hold 'pk' and 'sk', which gets serialized and deserialized
/// as "pk|sk" in communication with downstream clients, but are separate
/// properties in the underlying data store (primary_key and sort_key).
#[derive(Debug, PartialEq, Clone, Hash, Eq)]
pub struct PkSk {
    pub pk: String,
    pub sk: String,
}

/// Custom struct to hold a minimal reference to a PkSk for efficient database
/// storage, generally only storing the terminal ID value. The original PkSk can
/// be reconstructed by manually providing the context Pk and Sk prefixes. This
/// can be particularly useful for indexed singleton keys or GSIs.
#[derive(Debug, PartialEq, Clone, Hash, Eq)]
pub struct ForeignRef<'a>(Cow<'a, str>);

/// Empty data payload used by phantom objects.
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq)]
pub struct PhantomObjectData {}

/// Fields automatically populated by DynamoUtil. This struct is automatically
/// included in all DynamoObjects as a flattened field:
///
/// #[serde(flatten)]
/// pub auto_fields: AutoFields,
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
    #[serde(skip_serializing)] // Read-only.
    pub ttl: Option<i64>,
    #[serde(flatten, skip_serializing)] // Read-only.
    pub unknown_fields: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Timestamp {
    pub seconds: i64,
    pub nanos: u32,
}

/// Geographic coordinate (WGS-84) with optional zoom level.
///
/// * `latitude`  ∈ [-90.0, 90.0]
/// * `longitude` ∈ [-180.0, 180.0]
#[derive(Debug, Clone, PartialEq)]
pub struct Coordinate {
    pub latitude: f64,
    pub longitude: f64,
    pub zoom: Option<f64>,
}

/// Can be used to represent a rare state that can be used in a sparse index
/// GSI.
///
/// Instead of:
///   #[serde(default)]
///  pub is_processing: bool,
///
/// Can use:
///   pub is_processing: Option<True>,
///
/// Thereby allowing the GSI to only store the rare items that are processing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum True {
    #[serde(rename = "true")]
    Unit,
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
    dynamo_object!(
        Test1,
        Test1Data,
        "TEST1",
        IdLogic::UuidV4,
        NestingLogic::Root
    );

    #[derive(Debug, Serialize, Deserialize, Clone, Default)]
    pub struct Test2Data {}
    dynamo_object!(
        Test2,
        Test2Data,
        "TEST2",
        IdLogic::UuidV7,
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
        IdLogic::IndexedSingleton(Box::new(|obj: &Test4Data| Cow::Borrowed(&obj.key))),
        NestingLogic::InlineChildOf("TEST3")
    );
    phantom_object!(TestLookup, "LOOKUP", |space: &str, key: &str| PkSk {
        pk: "ROOT".to_string(),
        sk: format!("LOOKUP#{space}-{key}"),
    });
    phantom_object!(
        TestRootLookup,
        "ROOTLOOKUP",
        PkSk {
            pk: "ROOT".to_string(),
            sk: "ROOTLOOKUP".to_string(),
        }
    );

    #[test]
    fn test_auto_fields_default() {
        let obj = Test1 {
            id: PkSk::root().clone(),
            auto_fields: AutoFields::default(),
            data: Test1Data::default(),
        };
        assert!(obj.auto_fields.created_at.is_none());
        assert!(obj.auto_fields.updated_at.is_none());
        assert!(obj.auto_fields.sort.is_none());
        assert!(obj.auto_fields.ttl.is_none());
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
            ttl: Some(1625247602),
            unknown_fields,
        };

        let obj = Test1 {
            id: PkSk::root().clone(),
            auto_fields: auto_fields.clone(),
            data: Test1Data::default(),
        };

        assert_eq!(obj.created_at().unwrap().seconds, 1625247600);
        assert_eq!(obj.updated_at().unwrap().seconds, 1625247601);
        assert_eq!(obj.sort().unwrap(), 1.0);
        assert_eq!(obj.ttl().unwrap(), 1625247602);
        assert!(obj.has_unknown_fields());
        assert_eq!(obj.unknown_field_keys(), vec![&String::from("key")]);
    }

    #[test]
    fn test_id_logic_accessor() {
        assert!(matches!(Test1::id_logic(), IdLogic::UuidV4));
        assert!(matches!(Test2::id_logic(), IdLogic::UuidV7));
        assert!(matches!(Test3::id_logic(), IdLogic::Singleton));
        assert!(matches!(Test4::id_logic(), IdLogic::IndexedSingleton(_)));
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
    fn test_indexed_singleton_key_fn() {
        let obj = Test4 {
            id: PkSk::root().clone(),
            auto_fields: AutoFields::default(),
            data: Test4Data {
                key: String::from("KEY"),
            },
        };
        let IdLogic::IndexedSingleton(key_fn) = Test4::id_logic() else {
            panic!("Expected IndexedSingleton.");
        };
        assert_eq!(key_fn(&obj.data), "KEY");
    }

    #[test]
    fn test_phantom_object_macro() {
        let obj = TestLookup::of("image", "abc");

        assert_eq!(obj.id().pk, "ROOT");
        assert_eq!(obj.id().sk, "LOOKUP#image-abc");
        assert_eq!(TestLookup::id_label(), "LOOKUP");
        assert!(matches!(TestLookup::id_logic(), IdLogic::Phantom));
        assert!(matches!(TestLookup::nesting_logic(), NestingLogic::Root));

        let root_obj = TestRootLookup::of();

        assert_eq!(root_obj.id().pk, "ROOT");
        assert_eq!(root_obj.id().sk, "ROOTLOOKUP");
        assert_eq!(TestRootLookup::id_label(), "ROOTLOOKUP");
        assert!(matches!(TestRootLookup::id_logic(), IdLogic::Phantom));
        assert!(matches!(
            TestRootLookup::nesting_logic(),
            NestingLogic::Root
        ));
    }
}
