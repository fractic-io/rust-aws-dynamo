use fractic_server_error::ServerError;

use crate::{
    errors::DynamoInvalidOperation,
    schema::{
        identifiers::{
            place_terminal_segment, validate_parent_relation, ParsedIdPath, RawIdPath,
            TerminalSegmentKind,
        },
        DynamoObject, IdLogic, PkSk,
    },
};

/// Verifies that an ID belongs to `T` and is a logical object ID rather than a
/// physical ext-partition row.
#[track_caller]
pub fn validate_object_id<T: DynamoObject>(id: &PkSk) -> Result<(), ServerError> {
    let raw_path = RawIdPath::new(&id.sk);
    if id.sk != raw_path.logical_path() {
        return Err(DynamoInvalidOperation::new(&format!(
            "ID must be a logical object ID, not an ext-partition row ID: '{id}'"
        )));
    }
    let parsed: ParsedIdPath<'_> = raw_path.parse()?;
    if parsed.object_label() != T::id_label() {
        return Err(DynamoInvalidOperation::new(&format!(
            "ID does not match object type; expected object type '{}', got ID '{}'",
            T::id_label(),
            id
        )));
    }
    let expected_kind = match T::id_logic() {
        IdLogic::Singleton | IdLogic::SingletonExt => Some(TerminalSegmentKind::Singleton),
        IdLogic::IndexedSingleton(_) | IdLogic::IndexedSingletonExt(_) => {
            Some(TerminalSegmentKind::IndexedSingleton)
        }
        IdLogic::Uuid | IdLogic::Timestamp | IdLogic::BatchOptimized { .. } => {
            Some(TerminalSegmentKind::Regular)
        }
        IdLogic::Phantom => None,
    };
    if expected_kind.is_some_and(|expected| parsed.terminal_segment_kind() != expected) {
        return Err(DynamoInvalidOperation::new(&format!(
            "ID syntax does not match the configured ID logic for object type '{}': '{}'",
            T::id_label(),
            id
        )));
    }
    Ok(())
}

/// Verifies that `parent_id` satisfies `T`'s configured nesting relationship.
#[track_caller]
pub fn validate_parent_for<T: DynamoObject>(parent_id: &PkSk) -> Result<(), ServerError> {
    validate_parent_relation::<T>(parent_id)
        .map_err(|error| DynamoInvalidOperation::new(&error.to_string()))
}

/// Returns the `(pk, sk-prefix)` used to query all children of type `T`.
pub fn child_query_prefix<T: DynamoObject>(parent_id: &PkSk) -> PkSk {
    // * Singleton / IndexedSingleton →  "@LABEL"
    // * Everything else              →  "LABEL#"
    let sk_search_prefix = match T::id_logic() {
        IdLogic::Singleton
        | IdLogic::SingletonExt
        | IdLogic::IndexedSingleton(_)
        | IdLogic::IndexedSingletonExt(_) => {
            format!("@{}", T::id_label())
        }
        _ => format!("{}#", T::id_label()),
    };

    place_terminal_segment::<T>(parent_id, &sk_search_prefix)
}

#[cfg(test)]
mod tests {
    #![allow(unused)]

    use std::borrow::Cow;

    use super::*;

    use serde::{Deserialize, Serialize};

    use crate::{
        dynamo_object,
        schema::{IdLogic, NestingLogic, PkSk},
    };

    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    pub struct RootGroupData {}
    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    pub struct GroupTaskData {}
    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    pub struct GroupEventData {}
    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    pub struct InlineChildOfAnyData {}
    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    pub struct TopLevelChildOfAnyData {}
    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    pub struct InlineSingletonData {}
    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    pub struct InlineIndexedSingletonData {
        key: String,
    }
    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    pub struct InlineSingletonExtData {}
    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    pub struct InlineIndexedSingletonExtData {
        key: String,
    }
    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    pub struct InlineBatchData {}
    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    pub struct TopLevelBatchData {}

    dynamo_object!(
        RootGroup,
        RootGroupData,
        "GROUP",
        IdLogic::Uuid,
        NestingLogic::Root
    );
    dynamo_object!(
        GroupTask,
        GroupTaskData,
        "TASK",
        IdLogic::Uuid,
        NestingLogic::InlineChildOf("GROUP")
    );
    dynamo_object!(
        GroupEvent,
        GroupEventData,
        "EVENT",
        IdLogic::Uuid,
        NestingLogic::TopLevelChildOf("GROUP")
    );
    dynamo_object!(
        InlineChildOfAny,
        InlineChildOfAnyData,
        "ANYINL",
        IdLogic::Uuid,
        NestingLogic::InlineChildOfAny
    );
    dynamo_object!(
        TopLevelChildOfAny,
        TopLevelChildOfAnyData,
        "ANYTOP",
        IdLogic::Uuid,
        NestingLogic::TopLevelChildOfAny
    );
    dynamo_object!(
        InlineSingleton,
        InlineSingletonData,
        "INLSINGLE",
        IdLogic::Singleton,
        NestingLogic::InlineChildOfAny
    );
    dynamo_object!(
        InlineIndexedSingleton,
        InlineIndexedSingletonData,
        "INLFAM",
        IdLogic::IndexedSingleton(Box::new(|data: &InlineIndexedSingletonData| Cow::Borrowed(
            &data.key
        ))),
        NestingLogic::InlineChildOfAny
    );
    dynamo_object!(
        InlineSingletonExt,
        InlineSingletonExtData,
        "INLSINGLEEXT",
        IdLogic::SingletonExt,
        NestingLogic::InlineChildOfAny
    );
    dynamo_object!(
        InlineIndexedSingletonExt,
        InlineIndexedSingletonExtData,
        "INLFAMEXT",
        IdLogic::IndexedSingletonExt(Box::new(|data: &InlineIndexedSingletonExtData| {
            Cow::Borrowed(&data.key)
        })),
        NestingLogic::InlineChildOfAny
    );
    dynamo_object!(
        InlineBatch,
        InlineBatchData,
        "INLBATCH",
        IdLogic::BatchOptimized { batch_size: 10 },
        NestingLogic::InlineChildOfAny
    );
    dynamo_object!(
        TopLevelBatch,
        TopLevelBatchData,
        "TOPLBATCH",
        IdLogic::BatchOptimized { batch_size: 10 },
        NestingLogic::TopLevelChildOf("N")
    );

    #[test]
    fn test_validate_parent_for() {
        // Root OK / error --------------------------------------------------------
        assert!(validate_parent_for::<RootGroup>(PkSk::root()).is_ok());
        assert!(validate_parent_for::<RootGroup>(&PkSk {
            pk: "X".into(),
            sk: "Y".into()
        })
        .is_err());

        // TopLevelChildOf OK / wrong-type ----------------------------------------
        let group_parent = PkSk {
            pk: "ROOT".into(),
            sk: "GROUP#1".into(),
        };
        assert!(validate_parent_for::<GroupEvent>(&group_parent).is_ok());
        let wrong_parent = PkSk {
            pk: "ROOT".into(),
            sk: "OTHER#1".into(),
        };
        assert!(validate_parent_for::<GroupEvent>(&wrong_parent).is_err());

        // InlineChildOf OK / wrong-type ------------------------------------------
        let inl_parent = PkSk {
            pk: "ROOT".into(),
            sk: "GROUP#1".into(),
        };
        assert!(validate_parent_for::<GroupTask>(&inl_parent).is_ok());
        assert!(validate_parent_for::<GroupTask>(&wrong_parent).is_err());

        // “Any” variants never fail ----------------------------------------------
        assert!(validate_parent_for::<InlineChildOfAny>(&wrong_parent).is_ok());
        assert!(validate_parent_for::<TopLevelChildOfAny>(&wrong_parent).is_ok());
    }

    #[test]
    fn test_validate_object_id_rejects_ext_partition_row_ids() {
        assert!(validate_object_id::<InlineSingletonExt>(&PkSk {
            pk: "P".into(),
            sk: "S#@INLSINGLEEXT+0".into(),
        })
        .is_err());
        assert!(validate_object_id::<InlineIndexedSingletonExt>(&PkSk {
            pk: "P".into(),
            sk: "S#@INLFAMEXT[key]+12".into(),
        })
        .is_err());
        assert!(validate_object_id::<GroupTask>(&PkSk {
            pk: "P".into(),
            sk: "GROUP#1#TASK#2+0".into(),
        })
        .is_err());
    }

    #[test]
    fn test_validate_object_id_rejects_wrong_terminal_syntax() {
        assert!(validate_object_id::<InlineSingleton>(&PkSk {
            pk: "P".into(),
            sk: "INLSINGLE#value".into(),
        })
        .is_err());
        assert!(validate_object_id::<InlineIndexedSingleton>(&PkSk {
            pk: "P".into(),
            sk: "@INLFAM".into(),
        })
        .is_err());
        assert!(validate_object_id::<GroupTask>(&PkSk {
            pk: "P".into(),
            sk: "@TASK".into(),
        })
        .is_err());
    }

    #[test]
    fn test_child_query_prefix() {
        // Root-level child:
        let root_obj_search = child_query_prefix::<RootGroup>(PkSk::root());
        assert_eq!(
            root_obj_search,
            PkSk {
                pk: "ROOT".into(),
                sk: "GROUP#".into()
            }
        );

        // Top-level child:
        let top_level_child_search = PkSk {
            pk: "ROOT".into(),
            sk: "GROUP#1".into(),
        };
        assert_eq!(
            child_query_prefix::<GroupEvent>(&top_level_child_search),
            PkSk {
                pk: "GROUP#1".into(),
                sk: "EVENT#".into()
            }
        );

        // Inline-level child:
        let inline_child_search = PkSk {
            pk: "ROOT".into(),
            sk: "GROUP#1".into(),
        };
        assert_eq!(
            child_query_prefix::<GroupTask>(&inline_child_search),
            PkSk {
                pk: "ROOT".into(),
                sk: "GROUP#1#TASK#".into()
            }
        );

        // Inline Singleton:
        let inline_singleton_search = PkSk {
            pk: "P".into(),
            sk: "S".into(),
        };
        assert_eq!(
            child_query_prefix::<InlineSingleton>(&inline_singleton_search),
            PkSk {
                pk: "P".into(),
                sk: "S@INLSINGLE".into()
            }
        );

        // Inline IndexedSingleton:
        let inline_indexed_singleton_search = PkSk {
            pk: "P".into(),
            sk: "S".into(),
        };
        assert_eq!(
            child_query_prefix::<InlineIndexedSingleton>(&inline_indexed_singleton_search),
            PkSk {
                pk: "P".into(),
                sk: "S@INLFAM".into()
            }
        );

        // Inline SingletonExt:
        assert_eq!(
            child_query_prefix::<InlineSingletonExt>(&inline_singleton_search),
            PkSk {
                pk: "P".into(),
                sk: "S@INLSINGLEEXT".into()
            }
        );

        // Inline IndexedSingletonExt:
        assert_eq!(
            child_query_prefix::<InlineIndexedSingletonExt>(&inline_indexed_singleton_search),
            PkSk {
                pk: "P".into(),
                sk: "S@INLFAMEXT".into()
            }
        );

        // Inline BatchOptimized:
        let inline_batch_search = PkSk {
            pk: "P".into(),
            sk: "S".into(),
        };
        assert_eq!(
            child_query_prefix::<InlineBatch>(&inline_batch_search),
            PkSk {
                pk: "P".into(),
                sk: "S#INLBATCH#".into()
            }
        );

        // Top-level BatchOptimized:
        let top_level_batch_search = PkSk {
            pk: "P".into(),
            sk: "S#123#N#456".into(),
        };
        assert_eq!(
            child_query_prefix::<TopLevelBatch>(&top_level_batch_search),
            PkSk {
                pk: "S#123#N#456".into(),
                sk: "TOPLBATCH#".into()
            }
        );
    }
}
