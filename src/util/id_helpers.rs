use fractic_server_error::ServerError;

use crate::{
    errors::DynamoInvalidOperation,
    schema::{DynamoObject, IdLogic, NestingLogic, PkSk},
};

#[track_caller]
pub fn validate_id<T: DynamoObject>(id: &PkSk) -> Result<(), ServerError> {
    if id.object_type()? != T::id_label() {
        return Err(DynamoInvalidOperation::new(&format!(
            "ID does not match object type; expected object type '{}', got ID '{}'",
            T::id_label(),
            id
        )));
    }
    Ok(())
}

#[track_caller]
pub fn validate_parent_id<T: DynamoObject>(parent_id: &PkSk) -> Result<(), ServerError> {
    match T::nesting_logic() {
        NestingLogic::Root if parent_id != PkSk::root() => {
            return Err(DynamoInvalidOperation::new(
                "parent ID does not match root, as expected for NestingLogic::Root",
            ));
        }
        NestingLogic::TopLevelChildOf(ptype_req) if parent_id.object_type()? != ptype_req => {
            return Err(DynamoInvalidOperation::new(&format!(
                "parent ID does not match required object type '{}', got '{}'",
                ptype_req,
                parent_id.object_type()?
            )));
        }
        NestingLogic::InlineChildOf(ptype_req) if parent_id.object_type()? != ptype_req => {
            return Err(DynamoInvalidOperation::new(&format!(
                "parent ID does not match required object type '{}', got '{}'",
                ptype_req,
                parent_id.object_type()?
            )));
        }
        _ => {}
    }
    Ok(())
}

pub fn child_search_prefix<T: DynamoObject>(parent_id: &PkSk) -> PkSk {
    // * Singleton / SingletonFamily →  "@LABEL"
    // * Everything else             →  "LABEL#"
    let sk_search_prefix = match T::id_logic() {
        IdLogic::Singleton | IdLogic::SingletonFamily(_) => {
            format!("@{}", T::id_label())
        }
        _ => format!("{}#", T::id_label()),
    };

    match T::nesting_logic() {
        NestingLogic::Root => PkSk {
            pk: "ROOT".to_string(),
            sk: sk_search_prefix,
        },
        NestingLogic::TopLevelChildOfAny | NestingLogic::TopLevelChildOf(_) => PkSk {
            pk: parent_id.sk.clone(),
            sk: sk_search_prefix,
        },
        NestingLogic::InlineChildOfAny | NestingLogic::InlineChildOf(_) => PkSk {
            pk: parent_id.pk.clone(),
            sk: format!("{}#{}", parent_id.sk, sk_search_prefix),
        },
    }
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
    pub struct InlineSingletonFamData {
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
        InlineSingletonFam,
        InlineSingletonFamData,
        "INLFAM",
        IdLogic::SingletonFamily(Box::new(|data: &InlineSingletonFamData| Cow::Borrowed(
            &data.key
        ))),
        NestingLogic::InlineChildOfAny
    );
    dynamo_object!(
        InlineBatch,
        InlineBatchData,
        "INLBATCH",
        IdLogic::BatchOptimized { chunk_size: 10 },
        NestingLogic::InlineChildOfAny
    );
    dynamo_object!(
        TopLevelBatch,
        TopLevelBatchData,
        "TOPLBATCH",
        IdLogic::BatchOptimized { chunk_size: 10 },
        NestingLogic::TopLevelChildOf("N")
    );

    #[test]
    fn test_validate_parent_id() {
        // Root OK / error --------------------------------------------------------
        assert!(validate_parent_id::<RootGroup>(&PkSk::root()).is_ok());
        assert!(validate_parent_id::<RootGroup>(&PkSk {
            pk: "X".into(),
            sk: "Y".into()
        })
        .is_err());

        // TopLevelChildOf OK / wrong-type ----------------------------------------
        let group_parent = PkSk {
            pk: "ROOT".into(),
            sk: "GROUP#1".into(),
        };
        assert!(validate_parent_id::<GroupEvent>(&group_parent).is_ok());
        let wrong_parent = PkSk {
            pk: "ROOT".into(),
            sk: "OTHER#1".into(),
        };
        assert!(validate_parent_id::<GroupEvent>(&wrong_parent).is_err());

        // InlineChildOf OK / wrong-type ------------------------------------------
        let inl_parent = PkSk {
            pk: "ROOT".into(),
            sk: "GROUP#1".into(),
        };
        assert!(validate_parent_id::<GroupTask>(&inl_parent).is_ok());
        assert!(validate_parent_id::<GroupTask>(&wrong_parent).is_err());

        // “Any” variants never fail ----------------------------------------------
        assert!(validate_parent_id::<InlineChildOfAny>(&wrong_parent).is_ok());
        assert!(validate_parent_id::<TopLevelChildOfAny>(&wrong_parent).is_ok());
    }

    #[test]
    fn test_child_search_prefix() {
        // Root-level child:
        let root_obj_search = child_search_prefix::<RootGroup>(PkSk::root());
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
            child_search_prefix::<GroupEvent>(&top_level_child_search),
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
            child_search_prefix::<GroupTask>(&inline_child_search),
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
            child_search_prefix::<InlineSingleton>(&inline_singleton_search),
            PkSk {
                pk: "P".into(),
                sk: "S#@INLSINGLE".into()
            }
        );

        // Inline SingletonFamily:
        let inline_singleton_family_search = PkSk {
            pk: "P".into(),
            sk: "S".into(),
        };
        assert_eq!(
            child_search_prefix::<InlineSingletonFam>(&inline_singleton_family_search),
            PkSk {
                pk: "P".into(),
                sk: "S#@INLFAM".into()
            }
        );

        // Inline BatchOptimized:
        let inline_batch_search = PkSk {
            pk: "P".into(),
            sk: "S".into(),
        };
        assert_eq!(
            child_search_prefix::<InlineBatch>(&inline_batch_search),
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
            child_search_prefix::<TopLevelBatch>(&top_level_batch_search),
            PkSk {
                pk: "S#123#N#456".into(),
                sk: "TOPLBATCH#".into()
            }
        );
    }
}
