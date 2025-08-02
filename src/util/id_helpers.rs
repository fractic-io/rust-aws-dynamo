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
        NestingLogic::Root if *parent_id != PkSk::root() => {
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

pub fn child_search_prefix<T: DynamoObject>(parent_id: PkSk) -> PkSk {
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
            pk: parent_id.sk,
            sk: sk_search_prefix,
        },
        NestingLogic::InlineChildOfAny | NestingLogic::InlineChildOf(_) => PkSk {
            pk: parent_id.pk,
            sk: format!("{}#{}", parent_id.sk, sk_search_prefix),
        },
    }
}

#[cfg(test)]
mod tests {
    #![allow(unused)]

    use super::*;

    use serde::{Deserialize, Serialize};

    use crate::{
        dynamo_object,
        schema::{IdLogic, NestingLogic, PkSk},
    };

    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    pub struct RootObjUnitData {}
    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    pub struct GroupObjUnitData {}
    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    pub struct TaskObjUnitData {}
    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    pub struct EventObjUnitData {}
    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    pub struct AnyInlineObjUnitData {}
    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    pub struct AnyTopObjUnitData {}
    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    pub struct SingletonObjUnitData {}
    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    pub struct SingletonFamObjUnitData {
        key: String,
    }
    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    pub struct BatchObjUnitData {}

    dynamo_object!(
        RootObj,
        RootObjUnitData,
        "ROOTOBJ",
        IdLogic::Uuid,
        NestingLogic::Root
    );
    dynamo_object!(
        GroupObj,
        GroupObjUnitData,
        "GROUP",
        IdLogic::Uuid,
        NestingLogic::Root
    );
    dynamo_object!(
        TaskObj,
        TaskObjUnitData,
        "TASK",
        IdLogic::Uuid,
        NestingLogic::InlineChildOf("GROUP")
    );
    dynamo_object!(
        EventObj,
        EventObjUnitData,
        "EVENT",
        IdLogic::Uuid,
        NestingLogic::TopLevelChildOf("GROUP")
    );
    dynamo_object!(
        AnyInlineObj,
        AnyInlineObjUnitData,
        "ANYINL",
        IdLogic::Uuid,
        NestingLogic::InlineChildOfAny
    );
    dynamo_object!(
        AnyTopObj,
        AnyTopObjUnitData,
        "ANYTOP",
        IdLogic::Uuid,
        NestingLogic::TopLevelChildOfAny
    );
    dynamo_object!(
        SingletonObj,
        SingletonObjUnitData,
        "SINGLE",
        IdLogic::Singleton,
        NestingLogic::InlineChildOfAny
    );
    dynamo_object!(
        SingletonFamObj,
        SingletonFamObjUnitData,
        "SFAM",
        IdLogic::SingletonFamily(Box::new(|data: &SingletonFamObjUnitData| data.key.clone())),
        NestingLogic::InlineChildOfAny
    );
    dynamo_object!(
        BatchObj,
        BatchObjUnitData,
        "BATCH",
        IdLogic::BatchOptimized { chunk_size: 10 },
        NestingLogic::InlineChildOfAny
    );

    #[test]
    fn test_validate_parent_id() {
        // Root OK / error --------------------------------------------------------
        assert!(validate_parent_id::<RootObj>(&PkSk::root()).is_ok());
        assert!(validate_parent_id::<RootObj>(&PkSk {
            pk: "X".into(),
            sk: "Y".into()
        })
        .is_err());

        // TopLevelChildOf OK / wrong-type ----------------------------------------
        let group_parent = PkSk {
            pk: "ROOT".into(),
            sk: "GROUP#1".into(),
        };
        assert!(validate_parent_id::<EventObj>(&group_parent).is_ok());
        let wrong_parent = PkSk {
            pk: "ROOT".into(),
            sk: "OTHER#1".into(),
        };
        assert!(validate_parent_id::<EventObj>(&wrong_parent).is_err());

        // InlineChildOf OK / wrong-type ------------------------------------------
        let inl_parent = PkSk {
            pk: "ROOT".into(),
            sk: "GROUP#1".into(),
        };
        assert!(validate_parent_id::<TaskObj>(&inl_parent).is_ok());
        assert!(validate_parent_id::<TaskObj>(&wrong_parent).is_err());

        // “Any” variants never fail ----------------------------------------------
        assert!(validate_parent_id::<AnyInlineObj>(&wrong_parent).is_ok());
        assert!(validate_parent_id::<AnyTopObj>(&wrong_parent).is_ok());
    }

    #[test]
    fn test_child_search_prefix() {
        // Root-level child:
        let root_obj_search = child_search_prefix::<RootObj>(PkSk::root());
        assert_eq!(
            root_obj_search,
            PkSk {
                pk: "ROOT".into(),
                sk: "ROOTOBJ#".into()
            }
        );

        // Top-level child:
        let top_level_child_search = PkSk {
            pk: "ROOT".into(),
            sk: "GROUP#1".into(),
        };
        assert_eq!(
            child_search_prefix::<EventObj>(top_level_child_search.clone()),
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
            child_search_prefix::<TaskObj>(inline_child_search.clone()),
            PkSk {
                pk: "ROOT".into(),
                sk: "GROUP#1#TASK#".into()
            }
        );

        // Singleton:
        let singleton_search = PkSk {
            pk: "P".into(),
            sk: "S".into(),
        };
        assert_eq!(
            child_search_prefix::<SingletonObj>(singleton_search.clone()),
            PkSk {
                pk: "P".into(),
                sk: "S#@SINGLE".into()
            }
        );

        // SingletonFamily:
        let singleton_family_search = PkSk {
            pk: "P".into(),
            sk: "S".into(),
        };
        assert_eq!(
            child_search_prefix::<SingletonFamObj>(singleton_family_search.clone()),
            PkSk {
                pk: "P".into(),
                sk: "S#@SFAM".into()
            }
        );

        // BatchOptimized:
        let batch_search = PkSk {
            pk: "P".into(),
            sk: "S".into(),
        };
        assert_eq!(
            child_search_prefix::<BatchObj>(batch_search.clone()),
            PkSk {
                pk: "P".into(),
                sk: "S#BATCH#".into()
            }
        );
    }
}
