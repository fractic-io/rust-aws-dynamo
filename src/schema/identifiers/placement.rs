use crate::schema::{DynamoObject, NestingLogic, PkSk};

use super::sort_key::SortKey;

pub(crate) const ROOT_KEY: &str = "ROOT";

/// The three ways an object's terminal sort key can be placed in DynamoDB.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IdPlacement {
    Root,
    TopLevel,
    Inline,
}

/// Places an object's sort-key component using `T`'s declared nesting logic.
pub(crate) fn place_for<T: DynamoObject>(parent: &PkSk, object_sk: &str) -> PkSk {
    place_object(parent, object_sk, placement_for(T::nesting_logic()))
}

/// Places a logical terminal or relative sort key according to the requested
/// storage relationship.
pub(crate) fn place_object(parent: &PkSk, object_sk: &str, placement: IdPlacement) -> PkSk {
    let object_sk = SortKey::new(object_sk).logical();
    match placement {
        IdPlacement::Root => PkSk {
            pk: ROOT_KEY.to_string(),
            sk: object_sk.to_string(),
        },
        IdPlacement::TopLevel => PkSk {
            pk: parent.sk.clone(),
            sk: object_sk.to_string(),
        },
        IdPlacement::Inline => PkSk {
            pk: parent.pk.clone(),
            sk: join_inline_child(&parent.sk, object_sk),
        },
    }
}

fn join_inline_child(parent_sk: &str, child_component: &str) -> String {
    if child_component.starts_with('#') || child_component.starts_with('@') {
        format!("{parent_sk}{child_component}")
    } else {
        format!("{parent_sk}#{child_component}")
    }
}

fn placement_for(nesting: NestingLogic) -> IdPlacement {
    match nesting {
        NestingLogic::Root => IdPlacement::Root,
        NestingLogic::TopLevelChildOf(_) | NestingLogic::TopLevelChildOfAny => {
            IdPlacement::TopLevel
        }
        NestingLogic::InlineChildOf(_) | NestingLogic::InlineChildOfAny => IdPlacement::Inline,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn places_root_top_level_and_inline_components() {
        let parent = PkSk {
            pk: "PARENT_PARTITION".into(),
            sk: "PARENT#1".into(),
        };
        assert_eq!(
            place_object(&parent, "CHILD#2", IdPlacement::Root),
            PkSk {
                pk: "ROOT".into(),
                sk: "CHILD#2".into(),
            }
        );
        assert_eq!(
            place_object(&parent, "CHILD#2", IdPlacement::TopLevel),
            PkSk {
                pk: "PARENT#1".into(),
                sk: "CHILD#2".into(),
            }
        );
        assert_eq!(
            place_object(&parent, "CHILD#2", IdPlacement::Inline),
            PkSk {
                pk: "PARENT_PARTITION".into(),
                sk: "PARENT#1#CHILD#2".into(),
            }
        );
        assert_eq!(
            place_object(&parent, "@SETTINGS", IdPlacement::Inline).sk,
            "PARENT#1@SETTINGS"
        );
        assert_eq!(
            place_object(&parent, "#CHILD#2", IdPlacement::Inline).sk,
            "PARENT#1#CHILD#2"
        );
    }
}
