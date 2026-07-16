use crate::schema::{DynamoObject, NestingLogic, PkSk};

use super::id_path::RawIdPath;

pub(crate) const ROOT_KEY: &str = "ROOT";

/// The three ways an object's terminal segment can be placed in DynamoDB.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IdPlacement {
    Root,
    TopLevel,
    Inline,
}

/// Places an object's ID-path segment using `T`'s declared nesting logic.
pub(crate) fn place_id_for<T: DynamoObject>(parent: &PkSk, path: &str) -> PkSk {
    place_id_path(parent, path, placement_for(T::nesting_logic()))
}

/// Places a terminal segment or relative ID path according to the requested
/// storage relationship.
pub(crate) fn place_id_path(parent: &PkSk, path: &str, placement: IdPlacement) -> PkSk {
    let logical_path = RawIdPath::new(path).logical_path();
    match placement {
        IdPlacement::Root => PkSk {
            pk: ROOT_KEY.to_string(),
            sk: logical_path.to_string(),
        },
        IdPlacement::TopLevel => PkSk {
            pk: parent.sk.clone(),
            sk: logical_path.to_string(),
        },
        IdPlacement::Inline => PkSk {
            pk: parent.pk.clone(),
            sk: if logical_path.starts_with(['#', '@']) {
                format!("{}{logical_path}", parent.sk)
            } else {
                format!("{}#{logical_path}", parent.sk)
            },
        },
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
            place_id_path(&parent, "CHILD#2", IdPlacement::Root),
            PkSk {
                pk: "ROOT".into(),
                sk: "CHILD#2".into(),
            }
        );
        assert_eq!(
            place_id_path(&parent, "CHILD#2", IdPlacement::TopLevel),
            PkSk {
                pk: "PARENT#1".into(),
                sk: "CHILD#2".into(),
            }
        );
        assert_eq!(
            place_id_path(&parent, "CHILD#2", IdPlacement::Inline),
            PkSk {
                pk: "PARENT_PARTITION".into(),
                sk: "PARENT#1#CHILD#2".into(),
            }
        );
        assert_eq!(
            place_id_path(&parent, "@SETTINGS", IdPlacement::Inline).sk,
            "PARENT#1@SETTINGS"
        );
        assert_eq!(
            place_id_path(&parent, "#CHILD#2", IdPlacement::Inline).sk,
            "PARENT#1#CHILD#2"
        );
    }
}
