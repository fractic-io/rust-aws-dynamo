use super::True;

impl std::fmt::Display for True {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        serde::Serialize::serialize(self, f)
    }
}
