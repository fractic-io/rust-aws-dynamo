use std::borrow::Cow;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{identifiers::RawIdPath, ForeignRef, PkSk};

impl<'a> ForeignRef<'a> {
    /// Returns the raw internal reference string.
    pub fn raw(&self) -> &str {
        &self.0
    }

    /// Severs the reference from the original `PkSk` and returns an owned
    /// `ForeignRef<'static>` object.
    pub fn into_owned(self) -> ForeignRef<'static> {
        ForeignRef(Cow::Owned(self.0.into_owned()))
    }

    /// Builds a `PkSk` from this reference string using a caller-provided
    /// reconstruction function.
    ///
    /// The closure should interpret `ref_str` appropriately for the referenced
    /// type:
    /// - Singleton: `ref_str == ""`
    /// - IndexedSingleton: `ref_str` is the index key
    /// - Other: `ref_str` is the last segment after the final `#`
    pub fn deref<F>(&self, build: F) -> PkSk
    where
        F: FnOnce(&str) -> PkSk,
    {
        build(&self.0)
    }

    /// Like `deref(...)`, but consumes `self`.
    pub fn into_deref<F>(self, build: F) -> PkSk
    where
        F: FnOnce(Cow<'a, str>) -> PkSk,
    {
        build(self.0)
    }
}

impl<'a> From<&'a PkSk> for ForeignRef<'a> {
    fn from(id: &'a PkSk) -> Self {
        // Borrowed.
        ForeignRef(Cow::Borrowed(extract_ref_from_sk(&id.sk)))
    }
}

impl From<PkSk> for ForeignRef<'static> {
    fn from(id: PkSk) -> Self {
        // Owned.
        let extracted = extract_ref_from_sk(&id.sk);
        ForeignRef(Cow::Owned(extracted.to_string()))
    }
}

impl Serialize for ForeignRef<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ForeignRef<'static> {
    fn deserialize<D>(deserializer: D) -> Result<ForeignRef<'static>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        let normalized = normalize_ref(&raw);
        if normalized.len() == raw.len() {
            Ok(ForeignRef(Cow::Owned(raw)))
        } else {
            Ok(ForeignRef(Cow::Owned(normalized.to_owned())))
        }
    }
}

// Extraction logic.
// ---------------------------------------------------------------------------

/// For backwards compatibility, normalizes the reference string to the expected
/// minimal representation in case it was previously stored as a full PkSk, or
/// the full Sk string.
///
/// Values retain backwards-compatible detection:
/// 1) if they contain `|` => treat them as `pk|sk`
/// 2) if they contain `#` => treat them as a non-singleton `sk`
/// 3) if they start with `@` => treat them as a singleton `sk`
/// 4) otherwise => treat them as an already-minimal reference
fn normalize_ref(raw: &str) -> &str {
    match raw.split_once('|') {
        Some((_, sk)) => RawIdPath::new(sk).foreign_ref_value(),
        None if raw.contains('#') || raw.starts_with('@') => {
            RawIdPath::new(raw).foreign_ref_value()
        }
        None => raw,
    }
}

/// Extracts the minimal reference string from a full Dynamo sk.
fn extract_ref_from_sk(sk: &str) -> &str {
    RawIdPath::new(sk).foreign_ref_value()
}

// Tests.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_extract_ref_from_sk_non_singleton_simple() {
        let sk = "USER#0123456789ABCDEF";
        assert_eq!(extract_ref_from_sk(sk), "0123456789ABCDEF");
    }

    #[test]
    fn test_extract_ref_from_sk_non_singleton_nested() {
        let sk = "PARENT#AAAAAAAAAAAAAAAA#CHILD#BBBBBBBBBBBBBBBB";
        assert_eq!(extract_ref_from_sk(sk), "BBBBBBBBBBBBBBBB");
    }

    #[test]
    fn test_extract_ref_from_sk_singleton_empty() {
        let sk = "@SINGLETON";
        assert_eq!(extract_ref_from_sk(sk), "");
    }

    #[test]
    fn test_extract_ref_from_sk_indexed_singleton_key() {
        let sk = "@FAMILY[key123]";
        assert_eq!(extract_ref_from_sk(sk), "key123");
    }

    #[test]
    fn test_extract_ref_from_sk_inline_singleton() {
        let sk = "ORDER#AAAAAAAAAAAAAAAA@FAMILY[key123]";
        assert_eq!(extract_ref_from_sk(sk), "key123");
    }

    #[test]
    fn test_foreign_ref_from_pksk_and_raw() {
        let id = PkSk {
            pk: "PK".to_string(),
            sk: "USER#0123456789ABCDEF".to_string(),
        };
        let r = ForeignRef::from(&id);
        assert_eq!(r.raw(), "0123456789ABCDEF");
    }

    #[test]
    fn test_foreign_ref_from_indexed_singleton_with_at_sign_roundtrips() {
        let id = PkSk {
            pk: "ROOT".to_string(),
            sk: "@ACCOUNT[user@example.com]".to_string(),
        };
        let reference = ForeignRef::from(&id);
        assert_eq!(reference.raw(), "user@example.com");

        let serialized = serde_json::to_string(&reference).unwrap();
        assert_eq!(serialized, r#""user@example.com""#);
        let back: ForeignRef<'static> = serde_json::from_str(&serialized).unwrap();
        assert_eq!(back.raw(), reference.raw());
    }

    #[test]
    fn test_foreign_ref_serde_roundtrip_minimal() {
        let r = ForeignRef(Cow::Owned("0123456789ABCDEF".to_string()));
        let s = serde_json::to_string(&r).unwrap();
        assert_eq!(s, r#""0123456789ABCDEF""#);
        let back: ForeignRef<'static> = serde_json::from_str(&s).unwrap();
        assert_eq!(back.raw(), "0123456789ABCDEF");
    }

    #[test]
    fn test_foreign_ref_serde_roundtrip_empty() {
        let r = ForeignRef(Cow::Owned(String::new()));
        let s = serde_json::to_string(&r).unwrap();
        assert_eq!(s, r#""""#);
        let back: ForeignRef<'static> = serde_json::from_str(&s).unwrap();
        assert_eq!(back.raw(), "");
    }

    #[test]
    fn test_foreign_ref_serde_roundtrip_minimal_with_at_sign() {
        let raw = "user@example.com";
        let r = ForeignRef(Cow::Owned(raw.to_string()));
        let serialized = serde_json::to_string(&r).unwrap();
        assert_eq!(serialized, format!("\"{raw}\""));
        let back: ForeignRef<'static> = serde_json::from_str(&serialized).unwrap();
        assert_eq!(back.raw(), raw);
    }

    #[test]
    fn test_foreign_ref_deserialize_unescaped_minimal_with_internal_at_sign() {
        let r: ForeignRef<'static> = serde_json::from_str(r#""user@example.com""#).unwrap();
        assert_eq!(r.raw(), "user@example.com");
    }

    #[test]
    fn test_foreign_ref_deserialize_backcompat_from_full_pksk_string() {
        let legacy = r#""USER#AAAAAAAAAAAAAAAA|ORDER#BBBBBBBBBBBBBBBB""#;
        let r: ForeignRef<'static> = serde_json::from_str(legacy).unwrap();
        assert_eq!(r.raw(), "BBBBBBBBBBBBBBBB");
    }

    #[test]
    fn test_foreign_ref_deserialize_backcompat_from_full_sk_string() {
        let legacy = r#""ORDER#AAAAAAAAAAAAAAAA#ITEM#BBBBBBBBBBBBBBBB""#;
        let r: ForeignRef<'static> = serde_json::from_str(legacy).unwrap();
        assert_eq!(r.raw(), "BBBBBBBBBBBBBBBB");
    }

    #[test]
    fn test_foreign_ref_deserialize_backcompat_indexed_singleton_full_sk() {
        let legacy = r#""@FAMILY[user@example.com]""#;
        let r: ForeignRef<'static> = serde_json::from_str(legacy).unwrap();
        assert_eq!(r.raw(), "user@example.com");
    }

    #[test]
    fn test_foreign_ref_deserialize_backcompat_singleton_full_sk() {
        let legacy = r#""@SINGLETON""#;
        let r: ForeignRef<'static> = serde_json::from_str(legacy).unwrap();
        assert_eq!(r.raw(), "");
    }
}
