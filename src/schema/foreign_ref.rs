use std::borrow::Cow;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{ForeignRef, PkSk};

impl<'a> ForeignRef<'a> {
    /// Returns the raw internal reference string.
    pub fn raw(&self) -> &str {
        &self.0
    }

    /// Severs the reference from the original `PkSk` and returns an owned
    /// `ForeignRef<'static>` object.
    pub fn into_owned(self) -> ForeignRef<'static> {
        ForeignRef(Cow::Owned(self.0.to_string()))
    }

    /// Builds a `PkSk` from this reference string using a caller-provided
    /// reconstruction function.
    ///
    /// The closure should interpret `ref_str` appropriately for the referenced
    /// type:
    /// - Singleton: `ref_str == ""`
    /// - SingletonFamily: `ref_str` is the family key
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
        Ok(ForeignRef(Cow::Owned(normalize_ref(&raw).to_string())))
    }
}

// Extraction logic.
// ---------------------------------------------------------------------------

/// For backwards compatibility, normalizes the reference string to the expected
/// minimal representation in case it was previously stored as a full PkSk, or
/// the full Sk string.
///
/// The detection is intentionally simple for backwards compatibility:
/// 1) if it contains `|` => assume `pk|sk` and extract from the `sk` portion
/// 2) else if it contains `@` or `#` => assume full `sk` and extract from it
/// 3) else => assume it is already the minimal reference
fn normalize_ref(raw: &str) -> &str {
    let mut pipe: Option<usize> = None;
    let mut at: Option<usize> = None;
    let mut last_hash: Option<usize> = None;
    let mut open: Option<usize> = None;
    let mut close: Option<usize> = None;

    // Track extraction markers for "the current sk view". If we see a '|', we
    // reset markers and start tracking after it.
    for (i, b) in raw.bytes().enumerate() {
        match b {
            b'|' if pipe.is_none() => {
                pipe = Some(i);
                at = None;
                last_hash = None;
                open = None;
                close = None;
            }
            b'@' => {
                if at.is_none() {
                    at = Some(i);
                }
            }
            b'#' => {
                last_hash = Some(i);
            }
            b'[' => {
                if at.is_some() && open.is_none() {
                    open = Some(i);
                }
            }
            b']' => {
                if open.is_some() && close.is_none() {
                    close = Some(i);
                }
            }
            _ => {}
        }
    }

    let sk_start = pipe.map(|p| p + 1).unwrap_or(0);

    // If we saw '@' in the sk view, this is a singleton.
    if at.is_some() {
        if let (Some(o), Some(c)) = (open, close) {
            return &raw[o + 1..c];
        }
        return "";
    }

    // Otherwise, if we saw any '#', use the suffix after the last one.
    if let Some(h) = last_hash {
        return &raw[h + 1..];
    }

    // Otherwise, treat as already-minimal ref (or sk with no markers).
    &raw[sk_start..]
}

/// Extracts the minimal reference string from a full Dynamo sk.
///
/// Rules:
/// - If the sk contains '@' anywhere, it is treated as a singleton:
///   - Singleton: empty string ("")
///   - SingletonFamily: the text between '[' and ']' (if present)
/// - Otherwise, returns the text after the final '#'
fn extract_ref_from_sk(sk: &str) -> &str {
    let mut at: Option<usize> = None;
    let mut last_hash: Option<usize> = None;
    let mut open: Option<usize> = None;
    let mut close: Option<usize> = None;

    for (i, b) in sk.bytes().enumerate() {
        match b {
            b'@' => {
                if at.is_none() {
                    at = Some(i);
                }
            }
            b'#' => last_hash = Some(i),
            b'[' => {
                if at.is_some() && open.is_none() {
                    open = Some(i);
                }
            }
            b']' => {
                if open.is_some() && close.is_none() {
                    close = Some(i);
                }
            }
            _ => {}
        }
    }

    if at.is_some() {
        if let (Some(o), Some(c)) = (open, close) {
            return &sk[o + 1..c];
        }
        return "";
    }

    if let Some(h) = last_hash {
        return &sk[h + 1..];
    }

    sk
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
    fn test_extract_ref_from_sk_singleton_family_key() {
        let sk = "@FAMILY[key123]";
        assert_eq!(extract_ref_from_sk(sk), "key123");
    }

    #[test]
    fn test_extract_ref_from_sk_singleton_embedded() {
        let sk = "ORDER#AAAAAAAAAAAAAAAA#@FAMILY[key123]";
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
    fn test_foreign_ref_serde_roundtrip_minimal() {
        let r = ForeignRef(Cow::Owned("0123456789ABCDEF".to_string()));
        let s = serde_json::to_string(&r).unwrap();
        assert_eq!(s, r#""0123456789ABCDEF""#);
        let back: ForeignRef<'static> = serde_json::from_str(&s).unwrap();
        assert_eq!(back.raw(), "0123456789ABCDEF");
    }

    #[test]
    fn test_foreign_ref_serde_roundtrip_empty() {
        let r = ForeignRef(Cow::Owned("".to_string()));
        let s = serde_json::to_string(&r).unwrap();
        assert_eq!(s, r#""""#);
        let back: ForeignRef<'static> = serde_json::from_str(&s).unwrap();
        assert_eq!(back.raw(), "");
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
    fn test_foreign_ref_deserialize_backcompat_singleton_family_full_sk() {
        let legacy = r#""@FAMILY[key123]""#;
        let r: ForeignRef<'static> = serde_json::from_str(legacy).unwrap();
        assert_eq!(r.raw(), "key123");
    }

    #[test]
    fn test_foreign_ref_deserialize_backcompat_singleton_full_sk() {
        let legacy = r#""@SINGLETON""#;
        let r: ForeignRef<'static> = serde_json::from_str(legacy).unwrap();
        assert_eq!(r.raw(), "");
    }
}
