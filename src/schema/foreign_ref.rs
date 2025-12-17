use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{id_calculations::get_pk_sk_from_string, ForeignRef, PkSk};

impl ForeignRef {
    /// Returns the raw internal reference string.
    #[inline]
    pub fn raw(&self) -> &str {
        &self.0
    }

    /// Builds a `PkSk` from this reference string using a caller-provided
    /// reconstruction function.
    ///
    /// The closure should interpret `ref_str` appropriately for the referenced
    /// type:
    /// - Singleton: `ref_str == ""`
    /// - SingletonFamily: `ref_str` is the family key
    /// - Other: `ref_str` is the last segment after the final `#`
    #[inline]
    pub fn deref<F>(self, build: F) -> PkSk
    where
        F: FnOnce(&str) -> PkSk,
    {
        build(&self.0)
    }

    #[inline]
    fn from_sk(sk: &str) -> Self {
        ForeignRef(extract_ref_from_sk(sk).to_string())
    }

    #[inline]
    fn normalize_from_stored_str(raw: &str) -> Self {
        // Backwards compatibility:
        // - raw may be "pk|sk"
        // - raw may be a full sk (ex. "LABEL#id#...") or singleton format (contains '@')
        // - raw may already be the minimal ref string; in that case keep as-is.
        if let Ok((_pk, sk)) = get_pk_sk_from_string(raw) {
            return ForeignRef::from_sk(sk);
        }

        // Heuristic: only treat `raw` as a full sk when it looks like one.
        // This avoids accidentally "normalizing" already-minimal singleton family
        // keys that may contain arbitrary characters (including '#').
        if looks_like_full_sk(raw) {
            return ForeignRef::from_sk(raw);
        }

        ForeignRef(raw.to_string())
    }
}

impl From<&PkSk> for ForeignRef {
    #[inline]
    fn from(id: &PkSk) -> Self {
        ForeignRef::from_sk(&id.sk)
    }
}

impl From<PkSk> for ForeignRef {
    #[inline]
    fn from(id: PkSk) -> Self {
        ForeignRef::from_sk(&id.sk)
    }
}

impl Serialize for ForeignRef {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ForeignRef {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<ForeignRef, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Ok(ForeignRef::normalize_from_stored_str(&raw))
    }
}

// Core extraction logic.
// --------------------------------------------------

/// Extracts the minimal reference string from a full Dynamo sk.
///
/// Rules:
/// - If the sk contains '@' anywhere, it is treated as a singleton:
///   - Singleton: empty string ("")
///   - SingletonFamily: the text between '[' and ']' (if present)
/// - Otherwise, returns the text after the final '#'
#[inline]
fn extract_ref_from_sk(sk: &str) -> &str {
    if let Some(at_pos) = sk.find('@') {
        // Singleton or singleton-family.
        // We only care about a bracketed key if it exists after the '@'.
        let after_at = &sk[at_pos + 1..];
        if let Some(open) = after_at.find('[') {
            let after_open = &after_at[open + 1..];
            if let Some(close) = after_open.find(']') {
                return &after_open[..close];
            }
        }
        ""
    } else if let Some(pos) = sk.rfind('#') {
        &sk[pos + 1..]
    } else {
        sk
    }
}

#[inline]
fn is_label_like(seg: &str) -> bool {
    !seg.is_empty()
        && seg
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
}

#[inline]
fn looks_like_full_sk(s: &str) -> bool {
    // pk|sk is handled earlier.

    // Singleton formats always include '@' and are considered "full sk" only if
    // the '@' looks like our singleton marker (start-of-string or preceded by '#').
    if let Some(at) = s.find('@') {
        if at == 0 || s.as_bytes().get(at.wrapping_sub(1)) == Some(&b'#') {
            return true;
        }
    }

    // Non-singleton full sk is typically LABEL#id (or nested ...#LABEL#id).
    // We conservatively treat as "full sk" only if:
    // - there's at least one '#'
    // - the segment before the last '#' is label-like
    // This avoids normalizing arbitrary strings that merely contain '#'.
    let Some(last_hash) = s.rfind('#') else {
        return false;
    };
    let before_last = &s[..last_hash];
    let Some(prev_hash) = before_last.rfind('#') else {
        // LABEL#id: label is the prefix.
        return is_label_like(before_last);
    };
    let prev_seg = &before_last[prev_hash + 1..];
    is_label_like(prev_seg)
}

// Tests.
// --------------------------------------------------

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
        let r = ForeignRef("0123456789ABCDEF".to_string());
        let s = serde_json::to_string(&r).unwrap();
        assert_eq!(s, r#""0123456789ABCDEF""#);
        let back: ForeignRef = serde_json::from_str(&s).unwrap();
        assert_eq!(back.raw(), "0123456789ABCDEF");
    }

    #[test]
    fn test_foreign_ref_serde_roundtrip_empty() {
        let r = ForeignRef("".to_string());
        let s = serde_json::to_string(&r).unwrap();
        assert_eq!(s, r#""""#);
        let back: ForeignRef = serde_json::from_str(&s).unwrap();
        assert_eq!(back.raw(), "");
    }

    #[test]
    fn test_foreign_ref_deserialize_backcompat_from_full_pksk_string() {
        let legacy = r#""USER#AAAAAAAAAAAAAAAA|ORDER#BBBBBBBBBBBBBBBB""#;
        let r: ForeignRef = serde_json::from_str(legacy).unwrap();
        assert_eq!(r.raw(), "BBBBBBBBBBBBBBBB");
    }

    #[test]
    fn test_foreign_ref_deserialize_backcompat_from_full_sk_string() {
        let legacy = r#""ORDER#AAAAAAAAAAAAAAAA#ITEM#BBBBBBBBBBBBBBBB""#;
        let r: ForeignRef = serde_json::from_str(legacy).unwrap();
        assert_eq!(r.raw(), "BBBBBBBBBBBBBBBB");
    }

    #[test]
    fn test_foreign_ref_deserialize_backcompat_singleton_family_full_sk() {
        let legacy = r#""@FAMILY[key123]""#;
        let r: ForeignRef = serde_json::from_str(legacy).unwrap();
        assert_eq!(r.raw(), "key123");
    }

    #[test]
    fn test_foreign_ref_deserialize_backcompat_singleton_full_sk() {
        let legacy = r#""@SINGLETON""#;
        let r: ForeignRef = serde_json::from_str(legacy).unwrap();
        assert_eq!(r.raw(), "");
    }
}
