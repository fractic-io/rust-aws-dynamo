use fractic_server_error::ServerError;

use crate::errors::DynamoInvalidId;

/// A borrowed, potentially unvalidated object ID path.
///
/// This is the tolerant/raw entry point: ext-partition inspection and legacy
/// foreign-reference extraction do not require a valid object ID. Call
/// [`RawIdPath::parse`] before performing strict identifier operations.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RawIdPath<'a> {
    raw: &'a str,
}

impl<'a> RawIdPath<'a> {
    pub(crate) fn new(raw: &'a str) -> Self {
        Self { raw }
    }

    /// Parses and validates the final object represented by this ID path.
    pub(crate) fn parse(self) -> Result<ParsedIdPath<'a>, ServerError> {
        ParsedIdPath::parse(self.logical_path())
    }

    /// Returns the logical object sort key, without a physical ext-partition
    /// suffix such as `+0` or `+12`.
    pub(crate) fn logical_path(self) -> &'a str {
        split_ext_suffix(self.raw).0
    }

    /// Returns the physical ext-partition index, when the key ends in a valid
    /// `+N` suffix representable as a `usize`.
    pub(crate) fn ext_partition_index(self) -> Option<usize> {
        split_ext_suffix(self.raw).1
    }

    /// Whether the raw key contains singleton syntax.
    ///
    /// This intentionally remains infallible for parent rejection and legacy
    /// compatibility. Strict callers should use
    /// [`ParsedIdPath::terminal_segment_kind`].
    pub(crate) fn is_singleton(self) -> bool {
        self.logical_path().contains('@')
    }

    /// Returns the label of the final object represented by this ID path.
    pub(crate) fn object_label(self) -> Result<&'a str, ServerError> {
        self.parse().map(ParsedIdPath::object_label)
    }

    /// Returns the final self-contained segment for the path's object.
    pub(crate) fn terminal_segment(self, expected_label: &str) -> Result<&'a str, ServerError> {
        let parsed = self.parse()?;
        if parsed.object_label() != expected_label {
            return Err(DynamoInvalidId::with_debug(
                "ID-path label did not match the expected object label",
                &parsed.logical_path(),
            ));
        }
        Ok(parsed.terminal_segment())
    }

    /// Returns the minimal value stored by [`crate::schema::ForeignRef`].
    ///
    /// This intentionally remains tolerant of legacy malformed values because
    /// foreign references have historically been normalized during reads.
    pub(crate) fn foreign_ref_value(self) -> &'a str {
        let sk = self.logical_path();
        if let Some(singleton_start) = sk.find('@') {
            let singleton = &sk[singleton_start + 1..];
            return match (singleton.find('['), singleton.rfind(']')) {
                (Some(open), Some(close)) if open < close => &singleton[open + 1..close],
                _ => "",
            };
        }
        sk.rsplit_once('#').map_or(sk, |(_, value)| value)
    }

    /// Returns this child's logical ID path relative to its parent.
    pub(crate) fn relative_to(self, parent: RawIdPath<'_>) -> Result<&'a str, ServerError> {
        let child = self.parse()?;
        let parent = parent.parse()?;
        child
            .logical_path()
            .strip_prefix(parent.logical_path())
            .filter(|relative| relative.starts_with('#') || relative.starts_with('@'))
            .ok_or_else(|| {
                DynamoInvalidId::with_debug(
                    "child ID path did not start with its parent ID path",
                    &self.raw,
                )
            })
    }
}

/// An object ID path whose logical form has been parsed and validated.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ParsedIdPath<'a> {
    logical_path: &'a str,
    terminal_segment: TerminalSegment<'a>,
}

impl<'a> ParsedIdPath<'a> {
    fn parse(logical_path: &'a str) -> Result<Self, ServerError> {
        let terminal_segment = if let Some(singleton_start) = logical_path.find('@') {
            parse_singleton_segment(logical_path, singleton_start)?
        } else {
            parse_regular_segment(logical_path)?
        };
        Ok(Self {
            logical_path,
            terminal_segment,
        })
    }

    pub(crate) fn logical_path(self) -> &'a str {
        self.logical_path
    }

    pub(crate) fn object_label(self) -> &'a str {
        match self.terminal_segment {
            TerminalSegment::Regular { label, .. }
            | TerminalSegment::Singleton { label, .. }
            | TerminalSegment::IndexedSingleton { label, .. } => label,
        }
    }

    pub(crate) fn terminal_segment_kind(self) -> TerminalSegmentKind {
        match self.terminal_segment {
            TerminalSegment::Regular { .. } => TerminalSegmentKind::Regular,
            TerminalSegment::Singleton { .. } => TerminalSegmentKind::Singleton,
            TerminalSegment::IndexedSingleton { .. } => TerminalSegmentKind::IndexedSingleton,
        }
    }

    pub(crate) fn terminal_segment(self) -> &'a str {
        match self.terminal_segment {
            TerminalSegment::Regular { segment, .. }
            | TerminalSegment::Singleton { segment, .. }
            | TerminalSegment::IndexedSingleton { segment, .. } => segment,
        }
    }

    pub(super) fn with_terminal_segment_value(self, value: &str) -> String {
        match self.terminal_segment {
            TerminalSegment::Regular { prefix, .. } => format!("{prefix}#{value}"),
            TerminalSegment::Singleton { .. } | TerminalSegment::IndexedSingleton { .. } => {
                self.logical_path.to_string()
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TerminalSegmentKind {
    Regular,
    Singleton,
    IndexedSingleton,
}

#[derive(Clone, Copy, Debug)]
enum TerminalSegment<'a> {
    Regular {
        label: &'a str,
        prefix: &'a str,
        segment: &'a str,
    },
    Singleton {
        label: &'a str,
        segment: &'a str,
    },
    IndexedSingleton {
        label: &'a str,
        segment: &'a str,
    },
}

fn parse_regular_segment(sk: &str) -> Result<TerminalSegment<'_>, ServerError> {
    let Some((prefix, value)) = sk.rsplit_once('#') else {
        return Err(invalid_id_path("expected `LABEL#value`", sk));
    };
    if value.is_empty() {
        return Err(invalid_id_path("terminal segment value was empty", sk));
    }

    let (label, segment) = prefix
        .rsplit_once('#')
        .map_or((prefix, sk), |(ancestor, label)| {
            (label, &sk[ancestor.len() + 1..])
        });
    validate_label(label, sk)?;
    Ok(TerminalSegment::Regular {
        label,
        prefix,
        segment,
    })
}

fn parse_singleton_segment(
    sk: &str,
    singleton_start: usize,
) -> Result<TerminalSegment<'_>, ServerError> {
    let segment = &sk[singleton_start..];
    let singleton = &sk[singleton_start + 1..];
    if let Some(open) = singleton.find('[') {
        if !singleton.ends_with(']') {
            return Err(invalid_id_path(
                "indexed singleton was missing its closing `]`",
                sk,
            ));
        }
        let label = &singleton[..open];
        validate_label(label, sk)?;
        Ok(TerminalSegment::IndexedSingleton { label, segment })
    } else {
        if singleton.contains(']') {
            return Err(invalid_id_path("singleton contained an unmatched `]`", sk));
        }
        validate_label(singleton, sk)?;
        Ok(TerminalSegment::Singleton {
            label: singleton,
            segment,
        })
    }
}

fn validate_label(label: &str, full_sk: &str) -> Result<(), ServerError> {
    if label.is_empty() {
        return Err(invalid_id_path("object label was empty", full_sk));
    }
    if label.contains(['#', '@', '[', ']', '+']) {
        return Err(invalid_id_path(
            "object label contained a reserved ID character",
            full_sk,
        ));
    }
    Ok(())
}

fn invalid_id_path(message: &str, sk: &str) -> ServerError {
    DynamoInvalidId::with_debug(message, &sk)
}

fn split_ext_suffix(sk: &str) -> (&str, Option<usize>) {
    let bytes = sk.as_bytes();
    let digits_start = bytes
        .iter()
        .rposition(|byte| !byte.is_ascii_digit())
        .map_or(0, |index| index + 1);
    if digits_start == bytes.len() || digits_start == 0 || bytes[digits_start - 1] != b'+' {
        return (sk, None);
    }
    let Ok(index) = sk[digits_start..].parse::<usize>() else {
        return (sk, None);
    };
    (&sk[..digits_start - 1], Some(index))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_regular_and_singleton_segments() {
        let regular = RawIdPath::new("PARENT#1#CHILD#2").parse().unwrap();
        assert_eq!(regular.object_label(), "CHILD");
        assert_eq!(
            regular.terminal_segment_kind(),
            TerminalSegmentKind::Regular
        );
        assert_eq!(regular.terminal_segment(), "CHILD#2");

        let singleton = RawIdPath::new("PARENT#1@SETTINGS").parse().unwrap();
        assert_eq!(singleton.object_label(), "SETTINGS");
        assert_eq!(
            singleton.terminal_segment_kind(),
            TerminalSegmentKind::Singleton
        );
        assert_eq!(singleton.terminal_segment(), "@SETTINGS");

        let indexed = RawIdPath::new("PARENT#1@SETTINGS[user@example.com]")
            .parse()
            .unwrap();
        assert_eq!(indexed.object_label(), "SETTINGS");
        assert_eq!(
            indexed.terminal_segment_kind(),
            TerminalSegmentKind::IndexedSingleton
        );
        assert_eq!(indexed.terminal_segment(), "@SETTINGS[user@example.com]");
    }

    #[test]
    fn rejects_malformed_segments() {
        for malformed in ["INVALID", "LABEL#", "@", "@FAMILY[key", "@FAMILY]"] {
            assert!(RawIdPath::new(malformed).parse().is_err(), "{malformed}");
        }
    }

    #[test]
    fn handles_ext_partition_suffixes_consistently() {
        assert_eq!(RawIdPath::new("@SINGLETON").logical_path(), "@SINGLETON");
        assert_eq!(RawIdPath::new("@SINGLETON+0").logical_path(), "@SINGLETON");
        assert_eq!(
            RawIdPath::new("PARENT#1@FAMILY[key]+12").ext_partition_index(),
            Some(12)
        );

        let overflowing = format!("@SINGLETON+{}0", usize::MAX);
        assert_eq!(RawIdPath::new(&overflowing).logical_path(), overflowing);
        assert_eq!(RawIdPath::new(&overflowing).ext_partition_index(), None);
    }

    #[test]
    fn tolerant_foreign_reference_extraction_never_panics() {
        assert_eq!(
            RawIdPath::new("PARENT#1@FAMILY[user@example.com]").foreign_ref_value(),
            "user@example.com"
        );
        assert_eq!(RawIdPath::new("@FAMILY]broken[").foreign_ref_value(), "");
        assert_eq!(RawIdPath::new("PARENT#1#CHILD#2").foreign_ref_value(), "2");
    }

    #[test]
    fn calculates_relative_paths() {
        assert_eq!(
            RawIdPath::new("PARENT#1#CHILD#2")
                .relative_to(RawIdPath::new("PARENT#1"))
                .unwrap(),
            "#CHILD#2"
        );
        assert!(RawIdPath::new("PARENT#10#CHILD#2")
            .relative_to(RawIdPath::new("PARENT#1"))
            .is_err());
    }
}
