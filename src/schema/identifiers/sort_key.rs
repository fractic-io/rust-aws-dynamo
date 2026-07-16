use fractic_server_error::ServerError;

use crate::errors::DynamoInvalidId;

/// A borrowed DynamoDB sort key.
///
/// This is the tolerant/raw entry point: ext-partition inspection and legacy
/// foreign-reference extraction do not require a valid object ID. Call
/// [`SortKey::parse`] before performing strict identifier operations.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SortKey<'a> {
    raw: &'a str,
}

impl<'a> SortKey<'a> {
    pub(crate) fn new(raw: &'a str) -> Self {
        Self { raw }
    }

    /// Parses and validates the terminal object represented by this sort key.
    pub(crate) fn parse(self) -> Result<ParsedSortKey<'a>, ServerError> {
        ParsedSortKey::parse(self.logical())
    }

    /// Returns the logical object sort key, without a physical ext-partition
    /// suffix such as `+0` or `+12`.
    pub(crate) fn logical(self) -> &'a str {
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
    /// compatibility. Strict callers should use [`ParsedSortKey::terminal_kind`].
    pub(crate) fn is_singleton(self) -> bool {
        self.logical().contains('@')
    }

    /// Returns the label of the terminal object represented by this sort key.
    pub(crate) fn object_label(self) -> Result<&'a str, ServerError> {
        Ok(self.parse()?.object_label())
    }

    /// Returns the terminal object's self-contained sort-key component.
    pub(crate) fn terminal_component(self, expected_label: &str) -> Result<&'a str, ServerError> {
        let parsed = self.parse()?;
        if parsed.object_label() != expected_label {
            return Err(DynamoInvalidId::with_debug(
                "sort-key label did not match the expected object label",
                &parsed.logical(),
            ));
        }
        Ok(parsed.terminal_component())
    }

    /// Returns the minimal value stored by [`crate::schema::ForeignRef`].
    ///
    /// This intentionally remains tolerant of legacy malformed values because
    /// foreign references have historically been normalized during reads.
    pub(crate) fn foreign_ref_value(self) -> &'a str {
        let sk = self.logical();
        if let Some(singleton_start) = sk.find('@') {
            let singleton = &sk[singleton_start + 1..];
            if let (Some(open), Some(close)) = (singleton.find('['), singleton.rfind(']')) {
                if open < close {
                    return &singleton[open + 1..close];
                }
            }
            return "";
        }
        sk.rsplit_once('#').map_or(sk, |(_, value)| value)
    }

    /// Returns this child's logical sort-key path relative to its parent.
    pub(crate) fn relative_to(self, parent: SortKey<'_>) -> Result<&'a str, ServerError> {
        let child = self.parse()?;
        let parent = parent.parse()?;
        child
            .logical()
            .strip_prefix(parent.logical())
            .filter(|relative| relative.starts_with('#') || relative.starts_with('@'))
            .ok_or_else(|| {
                DynamoInvalidId::with_debug(
                    "child sort key did not start with its parent sort key",
                    &self.raw,
                )
            })
    }
}

/// A sort key whose logical terminal object has been parsed and validated.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ParsedSortKey<'a> {
    logical: &'a str,
    terminal: ParsedTerminal<'a>,
}

impl<'a> ParsedSortKey<'a> {
    fn parse(logical: &'a str) -> Result<Self, ServerError> {
        let terminal = if let Some(singleton_start) = logical.find('@') {
            parse_singleton_terminal(logical, singleton_start)?
        } else {
            parse_regular_terminal(logical)?
        };
        Ok(Self { logical, terminal })
    }

    pub(crate) fn logical(self) -> &'a str {
        self.logical
    }

    pub(crate) fn object_label(self) -> &'a str {
        match self.terminal {
            ParsedTerminal::Regular { label, .. }
            | ParsedTerminal::Singleton { label, .. }
            | ParsedTerminal::IndexedSingleton { label, .. } => label,
        }
    }

    pub(crate) fn terminal_kind(self) -> TerminalKind {
        match self.terminal {
            ParsedTerminal::Regular { .. } => TerminalKind::Regular,
            ParsedTerminal::Singleton { .. } => TerminalKind::Singleton,
            ParsedTerminal::IndexedSingleton { .. } => TerminalKind::IndexedSingleton,
        }
    }

    pub(crate) fn terminal_component(self) -> &'a str {
        match self.terminal {
            ParsedTerminal::Regular { component, .. }
            | ParsedTerminal::Singleton { component, .. }
            | ParsedTerminal::IndexedSingleton { component, .. } => component,
        }
    }

    pub(super) fn with_terminal_value(self, value: &str) -> String {
        match self.terminal {
            ParsedTerminal::Regular { prefix, .. } => format!("{prefix}#{value}"),
            ParsedTerminal::Singleton { .. } | ParsedTerminal::IndexedSingleton { .. } => {
                self.logical.to_string()
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TerminalKind {
    Regular,
    Singleton,
    IndexedSingleton,
}

#[derive(Clone, Copy, Debug)]
enum ParsedTerminal<'a> {
    Regular {
        label: &'a str,
        prefix: &'a str,
        component: &'a str,
    },
    Singleton {
        label: &'a str,
        component: &'a str,
    },
    IndexedSingleton {
        label: &'a str,
        component: &'a str,
    },
}

fn parse_regular_terminal(sk: &str) -> Result<ParsedTerminal<'_>, ServerError> {
    let Some((prefix, value)) = sk.rsplit_once('#') else {
        return Err(invalid_sort_key("expected `LABEL#value`", sk));
    };
    if value.is_empty() {
        return Err(invalid_sort_key("terminal ID value was empty", sk));
    }

    let label = prefix.rsplit_once('#').map_or(prefix, |(_, label)| label);
    validate_label(label, sk)?;
    let component = prefix
        .rsplit_once('#')
        .map_or(sk, |(ancestor, _)| &sk[ancestor.len() + 1..]);
    Ok(ParsedTerminal::Regular {
        label,
        prefix,
        component,
    })
}

fn parse_singleton_terminal(
    sk: &str,
    singleton_start: usize,
) -> Result<ParsedTerminal<'_>, ServerError> {
    let component = &sk[singleton_start..];
    let singleton = &sk[singleton_start + 1..];
    if let Some(open) = singleton.find('[') {
        if !singleton.ends_with(']') {
            return Err(invalid_sort_key(
                "indexed singleton was missing its closing `]`",
                sk,
            ));
        }
        let label = &singleton[..open];
        validate_label(label, sk)?;
        Ok(ParsedTerminal::IndexedSingleton { label, component })
    } else {
        if singleton.contains(']') {
            return Err(invalid_sort_key("singleton contained an unmatched `]`", sk));
        }
        validate_label(singleton, sk)?;
        Ok(ParsedTerminal::Singleton {
            label: singleton,
            component,
        })
    }
}

fn validate_label(label: &str, full_sk: &str) -> Result<(), ServerError> {
    if label.is_empty() {
        return Err(invalid_sort_key("object label was empty", full_sk));
    }
    if label.contains(['#', '@', '[', ']', '+']) {
        return Err(invalid_sort_key(
            "object label contained a reserved ID character",
            full_sk,
        ));
    }
    Ok(())
}

fn invalid_sort_key(message: &str, sk: &str) -> ServerError {
    DynamoInvalidId::with_debug(message, &sk)
}

fn split_ext_suffix(sk: &str) -> (&str, Option<usize>) {
    let Some(digits_start) = sk
        .char_indices()
        .rev()
        .take_while(|(_, character)| character.is_ascii_digit())
        .last()
        .map(|(index, _)| index)
    else {
        return (sk, None);
    };
    if digits_start == 0 || sk.as_bytes().get(digits_start - 1) != Some(&b'+') {
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
    fn parses_regular_and_singleton_terminals() {
        let regular = SortKey::new("PARENT#1#CHILD#2").parse().unwrap();
        assert_eq!(regular.object_label(), "CHILD");
        assert_eq!(regular.terminal_kind(), TerminalKind::Regular);
        assert_eq!(regular.terminal_component(), "CHILD#2");

        let singleton = SortKey::new("PARENT#1@SETTINGS").parse().unwrap();
        assert_eq!(singleton.object_label(), "SETTINGS");
        assert_eq!(singleton.terminal_kind(), TerminalKind::Singleton);
        assert_eq!(singleton.terminal_component(), "@SETTINGS");

        let indexed = SortKey::new("PARENT#1@SETTINGS[user@example.com]")
            .parse()
            .unwrap();
        assert_eq!(indexed.object_label(), "SETTINGS");
        assert_eq!(indexed.terminal_kind(), TerminalKind::IndexedSingleton);
        assert_eq!(indexed.terminal_component(), "@SETTINGS[user@example.com]");
    }

    #[test]
    fn rejects_malformed_terminals() {
        for malformed in ["INVALID", "LABEL#", "@", "@FAMILY[key", "@FAMILY]"] {
            assert!(SortKey::new(malformed).parse().is_err(), "{malformed}");
        }
    }

    #[test]
    fn handles_ext_partition_suffixes_consistently() {
        assert_eq!(SortKey::new("@SINGLETON").logical(), "@SINGLETON");
        assert_eq!(SortKey::new("@SINGLETON+0").logical(), "@SINGLETON");
        assert_eq!(
            SortKey::new("PARENT#1@FAMILY[key]+12").ext_partition_index(),
            Some(12)
        );

        let overflowing = format!("@SINGLETON+{}0", usize::MAX);
        assert_eq!(SortKey::new(&overflowing).logical(), overflowing);
        assert_eq!(SortKey::new(&overflowing).ext_partition_index(), None);
    }

    #[test]
    fn tolerant_foreign_reference_extraction_never_panics() {
        assert_eq!(
            SortKey::new("PARENT#1@FAMILY[user@example.com]").foreign_ref_value(),
            "user@example.com"
        );
        assert_eq!(SortKey::new("@FAMILY]broken[").foreign_ref_value(), "");
        assert_eq!(SortKey::new("PARENT#1#CHILD#2").foreign_ref_value(), "2");
    }

    #[test]
    fn calculates_relative_paths() {
        assert_eq!(
            SortKey::new("PARENT#1#CHILD#2")
                .relative_to(SortKey::new("PARENT#1"))
                .unwrap(),
            "#CHILD#2"
        );
        assert!(SortKey::new("PARENT#10#CHILD#2")
            .relative_to(SortKey::new("PARENT#1"))
            .is_err());
    }
}
