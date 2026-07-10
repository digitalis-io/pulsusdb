//! Duration and byte-size parsing per docs/configuration.md §9's unit
//! grammar: durations accept `ms|s|m|h|d|w`; byte sizes accept binary units
//! (`KiB/MiB/GiB/TiB`, base 1024), decimal units (`KB/MB/GB/TB`, base 1000),
//! or a bare integer of bytes. Hand-rolled (no `humantime`/`byte-unit`) — the
//! grammar is a leading unsigned integer plus a unit suffix, trivial to
//! parse and keeps the dependency tree lean. All arithmetic is
//! overflow-checked; these parsers never panic.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A parse error from [`parse_bytes`] or [`parse_duration`]. Never a panic —
/// bad input and overflow both surface here so callers can attach the
/// offending variable/field name.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum UnitError {
    #[error("value is empty")]
    Empty,
    #[error("invalid integer {0:?}: {1}")]
    InvalidInteger(String, String),
    #[error("invalid duration unit {0:?} (expected one of: ms, s, m, h, d, w)")]
    InvalidDurationUnit(String),
    #[error(
        "invalid byte-size unit {0:?} (expected one of: B, KB, MB, GB, TB, KiB, MiB, GiB, TiB, or a bare integer)"
    )]
    InvalidByteUnit(String),
    #[error("value overflowed while converting to base units")]
    Overflow,
}

/// Splits a leading run of ASCII digits from its trailing unit suffix.
/// Returns `(digits, suffix)`; `digits` is never empty on success.
fn split_digits(s: &str) -> Result<(&str, &str), UnitError> {
    if s.is_empty() {
        return Err(UnitError::Empty);
    }
    let digit_end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    if digit_end == 0 {
        return Err(UnitError::InvalidByteUnit(s.to_string()));
    }
    Ok(s.split_at(digit_end))
}

/// Parses a byte-size string (e.g. `16MiB`, `256MB`, `1024`) into a byte
/// count. Never panics: bad suffixes and overflow return [`UnitError`].
pub fn parse_bytes(s: &str) -> Result<u64, UnitError> {
    let s = s.trim();
    let (digits, unit) = split_digits(s)?;
    let value: u64 = digits.parse().map_err(|e: std::num::ParseIntError| {
        UnitError::InvalidInteger(digits.to_string(), e.to_string())
    })?;
    let multiplier: u64 = match unit {
        "" | "B" => 1,
        "KB" => 1_000,
        "MB" => 1_000_000,
        "GB" => 1_000_000_000,
        "TB" => 1_000_000_000_000,
        "KiB" => 1024,
        "MiB" => 1024 * 1024,
        "GiB" => 1024 * 1024 * 1024,
        "TiB" => 1024u64.pow(4),
        other => return Err(UnitError::InvalidByteUnit(other.to_string())),
    };
    value.checked_mul(multiplier).ok_or(UnitError::Overflow)
}

/// Parses a duration string (e.g. `5s`, `40h`, `90d`, `1w`) into a
/// [`Duration`]. A unit suffix is always required. Never panics: bad
/// suffixes and overflow return [`UnitError`].
pub fn parse_duration(s: &str) -> Result<Duration, UnitError> {
    let s = s.trim();
    let (digits, unit) = split_digits(s)?;
    let value: u64 = digits.parse().map_err(|e: std::num::ParseIntError| {
        UnitError::InvalidInteger(digits.to_string(), e.to_string())
    })?;
    if unit == "ms" {
        return Ok(Duration::from_millis(value));
    }
    let multiplier_secs: u64 = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 3_600,
        "d" => 86_400,
        "w" => 7 * 86_400,
        other => return Err(UnitError::InvalidDurationUnit(other.to_string())),
    };
    let secs = value
        .checked_mul(multiplier_secs)
        .ok_or(UnitError::Overflow)?;
    Ok(Duration::from_secs(secs))
}

/// A byte quantity. Parsed from strings like `16MiB` or a bare integer of
/// bytes (docs/configuration.md §9).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct ByteSize(pub u64);

impl fmt::Display for ByteSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for ByteSize {
    type Err = UnitError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_bytes(s).map(ByteSize)
    }
}

impl Serialize for ByteSize {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ByteSizeVisitor;

        impl Visitor<'_> for ByteSizeVisitor {
            type Value = ByteSize;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a byte size such as 16MiB, 256MB, or a bare integer of bytes")
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(ByteSize(v))
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                u64::try_from(v)
                    .map(ByteSize)
                    .map_err(|_| E::custom("byte size must not be negative"))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                parse_bytes(v).map(ByteSize).map_err(E::custom)
            }
        }

        deserializer.deserialize_any(ByteSizeVisitor)
    }
}

/// A duration. Parsed from strings like `5s`, `2m`, `1h` (docs/configuration.md §9).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct HumanDuration(pub Duration);

impl fmt::Display for HumanDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}ms", self.0.as_millis())
    }
}

impl FromStr for HumanDuration {
    type Err = UnitError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_duration(s).map(HumanDuration)
    }
}

impl Serialize for HumanDuration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}ms", self.0.as_millis()))
    }
}

impl<'de> Deserialize<'de> for HumanDuration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct HumanDurationVisitor;

        impl Visitor<'_> for HumanDurationVisitor {
            type Value = HumanDuration;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a duration such as 5s, 2m, 1h, 90d, or 1w")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                parse_duration(v).map(HumanDuration).map_err(E::custom)
            }
        }

        deserializer.deserialize_str(HumanDurationVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bytes_accepts_binary_and_decimal_units_and_bare_integers() {
        assert_eq!(parse_bytes("1024").unwrap(), 1024);
        assert_eq!(parse_bytes("1KiB").unwrap(), 1024);
        assert_eq!(parse_bytes("1KB").unwrap(), 1000);
    }

    #[test]
    fn parse_bytes_rejects_bad_suffix_without_panicking() {
        assert!(matches!(
            parse_bytes("5xyz"),
            Err(UnitError::InvalidByteUnit(_))
        ));
    }

    #[test]
    fn parse_bytes_rejects_overflow_without_panicking() {
        // u64::MAX bytes, multiplied by the TiB factor, overflows u64.
        assert!(matches!(
            parse_bytes("18446744073709551615TiB"),
            Err(UnitError::Overflow)
        ));
    }

    #[test]
    fn parse_duration_requires_a_unit_suffix() {
        assert!(matches!(
            parse_duration("200"),
            Err(UnitError::InvalidDurationUnit(_))
        ));
    }

    #[test]
    fn parse_duration_rejects_overflow_without_panicking() {
        // u64::MAX days, multiplied by 86_400 seconds/day, overflows u64.
        assert!(matches!(
            parse_duration("18446744073709551615d"),
            Err(UnitError::Overflow)
        ));
    }

    #[test]
    fn byte_size_and_human_duration_round_trip_through_yaml() {
        let bytes = ByteSize(16 * 1024 * 1024);
        let yaml = serde_norway::to_string(&bytes).unwrap();
        let parsed: ByteSize = serde_norway::from_str(&yaml).unwrap();
        assert_eq!(parsed, bytes);

        let duration = HumanDuration(Duration::from_secs(90));
        let yaml = serde_norway::to_string(&duration).unwrap();
        let parsed: HumanDuration = serde_norway::from_str(&yaml).unwrap();
        assert_eq!(parsed, duration);
    }
}
