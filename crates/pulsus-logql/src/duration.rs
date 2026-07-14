//! Prometheus/LogQL-style compound duration literals (`5m`, `1h30m`,
//! `500ms`, ...) parsed into nanoseconds with checked `u64` arithmetic —
//! an overflowing or malformed literal is a `ParseError`, never a panic
//! or a wrapped/truncated value (architect plan: "Duration & recursion as
//! panic vectors").

use crate::ast::Duration;
use crate::error::LogQlError;
use crate::token::Span;

/// Known units, longest representation first where it matters for
/// human-readable error messages. Matching in `lexer.rs`/`parse_duration`
/// does not depend on this order (no unit string here is a prefix of
/// another), but keeping it stable makes diffs and error text
/// deterministic.
pub(crate) const UNITS: &[(&str, u64)] = &[
    ("ns", 1),
    ("us", NS_PER_US),
    ("µs", NS_PER_US),
    ("ms", NS_PER_MS),
    ("s", NS_PER_S),
    ("m", NS_PER_M),
    ("h", NS_PER_H),
    ("d", NS_PER_D),
    ("w", NS_PER_W),
    ("y", NS_PER_Y),
];

const NS_PER_US: u64 = 1_000;
const NS_PER_MS: u64 = 1_000_000;
const NS_PER_S: u64 = 1_000_000_000;
const NS_PER_M: u64 = 60 * NS_PER_S;
const NS_PER_H: u64 = 60 * NS_PER_M;
const NS_PER_D: u64 = 24 * NS_PER_H;
const NS_PER_W: u64 = 7 * NS_PER_D;
const NS_PER_Y: u64 = 365 * NS_PER_D;

fn unit_nanos(unit: &str) -> Option<u64> {
    UNITS
        .iter()
        .find(|(name, _)| *name == unit)
        .map(|(_, nanos)| *nanos)
}

/// Parses a raw compound duration literal (as scanned by the lexer, e.g.
/// `"1h30m"`) into nanoseconds. Re-validates the literal independently of
/// the lexer's own scan (defense in depth: this function must never panic
/// or overflow on arbitrary/fuzzed `raw` text, since it is also reachable
/// from a directly-constructed raw string in tests).
pub(crate) fn parse_duration(raw: &str, span: Span) -> Result<Duration, LogQlError> {
    let mut idx = 0usize;
    let mut total: u64 = 0;
    let mut matched_any = false;

    while idx < raw.len() {
        let digit_start = idx;
        while raw.as_bytes().get(idx).is_some_and(u8::is_ascii_digit) {
            idx += 1;
        }
        if idx == digit_start {
            return Err(LogQlError::InvalidDuration {
                raw: raw.to_string(),
                reason: format!("expected a number at offset {idx} in the literal"),
                span,
            });
        }
        let number: u64 =
            raw[digit_start..idx]
                .parse()
                .map_err(|_| LogQlError::InvalidDuration {
                    raw: raw.to_string(),
                    reason: "numeric component out of range".to_string(),
                    span,
                })?;

        let unit_start = idx;
        let unit = UNITS
            .iter()
            .map(|(name, _)| *name)
            .filter(|name| raw[unit_start..].starts_with(name))
            .max_by_key(|name| name.len())
            .ok_or_else(|| LogQlError::InvalidDuration {
                raw: raw.to_string(),
                reason: format!("unknown unit at offset {unit_start}"),
                span,
            })?;
        idx = unit_start + unit.len();

        // Infallible: `unit` was just selected from `UNITS` above, so
        // `unit_nanos` always finds a match — documented invariant, not
        // reachable from untrusted input.
        let per_unit = unit_nanos(unit).expect("unit was selected from the UNITS table above");
        let component =
            number
                .checked_mul(per_unit)
                .ok_or_else(|| LogQlError::InvalidDuration {
                    raw: raw.to_string(),
                    reason: "duration component overflows u64 nanoseconds".to_string(),
                    span,
                })?;
        total = total
            .checked_add(component)
            .ok_or_else(|| LogQlError::InvalidDuration {
                raw: raw.to_string(),
                reason: "duration overflows u64 nanoseconds".to_string(),
                span,
            })?;
        matched_any = true;
    }

    if !matched_any {
        return Err(LogQlError::InvalidDuration {
            raw: raw.to_string(),
            reason: "empty duration literal".to_string(),
            span,
        });
    }
    Ok(Duration::from_nanos(total))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SPAN: Span = Span { start: 0, end: 0 };

    #[test]
    fn parses_a_single_unit_literal() {
        assert_eq!(parse_duration("5m", SPAN).unwrap().as_nanos(), 5 * NS_PER_M);
    }

    #[test]
    fn parses_a_compound_literal() {
        assert_eq!(
            parse_duration("1h30m", SPAN).unwrap().as_nanos(),
            NS_PER_H + 30 * NS_PER_M
        );
    }

    #[test]
    fn parses_milliseconds_without_colliding_with_meters_or_minutes() {
        assert_eq!(
            parse_duration("500ms", SPAN).unwrap().as_nanos(),
            500 * NS_PER_MS
        );
    }

    #[test]
    fn parses_a_three_component_compound_literal() {
        assert_eq!(
            parse_duration("1h30m5s", SPAN).unwrap().as_nanos(),
            NS_PER_H + 30 * NS_PER_M + 5 * NS_PER_S
        );
    }

    #[test]
    fn rejects_an_unknown_unit() {
        let err = parse_duration("5x", SPAN).unwrap_err();
        assert!(matches!(err, LogQlError::InvalidDuration { .. }));
    }

    #[test]
    fn rejects_an_empty_literal() {
        let err = parse_duration("", SPAN).unwrap_err();
        assert!(matches!(err, LogQlError::InvalidDuration { .. }));
    }

    #[test]
    fn rejects_a_missing_unit() {
        let err = parse_duration("5", SPAN).unwrap_err();
        assert!(matches!(err, LogQlError::InvalidDuration { .. }));
    }

    #[test]
    fn rejects_overflowing_components_without_panicking() {
        let err = parse_duration("99999999999999999999y", SPAN).unwrap_err();
        assert!(matches!(err, LogQlError::InvalidDuration { .. }));
    }

    #[test]
    fn rejects_a_sum_that_overflows_even_though_no_single_component_does() {
        // Two huge-but-individually-valid components whose sum overflows
        // u64 nanoseconds: the checked_add path, not checked_mul.
        let huge = u64::MAX / NS_PER_Y; // largest year count that alone fits
        let raw = format!("{huge}y{huge}y");
        let err = parse_duration(&raw, SPAN).unwrap_err();
        assert!(matches!(err, LogQlError::InvalidDuration { .. }));
    }
}
