//! TraceQL duration literals parsed into nanoseconds with checked
//! arithmetic. The grammar is normative in-house (docs/api.md §4.2, plan
//! v3 F3): an **unsigned** decimal number (integer or fraction — `2`,
//! `1.5`, `.5`) immediately followed by exactly **one** unit from
//! `{ns, us, µs, ms, s, m, h}`. No sign, no compound literals (`1h30m`),
//! and no LogQL/Prometheus `d`/`w`/`y` units. Fractional literals convert
//! **exactly**: a literal is valid iff `number × unit` is a whole number
//! of nanoseconds after decimal expansion — otherwise it is a positioned
//! [`TraceQlError::FractionalNanoseconds`]. No rounding, no truncation.

use crate::ast::Duration;
use crate::error::TraceQlError;
use crate::token::Span;

/// The complete supported unit table. `d`/`w`/`y` are deliberately absent
/// — they are LogQL/Prometheus-isms, not TraceQL (they get a dedicated
/// "not supported" reason below so the boundary is visible in the error).
pub(crate) const UNITS: &[(&str, u64)] = &[
    ("ns", 1),
    ("us", NS_PER_US),
    ("µs", NS_PER_US),
    ("ms", NS_PER_MS),
    ("s", NS_PER_S),
    ("m", NS_PER_M),
    ("h", NS_PER_H),
];

/// Units other duration grammars accept but TraceQL does not — named in
/// the error reason so `1d` fails as "not supported" rather than
/// "unknown".
const REJECTED_UNITS: &[&str] = &["d", "w", "y"];

const NS_PER_US: u64 = 1_000;
const NS_PER_MS: u64 = 1_000_000;
const NS_PER_S: u64 = 1_000_000_000;
const NS_PER_M: u64 = 60 * NS_PER_S;
const NS_PER_H: u64 = 60 * NS_PER_M;

const ALLOWED_UNITS_MSG: &str = "allowed units are ns, us, µs, ms, s, m, h";

/// The longest fraction (after stripping trailing zeros) that could ever
/// convert exactly: the largest unit is `h` = 3 600 000 000 000 ns =
/// 2¹³·3²·5¹¹, i.e. at most 13 twos and 11 fives, and a trimmed
/// fraction's numerator is not divisible by 10, so exactness requires at
/// most 13 fractional digits. 19 is a comfortably safe, `u128`-parseable
/// cap; anything longer is `FractionalNanoseconds` by construction.
const MAX_EXACT_FRACTION_DIGITS: usize = 19;

/// Parses a raw single-group duration literal (as scanned by the lexer,
/// e.g. `"1.5s"`) into nanoseconds. Re-validates the literal
/// independently of the lexer's own scan (defense in depth: this
/// function must never panic or overflow on arbitrary/fuzzed `raw` text).
pub(crate) fn parse_duration(raw: &str, span: Span) -> Result<Duration, TraceQlError> {
    let bytes = raw.as_bytes();
    let mut idx = 0usize;

    while bytes.get(idx).is_some_and(u8::is_ascii_digit) {
        idx += 1;
    }
    let int_digits = &raw[..idx];

    let mut frac_digits = "";
    if bytes.get(idx) == Some(&b'.') {
        let frac_start = idx + 1;
        idx = frac_start;
        while bytes.get(idx).is_some_and(u8::is_ascii_digit) {
            idx += 1;
        }
        frac_digits = &raw[frac_start..idx];
    }

    if int_digits.is_empty() && frac_digits.is_empty() {
        return Err(TraceQlError::InvalidDuration {
            raw: raw.to_string(),
            reason: "expected an unsigned decimal number before the unit".to_string(),
            span,
        });
    }

    // `idx` only ever advanced over ASCII digits and `.`, so this slice
    // is always on a UTF-8 boundary even with a multi-byte unit (`µs`).
    let unit = &raw[idx..];
    let Some(per_unit) = unit_nanos(unit) else {
        let reason = if REJECTED_UNITS.contains(&unit) {
            format!("unit {unit:?} is not supported; {ALLOWED_UNITS_MSG}")
        } else if unit.is_empty() {
            format!("missing unit; {ALLOWED_UNITS_MSG}")
        } else {
            format!("unknown unit {unit:?}; {ALLOWED_UNITS_MSG}")
        };
        return Err(TraceQlError::InvalidDuration {
            raw: raw.to_string(),
            reason,
            span,
        });
    };

    let overflow = || TraceQlError::InvalidDuration {
        raw: raw.to_string(),
        reason: "duration overflows u64 nanoseconds".to_string(),
        span,
    };

    let int_part: u128 = if int_digits.is_empty() {
        0
    } else {
        int_digits.parse().map_err(|_| overflow())?
    };
    let mut total = int_part
        .checked_mul(per_unit as u128)
        .ok_or_else(overflow)?;

    // Exact fractional conversion: value = frac × unit / 10^len, valid
    // iff the division is exact. Trailing zeros never affect exactness.
    let trimmed_frac = frac_digits.trim_end_matches('0');
    if !trimmed_frac.is_empty() {
        let fractional = || TraceQlError::FractionalNanoseconds {
            raw: raw.to_string(),
            span,
        };
        if trimmed_frac.len() > MAX_EXACT_FRACTION_DIGITS {
            return Err(fractional());
        }
        // ≤ 19 ASCII digits always parses into u128; treat a failure as
        // the fractional error rather than panicking (defense in depth).
        let numerator: u128 = trimmed_frac.parse().map_err(|_| fractional())?;
        let scaled = numerator
            .checked_mul(per_unit as u128)
            .ok_or_else(fractional)?;
        let denominator = 10u128.pow(trimmed_frac.len() as u32);
        if scaled % denominator != 0 {
            return Err(fractional());
        }
        total = total
            .checked_add(scaled / denominator)
            .ok_or_else(overflow)?;
    }

    let nanos = u64::try_from(total).map_err(|_| overflow())?;
    Ok(Duration::from_nanos(nanos))
}

fn unit_nanos(unit: &str) -> Option<u64> {
    UNITS
        .iter()
        .find(|(name, _)| *name == unit)
        .map(|(_, nanos)| *nanos)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SPAN: Span = Span { start: 0, end: 0 };

    fn nanos(raw: &str) -> u64 {
        parse_duration(raw, SPAN)
            .unwrap_or_else(|e| panic!("expected {raw:?} to parse, got {e}"))
            .as_nanos()
    }

    #[test]
    fn parses_every_supported_unit() {
        assert_eq!(nanos("2ns"), 2);
        assert_eq!(nanos("2us"), 2 * NS_PER_US);
        assert_eq!(nanos("2µs"), 2 * NS_PER_US);
        assert_eq!(nanos("2ms"), 2 * NS_PER_MS);
        assert_eq!(nanos("2s"), 2 * NS_PER_S);
        assert_eq!(nanos("2m"), 2 * NS_PER_M);
        assert_eq!(nanos("2h"), 2 * NS_PER_H);
    }

    #[test]
    fn parses_the_plan_accept_vectors() {
        assert_eq!(nanos("2s"), 2_000_000_000);
        assert_eq!(nanos("100ms"), 100_000_000);
        assert_eq!(nanos("1.5s"), 1_500_000_000);
        assert_eq!(nanos("500µs"), 500_000);
        assert_eq!(nanos(".5s"), 500_000_000);
        assert_eq!(nanos("0.5s"), 500_000_000);
    }

    #[test]
    fn exact_fractions_of_small_units_are_accepted() {
        // 0.25us = 250ns and 0.001ms = 1000ns are whole nanoseconds.
        assert_eq!(nanos("0.25us"), 250);
        assert_eq!(nanos("0.001ms"), 1_000);
    }

    #[test]
    fn the_finest_exact_fraction_of_a_second_is_one_nanosecond() {
        // 9 fractional digits on `s` is the precision boundary: exactly
        // 1ns; a 10th digit can no longer be whole.
        assert_eq!(nanos("0.000000001s"), 1);
        let err = parse_duration("0.0000000001s", SPAN).unwrap_err();
        assert!(matches!(err, TraceQlError::FractionalNanoseconds { .. }));
    }

    #[test]
    fn the_deepest_exact_fraction_uses_all_thirteen_tens_factors_of_an_hour() {
        // h = 3.6e12 ns = 2^13·3^2·5^11: a 13-digit fraction is the
        // deepest that can ever be exact (0.0000000000625h = 225ns);
        // 14 digits cannot.
        assert_eq!(nanos("0.0000000000625h"), 225);
        let err = parse_duration("0.00000000000625h", SPAN).unwrap_err();
        assert!(matches!(err, TraceQlError::FractionalNanoseconds { .. }));
    }

    #[test]
    fn trailing_zeros_do_not_affect_exactness() {
        assert_eq!(nanos("0.50s"), 500_000_000);
        assert_eq!(nanos("1.000000000s"), 1_000_000_000);
    }

    #[test]
    fn rejects_inexact_fractions_as_fractional_nanoseconds() {
        for raw in ["0.1ns", "0.0000001ms", "0.0000000001s", "1.0000000001s"] {
            let err = parse_duration(raw, SPAN).unwrap_err();
            assert!(
                matches!(err, TraceQlError::FractionalNanoseconds { .. }),
                "{raw:?} -> {err}"
            );
        }
    }

    #[test]
    fn rejects_an_over_long_fraction_without_panicking() {
        let raw = format!("0.{}5h", "0".repeat(40));
        let err = parse_duration(&raw, SPAN).unwrap_err();
        assert!(matches!(err, TraceQlError::FractionalNanoseconds { .. }));
    }

    #[test]
    fn rejects_logql_units_as_unsupported_not_unknown() {
        for raw in ["1d", "1w", "1y"] {
            let err = parse_duration(raw, SPAN).unwrap_err();
            match err {
                TraceQlError::InvalidDuration { reason, .. } => {
                    assert!(reason.contains("not supported"), "{raw:?} -> {reason}");
                }
                other => panic!("{raw:?} -> unexpected {other}"),
            }
        }
    }

    #[test]
    fn rejects_an_unknown_unit() {
        let err = parse_duration("5x", SPAN).unwrap_err();
        match err {
            TraceQlError::InvalidDuration { reason, .. } => {
                assert!(reason.contains("unknown unit"));
            }
            other => panic!("unexpected {other}"),
        }
    }

    #[test]
    fn rejects_a_corrupted_unit() {
        let err = parse_duration("5se", SPAN).unwrap_err();
        assert!(matches!(err, TraceQlError::InvalidDuration { .. }));
    }

    #[test]
    fn rejects_a_missing_unit() {
        let err = parse_duration("5", SPAN).unwrap_err();
        match err {
            TraceQlError::InvalidDuration { reason, .. } => {
                assert!(reason.contains("missing unit"));
            }
            other => panic!("unexpected {other}"),
        }
    }

    #[test]
    fn rejects_an_empty_literal() {
        let err = parse_duration("", SPAN).unwrap_err();
        assert!(matches!(err, TraceQlError::InvalidDuration { .. }));
    }

    #[test]
    fn rejects_a_bare_dot_with_no_digits() {
        let err = parse_duration(".s", SPAN).unwrap_err();
        assert!(matches!(err, TraceQlError::InvalidDuration { .. }));
    }

    #[test]
    fn rejects_overflowing_literals_without_panicking() {
        // > u64::MAX nanoseconds (u64::MAX ns ≈ 584y ≈ 5.1M hours).
        for raw in [
            "99999999999999999999s",
            "18446744073709551616ns",
            "6000000000h",
            &format!("{}s", "9".repeat(60)),
        ] {
            let err = parse_duration(raw, SPAN).unwrap_err();
            match err {
                TraceQlError::InvalidDuration { reason, .. } => {
                    assert!(reason.contains("overflows"), "{raw:?} -> {reason}");
                }
                other => panic!("{raw:?} -> unexpected {other}"),
            }
        }
    }

    #[test]
    fn accepts_the_exact_u64_maximum() {
        assert_eq!(nanos("18446744073709551615ns"), u64::MAX);
    }

    #[test]
    fn an_integer_plus_exact_fraction_combines_exactly() {
        assert_eq!(nanos("2.5ms"), 2_500_000);
    }

    #[test]
    fn zero_valued_literals_are_valid() {
        assert_eq!(nanos("0s"), 0);
        assert_eq!(nanos("0.0h"), 0);
    }
}
