//! Pure integer UTC civil-calendar helpers for the date/time-field
//! functions (issue #66, M6-03): [`field`] maps a unix-seconds instant to
//! one calendar/clock field with the exact values Go's
//! `time.Unix(sec, 0).UTC()` produces, and [`to_unix_secs`] is the total,
//! documented float→seconds conversion the vector-argument forms use.
//!
//! Hand-rolled (proleptic Gregorian, Howard Hinnant's `civil_from_days`/
//! `days_from_civil` algorithms) rather than pulling `chrono` into this
//! pure crate — the #66 adjudication's KISS call, same class as
//! `elementwise.rs`'s hand-mirrored Go `math.Min`/`math.Max`. Everything
//! here is integer-exact; the only floats are the final `as f64` results,
//! all of which are small integers representable exactly.

use crate::plan::DateFn;

/// Whole unix seconds for the date functions. Finite values whose
/// magnitude is `< 2^63` truncate toward zero — identical to Go
/// `int64(float64)` on the ONLY domain where Go is well-defined.
/// NaN, ±Inf, and `|v| >= 2^63` return `None`: Go's `int64(f)` for these
/// is platform-defined (amd64 CVTTSD2SI yields the "integer indefinite"
/// `0x8000_0000_0000_0000`), so we do NOT mirror it — our documented,
/// TOTAL behavior maps them to a NaN result value (and they are excluded
/// from the real-Prometheus differential for exactly that reason; plan v2
/// Δ1 on issue #66).
pub(crate) fn to_unix_secs(v: f64) -> Option<i64> {
    if v.is_nan() || v.is_infinite() || v.abs() >= 9_223_372_036_854_775_808.0 {
        return None;
    }
    Some(v.trunc() as i64)
}

/// Hinnant `civil_from_days`: days since 1970-01-01 → `(year, month,
/// day)` in the proleptic Gregorian calendar (UTC). Valid over the whole
/// range reachable from an `i64` seconds value.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Hinnant `days_from_civil`: the inverse of [`civil_from_days`] — used
/// here for `day_of_year` (days since January 1st of the same year).
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = i64::from(if m > 2 { m - 3 } else { m + 9 });
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

/// Gregorian leap rule — divisible by 4, except centuries unless
/// divisible by 400 (1900 is not a leap year; 2000 is).
fn is_leap_year(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(y) {
                29
            } else {
                28
            }
        }
        // `civil_from_days` only ever yields months 1..=12; kept total.
        other => unreachable!("civil month out of range: {other}"),
    }
}

/// One RFC 3339 UTC timestamp string with zero fractional seconds —
/// `time.Unix(unix_secs, 0).UTC().Format(time.RFC3339)` (Go's
/// `"2006-01-02T15:04:05Z07:00"` layout, always rendering the `Z` UTC
/// designator here since the instant is already UTC). The sole consumer
/// is [`crate::annotations::messages::histogram_quantile_forced_monotonicity_info`]
/// (`#124` review finding 2a): the pin's forced-monotonicity detail
/// renders each occurrence's timestamp this way
/// (`histogramQuantileForcedMonotonicityErr.Error()`, `annotations.go:
/// 333-341`).
pub(crate) fn rfc3339_utc_seconds(unix_secs: i64) -> String {
    let days = unix_secs.div_euclid(86_400);
    let second_of_day = unix_secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = second_of_day / 3_600;
    let minute = (second_of_day % 3_600) / 60;
    let second = second_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

/// One UTC calendar/clock field of `unix_secs`, per Go
/// `time.Unix(unix_secs, 0).UTC()`: `Year`/`Month` (1..12)/
/// `DayOfMonth` (1..31)/`DayOfWeek` (Sunday = 0)/`DayOfYear` (1..366)/
/// `DaysInMonth` (28..31)/`Hour` (0..23)/`Minute` (0..59). Euclidean
/// day/second-of-day splits keep pre-epoch instants correct (floor
/// semantics, exactly what a civil calendar needs).
pub(crate) fn field(func: DateFn, unix_secs: i64) -> f64 {
    let days = unix_secs.div_euclid(86_400);
    let second_of_day = unix_secs.rem_euclid(86_400);
    match func {
        DateFn::Year => civil_from_days(days).0 as f64,
        DateFn::Month => f64::from(civil_from_days(days).1),
        DateFn::DayOfMonth => f64::from(civil_from_days(days).2),
        // 1970-01-01 (day 0) was a Thursday (4); Sunday = 0 anchor.
        DateFn::DayOfWeek => ((days + 4).rem_euclid(7)) as f64,
        DateFn::DayOfYear => {
            let (y, _, _) = civil_from_days(days);
            (days - days_from_civil(y, 1, 1) + 1) as f64
        }
        DateFn::DaysInMonth => {
            let (y, m, _) = civil_from_days(days);
            f64::from(days_in_month(y, m))
        }
        DateFn::Hour => (second_of_day / 3_600) as f64,
        DateFn::Minute => ((second_of_day % 3_600) / 60) as f64,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every field of one instant at once — the cross-check shape the
    /// plan's reference tuples use.
    fn all_fields(secs: i64) -> (f64, f64, f64, f64, f64, f64, f64, f64) {
        (
            field(DateFn::Year, secs),
            field(DateFn::Month, secs),
            field(DateFn::DayOfMonth, secs),
            field(DateFn::DayOfWeek, secs),
            field(DateFn::DayOfYear, secs),
            field(DateFn::DaysInMonth, secs),
            field(DateFn::Hour, secs),
            field(DateFn::Minute, secs),
        )
    }

    #[test]
    fn epoch_is_thursday_january_first_1970() {
        // 1970-01-01 00:00:00 UTC.
        assert_eq!(all_fields(0), (1970.0, 1.0, 1.0, 4.0, 1.0, 31.0, 0.0, 0.0));
    }

    #[test]
    fn five_hundred_million_is_1985_11_05_00_53() {
        // 500000000 = 1985-11-05 00:53:20 UTC, a Tuesday, day 309 of a
        // non-leap year, in a 30-day month (the upstream date-function
        // corpus' own canonical instant).
        assert_eq!(
            all_fields(500_000_000),
            (1985.0, 11.0, 5.0, 2.0, 309.0, 30.0, 0.0, 53.0)
        );
    }

    #[test]
    fn one_million_one_hundred_eleven_thousand_etc_is_1970_01_13_20_38() {
        // 1111111 = 1970-01-13 20:38:31 UTC, a Tuesday (the upstream
        // at_modifier corpus' recurring eval instant).
        assert_eq!(
            all_fields(1_111_111),
            (1970.0, 1.0, 13.0, 2.0, 13.0, 31.0, 20.0, 38.0)
        );
    }

    #[test]
    fn century_leap_rule_2000_is_leap_1900_is_not() {
        // 2000-02-29 exists: 951782400 = 2000-02-29 00:00:00 UTC.
        assert_eq!(field(DateFn::DayOfMonth, 951_782_400), 29.0);
        assert_eq!(field(DateFn::Month, 951_782_400), 2.0);
        assert_eq!(field(DateFn::DaysInMonth, 951_782_400), 29.0);
        // 2000-12-31 is day 366 of a leap year: 978220800.
        assert_eq!(field(DateFn::DayOfYear, 978_220_800), 366.0);
        // 1900 is NOT a leap year (century, not divisible by 400):
        // -2203977600 = 1900-02-28 00:00:00 UTC; the next day is March 1.
        assert_eq!(field(DateFn::DaysInMonth, -2_203_977_600), 28.0);
        assert_eq!(field(DateFn::DayOfMonth, -2_203_977_600 + 86_400), 1.0);
        assert_eq!(field(DateFn::Month, -2_203_977_600 + 86_400), 3.0);
        // 2100 is not a leap year either; 2400 is (cross-checked against
        // upstream `days_in_month` expecteds' rule).
        assert!(!is_leap_year(2100));
        assert!(is_leap_year(2400));
        assert!(is_leap_year(2000));
        assert!(!is_leap_year(1900));
    }

    #[test]
    fn month_lengths_match_the_civil_calendar() {
        // Walk 2017 (non-leap, upstream's canonical days_in_month year):
        // Jan..Dec lengths.
        let want = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        // 2017-01-01 00:00:00 UTC = 1483228800.
        let mut secs = 1_483_228_800i64;
        for (m0, want_len) in want.iter().enumerate() {
            assert_eq!(
                field(DateFn::Month, secs),
                (m0 + 1) as f64,
                "walk desynced at month {}",
                m0 + 1
            );
            assert_eq!(
                field(DateFn::DaysInMonth, secs),
                f64::from(*want_len),
                "days_in_month for 2017-{:02}",
                m0 + 1
            );
            secs += i64::from(*want_len) * 86_400;
        }
        // Leap-year February: 2016-02-01 = 1454284800.
        assert_eq!(field(DateFn::DaysInMonth, 1_454_284_800), 29.0);
    }

    #[test]
    fn day_of_week_anchors_sunday_to_zero() {
        // 1970-01-04 was a Sunday (day 3 since epoch).
        assert_eq!(field(DateFn::DayOfWeek, 3 * 86_400), 0.0);
        // The day before the epoch (1969-12-31) was a Wednesday — the
        // negative-days branch of rem_euclid.
        assert_eq!(field(DateFn::DayOfWeek, -86_400), 3.0);
        // Saturday caps the range at 6: 1970-01-03.
        assert_eq!(field(DateFn::DayOfWeek, 2 * 86_400), 6.0);
    }

    #[test]
    fn pre_epoch_instants_use_floor_day_and_second_of_day_splits() {
        // One second before the epoch is 1969-12-31 23:59:59 UTC — a
        // truncating (toward-zero) split would wrongly say 1970-01-01
        // 00:00:-59.
        assert_eq!(
            all_fields(-1),
            (1969.0, 12.0, 31.0, 3.0, 365.0, 31.0, 23.0, 59.0)
        );
    }

    // --- to_unix_secs (plan v2 Δ1) ---

    #[test]
    fn to_unix_secs_truncates_finite_in_range_values_toward_zero() {
        assert_eq!(to_unix_secs(3.9), Some(3));
        assert_eq!(to_unix_secs(-3.9), Some(-3));
        assert_eq!(to_unix_secs(0.0), Some(0));
        assert_eq!(to_unix_secs(500_000_000.7), Some(500_000_000));
    }

    #[test]
    fn to_unix_secs_maps_nan_and_infinities_to_none() {
        assert_eq!(to_unix_secs(f64::NAN), None);
        assert_eq!(to_unix_secs(f64::INFINITY), None);
        assert_eq!(to_unix_secs(f64::NEG_INFINITY), None);
    }

    #[test]
    fn to_unix_secs_maps_out_of_range_magnitudes_to_none() {
        assert_eq!(to_unix_secs(1e19), None);
        assert_eq!(to_unix_secs(-1e19), None);
        // Exactly 2^63 (the smallest f64 not representable as i64) is out.
        assert_eq!(to_unix_secs(9_223_372_036_854_775_808.0), None);
        assert_eq!(to_unix_secs(-9_223_372_036_854_775_808.0), None);
        // The largest f64 strictly below 2^63 is in range (the boundary
        // the `>=` comparison pins).
        let below = f64::from_bits(9_223_372_036_854_775_808.0f64.to_bits() - 1);
        assert!(below < 9_223_372_036_854_775_808.0);
        assert_eq!(to_unix_secs(below), Some(below as i64));
    }

    #[test]
    fn rfc3339_utc_seconds_formats_the_epoch_and_a_realistic_instant() {
        assert_eq!(rfc3339_utc_seconds(0), "1970-01-01T00:00:00Z");
        // 2024-01-15T10:30:45Z, cross-checked against `date -u -d @<secs>`.
        assert_eq!(rfc3339_utc_seconds(1_705_314_645), "2024-01-15T10:30:45Z");
    }

    #[test]
    fn rfc3339_utc_seconds_handles_pre_epoch_instants() {
        // -86_400 = one day before the epoch.
        assert_eq!(rfc3339_utc_seconds(-86_400), "1969-12-31T00:00:00Z");
    }
}
