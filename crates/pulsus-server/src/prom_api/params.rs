//! `/api/v1/*` parameter parsing: timestamps, `step`, the 11,000-point
//! cap, and the shared `Vec<(String,String)>` pair core both GET (query
//! string) and POST (`application/x-www-form-urlencoded` body) handlers
//! parse into (issue #32 architect plan: "one shared GET+POST param core
//! over `Vec<(String,String)>`", mirroring `#13`'s `logs_api::params`).
//!
//! **Self-contained by design** (architect plan): `prom_api` does not
//! import anything from `logs_api::params`, even though the pair-parsing
//! core (`parse_pairs`/`get`/`get_all`/`percent_decode`) is a near-
//! duplicate — coders may be editing `logs_api/` concurrently, so a shared
//! extraction now would be a merge-conflict magnet. A dedupe follow-up is
//! tracked as out of scope for this issue.
//!
//! Metrics timestamps differ from the log API's in two ways (docs/api.md
//! §3.1): a plain numeric literal is **unix seconds** (not nanoseconds,
//! and may carry a fractional part, e.g. `"1435781451.781"`), and `step`
//! accepts either a bare (possibly fractional) seconds literal or a
//! Prometheus duration string (`"30s"`, `"1m30s"`, `"1h"`).

use thiserror::Error;

/// The hard cap on points per series for `query_range` (issue #32
/// architect plan): `points = (end-start)/step + 1`; a query landing
/// exactly on the cap passes, one past it is `400 bad_data`. Checked
/// before any engine/ClickHouse call ([`check_range`]).
pub(crate) const POINTS_CAP: i64 = 11_000;

/// Default `start`/`end` lookback (`end - start`) when `start` is omitted
/// from a discovery request (`/labels`, `/label/{name}/values`, `/series`)
/// — matches `logs_api`'s own "last hour" default (docs/api.md §2.1),
/// there being no more specific convention pinned for the metrics
/// discovery endpoints.
const DEFAULT_LOOKBACK_MS: i64 = 3_600_000;

/// Errors from parsing `/api/v1/*` request parameters — mapped to `400
/// bad_data` by `error::ApiError` (the one exception, `UnsupportedContentType`,
/// still maps to `400`, just for a POST-specific reason).
#[derive(Debug, Error)]
pub(crate) enum ParamError {
    #[error("missing required parameter 'query'")]
    MissingQuery,
    #[error("missing required parameter {0:?}")]
    MissingParam(&'static str),
    #[error("missing required parameter 'match[]': at least one selector is required")]
    MissingMatch,
    #[error("invalid time {0:?}: expected unix seconds (optionally fractional) or RFC3339")]
    InvalidTime(String),
    #[error("invalid 'step' {raw:?}: {reason}")]
    InvalidStep { raw: String, reason: String },
    #[error("query_range would return {points} points per series, exceeding the cap of {cap}")]
    TooManyPoints { points: i64, cap: i64 },
    #[error("'end' must not be before 'start'")]
    EndBeforeStart,
    #[error("start/end range is too large to evaluate")]
    RangeOverflow,
    #[error("invalid 'limit' {0:?}: expected a non-negative integer")]
    InvalidLimit(String),
    #[error("request body must be application/x-www-form-urlencoded, got {0:?}")]
    UnsupportedContentType(String),
    #[error("request body is not valid UTF-8")]
    InvalidFormBody,
}

/// Unix milliseconds, right now. Matches `logs_api::params::now_ns`'s own
/// `std::time::SystemTime`-based convention, at millisecond rather than
/// nanosecond resolution (metrics timestamps are millisecond-precision
/// throughout `pulsus-read::metrics`/`pulsus-promql`).
pub(crate) fn now_ms() -> i64 {
    let dur = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(dur.as_millis()).unwrap_or(i64::MAX)
}

/// Parses a `time`/`start`/`end` value (docs/api.md §3.1): a float **unix
/// seconds** literal (`"1435781451.781"` -> `(secs*1000).round()`), or
/// (when the input does not parse as a plain number) an RFC3339 timestamp.
pub(crate) fn parse_time(raw: &str) -> Result<i64, ParamError> {
    if let Ok(secs) = raw.parse::<f64>() {
        if !secs.is_finite() {
            return Err(ParamError::InvalidTime(raw.to_string()));
        }
        let millis = (secs * 1000.0).round();
        let clamped = millis.clamp(i64::MIN as f64, i64::MAX as f64);
        return Ok(clamped as i64);
    }
    let dt = chrono::DateTime::parse_from_rfc3339(raw)
        .map_err(|_| ParamError::InvalidTime(raw.to_string()))?;
    Ok(dt.timestamp_millis())
}

/// `start`'s default when omitted from a discovery request: `end - 1h`
/// (see [`DEFAULT_LOOKBACK_MS`]).
pub(crate) fn default_start_ms(end_ms: i64) -> i64 {
    end_ms.saturating_sub(DEFAULT_LOOKBACK_MS)
}

/// `step` (`query_range` only): a bare (possibly fractional) seconds
/// literal, or a Prometheus compound duration string (`"30s"`, `"1m30s"`,
/// `"1h"`). Always `> 0` — a non-positive step is `400 bad_data`.
pub(crate) fn parse_step(raw: &str) -> Result<i64, ParamError> {
    if let Ok(secs) = raw.parse::<f64>() {
        if !secs.is_finite() {
            return Err(invalid_step(raw, "step must be finite"));
        }
        let ms = (secs * 1000.0).round() as i64;
        return positive_step(raw, ms);
    }
    let ms = parse_duration_ms(raw)?;
    positive_step(raw, ms)
}

fn positive_step(raw: &str, ms: i64) -> Result<i64, ParamError> {
    if ms <= 0 {
        return Err(invalid_step(raw, "step must be greater than zero"));
    }
    Ok(ms)
}

/// `query_range`'s hard cap (issue #32 architect plan, checked **before**
/// any engine/ClickHouse call): `end < start` and a non-positive `step`
/// are both `400`; `points = (end-start)/step + 1` exceeding
/// [`POINTS_CAP`] is `400 bad_data` naming the cap. `points == POINTS_CAP`
/// passes (inclusive).
pub(crate) fn check_range(start_ms: i64, end_ms: i64, step_ms: i64) -> Result<(), ParamError> {
    if end_ms < start_ms {
        return Err(ParamError::EndBeforeStart);
    }
    if step_ms <= 0 {
        return Err(invalid_step(
            &step_ms.to_string(),
            "step must be greater than zero",
        ));
    }
    // Code-review round-1 fix: `start`/`end` are clamped-from-`f64`
    // milliseconds ([`parse_time`]), so an extreme (but individually
    // valid) pair can land at/near `i64::MIN`/`i64::MAX` — a plain `end_ms
    // - start_ms` can overflow `i64` before the cap is ever checked.
    // Checked arithmetic throughout; any overflow is an extreme,
    // unevaluable range -> `400 bad_data`, never a panic/wraparound.
    let span_ms = end_ms
        .checked_sub(start_ms)
        .ok_or(ParamError::RangeOverflow)?;
    let points = span_ms
        .checked_div(step_ms) // step_ms > 0, checked above.
        .and_then(|p| p.checked_add(1))
        .ok_or(ParamError::RangeOverflow)?;
    if points > POINTS_CAP {
        return Err(ParamError::TooManyPoints {
            points,
            cap: POINTS_CAP,
        });
    }
    Ok(())
}

/// A minimal compound duration parser (`"30s"`, `"1m30s"`, `"1h"`),
/// milliseconds. Self-contained (see the module doc) rather than reusing
/// `logs_api::params::parse_duration_ns` (nanosecond-scoped, `pub(super)`
/// to that sibling module).
fn parse_duration_ms(raw: &str) -> Result<i64, ParamError> {
    const UNITS: &[(&str, i64)] = &[
        ("ms", 1),
        ("s", 1_000),
        ("m", 60_000),
        ("h", 3_600_000),
        ("d", 86_400_000),
        ("w", 7 * 86_400_000),
        ("y", 365 * 86_400_000),
    ];

    let bytes = raw.as_bytes();
    let mut idx = 0usize;
    let mut total: i64 = 0;
    let mut matched_any = false;
    while idx < bytes.len() {
        let digit_start = idx;
        while bytes.get(idx).is_some_and(u8::is_ascii_digit) {
            idx += 1;
        }
        if idx == digit_start {
            return Err(invalid_step(raw, "expected a number"));
        }
        let number: i64 = raw[digit_start..idx]
            .parse()
            .map_err(|_| invalid_step(raw, "numeric component out of range"))?;
        let unit_start = idx;
        let unit = UNITS
            .iter()
            .map(|(name, _)| *name)
            .filter(|name| raw[unit_start..].starts_with(name))
            .max_by_key(|name| name.len())
            .ok_or_else(|| invalid_step(raw, "unknown duration unit"))?;
        idx = unit_start + unit.len();
        let per_unit = UNITS
            .iter()
            .find(|(name, _)| *name == unit)
            .map(|(_, n)| *n)
            .unwrap_or(1);
        let component = number
            .checked_mul(per_unit)
            .ok_or_else(|| invalid_step(raw, "duration component overflows"))?;
        total = total
            .checked_add(component)
            .ok_or_else(|| invalid_step(raw, "duration overflows"))?;
        matched_any = true;
    }
    if !matched_any {
        return Err(invalid_step(raw, "empty duration literal"));
    }
    Ok(total)
}

fn invalid_step(raw: &str, reason: &str) -> ParamError {
    ParamError::InvalidStep {
        raw: raw.to_string(),
        reason: reason.to_string(),
    }
}

/// `metric`: the optional exact-name filter for `/metadata`.
pub(crate) fn metric(pairs: &[(String, String)]) -> Option<&str> {
    get(pairs, "metric")
}

/// `limit`: the optional row cap for `/metadata`.
pub(crate) fn parse_limit(raw: Option<&str>) -> Result<Option<usize>, ParamError> {
    match raw {
        None => Ok(None),
        Some(s) => s
            .parse::<usize>()
            .map(Some)
            .map_err(|_| ParamError::InvalidLimit(s.to_string())),
    }
}

/// Splits an `application/x-www-form-urlencoded` string (GET query string
/// or POST form body — the same wire format) into ordered `(key, value)`
/// pairs. Repeats a key exactly as many times as it appears, so callers
/// needing `match[]`'s repeated-key semantics use [`get_all`] against this
/// output.
pub(crate) fn parse_pairs(raw: &str) -> Vec<(String, String)> {
    raw.split('&')
        .filter(|s| !s.is_empty())
        .map(|pair| {
            let mut it = pair.splitn(2, '=');
            let k = it.next().unwrap_or("");
            let v = it.next().unwrap_or("");
            (percent_decode(k), percent_decode(v))
        })
        .collect()
}

/// The first value for `key`, if present.
pub(crate) fn get<'a>(pairs: &'a [(String, String)], key: &str) -> Option<&'a str> {
    pairs
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

/// Every value for `key`, in appearance order (`match[]` repeats).
pub(crate) fn get_all<'a>(pairs: &'a [(String, String)], key: &str) -> Vec<&'a str> {
    pairs
        .iter()
        .filter(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
        .collect()
}

/// `application/x-www-form-urlencoded` percent-decoding: `+` decodes to a
/// space, `%XX` decodes to the raw byte; anything else passes through.
/// Malformed `%` escapes are left as literal `%` bytes rather than
/// rejected — the form is still meaningful to decode best-effort, and any
/// resulting garbage value simply fails whatever typed parse consumes it
/// next (e.g. [`parse_time`]).
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 3 <= bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok();
                match hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                    Some(byte) => {
                        out.push(byte);
                        i += 3;
                    }
                    None => {
                        out.push(bytes[i]);
                        i += 1;
                    }
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_time_reads_a_fractional_unix_seconds_literal() {
        assert_eq!(parse_time("1435781451.781").unwrap(), 1_435_781_451_781);
    }

    #[test]
    fn parse_time_reads_a_bare_integer_as_unix_seconds() {
        assert_eq!(parse_time("1435781451").unwrap(), 1_435_781_451_000);
    }

    #[test]
    fn parse_time_reads_rfc3339() {
        assert_eq!(
            parse_time("2026-07-01T00:00:00Z").unwrap(),
            1_782_864_000_000
        );
    }

    #[test]
    fn parse_time_rejects_garbage() {
        let err = parse_time("not-a-timestamp").unwrap_err();
        assert!(matches!(err, ParamError::InvalidTime(_)));
    }

    #[test]
    fn parse_time_rejects_nan_and_infinity() {
        assert!(matches!(
            parse_time("NaN").unwrap_err(),
            ParamError::InvalidTime(_)
        ));
        assert!(matches!(
            parse_time("inf").unwrap_err(),
            ParamError::InvalidTime(_)
        ));
    }

    #[test]
    fn default_start_ms_is_one_hour_before_end() {
        assert_eq!(default_start_ms(3_600_000), 0);
    }

    #[test]
    fn parse_step_accepts_a_fractional_seconds_literal() {
        assert_eq!(parse_step("1.5").unwrap(), 1_500);
    }

    #[test]
    fn parse_step_accepts_a_bare_integer_as_seconds() {
        assert_eq!(parse_step("30").unwrap(), 30_000);
    }

    #[test]
    fn parse_step_accepts_a_compound_duration_literal() {
        assert_eq!(parse_step("1m30s").unwrap(), 90_000);
    }

    #[test]
    fn parse_step_accepts_a_plain_hour_literal() {
        assert_eq!(parse_step("1h").unwrap(), 3_600_000);
    }

    #[test]
    fn parse_step_rejects_zero() {
        let err = parse_step("0").unwrap_err();
        assert!(matches!(err, ParamError::InvalidStep { .. }));
    }

    #[test]
    fn parse_step_rejects_a_negative_literal() {
        let err = parse_step("-5").unwrap_err();
        assert!(matches!(err, ParamError::InvalidStep { .. }));
    }

    #[test]
    fn parse_step_rejects_garbage() {
        let err = parse_step("banana").unwrap_err();
        assert!(matches!(err, ParamError::InvalidStep { .. }));
    }

    #[test]
    fn check_range_rejects_end_before_start() {
        let err = check_range(1_000, 0, 1).unwrap_err();
        assert!(matches!(err, ParamError::EndBeforeStart));
    }

    #[test]
    fn check_range_rejects_a_non_positive_step() {
        let err = check_range(0, 1_000, 0).unwrap_err();
        assert!(matches!(err, ParamError::InvalidStep { .. }));
    }

    #[test]
    fn check_range_accepts_exactly_the_cap_inclusive() {
        // (end - start) / step + 1 == POINTS_CAP exactly.
        let end = (POINTS_CAP - 1) * 1_000;
        assert!(check_range(0, end, 1_000).is_ok());
    }

    #[test]
    fn check_range_rejects_one_point_past_the_cap() {
        let end = POINTS_CAP * 1_000;
        let err = check_range(0, end, 1_000).unwrap_err();
        match err {
            ParamError::TooManyPoints { points, cap } => {
                assert_eq!(points, POINTS_CAP + 1);
                assert_eq!(cap, POINTS_CAP);
            }
            other => panic!("expected TooManyPoints, got {other:?}"),
        }
    }

    /// Code-review round-1 fix: `start`/`end` near the `i64` extremes
    /// (reachable via [`parse_time`]'s clamped-`f64` conversion for a
    /// wildly out-of-range `time`/`start`/`end` literal) must never
    /// panic/overflow-wrap `check_range`'s arithmetic — a genuinely
    /// unevaluable extreme range is `400 bad_data`, not a crash.
    #[test]
    fn check_range_rejects_an_extreme_range_as_overflow_not_a_panic() {
        let err = check_range(i64::MIN, i64::MAX, 1_000).unwrap_err();
        assert!(matches!(err, ParamError::RangeOverflow));
    }

    #[test]
    fn check_range_overflow_maps_to_a_400_bad_data_message() {
        let err = check_range(i64::MIN, i64::MAX, 1).unwrap_err();
        assert!(err.to_string().contains("too large"));
    }

    #[test]
    fn parse_time_of_an_extreme_float_seconds_literal_clamps_rather_than_panics() {
        // Far beyond any representable i64-milliseconds value once
        // multiplied by 1000 — must clamp to `i64::MAX`, never panic.
        let ms = parse_time("1e300").unwrap();
        assert_eq!(ms, i64::MAX);
    }

    #[test]
    fn parse_time_of_an_extreme_negative_float_seconds_literal_clamps() {
        let ms = parse_time("-1e300").unwrap();
        assert_eq!(ms, i64::MIN);
    }

    /// End-to-end regression for the round-1 finding: two individually
    /// `parse_time`-valid extreme timestamps feeding straight into
    /// `check_range` must still resolve to a clean `400`, not a panic.
    #[test]
    fn extreme_parsed_timestamps_feeding_check_range_do_not_panic() {
        let start_ms = parse_time("-1e300").unwrap();
        let end_ms = parse_time("1e300").unwrap();
        let err = check_range(start_ms, end_ms, 1_000).unwrap_err();
        assert!(matches!(err, ParamError::RangeOverflow));
    }

    #[test]
    fn parse_limit_defaults_to_none() {
        assert_eq!(parse_limit(None).unwrap(), None);
    }

    #[test]
    fn parse_limit_parses_a_valid_value() {
        assert_eq!(parse_limit(Some("10")).unwrap(), Some(10));
    }

    #[test]
    fn parse_limit_rejects_non_numeric_input() {
        assert!(matches!(
            parse_limit(Some("abc")).unwrap_err(),
            ParamError::InvalidLimit(_)
        ));
    }

    #[test]
    fn parse_pairs_splits_and_decodes_a_query_string() {
        let pairs = parse_pairs("query=up&time=1435781451.781");
        assert_eq!(
            pairs,
            vec![
                ("query".to_string(), "up".to_string()),
                ("time".to_string(), "1435781451.781".to_string()),
            ]
        );
    }

    #[test]
    fn parse_pairs_decodes_plus_as_space() {
        let pairs = parse_pairs("query=a+b");
        assert_eq!(pairs, vec![("query".to_string(), "a b".to_string())]);
    }

    #[test]
    fn parse_pairs_of_an_empty_string_is_empty() {
        assert!(parse_pairs("").is_empty());
    }

    #[test]
    fn get_all_collects_every_repeated_match_bracket_key() {
        let pairs = parse_pairs("match%5B%5D=up&match%5B%5D=down");
        let values = get_all(&pairs, "match[]");
        assert_eq!(values, vec!["up", "down"]);
    }

    #[test]
    fn get_returns_the_first_value_for_a_key() {
        let pairs = parse_pairs("start=1&start=2");
        assert_eq!(get(&pairs, "start"), Some("1"));
    }

    #[test]
    fn get_is_none_for_a_missing_key() {
        let pairs = parse_pairs("start=1");
        assert_eq!(get(&pairs, "end"), None);
    }

    #[test]
    fn metric_reads_the_metric_param() {
        let pairs = parse_pairs("metric=up");
        assert_eq!(metric(&pairs), Some("up"));
    }
}
