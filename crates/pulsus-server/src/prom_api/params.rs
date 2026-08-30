//! `/api/v1/*` parameter parsing: timestamps, `step`, the 11,000-step-
//! interval resolution cap, and the shared `Vec<(String,String)>` pair
//! core both GET (query
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

use std::future::Future;
use std::time::Duration;

use thiserror::Error;

/// The hard resolution cap for `query_range` (issue #32 architect plan;
/// corrected to count **step intervals** by issue #471 M3). The predicate
/// is `(end - start) / step > POINTS_CAP`: 11,000 intervals (11,001 grid
/// points) is SERVED, 11,001 intervals is `400 bad_data`. Checked before
/// any engine/ClickHouse call ([`check_range`]).
///
/// The number is spelled `11,000` inside
/// [`ParamError::MaxResolutionExceeded`]'s message; `points_cap_and_the_
/// cap_message_agree` in this module's tests is what stops the two
/// drifting apart.
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
    /// Issue #471 M3. The message is the metrics reference's own
    /// sentence, **not** the LogQL sibling's
    /// (`logs_api::params::ParamError::TooManyPoints` spells it
    /// `per time series` and ends `Try increasing the value of the step
    /// parameter`) — two references, two wordings, and copying the
    /// nearest correct-looking implementation is how a second defect gets
    /// introduced while fixing the first.
    #[error(
        "exceeded maximum resolution of 11,000 points per timeseries. \
         Try decreasing the query resolution (?step=XX)"
    )]
    MaxResolutionExceeded,
    #[error("'end' must not be before 'start'")]
    EndBeforeStart,
    #[error("start/end range is too large to evaluate")]
    RangeOverflow,
    #[error("invalid 'limit' {0:?}: expected a non-negative integer")]
    InvalidLimit(String),
    /// Issue #471 M4. The reference's own prose, byte-for-byte — unlike
    /// [`ParamError::LimitNotAnInteger`], whose reference counterpart is
    /// its runtime's integer-parse text and is deliberately not
    /// reproduced (see that variant).
    #[error("invalid parameter \"limit\": limit must be non-negative")]
    LimitNegative,
    /// Issue #471 M4. **Ours, not the reference's.** The reference emits
    /// two different strings here — one for a syntax failure and one for
    /// an out-of-range value — both naming its own runtime's integer
    /// parser, which we cannot reproduce and would not want to. The
    /// status (`400`) and `errorType` (`bad_data`) are identical, and the
    /// fact conveyed is the same, so this is not a ledgered divergence.
    #[error("invalid parameter \"limit\": cannot parse {0:?} to an integer")]
    LimitNotAnInteger(String),
    /// Issue #471 M6: a `U__`-escaped label name that unescapes to the
    /// empty string. The reference's own sentence, byte-for-byte. No
    /// payload, so the literal cannot drift.
    #[error("invalid label name: \"\"")]
    EmptyLabelName,
    /// Issue #471 M2: an unparseable `timeout` request parameter
    /// (`/query`, `/query_range` only).
    #[error("invalid 'timeout' {raw:?}: {reason}")]
    InvalidTimeout { raw: String, reason: String },
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

/// `query_range`'s hard resolution cap (issue #32 architect plan,
/// checked **before** any engine/ClickHouse call): `end < start` and a
/// non-positive `step` are both `400`; `(end - start) / step` exceeding
/// [`POINTS_CAP`] **step intervals** is `400 bad_data`.
///
/// **Read this before changing it (issue #471 M3).** The reference's
/// error message says *points* and its predicate counts *step intervals*.
/// Someone implemented the sentence instead of the predicate, and this
/// function rejected one step early — silently, because "11,000 points"
/// is exactly what the message says. A reference's prose is not its
/// contract; its predicate is, and where the two disagree is where the
/// bug will be.
///
/// `(end - start) / step == 11_000` is SERVED (11,001 grid points);
/// 11,001 intervals is `400 bad_data`. The two siblings in this repo that
/// already had the rule right are
/// `logs_api::params::ensure_range_resolution` and
/// `pulsus_read::logql::window::ensure_grid_resolution`, whose
/// `grid_resolution_fence_serves_11000_intervals_and_rejects_11001` is
/// this function's specification on the other surface. The three sites
/// keep separate constants deliberately: they answer to two different
/// references that happen to agree today.
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
    let intervals = span_ms
        .checked_div(step_ms) // step_ms > 0, checked above.
        .ok_or(ParamError::RangeOverflow)?;
    if intervals > POINTS_CAP {
        return Err(ParamError::MaxResolutionExceeded);
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

/// `limit` for the three **discovery** endpoints — `/labels`,
/// `/label/{name}/values`, `/series` (issue #471 M4).
///
/// Absent, empty and `"0"` all mean *no limit*, which is the reference's
/// rule on these three routes. It is **not** [`parse_limit`]'s rule:
/// `/metadata` reads `limit=0` as *return nothing* on both servers, so the
/// two parsers stay separate and must not be unified.
///
/// Parsed as `i64` rather than `usize` so a negative value is
/// distinguishable from a non-numeric one and each gets its own message;
/// a leading `+` is accepted (`"+2"` -> `Some(2)`) and an out-of-`i64`
/// value is [`ParamError::LimitNotAnInteger`].
///
/// Truncation is a **response-size** cap, exactly as in the reference —
/// never a scan bound (`PULSUS_PROMQL_MAX_METRIC_FANOUT` and
/// `PULSUS_PROMQL_MAX_CACHE_SCAN` remain the scan bounds).
pub(crate) fn parse_discovery_limit(raw: Option<&str>) -> Result<Option<usize>, ParamError> {
    let Some(s) = raw else {
        return Ok(None);
    };
    if s.is_empty() {
        return Ok(None);
    }
    let n: i64 = s
        .parse()
        .map_err(|_| ParamError::LimitNotAnInteger(s.to_string()))?;
    if n < 0 {
        return Err(ParamError::LimitNegative);
    }
    if n == 0 {
        return Ok(None);
    }
    usize::try_from(n)
        .map(Some)
        .map_err(|_| ParamError::LimitNotAnInteger(s.to_string()))
}

/// A parsed `timeout` request parameter (issue #471 M2).
///
/// The field is private to this module, so a sibling module (`handlers`)
/// cannot read a [`Duration`] out of it: no getter, no `Deref`, no
/// ordering, no `From`. The only thing that can be done with one is pass
/// it to [`run_under_request_deadline`], which applies the guard
/// internally — that is what makes "install the requested timeout
/// unconditionally" unwritable **by accident**.
///
/// **The bound, stated because it is easy to overstate.** This closes the
/// accidental route only. A handler still owns the raw parameter string
/// in its pair vector and could re-parse it and build its own timer; that
/// is a deliberate act, not a slip, and what catches it is the two
/// producer messages being asserted as literals on the wire.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RequestedTimeout(Duration);

/// `timeout` (`/query`, `/query_range` only — issue #471 M2). Exactly
/// [`parse_step`]'s accept set: a bare (possibly fractional) seconds
/// literal, or the compound duration grammar of [`parse_duration_ms`]
/// (so `"60"` is 60 s, `"1ms"` is 1 ms, `"1ns"` is rejected — `ns` is not
/// an accepted unit on the reference either). Must be `> 0` and must not
/// overflow `i64` milliseconds: unlike `step`, `"1e300"` is a rejection
/// rather than a saturation, because a saturated deadline would silently
/// become "no deadline at all".
pub(crate) fn parse_timeout(raw: &str) -> Result<RequestedTimeout, ParamError> {
    let ms = timeout_ms(raw).map_err(|reason| ParamError::InvalidTimeout {
        raw: raw.to_string(),
        reason,
    })?;
    // `ms > 0` was checked by `positive_timeout`.
    Ok(RequestedTimeout(Duration::from_millis(ms as u64)))
}

fn timeout_ms(raw: &str) -> Result<i64, String> {
    if let Ok(secs) = raw.parse::<f64>() {
        if !secs.is_finite() {
            return Err("timeout must be finite".to_string());
        }
        let ms = secs * 1000.0;
        if ms > i64::MAX as f64 {
            return Err("timeout is too large".to_string());
        }
        return positive_timeout(ms.round() as i64);
    }
    let ms = parse_duration_ms(raw).map_err(|e| match e {
        ParamError::InvalidStep { reason, .. } => reason,
        other => other.to_string(),
    })?;
    positive_timeout(ms)
}

fn positive_timeout(ms: i64) -> Result<i64, String> {
    if ms <= 0 {
        return Err("timeout must be greater than zero".to_string());
    }
    Ok(ms)
}

/// Runs `work` under the requested timeout when — and **only** when — it
/// is STRICTLY shorter than the server deadline (issue #471 M2).
///
/// Equal durations would put two timers on one request and make the
/// observed message a race; a longer one is preempted by the outer
/// request-deadline layer anyway. In both cases the server deadline
/// governs and the request-deadline producer answers, so the requested one
/// is not installed at all.
///
/// `Err(d)` carries the duration that expired — the only way a caller ever
/// learns it, and only after the fact.
pub(crate) async fn run_under_request_deadline<T, F>(
    requested: Option<RequestedTimeout>,
    server: Duration,
    work: F,
) -> Result<T, Duration>
where
    F: Future<Output = T>,
{
    match requested {
        Some(RequestedTimeout(d)) if d < server => match tokio::time::timeout(d, work).await {
            Ok(value) => Ok(value),
            Err(_) => Err(d),
        },
        _ => Ok(work.await),
    }
}

/// The reference's value-encoding unescape for a label name, applied at
/// the HTTP boundary before any storage lookup (issue #471 M6).
///
/// The datasource escapes a label name into the URL path whenever it is
/// not legacy-legal: a `U__` prefix, `_` -> `__`, valid legacy runes kept,
/// anything else `_<hex>_`. The reference unescapes unconditionally and
/// **returns the input unchanged on any malformed escape** rather than
/// erroring.
///
/// * no `U__` prefix -> returned unchanged;
/// * `__` -> `_`;
/// * `_<hex>_` -> that Unicode scalar, hex case-insensitive, **at most
///   five hex digits before the closing `_`** (six digits bail out, which
///   is why `U__x_10ffff_y` does not decode even where a label literally
///   named `x\u{10ffff}y` exists);
/// * any malformed escape — a non-hex byte, a missing closing `_`, a
///   trailing `_` at end of input, or a value that is not a Unicode scalar
///   (a surrogate) -> the ORIGINAL input, unchanged, never an error;
/// * only ONE prefix is stripped, so `U__U__job` unescapes to `U_job`;
/// * the walk is over BYTES, so a multi-byte character outside an escape
///   is copied through verbatim.
///
/// `U__` alone unescapes to the empty string, which the caller rejects as
/// [`ParamError::EmptyLabelName`]; a non-`U__` name that is merely not
/// legacy-legal (`a-b`) is not rejected at all.
pub(crate) fn unescape_label_name(name: &str) -> String {
    let Some(escaped) = name.strip_prefix("U__") else {
        return name.to_string();
    };
    let bytes = escaped.as_bytes();
    // A byte buffer, not a `String`: outside an escape the walk copies raw
    // bytes, so a multi-byte character passes through verbatim. Every byte
    // it copies is either ASCII or a continuation byte of a complete
    // sequence in `escaped` (which is valid UTF-8), and every rune it
    // decodes is pushed encoded, so the result is always valid UTF-8.
    let mut out: Vec<u8> = Vec::with_capacity(escaped.len());
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] != b'_' {
            out.push(bytes[i]);
            i += 1;
            continue;
        }
        i += 1;
        // A trailing `_` at end of input is malformed.
        if i >= bytes.len() {
            return name.to_string();
        }
        if bytes[i] == b'_' {
            out.push(b'_');
            i += 1;
            continue;
        }
        // `_<hex>_`: at most five hex digits before the closing `_`.
        let mut value: u32 = 0;
        let mut digits = 0usize;
        loop {
            if i >= bytes.len() {
                // Ran off the end without a closing `_`.
                return name.to_string();
            }
            if bytes[i] == b'_' {
                i += 1;
                break;
            }
            if digits >= 5 {
                return name.to_string();
            }
            let Some(d) = (bytes[i] as char).to_digit(16) else {
                return name.to_string();
            };
            value = value * 16 + d;
            digits += 1;
            i += 1;
        }
        let Some(c) = char::from_u32(value) else {
            // Surrogates and out-of-range values are malformed escapes.
            return name.to_string();
        };
        let mut buf = [0u8; 4];
        out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
    }
    String::from_utf8(out).unwrap_or_else(|_| name.to_string())
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

    /// Issue #471 M3. `(end - start) / step == POINTS_CAP` exactly —
    /// 11,000 step intervals, 11,001 grid points — is SERVED, which is
    /// the boundary `pulsus_read::logql::window`'s
    /// `grid_resolution_fence_serves_11000_intervals_and_rejects_11001`
    /// already pins on the other surface.
    #[test]
    fn check_range_serves_exactly_11000_intervals() {
        let end = POINTS_CAP * 1_000;
        assert!(check_range(0, end, 1_000).is_ok());
    }

    #[test]
    fn check_range_rejects_11001_intervals() {
        let end = (POINTS_CAP + 1) * 1_000;
        let err = check_range(0, end, 1_000).unwrap_err();
        assert!(
            matches!(err, ParamError::MaxResolutionExceeded),
            "expected MaxResolutionExceeded, got {err:?}"
        );
    }

    /// Issue #471 M3, the adversarial pair: 11,000 intervals reached by a
    /// fractional `end` and by a sub-second `step`, so a fix that
    /// special-cases whole-second inputs reddens here.
    #[test]
    fn check_range_serves_11000_intervals_at_a_fractional_end_and_a_subsecond_step() {
        // (11_000_500 - 0) / 1_000 == 11_000 intervals (integer division).
        assert!(check_range(0, 11_000_500, 1_000).is_ok());
        // (5_500_000 - 0) / 500 == 11_000 intervals.
        assert!(check_range(0, 5_500_000, 500).is_ok());
        // (5_500_500 - 0) / 500 == 11_001 intervals -> rejected.
        let err = check_range(0, 5_500_500, 500).unwrap_err();
        assert!(matches!(err, ParamError::MaxResolutionExceeded));
    }

    /// Issue #471 M3: the cap sentence hard-codes `11,000`, so this is
    /// what stops it drifting from the constant it describes.
    #[test]
    fn points_cap_and_the_cap_message_agree() {
        assert_eq!(POINTS_CAP, 11_000);
        assert_eq!(
            ParamError::MaxResolutionExceeded.to_string(),
            "exceeded maximum resolution of 11,000 points per timeseries. \
             Try decreasing the query resolution (?step=XX)"
        );
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

    // -----------------------------------------------------------------
    // Issue #471 M4 — `limit` on the three discovery endpoints
    // -----------------------------------------------------------------

    #[test]
    fn parse_discovery_limit_table() {
        assert_eq!(parse_discovery_limit(None).unwrap(), None);
        assert_eq!(parse_discovery_limit(Some("")).unwrap(), None);
        // `0` means *no limit* here — the opposite of `/metadata`'s rule.
        assert_eq!(parse_discovery_limit(Some("0")).unwrap(), None);
        assert_eq!(parse_discovery_limit(Some("1")).unwrap(), Some(1));
        // A leading `+` is accepted, exactly as on the reference.
        assert_eq!(parse_discovery_limit(Some("+2")).unwrap(), Some(2));
        assert!(matches!(
            parse_discovery_limit(Some("-1")).unwrap_err(),
            ParamError::LimitNegative
        ));
        assert!(matches!(
            parse_discovery_limit(Some("abc")).unwrap_err(),
            ParamError::LimitNotAnInteger(_)
        ));
        assert!(matches!(
            parse_discovery_limit(Some("1.5")).unwrap_err(),
            ParamError::LimitNotAnInteger(_)
        ));
        assert!(matches!(
            parse_discovery_limit(Some("99999999999999999999")).unwrap_err(),
            ParamError::LimitNotAnInteger(_)
        ));
    }

    /// Both rejection strings are asserted as literals, not by status:
    /// one is the reference's own prose and one is deliberately ours.
    #[test]
    fn discovery_limit_rejection_messages_are_the_two_pinned_literals() {
        assert_eq!(
            parse_discovery_limit(Some("-1")).unwrap_err().to_string(),
            "invalid parameter \"limit\": limit must be non-negative"
        );
        assert_eq!(
            parse_discovery_limit(Some("1.5")).unwrap_err().to_string(),
            "invalid parameter \"limit\": cannot parse \"1.5\" to an integer"
        );
    }

    /// `/metadata`'s parser is a different rule and must not be unified:
    /// there `limit=0` means *return nothing*, on both servers.
    #[test]
    fn metadata_limit_still_reads_zero_as_return_nothing() {
        assert_eq!(parse_limit(Some("0")).unwrap(), Some(0));
    }

    // -----------------------------------------------------------------
    // Issue #471 M2 — the `timeout` request parameter
    // -----------------------------------------------------------------

    #[test]
    fn parse_timeout_accepts_the_reference_set() {
        assert_eq!(parse_timeout("60").unwrap().0, Duration::from_secs(60));
        assert_eq!(parse_timeout("1ms").unwrap().0, Duration::from_millis(1));
        assert_eq!(parse_timeout("2m0s").unwrap().0, Duration::from_secs(120));
        assert_eq!(parse_timeout("1m30s").unwrap().0, Duration::from_secs(90));
        assert_eq!(parse_timeout("0.001").unwrap().0, Duration::from_millis(1));
    }

    #[test]
    fn parse_timeout_rejects_everything_outside_it() {
        for raw in ["0", "-1", "-1s", "1ns", "abc", "1e400", "1e300"] {
            let err = parse_timeout(raw).unwrap_err();
            assert!(
                matches!(err, ParamError::InvalidTimeout { .. }),
                "{raw:?} must be InvalidTimeout, got {err:?}"
            );
        }
    }

    /// The runner is where the strictly-shorter rule lives, because the
    /// end-to-end behaviour of the guarded and unguarded forms is
    /// identical: the outer request-deadline layer preempts any
    /// equal-or-longer inner deadline, so no live witness can separate
    /// them.
    #[tokio::test(start_paused = true)]
    async fn run_under_request_deadline_installs_only_a_strictly_shorter_timeout() {
        use std::future::pending;
        use tokio::time::{Duration as TDuration, advance};

        let server = Duration::from_secs(3);

        // Strictly shorter: installed, and the error carries the duration.
        let err = run_under_request_deadline::<(), _>(
            Some(parse_timeout("1ms").unwrap()),
            server,
            pending(),
        )
        .await
        .unwrap_err();
        assert_eq!(err, Duration::from_millis(1));

        let err = run_under_request_deadline::<(), _>(
            Some(parse_timeout("2.999").unwrap()),
            server,
            pending(),
        )
        .await
        .unwrap_err();
        assert_eq!(err, Duration::from_millis(2999));

        // Equal, longer, absent: NOT installed. Two timers on one request
        // would make the observed message a race, and a longer one is
        // preempted by the outer layer anyway.
        //
        // **Each future is polled BEFORE the clock is advanced, and that
        // ordering is the whole test.** `tokio::time::timeout` arms its
        // sleep on the first poll, so advancing a paused clock first and
        // polling once afterwards leaves the deadline in the future and
        // the future Pending whatever the comparison is. Written that way
        // this test stayed green with the guard changed to `<=` —
        // measured, on this exact break.
        for (label, requested) in [
            ("equal", Some(parse_timeout("3").unwrap())),
            ("longer", Some(parse_timeout("10").unwrap())),
            ("absent", None),
        ] {
            let mut fut = Box::pin(run_under_request_deadline::<(), _>(
                requested,
                server,
                pending(),
            ));
            assert!(
                futures::poll!(fut.as_mut()).is_pending(),
                "{label}: the first poll must not resolve"
            );
            advance(TDuration::from_secs(60)).await;
            assert!(
                futures::poll!(fut.as_mut()).is_pending(),
                "{label}: a deadline was installed that must not have been"
            );
        }

        // Work that completes inside a shorter deadline is passed through.
        let ok = run_under_request_deadline(
            Some(parse_timeout("1ms").unwrap()),
            server,
            std::future::ready(7u8),
        )
        .await;
        assert_eq!(ok, Ok(7u8));
    }

    // -----------------------------------------------------------------
    // Issue #471 M6 — `U__` unescaping
    // -----------------------------------------------------------------

    #[test]
    fn unescape_label_name_matches_the_reference_table() {
        let cases: &[(&str, &str)] = &[
            // No prefix: unchanged, whether or not it is legacy-legal.
            ("job", "job"),
            ("a-b", "a-b"),
            // An escaped LEGACY name must answer what the plain name does.
            ("U__job", "job"),
            // Hex is case-insensitive (`_6F_` is `o`).
            ("U__j_6F_b", "job"),
            // An escape at position 0.
            ("U___6a_ob", "job"),
            ("U__a_2d_b", "a-b"),
            // Five hex digits decode.
            ("U__x_1f600_y", "x\u{1f600}y"),
            // SIX hex digits bail out — the whole input comes back.
            ("U__x_10ffff_y", "U__x_10ffff_y"),
            // `U__` alone is the empty string; the caller rejects it.
            ("U__", ""),
            // Malformed escapes return the input unchanged, never error.
            ("U__bad_zz", "U__bad_zz"),
            ("U__x_", "U__x_"),
            // A surrogate is not a Unicode scalar.
            ("U__x_d800_y", "U__x_d800_y"),
            // Only ONE prefix is stripped; the rest is ordinary input.
            ("U__U__job", "U_job"),
            // `:` survives escaping as a valid legacy rune.
            ("U__http_2e_status:code", "http.status:code"),
            // A multi-byte character outside an escape passes through.
            ("U__caf\u{e9}", "caf\u{e9}"),
        ];
        for (input, want) in cases {
            assert_eq!(&unescape_label_name(input), want, "input {input:?}");
        }
    }

    #[test]
    fn empty_label_name_message_is_the_pinned_literal() {
        assert_eq!(
            ParamError::EmptyLabelName.to_string(),
            "invalid label name: \"\""
        );
    }
}
