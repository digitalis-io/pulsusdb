//! `/api/logs/v1` parameter parsing: timestamps, `limit`/`direction`/
//! `step`, and the shared `Vec<(String,String)>` pair core both GET (query
//! string) and POST (`application/x-www-form-urlencoded` body) handlers
//! parse into (issue #13 architect plan amendment §1: "one shared param
//! core over `Vec<(String,String)>` pairs").
//!
//! Percent-decoding is hand-rolled rather than a new dependency
//! (`form_urlencoded`/`serde_urlencoded` are already transitively resolved
//! via `axum`, but pulling either in directly is unnecessary for this small
//! a parser — matches the crate's existing minimal-deps convention, e.g.
//! `middleware::base64_encode`). `serde_urlencoded` (axum's own `Query`/
//! `Form` extractors) is deliberately *not* used here: it cannot collect
//! repeated `match[]=` keys into a `Vec<String>`, which `/series` needs.

use thiserror::Error;

use pulsus_read::{Direction, VolumeAggregateBy};

/// Default `limit` when the param is absent (docs/api.md §2.1).
pub(crate) const DEFAULT_LIMIT: u32 = 100;
/// Hard cap on `limit`; values above this are rejected with `400`, never
/// silently clamped (task-manager resolution #6 on issue #13).
pub(crate) const MAX_LIMIT: u32 = 5000;
/// Cap on the POST-DEDUPE `targetLabels` count (issue #169 plan v2,
/// docs/api.md §2.6): each target injects at most one `.+` matcher before
/// planning, so this bounds the injected matchers/stage-1 OR-branches at
/// 32 on top of the parsed selector — far beyond any real aggregation-key
/// dimensionality, and trivial against the 8 MiB rendered-SQL admission
/// envelope. Rejected `400`, never clamped (the `MAX_LIMIT` discipline);
/// a deliberate deviation from the cap-less oracle (grafana/loki:3.4.2).
pub(crate) const MAX_TARGET_LABELS: usize = 32;
/// Per-entry byte-length cap on a `targetLabels` label name
/// (post-percent-decode) — bounds each injected matcher's escaped SQL
/// fragment (issue #169 plan v2; same 400-not-clamp rule as above).
pub(crate) const MAX_TARGET_LABEL_BYTES: usize = 256;
/// Default lookback window (`end - start`) when `start` is omitted
/// (docs/api.md §2.1: "default: last hour").
const DEFAULT_LOOKBACK_NS: i64 = 3_600_000_000_000;
/// `step`'s target point count when derived rather than supplied
/// (architect plan: "derived `clamp((end-start)/250, >=1s)`").
const DERIVED_STEP_TARGET_POINTS: i64 = 250;
const ONE_SECOND_NS: u64 = 1_000_000_000;

/// Errors from parsing `/api/logs/v1` request parameters — mapped to `400
/// bad_data` by `error::ApiError` (the one exception, `UnsupportedContentType`,
/// still maps to `400`, just for a POST-specific reason).
#[derive(Debug, Error)]
pub(crate) enum ParamError {
    #[error("missing required parameter 'query'")]
    MissingQuery,
    #[error("missing required parameter 'match[]': at least one selector is required")]
    MissingMatch,
    #[error("invalid timestamp {0:?}: expected unix nanoseconds or RFC3339")]
    InvalidTimestamp(String),
    #[error("invalid 'limit' {0:?}: expected a non-negative integer")]
    InvalidLimit(String),
    #[error("'limit' {limit} exceeds the maximum of {max}")]
    LimitTooLarge { limit: u64, max: u32 },
    #[error("invalid 'direction' {0:?}: expected 'forward' or 'backward'")]
    InvalidDirection(String),
    #[error("invalid 'step' {raw:?}: {reason}")]
    InvalidStep { raw: String, reason: String },
    #[error("request body must be application/x-www-form-urlencoded, got {0:?}")]
    UnsupportedContentType(String),
    #[error("request body is not valid UTF-8")]
    InvalidFormBody,
    /// Issue #74: `/tail` and `/stats` take log stream queries only — a
    /// metric expression has no tail frames / stream statistics.
    #[error(
        "'query' must be a log stream selector query: {endpoint} does not support metric queries"
    )]
    MetricQueryUnsupported { endpoint: &'static str },
    /// Issue #74: `/stats` aggregates via pushdown only — parsers/
    /// formats/label filters have no pushdown aggregation shape, so they
    /// are rejected rather than silently over-counting.
    #[error("'query' supports a stream selector plus line filters only on the stats endpoint")]
    StatsPipelineUnsupported,
    /// Issue #74: the tail `limit` (entries per frame) — unlike the
    /// query endpoints' capped `limit`, values above
    /// `reader.tail_max_fetch_limit` are silently clamped, but zero or
    /// non-numeric input is still a 400.
    #[error("invalid 'limit' {0:?}: expected a positive integer")]
    InvalidTailLimit(String),
    /// Issue #74: `delay_for` (seconds tolerated for late arrivals) —
    /// values above `reader.tail_max_delay` are clamped, but non-numeric
    /// input is a 400.
    #[error("invalid 'delay_for' {0:?}: expected a non-negative integer number of seconds")]
    InvalidDelayFor(String),
    /// Issue #169: `/volume` aggregates via the body-content-blind rollup
    /// only — ANY pipeline stage (line filters included, unlike `/stats`)
    /// would silently over-count, so all are rejected.
    #[error("'query' must be a bare stream selector on the volume endpoint (no pipeline stages)")]
    VolumePipelineUnsupported,
    /// Issue #169: `aggregateBy` accepts `series`/`labels` only (oracle
    /// `volumeAggregateBy`).
    #[error("invalid 'aggregateBy' {0:?}: expected 'series' or 'labels'")]
    InvalidAggregateBy(String),
    /// Issue #169: `end < start` is an explicit 400 on the volume
    /// endpoint (oracle `errEndBeforeStart`).
    #[error("invalid time range: 'end' precedes 'start'")]
    EndBeforeStart,
    /// Issue #169 plan v2: the post-dedupe `targetLabels` count cap —
    /// enforced in pure param parsing, BEFORE any AST mutation, planning,
    /// or SQL rendering.
    #[error("too many 'targetLabels': {count} exceeds the maximum of {max}")]
    TooManyTargetLabels { count: usize, max: usize },
    /// Issue #169 plan v2: the per-entry `targetLabels` byte-length cap —
    /// same pre-planning stage as [`ParamError::TooManyTargetLabels`].
    #[error("'targetLabels' entry of {len} bytes exceeds the maximum of {max}")]
    TargetLabelTooLong { len: usize, max: usize },
}

/// Nanoseconds since the Unix epoch, right now. Matches the rest of the
/// workspace's `std::time::SystemTime`-based "now" convention (e.g.
/// `pulsus-read`/`pulsus-schema`'s live test fixtures) rather than
/// `chrono::Utc::now()` — `chrono` here is scoped to RFC3339 *parsing*
/// only (see [`parse_ts`]).
pub(crate) fn now_ns() -> i64 {
    let dur = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(dur.as_nanos()).unwrap_or(i64::MAX)
}

/// Parses a `start`/`end`/`time` timestamp: an integer literal is unix
/// nanoseconds; anything else is parsed as RFC3339 (docs/api.md §2.1's
/// "ns / RFC3339").
pub(crate) fn parse_ts(raw: &str) -> Result<i64, ParamError> {
    if let Ok(ns) = raw.parse::<i64>() {
        return Ok(ns);
    }
    let dt = chrono::DateTime::parse_from_rfc3339(raw)
        .map_err(|_| ParamError::InvalidTimestamp(raw.to_string()))?;
    dt.timestamp_nanos_opt()
        .ok_or_else(|| ParamError::InvalidTimestamp(raw.to_string()))
}

/// `start`'s default when omitted: `end - 1h` (docs/api.md §2.1).
pub(crate) fn default_start_ns(end_ns: i64) -> i64 {
    end_ns.saturating_sub(DEFAULT_LOOKBACK_NS)
}

/// `limit`: default 100, hard cap 5000 — values above the cap are a `400`,
/// never silently clamped (task-manager resolution #6).
pub(crate) fn parse_limit(raw: Option<&str>) -> Result<u32, ParamError> {
    let Some(raw) = raw else {
        return Ok(DEFAULT_LIMIT);
    };
    let n: u64 = raw
        .parse()
        .map_err(|_| ParamError::InvalidLimit(raw.to_string()))?;
    if n > u64::from(MAX_LIMIT) {
        return Err(ParamError::LimitTooLarge {
            limit: n,
            max: MAX_LIMIT,
        });
    }
    // `n <= MAX_LIMIT` (a `u32`) was just checked above, so this narrowing
    // conversion is always exact.
    Ok(n as u32)
}

/// The volume `limit` (issue #169, docs/api.md §2.6): absent **or 0** →
/// [`DEFAULT_LIMIT`] (the oracle's `volumeLimit` resets 0 to its default,
/// unlike [`parse_limit`], where 0 is taken literally); above
/// [`MAX_LIMIT`] → 400, never clamped; non-numeric → 400.
pub(crate) fn parse_volume_limit(raw: Option<&str>) -> Result<u32, ParamError> {
    match parse_limit(raw)? {
        0 => Ok(DEFAULT_LIMIT),
        n => Ok(n),
    }
}

/// `aggregateBy` (issue #169): `series` (default) | `labels`; anything
/// else is a 400 (oracle `volumeAggregateBy`).
pub(crate) fn parse_aggregate_by(raw: Option<&str>) -> Result<VolumeAggregateBy, ParamError> {
    match raw {
        None | Some("series") => Ok(VolumeAggregateBy::Series),
        Some("labels") => Ok(VolumeAggregateBy::Labels),
        Some(other) => Err(ParamError::InvalidAggregateBy(other.to_string())),
    }
}

/// `targetLabels` (issue #169): comma-separated label names. Pinned parse
/// order (plan v2): split on `,` → drop empties → dedupe (order-
/// preserving) → per-entry length cap ([`MAX_TARGET_LABEL_BYTES`]) →
/// post-dedupe count cap ([`MAX_TARGET_LABELS`]). Both caps reject 400
/// here, in PURE param parsing — before any AST mutation, planning, or
/// SQL rendering ever sees the values.
pub(crate) fn parse_target_labels(raw: Option<&str>) -> Result<Vec<String>, ParamError> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    let mut out: Vec<String> = Vec::new();
    for label in raw.split(',') {
        if label.is_empty() || out.iter().any(|seen| seen == label) {
            continue;
        }
        if label.len() > MAX_TARGET_LABEL_BYTES {
            return Err(ParamError::TargetLabelTooLong {
                len: label.len(),
                max: MAX_TARGET_LABEL_BYTES,
            });
        }
        out.push(label.to_string());
        // The post-dedupe count can only grow — reject as soon as it
        // passes the cap (bounds the dedupe scan at cap+1 entries, so a
        // hostile parameter never buys O(n²) work here).
        if out.len() > MAX_TARGET_LABELS {
            return Err(ParamError::TooManyTargetLabels {
                count: out.len(),
                max: MAX_TARGET_LABELS,
            });
        }
    }
    Ok(out)
}

/// `direction`: `forward`|`backward`, default `backward` (docs/api.md
/// §2.1).
pub(crate) fn parse_direction(raw: Option<&str>) -> Result<Direction, ParamError> {
    match raw {
        None | Some("backward") => Ok(Direction::Backward),
        Some("forward") => Ok(Direction::Forward),
        Some(other) => Err(ParamError::InvalidDirection(other.to_string())),
    }
}

/// `step` (query_range, metric queries only): a duration string or a
/// plain-integer number of seconds; absent ⇒ derived
/// `clamp((end-start)/250, >=1s)`; an explicit non-positive step is a
/// `400` (architect plan "Param parsing").
pub(crate) fn parse_step(raw: Option<&str>, start_ns: i64, end_ns: i64) -> Result<u64, ParamError> {
    match raw {
        None => Ok(derive_step_ns(start_ns, end_ns)),
        Some(raw) => {
            let ns = parse_duration_ns(raw)?;
            if ns == 0 {
                return Err(ParamError::InvalidStep {
                    raw: raw.to_string(),
                    reason: "step must be greater than zero".to_string(),
                });
            }
            Ok(ns)
        }
    }
}

fn derive_step_ns(start_ns: i64, end_ns: i64) -> u64 {
    let span_ns = end_ns.saturating_sub(start_ns).max(0);
    // `span_ns >= 0` (just clamped above), so this is a lossless widen.
    let span_ns = span_ns as u64;
    (span_ns / DERIVED_STEP_TARGET_POINTS as u64).max(ONE_SECOND_NS)
}

/// A minimal compound duration parser (`"30s"`, `"1m30s"`, or a bare
/// integer interpreted as seconds — Prometheus's own `step` convention).
/// Self-contained rather than reusing `pulsus-logql`'s duration parser: that
/// parser's `parse_duration` is `pub(crate)` to its own crate (LogQL range
/// literals, `[5m]`, are a distinct grammar element from an HTTP query
/// param).
fn parse_duration_ns(raw: &str) -> Result<u64, ParamError> {
    if let Ok(secs) = raw.parse::<u64>() {
        return secs
            .checked_mul(ONE_SECOND_NS)
            .ok_or_else(|| invalid_step(raw, "step in seconds overflows u64 nanoseconds"));
    }

    const UNITS: &[(&str, u64)] = &[
        ("ns", 1),
        ("us", 1_000),
        ("ms", 1_000_000),
        ("s", ONE_SECOND_NS),
        ("m", 60 * ONE_SECOND_NS),
        ("h", 3_600 * ONE_SECOND_NS),
        ("d", 86_400 * ONE_SECOND_NS),
    ];

    let bytes = raw.as_bytes();
    let mut idx = 0usize;
    let mut total: u64 = 0;
    let mut matched_any = false;
    while idx < bytes.len() {
        let digit_start = idx;
        while bytes.get(idx).is_some_and(u8::is_ascii_digit) {
            idx += 1;
        }
        if idx == digit_start {
            return Err(invalid_step(raw, "expected a number"));
        }
        let number: u64 = raw[digit_start..idx]
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
            .ok_or_else(|| invalid_step(raw, "duration component overflows u64 nanoseconds"))?;
        total = total
            .checked_add(component)
            .ok_or_else(|| invalid_step(raw, "duration overflows u64 nanoseconds"))?;
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

/// Splits an `application/x-www-form-urlencoded` string (GET query string
/// or POST form body — the same wire format) into ordered `(key, value)`
/// pairs. Repeats a key exactly as many times as it appears, so callers
/// needing `match[]`'s repeated-key semantics use [`get_all`] against this
/// output — the reason this crate does not use axum's `Query`/`Form`
/// extractors (`serde_urlencoded` cannot collect repeats into a `Vec`).
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
/// next (e.g. [`parse_ts`]).
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
    fn parse_ts_reads_a_plain_integer_as_unix_nanoseconds() {
        assert_eq!(parse_ts("1234567890").unwrap(), 1_234_567_890);
    }

    #[test]
    fn parse_ts_reads_rfc3339() {
        // 2026-07-01T00:00:00Z.
        assert_eq!(
            parse_ts("2026-07-01T00:00:00Z").unwrap(),
            1_782_864_000_000_000_000
        );
    }

    #[test]
    fn parse_ts_rejects_garbage() {
        let err = parse_ts("not-a-timestamp").unwrap_err();
        assert!(matches!(err, ParamError::InvalidTimestamp(_)));
    }

    #[test]
    fn default_start_ns_is_one_hour_before_end() {
        assert_eq!(default_start_ns(3_600_000_000_000), 0);
    }

    #[test]
    fn parse_limit_defaults_to_100() {
        assert_eq!(parse_limit(None).unwrap(), DEFAULT_LIMIT);
    }

    #[test]
    fn parse_limit_accepts_a_value_at_the_cap() {
        assert_eq!(parse_limit(Some("5000")).unwrap(), 5000);
    }

    #[test]
    fn parse_limit_rejects_a_value_above_the_cap() {
        let err = parse_limit(Some("5001")).unwrap_err();
        assert!(matches!(
            err,
            ParamError::LimitTooLarge {
                limit: 5001,
                max: 5000
            }
        ));
    }

    #[test]
    fn parse_limit_rejects_non_numeric_input() {
        assert!(matches!(
            parse_limit(Some("abc")).unwrap_err(),
            ParamError::InvalidLimit(_)
        ));
    }

    // -- Issue #169: /volume params --------------------------------------

    #[test]
    fn parse_volume_limit_defaults_to_100_when_absent() {
        assert_eq!(parse_volume_limit(None).unwrap(), DEFAULT_LIMIT);
    }

    /// Issue #169 plan v2 test gap (a): the oracle's `volumeLimit` resets
    /// an explicit 0 to the default (100 — `seriesvolume.DefaultLimit`),
    /// unlike the query endpoints' literal `limit=0`.
    #[test]
    fn parse_volume_limit_resets_an_explicit_zero_to_the_default() {
        assert_eq!(parse_volume_limit(Some("0")).unwrap(), 100);
    }

    #[test]
    fn parse_volume_limit_accepts_the_cap_and_rejects_one_above_it() {
        assert_eq!(parse_volume_limit(Some("5000")).unwrap(), 5000);
        assert!(matches!(
            parse_volume_limit(Some("5001")).unwrap_err(),
            ParamError::LimitTooLarge {
                limit: 5001,
                max: 5000
            }
        ));
    }

    #[test]
    fn parse_volume_limit_rejects_non_numeric_input() {
        assert!(matches!(
            parse_volume_limit(Some("abc")).unwrap_err(),
            ParamError::InvalidLimit(_)
        ));
    }

    #[test]
    fn parse_aggregate_by_defaults_to_series_and_accepts_labels() {
        assert_eq!(parse_aggregate_by(None).unwrap(), VolumeAggregateBy::Series);
        assert_eq!(
            parse_aggregate_by(Some("series")).unwrap(),
            VolumeAggregateBy::Series
        );
        assert_eq!(
            parse_aggregate_by(Some("labels")).unwrap(),
            VolumeAggregateBy::Labels
        );
    }

    #[test]
    fn parse_aggregate_by_rejects_anything_else() {
        assert!(matches!(
            parse_aggregate_by(Some("both")).unwrap_err(),
            ParamError::InvalidAggregateBy(_)
        ));
    }

    #[test]
    fn parse_target_labels_absent_is_empty() {
        assert!(parse_target_labels(None).unwrap().is_empty());
    }

    #[test]
    fn parse_target_labels_splits_on_commas_dropping_empties() {
        assert_eq!(
            parse_target_labels(Some(",env,,team,")).unwrap(),
            vec!["env".to_string(), "team".to_string()]
        );
    }

    #[test]
    fn parse_target_labels_dedupes_preserving_first_appearance_order() {
        assert_eq!(
            parse_target_labels(Some("team,env,team,env")).unwrap(),
            vec!["team".to_string(), "env".to_string()]
        );
    }

    /// Issue #169 plan v2 boundary: exactly [`MAX_TARGET_LABELS`]
    /// post-dedupe targets pass; one more is a 400.
    #[test]
    fn parse_target_labels_accepts_exactly_the_count_cap_and_rejects_one_more() {
        let at_cap = (0..MAX_TARGET_LABELS)
            .map(|i| format!("l{i}"))
            .collect::<Vec<_>>()
            .join(",");
        assert_eq!(
            parse_target_labels(Some(&at_cap)).unwrap().len(),
            MAX_TARGET_LABELS
        );
        let over = format!("{at_cap},one_more");
        assert!(matches!(
            parse_target_labels(Some(&over)).unwrap_err(),
            ParamError::TooManyTargetLabels { count: 33, max: 32 }
        ));
    }

    /// Issue #169 plan v2 boundary: exactly [`MAX_TARGET_LABEL_BYTES`]
    /// bytes pass; one more byte is a 400.
    #[test]
    fn parse_target_labels_accepts_exactly_the_length_cap_and_rejects_one_more_byte() {
        let at_cap = "x".repeat(MAX_TARGET_LABEL_BYTES);
        assert_eq!(parse_target_labels(Some(&at_cap)).unwrap(), vec![at_cap]);
        let over = "x".repeat(MAX_TARGET_LABEL_BYTES + 1);
        assert!(matches!(
            parse_target_labels(Some(&over)).unwrap_err(),
            ParamError::TargetLabelTooLong { len: 257, max: 256 }
        ));
    }

    /// Issue #169 plan v2 pin: dedupe runs BEFORE the count cap — 10k
    /// duplicates of one label collapse to 1 and pass.
    #[test]
    fn parse_target_labels_dedupes_before_the_count_cap() {
        let raw = vec!["env"; 10_000].join(",");
        assert_eq!(
            parse_target_labels(Some(&raw)).unwrap(),
            vec!["env".to_string()]
        );
    }

    #[test]
    fn parse_direction_defaults_to_backward() {
        assert_eq!(parse_direction(None).unwrap(), Direction::Backward);
    }

    #[test]
    fn parse_direction_accepts_forward_and_backward() {
        assert_eq!(
            parse_direction(Some("forward")).unwrap(),
            Direction::Forward
        );
        assert_eq!(
            parse_direction(Some("backward")).unwrap(),
            Direction::Backward
        );
    }

    #[test]
    fn parse_direction_rejects_anything_else() {
        assert!(matches!(
            parse_direction(Some("sideways")).unwrap_err(),
            ParamError::InvalidDirection(_)
        ));
    }

    #[test]
    fn parse_step_derives_from_the_window_when_absent() {
        // A 2500s window / 250 = 10s.
        let step = parse_step(None, 0, 2_500_000_000_000).unwrap();
        assert_eq!(step, 10_000_000_000);
    }

    #[test]
    fn parse_step_clamps_the_derived_value_to_at_least_one_second() {
        // A tiny window derives well under 1s; must clamp up.
        let step = parse_step(None, 0, 1_000_000_000).unwrap();
        assert_eq!(step, ONE_SECOND_NS);
    }

    #[test]
    fn parse_step_accepts_a_bare_integer_as_seconds() {
        assert_eq!(parse_step(Some("30"), 0, 0).unwrap(), 30_000_000_000);
    }

    #[test]
    fn parse_step_accepts_a_compound_duration_literal() {
        assert_eq!(parse_step(Some("1m30s"), 0, 0).unwrap(), 90_000_000_000);
    }

    #[test]
    fn parse_step_rejects_zero() {
        let err = parse_step(Some("0"), 0, 0).unwrap_err();
        assert!(matches!(err, ParamError::InvalidStep { .. }));
    }

    #[test]
    fn parse_step_rejects_garbage() {
        let err = parse_step(Some("banana"), 0, 0).unwrap_err();
        assert!(matches!(err, ParamError::InvalidStep { .. }));
    }

    #[test]
    fn parse_pairs_splits_and_decodes_a_query_string() {
        let pairs = parse_pairs("query=%7Bapp%3D%22x%22%7D&limit=10");
        assert_eq!(
            pairs,
            vec![
                ("query".to_string(), r#"{app="x"}"#.to_string()),
                ("limit".to_string(), "10".to_string()),
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
        let pairs = parse_pairs("match%5B%5D=%7Ba%3D%22x%22%7D&match%5B%5D=%7Bb%3D%22y%22%7D");
        let values = get_all(&pairs, "match[]");
        assert_eq!(values, vec![r#"{a="x"}"#, r#"{b="y"}"#]);
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
}
