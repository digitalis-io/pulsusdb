//! `/api/traces/v1` parameter parsing: the trace-fetch hex id
//! (docs/api.md §4.1: "16 or 32 chars, left-padded" — the injection
//! boundary for `point_read_sql`'s `unhex('...')` literal: only
//! `[0-9a-f]{32}` output ever leaves [`parse_trace_id`]) and the issue
//! #57 search params (docs/api.md §4.2: `q`/legacy params, `start`/`end`
//! unix seconds, `limit`, `spss`). The `(key, value)` pair core mirrors
//! `logs_api`/`prom_api`'s per-surface copies (each module owns its
//! params, established convention).

use thiserror::Error;

/// Errors from parsing the `{traceId}` path parameter — mapped to `400
/// bad_data` by `error::ApiError`.
#[derive(Debug, Error)]
pub(crate) enum TraceIdError {
    #[error("invalid trace id {0:?}: expected 16 or 32 hex characters")]
    InvalidLength(String),
    #[error("invalid trace id {0:?}: expected hex characters only")]
    NotHex(String),
}

/// Parses `raw` into the canonical 32-char lowercase hex trace id:
/// 32 hex chars pass through (lowercased); 16 hex chars are left-padded
/// with 16 `'0'`s (a 64-bit id in a 128-bit field, docs/api.md §4.1);
/// anything else — empty, odd, wrong length, non-hex — is rejected.
pub(crate) fn parse_trace_id(raw: &str) -> Result<String, TraceIdError> {
    if raw.len() != 16 && raw.len() != 32 {
        return Err(TraceIdError::InvalidLength(raw.to_string()));
    }
    if !raw.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(TraceIdError::NotHex(raw.to_string()));
    }
    let lowered = raw.to_ascii_lowercase();
    if lowered.len() == 16 {
        Ok(format!("0000000000000000{lowered}"))
    } else {
        Ok(lowered)
    }
}

/// Default `limit` when the param is absent (docs/api.md §4.2).
pub(crate) const DEFAULT_LIMIT: u32 = 20;
/// Default `spss` (spans-per-spanset cap) when the param is absent.
pub(crate) const DEFAULT_SPSS: u32 = 3;

/// Errors from parsing `/api/traces/v1/search` request parameters —
/// mapped to `400 bad_data` by `error::ApiError`.
#[derive(Debug, Error)]
pub(crate) enum SearchParamError {
    #[error("missing required parameter {0:?}: start and end are required")]
    MissingRange(&'static str),
    #[error("invalid timestamp {0:?}: expected unix seconds, unix nanoseconds, or RFC3339")]
    InvalidTimestamp(String),
    #[error("invalid range: end ({end}) must be greater than start ({start})")]
    InvalidRange { start: i64, end: i64 },
    #[error("invalid {name:?} {raw:?}: expected a positive integer")]
    InvalidCount { name: &'static str, raw: String },
    #[error(
        "'q' and the legacy search params (tags/minDuration/maxDuration) are mutually \
         exclusive: supply one or the other, never both"
    )]
    ConflictingQuery,
}

/// The raw, percent-decoded search request — `q` XOR the legacy params
/// (validated here; compilation of either into a TraceQL AST is
/// `search.rs`/`legacy.rs`'s job).
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RawSearchParams {
    pub q: Option<String>,
    pub tags: Option<String>,
    pub min_duration: Option<String>,
    pub max_duration: Option<String>,
    pub start_ns: i64,
    pub end_ns: i64,
    pub limit: u32,
    pub spss: u32,
}

/// Parses the search query string (docs/api.md §4.2). `q` present with
/// any legacy param present is an explicit `400` — never silent
/// precedence (task-manager ratification on plan v2).
pub(crate) fn parse_search_params(raw: &str) -> Result<RawSearchParams, SearchParamError> {
    let pairs = parse_pairs(raw);
    let q = get(&pairs, "q")
        .map(str::to_string)
        .filter(|s| !s.is_empty());
    let tags = get(&pairs, "tags")
        .map(str::to_string)
        .filter(|s| !s.is_empty());
    let min_duration = get(&pairs, "minDuration")
        .map(str::to_string)
        .filter(|s| !s.is_empty());
    let max_duration = get(&pairs, "maxDuration")
        .map(str::to_string)
        .filter(|s| !s.is_empty());
    if q.is_some() && (tags.is_some() || min_duration.is_some() || max_duration.is_some()) {
        return Err(SearchParamError::ConflictingQuery);
    }
    let start_ns = parse_unix_seconds_ns(&pairs, "start")?;
    let end_ns = parse_unix_seconds_ns(&pairs, "end")?;
    if end_ns <= start_ns {
        return Err(SearchParamError::InvalidRange {
            start: start_ns / 1_000_000_000,
            end: end_ns / 1_000_000_000,
        });
    }
    let limit = parse_count(&pairs, "limit", DEFAULT_LIMIT)?;
    let spss = parse_count(&pairs, "spss", DEFAULT_SPSS)?;
    Ok(RawSearchParams {
        q,
        tags,
        min_duration,
        max_duration,
        start_ns,
        end_ns,
        limit,
        spss,
    })
}

/// The one trace-surface timestamp grammar (docs/api.md §1: "trace APIs
/// accept unix seconds/nanoseconds/RFC3339"), shared by the search and
/// metrics parsers so the two endpoints can never drift:
///
/// - an integer literal with magnitude `>= 10^12` is unix **nanoseconds**
///   (10^12 s is the year 33658 — no realistic seconds value; 10^12 ns is
///   16 minutes past the 1970 epoch — no realistic trace timestamp loses
///   the disambiguation);
/// - any other integer literal is unix **seconds**;
/// - anything else parses as **RFC3339** (`chrono`, the `logs_api`
///   precedent — scoped to parsing only).
fn parse_timestamp_ns(raw: &str) -> Option<i64> {
    if let Ok(v) = raw.parse::<i64>() {
        return if v.unsigned_abs() >= 1_000_000_000_000 {
            Some(v)
        } else {
            v.checked_mul(1_000_000_000)
        };
    }
    let dt = chrono::DateTime::parse_from_rfc3339(raw).ok()?;
    dt.timestamp_nanos_opt()
}

fn parse_unix_seconds_ns(
    pairs: &[(String, String)],
    name: &'static str,
) -> Result<i64, SearchParamError> {
    let raw = get(pairs, name).ok_or(SearchParamError::MissingRange(name))?;
    parse_timestamp_ns(raw).ok_or_else(|| SearchParamError::InvalidTimestamp(raw.to_string()))
}

fn parse_count(
    pairs: &[(String, String)],
    name: &'static str,
    default: u32,
) -> Result<u32, SearchParamError> {
    let Some(raw) = get(pairs, name) else {
        return Ok(default);
    };
    raw.parse::<u32>()
        .ok()
        .filter(|n| *n >= 1)
        .ok_or_else(|| SearchParamError::InvalidCount {
            name,
            raw: raw.to_string(),
        })
}

/// Errors from parsing `/api/traces/v1/metrics/{query_range,query}`
/// request parameters (issue #59, docs/api.md §4.4) — mapped to `400
/// bad_data` by `error::ApiError`.
#[derive(Debug, Error)]
pub(crate) enum MetricsParamError {
    #[error("missing required parameter: 'q' (or 'query') must carry a TraceQL metrics expression")]
    MissingQuery,
    #[error("'q' and 'query' are aliases: supply one or the other, never both")]
    ConflictingQueryKeys,
    #[error(
        "'since' and 'start'/'end' are mutually exclusive: supply a relative window or an \
         absolute one, never both"
    )]
    ConflictingRange,
    #[error("missing required parameters: supply start and end (unix seconds), or since")]
    MissingRange,
    #[error("invalid timestamp {0:?}: expected unix seconds, unix nanoseconds, or RFC3339")]
    InvalidTimestamp(String),
    #[error("invalid range: end ({end}) must be greater than start ({start})")]
    InvalidRange { start: i64, end: i64 },
    #[error("invalid 'since' {0:?}: expected a whole-second duration (e.g. 1h, 30m, 90s)")]
    InvalidSince(String),
    #[error("invalid 'step' {0:?}: expected positive whole seconds (e.g. 60, 60s, 5m, 1h)")]
    InvalidStep(String),
}

/// The parsed metrics request: the TraceQL expression plus the validated
/// window and step. `step_s` is already defaulted via the committed
/// derivation formula (docs/api.md §4.4) when the request omitted `step`.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RawMetricsParams {
    pub q: String,
    pub start_ns: i64,
    pub end_ns: i64,
    pub step_s: i64,
}

/// Parses the metrics query string (docs/api.md §4.4). `now_s` feeds the
/// `since` relative window (injected for testability). `q`/`query` are
/// strict aliases (both present → 400); `since` conflicts with
/// `start`/`end` (never silent precedence — the search surface's
/// ratified rule); `step` accepts positive whole seconds (`60`) or
/// whole-second duration forms (`60s`, `5m`, `1h`, `60000ms`) —
/// non-positive or fractional-second steps are explicit 400s. When
/// `step` is omitted: `step_s = max(1, ⌊(end_s − start_s) /
/// DEFAULT_METRICS_POINTS⌋)` (the committed contract; the point cap is
/// enforced downstream by `plan_trace_metrics`).
pub(crate) fn parse_metrics_params(
    raw: &str,
    now_s: i64,
) -> Result<RawMetricsParams, MetricsParamError> {
    let pairs = parse_pairs(raw);
    let q_key = get(&pairs, "q").filter(|s| !s.is_empty());
    let query_key = get(&pairs, "query").filter(|s| !s.is_empty());
    let q = match (q_key, query_key) {
        (Some(_), Some(_)) => return Err(MetricsParamError::ConflictingQueryKeys),
        (Some(q), None) | (None, Some(q)) => q.to_string(),
        (None, None) => return Err(MetricsParamError::MissingQuery),
    };

    let start = parse_opt_unix_seconds_ns(&pairs, "start")?;
    let end = parse_opt_unix_seconds_ns(&pairs, "end")?;
    let since = get(&pairs, "since").filter(|s| !s.is_empty());
    let (start_ns, end_ns) = match (since, start, end) {
        (Some(_), Some(_), _) | (Some(_), _, Some(_)) => {
            return Err(MetricsParamError::ConflictingRange);
        }
        (Some(raw_since), None, None) => {
            let since_s = parse_whole_seconds(raw_since)
                .ok_or_else(|| MetricsParamError::InvalidSince(raw_since.to_string()))?;
            let end_ns = now_s
                .checked_mul(1_000_000_000)
                .ok_or_else(|| MetricsParamError::InvalidSince(raw_since.to_string()))?;
            let start_ns = now_s
                .checked_sub(since_s)
                .and_then(|s| s.checked_mul(1_000_000_000))
                .ok_or_else(|| MetricsParamError::InvalidSince(raw_since.to_string()))?;
            (start_ns, end_ns)
        }
        (None, Some(start_ns), Some(end_ns)) => (start_ns, end_ns),
        (None, _, _) => return Err(MetricsParamError::MissingRange),
    };
    if end_ns <= start_ns {
        return Err(MetricsParamError::InvalidRange {
            start: start_ns / 1_000_000_000,
            end: end_ns / 1_000_000_000,
        });
    }

    let step_s = match get(&pairs, "step").filter(|s| !s.is_empty()) {
        Some(raw_step) => parse_whole_seconds(raw_step)
            .ok_or_else(|| MetricsParamError::InvalidStep(raw_step.to_string()))?,
        // The committed derivation formula (docs/api.md §4.4).
        None => {
            // i128: with ns-precision endpoints the i64 width can wrap
            // (code review round 1) — the derived step must stay exact so
            // the planner's static point cap sees the true bucket count.
            let span_s =
                (i128::from(end_ns) - i128::from(start_ns)) / i128::from(1_000_000_000_i64);
            let step = (span_s / i128::from(pulsus_read::DEFAULT_METRICS_POINTS)).max(1);
            i64::try_from(step).unwrap_or(i64::MAX)
        }
    };

    Ok(RawMetricsParams {
        q,
        start_ns,
        end_ns,
        step_s,
    })
}

fn parse_opt_unix_seconds_ns(
    pairs: &[(String, String)],
    name: &str,
) -> Result<Option<i64>, MetricsParamError> {
    let Some(raw) = get(pairs, name).filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    // The shared trace-surface grammar (s / ns / RFC3339) — one parser
    // for search and metrics alike.
    parse_timestamp_ns(raw)
        .map(Some)
        .ok_or_else(|| MetricsParamError::InvalidTimestamp(raw.to_string()))
}

/// Parses a positive whole-second count: bare digits (seconds) or a
/// whole-second duration suffix form (`s`/`m`/`h`, or `ms` divisible by
/// 1000). Anything else — zero, negative, fractional seconds (`1.5`,
/// `500ms`), unknown units — is `None`.
fn parse_whole_seconds(raw: &str) -> Option<i64> {
    let (digits, unit_multiplier_ms) = if let Some(n) = raw.strip_suffix("ms") {
        (n, 1)
    } else if let Some(n) = raw.strip_suffix('s') {
        (n, 1_000)
    } else if let Some(n) = raw.strip_suffix('m') {
        (n, 60_000)
    } else if let Some(n) = raw.strip_suffix('h') {
        (n, 3_600_000)
    } else {
        (raw, 1_000)
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let value: i64 = digits.parse().ok()?;
    let ms = value.checked_mul(unit_multiplier_ms)?;
    if ms <= 0 || ms % 1_000 != 0 {
        return None;
    }
    Some(ms / 1_000)
}

/// Errors from parsing `/api/traces/v1/service_graph` request parameters
/// (issue #173, docs/api.md §4.5) — mapped to `400 bad_data` by
/// `error::ApiError`. Same window grammar as the metrics surface (`start`/
/// `end`/`since`), minus `q`/`step` — the service graph is a fixed
/// aggregation over a window, with no expression and no bucketing.
#[derive(Debug, Error)]
pub(crate) enum GraphParamError {
    #[error(
        "'since' and 'start'/'end' are mutually exclusive: supply a relative window or an \
         absolute one, never both"
    )]
    ConflictingRange,
    #[error("missing required parameters: supply start and end (unix seconds), or since")]
    MissingRange,
    #[error("invalid timestamp {0:?}: expected unix seconds, unix nanoseconds, or RFC3339")]
    InvalidTimestamp(String),
    #[error("invalid range: end ({end}) must be greater than start ({start})")]
    InvalidRange { start: i64, end: i64 },
    #[error("invalid 'since' {0:?}: expected a whole-second duration (e.g. 1h, 30m, 90s)")]
    InvalidSince(String),
}

/// The parsed service-graph request: just the validated window (issue
/// #173). No expression, no step — the read is a fixed
/// `(client, server, conn_type)` aggregation over `[start_ns, end_ns)`.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RawGraphParams {
    pub start_ns: i64,
    pub end_ns: i64,
}

/// Parses the service-graph query string (docs/api.md §4.5). Reuses the
/// metrics surface's window grammar exactly: `start`/`end` (unix s/ns/
/// RFC3339) XOR a relative `since` (whole-second duration), `now_s` feeding
/// the `since` window (injected for testability). Any missing/invalid/
/// conflicting window is an explicit `400 bad_data`.
pub(crate) fn parse_graph_params(raw: &str, now_s: i64) -> Result<RawGraphParams, GraphParamError> {
    let pairs = parse_pairs(raw);
    let parse_ts = |name: &str| -> Result<Option<i64>, GraphParamError> {
        let Some(raw) = get(&pairs, name).filter(|s| !s.is_empty()) else {
            return Ok(None);
        };
        parse_timestamp_ns(raw)
            .map(Some)
            .ok_or_else(|| GraphParamError::InvalidTimestamp(raw.to_string()))
    };
    let start = parse_ts("start")?;
    let end = parse_ts("end")?;
    let since = get(&pairs, "since").filter(|s| !s.is_empty());
    let (start_ns, end_ns) = match (since, start, end) {
        (Some(_), Some(_), _) | (Some(_), _, Some(_)) => {
            return Err(GraphParamError::ConflictingRange);
        }
        (Some(raw_since), None, None) => {
            let since_s = parse_whole_seconds(raw_since)
                .ok_or_else(|| GraphParamError::InvalidSince(raw_since.to_string()))?;
            let end_ns = now_s
                .checked_mul(1_000_000_000)
                .ok_or_else(|| GraphParamError::InvalidSince(raw_since.to_string()))?;
            let start_ns = now_s
                .checked_sub(since_s)
                .and_then(|s| s.checked_mul(1_000_000_000))
                .ok_or_else(|| GraphParamError::InvalidSince(raw_since.to_string()))?;
            (start_ns, end_ns)
        }
        (None, Some(start_ns), Some(end_ns)) => (start_ns, end_ns),
        (None, _, _) => return Err(GraphParamError::MissingRange),
    };
    if end_ns <= start_ns {
        return Err(GraphParamError::InvalidRange {
            start: start_ns / 1_000_000_000,
            end: end_ns / 1_000_000_000,
        });
    }
    Ok(RawGraphParams { start_ns, end_ns })
}

/// Errors from parsing the `/api/traces/v1/tags` query parameters —
/// mapped to `400 bad_data` by `error::ApiError` (issue #58).
#[derive(Debug, Error)]
pub(crate) enum TagsParamError {
    #[error(
        "unsupported scope {0:?}: expected \"resource\" or \"span\" (or omit the parameter \
         for both scopes)"
    )]
    UnsupportedScope(String),
}

/// Errors from parsing the `{tag}` path parameter of
/// `/api/traces/v1/tag/{tag}/values` — mapped to `400 bad_data`.
#[derive(Debug, Error)]
pub(crate) enum TagPathError {
    #[error("invalid tag: the attribute key must be non-empty")]
    EmptyKey,
}

/// The parsed `/api/traces/v1/tags` request: only `scope` filters.
/// `start`/`end` are accepted for client compatibility and IGNORED —
/// `trace_tag_catalog` has no timestamp column, so tag discovery is
/// time-less by contract (docs/api.md §4.3, issue #58 frozen-schema
/// resolution); any other parameter is likewise ignored.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct TagsParams {
    pub scope: Option<String>,
}

/// Parses the `/api/traces/v1/tags` query string (docs/api.md §4.3):
/// `scope` ∈ {`resource`, `span`, absent} — anything else (including
/// `intrinsic`/`none` and the empty string) is an explicit `400`, never
/// silently widened to "all scopes" (task-manager adjudication 4 on
/// issue #58).
pub(crate) fn parse_tags_params(raw: &str) -> Result<TagsParams, TagsParamError> {
    let pairs = parse_pairs(raw);
    let scope = match get(&pairs, "scope") {
        None => None,
        Some(s @ ("resource" | "span")) => Some(s.to_string()),
        Some(other) => return Err(TagsParamError::UnsupportedScope(other.to_string())),
    };
    Ok(TagsParams { scope })
}

/// Splits the `{tag}` path parameter into `(scope, key)` (docs/api.md
/// §4.3): a `resource.`/`span.` prefix scopes the lookup; a leading `.`
/// or a bare key is unscoped (both scopes). The remainder after the
/// prefix is the verbatim attribute key (`resource.service.name` →
/// scope `resource`, key `service.name`); an empty remainder is a `400`.
pub(crate) fn parse_tag_path(raw_tag: &str) -> Result<(Option<String>, String), TagPathError> {
    let (scope, key) = if let Some(key) = raw_tag.strip_prefix("resource.") {
        (Some("resource".to_string()), key)
    } else if let Some(key) = raw_tag.strip_prefix("span.") {
        (Some("span".to_string()), key)
    } else if let Some(key) = raw_tag.strip_prefix('.') {
        (None, key)
    } else {
        (None, raw_tag)
    };
    if key.is_empty() {
        return Err(TagPathError::EmptyKey);
    }
    Ok((scope, key.to_string()))
}

/// Splits a query string into ordered, percent-decoded `(key, value)`
/// pairs — the same per-surface pair core as `logs_api`/`prom_api`.
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

/// `application/x-www-form-urlencoded` percent-decoding: `+` → space,
/// `%XX` → the raw byte; malformed escapes pass through best-effort
/// (same behavior as the logs/prom copies).
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
    fn a_32_char_lowercase_hex_id_passes_through_unchanged() {
        assert_eq!(
            parse_trace_id("4bf92f3577b34da6a3ce929d0e0e4736").unwrap(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
    }

    #[test]
    fn a_16_char_hex_id_is_left_padded_with_16_zeros() {
        assert_eq!(
            parse_trace_id("a3ce929d0e0e4736").unwrap(),
            "0000000000000000a3ce929d0e0e4736"
        );
    }

    #[test]
    fn mixed_case_hex_is_lowercased() {
        assert_eq!(
            parse_trace_id("4BF92F3577B34DA6A3CE929D0E0E4736").unwrap(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
    }

    #[test]
    fn an_odd_length_id_is_rejected() {
        assert!(matches!(
            parse_trace_id("abc"),
            Err(TraceIdError::InvalidLength(_))
        ));
    }

    #[test]
    fn a_non_hex_id_of_valid_length_is_rejected() {
        assert!(matches!(
            parse_trace_id("zzzzzzzzzzzzzzzz"),
            Err(TraceIdError::NotHex(_))
        ));
    }

    #[test]
    fn an_empty_id_is_rejected() {
        assert!(matches!(
            parse_trace_id(""),
            Err(TraceIdError::InvalidLength(_))
        ));
    }

    #[test]
    fn a_too_long_id_is_rejected() {
        let raw = "4bf92f3577b34da6a3ce929d0e0e47360";
        assert!(matches!(
            parse_trace_id(raw),
            Err(TraceIdError::InvalidLength(_))
        ));
    }

    #[test]
    fn a_between_lengths_id_is_rejected() {
        assert!(matches!(
            parse_trace_id("a3ce929d0e0e47361"), // 17 chars
            Err(TraceIdError::InvalidLength(_))
        ));
    }

    // -- search params ---------------------------------------------------

    #[test]
    fn a_minimal_q_request_parses_with_defaults() {
        let p = parse_search_params("q=%7B%7D&start=100&end=200").unwrap();
        assert_eq!(p.q.as_deref(), Some("{}"));
        assert_eq!(p.start_ns, 100_000_000_000);
        assert_eq!(p.end_ns, 200_000_000_000);
        assert_eq!(p.limit, DEFAULT_LIMIT);
        assert_eq!(p.spss, DEFAULT_SPSS);
    }

    #[test]
    fn missing_start_or_end_is_rejected() {
        assert!(matches!(
            parse_search_params("q=%7B%7D&end=200"),
            Err(SearchParamError::MissingRange("start"))
        ));
        assert!(matches!(
            parse_search_params("q=%7B%7D&start=100"),
            Err(SearchParamError::MissingRange("end"))
        ));
    }

    #[test]
    fn unparseable_start_is_rejected() {
        assert!(matches!(
            parse_search_params("start=abc&end=200"),
            Err(SearchParamError::InvalidTimestamp(_))
        ));
        // A malformed RFC3339 string is the same rejection.
        assert!(matches!(
            parse_search_params("start=2023-11-14T99:99:99Z&end=200"),
            Err(SearchParamError::InvalidTimestamp(_))
        ));
    }

    // -- the shared trace-surface timestamp grammar (docs/api.md §1:
    //    unix s / ns / RFC3339; code review round 1 on issue #59) --------

    #[test]
    fn timestamp_grammar_accepts_seconds_nanoseconds_and_rfc3339() {
        // Seconds: below the 10^12 magnitude threshold.
        assert_eq!(
            parse_timestamp_ns("1700000000"),
            Some(1_700_000_000_000_000_000)
        );
        assert_eq!(parse_timestamp_ns("-100"), Some(-100_000_000_000));
        // Nanoseconds: at/above the threshold, passed through verbatim.
        assert_eq!(
            parse_timestamp_ns("1700000000000000000"),
            Some(1_700_000_000_000_000_000)
        );
        assert_eq!(parse_timestamp_ns("1000000000000"), Some(1_000_000_000_000));
        assert_eq!(
            parse_timestamp_ns(&i64::MAX.to_string()),
            Some(i64::MAX),
            "i64::MAX is nanoseconds, never a wrapping seconds multiply"
        );
        assert_eq!(parse_timestamp_ns(&i64::MIN.to_string()), Some(i64::MIN));
        // RFC3339: Z, offset, and fractional forms.
        assert_eq!(
            parse_timestamp_ns("2023-11-14T22:13:20Z"),
            Some(1_700_000_000_000_000_000)
        );
        assert_eq!(
            parse_timestamp_ns("2023-11-15T00:13:20+02:00"),
            Some(1_700_000_000_000_000_000)
        );
        assert_eq!(
            parse_timestamp_ns("2023-11-14T22:13:20.5Z"),
            Some(1_700_000_000_500_000_000)
        );
        // Rejects: garbage, empty, out-of-range calendar fields.
        for raw in ["abc", "", "2023-13-99T00:00:00Z", "12:30:00"] {
            assert_eq!(parse_timestamp_ns(raw), None, "{raw:?}");
        }
    }

    #[test]
    fn search_accepts_nanosecond_and_rfc3339_bounds() {
        let p = parse_search_params("q=%7B%7D&start=1700000000000000000&end=2023-11-14T23:13:20Z")
            .unwrap();
        assert_eq!(p.start_ns, 1_700_000_000_000_000_000);
        assert_eq!(p.end_ns, 1_700_003_600_000_000_000);
    }

    #[test]
    fn an_inverted_range_is_rejected() {
        assert!(matches!(
            parse_search_params("start=200&end=100"),
            Err(SearchParamError::InvalidRange { .. })
        ));
    }

    #[test]
    fn non_numeric_or_zero_limit_and_spss_are_rejected() {
        for query in [
            "start=1&end=2&limit=abc",
            "start=1&end=2&limit=0",
            "start=1&end=2&spss=abc",
            "start=1&end=2&spss=0",
        ] {
            assert!(
                matches!(
                    parse_search_params(query),
                    Err(SearchParamError::InvalidCount { .. })
                ),
                "{query} must be rejected"
            );
        }
    }

    #[test]
    fn q_together_with_any_legacy_param_is_a_conflict_never_precedence() {
        for query in [
            "q=%7B%7D&tags=a%3Db&start=1&end=2",
            "q=%7B%7D&minDuration=100ms&start=1&end=2",
            "q=%7B%7D&maxDuration=1s&start=1&end=2",
        ] {
            assert!(
                matches!(
                    parse_search_params(query),
                    Err(SearchParamError::ConflictingQuery)
                ),
                "{query} must conflict"
            );
        }
    }

    #[test]
    fn a_pure_legacy_request_parses() {
        let p =
            parse_search_params("tags=http.method%3DGET&minDuration=100ms&start=1&end=2").unwrap();
        assert_eq!(p.q, None);
        assert_eq!(p.tags.as_deref(), Some("http.method=GET"));
        assert_eq!(p.min_duration.as_deref(), Some("100ms"));
    }

    #[test]
    fn an_empty_request_is_a_time_only_search_not_an_error() {
        let p = parse_search_params("start=1&end=2").unwrap();
        assert_eq!(p.q, None);
        assert_eq!(p.tags, None);
    }

    // -- metrics params (issue #59) ------------------------------------------

    const NOW_S: i64 = 1_700_000_000;

    #[test]
    fn a_minimal_metrics_request_parses_with_the_derived_step() {
        let p = parse_metrics_params(
            "q=%7B%7D%20%7C%20rate()&start=1700000000&end=1700003600",
            NOW_S,
        )
        .unwrap();
        assert_eq!(p.q, "{} | rate()");
        assert_eq!(p.start_ns, 1_700_000_000_000_000_000);
        assert_eq!(p.end_ns, 1_700_003_600_000_000_000);
        // Derivation: max(1, 3600 / DEFAULT_METRICS_POINTS(100)) = 36.
        assert_eq!(p.step_s, 36);
    }

    #[test]
    fn a_short_window_derives_the_one_second_step_floor() {
        let p = parse_metrics_params("q=%7B%7D&start=100&end=110", NOW_S).unwrap();
        assert_eq!(p.step_s, 1);
    }

    #[test]
    fn the_query_alias_key_is_accepted_and_the_pair_conflicts() {
        let p = parse_metrics_params("query=%7B%7D&start=1&end=2", NOW_S).unwrap();
        assert_eq!(p.q, "{}");
        assert!(matches!(
            parse_metrics_params("q=%7B%7D&query=%7B%7D&start=1&end=2", NOW_S),
            Err(MetricsParamError::ConflictingQueryKeys)
        ));
    }

    #[test]
    fn a_missing_query_expression_is_rejected() {
        for raw in ["start=1&end=2", "q=&start=1&end=2"] {
            assert!(
                matches!(
                    parse_metrics_params(raw, NOW_S),
                    Err(MetricsParamError::MissingQuery)
                ),
                "{raw} must be rejected"
            );
        }
    }

    #[test]
    fn explicit_step_forms_parse_to_whole_seconds() {
        for (raw, expected) in [
            ("60", 60),
            ("60s", 60),
            ("5m", 300),
            ("1h", 3_600),
            ("60000ms", 60),
        ] {
            let p = parse_metrics_params(&format!("q=%7B%7D&start=1&end=7201&step={raw}"), NOW_S)
                .unwrap();
            assert_eq!(p.step_s, expected, "step={raw}");
        }
    }

    #[test]
    fn non_positive_or_fractional_second_steps_are_rejected() {
        for raw in ["0", "0s", "abc", "-60", "1.5", "500ms", "1500ms", "2d", ""] {
            let query = format!("q=%7B%7D&start=1&end=7201&step={raw}");
            let result = parse_metrics_params(&query, NOW_S);
            if raw.is_empty() {
                // An empty step falls back to derivation, not an error.
                assert!(result.is_ok());
            } else {
                assert!(
                    matches!(result, Err(MetricsParamError::InvalidStep(_))),
                    "step={raw} must be rejected, got {result:?}"
                );
            }
        }
    }

    #[test]
    fn since_derives_the_window_from_now_and_conflicts_with_absolute_bounds() {
        let p = parse_metrics_params("q=%7B%7D&since=1h", NOW_S).unwrap();
        assert_eq!(p.start_ns, (NOW_S - 3_600) * 1_000_000_000);
        assert_eq!(p.end_ns, NOW_S * 1_000_000_000);
        for raw in [
            "q=%7B%7D&since=1h&start=1",
            "q=%7B%7D&since=1h&end=2",
            "q=%7B%7D&since=1h&start=1&end=2",
        ] {
            assert!(
                matches!(
                    parse_metrics_params(raw, NOW_S),
                    Err(MetricsParamError::ConflictingRange)
                ),
                "{raw} must conflict"
            );
        }
        assert!(matches!(
            parse_metrics_params("q=%7B%7D&since=soon", NOW_S),
            Err(MetricsParamError::InvalidSince(_))
        ));
    }

    #[test]
    fn a_missing_or_partial_absolute_range_is_rejected() {
        for raw in ["q=%7B%7D", "q=%7B%7D&start=1", "q=%7B%7D&end=2"] {
            assert!(
                matches!(
                    parse_metrics_params(raw, NOW_S),
                    Err(MetricsParamError::MissingRange)
                ),
                "{raw} must be rejected"
            );
        }
    }

    #[test]
    fn metrics_accepts_nanosecond_and_rfc3339_bounds() {
        // The metrics endpoints share the search surface's timestamp
        // grammar (one parser — code review round 1 on issue #59).
        let p = parse_metrics_params(
            "q=%7B%7D&start=1700000000000000000&end=2023-11-14T23:13:20Z",
            NOW_S,
        )
        .unwrap();
        assert_eq!(p.start_ns, 1_700_000_000_000_000_000);
        assert_eq!(p.end_ns, 1_700_003_600_000_000_000);
        // Derived step over the ns-precision window: 3600 / 100 = 36.
        assert_eq!(p.step_s, 36);
    }

    #[test]
    fn metrics_extreme_ns_bounds_never_wrap_the_derived_step() {
        // Width > i64::MAX in nanoseconds: the i128 derivation still
        // produces a sane positive step (the planner's cap then 422s the
        // request downstream — never a wrapped/negative step here).
        let p = parse_metrics_params(
            &format!("q=%7B%7D&start={}&end={}", i64::MIN, i64::MAX),
            NOW_S,
        )
        .unwrap();
        assert!(p.step_s > 0);
        assert_eq!(p.step_s, ((u64::MAX / 1_000_000_000) / 100) as i64);
    }

    #[test]
    fn metrics_bad_timestamps_and_inverted_ranges_are_rejected() {
        assert!(matches!(
            parse_metrics_params("q=%7B%7D&start=abc&end=2", NOW_S),
            Err(MetricsParamError::InvalidTimestamp(_))
        ));
        assert!(matches!(
            parse_metrics_params("q=%7B%7D&start=200&end=100", NOW_S),
            Err(MetricsParamError::InvalidRange { .. })
        ));
        assert!(matches!(
            parse_metrics_params("q=%7B%7D&start=100&end=100", NOW_S),
            Err(MetricsParamError::InvalidRange { .. })
        ));
    }

    // -- service-graph params (issue #173) ---------------------------------

    #[test]
    fn a_minimal_graph_request_parses_an_absolute_window() {
        let p = parse_graph_params("start=1700000000&end=1700003600", NOW_S).unwrap();
        assert_eq!(p.start_ns, 1_700_000_000_000_000_000);
        assert_eq!(p.end_ns, 1_700_003_600_000_000_000);
    }

    #[test]
    fn graph_since_derives_the_window_and_conflicts_with_absolute_bounds() {
        let p = parse_graph_params("since=1h", NOW_S).unwrap();
        assert_eq!(p.start_ns, (NOW_S - 3_600) * 1_000_000_000);
        assert_eq!(p.end_ns, NOW_S * 1_000_000_000);
        for raw in [
            "since=1h&start=1",
            "since=1h&end=2",
            "since=1h&start=1&end=2",
        ] {
            assert!(
                matches!(
                    parse_graph_params(raw, NOW_S),
                    Err(GraphParamError::ConflictingRange)
                ),
                "{raw} must conflict"
            );
        }
        assert!(matches!(
            parse_graph_params("since=soon", NOW_S),
            Err(GraphParamError::InvalidSince(_))
        ));
    }

    #[test]
    fn graph_missing_or_partial_range_is_rejected() {
        for raw in ["", "start=1", "end=2"] {
            assert!(
                matches!(
                    parse_graph_params(raw, NOW_S),
                    Err(GraphParamError::MissingRange)
                ),
                "{raw:?} must be rejected"
            );
        }
    }

    #[test]
    fn graph_bad_timestamps_and_inverted_ranges_are_rejected() {
        assert!(matches!(
            parse_graph_params("start=abc&end=2", NOW_S),
            Err(GraphParamError::InvalidTimestamp(_))
        ));
        assert!(matches!(
            parse_graph_params("start=200&end=100", NOW_S),
            Err(GraphParamError::InvalidRange { .. })
        ));
        assert!(matches!(
            parse_graph_params("start=100&end=100", NOW_S),
            Err(GraphParamError::InvalidRange { .. })
        ));
    }

    #[test]
    fn graph_accepts_nanosecond_and_rfc3339_bounds() {
        let p = parse_graph_params("start=1700000000000000000&end=2023-11-14T23:13:20Z", NOW_S)
            .unwrap();
        assert_eq!(p.start_ns, 1_700_000_000_000_000_000);
        assert_eq!(p.end_ns, 1_700_003_600_000_000_000);
    }

    // -- tags params (issue #58) -------------------------------------------

    #[test]
    fn tags_scope_resource_and_span_parse() {
        assert_eq!(
            parse_tags_params("scope=resource")
                .unwrap()
                .scope
                .as_deref(),
            Some("resource")
        );
        assert_eq!(
            parse_tags_params("scope=span").unwrap().scope.as_deref(),
            Some("span")
        );
    }

    #[test]
    fn tags_absent_scope_means_both_scopes() {
        assert_eq!(parse_tags_params("").unwrap().scope, None);
    }

    #[test]
    fn tags_start_end_are_accepted_and_ignored_not_errors() {
        // The catalog is time-less: the bounds parse away without effect
        // (docs/api.md §4.3).
        let p = parse_tags_params("scope=span&start=100&end=200").unwrap();
        assert_eq!(p.scope.as_deref(), Some("span"));
    }

    #[test]
    fn tags_unknown_scope_is_rejected_never_widened() {
        for raw in ["scope=bogus", "scope=intrinsic", "scope=none", "scope="] {
            assert!(
                matches!(
                    parse_tags_params(raw),
                    Err(TagsParamError::UnsupportedScope(_))
                ),
                "{raw} must be rejected"
            );
        }
    }

    #[test]
    fn tag_path_prefixes_resolve_to_scopes() {
        assert_eq!(
            parse_tag_path("resource.service.name").unwrap(),
            (Some("resource".to_string()), "service.name".to_string())
        );
        assert_eq!(
            parse_tag_path("span.x").unwrap(),
            (Some("span".to_string()), "x".to_string())
        );
    }

    #[test]
    fn tag_path_leading_dot_and_bare_keys_are_unscoped() {
        assert_eq!(parse_tag_path(".x").unwrap(), (None, "x".to_string()));
        assert_eq!(parse_tag_path("x").unwrap(), (None, "x".to_string()));
    }

    #[test]
    fn tag_path_empty_keys_are_rejected() {
        for raw in ["", ".", "resource.", "span."] {
            assert!(
                matches!(parse_tag_path(raw), Err(TagPathError::EmptyKey)),
                "{raw:?} must be rejected"
            );
        }
    }
}
