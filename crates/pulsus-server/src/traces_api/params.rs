//! `/api/traces/v1` parameter parsing: the trace-fetch hex id
//! (docs/api.md §4.1: 1 to 32 hex characters, left-padded — the injection
//! boundary for the fetch statements' `unhex('...')` literals: only
//! `[0-9a-f]{32}` output ever leaves [`parse_trace_id`]) and the issue
//! #57 search params (docs/api.md §4.2: `q`/legacy params, `start`/`end`
//! unix seconds, `limit`, `spss`). The `(key, value)` pair core mirrors
//! `logs_api`/`prom_api`'s per-surface copies (each module owns its
//! params, established convention).

use thiserror::Error;

use pulsus_read::FetchWindow;
use pulsus_read::traces::spans::tags::ATTR_SCOPES;

use super::querytext::{QueryTextError, TraceQlText, validate_traceql_query};

/// Errors from parsing the `{traceId}` path parameter — mapped to `400`
/// by `error::ApiError`.
#[derive(Debug, Error)]
pub(crate) enum TraceIdError {
    #[error("invalid trace id {0:?}: expected 1 to 32 hex characters")]
    InvalidLength(String),
    #[error("invalid trace id {0:?}: expected hex characters only")]
    NotHex(String),
}

/// Parses `raw` into the canonical 32-char lowercase hex trace id: 1 to
/// 32 hex characters, lowercased and left-padded with `'0'` to 32 (the
/// reference's `HexStringToTraceID`, odd lengths included); empty, longer
/// than 32, or not hex is rejected.
pub(crate) fn parse_trace_id(raw: &str) -> Result<String, TraceIdError> {
    if raw.is_empty() || raw.len() > 32 {
        return Err(TraceIdError::InvalidLength(raw.to_string()));
    }
    if !raw.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(TraceIdError::NotHex(raw.to_string()));
    }
    Ok(format!("{:0>32}", raw.to_ascii_lowercase()))
}

/// The fetch routes' optional request window (issue #587).
///
/// **Every supplied bound is read before anything is decided.** A bound
/// present and unparseable is a `400` — the same grammar and the same
/// refusal as the search surface's — and that holds for a bound arriving
/// ALONE as much as for one half of a pair. Accepting a malformed
/// timestamp silently would answer over a window the caller did not ask
/// for, and the lone form is the likelier one: a half-filled time control
/// sends `start` with no `end`. The code review of 2026-10-03 found the
/// reverse order here, with "both or neither" applied first, so a lone
/// `?start=yesterday` was never parsed at all.
///
/// **Both bounds or neither, once both are known good.** The window is
/// read only by the fallback, which answers a trace the per-trace table
/// has not indexed; a half-open request has no window to answer over, so
/// one *valid* bound alone is the same as none. That rule chooses the
/// window to answer over; it was never a rule about which bounds to read.
///
/// **An empty or inverted window yields `None`.** The indexed statement
/// never reads the window, so the only thing a window can do is ADD an
/// answer; a window that cannot contain a span adds nothing, and refusing
/// it would turn a harmless request into an error on a route that
/// previously ignored these parameters altogether.
pub(crate) fn parse_fetch_window(
    query: Option<&str>,
) -> Result<Option<FetchWindow>, SearchParamError> {
    let Some(raw) = query else {
        return Ok(None);
    };
    let pairs = parse_pairs(raw);
    // Both parses happen before the both-or-neither test, so neither
    // bound's validity depends on the other's presence.
    let start_ns = parse_supplied_bound(&pairs, "start")?;
    let end_ns = parse_supplied_bound(&pairs, "end")?;
    let (Some(start_ns), Some(end_ns)) = (start_ns, end_ns) else {
        return Ok(None);
    };
    if end_ns <= start_ns {
        return Ok(None);
    }
    Ok(Some(FetchWindow { start_ns, end_ns }))
}

/// One fetch-window bound: `None` when the caller did not supply it (or
/// supplied it empty, which this surface has always read as absent), the
/// parsed nanosecond instant when they did, and a `400` when they
/// supplied something this grammar cannot read.
///
/// The same grammar as the search surface's — seconds, nanoseconds or
/// RFC3339 — through the one parser, so the two endpoints cannot drift.
fn parse_supplied_bound(
    pairs: &[(String, String)],
    name: &str,
) -> Result<Option<i64>, SearchParamError> {
    let Some(raw) = get(pairs, name).filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    parse_timestamp_ns(raw)
        .map(Some)
        .ok_or_else(|| SearchParamError::InvalidTimestamp(raw.to_string()))
}

/// Default `limit` when the param is absent (docs/api.md §4.2).
pub(crate) const DEFAULT_LIMIT: u32 = 20;
/// Default `spss` (spans-per-spanset cap) when the param is absent or `0`.
pub(crate) const DEFAULT_SPSS: u32 = 3;
/// The largest `limit`: the reference's default `max_limit`.
const MAX_LIMIT: u32 = 262_144;
/// The largest `spss`: the reference's default `max_spans_per_span_set`.
const MAX_SPSS: u32 = 100;

/// Errors from parsing `/api/traces/v1/search` request parameters —
/// mapped to `400` by `error::ApiError`.
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
    /// A `limit` above `MAX_LIMIT`, in the reference's words.
    #[error("limit {0} exceeds max limit 262144")]
    LimitTooLarge(u32),
    /// An `spss` above `MAX_SPSS`, in the reference's words.
    #[error("spans per span set exceeds 100. received {0}")]
    SpssTooLarge(u32),
    #[error(
        "'q' and the legacy search params (tags/minDuration/maxDuration) are mutually \
         exclusive: supply one or the other, never both"
    )]
    ConflictingQuery,
    /// The `q`/`query` expression failed the reference's query-frontend
    /// validator: over the cap (issue #284) or unparseable (issue #326).
    #[error(transparent)]
    QueryText(#[from] QueryTextError),
}

/// The raw, percent-decoded search request — `q` XOR the legacy params
/// (validated here; compilation of either into a TraceQL AST is
/// `search.rs`/`legacy.rs`'s job).
///
/// `q` is a [`TraceQlText`], so it cannot be populated without running
/// the issue #284 length cap. The legacy fields stay plain `String`s on
/// purpose: the reference caps only the `q`/`query` parameter, never the
/// expression its legacy params compile to.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RawSearchParams {
    pub q: Option<TraceQlText>,
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
        .filter(|s| !s.is_empty())
        .map(TraceQlText::new)
        .transpose()?;
    // The reference's frontend validator runs on the SEARCH pipeline too,
    // and it reads `q` and then `query`
    // (`async_query_validator_middleware.go:38-41`) — so `query` carries
    // BOTH the cap (issue #284) and the parse (issue #326) on this route
    // even though the search REQUEST parser reads `q` alone
    // (`pkg/api/http.go:180`, `urlParamQuery = "q"`; a lone `query` is
    // folded into the legacy tag map at `:222`, never treated as
    // TraceQL). Validating it here, and NOT making it an alias for `q`,
    // is what keeps both halves of that faithful. `q` itself gets the
    // same two checks — the cap above, the parse in `search::search_impl`
    // where the AST is actually needed.
    if let Some(query) = get(&pairs, "query") {
        validate_traceql_query(query)?;
    }
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
    let limit = parse_limit(&pairs)?;
    let spss = parse_spss(&pairs)?;
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

/// The one trace-surface timestamp rule (docs/api.md §1), the reference's
/// `parseTimestamp` (`pkg/api/http.go:626-650`), shared by every trace
/// route so they cannot drift:
///
/// - a value containing `.` that parses as a float is unix **seconds**
///   with a fraction rounded to the millisecond;
/// - an integer of ten characters or fewer is unix **seconds**, a longer
///   one unix **nanoseconds** — the length of the text, not the
///   magnitude, as the log APIs read it;
/// - anything else parses as **RFC3339**.
fn parse_timestamp_ns(raw: &str) -> Option<i64> {
    if raw.contains('.')
        && let Ok(f) = raw.parse::<f64>()
    {
        let secs = f.trunc();
        let frac_ms = ((f - secs) * 1000.0).round();
        return (secs as i64)
            .checked_mul(1_000_000_000)?
            .checked_add(frac_ms as i64 * 1_000_000);
    }
    if let Ok(v) = raw.parse::<i64>() {
        return if raw.len() <= 10 {
            v.checked_mul(1_000_000_000)
        } else {
            Some(v)
        };
    }
    chrono::DateTime::parse_from_rfc3339(raw)
        .ok()?
        .timestamp_nanos_opt()
}

fn parse_unix_seconds_ns(
    pairs: &[(String, String)],
    name: &'static str,
) -> Result<i64, SearchParamError> {
    let raw = get(pairs, name).ok_or(SearchParamError::MissingRange(name))?;
    parse_timestamp_ns(raw).ok_or_else(|| SearchParamError::InvalidTimestamp(raw.to_string()))
}

/// The search `limit`: default [`DEFAULT_LIMIT`], `1..=MAX_LIMIT`; zero,
/// negative or not an integer is `InvalidCount`, above the maximum
/// `LimitTooLarge` (`search_handlers.go:165-175`).
fn parse_limit(pairs: &[(String, String)]) -> Result<u32, SearchParamError> {
    let Some(raw) = get(pairs, "limit") else {
        return Ok(DEFAULT_LIMIT);
    };
    let n = parse_positive(raw, "limit")?;
    if n > MAX_LIMIT {
        return Err(SearchParamError::LimitTooLarge(n));
    }
    Ok(n)
}

/// The search `spss`: absent or `0` is [`DEFAULT_SPSS`] (`engine.go:120-123`),
/// otherwise `1..=MAX_SPSS`; above the maximum `SpssTooLarge`
/// (`search_sharder.go:108-113`), negative or not an integer
/// `InvalidCount`.
fn parse_spss(pairs: &[(String, String)]) -> Result<u32, SearchParamError> {
    match get(pairs, "spss") {
        None | Some("0") => Ok(DEFAULT_SPSS),
        Some(raw) => {
            let n = parse_positive(raw, "spss")?;
            if n > MAX_SPSS {
                return Err(SearchParamError::SpssTooLarge(n));
            }
            Ok(n)
        }
    }
}

fn parse_positive(raw: &str, name: &'static str) -> Result<u32, SearchParamError> {
    raw.parse::<u32>()
        .ok()
        .filter(|n| *n >= 1)
        .ok_or_else(|| SearchParamError::InvalidCount {
            name,
            raw: raw.to_string(),
        })
}

/// Errors from parsing `/api/traces/v1/metrics/{query_range,query}`
/// request parameters (issue #59, docs/api.md §4.4) — mapped to `400` by
/// `error::ApiError`.
#[derive(Debug, Error)]
pub(crate) enum MetricsParamError {
    #[error("missing required parameter: 'q' (or 'query') must carry a TraceQL metrics expression")]
    MissingQuery,
    #[error("invalid timestamp {0:?}: expected unix seconds, unix nanoseconds, or RFC3339")]
    InvalidTimestamp(String),
    #[error("invalid range: end ({end}) must be greater than start ({start})")]
    InvalidRange { start: i64, end: i64 },
    #[error("invalid 'since' {0:?}: expected a duration such as 90s, 1h30m, 1d or 2w")]
    InvalidSince(String),
    #[error(
        "invalid 'step' {0:?}: expected a positive whole number of milliseconds \
         (e.g. 60, 60s, 500ms, 1m30s, 1.5s)"
    )]
    InvalidStep(String),
    /// The `q`/`query` expression exceeded the reference's cap (issue
    /// #284).
    #[error(transparent)]
    QueryText(#[from] QueryTextError),
    /// A range request of more than [`MAX_RANGE_INTERVALS`] steps (issue
    /// #596 part 1), in the reference's words.
    #[error(
        "step of {} is too small, minimum step for given range is {}",
        pulsus_read::go_duration_string(*step_ns),
        pulsus_read::go_duration_string(*minimum_ns)
    )]
    StepTooSmall { step_ns: i64, minimum_ns: i64 },
}

/// The most steps a range request may span: the reference's
/// `max_intervals` (issue #596 part 1).
const MAX_RANGE_INTERVALS: i128 = 10_000;

/// Refuses a range request whose window holds more than
/// [`MAX_RANGE_INTERVALS`] steps, naming the smallest step it would take.
///
/// `(end − start) / step > 10,000` is decided exactly, in `i128`; the
/// smallest step is `(end − start) / 10,000` in whole nanoseconds.
pub(crate) fn check_range_intervals(params: &RawMetricsParams) -> Result<(), MetricsParamError> {
    let width = i128::from(params.end_ns) - i128::from(params.start_ns);
    let step_ns = i128::from(params.step_ms) * NS_PER_MS;
    if width <= MAX_RANGE_INTERVALS * step_ns {
        return Ok(());
    }
    Err(MetricsParamError::StepTooSmall {
        step_ns: i64::try_from(step_ns).unwrap_or(i64::MAX),
        minimum_ns: i64::try_from(width / MAX_RANGE_INTERVALS).unwrap_or(i64::MAX),
    })
}

/// The parsed metrics request: the TraceQL expression, the validated
/// window and step, and the normalised exemplar budget. `step_ms` is
/// already derived (docs/api.md §4.4) when the request omitted `step` or
/// arrived on the instant route.
///
/// `q` is a [`TraceQlText`] for the same reason the search params' is
/// (issue #284): the cap runs or the struct cannot be built.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RawMetricsParams {
    pub q: TraceQlText,
    pub start_ns: i64,
    pub end_ns: i64,
    /// Whole milliseconds (issue #477 (d)).
    pub step_ms: i64,
    /// The `exemplars` request parameter, normalised: `None` when absent,
    /// empty, unparseable, zero or negative. Never a `400` — the
    /// reference accepts every one of those and behaves as if the
    /// parameter were absent, and the datasource is what sends it.
    pub exemplars: Option<u32>,
}

/// Parses the metrics query string (docs/api.md §4.4), the reference's
/// `ParseQueryRangeRequest`/`ParseQueryInstantRequest`.
///
/// `q` wins over `query` when both are sent. The window is
/// [`parse_window`]'s, with `now_ns` injected for testability. On the
/// range route `step` accepts any positive whole number of MILLISECONDS
/// through [`parse_step_ms`] (issue #477 (d)) and, when absent, is
/// [`default_step_ms`]'s; the instant route reads no `step` and carries
/// the derived one, which only sizes the plan's unused range grid.
///
/// `exemplars` is read here (issue #477 (c)) and normalised to
/// `Option<u32>`; every unusable spelling behaves as absent rather than
/// as a 400.
pub(crate) fn parse_metrics_params(
    raw: &str,
    now_ns: i64,
    form: MetricsForm,
) -> Result<RawMetricsParams, MetricsParamError> {
    let pairs = parse_pairs(raw);
    let q_key = get(&pairs, "q").filter(|s| !s.is_empty());
    let query_key = get(&pairs, "query").filter(|s| !s.is_empty());
    let q = match q_key.or(query_key) {
        Some(q) => TraceQlText::new(q)?,
        None => return Err(MetricsParamError::MissingQuery),
    };

    let (start_ns, end_ns) = parse_window(&pairs, now_ns).map_err(|fault| match fault {
        WindowFault::Timestamp(raw) => MetricsParamError::InvalidTimestamp(raw),
        WindowFault::Since(raw) => MetricsParamError::InvalidSince(raw),
        WindowFault::Range { start, end } => MetricsParamError::InvalidRange { start, end },
    })?;

    let explicit = match form {
        MetricsForm::Range => get(&pairs, "step").filter(|s| !s.is_empty()),
        MetricsForm::Instant => None,
    };
    let step_ms = match explicit {
        Some(raw_step) => parse_step_ms(raw_step)
            .ok_or_else(|| MetricsParamError::InvalidStep(raw_step.to_string()))?,
        None => default_step_ms(start_ns, end_ns),
    };

    let exemplars = parse_exemplars(&pairs);

    Ok(RawMetricsParams {
        q,
        start_ns,
        end_ns,
        step_ms,
        exemplars,
    })
}

/// What can be wrong with a metrics or service-graph window.
enum WindowFault {
    Timestamp(String),
    Since(String),
    Range { start: i64, end: i64 },
}

/// The default lookback when neither `start` nor `since` is sent.
const DEFAULT_SINCE_NS: i64 = 3_600 * 1_000_000_000;

/// The metrics and service-graph window, the reference's
/// `determineBounds` (`pkg/api/http.go:516-556`): `end` is the parsed
/// `end`, else now; `start` is the parsed `start`, else `since` (default
/// one hour) before `min(end, now)`, and `since` is read only then. A
/// window whose `end` is not after its `start` is refused — the
/// reference accepts `start = end`, an empty window, and answers nothing
/// over it either way (docs/api.md §4.4).
fn parse_window(pairs: &[(String, String)], now_ns: i64) -> Result<(i64, i64), WindowFault> {
    let bound = |name: &str| -> Result<Option<i64>, WindowFault> {
        let Some(raw) = get(pairs, name).filter(|s| !s.is_empty()) else {
            return Ok(None);
        };
        parse_timestamp_ns(raw)
            .map(Some)
            .ok_or_else(|| WindowFault::Timestamp(raw.to_string()))
    };
    let start = bound("start")?;
    let end_ns = bound("end")?.unwrap_or(now_ns);
    let start_ns = match start {
        Some(start_ns) => start_ns,
        None => {
            let since = get(pairs, "since").filter(|s| !s.is_empty());
            let since_ns = match since {
                Some(raw) => parse_prom_duration_ns(raw)
                    .ok_or_else(|| WindowFault::Since(raw.to_string()))?,
                None => DEFAULT_SINCE_NS,
            };
            end_ns
                .min(now_ns)
                .checked_sub(since_ns)
                .ok_or_else(|| WindowFault::Since(since.unwrap_or_default().to_string()))?
        }
    };
    if end_ns <= start_ns {
        return Err(WindowFault::Range {
            start: start_ns / 1_000_000_000,
            end: end_ns / 1_000_000_000,
        });
    }
    Ok((start_ns, end_ns))
}

/// The derived range step, the reference's `DefaultQueryRangeStep`
/// (`pkg/traceql/engine_metrics.go:35-79`), in whole milliseconds: the
/// window over 240, rounded down to a multiple of the first of 1 h,
/// 5 min, 1 min, 15 s, 5 s and 1 s it exceeds, at least 1 s; under a
/// one-minute window, rounded down to a multiple of 50 ms, at least 50 ms.
fn default_step_ms(start_ns: i64, end_ns: i64) -> i64 {
    const MS: i128 = NS_PER_MS;
    const S: i128 = 1_000 * MS;
    // i128: the width of two extreme `i64` instants does not fit `i64`.
    let width = i128::from(end_ns) - i128::from(start_ns);
    let per_point = width / 240;
    let step_ns = if width < 60 * S {
        if per_point > 50 * MS {
            per_point / (50 * MS) * (50 * MS)
        } else {
            50 * MS
        }
    } else {
        [3_600 * S, 300 * S, 60 * S, 15 * S, 5 * S, S]
            .into_iter()
            .find(|g| per_point > *g)
            .map_or(S, |g| per_point / g * g)
    };
    i64::try_from(step_ns / MS).unwrap_or(i64::MAX)
}

/// A Prometheus duration (`model.ParseDuration`), in nanoseconds: one or
/// more `<digits><unit>` with units `y w d h m s ms`, each at most once and
/// in that order, or `0` alone. `1d` is 86,400 s, `1w` seven days, `1y`
/// 365 days.
fn parse_prom_duration_ns(raw: &str) -> Option<i64> {
    const S: i128 = 1_000_000_000;
    const UNITS: [(&str, i128); 7] = [
        ("y", 365 * 86_400 * S),
        ("w", 7 * 86_400 * S),
        ("d", 86_400 * S),
        ("h", 3_600 * S),
        ("m", 60 * S),
        ("s", S),
        ("ms", 1_000_000),
    ];
    if raw == "0" {
        return Some(0);
    }
    if raw.is_empty() {
        return None;
    }
    let bytes = raw.as_bytes();
    let mut idx = 0usize;
    // Units appear in `UNITS` order, once each: the next allowed index.
    let mut next_unit = 0usize;
    let mut total: i128 = 0;
    while idx < bytes.len() {
        let digits_start = idx;
        while bytes.get(idx).is_some_and(u8::is_ascii_digit) {
            idx += 1;
        }
        if idx == digits_start {
            return None;
        }
        let n: i128 = raw[digits_start..idx].parse().ok()?;
        let rest = &raw[idx..];
        // The longest match, so `5ms` is never `5m` then `s`.
        let (pos, (unit, per_unit)) = UNITS
            .iter()
            .enumerate()
            .filter(|(_, (u, _))| rest.starts_with(u))
            .max_by_key(|(_, (u, _))| u.len())?;
        if pos < next_unit {
            return None;
        }
        next_unit = pos + 1;
        idx += unit.len();
        total = total.checked_add(n.checked_mul(*per_unit)?)?;
    }
    i64::try_from(total).ok()
}

/// Which of the two metrics routes a request arrived on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MetricsForm {
    /// `query_range`: a grid of buckets.
    Range,
    /// `query`: one bucket over the requested window.
    Instant,
}

/// The `exemplars` request parameter (issue #477 (c)), normalised rather
/// than validated: absent, empty, non-numeric, zero or negative all yield
/// `None`, which the planner reads as "fall through to the next input".
///
/// **Never a 400.** Measured on the reference: `exemplars=0`,
/// `exemplars=-1` and `exemplars=abc` each answer `200` with the same
/// exemplars an omitted parameter produces. A 400 here would break a
/// panel for a value the reference serves.
fn parse_exemplars(pairs: &[(String, String)]) -> Option<u32> {
    get(pairs, "exemplars")
        .filter(|s| !s.is_empty())
        .and_then(|raw| raw.parse::<i64>().ok())
        .filter(|n| *n > 0)
        .map(|n| u32::try_from(n).unwrap_or(u32::MAX))
}

/// Nanoseconds in one millisecond — the resolution boundary the metrics
/// step is required to land on.
const NS_PER_MS: i128 = 1_000_000;

/// The metrics `step` grammar (issue #477 (d)): any positive duration
/// that is a whole number of MILLISECONDS, returned in milliseconds.
///
/// Accepted, all measured against the reference: a bare number
/// (seconds — the Prometheus `step` convention), an optional leading `+`,
/// a decimal fraction, and one or more `<number><unit>` components with
/// `ms`/`s`/`m`/`h`. So `30`, `30s`, `500ms`, `100ms`, `3ms`, `1.5s`,
/// `1500ms`, `0.5s`, `.5s`, `+30s`, `2.5s`, `1m30s`, `1s500ms`, `1h30m`,
/// `1m` and `1h` all parse.
///
/// Rejected: anything that is not a whole number of milliseconds **at any
/// magnitude** — `1.5ms`, `3.5ms` and `100.25ms` are all 400 here and all
/// `200` on the reference above its own `range/10000` floor. The bound is
/// deliberately "not a whole number of milliseconds" and not
/// "sub-millisecond", and `100.25ms` is the counterexample that says so.
/// We reject rather than emulate because the reference's own wire form is
/// lossy for such steps: a 3.5 ms grid renders with label spacing
/// `[3,4,3,4]` ms, because `timestampMs` truncates a nanosecond grid.
/// Ledgered as `traceql-metrics-fractional-ms-step-rejected`.
/// Also rejected, as there: `abc`, `30S` (case-sensitive), `5e2ms`, `0`,
/// `0s`, `-60`, and units outside the four (`500us`, `2d`).
///
/// Arithmetic is exact: each component is `numerator · ns_per_unit /
/// 10^fraction_digits` in `i128`, and a component that does not divide
/// evenly is a sub-nanosecond value and rejects.
fn parse_step_ms(raw: &str) -> Option<i64> {
    const UNITS: [(&str, i128); 4] = [
        ("ms", NS_PER_MS),
        ("s", 1_000 * NS_PER_MS),
        ("m", 60_000 * NS_PER_MS),
        ("h", 3_600_000 * NS_PER_MS),
    ];

    let body = raw.strip_prefix('+').unwrap_or(raw);
    if body.is_empty() {
        return None;
    }
    // A bare number carries no unit and means seconds.
    if body.bytes().all(|b| b.is_ascii_digit() || b == b'.') {
        let ns = decimal_component_ns(body, 1_000 * NS_PER_MS)?;
        return whole_ms(ns);
    }

    let bytes = body.as_bytes();
    let mut idx = 0usize;
    let mut total_ns: i128 = 0;
    while idx < bytes.len() {
        let number_start = idx;
        while bytes
            .get(idx)
            .is_some_and(|b| b.is_ascii_digit() || *b == b'.')
        {
            idx += 1;
        }
        if idx == number_start {
            return None;
        }
        let number = &body[number_start..idx];
        // Longest match, so `ms` is never read as `m` followed by `s`.
        let unit_start = idx;
        let (unit, per_unit_ns) = UNITS
            .iter()
            .filter(|(name, _)| body[unit_start..].starts_with(name))
            .max_by_key(|(name, _)| name.len())
            .copied()?;
        idx = unit_start + unit.len();
        total_ns = total_ns.checked_add(decimal_component_ns(number, per_unit_ns)?)?;
    }
    whole_ms(total_ns)
}

/// One `<number>` of a step component, in nanoseconds. The number may
/// carry a decimal point and may start with one (`.5`); it may not be
/// empty, carry two points, or resolve to a fraction of a nanosecond.
fn decimal_component_ns(number: &str, per_unit_ns: i128) -> Option<i128> {
    let (int_part, frac_part) = match number.split_once('.') {
        Some((i, f)) => (i, f),
        None => (number, ""),
    };
    if int_part.contains('.') || frac_part.contains('.') {
        return None;
    }
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    // Guard the exponent before building it: 10^n overflows `i128` past
    // n = 38, and a step written with 39 fraction digits is nonsense
    // rather than a value to compute.
    if frac_part.len() > 18 {
        return None;
    }
    let digits: i128 = format!("{int_part}{frac_part}").parse().ok()?;
    let denominator = 10_i128.checked_pow(frac_part.len() as u32)?;
    let scaled = digits.checked_mul(per_unit_ns)?;
    if scaled % denominator != 0 {
        return None;
    }
    Some(scaled / denominator)
}

/// A positive nanosecond duration as whole milliseconds, or `None`.
fn whole_ms(total_ns: i128) -> Option<i64> {
    if total_ns <= 0 || total_ns % NS_PER_MS != 0 {
        return None;
    }
    i64::try_from(total_ns / NS_PER_MS).ok()
}

/// Errors from parsing `/api/traces/v1/service_graph` request parameters
/// (issue #173, docs/api.md §4.5) — mapped to `400` by
/// `error::ApiError`. Same window grammar as the metrics surface (`start`/
/// `end`/`since`), minus `q`/`step` — the service graph is a fixed
/// aggregation over a window, with no expression and no bucketing.
// The variants keep the names their `MetricsParamError` twins carry.
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub(crate) enum GraphParamError {
    #[error("invalid timestamp {0:?}: expected unix seconds, unix nanoseconds, or RFC3339")]
    InvalidTimestamp(String),
    #[error("invalid range: end ({end}) must be greater than start ({start})")]
    InvalidRange { start: i64, end: i64 },
    #[error("invalid 'since' {0:?}: expected a duration such as 90s, 1h30m, 1d or 2w")]
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

/// Parses the service-graph query string (docs/api.md §4.5): the metrics
/// routes' window, [`parse_window`], with `now_ns` injected for
/// testability. An invalid or inverted window is an explicit `400`.
pub(crate) fn parse_graph_params(
    raw: &str,
    now_ns: i64,
) -> Result<RawGraphParams, GraphParamError> {
    let pairs = parse_pairs(raw);
    let (start_ns, end_ns) = parse_window(&pairs, now_ns).map_err(|fault| match fault {
        WindowFault::Timestamp(raw) => GraphParamError::InvalidTimestamp(raw),
        WindowFault::Since(raw) => GraphParamError::InvalidSince(raw),
        WindowFault::Range { start, end } => GraphParamError::InvalidRange { start, end },
    })?;
    Ok(RawGraphParams { start_ns, end_ns })
}

/// The first nanosecond a windowed trace read returns (docs/api.md §4,
/// "Retention"): a span is expired once
/// `intDiv(start_ns, 1e9) + retention_days * 86400 <= now`, the tables'
/// own TTL, so the first kept nanosecond is
/// `(now_s - retention_days * 86400 + 1) * 1e9`.
pub(crate) fn retention_floor_ns(retention_days: u32, now_ns: i64) -> i64 {
    now_ns
        .div_euclid(1_000_000_000)
        .saturating_sub(i64::from(retention_days).saturating_mul(86_400))
        .saturating_add(1)
        .saturating_mul(1_000_000_000)
}

/// A window's start raised to the retention floor, and never past its
/// end: a window wholly before the floor becomes the empty `[end, end)`.
/// Parameter faults are answered before this, on the window as sent.
pub(crate) fn floored_start(start_ns: i64, end_ns: i64, floor_ns: i64) -> i64 {
    start_ns.max(floor_ns).min(end_ns)
}

/// The tag routes' `limit` (`tag_handlers.go:600-619`): at most this many
/// names per scope, or values. Absent, `0`, or not an integer is no limit.
pub(crate) fn parse_tag_limit(pairs: &[(String, String)]) -> Option<usize> {
    get(pairs, "limit")
        .and_then(|raw| raw.parse::<u32>().ok())
        .filter(|n| *n > 0)
        .map(|n| n as usize)
}

/// Errors from parsing the `/api/traces/v1/tags` query parameters —
/// mapped to `400` by `error::ApiError` (issue #58).
#[derive(Debug, Error)]
pub(crate) enum TagsParamError {
    #[error(
        "unsupported scope {0:?}: expected \"event\", \"instrumentation\", \"intrinsic\", \
         \"link\", \"resource\", \"span\", \"trace\" or \"none\" (or omit the parameter \
         for every scope)"
    )]
    UnsupportedScope(String),
    /// A range FAULT on a names route (issue #478). A well-formed range
    /// is still accepted and ignored there — the catalog read is bounded
    /// by the retention floor alone, not by the request — so only the
    /// fault shapes reach this.
    #[error(transparent)]
    Range(#[from] RangeParamError),
}

/// The three range faults, rejected on every §4.3 route (issue #478).
///
/// **The rule that decides which way each parameter goes, written down
/// because a later reader extending either behaviour will need it:**
/// *tolerate what the client cannot avoid sending; reject what indicates
/// a genuine fault.* The editor emits half-typed text as `q` on every
/// keystroke, so a `400` there would break autocomplete for input the
/// user cannot help — `q` is tolerated and widens
/// (`pulsus_read::traces::tag_narrow`). A malformed `start`, a `start`
/// with no `end`, and an inverted range are none of those things: no
/// client sends them in normal operation, and answering them over a
/// substituted window would return values computed for a range the caller
/// never asked for with nothing in the response to say so. That is a
/// silent wrong answer.
///
/// [`RangeParamError::MissingRange`] and
/// [`RangeParamError::InvalidTimestamp`] are BYTE-IDENTICAL to their
/// [`SearchParamError`] counterparts. [`RangeParamError::InvalidRange`]
/// deliberately is NOT, and the difference is in the predicate as well as
/// the words: `/search` rejects `end <= start`, this route rejects
/// `end < start` only, because a zero-width range is ACCEPTED here —
/// measured, the reference answers `200` to it, and singling it out would
/// be arbitrary when every sub-day range is already widened to whole UTC
/// days by the day-grain bound. So the message may not claim `end` must
/// be *greater* than `start`; it says what is actually enforced.
#[derive(Debug, Error)]
pub(crate) enum RangeParamError {
    #[error("missing required parameter {0:?}: start and end are required")]
    MissingRange(&'static str),
    #[error("invalid timestamp {0:?}: expected unix seconds, unix nanoseconds, or RFC3339")]
    InvalidTimestamp(String),
    #[error("invalid range: end ({end}) must not be earlier than start ({start})")]
    InvalidRange { start: i64, end: i64 },
}

/// Resolves `start`/`end` for a §4.3 route (issue #478).
///
/// A bound is "supplied" when the parameter is present, non-empty, and
/// does not decode to `0` — measured, the reference treats a zero bound
/// as absent rather than as 1970. Neither supplied → `Ok(None)`. Exactly
/// one supplied → [`RangeParamError::MissingRange`]. Unparseable →
/// [`RangeParamError::InvalidTimestamp`]. `end < start` →
/// [`RangeParamError::InvalidRange`]. `end == start` is ACCEPTED.
///
/// The rendered `{start, end}` are whole SECONDS, matching
/// [`parse_search_params`]'s own division rather than the parsed
/// nanoseconds.
pub(crate) fn parse_range_params(
    pairs: &[(String, String)],
) -> Result<Option<(i64, i64)>, RangeParamError> {
    let supplied = |name: &'static str| -> Result<Option<i64>, RangeParamError> {
        let Some(raw) = get(pairs, name).filter(|s| !s.is_empty()) else {
            return Ok(None);
        };
        let ns = parse_timestamp_ns(raw)
            .ok_or_else(|| RangeParamError::InvalidTimestamp(raw.to_string()))?;
        // A zero bound is "not supplied", not the epoch. Checked AFTER
        // parsing, so `start=abc` is still a parse failure rather than an
        // absent bound.
        Ok(if ns == 0 { None } else { Some(ns) })
    };
    let start = supplied("start")?;
    let end = supplied("end")?;
    match (start, end) {
        (None, None) => Ok(None),
        (Some(_), None) => Err(RangeParamError::MissingRange("end")),
        (None, Some(_)) => Err(RangeParamError::MissingRange("start")),
        (Some(start_ns), Some(end_ns)) => {
            if end_ns < start_ns {
                return Err(RangeParamError::InvalidRange {
                    start: start_ns / 1_000_000_000,
                    end: end_ns / 1_000_000_000,
                });
            }
            Ok(Some((start_ns, end_ns)))
        }
    }
}

/// The parsed `/api/traces/v1/tag/{tag}/values` request (issue #478):
/// the client's raw `q` and the resolved window.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct TagValuesParams {
    pub q: Option<String>,
    pub start_ns: i64,
    pub end_ns: i64,
}

/// Parses a §4.3 values-route query string.
///
/// `now_ns`/`lookback_ns` are injected, the shape
/// [`parse_graph_params`] already uses. The window is the supplied range,
/// or `[now_ns - lookback_ns, now_ns]` when none was supplied; only a
/// range FAULT fails. `q` is carried through verbatim — it is never
/// parsed here, because parsing it is total downstream and an
/// unparseable one must widen rather than reject.
pub(crate) fn parse_tag_values_params(
    raw: &str,
    now_ns: i64,
    lookback_ns: i64,
) -> Result<TagValuesParams, RangeParamError> {
    let pairs = parse_pairs(raw);
    let (start_ns, end_ns) = match parse_range_params(&pairs)? {
        Some(window) => window,
        None => (now_ns.saturating_sub(lookback_ns), now_ns),
    };
    Ok(TagValuesParams {
        q: get(&pairs, "q")
            .filter(|s| !s.is_empty())
            .map(str::to_string),
        start_ns,
        end_ns,
    })
}

/// Errors from parsing the `{tag}` path parameter of
/// `/api/traces/v1/tag/{tag}/values` — mapped to `400`.
#[derive(Debug, Error)]
pub(crate) enum TagPathError {
    #[error("invalid tag: the attribute key must be non-empty")]
    EmptyKey,
}

/// Scope keywords accepted on the three names routes that name no
/// catalog scope and therefore carry no tags (issue #475): answered
/// `200` with an EMPTY scope list and NO catalog read. Disjoint from
/// `pulsus_read::traces::spans::tags::ATTR_SCOPES` by construction and by
/// unit test — a keyword here can never reach the
/// SQL `IN` list.
pub(crate) const EMPTY_SCOPES: [&str; 1] = ["trace"];

/// What a `scope=` value selects on the three names routes (issue #475).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TagScope {
    /// Absent, `none`, or empty — every attribute scope, plus the
    /// intrinsic scope on the routes that carry it.
    All,
    /// One member of `pulsus_read::traces::tags_sql::ATTR_SCOPES`.
    Attribute(&'static str),
    /// `intrinsic` — the static vocabulary only, no catalog read.
    Intrinsic,
    /// A member of [`EMPTY_SCOPES`] — an empty list, no catalog read.
    NoTags,
}

/// The parsed `/api/traces/v1/tags` request: only `scope` filters.
/// `start`/`end` are accepted for client compatibility and IGNORED —
/// the `tag_names` read is bounded by the retention floor alone
/// (docs/api.md §4.3, issue #496). `limit` is read by [`parse_tag_limit`] and cuts the
/// rendered lists; any other parameter is ignored.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct TagsParams {
    pub scope: TagScope,
}

/// Parses the `/api/traces/v1/tags` query string (docs/api.md §4.3, as
/// widened by issue #475). Accepts, case-sensitively: absent, `none` or
/// the empty string → [`TagScope::All`]; any [`ATTR_SCOPES`] member →
/// [`TagScope::Attribute`]; `intrinsic` → [`TagScope::Intrinsic`]; any
/// [`EMPTY_SCOPES`] member → [`TagScope::NoTags`]. Anything else — a
/// different casing included — is an explicit `400`, never silently
/// widened to "all scopes" (adjudication 4 on issue #58).
///
/// The accept list is DERIVED from `ATTR_SCOPES`, so the scope keywords
/// accepted here and the scopes the SQL can be confined to are one
/// constant and cannot drift apart.
pub(crate) fn parse_tags_params(raw: &str) -> Result<TagsParams, TagsParamError> {
    let pairs = parse_pairs(raw);
    // Issue #478: a range FAULT is rejected here too, so the six §4.3
    // routes answer the same malformed input the same way. A WELL-FORMED
    // range is still accepted and ignored — the catalog read is bounded by
    // the retention floor alone (issue #496), not by the request.
    parse_range_params(&pairs)?;
    let scope = match get(&pairs, "scope") {
        None | Some("" | "none") => TagScope::All,
        Some("intrinsic") => TagScope::Intrinsic,
        Some(other) => {
            if let Some(attr) = ATTR_SCOPES.iter().find(|s| **s == other) {
                TagScope::Attribute(attr)
            } else if EMPTY_SCOPES.contains(&other) {
                TagScope::NoTags
            } else {
                return Err(TagsParamError::UnsupportedScope(other.to_string()));
            }
        }
    };
    Ok(TagsParams { scope })
}

/// What a `{tag}` path segment resolves to on the native and v2 values
/// routes (issue #475).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TagLookup {
    /// An intrinsic spelling — answered from the static vocabulary, with
    /// no catalog read.
    Intrinsic(pulsus_traceql::Intrinsic),
    /// An attribute key, optionally confined to one attribute scope.
    Attribute { scope: Option<String>, key: String },
}

/// Resolves the `{tag}` path parameter of the native and v2 values
/// routes (issue #475).
///
/// Resolution order, and it is load-bearing: a `<s>.` prefix for any `s`
/// in [`ATTR_SCOPES`], or a leading `.`, makes it an ATTRIBUTE lookup —
/// so `span.name` is the attribute keyed `name` while `span:name` is the
/// intrinsic. Otherwise an intrinsic spelling (bare or colon-scoped,
/// resolved by `Intrinsic::from_discovery_spelling`), which is answered
/// without reading the store. Otherwise a bare attribute key, read
/// across the five attribute scopes. An empty key is a `400` in every
/// branch.
pub(crate) fn parse_tag_lookup(raw_tag: &str) -> Result<TagLookup, TagPathError> {
    for scope in ATTR_SCOPES {
        if let Some(rest) = raw_tag.strip_prefix(scope)
            && let Some(key) = rest.strip_prefix('.')
        {
            return Ok(TagLookup::Attribute {
                scope: Some(scope.to_string()),
                key: non_empty_key(key)?,
            });
        }
    }
    if let Some(key) = raw_tag.strip_prefix('.') {
        return Ok(TagLookup::Attribute {
            scope: None,
            key: non_empty_key(key)?,
        });
    }
    if let Some(intrinsic) = pulsus_traceql::Intrinsic::from_discovery_spelling(raw_tag) {
        return Ok(TagLookup::Intrinsic(intrinsic));
    }
    Ok(TagLookup::Attribute {
        scope: None,
        key: non_empty_key(raw_tag)?,
    })
}

fn non_empty_key(key: &str) -> Result<String, TagPathError> {
    if key.is_empty() {
        return Err(TagPathError::EmptyKey);
    }
    Ok(key.to_string())
}

/// Splits the `{tag}` path parameter into `(scope, key)` for the v1 FLAT
/// alias only (docs/api.md §8.1): a `resource.`/`span.` prefix scopes the
/// lookup; a leading `.` or a bare key is unscoped, which since issue
/// #475 means the five attribute scopes of `ATTR_SCOPES` — never the
/// writer-reserved intrinsic ones. The remainder after the prefix is the
/// verbatim attribute key (`resource.service.name` → scope `resource`,
/// key `service.name`); an empty remainder is a `400`.
///
/// Deliberately NOT intrinsic-aware: the v1 flat route keeps the
/// attribute-only reading, matching the reference, which answers the
/// same lookup differently on its v1 and v2 routes (ledger row
/// `traceql-v1-tag-values-statics-unimplemented`). The native and v2
/// routes use [`parse_tag_lookup`].
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

    use std::collections::BTreeSet;

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

    /// P4: 1 to 32 hex characters, left-padded with zeros to 32, the
    /// reference's `HexStringToTraceID`.
    #[test]
    fn a_trace_id_of_1_to_32_hex_characters_is_left_padded() {
        assert_eq!(
            parse_trace_id("11").unwrap(),
            format!("{}11", "0".repeat(30))
        );
        let thirty_one = "4bf92f3577b34da6a3ce929d0e0e473";
        assert_eq!(thirty_one.len(), 31);
        assert_eq!(
            parse_trace_id(thirty_one).unwrap(),
            format!("0{thirty_one}")
        );
        assert_eq!(parse_trace_id("A").unwrap(), format!("{}a", "0".repeat(31)));
        for raw in ["4bf92f3577b34da6a3ce929d0e0e47360", "", "zz"] {
            assert!(parse_trace_id(raw).is_err(), "{raw:?} must be refused");
        }
        assert_eq!(
            TraceIdError::InvalidLength("x".to_string()).to_string(),
            r#"invalid trace id "x": expected 1 to 32 hex characters"#
        );
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

    // -- search params ---------------------------------------------------

    #[test]
    fn a_minimal_q_request_parses_with_defaults() {
        let p = parse_search_params("q=%7B%7D&start=100&end=200").unwrap();
        assert_eq!(p.q.as_ref().map(TraceQlText::as_str), Some("{}"));
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
        // Seconds: ten characters or fewer.
        assert_eq!(
            parse_timestamp_ns("1700000000"),
            Some(1_700_000_000_000_000_000)
        );
        assert_eq!(parse_timestamp_ns("-100"), Some(-100_000_000_000));
        // Nanoseconds: longer than ten characters, passed through
        // verbatim — eleven digits included, though under 10^12.
        assert_eq!(parse_timestamp_ns("99999999999"), Some(99_999_999_999));
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

    /// P1: the reference's `parseTimestamp` — ten characters or fewer is
    /// seconds, longer nanoseconds, a `.` is fractional seconds rounded to
    /// the millisecond, anything else RFC3339.
    #[test]
    fn trace_timestamps_follow_the_reference_rule() {
        assert_eq!(
            parse_timestamp_ns("1700000000"),
            Some(1_700_000_000_000_000_000)
        );
        assert_eq!(parse_timestamp_ns("17000000000"), Some(17_000_000_000));
        assert_eq!(
            parse_timestamp_ns("1700000000.25"),
            Some(1_700_000_000_250_000_000)
        );
        assert_eq!(
            parse_timestamp_ns("1700000000.0004"),
            Some(1_700_000_000_000_000_000)
        );
        assert_eq!(
            parse_timestamp_ns("2026-10-20T10:00:00.5Z"),
            Some(1_792_490_400_500_000_000)
        );
        assert_eq!(parse_timestamp_ns("abc"), None);
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

    /// P2: `spss=0` is the default 3 (`engine.go:120-123`); `spss` over
    /// 100 and `limit` over 262,144 are refused, in the reference's words.
    #[test]
    fn limit_and_spss_follow_the_reference() {
        let parse = |extra: &str| parse_search_params(&format!("start=1&end=2&{extra}"));
        assert_eq!(parse("spss=0").unwrap().spss, 3);
        assert_eq!(parse("spss=100").unwrap().spss, 100);
        let err = parse("spss=101").unwrap_err();
        assert!(
            matches!(err, SearchParamError::SpssTooLarge(101)),
            "{err:?}"
        );
        assert_eq!(
            err.to_string(),
            "spans per span set exceeds 100. received 101"
        );
        assert_eq!(parse("limit=262144").unwrap().limit, 262_144);
        let err = parse("limit=262145").unwrap_err();
        assert!(
            matches!(err, SearchParamError::LimitTooLarge(262_145)),
            "{err:?}"
        );
        assert_eq!(err.to_string(), "limit 262145 exceeds max limit 262144");
        assert!(matches!(
            parse("limit=0"),
            Err(SearchParamError::InvalidCount { .. })
        ));
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
    const NOW_NS: i64 = NOW_S * 1_000_000_000;
    const S_NS: i64 = 1_000_000_000;

    #[test]
    fn a_minimal_metrics_request_parses_with_the_derived_step() {
        let p = parse_metrics_params(
            "q=%7B%7D%20%7C%20rate()&start=1700000000&end=1700003600",
            NOW_NS,
            MetricsForm::Range,
        )
        .unwrap();
        assert_eq!(p.q.as_str(), "{} | rate()");
        assert_eq!(p.start_ns, 1_700_000_000_000_000_000);
        assert_eq!(p.end_ns, 1_700_003_600_000_000_000);
        // Fence D: 3600 s / 240 = 15 s, already a multiple of 5 s.
        assert_eq!(p.step_ms, 15_000);
    }

    #[test]
    fn a_short_window_derives_the_fifty_millisecond_step_floor() {
        // Under one minute the derived step is a multiple of 50 ms, at
        // least 50 ms: 10 s / 240 is under 50 ms.
        let p =
            parse_metrics_params("q=%7B%7D&start=100&end=110", NOW_NS, MetricsForm::Range).unwrap();
        assert_eq!(p.step_ms, 50);
    }

    #[test]
    fn the_query_alias_key_is_accepted_and_q_wins_over_it() {
        let p =
            parse_metrics_params("query=%7B%7D&start=1&end=2", NOW_NS, MetricsForm::Range).unwrap();
        assert_eq!(p.q.as_str(), "{}");
        let p = parse_metrics_params(
            "q=%7B%7D&query=%7B%20%7D&start=1&end=2",
            NOW_NS,
            MetricsForm::Range,
        )
        .unwrap();
        assert_eq!(p.q.as_str(), "{}");
    }

    // --- issue #284: the TraceQL expression cap, at the reference's
    // layer (the `q`/`query` parameter) and on the reference's boundary
    // (`len > cap`, an INCLUSIVE maximum).

    /// A `q` value of `len` bytes, percent-encoded so `parse_pairs`
    /// decodes back to exactly `len` bytes: `%7B%7D` is the `{}` prefix
    /// (2 decoded bytes), padded with a comment-free run of `x`.
    ///
    /// The padded form is NOT valid TraceQL, which is fine for the pure
    /// LENGTH-boundary tests below — the reference measures length before
    /// it parses (`async_query_validator_middleware.go:45` precedes the
    /// `ParseNoOptimizations` call at `:49`), so an over-cap value is
    /// rejected on length whatever it contains. It is NOT fine for any
    /// test that reasons about what the reference would ACCEPT; those use
    /// [`valid_q_of_decoded_len`] (issue #284 review round 3). Since
    /// issue #326 that also covers anything AT the cap on the `query`
    /// parameter, which now reaches the parse step.
    fn q_of_decoded_len(len: usize) -> String {
        assert!(len >= 2);
        format!("%7B%7D{}", "x".repeat(len - 2))
    }

    /// A **parse-valid** TraceQL expression of exactly `len` decoded
    /// bytes, returned percent-encoded. Shape: `{ .k = "xx…x" }` — 11
    /// bytes of frame plus the padding. Round-tripped through the real
    /// parser by `the_valid_fixture_helper_really_produces_parseable_traceql`,
    /// so a test that claims "the reference would accept this" is backed
    /// by a fixture that can actually be accepted.
    fn valid_q_of_decoded_len(len: usize) -> String {
        const FRAME: usize = 11; // `{ .k = "` + `" }`
        assert!(len >= FRAME);
        let decoded = format!(r#"{{ .k = "{}" }}"#, "x".repeat(len - FRAME));
        assert_eq!(decoded.len(), len);
        percent_encode_query_value(&decoded)
    }

    /// Encodes every byte that is not an unreserved URL character, so the
    /// value survives `parse_pairs` unchanged (`&`, `=`, `+`, `%`, space,
    /// quotes and braces all matter here).
    fn percent_encode_query_value(s: &str) -> String {
        let mut out = String::with_capacity(s.len() * 3);
        for b in s.bytes() {
            if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
                out.push(b as char);
            } else {
                out.push_str(&format!("%{b:02X}"));
            }
        }
        out
    }

    #[test]
    fn the_valid_fixture_helper_really_produces_parseable_traceql() {
        for len in [11usize, 32, 1024, 131_072, 131_073] {
            let encoded = valid_q_of_decoded_len(len);
            let pairs = parse_pairs(&format!("q={encoded}"));
            let decoded = get(&pairs, "q").expect("q survives the round trip");
            assert_eq!(decoded.len(), len, "decoded length must be exact");
            assert!(
                pulsus_traceql::parse(decoded).is_ok(),
                "fixture of {len} bytes must parse: {:?}…",
                &decoded[..decoded.len().min(40)]
            );
        }
    }

    #[test]
    fn a_search_q_of_exactly_the_cap_is_accepted_and_one_byte_more_is_rejected() {
        let cap = super::super::querytext::MAX_QUERY_EXPRESSION_BYTES;
        let ok = parse_search_params(&format!("q={}&start=1&end=2", q_of_decoded_len(cap)))
            .expect("exactly the cap is accepted — the reference compares `>`");
        assert_eq!(
            ok.q.as_ref().map(TraceQlText::as_str).map(str::len),
            Some(cap)
        );

        let err = parse_search_params(&format!("q={}&start=1&end=2", q_of_decoded_len(cap + 1)))
            .expect_err("one byte over the cap is rejected");
        assert!(
            matches!(
                err,
                SearchParamError::QueryText(QueryTextError::TooLong { len, cap: c })
                    if len == cap + 1 && c == cap
            ),
            "got {err:?}"
        );
    }

    /// Issue #284 review, contested `[high]` — SETTLED FROM SOURCE, run 1
    /// was right on the fact. grafana/tempo v3.0.2 wires
    /// `queryValidatorWare` at `modules/frontend/frontend.go:159` (the
    /// SEARCH pipeline), `:215` (query_range) and `:229` (query_instant),
    /// and the middleware reads `q` then `query`
    /// (`async_query_validator_middleware.go:38-41`) regardless of which
    /// pipeline it sits in. So `/api/search?query=<over-cap>` is a `400`
    /// in the reference; before this fix PulsusDB ignored `query` on
    /// search entirely and served a time-only search `200`.
    ///
    /// The cap is where the parity is; the SELECTION is not. Tempo's
    /// search request parser reads `q` alone (`pkg/api/http.go:180`,
    /// `urlParamQuery = "q"`), and a lone `query` falls into the legacy
    /// tag map at `:222` — so `query` must NOT become an alias that
    /// drives the search.
    #[test]
    fn a_search_query_parameter_is_capped_but_never_becomes_the_search_expression() {
        let cap = super::super::querytext::MAX_QUERY_EXPRESSION_BYTES;

        // At the cap: accepted, and it does NOT populate `q`. Parse-valid
        // padding, because since issue #326 an accepted `query` has to
        // clear the parse step as well as the cap.
        let ok = parse_search_params(&format!(
            "query={}&start=1&end=2",
            valid_q_of_decoded_len(cap)
        ))
        .expect("exactly the cap is accepted");
        assert!(
            ok.q.is_none(),
            "`query` must not become the search expression"
        );

        // One byte over: rejected, as the reference's validator rejects it.
        let err = parse_search_params(&format!(
            "query={}&start=1&end=2",
            q_of_decoded_len(cap + 1)
        ))
        .expect_err("over-cap `query` is a 400 on the search route");
        assert!(
            matches!(
                err,
                SearchParamError::QueryText(QueryTextError::TooLong { len, cap: c })
                    if len == cap + 1 && c == cap
            ),
            "got {err:?}"
        );

        // An empty `query` is not measured — the reference's `if
        // traceQLQuery != ""` guard — and stays a plain time-only search.
        assert!(parse_search_params("query=&start=1&end=2").is_ok());
    }

    // --- issue #326: the validator's SECOND check, the parse -------------

    /// The defect this issue names, and unlike the cap it is reachable
    /// over the wire: a malformed `query` is short, so `http::Uri`'s
    /// 65,534-byte request-target limit does not hide it.
    ///
    /// `async_query_validator_middleware.go:49-55` parses the selected
    /// parameter and turns any error into `400 invalid TraceQL query: …`
    /// (`NewBadRequest`, `modules/frontend/pipeline/responses.go:69`).
    /// PulsusDB served these `200` before: `query` is not the searched
    /// expression here (`pkg/api/http.go:180` reads `q` alone), so
    /// nothing downstream ever looked at it. It still is not the searched
    /// expression — the accepted case below proves `q` stays `None`.
    #[test]
    fn a_malformed_search_query_parameter_is_rejected_though_it_never_runs() {
        for raw in [
            "%7B",                  // `{` — truncated
            "%7B%20.a%20%3D%20%7D", // `{ .a = }` — value missing
            "%7B%7D%20%7C",         // `{} |` — dangling pipeline
            "not+traceql+at+all",
        ] {
            let err = parse_search_params(&format!("query={raw}&start=1&end=2"))
                .expect_err("a malformed `query` is a 400 on the search route");
            assert!(
                matches!(
                    err,
                    SearchParamError::QueryText(QueryTextError::Invalid { .. })
                ),
                "{raw}: got {err:?}"
            );
            assert!(
                err.to_string().starts_with("invalid TraceQL query: "),
                "{raw}: the reference's `:54` wrapping, got {err}"
            );
            // Issue #384: the offset reaches the client inside the
            // message, not in a `position` field — every one of these
            // parse rejections still names a byte.
            assert!(
                err.to_string().contains("byte "),
                "{raw}: parse errors name their offset, got {err}"
            );
        }

        // Accepted, and STILL not the search expression: the parse step
        // changes what is rejected, never which parameter drives the
        // search (settled on issue #282).
        let ok = parse_search_params("query=%7B%7D&start=1&end=2").expect("`{}` parses");
        assert!(ok.q.is_none(), "`query` must not become the expression");

        // A metrics pipeline parses and passes `traceql.Validate`, so the
        // reference's validator accepts it on the search route too — it
        // simply never becomes the searched expression.
        let ok = parse_search_params("query=%7B%7D%20%7C%20rate()&start=1&end=2")
            .expect("a metrics pipeline is validator-clean on search");
        assert!(ok.q.is_none());
    }

    /// The two checks in the reference's order: `:45` returns before
    /// `:49` runs, so an over-cap value that is ALSO unparseable is a
    /// `TooLong`, not an `Invalid`. Swapping them flips this.
    #[test]
    fn an_over_cap_and_unparseable_query_is_rejected_on_length_not_on_parse() {
        let cap = super::super::querytext::MAX_QUERY_EXPRESSION_BYTES;
        let raw = q_of_decoded_len(cap + 1);
        // The fixture must genuinely fail both checks for the ordering to
        // be under test at all.
        let decoded = get(&parse_pairs(&format!("q={raw}")), "q")
            .expect("round trip")
            .to_string();
        assert_eq!(decoded.len(), cap + 1);
        assert!(pulsus_traceql::parse(&decoded).is_err());

        let err = parse_search_params(&format!("query={raw}&start=1&end=2"))
            .expect_err("rejected either way");
        assert!(
            matches!(
                err,
                SearchParamError::QueryText(QueryTextError::TooLong { .. })
            ),
            "got {err:?}"
        );
    }

    /// PulsusDB caps BOTH search query parameters; the reference caps
    /// only the one its last-write-wins selection lands on. This is the
    /// test that pins that deliberate divergence, so it has to be able to
    /// fail — review round 3 found the earlier version could not.
    ///
    /// Two fixes, both load-bearing:
    ///
    /// 1. **Parse-valid fixtures.** The old version padded with `x`,
    ///    which is not TraceQL, so the reference would have rejected the
    ///    selected `query` at parse time and the "the reference accepts
    ///    this" premise was false. Both values now parse.
    /// 2. **Both directions.** The rejection in the `q`-over-cap case
    ///    comes from the `q` cap, so deleting the `query` cap could never
    ///    redden it. The `query`-over-cap direction is added, and that
    ///    half does redden when the `query` cap is deleted. Between them
    ///    the assertion "we cap both" has a mutant for each conjunct:
    ///    adopting the reference's selection reddens the first, dropping
    ///    the `query` cap reddens the second.
    ///
    /// The divergence itself: the reference reads `q` and then lets
    /// `query` overwrite it, so an over-cap `q` beside an under-cap
    /// `query` is never measured — and the search handler, which reads
    /// `q` alone, then executes that unbounded text. That is the
    /// validator and the handler disagreeing about which parameter
    /// matters. PulsusDB bounds both: strictly safer, divergent only on
    /// this shape, and unreachable over the wire anyway (`http::Uri` caps
    /// the request target at 65,534 bytes on these GET-only routes).
    #[test]
    fn both_search_query_parameters_are_capped_where_the_reference_caps_only_the_selected_one() {
        let cap = super::super::querytext::MAX_QUERY_EXPRESSION_BYTES;
        let over = valid_q_of_decoded_len(cap + 1);
        let under = valid_q_of_decoded_len(64);

        // Direction A — the divergence. The reference selects `query`
        // (under cap, parses fine), never measures `q`, and then runs the
        // over-cap `q`. We reject.
        let err = parse_search_params(&format!("q={over}&query={under}&start=1&end=2"))
            .expect_err("PulsusDB bounds the expression it will actually execute");
        assert!(
            matches!(
                err,
                SearchParamError::QueryText(QueryTextError::TooLong { len, .. })
                    if len == cap + 1
            ),
            "direction A: got {err:?}"
        );

        // Direction B — plain parity, and the half that binds the `query`
        // cap: the reference selects the over-cap `query` and rejects, and
        // so do we.
        let err = parse_search_params(&format!("q={under}&query={over}&start=1&end=2"))
            .expect_err("an over-cap `query` is rejected, as the reference rejects it");
        assert!(
            matches!(
                err,
                SearchParamError::QueryText(QueryTextError::TooLong { len, .. })
                    if len == cap + 1
            ),
            "direction B: got {err:?}"
        );

        // Control: both under the cap and both parse — accepted, and `q`
        // (never `query`) is what the search will run.
        let ok = parse_search_params(&format!("q={under}&query={under}&start=1&end=2"))
            .expect("two under-cap expressions are fine");
        assert_eq!(
            ok.q.as_ref().map(TraceQlText::as_str).map(str::len),
            Some(64)
        );
    }

    #[test]
    fn both_metrics_query_keys_are_capped_on_the_same_boundary() {
        let cap = super::super::querytext::MAX_QUERY_EXPRESSION_BYTES;
        for key in ["q", "query"] {
            let ok = parse_metrics_params(
                &format!("{key}={}&start=1&end=2", q_of_decoded_len(cap)),
                NOW_NS,
                MetricsForm::Range,
            )
            .unwrap_or_else(|e| panic!("{key} at the cap must be accepted, got {e:?}"));
            assert_eq!(ok.q.as_str().len(), cap);

            let err = parse_metrics_params(
                &format!("{key}={}&start=1&end=2", q_of_decoded_len(cap + 1)),
                NOW_NS,
                MetricsForm::Range,
            )
            .expect_err("one byte over the cap is rejected");
            assert!(
                matches!(
                    err,
                    MetricsParamError::QueryText(QueryTextError::TooLong { len, .. })
                        if len == cap + 1
                ),
                "{key}: got {err:?}"
            );
        }
    }

    /// SCOPE, not just value: the reference's middleware measures the
    /// `q`/`query` parameter only, so the expression the LEGACY params
    /// compile to is uncapped there — and legacy compilation expands its
    /// input enough to cross the cap on its own. Capping it would be a
    /// rejection the reference does not have.
    #[test]
    fn a_legacy_tags_request_is_not_capped_even_when_it_compiles_over_the_cap() {
        let cap = super::super::querytext::MAX_QUERY_EXPRESSION_BYTES;
        // ONE logfmt pair with a long value — `a=<x…>` decoded,
        // `{ .a="<x…>" }` compiled, ten bytes wider than its input and
        // comfortably past the cap. Deliberately one conjunct, not many:
        // this test is about the cap's SCOPE, not about conjunct counts.
        let tags = format!("a%3D{}", "x".repeat(cap));
        let p = parse_search_params(&format!("tags={tags}&start=1&end=2"))
            .expect("legacy params carry no TraceQL expression to cap");
        assert!(p.q.is_none());

        let compiled = super::super::legacy::compile_legacy(p.tags.as_deref(), None, None)
            .expect("logfmt compiles");
        assert!(
            compiled.len() > cap,
            "the fixture must actually exceed the cap to be a scope test, got {}",
            compiled.len()
        );
        // And it still parses — i.e. the request is genuinely served.
        assert!(pulsus_traceql::parse(&compiled).is_ok());
    }

    #[test]
    fn a_missing_query_expression_is_rejected() {
        for raw in ["start=1&end=2", "q=&start=1&end=2"] {
            assert!(
                matches!(
                    parse_metrics_params(raw, NOW_NS, MetricsForm::Range),
                    Err(MetricsParamError::MissingQuery)
                ),
                "{raw} must be rejected"
            );
        }
    }

    /// AC6(i), accept half: the complete step spelling set measured
    /// `200` on the reference, each pinned to its exact millisecond
    /// value. `500ms` is the spelling the issue was filed on; the
    /// compound and fractional forms are what the Step box lets a user
    /// type.
    #[test]
    fn the_step_grammar_accepts_sub_second_and_compound_forms() {
        for (raw, expected_ms) in [
            ("30s", 30_000),
            ("500ms", 500),
            ("100ms", 100),
            ("3ms", 3),
            ("1.5s", 1_500),
            ("1500ms", 1_500),
            ("0.5s", 500),
            (".5s", 500),
            ("%2B30s", 30_000),
            ("2.5s", 2_500),
            ("1m30s", 90_000),
            ("1s500ms", 1_500),
            ("1h30m", 5_400_000),
            ("30", 30_000),
            ("1m", 60_000),
            ("1h", 3_600_000),
            ("60000ms", 60_000),
            ("5m", 300_000),
        ] {
            let p = parse_metrics_params(
                &format!("q=%7B%7D&start=1&end=7201&step={raw}"),
                NOW_NS,
                MetricsForm::Range,
            )
            .unwrap_or_else(|e| panic!("step={raw} must be accepted, got {e}"));
            assert_eq!(p.step_ms, expected_ms, "step={raw}");
        }
    }

    /// AC6(i), reject half. `1.5ms`/`3.5ms`/`100.25ms` are the
    /// whole-millisecond bound — the reference answers `200` for each
    /// above its own floor and we deliberately do not
    /// (`traceql-metrics-fractional-ms-step-rejected`). `100.25ms` is far
    /// above one millisecond and still rejected, which is why the bound
    /// is "not a whole number of milliseconds" and not "sub-millisecond".
    #[test]
    fn non_whole_millisecond_and_malformed_steps_are_rejected() {
        for raw in [
            "0", "0s", "abc", "-60", "1.5ms", "3.5ms", "100.25ms", "30S", "5e2ms", "500us", "2d",
            "",
        ] {
            let query = format!("q=%7B%7D&start=1&end=7201&step={raw}");
            let result = parse_metrics_params(&query, NOW_NS, MetricsForm::Range);
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

    /// AC5(ii): the `exemplars` request parameter is READ, and its
    /// normalised VALUE is what this asserts.
    ///
    /// The value is the point. Before issue #477 the parameter was not
    /// looked for anywhere, so every one of these requests parsed to a
    /// `200`-shaped params struct and a status-only assertion would have
    /// passed at base for the wrong reason.
    #[test]
    fn the_exemplars_parameter_normalises_by_value() {
        let parse = |suffix: &str| {
            parse_metrics_params(
                &format!("q=%7B%7D&start=1&end=7201&step=60{suffix}"),
                NOW_NS,
                MetricsForm::Range,
            )
            .unwrap_or_else(|e| panic!("exemplars{suffix:?} must not be an error, got {e}"))
        };
        assert_eq!(parse("").exemplars, None, "absent");
        assert_eq!(parse("&exemplars=").exemplars, None, "empty");
        assert_eq!(parse("&exemplars=0").exemplars, None, "zero");
        assert_eq!(parse("&exemplars=-1").exemplars, None, "negative");
        assert_eq!(parse("&exemplars=abc").exemplars, None, "non-numeric");
        assert_eq!(parse("&exemplars=5").exemplars, Some(5));
        assert_eq!(parse("&exemplars=100").exemplars, Some(100));
        // Clamping to the ceiling is the planner's job, not the parser's:
        // the parameter is carried whole so the precedence rule sees what
        // the client actually sent.
        assert_eq!(parse("&exemplars=100000").exemplars, Some(100_000));
    }

    #[test]
    fn since_sets_the_default_start_and_yields_to_an_absolute_one() {
        let p = parse_metrics_params("q=%7B%7D&since=1h", NOW_NS, MetricsForm::Range).unwrap();
        assert_eq!((p.start_ns, p.end_ns), (NOW_NS - 3_600 * S_NS, NOW_NS));
        let p = parse_metrics_params(
            "q=%7B%7D&since=1h&start=1699999000",
            NOW_NS,
            MetricsForm::Range,
        )
        .unwrap();
        assert_eq!((p.start_ns, p.end_ns), (1_699_999_000 * S_NS, NOW_NS));
        let p = parse_metrics_params(
            "q=%7B%7D&since=1h&end=1699990000",
            NOW_NS,
            MetricsForm::Range,
        )
        .unwrap();
        assert_eq!(
            (p.start_ns, p.end_ns),
            ((1_699_990_000 - 3_600) * S_NS, 1_699_990_000 * S_NS)
        );
        assert!(matches!(
            parse_metrics_params("q=%7B%7D&since=soon", NOW_NS, MetricsForm::Range),
            Err(MetricsParamError::InvalidSince(_))
        ));
    }

    #[test]
    fn a_missing_or_partial_range_takes_the_defaults() {
        for (raw, want) in [
            ("q=%7B%7D", (NOW_NS - 3_600 * S_NS, NOW_NS)),
            ("q=%7B%7D&start=1699999000", (1_699_999_000 * S_NS, NOW_NS)),
            (
                "q=%7B%7D&end=1699990000",
                ((1_699_990_000 - 3_600) * S_NS, 1_699_990_000 * S_NS),
            ),
        ] {
            let p = parse_metrics_params(raw, NOW_NS, MetricsForm::Range)
                .unwrap_or_else(|e| panic!("{raw}: {e}"));
            assert_eq!((p.start_ns, p.end_ns), want, "{raw}");
        }
    }

    /// P5: the reference's `determineBounds` — `end` defaults to now,
    /// `start` to `since` before `min(end, now)`, `since` defaults to one
    /// hour, is a Prometheus duration, and is read only when `start` is
    /// absent; `q` wins over `query`.
    #[test]
    fn metrics_windows_take_the_reference_defaults() {
        let window = |raw: &str| {
            let p = parse_metrics_params(&format!("q=%7B%7D{raw}"), NOW_NS, MetricsForm::Range)
                .unwrap_or_else(|e| panic!("{raw}: {e}"));
            (p.start_ns, p.end_ns)
        };
        assert_eq!(window(""), (NOW_NS - 3_600 * S_NS, NOW_NS));
        assert_eq!(window("&start=1699999000"), (1_699_999_000 * S_NS, NOW_NS));
        assert_eq!(
            window("&end=1699990000"),
            ((1_699_990_000 - 3_600) * S_NS, 1_699_990_000 * S_NS)
        );
        assert_eq!(
            window("&end=1700000600"),
            (NOW_NS - 3_600 * S_NS, 1_700_000_600 * S_NS),
            "an end after now: start is since before now"
        );
        for (since, secs) in [
            ("1h30m", 5_400),
            ("1d", 86_400),
            ("2w", 14 * 86_400),
            ("1y", 365 * 86_400),
        ] {
            assert_eq!(
                window(&format!("&since={since}")),
                (NOW_NS - secs * S_NS, NOW_NS),
                "since={since}"
            );
        }
        assert!(
            matches!(
                parse_metrics_params("q=%7B%7D&since=0", NOW_NS, MetricsForm::Range),
                Err(MetricsParamError::InvalidRange { .. })
            ),
            "since=0 parses and leaves an empty window, a 400"
        );
        assert_eq!(
            window("&since=1h&start=1699999000"),
            (1_699_999_000 * S_NS, NOW_NS)
        );
        let p = parse_metrics_params(
            "q=%7B%7D&query=%7B%20%7D&start=1&end=2",
            NOW_NS,
            MetricsForm::Range,
        )
        .unwrap();
        assert_eq!(p.q.as_str(), "{}");
        for bad in ["1.5h", "1h1h", "30m1h", "-1h", "h"] {
            assert!(
                matches!(
                    parse_metrics_params(
                        &format!("q=%7B%7D&since={bad}"),
                        NOW_NS,
                        MetricsForm::Range
                    ),
                    Err(MetricsParamError::InvalidSince(_))
                ),
                "since={bad}"
            );
        }
    }

    /// P6: the reference's `DefaultQueryRangeStep` — the window over 240,
    /// rounded down as fence D states.
    #[test]
    fn the_default_step_is_the_reference_s() {
        let base = 1_700_000_000 * S_NS;
        for (width_ms, want_ms) in [
            (3_600_000, 15_000),
            (86_400_000, 300_000),
            (7 * 86_400_000, 2_400_000),
            (50_000, 200),
            (30_000, 100),
            (10_000, 50),
            (61_000, 1_000),
            (59_999, 200),
            (60_000, 1_000),
        ] {
            let end = base + width_ms * 1_000_000;
            let p = parse_metrics_params(
                &format!("q=%7B%7D&start={base}&end={end}"),
                NOW_NS,
                MetricsForm::Range,
            )
            .unwrap();
            assert_eq!(p.step_ms, want_ms, "a {width_ms} ms window");
        }
    }

    /// P7: the instant request is one bucket; it reads no `step`.
    #[test]
    fn the_instant_route_reads_no_step() {
        for step in ["1.5ms", "junk"] {
            let raw = format!("q=%7B%7D&start=1700000000&end=1700003600&step={step}");
            assert!(
                parse_metrics_params(&raw, NOW_NS, MetricsForm::Instant).is_ok(),
                "step={step}"
            );
        }
    }

    /// P11: the first kept second under the tables' TTL, and the start a
    /// windowed read uses.
    #[test]
    fn the_retention_floor_is_the_first_kept_second() {
        assert_eq!(
            retention_floor_ns(1, 1_000_000 * S_NS + 500_000_000),
            (1_000_000 - 86_400 + 1) * S_NS
        );
        assert_eq!(floored_start(10, 100, 50), 50);
        assert_eq!(floored_start(70, 100, 50), 70);
        assert_eq!(floored_start(10, 100, 100), 100);
        assert_eq!(floored_start(10, 100, 150), 100);
    }

    #[test]
    fn metrics_accepts_nanosecond_and_rfc3339_bounds() {
        // The metrics endpoints share the search surface's timestamp
        // grammar (one parser — code review round 1 on issue #59).
        let p = parse_metrics_params(
            "q=%7B%7D&start=1700000000000000000&end=2023-11-14T23:13:20Z",
            NOW_NS,
            MetricsForm::Range,
        )
        .unwrap();
        assert_eq!(p.start_ns, 1_700_000_000_000_000_000);
        assert_eq!(p.end_ns, 1_700_003_600_000_000_000);
        // Derived step over the ns-precision window: 3600 s / 240 = 15 s.
        assert_eq!(p.step_ms, 15_000);
    }

    #[test]
    fn metrics_extreme_ns_bounds_never_wrap_the_derived_step() {
        // Width > i64::MAX in nanoseconds: the i128 derivation still
        // produces a sane positive step (the planner's cap then 422s the
        // request downstream — never a wrapped/negative step here).
        let p = parse_metrics_params(
            &format!("q=%7B%7D&start={}&end={}", i64::MIN, i64::MAX),
            NOW_NS,
            MetricsForm::Range,
        )
        .unwrap();
        assert!(p.step_ms > 0);
        // Fence D: the width over 240, rounded down to whole hours.
        let hour_ns = 3_600 * i128::from(S_NS);
        let per_point = i128::from(u64::MAX) / 240;
        assert_eq!(
            i128::from(p.step_ms),
            per_point / hour_ns * hour_ns / 1_000_000
        );
    }

    #[test]
    fn metrics_bad_timestamps_and_inverted_ranges_are_rejected() {
        assert!(matches!(
            parse_metrics_params("q=%7B%7D&start=abc&end=2", NOW_NS, MetricsForm::Range),
            Err(MetricsParamError::InvalidTimestamp(_))
        ));
        assert!(matches!(
            parse_metrics_params("q=%7B%7D&start=200&end=100", NOW_NS, MetricsForm::Range),
            Err(MetricsParamError::InvalidRange { .. })
        ));
        assert!(matches!(
            parse_metrics_params("q=%7B%7D&start=100&end=100", NOW_NS, MetricsForm::Range),
            Err(MetricsParamError::InvalidRange { .. })
        ));
    }

    // -- service-graph params (issue #173) ---------------------------------

    #[test]
    fn a_minimal_graph_request_parses_an_absolute_window() {
        let p = parse_graph_params("start=1700000000&end=1700003600", NOW_NS).unwrap();
        assert_eq!(p.start_ns, 1_700_000_000_000_000_000);
        assert_eq!(p.end_ns, 1_700_003_600_000_000_000);
    }

    #[test]
    fn graph_since_sets_the_default_start_and_yields_to_an_absolute_one() {
        let p = parse_graph_params("since=1h", NOW_NS).unwrap();
        assert_eq!((p.start_ns, p.end_ns), (NOW_NS - 3_600 * S_NS, NOW_NS));
        let p = parse_graph_params("since=1d&start=1699999000", NOW_NS).unwrap();
        assert_eq!((p.start_ns, p.end_ns), (1_699_999_000 * S_NS, NOW_NS));
        assert!(matches!(
            parse_graph_params("since=soon", NOW_NS),
            Err(GraphParamError::InvalidSince(_))
        ));
    }

    #[test]
    fn graph_missing_or_partial_range_takes_the_defaults() {
        for (raw, want) in [
            ("", (NOW_NS - 3_600 * S_NS, NOW_NS)),
            ("start=1699999000", (1_699_999_000 * S_NS, NOW_NS)),
            (
                "end=1699990000",
                ((1_699_990_000 - 3_600) * S_NS, 1_699_990_000 * S_NS),
            ),
        ] {
            let p = parse_graph_params(raw, NOW_NS).unwrap_or_else(|e| panic!("{raw:?}: {e}"));
            assert_eq!((p.start_ns, p.end_ns), want, "{raw:?}");
        }
    }

    #[test]
    fn graph_bad_timestamps_and_inverted_ranges_are_rejected() {
        assert!(matches!(
            parse_graph_params("start=abc&end=2", NOW_NS),
            Err(GraphParamError::InvalidTimestamp(_))
        ));
        assert!(matches!(
            parse_graph_params("start=200&end=100", NOW_NS),
            Err(GraphParamError::InvalidRange { .. })
        ));
        assert!(matches!(
            parse_graph_params("start=100&end=100", NOW_NS),
            Err(GraphParamError::InvalidRange { .. })
        ));
    }

    #[test]
    fn graph_accepts_nanosecond_and_rfc3339_bounds() {
        let p = parse_graph_params("start=1700000000000000000&end=2023-11-14T23:13:20Z", NOW_NS)
            .unwrap();
        assert_eq!(p.start_ns, 1_700_000_000_000_000_000);
        assert_eq!(p.end_ns, 1_700_003_600_000_000_000);
    }

    // -- tags params (issue #58, widened by #475) --------------------------

    #[test]
    fn every_attribute_scope_parses_to_its_own_keyword() {
        // Derived from the constant, so a scope added to `ATTR_SCOPES`
        // without an accept arm fails here rather than 400-ing at
        // runtime. `assert_eq` on the resolved keyword, not on
        // membership: two arms SWAPPED would keep the accepted set the
        // same and this still fails.
        for scope in ATTR_SCOPES {
            assert_eq!(
                parse_tags_params(&format!("scope={scope}")).unwrap().scope,
                TagScope::Attribute(scope),
                "scope={scope}"
            );
        }
    }

    #[test]
    fn absent_none_and_empty_scope_all_mean_every_scope() {
        for raw in ["", "scope=none", "scope="] {
            assert_eq!(
                parse_tags_params(raw).unwrap().scope,
                TagScope::All,
                "{raw} must select every scope"
            );
        }
    }

    #[test]
    fn the_intrinsic_and_empty_scopes_parse_to_their_own_variants() {
        assert_eq!(
            parse_tags_params("scope=intrinsic").unwrap().scope,
            TagScope::Intrinsic
        );
        for scope in EMPTY_SCOPES {
            assert_eq!(
                parse_tags_params(&format!("scope={scope}")).unwrap().scope,
                TagScope::NoTags,
                "scope={scope}"
            );
        }
    }

    /// A keyword that carries no tags must never reach the SQL `IN`
    /// list: if `trace` were appended to `ATTR_SCOPES` instead, every
    /// response body would still be right and a catalog read would be
    /// issued.
    #[test]
    fn the_empty_scopes_are_disjoint_from_every_catalog_scope() {
        for scope in EMPTY_SCOPES {
            assert!(!ATTR_SCOPES.contains(&scope), "{scope} is a catalog scope");
            assert!(
                !["intrinsic", "none", ""].contains(&scope),
                "{scope} collides with a reserved keyword"
            );
        }
    }

    #[test]
    fn tags_start_end_are_accepted_and_ignored_not_errors() {
        // The bounds parse away without effect: the catalog read is bounded
        // by the retention floor alone (docs/api.md §4.3).
        let p = parse_tags_params("scope=span&start=100&end=200").unwrap();
        assert_eq!(p.scope, TagScope::Attribute("span"));
    }

    #[test]
    fn tags_unknown_scope_is_rejected_never_widened() {
        // Case-sensitive and closed: accepting `trace` and `intrinsic`
        // makes their near-misses newly interesting, so they are checked
        // rather than assumed.
        for raw in [
            "scope=bogus",
            "scope=Intrinsic",
            "scope=TRACE",
            "scope=trace ",
            "scope=Resource",
            "scope=event:intrinsic",
        ] {
            assert!(
                matches!(
                    parse_tags_params(raw),
                    Err(TagsParamError::UnsupportedScope(_))
                ),
                "{raw} must be rejected"
            );
        }
    }

    /// The `400` body lists exactly the keywords the parser accepts.
    /// Relational, not a second copy of the literal: the tokens are
    /// extracted from the RENDERED message and compared with the
    /// constants, so widening `ATTR_SCOPES` without touching the message
    /// is a compile-green test failure.
    #[test]
    fn the_unsupported_scope_message_lists_exactly_the_accepted_keywords() {
        let rendered = TagsParamError::UnsupportedScope("bogus".to_string()).to_string();
        let (subject, tail) = rendered
            .split_once("\": expected ")
            .expect("the message names the offending value, then the accepted set");
        assert_eq!(subject, "unsupported scope \"bogus");
        let listed: BTreeSet<&str> = tail
            .split('"')
            .skip(1)
            .step_by(2)
            .filter(|t| !t.is_empty())
            .collect();
        let expected: BTreeSet<&str> = ATTR_SCOPES
            .iter()
            .copied()
            .chain(EMPTY_SCOPES)
            .chain(["intrinsic", "none"])
            .collect();
        assert_eq!(listed, expected, "message: {rendered}");
    }

    // -- the `{tag}` lookup (issue #475) ------------------------------------

    #[test]
    fn a_scope_prefixed_tag_is_an_attribute_lookup_even_when_the_key_is_an_intrinsic() {
        for scope in ATTR_SCOPES {
            assert_eq!(
                parse_tag_lookup(&format!("{scope}.name")).unwrap(),
                TagLookup::Attribute {
                    scope: Some(scope.to_string()),
                    key: "name".to_string(),
                },
                "{scope}.name"
            );
        }
        assert_eq!(
            parse_tag_lookup(".status").unwrap(),
            TagLookup::Attribute {
                scope: None,
                key: "status".to_string(),
            }
        );
    }

    /// One character apart, two different resolvers, two different
    /// answers — the confusion this issue exists to remove.
    #[test]
    fn the_dot_and_colon_forms_of_one_name_resolve_differently() {
        assert_eq!(
            parse_tag_lookup("link.spanID").unwrap(),
            TagLookup::Attribute {
                scope: Some("link".to_string()),
                key: "spanID".to_string(),
            }
        );
        assert_eq!(
            parse_tag_lookup("link:spanID").unwrap(),
            TagLookup::Intrinsic(pulsus_traceql::Intrinsic::LinkSpanId)
        );
    }

    #[test]
    fn every_served_intrinsic_spelling_resolves_to_an_intrinsic_lookup() {
        for &intrinsic in pulsus_traceql::Intrinsic::ALL {
            for spelling in intrinsic.discovery_spellings() {
                assert_eq!(
                    parse_tag_lookup(spelling).unwrap(),
                    TagLookup::Intrinsic(intrinsic),
                    "{spelling}"
                );
            }
        }
    }

    #[test]
    fn a_bare_non_intrinsic_key_is_an_unscoped_attribute_lookup() {
        // `spanID` is a reserved intrinsic KEY but not an intrinsic
        // SPELLING, so it stays an attribute lookup — and the SQL's
        // `scope IN` list is what keeps the reserved rows out of it.
        assert_eq!(
            parse_tag_lookup("spanID").unwrap(),
            TagLookup::Attribute {
                scope: None,
                key: "spanID".to_string(),
            }
        );
        assert_eq!(
            parse_tag_lookup("service.name").unwrap(),
            TagLookup::Attribute {
                scope: None,
                key: "service.name".to_string(),
            }
        );
    }

    #[test]
    fn an_empty_key_is_rejected_in_every_branch() {
        for raw in ["", ".", "resource.", "span.", "event."] {
            assert!(
                matches!(parse_tag_lookup(raw), Err(TagPathError::EmptyKey)),
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

    // -- issue #478: the §4.3 range contract ---------------------------

    /// One second, in nanoseconds — the unit `parse_range_params` returns.
    const S: i64 = 1_000_000_000;

    fn pairs(raw: &str) -> Vec<(String, String)> {
        parse_pairs(raw)
    }

    /// Issue #587: the fetch routes' optional window.
    ///
    /// **Both bounds or neither**, because the window is read only by the
    /// fallback and a half-open request has no window to answer over. An
    /// empty or inverted one yields `None` rather than a refusal: the
    /// indexed statement never reads the window, so the only thing a
    /// window can do is ADD an answer, and a window that cannot contain a
    /// span adds nothing.
    #[test]
    fn the_fetch_window_takes_both_bounds_or_neither() {
        let ns = |s, e| {
            Some(FetchWindow {
                start_ns: s,
                end_ns: e,
            })
        };
        for (raw, want) in [
            (None, None),
            (Some(""), None),
            (Some("limit=20"), None),
            // one bound alone is the same as none
            (Some("start=1699999999"), None),
            (Some("end=1700000002"), None),
            (Some("start=&end=1700000002"), None),
            // seconds, the trace surface's own grammar
            (
                Some("start=1699999999&end=1700000002"),
                ns(1_699_999_999_000_000_000, 1_700_000_002_000_000_000),
            ),
            // nanoseconds, longer than ten characters
            (
                Some("start=1699999999000000000&end=1700000002000000000"),
                ns(1_699_999_999_000_000_000, 1_700_000_002_000_000_000),
            ),
            // RFC3339
            (
                Some("start=2023-11-14T22:13:19Z&end=2023-11-14T22:13:22Z"),
                ns(1_699_999_999_000_000_000, 1_700_000_002_000_000_000),
            ),
            // an empty window and an inverted one add nothing
            (Some("start=1700000002&end=1700000002"), None),
            (Some("start=1700000002&end=1699999999"), None),
            // every other parameter is accepted and ignored
            (
                Some("limit=20&start=1699999999&end=1700000002&spss=3"),
                ns(1_699_999_999_000_000_000, 1_700_000_002_000_000_000),
            ),
        ] {
            assert_eq!(
                parse_fetch_window(raw).expect("a well-formed window"),
                want,
                "for {raw:?}"
            );
        }
    }

    /// A bound that is present and unparseable is a `400`, the same
    /// refusal and the same message as the search surface's: accepting it
    /// silently would answer over a window the caller did not ask for.
    ///
    /// **A LONE malformed bound is refused too**, and that is the half
    /// this case exists for. "Both bounds or neither" decides which
    /// window to *answer over*; it was never a licence to stop *reading*
    /// a bound the caller supplied. A lone `start=yesterday` discarded in
    /// silence is the same wrong as a pair with one bad half — the caller
    /// asked for a window and got an answer over a different one — and it
    /// is the likelier shape, because a half-filled time control sends
    /// one bound.
    #[test]
    fn an_unparseable_fetch_window_bound_is_refused() {
        for raw in [
            "start=yesterday&end=1700000002",
            "start=1699999999&end=tomorrow",
            // lone, with no partner at all
            "start=yesterday",
            "end=tomorrow",
            // lone, with an empty partner — the empty value is discarded
            // as absent, so this is the lone case by a second route
            "start=yesterday&end=",
            "start=&end=tomorrow",
            // the supplied bound is read whatever else the query carries
            "limit=20&start=yesterday&spss=3",
        ] {
            let err = parse_fetch_window(Some(raw)).expect_err("a malformed bound is refused");
            assert!(
                matches!(err, SearchParamError::InvalidTimestamp(_)),
                "for {raw}: got {err:?}"
            );
        }
    }

    /// Criterion 14. **The wording and the boundary are pinned together,
    /// in one table, so a message that contradicts the predicate cannot
    /// pass.** Two of the three messages are byte-identical to their
    /// `/search` counterparts; `InvalidRange` deliberately is not, because
    /// the two surfaces enforce DIFFERENT predicates — `/search` rejects
    /// `end <= start`, this route rejects `end < start` only. Asserting
    /// the literal beside the boundary that produces it is what ties the
    /// difference in wording to the difference in behaviour: a predicate
    /// of `<=` fails row 2, and the wording `must be greater than` fails
    /// row 1.
    #[test]
    fn criterion_14_range_wording_and_boundaries() {
        // Row 1: inverted — rejected, with the exact rendered message.
        let err = parse_range_params(&pairs("start=100&end=99")).expect_err("inverted rejects");
        assert_eq!(
            err.to_string(),
            "invalid range: end (99) must not be earlier than start (100)"
        );

        // Row 2: zero-width — ACCEPTED here, rejected on /search.
        assert_eq!(
            parse_range_params(&pairs("start=100&end=100")).unwrap(),
            Some((100 * S, 100 * S))
        );
        assert!(
            parse_search_params("start=100&end=100").is_err(),
            "/search rejects end == start; this route accepts it, and that is the \
             difference the wording above records"
        );

        // Row 3: ordinary.
        assert_eq!(
            parse_range_params(&pairs("start=100&end=101")).unwrap(),
            Some((100 * S, 101 * S))
        );

        // The two shared wordings, asserted as equalities against the
        // /search surface rather than as literals, so a change to either
        // side fails here.
        assert_eq!(
            parse_range_params(&pairs("start=abc&end=101"))
                .expect_err("unparseable")
                .to_string(),
            SearchParamError::InvalidTimestamp("abc".to_string()).to_string()
        );
        assert_eq!(
            parse_range_params(&pairs("start=100"))
                .expect_err("half-supplied")
                .to_string(),
            SearchParamError::MissingRange("end").to_string()
        );
    }

    /// Criterion 15. **A zero bound is absent only when BOTH are**, and
    /// the three cases are the exhaustive domain for two bounds each
    /// either zero or not, minus the both-nonzero case criterion 12
    /// covers. The pair is the discriminator: a rule that read `0` as 1970
    /// fails the first case, and one that read any `0` as absent fails the
    /// other two.
    #[test]
    fn criterion_15_a_zero_bound_is_absent_only_when_both_are() {
        assert_eq!(parse_range_params(&pairs("start=0&end=0")).unwrap(), None);
        assert!(matches!(
            parse_range_params(&pairs("start=0&end=101")),
            Err(RangeParamError::MissingRange("start"))
        ));
        assert!(matches!(
            parse_range_params(&pairs("start=100&end=0")),
            Err(RangeParamError::MissingRange("end"))
        ));
    }

    /// An absent range resolves to `[now - lookback, now]`, and a
    /// supplied one is carried through unchanged.
    #[test]
    fn tag_values_params_substitute_the_lookback_only_when_no_range_is_supplied() {
        let now = 1_700_000_000 * S;
        let lookback = 24 * 3_600 * S;

        let p = parse_tag_values_params("", now, lookback).unwrap();
        assert_eq!(p.start_ns, now - lookback);
        assert_eq!(p.end_ns, now);
        assert_eq!(p.q, None);

        let p = parse_tag_values_params("start=100&end=200", now, lookback).unwrap();
        assert_eq!(p.start_ns, 100 * S);
        assert_eq!(p.end_ns, 200 * S);
    }

    /// `q` is carried verbatim and NEVER parsed here — including the
    /// half-typed shapes the editor sends on every keystroke. An empty
    /// `q` is the same as an absent one.
    #[test]
    fn a_half_typed_q_is_carried_through_rather_than_rejected() {
        let now = 1_700_000_000 * S;
        for raw_q in ["%7Bspan.http.status_code%3D", "garbage", "%7B"] {
            let p = parse_tag_values_params(&format!("q={raw_q}"), now, S).unwrap();
            assert!(p.q.is_some(), "{raw_q} must be carried, not rejected");
        }
        assert_eq!(parse_tag_values_params("q=", now, S).unwrap().q, None);
    }

    /// A range FAULT reaches the names routes too, so the same malformed
    /// input is answered the same way on all six §4.3 routes — while a
    /// WELL-FORMED range is still accepted and ignored there.
    #[test]
    fn a_names_route_rejects_a_range_fault_and_ignores_a_well_formed_range() {
        assert!(matches!(
            parse_tags_params("start=abc&end=200"),
            Err(TagsParamError::Range(RangeParamError::InvalidTimestamp(_)))
        ));
        assert!(matches!(
            parse_tags_params("start=100"),
            Err(TagsParamError::Range(RangeParamError::MissingRange("end")))
        ));
        assert!(matches!(
            parse_tags_params("start=200&end=100"),
            Err(TagsParamError::Range(RangeParamError::InvalidRange { .. }))
        ));
        // Well-formed: still ignored, still `TagScope::All`.
        assert_eq!(
            parse_tags_params("start=100&end=200").unwrap(),
            TagsParams {
                scope: TagScope::All
            }
        );
    }
}
