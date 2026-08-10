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
///
/// This governs the **entry** axis only — how many log lines a request may
/// return or sample. It explicitly does **not** apply to
/// `/detected_fields`' field-name `limit`, which has no ceiling at all
/// ([`parse_field_limit`], issue #253).
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
/// `since`'s default — the lookback used for `start` when `start` is
/// omitted (docs/api.md §2.1: "default: last hour"). The reference's
/// `defaultSince = 1 * time.Hour` (`pkg/loghttp/params.go:23 @ v3.7.4
/// b318f282`).
pub(crate) const DEFAULT_SINCE_NS: i64 = 3_600_000_000_000;
/// `step`'s target point count when derived rather than supplied
/// (architect plan: "derived `clamp((end-start)/250, >=1s)`").
const DERIVED_STEP_TARGET_POINTS: i64 = 250;
const ONE_SECOND_NS: u64 = 1_000_000_000;

/// Errors from parsing `/api/logs/v1` request parameters — mapped to
/// `400` by `error::ApiError` (the one exception,
/// `MalformedContentType`, still maps to `400`, just for a POST-specific
/// reason).
#[derive(Debug, Error)]
pub(crate) enum ParamError {
    #[error("missing required parameter 'query'")]
    MissingQuery,
    #[error(
        "invalid timestamp {0:?}: expected unix seconds (<= 10 characters), unix nanoseconds, a \
         fractional-second value, or RFC3339"
    )]
    InvalidTimestamp(String),
    /// Issue #406 Part C: `since` — the `start` default's lookback,
    /// rejected rather than ignored (the reference's
    /// `could not parse 'since' parameter`, `pkg/loghttp/params.go:93-97 @
    /// v3.7.4 b318f282`).
    #[error("could not parse 'since' parameter {0:?}: expected a duration literal")]
    InvalidSince(String),
    #[error("invalid 'limit' {0:?}: expected a non-negative integer")]
    InvalidLimit(String),
    #[error("'limit' {limit} exceeds the maximum of {max}")]
    LimitTooLarge { limit: u64, max: u32 },
    #[error("invalid 'direction' {0:?}: expected 'forward' or 'backward'")]
    InvalidDirection(String),
    #[error("invalid 'step' {raw:?}: {reason}")]
    InvalidStep { raw: String, reason: String },
    /// Issue #406: a `Content-Type` that cannot be PARSED. The reference
    /// refuses one before any handler runs, because `ParseForm` returns
    /// `mime.ParseMediaType`'s error and `NewPrepopulateMiddleware` turns
    /// any `ParseForm` error into a `400`
    /// (`pkg/util/server/middleware.go:16-20 @ v3.7.4 b318f282`). A
    /// well-formed but non-form type is NOT this error — the body is
    /// simply not read; see [`form_body_disposition`].
    #[error("malformed 'Content-Type' header {0:?}")]
    MalformedContentType(String),
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
    /// Issue #170: `/detected_fields`' `line_limit` (sampled entries) —
    /// default 100 (the reference's `defaultQueryLimit`); zero or
    /// non-numeric input is a 400 (the house no-clamp rule; the cap
    /// breach reuses [`ParamError::LimitTooLarge`]).
    #[error("invalid 'line_limit' {0:?}: expected a positive integer")]
    InvalidLineLimit(String),
    /// Issue #170: `/detected_fields`' field-count cap — `limit` with the
    /// reference's legacy alias `field_limit`, default 1000
    /// (`defaultLimit`); zero or non-numeric input is a 400.
    #[error("invalid 'limit' {0:?}: expected a positive integer")]
    InvalidFieldLimit(String),
    /// Issue #171: `/patterns`' `(end - start) / step` bucket grid (after the
    /// 10s floor) exceeded [`PATTERN_MAX_GRID_BUCKETS`] — rejected before any
    /// engine/SQL work (the same bucket-grid discipline as the metrics
    /// endpoints).
    #[error("bucket grid too large: {buckets} steps exceeds the maximum of {max}")]
    PatternGridTooLarge { buckets: u64, max: u64 },
    /// Issue #171: `/patterns` serves precomputed templates from
    /// `log_patterns` — the bodies are gone, so ANY pipeline stage (line
    /// filters included, like `/volume`) would be meaningless; all are
    /// rejected.
    #[error("'query' must be a bare stream selector on the patterns endpoint (no pipeline stages)")]
    PatternsPipelineUnsupported,
    /// Issue #227: `query_range`'s `(end - start) / step > 11000` — Loki's own
    /// resolution limit (`loghttp/query.go` `errStepTooSmall`), enforced at
    /// request parsing and surfaced with Loki's EXACT 400 message (replacing
    /// the engine's `MetricBuckets` 422 at the request boundary).
    #[error(
        "exceeded maximum resolution of 11,000 points per time series. Try increasing the value \
         of the step parameter"
    )]
    MaxResolutionExceeded,
}

/// Loki's `query_range` points-per-series ceiling (`loghttp/query.go:29`):
/// `(end - start) / step > 11000` is a hard 400.
pub(crate) const MAX_RANGE_QUERY_POINTS: u64 = 11_000;

/// Enforces Loki's `(end - start) / step > 11000` resolution limit (issue
/// #227). The span SATURATES, exactly like the reference's (issue #227
/// review round 8): `End.Sub(Start)` is Go's `time.Time.Sub`, which clamps
/// an out-of-range difference to the int64-nanosecond `Duration` bounds
/// (`maxDuration = 1<<63-1`) instead of widening — so a full-domain span
/// at a huge step is SERVED (`maxDuration / step ≤ 11000`), where exact
/// i128 arithmetic would wrongly reject it. `end < start` never trips
/// (the reference 400s that as `errEndBeforeStart` before the fence, and
/// its saturated `minDuration / step ≤ 0` could not trip either); the
/// division truncates (Go integer `Duration / Duration`).
pub(crate) fn ensure_range_resolution(
    start_ns: i64,
    end_ns: i64,
    step_ns: u64,
) -> Result<(), ParamError> {
    if step_ns == 0 {
        return Ok(()); // a zero step is already a 400 from `parse_step`.
    }
    // Loki-exact: `end - start` clamped to i64 (`time.Time.Sub` saturation).
    let span = end_ns.saturating_sub(start_ns);
    if span > 0 && span as u64 / step_ns > MAX_RANGE_QUERY_POINTS {
        return Err(ParamError::MaxResolutionExceeded);
    }
    Ok(())
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

/// A `start`/`end`/`time` value, in the reference's exact reading
/// (`parseTimestamp`, `pkg/loghttp/params.go:161-186` @ grafana/loki
/// v3.7.4 `b318f2829f0ae2094ab3a1e90780450e9e4b03be`), issue #406 Part D.
///
/// Order is load-bearing and is the reference's:
///
/// 1. **A `.` means seconds with a fraction**, whatever the length: parse
///    as `f64`, split off the fractional part, round it to **three
///    decimal places** (`math.Round(frac*1000.0)/1000.0` — the reference
///    keeps only milliseconds), then `secs * 1e9 + frac * 1e9`. If the
///    float parse fails, fall THROUGH to step 2 rather than erroring —
///    `2026-08-10T06:18:26.5Z` contains a dot and is a valid RFC3339
///    timestamp.
/// 2. Otherwise parse as `i64`; if that fails, try RFC3339 (with
///    nanosecond fractions); if that fails, [`ParamError::InvalidTimestamp`].
/// 3. **`raw.len() <= 10` ⇒ the integer is SECONDS**, otherwise
///    nanoseconds. The test is the length of the ORIGINAL STRING, not the
///    magnitude of the number.
///
/// **The string-length rule is not a paraphrase of a magnitude rule**, and
/// the two disagree. Read off the reference's own query log (which prints
/// the parsed `start=`/`end=`), 2026-08-10 against `grafana/loki:3.7.4`:
/// `9999999999` → `2286-11-20T17:46:39Z` (10 characters ⇒ seconds);
/// `99999999999` → `1970-01-01T00:01:39.999999999Z` (11 ⇒ nanoseconds);
/// **`01786342706` → `1970-01-01T00:00:01.786342706Z`** — the same number
/// as `1786342706`, but eleven characters because of the leading zero, so
/// it is read as nanoseconds and lands in 1970; `-1` →
/// `1969-12-31T23:59:59Z` (the sign counts toward the length, so a
/// negative is seconds); `1786342706.123456` → `…T06:18:26.123Z`;
/// `1786342706.1239` → `…T06:18:26.124Z` (round-half-up at the third
/// decimal place).
///
/// **Do NOT unify this with `traces_api`'s or `prom_api`'s timestamp
/// parser.** The traces rule is on MAGNITUDE (`>= 10^12` ⇒ nanoseconds,
/// docs/api.md §4) and Prometheus's is seconds-with-a-fraction; each
/// matches its own reference. Tidying the three into one helper would
/// silently break one surface to make the code look neater.
///
/// Overflow is refused, not wrapped: a ten-character value up to
/// `9999999999` is ~1.0e19 ns and does NOT fit `i64`, so `checked_mul`
/// decides rather than a cast. Everything the reference can express and
/// `i64` can hold round-trips; anything else is an
/// [`ParamError::InvalidTimestamp`] rather than a silently wrapped
/// instant.
pub(crate) fn parse_ts(raw: &str) -> Result<i64, ParamError> {
    // 1. A fractional value is ALWAYS seconds, at any length.
    if raw.contains('.')
        && let Ok(t) = raw.parse::<f64>()
    {
        // `math.Modf`: integer and fractional parts, both carrying the
        // sign. The fraction is rounded to milliseconds BEFORE it becomes
        // nanoseconds — `…706.123456` is `…706.123` at the reference, a
        // real truncation and not a float artifact.
        let secs = t.trunc();
        let frac = (t.fract() * 1000.0).round() / 1000.0;
        // `f64 as i64` saturates in Rust, so a non-finite or astronomical
        // `secs` lands on `i64::MIN`/`i64::MAX` and the `checked_mul`
        // below refuses it — no wrap, no UB.
        let secs_ns = (secs as i64)
            .checked_mul(ONE_SECOND_NS as i64)
            .ok_or_else(|| ParamError::InvalidTimestamp(raw.to_string()))?;
        let frac_ns = (frac * ONE_SECOND_NS as f64) as i64;
        return secs_ns
            .checked_add(frac_ns)
            .ok_or_else(|| ParamError::InvalidTimestamp(raw.to_string()));
    }
    // 2/3. An integer literal: seconds at <= 10 characters, else nanoseconds.
    if let Ok(n) = raw.parse::<i64>() {
        return if raw.len() <= 10 {
            n.checked_mul(ONE_SECOND_NS as i64)
                .ok_or_else(|| ParamError::InvalidTimestamp(raw.to_string()))
        } else {
            Ok(n)
        };
    }
    let dt = chrono::DateTime::parse_from_rfc3339(raw)
        .map_err(|_| ParamError::InvalidTimestamp(raw.to_string()))?;
    dt.timestamp_nanos_opt()
        .ok_or_else(|| ParamError::InvalidTimestamp(raw.to_string()))
}

/// `since` (`pkg/loghttp/params.go:83-119` @ v3.7.4 `b318f282`), issue
/// #406 Part C: the `start` default's lookback, defaulting to 1 h. Read on
/// every logs route carrying a `start`/`end` pair — that is, every one but
/// `/query` (which has only `time`) — and used ONLY when `start` is
/// absent, so `?start=…&since=5m` answers from `start`.
///
/// Container-measured 2026-08-10 against `grafana/loki:3.7.4` (plan
/// tables on issue #406): `since=bogus` is a `400` there and was a
/// silently-ignored `200` here; `since=5m` against 20-minute-old data
/// returns empty there and returned everything here.
///
/// **The accepted duration set is [`parse_duration_ns`]'s, not
/// Prometheus's `model.ParseDuration`** (the plan's "no new duration
/// grammar"): ours additionally accepts a bare integer as seconds and the
/// `us`/`ns` units, and does not accept `w`/`y`. Every value either
/// grammar accepts is read identically; ours is the looser side, so no
/// request the reference serves is refused here.
pub(crate) fn parse_since(raw: Option<&str>) -> Result<i64, ParamError> {
    let Some(raw) = raw else {
        return Ok(DEFAULT_SINCE_NS);
    };
    let ns = parse_duration_ns(raw).map_err(|_| ParamError::InvalidSince(raw.to_string()))?;
    // `parse_duration_ns` is unsigned, so a negative literal never reaches
    // here (it fails the grammar); this refuses the other end instead — a
    // duration too large to be an `i64` nanosecond offset.
    i64::try_from(ns).map_err(|_| ParamError::InvalidSince(raw.to_string()))
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

/// Default `line_limit` for `/detected_fields` (issue #170) — the
/// reference's `defaultQueryLimit`.
pub(crate) const DEFAULT_LINE_LIMIT: u32 = 100;
/// Default field-count `limit` for `/detected_fields` (issue #170) — the
/// reference's `defaultLimit`.
pub(crate) const DEFAULT_FIELD_LIMIT: u32 = 1000;

/// `/detected_fields`' `line_limit` — the ENTRY axis (issues #170, #253,
/// docs/api.md §2.6.3). Absent **or empty** → [`DEFAULT_LINE_LIMIT`]: the
/// reference's `parseInt(value, def)` returns `def` for `""`
/// (`pkg/loghttp/params.go:154-159 @ v3.7.4 b318f282`) and Go's
/// `r.Form.Get` cannot tell an absent key from an empty one, so empty
/// **is** absent there. Outside `i64::from_str` → 400 — that is exactly
/// `strconv.Atoi`'s accepted set, established by measurement in
/// [`parse_field_limit`]. `n <= 0` → 400. `n > MAX_LIMIT` → 400.
///
/// **This function never converts an out-of-range value; it refuses it.**
/// `n` is compared against [`MAX_LIMIT`] as an `i64` and the `as u32`
/// below is reached only on `0 < n <= 5000`. So there is no clamp, no
/// saturation and no cast to wrap on this axis, at any magnitude — the
/// only outcomes are a value in `1..=5000` or a 400.
///
/// The 5000 is **parity, not a house cap**: the reference answers
/// `line_limit=5001` with a 400 reading `max entries limit per query
/// exceeded, limit > max_entries_limit_per_query (5001 > 5000)`, from
/// `validateMaxEntriesLimits` (`pkg/querier/queryrange/limits.go:767-780`,
/// called at `pkg/querier/queryrange/detected_fields.go:189`) against
/// `validation.max-entries-limit`, default 5000
/// (`pkg/validation/limits.go:355`). Container-measured against
/// `grafana/loki:3.7.4`, 2026-08-07. At that boundary only the 400's
/// message text differs, which the owner ruling on #253 puts below the
/// parity bar.
///
/// The two statuses agree over the whole of `[1, 2^32)`, which follows
/// from the two implementations rather than from the probe: we compare `n`
/// to 5000 directly, while the reference feeds `uint32(l)` — the identity
/// on that range — to a check against the same 5000. Above `2^32` its cast
/// (`pkg/loghttp/params.go:38-46`) stops being the identity and it accepts
/// what it should reject: measured discriminatingly on a 30-entry fixture,
/// `line_limit=4294967297` returns the same per-field cardinality 1 as
/// `line_limit=1`, and `line_limit=4294967326` the same 30 as
/// `line_limit=30` — it really does serve the wrapped value. We have no
/// such cast, so we go on rejecting; that asymmetry is recorded in the
/// `detected-fields-limit-saturates-not-wraps` ledger row, which names
/// both parameters and the different way each of ours avoids the wrap.
/// Pinned by `parse_line_limit_matches_the_reference_atoi_surface`.
pub(crate) fn parse_line_limit(raw: Option<&str>) -> Result<u32, ParamError> {
    let Some(text) = raw.filter(|s| !s.is_empty()) else {
        return Ok(DEFAULT_LINE_LIMIT);
    };
    let n: i64 = text
        .parse()
        .map_err(|_| ParamError::InvalidLineLimit(text.to_string()))?;
    if n <= 0 {
        return Err(ParamError::InvalidLineLimit(text.to_string()));
    }
    if n > i64::from(MAX_LIMIT) {
        return Err(ParamError::LimitTooLarge {
            // `n > MAX_LIMIT > 0` here, so the widening is exact.
            limit: n as u64,
            max: MAX_LIMIT,
        });
    }
    // `0 < n <= MAX_LIMIT` (a `u32`) was just checked, so this narrowing
    // conversion is always exact.
    Ok(n as u32)
}

/// `/detected_fields`' field-name `limit`, legacy alias `field_limit` —
/// the FIELD-NAME axis (issues #170, #253). `limit` first, then the alias,
/// an **empty** value on either counting as absent exactly as the
/// reference's `detectedFieldsLimit` does (`pkg/loghttp/params.go:49-64 @
/// v3.7.4 b318f282`); absent → [`DEFAULT_FIELD_LIMIT`]; outside
/// `i64::from_str` → 400; `n <= 0` → 400.
///
/// The accepted set is `i64::from_str` — the same grammar as `Atoi` on a
/// 64-bit platform (optional `+`/`-`, then ASCII digits only, value within
/// `i64`), established by probing the reference across the boundary forms
/// rather than by reading `Atoi`: leading `+`, leading zeros,
/// leading/trailing whitespace, underscores, exponent and hex forms,
/// non-ASCII digits, sign-only, overlong digit strings, `i64::MAX` and one
/// past it all agree. The table is
/// `parse_field_limit_matches_the_reference_atoi_surface`.
///
/// **There is NO ceiling on this axis** and none is reinstated. The
/// reference imposes none, and the container measurement recorded on issue
/// #253 (2026-08-07, `grafana/loki:3.7.4`) shows it does not degrade at
/// large values either: over a 50 000-field sample, `limit` from 50 000 to
/// 4 294 967 295 returned an identical body in an identical time. The work
/// is bounded by the sampled entries, and on our side additionally by
/// `pulsus_read::logql::detected::MAX_DETECTED_FIELD_BYTES`, which clamps
/// and serves.
///
/// Above `u32::MAX` we **saturate** where the reference's unchecked
/// `uint32(l)` wraps (`limit=4294967296` measures as zero fields there).
/// Deliberate, registered as `detected-fields-limit-saturates-not-wraps`
/// in docs/benchmarks/logs-differential-ledger.md and pinned by
/// `parse_field_limit_saturates_where_the_reference_wraps`.
pub(crate) fn parse_field_limit(
    limit: Option<&str>,
    field_limit: Option<&str>,
) -> Result<u32, ParamError> {
    let Some(text) = limit
        .filter(|s| !s.is_empty())
        .or_else(|| field_limit.filter(|s| !s.is_empty()))
    else {
        return Ok(DEFAULT_FIELD_LIMIT);
    };
    let n: i64 = text
        .parse()
        .map_err(|_| ParamError::InvalidFieldLimit(text.to_string()))?;
    if n <= 0 {
        return Err(ParamError::InvalidFieldLimit(text.to_string()));
    }
    Ok(u32::try_from(n).unwrap_or(u32::MAX))
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

/// The `log_patterns` ingest bucket resolution (M7-C3, issue #171): the
/// `/patterns` `step` is floored to this (and is never smaller than it),
/// matching the write-side `patterns::PATTERN_BUCKET_NS` — a finer step would
/// invent sub-bucket granularity the stored data does not carry.
pub(crate) const PATTERN_STEP_FLOOR_NS: u64 = 10_000_000_000;
/// The `/patterns` `(end - start) / step` bucket-grid cap (issue #171) — the
/// same 11,000 bound the metrics endpoints use.
pub(crate) const PATTERN_MAX_GRID_BUCKETS: u64 = 11_000;

/// `/patterns`' effective `step`: [`parse_step`], then floored to the 10s
/// ingest bucket (never below it), then the `(end - start) / step` grid is
/// rejected past [`PATTERN_MAX_GRID_BUCKETS`] (400) — all in pure param
/// parsing, before any engine/SQL work. `start_ns <= end_ns` is the caller's
/// precondition (checked separately as `EndBeforeStart`).
pub(crate) fn parse_pattern_step(
    raw: Option<&str>,
    start_ns: i64,
    end_ns: i64,
) -> Result<u64, ParamError> {
    let requested = parse_step(raw, start_ns, end_ns)?;
    // Floor to the 10s bucket, but never below it (a sub-10s step would floor
    // to 0).
    let step =
        (requested / PATTERN_STEP_FLOOR_NS * PATTERN_STEP_FLOOR_NS).max(PATTERN_STEP_FLOOR_NS);
    let span_ns = end_ns.saturating_sub(start_ns).max(0) as u64;
    let buckets = span_ns / step;
    if buckets > PATTERN_MAX_GRID_BUCKETS {
        return Err(ParamError::PatternGridTooLarge {
            buckets,
            max: PATTERN_MAX_GRID_BUCKETS,
        });
    }
    Ok(step)
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

/// The first value for `key`, treating a **present-but-empty** value as
/// absent — the reference's `r.Form.Get`, which returns `""` for both an
/// absent key and an empty one and so cannot distinguish them (issue
/// #391). Every scalar parameter on Loki's log surface is read through it
/// and every parse helper behind it defaults on `""`: `parseInt`
/// (`pkg/loghttp/params.go:152-159 @ v3.7.4 b318f282`), `parseTimestamp`
/// (`:161-186`), `parseDirection` (`:188-200`), plus the inline
/// `if value == ""` in `step` (`:122-128`), `interval` (`:130-136`),
/// `volumeAggregateBy` (`query.go:741-751`) and the split shape in
/// `targetLabels` (`query.go:714-721`).
///
/// **First, then filter — not "the first non-empty".** Go returns the
/// first value and *then* treats it as empty. Container-measured on a
/// 150-entry stream, 2026-08-09: `?limit=&limit=5` serves 100 entries
/// (the default), `?limit=5&limit=` serves 5. Pinned by
/// `get_takes_the_first_value_then_treats_empty_as_absent`.
///
/// **Only the empty string is empty.** Measured: `?limit=` and a bare
/// `?limit` with no `=` are 200; `?limit=%20`, `?limit=+`, `?limit=%09`
/// and `?limit=%00` are 400 (`strconv.Atoi: parsing " ": invalid
/// syntax`, `"\t"`, `"\x00"`). Our [`percent_decode`] already agrees with
/// `url.ParseQuery` on `+` and `%XX`, so the boundary needs no work here.
///
/// **[`get_all`] deliberately does NOT collapse.** The reference reads
/// repeated parameters through `r.Form[...]`, which keeps `""` as a
/// value: `?match[]=` is a 400 parse error there (`series.go:23-25`,
/// measured) while an absent `match[]` is a 200, and `?shards=` is a 500
/// (`params.go:79-81`, measured). Pinned by
/// `an_empty_repeated_match_is_a_value_not_an_absence`.
pub(crate) fn get<'a>(pairs: &'a [(String, String)], key: &str) -> Option<&'a str> {
    pairs
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
        .filter(|v| !v.is_empty())
}

/// Every value for `key`, in appearance order (`match[]` repeats).
///
/// **This seam does not collapse empty into absent, and must not start**
/// — see [`get`]'s note: the reference reads repeated parameters through
/// `r.Form[...]` rather than `r.Form.Get`, so an empty `match[]=` is a
/// value there and a 400, where an absent one is a 200 (issue #391).
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

// -- POST `Content-Type` (issue #406) ----------------------------------

/// What a POST's `Content-Type` says about its BODY.
///
/// Two outcomes, never three: the header decides whether the body is read,
/// and it never decides whether the request is served. A malformed header
/// is a separate axis and is a [`ParamError::MalformedContentType`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FormBody {
    /// `application/x-www-form-urlencoded` — parse the body as form pairs.
    Parse,
    /// Any other well-formed media type, and an absent or empty header —
    /// the body is not read at all. The URL query is still read.
    Ignore,
}

/// Whether a POST body is read, in the reference's exact reading
/// (`parsePostForm`, Go's `net/http/request.go:1263-1307` @ go1.25.5,
/// reached from `ParseForm` via `NewPrepopulateMiddleware`,
/// `pkg/util/server/middleware.go:12-23` @ grafana/loki v3.7.4
/// `b318f2829f0ae2094ab3a1e90780450e9e4b03be`), issue #406.
///
/// **The reference does not gate SERVICE on this header — it gates
/// BODY-READING.** An absent, empty or non-form `Content-Type` makes the
/// body invisible and the request is answered from the URL query alone;
/// only a header that cannot be PARSED is refused, and then by
/// `ParseForm` returning `mime.ParseMediaType`'s error, which the
/// middleware turns into a `400` before any handler runs.
///
/// Order is the reference's:
///
/// 1. An empty header value becomes `application/octet-stream`
///    (`request.go:1271-1273`, RFC 7231 §3.1.1.5). An **absent** header
///    reads the same, because Go's `Header.Get` returns `""` for both —
///    and so does an all-whitespace one, because both HTTP stacks strip
///    a header value's surrounding whitespace before we see it
///    (container-measured, below).
/// 2. [`parse_media_type`] — Go's `mime.ParseMediaType`. On error, the
///    whole request is a `400`; the body is never reached.
/// 3. The parsed media type, lowercased and stripped of its parameters,
///    is compared to `application/x-www-form-urlencoded`
///    (`request.go:1276`). Equal ⇒ [`FormBody::Parse`]; anything else,
///    including `multipart/form-data`, ⇒ [`FormBody::Ignore`].
///
/// Container-measured against `grafana/loki:3.7.4`, 2026-08-10, with all
/// parameters in the URL and `limit=5` in the body against a 150-entry
/// seed — so "5" reads the body and "7" (the URL's `limit`) ignores it:
/// absent ⇒ 7; `Content-Type:` ⇒ 7; `Content-Type:` + three spaces ⇒ 7;
/// `application/json` ⇒ 7; `text/plain` ⇒ 7; `application/octet-stream`
/// ⇒ 7; `multipart/form-data` ⇒ 7; `garbage` (a bare token, no slash,
/// which IS a legal media type) ⇒ 7; `application/x-www-form-urlencodedX`
/// ⇒ 7; `application/x-www-form-urlencoded` ⇒ 5;
/// `…urlencoded; charset=UTF-8` ⇒ 5; `APPLICATION/X-WWW-Form-URLENCODED`
/// ⇒ 5; `  application/x-www-form-urlencoded  ` ⇒ 5.
pub(crate) fn form_body_disposition(content_type: Option<&str>) -> Result<FormBody, ParamError> {
    let raw = content_type.unwrap_or("");
    // `request.go:1271-1273` — an empty type MAY be treated as
    // `application/octet-stream`, and Go takes that option. Spelled as the
    // substitution rather than as an early return so the fall-through is
    // the reference's own.
    let ct = if raw.is_empty() {
        "application/octet-stream"
    } else {
        raw
    };
    let media_type =
        parse_media_type(ct).map_err(|_| ParamError::MalformedContentType(raw.to_string()))?;
    if media_type == "application/x-www-form-urlencoded" {
        Ok(FormBody::Parse)
    } else {
        Ok(FormBody::Ignore)
    }
}

/// Go's `mime.ParseMediaType` (`mime/mediatype.go:134-227` @ go1.25.5),
/// reduced to what `parsePostForm` consumes: the lowercased media type,
/// and whether the value parsed at all. The parameter MAP is discarded
/// upstream too (`ct, _, err = mime.ParseMediaType(ct)`), but the
/// parameters are still WALKED, because a parameter that does not parse
/// makes the whole header — and therefore the whole request — an error.
///
/// Container-measured `400`s that reach us only through this walk:
/// `application/json; charset` and `application/x-www-form-urlencoded;
/// bogus` (`ErrInvalidMediaParameter`), and
/// `application/x-www-form-urlencoded; a=1; a=2` (duplicate parameter
/// name). Measured `200`s that would be `400`s without it:
/// `application/x-www-form-urlencoded; a=1; a=1` (a duplicate whose value
/// AGREES is allowed) and a trailing `;` (ignored, not an error).
///
/// RFC 2231 continuations (`a*0=…`) are walked and their values
/// duplicate-checked exactly as upstream, but never stitched: the
/// stitching pass (`mediatype.go:186-226`) writes only into the params map
/// and cannot fail — every one of its decode steps swallows its error into
/// an `ok` bool — so it cannot change either value this function returns.
fn parse_media_type(v: &str) -> Result<String, MediaTypeError> {
    let base = v.split(';').next().unwrap_or("");
    let media_type = base.trim().to_ascii_lowercase();
    check_media_type_disposition(&media_type)?;

    // Parameter walk. Upstream files each name into one of two maps —
    // `params`, or a per-base-name continuation map when the name contains
    // a `*` — but the map it picks is a function of the NAME, and the
    // lookup key is the whole name either way. So two names collide
    // exactly when they are equal, and the continuation bucketing cannot
    // change that; `seen` therefore keys on the full name.
    let mut seen: Vec<(String, String)> = Vec::new();
    let mut rest = &v[base.len()..];
    while !rest.is_empty() {
        rest = trim_start_unicode_space(rest);
        if rest.is_empty() {
            break;
        }
        match consume_media_param(rest) {
            Some((key, value, next)) => {
                // `mediatype.go:178-181`: a repeated parameter is an error
                // only when the values DISAGREE.
                if let Some((_, prev)) = seen.iter().find(|(k, _)| *k == key)
                    && *prev != value
                {
                    return Err(MediaTypeError::DuplicateParameterName);
                }
                seen.push((key, value));
                rest = next;
            }
            None => {
                // `mediatype.go:157-165`: a trailing `;` (possibly with
                // trailing space) is deliberately not an error.
                if rest.trim() == ";" {
                    break;
                }
                return Err(MediaTypeError::InvalidMediaParameter);
            }
        }
    }
    Ok(media_type)
}

/// Why a `Content-Type` could not be parsed. The variants exist so the
/// port stays legible against `mime/mediatype.go`; the wire carries one
/// `400` and PulsusDB's own prose either way — the reference's messages
/// (`mime: expected token after slash`, and three siblings) are its
/// implementation language, and message prose is below the parity bar
/// (issue #253 ruling).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MediaTypeError {
    /// `mime: no media type` — e.g. `;` or `/json`.
    NoMediaType,
    /// `mime: expected slash after first token` — e.g. `app lication/json`.
    ExpectedSlash,
    /// `mime: expected token after slash` — e.g. `application/`.
    ExpectedTokenAfterSlash,
    /// `mime: unexpected content after media subtype` — e.g.
    /// `application/json/x`.
    TrailingContent,
    /// `mime: invalid media parameter` — e.g. `application/json; charset`.
    InvalidMediaParameter,
    /// `mime: duplicate parameter name` — a repeat whose value disagrees.
    DuplicateParameterName,
}

/// Go's `checkMediaTypeDisposition` (`mime/mediatype.go:98-117`). A bare
/// token with no slash at all is **legal** (`rest == ""` returns `nil`),
/// which is why `Content-Type: garbage` is a measured `200` upstream and
/// not a `400`.
fn check_media_type_disposition(s: &str) -> Result<(), MediaTypeError> {
    let (typ, rest) = consume_token(s);
    if typ.is_empty() {
        return Err(MediaTypeError::NoMediaType);
    }
    if rest.is_empty() {
        return Ok(());
    }
    let Some(after_slash) = rest.strip_prefix('/') else {
        return Err(MediaTypeError::ExpectedSlash);
    };
    let (subtype, rest) = consume_token(after_slash);
    if subtype.is_empty() {
        return Err(MediaTypeError::ExpectedTokenAfterSlash);
    }
    if !rest.is_empty() {
        return Err(MediaTypeError::TrailingContent);
    }
    Ok(())
}

/// Go's `isTSpecial` (`mime/grammar.go`): the RFC 1521/2045 `tspecials`.
fn is_tspecial(c: u8) -> bool {
    matches!(
        c,
        b'(' | b')'
            | b'<'
            | b'>'
            | b'@'
            | b','
            | b';'
            | b':'
            | b'\\'
            | b'"'
            | b'/'
            | b'['
            | b']'
            | b'?'
            | b'='
    )
}

/// Go's `isTokenChar` (`mime/grammar.go`): any US-ASCII character that is
/// not SPACE, a CTL, or a `tspecial`. Bytes >= 0x80 are not token
/// characters, so a non-ASCII byte terminates a token exactly as upstream.
fn is_token_char(c: u8) -> bool {
    c > 0x20 && c < 0x7f && !is_tspecial(c)
}

/// Go's `consumeToken` (`mime/mediatype.go`): the longest token prefix,
/// and the remainder. Byte-wise, as upstream is.
fn consume_token(v: &str) -> (&str, &str) {
    let end = v
        .as_bytes()
        .iter()
        .position(|c| !is_token_char(*c))
        .unwrap_or(v.len());
    v.split_at(end)
}

/// Go's `consumeValue` (`mime/mediatype.go`): a token, or a quoted-string
/// with Go's MSIE-tolerant backslash rule (a backslash escapes only a
/// `tspecial`; before anything else it is a literal backslash). Returns
/// `None` when no value could be consumed, which is upstream's
/// `value == "" && rest2 == rest` signal.
fn consume_value(v: &str) -> Option<(String, &str)> {
    if v.is_empty() {
        return None;
    }
    let bytes = v.as_bytes();
    if bytes[0] != b'"' {
        let (token, rest) = consume_token(v);
        if token.is_empty() {
            return None;
        }
        return Some((token.to_string(), rest));
    }
    let mut out: Vec<u8> = Vec::new();
    let mut i = 1;
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'"' {
            return Some((String::from_utf8_lossy(&out).into_owned(), &v[i + 1..]));
        }
        if c == b'\\' && i + 1 < bytes.len() && is_tspecial(bytes[i + 1]) {
            out.push(bytes[i + 1]);
            i += 2;
            continue;
        }
        if c == b'\r' || c == b'\n' {
            return None;
        }
        out.push(c);
        i += 1;
    }
    // No closing quote.
    None
}

/// Go's `consumeMediaParam` (`mime/mediatype.go`): `; name=value`, with
/// unicode whitespace tolerated around each piece and the name lowercased.
/// `None` is upstream's "returned `v` unchanged", which the caller turns
/// into `ErrInvalidMediaParameter` unless what remains is a bare trailing
/// semicolon.
fn consume_media_param(v: &str) -> Option<(String, String, &str)> {
    let rest = trim_start_unicode_space(v);
    let rest = rest.strip_prefix(';')?;
    let rest = trim_start_unicode_space(rest);
    let (param, rest) = consume_token(rest);
    if param.is_empty() {
        return None;
    }
    let param = param.to_ascii_lowercase();
    let rest = trim_start_unicode_space(rest);
    let rest = rest.strip_prefix('=')?;
    let rest = trim_start_unicode_space(rest);
    let (value, rest) = consume_value(rest)?;
    Some((param, value, rest))
}

/// `strings.TrimLeftFunc(s, unicode.IsSpace)` — Go trims UNICODE space
/// here, not just ASCII, so the port does too.
fn trim_start_unicode_space(s: &str) -> &str {
    s.trim_start_matches(|c: char| c.is_whitespace())
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- Issue #406 Part D: the reference's length + fraction rules ------

    /// **Part D's discriminating test.** Every row is the reference's own
    /// parse, read off its query log (which prints the parsed
    /// `start=`/`end=`) against `grafana/loki:3.7.4`, 2026-08-10 — an
    /// observation of the parse itself rather than an inference from
    /// entry counts.
    ///
    /// **`"01786342706"` is the whole point of the table.** The plausible
    /// magnitude spelling — `if n < 10_000_000_000 { seconds }` — agrees
    /// with the reference on every other row and disagrees only here,
    /// where the number is small enough to look like seconds but the
    /// STRING is eleven characters long because of the leading zero. The
    /// reference follows the string (`len(value) <= 10`,
    /// `pkg/loghttp/params.go:183-186 @ v3.7.4 b318f282`) and reads it as
    /// nanoseconds, landing in 1970.
    #[test]
    fn parse_ts_follows_the_references_length_and_fraction_rules() {
        for (raw, want) in [
            // ref log: start=2026-05-08T06:18:26Z — 10 chars ⇒ seconds.
            ("1786342706", 1_786_342_706_000_000_000_i64),
            // ref log: start=1970-01-01T00:00:01.786342706Z — the SAME
            // number, eleven characters, so nanoseconds. Magnitude and
            // string length disagree here and only here.
            ("01786342706", 1_786_342_706),
            // ref log: start=1970-01-01T00:01:39.999999999Z — 11 chars.
            ("99999999999", 99_999_999_999),
            // ref log: start=1969-12-31T23:59:59Z — the sign counts
            // toward the length, so `-1` is two characters and SECONDS.
            ("-1", -1_000_000_000),
            // 0 s and 0 ns are the same instant either way.
            ("0", 0),
            // The largest 10-character seconds value an i64 nanosecond
            // domain can hold (2262-04-11T23:47:16Z); one more second is
            // refused below.
            ("9223372036", 9_223_372_036_000_000_000),
            // 19-digit nanoseconds, the control.
            ("1786342706000000000", 1_786_342_706_000_000_000),
            // ref log: start=…T06:18:26.123Z — the fraction is rounded to
            // THREE decimal places (milliseconds) before it becomes
            // nanoseconds. Dropping that round is a silent 456 ns skew.
            ("1786342706.123456", 1_786_342_706_123_000_000),
            // ref log: start=…T06:18:26.124Z — round-half-up at the 3rd dp.
            ("1786342706.1239", 1_786_342_706_124_000_000),
            // A fraction is seconds at ANY length, so the ten-character
            // rule never sees these.
            ("1786342706.5", 1_786_342_706_500_000_000),
            ("1786342706.0", 1_786_342_706_000_000_000),
            ("-1.5", -1_500_000_000),
            // RFC3339 is unchanged.
            ("2026-07-01T00:00:00Z", 1_782_864_000_000_000_000),
            // A dot that is not a float still reaches RFC3339: the `.`
            // branch must fall THROUGH on a failed float parse, never
            // error inside it.
            ("2026-07-01T00:00:00.5Z", 1_782_864_000_500_000_000),
        ] {
            assert_eq!(parse_ts(raw).unwrap(), want, "parse_ts({raw:?})");
        }

        for raw in [
            "not-a-timestamp",
            "1.2.3",
            "",
            " 1786342706",
            "1786342706abc",
        ] {
            assert!(
                matches!(parse_ts(raw), Err(ParamError::InvalidTimestamp(_))),
                "parse_ts({raw:?}) must be InvalidTimestamp"
            );
        }
    }

    /// **The one place we refuse where the reference serves**, and it is
    /// the same refusal our RFC3339 path has always made. Go's
    /// `time.Time` stores seconds and nanoseconds separately, so
    /// `9999999999` seconds (`2286-11-20T17:46:39Z`) is representable
    /// there; PulsusDB's whole timestamp domain is `i64` nanoseconds
    /// (`~1677-09-21` to `2262-04-11`), so `9999999999 * 1e9` ≈ 1.0e19
    /// does not fit and `checked_mul` refuses it rather than wrapping to
    /// some arbitrary instant. `chrono`'s `timestamp_nanos_opt` already
    /// refuses `2286-11-20T17:46:39Z` spelled as RFC3339, so this keeps
    /// the two spellings of one instant answering alike.
    ///
    /// Recorded as a deviation from issue #406's AC D1, whose
    /// `"9999999999" ⇒ 9_999_999_999 * 1e9` row is not representable.
    #[test]
    fn parse_ts_refuses_a_seconds_value_outside_the_i64_nanosecond_domain() {
        assert!(matches!(
            parse_ts("9999999999"),
            Err(ParamError::InvalidTimestamp(_))
        ));
        assert!(matches!(
            parse_ts("9223372037"),
            Err(ParamError::InvalidTimestamp(_))
        ));
        // The RFC3339 spelling of the same instant is refused too.
        assert!(matches!(
            parse_ts("2286-11-20T17:46:39Z"),
            Err(ParamError::InvalidTimestamp(_))
        ));
        // …and the fractional spelling.
        assert!(matches!(
            parse_ts("9999999999.5"),
            Err(ParamError::InvalidTimestamp(_))
        ));
    }

    /// Issue #406 Part D, the sweep of risk 12: after Part D a short
    /// integer `start`/`end`/`time` on a **logs** route means something
    /// different from what it meant when the suite was written. Every
    /// remaining one must be a value the change cannot move — i.e. `0`,
    /// where 0 s and 0 ns are the same instant.
    ///
    /// **Claimed domain = checked domain.** The claim is about the
    /// workspace's own request corpora, so the scan is exactly
    /// `crates/*/tests/**`, `e2e/**` and `xtask/**` — the directories that
    /// build requests. `src/**` is excluded because a handler's own unit
    /// tests are edited by the same change that edits the handler.
    ///
    /// Two filters keep the claim about LOGS requests rather than about
    /// every timestamp-shaped literal, and both are stated so a reader can
    /// see what the guard cannot see:
    ///
    /// * **Comment lines carry no request** — a line whose first non-space
    ///   characters are `//` or `#` is skipped. Two such lines exist today
    ///   (`logql_nested_ip_matrix.rs` and `logqltest/corpus/b20_nested_ip.test`
    ///   both quote the reference's `start=0&end=1` behaviour in prose).
    /// * **Non-logs surfaces keep their own timestamp rules.** `prom_api`
    ///   reads seconds and `traces_api` switches on magnitude; neither is
    ///   touched by Part D. A hit is counted only when the nearest
    ///   preceding SURFACE MARKER — a quoted route path, or an enclosing
    ///   `fn`/`const`/`static` name — is a logs one. Files with no marker
    ///   at all fall back to their path.
    #[test]
    fn no_pulsus_suite_sends_a_short_integer_timestamp_it_no_longer_means() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .expect("workspace root")
            .to_path_buf();
        let mut files: Vec<std::path::PathBuf> = Vec::new();
        for entry in std::fs::read_dir(root.join("crates")).expect("crates/") {
            let path = entry.expect("dir entry").path().join("tests");
            collect_scan_files(&path, &mut files);
        }
        collect_scan_files(&root.join("e2e"), &mut files);
        collect_scan_files(&root.join("xtask"), &mut files);
        assert!(
            files.len() > 50,
            "the sweep only found {} files — its domain has moved",
            files.len()
        );

        let mut offenders: Vec<String> = Vec::new();
        let mut logs_lines = 0usize;
        for path in &files {
            let rel = path
                .strip_prefix(&root)
                .unwrap_or(path)
                .to_string_lossy()
                .replace('\\', "/");
            let src = std::fs::read_to_string(path).expect("source");
            let mut surface = surface_of(&rel);
            for (i, line) in src.lines().enumerate() {
                if let Some(marked) = surface_marker(line) {
                    surface = marked;
                }
                let trimmed = line.trim_start();
                if trimmed.starts_with("//") || trimmed.starts_with('#') {
                    continue;
                }
                if surface != Surface::Logs {
                    continue;
                }
                logs_lines += 1;
                for hit in short_integer_timestamps(line) {
                    offenders.push(format!("{rel}:{}: {hit}", i + 1));
                }
            }
        }
        assert!(
            logs_lines > 500,
            "only {logs_lines} lines classified as a logs surface — the marker rule has broken \
             and this guard would pass vacuously"
        );
        assert!(
            offenders.is_empty(),
            "these logs-surface literals send a <= 10-character integer timestamp, which issue \
             #406 Part D now reads as unix SECONDS — re-express them in nanoseconds (or as `0`, \
             which means the same instant either way): {offenders:?}"
        );
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Surface {
        Logs,
        Other,
    }

    /// A file's default surface, from its path.
    fn surface_of(rel: &str) -> Surface {
        let lower = rel.to_ascii_lowercase();
        if lower.contains("prom") || lower.contains("trace") || lower.contains("profile") {
            Surface::Other
        } else if lower.contains("log") || lower.contains("loki") {
            Surface::Logs
        } else {
            Surface::Other
        }
    }

    /// A line that says which API surface the lines after it describe: a
    /// quoted route path, or an item declaration whose name names one.
    fn surface_marker(line: &str) -> Option<Surface> {
        if line.contains("/api/logs/") || line.contains("/loki/api/") {
            return Some(Surface::Logs);
        }
        if line.contains("/api/v1/")
            || line.contains("/api/traces/")
            || line.contains("/api/profiles/")
        {
            return Some(Surface::Other);
        }
        let rest = ["fn ", "const ", "static "]
            .iter()
            .find_map(|kw| line.split_once(kw).map(|(_, rest)| rest))?;
        let name: String = rest
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect::<String>()
            .to_ascii_lowercase();
        if name.is_empty() {
            return None;
        }
        if name.contains("prom") || name.contains("trace") || name.contains("profile") {
            Some(Surface::Other)
        } else if name.contains("log") || name.contains("loki") {
            Some(Surface::Logs)
        } else {
            None
        }
    }

    /// `start=`/`end=`/`time=` query-string keys carrying an integer of ten
    /// characters or fewer — the values Part D re-reads as seconds. `0` is
    /// excluded: it is the same instant on either rule.
    fn short_integer_timestamps(line: &str) -> Vec<String> {
        let mut out = Vec::new();
        for key in ["start=", "end=", "time="] {
            let mut from = 0usize;
            while let Some(rel) = line[from..].find(key) {
                let at = from + rel;
                from = at + key.len();
                // A query-string key, not `let start=` or `..._start=`.
                let prev = line[..at].chars().next_back();
                if !matches!(prev, None | Some('?') | Some('&') | Some('"') | Some('\'')) {
                    continue;
                }
                let value: String = line[from..]
                    .chars()
                    .take_while(|c| c.is_ascii_digit() || *c == '-')
                    .collect();
                // The next character must end the value, or these digits
                // are a prefix of something else (`{start}`, `%7D`).
                let after = line[from + value.len()..].chars().next();
                if !matches!(after, None | Some('&') | Some('"') | Some('\'') | Some(' ')) {
                    continue;
                }
                if value.is_empty() || value.len() > 10 || value.trim_start_matches('-') == "0" {
                    continue;
                }
                out.push(format!("{key}{value}"));
            }
        }
        out
    }

    /// Every `.rs` source and `.test` corpus file under `dir`.
    fn collect_scan_files(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_scan_files(&path, out);
            } else if path.extension().is_some_and(|x| x == "rs" || x == "test") {
                out.push(path);
            }
        }
    }

    // -- Issue #406 Part C: `since` --------------------------------------

    #[test]
    fn parse_since_defaults_to_one_hour_and_rejects_a_bad_value() {
        assert_eq!(parse_since(None).unwrap(), DEFAULT_SINCE_NS);
        assert_eq!(parse_since(Some("30m")).unwrap(), 1_800_000_000_000);
        assert_eq!(parse_since(Some("5m")).unwrap(), 300_000_000_000);
        assert_eq!(parse_since(Some("1h")).unwrap(), DEFAULT_SINCE_NS);
        // A bare integer is seconds (`parse_duration_ns`'s grammar).
        assert_eq!(parse_since(Some("300")).unwrap(), 300_000_000_000);
        for raw in ["bogus", "-5m", "-5", "5x", ""] {
            assert!(
                matches!(parse_since(Some(raw)), Err(ParamError::InvalidSince(_))),
                "since={raw:?} must be a 400"
            );
        }
    }

    // -- Issue #406: the POST `Content-Type` rule ------------------------

    /// **Every row is a container measurement, not a source reading**, and
    /// the two columns are the only two questions the reference answers
    /// about this header: is the BODY read, and is the REQUEST refused.
    ///
    /// Measured 2026-08-10 against `grafana/loki:3.7.4` (`b318f282`) and,
    /// side by side, against a `pulsusdb` carrying the same 160-entry
    /// corpus: a `POST /query_range` with `limit=7` in the URL and
    /// `limit=5` in the body, so the ANSWER distinguishes the outcomes —
    /// 5 entries means the body was read, 7 means it was ignored, and a
    /// `400` means the request was refused. All 52 probed rows (this
    /// table plus [`the_media_type_parameter_walk_matches_the_reference`])
    /// agreed on both stores.
    ///
    /// **The discriminating rows are the ones a "just accept everything"
    /// fix gets wrong.** `application/` and `;` are `400` at the reference
    /// — its `ParseForm` propagates `mime.ParseMediaType`'s error and the
    /// middleware turns it into a `400` before any handler runs — and
    /// they were `400` here BEFORE this change too, for the unrelated
    /// reason that we refused everything non-form. Widening to
    /// "unconditionally ignore a non-form body" would turn those into
    /// `200`s and trade one divergence for another; that is what the
    /// [`MediaTypeError`] half of the port exists for.
    ///
    /// `application/x-www-form-urlencodedX` is the mirror image: a prefix
    /// match (which is what this gate used to be) READS a body the
    /// reference ignores.
    #[test]
    fn the_post_content_type_decides_the_body_not_the_request() {
        use FormBody::{Ignore, Parse};

        // (Content-Type, expected disposition — `None` = a 400.)
        let cases: &[(Option<&str>, Option<FormBody>)] = &[
            // Absent, empty, and whitespace-only all read as `""`, which
            // Go substitutes with `application/octet-stream`. (Both HTTP
            // stacks strip a header value's surrounding whitespace before
            // the handler sees it, so the third case arrives as the
            // second — measured on both stores, raw socket.)
            (None, Some(Ignore)),
            (Some(""), Some(Ignore)),
            // Well-formed, non-form: the body is invisible, the request
            // is served. This is the row that refused working clients.
            (Some("application/json"), Some(Ignore)),
            (Some("APPLICATION/JSON"), Some(Ignore)),
            (Some("text/plain"), Some(Ignore)),
            (Some("application/octet-stream"), Some(Ignore)),
            (Some("application/xhtml+xml"), Some(Ignore)),
            // `multipart/form-data` is a form type and STILL does not
            // reach `parsePostForm`'s body read (`request.go:1298-1304`
            // is an empty case) — so it ignores, like any other.
            (Some("multipart/form-data"), Some(Ignore)),
            // A bare token with no slash is a LEGAL media type
            // (`checkMediaTypeDisposition` returns nil when nothing
            // follows the first token), so it is served, not refused.
            (Some("garbage"), Some(Ignore)),
            (Some("x"), Some(Ignore)),
            (Some("x/y"), Some(Ignore)),
            // Form: the body is read.
            (Some("application/x-www-form-urlencoded"), Some(Parse)),
            (
                Some("application/x-www-form-urlencoded; charset=UTF-8"),
                Some(Parse),
            ),
            // Case-insensitive: `mime.ParseMediaType` lowercases the base
            // type. The old prefix gate refused this one.
            (Some("APPLICATION/X-WWW-Form-URLENCODED"), Some(Parse)),
            // Surrounding whitespace is trimmed off the base type.
            (Some("  application/x-www-form-urlencoded  "), Some(Parse)),
            // NOT a prefix match: one trailing character makes it a
            // different media type, whose body is ignored.
            (Some("application/x-www-form-urlencodedX"), Some(Ignore)),
            // Malformed — refused, on both stores, before the body.
            (Some("application/"), None),
            (Some("/json"), None),
            (Some(";"), None),
            (Some("="), None),
            (Some("application/json/x"), None),
            (Some("x/y/z"), None),
            (Some("app lication/json"), None),
            (Some("\"quoted/type\""), None),
            (Some("application/x-www-form-urlencoded, text/plain"), None),
        ];

        for (raw, expected) in cases {
            let got = form_body_disposition(*raw);
            match expected {
                Some(disposition) => assert_eq!(
                    got.as_ref().ok(),
                    Some(disposition),
                    "Content-Type {raw:?} must be {disposition:?}, got {got:?}"
                ),
                None => assert!(
                    matches!(got, Err(ParamError::MalformedContentType(_))),
                    "Content-Type {raw:?} must be a 400, got {got:?}"
                ),
            }
        }
    }

    /// The parameter half of `mime.ParseMediaType`, which
    /// `parsePostForm` discards (`ct, _, err = …`) but still WALKS — so a
    /// parameter that does not parse refuses the whole request even when
    /// the base type is the form type it was going to accept.
    ///
    /// Every row container-measured 2026-08-10, same two stores, same
    /// probe. The pairs are the point: `a=1; a=1` is served and
    /// `a=1; a=2` is refused (a duplicate is an error only when the
    /// values DISAGREE); `charset="utf-8"` is served and
    /// `charset="utf-8` is refused (an unterminated quoted-string
    /// consumes no value); a trailing `;` is served and a trailing `; ;`
    /// is refused. Dropping the walk entirely satisfies neither half of
    /// any pair.
    #[test]
    fn the_media_type_parameter_walk_matches_the_reference() {
        use FormBody::{Ignore, Parse};

        let cases: &[(&str, Option<FormBody>)] = &[
            (
                "application/x-www-form-urlencoded; charset=\"utf-8\"",
                Some(Parse),
            ),
            // No closing quote: `consumeValue` consumes nothing.
            ("application/x-www-form-urlencoded; charset=\"utf-8", None),
            // Go's MSIE rule: a backslash escapes a `tspecial` and is a
            // literal byte before anything else. Both parse.
            (
                "application/x-www-form-urlencoded; charset=\"a\\;b\"",
                Some(Parse),
            ),
            (
                "application/x-www-form-urlencoded; charset=\"a\\qb\"",
                Some(Parse),
            ),
            // A bare `=` with nothing after it consumes no value.
            ("application/x-www-form-urlencoded; charset=", None),
            // An empty QUOTED value does.
            (
                "application/x-www-form-urlencoded; charset=\"\"",
                Some(Parse),
            ),
            (
                "application/x-www-form-urlencoded ; charset=utf-8",
                Some(Parse),
            ),
            (
                "application/x-www-form-urlencoded;charset=utf-8;boundary=x",
                Some(Parse),
            ),
            // RFC 2231 continuations: walked and duplicate-checked, never
            // stitched (the stitching pass cannot fail).
            (
                "application/x-www-form-urlencoded; a*0=1; a*1=2",
                Some(Parse),
            ),
            ("application/x-www-form-urlencoded; a*0=1; a*0=2", None),
            // A repeat whose value agrees is allowed; one that disagrees
            // is not — and the name is compared case-folded.
            ("application/x-www-form-urlencoded; a=1; a=1", Some(Parse)),
            ("application/x-www-form-urlencoded; a=1; a=2", None),
            ("application/x-www-form-urlencoded; A=1; a=2", None),
            (
                "application/x-www-form-urlencoded; charset=utf-8; charset=utf-8",
                Some(Parse),
            ),
            ("application/x-www-form-urlencoded; a=1; b=1", Some(Parse)),
            // One trailing `;` is deliberately not an error; a second is.
            ("application/x-www-form-urlencoded;", Some(Parse)),
            ("application/x-www-form-urlencoded;;", None),
            ("application/x-www-form-urlencoded; ;", None),
            ("application/x-www-form-urlencoded; bogus", None),
            (
                "APPLICATION/X-WWW-FORM-URLENCODED; CHARSET=UTF-8",
                Some(Parse),
            ),
            // The walk applies to a type whose body would be ignored too.
            ("application/json; charset=utf-8", Some(Ignore)),
            ("application/json; charset", None),
            ("text/plain; a=1; a=2", None),
        ];

        for (raw, expected) in cases {
            let got = form_body_disposition(Some(raw));
            match expected {
                Some(disposition) => assert_eq!(
                    got.as_ref().ok(),
                    Some(disposition),
                    "Content-Type {raw:?} must be {disposition:?}, got {got:?}"
                ),
                None => assert!(
                    matches!(got, Err(ParamError::MalformedContentType(_))),
                    "Content-Type {raw:?} must be a 400, got {got:?}"
                ),
            }
        }
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

    // -- Issue #170: /detected_fields params ------------------------------

    #[test]
    fn parse_line_limit_defaults_to_100_when_absent() {
        assert_eq!(parse_line_limit(None).unwrap(), DEFAULT_LINE_LIMIT);
    }

    #[test]
    fn parse_line_limit_rejects_zero_and_non_numeric() {
        assert!(matches!(
            parse_line_limit(Some("0")).unwrap_err(),
            ParamError::InvalidLineLimit(_)
        ));
        assert!(matches!(
            parse_line_limit(Some("abc")).unwrap_err(),
            ParamError::InvalidLineLimit(_)
        ));
    }

    #[test]
    fn parse_line_limit_accepts_the_cap_and_rejects_one_above_it() {
        assert_eq!(parse_line_limit(Some("5000")).unwrap(), 5000);
        assert!(matches!(
            parse_line_limit(Some("5001")).unwrap_err(),
            ParamError::LimitTooLarge {
                limit: 5001,
                max: 5000
            }
        ));
    }

    #[test]
    fn parse_field_limit_defaults_to_1000_when_both_params_are_absent() {
        assert_eq!(parse_field_limit(None, None).unwrap(), DEFAULT_FIELD_LIMIT);
    }

    /// The reference reads `limit` first and only falls back to the
    /// legacy `field_limit` alias.
    #[test]
    fn parse_field_limit_prefers_limit_over_the_legacy_field_limit_alias() {
        assert_eq!(parse_field_limit(Some("7"), Some("9")).unwrap(), 7);
        assert_eq!(parse_field_limit(None, Some("9")).unwrap(), 9);
    }

    #[test]
    fn parse_field_limit_rejects_zero_and_non_numeric() {
        assert!(matches!(
            parse_field_limit(Some("0"), None).unwrap_err(),
            ParamError::InvalidFieldLimit(_)
        ));
        assert!(matches!(
            parse_field_limit(None, Some("abc")).unwrap_err(),
            ParamError::InvalidFieldLimit(_)
        ));
    }

    // -- Issue #253: /detected_fields' three limit params -----------------
    //
    // Every `// ref:` comment below records what `grafana/loki:3.7.4`
    // answered for that exact spelling, measured 2026-08-07 against a
    // single-binary container (default `limits_config` plus
    // `allow_structured_metadata: true` / `discover_log_levels: false` /
    // `split_queries_by_interval: 0`) holding one stream of 30 JSON
    // entries with 41 distinct field names.

    /// Present-but-empty is ABSENT on this endpoint's limit params: Go's
    /// `r.Form.Get` cannot distinguish the two and the reference's
    /// `parseInt(value, def)` returns `def` for `""`
    /// (`pkg/loghttp/params.go:154-159 @ v3.7.4 b318f282`). Our
    /// `params::get` returns `Some("")`, which used to fail the typed
    /// parse and 400.
    #[test]
    fn parse_field_limit_treats_an_empty_limit_as_absent_and_falls_through_to_the_alias() {
        // ref: `?limit=&field_limit=7` -> 200, 7 fields, "limit":7
        assert_eq!(parse_field_limit(Some(""), Some("7")).unwrap(), 7);
        // ref: `?limit=&field_limit=` -> 200, 41 fields, "limit":1000
        assert_eq!(
            parse_field_limit(Some(""), Some("")).unwrap(),
            DEFAULT_FIELD_LIMIT
        );
        // ref: `?limit=` -> 200, 41 fields, "limit":1000
        assert_eq!(
            parse_field_limit(Some(""), None).unwrap(),
            DEFAULT_FIELD_LIMIT
        );
        // ref: `?field_limit=` -> 200, 41 fields, "limit":1000
        assert_eq!(
            parse_field_limit(None, Some("")).unwrap(),
            DEFAULT_FIELD_LIMIT
        );
    }

    /// Same rule on the entry axis: `?line_limit=` measures 200 with the
    /// 100 default on the reference.
    #[test]
    fn parse_line_limit_treats_an_empty_value_as_absent() {
        assert_eq!(parse_line_limit(Some("")).unwrap(), DEFAULT_LINE_LIMIT);
    }

    /// The accepted numeric-literal set is `strconv.Atoi`'s, established
    /// by probing the reference across the boundary forms rather than by
    /// reading `Atoi`'s source: every spelling below was sent to the
    /// container as `limit=` and `i64::from_str` agrees with each.
    /// `Ok`/`Err` here is the reference's `200`/`400` — see each `// ref:`
    /// note. The alias is asserted to share that surface, which the
    /// reference's `detectedFieldsLimit` does by construction (one
    /// `parseInt` over whichever of the two keys it picked) and which was
    /// spot-measured at `field_limit=5001`, `4294967295` and `4294967296`.
    #[test]
    fn parse_field_limit_matches_the_reference_atoi_surface() {
        // Accepted, with the value the reference echoed back.
        for (raw, want) in [
            ("1", 1_u32),                      // ref: 200, "limit":1
            ("5000", 5000),                    // ref: 200, "limit":5000
            ("5001", 5001),                    // ref: 200, "limit":5001
            ("50000", 50_000),                 // ref: 200, "limit":50000
            ("1000000", 1_000_000),            // ref: 200, "limit":1000000
            ("2147483647", 2_147_483_647),     // ref: 200, "limit":2147483647
            ("+100", 100),                     // ref: 200, "limit":100
            ("+007", 7),                       // ref: 200, "limit":7
            ("007", 7),                        // ref: 200, "limit":7
            ("000000000000000000005", 5),      // ref: 200, "limit":5
            ("4294967295", u32::MAX),          // ref: 200, "limit":4294967295
            ("9223372036854775807", u32::MAX), // ref: 200, "limit":4294967295
        ] {
            assert_eq!(
                parse_field_limit(Some(raw), None).unwrap(),
                want,
                "limit={raw:?}"
            );
            assert_eq!(
                parse_field_limit(None, Some(raw)).unwrap(),
                want,
                "field_limit={raw:?}"
            );
        }

        // Rejected as non-positive: `Atoi` accepts the literal, the
        // reference's own `if l <= 0` then answers
        // `400 limit must be a positive value`.
        for raw in [
            "0",
            "-1",
            "+0",
            "-0",
            "00",
            "-007",
            "0000000000000000000000000",
        ] {
            assert!(
                matches!(
                    parse_field_limit(Some(raw), None).unwrap_err(),
                    ParamError::InvalidFieldLimit(_)
                ),
                "limit={raw:?} must be a 400"
            );
        }

        // Rejected by the literal grammar: the reference answers
        // `400 strconv.Atoi: parsing <raw>: invalid syntax` for the first
        // group and `... value out of range` for the overlong group.
        for raw in [
            " 100",                  // leading space
            "100 ",                  // trailing space
            "  ",                    // whitespace only
            "1.5",                   // decimal point
            "1e3",                   // exponent
            "0x10",                  // hex
            "1_0",                   // underscore separator
            "abc",                   // non-numeric
            "+",                     // sign only
            "-",                     // sign only
            "\u{661}\u{662}\u{663}", // Arabic-Indic digits: Atoi is ASCII-only
            "9223372036854775808",   // i64::MAX + 1
            "18446744073709551615",  // u64::MAX  (a u64 parse would have accepted)
            "18446744073709551616",  // u64::MAX + 1
            "999999999999999999999999999999",
        ] {
            assert!(
                matches!(
                    parse_field_limit(Some(raw), None).unwrap_err(),
                    ParamError::InvalidFieldLimit(_)
                ),
                "limit={raw:?} must be a 400"
            );
        }
    }

    /// The same accepted set on the entry axis, where [`MAX_LIMIT`] then
    /// applies. The reference rejects everything above 5000 too — this is
    /// parity, not a house cap (`validation.max-entries-limit`, default
    /// 5000, `pkg/validation/limits.go:355`, enforced by
    /// `validateMaxEntriesLimits`, `pkg/querier/queryrange/limits.go:767-780`).
    #[test]
    fn parse_line_limit_matches_the_reference_atoi_surface() {
        // ref: 200 (the response is identical for every accepted value —
        // `line_limit` is not echoed).
        for (raw, want) in [("1", 1_u32), ("+100", 100), ("007", 7), ("5000", 5000)] {
            assert_eq!(parse_line_limit(Some(raw)).unwrap(), want, "{raw:?}");
        }

        // ref: 400 `limit must be a positive value`.
        for raw in ["0", "-1", "00"] {
            assert!(
                matches!(
                    parse_line_limit(Some(raw)).unwrap_err(),
                    ParamError::InvalidLineLimit(_)
                ),
                "{raw:?}"
            );
        }

        // ref: 400 `strconv.Atoi: ... invalid syntax` / `value out of range`.
        for raw in [" 100", "1.5", "1_0", "abc", "9223372036854775808"] {
            assert!(
                matches!(
                    parse_line_limit(Some(raw)).unwrap_err(),
                    ParamError::InvalidLineLimit(_)
                ),
                "{raw:?}"
            );
        }

        // ref: 400 `max entries limit per query exceeded, limit >
        // max_entries_limit_per_query (N > 5000)`.
        for raw in ["5001", "50000", "4294967295"] {
            assert!(
                matches!(
                    parse_line_limit(Some(raw)).unwrap_err(),
                    ParamError::LimitTooLarge { max: 5000, .. }
                ),
                "{raw:?}"
            );
        }

        // Above 2^32 the reference's unchecked `uint32(l)`
        // (`pkg/loghttp/params.go:38-46`) wraps and it ACCEPTS: measured,
        // `line_limit=4294967296` -> 400 `limit must be a positive value`
        // (wrapped to 0), `4294967396` -> 200. That the wrapped value is
        // what it actually SAMPLES was measured on a per-entry-varying
        // field's cardinality: `4294967297` -> 1, like `line_limit=1`;
        // `4294967326` -> 30, like `line_limit=30`. `parse_line_limit`
        // performs no such cast — it compares the `i64` to MAX_LIMIT and
        // REJECTS, at every magnitude — so these stay 400 here. The
        // `detected-fields-limit-saturates-not-wraps` ledger row.
        for raw in ["4294967296", "4294967396", "8589934692"] {
            assert!(
                matches!(
                    parse_line_limit(Some(raw)).unwrap_err(),
                    ParamError::LimitTooLarge { max: 5000, .. }
                ),
                "{raw:?}"
            );
        }
    }

    /// The entry axis REFUSES an out-of-range value; it does not saturate
    /// and then fail a ceiling. The two are indistinguishable by status,
    /// so this pins the observable that separates them: the reported
    /// `limit` is the value as parsed, never `u32::MAX`. A
    /// saturate-then-check implementation would report `4294967295` for
    /// every one of these.
    ///
    /// Also states the range invariant the doc comment claims — every
    /// accepted `line_limit`, including the absent/empty default, lands in
    /// `1..=MAX_LIMIT` — which holds because the `as u32` in
    /// [`parse_line_limit`] is reachable only under `0 < n <= MAX_LIMIT`.
    #[test]
    fn parse_line_limit_refuses_out_of_range_rather_than_saturating() {
        for (raw, want) in [
            ("5001", 5001_u64),
            ("4294967295", 4_294_967_295),
            ("4294967296", 4_294_967_296),
            ("9223372036854775807", 9_223_372_036_854_775_807),
        ] {
            match parse_line_limit(Some(raw)).unwrap_err() {
                ParamError::LimitTooLarge { limit, max } => {
                    assert_eq!(limit, want, "{raw:?} must be reported as parsed");
                    assert_eq!(max, MAX_LIMIT, "{raw:?}");
                }
                other => panic!("{raw:?} must be LimitTooLarge, got {other:?}"),
            }
        }

        for raw in [None, Some(""), Some("1"), Some("100"), Some("5000")] {
            let got = parse_line_limit(raw).expect("accepted");
            assert!(
                (1..=MAX_LIMIT).contains(&got),
                "{raw:?} produced {got}, outside 1..={MAX_LIMIT}"
            );
        }
    }

    /// The one deliberate value divergence on this endpoint: the
    /// reference's effective field limit is `uint32(l)` with no range
    /// check (`pkg/loghttp/params.go:63 @ v3.7.4 b318f282`), so a larger
    /// `limit` can return FEWER fields. Measured on a 41-field fixture:
    /// `4294967295` -> 41 fields, echo `4294967295`; `4294967296` -> **0
    /// fields, no `limit` key at all**; `4294967297` -> 1 field, echo 1;
    /// `9223372036854775807` -> 41 fields, echo `4294967295`. We saturate.
    /// Registered as `detected-fields-limit-saturates-not-wraps` in
    /// docs/benchmarks/logs-differential-ledger.md.
    #[test]
    fn parse_field_limit_saturates_where_the_reference_wraps() {
        assert_eq!(
            parse_field_limit(Some("4294967295"), None).unwrap(),
            u32::MAX
        );
        assert_eq!(
            parse_field_limit(Some("4294967296"), None).unwrap(),
            u32::MAX
        );
        assert_eq!(
            parse_field_limit(Some("4294967297"), None).unwrap(),
            u32::MAX
        );
        assert_eq!(
            parse_field_limit(Some("9223372036854775807"), None).unwrap(),
            u32::MAX
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

    // -- Issue #227: Loki's (end-start)/step > 11000 resolution limit ----

    #[test]
    fn resolution_at_the_11000_point_limit_is_accepted() {
        let s = 1_000_000_000i64;
        // 11000 intervals exactly (Loki trips only on `> 11000`).
        assert!(ensure_range_resolution(0, 11_000 * s, s as u64).is_ok());
    }

    #[test]
    fn resolution_over_the_11000_point_limit_is_the_loki_400_message() {
        let s = 1_000_000_000i64;
        let err = ensure_range_resolution(0, 11_001 * s, s as u64).unwrap_err();
        assert!(matches!(err, ParamError::MaxResolutionExceeded));
        assert_eq!(
            err.to_string(),
            "exceeded maximum resolution of 11,000 points per time series. Try increasing the \
             value of the step parameter"
        );
    }

    #[test]
    fn resolution_check_does_not_overflow_at_the_full_i64_span() {
        // i64::MIN..i64::MAX at a 1ns step: the SATURATED span (i64::MAX
        // ns, Go `time.Time.Sub`'s `maxDuration` clamp) is still ~9.2e18
        // intervals — reject, never panic/wrap.
        assert!(matches!(
            ensure_range_resolution(i64::MIN, i64::MAX, 1).unwrap_err(),
            ParamError::MaxResolutionExceeded
        ));
    }

    #[test]
    fn resolution_fence_saturates_the_span_like_the_reference() {
        // Issue #227 review round 8: the reference's `End.Sub(Start)` (Go
        // `time.Time.Sub`) SATURATES an out-of-range difference at
        // `maxDuration = 1<<63-1` ns rather than widening. The full-domain
        // span at a 1_000_000s step therefore counts i64::MAX/step = 9_223
        // intervals — SERVED — where exact i128 arithmetic counted the true
        // 2^64-1 ns span as 18_446 and wrongly rejected.
        const STEP: u64 = 1_000_000_000_000_000; // 1_000_000s in ns
        assert!(ensure_range_resolution(i64::MIN, i64::MAX, STEP).is_ok());

        // The saturated-fence boundary: floor(i64::MAX / 11_001) is the
        // largest step counting 11_001 saturated intervals (reject); one
        // nanosecond of step more counts 11_000 (admit).
        let reject_step = (i64::MAX / 11_001) as u64;
        assert!(matches!(
            ensure_range_resolution(i64::MIN, i64::MAX, reject_step).unwrap_err(),
            ParamError::MaxResolutionExceeded
        ));
        assert!(ensure_range_resolution(i64::MIN, i64::MAX, reject_step + 1).is_ok());

        // Saturation onset: a span of exactly i64::MAX ns is byte-identical
        // saturated or exact; the ordinary domain is untouched.
        assert!(ensure_range_resolution(-1, i64::MAX - 1, STEP).is_ok());
        assert!(ensure_range_resolution(-1, i64::MAX - 1, reject_step).is_err());
    }

    #[test]
    fn resolution_check_ignores_a_non_positive_span() {
        assert!(ensure_range_resolution(100, 50, 1).is_ok());
    }

    // -- Issue #171: /patterns step floor + grid ------------------------

    #[test]
    fn parse_pattern_step_floors_a_finer_step_up_to_the_10s_bucket() {
        // 3s requested → floors to the 10s bucket resolution.
        assert_eq!(
            parse_pattern_step(Some("3"), 0, 60_000_000_000).unwrap(),
            PATTERN_STEP_FLOOR_NS
        );
        // 25s requested → floors DOWN to 20s (a multiple of 10s).
        assert_eq!(
            parse_pattern_step(Some("25"), 0, 60_000_000_000).unwrap(),
            20_000_000_000
        );
    }

    #[test]
    fn parse_pattern_step_derived_default_is_at_least_the_10s_floor() {
        // A short window derives a sub-10s step; it floors up to 10s.
        assert_eq!(
            parse_pattern_step(None, 0, 1_000_000_000).unwrap(),
            PATTERN_STEP_FLOOR_NS
        );
    }

    #[test]
    fn parse_pattern_step_rejects_an_over_11k_grid() {
        // 11_001 × 10s window at the 10s floor step ⇒ 11_001 buckets > 11_000.
        let end = 11_001 * PATTERN_STEP_FLOOR_NS as i64;
        assert!(matches!(
            parse_pattern_step(Some("10"), 0, end).unwrap_err(),
            ParamError::PatternGridTooLarge {
                buckets: 11_001,
                max: 11_000
            }
        ));
        // Exactly at the cap passes.
        let end_ok = 11_000 * PATTERN_STEP_FLOOR_NS as i64;
        assert_eq!(
            parse_pattern_step(Some("10"), 0, end_ok).unwrap(),
            PATTERN_STEP_FLOOR_NS
        );
    }

    #[test]
    fn parse_pattern_step_rejects_a_non_positive_explicit_step() {
        assert!(matches!(
            parse_pattern_step(Some("0"), 0, 0).unwrap_err(),
            ParamError::InvalidStep { .. }
        ));
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

    // -- Issue #391: a present-but-empty SCALAR param is an absent one ----
    //
    // Every `// ref:` comment below records what `grafana/loki:3.7.4`
    // answered for that exact spelling, container-measured 2026-08-09
    // (issue #391 architect plan) against a single-binary container
    // holding one 150-entry stream.

    /// **First, then filter — not "the first non-empty".** Go's
    /// `r.Form.Get` returns the FIRST value for the key, and the parse
    /// helper behind it then treats `""` as absent
    /// (`pkg/loghttp/params.go:152-159 @ v3.7.4 b318f282`); it never skips
    /// to the first non-empty occurrence. Only the duplicate-key pair
    /// discriminates between the two candidate spellings of the collapse
    /// — every single-occurrence case passes either way, so this is the
    /// test that pins the fix SHAPE.
    ///
    /// The rest of the case list is the measured boundary: only the
    /// literal empty string collapses (and a bare key with no `=`, which
    /// decodes to one). A space, a `+`, a tab and a NUL are VALUES, and
    /// 400s, on both sides.
    #[test]
    fn get_takes_the_first_value_then_treats_empty_as_absent() {
        // ref: `?limit=&limit=5` -> 200, 100 entries (the default), NOT 5.
        assert_eq!(get(&parse_pairs("limit=&limit=5"), "limit"), None);
        // ref: `?limit=5&limit=` -> 200, 5 entries.
        assert_eq!(get(&parse_pairs("limit=5&limit="), "limit"), Some("5"));
        // ref: `?limit` (no `=` at all) -> 200, 100 entries.
        assert_eq!(get(&parse_pairs("limit"), "limit"), None);
        // ref: `?limit=%20` -> 400 `strconv.Atoi: parsing " ": invalid syntax`.
        assert_eq!(get(&parse_pairs("limit=%20"), "limit"), Some(" "));
        // ref: `?limit=+` -> 400, same message (`+` decodes to a space).
        assert_eq!(get(&parse_pairs("limit=+"), "limit"), Some(" "));
        // ref: `?limit=%09` -> 400 `strconv.Atoi: parsing "\t": invalid syntax`.
        assert_eq!(get(&parse_pairs("limit=%09"), "limit"), Some("\t"));
        // ref: `?limit=%00` -> 400 `strconv.Atoi: parsing "\x00": invalid syntax`.
        assert_eq!(get(&parse_pairs("limit=%00"), "limit"), Some("\0"));
    }

    /// Issue #391: empty and absent produce the same VALUE, not merely
    /// the same status — for every scalar param on this surface the
    /// collapsed result is the documented default the absent path already
    /// returns. Status identity alone would be satisfied by defaulting to
    /// something else entirely; this is the assertion that makes "no
    /// parameter here has a destructive default" breakable.
    #[test]
    fn every_scalar_param_defaults_identically_whether_empty_or_absent() {
        let empty = parse_pairs("k=");
        let absent: Vec<(String, String)> = Vec::new();

        // ref: `?limit=` -> 200, 100 entries against a 150-entry stream
        // (`?limit=150` -> 150).
        assert_eq!(
            parse_limit(get(&empty, "k")).unwrap(),
            parse_limit(get(&absent, "k")).unwrap()
        );
        assert_eq!(parse_limit(get(&empty, "k")).unwrap(), DEFAULT_LIMIT);

        // ref: `/index/volume?limit=` -> 200, the `seriesvolume.DefaultLimit`
        // of 100 (`pkg/loghttp/query.go:723-739`, `volume.go:13`).
        assert_eq!(
            parse_volume_limit(get(&empty, "k")).unwrap(),
            parse_volume_limit(get(&absent, "k")).unwrap()
        );
        assert_eq!(parse_volume_limit(get(&empty, "k")).unwrap(), DEFAULT_LIMIT);

        // ref: `?direction=` -> 200, first entry `seq 0` — identical to
        // `backward` and unlike `forward`'s `seq 29`.
        assert_eq!(
            parse_direction(get(&empty, "k")).unwrap(),
            parse_direction(get(&absent, "k")).unwrap()
        );
        assert_eq!(
            parse_direction(get(&empty, "k")).unwrap(),
            Direction::Backward
        );

        // ref: `/index/volume?aggregateBy=` -> 200, label-PAIR metrics —
        // identical to `series` and unlike `labels`' `{"env":""}` shape.
        assert_eq!(
            parse_aggregate_by(get(&empty, "k")).unwrap(),
            parse_aggregate_by(get(&absent, "k")).unwrap()
        );
        assert_eq!(
            parse_aggregate_by(get(&empty, "k")).unwrap(),
            VolumeAggregateBy::Series
        );

        // ref: `/index/volume?targetLabels=` -> 200, unaggregated (nil
        // targets, `pkg/loghttp/query.go:714-721`).
        assert_eq!(
            parse_target_labels(get(&empty, "k")).unwrap(),
            parse_target_labels(get(&absent, "k")).unwrap()
        );
        assert!(parse_target_labels(get(&empty, "k")).unwrap().is_empty());

        // ref: `/detected_fields?line_limit=` -> 200, the 100 default
        // (issue #253's own measurement, re-pinned here through `get`).
        assert_eq!(
            parse_line_limit(get(&empty, "k")).unwrap(),
            parse_line_limit(get(&absent, "k")).unwrap()
        );
        assert_eq!(
            parse_line_limit(get(&empty, "k")).unwrap(),
            DEFAULT_LINE_LIMIT
        );

        // ref: `/detected_fields?limit=&field_limit=` -> 200, "limit":1000.
        assert_eq!(
            parse_field_limit(get(&empty, "k"), get(&empty, "k")).unwrap(),
            parse_field_limit(get(&absent, "k"), get(&absent, "k")).unwrap()
        );
        assert_eq!(
            parse_field_limit(get(&empty, "k"), get(&empty, "k")).unwrap(),
            DEFAULT_FIELD_LIMIT
        );

        // `start`/`end`/`time` are read as
        // `match get(..) { Some(v) => parse_ts(v)?, None => <default> }`
        // (`handlers::parse_bounds`, `handlers::query_impl`), so the seam
        // is modelled rather than called through the handler here — the
        // routed half is `mod.rs`'s identity table.
        // ref: `?start=`/`?end=`/`?time=` -> 200, the same window as absent
        // (`parseTimestamp(value, def)`, `pkg/loghttp/params.go:161-186`).
        let ts_or_default = |raw: Option<&str>, default_ns: i64| match raw {
            Some(v) => parse_ts(v).unwrap(),
            None => default_ns,
        };
        const SENTINEL_NS: i64 = 1_782_864_000_000_000_000;
        assert_eq!(
            ts_or_default(get(&empty, "k"), SENTINEL_NS),
            ts_or_default(get(&absent, "k"), SENTINEL_NS)
        );
        assert_eq!(ts_or_default(get(&empty, "k"), SENTINEL_NS), SENTINEL_NS);

        // ref: `?step=` on a 1h metric query -> 200, 11 points — identical
        // to absent and to the derived `step=14s`, unlike `step=60s`'s 3.
        let start_ns = 1_782_864_000_000_000_000;
        let end_ns = start_ns + 3_600_000_000_000;
        assert_eq!(
            parse_step(get(&empty, "k"), start_ns, end_ns).unwrap(),
            parse_step(get(&absent, "k"), start_ns, end_ns).unwrap()
        );
        assert_eq!(
            parse_step(get(&empty, "k"), start_ns, end_ns).unwrap(),
            parse_step(None, start_ns, end_ns).unwrap()
        );

        // `/patterns`' step rides the same seam through `parse_step`.
        assert_eq!(
            parse_pattern_step(get(&empty, "k"), start_ns, end_ns).unwrap(),
            parse_pattern_step(get(&absent, "k"), start_ns, end_ns).unwrap()
        );
    }
}
