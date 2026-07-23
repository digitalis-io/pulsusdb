//! `ReadError` — the LogQL read-path error taxonomy. Follows
//! `pulsus-schema::SchemaError`/`pulsus-write::LogsIngestError`'s style:
//! `thiserror`, one variant per distinguishable failure, each carrying
//! enough context to be actionable in the `X-Pulsus-Explain`/query-error
//! envelope #13 builds.
//!
//! **Budget vs stream-cap are structurally distinct** (architect plan
//! amendment §4, review finding 4): a ClickHouse row/stream limit must
//! never masquerade as the byte scan budget, and neither is ever surfaced
//! as a generic `ChError::Timeout` — a caller inspecting `ReadError` can
//! always tell "this query was too broad" from "this query's execution
//! genuinely stalled".

use std::fmt;

use pulsus_clickhouse::ChError;
use pulsus_logql::LogQlError;
use thiserror::Error;

/// Why a query was rejected as too broad. Six structurally separate
/// reason families, never conflated (architect plan amendment §4; issue
/// #57 adds the traces row budget and (re-audit) the traces generator
/// memory budget; issue #59 adds the trace-metrics IN-set budget), each
/// with its own exclusive code path:
///
/// - [`TooBroadReason::ScanBudgetBytes`] — byte budgets only: LogQL
///   `max_bytes_to_read` (code 307), the traces `max_bytes_to_read`/
///   `max_result_bytes` throw settings (codes 307/396), and the traces
///   engine's Rust-side retention counter.
/// - [`TooBroadReason::StreamCap`] — the LogQL Stage-1 fingerprint cap,
///   a Rust-side structural limit; never produced from a ClickHouse
///   error code.
/// - [`TooBroadReason::TraceScanBudgetRows`] — `max_rows_to_read` (code
///   158) on the traces read paths only; LogQL never sets that setting.
/// - [`TooBroadReason::TraceMetricsSetRows`] — the trace-metrics
///   semi-join IN-set limits (`max_rows_in_set`/`max_bytes_in_set`,
///   throw — code 191) set **only** by `traces::exec`'s metrics query
///   settings; no other path sets a set limit, and no other code maps
///   code 191.
/// - [`TooBroadReason::TraceGeneratorMemory`] — issue #57 re-audit: the
///   traces phase-1 candidate-generator's memory ceiling
///   (`max_memory_usage` + `max_bytes_before_external_group_by = 0`,
///   throw — code 241) set **only** on generator reads; no other path
///   sets a memory limit, and no other code maps code 241.
/// - [`TooBroadReason::QueryTextBytes`] — issue #35: the FINAL rendered
///   SQL text (after placeholder-doubling) reached [`crate::querytext::MAX_QUERY_TEXT_BYTES`]
///   — a Rust-side pre-dispatch admission check
///   ([`crate::querytext::ensure_query_text_fits`]), never a ClickHouse
///   error code (the raised `max_query_size` setting means ClickHouse's own
///   parse-buffer rejection should never be reached in practice).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TooBroadReason {
    /// A ClickHouse-side or engine-side **byte** budget was exceeded. On
    /// the LogQL path this is the `max_bytes_to_read` setting (set from
    /// `reader.logql_scan_budget_bytes`) — server code 307
    /// (`TOO_MANY_BYTES`). On the traces search path (issue #57) the same
    /// reason carries every byte-budget breach: the per-query
    /// `max_bytes_to_read`/`max_result_bytes` throw settings (codes 307 /
    /// 396) and the engine's request-scoped retention counter
    /// (`traces::exec::HYDRATION_BYTE_BUDGET`). `estimate` is the
    /// pre-flight selectivity-probe estimate when one was computed
    /// (best-effort early-abort only; the budget itself is the
    /// authoritative backstop).
    ScanBudgetBytes {
        budget_bytes: u64,
        estimate: Option<u64>,
    },
    /// Stage 1 resolved more fingerprints than [`crate::logql::params::DEFAULT_MAX_STREAMS`]
    /// (or a caller-supplied cap) — a Rust-side structural limit, never a
    /// ClickHouse row cap. `max_rows_to_read` is never set on **LogQL**
    /// read paths (the traces scan budget sets it deliberately on its
    /// generator queries, where code 158 maps to
    /// [`TooBroadReason::TraceScanBudgetRows`]), so on the LogQL path
    /// ClickHouse code 158 `TOO_MANY_ROWS` can never masquerade as this.
    StreamCap { count: usize, cap: usize },
    /// The traces search scan budget (`reader.traceql_scan_budget_rows`,
    /// applied as `max_rows_to_read` + `read_overflow_mode='throw'` to
    /// every candidate-generator/hydration query — issue #57 plan v4
    /// delta 2) was exceeded — server code 158 (`TOO_MANY_ROWS`). Set
    /// **only** by `traces::exec`'s own error mapper; LogQL's
    /// `map_read_error` never produces it.
    TraceScanBudgetRows { budget_rows: u64 },
    /// A TraceQL metrics attribute-filter semi-join's IN-set exceeded its
    /// budget (`max_rows_in_set`/`max_bytes_in_set` +
    /// `set_overflow_mode='throw'` — server code 191, issue #59 plan v2
    /// delta 3 as amended). Set **only** by the trace-metrics error
    /// mapper (`traces::exec::map_trace_metrics_error`); never conflated
    /// with the byte scan budget or the trace row budget.
    TraceMetricsSetRows { max_set_rows: u64 },
    /// Issue #182: a TraceQL metrics `by(...)` query resolved more distinct
    /// output series than `reader.traceql_max_series`. A Rust-side
    /// structural limit — the distinct-by-key `GROUP BY <by-keys> LIMIT
    /// cap+1` probe returned `cap+1` before the main query ran — never
    /// from a ClickHouse error code. `Trace…`-prefixed to sit unconflated
    /// beside the LogQL `MetricSeries` cap (a different construct).
    /// Complete-or-error: a breach is a static reject, never a silent
    /// subset. Operator scale tuning routes to issue #25.
    TraceMetricsSeriesCap { count: u64, cap: u64 },
    /// Issue #185: a TraceQL SEARCH `| by(...)` spanset-grouping stage
    /// resolved more distinct groups than `reader.traceql_max_series` — the
    /// SAME cap and the SAME distinct-by-key `GROUP BY <keys> LIMIT cap+1`
    /// pre-flight probe as the metric `by(...)` cap
    /// ([`TooBroadReason::TraceMetricsSeriesCap`]), applied before the main
    /// search runs. A breach is a static `422 query_too_broad`, never a
    /// silent subset. Operator scale tuning routes to issue #25.
    TraceSearchSeriesCap { count: u64, cap: u64 },
    /// Issue #57 re-audit (sub-problem B): the traces phase-1 candidate-
    /// generator read's memory ceiling (`max_memory_usage` +
    /// `max_bytes_before_external_group_by = 0`, throw — server code 241
    /// `MEMORY_LIMIT_EXCEEDED`) was exceeded — a dense common-value
    /// prefix's `GROUP BY trace_id` aggregation state grew past the
    /// budget. Set **only** by `traces::exec`'s generator error mapper
    /// (`map_trace_generator_error`); never conflated with the row/byte
    /// scan budgets or the trace-metrics set budget.
    TraceGeneratorMemory { budget_bytes: u64 },
    /// Issue #85 (M6-08c): a name-less/regex-`__name__` PromQL selector
    /// matched more metric names than the configured fan-out cap
    /// (`reader.promql_max_metric_fanout`, default 1000 — the adjudicated
    /// value). A Rust-side structural limit produced **only** by
    /// `metrics::exec`'s multi-metric resolution — never from a
    /// ClickHouse error code. Operator-scale tuning routes to issue #25.
    MetricFanout { matched: usize, cap: u64 },
    /// Issue M6-10 (review round 1): a client-aggregated LogQL metric
    /// query's `(end - start) / step` bucket grid exceeded
    /// [`crate::logql::exec::MAX_CLIENT_AGG_BUCKETS`] — rejected BEFORE
    /// any grid/accumulator materialization (an `absent_over_time` over
    /// a huge range with a tiny step must never allocate an
    /// attacker-sized grid outside the scan budget). A Rust-side
    /// structural limit, never from a ClickHouse error code.
    MetricBuckets { buckets: u64, cap: u64 },
    /// Issue M6-10 (review round 1): a `quantile_over_time` evaluation
    /// retained more exact sample values than
    /// [`crate::logql::exec::MAX_QUANTILE_VALUES`] across its buckets —
    /// the one client-side reducer whose state grows with surviving
    /// rows rather than with `buckets × series`. Complete-or-error: the
    /// query aborts with this named error, never an OOM and never a
    /// silently approximated quantile.
    QuantileValues { count: u64, cap: u64 },
    /// Issue #73 (retroactive re-review): a client-aggregated LogQL
    /// metric query resolved more distinct output series (final label
    /// sets on the fan-out/label-mutating path, or fingerprints on the
    /// non-mutating path) than
    /// [`crate::logql::exec::MAX_CLIENT_AGG_SERIES`] — rejected DURING
    /// aggregation before the (cap+1)-th group is materialized, so the
    /// group axis of reducer state (`groups x buckets`) stays bounded.
    /// Complete-or-error, never a truncated result. A Rust-side
    /// structural limit, never from a ClickHouse error code.
    MetricSeries { cap: u64 },
    /// Issue #89 (retroactive re-review): a regex/negated-`__name__`
    /// PromQL selector's multi-metric resolution *examined* more resident
    /// cache entries (metric names plus candidate fingerprints) than
    /// `reader.promql_max_cache_scan` before it could finish — an
    /// independent enumeration bound, distinct from [`Self::MetricFanout`]
    /// (which counts only matched names) and `OverCardinality` (which
    /// counts only matched series): a selector whose matchers yield few or
    /// no matches can still examine the whole resident cache. Produced
    /// **only** by `metrics::exec`'s multi-metric resolution, on both the
    /// query and discovery paths — the discovery path never routes this to
    /// the degraded-cache probe fallback (a warm cache that reaches this
    /// bound is not degraded, it is genuinely too broad). A Rust-side
    /// structural limit, never from a ClickHouse error code.
    CacheScan { cap: u64 },
    /// Issue #82 (retroactive re-review): a PromQL `info()` node's
    /// synthetic `*_info` metadata-family selector (`SelectorSpec::
    /// info_family`) resolved more series than
    /// `reader.promql_max_info_series` — a pathological-cardinality
    /// backstop, enforced BEFORE any sample fetch is issued (the warm
    /// label-cache path caps `pairs.len()` before building chunk SQL; the
    /// degraded/regex paths bound the series-selection query itself with
    /// `LIMIT cap+1`, mirroring the `MetricFanout` probe shape). Produced
    /// **only** by `metrics::exec`'s info-family resolution, never from a
    /// ClickHouse error code. Identifying-label VALUE narrowing of the
    /// fetch (closing the gap this cap merely backstops) routes to #25.
    InfoCardinality { matched: usize, cap: u64 },
    /// Issue #35: the rendered SQL text (after placeholder-doubling) for a
    /// read-path query reached or exceeded
    /// [`crate::querytext::MAX_QUERY_TEXT_BYTES`] — rejected pre-dispatch by
    /// [`crate::querytext::ensure_query_text_fits`], never by a ClickHouse
    /// server error code.
    QueryTextBytes { rendered_bytes: u64, cap: u64 },
    /// Issue #138: the per-query fetched-sample budget
    /// (`reader.promql_max_samples`) was exceeded while draining
    /// `metric_samples`/`metric_hist_samples` rows. Rust-side, per-row,
    /// pre-materialization (the drain aborts on the first over-cap row);
    /// never produced from a ClickHouse error code. Produced **only** by
    /// `metrics::exec`'s `SampleBudget` on the six sample-fetch dispatches
    /// — hydration/probe/discovery fetches never charge it.
    MetricSamples { cap: u64 },
}

impl fmt::Display for TooBroadReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TooBroadReason::ScanBudgetBytes {
                budget_bytes,
                estimate,
            } => match estimate {
                Some(est) => write!(
                    f,
                    "scan budget of {budget_bytes} bytes exceeded (estimate: {est} bytes)"
                ),
                None => write!(f, "scan budget of {budget_bytes} bytes exceeded"),
            },
            TooBroadReason::StreamCap { count, cap } => {
                write!(
                    f,
                    "resolved {count} streams, exceeding the {cap}-stream cap"
                )
            }
            TooBroadReason::TraceScanBudgetRows { budget_rows } => {
                write!(f, "trace scan budget of {budget_rows} rows exceeded")
            }
            TooBroadReason::TraceMetricsSetRows { max_set_rows } => {
                write!(
                    f,
                    "trace metrics attribute-set budget of {max_set_rows} rows exceeded"
                )
            }
            TooBroadReason::TraceMetricsSeriesCap { count, cap } => {
                write!(
                    f,
                    "trace metrics by() resolved at least {count} series, exceeding the \
                     {cap}-series cap"
                )
            }
            TooBroadReason::TraceSearchSeriesCap { count, cap } => {
                write!(
                    f,
                    "trace search by() resolved at least {count} groups, exceeding the \
                     {cap}-series cap"
                )
            }
            TooBroadReason::TraceGeneratorMemory { budget_bytes } => {
                write!(
                    f,
                    "trace search generator memory budget of {budget_bytes} bytes exceeded"
                )
            }
            TooBroadReason::MetricBuckets { buckets, cap } => {
                write!(
                    f,
                    "range/step resolves {buckets} evaluation buckets, exceeding the \
                     {cap}-bucket cap — use a larger step or a narrower window"
                )
            }
            TooBroadReason::QuantileValues { count, cap } => {
                write!(
                    f,
                    "quantile_over_time retained {count} sample values, exceeding the \
                     {cap}-value cap — narrow the window or filter the pipeline"
                )
            }
            // Lower-bound wording (code review round 1, finding 2):
            // resolution bails at the breach point, so `matched` is a
            // lower bound, never the final count — the message must not
            // imply an exact tally.
            TooBroadReason::MetricFanout { matched: _, cap } => {
                write!(
                    f,
                    "name-less selector matched more than {cap} metric names, exceeding the \
                     fan-out cap (reader.promql_max_metric_fanout)"
                )
            }
            // Lower-bound wording, like `MetricFanout`: the guard bails at
            // the breach point (the cap+1-th group), so this is not an
            // exact final tally.
            TooBroadReason::MetricSeries { cap } => {
                write!(
                    f,
                    "resolved more than {cap} series — narrow the pipeline (fewer parsed/\
                     formatted labels), use a coarser grouping, or a narrower window"
                )
            }
            // Lower-bound wording, like `MetricFanout`/`MetricSeries`: the
            // walk bails the instant it would cross the budget, so
            // `examined` is never presented here as an exact final tally.
            TooBroadReason::CacheScan { cap } => {
                write!(
                    f,
                    "regex/negated-name selector examined more than {cap} cache entries, \
                     exceeding the scan budget (reader.promql_max_cache_scan) — narrow the \
                     __name__ matcher or use a metric-scoped selector"
                )
            }
            // Lower-bound wording, like `MetricFanout`: the resolution
            // bails at the breach point (before or at the `cap+1`-th
            // row), so `matched` is never presented as an exact final
            // tally.
            TooBroadReason::InfoCardinality { matched: _, cap } => {
                write!(
                    f,
                    "info() metadata family matched more than {cap} series, exceeding the \
                     cardinality cap (reader.promql_max_info_series)"
                )
            }
            TooBroadReason::QueryTextBytes {
                rendered_bytes,
                cap,
            } => {
                write!(
                    f,
                    "rendered query text is {rendered_bytes} bytes, exceeding the {cap}-byte \
                     query-text cap — narrow the selector (fewer streams/metric names) or \
                     shorten the query"
                )
            }
            // Lower-bound wording, like `MetricFanout`: the drain aborts
            // on the first over-cap row, so no exact final tally exists.
            TooBroadReason::MetricSamples { cap } => {
                write!(
                    f,
                    "query would fetch more than {cap} samples, exceeding the evaluation \
                     sample budget (reader.promql_max_samples) — narrow the selector or \
                     the time range"
                )
            }
        }
    }
}

/// Errors from planning or executing a LogQL **or PromQL** query — despite
/// living in the `logql` module (this crate's original, pre-#31 error
/// type), `ReadError` is shared crate-wide; issue #31's
/// `metrics::exec::MetricsEngine` reuses it rather than minting a second
/// top-level error type, via the `Promql` variant below.
#[derive(Debug, Error)]
pub enum ReadError {
    /// Surfaced by callers (e.g. #13) that parse a raw query string before
    /// handing the AST to this crate; `plan`/`LogQlEngine` themselves never
    /// parse, so this variant is never constructed inside `pulsus-read`.
    #[error("logql parse error: {0}")]
    Parse(#[from] LogQlError),

    /// Issue #31: any `pulsus_promql::plan`/`evaluate` failure — parse,
    /// unsupported-construct, bad vector matching, or a
    /// `histogram_quantile` bucket error. The inner `PromqlError`'s own
    /// `Display` (in particular `PromqlError::Parse`, which carries the
    /// vendored parser's upstream error text verbatim) is preserved
    /// unmodified inside this variant's message — only this outer "promql:
    /// " prefix is added.
    #[error("promql: {0}")]
    Promql(#[from] pulsus_promql::PromqlError),

    /// The selector has no positive (`=`/`=~`) matcher — Loki's
    /// "match-everything" rejection (task-manager resolution #2: the coarse
    /// "≥1 positive matcher" rule; precise regex-matches-empty detection is
    /// deferred to M6).
    #[error("selector matches everything: at least one equality or regexp matcher is required")]
    EmptyMatcherSet,

    /// Two `Eq` matchers on the same label key with different literal
    /// values — provably empty (a stream's `(key, val)` pair cannot equal
    /// two different values at once).
    #[error("matchers are contradictory: the selector can never match a stream")]
    ContradictoryMatchers,

    /// The query was rejected as too broad. See [`TooBroadReason`].
    #[error("query too broad: {0}")]
    QueryTooBroad(TooBroadReason),

    /// Issue #85 (M6-08c): a name-less/regex-`__name__` PromQL selector
    /// resolves exclusively through the warm in-process label cache (the
    /// name-keyed `by_metric` map) — no metric-scoped SQL fallback shape
    /// exists for "every metric name matching these matchers". When the
    /// cache is not authoritative for the query's window (cold, stale,
    /// out-of-window, over-cardinality, or an in-process-unevaluable
    /// regex), the selector fails with this named error rather than
    /// falling back to an unbounded scan. `reason` is the
    /// [`crate::metrics::FallbackReason`]'s debug rendering.
    #[error(
        "name-less selector cannot be resolved: {reason} — a selector without a single \
         concrete metric name requires the warm in-process label cache (no metric-scoped \
         SQL fallback exists for it)"
    )]
    NamelessSelectorUnresolvable { reason: String },

    /// Issue M6-09: a metric query whose log range carries a pipeline
    /// stage beyond plain line filters (a parser, label filter,
    /// `line_format`, `label_format`, or `unwrap`). Executing the pipeline
    /// inside a range aggregation is the M6-10 seam — rejected by name
    /// here rather than silently counting unfiltered lines.
    #[error(
        "`{construct}` is not yet supported inside a metric query: pipeline execution within \
         range aggregations lands with the M6-10 metric-pipeline milestone (features.md §2)"
    )]
    PipelineUnsupportedInMetric { construct: String },

    /// Issue M6-09: a log pipeline that parses but cannot be compiled or
    /// planned — a bad `regexp`/`pattern` expression, an unsupported
    /// template function, an invalid `json` extraction path, an
    /// uninterpretable numeric literal, or `unwrap` outside a range
    /// aggregation. Issue M6-10 adds the metric-language semantic
    /// rejections in the same family (unwrap-required/-forbidden ops,
    /// bad aggregation parameters, set operations against scalars).
    /// Always a 400-class client error.
    #[error("invalid pipeline: {reason}")]
    PipelineInvalid { reason: String },

    /// Issue M6-10 (adjudication #1): a metric-query line retained a
    /// nonempty `__error__` after the FULL pipeline — no downstream
    /// `__error__` filter consumed it — so the aggregation would be
    /// silently wrong. The message is the live-probed oracle template
    /// verbatim (`pipeline error: '<class>' for series: ...`, HTTP 400
    /// on the oracle; mapped 400 by the API layers). `series` is the
    /// failed line's sorted final label set rendered `{k="v", ...}`.
    #[error(
        "pipeline error: '{error_type}' for series: '{series}'.\n\
         Use a label filter to intentionally skip this error. (e.g | __error__!=\"{error_type}\").\n\
         To skip all potential errors you can match empty errors.(e.g __error__=\"\")\n\
         The label filter can also be specified after unwrap. (e.g | unwrap latency | __error__=\"\" )"
    )]
    MetricPipelineError { error_type: String, series: String },

    /// A `Range` metric query's `step_ns` was zero. `0.is_multiple_of(_)`
    /// is trivially `true`, which would otherwise let the routing decision
    /// pick rollup and render `intDiv(bucket_ns, 0)` — undefined in
    /// ClickHouse; the raw fallback's own `intDiv(timestamp_ns, 0)`
    /// bucketing is equally invalid. Rejected in [`super::plan::plan`]
    /// before either SQL builder is reached — defense in depth ahead of
    /// whatever request-level `step > 0` validation #13 adds (task-manager
    /// resolution #4 on issue #12).
    #[error("range query step_ns must be greater than zero")]
    InvalidStep,

    /// M7-A5a: the metrics dual-read decoded a `metric_hist_samples` row
    /// whose value columns cannot rebuild a [`NativeHistogram`]
    /// (`from_columns` structural failure — parallel span arrays of
    /// unequal length). Structurally unreachable for writer-produced rows
    /// (the A4 ingest seam validated before storing), so this is a
    /// data-integrity defect surfaced defensively, never a client error.
    /// `pulsus_model::NativeHistogram` is referenced only in this doc.
    #[error("histogram decode: {0}")]
    HistogramDecode(#[from] pulsus_model::HistogramError),

    /// M7-A5a: a well-formed, executed query whose evaluated result is a
    /// native-histogram vector/matrix element — a result type the current
    /// (A5a) response encoder declines to render. The native-histogram
    /// **function set and JSON encoder** land in **M7-A5b**; until then no
    /// code path emits `0.0` for a histogram value — this is returned
    /// instead. Maps like [`ReadError::QueryTooBroad`]: 422 `execution`
    /// (the engine declines to render the result), never 400/5xx.
    #[error(
        "native-histogram query results are not yet renderable: the histogram function set and \
         JSON encoding land with M7-A5b — until then a bare native-histogram selector result \
         cannot be returned over the query API"
    )]
    HistogramResultUnsupported,

    /// An unclassified/passthrough ClickHouse error (network, decode,
    /// server exception not mapped to [`ReadError::QueryTooBroad`]).
    #[error("clickhouse: {0}")]
    Clickhouse(#[from] ChError),
}

impl From<super::pipeline::PipelineError> for ReadError {
    fn from(e: super::pipeline::PipelineError) -> Self {
        ReadError::PipelineInvalid {
            reason: e.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_budget_bytes_display_names_the_budget() {
        let reason = TooBroadReason::ScanBudgetBytes {
            budget_bytes: 1024,
            estimate: None,
        };
        assert!(reason.to_string().contains("1024"));
    }

    /// Issue #85 (M6-08c), review round 1 finding 2: the fan-out breach
    /// message uses LOWER-BOUND wording ("more than {cap}") — resolution
    /// bails at cap+1, so an exact matched count would be a lie.
    #[test]
    fn metric_fanout_display_uses_lower_bound_wording_and_names_the_knob() {
        let reason = TooBroadReason::MetricFanout {
            matched: 1_001,
            cap: 1_000,
        };
        let msg = ReadError::QueryTooBroad(reason).to_string();
        assert!(msg.contains("query too broad"), "{msg}");
        assert!(msg.contains("matched more than 1000 metric names"), "{msg}");
        assert!(
            !msg.contains("1001"),
            "must not present the breach point as an exact count: {msg}"
        );
        assert!(msg.contains("promql_max_metric_fanout"), "{msg}");
    }

    /// Issue #85 (M6-08c): the degraded-cache name-less-selector failure
    /// is named and explains itself.
    #[test]
    fn nameless_selector_unresolvable_display_names_the_reason() {
        let err = ReadError::NamelessSelectorUnresolvable {
            reason: "ColdCache".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("name-less selector"), "{msg}");
        assert!(msg.contains("ColdCache"), "{msg}");
    }

    #[test]
    fn scan_budget_bytes_display_names_the_estimate_when_present() {
        let reason = TooBroadReason::ScanBudgetBytes {
            budget_bytes: 1024,
            estimate: Some(4096),
        };
        let msg = reason.to_string();
        assert!(msg.contains("1024"));
        assert!(msg.contains("4096"));
    }

    #[test]
    fn stream_cap_display_names_count_and_cap() {
        let reason = TooBroadReason::StreamCap {
            count: 150_000,
            cap: 100_000,
        };
        let msg = reason.to_string();
        assert!(msg.contains("150000"));
        assert!(msg.contains("100000"));
    }

    /// Issue M6-10 review round 1: the two client-aggregation breadth
    /// guards are named, actionable too-broad reasons.
    #[test]
    fn metric_buckets_display_names_count_and_cap() {
        let msg = TooBroadReason::MetricBuckets {
            buckets: 3_600_000,
            cap: 11_000,
        }
        .to_string();
        assert!(msg.contains("3600000"), "{msg}");
        assert!(msg.contains("11000-bucket cap"), "{msg}");
    }

    #[test]
    fn quantile_values_display_names_count_and_cap() {
        let msg = TooBroadReason::QuantileValues {
            count: 4_000_001,
            cap: 4_000_000,
        }
        .to_string();
        assert!(msg.contains("4000001"), "{msg}");
        assert!(msg.contains("4000000-value cap"), "{msg}");
    }

    /// Issue #73 (retroactive re-review): the derived-series cap uses
    /// LOWER-BOUND wording ("more than {cap}"), like `MetricFanout` — the
    /// guard bails at the breach point, never an exact final tally.
    #[test]
    fn metric_series_display_names_the_cap_with_lower_bound_wording() {
        let msg = ReadError::QueryTooBroad(TooBroadReason::MetricSeries { cap: 500 }).to_string();
        assert!(msg.contains("query too broad"), "{msg}");
        assert!(msg.contains("resolved more than 500 series"), "{msg}");
    }

    /// Issue #89 (retroactive re-review): the cache-scan budget uses
    /// LOWER-BOUND wording, names the knob, and maps `QueryTooBroad` to a
    /// 422-class message — the message-discrimination string live tests
    /// pin against `MetricFanout`/`NamelessSelectorUnresolvable`.
    #[test]
    fn cache_scan_display_names_the_cap_and_the_knob() {
        let msg = ReadError::QueryTooBroad(TooBroadReason::CacheScan { cap: 200_000 }).to_string();
        assert!(msg.contains("query too broad"), "{msg}");
        assert!(
            msg.contains("examined more than 200000 cache entries"),
            "{msg}"
        );
        assert!(
            msg.contains("scan budget (reader.promql_max_cache_scan)"),
            "{msg}"
        );
    }

    #[test]
    fn trace_metrics_set_rows_display_names_the_set_budget() {
        let reason = TooBroadReason::TraceMetricsSetRows {
            max_set_rows: 1_000_000,
        };
        let msg = reason.to_string();
        assert!(msg.contains("1000000"));
        assert!(msg.contains("attribute-set"));
    }

    /// Issue #57 re-audit: the generator memory reason names the budget.
    #[test]
    fn trace_generator_memory_display_names_the_memory_budget() {
        let reason = TooBroadReason::TraceGeneratorMemory {
            budget_bytes: 1_048_576,
        };
        let msg = reason.to_string();
        assert!(msg.contains("1048576"), "{msg}");
        assert!(msg.contains("generator memory"), "{msg}");
    }

    /// Issue #138 AC1: the sample-budget breach message names the knob
    /// (`reader.promql_max_samples`) and the cap, with lower-bound
    /// wording — the drain aborts on the first over-cap row, so no exact
    /// final tally exists.
    #[test]
    fn metric_samples_display_names_the_cap_and_the_knob() {
        let msg =
            ReadError::QueryTooBroad(TooBroadReason::MetricSamples { cap: 50_000_000 }).to_string();
        assert!(msg.contains("query too broad"), "{msg}");
        assert!(msg.contains("more than 50000000 samples"), "{msg}");
        assert!(
            msg.contains("sample budget (reader.promql_max_samples)"),
            "{msg}"
        );
    }

    /// Issue #35: the query-text guard's message names both the rendered
    /// size and the cap, and states the remedy.
    #[test]
    fn query_text_bytes_display_names_both_numbers_and_the_remedy() {
        let reason = TooBroadReason::QueryTextBytes {
            rendered_bytes: 9_000_000,
            cap: 8_388_608,
        };
        let msg = reason.to_string();
        assert!(msg.contains("9000000"), "{msg}");
        assert!(msg.contains("8388608"), "{msg}");
        assert!(msg.contains("narrow the selector"), "{msg}");
    }

    #[test]
    fn query_too_broad_display_wraps_the_reason() {
        let err = ReadError::QueryTooBroad(TooBroadReason::StreamCap { count: 1, cap: 1 });
        assert!(err.to_string().contains("query too broad"));
    }

    #[test]
    fn empty_matcher_set_message_explains_the_rule() {
        assert!(
            ReadError::EmptyMatcherSet
                .to_string()
                .contains("at least one equality or regexp matcher")
        );
    }

    #[test]
    fn contradictory_matchers_message_is_descriptive() {
        assert!(
            ReadError::ContradictoryMatchers
                .to_string()
                .contains("contradictory")
        );
    }

    #[test]
    fn invalid_step_message_names_the_zero_step_rule() {
        assert!(ReadError::InvalidStep.to_string().contains("step_ns"));
    }

    #[test]
    fn promql_display_wraps_the_inner_error_verbatim() {
        let inner = pulsus_promql::PromqlError::Unsupported {
            construct: "the @ modifier".to_string(),
        };
        let err = ReadError::Promql(inner);
        assert!(err.to_string().starts_with("promql: "));
        assert!(err.to_string().contains("the @ modifier"));
    }

    #[test]
    fn promql_parse_error_text_survives_unmodified_inside_read_error() {
        let inner = pulsus_promql::PromqlError::Parse("unexpected character: 'x'".to_string());
        let err = ReadError::from(inner);
        assert_eq!(err.to_string(), "promql: unexpected character: 'x'");
    }
}
