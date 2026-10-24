//! `TraceEngine` — executes the §4.2 trace-by-ID point read, the TraceQL
//! search, metrics, tag and service-graph reads against ClickHouse via
//! `ChClient`. Deliberately OTLP-agnostic (see [`super`]'s module doc):
//! payload decoding/dedup/assembly is `pulsus-server`'s job; search never
//! reads payloads at all.
//!
//! **Search** ([`TraceEngine::search_routed`]) runs the search statement
//! (`spans::search`, `docs/TraceQL/sql-schema.md` §5.2–§5.3): newest slice
//! first (`exec/newest_first.rs`), then the whole window, and decodes the
//! rows it returns. The previous two-phase engine this module once drove
//! (generators, hydration batches, a result heap) was deleted by #602 part
//! 2c-2b-2.
//!
//! - **Memory contract (issue #57 re-audit):** Layer 1 — every query
//!   carries `max_bytes_to_read`/`read_overflow_mode='throw'`,
//!   `max_result_bytes`/`result_overflow_mode='throw'`, the row scan
//!   budget, and [`TRACE_SEARCH_MAX_BLOCK_ROWS`] (`max_block_size`);
//!   breach → 422. Issue #398 adds `max_memory_usage` +
//!   `max_bytes_before_external_group_by=0` to EVERY trace read at all
//!   three of this module's settings origins — [`search_settings`], the
//!   independent [`catalog_settings`], and the §4.2 point read
//!   ([`fetch_settings`]) — from `config.read_max_memory_bytes`; breach →
//!   code 241 → [`TooBroadReason::TraceReadMemory`] → 422. Layer 2 — a
//!   single request-scoped byte counter ([`HYDRATION_BYTE_BUDGET`])
//!   charges every retained byte of a search's decode; breach → 422.
//!
//! ## Allocation-charge audit (code review round 3) — engine side
//!
//! Invariant: **no retained or intermediate collection exists
//! uncharged**. The sites this file still holds, and each one's charge,
//! are the allowlist of `tests/traces_alloc_audit.rs`, which fails on any
//! new collection-allocation token in this file and in `values.rs` until
//! it is allowlisted with its charge site documented. The trace FETCH's
//! one row ([`TraceEngine::fetch_by_id`], issue #587) has no allocation
//! site of its own: each of its statements returns exactly ONE row, and
//! what bounds it is [`fetch_settings`]' `max_memory_usage`, applied
//! server-side.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChError, ChRow, QuerySettings};

use super::metrics_plan::{MetricsCtx, TraceMetricsPlan};
use super::metrics_result::{
    MetricExemplar, MetricLabel, MetricLabelValue, TraceMetricSeries, TraceMetricsResult,
};
use super::spans::fetch as span_fetch;
use super::spans::graph::{GraphWindow, SERVICE_GRAPH_MAX_EDGES};
use super::spans::rows::{
    FallbackFetchRow, FetchRoute, FetchWindow, FetchedTrace, IndexedFetchRow, SearchGroupTuple,
    SearchGroupedRow, SearchProjected, SearchSpanTuple, SearchTraceRow, WideFetchRow,
};

use super::rows::{GraphEdgeRow, GraphTargetRow, SpanNameRow, TagNameRow, TagValueRow};
use super::spans::predicate::PredicateCtx;
use super::spans::tags::{
    TAG_STORE_READ_ROWS, TagNarrowing, TagShape, ValueScope, cut_rows, name_values_sql,
    narrowed_values_sql, narrowing_from_query, table_reads,
};
use super::values::{GroupCardinalityCounter, SpanSetGroup, SpanSummary};
use super::window_sql::WindowSql;
use crate::logql::error::{ReadError, TooBroadReason};
use crate::logql::explain::PlanExplain;

/// Response cap for the §4.3 tag-names read (`GET /api/traces/v1/tags`,
/// issue #58) — a documented constant (docs/api.md §4.3; promoted to
/// config only on evidence). The generated SQL carries
/// `LIMIT TAG_NAMES_MAX + 1`: the extra row is the truncation probe (the
/// search path's `gen_cap + 1` convention) — the engine returns at most
/// `TAG_NAMES_MAX` rows plus a non-silent `truncated` flag.
pub const TAG_NAMES_MAX: usize = 10_000;

/// Response cap for the §4.3 tag-values read
/// (`GET /api/traces/v1/tag/{tag}/values`) — same probe convention as
/// [`TAG_NAMES_MAX`].
pub const TAG_VALUES_MAX: usize = 1_000;

/// Layer 2 — the single request-scoped retention budget: every byte a
/// search's statement rows and decoded response retain is charged against
/// this counter; a breach is a `422 query_too_broad`, never an OOM. Its
/// load-bearing role is what no per-query server setting can see: the
/// rows and response held across the slices of one request.
pub const HYDRATION_BYTE_BUDGET: usize = 256 * 1024 * 1024;

/// Layer 1 read-side byte budget (`max_bytes_to_read`, throw) applied to
/// every search query — the logs-budget-analogous default
/// (`reader.logql_scan_budget_bytes`' 50 GiB).
pub const TRACE_READ_BYTES_BUDGET: u64 = 50 * 1024 * 1024 * 1024;

/// Layer 1 result-side byte ceiling (`max_result_bytes`, throw) applied
/// to every search query — bounds any single result set independent of
/// string payload lengths, firing server-side before the driver
/// materializes anything; the [`HYDRATION_BYTE_BUDGET`] retention counter
/// remains the binding bound on what one request retains.
pub const TRACE_MAX_RESULT_BYTES: u64 = 64 * 1024 * 1024;

/// Layer 1 block-row cap (`max_block_size`) applied to every search
/// query (issue #57 re-audit, sub-problem A): bounds the row width of
/// any single transiently-buffered result block, so the driver's
/// documented one-block residual is a hard product with
/// [`crate::traces::sql_text::TRACE_STR_COL_CAP`] rather than
/// a-priori row-unbounded — see the module doc's memory-contract
/// paragraph.
pub const TRACE_SEARCH_MAX_BLOCK_ROWS: u64 = 4096;

/// ClickHouse overflow codes the trace search budget settings can raise.
const CODE_TOO_MANY_ROWS: i32 = 158;
const CODE_TOO_MANY_BYTES: i32 = 307;
const CODE_TOO_MANY_ROWS_OR_BYTES: i32 = 396;
/// `MEMORY_LIMIT_EXCEEDED` — raised by the surface-wide memory ceiling
/// every trace read carries (issue #398, `max_memory_usage` and
/// `max_bytes_before_external_group_by = 0`, throw-not-spill); maps to
/// [`TooBroadReason::TraceReadMemory`] in [`map_trace_read_error`].
const CODE_MEMORY_LIMIT_EXCEEDED: i32 = 241;
/// `SET_SIZE_LIMIT_EXCEEDED` — raised by an `IN (SELECT …)` set built
/// under [`TRACE_METRICS_MAX_SET_ROWS`]/[`TRACE_METRICS_MAX_SET_BYTES`]
/// with `set_overflow_mode='throw'`. No other trace or LogQL query sets a
/// set limit, so this code maps exclusively on the metrics path.
///
/// **Issue #559 removed the surface this used to name.** Until then the
/// metrics FILTER lowered an attribute condition to
/// `(trace_id, span_id) [NOT] IN (SELECT …)` over the attribute index, and
/// this code was that set overflowing; measured, 524,288 matching spans
/// answered and 524,289 did not. The filter is now a predicate over the
/// span row's own arrays, so a metrics query with an attribute condition
/// cannot raise this any more — its binding limit is
/// `max_rows_to_read = reader.traceql_scan_budget_rows`, code 158.
///
/// The set limits are inert for the metrics statement: it builds sets only
/// for the seven fields (#594 part 5) and sends `max_rows_in_set = 0` and
/// `max_bytes_in_set = 0` ([`statement_settings`]). The previous planner's
/// reads they bound went with the previous metrics SQL (#602 part 3a).
/// Constant, mapping, 422 unchanged.
const CODE_SET_SIZE_LIMIT_EXCEEDED: i32 = 191;

/// The IN-set row budget (`max_rows_in_set`, throw) carried by every read
/// that takes [`metrics_settings`]: it bounds the materialized
/// `(trace_id, span_id)` set of an `IN (SELECT …)` subquery, so a window
/// matching more than this many rows is a `422 query_too_broad` rather
/// than an unbounded in-memory set. Documented constant
/// (docs/schemas.md §4.2; promoted to config only on evidence).
///
/// **Inert for the metrics statement**, which builds sets only for the
/// seven fields (#594 part 5) and lifts the limit ([`statement_settings`]);
/// the previous planner's reads it bound went with the previous metrics SQL
/// (#602 part 3a). The value does not move.
pub const TRACE_METRICS_MAX_SET_ROWS: u64 = 1_000_000;

/// The metrics IN-set byte budget (`max_bytes_in_set`, throw) — the byte
/// twin of [`TRACE_METRICS_MAX_SET_ROWS`], same scale as
/// [`TRACE_MAX_RESULT_BYTES`].
pub const TRACE_METRICS_MAX_SET_BYTES: u64 = 64 * 1024 * 1024;

/// Per-entry container-overhead envelope, charged on top of every
/// retained entry's `size_of`-based payload cost: covers hash-table
/// bucket/control bytes and slot padding (`hashbrown` ≈ 1 control byte +
/// slot rounding per entry at ≤ 7/8 load) and `Vec`/map capacity-doubling
/// slack (growth doubling retains at most one extra entry-width per live
/// entry). 64 bytes per entry is a stated conservative envelope over
/// both — the review-round invariant is that **no retained collection
/// grows without a corresponding live charge**, so every charge below is
/// `size_of::<entry>() + RETAINED_ENTRY_OVERHEAD (+ string payloads)`.
/// `pub`: the statement's decode and its tests charge with it
/// (`values.rs`, `spans/search.rs`).
pub const RETAINED_ENTRY_OVERHEAD: usize = 64;

/// A destination for streamed rows that can PRICE a row before accepting
/// it (issue #351 review 2).
///
/// Two methods rather than one closure because the ORDER is the whole
/// point: `cost` runs, the budget is charged, and only then does `accept`
/// take ownership — so a breach refuses the value instead of retaining
/// it. Expressing that as a trait puts the order in one place
/// ([`TraceEngine::stream_rows_charged`]) rather than in every caller.
trait ChargedRowSink<R> {
    /// The retained cost of `row` — everything that will still be live
    /// after `accept` returns.
    fn cost(&mut self, row: &R) -> usize;
    /// Takes ownership. Called only after the charge succeeded.
    fn accept(&mut self, row: R);
}

/// The closure-pair [`ChargedRowSink`]: a pricing function and a
/// retaining function, so a caller can keep using closures without the
/// two drifting apart into separate parameters.
struct FnRowSink<C, A> {
    cost: C,
    accept: A,
}

impl<R, C: FnMut(&R) -> usize, A: FnMut(R)> ChargedRowSink<R> for FnRowSink<C, A> {
    fn cost(&mut self, row: &R) -> usize {
        (self.cost)(row)
    }

    fn accept(&mut self, row: R) {
        (self.accept)(row)
    }
}

/// Owned table/budget configuration a [`TraceEngine`] reads against —
/// mirrors [`crate::logql::EngineConfig`]'s "owned `String`, no borrowed
/// lifetime on the engine itself" shape. Every trace read is on the span
/// design's tables: `spans`, `error_spans`, `traces` and `resources`.
#[derive(Debug, Clone)]
pub struct TraceReadConfig {
    /// `spans{_dist}` — the span table every search, fetch, metrics, tag
    /// and service-graph read uses (issue #587). The table co-shards on
    /// `cityHash64(trace_id)`; `_dist` when clustered, as every other read
    /// engine's config.
    pub spans_table: String,
    /// `error_spans{_dist}` — the error-span table (issue #601 part 4b):
    /// the search statement of a query
    /// [`super::spans::error_spans::admits`] reads it in place of
    /// [`Self::spans_table`]. Same `_dist` rule.
    pub error_spans_table: String,
    /// `traces{_dist}` — the per-trace table the indexed fetch reads its
    /// bucket set and its extent from (issue #587). Same `_dist` rule.
    pub traces_table: String,
    /// `resources` — the fetch's resource table (issue #587).
    ///
    /// **Never `_dist`-suffixed**: the table is `Replication::Global`, one
    /// cluster-wide replica set read from the local replica, the same
    /// carve-out the tag catalog has. A `_dist` wrapper here would fan a
    /// read out to every shard for rows every shard already holds.
    pub resources_table: String,
    /// `reader.traceql_scan_budget_rows` — `max_rows_to_read` (throw) on
    /// every search query; breach → 422 (code 158 →
    /// [`TooBroadReason::TraceScanBudgetRows`]).
    pub scan_budget_rows: u64,
    /// `reader.traceql_max_depth` (`PULSUS_TRACEQL_MAX_DEPTH`, issue #593
    /// part 2): the most parent links one climb of `>>` or `<<` follows.
    /// A span with a parent left to follow past it answers `422`
    /// ([`TooBroadReason::TraceStructuralDepth`]).
    pub max_depth: u32,
    /// `reader.traceql_max_series` (issue #182) — the metrics series cap;
    /// the statement throws past it → 422
    /// ([`TooBroadReason::TraceMetricsSeriesCap`]).
    pub max_series: u64,
    /// Issue #398: `reader.traceql_read_max_memory_bytes` — the
    /// `max_memory_usage` ceiling (throw-not-spill) carried by EVERY trace
    /// read, at all three of this module's settings origins:
    /// [`search_settings`], [`catalog_settings`], and the §4.2 point read
    /// ([`TraceEngine::fetch_by_id`], which sent a bare
    /// `QuerySettings::new()` and mapped errors around
    /// [`map_trace_read_error`] entirely before #398). Breach → server code
    /// 241 → [`TooBroadReason::TraceReadMemory`] → `422`.
    pub read_max_memory_bytes: u64,
    /// Clustered mode: inject the docs/schemas.md §7 clustered-reader
    /// settings on every search query (both phases are shard-local by
    /// the `cityHash64(trace_id)` co-sharding).
    pub distributed: bool,
    /// `PULSUS_SKIP_UNAVAILABLE_SHARDS` passthrough for the §7 settings.
    pub skip_unavailable_shards: bool,
}

/// A returned trace's root metadata, from the trace-wide per-trace read
/// of the search statement (`spans/search.rs`'s `TRACE_READ`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootSummary {
    pub service: String,
    pub name: String,
    pub start_ns: i64,
    pub duration_ns: i64,
}

/// One returned trace: root metadata + the trace-wide envelope + the
/// matched spanset summaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceSearchResult {
    pub trace_id: [u8; 16],
    /// The ROOT SPAN's own metadata. `service`/`name` are the reference's
    /// `rootServiceName`/`rootTraceName`; `start_ns`/`duration_ns` are the
    /// root span's own and are NOT what the trace level reports.
    pub root: RootSummary,
    /// The reference's `startTimeUnixNano` = `Spanset.StartTimeUnixNanos`
    /// = `traceStart` (`tempodb/encoding/vparquet4/schema.go:558` @
    /// v3.0.2), filled from the spanset at `pkg/traceql/engine.go:294`.
    pub trace_start_ns: i64,
    /// The reference's `durationMs` before the millisecond divide =
    /// `Spanset.DurationNanos` = `traceEnd - traceStart`
    /// (`tempodb/encoding/vparquet4/schema.go:560` @ v3.0.2), filled at
    /// `pkg/traceql/engine.go:295`.
    pub trace_duration_ns: i64,
    /// Total exactly-matched spans (pre-`spss` cap).
    pub matched: u32,
    /// `spss`-capped matched-span summaries, ascending `(start_ns, span_id)`.
    pub spans: Vec<SpanSummary>,
    /// The `by()`-regrouped spanSets (issue #193): `Some` iff a `by()`
    /// grouping stage is active and not collapsed by a trailing
    /// `coalesce()`. `None` keeps the flat single-spanSet response
    /// byte-identical (the default path). When `Some`, the encoder emits
    /// one spanSet per group (carrying typed `attributes`) and the flat
    /// `matched`/`spans` are not serialized.
    pub groups: Option<Vec<SpanSetGroup>>,
}

/// The search result: `traces` ordered by the public contract (max
/// matched-span `timestamp_ns` DESC, `trace_id` ASC — docs/api.md §4.2).
///
/// It carries no completeness flag: a search is answered whole or refused
/// (issue #602 part 3c), so the response's `metrics` block is always one
/// completed job of one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchOutput {
    pub traces: Vec<TraceSearchResult>,
    /// `traces.len()`. Not a wire field either (issue #464 retired the
    /// invented `metrics.returned`); it stays because the read-path bench
    /// reports it from this struct.
    pub returned: u32,
    /// The request's own `limit`, echoed back to the caller of this
    /// crate. **No longer serialized** — `metrics.limit` was invented and
    /// issue #464 removed it, since a caller already holds its own
    /// request parameter. The field stays because it is part of this
    /// crate's public result type and the search bench reads it.
    pub limit: u32,
}

/// [`TraceEngine::service_graph`]'s output (issue #173): the aggregated
/// service-graph edges, ordered `calls DESC, client ASC, server ASC`, at
/// most [`SERVICE_GRAPH_MAX_EDGES`] of them; `truncated` is the
/// non-silent cap indicator — `true` iff the `LIMIT cap + 1` probe row
/// appeared (the search path's convention).
#[derive(Debug, Clone, PartialEq)]
pub struct ServiceGraph {
    pub edges: Vec<GraphEdgeRow>,
    pub truncated: bool,
}

/// The cluster and database the clustered service graph reads (issue
/// #599): the first two arguments of the `Distributed(…)` definition of the
/// reader's `spans` routing table, so the graph reads the target the other
/// trace routes read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphTarget {
    pub cluster: String,
    pub database: String,
}

/// The [`GraphTarget`] a routing table's `system.tables.engine_full`
/// names: `Distributed('<cluster>', '<database>', …)`, both arguments
/// quoted, as the server writes them. `None` for anything else.
pub fn graph_target_from_engine(engine_full: &str) -> Option<GraphTarget> {
    let rest = engine_full.strip_prefix("Distributed(")?;
    let (cluster, rest) = quoted_argument(rest)?;
    let rest = rest.strip_prefix(", ")?;
    let (database, rest) = quoted_argument(rest)?;
    if !(rest.starts_with(',') || rest.starts_with(')')) {
        return None;
    }
    Some(GraphTarget { cluster, database })
}

/// The single-quoted literal `s` starts with, unescaped, and what follows
/// it; `None` when `s` does not start with one or it is empty.
fn quoted_argument(s: &str) -> Option<(String, &str)> {
    let body = s.strip_prefix('\'')?;
    let mut out = String::new();
    let mut chars = body.char_indices();
    while let Some((i, c)) = chars.next() {
        match c {
            '\\' => out.push(chars.next()?.1),
            '\'' => {
                if out.is_empty() {
                    return None;
                }
                return Some((out, &body[i + 1..]));
            }
            other => out.push(other),
        }
    }
    None
}

/// The process-wide [`GraphTarget`], read once (issue #599).
///
/// The server builds a [`TraceEngine`] per request, so the cache lives in
/// the server's shared state and is handed to each engine; cloning shares
/// it. A failed read leaves it empty, and the next request reads again.
#[derive(Debug, Clone, Default)]
pub struct GraphTargetCache(std::sync::Arc<tokio::sync::OnceCell<GraphTarget>>);

/// [`TraceEngine::list_tag_names`]'s output (issue #58): distinct
/// `(scope, key)` pairs in the catalog's own `(scope, key)` order, at
/// most [`TAG_NAMES_MAX`] of them; `truncated` is the non-silent cap
/// indicator — `true` iff the `LIMIT cap + 1` probe row appeared.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagNames {
    pub names: Vec<(String, String)>,
    pub truncated: bool,
}

/// One catalog tag value and the OTLP type it was stored with (issue
/// #476).
///
/// `val_type` is `string`, `int`, `float` or `bool`, never empty: the
/// writer stores the OTLP kind (`TraceJsonScalar::val_type`), and every
/// read over `spans`, `resources` and the bucket catalogues types each
/// value by a literal per JSON type. The renderer reports it verbatim and
/// derives nothing from the characters (issue #602 part 3c).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagValue {
    pub val: String,
    pub val_type: String,
}

/// The wire type every span name is reported with (issue #478).
///
/// A span name is a `String` column, so this is a fact about the schema
/// rather than a reading of the characters: the reference types the span
/// names `500`, `1.5s`, `true` and `-3` as `string` too, where a
/// text-classifying inference would call them `int`, `duration`, `bool`
/// and `int`.
pub const SPAN_NAME_VALUE_TYPE: &str = "string";

/// One span name as a [`TagValue`] — the ONE place the span-name type is
/// decided (issue #478).
///
/// A function rather than an inline struct literal because this is the
/// assertion criterion 5a is about: the renderer copies `val_type`
/// verbatim (issue #602 part 3c), so a read that left the type EMPTY
/// would put `"type":""` on the wire. The type is decided here, at the
/// engine boundary, from the column, which is where the gate has to
/// sit.
fn span_name_value(val: String) -> TagValue {
    TagValue {
        val,
        val_type: SPAN_NAME_VALUE_TYPE.to_string(),
    }
}

/// What a §4.3 value read needs beyond its key and scope (issue #478).
///
/// `q` is the client's raw query text, percent-decoded — NOT a parsed
/// AST, and deliberately so: lowering it is total
/// ([`crate::traces::spans::tags::narrowing_from_query`]), so an
/// unparseable `q` is answered as an absent one instead of producing an
/// error the handler would have to turn into a status code.
#[derive(Debug, Clone, Copy)]
pub struct TagValuesRequest<'a> {
    /// `q` as the client sent it, percent-decoded. `None` when absent or
    /// empty after decoding.
    pub q: Option<&'a str>,
    /// The resolved window. ALWAYS present: the caller substitutes the
    /// configured lookback when the client sends no usable range, so this
    /// type cannot represent an unbounded read.
    pub start_ns: i64,
    pub end_ns: i64,
    /// The retention floor (issue #496): the catalog read lists only values
    /// whose latest span started at or after it.
    pub floor_ns: i64,
}

impl TagValuesRequest<'_> {
    /// The narrowing this request's `q` contributes (issue #598 part 2):
    /// no body for an absent `q` and for every `q` that is not a root
    /// filter with one.
    fn narrowing(&self, ctx: &PredicateCtx<'_>) -> TagNarrowing {
        match self.q {
            None => TagNarrowing::default(),
            Some(q) => narrowing_from_query(q, ctx),
        }
    }

    /// The request's window, `[start, end)` at nanosecond precision.
    fn window(&self) -> WindowSql {
        WindowSql::start_closed_end_open(self.start_ns, self.end_ns)
    }
}

/// [`TraceEngine::list_tag_values`]'s output (issue #58): distinct
/// `(value, type)` pairs for one key, ordered ascending by value then
/// type, at most [`TAG_VALUES_MAX`]; `truncated` as in [`TagNames`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagValues {
    pub values: Vec<TagValue>,
    pub truncated: bool,
}

/// Charged before a search response's trace buffer is reserved at `n`
/// entries: the buffer's slots and one entry's overhead (issue #591 part 3
/// shared it between the previous search engine and the search statement's
/// decode).
pub(crate) fn output_reserve_bytes(n: usize) -> usize {
    n * std::mem::size_of::<TraceSearchResult>() + RETAINED_ENTRY_OVERHEAD
}

/// The Layer-2 retention counter: one per request, charged on every
/// retained allocation, released when a batch is discarded. A charge
/// that would breach the cap is a `422 query_too_broad` — the byte
/// family of [`TooBroadReason::ScanBudgetBytes`].
///
/// **Error-path contract (round-4 adjudication — intended design):**
/// this counter is strictly request-scoped. On any error the whole
/// budget is dropped with the failing request, so intermediate charges
/// held by values that a `?` unwinds past (charged sets, transients,
/// partially built batches) are **not** individually released on error
/// paths — releasing into a dying counter would be dead work, and no
/// cross-request state exists for a leak to accumulate in. The
/// `used == live allocations` exactness invariant (and its unit tests)
/// therefore applies to the success path and to the pre-error prefix of
/// a failing path, never to post-error bookkeeping.
///
/// **Two disciplines govern this seam** — see docs/architecture.md §5.6.
/// Charging before allocating is the one this type exists for; the other
/// is that a breach here must not preempt a refusal the request already
/// earned for its MEANING, which §5.6's (P)/(A) test is how to tell
/// apart.
#[derive(Debug)]
pub(crate) struct ByteBudget {
    used: usize,
    cap: usize,
}

impl ByteBudget {
    pub(crate) fn new(cap: usize) -> Self {
        ByteBudget { used: 0, cap }
    }

    /// Atomic check-then-add (code review round 3): a FAILED charge does
    /// not mutate the counter — `used` never carries a phantom charge for
    /// an allocation that was refused before it happened, so at a breach
    /// the counter reflects exactly the live allocations.
    pub(crate) fn charge(&mut self, bytes: usize) -> Result<(), ReadError> {
        let would_be = self.used.saturating_add(bytes);
        if would_be > self.cap {
            return Err(ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes {
                budget_bytes: self.cap as u64,
                estimate: None,
            }));
        }
        self.used = would_be;
        Ok(())
    }

    pub(crate) fn release(&mut self, bytes: usize) {
        self.used = self.used.saturating_sub(bytes);
    }

    /// Test-only introspection (the unit-tested accounting the final
    /// amendment mandates for Layer 2).
    #[cfg(test)]
    pub(crate) fn used(&self) -> usize {
        self.used
    }
}

/// Maps a ClickHouse error on the **trace metrics** path (issue #59):
/// code 191 (`SET_SIZE_LIMIT_EXCEEDED`) — raised only by the metrics
/// semi-join IN-set limits — maps to the dedicated, never-conflated
/// [`TooBroadReason::TraceMetricsSetRows`]; everything else delegates to
/// the shared trace mapper ([`map_trace_read_error`]), which never maps
/// 191 itself.
fn map_trace_metrics_error(e: ChError, config: &TraceReadConfig) -> ReadError {
    if let ChError::Server {
        code: CODE_SET_SIZE_LIMIT_EXCEEDED,
        ..
    } = &e
    {
        return ReadError::QueryTooBroad(TooBroadReason::TraceMetricsSetRows {
            max_set_rows: TRACE_METRICS_MAX_SET_ROWS,
        });
    }
    map_trace_read_error(e, config)
}

/// Maps a ClickHouse error on the **trace search** path, the (issue #58
/// re-review) two §4.3 catalog reads that carry the same budget via
/// [`catalog_settings`], and (issue #398) the §4.2 trace-by-id point read.
/// Unlike the LogQL mapper, this one deliberately sets `max_rows_to_read`,
/// so code 158 maps to [`TooBroadReason::TraceScanBudgetRows`]; the
/// read/result byte ceilings (codes 307/396) map to the shared byte-budget
/// reason; and code 241 maps to [`TooBroadReason::TraceReadMemory`].
/// Everything else passes through unmapped (never reinterpreted as a
/// timeout or vice versa).
///
/// **The #412 rule** applies here exactly as it does in
/// `logql::exec::map_read_error` (see that doc for the full argument): the
/// BOUND is enforced by ClickHouse regardless of how the code was parsed,
/// a missed 241 falls open to the pre-#398 `500`, a false 241 only
/// relabels an already-failing query, and this mapper reads only the
/// already-parsed `code` field — so it inherited #412's fix with no edit
/// here when the streaming path stopped searching result bytes on a tagged
/// response (`vendor/clickhouse/PATCHES.md` §2).
/// The search statement's error mapper (issue #591 part 3): `Code: 395`,
/// a `throwIf` demand, is kept as it came, so the fork can tell its own
/// demands from any other; everything else is [`map_trace_read_error`],
/// as every trace read.
fn map_search_statement_error(e: ChError, config: &TraceReadConfig) -> ReadError {
    match e {
        // Issue #593 part 2: a climb with a parent link left after
        // `max_depth` links — a chain deeper than the bound.
        ChError::Server {
            code: 395,
            ref message,
        } if message.contains(super::spans::structural::CLIMB_OVERFLOW) => {
            ReadError::QueryTooBroad(TooBroadReason::TraceStructuralDepth {
                max_depth: u64::from(config.max_depth),
            })
        }
        ChError::Server { code: 395, .. } => ReadError::Clickhouse(e),
        other => map_trace_read_error(other, config),
    }
}

/// One search statement row's retained bytes, charged as it streams: the
/// row, its two root strings, and per span the tuple, its service and per
/// projected value the value and its two strings.
fn search_row_bytes(row: &SearchTraceRow) -> usize {
    std::mem::size_of::<SearchTraceRow>()
        + row.root_service.len()
        + row.root_name.len()
        + search_spans_bytes(&row.spans)
}

/// The spans of a search statement row: per span the tuple, its service
/// and per projected value the value and its two strings.
fn search_spans_bytes(spans: &[SearchSpanTuple]) -> usize {
    spans
        .iter()
        .map(|s| {
            std::mem::size_of::<SearchSpanTuple>()
                + s.service.len()
                + s.projected
                    .iter()
                    .map(|p| std::mem::size_of::<SearchProjected>() + p.value.len() + p.kind.len())
                    .sum::<usize>()
        })
        .sum::<usize>()
}

/// [`search_row_bytes`] for a grouped statement's row (issue #592 part 2):
/// the trace's columns, and per group the tuple, its two strings and its
/// spans.
fn search_grouped_row_bytes(row: &SearchGroupedRow) -> usize {
    std::mem::size_of::<SearchGroupedRow>()
        + row.root_service.len()
        + row.root_name.len()
        + search_spans_bytes(&row.spans)
        + row
            .groups
            .iter()
            .map(|g| {
                std::mem::size_of::<SearchGroupTuple>()
                    + g.attrs
                        .iter()
                        .map(|(v, t)| std::mem::size_of::<(String, String)>() + v.len() + t.len())
                        .sum::<usize>()
                    + search_spans_bytes(&g.spans)
            })
            .sum::<usize>()
}

fn map_trace_read_error(e: ChError, config: &TraceReadConfig) -> ReadError {
    if let ChError::Server { code, .. } = &e {
        match *code {
            // Issue #398: the surface-wide memory ceiling
            // `search_settings`/`catalog_settings` carry.
            CODE_MEMORY_LIMIT_EXCEEDED => {
                return ReadError::QueryTooBroad(TooBroadReason::TraceReadMemory {
                    budget_bytes: config.read_max_memory_bytes,
                });
            }
            CODE_TOO_MANY_ROWS => {
                return ReadError::QueryTooBroad(TooBroadReason::TraceScanBudgetRows {
                    budget_rows: config.scan_budget_rows,
                });
            }
            CODE_TOO_MANY_BYTES => {
                return ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes {
                    budget_bytes: TRACE_READ_BYTES_BUDGET,
                    estimate: None,
                });
            }
            CODE_TOO_MANY_ROWS_OR_BYTES => {
                return ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes {
                    budget_bytes: TRACE_MAX_RESULT_BYTES,
                    estimate: None,
                });
            }
            _ => {}
        }
    }
    ReadError::Clickhouse(e)
}

pub struct TraceEngine {
    dispatch: super::dispatch::TraceDispatch,
    config: TraceReadConfig,
    indexed: Vec<super::spans::attr_index::IndexedAttr>,
    /// The ceiling on `R`, the newest spans a span-name or narrowed-values
    /// read reaches back to (issue #598 part 2a).
    tag_read_rows: u64,
    /// The clustered service graph's target, shared across engines.
    graph_target: GraphTargetCache,
}

impl TraceEngine {
    pub fn new(client: ChClient, config: TraceReadConfig) -> Self {
        Self {
            dispatch: super::dispatch::TraceDispatch::new(client),
            config,
            indexed: Vec::new(),
            tag_read_rows: TAG_STORE_READ_ROWS,
            graph_target: GraphTargetCache::default(),
        }
    }

    /// Shares `cache` as this engine's [`GraphTarget`] cache (issue #599):
    /// the server hands every per-request engine the one process-wide
    /// cache, so the target is read once per process.
    pub fn with_graph_target_cache(mut self, cache: GraphTargetCache) -> Self {
        self.graph_target = cache;
        self
    }

    /// Lowers the ceiling on `R`, the spans a span-name or narrowed-values
    /// read reaches back to, from [`TAG_STORE_READ_ROWS`] (issue #598
    /// part 2a); for tests that stop a read on a small corpus. `R` stays
    /// sized from the scan budget below it.
    pub fn with_tag_read_rows(mut self, rows: u64) -> Self {
        self.tag_read_rows = rows;
        self
    }

    /// Stamps every statement this engine issues with
    /// `<prefix>-<n>`, `n` counting from 1 in issue order (issue #587).
    ///
    /// The server mints one prefix per fetch request and returns it in a
    /// response header, so a caller reads the prefix rather than
    /// recomputing it; a test then reads the statements the request
    /// issued out of the engine's own `query_log` rows.
    pub fn with_statement_prefix(mut self, prefix: &str) -> Self {
        self.dispatch = self.dispatch.with_statement_prefix(prefix);
        self
    }

    /// The metrics planning context this engine's configuration implies —
    /// callers feed it to [`super::metrics_plan::plan_trace_metrics`]
    /// (issue #59).
    pub fn metrics_ctx(&self) -> MetricsCtx {
        MetricsCtx {
            scan_budget_rows: self.config.scan_budget_rows,
            max_series: self.config.max_series,
            distributed: self.config.distributed,
            skip_unavailable_shards: self.config.skip_unavailable_shards,
        }
    }

    /// Executes the §4.5 service-graph read (issues #173 and #599): one
    /// statement over `service_edges` and `spans` (`spans::graph`) — the
    /// pairing of client and server halves and the
    /// per-`(client, server, conn_type)` rollup all happen in ClickHouse; the
    /// engine only frames at most [`SERVICE_GRAPH_MAX_EDGES`]
    /// edges. The `LIMIT cap + 1` probe row (never returned) flips
    /// `truncated` rather than shipping a silent subset. `max_rows_to_read =
    /// scan_budget_rows` (throw) bounds the join's scan/hash-table cost — a
    /// breach maps through [`map_trace_read_error`] (code 158) to `422
    /// query_too_broad`; clustered mode injects the §7 clustered-reader
    /// settings + `distributed_product_mode='local'` so the join runs
    /// shard-local. Merge-invariant by construction (per-side read-time
    /// dedup), so the result is byte-identical before and after
    /// `OPTIMIZE ... FINAL`.
    pub async fn service_graph(&self, window: GraphWindow) -> Result<ServiceGraph, ReadError> {
        let cap = SERVICE_GRAPH_MAX_EDGES;
        let settings = graph_settings(&self.config);
        let target = if self.config.distributed {
            Some(self.graph_target(&settings).await?)
        } else {
            None
        };
        let sql = super::spans::graph::spans_graph_sql(window, target, cap);
        let mut edges: Vec<GraphEdgeRow> = Vec::new();
        // Scoped stream (module convention): the pooled-connection lease
        // drops at return, after full consumption (≤ cap + 1 rows by the SQL
        // LIMIT).
        let mut stream = self
            .dispatch
            .query_stream::<GraphEdgeRow, _>(&sql, &settings, |e| {
                map_trace_read_error(e, &self.config)
            })
            .await?;
        while let Some(row) = stream.next().await {
            edges.push(row.map_err(|e| map_trace_read_error(e, &self.config))?);
        }
        let truncated = edges.len() as u64 > cap;
        edges.truncate(cap as usize);
        Ok(ServiceGraph { edges, truncated })
    }

    /// The clustered service graph's [`GraphTarget`] (issue #599): read
    /// from the routing table's definition on the first call in the
    /// process, under the request's id and settings, and kept in the
    /// shared [`GraphTargetCache`]. A definition that names no target fails
    /// the request and keeps nothing, so the next call reads it again.
    async fn graph_target(&self, settings: &QuerySettings) -> Result<&GraphTarget, ReadError> {
        self.graph_target
            .0
            .get_or_try_init(|| async {
                let table = &self.config.spans_table;
                let sql = super::spans::graph::graph_target_sql(table);
                let mut stream = self
                    .dispatch
                    .query_stream::<GraphTargetRow, _>(&sql, settings, |e| {
                        map_trace_read_error(e, &self.config)
                    })
                    .await?;
                let mut definition = None;
                while let Some(row) = stream.next().await {
                    let row = row.map_err(|e| map_trace_read_error(e, &self.config))?;
                    definition.get_or_insert(row.engine_full);
                }
                definition
                    .as_deref()
                    .and_then(graph_target_from_engine)
                    .ok_or_else(|| {
                        ReadError::Clickhouse(super::spans::graph::no_graph_target(
                            table,
                            definition.as_deref(),
                        ))
                    })
            })
            .await
    }

    /// Fetches one trace from the span, per-trace and resource tables
    /// (issue #587).
    ///
    /// `hex32` must already be validated as exactly 32 lowercase hex chars
    /// (the server's `parse_trace_id` is the one validation point) —
    /// injection-safe because only `[0-9a-f]` can then reach the
    /// `unhex('...')` literal. An empty `spans` means no statement found
    /// the trace; duplicate `(span_id, kind)` pairs — a span stored twice,
    /// read without `final` — are returned as stored, because the dedup is
    /// the assembler's read-time concern.
    ///
    /// `window` is the request's own `start`/`end` when it supplied them.
    /// **The indexed statement never reads it** — its bounds come from the
    /// per-trace row — so the window only ever ADDS an answer, which is
    /// what keeps the documented property that ignoring `start`/`end`
    /// returns a superset and never a wrong answer. With no window a trace
    /// the per-trace table has not indexed answers empty rather than
    /// taking a `trace_id`-only predicate over every partition in
    /// retention.
    ///
    /// **Issue #509: this read is not exempt from the query-text guard —
    /// it PASSES it.** All three statements are fixed templates plus 32
    /// caller-validated hex chars and at most three nanosecond literals,
    /// so the text is bounded by construction (pinned by
    /// `both_fetch_statements_stay_under_the_query_text_cap` in this
    /// module's tests) and the guard `traces::dispatch` applies to every
    /// read on this path can never fire here.
    pub async fn fetch_by_id(
        &self,
        hex32: &str,
        window: Option<FetchWindow>,
    ) -> Result<FetchedTrace, ReadError> {
        let settings = fetch_settings(&self.config);
        let sql = span_fetch::indexed_fetch_sql(
            &self.config.traces_table,
            &self.config.spans_table,
            &self.config.resources_table,
            hex32,
        );
        // Statement 1 returns exactly ONE row whatever `index_rows` turns
        // out to be — three scalars and no `FROM` for an absent trace —
        // so no row at all is an engine fault rather than an absent
        // trace.
        let first: IndexedFetchRow = self.one_fetch_row(&sql, &settings).await?;
        let (route, second) = fetch_route_and_second(
            &self.config,
            hex32,
            first.index_rows,
            first.bucket_count,
            !first.spans.is_empty(),
            window,
        );
        let Some(second_sql) = second else {
            return Ok(FetchedTrace {
                spans: first.spans,
                resources: first.resources,
                statements: 1,
                route,
            });
        };
        if route == FetchRoute::TruncatedSet {
            let wide: WideFetchRow = self.one_fetch_row(&second_sql, &settings).await?;
            return Ok(FetchedTrace {
                spans: wide.spans,
                resources: wide.resources,
                statements: 2,
                route,
            });
        }
        let fallback: FallbackFetchRow = self.one_fetch_row(&second_sql, &settings).await?;
        Ok(FetchedTrace {
            spans: fallback.spans,
            resources: fallback.resources,
            statements: 2,
            route,
        })
    }

    /// One statement that returns exactly one row.
    ///
    /// Every one of the fetch's three statements does: each aggregates its
    /// span read into one scalar and projects it beside the resource
    /// scalar, so there is no stream to collect and no per-row budget to
    /// charge — the whole answer is one row, and the engine's own
    /// `max_memory_usage` is what bounds the aggregate state behind it
    /// (the limit this part records rather than predicts).
    async fn one_fetch_row<R: ChRow>(
        &self,
        sql: &str,
        settings: &QuerySettings,
    ) -> Result<R, ReadError> {
        let mut stream = self
            .dispatch
            .query_stream::<R, _>(sql, settings, |e| map_trace_read_error(e, &self.config))
            .await?;
        stream
            .next()
            .await
            .ok_or_else(|| {
                ReadError::Clickhouse(ChError::Decode(
                    "a trace fetch statement returned no row; each of the three returns \
                     exactly one"
                        .to_string(),
                ))
            })?
            .map_err(|e| map_trace_read_error(e, &self.config))
    }

    /// Streams the §4.3 tag-names read (issue #58): distinct
    /// `(scope, key)` pairs from the Global tag catalog — only ever
    /// [`spans::tags`](super::spans::tags) (`tag_names`,
    /// never `_dist`, never a span/attr table: discovery never scans
    /// payloads, epic #19 AC1).
    /// `scope` is escaped HERE (`ch_string`) before it reaches the pure
    /// builder — the engine is the catalog reads' injection boundary.
    /// Bounded by the SQL `LIMIT` cap + 1 probe: at most
    /// [`TAG_NAMES_MAX`] rows return, and the probe row (row cap + 1)
    /// flips `truncated` instead of shipping a silent subset. The `LIMIT`
    /// bounds *returned* rows only; an unscoped read carries the
    /// `scope IN (…)` attribute-scope predicate on the catalog's leading
    /// primary-key column (issue #475), so it prunes rather than scanning
    /// the whole table, and [`catalog_settings`] (issue #58 re-review)
    /// bounds *scanned* rows regardless: a breach maps through
    /// [`map_trace_read_error`] to `422 query_too_broad` instead of
    /// running unbounded.
    pub async fn list_tag_names(
        &self,
        scope: Option<&str>,
        floor_ns: i64,
    ) -> Result<TagNames, ReadError> {
        let scope_literal = scope.map(crate::logql::escape::ch_string);
        let sql = super::spans::tags::tag_names_sql(
            scope_literal.as_deref(),
            floor_ns,
            TAG_NAMES_MAX + 1,
        );
        let settings = catalog_settings(&self.config);
        let mut names = Vec::new();
        // Scoped stream (module convention): the pooled-connection lease
        // drops at return, after full consumption — the stream is always
        // drained (≤ cap + 1 rows by the SQL LIMIT).
        let mut stream = self
            .dispatch
            .query_stream::<TagNameRow, _>(&sql, &settings, |e| {
                map_trace_read_error(e, &self.config)
            })
            .await?;
        while let Some(row) = stream.next().await {
            let row = row.map_err(|e| map_trace_read_error(e, &self.config))?;
            names.push((row.scope, row.key));
        }
        let truncated = names.len() > TAG_NAMES_MAX;
        names.truncate(TAG_NAMES_MAX);
        Ok(TagNames { names, truncated })
    }

    /// Streams the §4.3 tag-values read for an ATTRIBUTE key (issues #58
    /// and #478): distinct `(value, type)` pairs for one key, optionally
    /// scope-confined, capped at [`TAG_VALUES_MAX`] by the `LIMIT` cap + 1
    /// probe.
    ///
    /// **Two reads, chosen by whether `req` narrows anything, and the
    /// unnarrowed one is unchanged.** With no pushable `q` term this is
    /// exactly the issue #58 catalog read — same SQL bytes, same
    /// [`catalog_settings`] budget, same one-part index-served cost — so a
    /// dropdown that narrows nothing costs what it always did. With at
    /// least one term it becomes a read of `spans` narrowed to the
    /// matching span set, carrying
    /// [`metrics_settings`] (the semi-join budget set, and
    /// `distributed_product_mode = 'local'` when clustered so the
    /// membership probe stays shard-local — both tables co-shard on
    /// `cityHash64(trace_id)`).
    ///
    /// A bare-key lookup (no `scope`) prunes the leading
    /// `scope IN (…)` attribute-scope list on either shape (issue #475),
    /// so it reads the five attribute scopes and never the
    /// writer-reserved intrinsic ones. `key`/`scope` are escaped HERE —
    /// the engine is this read's injection boundary — and a narrowing
    /// term's own literals are escaped by the builder.
    ///
    /// One SQL per request on both shapes: the single-read-per-request
    /// property of the §4.3 handlers is preserved.
    pub async fn list_tag_values(
        &self,
        key: &str,
        scope: Option<&str>,
        req: TagValuesRequest<'_>,
    ) -> Result<TagValues, ReadError> {
        let w = req.window();
        let ctx = PredicateCtx {
            window: w,
            spans_table: &self.config.spans_table,
            resources_table: &self.config.resources_table,
            indexed: &[],
        };
        let n = req.narrowing(&ctx);
        if !n.has_body() {
            let key_literal = crate::logql::escape::ch_string(key);
            let scope_literal = scope.map(crate::logql::escape::ch_string);
            let sql = super::spans::tags::tag_values_sql(
                &key_literal,
                scope_literal.as_deref(),
                req.floor_ns,
                TAG_VALUES_MAX + 1,
            );
            return self
                .stream_tag_values(&sql, &catalog_settings(&self.config), &n)
                .await;
        }
        // Issue #601 parts 4b and 4c: a span- or resource-scoped key narrowed
        // by one service equality answers from its bucket catalogue over the
        // whole window, unless an interior bucket of a span key is
        // overflowed, when today's statement answers.
        if let Some(service) = n.service_equality()
            && let Some(values) = self.catalogue_values(scope, service, key, req).await?
        {
            return Ok(values);
        }
        // Issue #509: neither branch escapes a `?` here. The doubling
        // happens once, in `traces::dispatch`, for every read on this
        // path. The key is escaped into a JSON path by the builder.
        let sql = tag_values_statement(&self.config, self.tag_read_rows, scope, key, w, &n);
        self.stream_tag_values(&sql, &tag_store_settings(&self.config), &n)
            .await
    }

    /// Streams the §4.3 SPAN-NAME values read (issue #478 Part 1) —
    /// ALWAYS store-backed, never the catalog.
    ///
    /// The catalog cannot answer this: no span-`name` row exists in
    /// `tag_values`. What a
    /// bare `name` lookup reached before was span EVENT names under a
    /// reserved intrinsic scope; reading `spans.name` is
    /// structurally immune to that, because that column holds span names
    /// and nothing else.
    ///
    /// Every value is typed `string` EXPLICITLY rather than left empty:
    /// the renderer copies `val_type` verbatim (issue #602 part 3c), so
    /// an unset type would reach the wire as `"type":""`. The type is set
    /// here, where it is a fact about the column (issue #476 introduced
    /// the stored type).
    ///
    pub async fn list_span_name_values(
        &self,
        req: TagValuesRequest<'_>,
    ) -> Result<TagValues, ReadError> {
        let w = req.window();
        let ctx = PredicateCtx {
            window: w,
            spans_table: &self.config.spans_table,
            resources_table: &self.config.resources_table,
            indexed: &[],
        };
        let n = req.narrowing(&ctx);
        let sql = span_names_statement(&self.config, self.tag_read_rows, w, &n);
        let settings = tag_store_settings(&self.config);
        let mut values = Vec::new();
        // Scoped stream: same lease/drain contract as list_tag_names.
        let mut stream = self
            .dispatch
            .query_stream::<SpanNameRow, _>(&sql, &settings, |e| {
                tag_read_error(e, &n, &self.config)
            })
            .await?;
        while let Some(row) = stream.next().await {
            let row = row.map_err(|e| tag_read_error(e, &n, &self.config))?;
            values.push(span_name_value(row.val));
        }
        let truncated = values.len() > TAG_VALUES_MAX;
        values.truncate(TAG_VALUES_MAX);
        Ok(TagValues { values, truncated })
    }

    /// The `(val, val_type)` streaming both attribute-values shapes share
    /// — one place that caps, truncates and reports, so the narrowed and
    /// unnarrowed reads cannot differ in how they bound themselves.
    async fn stream_tag_values(
        &self,
        sql: &str,
        settings: &QuerySettings,
        n: &TagNarrowing,
    ) -> Result<TagValues, ReadError> {
        let mut values = Vec::new();
        // Scoped stream: same lease/drain contract as list_tag_names.
        let mut stream = self
            .dispatch
            .query_stream::<TagValueRow, _>(sql, settings, |e| tag_read_error(e, n, &self.config))
            .await?;
        while let Some(row) = stream.next().await {
            let row = row.map_err(|e| tag_read_error(e, n, &self.config))?;
            values.push(TagValue {
                val: row.val,
                val_type: row.val_type,
            });
        }
        let truncated = values.len() > TAG_VALUES_MAX;
        values.truncate(TAG_VALUES_MAX);
        Ok(TagValues { values, truncated })
    }

    /// Answers `request` with the search statement (issue #602 part 2b),
    /// the newest slices first. It is the route's only engine: the
    /// statement's error is the request's, a `!` demand as `400`.
    pub async fn search_routed(
        &self,
        request: &super::spans::search::SearchRequest,
    ) -> Result<SearchOutput, ReadError> {
        self.search_routed_inner(request, None).await
    }

    /// [`Self::search_routed`], with the per-stage SQL: each
    /// `search_slice` and, when the slices do not answer, the
    /// `search_statement` stage.
    pub async fn search_routed_explained(
        &self,
        request: &super::spans::search::SearchRequest,
    ) -> Result<(SearchOutput, PlanExplain), ReadError> {
        let mut explain = PlanExplain::new("traces");
        let output = self
            .search_routed_inner(request, Some(&mut explain))
            .await?;
        Ok((output, explain))
    }

    /// The shared body: each statement with its own settings
    /// (`newest_first.rs`'s `statement_settings`), its rows and decode
    /// charged to one 256 MiB budget. A
    /// request the statement's compiler refuses is a `400` with its text.
    async fn search_routed_inner(
        &self,
        request: &super::spans::search::SearchRequest,
        mut explain: Option<&mut PlanExplain>,
    ) -> Result<SearchOutput, ReadError> {
        let stmt =
            self.search_statement(request, None)
                .map_err(|e| ReadError::PipelineInvalid {
                    reason: e.to_string(),
                })?;
        if let Some(out) = self.newest_first(request, &stmt, &mut explain).await? {
            return Ok(out);
        }
        let mut budget = ByteBudget::new(HYDRATION_BYTE_BUDGET);
        let mut charged = 0usize;
        match self.statement_rows(&stmt, &mut budget, &mut charged).await {
            Ok(rows) => rows.decode(&stmt, request.limit, &mut budget, self.config.max_series),
            Err(e) => Err(newest_first::statement_error(&stmt, e, &self.config)),
        }
    }

    /// The statement for `request`, its top-K reading the newest
    /// `slice_ns` when given; on the error-span table when
    /// [`super::spans::error_spans::admits`] the query (issue #601 part 4b).
    pub(super) fn search_statement(
        &self,
        request: &super::spans::search::SearchRequest,
        slice_ns: Option<i64>,
    ) -> Result<super::spans::search::SearchStatement, super::PlanError> {
        let spans = if super::spans::error_spans::admits(&request.query) {
            &self.config.error_spans_table
        } else {
            &self.config.spans_table
        };
        super::spans::search::plan_statement_sliced(
            request,
            spans,
            &self.config.traces_table,
            &self.config.resources_table,
            self.config.max_depth,
            &self.indexed,
            slice_ns,
        )
    }

    /// Runs one search query to completion inside its own scope (the
    /// pooled-connection lease drops at return), charging every row's
    /// retention cost against the Layer-2 budget **as it streams** — the
    /// counter trips mid-stream, so accumulated state never exceeds the
    /// budget by more than the driver's one-block transient (the
    /// documented Layer-1 residual). `charged` accumulates what the
    /// caller must release when it discards the rows.
    ///
    /// **Issue #35: the single choke point for every search CHARGE.**
    /// The search statement's rows route through this one function, so
    /// the byte budget is priced, charged and accepted in one place.
    ///
    /// **Issue #509: it is no longer the choke point for query TEXT.**
    /// The `?`-doubling and
    /// [`crate::querytext::ensure_query_text_fits`] used to run here,
    /// which covered the search phase and left the tag, point-read and
    /// metrics reads to remember them site by site — three of the
    /// twenty-seven did not. Both now run in `traces::dispatch`, once,
    /// for every read this module issues.
    ///
    /// **Issue #57 re-audit:** `mapper` is the caller's error mapping; the
    /// search statement passes [`map_search_statement_error`].
    ///
    /// **If the returned `Vec` is NOT your retained form, use
    /// [`Self::stream_rows_charged`] instead** (issue #351 review 2). A
    /// caller that collects here and then reshapes the rows into another
    /// structure holds BOTH at the same instant — charged once, live
    /// twice — and the charge, however careful, describes only one of
    /// them. The streaming form has no intermediate collection, so the
    /// caller's structure is the whole peak.
    async fn collect_rows_charged<R: ChRow, F: FnMut(&R) -> usize>(
        &self,
        sql: &str,
        settings: &QuerySettings,
        budget: &mut ByteBudget,
        charged: &mut usize,
        mapper: fn(ChError, &TraceReadConfig) -> ReadError,
        cost: F,
    ) -> Result<Vec<R>, ReadError> {
        let mut rows = Vec::new();
        {
            let mut sink = FnRowSink {
                cost,
                accept: |row| rows.push(row),
            };
            self.stream_rows_charged(sql, settings, budget, charged, mapper, &mut sink)
                .await?;
        }
        Ok(rows)
    }

    /// The same choke point as [`Self::collect_rows_charged`], for a read
    /// whose retained form is NOT the row vector (issue #351 review 2).
    ///
    /// **Why it exists.** `collect_rows_charged` returns every row, so a
    /// caller that then reshapes them into another structure holds BOTH
    /// at once — charged once, live twice. Handing each row to a `sink`
    /// as it streams removes the intermediate collection entirely: the
    /// caller's own structure is the only place a value ever lands, and
    /// the peak live set is that structure plus the single row the driver
    /// has decoded. `collect_rows_charged` is now this function with a
    /// `Vec::push` sink, so the two cannot drift on query-text checking,
    /// settings, error mapping, or charge ORDER.
    ///
    /// The charge runs BEFORE the row reaches the sink, so a breach
    /// refuses the value rather than retaining it.
    async fn stream_rows_charged<R: ChRow>(
        &self,
        sql: &str,
        settings: &QuerySettings,
        budget: &mut ByteBudget,
        charged: &mut usize,
        mapper: fn(ChError, &TraceReadConfig) -> ReadError,
        sink: &mut impl ChargedRowSink<R>,
    ) -> Result<(), ReadError> {
        let mut stream = self
            .dispatch
            .query_stream::<R, _>(sql, settings, |e| mapper(e, &self.config))
            .await?;
        while let Some(row) = stream.next().await {
            let row = row.map_err(|e| mapper(e, &self.config))?;
            // Price, charge, THEN hand over — the order is the invariant
            // the sink trait exists to make structural.
            let bytes = sink.cost(&row);
            budget.charge(bytes)?;
            *charged += bytes;
            sink.accept(row);
        }
        Ok(())
    }
}

/// The Layer-1 budget settings every search query carries (issue #57
/// re-audit): the row scan budget plus read-side and result-side byte
/// budgets, all with throw semantics, plus [`TRACE_SEARCH_MAX_BLOCK_ROWS`]
/// (`max_block_size`) bounding the row width of any single transiently-
/// buffered block; clustered mode adds the docs/schemas.md §7
/// clustered-reader settings first. The accepted, documented residual is
/// block-granular enforcement — the driver may transiently hold at most
/// one block, now hard-bounded by `max_block_size` rows ×
/// [`crate::traces::sql_text::TRACE_STR_COL_CAP`]-capped string columns
/// (never a-priori row-unbounded); the Layer-2 retention counter is the
/// binding bound on accumulated state across the whole request.
///
/// #603 follow-up: the answer pins follow the base, before every other
/// setting, pruning on only when sharded (trace rows are placed by
/// `cityHash64(trace_id)`).
fn search_settings(config: &TraceReadConfig) -> QuerySettings {
    let base = if config.distributed {
        QuerySettings::clustered_reader(config.skip_unavailable_shards)
    } else {
        QuerySettings::new()
    };
    base.answer_pins(config.distributed, config.skip_unavailable_shards)
        .set("max_rows_to_read", config.scan_budget_rows)
        .set("max_bytes_to_read", TRACE_READ_BYTES_BUDGET)
        .set("read_overflow_mode", "throw")
        .set("max_result_bytes", TRACE_MAX_RESULT_BYTES)
        .set("result_overflow_mode", "throw")
        .set("max_block_size", TRACE_SEARCH_MAX_BLOCK_ROWS)
        // Issue #35: the raised `max_query_size` parse-buffer cap — every
        // search statement routes through `collect_rows_charged`, which
        // carries this settings object.
        .set("max_query_size", crate::querytext::MAX_QUERY_TEXT_BYTES)
        // Issue #398: the surface-wide per-query memory ceiling
        // (`reader.traceql_read_max_memory_bytes`), throw-not-spill.
        .set("max_memory_usage", config.read_max_memory_bytes)
        .set("max_bytes_before_external_group_by", 0u64)
}

/// The settings the span-name and narrowed-values reads carry (issue #598
/// part 2a): [`search_settings`], whose `max_rows_to_read` is the scan
/// budget with `throw`, a backstop behind the cut; at most 10,000 distinct
/// values collected, then `break`, so memory is bounded by the answer; and
/// `max_threads` pinned at 4, so the read-ahead past the cut does not
/// depend on the host. Not [`metrics_settings`]: its clustered
/// `distributed_product_mode = 'local'` would read a resource read's
/// span subquery from one shard only.
fn tag_store_settings(config: &TraceReadConfig) -> QuerySettings {
    search_settings(config)
        .set("max_rows_in_distinct", 10_000u64)
        .set("distinct_overflow_mode", "break")
        .set("max_threads", 4u64)
}

/// The narrowed values statement for one attribute key (issue #598 part
/// 2a), its cut sized from `ceiling`, the scan budget and the statement's
/// table reads.
fn tag_values_statement(
    config: &TraceReadConfig,
    ceiling: u64,
    scope: Option<&str>,
    key: &str,
    w: WindowSql,
    n: &TagNarrowing,
) -> String {
    let vs = ValueScope::from_attr_scope(scope);
    narrowed_values_sql(
        &config.spans_table,
        &config.resources_table,
        &config.traces_table,
        vs,
        key,
        w,
        n,
        cut_rows(
            ceiling,
            config.scan_budget_rows,
            table_reads(TagShape::Values(vs), n),
        ),
        TAG_VALUES_MAX + 1,
    )
}

/// The span-name statement (issue #598 part 2a), its cut sized as
/// [`tag_values_statement`]'s.
fn span_names_statement(
    config: &TraceReadConfig,
    ceiling: u64,
    w: WindowSql,
    n: &TagNarrowing,
) -> String {
    name_values_sql(
        &config.spans_table,
        &config.traces_table,
        w,
        n,
        cut_rows(
            ceiling,
            config.scan_budget_rows,
            table_reads(TagShape::Names, n),
        ),
        TAG_VALUES_MAX + 1,
    )
}

/// A tag read's ClickHouse error as the request's: a `!` demand the
/// predicate raised (`Code: 395` carrying one of its messages) is a `400`
/// with that message, as on the search route; any other error as the
/// trace read path maps it.
fn tag_read_error(e: ChError, n: &TagNarrowing, config: &TraceReadConfig) -> ReadError {
    if let ChError::Server { code: 395, message } = &e
        && let Some(d) = n.predicate().and_then(|p| {
            p.demand_messages()
                .iter()
                .find(|d| message.contains(d.as_str()))
        })
    {
        return ReadError::PipelineInvalid { reason: d.clone() };
    }
    map_trace_read_error(e, config)
}

/// The Layer-1 read budget the two §4.3 catalog reads carry (issue #58
/// re-review): `max_rows_to_read` (reusing
/// `reader.traceql_scan_budget_rows` — the same knob [`search_settings`]
/// uses, one number, no dedicated catalog config surface) plus the
/// read-side byte budget, both throw. The catalog is `Replication::Global`
/// and never `_dist`-suffixed, so — unlike [`search_settings`] — this
/// deliberately never adds the clustered-reader settings: there is no
/// coordinator fan-out to bound. Result-side (`max_result_bytes`) is
/// deliberately omitted: it does not reliably throw on a streamed
/// `DISTINCT` shape (docs/schemas.md §7); the read-side row budget is the
/// binding bound a breach maps through ([`map_trace_read_error`], code
/// 158 → [`TooBroadReason::TraceScanBudgetRows`]). A breach means an
/// over-broad discovery scan (unscoped `/tags`, or a bare-key `/values`
/// lookup with no scope) aborts loud at `422` rather than serving a slow
/// unbounded scan; scoped reads that prune to a small partition stay
/// under budget and return `200` as before.
fn catalog_settings(config: &TraceReadConfig) -> QuerySettings {
    // The catalogue tables are copied whole to every node, so no read of
    // them fans out: the answer pins with pruning off.
    QuerySettings::new()
        .answer_pins(false, config.skip_unavailable_shards)
        .set("max_rows_to_read", config.scan_budget_rows)
        .set("max_bytes_to_read", TRACE_READ_BYTES_BUDGET)
        .set("read_overflow_mode", "throw")
        // Issue #35: same raised parse-buffer cap as `search_settings`.
        .set("max_query_size", crate::querytext::MAX_QUERY_TEXT_BYTES)
        // Issue #398: same memory ceiling as `search_settings`. This root
        // is deliberately INDEPENDENT of `search_settings` (it omits the
        // clustered-reader block on purpose), which is precisely why the
        // ceiling has to be repeated here rather than inherited — and this
        // is the root whose unbounded `/api/search/tag/{tag}/values` read
        // produced the measured `500` that opened #398.
        .set("max_memory_usage", config.read_max_memory_bytes)
        .set("max_bytes_before_external_group_by", 0u64)
}

/// The trace fetch's Layer-1 settings (issue #587): a **third root**
/// beside [`search_settings`] and [`catalog_settings`], not a change to
/// either.
///
/// [`catalog_settings`]'s budget set, **plus**
/// `QuerySettings::clustered_reader` as the base when clustered.
/// `catalog_settings` deliberately carries no clustered-reader block
/// because the catalogs are `Replication::Global`; the fetch reads two
/// `PerShard` tables, so it needs one.
///
/// **No `final`** (issue #601 part 4a). A retried span stored twice comes
/// back twice in the span array and the server's rebuild keeps one per
/// `(span_id, kind)`; the resource read groups by `resource_id` with `any`;
/// and on the per-trace table `index_rows` is read only as `!= 0` and
/// `min`/`max` are idempotent under aggregation, so they return the same
/// value whether the trace's day rows have been collapsed or not.
///
/// **`distributed_product_mode` is deliberately not set, and there is
/// nothing left for it to act on.** It exists to rewrite an
/// `IN (SELECT … FROM <t>_dist)` to the local shard, and neither statement
/// here contains a subquery over a `Distributed` table: the tuple `IN`s
/// read `arrayJoin(bk)` and `arrayJoin(sp.2)`, and neither reads a table
/// at all. A setting with no subject is not set.
///
/// **`load_balancing` is deliberately not set either.** A replica
/// preference puts two reads on one *host*, which is not one *position*,
/// so it closes none of the visibility states it was once proposed for.
fn fetch_settings(config: &TraceReadConfig) -> QuerySettings {
    let base = if config.distributed {
        QuerySettings::clustered_reader(config.skip_unavailable_shards)
    } else {
        QuerySettings::new()
    };
    base.answer_pins(config.distributed, config.skip_unavailable_shards)
        .set("max_rows_to_read", config.scan_budget_rows)
        .set("max_bytes_to_read", TRACE_READ_BYTES_BUDGET)
        .set("read_overflow_mode", "throw")
        .set("max_query_size", crate::querytext::MAX_QUERY_TEXT_BYTES)
        .set("max_memory_usage", config.read_max_memory_bytes)
        .set("max_bytes_before_external_group_by", 0u64)
}

/// `final = 1`, with each partition merged on its own (issue #594 part 1):
/// on `traces` and `resources`, whose partition key is not in their
/// sorting key, a `FINAL` merging across partitions defers partition
/// pruning until after it, so a `day` bound prunes nothing and filters
/// rows merged from several days by an arbitrary one. No read here needs a
/// row merged across days: a `spans` row's partition follows from its key,
/// every `traces` read aggregates its rows, and a `resources` row is the
/// same on every day it is seen.
fn with_final(s: QuerySettings) -> QuerySettings {
    s.set("final", 1)
        .set("do_not_merge_across_partitions_select_final", 1)
}

/// What §2.4's branch decides: which route answers, and the second
/// statement's text when there is one.
///
/// **Pure, and that is deliberate** — the branch is three conditions over
/// statement 1's answer and the request's window, so it is testable
/// without a database and [`TraceEngine::fetch_by_id`] is its one caller.
/// `statements` is `1` when the second element is `None` and `2` when it
/// is `Some`, which is the whole of the count the measurement record
/// publishes.
///
/// The order of the conditions is load-bearing. **The truncation branch is
/// tested first** because the suppression conjunct makes statement 1's
/// array empty on that route, and an empty array would otherwise fall
/// through to the window fallback and answer from a window the caller may
/// not have supplied. It needs no `index_rows` guard: with no per-trace
/// row the bucket set defaults to the empty array and its length is `0`,
/// so the test is false for an absent trace by construction.
///
/// **Both halves of the second condition are load-bearing.**
/// `index_rows != 0` is needed because the state where the per-trace table
/// holds a row and the span table holds nothing is reachable — a partial
/// view fan-out, or a stale extent — and with a window the fallback
/// answers it. A non-empty span array is needed because statement 1
/// answers one row whatever `index_rows` turns out to be.
fn fetch_route_and_second(
    config: &TraceReadConfig,
    hex32: &str,
    index_rows: u64,
    bucket_count: u32,
    spans_non_empty: bool,
    window: Option<FetchWindow>,
) -> (FetchRoute, Option<String>) {
    // The stored set may have truncated. `>=` and not `==`: an uncapped
    // union of a truncated row with one further bucket is 4,097,
    // measured, so equality is false exactly when truncation has
    // happened.
    if bucket_count >= span_fetch::BUCKET_SET_CAP {
        return (
            FetchRoute::TruncatedSet,
            Some(span_fetch::wide_fetch_sql(
                &config.traces_table,
                &config.spans_table,
                &config.resources_table,
                hex32,
            )),
        );
    }
    if index_rows != 0 && spans_non_empty {
        return (FetchRoute::Indexed, None);
    }
    match window {
        None => (FetchRoute::Indexed, None),
        Some(window) => (
            FetchRoute::Fallback,
            Some(span_fetch::fallback_fetch_sql(
                &config.spans_table,
                &config.resources_table,
                hex32,
                window,
            )),
        ),
    }
}

fn metrics_settings(config: &TraceReadConfig) -> QuerySettings {
    // `max_query_size` is already present, inherited from `search_settings`
    // — set again here (idempotent, `QuerySettings::set` overrides rather
    // than duplicates) so the setting's presence is explicit at this call
    // site too, per issue #35's plan.
    let base = search_settings(config)
        .set("max_rows_in_set", TRACE_METRICS_MAX_SET_ROWS)
        .set("max_bytes_in_set", TRACE_METRICS_MAX_SET_BYTES)
        .set("set_overflow_mode", "throw")
        .set("max_query_size", crate::querytext::MAX_QUERY_TEXT_BYTES);
    if config.distributed {
        base.set("distributed_product_mode", "local")
    } else {
        base
    }
}

/// The settings the metrics statement runs under (issue #594 part 5):
/// [`metrics_settings`] with no set limit, and with `final = 1` when the
/// statement keeps it (issue #601 part 4a: it reads the nested-set
/// numbering; every other statement keeps one row per span identity
/// itself). The statement builds the sets of the seven trace-level and
/// nested-set fields, bounded by the read memory ceiling and the scan
/// budget, not by a set limit.
fn statement_settings(config: &TraceReadConfig, keeps_final: bool) -> QuerySettings {
    let settings = metrics_settings(config);
    let settings = if keeps_final {
        with_final(settings)
    } else {
        settings
    };
    settings
        .set("max_rows_in_set", 0u64)
        .set("max_bytes_in_set", 0u64)
}

/// The Layer-1 settings the §4.5 service-graph query carries (issue #173):
/// the full search budget set ([`search_settings`] — `max_rows_to_read =
/// scan_budget_rows` throw bounds the join's scan + hash-table cost, plus
/// the read/result byte ceilings and `max_block_size`), with
/// `distributed_product_mode='local'` added in clustered mode so the
/// within-`conn_type` `pair_id` join executes per shard (halves co-shard on
/// `cityHash64(trace_id)`, so each shard's local join is complete and the
/// initiator merges only per-`(client, server, conn_type)` partial states).
/// The graph query carries no `IN`-set, so the metrics set-limits are
/// deliberately omitted.
fn graph_settings(config: &TraceReadConfig) -> QuerySettings {
    let base = search_settings(config);
    if config.distributed {
        // Issue #599 part A2: with `1` the initiator's own shard runs
        // in-process, `shardNum()` is not folded to a constant there, and
        // that shard's span read loses its primary-key pruning. Part B:
        // `in_order` sends a shard's totals and spans read and its marker
        // fan-out to the same replica unless one is down, so the markers the
        // read trusts are that replica's own.
        base.set("distributed_product_mode", "local")
            .set("prefer_localhost_replica", 0u64)
            .set("load_balancing", "in_order")
    } else {
        base
    }
}

/// Rewrites `series.samples` as exactly one sample per axis label, in
/// ascending label order, taking the framed value where the query
/// produced one and `0.0` where it did not.
///
/// Every label the SQL can produce is an axis label by construction: the
/// range window's instants are `(aS - step, aE]`, whose right-closed
/// bucket labels run from `aS` to `aE` inclusive — the axis exactly. A
/// framed sample on some other label would therefore be a defect, and
/// dropping it here is the honest handling: the axis is what the response
/// promises.
fn densify(series: &mut TraceMetricSeries, axis: super::metrics_plan::RangeAxis) {
    let framed: BTreeMap<i64, f64> = series.samples.iter().copied().collect();
    let mut dense = Vec::with_capacity(axis.points);
    for i in 0..axis.points {
        let label = axis.label_ms(i);
        dense.push((label, framed.get(&label).copied().unwrap_or(0.0)));
    }
    series.samples = dense;
}

/// The string value of `series`' label `key`, if it carries one.
///
/// A comparison's series are labelled `MetricLabel::str(<key>, <value>)`,
/// so this is the inverse of that construction.
fn series_label_value<'a>(series: &'a TraceMetricSeries, key: &str) -> Option<&'a str> {
    series.labels.iter().find(|l| l.key == key).and_then(|l| {
        match &l.value {
            MetricLabelValue::Str(v) => Some(v.as_str()),
            // A by-key group value is always rendered as a string; the
            // numeric label forms belong to `p=` and `__bucket=`, which
            // are never grouped shapes.
            _ => None,
        }
    })
}

/// Appends one exemplar per sampled span of a bucket row to `collected`,
/// stamped with series `i`'s own value at that bucket.
///
/// A bucket the series does not carry is skipped outright (issue #477
/// wave 2): the exemplar statement and the range statement read the same
/// rows over the same window, so a hit here would mean the two disagreed,
/// and the old `unwrap_or(0.0)` turned that disagreement into a
/// measured-looking zero.
fn push_bucket_exemplars(
    collected: &mut Vec<(usize, MetricExemplar)>,
    i: usize,
    value_at: &BTreeMap<i64, f64>,
    t_ms: i64,
    sampled: Vec<([u8; 16], i64)>,
) {
    let Some(&value) = value_at.get(&t_ms) else {
        return;
    };
    for (trace_id, ts_ns) in sampled {
        collected.push((
            i,
            MetricExemplar {
                labels: vec![MetricLabel::str("trace:id", hex16(&trace_id))],
                value,
                timestamp_ms: ts_ns / 1_000_000,
            },
        ));
    }
}

/// The `(__meta_type, attribute key)` pair identifying a `compare()`
/// TOTALS series, or `None` for any other series — the inverse of
/// [`frame_compare_rows`]' total-series labels and the join column for a
/// comparison exemplar.
fn compare_total_series_key(series: &TraceMetricSeries) -> Option<(&str, &str)> {
    let kind = series_label_value(series, "__meta_type")?;
    if kind != "baseline_total" && kind != "selection_total" {
        return None;
    }
    let key = series.labels.iter().find(|l| l.key != "__meta_type")?;
    Some((kind, key.key.as_str()))
}

/// Frames the `compare()` statement's rows (issue #597): the kept values,
/// typed by their stored type; per key and side the denominators (D9) — for
/// a key of [`WELL_KNOWN_COMPARE_KEYS`] `<side>_total` the side's
/// population and `key=nil` the spans without the key, for any other key
/// `<side>_total` the key's untrimmed sum and no `key=nil`; a well-known key
/// no span carries as `nil` and total the population. All-zero series are
/// dropped and, on a range, every series densified to the axis; then each
/// key's exemplars go onto its side's total at their bucket, thinned to the
/// budget over the whole answer.
///
/// [`WELL_KNOWN_COMPARE_KEYS`]: super::metrics_plan::WELL_KNOWN_COMPARE_KEYS
fn frame_compare_rows(
    plan: &TraceMetricsPlan,
    rows: Vec<super::spans::metrics::CompareStatementRow>,
    axis: Option<super::metrics_plan::RangeAxis>,
) -> Result<TraceMetricsResult, ReadError> {
    const SIDES: [&str; 2] = ["baseline", "selection"];
    const TOTALS: [&str; 2] = ["baseline_total", "selection_total"];
    let side = |s: u8| usize::from(s.min(1));
    // Per side: the population and each key's untrimmed sums, per bucket.
    let mut population: [BTreeMap<i64, u64>; 2] = [BTreeMap::new(), BTreeMap::new()];
    let mut sums: BTreeMap<String, [BTreeMap<i64, u64>; 2]> = BTreeMap::new();
    // (side, key, its per-bucket exemplars `(bucket, (trace id, start ns))`).
    type KeyExemplars = Vec<(i64, ([u8; 16], i64))>;
    let mut exemplars: Vec<(usize, String, KeyExemplars)> = Vec::new();
    let mut values: BTreeMap<String, Vec<TraceMetricSeries>> = BTreeMap::new();
    for row in rows {
        let at = side(row.s);
        if row.g == 1 && row.k.is_empty() {
            population[at].extend(row.pts.iter().copied());
        } else if row.g == 1 {
            sums.entry(row.k.clone()).or_default()[at].extend(row.pts.iter().copied());
            if !row.ex.is_empty() {
                exemplars.push((at, row.k, row.ex));
            }
        } else {
            let value = metrics_statement::typed_value(&row.v, &row.y)?;
            values
                .entry(row.k.clone())
                .or_default()
                .push(TraceMetricSeries {
                    labels: vec![
                        MetricLabel::str("__meta_type", SIDES[at]),
                        MetricLabel { key: row.k, value },
                    ],
                    samples: row.pts.iter().map(|&(t, n)| (t, n as f64)).collect(),
                    exemplars: Vec::new(),
                });
        }
    }
    let well_known = super::metrics_plan::WELL_KNOWN_COMPARE_KEYS;
    let mut keys: BTreeSet<String> = sums.keys().cloned().collect();
    keys.extend(well_known.iter().map(|k| k.to_string()));
    let empty: [BTreeMap<i64, u64>; 2] = [BTreeMap::new(), BTreeMap::new()];
    let mut series: Vec<TraceMetricSeries> = Vec::new();
    for key in &keys {
        series.extend(values.remove(key).unwrap_or_default());
        let of_key = sums.get(key).unwrap_or(&empty);
        let nil_label = |meta: &str| {
            vec![
                MetricLabel::str("__meta_type", meta),
                MetricLabel::str(key.clone(), "nil"),
            ]
        };
        let known = well_known.contains(&key.as_str());
        if known {
            for at in 0..2 {
                series.push(TraceMetricSeries {
                    labels: nil_label(SIDES[at]),
                    samples: population[at]
                        .iter()
                        .map(|(&t, &n)| {
                            let carried = of_key[at].get(&t).copied().unwrap_or(0);
                            (t, n.saturating_sub(carried) as f64)
                        })
                        .collect(),
                    exemplars: Vec::new(),
                });
            }
        }
        for at in 0..2 {
            let total = if known { &population[at] } else { &of_key[at] };
            series.push(TraceMetricSeries {
                labels: nil_label(TOTALS[at]),
                samples: total.iter().map(|(&t, &n)| (t, n as f64)).collect(),
                exemplars: Vec::new(),
            });
        }
    }
    series.retain(|s| s.samples.iter().any(|(_, v)| *v != 0.0));
    let mut result = TraceMetricsResult { series };
    let Some(axis) = axis else {
        return Ok(result);
    };
    for s in &mut result.series {
        densify(s, axis);
    }
    if plan.exemplar_budget() == 0 || exemplars.is_empty() {
        return Ok(result);
    }
    let value_at: Vec<BTreeMap<i64, f64>> = result
        .series
        .iter()
        .map(|s| s.samples.iter().copied().collect())
        .collect();
    let index: HashMap<(&str, &str), usize> = result
        .series
        .iter()
        .enumerate()
        .filter_map(|(i, s)| compare_total_series_key(s).map(|k| (k, i)))
        .collect();
    let mut collected: Vec<(usize, MetricExemplar)> = Vec::new();
    for (at, key, ex) in &exemplars {
        let Some(&i) = index.get(&(TOTALS[*at], key.as_str())) else {
            continue;
        };
        for &(t, (trace_id, start_ns)) in ex {
            push_bucket_exemplars(
                &mut collected,
                i,
                &value_at[i],
                t,
                vec![(trace_id, start_ns)],
            );
            decimate_if_full(&mut collected);
        }
    }
    thin_collected_exemplars(&mut collected, plan.exemplar_budget());
    for (i, ex) in collected {
        result.series[i].exemplars.push(ex);
    }
    Ok(result)
}

/// The ceiling on the IN-FLIGHT exemplar list, before the final thinning
/// (issue #477 wave 2).
///
/// The exemplar statement returns one row per (bucket × series identity),
/// and since this wave the identity is per shape: a `__bucket` per log2
/// duration bucket that occurred, an `(is_sel, akey)` per side and
/// attribute key. The row count is therefore no longer bounded by the
/// bucket grid alone — at the interval cap a comparison over a wide
/// attribute universe can return orders of magnitude more rows than a
/// count does, and every sample on them would be materialized before the
/// stride ran. The response keeps at most `MAX_EXEMPLARS`, so holding
/// millions to pick 100 is pure waste as well as a memory risk.
const EXEMPLAR_COLLECTION_CEILING: usize = 8_192;

/// Halves `collected` in place when it reaches
/// [`EXEMPLAR_COLLECTION_CEILING`], keeping every other entry.
///
/// Halving rather than truncating, and rather than thinning straight to
/// the budget: keeping every other entry of what has been seen so far
/// leaves the survivors spread evenly over it, so repeated halving over a
/// long stream is still an even sample of the whole stream. Truncating
/// would keep only the earliest buckets and thinning to the budget each
/// time would let the tail crowd out the head — both are the bias the
/// final stride exists to avoid. Deterministic: the same row order always
/// yields the same list.
///
/// **Halving can discard an exemplar the final stride would have kept**
/// (recorded on the wave-3 review). The two passes index different
/// lists: a halved list has renumbered every survivor, so
/// [`thin_collected_exemplars`]'s `i * len / budget` lands on a different
/// original row than it would have on the unhalved stream. Worked at the
/// committed constants — ceiling 8 192, at most 50 samples on one
/// returned row, so at most 8 241 entries exist before a halving — the
/// stride's `i = 3` position is original index 247 without halving and
/// 246 with it. **What halving does not change is the COUNT**: the final
/// pass returns exactly the budget whenever at least that many exemplars
/// survive, and every available one when fewer do. The bias the ceiling
/// trades away is which representative of a neighbourhood is shown, not
/// how many or from where in the window — both passes keep an even
/// spread over the stream.
fn decimate_if_full(collected: &mut Vec<(usize, MetricExemplar)>) {
    if collected.len() < EXEMPLAR_COLLECTION_CEILING {
        return;
    }
    let mut i = 0usize;
    collected.retain(|_| {
        let keep = i.is_multiple_of(2);
        i += 1;
        keep
    });
}

/// Reduces a bucket-ordered exemplar list to at most `budget` entries by
/// even stride, keeping index `i * len / budget` for `i in 0..budget`.
///
/// A total budget cannot be enforced in the SQL, because
/// `groupArraySample`'s `k` is per group and the number of occupied
/// groups is not known until the rows come back. Taking an even stride
/// rather than the first `budget` entries keeps the surviving exemplars
/// spread across the window, which is what a panel draws them on; the
/// stride is exact integer arithmetic, so it is deterministic and the
/// same list always thins the same way.
///
/// The list is carried as `(series index, exemplar)` pairs (issue #477
/// wave 2) so one stride covers the WHOLE response: the budget is a total
/// (ruling 1 on issue #477), and thinning each grouped series separately
/// would multiply it by the number of groups.
fn thin_collected_exemplars(collected: &mut Vec<(usize, MetricExemplar)>, budget: u32) {
    let budget = budget as usize;
    if budget == 0 {
        collected.clear();
        return;
    }
    let len = collected.len();
    if len <= budget {
        return;
    }
    let kept: Vec<(usize, MetricExemplar)> = (0..budget)
        .map(|i| collected[i * len / budget].clone())
        .collect();
    *collected = kept;
}

/// Lowercase hex of a 16-byte trace id (the `trace:id` exemplar label).
fn hex16(bytes: &[u8; 16]) -> String {
    let mut s = String::with_capacity(32);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::traces::spans::fetch::BUCKET_SET_CAP;

    /// Issue #398: a distinctive, non-default
    /// `reader.traceql_read_max_memory_bytes` for these tests —
    /// deliberately unequal to every other byte setting in the config
    /// below, so it cannot be confused for one in an assertion.
    const TEST_READ_MEM: u64 = 7_654_321;

    /// Issue #477 wave 2: the in-flight exemplar list stays under its
    /// ceiling however many rows the statement returns, and what survives
    /// is still spread over the whole stream.
    ///
    /// The stream is deliberately much longer than the ceiling — the
    /// shape a comparison over a wide attribute universe produces — so
    /// the halving runs several times rather than once.
    #[test]
    fn the_exemplar_collection_stays_under_its_ceiling_and_stays_even() {
        let mut collected: Vec<(usize, MetricExemplar)> = Vec::new();
        let n = EXEMPLAR_COLLECTION_CEILING * 9;
        for i in 0..n {
            collected.push((
                0,
                MetricExemplar {
                    labels: vec![MetricLabel::str("trace:id", format!("{i:032x}"))],
                    value: 1.0,
                    timestamp_ms: i as i64,
                },
            ));
            decimate_if_full(&mut collected);
            assert!(
                collected.len() <= EXEMPLAR_COLLECTION_CEILING,
                "the list breached its ceiling at row {i}: {}",
                collected.len()
            );
        }
        // Spread, not a prefix and not a suffix: the survivors must reach
        // both ends of the stream. A truncating bound keeps only the head
        // and a thin-to-budget bound keeps only the tail; either would
        // fail here.
        let first = collected.first().expect("survivors").1.timestamp_ms;
        let last = collected.last().expect("survivors").1.timestamp_ms;
        assert_eq!(first, 0, "the head of the stream must survive");
        assert!(
            last as usize > n - EXEMPLAR_COLLECTION_CEILING,
            "the tail of the stream must survive, last was {last} of {n}"
        );
        // …and the final stride still lands on the budget.
        thin_collected_exemplars(&mut collected, 100);
        assert_eq!(collected.len(), 100);
    }

    #[test]
    fn trace_read_config_is_cloneable_and_debuggable() {
        let config = TraceReadConfig {
            spans_table: "spans".to_string(),
            error_spans_table: "error_spans".to_string(),
            traces_table: "traces".to_string(),
            resources_table: "resources".to_string(),
            scan_budget_rows: 50_000_000,
            max_depth: 64,
            max_series: 1_000,
            read_max_memory_bytes: TEST_READ_MEM,
            distributed: false,
            skip_unavailable_shards: false,
        };
        let clone = config.clone();
        assert_eq!(clone.spans_table, "spans");
        assert!(format!("{config:?}").contains("spans_table: \"spans\""));
    }

    fn cfg() -> TraceReadConfig {
        TraceReadConfig {
            spans_table: "spans".to_string(),
            error_spans_table: "error_spans".to_string(),
            traces_table: "traces".to_string(),
            resources_table: "resources".to_string(),
            scan_budget_rows: 1_000,
            max_depth: 64,
            max_series: 1_000,
            read_max_memory_bytes: TEST_READ_MEM,
            distributed: false,
            skip_unavailable_shards: false,
        }
    }

    /// `F-17`: **`fetch_settings`' entry set, exactly**, through
    /// `QuerySettings::entries` in insertion order.
    ///
    /// An exact list is what makes an accidental addition fail. Two
    /// absences are asserted as well as the twelve entries, each for its
    /// own reason: `distributed_product_mode`, because neither statement
    /// contains a subquery over a `Distributed` table for it to act on;
    /// and `load_balancing`, because a replica preference puts two reads
    /// on one host, which is not one position, so it closes none of the
    /// visibility states it was proposed for.
    #[test]
    fn fetch_settings_carries_exactly_the_clustered_budget_set_and_no_final() {
        let clustered = TraceReadConfig {
            distributed: true,
            skip_unavailable_shards: true,
            ..cfg()
        };
        let got: Vec<(String, String)> = fetch_settings(&clustered)
            .entries()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let mem = TEST_READ_MEM.to_string();
        // `QuerySettings::set` keeps an existing key's position and appends
        // a new one, so the answer pins (#603 follow-up) the clustered base
        // does not already carry follow it, in `ANSWER_PIN_NAMES` order.
        let want: Vec<(String, String)> = [
            ("optimize_skip_unused_shards", "1"),
            ("optimize_distributed_group_by_sharding_key", "1"),
            ("distributed_aggregation_memory_efficient", "1"),
            ("prefer_localhost_replica", "1"),
            ("skip_unavailable_shards", "1"),
            (
                "skip_unavailable_shards_mode",
                "unavailable_or_table_missing",
            ),
            ("distributed_group_by_no_merge", "0"),
            ("enable_parallel_replicas", "0"),
            ("max_parallel_replicas", "1"),
            ("distinct_overflow_mode", "throw"),
            ("group_by_overflow_mode", "throw"),
            ("join_overflow_mode", "throw"),
            ("read_overflow_mode", "throw"),
            ("read_overflow_mode_leaf", "throw"),
            ("result_overflow_mode", "throw"),
            ("set_overflow_mode", "throw"),
            ("sort_overflow_mode", "throw"),
            ("timeout_overflow_mode", "throw"),
            ("timeout_overflow_mode_leaf", "throw"),
            ("transfer_overflow_mode", "throw"),
            ("max_rows_to_read", "1000"),
            ("max_bytes_to_read", "53687091200"),
            ("max_query_size", "8388608"),
            // From the fixture's own ceiling, not restated: a literal here
            // would pass against a config that carried something else.
            ("max_memory_usage", mem.as_str()),
            ("max_bytes_before_external_group_by", "0"),
        ]
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
        assert_eq!(got, want, "the fetch's entry set, exactly and in order");

        let keys: Vec<&str> = got.iter().map(|(k, _)| k.as_str()).collect();
        assert!(
            !keys.contains(&"distributed_product_mode"),
            "the setting has no subquery over a Distributed table to act on"
        );
        assert!(
            !keys.contains(&"load_balancing"),
            "a replica preference closes none of the visibility states"
        );

        // Unclustered: the 17 answer pins, then the same budgets, with no
        // clustered-reader block at all and no `final`.
        let local: Vec<String> = fetch_settings(&cfg())
            .entries()
            .map(|(k, _)| k.to_string())
            .collect();
        let mut want_local: Vec<&str> = pulsus_clickhouse::ANSWER_PIN_NAMES.to_vec();
        want_local.extend([
            "max_rows_to_read",
            "max_bytes_to_read",
            "max_query_size",
            "max_memory_usage",
            "max_bytes_before_external_group_by",
        ]);
        assert_eq!(local, want_local);
    }

    /// `F-19`: **the route flag, one sub-case per arm, with the second
    /// statement each arm renders.**
    ///
    /// `statements` is `1` when the branch returns no second statement and
    /// `2` when it returns one, so the count is derived here rather than
    /// asserted separately.
    ///
    /// **The second and third sub-cases are the load-bearing pair**: both
    /// give `statements == 2`, which is why the count cannot carry the
    /// signal and the enum exists.
    ///
    /// The second sub-case's text assertion says the **wide builder ran**
    /// and that it carries its own bounds — not that its text is correct.
    /// Both sides of a comparison against that builder's own output would
    /// come from the same function, so it would hold for any text the
    /// function produces; the byte-freeze case in
    /// [`crate::traces::spans::fetch`] is what holds the content.
    #[test]
    fn the_route_flag_names_which_statement_answered() {
        const HEX: &str = "aa000000000000000000000000000001";
        let config = cfg();
        let window = Some(FetchWindow {
            start_ns: 1_699_999_999_000_000_000,
            end_ns: 1_700_000_002_000_000_000,
        });

        // (a) an indexed answer: a complete bucket set and a non-empty
        // span array, so one statement answers.
        let (route, second) = fetch_route_and_second(&config, HEX, 1, 0, true, None);
        assert_eq!(route, FetchRoute::Indexed);
        assert!(second.is_none(), "the indexed route issues one statement");

        // (b) a bucket set that may have truncated. `4097` and not
        // `4096`: the value an uncapped union of a truncated row with one
        // further bucket produces, so this sub-case also pins that the
        // test is `>=` and not `==`.
        let (route, second) = fetch_route_and_second(&config, HEX, 1, 4097, false, None);
        assert_eq!(route, FetchRoute::TruncatedSet);
        let wide = second.expect("the truncated-set route issues a second statement");
        assert!(
            wide.contains("min(start_ns)"),
            "the wide builder ran and reads its own bounds:\n{wide}"
        );

        // (c) no index row, no spans, and a window: the fallback answers,
        // also at two statements.
        let (route, second) = fetch_route_and_second(&config, HEX, 0, 0, false, window);
        assert_eq!(route, FetchRoute::Fallback);
        let fallback = second.expect("the fallback route issues a second statement");
        assert!(
            fallback.contains("start_ns >= 1699999999000000000"),
            "the fallback renders the request's own window, which statement 1 never \
             renders in any form:\n{fallback}"
        );

        // (d) no index row, no spans and NO window: one statement, and the
        // answer is empty rather than a trace_id-only predicate over every
        // partition in retention.
        let (route, second) = fetch_route_and_second(&config, HEX, 0, 0, false, None);
        assert_eq!(route, FetchRoute::Indexed);
        assert!(
            second.is_none(),
            "with no window the fetch answers empty at one statement"
        );

        // (e) an index row with an EMPTY span array and a window: the
        // state where the per-trace table holds a row and the span table
        // holds nothing. Both halves of the second condition are
        // load-bearing and this is the half that is not `index_rows`.
        let (route, second) = fetch_route_and_second(&config, HEX, 1, 0, false, window);
        assert_eq!(route, FetchRoute::Fallback);
        assert!(second.is_some());

        // (f) the truncation branch is tested FIRST, and it needs no
        // `index_rows` guard: with no per-trace row the bucket set is
        // empty and its length is 0, so the test is false for an absent
        // trace by construction. With a window supplied, an empty
        // statement-1 array on the truncated route must NOT fall through
        // to the fallback.
        let (route, _) = fetch_route_and_second(&config, HEX, 1, 4096, false, window);
        assert_eq!(
            route,
            FetchRoute::TruncatedSet,
            "an empty array on the truncated route must not reach the window fallback"
        );
        assert_eq!(BUCKET_SET_CAP, 4096, "the cap the branch tests against");
    }

    #[test]
    fn code_158_maps_to_the_trace_row_budget_on_the_trace_path() {
        let e = ChError::Server {
            code: 158,
            message: "Limit for rows to read exceeded".to_string(),
        };
        match map_trace_read_error(e, &cfg()) {
            ReadError::QueryTooBroad(TooBroadReason::TraceScanBudgetRows { budget_rows }) => {
                assert_eq!(budget_rows, 1_000);
            }
            other => panic!("expected TraceScanBudgetRows, got {other:?}"),
        }
    }

    #[test]
    fn code_307_maps_to_the_read_side_byte_budget() {
        let e = ChError::Server {
            code: 307,
            message: "Limit for bytes to read exceeded".to_string(),
        };
        match map_trace_read_error(e, &cfg()) {
            ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes { budget_bytes, .. }) => {
                assert_eq!(budget_bytes, TRACE_READ_BYTES_BUDGET);
            }
            other => panic!("expected ScanBudgetBytes, got {other:?}"),
        }
    }

    #[test]
    fn code_396_maps_to_the_result_side_byte_ceiling() {
        let e = ChError::Server {
            code: 396,
            message: "Limit for result exceeded".to_string(),
        };
        match map_trace_read_error(e, &cfg()) {
            ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes { budget_bytes, .. }) => {
                assert_eq!(budget_bytes, TRACE_MAX_RESULT_BYTES);
            }
            other => panic!("expected ScanBudgetBytes, got {other:?}"),
        }
    }

    /// Issue #382, the user-visible defect: the three tests above hand
    /// `map_trace_read_error` a `ChError::Server` they built themselves, so
    /// none of them can see how the code is READ off the wire — and that is
    /// where the defect was. These start from the `BadResponse` the
    /// `clickhouse` crate hands us when ClickHouse raised the limit AFTER it
    /// had already written output.
    ///
    /// The bodies are verbatim captures from 26.3.17.110 (`SELECT
    /// toString(number) AS v FROM numbers(100000000)` with
    /// `max_result_bytes=1000000, result_overflow_mode=throw` for 396 and
    /// `max_bytes_to_read=1100000` for 307), prefixed with the
    /// header-derived `Code: N` that the ADR 0007 vendored patch
    /// (`vendor/clickhouse/PATCHES.md`) puts in front of them. That prefix is
    /// the whole fix: the result bytes after it are tenant data and cannot be
    /// parsed for a code soundly, so without it these map to 500 `internal`
    /// rather than the 422 `query_too_broad` the query deserves.
    ///
    /// 307 is here because 396 is not the only affected arm — every code in
    /// this mapper reaches it through the same single parse.
    #[test]
    fn a_limit_raised_after_output_was_written_still_maps_to_its_byte_budget() {
        let body_396 = "\u{1}\u{1}v\u{6}StringCode: 396. DB::Exception: Limit for result \
                        exceeded, max bytes: 976.56 KiB, current bytes: 1.64 MiB. \
                        (TOO_MANY_ROWS_OR_BYTES) (version 26.3.17.110 (official build))";
        let body_307 = "\u{1}\u{1}v\u{6}StringCode: 307. DB::Exception: Limit for rows or bytes \
                        to read exceeded, max bytes: 1.05 MiB, current bytes: 1.50 MiB: While \
                        executing NumbersRange. (TOO_MANY_BYTES) (version 26.3.17.110 (official \
                        build))";
        // Verbatim captures: the lengths are pinned so an edit that quietly
        // reshapes a fixture fails here instead of weakening the case.
        assert_eq!(body_396.len(), 174);
        assert_eq!(body_307.len(), 209);
        for (code, body, expected) in [
            (396, body_396, TRACE_MAX_RESULT_BYTES),
            (307, body_307, TRACE_READ_BYTES_BUDGET),
        ] {
            // Unpatched, the code is not in a trustworthy position and the
            // client gets the generic passthrough. Stated, not hidden.
            let raw = ChError::from(clickhouse::error::Error::BadResponse(body.to_string()));
            assert!(matches!(
                map_trace_read_error(raw, &cfg()),
                ReadError::Clickhouse(_)
            ));

            let patched = format!("Code: {code}\n{body}");
            match map_trace_read_error(
                ChError::from(clickhouse::error::Error::BadResponse(patched)),
                &cfg(),
            ) {
                ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes {
                    budget_bytes, ..
                }) => assert_eq!(budget_bytes, expected),
                other => panic!("expected ScanBudgetBytes, got {other:?}"),
            }
        }
    }

    #[test]
    fn code_191_maps_to_the_metrics_set_budget_on_the_metrics_path_only() {
        let e = || ChError::Server {
            code: 191,
            message: "Limit for size of set exceeded".to_string(),
        };
        match map_trace_metrics_error(e(), &cfg()) {
            ReadError::QueryTooBroad(TooBroadReason::TraceMetricsSetRows { max_set_rows }) => {
                assert_eq!(max_set_rows, TRACE_METRICS_MAX_SET_ROWS);
            }
            other => panic!("expected TraceMetricsSetRows, got {other:?}"),
        }
        // The search-path mapper never maps 191 — the set limits are set
        // only on metrics queries, and the reasons stay unconflated.
        assert!(matches!(
            map_trace_read_error(e(), &cfg()),
            ReadError::Clickhouse(_)
        ));
    }

    #[test]
    fn the_metrics_mapper_delegates_everything_else_to_the_shared_mapper() {
        let e = ChError::Server {
            code: 158,
            message: "Limit for rows to read exceeded".to_string(),
        };
        assert!(matches!(
            map_trace_metrics_error(e, &cfg()),
            ReadError::QueryTooBroad(TooBroadReason::TraceScanBudgetRows { budget_rows: 1_000 })
        ));
        let t = ChError::Timeout("deadline".to_string());
        assert!(matches!(
            map_trace_metrics_error(t, &cfg()),
            ReadError::Clickhouse(_)
        ));
    }

    #[test]
    fn metrics_settings_carry_the_set_limits_and_gate_the_local_product_mode() {
        let local = format!("{:?}", metrics_settings(&cfg()));
        for needle in [
            "max_rows_in_set",
            "max_bytes_in_set",
            "set_overflow_mode",
            "max_rows_to_read",
            "max_bytes_to_read",
            "max_result_bytes",
            "max_query_size",
        ] {
            assert!(local.contains(needle), "missing {needle} in {local}");
        }
        assert!(
            !local.contains("distributed_product_mode"),
            "the local-product rewrite is clustered-only: {local}"
        );
        let mut clustered_cfg = cfg();
        clustered_cfg.distributed = true;
        let clustered = format!("{:?}", metrics_settings(&clustered_cfg));
        assert!(clustered.contains("distributed_product_mode"));
        assert!(clustered.contains("local"));
    }

    /// Issue #594 part 5 (D3): the metrics statement builds the sets of
    /// the seven fields under the read memory ceiling, with no set limit;
    /// clustered, it keeps the local product mode the sets need.
    #[test]
    fn the_statement_builds_sets_under_the_memory_ceiling() {
        let s = statement_settings(&cfg(), false);
        assert_eq!(s.get("max_rows_in_set"), Some("0"));
        assert_eq!(s.get("max_bytes_in_set"), Some("0"));
        assert_eq!(
            s.get("max_memory_usage"),
            Some(TEST_READ_MEM.to_string().as_str())
        );
        assert_eq!(s.get("distributed_product_mode"), None);
        let mut clustered = cfg();
        clustered.distributed = true;
        assert_eq!(
            statement_settings(&clustered, false).get("distributed_product_mode"),
            Some("local")
        );
    }

    /// T14 (issue #601 part 4a): a metrics statement runs under `final`
    /// only when it reads the nested-set numbering; every other statement
    /// removes a stored span's copies itself.
    #[test]
    fn a_metrics_statement_keeps_final_only_when_it_numbers() {
        use crate::traces::metrics_plan::MetricsParams;
        use crate::traces::spans::metrics::{StatementTables, plan_trace_metrics_routed};
        let ctx = crate::traces::metrics_plan::MetricsCtx {
            scan_budget_rows: 50_000_000,
            max_series: 1_000,
            distributed: false,
            skip_unavailable_shards: false,
        };
        let params = MetricsParams {
            start_ns: 1_700_000_000_000_000_000,
            end_ns: 1_700_000_600_000_000_000,
            step_ms: 60_000,
            exemplars: None,
            floor_ns: i64::MIN,
        };
        let tables = StatementTables {
            spans: "spans",
            error_spans: "error_spans",
            resources: "resources",
            traces: "traces",
            indexed: &[],
        };
        for (q, want) in [
            ("{ status = error } | count_over_time()", None),
            (
                "{ status = error } | count_over_time() by(nestedSetParent)",
                Some("1"),
            ),
        ] {
            let routed = plan_trace_metrics_routed(
                &pulsus_traceql::parse(q).expect("parses"),
                &params,
                &ctx,
                &tables,
            )
            .unwrap_or_else(|e| panic!("{q}: {e}"));
            let stmt = routed.statement();
            assert_eq!(
                statement_settings(&cfg(), stmt.keeps_final()).get("final"),
                want,
                "{q}"
            );
        }
    }

    /// Issue #173: `graph_settings` carries the search row/byte budget and
    /// gates `distributed_product_mode='local'` on clustered mode only (so
    /// the `pair_id` join runs shard-local), and never carries the metrics
    /// set-limits (the graph query has no `IN`-set).
    #[test]
    fn graph_settings_carry_the_scan_budget_and_gate_the_local_product_mode() {
        let local = format!("{:?}", graph_settings(&cfg()));
        for needle in ["max_rows_to_read", "max_bytes_to_read", "max_result_bytes"] {
            assert!(local.contains(needle), "missing {needle} in {local}");
        }
        assert!(
            !local.contains("distributed_product_mode"),
            "the local-product rewrite is clustered-only: {local}"
        );
        assert!(
            !local.contains("max_rows_in_set"),
            "the graph query has no IN-set: {local}"
        );
        let mut clustered = cfg();
        clustered.distributed = true;
        let clustered = format!("{:?}", graph_settings(&clustered));
        assert!(clustered.contains("distributed_product_mode"));
        assert!(clustered.contains("local"));
        assert!(clustered.contains("optimize_skip_unused_shards"));
    }

    /// U4 (issue #599 part A2): the clustered graph read never runs the
    /// initiator's shard in-process, and the single-node read is untouched.
    #[test]
    fn the_clustered_graph_read_prefers_no_local_replica() {
        let local = graph_settings(&cfg());
        assert_eq!(local.get("prefer_localhost_replica"), None);
        let mut clustered = cfg();
        clustered.distributed = true;
        assert_eq!(
            graph_settings(&clustered).get("prefer_localhost_replica"),
            Some("0")
        );
    }

    /// KU1 (issue #599 part B): the clustered graph read sends a shard's
    /// totals and spans read and its marker fan-out to the same replica, the
    /// first listed, unless that one is down; the single-node read is
    /// untouched.
    #[test]
    fn the_clustered_graph_read_takes_the_replicas_in_order() {
        let local = graph_settings(&cfg());
        assert_eq!(local.get("load_balancing"), None);
        let mut clustered = cfg();
        clustered.distributed = true;
        assert_eq!(
            graph_settings(&clustered).get("load_balancing"),
            Some("in_order")
        );
    }

    /// U2 (issue #599): the clustered graph's target is the first two
    /// quoted arguments of the routing table's `Distributed(…)` definition,
    /// as `system.tables.engine_full` spells it, and nothing else is a
    /// target.
    #[test]
    fn the_graph_target_comes_from_the_distributed_definition() {
        let target = |c: &str, d: &str| GraphTarget {
            cluster: c.to_string(),
            database: d.to_string(),
        };
        for (engine_full, want) in [
            (
                "Distributed('c1', 'pulsus', 'spans', cityHash64(trace_id))",
                Some(target("c1", "pulsus")),
            ),
            (
                "Distributed('c1', 'pulsus', 'spans', cityHash64(trace_id)) \
                 SETTINGS fsync_after_insert = 1, fsync_directories = 1",
                Some(target("c1", "pulsus")),
            ),
            (
                "Distributed('q', 'store', 'spans')",
                Some(target("q", "store")),
            ),
            (
                "ReplacingMergeTree PARTITION BY toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') \
                 ORDER BY (intDiv(start_ns, 300000000000), trace_id)",
                None,
            ),
            ("MergeTree", None),
            ("Distributed(c1)", None),
            ("", None),
        ] {
            assert_eq!(
                graph_target_from_engine(engine_full),
                want,
                "the target named by {engine_full:?}"
            );
        }
    }

    /// Issue #133 AC5: `search_settings` and `catalog_settings` carry
    /// `max_rows_to_read` VERBATIM at the accepted minimum (1) and at the
    /// maximum config-accepted `reader.traceql_scan_budget_rows` — never
    /// ClickHouse's `0` (unlimited) sentinel, which would silently
    /// disable the trace scan budget.
    #[test]
    fn scan_budget_rows_pass_through_verbatim_at_the_accepted_min_and_ceiling() {
        for budget in [1u64, pulsus_config::TRACEQL_SCAN_BUDGET_ROWS_CEILING] {
            let mut c = cfg();
            c.scan_budget_rows = budget;
            let expected = budget.to_string();
            for s in [search_settings(&c), catalog_settings(&c)] {
                assert_eq!(
                    s.get("max_rows_to_read"),
                    Some(expected.as_str()),
                    "the row budget must pass through verbatim"
                );
                assert_ne!(s.get("max_rows_to_read"), Some("0"));
            }
        }
    }

    // The `__bucket` rendering pins moved to
    // `traces::log2_histogram`'s own tests with the function itself
    // (issue #252) — `bucket_seconds` is no longer an `exec.rs` local.

    #[test]
    fn a_timeout_is_never_reinterpreted_as_a_budget_error() {
        let e = ChError::Timeout("deadline".to_string());
        assert!(matches!(
            map_trace_read_error(e, &cfg()),
            ReadError::Clickhouse(_)
        ));
    }

    #[test]
    fn a_generic_server_error_passes_through_unmapped() {
        let e = ChError::Server {
            code: 62,
            message: "syntax error".to_string(),
        };
        assert!(matches!(
            map_trace_read_error(e, &cfg()),
            ReadError::Clickhouse(_)
        ));
    }

    #[test]
    fn byte_budget_trips_only_past_the_cap_and_releases_restore_headroom() {
        let mut budget = ByteBudget::new(100);
        assert!(budget.charge(60).is_ok());
        assert!(budget.charge(40).is_ok(), "exactly at the cap is fine");
        let err = budget.charge(1).unwrap_err();
        assert!(matches!(
            err,
            ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes {
                budget_bytes: 100,
                ..
            })
        ));
        // Round-3: the failed charge is atomic — it never counted, so
        // the counter reflects only live allocations (no phantoms).
        assert_eq!(budget.used(), 100);
        budget.release(40);
        assert_eq!(budget.used(), 60);
        assert!(budget.charge(40).is_ok());
    }

    #[test]
    fn byte_budget_aggregates_across_individually_small_charges() {
        // Round-5/6 finding: many individually sub-ceiling charges must
        // trip the single counter in aggregate.
        let mut budget = ByteBudget::new(1_000);
        for _ in 0..100 {
            let _ = budget.charge(10);
        }
        assert!(budget.charge(1).is_err());
    }

    /// `QuerySettings` has no public getters; its `Debug` rendering is
    /// the stable introspection surface for pinning the Layer-1 budget
    /// contract (final amendment).
    #[test]
    fn search_settings_pin_the_layer_1_budget_contract() {
        let rendered = format!("{:?}", search_settings(&cfg()));
        for expected in [
            "max_rows_to_read",
            "1000",
            "max_bytes_to_read",
            "read_overflow_mode",
            "throw",
            "max_result_bytes",
            "result_overflow_mode",
            "max_block_size",
            "4096",
            "max_query_size",
        ] {
            assert!(
                rendered.contains(expected),
                "missing {expected} in {rendered}"
            );
        }
        // `optimize_skip_unused_shards` is an answer pin, sent as `0`
        // (#603 follow-up); the rest of the §7 block stays clustered-only.
        assert!(
            !rendered.contains("prefer_localhost_replica"),
            "unclustered engines must not carry the §7 settings"
        );
    }

    fn tag_narrowing(
        q: &str,
        w: super::super::window_sql::WindowSql,
    ) -> super::super::spans::tags::TagNarrowing {
        super::super::spans::tags::narrowing_from_query(
            q,
            &super::super::spans::predicate::PredicateCtx {
                window: w,
                spans_table: "spans",
                resources_table: "resources",
                indexed: &[],
            },
        )
    }

    /// U6 (issue #598 part 2a): `R` is sized from the budget and the
    /// statement's table reads, and the store reads keep the scan budget's
    /// `throw` as a backstop, bound memory by a distinct cap, and pin
    /// `max_threads`.
    #[test]
    fn tag_store_settings_bound_rows_and_memory() {
        use crate::traces::spans::tags::{TagShape, ValueScope, cut_rows, table_reads};
        for (k, want) in [
            (2, 10_000_000),
            (3, 8_333_333),
            (4, 6_250_000),
            (5, 5_000_000),
        ] {
            assert_eq!(cut_rows(10_000_000, 50_000_000, k), want, "k = {k}");
        }
        assert_eq!(cut_rows(10_000_000, 1, 2), 1);
        assert_eq!(cut_rows(10_000, 50_000_000, 2), 10_000);
        let w = super::super::window_sql::WindowSql::start_closed_end_open(
            1_700_000_000_000_000_000,
            1_700_003_600_000_000_000,
        );
        let none = tag_narrowing(r#"{span.a="x"}"#, w);
        let trace = tag_narrowing("{traceDuration > 1s}", w);
        assert_eq!(table_reads(TagShape::Names, &none), 2);
        assert_eq!(table_reads(TagShape::Values(ValueScope::Span), &none), 2);
        assert_eq!(
            table_reads(TagShape::Values(ValueScope::Resource), &none),
            2
        );
        assert_eq!(
            table_reads(TagShape::Values(ValueScope::Unscoped), &none),
            3
        );
        assert_eq!(table_reads(TagShape::Values(ValueScope::Span), &trace), 4);
        assert_eq!(
            table_reads(TagShape::Values(ValueScope::Unscoped), &trace),
            5
        );
        let config = cfg();
        let s = tag_store_settings(&config);
        assert_eq!(
            s.get("max_rows_to_read"),
            Some(config.scan_budget_rows.to_string().as_str())
        );
        assert_eq!(s.get("read_overflow_mode"), Some("throw"));
        assert_eq!(s.get("max_rows_in_distinct"), Some("10000"));
        assert_eq!(s.get("distinct_overflow_mode"), Some("break"));
        assert_eq!(s.get("max_threads"), Some("4"));
        assert_eq!(s.get("distributed_product_mode"), None);
    }

    /// U8 (issue #598 part 2a): the cut size the statement functions
    /// compute reaches the statement.
    #[test]
    fn the_cut_size_reaches_the_statement() {
        let w = super::super::window_sql::WindowSql::start_closed_end_open(
            1_700_000_000_000_000_000,
            1_700_003_600_000_000_000,
        );
        let mut config = cfg();
        config.scan_budget_rows = 1_200_000;
        let ceiling = crate::traces::spans::tags::TAG_STORE_READ_ROWS;
        let span_a = tag_narrowing(r#"{span.a="x"}"#, w);
        let mixed = tag_narrowing(r#"{traceDuration > 1s && span.a="x"}"#, w);
        let cases = [
            (
                span_names_statement(&config, ceiling, w, &span_a),
                "LIMIT 300000)) AS cut",
            ),
            (
                tag_values_statement(&config, ceiling, None, "http.route", w, &span_a),
                "LIMIT 200000)) AS cut",
            ),
            (
                tag_values_statement(&config, ceiling, Some("span"), "http.route", w, &mixed),
                "LIMIT 150000)) AS cut",
            ),
        ];
        let mut wide = cfg();
        wide.scan_budget_rows = 50_000_000;
        let last = span_names_statement(&wide, 10_000, w, &span_a);
        for (sql, want) in cases.iter().chain([(last, "LIMIT 10000)) AS cut")].iter()) {
            assert!(sql.contains(want), "want {want}: {sql}");
            assert!(!sql.contains("LIMIT 10000000)"), "a fixed R: {sql}");
        }
    }

    #[test]
    fn clustered_search_settings_add_the_section_7_reader_settings() {
        let mut config = cfg();
        config.distributed = true;
        let rendered = format!("{:?}", search_settings(&config));
        for expected in [
            "optimize_skip_unused_shards",
            "prefer_localhost_replica",
            "max_rows_to_read",
            "result_overflow_mode",
        ] {
            assert!(
                rendered.contains(expected),
                "missing {expected} in {rendered}"
            );
        }
    }

    /// AC1 (issue #58 re-review): the catalog reads carry the same
    /// row-budget/throw contract the search path pins above, but never
    /// the clustered-reader settings — the catalog is a Global, un-`_dist`
    /// table with no coordinator fan-out to bound.
    #[test]
    fn catalog_settings_pin_the_layer_1_read_budget_contract() {
        let rendered = format!("{:?}", catalog_settings(&cfg()));
        for expected in [
            "max_rows_to_read",
            "1000",
            "read_overflow_mode",
            "throw",
            "max_query_size",
        ] {
            assert!(
                rendered.contains(expected),
                "missing {expected} in {rendered}"
            );
        }
        // `optimize_skip_unused_shards` is an answer pin, sent as `0`
        // (#603 follow-up).
        assert!(
            !rendered.contains("prefer_localhost_replica"),
            "unexpected prefer_localhost_replica in {rendered} — catalog reads are never clustered"
        );
    }

    /// Distributed config must not leak the clustered-reader settings into
    /// the catalog read either — the catalog table itself is never
    /// `_dist`, regardless of whether the *rest* of this engine's config
    /// targets a clustered deployment.
    #[test]
    fn catalog_settings_stay_unclustered_even_when_the_engine_config_is_distributed() {
        let mut config = cfg();
        config.distributed = true;
        let rendered = format!("{:?}", catalog_settings(&config));
        // `optimize_skip_unused_shards` is an answer pin, sent as `0`
        // (#603 follow-up).
        assert!(!rendered.contains("prefer_localhost_replica"));
    }

    /// TP1 (#603 follow-up): every trace settings builder carries the
    /// answer pins, pruning as the deployment is sharded and the option as
    /// configured, except the tag reads' `distinct_overflow_mode = 'break'`
    /// (their 10,000-value cut); the catalogue reads never prune.
    #[test]
    fn every_trace_read_carries_the_answer_pins() {
        for distributed in [false, true] {
            for skip in [false, true] {
                let config = TraceReadConfig {
                    distributed,
                    skip_unavailable_shards: skip,
                    ..cfg()
                };
                let pins = QuerySettings::new().answer_pins(distributed, skip);
                let builders = [
                    ("search_settings", search_settings(&config)),
                    ("tag_store_settings", tag_store_settings(&config)),
                    ("fetch_settings", fetch_settings(&config)),
                    ("metrics_settings", metrics_settings(&config)),
                    ("statement_settings", statement_settings(&config, false)),
                    ("graph_settings", graph_settings(&config)),
                ];
                for (builder, s) in &builders {
                    for name in pulsus_clickhouse::ANSWER_PIN_NAMES {
                        let want = if *builder == "tag_store_settings"
                            && name == "distinct_overflow_mode"
                        {
                            Some("break")
                        } else {
                            pins.get(name)
                        };
                        assert_eq!(
                            s.get(name),
                            want,
                            "{builder}, distributed {distributed}, skip {skip}: {name}"
                        );
                    }
                }
                let catalog_pins = QuerySettings::new().answer_pins(false, skip);
                let catalog = catalog_settings(&config);
                for name in pulsus_clickhouse::ANSWER_PIN_NAMES {
                    assert_eq!(
                        catalog.get(name),
                        catalog_pins.get(name),
                        "catalog_settings, distributed {distributed}, skip {skip}: {name}"
                    );
                }
            }
        }
    }

    // --- Issue #35: full-shape parse bound (traces) ---

    /// Each of the fetch's three templates plus 32 hex chars and its own
    /// bounds stays well under any plausible query-text cap, let alone
    /// [`crate::querytext::MAX_QUERY_TEXT_BYTES`]. Issue #35 used the
    /// equivalent of this to justify EXEMPTING `fetch_by_id` from
    /// [`crate::querytext::ensure_query_text_fits`]; since issue #509 the
    /// fetch passes that guard like every other read on this path, and
    /// this test is what says the guard can never fire on it.
    ///
    /// **More load-bearing than before, not less** (issue #587): these
    /// three statements are the longest text this path renders, where the
    /// old point read was the shortest.
    #[test]
    fn both_fetch_statements_stay_under_the_query_text_cap() {
        const HEX: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
        let window = FetchWindow {
            start_ns: 1_699_999_999_000_000_000,
            end_ns: 1_700_000_002_000_000_000,
        };
        for (label, sql) in [
            (
                "indexed",
                crate::traces::spans::fetch::indexed_fetch_sql(
                    "traces_dist",
                    "spans_dist",
                    "resources",
                    HEX,
                ),
            ),
            (
                "wide",
                crate::traces::spans::fetch::wide_fetch_sql(
                    "traces_dist",
                    "spans_dist",
                    "resources",
                    HEX,
                ),
            ),
            (
                "fallback",
                crate::traces::spans::fetch::fallback_fetch_sql(
                    "spans_dist",
                    "resources",
                    HEX,
                    window,
                ),
            ),
        ] {
            assert!(
                sql.len() < 4096,
                "the {label} statement is {} bytes, expected < 4 KiB",
                sql.len()
            );
            assert!(
                crate::querytext::ensure_query_text_fits(&sql).is_ok(),
                "the {label} statement must pass the query-text guard"
            );
        }
    }

    /// Issue #478, criterion 5a (hermetic half). **Every span name is
    /// typed `string` EXPLICITLY, never left empty.**
    ///
    /// The adversarial names are the ones a text-classifying inference
    /// would type `int`, `float`, `bool` and `duration`; the reference
    /// types all of them `string`, because a span name is a String
    /// column. The empty-type case is the trap this test exists for:
    /// the renderer copies `val_type` verbatim, so a read that set no
    /// type would put `"type":""` on the wire — the assertion is on
    /// `val_type` itself, not on the rendering.
    /// Issue #398 AC T1: **all three** of this module's settings origins
    /// carry the surface-wide memory ceiling and throw rather than spill.
    ///
    /// Three, not one. `search_settings` is the obvious root;
    /// `catalog_settings` is a DELIBERATELY INDEPENDENT root (it omits the
    /// clustered-reader block on purpose) and is the one whose unbounded
    /// `/api/search/tag/{tag}/values` read produced the measured 500 that
    /// opened this issue; and the §4.2 point read
    /// (`TraceEngine::fetch_by_id`) sent a bare `QuerySettings::new()`
    /// while mapping errors around `map_trace_read_error` entirely. The
    /// point read is included here by asserting the settings object it now
    /// dispatches with — `catalog_settings(&self.config)`, its whole body.
    ///
    /// This unit test alone is NOT the completeness proof: it can only
    /// check the origins someone remembered to list. That is what
    /// `every_trace_engine_query_carries_the_memory_ceiling` in
    /// `tests/query_log_gates.rs` is for — a `system.query_log` sweep sees
    /// a dispatch site no enumeration mentions.
    #[test]
    fn trace_catalog_settings_carry_the_memory_ceiling() {
        for bytes in [1u64, pulsus_config::TRACEQL_READ_MAX_MEMORY_BYTES_CEILING] {
            let mut c = cfg();
            c.read_max_memory_bytes = bytes;
            let expected = bytes.to_string();
            for (name, s) in [
                ("search_settings", search_settings(&c)),
                ("catalog_settings", catalog_settings(&c)),
                // `fetch_by_id`'s settings object, by construction.
                ("fetch_by_id (point read)", catalog_settings(&c)),
                ("metrics_settings", metrics_settings(&c)),
                ("statement_settings", statement_settings(&c, false)),
                ("graph_settings", graph_settings(&c)),
            ] {
                assert_eq!(
                    s.get("max_memory_usage"),
                    Some(expected.as_str()),
                    "{name} must carry the surface-wide memory ceiling verbatim"
                );
                assert_ne!(
                    s.get("max_memory_usage"),
                    Some("0"),
                    "{name} must never send ClickHouse's unlimited sentinel"
                );
                assert_eq!(
                    s.get("max_bytes_before_external_group_by"),
                    Some("0"),
                    "{name} must throw rather than spill"
                );
            }
        }
    }

    /// M1 (issue #57 re-audit round-4/5 finding): the hermetic mapper pin
    /// — every trace-search overflow code routes to ITS OWN reason (never
    /// impersonating another), and the two byte-budget constants that
    /// share `ScanBudgetBytes` are provably distinct from the Layer-2
    /// retention budget, so a Layer-1 byte preempt can never impersonate
    /// the retention-counter trip the `traces_search_explain.rs`
    /// AC-A3 gate asserts on.
    #[test]
    fn m1_every_overflow_code_maps_to_its_own_reason_and_the_budgets_are_distinct() {
        let server = |code| ChError::Server {
            code,
            message: "overflow".to_string(),
        };
        match map_trace_read_error(server(307), &cfg()) {
            ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes { budget_bytes, .. }) => {
                assert_eq!(budget_bytes, TRACE_READ_BYTES_BUDGET);
            }
            other => panic!("expected ScanBudgetBytes(TRACE_READ_BYTES_BUDGET), got {other:?}"),
        }
        match map_trace_read_error(server(396), &cfg()) {
            ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes { budget_bytes, .. }) => {
                assert_eq!(budget_bytes, TRACE_MAX_RESULT_BYTES);
            }
            other => panic!("expected ScanBudgetBytes(TRACE_MAX_RESULT_BYTES), got {other:?}"),
        }
        assert!(matches!(
            map_trace_read_error(server(158), &cfg()),
            ReadError::QueryTooBroad(TooBroadReason::TraceScanBudgetRows { .. })
        ));
        // Distinctness: neither Layer-1 byte-budget constant equals the
        // Layer-2 retention budget, so `budget_bytes` equality is a
        // sound discriminator between a Layer-1 preempt and the
        // retention-counter trip.
        assert_ne!(TRACE_READ_BYTES_BUDGET, HYDRATION_BYTE_BUDGET as u64);
        assert_ne!(TRACE_MAX_RESULT_BYTES, HYDRATION_BYTE_BUDGET as u64);
    }

    #[test]
    fn a_span_name_value_carries_an_explicit_string_type() {
        for name in ["500", "1.5", "true", "1.5s", "-3", "checkout", ""] {
            let value = span_name_value(name.to_string());
            assert_eq!(value.val, name);
            assert_eq!(
                value.val_type, "string",
                "{name:?} must carry an explicit string type, not an empty one"
            );
            assert!(
                !value.val_type.is_empty(),
                "an empty val_type would reach the wire as `\"type\":\"\"`, and is asserted \
                 here on the engine's value"
            );
        }
    }

    fn tag_ctx(w: WindowSql) -> PredicateCtx<'static> {
        PredicateCtx {
            window: w,
            spans_table: "spans",
            resources_table: "resources",
            indexed: &[],
        }
    }

    /// The window a request carries is `[start, end)` at nanosecond
    /// precision (issue #598 part 2) — the bound both store-backed reads
    /// are given.
    #[test]
    fn a_tag_values_request_carries_its_window_half_open() {
        let req = TagValuesRequest {
            q: None,
            start_ns: 1_700_000_000_000_000_000,
            end_ns: 1_700_010_800_000_000_000,
            floor_ns: i64::MIN,
        };
        assert_eq!(
            req.window().span_time_clause(),
            "start_ns >= 1700000000000000000 AND start_ns < 1700010800000000000"
        );
        assert!(
            !req.narrowing(&tag_ctx(req.window())).has_body(),
            "no q means no narrowing"
        );
    }

    /// An unlowerable `q` narrows NOTHING rather than erroring — the
    /// request type has no error path for it to take.
    #[test]
    fn an_unlowerable_q_leaves_the_request_unnarrowed() {
        for q in ["{span.http.status_code=", "garbage", "{}", ""] {
            let req = TagValuesRequest {
                q: Some(q),
                start_ns: 0,
                end_ns: 0,
                floor_ns: i64::MIN,
            };
            assert!(
                !req.narrowing(&tag_ctx(req.window())).has_body(),
                "{q:?} must not narrow"
            );
        }
        let req = TagValuesRequest {
            q: Some("{resource.service.name=\"cart\"}"),
            start_ns: 0,
            end_ns: 0,
            floor_ns: i64::MIN,
        };
        assert!(
            req.narrowing(&tag_ctx(req.window())).predicate().is_some(),
            "a well-formed q must narrow"
        );
    }
}

mod catalogue_values;
mod metrics_statement;
mod newest_first;
