//! `TraceEngine` — executes the §4.2 trace-by-ID point read and the
//! issue #57 two-phase TraceQL search against ClickHouse via `ChClient`.
//! Deliberately OTLP-agnostic (see [`super`]'s module doc): payload
//! decoding/dedup/assembly is `pulsus-server`'s job; search never reads
//! payloads at all.
//!
//! **Search execution model (plan v7 as amended):**
//!
//! - **Phase 1:** every generator in [`SearchPlan::generator_sqls`] runs
//!   as its own bounded, index-served ranked top-K query
//!   (`LIMIT gen_cap + 1`); the engine merges the `(trace_id, bound_ts)`
//!   tuples in Rust (`max` per trace — [`merge_candidates`]) into one
//!   ranked candidate list (`bound_ts DESC, trace_id ASC`).
//! - **Phase 2:** candidates are consumed newest-bound-first in batches
//!   of [`BATCH_TRACES`]; each batch is hydrated by primary key
//!   (`LIMIT MAX_SPANS_PER_TRACE + 1 BY trace_id` — the `+1` is the
//!   per-trace overflow probe), deduped by `span_id`, joined with its
//!   attribute membership/value reads, and evaluated **exactly**
//!   (`search_eval`). Matches enter a `limit`-size heap of response
//!   summaries only; consumption stops at the threshold rule (heap full
//!   AND next `bound_ts` strictly below the k-th held sort key — sound
//!   because `bound_ts` upper-bounds the public sort key, docs/api.md
//!   §4.2 ordering contract), at stream exhaustion, or at the
//!   `max_candidates` ceiling.
//! - **Memory contract (issue #57 re-audit):** Layer 1 — every query
//!   carries `max_bytes_to_read`/`read_overflow_mode='throw'`,
//!   `max_result_bytes`/`result_overflow_mode='throw'`, the row scan
//!   budget, and [`TRACE_SEARCH_MAX_BLOCK_ROWS`] (`max_block_size`);
//!   breach → 422. The accepted residual is one transiently-buffered
//!   block, now HARD-bounded: at most `TRACE_SEARCH_MAX_BLOCK_ROWS` rows
//!   × (fixed-width columns + string columns each capped at
//!   [`crate::traces::search_sql::TRACE_STR_COL_CAP`] bytes at the
//!   source) — never a-priori row-unbounded (docs/schemas.md §7). Phase-1
//!   generator reads additionally carry `max_memory_usage`
//!   (`config.generator_max_memory_bytes`) +
//!   `max_bytes_before_external_group_by=0`, bounding a dense
//!   common-value prefix's `GROUP BY` aggregation state; breach → code
//!   241 → [`TooBroadReason::TraceGeneratorMemory`] → 422. Issue #398
//!   extends `max_memory_usage` + `max_bytes_before_external_group_by=0`
//!   to EVERY trace read at all three of this module's settings origins —
//!   [`search_settings`], the independent [`catalog_settings`], and the
//!   §4.2 point read ([`TraceEngine::fetch_by_id`], which carried no
//!   settings at all and bypassed [`map_trace_read_error`]) — from
//!   `config.read_max_memory_bytes`; breach → code 241 →
//!   [`TooBroadReason::TraceReadMemory`] → 422. The generator's tighter
//!   ceiling layers on top and keeps its own reason. Layer 2 — a
//!   single request-scoped byte counter ([`HYDRATION_BYTE_BUDGET`])
//!   charges every retained byte (merge tuples, batch rows, membership
//!   sets, heap summaries); breach → 422.
//! - **Partiality (exhaustive conservative rule, plan v7 delta 2):**
//!   `partial = true` iff a generator returned `gen_cap + 1` rows, the
//!   consumption ceiling was reached with a lookahead candidate present,
//!   or a per-trace span overflow occurred. Budget breaches are hard
//!   `422`s, never silent partial results.
//!
//! ## Allocation-charge audit (code review round 3) — engine side
//!
//! Invariant: **no retained or intermediate collection exists
//! uncharged**. Site → charge (always before/as the allocation):
//!
//! | Allocation site | Charge |
//! |---|---|
//! | per-generator candidate row Vecs | per row during streaming (`collect_rows_charged`, `CANDIDATE_TUPLE_BYTES`) |
//! | merge map + ranked candidate list | one more `rows × CANDIDATE_TUPLE_BYTES` pre-charged before [`merge_candidates`]; input-side charge released after the per-generator Vecs drop, then reconciled down to the surviving deduped list (round 4) |
//! | batch id list | `id_list_charge` before the collect (released with the batch) |
//! | hydration row Vec | per row during streaming (`size_of::<HydrationRow>` + overhead + strings) |
//! | grouped `HydratedSpan` slots + `span_id` dedup-set entries | [`group_hydrated_rows`] (pure, unit-tested exact accounting): first-push initial reservations (`VEC_INITIAL_RESERVATION_SLOTS`) + per-group 2× outer slot + overhead + inner initial reservation + per-UNIQUE-span 2× inner slot + set entry at the standard hash cost (`[u8;8]` + overhead); replays are contains-checked first and charge nothing (round 5) |
//! | membership sets / numeric maps / select-value maps | per row during streaming (entry costs incl. overhead; string values by length) |
//! | trace-context / child-count co-load maps (issue #184) | per row during streaming (`size_of::<TraceCtxRow>` + overhead + root strings; `CHILD_COUNT_ENTRY_BYTES`) — issued only when the plan's `needs_trace_ctx()`/`needs_child_counts()` flags demand them, released with the batch |
//! | root row Vec | per row during streaming; charge transferred to the retained `roots` map ([`roots_retained_bytes`]) before the row charge is released |
//! | winner id list | charged before the collect; released when the list dies after the root read (round 4) |
//! | result heap entries | charged inside `evaluate_batch` (see `search_eval`'s audit); evict releases `retained_bytes` (the identical cost model) |
//! | heap→winners Vec + output slots + root-summary clones | COMPLETE output-slot capacity pre-charged before `Vec::with_capacity` (round 4); each root clone's string bytes charged before that clone |
//! | `PlanExplain` stage SQL/note clones (explained mode) | [`charge_explain`] before every clone/format (retained for the request) |
//! | per-query SQL text `String`s | stated residual: bounded by construction (template + ≤ 48 B × batch ids ≈ ≤ 2 KB per read at `BATCH_TRACES` = 32), same class as the driver's one-block transient |
//!
//! This table (and `search_eval`'s) is enforced MECHANICALLY by
//! `tests/traces_alloc_audit.rs` (round 4): any new collection-allocation
//! token in these two files fails that guard until it is allowlisted with
//! its charge site documented.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChError, ChRow, QuerySettings};

use super::graph_sql::{self, GraphWindow};
use super::log2_histogram;
use super::metrics_plan::{ExemplarSeriesKey, MetricsCtx, PlanKind, TraceMetricsPlan};
use super::metrics_result::{
    MetricExemplar, MetricLabel, MetricLabelValue, TraceMetricSeries, TraceMetricsResult,
};
use super::rows::{
    CandidateRow, ChildCountRow, CompareCrossTabRow, CompareTotalsRow, GraphEdgeRow, HydrationRow,
    MembershipRow, MetricAggGroupInstantRow, MetricAggGroupRow, MetricAggInstantRow, MetricAggRow,
    MetricBucketRow, MetricCompareExemplarRow, MetricCountRow, MetricExemplarRow,
    MetricGroupCountInstantRow, MetricGroupCountRow, MetricGroupExemplarRow,
    MetricLog2BucketInstantRow, MetricLog2BucketRow, MetricLog2ExemplarRow,
    MetricQuantileExemplarRow, MetricQuantileInstantRow, MetricQuantileRow, NumValueRow, RootRow,
    SpanNameRow, StoredSpan, StoredSpanRow, StrValueRow, TagNameRow, TagValueRow, TraceCtxRow,
};
use super::search_eval::{
    self, BatchAttrs, EventValues, GroupCardinalityCounter, HydratedSpan, ProbeMembership, SpanKey,
    SpanSetGroup, SpanSummary, TraceCtxInfo, TraceMatch, TraceSpans,
};
use super::search_plan::{SearchCtx, SearchPlan};
use super::tag_narrow::{TagNarrowing, narrowing_from_query};
use super::tags_sql::DaySpan;
use crate::logql::error::{ReadError, TooBroadReason};
use crate::logql::explain::PlanExplain;

/// Phase-2 batch width: candidates hydrated/evaluated per round trip.
/// Documented constant (promote to config only on benchmark evidence).
pub const BATCH_TRACES: usize = 32;

/// Per-trace hydration span cap; the `+1` probe detects overflow, which
/// truncates that trace's evaluation set and marks the response partial
/// (a truncated trace is never silently reported complete).
pub const MAX_SPANS_PER_TRACE: usize = 10_000;

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

/// Layer 2 — the single request-scoped retention budget: every byte the
/// search accumulates (merge tuples, in-flight batch rows, membership
/// sets, heap-held response summaries) is charged against this counter;
/// a breach is a `422 query_too_broad`, never an OOM. With Layer 1's
/// [`TRACE_MAX_RESULT_BYTES`] now effective per hydration batch (issue
/// #57 re-audit v7), this counter's distinct, load-bearing role is
/// **cross-batch retained accumulation** — heap-held response summaries
/// (and merge tuples / root summaries) survive the per-batch charge
/// release and grow across batches, where no per-query server setting
/// can see them.
pub const HYDRATION_BYTE_BUDGET: usize = 256 * 1024 * 1024;

/// Layer 1 read-side byte budget (`max_bytes_to_read`, throw) applied to
/// every search query — the logs-budget-analogous default
/// (`reader.logql_scan_budget_bytes`' 50 GiB).
pub const TRACE_READ_BYTES_BUDGET: u64 = 50 * 1024 * 1024 * 1024;

/// Layer 1 result-side byte ceiling (`max_result_bytes`, throw) applied
/// to every search query — bounds any single result set independent of
/// string payload lengths. Issue #57 re-audit v7: the sub-A
/// source-truncation projection makes this setting's accounting
/// **effective on the hydration/root/value reads** (live-verified —
/// unwrapped passthrough columns were never accounted), a deliberate
/// hardening: this is now the practical **per-batch** byte bound, firing
/// server-side before the driver materializes anything; the
/// [`HYDRATION_BYTE_BUDGET`] retention counter remains the binding bound
/// on cross-batch retained accumulation.
pub const TRACE_MAX_RESULT_BYTES: u64 = 64 * 1024 * 1024;

/// Layer 1 block-row cap (`max_block_size`) applied to every search
/// query (issue #57 re-audit, sub-problem A): bounds the row width of
/// any single transiently-buffered result block, so the driver's
/// documented one-block residual is a hard product with
/// [`crate::traces::search_sql::TRACE_STR_COL_CAP`] rather than
/// a-priori row-unbounded — see the module doc's memory-contract
/// paragraph.
pub const TRACE_SEARCH_MAX_BLOCK_ROWS: u64 = 4096;

/// ClickHouse overflow codes the trace search budget settings can raise.
const CODE_TOO_MANY_ROWS: i32 = 158;
const CODE_TOO_MANY_BYTES: i32 = 307;
const CODE_TOO_MANY_ROWS_OR_BYTES: i32 = 396;
/// `MEMORY_LIMIT_EXCEEDED` — raised only by the phase-1 candidate-
/// generator memory ceiling ([`generator_settings`]'s `max_memory_usage`
/// and `max_bytes_before_external_group_by = 0`, throw-not-spill); maps
/// exclusively to [`TooBroadReason::TraceGeneratorMemory`] via
/// [`map_trace_generator_error`], applied only to phase-1 generator
/// reads (issue #57 re-audit, sub-problem B).
const CODE_MEMORY_LIMIT_EXCEEDED: i32 = 241;
/// `SET_SIZE_LIMIT_EXCEEDED` — raised only by the metrics semi-join
/// IN-set limits ([`TRACE_METRICS_MAX_SET_ROWS`]/[`TRACE_METRICS_MAX_SET_BYTES`],
/// `set_overflow_mode='throw'`); no other trace/LogQL query sets a set
/// limit, so this code maps exclusively on the metrics path (issue #59
/// plan v2 delta 3 as amended, confirmed against a live 24.8 in
/// `tests/traces_metrics_explain.rs`).
const CODE_SET_SIZE_LIMIT_EXCEEDED: i32 = 191;

/// The metrics attribute semi-join IN-set row budget (`max_rows_in_set`,
/// throw): bounds the materialized `(trace_id, span_id)` set of every
/// attr-filter membership subquery — a metrics window matching more
/// than this many attribute rows is a `422 query_too_broad`, never an
/// unbounded in-memory set. Documented constant (docs/schemas.md §4.2;
/// promoted to config only on evidence).
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
/// `pub` (issue #57 re-audit, visibility-only): the retention-gate drift
/// guard in `tests/traces_search_explain.rs` derives its pre-hydration
/// charge bound from this and [`CANDIDATE_TUPLE_BYTES`] rather than
/// re-hardcoding them.
pub const RETAINED_ENTRY_OVERHEAD: usize = 64;

/// Retention charge for one merged `(trace_id, bound_ts)` tuple — the
/// per-generator row is charged at the merged-map entry's full cost
/// (rows ≥ merged entries, so this upper-bounds the map, including when
/// generators overlap on a trace). `pub` for the same reason as
/// [`RETAINED_ENTRY_OVERHEAD`].
pub const CANDIDATE_TUPLE_BYTES: usize =
    std::mem::size_of::<([u8; 16], i64)>() + RETAINED_ENTRY_OVERHEAD;
/// Retention charge for one membership set entry.
const MEMBERSHIP_ENTRY_BYTES: usize =
    std::mem::size_of::<([u8; 16], [u8; 8])>() + RETAINED_ENTRY_OVERHEAD;
/// Retention charge for one numeric attribute value entry.
const NUM_VALUE_ENTRY_BYTES: usize =
    std::mem::size_of::<(([u8; 16], [u8; 8]), f64)>() + RETAINED_ENTRY_OVERHEAD;
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

/// Retention charge for ONE co-loaded event/link value (issue #351,
/// review 2), covering every structure that holds it at the peak:
///
/// * its slot in the span's `Vec<String>`/`Vec<f64>`, charged at
///   [`VEC_INITIAL_RESERVATION_SLOTS`] slots per value. **Not 2×**
///   (review 3, `[high]`): a fresh `Vec`'s FIRST push reserves 4 slots,
///   so a 2× charge under-charges the first value of every span by half.
///   Charging 4 slots per value upper-bounds the capacity at every
///   length, since a `Vec` holding `n` values has capacity
///   `max(4, 2^ceil(log2 n)) ≤ max(4, 2n) ≤ 4n` for `n ≥ 1` — the
///   initial reservation is covered by the first value's own charge, and
///   the doubling by every value's. `size_of::<String>()` (24 B)
///   upper-bounds the `f64` slot (8 B), so one constant serves both
///   branches;
/// * a WHOLE map entry plus the hash-table envelope. The map holds one
///   entry per SPAN, not per value, so charging one per value
///   over-charges — the direction a budget must err in.
///
/// The value's own bytes are added by the caller on the text branch. The
/// string payload is MOVED out of the decoded row into the vec, so it is
/// charged once and exists once.
const EVENT_VALUE_ENTRY_BYTES: usize = VEC_INITIAL_RESERVATION_SLOTS
    * std::mem::size_of::<String>()
    + std::mem::size_of::<(SpanKey, EventValues)>()
    + RETAINED_ENTRY_OVERHEAD;
/// Retention charge for one direct-child-count co-load entry (issue
/// #184): the `(trace_id, parent span_id) → count` map entry.
const CHILD_COUNT_ENTRY_BYTES: usize =
    std::mem::size_of::<(([u8; 16], [u8; 8]), u64)>() + RETAINED_ENTRY_OVERHEAD;

/// Owned table/budget configuration a [`TraceEngine`] reads against —
/// mirrors [`crate::logql::EngineConfig`]'s "owned `String`, no borrowed
/// lifetime on the engine itself" shape. The point read uses only
/// `spans_table`; the search path (issue #57) uses everything.
#[derive(Debug, Clone)]
pub struct TraceReadConfig {
    /// `trace_spans` (or `trace_spans_dist` when clustered — the caller
    /// applies the same `_dist` rule as every other read engine's config).
    pub spans_table: String,
    /// `trace_attrs_idx{_dist}` — the attribute index the search
    /// generators/membership reads target.
    pub attrs_table: String,
    /// `trace_edges{_dist}` — the service-graph half-row ledger the
    /// `service_graph` read targets (issue #173). `_dist`-suffixed when
    /// clustered exactly like `spans_table`/`attrs_table` (halves co-shard
    /// on `cityHash64(trace_id)`, so the query-time join is shard-local).
    pub edges_table: String,
    /// `reader.traceql_max_candidates` — per-generator top-K depth and
    /// the merged consumption ceiling.
    pub max_candidates: u64,
    /// `reader.traceql_scan_budget_rows` — `max_rows_to_read` (throw) on
    /// every search query; breach → 422 (code 158 →
    /// [`TooBroadReason::TraceScanBudgetRows`]).
    pub scan_budget_rows: u64,
    /// `reader.traceql_max_series` (issue #182) — the metrics `by(...)`
    /// distinct-series cap; the `LIMIT cap+1` probe breach → 422
    /// ([`TooBroadReason::TraceMetricsSeriesCap`]).
    pub max_series: u64,
    /// `reader.traceql_generator_max_memory_bytes` — the phase-1
    /// candidate-generator query's `max_memory_usage` ceiling (issue #57
    /// re-audit, sub-problem B): bounds a dense common-value prefix's
    /// `GROUP BY trace_id` aggregation state; breach → 422 (code 241 →
    /// [`TooBroadReason::TraceGeneratorMemory`]). Never applied to
    /// phase-2 reads (hydration/membership/value/root), which set no
    /// memory limit of their own.
    pub generator_max_memory_bytes: u64,
    /// Issue #398: `reader.traceql_read_max_memory_bytes` — the
    /// `max_memory_usage` ceiling (throw-not-spill) carried by EVERY trace
    /// read, at all three of this module's settings origins:
    /// [`search_settings`], [`catalog_settings`], and the §4.2 point read
    /// ([`TraceEngine::fetch_by_id`], which sent a bare
    /// `QuerySettings::new()` and mapped errors around
    /// [`map_trace_read_error`] entirely before #398). Breach → server code
    /// 241 → [`TooBroadReason::TraceReadMemory`] → `422`. Phase-1
    /// generator reads layer the TIGHTER
    /// [`Self::generator_max_memory_bytes`] on top and keep their own
    /// reason.
    pub read_max_memory_bytes: u64,
    /// Clustered mode: inject the docs/schemas.md §7 clustered-reader
    /// settings on every search query (both phases are shard-local by
    /// the `cityHash64(trace_id)` co-sharding).
    pub distributed: bool,
    /// `PULSUS_SKIP_UNAVAILABLE_SHARDS` passthrough for the §7 settings.
    pub skip_unavailable_shards: bool,
}

/// The final winners' root metadata (root span = `parent_id` all-zero,
/// else timestamp-earliest of the **full** trace — root hydration is
/// trace-wide, not window-bounded, plan v4 delta 4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootSummary {
    pub service: String,
    pub name: String,
    pub start_ns: i64,
    pub duration_ns: i64,
}

/// A winner trace's root-span summary plus its trace-wide time envelope
/// (issue #464), both derived in ONE pass over the same trace-wide,
/// window-free, uncapped `root_sql` rows — so the envelope is
/// full-trace-exact for the same reason the root is, and costs no extra
/// query, no extra round trip and no second walk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceContext {
    pub root: RootSummary,
    /// `min(timestamp_ns)` over every span of the trace.
    pub trace_start_ns: i64,
    /// `max(timestamp_ns + duration_ns) - min(timestamp_ns)`.
    pub trace_duration_ns: i64,
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchOutput {
    pub traces: Vec<TraceSearchResult>,
    /// Any internal bound engaged before natural exhaustion. Not a wire
    /// field: the response renderer turns it into the reference's own
    /// incompleteness signal, `completedJobs < totalJobs` (issue #464).
    pub partial: bool,
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
/// most [`graph_sql::SERVICE_GRAPH_MAX_EDGES`] of them; `truncated` is the
/// non-silent cap indicator — `true` iff the `LIMIT cap + 1` probe row
/// appeared (the search path's convention).
#[derive(Debug, Clone, PartialEq)]
pub struct ServiceGraph {
    pub edges: Vec<GraphEdgeRow>,
    pub truncated: bool,
}

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
/// `val_type` is the EMPTY string for a row written before migration 41
/// added the column. Nothing stored distinguishes such a row's original
/// type — `val` is the rendered text and `val_num` is a parse of that
/// text — so the renderer reports it as `string` and derives nothing from
/// the characters.
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
/// assertion criterion 5a is about: `entry_type("")` renders `string`
/// too, so a read that left the type EMPTY would produce byte-identical
/// wire output while meaning "a legacy row of unknown type" instead of "a
/// String column". The two are only distinguishable here, at the engine
/// boundary, which is where the gate has to sit.
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
/// ([`crate::traces::tag_narrow::narrowing_from_query`]), so an
/// unparseable `q` widens the read instead of producing an error the
/// handler would have to turn into a status code.
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
}

impl TagValuesRequest<'_> {
    /// The terms this request's `q` contributes — empty for an absent
    /// `q` and for every `q` the lowering cannot handle.
    fn narrowing(&self) -> TagNarrowing {
        match self.q {
            None => TagNarrowing::default(),
            Some(q) => narrowing_from_query(q),
        }
    }

    /// The UTC days the window touches — the bound both store-backed
    /// reads carry.
    fn days(&self) -> DaySpan {
        DaySpan::from_window(self.start_ns, self.end_ns)
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

/// Pure Rust-side merge of the per-generator candidate outputs: `max`
/// `bound_ts` per trace (an explicit max — anything less could
/// under-bound and break threshold termination, plan v5 delta 1), ranked
/// `(bound_ts DESC, trace_id ASC)`.
pub(crate) fn merge_candidates(per_generator: &[Vec<([u8; 16], i64)>]) -> Vec<([u8; 16], i64)> {
    let mut merged: HashMap<[u8; 16], i64> = HashMap::new();
    for rows in per_generator {
        for (trace_id, bound_ts) in rows {
            merged
                .entry(*trace_id)
                .and_modify(|existing| *existing = (*existing).max(*bound_ts))
                .or_insert(*bound_ts);
        }
    }
    let mut out: Vec<([u8; 16], i64)> = merged.into_iter().collect();
    out.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    out
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
fn map_trace_read_error(e: ChError, config: &TraceReadConfig) -> ReadError {
    if let ChError::Server { code, .. } = &e {
        match *code {
            // Issue #398: the surface-wide memory ceiling
            // `search_settings`/`catalog_settings` now carry. Generator
            // reads never reach this arm — `map_trace_generator_error`
            // classifies 241 as the tighter, more specific
            // `TraceGeneratorMemory` first and only delegates the rest.
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

/// Maps a ClickHouse error on the **phase-1 candidate-generator** read
/// path only (issue #57 re-audit, sub-problem B): code 241
/// (`MEMORY_LIMIT_EXCEEDED`) — raised only by [`generator_settings`]'s
/// memory ceiling — maps to the dedicated, never-conflated
/// [`TooBroadReason::TraceGeneratorMemory`]; everything else delegates
/// to the shared trace mapper ([`map_trace_read_error`]).
///
/// Issue #398: [`map_trace_read_error`] DOES map 241 now, to the
/// surface-wide [`TooBroadReason::TraceReadMemory`]. The order below is
/// therefore load-bearing rather than incidental — this mapper runs first
/// on generator reads and returns before delegating, so the generator's
/// tighter ceiling keeps reporting its own reason and the two never mix.
fn map_trace_generator_error(e: ChError, config: &TraceReadConfig) -> ReadError {
    if let ChError::Server {
        code: CODE_MEMORY_LIMIT_EXCEEDED,
        ..
    } = &e
    {
        return ReadError::QueryTooBroad(TooBroadReason::TraceGeneratorMemory {
            budget_bytes: config.generator_max_memory_bytes,
        });
    }
    map_trace_read_error(e, config)
}

/// Worst-first heap ordering: the max-heap "greatest" entry is the WORST
/// result under the public contract (smallest sort key; among ties the
/// LARGEST trace id, since ascending trace id wins).
#[derive(Debug)]
struct HeapEntry(TraceMatch);

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.0.sort_key == other.0.sort_key && self.0.trace_id == other.0.trace_id
    }
}
impl Eq for HeapEntry {}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse sort_key (smaller = "greater" = worse), then trace_id
        // (larger = worse).
        other
            .0
            .sort_key
            .cmp(&self.0.sort_key)
            .then(self.0.trace_id.cmp(&other.0.trace_id))
    }
}

pub struct TraceEngine {
    dispatch: super::dispatch::TraceDispatch,
    config: TraceReadConfig,
}

impl TraceEngine {
    pub fn new(client: ChClient, config: TraceReadConfig) -> Self {
        Self {
            dispatch: super::dispatch::TraceDispatch::new(client),
            config,
        }
    }

    /// The planning context this engine's configuration implies —
    /// callers feed it to [`super::search_plan::plan_search`].
    pub fn search_ctx(&self) -> SearchCtx<'_> {
        SearchCtx {
            filter: super::filter::SpanFilterCtx {
                spans_table: &self.config.spans_table,
                attrs_table: &self.config.attrs_table,
            },
            max_candidates: self.config.max_candidates,
            max_series: self.config.max_series,
            distributed: self.config.distributed,
        }
    }

    /// The metrics planning context this engine's configuration implies —
    /// callers feed it to [`super::metrics_plan::plan_trace_metrics`]
    /// (issue #59), mirroring [`Self::search_ctx`].
    pub fn metrics_ctx(&self) -> MetricsCtx<'_> {
        MetricsCtx {
            filter: super::filter::SpanFilterCtx {
                spans_table: &self.config.spans_table,
                attrs_table: &self.config.attrs_table,
            },
            scan_budget_rows: self.config.scan_budget_rows,
            max_series: self.config.max_series,
            distributed: self.config.distributed,
            skip_unavailable_shards: self.config.skip_unavailable_shards,
        }
    }

    /// Executes a metrics range plan (issue #59): one fully-pushed-down
    /// time-bucketed query — bucketing, replay-deduped counting, and time
    /// pruning all happen in ClickHouse; the engine frames exactly
    /// `range_axis().points` `(t_ms, value)` samples per densified series
    /// (at most `MAX_METRICS_POINTS + 1`; the plan enforced the cap
    /// statically) and applies the explicit encode-boundary conversions
    /// (`n as f64`, rate ÷ the step in fractional seconds; the row's
    /// `t_ms` is already the millisecond point unit the encoder consumes —
    /// issue #59 re-audit, `Int64` epoch-milliseconds).
    ///
    /// Membership follows the function (issue #477 (a)): an ungrouped
    /// `rate`/`count_over_time` always returns its one series, zero-filled
    /// where nothing matched; the value aggregations, the grouped shapes,
    /// `quantile_over_time`, `histogram_over_time` and `compare()` return
    /// an empty series list when nothing matched.
    pub async fn metrics_range(
        &self,
        plan: &TraceMetricsPlan,
    ) -> Result<TraceMetricsResult, ReadError> {
        let mut result = self.frame_range(plan).await?;
        // P5: attach exemplars and apply the topk/bottomk second-stage
        // reduction (issue #182).
        //
        // Issue #477 (c) turns exemplars on by DEFAULT, so this is now a
        // second ClickHouse statement on every range panel rather than
        // only under a `with()` hint. The skip below is what bounds that:
        // a frame whose samples are all zero has no bucket an exemplar
        // could belong to, and after densification an empty answer is a
        // full grid of zeros — exactly the shape a sparse panel produces
        // most often. Checked against the FRAMED result, so it costs no
        // extra query to decide.
        if plan.exemplar_sql().is_some() && has_a_non_zero_sample(&result) {
            self.attach_range_exemplars(plan, &mut result).await?;
        }
        // P6b: the metrics-result comparison post-filter (`… > 5`).
        if let Some(rf) = plan.result_filter() {
            apply_result_filter(rf, &mut result);
        }
        if let Some(reduce) = plan.reduce() {
            apply_series_reduce(reduce, &mut result);
        }
        Ok(result)
    }

    /// Frames the first-stage range result (before P5 exemplars/reduce),
    /// then DENSIFIES it onto the plan's bucket axis (issue #477 (a)).
    ///
    /// The fill happens here, at the framing boundary, and never in SQL:
    /// the query still groups only over rows that exist, and the missing
    /// buckets are materialised from the window and the step, which are
    /// already known. A ClickHouse-side gap fill, a second query, or a
    /// wider scan would all be the wrong shape — emitting every bucket is
    /// a rendering property, not a scan property.
    async fn frame_range(&self, plan: &TraceMetricsPlan) -> Result<TraceMetricsResult, ReadError> {
        self.enforce_series_cap(plan.range_probe_sql()).await?;
        let mut result = self.frame_range_series(plan).await?;
        if densifies(plan.kind()) {
            let axis = plan.range_axis();
            for series in &mut result.series {
                densify(series, axis);
            }
        }
        Ok(result)
    }

    /// The per-shape range framing, before densification.
    async fn frame_range_series(
        &self,
        plan: &TraceMetricsPlan,
    ) -> Result<TraceMetricsResult, ReadError> {
        if plan.kind() == PlanKind::Compare {
            let (cross_tab, totals) = plan
                .compare_range()
                .expect("compare plan carries range SQL");
            return self
                .frame_compare(cross_tab, totals, plan.compare_top_n())
                .await;
        }
        let settings = metrics_settings(&self.config);
        let sql = plan.range_sql();
        match (plan.kind(), plan.group_label()) {
            (PlanKind::Quantile, _) => {
                // One series per requested quantile (`p=<q>`); the TDigest
                // result array is ordered as requested. ns→seconds scale.
                let quantiles = plan.quantiles();
                let mut series: Vec<TraceMetricSeries> = quantiles
                    .iter()
                    .map(|q| TraceMetricSeries {
                        labels: vec![MetricLabel::double("p", *q)],
                        samples: Vec::new(),
                        exemplars: Vec::new(),
                    })
                    .collect();
                let mut stream = self
                    .dispatch
                    .query_stream::<MetricQuantileRow, _>(sql, &settings, |e| {
                        map_trace_metrics_error(e, &self.config)
                    })
                    .await?;
                while let Some(row) = stream.next().await {
                    let row = row.map_err(|e| map_trace_metrics_error(e, &self.config))?;
                    for (i, s) in series.iter_mut().enumerate() {
                        let v = row.qs.get(i).copied().unwrap_or(0.0);
                        s.samples.push((row.t_ms, agg_value(finite_or_zero(v))));
                    }
                }
                if series.iter().all(|s| s.samples.is_empty()) {
                    return Ok(TraceMetricsResult { series: vec![] });
                }
                Ok(TraceMetricsResult { series })
            }
            (PlanKind::Histogram, _) => {
                // Issue #252: one PLAIN-TALLY series per power-of-two
                // bucket that actually occurred (`__bucket=<seconds>`).
                // No ladder, nothing cumulative, and a bucket with no
                // spans anywhere in the window emits NO series at all —
                // the reference creates a series on first observation of
                // the key (`engine_metrics.go:788-793 @ v3.0.2`).
                //
                // The SQL orders by `(t, bucket)`, so collecting into a
                // `BTreeMap` keyed by bucket keeps each series' samples
                // in timestamp order without a second sort.
                let mut by_bucket: BTreeMap<u64, Vec<(i64, f64)>> = BTreeMap::new();
                let mut stream = self
                    .dispatch
                    .query_stream::<MetricLog2BucketRow, _>(sql, &settings, |e| {
                        map_trace_metrics_error(e, &self.config)
                    })
                    .await?;
                while let Some(row) = stream.next().await {
                    let row = row.map_err(|e| map_trace_metrics_error(e, &self.config))?;
                    by_bucket
                        .entry(row.bucket_ns)
                        .or_default()
                        .push((row.t_ms, row.n as f64));
                }
                if by_bucket.is_empty() {
                    return Ok(TraceMetricsResult { series: vec![] });
                }
                let mut series: Vec<TraceMetricSeries> = by_bucket
                    .into_iter()
                    .map(|(bucket_ns, samples)| TraceMetricSeries {
                        labels: vec![MetricLabel::double(
                            "__bucket",
                            log2_histogram::bucket_seconds(bucket_ns),
                        )],
                        samples,
                        exemplars: Vec::new(),
                    })
                    .collect();
                sort_histogram_series_by_bucket_ascending(&mut series);
                Ok(TraceMetricsResult { series })
            }
            (kind, None) => {
                let mut samples: Vec<(i64, f64)> = Vec::new();
                match kind {
                    PlanKind::Count { is_rate } => {
                        let denom = plan.step_seconds();
                        let mut stream = self
                            .dispatch
                            .query_stream::<MetricBucketRow, _>(sql, &settings, |e| {
                                map_trace_metrics_error(e, &self.config)
                            })
                            .await?;
                        while let Some(row) = stream.next().await {
                            let row = row.map_err(|e| map_trace_metrics_error(e, &self.config))?;
                            samples.push((row.t_ms, count_value(is_rate, row.n, denom)));
                        }
                    }
                    PlanKind::Agg(_) => {
                        let mut stream = self
                            .dispatch
                            .query_stream::<MetricAggRow, _>(sql, &settings, |e| {
                                map_trace_metrics_error(e, &self.config)
                            })
                            .await?;
                        while let Some(row) = stream.next().await {
                            let row = row.map_err(|e| map_trace_metrics_error(e, &self.config))?;
                            samples.push((row.t_ms, agg_value(row.v)));
                        }
                    }
                    PlanKind::Quantile | PlanKind::Histogram | PlanKind::Compare => {
                        unreachable!("quantile/histogram are framed above")
                    }
                }
                // Issue #477 (a): membership is per function. An ungrouped
                // `rate`/`count_over_time` ALWAYS emits its one `__name__`
                // series, even with no matching row anywhere in the window
                // — densification then fills every bucket with a zero the
                // encoder omits, and the datasource reads an absent value
                // back as zero, which is the right answer. The value
                // aggregations stay sparse and keep returning nothing:
                // measured, the reference answers `{"series":[]}` for a
                // no-match `avg_over_time` and a zero-filled series for a
                // no-match `rate`.
                if samples.is_empty() && !matches!(kind, PlanKind::Count { .. }) {
                    return Ok(TraceMetricsResult { series: vec![] });
                }
                Ok(TraceMetricsResult {
                    series: vec![TraceMetricSeries {
                        labels: vec![MetricLabel::str("__name__", plan.metric_name())],
                        samples,
                        exemplars: vec![],
                    }],
                })
            }
            (kind, Some(label)) => {
                // Grouped: collect samples per group value into a
                // deterministic (BTreeMap-ordered) set of labelled series.
                let mut by_group: BTreeMap<String, Vec<(i64, f64)>> = BTreeMap::new();
                match kind {
                    PlanKind::Count { is_rate } => {
                        let denom = plan.step_seconds();
                        let mut stream = self
                            .dispatch
                            .query_stream::<MetricGroupCountRow, _>(sql, &settings, |e| {
                                map_trace_metrics_error(e, &self.config)
                            })
                            .await?;
                        while let Some(row) = stream.next().await {
                            let row = row.map_err(|e| map_trace_metrics_error(e, &self.config))?;
                            by_group
                                .entry(row.g0)
                                .or_default()
                                .push((row.t_ms, count_value(is_rate, row.n, denom)));
                        }
                    }
                    PlanKind::Agg(_) => {
                        let mut stream = self
                            .dispatch
                            .query_stream::<MetricAggGroupRow, _>(sql, &settings, |e| {
                                map_trace_metrics_error(e, &self.config)
                            })
                            .await?;
                        while let Some(row) = stream.next().await {
                            let row = row.map_err(|e| map_trace_metrics_error(e, &self.config))?;
                            by_group
                                .entry(row.g0)
                                .or_default()
                                .push((row.t_ms, agg_value(row.v)));
                        }
                    }
                    PlanKind::Quantile | PlanKind::Histogram | PlanKind::Compare => {
                        unreachable!("quantile/histogram are framed above")
                    }
                }
                Ok(TraceMetricsResult {
                    series: by_group
                        .into_iter()
                        .map(|(g, samples)| TraceMetricSeries {
                            labels: vec![MetricLabel::str(label, g)],
                            samples,
                            exemplars: vec![],
                        })
                        .collect(),
                })
            }
        }
    }

    /// Runs the grouped-query distinct-by-key series probe (issue #182):
    /// a `cap+1` result is a static `422 query_too_broad`
    /// ([`TooBroadReason::TraceMetricsSeriesCap`]). Ungrouped plans have
    /// no probe and return immediately.
    ///
    /// The probe is PASSED IN rather than read off the plan (issue #477):
    /// the two routes count over different windows, so the range path
    /// hands it [`TraceMetricsPlan::range_probe_sql`] and the instant path
    /// [`TraceMetricsPlan::instant_probe_sql`]. Reading one accessor here
    /// is what made the range answer's cap countable only over the
    /// instant window.
    async fn enforce_series_cap(&self, probe: Option<&str>) -> Result<(), ReadError> {
        let Some(probe) = probe else {
            return Ok(());
        };
        let cap = self.config.max_series;
        let settings = metrics_settings(&self.config);
        let mut count: u64 = 0;
        // Scoped stream: the probe returns exactly one `count()` row.
        let mut stream = self
            .dispatch
            .query_stream::<MetricCountRow, _>(probe, &settings, |e| {
                map_trace_metrics_error(e, &self.config)
            })
            .await?;
        while let Some(row) = stream.next().await {
            count = row.map_err(|e| map_trace_metrics_error(e, &self.config))?.n;
        }
        if count > cap {
            return Err(ReadError::QueryTooBroad(
                TooBroadReason::TraceMetricsSeriesCap { count, cap },
            ));
        }
        Ok(())
    }

    /// Frames a `compare()` result (issue #182 P6b) from its cross-tab and
    /// totals queries into the captured Tempo meta-series shape: per
    /// attribute `(key, value)` a `baseline` (all outer spans) and a
    /// `selection` (`is_sel`) series; per key a `key=nil` complement
    /// (`total − Σ present`) and the `baseline_total`/`selection_total`
    /// denominators (`__meta_type` label + one attribute label).
    ///
    /// Fully data-driven: this loop enumerates whatever `(key, value)` the
    /// cross-tab emits and the well-known loop below only fills in keys with
    /// zero present rows. Issue #189 wires the cross-tab to emit real values
    /// for `statusMessage`/`rootName`/`rootServiceName` (see
    /// [`super::metrics_sql::metrics_compare_sql`]) — those keys are now
    /// data-driven-when-present here with no per-key branch, and only remain
    /// well-known-`nil` when fully absent (`instrumentation:name`/
    /// `instrumentation:version` still always `nil` pending #179).
    ///
    /// # `top_n` (issue #460)
    ///
    /// `compare()`'s second argument, defaulting to **10** in the
    /// reference's own grammar (`expr.y:324`), so it bites on the
    /// one-argument form too. Per attribute AND per side independently,
    /// that side's distinct values are ranked by the sum of their counts
    /// over the whole window and only the top `top_n` are emitted
    /// (`engine_metrics_compare.go:234-256` — `addValues` builds a
    /// per-side `topN` and calls `top.get(m.topN, …)`, whose key is
    /// `Σ values`). Trimmed values are **dropped, not folded**: they stay
    /// in `key_bucket_sum`, so the `key=nil` complement and the `*_total`
    /// denominators are unchanged.
    ///
    /// Ties are broken here by ascending value string. The reference's
    /// order under a tie is `sort.Slice`, which is not stable, so its own
    /// survivors are arbitrary (measured twice, two different arbitrary
    /// sets); ours is deterministic, which is a deliberate refinement —
    /// ledger `traceql-compare-topn-tie-order`.
    ///
    /// # The error series we deliberately do NOT emit
    ///
    /// When a key exceeds `topN` the reference's raw job also emits a
    /// series labelled with its `internalLabelErrorTooManyValues`
    /// (`engine_metrics_compare.go:28,252-267` @ v3.0.2), carrying
    /// `Values: nil`. It is **unreachable on the wire**:
    /// `SeriesSet.ToProto` drops zero-sample series
    /// (`engine_metrics.go:380-383`), and it is measured absent from the
    /// container's response body at `topN` 1 and 3. Emitting one here
    /// would therefore be a divergence from what a user actually sees, so
    /// this function emits none.
    ///
    /// **That is checkable, and the check's own scope is the point.** The
    /// label name appears in no production source in this workspace; the
    /// exact command, its domain and its one carve-out are recorded beside
    /// the assertion that consumes them, in
    /// `crates/pulsus-read/tests/compare_arity_differential.rs` — which is
    /// the one file that must name the label, in order to assert its
    /// absence from a rendered body.
    ///
    /// Cost: the ranking runs over rows [`Self::enforce_series_cap`] has
    /// already bounded to `reader.traceql_max_series` distinct
    /// `(key, value)` pairs, with no extra query and no extra round trip.
    async fn frame_compare(
        &self,
        cross_tab_sql: &str,
        totals_sql: &str,
        top_n: usize,
    ) -> Result<TraceMetricsResult, ReadError> {
        let settings = metrics_settings(&self.config);
        // (key, value) -> [(t, base_n, sel_n)]; (key, t) -> Σ present.
        let mut per_kv: BTreeMap<(String, String), CompareValueBuckets> = BTreeMap::new();
        let mut key_bucket_sum: BTreeMap<(String, i64), (u64, u64)> = BTreeMap::new();
        let mut keys: BTreeSet<String> = BTreeSet::new();
        {
            let mut stream = self
                .dispatch
                .query_stream::<CompareCrossTabRow, _>(cross_tab_sql, &settings, |e| {
                    map_trace_metrics_error(e, &self.config)
                })
                .await?;
            while let Some(row) = stream.next().await {
                let row = row.map_err(|e| map_trace_metrics_error(e, &self.config))?;
                keys.insert(row.akey.clone());
                per_kv
                    .entry((row.akey.clone(), row.aval))
                    .or_default()
                    .push((row.t_ms, row.base_n, row.sel_n));
                let e = key_bucket_sum.entry((row.akey, row.t_ms)).or_default();
                e.0 += row.base_n;
                e.1 += row.sel_n;
            }
        }
        // Per-bucket baseline/selection totals (the denominators).
        let mut totals: BTreeMap<i64, (u64, u64)> = BTreeMap::new();
        {
            let mut stream = self
                .dispatch
                .query_stream::<CompareTotalsRow, _>(totals_sql, &settings, |e| {
                    map_trace_metrics_error(e, &self.config)
                })
                .await?;
            while let Some(row) = stream.next().await {
                let row = row.map_err(|e| map_trace_metrics_error(e, &self.config))?;
                totals.insert(row.t_ms, (row.base_total, row.sel_total));
            }
        }

        let mut series: Vec<TraceMetricSeries> = Vec::new();
        let meta = |kind: &str, key: &str, val: &str| -> Vec<MetricLabel> {
            vec![
                MetricLabel::str("__meta_type", kind),
                MetricLabel::str(key, val),
            ]
        };
        // The per-key-per-side rank-and-keep buffers, reused across keys so
        // the ranking allocates once rather than once per attribute. Both
        // are bounded by the probe-capped distinct-pair count.
        let mut rank_base: Vec<(u64, &str)> = Vec::new();
        let mut rank_sel: Vec<(u64, &str)> = Vec::new();
        for key in &keys {
            // topN (issue #460): rank this key's present values on EACH
            // side by that side's window sum, keep the top `top_n`.
            rank_base.clear();
            rank_sel.clear();
            for ((k, val), rows) in per_kv.range((key.clone(), String::new())..) {
                if k != key {
                    break;
                }
                let (bsum, ssum) = rows
                    .iter()
                    .fold((0u64, 0u64), |(b, s), (_, rb, rs)| (b + rb, s + rs));
                rank_base.push((bsum, val.as_str()));
                rank_sel.push((ssum, val.as_str()));
            }
            let keep_base = keep_top_n(&mut rank_base, top_n);
            let keep_sel = keep_top_n(&mut rank_sel, top_n);

            // Present values: one baseline + one selection series each,
            // emitted only for that side's kept set.
            for ((k, val), rows) in per_kv.range((key.clone(), String::new())..) {
                if k != key {
                    break;
                }
                if keep_base.contains(val) {
                    let base: Vec<(i64, f64)> =
                        rows.iter().map(|(t, b, _)| (*t, *b as f64)).collect();
                    series.push(TraceMetricSeries {
                        labels: meta("baseline", key, val),
                        samples: base,
                        exemplars: vec![],
                    });
                }
                if keep_sel.contains(val) {
                    let sel: Vec<(i64, f64)> =
                        rows.iter().map(|(t, _, s)| (*t, *s as f64)).collect();
                    series.push(TraceMetricSeries {
                        labels: meta("selection", key, val),
                        samples: sel,
                        exemplars: vec![],
                    });
                }
            }
            // `key=nil` complement + the `*_total` denominators, per
            // bucket. Built from the UNTRIMMED `key_bucket_sum`: a value
            // topN dropped is gone, never folded into the complement or
            // the denominator (`selection_total` stays the whole
            // population, which is what the reference returns).
            let mut base_nil = Vec::new();
            let mut sel_nil = Vec::new();
            let mut base_total = Vec::new();
            let mut sel_total = Vec::new();
            for (&t, &(bt, st)) in &totals {
                let (bp, sp) = key_bucket_sum
                    .get(&(key.clone(), t))
                    .copied()
                    .unwrap_or((0, 0));
                base_nil.push((t, bt.saturating_sub(bp) as f64));
                sel_nil.push((t, st.saturating_sub(sp) as f64));
                base_total.push((t, bt as f64));
                sel_total.push((t, st as f64));
            }
            series.push(TraceMetricSeries {
                labels: meta("baseline", key, "nil"),
                samples: base_nil,
                exemplars: vec![],
            });
            series.push(TraceMetricSeries {
                labels: meta("selection", key, "nil"),
                samples: sel_nil,
                exemplars: vec![],
            });
            series.push(TraceMetricSeries {
                labels: meta("baseline_total", key, "nil"),
                samples: base_total,
                exemplars: vec![],
            });
            series.push(TraceMetricSeries {
                labels: meta("selection_total", key, "nil"),
                samples: sel_total,
                exemplars: vec![],
            });
        }
        // The well-known-absent-attribute universe (issue #182 review Fix
        // 3): Tempo enumerates a fixed set of intrinsic/resource/OTLP-
        // semconv keys even when no span carries them, as `key=nil`. For a
        // fully-absent key every span lacks it, so `baseline`/`selection`
        // `key=nil` equal the totals. Captured black-box + OTLP semconv
        // (see [`WELL_KNOWN_COMPARE_KEYS`]). The `continue` below is what
        // lets a now-data-driven well-known key (issue #189:
        // `statusMessage`/`rootName`/`rootServiceName`) render its real
        // present values instead — it is enumerated as `key=nil` here only
        // when NO span carried it.
        for &wk in super::metrics_plan::WELL_KNOWN_COMPARE_KEYS {
            if keys.contains(wk) {
                continue; // already enumerated from present data
            }
            let base_total: Vec<(i64, f64)> =
                totals.iter().map(|(&t, &(b, _))| (t, b as f64)).collect();
            let sel_total: Vec<(i64, f64)> =
                totals.iter().map(|(&t, &(_, s))| (t, s as f64)).collect();
            for kind in ["baseline", "baseline_total"] {
                series.push(TraceMetricSeries {
                    labels: meta(kind, wk, "nil"),
                    samples: base_total.clone(),
                    exemplars: vec![],
                });
            }
            for kind in ["selection", "selection_total"] {
                series.push(TraceMetricSeries {
                    labels: meta(kind, wk, "nil"),
                    samples: sel_total.clone(),
                    exemplars: vec![],
                });
            }
        }
        // Drop all-zero meta-series (e.g. a selection value never appears
        // under `baseline`) — matches Tempo's omission of empty series.
        series.retain(|s| s.samples.iter().any(|(_, v)| *v != 0.0));
        Ok(TraceMetricsResult { series })
    }

    /// Runs the exemplar-collection query (issue #182 P5) and attaches a
    /// bounded `trace:id` exemplar per sampled span to the series it
    /// belongs to. Each exemplar carries that series' value at its own
    /// bucket and the span's own timestamp (only the trace reference is
    /// emitted; the sampled `span_id` is not a wire field).
    ///
    /// The bucket lookup keys on the row's `t`, which after issue #477 (b)
    /// is the RIGHT-CLOSED label — the same label the series' samples are
    /// stamped with. A left-edge lookup returned the previous bucket's
    /// value, which is the wrong number whenever the two buckets differ.
    ///
    /// **Which series** is decided by the row, not by position (issue
    /// #477 wave 2). A grouped range answer is one series per group
    /// value; attaching every row to `series.first_mut()` put the second
    /// group's traces on the first group's line, and read the value out
    /// of that first series — which for a densified shape is the `0.0`
    /// of a bucket where that group had nothing, and for a sparse
    /// aggregation is a bucket the series does not carry at all. Both
    /// render as a measured zero. The SQL now returns `g0` for the
    /// grouped shapes ([`TraceMetricsPlan::exemplar_group_label`]) and
    /// the row is matched to the series carrying that label value.
    ///
    /// A row whose group has no series, or whose bucket the matched
    /// series does not carry, is DROPPED rather than attached at `0.0`:
    /// neither can arise from a consistent pair of statements over the
    /// same rows and the same window, and a fabricated zero is worse than
    /// a missing exemplar because it reads as a measurement.
    ///
    /// The SQL's `groupArraySample(k, …)` is a PER-BUCKET bound; the
    /// resolved budget is a TOTAL (ruling 1 on issue #477), so the
    /// collected list is thinned to it here — across every series, since
    /// the budget is for the whole response.
    async fn attach_range_exemplars(
        &self,
        plan: &TraceMetricsPlan,
        result: &mut TraceMetricsResult,
    ) -> Result<(), ReadError> {
        let Some(exemplar_sql) = plan.exemplar_sql() else {
            return Ok(());
        };
        if result.series.is_empty() {
            return Ok(());
        }
        // Bucket label (ms) → the value of series `i` at that bucket.
        let value_at: Vec<BTreeMap<i64, f64>> = result
            .series
            .iter()
            .map(|s| s.samples.iter().copied().collect())
            .collect();
        let settings = metrics_settings(&self.config);
        let sql = exemplar_sql;
        // `(series index, exemplar)`, in bucket order, so the thinning
        // stride below is taken over the whole response exactly as it was
        // when every exemplar lived on one series.
        let mut collected: Vec<(usize, MetricExemplar)> = Vec::new();
        match plan.exemplar_key() {
            ExemplarSeriesKey::Single => {
                let mut stream = self
                    .dispatch
                    .query_stream::<MetricExemplarRow, _>(sql, &settings, |e| {
                        map_trace_metrics_error(e, &self.config)
                    })
                    .await?;
                while let Some(row) = stream.next().await {
                    let row = row.map_err(|e| map_trace_metrics_error(e, &self.config))?;
                    push_bucket_exemplars(&mut collected, 0, &value_at[0], row.t_ms, row.ex);
                    decimate_if_full(&mut collected);
                }
            }
            ExemplarSeriesKey::Group { label } => {
                // Group value → the index of the series carrying it. Built
                // from the framed answer, so a group the answer does not
                // contain has nowhere to land and its rows are dropped.
                let index: HashMap<&str, usize> = result
                    .series
                    .iter()
                    .enumerate()
                    .filter_map(|(i, s)| series_label_value(s, label).map(|v| (v, i)))
                    .collect();
                let mut stream = self
                    .dispatch
                    .query_stream::<MetricGroupExemplarRow, _>(sql, &settings, |e| {
                        map_trace_metrics_error(e, &self.config)
                    })
                    .await?;
                while let Some(row) = stream.next().await {
                    let row = row.map_err(|e| map_trace_metrics_error(e, &self.config))?;
                    let Some(&i) = index.get(row.g0.as_str()) else {
                        continue;
                    };
                    push_bucket_exemplars(&mut collected, i, &value_at[i], row.t_ms, row.ex);
                    decimate_if_full(&mut collected);
                }
            }
            ExemplarSeriesKey::Quantile => {
                // No index: the join is numeric. Each sampled span goes to
                // the `p=` series whose value at the span's OWN bucket is
                // nearest the span's own duration.
                let mut stream = self
                    .dispatch
                    .query_stream::<MetricQuantileExemplarRow, _>(sql, &settings, |e| {
                        map_trace_metrics_error(e, &self.config)
                    })
                    .await?;
                while let Some(row) = stream.next().await {
                    let row = row.map_err(|e| map_trace_metrics_error(e, &self.config))?;
                    push_quantile_exemplars(&mut collected, &value_at, row.t_ms, row.ex);
                    decimate_if_full(&mut collected);
                }
            }
            ExemplarSeriesKey::HistogramBucket => {
                // Bucket bound → the index of the `__bucket` series
                // carrying it. Keyed on the RENDERED label (the seconds
                // double the framing put on the wire) so the join is the
                // exact inverse of the construction, not a re-derivation.
                let index: HashMap<u64, usize> = result
                    .series
                    .iter()
                    .enumerate()
                    .map(|(i, s)| (bucket_label(s).to_bits(), i))
                    .collect();
                let mut stream = self
                    .dispatch
                    .query_stream::<MetricLog2ExemplarRow, _>(sql, &settings, |e| {
                        map_trace_metrics_error(e, &self.config)
                    })
                    .await?;
                while let Some(row) = stream.next().await {
                    let row = row.map_err(|e| map_trace_metrics_error(e, &self.config))?;
                    let key = log2_histogram::bucket_seconds(row.bucket_ns).to_bits();
                    let Some(&i) = index.get(&key) else {
                        continue;
                    };
                    push_bucket_exemplars(&mut collected, i, &value_at[i], row.t_ms, row.ex);
                    decimate_if_full(&mut collected);
                }
            }
            ExemplarSeriesKey::CompareSide => {
                // `(__meta_type, attribute key)` → series index, over the
                // two TOTAL meta-types only: those are the series the
                // reference attaches a comparison exemplar to. A key whose
                // totals series the answer dropped (all-zero) has nowhere
                // to land and its rows are dropped.
                let index: HashMap<(&str, &str), usize> = result
                    .series
                    .iter()
                    .enumerate()
                    .filter_map(|(i, s)| compare_total_series_key(s).map(|k| (k, i)))
                    .collect();
                let mut stream = self
                    .dispatch
                    .query_stream::<MetricCompareExemplarRow, _>(sql, &settings, |e| {
                        map_trace_metrics_error(e, &self.config)
                    })
                    .await?;
                while let Some(row) = stream.next().await {
                    let row = row.map_err(|e| map_trace_metrics_error(e, &self.config))?;
                    let kind = compare_total_meta_type(row.is_sel);
                    let Some(&i) = index.get(&(kind, row.akey.as_str())) else {
                        continue;
                    };
                    push_bucket_exemplars(&mut collected, i, &value_at[i], row.t_ms, row.ex);
                    decimate_if_full(&mut collected);
                }
            }
        }
        thin_collected_exemplars(&mut collected, plan.exemplar_budget());
        for series in result.series.iter_mut() {
            series.exemplars.clear();
        }
        for (i, ex) in collected {
            result.series[i].exemplars.push(ex);
        }
        Ok(())
    }

    /// Executes a metrics instant plan (issue #59): the same pushed-down
    /// body over the whole snapped window `[S, E)` with no bucketing —
    /// exactly one row (`uniqExact` with no `GROUP BY`), returned as a
    /// one-sample label-less vector; the instant `rate` denominator is
    /// the snapped window width (plan v2 delta 2). The caller stamps the
    /// sample at [`TraceMetricsPlan::snapped_end_ms`].
    pub async fn metrics_instant(
        &self,
        plan: &TraceMetricsPlan,
    ) -> Result<TraceMetricsResult, ReadError> {
        let mut result = self.frame_instant(plan).await?;
        if let Some(rf) = plan.result_filter() {
            apply_result_filter(rf, &mut result);
        }
        if let Some(reduce) = plan.reduce() {
            apply_series_reduce(reduce, &mut result);
        }
        Ok(result)
    }

    /// Frames the first-stage instant result (before the P5 reduction).
    async fn frame_instant(
        &self,
        plan: &TraceMetricsPlan,
    ) -> Result<TraceMetricsResult, ReadError> {
        self.enforce_series_cap(plan.instant_probe_sql()).await?;
        if plan.kind() == PlanKind::Compare {
            let (cross_tab, totals) = plan
                .compare_instant()
                .expect("compare plan carries instant SQL");
            return self
                .frame_compare(cross_tab, totals, plan.compare_top_n())
                .await;
        }
        let settings = metrics_settings(&self.config);
        let sql = plan.instant_sql();
        let at_ms = plan.snapped_end_ms();
        match (plan.kind(), plan.group_label()) {
            (PlanKind::Quantile, _) => {
                let quantiles = plan.quantiles();
                let mut qs: Vec<f64> = Vec::new();
                let mut stream = self
                    .dispatch
                    .query_stream::<MetricQuantileInstantRow, _>(sql, &settings, |e| {
                        map_trace_metrics_error(e, &self.config)
                    })
                    .await?;
                while let Some(row) = stream.next().await {
                    qs = row
                        .map_err(|e| map_trace_metrics_error(e, &self.config))?
                        .qs;
                }
                Ok(TraceMetricsResult {
                    series: quantiles
                        .iter()
                        .enumerate()
                        .map(|(i, q)| {
                            let v = agg_value(finite_or_zero(qs.get(i).copied().unwrap_or(0.0)));
                            TraceMetricSeries {
                                labels: vec![MetricLabel::double("p", *q)],
                                samples: vec![(at_ms, v)],
                                exemplars: vec![],
                            }
                        })
                        .collect(),
                })
            }
            (PlanKind::Histogram, _) => {
                // The instant twin of the range arm: one plain tally per
                // OCCUPIED power-of-two bucket over the whole snapped
                // window (issue #252).
                let mut series: Vec<TraceMetricSeries> = Vec::new();
                let mut stream = self
                    .dispatch
                    .query_stream::<MetricLog2BucketInstantRow, _>(sql, &settings, |e| {
                        map_trace_metrics_error(e, &self.config)
                    })
                    .await?;
                while let Some(row) = stream.next().await {
                    let row = row.map_err(|e| map_trace_metrics_error(e, &self.config))?;
                    series.push(TraceMetricSeries {
                        labels: vec![MetricLabel::double(
                            "__bucket",
                            log2_histogram::bucket_seconds(row.bucket_ns),
                        )],
                        samples: vec![(at_ms, row.n as f64)],
                        exemplars: vec![],
                    });
                }
                sort_histogram_series_by_bucket_ascending(&mut series);
                Ok(TraceMetricsResult { series })
            }
            (kind, None) => {
                // Ungrouped: exactly one row (aggregate with no GROUP BY).
                let value = match kind {
                    PlanKind::Count { is_rate } => {
                        let denom = plan.window_seconds();
                        let mut n: u64 = 0;
                        let mut stream = self
                            .dispatch
                            .query_stream::<MetricCountRow, _>(sql, &settings, |e| {
                                map_trace_metrics_error(e, &self.config)
                            })
                            .await?;
                        while let Some(row) = stream.next().await {
                            n = row.map_err(|e| map_trace_metrics_error(e, &self.config))?.n;
                        }
                        count_value(is_rate, n, denom)
                    }
                    PlanKind::Agg(_) => {
                        // `any(...)` over an empty set yields no row; an
                        // empty aggregate window is a 0-valued sample.
                        let mut v: Option<f64> = None;
                        let mut stream = self
                            .dispatch
                            .query_stream::<MetricAggInstantRow, _>(sql, &settings, |e| {
                                map_trace_metrics_error(e, &self.config)
                            })
                            .await?;
                        while let Some(row) = stream.next().await {
                            v = Some(row.map_err(|e| map_trace_metrics_error(e, &self.config))?.v);
                        }
                        v.map(agg_value).unwrap_or(0.0)
                    }
                    PlanKind::Quantile | PlanKind::Histogram | PlanKind::Compare => {
                        unreachable!("quantile/histogram are framed above")
                    }
                };
                Ok(TraceMetricsResult {
                    series: vec![TraceMetricSeries {
                        labels: vec![MetricLabel::str("__name__", plan.metric_name())],
                        samples: vec![(at_ms, value)],
                        exemplars: vec![],
                    }],
                })
            }
            (kind, Some(label)) => {
                let mut by_group: BTreeMap<String, f64> = BTreeMap::new();
                match kind {
                    PlanKind::Count { is_rate } => {
                        let denom = plan.window_seconds();
                        let mut stream = self
                            .dispatch
                            .query_stream::<MetricGroupCountInstantRow, _>(sql, &settings, |e| {
                                map_trace_metrics_error(e, &self.config)
                            })
                            .await?;
                        while let Some(row) = stream.next().await {
                            let row = row.map_err(|e| map_trace_metrics_error(e, &self.config))?;
                            by_group.insert(row.g0, count_value(is_rate, row.n, denom));
                        }
                    }
                    PlanKind::Agg(_) => {
                        let mut stream = self
                            .dispatch
                            .query_stream::<MetricAggGroupInstantRow, _>(sql, &settings, |e| {
                                map_trace_metrics_error(e, &self.config)
                            })
                            .await?;
                        while let Some(row) = stream.next().await {
                            let row = row.map_err(|e| map_trace_metrics_error(e, &self.config))?;
                            by_group.insert(row.g0, agg_value(row.v));
                        }
                    }
                    PlanKind::Quantile | PlanKind::Histogram | PlanKind::Compare => {
                        unreachable!("quantile/histogram are framed above")
                    }
                }
                Ok(TraceMetricsResult {
                    series: by_group
                        .into_iter()
                        .map(|(g, value)| TraceMetricSeries {
                            labels: vec![MetricLabel::str(label, g)],
                            samples: vec![(at_ms, value)],
                            exemplars: vec![],
                        })
                        .collect(),
                })
            }
        }
    }

    /// Executes the §4.5 service-graph read (issue #173): one fully-pushed-
    /// down two-level aggregation over the `trace_edges` half-row ledger —
    /// per-side dedup, the within-`conn_type` `pair_id` equi-join, and the
    /// per-`(client, server, conn_type)` rollup all happen in ClickHouse; the
    /// engine only frames at most [`graph_sql::SERVICE_GRAPH_MAX_EDGES`]
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
        let cap = graph_sql::SERVICE_GRAPH_MAX_EDGES;
        let sql = graph_sql::service_graph_sql(window, &self.config.edges_table, cap);
        let settings = graph_settings(&self.config);
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

    /// Streams the §4.2 point read for one trace. `hex32` must already be
    /// validated as exactly 32 lowercase hex chars (the server's
    /// `parse_trace_id` is the one validation point) — injection-safe
    /// because only `[0-9a-f]` can then reach the `unhex('...')` literal.
    /// An empty `Vec` means the trace is absent (the handler maps that to
    /// `404`); duplicate `span_id`s from at-least-once ingest are returned
    /// as stored — dedup is the assembler's read-time concern.
    ///
    /// **Issue #509: no longer exempt from the query-text guard — it
    /// PASSES it.** `point_read_sql` is a fixed template plus 32
    /// caller-validated hex chars, SQL well under 1 KiB by construction
    /// with no unbounded-width component (pinned by
    /// `point_read_sql_stays_under_4kib_by_construction` in this
    /// module's tests), so the guard `traces::dispatch` now applies to
    /// every read on this path can never fire here. Issue #35's
    /// exemption was a statement about where the check ran, not about
    /// this query being unable to survive it.
    pub async fn fetch_by_id(&self, hex32: &str) -> Result<Vec<StoredSpan>, ReadError> {
        let sql = super::sql::point_read_sql(&self.config.spans_table, hex32);
        let mut spans = Vec::new();
        // Scoped stream: the pooled-connection lease is dropped when this
        // binding leaves scope at the end of the function, after full
        // consumption.
        //
        // Issue #398: this read sent a bare `QuerySettings::new()` — no
        // budget of any kind — and mapped both `ChError` seams with
        // `ReadError::Clickhouse` DIRECTLY, bypassing
        // [`map_trace_read_error`] entirely. It now carries
        // [`catalog_settings`] (the point read is a primary-key-prefix
        // lookup on the Traces family, the same read-budget class as the
        // catalog scans) and routes both seams through the shared mapper,
        // so a memory breach here is a `422`, not a `500`.
        let settings = catalog_settings(&self.config);
        let mut stream = self
            .dispatch
            .query_stream::<StoredSpanRow, _>(&sql, &settings, |e| {
                map_trace_read_error(e, &self.config)
            })
            .await?;
        while let Some(row) = stream.next().await {
            let row = row.map_err(|e| map_trace_read_error(e, &self.config))?;
            spans.push(StoredSpan::from(row));
        }
        Ok(spans)
    }

    /// Streams the §4.3 tag-names read (issue #58): distinct
    /// `(scope, key)` pairs from the Global tag catalog — only ever
    /// [`tags_sql::CATALOG_TABLE`](super::tags_sql) (`trace_tag_catalog`,
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
    pub async fn list_tag_names(&self, scope: Option<&str>) -> Result<TagNames, ReadError> {
        let scope_literal = scope.map(crate::logql::escape::ch_string);
        let sql = super::tags_sql::tag_names_sql(scope_literal.as_deref(), TAG_NAMES_MAX + 1);
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
    /// least one term it becomes `attr_values_narrowed_sql` over
    /// `trace_attrs_idx` intersected with the matching span set, carrying
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
        let key_literal = crate::logql::escape::ch_string(key);
        let scope_literal = scope.map(crate::logql::escape::ch_string);
        let narrowing = req.narrowing();
        let (sql, settings) = if narrowing.is_empty() {
            (
                super::tags_sql::tag_values_sql(
                    &key_literal,
                    scope_literal.as_deref(),
                    TAG_VALUES_MAX + 1,
                ),
                catalog_settings(&self.config),
            )
        } else {
            // Issue #509: NEITHER branch escapes here, and the comment
            // this replaced is why. It claimed only the narrowing term
            // could carry a `?` — "the catalog shape cannot contain one"
            // — but the unnarrowed shape inlines the requested attribute
            // KEY, and an OTLP attribute may be named `http.target?raw`.
            // Measured at `acf44c49`: that key was a `500` on all three
            // values routes, `a??b` silently read back key `a?b`, and
            // `a?fields` had the row's column list substituted into the
            // literal. The doubling now happens once, in
            // `traces::dispatch`, for every read on this path.
            (
                super::tags_sql::attr_values_narrowed_sql(
                    self.span_filter_ctx(),
                    &key_literal,
                    scope_literal.as_deref(),
                    req.days(),
                    narrowing.terms(),
                    TAG_VALUES_MAX + 1,
                ),
                metrics_settings(&self.config),
            )
        };
        self.stream_tag_values(&sql, &settings).await
    }

    /// Streams the §4.3 SPAN-NAME values read (issue #478 Part 1) —
    /// ALWAYS store-backed, never the catalog.
    ///
    /// The catalog cannot answer this: `trace_tag_catalog_mv` selects from
    /// `trace_attrs_idx` alone, so no span-`name` row exists in it. What a
    /// bare `name` lookup reached before was span EVENT names under a
    /// reserved intrinsic scope; reading `trace_spans.name` is
    /// structurally immune to that, because that column holds span names
    /// and nothing else.
    ///
    /// Every value is typed `string` EXPLICITLY rather than left empty:
    /// `traces_api::tags_response::entry_type` renders an empty
    /// `val_type` as `string` too, so an unset type would produce
    /// byte-identical output while meaning something different (issue
    /// #476's legacy-row branch). The type is set here, where it is a
    /// fact about the column rather than a fallback.
    pub async fn list_span_name_values(
        &self,
        req: TagValuesRequest<'_>,
    ) -> Result<TagValues, ReadError> {
        let narrowing = req.narrowing();
        let sql = super::tags_sql::span_name_values_sql(
            self.span_filter_ctx(),
            req.days(),
            narrowing.terms(),
            TAG_VALUES_MAX + 1,
        );
        let settings = metrics_settings(&self.config);
        let mut values = Vec::new();
        // Scoped stream: same lease/drain contract as list_tag_names.
        let mut stream = self
            .dispatch
            .query_stream::<SpanNameRow, _>(&sql, &settings, |e| {
                map_trace_read_error(e, &self.config)
            })
            .await?;
        while let Some(row) = stream.next().await {
            let row = row.map_err(|e| map_trace_read_error(e, &self.config))?;
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
    ) -> Result<TagValues, ReadError> {
        let mut values = Vec::new();
        // Scoped stream: same lease/drain contract as list_tag_names.
        let mut stream = self
            .dispatch
            .query_stream::<TagValueRow, _>(sql, settings, |e| {
                map_trace_read_error(e, &self.config)
            })
            .await?;
        while let Some(row) = stream.next().await {
            let row = row.map_err(|e| map_trace_read_error(e, &self.config))?;
            values.push(TagValue {
                val: row.val,
                val_type: row.val_type,
            });
        }
        let truncated = values.len() > TAG_VALUES_MAX;
        values.truncate(TAG_VALUES_MAX);
        Ok(TagValues { values, truncated })
    }

    /// The table pair the store-backed tag reads name — the same
    /// `TraceReadConfig` fields (and therefore the same `_dist` suffixing)
    /// the whole search path uses.
    fn span_filter_ctx(&self) -> super::filter::SpanFilterCtx<'_> {
        super::filter::SpanFilterCtx {
            spans_table: &self.config.spans_table,
            attrs_table: &self.config.attrs_table,
        }
    }

    /// Executes a [`SearchPlan`] end to end (module doc for the model).
    pub async fn search(&self, plan: &SearchPlan) -> Result<SearchOutput, ReadError> {
        self.search_inner(plan, None).await
    }

    /// One execution that also captures the per-stage SQL trace — same
    /// single-pass contract as `LogQlEngine::query_explained` (no double
    /// execution).
    pub async fn search_explained(
        &self,
        plan: &SearchPlan,
    ) -> Result<(SearchOutput, PlanExplain), ReadError> {
        let mut explain = PlanExplain::new("traces");
        let output = self.search_inner(plan, Some(&mut explain)).await?;
        Ok((output, explain))
    }

    fn search_settings(&self) -> QuerySettings {
        search_settings(&self.config)
    }

    /// Runs one search query to completion inside its own scope (the
    /// pooled-connection lease drops at return), charging every row's
    /// retention cost against the Layer-2 budget **as it streams** — the
    /// counter trips mid-stream, so accumulated state never exceeds the
    /// budget by more than the driver's one-block transient (the
    /// documented Layer-1 residual). `charged` accumulates what the
    /// caller must release when it discards the rows.
    ///
    /// **Issue #35: the single choke point for every search-phase
    /// CHARGE.** Every phase-1 generator, phase-2 hydration/membership/
    /// attribute-value batch, and the root-hydration read route through
    /// this one function, so the byte budget is priced, charged and
    /// accepted in one place rather than at each of the half-dozen call
    /// sites.
    ///
    /// **Issue #509: it is no longer the choke point for query TEXT.**
    /// The `?`-doubling and
    /// [`crate::querytext::ensure_query_text_fits`] used to run here,
    /// which covered the search phase and left the tag, point-read and
    /// metrics reads to remember them site by site — three of the
    /// twenty-seven did not. Both now run in `traces::dispatch`, once,
    /// for every read this module issues.
    ///
    /// **Issue #57 re-audit:** `mapper` lets phase-1 generator reads route
    /// through [`map_trace_generator_error`] (which alone maps code 241 →
    /// [`TooBroadReason::TraceGeneratorMemory`]) while every other call
    /// site keeps [`map_trace_read_error`] — a single choke point, two
    /// error taxonomies, never conflated.
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

    /// Runs the spanset `| by(...)` distinct-by-key cardinality probe
    /// (issue #185) before the main search: a `cap+1` result is a static
    /// `422 query_too_broad` ([`TooBroadReason::TraceSearchSeriesCap`]) —
    /// the SAME `reader.traceql_max_series` cap and mechanism as the metric
    /// `by()` cap ([`Self::enforce_series_cap`]). Plans with no probe (no
    /// `by()`, an unsupported by-key, or a composite spanset) return
    /// immediately.
    async fn enforce_search_series_cap(&self, plan: &SearchPlan) -> Result<(), ReadError> {
        let Some(probe) = plan.by_probe_sql() else {
            return Ok(());
        };
        let cap = self.config.max_series;
        let settings = self.search_settings();
        let mut count: u64 = 0;
        // Scoped stream: the probe returns exactly one `count()` row.
        let mut stream = self
            .dispatch
            .query_stream::<MetricCountRow, _>(probe, &settings, |e| {
                map_trace_read_error(e, &self.config)
            })
            .await?;
        while let Some(row) = stream.next().await {
            count = row.map_err(|e| map_trace_read_error(e, &self.config))?.n;
        }
        if count > cap {
            return Err(ReadError::QueryTooBroad(
                TooBroadReason::TraceSearchSeriesCap { count, cap },
            ));
        }
        Ok(())
    }

    async fn search_inner(
        &self,
        plan: &SearchPlan,
        mut explain: Option<&mut PlanExplain>,
    ) -> Result<SearchOutput, ReadError> {
        let settings = self.search_settings();
        let gen_settings = generator_settings(&self.config);
        let mut budget = ByteBudget::new(HYDRATION_BYTE_BUDGET);

        // The spanset `by()` cardinality cap runs before any main-query
        // work (issue #185) — a static `422 query_too_broad` on breach.
        self.enforce_search_series_cap(plan).await?;

        // ---- Phase 1: per-generator bounded ranked queries + merge ----
        // Every pre-hydration Layer-2 charge in this phase is enumerated
        // and bounded by the `traces_search_explain.rs` retention-gate
        // drift guard's P10 formula: `2 * generator_sqls.len() *
        // (plan.max_candidates() + 1) * CANDIDATE_TUPLE_BYTES +
        // BATCH_TRACES * size_of::<[u8; 16]>() + RETAINED_ENTRY_OVERHEAD`
        // — a new pre-hydration charge site must be added to that
        // formula's site inventory, not just this function.
        let gen_probe = plan.max_candidates() + 1;
        let mut generator_truncated = false;
        let mut per_generator: Vec<Vec<([u8; 16], i64)>> = Vec::new();
        let mut phase1_charged = 0usize;
        for sql in &plan.generator_sqls {
            charge_explain(
                &mut explain,
                &mut budget,
                "phase1_candidate_generator",
                sql,
                None,
            )?;
            let rows: Vec<CandidateRow> = self
                .collect_rows_charged(
                    sql,
                    &gen_settings,
                    &mut budget,
                    &mut phase1_charged,
                    map_trace_generator_error,
                    |_| CANDIDATE_TUPLE_BYTES,
                )
                .await?;
            if rows.len() as u64 == gen_probe {
                generator_truncated = true;
            }
            per_generator.push(rows.into_iter().map(|r| (r.trace_id, r.bound_ts)).collect());
        }
        // The merge's map + ranked output are charged BEFORE they are
        // built (round-3 audit): the merged entry count is bounded by the
        // charged input rows, so one more `rows × tuple-cost` covers the
        // map-and-output side while both coexist with the inputs; the
        // input-side charge is released once the per-generator Vecs drop,
        // leaving the ranked candidate list charged (at its upper bound)
        // for the rest of the request.
        let total_rows: usize = per_generator.iter().map(Vec::len).sum();
        budget.charge(total_rows * CANDIDATE_TUPLE_BYTES)?;
        let candidates = merge_candidates(&per_generator);
        drop(per_generator);
        budget.release(phase1_charged);
        // Reconcile to the survivor (round-4): the merge map is dead —
        // release the dedup'd difference so only the ranked candidate
        // list's actual entries stay charged.
        budget.release((total_rows - candidates.len()) * CANDIDATE_TUPLE_BYTES);

        // ---- Phase 2: streaming batched exact evaluation --------------
        let limit = plan.limit() as usize;
        // The issue #193 distinct-group cardinality cap: a cross-batch
        // running accumulator enforced INSIDE `by()` grouping at
        // production time (before any `coalesce()` collapse and before
        // winner eviction), so every regrouped form is bounded by the
        // SAME static `422 TraceSearchSeriesCap` as the #185 pre-flight.
        // Inert (no charge, no work) for non-`by()` queries.
        let mut group_counter = GroupCardinalityCounter::new(self.config.max_series);
        let mut heap: std::collections::BinaryHeap<HeapEntry> = std::collections::BinaryHeap::new();
        let mut consumed: u64 = 0;
        let mut ceiling_hit = false;
        let mut overflow_partial = false;
        let mut idx = 0usize;
        while idx < candidates.len() {
            // The consumption ceiling is checked and recorded FIRST (code
            // review round 1: an engaged ceiling with a lookahead
            // candidate present is a partiality source under the
            // exhaustive conservative rule, even when threshold
            // termination is simultaneously eligible — the threshold
            // check must never mask it).
            if consumed >= plan.max_candidates() {
                ceiling_hit = true;
                break;
            }
            // Threshold termination (checked against the NEXT candidate
            // before deciding EOF vs ceiling-stop — the one-row
            // lookahead): no unseen candidate can beat the k-th held
            // match, because bound_ts upper-bounds the public sort key.
            if heap.len() == limit
                && heap
                    .peek()
                    .is_some_and(|worst| candidates[idx].1 < worst.0.sort_key)
            {
                break;
            }
            let remaining = usize::try_from(plan.max_candidates() - consumed).unwrap_or(usize::MAX);
            let take = BATCH_TRACES.min(candidates.len() - idx).min(remaining);
            let mut batch_charged = 0usize;
            // The batch id list is charged before it is collected
            // (round-3 audit; released with the rest of the batch).
            let id_list_charge = take * std::mem::size_of::<[u8; 16]>() + RETAINED_ENTRY_OVERHEAD;
            budget.charge(id_list_charge)?;
            batch_charged += id_list_charge;
            let batch_ids: Vec<[u8; 16]> =
                candidates[idx..idx + take].iter().map(|c| c.0).collect();
            let (traces, overflowed) = self
                .hydrate_batch(
                    plan,
                    &batch_ids,
                    &settings,
                    &mut budget,
                    &mut batch_charged,
                    &mut explain,
                )
                .await?;
            if overflowed {
                overflow_partial = true;
            }
            let attrs = self
                .batch_attrs(
                    plan,
                    &batch_ids,
                    &settings,
                    &mut budget,
                    &mut batch_charged,
                    &mut explain,
                )
                .await?;

            // Matches arrive ALREADY charged — `evaluate_batch` charges
            // every retained byte before allocating it (round-2 finding:
            // charge must never trail materialization); the heap-evict
            // release below returns exactly what was charged
            // (`retained_bytes` is the same capacity-based cost model).
            for m in
                search_eval::evaluate_batch(plan, &traces, &attrs, &mut group_counter, &mut budget)?
            {
                heap.push(HeapEntry(m));
                if heap.len() > limit
                    && let Some(worst) = heap.pop()
                {
                    budget.release(worst.0.retained_bytes());
                }
            }
            // The batch's hydrated rows / membership sets are discarded
            // here — only the heap summaries survive (plan v6 delta 2).
            budget.release(batch_charged);
            drop(traces);
            drop(attrs);

            consumed += take as u64;
            idx += take;
        }

        let partial = generator_truncated || ceiling_hit || overflow_partial;

        // The group-cardinality accumulator has done its enforcement job;
        // release its retained charge (its bytes live in the winners'
        // `retained_bytes` via each `TraceMatch.groups`, released on the
        // usual heap-evict / return path). Success-path only — an error
        // above dropped the whole budget with the request.
        group_counter.release(&mut budget);

        // ---- Winners: rank + trace-wide root hydration -----------------
        let mut winners: Vec<TraceMatch> = heap.into_iter().map(|e| e.0).collect();
        winners.sort_by(|a, b| {
            b.sort_key
                .cmp(&a.sort_key)
                .then(a.trace_id.cmp(&b.trace_id))
        });

        let roots = if winners.is_empty() {
            HashMap::new()
        } else {
            // The winner id list is charged before it is collected and
            // released when it dies with this block (round-4
            // reconciliation).
            let winner_ids_charge =
                winners.len() * std::mem::size_of::<[u8; 16]>() + RETAINED_ENTRY_OVERHEAD;
            budget.charge(winner_ids_charge)?;
            let ids: Vec<[u8; 16]> = winners.iter().map(|w| w.trace_id).collect();
            let sql = plan.root_sql_for(&ids);
            charge_explain(&mut explain, &mut budget, "root_hydration", &sql, None)?;
            let mut root_rows_charged = 0usize;
            let rows: Vec<RootRow> = self
                .collect_rows_charged(
                    &sql,
                    &settings,
                    &mut budget,
                    &mut root_rows_charged,
                    map_trace_read_error,
                    |row: &RootRow| {
                        std::mem::size_of::<RootRow>()
                            + RETAINED_ENTRY_OVERHEAD
                            + row.service.len()
                            + row.name.len()
                    },
                )
                .await?;
            // Transfer the charge with ownership (code review round 1):
            // the transient row Vec is released only AFTER the retained
            // `roots` map has been charged — the map (and the output rows
            // its summaries move into) stays charged for as long as it
            // lives, i.e. until this request returns.
            let roots = pick_roots(rows);
            budget.charge(roots_retained_bytes(&roots))?;
            budget.release(root_rows_charged);
            budget.release(winner_ids_charge);
            roots
        };

        // Output assembly (rounds 3-4): the COMPLETE slot capacity is
        // charged before `Vec::with_capacity` reserves it (round-4: the
        // reservation materializes every slot up front), then each
        // root-summary CLONE's string bytes (the map entry stays live
        // alongside the clone) are charged before that clone is made.
        budget.charge(
            winners.len() * std::mem::size_of::<TraceSearchResult>() + RETAINED_ENTRY_OVERHEAD,
        )?;
        let mut traces: Vec<TraceSearchResult> = Vec::with_capacity(winners.len());
        for w in winners {
            // Charge-before-materialize in BOTH branches (the module's
            // standing invariant), which is why only the fallback's
            // CONSTRUCTION moved into a function, not the branch itself.
            let ctx = match roots.get(&w.trace_id) {
                Some(ctx) => {
                    budget.charge(ctx.root.service.len() + ctx.root.name.len())?;
                    ctx.clone()
                }
                // A winner whose root read returned nothing (TTL race —
                // pathological) falls back to its matched-span metadata
                // rather than being silently dropped.
                None => {
                    let name_len = w
                        .spans
                        .first()
                        .and_then(SpanSummary::name)
                        .map_or(0, str::len);
                    budget.charge(name_len)?;
                    fallback_trace_context(&w.spans, w.sort_key)
                }
            };
            traces.push(TraceSearchResult {
                trace_id: w.trace_id,
                root: ctx.root,
                trace_start_ns: ctx.trace_start_ns,
                trace_duration_ns: ctx.trace_duration_ns,
                matched: w.matched,
                spans: w.spans,
                groups: w.groups,
            });
        }

        let returned = traces.len() as u32;
        Ok(SearchOutput {
            traces,
            partial,
            returned,
            limit: plan.limit(),
        })
    }

    /// Hydrates one batch's spans, groups them per trace, dedups by
    /// `span_id`, and detects per-trace overflow via the `+1` probe.
    async fn hydrate_batch(
        &self,
        plan: &SearchPlan,
        batch_ids: &[[u8; 16]],
        settings: &QuerySettings,
        budget: &mut ByteBudget,
        batch_charged: &mut usize,
        explain: &mut Option<&mut PlanExplain>,
    ) -> Result<(Vec<TraceSpans>, bool), ReadError> {
        let sql = plan.hydration_sql_for(batch_ids);
        charge_explain(explain, budget, "phase2_hydration", &sql, None)?;
        // Charged per row DURING streaming (unbounded String columns are
        // exactly what the Layer-2 counter must bind — `max_result_bytes`
        // does not throw on streamed SELECT shapes).
        let rows: Vec<HydrationRow> = self
            .collect_rows_charged(
                &sql,
                settings,
                budget,
                batch_charged,
                map_trace_read_error,
                |row: &HydrationRow| {
                    std::mem::size_of::<HydrationRow>()
                        + RETAINED_ENTRY_OVERHEAD
                        + row.service.len()
                        + row.name.len()
                },
            )
            .await?;
        group_hydrated_rows(rows, budget, batch_charged)
    }

    /// Runs the batch's attribute membership / aggregate / `select()`
    /// value reads.
    async fn batch_attrs(
        &self,
        plan: &SearchPlan,
        batch_ids: &[[u8; 16]],
        settings: &QuerySettings,
        budget: &mut ByteBudget,
        batch_charged: &mut usize,
        explain: &mut Option<&mut PlanExplain>,
    ) -> Result<BatchAttrs, ReadError> {
        let mut attrs = BatchAttrs::default();
        for probe_idx in 0..plan.probes.len() {
            let sql = plan.membership_sql_for(probe_idx, batch_ids);
            charge_explain(
                explain,
                budget,
                "phase2_attr_membership",
                &sql,
                Some(("probe = ", &plan.probes[probe_idx].key)),
            )?;
            // Issue #479: a probe whose matched VALUE a projection needs
            // decodes the SAME read's fused `v` column. RowBinary is
            // positional, so `StrValueRow` reads the three-column
            // projection with no new row type and no second statement —
            // the pattern the `select()` value read already uses.
            if plan.probe_fuses_value(probe_idx) {
                let rows: Vec<StrValueRow> = self
                    .collect_rows_charged(
                        &sql,
                        settings,
                        budget,
                        batch_charged,
                        map_trace_read_error,
                        |row: &StrValueRow| MEMBERSHIP_ENTRY_BYTES + row.v.len(),
                    )
                    .await?;
                let mut map = HashMap::with_capacity(rows.len());
                for row in rows {
                    // `SELECT DISTINCT trace_id, span_id, v` can return
                    // several rows for one span under a range / regex /
                    // existence predicate. The FIRST wins; the reference
                    // also keeps one arbitrary value (its collector is a
                    // map).
                    map.entry((row.trace_id, row.span_id)).or_insert(row.v);
                }
                attrs.membership.push(ProbeMembership::Values(map));
            } else {
                let rows: Vec<MembershipRow> = self
                    .collect_rows_charged(
                        &sql,
                        settings,
                        budget,
                        batch_charged,
                        map_trace_read_error,
                        |_| MEMBERSHIP_ENTRY_BYTES,
                    )
                    .await?;
                attrs.membership.push(ProbeMembership::Keys(
                    rows.into_iter().map(|r| (r.trace_id, r.span_id)).collect(),
                ));
            }
        }
        for field_idx in 0..plan.agg_fields.len() {
            let sql = plan.agg_values_sql_for(field_idx, batch_ids);
            charge_explain(
                explain,
                budget,
                "phase2_attr_values",
                &sql,
                Some(("aggregate field = ", &plan.agg_fields[field_idx].key)),
            )?;
            let rows: Vec<NumValueRow> = self
                .collect_rows_charged(
                    &sql,
                    settings,
                    budget,
                    batch_charged,
                    map_trace_read_error,
                    |_| NUM_VALUE_ENTRY_BYTES,
                )
                .await?;
            attrs.agg_values.push(
                rows.into_iter()
                    .filter_map(|r| r.v.map(|v| ((r.trace_id, r.span_id), v)))
                    .collect(),
            );
        }
        for field_idx in 0..plan.select_attrs.len() {
            let sql = plan.select_values_sql_for(field_idx, batch_ids);
            charge_explain(
                explain,
                budget,
                "phase2_attr_values",
                &sql,
                Some(("select field = ", &plan.select_attrs[field_idx].key)),
            )?;
            let rows: Vec<StrValueRow> = self
                .collect_rows_charged(
                    &sql,
                    settings,
                    budget,
                    batch_charged,
                    map_trace_read_error,
                    |row: &StrValueRow| MEMBERSHIP_ENTRY_BYTES + row.v.len(),
                )
                .await?;
            let mut map = HashMap::with_capacity(rows.len());
            for row in rows {
                map.insert((row.trace_id, row.span_id), row.v);
            }
            attrs.select_values.push(map);
        }
        // Issue #351: the MULTI-VALUED event/link values — ONE ROW PER
        // VALUE, on the same `(key, scope)` index prefix the literal form
        // probes. Issued only when a leaf compares one against another
        // field (`needs_event_sets()`), so every other query pays
        // nothing.
        //
        // **Row-per-value is the memory contract.** The first cut read
        // `groupUniqArray(...) GROUP BY trace_id, span_id`, which broke
        // this module's own Layer-1 residual bound — "never a-priori
        // row-unbounded", above — because an array column is an
        // unbounded number of capped strings in ONE row: the server-side
        // aggregate state AND the client's decoded row both grew with a
        // span's distinct event count before any charge could run, and
        // phase-2 reads carry no `max_memory_usage`, so a server-side
        // blow-up would have been a 500 rather than the required 422.
        //
        // Now every row is fixed-width columns plus one byte-capped
        // string — the documented block shape. Duplicate rows from
        // at-least-once replays need no `DISTINCT`: ANY-match is
        // unaffected by a repeat and ALL-match compares
        // `matchCount == elemCount`, which a repeat increments on both
        // sides.
        //
        // **The PEAK LIVE SET, stated as a set of structures rather than
        // as what each charge covers** (the second review's point: the
        // first row-per-value cut charged once and held twice — a
        // `Vec<StrValueRow>` of every row AND the per-span map built from
        // it, both live at the same instant). Streaming into the map
        // through [`Self::stream_rows_charged`] means that at any moment
        // the live structures holding a co-loaded value are exactly:
        //
        //   1. the per-span `Vec<String>`/`Vec<f64>` inside `map` — its
        //      capacity, which is the initial 4-slot reservation and then
        //      the doubling slack;
        //   2. `map`'s own entry for that span — ONE per span, not per
        //      value;
        //   3. the string payload itself, which is MOVED out of the row
        //      into (1) and so exists once, never copied;
        //   4. the driver's transiently buffered BLOCK — up to
        //      `max_block_size` (`TRACE_SEARCH_MAX_BLOCK_ROWS`, 4096)
        //      decoded rows, not a single row (review 3's correction).
        //      It is bounded: each row here is fixed-width columns plus
        //      ONE `TRACE_STR_COL_CAP`-capped string, which is exactly
        //      the Layer-1 residual shape this module's contract states,
        //      and `max_result_bytes` (64 MiB, throw) bounds it besides.
        //
        // [`EVENT_VALUE_ENTRY_BYTES`] + the payload length upper-bounds
        // (1)+(2)+(3) per value — see that constant for why the slot is
        // charged at the 4-slot reservation rather than 2× — and (4) is
        // the documented Layer-1 block residual. No second collection
        // exists to hold anything a second time.
        for set_idx in 0..plan.event_sets.len() {
            let sql = plan.event_set_sql_for(set_idx, batch_ids);
            charge_explain(
                explain,
                budget,
                "phase2_event_sets",
                &sql,
                Some(("event/link set = ", plan.event_sets[set_idx].display())),
            )?;
            let mut map: HashMap<SpanKey, EventValues> = HashMap::new();
            if plan.event_sets[set_idx].is_numeric() {
                let mut sink = FnRowSink {
                    cost: |_: &NumValueRow| EVENT_VALUE_ENTRY_BYTES,
                    accept: |row: NumValueRow| {
                        let Some(v) = row.v else { return };
                        match map
                            .entry((row.trace_id, row.span_id))
                            .or_insert_with(|| EventValues::Num(Vec::new()))
                        {
                            EventValues::Num(values) => values.push(v),
                            EventValues::Text(_) => {
                                unreachable!("a numeric set never decodes text values")
                            }
                        }
                    },
                };
                self.stream_rows_charged(
                    &sql,
                    settings,
                    budget,
                    batch_charged,
                    map_trace_read_error,
                    &mut sink,
                )
                .await?;
            } else {
                let mut sink = FnRowSink {
                    cost: |row: &StrValueRow| EVENT_VALUE_ENTRY_BYTES + row.v.len(),
                    accept: |row: StrValueRow| match map
                        .entry((row.trace_id, row.span_id))
                        .or_insert_with(|| EventValues::Text(Vec::new()))
                    {
                        EventValues::Text(values) => values.push(row.v),
                        EventValues::Num(_) => {
                            unreachable!("a text set never decodes numeric values")
                        }
                    },
                };
                self.stream_rows_charged(
                    &sql,
                    settings,
                    budget,
                    batch_charged,
                    map_trace_read_error,
                    &mut sink,
                )
                .await?;
            }
            attrs.event_sets.push(map);
        }
        // Issue #184: the trace-wide co-loads — deliberately WINDOW-FREE
        // `trace_id IN` PK reads (the `root_sql` precedent generalized to
        // the filter phase), so the trace-level intrinsics evaluate
        // full-trace-exact regardless of the search window or the
        // per-trace hydration cap. Issued only when the plan uses the
        // corresponding intrinsics — every other query pays nothing.
        if plan.needs_trace_ctx() {
            let sql = plan.trace_ctx_sql_for(batch_ids);
            charge_explain(explain, budget, "phase2_trace_context", &sql, None)?;
            let rows: Vec<TraceCtxRow> = self
                .collect_rows_charged(
                    &sql,
                    settings,
                    budget,
                    batch_charged,
                    map_trace_read_error,
                    |row: &TraceCtxRow| {
                        std::mem::size_of::<TraceCtxRow>()
                            + RETAINED_ENTRY_OVERHEAD
                            + row.root_name.len()
                            + row.root_service.len()
                    },
                )
                .await?;
            let mut map = HashMap::with_capacity(rows.len());
            for row in rows {
                map.insert(
                    row.trace_id,
                    TraceCtxInfo {
                        trace_start_ns: row.trace_start_ns,
                        trace_end_ns: row.trace_end_ns,
                        root_name: row.root_name,
                        root_service: row.root_service,
                    },
                );
            }
            attrs.trace_ctx = map;
        }
        if plan.needs_child_counts() {
            let sql = plan.child_count_sql_for(batch_ids);
            charge_explain(explain, budget, "phase2_child_counts", &sql, None)?;
            let rows: Vec<ChildCountRow> = self
                .collect_rows_charged(
                    &sql,
                    settings,
                    budget,
                    batch_charged,
                    map_trace_read_error,
                    |_| CHILD_COUNT_ENTRY_BYTES,
                )
                .await?;
            attrs.child_counts = rows
                .into_iter()
                .map(|r| ((r.trace_id, r.parent_id), r.child_count))
                .collect();
        }
        Ok(attrs)
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
/// [`crate::traces::search_sql::TRACE_STR_COL_CAP`]-capped string columns
/// (never a-priori row-unbounded); the Layer-2 retention counter is the
/// binding bound on accumulated state across the whole request.
fn search_settings(config: &TraceReadConfig) -> QuerySettings {
    let base = if config.distributed {
        QuerySettings::clustered_reader(config.skip_unavailable_shards)
    } else {
        QuerySettings::new()
    };
    base.set("max_rows_to_read", config.scan_budget_rows)
        .set("max_bytes_to_read", TRACE_READ_BYTES_BUDGET)
        .set("read_overflow_mode", "throw")
        .set("max_result_bytes", TRACE_MAX_RESULT_BYTES)
        .set("result_overflow_mode", "throw")
        .set("max_block_size", TRACE_SEARCH_MAX_BLOCK_ROWS)
        // Issue #35: the raised `max_query_size` parse-buffer cap — every
        // search-phase read (generators, hydration/membership/attribute
        // batches, root hydration) routes through `collect_rows_charged`,
        // which carries this settings object.
        .set("max_query_size", crate::querytext::MAX_QUERY_TEXT_BYTES)
        // Issue #398: the surface-wide per-query memory ceiling
        // (`reader.traceql_read_max_memory_bytes`), throw-not-spill. Every
        // phase-2 read (hydration/membership/value/root) previously set no
        // memory limit at all, so a memory breach there was a `500`.
        // Phase-1 generator reads override this with their own tighter
        // ceiling in `generator_settings`.
        .set("max_memory_usage", config.read_max_memory_bytes)
        .set("max_bytes_before_external_group_by", 0u64)
}

/// The phase-1 candidate-generator query settings (issue #57 re-audit,
/// sub-problem B): [`search_settings`] plus the generator memory ceiling
/// — `max_memory_usage` (from `config.generator_max_memory_bytes`) and
/// `max_bytes_before_external_group_by = 0` (throw-not-spill: a spilled
/// aggregation would silently slow rather than fail loud). Bounds a
/// dense common-value prefix's `GROUP BY trace_id` aggregation state;
/// breach → server code 241 → [`map_trace_generator_error`].
///
/// **Issue #398: this OVERRIDES [`search_settings`]' surface-wide ceiling
/// with a tighter one (512 MiB by default vs 8 GiB), and that is not a
/// carve-out.** A carve-out leaves a path *unbounded*; this path ends up
/// bounded more strictly, not less — and it keeps its own, more specific
/// [`TooBroadReason::TraceGeneratorMemory`] via [`map_trace_generator_error`],
/// which runs before the shared mapper. Removing it would LOOSEN a
/// shipped guarantee. The `.set` calls below are ordinary overrides
/// (`QuerySettings::set` replaces rather than duplicates), so a generator
/// read carries exactly one `max_memory_usage`, the generator's.
fn generator_settings(config: &TraceReadConfig) -> QuerySettings {
    search_settings(config)
        .set("max_memory_usage", config.generator_max_memory_bytes)
        .set("max_bytes_before_external_group_by", 0u64)
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
    QuerySettings::new()
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

/// The Layer-1 settings every metrics query carries (issue #59 plan v2
/// delta 3): the full search budget set ([`search_settings`]) plus the
/// IN-set limits bounding every attribute semi-join's materialized set
/// (`max_rows_in_set`/`max_bytes_in_set`, throw → code 191 → 422 via the
/// dedicated [`TooBroadReason::TraceMetricsSetRows`]). Clustered mode
/// additionally injects `distributed_product_mode='local'`, rewriting
/// `IN (SELECT … FROM trace_attrs_idx_dist …)` to the **local** shard
/// table — co-sharding on `cityHash64(trace_id)` makes each shard's
/// semi-join exact and kills the `_dist`-inside-`_dist`
/// double-distributed path. (Honesty note: the time-bucket `GROUP BY`
/// itself is *not* shard-local — buckets exist on every shard; the
/// coordinator merges per-bucket partial states, bounded by the point
/// cap × shards. Scale evidence routes to #25.)
/// `compare()`'s per-attribute-per-side rank-and-keep (issue #460).
///
/// `ranked` holds one `(window sum, value)` entry per distinct value on
/// ONE side of ONE attribute. It is ranked descending by sum and the top
/// `top_n` values are returned. The ranking key is the reference's:
/// `topN.add` sums a value's per-bucket counts and `topN.get` sorts
/// descending (`engine_metrics_compare.go:535-563` @ v3.0.2).
///
/// **Ties are broken by ascending value string, and that tie order is
/// OURS.** The reference sorts with `sort.Slice`, which is not stable, so
/// among equal sums its survivors are arbitrary — measured twice on issue
/// #460, two different arbitrary sets from the same input shape.
/// Determinism is a deliberate refinement, ledgered as
/// `traceql-compare-topn-tie-order`; no test may assert WHICH member of a
/// tie survives, only how many do.
///
/// Sorts in place so the caller can reuse one buffer across attributes.
fn keep_top_n(ranked: &mut [(u64, &str)], top_n: usize) -> BTreeSet<String> {
    ranked.sort_unstable_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(b.1)));
    ranked
        .iter()
        .take(top_n)
        .map(|(_, v)| (*v).to_string())
        .collect()
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
        base.set("distributed_product_mode", "local")
    } else {
        base
    }
}

/// The explicit encode-boundary value conversion (issue #59 plan v2
/// delta 5): the SQL side always ships the deduped `UInt64` count;
/// Whether a range shape emits EVERY bucket in the window or only the
/// occupied ones (issue #477 (a)).
///
/// Density follows the FUNCTION, not the query — measured against the
/// reference over one window with interior and edge gaps:
/// `count_over_time`, `rate`, `quantile_over_time`, `histogram_over_time`,
/// `count_over_time by(name)` and every series of a `compare()` come back
/// dense, while `sum`/`min`/`max`/`avg_over_time` come back sparse,
/// grouped or not. A blanket fill would create four fresh divergences;
/// the value aggregations are already correct and must be left alone.
fn densifies(kind: PlanKind) -> bool {
    match kind {
        PlanKind::Count { .. } | PlanKind::Quantile | PlanKind::Histogram | PlanKind::Compare => {
            true
        }
        PlanKind::Agg(_) => false,
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

/// Whether any framed sample carries a non-zero value — the precondition
/// for issuing the exemplar query at all (issue #477 (c)).
fn has_a_non_zero_sample(result: &TraceMetricsResult) -> bool {
    result
        .series
        .iter()
        .any(|s| s.samples.iter().any(|(_, v)| *v != 0.0))
}

/// The string value of `series`' label `key`, if it carries one.
///
/// Grouped range series are labelled `MetricLabel::str(<by-key>, <group
/// value>)` by `frame_range_series`, so this is the inverse of that
/// construction and the join column between an exemplar row's `g0` and
/// the series it belongs to (issue #477 wave 2).
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

/// Places each sampled span of a `quantile_over_time` bucket row on the
/// `p=` series nearest its own duration, and stamps it with that duration
/// (issue #477 wave 2).
///
/// Two rules from the reference, both different from every other shape:
///
/// * **Placement** is nearest-value, not nearest-anything-else: the
///   exemplar goes to the quantile whose value is closest to the span's
///   duration, ties to the lowest index (`assignExemplarToQuantile`,
///   `pkg/traceql/engine_metrics.go:2013-2031 @ v3.0.2` — a strict `<`
///   keeps the first of equals).
/// * **The value is the span's own duration**, not the series' sample.
///   `quantile_over_time(duration)` records a real observed value
///   (`exemplarDuration`, `pkg/traceql/ast_metrics.go:235-239 @ v3.0.2`)
///   and only the NaN placeholders the counting shapes record are
///   rewritten to the series sample at the end
///   (`modules/frontend/combiner/metrics_query_range.go:278-305 @ v3.0.2`).
///   The ns→seconds conversion is [`agg_value`], the same one the series
///   values go through, so the comparison is between like units.
///
/// **The comparison is against the `p` values at the exemplar's OWN
/// bucket — the numbers the panel draws beside it — and that is a
/// deliberate divergence.** The reference compares against a distribution
/// it never draws: it pools every interval's bucket counts into one map
/// and takes the requested quantiles of THAT once per response
/// (`aggregatedBuckets` / `quantileValues`,
/// `pkg/traceql/engine_metrics.go:1933-1962 @ v3.0.2`), while the value
/// each `p=` series carries is computed per interval from that interval's
/// own buckets (`:1993`); placement then compares the exemplar against
/// the pooled array (`:1996-2001`). So the series an exemplar is put on
/// is chosen from numbers that series never carries, and where load
/// varies across the window the two disagree. Its placement function is
/// also unfinished rather than designed: the doc comment promises a `-1`
/// "doesn't fit any quantile reasonably well" return and "reasonable
/// bucket validation" (`:2010-2012`), and the body implements neither —
/// `buckets` is dead past an emptiness check and the nearest index is
/// always returned (`:2013-2031`). Ledgered as
/// `traceql-metrics-quantile-exemplar-placement-domain` in
/// docs/benchmarks/traces-differential-ledger.md, which cross-references
/// `2026-08-05-traceql-quantile-over-time-tdigest` (issue #252): our `p=`
/// values are already computed differently, so placing against our own
/// values is what keeps an exemplar coherent with the series it sits on.
///
/// With one span in a bucket every quantile of that bucket is that span's
/// duration, so every candidate ties and the lowest `p` wins. That is a
/// property of degenerate input — one observation has no spread — not a
/// defect in the rule.
///
/// A bucket no series carries drops the sample, exactly as
/// [`push_bucket_exemplars`] does.
fn push_quantile_exemplars(
    collected: &mut Vec<(usize, MetricExemplar)>,
    value_at: &[BTreeMap<i64, f64>],
    t_ms: i64,
    sampled: Vec<([u8; 16], i64, i64)>,
) {
    for (trace_id, ts_ns, duration_ns) in sampled {
        let value = agg_value(duration_ns as f64);
        let mut best: Option<(usize, f64)> = None;
        for (i, series) in value_at.iter().enumerate() {
            let Some(&q) = series.get(&t_ms) else {
                continue;
            };
            let diff = (value - q).abs();
            let better = match best {
                Some((_, b)) => diff < b,
                None => true,
            };
            if better {
                best = Some((i, diff));
            }
        }
        let Some((i, _)) = best else {
            continue;
        };
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

/// The `__meta_type` a `compare()` exemplar row's side names (issue #477
/// wave 2): the reference attaches baseline samples to `baseline_total`
/// and selection samples to `selection_total`, never to the per-value
/// `baseline`/`selection` series
/// (`pkg/traceql/engine_metrics_compare.go:296-301 @ v3.0.2`).
fn compare_total_meta_type(is_sel: u8) -> &'static str {
    if is_sel == 0 {
        "baseline_total"
    } else {
        "selection_total"
    }
}

/// The `(__meta_type, attribute key)` pair identifying a `compare()`
/// TOTALS series, or `None` for any other series — the inverse of
/// `frame_compare`'s `meta("baseline_total", key, "nil")` construction
/// and the join column for a comparison exemplar row.
fn compare_total_series_key(series: &TraceMetricSeries) -> Option<(&str, &str)> {
    let kind = series_label_value(series, "__meta_type")?;
    if kind != "baseline_total" && kind != "selection_total" {
        return None;
    }
    let key = series.labels.iter().find(|l| l.key != "__meta_type")?;
    Some((kind, key.key.as_str()))
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

/// The count-path (`rate`/`count_over_time`) encode-boundary value:
/// `rate` divides by its denominator (one bucket's width per range
/// sample, the snapped window's width for an instant) in `f64` here —
/// never in SQL; `count_over_time` is the deduped count itself.
///
/// The denominator is **fractional seconds** (issue #477 (d)): a
/// sub-second step, or a snapped window narrower than a second, truncated
/// to `0` under the old whole-second form and made every rate `inf`.
fn count_value(is_rate: bool, n: u64, rate_denominator_seconds: f64) -> f64 {
    if is_rate {
        n as f64 / rate_denominator_seconds
    } else {
        n as f64
    }
}

/// The value-aggregation (`*_over_time`) encode-boundary value: the
/// `toFloat64`-cast aggregate over the physical `duration_ns` scaled
/// nanoseconds→seconds (Tempo's duration-metric unit). Attribute value
/// targets — when wired — will carry a unit scale of 1.
///
/// Issue #237 (settled — do NOT "fix" this like #232): the reference's
/// ns→seconds conversion is the SINGLE-rounding `float64(ns) / 1e9`,
/// not the two-rounding `float64(sec) + float64(nsec)/1e9` form that
/// #232 established for the LogQL rate divisor (a different reference).
/// Evidence of record: 17-significant-digit raw-wire captures from the
/// pinned reference container (`grafana/tempo:3.0.2@sha256:cda87c21…`,
/// the digest in `deploy/e2e/compose.single.yaml`, probed 2026-07-26)
/// for six widths where the two forms differ by 1 ULP —
/// 18_014_398_509_482_025 / _035 / _017, 1_088_608_058_291_172_412,
/// 10_000_000_000_000_005 / _015 ns. Each emitted value is the shortest
/// rendering of the single-rounding f64 at 17 significant digits, which
/// no ≤16-digit formatter can produce and which determines the f64
/// uniquely; every witness exceeds 2^53, so the int64→f64 cast is lossy
/// and the (more accurate) two-rounding value would have been visibly
/// different. Corroboration only (not proof — a response's exemplar
/// label does not establish the numeric field's source path): the same
/// bodies render the raw duration losslessly in an exemplar label while
/// the numeric field carries the cast-first value, and the reference's
/// own comparison operator brackets stored values at the single-rounding
/// f64 (`>= L` matches, `> L` does not, for the 1_118_000_000 ns width).
/// The claim is observed-behaviour only, at emitted-value granularity;
/// no reference source was read. Applying #232's two-rounding fix here
/// would INTRODUCE a 1-ULP divergence — the bit-exact pins in this
/// file's tests carry paired `assert_ne!`s against that form and will
/// fail loudly on such a change.
fn agg_value(v: f64) -> f64 {
    v / 1_000_000_000.0
}

/// Orders `histogram_over_time` series **ascending by bucket** — a
/// deliberate divergence from the reference, ledgered as
/// `2026-08-05-traceql-histogram-series-order` (docs/api.md §4.4.1).
///
/// The reference's `sortResponse`
/// (`modules/frontend/combiner/metrics_query_range.go:245-266 @ v3.0.2`)
/// compares `Label.Value.String()`, and that `Value` is a protobuf
/// `AnyValue` whose `String()` is `proto.CompactTextString`
/// (`pkg/tempopb/common/v1/common.pb.go:46 @ v3.0.2`), ending at gogo's
/// `writeAny` `default:` arm — `fmt.Fprint`, i.e. Go's `%v`/`%g` for a
/// `float64`. So its order is lexicographic on a RENDERING of the
/// bucket, which is neither numeric nor the order of its own JSON body.
/// Measured on the pinned container: spans at 1 µs, 16 µs, 1 ms and 1 s
/// come back `1 ms, 1 µs, 1 s, 16 µs` (capture corpus `mixladder`).
///
/// That is a determinism device, not a semantic one, and a histogram is
/// drawn smallest-bucket-first everywhere a user has seen one — so we
/// emit ascending. **Series ORDER only**: labels, tallies, membership
/// and non-cumulativity all match the reference exactly, and any client
/// that reads the `__bucket` label rather than the array position sees
/// no difference at all.
///
/// The range arm's `BTreeMap` and the instant arm's `ORDER BY bucket ASC`
/// already yield this order; sorting here makes the guarantee local to
/// the framing rather than an inherited property of two other places.
pub fn sort_histogram_series_by_bucket_ascending(series: &mut [TraceMetricSeries]) {
    series.sort_by(|a, b| bucket_label(a).total_cmp(&bucket_label(b)));
}

/// The `__bucket` label's value, or `-inf` for a series without one (a
/// shape this framing never produces — it keeps the comparator total).
fn bucket_label(series: &TraceMetricSeries) -> f64 {
    series
        .labels
        .iter()
        .find(|l| l.key == "__bucket")
        .and_then(|l| match l.value {
            MetricLabelValue::Double(d) => Some(d),
            MetricLabelValue::Str(_) => None,
        })
        .unwrap_or(f64::NEG_INFINITY)
}

/// Sanitizes a non-finite aggregate (e.g. `quantilesTDigest` over an empty
/// bucket yields NaN) to `0.0` so the JSON encoder never emits `NaN`.
fn finite_or_zero(v: f64) -> f64 {
    if v.is_finite() { v } else { 0.0 }
}

/// Per-bucket `(t_ms, baseline_n, selection_n)` counts for one compare()
/// attribute `(key, value)` (issue #182 P6b).
type CompareValueBuckets = Vec<(i64, u64, u64)>;

/// Lowercase hex of a 16-byte trace id (the `trace:id` exemplar label).
fn hex16(bytes: &[u8; 16]) -> String {
    let mut s = String::with_capacity(32);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Applies a metrics-result comparison post-filter (`… > 5`, issue #182
/// P6b): keeps only samples whose value satisfies `value <op> threshold`;
/// series left empty are dropped.
fn apply_result_filter(
    filter: (pulsus_traceql::ComparisonOp, f64),
    result: &mut TraceMetricsResult,
) {
    use pulsus_traceql::ComparisonOp;
    let (op, threshold) = filter;
    let keep = |v: f64| -> bool {
        match op {
            ComparisonOp::Eq => v == threshold,
            ComparisonOp::Neq => v != threshold,
            ComparisonOp::Gt => v > threshold,
            ComparisonOp::Gte => v >= threshold,
            ComparisonOp::Lt => v < threshold,
            ComparisonOp::Lte => v <= threshold,
            // Regex operators are rejected at parse time for result
            // comparisons; treat defensively as no-match.
            ComparisonOp::Re | ComparisonOp::Nre => false,
        }
    };
    for s in &mut result.series {
        s.samples.retain(|(_, v)| keep(*v));
    }
    result.series.retain(|s| !s.samples.is_empty());
}

/// Applies a `topk(n)`/`bottomk(n)` second-stage reduction (issue #182 P5)
/// per timestamp over the series set: at each timestamp the `n` series
/// with the largest (topk) / smallest (bottomk) value keep their sample;
/// the rest drop it. Series left with no samples are removed. Ties break
/// deterministically by series index.
fn apply_series_reduce(reduce: super::metrics_plan::SeriesReduce, result: &mut TraceMetricsResult) {
    use super::metrics_plan::SeriesReduce;
    let (k, top) = match reduce {
        SeriesReduce::TopK(n) => (n as usize, true),
        SeriesReduce::BottomK(n) => (n as usize, false),
    };
    if k == 0 {
        result.series.clear();
        return;
    }
    // Every distinct timestamp across all series.
    let mut timestamps: BTreeSet<i64> = BTreeSet::new();
    for s in &result.series {
        for (t, _) in &s.samples {
            timestamps.insert(*t);
        }
    }
    for t in timestamps {
        // (series_idx, value) for series present at this timestamp.
        let mut present: Vec<(usize, f64)> = result
            .series
            .iter()
            .enumerate()
            .filter_map(|(i, s)| {
                s.samples
                    .iter()
                    .find(|(ts, _)| *ts == t)
                    .map(|(_, v)| (i, *v))
            })
            .collect();
        if present.len() <= k {
            continue;
        }
        // Rank: topk keeps largest values; bottomk keeps smallest. Ties
        // break by series index (ascending) deterministically.
        present.sort_by(|a, b| {
            let ord = a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal);
            if top { ord.reverse() } else { ord }.then(a.0.cmp(&b.0))
        });
        let keep: HashSet<usize> = present.iter().take(k).map(|(i, _)| *i).collect();
        for (i, s) in result.series.iter_mut().enumerate() {
            if !keep.contains(&i) {
                s.samples.retain(|(ts, _)| *ts != t);
            }
        }
    }
    result.series.retain(|s| !s.samples.is_empty());
}

/// A fresh `Vec`'s initial reservation, in element slots: `std`'s
/// `RawVec` first non-zero allocation reserves 4 slots for element types
/// ≤ 1024 bytes (8 for 1-byte elements — every element type here is far
/// larger than 1 B and far smaller than 1 KiB, so 4 is the exact bound).
/// Charged when a fresh per-group Vec (or the batch's outer Vec) is
/// about to make its first push (code review round 5).
const VEC_INITIAL_RESERVATION_SLOTS: usize = 4;

/// Groups a batch's (already per-row-charged) hydration rows into
/// per-trace span lists, deduping `span_id` replays and detecting the
/// per-trace overflow probe — pure, so the accounting is unit-testable
/// (code review round 5).
///
/// Charge model (all BEFORE the allocation they cover):
/// - first group: the outer Vec's initial reservation
///   (`VEC_INITIAL_RESERVATION_SLOTS × size_of::<TraceSpans>()`);
/// - per group: 2× the outer `TraceSpans` slot (doubling slack) +
///   overhead envelope + the fresh inner Vec's initial reservation
///   (`VEC_INITIAL_RESERVATION_SLOTS × size_of::<HydratedSpan>()`);
/// - per UNIQUE span: 2× the inner slot (doubling slack past the initial
///   reservation) + the dedup-set entry at the standard hash-container
///   cost (`[u8; 8]` + `RETAINED_ENTRY_OVERHEAD` — the same envelope as
///   every other set/map site; it also covers the set's own initial
///   bucket group). Replayed rows are checked with `contains` FIRST and
///   are accounting no-ops (round-5 medium: duplicates allocate nothing,
///   so they charge nothing).
///
/// `pub(super)` so the sibling evaluator's
/// `count_matches_the_deduped_span_set` (issue #492 item 2) can prove the
/// dedupe through THIS function rather than through a hand-deduped
/// fixture — `count()` now counts a spanset's members, and that equals
/// the old matched-id-set size only because the rows arriving here are
/// deduped by `span_id`.
pub(super) fn group_hydrated_rows(
    rows: Vec<HydrationRow>,
    budget: &mut ByteBudget,
    batch_charged: &mut usize,
) -> Result<(Vec<TraceSpans>, bool), ReadError> {
    let mut overflowed = false;
    let mut traces: Vec<TraceSpans> = Vec::new();
    let mut raw_count = 0usize;
    let mut seen: HashSet<[u8; 8]> = HashSet::new();
    for row in rows {
        let start_new = traces.last().is_none_or(|t| t.trace_id != row.trace_id);
        if start_new {
            let mut outer_charge = 2 * std::mem::size_of::<TraceSpans>()
                + RETAINED_ENTRY_OVERHEAD
                + VEC_INITIAL_RESERVATION_SLOTS * std::mem::size_of::<HydratedSpan>();
            if traces.is_empty() {
                outer_charge += VEC_INITIAL_RESERVATION_SLOTS * std::mem::size_of::<TraceSpans>();
            }
            budget.charge(outer_charge)?;
            *batch_charged += outer_charge;
            traces.push(TraceSpans {
                trace_id: row.trace_id,
                spans: Vec::new(),
            });
            raw_count = 0;
            seen.clear();
        }
        raw_count += 1;
        if raw_count == MAX_SPANS_PER_TRACE + 1 {
            // The overflow probe row: this trace was truncated at
            // hydration — evaluate the truncated set, mark partial.
            overflowed = true;
            continue;
        }
        if seen.contains(&row.span_id) {
            continue; // at-least-once replay — no allocation, no charge
        }
        let group_charge = 2 * std::mem::size_of::<HydratedSpan>()
            + std::mem::size_of::<[u8; 8]>()
            + RETAINED_ENTRY_OVERHEAD;
        budget.charge(group_charge)?;
        *batch_charged += group_charge;
        seen.insert(row.span_id);
        traces
            .last_mut()
            .expect("a trace group was just pushed")
            .spans
            .push(HydratedSpan {
                span_id: row.span_id,
                parent_id: row.parent_id,
                service: row.service,
                name: row.name,
                timestamp_ns: row.timestamp_ns,
                duration_ns: row.duration_ns,
                status_code: row.status_code,
                status_message: row.status_message,
                kind: row.kind,
                scope_name: row.scope_name,
                scope_version: row.scope_version,
            });
    }
    Ok((traces, overflowed))
}

/// Charges and records one explain stage (round-3 audit: `PlanExplain`
/// retains an SQL clone (+ note) per stage for the whole request — that
/// growth is budgeted like any other retained state, charged BEFORE the
/// clone/format is made). `note` is `(prefix, value)` rendered as
/// `"{prefix}{value}"` so its length is known pre-allocation.
fn charge_explain(
    explain: &mut Option<&mut PlanExplain>,
    budget: &mut ByteBudget,
    name: &'static str,
    sql: &str,
    note: Option<(&str, &str)>,
) -> Result<(), ReadError> {
    if let Some(e) = explain.as_mut() {
        let note_len = note
            .map(|(prefix, value)| prefix.len() + value.len())
            .unwrap_or(0);
        budget.charge(sql.len() + note_len + RETAINED_ENTRY_OVERHEAD)?;
        e.push(
            name,
            sql.to_string(),
            note.map(|(prefix, value)| format!("{prefix}{value}")),
        );
    }
    Ok(())
}

/// The retained cost of the winners' context map — per entry the map key,
/// the [`TraceContext`] struct (the root summary PLUS the two envelope
/// `i64`s, issue #464), its string payloads, and the container-overhead
/// envelope. Charged after [`pick_roots`] and held for the rest of the
/// request (the contexts move into the returned [`TraceSearchResult`]s).
fn roots_retained_bytes(roots: &HashMap<[u8; 16], TraceContext>) -> usize {
    roots
        .values()
        .map(|ctx| {
            std::mem::size_of::<[u8; 16]>()
                + std::mem::size_of::<TraceContext>()
                + RETAINED_ENTRY_OVERHEAD
                + ctx.root.service.len()
                + ctx.root.name.len()
        })
        .sum()
}

/// Picks each trace's root from its trace-wide root-hydration rows —
/// `parent_id` all-zero (earliest such span under `(ts, span_id)`), else
/// the timestamp-earliest span of the full trace — and folds the trace's
/// time envelope over the SAME rows in the SAME pass (issue #464).
///
/// The envelope folds over **every** row, whether or not that row wins the
/// root contest: `min(timestamp_ns)` and `max(timestamp_ns + duration_ns)`
/// over the whole trace, emitted as a WIDTH (`end - start`), because the
/// reference's `Spanset.DurationNanos` is `traceEnd - traceStart`
/// (`tempodb/encoding/vparquet4/schema.go:558-560` @ v3.0.2) and not an
/// end instant. Root selection is unchanged, term for term.
fn pick_roots(rows: Vec<RootRow>) -> HashMap<[u8; 16], TraceContext> {
    /// [`pick_roots`]' per-trace accumulator: the root contest's running
    /// winner, plus the envelope's running `(start, end)`.
    struct RootPick {
        is_root: bool,
        start_ns: i64,
        span_id: [u8; 8],
        summary: RootSummary,
        envelope_start_ns: i64,
        envelope_end_ns: i64,
    }

    let mut best: HashMap<[u8; 16], RootPick> = HashMap::new();
    for row in rows {
        let is_root = row.parent_id == [0u8; 8];
        let span_start = row.timestamp_ns;
        // `saturating_add` rather than `+`: a stored width is
        // non-negative by ingest construction, but a corrupt row must not
        // panic a read.
        let span_end = row.timestamp_ns.saturating_add(row.duration_ns);
        let candidate = RootPick {
            is_root,
            start_ns: row.timestamp_ns,
            span_id: row.span_id,
            summary: RootSummary {
                service: row.service,
                name: row.name,
                start_ns: row.timestamp_ns,
                duration_ns: row.duration_ns,
            },
            envelope_start_ns: span_start,
            envelope_end_ns: span_end,
        };
        match best.get_mut(&row.trace_id) {
            None => {
                best.insert(row.trace_id, candidate);
            }
            Some(current) => {
                // The envelope sees EVERY span; the root contest is
                // decided separately and does not gate it.
                current.envelope_start_ns = current.envelope_start_ns.min(span_start);
                current.envelope_end_ns = current.envelope_end_ns.max(span_end);
                // A true root always beats a non-root; within the same
                // class, earlier (ts, span_id) wins.
                let better = (candidate.is_root && !current.is_root)
                    || (candidate.is_root == current.is_root
                        && (candidate.start_ns, candidate.span_id)
                            < (current.start_ns, current.span_id));
                if better {
                    current.is_root = candidate.is_root;
                    current.start_ns = candidate.start_ns;
                    current.span_id = candidate.span_id;
                    current.summary = candidate.summary;
                }
            }
        }
    }
    best.into_iter()
        .map(|(trace_id, pick)| {
            (
                trace_id,
                TraceContext {
                    root: pick.summary,
                    trace_start_ns: pick.envelope_start_ns,
                    trace_duration_ns: pick.envelope_end_ns.saturating_sub(pick.envelope_start_ns),
                },
            )
        })
        .collect()
}

/// The context for a winner whose trace-wide root read returned NOTHING (a
/// TTL race — pathological): its matched-span metadata rather than a
/// silently dropped trace.
///
/// **Pure and separate from the assembly loop so both fallback fields are
/// unit-testable without an engine** — the assembly branch is reachable
/// only from a live read that loses its rows between phases, so without
/// this split nothing in the suite pins the values (issue #464).
///
/// The width is **zero**, never the matched span's own: no row survives to
/// tell us the trace's extent, and a non-zero width here would be
/// invented.
fn fallback_trace_context(spans: &[SpanSummary], sort_key: i64) -> TraceContext {
    let start_ns = spans.first().map(|s| s.start_ns).unwrap_or(sort_key);
    TraceContext {
        root: RootSummary {
            service: String::new(),
            // Issue #479: the matched span carries a name only when the
            // query collected one, and the first matched span's name is
            // not the root's name in any case. An uncollected name
            // becomes the empty string here, which is consistent with
            // the two fields this path already refuses to invent.
            name: spans
                .first()
                .and_then(SpanSummary::name)
                .unwrap_or_default()
                .to_string(),
            start_ns,
            duration_ns: 0,
        },
        trace_start_ns: start_ns,
        trace_duration_ns: 0,
    }
}

#[cfg(test)]
mod wire_literal {
    /// A reference-captured wire rendering (issue #237). Nothing textual
    /// or numeric leaves this module: every exit is a predicate. There is
    /// no `value()`, no `tokens()`, and no raw-text accessor, conversion,
    /// deref or formatting impl of any spelling — and this copy
    /// deliberately has no text-accepting method at all, so a body assertion is not even
    /// expressible here (no serialized body exists in this file's scope).
    /// The whole block, including the attribute above it, is byte-frozen
    /// by `the_wire_literal_module_is_byte_frozen`; it invokes no macro,
    /// so no macro can be shadowed into it.
    pub(crate) struct WireLiteral(&'static str);

    impl WireLiteral {
        pub(crate) const fn new(text: &'static str) -> Self {
            Self(text)
        }

        /// True iff the captured text is EXACTLY what the locked encoder
        /// emits for `want`: it parses bit-identically to `want` AND
        /// `serde_json::to_string(&want)` reproduces it. This is the
        /// per-row transcription pin for the captured table.
        pub(crate) fn denotes(&self, want: f64) -> bool {
            let parses = match self.0.parse::<f64>() {
                Ok(v) => v.to_bits() == want.to_bits(),
                Err(_) => false,
            };
            let renders = match serde_json::to_string(&want) {
                Ok(s) => s == self.0,
                Err(_) => false,
            };
            parses && renders
        }

        /// Significant digits of the captured rendering (leading and
        /// trailing zeros stripped). A width only discriminates the two
        /// rounding forms formatter-independently if this is 17.
        pub(crate) fn significant_digits(&self) -> usize {
            let mut digits = String::new();
            for c in self.0.chars() {
                if c.is_ascii_digit() {
                    digits.push(c);
                }
            }
            digits.trim_start_matches('0').trim_end_matches('0').len()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Issue #398: a distinctive, non-default
    /// `reader.traceql_read_max_memory_bytes` for these tests —
    /// deliberately unequal to `generator_max_memory_bytes` above, so the
    /// surface-wide ceiling and the generator's tighter one can never be
    /// confused for one another in an assertion.
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

    /// Issue #460 AC 9 — under a TIE, only the CARDINALITY is a
    /// specification.
    ///
    /// The reference's own survivors among equal counts are arbitrary
    /// (`sort.Slice` is not stable; measured twice, two different sets),
    /// so nothing here asserts which value lives. What is asserted is
    /// `n_kept == min(top_n, distinct)` — and, separately, that OUR
    /// choice is deterministic across runs, which is the refinement the
    /// ledger records rather than a parity claim.
    #[test]
    fn the_topn_keep_is_cardinality_only_under_a_tie() {
        let vals: Vec<String> = (0..12).map(|i| format!("v{i:02}")).collect();
        let ranked: Vec<(u64, &str)> = vals.iter().map(|v| (7u64, v.as_str())).collect();
        for top_n in [1usize, 3, 10, 11, 12, 13, 100] {
            let mut buf = ranked.clone();
            let kept = keep_top_n(&mut buf, top_n);
            assert_eq!(
                kept.len(),
                top_n.min(vals.len()),
                "top_n {top_n} over 12 equal-count values must keep min(top_n, 12)"
            );
            // Deterministic, not merely stable-looking: the same input
            // gives the same set, from an INDEPENDENTLY shuffled buffer.
            let mut shuffled: Vec<(u64, &str)> = ranked.iter().rev().copied().collect();
            assert_eq!(
                keep_top_n(&mut shuffled, top_n),
                kept,
                "top_n {top_n}: the kept set must not depend on input order"
            );
        }

        // With DISTINCT sums the set itself is specified, and that is what
        // the live differential's tie-free fixture relies on.
        let mut distinct: Vec<(u64, &str)> = vals
            .iter()
            .enumerate()
            .map(|(i, v)| (i as u64 + 1, v.as_str()))
            .collect();
        assert_eq!(
            keep_top_n(&mut distinct, 3),
            ["v09", "v10", "v11"]
                .into_iter()
                .map(str::to_string)
                .collect::<BTreeSet<String>>()
        );
    }

    #[test]
    fn trace_read_config_is_cloneable_and_debuggable() {
        let config = TraceReadConfig {
            spans_table: "trace_spans".to_string(),
            attrs_table: "trace_attrs_idx".to_string(),
            edges_table: "trace_edges".to_string(),
            max_candidates: 100_000,
            scan_budget_rows: 50_000_000,
            max_series: 1_000,
            generator_max_memory_bytes: 536_870_912,
            read_max_memory_bytes: TEST_READ_MEM,
            distributed: false,
            skip_unavailable_shards: false,
        };
        let clone = config.clone();
        assert_eq!(clone.spans_table, "trace_spans");
        assert_eq!(clone.attrs_table, "trace_attrs_idx");
        assert!(format!("{config:?}").contains("trace_spans"));
    }

    fn cfg() -> TraceReadConfig {
        TraceReadConfig {
            spans_table: "trace_spans".to_string(),
            attrs_table: "trace_attrs_idx".to_string(),
            edges_table: "trace_edges".to_string(),
            max_candidates: 100,
            scan_budget_rows: 1_000,
            max_series: 1_000,
            generator_max_memory_bytes: 536_870_912,
            read_max_memory_bytes: TEST_READ_MEM,
            distributed: false,
            skip_unavailable_shards: false,
        }
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

    /// Issue #133 AC9: `generator_settings` carries `max_memory_usage`
    /// VERBATIM at the accepted minimum (1) and at the maximum
    /// config-accepted `reader.traceql_generator_max_memory_bytes` —
    /// never `0` (ClickHouse-unlimited, a silently disabled
    /// throw-not-OOM guard).
    #[test]
    fn generator_memory_passes_through_verbatim_at_the_accepted_min_and_ceiling() {
        for bytes in [
            1u64,
            pulsus_config::TRACEQL_GENERATOR_MAX_MEMORY_BYTES_CEILING,
        ] {
            let mut c = cfg();
            c.generator_max_memory_bytes = bytes;
            let s = generator_settings(&c);
            assert_eq!(
                s.get("max_memory_usage"),
                Some(bytes.to_string().as_str()),
                "the generator memory ceiling must pass through verbatim"
            );
            assert_ne!(s.get("max_memory_usage"), Some("0"));
        }
    }

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
            // The generator layers its own TIGHTER ceiling on top and wins.
            // Not a carve-out: this path ends up bounded more strictly.
            let g = generator_settings(&c);
            assert_eq!(
                g.get("max_memory_usage"),
                Some(c.generator_max_memory_bytes.to_string().as_str()),
                "the generator's own ceiling must override the surface-wide one"
            );
        }
    }

    /// Issue #133 AC12 (plan v3 delta 3): both sides of the
    /// budget-derived `TRACEQL_MAX_CANDIDATES_CEILING`, via the committed
    /// P10 pre-hydration charge formula
    /// (`2 x generators x (cap + 1) x CANDIDATE_TUPLE_BYTES`): a
    /// single-generator search at the ceiling cap FITS
    /// [`HYDRATION_BYTE_BUDGET`] (a cap-reaching search can complete),
    /// while two generators at the ceiling EXCEED it — and that
    /// aggregate retention fails LOUDLY through [`ByteBudget::charge`]
    /// (the mapped 422 `query_too_broad` path), never by silent
    /// truncation or OOM. Arithmetic identity + charge counters only —
    /// no O(ceiling) allocation.
    #[test]
    fn multi_generator_retention_at_the_candidates_ceiling_fails_loud_through_the_byte_budget() {
        let cap = usize::try_from(pulsus_config::TRACEQL_MAX_CANDIDATES_CEILING)
            .expect("the candidates ceiling fits usize");
        let one_generator = 2 * (cap + 1) * CANDIDATE_TUPLE_BYTES;
        assert!(
            one_generator <= HYDRATION_BYTE_BUDGET,
            "a single generator at the ceiling cap must fit the retention budget \
             ({one_generator} B vs {HYDRATION_BYTE_BUDGET} B)"
        );
        let two_generators = 2 * 2 * (cap + 1) * CANDIDATE_TUPLE_BYTES;
        assert!(
            two_generators > HYDRATION_BYTE_BUDGET,
            "two generators at the ceiling cap must exceed the retention budget \
             ({two_generators} B vs {HYDRATION_BYTE_BUDGET} B)"
        );

        let mut budget = ByteBudget::new(HYDRATION_BYTE_BUDGET);
        match budget.charge(two_generators) {
            Err(ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes { .. })) => {}
            other => panic!("expected a loud ScanBudgetBytes rejection, got {other:?}"),
        }
        // A failed charge never mutates the counter, and the fitting
        // single-generator charge still admits.
        assert_eq!(budget.used(), 0);
        assert!(budget.charge(one_generator).is_ok());
    }

    #[test]
    fn metric_values_convert_at_the_encode_boundary() {
        assert_eq!(count_value(true, 120, 60.0), 2.0);
        assert_eq!(count_value(false, 120, 60.0), 120.0);
        assert_eq!(count_value(true, 0, 3_600.0), 0.0);
        // Value aggregations scale duration ns → seconds.
        assert_eq!(agg_value(2_000_000_000.0), 2.0);
        assert_eq!(agg_value(0.0), 0.0);
    }

    use super::wire_literal::WireLiteral;

    /// Reference-captured ns→seconds renderings (issue #237).
    /// `grafana/tempo:3.0.2@sha256:cda87c21…`, probed 2026-07-26.
    /// `(ns, seconds value, captured rendering, two-rounding rendering)`.
    /// One copy per site by design — do NOT lift into a shared crate.
    const REFERENCE_DURATION_SECONDS: &[(i64, f64, WireLiteral, WireLiteral)] = &[
        // ≤16-digit group: 1 ULP apart; pinned by the reference's own
        // comparison operator (`>= L` matches, `> L` does not).
        (
            1_118_000_000,
            1.118,
            WireLiteral::new("1.118"),
            WireLiteral::new("1.1179999999999999"),
        ),
        (
            1_122_000_000,
            1.122,
            WireLiteral::new("1.122"),
            WireLiteral::new("1.1219999999999999"),
        ),
        (
            1_128_000_000,
            1.128,
            WireLiteral::new("1.128"),
            WireLiteral::new("1.1280000000000001"),
        ),
        (
            1_235_000_000,
            1.235,
            WireLiteral::new("1.235"),
            WireLiteral::new("1.2349999999999999"),
        ),
        (
            31_952_000_000,
            31.952,
            WireLiteral::new("31.952"),
            WireLiteral::new("31.951999999999998"),
        ),
        (
            1_000_064_438,
            1.000064438,
            WireLiteral::new("1.000064438"),
            WireLiteral::new("1.0000644379999999"),
        ),
        // 17-significant-digit group: the formatter-independent RAW-WIRE
        // discriminators (#237 round 3). `ns > 2^53`, so the `int64->f64`
        // cast is lossy and the two-rounding value is the correctly
        // rounded one — the reference emitting the single-rounding value
        // positively identifies a cast-first form.
        (
            18_014_398_509_482_025,
            18_014_398.509_482_022,
            WireLiteral::new("18014398.509482022"),
            WireLiteral::new("18014398.509482026"),
        ),
        (
            18_014_398_509_482_035,
            18_014_398.509_482_037,
            WireLiteral::new("18014398.509482037"),
            WireLiteral::new("18014398.509482034"),
        ),
        (
            18_014_398_509_482_017,
            18_014_398.509_482_015,
            WireLiteral::new("18014398.509482015"),
            WireLiteral::new("18014398.50948202"),
        ),
        (
            1_088_608_058_291_172_412,
            1_088_608_058.291_172_3,
            WireLiteral::new("1088608058.2911723"),
            WireLiteral::new("1088608058.2911725"),
        ),
        (
            10_000_000_000_000_005,
            10_000_000.000_000_004,
            WireLiteral::new("10000000.000000004"),
            WireLiteral::new("10000000.000000006"),
        ),
        (
            10_000_000_000_000_015,
            10_000_000.000_000_017,
            WireLiteral::new("10000000.000000017"),
            WireLiteral::new("10000000.000000015"),
        ),
    ];

    /// Exactly representable under both rounding forms — these prove
    /// nothing on their own and exist only to catch a gross scaling
    /// error. Bit-level only: integral-double JSON rendering is a
    /// protojson number-format question (#263), not the ns→seconds
    /// conversion #237 settles, so controls carry NO wire literal and are
    /// never asserted as text.
    const REFERENCE_DURATION_CONTROLS: &[(i64, f64)] = &[
        (500_000_000, 0.5),
        (1_500_000_000, 1.5),
        (2_000_000_000, 2.0),
    ];

    /// The 17-significant-digit subset of `REFERENCE_DURATION_SECONDS`.
    const SEVENTEEN_DIGIT_WIDTHS: &[i64] = &[
        18_014_398_509_482_025,
        18_014_398_509_482_035,
        18_014_398_509_482_017,
        1_088_608_058_291_172_412,
        10_000_000_000_000_005,
        10_000_000_000_000_015,
    ];

    /// Of those, the ones whose `int64->f64` cast is NOT an exact tie
    /// (ulp = 4 in `[2^54, 2^55)`; `ns % 4 in {1, 3}`). These need no
    /// round-half-to-even assumption anywhere in the chain.
    const TIE_FREE_WIDTHS: &[i64] = &[
        18_014_398_509_482_025,
        18_014_398_509_482_035,
        18_014_398_509_482_017,
        1_088_608_058_291_172_412,
    ];

    /// The two-rounding form, transcribed ONLY so the tests can assert
    /// the production code is NOT it (issues #237 / #232).
    fn two_rounding_seconds(ns: i64) -> f64 {
        (ns / 1_000_000_000) as f64 + (ns % 1_000_000_000) as f64 / 1e9
    }

    // The `__bucket` rendering pins moved to
    // `traces::log2_histogram`'s own tests with the function itself
    // (issue #252) — `bucket_seconds` is no longer an `exec.rs` local.

    /// Issue #252 (owner ruling, 2026-08-05): histogram series come out
    /// ASCENDING BY BUCKET, whatever order the rows arrived in. The
    /// reference's own order is lexicographic on a `%g` rendering of the
    /// label and is neither numeric nor stable in any way a client could
    /// use — a determinism device, ledgered as
    /// `2026-08-05-traceql-histogram-series-order`. The captured
    /// reference orders are pinned in `tests/traces_log2_reference.rs`
    /// beside ours, so both sides are visible.
    #[test]
    fn histogram_series_are_emitted_ascending_by_bucket() {
        let bucket = |ns: u64| TraceMetricSeries {
            labels: vec![MetricLabel::double(
                "__bucket",
                log2_histogram::bucket_seconds(ns),
            )],
            samples: vec![(0, 1.0)],
            exemplars: vec![],
        };
        // Deliberately shuffled, and spanning the renderings that make
        // the reference's own comparator disagree with numeric order
        // (2^10 and 2^14 render in exponent form under `%g`).
        let mut series = vec![
            bucket(1 << 20),
            bucket(1 << 63),
            bucket(2),
            bucket(1 << 14),
            bucket(1 << 10),
        ];
        sort_histogram_series_by_bucket_ascending(&mut series);
        let ordered: Vec<f64> = series
            .iter()
            .map(|s| match s.labels[0].value {
                MetricLabelValue::Double(d) => d,
                MetricLabelValue::Str(_) => panic!("__bucket is a double"),
            })
            .collect();
        let want: Vec<f64> = [2u64, 1 << 10, 1 << 14, 1 << 20, 1 << 63]
            .into_iter()
            .map(log2_histogram::bucket_seconds)
            .collect();
        assert_eq!(ordered, want);
        // Ascending in the values themselves, not merely equal to a list.
        assert!(ordered.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn agg_value_uses_the_references_single_rounding_conversion() {
        for (ns, want, wire, t_wire) in REFERENCE_DURATION_SECONDS {
            // Transcription guards: the two rendering columns must denote
            // the two f64s, or every negative below is vacuous.
            assert!(wire.denotes(*want), "{ns}: captured rendering");
            assert!(
                t_wire.denotes(two_rounding_seconds(*ns)),
                "{ns}: two-rounding rendering"
            );
            // The two forms MUST disagree here, or the case proves nothing.
            assert_ne!(
                (*ns as f64 / 1e9).to_bits(),
                two_rounding_seconds(*ns).to_bits(),
                "{ns} ns must be a width where the two forms disagree"
            );
            assert_eq!(agg_value(*ns as f64).to_bits(), want.to_bits(), "{ns}");
            assert_ne!(
                agg_value(*ns as f64).to_bits(),
                two_rounding_seconds(*ns).to_bits(),
                "{ns}"
            );
        }
        for (ns, want) in REFERENCE_DURATION_CONTROLS {
            assert_eq!(agg_value(*ns as f64).to_bits(), want.to_bits(), "{ns}");
            assert_eq!(
                agg_value(*ns as f64).to_bits(),
                two_rounding_seconds(*ns).to_bits(),
                "{ns}"
            );
        }
    }

    /// Issue #237 round 3: pins the PROPERTY that makes the raw-wire
    /// capture formatter-independent. A width only discriminates if its
    /// reference rendering carries 17 significant digits (no <=16-digit
    /// formatter can emit it) — and 17 digits forces `ns > 2^53`, hence a
    /// lossy cast.
    #[test]
    fn the_seventeen_digit_witnesses_are_formatter_independent_discriminators() {
        for ns in SEVENTEEN_DIGIT_WIDTHS {
            let (_, want, wire, t_wire) = REFERENCE_DURATION_SECONDS
                .iter()
                .find(|(n, ..)| n == ns)
                .expect("witness is in the captured table");
            assert_eq!(wire.significant_digits(), 17, "{ns}");
            // The two renderings denote distinct f64s (bit level, never a
            // string comparison — #237 plan v4 §3).
            assert!(wire.denotes(*ns as f64 / 1e9), "{ns}");
            assert!(!t_wire.denotes(*ns as f64 / 1e9), "{ns}");
            assert!(
                *ns > (1i64 << 53),
                "{ns}: a 17-digit witness must exceed 2^53"
            );
            assert_ne!(
                (*ns as f64) as i64,
                *ns,
                "{ns}: the int64->f64 cast is lossy"
            );
            assert_eq!(agg_value(*ns as f64).to_bits(), want.to_bits(), "{ns}");
            assert_ne!(
                agg_value(*ns as f64).to_bits(),
                two_rounding_seconds(*ns).to_bits(),
                "{ns}"
            );
        }
        for ns in TIE_FREE_WIDTHS {
            assert_ne!(ns.rem_euclid(4), 2, "{ns}: cast must not be an exact tie");
        }
    }

    /// The byte-frozen text of this file's `mod wire_literal` block,
    /// including the `#[cfg(test)]` line above it (issue #237 Rule C,
    /// upward-extended span). Regenerated only as a deliberate, reviewed
    /// edit alongside the module itself.
    const FROZEN_WIRE_LITERAL_EXEC: &[&str] = &[
        "#[cfg(test)]",
        "mod wire_literal {",
        "    /// A reference-captured wire rendering (issue #237). Nothing textual",
        "    /// or numeric leaves this module: every exit is a predicate. There is",
        "    /// no `value()`, no `tokens()`, and no raw-text accessor, conversion,",
        "    /// deref or formatting impl of any spelling — and this copy",
        "    /// deliberately has no text-accepting method at all, so a body assertion is not even",
        "    /// expressible here (no serialized body exists in this file's scope).",
        "    /// The whole block, including the attribute above it, is byte-frozen",
        "    /// by `the_wire_literal_module_is_byte_frozen`; it invokes no macro,",
        "    /// so no macro can be shadowed into it.",
        "    pub(crate) struct WireLiteral(&'static str);",
        "",
        "    impl WireLiteral {",
        "        pub(crate) const fn new(text: &'static str) -> Self {",
        "            Self(text)",
        "        }",
        "",
        "        /// True iff the captured text is EXACTLY what the locked encoder",
        "        /// emits for `want`: it parses bit-identically to `want` AND",
        "        /// `serde_json::to_string(&want)` reproduces it. This is the",
        "        /// per-row transcription pin for the captured table.",
        "        pub(crate) fn denotes(&self, want: f64) -> bool {",
        "            let parses = match self.0.parse::<f64>() {",
        "                Ok(v) => v.to_bits() == want.to_bits(),",
        "                Err(_) => false,",
        "            };",
        "            let renders = match serde_json::to_string(&want) {",
        "                Ok(s) => s == self.0,",
        "                Err(_) => false,",
        "            };",
        "            parses && renders",
        "        }",
        "",
        "        /// Significant digits of the captured rendering (leading and",
        "        /// trailing zeros stripped). A width only discriminates the two",
        "        /// rounding forms formatter-independently if this is 17.",
        "        pub(crate) fn significant_digits(&self) -> usize {",
        "            let mut digits = String::new();",
        "            for c in self.0.chars() {",
        "                if c.is_ascii_digit() {",
        "                    digits.push(c);",
        "                }",
        "            }",
        "            digits.trim_start_matches('0').trim_end_matches('0').len()",
        "        }",
        "    }",
        "}",
    ];

    /// Issue #237 residual R4, closed mechanically: `mod wire_literal`
    /// (including the `#[cfg(test)]` attribute attached to it) is
    /// byte-frozen against this file's own source. Any attribute above
    /// it, added method, changed signature or changed body fails here —
    /// the span extends upward to the previous column-0 `}` so an outer
    /// attribute cannot sit outside the frozen text.
    #[test]
    fn the_wire_literal_module_is_byte_frozen() {
        let src = include_str!("exec.rs");
        let lines: Vec<&str> = src.lines().collect();
        let mut mod_lines: Vec<usize> = Vec::new();
        for (i, line) in lines.iter().enumerate() {
            if line.trim() == "mod wire_literal {" {
                mod_lines.push(i);
            }
        }
        assert_eq!(mod_lines.len(), 1, "exactly one wire_literal module");
        let m = mod_lines[0];
        let mut e = None;
        for (i, line) in lines.iter().enumerate().skip(m + 1) {
            if *line == "}" {
                e = Some(i);
                break;
            }
        }
        let e = e.expect("module closes at column 0");
        let mut s = 0;
        for i in (0..m).rev() {
            if lines[i] == "}" {
                s = i + 1;
                break;
            }
        }
        while s < m && lines[s].trim().is_empty() {
            s += 1;
        }
        assert_eq!(
            lines[s..=e].to_vec(),
            FROZEN_WIRE_LITERAL_EXEC,
            "mod wire_literal must stay byte-identical to the frozen text \
             (issue #237; update both sides in the same commit, loudly)"
        );
    }

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
    fn code_241_maps_to_the_generator_memory_reason_on_generator_reads_only() {
        let e = || ChError::Server {
            code: 241,
            message: "Memory limit (for query) exceeded".to_string(),
        };
        match map_trace_generator_error(e(), &cfg()) {
            ReadError::QueryTooBroad(TooBroadReason::TraceGeneratorMemory { budget_bytes }) => {
                assert_eq!(budget_bytes, cfg().generator_max_memory_bytes);
            }
            other => panic!("expected TraceGeneratorMemory, got {other:?}"),
        }
        // Issue #398: the shared trace mapper DOES map 241 now — every
        // trace read carries the surface-wide ceiling — but to its own,
        // separate reason carrying its own, separate budget. The two stay
        // unconflated, which is what this test has always been for, and
        // the generator's mapper returning FIRST is what keeps the tighter
        // ceiling reporting as `TraceGeneratorMemory`.
        match map_trace_read_error(e(), &cfg()) {
            ReadError::QueryTooBroad(TooBroadReason::TraceReadMemory { budget_bytes }) => {
                assert_eq!(budget_bytes, cfg().read_max_memory_bytes);
                assert_ne!(
                    budget_bytes,
                    cfg().generator_max_memory_bytes,
                    "the two memory budgets must be distinguishable in the message"
                );
            }
            other => panic!("expected TraceReadMemory, got {other:?}"),
        }
    }

    #[test]
    fn the_generator_mapper_delegates_everything_else_to_the_shared_mapper() {
        let e = ChError::Server {
            code: 158,
            message: "Limit for rows to read exceeded".to_string(),
        };
        assert!(matches!(
            map_trace_generator_error(e, &cfg()),
            ReadError::QueryTooBroad(TooBroadReason::TraceScanBudgetRows { budget_rows: 1_000 })
        ));
        let t = ChError::Timeout("deadline".to_string());
        assert!(matches!(
            map_trace_generator_error(t, &cfg()),
            ReadError::Clickhouse(_)
        ));
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
        assert!(matches!(
            map_trace_generator_error(server(241), &cfg()),
            ReadError::QueryTooBroad(TooBroadReason::TraceGeneratorMemory { .. })
        ));
        // Distinctness: neither Layer-1 byte-budget constant equals the
        // Layer-2 retention budget, so `budget_bytes` equality is a
        // sound discriminator between a Layer-1 preempt and the
        // retention-counter trip.
        assert_ne!(TRACE_READ_BYTES_BUDGET, HYDRATION_BYTE_BUDGET as u64);
        assert_ne!(TRACE_MAX_RESULT_BYTES, HYDRATION_BYTE_BUDGET as u64);
    }

    fn tid(n: u8) -> [u8; 16] {
        let mut id = [0u8; 16];
        id[15] = n;
        id
    }

    #[test]
    fn merge_takes_the_max_bound_when_generators_disagree() {
        // Round-4 finding 1: a trace emitted by multiple generators with
        // different bounds must keep the LARGER bound — anything less
        // could under-bound and drop a winner at threshold termination.
        let merged = merge_candidates(&[
            vec![(tid(1), 100), (tid(2), 90)],
            vec![(tid(1), 250), (tid(3), 80)],
        ]);
        assert_eq!(merged, vec![(tid(1), 250), (tid(2), 90), (tid(3), 80)]);
    }

    #[test]
    fn merge_ranks_by_bound_desc_then_trace_id_asc() {
        let merged = merge_candidates(&[vec![(tid(9), 100), (tid(2), 100), (tid(5), 200)]]);
        assert_eq!(merged, vec![(tid(5), 200), (tid(2), 100), (tid(9), 100)]);
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

    /// Code review round 1 (container overhead): every retained-entry
    /// charge constant covers its `size_of` payload PLUS the documented
    /// overhead envelope — no retained collection grows without a
    /// corresponding live charge.
    #[test]
    fn retained_entry_charges_cover_size_of_plus_the_overhead_envelope() {
        assert_eq!(
            CANDIDATE_TUPLE_BYTES,
            std::mem::size_of::<([u8; 16], i64)>() + RETAINED_ENTRY_OVERHEAD
        );
        assert_eq!(
            MEMBERSHIP_ENTRY_BYTES,
            std::mem::size_of::<([u8; 16], [u8; 8])>() + RETAINED_ENTRY_OVERHEAD
        );
        assert_eq!(
            NUM_VALUE_ENTRY_BYTES,
            std::mem::size_of::<(([u8; 16], [u8; 8]), f64)>() + RETAINED_ENTRY_OVERHEAD
        );
        // Heap summaries: the retained cost is size_of-based + overhead +
        // string payloads (never a bare fixed constant).
        let m = TraceMatch {
            trace_id: tid(1),
            sort_key: 1,
            matched: 1,
            spans: vec![SpanSummary::new(
                [1; 8],
                Some("n".repeat(10)),
                1,
                1,
                vec![search_eval::ProjectedAttribute::new(
                    &pulsus_traceql::Field::Attribute {
                        scope: pulsus_traceql::AttrScope::Span,
                        key: "k".to_string(),
                    },
                    "v".to_string(),
                )],
            )],
            groups: None,
        };
        assert!(
            m.retained_bytes()
                >= std::mem::size_of::<TraceMatch>()
                    + std::mem::size_of::<SpanSummary>()
                    + 2 * RETAINED_ENTRY_OVERHEAD
                    + 10
                    + 2,
            "heap-entry charge must cover struct sizes, overhead, and strings (got {})",
            m.retained_bytes()
        );
    }

    /// Code review round 1 (merge overlap): the per-generator row charge
    /// upper-bounds the merged map even when generators overlap — the
    /// merged entry count never exceeds the charged row count.
    #[test]
    fn merge_overlap_never_exceeds_the_charged_row_count() {
        let per_generator = vec![
            vec![(tid(1), 100), (tid(2), 90)],
            vec![(tid(1), 250), (tid(2), 80), (tid(3), 70)],
        ];
        let charged_rows: usize = per_generator.iter().map(Vec::len).sum();
        let merged = merge_candidates(&per_generator);
        assert!(merged.len() <= charged_rows);
        // And the charge itself covers the merged-map entry cost.
        let mut budget = ByteBudget::new(charged_rows * CANDIDATE_TUPLE_BYTES);
        assert!(budget.charge(charged_rows * CANDIDATE_TUPLE_BYTES).is_ok());
        assert!(
            merged.len() * (std::mem::size_of::<([u8; 16], i64)>() + RETAINED_ENTRY_OVERHEAD)
                <= charged_rows * CANDIDATE_TUPLE_BYTES
        );
    }

    /// Code review round 1 (roots retention): the transient root-row
    /// charge is released only AFTER the retained `roots` map has been
    /// charged, and the retained charge stays live — the transfer never
    /// leaves the map uncharged.
    #[test]
    fn root_charges_transfer_to_the_retained_map_not_released_with_the_rows() {
        let row = |name: &str| RootRow {
            trace_id: tid(1),
            span_id: [1; 8],
            parent_id: [0; 8],
            service: "svc".to_string(),
            name: name.to_string(),
            timestamp_ns: 1,
            duration_ns: 1,
        };
        let rows = vec![row("root-name"), row("other")];
        let row_cost = |r: &RootRow| {
            std::mem::size_of::<RootRow>()
                + RETAINED_ENTRY_OVERHEAD
                + r.service.len()
                + r.name.len()
        };
        let transient: usize = rows.iter().map(row_cost).sum();

        // Replay the exec flow's accounting exactly: charge rows while
        // streaming, charge the retained map, THEN release the rows.
        let mut budget = ByteBudget::new(HYDRATION_BYTE_BUDGET);
        budget.charge(transient).expect("transient rows charge");
        let roots = pick_roots(rows);
        let retained = roots_retained_bytes(&roots);
        assert!(retained > 0, "a live roots map must carry a live charge");
        budget.charge(retained).expect("retained roots charge");
        budget.release(transient);
        assert_eq!(
            budget.used(),
            retained,
            "after the transfer, exactly the retained roots bytes stay charged"
        );
        // The retained charge is EXACTLY the per-entry map key +
        // `TraceContext` struct + string payloads + container overhead,
        // summed over the map — issue #464 replaced a `>=` bound that
        // could not see an undercharge, because a bound computed from the
        // same `size_of` the accounting used moved with it.
        assert!(
            std::mem::size_of::<TraceContext>()
                >= std::mem::size_of::<RootSummary>() + 2 * std::mem::size_of::<i64>(),
            "TraceContext is the root summary PLUS the two envelope i64s"
        );
        let expected: usize = roots
            .values()
            .map(|ctx| {
                std::mem::size_of::<[u8; 16]>()
                    + std::mem::size_of::<TraceContext>()
                    + RETAINED_ENTRY_OVERHEAD
                    + ctx.root.service.len()
                    + ctx.root.name.len()
            })
            .sum();
        assert_eq!(
            retained, expected,
            "the retained charge must be computed from TraceContext (root summary PLUS the two envelope i64s), not from RootSummary"
        );
    }

    #[test]
    fn heap_entry_ordering_evicts_the_oldest_then_largest_trace_id() {
        let entry = |ts: i64, id: u8| {
            HeapEntry(TraceMatch {
                trace_id: tid(id),
                sort_key: ts,
                matched: 1,
                spans: Vec::new(),
                groups: None,
            })
        };
        let mut heap = std::collections::BinaryHeap::new();
        heap.push(entry(100, 1));
        heap.push(entry(50, 2));
        heap.push(entry(50, 3));
        // Worst = smallest ts; among ties the larger trace id.
        assert_eq!(heap.pop().unwrap().0.trace_id, tid(3));
        assert_eq!(heap.pop().unwrap().0.trace_id, tid(2));
        assert_eq!(heap.pop().unwrap().0.trace_id, tid(1));
    }

    #[test]
    fn pick_roots_prefers_an_all_zero_parent_over_an_earlier_child() {
        let row = |ts: i64, span: u8, parent: u8, name: &str| RootRow {
            trace_id: tid(1),
            span_id: {
                let mut id = [0u8; 8];
                id[7] = span;
                id
            },
            parent_id: {
                let mut id = [0u8; 8];
                id[7] = parent;
                id
            },
            service: "svc".to_string(),
            name: name.to_string(),
            timestamp_ns: ts,
            duration_ns: 5,
        };
        let roots = pick_roots(vec![row(10, 2, 9, "early-child"), row(20, 1, 0, "root")]);
        assert_eq!(roots[&tid(1)].root.name, "root");
    }

    #[test]
    fn pick_roots_falls_back_to_the_earliest_span_when_no_root_is_stored() {
        let row = |ts: i64, span: u8, name: &str| RootRow {
            trace_id: tid(1),
            span_id: {
                let mut id = [0u8; 8];
                id[7] = span;
                id
            },
            parent_id: [9u8; 8],
            service: "svc".to_string(),
            name: name.to_string(),
            timestamp_ns: ts,
            duration_ns: 5,
        };
        let roots = pick_roots(vec![row(20, 2, "later"), row(10, 1, "earliest")]);
        assert_eq!(roots[&tid(1)].root.name, "earliest");
    }

    /// Issue #464: the trace-level envelope is folded over EVERY span of
    /// the trace-wide root read, in the same pass that picks the root, and
    /// it is a WIDTH.
    ///
    /// The base instant is deliberately non-zero, so reporting the
    /// envelope's END instead of its width is distinguishable from
    /// reporting the width; with `B == 0` the two are the same number and
    /// this test could not tell them apart.
    ///
    /// The root here is neither the earliest span nor the widest, so the
    /// envelope cannot be satisfied by the root's own window: the root
    /// starts 1_000 ns after the trace does and is 42 ns wide, while the
    /// trace runs from `B` to `B + 10_000`.
    #[test]
    fn pick_roots_folds_the_trace_envelope_over_every_span_as_a_width() {
        const B: i64 = 1_700_000_000_000_000_000;
        let row = |ts: i64, dur: i64, span: u8, parent: u8, name: &str| RootRow {
            trace_id: tid(1),
            span_id: {
                let mut id = [0u8; 8];
                id[7] = span;
                id
            },
            parent_id: {
                let mut id = [0u8; 8];
                id[7] = parent;
                id
            },
            service: "svc".to_string(),
            name: name.to_string(),
            timestamp_ns: ts,
            duration_ns: dur,
        };
        let roots = pick_roots(vec![
            row(B + 1_000, 42, 1, 0, "root"),
            row(B, 0, 2, 1, "earliest-child"),
            row(B + 1_000, 9_000, 3, 1, "latest-ending-child"),
        ]);
        let ctx = &roots[&tid(1)];
        assert_eq!(
            ctx.trace_start_ns, B,
            "the envelope starts at the EARLIEST span, not the root"
        );
        assert_eq!(
            ctx.trace_duration_ns, 10_000,
            "the envelope is a WIDTH: (B + 1_000 + 9_000) - B, not the root's 42 and not the \
             envelope's end"
        );
        // Root selection is untouched: the all-zero parent still wins, and
        // its OWN window is still the root span's.
        assert_eq!(ctx.root.name, "root");
        assert_eq!(ctx.root.start_ns, B + 1_000);
        assert_eq!(ctx.root.duration_ns, 42);
    }

    /// Issue #464, the single-span control: the root IS the trace, so the
    /// trace-level rule and the root-span rule agree. It cannot
    /// discriminate the two — that is
    /// [`pick_roots_folds_the_trace_envelope_over_every_span_as_a_width`]'s
    /// job — and it is kept so the width formula is pinned where there is
    /// nothing to fold. The base is non-zero here too, so reporting the
    /// envelope's END instead of its width still fails.
    #[test]
    fn pick_roots_reports_a_single_span_trace_as_that_span_s_own_window() {
        const B: i64 = 1_700_000_000_000_000_000;
        let only = pick_roots(vec![RootRow {
            trace_id: tid(1),
            span_id: [1u8; 8],
            parent_id: [0u8; 8],
            service: "svc".to_string(),
            name: "alone".to_string(),
            timestamp_ns: B + 5,
            duration_ns: 7,
        }]);
        let ctx = &only[&tid(1)];
        assert_eq!(ctx.root.name, "alone");
        assert_eq!(ctx.trace_start_ns, B + 5);
        assert_eq!(
            ctx.trace_duration_ns, 7,
            "the envelope is a WIDTH: (B + 5 + 7) - (B + 5), not the envelope's end"
        );
    }

    /// Issue #464: the TTL-race fallback — a winner whose trace-wide root
    /// read returned nothing. The assembly branch it serves is reachable
    /// only from a live read that loses its rows between phases, so the
    /// construction is a pure function precisely so both envelope fields
    /// can be pinned here without an engine.
    #[test]
    fn the_ttl_race_fallback_reports_the_matched_span_start_and_a_zero_width() {
        let matched = SpanSummary::new(
            [7u8; 8],
            Some("matched".to_string()),
            1_700_000_000_000_000_000,
            7_777,
            vec![],
        );
        let ctx = fallback_trace_context(std::slice::from_ref(&matched), 42);
        assert_eq!(ctx.root.service, "");
        assert_eq!(ctx.root.name, "matched");
        assert_eq!(ctx.root.start_ns, matched.start_ns);
        assert_eq!(ctx.root.duration_ns, 0);
        assert_eq!(
            ctx.trace_start_ns, matched.start_ns,
            "with no root row, the matched span's own start is the only instant we have"
        );
        assert_eq!(
            ctx.trace_duration_ns, 0,
            "and to a ZERO width — a non-zero fallback width would be invented, and the span's \
             own 7_777 ns is NOT the trace's"
        );

        // No matched spans at all: the heap sort key is the only instant
        // left, and the width is still zero.
        let empty = fallback_trace_context(&[], 99);
        assert_eq!(empty.root.service, "");
        assert_eq!(empty.root.name, "");
        assert_eq!(empty.root.start_ns, 99);
        assert_eq!(empty.root.duration_ns, 0);
        assert_eq!(empty.trace_start_ns, 99);
        assert_eq!(empty.trace_duration_ns, 0);
    }

    fn hyd_row(trace: u8, span: u8) -> HydrationRow {
        HydrationRow {
            trace_id: tid(trace),
            span_id: {
                let mut id = [0u8; 8];
                id[7] = span;
                id
            },
            parent_id: [0u8; 8],
            service: "svc".to_string(),
            name: "op".to_string(),
            timestamp_ns: span as i64,
            duration_ns: 1,
            status_code: 0,
            status_message: String::new(),
            kind: 1,
            scope_name: String::new(),
            scope_version: String::new(),
        }
    }

    /// The exact per-group / per-unique-span charge formulas
    /// [`group_hydrated_rows`] applies (kept in one place so the tests
    /// below validate the REAL formulas, not re-derivations).
    fn expected_group_cost(groups: usize, unique_spans: usize) -> usize {
        let first_outer = VEC_INITIAL_RESERVATION_SLOTS * std::mem::size_of::<TraceSpans>();
        let per_group = 2 * std::mem::size_of::<TraceSpans>()
            + RETAINED_ENTRY_OVERHEAD
            + VEC_INITIAL_RESERVATION_SLOTS * std::mem::size_of::<HydratedSpan>();
        let per_span = 2 * std::mem::size_of::<HydratedSpan>()
            + std::mem::size_of::<[u8; 8]>()
            + RETAINED_ENTRY_OVERHEAD;
        first_outer + groups * per_group + unique_spans * per_span
    }

    /// Round-5 medium: replayed rows are accounting no-ops — a
    /// replay-heavy batch (every row duplicated) ends with exactly the
    /// deduped groups' charge, never a phantom per-duplicate charge.
    #[test]
    fn replayed_rows_charge_exactly_the_deduped_retained_bytes() {
        let mut rows = Vec::new();
        for trace in 1..=3u8 {
            for span in 1..=5u8 {
                rows.push(hyd_row(trace, span));
                rows.push(hyd_row(trace, span)); // every row replayed
            }
        }
        let mut budget = ByteBudget::new(usize::MAX);
        let mut charged = 0usize;
        let (traces, overflowed) =
            group_hydrated_rows(rows, &mut budget, &mut charged).expect("in budget");
        assert!(!overflowed);
        assert_eq!(traces.len(), 3);
        assert!(traces.iter().all(|t| t.spans.len() == 5), "deduped");
        assert_eq!(
            charged,
            expected_group_cost(3, 15),
            "duplicates must not accumulate phantom charges"
        );
        assert_eq!(budget.used(), charged);
    }

    /// Round-5 high: the growth/initial-reservation formulas are exact —
    /// covering the fresh outer/inner Vec initial reservations
    /// (`VEC_INITIAL_RESERVATION_SLOTS`) and the standard hash-container
    /// entry cost for the dedup set — and the charges cover the real
    /// reserved capacities.
    #[test]
    fn group_charges_cover_initial_reservations_and_real_capacities() {
        // One group, one span: the smallest shape exercises both initial
        // reservations.
        let mut budget = ByteBudget::new(usize::MAX);
        let mut charged = 0usize;
        let (traces, _) =
            group_hydrated_rows(vec![hyd_row(1, 1)], &mut budget, &mut charged).expect("fits");
        assert_eq!(charged, expected_group_cost(1, 1));
        // The charge covers what was actually reserved.
        assert!(
            charged
                >= traces.capacity() * std::mem::size_of::<TraceSpans>()
                    + traces[0].spans.capacity() * std::mem::size_of::<HydratedSpan>(),
            "charge {} must cover outer cap {} + inner cap {}",
            charged,
            traces.capacity(),
            traces[0].spans.capacity()
        );

        // Many spans across several doublings: still covered.
        let rows: Vec<HydrationRow> = (0..200u8).map(|n| hyd_row(1, n)).collect();
        let mut budget = ByteBudget::new(usize::MAX);
        let mut charged = 0usize;
        let (traces, _) = group_hydrated_rows(rows, &mut budget, &mut charged).expect("fits");
        assert_eq!(charged, expected_group_cost(1, 200));
        assert!(
            charged
                >= traces.capacity() * std::mem::size_of::<TraceSpans>()
                    + traces[0].spans.capacity() * std::mem::size_of::<HydratedSpan>()
                    + 200 * std::mem::size_of::<[u8; 8]>(),
            "growth stays within the doubling-slack model"
        );
        assert!(!traces.is_empty());
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
        assert!(
            !rendered.contains("optimize_skip_unused_shards"),
            "unclustered engines must not carry the §7 settings"
        );
    }

    /// Issue #57 re-audit AC-A1: the phase-1 generator settings pin —
    /// `search_settings` plus the memory ceiling, throw-not-spill.
    #[test]
    fn generator_settings_pin_the_memory_ceiling_and_throw_not_spill() {
        let rendered = format!("{:?}", generator_settings(&cfg()));
        for expected in [
            "max_memory_usage",
            "536870912",
            "max_bytes_before_external_group_by",
            // The search settings must still be present underneath.
            "max_rows_to_read",
            "max_block_size",
        ] {
            assert!(
                rendered.contains(expected),
                "missing {expected} in {rendered}"
            );
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
        for absent in ["optimize_skip_unused_shards", "prefer_localhost_replica"] {
            assert!(
                !rendered.contains(absent),
                "unexpected {absent} in {rendered} — catalog reads are never clustered"
            );
        }
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
        assert!(!rendered.contains("optimize_skip_unused_shards"));
        assert!(!rendered.contains("prefer_localhost_replica"));
    }

    // --- Issue #35: full-shape parse bound (traces) ---

    /// The point-read template plus 32 hex chars stays well under any
    /// plausible query-text cap, let alone
    /// [`crate::querytext::MAX_QUERY_TEXT_BYTES`]. Issue #35 used this
    /// to justify EXEMPTING `fetch_by_id` from
    /// [`crate::querytext::ensure_query_text_fits`]; since issue #509 the
    /// point read passes that guard like every other read on this path,
    /// and this test is what says the guard can never fire on it.
    #[test]
    fn point_read_sql_stays_under_4kib_by_construction() {
        let sql =
            crate::traces::sql::point_read_sql("trace_spans", "4bf92f3577b34da6a3ce929d0e0e4736");
        assert!(
            sql.len() < 4096,
            "point-read SQL is {} bytes, expected < 4 KiB",
            sql.len()
        );
    }

    /// The batch hydration read at the `BATCH_TRACES` batch size — the
    /// module's own documented residual (≤ 48 B × 32 ≈ ≤ 2 KB, module
    /// doc) — stays well under the guard's cap. Pinned so a future
    /// `BATCH_TRACES` increase that breaks the assumption is caught here,
    /// not live.
    #[test]
    fn hydration_sql_at_batch_traces_batch_size_stays_under_4kib() {
        let trace_ids: Vec<[u8; 16]> = (0..BATCH_TRACES as u8).map(|i| [i; 16]).collect();
        let sql = crate::traces::search_sql::hydration_sql(
            "trace_spans",
            &trace_ids,
            crate::logql::sql::TimeWindow {
                start_ns: 0,
                end_ns: i64::MAX,
            },
            MAX_SPANS_PER_TRACE,
        );
        assert!(
            sql.len() < 4096,
            "hydration SQL at the BATCH_TRACES={} batch size is {} bytes, expected < 4 KiB",
            BATCH_TRACES,
            sql.len()
        );
    }

    /// Issue #478, criterion 5a (hermetic half). **Every span name is
    /// typed `string` EXPLICITLY, never left empty.**
    ///
    /// The adversarial names are the ones a text-classifying inference
    /// would type `int`, `float`, `bool` and `duration`; the reference
    /// types all of them `string`, because a span name is a String
    /// column. The empty-type case is the trap this test exists for:
    /// `tags_response::entry_type("")` also renders `string`, so a read
    /// that set no type at all would be invisible on the wire — the
    /// assertion is on `val_type` itself, not on the rendering.
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
                "an empty val_type renders as `string` too, which is why this is asserted \
                 separately from the rendering"
            );
        }
    }

    /// The window a request carries resolves to the UTC days it touches
    /// — the bound both store-backed reads are given.
    #[test]
    fn a_tag_values_request_resolves_its_window_to_utc_days() {
        let req = TagValuesRequest {
            q: None,
            start_ns: 1_700_000_000_000_000_000,
            end_ns: 1_700_010_800_000_000_000,
        };
        assert_eq!(
            req.days(),
            DaySpan {
                start_days: 19_675,
                end_days: 19_676
            }
        );
        assert!(req.narrowing().is_empty(), "no q means no narrowing");
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
            };
            assert!(req.narrowing().is_empty(), "{q:?} must not narrow");
        }
        let req = TagValuesRequest {
            q: Some("{resource.service.name=\"cart\"}"),
            start_ns: 0,
            end_ns: 0,
        };
        assert!(!req.narrowing().is_empty(), "a well-formed q must narrow");
    }
}
