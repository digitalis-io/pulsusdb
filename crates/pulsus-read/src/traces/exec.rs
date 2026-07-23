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
//!   241 → [`TooBroadReason::TraceGeneratorMemory`] → 422. Layer 2 — a
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

use std::collections::{HashMap, HashSet};

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChError, ChRow, QuerySettings};

use super::graph_sql::{self, GraphWindow};
use super::metrics_plan::{MetricFunc, MetricsCtx, TraceMetricsPlan};
use super::rows::{
    CandidateRow, GraphEdgeRow, HydrationRow, MembershipRow, MetricBucketRow, MetricCountRow,
    NumValueRow, RootRow, StoredSpan, StoredSpanRow, StrValueRow, TagNameRow, TagValueRow,
};
use super::search_eval::{self, BatchAttrs, HydratedSpan, SpanSummary, TraceMatch, TraceSpans};
use super::search_plan::{SearchCtx, SearchPlan};
use crate::logql::error::{ReadError, TooBroadReason};
use crate::logql::exec::{MatrixSeries, QueryResult, VectorSample, escape_query_placeholders};
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
    /// `trace_tag_catalog` — the Global tag catalog the §4.3 discovery
    /// reads (issue #58) target. NEVER `_dist`-suffixed: migration 18 is
    /// `Replication::Global, family: None` (no `_dist` wrapper exists to
    /// name), so every catalog read is a local-replica primary-key-prefix
    /// scan with no coordinator fan-out (docs/schemas.md §4.1/§7 —
    /// `chconfig::trace_read_config_from` sets it unconditionally, the
    /// `metric_metadata` carve-out pattern).
    pub catalog_table: String,
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
    /// `reader.traceql_generator_max_memory_bytes` — the phase-1
    /// candidate-generator query's `max_memory_usage` ceiling (issue #57
    /// re-audit, sub-problem B): bounds a dense common-value prefix's
    /// `GROUP BY trace_id` aggregation state; breach → 422 (code 241 →
    /// [`TooBroadReason::TraceGeneratorMemory`]). Never applied to
    /// phase-2 reads (hydration/membership/value/root), which set no
    /// memory limit of their own.
    pub generator_max_memory_bytes: u64,
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

/// One returned trace: root metadata + the matched spanset summaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceSearchResult {
    pub trace_id: [u8; 16],
    pub root: RootSummary,
    /// Total exactly-matched spans (pre-`spss` cap).
    pub matched: u32,
    /// `spss`-capped matched-span summaries, ascending `(start_ns, span_id)`.
    pub spans: Vec<SpanSummary>,
}

/// The search result: `traces` ordered by the public contract (max
/// matched-span `timestamp_ns` DESC, `trace_id` ASC — docs/api.md §4.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchOutput {
    pub traces: Vec<TraceSearchResult>,
    pub partial: bool,
    pub returned: u32,
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

/// [`TraceEngine::list_tag_values`]'s output (issue #58): distinct
/// values for one key, ordered ascending, at most [`TAG_VALUES_MAX`];
/// `truncated` as in [`TagNames`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagValues {
    pub values: Vec<String>,
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

/// Maps a ClickHouse error on the **trace search** path, and (issue #58
/// re-review) the two §4.3 catalog reads that carry the same budget via
/// [`catalog_settings`]. Unlike the LogQL mapper, this one deliberately
/// sets `max_rows_to_read`, so code 158 maps to
/// [`TooBroadReason::TraceScanBudgetRows`]; the read/result byte ceilings
/// (codes 307/396) map to the shared byte-budget reason. Everything else
/// passes through unmapped (never reinterpreted as a timeout or vice
/// versa).
fn map_trace_read_error(e: ChError, config: &TraceReadConfig) -> ReadError {
    if let ChError::Server { code, .. } = &e {
        match *code {
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
/// to the shared trace mapper ([`map_trace_read_error`]), which never
/// maps 241 itself (no other trace/LogQL query sets a memory limit).
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
    client: ChClient,
    config: TraceReadConfig,
}

impl TraceEngine {
    pub fn new(client: ChClient, config: TraceReadConfig) -> Self {
        Self { client, config }
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
            distributed: self.config.distributed,
            skip_unavailable_shards: self.config.skip_unavailable_shards,
        }
    }

    /// Executes a metrics range plan (issue #59): one fully-pushed-down
    /// time-bucketed query — bucketing, replay-deduped counting, and time
    /// pruning all happen in ClickHouse; the engine only frames at most
    /// `MAX_METRICS_POINTS` `(t_ms, value)` points (the plan enforced the
    /// cap statically) and applies the explicit encode-boundary
    /// conversions (`n as f64`, rate ÷ `step_s`; the row's `t_ms` is
    /// already the millisecond point unit `prom_api::encode` consumes —
    /// issue #59 re-audit, `Int64` epoch-milliseconds). Empty result
    /// → `Matrix(vec![])` (the documented empty-DB oracle); otherwise one
    /// label-less series (single-series M4 output — `by()` is M7).
    pub async fn metrics_range(&self, plan: &TraceMetricsPlan) -> Result<QueryResult, ReadError> {
        let settings = metrics_settings(&self.config);
        let sql = escape_query_placeholders(plan.range_sql());
        crate::querytext::ensure_query_text_fits(&sql).map_err(ReadError::QueryTooBroad)?;
        let mut points: Vec<(i64, f64)> = Vec::new();
        // Scoped stream (module convention): the pooled-connection lease
        // drops at return, after full consumption (≤ cap buckets).
        let mut stream = self
            .client
            .query_stream::<MetricBucketRow>(&sql, &settings)
            .await
            .map_err(|e| map_trace_metrics_error(e, &self.config))?;
        while let Some(row) = stream.next().await {
            let row = row.map_err(|e| map_trace_metrics_error(e, &self.config))?;
            points.push((row.t_ms, metric_value(plan.func(), row.n, plan.step_s())));
        }
        if points.is_empty() {
            return Ok(QueryResult::Matrix(vec![]));
        }
        Ok(QueryResult::Matrix(vec![MatrixSeries {
            labels: vec![],
            points,
        }]))
    }

    /// Executes a metrics instant plan (issue #59): the same pushed-down
    /// body over the whole snapped window `[S, E)` with no bucketing —
    /// exactly one row (`uniqExact` with no `GROUP BY`), returned as a
    /// one-sample label-less vector; the instant `rate` denominator is
    /// the snapped window width (plan v2 delta 2). The caller stamps the
    /// sample at [`TraceMetricsPlan::snapped_end_ms`].
    pub async fn metrics_instant(&self, plan: &TraceMetricsPlan) -> Result<QueryResult, ReadError> {
        let settings = metrics_settings(&self.config);
        let sql = escape_query_placeholders(plan.instant_sql());
        crate::querytext::ensure_query_text_fits(&sql).map_err(ReadError::QueryTooBroad)?;
        let mut count: Option<u64> = None;
        // Scoped stream: same lease/drain contract as metrics_range
        // (exactly one row by the SQL shape).
        let mut stream = self
            .client
            .query_stream::<MetricCountRow>(&sql, &settings)
            .await
            .map_err(|e| map_trace_metrics_error(e, &self.config))?;
        while let Some(row) = stream.next().await {
            let row = row.map_err(|e| map_trace_metrics_error(e, &self.config))?;
            count = Some(row.n);
        }
        let n = count.unwrap_or(0);
        Ok(QueryResult::Vector(vec![VectorSample {
            labels: vec![],
            value: metric_value(plan.func(), n, plan.window_s()),
        }]))
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
        let raw_sql = graph_sql::service_graph_sql(window, &self.config.edges_table, cap);
        let sql = escape_query_placeholders(&raw_sql);
        crate::querytext::ensure_query_text_fits(&sql).map_err(ReadError::QueryTooBroad)?;
        let settings = graph_settings(&self.config);
        let mut edges: Vec<GraphEdgeRow> = Vec::new();
        // Scoped stream (module convention): the pooled-connection lease
        // drops at return, after full consumption (≤ cap + 1 rows by the SQL
        // LIMIT).
        let mut stream = self
            .client
            .query_stream::<GraphEdgeRow>(&sql, &settings)
            .await
            .map_err(|e| map_trace_read_error(e, &self.config))?;
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
    /// **Issue #35: exempt from the query-text guard.** `point_read_sql`
    /// is a fixed template plus 32 caller-validated hex chars — SQL well
    /// under 1 KiB by construction, with no unbounded-width component
    /// (pinned by `point_read_sql_stays_under_4kib_by_construction` in
    /// this module's tests).
    pub async fn fetch_by_id(&self, hex32: &str) -> Result<Vec<StoredSpan>, ReadError> {
        let sql = super::sql::point_read_sql(&self.config.spans_table, hex32);
        let mut spans = Vec::new();
        // Scoped stream: the pooled-connection lease is dropped when this
        // binding leaves scope at the end of the function, after full
        // consumption.
        let mut stream = self
            .client
            .query_stream::<StoredSpanRow>(&sql, &QuerySettings::new())
            .await
            .map_err(ReadError::Clickhouse)?;
        while let Some(row) = stream.next().await {
            let row = row.map_err(ReadError::Clickhouse)?;
            spans.push(StoredSpan::from(row));
        }
        Ok(spans)
    }

    /// Streams the §4.3 tag-names read (issue #58): distinct
    /// `(scope, key)` pairs from the Global tag catalog — only ever
    /// `config.catalog_table` (`trace_tag_catalog`, never `_dist`, never
    /// a span/attr table: discovery never scans payloads, epic #19 AC1).
    /// `scope` is escaped HERE (`ch_string`) before it reaches the pure
    /// builder — the engine is the catalog reads' injection boundary.
    /// Bounded by the SQL `LIMIT` cap + 1 probe: at most
    /// [`TAG_NAMES_MAX`] rows return, and the probe row (row cap + 1)
    /// flips `truncated` instead of shipping a silent subset. The `LIMIT`
    /// bounds *returned* rows only — an unscoped read has no `WHERE`
    /// predicate, so it is a full catalog scan; [`catalog_settings`]
    /// (issue #58 re-review) bounds *scanned* rows: a breach maps through
    /// [`map_trace_read_error`] to `422 query_too_broad` instead of
    /// running unbounded.
    pub async fn list_tag_names(&self, scope: Option<&str>) -> Result<TagNames, ReadError> {
        let scope_literal = scope.map(crate::logql::escape::ch_string);
        let sql = super::tags_sql::tag_names_sql(
            &self.config.catalog_table,
            scope_literal.as_deref(),
            TAG_NAMES_MAX + 1,
        );
        let settings = catalog_settings(&self.config);
        crate::querytext::ensure_query_text_fits(&sql).map_err(ReadError::QueryTooBroad)?;
        let mut names = Vec::new();
        // Scoped stream (module convention): the pooled-connection lease
        // drops at return, after full consumption — the stream is always
        // drained (≤ cap + 1 rows by the SQL LIMIT).
        let mut stream = self
            .client
            .query_stream::<TagNameRow>(&sql, &settings)
            .await
            .map_err(|e| map_trace_read_error(e, &self.config))?;
        while let Some(row) = stream.next().await {
            let row = row.map_err(|e| map_trace_read_error(e, &self.config))?;
            names.push((row.scope, row.key));
        }
        let truncated = names.len() > TAG_NAMES_MAX;
        names.truncate(TAG_NAMES_MAX);
        Ok(TagNames { names, truncated })
    }

    /// Streams the §4.3 tag-values read (issue #58): distinct values for
    /// one key, optionally scope-confined — same catalog-only,
    /// escape-at-the-engine, `LIMIT` cap + 1 probe, and [`catalog_settings`]
    /// read-budget contract as [`Self::list_tag_names`], capped at
    /// [`TAG_VALUES_MAX`]. A bare-key lookup (no `scope`) cannot prune the
    /// catalog's leading `(scope, key, val)` primary-key column, so it is
    /// a full scan bounded only by the budget.
    pub async fn list_tag_values(
        &self,
        key: &str,
        scope: Option<&str>,
    ) -> Result<TagValues, ReadError> {
        let key_literal = crate::logql::escape::ch_string(key);
        let scope_literal = scope.map(crate::logql::escape::ch_string);
        let sql = super::tags_sql::tag_values_sql(
            &self.config.catalog_table,
            &key_literal,
            scope_literal.as_deref(),
            TAG_VALUES_MAX + 1,
        );
        let settings = catalog_settings(&self.config);
        crate::querytext::ensure_query_text_fits(&sql).map_err(ReadError::QueryTooBroad)?;
        let mut values = Vec::new();
        // Scoped stream: same lease/drain contract as list_tag_names.
        let mut stream = self
            .client
            .query_stream::<TagValueRow>(&sql, &settings)
            .await
            .map_err(|e| map_trace_read_error(e, &self.config))?;
        while let Some(row) = stream.next().await {
            let row = row.map_err(|e| map_trace_read_error(e, &self.config))?;
            values.push(row.val);
        }
        let truncated = values.len() > TAG_VALUES_MAX;
        values.truncate(TAG_VALUES_MAX);
        Ok(TagValues { values, truncated })
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
    /// **Issue #35: the single choke point for every search-phase read.**
    /// Every phase-1 generator, phase-2 hydration/membership/attribute-
    /// value batch, and the root-hydration read all route through this one
    /// function — [`crate::querytext::ensure_query_text_fits`] runs once
    /// here (against the FINAL escaped text) rather than at each of the
    /// half-dozen call sites.
    ///
    /// **Issue #57 re-audit:** `mapper` lets phase-1 generator reads route
    /// through [`map_trace_generator_error`] (which alone maps code 241 →
    /// [`TooBroadReason::TraceGeneratorMemory`]) while every other call
    /// site keeps [`map_trace_read_error`] — a single choke point, two
    /// error taxonomies, never conflated.
    async fn collect_rows_charged<R: ChRow, F: FnMut(&R) -> usize>(
        &self,
        sql: &str,
        settings: &QuerySettings,
        budget: &mut ByteBudget,
        charged: &mut usize,
        mapper: fn(ChError, &TraceReadConfig) -> ReadError,
        mut cost: F,
    ) -> Result<Vec<R>, ReadError> {
        let sql = escape_query_placeholders(sql);
        if let Err(reason) = crate::querytext::ensure_query_text_fits(&sql) {
            return Err(ReadError::QueryTooBroad(reason));
        }
        let mut rows = Vec::new();
        let mut stream = self
            .client
            .query_stream::<R>(&sql, settings)
            .await
            .map_err(|e| mapper(e, &self.config))?;
        while let Some(row) = stream.next().await {
            let row = row.map_err(|e| mapper(e, &self.config))?;
            let bytes = cost(&row);
            budget.charge(bytes)?;
            *charged += bytes;
            rows.push(row);
        }
        Ok(rows)
    }

    async fn search_inner(
        &self,
        plan: &SearchPlan,
        mut explain: Option<&mut PlanExplain>,
    ) -> Result<SearchOutput, ReadError> {
        let settings = self.search_settings();
        let gen_settings = generator_settings(&self.config);
        let mut budget = ByteBudget::new(HYDRATION_BYTE_BUDGET);

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
            for m in search_eval::evaluate_batch(plan, &traces, &attrs, &mut budget)? {
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
            let root = match roots.get(&w.trace_id) {
                Some(root) => {
                    budget.charge(root.service.len() + root.name.len())?;
                    root.clone()
                }
                // A winner whose root read returned nothing (TTL race —
                // pathological) falls back to its matched-span metadata
                // rather than being silently dropped.
                None => {
                    let name_len = w.spans.first().map(|s| s.name.len()).unwrap_or(0);
                    budget.charge(name_len)?;
                    RootSummary {
                        service: String::new(),
                        name: w.spans.first().map(|s| s.name.clone()).unwrap_or_default(),
                        start_ns: w.spans.first().map(|s| s.start_ns).unwrap_or(w.sort_key),
                        duration_ns: 0,
                    }
                }
            };
            traces.push(TraceSearchResult {
                trace_id: w.trace_id,
                root,
                matched: w.matched,
                spans: w.spans,
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
            attrs
                .membership
                .push(rows.into_iter().map(|r| (r.trace_id, r.span_id)).collect());
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
}

/// The phase-1 candidate-generator query settings (issue #57 re-audit,
/// sub-problem B): [`search_settings`] plus the generator memory ceiling
/// — `max_memory_usage` (from `config.generator_max_memory_bytes`) and
/// `max_bytes_before_external_group_by = 0` (throw-not-spill: a spilled
/// aggregation would silently slow rather than fail loud). Bounds a
/// dense common-value prefix's `GROUP BY trace_id` aggregation state;
/// breach → server code 241 → [`map_trace_generator_error`]. Applied
/// ONLY to phase-1 generator reads — phase-2 reads set no memory limit
/// of their own.
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
/// `rate` divides by its denominator (`step_s` per range bucket, the
/// snapped window width for an instant) in `f64` here — never in SQL.
fn metric_value(func: MetricFunc, n: u64, rate_denominator_s: i64) -> f64 {
    match func {
        MetricFunc::Rate => n as f64 / rate_denominator_s as f64,
        MetricFunc::CountOverTime => n as f64,
    }
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
fn group_hydrated_rows(
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
                kind: row.kind,
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

/// The retained cost of the winners' root map — per entry the map key,
/// the summary struct, its string payloads, and the container-overhead
/// envelope. Charged after [`pick_roots`] and held for the rest of the
/// request (the summaries move into the returned [`TraceSearchResult`]s).
fn roots_retained_bytes(roots: &HashMap<[u8; 16], RootSummary>) -> usize {
    roots
        .values()
        .map(|root| {
            std::mem::size_of::<[u8; 16]>()
                + std::mem::size_of::<RootSummary>()
                + RETAINED_ENTRY_OVERHEAD
                + root.service.len()
                + root.name.len()
        })
        .sum()
}

/// Picks each trace's root from its trace-wide root-hydration rows:
/// `parent_id` all-zero (earliest such span under `(ts, span_id)`), else
/// the timestamp-earliest span of the full trace.
fn pick_roots(rows: Vec<RootRow>) -> HashMap<[u8; 16], RootSummary> {
    let mut best: HashMap<[u8; 16], (bool, i64, [u8; 8], RootSummary)> = HashMap::new();
    for row in rows {
        let is_root = row.parent_id == [0u8; 8];
        let summary = RootSummary {
            service: row.service,
            name: row.name,
            start_ns: row.timestamp_ns,
            duration_ns: row.duration_ns,
        };
        let candidate = (is_root, row.timestamp_ns, row.span_id, summary);
        match best.get_mut(&row.trace_id) {
            None => {
                best.insert(row.trace_id, candidate);
            }
            Some(current) => {
                // A true root always beats a non-root; within the same
                // class, earlier (ts, span_id) wins.
                let better = (candidate.0 && !current.0)
                    || (candidate.0 == current.0
                        && (candidate.1, candidate.2) < (current.1, current.2));
                if better {
                    *current = candidate;
                }
            }
        }
    }
    best.into_iter()
        .map(|(trace_id, (_, _, _, summary))| (trace_id, summary))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_read_config_is_cloneable_and_debuggable() {
        let config = TraceReadConfig {
            spans_table: "trace_spans".to_string(),
            attrs_table: "trace_attrs_idx".to_string(),
            catalog_table: "trace_tag_catalog".to_string(),
            edges_table: "trace_edges".to_string(),
            max_candidates: 100_000,
            scan_budget_rows: 50_000_000,
            generator_max_memory_bytes: 536_870_912,
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
            catalog_table: "trace_tag_catalog".to_string(),
            edges_table: "trace_edges".to_string(),
            max_candidates: 100,
            scan_budget_rows: 1_000,
            generator_max_memory_bytes: 536_870_912,
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
        assert_eq!(metric_value(MetricFunc::Rate, 120, 60), 2.0);
        assert_eq!(metric_value(MetricFunc::CountOverTime, 120, 60), 120.0);
        assert_eq!(metric_value(MetricFunc::Rate, 0, 3_600), 0.0);
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
        // The shared trace mapper never maps 241 — the memory ceiling is
        // set only on the phase-1 generator settings, and the reasons
        // stay unconflated.
        assert!(matches!(
            map_trace_read_error(e(), &cfg()),
            ReadError::Clickhouse(_)
        ));
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
            spans: vec![SpanSummary {
                span_id: [1; 8],
                name: "n".repeat(10),
                start_ns: 1,
                duration_ns: 1,
                attributes: vec![("k".to_string(), "v".to_string())],
            }],
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
        // The retained charge covers the map entry, struct, strings, and
        // container overhead per entry.
        let root = &roots[&tid(1)];
        assert!(
            retained
                >= std::mem::size_of::<[u8; 16]>()
                    + std::mem::size_of::<RootSummary>()
                    + RETAINED_ENTRY_OVERHEAD
                    + root.service.len()
                    + root.name.len()
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
        assert_eq!(roots[&tid(1)].name, "root");
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
        assert_eq!(roots[&tid(1)].name, "earliest");
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
            kind: 1,
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

    /// Pins `fetch_by_id`'s guard exemption: the point-read template plus
    /// 32 hex chars stays well under any plausible query-text cap, let
    /// alone [`crate::querytext::MAX_QUERY_TEXT_BYTES`] — `fetch_by_id`
    /// never calls [`crate::querytext::ensure_query_text_fits`], and this
    /// is why that is safe.
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
}
