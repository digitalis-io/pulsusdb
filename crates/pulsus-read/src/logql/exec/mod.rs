//! `LogQlEngine` — executes a [`super::plan::Plan`] against ClickHouse via
//! `ChClient`, injects the scan budget, maps overflow codes to
//! [`ReadError::QueryTooBroad`], and finishes vector aggregations in Rust
//! (docs/schemas.md §3.2: "the engine maps fingerprints to `service` and
//! finishes the `sum by`"). Deliberately **not** snapshot-tested — SQL
//! generation itself is `plan`/`sql`'s job and is tested there without a
//! database; this module's own test coverage is the error-mapping unit
//! tests (architect plan amendment §4).

use super::detected::{
    self, DetectedFieldValues, DetectedFields, DetectedLabelOut, FieldAccumulator,
};
use super::error::{ReadError, TooBroadReason};
use super::explain::PlanExplain;
use super::params::{Direction, PlanCtx, QueryParams, QuerySpec, ResponseOptions, TimeBounds};
use super::pipeline::{CompiledPipeline, ParentSum};
use super::plan::{self, ClientAgg, ClientValue, MetricNode, MetricPlan, Plan, StreamsPlan};
use super::predicate::{BucketGridRefusal, CheckedFragment, CheckedLiteral};
use super::rows::{
    DetectedLabelRow, FirstLineRow, LabelNameRow, LabelValueRow, LogStatsRow, MetricInstantRow,
    MetricRangeBucketRow, MetricRangeJsonRow, MetricRangeLogfmtRow, MetricRangeRegexpRow,
    MetricReducedRow, MetricScanRow, MetricScanRxRow, MetricSumRow, PatternFetchRow, SampleRow,
    SampleRxRow, StreamMetaRow, StreamRow, TailSampleRow, TailSampleRxRow, VolumeRow,
};
use futures::Stream;
use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChError, ChRow, ChRowStream, QuerySettings, ServerFailure};
use pulsus_logql::{
    Expr, LabelFilterExpr, LogExpr, MatchOp, Matcher, RangeAggOp, Stage, StreamSelector,
};
use pulsus_model::{Fingerprint, FpLiteral, Tenant};

use super::charge::{
    MAX_STREAMS_RESULT_BYTES, MUT_GROUP_SLOT, PUSHDOWN_INSTANT_SLOT, PUSHDOWN_RANGE_DENSE_SLOT,
    PUSHDOWN_RANGE_POINT_SLOT, PUSHDOWN_RANGE_SLOT, SERIES_OUT_SLOT, StreamsResultBudget,
    charge_group_bytes, charge_result_points, group_entry_bytes, map_entry_bytes,
};
use super::client_agg::check_surviving_error;
use super::labels::render_series_labels;
use super::sql::ScanProjection;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use super::agg::{InstantSeries, LabelSet};
use super::charge::{AggCaps, cap_result_series};
use super::client_agg::{
    CLIENT_AGG_CHUNK_ROWS, ClientAggState, MetricAggState, RangeSlideState, run_client_agg_rows,
    shift_emitted_points,
};
use super::detected_probe::{
    DetectedPagedState, DetectedRowFeeder, FanOutGroup, GroupKey, LabelScratch,
    SmFanOutAccumulator, TailCursorTracker, classify_page_error, eval_structured_metadata_row,
    push_fanout_entry, recycle_label_scratch, split_categories, split_merged_categories,
};

use super::labels::{
    EMPTY_STRUCTURED_METADATA, StructuredMetadataCtx, derived_stream_fingerprint,
    merge_labels_with_structured_metadata, render_labels_json_sorted, series_labels,
};
use super::post_agg::{
    MAX_POST_AGG_BYTES, apply_label_replace, apply_vector_aggs, charged_instant_chain,
    combine_binary,
};
use super::variants::{MAX_VARIANT_FANOUT_STATE_BYTES, VariantArena, VariantsAggState};
use super::warnings::Warnings;
use super::window::{ClientWindow, materialize_vector_lit};
use super::{
    client_agg, labels, params, pipeline, predicate, reduce, rollup, sql, walkbound, window,
};
use crate::canonical_labels::parse_canonical_labels;

mod detected_reads;
mod discovery;
mod explain_read;
mod fetch;
mod metric;
mod metric_read;
mod metric_routes;
mod predicates;
mod pushdown_instant;
mod pushdown_range;
mod query;
mod results;
mod settings_errors;
mod stats;
mod streams;
mod tail;
mod tail_state;
mod volume;

use discovery::*;
pub(in crate::logql) use metric::*;
pub use metric_read::*;
use predicates::*;
pub(in crate::logql) use pushdown_instant::*;
pub(in crate::logql) use pushdown_range::*;
pub use query::*;
pub use results::*;
pub use settings_errors::*;
pub use stats::*;
pub use streams::*;
pub use tail::*;
pub use tail_state::*;
pub use volume::*;

/// Mints a resolved fingerprint set into the only form that may enter SQL
/// (issue #498). Every `super::sql` builder takes `&[FpLiteral]`, so a raw
/// identity cannot reach a statement: above `2^64` a bare decimal literal
/// is read by ClickHouse as `Float64` and silently matches the wrong row.
/// One allocation per statement built, 16 bytes per fingerprint.
fn sql_literals(fps: &[Fingerprint]) -> Vec<FpLiteral> {
    fps.iter().copied().map(Fingerprint::sql_literal).collect()
}

/// Owned table/budget configuration a [`LogQlEngine`] plans every query
/// against. Mirrors [`PlanCtx`]'s fields as owned `String`s/values so the
/// engine can hand out a borrowed [`PlanCtx`] per call without pinning a
/// lifetime on the engine itself.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub db: String,
    pub streams_idx: String,
    pub streams: String,
    pub samples: String,
    pub rollup_table: String,
    /// `log_patterns` (M7-C3, issue #171), `_dist`-aware exactly like the
    /// other Logs-family tables. Only the `/api/logs/v1/patterns` read
    /// targets it.
    pub patterns_table: String,
    pub rollup_res_ns: u64,
    pub scan_budget_bytes: u64,
    /// Issue #398: `reader.logql_read_max_memory_bytes` — the
    /// `max_memory_usage` ceiling (throw-not-spill) every read this engine
    /// issues carries, applied in [`read_query_settings`] so no dispatch
    /// site can be missed. Breach → server code 241 →
    /// [`TooBroadReason::LogqlReadMemory`] → `422`.
    pub read_max_memory_bytes: u64,
    pub max_streams: usize,
    /// `reader.logql_pipeline_scan_factor` (issue M6-09) — see
    /// [`PlanCtx::pipeline_scan_factor`].
    pub pipeline_scan_factor: u32,
    /// Clustered mode (`PULSUS_CLUSTER` set). Gates
    /// `distributed_product_mode='local'` on the three label-discovery
    /// queries that carry an `IN (subquery)` after issue #399; mirrors
    /// `MetricsConfig::distributed` and `TraceReadConfig::distributed`.
    /// See [`LogQlEngine::activity_settings`].
    pub distributed: bool,
    /// `PULSUS_SKIP_UNAVAILABLE_SHARDS` (#603 follow-up): what a read does
    /// when a shard cannot be reached or does not have the table. Sent on
    /// every read as part of the answer pins ([`read_query_settings`]).
    pub skip_unavailable_shards: bool,
}

impl EngineConfig {
    fn plan_ctx<'a>(&'a self, tenant: &'a Tenant) -> PlanCtx<'a> {
        PlanCtx {
            tenant,
            db: &self.db,
            streams_idx: &self.streams_idx,
            streams: &self.streams,
            samples: &self.samples,
            rollup_table: &self.rollup_table,
            rollup_res_ns: self.rollup_res_ns,
            scan_budget_bytes: self.scan_budget_bytes,
            max_streams: self.max_streams,
            pipeline_scan_factor: self.pipeline_scan_factor,
        }
    }
}

pub struct LogQlEngine {
    client: ChClient,
    config: EngineConfig,
    /// The request's tenant (issue #653). An engine exists only for one.
    tenant: Tenant,
    key_route_test: KeyRouteTestHooks,
    /// Test-only: the statement's structured-metadata fragment budget
    /// (issue #544), lowered so a live test can reach the fallback on
    /// three streams instead of the ~48,000 it takes to spend
    /// [`super::predicate::MAX_METADATA_FRAGMENT_BYTES`]. `None` is the
    /// shipped constant; production never sets it.
    metadata_fragment_budget: Option<usize>,
    /// The `log_comment` every read this engine issues carries, so
    /// `system.query_log` can attribute a statement to the request that
    /// made it: the server sets the request's id (issue #614), a test its
    /// own tag (issue #544). `None` sends nothing.
    query_log_comment: Option<String>,
    /// The discovery answer's row bound (issue #399):
    /// [`super::params::DISCOVERY_MAX_ROWS`] unless the test seam
    /// [`LogQlEngine::with_discovery_max_rows`] set it.
    discovery_max_rows: u64,
}

/// A discovery answer (issue #399): the items, and whether the answer was
/// cut at its row bound. The return of `label_names`, `label_values` and
/// `detected_labels`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveryAnswer<T> {
    pub items: Vec<T>,
    pub truncated: bool,
}

/// Test-only settings for the lowered reads (issues #507 and #624). The
/// default changes nothing; production never sets them.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct KeyRouteTestHooks {
    /// Test-only: renders a statement-level memory ceiling,
    /// `SETTINGS max_memory_usage = <n>`, into either bucketed range
    /// statement and the reduced `| unwrap` statement (issue #624), so a
    /// live test can make it fail with the server's memory code.
    pub bucketed_statement_max_memory_bytes: Option<u64>,
    /// Test-only: cap overrides laid over the defaults, applied to the
    /// lowered range fold and to today's route alike (issue #624, part 2).
    pub agg_caps: AggCapOverrides,
    /// Test-only: runs a staged plan on today's route (issue #624, part 2).
    pub todays_route_only: bool,
}

/// Test-only cap overrides (issue #624, part 2): each `Some` replaces that
/// cap's default; `None` keeps it.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AggCapOverrides {
    pub group_bytes: Option<u64>,
    pub result_points: Option<u64>,
    /// The retained-window point cap, so a live test can trip it on a few
    /// thousand samples rather than the four million the default takes.
    pub retention_points: Option<u64>,
}

impl LogQlEngine {
    pub fn new(client: ChClient, config: EngineConfig, tenant: Tenant) -> Self {
        Self {
            client,
            config,
            tenant,
            key_route_test: KeyRouteTestHooks::default(),
            metadata_fragment_budget: None,
            query_log_comment: None,
            discovery_max_rows: super::params::DISCOVERY_MAX_ROWS,
        }
    }

    /// Tags every read this engine issues with `comment`, recorded by the
    /// database as `log_comment` (issue #544 code review round 1).
    ///
    /// **The server sets the request's id (issue #614).** A benchmark sets
    /// its own tag to attribute each statement to the request that made
    /// it, and the alternative — matching the statement TEXT for a
    /// predicate literal — attributes nothing on a route whose predicate
    /// never reaches SQL, which is precisely the route being compared.
    /// Every PromQL and LogQL response names the id (docs/api.md).
    pub fn with_query_log_comment(mut self, comment: impl Into<String>) -> Self {
        self.query_log_comment = Some(comment.into());
        self
    }

    /// Lowers the statement's structured-metadata fragment budget (issue
    /// #544). Test-only: it makes the budget fallback reachable on a
    /// three-stream corpus, where the shipped constant needs about 48,000
    /// selected streams.
    #[doc(hidden)]
    pub fn with_metadata_fragment_budget(mut self, bytes: usize) -> Self {
        self.metadata_fragment_budget = Some(bytes);
        self
    }

    /// Lowers the discovery answer's row bound (issue #399). Test-only: it
    /// makes the bound reachable on a three-stream corpus.
    #[doc(hidden)]
    pub fn with_discovery_max_rows(mut self, n: u64) -> Self {
        self.discovery_max_rows = n;
        self
    }

    /// The first server failure this engine's reads met since its client
    /// was bound (issue #803).
    pub fn server_failure(&self) -> Option<ServerFailure> {
        self.client.server_failure()
    }

    /// Saves what this engine's accepted request read into state shared
    /// beyond it (issue #803). The logs engine keeps nothing, so this does
    /// nothing.
    pub fn commit(&self) {}

    /// Binds this engine's reads to a new endpoint and clears its failure
    /// (issue #803): a live tail between polls.
    pub fn rebind(&self) {
        self.client.rebind();
    }

    /// [`Self::rebind`] to any endpoint but `avoid` while another exists
    /// (issue #803): a tail whose server failed does not go back to it.
    pub fn rebind_avoiding(&self, avoid: Option<usize>) {
        self.client.rebind_avoiding(avoid);
    }

    /// Whether an endpoint other than `endpoint` is healthy (issue #803):
    /// whether a tail whose server failed may repeat its poll elsewhere.
    pub fn other_healthy(&self, endpoint: usize) -> bool {
        self.client.other_healthy(endpoint)
    }

    /// The budget this read renders under.
    fn metadata_budget(&self) -> usize {
        self.metadata_fragment_budget
            .unwrap_or(super::predicate::MAX_METADATA_FRAGMENT_BYTES)
    }

    /// Applies [`KeyRouteTestHooks`] (issue #507). Test-only.
    #[doc(hidden)]
    pub fn with_key_route_test_hooks(mut self, hooks: KeyRouteTestHooks) -> Self {
        self.key_route_test = hooks;
        self
    }

    /// The retention caps today's client route runs under: the defaults,
    /// with the test hook's overrides laid over them.
    fn todays_route_caps(&self) -> AggCaps {
        self.lowered_caps()
    }

    /// The caps the lowered range fold runs under: the defaults, with the
    /// test hook's overrides laid over them (issue #624, part 2).
    fn lowered_caps(&self) -> AggCaps {
        let o = self.key_route_test.agg_caps;
        AggCaps {
            group_bytes: o.group_bytes.unwrap_or(AggCaps::DEFAULT.group_bytes),
            result_points: o.result_points.unwrap_or(AggCaps::DEFAULT.result_points),
            retention_points: o
                .retention_points
                .unwrap_or(AggCaps::DEFAULT.retention_points),
            ..AggCaps::DEFAULT
        }
    }
}

/// Issue #286: returns [`CheckedLiteral`]s. The mint IS
/// [`super::escape::ch_string`], so "every service literal reaching a
/// `logql::sql` builder was escaped" stops being a fact about this function
/// and becomes one rustc holds up at the parameter.
fn distinct_escaped_services(meta: &HashMap<Fingerprint, StreamMetaRow>) -> Vec<CheckedLiteral> {
    let mut services: Vec<&str> = meta.values().map(|m| m.service.as_str()).collect();
    services.sort_unstable();
    services.dedup();
    services
        .into_iter()
        .map(super::predicate::literal)
        .collect()
}

#[cfg(test)]
use super::charge;
