//! The newest-slice-first search plan (issue #595,
//! `docs/TraceQL/server-implementation.md` §3.5).

use pulsus_clickhouse::{ChError, QuerySettings};

use super::{
    ByteBudget, GroupCardinalityCounter, HYDRATION_BYTE_BUDGET, SearchOutput, TraceEngine,
    TraceReadConfig, map_search_statement_error, map_trace_read_error, search_grouped_row_bytes,
    search_row_bytes, with_final,
};
use crate::logql::error::ReadError;
use crate::logql::explain::PlanExplain;
use crate::traces::spans::rows::{SearchGroupedRow, SearchTraceRow};
use crate::traces::spans::search::{SLICE_NS, SearchRequest, SearchStatement};

impl TraceEngine {
    /// The attribute indexes this engine's search statements may hint
    /// (issue #595 part 2): `PULSUS_TRACEQL_INDEXED_ATTRIBUTES`.
    pub fn with_indexed_attrs(
        mut self,
        indexed: Vec<crate::traces::spans::attr_index::IndexedAttr>,
    ) -> Self {
        self.indexed = indexed;
        self
    }

    /// Runs `stmt`'s top-K over the newest 5 minutes of the window, then
    /// 10, then 20, while the slice holds fewer than `limit` traces and
    /// its density says doubling is cheaper than the window: `n * k >=
    /// 2 * limit`, `k` the slices of the current width the window holds.
    /// `Some` is a slice's answer; `None` means the whole-window statement
    /// answers, and its `search_statement` stage is pushed. A slice's
    /// demand is the request's `400`. The trace-level statement (issue #601
    /// part 4a) reads the window in one statement, so no slice runs for it.
    pub(super) async fn newest_first(
        &self,
        request: &SearchRequest,
        stmt: &SearchStatement,
        explain: &mut Option<&mut PlanExplain>,
    ) -> Result<Option<SearchOutput>, ReadError> {
        let window_ns = request.end_ns.saturating_sub(request.start_ns);
        let limit = u64::from(request.limit);
        let mut budget = ByteBudget::new(HYDRATION_BYTE_BUDGET);
        let mut charged = 0usize;
        let mut len = SLICE_NS;
        while len < window_ns && !stmt.is_trace_level() {
            let sliced = self
                .search_statement(request, Some(len))
                .expect("a statement that compiled whole compiles sliced");
            if let Some(e) = explain.as_mut() {
                e.push("search_slice", sliced.sql(), None);
            }
            let rows = match self
                .statement_rows(&sliced, &mut budget, &mut charged)
                .await
            {
                Ok(rows) => rows,
                Err(e) if demand_of(&sliced, &e).is_some() => {
                    return Err(statement_error(&sliced, e, &self.config));
                }
                // A slice a budget refuses hands the search to the
                // whole-window statement, whose own refusal, if any, stands.
                Err(ReadError::QueryTooBroad(_)) => break,
                Err(e) => return Err(read_error(e, &self.config)),
            };
            let n = rows.len() as u64;
            if n >= limit {
                return rows
                    .decode(&sliced, request.limit, &mut budget, self.config.max_series)
                    .map(Some);
            }
            drop(rows);
            budget.release(charged);
            charged = 0;
            let k = u64::try_from((window_ns + len - 1) / len).unwrap_or(u64::MAX);
            // Issue #594 part 2: a statement that reads the nested-set
            // numbering keeps doubling, its whole-window statement numbering
            // every trace of the window.
            if n.saturating_mul(k) < 2 * limit && !stmt.reads_numbering() {
                break;
            }
            len = len.saturating_mul(2);
        }
        if let Some(e) = explain.as_mut() {
            e.push("search_statement", stmt.sql(), None);
        }
        Ok(None)
    }
}

/// The demand of `stmt`'s that `e` carries, when `e` is `Code: 395`.
fn demand_of<'a>(stmt: &'a SearchStatement, e: &ReadError) -> Option<&'a String> {
    match e {
        ReadError::Clickhouse(ChError::Server { code: 395, message }) => {
            stmt.demands().iter().find(|d| message.contains(d.as_str()))
        }
        _ => None,
    }
}

/// A statement's error as the request's: a `!` demand it raised is a
/// `400` carrying the demand's text (issue #602 part 2b), as the metrics
/// statement's is; any other ClickHouse error as the trace read path maps
/// it.
pub(super) fn statement_error(
    stmt: &SearchStatement,
    e: ReadError,
    config: &TraceReadConfig,
) -> ReadError {
    if let Some(d) = demand_of(stmt, &e) {
        return ReadError::PipelineInvalid { reason: d.clone() };
    }
    read_error(e, config)
}

/// A statement's ClickHouse error as the trace read path maps it.
pub(super) fn read_error(e: ReadError, config: &TraceReadConfig) -> ReadError {
    match e {
        ReadError::Clickhouse(e) => map_trace_read_error(e, config),
        other => other,
    }
}

/// A search statement's rows: one per trace, flat or with its spansets.
pub(super) enum StatementRows {
    Flat(Vec<SearchTraceRow>),
    Grouped(Vec<SearchGroupedRow>),
}

impl StatementRows {
    pub(super) fn len(&self) -> usize {
        match self {
            StatementRows::Flat(r) => r.len(),
            StatementRows::Grouped(r) => r.len(),
        }
    }

    /// The rows as the answer, the groups counted against `max_series`.
    pub(super) fn decode(
        self,
        stmt: &SearchStatement,
        limit: u32,
        budget: &mut ByteBudget,
        max_series: u64,
    ) -> Result<SearchOutput, ReadError> {
        match (self, stmt.grouping()) {
            (StatementRows::Grouped(rows), Some(grouping)) => {
                let mut counter = GroupCardinalityCounter::new(max_series);
                crate::traces::spans::search::decode_search_grouped_charged(
                    rows,
                    stmt.projection(),
                    grouping,
                    limit,
                    budget,
                    &mut counter,
                )
            }
            (StatementRows::Flat(rows), None) => {
                crate::traces::spans::search::decode_search_charged(
                    rows,
                    stmt.projection(),
                    limit,
                    budget,
                )
            }
            _ => unreachable!("the rows follow the statement's grouping"),
        }
    }
}

impl TraceEngine {
    /// Issues `stmt` once with [`Self::statement_settings`] (and a climb's
    /// depth bound), collecting its rows against `budget`. A ClickHouse
    /// error comes back as it came, for [`statement_error`].
    pub(super) async fn statement_rows(
        &self,
        stmt: &SearchStatement,
        budget: &mut ByteBudget,
        charged: &mut usize,
    ) -> Result<StatementRows, ReadError> {
        let mut settings = self.statement_settings(stmt);
        if let Some(depth) = stmt.climb_depth() {
            settings = settings.set("max_recursive_cte_evaluation_depth", u64::from(depth) + 1);
        }
        if stmt.grouping().is_some() {
            self.collect_rows_charged::<SearchGroupedRow, _>(
                stmt.sql(),
                &settings,
                budget,
                charged,
                map_search_statement_error,
                search_grouped_row_bytes,
            )
            .await
            .map(StatementRows::Grouped)
        } else {
            self.collect_rows_charged::<SearchTraceRow, _>(
                stmt.sql(),
                &settings,
                budget,
                charged,
                map_search_statement_error,
                search_row_bytes,
            )
            .await
            .map(StatementRows::Flat)
        }
    }
}

impl TraceEngine {
    /// A search statement's settings: the search settings, with
    /// [`with_final`] when the statement keeps `final` (issue #601 part 4a:
    /// it reads the nested-set numbering; every other statement keeps one
    /// row per span identity itself), and, clustered,
    /// `distributed_product_mode = 'local'` for a
    /// statement that reads the nested-set numbering (issue #594 part 2),
    /// holds a structural operator (issue #602 part 2b) or reads a span's
    /// child count (issue #600 part 2). That statement
    /// nests a read of `spans` and of `traces` inside reads of the same
    /// tables; clustered, those are `_dist` tables, which ClickHouse's
    /// default `'deny'` refuses. `'local'` reads each nested table on the
    /// shard: both are sharded by `cityHash64(trace_id)`, so a trace's
    /// rows, and so its numbering, its relations and its child counts, are
    /// whole on one shard.
    pub(super) fn statement_settings(&self, stmt: &SearchStatement) -> QuerySettings {
        search_statement_settings(&self.config, stmt)
    }
}

/// [`TraceEngine::statement_settings`], on the engine's configuration.
fn search_statement_settings(config: &TraceReadConfig, stmt: &SearchStatement) -> QuerySettings {
    let settings = super::search_settings(config);
    let settings = if stmt.keeps_final() {
        with_final(settings)
    } else {
        settings
    };
    product_mode(settings, config.distributed, stmt.nests_reads())
}

/// [`TraceEngine::statement_settings`]'s rule, on its own.
fn product_mode(settings: QuerySettings, distributed: bool, nested: bool) -> QuerySettings {
    if distributed && nested {
        settings.set("distributed_product_mode", "local")
    } else {
        settings
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Only a clustered statement that reads the numbering is shard-local.
    #[test]
    fn a_clustered_numbered_statement_reads_its_nested_tables_locally() {
        for (distributed, numbered, want) in [
            (true, true, Some("local")),
            (true, false, None),
            (false, true, None),
            (false, false, None),
        ] {
            let s = product_mode(QuerySettings::new(), distributed, numbered);
            assert_eq!(
                s.get("distributed_product_mode"),
                want,
                "distributed {distributed}, numbered {numbered}"
            );
        }
    }

    /// T13 (issue #601 part 4a): a search statement runs under `final`
    /// only when it reads the nested-set numbering — a predicate, the
    /// projection or a `by()` key.
    #[test]
    fn a_statement_keeps_final_only_when_it_numbers() {
        let cfg = TraceReadConfig {
            spans_table: "trace_spans".to_string(),
            attrs_table: "trace_attrs_idx".to_string(),
            edges_table: "trace_edges".to_string(),
            recent_table: "trace_recent".to_string(),
            errors_table: "trace_error_spans".to_string(),
            spans_v2_table: "spans".to_string(),
            error_spans_table: "error_spans".to_string(),
            traces_table: "traces".to_string(),
            resources_table: "resources".to_string(),
            scan_budget_rows: 1_000,
            max_depth: 64,
            max_series: 1_000,
            read_max_memory_bytes: 1 << 30,
            distributed: false,
            skip_unavailable_shards: false,
        };
        for (q, want) in [
            ("{ status = error }", None),
            ("{ status = error } | count() > 1", None),
            ("{ nestedSetParent > 0 }", Some("1")),
            ("{ } | select(nestedSetLeft)", Some("1")),
        ] {
            let request = SearchRequest {
                start_ns: 1_700_000_000_000_000_000,
                end_ns: 1_700_003_600_000_000_000,
                limit: 20,
                spss: 3,
                query: pulsus_traceql::parse(q).expect("parses"),
            };
            let stmt = crate::traces::spans::search::plan_statement_sliced(
                &request,
                "spans",
                "traces",
                "resources",
                64,
                &[],
                None,
            )
            .unwrap_or_else(|e| panic!("{q}: {e}"));
            assert_eq!(
                search_statement_settings(&cfg, &stmt).get("final"),
                want,
                "{q}"
            );
        }
    }

    /// TP1b (#603 follow-up): the newest-first statements' settings — the
    /// composition `TraceEngine::statement_settings` returns — carry the
    /// answer pins, pruning as the deployment is sharded.
    #[test]
    fn the_newest_first_statements_carry_the_answer_pins() {
        for distributed in [false, true] {
            for nested in [false, true] {
                for skip in [false, true] {
                    let cfg = TraceReadConfig {
                        spans_table: "trace_spans".to_string(),
                        attrs_table: "trace_attrs_idx".to_string(),
                        edges_table: "trace_edges".to_string(),
                        recent_table: "trace_recent".to_string(),
                        errors_table: "trace_error_spans".to_string(),
                        spans_v2_table: "spans".to_string(),
                        error_spans_table: "error_spans".to_string(),
                        traces_table: "traces".to_string(),
                        resources_table: "resources".to_string(),
                        scan_budget_rows: 1_000,
                        max_depth: 64,
                        max_series: 1_000,
                        read_max_memory_bytes: 1 << 30,
                        distributed,
                        skip_unavailable_shards: skip,
                    };
                    let s = product_mode(
                        with_final(super::super::search_settings(&cfg)),
                        distributed,
                        nested,
                    );
                    let pins = QuerySettings::new().answer_pins(distributed, skip);
                    for name in pulsus_clickhouse::ANSWER_PIN_NAMES {
                        assert_eq!(
                            s.get(name),
                            pins.get(name),
                            "distributed {distributed}, nested {nested}, skip {skip}: {name}"
                        );
                    }
                }
            }
        }
    }
}
