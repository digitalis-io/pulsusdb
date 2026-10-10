//! The newest-slice-first search plan (issue #595,
//! `docs/TraceQL/server-implementation.md` §3.5).

use pulsus_clickhouse::ChError;

use super::{
    ByteBudget, HYDRATION_BYTE_BUDGET, SearchOutput, TraceEngine, map_search_statement_error,
    map_trace_read_error, search_row_bytes, with_final,
};
use crate::logql::error::ReadError;
use crate::logql::explain::PlanExplain;
use crate::traces::search_plan::SearchPlan;
use crate::traces::spans::rows::SearchTraceRow;
use crate::traces::spans::search::{
    SLICE_NS, SearchStatement, decode_search_charged, plan_statement_sliced,
};

impl TraceEngine {
    /// Runs `stmt`'s top-K over the newest 5 minutes of the window, then
    /// 10, then 20, while the slice holds fewer than `limit` traces and
    /// its density says doubling is cheaper than the window: `n * k >=
    /// 2 * limit`, `k` the slices of the current width the window holds.
    /// `Some` is the answer, a slice's or today's engine's after a
    /// demand; `None` means the whole-window statement answers, and its
    /// `search_statement` stage is pushed.
    pub(super) async fn newest_first(
        &self,
        plan: &SearchPlan,
        stmt: &SearchStatement,
        explain: &mut Option<&mut PlanExplain>,
    ) -> Result<Option<SearchOutput>, ReadError> {
        let window_ns = plan.window.end_ns.saturating_sub(plan.window.start_ns);
        let limit = u64::from(plan.limit);
        let settings = with_final(self.search_settings());
        let mut budget = ByteBudget::new(HYDRATION_BYTE_BUDGET);
        let mut charged = 0usize;
        let mut len = SLICE_NS;
        while stmt.sliceable() && len < window_ns {
            let sliced = plan_statement_sliced(
                plan,
                &self.config.spans_v2_table,
                &self.config.traces_table,
                &self.config.resources_table,
                self.config.max_depth,
                Some(len),
            )
            .expect("a statement that compiled whole compiles sliced");
            if let Some(e) = explain.as_mut() {
                e.push("search_slice", sliced.sql(), None);
            }
            let rows = match self
                .collect_rows_charged::<SearchTraceRow, _>(
                    sliced.sql(),
                    &settings,
                    &mut budget,
                    &mut charged,
                    map_search_statement_error,
                    search_row_bytes,
                )
                .await
            {
                Ok(rows) => rows,
                Err(ReadError::Clickhouse(ChError::Server { code: 395, message }))
                    if sliced
                        .demands()
                        .iter()
                        .any(|demand| message.contains(demand.as_str())) =>
                {
                    return self
                        .search_inner(plan, explain.as_deref_mut())
                        .await
                        .map(Some);
                }
                Err(ReadError::Clickhouse(e)) => {
                    return Err(map_trace_read_error(e, &self.config));
                }
                // A slice a budget refuses hands the search to the
                // whole-window statement, whose own refusal, if any, stands.
                Err(ReadError::QueryTooBroad(_)) => break,
                Err(other) => return Err(other),
            };
            let n = rows.len() as u64;
            if n >= limit {
                return decode_search_charged(rows, sliced.projection(), plan.limit, &mut budget)
                    .map(Some);
            }
            drop(rows);
            budget.release(charged);
            charged = 0;
            let k = u64::try_from((window_ns + len - 1) / len).unwrap_or(u64::MAX);
            if n.saturating_mul(k) < 2 * limit {
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
