//! Issue #827: `Diff` — the diff of two selections' flame graphs — over the
//! Connect protocol.
//!
//! One statement per request (`pulsus_read::profiles::diff`), both merged
//! trees computed in ClickHouse; each side's trim and the diff run here
//! (`pulsus_read::profiles::flamegraph_diff`). In the order: the tenant, the
//! codec and the body, then the planner's own checks — the larger
//! `max_nodes`, the left side, the right side. An absent side is the
//! default request; each side's `format` is ignored.

use axum::body::Body;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use pulsus_model::Tenant;
use pulsus_profiles_wire::querier_v1::{
    DiffRequest, DiffResponse, FlameGraphDiff, Level, SelectMergeStacktracesRequest,
};
use pulsus_read::profiles::ProfilesReadConfig;
use pulsus_read::profiles::diff;
use pulsus_read::profiles::flamegraph_diff;
use pulsus_read::profiles::tree::TreeRequest;

use crate::app::AppState;

use super::connect::ConnectError;
use super::engine::with_engine;
use super::routes::{finish, now_ms, prelude};

/// A side's call site, root first.
fn call_site(msg: &SelectMergeStacktracesRequest) -> Vec<String> {
    msg.stack_trace_selector
        .as_ref()
        .map(|s| s.call_site.iter().map(|l| l.name.clone()).collect())
        .unwrap_or_default()
}

fn tree_request<'a>(
    msg: &'a SelectMergeStacktracesRequest,
    call_site: &'a [String],
) -> TreeRequest<'a> {
    TreeRequest {
        profile_type: &msg.profile_type_id,
        label_selector: &msg.label_selector,
        start_ms: msg.start,
        end_ms: msg.end,
        max_nodes: msg.max_nodes,
        call_site,
        span_selector: &msg.span_selector,
        trace_id_selector: &msg.trace_id_selector,
        profile_id_selector: &msg.profile_id_selector,
    }
}

/// The diff `msg` asks for, in one statement, or none when both windows
/// end before the lookback floor.
async fn flame_graph_diff(
    state: &AppState,
    tenant: &Tenant,
    msg: DiffRequest,
) -> Result<FlameGraphDiff, ConnectError> {
    let left = msg.left.unwrap_or_default();
    let right = msg.right.unwrap_or_default();
    let (left_call_site, right_call_site) = (call_site(&left), call_site(&right));
    let config = ProfilesReadConfig::from_config(&state.config);
    let Some(planned) = diff::plan(
        tenant,
        &config,
        &tree_request(&left, &left_call_site),
        &tree_request(&right, &right_call_site),
        now_ms(),
    )?
    else {
        return Ok(wire(flamegraph_diff::flame_graph_diff(&[], 0, 0)?));
    };
    let sql = planned.sql.as_str();
    let rows = with_engine(state, tenant, |engine| async move {
        Ok(engine.read_diff(sql).await?)
    })
    .await?;
    Ok(wire(flamegraph_diff::flame_graph_diff(
        &rows,
        planned.left_max_nodes,
        planned.right_max_nodes,
    )?))
}

fn wire(g: flamegraph_diff::FlameGraphDiff) -> FlameGraphDiff {
    FlameGraphDiff {
        names: g.names,
        levels: g
            .levels
            .into_iter()
            .map(|values| Level { values })
            .collect(),
        total: g.total,
        max_self: g.max_self,
        left_ticks: g.left_ticks,
        right_ticks: g.right_ticks,
    }
}

/// `POST …/querier.v1.QuerierService/Diff`.
pub(crate) async fn diff(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let (tenant, codec, msg) =
        match prelude::<DiffRequest>(&headers, body, "querier.v1.DiffRequest").await {
            Ok(p) => p,
            Err(refused) => return refused.into_response(),
        };
    let answer = flame_graph_diff(&state, &tenant, msg)
        .await
        .map(|g| DiffResponse {
            flamegraph: Some(g),
        });
    finish(codec, answer)
}
