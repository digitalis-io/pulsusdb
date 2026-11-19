//! Issue #823: `SelectMergeStacktraces` — the merged flame graph — over the
//! Connect protocol, and `GET /api/profiles/v1/merge`.
//!
//! One statement per request (`pulsus_read::profiles::tree`), the merge done
//! in ClickHouse; the trim to `max_nodes` runs here
//! (`pulsus_read::profiles::flamegraph`). The Connect answer carries the
//! trimmed tree's bytes beside the flame graph; `GET /merge` answers the
//! flame graph alone and builds no bytes. In the order: the tenant, the
//! codec and the body, a span selector with a trace selector, the format,
//! then the planner's own checks.

use axum::body::Body;
use axum::extract::{RawQuery, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use pulsus_model::Tenant;
use pulsus_profiles_wire::querier_v1::{
    FlameGraph, Level, ProfileFormat, SelectMergeStacktracesRequest, SelectMergeStacktracesResponse,
};
use pulsus_read::profiles::ProfilesReadConfig;
use pulsus_read::profiles::flamegraph;
use pulsus_read::profiles::tree::{self, TreeRequest, TreeRow};

use crate::app::AppState;
use crate::logs_api::params::parse_pairs;

use super::connect::{Code, Codec, ConnectError};
use super::engine::with_engine;
use super::routes::{all, finish, now_ms, prelude, split_query, tenant_of, time_param};

/// The flame graph `msg` asks for, in one statement, or none when its
/// window ends before the lookback floor; with `with_tree`, the trimmed
/// tree's bytes beside it (empty otherwise).
async fn flame_graph(
    state: &AppState,
    tenant: &Tenant,
    msg: SelectMergeStacktracesRequest,
    with_tree: bool,
) -> Result<(FlameGraph, Vec<u8>), ConnectError> {
    tree::refuse_span_with_trace(&msg.span_selector, &msg.trace_id_selector)?;
    if let Ok(f @ (ProfileFormat::Tree | ProfileFormat::Dot | ProfileFormat::Pprof)) =
        ProfileFormat::try_from(msg.format)
    {
        return Err(ConnectError::new(
            Code::Unimplemented,
            format!("profile format {} is not served yet", f.as_str_name()),
        ));
    }
    let config = ProfilesReadConfig::from_config(&state.config);
    let call_site: Vec<String> = msg
        .stack_trace_selector
        .map(|s| s.call_site.into_iter().map(|l| l.name).collect())
        .unwrap_or_default();
    let req = TreeRequest {
        profile_type: &msg.profile_type_id,
        label_selector: &msg.label_selector,
        start_ms: msg.start,
        end_ms: msg.end,
        max_nodes: msg.max_nodes,
        call_site: &call_site,
        span_selector: &msg.span_selector,
        trace_id_selector: &msg.trace_id_selector,
        profile_id_selector: &msg.profile_id_selector,
    };
    let Some(planned) = tree::plan(tenant, &config, &req, now_ms())? else {
        return Ok((wire(flamegraph::flame_graph(&[], 0, 0)?), Vec::new()));
    };
    let sql = planned.sql.as_str();
    let rows = with_engine(state, tenant, |engine| async move {
        Ok(engine.read_tree(sql).await?)
    })
    .await?;
    let (graph, tree) = trimmed(&rows, planned.max_nodes, planned.raw_max_nodes, with_tree)?;
    Ok((wire(graph), tree))
}

/// The flame graph of S5's rows, and the trimmed tree's bytes when
/// `with_tree` (empty otherwise).
fn trimmed(
    rows: &[TreeRow],
    m: i64,
    raw: i64,
    with_tree: bool,
) -> Result<(flamegraph::FlameGraph, Vec<u8>), ConnectError> {
    if with_tree {
        Ok(flamegraph::flame_graph_with_tree(rows, m, raw)?)
    } else {
        Ok((flamegraph::flame_graph(rows, m, raw)?, Vec::new()))
    }
}

fn wire(g: flamegraph::FlameGraph) -> FlameGraph {
    FlameGraph {
        names: g.names,
        levels: g
            .levels
            .into_iter()
            .map(|values| Level { values })
            .collect(),
        total: g.total,
        max_self: g.max_self,
    }
}

/// `POST …/querier.v1.QuerierService/SelectMergeStacktraces`.
pub(crate) async fn select_merge_stacktraces(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let (tenant, codec, msg) = match prelude::<SelectMergeStacktracesRequest>(
        &headers,
        body,
        "querier.v1.SelectMergeStacktracesRequest",
    )
    .await
    {
        Ok(p) => p,
        Err(refused) => return refused.into_response(),
    };
    let answer = flame_graph(&state, &tenant, msg, true)
        .await
        .map(|(g, tree)| SelectMergeStacktracesResponse {
            flamegraph: Some(g),
            tree,
            pprof: None,
        });
    finish(codec, answer)
}

/// Whether `GET /merge` builds the tree bytes: it answers the flame graph
/// alone, so it does not.
const MERGE_GET_WITH_TREE: bool = false;

/// `GET /api/profiles/v1/merge?query=&from=&until=&maxNodes=`: the
/// `FlameGraph` message's JSON. `query` is the type, then the selector from
/// its first `{`; with no `{`, the selector is `{}`.
pub(crate) async fn merge_get(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Response {
    let tenant = match tenant_of(&headers) {
        Ok(t) => t,
        Err(e) => return e.into_response(),
    };
    let pairs = parse_pairs(raw.as_deref().unwrap_or(""));
    let msg = match merge_request(&pairs, now_ms()) {
        Ok(m) => m,
        Err(e) => return e.into_response(),
    };
    finish(
        Codec::Json,
        flame_graph(&state, &tenant, msg, MERGE_GET_WITH_TREE)
            .await
            .map(|(g, _)| g),
    )
}

/// The `GET` route's parameters as the Connect request.
fn merge_request(
    pairs: &[(String, String)],
    now: i64,
) -> Result<SelectMergeStacktracesRequest, ConnectError> {
    let first = |key: &str| {
        pairs
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    };
    let (profile_type_id, label_selector) = split_query(first("query").unwrap_or(""));
    let max_nodes = match first("maxNodes") {
        None => None,
        Some(v) => Some(v.parse::<i64>().map_err(|_| {
            ConnectError::new(Code::InvalidArgument, format!("invalid maxNodes {v:?}"))
        })?),
    };
    Ok(SelectMergeStacktracesRequest {
        profile_type_id: profile_type_id.to_string(),
        label_selector: label_selector.to_string(),
        start: time_param(pairs, "from", now),
        end: time_param(pairs, "until", now),
        max_nodes,
        span_selector: all(pairs, "spanSelector[]"),
        trace_id_selector: all(pairs, "traceIdSelector[]"),
        ..SelectMergeStacktracesRequest::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulsus_write::protocols::profile_model;

    /// D2: the stack read's day slack is the ingest admission window — a
    /// sample is received at most 10 minutes before and 1 hour after its
    /// time.
    #[test]
    fn the_stack_day_slack_is_the_admission_window() {
        assert_eq!(
            tree::STACK_DAY_BEFORE_MS,
            profile_model::REJECT_NEWER_THAN_MS
        );
        assert_eq!(
            tree::STACK_DAY_AFTER_MS,
            profile_model::REJECT_OLDER_THAN_MS
        );
    }

    /// `GET /merge` answers the flame graph alone, so it builds no tree
    /// bytes; the Connect answer builds them from the same rows.
    #[test]
    fn the_get_route_builds_no_tree() {
        let rows = [
            TreeRow {
                node: 1,
                parent: 0,
                name: "main".to_string(),
                total: 5,
                self_value: 0,
            },
            TreeRow {
                node: 2,
                parent: 1,
                name: "a".to_string(),
                total: 5,
                self_value: 5,
            },
        ];
        let graph = flamegraph::flame_graph(&rows, 8192, 0).expect("a graph");
        let (get, tree) = trimmed(&rows, 8192, 0, MERGE_GET_WITH_TREE).expect("a graph");
        assert_eq!((get, tree), (graph.clone(), Vec::new()), "GET /merge");
        let (connect, tree) = trimmed(&rows, 8192, 0, true).expect("a graph");
        assert_eq!(connect, graph, "Connect");
        assert!(!tree.is_empty(), "the Connect answer builds the tree bytes");
    }

    fn pairs(v: &[(&str, &str)]) -> Vec<(String, String)> {
        v.iter()
            .map(|(k, val)| (k.to_string(), val.to_string()))
            .collect()
    }

    /// The `GET` route splits `query` at its first `{`, reads `maxNodes`
    /// as an integer, and refuses anything else.
    #[test]
    fn the_get_route_reads_its_parameters() {
        let m = merge_request(
            &pairs(&[
                ("query", r#"a:b:c:d:e{service_name="x"}"#),
                ("from", "1000"),
                ("until", "2000"),
                ("maxNodes", "4"),
                ("spanSelector[]", "0000000000000001"),
            ]),
            0,
        )
        .expect("a request");
        assert_eq!(m.profile_type_id, "a:b:c:d:e");
        assert_eq!(m.label_selector, r#"{service_name="x"}"#);
        assert_eq!((m.start, m.end), (1_000_000, 2_000_000));
        assert_eq!(m.max_nodes, Some(4));
        assert_eq!(m.span_selector, ["0000000000000001"]);
        let m = merge_request(&pairs(&[("query", "a:b:c:d:e")]), 0).expect("a request");
        assert_eq!((m.label_selector.as_str(), m.max_nodes), ("{}", None));
        let e = merge_request(&pairs(&[("maxNodes", "x")]), 0).expect_err("refused");
        assert_eq!(
            (e.code, e.message.as_str()),
            (Code::InvalidArgument, r#"invalid maxNodes "x""#)
        );
    }
}
