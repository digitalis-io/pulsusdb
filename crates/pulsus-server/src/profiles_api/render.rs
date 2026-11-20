//! Issue #829: the render routes — `GET /api/profiles/v1/render`,
//! `/render-diff` and `/label-values`, and their compat paths under
//! `/pyroscope` — answered as the reference's HTTP handlers answer them
//! (`pkg/querier/http.go:39-325`), each in one statement.
//!
//! `render` reads the merged tree and the time series of one selection in
//! one statement (`pulsus_read::profiles::render`) and answers the
//! flamebearer with its timeline; `render-diff` is the `Diff` method's one
//! statement as the `double` flamebearer; `label-values` is the profile
//! types for `__name__`, else the named label's values over the last hour.
//! In the order: the tenant, then the parameters, then the planner's own
//! checks.

use std::collections::BTreeMap;

use axum::extract::{RawQuery, State};
use axum::http::{HeaderMap, header};
use axum::response::{IntoResponse, Response};
use pulsus_model::Tenant;
use pulsus_profiles_wire::connect::go_string_array;
use pulsus_profiles_wire::flamebearer::{Flamebearer, Timeline};
use pulsus_profiles_wire::querier_v1::{
    DiffRequest, ProfileTypesRequest, SelectMergeStacktracesRequest,
};
use pulsus_profiles_wire::types_v1::LabelValuesRequest;
use pulsus_read::profiles::ProfilesReadConfig;
use pulsus_read::profiles::flamegraph;
use pulsus_read::profiles::render::{self, RenderRequest};
use pulsus_read::profiles::series;
use pulsus_read::profiles::tree::TreeRequest;

use crate::app::AppState;
use crate::logs_api::params::parse_pairs;

use super::connect::{Code, ConnectError};
use super::diff::flame_graph_diff;
use super::engine::with_engine;
use super::routes::{
    attime, label_values_answer, now_ms, profile_types_answer, split_query, tenant_of,
};

/// The window `label-values` reads for a label other than `__name__`.
const LABEL_VALUES_WINDOW_MS: i64 = 3_600_000;

/// One selection's parameters, as `parseSelectProfilesRequest` reads them.
#[derive(Debug)]
struct Selection {
    type_id: String,
    selector: String,
    start_ms: i64,
    end_ms: i64,
}

/// The first value of `key`.
fn first<'a>(pairs: &'a [(String, String)], key: &str) -> Option<&'a str> {
    pairs
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

/// The selection named by `query`, `from` and `until`: `query` required and
/// split at its first `{`; a time absent or empty is `now`.
fn selection(
    pairs: &[(String, String)],
    [query, from, until]: [&str; 3],
    now: i64,
) -> Result<Selection, ConnectError> {
    let q = first(pairs, query).unwrap_or_default();
    if q.is_empty() {
        return Err(ConnectError::new(
            Code::InvalidArgument,
            format!("{query:?} is required"),
        ));
    }
    let (type_id, selector) = split_query(q);
    let time = |key: &str| match first(pairs, key) {
        Some(v) if !v.is_empty() => attime(v, now),
        _ => now,
    };
    Ok(Selection {
        type_id: type_id.to_string(),
        selector: selector.to_string(),
        start_ms: time(from),
        end_ms: time(until),
    })
}

/// `max-nodes`, then `maxNodes`, the later winning: a value that is not an
/// integer, or 0, is ignored; neither is 0, the default.
fn max_nodes(pairs: &[(String, String)]) -> i64 {
    ["maxNodes", "max-nodes"]
        .iter()
        .filter_map(|key| first(pairs, key)?.parse::<i64>().ok())
        .find(|n| *n != 0)
        .unwrap_or(0)
}

fn json(body: String) -> Response {
    ([(header::CONTENT_TYPE, "application/json")], body).into_response()
}

fn answer(body: Result<String, ConnectError>) -> Response {
    match body {
        Ok(body) => json(body),
        Err(e) => e.into_response(),
    }
}

/// `GET /api/profiles/v1/render?query=<type>{selector}&from=&until=&max-nodes=&groupBy=&aggregation=&format=`.
pub(crate) async fn render_get(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Response {
    let tenant = match tenant_of(&headers) {
        Ok(t) => t,
        Err(e) => return e.into_response(),
    };
    let pairs = parse_pairs(raw.as_deref().unwrap_or(""));
    answer(render_answer(&state, &tenant, &pairs).await)
}

async fn render_answer(
    state: &AppState,
    tenant: &Tenant,
    pairs: &[(String, String)],
) -> Result<String, ConnectError> {
    let sel = selection(pairs, ["query", "from", "until"], now_ms())?;
    let max_nodes = max_nodes(pairs);
    let group_by: Vec<String> = pairs
        .iter()
        .filter(|(k, _)| k == "groupBy")
        .map(|(_, v)| v.clone())
        .collect();
    let average = first(pairs, "aggregation") == Some("avg");
    if first(pairs, "format") == Some("dot") {
        return Err(ConnectError::new(
            Code::Unimplemented,
            "format dot is not served yet",
        ));
    }
    let config = ProfilesReadConfig::from_config(&state.config);
    let req = RenderRequest {
        tree: TreeRequest {
            profile_type: &sel.type_id,
            label_selector: &sel.selector,
            start_ms: sel.start_ms,
            end_ms: sel.end_ms,
            max_nodes: Some(max_nodes),
            ..TreeRequest::default()
        },
        group_by: &group_by,
        average,
    };
    let (graph, series) = match render::plan(tenant, &config, &req, now_ms())? {
        None => (flamegraph::flame_graph(&[], 0, 0)?, Vec::new()),
        Some(planned) => {
            let sql = planned.sql.as_str();
            let rows = with_engine(state, tenant, |engine| async move {
                Ok(engine.read_render(sql).await?)
            })
            .await?;
            let (tree_rows, series_rows) = render::split(rows);
            (
                flamegraph::flame_graph(&tree_rows, planned.max_nodes, planned.raw_max_nodes)?,
                series::series(series_rows, &group_by, None),
            )
        }
    };
    let step_s = render::point_interval_s(sel.start_ms, sel.end_ms);
    let timeline_of = |s: Option<&series::Series>| {
        let points: Vec<(i64, f64)> = s
            .map(|s| s.points.iter().map(|p| (p.timestamp, p.value)).collect())
            .unwrap_or_default();
        let (start_time, samples) = render::timeline(&points, sel.start_ms, sel.end_ms, step_s);
        Timeline {
            start_time,
            samples,
            duration_delta: step_s,
        }
    };
    let timeline = timeline_of(match series.as_slice() {
        [one] => Some(one),
        _ => None,
    });
    let groups = (!group_by.is_empty()).then(|| {
        let mut groups = BTreeMap::new();
        for s in &series {
            let key = s
                .labels
                .iter()
                .find(|(name, _)| *name == group_by[0])
                .map_or("*", |(_, value)| value.as_str());
            groups.insert(key.to_string(), timeline_of(Some(s)));
        }
        groups
    });
    Ok(Flamebearer {
        names: graph.names,
        levels: graph.levels,
        num_ticks: graph.total,
        max_self: graph.max_self,
        double: false,
        type_id: sel.type_id,
        timeline: Some(timeline),
        groups,
        left_ticks: 0,
        right_ticks: 0,
    }
    .to_json())
}

/// `GET /api/profiles/v1/render-diff?leftQuery=&leftFrom=&leftUntil=&rightQuery=&rightFrom=&rightUntil=&max-nodes=`.
pub(crate) async fn render_diff_get(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Response {
    let tenant = match tenant_of(&headers) {
        Ok(t) => t,
        Err(e) => return e.into_response(),
    };
    let pairs = parse_pairs(raw.as_deref().unwrap_or(""));
    answer(render_diff_answer(&state, &tenant, &pairs).await)
}

async fn render_diff_answer(
    state: &AppState,
    tenant: &Tenant,
    pairs: &[(String, String)],
) -> Result<String, ConnectError> {
    let now = now_ms();
    let left = selection(pairs, ["leftQuery", "leftFrom", "leftUntil"], now)?;
    let right = selection(pairs, ["rightQuery", "rightFrom", "rightUntil"], now)?;
    if left.type_id != right.type_id {
        return Err(ConnectError::new(
            Code::InvalidArgument,
            "profile types must match",
        ));
    }
    let n = max_nodes(pairs);
    let side = |s: &Selection| SelectMergeStacktracesRequest {
        profile_type_id: s.type_id.clone(),
        label_selector: s.selector.clone(),
        start: s.start_ms,
        end: s.end_ms,
        max_nodes: Some(n),
        ..SelectMergeStacktracesRequest::default()
    };
    let msg = DiffRequest {
        left: Some(side(&left)),
        right: Some(side(&right)),
    };
    let g = flame_graph_diff(state, tenant, msg).await?;
    Ok(Flamebearer {
        names: g.names,
        levels: g.levels,
        num_ticks: g.total,
        max_self: g.max_self,
        double: true,
        type_id: left.type_id,
        timeline: None,
        groups: None,
        left_ticks: g.left_ticks,
        right_ticks: g.right_ticks,
    }
    .to_json())
}

/// `GET /api/profiles/v1/label-values?label=`: the profile types' ids for
/// `__name__`, else the label's values over the last hour, with no
/// matchers. The reference sends no name, so answers `400` for every
/// other label; `docs/reference-defects-we-do-not-copy.md` §49.
pub(crate) async fn label_values_get(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Response {
    let tenant = match tenant_of(&headers) {
        Ok(t) => t,
        Err(e) => return e.into_response(),
    };
    let pairs = parse_pairs(raw.as_deref().unwrap_or(""));
    answer(label_values_answer_json(&state, &tenant, &pairs).await)
}

async fn label_values_answer_json(
    state: &AppState,
    tenant: &Tenant,
    pairs: &[(String, String)],
) -> Result<String, ConnectError> {
    let label = first(pairs, "label").unwrap_or_default();
    if label.is_empty() {
        return Err(ConnectError::new(
            Code::InvalidArgument,
            "label parameter is required",
        ));
    }
    let values: Vec<String> = if label == "__name__" {
        profile_types_answer(state, tenant, ProfileTypesRequest { start: 0, end: 0 })
            .await?
            .profile_types
            .into_iter()
            .map(|t| t.id)
            .collect()
    } else {
        let now = now_ms();
        let msg = LabelValuesRequest {
            name: label.to_string(),
            matchers: Vec::new(),
            start: now - LABEL_VALUES_WINDOW_MS,
            end: now,
        };
        label_values_answer(state, tenant, msg).await?.names
    };
    Ok(go_string_array(&values))
}
