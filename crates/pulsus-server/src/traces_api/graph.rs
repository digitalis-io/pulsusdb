//! `GET /api/traces/v1/service_graph` (issue #173, M7-E1; docs/api.md
//! §4.5): parse the window (`params.rs`) — all **before** the pool, same
//! discipline as `search.rs`/`metrics.rs`, so every 400-class failure
//! resolves without ClickHouse — → `TraceEngine::service_graph` → shape the
//! documented JSON envelope here. Thin by design: the two-level pushed-down
//! aggregation and its 422 scan-budget throw stay in `pulsus-read`. There is
//! no Tempo-compat alias: the interop reference has no service-graph HTTP
//! endpoint (its panels read the edge metrics as Prometheus series), so this
//! is a PulsusDB-native surface only.

use axum::Json;
use axum::extract::{RawQuery, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};

use pulsus_read::{GraphWindow, ServiceGraph};

use crate::app::AppState;

use super::error::ApiError;
use super::handlers::engine_for;
use super::params;

/// `GET /api/traces/v1/service_graph`.
pub(crate) async fn service_graph(
    State(state): State<AppState>,
    RawQuery(raw): RawQuery,
) -> Response {
    match graph_impl(state, raw.as_deref().unwrap_or("")).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

fn now_unix_seconds() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_secs()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}

async fn graph_impl(state: AppState, raw: &str) -> Result<Response, ApiError> {
    let params = params::parse_graph_params(raw, now_unix_seconds())?;
    let window = GraphWindow {
        start_ns: params.start_ns,
        end_ns: params.end_ns,
    };

    let engine = engine_for(&state).await?;
    let graph = engine.service_graph(window).await?;
    Ok((StatusCode::OK, Json(render(&graph))).into_response())
}

/// Shapes the documented docs/api.md §4.5 envelope: one object per
/// `(client, server, connectionType)` edge with its replay-deduped `calls`,
/// `failed` count, and the three TDigest latency quantiles as `p50Ns`/
/// `p95Ns`/`p99Ns` (nanoseconds, `f64`). `truncated` is the non-silent cap
/// indicator.
fn render(graph: &ServiceGraph) -> Value {
    let edges: Vec<Value> = graph
        .edges
        .iter()
        .map(|e| {
            // `quantiles_ns` is `[p50, p95, p99]` by construction
            // (`quantilesTDigest(0.5, 0.95, 0.99)`); index defensively so a
            // pathologically short array can never panic the handler.
            let q = |i: usize| e.quantiles_ns.get(i).copied().unwrap_or(0.0);
            json!({
                "client": e.client,
                "server": e.server,
                "connectionType": e.conn_type,
                "calls": e.calls,
                "failed": e.failed,
                "p50Ns": q(0),
                "p95Ns": q(1),
                "p99Ns": q(2),
            })
        })
        .collect();
    json!({ "edges": edges, "truncated": graph.truncated })
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;

    use super::*;
    use crate::app::BuildInfo;
    use crate::ingest::{MetricWriterSink, TraceWriterSink, WriterSink};
    use pulsus_config::Config;
    use pulsus_read::GraphEdgeRow;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    fn test_state() -> AppState {
        AppState {
            pool: Arc::new(RwLock::new(None)),
            config: Arc::new(Config::default()),
            metrics: metrics_exporter_prometheus::PrometheusBuilder::new()
                .build_recorder()
                .handle(),
            build: BuildInfo::from_build_env(),
            writer: Arc::new(WriterSink::new(Arc::new(std::sync::OnceLock::new()))),
            metric_writer: Arc::new(MetricWriterSink::new(Arc::new(std::sync::OnceLock::new()))),
            trace_writer: Arc::new(TraceWriterSink::new(Arc::new(std::sync::OnceLock::new()))),
            label_cache: Arc::new(std::sync::OnceLock::new()),
            eval_gate: Arc::new(pulsus_read::EvalGate::new(
                pulsus_config::Config::default()
                    .reader
                    .query_eval_concurrency,
            )),
            started_at: std::time::SystemTime::now(),
            tail: std::sync::Arc::new(crate::app::TailRuntime::for_tests()),
        }
    }

    async fn status_and_body(res: Response) -> (StatusCode, serde_json::Value) {
        let status = res.status();
        let bytes = to_bytes(res.into_body(), usize::MAX).await.expect("body");
        let json: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        (status, json)
    }

    async fn run(query: &str) -> (StatusCode, serde_json::Value) {
        let res = service_graph(State(test_state()), RawQuery(Some(query.to_string()))).await;
        status_and_body(res).await
    }

    // Param failures resolve BEFORE the pool is consulted, so the no-pool
    // test state exercises them end to end; a well-formed request stops at
    // 503 (no pool), proving parse precedes execution.

    #[tokio::test]
    async fn a_missing_window_is_400_bad_data() {
        let (status, json) = run("").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(json.get("position").is_none());
    }

    #[tokio::test]
    async fn an_inverted_range_is_400_bad_data() {
        let (status, json) = run("start=200&end=100").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data", "body {json}");
    }

    #[tokio::test]
    async fn since_together_with_absolute_bounds_is_400_bad_data() {
        let (status, json) = run("since=1h&start=1700000000&end=1700003600").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data", "body {json}");
    }

    #[tokio::test]
    async fn a_well_formed_request_without_a_pool_is_503_unavailable() {
        let (status, json) = run("start=1700000000&end=1700003600").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }

    #[test]
    fn render_shapes_the_documented_envelope() {
        let graph = ServiceGraph {
            edges: vec![GraphEdgeRow {
                client: "checkout".to_string(),
                server: "payments".to_string(),
                conn_type: "rpc".to_string(),
                calls: 123,
                failed: 4,
                quantiles_ns: vec![1_200_000.0, 3_400_000.0, 9_900_000.0],
            }],
            truncated: false,
        };
        let json = render(&graph);
        assert_eq!(json["truncated"], false);
        let edge = &json["edges"][0];
        assert_eq!(edge["client"], "checkout");
        assert_eq!(edge["server"], "payments");
        assert_eq!(edge["connectionType"], "rpc");
        assert_eq!(edge["calls"], 123);
        assert_eq!(edge["failed"], 4);
        assert_eq!(edge["p50Ns"], 1_200_000.0);
        assert_eq!(edge["p95Ns"], 3_400_000.0);
        assert_eq!(edge["p99Ns"], 9_900_000.0);
    }

    #[test]
    fn render_of_an_empty_graph_is_the_empty_envelope() {
        let json = render(&ServiceGraph {
            edges: vec![],
            truncated: false,
        });
        assert_eq!(json, json!({ "edges": [], "truncated": false }));
    }
}
