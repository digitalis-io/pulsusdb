//! `GET /api/traces/v1/metrics/{query_range,query}` (issue #59;
//! docs/api.md §4.4): parse params (`params.rs`) → `pulsus_traceql::parse`
//! → `pulsus_read::plan_trace_metrics` — all **before** the pool, same
//! discipline as `search.rs`, so every 400/422-class failure resolves
//! without ClickHouse — → `TraceEngine::metrics_range`/`metrics_instant`
//! → the shared Prometheus matrix/vector encoder
//! (`prom_api::encode::query_response`). Thin by design: SQL/execution
//! stays in `pulsus-read`.

use axum::extract::{RawQuery, State};
use axum::response::{IntoResponse, Response};

use crate::app::AppState;

use super::error::ApiError;
use super::handlers::engine_for;
use super::params;

/// Which of the two response forms a request evaluates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetricsForm {
    /// `query_range` → Prometheus matrix.
    Range,
    /// `query` → Prometheus vector (one bucket over the snapped window).
    Instant,
}

/// `GET /api/traces/v1/metrics/query_range`.
pub(crate) async fn metrics_query_range(
    State(state): State<AppState>,
    RawQuery(raw): RawQuery,
) -> Response {
    match metrics_impl(state, raw.as_deref().unwrap_or(""), MetricsForm::Range).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

/// `GET /api/traces/v1/metrics/query`.
pub(crate) async fn metrics_query(
    State(state): State<AppState>,
    RawQuery(raw): RawQuery,
) -> Response {
    match metrics_impl(state, raw.as_deref().unwrap_or(""), MetricsForm::Instant).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

fn now_unix_seconds() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_secs()).unwrap_or(i64::MAX),
        // A pre-epoch clock is pathological; `since` windows anchored at
        // 0 simply fail range validation downstream.
        Err(_) => 0,
    }
}

async fn metrics_impl(state: AppState, raw: &str, form: MetricsForm) -> Result<Response, ApiError> {
    let params = params::parse_metrics_params(raw, now_unix_seconds())?;
    let query = pulsus_traceql::parse(&params.q).map_err(ApiError::Query)?;

    // Plan BEFORE acquiring the pool: planning needs only config-derived
    // table names/budgets, so parse/param/plan failures (including the
    // static 422 point-cap rejection) resolve without ClickHouse.
    let read_config = crate::chconfig::trace_read_config_from(&state.config);
    let ctx = pulsus_read::MetricsCtx {
        filter: pulsus_read::SpanFilterCtx {
            spans_table: &read_config.spans_table,
            attrs_table: &read_config.attrs_table,
        },
        scan_budget_rows: read_config.scan_budget_rows,
        distributed: read_config.distributed,
        skip_unavailable_shards: read_config.skip_unavailable_shards,
    };
    let metrics_params = pulsus_read::MetricsParams {
        start_ns: params.start_ns,
        end_ns: params.end_ns,
        step_s: params.step_s,
    };
    let plan =
        pulsus_read::plan_trace_metrics(&query, &metrics_params, &ctx).map_err(ApiError::Plan)?;

    let engine = engine_for(&state).await?;
    let (result, at_ms) = match form {
        // `at_ms` is only read for vector/scalar results — the matrix's
        // points carry their own bucket timestamps.
        MetricsForm::Range => (engine.metrics_range(&plan).await?, 0),
        MetricsForm::Instant => (engine.metrics_instant(&plan).await?, plan.snapped_end_ms()),
    };
    Ok(crate::prom_api::encode::query_response(result, None, at_ms))
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;
    use axum::http::StatusCode;

    use super::*;
    use crate::app::BuildInfo;
    use crate::ingest::{MetricWriterSink, TraceWriterSink, WriterSink};
    use pulsus_config::Config;
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
            started_at: std::time::SystemTime::now(),
        }
    }

    async fn status_and_body(res: Response) -> (StatusCode, serde_json::Value) {
        let status = res.status();
        let bytes = to_bytes(res.into_body(), usize::MAX).await.expect("body");
        let json: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        (status, json)
    }

    async fn run_range(query: &str) -> (StatusCode, serde_json::Value) {
        let res = metrics_query_range(State(test_state()), RawQuery(Some(query.to_string()))).await;
        status_and_body(res).await
    }

    async fn run_instant(query: &str) -> (StatusCode, serde_json::Value) {
        let res = metrics_query(State(test_state()), RawQuery(Some(query.to_string()))).await;
        status_and_body(res).await
    }

    // Param/parse/plan failures resolve BEFORE the pool is consulted, so
    // the no-pool test state exercises them end to end; a well-formed
    // request stops at 503 (no pool), proving plan precedes execution.

    const RATE_Q: &str = "q=%7B%7D%20%7C%20rate()";

    #[tokio::test]
    async fn a_malformed_traceql_expression_is_400_bad_data_with_a_position() {
        let (status, json) = run_range("q=%7B&start=1700000000&end=1700003600").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(json.get("position").is_some(), "body {json}");
    }

    #[tokio::test]
    async fn a_missing_range_is_400_bad_data() {
        let (status, json) = run_range(RATE_Q).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(json.get("position").is_none());
    }

    #[tokio::test]
    async fn a_bad_step_is_400_bad_data_on_both_forms() {
        let query = format!("{RATE_Q}&start=1700000000&end=1700003600&step=500ms");
        let (status, json) = run_range(&query).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data", "body {json}");
        let (status, json) = run_instant(&query).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data", "body {json}");
    }

    #[tokio::test]
    async fn a_search_only_pipeline_on_metrics_is_400_bad_data() {
        // `{} | count() > 2` parses but the metrics planner rejects it.
        let (status, json) =
            run_range("q=%7B%7D%20%7C%20count()%20%3E%202&start=1700000000&end=1700003600").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data", "body {json}");
    }

    #[tokio::test]
    async fn a_missing_metric_stage_is_400_bad_data() {
        let (status, json) = run_range("q=%7B%7D&start=1700000000&end=1700003600").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data", "body {json}");
    }

    #[tokio::test]
    async fn a_cross_spanset_metrics_query_is_400_bad_data() {
        let q = "q=%7B%7D%20%26%26%20%7B%7D%20%7C%20rate()&start=1700000000&end=1700003600";
        let (status, json) = run_range(q).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data", "body {json}");
    }

    #[tokio::test]
    async fn exceeding_the_point_cap_is_a_static_422_query_too_broad() {
        // 1,000,000 seconds at step=1 → 1M buckets >> MAX_METRICS_POINTS,
        // rejected at plan time — no pool needed, so this no-pool state
        // proves the rejection is pre-execution.
        let (status, json) =
            run_range(&format!("{RATE_Q}&start=1700000000&end=1701000000&step=1")).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json["errorType"], "query_too_broad", "body {json}");
        assert!(json.get("position").is_none());
    }

    #[tokio::test]
    async fn a_well_formed_request_without_a_pool_is_503_on_both_forms() {
        let query = format!("{RATE_Q}&start=1700000000&end=1700003600&step=60");
        let (status, json) = run_range(&query).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
        let (status, json) = run_instant(&query).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }

    #[tokio::test]
    async fn nanosecond_and_rfc3339_timestamps_are_accepted_on_both_forms() {
        // docs/api.md §1/§4.4 (code review round 1): the metrics
        // endpoints accept unix s / ns / RFC3339. A well-formed request
        // in either form reaches the pool gate (503 here), never a 400.
        let query =
            format!("{RATE_Q}&start=1700000000000000000&end=2023-11-14T23%3A13%3A20Z&step=60");
        let (status, json) = run_range(&query).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "body {json}");
        let (status, json) = run_instant(&query).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "body {json}");
    }

    #[tokio::test]
    async fn extreme_ns_endpoints_are_a_static_422_never_a_panic_or_500() {
        // Full-i64-range endpoints: the i128 plan math resolves this to
        // the static point-cap 422 before any pool/SQL work (code review
        // round 1's overflow class, proven end to end through the
        // handler).
        let query = format!("{RATE_Q}&start={}&end={}&step=1", i64::MIN, i64::MAX);
        let (status, json) = run_range(&query).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "body {json}");
        assert_eq!(json["errorType"], "query_too_broad", "body {json}");
    }

    #[tokio::test]
    async fn a_missing_query_string_entirely_is_400() {
        let res = metrics_query_range(State(test_state()), RawQuery(None)).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
    }
}
