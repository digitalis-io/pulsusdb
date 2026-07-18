//! `GET /api/traces/v1/search` (issue #57; docs/api.md §4.2): parse
//! params (`params.rs`) → obtain the TraceQL AST (`q` via
//! `pulsus_traceql::parse`, or the legacy params via `legacy.rs` — one
//! validation path either way) → plan (`pulsus_read::plan_search`) →
//! execute (`TraceEngine::search`) → shape the documented JSON
//! (`search_response.rs`). Thin by design: SQL/execution stays in
//! `pulsus-read`.

use axum::Json;
use axum::extract::{RawQuery, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::app::AppState;

use super::error::ApiError;
use super::handlers::engine_for;
use super::{legacy, params, search_response};

/// `GET /api/traces/v1/search`.
pub(crate) async fn search(State(state): State<AppState>, RawQuery(raw): RawQuery) -> Response {
    match search_impl(state, raw.as_deref().unwrap_or("")).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

async fn search_impl(state: AppState, raw: &str) -> Result<Response, ApiError> {
    let params = params::parse_search_params(raw)?;
    // `q` XOR legacy (both present is already a 400 in the parser);
    // neither present compiles to the `{}` time-only search.
    let q_string = match &params.q {
        Some(q) => q.clone(),
        None => legacy::compile_legacy(
            params.tags.as_deref(),
            params.min_duration.as_deref(),
            params.max_duration.as_deref(),
        )?,
    };
    let query = pulsus_traceql::parse(&q_string).map_err(ApiError::Query)?;

    // Plan BEFORE acquiring the pool: planning needs only config-derived
    // table names/budgets, so every 400-class failure (parse, params,
    // plan) resolves without ClickHouse — same discipline as the logs
    // surface's parse-before-engine ordering.
    let read_config = crate::chconfig::trace_read_config_from(&state.config);
    let ctx = pulsus_read::SearchCtx {
        filter: pulsus_read::SpanFilterCtx {
            spans_table: &read_config.spans_table,
            attrs_table: &read_config.attrs_table,
        },
        max_candidates: read_config.max_candidates,
        distributed: read_config.distributed,
    };
    let search_params = pulsus_read::SearchParams {
        start_ns: params.start_ns,
        end_ns: params.end_ns,
        limit: params.limit,
        spss: params.spss,
    };
    let plan = pulsus_read::plan_search(&query, &search_params, &ctx).map_err(ApiError::Plan)?;

    let engine = engine_for(&state).await?;
    let output = engine.search(&plan).await?;
    Ok((StatusCode::OK, Json(search_response::render(&output))).into_response())
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;

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
        let res = search(State(test_state()), RawQuery(Some(query.to_string()))).await;
        status_and_body(res).await
    }

    // Param/parse failures resolve BEFORE the pool is consulted, so the
    // no-pool test state exercises them end to end; a well-formed request
    // stops at 503 (no pool), proving parse precedes execution.

    #[tokio::test]
    async fn malformed_q_is_400_bad_data_with_a_position() {
        let (status, json) = run("q=%7B&start=1&end=2").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(json.get("position").is_some(), "body {json}");
    }

    #[tokio::test]
    async fn bad_start_is_400_bad_data() {
        let (status, json) = run("q=%7B%7D&start=abc&end=2").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(json.get("position").is_none());
    }

    #[tokio::test]
    async fn q_plus_legacy_is_400_bad_data() {
        let (status, json) = run("q=%7B%7D&tags=a%3Db&start=1&end=2").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
    }

    #[tokio::test]
    async fn malformed_tags_logfmt_is_400_bad_data() {
        let (status, json) = run("tags=barekey&start=1&end=2").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
    }

    #[tokio::test]
    async fn a_planner_type_mismatch_is_400_bad_data() {
        // `name > "x"` parses but the planner rejects ordering on strings.
        let (status, json) = run("q=%7B%20name%20%3E%20%22x%22%20%7D&start=1&end=2").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data", "body {json}");
    }

    #[tokio::test]
    async fn a_metrics_stage_on_search_is_400_bad_data_without_a_position() {
        // Issue #59 error-shape shift: `q={}|rate()` used to be a
        // positioned NotYetSupported parse error; it now PARSES and
        // fails in plan_search — still 400 bad_data, but with no
        // `position` (a plan error, not a parse error).
        let (status, json) = run("q=%7B%7D%20%7C%20rate()&start=1&end=2").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data", "body {json}");
        assert!(json.get("position").is_none(), "body {json}");
        assert!(
            json["error"]
                .as_str()
                .is_some_and(|m| m.contains("metrics")),
            "message must point at the metrics surface, got {json}"
        );
    }

    #[tokio::test]
    async fn a_well_formed_request_without_a_pool_is_503_unavailable() {
        let (status, json) = run("q=%7B%7D&start=1&end=2").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }

    #[tokio::test]
    async fn a_missing_query_string_entirely_is_400_for_the_missing_range() {
        let res = search(State(test_state()), RawQuery(None)).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
    }
}
