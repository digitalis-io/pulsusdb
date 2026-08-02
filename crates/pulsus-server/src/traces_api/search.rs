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
        Some(q) => q.as_str().to_string(),
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
        max_series: read_config.max_series,
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

    // --- issue #326: the query-frontend validator's parse step ----------

    /// The reference validates the `query` parameter on the search
    /// pipeline even though its request parser never reads it as TraceQL
    /// (`async_query_validator_middleware.go:49-55` vs
    /// `pkg/api/http.go:180`). PulsusDB served this `200` before; it is a
    /// `400` now, with the same envelope a malformed `q` produces.
    ///
    /// Reachable over the wire, unlike the issue #284 length cap: a
    /// malformed expression is short, so `http::Uri`'s 65,534-byte
    /// request-target limit (#296) never intercepts it.
    #[tokio::test]
    async fn a_malformed_query_parameter_is_400_bad_data_with_a_position() {
        let (status, json) = run("query=%7B&start=1&end=2").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert_eq!(json["position"], 1, "body {json}");
        assert!(
            json["error"]
                .as_str()
                .is_some_and(|m| m.starts_with("invalid TraceQL query: ")),
            "the reference's wrapping, got {json}"
        );
    }

    /// The rejection is about admission, not about routing: a `query`
    /// that parses is accepted and the search still runs the time-only
    /// `{}` (503 here only because no pool is configured). Were `query`
    /// wrongly aliased to `q`, this would still be 503 — so the
    /// not-an-alias half stays pinned by
    /// `params::tests::a_malformed_search_query_parameter_is_rejected_though_it_never_runs`,
    /// which asserts `q` is `None`.
    #[tokio::test]
    async fn a_parseable_query_parameter_is_admitted_and_the_search_still_runs() {
        let (status, json) =
            run("query=%7B%20resource.service.name%20%3D%20%22checkout%22%20%7D&start=1&end=2")
                .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "body {json}");
    }

    /// DIVERGENCE PIN (issue #326). The reference's validator is
    /// `ParseNoOptimizations` **and then** `traceql.Validate`
    /// (`async_query_validator_middleware.go:49-52`). Only the parse half
    /// is reproduced: PulsusDB's semantic rejections live in
    /// `pulsus_read::plan_search`, which is route-specific — planning the
    /// `query` parameter would reject `{} | rate()`, which the reference's
    /// validator accepts.
    ///
    /// The contrast is the whole point, and it is why this test can fail:
    /// ONE expression, a regex literal that does not compile
    /// (`ast_validate.go:222-245` rejects it; the reference's own
    /// middleware test pins that shape as `400`). As `q` — executed, so
    /// planned — it is a `400` here. As `query` — validated only — it is
    /// accepted, where the reference rejects it. A route-independent
    /// validator would turn the second half RED and retire the
    /// divergence note in `querytext.rs`.
    #[tokio::test]
    async fn a_query_parameter_failing_only_the_reference_semantic_validation_is_accepted() {
        // `{ span.a =~ "[" }` — an unterminated character class.
        const EXPR: &str = "%7B%20span.a%20%3D~%20%22%5B%22%20%7D";

        let (status, json) = run(&format!("q={EXPR}&start=1&end=2")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "body {json}");
        assert!(
            json["error"]
                .as_str()
                .is_some_and(|m| m.contains("invalid regex")),
            "the planner is what sees it, got {json}"
        );

        let (status, json) = run(&format!("query={EXPR}&start=1&end=2")).await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "accepted today — the `traceql.Validate` half is not reproduced; body {json}"
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
