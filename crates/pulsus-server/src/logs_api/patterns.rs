//! `GET|POST /api/logs/v1/patterns` (M7-C3, issue #171, docs/api.md §2.6): parse
//! `query`/`start`/`end`/`step`, validate the query shape (a bare stream
//! selector — ANY pipeline stage is a 400, line filters included, like
//! `/volume`; templates are precomputed and the bodies are gone), floor `step`
//! to the 10s ingest bucket and reject an over-11k grid, dispatch to
//! `LogQlEngine::patterns`, and encode the Loki-interop envelope. Pushdown-only
//! by construction: one aggregate over `log_patterns` with `fingerprint`
//! primary-key prefix pruning and a server-side top-1000 (no hydration, no body
//! read), visible via `X-Pulsus-Explain`.

use axum::body::Body;
use axum::extract::{RawQuery, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};

use pulsus_logql::Expr;
use pulsus_read::TimeBounds;

use crate::app::AppState;

use super::encode;
use super::error::ApiError;
use super::handlers::{engine_for, parse_bounds, read_form_pairs};
use super::params::{self, ParamError};

/// `X-Pulsus-Explain: 1` — same header contract as the query endpoints.
fn wants_explain(headers: &HeaderMap) -> bool {
    headers
        .get("x-pulsus-explain")
        .and_then(|v| v.to_str().ok())
        == Some("1")
}

pub(crate) async fn patterns(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Response {
    let pairs = params::parse_pairs(raw.as_deref().unwrap_or(""));
    match patterns_impl(state, &headers, pairs).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

/// `POST /api/logs/v1/patterns` (issue #406 Part B2): the reference
/// registers `/loki/api/v1/patterns` `Methods("GET","POST")`
/// (`pkg/loki/modules.go:692`, `:1371` @ v3.7.4 `b318f282`) and answers a
/// form POST `200` where we answered `405` — measured 2026-08-10.
pub(crate) async fn patterns_post(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
    body: Body,
) -> Response {
    match read_form_pairs(&headers, raw.as_deref(), body).await {
        Ok(pairs) => match patterns_impl(state, &headers, pairs).await {
            Ok(res) => res,
            Err(e) => e.into_response(),
        },
        Err(response) => response,
    }
}

async fn patterns_impl(
    state: AppState,
    headers: &HeaderMap,
    pairs: Vec<(String, String)>,
) -> Result<Response, ApiError> {
    let query = params::get(&pairs, "query").ok_or(ParamError::MissingQuery)?;
    let expr = super::parse_logql(query)?;
    validate_patterns_query(&expr)?;
    // `parse_bounds`, NOT `parse_bounds_ordered`: `/patterns` is one of
    // the reference's two `end < start` exemptions —
    // `ParsePatternsQuery` never compares the bounds
    // (`pkg/loghttp/patterns.go:9-38` @ v3.7.4 `b318f282`), and a
    // reversed window measured `200` there. That measurement is only
    // meaningful against a reference whose `/patterns` can answer at all
    // (`pattern_ingester.enabled: true`, else a nil `patternQuerier`
    // 404s — `pkg/querier/querier.go:810-816`); the control that
    // establishes it, and the container configuration it needs, are
    // written out at `mod.rs`'s
    // `end_before_start_is_refused_on_exactly_the_reference_routes`,
    // which pins this route as a NEGATIVE row.
    let (start_ns, end_ns) = parse_bounds(&pairs)?;
    // Floor to 10s + reject an over-11k grid, in pure param parsing — BEFORE
    // any pool/engine/SQL work.
    let step_ns = params::parse_pattern_step(params::get(&pairs, "step"), start_ns, end_ns)?;
    let bounds = TimeBounds { start_ns, end_ns };

    // Kill-switch (M7-C3 AC 5b): `PULSUS_LOG_PATTERNS=false` disables the whole
    // feature — the endpoint stays mounted but serves empty data, never reading
    // historical rows extracted while the flag was on. Short-circuit AFTER param
    // validation (a malformed request is still a 400) but BEFORE any pool/engine
    // work.
    if !state.config.writer.log_patterns {
        return Ok(encode::patterns_response(Vec::new(), None));
    }

    let engine = engine_for(&state).await?;
    if wants_explain(headers) {
        let (series, explain) = engine.patterns_explained(&expr, bounds, step_ns).await?;
        Ok(encode::patterns_response(series, Some(explain)))
    } else {
        let series = engine.patterns(&expr, bounds, step_ns).await?;
        Ok(encode::patterns_response(series, None))
    }
}

/// Patterns accepts a bare stream selector only: a metric query has no log
/// patterns, and the stored templates are body-content-blind — ANY pipeline
/// stage (even a line filter, which `/stats` tolerates via its raw fallback)
/// is meaningless, so all are rejected 400 here, before any engine/pool work.
fn validate_patterns_query(expr: &Expr) -> Result<(), ParamError> {
    match expr {
        Expr::Log(le) if le.pipeline.is_empty() => Ok(()),
        Expr::Log(_) => Err(ParamError::PatternsPipelineUnsupported),
        _ => Err(ParamError::MetricQueryUnsupported {
            endpoint: "patterns",
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::body::to_bytes;
    use axum::http::StatusCode;
    use pulsus_config::Config;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    use crate::app::BuildInfo;
    use crate::ingest::{MetricWriterSink, TraceWriterSink, WriterSink};

    fn test_state() -> AppState {
        state_with_config(Config::default())
    }

    fn state_with_config(config: Config) -> AppState {
        AppState {
            pool: Arc::new(RwLock::new(None)),
            config: Arc::new(config),
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

    /// `(status, body text)`. Issue #264: every error on this surface is a
    /// bare `text/plain` body, so tests assert the raw message; success
    /// bodies are still JSON and those cases parse the returned string.
    ///
    /// On any 4xx/5xx this also asserts the reference's error container —
    /// `Content-Type: text/plain; charset=utf-8` + `X-Content-Type-Options:
    /// nosniff`, non-empty body (`pkg/util/server/error.go:48-51 @
    /// v3.7.4`) — so every error case in this module covers the wire
    /// shape, not just its status code.
    async fn status_and_body(res: Response) -> (StatusCode, String) {
        let status = res.status();
        if status.is_client_error() || status.is_server_error() {
            assert_eq!(
                res.headers()
                    .get(axum::http::header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok()),
                Some("text/plain; charset=utf-8"),
            );
            assert_eq!(
                res.headers()
                    .get(axum::http::header::X_CONTENT_TYPE_OPTIONS)
                    .and_then(|v| v.to_str().ok()),
                Some("nosniff"),
            );
        }
        let bytes = to_bytes(res.into_body(), usize::MAX).await.expect("body");
        let text = String::from_utf8(bytes.to_vec()).expect("utf-8 body");
        if status.is_client_error() || status.is_server_error() {
            assert!(!text.is_empty(), "an error body is never empty");
        }
        (status, text)
    }

    async fn get(query: Option<&str>) -> (StatusCode, String) {
        get_with(test_state(), query).await
    }

    async fn get_with(state: AppState, query: Option<&str>) -> (StatusCode, String) {
        let res = patterns(
            State(state),
            HeaderMap::new(),
            RawQuery(query.map(str::to_string)),
        )
        .await;
        status_and_body(res).await
    }

    const SELECTOR: &str = "query=%7Bservice_name%3D%22checkout%22%7D";

    #[tokio::test]
    async fn patterns_missing_query_param_is_400() {
        let (status, _body) = get(None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn patterns_malformed_logql_is_400_with_the_byte_offset_in_the_message() {
        let (status, body) = get(Some("query=%7B")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("at byte"), "{body}");
    }

    /// A metric query on the patterns surface is rejected 400 BEFORE any
    /// pool/engine work (no pool exists here, yet it is 400, not the
    /// 503 the pool check would produce).
    #[tokio::test]
    async fn patterns_metric_query_is_400_before_the_pool_check() {
        let (status, body) = get(Some("query=count_over_time(%7Bapp%3D%22x%22%7D%5B1h%5D)")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("metric queries"), "{body}");
    }

    /// Unlike `/stats`, even a LINE FILTER is rejected — the stored templates
    /// are body-content-blind and patterns has no raw fallback.
    #[tokio::test]
    async fn patterns_line_filter_pipeline_is_400() {
        let (status, body) = get(Some("query=%7Bapp%3D%22x%22%7D%20%7C%3D%20%22err%22")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("bare stream selector"), "{body}");
    }

    #[tokio::test]
    async fn patterns_parser_pipeline_is_400() {
        let (status, _body) = get(Some("query=%7Bapp%3D%22x%22%7D%20%7C%20logfmt")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn patterns_non_positive_step_is_400() {
        let (status, _body) = get(Some(&format!("{SELECTOR}&step=0"))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    /// An over-11k bucket grid is rejected 400 in pure param parsing (no pool
    /// exists, yet it is 400, not 503).
    #[tokio::test]
    async fn patterns_over_11k_grid_is_400_before_the_pool_check() {
        // 11_001 × 10s window at a 10s step ⇒ 11_001 buckets > 11_000.
        let end = 11_001i64 * 10_000_000_000;
        let (status, body) = get(Some(&format!("{SELECTOR}&start=0&end={end}&step=10"))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("bucket grid too large"), "{body}");
    }

    /// A bare selector (with in-cap params) is shape-valid; with no pool it
    /// reaches the 503 pool check (proving validation passed).
    #[tokio::test]
    async fn patterns_valid_selector_passes_validation_then_503_without_a_pool() {
        let (status, _body) = get(Some(&format!("{SELECTOR}&step=30"))).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    /// Issue #279 (AC4): `/patterns` — a valid selector of exactly
    /// 131,072 bytes (`MAX_QUERY_BYTES`, the reference's `maxInputSize`;
    /// one byte past the longest accepted query) is 400 against a
    /// POOLLESS state, while a valid query is 503 (the test above) — the
    /// parse precedes the pool check, so the 400 is the cap.
    #[tokio::test]
    async fn patterns_rejects_an_over_cap_query_400_before_the_pool_check() {
        let (status, body) = get(Some(&format!(
            r#"query={{app="{}"}}"#,
            "a".repeat(131_072 - 8)
        )))
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body, "input size too long (131072 > 131072)");
    }

    /// AC 5b (M7-C3, issue #171 review finding 2): with the kill-switch OFF,
    /// the endpoint stays mounted but serves the empty success payload — a
    /// valid selector returns `{"status":"success","data":[]}` and NEVER
    /// reaches the pool (no 503), so historical rows are never re-served.
    #[tokio::test]
    async fn patterns_kill_switch_off_returns_empty_success_without_the_pool() {
        let mut config = Config::default();
        config.writer.log_patterns = false;
        let (status, body) = get_with(state_with_config(config), Some(SELECTOR)).await;
        assert_eq!(status, StatusCode::OK);
        let json: serde_json::Value = serde_json::from_str(&body).expect("success body is JSON");
        assert_eq!(json["status"], "success");
        assert_eq!(json["data"], serde_json::json!([]));
    }
}
