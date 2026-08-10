//! `GET|POST /api/logs/v1/volume` (issue #169, docs/api.md §2.6): parse
//! `query`/`start`/`end`/`limit`/`aggregateBy`/`targetLabels`, validate
//! the query shape (a bare stream selector — ANY pipeline stage is a 400,
//! line filters included, unlike `/stats`), dispatch to
//! `LogQlEngine::volume`, and encode the order-preserving vector envelope
//! at `end`. Rollup-only by construction: matchers-only queries are
//! always served from `log_metrics_5s` with zero body reads (there is no
//! raw fallback), visible via `X-Pulsus-Explain`. The `targetLabels`
//! caps (`params::MAX_TARGET_LABELS`/`MAX_TARGET_LABEL_BYTES`) reject
//! here, in pure param parsing, BEFORE any AST mutation/planning/SQL.

use axum::body::Body;
use axum::extract::{RawQuery, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};

use pulsus_logql::Expr;
use pulsus_read::{TimeBounds, VolumeQuery};

use crate::app::AppState;

use super::encode;
use super::error::ApiError;
use super::handlers::{engine_for, parse_bounds_ordered, read_form_pairs};
use super::params::{self, ParamError};

/// `X-Pulsus-Explain: 1` — same header contract as the query endpoints.
fn wants_explain(headers: &HeaderMap) -> bool {
    headers
        .get("x-pulsus-explain")
        .and_then(|v| v.to_str().ok())
        == Some("1")
}

pub(crate) async fn volume(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Response {
    let pairs = params::parse_pairs(raw.as_deref().unwrap_or(""));
    match volume_impl(state, &headers, pairs).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

/// `POST /api/logs/v1/volume` (issue #406 Part B2): the reference
/// registers `/loki/api/v1/index/volume` `Methods("GET","POST")`
/// (`pkg/loki/modules.go:691`, `:1369` @ v3.7.4 `b318f282`) and answers a
/// form POST `200` where we answered `405` — measured 2026-08-10.
pub(crate) async fn volume_post(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
    body: Body,
) -> Response {
    match read_form_pairs(&headers, raw.as_deref(), body).await {
        Ok(pairs) => match volume_impl(state, &headers, pairs).await {
            Ok(res) => res,
            Err(e) => e.into_response(),
        },
        Err(response) => response,
    }
}

async fn volume_impl(
    state: AppState,
    headers: &HeaderMap,
    pairs: Vec<(String, String)>,
) -> Result<Response, ApiError> {
    let query = params::get(&pairs, "query").ok_or(ParamError::MissingQuery)?;
    let expr = super::parse_logql(query)?;
    validate_volume_query(&expr)?;
    // `/volume`'s `end < start` refusal predates issue #406 and is now
    // the shared one (`parse_bounds_ordered`), so all seven routes that
    // carry it carry the same implementation and the same message.
    let (start_ns, end_ns) = parse_bounds_ordered(&pairs)?;
    let limit = params::parse_volume_limit(params::get(&pairs, "limit"))?;
    let aggregate_by = params::parse_aggregate_by(params::get(&pairs, "aggregateBy"))?;
    // Bounded HERE (count + per-entry length caps), before the engine
    // ever injects a matcher from these values (issue #169 plan v2).
    let target_labels = params::parse_target_labels(params::get(&pairs, "targetLabels"))?;
    let q = VolumeQuery {
        bounds: TimeBounds { start_ns, end_ns },
        limit,
        aggregate_by,
        target_labels,
    };

    let engine = engine_for(&state).await?;
    if wants_explain(headers) {
        let (entries, explain) = engine.volume_explained(&expr, &q).await?;
        Ok(encode::volume_response(entries, end_ns, Some(explain)))
    } else {
        let entries = engine.volume(&expr, &q).await?;
        Ok(encode::volume_response(entries, end_ns, None))
    }
}

/// Volume accepts a bare stream selector only: a metric query has no
/// stream volume, and the rollup is body-content-blind — ANY pipeline
/// stage (even a line filter, which `/stats` tolerates via its raw
/// fallback) would silently over-count, so all are rejected 400 here,
/// before any engine/pool work.
fn validate_volume_query(expr: &Expr) -> Result<(), ParamError> {
    match expr {
        Expr::Log(le) if le.pipeline.is_empty() => Ok(()),
        Expr::Log(_) => Err(ParamError::VolumePipelineUnsupported),
        _ => Err(ParamError::MetricQueryUnsupported { endpoint: "volume" }),
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
        let res = volume(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(query.map(str::to_string)),
        )
        .await;
        status_and_body(res).await
    }

    const SELECTOR: &str = "query=%7Bservice_name%3D%22checkout%22%7D";

    #[tokio::test]
    async fn volume_missing_query_param_is_400() {
        let (status, _body) = get(None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn volume_malformed_logql_is_400_with_the_byte_offset_in_the_message() {
        let (status, body) = get(Some("query=%7B")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("at byte"), "{body}");
    }

    /// Issue #169: a metric query on the volume surface is rejected 400
    /// BEFORE any pool/engine work (no pool exists here, yet the error is
    /// 400, not the 503 the pool check would produce).
    #[tokio::test]
    async fn volume_metric_query_is_400_before_the_pool_check() {
        let (status, body) = get(Some("query=count_over_time(%7Bapp%3D%22x%22%7D%5B1h%5D)")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("metric queries"), "{body}");
    }

    /// Issue #169: unlike `/stats`, even a LINE FILTER is rejected — the
    /// rollup is body-content-blind and volume has no raw fallback.
    #[tokio::test]
    async fn volume_line_filter_pipeline_is_400() {
        let (status, body) = get(Some("query=%7Bapp%3D%22x%22%7D%20%7C%3D%20%22err%22")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("bare stream selector"), "{body}");
    }

    #[tokio::test]
    async fn volume_parser_pipeline_is_400() {
        let (status, _body) = get(Some("query=%7Bapp%3D%22x%22%7D%20%7C%20logfmt")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn volume_invalid_aggregate_by_is_400() {
        let (status, body) = get(Some(&format!("{SELECTOR}&aggregateBy=both"))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("aggregateBy"), "{body}");
    }

    #[tokio::test]
    async fn volume_limit_above_the_cap_is_400() {
        let (status, _body) = get(Some(&format!("{SELECTOR}&limit=5001"))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn volume_end_before_start_is_400() {
        let (status, body) = get(Some(&format!("{SELECTOR}&start=200&end=100"))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("precedes"), "{body}");
    }

    /// Issue #169 plan v2 (b)(ii): oversized `targetLabels` (count) is
    /// rejected 400 while NO pool exists — mechanical proof the rejection
    /// precedes injection/engine/SQL work.
    #[tokio::test]
    async fn volume_too_many_target_labels_is_400_before_the_pool_check() {
        let over_cap = (0..33)
            .map(|i| format!("l{i}"))
            .collect::<Vec<_>>()
            .join(",");
        let (status, body) = get(Some(&format!("{SELECTOR}&targetLabels={over_cap}"))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("too many 'targetLabels'"), "{body}");
    }

    /// Issue #169 plan v2 (b)(ii): the per-entry length cap, same
    /// pool-less pre-injection proof.
    #[tokio::test]
    async fn volume_overlong_target_label_is_400_before_the_pool_check() {
        let long = "x".repeat(257);
        let (status, body) = get(Some(&format!("{SELECTOR}&targetLabels={long}"))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("exceeds the maximum"), "{body}");
    }

    /// A bare selector (with in-cap params) is shape-valid; with no pool
    /// it reaches the 503 pool check (proving validation passed).
    #[tokio::test]
    async fn volume_valid_selector_passes_validation_then_503_without_a_pool() {
        let (status, _body) = get(Some(&format!(
            "{SELECTOR}&limit=0&aggregateBy=labels&targetLabels=env,team"
        )))
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    /// Issue #279 (AC4): `/volume` — a valid selector of exactly 131,072
    /// bytes (`MAX_QUERY_BYTES`, the reference's `maxInputSize`; one byte
    /// past the longest accepted query) is 400 against a POOLLESS state,
    /// while a valid query is 503 (the test above) — the parse precedes
    /// the pool check, so the 400 is the cap.
    #[tokio::test]
    async fn volume_rejects_an_over_cap_query_400_before_the_pool_check() {
        let (status, body) = get(Some(&format!(
            r#"query={{app="{}"}}"#,
            "a".repeat(131_072 - 8)
        )))
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body, "input size too long (131072 > 131072)");
    }
}
