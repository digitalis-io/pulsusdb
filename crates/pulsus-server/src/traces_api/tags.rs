//! The two §4.3 tag-discovery handlers (issue #58): parse (`params.rs`)
//! → catalog read via `TraceEngine::list_tag_names`/`list_tag_values`
//! (`pulsus-read` — `trace_tag_catalog` ONLY, never spans/attr-index/
//! payloads) → shape the documented JSON (`tags_response.rs`). Thin by
//! design, mirroring `search.rs`.
//!
//! Contract corners (docs/api.md §4.3, adjudicated on issue #58):
//! `start`/`end` are accepted and ignored on both routes (the catalog is
//! time-less); `q=` on the values route is accepted and ignored —
//! results may be a superset of what a narrowing query would return
//! (Tempo's own best-effort semantics; a 400 would break Grafana
//! autocomplete). The values route therefore never parses its query
//! string at all.

use axum::Json;
use axum::extract::{Path, RawQuery, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::app::AppState;

use super::error::ApiError;
use super::handlers::engine_for;
use super::{params, tags_response};

/// `GET /api/traces/v1/tags` — scoped tag-name discovery.
pub(crate) async fn tags(State(state): State<AppState>, RawQuery(raw): RawQuery) -> Response {
    match tags_impl(state, raw.as_deref().unwrap_or("")).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

async fn tags_impl(state: AppState, raw: &str) -> Result<Response, ApiError> {
    // Parse before the pool: `scope=bogus` resolves 400 without
    // ClickHouse — the search surface's parse-before-engine ordering.
    let params = params::parse_tags_params(raw)?;
    let engine = engine_for(&state).await?;
    let names = engine.list_tag_names(params.scope.as_deref()).await?;
    Ok((
        StatusCode::OK,
        Json(tags_response::render_tag_names(&names)),
    )
        .into_response())
}

/// `GET /api/traces/v1/tag/{tag}/values` — typed value discovery for one
/// key. The query string (`q`/`start`/`end`) is ignored entirely
/// (module doc); the scope comes from the `{tag}` prefix.
pub(crate) async fn tag_values(State(state): State<AppState>, Path(tag): Path<String>) -> Response {
    match tag_values_impl(state, &tag).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

async fn tag_values_impl(state: AppState, raw_tag: &str) -> Result<Response, ApiError> {
    let (scope, key) = params::parse_tag_path(raw_tag)?;
    let engine = engine_for(&state).await?;
    let values = engine.list_tag_values(&key, scope.as_deref()).await?;
    Ok((
        StatusCode::OK,
        Json(tags_response::render_tag_values(&values)),
    )
        .into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::body::to_bytes;
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

    // Param failures resolve BEFORE the pool is consulted (no-pool test
    // state); a well-formed request stops at 503, proving parse precedes
    // execution.

    #[tokio::test]
    async fn a_bogus_scope_is_400_bad_data_before_the_pool() {
        let res = tags(
            State(test_state()),
            RawQuery(Some("scope=bogus".to_string())),
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
    }

    #[tokio::test]
    async fn a_well_formed_tags_request_without_a_pool_is_503_unavailable() {
        let res = tags(State(test_state()), RawQuery(None)).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }

    #[tokio::test]
    async fn an_empty_tag_key_is_400_bad_data_before_the_pool() {
        let res = tag_values(State(test_state()), Path("resource.".to_string())).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
    }

    #[tokio::test]
    async fn a_well_formed_values_request_without_a_pool_is_503_unavailable() {
        let res = tag_values(State(test_state()), Path("service.name".to_string())).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }
}
