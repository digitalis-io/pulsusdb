//! The T9 Tempo-compat handlers that are NOT pure route bindings (issue
//! #61, docs/api.md §8.1): the v1 flat (`/api/search/tags`,
//! `/api/search/tag/{tag}/values`) and v2 scoped/typed
//! (`/api/v2/search/tags`, `/api/v2/search/tag/{tag}/values`)
//! tag-discovery reshapings, plus the constant `/api/echo`. The
//! reshapings are in-memory projections over the SAME single
//! `trace_tag_catalog` read the native §4.3 handlers perform
//! (`engine_for` + `list_tag_names`/`list_tag_values` — zero extra query
//! work); param parsing is shared with the native handlers
//! (`params.rs`), so alias error behavior (bogus scope 400, empty key
//! 400, no-pool 503) is native-identical — only the success body shape
//! differs, per renderer (`tags_response.rs`). Mounting is
//! `compat_router()`'s job (`mod.rs`), gated by `crate::compat`.

use axum::Json;
use axum::extract::{Path, RawQuery, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use pulsus_read::{TagNames, TagValues};

use crate::app::AppState;

use super::error::ApiError;
use super::handlers::engine_for;
use super::{params, tags_response};

/// Shared fetch for both tag-name reshapings — parse-before-pool, then
/// the exact catalog read `tags::tags` performs.
async fn tag_names_for(state: AppState, raw: &str) -> Result<TagNames, ApiError> {
    let params = params::parse_tags_params(raw)?;
    let engine = engine_for(&state).await?;
    Ok(engine.list_tag_names(params.scope.as_deref()).await?)
}

/// Shared fetch for both tag-value reshapings — parse-before-pool, then
/// the exact catalog read `tags::tag_values` performs (the query string
/// is ignored entirely, same contract as native).
async fn tag_values_for(state: AppState, raw_tag: &str) -> Result<TagValues, ApiError> {
    let (scope, key) = params::parse_tag_path(raw_tag)?;
    let engine = engine_for(&state).await?;
    Ok(engine.list_tag_values(&key, scope.as_deref()).await?)
}

/// `GET /api/search/tags` — Tempo v1 flat `{"tagNames":[…]}`.
pub(crate) async fn tags_v1(State(state): State<AppState>, RawQuery(raw): RawQuery) -> Response {
    match tag_names_for(state, raw.as_deref().unwrap_or("")).await {
        Ok(names) => (
            StatusCode::OK,
            Json(tags_response::render_tag_names_flat(&names)),
        )
            .into_response(),
        Err(e) => e.into_response(),
    }
}

/// `GET /api/search/tag/{tag}/values` — Tempo v1 flat
/// `{"tagValues":[…]}` (bare strings).
pub(crate) async fn tag_values_v1(
    State(state): State<AppState>,
    Path(tag): Path<String>,
) -> Response {
    match tag_values_for(state, &tag).await {
        Ok(values) => (
            StatusCode::OK,
            Json(tags_response::render_tag_values_flat(&values)),
        )
            .into_response(),
        Err(e) => e.into_response(),
    }
}

/// `GET /api/v2/search/tags` — the native scoped shape minus
/// `truncated`.
pub(crate) async fn tags_v2(State(state): State<AppState>, RawQuery(raw): RawQuery) -> Response {
    match tag_names_for(state, raw.as_deref().unwrap_or("")).await {
        Ok(names) => (
            StatusCode::OK,
            Json(tags_response::render_tag_names_scoped_v2(&names)),
        )
            .into_response(),
        Err(e) => e.into_response(),
    }
}

/// `GET /api/v2/search/tag/{tag}/values` — the native typed shape minus
/// `truncated`.
pub(crate) async fn tag_values_v2(
    State(state): State<AppState>,
    Path(tag): Path<String>,
) -> Response {
    match tag_values_for(state, &tag).await {
        Ok(values) => (
            StatusCode::OK,
            Json(tags_response::render_tag_values_typed_v2(&values)),
        )
            .into_response(),
        Err(e) => e.into_response(),
    }
}

/// `GET /api/echo` — Tempo's constant liveness echo: `200` with the
/// exact body `echo`. No I/O, no pool.
pub(crate) async fn echo() -> impl IntoResponse {
    (StatusCode::OK, "echo")
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
        }
    }

    async fn status_and_body(res: Response) -> (StatusCode, serde_json::Value) {
        let status = res.status();
        let bytes = to_bytes(res.into_body(), usize::MAX).await.expect("body");
        let json: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        (status, json)
    }

    // The reshaping handlers share the native parse-before-pool ordering:
    // a param failure resolves 400 without ClickHouse; a well-formed
    // request against the no-pool test state stops at 503.

    #[tokio::test]
    async fn a_well_formed_v1_tags_request_without_a_pool_is_503_unavailable() {
        let res = tags_v1(State(test_state()), RawQuery(None)).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }

    #[tokio::test]
    async fn a_well_formed_v1_values_request_without_a_pool_is_503_unavailable() {
        let res = tag_values_v1(State(test_state()), Path("service.name".to_string())).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }

    #[tokio::test]
    async fn a_well_formed_v2_tags_request_without_a_pool_is_503_unavailable() {
        let res = tags_v2(State(test_state()), RawQuery(None)).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }

    #[tokio::test]
    async fn a_well_formed_v2_values_request_without_a_pool_is_503_unavailable() {
        let res = tag_values_v2(State(test_state()), Path("service.name".to_string())).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }

    #[tokio::test]
    async fn a_bogus_scope_on_the_v1_tags_alias_is_400_bad_data_before_the_pool() {
        let res = tags_v1(
            State(test_state()),
            RawQuery(Some("scope=bogus".to_string())),
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
    }

    #[tokio::test]
    async fn an_empty_tag_key_on_the_v2_values_alias_is_400_bad_data_before_the_pool() {
        let res = tag_values_v2(State(test_state()), Path("resource.".to_string())).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
    }

    #[tokio::test]
    async fn echo_returns_200_with_the_constant_body() {
        let res = echo().await.into_response();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = to_bytes(res.into_body(), usize::MAX).await.expect("body");
        assert_eq!(&bytes[..], b"echo");
    }
}
