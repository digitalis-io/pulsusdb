//! The five `/api/logs/v1` handlers (docs/api.md §2): parse params → parse
//! LogQL (`pulsus-logql`) → dispatch to `LogQlEngine` (`pulsus-read`) →
//! encode the envelope (`encode.rs`). Thin by design — all planning/SQL/
//! execution stays in `pulsus-read` (issue #13 architect plan).

use axum::body::Bytes;
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, header};
use axum::response::{IntoResponse, Response};

use pulsus_logql::{Expr, LogExpr};
use pulsus_read::{LogQlEngine, QueryParams, QuerySpec, TimeBounds};

use crate::app::AppState;
use crate::chconfig;

use super::encode;
use super::error::ApiError;
use super::params::{self, ParamError};

/// `X-Pulsus-Explain: 1` (docs/api.md "Request headers"): included on all
/// five endpoints (issue #13 architect plan amendment §4).
fn wants_explain(headers: &HeaderMap) -> bool {
    headers
        .get("x-pulsus-explain")
        .and_then(|v| v.to_str().ok())
        == Some("1")
}

/// Acquires the shared `Arc<ChPool>` from `AppState` (mirrors `ops::ready`'s
/// pattern: clone the `Option` out from behind the lock, drop the guard
/// before doing anything else) and builds a `LogQlEngine` over it —
/// `503 unavailable` before the pool is established, matching `/ready`.
async fn engine_for(state: &AppState) -> Result<LogQlEngine, ApiError> {
    let pool = {
        let guard = state.pool.read().await;
        guard.clone()
    };
    let pool = pool.ok_or(ApiError::PoolUnavailable)?;
    Ok(chconfig::logql_engine(pool, &state.config))
}

/// Parses `start`/`end` (defaults: `end = now`, `start = end - 1h`,
/// docs/api.md §2.1).
fn parse_bounds(pairs: &[(String, String)]) -> Result<(i64, i64), ParamError> {
    let now = params::now_ns();
    let end_ns = match params::get(pairs, "end") {
        Some(v) => params::parse_ts(v)?,
        None => now,
    };
    let start_ns = match params::get(pairs, "start") {
        Some(v) => params::parse_ts(v)?,
        None => params::default_start_ns(end_ns),
    };
    Ok((start_ns, end_ns))
}

async fn read_form_pairs(
    headers: &HeaderMap,
    body: Bytes,
) -> Result<Vec<(String, String)>, ApiError> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !content_type.starts_with("application/x-www-form-urlencoded") {
        return Err(ApiError::Param(ParamError::UnsupportedContentType(
            content_type.to_string(),
        )));
    }
    let text =
        std::str::from_utf8(&body).map_err(|_| ApiError::Param(ParamError::InvalidFormBody))?;
    Ok(params::parse_pairs(text))
}

// ---------------------------------------------------------------------
// GET|POST /api/logs/v1/query_range
// ---------------------------------------------------------------------

pub(crate) async fn query_range(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Response {
    let pairs = params::parse_pairs(raw.as_deref().unwrap_or(""));
    match query_range_impl(state, &headers, pairs).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

/// `POST /api/logs/v1/query_range`: same param names as GET, as an
/// `application/x-www-form-urlencoded` body (task-manager ratification on
/// issue #13 amendment 3 finding 2 — large queries/long ranges can exceed
/// URL length limits; mainstream Loki-datasource clients POST this
/// endpoint).
pub(crate) async fn query_range_post(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    match read_form_pairs(&headers, body).await {
        Ok(pairs) => match query_range_impl(state, &headers, pairs).await {
            Ok(res) => res,
            Err(e) => e.into_response(),
        },
        Err(e) => e.into_response(),
    }
}

async fn query_range_impl(
    state: AppState,
    headers: &HeaderMap,
    pairs: Vec<(String, String)>,
) -> Result<Response, ApiError> {
    let query = params::get(&pairs, "query").ok_or(ParamError::MissingQuery)?;
    let expr = pulsus_logql::parse(query)?;
    let (start_ns, end_ns) = parse_bounds(&pairs)?;
    let step_ns = params::parse_step(params::get(&pairs, "step"), start_ns, end_ns)?;
    let limit = params::parse_limit(params::get(&pairs, "limit"))?;
    let direction = params::parse_direction(params::get(&pairs, "direction"))?;
    let query_params = QueryParams {
        spec: QuerySpec::Range {
            start_ns,
            end_ns,
            step_ns,
        },
        limit,
        direction,
    };

    let engine = engine_for(&state).await?;
    run_query(
        &engine,
        &expr,
        &query_params,
        wants_explain(headers),
        end_ns,
    )
    .await
}

// ---------------------------------------------------------------------
// GET|POST /api/logs/v1/query
// ---------------------------------------------------------------------

pub(crate) async fn query(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Response {
    let pairs = params::parse_pairs(raw.as_deref().unwrap_or(""));
    match query_impl(state, &headers, pairs).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

/// `POST /api/logs/v1/query`: same param names as GET, form-encoded (same
/// rationale as `query_range_post`).
pub(crate) async fn query_post(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    match read_form_pairs(&headers, body).await {
        Ok(pairs) => match query_impl(state, &headers, pairs).await {
            Ok(res) => res,
            Err(e) => e.into_response(),
        },
        Err(e) => e.into_response(),
    }
}

async fn query_impl(
    state: AppState,
    headers: &HeaderMap,
    pairs: Vec<(String, String)>,
) -> Result<Response, ApiError> {
    let query = params::get(&pairs, "query").ok_or(ParamError::MissingQuery)?;
    let expr = pulsus_logql::parse(query)?;
    let at_ns = match params::get(&pairs, "time") {
        Some(v) => params::parse_ts(v)?,
        None => params::now_ns(),
    };
    let limit = params::parse_limit(params::get(&pairs, "limit"))?;
    let direction = params::parse_direction(params::get(&pairs, "direction"))?;
    let query_params = QueryParams {
        spec: QuerySpec::Instant { at_ns },
        limit,
        direction,
    };

    let engine = engine_for(&state).await?;
    run_query(&engine, &expr, &query_params, wants_explain(headers), at_ns).await
}

/// Shared success path for `query`/`query_range`: run with or without the
/// explain side channel (single execution either way — see
/// `LogQlEngine::query_explained`'s doc comment), then encode.
async fn run_query(
    engine: &LogQlEngine,
    expr: &Expr,
    query_params: &QueryParams,
    explain: bool,
    at_ns: i64,
) -> Result<Response, ApiError> {
    if explain {
        let (result, plan_explain) = engine.query_explained(expr, query_params).await?;
        Ok(encode::query_response(result, Some(plan_explain), at_ns))
    } else {
        let result = engine.query(expr, query_params).await?;
        Ok(encode::query_response(result, None, at_ns))
    }
}

// ---------------------------------------------------------------------
// GET|POST /api/logs/v1/labels
// ---------------------------------------------------------------------

pub(crate) async fn labels_get(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Response {
    let pairs = params::parse_pairs(raw.as_deref().unwrap_or(""));
    match labels_impl(state, &headers, pairs).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

pub(crate) async fn labels_post(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    match read_form_pairs(&headers, body).await {
        Ok(pairs) => match labels_impl(state, &headers, pairs).await {
            Ok(res) => res,
            Err(e) => e.into_response(),
        },
        Err(e) => e.into_response(),
    }
}

async fn labels_impl(
    state: AppState,
    headers: &HeaderMap,
    pairs: Vec<(String, String)>,
) -> Result<Response, ApiError> {
    let (start_ns, end_ns) = parse_bounds(&pairs)?;
    let bounds = TimeBounds { start_ns, end_ns };
    let engine = engine_for(&state).await?;
    if wants_explain(headers) {
        let (names, explain) = engine.label_names_explained(bounds).await?;
        Ok(encode::string_array_response(names, Some(explain)))
    } else {
        let names = engine.label_names(bounds).await?;
        Ok(encode::string_array_response(names, None))
    }
}

// ---------------------------------------------------------------------
// GET /api/logs/v1/label/{name}/values
// ---------------------------------------------------------------------

pub(crate) async fn label_values(
    State(state): State<AppState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Response {
    let pairs = params::parse_pairs(raw.as_deref().unwrap_or(""));
    match label_values_impl(state, &name, &headers, pairs).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

async fn label_values_impl(
    state: AppState,
    name: &str,
    headers: &HeaderMap,
    pairs: Vec<(String, String)>,
) -> Result<Response, ApiError> {
    let (start_ns, end_ns) = parse_bounds(&pairs)?;
    let bounds = TimeBounds { start_ns, end_ns };
    let engine = engine_for(&state).await?;
    if wants_explain(headers) {
        let (values, explain) = engine.label_values_explained(name, bounds).await?;
        Ok(encode::string_array_response(values, Some(explain)))
    } else {
        let values = engine.label_values(name, bounds).await?;
        Ok(encode::string_array_response(values, None))
    }
}

// ---------------------------------------------------------------------
// GET|POST /api/logs/v1/series
// ---------------------------------------------------------------------

pub(crate) async fn series_get(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Response {
    let pairs = params::parse_pairs(raw.as_deref().unwrap_or(""));
    match series_impl(state, &headers, pairs).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

pub(crate) async fn series_post(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    match read_form_pairs(&headers, body).await {
        Ok(pairs) => match series_impl(state, &headers, pairs).await {
            Ok(res) => res,
            Err(e) => e.into_response(),
        },
        Err(e) => e.into_response(),
    }
}

async fn series_impl(
    state: AppState,
    headers: &HeaderMap,
    pairs: Vec<(String, String)>,
) -> Result<Response, ApiError> {
    let matches = params::get_all(&pairs, "match[]");
    if matches.is_empty() {
        return Err(ApiError::Param(ParamError::MissingMatch));
    }
    let mut selectors = Vec::with_capacity(matches.len());
    for m in matches {
        let selector = pulsus_logql::parse_selector(m)?;
        selectors.push(Expr::Log(LogExpr {
            selector,
            pipeline: Vec::new(),
        }));
    }
    let (start_ns, end_ns) = parse_bounds(&pairs)?;
    let bounds = TimeBounds { start_ns, end_ns };
    let engine = engine_for(&state).await?;
    if wants_explain(headers) {
        let (data, explain) = engine.series_explained(&selectors, bounds).await?;
        Ok(encode::json_array_response(data, Some(explain)))
    } else {
        let data = engine.series(&selectors, bounds).await?;
        Ok(encode::json_array_response(data, None))
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
    use crate::ingest::{MetricWriterSink, WriterSink};

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
            label_cache: Arc::new(std::sync::OnceLock::new()),
        }
    }

    async fn status_and_body(res: Response) -> (StatusCode, serde_json::Value) {
        let status = res.status();
        let bytes = to_bytes(res.into_body(), usize::MAX).await.expect("body");
        let json: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        (status, json)
    }

    #[tokio::test]
    async fn query_range_without_a_pool_is_503_unavailable() {
        let res = query_range(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some(r#"query={app="x"}"#.to_string())),
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }

    #[tokio::test]
    async fn query_range_missing_query_param_is_400_bad_data() {
        let res = query_range(State(test_state()), HeaderMap::new(), RawQuery(None)).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
    }

    #[tokio::test]
    async fn query_range_malformed_logql_is_400_bad_data_with_a_position() {
        let res = query_range(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some("query=%7B".to_string())), // "{" — unterminated selector
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(json.get("position").is_some());
    }

    #[tokio::test]
    async fn query_range_limit_above_the_cap_is_400_bad_data() {
        let res = query_range(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some(r#"query={app="x"}&limit=5001"#.to_string())),
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
    }

    #[tokio::test]
    async fn query_range_post_rejects_a_non_form_content_type() {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
        let res = query_range_post(State(test_state()), headers, Bytes::from_static(b"{}")).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
    }

    #[tokio::test]
    async fn query_range_post_without_a_pool_is_503_once_the_form_is_valid() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            "application/x-www-form-urlencoded".parse().unwrap(),
        );
        let body = Bytes::from_static(b"query=%7Bapp%3D%22x%22%7D");
        let res = query_range_post(State(test_state()), headers, body).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }

    #[tokio::test]
    async fn query_post_missing_query_param_is_400_bad_data() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            "application/x-www-form-urlencoded".parse().unwrap(),
        );
        let res = query_post(State(test_state()), headers, Bytes::from_static(b"")).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
    }

    #[tokio::test]
    async fn query_post_without_a_pool_is_503_once_the_form_is_valid() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            "application/x-www-form-urlencoded".parse().unwrap(),
        );
        let body = Bytes::from_static(b"query=%7Bapp%3D%22x%22%7D");
        let res = query_post(State(test_state()), headers, body).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }

    #[tokio::test]
    async fn series_without_any_match_param_is_400_bad_data() {
        let res = series_get(State(test_state()), HeaderMap::new(), RawQuery(None)).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
    }

    #[tokio::test]
    async fn series_post_rejects_a_non_form_content_type() {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
        let res = series_post(State(test_state()), headers, Bytes::from_static(b"{}")).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
    }

    #[tokio::test]
    async fn series_post_without_a_pool_is_503_once_the_form_is_valid() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            "application/x-www-form-urlencoded".parse().unwrap(),
        );
        let body = Bytes::from_static(b"match%5B%5D=%7Bapp%3D%22x%22%7D");
        let res = series_post(State(test_state()), headers, body).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }

    #[tokio::test]
    async fn label_values_without_a_pool_is_503_unavailable() {
        let res = label_values(
            State(test_state()),
            Path("env".to_string()),
            HeaderMap::new(),
            RawQuery(None),
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }

    #[tokio::test]
    async fn query_instant_missing_query_param_is_400_bad_data() {
        let res = query(State(test_state()), HeaderMap::new(), RawQuery(None)).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
    }
}
