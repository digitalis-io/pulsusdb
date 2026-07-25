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
pub(super) async fn engine_for(state: &AppState) -> Result<LogQlEngine, ApiError> {
    let pool = {
        let guard = state.pool.read().await;
        guard.clone()
    };
    let pool = pool.ok_or(ApiError::PoolUnavailable)?;
    // Issue #114: the consistency-config invariant is already enforced at
    // config load, so this is unreachable in the real binary; a failure maps
    // to the existing 503 "not serving" semantics.
    chconfig::logql_engine(pool, &state.config).map_err(|_| ApiError::PoolUnavailable)
}

/// Parses `start`/`end` (defaults: `end = now`, `start = end - 1h`,
/// docs/api.md §2.1).
pub(super) fn parse_bounds(pairs: &[(String, String)]) -> Result<(i64, i64), ParamError> {
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

/// `pub(super)` (issue #170): the detected_labels/detected_fields POST
/// handlers (`detected.rs`) reuse the same form-decode core.
pub(super) async fn read_form_pairs(
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
    // Issue #227: Loki's `(end-start)/step > 11000` resolution limit — a hard
    // 400 at request parsing with Loki's exact message (the engine keeps its
    // `MetricBuckets` guard as a defense-in-depth backstop).
    params::ensure_range_resolution(start_ns, end_ns, step_ns)?;
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
    // Preserve the engine's value order on the wire only for a terminal
    // sort/sort_desc INSTANT query (mirrors the PromQL `step_ms == 0 &&
    // expr_is_sort_root` gate). A range sort yields a matrix and keeps the
    // deterministic label-sort.
    let preserve_vector_order = matches!(query_params.spec, QuerySpec::Instant { .. })
        && pulsus_read::logql::terminal_sort(expr);
    if explain {
        let (result, plan_explain) = engine.query_explained(expr, query_params).await?;
        Ok(encode::query_response(
            result,
            Some(plan_explain),
            at_ns,
            preserve_vector_order,
        ))
    } else {
        let result = engine.query(expr, query_params).await?;
        Ok(encode::query_response(
            result,
            None,
            at_ns,
            preserve_vector_order,
        ))
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

    /// Issue #221: an over-cap **leafless** `vector(n)` range query is guarded
    /// at the engine's materialization boundary (no leaf ever runs, so it
    /// bypasses `ClientAggState::new`'s cap) and returns the same
    /// `QueryTooBroad(MetricBuckets)` a leaf over-cap range query trips —
    /// cap-checked BEFORE allocating any grid, with no DB round-trip. This
    /// drives the REAL over-cap-vector error (`materialize_vector_lit`, the
    /// exact function `run_metric_node` calls for a `VectorLit`) through the
    /// handler's `ReadError` → response conversion (the same
    /// `From<ReadError> for ApiError` + `IntoResponse` path `query_range` uses
    /// via `?`), asserting it surfaces end-to-end as **422 query_too_broad**
    /// with the 11000-bucket-cap message — identical to any other over-cap
    /// LogQL range query.
    ///
    /// Driving the `query_range` HTTP entrypoint itself to the engine requires
    /// a live `ChPool` (there is no hermetic `ChPool`/`ChClient`/`LogQlEngine`
    /// constructor — every path pings a real endpoint), and the LogQL handler
    /// deliberately has NO param-layer cap pre-check (plan v4/v5: it would
    /// diverge the leaf error semantics 422→400). So the hermetic ceiling is
    /// this composed proof: the real leafless-vector error + the handler's
    /// real error-mapping. The window matches the `QuerySpec::Range` →
    /// evaluation-window mapping `query_range_impl` builds (`start_ns`/
    /// `end_ns`/`step_ns`), so it is the exact error an over-cap
    /// `query_range` would carry.
    #[tokio::test]
    async fn over_cap_leafless_vector_range_maps_to_422_query_too_broad() {
        const S: i64 = 1_000_000_000; // 1s
        // 11_001 buckets over `(0, 11000s]` at a 1s step > the 11000 cap.
        let window = pulsus_read::logql::ClientWindow {
            start_ns: 0,
            end_ns: 11_000 * S,
            step_ns: Some(S as u64),
            range_ns: 0,
        };
        let err = pulsus_read::logql::materialize_vector_lit(0.0, &window)
            .expect_err("an over-cap vector(n) range query must reject");
        let (status, json) = status_and_body(ApiError::Read(err).into_response()).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json["errorType"], "query_too_broad");
        assert!(
            json["error"].as_str().unwrap_or_default().contains("11000"),
            "over-cap message must name the 11000-bucket cap: {json:?}"
        );
    }

    /// Issue #227: at the REQUEST boundary, `(end-start)/step > 11000` is
    /// Loki's HTTP **400** with its exact message (the engine's
    /// `MetricBuckets` 422 above is now only a defense-in-depth backstop).
    #[tokio::test]
    async fn query_range_over_the_11000_resolution_is_400_with_lokis_message() {
        // (0, 11001s] at a 1s step = 11001 intervals > 11000.
        let q = "query=count_over_time(%7Bapp%3D%22a%22%7D%5B5m%5D)\
                 &start=0&end=11001000000000&step=1s";
        let res = query_range(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some(q.to_string())),
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert_eq!(
            json["error"],
            "exceeded maximum resolution of 11,000 points per time series. Try increasing the \
             value of the step parameter"
        );
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
