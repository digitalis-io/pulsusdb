//! The `/api/v1/*` handlers (docs/api.md §3): parse params → parse PromQL
//! (`pulsus-promql`) → dispatch to `MetricsEngine` (`pulsus-read`) → encode
//! the envelope (`encode.rs`). Thin by design — all planning/SQL/execution
//! stays in `pulsus-read`/`pulsus-promql` (issue #32 architect plan,
//! mirroring `logs_api::handlers`'s own contract).

use axum::body::Bytes;
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, header};
use axum::response::{IntoResponse, Response};

use pulsus_promql::parser::Expr;
use pulsus_read::{DataWindow, DiscoveryFilter, MetricQueryParams, MetricsEngine};

use crate::app::AppState;
use crate::chconfig;

use super::encode;
use super::error::ApiError;
use super::params::{self, ParamError};

/// `X-Pulsus-Explain: 1` (docs/api.md "Request headers"). Emitted at
/// `data.explain` on `query`/`query_range` only (architect plan amendment
/// — the discovery/status endpoints never carry it).
fn wants_explain(headers: &HeaderMap) -> bool {
    headers
        .get("x-pulsus-explain")
        .and_then(|v| v.to_str().ok())
        == Some("1")
}

/// Acquires the shared `Arc<ChPool>` and the constructed `Arc<LabelCache>`
/// from `AppState` (mirrors `logs_api::handlers::engine_for`'s pattern:
/// clone each `Option`/`OnceLock` slot out, never hold a lock across an
/// `.await`) and builds a `MetricsEngine` over both — `503 unavailable`
/// before either exists, matching `/ready`.
async fn engine_for(state: &AppState) -> Result<MetricsEngine, ApiError> {
    let pool = {
        let guard = state.pool.read().await;
        guard.clone()
    };
    let pool = pool.ok_or(ApiError::Unavailable)?;
    let label_cache = state
        .label_cache
        .get()
        .cloned()
        .ok_or(ApiError::Unavailable)?;
    // Issue #114: the consistency-config invariant is already enforced at
    // config load, so this is unreachable in the real binary; a failure maps
    // to the existing 503 "not serving" semantics.
    chconfig::metrics_engine(pool, label_cache, &state.config, state.eval_gate.clone())
        .map_err(|_| ApiError::Unavailable)
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

/// Parses `start`/`end` for the discovery endpoints (defaults: `end =
/// now`, `start = end - 1h` — see `params::default_start_ms`).
fn parse_bounds(pairs: &[(String, String)]) -> Result<(i64, i64), ParamError> {
    let now = params::now_ms();
    let end_ms = match params::get(pairs, "end") {
        Some(v) => params::parse_time(v)?,
        None => now,
    };
    let start_ms = match params::get(pairs, "start") {
        Some(v) => params::parse_time(v)?,
        None => params::default_start_ms(end_ms),
    };
    Ok((start_ms, end_ms))
}

/// Parses every `match[]` value into a [`DiscoveryFilter`] (issue #32,
/// code-review round-1 fix): `pulsus_promql::parse` ->
/// `pulsus_promql::series_selector` — **not** `pulsus_promql::plan`, whose
/// #31 structural contract requires a concrete metric name and therefore
/// rejects Prometheus's own matcher-only `match[]` selectors (e.g.
/// `{job="api"}`). `series_selector` permits a missing metric name
/// (`DiscoveryFilter::metric_name = None`, routing to
/// `discovery_query`'s already-existing unscoped branch — see that
/// function's own doc comment) and, since issue #89, extracts a
/// `__name__` regex/negative matcher into `DiscoveryFilter::name_matchers`
/// rather than rejecting it: the outcome is now the read path's to decide
/// (cache-resolved flat IN×IN fetch; a degraded cache is a named `422`).
/// A non-vector-selector `match[]` value (e.g. `sum(up)`) remains
/// `PromqlError::Unsupported` -> `422 execution`.
fn parse_match_selectors(raw_matches: &[&str]) -> Result<Vec<DiscoveryFilter>, ApiError> {
    let mut filters = Vec::with_capacity(raw_matches.len());
    for raw in raw_matches {
        let expr = pulsus_promql::parse(raw)?;
        let (metric_name, name_matchers, matchers) = pulsus_promql::series_selector(&expr)?;
        filters.push(DiscoveryFilter {
            metric_name,
            name_matchers,
            matchers,
        });
    }
    Ok(filters)
}

// ---------------------------------------------------------------------
// GET|POST /api/v1/query
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
    let expr = pulsus_promql::parse(query)?;
    let at_ms = match params::get(&pairs, "time") {
        Some(v) => params::parse_time(v)?,
        None => params::now_ms(),
    };
    let query_params = MetricQueryParams {
        start_ms: at_ms,
        end_ms: at_ms,
        step_ms: 0,
    };
    let engine = engine_for(&state).await?;
    run_query(
        &engine,
        &expr,
        query,
        &query_params,
        wants_explain(headers),
        at_ms,
    )
    .await
}

// ---------------------------------------------------------------------
// GET|POST /api/v1/query_range
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
    let expr = pulsus_promql::parse(query)?;
    let start_ms =
        params::parse_time(params::get(&pairs, "start").ok_or(ParamError::MissingParam("start"))?)?;
    let end_ms =
        params::parse_time(params::get(&pairs, "end").ok_or(ParamError::MissingParam("end"))?)?;
    let step_ms =
        params::parse_step(params::get(&pairs, "step").ok_or(ParamError::MissingParam("step"))?)?;
    // Checked before any engine/ClickHouse call (architect plan AC).
    params::check_range(start_ms, end_ms, step_ms)?;
    let query_params = MetricQueryParams {
        start_ms,
        end_ms,
        step_ms,
    };
    let engine = engine_for(&state).await?;
    run_query(
        &engine,
        &expr,
        query,
        &query_params,
        wants_explain(headers),
        end_ms,
    )
    .await
}

/// Shared success path for `query`/`query_range`: run with or without the
/// explain side channel (single execution either way), then encode.
/// `query` is the raw query text the caller parsed `expr` from — the
/// annotations' position offsets index into exactly this string (issue
/// #128, upstream `AsStrings(r.FormValue("query"), 10, 10)`).
async fn run_query(
    engine: &MetricsEngine,
    expr: &Expr,
    query: &str,
    query_params: &MetricQueryParams,
    explain: bool,
    at_ms: i64,
) -> Result<Response, ApiError> {
    // Issue #68 (M6-05): a sort-rooted INSTANT query's wire order is the
    // evaluator's own (that ordering is the function's whole point);
    // everything else — every non-sort query, and every range query
    // (upstream's own "sort is ineffective for range queries") — keeps
    // the encoder's deterministic label sort.
    let ordered = query_params.step_ms == 0 && pulsus_promql::expr_is_sort_root(expr);
    if explain {
        let (result, annotations, plan_explain) =
            engine.query_explained(expr, query_params).await?;
        Ok(encode::query_response_annotated(
            result,
            Some(plan_explain),
            at_ms,
            ordered,
            query,
            &annotations,
        ))
    } else {
        let (result, annotations) = engine.query(expr, query_params).await?;
        Ok(encode::query_response_annotated(
            result,
            None,
            at_ms,
            ordered,
            query,
            &annotations,
        ))
    }
}

// ---------------------------------------------------------------------
// GET|POST /api/v1/labels
// ---------------------------------------------------------------------

pub(crate) async fn labels(State(state): State<AppState>, RawQuery(raw): RawQuery) -> Response {
    let pairs = params::parse_pairs(raw.as_deref().unwrap_or(""));
    match labels_impl(state, pairs).await {
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
        Ok(pairs) => match labels_impl(state, pairs).await {
            Ok(res) => res,
            Err(e) => e.into_response(),
        },
        Err(e) => e.into_response(),
    }
}

async fn labels_impl(state: AppState, pairs: Vec<(String, String)>) -> Result<Response, ApiError> {
    let (start_ms, end_ms) = parse_bounds(&pairs)?;
    let window = DataWindow { start_ms, end_ms };
    let matches = params::get_all(&pairs, "match[]");
    let filters = parse_match_selectors(&matches)?;
    let engine = engine_for(&state).await?;
    let names = engine.label_names(&filters, window).await?;
    Ok(encode::string_array_response(names))
}

// ---------------------------------------------------------------------
// GET /api/v1/label/{name}/values
// ---------------------------------------------------------------------

pub(crate) async fn label_values(
    State(state): State<AppState>,
    Path(name): Path<String>,
    RawQuery(raw): RawQuery,
) -> Response {
    let pairs = params::parse_pairs(raw.as_deref().unwrap_or(""));
    match label_values_impl(state, &name, pairs).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

async fn label_values_impl(
    state: AppState,
    name: &str,
    pairs: Vec<(String, String)>,
) -> Result<Response, ApiError> {
    let (start_ms, end_ms) = parse_bounds(&pairs)?;
    let window = DataWindow { start_ms, end_ms };
    let matches = params::get_all(&pairs, "match[]");
    let filters = parse_match_selectors(&matches)?;
    let engine = engine_for(&state).await?;
    let values = engine.label_values(name, &filters, window).await?;
    Ok(encode::string_array_response(values))
}

// ---------------------------------------------------------------------
// GET|POST /api/v1/series
// ---------------------------------------------------------------------

pub(crate) async fn series(State(state): State<AppState>, RawQuery(raw): RawQuery) -> Response {
    let pairs = params::parse_pairs(raw.as_deref().unwrap_or(""));
    match series_impl(state, pairs).await {
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
        Ok(pairs) => match series_impl(state, pairs).await {
            Ok(res) => res,
            Err(e) => e.into_response(),
        },
        Err(e) => e.into_response(),
    }
}

async fn series_impl(state: AppState, pairs: Vec<(String, String)>) -> Result<Response, ApiError> {
    let matches = params::get_all(&pairs, "match[]");
    if matches.is_empty() {
        return Err(ApiError::Param(ParamError::MissingMatch));
    }
    let (start_ms, end_ms) = parse_bounds(&pairs)?;
    let window = DataWindow { start_ms, end_ms };
    let filters = parse_match_selectors(&matches)?;
    let engine = engine_for(&state).await?;
    let data = engine.series(&filters, window).await?;
    Ok(encode::series_response(data))
}

// ---------------------------------------------------------------------
// GET /api/v1/metadata
// ---------------------------------------------------------------------

pub(crate) async fn metadata(State(state): State<AppState>, RawQuery(raw): RawQuery) -> Response {
    match metadata_impl(state, params::parse_pairs(raw.as_deref().unwrap_or(""))).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

async fn metadata_impl(
    state: AppState,
    pairs: Vec<(String, String)>,
) -> Result<Response, ApiError> {
    let metric = params::metric(&pairs);
    let limit = params::parse_limit(params::get(&pairs, "limit"))?;
    let engine = engine_for(&state).await?;
    let items = engine.metadata(metric, limit).await?;
    Ok(encode::metadata_response(items))
}

// ---------------------------------------------------------------------
// GET|POST /api/v1/query_exemplars (empty-success stub, issue #32 scope)
// ---------------------------------------------------------------------

pub(crate) async fn query_exemplars() -> Response {
    encode::query_exemplars_response()
}

pub(crate) async fn query_exemplars_post() -> Response {
    encode::query_exemplars_response()
}

// ---------------------------------------------------------------------
// GET /api/v1/status/*
// ---------------------------------------------------------------------

pub(crate) async fn status_buildinfo(State(state): State<AppState>) -> Response {
    encode::status_buildinfo_response(&state.build)
}

pub(crate) async fn status_config(State(state): State<AppState>) -> Response {
    match state.config.to_redacted_yaml() {
        Ok(yaml) => encode::status_config_response(&yaml),
        Err(err) => {
            tracing::error!(error = %err, "failed to render redacted config for status/config");
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "failed to render configuration",
            )
                .into_response()
        }
    }
}

pub(crate) async fn status_flags() -> Response {
    encode::status_flags_response()
}

pub(crate) async fn status_runtimeinfo(State(state): State<AppState>) -> Response {
    let start_time = chrono::DateTime::<chrono::Utc>::from(state.started_at).to_rfc3339();
    encode::status_runtimeinfo_response(start_time, state.config.retention_days)
}

pub(crate) async fn status_tsdb(State(state): State<AppState>) -> Response {
    match status_tsdb_impl(state).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

async fn status_tsdb_impl(state: AppState) -> Result<Response, ApiError> {
    let engine = engine_for(&state).await?;
    let status = engine.tsdb_status().await?;
    Ok(encode::status_tsdb_response(status))
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
    async fn query_without_a_pool_is_503_unavailable() {
        let res = query(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some("query=up".to_string())),
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }

    #[tokio::test]
    async fn query_missing_query_param_is_400_bad_data() {
        let res = query(State(test_state()), HeaderMap::new(), RawQuery(None)).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
    }

    #[tokio::test]
    async fn query_malformed_promql_is_400_bad_data() {
        let res = query(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some("query=up{".to_string())),
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        // Envelope has exactly 3 fields — no `position`.
        assert_eq!(json.as_object().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn query_post_rejects_a_non_form_content_type() {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
        let res = query_post(State(test_state()), headers, Bytes::from_static(b"{}")).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
    }

    #[tokio::test]
    async fn query_range_missing_start_is_400_bad_data() {
        let res = query_range(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some("query=up&end=100&step=10".to_string())),
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
    }

    #[tokio::test]
    async fn query_range_rejects_more_than_11000_points_before_any_pool_check() {
        // No pool established at all — if this were 503 instead of 400, the
        // 11k cap would not be "checked before any engine/DB call".
        let res = query_range(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some("query=up&start=0&end=11000&step=1".to_string())),
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(json["error"].as_str().unwrap().contains("11000"));
    }

    #[tokio::test]
    async fn query_range_at_exactly_the_cap_passes_param_validation() {
        // 11,000 points exactly: (10999-0)/1 + 1 == 11000, must pass
        // `check_range` and reach the (missing) pool -> 503, not 400.
        let res = query_range(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some("query=up&start=0&end=10999&step=1".to_string())),
        )
        .await;
        let (status, _) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn query_range_end_before_start_is_400_bad_data() {
        let res = query_range(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some("query=up&start=1000&end=0&step=1".to_string())),
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
    }

    /// Code-review round-1 fix: extreme (but individually parseable)
    /// `start`/`end` timestamps must resolve to a clean `400`, never a
    /// panic from `check_range`'s internal arithmetic.
    #[tokio::test]
    async fn query_range_with_extreme_timestamps_is_400_not_a_panic() {
        let res = query_range(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some("query=up&start=-1e300&end=1e300&step=1".to_string())),
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
    }

    #[tokio::test]
    async fn labels_without_a_pool_is_503_unavailable() {
        let res = labels(State(test_state()), RawQuery(None)).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }

    #[tokio::test]
    async fn labels_with_no_match_params_still_reaches_the_pool_check() {
        // Optional match[] (Prometheus's own "no filter" contract) must not
        // be rejected as a param error.
        let res = labels(State(test_state()), RawQuery(None)).await;
        let (status, _) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    /// Code-review round-1 fix: `{job="x"}` is a valid Prometheus `match[]`
    /// selector (matcher-only, no concrete metric name) — must reach the
    /// pool check (503), never `422` from the PromQL planner's stricter
    /// query-path contract.
    #[tokio::test]
    async fn labels_with_a_matcher_only_selector_reaches_the_pool_check() {
        let res = labels(
            State(test_state()),
            RawQuery(Some(r#"match[]=%7Bjob%3D%22x%22%7D"#.to_string())),
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }

    #[tokio::test]
    async fn label_values_with_a_matcher_only_selector_reaches_the_pool_check() {
        let res = label_values(
            State(test_state()),
            Path("job".to_string()),
            RawQuery(Some(r#"match[]=%7Bjob%3D%22x%22%7D"#.to_string())),
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }

    #[tokio::test]
    async fn series_with_a_matcher_only_selector_reaches_the_pool_check() {
        let res = series(
            State(test_state()),
            RawQuery(Some(r#"match[]=%7Bjob%3D%22x%22%7D"#.to_string())),
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }

    /// Issue #89: a `__name__` regex matcher in `match[]` is no longer a
    /// parse-time rejection — it reaches the read path (here: the pool
    /// check) carrying its name matchers, exactly as the matcher-only
    /// selector above does.
    #[tokio::test]
    async fn labels_with_a_name_regex_matcher_reaches_the_pool_check() {
        let res = labels(
            State(test_state()),
            RawQuery(Some(
                r#"match[]=%7B__name__%3D~%22up.%2A%22%7D"#.to_string(),
            )),
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }

    /// A non-vector-selector `match[]` value stays `422 execution` — the
    /// remaining, deterministic `series_selector` rejection (the
    /// conformance manifest's retargeted `unsupported-selector` pin).
    #[tokio::test]
    async fn labels_with_a_non_selector_match_is_422_execution() {
        let res = labels(
            State(test_state()),
            RawQuery(Some(r#"match[]=sum(up)"#.to_string())),
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json["errorType"], "execution");
    }

    /// AC2: the parse layer populates the `name_matchers` channel.
    #[test]
    fn parse_match_selectors_extracts_a_name_regex_into_the_name_channel() {
        let filters = parse_match_selectors(&[r#"{__name__=~"up.*",job="api"}"#]).unwrap();
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].metric_name, None);
        assert_eq!(filters[0].name_matchers.len(), 1);
        assert_eq!(filters[0].name_matchers[0].key, "__name__");
        assert_eq!(filters[0].matchers.len(), 1);
        assert_eq!(filters[0].matchers[0].key, "job");
    }

    /// The common cases keep an empty name channel (no fan-out routing).
    #[test]
    fn parse_match_selectors_leaves_the_name_channel_empty_for_ordinary_selectors() {
        let filters = parse_match_selectors(&[r#"up{job="api"}"#, r#"{job="api"}"#]).unwrap();
        assert_eq!(filters[0].metric_name, Some("up".to_string()));
        assert!(filters[0].name_matchers.is_empty());
        assert_eq!(filters[1].metric_name, None);
        assert!(filters[1].name_matchers.is_empty());
    }

    #[tokio::test]
    async fn label_values_without_a_pool_is_503_unavailable() {
        let res = label_values(State(test_state()), Path("job".to_string()), RawQuery(None)).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }

    #[tokio::test]
    async fn series_without_any_match_param_is_400_bad_data() {
        let res = series(State(test_state()), RawQuery(None)).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
    }

    #[tokio::test]
    async fn series_with_a_match_param_reaches_the_pool_check() {
        let res = series(
            State(test_state()),
            RawQuery(Some("match[]=up".to_string())),
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
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
    async fn metadata_without_a_pool_is_503_unavailable() {
        let res = metadata(State(test_state()), RawQuery(None)).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }

    #[tokio::test]
    async fn metadata_invalid_limit_is_400_bad_data() {
        let res = metadata(State(test_state()), RawQuery(Some("limit=abc".to_string()))).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
    }

    #[tokio::test]
    async fn query_exemplars_is_an_empty_success_with_no_engine_call() {
        // No pool established at all — a stub that reached the engine would
        // 503 instead.
        let res = query_exemplars().await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["status"], "success");
        assert_eq!(json["data"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn query_exemplars_post_is_an_empty_success() {
        let res = query_exemplars_post().await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["data"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn status_buildinfo_has_the_documented_fields() {
        let res = status_buildinfo(State(test_state())).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::OK);
        for field in [
            "version",
            "revision",
            "branch",
            "buildUser",
            "buildDate",
            "goVersion",
        ] {
            assert!(
                json["data"].get(field).is_some(),
                "missing field {field:?} in {json}"
            );
        }
    }

    #[tokio::test]
    async fn status_config_redacts_the_password() {
        let mut cfg = Config::default();
        cfg.clickhouse.auth.password = pulsus_config::Secret::new("s3cret");
        let state = AppState {
            config: Arc::new(cfg),
            ..test_state()
        };
        let res = status_config(State(state)).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::OK);
        assert!(!json["data"]["yaml"].as_str().unwrap().contains("s3cret"));
    }

    #[tokio::test]
    async fn status_flags_is_ok_with_no_pool_needed() {
        let res = status_flags().await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["data"], serde_json::json!({}));
    }

    #[tokio::test]
    async fn status_runtimeinfo_reports_retention_and_a_parseable_start_time() {
        let res = status_runtimeinfo(State(test_state())).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["data"]["storageRetention"], "7d");
        let start_time = json["data"]["startTime"].as_str().unwrap();
        assert!(chrono::DateTime::parse_from_rfc3339(start_time).is_ok());
    }

    #[tokio::test]
    async fn status_tsdb_without_a_pool_is_503_unavailable() {
        let res = status_tsdb(State(test_state())).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }
}
