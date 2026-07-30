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

    /// Issue #279: a syntactically valid selector of exactly 131,072
    /// bytes — `MAX_QUERY_BYTES`, the reference's `maxInputSize`
    /// (grafana/loki v3.7.4 `pkg/logql/syntax/parser.go:42`), one byte
    /// past the longest accepted query. Valid syntax so the only possible
    /// rejection is the cap itself.
    fn oversized_query() -> String {
        format!(r#"{{app="{}"}}"#, "a".repeat(131_072 - 8))
    }

    /// Issue #279: the over-cap rejection envelope — 400 `bad_data`, the
    /// reference's verbatim message (its own `len > cap` rendering of a
    /// `>=` comparison), `position` 0.
    fn assert_query_too_long(status: StatusCode, json: &serde_json::Value) {
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert_eq!(json["error"], "input size too long (131072 > 131072)");
        assert_eq!(json["position"], 0);
    }

    /// Issue #279 (AC4): `/query_range` rejects an over-cap query 400
    /// against a POOLLESS state, while a valid query on the same state is
    /// 503 — proving the parse (and so the cap) precedes the pool check,
    /// so the 400 is the cap and not an artefact.
    #[tokio::test]
    async fn query_range_rejects_an_over_cap_query_400_before_the_pool_check() {
        let res = query_range(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some(format!("query={}", oversized_query()))),
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_query_too_long(status, &json);
        // The valid-query half lives in
        // `query_range_without_a_pool_is_503_unavailable` above.
    }

    /// Issue #279 (AC4): `/query` — over-cap 400 vs valid 503, poolless.
    #[tokio::test]
    async fn query_rejects_an_over_cap_query_400_before_the_pool_check() {
        let res = query(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some(format!("query={}", oversized_query()))),
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_query_too_long(status, &json);

        let res = query(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some(r#"query={app="x"}"#.to_string())),
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }

    /// Issue #279 (AC4): `/series` — an over-cap `match[]` value is 400
    /// (the cap applies per matcher string, matching the reference's
    /// one-input-at-a-time `ParseMatchers`) vs valid 503, poolless.
    #[tokio::test]
    async fn series_rejects_an_over_cap_match_value_400_before_the_pool_check() {
        let res = series_get(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some(format!("match[]={}", oversized_query()))),
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_query_too_long(status, &json);

        let res = series_get(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some(r#"match[]={app="x"}"#.to_string())),
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
        // 11_001 step INTERVALS over `[0, 11001s]` at a 1s step > the 11000
        // cap (issue #227 review round 7, finding 1: exactly 11_000
        // intervals — 11_001 grid points — is SERVED, matching the
        // reference's `(end-start)/step > 11000` rule).
        let window = pulsus_read::logql::GridWindow {
            start_ns: 0,
            end_ns: 11_001 * S,
            step_ns: Some(
                pulsus_read::logql::validate_duration_ns(S as u64, "step").expect("valid step"),
            ),
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

    /// Issue #221 (AC 9/10): `QueryTooBroad(VariantSubStates)` maps to the
    /// same 422 `query_too_broad` surface as every other too-broad LogQL
    /// query, via the real `ReadError` → `ApiError` → response path
    /// `query_range` uses.
    ///
    /// **Issue #236 changed how this is driven, not what it asserts.**
    /// The test used to build a 501-variant query, because the DERIVED
    /// backstop was then 500. #236 deleted `AggCaps::series`, moving
    /// `min_field()` onto `MAX_TS_COLLISION_GROUP` = 10 000 — and at
    /// #279's `MAX_QUERY_BYTES` the largest expressible variants query
    /// carries 4 368 variants, so the backstop can no longer be reached
    /// through a parse. The reason is asserted here by CONSTRUCTION
    /// instead, which is what this test was ever really about (the
    /// mapping, not the guard); the guard's own arithmetic and its
    /// unreachability verdict are pinned in
    /// `plan::tests::variants_past_the_derived_backstop_reject_at_plan_time`.
    #[tokio::test]
    async fn variants_past_the_sub_state_backstop_maps_to_422_query_too_broad() {
        let err = pulsus_read::logql::ReadError::QueryTooBroad(
            pulsus_read::logql::TooBroadReason::VariantSubStates {
                count: 10_001,
                cap: 10_000,
            },
        );
        let (status, json) = status_and_body(ApiError::Read(err).into_response()).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json["errorType"], "query_too_broad");
        assert!(
            json["error"]
                .as_str()
                .unwrap_or_default()
                .contains("10001 variants, exceeding the 10000-variant cap"),
            "the message names the derived cap: {json:?}"
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

    /// Issue #227 review round 7, finding 1: EXACTLY `(end-start)/step ==
    /// 11000` is inside the reference's fence (`> 11000` rejects), so the
    /// request guard must admit it — the request proceeds past parameter
    /// parsing (hermetically that surfaces as the engine-pool 503, never
    /// the resolution 400).
    #[tokio::test]
    async fn query_range_at_exactly_the_11000_resolution_passes_the_request_guard() {
        // (end - start) / step == 11000 exactly (0 → 11000s at a 1s step).
        let q = "query=count_over_time(%7Bapp%3D%22a%22%7D%5B5m%5D)\
                 &start=0&end=11000000000000&step=1s";
        let res = query_range(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some(q.to_string())),
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "an exactly-at-the-limit request must pass the resolution guard \
             (got {json:?})"
        );
    }

    /// Issue #227 review round 8 (end-to-end): a full-i64-domain request at
    /// a 1_000_000s step must pass the resolution guard — the reference's
    /// span subtraction saturates at the int64-ns duration bound (Go
    /// `time.Time.Sub`), so it counts 9_223 intervals, not the true span's
    /// 18_446 — and proceed past parameter parsing (hermetically the
    /// engine-pool 503, never the resolution 400 the pre-round-8 exact
    /// fence returned).
    #[tokio::test]
    async fn query_range_across_the_full_timestamp_domain_passes_the_request_guard() {
        let q = format!(
            "query=count_over_time(%7Bapp%3D%22a%22%7D%5B5m%5D)\
             &start={}&end={}&step=1000000s",
            i64::MIN,
            i64::MAX
        );
        let res = query_range(State(test_state()), HeaderMap::new(), RawQuery(Some(q))).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "a saturated-span request the reference serves must pass the \
             resolution guard (got {json:?})"
        );
    }

    /// Issue #227 review round 10 (the reported case, verbatim):
    /// `query=vector(1)&start=0&end=0&step=3153600000s` — a 100-year step —
    /// fits the reference's positive `time.Duration` and passes its
    /// resolution fence, so the reference serves it. The request must clear
    /// parameter parsing and the resolution guard (hermetically the
    /// engine-pool 503, never a 400); the engine leg of the same case —
    /// validator + grid guard — is pinned by the agreement table above and
    /// the read-crate's `a_100_year_step_the_reference_serves_is_served`.
    #[tokio::test]
    async fn query_range_with_a_100_year_step_passes_the_request_guard() {
        let q = "query=vector(1)&start=0&end=0&step=3153600000s";
        let res = query_range(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some(q.to_string())),
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "a 100-year step the reference serves must pass request parsing \
             (got {json:?})"
        );
    }

    /// Issue #227 review round 7, finding 1 (extended in round 8 to the
    /// extreme timestamp domain): the HTTP request guard
    /// (`ensure_range_resolution`) and the engine's grid guard
    /// (`ensure_grid_resolution`, driven here through the public
    /// `materialize_vector_lit` — the SAME function `RangeSlideState::new`
    /// funnels through) implement the identical Loki fence — including its
    /// SATURATING span subtraction (Go `time.Time.Sub` clamps at
    /// `maxDuration = 1<<63-1` ns) — so the engine can never 422 a request
    /// the guard admitted, across the WHOLE i64 domain. Enumerates both
    /// sides of the fence: aligned, unaligned, and saturated.
    #[tokio::test]
    async fn engine_grid_guard_agrees_with_the_request_guard_at_every_fence_case() {
        const S: i64 = 1_000_000_000; // 1s step
        const BIG: u64 = 1_000_000_000_000_000; // 1_000_000s step
        let step = S as u64;
        // floor(i64::MAX/11_001): the largest step whose SATURATED
        // full-domain span counts 11_001 intervals (reject); +1ns admits.
        let sat_reject = (i64::MAX / 11_001) as u64;
        // (start, end, step, expect-admitted)
        let cases: &[(i64, i64, u64, bool)] = &[
            (0, 10_999 * S, step, true),         // one interval under
            (0, 11_000 * S, step, true),         // exactly at the fence
            (0, 11_001 * S, step, false),        // one interval over
            (0, 11_000 * S + S / 2, step, true), // step does not divide the span
            (0, 11_001 * S - 1, step, true),     // 1ns under the rejecting interval
            (7, 7 + 11_000 * S, step, true),     // unaligned start, at the fence
            (7, 7 + 11_001 * S, step, false),    // unaligned start, over
            // Round 8: the saturated extreme — the true 2^64-1 ns span
            // counts 18_446 intervals, but the reference's saturated span
            // (i64::MAX ns) counts 9_223 → SERVED.
            (i64::MIN, i64::MAX, BIG, true),
            (i64::MIN, i64::MAX, sat_reject, false), // 11_001 saturated
            (i64::MIN, i64::MAX, sat_reject + 1, true), // 11_000 saturated
            (i64::MIN, i64::MAX, 1, false),          // 1ns step, far over
            (-1, i64::MAX - 1, BIG, true),           // saturation onset, exact
            // Round 10: the widened duration domain — the reference accepts
            // ANY positive int64-ns step. A 100-year step (the reported
            // reject-a-served-request case) and the largest representable
            // step must both pass BOTH guards (the retired `i64::MAX / 4`
            // validator cap panicked this table's engine leg on them).
            (0, 0, 3_153_600_000_000_000_000, true), // 100y (3153600000s)
            (0, 0, i64::MAX as u64, true),           // Go's maximum Duration
            (i64::MIN, i64::MAX, i64::MAX as u64, true), // full domain, max step
        ];
        for &(start_ns, end_ns, step_ns, admitted) in cases {
            let guard_ok = params::ensure_range_resolution(start_ns, end_ns, step_ns).is_ok();
            assert_eq!(
                guard_ok, admitted,
                "request guard disagrees with the reference at \
                 ({start_ns}, {end_ns}, {step_ns})"
            );
            let window = pulsus_read::logql::GridWindow {
                start_ns,
                end_ns,
                step_ns: Some(
                    pulsus_read::logql::validate_duration_ns(step_ns, "step").expect("valid step"),
                ),
            };
            let engine_ok = pulsus_read::logql::materialize_vector_lit(0.0, &window).is_ok();
            assert_eq!(
                engine_ok, guard_ok,
                "engine grid guard disagrees with the request guard at \
                 ({start_ns}, {end_ns}, {step_ns})"
            );
        }
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
