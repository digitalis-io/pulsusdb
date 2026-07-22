//! `GET|POST /api/logs/v1/{detected_labels,detected_fields}` (issue #170,
//! docs/api.md §2.6): the drilldown field/label discovery endpoints,
//! semantics pinned against the repo's interop reference.
//!
//! - **detected_labels** reads ONLY the stream index (`log_streams_idx`)
//!   via one server-side aggregation — never `log_samples`. `query=` is
//!   optional and **matchers only** (`parse_selector`; a pipeline in
//!   `query` is a 400 parse error with `position`).
//! - **detected_fields** samples <= `line_limit` **post-pipeline
//!   matching** entries (structured metadata + pipeline extractions +
//!   json/logfmt auto-detection); `query` is required and accepts the
//!   full log-selector grammar including pipelines; metric queries are
//!   400. Budget-truncated sampling is signaled by the additive
//!   `pulsus_partial: true` response key (omitted when false).
//!
//! Both are `GET|POST` form-encoded (the house `/labels`/`/series`
//! precedent — a documented deviation from api.md's earlier GET-only
//! sketch, ratified on the issue); all validation runs BEFORE pool
//! acquisition (the stats precedent). `step`/`since` are accepted and
//! ignored (documented).

use axum::body::Bytes;
use axum::extract::{RawQuery, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};

use pulsus_logql::{Expr, LogExpr};
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

// ---------------------------------------------------------------------
// GET|POST /api/logs/v1/detected_labels
// ---------------------------------------------------------------------

pub(crate) async fn detected_labels(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Response {
    let pairs = params::parse_pairs(raw.as_deref().unwrap_or(""));
    match detected_labels_impl(state, &headers, pairs).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

pub(crate) async fn detected_labels_post(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    match read_form_pairs(&headers, body).await {
        Ok(pairs) => match detected_labels_impl(state, &headers, pairs).await {
            Ok(res) => res,
            Err(e) => e.into_response(),
        },
        Err(e) => e.into_response(),
    }
}

async fn detected_labels_impl(
    state: AppState,
    headers: &HeaderMap,
    pairs: Vec<(String, String)>,
) -> Result<Response, ApiError> {
    // `query` is optional, matchers only (the reference's
    // `syntax.ParseMatchers`): absent OR empty = the unscoped form
    // (matching the reference's empty-string handling); anything else
    // must parse as a bare selector — a pipeline is a parse error with
    // `position`, BEFORE any pool work.
    let selector: Option<Expr> = match params::get(&pairs, "query") {
        None | Some("") => None,
        Some(q) => {
            let selector = pulsus_logql::parse_selector(q)?;
            Some(Expr::Log(LogExpr {
                selector,
                pipeline: Vec::new(),
            }))
        }
    };
    let (start_ns, end_ns) = parse_bounds(&pairs)?;
    let bounds = TimeBounds { start_ns, end_ns };

    let engine = engine_for(&state).await?;
    if wants_explain(headers) {
        let (labels, explain) = engine
            .detected_labels_explained(selector.as_ref(), bounds)
            .await?;
        Ok(encode::detected_labels_response(labels, Some(explain)))
    } else {
        let labels = engine.detected_labels(selector.as_ref(), bounds).await?;
        Ok(encode::detected_labels_response(labels, None))
    }
}

// ---------------------------------------------------------------------
// GET|POST /api/logs/v1/detected_fields
// ---------------------------------------------------------------------

pub(crate) async fn detected_fields(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Response {
    let pairs = params::parse_pairs(raw.as_deref().unwrap_or(""));
    match detected_fields_impl(state, &headers, pairs).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

pub(crate) async fn detected_fields_post(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    match read_form_pairs(&headers, body).await {
        Ok(pairs) => match detected_fields_impl(state, &headers, pairs).await {
            Ok(res) => res,
            Err(e) => e.into_response(),
        },
        Err(e) => e.into_response(),
    }
}

async fn detected_fields_impl(
    state: AppState,
    headers: &HeaderMap,
    pairs: Vec<(String, String)>,
) -> Result<Response, ApiError> {
    // `query` is required and non-empty (the reference's
    // `syntax.ParseLogSelector` — full log-selector grammar incl.
    // pipelines); a metric query has no per-entry fields. Both rejected
    // BEFORE any pool work (the stats precedent).
    let query = match params::get(&pairs, "query") {
        None | Some("") => return Err(ParamError::MissingQuery.into()),
        Some(q) => q,
    };
    let expr = pulsus_logql::parse(query)?;
    if !matches!(expr, Expr::Log(_)) {
        return Err(ParamError::MetricQueryUnsupported {
            endpoint: "detected_fields",
        }
        .into());
    }
    let (start_ns, end_ns) = parse_bounds(&pairs)?;
    let bounds = TimeBounds { start_ns, end_ns };
    let line_limit = params::parse_line_limit(params::get(&pairs, "line_limit"))?;
    let field_limit = params::parse_field_limit(
        params::get(&pairs, "limit"),
        params::get(&pairs, "field_limit"),
    )?;

    let engine = engine_for(&state).await?;
    if wants_explain(headers) {
        let (out, explain) = engine
            .detected_fields_explained(&expr, bounds, line_limit, field_limit)
            .await?;
        Ok(encode::detected_fields_response(
            out,
            field_limit,
            Some(explain),
        ))
    } else {
        let out = engine
            .detected_fields(&expr, bounds, line_limit, field_limit)
            .await?;
        Ok(encode::detected_fields_response(out, field_limit, None))
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

    async fn labels_get(query: Option<&str>) -> (StatusCode, serde_json::Value) {
        let res = detected_labels(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(query.map(str::to_string)),
        )
        .await;
        status_and_body(res).await
    }

    async fn fields_get(query: Option<&str>) -> (StatusCode, serde_json::Value) {
        let res = detected_fields(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(query.map(str::to_string)),
        )
        .await;
        status_and_body(res).await
    }

    const SELECTOR: &str = "query=%7Bservice_name%3D%22checkout%22%7D";

    // -- detected_labels ---------------------------------------------------

    /// An absent `query` is the UNSCOPED form — valid, so with no pool it
    /// reaches the 503 pool check (proving validation passed).
    #[tokio::test]
    async fn detected_labels_without_query_is_unscoped_then_503_without_a_pool() {
        let (status, json) = labels_get(None).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }

    /// An empty `query=` is the same unscoped form (the reference's
    /// empty-string handling).
    #[tokio::test]
    async fn detected_labels_empty_query_is_unscoped_then_503_without_a_pool() {
        let (status, json) = labels_get(Some("query=")).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }

    #[tokio::test]
    async fn detected_labels_malformed_query_is_400_bad_data_with_a_position() {
        let (status, json) = labels_get(Some("query=%7B")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(json.get("position").is_some());
    }

    /// Issue #170: `query` is matchers-only (`parse_selector`) — a
    /// pipeline in `query` is a 400 parse error with `position`, BEFORE
    /// any pool work (no pool exists here, yet the error is `bad_data`).
    #[tokio::test]
    async fn detected_labels_pipeline_in_query_is_400_with_a_position_before_the_pool_check() {
        let (status, json) = labels_get(Some(&format!("{SELECTOR}%20%7C%3D%20%22err%22"))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(json.get("position").is_some());
    }

    // -- detected_fields ---------------------------------------------------

    #[tokio::test]
    async fn detected_fields_missing_query_is_400_bad_data() {
        let (status, json) = fields_get(None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(json.get("position").is_none());
    }

    /// Unlike detected_labels, an EMPTY `query=` is missing here — the
    /// param is required.
    #[tokio::test]
    async fn detected_fields_empty_query_is_400_bad_data() {
        let (status, json) = fields_get(Some("query=")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
    }

    #[tokio::test]
    async fn detected_fields_malformed_query_is_400_bad_data_with_a_position() {
        let (status, json) = fields_get(Some("query=%7B")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(json.get("position").is_some());
    }

    /// Issue #170: a metric query is rejected 400 BEFORE any pool/engine
    /// work (no pool exists here, yet the error is `bad_data`, not 503).
    #[tokio::test]
    async fn detected_fields_metric_query_is_400_bad_data_before_the_pool_check() {
        let (status, json) =
            fields_get(Some("query=count_over_time(%7Bapp%3D%22x%22%7D%5B1h%5D)")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(
            json["error"]
                .as_str()
                .unwrap_or_default()
                .contains("metric queries")
        );
    }

    #[tokio::test]
    async fn detected_fields_line_limit_zero_is_400_bad_data() {
        let (status, json) = fields_get(Some(&format!("{SELECTOR}&line_limit=0"))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(
            json["error"]
                .as_str()
                .unwrap_or_default()
                .contains("line_limit")
        );
    }

    #[tokio::test]
    async fn detected_fields_limit_above_the_cap_is_400_bad_data() {
        let (status, json) = fields_get(Some(&format!("{SELECTOR}&limit=999999"))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
    }

    /// A full log-selector query WITH a pipeline is shape-valid on
    /// detected_fields (unlike detected_labels); with no pool it reaches
    /// the 503 pool check (proving validation passed).
    #[tokio::test]
    async fn detected_fields_pipeline_query_passes_validation_then_503_without_a_pool() {
        let (status, json) = fields_get(Some(&format!(
            "{SELECTOR}%20%7C%20json&line_limit=50&field_limit=10"
        )))
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }
}
