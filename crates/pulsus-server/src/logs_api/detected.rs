//! `GET|POST /api/logs/v1/{detected_labels,detected_fields}` (issue #170,
//! docs/api.md §2.6): the drilldown field/label discovery endpoints,
//! semantics pinned against the repo's interop reference.
//!
//! - **detected_labels** reads ONLY the stream index (`log_streams_idx`)
//!   via one server-side aggregation — never `log_samples`. `query=` is
//!   optional and **matchers only** (`parse_selector`; a pipeline in
//!   `query` is a 400 parse error).
//! - **detected_fields** samples <= `line_limit` **post-pipeline
//!   matching** entries (structured metadata + pipeline extractions +
//!   json/logfmt auto-detection); `query` is required and accepts the
//!   full log-selector grammar including pipelines; metric queries are
//!   400. Budget-truncated sampling OR a retention-capped cardinality
//!   (issue #244: the server-side `MAX_DETECTED_FIELD_BYTES` ceiling
//!   refused a distinct value/name — clamped and served, never an error)
//!   is signaled by the additive `pulsus_partial: true` response key
//!   (omitted when false).
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
    // BEFORE any pool work.
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
    let expr = super::parse_logql(query)?;
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

    async fn labels_get(query: Option<&str>) -> (StatusCode, String) {
        let res = detected_labels(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(query.map(str::to_string)),
        )
        .await;
        status_and_body(res).await
    }

    async fn fields_get(query: Option<&str>) -> (StatusCode, String) {
        let res = detected_fields(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(query.map(str::to_string)),
        )
        .await;
        status_and_body(res).await
    }

    const SELECTOR: &str = "query=%7Bservice_name%3D%22checkout%22%7D";

    /// Issue #279: a valid selector of exactly 131,072 bytes —
    /// `MAX_QUERY_BYTES`, the reference's `maxInputSize` — one byte past
    /// the longest accepted query.
    fn oversized_query_param() -> String {
        format!(r#"query={{app="{}"}}"#, "a".repeat(131_072 - 8))
    }

    fn assert_query_too_long(status: StatusCode, body: &str) {
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body, "input size too long (131072 > 131072)");
    }

    // -- detected_labels ---------------------------------------------------

    /// An absent `query` is the UNSCOPED form — valid, so with no pool it
    /// reaches the 503 pool check (proving validation passed).
    #[tokio::test]
    async fn detected_labels_without_query_is_unscoped_then_503_without_a_pool() {
        let (status, _body) = labels_get(None).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    /// An empty `query=` is the same unscoped form (the reference's
    /// empty-string handling).
    #[tokio::test]
    async fn detected_labels_empty_query_is_unscoped_then_503_without_a_pool() {
        let (status, _body) = labels_get(Some("query=")).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn detected_labels_malformed_query_is_400_with_the_byte_offset_in_the_message() {
        let (status, body) = labels_get(Some("query=%7B")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("at byte"), "{body}");
    }

    /// Issue #170: `query` is matchers-only (`parse_selector`) — a
    /// pipeline in `query` is a 400 parse error BEFORE any pool work
    /// (no pool exists here, yet the answer is 400, not 503).
    #[tokio::test]
    async fn detected_labels_pipeline_in_query_is_400_with_the_byte_offset_in_the_message_before_the_pool_check()
     {
        let (status, body) = labels_get(Some(&format!("{SELECTOR}%20%7C%3D%20%22err%22"))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("at byte"), "{body}");
    }

    /// Issue #279 (AC4): `/detected_labels` — an over-cap `query=` (the
    /// param is optional; the cap applies when present) is 400 against a
    /// POOLLESS state, while a valid selector is 503 — the parse precedes
    /// the pool check, so the 400 is the cap.
    #[tokio::test]
    async fn detected_labels_rejects_an_over_cap_query_400_before_the_pool_check() {
        let (status, body) = labels_get(Some(&oversized_query_param())).await;
        assert_query_too_long(status, &body);

        let (status, _body) = labels_get(Some(SELECTOR)).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    /// Issue #279 (AC4): `/detected_fields` — over-cap 400 vs valid 503,
    /// poolless.
    #[tokio::test]
    async fn detected_fields_rejects_an_over_cap_query_400_before_the_pool_check() {
        let (status, body) = fields_get(Some(&oversized_query_param())).await;
        assert_query_too_long(status, &body);

        let (status, _body) = fields_get(Some(SELECTOR)).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    // -- detected_fields ---------------------------------------------------

    #[tokio::test]
    async fn detected_fields_missing_query_is_400() {
        let (status, _body) = fields_get(None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    /// Unlike detected_labels, an EMPTY `query=` is missing here — the
    /// param is required.
    #[tokio::test]
    async fn detected_fields_empty_query_is_400() {
        let (status, _body) = fields_get(Some("query=")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn detected_fields_malformed_query_is_400_with_the_byte_offset_in_the_message() {
        let (status, body) = fields_get(Some("query=%7B")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("at byte"), "{body}");
    }

    /// Issue #170: a metric query is rejected 400 BEFORE any pool/engine
    /// work (no pool exists here, yet the answer is 400, not 503).
    #[tokio::test]
    async fn detected_fields_metric_query_is_400_before_the_pool_check() {
        let (status, body) =
            fields_get(Some("query=count_over_time(%7Bapp%3D%22x%22%7D%5B1h%5D)")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("metric queries"), "{body}");
    }

    #[tokio::test]
    async fn detected_fields_line_limit_zero_is_400() {
        let (status, body) = fields_get(Some(&format!("{SELECTOR}&line_limit=0"))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("line_limit"), "{body}");
    }

    /// Issue #253: the field-name axis has NO ceiling. Reaching the
    /// poolless `503` is what proves param validation passed (this
    /// module's established idiom, below) — pre-#253 every one of these
    /// was a `400`. The reference answers `200` to all of them
    /// (`grafana/loki:3.7.4`, measured 2026-08-07).
    ///
    /// The `limit=4294967296` and `limit=9223372036854775807` rows are the
    /// FIELD axis, where `parse_field_limit` saturates at `u32::MAX` and
    /// the reference's unchecked `uint32()` wraps. The `line_limit=` row
    /// is here for empty-is-absent only, not for the cast — that axis
    /// refuses out-of-range values rather than saturating
    /// (`parse_line_limit`), which
    /// `parse_line_limit_matches_the_reference_atoi_surface` pins.
    #[tokio::test]
    async fn detected_fields_limit_far_above_the_entry_cap_passes_validation_then_503_without_a_pool()
     {
        for qs in [
            "limit=5001",
            "limit=50000",
            "limit=1000000",
            "limit=2147483647",
            "limit=4294967295",
            "limit=4294967296",
            "limit=9223372036854775807",
            "field_limit=5001",
            "field_limit=4294967295",
            // Present-but-empty is absent, so the alias is what is used.
            "limit=&field_limit=5001",
            "limit=",
            "line_limit=",
        ] {
            let (status, body) = fields_get(Some(&format!("{SELECTOR}&{qs}"))).await;
            assert_eq!(
                status,
                StatusCode::SERVICE_UNAVAILABLE,
                "{qs} must pass validation: {body}"
            );
        }
    }

    /// The reject surface that stays: values `<= 0`, spellings outside
    /// `i64::from_str`, and `line_limit` past `MAX_LIMIT` are all still
    /// a `400`, on both parameters. The reference answers `400` to each of
    /// these too (measured).
    #[tokio::test]
    async fn detected_fields_non_positive_and_non_numeric_limits_are_still_400() {
        for qs in [
            "limit=0",
            "limit=-1",
            "limit=00",
            "limit=abc",
            "limit=1.5",
            "limit=9223372036854775808",
            "field_limit=0",
            "line_limit=0",
            "line_limit=5001",
        ] {
            let (status, _body) = fields_get(Some(&format!("{SELECTOR}&{qs}"))).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{qs} must be a 400");
        }
    }

    /// A full log-selector query WITH a pipeline is shape-valid on
    /// detected_fields (unlike detected_labels); with no pool it reaches
    /// the 503 pool check (proving validation passed).
    #[tokio::test]
    async fn detected_fields_pipeline_query_passes_validation_then_503_without_a_pool() {
        let (status, _body) = fields_get(Some(&format!(
            "{SELECTOR}%20%7C%20json&line_limit=50&field_limit=10"
        )))
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }
}
