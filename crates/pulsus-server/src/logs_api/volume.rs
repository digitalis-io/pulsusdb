//! `GET /api/logs/v1/volume` (issue #169, docs/api.md §2.6): parse
//! `query`/`start`/`end`/`limit`/`aggregateBy`/`targetLabels`, validate
//! the query shape (a bare stream selector — ANY pipeline stage is a 400,
//! line filters included, unlike `/stats`), dispatch to
//! `LogQlEngine::volume`, and encode the order-preserving vector envelope
//! at `end`. Rollup-only by construction: matchers-only queries are
//! always served from `log_metrics_5s` with zero body reads (there is no
//! raw fallback), visible via `X-Pulsus-Explain`. The `targetLabels`
//! caps (`params::MAX_TARGET_LABELS`/`MAX_TARGET_LABEL_BYTES`) reject
//! here, in pure param parsing, BEFORE any AST mutation/planning/SQL.

use axum::extract::{RawQuery, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};

use pulsus_logql::Expr;
use pulsus_read::{TimeBounds, VolumeQuery};

use crate::app::AppState;

use super::encode;
use super::error::ApiError;
use super::handlers::{engine_for, parse_bounds};
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

async fn volume_impl(
    state: AppState,
    headers: &HeaderMap,
    pairs: Vec<(String, String)>,
) -> Result<Response, ApiError> {
    let query = params::get(&pairs, "query").ok_or(ParamError::MissingQuery)?;
    let expr = pulsus_logql::parse(query)?;
    validate_volume_query(&expr)?;
    let (start_ns, end_ns) = parse_bounds(&pairs)?;
    if end_ns < start_ns {
        return Err(ParamError::EndBeforeStart.into());
    }
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

    async fn status_and_body(res: Response) -> (StatusCode, serde_json::Value) {
        let status = res.status();
        let bytes = to_bytes(res.into_body(), usize::MAX).await.expect("body");
        let json: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        (status, json)
    }

    async fn get(query: Option<&str>) -> (StatusCode, serde_json::Value) {
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
    async fn volume_missing_query_param_is_400_bad_data() {
        let (status, json) = get(None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
    }

    #[tokio::test]
    async fn volume_malformed_logql_is_400_bad_data_with_a_position() {
        let (status, json) = get(Some("query=%7B")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(json.get("position").is_some());
    }

    /// Issue #169: a metric query on the volume surface is rejected 400
    /// BEFORE any pool/engine work (no pool exists here, yet the error is
    /// `bad_data`, not the 503 the pool check would produce).
    #[tokio::test]
    async fn volume_metric_query_is_400_bad_data_before_the_pool_check() {
        let (status, json) = get(Some("query=count_over_time(%7Bapp%3D%22x%22%7D%5B1h%5D)")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(
            json["error"]
                .as_str()
                .unwrap_or_default()
                .contains("metric queries")
        );
    }

    /// Issue #169: unlike `/stats`, even a LINE FILTER is rejected — the
    /// rollup is body-content-blind and volume has no raw fallback.
    #[tokio::test]
    async fn volume_line_filter_pipeline_is_400_bad_data() {
        let (status, json) = get(Some("query=%7Bapp%3D%22x%22%7D%20%7C%3D%20%22err%22")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(
            json["error"]
                .as_str()
                .unwrap_or_default()
                .contains("bare stream selector")
        );
    }

    #[tokio::test]
    async fn volume_parser_pipeline_is_400_bad_data() {
        let (status, json) = get(Some("query=%7Bapp%3D%22x%22%7D%20%7C%20logfmt")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
    }

    #[tokio::test]
    async fn volume_invalid_aggregate_by_is_400_bad_data() {
        let (status, json) = get(Some(&format!("{SELECTOR}&aggregateBy=both"))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(
            json["error"]
                .as_str()
                .unwrap_or_default()
                .contains("aggregateBy")
        );
    }

    #[tokio::test]
    async fn volume_limit_above_the_cap_is_400_bad_data() {
        let (status, json) = get(Some(&format!("{SELECTOR}&limit=5001"))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
    }

    #[tokio::test]
    async fn volume_end_before_start_is_400_bad_data() {
        let (status, json) = get(Some(&format!("{SELECTOR}&start=200&end=100"))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(
            json["error"]
                .as_str()
                .unwrap_or_default()
                .contains("precedes")
        );
    }

    /// Issue #169 plan v2 (b)(ii): oversized `targetLabels` (count) is
    /// rejected 400 while NO pool exists — mechanical proof the rejection
    /// precedes injection/engine/SQL work.
    #[tokio::test]
    async fn volume_too_many_target_labels_is_400_bad_data_before_the_pool_check() {
        let over_cap = (0..33)
            .map(|i| format!("l{i}"))
            .collect::<Vec<_>>()
            .join(",");
        let (status, json) = get(Some(&format!("{SELECTOR}&targetLabels={over_cap}"))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(
            json["error"]
                .as_str()
                .unwrap_or_default()
                .contains("too many 'targetLabels'")
        );
    }

    /// Issue #169 plan v2 (b)(ii): the per-entry length cap, same
    /// pool-less pre-injection proof.
    #[tokio::test]
    async fn volume_overlong_target_label_is_400_bad_data_before_the_pool_check() {
        let long = "x".repeat(257);
        let (status, json) = get(Some(&format!("{SELECTOR}&targetLabels={long}"))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(
            json["error"]
                .as_str()
                .unwrap_or_default()
                .contains("exceeds the maximum")
        );
    }

    /// A bare selector (with in-cap params) is shape-valid; with no pool
    /// it reaches the 503 pool check (proving validation passed).
    #[tokio::test]
    async fn volume_valid_selector_passes_validation_then_503_without_a_pool() {
        let (status, json) = get(Some(&format!(
            "{SELECTOR}&limit=0&aggregateBy=labels&targetLabels=env,team"
        )))
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }
}
