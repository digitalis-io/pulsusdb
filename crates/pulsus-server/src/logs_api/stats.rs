//! `GET /api/logs/v1/stats` (issue #74, docs/api.md §2.5): parse
//! `query`/`start`/`end`, validate the query shape (a log stream selector
//! plus optional line filters — nothing else has a pushdown aggregation),
//! dispatch to `LogQlEngine::stats`, and encode the bare
//! `{"streams","chunks","entries","bytes"}` object. Pushdown-first:
//! rollup-routed with zero body reads when there is no line filter, a
//! skip-index `log_samples` scan otherwise — the routing is visible via
//! `X-Pulsus-Explain`.

use axum::extract::{RawQuery, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};

use pulsus_logql::{Expr, Stage};
use pulsus_read::TimeBounds;

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

pub(crate) async fn stats(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Response {
    let pairs = params::parse_pairs(raw.as_deref().unwrap_or(""));
    match stats_impl(state, &headers, pairs).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

async fn stats_impl(
    state: AppState,
    headers: &HeaderMap,
    pairs: Vec<(String, String)>,
) -> Result<Response, ApiError> {
    let query = params::get(&pairs, "query").ok_or(ParamError::MissingQuery)?;
    let expr = pulsus_logql::parse(query)?;
    validate_stats_query(&expr)?;
    let (start_ns, end_ns) = parse_bounds(&pairs)?;
    let bounds = TimeBounds { start_ns, end_ns };

    let engine = engine_for(&state).await?;
    if wants_explain(headers) {
        let (stats, explain) = engine.stats_explained(&expr, bounds).await?;
        Ok(encode::stats_response(stats, Some(explain)))
    } else {
        let stats = engine.stats(&expr, bounds).await?;
        Ok(encode::stats_response(stats, None))
    }
}

/// Stats accepts a stream selector plus line filters only: a metric
/// query has no stream statistics, and a parser/format/label-filter
/// stage has no pushdown aggregation shape (ignoring it would silently
/// over-count). Rejected 400 here, before any engine/pool work.
fn validate_stats_query(expr: &Expr) -> Result<(), ParamError> {
    match expr {
        Expr::Log(le) => {
            if le
                .pipeline
                .iter()
                .all(|s| matches!(s, Stage::LineFilter(_)))
            {
                Ok(())
            } else {
                Err(ParamError::StatsPipelineUnsupported)
            }
        }
        _ => Err(ParamError::MetricQueryUnsupported { endpoint: "stats" }),
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
    async fn stats_missing_query_param_is_400_bad_data() {
        let res = stats(State(test_state()), HeaderMap::new(), RawQuery(None)).await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
    }

    #[tokio::test]
    async fn stats_malformed_logql_is_400_bad_data_with_a_position() {
        let res = stats(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some("query=%7B".to_string())),
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(json.get("position").is_some());
    }

    /// Issue #74: a metric query on the stats surface is rejected 400
    /// BEFORE any pool/engine work (no pool exists here, yet the error
    /// is `bad_data`, not the 503 the pool check would produce).
    #[tokio::test]
    async fn stats_metric_query_is_400_bad_data_before_the_pool_check() {
        let res = stats(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some(
                "query=count_over_time(%7Bapp%3D%22x%22%7D%5B1h%5D)".to_string(),
            )),
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(
            json["error"]
                .as_str()
                .unwrap_or_default()
                .contains("metric queries")
        );
    }

    /// Issue #74: a pipeline stage beyond a line filter (here `| logfmt`)
    /// is rejected 400 — nothing else has a pushdown aggregation shape.
    #[tokio::test]
    async fn stats_parser_pipeline_is_400_bad_data() {
        let res = stats(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some("query=%7Bapp%3D%22x%22%7D%20%7C%20logfmt".to_string())),
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(
            json["error"]
                .as_str()
                .unwrap_or_default()
                .contains("line filters only")
        );
    }

    /// A selector-plus-line-filter query is shape-valid; with no pool it
    /// reaches the 503 pool check (proving validation passed).
    #[tokio::test]
    async fn stats_selector_with_line_filter_passes_validation_then_503_without_a_pool() {
        let res = stats(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some(
                "query=%7Bapp%3D%22x%22%7D%20%7C%3D%20%22err%22".to_string(),
            )),
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }
}
