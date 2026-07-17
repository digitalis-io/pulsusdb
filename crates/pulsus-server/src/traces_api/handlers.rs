//! The two `/api/traces/v1/trace/{traceId}` handlers (docs/api.md §4.1):
//! acquire the pool → parse the hex trace id (`params.rs`) → point-read
//! via `TraceEngine` (`pulsus-read`, empty ⇒ 404) → assemble the OTLP
//! `TracesData` (`assemble.rs`) → negotiate the representation
//! (`negotiate.rs`; the `/json` route forces JSON before `Accept` is ever
//! consulted) → encode. Thin by design — SQL/execution stays in
//! `pulsus-read`, OTLP assembly in `assemble.rs`.

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};

use pulsus_read::TraceEngine;

use crate::app::AppState;
use crate::chconfig;

use super::assemble::{self, AssembleError};
use super::error::ApiError;
use super::negotiate::{self, Wants};
use super::params;

/// Acquires the shared `Arc<ChPool>` from `AppState` (the `engine_for`
/// pattern: clone the `Option` out from behind the lock, drop the guard
/// before doing anything else) and builds a `TraceEngine` over it —
/// `503 unavailable` before the pool is established, matching `/ready`.
/// `pub(super)`: `search.rs` shares the same engine acquisition.
pub(super) async fn engine_for(state: &AppState) -> Result<TraceEngine, ApiError> {
    let pool = {
        let guard = state.pool.read().await;
        guard.clone()
    };
    let pool = pool.ok_or(ApiError::PoolUnavailable)?;
    Ok(chconfig::trace_engine(pool, &state.config))
}

/// `GET /api/traces/v1/trace/{traceId}` — representation by `Accept`
/// (default JSON).
pub(crate) async fn trace_by_id(
    State(state): State<AppState>,
    Path(trace_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    match trace_by_id_impl(state, &trace_id, Some(&headers)).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

/// `GET /api/traces/v1/trace/{traceId}/json` — forces JSON; never
/// negotiates, never 406 (docs/api.md §4.1).
pub(crate) async fn trace_by_id_json(
    State(state): State<AppState>,
    Path(trace_id): Path<String>,
    _headers: HeaderMap,
) -> Response {
    match trace_by_id_impl(state, &trace_id, None).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

/// Shared fetch path. `negotiate_headers` is `Some` for the negotiating
/// route and `None` for the `/json` route (forced JSON — `Accept` is never
/// consulted, so it can never 406).
async fn trace_by_id_impl(
    state: AppState,
    raw_trace_id: &str,
    negotiate_headers: Option<&HeaderMap>,
) -> Result<Response, ApiError> {
    let engine = engine_for(&state).await?;
    let hex32 = params::parse_trace_id(raw_trace_id)?;
    let spans = engine.fetch_by_id(&hex32).await?;
    if spans.is_empty() {
        return Err(ApiError::NotFound);
    }
    let data = assemble::assemble(spans)?;
    let wants = match negotiate_headers {
        None => Wants::Json,
        // `negotiate_from_headers` combines every repeated `Accept` field
        // line per RFC 9110 §5.3 before parsing (issue #55 code review) —
        // never just the first line.
        Some(headers) => negotiate::negotiate_from_headers(headers)?,
    };
    let (content_type, body) = match wants {
        Wants::Json => (
            "application/json",
            assemble::encode_json(&data).map_err(AssembleError::from)?,
        ),
        // Response Content-Type is `application/protobuf` (Tempo/OTLP-HTTP
        // convention), deliberately asymmetric with ingest's
        // `application/x-protobuf` — docs/api.md §4.1.
        Wants::Protobuf => ("application/protobuf", assemble::encode_protobuf(&data)),
    };
    Ok((StatusCode::OK, [(header::CONTENT_TYPE, content_type)], body).into_response())
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

    #[tokio::test]
    async fn trace_by_id_without_a_pool_is_503_unavailable() {
        let res = trace_by_id(
            State(test_state()),
            Path("4bf92f3577b34da6a3ce929d0e0e4736".to_string()),
            HeaderMap::new(),
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }

    #[tokio::test]
    async fn trace_by_id_json_without_a_pool_is_503_unavailable() {
        let res = trace_by_id_json(
            State(test_state()),
            Path("4bf92f3577b34da6a3ce929d0e0e4736".to_string()),
            HeaderMap::new(),
        )
        .await;
        let (status, json) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }
}
