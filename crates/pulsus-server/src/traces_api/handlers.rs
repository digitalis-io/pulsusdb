//! The two `/api/traces/v1/trace/{traceId}` handlers (docs/api.md §4.1):
//! acquire the pool → parse the hex trace id (`params.rs`) → point-read
//! via `TraceEngine` (`pulsus-read`, empty ⇒ 404) → assemble the OTLP
//! `TracesData` (`assemble.rs`) → negotiate the representation
//! (`negotiate.rs`; the `/json` route forces JSON before `Accept` is ever
//! consulted) → encode. Thin by design — SQL/execution stays in
//! `pulsus-read`, OTLP assembly in `assemble.rs`.
//!
//! [`trace_by_id_v2`] (issue #474) serves the `/api/v2/traces/{traceId}`
//! compat alias through the same steps, wrapping the result in
//! `fetch_v2.rs`'s envelope and answering `200` with an empty trace where
//! the v1 handlers answer `404`.

use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};

use pulsus_read::TraceEngine;

use crate::app::AppState;
use crate::chconfig;

use super::assemble::{self, AssembleError, AssembledTrace};
use super::error::ApiError;
use super::fetch_v2;
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
    // Issue #114: the consistency-config invariant is already enforced at
    // config load, so this is unreachable in the real binary; a failure maps
    // to the existing 503 "not serving" semantics.
    chconfig::trace_engine(pool, &state.config).map_err(|_| ApiError::PoolUnavailable)
}

/// `GET /api/traces/v1/trace/{traceId}` — representation by `Accept`
/// (default JSON). Every response (success or error) carries
/// `Vary: accept` (RFC 9110 §12.5.5, issue #55 review): the 200/406
/// genuinely vary by `Accept`, and a blanket insert on the pre-negotiation
/// error paths (400/404/503) is conservative-but-cache-safe and avoids
/// plumbing "negotiation reached" state through `ApiError`. The `/json`
/// route below never consults `Accept`, so it gets no `Vary`.
pub(crate) async fn trace_by_id(
    State(state): State<AppState>,
    Path(trace_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let mut res = match trace_by_id_impl(state, &trace_id, Some(&headers)).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    };
    res.headers_mut()
        .insert(header::VARY, HeaderValue::from_static("accept"));
    res
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
    let data = AssembledTrace::from_stored(spans)?;
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

/// `GET /api/v2/traces/{traceId}` (issue #474) — the fourteenth compat
/// alias. Same order of operations as [`trace_by_id_impl`] (pool, then id
/// parse, then fetch) so error precedence matches the v1 alias exactly;
/// same `negotiate.rs`; same `Vary: accept` on every response the handler
/// returns. The ONE difference: an empty fetch is `200` with
/// [`AssembledTrace::empty`], never `404` — the client dereferences the
/// envelope's `trace` field without a nil check, and a `404` here is what
/// made a trace outside the queried time range render as a raw HTTP error
/// string instead of a sentence about the range.
///
/// Query parameters are accepted and ignored, exactly as the v1 fetch
/// route ignores them: our point read has no time bound, so ignoring
/// `start`/`end` returns a superset, never a wrong answer.
pub(crate) async fn trace_by_id_v2(
    State(state): State<AppState>,
    Path(trace_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let mut res = match trace_by_id_v2_impl(state, &trace_id, &headers).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    };
    res.headers_mut()
        .insert(header::VARY, HeaderValue::from_static("accept"));
    res
}

async fn trace_by_id_v2_impl(
    state: AppState,
    raw_trace_id: &str,
    negotiate_headers: &HeaderMap,
) -> Result<Response, ApiError> {
    let engine = engine_for(&state).await?;
    let hex32 = params::parse_trace_id(raw_trace_id)?;
    let spans = engine.fetch_by_id(&hex32).await?;
    // The empty case is the whole reason this route exists: a present,
    // empty trace, not a 404.
    let trace = if spans.is_empty() {
        AssembledTrace::empty()
    } else {
        AssembledTrace::from_stored(spans)?
    };
    let wants = negotiate::negotiate_from_headers(negotiate_headers)?;
    let (content_type, body) = match wants {
        Wants::Json => (
            "application/json",
            fetch_v2::encode_json(&trace).map_err(AssembleError::from)?,
        ),
        Wants::Protobuf => ("application/protobuf", fetch_v2::encode_protobuf(&trace)),
    };
    Ok((StatusCode::OK, [(header::CONTENT_TYPE, content_type)], body).into_response())
}

#[cfg(test)]
mod tests {
    use super::super::error::testutil::error_body;
    use super::*;

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

    #[tokio::test]
    async fn trace_by_id_without_a_pool_is_503() {
        let res = trace_by_id(
            State(test_state()),
            Path("4bf92f3577b34da6a3ce929d0e0e4736".to_string()),
            HeaderMap::new(),
        )
        .await;
        assert_eq!(
            res.headers().get(header::VARY).map(|v| v.as_bytes()),
            Some(b"accept".as_slice())
        );
        // `error_body` asserts Tempo's container on the way through, so
        // the fetch route is not a hole in the #384 check.
        let (status, body) = error_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, "clickhouse pool not yet established");
    }

    #[tokio::test]
    async fn trace_by_id_json_without_a_pool_is_503() {
        let res = trace_by_id_json(
            State(test_state()),
            Path("4bf92f3577b34da6a3ce929d0e0e4736".to_string()),
            HeaderMap::new(),
        )
        .await;
        assert!(res.headers().get(header::VARY).is_none());
        let (status, body) = error_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, "clickhouse pool not yet established");
    }
}
