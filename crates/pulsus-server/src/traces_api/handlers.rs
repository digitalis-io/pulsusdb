//! The two `/api/traces/v1/trace/{traceId}` handlers (docs/api.md §4.1):
//! acquire the pool → parse the hex trace id and the optional request
//! window (`params.rs`) → fetch via `TraceEngine` (`pulsus-read`, empty ⇒
//! 404) → rebuild the OTLP `TracesData` from the fetched columns
//! (`assemble.rs`) → negotiate the representation (`negotiate.rs`; the
//! `/json` route forces JSON before `Accept` is ever consulted) → encode.
//! Thin by design — SQL/execution stays in `pulsus-read`, OTLP assembly in
//! `assemble.rs`.
//!
//! **Three things each handler owns since issue #587.** It mints one
//! statement-id prefix per request — before parsing anything — and
//! returns it in an `X-Pulsus-Query-Id` response header on **every**
//! response, the errors included, so a caller (or a test) reads the prefix
//! of the statement ids the request issued rather than recomputing it. It
//! reports the one degraded state a fetch can detect from its own inputs:
//! a span whose `resource_id` has no row in the resource array, which is a
//! `200` going out without the sender's resource attributes. And it
//! reports the route the read took, which is how the complete-predicate
//! fetch's second statement becomes visible to an operator.
//!
//! [`trace_by_id_v2`] (issue #474) serves the `/api/v2/traces/{traceId}`
//! compat alias through the same steps, wrapping the result in
//! `fetch_v2.rs`'s envelope and answering `200` with an empty trace where
//! the v1 handlers answer `404`.

use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};

use pulsus_read::{FetchRoute, TraceEngine};

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
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Response {
    let stmt_prefix = new_statement_prefix();
    let mut res = match trace_by_id_impl(
        state,
        &trace_id,
        query.as_deref(),
        Some(&headers),
        &stmt_prefix,
    )
    .await
    {
        Ok(res) => res,
        Err(e) => e.into_response(),
    };
    attach_query_id(&mut res, &stmt_prefix);
    res.headers_mut()
        .insert(header::VARY, HeaderValue::from_static("accept"));
    res
}

/// `GET /api/traces/v1/trace/{traceId}/json` — forces JSON; never
/// negotiates, never 406 (docs/api.md §4.1).
pub(crate) async fn trace_by_id_json(
    State(state): State<AppState>,
    Path(trace_id): Path<String>,
    RawQuery(query): RawQuery,
    _headers: HeaderMap,
) -> Response {
    let stmt_prefix = new_statement_prefix();
    let mut res =
        match trace_by_id_impl(state, &trace_id, query.as_deref(), None, &stmt_prefix).await {
            Ok(res) => res,
            Err(e) => e.into_response(),
        };
    attach_query_id(&mut res, &stmt_prefix);
    res
}

/// Shared fetch path. `negotiate_headers` is `Some` for the negotiating
/// route and `None` for the `/json` route (forced JSON — `Accept` is never
/// consulted, so it can never 406). `stmt_prefix` is minted by the caller,
/// because the caller is what attaches it to the response and it must be
/// attached to the errors this function returns as well as to its `200`.
async fn trace_by_id_impl(
    state: AppState,
    raw_trace_id: &str,
    raw_query: Option<&str>,
    negotiate_headers: Option<&HeaderMap>,
    stmt_prefix: &str,
) -> Result<Response, ApiError> {
    let hex32 = params::parse_trace_id(raw_trace_id)?;
    let window = params::parse_fetch_window(raw_query)?;
    let engine = engine_for(&state).await?.with_statement_prefix(stmt_prefix);
    let fetched = engine.fetch_by_id(&hex32, window).await?;
    report_fetch_route(fetched.route);
    if fetched.spans.is_empty() {
        return Err(ApiError::NotFound);
    }
    let data = AssembledTrace::from_fetched(&trace_id_bytes(&hex32), fetched)?;
    report_missing_resources(&state, data.missing_resources());
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
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, content_type.to_string())],
        body,
    )
        .into_response())
}

/// The response header carrying a request's statement-id prefix: the
/// statements the request issued are `<prefix>-1`, `<prefix>-2`, … in
/// issue order.
pub(super) const QUERY_ID_HEADER: HeaderName = HeaderName::from_static("x-pulsus-query-id");

/// Attaches the prefix to whatever the fetch path produced — the `200`
/// and **every error alike** (docs/api.md §4.1: *"every response from the
/// fetch routes"*).
///
/// On the way out rather than on the success path, and that is the point.
/// An operator chasing a `404` or a `500` through `system.query_log` needs
/// that request's own prefix more than a successful caller does; on a
/// `400` or a `503` no statement ran, so the prefix correctly names an
/// empty set rather than somebody else's rows. One insert at the exit
/// cannot have a branch that forgets it, which is what the code review of
/// 2026-10-03 found: the header was built into the `200` tuple, so the
/// four error statuses went out without it.
fn attach_query_id(res: &mut Response, prefix: &str) {
    let value = HeaderValue::from_str(prefix).expect("the prefix is 32 hex characters");
    res.headers_mut().insert(QUERY_ID_HEADER, value);
}

/// One prefix per request: **32 hex chars** of operating-system
/// randomness, so it is a legal `query_id` and two concurrent requests
/// cannot be given one.
///
/// **`getrandom` rather than `uuid`**, for the reason the workspace's own
/// entry gives: one opaque string per request is the same shape the write
/// path's token mint already declines `uuid` for, and `getrandom` adds no
/// crate to this binary's graph. A failure to read randomness falls back
/// to the monotonic clock plus a process-local counter, which is still
/// unique within a process and is the only thing left when the kernel's
/// own source is unavailable.
fn new_statement_prefix() -> String {
    match (getrandom::u64(), getrandom::u64()) {
        (Ok(hi), Ok(lo)) => format!("{hi:016x}{lo:016x}"),
        _ => {
            static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            format!("{nanos:016x}{n:016x}")
        }
    }
}

/// The validated 32-char hex id as the 16 bytes every rebuilt span's
/// `trace_id` carries. `parse_trace_id` is the one validation point, so
/// this cannot fail for anything it admits.
fn trace_id_bytes(hex32: &str) -> [u8; 16] {
    let mut out = [0u8; 16];
    for (i, slot) in out.iter_mut().enumerate() {
        *slot = u8::from_str_radix(&hex32[i * 2..i * 2 + 2], 16)
            .expect("parse_trace_id admits only hex digits");
    }
    out
}

/// Reports the spans whose resource row was missing, through the metric
/// facade the server already holds.
///
/// **It makes no answer correct.** It exists so a `200` going out without
/// the sender's resource attributes is visible rather than silent: the
/// reader knows which `resource_id`s its spans reference and which came
/// back, which is the one degraded state a fetch can detect from its own
/// inputs.
fn report_missing_resources(state: &AppState, missing: u64) {
    let _ = state;
    crate::ops::record_fetch_missing_resources(missing);
}

/// Reports a fetch that took the complete-predicate route (§3.4), through
/// the same metric facade.
///
/// **Only `TruncatedSet`.** A counter that fired on every route would
/// report nothing; this one says a trace occupied 4,096 or more
/// five-minute buckets, so the per-trace table's stored set may have been
/// cut and the fetch paid for a second statement. `statements == 2` cannot
/// carry that signal, because the window fallback gives 2 as well — which
/// is why the route travels on `FetchedTrace` as an enum rather than being
/// inferred from the count (§3.4a).
///
/// **It makes no answer correct either.** The complete-predicate route
/// answers the whole trace; the counter exists so that paying twice for it
/// is visible.
fn report_fetch_route(route: FetchRoute) {
    if route == FetchRoute::TruncatedSet {
        crate::ops::record_fetch_truncated_set();
    }
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
/// `start`/`end` are read, and only for the fallback (issue #587). The
/// indexed statement never reads the window, so the property that made
/// ignoring them safe is preserved: the window only ever ADDS an answer —
/// it answers a trace the per-trace table has not indexed, and it can
/// never narrow one the indexed statement found. Every other query
/// parameter is still accepted and ignored.
pub(crate) async fn trace_by_id_v2(
    State(state): State<AppState>,
    Path(trace_id): Path<String>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Response {
    let stmt_prefix = new_statement_prefix();
    let mut res =
        match trace_by_id_v2_impl(state, &trace_id, query.as_deref(), &headers, &stmt_prefix).await
        {
            Ok(res) => res,
            Err(e) => e.into_response(),
        };
    attach_query_id(&mut res, &stmt_prefix);
    res.headers_mut()
        .insert(header::VARY, HeaderValue::from_static("accept"));
    res
}

async fn trace_by_id_v2_impl(
    state: AppState,
    raw_trace_id: &str,
    raw_query: Option<&str>,
    negotiate_headers: &HeaderMap,
    stmt_prefix: &str,
) -> Result<Response, ApiError> {
    let hex32 = params::parse_trace_id(raw_trace_id)?;
    let window = params::parse_fetch_window(raw_query)?;
    let engine = engine_for(&state).await?.with_statement_prefix(stmt_prefix);
    let fetched = engine.fetch_by_id(&hex32, window).await?;
    report_fetch_route(fetched.route);
    // The empty case is the whole reason this route exists: a present,
    // empty trace, not a 404.
    let trace = if fetched.spans.is_empty() {
        AssembledTrace::empty()
    } else {
        AssembledTrace::from_fetched(&trace_id_bytes(&hex32), fetched)?
    };
    report_missing_resources(&state, trace.missing_resources());
    let wants = negotiate::negotiate_from_headers(negotiate_headers)?;
    let (content_type, body) = match wants {
        Wants::Json => (
            "application/json",
            fetch_v2::encode_json(&trace).map_err(AssembleError::from)?,
        ),
        Wants::Protobuf => ("application/protobuf", fetch_v2::encode_protobuf(&trace)),
    };
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, content_type.to_string())],
        body,
    )
        .into_response())
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
            RawQuery(None),
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
            RawQuery(None),
            HeaderMap::new(),
        )
        .await;
        assert!(res.headers().get(header::VARY).is_none());
        let (status, body) = error_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, "clickhouse pool not yet established");
    }

    /// A valid 32-char hex id, so every case below reaches past
    /// `parse_trace_id`.
    const VALID_ID: &str = "4bf92f3577b34da6a3ce929d0e0e4736";

    /// The three fetch entry points, called with one id and one query
    /// string. Returns the route's label beside its response so a failure
    /// names which one.
    async fn each_route(id: &str, query: Option<&str>) -> Vec<(&'static str, Response)> {
        let q = query.map(str::to_string);
        vec![
            (
                "v1",
                trace_by_id(
                    State(test_state()),
                    Path(id.to_string()),
                    RawQuery(q.clone()),
                    HeaderMap::new(),
                )
                .await,
            ),
            (
                "v1/json",
                trace_by_id_json(
                    State(test_state()),
                    Path(id.to_string()),
                    RawQuery(q.clone()),
                    HeaderMap::new(),
                )
                .await,
            ),
            (
                "v2",
                trace_by_id_v2(
                    State(test_state()),
                    Path(id.to_string()),
                    RawQuery(q),
                    HeaderMap::new(),
                )
                .await,
            ),
        ]
    }

    /// Issue #587, the documented response-header contract (docs/api.md
    /// §4.1: *"Every response from the fetch routes … carries
    /// `X-Pulsus-Query-Id`"*). **Every** includes the errors, and they
    /// are the cases the header is most use for: an operator chasing a
    /// `500` or an empty `404` wants that request's own `query_log` rows,
    /// and on a `400` or a `503` the prefix correctly names an empty set
    /// rather than someone else's statements.
    ///
    /// Three error shapes are reachable with no pool and no server — a
    /// malformed id, a malformed window and the unavailable pool — across
    /// all three entry points, which is nine responses from one case.
    #[tokio::test]
    async fn every_fetch_error_response_carries_the_query_id_header() {
        let cases: [(&str, &str, Option<&str>, StatusCode); 3] = [
            ("malformed id", "nothex", None, StatusCode::BAD_REQUEST),
            (
                "malformed window",
                VALID_ID,
                Some("start=yesterday"),
                StatusCode::BAD_REQUEST,
            ),
            (
                "pool unavailable",
                VALID_ID,
                None,
                StatusCode::SERVICE_UNAVAILABLE,
            ),
        ];
        for (shape, id, query, want_status) in cases {
            for (route, res) in each_route(id, query).await {
                let where_ = format!("{route} / {shape}");
                assert_eq!(res.status(), want_status, "status for {where_}");
                let got = res
                    .headers()
                    .get(QUERY_ID_HEADER)
                    .unwrap_or_else(|| panic!("no {QUERY_ID_HEADER} on {where_}"))
                    .to_str()
                    .unwrap_or_else(|e| panic!("non-ascii header on {where_}: {e}"))
                    .to_string();
                assert_eq!(got.len(), 32, "prefix width on {where_}: {got:?}");
                assert!(
                    got.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')),
                    "prefix is not 32 lowercase hex chars on {where_}: {got:?}"
                );
            }
        }
    }

    /// A malformed bound is a `400` **at the route**, not just in the
    /// parser — the route test the parser's own case cannot stand in for,
    /// because the handler decides the order of validation against the
    /// pool acquisition. A lone bound is included: it is the shape a
    /// half-filled time control sends.
    #[tokio::test]
    async fn a_malformed_fetch_bound_is_a_400_on_every_fetch_route() {
        for query in [
            "start=yesterday",
            "end=tomorrow",
            "start=yesterday&end=1700000002",
            "start=1699999999&end=tomorrow",
            "start=yesterday&end=",
        ] {
            for (route, res) in each_route(VALID_ID, Some(query)).await {
                let status = res.status();
                let (_, body) = error_body(res).await;
                assert_eq!(status, StatusCode::BAD_REQUEST, "{route} / {query:?}");
                assert!(
                    body.starts_with("invalid timestamp"),
                    "{route} / {query:?}: body {body:?}"
                );
            }
        }
    }

    /// §3.4's truncated-set counter, and the route flag that carries it
    /// (§3.4a). One sub-case per `FetchRoute` arm, because a counter that
    /// fires on every route reports nothing: the whole point is that a
    /// trace occupying 4,096 or more buckets took the complete-predicate
    /// route, and `statements == 2` cannot say so — the window fallback
    /// gives 2 as well.
    #[test]
    fn only_the_truncated_set_route_increments_its_counter() {
        for (route, want) in [
            (FetchRoute::Indexed, 0u64),
            (FetchRoute::TruncatedSet, 1u64),
            (FetchRoute::Fallback, 0u64),
        ] {
            let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
            let handle = recorder.handle();
            metrics::with_local_recorder(&recorder, || report_fetch_route(route));
            let rendered = handle.render();
            let got: u64 = rendered
                .lines()
                .find_map(|l| l.strip_prefix("pulsus_trace_fetch_truncated_set_total "))
                .map(|v| v.trim().parse().expect("an integer counter value"))
                .unwrap_or(0);
            assert_eq!(got, want, "for {route:?}, rendered:\n{rendered}");
        }
    }
}
