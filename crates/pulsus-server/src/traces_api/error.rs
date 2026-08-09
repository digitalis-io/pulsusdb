//! `/api/traces/v1`'s error body: a BARE `text/plain` message (docs/api.md
//! §4 "Errors"), and the status-code mapping table pinned by the issue #55
//! plan (v2's error table + v3's `406 not_acceptable`) plus issue #57's
//! search rows.
//!
//! Issue #384 replaced the previous `{"status","errorType","error",
//! "position"?}` JSON envelope with the reference's shape. **Which
//! reference writer, exactly, matters here** — Tempo has two, they
//! disagree, and only one of them is user-facing:
//!
//! - **The query frontend** serves every user-facing `/api/*` query route
//!   (`cmd/tempo/app/modules.go:500-512 @ v3.0.2`, each `base.Wrap(
//!   queryFrontend.…Handler)`). Its request-validation rejections — the
//!   4xx class this module answers — are `*http.Response` values built
//!   with a **nil `Header` map** (`httpInvalidRequest`,
//!   `modules/frontend/metrics_query_range_handler.go:266-272`;
//!   `extractTenant`, `modules/frontend/util.go:15-25`; the same literal
//!   recurs across `search_handlers.go`, `tag_handlers.go`,
//!   `traceid_handlers.go`, `metrics_query_handler.go`), which
//!   `handler.ServeHTTP` copies out verbatim —
//!   `modules/frontend/handler.go:113-116`: `copyHeader`,
//!   `WriteHeader(resp.StatusCode)`, `io.Copy`. No header is set at all,
//!   so Go's `net/http` sniffs `Content-Type` from the body, **nothing
//!   sets `X-Content-Type-Options`**, and nothing appends a terminator.
//!   (`writeError` -> dskit `httpgrpc.WriteError` is the *other* branch of
//!   the same handler, `handler.go:160-169`; it sets no headers either —
//!   `WriteResponse` copies the httpgrpc response's own headers, of which
//!   `httpgrpc.Error` has none — and its non-httpgrpc fallback is a
//!   hard-coded **500**, `vendor/github.com/grafana/dskit/httpgrpc/
//!   httpgrpc.go:81-88`, so it produces no 4xx of this class.)
//! - **The querier's own handlers** — the `http.Error` sites in
//!   `modules/querier/http.go` — are registered ONLY under
//!   `path.Join(api.PathPrefixQuerier, …)` (`modules.go:438-459`, with
//!   `PathPrefixQuerier = "/querier"`, `pkg/api/http.go:67`), an internal
//!   path no client of ours meets. Go's `http.Error` sets both headers and
//!   appends an LF, so those responses look different — measurably so.
//!
//! So this surface's container is: the message verbatim, **no** trailing
//! newline, `Content-Type: text/plain; charset=utf-8`, and **no**
//! `X-Content-Type-Options`.
//!
//! **This is NOT `logs_api::error::plain_text_error`.** That one sets
//! `nosniff`, because Loki's `WriteError` does
//! (`pkg/util/server/error.go:49 @ loki v3.7.4`). The two writers agree on
//! the content type and on the terminator and differ on exactly that one
//! header, so sharing a responder — or sharing a conformance expectation —
//! would make us emit a header Tempo never emits, with every other
//! assertion still green. The two stay separate functions on purpose; do
//! not factor them into one with a flag.
//!
//! The container is pinned by test, not by this comment:
//! [`testutil::assert_reference_container`] asserts the content type, its
//! uniqueness, and `nosniff`'s ABSENCE on every error every module in this
//! surface renders; the terminator is asserted on raw bytes in
//! `tests::an_error_is_the_bare_message_with_no_json_and_no_trailing_newline`;
//! and the live wire leg is `api_conformance`'s
//! `PlainTextWriter::TempoFrontendResponse` arm plus the two-sided
//! Tempo-vs-PulsusDB comparison in `e2e/src/traces.rs`'s
//! `assert_rejection_parity`.
//!
//! The container does not vary by status code — the frontend's write path
//! takes the status as a *value* off the `*http.Response` and copies the
//! same (empty) header set regardless — which is what lets the `422`/
//! `500`/`503`/`504` rows of the table below rest on the same rule even
//! though a bad request cannot reach them.
//!
//! `Content-Type` is set explicitly here rather than left to a sniffed
//! value: Go's `DetectContentType` derives `text/plain; charset=utf-8` for
//! every message this surface can produce (they begin `invalid `,
//! `unexpected `, `trace not found`, … — never an HTML token), axum's
//! `String` responder would set the same value anyway, and setting it
//! explicitly is what makes the duplicate-`Content-Type` trap testable.
//!
//! **What this does NOT cover: the routing layer.** Rejections made above
//! the handlers — axum's own 404/405, and the server-wide `TimeoutLayer`'s
//! 408 — are not written by this module. They diverge from the reference,
//! they pre-date #384, and it neither changed nor covers them (the same
//! boundary #264 drew for LogQL).
//!
//! The parse-error byte offset that used to travel in a `position` field
//! now travels inside the message text, as the reference's `line, col`
//! does — asserted per variant in
//! `the_error_table_holds_for_every_api_error_variant`, which is also this
//! file's pin on the table in [`ApiError`]'s doc (issue #266).
//!
//! Which variants a §4.1 fetch can raise is decided by
//! `handlers::trace_by_id_impl`'s call graph rather than here — read it
//! there. As read at this commit it reaches `Param`, `NotFound`,
//! `NotAcceptable`, `Read`, `Assemble` and `PoolUnavailable`.
//!
//! Whether an error may switch to protobuf under `Accept` is settled in
//! docs/api.md §4.1 and exercised on the wire by `api_conformance`'s
//! `assert_traces_fetch_route`, case `absent-404-stays-plain-text` — the
//! fetch route's 404 under both protobuf `Accept` spellings. That same
//! mounted-but-absent body is the suite's mounting oracle for the fetch
//! surface, which is why it stays NON-EMPTY where Tempo's is empty — a
//! named residual, docs/api.md §4.1 and `traces-absent-trace-404-body` in
//! docs/benchmarks/traces-differential-ledger.md.

use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};

use pulsus_clickhouse::ChError;
use pulsus_read::logql::ReadError;
use pulsus_traceql::TraceQlError;

use super::assemble::AssembleError;
use super::legacy::LegacyError;
use super::params::{
    GraphParamError, MetricsParamError, SearchParamError, TagPathError, TagsParamError,
    TraceIdError,
};

/// A `/api/traces/v1` handler's failure, converted to the bare plain-text
/// error body by [`IntoResponse`]. The table is asserted case by case in
/// `the_error_table_holds_for_every_api_error_variant`.
///
/// It is the whole handler-visible failure surface: read the six route
/// modules (`handlers.rs`, `search.rs`, `tags.rs`, `metrics.rs`,
/// `graph.rs`, `compat.rs`) and, at this commit, every route handler
/// either cannot fail (`compat::echo`, a constant 200) or renders its
/// error through this enum. Responses made ABOVE the handlers are not
/// `ApiError`s and are not in the table — the server-wide `TimeoutLayer`'s
/// 408 (`middleware.rs`) and axum's own 404/405 for an unmounted path or
/// method.
///
/// Issue #384 dropped the `errorType` column with the JSON envelope — the
/// status code is the whole machine-readable classification now, exactly
/// as in the reference, whose frontend rejections carry a status and a
/// message and nothing else.
///
/// | variant | HTTP |
/// |---|---|
/// | `Param` / `SearchParam` / `MetricsParam` / `GraphParam` / `TagsParam` / `TagPath` / `QueryText` / `Query` / `Legacy` | 400 |
/// | `Plan` | 400 |
/// | `Plan(MetricsPointCap)` (issue #59 static pre-execution rejection) | 422 |
/// | `NotFound` | 404 |
/// | `NotAcceptable` | 406 |
/// | `Read(…)` | see [`read_error_parts`] — matched exhaustively (issue #266) |
/// | `Assemble(_)` | 500 |
/// | `PoolUnavailable` | 503 |
#[derive(Debug)]
pub(crate) enum ApiError {
    Param(TraceIdError),
    /// Search request-parameter failures (issue #57).
    SearchParam(SearchParamError),
    /// Metrics request-parameter failures (issue #59).
    MetricsParam(MetricsParamError),
    /// Service-graph request-parameter failures (issue #173).
    GraphParam(GraphParamError),
    /// `/tags` request-parameter failures (issue #58).
    TagsParam(TagsParamError),
    /// `{tag}` path-parameter failures (issue #58).
    TagPath(TagPathError),
    /// Legacy `tags` logfmt failures (issue #57).
    Legacy(LegacyError),
    /// TraceQL parse failure — `400`. The parser's own byte offset
    /// travels inside the message, which is where the reference puts its
    /// `line, col` too.
    Query(TraceQlError),
    /// Query-text admission failure raised by a HANDLER rather than
    /// parameter parsing (issue #328): the executed expression parsed
    /// but failed the reference's semantic validation
    /// (`querytext::validate_semantics`). `400`, with the
    /// `invalid TraceQL query: ` wrapping.
    QueryText(super::querytext::QueryTextError),
    /// Search planning failure (unsupported field / type mismatch).
    Plan(pulsus_read::TracePlanError),
    /// The trace has no stored spans (an empty §4.1 fetch).
    NotFound,
    /// RFC 9110: no served representation is acceptable under the
    /// request's `Accept` header (plan v3 §3).
    NotAcceptable,
    Read(ReadError),
    Assemble(AssembleError),
    /// The ClickHouse pool has not been established yet — same "not yet
    /// serving" contract as `logs_api::error::ApiError::PoolUnavailable`
    /// (mirrors `/ready`'s 503).
    PoolUnavailable,
}

impl From<TraceIdError> for ApiError {
    fn from(e: TraceIdError) -> Self {
        ApiError::Param(e)
    }
}

impl From<SearchParamError> for ApiError {
    fn from(e: SearchParamError) -> Self {
        ApiError::SearchParam(e)
    }
}

impl From<super::querytext::QueryTextError> for ApiError {
    fn from(e: super::querytext::QueryTextError) -> Self {
        ApiError::QueryText(e)
    }
}

impl From<MetricsParamError> for ApiError {
    fn from(e: MetricsParamError) -> Self {
        ApiError::MetricsParam(e)
    }
}

impl From<GraphParamError> for ApiError {
    fn from(e: GraphParamError) -> Self {
        ApiError::GraphParam(e)
    }
}

impl From<LegacyError> for ApiError {
    fn from(e: LegacyError) -> Self {
        ApiError::Legacy(e)
    }
}

impl From<TagsParamError> for ApiError {
    fn from(e: TagsParamError) -> Self {
        ApiError::TagsParam(e)
    }
}

impl From<TagPathError> for ApiError {
    fn from(e: TagPathError) -> Self {
        ApiError::TagPath(e)
    }
}

impl From<ReadError> for ApiError {
    fn from(e: ReadError) -> Self {
        ApiError::Read(e)
    }
}

impl From<AssembleError> for ApiError {
    fn from(e: AssembleError) -> Self {
        ApiError::Assemble(e)
    }
}

/// Writes Tempo's user-facing query-error response: the message verbatim,
/// no trailing newline, `text/plain; charset=utf-8`, and **no**
/// `X-Content-Type-Options` (the frontend sets no header at all —
/// `modules/frontend/handler.go:113-116` copying a nil-`Header`
/// `*http.Response`, quoted in this module's header).
///
/// Deliberately NOT `logs_api::error::plain_text_error`, which sets
/// `nosniff` because Loki's `WriteError` does. The two differ by exactly
/// that header; see this module's header for why sharing one responder
/// would be invisible to everything except a live leg.
fn plain_text_error(status: StatusCode, message: String) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        message,
    )
        .into_response()
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        // Every arm's message keeps whatever byte offset it carries INSIDE
        // the text (`pulsus-traceql`'s `at byte {n}`, `LegacyError`'s `at
        // byte {pos}`), which is where it lives now that there is no
        // `position` field — asserted per variant in
        // `the_error_table_holds_for_every_api_error_variant`.
        let (status, message) = match &self {
            ApiError::Param(e) => (StatusCode::BAD_REQUEST, e.to_string()),
            ApiError::SearchParam(e) => (StatusCode::BAD_REQUEST, e.to_string()),
            ApiError::MetricsParam(e) => (StatusCode::BAD_REQUEST, e.to_string()),
            ApiError::GraphParam(e) => (StatusCode::BAD_REQUEST, e.to_string()),
            ApiError::TagsParam(e) => (StatusCode::BAD_REQUEST, e.to_string()),
            ApiError::TagPath(e) => (StatusCode::BAD_REQUEST, e.to_string()),
            ApiError::Legacy(e) => (StatusCode::BAD_REQUEST, e.to_string()),
            ApiError::Query(e) => (StatusCode::BAD_REQUEST, e.to_string()),
            ApiError::QueryText(e) => (StatusCode::BAD_REQUEST, e.to_string()),
            // Issue #59 adjudication: a static pre-execution rejection in
            // the too-broad family — a bounded response, never a silent
            // truncation.
            ApiError::Plan(e @ pulsus_read::TracePlanError::MetricsPointCap { .. }) => {
                (StatusCode::UNPROCESSABLE_ENTITY, e.to_string())
            }
            ApiError::Plan(e) => (StatusCode::BAD_REQUEST, e.to_string()),
            ApiError::NotFound => (StatusCode::NOT_FOUND, "trace not found".to_string()),
            ApiError::NotAcceptable => (
                StatusCode::NOT_ACCEPTABLE,
                "no acceptable representation: this endpoint serves application/json and \
                 application/protobuf"
                    .to_string(),
            ),
            ApiError::Read(e) => read_error_parts(e),
            ApiError::Assemble(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            ApiError::PoolUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "clickhouse pool not yet established".to_string(),
            ),
        };
        plain_text_error(status, message)
    }
}

/// The `ReadError` half of the table above, matched **exhaustively**
/// (issue #266). What that does and does not buy, exactly:
///
/// - Adding a variant to `ReadError` fails the build here (E0004): no
///   arm covers it.
/// - Deleting an arm below, or narrowing one so a variant loses its
///   cover, fails the build the same way.
/// - Making an ALREADY-COVERED variant reachable on this surface does
///   NOT fail the build. A re-route in the call graph compiles silently
///   and takes whatever arm already covers it. That is the case a
///   maintainer actually meets, and it is why every variant below gets a
///   decided mapping: the re-route then lands on a status someone chose,
///   instead of on 500 by omission.
///
/// So do not reintroduce a `_` arm in place of these: a wildcard covers
/// every future variant, which removes the first case above — the one
/// that fires when the change is in another crate.
///
/// Absorbing a variant onto 500 that way is a live hazard, not a tidiness
/// one: Grafana's Tempo datasource proxies our status and body through
/// verbatim — `grafana/grafana-tempo-datasource`
/// `pkg/tempo/tempo.go:370,373` @
/// `3c7375bb541c3acde1deb068ea7ead9ebfdf56b9` (`v13.1.5-11-g3c7375b`)
/// copies the upstream headers, then `rw.WriteHeader(resp.StatusCode)`
/// and `io.Copy(rw, resp.Body)` with no status rewriting. So a rejection
/// that should be 400 arriving as 500 stops being "your query is wrong"
/// and becomes "this datasource is failing": Grafana reports the
/// datasource unhealthy and dependent alert rules go to Error state, over
/// a database that is fine.
///
/// `logs_api::error::read_error_parts` and
/// `prom_api::error::read_error_parts` match `ReadError` the same way,
/// without a wildcard — read either. They did so before this issue too,
/// which is what #266 closed: adding a variant was a build failure on
/// those two surfaces and a silent 500 on this one.
///
/// No arm renders a byte offset, and none could: a [`ReadError`] carries
/// LogQL/PromQL spans, offsets into query texts this surface never
/// receives. That this is a decision and not an omission is asserted in
/// `a_logql_parse_error_body_is_the_inner_error_and_nothing_added`.
fn read_error_parts(e: &ReadError) -> (StatusCode, String) {
    match e {
        // A bounded-response rejection (docs/api.md §4.2-§4.4). Pinned by
        // `query_too_broad_maps_to_422`.
        ReadError::QueryTooBroad(_) => (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()),
        // The engine declined a well-formed query: a bounded-response
        // rejection, never a server fault — the too-broad class docs/api.md
        // §4.2/§4.3/§4.4 documents on 422. Pinned by
        // `a_metrics_engine_decline_maps_to_422_not_500`.
        ReadError::NamelessSelectorUnresolvable { .. } | ReadError::HistogramResultUnsupported => {
            (StatusCode::UNPROCESSABLE_ENTITY, e.to_string())
        }
        // A malformed query is the client's error (issue #240).
        // `Display` is transparent, so `e.to_string()` is the body,
        // undecorated — pinned by
        // `read_error_pipeline_invalid_body_is_the_reason_exactly_once`.
        //
        // STATUS DIVERGENCE, deliberate: the reference answers 500,
        // because the failure happens mid-scan inside its querier. Issue
        // #335 Stage B made this arm reachable (a present NON-boolean
        // operand under `!`), and the divergence is pinned end to end by
        // `traces_search_live::
        // negation_demands_a_boolean_where_truthiness_tolerates_a_string`.
        ReadError::PipelineInvalid { .. } => (StatusCode::BAD_REQUEST, e.to_string()),
        // NOT a malformed request — which is why it is split out of the
        // uniform 400 below. A cancelled evaluation means the awaiting
        // request future was already dropped: a client disconnect, or the
        // server-wide `TimeoutLayer` firing first (`middleware.rs`, itself
        // a 408). 400 would accuse the client of a bad query. 408 is this
        // surface's documented status for the ClickHouse read timeout
        // (docs/api.md §4.1-§4.3). Pinned by
        // `a_cancelled_promql_evaluation_maps_to_408_not_400`.
        ReadError::Promql(pulsus_promql::PromqlError::Cancelled) => {
            (StatusCode::REQUEST_TIMEOUT, e.to_string())
        }
        // A malformed or out-of-domain client query is 400, never a 500
        // that blames the database for it — uniformly, the remaining
        // `Promql` inners included. Pinned by
        // `a_logql_planner_read_error_maps_to_400_not_500` and
        // `a_non_cancellation_promql_error_maps_to_400`.
        //
        // The comparison a reader will want, because the two surfaces
        // differ here: `prom_api` splits the same ten `PromqlError`
        // inners three ways — read `prom_api::error::promql_error_parts`;
        // as read at this commit, two on 400 `bad_data`, seven on 422
        // `execution`, `Cancelled` on 408. This surface keeps the uniform
        // 400 on purpose, and puts its declined-query class on 422 in the
        // arms above (docs/api.md §4.2/§4.3/§4.4).
        ReadError::Parse(_)
        | ReadError::Promql(_)
        | ReadError::EmptyMatcherSet
        | ReadError::ContradictoryMatchers
        | ReadError::InvalidStep
        | ReadError::DurationOutOfRange { .. }
        | ReadError::QuerySpanTooLong { .. }
        | ReadError::MetricPipelineError { .. }
        | ReadError::PipelineUnsupportedInMetric { .. } => (StatusCode::BAD_REQUEST, e.to_string()),
        // A `metric_hist_samples` row that cannot rebuild a histogram is
        // our data-integrity defect, not the request's — a genuine 500.
        // Pinned by `a_histogram_decode_failure_stays_500`.
        ReadError::HistogramDecode(_) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        // The INNERS are enumerated rather than left on a `_` so a new
        // `ChError` variant is a decision here too — do not collapse this
        // to a wildcard. `Connect` keeps the 500 it had before issue #266;
        // re-routing it is a wire change with its own sweep, not a
        // by-product of making the match total, and is held meanwhile by
        // `read_clickhouse_connect_error_still_maps_to_500`.
        ReadError::Clickhouse(ch) => match ch {
            ChError::Timeout(_) => (StatusCode::GATEWAY_TIMEOUT, e.to_string()),
            ChError::Connect(_)
            | ChError::Io(_)
            | ChError::Server { .. }
            | ChError::Decode(_)
            | ChError::Config(_)
            | ChError::InsertUncertain(_) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        },
    }
}

/// Shared error-response assertions for this surface's OTHER route
/// modules' tests (`search`, `metrics`, `tags`, `graph`, `compat`,
/// `handlers`). They live here, next to the responder, so that every
/// module rendering an [`ApiError`] reaches the container assertion
/// through one door — a module that decoded the body itself would be the
/// one hole in the check.
#[cfg(test)]
pub(super) mod testutil {
    use axum::http::{StatusCode, header};
    use axum::response::Response;

    /// Asserts Tempo's frontend error container on a rendered response:
    /// `Content-Type: text/plain; charset=utf-8`, exactly once, and
    /// `X-Content-Type-Options` **absent**.
    ///
    /// The absence is asserted, not merely unmentioned: it is the ONE
    /// property that separates this container from `logs_api`'s
    /// (`pkg/util/server/error.go:49 @ loki v3.7.4` sets `nosniff`;
    /// Tempo's frontend sets no headers at all — see this module's
    /// header). A shared responder would pass every other check here.
    ///
    /// The `Content-Type` COUNT matters because `plain_text_error` sets it
    /// explicitly on top of a `String` body, which sets it too.
    /// `HeaderMap::get` returns the first of a duplicated pair and hides
    /// the second, and so would the conformance suite's header `HashMap`.
    pub(in crate::traces_api) fn assert_reference_container(res: &Response) {
        assert_eq!(
            res.headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("text/plain; charset=utf-8"),
        );
        assert_eq!(
            res.headers().get_all(header::CONTENT_TYPE).iter().count(),
            1,
            "exactly one Content-Type, never a duplicated pair",
        );
        assert_eq!(
            res.headers().get(header::X_CONTENT_TYPE_OPTIONS),
            None,
            "Tempo's query frontend sets no headers at all — nosniff must be ABSENT here, \
             unlike logs_api's WriteError container",
        );
    }

    /// Renders an error [`Response`] to `(status, body)`, asserting the
    /// container via [`assert_reference_container`] on the way through.
    pub(in crate::traces_api) async fn error_body(res: Response) -> (StatusCode, String) {
        let status = res.status();
        assert_reference_container(&res);
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .expect("read body");
        (
            status,
            String::from_utf8(body.to_vec()).expect("utf-8 body"),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::testutil::{assert_reference_container, error_body};
    use super::*;

    async fn rendered(err: ApiError) -> (StatusCode, String) {
        error_body(err.into_response()).await
    }

    /// Issue #384: the exact wire container. The body is the message and
    /// nothing else — no JSON, no trailing newline (the frontend's
    /// `io.Copy(w, resp.Body)`, `modules/frontend/handler.go:116 @
    /// v3.0.2`, appends nothing). Asserted on the raw bytes, not on a
    /// trimmed string, so a stray newline fails.
    ///
    /// Cannot use [`rendered`], which decodes the body to a `String`; it
    /// calls [`assert_reference_container`] directly instead, so this test
    /// covers the headers too rather than being the one hole in them.
    ///
    /// The terminator is a rule the reference gives us that does NOT
    /// discriminate between Tempo's frontend and Loki's `WriteError` —
    /// both write none. It is pinned anyway; `nosniff` is the property
    /// that separates them, and it is in
    /// [`assert_reference_container`].
    #[tokio::test]
    async fn an_error_is_the_bare_message_with_no_json_and_no_trailing_newline() {
        let res = ApiError::Read(ReadError::PipelineInvalid {
            reason: "bad regex: unclosed group".to_string(),
        })
        .into_response();
        assert_reference_container(&res);
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .expect("read body");
        assert_eq!(body.as_ref(), b"bad regex: unclosed group");
        assert!(!body.ends_with(b"\n"), "body must not end with a newline");
        assert!(
            serde_json::from_slice::<serde_json::Value>(&body).is_err(),
            "the body must not be JSON at all"
        );
    }

    /// Issue #240: the LogQL-class rejection is 400 with the BARE reason
    /// as the whole body — restoring a decorating `#[error]` prefix breaks
    /// the byte-exact body assertion.
    #[tokio::test]
    async fn read_error_pipeline_invalid_maps_to_400() {
        let err = ApiError::Read(ReadError::PipelineInvalid {
            reason: "bad regex: unclosed group".to_string(),
        });
        let (status, body) = rendered(err).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body, "bad regex: unclosed group");
    }

    /// Issue #240 (AC2's once-only leg): a reason that itself begins
    /// `parse error ` appears in the rendered body exactly once — no
    /// renderer- or variant-level decoration doubles it.
    #[tokio::test]
    async fn read_error_pipeline_invalid_body_is_the_reason_exactly_once() {
        let reason = "parse error : synthetic prefix-collision probe";
        let err = ApiError::Read(ReadError::PipelineInvalid {
            reason: reason.to_string(),
        });
        let (status, body) = rendered(err).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body, reason);
        assert_eq!(body.matches("parse error ").count(), 1);
    }

    #[tokio::test]
    async fn param_error_maps_to_400() {
        let err = TraceIdError::InvalidLength("abc".to_string());
        let expected = err.to_string();
        let (status, body) = rendered(ApiError::Param(err)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body, expected);
    }

    #[tokio::test]
    async fn tags_param_error_maps_to_400_naming_the_scope() {
        let err = ApiError::TagsParam(TagsParamError::UnsupportedScope("bogus".to_string()));
        let (status, body) = rendered(err).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            body.contains("bogus"),
            "message must name the rejected scope, got {body}"
        );
    }

    #[tokio::test]
    async fn tag_path_error_maps_to_400() {
        let (status, body) = rendered(ApiError::TagPath(TagPathError::EmptyKey)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body, TagPathError::EmptyKey.to_string());
    }

    #[tokio::test]
    async fn not_found_maps_to_404() {
        let (status, body) = rendered(ApiError::NotFound).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body, "trace not found");
    }

    #[tokio::test]
    async fn not_acceptable_maps_to_406() {
        let (status, body) = rendered(ApiError::NotAcceptable).await;
        assert_eq!(status, StatusCode::NOT_ACCEPTABLE);
        assert!(body.contains("no acceptable representation"), "{body}");
    }

    #[tokio::test]
    async fn read_clickhouse_timeout_maps_to_504() {
        let err = ApiError::Read(ReadError::Clickhouse(ChError::Timeout(
            "deadline".to_string(),
        )));
        let (status, _) = rendered(err).await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    }

    #[tokio::test]
    async fn read_other_clickhouse_error_maps_to_500() {
        let err = ApiError::Read(ReadError::Clickhouse(ChError::Decode(
            "bad row".to_string(),
        )));
        let (status, _) = rendered(err).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    /// Issue #266: a connect failure keeps the 500 it had before the
    /// match became exhaustive. Re-routing it is a live wire change and
    /// is deliberately out of scope here.
    #[tokio::test]
    async fn read_clickhouse_connect_error_still_maps_to_500() {
        let err = ApiError::Read(ReadError::Clickhouse(ChError::Connect(
            "refused".to_string(),
        )));
        let (status, _) = rendered(err).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    /// Issue #266: a LogQL planner/pipeline client-input rejection is 400,
    /// not the 500 the removed catch-all gave it.
    #[tokio::test]
    async fn a_logql_planner_read_error_maps_to_400_not_500() {
        for err in [
            ReadError::EmptyMatcherSet,
            ReadError::ContradictoryMatchers,
            ReadError::InvalidStep,
            ReadError::DurationOutOfRange {
                what: "range",
                value: 0,
                max: i64::MAX,
            },
            ReadError::QuerySpanTooLong {
                value: 1,
                max: pulsus_logql::MAX_QUERY_SPAN_NS,
            },
            ReadError::MetricPipelineError {
                error_type: "JSONParserErr".to_string(),
                series: "{a=\"b\"}".to_string(),
            },
            ReadError::PipelineUnsupportedInMetric {
                construct: "| json".to_string(),
            },
        ] {
            let rendered_msg = format!("{err}");
            let (status, body) = rendered(ApiError::Read(err)).await;
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "expected 400 for {rendered_msg}, got body {body}"
            );
            assert_eq!(body, rendered_msg);
        }
    }

    /// Issue #266 review round 1: a cancelled PromQL evaluation is a
    /// dropped request future (client disconnect, or the server's own 408
    /// `TimeoutLayer`), never malformed input — 408, not the uniform 400
    /// of the arm below it.
    #[tokio::test]
    async fn a_cancelled_promql_evaluation_maps_to_408_not_400() {
        let err = ApiError::Read(ReadError::Promql(pulsus_promql::PromqlError::Cancelled));
        let (status, _) = rendered(err).await;
        assert_eq!(status, StatusCode::REQUEST_TIMEOUT);
    }

    /// Issue #266: a non-cancellation `Promql` inner takes the uniform 400
    /// of the arm it falls into.
    #[tokio::test]
    async fn a_non_cancellation_promql_error_maps_to_400() {
        let err = ApiError::Read(ReadError::Promql(pulsus_promql::PromqlError::Unsupported {
            construct: "the @ modifier".to_string(),
        }));
        let (status, _) = rendered(err).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    /// Issue #266 review round 1, restated for #384: [`ReadError::Parse`]
    /// carries a **LogQL** span, an offset into a query text this surface
    /// never receives, so this surface must never RE-EXPOSE it as an
    /// offset of its own. Before #384 that meant the envelope's `position`
    /// field had to stay absent; now [`read_error_parts`] returns only
    /// `(status, message)`, so there is no channel for it at all and the
    /// property holds structurally.
    ///
    /// What the body carries instead is the inner error's own rendering,
    /// verbatim — including whatever `at byte N` the LOGQL parser writes
    /// into its own message, which was equally true of the old envelope's
    /// `error` string. That is the thing asserted here: the body is
    /// `ReadError::Parse`'s `Display` and nothing added, so no traces-level
    /// offset can be grafted on without failing this.
    #[tokio::test]
    async fn a_logql_parse_error_body_is_the_inner_error_and_nothing_added() {
        let inner = pulsus_logql::parse("{").expect_err("must fail");
        assert!(
            inner.span().end <= "{".len(),
            "the span must be an offset into the LOGQL text, got {:?}",
            inner.span()
        );
        let read_err = ReadError::Parse(inner);
        let expected = read_err.to_string();
        let (status, body) = rendered(ApiError::Read(read_err)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body, expected);
    }

    /// Issue #266: the two metrics-path "engine declines a well-formed
    /// query" variants take this surface's bounded-response family — 422,
    /// the status docs/api.md §4.2/§4.3/§4.4 documents for traces — not
    /// the removed catch-all's 500.
    #[tokio::test]
    async fn a_metrics_engine_decline_maps_to_422_not_500() {
        for err in [
            ReadError::NamelessSelectorUnresolvable {
                reason: "ColdCache".to_string(),
            },
            ReadError::HistogramResultUnsupported,
        ] {
            let rendered_msg = format!("{err}");
            let (status, body) = rendered(ApiError::Read(err)).await;
            assert_eq!(
                status,
                StatusCode::UNPROCESSABLE_ENTITY,
                "expected 422 for {rendered_msg}, got body {body}"
            );
        }
    }

    /// Issue #266: a histogram-decode failure genuinely IS a 500 — our
    /// stored row is malformed, the client's request was not.
    #[tokio::test]
    async fn a_histogram_decode_failure_stays_500() {
        let err = ReadError::HistogramDecode(pulsus_model::HistogramError::SchemaOutOfRange(200));
        let (status, _) = rendered(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn assemble_unsupported_payload_type_maps_to_500_naming_the_count() {
        let err = ApiError::Assemble(AssembleError::UnsupportedPayloadType { count: 3 });
        let (status, body) = rendered(err).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(
            body.contains("3"),
            "message must name the count, got {body}"
        );
    }

    #[tokio::test]
    async fn pool_unavailable_maps_to_503() {
        let (status, body) = rendered(ApiError::PoolUnavailable).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, "clickhouse pool not yet established");
    }

    #[tokio::test]
    async fn a_traceql_parse_error_maps_to_400_with_its_offset_in_the_message() {
        let err = pulsus_traceql::parse("{ ").expect_err("must fail");
        let want_offset = format!("byte {}", err.span().start);
        let expected = err.to_string();
        let (status, body) = rendered(ApiError::Query(err)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body, expected);
        assert!(body.contains(&want_offset), "body {body}");
    }

    #[tokio::test]
    async fn query_too_broad_maps_to_422() {
        let err = ApiError::Read(ReadError::QueryTooBroad(
            pulsus_read::logql::TooBroadReason::TraceScanBudgetRows { budget_rows: 42 },
        ));
        let (status, _) = rendered(err).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn a_logfmt_error_maps_to_400_with_its_tags_offset_in_the_message() {
        let err = ApiError::Legacy(LegacyError::UnquotedEquals {
            key: "a".to_string(),
            pos: 3,
        });
        let (status, body) = rendered(err).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("byte 3"), "body {body}");
    }

    /// Issue #266, carried across #384: where [`ApiError`]'s doc table and
    /// the byte-offset rule are asserted rather than restated in prose. A
    /// case per variant of the enum, plus the two splits the table calls
    /// out (`Plan`'s point cap, and `QueryText`'s inners under both
    /// carriers).
    ///
    /// Per case it asserts: the status; that the WHOLE body is the
    /// variant's own `Display` and nothing else; that the body does not
    /// parse as JSON; and whether the body carries the variant's byte
    /// offset as the literal `byte {n}` — the column that replaced the old
    /// `position` field when #384 dropped the envelope. Every case reaches
    /// [`assert_reference_container`] through [`rendered`], so the header
    /// rules hold for the whole table too.
    ///
    /// It does not fail when a variant is ADDED: a list of cases cannot
    /// notice a variant nobody listed. What the compiler forces on a new
    /// variant is a MAPPING — [`IntoResponse`]'s own exhaustive `match`,
    /// and [`read_error_parts`]'s for `ReadError`; adding the case here is
    /// on whoever adds the variant.
    #[tokio::test]
    async fn the_error_table_holds_for_every_api_error_variant() {
        use super::super::querytext::QueryTextError;

        // Built the way production builds it — from a real parse
        // failure — because the assertion below is that the parser's own
        // `at byte {n}` survives into the wire body. A synthetic message
        // would assert nothing.
        let invalid = || QueryTextError::Invalid {
            message: pulsus_traceql::parse("{ .a = }")
                .expect_err("must fail")
                .to_string(),
        };
        let invalid_offset = pulsus_traceql::parse("{ .a = }")
            .expect_err("must fail")
            .span()
            .start;
        let semantic = || {
            QueryTextError::Semantic(pulsus_traceql::ValidateError::TypeMismatch {
                expr: "1 = `a`".to_string(),
            })
        };
        let too_long = || QueryTextError::TooLong { len: 9, cap: 8 };
        let traceql_err = || pulsus_traceql::parse("{ ").expect_err("must fail");
        let legacy_err = || LegacyError::UnquotedEquals {
            key: "a".to_string(),
            pos: 3,
        };

        // (case, error, status, the byte offset the MESSAGE must carry)
        let cases: Vec<(&str, ApiError, StatusCode, Option<usize>)> = vec![
            (
                "Param",
                ApiError::Param(TraceIdError::InvalidLength("abc".to_string())),
                StatusCode::BAD_REQUEST,
                None,
            ),
            (
                "SearchParam",
                ApiError::SearchParam(SearchParamError::ConflictingQuery),
                StatusCode::BAD_REQUEST,
                None,
            ),
            (
                "SearchParam(QueryText(Invalid))",
                ApiError::SearchParam(SearchParamError::QueryText(invalid())),
                StatusCode::BAD_REQUEST,
                Some(invalid_offset),
            ),
            (
                "SearchParam(QueryText(TooLong))",
                ApiError::SearchParam(SearchParamError::QueryText(too_long())),
                StatusCode::BAD_REQUEST,
                None,
            ),
            (
                "SearchParam(QueryText(Semantic))",
                ApiError::SearchParam(SearchParamError::QueryText(semantic())),
                StatusCode::BAD_REQUEST,
                None,
            ),
            (
                "MetricsParam",
                ApiError::MetricsParam(MetricsParamError::InvalidStep("500ms".to_string())),
                StatusCode::BAD_REQUEST,
                None,
            ),
            (
                "GraphParam",
                ApiError::GraphParam(GraphParamError::MissingRange),
                StatusCode::BAD_REQUEST,
                None,
            ),
            (
                "TagsParam",
                ApiError::TagsParam(TagsParamError::UnsupportedScope("bogus".to_string())),
                StatusCode::BAD_REQUEST,
                None,
            ),
            (
                "TagPath",
                ApiError::TagPath(TagPathError::EmptyKey),
                StatusCode::BAD_REQUEST,
                None,
            ),
            (
                "Legacy",
                ApiError::Legacy(legacy_err()),
                StatusCode::BAD_REQUEST,
                Some(3),
            ),
            (
                "Query",
                ApiError::Query(traceql_err()),
                StatusCode::BAD_REQUEST,
                Some(traceql_err().span().start),
            ),
            (
                "QueryText(Invalid)",
                ApiError::QueryText(invalid()),
                StatusCode::BAD_REQUEST,
                Some(invalid_offset),
            ),
            (
                "QueryText(TooLong)",
                ApiError::QueryText(too_long()),
                StatusCode::BAD_REQUEST,
                None,
            ),
            (
                "QueryText(Semantic)",
                ApiError::QueryText(semantic()),
                StatusCode::BAD_REQUEST,
                None,
            ),
            (
                "Plan",
                ApiError::Plan(pulsus_read::TracePlanError::TypeMismatch(
                    "status supports only = and !=".to_string(),
                )),
                StatusCode::BAD_REQUEST,
                None,
            ),
            (
                "Plan(MetricsPointCap)",
                ApiError::Plan(pulsus_read::TracePlanError::MetricsPointCap {
                    buckets: 12_000,
                    cap: 11_000,
                }),
                StatusCode::UNPROCESSABLE_ENTITY,
                None,
            ),
            ("NotFound", ApiError::NotFound, StatusCode::NOT_FOUND, None),
            (
                "NotAcceptable",
                ApiError::NotAcceptable,
                StatusCode::NOT_ACCEPTABLE,
                None,
            ),
            (
                "Read",
                ApiError::Read(ReadError::EmptyMatcherSet),
                StatusCode::BAD_REQUEST,
                None,
            ),
            (
                "Assemble",
                ApiError::Assemble(AssembleError::UnsupportedPayloadType { count: 3 }),
                StatusCode::INTERNAL_SERVER_ERROR,
                None,
            ),
            (
                "PoolUnavailable",
                ApiError::PoolUnavailable,
                StatusCode::SERVICE_UNAVAILABLE,
                None,
            ),
        ];

        for (case, err, want_status, want_offset) in cases {
            // The whole body must be this variant's own rendering, so the
            // expectation is taken from the error itself before it moves.
            let want_body = match &err {
                ApiError::NotFound => "trace not found".to_string(),
                ApiError::NotAcceptable => {
                    "no acceptable representation: this endpoint serves application/json and \
                     application/protobuf"
                        .to_string()
                }
                ApiError::PoolUnavailable => "clickhouse pool not yet established".to_string(),
                ApiError::Param(e) => e.to_string(),
                ApiError::SearchParam(e) => e.to_string(),
                ApiError::MetricsParam(e) => e.to_string(),
                ApiError::GraphParam(e) => e.to_string(),
                ApiError::TagsParam(e) => e.to_string(),
                ApiError::TagPath(e) => e.to_string(),
                ApiError::Legacy(e) => e.to_string(),
                ApiError::Query(e) => e.to_string(),
                ApiError::QueryText(e) => e.to_string(),
                ApiError::Plan(e) => e.to_string(),
                ApiError::Read(e) => e.to_string(),
                ApiError::Assemble(e) => e.to_string(),
            };
            let (status, body) = rendered(err).await;
            assert_eq!(status, want_status, "{case}: status, body {body}");
            assert_eq!(body, want_body, "{case}: the body is the message, verbatim");
            assert!(
                serde_json::from_str::<serde_json::Value>(&body).is_err(),
                "{case}: the body must not parse as JSON, got {body}"
            );
            match want_offset {
                Some(n) => assert!(
                    body.contains(&format!("byte {n}")),
                    "{case}: the offset must survive in the message as `byte {n}`, got {body}"
                ),
                None => assert!(
                    !body.contains("byte "),
                    "{case}: no offset may appear in the message, got {body}"
                ),
            }
        }
    }

    #[tokio::test]
    async fn a_plan_error_maps_to_400() {
        let err =
            pulsus_read::TracePlanError::TypeMismatch("status supports only = and !=".to_string());
        let expected = err.to_string();
        let (status, body) = rendered(ApiError::Plan(err)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body, expected);
    }

    #[tokio::test]
    async fn a_metrics_param_error_maps_to_400_naming_the_step() {
        let err = ApiError::MetricsParam(MetricsParamError::InvalidStep("500ms".to_string()));
        let (status, body) = rendered(err).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            body.contains("500ms"),
            "message must name the rejected step, got {body}"
        );
    }

    #[tokio::test]
    async fn the_metrics_point_cap_plan_error_maps_to_422() {
        // Issue #59 adjudication: a static pre-execution rejection, not
        // the 400 plan family.
        let err = ApiError::Plan(pulsus_read::TracePlanError::MetricsPointCap {
            buckets: 12_000,
            cap: 11_000,
        });
        let (status, body) = rendered(err).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(
            body.contains("12000"),
            "message must name the bucket count, got {body}"
        );
    }

    #[tokio::test]
    async fn the_metrics_set_budget_reason_maps_to_422() {
        let err = ApiError::Read(ReadError::QueryTooBroad(
            pulsus_read::logql::TooBroadReason::TraceMetricsSetRows {
                max_set_rows: 1_000_000,
            },
        ));
        let (status, _) = rendered(err).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    }

    /// Issue #57 re-audit AC-conformance: the generator-memory reason is
    /// covered by `ApiError::Read`'s `QueryTooBroad(_)` arm — no dedicated
    /// match arm was needed — and names its reason in the body.
    #[tokio::test]
    async fn the_generator_memory_reason_maps_to_422() {
        let err = ApiError::Read(ReadError::QueryTooBroad(
            pulsus_read::logql::TooBroadReason::TraceGeneratorMemory {
                budget_bytes: 1_048_576,
            },
        ));
        let (status, body) = rendered(err).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(
            body.contains("generator memory"),
            "message must name the reason, got {body}"
        );
    }
}
