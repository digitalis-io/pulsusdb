//! `/api/traces/v1`'s error envelope: `{"status":"error","errorType",
//! "error","position"?}` (docs/api.md §4.1/§4.2), and the status-code
//! mapping table pinned by the issue #55 plan (v2's error table + v3's
//! `406 not_acceptable`) plus issue #57's search rows. Mirrors
//! `logs_api/error.rs`'s structure.
//!
//! Which variants render a `position` byte offset, and which do not, is
//! asserted in `the_envelope_table_holds_for_every_api_error_variant`
//! below — which is also this file's pin on the table in [`ApiError`]'s
//! doc (issue #266).
//!
//! Whether an error may switch to protobuf under `Accept` is settled in
//! docs/api.md §4.1 and exercised on the wire by `api_conformance`'s
//! `assert_traces_fetch_route`, case `absent-404-stays-json` — the fetch
//! route's 404 under both protobuf `Accept` spellings, asserting the JSON
//! content type. That same mounted-but-absent envelope is the suite's
//! mounting oracle for the fetch surface.
//!
//! Which variants a §4.1 fetch can raise is decided by
//! `handlers::trace_by_id_impl`'s call graph rather than here — read it
//! there. As read at this commit it reaches `Param`, `NotFound`,
//! `NotAcceptable`, `Read`, `Assemble` and `PoolUnavailable`, none of
//! which renders a `position`, so a fetch error carries none.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use pulsus_clickhouse::ChError;
use pulsus_read::logql::ReadError;
use pulsus_traceql::TraceQlError;

use super::assemble::AssembleError;
use super::legacy::LegacyError;
use super::params::{
    GraphParamError, MetricsParamError, SearchParamError, TagPathError, TagsParamError,
    TraceIdError,
};

/// A `/api/traces/v1` handler's failure, converted to the documented
/// error envelope by [`IntoResponse`]. The table is asserted case by case
/// in `the_envelope_table_holds_for_every_api_error_variant`.
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
/// | variant | HTTP | `errorType` |
/// |---|---|---|
/// | `Param` / `SearchParam` / `MetricsParam` / `GraphParam` / `TagsParam` / `TagPath` / `QueryText` / `Query` / `Legacy` | 400 | `bad_data` |
/// | `Plan` | 400 | `bad_data` |
/// | `Plan(MetricsPointCap)` (issue #59 static pre-execution rejection) | 422 | `query_too_broad` |
/// | `NotFound` | 404 | `not_found` |
/// | `NotAcceptable` | 406 | `not_acceptable` |
/// | `Read(…)` | see [`read_error_parts`] — matched exhaustively (issue #266) |
/// | `Assemble(_)` | 500 | `internal` |
/// | `PoolUnavailable` | 503 | `unavailable` |
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
    /// TraceQL parse failure — `400 bad_data`, matching the LogQL
    /// parse-error envelope.
    Query(TraceQlError),
    /// Query-text admission failure raised by a HANDLER rather than
    /// parameter parsing (issue #328): the executed expression parsed
    /// but failed the reference's semantic validation
    /// (`querytext::validate_semantics`). `400 bad_data`, with the
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

#[derive(Serialize)]
struct ErrorEnvelope {
    status: &'static str,
    #[serde(rename = "errorType")]
    error_type: &'static str,
    error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    position: Option<usize>,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, error_type, message, position) = match &self {
            ApiError::Param(e) => (StatusCode::BAD_REQUEST, "bad_data", e.to_string(), None),
            ApiError::SearchParam(e) => (
                StatusCode::BAD_REQUEST,
                "bad_data",
                e.to_string(),
                e.position(),
            ),
            ApiError::MetricsParam(e) => (StatusCode::BAD_REQUEST, "bad_data", e.to_string(), None),
            ApiError::GraphParam(e) => (StatusCode::BAD_REQUEST, "bad_data", e.to_string(), None),
            ApiError::TagsParam(e) => (StatusCode::BAD_REQUEST, "bad_data", e.to_string(), None),
            ApiError::TagPath(e) => (StatusCode::BAD_REQUEST, "bad_data", e.to_string(), None),
            // The offset indexes the decoded `tags` value, not a query
            // expression (docs/api.md §4.2).
            ApiError::Legacy(e) => (
                StatusCode::BAD_REQUEST,
                "bad_data",
                e.to_string(),
                Some(e.pos()),
            ),
            ApiError::Query(e) => (
                StatusCode::BAD_REQUEST,
                "bad_data",
                e.to_string(),
                Some(e.span().start),
            ),
            ApiError::QueryText(e) => (
                StatusCode::BAD_REQUEST,
                "bad_data",
                e.to_string(),
                e.position(),
            ),
            // Issue #59 adjudication: a static pre-execution rejection in
            // the too-broad family — a bounded response, never a silent
            // truncation.
            ApiError::Plan(e @ pulsus_read::TracePlanError::MetricsPointCap { .. }) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "query_too_broad",
                e.to_string(),
                None,
            ),
            ApiError::Plan(e) => (StatusCode::BAD_REQUEST, "bad_data", e.to_string(), None),
            ApiError::NotFound => (
                StatusCode::NOT_FOUND,
                "not_found",
                "trace not found".to_string(),
                None,
            ),
            ApiError::NotAcceptable => (
                StatusCode::NOT_ACCEPTABLE,
                "not_acceptable",
                "no acceptable representation: this endpoint serves application/json and \
                 application/protobuf"
                    .to_string(),
                None,
            ),
            ApiError::Read(e) => {
                let (status, error_type, message) = read_error_parts(e);
                (status, error_type, message, None)
            }
            ApiError::Assemble(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                e.to_string(),
                None,
            ),
            ApiError::PoolUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                "clickhouse pool not yet established".to_string(),
                None,
            ),
        };
        let body = ErrorEnvelope {
            status: "error",
            error_type,
            error: message,
            position,
        };
        (status, Json(body)).into_response()
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
/// No arm returns an offset, and the signature is why — a 3-tuple, with
/// the call site supplying `None`. That this is a decision and not an
/// omission is asserted in
/// `a_logql_parse_error_renders_no_traces_position`: [`ReadError::Parse`]
/// carries a LogQL span, an offset into a query text this surface never
/// receives.
fn read_error_parts(e: &ReadError) -> (StatusCode, &'static str, String) {
    match e {
        // A bounded-response rejection (docs/api.md §4.2-§4.4). Pinned by
        // `query_too_broad_maps_to_422_query_too_broad`.
        ReadError::QueryTooBroad(_) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            "query_too_broad",
            e.to_string(),
        ),
        // The engine declined a well-formed query: a bounded-response
        // rejection, never a server fault. `query_too_broad` is the name
        // the traces taxonomy gives that class (docs/api.md
        // §4.2/§4.3/§4.4). Pinned by
        // `a_metrics_engine_decline_maps_to_422_query_too_broad_not_500`.
        ReadError::NamelessSelectorUnresolvable { .. } | ReadError::HistogramResultUnsupported => (
            StatusCode::UNPROCESSABLE_ENTITY,
            "query_too_broad",
            e.to_string(),
        ),
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
        ReadError::PipelineInvalid { .. } => (StatusCode::BAD_REQUEST, "bad_data", e.to_string()),
        // NOT a malformed request — which is why it is split out of the
        // uniform 400 below. A cancelled evaluation means the awaiting
        // request future was already dropped: a client disconnect, or the
        // server-wide `TimeoutLayer` firing first (`middleware.rs`, itself
        // a 408). 400 `bad_data` would accuse the client of a bad query.
        // `timeout` is this surface's documented `errorType` for the
        // ClickHouse read timeout (docs/api.md §4.1-§4.3). Pinned by
        // `a_cancelled_promql_evaluation_maps_to_408_timeout_not_400_bad_data`.
        ReadError::Promql(pulsus_promql::PromqlError::Cancelled) => {
            (StatusCode::REQUEST_TIMEOUT, "timeout", e.to_string())
        }
        // A malformed or out-of-domain client query is 400 `bad_data`,
        // never a 500 that blames the database for it — uniformly, the
        // remaining `Promql` inners included. Pinned by
        // `a_logql_planner_read_error_maps_to_400_bad_data_not_500` and
        // `a_non_cancellation_promql_error_maps_to_400_bad_data`.
        //
        // The comparison a reader will want, because the two surfaces
        // differ here: `prom_api` splits the same ten `PromqlError`
        // inners three ways — read `prom_api::error::promql_error_parts`;
        // as read at this commit, two on 400 `bad_data`, seven on 422
        // `execution`, `Cancelled` on 408. This surface keeps the uniform
        // 400 on purpose, and spells its own declined-query class
        // `query_too_broad` (docs/api.md §4.2/§4.3/§4.4) in the arms
        // above.
        ReadError::Parse(_)
        | ReadError::Promql(_)
        | ReadError::EmptyMatcherSet
        | ReadError::ContradictoryMatchers
        | ReadError::InvalidStep
        | ReadError::DurationOutOfRange { .. }
        | ReadError::QuerySpanTooLong { .. }
        | ReadError::MetricPipelineError { .. }
        | ReadError::PipelineUnsupportedInMetric { .. } => {
            (StatusCode::BAD_REQUEST, "bad_data", e.to_string())
        }
        // A `metric_hist_samples` row that cannot rebuild a histogram is
        // our data-integrity defect, not the request's — a genuine 500.
        // Pinned by `a_histogram_decode_failure_stays_500_internal`.
        ReadError::HistogramDecode(_) => {
            (StatusCode::INTERNAL_SERVER_ERROR, "internal", e.to_string())
        }
        // The INNERS are enumerated rather than left on a `_` so a new
        // `ChError` variant is a decision here too — do not collapse this
        // to a wildcard. `Connect` keeps the 500 it had before issue #266;
        // re-routing it is a wire change with its own sweep, not a
        // by-product of making the match total, and is held meanwhile by
        // `read_clickhouse_connect_error_still_maps_to_500_internal`.
        ReadError::Clickhouse(ch) => match ch {
            ChError::Timeout(_) => (StatusCode::GATEWAY_TIMEOUT, "timeout", e.to_string()),
            ChError::Connect(_)
            | ChError::Io(_)
            | ChError::Server { .. }
            | ChError::Decode(_)
            | ChError::Config(_)
            | ChError::InsertUncertain(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "internal", e.to_string())
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn envelope(err: ApiError) -> (StatusCode, serde_json::Value) {
        let res = err.into_response();
        let status = res.status();
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .expect("read body");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("valid json");
        (status, json)
    }

    /// Issue #240: the LogQL-class rejection is 400 `bad_data` with the
    /// BARE reason as the whole body — restoring a decorating `#[error]`
    /// prefix breaks the byte-exact `error`-field assertion.
    #[tokio::test]
    async fn read_error_pipeline_invalid_maps_to_400_bad_data() {
        let err = ApiError::Read(ReadError::PipelineInvalid {
            reason: "bad regex: unclosed group".to_string(),
        });
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["status"], "error");
        assert_eq!(json["errorType"], "bad_data");
        assert!(json.get("position").is_none());
        assert_eq!(json["error"], "bad regex: unclosed group");
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
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let body = json["error"].as_str().expect("error body");
        assert_eq!(body, reason);
        assert_eq!(body.matches("parse error ").count(), 1);
    }

    #[tokio::test]
    async fn param_error_maps_to_400_bad_data() {
        let err = ApiError::Param(TraceIdError::InvalidLength("abc".to_string()));
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["status"], "error");
        assert_eq!(json["errorType"], "bad_data");
        assert!(json.get("position").is_none());
    }

    #[tokio::test]
    async fn tags_param_error_maps_to_400_bad_data_without_a_position() {
        let err = ApiError::TagsParam(TagsParamError::UnsupportedScope("bogus".to_string()));
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["status"], "error");
        assert_eq!(json["errorType"], "bad_data");
        assert!(json.get("position").is_none());
        assert!(
            json["error"].as_str().is_some_and(|m| m.contains("bogus")),
            "message must name the rejected scope, got {json}"
        );
    }

    #[tokio::test]
    async fn tag_path_error_maps_to_400_bad_data_without_a_position() {
        let err = ApiError::TagPath(TagPathError::EmptyKey);
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(json.get("position").is_none());
    }

    #[tokio::test]
    async fn not_found_maps_to_404_not_found() {
        let (status, json) = envelope(ApiError::NotFound).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(json["errorType"], "not_found");
    }

    #[tokio::test]
    async fn not_acceptable_maps_to_406_not_acceptable() {
        let (status, json) = envelope(ApiError::NotAcceptable).await;
        assert_eq!(status, StatusCode::NOT_ACCEPTABLE);
        assert_eq!(json["errorType"], "not_acceptable");
    }

    #[tokio::test]
    async fn read_clickhouse_timeout_maps_to_504_timeout() {
        let err = ApiError::Read(ReadError::Clickhouse(ChError::Timeout(
            "deadline".to_string(),
        )));
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(json["errorType"], "timeout");
    }

    #[tokio::test]
    async fn read_other_clickhouse_error_maps_to_500_internal() {
        let err = ApiError::Read(ReadError::Clickhouse(ChError::Decode(
            "bad row".to_string(),
        )));
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(json["errorType"], "internal");
    }

    /// Issue #266: a connect failure keeps the 500 it had before the
    /// match became exhaustive. Re-routing it is a live wire change and
    /// is deliberately out of scope here.
    #[tokio::test]
    async fn read_clickhouse_connect_error_still_maps_to_500_internal() {
        let err = ApiError::Read(ReadError::Clickhouse(ChError::Connect(
            "refused".to_string(),
        )));
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(json["errorType"], "internal");
    }

    /// Issue #266: a LogQL planner/pipeline client-input rejection is 400
    /// `bad_data`, not the 500 the removed catch-all gave it.
    #[tokio::test]
    async fn a_logql_planner_read_error_maps_to_400_bad_data_not_500() {
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
            let rendered = format!("{err}");
            let (status, json) = envelope(ApiError::Read(err)).await;
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "expected 400 for {rendered}, got {json}"
            );
            assert_eq!(json["errorType"], "bad_data", "for {rendered}");
            assert!(json.get("position").is_none(), "for {rendered}");
        }
    }

    /// Issue #266 review round 1: a cancelled PromQL evaluation is a
    /// dropped request future (client disconnect, or the server's own 408
    /// `TimeoutLayer`), never malformed input — 408 `timeout`, not the
    /// uniform 400 `bad_data` of the arm below it.
    #[tokio::test]
    async fn a_cancelled_promql_evaluation_maps_to_408_timeout_not_400_bad_data() {
        let err = ApiError::Read(ReadError::Promql(pulsus_promql::PromqlError::Cancelled));
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::REQUEST_TIMEOUT);
        assert_eq!(json["errorType"], "timeout");
        assert!(json.get("position").is_none(), "body {json}");
    }

    /// Issue #266: a non-cancellation `Promql` inner takes the uniform 400
    /// `bad_data` of the arm it falls into, using the `errorType` vocabulary
    /// docs/api.md §4.1 defines for traces.
    #[tokio::test]
    async fn a_non_cancellation_promql_error_maps_to_400_bad_data() {
        let err = ApiError::Read(ReadError::Promql(pulsus_promql::PromqlError::Unsupported {
            construct: "the @ modifier".to_string(),
        }));
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(json.get("position").is_none());
    }

    /// Issue #266 review round 1: [`ReadError::Parse`] carries a **LogQL**
    /// span, an offset into a query text this surface never receives, so
    /// it must never render a traces `position`. The renderer guarantees
    /// that structurally — `read_error_parts` returns a 3-tuple and the
    /// call site supplies `None` — and this pins it against a widening.
    #[tokio::test]
    async fn a_logql_parse_error_renders_no_traces_position() {
        let inner = pulsus_logql::parse("{").expect_err("must fail");
        assert!(
            inner.span().end <= "{".len(),
            "the span must be an offset into the LOGQL text, got {:?}",
            inner.span()
        );
        let (status, json) = envelope(ApiError::Read(ReadError::Parse(inner))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(json.get("position").is_none(), "body {json}");
    }

    /// Issue #266: the two metrics-path "engine declines a well-formed
    /// query" variants take this surface's bounded-response family — 422
    /// `query_too_broad`, the `errorType` docs/api.md §4.2/§4.3/§4.4
    /// defines for traces — not the removed catch-all's 500.
    #[tokio::test]
    async fn a_metrics_engine_decline_maps_to_422_query_too_broad_not_500() {
        for err in [
            ReadError::NamelessSelectorUnresolvable {
                reason: "ColdCache".to_string(),
            },
            ReadError::HistogramResultUnsupported,
        ] {
            let rendered = format!("{err}");
            let (status, json) = envelope(ApiError::Read(err)).await;
            assert_eq!(
                status,
                StatusCode::UNPROCESSABLE_ENTITY,
                "expected 422 for {rendered}, got {json}"
            );
            assert_eq!(json["errorType"], "query_too_broad", "for {rendered}");
        }
    }

    /// Issue #266: a histogram-decode failure genuinely IS a 500 — our
    /// stored row is malformed, the client's request was not.
    #[tokio::test]
    async fn a_histogram_decode_failure_stays_500_internal() {
        let err = ReadError::HistogramDecode(pulsus_model::HistogramError::SchemaOutOfRange(200));
        let (status, json) = envelope(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(json["errorType"], "internal");
    }

    #[tokio::test]
    async fn assemble_unsupported_payload_type_maps_to_500_internal_naming_the_count() {
        let err = ApiError::Assemble(AssembleError::UnsupportedPayloadType { count: 3 });
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(json["errorType"], "internal");
        assert!(
            json["error"].as_str().is_some_and(|m| m.contains("3")),
            "message must name the count, got {json}"
        );
    }

    #[tokio::test]
    async fn pool_unavailable_maps_to_503() {
        let (status, json) = envelope(ApiError::PoolUnavailable).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }

    #[tokio::test]
    async fn a_traceql_parse_error_maps_to_400_bad_data_with_a_position() {
        let err = pulsus_traceql::parse("{ ").expect_err("must fail");
        let (status, json) = envelope(ApiError::Query(err)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(json["position"].is_u64(), "body {json}");
    }

    #[tokio::test]
    async fn query_too_broad_maps_to_422_query_too_broad() {
        let err = ApiError::Read(ReadError::QueryTooBroad(
            pulsus_read::logql::TooBroadReason::TraceScanBudgetRows { budget_rows: 42 },
        ));
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json["errorType"], "query_too_broad");
        assert!(json.get("position").is_none());
    }

    #[tokio::test]
    async fn a_logfmt_error_maps_to_400_bad_data_with_its_tags_offset() {
        let err = ApiError::Legacy(LegacyError::UnquotedEquals {
            key: "a".to_string(),
            pos: 3,
        });
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert_eq!(json["position"], 3, "body {json}");
    }

    /// Issue #266: where [`ApiError`]'s doc table and the `position` rule
    /// are asserted rather than restated in prose. A case per variant of
    /// the enum, plus the two splits the table calls out (`Plan`'s point
    /// cap, and `QueryText`'s inners under both carriers), each checking
    /// status, `errorType` and whether a `position` offset is rendered.
    ///
    /// It does not fail when a variant is ADDED: a list of cases cannot
    /// notice a variant nobody listed. What the compiler forces on a new
    /// variant is a MAPPING — [`IntoResponse`]'s own exhaustive `match`,
    /// and [`read_error_parts`]'s for `ReadError`; adding the case here is
    /// on whoever adds the variant.
    #[tokio::test]
    async fn the_envelope_table_holds_for_every_api_error_variant() {
        use super::super::querytext::QueryTextError;

        let invalid = || QueryTextError::Invalid {
            message: "syntax error".to_string(),
            position: 7,
        };
        let semantic = || {
            QueryTextError::Semantic(pulsus_traceql::ValidateError::TypeMismatch {
                expr: "1 = `a`".to_string(),
            })
        };
        let too_long = || QueryTextError::TooLong { len: 9, cap: 8 };

        // (case, error, status, errorType, renders a `position`)
        let cases: Vec<(&str, ApiError, StatusCode, &str, bool)> = vec![
            (
                "Param",
                ApiError::Param(TraceIdError::InvalidLength("abc".to_string())),
                StatusCode::BAD_REQUEST,
                "bad_data",
                false,
            ),
            (
                "SearchParam",
                ApiError::SearchParam(SearchParamError::ConflictingQuery),
                StatusCode::BAD_REQUEST,
                "bad_data",
                false,
            ),
            (
                "SearchParam(QueryText(Invalid))",
                ApiError::SearchParam(SearchParamError::QueryText(invalid())),
                StatusCode::BAD_REQUEST,
                "bad_data",
                true,
            ),
            (
                "SearchParam(QueryText(TooLong))",
                ApiError::SearchParam(SearchParamError::QueryText(too_long())),
                StatusCode::BAD_REQUEST,
                "bad_data",
                false,
            ),
            (
                "SearchParam(QueryText(Semantic))",
                ApiError::SearchParam(SearchParamError::QueryText(semantic())),
                StatusCode::BAD_REQUEST,
                "bad_data",
                false,
            ),
            (
                "MetricsParam",
                ApiError::MetricsParam(MetricsParamError::InvalidStep("500ms".to_string())),
                StatusCode::BAD_REQUEST,
                "bad_data",
                false,
            ),
            (
                "GraphParam",
                ApiError::GraphParam(GraphParamError::MissingRange),
                StatusCode::BAD_REQUEST,
                "bad_data",
                false,
            ),
            (
                "TagsParam",
                ApiError::TagsParam(TagsParamError::UnsupportedScope("bogus".to_string())),
                StatusCode::BAD_REQUEST,
                "bad_data",
                false,
            ),
            (
                "TagPath",
                ApiError::TagPath(TagPathError::EmptyKey),
                StatusCode::BAD_REQUEST,
                "bad_data",
                false,
            ),
            (
                "Legacy",
                ApiError::Legacy(LegacyError::UnquotedEquals {
                    key: "a".to_string(),
                    pos: 3,
                }),
                StatusCode::BAD_REQUEST,
                "bad_data",
                true,
            ),
            (
                "Query",
                ApiError::Query(pulsus_traceql::parse("{ ").expect_err("must fail")),
                StatusCode::BAD_REQUEST,
                "bad_data",
                true,
            ),
            (
                "QueryText(Invalid)",
                ApiError::QueryText(invalid()),
                StatusCode::BAD_REQUEST,
                "bad_data",
                true,
            ),
            (
                "QueryText(TooLong)",
                ApiError::QueryText(too_long()),
                StatusCode::BAD_REQUEST,
                "bad_data",
                false,
            ),
            (
                "QueryText(Semantic)",
                ApiError::QueryText(semantic()),
                StatusCode::BAD_REQUEST,
                "bad_data",
                false,
            ),
            (
                "Plan",
                ApiError::Plan(pulsus_read::TracePlanError::TypeMismatch(
                    "status supports only = and !=".to_string(),
                )),
                StatusCode::BAD_REQUEST,
                "bad_data",
                false,
            ),
            (
                "Plan(MetricsPointCap)",
                ApiError::Plan(pulsus_read::TracePlanError::MetricsPointCap {
                    buckets: 12_000,
                    cap: 11_000,
                }),
                StatusCode::UNPROCESSABLE_ENTITY,
                "query_too_broad",
                false,
            ),
            (
                "NotFound",
                ApiError::NotFound,
                StatusCode::NOT_FOUND,
                "not_found",
                false,
            ),
            (
                "NotAcceptable",
                ApiError::NotAcceptable,
                StatusCode::NOT_ACCEPTABLE,
                "not_acceptable",
                false,
            ),
            (
                "Read",
                ApiError::Read(ReadError::EmptyMatcherSet),
                StatusCode::BAD_REQUEST,
                "bad_data",
                false,
            ),
            (
                "Assemble",
                ApiError::Assemble(AssembleError::UnsupportedPayloadType { count: 3 }),
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                false,
            ),
            (
                "PoolUnavailable",
                ApiError::PoolUnavailable,
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                false,
            ),
        ];

        for (case, err, want_status, want_type, want_position) in cases {
            let (status, json) = envelope(err).await;
            assert_eq!(status, want_status, "{case}: status, body {json}");
            assert_eq!(json["errorType"], want_type, "{case}: body {json}");
            assert_eq!(
                json.get("position").is_some(),
                want_position,
                "{case}: `position` presence, body {json}"
            );
            if want_position {
                assert!(json["position"].is_u64(), "{case}: body {json}");
            }
        }
    }

    #[tokio::test]
    async fn a_plan_error_maps_to_400_bad_data_without_a_position() {
        let err = ApiError::Plan(pulsus_read::TracePlanError::TypeMismatch(
            "status supports only = and !=".to_string(),
        ));
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(json.get("position").is_none());
    }

    #[tokio::test]
    async fn a_metrics_param_error_maps_to_400_bad_data_without_a_position() {
        let err = ApiError::MetricsParam(MetricsParamError::InvalidStep("500ms".to_string()));
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(json.get("position").is_none());
        assert!(
            json["error"].as_str().is_some_and(|m| m.contains("500ms")),
            "message must name the rejected step, got {json}"
        );
    }

    #[tokio::test]
    async fn the_metrics_point_cap_plan_error_maps_to_422_query_too_broad() {
        // Issue #59 adjudication: a static pre-execution rejection, not
        // the 400 `bad_data` plan family.
        let err = ApiError::Plan(pulsus_read::TracePlanError::MetricsPointCap {
            buckets: 12_000,
            cap: 11_000,
        });
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json["errorType"], "query_too_broad");
        assert!(json.get("position").is_none());
        assert!(
            json["error"].as_str().is_some_and(|m| m.contains("12000")),
            "message must name the bucket count, got {json}"
        );
    }

    #[tokio::test]
    async fn the_metrics_set_budget_reason_maps_to_422_query_too_broad() {
        let err = ApiError::Read(ReadError::QueryTooBroad(
            pulsus_read::logql::TooBroadReason::TraceMetricsSetRows {
                max_set_rows: 1_000_000,
            },
        ));
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json["errorType"], "query_too_broad");
    }

    /// Issue #57 re-audit AC-conformance: the generator-memory reason is
    /// covered by `ApiError::Read`'s `QueryTooBroad(_)` arm — no dedicated
    /// match arm was needed — and names its reason in the body.
    #[tokio::test]
    async fn the_generator_memory_reason_maps_to_422_query_too_broad() {
        let err = ApiError::Read(ReadError::QueryTooBroad(
            pulsus_read::logql::TooBroadReason::TraceGeneratorMemory {
                budget_bytes: 1_048_576,
            },
        ));
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json["errorType"], "query_too_broad");
        assert!(
            json["error"]
                .as_str()
                .is_some_and(|m| m.contains("generator memory")),
            "message must name the reason, got {json}"
        );
    }
}
