//! `/api/traces/v1`'s error envelope: `{"status":"error","errorType",
//! "error","position"?}` (docs/api.md §4.1/§4.2), and the status-code
//! mapping table pinned by the issue #55 plan (v2's error table + v3's
//! `406 not_acceptable`) plus issue #57's search rows. Mirrors
//! `logs_api/error.rs`'s structure; `position` (a byte offset) appears
//! only on TraceQL parse errors — the fetch surface never carries it.
//!
//! Errors are **always** this JSON envelope, never protobuf, regardless of
//! the request's `Accept` header (docs/api.md §4.1) — the mounted-but-
//! absent `404` JSON envelope doubles as the conformance suite's mounting
//! oracle (an unmounted path returns axum's empty `404` instead).

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
    MetricsParamError, SearchParamError, TagPathError, TagsParamError, TraceIdError,
};

/// Every failure mode a `/api/traces/v1` handler can return, converted
/// to the documented error envelope by [`IntoResponse`]:
///
/// | variant | HTTP | `errorType` |
/// |---|---|---|
/// | `Param` / `SearchParam` / `MetricsParam` / `TagsParam` / `TagPath` | 400 | `bad_data` |
/// | `Plan` (except the point cap) | 400 | `bad_data` |
/// | `Query` (TraceQL parse, carries `position`) | 400 | `bad_data` |
/// | `Legacy` (strict logfmt, carries `position` into `tags`) | 400 | `bad_data` |
/// | `NotFound` | 404 | `not_found` |
/// | `NotAcceptable` | 406 | `not_acceptable` |
/// | `Plan(MetricsPointCap)` (issue #59 static pre-execution rejection) | 422 | `query_too_broad` |
/// | `Read(QueryTooBroad)` | 422 | `query_too_broad` |
/// | `PoolUnavailable` | 503 | `unavailable` |
/// | `Read(Clickhouse(Timeout))` | 504 | `timeout` |
/// | `Read(_)` / `Assemble(_)` | 500 | `internal` |
#[derive(Debug)]
pub(crate) enum ApiError {
    Param(TraceIdError),
    /// Search request-parameter failures (issue #57).
    SearchParam(SearchParamError),
    /// Metrics request-parameter failures (issue #59, no `position`).
    MetricsParam(MetricsParamError),
    /// `/tags` request-parameter failures (issue #58, no `position`).
    TagsParam(TagsParamError),
    /// `{tag}` path-parameter failures (issue #58, no `position`).
    TagPath(TagPathError),
    /// Legacy `tags` logfmt failures (issue #57).
    Legacy(LegacyError),
    /// TraceQL parse failure — `400 bad_data` with a `position` byte
    /// offset, matching the LogQL parse-error envelope.
    Query(TraceQlError),
    /// Search planning failure (unsupported field / type mismatch).
    Plan(pulsus_read::TracePlanError),
    /// The trace has no stored spans (an empty §4.2 fetch).
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

impl From<MetricsParamError> for ApiError {
    fn from(e: MetricsParamError) -> Self {
        ApiError::MetricsParam(e)
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
            ApiError::SearchParam(e) => (StatusCode::BAD_REQUEST, "bad_data", e.to_string(), None),
            ApiError::MetricsParam(e) => (StatusCode::BAD_REQUEST, "bad_data", e.to_string(), None),
            ApiError::TagsParam(e) => (StatusCode::BAD_REQUEST, "bad_data", e.to_string(), None),
            ApiError::TagPath(e) => (StatusCode::BAD_REQUEST, "bad_data", e.to_string(), None),
            // Strict logfmt errors carry a byte offset into the decoded
            // `tags` value (code review round 1 — documented in
            // docs/api.md §4.2 alongside the TraceQL parse offset).
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
            // The metrics point cap is the one plan-time 422 (issue #59
            // adjudication: a static pre-execution rejection in the
            // too-broad family — bounded response, never a silent
            // truncation); every other plan failure stays a 400.
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
            ApiError::Read(e) => match e {
                ReadError::QueryTooBroad(_) => (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "query_too_broad",
                    e.to_string(),
                    None,
                ),
                ReadError::Clickhouse(ChError::Timeout(_)) => {
                    (StatusCode::GATEWAY_TIMEOUT, "timeout", e.to_string(), None)
                }
                _ => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    e.to_string(),
                    None,
                ),
            },
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
        // Issue #59 adjudication: the one plan-time 422 — never conflated
        // with the 400 bad_data plan family.
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

    /// Issue #57 re-audit AC-conformance: the generator-memory reason
    /// carries the same envelope as every other `QueryTooBroad` variant
    /// — no dedicated match arm was needed (`ApiError::Read`'s
    /// `QueryTooBroad(_)` arm already covers it).
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
