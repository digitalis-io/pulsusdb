//! `/api/traces/v1`'s error envelope: `{"status":"error","errorType",
//! "error"}` (docs/api.md §4.1), and the status-code mapping table pinned
//! by the issue #55 plan (v2's error table + v3's `406 not_acceptable`).
//! Mirrors `logs_api/error.rs`'s structure; no `position` field — there is
//! no query language to report a byte offset into on this surface.
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

use super::assemble::AssembleError;
use super::params::TraceIdError;

/// Every failure mode a `/api/traces/v1/trace/{traceId}` handler can
/// return, converted to the documented error envelope by [`IntoResponse`]:
///
/// | variant | HTTP | `errorType` |
/// |---|---|---|
/// | `Param` | 400 | `bad_data` |
/// | `NotFound` | 404 | `not_found` |
/// | `NotAcceptable` | 406 | `not_acceptable` |
/// | `PoolUnavailable` | 503 | `unavailable` |
/// | `Read(Clickhouse(Timeout))` | 504 | `timeout` |
/// | `Read(_)` / `Assemble(_)` | 500 | `internal` |
#[derive(Debug)]
pub(crate) enum ApiError {
    Param(TraceIdError),
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
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, error_type, message) = match &self {
            ApiError::Param(e) => (StatusCode::BAD_REQUEST, "bad_data", e.to_string()),
            ApiError::NotFound => (
                StatusCode::NOT_FOUND,
                "not_found",
                "trace not found".to_string(),
            ),
            ApiError::NotAcceptable => (
                StatusCode::NOT_ACCEPTABLE,
                "not_acceptable",
                "no acceptable representation: this endpoint serves application/json and \
                 application/protobuf"
                    .to_string(),
            ),
            ApiError::Read(e) => match e {
                ReadError::Clickhouse(ChError::Timeout(_)) => {
                    (StatusCode::GATEWAY_TIMEOUT, "timeout", e.to_string())
                }
                _ => (StatusCode::INTERNAL_SERVER_ERROR, "internal", e.to_string()),
            },
            ApiError::Assemble(e) => (StatusCode::INTERNAL_SERVER_ERROR, "internal", e.to_string()),
            ApiError::PoolUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                "clickhouse pool not yet established".to_string(),
            ),
        };
        let body = ErrorEnvelope {
            status: "error",
            error_type,
            error: message,
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
}
