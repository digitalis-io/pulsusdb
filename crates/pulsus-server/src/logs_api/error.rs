//! `/api/logs/v1`'s error envelope: `{"status":"error","errorType",...
//! "error","position"?}` (docs/api.md "Errors"), and the status-code
//! mapping table pinned by the issue #13 architect plan.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use pulsus_clickhouse::ChError;
use pulsus_logql::LogQlError;
use pulsus_read::ReadError;

use super::params::ParamError;

/// Every failure mode a `/api/logs/v1` handler can return, converted to the
/// documented error envelope by [`IntoResponse`].
#[derive(Debug)]
pub(crate) enum ApiError {
    Param(ParamError),
    LogQl(LogQlError),
    Read(ReadError),
    /// The ClickHouse pool has not been established yet (mirrors `/ready`'s
    /// 503 — `ops::ready`) — not one of the architect plan's error-table
    /// rows (that table covers parse/plan/execute failures against an
    /// already-live pool), but the same "not yet serving" contract every
    /// other data-plane route needs before a pool exists.
    PoolUnavailable,
}

impl From<ParamError> for ApiError {
    fn from(e: ParamError) -> Self {
        ApiError::Param(e)
    }
}

impl From<LogQlError> for ApiError {
    fn from(e: LogQlError) -> Self {
        ApiError::LogQl(e)
    }
}

impl From<ReadError> for ApiError {
    fn from(e: ReadError) -> Self {
        ApiError::Read(e)
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
            ApiError::LogQl(e) => (
                StatusCode::BAD_REQUEST,
                "bad_data",
                e.to_string(),
                Some(e.span().start),
            ),
            ApiError::Read(e) => read_error_parts(e),
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

/// The `ReadError` branch of the architect plan's status-code table:
///
/// | source | HTTP | errorType |
/// |---|---|---|
/// | `Parse`/`EmptyMatcherSet`/`ContradictoryMatchers`/`InvalidStep` | 400 | `bad_data` |
/// | `QueryTooBroad` | 422 | `query_too_broad` |
/// | `Clickhouse(Timeout)` | 504 | `timeout` |
/// | `Clickhouse(_)` | 500 | `internal` |
fn read_error_parts(e: &ReadError) -> (StatusCode, &'static str, String, Option<usize>) {
    match e {
        ReadError::Parse(inner) => (
            StatusCode::BAD_REQUEST,
            "bad_data",
            e.to_string(),
            Some(inner.span().start),
        ),
        // Issue M6-09 plan v3 delta 6: pipeline-validation failures (bad
        // regex / unsupported template function / bad parser expression /
        // `unwrap` outside a range aggregation) and the M6-10
        // metric-pipeline deferral are both client-caused query-shape
        // rejections — 400 `bad_data`, alongside the other planner
        // validation variants.
        ReadError::EmptyMatcherSet
        | ReadError::ContradictoryMatchers
        | ReadError::InvalidStep
        | ReadError::PipelineInvalid { .. }
        | ReadError::PipelineUnsupportedInMetric { .. } => {
            (StatusCode::BAD_REQUEST, "bad_data", e.to_string(), None)
        }
        // Issue #85: `NamelessSelectorUnresolvable` is a PromQL-only
        // variant (the LogQL engine never produces it), matched here for
        // exhaustiveness like `ReadError::Promql` below — mapped as a
        // too-broad-class client rejection, mirroring
        // `prom_api::error::read_error_parts`'s own 422 for it.
        ReadError::QueryTooBroad(_) | ReadError::NamelessSelectorUnresolvable { .. } => (
            StatusCode::UNPROCESSABLE_ENTITY,
            "query_too_broad",
            e.to_string(),
            None,
        ),
        ReadError::Clickhouse(ch) => match ch {
            ChError::Timeout(_) => (StatusCode::GATEWAY_TIMEOUT, "timeout", e.to_string(), None),
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                e.to_string(),
                None,
            ),
        },
        // Issue #31: every `pulsus_promql::PromqlError` variant (parse,
        // unsupported construct, bad vector matching, histogram_quantile
        // bucket error) is a client-caused failure — mapped uniformly to
        // 400 `bad_data`, no position (unlike `LogQlError`'s span-tracked
        // parser, `PromqlError::Parse` carries only the vendored parser's
        // message text). `pulsus-server` does not yet wire `MetricsEngine`
        // into a route (that is #32), so this arm is unreachable from any
        // request today; #32 may split it into a finer-grained mapping
        // (e.g. `Unsupported` -> a distinct `errorType`) if the PromQL API
        // surface needs one — this keeps `ReadError` matches exhaustive
        // and correct in the meantime.
        ReadError::Promql(_) => (StatusCode::BAD_REQUEST, "bad_data", e.to_string(), None),
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
        let (status, json) = envelope(ApiError::Param(ParamError::MissingQuery)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["status"], "error");
        assert_eq!(json["errorType"], "bad_data");
        assert!(json.get("position").is_none());
    }

    #[tokio::test]
    async fn logql_error_maps_to_400_bad_data_with_a_position() {
        let err = LogQlError::EmptySelector {
            span: pulsus_logql::Span { start: 4, end: 6 },
        };
        let (status, json) = envelope(ApiError::LogQl(err)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert_eq!(json["position"], 4);
    }

    #[tokio::test]
    async fn read_error_empty_matcher_set_maps_to_400_bad_data() {
        let (status, json) = envelope(ApiError::Read(ReadError::EmptyMatcherSet)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
    }

    #[tokio::test]
    async fn read_error_invalid_step_maps_to_400_bad_data() {
        let (status, json) = envelope(ApiError::Read(ReadError::InvalidStep)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
    }

    /// Issue M6-09 plan v3 delta 6: both pipeline rejection classes map
    /// to 400 `bad_data` (matching the
    /// `read_error_empty_matcher_set_maps_to_400_bad_data` idiom).
    #[tokio::test]
    async fn read_error_pipeline_invalid_maps_to_400_bad_data() {
        let err = ReadError::PipelineInvalid {
            reason: "bad regex: unclosed group".to_string(),
        };
        let (status, json) = envelope(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(
            json["error"]
                .as_str()
                .unwrap_or_default()
                .contains("invalid pipeline")
        );
    }

    #[tokio::test]
    async fn read_error_pipeline_unsupported_in_metric_maps_to_400_bad_data() {
        let err = ReadError::PipelineUnsupportedInMetric {
            construct: "json".to_string(),
        };
        let (status, json) = envelope(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(json["error"].as_str().unwrap_or_default().contains("json"));
    }

    #[tokio::test]
    async fn read_error_query_too_broad_maps_to_422() {
        let err = ReadError::QueryTooBroad(pulsus_read::logql::TooBroadReason::StreamCap {
            count: 1,
            cap: 1,
        });
        let (status, json) = envelope(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json["errorType"], "query_too_broad");
    }

    #[tokio::test]
    async fn read_error_clickhouse_timeout_maps_to_504() {
        let err = ReadError::Clickhouse(ChError::Timeout("deadline".to_string()));
        let (status, json) = envelope(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(json["errorType"], "timeout");
    }

    #[tokio::test]
    async fn read_error_clickhouse_other_maps_to_500_internal() {
        let err = ReadError::Clickhouse(ChError::Decode("bad row".to_string()));
        let (status, json) = envelope(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(json["errorType"], "internal");
    }

    #[tokio::test]
    async fn pool_unavailable_maps_to_503() {
        let (status, json) = envelope(ApiError::PoolUnavailable).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }
}
