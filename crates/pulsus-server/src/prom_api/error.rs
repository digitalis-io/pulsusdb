//! `/api/v1/*`'s error envelope: `{"status":"error","errorType",...
//! "error"}` — **exactly** these three fields, no `position` field (issue
//! #32 architect plan: unlike `logs_api`'s `{..,"position"}`, a PromQL
//! parse error's position is embedded verbatim inside the `error` message
//! string, Prometheus-style — `pulsus_promql::PromqlError::Parse`'s
//! `Display` already carries the vendored parser's own positional text,
//! see docs/api.md §3's "Errors" section). The five-type taxonomy below is
//! pinned by the plan amendment (task-manager resolution, overruling the
//! original draft's four-type collapse): `timeout` is distinct from
//! `unavailable` so Prometheus-compatible clients can branch on it.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use pulsus_clickhouse::ChError;
use pulsus_promql::PromqlError;
use pulsus_read::ReadError;

use super::params::ParamError;

/// Every failure mode a `/api/v1` handler can return, converted to the
/// documented error envelope by [`IntoResponse`].
#[derive(Debug)]
pub(crate) enum ApiError {
    Param(ParamError),
    /// A parse-time `pulsus_promql::parse` failure (before the engine is
    /// ever reached) — always `PromqlError::Parse` in practice (`parse`'s
    /// own contract only ever constructs that variant), matched
    /// exhaustively all the same so this stays correct if that ever
    /// changes.
    Promql(PromqlError),
    Read(ReadError),
    /// The ClickHouse pool or the label cache has not been established yet
    /// (mirrors `logs_api::error::ApiError::PoolUnavailable` / `/ready`'s
    /// 503 — `ops::ready`).
    Unavailable,
}

impl From<ParamError> for ApiError {
    fn from(e: ParamError) -> Self {
        ApiError::Param(e)
    }
}

impl From<PromqlError> for ApiError {
    fn from(e: PromqlError) -> Self {
        ApiError::Promql(e)
    }
}

impl From<ReadError> for ApiError {
    fn from(e: ReadError) -> Self {
        ApiError::Read(e)
    }
}

/// The Prometheus-exact error envelope — three fields, always in this
/// order, never a `position` field (see the module doc).
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
            ApiError::Promql(e) => promql_error_parts(e),
            ApiError::Read(e) => read_error_parts(e),
            ApiError::Unavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                "clickhouse pool or label cache not yet established".to_string(),
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

/// The five-type taxonomy (docs/api.md §3's "Errors" section, task-manager
/// resolution overruling the original four-type draft):
///
/// | source | HTTP | `errorType` |
/// |---|---|---|
/// | `PromqlError::Parse` (position **in** the message) | 400 | `bad_data` |
/// | `PromqlError::{Unsupported,BadMatching,HistogramBucket,InvalidParameter,LabelSet}` | 422 | `execution` |
/// | `ChError::Timeout` | 503 | `timeout` |
/// | `ChError::Connect` | 503 | `unavailable` |
/// | `ChError::{Io,Server,Decode,Config,InsertUncertain}` | 500 | `internal` |
/// | `PromqlError::Cancelled` (issue #93, unreachable in practice) | 408 | `timeout` |
fn promql_error_parts(e: &PromqlError) -> (StatusCode, &'static str, String) {
    match e {
        PromqlError::Parse(_) => (StatusCode::BAD_REQUEST, "bad_data", e.to_string()),
        // `InvalidParameter` (issue #67: an out-of-range
        // `double_exponential_smoothing` factor) maps like
        // `HistogramBucket`: a well-formed query whose evaluation is
        // rejected — 422 `execution`, the adjudicated precedent.
        // `LabelSet` (issue #68: label_replace/label_join invalid
        // regex/label-name and duplicate-output-labelset errors) maps the
        // same way — a well-formed query whose evaluation is rejected,
        // exactly upstream's 422 `execution` for these.
        PromqlError::Unsupported { .. }
        | PromqlError::BadMatching { .. }
        | PromqlError::HistogramBucket { .. }
        | PromqlError::InvalidParameter { .. }
        | PromqlError::LabelSet { .. } => {
            (StatusCode::UNPROCESSABLE_ENTITY, "execution", e.to_string())
        }
        // Issue #93: a live `CancelToken` fired because the awaiting
        // request future was already dropped (client disconnect, or the
        // `TimeoutLayer` firing first — `middleware.rs`'s own 408
        // `query_timeout`). Matched for exhaustiveness only — by the time
        // this variant exists, the future that would encode this response
        // is gone, so this arm is unreachable in practice. `408`/`timeout`
        // mirrors the same convention rather than inventing a new status.
        PromqlError::Cancelled => (StatusCode::REQUEST_TIMEOUT, "timeout", e.to_string()),
    }
}

fn read_error_parts(e: &ReadError) -> (StatusCode, &'static str, String) {
    match e {
        ReadError::Promql(inner) => promql_error_parts(inner),
        ReadError::Clickhouse(ch) => match ch {
            ChError::Timeout(_) => (StatusCode::SERVICE_UNAVAILABLE, "timeout", e.to_string()),
            ChError::Connect(_) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                e.to_string(),
            ),
            ChError::Io(_)
            | ChError::Server { .. }
            | ChError::Decode(_)
            | ChError::Config(_)
            | ChError::InsertUncertain(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "internal", e.to_string())
            }
        },
        // `MetricsEngine` never produces any of these — they are
        // `LogQlEngine`-only variants of the shared `ReadError` enum
        // (mirrors `logs_api::error::read_error_parts`'s own precedent for
        // its unreachable-today `ReadError::Promql` arm). Matched
        // exhaustively so this stays correct rather than merely
        // "impossible today".
        ReadError::Parse(_)
        | ReadError::EmptyMatcherSet
        | ReadError::ContradictoryMatchers
        | ReadError::InvalidStep
        | ReadError::PipelineInvalid { .. }
        | ReadError::MetricPipelineError { .. }
        | ReadError::PipelineUnsupportedInMetric { .. } => {
            (StatusCode::BAD_REQUEST, "bad_data", e.to_string())
        }
        // M7-A5a: a `metric_hist_samples` row that cannot rebuild a
        // histogram is a storage/data-integrity defect (validated at
        // ingest, so unreachable for writer-produced rows), not a client
        // error — 500 `internal`, exactly like `ChError::Decode`.
        ReadError::HistogramDecode(_) => {
            (StatusCode::INTERNAL_SERVER_ERROR, "internal", e.to_string())
        }
        // Issue #85 (M6-08c): the name-less-selector fan-out cap
        // (`TooBroadReason::MetricFanout`) rides the existing
        // QueryTooBroad -> 422 `execution` mapping; the degraded-cache
        // name-less failure is likewise a well-formed query the engine
        // declines to execute — 422 `execution`, never a 5xx (ClickHouse
        // is healthy; the in-process cache just cannot answer it).
        //
        // M7-A5a: `HistogramResultUnsupported` joins this arm (plan v3
        // finding 1) — a well-formed, executed query whose result type the
        // A5a encoder declines to render (the histogram JSON encoder is
        // A5b), the same class as `QueryTooBroad`. NOT 400 `bad_data`
        // (that is the LogQL parse/matcher arm) and NOT a 5xx.
        ReadError::QueryTooBroad(_)
        | ReadError::NamelessSelectorUnresolvable { .. }
        | ReadError::HistogramResultUnsupported => {
            (StatusCode::UNPROCESSABLE_ENTITY, "execution", e.to_string())
        }
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
    }

    #[tokio::test]
    async fn envelope_has_exactly_three_fields_never_a_position() {
        let (_, json) = envelope(ApiError::Param(ParamError::MissingQuery)).await;
        let obj = json.as_object().expect("object");
        assert_eq!(obj.len(), 3, "envelope must have exactly 3 fields: {obj:?}");
        assert!(obj.contains_key("status"));
        assert!(obj.contains_key("errorType"));
        assert!(obj.contains_key("error"));
        assert!(!obj.contains_key("position"));
    }

    /// Issue M6-10 review round 1, gap (d): the surviving-metric-
    /// pipeline-error variant maps to 400 `bad_data` here too (this
    /// mapper matches `ReadError` exhaustively — a LogQL-only variant
    /// must still carry a correct mapping, mirroring the
    /// unreachable-today `Parse` arm's precedent).
    #[tokio::test]
    async fn read_error_metric_pipeline_error_maps_to_400_bad_data() {
        let err = pulsus_read::logql::ReadError::MetricPipelineError {
            error_type: "SampleExtractionErr".to_string(),
            series: r#"{__error__="SampleExtractionErr", app="x"}"#.to_string(),
        };
        let (status, json) = envelope(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(
            json["error"]
                .as_str()
                .unwrap_or_default()
                .starts_with("pipeline error: 'SampleExtractionErr'"),
            "{json}"
        );
    }

    /// M7-A5a AC8c: a native-histogram-valued query result surfaces as
    /// 422 `execution` (the well-formed-but-undeclinable class), NOT 400
    /// `bad_data`, and the message names M7-A5b.
    #[tokio::test]
    async fn read_error_histogram_result_unsupported_maps_to_422_execution() {
        let err = pulsus_read::logql::ReadError::HistogramResultUnsupported;
        let (status, json) = envelope(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json["errorType"], "execution");
        assert!(
            json["error"]
                .as_str()
                .unwrap_or_default()
                .contains("M7-A5b"),
            "{json}"
        );
    }

    #[tokio::test]
    async fn promql_parse_error_maps_to_400_bad_data_and_embeds_the_message() {
        let err = PromqlError::Parse("unexpected token at char 3".to_string());
        let (status, json) = envelope(ApiError::Promql(err)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(
            json["error"]
                .as_str()
                .unwrap()
                .contains("unexpected token at char 3")
        );
    }

    #[tokio::test]
    async fn promql_unsupported_error_maps_to_422_execution() {
        let err = PromqlError::Unsupported {
            construct: "the @ modifier".to_string(),
        };
        let (status, json) = envelope(ApiError::Promql(err)).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json["errorType"], "execution");
    }

    #[tokio::test]
    async fn promql_bad_matching_error_maps_to_422_execution() {
        // Issue #70: the duplicate-match detail is the upstream text
        // verbatim — no added prefix — asserted byte-equal at the HTTP
        // surface, not just by substring.
        let err = PromqlError::BadMatching {
            detail: "multiple matches for labels: many-to-one matching must be explicit \
                     (group_left/group_right)"
                .to_string(),
        };
        let (status, json) = envelope(ApiError::Promql(err)).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json["errorType"], "execution");
        assert_eq!(
            json["error"],
            "multiple matches for labels: many-to-one matching must be explicit \
             (group_left/group_right)"
        );
    }

    #[tokio::test]
    async fn promql_histogram_bucket_error_maps_to_422_execution() {
        let err = PromqlError::HistogramBucket {
            detail: "no +Inf bucket found".to_string(),
        };
        let (status, json) = envelope(ApiError::Promql(err)).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json["errorType"], "execution");
    }

    #[tokio::test]
    async fn promql_invalid_parameter_error_maps_to_422_execution() {
        // Issue #67: `double_exponential_smoothing`'s factor validation —
        // maps on the HistogramBucket precedent.
        let err = PromqlError::InvalidParameter {
            detail: "invalid smoothing factor: expected 0 < sf < 1, got 2".to_string(),
        };
        let (status, json) = envelope(ApiError::Promql(err)).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json["errorType"], "execution");
        assert!(
            json["error"]
                .as_str()
                .unwrap()
                .contains("invalid smoothing factor")
        );
    }

    /// Issue #93 (plan-review note 1): a cancelled offloaded eval maps to
    /// 408, `errorType: "timeout"` — matching `middleware.rs`'s existing
    /// `TimeoutLayer` 408 convention, not `503`/`unavailable` (the
    /// `ChError::Timeout` mapping above) and not a made-up `499`.
    #[tokio::test]
    async fn promql_cancelled_error_maps_to_408_timeout() {
        let err = PromqlError::Cancelled;
        let (status, json) = envelope(ApiError::Promql(err)).await;
        assert_eq!(status, StatusCode::REQUEST_TIMEOUT);
        assert_eq!(json["errorType"], "timeout");
    }

    #[tokio::test]
    async fn promql_label_set_error_maps_to_422_execution_with_the_raw_message() {
        // Issue #68: label_replace/label_join validation and
        // duplicate-labelset errors — the message is the upstream text
        // verbatim (asserted by substring in the vendored corpus).
        let err = PromqlError::LabelSet {
            detail: "vector cannot contain metrics with the same labelset".to_string(),
        };
        let (status, json) = envelope(ApiError::Promql(err)).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json["errorType"], "execution");
        assert_eq!(
            json["error"],
            "vector cannot contain metrics with the same labelset"
        );
    }

    #[tokio::test]
    async fn read_error_promql_delegates_to_the_same_promql_mapping() {
        let err = ReadError::Promql(PromqlError::Unsupported {
            construct: "subqueries".to_string(),
        });
        let (status, json) = envelope(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json["errorType"], "execution");
    }

    #[tokio::test]
    async fn read_error_clickhouse_timeout_maps_to_503_timeout() {
        let err = ReadError::Clickhouse(ChError::Timeout("deadline".to_string()));
        let (status, json) = envelope(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "timeout");
    }

    #[tokio::test]
    async fn read_error_clickhouse_connect_maps_to_503_unavailable() {
        let err = ReadError::Clickhouse(ChError::Connect("refused".to_string()));
        let (status, json) = envelope(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }

    #[tokio::test]
    async fn read_error_clickhouse_other_maps_to_500_internal() {
        let err = ReadError::Clickhouse(ChError::Decode("bad row".to_string()));
        let (status, json) = envelope(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(json["errorType"], "internal");
    }

    #[tokio::test]
    async fn unavailable_maps_to_503_unavailable() {
        let (status, json) = envelope(ApiError::Unavailable).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }

    #[tokio::test]
    async fn too_many_points_param_error_names_the_cap() {
        let err = ParamError::TooManyPoints {
            points: 11_001,
            cap: 11_000,
        };
        let (status, json) = envelope(ApiError::Param(err)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(json["error"].as_str().unwrap().contains("11000"));
    }
}
