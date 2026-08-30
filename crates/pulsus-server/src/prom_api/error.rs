//! `/api/v1/*`'s error envelope: `{"status":"error","errorType",...
//! "error"}` — **exactly** these three fields, no `position` field (issue
//! #32 architect plan: a PromQL parse error's position is embedded
//! verbatim inside the `error` message string, Prometheus-style —
//! `pulsus_promql::PromqlError::Parse`'s `Display` already carries the
//! vendored parser's own positional text, see docs/api.md §3's "Errors"
//! section). The five-type taxonomy below is
//! pinned by the plan amendment (task-manager resolution, overruling the
//! original draft's four-type collapse): `timeout` is distinct from
//! `unavailable` so Prometheus-compatible clients can branch on it.
//!
//! Issue #264 moved the LogQL surface (`logs_api/error.rs`) off its JSON
//! envelope onto a bare `text/plain` body and left this one alone
//! deliberately: upstream Prometheus writes every API error as
//! `application/json` (`respondError`, `web/api/v1/api.go:2200-2230`,
//! read at `vendor/github.com/prometheus/prometheus/` @ grafana/loki
//! v3.7.4), so making the two surfaces symmetric would have created a
//! divergence rather than closing one.

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
    /// Issue #471 M2: a request deadline expired. `503`/`timeout`, with a
    /// message naming which deadline it was — see [`DeadlineProducer`].
    Deadline(DeadlineProducer),
}

/// Which deadline expired (issue #471 M2). One message per producer, each
/// true on **every** path that can produce it.
///
/// The reference's own sentence attributes the expiry to expression
/// evaluation, which is true for it because it is handed the phase that
/// expired. Ours can expire in parameter parsing, PromQL parsing,
/// planning, the ClickHouse round trip, client-side evaluation or
/// encoding — so that sentence would be a guess that reads like a fact.
/// A message that is false some of the time is worse than one that differs
/// from the reference, and `errorType`, the field a client branches on, is
/// identical either way. Ledgered as
/// `promql-timeout-message-names-the-layer`
/// (docs/benchmarks/metrics-differential-ledger.md).
#[derive(Debug, Clone, Copy)]
pub(crate) enum DeadlineProducer {
    /// `middleware::timeout_layer` — the whole-request deadline.
    ///
    /// Says **request**, not **query**: five of the twelve classified
    /// paths are `status/*`, and `/api/v1/status/tsdb` is served entirely
    /// from the resident label-cache snapshot with zero ClickHouse and no
    /// actual await (`pulsus_read`'s `MetricsEngine::tsdb_status`).
    /// Narrowing the classification instead would be worse — a timeout on
    /// `status/*` would revert to the bare `408` this entry exists to
    /// remove — so the message widens rather than the path set narrowing.
    ServerRequest(std::time::Duration),
    /// The `timeout` request parameter — `/query`, `/query_range` only.
    /// Keeps "query" because both of those genuinely are queries.
    RequestedTimeout(std::time::Duration),
}

impl DeadlineProducer {
    pub(crate) fn message(self) -> String {
        match self {
            // `{:?}` on a `std::time::Duration` renders `120s` / `1.5s` /
            // `20ms`. NEVER `pulsus_config::HumanDuration`'s `Display`,
            // which renders the same 120 s as `120000ms`.
            Self::ServerRequest(d) => {
                format!("request exceeded the server deadline of {d:?} (PULSUS_QUERY_TIMEOUT)")
            }
            Self::RequestedTimeout(d) => {
                format!("query exceeded the requested timeout of {d:?} (timeout parameter)")
            }
        }
    }
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
            ApiError::Deadline(producer) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "timeout",
                producer.message(),
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
/// | `PromqlError::ExprTooDeep` (issue #262, docs/benchmarks/metrics-differential-ledger.md) | 400 | `bad_data` |
/// | `PromqlError::{Unsupported,BadMatching,HistogramBucket,InvalidParameter,LabelSet,ScalarOp,ExtendedHistogram}` | 422 | `execution` |
/// | `ChError::Timeout` | 503 | `timeout` |
/// | `ChError::Connect` | 503 | `unavailable` |
/// | `ChError::Server { code: 159 }` (ClickHouse `TIMEOUT_EXCEEDED`, issue #471 M2) | 503 | `timeout` |
/// | `ChError::{Io,Server(other),Decode,Config,InsertUncertain}` | 500 | `internal` |
/// | `PromqlError::Cancelled` (issue #93, unreachable in practice) | 503 | `timeout` |
/// | `ApiError::Deadline` (issue #471 M2) | 503 | `timeout` |
fn promql_error_parts(e: &PromqlError) -> (StatusCode, &'static str, String) {
    match e {
        // Issue #280: an RE2-rejected label-matcher regex. Upstream
        // Prometheus compiles every matcher with Go's `regexp` (RE2)
        // inside `promql/parser`, so this input is a **parse-time 400
        // `bad_data`** there — never a 5xx. This engine only learns the
        // verdict from ClickHouse (RE2 is deliberately the authority, not
        // the Rust `regex` crate — see the variant's own doc), so it
        // arrives via `ReadError::Promql` at execution time; the status
        // and `errorType` still have to be Prometheus's, which is why
        // this rides the `Parse` arm's mapping and NOT the 422
        // `execution` class below (an invalid regex is a malformed
        // request, not a well-formed query the engine declined).
        //
        // Issue #262: `ExprTooDeep` rides the same arm. A parsed
        // expression deeper than `pulsus_promql::MAX_EXPR_DEPTH` is a
        // malformed request in exactly the sense `Parse` is — the guard
        // sits INSIDE `pulsus_promql::parse`, before anything plans or
        // evaluates — so it is a 400 `bad_data`, never a 422 declined
        // query. Prometheus has no such rejection at all (it grows its
        // stacks and answers the query), which is why this is a
        // ledgered divergence rather than a parity fix: docs/api.md
        // docs/benchmarks/metrics-differential-ledger.md, row
        // `promql-expression-depth-cap`.
        PromqlError::Parse(_)
        | PromqlError::InvalidRegexMatcher { .. }
        | PromqlError::ExprTooDeep { .. } => (StatusCode::BAD_REQUEST, "bad_data", e.to_string()),
        // `InvalidParameter` (issue #67: an out-of-range
        // `double_exponential_smoothing` factor) maps like
        // `HistogramBucket`: a well-formed query whose evaluation is
        // rejected — 422 `execution`, the adjudicated precedent.
        // `LabelSet` (issue #68: label_replace/label_join invalid
        // regex/label-name and duplicate-output-labelset errors) maps the
        // same way — a well-formed query whose evaluation is rejected,
        // exactly upstream's 422 `execution` for these. `ScalarOp` (issue
        // #129: a native-histogram trim operator between two scalars)
        // rides the same mapping — upstream surfaces its `scalarBinop`
        // panic as a query execution error (`ev.recover`), never a 5xx.
        // `ExtendedHistogram` (issue #166: a bare anchored/smoothed
        // matrix-selector root over histogram samples) is upstream's
        // `ev.errorf` whole-query abort — the same execution class.
        PromqlError::Unsupported { .. }
        | PromqlError::BadMatching { .. }
        | PromqlError::HistogramBucket { .. }
        | PromqlError::InvalidParameter { .. }
        | PromqlError::LabelSet { .. }
        | PromqlError::ScalarOp { .. }
        | PromqlError::ExtendedHistogram { .. } => {
            (StatusCode::UNPROCESSABLE_ENTITY, "execution", e.to_string())
        }
        // Issue #93: a live `CancelToken` fired because the awaiting
        // request future was already dropped (client disconnect, or the
        // request-deadline layer firing first — `middleware.rs`'s
        // `RequestDeadlineLayer`, `query_timeout`). Matched for
        // exhaustiveness only — by the time this variant exists, the
        // future that would encode this response is gone, so this arm is
        // unreachable in practice.
        //
        // Issue #471 M2 moved this from `408` to `503`: the arm existed to
        // mirror the middleware's `408` convention, and M2 replaced that
        // convention on this surface. After M2 a `408` under `/api/v1/`
        // means the deadline layer on an excluded path and nothing else,
        // which is what makes the bare/envelope assertions unambiguous.
        // `errorType` stays `timeout`. (The reference answers a cancelled
        // query `499`/`canceled`; that pre-existing, unledgered gap is
        // neither created nor closed here.)
        PromqlError::Cancelled => (StatusCode::SERVICE_UNAVAILABLE, "timeout", e.to_string()),
    }
}

fn read_error_parts(e: &ReadError) -> (StatusCode, &'static str, String) {
    match e {
        ReadError::Promql(inner) => promql_error_parts(inner),
        ReadError::Clickhouse(ch) => match ch {
            ChError::Timeout(_) => (StatusCode::SERVICE_UNAVAILABLE, "timeout", e.to_string()),
            // Issue #471 M2. ClickHouse's own server-side
            // `max_execution_time` breach never reaches `ChError::Timeout`
            // — the server returns it as an exception body, which
            // `pulsus_clickhouse::error` routes to `ChError::Server` with
            // code 159 (`TIMEOUT_EXCEEDED`, already named as such in that
            // module's `RETRYABLE_SERVER_CODES`). It is a timeout, so it
            // answers `503`/`timeout` like the other four producers rather
            // than falling through to the `500`/`internal` arm below.
            // Guarded on the exact code: the class must not widen.
            ChError::Server { code: 159, .. } => {
                (StatusCode::SERVICE_UNAVAILABLE, "timeout", e.to_string())
            }
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
        // Issue #227: LogQL-only (the LogQL planner's duration boundary).
        | ReadError::DurationOutOfRange { .. }
        // Issue #343: LogQL-only (the 5-year query-span cap, applied in the
        // LogQL planner); matched here for exhaustiveness, same 400 class.
        | ReadError::QuerySpanTooLong { .. }
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
    /// Issue #376, code review finding 5 — the Prometheus surface's half of
    /// the same leak. See the logs surface's twin for the argument.
    #[tokio::test]
    async fn a_raw_clickhouse_server_error_never_puts_the_server_version_in_the_envelope() {
        let raw = "Code: 241. DB::Exception: Query memory limit exceeded: would use 194.36 \
                   MiB. (MEMORY_LIMIT_EXCEEDED) (version 26.3.17.110 (official build))";
        let err = pulsus_read::logql::ReadError::Clickhouse(pulsus_clickhouse::ChError::Server {
            code: 241,
            message: raw.to_string(),
        });
        let (status, json) = envelope(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        let body = json["error"].as_str().unwrap_or_default();
        assert!(
            !carries_a_server_version_banner(body),
            "the connected server's version leaked into a client envelope: {json}"
        );
        assert!(carries_a_server_version_banner(raw), "premise");
    }

    /// `true` when `body` carries anything SHAPED like a ClickHouse server
    /// version banner — the literal `version ` followed by `<digits>.<digits>`.
    ///
    /// Issue #376: the assertions below used to name `"version 24.8"`. That
    /// made a claim about *any* server version leaking, checked against
    /// *one* server's spelling — on 26.3 the fragment never appears at all,
    /// so the check would have passed while testing nothing. Matching the
    /// shape instead means the next version bump cannot silently retire it.
    fn carries_a_server_version_banner(body: &str) -> bool {
        let mut rest = body;
        while let Some(i) = rest.find("version ") {
            let tail = &rest[i + "version ".len()..];
            let digits: String = tail.chars().take_while(|c| c.is_ascii_digit()).collect();
            let after = &tail[digits.len()..];
            if !digits.is_empty()
                && after.starts_with('.')
                && after[1..].starts_with(|c: char| c.is_ascii_digit())
            {
                return true;
            }
            rest = &rest[i + "version ".len()..];
        }
        false
    }

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

    /// Issue #240: the LogQL pipeline rejection's body is the BARE
    /// reason on this surface too — whole `error` field, byte-exact,
    /// present exactly once even when the reason itself begins
    /// `parse error `.
    #[tokio::test]
    async fn read_error_pipeline_invalid_body_is_the_bare_reason() {
        let reason = "parse error : synthetic prefix-collision probe";
        let err = pulsus_read::logql::ReadError::PipelineInvalid {
            reason: reason.to_string(),
        };
        let (status, json) = envelope(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(json.get("position").is_none());
        let body = json["error"].as_str().expect("error body");
        assert_eq!(body, reason);
        assert_eq!(body.matches("parse error ").count(), 1);
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

    /// Issue #35: the query-text guard's reason rides the existing
    /// `QueryTooBroad(_)` wildcard arm — no mapper change was needed, and
    /// this test proves it.
    #[tokio::test]
    async fn read_error_query_text_bytes_maps_to_422_execution() {
        let err = pulsus_read::logql::ReadError::QueryTooBroad(
            pulsus_read::logql::TooBroadReason::QueryTextBytes {
                rendered_bytes: 9_000_000,
                cap: 8_388_608,
            },
        );
        let (status, json) = envelope(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json["errorType"], "execution");
    }

    /// Issue #138 AC7: the per-query evaluation sample budget's breach
    /// (`TooBroadReason::MetricSamples`) rides the existing
    /// `QueryTooBroad(_)` wildcard arm — 422 `execution`, no mapper
    /// change was needed, and this test proves it (mirrors the #35
    /// query-text precedent above).
    #[tokio::test]
    async fn read_error_metric_samples_maps_to_422_execution() {
        let err = pulsus_read::logql::ReadError::QueryTooBroad(
            pulsus_read::logql::TooBroadReason::MetricSamples { cap: 50_000_000 },
        );
        let (status, json) = envelope(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json["errorType"], "execution");
        assert!(
            json["error"]
                .as_str()
                .unwrap_or_default()
                .contains("promql_max_samples"),
            "{json}"
        );
    }

    /// Issue #398 AC M2: a PromQL read that exhausted ClickHouse's memory
    /// answers **422 `execution`**, not 500 `internal`.
    ///
    /// That envelope is not a compromise — it is what prometheus/prometheus
    /// v3.13.0 returns for its own memory refusal, measured against a
    /// container with data in the store: `--query.max-samples=1` on both
    /// `/api/v1/query` and `/api/v1/query_range` gives 422 with
    /// `"errorType":"execution"` (`web/api/v1/api.go:2236-2237 @ v3.13.0`).
    /// So the metrics surface needs no ledger row at all.
    #[tokio::test]
    async fn promql_read_memory_maps_to_422_execution() {
        let err = pulsus_read::logql::ReadError::QueryTooBroad(
            pulsus_read::logql::TooBroadReason::PromqlReadMemory {
                budget_bytes: 8_589_934_592,
            },
        );
        let (status, json) = envelope(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json["errorType"], "execution");
        assert_eq!(json["status"], "error");
        let body = json["error"].as_str().unwrap_or_default();
        assert!(
            body.contains("reader.promql_read_max_memory_bytes"),
            "the body must name the knob an operator would raise: {json}"
        );
        assert_eq!(
            body,
            pulsus_read::logql::ReadError::QueryTooBroad(
                pulsus_read::logql::TooBroadReason::PromqlReadMemory {
                    budget_bytes: 8_589_934_592,
                },
            )
            .to_string(),
            "the 422 body must be exactly our own rendered message: {json}"
        );
        assert!(
            !body.contains("DB::Exception"),
            "the 422 body must carry only our own message: {json}"
        );
        assert!(
            !carries_a_server_version_banner(body),
            "a server version banner leaked into the 422 body: {json}"
        );
    }

    /// Issue #398: the LogQL and TraceQL read-memory reasons ride the same
    /// `QueryTooBroad(_)` wildcard here, so a future cross-surface route
    /// cannot land on 500 by accident.
    #[tokio::test]
    async fn the_sibling_surfaces_read_memory_reasons_also_map_to_422_execution() {
        for reason in [
            pulsus_read::logql::TooBroadReason::LogqlReadMemory { budget_bytes: 4096 },
            pulsus_read::logql::TooBroadReason::TraceReadMemory { budget_bytes: 4096 },
        ] {
            let (status, json) = envelope(ApiError::Read(
                pulsus_read::logql::ReadError::QueryTooBroad(reason),
            ))
            .await;
            assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{reason:?}");
            assert_eq!(json["errorType"], "execution");
        }
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

    /// Issue #280: an RE2-rejected matcher regex is upstream
    /// Prometheus's 400 `bad_data`, reached here through
    /// `ReadError::Promql` because the verdict comes from ClickHouse —
    /// never the 500 `internal` a raw `ChError::Server` passthrough gave,
    /// and never the 422 `execution` the other non-`Parse` variants take.
    #[tokio::test]
    async fn read_error_invalid_regex_matcher_maps_to_400_bad_data() {
        let err = ReadError::Promql(PromqlError::InvalidRegexMatcher {
            detail: "^(?:\\p{Alphabetic})$, error: invalid character class range: \\p{Alphabetic}"
                .to_string(),
        });
        let (status, json) = envelope(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert_eq!(
            json["error"],
            "invalid regexp: ^(?:\\p{Alphabetic})$, error: invalid character class range: \
             \\p{Alphabetic}"
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

    /// Issue #93 (plan-review note 1), restated by issue #471 M2: a
    /// cancelled offloaded eval maps to `503`, `errorType: "timeout"`.
    /// It used to be `408` to mirror the middleware's convention; M2
    /// replaced that convention on this surface, so after M2 a `408` under
    /// `/api/v1/` means the deadline layer on an excluded path and nothing
    /// else. Not `503`/`unavailable`, and not a made-up `499`.
    #[tokio::test]
    async fn promql_cancelled_error_maps_to_503_timeout() {
        let err = PromqlError::Cancelled;
        let (status, json) = envelope(ApiError::Promql(err)).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
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
    async fn promql_scalar_op_error_maps_to_422_execution_with_the_raw_message() {
        // Issue #129: a native-histogram trim operator between two
        // scalars — the upstream `scalarBinop` panic text verbatim.
        let err = PromqlError::ScalarOp { op: "</" };
        let (status, json) = envelope(ApiError::Promql(err)).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json["errorType"], "execution");
        assert_eq!(
            json["error"],
            "operator \"</\" not allowed for Scalar operations"
        );
    }

    /// Issue #166: a bare anchored/smoothed matrix-selector root over
    /// histogram samples — upstream's `ev.errorf` text verbatim, mapped
    /// like every other well-formed-query eval rejection: 422
    /// `execution`, never a 5xx.
    #[tokio::test]
    async fn promql_extended_histogram_error_maps_to_422_execution_with_the_raw_message() {
        let err = PromqlError::ExtendedHistogram {
            modifier: "anchored",
        };
        let (status, json) = envelope(ApiError::Promql(err)).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json["errorType"], "execution");
        assert_eq!(
            json["error"],
            "anchored modifier is not supported with histograms"
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

    /// Issue #471 M3: the cap sentence is the metrics reference's own,
    /// byte-for-byte — not a `contains` over the bare number, which is
    /// what let the old point-rule wording pass while the predicate was
    /// wrong, and not the LogQL sibling's different spelling.
    #[tokio::test]
    async fn max_resolution_param_error_carries_the_reference_sentence() {
        let (status, json) = envelope(ApiError::Param(ParamError::MaxResolutionExceeded)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert_eq!(
            json["error"],
            "exceeded maximum resolution of 11,000 points per timeseries. \
             Try decreasing the query resolution (?step=XX)"
        );
    }

    // -----------------------------------------------------------------
    // Issue #471 M2 — the deadline producers
    // -----------------------------------------------------------------

    /// The two producer messages, pinned as literals. `{:?}` on a
    /// `std::time::Duration` renders `120s`; `HumanDuration`'s `Display`
    /// renders the same duration `120000ms`, so using it reddens here.
    #[test]
    fn deadline_producer_messages_are_the_two_pinned_literals() {
        assert_eq!(
            DeadlineProducer::ServerRequest(std::time::Duration::from_secs(120)).message(),
            "request exceeded the server deadline of 120s (PULSUS_QUERY_TIMEOUT)"
        );
        assert_eq!(
            DeadlineProducer::RequestedTimeout(std::time::Duration::from_millis(1)).message(),
            "query exceeded the requested timeout of 1ms (timeout parameter)"
        );
    }

    #[tokio::test]
    async fn deadline_maps_to_503_timeout_with_the_producer_message() {
        let (status, json) = envelope(ApiError::Deadline(DeadlineProducer::ServerRequest(
            std::time::Duration::from_secs(3),
        )))
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "timeout");
        assert_eq!(
            json["error"],
            "request exceeded the server deadline of 3s (PULSUS_QUERY_TIMEOUT)"
        );
    }

    /// Issue #471 M2: ClickHouse's server-side `max_execution_time` breach
    /// arrives as `ChError::Server { code: 159 }` and is a timeout, not an
    /// internal error. The two neighbouring codes are the control — the
    /// class must not widen into a range.
    #[tokio::test]
    async fn clickhouse_server_timeout_code_is_503_timeout_and_the_class_does_not_widen() {
        let err = ReadError::Clickhouse(ChError::Server {
            code: 159,
            message: "Timeout exceeded".to_string(),
        });
        let (status, json) = envelope(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "timeout");

        for code in [158i32, 160i32] {
            let err = ReadError::Clickhouse(ChError::Server {
                code,
                message: "other".to_string(),
            });
            let (status, json) = envelope(ApiError::Read(err)).await;
            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "code {code}");
            assert_eq!(json["errorType"], "internal", "code {code}");
        }
    }
}
