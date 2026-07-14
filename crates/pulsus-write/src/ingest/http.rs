//! The `POST /v1/logs` axum handler (docs/api.md §1.1): decompress ->
//! prost-decode -> `otlp_logs::parse` -> hand rows to a [`LogSink`] ->
//! render the OTLP response. A thin layer over the pure parser — no
//! batching, no ClickHouse writes (architect plan, "out of scope").
//!
//! [`ingest`] is the state-agnostic core (issue #15 architect plan):
//! `&dyn LogSink` rather than a generic `State<Arc<S>>` extractor, because
//! `pulsus-server`'s concrete sink (`WriterSink`, wrapping the
//! async-filled writer slot) cannot implement `FromRef<AppState>` through
//! an `Arc` — `Arc` is not `#[fundamental]`, so `Arc<WriterSink>:
//! FromRef<AppState>` is an orphan-rule violation from this crate's side.
//! A `&dyn LogSink` core sidesteps the state-type gymnastics entirely: the
//! server's own thin `axum::extract::State<AppState>` handler pulls its
//! sink out of `AppState` and calls straight into [`ingest`]. [`logs`]
//! (this crate's own generic-`State` mount point, used by its own tests
//! and any caller with a concrete, `FromRef`-able sink type) is now a
//! one-line delegate to it.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::Response;
use http_body_util::BodyExt;
use prost::Message;

use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse,
};

use crate::error::LogsIngestError;
use crate::ingest::decompress::{self, Encoding};
use crate::ingest::{Backpressure, LogSink};
use crate::protocols::otlp_logs;

/// `X-Pulsus-Async` request header (docs/api.md "Request headers"): `1`
/// selects async-mode (enqueue, `202`); absent or any other value selects
/// sync-mode (confirm flush, `200`) — this handler has no `Config` to read
/// a configured default from (out of scope, architect plan: "sync/async
/// flush confirmation beyond reading `X-Pulsus-Async`"), so sync is the
/// hardcoded default for a missing header.
const ASYNC_HEADER: &str = "x-pulsus-async";

const PROTOBUF_CONTENT_TYPE: HeaderValue = HeaderValue::from_static("application/x-protobuf");

/// Hand-rolled minimal `google.rpc.Status` (architect plan amendment 2):
/// just `code`/`message` at their real `google.rpc.Status` field tags (1,
/// 2) — no `details`, which this receiver never needs. The whole-request
/// error body for every failure class this handler returns (400/429/500).
// `::prost::Message`'s derive macro generates its own `Debug` impl (the
// generated OTLP message types below follow the same pattern), so `Debug`
// is not in this derive list.
#[derive(Clone, PartialEq, ::prost::Message)]
struct Status {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
}

/// `POST /v1/logs`: decompress, decode, parse, admit, respond — the
/// state-agnostic core every mount point (this crate's own [`logs`],
/// `pulsus-server`'s `ingest_logs`) delegates to.
///
/// Extracts `Body` rather than `Bytes` (plan amendment 3, code review
/// finding): the `Bytes` extractor engages axum's `DefaultBodyLimit` (2 MiB
/// default) and rejects an over-limit body with a plain `413` before this
/// handler ever runs, which pre-empts the documented 64 MiB decompressed-
/// size cap and its `OversizeBody -> 400/code=3` OTLP error mapping.
/// `Body` bypasses `DefaultBodyLimit` entirely, so [`read_capped_body`]
/// becomes the sole bound — no `DefaultBodyLimit::disable()` layer needed.
pub async fn ingest(sink: &dyn LogSink, headers: HeaderMap, body: Body) -> Response {
    let now_ns = now_unix_nanos();

    let body = match read_capped_body(body, decompress::MAX_DECOMPRESSED_BYTES).await {
        Ok(body) => body,
        Err(err) => return error_response(err),
    };

    let request = match decode_request(&headers, &body) {
        Ok(request) => request,
        Err(err) => return error_response(err),
    };

    let parsed = otlp_logs::parse(&request, now_ns);
    let rejected = parsed.rejected;
    let rejected_message = parsed.rejected_message.clone();

    if is_async(&headers) {
        return match sink.admit(parsed) {
            Ok(()) => export_response(StatusCode::ACCEPTED, rejected, rejected_message),
            Err(Backpressure) => backpressure_response(),
        };
    }

    match sink.admit_flush(parsed) {
        Ok(wait) => match wait.await {
            Ok(()) => export_response(StatusCode::OK, rejected, rejected_message),
            Err(err) => error_response(err),
        },
        Err(Backpressure) => backpressure_response(),
    }
}

/// This crate's own generic-`State` mount point: `S` is the concrete
/// [`LogSink`] the caller mounts this handler with via
/// `axum::routing::post(logs::<S>).with_state(Arc::new(sink))`. A one-line
/// delegate to [`ingest`] — see this module's doc comment for why
/// `pulsus-server` cannot reuse this generic form and mounts [`ingest`]
/// directly instead.
pub async fn logs<S>(State(sink): State<Arc<S>>, headers: HeaderMap, body: Body) -> Response
where
    S: LogSink + 'static,
{
    ingest(sink.as_ref(), headers, body).await
}

/// `true` when `X-Pulsus-Async: 1` selects async-mode admission.
fn is_async(headers: &HeaderMap) -> bool {
    headers
        .get(ASYNC_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim() == "1")
        .unwrap_or(false)
}

/// Reads `body` to completion, frame-by-frame, rejecting anything whose
/// running length would exceed `cap` before it is fully buffered (plan
/// amendment 3) rather than allocating an unbounded buffer first — mirrors
/// [`decompress::read_capped`]'s bounded-read pattern, one layer further
/// out. `cap` bounds the raw, pre-decompress *encoded* body length; it
/// reuses [`decompress::MAX_DECOMPRESSED_BYTES`]'s value for convenience,
/// but is a distinct guard with its own purpose — an OOM/DoS bound on how
/// much compressed-but-unparsed wire data this handler will buffer, not a
/// derived consequence of the decompressed cap. The two are independent:
/// a pathological payload whose *encoded* size exceeds `cap` is rejected
/// here (400/code=3) even if its *decompressed* size would have fit under
/// the inner per-encoding decompressed cap (in [`decode_request`]) — e.g.
/// an incompressible payload just over 64 MiB on the wire. A frame read
/// failure (e.g. the client disconnects mid-upload) is not attributable to
/// the payload's shape, so it maps to `BodyRead` (500/13), not an oversize/
/// malformed-payload class.
async fn read_capped_body(mut body: Body, cap: usize) -> Result<Vec<u8>, LogsIngestError> {
    let mut out = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|source| LogsIngestError::BodyRead(source.to_string()))?;
        let Ok(data) = frame.into_data() else {
            // A trailers frame, not data — OTLP/HTTP requests carry none;
            // skip rather than treat as an error.
            continue;
        };
        if out.len() + data.len() > cap {
            return Err(LogsIngestError::OversizeBody { limit: cap });
        }
        out.extend_from_slice(&data);
    }
    Ok(out)
}

/// Reads `Content-Encoding` (defaulting to `identity` when absent or
/// empty), decompresses, and prost-decodes the request body.
fn decode_request(
    headers: &HeaderMap,
    body: &[u8],
) -> Result<ExportLogsServiceRequest, LogsIngestError> {
    let encoding = content_encoding(headers)?;
    let decompressed = decompress::decompress(encoding, body)?;
    otlp_logs::decode(&decompressed)
}

fn content_encoding(headers: &HeaderMap) -> Result<Encoding, LogsIngestError> {
    let Some(value) = headers.get(header::CONTENT_ENCODING) else {
        return Ok(Encoding::Identity);
    };
    let value = value.to_str().map_err(|_| {
        LogsIngestError::UnsupportedEncoding("<non-UTF-8 Content-Encoding value>".to_string())
    })?;
    if value.trim().is_empty() {
        Ok(Encoding::Identity)
    } else {
        Encoding::from_header_value(value)
    }
}

/// Wall-clock now, nanoseconds since the Unix epoch — the handler-
/// injected `now_ns` argument to the otherwise-pure `otlp_logs::parse`
/// (architect plan: `parse` stays deterministic in its arguments; only the
/// handler touches the clock). `SystemTime::now()` predating the Unix
/// epoch is a broken-clock scenario, not one that happens on any deployed
/// system; it degrades to `0` rather than panicking.
fn now_unix_nanos() -> i64 {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(elapsed.as_nanos()).unwrap_or(i64::MAX)
}

/// Maps a whole-request [`LogsIngestError`] to its `(HTTP status,
/// google.rpc.Status.code)` pair (architect plan amendment 2): malformed
/// protobuf / failed decompression / oversize body -> 400 / `code = 3`
/// (`INVALID_ARGUMENT`); anything else (not attributable to the request
/// payload) -> 500 / `code = 13` (`INTERNAL`).
fn classify(err: &LogsIngestError) -> (StatusCode, i32) {
    match err {
        LogsIngestError::UnsupportedEncoding(_)
        | LogsIngestError::Decompress { .. }
        | LogsIngestError::OversizeBody { .. }
        | LogsIngestError::Decode(_) => (StatusCode::BAD_REQUEST, 3),
        LogsIngestError::BodyRead(_) | LogsIngestError::FlushFailed(_) => {
            (StatusCode::INTERNAL_SERVER_ERROR, 13)
        }
    }
}

fn error_response(err: LogsIngestError) -> Response {
    let (status, code) = classify(&err);
    status_response(status, code, err.to_string())
}

/// Sink backpressure -> 429 / `code = 8` (`RESOURCE_EXHAUSTED`, architect
/// plan amendment 2). Not routed through [`classify`]: `Backpressure`
/// originates from the sink seam, not from parsing the request, so it is
/// not a variant of `LogsIngestError`.
fn backpressure_response() -> Response {
    status_response(
        StatusCode::TOO_MANY_REQUESTS,
        8,
        "sink is applying backpressure: buffers are full".to_string(),
    )
}

fn status_response(status: StatusCode, code: i32, message: String) -> Response {
    let body = Status { code, message }.encode_to_vec();
    protobuf_response(status, body)
}

/// Success/accepted response body: an `ExportLogsServiceResponse` carrying
/// `partial_success` iff any records were rejected (architect plan: 200
/// with OTLP partial-success message when applicable). Used for both the
/// sync `200` and the async `202` paths — `parse` already ran (and knows
/// `rejected`) before either admission call, so both report it.
fn export_response(
    status: StatusCode,
    rejected: u64,
    rejected_message: Option<String>,
) -> Response {
    let partial_success = (rejected > 0).then(|| ExportLogsPartialSuccess {
        // `rejected` cannot realistically exceed `i64::MAX` (bounded by
        // one request's record count); saturate rather than panic on a
        // pathological/malicious count instead of an infallible cast.
        rejected_log_records: i64::try_from(rejected).unwrap_or(i64::MAX),
        error_message: rejected_message.unwrap_or_default(),
    });
    let body = ExportLogsServiceResponse { partial_success }.encode_to_vec();
    protobuf_response(status, body)
}

fn protobuf_response(status: StatusCode, body: Vec<u8>) -> Response {
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, PROTOBUF_CONTENT_TYPE);
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::Router;
    use axum::routing::post;
    use std::sync::Mutex;
    use tower::ServiceExt;

    use crate::ingest::FlushWait;
    use crate::protocols::otlp_logs::ParsedLogs;
    use opentelemetry_proto::tonic::common::v1::AnyValue;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};

    /// A [`LogSink`] test double whose `admit`/`admit_flush` outcome is
    /// fixed at construction and whose admitted batches are recorded for
    /// inspection.
    struct MockSink {
        outcome: Outcome,
        admitted: Mutex<Vec<ParsedLogs>>,
    }

    #[derive(Clone)]
    enum Outcome {
        Admit,
        Backpressure,
        FlushFails,
    }

    impl MockSink {
        fn new(outcome: Outcome) -> Arc<MockSink> {
            Arc::new(MockSink {
                outcome,
                admitted: Mutex::new(Vec::new()),
            })
        }
    }

    impl LogSink for MockSink {
        fn admit(&self, batch: ParsedLogs) -> Result<(), Backpressure> {
            self.admitted.lock().unwrap().push(batch);
            match self.outcome {
                Outcome::Admit | Outcome::FlushFails => Ok(()),
                Outcome::Backpressure => Err(Backpressure),
            }
        }

        fn admit_flush(&self, batch: ParsedLogs) -> Result<FlushWait, Backpressure> {
            self.admitted.lock().unwrap().push(batch);
            match self.outcome {
                Outcome::Admit => Ok(FlushWait::new(async { Ok(()) })),
                Outcome::FlushFails => Ok(FlushWait::new(async {
                    Err(LogsIngestError::FlushFailed("writer shut down".to_string()))
                })),
                Outcome::Backpressure => Err(Backpressure),
            }
        }
    }

    fn router(sink: Arc<MockSink>) -> Router {
        Router::new()
            .route("/v1/logs", post(logs::<MockSink>))
            .with_state(sink)
    }

    fn valid_request_body() -> Vec<u8> {
        let record = LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000,
            body: Some(AnyValue {
                value: Some(Value::StringValue("hello".to_string())),
            }),
            ..Default::default()
        };
        let req = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: None,
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![record],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        req.encode_to_vec()
    }

    async fn post_body(router: Router, body: Vec<u8>, headers: &[(&str, &str)]) -> Response {
        let mut builder = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/logs");
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        let request = builder.body(Body::from(body)).unwrap();
        router.oneshot(request).await.unwrap()
    }

    #[tokio::test]
    async fn sync_mode_admits_via_admit_flush_and_returns_200() {
        let sink = MockSink::new(Outcome::Admit);
        let res = post_body(router(sink.clone()), valid_request_body(), &[]).await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(sink.admitted.lock().unwrap().len(), 1);
    }

    /// Request `Content-Type` handling (code review follow-up, issue #15):
    /// this handler decodes every request body as protobuf unconditionally
    /// — it keys decompression off `Content-Encoding` only ([`content_encoding`]),
    /// never inspects the request's `Content-Type` at all. An explicit
    /// `content-type: application/x-protobuf` header (what a real OTLP/HTTP
    /// exporter — including the collector's `otlphttp` re-export after
    /// translating an operator's OTLP/JSON push — actually sends) must
    /// therefore admit identically to no header being present at all, which
    /// this pins byte-for-byte. The JSON-in/protobuf-out half of "protobuf
    /// accepted, JSON via collector translation" is exercised end-to-end by
    /// `pulsus-e2e`'s `logs_roundtrip` scenario, which pushes OTLP/JSON into
    /// a real collector and asserts the protobuf re-export this handler
    /// receives round-trips correctly.
    #[tokio::test]
    async fn an_explicit_protobuf_content_type_header_admits_identically_to_no_header() {
        let body = valid_request_body();

        let sink_with_header = MockSink::new(Outcome::Admit);
        let with_header = post_body(
            router(sink_with_header.clone()),
            body.clone(),
            &[("content-type", "application/x-protobuf")],
        )
        .await;

        let sink_without_header = MockSink::new(Outcome::Admit);
        let without_header = post_body(router(sink_without_header.clone()), body, &[]).await;

        assert_eq!(with_header.status(), StatusCode::OK);
        assert_eq!(with_header.status(), without_header.status());
        assert_eq!(sink_with_header.admitted.lock().unwrap().len(), 1);
        assert_eq!(sink_without_header.admitted.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn async_mode_admits_via_admit_and_returns_202() {
        let sink = MockSink::new(Outcome::Admit);
        let res = post_body(
            router(sink.clone()),
            valid_request_body(),
            &[("x-pulsus-async", "1")],
        )
        .await;
        assert_eq!(res.status(), StatusCode::ACCEPTED);
        assert_eq!(sink.admitted.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn async_header_value_zero_stays_sync() {
        let sink = MockSink::new(Outcome::Admit);
        let res = post_body(
            router(sink),
            valid_request_body(),
            &[("x-pulsus-async", "0")],
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn malformed_body_returns_400_with_status_code_3() {
        let sink = MockSink::new(Outcome::Admit);
        let res = post_body(router(sink), b"not a protobuf message".to_vec(), &[]).await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let status = decode_status_body(res).await;
        assert_eq!(status.code, 3);
    }

    #[tokio::test]
    async fn unsupported_content_encoding_returns_400_with_status_code_3() {
        let sink = MockSink::new(Outcome::Admit);
        let res = post_body(
            router(sink),
            valid_request_body(),
            &[("content-encoding", "br")],
        )
        .await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let status = decode_status_body(res).await;
        assert_eq!(status.code, 3);
    }

    #[tokio::test]
    async fn sink_backpressure_returns_429_with_status_code_8() {
        let sink = MockSink::new(Outcome::Backpressure);
        let res = post_body(router(sink), valid_request_body(), &[]).await;
        assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);
        let status = decode_status_body(res).await;
        assert_eq!(status.code, 8);
    }

    #[tokio::test]
    async fn async_mode_sink_backpressure_also_returns_429_with_status_code_8() {
        let sink = MockSink::new(Outcome::Backpressure);
        let res = post_body(
            router(sink),
            valid_request_body(),
            &[("x-pulsus-async", "1")],
        )
        .await;
        assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);
        let status = decode_status_body(res).await;
        assert_eq!(status.code, 8);
    }

    #[tokio::test]
    async fn flush_failure_returns_500_with_status_code_13() {
        let sink = MockSink::new(Outcome::FlushFails);
        let res = post_body(router(sink), valid_request_body(), &[]).await;
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let status = decode_status_body(res).await;
        assert_eq!(status.code, 13);
    }

    /// Builds a valid, `identity`-encoded `ExportLogsServiceRequest` whose
    /// encoded size is `target_len` bytes, padded via a single log record's
    /// string body — used to exercise sizes axum's 2 MiB `DefaultBodyLimit`
    /// would reject if the handler still extracted `Bytes` (plan amendment
    /// 3, code review finding).
    fn request_body_of_len(target_len: usize) -> Vec<u8> {
        let mut record = LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000,
            body: Some(AnyValue {
                value: Some(Value::StringValue(String::new())),
            }),
            ..Default::default()
        };
        let req = |record: LogRecord| ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: None,
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![record],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let base_len = req(record.clone()).encode_to_vec().len();
        let padding = target_len.saturating_sub(base_len);
        record.body = Some(AnyValue {
            value: Some(Value::StringValue("a".repeat(padding))),
        });
        let body = req(record).encode_to_vec();
        assert!(body.len() >= target_len, "padding undershot target_len");
        body
    }

    #[tokio::test]
    async fn body_above_axum_default_limit_but_within_the_cap_is_accepted() {
        // axum 0.8's `DefaultBodyLimit` default is 2 MiB; this body sits
        // comfortably above that and well under the 64 MiB decompressed
        // cap. A `Bytes`-extracting handler would reject this with a plain
        // `413` before ever running (the code-review finding this test
        // guards against); `Body` extraction must accept it.
        let target_len = 3 * 1024 * 1024;
        let sink = MockSink::new(Outcome::Admit);
        let res = post_body(router(sink.clone()), request_body_of_len(target_len), &[]).await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(sink.admitted.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn body_over_the_64_mib_cap_returns_400_with_status_code_3() {
        // Raw length alone exceeds `MAX_DECOMPRESSED_BYTES`; `read_capped_body`
        // must reject it before `decode_request` (and thus `otlp_logs::decode`)
        // ever runs, proving the OversizeBody -> 400/code=3 OTLP contract now
        // covers the HTTP path (not axum's `413`).
        let body = vec![0u8; decompress::MAX_DECOMPRESSED_BYTES + 1024];
        let sink = MockSink::new(Outcome::Admit);
        let res = post_body(router(sink), body, &[]).await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let status = decode_status_body(res).await;
        assert_eq!(status.code, 3);
    }

    #[tokio::test]
    async fn success_response_reports_partial_success_when_records_were_rejected() {
        let record_bad = LogRecord {
            time_unix_nano: u64::MAX,
            body: Some(AnyValue {
                value: Some(Value::StringValue("bad".to_string())),
            }),
            ..Default::default()
        };
        let record_good = LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000,
            body: Some(AnyValue {
                value: Some(Value::StringValue("good".to_string())),
            }),
            ..Default::default()
        };
        let req = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: None,
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![record_bad, record_good],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let sink = MockSink::new(Outcome::Admit);
        let res = post_body(router(sink), req.encode_to_vec(), &[]).await;
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let response = ExportLogsServiceResponse::decode(bytes.as_ref()).unwrap();
        let partial = response.partial_success.expect("partial success is set");
        assert_eq!(partial.rejected_log_records, 1);
        assert!(!partial.error_message.is_empty());
    }

    async fn decode_status_body(res: Response) -> Status {
        assert_eq!(
            res.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/x-protobuf"
        );
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        Status::decode(bytes.as_ref()).unwrap()
    }
}
