//! The `POST /v1/logs`, `POST /v1/metrics`, `POST /v1/traces`, and
//! `POST /api/v1/write` axum handlers (docs/api.md §1.1-1.2): decompress ->
//! prost-decode -> `otlp_logs::parse`/`otlp_metrics::parse`/
//! `otlp_traces::parse`/`remote_write::parse` -> hand rows to a
//! [`LogSink`]/[`MetricSink`]/[`TraceSink`] -> render the response. A thin
//! layer over the pure parsers — no batching, no ClickHouse writes
//! (architect plan, "out of scope"). [`ingest_metrics`]/[`metrics`] (issue
//! #27) and [`ingest_traces`]/[`traces`] (issue #54) reuse every piece of
//! [`ingest`]/[`logs`]'s (issue #8/#15) machinery below — capped body
//! reads, decompression, `google.rpc.Status` error rendering,
//! classification, and the sync/async admission branch — the only
//! signal-specific additions are [`decode_metrics_request`]/
//! [`decode_traces_request`] and [`export_metrics_response`]/
//! [`export_traces_response`]. [`ingest_remote_write`] (issue #28) reuses
//! the same capped-body-read/classification/sync-async-admission shape but
//! renders remote-write-shaped responses instead (empty `204`/`202`,
//! plain-text errors — see its own doc comment for why it cannot reuse
//! [`export_metrics_response`]/[`status_response`]).
//!
//! [`ingest`]/[`ingest_metrics`]/[`ingest_remote_write`] are the
//! state-agnostic cores (issue #15 architect plan): `&dyn LogSink`/
//! `&dyn MetricSink` rather than a generic `State<Arc<S>>` extractor,
//! because `pulsus-server`'s concrete sinks (`WriterSink`/
//! `MetricWriterSink`, each wrapping an async-filled writer slot) cannot
//! implement `FromRef<AppState>` through an `Arc` — `Arc` is not
//! `#[fundamental]`, so `Arc<WriterSink>: FromRef<AppState>` is an
//! orphan-rule violation from this crate's side. A `&dyn LogSink`/
//! `&dyn MetricSink` core sidesteps the state-type gymnastics entirely:
//! the server's own thin `axum::extract::State<AppState>` handler pulls
//! its sink out of `AppState` and calls straight into [`ingest`]/
//! [`ingest_metrics`]/[`ingest_remote_write`]. [`logs`]/[`metrics`] (this
//! crate's own generic-`State` mount points, used by its own tests and any
//! caller with a concrete, `FromRef`-able sink type) are now one-line
//! delegates to them; [`ingest_remote_write`] has no such generic-`State`
//! wrapper (no test or caller has needed one yet).

use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::Response;
use http_body_util::BodyExt;
use prost::Message;
use pulsus_config::ExpHistogramMode;

use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsPartialSuccess, ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTracePartialSuccess, ExportTraceServiceRequest, ExportTraceServiceResponse,
};

use crate::error::LogsIngestError;
use crate::ingest::decompress::{self, Encoding};
use crate::ingest::metrics::MetricSink;
use crate::ingest::traces::TraceSink;
use crate::ingest::{Backpressure, LogSink};
use crate::protocols::{loki_push, otlp_logs, otlp_metrics, otlp_traces, remote_write, zipkin};

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

/// `POST /v1/metrics` (issue #27): the metrics analog of [`ingest`] —
/// identical decompress/decode/parse/admit/respond shape, reusing every
/// shared helper below verbatim. See this module's doc comment for why
/// `pulsus-server` mounts this `&dyn MetricSink` core directly rather than
/// [`metrics`]'s generic-`State` form.
pub async fn ingest_metrics(
    sink: &dyn MetricSink,
    headers: HeaderMap,
    body: Body,
    mode: ExpHistogramMode,
) -> Response {
    let now_ns = now_unix_nanos();

    let body = match read_capped_body(body, decompress::MAX_DECOMPRESSED_BYTES).await {
        Ok(body) => body,
        Err(err) => return error_response(err),
    };

    let request = match decode_metrics_request(&headers, &body) {
        Ok(request) => request,
        Err(err) => return error_response(err),
    };

    // Fallible, unlike the logs parse: the metrics parser's expansion
    // budget (`otlp_metrics::MAX_EXPANDED_BYTES`, a structural whole-request
    // bound, issue #62) surfaces here as the same 400/`code = 3`
    // classification a decode failure gets. `mode` (issue #120) selects the
    // OTLP exponential-histogram storage path.
    let parsed = match otlp_metrics::parse(&request, now_ns, mode) {
        Ok(parsed) => parsed,
        Err(err) => return error_response(err),
    };
    let rejected = parsed.rejected;
    let rejected_message = parsed.rejected_message.clone();

    if is_async(&headers) {
        return match sink.admit(parsed) {
            Ok(()) => export_metrics_response(StatusCode::ACCEPTED, rejected, rejected_message),
            Err(Backpressure) => backpressure_response(),
        };
    }

    match sink.admit_flush(parsed) {
        Ok(wait) => match wait.await {
            Ok(()) => export_metrics_response(StatusCode::OK, rejected, rejected_message),
            Err(err) => error_response(err),
        },
        Err(Backpressure) => backpressure_response(),
    }
}

/// This crate's own generic-`State` mount point for [`ingest_metrics`] —
/// mirrors [`logs`] exactly.
pub async fn metrics<S>(State(sink): State<Arc<S>>, headers: HeaderMap, body: Body) -> Response
where
    S: MetricSink + 'static,
{
    // This crate's own generic mount point carries no `Config`; it defaults
    // to `Classic` (current behavior). The server threads the configured
    // mode through `pulsus_write::ingest_metrics` directly.
    ingest_metrics(sink.as_ref(), headers, body, ExpHistogramMode::Classic).await
}

/// `POST /v1/traces` (issue #54): the traces analog of [`ingest`] —
/// identical decompress/decode/parse/admit/respond shape, reusing every
/// shared helper below verbatim. See this module's doc comment for why
/// `pulsus-server` mounts this `&dyn TraceSink` core directly rather than
/// [`traces`]'s generic-`State` form.
pub async fn ingest_traces(sink: &dyn TraceSink, headers: HeaderMap, body: Body) -> Response {
    let now_ns = now_unix_nanos();

    let body = match read_capped_body(body, decompress::MAX_DECOMPRESSED_BYTES).await {
        Ok(body) => body,
        Err(err) => return error_response(err),
    };

    let request = match decode_traces_request(&headers, &body) {
        Ok(request) => request,
        Err(err) => return error_response(err),
    };

    // Fallible, unlike the logs/metrics parses: the traces parser's
    // expansion budget (`otlp_traces::MAX_EXPANDED_BYTES`, a structural
    // whole-request bound) surfaces here as the same 400/`code = 3`
    // classification a decode failure gets.
    let parsed = match otlp_traces::parse(&request, now_ns) {
        Ok(parsed) => parsed,
        Err(err) => return error_response(err),
    };
    let rejected = parsed.rejected;
    let rejected_message = parsed.rejected_message.clone();

    if is_async(&headers) {
        return match sink.admit(parsed) {
            Ok(()) => export_traces_response(StatusCode::ACCEPTED, rejected, rejected_message),
            Err(Backpressure) => backpressure_response(),
        };
    }

    match sink.admit_flush(parsed) {
        Ok(wait) => match wait.await {
            Ok(()) => export_traces_response(StatusCode::OK, rejected, rejected_message),
            Err(err) => error_response(err),
        },
        Err(Backpressure) => backpressure_response(),
    }
}

/// This crate's own generic-`State` mount point for [`ingest_traces`] —
/// mirrors [`logs`]/[`metrics`] exactly.
pub async fn traces<S>(State(sink): State<Arc<S>>, headers: HeaderMap, body: Body) -> Response
where
    S: TraceSink + 'static,
{
    ingest_traces(sink.as_ref(), headers, body).await
}

/// `POST /api/v1/write` (issue #28, docs/api.md §1.2): Prometheus
/// remote-write's `prompb.WriteRequest`. Structurally the same decompress/
/// decode/parse/admit/respond shape as [`ingest`]/[`ingest_metrics`], with
/// two deliberate differences (architect plan):
///
/// 1. **Decompression is unconditional block-snappy**, never dispatched on
///    `Content-Encoding` — the RW spec and Prometheus's own
///    `remote.DecodeWriteRequest` always `snappy.Decode` the body
///    regardless of (and typically alongside) an explicit `Content-
///    Encoding: snappy` header, so this handler never calls
///    [`content_encoding`] and never returns `UnsupportedEncoding`.
/// 2. **Responses are remote-write-shaped, not OTLP protobuf**: an empty
///    `204`/`202` on success (never [`export_metrics_response`]'s OTLP
///    partial-success message — remote-write has no partial-success
///    envelope, so a per-series drop only surfaces via the writer's
///    `rejected_total` metric/log, never in the response body) and a
///    plain-text `400`/`429`/`500` on error (never [`status_response`]'s
///    `google.rpc.Status` protobuf — a real Prometheus/collector sender
///    must not have to guess whether an error body is protobuf or text).
pub async fn ingest_remote_write(
    sink: &dyn MetricSink,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let now_ns = now_unix_nanos();

    let body = match read_capped_body(body, decompress::MAX_DECOMPRESSED_BYTES).await {
        Ok(body) => body,
        Err(err) => return rw_error_response(&err),
    };

    let request = match decode_remote_write_request(&body) {
        Ok(request) => request,
        Err(err) => return rw_error_response(&err),
    };

    // Fallible, unlike the logs parse: the remote-write parser's expansion
    // budget (`remote_write::MAX_EXPANDED_BYTES`, a structural whole-request
    // bound, issue #62) surfaces here as the same plain-text 400 a decode
    // failure gets.
    let parsed = match remote_write::parse(&request, now_ns) {
        Ok(parsed) => parsed,
        Err(err) => return rw_error_response(&err),
    };

    if is_async(&headers) {
        return match sink.admit(parsed) {
            Ok(()) => rw_success_response(StatusCode::ACCEPTED),
            Err(Backpressure) => rw_backpressure_response(),
        };
    }

    match sink.admit_flush(parsed) {
        Ok(wait) => match wait.await {
            Ok(()) => rw_success_response(StatusCode::NO_CONTENT),
            Err(err) => rw_error_response(&err),
        },
        Err(Backpressure) => rw_backpressure_response(),
    }
}

/// `POST /loki/api/v1/push` (issue #77, docs/api.md §8.2): the flag-gated
/// Loki log push receiver. A **decoder** feeding the *existing* log-storage
/// path — no new read path, no schema change — so it is the structural
/// analog of [`ingest_remote_write`] (empty `204`/`202` success, plain-text
/// errors, the shared 64 MiB [`read_capped_body`] cap), but with a
/// content-negotiated body and the OTLP-log admission unit ([`ParsedLogs`]):
///
/// - **Both encodings** (task-manager Q2 adjudication, oracle-pinned against
///   grafana/loki 3.4.2): `Content-Type` containing `application/json` →
///   the JSON path (honoring `Content-Encoding`, since a Loki JSON body may
///   be gzip); anything else or absent → the protobuf path, which — like
///   [`decode_remote_write_request`] and Loki's own server — **always
///   block-snappy-decompresses** (a Loki agent sends
///   `application/x-protobuf` with a snappy body and no `Content-Encoding`).
///   Uncompressed protobuf is therefore unsupported, exactly as upstream.
/// - **All-or-nothing** (Loki has no partial-success channel): a malformed
///   body / label string / timestamp is a whole-request `LokiDecode` 400,
///   never a per-entry drop — `parsed.rejected` is always 0 here.
/// - **Structured metadata accept-and-drop** (task-manager Q1): parsed only
///   as far as prost's byte-skip (the tag is undeclared), never stored —
///   `log_samples` has no per-entry column (docs/api.md §8.2, follow-up
///   filed).
///
/// Response codes are oracle-pinned where Loki has an equivalent (204
/// success both encodings, 400 plain-text malformed/oversize) and PulsusDB-
/// contract where it does not (202 async, 429 backpressure) — see the issue
/// #77 v2 response matrix.
pub async fn ingest_loki_push(sink: &dyn LogSink, headers: HeaderMap, body: Body) -> Response {
    let now_ns = now_unix_nanos();

    let body = match read_capped_body(body, decompress::MAX_DECOMPRESSED_BYTES).await {
        Ok(body) => body,
        // Reuses the signal-neutral empty-`204`/plain-text response family
        // (the `rw_*` helpers) — Loki and remote-write share that exact
        // response shape.
        Err(err) => return rw_error_response(&err),
    };

    let parsed = match decode_loki_push(&headers, &body, now_ns) {
        Ok(parsed) => parsed,
        Err(err) => return rw_error_response(&err),
    };

    if is_async(&headers) {
        return match sink.admit(parsed) {
            Ok(()) => rw_success_response(StatusCode::ACCEPTED),
            Err(Backpressure) => rw_backpressure_response(),
        };
    }

    match sink.admit_flush(parsed) {
        Ok(wait) => match wait.await {
            Ok(()) => rw_success_response(StatusCode::NO_CONTENT),
            Err(err) => rw_error_response(&err),
        },
        Err(Backpressure) => rw_backpressure_response(),
    }
}

/// Content-negotiates and decodes a Loki push body into [`ParsedLogs`]. See
/// [`ingest_loki_push`]'s doc comment for the negotiation rule; the two
/// paths funnel through `loki_push::parse_json`/`parse_protobuf`, both of
/// which route labels through the identical `LabelSet::from_normalized` +
/// `stream_fingerprint` seam `otlp_logs::parse` uses.
fn decode_loki_push(
    headers: &HeaderMap,
    body: &[u8],
    now_ns: i64,
) -> Result<otlp_logs::ParsedLogs, LogsIngestError> {
    if is_json_content_type(headers) {
        let encoding = content_encoding(headers)?;
        let decompressed = decompress::decompress(encoding, body)?;
        loki_push::parse_json(&decompressed, now_ns)
    } else {
        let decompressed = decompress::decompress(Encoding::Snappy, body)?;
        let request = loki_push::decode_protobuf(&decompressed)?;
        loki_push::parse_protobuf(&request, now_ns)
    }
}

/// `POST /api/v2/spans` (+ `/tempo/spans`) (issue #75, docs/api.md §8.2):
/// the flag-gated Zipkin v2 JSON trace receiver. A **decoder + model
/// adapter** feeding the *existing* trace-storage path — it decodes the
/// Zipkin span array, adapts each span into one self-contained OTLP
/// `ResourceSpans` (`zipkin::to_otlp`), and hands the batch to the same
/// `otlp_traces::parse` the native `/v1/traces` receiver uses (so
/// `payload_type = 1`, the id/attr/expansion contracts, and the whole read
/// path are byte-identical). Structurally the analog of [`ingest_loki_push`]
/// (empty-body success, plain-text errors, the shared 64 MiB
/// [`read_capped_body`] cap), with two Zipkin-specific choices:
///
/// - **Success is 202 Accepted both sync and async** (oracle-pinned to
///   OpenZipkin's `POST /api/v2/spans`, which answers 202 on accept), unlike
///   the OTLP 200-sync / Loki 204-sync conventions — each compat endpoint
///   matches ITS oracle.
/// - **v2 JSON only, all-or-nothing** (Zipkin has no partial-success
///   channel): the body is always decoded as a Zipkin v2 JSON span array
///   (`Content-Type` is not a fork discriminator — v2 JSON is the sole
///   supported encoding; v1/protobuf/thrift are out of scope), decompressed
///   per `Content-Encoding` for gzip; a malformed array or any span with a
///   bad id/timestamp is a whole-request `ZipkinDecode` 400 plain-text.
///   `parsed.rejected` is therefore always 0 here.
pub async fn ingest_zipkin(sink: &dyn TraceSink, headers: HeaderMap, body: Body) -> Response {
    let now_ns = now_unix_nanos();

    let body = match read_capped_body(body, decompress::MAX_DECOMPRESSED_BYTES).await {
        Ok(body) => body,
        // Reuses the signal-neutral empty-body/plain-text response family
        // (the `rw_*` helpers), like the Loki push receiver.
        Err(err) => return rw_error_response(&err),
    };

    let parsed = match decode_zipkin(&headers, &body, now_ns) {
        Ok(parsed) => parsed,
        Err(err) => return rw_error_response(&err),
    };

    if is_async(&headers) {
        return match sink.admit(parsed) {
            Ok(()) => rw_success_response(StatusCode::ACCEPTED),
            Err(Backpressure) => rw_backpressure_response(),
        };
    }

    match sink.admit_flush(parsed) {
        Ok(wait) => match wait.await {
            // 202 (not 200) on sync success too — the Zipkin oracle answers
            // 202 Accepted regardless of the async header.
            Ok(()) => rw_success_response(StatusCode::ACCEPTED),
            Err(err) => rw_error_response(&err),
        },
        Err(Backpressure) => rw_backpressure_response(),
    }
}

/// Decompresses (honoring `Content-Encoding` for gzip), decodes the Zipkin
/// v2 JSON span array, adapts it to OTLP, and runs the shared
/// `otlp_traces::parse`. See [`ingest_zipkin`] for why `Content-Type` is
/// not consulted.
fn decode_zipkin(
    headers: &HeaderMap,
    body: &[u8],
    now_ns: i64,
) -> Result<crate::ingest::traces::ParsedTraces, LogsIngestError> {
    let encoding = content_encoding(headers)?;
    let decompressed = decompress::decompress(encoding, body)?;
    let spans = zipkin::decode(&decompressed)?;
    let request = zipkin::to_otlp(spans)?;
    otlp_traces::parse(&request, now_ns)
}

/// `true` when the request's `Content-Type` selects a JSON body — the Loki
/// JSON push path (issue #77) and the OTLP/JSON encoding on `/v1/logs`,
/// `/v1/metrics`, `/v1/traces` (issue #76) share this one predicate (contains
/// `application/json`, case-insensitively — a real client sends
/// `application/json` or `application/json; charset=utf-8`). Anything else, or
/// an absent header, selects the protobuf path (the agent default;
/// `application/x-protobuf` for OTLP, snappy-protobuf for Loki).
fn is_json_content_type(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.to_ascii_lowercase().contains("application/json"))
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
    if is_json_content_type(headers) {
        otlp_logs::decode_json(&decompressed)
    } else {
        otlp_logs::decode(&decompressed)
    }
}

/// Reads `Content-Encoding` (defaulting to `identity` when absent or
/// empty), decompresses, and prost-decodes the request body — the
/// metrics analog of [`decode_request`].
fn decode_metrics_request(
    headers: &HeaderMap,
    body: &[u8],
) -> Result<ExportMetricsServiceRequest, LogsIngestError> {
    let encoding = content_encoding(headers)?;
    let decompressed = decompress::decompress(encoding, body)?;
    if is_json_content_type(headers) {
        otlp_metrics::decode_json(&decompressed)
    } else {
        otlp_metrics::decode(&decompressed)
    }
}

/// Reads `Content-Encoding` (defaulting to `identity` when absent or
/// empty), decompresses, and prost-decodes the request body — the traces
/// analog of [`decode_request`].
fn decode_traces_request(
    headers: &HeaderMap,
    body: &[u8],
) -> Result<ExportTraceServiceRequest, LogsIngestError> {
    let encoding = content_encoding(headers)?;
    let decompressed = decompress::decompress(encoding, body)?;
    if is_json_content_type(headers) {
        otlp_traces::decode_json(&decompressed)
    } else {
        otlp_traces::decode(&decompressed)
    }
}

/// Decompresses (always block-snappy — see [`ingest_remote_write`]'s doc
/// comment for why `Content-Encoding` is never consulted) and prost-decodes
/// the request body.
fn decode_remote_write_request(body: &[u8]) -> Result<remote_write::WriteRequest, LogsIngestError> {
    let decompressed = decompress::decompress(Encoding::Snappy, body)?;
    remote_write::decode(&decompressed)
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
        | LogsIngestError::OversizeMessage { .. }
        | LogsIngestError::LokiDecode(_)
        | LogsIngestError::ZipkinDecode(_)
        | LogsIngestError::Decode(_)
        | LogsIngestError::DecodeJson(_) => (StatusCode::BAD_REQUEST, 3),
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

/// Success/accepted response body for `/v1/metrics` — the metrics analog
/// of [`export_response`], carrying `partial_success.rejected_data_points`
/// (the OTLP metrics partial-success field name) instead of
/// `rejected_log_records`.
fn export_metrics_response(
    status: StatusCode,
    rejected: u64,
    rejected_message: Option<String>,
) -> Response {
    let partial_success = (rejected > 0).then(|| ExportMetricsPartialSuccess {
        rejected_data_points: i64::try_from(rejected).unwrap_or(i64::MAX),
        error_message: rejected_message.unwrap_or_default(),
    });
    let body = ExportMetricsServiceResponse { partial_success }.encode_to_vec();
    protobuf_response(status, body)
}

/// Success/accepted response body for `/v1/traces` — the traces analog of
/// [`export_response`], carrying `partial_success.rejected_spans` (the
/// OTLP traces partial-success field name).
fn export_traces_response(
    status: StatusCode,
    rejected: u64,
    rejected_message: Option<String>,
) -> Response {
    let partial_success = (rejected > 0).then(|| ExportTracePartialSuccess {
        rejected_spans: i64::try_from(rejected).unwrap_or(i64::MAX),
        error_message: rejected_message.unwrap_or_default(),
    });
    let body = ExportTraceServiceResponse { partial_success }.encode_to_vec();
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

const PLAIN_TEXT_CONTENT_TYPE: HeaderValue = HeaderValue::from_static("text/plain; charset=utf-8");

/// `/api/v1/write`'s empty-body success/accepted response (architect plan:
/// remote-write has no partial-success envelope, so `rejected` never
/// appears here — only via the writer's `rejected_total` metric/log).
fn rw_success_response(status: StatusCode) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = status;
    response
}

/// `/api/v1/write`'s whole-request error response: `err`'s [`classify`]d
/// status with a plain-text body — never [`status_response`]'s
/// `google.rpc.Status` protobuf (architect plan edge case 3).
fn rw_error_response(err: &LogsIngestError) -> Response {
    let (status, _code) = classify(err);
    plain_text_response(status, err.to_string())
}

/// `/api/v1/write`'s sink-backpressure response: `429`, plain text (the
/// remote-write-shaped counterpart of [`backpressure_response`]).
fn rw_backpressure_response() -> Response {
    plain_text_response(
        StatusCode::TOO_MANY_REQUESTS,
        "sink is applying backpressure: buffers are full".to_string(),
    )
}

fn plain_text_response(status: StatusCode, message: String) -> Response {
    let mut response = Response::new(Body::from(message));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, PLAIN_TEXT_CONTENT_TYPE);
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

    /// OTLP/JSON (proto3-JSON) encoding of [`valid_request_body`]'s exact
    /// logical payload — used by the content-negotiation tests (issue #76).
    fn valid_json_request_body() -> Vec<u8> {
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
        serde_json::to_vec(&req).expect("serialize OTLP/JSON logs body")
    }

    /// Issue #76 dispatch (a): a valid OTLP/JSON body under `Content-Type:
    /// application/json` selects the JSON decode fork and admits exactly once.
    #[tokio::test]
    async fn json_content_type_admits_a_valid_otlp_json_body() {
        let sink = MockSink::new(Outcome::Admit);
        let res = post_body(
            router(sink.clone()),
            valid_json_request_body(),
            &[("content-type", "application/json")],
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(sink.admitted.lock().unwrap().len(), 1);
    }

    /// Issue #76 dispatch (b): a protobuf body under `Content-Type:
    /// application/json` is routed to the JSON decoder (content negotiation is
    /// real, not ignored) and rejected 400/code 3 — the conformance flip.
    #[tokio::test]
    async fn json_content_type_rejects_a_protobuf_body_with_400_code_3() {
        let sink = MockSink::new(Outcome::Admit);
        let res = post_body(
            router(sink.clone()),
            valid_request_body(),
            &[("content-type", "application/json")],
        )
        .await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let status = decode_status_body(res).await;
        assert_eq!(status.code, 3);
        assert_eq!(sink.admitted.lock().unwrap().len(), 0);
    }

    /// Issue #76 dispatch (c): a JSON body with NO / non-JSON Content-Type is
    /// decoded as protobuf (protobuf stays the default) and therefore 400s —
    /// proving the fork keys strictly on `application/json`.
    #[tokio::test]
    async fn absent_content_type_decodes_json_body_as_protobuf_and_400s() {
        let sink = MockSink::new(Outcome::Admit);
        let res = post_body(router(sink.clone()), valid_json_request_body(), &[]).await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let status = decode_status_body(res).await;
        assert_eq!(status.code, 3);
        assert_eq!(sink.admitted.lock().unwrap().len(), 0);
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

    // -- `/v1/metrics` (issue #27) ---------------------------------------
    // Mirrors the `/v1/logs` test suite above exactly — same shared
    // helpers (`read_capped_body`, `content_encoding`, `classify`,
    // `error_response`, `backpressure_response`, `protobuf_response`),
    // only the sink/request/response types differ.

    use crate::ingest::metrics::ParsedMetrics;
    use opentelemetry_proto::tonic::metrics::v1::{
        Gauge, Metric, ResourceMetrics, ScopeMetrics, metric, number_data_point,
    };

    struct MockMetricSink {
        outcome: Outcome,
        admitted: Mutex<Vec<ParsedMetrics>>,
    }

    impl MockMetricSink {
        fn new(outcome: Outcome) -> Arc<MockMetricSink> {
            Arc::new(MockMetricSink {
                outcome,
                admitted: Mutex::new(Vec::new()),
            })
        }
    }

    impl MetricSink for MockMetricSink {
        fn admit(&self, batch: ParsedMetrics) -> Result<(), Backpressure> {
            self.admitted.lock().unwrap().push(batch);
            match self.outcome {
                Outcome::Admit | Outcome::FlushFails => Ok(()),
                Outcome::Backpressure => Err(Backpressure),
            }
        }

        fn admit_flush(&self, batch: ParsedMetrics) -> Result<FlushWait, Backpressure> {
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

    fn metrics_router(sink: Arc<MockMetricSink>) -> Router {
        Router::new()
            .route("/v1/metrics", post(metrics::<MockMetricSink>))
            .with_state(sink)
    }

    /// [`post_body`]'s `/v1/metrics` counterpart — that helper hardcodes
    /// `/v1/logs`, so this suite posts to its own path rather than
    /// generalizing a shared helper only two call sites would use
    /// differently.
    async fn post_metrics_body(
        router: Router,
        body: Vec<u8>,
        headers: &[(&str, &str)],
    ) -> Response {
        let mut builder = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/metrics");
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        let request = builder.body(Body::from(body)).unwrap();
        router.oneshot(request).await.unwrap()
    }

    fn valid_metrics_request_body() -> Vec<u8> {
        let req = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "up".to_string(),
                        description: String::new(),
                        unit: String::new(),
                        metadata: vec![],
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![
                                opentelemetry_proto::tonic::metrics::v1::NumberDataPoint {
                                    attributes: vec![],
                                    start_time_unix_nano: 0,
                                    time_unix_nano: 1_700_000_000_000_000_000,
                                    exemplars: vec![],
                                    flags: 0,
                                    value: Some(number_data_point::Value::AsDouble(1.0)),
                                },
                            ],
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        req.encode_to_vec()
    }

    #[tokio::test]
    async fn metrics_sync_mode_admits_via_admit_flush_and_returns_200() {
        let sink = MockMetricSink::new(Outcome::Admit);
        let res = post_metrics_body(
            metrics_router(sink.clone()),
            valid_metrics_request_body(),
            &[],
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(sink.admitted.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn metrics_async_mode_admits_via_admit_and_returns_202() {
        let sink = MockMetricSink::new(Outcome::Admit);
        let res = post_metrics_body(
            metrics_router(sink.clone()),
            valid_metrics_request_body(),
            &[("x-pulsus-async", "1")],
        )
        .await;
        assert_eq!(res.status(), StatusCode::ACCEPTED);
        assert_eq!(sink.admitted.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn metrics_malformed_body_returns_400_with_status_code_3() {
        let sink = MockMetricSink::new(Outcome::Admit);
        let res = post_metrics_body(
            metrics_router(sink),
            b"not a protobuf message".to_vec(),
            &[],
        )
        .await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let status = decode_status_body(res).await;
        assert_eq!(status.code, 3);
    }

    #[tokio::test]
    async fn metrics_sink_backpressure_returns_429_with_status_code_8() {
        let sink = MockMetricSink::new(Outcome::Backpressure);
        let res = post_metrics_body(metrics_router(sink), valid_metrics_request_body(), &[]).await;
        assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);
        let status = decode_status_body(res).await;
        assert_eq!(status.code, 8);
    }

    #[tokio::test]
    async fn metrics_flush_failure_returns_500_with_status_code_13() {
        let sink = MockMetricSink::new(Outcome::FlushFails);
        let res = post_metrics_body(metrics_router(sink), valid_metrics_request_body(), &[]).await;
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let status = decode_status_body(res).await;
        assert_eq!(status.code, 13);
    }

    /// Issue #62, at the route level: a body well inside the 64 MiB cap
    /// whose base × data-point fan-out exceeds
    /// `otlp_metrics::MAX_EXPANDED_BYTES` is a whole-request 400/code 3 (the
    /// structural-oversize class), and the sink is never admitted to.
    #[tokio::test]
    async fn metrics_expansion_budget_overflow_returns_400_with_status_code_3() {
        use opentelemetry_proto::tonic::common::v1::any_value::Value;
        use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
        use opentelemetry_proto::tonic::metrics::v1::NumberDataPoint;
        use opentelemetry_proto::tonic::resource::v1::Resource;

        // One ~1 MiB resource attribute is cloned into every data point's
        // LabelSet, so the per-sample charge (~1 MiB) trips the 256 MiB
        // budget within a few hundred of the many data points. Derived from
        // the constant so a budget retune cannot silently weaken this.
        let big_value = "v".repeat(1024 * 1024);
        let resource = Resource {
            attributes: vec![KeyValue {
                key: "big.attr".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue(big_value)),
                }),
                key_strindex: 0,
            }],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        };
        let dp_count = otlp_metrics::MAX_EXPANDED_BYTES / (1024 * 1024) + 2;
        let data_points: Vec<NumberDataPoint> = (0..dp_count)
            .map(|i| NumberDataPoint {
                attributes: vec![],
                start_time_unix_nano: 0,
                time_unix_nano: 1_700_000_000_000_000_000,
                exemplars: vec![],
                flags: 0,
                value: Some(number_data_point::Value::AsDouble(i as f64)),
            })
            .collect();
        let req = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(resource),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "cpu".to_string(),
                        description: String::new(),
                        unit: String::new(),
                        metadata: vec![],
                        data: Some(metric::Data::Gauge(Gauge { data_points })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let sink = MockMetricSink::new(Outcome::Admit);
        let res = post_metrics_body(metrics_router(sink.clone()), req.encode_to_vec(), &[]).await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let status = decode_status_body(res).await;
        assert_eq!(status.code, 3);
        assert!(
            sink.admitted.lock().unwrap().is_empty(),
            "an over-budget request must never reach the sink (no partial write)"
        );
    }

    #[tokio::test]
    async fn metrics_success_response_reports_partial_success_when_points_were_rejected() {
        let req = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "up".to_string(),
                        description: String::new(),
                        unit: String::new(),
                        metadata: vec![],
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![
                                // Zero timestamp: rejected (partial success).
                                opentelemetry_proto::tonic::metrics::v1::NumberDataPoint {
                                    attributes: vec![],
                                    start_time_unix_nano: 0,
                                    time_unix_nano: 0,
                                    exemplars: vec![],
                                    flags: 0,
                                    value: Some(number_data_point::Value::AsDouble(1.0)),
                                },
                                opentelemetry_proto::tonic::metrics::v1::NumberDataPoint {
                                    attributes: vec![],
                                    start_time_unix_nano: 0,
                                    time_unix_nano: 1_700_000_000_000_000_000,
                                    exemplars: vec![],
                                    flags: 0,
                                    value: Some(number_data_point::Value::AsDouble(2.0)),
                                },
                            ],
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let sink = MockMetricSink::new(Outcome::Admit);
        let res = post_metrics_body(metrics_router(sink), req.encode_to_vec(), &[]).await;
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let response = ExportMetricsServiceResponse::decode(bytes.as_ref()).unwrap();
        let partial = response.partial_success.expect("partial success is set");
        assert_eq!(partial.rejected_data_points, 1);
        assert!(!partial.error_message.is_empty());
    }

    // -- OTLP/JSON decode surfacing (issue #103, docs/decisions/0004 P6) -----
    // The vendored serde flatten oneofs used to SWALLOW a malformed inner field
    // to `None`, so a bad OTLP/JSON metric returned 202 with the metric silently
    // dropped instead of the 400 it deserves. These end-to-end tests prove the
    // decode error now reaches the handler (400/code 3), that spec-valid sparse
    // points and canonical string-encoded `asInt` still ingest (202), and that
    // decode never regresses to a silent drop.

    const JSON_CT: (&str, &str) = ("content-type", "application/json");

    #[tokio::test]
    async fn metrics_json_malformed_exphist_value_returns_400_with_status_code_3() {
        // A bad `count` deep inside the exphist data subtree used to collapse
        // `Metric.data` to `None` (Ok -> 202, metric dropped). It must now be a
        // named 400 (mirrors `metrics_malformed_body_returns_400_with_status_code_3`).
        let sink = MockMetricSink::new(Outcome::Admit);
        let body = br#"{"resourceMetrics":[{"scopeMetrics":[{"metrics":[
            {"name":"m","exponentialHistogram":{"dataPoints":[{"count":"nope"}]}}]}]}]}"#
            .to_vec();
        let res = post_metrics_body(metrics_router(sink.clone()), body, &[JSON_CT]).await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let status = decode_status_body(res).await;
        assert_eq!(status.code, 3);
        // Nothing admitted — the malformed body never reached the sink.
        assert!(sink.admitted.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn metrics_json_multiple_data_oneof_members_returns_400() {
        let sink = MockMetricSink::new(Outcome::Admit);
        let body = br#"{"resourceMetrics":[{"scopeMetrics":[{"metrics":[
            {"name":"m","gauge":{"dataPoints":[]},"sum":{"dataPoints":[]}}]}]}]}"#
            .to_vec();
        let res = post_metrics_body(metrics_router(sink), body, &[JSON_CT]).await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let status = decode_status_body(res).await;
        assert_eq!(status.code, 3);
    }

    #[tokio::test]
    async fn metrics_json_sparse_exphist_point_is_accepted() {
        // proto3-JSON omits default-valued fields; P6's `serde(default)` lets a
        // sparse-but-valid exphist point decode (was a spurious 400 once decode
        // errors became visible). Async mode => 202.
        let sink = MockMetricSink::new(Outcome::Admit);
        let body = br#"{"resourceMetrics":[{"scopeMetrics":[{"metrics":[
            {"name":"eh","exponentialHistogram":{"aggregationTemporality":2,
             "dataPoints":[{"timeUnixNano":"1700000000000000000","count":"0"}]}}]}]}]}"#
            .to_vec();
        let res = post_metrics_body(
            metrics_router(sink.clone()),
            body,
            &[JSON_CT, ("x-pulsus-async", "1")],
        )
        .await;
        assert_eq!(res.status(), StatusCode::ACCEPTED);
        assert_eq!(sink.admitted.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn metrics_json_string_encoded_int_counter_ingests() {
        // Canonical OTLP/JSON string-encodes `asInt`; because decode is
        // whole-request atomic, a number-only decoder would 400 the whole batch.
        // The batch must be accepted (202) and the counter admitted as a sample.
        let sink = MockMetricSink::new(Outcome::Admit);
        let body = br#"{"resourceMetrics":[{"scopeMetrics":[{"metrics":[
            {"name":"requests_total","sum":{"aggregationTemporality":2,"isMonotonic":true,
             "dataPoints":[{"timeUnixNano":"1700000000000000000","asInt":"42"}]}}]}]}]}"#
            .to_vec();
        let res = post_metrics_body(
            metrics_router(sink.clone()),
            body,
            &[JSON_CT, ("x-pulsus-async", "1")],
        )
        .await;
        assert_eq!(res.status(), StatusCode::ACCEPTED);
        let admitted = sink.admitted.lock().unwrap();
        assert_eq!(admitted.len(), 1);
        // The string-encoded counter is queryable: it decoded to a sample with
        // the value 42, not silently dropped.
        let batch = &admitted[0];
        assert_eq!(batch.rejected, 0, "no data point should be rejected");
        assert_eq!(batch.samples.len(), 1, "the asInt counter must ingest");
        assert_eq!(batch.samples[0].value, 42.0);
    }

    // -- `/v1/traces` (issue #54) ------------------------------------------
    // Mirrors the `/v1/logs`/`/v1/metrics` suites above exactly — same
    // shared helpers, only the sink/request/response types differ.

    use crate::ingest::traces::{ParsedTraces, TraceSink};
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

    struct MockTraceSink {
        outcome: Outcome,
        admitted: Mutex<Vec<ParsedTraces>>,
    }

    impl MockTraceSink {
        fn new(outcome: Outcome) -> Arc<MockTraceSink> {
            Arc::new(MockTraceSink {
                outcome,
                admitted: Mutex::new(Vec::new()),
            })
        }
    }

    impl TraceSink for MockTraceSink {
        fn admit(&self, batch: ParsedTraces) -> Result<(), Backpressure> {
            self.admitted.lock().unwrap().push(batch);
            match self.outcome {
                Outcome::Admit | Outcome::FlushFails => Ok(()),
                Outcome::Backpressure => Err(Backpressure),
            }
        }

        fn admit_flush(&self, batch: ParsedTraces) -> Result<FlushWait, Backpressure> {
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

    fn traces_router(sink: Arc<MockTraceSink>) -> Router {
        Router::new()
            .route("/v1/traces", post(traces::<MockTraceSink>))
            .with_state(sink)
    }

    /// [`post_body`]'s `/v1/traces` counterpart (same rationale as
    /// [`post_metrics_body`]'s).
    async fn post_traces_body(router: Router, body: Vec<u8>, headers: &[(&str, &str)]) -> Response {
        let mut builder = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/traces");
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        let request = builder.body(Body::from(body)).unwrap();
        router.oneshot(request).await.unwrap()
    }

    fn trace_span(trace_id: Vec<u8>, span_id: Vec<u8>) -> Span {
        Span {
            trace_id,
            span_id,
            name: "op-a".to_string(),
            start_time_unix_nano: 1_700_000_000_000_000_000,
            end_time_unix_nano: 1_700_000_001_000_000_000,
            ..Default::default()
        }
    }

    fn traces_request(spans: Vec<Span>) -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: None,
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    fn valid_traces_request_body() -> Vec<u8> {
        traces_request(vec![trace_span(vec![1; 16], vec![2; 8])]).encode_to_vec()
    }

    #[tokio::test]
    async fn traces_sync_mode_admits_via_admit_flush_and_returns_200() {
        let sink = MockTraceSink::new(Outcome::Admit);
        let res = post_traces_body(
            traces_router(sink.clone()),
            valid_traces_request_body(),
            &[],
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(sink.admitted.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn traces_async_header_value_zero_stays_sync() {
        let sink = MockTraceSink::new(Outcome::Admit);
        let res = post_traces_body(
            traces_router(sink.clone()),
            valid_traces_request_body(),
            &[("x-pulsus-async", "0")],
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(sink.admitted.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn traces_async_mode_admits_via_admit_and_returns_202() {
        let sink = MockTraceSink::new(Outcome::Admit);
        let res = post_traces_body(
            traces_router(sink.clone()),
            valid_traces_request_body(),
            &[("x-pulsus-async", "1")],
        )
        .await;
        assert_eq!(res.status(), StatusCode::ACCEPTED);
        assert_eq!(sink.admitted.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn traces_malformed_body_returns_400_with_status_code_3() {
        let sink = MockTraceSink::new(Outcome::Admit);
        let res =
            post_traces_body(traces_router(sink), b"not a protobuf message".to_vec(), &[]).await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let status = decode_status_body(res).await;
        assert_eq!(status.code, 3);
    }

    #[tokio::test]
    async fn traces_unsupported_content_encoding_returns_400_with_status_code_3() {
        let sink = MockTraceSink::new(Outcome::Admit);
        let res = post_traces_body(
            traces_router(sink),
            valid_traces_request_body(),
            &[("content-encoding", "br")],
        )
        .await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let status = decode_status_body(res).await;
        assert_eq!(status.code, 3);
    }

    /// Issue #54 code-review [high] fix, at the route level: a body well
    /// inside the 64 MiB cap whose resource × span fan-out exceeds
    /// `otlp_traces::MAX_EXPANDED_BYTES` is a whole-request 400/code 3
    /// (the structural-oversize class), and the sink is never admitted to.
    #[tokio::test]
    async fn traces_expansion_budget_overflow_returns_400_with_status_code_3() {
        use opentelemetry_proto::tonic::common::v1::any_value::Value;
        use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
        use opentelemetry_proto::tonic::resource::v1::Resource;

        let big_value = "v".repeat(1024 * 1024); // 1 MiB resource attr value
        let resource = Resource {
            attributes: vec![KeyValue {
                key: "big.attr".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue(big_value)),
                }),
                key_strindex: 0,
            }],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        };
        let span_count = crate::protocols::otlp_traces::MAX_EXPANDED_BYTES / (1024 * 1024) + 2;
        let req = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(resource),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: (0..span_count)
                        .map(|_| trace_span(vec![1; 16], vec![2; 8]))
                        .collect(),
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let sink = MockTraceSink::new(Outcome::Admit);
        let res = post_traces_body(traces_router(sink.clone()), req.encode_to_vec(), &[]).await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let status = decode_status_body(res).await;
        assert_eq!(status.code, 3);
        assert!(
            sink.admitted.lock().unwrap().is_empty(),
            "an over-budget request must never reach the sink (no partial write)"
        );
    }

    #[tokio::test]
    async fn traces_body_over_the_64_mib_cap_returns_400_with_status_code_3() {
        let body = vec![0u8; decompress::MAX_DECOMPRESSED_BYTES + 1024];
        let sink = MockTraceSink::new(Outcome::Admit);
        let res = post_traces_body(traces_router(sink), body, &[]).await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let status = decode_status_body(res).await;
        assert_eq!(status.code, 3);
    }

    #[tokio::test]
    async fn traces_sink_backpressure_returns_429_with_status_code_8() {
        let sink = MockTraceSink::new(Outcome::Backpressure);
        let res = post_traces_body(traces_router(sink), valid_traces_request_body(), &[]).await;
        assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);
        let status = decode_status_body(res).await;
        assert_eq!(status.code, 8);
    }

    #[tokio::test]
    async fn traces_async_mode_sink_backpressure_also_returns_429_with_status_code_8() {
        let sink = MockTraceSink::new(Outcome::Backpressure);
        let res = post_traces_body(
            traces_router(sink),
            valid_traces_request_body(),
            &[("x-pulsus-async", "1")],
        )
        .await;
        assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);
        let status = decode_status_body(res).await;
        assert_eq!(status.code, 8);
    }

    #[tokio::test]
    async fn traces_flush_failure_returns_500_with_status_code_13() {
        let sink = MockTraceSink::new(Outcome::FlushFails);
        let res = post_traces_body(traces_router(sink), valid_traces_request_body(), &[]).await;
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let status = decode_status_body(res).await;
        assert_eq!(status.code, 13);
    }

    #[tokio::test]
    async fn traces_success_response_reports_partial_success_when_spans_were_rejected() {
        // One bad span (15-byte trace_id) alongside one valid span.
        let req = traces_request(vec![
            trace_span(vec![1; 15], vec![2; 8]),
            trace_span(vec![1; 16], vec![2; 8]),
        ]);
        let sink = MockTraceSink::new(Outcome::Admit);
        let res = post_traces_body(traces_router(sink), req.encode_to_vec(), &[]).await;
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let response = ExportTraceServiceResponse::decode(bytes.as_ref()).unwrap();
        let partial = response.partial_success.expect("partial success is set");
        assert_eq!(partial.rejected_spans, 1);
        assert!(!partial.error_message.is_empty());
    }

    // -- `/api/v1/write` (issue #28) --------------------------------------
    // No generic-`State` router mount point exists for `ingest_remote_write`
    // (see this module's doc comment) — these tests call the
    // `&dyn MetricSink` core directly with hand-built `HeaderMap`/`Body`
    // values rather than routing through an `axum::Router`, since no path-
    // dispatch logic lives at this layer (that is `pulsus-server`'s
    // `subsystems.rs`, covered by its own route-presence test). Reuses
    // [`MockMetricSink`] — the same sink trait, no new mock needed.

    use crate::protocols::remote_write::{Label, Sample, TimeSeries, WriteRequest};

    fn snappy_compress(data: &[u8]) -> Vec<u8> {
        snap::raw::Encoder::new().compress_vec(data).unwrap()
    }

    /// Encodes+compresses a minimal, well-formed `WriteRequest` — the
    /// `/api/v1/write` analog of [`valid_metrics_request_body`].
    fn valid_remote_write_body() -> Vec<u8> {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![Label {
                    name: "__name__".to_string(),
                    value: "up".to_string(),
                }],
                samples: vec![Sample {
                    value: 1.0,
                    timestamp: 1_700_000_000_000,
                }],
            }],
            metadata: vec![],
        };
        snappy_compress(&req.encode_to_vec())
    }

    async fn call_remote_write(
        sink: &MockMetricSink,
        body: Vec<u8>,
        headers: &[(&str, &str)],
    ) -> Response {
        let mut header_map = HeaderMap::new();
        for (name, value) in headers {
            header_map.insert(
                axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }
        ingest_remote_write(sink, header_map, Body::from(body)).await
    }

    async fn plain_text_body(res: Response) -> String {
        assert_eq!(
            res.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/plain; charset=utf-8"
        );
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn remote_write_sync_mode_admits_via_admit_flush_and_returns_204_with_empty_body() {
        let sink = MockMetricSink::new(Outcome::Admit);
        let res = call_remote_write(&sink, valid_remote_write_body(), &[]).await;
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        assert_eq!(sink.admitted.lock().unwrap().len(), 1);
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(bytes.is_empty(), "204 success must carry no body");
    }

    #[tokio::test]
    async fn remote_write_async_mode_admits_via_admit_and_returns_202() {
        let sink = MockMetricSink::new(Outcome::Admit);
        let res =
            call_remote_write(&sink, valid_remote_write_body(), &[("x-pulsus-async", "1")]).await;
        assert_eq!(res.status(), StatusCode::ACCEPTED);
        assert_eq!(sink.admitted.lock().unwrap().len(), 1);
    }

    /// Architect plan: decompression is unconditional block-snappy,
    /// `Content-Encoding` is never consulted — a bogus/absent header must
    /// admit identically to no header at all.
    #[tokio::test]
    async fn remote_write_content_encoding_header_is_ignored_snappy_is_unconditional() {
        let sink_with_bogus_header = MockMetricSink::new(Outcome::Admit);
        let with_header = call_remote_write(
            &sink_with_bogus_header,
            valid_remote_write_body(),
            &[("content-encoding", "identity")],
        )
        .await;
        assert_eq!(with_header.status(), StatusCode::NO_CONTENT);

        let sink_without_header = MockMetricSink::new(Outcome::Admit);
        let without_header =
            call_remote_write(&sink_without_header, valid_remote_write_body(), &[]).await;
        assert_eq!(without_header.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn remote_write_malformed_snappy_returns_400_plain_text() {
        let sink = MockMetricSink::new(Outcome::Admit);
        let res = call_remote_write(&sink, b"\xFF\xFF\xFF not snappy".to_vec(), &[]).await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let body = plain_text_body(res).await;
        assert!(!body.is_empty());
    }

    #[tokio::test]
    async fn remote_write_malformed_protobuf_after_valid_snappy_returns_400_plain_text() {
        let sink = MockMetricSink::new(Outcome::Admit);
        let body = snappy_compress(b"not a valid WriteRequest protobuf \xFF\xFF");
        let res = call_remote_write(&sink, body, &[]).await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn remote_write_sink_backpressure_returns_429_plain_text() {
        let sink = MockMetricSink::new(Outcome::Backpressure);
        let res = call_remote_write(&sink, valid_remote_write_body(), &[]).await;
        assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);
        let body = plain_text_body(res).await;
        assert!(body.contains("backpressure"));
    }

    #[tokio::test]
    async fn remote_write_async_mode_sink_backpressure_also_returns_429_plain_text() {
        let sink = MockMetricSink::new(Outcome::Backpressure);
        let res =
            call_remote_write(&sink, valid_remote_write_body(), &[("x-pulsus-async", "1")]).await;
        assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn remote_write_flush_failure_returns_500_plain_text() {
        let sink = MockMetricSink::new(Outcome::FlushFails);
        let res = call_remote_write(&sink, valid_remote_write_body(), &[]).await;
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = plain_text_body(res).await;
        assert!(!body.is_empty());
    }

    /// Issue #62 (Δ1): a snappy-prompb body well inside the 64 MiB cap
    /// carrying more than the admissible ~4.2M-sample ceiling — ~4.3M
    /// minimal 2-byte samples (`value`/`timestamp` both proto3 defaults,
    /// ≈ 8 MiB on the wire) spread across the fewest series the per-series
    /// cap allows — trips `remote_write::MAX_EXPANDED_BYTES` and is a
    /// whole-request plain-text 400; the sink is never admitted to (proving
    /// the abort fires before the ~33.5M-sample-class output materializes).
    /// Sample count derives from the constants so a retune cannot silently
    /// weaken it.
    #[tokio::test]
    async fn remote_write_expansion_budget_overflow_returns_400() {
        use crate::protocols::remote_write::{MAX_EXPANDED_BYTES, MAX_SAMPLES_PER_SERIES};

        // `MAX_EXPANDED_BYTES / 64` is the sample ceiling (SAMPLE_ROW_OVERHEAD
        // = 64); one extra full series guarantees the budget trips.
        let total = MAX_EXPANDED_BYTES / 64 + MAX_SAMPLES_PER_SERIES;
        let series_count = total.div_ceil(MAX_SAMPLES_PER_SERIES);
        let timeseries: Vec<TimeSeries> = (0..series_count)
            .map(|s| TimeSeries {
                labels: vec![Label {
                    name: "__name__".to_string(),
                    value: format!("m{s}"),
                }],
                samples: vec![
                    Sample {
                        value: 0.0,
                        timestamp: 0,
                    };
                    MAX_SAMPLES_PER_SERIES
                ],
            })
            .collect();
        let req = WriteRequest {
            timeseries,
            metadata: vec![],
        };
        let body = snappy_compress(&req.encode_to_vec());

        let sink = MockMetricSink::new(Outcome::Admit);
        let res = call_remote_write(&sink, body, &[]).await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let text = plain_text_body(res).await;
        assert!(!text.is_empty());
        assert!(
            sink.admitted.lock().unwrap().is_empty(),
            "an over-budget request must never reach the sink (no partial write)"
        );
    }

    /// Architect plan reject boundary: a series missing `__name__` is
    /// dropped (semantic, per-series), never a whole-request `400` — the
    /// request still succeeds with `204`, only the writer's
    /// `rejected_total` metric/log sees it (no partial-success envelope in
    /// the response).
    #[tokio::test]
    async fn remote_write_missing_name_label_still_returns_204_body_still_admitted() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![Label {
                    name: "job".to_string(),
                    value: "checkout".to_string(),
                }],
                samples: vec![Sample {
                    value: 1.0,
                    timestamp: 1,
                }],
            }],
            metadata: vec![],
        };
        let body = snappy_compress(&req.encode_to_vec());
        let sink = MockMetricSink::new(Outcome::Admit);
        let res = call_remote_write(&sink, body, &[]).await;
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        let admitted = sink.admitted.lock().unwrap();
        assert_eq!(admitted.len(), 1);
        assert_eq!(admitted[0].rejected, 1);
        assert!(admitted[0].samples.is_empty());
    }

    // -- `/loki/api/v1/push` (issue #77) ----------------------------------
    // The oracle-pinned response matrix (issue #77 v2 delta 4, probed against
    // grafana/loki 3.4.2): success 204 both encodings, 202 async
    // (PulsusDB), malformed/oversize 400 plain-text, backpressure 429
    // (PulsusDB). Reuses `MockSink` (the `LogSink` double) and the
    // remote-write `snappy_compress`/`plain_text_body` helpers.

    use crate::protocols::loki_push::{EntryAdapter, PushRequest, StreamAdapter, Timestamp};

    fn valid_loki_protobuf_body() -> Vec<u8> {
        let req = PushRequest {
            streams: vec![StreamAdapter {
                labels: r#"{service_name="checkout", env="prod"}"#.to_string(),
                entries: vec![EntryAdapter {
                    timestamp: Some(Timestamp {
                        seconds: 1_700_000_000,
                        nanos: 0,
                    }),
                    line: "hello".to_string(),
                    structured_metadata: Vec::new(),
                }],
            }],
        };
        snappy_compress(&req.encode_to_vec())
    }

    fn valid_loki_json_body() -> Vec<u8> {
        br#"{"streams":[{"stream":{"service_name":"checkout","env":"prod"},
            "values":[["1700000000000000000","hello"]]}]}"#
            .to_vec()
    }

    async fn call_loki(sink: &MockSink, body: Vec<u8>, headers: &[(&str, &str)]) -> Response {
        let mut header_map = HeaderMap::new();
        for (name, value) in headers {
            header_map.insert(
                axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }
        ingest_loki_push(sink, header_map, Body::from(body)).await
    }

    #[tokio::test]
    async fn loki_protobuf_default_sync_returns_204_empty() {
        let sink = MockSink::new(Outcome::Admit);
        let res = call_loki(
            &sink,
            valid_loki_protobuf_body(),
            &[("content-type", "application/x-protobuf")],
        )
        .await;
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        assert!(res.headers().get(header::CONTENT_TYPE).is_none());
        assert_eq!(sink.admitted.lock().unwrap().len(), 1);
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(bytes.is_empty(), "204 success must carry no body");
    }

    #[tokio::test]
    async fn loki_protobuf_absent_content_type_defaults_to_protobuf_204() {
        // A Loki agent sends a snappy protobuf body with no Content-Type;
        // negotiation defaults non-JSON to the protobuf path.
        let sink = MockSink::new(Outcome::Admit);
        let res = call_loki(&sink, valid_loki_protobuf_body(), &[]).await;
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        assert_eq!(sink.admitted.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn loki_json_sync_returns_204() {
        let sink = MockSink::new(Outcome::Admit);
        let res = call_loki(
            &sink,
            valid_loki_json_body(),
            &[("content-type", "application/json")],
        )
        .await;
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        let admitted = sink.admitted.lock().unwrap();
        assert_eq!(admitted.len(), 1);
        assert_eq!(admitted[0].rows.len(), 1);
        assert_eq!(admitted[0].rows[0].service, "checkout");
    }

    #[tokio::test]
    async fn loki_async_mode_returns_202_empty() {
        let sink = MockSink::new(Outcome::Admit);
        let res = call_loki(
            &sink,
            valid_loki_protobuf_body(),
            &[
                ("content-type", "application/x-protobuf"),
                ("x-pulsus-async", "1"),
            ],
        )
        .await;
        assert_eq!(res.status(), StatusCode::ACCEPTED);
        assert_eq!(sink.admitted.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn loki_json_body_sent_as_protobuf_is_400_plain_text() {
        // Oracle (loki 3.4.2): "snappy: corrupt input" -> 400. A raw JSON
        // body under `application/x-protobuf` fails the mandatory snappy
        // decompress.
        let sink = MockSink::new(Outcome::Admit);
        let res = call_loki(
            &sink,
            valid_loki_json_body(),
            &[("content-type", "application/x-protobuf")],
        )
        .await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        assert!(!plain_text_body(res).await.is_empty());
        assert!(sink.admitted.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn loki_unsupported_content_type_defaults_to_protobuf_and_400s() {
        // Oracle (loki 3.4.2): an unrecognized Content-Type (`text/plain`)
        // carrying a non-protobuf body defaults to the protobuf path and
        // fails snappy decode -> 400 plain-text.
        let sink = MockSink::new(Outcome::Admit);
        let res = call_loki(
            &sink,
            valid_loki_json_body(),
            &[("content-type", "text/plain")],
        )
        .await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        assert!(!plain_text_body(res).await.is_empty());
    }

    #[tokio::test]
    async fn loki_malformed_json_is_400_plain_text() {
        let sink = MockSink::new(Outcome::Admit);
        let res = call_loki(
            &sink,
            b"{not json".to_vec(),
            &[("content-type", "application/json")],
        )
        .await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        assert!(!plain_text_body(res).await.is_empty());
    }

    #[tokio::test]
    async fn loki_bad_label_string_is_400_plain_text() {
        let req = PushRequest {
            streams: vec![StreamAdapter {
                labels: "not a label set".to_string(),
                entries: vec![EntryAdapter {
                    timestamp: Some(Timestamp {
                        seconds: 1,
                        nanos: 0,
                    }),
                    line: "x".to_string(),
                    structured_metadata: Vec::new(),
                }],
            }],
        };
        let sink = MockSink::new(Outcome::Admit);
        let res = call_loki(
            &sink,
            snappy_compress(&req.encode_to_vec()),
            &[("content-type", "application/x-protobuf")],
        )
        .await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        assert!(!plain_text_body(res).await.is_empty());
        assert!(sink.admitted.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn loki_structured_metadata_is_accepted_and_dropped_204() {
        // A JSON `values` entry with a trailing structured-metadata object:
        // accepted (never an error), the metadata dropped.
        let body = br#"{"streams":[{"stream":{"a":"b"},
            "values":[["1700000000000000000","hello",{"trace_id":"abc"}]]}]}"#
            .to_vec();
        let sink = MockSink::new(Outcome::Admit);
        let res = call_loki(&sink, body, &[("content-type", "application/json")]).await;
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        let admitted = sink.admitted.lock().unwrap();
        assert_eq!(admitted[0].rows.len(), 1);
        assert_eq!(admitted[0].rows[0].body, "hello");
    }

    #[tokio::test]
    async fn loki_sink_backpressure_returns_429_plain_text() {
        let sink = MockSink::new(Outcome::Backpressure);
        let res = call_loki(
            &sink,
            valid_loki_protobuf_body(),
            &[("content-type", "application/x-protobuf")],
        )
        .await;
        assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(!plain_text_body(res).await.is_empty());
    }

    #[tokio::test]
    async fn loki_flush_failure_returns_500_plain_text() {
        let sink = MockSink::new(Outcome::FlushFails);
        let res = call_loki(
            &sink,
            valid_loki_protobuf_body(),
            &[("content-type", "application/x-protobuf")],
        )
        .await;
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(!plain_text_body(res).await.is_empty());
    }

    /// Gzip-encoded JSON is honored on the JSON path (`Content-Encoding`
    /// respected there, unlike the always-snappy protobuf path).
    #[tokio::test]
    async fn loki_gzip_encoded_json_is_decoded_and_admitted() {
        use std::io::Write;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(&valid_loki_json_body()).unwrap();
        let gz = encoder.finish().unwrap();
        let sink = MockSink::new(Outcome::Admit);
        let res = call_loki(
            &sink,
            gz,
            &[
                ("content-type", "application/json"),
                ("content-encoding", "gzip"),
            ],
        )
        .await;
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        assert_eq!(sink.admitted.lock().unwrap().len(), 1);
    }
}
