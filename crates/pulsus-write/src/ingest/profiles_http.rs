//! The profile push receivers: `POST /push.v1.PusherService/Push`, a Connect
//! unary call carrying a `PushRequest`, and `POST /ingest`, the reference's
//! legacy route, which serves every format but JFR. Each reads the
//! tenant first, reads the body under [`MAX_PROFILE_BODY_BYTES`], decodes and
//! validates every profile, and only then admits the push whole, the shape of
//! the trace JSON receiver in `ingest/http.rs`. Nothing is read from
//! ClickHouse; the sink's one landing insert is the push's only statement.

use std::io::Read;

use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::Response;
use http_body_util::BodyExt;
use prost::Message;
use pulsus_profiles_wire::connect::{Code, error_json};
use pulsus_profiles_wire::push_v1::PushRequest;

use crate::error::LogsIngestError;
use crate::ingest::http::{is_async, push_headers, read_capped_body};
use crate::ingest::profiles::{ParsedProfileLanding, ProfileSink};
use crate::ingest::{
    AdmitRefusal, KEY_REUSED_MESSAGE, push_too_large_message, push_too_many_days_message,
};
use crate::protocols::profile_model::{MAX_PROFILE_SIZE_BYTES, decode_pprof_sized, parse_profiles};
use crate::protocols::profile_push::{
    LegacyQuery, Multipart, legacy_pprof_input, parse_legacy_query, parse_multipart, push_inputs,
};
use crate::protocols::profileql_legacy::folded::{parse_groups, parse_lines};
use crate::protocols::profileql_legacy::speedscope::parse_speedscope;
use crate::protocols::profileql_legacy::tree::parse_tree;
use crate::protocols::profileql_legacy::trie::parse_trie;
use crate::protocols::profileql_legacy::{ADAPTER, LegacyError, put_input};

/// The bound on a request body as read, and on the push route's body after
/// gzip: the reference's `distributor.ingestion-body-limit-mb` default
/// (`pkg/validation/limits.go:24,164`).
pub const MAX_PROFILE_BODY_BYTES: usize = 268_435_456;

/// The text every backpressure refusal carries, on both routes.
const BACKPRESSURE_MESSAGE: &str = "sink is applying backpressure: buffers are full";

/// The push route's two media types.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Media {
    Proto,
    Json,
}

impl Media {
    fn as_str(self) -> &'static str {
        match self {
            Media::Proto => "application/proto",
            Media::Json => "application/json",
        }
    }
}

fn now_ms() -> i64 {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
}

fn json_response(status: StatusCode, body: String, nosniff: bool) -> Response {
    let mut res = Response::new(Body::from(body));
    *res.status_mut() = status;
    let headers = res.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    if nosniff {
        headers.insert(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        );
    }
    res
}

// ---------------------------------------------------------------------------
// The push route
// ---------------------------------------------------------------------------

/// A Connect unary error (§3.2): the code's status and its JSON body.
fn connect_error(code: Code, message: impl AsRef<str>) -> Response {
    let status =
        StatusCode::from_u16(code.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    json_response(status, error_json(code, message.as_ref()), false)
}

fn connect_refusal(refusal: AdmitRefusal) -> Response {
    match refusal {
        AdmitRefusal::Backpressure | AdmitRefusal::DedupShed | AdmitRefusal::DedupWaitShed => {
            connect_error(Code::ResourceExhausted, BACKPRESSURE_MESSAGE)
        }
        AdmitRefusal::KeyReused => connect_error(Code::InvalidArgument, KEY_REUSED_MESSAGE),
        AdmitRefusal::PushTooLarge {
            rows,
            row_limit,
            bytes,
            byte_limit,
        } => connect_error(
            Code::InvalidArgument,
            push_too_large_message(rows, row_limit, bytes, byte_limit),
        ),
        AdmitRefusal::PushTooManyDays { days, day_limit } => connect_error(
            Code::InvalidArgument,
            push_too_many_days_message(days, day_limit),
        ),
    }
}

/// The request's media type, lower-cased with its parameters dropped
/// (`handler.go:282-295` of the Connect library).
fn media_of(headers: &HeaderMap) -> Option<Media> {
    let value = headers.get(header::CONTENT_TYPE)?.to_str().ok()?;
    let media = value
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    match media.as_str() {
        "application/proto" => Some(Media::Proto),
        "application/json" => Some(Media::Json),
        _ => None,
    }
}

/// A Connect error before the push is decoded: its code and message.
type Refusal = (Code, String);

fn too_big(n: usize) -> Refusal {
    (
        Code::ResourceExhausted,
        format!("message size {n} is larger than configured max {MAX_PROFILE_BODY_BYTES}"),
    )
}

/// Reads the body under [`MAX_PROFILE_BODY_BYTES`]; over it, the bytes read
/// when the bound tripped.
async fn read_connect_body(mut body: Body) -> Result<Vec<u8>, Refusal> {
    let mut out = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|e| (Code::Unknown, format!("read message: {e}")))?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        if out.len() + data.len() > MAX_PROFILE_BODY_BYTES {
            return Err(too_big(out.len() + data.len()));
        }
        out.extend_from_slice(&data);
    }
    Ok(out)
}

/// `Content-Encoding` as Connect reads it (`protocol.go:307-323`,
/// `compression.go:82-106`): identity or gzip, the output under the body
/// bound.
fn decode_content(headers: &HeaderMap, body: Vec<u8>) -> Result<Vec<u8>, Refusal> {
    let encoding = headers
        .get(header::CONTENT_ENCODING)
        .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
        .unwrap_or_default();
    match encoding.as_str() {
        "" | "identity" => Ok(body),
        "gzip" => {
            if body.is_empty() {
                return Ok(body);
            }
            let mut out = Vec::new();
            flate2::read::MultiGzDecoder::new(body.as_slice())
                .take(MAX_PROFILE_BODY_BYTES as u64 + 1)
                .read_to_end(&mut out)
                .map_err(|e| (Code::InvalidArgument, format!("decompress: {e}")))?;
            if out.len() > MAX_PROFILE_BODY_BYTES {
                return Err(too_big(out.len()));
            }
            Ok(out)
        }
        other => Err((
            Code::Unimplemented,
            format!("unknown compression \"{other}\": supported encodings are gzip"),
        )),
    }
}

/// `POST /push.v1.PusherService/Push` (§3): a Connect unary `PushRequest`,
/// protobuf or proto3 JSON. Every profile is decoded and validated before
/// the push is admitted; success is `200` and an empty `PushResponse`.
pub async fn ingest_profile_push(
    sink: &dyn ProfileSink,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let tenant = match pulsus_model::tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(err) => return connect_error(Code::InvalidArgument, err.to_string()),
    };
    let Some(media) = media_of(&headers) else {
        let mut res = Response::new(Body::empty());
        *res.status_mut() = StatusCode::UNSUPPORTED_MEDIA_TYPE;
        res.headers_mut().insert(
            "accept-post",
            HeaderValue::from_static("application/json, application/proto"),
        );
        return res;
    };
    let now_ms = now_ms();
    let body = match read_connect_body(body).await {
        Ok(body) => decode_content(&headers, body),
        Err(refusal) => Err(refusal),
    };
    let body = match body {
        Ok(body) => body,
        Err((code, message)) => return connect_error(code, message),
    };
    let request = match media {
        Media::Proto => PushRequest::decode(body.as_slice()).map_err(|e| e.to_string()),
        Media::Json => serde_json::from_slice::<PushRequest>(&body).map_err(|e| e.to_string()),
    };
    let request = match request {
        Ok(request) => request,
        Err(e) => return connect_error(Code::InvalidArgument, format!("unmarshal message: {e}")),
    };
    let inputs = match push_inputs(request) {
        Ok(inputs) => inputs,
        Err(e) => return connect_error(e.code, e.message),
    };
    let landing = match parse_profiles(inputs, now_ms) {
        Ok(landing) => landing,
        Err(r) => return connect_error(Code::InvalidArgument, r.message),
    };
    let outcome = admit(sink, &headers, &tenant, landing).await;
    match outcome {
        Ok(()) => {
            let body = match media {
                Media::Proto => Body::empty(),
                Media::Json => Body::from("{}"),
            };
            let mut res = Response::new(body);
            res.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static(media.as_str()),
            );
            res
        }
        Err(Failure::Refused(r)) => connect_refusal(r),
        Err(Failure::Insert(e)) => connect_error(Code::Internal, e.to_string()),
    }
}

/// Why an admission stored nothing.
enum Failure {
    Refused(AdmitRefusal),
    Insert(LogsIngestError),
}

/// `X-Pulsus-Async: 1` admits without waiting; otherwise the landing insert
/// is awaited.
async fn admit(
    sink: &dyn ProfileSink,
    headers: &HeaderMap,
    tenant: &pulsus_model::Tenant,
    landing: ParsedProfileLanding,
) -> Result<(), Failure> {
    let push = push_headers(headers);
    if is_async(headers) {
        return sink.admit(tenant, landing, push).map_err(Failure::Refused);
    }
    sink.admit_flush(tenant, landing, push)
        .map_err(Failure::Refused)?
        .await
        .map_err(Failure::Insert)
}

// ---------------------------------------------------------------------------
// `/ingest`
// ---------------------------------------------------------------------------

/// The reference's plain-HTTP error (`pkg/util/http/error.go:44-58`): the
/// code named from the status, `nosniff`, and a trailing newline.
fn legacy_error(status: StatusCode, message: impl AsRef<str>) -> Response {
    let mut body = error_json(Code::from_http_status(status.as_u16()), message.as_ref());
    body.push('\n');
    json_response(status, body, true)
}

fn legacy_refusal(refusal: AdmitRefusal) -> Response {
    match refusal {
        AdmitRefusal::Backpressure | AdmitRefusal::DedupShed | AdmitRefusal::DedupWaitShed => {
            legacy_error(StatusCode::TOO_MANY_REQUESTS, BACKPRESSURE_MESSAGE)
        }
        AdmitRefusal::KeyReused => legacy_error(StatusCode::BAD_REQUEST, KEY_REUSED_MESSAGE),
        AdmitRefusal::PushTooLarge {
            rows,
            row_limit,
            bytes,
            byte_limit,
        } => legacy_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            push_too_large_message(rows, row_limit, bytes, byte_limit),
        ),
        AdmitRefusal::PushTooManyDays { days, day_limit } => legacy_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            push_too_many_days_message(days, day_limit),
        ),
    }
}

fn empty(status: StatusCode) -> Response {
    let mut res = Response::new(Body::empty());
    *res.status_mut() = status;
    res
}

/// The body `/ingest` serves, by `format` and `Content-Type`, first match
/// winning (`pkg/ingester/…/ingest_handler.go:207-245`).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Format {
    Pprof,
    Multipart,
    Groups,
    Lines,
    Trie,
    Tree,
    Speedscope,
    Jfr,
}

fn format_of(format: Option<&str>, content_type: &str) -> Format {
    let f = format.unwrap_or("");
    if f == "trie" || content_type == "binary/octet-stream+trie" {
        return Format::Trie;
    }
    if f == "tree" || content_type == "binary/octet-stream+tree" {
        return Format::Tree;
    }
    match f {
        "lines" => Format::Lines,
        "jfr" => Format::Jfr,
        "pprof" => Format::Pprof,
        "speedscope" => Format::Speedscope,
        _ if content_type.contains("multipart/form-data") => Format::Multipart,
        _ => Format::Groups,
    }
}

/// A converted body's refusal, in its format's family (§3.5): speedscope
/// answers as the legacy route does, every other format as the push route.
fn converted_error(speedscope: bool, e: LegacyError) -> Response {
    match (e, speedscope) {
        (LegacyError::Parse(m), _) => legacy_error(StatusCode::UNPROCESSABLE_ENTITY, m),
        (LegacyError::Metadata(m) | LegacyError::Size(m), false) => {
            connect_error(Code::InvalidArgument, m)
        }
        (LegacyError::Metadata(m), true) => legacy_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("invalid_argument: {m}"),
        ),
        (LegacyError::Size(m), true) => legacy_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("{ADAPTER} failed to push: invalid_argument: {m}"),
        ),
    }
}

/// Folded stacks, lines, trie, tree and speedscope (§3.5): the body becomes
/// stack sets, each one profile, and the push is one model call and one
/// admission. A push whose profiles hold no sample stores nothing.
async fn ingest_converted(
    sink: &dyn ProfileSink,
    headers: &HeaderMap,
    tenant: &pulsus_model::Tenant,
    query: &LegacyQuery,
    format: Format,
    body: &[u8],
    now_ms: i64,
) -> Response {
    let speedscope = format == Format::Speedscope;
    let one = |set| put_input(set, query, &query.name).map(|input| vec![input]);
    let inputs = match format {
        Format::Lines => parse_lines(body).and_then(one),
        Format::Trie => parse_trie(body).and_then(one),
        Format::Tree => parse_tree(body).and_then(one),
        Format::Speedscope => parse_speedscope(body, query),
        _ => parse_groups(body).and_then(one),
    };
    let inputs = match inputs {
        Ok(inputs) => inputs,
        Err(e) => return converted_error(speedscope, e),
    };
    let landing = match parse_profiles(inputs, now_ms) {
        Ok(landing) => landing,
        Err(r) if speedscope => {
            return legacy_error(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("{ADAPTER} failed to push: invalid_argument: {}", r.message),
            );
        }
        Err(r) => return connect_error(Code::InvalidArgument, r.message),
    };
    if landing.is_empty() {
        return empty(StatusCode::OK);
    }
    let asynchronous = is_async(headers);
    match admit(sink, headers, tenant, landing).await {
        Ok(()) if asynchronous => empty(StatusCode::ACCEPTED),
        Ok(()) => empty(StatusCode::OK),
        Err(Failure::Refused(r)) if speedscope => legacy_refusal(r),
        Err(Failure::Refused(r)) => connect_refusal(r),
        Err(Failure::Insert(e)) if speedscope => {
            legacy_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        }
        Err(Failure::Insert(e)) => connect_error(Code::Internal, e.to_string()),
    }
}

/// `POST /ingest`, `/<ref>/ingest` and `/api/profiles/v1/ingest` (§4): a
/// whole-body pprof profile, a multipart form, or a body of one of the
/// converted formats. Success is `200` with an empty body, `202` under
/// `X-Pulsus-Async: 1`.
pub async fn ingest_profile_legacy(
    sink: &dyn ProfileSink,
    query: Option<&str>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let tenant = match pulsus_model::tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(err) => return legacy_error(StatusCode::BAD_REQUEST, err.to_string()),
    };
    let now_ms = now_ms();
    let query = match parse_legacy_query(query, now_ms.saturating_mul(1_000_000)) {
        Ok(query) => query,
        Err(message) => return legacy_error(StatusCode::BAD_REQUEST, message),
    };
    let body = match read_capped_body(body, MAX_PROFILE_BODY_BYTES).await {
        Ok(body) => body,
        Err(LogsIngestError::BodyRead(e)) => return legacy_error(StatusCode::REQUEST_TIMEOUT, e),
        Err(_) => {
            return legacy_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                format!(
                    "request body too large: error reading request body bytes_read \
                     {MAX_PROFILE_BODY_BYTES}: http: request body too large"
                ),
            );
        }
    };
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let form = match format_of(query.format.as_deref(), content_type) {
        Format::Jfr => {
            return legacy_error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "format jfr is not supported",
            );
        }
        Format::Pprof => Multipart {
            profile: Some(body),
            sample_type_config: None,
        },
        Format::Multipart => match parse_multipart(content_type, &body) {
            Ok(form) => form,
            Err(e) => {
                return legacy_error(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    format!(
                        "parsing IngestInput-pprof failed failed to parse pprof /ingest \
                         multipart form {e}"
                    ),
                );
            }
        },
        format => {
            return ingest_converted(sink, &headers, &tenant, &query, format, &body, now_ms).await;
        }
    };
    let Some(bytes) = form.profile.filter(|b| !b.is_empty()) else {
        return empty(StatusCode::OK);
    };
    let profile = match decode_pprof_sized(&bytes, MAX_PROFILE_SIZE_BYTES) {
        Ok((profile, _)) => profile,
        Err(e) => {
            return legacy_error(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("parsing IngestInput-pprof failed invalid_argument: {e}"),
            );
        }
    };
    let input = legacy_pprof_input(profile, &query, form.sample_type_config.as_ref());
    let landing = match parse_profiles(vec![input], now_ms) {
        Ok(landing) => landing,
        Err(r) => {
            return legacy_error(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!(
                    "pushing IngestInput-pprof failed push series with index 0 and id  failed: \
                     invalid_argument: {}",
                    r.message
                ),
            );
        }
    };
    let asynchronous = is_async(&headers);
    match admit(sink, &headers, &tenant, landing).await {
        Ok(()) if asynchronous => empty(StatusCode::ACCEPTED),
        Ok(()) => empty(StatusCode::OK),
        Err(Failure::Refused(r)) => legacy_refusal(r),
        Err(Failure::Insert(e)) => legacy_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::Router;
    use axum::extract::{RawQuery, State};
    use axum::routing::post;
    use http_body_util::BodyExt;
    use prost::Message;
    use pulsus_profiles_wire::push_v1::{PushRequest, RawProfileSeries, RawSample};
    use pulsus_profiles_wire::types_v1::LabelPair;
    use tower::ServiceExt;

    use super::*;
    use crate::error::LogsIngestError;
    use crate::ingest::profiles::ParsedProfileLanding;
    use crate::ingest::{AdmitRefusal, FlushWait, PushHeaders};
    use crate::protocols::profile_push::fixture::{self, Pb};

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Mode {
        Admit,
        AdmitFlush,
    }

    #[derive(Clone, Copy)]
    enum Outcome {
        Ok,
        Backpressure,
        FlushFails,
    }

    /// `http.rs`'s `MockTraceSink`, for [`ProfileSink`]: records each landing
    /// and which admission took it.
    struct MockProfileSink {
        outcome: Outcome,
        admitted: Mutex<Vec<(ParsedProfileLanding, Mode)>>,
    }

    impl MockProfileSink {
        fn new(outcome: Outcome) -> Arc<MockProfileSink> {
            Arc::new(MockProfileSink {
                outcome,
                admitted: Mutex::new(Vec::new()),
            })
        }

        fn landings(&self) -> Vec<(ParsedProfileLanding, Mode)> {
            self.admitted.lock().unwrap().clone()
        }
    }

    impl ProfileSink for MockProfileSink {
        fn admit(
            &self,
            _tenant: &pulsus_model::Tenant,
            landing: ParsedProfileLanding,
            _push: PushHeaders,
        ) -> Result<(), AdmitRefusal> {
            self.admitted.lock().unwrap().push((landing, Mode::Admit));
            match self.outcome {
                Outcome::Backpressure => Err(AdmitRefusal::Backpressure),
                _ => Ok(()),
            }
        }

        fn admit_flush(
            &self,
            _tenant: &pulsus_model::Tenant,
            landing: ParsedProfileLanding,
            _push: PushHeaders,
        ) -> Result<FlushWait, AdmitRefusal> {
            self.admitted
                .lock()
                .unwrap()
                .push((landing, Mode::AdmitFlush));
            match self.outcome {
                Outcome::Backpressure => Err(AdmitRefusal::Backpressure),
                Outcome::FlushFails => Ok(FlushWait::new(async {
                    Err(LogsIngestError::FlushFailed("insert refused".to_string()))
                })),
                Outcome::Ok => Ok(FlushWait::new(async { Ok(()) })),
            }
        }
    }

    const PUSH: &str = "/push.v1.PusherService/Push";

    fn router(sink: Arc<MockProfileSink>) -> Router {
        async fn push(
            State(sink): State<Arc<MockProfileSink>>,
            headers: HeaderMap,
            body: Body,
        ) -> Response {
            ingest_profile_push(sink.as_ref(), headers, body).await
        }
        async fn legacy(
            State(sink): State<Arc<MockProfileSink>>,
            RawQuery(query): RawQuery,
            headers: HeaderMap,
            body: Body,
        ) -> Response {
            ingest_profile_legacy(sink.as_ref(), query.as_deref(), headers, body).await
        }
        Router::new()
            .route(PUSH, post(push))
            .route("/ingest", post(legacy))
            .with_state(sink)
    }

    struct Answer {
        status: StatusCode,
        headers: HeaderMap,
        body: Vec<u8>,
    }

    impl Answer {
        fn content_type(&self) -> Option<&str> {
            self.headers
                .get("content-type")
                .and_then(|v| v.to_str().ok())
        }

        fn text(&self) -> String {
            String::from_utf8_lossy(&self.body).into_owned()
        }
    }

    async fn send(
        sink: &Arc<MockProfileSink>,
        uri: &str,
        headers: &[(&str, &str)],
        body: Vec<u8>,
    ) -> Answer {
        let mut builder = axum::http::Request::builder().method("POST").uri(uri);
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        let res = router(sink.clone())
            .oneshot(builder.body(Body::from(body)).unwrap())
            .await
            .unwrap();
        let status = res.status();
        let headers = res.headers().clone();
        let body = res.into_body().collect().await.unwrap().to_bytes().to_vec();
        Answer {
            status,
            headers,
            body,
        }
    }

    fn label(name: &str, value: &str) -> LabelPair {
        LabelPair {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    /// §0's push: one series, one profile.
    fn push_request(raw_profile: Vec<u8>) -> PushRequest {
        PushRequest {
            series: vec![RawProfileSeries {
                labels: vec![
                    label("__name__", "process_cpu"),
                    label("service_name", "checkout"),
                    label("env", "prod"),
                ],
                samples: vec![RawSample {
                    raw_profile,
                    id: String::new(),
                }],
                annotations: Vec::new(),
            }],
        }
    }

    const PROTO: (&str, &str) = ("content-type", "application/proto");
    const JSON: (&str, &str) = ("content-type", "application/json");

    fn b64_std(bytes: &[u8]) -> String {
        const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let n = match chunk.len() {
                3 => (chunk[0] as u32) << 16 | (chunk[1] as u32) << 8 | chunk[2] as u32,
                2 => (chunk[0] as u32) << 16 | (chunk[1] as u32) << 8,
                _ => (chunk[0] as u32) << 16,
            };
            for i in 0..4 {
                if i <= chunk.len() {
                    out.push(A[(n >> (18 - 6 * i) & 63) as usize] as char);
                } else {
                    out.push('=');
                }
            }
        }
        out
    }

    fn b64_url_unpadded(bytes: &[u8]) -> String {
        b64_std(bytes)
            .trim_end_matches('=')
            .replace('+', "-")
            .replace('/', "_")
    }

    fn json_push(field: &str, raw: &str) -> Vec<u8> {
        format!(
            r#"{{"series":[{{"labels":[{{"name":"__name__","value":"process_cpu"}},{{"name":"service_name","value":"checkout"}},{{"name":"env","value":"prod"}}],"samples":[{{"{field}":"{raw}","ID":""}}],"unknownField":1}}]}}"#
        )
        .into_bytes()
    }

    #[tokio::test]
    async fn h1_the_push_route_admits_a_protobuf_push() {
        let sink = MockProfileSink::new(Outcome::Ok);
        let body = push_request(fixture::pb()).encode_to_vec();
        let res = send(&sink, PUSH, &[PROTO, ("x-scope-orgid", "t1")], body).await;
        assert_eq!(res.status, StatusCode::OK, "{}", res.text());
        assert_eq!(res.content_type(), Some("application/proto"));
        assert!(res.body.is_empty());
        let landings = sink.landings();
        assert_eq!(landings.len(), 1);
        assert_eq!(landings[0].1, Mode::AdmitFlush);
        assert_eq!(landings[0].0.series.len(), 2);
    }

    #[tokio::test]
    async fn h2_the_push_route_reads_proto3_json() {
        let pb = fixture::pb();
        let sink = MockProfileSink::new(Outcome::Ok);
        let proto = send(
            &sink,
            PUSH,
            &[PROTO],
            push_request(pb.clone()).encode_to_vec(),
        )
        .await;
        assert_eq!(proto.status, StatusCode::OK, "{}", proto.text());
        for (field, raw) in [
            ("rawProfile", b64_std(&pb)),
            ("raw_profile", b64_url_unpadded(&pb)),
        ] {
            let res = send(&sink, PUSH, &[JSON], json_push(field, &raw)).await;
            assert_eq!(res.status, StatusCode::OK, "{field}: {}", res.text());
            assert_eq!(res.content_type(), Some("application/json"), "{field}");
            assert_eq!(res.body, b"{}", "{field}");
        }
        let landings = sink.landings();
        assert_eq!(landings.len(), 3);
        assert_eq!(landings[1].0, landings[0].0, "rawProfile");
        assert_eq!(landings[2].0, landings[0].0, "raw_profile");
    }

    #[tokio::test]
    async fn h3_the_push_route_gunzips_the_body() {
        let pb = fixture::pb();
        let sink = MockProfileSink::new(Outcome::Ok);
        let body = push_request(pb).encode_to_vec();
        let plain = send(&sink, PUSH, &[PROTO], body.clone()).await;
        assert_eq!(plain.status, StatusCode::OK, "{}", plain.text());
        let res = send(
            &sink,
            PUSH,
            &[PROTO, ("content-encoding", "gzip")],
            fixture::gzip(&body),
        )
        .await;
        assert_eq!(res.status, StatusCode::OK, "{}", res.text());
        let landings = sink.landings();
        assert_eq!(landings.len(), 2);
        assert_eq!(landings[1].0, landings[0].0);
    }

    #[tokio::test]
    async fn h4_the_push_route_negotiates_as_connect_does() {
        let sink = MockProfileSink::new(Outcome::Ok);
        let body = push_request(fixture::pb()).encode_to_vec();
        let res = send(
            &sink,
            PUSH,
            &[("content-type", "application/x-protobuf")],
            body.clone(),
        )
        .await;
        assert_eq!(res.status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert!(res.body.is_empty());
        assert_eq!(
            res.headers.get("accept-post").and_then(|v| v.to_str().ok()),
            Some("application/json, application/proto")
        );
        let res = send(&sink, PUSH, &[PROTO, ("content-encoding", "zstd")], body).await;
        assert_eq!(res.status, StatusCode::NOT_IMPLEMENTED);
        assert_eq!(res.content_type(), Some("application/json"));
        assert_eq!(
            res.text(),
            r#"{"code":"unimplemented","message":"unknown compression \"zstd\": supported encodings are gzip"}"#
        );
        assert!(sink.landings().is_empty());
    }

    #[tokio::test]
    async fn h5_the_push_route_refuses_what_the_reference_refuses() {
        let sink = MockProfileSink::new(Outcome::Ok);
        let big = fixture::gzip(&vec![0u8; 4_194_305]);
        let cases = [
            (PushRequest { series: Vec::new() }, "no profiles received"),
            (
                push_request(fixture::no_sample_types().encode_to_vec()),
                "empty profile",
            ),
            (
                push_request(big),
                "uncompressed profile payload size exceeds limit of 4.2 MB",
            ),
        ];
        for (req, message) in cases {
            let res = send(&sink, PUSH, &[PROTO], req.encode_to_vec()).await;
            assert_eq!(res.status, StatusCode::BAD_REQUEST, "{message}");
            assert_eq!(res.content_type(), Some("application/json"), "{message}");
            assert_eq!(
                res.text(),
                format!(r#"{{"code":"invalid_argument","message":"{message}"}}"#)
            );
        }
        assert!(sink.landings().is_empty());
    }

    #[tokio::test]
    async fn h6_both_routes_read_the_tenant_first() {
        let sink = MockProfileSink::new(Outcome::Ok);
        let tenant = ("x-scope-orgid", "a|b");
        let res = send(
            &sink,
            PUSH,
            &[PROTO, tenant],
            push_request(fixture::pb()).encode_to_vec(),
        )
        .await;
        assert_eq!(res.status, StatusCode::BAD_REQUEST);
        assert_eq!(
            res.text(),
            r#"{"code":"invalid_argument","message":"invalid X-Scope-OrgID"}"#
        );
        let res = send(
            &sink,
            "/ingest?name=app&format=pprof",
            &[tenant],
            fixture::pb(),
        )
        .await;
        assert_eq!(res.status, StatusCode::BAD_REQUEST);
        assert_eq!(
            res.text(),
            "{\"code\":\"invalid_argument\",\"message\":\"invalid X-Scope-OrgID\"}\n"
        );
        assert!(sink.landings().is_empty());
    }

    #[tokio::test]
    async fn h7_backpressure_is_resource_exhausted_on_both_routes() {
        let sink = MockProfileSink::new(Outcome::Backpressure);
        let message = "sink is applying backpressure: buffers are full";
        let res = send(
            &sink,
            PUSH,
            &[PROTO],
            push_request(fixture::pb()).encode_to_vec(),
        )
        .await;
        assert_eq!(res.status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            res.text(),
            format!(r#"{{"code":"resource_exhausted","message":"{message}"}}"#)
        );
        let res = send(&sink, "/ingest?name=app&format=pprof", &[], fixture::pb()).await;
        assert_eq!(res.status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            res.text(),
            format!("{{\"code\":\"resource_exhausted\",\"message\":\"{message}\"}}\n")
        );
        assert_eq!(
            res.headers
                .get("x-content-type-options")
                .and_then(|v| v.to_str().ok()),
            Some("nosniff")
        );
        assert_eq!(res.content_type(), Some("application/json"));
    }

    #[tokio::test]
    async fn h8_ingest_names_the_series_by_the_whole_name() {
        let sink = MockProfileSink::new(Outcome::Ok);
        let res = send(
            &sink,
            "/ingest?name=app.cpu%7Benv%3Dprod%7D&format=pprof",
            &[],
            fixture::pb(),
        )
        .await;
        assert_eq!(res.status, StatusCode::OK, "{}", res.text());
        assert!(res.body.is_empty());
        assert_eq!(res.content_type(), None);
        let landings = sink.landings();
        assert_eq!(landings.len(), 1);
        assert_eq!(
            landings[0].0.series[0].labels,
            r#"{"__name__":"process_cpu","__period_type__":"cpu","__period_unit__":"nanoseconds","__profile_type__":"process_cpu:samples:count:cpu:nanoseconds","__service_name__":"app.cpu","__type__":"samples","__unit__":"count","env":"prod","pyroscope_spy":"unknown","service_name":"app.cpu"}"#
        );
    }

    fn lock_profile() -> Vec<u8> {
        let mut b = Pb::new(fixture::now_ns());
        b.sample_type("contentions", "count")
            .sample_type("delay", "nanoseconds")
            .period("contentions", "count")
            .func(1, "main", "main.go")
            .loc(1, &[1])
            .sample(&[1], &[2, 2_000]);
        b.build().encode_to_vec()
    }

    #[tokio::test]
    async fn h9_ingest_names_the_series_through_the_sample_type_config() {
        let config = br#"{"contentions":{"display-name":"block_count"},"delay":{"display-name":"block_duration"}}"#;
        let profile = lock_profile();
        let ct = ("content-type", "multipart/form-data; boundary=b0undary");
        let cases: [(Option<&[u8]>, [&str; 2]); 2] = [
            (
                Some(config),
                [
                    "block:contentions:count:contentions:count",
                    "block:delay:nanoseconds:contentions:count",
                ],
            ),
            (
                None,
                [
                    "mutex:contentions:count:contentions:count",
                    "mutex:delay:nanoseconds:contentions:count",
                ],
            ),
        ];
        for (config, want) in cases {
            let sink = MockProfileSink::new(Outcome::Ok);
            let mut parts: Vec<(&str, Option<&str>, &[u8])> =
                vec![("profile", Some("profile.pprof"), &profile)];
            if let Some(c) = config {
                parts.push(("sample_type_config", Some("config.json"), c));
            }
            let body = fixture::multipart("b0undary", &parts);
            let res = send(&sink, "/ingest?name=app&spyName=gospy", &[ct], body).await;
            assert_eq!(res.status, StatusCode::OK, "{}", res.text());
            let landings = sink.landings();
            assert_eq!(landings.len(), 1);
            let type_ids: Vec<&str> = landings[0].0.series.iter().map(|s| &*s.type_id).collect();
            assert_eq!(type_ids, want, "config {}", config.is_some());
        }
    }

    #[tokio::test]
    async fn h10_ingest_without_a_name_is_refused() {
        let sink = MockProfileSink::new(Outcome::Ok);
        let res = send(&sink, "/ingest?format=pprof", &[], fixture::pb()).await;
        assert_eq!(res.status, StatusCode::BAD_REQUEST);
        assert_eq!(
            res.text(),
            "{\"code\":\"invalid_argument\",\"message\":\"name: service name is required\"}\n"
        );
        assert!(sink.landings().is_empty());
    }

    #[tokio::test]
    async fn h11_ingest_refusals_are_unprocessable() {
        let sink = MockProfileSink::new(Outcome::Ok);
        let uri = "/ingest?name=app&format=pprof";
        let res = send(&sink, uri, &[], fixture::no_sample_types().encode_to_vec()).await;
        assert_eq!(res.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(
            res.text(),
            "{\"code\":\"unknown\",\"message\":\"pushing IngestInput-pprof failed push series with index 0 and id  failed: invalid_argument: empty profile\"}\n"
        );
        let res = send(&sink, uri, &[], b"garbage".to_vec()).await;
        assert_eq!(res.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(
            res.text()
                .starts_with("{\"code\":\"unknown\",\"message\":\"parsing IngestInput-pprof failed invalid_argument: "),
            "{}",
            res.text()
        );
        assert!(sink.landings().is_empty());
    }

    #[tokio::test]
    async fn h12_ingest_without_a_profile_stores_nothing() {
        let sink = MockProfileSink::new(Outcome::Ok);
        let res = send(&sink, "/ingest?name=app&format=pprof", &[], Vec::new()).await;
        assert_eq!(res.status, StatusCode::OK, "{}", res.text());
        let body = fixture::multipart("b0undary", &[("other", Some("x"), b"x")]);
        let res = send(
            &sink,
            "/ingest?name=app",
            &[("content-type", "multipart/form-data; boundary=b0undary")],
            body,
        )
        .await;
        assert_eq!(res.status, StatusCode::OK, "{}", res.text());
        assert!(sink.landings().is_empty());
    }

    #[tokio::test]
    async fn h13_a_body_that_is_not_a_recording_is_refused() {
        let sink = MockProfileSink::new(Outcome::Ok);
        let res = send(&sink, "/ingest?name=app&format=jfr", &[], b"JFR".to_vec()).await;
        assert_eq!(res.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(nosniff(&res));
        assert_eq!(
            res.text(),
            "{\"code\":\"unknown\",\"message\":\"parsing IngestInput-pprof failed jfr parser \
             ParseEvent error: error reading chunk header: unexpected EOF\"}\n"
        );
        assert!(sink.landings().is_empty());
    }

    #[tokio::test]
    async fn h14_async_admits_without_waiting() {
        let sink = MockProfileSink::new(Outcome::Ok);
        let asynch = ("x-pulsus-async", "1");
        let res = send(
            &sink,
            "/ingest?name=app&format=pprof",
            &[asynch],
            fixture::pb(),
        )
        .await;
        assert_eq!(res.status, StatusCode::ACCEPTED, "{}", res.text());
        let res = send(
            &sink,
            PUSH,
            &[PROTO, asynch],
            push_request(fixture::pb()).encode_to_vec(),
        )
        .await;
        assert_eq!(res.status, StatusCode::OK, "{}", res.text());
        let modes: Vec<Mode> = sink.landings().iter().map(|(_, m)| *m).collect();
        assert_eq!(modes, [Mode::Admit, Mode::Admit]);
    }

    #[tokio::test]
    async fn h15_a_failed_insert_is_a_server_error() {
        let sink = MockProfileSink::new(Outcome::FlushFails);
        let res = send(
            &sink,
            PUSH,
            &[PROTO],
            push_request(fixture::pb()).encode_to_vec(),
        )
        .await;
        assert_eq!(res.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(
            res.text().starts_with(r#"{"code":"internal","message":""#),
            "{}",
            res.text()
        );
        let res = send(&sink, "/ingest?name=app&format=pprof", &[], fixture::pb()).await;
        assert_eq!(res.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(
            res.text().starts_with(r#"{"code":"unknown","message":""#),
            "{}",
            res.text()
        );
    }

    fn nosniff(res: &Answer) -> bool {
        res.headers
            .get("x-content-type-options")
            .is_some_and(|v| v == "nosniff")
    }

    /// §0's push: folded stacks, no `format`, no `Content-Type`.
    #[tokio::test]
    async fn h16_a_folded_push_is_stored_as_one_cpu_profile() {
        let sink = MockProfileSink::new(Outcome::Ok);
        let res = send(
            &sink,
            "/ingest?name=checkout.cpu%7Benv%3Dprod%7D",
            &[("x-scope-orgid", "t1")],
            b"main;work 3\nmain;idle 2\n".to_vec(),
        )
        .await;
        assert_eq!(res.status, StatusCode::OK, "{}", res.text());
        assert!(res.body.is_empty());
        let landings = sink.landings();
        assert_eq!(landings.len(), 1);
        let (landing, mode) = &landings[0];
        assert_eq!(*mode, Mode::AdmitFlush);
        assert_eq!(landing.series.len(), 1);
        assert_eq!(
            &*landing.series[0].type_id,
            "process_cpu:cpu:nanoseconds:cpu:nanoseconds"
        );
        assert_eq!(
            landing.series[0].labels,
            r#"{"__name__":"process_cpu","__period_type__":"cpu","__period_unit__":"nanoseconds","__profile_type__":"process_cpu:cpu:nanoseconds:cpu:nanoseconds","__service_name__":"checkout","__type__":"cpu","__unit__":"nanoseconds","env":"prod","pyroscope_spy":"unknown","service_name":"checkout"}"#
        );
        assert_eq!(landing.entries.len(), 1);
        assert_eq!(landing.entries[0].total, 50_000_000);
        let mut values: Vec<i64> = landing.samples.iter().map(|s| s.value).collect();
        values.sort();
        assert_eq!(values, [20_000_000, 30_000_000]);
        assert_eq!(landing.stacks.len(), 2);
        assert_eq!(landing.locations.len(), 3);
    }

    #[tokio::test]
    async fn h17_each_format_is_found_by_format_or_content_type() {
        use crate::protocols::profileql_legacy::test_support::{TREE, TRIE, two_sampled};
        let tree = ("content-type", "binary/octet-stream+tree");
        // The URI, the headers, the body, and the series and stacks stored.
        type Case<'a> = (&'a str, &'a [(&'a str, &'a str)], Vec<u8>, usize, usize);
        let cases: [Case; 6] = [
            ("/ingest?name=app&format=trie", &[], TRIE.to_vec(), 1, 2),
            ("/ingest?name=app", &[tree], TREE.to_vec(), 1, 2),
            (
                "/ingest?name=app&format=lines",
                &[],
                b"a;b\na;b\na;c\n".to_vec(),
                1,
                2,
            ),
            (
                "/ingest?name=app&format=groups",
                &[],
                b"a;b 1\na;c 2\n".to_vec(),
                1,
                2,
            ),
            (
                "/ingest?name=app&format=bogus",
                &[],
                b"a;b 1\na;c 2\n".to_vec(),
                1,
                2,
            ),
            (
                "/ingest?name=foo%7Bx%3Dy%7D&format=speedscope",
                &[],
                two_sampled().into_bytes(),
                2,
                3,
            ),
        ];
        for (uri, headers, body, series, stacks) in cases {
            let sink = MockProfileSink::new(Outcome::Ok);
            let res = send(&sink, uri, headers, body).await;
            assert_eq!(res.status, StatusCode::OK, "{uri}: {}", res.text());
            let landings = sink.landings();
            assert_eq!(landings.len(), 1, "{uri}");
            assert_eq!(landings[0].0.series.len(), series, "{uri}");
            assert_eq!(landings[0].0.stacks.len(), stacks, "{uri}");
        }
    }

    #[tokio::test]
    async fn h18_converted_errors_take_the_references_two_families() {
        use crate::protocols::profileql_legacy::test_support::SIMPLE;
        let sink = MockProfileSink::new(Outcome::Ok);
        let res = send(&sink, "/ingest?name=app", &[], b"a x".to_vec()).await;
        assert_eq!(res.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(nosniff(&res));
        assert_eq!(
            res.text(),
            "{\"code\":\"unknown\",\"message\":\"strconv.Atoi: parsing \\\"x\\\": invalid syntax\"}\n"
        );

        let res = send(&sink, "/ingest?name=my.app", &[], b"a 1\n".to_vec()).await;
        assert_eq!(res.status, StatusCode::BAD_REQUEST);
        assert!(!nosniff(&res));
        assert_eq!(
            res.text(),
            r#"{"code":"invalid_argument","message":"pyroscopeIngesterAdapter failed to convert metadata: unknown profile type: app"}"#
        );

        let res = send(
            &sink,
            "/ingest?name=my.app&format=speedscope",
            &[],
            SIMPLE.as_bytes().to_vec(),
        )
        .await;
        assert_eq!(res.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(nosniff(&res));
        assert_eq!(
            res.text(),
            "{\"code\":\"unknown\",\"message\":\"invalid_argument: pyroscopeIngesterAdapter failed to convert metadata: unknown profile type: app\"}\n"
        );

        let res = send(
            &sink,
            "/ingest?name=app&from=now-2h",
            &[],
            b"a 1\n".to_vec(),
        )
        .await;
        assert_eq!(res.status, StatusCode::BAD_REQUEST, "{}", res.text());
        assert!(
            res.text()
                .starts_with(r#"{"code":"invalid_argument","message":"profile with labels '"#),
            "{}",
            res.text()
        );
        assert!(!res.text().ends_with('\n'));
        assert!(sink.landings().is_empty());
    }

    #[tokio::test]
    async fn h19_an_empty_folded_body_stores_nothing() {
        let sink = MockProfileSink::new(Outcome::Ok);
        let res = send(&sink, "/ingest?name=app", &[], Vec::new()).await;
        assert_eq!(res.status, StatusCode::OK, "{}", res.text());
        assert!(res.body.is_empty());
        assert!(sink.landings().is_empty());
    }

    #[tokio::test]
    async fn h20_backpressure_answers_in_each_formats_family() {
        use crate::protocols::profileql_legacy::test_support::two_sampled;
        let sink = MockProfileSink::new(Outcome::Backpressure);
        let message = r#"{"code":"resource_exhausted","message":"sink is applying backpressure: buffers are full"}"#;
        let res = send(&sink, "/ingest?name=app", &[], b"a 1\n".to_vec()).await;
        assert_eq!(res.status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(res.text(), message);
        assert!(!nosniff(&res));
        let res = send(
            &sink,
            "/ingest?name=foo&format=speedscope",
            &[],
            two_sampled().into_bytes(),
        )
        .await;
        assert_eq!(res.status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(res.text(), format!("{message}\n"));
        assert!(nosniff(&res));
    }

    const JFR_FORM: (&str, &str) = ("content-type", "multipart/form-data; boundary=b0undary");

    /// §0's push of issue #819: `example.jfr`, gzipped, as the form's `jfr`
    /// part, its window ending now unless `from` is given.
    fn jfr_push(from: Option<&str>) -> (String, Vec<u8>) {
        use crate::protocols::profileql_legacy::jfr::test_support::fixture as jfr_file;
        let now_s = fixture::now_ns() / 1_000_000_000;
        let from = from.map_or_else(|| (now_s - 10).to_string(), str::to_string);
        let uri = format!(
            "/ingest?name=checkout%7Benv%3Dprod%7D&format=jfr&spyName=javaspy&sampleRate=100\
             &from={from}&until={now_s}"
        );
        let body = fixture::multipart(
            "b0undary",
            &[("jfr", Some("jfr"), &jfr_file("example.jfr.gz"))],
        );
        (uri, body)
    }

    /// Each series' type id and the sum of its entries' totals.
    fn totals(landing: &ParsedProfileLanding) -> std::collections::BTreeMap<String, i64> {
        let mut out = std::collections::BTreeMap::new();
        for e in &landing.entries {
            let id = landing.series[e.series as usize].type_id.to_string();
            *out.entry(id).or_insert(0) += e.total;
        }
        out
    }

    /// §0's push: one insert of the seven series the reference stores, the
    /// lock delay in nanoseconds.
    #[tokio::test]
    async fn h21_a_jfr_push_is_stored_as_the_references_series() {
        let sink = MockProfileSink::new(Outcome::Ok);
        let (uri, body) = jfr_push(None);
        let res = send(&sink, &uri, &[("x-scope-orgid", "t1"), JFR_FORM], body).await;
        assert_eq!(res.status, StatusCode::OK, "{}", res.text());
        assert!(res.body.is_empty());
        let landings = sink.landings();
        assert_eq!(landings.len(), 1);
        let (landing, mode) = &landings[0];
        assert_eq!(*mode, Mode::AdmitFlush);
        let mut totals = totals(landing);
        let delay = totals.remove("mutex:delay:nanoseconds:mutex:count");
        assert!(
            delay.is_some_and(|d| (8_005_192_299..=8_005_192_311).contains(&d)),
            "{delay:?}"
        );
        let want: std::collections::BTreeMap<String, i64> = [
            (
                "process_cpu:cpu:nanoseconds:cpu:nanoseconds",
                10_310_000_000,
            ),
            ("memory:alloc_in_new_tlab_objects:count:space:bytes", 30),
            (
                "memory:alloc_in_new_tlab_bytes:bytes:space:bytes",
                27_650_544,
            ),
            ("memory:alloc_outside_tlab_objects:count:space:bytes", 260),
            (
                "memory:alloc_outside_tlab_bytes:bytes:space:bytes",
                139_855_503_616,
            ),
            ("mutex:contentions:count:mutex:count", 13),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), *v))
        .collect();
        assert_eq!(totals, want);
        assert_eq!(landing.stacks.len(), 258);
        assert_eq!(landing.locations.len(), 756);
        let cpu = landing
            .series
            .iter()
            .find(|s| &*s.type_id == "process_cpu:cpu:nanoseconds:cpu:nanoseconds")
            .map(|s| s.labels.as_str());
        assert_eq!(
            cpu,
            Some(
                r#"{"__name__":"process_cpu","__period_type__":"cpu","__period_unit__":"nanoseconds","__profile_type__":"process_cpu:cpu:nanoseconds:cpu:nanoseconds","__service_name__":"checkout","__type__":"cpu","__unit__":"nanoseconds","env":"prod","jfr_event":"cpu","pyroscope_spy":"javaspy","service_name":"checkout"}"#
            )
        );
    }

    /// The reference's own labelled push (`ingest_jfr_test.go:105-113`): the
    /// form's `labels` part names each sample's thread.
    #[tokio::test]
    async fn h22_the_labels_part_splits_the_series_by_thread() {
        use crate::protocols::profileql_legacy::jfr::test_support::fixture as jfr_file;
        let sink = MockProfileSink::new(Outcome::Ok);
        let body = fixture::multipart(
            "b0undary",
            &[
                ("jfr", Some("jfr"), &jfr_file("dump1.jfr.gz")),
                ("labels", Some("labels"), &jfr_file("dump1.labels.pb.gz")),
            ],
        );
        let uri = "/ingest?name=javaapp&format=jfr&spyName=javaspy";
        let res = send(&sink, uri, &[JFR_FORM], body).await;
        assert_eq!(res.status, StatusCode::OK, "{}", res.text());
        let landings = sink.landings();
        assert_eq!(landings.len(), 1);
        let landing = &landings[0].0;
        assert_eq!(landing.series.len(), 5, "{:?}", landing.series);
        assert!(
            landing
                .series
                .iter()
                .all(|s| &*s.type_id == "process_cpu:cpu:nanoseconds:cpu:nanoseconds")
        );
        let threads: std::collections::BTreeSet<&str> = landing
            .series
            .iter()
            .filter_map(|s| {
                let at = s.labels.find(r#""thread_name":""#)?;
                let rest = &s.labels[at + 15..];
                rest.split('"').next()
            })
            .collect();
        assert_eq!(threads.len(), 4, "{threads:?}");
        assert_eq!(
            landing.entries.iter().map(|e| e.total).sum::<i64>(),
            21_880_000_000
        );
        for s in &landing.series {
            for part in [r#""jfr_event":"itimer""#, r#""service_name":"javaapp""#] {
                assert!(s.labels.contains(part), "{part} in {}", s.labels);
            }
            assert!(!s.labels.contains("app_name"), "{}", s.labels);
        }
    }

    #[tokio::test]
    async fn h23_jfr_refusals_answer_in_the_legacy_form() {
        use crate::protocols::profileql_legacy::jfr::test_support::unzipped;
        let sink = MockProfileSink::new(Outcome::Ok);
        let cut = unzipped("example.jfr.gz")[..1000].to_vec();
        let res = send(&sink, "/ingest?name=app&format=jfr", &[], cut).await;
        assert_eq!(res.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(nosniff(&res));
        assert_eq!(
            res.text(),
            "{\"code\":\"unknown\",\"message\":\"parsing IngestInput-pprof failed jfr parser \
             ParseEvent error: error reading metadata: unexpected EOF\"}\n"
        );

        let body = fixture::multipart("b0undary", &[("other", Some("x"), b"x")]);
        let res = send(&sink, "/ingest?name=app&format=jfr", &[JFR_FORM], body).await;
        assert_eq!(res.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(
            res.text(),
            "{\"code\":\"unknown\",\"message\":\"parsing IngestInput-pprof failed jfr field is \
             required\"}\n"
        );

        let (uri, body) = jfr_push(Some("now-2h"));
        let res = send(&sink, &uri, &[JFR_FORM], body).await;
        assert_eq!(
            res.status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "{}",
            res.text()
        );
        assert!(nosniff(&res));
        assert!(
            res.text().starts_with(
                "{\"code\":\"unknown\",\"message\":\"pushing IngestInput-pprof failed push series \
                 with index 0 and id  failed: invalid_argument: profile with labels '"
            ),
            "{}",
            res.text()
        );
        assert!(sink.landings().is_empty());

        let sink = MockProfileSink::new(Outcome::Backpressure);
        let (uri, body) = jfr_push(None);
        let res = send(&sink, &uri, &[JFR_FORM], body).await;
        assert_eq!(res.status, StatusCode::TOO_MANY_REQUESTS);
        assert!(nosniff(&res));
        assert_eq!(
            res.text(),
            "{\"code\":\"resource_exhausted\",\"message\":\"sink is applying backpressure: \
             buffers are full\"}\n"
        );
    }
}
