//! OTLP over gRPC (issue #816, docs/api.md §1.1): a unary shim in front of
//! the four OTLP/HTTP export handlers.
//!
//! The shim strips the 5-byte gRPC frame and hands the message bytes, as
//! sent, to the signal's own `/v1/*` handler, then turns that handler's
//! HTTP answer into a gRPC one. It decodes nothing itself: a generated
//! gRPC server decodes before the handler runs, and the handler would then
//! see a re-encoded message, without the unknown fields the profiles route
//! refuses.

use std::future::Future;
use std::sync::{Arc, OnceLock};

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use futures::StreamExt;
use http_body_util::{BodyExt, Full};
use prost::Message;

use crate::app::AppState;

/// `LogsService/Export`: the `/v1/logs` handler behind the shim.
pub(crate) async fn export_logs(State(s): State<AppState>, h: HeaderMap, b: Body) -> Response {
    unary(h, b, |h, b| crate::ingest::ingest_logs(State(s), h, b)).await
}

/// `MetricsService/Export`: the `/v1/metrics` handler behind the shim.
pub(crate) async fn export_metrics(State(s): State<AppState>, h: HeaderMap, b: Body) -> Response {
    unary(h, b, |h, b| crate::ingest::ingest_metrics(State(s), h, b)).await
}

/// `TraceService/Export`: the `/v1/traces` handler behind the shim.
pub(crate) async fn export_traces(State(s): State<AppState>, h: HeaderMap, b: Body) -> Response {
    unary(h, b, |h, b| crate::ingest::ingest_traces(State(s), h, b)).await
}

/// `ProfilesService/Export`: the `/v1development/profiles` handler behind
/// the shim.
pub(crate) async fn export_profiles(State(s): State<AppState>, h: HeaderMap, b: Body) -> Response {
    unary(h, b, |h, b| {
        crate::ingest::ingest_otlp_profiles(State(s), h, b)
    })
    .await
}

const GRPC_CONTENT_TYPE: &str = "application/grpc";
const DEADLINE_EXCEEDED: i32 = 4;
const RESOURCE_EXHAUSTED: i32 = 8;
const UNIMPLEMENTED: i32 = 12;
const INTERNAL: i32 = 13;
const UNAUTHENTICATED: i32 = 16;

const TRUNCATED: &str = "truncated gRPC message";
const EXTRA_MESSAGE: &str = "more than one message in a unary call";
const RETRY_INFO: &str = "type.googleapis.com/google.rpc.RetryInfo";

/// The frame error the message stream met, if any. Set by the stream the
/// handler reads, read by [`to_grpc`] once the handler has answered.
type FrameError = Arc<OnceLock<String>>;

/// `google.rpc.Status`.
#[derive(Clone, PartialEq, ::prost::Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<ProtoAny>,
}

/// `google.protobuf.Any`.
#[derive(Clone, PartialEq, ::prost::Message)]
struct ProtoAny {
    #[prost(string, tag = "1")]
    type_url: String,
    #[prost(bytes = "vec", tag = "2")]
    value: Vec<u8>,
}

/// The gRPC code for a handler's HTTP status (docs/api.md §1.1). `500` and
/// `503` are `UNAVAILABLE`, which the collector's exporter retries; `401`
/// is answered by the authentication layer before the shim runs, as a
/// trailers-only `UNAUTHENTICATED` ([`unauthenticated_response`]).
pub(crate) fn grpc_code(status: StatusCode) -> i32 {
    match status.as_u16() {
        200..=299 => 0,
        400 | 413 => 3,
        401 => UNAUTHENTICATED,
        429 => RESOURCE_EXHAUSTED,
        500 | 503 => 14,
        _ => INTERNAL,
    }
}

/// Reads the 5-byte prefix: the compressed flag and the message length.
/// The returned body yields the rest, exactly the length's bytes, and
/// fails if more arrive or the stream ends short, recording why in the
/// returned [`FrameError`].
async fn unframe(body: Body) -> Result<(bool, Body, FrameError), String> {
    let mut stream = body.into_data_stream();
    let mut head = Vec::with_capacity(5);
    while head.len() < 5 {
        match stream.next().await {
            Some(Ok(chunk)) => head.extend_from_slice(&chunk),
            Some(Err(e)) => return Err(e.to_string()),
            None => return Err(TRUNCATED.to_string()),
        }
    }
    let flag = head[0];
    if flag > 1 {
        return Err(format!("invalid gRPC compressed flag {flag}"));
    }
    let len = u64::from(u32::from_be_bytes([head[1], head[2], head[3], head[4]]));
    let rest = Bytes::from(head).slice(5..);
    let error = FrameError::default();
    let state = Unframing {
        stream,
        pending: (!rest.is_empty()).then_some(rest),
        remaining: len,
        error: Arc::clone(&error),
        done: false,
    };
    let message = futures::stream::unfold(state, |mut s| async move {
        let item = s.next().await?;
        Some((item, s))
    });
    Ok((flag == 1, Body::from_stream(message), error))
}

/// The state of the message stream [`unframe`] returns.
struct Unframing {
    stream: axum::body::BodyDataStream,
    pending: Option<Bytes>,
    remaining: u64,
    error: FrameError,
    done: bool,
}

impl Unframing {
    async fn next(&mut self) -> Option<Result<Bytes, std::io::Error>> {
        while !self.done {
            let chunk = match self.pending.take() {
                Some(chunk) => Some(Ok(chunk)),
                None => self.stream.next().await,
            };
            match chunk {
                Some(Ok(chunk)) if chunk.is_empty() => {}
                Some(Ok(chunk)) => {
                    let n = chunk.len() as u64;
                    if n > self.remaining {
                        return Some(Err(self.fail(EXTRA_MESSAGE)));
                    }
                    self.remaining -= n;
                    return Some(Ok(chunk));
                }
                Some(Err(e)) => {
                    self.done = true;
                    return Some(Err(std::io::Error::other(e)));
                }
                None if self.remaining > 0 => return Some(Err(self.fail(TRUNCATED))),
                None => self.done = true,
            }
        }
        None
    }

    fn fail(&mut self, message: &str) -> std::io::Error {
        self.done = true;
        let _ = self.error.set(message.to_string());
        std::io::Error::other(message)
    }
}

/// One unary call: the gRPC request becomes the handler's HTTP request, and
/// the handler's answer becomes the gRPC answer.
async fn unary<F, Fut>(mut headers: HeaderMap, body: Body, handler: F) -> Response
where
    F: FnOnce(HeaderMap, Body) -> Fut,
    Fut: Future<Output = Response>,
{
    let is_grpc = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with(GRPC_CONTENT_TYPE));
    if !is_grpc {
        return StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response();
    }
    let gzip = match headers.get("grpc-encoding").map(HeaderValue::as_bytes) {
        None | Some(b"identity") => false,
        Some(b"gzip") => true,
        Some(other) => {
            let mut res = trailers_only(
                UNIMPLEMENTED,
                &format!(
                    "grpc-encoding {} is not supported",
                    String::from_utf8_lossy(other)
                ),
            );
            res.headers_mut()
                .insert("grpc-accept-encoding", HeaderValue::from_static("gzip"));
            return res;
        }
    };
    let (compressed, message, frame_error) = match unframe(body).await {
        Ok(unframed) => unframed,
        Err(message) => return trailers_only(INTERNAL, &message),
    };
    if compressed && !gzip {
        return trailers_only(INTERNAL, "compressed flag set without grpc-encoding");
    }
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-protobuf"),
    );
    headers.remove(header::CONTENT_LENGTH);
    if compressed {
        headers.insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    } else {
        headers.remove(header::CONTENT_ENCODING);
    }
    let mut resp = handler(headers, message).await;
    // Issue #901: the route's refused push, carried to the gRPC answer for
    // the refused-push layer outside this shim.
    let refused = resp.extensions_mut().remove::<pulsus_write::RefusedPush>();
    let mut answer = to_grpc(resp, &frame_error).await;
    if let Some(refused) = refused {
        answer.extensions_mut().insert(refused);
    }
    answer
}

/// The handler's answer as a gRPC answer. A frame error wins over whatever
/// the handler said, so a malformed message is never retried.
async fn to_grpc(resp: Response, frame_error: &OnceLock<String>) -> Response {
    let status = resp.status();
    let protobuf = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .is_some_and(|v| v.as_bytes() == b"application/x-protobuf");
    // These bodies are small: an export response or an error text.
    let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await;
    if let Some(message) = frame_error.get() {
        return trailers_only(INTERNAL, message);
    }
    let body = match body {
        Ok(body) => body,
        Err(e) => return trailers_only(INTERNAL, &e.to_string()),
    };
    if status.is_success() {
        return success(&body);
    }
    let message = if protobuf {
        RpcStatus::decode(body)
            .map(|s| s.message)
            .unwrap_or_default()
    } else {
        let text = body.strip_suffix(b"\n").unwrap_or(&body);
        String::from_utf8_lossy(text).into_owned()
    };
    trailers_only(grpc_code(status), &message)
}

/// `200`, the message in one frame, and `grpc-status: 0` in the trailers.
fn success(message: &[u8]) -> Response {
    let len = u32::try_from(message.len()).unwrap_or(u32::MAX);
    let mut frame = Vec::with_capacity(5 + message.len());
    frame.push(0);
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(message);
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", HeaderValue::from_static("0"));
    let body = Full::new(Bytes::from(frame))
        .with_trailers(async move { Some(Ok::<_, std::convert::Infallible>(trailers)) });
    let mut res = Response::new(Body::new(body));
    res.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(GRPC_CONTENT_TYPE),
    );
    res
}

/// A trailers-only answer: `200`, the status in the headers, no body. A
/// `RESOURCE_EXHAUSTED` carries a `RetryInfo` detail, without which the
/// collector's exporter does not retry it.
fn trailers_only(code: i32, message: &str) -> Response {
    let mut res = Response::new(Body::empty());
    let headers = res.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(GRPC_CONTENT_TYPE),
    );
    headers.insert("grpc-status", HeaderValue::from(code));
    if !message.is_empty()
        && let Ok(value) = HeaderValue::try_from(encode_message(message))
    {
        headers.insert("grpc-message", value);
    }
    if code == RESOURCE_EXHAUSTED {
        let details = RpcStatus {
            code,
            message: message.to_string(),
            details: vec![ProtoAny {
                type_url: RETRY_INFO.to_string(),
                value: vec![],
            }],
        }
        .encode_to_vec();
        let encoded = base64::engine::general_purpose::STANDARD_NO_PAD.encode(details);
        if let Ok(value) = HeaderValue::try_from(encoded) {
            headers.insert("grpc-status-details-bin", value);
        }
    }
    res
}

/// `grpc-message`'s percent-encoding: every byte outside `0x20..=0x7E`, and
/// `%` itself, as `%XX`.
pub(crate) fn encode_message(message: &str) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(message.len());
    for &b in message.as_bytes() {
        if (0x20..=0x7E).contains(&b) && b != b'%' {
            out.push(char::from(b));
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}

/// The authentication layer's refusal on the gRPC export paths:
/// `UNAUTHENTICATED`, read the same way by every gRPC client.
pub(crate) fn unauthenticated_response() -> Response {
    trailers_only(UNAUTHENTICATED, "unauthorized")
}

/// The request deadline's answer on the gRPC export paths:
/// `DEADLINE_EXCEEDED`, which the exporter retries.
pub(crate) fn deadline_response() -> Response {
    trailers_only(DEADLINE_EXCEEDED, "deadline exceeded")
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;

    use axum::body::Bytes;
    use axum::http::header;
    use http_body_util::BodyExt;
    use prost::Message;

    /// The codes the collector's gRPC exporter retries without a
    /// `RetryInfo`: `exporter/otlpexporter/otlp.go:233-238` @ `f50b2c48`.
    const EXPORTER_RETRIES: [i32; 6] = [1, 4, 10, 11, 14, 15];

    fn header_of<'a>(res: &'a Response, name: &str) -> Option<&'a str> {
        res.headers().get(name).and_then(|v| v.to_str().ok())
    }

    fn body_of(frames: &[&'static [u8]]) -> Body {
        let chunks: Vec<Result<Bytes, std::io::Error>> =
            frames.iter().map(|f| Ok(Bytes::from_static(f))).collect();
        Body::from_stream(futures::stream::iter(chunks))
    }

    fn grpc_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, "application/grpc".parse().unwrap());
        headers
    }

    fn answer(status: u16, content_type: &str, body: Vec<u8>) -> Response {
        let mut res = Response::new(Body::from(body));
        *res.status_mut() = StatusCode::from_u16(status).unwrap();
        res.headers_mut()
            .insert(header::CONTENT_TYPE, content_type.parse().unwrap());
        res
    }

    fn rpc_status(code: i32, message: &str) -> Vec<u8> {
        RpcStatus {
            code,
            message: message.to_string(),
            details: vec![],
        }
        .encode_to_vec()
    }

    fn code_of(res: &Response) -> Option<i32> {
        header_of(res, "grpc-status").and_then(|v| v.parse().ok())
    }

    #[test]
    fn status_table_matches_the_design() {
        let codes: Vec<i32> = [200, 202, 400, 401, 413, 422, 429, 500, 503]
            .iter()
            .map(|s| grpc_code(StatusCode::from_u16(*s).unwrap()))
            .collect();
        assert_eq!(codes, [0, 0, 3, 16, 3, 13, 8, 14, 14]);
    }

    #[tokio::test]
    async fn transient_failures_get_codes_the_exporter_retries() {
        let code = |s: u16| grpc_code(StatusCode::from_u16(s).unwrap());
        assert!(EXPORTER_RETRIES.contains(&code(500)), "500");
        assert!(EXPORTER_RETRIES.contains(&code(503)), "503");
        let deadline = code_of(&deadline_response());
        assert!(
            deadline.is_some_and(|c| EXPORTER_RETRIES.contains(&c)),
            "the deadline answer's code {deadline:?}"
        );
        assert!(!EXPORTER_RETRIES.contains(&code(400)), "400");
        assert!(!EXPORTER_RETRIES.contains(&code(413)), "413");
        assert_eq!(code(429), 8);

        let res = unary(grpc_headers(), body_of(&[&[0, 0, 0, 0, 0]]), |_, _| async {
            answer(
                429,
                "application/x-protobuf",
                rpc_status(8, "sink is applying backpressure: buffers are full"),
            )
        })
        .await;
        assert_eq!(code_of(&res), Some(8));
        let details = header_of(&res, "grpc-status-details-bin").map(|v| {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD_NO_PAD
                .decode(v)
                .expect("base64")
        });
        let status = details.map(|d| RpcStatus::decode(d.as_slice()).expect("google.rpc.Status"));
        assert_eq!(
            status.map(|s| (s.code, s.message, s.details)),
            Some((
                8,
                "sink is applying backpressure: buffers are full".to_string(),
                vec![ProtoAny {
                    type_url: "type.googleapis.com/google.rpc.RetryInfo".to_string(),
                    value: vec![],
                }]
            ))
        );
    }

    #[tokio::test]
    async fn a_flush_failure_is_unavailable() {
        let message = "insert failed and the batch was spooled to poison: x";
        let res = unary(grpc_headers(), body_of(&[&[0, 0, 0, 0, 0]]), |_, _| async {
            answer(500, "application/x-protobuf", rpc_status(13, message))
        })
        .await;
        assert_eq!(code_of(&res), Some(14));
        assert_eq!(header_of(&res, "grpc-message"), Some(message));
    }

    #[tokio::test]
    async fn a_frame_error_stays_internal_whatever_the_handler_says() {
        let res = unary(
            grpc_headers(),
            body_of(&[&[0, 0, 0, 0, 3, 1]]),
            |_, body| async move {
                match axum::body::to_bytes(body, usize::MAX).await {
                    Ok(_) => answer(200, "application/x-protobuf", vec![]),
                    Err(e) => answer(
                        500,
                        "application/x-protobuf",
                        rpc_status(13, &e.to_string()),
                    ),
                }
            },
        )
        .await;
        assert_eq!(code_of(&res), Some(13));
        assert_eq!(
            header_of(&res, "grpc-message"),
            Some("truncated gRPC message")
        );
    }

    /// The review's note: the frame error keeps its message, so an extra
    /// message answers differently from a cut-off one.
    #[tokio::test]
    async fn a_second_message_is_named_as_such() {
        let res = unary(
            grpc_headers(),
            body_of(&[&[0, 0, 0, 0, 1, 1], &[0, 0, 0, 0, 1, 2]]),
            |_, body| async move {
                match axum::body::to_bytes(body, usize::MAX).await {
                    Ok(_) => answer(200, "application/x-protobuf", vec![]),
                    Err(e) => answer(
                        500,
                        "application/x-protobuf",
                        rpc_status(13, &e.to_string()),
                    ),
                }
            },
        )
        .await;
        assert_eq!(code_of(&res), Some(13));
        assert_eq!(
            header_of(&res, "grpc-message"),
            Some("more than one message in a unary call")
        );
    }

    #[tokio::test]
    async fn unframe_joins_a_message_split_across_frames() {
        let unframed = unframe(body_of(&[&[0, 0, 0], &[0, 3, 1], &[2, 3]])).await;
        let (compressed, body, _) = unframed.expect("a whole message");
        assert!(!compressed);
        let bytes = axum::body::to_bytes(body, usize::MAX)
            .await
            .map(|b| b.to_vec());
        assert_eq!(bytes.ok(), Some(vec![1, 2, 3]));
    }

    #[tokio::test]
    async fn unframe_refuses_short_long_and_bad_flag() {
        let read = |frames: &'static [&'static [u8]]| async move {
            let (_, body, _) = unframe(body_of(frames)).await.expect("a prefix");
            axum::body::to_bytes(body, usize::MAX)
                .await
                .map_err(|e| e.to_string())
        };
        assert_eq!(
            read(&[&[0, 0, 0, 0, 3, 1, 2]]).await.err().as_deref(),
            Some("truncated gRPC message")
        );
        assert_eq!(
            read(&[&[0, 0, 0, 0, 1, 1, 2]]).await.err().as_deref(),
            Some("more than one message in a unary call")
        );
        assert!(unframe(body_of(&[&[2, 0, 0, 0, 0]])).await.is_err());
    }

    #[tokio::test]
    async fn gzip_flag_sets_content_encoding_for_the_handler() {
        for (flag, encoding) in [(1u8, Some("gzip")), (0u8, None)] {
            let seen = Arc::new(Mutex::new(None::<HeaderMap>));
            let mut headers = grpc_headers();
            headers.insert("grpc-encoding", "gzip".parse().unwrap());
            headers.insert(header::CONTENT_LENGTH, "5".parse().unwrap());
            headers.insert(header::CONTENT_ENCODING, "br".parse().unwrap());
            let frame: &'static [u8] = if flag == 1 {
                &[1, 0, 0, 0, 0]
            } else {
                &[0, 0, 0, 0, 0]
            };
            let record = Arc::clone(&seen);
            let _ = unary(headers, body_of(&[frame]), |h, _| async move {
                *record.lock().unwrap() = Some(h);
                answer(200, "application/x-protobuf", vec![])
            })
            .await;
            let seen = seen.lock().unwrap().take().expect("the handler ran");
            let get = |name| seen.get(name).and_then(|v| v.to_str().ok());
            assert_eq!(get(header::CONTENT_ENCODING), encoding, "flag {flag}");
            assert_eq!(
                get(header::CONTENT_TYPE),
                Some("application/x-protobuf"),
                "flag {flag}"
            );
            assert_eq!(get(header::CONTENT_LENGTH), None, "flag {flag}");
        }
    }

    #[tokio::test]
    async fn success_is_framed_with_status_trailer() {
        let res = unary(grpc_headers(), body_of(&[&[0, 0, 0, 0, 0]]), |_, _| async {
            answer(200, "application/x-protobuf", vec![9, 9])
        })
        .await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            header_of(&res, header::CONTENT_TYPE.as_str()),
            Some("application/grpc")
        );
        assert_eq!(header_of(&res, "grpc-status"), None);
        let collected = res.into_body().collect().await.expect("a body");
        let trailers = collected.trailers().cloned();
        assert_eq!(collected.to_bytes().to_vec(), vec![0, 0, 0, 0, 2, 9, 9]);
        assert_eq!(
            trailers
                .as_ref()
                .and_then(|t| t.get("grpc-status"))
                .and_then(|v| v.to_str().ok()),
            Some("0")
        );
    }

    #[tokio::test]
    async fn plain_text_refusal_becomes_the_message() {
        let res = unary(grpc_headers(), body_of(&[&[0, 0, 0, 0, 0]]), |_, _| async {
            answer(
                413,
                "text/plain; charset=utf-8",
                b"profile payload size exceeds limit of 1.0 kB\n".to_vec(),
            )
        })
        .await;
        assert_eq!(code_of(&res), Some(3));
        assert_eq!(
            header_of(&res, "grpc-message"),
            Some("profile payload size exceeds limit of 1.0 kB")
        );
    }

    #[test]
    fn message_is_percent_encoded() {
        assert_eq!(encode_message("50% é"), "50%25 %C3%A9");
    }
}
