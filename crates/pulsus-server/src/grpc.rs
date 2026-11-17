//! OTLP over gRPC (issue #816, docs/api.md §1.1): a unary shim in front of
//! the four OTLP/HTTP export handlers.

use std::future::Future;
use std::sync::{Arc, OnceLock};

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;

use crate::app::AppState;

/// `LogsService/Export`: the `/v1/logs` handler behind the shim.
pub(crate) async fn export_logs(State(s): State<AppState>, h: HeaderMap, b: Body) -> Response {
    unary(h, b, |h, b| crate::ingest::ingest_logs(State(s), h, b)).await
}

/// The frame error the message stream met, if any.
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

pub(crate) fn grpc_code(_status: StatusCode) -> i32 {
    2
}

async fn unframe(body: Body) -> Result<(bool, Body, FrameError), String> {
    Ok((false, body, FrameError::default()))
}

async fn unary<F, Fut>(headers: HeaderMap, body: Body, handler: F) -> Response
where
    F: FnOnce(HeaderMap, Body) -> Fut,
    Fut: Future<Output = Response>,
{
    handler(headers, body).await
}

pub(crate) fn encode_message(message: &str) -> String {
    message.to_string()
}

pub(crate) fn deadline_response() -> Response {
    Response::default()
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
