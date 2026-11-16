//! Issue #821: the Connect protocol's unary calls, as the profiles query
//! API serves them — the request codec by `Content-Type`, the body's
//! `Content-Encoding`, the response in the request's codec, and the error
//! body `{"code","message"}` with each code's status.
//!
//! `Connect-Protocol-Version` and `Connect-Timeout-Ms` are read by nothing.

use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use pulsus_read::profiles::ProfilesReadError;
use pulsus_write::ingest::decompress::{Encoding, MAX_DECOMPRESSED_BYTES, decompress};

/// The reference's deadline message (`pkg/util/http/error.go:25`), chosen
/// for a deadline at `:80-82` and answered as `deadline_exceeded` by
/// `pkg/util/connectgrpc/connectgrpc.go:297-298`.
pub(crate) const DEADLINE_MESSAGE: &str = "request timed out, decrease the duration of the request or add more label matchers (prefer exact match over regex match) to reduce the amount of data processed";

/// A request body's codec.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Codec {
    Proto,
    Json,
}

/// The Connect error codes, the wire crate's table (issue #817).
pub(crate) use pulsus_profiles_wire::connect::Code;
use pulsus_profiles_wire::connect::error_json;

/// `code`'s HTTP status.
fn status_of(code: Code) -> StatusCode {
    StatusCode::from_u16(code.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
}

/// A Connect error: its code and message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConnectError {
    pub(crate) code: Code,
    pub(crate) message: String,
}

impl ConnectError {
    pub(crate) fn new(code: Code, message: impl Into<String>) -> Self {
        ConnectError {
            code,
            message: message.into(),
        }
    }
}

impl From<ProfilesReadError> for ConnectError {
    fn from(e: ProfilesReadError) -> Self {
        match e {
            ProfilesReadError::InvalidArgument(m) => ConnectError::new(Code::InvalidArgument, m),
            ProfilesReadError::TooBroad(m) => ConnectError::new(Code::ResourceExhausted, m),
            ProfilesReadError::DeadlineExceeded => {
                ConnectError::new(Code::DeadlineExceeded, DEADLINE_MESSAGE)
            }
            ProfilesReadError::Unavailable(m) => ConnectError::new(Code::Unavailable, m),
            ProfilesReadError::Internal(m) => ConnectError::new(Code::Internal, m),
        }
    }
}

impl IntoResponse for ConnectError {
    fn into_response(self) -> Response {
        (
            status_of(self.code),
            [(header::CONTENT_TYPE, "application/json")],
            error_json(self.code, &self.message),
        )
            .into_response()
    }
}

/// The answer to a request deadline on the profiles Connect paths (D15).
pub(crate) fn deadline_response() -> Response {
    ConnectError::new(Code::DeadlineExceeded, DEADLINE_MESSAGE).into_response()
}

/// The answer to a request deadline on the profiles `GET` routes: the
/// reference's `ErrorWithStatus` (`pkg/util/http/error.go:44-58`) — the
/// same status and body as [`deadline_response`], `nosniff`, and the
/// `json.Encoder`'s trailing newline.
pub(crate) fn http_deadline_response() -> Response {
    let body = error_json(Code::DeadlineExceeded, DEADLINE_MESSAGE);
    (
        status_of(Code::DeadlineExceeded),
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        format!("{body}\n"),
    )
        .into_response()
}

/// A request `Content-Type` this surface does not read: `415`, an empty
/// body and `Accept-Post`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UnsupportedMedia;

impl IntoResponse for UnsupportedMedia {
    fn into_response(self) -> Response {
        let mut res = Response::new(Body::empty());
        *res.status_mut() = StatusCode::UNSUPPORTED_MEDIA_TYPE;
        res.headers_mut().insert(
            "accept-post",
            HeaderValue::from_static("application/json, application/proto"),
        );
        res
    }
}

/// The codec `Content-Type` names: `application/proto`, or `application/json`
/// with or without parameters. Anything else, or none, is
/// [`UnsupportedMedia`].
pub(crate) fn request_codec(headers: &HeaderMap) -> Result<Codec, UnsupportedMedia> {
    let media = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|v| {
            v.split(';')
                .next()
                .unwrap_or("")
                .trim()
                .to_ascii_lowercase()
        });
    match media.as_deref() {
        Some("application/proto") => Ok(Codec::Proto),
        Some("application/json") => Ok(Codec::Json),
        _ => Err(UnsupportedMedia),
    }
}

/// The request body after its `Content-Encoding`: absent or `identity` as
/// is, `gzip` through the ingest decompressor (64 MiB cap), anything else
/// `unimplemented`.
pub(crate) fn request_bytes(headers: &HeaderMap, raw: &[u8]) -> Result<Vec<u8>, ConnectError> {
    let encoding = headers
        .get(header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .unwrap_or("");
    if encoding.is_empty() || encoding.eq_ignore_ascii_case("identity") {
        return Ok(raw.to_vec());
    }
    if encoding.eq_ignore_ascii_case("gzip") {
        return decompress(Encoding::Gzip, raw)
            .map_err(|e| ConnectError::new(Code::InvalidArgument, e.to_string()));
    }
    Err(ConnectError::new(
        Code::Unimplemented,
        format!("unknown compression {encoding:?}: supported encodings are gzip"),
    ))
}

/// Reads a request body, at most the decompressor's cap.
pub(crate) async fn read_body(headers: &HeaderMap, body: Body) -> Result<Vec<u8>, ConnectError> {
    let raw = axum::body::to_bytes(body, MAX_DECOMPRESSED_BYTES)
        .await
        .map_err(|e| ConnectError::new(Code::InvalidArgument, e.to_string()))?;
    request_bytes(headers, &raw)
}

/// Decodes a request message in `codec`; a bad body is `invalid_argument`,
/// `unmarshal into <full_name>: <decoder text>`.
pub(crate) fn decode<M>(codec: Codec, bytes: &[u8], full_name: &str) -> Result<M, ConnectError>
where
    M: prost::Message + Default + serde::de::DeserializeOwned,
{
    let decoded = match codec {
        Codec::Proto => M::decode(bytes).map_err(|e| e.to_string()),
        Codec::Json => serde_json::from_slice(bytes).map_err(|e| e.to_string()),
    };
    decoded.map_err(|e| {
        ConnectError::new(
            Code::InvalidArgument,
            format!("unmarshal into {full_name}: {e}"),
        )
    })
}

/// A `200` carrying `msg` in `codec`.
pub(crate) fn respond<M>(codec: Codec, msg: &M) -> Response
where
    M: prost::Message + serde::Serialize,
{
    match codec {
        Codec::Proto => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/proto")],
            msg.encode_to_vec(),
        )
            .into_response(),
        Codec::Json => match serde_json::to_vec(msg) {
            Ok(body) => (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                body,
            )
                .into_response(),
            Err(e) => ConnectError::new(Code::Internal, e.to_string()).into_response(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use pulsus_profiles_wire::types_v1::LabelValuesRequest;

    fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.append(*k, HeaderValue::from_str(v).expect("a header value"));
        }
        h
    }

    async fn body_of(res: Response) -> Vec<u8> {
        axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .expect("body")
            .to_vec()
    }

    /// C1: the request codec by `Content-Type`; anything else is `415`
    /// with `Accept-Post`.
    #[tokio::test]
    async fn c1_the_content_type_picks_the_codec() {
        assert_eq!(
            request_codec(&headers(&[("content-type", "application/proto")])).ok(),
            Some(Codec::Proto)
        );
        for ct in ["application/json", "application/json; charset=utf-8"] {
            assert_eq!(
                request_codec(&headers(&[("content-type", ct)])).ok(),
                Some(Codec::Json),
                "{ct}"
            );
        }
        for h in [headers(&[("content-type", "text/plain")]), HeaderMap::new()] {
            let res = request_codec(&h).expect_err("refused").into_response();
            assert_eq!(res.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE, "{h:?}");
            assert_eq!(
                res.headers()
                    .get("accept-post")
                    .and_then(|v| v.to_str().ok()),
                Some("application/json, application/proto"),
                "{h:?}"
            );
            assert!(body_of(res).await.is_empty(), "{h:?}");
        }
    }

    /// C2: each code's status, and the JSON error body with its message
    /// escaped.
    #[tokio::test]
    async fn c2_each_code_answers_its_status_and_the_json_body() {
        for (code, status, name) in [
            (Code::InvalidArgument, 400, "invalid_argument"),
            (Code::ResourceExhausted, 429, "resource_exhausted"),
            (Code::Unimplemented, 501, "unimplemented"),
            (Code::Internal, 500, "internal"),
            (Code::Unavailable, 503, "unavailable"),
            (Code::DeadlineExceeded, 504, "deadline_exceeded"),
        ] {
            let res = ConnectError::new(code, "a\"b").into_response();
            assert_eq!(res.status().as_u16(), status, "{name}");
            assert_eq!(
                res.headers()
                    .get("content-type")
                    .and_then(|v| v.to_str().ok()),
                Some("application/json"),
                "{name}"
            );
            assert_eq!(
                String::from_utf8(body_of(res).await).expect("utf-8"),
                format!(r#"{{"code":"{name}","message":"a\"b"}}"#)
            );
        }
    }

    /// The error body escapes `<`, `>` and `&` as the reference's JSON
    /// marshalling does, through the wire crate's one writer (issue #817).
    #[tokio::test]
    async fn the_error_body_escapes_angle_brackets_and_ampersands() {
        let res = ConnectError::new(Code::InvalidArgument, "{a=~\"<x>&y\"}").into_response();
        assert_eq!(
            String::from_utf8(body_of(res).await).expect("utf-8"),
            r#"{"code":"invalid_argument","message":"{a=~\"\u003cx\u003e\u0026y\"}"}"#
        );
    }

    /// C3: a gzip body is read through the ingest decompressor; another
    /// encoding is `unimplemented`.
    #[test]
    fn c3_a_gzip_body_is_read_and_another_encoding_refused() {
        use std::io::Write;
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(br#"{"name":"env"}"#).expect("gzip");
        let raw = gz.finish().expect("gzip");
        let h = headers(&[("content-encoding", "gzip")]);
        let bytes = request_bytes(&h, &raw).expect("read");
        let msg: LabelValuesRequest =
            decode(Codec::Json, &bytes, "types.v1.LabelValuesRequest").expect("decode");
        assert_eq!(msg.name, "env");
        let err =
            request_bytes(&headers(&[("content-encoding", "br")]), b"x").expect_err("refused");
        assert_eq!(err.code, Code::Unimplemented);
    }
}
