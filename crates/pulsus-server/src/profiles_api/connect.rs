//! Stub.

use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

/// A request body's codec.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Codec {
    Proto,
    Json,
}

/// The Connect error codes this surface answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Code {
    InvalidArgument,
    ResourceExhausted,
    Unimplemented,
    Internal,
    Unavailable,
    DeadlineExceeded,
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

impl IntoResponse for ConnectError {
    fn into_response(self) -> Response {
        StatusCode::OK.into_response()
    }
}

pub(crate) fn request_codec(_headers: &HeaderMap) -> Result<Codec, Response> {
    Err(StatusCode::OK.into_response())
}

pub(crate) fn request_bytes(_headers: &HeaderMap, raw: &[u8]) -> Result<Vec<u8>, ConnectError> {
    Ok(raw.to_vec())
}

pub(crate) fn decode<M: Default>(
    _codec: Codec,
    _bytes: &[u8],
    _full_name: &str,
) -> Result<M, ConnectError> {
    Ok(M::default())
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
            let res = request_codec(&h).expect_err("refused");
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
