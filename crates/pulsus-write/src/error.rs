//! `LogsIngestError` taxonomy for the OTLP logs receiver (issue #8).
//! Every variant maps to exactly one whole-request `(HTTP status,
//! google.rpc.Status.code)` pair (architect plan amendment 2,
//! `src/ingest/http.rs`) — malformed/decompression/oversize classes to
//! HTTP 400 / `code = 3` (`INVALID_ARGUMENT`), everything else here
//! (channel/body-read failures not attributable to the payload) to HTTP
//! 500 / `code = 13` (`INTERNAL`). Sink backpressure (HTTP 429 / `code =
//! 8`, `RESOURCE_EXHAUSTED`) is deliberately not a variant of this enum —
//! it is carried by [`crate::ingest::Backpressure`], a distinct type,
//! because it originates from the sink seam, not from parsing the request.

use thiserror::Error;

/// Errors from decompressing/decoding an OTLP `/v1/logs` request body.
/// Follows the `pulsus-schema::SchemaError` style: one variant per
/// distinguishable failure, each carrying enough context for an actionable
/// error message.
#[derive(Debug, Error)]
pub enum LogsIngestError {
    /// The request body could not be read off the connection (e.g. the
    /// client disconnected mid-upload). Not a malformed-payload class
    /// error — the payload's shape was never established — so it maps to
    /// `INTERNAL` (500), not `INVALID_ARGUMENT` (400).
    #[error("failed to read request body: {0}")]
    BodyRead(String),

    /// `Content-Encoding` names a scheme this receiver does not support.
    #[error("unsupported Content-Encoding {0:?}: expected identity, gzip, zstd, or snappy")]
    UnsupportedEncoding(String),

    /// Decompressing the body failed (gzip/zstd/snappy stream corrupt or
    /// truncated). `reason` is a message, not a boxed `std::error::Error`
    /// (deliberately not named `source` — `thiserror` treats a field named
    /// `source` as the error's `Error::source()`, which requires it to
    /// implement `std::error::Error` itself).
    #[error("failed to decompress {encoding} request body: {reason}")]
    Decompress {
        encoding: &'static str,
        reason: String,
    },

    /// The decompressed body exceeds the documented zip-bomb guard
    /// (`crate::ingest::decompress::MAX_DECOMPRESSED_BYTES`, architect plan
    /// amendment 2).
    #[error(
        "decompressed request body exceeds the {limit}-byte cap (zip-bomb guard, \
         docs/architecture.md §4)"
    )]
    OversizeBody { limit: usize },

    /// The (decompressed) request body is not a valid
    /// `ExportLogsServiceRequest` protobuf message. Whole-request atomic
    /// failure — never partially applied (architect plan amendment).
    #[error("malformed ExportLogsServiceRequest protobuf: {0}")]
    Decode(#[from] prost::DecodeError),

    /// The (decompressed) request body was sent with `Content-Type:
    /// application/json` but is not a valid OTLP/JSON (proto3-JSON) encoding
    /// of the signal's `Export*ServiceRequest` — malformed JSON, a wrong
    /// field shape, or a value form this receiver cannot resolve (e.g. an
    /// unknown enum name; both the integer and the proto3-JSON string-name
    /// enum forms are accepted — issues #76, #98).
    /// Same whole-request 400-class as [`Self::Decode`]: OTLP/JSON is a second
    /// wire encoding of the identical message, so a decode failure is the same
    /// structural error, never a partial apply.
    #[error("malformed OTLP/JSON request body: {0}")]
    DecodeJson(#[from] serde_json::Error),

    /// A decoded message's repeated-field count exceeds a documented
    /// structural bound (issue #28 code review hardening finding): a
    /// decode-time DoS guard against a body that decodes successfully
    /// (within the 64 MiB decompressed-size cap) but unpacks into a far
    /// larger in-memory structure via many minimal-length repeated
    /// submessages — e.g. millions of near-empty `TimeSeries`/`Label`
    /// entries, each only a few wire bytes but tens of heap-adjacent bytes
    /// once decoded. `field` names the exceeded repeated field, `limit`/
    /// `actual` make the whole-request `400` actionable. Structural class
    /// (same as [`Self::OversizeBody`]) — checked immediately after decode,
    /// before any further per-element processing.
    #[error("{field} count {actual} exceeds the documented limit of {limit}")]
    OversizeMessage {
        field: &'static str,
        limit: usize,
        actual: usize,
    },

    /// A Loki push body (issue #77) was structurally decodable but
    /// semantically malformed: bad JSON, a malformed Prometheus label-set
    /// literal in a `StreamAdapter.labels` string, or a non-numeric/
    /// overflowing entry timestamp. A whole-request 400-class failure (Loki
    /// has no partial-success channel) — same `(400, code = 3)` group as the
    /// structural/decode variants above, added to [`classify`]'s 400 arm
    /// (`ingest/http.rs`).
    ///
    /// [`classify`]: crate::ingest::http
    #[error("malformed Loki push request: {0}")]
    LokiDecode(String),

    /// A Zipkin v2 JSON span array (issue #75, `POST /api/v2/spans`) was not
    /// decodable into the span model: malformed JSON, or a span carrying a
    /// non-hex / wrong-length `traceId`/`id`/`parentId` or an
    /// unrepresentable (i64-overflowing) microsecond timestamp. A
    /// whole-request 400-class failure (Zipkin has no partial-success
    /// channel — the whole span list is accepted or rejected) — same
    /// `(400, code = 3)` group as the structural/decode variants above,
    /// added to [`classify`]'s 400 arm (`ingest/http.rs`).
    ///
    /// [`classify`]: crate::ingest::http
    #[error("malformed Zipkin v2 request: {0}")]
    ZipkinDecode(String),

    /// A sync-mode ([`crate::ingest::LogSink::admit_flush`]) request's
    /// completion future resolved with an error the writer core did not
    /// classify further (e.g. the writer shut down mid-flush without
    /// confirming). Not attributable to the request payload, so this maps
    /// to `INTERNAL` (500).
    #[error("flush did not complete: {0}")]
    FlushFailed(String),
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;

    #[test]
    fn unsupported_encoding_message_names_the_value() {
        let err = LogsIngestError::UnsupportedEncoding("br".to_string());
        assert!(err.to_string().contains("br"));
    }

    #[test]
    fn decompress_message_names_the_encoding_and_reason() {
        let err = LogsIngestError::Decompress {
            encoding: "gzip",
            reason: "unexpected eof".to_string(),
        };
        let message = err.to_string();
        assert!(message.contains("gzip"));
        assert!(message.contains("unexpected eof"));
    }

    #[test]
    fn oversize_body_message_names_the_limit() {
        let err = LogsIngestError::OversizeBody {
            limit: 64 * 1024 * 1024,
        };
        assert!(err.to_string().contains("67108864"));
    }

    #[test]
    fn oversize_message_names_the_field_and_both_counts() {
        let err = LogsIngestError::OversizeMessage {
            field: "timeseries",
            limit: 1_000_000,
            actual: 1_000_001,
        };
        let message = err.to_string();
        assert!(message.contains("timeseries"));
        assert!(message.contains("1000000"));
        assert!(message.contains("1000001"));
    }

    #[test]
    fn decode_error_converts_via_from() {
        // A minimal real `prost::Message` impl, decoded from garbage bytes,
        // to obtain a genuine `DecodeError` rather than
        // `prost::DecodeError::new` (an internal-use-only, deprecated
        // constructor).
        #[derive(Clone, PartialEq, ::prost::Message)]
        struct Dummy {
            #[prost(string, tag = "1")]
            field: String,
        }
        let decode_err = Dummy::decode(&b"\xFF\xFF\xFF"[..]).unwrap_err();
        let err: LogsIngestError = decode_err.into();
        assert!(matches!(err, LogsIngestError::Decode(_)));
    }

    #[test]
    fn body_read_message_names_the_source() {
        let err = LogsIngestError::BodyRead("connection reset".to_string());
        assert!(err.to_string().contains("connection reset"));
    }

    #[test]
    fn flush_failed_message_names_the_source() {
        let err = LogsIngestError::FlushFailed("writer shut down".to_string());
        assert!(err.to_string().contains("writer shut down"));
    }
}
