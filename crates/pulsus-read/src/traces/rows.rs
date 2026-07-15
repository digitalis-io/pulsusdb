//! Result-row shapes for the trace point read, deserialized straight off
//! `ChClient::query_stream` (`pulsus_clickhouse::Row` derive, RowBinary —
//! same convention as [`crate::logql::rows`]).

use pulsus_clickhouse::Row;
use serde::{Deserialize, Serialize};

/// One `trace_spans` row of the §4.2 point read. Field order matches the
/// documented `SELECT trace_id, span_id, parent_id, payload_type, payload`
/// column order exactly — RowBinary decoding is positional. `trace_id`/
/// `parent_id` are read only for that column alignment (the caller already
/// knows the trace id, and the stored payload carries the parent link);
/// assembly consumes `span_id`/`payload_type`/`payload` via [`StoredSpan`].
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct StoredSpanRow {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub parent_id: [u8; 8],
    pub payload_type: i8,
    /// The stored per-span payload blob (`String CODEC(ZSTD(3))` column) —
    /// `serde_bytes` routes the `Vec<u8>` through RowBinary's
    /// length-prefixed byte-string encoding, not serde's default
    /// `Array(UInt8)` sequence encoding (same rationale as
    /// `pulsus-write`'s `TraceSpanRow.payload`).
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
}

/// What [`super::exec::TraceEngine::fetch_by_id`] hands to callers: the
/// assembly-relevant subset of [`StoredSpanRow`], keeping this crate's
/// public trace-read surface free of the read-alignment-only columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredSpan {
    pub span_id: [u8; 8],
    pub payload_type: i8,
    pub payload: Vec<u8>,
}

impl From<StoredSpanRow> for StoredSpan {
    fn from(row: StoredSpanRow) -> Self {
        StoredSpan {
            span_id: row.span_id,
            payload_type: row.payload_type,
            payload: row.payload,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_span_row_converts_to_the_public_stored_span_shape() {
        let row = StoredSpanRow {
            trace_id: [1; 16],
            span_id: [2; 8],
            parent_id: [3; 8],
            payload_type: 1,
            payload: vec![0xde, 0xad],
        };
        let span = StoredSpan::from(row.clone());
        assert_eq!(span.span_id, row.span_id);
        assert_eq!(span.payload_type, 1);
        assert_eq!(span.payload, vec![0xde, 0xad]);
    }
}
