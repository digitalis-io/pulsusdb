//! Result-row shapes for the trace point read, deserialized straight off
//! `ChClient::query_stream` (`pulsus_clickhouse::Row` derive, RowBinary —
//! same convention as [`crate::logql::rows`]).

use pulsus_clickhouse::Row;
use serde::{Deserialize, Serialize};

/// One `trace_spans` row of the §4.2 point read. Field order matches the
/// documented `SELECT trace_id, span_id, parent_id, payload_type, kind,
/// payload` column order exactly — RowBinary decoding is positional.
/// `trace_id`/`parent_id` are read only for that column alignment (the
/// caller already knows the trace id, and the stored payload carries the
/// parent link); assembly consumes `span_id`/`payload_type`/`kind`/`payload`
/// via [`StoredSpan`]. `kind` is the projected-only column (issue #75): the
/// trace-by-ID response renders `kind` from each winner's decoded OTLP
/// payload, never from this column — this projection serves solely as the
/// assembler's `(span_id, kind)` dedup key, so a Zipkin shared span's SERVER
/// and CLIENT sides (identical `(trace_id, span_id)`, different `kind`) are
/// not collapsed into one on retrieval.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct StoredSpanRow {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub parent_id: [u8; 8],
    pub payload_type: i8,
    /// `trace_spans.kind Int8` (`catalog.rs`), already stored by the writer
    /// — a read-path projection, not a schema change.
    pub kind: i8,
    /// The stored per-span payload blob (`String CODEC(ZSTD(3))` column) —
    /// `serde_bytes` routes the `Vec<u8>` through RowBinary's
    /// length-prefixed byte-string encoding, not serde's default
    /// `Array(UInt8)` sequence encoding (same rationale as
    /// `pulsus-write`'s `TraceSpanRow.payload`).
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
}

/// One Phase-1 candidate-generator row (issue #57): a trace id plus its
/// `bound_ts` — the newest leaf-matching span's timestamp, the upper
/// bound on the trace's final public sort key that licenses the engine's
/// threshold termination.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct CandidateRow {
    pub trace_id: [u8; 16],
    pub bound_ts: i64,
}

/// One Phase-2 batch-hydration row — the physical summary columns only
/// (never `payload`; `pulsus-read` stays OTLP-agnostic). Field order
/// matches `search_sql::hydration_sql`'s SELECT list exactly (RowBinary
/// is positional).
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct HydrationRow {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub parent_id: [u8; 8],
    pub service: String,
    pub name: String,
    pub timestamp_ns: i64,
    pub duration_ns: i64,
    pub status_code: i8,
    pub kind: i8,
}

/// One attribute-membership row (`search_sql::membership_sql`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct MembershipRow {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
}

/// One numeric attribute value row (`search_sql::attr_values_sql` with
/// `numeric = true`; `val_num` is `Nullable(Float64)` — `isNotNull` is in
/// the predicate but `any()` keeps the column Nullable).
#[derive(Debug, Clone, Copy, PartialEq, Row, Serialize, Deserialize)]
pub struct NumValueRow {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub v: Option<f64>,
}

/// One string attribute value row (`search_sql::attr_values_sql` with
/// `numeric = false`).
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct StrValueRow {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub v: String,
}

/// One winners' root-hydration row (`search_sql::root_sql`).
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct RootRow {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub parent_id: [u8; 8],
    pub service: String,
    pub name: String,
    pub timestamp_ns: i64,
    pub duration_ns: i64,
}

/// One metrics range-query bucket row (`metrics_sql::metrics_range_sql`,
/// issue #59): `t` is the `toUnixTimestamp64Milli(...)`-pinned `Int64`
/// epoch-milliseconds bucket start (covers pre-1970/post-2106 buckets that
/// a `UInt32` epoch-seconds column would wrap — issue #59 re-audit), `n`
/// the `uniqExact(trace_id, span_id)` replay-deduped span count (`UInt64`
/// — conversions to the wire's `f64` happen explicitly at the encode
/// boundary, plan v2 delta 5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct MetricBucketRow {
    /// Renamed to the SQL alias `t` for the driver's column-name check.
    #[serde(rename = "t")]
    pub t_ms: i64,
    pub n: u64,
}

/// One metrics instant-query row (`metrics_sql::metrics_instant_sql`):
/// the whole snapped window's deduped count — always exactly one row
/// (aggregate with no `GROUP BY`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct MetricCountRow {
    pub n: u64,
}

/// One service-graph edge row (`graph_sql::service_graph_sql`, issue #173):
/// the aggregated `(client, server, conn_type)` edge with its replay-deduped
/// call `count`, failed-instance count, and TDigest latency quantiles.
/// `quantiles_ns` is the SQL-pinned `Array(Float64)` (`CAST(quantilesTDigest
/// (...) AS Array(Float64))`, issue #173 Fix 2 — no f32 on the decode path),
/// ordered `[p50, p95, p99]`. Field order matches the SELECT list exactly
/// (RowBinary is positional).
#[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
pub struct GraphEdgeRow {
    pub client: String,
    pub server: String,
    pub conn_type: String,
    pub calls: u64,
    pub failed: u64,
    pub quantiles_ns: Vec<f64>,
}

/// One `trace_tag_catalog` row of the §4.3 tag-names read
/// (`tags_sql::tag_names_sql` — `SELECT DISTINCT scope, key`, issue #58).
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct TagNameRow {
    pub scope: String,
    pub key: String,
}

/// One `trace_tag_catalog` row of the §4.3 tag-values read
/// (`tags_sql::tag_values_sql` — `SELECT DISTINCT val`, issue #58).
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct TagValueRow {
    pub val: String,
}

/// What [`super::exec::TraceEngine::fetch_by_id`] hands to callers: the
/// assembly-relevant subset of [`StoredSpanRow`], keeping this crate's
/// public trace-read surface free of the read-alignment-only columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredSpan {
    pub span_id: [u8; 8],
    pub payload_type: i8,
    /// The span's stored `kind` (issue #75) — the assembler's
    /// `(span_id, kind)` dedup key only; never rendered into the response
    /// (that comes from the decoded payload). See [`StoredSpanRow::kind`].
    pub kind: i8,
    pub payload: Vec<u8>,
}

impl From<StoredSpanRow> for StoredSpan {
    fn from(row: StoredSpanRow) -> Self {
        StoredSpan {
            span_id: row.span_id,
            payload_type: row.payload_type,
            kind: row.kind,
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
            kind: 2,
            payload: vec![0xde, 0xad],
        };
        let span = StoredSpan::from(row.clone());
        assert_eq!(span.span_id, row.span_id);
        assert_eq!(span.payload_type, 1);
        assert_eq!(span.kind, 2);
        assert_eq!(span.payload, vec![0xde, 0xad]);
    }
}
