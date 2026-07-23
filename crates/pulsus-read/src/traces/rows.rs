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
    /// The OTLP `Status.message` (issue #184's `statusMessage` intrinsic),
    /// byte-capped like `service`/`name`. Positioned between `status_code`
    /// and `kind` — in lockstep with `search_sql::hydration_sql`'s SELECT
    /// list (RowBinary is positional).
    pub status_message: String,
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

/// One trace-level context co-load row (`search_sql::trace_ctx_sql`,
/// issue #184): the trace-wide time envelope plus the root span's
/// byte-capped name/service, selected by the `pick_roots`-equivalent
/// `argMin` ordering. Field order matches the SELECT list (RowBinary is
/// positional).
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct TraceCtxRow {
    pub trace_id: [u8; 16],
    pub trace_start_ns: i64,
    pub trace_end_ns: i64,
    pub root_name: String,
    pub root_service: String,
}

/// One direct-child-count co-load row (`search_sql::child_count_sql`,
/// issue #184): the number of distinct direct children of `parent_id`
/// within its trace (trace-wide, replay-deduped).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct ChildCountRow {
    pub trace_id: [u8; 16],
    pub parent_id: [u8; 8],
    pub child_count: u64,
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
/// (aggregate with no `GROUP BY`). Also the shape of the by()-series
/// distinct-by-key probe (`metrics_series_probe_sql`, issue #182): the
/// probe's `count()` of distinct label-sets, capped at `cap+1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct MetricCountRow {
    pub n: u64,
}

/// One grouped metrics count range-query row (`metrics_count_range_sql`
/// with one by-key, issue #182): the bucket start `t`, the single group
/// label value `g0`, and the replay-deduped `uniqExact` count. Field order
/// matches the SELECT list (RowBinary is positional).
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct MetricGroupCountRow {
    #[serde(rename = "t")]
    pub t_ms: i64,
    pub g0: String,
    pub n: u64,
}

/// One grouped metrics count instant-query row
/// (`metrics_count_instant_sql` with one by-key): the group label value
/// and its deduped count, one row per label-set.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct MetricGroupCountInstantRow {
    pub g0: String,
    pub n: u64,
}

/// One ungrouped value-aggregation range-query row (`metrics_agg_range_sql`
/// with no by-keys, issue #182): the bucket start `t` and the
/// `toFloat64`-cast aggregate value `v`.
#[derive(Debug, Clone, Copy, PartialEq, Row, Serialize, Deserialize)]
pub struct MetricAggRow {
    #[serde(rename = "t")]
    pub t_ms: i64,
    pub v: f64,
}

/// One ungrouped value-aggregation instant-query row.
#[derive(Debug, Clone, Copy, PartialEq, Row, Serialize, Deserialize)]
pub struct MetricAggInstantRow {
    pub v: f64,
}

/// One grouped value-aggregation range-query row (one by-key): bucket
/// start, group label value, aggregate value.
#[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
pub struct MetricAggGroupRow {
    #[serde(rename = "t")]
    pub t_ms: i64,
    pub g0: String,
    pub v: f64,
}

/// One grouped value-aggregation instant-query row (one by-key).
#[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
pub struct MetricAggGroupInstantRow {
    pub g0: String,
    pub v: f64,
}

/// One `quantile_over_time` range-query row (`metrics_quantile_range_sql`,
/// issue #182): the bucket start and the `quantilesTDigest` result array
/// (`[q0, q1, …]`, one per requested quantile, in ns — scaled ns→seconds
/// at the encode boundary). `qs` is the SQL-pinned `Array(Float64)`.
#[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
pub struct MetricQuantileRow {
    #[serde(rename = "t")]
    pub t_ms: i64,
    pub qs: Vec<f64>,
}

/// One `quantile_over_time` instant-query row — the whole-window TDigest
/// array (no time bucket).
#[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
pub struct MetricQuantileInstantRow {
    pub qs: Vec<f64>,
}

/// One `histogram_over_time` range-query row (`metrics_histogram_range_sql`,
/// issue #182): the bucket start and the cumulative per-`le`-bucket count
/// array (`Array(UInt64)`, one entry per exponential boundary).
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct MetricHistogramRow {
    #[serde(rename = "t")]
    pub t_ms: i64,
    pub bkts: Vec<u64>,
}

/// One `histogram_over_time` instant-query row — the whole-window
/// cumulative bucket-count array.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct MetricHistogramInstantRow {
    pub bkts: Vec<u64>,
}

/// One per-bucket exemplar-collection row (`metrics_exemplar_range_sql`,
/// issue #182 P5): the bucket start and a bounded `groupArraySample` of
/// `(trace_id, timestamp_ns)` tuples (`Array(Tuple(FixedString(16),
/// Int64))`).
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct MetricExemplarRow {
    #[serde(rename = "t")]
    pub t_ms: i64,
    pub ex: Vec<([u8; 16], i64)>,
}

/// One `compare()` cross-tab row (`metrics_compare_sql`, issue #182 P6b):
/// the bucket start, an attribute `(key, value)`, and its baseline (all
/// outer spans) and selection (`countIf(is_sel)`) counts.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct CompareCrossTabRow {
    #[serde(rename = "t")]
    pub t_ms: i64,
    pub akey: String,
    pub aval: String,
    pub base_n: u64,
    pub sel_n: u64,
}

/// One `compare()` per-bucket totals row: the baseline/selection span
/// totals (the `*_total` denominators and the `key=nil` complement base).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct CompareTotalsRow {
    #[serde(rename = "t")]
    pub t_ms: i64,
    pub base_total: u64,
    pub sel_total: u64,
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
