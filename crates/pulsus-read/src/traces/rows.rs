//! Result-row shapes for the trace SEARCH path, deserialized straight off
//! `ChClient::query_stream` (`pulsus_clickhouse::Row` derive, RowBinary —
//! same convention as [`crate::logql::rows`]).
//!
//! **The trace FETCH's rows are not here** (issue #587): they are
//! [`crate::traces::spans::rows`]'s, because the fetch reads three
//! different tables through three different statements. Every `kind` and
//! `status_code` in this file is `i8` and must stay so — they decode the
//! old span table, whose columns are `Int8`, and they move when #602
//! moves search off it.

use pulsus_clickhouse::Row;
use serde::{Deserialize, Serialize};

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

/// One `(t, bucket, count)` log2 tally row
/// (`metrics_log2_bucket_range_sql`, issue #252): the bucket start, the
/// occupied power-of-two nanosecond bucket, and its plain `count()`.
/// One row per OCCUPIED `(t, bucket)` — never a fixed-width array, never
/// cumulative.
///
/// `bucket_ns` is `UInt64` because the SQL expression is
/// `toUInt64(roundToExp2(val - 1)) * 2`: a duration above `2^62` buckets
/// to `2^63`, which is not representable as `Int64` (it would decode as
/// a negative `__bucket` label).
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct MetricLog2BucketRow {
    #[serde(rename = "t")]
    pub t_ms: i64,
    #[serde(rename = "bucket")]
    pub bucket_ns: u64,
    pub n: u64,
}

/// One `histogram_over_time` instant-form tally row — the same
/// `(bucket, count)` over the whole snapped window, no time bucket.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct MetricLog2BucketInstantRow {
    #[serde(rename = "bucket")]
    pub bucket_ns: u64,
    pub n: u64,
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

/// One per-bucket exemplar-collection row for a GROUPED range shape
/// (`metrics_exemplar_range_sql` with one by-key, issue #477 wave 2): the
/// bucket start, the group label value `g0`, and the bounded
/// `groupArraySample` of `(trace_id, timestamp_ns)` tuples. Field order
/// matches the SELECT list (RowBinary is positional), and `g0` is what
/// lets the engine attach each sample to the series it came from instead
/// of to whichever series happens to be first.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct MetricGroupExemplarRow {
    #[serde(rename = "t")]
    pub t_ms: i64,
    pub g0: String,
    pub ex: Vec<([u8; 16], i64)>,
}

/// One per-bucket exemplar-collection row for `quantile_over_time`
/// (`metrics_quantile_exemplar_range_sql`, issue #477 wave 2): the bucket
/// start and a bounded `groupArraySample` of `(trace_id, timestamp_ns,
/// duration_ns)` triples. The duration is what says which `p=` series the
/// sample belongs to, and it is also the exemplar's own value.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct MetricQuantileExemplarRow {
    #[serde(rename = "t")]
    pub t_ms: i64,
    pub ex: Vec<([u8; 16], i64, i64)>,
}

/// One per-bucket exemplar-collection row for `histogram_over_time`
/// (`metrics_log2_bucket_exemplar_range_sql`, issue #477 wave 2): the
/// bucket start, the log2 duration bucket bound that names the series,
/// and the bounded `groupArraySample` of `(trace_id, timestamp_ns)`
/// tuples. Field order matches the SELECT list (RowBinary is positional).
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct MetricLog2ExemplarRow {
    #[serde(rename = "t")]
    pub t_ms: i64,
    #[serde(rename = "bucket")]
    pub bucket_ns: u64,
    pub ex: Vec<([u8; 16], i64)>,
}

/// One per-bucket exemplar-collection row for `compare()`
/// (`metrics_compare_exemplar_range_sql`, issue #477 wave 2): the bucket
/// start, the selection side (`0` baseline, `1` selection), the attribute
/// key whose `<side>_total` series the sample belongs to, and the bounded
/// `groupArraySample` of `(trace_id, timestamp_ns)` tuples.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct MetricCompareExemplarRow {
    #[serde(rename = "t")]
    pub t_ms: i64,
    pub is_sel: u8,
    pub akey: String,
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

/// One `trace_spans` row of the §4.3 span-NAME values read
/// (`tags_sql::span_name_values_sql` — `SELECT DISTINCT <capped name> AS
/// val`, issue #478). One column: the type is not read from the store,
/// because a span name is a `String` column and is always reported
/// `string`.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct SpanNameRow {
    pub val: String,
}

/// One `trace_tag_catalog` row of the §4.3 tag-values read
/// (`tags_sql::tag_values_sql` — `SELECT DISTINCT val, val_type`, issues
/// #58 and #476). Field order matches the SELECT list exactly (RowBinary
/// is positional).
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct TagValueRow {
    pub val: String,
    /// The stored OTLP type (`string`/`int`/`float`/`bool`), or the EMPTY
    /// string for a row written before issue #476's migration 41 added the
    /// column. The renderer reports an empty one as `string`.
    pub val_type: String,
}
