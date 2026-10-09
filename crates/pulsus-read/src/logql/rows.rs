//! Result-row shapes for each stage/query kind, deserialized straight off
//! `ChClient::query_stream` (`pulsus_clickhouse::Row` derive, matching the
//! crate's RowBinary convention).

use pulsus_clickhouse::Row;
use pulsus_model::Fingerprint;
use serde::{Deserialize, Serialize};

/// Stage 1 — stream resolution (`log_streams_idx`): one fingerprint per
/// matching stream.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct StreamRow {
    pub fingerprint: Fingerprint,
}

/// Stage 2 — hydration (`log_streams`): response labels plus the `service`
/// set stage 3 needs. Reads without `FINAL` may return pre-merge duplicate
/// rows per fingerprint (`ReplacingMergeTree`); the engine dedups by
/// `fingerprint` (labels/service are identical per fingerprint, so keeping
/// any one row is safe — docs/schemas.md §3.2 edge cases).
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct StreamMetaRow {
    pub fingerprint: Fingerprint,
    pub service: String,
    /// Canonical JSON, sorted keys (docs/schemas.md §3.1).
    pub labels: String,
}

/// Stage 3 — samples (`log_samples`): one matching log line.
/// `structured_metadata` is the per-entry canonical JSON String (issue #97),
/// the LAST projected column (append-only, aligning with the additive
/// `ADD COLUMN` migration). Empty string = no structured metadata (also what
/// pre-#97 rows read back via the column's `DEFAULT ''`).
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct SampleRow {
    pub fingerprint: Fingerprint,
    pub timestamp_ns: i64,
    pub body: String,
    pub structured_metadata: String,
    /// The captures of each `regexp` stage the database runs (issue #624,
    /// part 3a): one element per pattern, `extractGroups(body, <p>)`, empty
    /// when the line does not match. Never a column of this type — it is
    /// decoded by the twin that names it, and empty when the statement sent
    /// no such column.
    #[serde(skip)]
    pub rx: Vec<Vec<String>>,
}

/// [`SampleRow`] with the `rx` column (issue #624, part 3a), decoded when
/// the statement sends it.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct SampleRxRow {
    pub fingerprint: Fingerprint,
    pub timestamp_ns: i64,
    pub body: String,
    pub structured_metadata: String,
    pub rx: Vec<Vec<String>>,
}

impl From<SampleRxRow> for SampleRow {
    fn from(r: SampleRxRow) -> Self {
        SampleRow {
            fingerprint: r.fingerprint,
            timestamp_ns: r.timestamp_ns,
            body: r.body,
            structured_metadata: r.structured_metadata,
            rx: r.rx,
        }
    }
}

/// A live-tail keyset page row (issue #74): stage 3's sample columns plus
/// the ClickHouse-computed `cityHash64(body)` the composite cursor is
/// keyed on (projected server-side so the cursor can never diverge from
/// the SQL predicate's own hash). `structured_metadata` (issue #97) is the
/// per-entry JSON String; the cursor keys on `(timestamp_ns, fingerprint,
/// body_hash)` only — structured metadata never enters it.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct TailSampleRow {
    pub fingerprint: Fingerprint,
    pub timestamp_ns: i64,
    pub body: String,
    pub body_hash: u64,
    pub structured_metadata: String,
    /// The captures of each `regexp` stage the database runs (issue #624,
    /// part 3a): one element per pattern, `extractGroups(body, <p>)`, empty
    /// when the line does not match. Never a column of this type — it is
    /// decoded by the twin that names it, and empty when the statement sent
    /// no such column.
    #[serde(skip)]
    pub rx: Vec<Vec<String>>,
}

/// [`TailSampleRow`] with the `rx` column (issue #624, part 3a).
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct TailSampleRxRow {
    pub fingerprint: Fingerprint,
    pub timestamp_ns: i64,
    pub body: String,
    pub body_hash: u64,
    pub structured_metadata: String,
    pub rx: Vec<Vec<String>>,
}

impl From<TailSampleRxRow> for TailSampleRow {
    fn from(r: TailSampleRxRow) -> Self {
        TailSampleRow {
            fingerprint: r.fingerprint,
            timestamp_ns: r.timestamp_ns,
            body: r.body,
            body_hash: r.body_hash,
            structured_metadata: r.structured_metadata,
            rx: r.rx,
        }
    }
}

/// The client-aggregated LogQL metric raw scan (`metric_raw_samples` /
/// `metric_raw_samples_sliding`): stage 3's columns, `structured_metadata`
/// LAST (append-only, the [`SampleRow`] precedent).
///
/// The metric path MERGES structured metadata into the label set, exactly as
/// the streams path does — both sample extractors add it as their FIRST act,
/// before any pipeline stage runs (`pkg/logql/log/metrics_extraction.go:102-104`
/// and `:202-205 @ v3.7.4`, `builder.Add(StructuredMetadataLabel, …)`), and
/// the `NoopStage` short-circuit at `:104-108` is AFTER the `Add`, so a query
/// with NO pipeline stages merges too. The merged set is what `by`/`without`
/// group on, what label filters see, and what `line_format`/`label_format`
/// read (issue #249).
///
/// Empty string = no structured metadata — also what pre-#97 rows read back
/// through the column's `DEFAULT ''`, and what the writer stores for an entry
/// with none (`structured_metadata_json` returns `""`, never `"{}"`).
///
/// `absent_over_time` is the ONE reader that does not project this column:
/// `syntax/extractor.go:46-47 @ v3.7.4` forces `noLabels = true` and
/// `labels.go:667-668` then returns `EmptyLabelsResult`, so its label set is
/// provably metadata-independent and the unbounded scan need not read a
/// column it cannot use (the query-performance mandate). See
/// [`super::sql::ScanProjection`].
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct MetricScanRow {
    pub fingerprint: Fingerprint,
    pub timestamp_ns: i64,
    pub body: String,
    pub structured_metadata: String,
    /// The captures of each `regexp` stage the database runs (issue #624,
    /// part 3a): one element per pattern, `extractGroups(body, <p>)`, empty
    /// when the line does not match. Never a column of this type — it is
    /// decoded by the twin that names it, and empty when the statement sent
    /// no such column.
    #[serde(skip)]
    pub rx: Vec<Vec<String>>,
}

/// [`MetricScanRow`] with the `rx` column (issue #624, part 3a).
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct MetricScanRxRow {
    pub fingerprint: Fingerprint,
    pub timestamp_ns: i64,
    pub body: String,
    pub structured_metadata: String,
    pub rx: Vec<Vec<String>>,
}

impl From<MetricScanRxRow> for MetricScanRow {
    fn from(r: MetricScanRxRow) -> Self {
        MetricScanRow {
            fingerprint: r.fingerprint,
            timestamp_ns: r.timestamp_ns,
            body: r.body,
            structured_metadata: r.structured_metadata,
            rx: r.rx,
        }
    }
}

/// The single `/api/logs/v1/stats` aggregation row (issue #74): both the
/// rollup-served and the raw-fallback shapes project exactly these four
/// counters, in this order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct LogStatsRow {
    pub streams: u64,
    pub chunks: u64,
    pub entries: u64,
    pub bytes: u64,
}

/// One `/api/logs/v1/volume` aggregation row (issue #169): a fingerprint's
/// summed byte volume over the query window, off `log_metrics_<res>`
/// (rollup-only — the volume endpoint has no raw fallback).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct VolumeRow {
    pub fingerprint: Fingerprint,
    pub bytes: u64,
}

/// One `/api/logs/v1/detected_labels` aggregation row (issue #170): a
/// distinct `log_streams_idx` key with its exact value cardinality and the
/// count of values that are neither float nor UUID (`non_id_values` — the
/// server-side half of the reference's `containsAllIDTypes` filter; the
/// engine keeps a key iff it is a static label or `non_id_values > 0`).
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct DetectedLabelRow {
    pub key: String,
    pub cardinality: u64,
    pub non_id_values: u64,
}

/// One `/api/logs/v1/patterns` aggregation row (M7-C3, issue #171): a
/// distinct template, its total count across the window, and the ascending
/// `(ts_ns, count)` per-step samples the server-side `groupArray` assembled.
/// `samples` maps to `Array(Tuple(Int64, UInt64))` on the RowBinary wire.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct PatternFetchRow {
    pub pattern: String,
    pub total: u64,
    pub samples: Vec<(i64, u64)>,
}

/// Labels discovery (`log_streams_idx`): one distinct label key.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct LabelNameRow {
    pub name: String,
}

/// Label-values discovery (`log_streams_idx`): one distinct value of the
/// requested key.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct LabelValueRow {
    pub value: String,
}

/// A selectivity probe result (`count()` over one matcher's index prefix).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct ProbeRow {
    pub n: u64,
}

/// A range-query metric bucket: one `(fingerprint, step, n)` point, from
/// either the rollup table (`sum(count)`/`sum(bytes)`) or the raw fallback
/// (`count()`/`sum(length(body))`) — same shape either way
/// (docs/schemas.md §3.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct MetricBucketRow {
    pub fingerprint: Fingerprint,
    pub step: i64,
    pub n: u64,
}

/// An instant-query metric point: one `(fingerprint, structured_metadata, n)`
/// aggregate over the single evaluation window — structurally no `step`
/// column, matching [`crate::logql::params::QuerySpec::Instant`]'s "no
/// bucketing" contract.
///
/// Grouped server-side by `(fingerprint, structured_metadata)` since issue
/// #249, because the metric path merges structured metadata into the label
/// set and one fingerprint therefore covers N output series. The client
/// RE-groups the returned rows by the merged final label set and sums `n`
/// BEFORE `apply_rate`; that is exact because every op reaching this path is
/// a linear sum (`count()` / `sum(length(body))` — the only two `agg_expr`
/// values a `client == None` plan can carry, `plan.rs`), so server-grouping
/// on the raw metadata string and client-regrouping on the final label set
/// produce bit-identical values to the client path's single accumulator.
///
/// `structured_metadata` is LAST (append-only, the [`SampleRow`]
/// precedent). Empty string = none.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct MetricInstantRow {
    pub fingerprint: Fingerprint,
    pub n: u64,
    pub structured_metadata: String,
}

/// A bucketed range-query partial (issue #507, W2): one
/// `(fingerprint, grid point, structured_metadata)` group from
/// [`crate::logql::sql::metric_range_bucketed`], which counts in the
/// database instead of returning one row per log line.
///
/// `bucket_ns` is the emit GRID POINT — the anchored ceiling
/// `lo + intDiv(timestamp_ns - lo + step - 1, step) * step` with
/// `lo = grid_start_ns - step_ns`, so a row's bucket is the grid point
/// whose window `(g - range, g]` contains it. It is a timestamp, never a
/// duration, which is why it is `Int64` on both sides.
///
/// `n` is `count()` or `sum(length(body))` — both `UInt64`, both exact
/// under client-side addition, which is what lets the four counting
/// reducers claim [`crate::compile::plan::Fidelity::Equivalent`] on this
/// path.
///
/// `structured_metadata` is LAST, the [`MetricInstantRow`]/[`SampleRow`]
/// convention. Empty string = none. The column is carried raw and
/// uninterpreted; the reader decides what it means.
///
/// **One row type, always four columns.** [`crate::logql::sql::ScanProjection::Lean`]
/// would drop the fourth, and no read path takes it (issue #624 part 2).
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct MetricRangeBucketRow {
    pub fingerprint: Fingerprint,
    pub bucket_ns: i64,
    pub n: u64,
    pub structured_metadata: String,
    /// Whether the group's lines matched the `regexp` stage (issue #624,
    /// part 3a). Never a column of this type: decoded by
    /// [`MetricRangeRegexpRow`], and 0 for a statement with no stage.
    #[serde(skip)]
    pub matched: u8,
    /// The captures the plan sends, in capture-index order; empty when the
    /// lines did not match. Decoded as `matched` is.
    #[serde(skip)]
    pub caps: Vec<String>,
}

/// [`MetricRangeBucketRow`] with the `regexp` stage's two group-key columns
/// (issue #624, part 3a).
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct MetricRangeRegexpRow {
    pub fingerprint: Fingerprint,
    pub bucket_ns: i64,
    pub n: u64,
    pub structured_metadata: String,
    pub matched: u8,
    pub caps: Vec<String>,
}

impl From<MetricRangeRegexpRow> for MetricRangeBucketRow {
    fn from(r: MetricRangeRegexpRow) -> Self {
        MetricRangeBucketRow {
            fingerprint: r.fingerprint,
            bucket_ns: r.bucket_ns,
            n: r.n,
            structured_metadata: r.structured_metadata,
            matched: r.matched,
            caps: r.caps,
        }
    }
}

/// One row of the extracted-field group key statement, S1 (issue #507,
/// [`crate::logql::sql::metric_range_unwrapped`]): one (class, grid point,
/// key labels, projected metadata) group.
///
/// `keys` is `(present, text)` per key label, in the plan's key order;
/// `present = 0` is an absent or blanked key. `v` sums the decided rows'
/// values; `n_value` counts them, `n_missing` counts the rows dropped for
/// having no value, and `n_undecided` the rows the database could not
/// decide (zero whenever the statement throws on them).
#[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
pub struct MetricRangeUnwrappedRow {
    /// The class id, or the fingerprint itself when the plan groups per
    /// fingerprint — one `UInt128` column either way (issue #498).
    pub class: Fingerprint,
    pub bucket_ns: i64,
    pub keys: Vec<(u8, String)>,
    pub v: f64,
    pub n_value: u64,
    pub n_missing: u64,
    pub n_undecided: u64,
    pub sm_text: String,
    pub sm_kept: Vec<(String, String)>,
}

/// One row of the one read, L (issue #507,
/// [`crate::logql::sql::metric_range_unwrapped_rows`]): a decided row with
/// its key labels and value, or an undecided row with its `body`, its
/// `fingerprint` and its stored metadata in `sm_text`. A missing row is
/// never sent.
#[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
pub struct UnwrappedLaneRow {
    /// The class id, or the fingerprint itself when the plan groups per
    /// fingerprint — one `UInt128` column either way (issue #498).
    pub class: Fingerprint,
    pub bucket_ns: i64,
    pub decided: u8,
    pub keys: Vec<(u8, String)>,
    pub v: f64,
    pub body: String,
    pub fingerprint: Fingerprint,
    pub sm_text: String,
    pub sm_kept: Vec<(String, String)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_row_derives_are_usable() {
        let a = StreamRow {
            fingerprint: Fingerprint::from_raw(1),
        };
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn label_name_row_derives_are_usable() {
        let a = LabelNameRow {
            name: "env".to_string(),
        };
        assert_eq!(a.clone(), a);
    }

    #[test]
    fn label_value_row_derives_are_usable() {
        let a = LabelValueRow {
            value: "prod".to_string(),
        };
        assert_eq!(a.clone(), a);
    }

    #[test]
    fn metric_bucket_row_derives_are_usable() {
        let a = MetricBucketRow {
            fingerprint: Fingerprint::from_raw(1),
            step: 0,
            n: 5,
        };
        assert_eq!(a, a);
    }
}
