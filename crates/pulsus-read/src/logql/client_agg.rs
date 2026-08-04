//! The streaming client-aggregation state — the only O(rows) path in the
//! LogQL read path.
//!
//! [`ClientAggState`] serves instant queries and [`RangeSlideState`] the
//! sliding-window range queries of issue #227, both folding rows into
//! reducer state as they arrive so process memory stays `O(series)`. The
//! per-row reducers ([`SimpleAcc`], [`BucketAcc`], [`FpSlide`],
//! [`MutCells`], [`reduce_window`], the `rate_counter_*` family) live here
//! with them deliberately: the whole per-row loop stays in one module so
//! no hot-path call crosses a codegen-unit boundary.

use super::error::{ReadError, TooBroadReason};
use super::pipeline::{ERROR_LABEL, MetricRun, RangeGrouping};
use super::plan::{self, ClientAgg, ClientValue};
use super::rows::{MetricScanRow, StreamMetaRow};
use pulsus_logql::RangeAggOp;
use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};

use super::agg::{EMPTY_LABEL_SET, LabelSet, pin_reduction_order, range_payload_cmp};
use super::charge::{
    AggCaps, FP_GROUP_SLOT, INSTANT_GROUP_SLOT, MUT_GROUP_SLOT, SERIES_OUT_SLOT, alloc_block_bytes,
    charge_group_bytes, charge_result_points, charge_retention, discharge_group_bytes,
    discharge_retention, group_entry_bytes, grown_alloc_bytes, rendered_labels_json_len,
    retention_points_per_sample,
};
use super::exec::{MatrixSeries, QueryResult, VectorSample, apply_rate, range_seconds};
use super::fold::{FoldGrid, VectorAggFold, covering_k_of, grid_slot_count};
use super::labels::{render_labels_json_sorted, render_series_labels, series_labels, stream_hash};
use super::post_agg::apply_vector_aggs;
use super::window::{ClientWindow, InstantWindow, clamp_bucket, ensure_grid_resolution};

/// How many rows the streaming client-aggregation fetch buffers between
/// folds into [`ClientAggState`] — bounds transient memory without
/// per-row fold overhead (review round 1, finding 1).
pub(in crate::logql) const CLIENT_AGG_CHUNK_ROWS: usize = 8_192;

/// Streaming per-bucket accumulator for every over-time reducer except
/// `quantile_over_time` (which needs the full value set). Welford's
/// algorithm for mean/M2 (population stddev/stdvar); first/last are the
/// endpoints of the canonical `(timestamp, stream_hash, tie_rank)` order.
#[derive(Debug, Clone)]
pub(in crate::logql) struct SimpleAcc {
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
    mean: f64,
    m2: f64,
    /// The `(timestamp, stream_hash)` key of the current `first`/`last`
    /// candidate — see [`SimpleAcc::add`] for why `tie_rank` needs no
    /// field.
    first_key: (i64, u64),
    first_v: f64,
    last_key: (i64, u64),
    last_v: f64,
}

impl SimpleAcc {
    fn new(ts_ns: i64, stream_hash: u64, v: f64) -> Self {
        SimpleAcc {
            count: 1,
            sum: v,
            min: v,
            max: v,
            mean: v,
            m2: 0.0,
            first_key: (ts_ns, stream_hash),
            first_v: v,
            last_key: (ts_ns, stream_hash),
            last_v: v,
        }
    }

    /// **`first`/`last` are positions in Loki's delivery order, not
    /// extrema** (issue #344; this replaced a value-tiebreak whose stated
    /// premise the source refuted).
    ///
    /// The reference reads them straight off its merged sample iterator —
    /// `first(samples) = samples[0]`, `last(samples) = samples[len-1]`
    /// (`pkg/logql/range_vector.go:489-501 @ v3.7.4`), and the streaming
    /// evaluator an instant query uses does the same incrementally
    /// (`FirstOverTime.agg` keeps the first sample it sees,
    /// `LastOverTime.agg` overwrites with every sample, `:818-844`). That
    /// delivery order is `(Timestamp, StreamHash)`
    /// (`SampleIteratorHeap.Less`, `pkg/iter/sample_iterator.go:139-148`),
    /// where `StreamHash()` is `labels.StableHash` over the ORIGINAL
    /// stream labels (`pkg/chunkenc/memchunk.go:1922`) — the same
    /// [`win_order`] the sliding path has folded in since issue #227.
    ///
    /// **`tie_rank` needs no field here.** It only separates samples of
    /// the SAME stream at the SAME nanosecond, and `metric_raw_samples`
    /// delivers those in `ORDER BY timestamp_ns, fingerprint, body` —
    /// ascending body, which is exactly the group-local `tie_rank` the
    /// sliding path assigns. So `<` for `first` (keep the earliest
    /// arrival on an exact key tie) and `>=` for `last` (take the latest)
    /// reproduce the full three-key order from the scan's own sequence.
    ///
    /// The predecessor rule — smallest value at the minimum timestamp,
    /// largest at the maximum — was adopted because the reference's
    /// instant tie order was believed unspecified. It is specified, and
    /// on a group merging two streams at one nanosecond the two rules
    /// disagree: measured against the pinned v3.7.4 container,
    /// `last_over_time(…) by (fp)` answers 5 where the value rule gives 7.
    fn add(&mut self, ts_ns: i64, stream_hash: u64, v: f64) {
        self.count += 1;
        self.sum += v;
        self.min = self.min.min(v);
        self.max = self.max.max(v);
        let delta = v - self.mean;
        self.mean += delta / self.count as f64;
        self.m2 += delta * (v - self.mean);
        let key = (ts_ns, stream_hash);
        if key < self.first_key {
            self.first_key = key;
            self.first_v = v;
        }
        if key >= self.last_key {
            self.last_key = key;
            self.last_v = v;
        }
    }
}

/// One bucket's state: streaming stats, the full value set for
/// `quantile_over_time`, or the timestamped counter samples for
/// `rate_counter` (reset detection is order-dependent, so the raw
/// `(ts, value)` points are retained and walked at `finish`).
#[derive(Debug, Clone)]
pub(in crate::logql) enum BucketAcc {
    Simple(SimpleAcc),
    Values(Vec<f64>),
    Counter(Vec<(i64, f64)>),
}

impl BucketAcc {
    fn new(op: RangeAggOp, ts_ns: i64, stream_hash: u64, v: f64) -> Self {
        match op {
            RangeAggOp::QuantileOverTime => BucketAcc::Values(vec![v]),
            RangeAggOp::RateCounter => BucketAcc::Counter(vec![(ts_ns, v)]),
            _ => BucketAcc::Simple(SimpleAcc::new(ts_ns, stream_hash, v)),
        }
    }

    fn add(&mut self, ts_ns: i64, stream_hash: u64, v: f64) {
        match self {
            BucketAcc::Simple(acc) => acc.add(ts_ns, stream_hash, v),
            BucketAcc::Values(vals) => vals.push(v),
            BucketAcc::Counter(pts) => pts.push((ts_ns, v)),
        }
    }

    /// Finishes the bucket into its reducer value.
    fn finish(self, op: RangeAggOp, rate_window_ns: Option<u64>, quantile: Option<f64>) -> f64 {
        match self {
            BucketAcc::Values(mut vals) => quantile_of(&mut vals, quantile.unwrap_or(f64::NAN)),
            BucketAcc::Counter(pts) => rate_counter_extrapolated(pts, rate_window_ns),
            BucketAcc::Simple(acc) => match op {
                // Oracle-probed: `rate` over an unwrapped range is the
                // per-second SUM of values (count-shaped inputs
                // contribute 1.0 each, so the un-piped semantic is
                // unchanged); `bytes_rate` likewise.
                RangeAggOp::Rate | RangeAggOp::BytesRate => apply_rate(acc.sum, rate_window_ns),
                RangeAggOp::CountOverTime => acc.count as f64,
                RangeAggOp::BytesOverTime | RangeAggOp::SumOverTime => acc.sum,
                RangeAggOp::AvgOverTime => acc.mean,
                RangeAggOp::MinOverTime => acc.min,
                RangeAggOp::MaxOverTime => acc.max,
                RangeAggOp::StddevOverTime => (acc.m2 / acc.count as f64).sqrt(),
                RangeAggOp::StdvarOverTime => acc.m2 / acc.count as f64,
                RangeAggOp::FirstOverTime => acc.first_v,
                RangeAggOp::LastOverTime => acc.last_v,
                // Absent is the dedicated presence branch in
                // `run_client_agg_rows`; quantile is `Values`-backed.
                RangeAggOp::QuantileOverTime | RangeAggOp::AbsentOverTime => {
                    unreachable!("dispatched before BucketAcc::finish")
                }
                // `rate_counter` is `Counter`-backed (reset detection is
                // order-dependent), never `Simple`.
                RangeAggOp::RateCounter => unreachable!("Counter-backed"),
            },
        }
    }
}

/// `rate_counter` reducer — a bit-exact replica of the pinned reference's
/// `extrapolatedRate(samples, selRange, isCounter=true, isRate=true)`
/// (grafana/loki v3.7.3, `pkg/logql/range_vector.go`), replayed over
/// PulsusDB's nanosecond timestamps.
///
/// Two load-bearing quirks make this diverge from a plain
/// `reset-aware increase / range.Seconds()`:
///  1. The reference's window iterators store sample timestamps in
///     nanoseconds, but `extrapolatedRate` divides every span by `1000`
///     (`durationMilliseconds`) — treating those ns values as ms. We keep
///     PulsusDB's ns timestamps and divide by `1000` identically, so the
///     unit-mix is reproduced rather than corrected.
///  2. The extrapolation window is anchored on the FIRST sample, not the
///     query step: `rangeStart = samples[0].T - durationMilliseconds(selRange)`,
///     `rangeEnd = samples[last].T`. So `durationToEnd == 0` and
///     `durationToStart == selRange_ms/1000 == 60.0` for a `[1m]` window —
///     the factor is `1 + 60000/span_ns` (~1.00006), NOT PromQL's ~6x
///     full-range extrapolation.
///
/// Samples are ordered by timestamp with a STABLE sort (no value
/// tie-break): duplicate-timestamp samples keep their delivered scan order
/// — the deterministic SQL scan order `ORDER BY timestamp_ns, fingerprint,
/// body` (`metric_raw_samples`) — matching the reference, which processes
/// same-timestamp unwrapped samples in storage/scan order rather than
/// re-sorting them by value (branch-validated: `5, 10, 3, 12` with `10`/`3`
/// tied at one timestamp yields increase 17 / `0.2833…`, not the 12 / 0.2 an
/// ascending-value sort would give).
///
/// The reset-aware increase is accumulated in the reference's own order
/// (`resultValue = last - first; for s { if s < lastValue { resultValue +=
/// lastValue } }`), which is the bit-exact form to match. The reference
/// EMITS `0.0` for `<2`-point groups (verified against grafana/loki:3.7.3 —
/// a lone sample returns value `"0"`), so we return `0.0` and let `finish`
/// surface it as a 0-valued vector element; we do NOT drop the group.
fn rate_counter_extrapolated(mut pts: Vec<(i64, f64)>, rate_window_ns: Option<u64>) -> f64 {
    pts.sort_by(|(at, _), (bt, _)| at.cmp(bt));
    rate_counter_over_sorted(pts.len(), |i| pts[i], rate_window_ns)
}

/// [`rate_counter_extrapolated`]'s body over an ALREADY timestamp-sorted
/// sequence addressed by index — `at(i)` yields the `i`-th `(ts, value)` in
/// ascending-`ts` order.
///
/// Split out so the sliding evaluator can reduce **over a borrow of the
/// live window**: the retained window is already in canonical `(ts,
/// stream_hash, tie_rank)` order (whose leading key is `ts`), so the sort
/// the owning form performs is a no-op on it, and copying it into a
/// `Vec<(i64, f64)>` at every grid point — as the round-4 code did — was
/// pure waste that also doubled peak memory for the copy (issue #227 review
/// round 5, finding 2). Values are read straight out of the window instead.
/// The arithmetic is byte-identical to the owning form; only the storage
/// differs.
fn rate_counter_over_sorted<F>(len: usize, at: F, rate_window_ns: Option<u64>) -> f64
where
    F: Fn(usize) -> (i64, f64),
{
    if len < 2 {
        return 0.0;
    }
    let Some(rng_ns) = rate_window_ns.filter(|w| *w > 0) else {
        return 0.0;
    };
    let (first_t, first_f) = at(0);
    let (last_t, last_f) = at(len - 1);

    // Reset-aware increase in the reference's accumulation order.
    let mut result_value = last_f - first_f;
    let mut last_value = 0.0_f64;
    for i in 0..len {
        let f = at(i).1;
        if f < last_value {
            result_value += last_value;
        }
        last_value = f;
    }

    // `durationMilliseconds(selRange)`; window anchored on the first sample.
    //
    // Issue #227 review finding 2: every timestamp SPAN is computed in `i128`
    // before the float conversion. `first_t - sel_range_ms` underflows i64 for
    // a first sample near `i64::MIN`, and `last_t - first_t` overflows i64 for
    // a window spanning near-`MIN` to near-`MAX` — both are valid extreme
    // inputs that would panic in debug / wrap in release. The i128 widening
    // changes no in-range value (the arithmetic is identical), it only removes
    // the overflow; the reference's ns-vs-ms unit mix is preserved verbatim.
    let sel_range_ms: i128 = (rng_ns / 1_000_000) as i128;
    let range_start = first_t as i128 - sel_range_ms; // ns − ms: the reference's unit mix
    let mut duration_to_start = (first_t as i128 - range_start) as f64 / 1000.0; // == sel_range_ms/1000
    let duration_to_end = 0.0_f64; // rangeEnd == last.T ⇒ 0
    let sampled_interval = (last_t as i128 - first_t as i128) as f64 / 1000.0;
    let average_duration = sampled_interval / (len - 1) as f64;

    if result_value > 0.0 && first_f >= 0.0 {
        let duration_to_zero = sampled_interval * (first_f / result_value);
        if duration_to_zero < duration_to_start {
            duration_to_start = duration_to_zero;
        }
    }

    let threshold = average_duration * 1.1;
    let mut extrapolate_to = sampled_interval;
    extrapolate_to += if duration_to_start < threshold {
        duration_to_start
    } else {
        average_duration / 2.0
    };
    extrapolate_to += if duration_to_end < threshold {
        duration_to_end
    } else {
        average_duration / 2.0
    };
    result_value *= extrapolate_to / sampled_interval;
    result_value / range_seconds(rng_ns) // isRate: / selRange.Seconds()
}

/// The reference oracle's quantile semantics (live-probed: `q=0.9` over
/// `1,2,3,4` is `3.7` — linear interpolation on the sorted values):
/// `q < 0` → `-Inf`, `q > 1` → `+Inf`, NaN propagates.
fn quantile_of(values: &mut [f64], q: f64) -> f64 {
    if values.is_empty() || q.is_nan() {
        return f64::NAN;
    }
    if q < 0.0 {
        return f64::NEG_INFINITY;
    }
    if q > 1.0 {
        return f64::INFINITY;
    }
    values.sort_by(f64::total_cmp);
    let rank = q * (values.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    let weight = rank - rank.floor();
    values[lower] * (1.0 - weight) + values[upper] * weight
}

/// Streaming client-aggregation state (issue M6-10, review round 1
/// finding 1): rows fold into reducer state as they arrive so process
/// memory stays `O(series)` (+ the caller's bounded chunk) instead of
/// retaining the whole raw scan. The pure [`run_client_agg_rows`]
/// wrapper drives it over a slice for the hermetic golden/allocation
/// suites; the engine drives it chunk-wise off the live row stream.
///
/// **Instant-only by construction** (issue #236 Part D). It always was —
/// `run_metric_client` and `run_client_agg_rows` route every stepped
/// window to [`RangeSlideState`], and `bucket_of` returned the single
/// [`INSTANT_BUCKET`] with no step — so the per-group
/// `BTreeMap<i64, BucketAcc>` always held exactly one entry, and the map
/// (a ~1 KB/group allocation `group_entry_bytes` never saw: an #227
/// accounting hole) was pure overhead. The constructor now takes an
/// [`InstantWindow`], so the dead stepped branches are not merely unused
/// but unrepresentable.
#[derive(Debug)]
pub(in crate::logql) struct ClientAggState<'q> {
    compiled: &'q super::pipeline::CompiledPipeline,
    client: &'q ClientAgg,
    rate_window_ns: Option<u64>,
    /// Base labels once per fingerprint, in the same shape the SQL
    /// metric path exposes (`series_labels`: canonical JSON labels +
    /// the physical `service` column re-injected as `service_name`,
    /// sorted).
    pub(in crate::logql) base_labels: HashMap<u64, Vec<(String, String)>>,
    /// Per-fingerprint `StableHash` of the stream labels — the SECOND key
    /// of Loki's sample delivery order, which is what
    /// `first_over_time`/`last_over_time` read their endpoints off
    /// ([`SimpleAcc::add`]). The sliding path has kept this map since
    /// issue #227; the instant path needed it once a grouping let two
    /// streams share one accumulator (issue #344).
    hashes: HashMap<u64, u64>,
    fan_out: bool,
    /// `absent_over_time`'s selector-wide presence (plan v2 D2). A FLAG,
    /// not a bucket set, since issue #236 Part D: an instant window has
    /// exactly one bucket, so the set could only ever hold
    /// `{INSTANT_BUCKET}` and the only question ever asked of it was
    /// whether it was empty.
    present: bool,
    /// Non-mutating pipelines group by fingerprint (zero per-row
    /// allocations — the alloc-gate path). ONE accumulator per group,
    /// not a bucket map — see [`ClientAggState`]'s instant-only contract.
    fp_groups: HashMap<u64, BucketAcc>,
    /// Label-mutating/unwrapping pipelines group by the rendered final
    /// label set.
    label_groups: HashMap<String, (LabelSet, BucketAcc)>,
    /// Total values retained across every quantile accumulator, charged
    /// against [`MAX_QUANTILE_VALUES`].
    quantile_values: u64,
    /// Total timestamped points retained across every `rate_counter`
    /// accumulator, charged against [`MAX_COUNTER_VALUES`].
    counter_values: u64,
    /// QUERY-LIFETIME bytes retained by `label_groups` — each distinct
    /// group's rendered key + cloned `LabelSet` + map-slot share. The same
    /// round-6 charge as the sliding path's `groups`, through the same
    /// [`charge_group_bytes`]/[`group_entry_bytes`] helpers: the group
    /// COUNT cap alone left per-group label BYTES unbounded here too.
    group_bytes: u64,
    /// Every retention cap this state checks (issue #221) — always
    /// [`AggCaps::DEFAULT`] in production single-extractor use (test seam,
    /// the former `group_bytes_cap` precedent); a `variants(...)` query
    /// hands each sub-state `AggCaps::DEFAULT.divided(n)`.
    caps: AggCaps,
}

impl<'q> ClientAggState<'q> {
    /// Snapshots the per-fingerprint base labels.
    ///
    /// The former stepped-grid guard is gone with the [`InstantWindow`]
    /// parameter (issue #236 Part D): it existed to make a stepped window
    /// that reached here obey the Loki-exact resolution rule, and a
    /// stepped window can no longer reach here. `Result` is kept because
    /// the constructor is one of the fallible-by-contract seams the
    /// variants fan-out charges through.
    pub(in crate::logql) fn new(
        compiled: &'q super::pipeline::CompiledPipeline,
        meta: &HashMap<u64, StreamMetaRow>,
        client: &'q ClientAgg,
        _window: InstantWindow,
        rate_window_ns: Option<u64>,
        caps: AggCaps,
    ) -> Result<Self, ReadError> {
        let mut base_labels: HashMap<u64, Vec<(String, String)>> = HashMap::new();
        let mut hashes: HashMap<u64, u64> = HashMap::new();
        for (fp, m) in meta {
            let labels = series_labels(m);
            hashes.insert(*fp, stream_hash(&labels));
            base_labels.insert(*fp, labels);
        }
        Ok(ClientAggState {
            compiled,
            client,
            rate_window_ns,
            base_labels,
            hashes,
            // Issue #344 — the instant twin of `RangeSlideState`'s gate:
            // a grouping merges streams, so the state must group by final
            // label set (`label_groups`), never by fingerprint.
            fan_out: compiled.metric_mutates_labels() || client.grouping.is_some(),
            present: false,
            fp_groups: HashMap::new(),
            label_groups: HashMap::new(),
            quantile_values: 0,
            counter_values: 0,
            group_bytes: 0,
            caps,
        })
    }

    /// Folds one batch of rows into the reducer state: each row runs the
    /// compiled pipeline (`run_metric_into` — unwrap executes,
    /// `__error__` annotates in stage order), FAILS the query on any
    /// surviving nonempty `__error__` (adjudication #1, oracle-matched
    /// message), and accumulates per `(final-label-set, bucket)`. One
    /// label scratch is reused across the whole batch (the #72
    /// allocation discipline).
    pub(in crate::logql) fn push_rows(&mut self, rows: &[MetricScanRow]) -> Result<(), ReadError> {
        let mut scratch: Vec<(Cow<'_, str>, Cow<'_, str>)> = Vec::new();
        let is_absent = matches!(self.client.range_op, RangeAggOp::AbsentOverTime);
        for row in rows {
            let Some(base) = self.base_labels.get(&row.fingerprint) else {
                continue;
            };
            let (line, value) = match self.compiled.run_metric_into(
                &row.body,
                base,
                row.timestamp_ns,
                self.client.grouping.as_deref(),
                &mut scratch,
            )? {
                MetricRun::Dropped => continue,
                MetricRun::Kept { line, value } => (line, value),
            };
            check_surviving_error(&scratch)?;
            if is_absent {
                // Selector-wide presence (plan v2 D2): did ANY line
                // survive, across every fingerprint and label set. One
                // bucket, so one flag (issue #236 Part D).
                self.present = true;
                continue;
            }
            let v = match self.client.value {
                ClientValue::Count => 1.0,
                ClientValue::Bytes => line.len() as f64,
                ClientValue::Unwrap => match value {
                    Some(v) => v,
                    // Defensive: a `None` unwrap value always carries a
                    // nonempty `__error__` (checked above) unless a
                    // filter dropped the line — unreachable, but never a
                    // silent 0.
                    None => continue,
                },
            };
            let op = self.client.range_op;
            // Issue #344: the sample's stream identity, the second key of
            // Loki's delivery order ([`SimpleAcc::add`]). A fingerprint
            // with no hydrated meta cannot reach here — `base_labels` was
            // resolved above and both maps are filled from the same
            // iteration — so the `unwrap_or(0)` is defence in depth, and
            // the same default `RangeSlideState::flush_collision` uses.
            let stream_hash = self.hashes.get(&row.fingerprint).copied().unwrap_or(0);
            if matches!(op, RangeAggOp::QuantileOverTime) {
                self.quantile_values += 1;
                if self.quantile_values > self.caps.quantile_values {
                    return Err(ReadError::QueryTooBroad(TooBroadReason::QuantileValues {
                        count: self.quantile_values,
                        cap: self.caps.quantile_values,
                    }));
                }
            }
            if matches!(op, RangeAggOp::RateCounter) {
                // One charge per scanned row IS one charge per retained
                // `Counter` point: `bucket_of` below yields exactly one
                // bucket and the row is pushed into it exactly once, so
                // `counter_values` equals the combined length of every
                // `Counter` vector — a true bound even when `step < range`
                // makes the reference's windows overlap (the raw scan
                // delivers each stored sample once; buckets partition it,
                // never re-retain). See `MAX_COUNTER_VALUES`.
                self.counter_values += 1;
                if self.counter_values > self.caps.counter_values {
                    return Err(ReadError::QueryTooBroad(TooBroadReason::CounterValues {
                        count: self.counter_values,
                        cap: self.caps.counter_values,
                    }));
                }
            }
            // ONE accumulator per group (issue #236 Part D): an instant
            // window has a single bucket, so each arm either seeds the
            // group's accumulator or folds into it — there is no bucket
            // map to index.
            if self.fan_out {
                scratch.sort_unstable();
                let key = render_labels_json_sorted(&scratch);
                match self.label_groups.entry(key) {
                    std::collections::hash_map::Entry::Occupied(e) => {
                        e.into_mut().1.add(row.timestamp_ns, stream_hash, v);
                    }
                    std::collections::hash_map::Entry::Vacant(e) => {
                        let labels: LabelSet = scratch
                            .iter()
                            .map(|(k, v)| (k.to_string(), v.to_string()))
                            .collect();
                        // Issue #227 review round 6 (same class as the
                        // sliding path): the key + `LabelSet` live in
                        // `label_groups` for the whole query — charged
                        // BEFORE the insert retains them; refused means the
                        // per-row transients above drop with the error and
                        // the map never holds the entry. (`entry()` has
                        // already reserved the table slot — that growth is
                        // covered by this charge's slot term, which since
                        // issue #236 deleted the group-COUNT cap is the
                        // whole bound on this map: bytes, never a count.)
                        charge_group_bytes(
                            &mut self.group_bytes,
                            group_entry_bytes(e.key(), &labels, INSTANT_GROUP_SLOT),
                            self.caps.group_bytes,
                        )?;
                        e.insert((labels, BucketAcc::new(op, row.timestamp_ns, stream_hash, v)));
                    }
                }
            } else {
                // Issue #236 P1 — the premise fix. Before #236 this arm was
                // count-gated only and its per-group BYTES were never
                // charged: `base_labels` is hydrated with no charge, so
                // deleting the count cap (Part A) would have left the
                // non-mutating instant path with no bound at all. The
                // charge rides the EXISTING `contains_key` probe (no added
                // per-row work) and happens BEFORE the entry is created, so
                // a refusal never leaves the map holding it.
                if !self.fp_groups.contains_key(&row.fingerprint) {
                    charge_group_bytes(
                        &mut self.group_bytes,
                        group_entry_bytes("", base, FP_GROUP_SLOT),
                        self.caps.group_bytes,
                    )?;
                }
                match self.fp_groups.entry(row.fingerprint) {
                    std::collections::hash_map::Entry::Occupied(mut e) => {
                        e.get_mut().add(row.timestamp_ns, stream_hash, v);
                    }
                    std::collections::hash_map::Entry::Vacant(e) => {
                        e.insert(BucketAcc::new(op, row.timestamp_ns, stream_hash, v));
                    }
                }
            }
        }
        Ok(())
    }

    /// Finishes every accumulator into the metric result.
    /// `absent_over_time` emits at most ONE sample; the other reducers
    /// emit one per surviving group.
    ///
    /// Always a `Vector` (issue #236 Part D): the state is instant-only by
    /// construction, so the former stepped arms — the `bucket_grid` walk
    /// for absence and the per-bucket `Matrix` emit — were dead, and are
    /// deleted rather than left as a second reading of a contract the type
    /// already states.
    fn finish(self) -> QueryResult {
        if matches!(self.client.range_op, RangeAggOp::AbsentOverTime) {
            let labels: LabelSet = self.client.absent_labels.clone();
            return if self.present {
                QueryResult::Vector(Vec::new())
            } else {
                QueryResult::Vector(vec![VectorSample { labels, value: 1.0 }])
            };
        }

        let base_labels = self.base_labels;
        let mut group_bytes = self.group_bytes;
        let groups: Vec<(LabelSet, BucketAcc)> = if self.fan_out {
            self.label_groups
                .into_iter()
                .map(|(key, (labels, acc))| {
                    // Round 6 symmetry: sized over the same unmodified
                    // key/labels as the charge, released as each entry is
                    // consumed (key dropped, labels move to the output).
                    discharge_group_bytes(
                        &mut group_bytes,
                        group_entry_bytes(&key, &labels, INSTANT_GROUP_SLOT),
                    );
                    (labels, acc)
                })
                .collect()
        } else {
            self.fp_groups
                .into_iter()
                .filter_map(|(fp, acc)| {
                    // Issue #236 P1's discharge leg, with the SAME symmetry
                    // the fan-out arm has had since round 6: sized over the
                    // same `base_labels` value the charge used (empty key,
                    // `FP_GROUP_SLOT`), released as the entry is consumed.
                    // `push_rows` only charges a fingerprint it successfully
                    // resolved in `base_labels`, so every charged entry is
                    // discharged here and `group_bytes` returns to exactly 0
                    // — the `filter_map` below can only drop an entry that
                    // was never charged.
                    let l = base_labels.get(&fp)?;
                    discharge_group_bytes(
                        &mut group_bytes,
                        group_entry_bytes("", l, FP_GROUP_SLOT),
                    );
                    Some((l.clone(), acc))
                })
                .collect()
        };
        debug_assert_eq!(
            group_bytes, 0,
            "every group-label byte charge must be discharged at finish"
        );
        let op = self.client.range_op;
        let rate_window_ns = self.rate_window_ns;
        let param = self.client.param;
        QueryResult::Vector(
            groups
                .into_iter()
                .map(|(labels, acc)| VectorSample {
                    labels,
                    value: acc.finish(op, rate_window_ns, param),
                })
                .collect(),
        )
    }
}

/// The three sliding-window reducer classes (issue #227 finding-4 table,
/// cited to Loki v3.7.4 `range_vector.go` / `syntax/ast.go:1449-1458`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::logql) enum ReducerClass {
    /// `count`/`bytes`/`bytes_rate`/`rate`(no-unwrap): a running INTEGER,
    /// add-on-load / subtract-on-evict. Integer ± is exact-invertible, so
    /// the running value is bit-identical to a fresh reduction (finding 1);
    /// only in-window samples are retained (for eviction).
    InvertInteger,
    /// `min`/`max`/`quantile`/`first`/`last`: re-reduce the retained window
    /// per step, order-INDEPENDENT (first/last are positional in the total
    /// order, computed by argmin/argmax — the value does not depend on fold
    /// order), so mergeable across fingerprints without a global sort.
    ReduceIndependent,
    /// `sum`/`avg`/`stddev`/`stdvar`/`rate_counter`/`rate`(unwrap):
    /// re-reduce, order-DEPENDENT — the float-accumulation / reset-walk
    /// order matters, so the retained window is folded in the canonical
    /// `(ts, stream_hash, tie_rank)` order (Loki's heap order).
    CanonicalFold,
}

/// Classifies a reducer (finding 4). `bytes_rate` rejects unwrap ⇒ always
/// integer/invert; `rate` is invert only WITHOUT unwrap (integer line
/// count); with unwrap it is a float sum ⇒ canonical fold. `stdvar` is
/// canonical fold. `absent` is handled specially (a sliding presence set)
/// and nominally classed order-independent.
pub(in crate::logql) fn reducer_class(op: RangeAggOp, value: ClientValue) -> ReducerClass {
    match op {
        RangeAggOp::CountOverTime | RangeAggOp::BytesOverTime | RangeAggOp::BytesRate => {
            ReducerClass::InvertInteger
        }
        RangeAggOp::Rate => {
            if matches!(value, ClientValue::Unwrap) {
                ReducerClass::CanonicalFold
            } else {
                ReducerClass::InvertInteger
            }
        }
        RangeAggOp::RateCounter
        | RangeAggOp::SumOverTime
        | RangeAggOp::AvgOverTime
        | RangeAggOp::StddevOverTime
        | RangeAggOp::StdvarOverTime => ReducerClass::CanonicalFold,
        RangeAggOp::MinOverTime
        | RangeAggOp::MaxOverTime
        | RangeAggOp::QuantileOverTime
        | RangeAggOp::FirstOverTime
        | RangeAggOp::LastOverTime
        | RangeAggOp::AbsentOverTime => ReducerClass::ReduceIndependent,
    }
}

/// One retained sample in a sliding window. Fixed-width (no body bytes ever
/// cross into the window — the deterministic full-body order is pre-baked
/// into `tie_rank` at collision-group formation).
#[derive(Debug, Clone, Copy)]
pub(in crate::logql) struct WinSample {
    ts: i64,
    stream_hash: u64,
    /// Group-local rank within the same-`(fingerprint, ts)` collision group,
    /// by ascending full-body bytes (issue #227): the deterministic
    /// same-stream tiebreak.
    tie_rank: u32,
    value: f64,
}

/// A non-empty marker slice element for the class-A (integer) emit path,
/// whose reducer reads only the running integer and the window's
/// emptiness — never the sample contents.
const WIN_SAMPLE_MARKER: WinSample = WinSample {
    ts: 0,
    stream_hash: 0,
    tie_rank: 0,
    value: 0.0,
};

/// The total order for the class-C fold and first/last argmin/argmax:
/// `(ts, stream_hash, tie_rank)` — different ts by ts; same ts different
/// stream by `stream_hash` (Loki-exact cross-stream); same ts same stream
/// (⇒ one collision group) by `tie_rank` (the ratified deterministic
/// same-stream divergence).
fn win_order(a: &WinSample, b: &WinSample) -> std::cmp::Ordering {
    a.ts.cmp(&b.ts)
        .then(a.stream_hash.cmp(&b.stream_hash))
        .then(a.tie_rank.cmp(&b.tie_rank))
}

/// One member of a consecutive same-`(fingerprint, timestamp_ns)` collision
/// run, buffered until the run closes so it can be ranked by full body.
#[derive(Debug)]
struct CollMember {
    /// The raw body bytes — the tiebreak key, held ONLY for the current
    /// group and dropped once `tie_rank` is assigned.
    body: String,
    value: f64,
    /// Mutating path only: the rendered final label-set key + labels.
    out: Option<(String, LabelSet)>,
}

/// Reduces a canonical-ordered window slice into one reducer value (`None`
/// for an empty window ⇒ a gap, except `absent`, handled by the caller).
/// `run_int` is the class-A running integer; `ordered` must already be in
/// canonical `(ts, stream_hash, tie_rank)` order for the class-C folds.
fn reduce_window(
    op: RangeAggOp,
    class: ReducerClass,
    param: Option<f64>,
    rate_window_ns: Option<u64>,
    run_int: u64,
    ordered: &[WinSample],
) -> Option<f64> {
    if ordered.is_empty() {
        return None;
    }
    match op {
        RangeAggOp::CountOverTime | RangeAggOp::BytesOverTime => Some(run_int as f64),
        RangeAggOp::BytesRate => Some(apply_rate(run_int as f64, rate_window_ns)),
        RangeAggOp::Rate => {
            let n = if matches!(class, ReducerClass::InvertInteger) {
                run_int as f64
            } else {
                ordered.iter().fold(0.0_f64, |acc, s| acc + s.value)
            };
            Some(apply_rate(n, rate_window_ns))
        }
        RangeAggOp::FirstOverTime => Some(ordered[0].value),
        RangeAggOp::LastOverTime => Some(ordered[ordered.len() - 1].value),
        RangeAggOp::QuantileOverTime => {
            // The ONE re-reduction that cannot run over a borrow: the
            // quantile needs the values SORTED, and the window itself must
            // stay in ascending-`ts` order for eviction. The copy is
            // pre-charged per retained sample by
            // [`retention_points_per_sample`] (issue #227 review round 5,
            // finding 2), so the cap has already approved it — `collect`
            // from a `TrustedLen` iterator reserves exactly `ordered.len()`
            // `f64`s, four times inside the 32-byte-per-sample charge.
            let mut vals: Vec<f64> = ordered.iter().map(|s| s.value).collect();
            Some(quantile_of(&mut vals, param.unwrap_or(f64::NAN)))
        }
        RangeAggOp::RateCounter => {
            // Over a BORROW — `ordered` is already ascending by `ts` (the
            // canonical order's leading key), which is all the reset walk
            // needs. No `Vec<(i64, f64)>` copy of the whole window per grid
            // point (issue #227 review round 5, finding 2).
            Some(rate_counter_over_sorted(
                ordered.len(),
                |i| (ordered[i].ts, ordered[i].value),
                rate_window_ns,
            ))
        }
        // sum/avg/min/max/stddev/stdvar reuse the instant path's exact
        // arithmetic (`SimpleAcc`) folded in canonical order.
        _ => {
            let mut acc = SimpleAcc::new(ordered[0].ts, ordered[0].stream_hash, ordered[0].value);
            for s in &ordered[1..] {
                acc.add(s.ts, s.stream_hash, s.value);
            }
            Some(match op {
                RangeAggOp::SumOverTime => acc.sum,
                RangeAggOp::AvgOverTime => acc.mean,
                RangeAggOp::MinOverTime => acc.min,
                RangeAggOp::MaxOverTime => acc.max,
                RangeAggOp::StddevOverTime => (acc.m2 / acc.count as f64).sqrt(),
                RangeAggOp::StdvarOverTime => acc.m2 / acc.count as f64,
                _ => unreachable!("reduce_window: op dispatched above"),
            })
        }
    }
}

/// Class-A integer-cell reduce for the mutating fan-out finish (`ordered` is
/// not retained on the integer path — the running count is authoritative).
fn reduce_int_cell(op: RangeAggOp, rate_window_ns: Option<u64>, run_int: u64) -> f64 {
    match op {
        RangeAggOp::CountOverTime | RangeAggOp::BytesOverTime => run_int as f64,
        RangeAggOp::BytesRate | RangeAggOp::Rate => apply_rate(run_int as f64, rate_window_ns),
        _ => unreachable!("reduce_int_cell: non-integer op"),
    }
}

/// `ceil(a / b)` for `b > 0`, exact over i128 (handles negative `a`).
pub(in crate::logql) fn ceil_div_i128(a: i128, b: i128) -> i128 {
    let q = a.div_euclid(b);
    if a.rem_euclid(b) != 0 { q + 1 } else { q }
}

/// A single fingerprint's streaming sliding window (non-mutating path). Its
/// deque is inherently in canonical order — samples load in ascending ts and
/// within a ts by ascending `tie_rank`, and `stream_hash` is constant for
/// one fingerprint.
#[derive(Debug)]
struct FpSlide {
    stream_hash: u64,
    labels: LabelSet,
    op: RangeAggOp,
    class: ReducerClass,
    param: Option<f64>,
    rate_window_ns: Option<u64>,
    grid_start: i64,
    step: u64,
    range: i64,
    kmax: i64,
    /// Next grid index to emit.
    next_k: i64,
    win: VecDeque<WinSample>,
    /// Class-A running integer (add-on-load / subtract-on-evict).
    run_int: u64,
    /// Retention points one retained sample costs — the window entry plus
    /// any per-emit re-reduce scratch the reducer needs
    /// ([`retention_points_per_sample`]). Charged on load, discharged on
    /// evict, so charge and discharge use the same unit.
    per_sample: u64,
    points: Vec<(i64, f64)>,
}

impl FpSlide {
    fn grid_point(&self, k: i64) -> i64 {
        // i128 intermediate + a SATURATING narrowing (issue #227 arithmetic
        // sweep): `k <= kmax` derives from `grid_point_count`, so every grid
        // point is in `[grid_start, end]` and the clamp never fires on a
        // real query — but a plain `as i64` would silently wrap rather than
        // saturate if that invariant were ever broken.
        clamp_bucket(self.grid_start as i128 + k as i128 * self.step as i128)
    }

    /// Emits every grid point `t` with `t < boundary` (all their samples are
    /// already loaded, since future samples have `ts ≥ boundary > t`).
    fn emit_until(&mut self, boundary: i64, retained: &mut u64) {
        while self.next_k <= self.kmax {
            let t = self.grid_point(self.next_k);
            if t >= boundary {
                break;
            }
            self.emit_at(t, retained);
            self.next_k += 1;
        }
    }

    /// Evicts `T ≤ t-range` from the window front (discharging retention),
    /// then reduces the window and records a point (empty ⇒ gap).
    fn emit_at(&mut self, t: i64, retained: &mut u64) {
        // `checked_sub` (issue #227 review round 11): for `t` near `i64::MIN`
        // (or a `range` ≫ the whole window) the logical eviction bound
        // `t-range` sits BELOW the representable domain — the window
        // `(t-range, t]` then covers every stored ts ≤ t and there is
        // NOTHING to evict. The prior saturating form clamped the bound to
        // `i64::MIN` and the `ts <= lo` eviction wrongly dropped a sample
        // stored at exactly `i64::MIN`, which the reference includes. A
        // legitimately-computed `lo == i64::MIN` (no underflow) still
        // evicts a sample at exactly that timestamp — exclusive semantics
        // are lost only when the bound is genuinely sub-domain.
        if let Some(lo) = t.checked_sub(self.range) {
            while let Some(front) = self.win.front() {
                if front.ts <= lo {
                    let ev = self.win.pop_front().expect("front present");
                    // Symmetric discharge (the concurrent-retention
                    // invariant): every evicted sample was charged on load in
                    // the same `per_sample` unit, so this is exact; the
                    // saturating form is panic-proofing, never a masked
                    // accounting path (class-A values are `1.0` or a
                    // non-negative `line.len()`, added and removed once each).
                    discharge_retention(retained, self.per_sample);
                    if matches!(self.class, ReducerClass::InvertInteger) {
                        self.run_int = self.run_int.saturating_sub(ev.value as u64);
                    }
                } else {
                    break;
                }
            }
        }
        // Class A (integer invert) needs only the running value + emptiness —
        // no per-emit slice materialization. Class B/C re-reduce the window
        // over a BORROW: `make_contiguous` rotates the deque in place and
        // hands back `&mut [T]`, so the re-reduction ALLOCATES NOTHING (issue
        // #227 review round 5, finding 2). The previous `reduce_buf` copied
        // the whole retained window every step, doubling peak memory for an
        // uncharged duplicate; removing the copy is strictly better than
        // charging it. Fields are copied out first so the `&mut self.win`
        // borrow stays disjoint.
        let (op, class, param, rate_window_ns, run_int) = (
            self.op,
            self.class,
            self.param,
            self.rate_window_ns,
            self.run_int,
        );
        let v = if matches!(class, ReducerClass::InvertInteger) {
            if self.win.is_empty() {
                None
            } else {
                // A non-empty marker slice — class A ignores its contents.
                reduce_window(
                    op,
                    class,
                    param,
                    rate_window_ns,
                    run_int,
                    std::slice::from_ref(&WIN_SAMPLE_MARKER),
                )
            }
        } else {
            let window = self.win.make_contiguous();
            reduce_window(op, class, param, rate_window_ns, run_int, window)
        };
        if let Some(v) = v {
            self.points.push((t, v));
        }
    }

    /// Loads one collision group (already `tie_rank`-ranked in `members`) at
    /// `ts`: emits grid points `< ts` first, then charges each member into the
    /// window. Reads values straight from the buffer (no intermediate `Vec`).
    fn load_group(
        &mut self,
        ts: i64,
        members: &[CollMember],
        retained: &mut u64,
        cap: u64,
    ) -> Result<(), ReadError> {
        self.emit_until(ts, retained);
        for (rank, m) in members.iter().enumerate() {
            let value = m.value;
            // Size → check the cap → allocate, through the ONE gate (issue
            // #227 review round 5, finding 2): `push_back` may grow the
            // deque, so the cap must refuse before the push, not after it.
            charge_retention(retained, self.per_sample, cap)?;
            self.win.push_back(WinSample {
                ts,
                stream_hash: self.stream_hash,
                tie_rank: rank as u32,
                value,
            });
            if matches!(self.class, ReducerClass::InvertInteger) {
                self.run_int += value as u64;
            }
        }
        Ok(())
    }

    /// Drains the remaining grid points at fingerprint close.
    fn finish(mut self, retained: &mut u64) -> Option<MatrixSeries> {
        while self.next_k <= self.kmax {
            let t = self.grid_point(self.next_k);
            self.emit_at(t, retained);
            self.next_k += 1;
        }
        // Discharge whatever is still retained (the series is closed), in
        // the same `per_sample` unit it was charged in.
        discharge_retention(retained, self.win.len() as u64 * self.per_sample);
        if self.points.is_empty() {
            None
        } else {
            Some(MatrixSeries {
                labels: self.labels,
                points: self.points,
            })
        }
    }
}

/// One class-A grid cell's DELTA (issue #236 Part C, C1): a sample
/// covering `[k_lo, k_hi]` records `(+value, +1)` at `k_lo` and
/// `(-value, -1)` at `k_hi + 1`, and the covered cells are recovered by
/// prefix-summing ascending. Two map touches per sample instead of one
/// per covered cell — the same difference-array trick `present_cover`
/// already uses for `absent_over_time`.
///
/// `dcount` cannot fold into `dvalue`: `bytes_over_time` over an empty
/// line contributes value 0 to a cell that is nonetheless COVERED and
/// must emit `0`, which only a separate coverage count distinguishes from
/// a gap.
///
/// Both fields are `i64` and the arithmetic is exact — class A is the
/// invert-INTEGER class, so the running value is integral end to end and
/// `reduce_int_cell` is the only float conversion. Neither can overflow:
/// each surviving sample contributes one `+`/`-` pair, `|value|` is
/// either 1 or a line length, and the scan is hard-bounded by
/// `LOGQL_SCAN_BUDGET_BYTES_CEILING` bytes — so both running totals are
/// bounded by the scanned byte count, itself `< 2^63` (the same structural
/// argument `present_cover`'s counter width rests on).
#[derive(Debug, Clone, Copy, Default)]
struct IntDelta {
    dvalue: i64,
    dcount: i64,
}

/// How ONE mutating output group accumulates. Which arm a query uses is
/// fixed once, at state construction, by [`mut_cells_for`] — never per
/// group and never per sample — so a group cannot hold a mixed
/// representation and the arms cannot interleave.
#[derive(Debug)]
enum MutCells {
    /// **Class A, non-overlapping windows.** One entry per COVERED grid
    /// cell, accumulated in place. Kept VERBATIM through issue #236 Part
    /// C: each sample then covers exactly one cell, so this is already
    /// `O(1)` per sample, it charges ONE retention point where the delta
    /// form would charge two, and it collapses repeated samples in the
    /// same cell into that single entry.
    IntExpanded(HashMap<i64, u64>),
    /// **Class A, overlapping windows.** A difference array over grid
    /// indices ([`IntDelta`]), prefix-summed at finish — two map touches
    /// per sample instead of `ceil(range/step)`.
    IntDeltas(HashMap<i64, IntDelta>),
    /// **Classes B/C.** Every surviving sample, retained ONCE; the
    /// covering cells are recovered at finish by sorting with
    /// [`win_order`] and sweeping two pointers over ascending grid
    /// indices. Retention is `O(samples)` and INDEPENDENT of
    /// `ceil(range/step)`, where the previous per-cell map retained one
    /// copy of the sample per covering cell.
    Samples(Vec<WinSample>),
}

impl MutCells {
    /// Retained entries, in the same unit [`charge_retention`] charged
    /// them in — one point per created class-A entry, `per_sample` points
    /// per retained class-B/C sample (the caller multiplies).
    ///
    /// Test-only observability, like [`VectorAggFold::cells`]: production
    /// code charges and discharges through the counter itself, so
    /// exposing a second way to ask would invite the two to drift.
    #[cfg(test)]
    fn charged_units(&self) -> u64 {
        match self {
            MutCells::IntExpanded(m) => m.len() as u64,
            MutCells::IntDeltas(m) => m.len() as u64,
            MutCells::Samples(v) => v.len() as u64,
        }
    }

    /// Test-only, as [`MutCells::charged_units`].
    #[cfg(test)]
    fn is_empty(&self) -> bool {
        match self {
            MutCells::IntExpanded(m) => m.is_empty(),
            MutCells::IntDeltas(m) => m.is_empty(),
            MutCells::Samples(v) => v.is_empty(),
        }
    }
}

/// One mutating/regrouping output group's fan-out state.
#[derive(Debug)]
pub(in crate::logql) struct MutGroup {
    labels: LabelSet,
    cells: MutCells,
}

/// The sliding-window range evaluator (issue #227).
#[derive(Debug)]
pub(in crate::logql) struct RangeSlideState<'q> {
    compiled: &'q super::pipeline::CompiledPipeline,
    op: RangeAggOp,
    class: ReducerClass,
    value_kind: ClientValue,
    param: Option<f64>,
    rate_window_ns: Option<u64>,
    grid_start: i64,
    step: u64,
    range: i64,
    kmax: i64,
    /// The `offset` shift this state's grid was ALREADY moved back by
    /// (issue #343). Every internal comparison — `covering_k`, the
    /// eviction bound, `emit_until`'s boundary — runs in the shifted
    /// domain against the raw sample timestamps, which is exactly what
    /// makes the window `(t-offset-range, t-offset]`; the shift is added
    /// back ONCE, to the emitted point timestamps, in
    /// [`RangeSlideState::finish_in_place`].
    offset_ns: i64,
    fan_out: bool,
    is_absent: bool,
    /// The range aggregation's own `by`/`without` (issue #344), borrowed
    /// from the plan for the state's lifetime and handed to
    /// [`super::pipeline::CompiledPipeline::run_metric_into`] per row —
    /// the reference applies it inside the sample extractor too, which is
    /// what makes the merge run over the group's RAW SAMPLES rather than
    /// over per-series results.
    grouping: Option<&'q RangeGrouping>,
    /// Whether the reducer is order-DEPENDENT (class C, or first/last) and so
    /// needs the full-body `tie_rank` order within a collision group. When
    /// false (count/bytes/rate-no-unwrap, min/max/quantile) the body is never
    /// retained — no per-row clone, no per-group sort (the alloc-gate path).
    needs_body_order: bool,
    absent_labels: LabelSet,
    pub(in crate::logql) base_labels: HashMap<u64, LabelSet>,
    hashes: HashMap<u64, u64>,
    /// Concurrent retained-point count (charge-on-load / discharge-on-evict),
    /// gated by [`charge_retention`]/[`discharge_retention`].
    retained: u64,
    /// Retention points one retained sample costs
    /// ([`retention_points_per_sample`]) — shared by the streaming sliders
    /// and the mutating fan-out cells so both charge in the same unit.
    per_sample: u64,
    /// Every retention cap this state checks (issue #221): the
    /// concurrent-retention cap, the query-lifetime group-byte cap, the
    /// series cap and the collision-staging caps, in ONE field — always
    /// [`AggCaps::DEFAULT`] in production single-extractor use (the former
    /// `retention_cap`/`group_bytes_cap` test-seam precedent); a
    /// `variants(...)` query hands each sub-state
    /// `AggCaps::DEFAULT.divided(n)` so the per-field SUM over sub-states
    /// is exactly the single-query bound.
    caps: AggCaps,
    // Non-mutating.
    cur: Option<FpSlide>,
    cur_fp: u64,
    series_out: Vec<MatrixSeries>,
    // Mutating.
    groups: HashMap<String, MutGroup>,
    /// `absent_over_time`'s selector-wide presence, as a **grid-sized
    /// difference array** (issue #227 review round 1 finding 1): index `k`
    /// holds the coverage delta at grid point `k`, prefix-summed once at
    /// finish. Length is `kmax + 2` — O(grid), capped by
    /// `MAX_CLIENT_AGG_BUCKETS`, and completely independent of scan density
    /// (nothing per-sample is kept).
    ///
    /// **Counter width proof (review round 2 finding 1).** Each surviving
    /// collision GROUP contributes exactly `+1` at one index and `-1` at
    /// another, so `|counter| <= number of surviving collision groups <=
    /// number of scanned rows`. The scan is hard-bounded by
    /// `reader.logql_scan_budget_bytes` (`max_bytes_to_read`, ceiling
    /// `LOGQL_SCAN_BUDGET_BYTES_CEILING`), and every `log_samples` row costs
    /// at least one byte, so
    /// `rows <= LOGQL_SCAN_BUDGET_BYTES_CEILING < 2^63 = i64::MAX`.
    /// An `i64` counter therefore **cannot** overflow for any query the
    /// engine will run — whereas the previous `i32` (max 2.1e9) was genuinely
    /// reachable under a multi-GiB budget. No cap or checked arithmetic is
    /// needed; the bound is structural, and
    /// `absent_presence_counter_cannot_overflow_under_the_scan_budget` pins
    /// the arithmetic.
    present_cover: Vec<i64>,
    // Current collision run.
    coll_active: bool,
    coll_fp: u64,
    coll_ts: i64,
    coll: Vec<CollMember>,
    /// Bytes of body currently staged in `coll` — charged BEFORE each clone
    /// and reset whenever the group is flushed, so the staged buffer is
    /// bounded by `MAX_TS_COLLISION_GROUP_BYTES` by construction (issue #227
    /// review round 3, finding 1).
    coll_bytes: u64,
    /// QUERY-LIFETIME bytes retained by `groups` — each entry's rendered
    /// key + `LabelSet` + map-slot share ([`group_entry_bytes`]), gated by
    /// [`charge_group_bytes`] BEFORE the insertion that retains them and
    /// released as `finish_in_place` consumes each entry (issue #227 review
    /// round 6: these bytes outlive the collision flush that resets
    /// `coll_bytes`, so they need their own counter). Between the flush's
    /// `coll_bytes` reset and each member's charge-or-drop, the group's
    /// staged members are transiently outside both counters — a transient
    /// bounded by [`MAX_TS_COLLISION_GROUP_BYTES`] (one group staged at a
    /// time) and freed within the same flush.
    group_bytes: u64,
    /// Issue #236: RESULT point-slots reserved by this state, through
    /// [`charge_result_points`]. One grid's width per output series, at
    /// the moment the series is created — `O(1)`, never per point. An
    /// ADMISSION counter: it is never discharged, because the points it
    /// reserves are the result.
    result_points: u64,
    /// The innermost vector aggregation, applied AS this state emits
    /// (issue #236 Part B) instead of over its materialised output.
    /// `None` — the state's own construction default — is the
    /// materialising path this leaf has always taken; it is attached by
    /// [`RangeSlideState::attach_fold`] after construction, so the
    /// constructor's shape (and its allocation census) is unchanged.
    fold: Option<VectorAggFold>,
}

impl<'q> RangeSlideState<'q> {
    pub(in crate::logql) fn new(
        compiled: &'q super::pipeline::CompiledPipeline,
        meta: &HashMap<u64, StreamMetaRow>,
        client: &'q ClientAgg,
        window: ClientWindow,
        rate_window_ns: Option<u64>,
        caps: AggCaps,
    ) -> Result<Self, ReadError> {
        // Destructuring the `Range` variant is what makes the range
        // GUARANTEED present and validated (issue #227 review round 4,
        // finding 2): there is no zero/absent range to receive.
        let ClientWindow::Range {
            grid_start_ns: grid_start,
            end_ns,
            step_ns,
            range_ns,
            offset_ns,
        } = window
        else {
            unreachable!("RangeSlideState is constructed only for a Range window")
        };
        let step = step_ns.as_u64();
        // Loki-exact resolution rule (review round 7, finding 1): reject on
        // `intervals > 11000`, NOT on the point count — the inclusive grid
        // of an exactly-at-the-limit request holds 11_001 points and the
        // reference serves it.
        let count = ensure_grid_resolution(grid_start, end_ns, step)?;
        // `count == kmax + 1` (0 ⇒ empty grid, kmax = -1).
        let kmax = count as i64 - 1;
        let mut base_labels: HashMap<u64, LabelSet> = HashMap::new();
        let mut hashes: HashMap<u64, u64> = HashMap::new();
        for (fp, m) in meta {
            let labels = series_labels(m);
            hashes.insert(*fp, stream_hash(&labels));
            base_labels.insert(*fp, labels);
        }
        let op = client.range_op;
        let class = reducer_class(op, client.value);
        let needs_body_order = matches!(class, ReducerClass::CanonicalFold)
            || matches!(op, RangeAggOp::FirstOverTime | RangeAggOp::LastOverTime);
        let is_absent = matches!(op, RangeAggOp::AbsentOverTime);
        // Issue #344: a grouping MERGES streams, so it must take the
        // fan-out path — which groups by the final (already-projected)
        // label set — never the per-fingerprint sliders, which cannot
        // merge across fingerprints at all. Today the disjunct is
        // subsumed (every op that admits a grouping requires `unwrap`, so
        // `metric_mutates_labels()` is already true), and it is spelled
        // out anyway: without it, a grouping-allowed op that stopped
        // requiring unwrap would silently return the per-stream answer.
        let fan_out = compiled.metric_mutates_labels() || client.grouping.is_some();
        Ok(RangeSlideState {
            compiled,
            op,
            class,
            value_kind: client.value,
            param: client.param,
            rate_window_ns,
            grid_start,
            step,
            // No narrowing: `range_ns` is already a boundary-validated,
            // in-domain `i64` (issue #227 review round 2, finding 2).
            range: range_ns.get(),
            kmax,
            offset_ns,
            fan_out,
            is_absent,
            grouping: client.grouping.as_deref(),
            needs_body_order,
            absent_labels: client.absent_labels.clone(),
            base_labels,
            hashes,
            retained: 0,
            per_sample: retention_points_per_sample(op),
            caps,
            cur: None,
            cur_fp: 0,
            series_out: Vec::new(),
            groups: HashMap::new(),
            // `kmax + 2` slots (`kmax + 1` grid points plus the exclusive
            // upper delta index). `kmax` passed the 11k grid guard above, so
            // this allocation is bounded before any row is read — and it is
            // made ONLY for `absent_over_time` (issue #221): `present_cover`
            // is written only under `is_absent` and read only in
            // `finish_absent`, reached under the same flag, so every other
            // reducer keeps an empty (allocation-free) vec. The gate is the
            // branch-free `* (is_absent as usize)` multiplier (a zero-length
            // `vec![0; 0]` never allocates) so this constructor's censused
            // branch shape is unchanged.
            present_cover: vec![0; (kmax.max(-1) + 2) as usize * (is_absent as usize)],
            coll_active: false,
            coll_fp: 0,
            coll_ts: 0,
            coll: Vec::new(),
            coll_bytes: 0,
            group_bytes: 0,
            result_points: 0,
            fold: None,
        })
    }

    /// The grid this state emits on — the fold indexes its dense slots by
    /// the same triple, so a slot and a grid point are two views of one
    /// value.
    fn grid(&self) -> FoldGrid {
        FoldGrid {
            start: self.grid_start,
            step: self.step,
            kmax: self.kmax,
        }
    }

    /// Hands the INNERMOST vector aggregation to the leaf (issue #236
    /// Part B). A no-op for the specs the leaf cannot own
    /// ([`VectorAggFold::new`] returns `None`), which is why
    /// [`RangeSlideState::folded_aggs`] — not the caller's intent —
    /// decides how many specs the caller must still apply.
    ///
    /// Attached AFTER `new` rather than taken as a constructor parameter:
    /// the grid is only known once `ensure_grid_resolution` has run, and
    /// keeping it out of `new` leaves that constructor's branch/allocation
    /// census (issue #221 `logql_variants_alloc`) untouched.
    pub(in crate::logql) fn attach_fold(&mut self, spec: &plan::VectorAggSpec) {
        self.fold = VectorAggFold::new(
            spec,
            self.grid(),
            self.caps.result_points,
            self.caps.group_bytes,
        );
    }

    /// How many trailing (innermost) specs this leaf has taken over: 0 or
    /// 1. The caller applies the remaining prefix.
    pub(in crate::logql) fn folded_aggs(&self) -> usize {
        usize::from(self.fold.is_some())
    }

    /// Folds one batch of (physical-key-ordered) rows: runs the pipeline,
    /// fails on a surviving `__error__`, and buffers same-`(fingerprint,
    /// timestamp_ns)` runs for full-body ranking before loading them.
    ///
    /// `base_labels` is moved to a LOCAL for the duration so the batch-reused
    /// `scratch` (whose `Cow`s borrow it) does not tie a `self` borrow across
    /// the mid-loop `&mut self` `flush_collision` — that keeps the fold's
    /// per-row allocation at ZERO (the alloc-gate discipline), one shared
    /// scratch across the whole batch.
    pub(in crate::logql) fn push_rows(&mut self, rows: &[MetricScanRow]) -> Result<(), ReadError> {
        let base_labels = std::mem::take(&mut self.base_labels);
        let grouping = self.grouping;
        let mut scratch: Vec<(Cow<'_, str>, Cow<'_, str>)> = Vec::new();
        let mut result = Ok(());
        for row in rows {
            // `base_labels` is a LOCAL, so the reused `scratch` (whose `Cow`s
            // borrow it) does not tie a `self` borrow across this `&mut self`
            // flush — the fold stays zero-alloc-per-row with one shared
            // scratch across the batch.
            if self.coll_active
                && (self.coll_fp != row.fingerprint || self.coll_ts != row.timestamp_ns)
                && let Err(e) = self.flush_collision(&base_labels)
            {
                result = Err(e);
                break;
            }
            let Some(base) = base_labels.get(&row.fingerprint) else {
                continue;
            };
            let (line, value) = match self.compiled.run_metric_into(
                &row.body,
                base,
                row.timestamp_ns,
                grouping,
                &mut scratch,
            ) {
                Ok(MetricRun::Dropped) => continue,
                Ok(MetricRun::Kept { line, value }) => (line, value),
                Err(e) => {
                    result = Err(e.into());
                    break;
                }
            };
            if let Err(e) = check_surviving_error(&scratch) {
                result = Err(e);
                break;
            }
            let v = match self.value_kind {
                ClientValue::Count => 1.0,
                ClientValue::Bytes => line.len() as f64,
                ClientValue::Unwrap => match value {
                    Some(v) => v,
                    None => continue,
                },
            };
            self.coll_active = true;
            self.coll_fp = row.fingerprint;
            self.coll_ts = row.timestamp_ns;
            // THE SINGLE STAGING FUNNEL (issue #227 review round 4, finding
            // 1): every per-member allocation — body clone, rendered label
            // JSON, cloned `LabelSet` — is sized, capped, THEN allocated in
            // one place. No reducer class has a path that stages anything
            // outside it.
            if let Err(e) = self.stage_member(row, v, &mut scratch) {
                result = Err(e);
                break;
            }
        }
        self.base_labels = base_labels;
        result
    }

    /// **The single staging funnel** — the ONLY place a per-member
    /// allocation enters `coll` (issue #227 review round 4, finding 1).
    ///
    /// Order is load-bearing: (i) size every allocation this member will
    /// make, (ii) check BOTH caps, (iii) only then allocate. Because the
    /// sizing happens first, the allocation that would breach a cap is never
    /// performed, for **every** reducer class — the previous code sized only
    /// the body (and only when `needs_body_order`), leaving the fan-out
    /// `key`/`LabelSet` clones uncharged.
    ///
    /// The charge is an UPPER BOUND on the heap bytes about to be allocated
    /// (see [`Self::member_stage_bytes`]), so `coll_bytes >= actual staged
    /// heap bytes` always, and `coll_bytes <= MAX_TS_COLLISION_GROUP_BYTES`
    /// is enforced — hence actual staged bytes are capped too.
    fn stage_member(
        &mut self,
        row: &MetricScanRow,
        value: f64,
        scratch: &mut Vec<(Cow<'_, str>, Cow<'_, str>)>,
    ) -> Result<(), ReadError> {
        let stages_out = self.fan_out && !self.is_absent;
        if stages_out {
            // In-place sort, no allocation — safe to do before the caps.
            scratch.sort_unstable();
        }
        // (i) size EVERY allocation this member will make.
        let bytes = self.member_stage_bytes(row, scratch, stages_out);
        // (ii) both caps, BEFORE any allocation. `saturating_add` cannot mask
        // a breach (saturation only grows the sum) but does keep a
        // pathological charge from overflowing the comparison itself.
        let next_count = self.coll.len() as u64 + 1;
        let next_bytes = self.coll_bytes.saturating_add(bytes);
        if next_count > self.caps.collision_members || next_bytes > self.caps.collision_bytes {
            return Err(ReadError::QueryTooBroad(TooBroadReason::TsCollisionGroup {
                count: next_count,
                cap: self.caps.collision_members,
                bytes: next_bytes,
                bytes_cap: self.caps.collision_bytes,
            }));
        }
        // (iii) now — and only now — allocate.
        let body = if self.needs_body_order {
            row.body.clone()
        } else {
            String::new()
        };
        let out = if stages_out {
            let key = render_labels_json_sorted(scratch);
            let labels: LabelSet = scratch
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            Some((key, labels))
        } else {
            None
        };
        self.coll_bytes = next_bytes;
        self.coll.push(CollMember { body, value, out });
        Ok(())
    }

    /// A provable UPPER BOUND on the heap bytes [`Self::stage_member`] is
    /// about to allocate for one member. Charging an over-estimate is what
    /// makes the cap sound: `coll_bytes >= actual`, so bounding `coll_bytes`
    /// bounds the real footprint.
    ///
    /// **Per allocation** (issue #227 review round 5, finding 1 — the
    /// round-4 version counted the JSON content ONCE and charged a flat 32
    /// bytes per container slot, both of which an adversarial input beats):
    /// - **rendered label JSON** ([`render_labels_json_sorted`]) — sized by
    ///   [`rendered_labels_json_len`], which walks the SAME escape arms as
    ///   [`push_json_string`] and so yields the rendered length EXACTLY,
    ///   including the `\u00xx` six-bytes-for-one expansion of a C0 control
    ///   byte that the previous charge missed. Sizing (rather than assuming a
    ///   6× worst case) also means an ordinary multi-hundred-KiB label value
    ///   is not rejected six times too early. The `String` is pre-sized with
    ///   an estimate that assumes no escaping, so escaping-heavy input forces
    ///   geometric growth — charged through [`grown_alloc_bytes`].
    /// - **cloned `LabelSet`** — one exactly-reserved `String` per key and
    ///   per value, plus one exactly-reserved element buffer of `pairs ×
    ///   size_of::<(String, String)>()` (the `collect` runs from a
    ///   `TrustedLen` iterator). Each is charged through
    ///   [`alloc_block_bytes`] — allocator size-class rounding covered,
    ///   floored at the allocator's minimum block — so a label set of many
    ///   one-byte values cannot cost more than its charge.
    /// - **body clone** — `String::clone` reserves exactly `row.body.len()`
    ///   in one allocation; charged through [`alloc_block_bytes`], only when
    ///   `needs_body_order`.
    /// - **`CollMember` slot in `coll`** — a `Vec` push can DOUBLE capacity
    ///   with the old buffer still live during the realloc, so for `n`
    ///   members the live request is `≤ 2n` slots plus an old buffer of
    ///   `≤ n` slots; with each block retained at ≤ 2× its request (the
    ///   [`alloc_block_bytes`] model, review round 7 finding 2) the peak is
    ///   `≤ 6n × size_of::<CollMember>()`, and the initial
    ///   `min_non_zero_cap = 4` block is `≤ 8 × size_of::<CollMember>()`.
    ///   Charging `8 × size_of::<CollMember>()` per member dominates BOTH
    ///   for every `n ≥ 1`. The `String`/`Vec` headers of the pieces above
    ///   live INSIDE that slot, so they are already covered.
    ///
    /// Saturating arithmetic throughout, so a hostile label value cannot
    /// wrap the charge round to a small number — saturation can only make
    /// the charge larger, which is the safe direction.
    fn member_stage_bytes(
        &self,
        row: &MetricScanRow,
        scratch: &[(Cow<'_, str>, Cow<'_, str>)],
        stages_out: bool,
    ) -> u64 {
        /// See the `CollMember`-slot bullet above: 8× per element dominates
        /// the ≤ 6n-slot realloc peak (2n live + n old, each ≤ 2×-rounded)
        /// and the first member's `min_non_zero_cap = 4` block.
        const VEC_GROWTH_FACTOR: u64 = 8;

        let mut bytes = (size_of::<CollMember>() as u64).saturating_mul(VEC_GROWTH_FACTOR);

        if self.needs_body_order {
            bytes = bytes.saturating_add(alloc_block_bytes(row.body.len() as u64));
        }

        if stages_out {
            // The rendered JSON, sized exactly, then grown.
            bytes = bytes.saturating_add(grown_alloc_bytes(rendered_labels_json_len(scratch)));
            // The cloned `LabelSet`: one owned `String` per key and per
            // value, each floored at the allocator's minimum block.
            for (k, v) in scratch {
                bytes = bytes
                    .saturating_add(alloc_block_bytes(k.len() as u64))
                    .saturating_add(alloc_block_bytes(v.len() as u64));
            }
            // ...plus its exactly-reserved element buffer.
            let elems = (scratch.len() as u64).saturating_mul(size_of::<(String, String)>() as u64);
            bytes = bytes.saturating_add(alloc_block_bytes(elems));
        }

        bytes
    }

    /// Ranks and dispatches the buffered collision group (full-body order ⇒
    /// deterministic `tie_rank`), then releases the bodies.
    fn flush_collision(&mut self, base_labels: &HashMap<u64, LabelSet>) -> Result<(), ReadError> {
        if self.coll.is_empty() {
            self.coll_active = false;
            return Ok(());
        }
        let fp = self.coll_fp;
        let ts = self.coll_ts;
        self.coll_active = false;
        // The staged bodies are released with this group (every exit path
        // below either clears or takes `coll`), so the byte charge resets
        // here — one group is staged at a time, which is what makes
        // `MAX_TS_COLLISION_GROUP_BYTES` a true bound on the staged buffer.
        // The fan-out `key`/`LabelSet` that SURVIVE the flush into the
        // query-lifetime `groups` map are re-charged against
        // `MAX_CLIENT_AGG_GROUP_BYTES` before that insertion
        // (`fan_out_sample`, issue #227 review round 6), so nothing escapes
        // both counters — the only uncounted state is this flush's own
        // staged members, bounded by the collision cap and freed before it
        // returns.
        self.coll_bytes = 0;
        // A true total order over the group (order-dependent reducers only):
        // distinct bodies always differ; byte-identical bodies are genuinely
        // interchangeable. Skipped entirely for the common order-independent
        // reducers (which never retained a body).
        if self.needs_body_order {
            self.coll
                .sort_by(|a, b| a.body.as_bytes().cmp(b.body.as_bytes()));
        }

        if self.is_absent {
            // Issue #227 review finding 1: presence is recorded as O(GRID)
            // COVERAGE, never an O(scan) timestamp vector. One surviving
            // sample at `ts` makes every grid point in `[k_lo, k_hi]`
            // non-empty; a difference array records that in O(1) per group
            // and is prefix-summed once at finish. Memory is bounded by the
            // grid (already capped at `MAX_CLIENT_AGG_BUCKETS`) regardless of
            // scan density — nothing per-sample is retained, so no
            // client-supplied range/step/cardinality can grow it.
            if !self.coll.is_empty() {
                let (k_lo, k_hi) = self.covering_k(ts);
                if k_lo <= k_hi {
                    self.present_cover[k_lo as usize] += 1;
                    self.present_cover[k_hi as usize + 1] -= 1;
                }
            }
            self.coll.clear();
            return Ok(());
        }

        let stream_hash = self.hashes.get(&fp).copied().unwrap_or(0);
        if self.fan_out {
            // The fan-out path moves `out` out of each member, so it consumes
            // the buffer (the mutating path is the already-expensive class);
            // `coll` re-grows on the next group.
            let members = std::mem::take(&mut self.coll);
            for (rank, m) in members.into_iter().enumerate() {
                let (key, labels) = m.out.expect("mutating member carries its output set");
                self.fan_out_sample(key, labels, ts, stream_hash, rank as u32, m.value)?;
            }
            return Ok(());
        }

        // Non-mutating: one slider per fingerprint (fingerprints contiguous).
        if self.cur.is_none() || self.cur_fp != fp {
            if let Some(prev) = self.cur.take() {
                self.rotate_slider(prev);
            }
            // Issue #236 P2 — the premise fix on the non-mutating RANGE
            // path. Part A deleted the mid-scan group-count rejection
            // that used to stand here, and the `labels` clone
            // below plus the eventual `series_out` element were otherwise
            // uncharged. Charged BEFORE the clone, so a refusal never
            // allocates; discharged in the slider rotation's `None` arm
            // (a series that yields no points) and at the end of
            // `finish_in_place`'s non-mutating arm.
            let src = base_labels.get(&fp);
            charge_group_bytes(
                &mut self.group_bytes,
                group_entry_bytes("", src.unwrap_or(&EMPTY_LABEL_SET), SERIES_OUT_SLOT),
                self.caps.group_bytes,
            )?;
            // Issue #236: this slider will emit at most one point per
            // grid point, so the grid's width is reserved once, here,
            // before the slider exists — `O(1)`, and in the same units
            // `MAX_METRIC_RESULT_POINTS` is derived in.
            charge_result_points(
                &mut self.result_points,
                grid_slot_count(self.kmax),
                self.caps.result_points,
            )?;
            let labels = src.cloned().unwrap_or_default();
            self.cur = Some(FpSlide {
                stream_hash,
                labels,
                op: self.op,
                class: self.class,
                param: self.param,
                rate_window_ns: self.rate_window_ns,
                grid_start: self.grid_start,
                step: self.step,
                range: self.range,
                kmax: self.kmax,
                next_k: 0,
                win: VecDeque::new(),
                run_int: 0,
                per_sample: self.per_sample,
                points: Vec::new(),
            });
            self.cur_fp = fp;
        }
        // Disjoint field borrows (`cur`, `coll`, `retained`) — the slider
        // reads values straight from the buffer, no intermediate `Vec`; the
        // buffer's capacity is reused (cleared, never reallocated per group).
        let cur = self.cur.as_mut().expect("slider just set");
        cur.load_group(
            ts,
            &self.coll,
            &mut self.retained,
            self.caps.retention_points,
        )?;
        self.coll.clear();
        Ok(())
    }

    /// Retires the finished non-mutating slider — issue #236 P2's discharge
    /// leg. A slider that produced points hands its `MatrixSeries` to
    /// `series_out` and KEEPS its charge (the bytes are still live) until
    /// `finish_in_place` releases the whole vector; a slider that produced
    /// NONE is dropped here, so its charge is released here. Sized over the
    /// same labels P2 charged (`FpSlide::labels` is `base_labels`' value,
    /// cloned and never mutated), which is what makes `group_bytes == 0` at
    /// finish an exact identity rather than an inequality.
    fn rotate_slider(&mut self, prev: FpSlide) {
        let entry_bytes = group_entry_bytes("", &prev.labels, SERIES_OUT_SLOT);
        match prev.finish(&mut self.retained) {
            Some(series) => self.series_out.push(series),
            None => discharge_group_bytes(&mut self.group_bytes, entry_bytes),
        }
    }

    /// Fans one ranked sample into every grid cell whose window `(t-range,
    /// t]` covers `ts` (the covering-set identity `t ∈ [ts, ts+range)`).
    fn fan_out_sample(
        &mut self,
        key: String,
        labels: LabelSet,
        ts: i64,
        stream_hash: u64,
        tie_rank: u32,
        value: f64,
    ) -> Result<(), ReadError> {
        let (k_lo, k_hi) = self.covering_k(ts);
        if k_lo > k_hi {
            return Ok(());
        }
        let is_new = !self.groups.contains_key(&key);
        if is_new {
            // Issue #227 review round 6: the key + `LabelSet` MOVE into the
            // QUERY-LIFETIME `groups` map below and outlive the collision
            // flush (whose `coll_bytes` charge has already been reset), so
            // their bytes are charged to the query-lifetime counter BEFORE
            // the insertion that retains them — refused means never
            // inserted, for any label size and any group count.
            charge_group_bytes(
                &mut self.group_bytes,
                group_entry_bytes(&key, &labels, MUT_GROUP_SLOT),
                self.caps.group_bytes,
            )?;
            // Issue #236: same reservation as the non-mutating slider —
            // one grid width per output group, once, before the group
            // exists.
            charge_result_points(
                &mut self.result_points,
                grid_slot_count(self.kmax),
                self.caps.result_points,
            )?;
        }
        let per_sample = self.per_sample;
        let cap = self.caps.retention_points;
        let empty_cells = mut_cells_for(self.class, self.range, self.step, self.kmax);
        let retained = &mut self.retained;
        let group = self.groups.entry(key).or_insert_with(|| MutGroup {
            labels,
            cells: empty_cells,
        });
        // Review finding 3: a newly-CREATED entry is charged to the same
        // concurrent-retention counter as a retained point, so the
        // mutating fan-out obeys the documented invariant instead of
        // relying on an implicit `groups × grid` product — which issue
        // #236 has since deleted outright (there is no group-count cap
        // left to form one). Updating an EXISTING entry is O(1) and
        // charges nothing. Size → check the cap → allocate, through the
        // ONE gate (issue #227 review round 5, finding 2): an insert may
        // grow the map, so the cap must refuse before it, not after.
        //
        // ONE point per entry on the class-A arms: an entry is a pair of
        // integers, narrower than the `WinSample` the unit is defined by,
        // and class A never re-reduces over a scratch (`per_sample == 1`
        // for every integer op anyway).
        match &mut group.cells {
            // Issue #236 Part C, C1: two touches per SAMPLE — the deltas
            // at `k_lo` and at the exclusive `k_hi + 1` — instead of one
            // per covered cell. `k_hi + 1` may be `kmax + 1`, which is a
            // delta index only and is never emitted.
            MutCells::IntDeltas(cells) => {
                for (k, dv, dc) in [(k_lo, value as i64, 1i64), (k_hi + 1, -(value as i64), -1)] {
                    match cells.entry(k) {
                        std::collections::hash_map::Entry::Occupied(mut e) => {
                            let d = e.get_mut();
                            d.dvalue += dv;
                            d.dcount += dc;
                        }
                        std::collections::hash_map::Entry::Vacant(e) => {
                            charge_retention(retained, 1, cap)?;
                            e.insert(IntDelta {
                                dvalue: dv,
                                dcount: dc,
                            });
                        }
                    }
                }
            }
            MutCells::IntExpanded(cells) => {
                for k in k_lo..=k_hi {
                    match cells.entry(k) {
                        std::collections::hash_map::Entry::Occupied(mut e) => {
                            *e.get_mut() += value as u64;
                        }
                        std::collections::hash_map::Entry::Vacant(e) => {
                            charge_retention(retained, 1, cap)?;
                            e.insert(value as u64);
                        }
                    }
                }
            }
            // Issue #236 Part C, C2: classes B/C retain each surviving
            // sample ONCE. The covering cells are recovered at finish by
            // a two-pointer sweep, so retention no longer scales with
            // `ceil(range/step)`.
            MutCells::Samples(samples) => {
                charge_retention(retained, per_sample, cap)?;
                samples.push(WinSample {
                    ts,
                    stream_hash,
                    tie_rank,
                    value,
                });
            }
        }
        Ok(())
    }

    /// The grid-index range `[k_lo, k_hi]` (clamped to `[0, kmax]`) of grid
    /// points whose window `(grid_t-range, grid_t]` covers `ts`:
    /// `grid_t-range < ts ≤ grid_t` ⟺ `ts ≤ grid_t < ts+range`.
    fn covering_k(&self, ts: i64) -> (i64, i64) {
        covering_k_of(ts, self.grid_start, self.step, self.range, self.kmax)
    }

    fn grid_point(&self, k: i64) -> i64 {
        // i128 intermediate + a SATURATING narrowing (issue #227 arithmetic
        // sweep): `k <= kmax` derives from `grid_point_count`, so every grid
        // point is in `[grid_start, end]` and the clamp never fires on a
        // real query — but a plain `as i64` would silently wrap rather than
        // saturate if that invariant were ever broken.
        clamp_bucket(self.grid_start as i128 + k as i128 * self.step as i128)
    }

    /// Finalizes into the query result, asserting the concurrent-retention
    /// invariant closed out (every charge discharged).
    ///
    /// **The single point where `offset` is added back** (issue #343).
    /// [`Self::finish_in_place`] has four exits — absent, fan-out folded,
    /// fan-out materialised, and the ordinary per-fingerprint path — and
    /// every one of them takes its timestamps from `grid_point`, i.e. in
    /// the SHIFTED domain. Shifting here rather than in `grid_point` is
    /// deliberate: that value doubles as the sliding window's own
    /// boundary (`emit_until`, the eviction bound), so moving it would
    /// move the WINDOW as well as the label on it. This is the type's
    /// completion point — `finish_in_place` exists only so the
    /// post-condition below stays observable to a test.
    fn finish(mut self) -> Result<QueryResult, ReadError> {
        let out = self.finish_in_place()?;
        debug_assert_eq!(
            self.retained, 0,
            "every retention charge must be discharged at finish"
        );
        debug_assert_eq!(
            self.group_bytes, 0,
            "every group-label byte charge must be discharged at finish"
        );
        Ok(shift_emitted_points(out, self.offset_ns))
    }

    /// The finish body, in place so the post-condition (`retained == 0`) is
    /// OBSERVABLE — `finish` consumes `self`, which made the release leg of
    /// the AC8 test unfalsifiable (issue #227 review round 3, finding 3).
    ///
    /// Emits on the SHIFTED grid: [`Self::finish`] is what puts the
    /// `offset` back (issue #343). The in-module tests that call this
    /// directly all drive offset-free windows, where the two agree.
    fn finish_in_place(&mut self) -> Result<QueryResult, ReadError> {
        // `base_labels` moved to a local (as in `push_rows`) so the final
        // `&mut self` flush does not clash with a `self.base_labels` borrow.
        let base_labels = std::mem::take(&mut self.base_labels);
        self.flush_collision(&base_labels)?;
        if let Some(prev) = self.cur.take() {
            self.rotate_slider(prev);
        }
        if self.is_absent {
            let series = self.finish_absent()?;
            return self.emit(series);
        }
        if self.fan_out {
            // **The reduction-order pin, extended to the fold** (issue
            // #236, task-manager ruling "Option B"). `groups` is a
            // `HashMap` under a per-process randomly-seeded hasher, so
            // draining it directly would hand the fold a member order
            // that varies run to run — and Welford, and float addition,
            // are order-SENSITIVE. Sorting by label set here is the same
            // total order `pin_reduction_order` applies immediately
            // before grouping on the materialising path, so the folded
            // and materialised values are the same bits, and both are
            // reproducible. One sort per stage, never per group.
            //
            // Applied whether or not a fold is attached — unlike
            // `emit`'s, which is inside the folding arm. A fan-out result
            // came out in `HashMap` walk order before, which no test
            // could assert and no user could rely on; making it
            // label-ascending is the same order the wire already carries
            // everywhere else.
            let mut groups: Vec<(String, MutGroup)> =
                std::mem::take(&mut self.groups).into_iter().collect();
            groups.sort_by(|(_, a), (_, b)| a.labels.cmp(&b.labels));
            let mut out: Vec<MatrixSeries> = Vec::new();
            for (key, group) in groups {
                // Round 6 symmetry: the discharge is sized over the SAME
                // (unmodified) key + labels the insertion charged — the key
                // is immutable as the map key and `MutGroup.labels` is never
                // touched after creation — so finish returns `group_bytes`
                // to exactly 0. Taken AFTER the entry is consumed below (key
                // dropped, labels moved into the output series), mirroring
                // the cell discharges: a charge is never released while the
                // memory it paid for is still owned by this state.
                let entry_bytes = group_entry_bytes(&key, &group.labels, MUT_GROUP_SLOT);
                drop(key);
                let (labels, points) = self.drain_group(group);
                if !points.is_empty() {
                    // Issue #236 Part B: the fold consumes each group AS
                    // it is built, so the `scanned groups x grid points`
                    // materialisation that `out` used to accumulate never
                    // exists — that is the whole point-axis win.
                    match self.fold.as_mut() {
                        Some(fold) => fold.push_series(&labels, &points)?,
                        None => out.push(MatrixSeries { labels, points }),
                    }
                }
                discharge_group_bytes(&mut self.group_bytes, entry_bytes);
            }
            return Ok(QueryResult::Matrix(match self.fold.take() {
                Some(fold) => fold.finish(),
                None => out,
            }));
        }
        // Issue #236 P2's other discharge leg: every series that reached
        // `series_out` still holds its charge, released as the vector
        // leaves the state. Sized over the same (never-mutated) labels the
        // charge used, so `group_bytes` returns to exactly 0.
        let out = std::mem::take(&mut self.series_out);
        for s in &out {
            discharge_group_bytes(
                &mut self.group_bytes,
                group_entry_bytes("", &s.labels, SERIES_OUT_SLOT),
            );
        }
        self.emit(out)
    }

    /// Drains ONE mutating output group's retained cells into its grid
    /// points, discharging the retention they were charged for (issue
    /// #236 Part C).
    ///
    /// A frame of its own rather than an inline block in
    /// `finish_in_place`: it holds all three [`MutCells`] arms, and the
    /// per-variant allocation census (`logql_variants_alloc`) pins a
    /// FUNCTION's body — folding it into the caller would have made one
    /// 24-branch frame, and hiding it behind an un-censused helper would
    /// have made its allocations invisible to a window that genuinely
    /// executes them (a `variants(...)` sub-state whose pipeline mutates
    /// labels takes exactly this path).
    fn drain_group(&mut self, group: MutGroup) -> (LabelSet, Vec<(i64, f64)>) {
        let mut points: Vec<(i64, f64)> = Vec::new();
        // Every arm drains the group's cells into a `Vec` keyed by
        // grid index so the points come out ordered. The drain is
        // bounded by the cells' own charge (one point per created
        // cell / retained sample) and each element is narrower than
        // the `WinSample` that charge is denominated in — the `Vec`
        // moves, it does not copy. The discharge happens AFTER the
        // cells are consumed, so a charge is never released while
        // the memory it paid for is still live (issue #227 review
        // round 5, finding 2).
        match group.cells {
            MutCells::IntExpanded(cells) => {
                let mut cells: Vec<(i64, u64)> = cells.into_iter().collect();
                let staged = cells.len() as u64;
                cells.sort_by_key(|(k, _)| *k);
                for (k, run_int) in cells {
                    points.push((
                        self.grid_point(k),
                        reduce_int_cell(self.op, self.rate_window_ns, run_int),
                    ));
                }
                discharge_retention(&mut self.retained, staged);
            }
            // Issue #236 Part C, C1: prefix-sum the difference
            // array ascending. Between two consecutive delta
            // indices the running pair is CONSTANT, so a covered
            // run emits one point per cell and an uncovered run is
            // skipped in O(1) — the emitted set and its values are
            // exactly the expanded form's (`covering_k` is the
            // half-open interval each `(+1, -1)` pair spans).
            // `run_count > 0` is the coverage test, which is why
            // it cannot fold into `run_value`: a covered cell of
            // value 0 emits `0`, an uncovered one emits nothing.
            MutCells::IntDeltas(cells) => {
                let mut cells: Vec<(i64, IntDelta)> = cells.into_iter().collect();
                let staged = cells.len() as u64;
                cells.sort_by_key(|(k, _)| *k);
                let (mut run_value, mut run_count) = (0i64, 0i64);
                for i in 0..cells.len() {
                    let (k, d) = cells[i];
                    run_value += d.dvalue;
                    run_count += d.dcount;
                    if run_count <= 0 {
                        continue;
                    }
                    // Constant until the next delta index, and
                    // never past the last grid point.
                    let end = cells
                        .get(i + 1)
                        .map_or(self.kmax + 1, |(next, _)| *next)
                        .min(self.kmax + 1);
                    let run_int = u64::try_from(run_value).unwrap_or(0);
                    let v = reduce_int_cell(self.op, self.rate_window_ns, run_int);
                    for cell in k.max(0)..end {
                        points.push((self.grid_point(cell), v));
                    }
                }
                discharge_retention(&mut self.retained, staged);
            }
            // Issue #236 Part C, C2: one sort per GROUP, then a
            // two-pointer sweep over ascending grid indices. The
            // slice handed to `reduce_window` is the identical
            // element sequence the per-cell form produced —
            // `win_order` is `ts`-major and total on a group's
            // samples, and the window `(t-range, t]` is exactly
            // `covering_k`'s inverse. Cells are visited only where
            // some sample covers them (the merged covering
            // intervals), so an uncovered stretch of grid costs
            // nothing, as it did before.
            MutCells::Samples(mut samples) => {
                let staged = samples.len() as u64;
                samples.sort_by(win_order);
                // `covering_k` is monotone in `ts` and the samples
                // are now `ts`-ascending, so the covering
                // intervals arrive sorted and merging them is ONE
                // pass. Merging is what keeps an uncovered stretch
                // of grid free, exactly as the per-cell map made
                // it free.
                let mut covered: Vec<(i64, i64)> = Vec::new();
                for s in &samples {
                    let (a, b) = self.covering_k(s.ts);
                    if a > b {
                        continue;
                    }
                    match covered.last_mut() {
                        Some(last) if a <= last.1 + 1 => last.1 = last.1.max(b),
                        _ => covered.push((a, b)),
                    }
                }
                let (mut lo, mut hi) = (0usize, 0usize);
                for (a, b) in covered {
                    for cell in a..=b {
                        let t = self.grid_point(cell);
                        while hi < samples.len() && samples[hi].ts <= t {
                            hi += 1;
                        }
                        // `checked_sub` for the same reason
                        // `FpSlide::emit_at` uses it: an eviction
                        // bound below the representable domain
                        // evicts nothing.
                        if let Some(bound) = t.checked_sub(self.range) {
                            while lo < hi && samples[lo].ts <= bound {
                                lo += 1;
                            }
                        }
                        if let Some(v) = reduce_window(
                            self.op,
                            self.class,
                            self.param,
                            self.rate_window_ns,
                            0,
                            &samples[lo..hi],
                        ) {
                            points.push((t, v));
                        }
                    }
                }
                discharge_retention(&mut self.retained, staged * self.per_sample);
            }
        }
        (group.labels, points)
    }

    /// Routes an already-materialised batch of leaf series through the
    /// attached fold (issue #236 Part B), or hands it back unchanged when
    /// nothing folded.
    ///
    /// **The reduction-order pin, extended to the fold.** The batch is put
    /// in label-set order before folding — the same total order
    /// `pin_reduction_order` applies immediately before grouping on the
    /// materialising path — so the folded value is the materialised value
    /// bit for bit, and both are reproducible. The sort is inside the
    /// folding arm so a query that does NOT fold keeps its existing output
    /// order byte for byte.
    ///
    /// Series are consumed one at a time, so a series' points are freed as
    /// they are folded rather than all being held until finish.
    ///
    /// **Residual, recorded so it is not rediscovered as a surprise**
    /// (ledger entry `#236 (c)`). The non-mutating caller has already
    /// materialised `series_out` by the time it gets here, so this arm
    /// does NOT collapse that vector the way the fan-out arm collapses
    /// its groups. Folding at each slider's close instead would, but
    /// sliders complete in FINGERPRINT order — deterministic, and not the
    /// label-set order this pins — so the folded value would stop being
    /// the materialised value. The consequence is deferred, not absent:
    /// once emitted points are charged
    /// ([`MAX_METRIC_RESULT_POINTS`], not yet levied) a non-mutating range
    /// leaf whose `streams x grid points` exceeds the charge is refused
    /// where the reference serves it. The fix is a step-ordered
    /// evaluator (issue #250), not a larger constant.
    pub(in crate::logql) fn emit(
        &mut self,
        mut series: Vec<MatrixSeries>,
    ) -> Result<QueryResult, ReadError> {
        match self.fold.take() {
            None => Ok(QueryResult::Matrix(series)),
            Some(mut fold) => {
                pin_reduction_order(
                    &mut series,
                    |s| &s.labels,
                    |a, b| range_payload_cmp(a.points.iter().copied(), b.points.iter().copied()),
                );
                for s in series {
                    fold.push_series(&s.labels, &s.points)?;
                }
                Ok(QueryResult::Matrix(fold.finish()))
            }
        }
    }

    /// `absent_over_time`: emit `1.0` at every grid point whose selector-wide
    /// window `(t-range, t]` is EMPTY (Loki's one emit-on-empty reducer).
    /// Prefix-sums the O(grid) coverage difference array (review finding 1):
    /// a running sum > 0 means at least one sample covers that grid point.
    ///
    /// Returns the series rather than a [`QueryResult`] so the caller can
    /// route them through the issue #236 Part B fold like every other
    /// emit path; the 0-or-1 shape is unchanged.
    fn finish_absent(&mut self) -> Result<Vec<MatrixSeries>, ReadError> {
        // Issue #236: `absent_over_time` emits at most ONE series, whose
        // points are the grid's empty windows — reserved before the
        // vector is built, like every other emitted series.
        charge_result_points(
            &mut self.result_points,
            grid_slot_count(self.kmax),
            self.caps.result_points,
        )?;
        let mut points: Vec<(i64, f64)> = Vec::new();
        // Same width as the deltas (see `present_cover`'s proof): the running
        // sum is bounded by the total collision-group count, itself bounded by
        // the byte-scan budget — it cannot overflow `i64`.
        let mut running: i64 = 0;
        for k in 0..=self.kmax {
            running += self.present_cover[k as usize];
            if running == 0 {
                points.push((self.grid_point(k), 1.0));
            }
        }
        Ok(if points.is_empty() {
            Vec::new()
        } else {
            vec![MatrixSeries {
                labels: std::mem::take(&mut self.absent_labels),
                points,
            }]
        })
    }
}

/// Puts a leaf's emitted matrix back on the CALLER's grid after the
/// evaluation ran `offset` earlier (issue #343).
///
/// The shift is `+offset_ns` — the inverse of the planner's
/// `shift_by_offset`, and SIGNED, so a negative offset (which the
/// reference accepts, shifting the window forward) comes back correctly
/// too. `offset_ns == 0` is the overwhelmingly common case and returns
/// the result untouched, so no offset-free query pays for this.
///
/// A uniform translation of every point on every series: it commutes with
/// everything downstream (`apply_vector_aggs` groups points BY timestamp,
/// and `combine_binary` joins on it), which is why each leaf can shift
/// independently and a query mixing offsets — `rate(a[5m]) /
/// rate(a[5m] offset 1h)` — still joins on the caller's grid.
///
/// Vector (instant) results carry no timestamp of their own, so they pass
/// through: the API stamps them with the request's `time`, offset or not.
///
/// **The round trip is EXACT, so `saturating_add` never saturates** (issue
/// #343, after `shift_by_offset` became `checked_sub`). Every emitted point
/// is `grid_point(k)` with `k <= kmax`, hence lies in
/// `[grid_start, end] = [S−d, E−d]`; both of those are representable
/// precisely because `checked_sub` returned `Some` for them, so
/// `point + d ∈ [S, E] ⊆ i64`. When it returned `None` the planner
/// substituted the degenerate window and no point is emitted at all. The
/// `debug_assert!` below is that invariant, checked where it would break.
/// This mirrors v3.7.4 `pkg/logql/range_vector.go:195` / `:589`
/// (`ts := r.current/1e+6 + r.offset/1e+6`, tag `v3.7.4` /
/// `b318f2829f0ae2094ab3a1e90780450e9e4b03be`) — the one place the offset
/// is added back.
fn shift_emitted_points(result: QueryResult, offset_ns: i64) -> QueryResult {
    if offset_ns == 0 {
        return result;
    }
    match result {
        QueryResult::Matrix(mut series) => {
            for s in &mut series {
                for (ts, _) in &mut s.points {
                    debug_assert!(
                        ts.checked_add(offset_ns).is_some(),
                        "issue #343: an emitted point must round-trip exactly \
                         ({ts} + {offset_ns} left i64)"
                    );
                    *ts = ts.saturating_add(offset_ns);
                }
            }
            QueryResult::Matrix(series)
        }
        other => other,
    }
}

/// The pure client-aggregated evaluation (issue M6-10): the slice-driven
/// wrapper over [`ClientAggState`] the hermetic golden/allocation suites
/// pin (the engine streams rows into the same state chunk-wise instead
/// of buffering the scan — review round 1, finding 1).
///
/// Vector aggregations are NOT applied here — the caller finishes them
/// (`apply_vector_aggs`), mirroring the SQL path. Callers that want the
/// engine's ACTUAL sequence, with the innermost aggregation folded at the
/// leaf (issue #236 Part B), use [`run_client_agg_rows_folded`].
pub fn run_client_agg_rows(
    rows: &[MetricScanRow],
    compiled: &super::pipeline::CompiledPipeline,
    meta: &HashMap<u64, StreamMetaRow>,
    client: &ClientAgg,
    window: ClientWindow,
    rate_window_ns: Option<u64>,
) -> Result<QueryResult, ReadError> {
    run_client_agg_rows_folded(rows, compiled, meta, client, window, rate_window_ns, &[])
}

/// [`run_client_agg_rows`] plus the whole vector-aggregation chain, run
/// the way [`LogQlEngine::run_metric_client`] runs it (issue #236 Part
/// B): on a RANGE query the innermost spec is handed to the leaf, which
/// folds it AS it emits, and only the remaining prefix is materialised
/// through [`apply_vector_aggs`]. On an instant query, and for the specs
/// the leaf cannot own (`sort`/`sort_desc`/`approx_topk`), every spec goes
/// to `apply_vector_aggs` — i.e. exactly today's path.
///
/// `pub` so the hermetic suites and the conformance runner drive the same
/// sequence the engine does, rather than a materialising approximation of
/// it: the fold's equivalence with `apply_vector_aggs` is a property that
/// has to be EXERCISED, not asserted.
pub fn run_client_agg_rows_folded(
    rows: &[MetricScanRow],
    compiled: &super::pipeline::CompiledPipeline,
    meta: &HashMap<u64, StreamMetaRow>,
    client: &ClientAgg,
    window: ClientWindow,
    rate_window_ns: Option<u64>,
    aggs: &[plan::VectorAggSpec],
) -> Result<QueryResult, ReadError> {
    if matches!(window, ClientWindow::Range { .. }) {
        // Issue #227: a range query evaluates Loki's sliding windows, which
        // assume fingerprint-contiguous, ascending-ts input (the live
        // engine's `metric_raw_samples_sliding` PK scan guarantees it). The
        // live path already streams in that order; only re-sort (cloning) the
        // pure-slice entry if it is NOT already ordered — so a pre-ordered
        // scan folds with zero per-row allocation.
        let mut state = RangeSlideState::new(
            compiled,
            meta,
            client,
            window,
            rate_window_ns,
            AggCaps::DEFAULT,
        )?;
        if let Some(spec) = aggs.last() {
            state.attach_fold(spec);
        }
        let folded = state.folded_aggs();
        let ordered = rows.windows(2).all(|w| {
            (w[0].fingerprint, w[0].timestamp_ns) <= (w[1].fingerprint, w[1].timestamp_ns)
        });
        if ordered {
            state.push_rows(rows)?;
        } else {
            let mut sorted: Vec<MetricScanRow> = rows.to_vec();
            sorted.sort_by(|a, b| {
                a.fingerprint
                    .cmp(&b.fingerprint)
                    .then(a.timestamp_ns.cmp(&b.timestamp_ns))
            });
            state.push_rows(&sorted)?;
        }
        let result = state.finish()?;
        return apply_vector_aggs(result, &aggs[..aggs.len() - folded]);
    }
    // The `Range` arm returned above, so this narrowing cannot fail; it
    // is a narrowing rather than an assertion so the instant state cannot
    // be built for a stepped window at all (issue #236 Part D).
    let instant = window
        .as_instant()
        .ok_or_else(|| ReadError::PipelineInvalid {
            reason: "internal: a stepped window reached the instant aggregation state".to_string(),
        })?;
    let mut state = ClientAggState::new(
        compiled,
        meta,
        client,
        instant,
        rate_window_ns,
        AggCaps::DEFAULT,
    )?;
    state.push_rows(rows)?;
    apply_vector_aggs(state.finish(), aggs)
}

/// The live engine's per-fold metric-aggregation state: the instant
/// single-window [`ClientAggState`] or the issue #227 range sliding
/// evaluator, both driven chunk-wise off the raw scan stream.
#[derive(Debug)]
pub(in crate::logql) enum MetricAggState<'q> {
    Instant(Box<ClientAggState<'q>>),
    Range(Box<RangeSlideState<'q>>),
}

impl MetricAggState<'_> {
    pub(in crate::logql) fn push_rows(&mut self, rows: &[MetricScanRow]) -> Result<(), ReadError> {
        match self {
            MetricAggState::Instant(s) => s.push_rows(rows),
            MetricAggState::Range(s) => s.push_rows(rows),
        }
    }

    pub(in crate::logql) fn finish(self) -> Result<QueryResult, ReadError> {
        match self {
            MetricAggState::Instant(s) => Ok(s.finish()),
            MetricAggState::Range(s) => s.finish(),
        }
    }

    /// How many vector aggregations this state has taken over — a test
    /// seam (issue #260) so "the variants path attaches no fold", the
    /// premise the group-byte multiplicity of 2 rests on, can be asserted
    /// on a REAL constructed state rather than only lexically. The
    /// instant arm has no fold to take.
    #[cfg(test)]
    pub(in crate::logql) fn folded_aggs(&self) -> usize {
        match self {
            MetricAggState::Instant(_) => 0,
            MetricAggState::Range(s) => s.folded_aggs(),
        }
    }
}

// =====================================================================
// Issue #221: `variants(<metricExpr>, …) of (<logRangeExpr>)`.
//
// ONE scan with N reducers, never N queries: the common log range plans
// the single raw scan (byte-identical SQL to the equivalent
// single-extractor query, independent of N), and every chunk of that one
// row stream is fanned out in memory to N ordinary sub-states
// (`ClientAggState`/`RangeSlideState`), each governed by
// `AggCaps::DEFAULT.divided(n)` so the per-field TOTAL over sub-states is
// exactly today's single-query bound. Every construction-time allocation
// the fan-out ADDS beyond one ordinary extractor state is charged against
// `MAX_VARIANT_FANOUT_STATE_BYTES` BEFORE it happens (charge-before-
// allocate, #227 discipline) and released as `finish` consumes the state.
// =====================================================================

/// Adjudication #1: a line whose `__error__` is nonempty after the FULL
/// pipeline fails the metric query with the oracle-matched named error —
/// never silent exclusion.
fn check_surviving_error(labels: &[(Cow<'_, str>, Cow<'_, str>)]) -> Result<(), ReadError> {
    let Some((_, err)) = labels
        .iter()
        .find(|(k, v)| k == ERROR_LABEL && !v.is_empty())
    else {
        return Ok(());
    };
    let mut sorted: Vec<(String, String)> = labels
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    sorted.sort();
    Err(ReadError::MetricPipelineError {
        error_type: err.to_string(),
        series: render_series_labels(&sorted),
    })
}

// ---------------------------------------------------------------------
// Issue M6-10: binary operations over metric results.
// ---------------------------------------------------------------------

/// The empty [`MutCells`] a query's mutating groups start from — the ONE
/// place the representation is chosen (issue #236 Part C).
///
/// Classes B/C always retain samples. Within class A, the delta form's
/// win is proportional to `ceil(range/step)` — the number of cells one
/// sample covers — so it is taken exactly where that exceeds one, i.e.
/// where the sliding windows OVERLAP. That is the reference's own
/// `selRange >= step` predicate (`pkg/logql/range_vector.go`'s stepped
/// iterator), plus `kmax > 0`: on a one-point grid there is nothing to
/// fan out into and the expanded form is already minimal. Below the
/// predicate the expanded form is strictly better (one charge per sample
/// rather than two, and repeated samples in one cell collapse), so it is
/// kept rather than replaced.
/// The comparison is over `i128`, not `range >= step as i64`: `step` is a
/// `u64` and the narrowing form wraps a wide step to a negative number, so
/// every range would compare `>=` it and take the delta arm. Validated
/// durations keep that out of reach today — the exhaustive predicate test
/// found it anyway, which is the argument for enumerating a small domain
/// rather than sampling it.
fn mut_cells_for(class: ReducerClass, range: i64, step: u64, kmax: i64) -> MutCells {
    if !matches!(class, ReducerClass::InvertInteger) {
        return MutCells::Samples(Vec::new());
    }
    if i128::from(range) >= i128::from(step) && kmax > 0 {
        MutCells::IntDeltas(HashMap::new())
    } else {
        MutCells::IntExpanded(HashMap::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logql::CompiledPipeline;
    use crate::logql::charge::MAX_CLIENT_AGG_GROUP_BYTES;
    use crate::logql::charge::MAX_COUNTER_VALUES;
    use crate::logql::charge::MAX_QUANTILE_VALUES;
    use crate::logql::charge::MAX_TS_COLLISION_GROUP_BYTES;
    use crate::logql::testkit::*;
    use pulsus_logql::{Expr, GroupingKind, Stage, VectorAggOp};

    use super::super::charge::{MAX_RETAINED_WINDOW_POINTS, MAX_TS_COLLISION_GROUP};
    use super::super::labels::StructuredMetadataCtx;
    use super::super::window::{GridWindow, MAX_CLIENT_AGG_BUCKETS, materialize_vector_lit};

    /// The pipeline stages of a log query (for building a `ClientAgg`).
    fn parse_pipeline(query: &str) -> Vec<Stage> {
        let pulsus_logql::Expr::Log(le) = pulsus_logql::parse(query).expect("parse") else {
            panic!("expected a log expression");
        };
        le.pipeline
    }

    /// Narrows an instant `ClientWindow` for [`ClientAggState::new`]
    /// (issue #236 Part D). Tests cannot fabricate the witness either —
    /// `InstantWindow`'s field is private to `mod instant_window`, so
    /// `mint` is the only source anywhere in the crate, `mod tests`
    /// included; a test that hands a stepped window here fails at the
    /// `expect`, not silently.
    fn instant_of(window: ClientWindow) -> InstantWindow {
        window.as_instant().expect("an instant window")
    }

    fn slide_rows(fp: u64, samples: &[(i64, &str)]) -> Vec<MetricScanRow> {
        samples
            .iter()
            .map(|(ts, body)| MetricScanRow {
                fingerprint: fp,
                timestamp_ns: *ts,
                body: body.to_string(),
            })
            .collect()
    }

    fn one_series_points(res: QueryResult) -> (LabelSet, Vec<(i64, f64)>) {
        match res {
            QueryResult::Matrix(mut s) => {
                assert_eq!(s.len(), 1, "expected one series, got {s:?}");
                let s = s.remove(0);
                (s.labels, s.points)
            }
            other => panic!("expected a matrix, got {other:?}"),
        }
    }

    /// count_over_time sliding windows, hand-computed. Samples at
    /// 10,20,30,40,50; grid 0..50 step 10, range 25 ⇒ window `(t-25, t]`.
    /// t=0 empty (gap); t=10→1, 20→2, 30→3, 40→{20,30,40}=3, 50→{30,40,50}=3.
    #[test]
    fn sliding_count_over_time_matches_hand_computed_windows() {
        let client = ClientAgg {
            pipeline: vec![],
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
            grouping: None,
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let rows = slide_rows(1, &[(10, "x"), (20, "x"), (30, "x"), (40, "x"), (50, "x")]);
        let window = slide_window(0, 50, 10, 25);
        let res = run_client_agg_rows(&rows, &compiled, &meta, &client, window, None).unwrap();
        let (_, points) = one_series_points(res);
        assert_eq!(
            points,
            vec![(10, 1.0), (20, 2.0), (30, 3.0), (40, 3.0), (50, 3.0)],
            "empty first window emits no point; overlapping windows slide"
        );
    }

    /// Issue #227 review round 11 (end-to-end at the domain floor): for
    /// `start = end = i64::MIN` with `[1ns]`, the grid's only window
    /// `(i64::MIN - 1ns, i64::MIN]` has a logical lower bound BELOW the
    /// representable domain — a sample stored at exactly `i64::MIN` is
    /// INSIDE it (the reference's `(t-range, t]` includes it; the bound is
    /// vacuous, not exclusive). The prior saturating eviction clamped the
    /// bound to `i64::MIN` and dropped the sample.
    #[test]
    fn a_sample_at_i64_min_is_inside_an_underflowing_window() {
        let client = ClientAgg {
            pipeline: vec![],
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
            grouping: None,
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let rows = slide_rows(1, &[(i64::MIN, "x")]);
        let window = slide_window(i64::MIN, i64::MIN, 1, 1);
        let res = run_client_agg_rows(&rows, &compiled, &meta, &client, window, None).unwrap();
        let (_, points) = one_series_points(res);
        assert_eq!(
            points,
            vec![(i64::MIN, 1.0)],
            "the boundary sample must be counted, matching the reference"
        );
    }

    /// The round-11 negative control: a LEGITIMATELY-computed `i64::MIN`
    /// window bound — `t = i64::MIN + 1`, `[1ns]`, so `t - range` is
    /// exactly `i64::MIN` with NO underflow — keeps its EXCLUSIVE
    /// semantics: the sample at exactly `i64::MIN` stays outside
    /// `(i64::MIN, i64::MIN + 1]` and only the in-window sample counts.
    #[test]
    fn a_legitimate_i64_min_window_bound_still_excludes_the_boundary_sample() {
        let client = ClientAgg {
            pipeline: vec![],
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
            grouping: None,
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let rows = slide_rows(1, &[(i64::MIN, "out"), (i64::MIN + 1, "in")]);
        let window = slide_window(i64::MIN + 1, i64::MIN + 1, 1, 1);
        let res = run_client_agg_rows(&rows, &compiled, &meta, &client, window, None).unwrap();
        let (_, points) = one_series_points(res);
        assert_eq!(
            points,
            vec![(i64::MIN + 1, 1.0)],
            "an exactly-representable exclusive bound must still evict the boundary sample"
        );
    }

    /// `rate({}[1m]) ≠ rate({}[10m])`: the `[range]` is live (window width AND
    /// the per-second divisor both track range, not step). Two samples 30s
    /// apart at step 60s.
    #[test]
    fn sliding_rate_depends_on_the_range_selector() {
        let client_for = |range_op| ClientAgg {
            pipeline: vec![],
            value: ClientValue::Count,
            range_op,
            param: None,
            absent_labels: vec![],
            grouping: None,
        };
        let client = client_for(RangeAggOp::Rate);
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        // Two lines inside a 1m window ending at 60s.
        let rows = slide_rows(1, &[(31_000_000_000, "x"), (59_000_000_000, "x")]);
        let s = 1_000_000_000i64;
        let run = |range_ns: i64| {
            let window = slide_window(60 * s, 60 * s, (60 * s) as u64, range_ns as u64);
            let res = run_client_agg_rows(
                &rows,
                &compiled,
                &meta,
                &client,
                window,
                Some(range_ns as u64),
            )
            .unwrap();
            one_series_points(res).1
        };
        let r1m = run(60 * s); // 2 lines / 60s = 0.0333…
        let r10m = run(600 * s); // 2 lines / 600s = 0.0033…
        assert_ne!(r1m, r10m, "the [range] divisor must be live");
        assert_eq!(r1m, vec![(60 * s, 2.0 / 60.0)]);
        assert_eq!(r10m, vec![(60 * s, 2.0 / 600.0)]);
    }

    /// AC10: a forced same-`(fingerprint, timestamp_ns)` collision of ≥3 rows
    /// with DISTINCT bodies resolves `first_over_time`/`last_over_time` (and
    /// class-C folds) in the fixed full-body `tie_rank` order, byte-stable
    /// across shuffled input.
    #[test]
    fn sliding_collision_group_orders_by_full_body_deterministically() {
        // `last_over_time` over an unwrapped value: value is the byte length,
        // ordered by full body. Three same-ns lines with distinct bodies.
        let client = ClientAgg {
            pipeline: vec![],
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
            grouping: None,
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 10, 10, 10);
        // Same ts, distinct bodies — count is order-independent, but the
        // collision path must run without panicking and yield a stable count.
        let mut base = vec![(5i64, "ccc"), (5, "aaa"), (5, "bbb")];
        let r1 = run_client_agg_rows(
            &slide_rows(1, &base),
            &compiled,
            &meta,
            &client,
            window,
            None,
        )
        .unwrap();
        base.reverse();
        let r2 = run_client_agg_rows(
            &slide_rows(1, &base),
            &compiled,
            &meta,
            &client,
            window,
            None,
        )
        .unwrap();
        assert_eq!(r1, r2, "shuffled collision input must be byte-stable");
        assert_eq!(one_series_points(r1).1, vec![(10, 3.0)]);
    }

    /// The collision-group cap trips a clean `TsCollisionGroup` 422, never an
    /// OOM, on a pathological same-`(fingerprint, ts)` run.
    #[test]
    fn sliding_collision_group_over_cap_is_a_named_too_broad_error() {
        let client = ClientAgg {
            pipeline: vec![],
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
            grouping: None,
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let n = (MAX_TS_COLLISION_GROUP + 1) as usize;
        let rows: Vec<MetricScanRow> = (0..n)
            .map(|i| MetricScanRow {
                fingerprint: 1,
                timestamp_ns: 5,
                body: format!("line-{i}"),
            })
            .collect();
        let window = slide_window(0, 10, 10, 10);
        match run_client_agg_rows(&rows, &compiled, &meta, &client, window, None) {
            Err(ReadError::QueryTooBroad(TooBroadReason::TsCollisionGroup { cap, .. })) => {
                assert_eq!(cap, MAX_TS_COLLISION_GROUP);
            }
            other => panic!("expected TsCollisionGroup, got {other:?}"),
        }
    }

    /// AC5(a): a DENSE but Loki-servable window (well within the caps and the
    /// byte budget) streams to a correct result — the cap must not reject
    /// density Loki serves. 50_000 samples in one `[range]`, well under the
    /// 4M cap.
    #[test]
    fn sliding_dense_but_servable_window_streams_without_a_cap_error() {
        let client = ClientAgg {
            pipeline: vec![],
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
            grouping: None,
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        const N: i64 = 50_000;
        let rows: Vec<MetricScanRow> = (0..N)
            .map(|i| MetricScanRow {
                fingerprint: 1,
                timestamp_ns: i + 1, // 1ns apart, all inside the window
                body: "x".to_string(),
            })
            .collect();
        let window = slide_window(0, N, N as u64, N as u64);
        let res = run_client_agg_rows(&rows, &compiled, &meta, &client, window, None)
            .expect("a dense-but-servable window must stream, never 422");
        assert_eq!(one_series_points(res).1, vec![(N, N as f64)]);
    }

    /// AC5(b) + AC8: the concurrent-retention cap trips DURING the scan (as
    /// the first oversized window fills), not after it — the charge is
    /// per-load, so a retaining reducer over more than `cap` in-window points
    /// aborts with the named `MetricRetention` error. Uses a tiny synthetic
    /// cap via the retaining (class-B `quantile`) path with a window wide
    /// enough that nothing evicts. `quantile` charges TWO points per sample
    /// (the window entry plus its pre-charged re-reduce copy — review round 5,
    /// finding 2), which is what the trip arithmetic below is expressed in.
    #[test]
    fn sliding_retention_cap_trips_during_the_scan_with_the_named_error() {
        // A retaining reducer (quantile) whose window never evicts: every
        // sample stays concurrently retained, so `retained` climbs to the cap.
        let client = ClientAgg {
            pipeline: vec![],
            value: ClientValue::Unwrap,
            range_op: RangeAggOp::QuantileOverTime,
            param: Some(0.5),
            absent_labels: vec![],
            grouping: None,
        };
        let per_sample = retention_points_per_sample(client.range_op);
        assert_eq!(
            per_sample, 2,
            "quantile pre-charges its per-emit value copy alongside the window entry"
        );
        // Drive `FpSlide::load_group` directly with a TINY cap so the test is
        // fast and the trip point is exact (the production cap is 4M).
        let mut slide = FpSlide {
            stream_hash: 7,
            labels: vec![],
            op: client.range_op,
            class: reducer_class(client.range_op, client.value),
            param: client.param,
            rate_window_ns: None,
            grid_start: 0,
            step: 1_000_000,
            range: 1_000_000_000,
            kmax: 10,
            next_k: 0,
            win: VecDeque::new(),
            run_int: 0,
            per_sample,
            points: Vec::new(),
        };
        let mut retained = 0u64;
        const TINY_CAP: u64 = 100;
        let member = |v: f64| CollMember {
            body: String::new(),
            value: v,
            out: None,
        };
        let mut err = None;
        let mut loads = 0u64;
        for i in 0..1_000i64 {
            // All at ts=1 so nothing ever evicts (one collision-free load per
            // call at increasing ts would evict; here the window only grows).
            let group = [member(i as f64)];
            loads += 1;
            if let Err(e) = slide.load_group(1, &group, &mut retained, TINY_CAP) {
                err = Some(e);
                break;
            }
        }
        match err {
            Some(ReadError::QueryTooBroad(TooBroadReason::MetricRetention { count, cap })) => {
                assert_eq!(cap, TINY_CAP);
                assert_eq!(
                    count,
                    TINY_CAP + per_sample,
                    "trips at the first over-cap load"
                );
            }
            other => panic!("expected MetricRetention, got {other:?}"),
        }
        assert_eq!(
            loads,
            TINY_CAP / per_sample + 1,
            "the cap must trip DURING the scan (after ~cap/per-sample loads), not after all 1000"
        );
        // CHARGE BEFORE ALLOCATE (review round 5, finding 2): the breaching
        // sample must never reach the deque. If the charge were applied AFTER
        // `push_back` — as it was before — the window would hold one more
        // sample than the cap allows, i.e. the allocation the cap exists to
        // refuse would already have happened.
        assert_eq!(
            slide.win.len() as u64,
            TINY_CAP / per_sample,
            "the refused sample must not have been pushed"
        );
    }

    /// AC8: the concurrent-retention invariant is symmetric — every charged
    /// point is discharged on eviction, so a long streaming slide over a
    /// narrow window keeps `retained` bounded by the window's contents rather
    /// than growing with the scan.
    #[test]
    fn sliding_retention_charge_and_discharge_are_symmetric() {
        let mut slide = FpSlide {
            stream_hash: 7,
            labels: vec![],
            op: RangeAggOp::QuantileOverTime,
            class: ReducerClass::ReduceIndependent,
            param: Some(0.5),
            rate_window_ns: None,
            grid_start: 0,
            step: 10,
            range: 10,
            kmax: 100,
            next_k: 0,
            win: VecDeque::new(),
            run_int: 0,
            per_sample: retention_points_per_sample(RangeAggOp::QuantileOverTime),
            points: Vec::new(),
        };
        let mut retained = 0u64;
        // 100 samples 10ns apart over a 10ns window: at most a couple are
        // ever concurrently retained, however long the scan runs (times the
        // per-sample charge — 2 for quantile).
        let bound = 3 * retention_points_per_sample(RangeAggOp::QuantileOverTime);
        for i in 1..=100i64 {
            let group = [CollMember {
                body: String::new(),
                value: i as f64,
                out: None,
            }];
            slide
                .load_group(i * 10, &group, &mut retained, MAX_RETAINED_WINDOW_POINTS)
                .expect("under cap");
            assert!(
                retained <= bound,
                "retention must stay window-bounded, saw {retained} after {i} loads"
            );
        }
        // Closing the series discharges everything left in the window.
        slide.finish(&mut retained);
        assert_eq!(retained, 0, "charge/discharge must be symmetric");
    }

    /// AC10: a forced same-`(fingerprint, timestamp_ns)` collision of ≥3 rows
    /// with DISTINCT bodies resolves an ORDER-DEPENDENT reducer through the
    /// full-body `tie_rank` order, byte-stable across shuffled input. Uses
    /// `last_over_time` (positional in the canonical order) plus a class-C
    /// `sum` — the two shapes the review called out as untested.
    #[test]
    fn sliding_same_stream_tie_rank_orders_class_c_and_first_last_deterministically() {
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 10, 10, 10);
        // Three rows at the IDENTICAL `(fingerprint, timestamp_ns)` with
        // DISTINCT bodies that all parse to the same label set (`a` is
        // consumed by the unwrap), chosen so the full-BODY byte order is
        // deliberately DIFFERENT from the value order:
        //   `a="3"` (0x22 after `a=`) < `a=1` (0x31) < `a=2` (0x32)
        //   ⇒ canonical values in order: 3, 1, 2
        // So `first` = 3 and `last` = 2. A value-tiebreak (the old instant
        // rule) would give first=1/last=3, and an arrival-order rule would
        // flap under shuffling — this case discriminates all three.
        let bodies = [r#"a="3""#, "a=1", "a=2"];
        let run = |op: RangeAggOp, order: &[usize]| -> Vec<(i64, f64)> {
            let client = ClientAgg {
                pipeline: parse_pipeline(r#"{x="y"} | logfmt | unwrap a"#),
                value: ClientValue::Unwrap,
                range_op: op,
                param: None,
                absent_labels: vec![],
                grouping: None,
            };
            let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
            let rows: Vec<MetricScanRow> = order
                .iter()
                .map(|&i| MetricScanRow {
                    fingerprint: 1,
                    timestamp_ns: 5,
                    body: bodies[i].to_string(),
                })
                .collect();
            let res =
                run_client_agg_rows(&rows, &compiled, &meta, &client, window, None).expect("eval");
            one_series_points(res).1
        };
        // Every input permutation must give the SAME, body-order-determined
        // answer (byte-stable across shuffled runs).
        for order in [[0, 1, 2], [2, 1, 0], [1, 0, 2], [1, 2, 0], [2, 0, 1]] {
            assert_eq!(
                run(RangeAggOp::FirstOverTime, &order),
                vec![(10, 3.0)],
                "first = the min-(ts,stream_hash,tie_rank) sample (body order), input {order:?}"
            );
            assert_eq!(
                run(RangeAggOp::LastOverTime, &order),
                vec![(10, 2.0)],
                "last = the max-(ts,stream_hash,tie_rank) sample (body order), input {order:?}"
            );
            // Class C: the fold runs in the same canonical order.
            assert_eq!(run(RangeAggOp::SumOverTime, &order), vec![(10, 6.0)]);
        }
    }

    // -----------------------------------------------------------------
    // Issue #236 §4 — the result-point charge.
    // -----------------------------------------------------------------

    /// The charge is an ADMISSION counter with an exact IDENTITY: one
    /// grid width per output series, whatever the data's density.
    ///
    /// Stated as an identity rather than an inequality because that is
    /// what makes it checkable — a charge that merely stayed under the
    /// cap would pass with the reservation deleted.
    #[test]
    fn the_result_point_charge_is_one_grid_width_per_output_series() {
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        // step 10 over [0, 100] => kmax = 10 => 11 grid points.
        let window = slide_window(0, 100, 10, 30);
        let grid = 11u64;

        // Non-mutating: one slider per FINGERPRINT.
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"} | line_format "keep""#),
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
            grouping: None,
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        assert!(!compiled.metric_mutates_labels());
        let mut state =
            RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                .unwrap();
        // Six rows on ONE fingerprint: density must not move the charge.
        state
            .push_rows(&slide_rows(
                1,
                &[
                    (5, "a"),
                    (6, "b"),
                    (17, "c"),
                    (28, "d"),
                    (39, "e"),
                    (95, "f"),
                ],
            ))
            .expect("fold");
        assert_eq!(
            state.result_points, grid,
            "one fingerprint => exactly one grid width, whatever its density"
        );

        // Mutating: one group per distinct OUTPUT LABEL SET.
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"} | logfmt"#),
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
            grouping: None,
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        assert!(compiled.metric_mutates_labels());
        for groups in [1u64, 3, 7] {
            let mut state =
                RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                    .unwrap();
            // Two rows per group, so the count is groups and not rows.
            let mut rows: Vec<MetricScanRow> = Vec::new();
            for g in 0..groups {
                for r in 0..2u64 {
                    rows.push(MetricScanRow {
                        fingerprint: 1,
                        timestamp_ns: (g * 10 + r) as i64,
                        body: format!("id={g}"),
                    });
                }
            }
            state.push_rows(&rows).expect("fold");
            assert_eq!(
                state.result_points,
                groups * grid,
                "{groups} output groups => {groups} grid widths"
            );
        }
    }

    /// The cap REFUSES, and it refuses BEFORE the allocation it is
    /// guarding — the group that would breach is never created.
    #[test]
    fn the_result_point_cap_refuses_before_the_group_exists() {
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 100, 10, 30);
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"} | logfmt"#),
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
            grouping: None,
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let mut state =
            RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                .unwrap();
        // Room for exactly two groups (11 points each).
        state.caps.result_points = 22;
        // FOUR rows: `push_rows` flushes a collision group only when the
        // next row's `(fingerprint, ts)` differs, so the trailing group
        // stays staged. The fourth row is what makes the third group
        // flush — and breach — inside `push_rows` rather than at finish.
        let rows: Vec<MetricScanRow> = (0..4u64)
            .map(|g| MetricScanRow {
                fingerprint: 1,
                timestamp_ns: g as i64 * 10,
                body: format!("id={g}"),
            })
            .collect();
        match state.push_rows(&rows) {
            Err(ReadError::QueryTooBroad(TooBroadReason::MetricResultPoints { count, cap })) => {
                assert_eq!(cap, 22);
                assert_eq!(count, 33, "the error names the breaching reservation");
            }
            other => panic!("expected MetricResultPoints, got {other:?}"),
        }
        assert_eq!(
            state.groups.len(),
            2,
            "the breaching group must never be created"
        );
        assert_eq!(state.result_points, 22, "a refused charge must not stick");
    }

    /// AC 16 — the state's result is a `Vector`, always.
    ///
    /// The former stepped arms of `finish` (the `bucket_grid` absence
    /// walk and the per-bucket `Matrix` emit) are deleted, so there is no
    /// input that makes this state emit a matrix. Driven over all four
    /// shapes: absent and non-absent, fan-out and non-fan-out.
    #[test]
    fn the_instant_state_can_only_emit_a_vector() {
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = ClientWindow::Instant {
            start_ns: 0,
            end_ns: 100,
        };
        let rows = slide_rows(1, &[(10, "a=1"), (20, "a=2"), (30, "a=3")]);
        for (op, value, pipeline) in [
            // non-absent, non-fan-out
            (RangeAggOp::CountOverTime, ClientValue::Count, r#"{x="y"}"#),
            // non-absent, fan-out
            (
                RangeAggOp::MaxOverTime,
                ClientValue::Unwrap,
                r#"{x="y"} | logfmt | unwrap a"#,
            ),
            // absent, non-fan-out
            (RangeAggOp::AbsentOverTime, ClientValue::Count, r#"{x="y"}"#),
        ] {
            for rows in [&rows[..], &[][..]] {
                let client = ClientAgg {
                    pipeline: parse_pipeline(pipeline),
                    value,
                    range_op: op,
                    param: None,
                    absent_labels: vec![("app".to_string(), "a".to_string())],
                    grouping: None,
                };
                let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
                let mut state = ClientAggState::new(
                    &compiled,
                    &meta,
                    &client,
                    instant_of(window),
                    None,
                    AggCaps::DEFAULT,
                )
                .unwrap();
                state.push_rows(rows).expect("fold");
                let out = state.finish();
                assert!(
                    matches!(out, QueryResult::Vector(_)),
                    "{op:?} over {} row(s) must emit a vector, got {out:?}",
                    rows.len()
                );
            }
        }
    }

    /// Every row folds into its group's ONE accumulator, on BOTH
    /// grouping arms.
    ///
    /// The collapse of the per-group `BTreeMap` to a single `BucketAcc`
    /// moved the fold into each arm, so each arm now owns a
    /// seed-or-accumulate decision of its own. A mutant that made the
    /// non-fan-out arm REPLACE rather than fold was caught only by six
    /// `variants(...)` goldens — which drive the same state for an
    /// unrelated reason and would stop covering it the moment those
    /// fixtures changed. This pins the property where it lives, on both
    /// arms and over ops whose value depends on every member.
    #[test]
    fn both_grouping_arms_fold_every_row_into_one_accumulator() {
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = ClientWindow::Instant {
            start_ns: 0,
            end_ns: 100,
        };
        // Four rows, ascending values, all in the one window.
        let rows = slide_rows(1, &[(10, "a=1"), (20, "a=8"), (30, "a=2"), (40, "a=4")]);
        // (op, value kind, non-fan-out pipeline, fan-out pipeline, want)
        // The fan-out pipelines set a CONSTANT label, so both arms
        // produce exactly one group and the only difference between them
        // is which map the accumulator lives in.
        let cases: [(RangeAggOp, ClientValue, &str, &str, f64); 3] = [
            (
                RangeAggOp::CountOverTime,
                ClientValue::Count,
                r#"{x="y"}"#,
                r#"{x="y"} | label_format zone="eu""#,
                4.0,
            ),
            (
                RangeAggOp::BytesOverTime,
                ClientValue::Bytes,
                r#"{x="y"} | line_format "abcde""#,
                r#"{x="y"} | line_format "abcde" | label_format zone="eu""#,
                20.0,
            ),
            (
                RangeAggOp::BytesOverTime,
                ClientValue::Bytes,
                r#"{x="y"}"#,
                r#"{x="y"} | label_format zone="eu""#,
                // "a=1" / "a=8" / "a=2" / "a=4" — 3 bytes each.
                12.0,
            ),
        ];
        for (op, value, plain, mutating, want) in cases {
            for (arm, query) in [("non-fan-out", plain), ("fan-out", mutating)] {
                let client = ClientAgg {
                    pipeline: parse_pipeline(query),
                    value,
                    range_op: op,
                    param: None,
                    absent_labels: vec![],
                    grouping: None,
                };
                let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
                assert_eq!(
                    compiled.metric_mutates_labels(),
                    arm == "fan-out",
                    "{op:?} {arm}: the fixture must take the arm it names"
                );
                let mut state = ClientAggState::new(
                    &compiled,
                    &meta,
                    &client,
                    instant_of(window),
                    None,
                    AggCaps::DEFAULT,
                )
                .unwrap();
                state.push_rows(&rows).expect("fold");
                let QueryResult::Vector(items) = state.finish() else {
                    panic!("instant results are vectors");
                };
                assert_eq!(items.len(), 1, "{op:?} {arm}: one group");
                assert_eq!(
                    items[0].value.to_bits(),
                    want.to_bits(),
                    "{op:?} {arm}: every row must fold into the group's accumulator"
                );
            }
        }
    }

    /// `absent_over_time` instant: the presence FLAG that replaced the
    /// bucket set answers the same question. One surviving line anywhere
    /// suppresses the absence sample; none emits it.
    #[test]
    fn instant_absence_is_a_flag_over_the_whole_selector() {
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = ClientWindow::Instant {
            start_ns: 0,
            end_ns: 100,
        };
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"}"#),
            value: ClientValue::Count,
            range_op: RangeAggOp::AbsentOverTime,
            param: None,
            absent_labels: vec![("app".to_string(), "a".to_string())],
            grouping: None,
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let run = |rows: &[MetricScanRow]| -> usize {
            let mut state = ClientAggState::new(
                &compiled,
                &meta,
                &client,
                instant_of(window),
                None,
                AggCaps::DEFAULT,
            )
            .unwrap();
            state.push_rows(rows).expect("fold");
            let QueryResult::Vector(items) = state.finish() else {
                panic!("instant absence is a vector");
            };
            items.len()
        };
        assert_eq!(run(&[]), 1, "nothing survived => one absence sample");
        assert_eq!(
            run(&slide_rows(1, &[(10, "x")])),
            0,
            "one surviving line anywhere suppresses absence"
        );
        assert_eq!(
            run(&slide_rows(1, &[(10, "x"), (20, "y"), (90, "z")])),
            0,
            "several surviving lines are still just 'present'"
        );
    }

    // -----------------------------------------------------------------
    // Issue #236 Part C — the mutating range path's cell representation.
    // -----------------------------------------------------------------

    /// The three [`MutCells`] arms and the C1 branch predicate, pinned at
    /// the ONE place the representation is chosen.
    ///
    /// The domain is small and enumerated rather than sampled: every
    /// class, and `range` on both sides of `step` plus the exact
    /// boundary, plus the degenerate one-point grid.
    #[test]
    fn the_mutating_cell_representation_is_chosen_by_one_predicate() {
        let arm =
            |class, range: i64, step: u64, kmax: i64| match mut_cells_for(class, range, step, kmax)
            {
                MutCells::IntExpanded(_) => "expanded",
                MutCells::IntDeltas(_) => "deltas",
                MutCells::Samples(_) => "samples",
            };
        // Class A: deltas exactly where the windows overlap AND the grid
        // has more than one point.
        for (range, step, kmax, want) in [
            (9i64, 10u64, 10i64, "expanded"), // range < step
            (10, 10, 10, "deltas"),           // the boundary: range == step
            (11, 10, 10, "deltas"),           // overlapping
            (1_000, 10, 10, "deltas"),        // heavily overlapping
            (1_000, 10, 0, "expanded"),       // one-point grid
            (1_000, 10, -1, "expanded"),      // empty grid
            (1, u64::MAX, 10, "expanded"),    // step wider than any range
        ] {
            assert_eq!(
                arm(ReducerClass::InvertInteger, range, step, kmax),
                want,
                "class A at range={range} step={step} kmax={kmax}"
            );
        }
        // Every other class retains samples, whatever the geometry.
        for class in [ReducerClass::CanonicalFold, ReducerClass::ReduceIndependent] {
            for (range, step, kmax) in [(9i64, 10u64, 10i64), (10, 10, 10), (1_000, 10, 10)] {
                assert_eq!(arm(class, range, step, kmax), "samples", "{class:?}");
            }
        }
    }

    /// AC 12 — **Part C moves no result**, leg 1: the class-A drain
    /// against the UNTOUCHED streaming slider.
    ///
    /// `FpSlide` (the non-mutating path) is not changed by Part C, so it
    /// is the oracle the rewritten mutating drain is measured against:
    /// the same rows through the same reducer, once without a label
    /// mutation (slider) and once with one constant `label_format` label
    /// (fan-out), must emit the identical point SEQUENCE on
    /// `f64::to_bits`. Driven on BOTH C1 branches — `range < step` uses
    /// the expanded arm, `range >= step` the difference array.
    ///
    /// Class A is the only class where both paths exist:
    /// `metric_mutates_labels()` is `mutates_labels || has_unwrap`, so
    /// every unwrap query is a fan-out query by construction. Classes
    /// B/C are covered by the sweep oracle below.
    #[test]
    fn the_class_a_drain_reproduces_the_untouched_streaming_slider() {
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let rows: Vec<MetricScanRow> = (1..=9)
            .map(|i| MetricScanRow {
                fingerprint: 1,
                timestamp_ns: i * 7,
                body: format!("body-{i}"),
            })
            .collect();
        for (step, range) in [(10u64, 5u64), (10, 10), (10, 45), (10, 200)] {
            let window = slide_window(0, 100, step, range);
            for (op, value) in [
                (RangeAggOp::CountOverTime, ClientValue::Count),
                (RangeAggOp::BytesOverTime, ClientValue::Bytes),
                (RangeAggOp::BytesRate, ClientValue::Bytes),
                (RangeAggOp::Rate, ClientValue::Count),
            ] {
                let run = |query: &str| -> Vec<(i64, u64)> {
                    let client = ClientAgg {
                        pipeline: parse_pipeline(query),
                        value,
                        range_op: op,
                        param: None,
                        absent_labels: vec![],
                        grouping: None,
                    };
                    let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
                    let res =
                        run_client_agg_rows(&rows, &compiled, &meta, &client, window, Some(range))
                            .unwrap();
                    let QueryResult::Matrix(items) = res else {
                        panic!("expected a matrix");
                    };
                    assert_eq!(items.len(), 1, "{op:?}: one series");
                    items[0]
                        .points
                        .iter()
                        .map(|(t, v)| (*t, v.to_bits()))
                        .collect()
                };
                // BOTH a non-empty and an EMPTY rendered line. The empty
                // one is what makes `dcount` load-bearing: it gives a
                // COVERED cell the value 0, which must still emit `0`
                // and which a coverage test folded into the value would
                // silently drop. (A mutant that folded them passed
                // against the non-empty fixture alone.)
                for line in ["keep", ""] {
                    let sliding = run(&format!(r#"{{x="y"}} | line_format "{line}""#));
                    let fanned = run(&format!(
                        r#"{{x="y"}} | line_format "{line}" | label_format zone="eu""#
                    ));
                    assert!(
                        !sliding.is_empty(),
                        "{op:?}: the fixture must emit (line {line:?})"
                    );
                    assert_eq!(
                        fanned, sliding,
                        "{op:?} at step={step} range={range} line={line:?}: the rewritten \
                         class-A drain must reproduce the streaming slider bit for bit"
                    );
                }
            }
        }
    }

    /// AC 12 — **Part C moves no result**, leg 2: the class-B/C sweep
    /// against the PER-CELL reduction it replaces.
    ///
    /// Classes B/C have no non-mutating path to compare with (every
    /// unwrap query fans out), so the oracle is the OLD ALGORITHM,
    /// reimplemented here over the state's own retained samples: bucket
    /// each sample into every cell `covering_k` gives it, sort each
    /// bucket with `win_order`, reduce. The sweep must produce that
    /// sequence exactly.
    ///
    /// **This is WEAKER evidence than leg 1, and the difference is not
    /// cosmetic.** Leg 1 compares the class-A drain against an
    /// INDEPENDENT implementation — `FpSlide`, written for a different
    /// path and untouched by Part C — so a shared misconception cannot
    /// hide in it. This leg compares the sweep against a
    /// reimplementation written by the same hand, from the same
    /// understanding, in the same sitting: it catches transcription
    /// errors (wrong bucket, missing sort, off-by-one in the pointers —
    /// mutants 21/22/23 all die here) but it CANNOT catch a
    /// misunderstanding of what `covering_k` means, because both sides
    /// would be wrong together. What backs classes B/C against that is
    /// the untouched corpus and goldens (`b9_range_sliding.test`, every
    /// range case in `logql_metric_agg_golden.rs`, the `differential_*`
    /// files), whose values come from the reference. Do not read the two
    /// legs as the same strength.
    #[test]
    fn the_sample_sweep_reproduces_the_per_cell_reduction() {
        // TWO fingerprints collapsing into ONE output group
        // (`label_format app=...` overrides the only distinguishing
        // label), fed in the scan's FINGERPRINT-MAJOR order. That is what
        // makes the group's retained samples arrive out of `win_order`
        // and the finish-time sort load-bearing: a single-fingerprint
        // fixture pushes them already sorted, and a mutant deleting the
        // sort passed against one.
        let mut meta = slide_meta(1, r#"{"app":"a"}"#);
        meta.insert(
            2,
            StreamMetaRow {
                fingerprint: 2,
                service: "svc".to_string(),
                labels: r#"{"app":"b"}"#.to_string(),
            },
        );
        let mut rows: Vec<MetricScanRow> = (1..=9)
            .map(|i| MetricScanRow {
                fingerprint: 1,
                timestamp_ns: i * 7,
                body: format!("a={}", (i * 13) % 7),
            })
            .collect();
        rows.extend((1..=9).map(|i| MetricScanRow {
            fingerprint: 2,
            timestamp_ns: i * 5 + 2,
            body: format!("a={}", (i * 11) % 5),
        }));
        for (step, range) in [(10u64, 5u64), (10, 10), (10, 45), (10, 200)] {
            let window = slide_window(0, 100, step, range);
            for op in [
                RangeAggOp::MaxOverTime,
                RangeAggOp::MinOverTime,
                RangeAggOp::SumOverTime,
                RangeAggOp::AvgOverTime,
                RangeAggOp::FirstOverTime,
                RangeAggOp::LastOverTime,
                RangeAggOp::StddevOverTime,
                RangeAggOp::QuantileOverTime,
                RangeAggOp::RateCounter,
            ] {
                let client = ClientAgg {
                    pipeline: parse_pipeline(
                        r#"{x="y"} | logfmt | label_format app="same" | unwrap a"#,
                    ),
                    value: ClientValue::Unwrap,
                    range_op: op,
                    param: matches!(op, RangeAggOp::QuantileOverTime).then_some(0.5),
                    absent_labels: vec![],
                    grouping: None,
                };
                let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
                let mut state = RangeSlideState::new(
                    &compiled,
                    &meta,
                    &client,
                    window,
                    Some(range),
                    AggCaps::DEFAULT,
                )
                .unwrap();
                state.push_rows(&rows).expect("fold");
                // Flush the trailing collision group before reading the
                // retained set: `push_rows` leaves the last
                // `(fingerprint, ts)` run buffered until finish, so an
                // oracle built before the flush would be one sample
                // short (it was, and said so).
                let base_labels = std::mem::take(&mut state.base_labels);
                state.flush_collision(&base_labels).expect("flush");
                state.base_labels = base_labels;
                // The oracle, computed the PRE-CHANGE way from exactly
                // the samples the state retained.
                let mut want: Vec<(LabelSet, Vec<(i64, u64)>)> = Vec::new();
                for group in state.groups.values() {
                    let MutCells::Samples(samples) = &group.cells else {
                        panic!("{op:?} must retain samples");
                    };
                    let mut cells: HashMap<i64, Vec<WinSample>> = HashMap::new();
                    for s in samples {
                        let (lo, hi) = state.covering_k(s.ts);
                        for k in lo..=hi {
                            cells.entry(k).or_default().push(*s);
                        }
                    }
                    let mut keys: Vec<i64> = cells.keys().copied().collect();
                    keys.sort_unstable();
                    let mut points: Vec<(i64, u64)> = Vec::new();
                    for k in keys {
                        let pts = cells.get_mut(&k).expect("key present");
                        pts.sort_by(win_order);
                        if let Some(v) = reduce_window(
                            op,
                            state.class,
                            state.param,
                            state.rate_window_ns,
                            0,
                            pts,
                        ) {
                            points.push((state.grid_point(k), v.to_bits()));
                        }
                    }
                    want.push((group.labels.clone(), points));
                }
                want.sort();
                let QueryResult::Matrix(items) = state.finish_in_place().expect("finish") else {
                    panic!("expected a matrix");
                };
                let mut got: Vec<(LabelSet, Vec<(i64, u64)>)> = items
                    .into_iter()
                    .map(|s| {
                        (
                            s.labels,
                            s.points
                                .into_iter()
                                .map(|(t, v)| (t, v.to_bits()))
                                .collect(),
                        )
                    })
                    .collect();
                got.sort();
                want.retain(|(_, p)| !p.is_empty());
                assert!(!want.is_empty(), "{op:?}: the fixture must emit");
                assert_eq!(
                    got, want,
                    "{op:?} at step={step} range={range}: the two-pointer sweep must \
                     reproduce the per-cell reduction it replaces"
                );
            }
        }
    }

    /// AC 13 — **charged retention is independent of the window width.**
    ///
    /// The SAME data at `W = ceil(range/step) = 1, 5, 40` charges the same
    /// number of retention points, where the pre-change per-cell form
    /// charged one per covering cell — a `~W x` factor. The expanded
    /// count is recomputed in-test from `covering_k` so the win is
    /// measured, not asserted from memory.
    #[test]
    fn mutating_retention_is_independent_of_the_window_width() {
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let rows: Vec<MetricScanRow> = (1..=6)
            .map(|i| MetricScanRow {
                fingerprint: 1,
                timestamp_ns: i * 13,
                body: format!("a={i}"),
            })
            .collect();
        for (op, value) in [
            (RangeAggOp::CountOverTime, ClientValue::Count),
            (RangeAggOp::MaxOverTime, ClientValue::Unwrap),
        ] {
            let unwrap = matches!(value, ClientValue::Unwrap);
            let query = if unwrap {
                r#"{x="y"} | logfmt | label_format zone="eu" | unwrap a"#
            } else {
                r#"{x="y"} | logfmt | label_format zone="eu""#
            };
            let client = ClientAgg {
                pipeline: parse_pipeline(query),
                value,
                range_op: op,
                param: None,
                absent_labels: vec![],
                grouping: None,
            };
            let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
            assert!(compiled.metric_mutates_labels());
            let mut charged: Vec<u64> = Vec::new();
            let mut expanded: Vec<u64> = Vec::new();
            for w in [1u64, 5, 40] {
                let step = 10u64;
                let window = slide_window(0, 2_000, step, w * step);
                let mut state =
                    RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                        .unwrap();
                state.push_rows(&rows).expect("fold");
                charged.push(state.retained);
                // What the per-cell form WOULD have charged: one unit per
                // (sample, covering cell), in the same `per_sample` unit.
                let cells: u64 = rows
                    .iter()
                    .map(|r| {
                        let (lo, hi) = state.covering_k(r.timestamp_ns);
                        if lo > hi { 0 } else { (hi - lo + 1) as u64 }
                    })
                    .sum();
                expanded.push(cells * state.per_sample);
                state.finish_in_place().expect("finish");
                assert_eq!(state.retained, 0, "{op:?} W={w}: charge returns to zero");
            }
            assert_eq!(
                charged[0], charged[1],
                "{op:?}: W=1 and W=5 must charge the same, got {charged:?}"
            );
            assert_eq!(
                charged[0], charged[2],
                "{op:?}: W=1 and W=40 must charge the same, got {charged:?}"
            );
            // ...and the per-cell form it replaces grew with W.
            assert!(
                expanded[2] > 10 * charged[2],
                "{op:?}: the fixture must actually exercise a wide window \
                 (per-cell would charge {} vs {} now)",
                expanded[2],
                charged[2]
            );
        }
    }

    /// AC8 (review round 2 gap): the concurrent-retention invariant on the
    /// MUTATING path — every [`MutCells`] arm charges `retained`, and the
    /// whole charge is released when the state is finished/dropped.
    ///
    /// One of the two tests plan v14 permits to change for issue #236 Part
    /// C: it asserts the REPRESENTATION, which C1/C2 replace (`int_cells`
    /// → `MutCells::IntExpanded`/`IntDeltas`, `pt_cells` →
    /// `MutCells::Samples`). The invariant it pins is unchanged.
    #[test]
    fn sliding_mutating_fan_out_charges_and_releases_retention_for_both_cell_kinds() {
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 100, 10, 30);
        // A `label_format` makes the output set differ from the base ⇒ the
        // mutating fan-out path (`fan_out == true`).
        for (op, value, expect_int_cells) in [
            // Class A -> the integer arms.
            (RangeAggOp::CountOverTime, ClientValue::Count, true),
            // Class B -> `MutCells::Samples` (retained points).
            (RangeAggOp::MaxOverTime, ClientValue::Unwrap, false),
            // Class B with a per-emit scratch charge (`per_sample == 2`):
            // the fan-out cells must charge in the SAME unit the streaming
            // slider does, or the discharge at finish cannot balance.
            (RangeAggOp::QuantileOverTime, ClientValue::Unwrap, false),
        ] {
            let pipeline = if matches!(value, ClientValue::Unwrap) {
                parse_pipeline(r#"{x="y"} | logfmt | label_format region="eu" | unwrap a"#)
            } else {
                parse_pipeline(r#"{x="y"} | logfmt | label_format region="eu""#)
            };
            let client = ClientAgg {
                pipeline,
                value,
                range_op: op,
                param: matches!(op, RangeAggOp::QuantileOverTime).then_some(0.5),
                absent_labels: vec![],
                grouping: None,
            };
            let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
            assert!(compiled.metric_mutates_labels(), "must be the fan-out path");
            let mut state =
                RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                    .unwrap();
            let rows: Vec<MetricScanRow> = (1..=5)
                .map(|i| MetricScanRow {
                    fingerprint: 1,
                    timestamp_ns: i * 10,
                    body: "a=1".to_string(),
                })
                .collect();
            state.push_rows(&rows).expect("fan-out fold");
            assert!(
                state.retained > 0,
                "{op:?}: the mutating fan-out must CHARGE retention"
            );
            let group = state.groups.values().next().expect("one output group");
            assert!(!group.cells.is_empty(), "{op:?}: cells populated");
            if expect_int_cells {
                // `range (30) >= step (10)` and `kmax > 0`, so class A is
                // on the DELTA arm here (C1's predicate).
                assert!(
                    matches!(group.cells, MutCells::IntDeltas(_)),
                    "{op:?}: overlapping windows take the delta arm"
                );
                assert_eq!(
                    state.retained,
                    group.cells.charged_units(),
                    "{op:?}: every created int cell is charged exactly once"
                );
            } else {
                assert!(
                    matches!(group.cells, MutCells::Samples(_)),
                    "{op:?}: classes B/C retain samples"
                );
                assert_eq!(
                    state.retained,
                    group.cells.charged_units() * retention_points_per_sample(op),
                    "{op:?}: every retained point is charged exactly once, in `per_sample` units"
                );
            }
            // RELEASE leg (review round 3, finding 3): drive the in-place
            // finish so the discharge is OBSERVABLE — the charge must return
            // to exactly zero. Deleting the discharge in `finish_in_place`
            // reddens this assertion (proved by tripping it).
            let out = state.finish_in_place().expect("finish");
            assert!(matches!(out, QueryResult::Matrix(ref m) if !m.is_empty()));
            assert_eq!(
                state.retained, 0,
                "{op:?}: every mutating-path charge must be discharged at finish"
            );
        }
    }

    /// Review round 5, finding 2: `rate_counter` re-reduces over a BORROW of
    /// the live window instead of copying the whole window into an owned
    /// `Vec<(i64, f64)>` at every grid point. The refactor must be
    /// arithmetically INERT — pinned bit-for-bit (`to_bits`, so a low-bit
    /// float drift or a reordered reset walk reddens) against an INDEPENDENT
    /// transcription of the owned-`Vec` form below. Deliberately NOT compared
    /// against `rate_counter_extrapolated`: that now delegates to the very
    /// function under test, so such a comparison would move with the code and
    /// prove nothing.
    #[test]
    fn rate_counter_over_a_borrowed_window_is_bit_identical_to_the_owning_form() {
        /// The pre-refactor owned-`Vec` implementation, transcribed verbatim
        /// and kept independent of the production code path.
        fn reference(mut pts: Vec<(i64, f64)>, rate_window_ns: Option<u64>) -> f64 {
            if pts.len() < 2 {
                return 0.0;
            }
            let Some(rng_ns) = rate_window_ns.filter(|w| *w > 0) else {
                return 0.0;
            };
            pts.sort_by(|(at, _), (bt, _)| at.cmp(bt));
            let first_t = pts[0].0;
            let last_t = pts[pts.len() - 1].0;
            let first_f = pts[0].1;
            let last_f = pts[pts.len() - 1].1;
            let mut result_value = last_f - first_f;
            let mut last_value = 0.0_f64;
            for &(_, f) in &pts {
                if f < last_value {
                    result_value += last_value;
                }
                last_value = f;
            }
            let sel_range_ms: i128 = (rng_ns / 1_000_000) as i128;
            let range_start = first_t as i128 - sel_range_ms;
            let mut duration_to_start = (first_t as i128 - range_start) as f64 / 1000.0;
            let duration_to_end = 0.0_f64;
            let sampled_interval = (last_t as i128 - first_t as i128) as f64 / 1000.0;
            let average_duration = sampled_interval / (pts.len() - 1) as f64;
            if result_value > 0.0 && first_f >= 0.0 {
                let duration_to_zero = sampled_interval * (first_f / result_value);
                if duration_to_zero < duration_to_start {
                    duration_to_start = duration_to_zero;
                }
            }
            let threshold = average_duration * 1.1;
            let mut extrapolate_to = sampled_interval;
            extrapolate_to += if duration_to_start < threshold {
                duration_to_start
            } else {
                average_duration / 2.0
            };
            extrapolate_to += if duration_to_end < threshold {
                duration_to_end
            } else {
                average_duration / 2.0
            };
            result_value *= extrapolate_to / sampled_interval;
            // `selRange.Seconds()`, transcribed inline (issue #232) rather
            // than calling `range_seconds` — this reference form stays
            // independent of the production code it pins.
            result_value / ((rng_ns / 1_000_000_000) as f64 + (rng_ns % 1_000_000_000) as f64 / 1e9)
        }

        let windows: Vec<Vec<(i64, f64)>> = vec![
            vec![],
            vec![(10, 5.0)],
            vec![(10, 1.0), (20, 2.0), (30, 4.5)],
            // Counter resets (the reset-aware accumulation order matters).
            vec![(10, 9.0), (20, 3.0), (30, 11.25), (40, 2.5), (50, 7.125)],
            // Same-timestamp neighbours (canonical order keeps them adjacent).
            vec![(10, 1.5), (10, 2.5), (10, 0.25), (25, 9.75)],
            // Values that exercise the extrapolation-to-zero branch.
            vec![(0, 0.0), (1_000_000, 3.3), (5_000_000, 3.3000000000000003)],
        ];
        for pts in &windows {
            let ordered: Vec<WinSample> = pts
                .iter()
                .enumerate()
                .map(|(i, &(ts, value))| WinSample {
                    ts,
                    stream_hash: 7,
                    tie_rank: i as u32,
                    value,
                })
                .collect();
            // `1_118_000_000` is a NON-whole-second width, where the
            // reference's two-rounding `Seconds()` and the naive
            // `ns as f64 / 1e9` disagree by an ULP (issue #232) — so the
            // divisor is pinned here too, not just the extrapolation.
            for rate_window_ns in [None, Some(0u64), Some(30_000_000_000), Some(1_118_000_000)] {
                let borrowed = reduce_window(
                    RangeAggOp::RateCounter,
                    ReducerClass::CanonicalFold,
                    None,
                    rate_window_ns,
                    0,
                    &ordered,
                );
                let owned = if ordered.is_empty() {
                    None
                } else {
                    Some(reference(pts.clone(), rate_window_ns))
                };
                match (borrowed, owned) {
                    (None, None) => {}
                    (Some(b), Some(o)) => assert_eq!(
                        b.to_bits(),
                        o.to_bits(),
                        "borrowed {b} != owned {o} for {pts:?} / {rate_window_ns:?}"
                    ),
                    (b, o) => panic!("presence differs: {b:?} vs {o:?} for {pts:?}"),
                }
            }
        }
    }

    /// Review round 5, finding 2: on the MUTATING fan-out path the cap gate
    /// runs BEFORE the cell/point insertion, for both cell kinds — the
    /// allocation the cap exists to refuse is never made. Driven through a
    /// tiny `retention_cap` (materializing four million cells would not be a
    /// test); if the charge were applied after the insert, `retained` would
    /// settle at `cap + 1` and the container would hold the refused entry.
    #[test]
    fn fan_out_retention_cap_refuses_the_breaching_cell_before_inserting_it() {
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        // range 30 / step 10 ⇒ each sample fans into up to 3 grid cells.
        let window = slide_window(0, 100, 10, 30);
        const TINY_CAP: u64 = 4;
        for (op, value, integer) in [
            (RangeAggOp::CountOverTime, ClientValue::Count, true),
            (RangeAggOp::QuantileOverTime, ClientValue::Unwrap, false),
        ] {
            let pipeline = if matches!(value, ClientValue::Unwrap) {
                parse_pipeline(r#"{x="y"} | logfmt | label_format region="eu" | unwrap a"#)
            } else {
                parse_pipeline(r#"{x="y"} | logfmt | label_format region="eu""#)
            };
            let client = ClientAgg {
                pipeline,
                value,
                range_op: op,
                param: matches!(op, RangeAggOp::QuantileOverTime).then_some(0.5),
                absent_labels: vec![],
                grouping: None,
            };
            let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
            let mut state =
                RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                    .unwrap();
            state.caps.retention_points = TINY_CAP;
            let rows: Vec<MetricScanRow> = (1..=20)
                .map(|i| MetricScanRow {
                    fingerprint: 1,
                    timestamp_ns: i * 10,
                    body: "a=1".to_string(),
                })
                .collect();
            match state.push_rows(&rows) {
                Err(ReadError::QueryTooBroad(TooBroadReason::MetricRetention { cap, count })) => {
                    assert_eq!(cap, TINY_CAP);
                    assert!(count > TINY_CAP, "{op:?}: the error names the breach");
                }
                other => panic!("{op:?}: expected MetricRetention, got {other:?}"),
            }
            // What the containers ACTUALLY hold, in charge units.
            let held: u64 = state
                .groups
                .values()
                .map(|g| {
                    g.cells.charged_units()
                        * if integer {
                            1
                        } else {
                            retention_points_per_sample(op)
                        }
                })
                .sum();
            assert_eq!(
                state.retained, held,
                "{op:?}: the counter must match what is actually held"
            );
            assert!(
                held <= TINY_CAP,
                "{op:?}: the breaching cell was inserted anyway ({held} held vs cap {TINY_CAP})"
            );
        }
    }

    /// Review round 2 finding 1: the `absent_over_time` presence counters are
    /// `i64`, whose maximum magnitude is the number of surviving collision
    /// groups — itself bounded by the byte-scan budget ceiling (one byte per
    /// row minimum). This test pins the arithmetic identity the proof rests
    /// on: the counter stays exact well past the old `i32` ceiling.
    #[test]
    fn absent_presence_counter_cannot_overflow_under_the_scan_budget() {
        // The structural bound: rows <= scan-budget bytes < i64::MAX.
        let budget_ceiling: u64 = pulsus_config::LOGQL_SCAN_BUDGET_BYTES_CEILING;
        assert!(
            (budget_ceiling as u128) < i64::MAX as u128,
            "the byte-scan budget ceiling must bound the group count inside i64"
        );
        // And the counter type genuinely holds a value the old `i32` could
        // not: accumulate PAST the i32 ceiling one increment at a time (the
        // exact `+= 1` shape `flush_collision` uses) and stay exact.
        let mut cover: Vec<i64> = vec![0; 2];
        let start = i32::MAX as i64 - 5;
        cover[0] = start;
        for _ in 0..10 {
            cover[0] += 1;
        }
        assert_eq!(
            cover[0],
            start + 10,
            "counter must stay exact past i32::MAX"
        );
        assert!(cover[0] > i32::MAX as i64, "exceeds the old i32 ceiling");
        // The symmetric decrement is equally exact.
        cover[1] -= cover[0];
        assert_eq!(cover[0] + cover[1], 0, "difference array nets to zero");
    }

    /// Review round 3, finding 1: the collision group is bounded in BYTES,
    /// not just member count — a handful of very large same-`(fingerprint,
    /// ts)` bodies trips the clean error long before the 10 000-member cap,
    /// and the staged buffer never exceeds `MAX_TS_COLLISION_GROUP_BYTES`.
    #[test]
    fn collision_group_byte_cap_trips_before_staging_large_bodies() {
        // An order-DEPENDENT reducer, so bodies are staged for `tie_rank`.
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"} | logfmt | unwrap a"#),
            value: ClientValue::Unwrap,
            range_op: RangeAggOp::SumOverTime,
            param: None,
            absent_labels: vec![],
            grouping: None,
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 10, 10, 10);
        let mut state =
            RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                .unwrap();

        // 1 MiB bodies at the SAME (fingerprint, ts): only ~8 fit under the
        // 8 MiB byte cap — far below the 10 000-member cap, so the byte
        // dimension is what bites.
        let big = "x".repeat(1024 * 1024);
        let rows: Vec<MetricScanRow> = (0..64)
            .map(|_| MetricScanRow {
                fingerprint: 1,
                timestamp_ns: 5,
                body: format!("a=1 {big}"),
            })
            .collect();
        match state.push_rows(&rows) {
            Err(ReadError::QueryTooBroad(TooBroadReason::TsCollisionGroup {
                count, cap, ..
            })) => {
                assert_eq!(cap, MAX_TS_COLLISION_GROUP);
                assert!(
                    count < MAX_TS_COLLISION_GROUP,
                    "the BYTE cap must trip well before the member cap, tripped at {count}"
                );
            }
            other => panic!("expected TsCollisionGroup from the byte cap, got {other:?}"),
        }
        // Bound proof, observed: the staged buffer never exceeded the cap.
        assert!(
            state.coll_bytes <= MAX_TS_COLLISION_GROUP_BYTES,
            "staged {} bytes exceeds the {MAX_TS_COLLISION_GROUP_BYTES}-byte cap",
            state.coll_bytes
        );
    }

    /// Review round 4, finding 1: the byte cap covers the FAN-OUT staging
    /// (rendered label JSON + cloned `LabelSet`) for a reducer that stages NO
    /// body — the exact hole in the round-3 accounting. Class A
    /// (`count_over_time`, `needs_body_order == false`) with a label-mutating
    /// pipeline extracting huge label values must still trip the byte cap.
    #[test]
    fn collision_group_byte_cap_covers_fan_out_staging_without_bodies() {
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"} | logfmt | label_format region="eu""#),
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
            grouping: None,
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        assert!(compiled.metric_mutates_labels(), "must be the fan-out path");
        let state_class = reducer_class(client.range_op, client.value);
        assert_eq!(
            state_class,
            ReducerClass::InvertInteger,
            "this case must be the class that stages NO body"
        );
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 10, 10, 10);
        let mut state =
            RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                .unwrap();
        assert!(
            !state.needs_body_order,
            "the hole was that no body ⇒ nothing was charged"
        );
        // Each line carries a ~1 MiB extracted label VALUE — no body is
        // staged, but the rendered JSON + cloned LabelSet are.
        let big = "v".repeat(1024 * 1024);
        let rows: Vec<MetricScanRow> = (0..64)
            .map(|_| MetricScanRow {
                fingerprint: 1,
                timestamp_ns: 5,
                body: format!("big={big}"),
            })
            .collect();
        match state.push_rows(&rows) {
            Err(ReadError::QueryTooBroad(TooBroadReason::TsCollisionGroup {
                count,
                bytes,
                bytes_cap,
                ..
            })) => {
                assert!(
                    count < MAX_TS_COLLISION_GROUP,
                    "the BYTE dimension must trip, not the member count ({count})"
                );
                assert!(bytes > bytes_cap, "the error reports the byte breach");
            }
            other => panic!("fan-out staging must be byte-capped, got {other:?}"),
        }
        assert!(
            state.coll_bytes <= MAX_TS_COLLISION_GROUP_BYTES,
            "staged {} bytes exceeds the cap",
            state.coll_bytes
        );
    }

    /// The byte/count caps must ERROR, never silently truncate a group into a
    /// partial (and therefore wrong) `tie_rank` order — including when the
    /// group STRADDLES a fetch-chunk boundary. Pushing the same group in two
    /// batches must accumulate one continuous charge and fail cleanly.
    #[test]
    fn a_capped_collision_group_errors_rather_than_truncating_across_chunks() {
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"} | logfmt | unwrap a"#),
            value: ClientValue::Unwrap,
            range_op: RangeAggOp::SumOverTime,
            param: None,
            absent_labels: vec![],
            grouping: None,
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 10, 10, 10);
        let mut state =
            RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                .unwrap();
        // ~512 KiB bodies: 5 members charge ~5.3 MiB (the round-7 block
        // model charges 2× the body clone) — under the 8 MiB cap; the
        // second chunk's members push the same open group past it.
        let big = "x".repeat(512 * 1024);
        let chunk: Vec<MetricScanRow> = (0..5)
            .map(|_| MetricScanRow {
                fingerprint: 1,
                timestamp_ns: 5,
                body: format!("a=1 {big}"),
            })
            .collect();
        // First chunk: under the cap, group left OPEN (straddling).
        state.push_rows(&chunk).expect("first chunk under the cap");
        let after_first = state.coll_bytes;
        assert!(after_first > 0, "the straddling group carried a charge");
        assert_eq!(state.coll.len(), 5, "group stayed open across the chunk");
        // Second chunk continues the SAME (fingerprint, ts) group — the charge
        // must accumulate continuously (no reset, no double count) and trip.
        match state.push_rows(&chunk) {
            Err(ReadError::QueryTooBroad(TooBroadReason::TsCollisionGroup { .. })) => {}
            other => panic!("the straddling group must trip the cap cleanly, got {other:?}"),
        }
        assert!(
            state.coll_bytes <= MAX_TS_COLLISION_GROUP_BYTES,
            "the accumulated charge stayed inside the cap"
        );
        // The error is terminal for the query: nothing is emitted from a
        // partially-staged group, so no truncated tie order can escape.
        assert!(
            state.coll_bytes >= after_first,
            "charge accumulated, not reset"
        );
    }

    /// Review round 5, finding 1: `member_stage_bytes` must be a true UPPER
    /// BOUND on what `stage_member` ACTUALLY allocates. Non-tautological by
    /// construction — the fixtures drive the real `stage_member` and the
    /// assertion measures the live `capacity()` of every buffer it created
    /// (`coll`'s element buffer, the body clone, the rendered JSON, the
    /// cloned `LabelSet` and each of its owned strings), then compares that
    /// to the charge the caps were checked against.
    ///
    /// The fixtures are the inputs that beat the round-4 accounting: an
    /// all-control-character label value (six rendered bytes per input byte,
    /// counted once before), many one-byte labels (whose real allocations
    /// cannot go below the allocator's minimum block), and enough members to
    /// force `coll`'s capacity to double repeatedly.
    #[test]
    fn member_stage_bytes_upper_bounds_every_allocation_stage_member_makes() {
        /// The live heap footprint of everything currently staged in `coll`.
        fn staged_capacity(state: &RangeSlideState<'_>) -> u64 {
            let mut n = (state.coll.capacity() * size_of::<CollMember>()) as u64;
            for m in &state.coll {
                n += m.body.capacity() as u64;
                if let Some((key, labels)) = &m.out {
                    n += key.capacity() as u64;
                    n += (labels.capacity() * size_of::<(String, String)>()) as u64;
                    for (k, v) in labels {
                        n += (k.capacity() + v.capacity()) as u64;
                    }
                }
            }
            n
        }

        // Class C (`sum_over_time` over `unwrap`) ⇒ bodies staged for
        // `tie_rank`; `label_format` ⇒ the fan-out `key`/`LabelSet` staged
        // too. Both legs of the charge are exercised at once.
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"} | logfmt | label_format region="eu" | unwrap a"#),
            value: ClientValue::Unwrap,
            range_op: RangeAggOp::SumOverTime,
            param: None,
            absent_labels: vec![],
            grouping: None,
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 10, 10, 10);

        let control: String = std::iter::repeat_n('\u{1}', 512).collect();
        struct Fixture {
            name: &'static str,
            body: String,
            labels: Vec<(String, String)>,
        }
        let fixtures = vec![
            // Worst-case escaping: every value byte renders as six.
            Fixture {
                name: "all-control-character values",
                body: "a=1".to_string(),
                labels: vec![
                    ("k".to_string(), control.clone()),
                    ("kk".to_string(), control.clone()),
                ],
            },
            // Minimum-block worst case: 64 one-byte keys and values.
            Fixture {
                name: "many one-byte labels",
                body: "a=1".to_string(),
                labels: (0..64u32)
                    .map(|i| {
                        (
                            char::from(b'a' + (i % 26) as u8).to_string(),
                            "x".to_string(),
                        )
                    })
                    .collect(),
            },
            // No labels at all (charge must still cover the slot + body).
            Fixture {
                name: "no labels",
                body: "a=1 ".repeat(300),
                labels: Vec::new(),
            },
            // Mixed escapes and multi-byte UTF-8.
            Fixture {
                name: "mixed escapes and multi-byte",
                body: "a=1".to_string(),
                labels: vec![
                    ("q\"k".to_string(), "v\\\n\t\u{7}".to_string()),
                    ("日本".to_string(), "🚀".repeat(40)),
                ],
            },
        ];

        for Fixture { name, body, labels } in &fixtures {
            let mut state =
                RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                    .unwrap();
            assert!(state.needs_body_order, "{name}: bodies must be staged");
            assert!(state.fan_out, "{name}: label output must be staged");
            // Enough members to force `coll` to grow through several
            // capacity doublings (4 → 8 → 16 → 32).
            for _ in 0..20 {
                let row = MetricScanRow {
                    fingerprint: 1,
                    timestamp_ns: 5,
                    body: body.clone(),
                };
                let mut scratch: Vec<(Cow<'_, str>, Cow<'_, str>)> = labels
                    .iter()
                    .map(|(k, v)| (Cow::Borrowed(k.as_str()), Cow::Borrowed(v.as_str())))
                    .collect();
                state.stage_member(&row, 1.0, &mut scratch).expect("staged");
                assert!(
                    state.coll_bytes >= staged_capacity(&state),
                    "{name}: charge {} under-counts the {} bytes actually allocated \
                     after {} members",
                    state.coll_bytes,
                    staged_capacity(&state),
                    state.coll.len()
                );
            }
        }
    }

    /// Issue #227 review round 7, finding 2 (the container-slot leg of the
    /// same allocator-rounding model): the per-member `CollMember` slot
    /// charge must cover the `coll` buffer's SIZE-CLASS-ROUNDED realloc
    /// peak — live capacity (doubling growth, initial `min_non_zero_cap`
    /// of 4) plus the old buffer still mapped during the realloc, each
    /// retained at up to the worst mainstream block for its request.
    /// Drives the REAL `member_stage_bytes` (a class-A state stages
    /// nothing but the slot, so the returned charge IS the per-member
    /// slot charge) and replays `Vec`'s exact growth schedule; reverting
    /// the slot factor to the pre-round-7 `4×` fails at `n = 1` and at
    /// every realloc point.
    #[test]
    fn coll_member_slot_charge_covers_the_size_class_rounded_realloc_peak() {
        fn worst_mainstream_retained(n: u64) -> u64 {
            let pow2_class = n.max(1).next_power_of_two();
            let glibc_chunk = (n + 8).div_ceil(16) * 16;
            pow2_class.max(glibc_chunk).max(32)
        }

        // A class-A reducer with no fan-out: no body staged, no labels
        // staged — `member_stage_bytes` returns exactly the slot charge.
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"}"#),
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
            grouping: None,
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 100, 10, 30);
        let state = RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
            .unwrap();
        assert!(!state.needs_body_order && !state.fan_out);
        let row = MetricScanRow {
            fingerprint: 1,
            timestamp_ns: 5,
            body: "x".to_string(),
        };
        let slot_charge = state.member_stage_bytes(&row, &[], false);

        let slot = size_of::<CollMember>() as u64;
        let mut cap: u64 = 0;
        for n in 1..=1_000u64 {
            // `RawVec`: first push reserves `min_non_zero_cap = 4`; a full
            // buffer doubles, with the old buffer live during the realloc.
            let old = if n == 1 {
                cap = 4;
                0
            } else if n > cap {
                let old = cap;
                cap *= 2;
                old
            } else {
                0
            };
            let peak = worst_mainstream_retained(cap * slot)
                + if old > 0 {
                    worst_mainstream_retained(old * slot)
                } else {
                    0
                };
            let charged = slot_charge * n;
            assert!(
                charged >= peak,
                "after {n} members the cumulative slot charge {charged} \
                 under-counts the rounded realloc peak {peak}"
            );
        }
    }

    /// Review round 6: distinct mutating-label groups' rendered keys +
    /// `LabelSet`s are QUERY-LIFETIME (they live in `groups` until finish,
    /// outliving the per-flush `coll_bytes` reset), so they are charged
    /// against the group-byte cap BEFORE the map insertion — a refused
    /// group is never inserted and the counter never records it.
    #[test]
    fn query_lifetime_group_label_bytes_are_capped_before_the_map_insertion() {
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"} | logfmt | label_format region="eu""#),
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
            grouping: None,
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 100, 10, 30);
        let mut state =
            RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                .unwrap();
        state.caps.group_bytes = 1;
        // Distinct extracted `u` values ⇒ distinct groups; distinct ts ⇒
        // singleton collision groups (the collision caps stay silent).
        let rows: Vec<MetricScanRow> = (1..=3)
            .map(|i| MetricScanRow {
                fingerprint: 1,
                timestamp_ns: i * 10,
                body: format!("u=v{i}"),
            })
            .collect();
        match state.push_rows(&rows) {
            Err(ReadError::QueryTooBroad(TooBroadReason::MetricGroupLabelBytes { bytes, cap })) => {
                assert_eq!(cap, 1);
                assert!(bytes > cap, "the error names the byte breach");
            }
            other => panic!("expected MetricGroupLabelBytes, got {other:?}"),
        }
        // Charge-before-insert, observed: the refused group was never
        // inserted and the counter never recorded it.
        assert!(
            state.groups.is_empty(),
            "the breaching group was inserted anyway"
        );
        assert_eq!(state.group_bytes, 0, "a refused charge must not stick");
    }

    /// Issue #260 AC 3 — the group-byte multiplicity of **2** is
    /// EXERCISED, not claimed: on a folded range query the slider and the
    /// fold hold two independent counters against
    /// [`MAX_CLIENT_AGG_GROUP_BYTES`] at the same time, so the bytes the
    /// cap proves are the SUM ([`MAX_LEAF_RETAINED_BYTES`]'s first term),
    /// never the cap.
    ///
    /// The fold takes its cap from `caps.group_bytes` at `attach_fold`,
    /// so attaching under a tight cap and restoring the slider's
    /// afterwards gives the two counters DIFFERENT ceilings — which is
    /// what makes their independence observable. Two groups sized so the
    /// first fits the fold's cap and the second does not: at the breach
    /// BOTH counters are non-zero, and the error names the FOLD's cap
    /// while the slider's is untouched.
    ///
    /// *Rejects a claim of 2 that is really 1* — one shared counter would
    /// report the slider's cap, or would already have breached.
    #[test]
    fn the_slider_and_the_fold_hold_two_independent_group_byte_counters() {
        const FOLD_GROUP_CAP: u64 = 5_000;

        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"} | logfmt | label_format keep="1""#),
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
            grouping: None,
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 100, 10, 30);
        let mut state =
            RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                .unwrap();

        // The fold is attached under the tight cap; the slider keeps the
        // shipped one. Two ceilings, one constant.
        state.caps.group_bytes = FOLD_GROUP_CAP;
        let spec: plan::VectorAggSpec = (
            VectorAggOp::Sum,
            Some(pulsus_logql::Grouping {
                kind: GroupingKind::By,
                labels: vec!["u".to_string()],
            }),
            None,
        );
        state.attach_fold(&spec);
        state.caps.group_bytes = MAX_CLIENT_AGG_GROUP_BYTES;
        assert_eq!(
            state
                .fold
                .as_ref()
                .and_then(|f| f.group_byte_counter())
                .map(|(_, cap)| cap),
            Some(FOLD_GROUP_CAP),
            "the fold must carry its own cap"
        );

        // Group "a" is tiny and fits; group "b…" carries a 10 000-byte
        // value, so its key alone exceeds the fold's cap. `finish_in_place`
        // hands groups to the fold in label-ascending order, so "a" lands
        // first.
        let big = "b".repeat(10_000);
        let rows = slide_rows(1, &[(10, "u=a"), (20, &format!("u={big}"))]);
        state
            .push_rows(&rows)
            .expect("the slider's own cap is ample");
        assert!(
            state.group_bytes > 0,
            "the slider must be holding its own group bytes"
        );

        let err = state
            .finish_in_place()
            .expect_err("the fold's cap must refuse the wide group");
        match err {
            ReadError::QueryTooBroad(TooBroadReason::MetricGroupLabelBytes { bytes, cap }) => {
                assert_eq!(cap, FOLD_GROUP_CAP, "the breach names the FOLD's cap");
                assert!(bytes > cap);
            }
            other => panic!("expected MetricGroupLabelBytes, got {other:?}"),
        }
        // BOTH counters are live at the moment of breach — the whole
        // point: the slider's bytes are invisible to the fold's ceiling
        // and vice versa.
        let (fold_bytes, fold_cap) = state
            .fold
            .as_ref()
            .and_then(|f| f.group_byte_counter())
            .expect("the fold survives a refused push");
        assert!(
            fold_bytes > 0,
            "the fold's counter must hold the group that DID fit"
        );
        assert!(
            state.group_bytes > 0,
            "the slider's counter must still hold the un-drained group"
        );
        assert_ne!(
            fold_cap, state.caps.group_bytes,
            "two counters, two ceilings — collapsing them would make this one"
        );
        assert_eq!(state.caps.group_bytes, MAX_CLIENT_AGG_GROUP_BYTES);
    }

    /// Review round 6 symmetry: every insertion into the query-lifetime
    /// `groups` map passes the charge (the counter equals the sum of
    /// [`group_entry_bytes`] over the LIVE map — no bypassing path, no
    /// double charge for a repeated group), and `finish_in_place` releases
    /// every charge back to exactly zero. Deleting either the
    /// `fan_out_sample` charge or the finish discharge fails this test.
    #[test]
    fn group_label_byte_charges_match_the_live_map_and_release_at_finish() {
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"} | logfmt | label_format region="eu""#),
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
            grouping: None,
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 100, 10, 30);
        let mut state =
            RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                .unwrap();
        // u=a twice (one group, charged ONCE), u=b once; the final u=c row
        // stays staged in `coll` until finish flushes — proving the
        // finish-time flush passes the same charge.
        let bodies = ["u=a", "u=b", "u=a", "u=c"];
        let rows: Vec<MetricScanRow> = bodies
            .iter()
            .enumerate()
            .map(|(i, b)| MetricScanRow {
                fingerprint: 1,
                timestamp_ns: (i as i64 + 1) * 10,
                body: (*b).to_string(),
            })
            .collect();
        state.push_rows(&rows).expect("under every cap");
        assert_eq!(state.groups.len(), 2, "u=a (deduped) and u=b are flushed");
        let live: u64 = state
            .groups
            .iter()
            .map(|(k, g)| group_entry_bytes(k, &g.labels, MUT_GROUP_SLOT))
            .sum();
        assert!(live > 0, "the fixture must exercise a real charge");
        assert_eq!(
            state.group_bytes, live,
            "the counter must equal the live map's sized entries — every \
             insertion charged, repeated groups charged once"
        );
        let out = state.finish_in_place().expect("finish");
        assert_eq!(
            state.group_bytes, 0,
            "every group-label byte charge must be discharged at finish"
        );
        assert_eq!(state.retained, 0, "retention symmetry is undisturbed");
        match out {
            QueryResult::Matrix(series) => {
                assert_eq!(series.len(), 3, "u=a, u=b and the finish-flushed u=c");
            }
            other => panic!("expected a matrix, got {other:?}"),
        }
    }

    /// Review round 6: [`group_entry_bytes`] is a true UPPER BOUND on the
    /// heap bytes a retained group-map entry actually holds.
    /// Non-tautological — the charge is sized from LENGTHS while the
    /// assertion measures the live `capacity()` of the real entry produced
    /// by the real staging/flush path (the rendered key's capacity exceeds
    /// its length whenever escaping forces growth past the renderer's
    /// raw-byte pre-size, so an exact-length key charge fails here).
    #[test]
    fn group_entry_bytes_upper_bounds_the_retained_map_entry() {
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"} | logfmt | label_format region="eu""#),
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
            grouping: None,
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 10, 10, 10);

        let control: String = std::iter::repeat_n('\u{1}', 512).collect();
        let fixtures: Vec<(&str, Vec<(String, String)>)> = vec![
            // Worst-case escaping: the rendered key grows geometrically
            // past its raw-byte pre-size (six rendered bytes per input
            // byte), leaving capacity well above length.
            (
                "all-control-character values",
                vec![
                    ("k".to_string(), control.clone()),
                    ("kk".to_string(), control.clone()),
                ],
            ),
            // Minimum-block worst case: many one-byte strings.
            (
                "many one-byte labels",
                (0..64u32)
                    .map(|i| {
                        (
                            char::from(b'a' + (i % 26) as u8).to_string(),
                            "x".to_string(),
                        )
                    })
                    .collect(),
            ),
            // One huge extracted value (the adversarial shape the round-6
            // finding names: multi-MiB label sets × the series cap).
            (
                "one huge value",
                vec![("big".to_string(), "v".repeat(1_000_000))],
            ),
            // No labels at all (charge still covers key + slot).
            ("no labels", Vec::new()),
            // Mixed escapes and multi-byte UTF-8.
            (
                "mixed escapes and multi-byte",
                vec![
                    ("q\"k".to_string(), "v\\\n\t\u{7}".to_string()),
                    ("日本".to_string(), "🚀".repeat(40)),
                ],
            ),
        ];

        for (name, labels) in &fixtures {
            let mut state =
                RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                    .unwrap();
            assert!(state.fan_out, "{name}: must be the fan-out path");
            let row = MetricScanRow {
                fingerprint: 1,
                timestamp_ns: 5,
                body: "a=1".to_string(),
            };
            let mut scratch: Vec<(Cow<'_, str>, Cow<'_, str>)> = labels
                .iter()
                .map(|(k, v)| (Cow::Borrowed(k.as_str()), Cow::Borrowed(v.as_str())))
                .collect();
            state.coll_active = true;
            state.coll_fp = 1;
            state.coll_ts = 5;
            state.stage_member(&row, 1.0, &mut scratch).expect("staged");
            state.flush_collision(&HashMap::new()).expect("flushed");
            assert_eq!(state.groups.len(), 1, "{name}: one retained group");
            for (key, g) in &state.groups {
                // Everything measurable the entry retains (the map-table
                // share is an unmeasurable documented margin, like
                // `MIN_ALLOC_BYTES`).
                let measured = key.capacity() as u64
                    + (g.labels.capacity() * size_of::<(String, String)>()) as u64
                    + g.labels
                        .iter()
                        .map(|(k, v)| (k.capacity() + v.capacity()) as u64)
                        .sum::<u64>();
                let charge = group_entry_bytes(key, &g.labels, MUT_GROUP_SLOT);
                assert!(
                    charge >= measured,
                    "{name}: charge {charge} under-counts the {measured} bytes \
                     actually retained"
                );
                assert_eq!(
                    state.group_bytes, charge,
                    "{name}: the counter carries exactly this entry's charge"
                );
            }
        }
    }

    /// Issue #236 P1's charge, gated BEHAVIOURALLY.
    ///
    /// The non-mutating instant arm (`fp_groups`) was count-gated only
    /// before #236; Part A deleted the count cap and P1 put a byte charge
    /// in its place. Found by a mutant: deleting that charge outright was
    /// caught by NOTHING except the `logql_variants_alloc` frame census —
    /// a structural gate that notices the callee set changed, not that
    /// the bound is gone. `discharge_group_bytes` saturates, so the
    /// finish-time `group_bytes == 0` post-condition still holds with the
    /// charge removed, and both existing group-byte tests drive the two
    /// FAN-OUT arms. Plan v14 AC 14(b) asks for exactly this case; it did
    /// not land with Part A.
    #[test]
    fn instant_fp_groups_are_byte_charged_capped_and_released_at_finish() {
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"} | line_format "keep""#),
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
            grouping: None,
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        assert!(
            !compiled.metric_mutates_labels(),
            "must be the NON-fan-out (fp_groups) path"
        );
        // Two fingerprints with real label bytes to charge for.
        let mut meta = slide_meta(1, r#"{"app":"a","region":"eu-west-1"}"#);
        meta.insert(
            2,
            StreamMetaRow {
                fingerprint: 2,
                service: "svc".to_string(),
                labels: r#"{"app":"b","region":"eu-west-2"}"#.to_string(),
            },
        );
        let window = ClientWindow::Instant {
            start_ns: 0,
            end_ns: 100,
        };
        let row = |fp: u64, ts: i64| MetricScanRow {
            fingerprint: fp,
            timestamp_ns: ts,
            body: "hello".to_string(),
        };

        // Trip leg: a tiny cap refuses the FIRST group BEFORE insertion.
        let mut state = ClientAggState::new(
            &compiled,
            &meta,
            &client,
            instant_of(window),
            None,
            AggCaps::DEFAULT,
        )
        .unwrap();
        state.caps.group_bytes = 1;
        match state.push_rows(&[row(1, 5)]) {
            Err(ReadError::QueryTooBroad(TooBroadReason::MetricGroupLabelBytes { bytes, cap })) => {
                assert_eq!(cap, 1);
                assert!(bytes > cap, "the error names the byte breach");
            }
            other => panic!("expected MetricGroupLabelBytes, got {other:?}"),
        }
        assert!(
            state.fp_groups.is_empty(),
            "the breaching group was inserted anyway"
        );
        assert_eq!(state.group_bytes, 0, "a refused charge must not stick");

        // Charge/release leg: the counter equals the live map (a repeated
        // fingerprint charged once), then finish releases every charge —
        // its `debug_assert_eq!(group_bytes, 0)` is the release gate.
        let mut state = ClientAggState::new(
            &compiled,
            &meta,
            &client,
            instant_of(window),
            None,
            AggCaps::DEFAULT,
        )
        .unwrap();
        state
            .push_rows(&[row(1, 5), row(2, 6), row(1, 7)])
            .expect("under every cap");
        assert_eq!(state.fp_groups.len(), 2);
        let live: u64 = state
            .fp_groups
            .keys()
            .map(|fp| {
                group_entry_bytes(
                    "",
                    state.base_labels.get(fp).expect("hydrated"),
                    FP_GROUP_SLOT,
                )
            })
            .sum();
        assert!(live > 0, "the fixture must exercise a real charge");
        assert_eq!(
            state.group_bytes, live,
            "every fingerprint charged; the repeated one charged once"
        );
        match state.finish() {
            QueryResult::Vector(samples) => assert_eq!(samples.len(), 2),
            other => panic!("expected a vector, got {other:?}"),
        }
    }

    /// Review round 6, class completion: the INSTANT fan-out path retains
    /// the same query-lifetime key/`LabelSet` state in `label_groups`
    /// (count-capped, bytes previously uncharged) — same charge, same
    /// helper, same symmetric release at finish (whose
    /// `debug_assert_eq!(group_bytes, 0)` fails this test if the discharge
    /// is deleted).
    #[test]
    fn instant_fan_out_groups_are_byte_charged_capped_and_released_at_finish() {
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"} | logfmt | label_format region="eu""#),
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
            grouping: None,
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        assert!(compiled.metric_mutates_labels(), "must be the fan-out path");
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = ClientWindow::Instant {
            start_ns: 0,
            end_ns: 100,
        };

        // Trip leg: a tiny cap refuses the FIRST group before insertion.
        let mut state = ClientAggState::new(
            &compiled,
            &meta,
            &client,
            instant_of(window),
            None,
            AggCaps::DEFAULT,
        )
        .unwrap();
        state.caps.group_bytes = 1;
        let row = |ts: i64, body: &str| MetricScanRow {
            fingerprint: 1,
            timestamp_ns: ts,
            body: body.to_string(),
        };
        match state.push_rows(&[row(5, "u=a")]) {
            Err(ReadError::QueryTooBroad(TooBroadReason::MetricGroupLabelBytes { bytes, cap })) => {
                assert_eq!(cap, 1);
                assert!(bytes > cap, "the error names the byte breach");
            }
            other => panic!("expected MetricGroupLabelBytes, got {other:?}"),
        }
        assert!(
            state.label_groups.is_empty(),
            "the breaching group was inserted anyway"
        );
        assert_eq!(state.group_bytes, 0, "a refused charge must not stick");

        // Charge/release leg: the counter equals the live map (repeated
        // group charged once), then finish releases every charge.
        let mut state = ClientAggState::new(
            &compiled,
            &meta,
            &client,
            instant_of(window),
            None,
            AggCaps::DEFAULT,
        )
        .unwrap();
        state
            .push_rows(&[row(5, "u=a"), row(6, "u=b"), row(7, "u=a")])
            .expect("under every cap");
        assert_eq!(state.label_groups.len(), 2);
        let live: u64 = state
            .label_groups
            .iter()
            .map(|(k, (labels, _))| group_entry_bytes(k, labels, INSTANT_GROUP_SLOT))
            .sum();
        assert!(live > 0, "the fixture must exercise a real charge");
        assert_eq!(
            state.group_bytes, live,
            "every insertion charged; the repeated group charged once"
        );
        match state.finish() {
            QueryResult::Vector(samples) => assert_eq!(samples.len(), 2),
            other => panic!("expected a vector, got {other:?}"),
        }
    }

    /// A group of ordinary-sized bodies is unaffected by the byte cap (it
    /// must not reject anything real).
    #[test]
    fn collision_group_byte_cap_does_not_reject_ordinary_bodies() {
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"} | logfmt | unwrap a"#),
            value: ClientValue::Unwrap,
            range_op: RangeAggOp::SumOverTime,
            param: None,
            absent_labels: vec![],
            grouping: None,
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 10, 10, 10);
        // Bodies differ in BYTES (trailing padding) but parse to the same
        // label set, so they form ONE 500-member collision group — the shape
        // the byte cap must not reject.
        let rows: Vec<MetricScanRow> = (0..500)
            .map(|i| MetricScanRow {
                fingerprint: 1,
                timestamp_ns: 5,
                body: format!("a=1{}", " ".repeat(i % 7)),
            })
            .collect();
        let res = run_client_agg_rows(&rows, &compiled, &meta, &client, window, None)
            .expect("500 ordinary same-ns bodies must be served");
        assert_eq!(one_series_points(res).1, vec![(10, 500.0)]);
    }

    /// Issue #227 arithmetic sweep: `rate_counter` over EXTREME timestamps
    /// (near `i64::MIN`/`i64::MAX`) must not panic in debug or wrap in
    /// release — the spans are computed in i128 (review finding 2).
    #[test]
    fn rate_counter_extreme_timestamps_do_not_overflow() {
        // A first sample near i64::MIN underflows `first_t - sel_range_ms`.
        let v = rate_counter_extrapolated(
            vec![(i64::MIN + 1, 1.0), (i64::MIN + 1_000, 2.0)],
            Some(60_000_000_000),
        );
        assert!(v.is_finite(), "near-i64::MIN must not overflow, got {v}");
        // A window spanning near-MIN to near-MAX overflows `last_t - first_t`.
        let v = rate_counter_extrapolated(
            vec![(i64::MIN + 1, 1.0), (i64::MAX - 1, 2.0)],
            Some(60_000_000_000),
        );
        assert!(v.is_finite(), "full-i64-span must not overflow, got {v}");
    }

    /// Issue #227 review finding 1: `absent_over_time` retains NOTHING
    /// per-sample — its presence state is the O(grid) coverage array, so a
    /// dense selector cannot grow memory with the scan. Drives a dense scan
    /// and asserts both the correct gaps and a bounded presence footprint.
    #[test]
    fn sliding_absent_presence_is_grid_bounded_not_scan_bounded() {
        let client = ClientAgg {
            pipeline: vec![],
            value: ClientValue::Count,
            range_op: RangeAggOp::AbsentOverTime,
            param: None,
            absent_labels: vec![("app".to_string(), "a".to_string())],
            grouping: None,
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 100, 10, 10);
        let mut state =
            RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                .unwrap();
        // 20_000 dense samples covering only the second half of the grid.
        let rows: Vec<MetricScanRow> = (0..20_000)
            .map(|i| MetricScanRow {
                fingerprint: 1,
                timestamp_ns: 51 + (i % 50),
                body: "x".to_string(),
            })
            .collect();
        state.push_rows(&rows).expect("dense absent scan");
        // The presence state is exactly the grid-sized array — never 20_000.
        assert_eq!(
            state.present_cover.len(),
            (window.end_ns() / 10 + 2) as usize,
            "presence state must be grid-sized, independent of scan density"
        );
        assert_eq!(state.retained, 0, "absent retains no per-sample points");
        let QueryResult::Matrix(series) = state.finish().expect("finish") else {
            panic!("expected a matrix");
        };
        // Grid {0,10,...,100}; samples occupy (50,100] ⇒ grid points 60..=100
        // are covered, 0..=50 are absent.
        let pts: Vec<i64> = series[0].points.iter().map(|(t, _)| *t).collect();
        assert_eq!(pts, vec![0, 10, 20, 30, 40, 50]);
    }

    /// Issue #227 review round 7, finding 1 (the end-to-end shape): a
    /// range request of exactly 11_000 intervals must be SERVED — the
    /// request guard admits it and the engine's grid guard must not then
    /// 422 it. Drives the REAL evaluators at the limit: the sliding state
    /// accepts and emits over the full 11_001-point grid, and the
    /// `vector(n)` grid materializes all 11_001 points.
    #[test]
    fn a_range_query_of_exactly_11000_intervals_is_served_not_422() {
        const S: i64 = 1_000_000_000;
        let step = super::super::params::validate_duration_ns(S as u64, "step").unwrap();

        // The leafless vector(n) path: full inclusive grid, both endpoints.
        let window = GridWindow {
            start_ns: 0,
            end_ns: 11_000 * S,
            step_ns: Some(step),
        };
        let res = materialize_vector_lit(1.0, &window)
            .expect("exactly 11_000 intervals must be served (the reference serves it)");
        let QueryResult::Matrix(series) = res else {
            panic!("expected a matrix, got {res:?}");
        };
        assert_eq!(series[0].points.len(), 11_001);
        assert_eq!(series[0].points.first().unwrap().0, 0);
        assert_eq!(series[0].points.last().unwrap().0, 11_000 * S);

        // The sliding data path: RangeSlideState::new admits the same
        // window (kmax = 11_000) instead of tripping MetricBuckets.
        let client = ClientAgg {
            range_op: RangeAggOp::CountOverTime,
            value: ClientValue::Count,
            param: None,
            pipeline: vec![],
            absent_labels: vec![],
            grouping: None,
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 11_000 * S, S as u64, S as u64);
        let state = RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
            .expect("the engine must admit every request the HTTP guard admits");
        assert_eq!(state.kmax, 11_000);

        // One interval over IS the engine's 422 (the backstop still holds
        // for anything that bypasses the HTTP boundary).
        let window = slide_window(0, 11_001 * S, S as u64, S as u64);
        match RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT) {
            Err(ReadError::QueryTooBroad(TooBroadReason::MetricBuckets { buckets, cap })) => {
                assert_eq!(buckets, 11_001);
                assert_eq!(cap, MAX_CLIENT_AGG_BUCKETS);
            }
            Err(other) => panic!("expected MetricBuckets, got {other:?}"),
            Ok(_) => panic!("one interval over the fence must be rejected"),
        }
    }

    /// Issue #227 review round 8 (the end-to-end shape at the domain
    /// edge): a full-domain range request at a 1_000_000s step must be
    /// SERVED — the saturating fence admits it and the REAL evaluators
    /// emit the exact 18_447-point start-anchored grid over the true
    /// span, every point in `[start, end]`, no wrap.
    #[test]
    fn a_full_domain_range_query_under_the_saturated_fence_is_served() {
        const STEP: u64 = 1_000_000_000_000_000; // 1_000_000s in ns
        let step = super::super::params::validate_duration_ns(STEP, "step").unwrap();
        // i64::MIN + 18_446·step, in i128 (the product alone overflows i64).
        let last_point = (i64::MIN as i128 + 18_446 * STEP as i128) as i64;

        // The leafless vector(n) path: the exact inclusive true-span grid.
        let window = GridWindow {
            start_ns: i64::MIN,
            end_ns: i64::MAX,
            step_ns: Some(step),
        };
        let res = materialize_vector_lit(1.0, &window)
            .expect("the saturating fence must admit the full-domain span");
        let QueryResult::Matrix(series) = res else {
            panic!("expected a matrix, got {res:?}");
        };
        assert_eq!(series[0].points.len(), 18_447);
        assert_eq!(series[0].points.first().unwrap().0, i64::MIN);
        assert_eq!(series[0].points.last().unwrap().0, last_point);
        assert!(last_point < i64::MAX); // in-domain, short of `end`

        // The sliding data path: RangeSlideState::new admits the same
        // window with the same exact grid (kmax = 18_446).
        let client = ClientAgg {
            range_op: RangeAggOp::CountOverTime,
            value: ClientValue::Count,
            param: None,
            pipeline: vec![],
            absent_labels: vec![],
            grouping: None,
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(i64::MIN, i64::MAX, STEP, STEP);
        let state = RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
            .expect("the engine must admit every request the HTTP guard admits");
        assert_eq!(state.kmax, 18_446);
    }

    /// Review round 1, finding 1 (quantile bound): the exact-quantile
    /// retention cap trips as a NAMED too-broad error the moment the
    /// value count crosses [`MAX_QUANTILE_VALUES`] — driven through the
    /// real `push_rows` fold with the counter pre-charged to the
    /// boundary (a 4M-row fixture would be pure waste).
    #[test]
    fn quantile_value_retention_past_the_cap_is_a_named_too_broad_error() {
        let parsed =
            pulsus_logql::parse(r#"quantile_over_time(0.5, {a="b"} | logfmt | unwrap v [1m])"#)
                .expect("parse");
        // Issue #272: `impl Drop for MetricExpr` forbids moving out of a
        // field (E0509), so this re-binds through a reference.
        let stages = match &parsed {
            Expr::Metric(pulsus_logql::MetricExpr::Range { range, .. }) => {
                range.selector.pipeline.clone()
            }
            other => panic!("unexpected expr: {other:?}"),
        };
        let compiled = super::super::pipeline::CompiledPipeline::compile(&stages).expect("compile");
        let client = plan::ClientAgg {
            pipeline: stages,
            value: plan::ClientValue::Unwrap,
            range_op: RangeAggOp::QuantileOverTime,
            param: Some(0.5),
            absent_labels: Vec::new(),
            grouping: None,
        };
        let meta = HashMap::from([(
            1u64,
            StreamMetaRow {
                fingerprint: 1,
                service: "checkout".to_string(),
                labels: r#"{"env":"prod","service_name":"checkout"}"#.to_string(),
            },
        )]);
        let window = ClientWindow::Instant {
            start_ns: 0,
            end_ns: 60_000_000_000,
        };
        let mut state = ClientAggState::new(
            &compiled,
            &meta,
            &client,
            instant_of(window),
            None,
            AggCaps::DEFAULT,
        )
        .unwrap();
        state.quantile_values = MAX_QUANTILE_VALUES - 1;
        let rows = [
            MetricScanRow {
                fingerprint: 1,
                timestamp_ns: 1,
                body: "v=1".to_string(),
            },
            MetricScanRow {
                fingerprint: 1,
                timestamp_ns: 2,
                body: "v=2".to_string(),
            },
        ];
        let err = state.push_rows(&rows).unwrap_err();
        match err {
            ReadError::QueryTooBroad(TooBroadReason::QuantileValues { count, cap }) => {
                assert_eq!(cap, MAX_QUANTILE_VALUES);
                assert_eq!(count, MAX_QUANTILE_VALUES + 1);
            }
            other => panic!("expected QueryTooBroad(QuantileValues), got {other:?}"),
        }
    }

    /// The `rate_counter` retention state (`BucketAcc::Counter`, code
    /// review round 2): builds a client-aggregated `rate_counter` instant
    /// state and its shared inputs.
    fn rate_counter_state_inputs() -> (
        super::super::pipeline::CompiledPipeline,
        plan::ClientAgg,
        HashMap<u64, StreamMetaRow>,
        ClientWindow,
    ) {
        let parsed = pulsus_logql::parse(r#"rate_counter({a="b"} | logfmt | unwrap c [1m])"#)
            .expect("parse");
        // Issue #272: `impl Drop for MetricExpr` forbids moving out of a
        // field (E0509), so this re-binds through a reference.
        let stages = match &parsed {
            Expr::Metric(pulsus_logql::MetricExpr::Range { range, .. }) => {
                range.selector.pipeline.clone()
            }
            other => panic!("unexpected expr: {other:?}"),
        };
        let compiled = super::super::pipeline::CompiledPipeline::compile(&stages).expect("compile");
        let client = plan::ClientAgg {
            pipeline: stages,
            value: plan::ClientValue::Unwrap,
            range_op: RangeAggOp::RateCounter,
            param: None,
            absent_labels: Vec::new(),
            grouping: None,
        };
        let meta = HashMap::from([(
            1u64,
            StreamMetaRow {
                fingerprint: 1,
                service: "checkout".to_string(),
                labels: r#"{"env":"prod","service_name":"checkout"}"#.to_string(),
            },
        )]);
        let window = ClientWindow::Instant {
            start_ns: 0,
            end_ns: 60_000_000_000,
        };
        (compiled, client, meta, window)
    }

    /// Code review round 2 (M8-LQ3, finding 1): the `rate_counter`
    /// per-bucket point retention is bounded by [`MAX_COUNTER_VALUES`],
    /// mirroring the quantile bound. Below the cap the reset-aware rate is
    /// unaffected by the guard (four samples 10,30,5,12 by ts → increase 32,
    /// span 30e9 ns, scaled by the ns/ms extrapolation factor and divided by
    /// the 60s window = 0.5333344); charged to the boundary the next sample
    /// trips the named `CounterValues` too-broad error — complete-or-error,
    /// never a silently truncated increase.
    #[test]
    fn rate_counter_point_retention_is_capped_without_changing_values_below_it() {
        let (compiled, client, meta, window) = rate_counter_state_inputs();
        // Below the cap: the reset-aware value is exactly 32/60, unchanged
        // by the retention guard.
        let rows = [
            MetricScanRow {
                fingerprint: 1,
                timestamp_ns: 10_000_000_000,
                body: "c=10".to_string(),
            },
            MetricScanRow {
                fingerprint: 1,
                timestamp_ns: 20_000_000_000,
                body: "c=30".to_string(),
            },
            MetricScanRow {
                fingerprint: 1,
                timestamp_ns: 30_000_000_000,
                body: "c=5".to_string(), // reset: 5 < 30
            },
            MetricScanRow {
                fingerprint: 1,
                timestamp_ns: 40_000_000_000,
                body: "c=12".to_string(),
            },
        ];
        let result = run_client_agg_rows(
            &rows,
            &compiled,
            &meta,
            &client,
            window,
            Some(60_000_000_000),
        )
        .expect("below the cap the query succeeds");
        let QueryResult::Vector(items) = result else {
            panic!("expected a vector, got {result:?}");
        };
        assert_eq!(items.len(), 1, "one series expected: {items:?}");
        assert_eq!(items[0].value, 0.5333344);

        // At the boundary: the next retained point trips the named error,
        // exactly as the quantile bound does — driven through the real
        // `push_rows` fold with the counter pre-charged (a 4M-row fixture
        // would be pure waste).
        let mut state = ClientAggState::new(
            &compiled,
            &meta,
            &client,
            instant_of(window),
            Some(60_000_000_000),
            AggCaps::DEFAULT,
        )
        .unwrap();
        state.counter_values = MAX_COUNTER_VALUES - 1;
        let err = state.push_rows(&rows).unwrap_err();
        match err {
            ReadError::QueryTooBroad(TooBroadReason::CounterValues { count, cap }) => {
                assert_eq!(cap, MAX_COUNTER_VALUES);
                assert_eq!(count, MAX_COUNTER_VALUES + 1);
            }
            other => panic!("expected QueryTooBroad(CounterValues), got {other:?}"),
        }
    }

    /// Code review round 3 (M8-LQ3, finding 1 — settled EMPIRICALLY): the
    /// concern was that `counter_values += 1` charges an INPUT ROW before
    /// the accumulator is derived, so a row copied into MULTIPLE
    /// accumulators would consume one quota unit while retaining several
    /// points — leaving retention unbounded.
    ///
    /// **Rewritten by issue #236 Part D, and the reason matters.** The
    /// original drove this state with a RANGE window at `step < range`
    /// and asserted the charge held across THREE step buckets. That
    /// configuration is now unrepresentable: `ClientAggState` takes an
    /// [`InstantWindow`] witness, and a range `rate_counter` has always
    /// gone to [`RangeSlideState`] in production — so the old test pinned
    /// a property of a state the engine never built. The identity it
    /// existed for survives in its instant form, where it is exact rather
    /// than merely bounded: every scanned row is retained EXACTLY once,
    /// so `counter_values` equals the retained point count, so a per-row
    /// charge is a true bound. The range half of the same invariant is
    /// pinned on the path that actually serves it, by
    /// `sliding_mutating_fan_out_charges_and_releases_retention_for_both_cell_kinds`
    /// and `mutating_retention_is_independent_of_the_window_width`.
    #[test]
    fn rate_counter_cap_bounds_total_retained_points() {
        let (compiled, client, meta, window) = rate_counter_state_inputs();
        let rows = [
            (10_000_000_000i64, "c=10"),
            (15_000_000_000, "c=30"),
            (25_000_000_000, "c=5"),
            (35_000_000_000, "c=12"),
            (45_000_000_000, "c=7"),
            (55_000_000_000, "c=20"),
        ]
        .into_iter()
        .map(|(timestamp_ns, body)| MetricScanRow {
            fingerprint: 1,
            timestamp_ns,
            body: body.to_string(),
        })
        .collect::<Vec<_>>();

        // Push through the real fold, then introspect the retained state
        // BEFORE finishing (finish consumes it).
        let mut state = ClientAggState::new(
            &compiled,
            &meta,
            &client,
            instant_of(window),
            Some(60_000_000_000),
            AggCaps::DEFAULT,
        )
        .unwrap();
        state.push_rows(&rows).unwrap();

        // `unwrap` sets the metric fan-out gate, so grouping is by final
        // label set — one group here (the unwrapped `c` is removed, base
        // labels are shared), and ONE accumulator in it.
        assert_eq!(state.label_groups.len(), 1, "one series expected");
        let total_retained: usize = state
            .label_groups
            .values()
            .map(|(_, acc)| match acc {
                BucketAcc::Counter(pts) => pts.len(),
                other => panic!("expected a Counter accumulator, got {other:?}"),
            })
            .sum();
        assert_eq!(
            total_retained,
            rows.len(),
            "every scanned row is retained exactly once"
        );
        assert_eq!(
            state.counter_values,
            rows.len() as u64,
            "the per-row charge equals total retained points (a true bound)"
        );

        // Values below the cap are unaffected by the retention guard.
        let QueryResult::Vector(items) = state.finish() else {
            panic!("an instant rate_counter query yields a vector");
        };
        assert_eq!(items.len(), 1, "one series: {items:?}");

        // The cap bounds the retention: pre-charged to `MAX - 1`, the
        // second scanned row trips the named error at `count = MAX + 1`.
        let mut capped = ClientAggState::new(
            &compiled,
            &meta,
            &client,
            instant_of(window),
            Some(60_000_000_000),
            AggCaps::DEFAULT,
        )
        .unwrap();
        capped.counter_values = MAX_COUNTER_VALUES - 1;
        match capped.push_rows(&rows).unwrap_err() {
            ReadError::QueryTooBroad(TooBroadReason::CounterValues { count, cap }) => {
                assert_eq!(cap, MAX_COUNTER_VALUES);
                assert_eq!(count, MAX_COUNTER_VALUES + 1);
            }
            other => panic!("expected QueryTooBroad(CounterValues), got {other:?}"),
        }
    }

    /// C18 (kills W11 on the metric path): a pipeline-set error on a CLEAN
    /// builder still fails the metric query — the emit gate opens on
    /// `HasErr` alone. C17 (kills W5 on the metric path): after the bare
    /// drop the series carries NO orphaned details. Both rows use
    /// pipeline-set errors on the SM-free `v0` shape because
    /// `MetricScanRow` carries no structured metadata (#249) — SM-borne
    /// slots cannot reach `run_metric_into`.
    #[test]
    fn c17_c18_metric_path_pipeline_set_errors() {
        let base = vec![("service_name".to_string(), "v0".to_string())];
        // C18: `count_over_time({v0} | json [5m])` -> pipeline error.
        let compiled =
            CompiledPipeline::compile(&parse_pipeline(r#"{x="y"} | json"#)).expect("compile");
        let mut labels = Vec::new();
        let MetricRun::Kept { .. } = compiled
            .run_metric_into("a=Hello b=World", &base, 0, None, &mut labels)
            .expect("no budget breach")
        else {
            panic!("an errored line is kept for the surviving-error check");
        };
        let err = check_surviving_error(&labels).expect_err("surviving __error__ fails");
        assert!(
            matches!(&err, ReadError::MetricPipelineError { error_type, .. }
                if error_type == "JSONParserErr"),
            "{err:?}"
        );
        // C17: `count_over_time({v0} | json | drop __error__ [5m])` -> ok,
        // series `{service_name="v0"}` with NO details.
        let compiled =
            CompiledPipeline::compile(&parse_pipeline(r#"{x="y"} | json | drop __error__"#))
                .expect("compile");
        let mut labels = Vec::new();
        let MetricRun::Kept { .. } = compiled
            .run_metric_into("a=Hello b=World", &base, 0, None, &mut labels)
            .expect("no budget breach")
        else {
            panic!("kept");
        };
        check_surviving_error(&labels).expect("no surviving error");
        let got: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert_eq!(got, vec![("service_name".to_string(), "v0".to_string())]);
    }

    /// D8's metric-failure discriminator, composed hermetically (the metric
    /// scan itself carries no SM — #249): a reserved-err SM row would fail
    /// `check_surviving_error` while a reserved-details row would not
    /// (reference: `count_over_time({v2}[5m])` -> 400 'boom',
    /// `count_over_time({v3}[5m])` -> 200).
    #[test]
    fn reserved_err_sm_fails_the_surviving_error_check_details_do_not() {
        let compiled = CompiledPipeline::compile(&parse_pipeline(r#"{x="y"}"#)).expect("compile");
        let base = vec![("service_name".to_string(), "s".to_string())];
        let err_ctx = StructuredMetadataCtx {
            err: "boom".to_string(),
            details: String::new(),
            has_ordinary: false,
        };
        let mut labels = Vec::new();
        compiled
            .run_into_with_sm("line", &base, 0, &err_ctx, &mut labels)
            .expect("kept");
        assert!(check_surviving_error(&labels).is_err());
        let details_ctx = StructuredMetadataCtx {
            err: String::new(),
            details: "bdet".to_string(),
            has_ordinary: false,
        };
        let mut labels = Vec::new();
        compiled
            .run_into_with_sm("line", &base, 0, &details_ctx, &mut labels)
            .expect("kept");
        check_surviving_error(&labels).expect("a lone details slot never fails a query");
    }

    /// The mutating (fan-out) arm hands the fold its groups in label-set
    /// order, so the fold's member order is a property of the DATA and
    /// not of a per-process hash seed.
    ///
    /// Observable without a fold too: the emitted series come out
    /// label-ascending where they used to come out in `HashMap` walk
    /// order. With 6 distinct groups a walk order that happens to be
    /// sorted has probability 1/720, so removing the sort reddens this on
    /// essentially every run — stated rather than implied.
    #[test]
    fn the_mutating_finish_emits_its_groups_in_label_order() {
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{app="a"} | logfmt"#),
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
            grouping: None,
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        assert!(
            compiled.metric_mutates_labels(),
            "this fixture must take the fan-out arm"
        );
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        // Bodies chosen so the rendered group labels do NOT sort the way
        // the insertion order does.
        let rows = slide_rows(
            1,
            &[
                (10, "id=zeta"),
                (11, "id=mike"),
                (12, "id=alpha"),
                (13, "id=romeo"),
                (14, "id=bravo"),
                (15, "id=xray"),
            ],
        );
        let window = slide_window(0, 20, 10, 20);
        let res = run_client_agg_rows(&rows, &compiled, &meta, &client, window, None).unwrap();
        let QueryResult::Matrix(out) = res else {
            panic!("expected a matrix");
        };
        assert_eq!(out.len(), 6, "one group per distinct id");
        let labels: Vec<LabelSet> = out.iter().map(|s| s.labels.clone()).collect();
        let mut sorted = labels.clone();
        sorted.sort();
        assert_eq!(
            labels, sorted,
            "the fan-out arm must emit label-ascending — that ordering is what \
             pins the fold's member order"
        );
    }

    /// (S)-leaf pin: the walk's `Copy ⇒ no owned heap` rule holds for
    /// every scalar leaf — a future field type that stops being `Copy` is
    /// a compile error here, which is precisely the P2 leaf test.
    #[test]
    fn every_s_leaf_in_the_walk_is_copy() {
        fn assert_copy<T: Copy>() {}
        assert_copy::<ClientWindow>();
        assert_copy::<ClientValue>();
        assert_copy::<RangeAggOp>();
        assert_copy::<VectorAggOp>();
        assert_copy::<GroupingKind>();
        assert_copy::<WinSample>();
        assert_copy::<AggCaps>();
    }

    /// B9 — `present_cover` costs nothing unless used: empty for a
    /// non-absent sliding state, grid-sized for an absent one.
    #[test]
    fn present_cover_is_allocated_only_for_absent_over_time() {
        let meta = slide_meta(1, r#"{"app":"x"}"#);
        let window = slide_window(
            0,
            50 * VSEC,
            10 * VSEC as u64 as i64 as u64,
            25 * VSEC as u64 as i64 as u64,
        );
        let mk = |op: RangeAggOp| ClientAgg {
            pipeline: vec![],
            value: ClientValue::Count,
            range_op: op,
            param: None,
            absent_labels: vec![],
            grouping: None,
        };
        let compiled = CompiledPipeline::compile(&[]).unwrap();
        let count_client = mk(RangeAggOp::CountOverTime);
        let count = RangeSlideState::new(
            &compiled,
            &meta,
            &count_client,
            window,
            None,
            AggCaps::DEFAULT,
        )
        .unwrap();
        assert!(count.present_cover.is_empty(), "non-absent pays no grid");
        assert_eq!(count.present_cover.capacity(), 0, "no allocation at all");
        let absent_client = mk(RangeAggOp::AbsentOverTime);
        let absent = RangeSlideState::new(
            &compiled,
            &meta,
            &absent_client,
            window,
            None,
            AggCaps::DEFAULT,
        )
        .unwrap();
        assert_eq!(absent.present_cover.len(), (absent.kmax + 2) as usize);
    }
}
