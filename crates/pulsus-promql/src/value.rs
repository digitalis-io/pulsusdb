//! Evaluator I/O data types: what the fetch layer (issue #31's
//! `pulsus-read::metrics::exec`) hands the pure evaluator, and what the
//! evaluator hands back. No ClickHouse/network types leak in here — this
//! module is as pure as the rest of the crate.

use std::collections::HashMap;

use pulsus_model::{FloatHistogram, STALE_NAN_BITS};

use crate::plan::SelectorId;

/// One `(timestamp_ms, value)` point. `t_ms` is milliseconds since the
/// Unix epoch (`metric_samples.unix_milli`, docs/schemas.md §2.3) —
/// verbatim, never rounded.
///
/// **Native-histogram channel (M7-A5a; M7-A5b-i migrated the type from the
/// integer [`pulsus_model::NativeHistogram`] to [`FloatHistogram`]):** `h`
/// carries a decoded, float-bucket histogram for a histogram sample (from
/// `metric_hist_samples`, merged into the sample stream by the read path,
/// `NativeHistogram::to_float`'d once at `decode_hist` — no integer
/// histogram survives past the read boundary). It is `Some` only for a
/// histogram value; a float sample leaves it `None` and reads `v`. The two
/// are mutually exclusive by construction — `metric_samples` and
/// `metric_hist_samples` never carry the same `(name, fp, unix_milli)`.
/// `Box`ed so the float `Sample` stays small (a null pointer, no heap
/// alloc for the `None` case). `Copy` is dropped for `Clone` because
/// `FloatHistogram` owns `Vec`s. `FloatHistogram` (not the integer form) is
/// also THE eval-result type: `rate`/`sum`/binop-derived histogram outputs
/// carry fractional bucket counts the integer form cannot represent
/// (M7-A5b plan v3 finding 1).
#[derive(Debug, Clone)]
pub struct Sample {
    pub t_ms: i64,
    pub v: f64,
    pub h: Option<Box<FloatHistogram>>,
}

impl Sample {
    /// A float sample (`h: None`).
    pub fn float(t_ms: i64, v: f64) -> Self {
        Self { t_ms, v, h: None }
    }

    /// A native-histogram sample. `v` is unused for a histogram value and
    /// set to `0.0` so the hand-written `PartialEq` float arm (`v == v`)
    /// holds and `h` disambiguates.
    pub fn hist(t_ms: i64, h: FloatHistogram) -> Self {
        Self {
            t_ms,
            v: 0.0,
            h: Some(Box::new(h)),
        }
    }

    /// Whether this sample is Prometheus's stale marker: the float
    /// `STALE_NAN` bit pattern, or (for a histogram) a `sum` carrying that
    /// same pattern (A4 encodes histogram staleness as an empty histogram
    /// with `sum = STALE_NAN_BITS`). An ordinary NaN is never stale — only
    /// this exact bit pattern is (`value.IsStaleNaN`).
    pub fn is_stale(&self) -> bool {
        match &self.h {
            Some(h) => h.sum.to_bits() == STALE_NAN_BITS,
            None => self.v.to_bits() == STALE_NAN_BITS,
        }
    }
}

/// Bit-exact equality preserving the pre-M7 derived float semantics: the
/// float value compares with native `f64::eq` (`self.v == o.v`, so
/// `NaN != NaN`, exactly the old `#[derive(PartialEq)]`), and the
/// histogram channel compares by [`FloatHistogram::bits_eq`]. A float
/// sample (`h: None`) and a histogram sample (`h: Some`) are never equal.
impl PartialEq for Sample {
    fn eq(&self, o: &Self) -> bool {
        self.t_ms == o.t_ms && self.v == o.v && hist_opt_eq(&self.h, &o.h)
    }
}

/// One range-vector (matrix) point: a float or histogram value at `t_ms`.
/// The histogram channel mirrors [`Sample::h`]. `PartialEq` is hand-written
/// (same contract as [`Sample`]) because [`FloatHistogram`] lacks a derive
/// (NaN-bearing `sum`/`zero_threshold`/`custom_values`).
#[derive(Debug, Clone)]
pub struct Point {
    pub t_ms: i64,
    pub v: f64,
    pub h: Option<Box<FloatHistogram>>,
}

impl Point {
    /// A float point (`h: None`).
    pub fn float(t_ms: i64, v: f64) -> Self {
        Self { t_ms, v, h: None }
    }

    /// A native-histogram point (`v` set to `0.0`, see [`Sample::hist`]).
    pub fn hist(t_ms: i64, h: FloatHistogram) -> Self {
        Self {
            t_ms,
            v: 0.0,
            h: Some(Box::new(h)),
        }
    }
}

impl PartialEq for Point {
    fn eq(&self, o: &Self) -> bool {
        self.t_ms == o.t_ms && self.v == o.v && hist_opt_eq(&self.h, &o.h)
    }
}

/// A float point compares equal to a `(t_ms, value)` tuple with the exact
/// pre-M7 semantics (`v` via native `f64::eq`), so the `RangeSeries.points:
/// Vec<(i64, f64)>` → `Vec<Point>` migration leaves every existing
/// float-matrix `assert_eq!(series.points, vec![(t, v), …])` verdict
/// unchanged (AC5, diff-gated). A histogram point (`h: Some`) is never
/// equal to a bare float tuple — the tuple carries no histogram — so this
/// can never mask a histogram-valued point as a float.
impl PartialEq<(i64, f64)> for Point {
    fn eq(&self, o: &(i64, f64)) -> bool {
        self.h.is_none() && self.t_ms == o.0 && self.v == o.1
    }
}

/// Shared histogram-channel equality for the hand-written `PartialEq`s
/// ([`Sample`], [`Point`], [`InstantSample`], [`RangeSeries`]): both absent
/// is equal, both present compares by bits, presence-mismatch is unequal.
fn hist_opt_eq(a: &Option<Box<FloatHistogram>>, b: &Option<Box<FloatHistogram>>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => x.bits_eq(y),
        _ => false,
    }
}

/// A thin, sorted `(key, value)` label vector — deliberately not
/// `pulsus_model::LabelSet` (which canonicalizes keys and is tuned for the
/// ingest path): the evaluator only ever needs sort + equality + grouping,
/// and `__name__` is dropped per Prometheus's output-label rule (a query
/// result's series labels never carry the metric name as a plain label —
/// docs/architecture.md §2.3's canonical label model, mirrored here for
/// query output).
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash, PartialOrd, Ord)]
pub struct Labels(pub Vec<(String, String)>);

impl Labels {
    /// Builds a [`Labels`] from `pairs`, sorting by key and dropping any
    /// `__name__` entry (Prometheus's output-label rule).
    pub fn new(pairs: impl IntoIterator<Item = (String, String)>) -> Self {
        let mut v: Vec<(String, String)> =
            pairs.into_iter().filter(|(k, _)| k != "__name__").collect();
        v.sort();
        Labels(v)
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// The label set retaining only `keys` — the `by (...)` matching set.
    pub fn only(&self, keys: &[String]) -> Labels {
        Labels(
            self.0
                .iter()
                .filter(|(k, _)| keys.contains(k))
                .cloned()
                .collect(),
        )
    }

    /// The label set dropping `keys` — the `without (...)`/`ignoring (...)`
    /// matching set.
    pub fn without(&self, keys: &[String]) -> Labels {
        Labels(
            self.0
                .iter()
                .filter(|(k, _)| !keys.contains(k))
                .cloned()
                .collect(),
        )
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Sets `key` to `val`, overwriting an existing entry (issue #69,
    /// M6-06: `count_values`' value-label injection — the vendored
    /// `aggregators.test:467-479` "Overwrite label with output" cases).
    /// Preserves the sorted-by-key invariant. `__name__` must never be
    /// passed here — it is carried outside `Labels` by construction (see
    /// the type doc); callers route it to `InstantSample::metric_name`
    /// instead (the `eval::labels::set_or_delete` precedent).
    pub fn set(&mut self, key: String, val: String) {
        debug_assert!(
            key != "__name__",
            "__name__ is carried outside Labels — write metric_name instead"
        );
        match self.0.iter().position(|(k, _)| *k == key) {
            Some(i) => self.0[i].1 = val,
            None => {
                // Keys are unique, so key-sorted insertion preserves the
                // full `(key, value)` sort invariant.
                let pos = self.0.partition_point(|(k, _)| *k < key);
                self.0.insert(pos, (key, val));
            }
        }
    }
}

/// One resolved series' fetched samples, pre-sorted ascending by `t_ms`
/// (the fetch `ORDER BY fingerprint, unix_milli` — docs/schemas.md §2.3).
///
/// **`metric_name` (issue #85, M6-08c):** each fetched series' own metric
/// name, carried by the fetch layer alongside the `__name__`-free
/// [`Labels`] — the same split-name channel [`InstantSample::metric_name`]
/// documents, now starting at the fetch boundary. A concrete-name
/// selector's fetch sets every series to that one name; a matcher-only or
/// regex-`__name__` selector (`SelectorSpec::metric_name: None`) resolves
/// per-series names from its name-keyed source (the live label cache /
/// the test store's stored `__name__`), so the evaluator's bare-selector
/// arm emits **per-series** names instead of synthesizing one from the
/// spec. `None` only when the fetch source itself has no name for the
/// series (never the case for `metric_samples`-backed fetches).
#[derive(Debug, Clone, PartialEq)]
pub struct FetchedSeries {
    pub fingerprint: u64,
    pub metric_name: Option<String>,
    pub labels: Labels,
    pub samples: Vec<Sample>,
}

/// Every selector's fetched series, keyed by [`SelectorId`] — populated by
/// the fetch layer, consumed by [`crate::eval::evaluate`]. A selector
/// matching zero fingerprints is present with an empty `Vec`, never a
/// missing key (edge case 8: absent series is an empty result, not an
/// error).
#[derive(Debug, Clone, Default)]
pub struct SeriesData {
    by_selector: HashMap<SelectorId, Vec<FetchedSeries>>,
}

impl SeriesData {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, id: SelectorId, series: Vec<FetchedSeries>) {
        self.by_selector.insert(id, series);
    }

    /// The selector's fetched series, or an empty slice if the selector
    /// was never populated (treated identically to "matched zero
    /// fingerprints" — never an error).
    pub fn get(&self, id: SelectorId) -> &[FetchedSeries] {
        self.by_selector.get(&id).map(Vec::as_slice).unwrap_or(&[])
    }
}

/// One instant-vector series: labels plus a single `(t_ms, value)`.
///
/// **`metric_name` (issue #37; retargeted by issue #86, M6-08d):**
/// `__name__` is deliberately carried *outside* [`Labels`] (which never
/// contains it, see that type's own doc) rather than special-cased back
/// into the label vector/grouping-key machinery every `Labels`-keyed
/// `HashMap` in `eval/{aggregation,binop}.rs` already relies on.
/// `metric_name` is the series' **retained** name — `Some` whenever the
/// series has (or still had) one — and the keep/drop **verdict** lives in
/// [`InstantSample::drop_name`], mirroring upstream's delayed-name-removal
/// model (`promql/value.go` `Sample.DropName` at the pinned v3.13.0 SHA;
/// the corpus oracle runs with `EnableDelayedNameRemoval: true`,
/// `promql/promqltest/test.go:111`). Mid-tree, a name-dropping construct
/// (`rate`, arithmetic, `bool` comparisons, most aggregations, …) RETAINS
/// the input's name and sets `drop_name: true`, so downstream name readers
/// (`label_replace`/`label_join` on `__name__`, `by(__name__)` grouping)
/// still see it — the vendored `name_label_dropping.test` cases. The one
/// deliberate exception is vector-vector **arithmetic**, whose upstream
/// `resultMetric` deletes the name immediately (`changesMetricSchema`,
/// engine.go) — there `metric_name` really is `None` mid-tree. The
/// terminal `eval::finalize_metadata_labels` cleanup then nulls
/// `metric_name` (and strips `__type__`/`__unit__`) for every
/// `drop_name == true` element, so `evaluate()`'s **final** output still
/// carries `None` for dropped series — the contract
/// `pulsus-read::metrics::exec::with_metric_name` and the corpus judge
/// consume (they read `metric_name` only, never `drop_name`).
#[derive(Debug, Clone)]
pub struct InstantSample {
    pub labels: Labels,
    pub metric_name: Option<String>,
    /// The delayed name-removal verdict: `true` means the terminal cleanup
    /// drops `__name__` + `__type__` + `__unit__` for this element (see
    /// [`InstantSample::metric_name`]). Invariants:
    /// - name-dropping op: `out.metric_name = in.metric_name`,
    ///   `out.drop_name = true`;
    /// - name-keeping op: `out.metric_name = in.metric_name`,
    ///   `out.drop_name = in.drop_name`;
    /// - genuinely nameless output (scalar-derived, `absent`/`vector()`,
    ///   vector-vector arithmetic via `resultMetric`):
    ///   `metric_name = None`, `drop_name = false`;
    /// - explicit `__name__` write (`label_replace`/`label_join` dst):
    ///   sets `metric_name` and CLEARS `drop_name` (upstream
    ///   `funcLabelReplace`/`evalLabelJoin`: `DropName = false` when
    ///   `dst == __name__` — an empty value is an explicit delete, not a
    ///   drop).
    pub drop_name: bool,
    pub t_ms: i64,
    pub v: f64,
    /// The native-histogram channel (M7-A5a) — mirrors [`Sample::h`].
    /// `Some` only for a histogram-valued instant sample carried through
    /// the evaluator's **selection** path; float samples leave it `None`.
    pub h: Option<Box<FloatHistogram>>,
}

/// Bit-exact equality with the pre-M7 derived float semantics preserved
/// (`v` via native `f64::eq`); the histogram channel compares by
/// [`FloatHistogram::bits_eq`]. See [`Sample`]'s `PartialEq`.
impl PartialEq for InstantSample {
    fn eq(&self, o: &Self) -> bool {
        self.labels == o.labels
            && self.metric_name == o.metric_name
            && self.drop_name == o.drop_name
            && self.t_ms == o.t_ms
            && self.v == o.v
            && hist_opt_eq(&self.h, &o.h)
    }
}

/// One range-vector (matrix) series: labels plus its ascending points. See
/// [`InstantSample::metric_name`]'s doc for the retained-name/`drop_name`
/// contract. `drop_name` here is the **first-step latch** (issue #86 plan
/// v2 Δ1): upstream keys range accumulation on the full metric identity
/// (retained `__name__` included) and sets `DropName` once, when the
/// series is first created at a step (`engine.go` `rangeEval`
/// `seriess[h]` else-branch); later steps of the same identity never
/// touch it — so `(m > 0) or (m + 1)`, whose per-step verdict alternates,
/// is decided by whichever branch produced the identity's first step.
#[derive(Debug, Clone)]
pub struct RangeSeries {
    pub labels: Labels,
    pub metric_name: Option<String>,
    /// See [`InstantSample::drop_name`] — latched at the series' first
    /// evaluated step.
    pub drop_name: bool,
    /// Ascending by `t_ms` — one [`Point`] per evaluated step. Each point
    /// is float or (M7-A5a, selection path) native-histogram valued.
    pub points: Vec<Point>,
}

/// Field-wise equality; `points` compares element-wise through [`Point`]'s
/// hand-written `PartialEq` (bit-exact float arm + `bits_eq` histogram
/// arm), preserving the pre-M7 derived verdict for float-only series.
impl PartialEq for RangeSeries {
    fn eq(&self, o: &Self) -> bool {
        self.labels == o.labels
            && self.metric_name == o.metric_name
            && self.drop_name == o.drop_name
            && self.points == o.points
    }
}

/// The evaluator's result. An instant query ([`crate::plan::PlanParams`]
/// with `start_ms == end_ms`) yields [`QueryValue::Vector`],
/// [`QueryValue::Scalar`], or — for a top-level string-literal query
/// (issue #86, M6-08d) — [`QueryValue::String`]; a range query yields
/// [`QueryValue::Matrix`]. `String` carries the literal's value only: the
/// wire timestamp is stamped externally by the response encoder from the
/// request's evaluation time (the `Scalar`/`at_ms` precedent).
#[derive(Debug, Clone, PartialEq)]
pub enum QueryValue {
    Vector(Vec<InstantSample>),
    Matrix(Vec<RangeSeries>),
    Scalar(f64),
    String(String),
}

#[cfg(test)]
mod tests {
    use pulsus_model::{NativeHistogram, STALE_NAN_BITS, Span};

    use super::*;

    /// `single_histogram` (`native_histograms.test:34`), the A3 corpus
    /// fixture: `schema:0 sum:5 count:4 buckets:[1 2 1]` (deltas `[1 1 -1]`).
    /// Returned already `to_float`'d (M7-A5b-i): the value-model histogram
    /// channel is `FloatHistogram`, never the integer form.
    fn single_histogram() -> FloatHistogram {
        NativeHistogram {
            counter_reset_hint: pulsus_model::CounterResetHint::Unknown,
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 4,
            sum: 5.0,
            positive_spans: vec![Span {
                offset: 0,
                length: 3,
            }],
            negative_spans: vec![],
            positive_buckets: vec![1, 1, -1],
            negative_buckets: vec![],
            custom_values: vec![],
        }
        .to_float()
    }

    // -- M7-A5a/A5b-i: value-model equality (AC9) — float arm byte-identical,
    //    histogram arm via FloatHistogram::bits_eq --

    #[test]
    fn two_equal_float_samples_are_eq_unchanged() {
        assert_eq!(Sample::float(1000, 3.5), Sample::float(1000, 3.5));
        assert_ne!(Sample::float(1000, 3.5), Sample::float(1000, 4.0));
        // NaN != NaN preserved (native f64::eq), exactly the old derive.
        assert_ne!(Sample::float(1000, f64::NAN), Sample::float(1000, f64::NAN));
    }

    #[test]
    fn a_float_sample_and_a_histogram_sample_are_never_equal() {
        assert_ne!(
            Sample::float(1000, 0.0),
            Sample::hist(1000, single_histogram())
        );
    }

    #[test]
    fn two_equal_histogram_samples_are_eq_by_bits() {
        assert_eq!(
            Sample::hist(1000, single_histogram()),
            Sample::hist(1000, single_histogram())
        );
        let mut other = single_histogram();
        other.count = 5.0;
        assert_ne!(
            Sample::hist(1000, single_histogram()),
            Sample::hist(1000, other)
        );
    }

    #[test]
    fn stale_nan_sum_histogram_samples_are_eq_by_bits() {
        let mut a = single_histogram();
        a.sum = f64::from_bits(STALE_NAN_BITS);
        let mut b = single_histogram();
        b.sum = f64::from_bits(STALE_NAN_BITS);
        assert_eq!(Sample::hist(1000, a), Sample::hist(1000, b));
    }

    #[test]
    fn is_stale_covers_float_and_histogram_channels() {
        assert!(Sample::float(0, f64::from_bits(STALE_NAN_BITS)).is_stale());
        assert!(!Sample::float(0, 1.0).is_stale());
        // An ordinary NaN is never stale (only the exact bit pattern is).
        assert!(!Sample::float(0, f64::from_bits(0x7FF8_0000_0000_0001)).is_stale());
        let mut stale = single_histogram();
        stale.sum = f64::from_bits(STALE_NAN_BITS);
        assert!(Sample::hist(0, stale).is_stale());
        assert!(!Sample::hist(0, single_histogram()).is_stale());
    }

    #[test]
    fn point_float_equals_the_tuple_form_for_the_migration() {
        assert_eq!(Point::float(1000, 3.5), (1000i64, 3.5f64));
        assert_ne!(Point::float(1000, 3.5), (1000i64, 4.0f64));
        // A histogram point is never equal to a bare float tuple.
        assert_ne!(Point::hist(1000, single_histogram()), (1000i64, 0.0f64));
        // Vec<Point> == Vec<(i64, f64)> (the assertion-preserving path).
        assert_eq!(
            vec![Point::float(0, 1.0), Point::float(10, 2.0)],
            vec![(0i64, 1.0f64), (10i64, 2.0f64)]
        );
    }

    #[test]
    fn instant_and_range_equality_thread_the_histogram_channel() {
        let a = InstantSample {
            labels: Labels::default(),
            metric_name: None,
            drop_name: false,
            t_ms: 0,
            v: 0.0,
            h: Some(Box::new(single_histogram())),
        };
        let b = a.clone();
        assert_eq!(a, b);
        let mut c = a.clone();
        c.h = None;
        assert_ne!(a, c); // presence mismatch
        let r1 = RangeSeries {
            labels: Labels::default(),
            metric_name: None,
            drop_name: false,
            points: vec![Point::hist(0, single_histogram())],
        };
        let r2 = r1.clone();
        assert_eq!(r1, r2);
    }

    #[test]
    fn labels_new_sorts_by_key_and_drops_dunder_name() {
        let labels = Labels::new(vec![
            ("job".to_string(), "api".to_string()),
            ("__name__".to_string(), "up".to_string()),
            ("env".to_string(), "prod".to_string()),
        ]);
        assert_eq!(
            labels.0,
            vec![
                ("env".to_string(), "prod".to_string()),
                ("job".to_string(), "api".to_string()),
            ]
        );
    }

    #[test]
    fn labels_get_finds_an_existing_key() {
        let labels = Labels::new(vec![("job".to_string(), "api".to_string())]);
        assert_eq!(labels.get("job"), Some("api"));
        assert_eq!(labels.get("missing"), None);
    }

    #[test]
    fn labels_only_retains_the_named_keys() {
        let labels = Labels::new(vec![
            ("job".to_string(), "api".to_string()),
            ("env".to_string(), "prod".to_string()),
        ]);
        let only = labels.only(&["job".to_string()]);
        assert_eq!(only.0, vec![("job".to_string(), "api".to_string())]);
    }

    #[test]
    fn labels_without_drops_the_named_keys() {
        let labels = Labels::new(vec![
            ("job".to_string(), "api".to_string()),
            ("env".to_string(), "prod".to_string()),
        ]);
        let rest = labels.without(&["job".to_string()]);
        assert_eq!(rest.0, vec![("env".to_string(), "prod".to_string())]);
    }

    #[test]
    fn series_data_get_of_an_unpopulated_selector_is_an_empty_slice_not_a_panic() {
        let data = SeriesData::new();
        assert!(data.get(0).is_empty());
    }

    #[test]
    fn series_data_insert_then_get_round_trips() {
        let mut data = SeriesData::new();
        let series = vec![FetchedSeries {
            fingerprint: 1,
            metric_name: Some("up".to_string()),
            labels: Labels::new(vec![("job".to_string(), "api".to_string())]),
            samples: vec![Sample::float(0, 1.0)],
        }];
        data.insert(0, series.clone());
        assert_eq!(data.get(0), series.as_slice());
    }
}
