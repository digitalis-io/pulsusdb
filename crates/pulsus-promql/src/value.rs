//! Evaluator I/O data types: what the fetch layer (issue #31's
//! `pulsus-read::metrics::exec`) hands the pure evaluator, and what the
//! evaluator hands back. No ClickHouse/network types leak in here — this
//! module is as pure as the rest of the crate.

use std::collections::HashMap;

use crate::plan::SelectorId;

/// One `(timestamp_ms, value)` point. `t_ms` is milliseconds since the
/// Unix epoch (`metric_samples.unix_milli`, docs/schemas.md §2.3) —
/// verbatim, never rounded.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Sample {
    pub t_ms: i64,
    pub v: f64,
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
#[derive(Debug, Clone, PartialEq)]
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
#[derive(Debug, Clone, PartialEq)]
pub struct RangeSeries {
    pub labels: Labels,
    pub metric_name: Option<String>,
    /// See [`InstantSample::drop_name`] — latched at the series' first
    /// evaluated step.
    pub drop_name: bool,
    /// `(t_ms, value)`, ascending by `t_ms` — one point per evaluated step.
    pub points: Vec<(i64, f64)>,
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
    use super::*;

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
            samples: vec![Sample { t_ms: 0, v: 1.0 }],
        }];
        data.insert(0, series.clone());
        assert_eq!(data.get(0), series.as_slice());
    }
}
