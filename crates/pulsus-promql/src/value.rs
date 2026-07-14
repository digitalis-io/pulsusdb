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
}

/// One resolved series' fetched samples, pre-sorted ascending by `t_ms`
/// (the fetch `ORDER BY fingerprint, unix_milli` — docs/schemas.md §2.3).
#[derive(Debug, Clone, PartialEq)]
pub struct FetchedSeries {
    pub fingerprint: u64,
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
#[derive(Debug, Clone, PartialEq)]
pub struct InstantSample {
    pub labels: Labels,
    pub t_ms: i64,
    pub v: f64,
}

/// One range-vector (matrix) series: labels plus its ascending points.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeSeries {
    pub labels: Labels,
    /// `(t_ms, value)`, ascending by `t_ms` — one point per evaluated step.
    pub points: Vec<(i64, f64)>,
}

/// The evaluator's result. An instant query ([`crate::plan::PlanParams`]
/// with `start_ms == end_ms`) always yields [`QueryValue::Vector`] or
/// [`QueryValue::Scalar`]; a range query yields [`QueryValue::Matrix`].
#[derive(Debug, Clone, PartialEq)]
pub enum QueryValue {
    Vector(Vec<InstantSample>),
    Matrix(Vec<RangeSeries>),
    Scalar(f64),
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
            labels: Labels::new(vec![("job".to_string(), "api".to_string())]),
            samples: vec![Sample { t_ms: 0, v: 1.0 }],
        }];
        data.insert(0, series.clone());
        assert_eq!(data.get(0), series.as_slice());
    }
}
