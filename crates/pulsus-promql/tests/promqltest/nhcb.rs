//! Issue #154: the `load_with_nhcb` conversion — a clean-room port of
//! the pinned classic-bucket-to-NHCB collation the upstream promqltest
//! driver runs on such a load block (`appendCustomHistogram` +
//! `processClassicHistogramSeries`, `promql/promqltest/test.go:917-1029`,
//! over `util/convertnhcb/convertnhcb.go`'s `TempHistogram`, all at the
//! pinned v3.13.0 SHA).
//!
//! Semantics (per the pin): the block's classic series are appended
//! AS-IS by the store, PLUS a converted set — `<base>_bucket`/`_count`/
//! `_sum` series are grouped by base labels (name suffix stripped, `le`
//! dropped) per timestamp into a [`TempHistogram`] (a `_bucket` series
//! whose `le` fails float parse or is NaN skips the whole series; a
//! `_bucket` series with NO `le` label is a loud error — upstream
//! panics; histogram samples are skipped inside a group), then each
//! per-timestamp temp histogram converts to a custom-bucket (schema −53)
//! float histogram appended under the base name sorted by timestamp.

use std::collections::BTreeMap;

use pulsus_model::{CUSTOM_BUCKETS_SCHEMA, CounterResetHint, FloatHistogram, Span};
use pulsus_promql::Sample;

/// One classic/converted series: full label set (including `__name__`)
/// plus its timestamped samples.
pub type LabeledSeries = (BTreeMap<String, String>, Vec<Sample>);

/// Collects one timestamp's classic-histogram components incrementally —
/// the pin's `convertnhcb.TempHistogram`: cumulative `(le, count)`
/// buckets kept sorted by `le` with insert-position cumulativity checks,
/// duplicate `le` ignored, an error LATCHED on first violation (the
/// promqltest caller ignores per-set errors; [`TempHistogram::convert`]
/// surfaces the latch).
#[derive(Debug, Clone, Default)]
pub struct TempHistogram {
    /// `(le, cumulative_count)`, ascending by `le`.
    buckets: Vec<(f64, f64)>,
    count: f64,
    sum: f64,
    has_count: bool,
    err: Option<String>,
}

impl TempHistogram {
    pub fn new() -> Self {
        Self::default()
    }

    /// `SetBucketCount` (convertnhcb.go:71-119): NaN boundary and
    /// negative count latch errors; appends in the happy `<` case with a
    /// cumulativity check against the previous bucket; equal `le` is a
    /// duplicate sample and is IGNORED; an out-of-order `le` inserts at
    /// its sorted position with cumulativity checks against BOTH
    /// neighbours.
    pub fn set_bucket_count(&mut self, le: f64, count: f64) {
        if self.err.is_some() {
            return;
        }
        if le.is_nan() {
            self.err = Some("bucket boundary must not be NaN".to_string());
            return;
        }
        if count < 0.0 {
            self.err = Some(format!(
                "bucket count must be non-negative: le={le}, count={count}"
            ));
            return;
        }
        match self.buckets.last() {
            None => self.buckets.push((le, count)),
            Some(&(last_le, last_count)) if last_le < le => {
                if count < last_count {
                    self.err = Some(format!("count is not cumulative: {count} < {last_count}"));
                    return;
                }
                self.buckets.push((le, count));
            }
            Some(&(last_le, _)) if last_le == le => {
                // Duplicate sample — ignored (convertnhcb.go:94-95).
            }
            _ => {
                // Out-of-order: find the sorted insert position (the
                // pin's `sort.Search(le >= boundary)`); `le` is not NaN
                // and is < the last bucket's `le`, so an index always
                // exists.
                let i = self.buckets.partition_point(|&(b_le, _)| b_le < le);
                if self.buckets[i].0 == le {
                    return; // duplicate sample — ignored
                }
                if i > 0 && count < self.buckets[i - 1].1 {
                    self.err = Some(format!(
                        "count is not cumulative: {count} < {}",
                        self.buckets[i - 1].1
                    ));
                    return;
                }
                if count > self.buckets[i].1 {
                    self.err = Some(format!(
                        "count is not cumulative: {count} > {}",
                        self.buckets[i].1
                    ));
                    return;
                }
                self.buckets.insert(i, (le, count));
            }
        }
    }

    /// `SetCount` (convertnhcb.go:121-132).
    pub fn set_count(&mut self, count: f64) {
        if self.err.is_some() {
            return;
        }
        if count < 0.0 {
            self.err = Some(format!("count must be non-negative: count={count}"));
            return;
        }
        self.count = count;
        self.has_count = true;
    }

    /// `SetSum` (convertnhcb.go:134-140).
    pub fn set_sum(&mut self, sum: f64) {
        if self.err.is_some() {
            return;
        }
        self.sum = sum;
    }

    /// `Convert` (convertnhcb.go:142-235), single float path (schema
    /// `CUSTOM_BUCKETS_SCHEMA` = −53, one span `{offset:0, len:n}`,
    /// de-cumulated absolute bucket counts, `custom_values` sans +Inf,
    /// hint `Unknown`, then `compact()`). Outcome-identical to the
    /// oracle's int(`Compact(2)`)/float(`Compact(0)`) fork: the runner
    /// compacts both sides before comparing (runner.rs
    /// `histogram_almost_equal`). Count inference and +Inf synthesis
    /// per the pin: no `_count` ⇒ count = last bucket's count; no +Inf
    /// bucket ⇒ one is synthesized carrying the overall count; a final
    /// count-vs-+Inf mismatch is an error.
    pub fn convert(mut self) -> Result<FloatHistogram, String> {
        if let Some(e) = self.err {
            return Err(e);
        }
        if !self.has_count && !self.buckets.is_empty() {
            self.count = self.buckets[self.buckets.len() - 1].1;
            self.has_count = true;
        }
        if self.buckets.last().map(|b| b.0) != Some(f64::INFINITY) {
            self.buckets.push((f64::INFINITY, self.count));
        }
        let n = self.buckets.len();
        let mut custom_values = Vec::with_capacity(n.saturating_sub(1));
        let mut positive_buckets = Vec::with_capacity(n);
        let mut prev = 0.0;
        for &(le, count) in &self.buckets {
            positive_buckets.push(count - prev);
            prev = count;
            if le != f64::INFINITY {
                custom_values.push(le);
            }
        }
        let last = self.buckets[n - 1];
        // NaN-faithful: Go `h.count != h.buckets[last].count` — a NaN on
        // either side compares unequal and errors, exactly like the pin.
        #[allow(clippy::float_cmp)]
        if self.count != last.1 {
            return Err(format!(
                "count mismatch: count={} != le={} count={}",
                self.count, last.0, last.1
            ));
        }
        let mut fh = FloatHistogram {
            counter_reset_hint: CounterResetHint::Unknown,
            schema: CUSTOM_BUCKETS_SCHEMA,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count: self.count,
            sum: self.sum,
            positive_spans: vec![Span {
                offset: 0,
                length: n as u32,
            }],
            negative_spans: Vec::new(),
            positive_buckets,
            negative_buckets: Vec::new(),
            custom_values,
        };
        fh.compact();
        Ok(fh)
    }
}

/// The pin's `GetHistogramMetricBaseName` (convertnhcb.go:256-267): which
/// classic-histogram component a metric name is, plus the base name.
enum Suffix<'a> {
    Bucket(&'a str),
    Sum(&'a str),
    Count(&'a str),
    None,
}

fn strip_suffix(name: &str) -> Suffix<'_> {
    if let Some(base) = name.strip_suffix("_bucket") {
        return Suffix::Bucket(base);
    }
    if let Some(base) = name.strip_suffix("_sum") {
        return Suffix::Sum(base);
    }
    if let Some(base) = name.strip_suffix("_count") {
        return Suffix::Count(base);
    }
    Suffix::None
}

/// One `load_with_nhcb` block's conversion: classic `(labels,
/// timestamped samples)` in → converted `(base labels: name suffix
/// stripped, `le` dropped)` series out, samples sorted by timestamp.
/// `Err` = loud load failure for the whole file, mirroring the oracle's
/// error/panic paths (`appendCustomHistogram` returns the conversion
/// error; a `_bucket` series without `le` panics).
pub fn convert_block(series: &[LabeledSeries]) -> Result<Vec<LabeledSeries>, String> {
    enum Update {
        Bucket(f64),
        Count,
        Sum,
    }
    // Base labels → (timestamp → temp histogram). BTreeMaps give the
    // deterministic iteration the oracle gets from its post-collation
    // sort (its Go-map group order is irrelevant to the appended data).
    let mut groups: BTreeMap<BTreeMap<String, String>, BTreeMap<i64, TempHistogram>> =
        BTreeMap::new();

    for (labels, samples) in series {
        let Some(name) = labels.get("__name__") else {
            continue;
        };
        let (base, update) = match strip_suffix(name) {
            Suffix::Bucket(base) => {
                let le_str = labels
                    .get("le")
                    .ok_or_else(|| format!("expected bucket label in metric {name}{labels:?}"))?;
                // Unparseable or NaN `le` skips the SERIES (test.go:982-984).
                let Ok(le) = le_str.parse::<f64>() else {
                    continue;
                };
                if le.is_nan() {
                    continue;
                }
                (base, Update::Bucket(le))
            }
            Suffix::Sum(base) => (base, Update::Sum),
            Suffix::Count(base) => (base, Update::Count),
            Suffix::None => continue,
        };
        let mut base_labels = labels.clone();
        base_labels.insert("__name__".to_string(), base.to_string());
        base_labels.remove("le");
        let group = groups.entry(base_labels).or_default();
        for s in samples {
            // Histogram samples are skipped inside a group
            // (`processClassicHistogramSeries`, test.go:953-955); float
            // samples — stale markers included, they are floats upstream
            // too — participate by value.
            if s.h.is_some() {
                continue;
            }
            let th = group.entry(s.t_ms).or_default();
            match update {
                Update::Bucket(le) => th.set_bucket_count(le, s.v),
                Update::Count => th.set_count(s.v),
                Update::Sum => th.set_sum(s.v),
            }
        }
    }

    let mut out = Vec::with_capacity(groups.len());
    for (labels, by_ts) in groups {
        // BTreeMap iteration is ascending by timestamp — the oracle's
        // explicit post-conversion sort (test.go:1021).
        let mut samples = Vec::with_capacity(by_ts.len());
        for (t_ms, th) in by_ts {
            let fh = th.convert()?;
            samples.push(Sample::hist(t_ms, fh));
        }
        if samples.is_empty() {
            // A group whose every sample was a histogram converts to
            // nothing — the oracle appends zero samples for it; the
            // store has no zero-sample series shape, so it is dropped.
            continue;
        }
        out.push((labels, samples));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_hist(les_counts: &[(f64, f64)], count: Option<f64>, sum: Option<f64>) -> TempHistogram {
        let mut th = TempHistogram::new();
        for &(le, c) in les_counts {
            th.set_bucket_count(le, c);
        }
        if let Some(c) = count {
            th.set_count(c);
        }
        if let Some(s) = sum {
            th.set_sum(s);
        }
        th
    }

    /// Duplicate `le` AFTER normalization (`"0.2"` and `"2e-1"` both
    /// parse to 0.2): the SECOND set is ignored — the first bucket
    /// count wins (convertnhcb.go:94-95 "Ignore this, as it is a
    /// duplicate sample").
    #[test]
    fn duplicate_le_after_normalization_is_ignored_first_wins() {
        let mut th = TempHistogram::new();
        th.set_bucket_count("0.2".parse().unwrap(), 5.0);
        th.set_bucket_count("2e-1".parse().unwrap(), 7.0);
        let fh = th.convert().unwrap();
        assert_eq!(fh.count, 5.0, "count inferred from the FIRST 0.2 bucket");
        assert_eq!(fh.custom_values, vec![0.2]);
    }

    /// Count inference (no `_count` series): count = the highest
    /// bucket's count; +Inf synthesized carrying it
    /// (convertnhcb.go:147-157).
    #[test]
    fn count_inferred_from_last_bucket_and_inf_synthesized() {
        let th = base_hist(&[(0.1, 5.0), (0.2, 7.0)], None, None);
        let fh = th.convert().unwrap();
        assert_eq!(fh.schema, CUSTOM_BUCKETS_SCHEMA);
        assert_eq!(fh.count, 7.0);
        assert_eq!(fh.custom_values, vec![0.1, 0.2], "+Inf never stored");
        // De-cumulated: 5, 2, and the synthesized +Inf bucket adds 0 —
        // compact() drops the zero bucket.
        assert_eq!(fh.positive_buckets, vec![5.0, 2.0]);
    }

    /// A non-cumulative bucket sequence latches an error that surfaces
    /// at convert (convertnhcb.go:87-93).
    #[test]
    fn non_cumulative_counts_error_at_convert() {
        let th = base_hist(&[(0.1, 5.0), (0.2, 3.0)], None, None);
        let err = th.convert().unwrap_err();
        assert!(err.contains("count is not cumulative"), "got {err:?}");
        // Out-of-order insert checks both neighbours too.
        let th = base_hist(&[(0.1, 2.0), (0.5, 6.0), (0.2, 7.0)], None, None);
        let err = th.convert().unwrap_err();
        assert!(err.contains("count is not cumulative"), "got {err:?}");
    }

    /// `_count` disagreeing with the +Inf bucket is the pin's count
    /// mismatch error (convertnhcb.go:229-232).
    #[test]
    fn count_mismatch_with_inf_bucket_errors() {
        let th = base_hist(&[(0.1, 5.0), (f64::INFINITY, 7.0)], Some(9.0), None);
        let err = th.convert().unwrap_err();
        assert!(err.contains("count mismatch"), "got {err:?}");
    }

    /// Block conversion: suffix stripping, `le` dropping, grouping, and
    /// timestamp sorting.
    #[test]
    fn convert_block_groups_by_base_labels_and_timestamp() {
        let labels = |name: &str, le: Option<&str>| {
            let mut m = BTreeMap::from([("__name__".to_string(), name.to_string())]);
            if let Some(le) = le {
                m.insert("le".to_string(), le.to_string());
            }
            m
        };
        let series = vec![
            (
                labels("h_bucket", Some(".2")),
                vec![Sample::float(60_000, 3.0), Sample::float(0, 1.0)],
            ),
            (
                labels("h_bucket", Some("+Inf")),
                vec![Sample::float(0, 2.0), Sample::float(60_000, 5.0)],
            ),
            (labels("h_sum", None), vec![Sample::float(0, 0.5)]),
            (labels("h_count", None), vec![Sample::float(0, 2.0)]),
            // A non-component series contributes nothing.
            (labels("other", None), vec![Sample::float(0, 9.0)]),
        ];
        let out = convert_block(&series).unwrap();
        assert_eq!(out.len(), 1);
        let (base_labels, samples) = &out[0];
        assert_eq!(
            base_labels,
            &labels("h", None),
            "suffix stripped, le dropped"
        );
        assert_eq!(
            samples.iter().map(|s| s.t_ms).collect::<Vec<_>>(),
            vec![0, 60_000],
            "sorted by timestamp"
        );
        let h0 = samples[0].h.as_deref().unwrap();
        assert_eq!(h0.count, 2.0);
        assert_eq!(h0.sum, 0.5);
        assert_eq!(
            h0.custom_values,
            vec![0.2],
            "`.2` parses like Go ParseFloat"
        );
        let h1 = samples[1].h.as_deref().unwrap();
        assert_eq!(
            h1.count, 5.0,
            "t=60s has no _count sample: inferred from +Inf"
        );
        assert_eq!(h1.sum, 0.0, "t=60s has no _sum sample: defaults to 0");
    }

    /// An unparseable `le` and a NaN `le` both skip the SERIES (whole
    /// series, not just the sample — test.go:982-984), leaving the rest
    /// of the group intact.
    #[test]
    fn unparseable_and_nan_le_skip_the_series() {
        let labels = |le: &str| {
            BTreeMap::from([
                ("__name__".to_string(), "h_bucket".to_string()),
                ("le".to_string(), le.to_string()),
            ])
        };
        let series = vec![
            (labels("0.1"), vec![Sample::float(0, 1.0)]),
            (labels("garbage"), vec![Sample::float(0, 2.0)]),
            (labels("NaN"), vec![Sample::float(0, 3.0)]),
            (labels("+Inf"), vec![Sample::float(0, 4.0)]),
        ];
        let out = convert_block(&series).unwrap();
        assert_eq!(out.len(), 1);
        let h = out[0].1[0].h.as_deref().unwrap();
        assert_eq!(
            h.custom_values,
            vec![0.1],
            "the garbage/NaN le series must not contribute buckets"
        );
        assert_eq!(h.count, 4.0);
    }

    /// A `_bucket` series with NO `le` label is a loud error (the
    /// oracle panics, test.go:979-981).
    #[test]
    fn bucket_series_without_le_label_errors_loudly() {
        let series = vec![(
            BTreeMap::from([("__name__".to_string(), "h_bucket".to_string())]),
            vec![Sample::float(0, 1.0)],
        )];
        let err = convert_block(&series).unwrap_err();
        assert!(err.contains("expected bucket label"), "got {err:?}");
    }

    /// Histogram samples inside a component series are skipped
    /// (test.go:953-955): only float samples collate.
    #[test]
    fn histogram_samples_are_skipped_in_collation() {
        let fh = FloatHistogram {
            counter_reset_hint: CounterResetHint::Unknown,
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count: 1.0,
            sum: 1.0,
            positive_spans: vec![Span {
                offset: 0,
                length: 1,
            }],
            negative_spans: vec![],
            positive_buckets: vec![1.0],
            negative_buckets: vec![],
            custom_values: vec![],
        };
        let series = vec![(
            BTreeMap::from([
                ("__name__".to_string(), "h_bucket".to_string()),
                ("le".to_string(), "+Inf".to_string()),
            ]),
            vec![Sample::hist(0, fh), Sample::float(60_000, 2.0)],
        )];
        let out = convert_block(&series).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].1.iter().map(|s| s.t_ms).collect::<Vec<_>>(),
            vec![60_000],
            "the t=0 histogram sample must not open a temp histogram"
        );
    }
}
