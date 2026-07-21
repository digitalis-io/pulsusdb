//! Synthetic in-memory storage for the corpus driver: `load` blocks
//! accumulate series (base epoch `T0 = 0 ms`, upstream
//! `testStartTime = time.Unix(0,0)`), `clear` wipes them, and
//! [`TestStorage::fetch`] replicates `pulsus-read::metrics::exec`'s
//! per-selector match-and-window step against a [`QueryPlan`] — matcher
//! semantics (`Eq`/`Neq`/`Re`/`Nre`, regex fully anchored `^(?:pat)$`,
//! missing label matched as `""`, exactly like Prometheus's
//! `labels.Matcher`) plus the left-open right-closed
//! [`SelectorSpec::fetch_window`] bounds. The evaluator itself is the real
//! `pulsus_promql::evaluate` — this store only stands in for the
//! ClickHouse fetch layer, keeping the whole replay hermetic.

use std::collections::BTreeMap;

use pulsus_model::{CounterResetHint, FloatHistogram, MatchOp, STALE_NAN_BITS};
use pulsus_promql::{FetchedSeries, Labels, QueryPlan, Sample, SeriesData};

use super::grammar::LoadSeries;
use super::series::SeqValue;

/// One loaded series: full label set (including `__name__`) plus its
/// samples, ascending by timestamp. `readback` (issue #125) is the
/// per-sample STORAGE read-back counter-reset hint, recomputed over the
/// whole merged series after every `load` (never per-block, never
/// per-fetch-window) — `samples` keeps the ORIGINAL loaded hints so later
/// `load` merges re-derive chunk cuts from what the appender actually
/// saw, and `fetch` substitutes `readback[i]` on the clone it hands out.
#[derive(Debug, Clone)]
struct StoredSeries {
    labels: BTreeMap<String, String>,
    samples: Vec<Sample>,
    readback: Vec<CounterResetHint>,
}

#[derive(Debug, Default)]
pub struct TestStorage {
    series: Vec<StoredSeries>,
}

impl TestStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear(&mut self) {
        self.series.clear();
    }

    /// Applies one `load <step>` block: sample `k` of a series lands at
    /// `t = k * step_ms`. A series whose labels already exist gets its new
    /// samples appended (then re-sorted), matching upstream's
    /// append-to-storage behaviour across multiple `load` blocks. After
    /// every merge+sort the storage READ-BACK hints are recomputed over
    /// the whole series ([`readback_hints`], issue #125).
    pub fn load(&mut self, step_ms: i64, series: &[LoadSeries]) -> Result<(), String> {
        for s in series {
            let mut samples = Vec::new();
            for (k, v) in s.values.iter().enumerate() {
                let t_ms = k as i64 * step_ms;
                match v {
                    SeqValue::Gap => {}
                    SeqValue::Stale => {
                        samples.push(Sample::float(t_ms, f64::from_bits(STALE_NAN_BITS)))
                    }
                    SeqValue::Value(v) => samples.push(Sample::float(t_ms, *v)),
                    // `load` ignores hint_set; the hint VALUE rides inside
                    // the histogram (an explicit gauge/reset drives the
                    // chunk-cut emulation below).
                    SeqValue::Histogram(h, _) => samples.push(Sample::hist(t_ms, h.clone())),
                }
            }
            match self.series.iter_mut().find(|st| st.labels == s.labels) {
                Some(existing) => {
                    existing.samples.extend(samples);
                    existing.samples.sort_by_key(|s| s.t_ms);
                    existing.readback = readback_hints(&existing.samples);
                }
                None => {
                    let readback = readback_hints(&samples);
                    self.series.push(StoredSeries {
                        labels: s.labels.clone(),
                        samples,
                        readback,
                    });
                }
            }
        }
        Ok(())
    }

    /// Resolves and windows every selector of `plan` — the driver's stand-in
    /// for the resolve+fetch layer. Issue #85 (M6-08c): a selector with
    /// `metric_name: None` scans every stored series (the name-keyed-cache
    /// stand-in), `name_matchers` filter candidate names on both paths,
    /// and every fetched series carries its own stored `__name__` on the
    /// per-series `FetchedSeries::metric_name` channel.
    pub fn fetch(&self, plan: &QueryPlan) -> Result<SeriesData, String> {
        let mut data = SeriesData::new();
        for spec in &plan.selectors {
            let (lower_excl, upper_incl) = spec.fetch_window(&plan.params);
            let mut fetched = Vec::new();
            for (idx, stored) in self.series.iter().enumerate() {
                let name = stored.labels.get("__name__").map(String::as_str);
                if let Some(want) = &spec.metric_name
                    && name != Some(want.as_str())
                {
                    continue;
                }
                let mut matched = true;
                for m in &spec.name_matchers {
                    // Absent `__name__` matches as `""`, like any label.
                    if !matcher_matches(&m.op, &m.value, name.unwrap_or(""))? {
                        matched = false;
                        break;
                    }
                }
                for m in &spec.matchers {
                    if !matched {
                        break;
                    }
                    let value = stored.labels.get(&m.key).map(String::as_str).unwrap_or("");
                    if !matcher_matches(&m.op, &m.value, value)? {
                        matched = false;
                    }
                }
                if !matched {
                    continue;
                }
                let samples: Vec<Sample> = stored
                    .samples
                    .iter()
                    .zip(&stored.readback)
                    .filter(|(s, _)| s.t_ms > lower_excl && s.t_ms <= upper_incl)
                    .map(|(s, hint)| {
                        // Issue #125: what the engine sees is the STORAGE
                        // read-back hint, not the literal's — explicit
                        // NCR/CR per-sample hints are deliberately not
                        // round-tripped (chunks store only headers).
                        let mut s = s.clone();
                        if let Some(h) = s.h.as_mut() {
                            h.counter_reset_hint = *hint;
                        }
                        s
                    })
                    .collect();
                fetched.push(FetchedSeries {
                    fingerprint: idx as u64,
                    metric_name: name.map(str::to_string),
                    // `Labels::new` drops `__name__` itself.
                    labels: Labels::new(stored.labels.iter().map(|(k, v)| (k.clone(), v.clone()))),
                    samples,
                });
            }
            data.insert(spec.id, fetched);
        }
        Ok(data)
    }
}

/// Issue #125: the storage READ-BACK counter-reset hint per sample — the
/// promqltest-store emulation of what the pinned TSDB hands the engine
/// after a `load`. Two pinned layers compose here:
///
/// **Chunk cuts** (`tsdb/chunkenc/histogram.go` `AppendHistogram` via
/// `appendable`/`appendableGauge`, `:255-330,500-545,751-880`): a float
/// sample (stale markers included — the test grammar's `stale` is a float
/// append) ends any histogram chunk; a histogram sample cuts a new chunk
/// on a gauge↔counter hint transition, an explicit `CounterReset` hint
/// (always honored), a schema or zero-threshold change, an NHCB
/// custom-bounds change, or — counter chunks only — a count/zero-count/
/// bucket drop (full `detect_reset`, run with the sample's own hint
/// neutralized: `appendable` ignores a `NotCounterReset` hint and does
/// the real comparison). Gauge chunks never reset-cut (`appendableGauge`
/// checks layout only).
///
/// **Read-back** (`tsdb/chunkenc/histogram_meta.go:471-492`
/// `counterResetHint`): a gauge chunk reads back `Gauge` for EVERY
/// sample; a counter chunk reads back `Unknown` for its FIRST sample —
/// even when the chunk was cut BY a reset or an explicit `reset` hint
/// (the pinned issue-15346 behaviour: the header is not trusted across
/// chunks) — and `NotCounterReset` for every later sample.
fn readback_hints(samples: &[Sample]) -> Vec<CounterResetHint> {
    // The current chunk: gauge?, the last appended histogram (full), and
    // how many samples it holds.
    struct Chunk {
        gauge: bool,
        last: FloatHistogram,
        num: usize,
    }
    let mut chunk: Option<Chunk> = None;
    let mut out = Vec::with_capacity(samples.len());
    for s in samples {
        let Some(h) = s.h.as_deref() else {
            // Float (incl. stale marker): lands in a float chunk — the
            // histogram chunk is over. The hint slot is meaningless for a
            // float; Unknown fills the parallel vec.
            chunk = None;
            out.push(CounterResetHint::Unknown);
            continue;
        };
        let is_gauge = h.counter_reset_hint == CounterResetHint::Gauge;
        let cut = match &chunk {
            None => true,
            Some(c) => {
                if c.gauge != is_gauge {
                    // Gauge sample into a counter chunk or vice versa —
                    // both `appendable` paths bail immediately.
                    true
                } else if h.counter_reset_hint == CounterResetHint::CounterReset {
                    // "Always honor the explicit counter reset hint."
                    true
                } else if s.is_stale() {
                    // A stale HISTOGRAM sample is always appendable (its
                    // buckets/spans don't matter). (The test grammar
                    // produces float stales, so this arm is defensive.)
                    false
                } else if c.last.sum.to_bits() == STALE_NAN_BITS {
                    // "If the last sample was stale, then we can only
                    // accept stale samples in this chunk."
                    true
                } else if h.schema != c.last.schema
                    || h.zero_threshold != c.last.zero_threshold
                    || (h.uses_custom_buckets() && h.custom_values != c.last.custom_values)
                {
                    // Schema/zero-threshold/NHCB-bounds change — both
                    // appendable paths cut without full reset detection.
                    true
                } else if !is_gauge {
                    // Counter chunk: the full reset detection, with the
                    // sample's own (possibly NCR) hint neutralized so the
                    // shortcut cannot mask a real drop.
                    let mut probe = h.clone();
                    probe.counter_reset_hint = CounterResetHint::Unknown;
                    probe.detect_reset(&c.last)
                } else {
                    // Gauge chunk: layout recodes, never reset-cuts.
                    false
                }
            }
        };
        let num = if cut {
            1
        } else {
            chunk.as_ref().map(|c| c.num + 1).unwrap_or(1)
        };
        out.push(match (is_gauge, num) {
            (true, _) => CounterResetHint::Gauge,
            (false, 1) => CounterResetHint::Unknown,
            (false, _) => CounterResetHint::NotCounterReset,
        });
        chunk = Some(Chunk {
            gauge: is_gauge,
            last: h.clone(),
            num,
        });
    }
    out
}

/// Prometheus matcher semantics: regexes fully anchored (`^(?:pat)$`), a
/// missing label matches as the empty string (handled by the caller).
fn matcher_matches(op: &MatchOp, pattern: &str, value: &str) -> Result<bool, String> {
    match op {
        MatchOp::Eq => Ok(value == pattern),
        MatchOp::Neq => Ok(value != pattern),
        MatchOp::Re | MatchOp::Nre => {
            let re = regex::Regex::new(&format!("^(?:{pattern})$"))
                .map_err(|e| format!("invalid selector regex {pattern:?}: {e}"))?;
            let is_match = re.is_match(value);
            Ok(match op {
                MatchOp::Re => is_match,
                _ => !is_match,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A schema-0 histogram sample: `count` observations in one bucket,
    /// with the given loaded (pre-storage) hint.
    fn hist(t_ms: i64, count: f64, hint: CounterResetHint) -> Sample {
        Sample::hist(
            t_ms,
            FloatHistogram {
                counter_reset_hint: hint,
                schema: 0,
                zero_threshold: 0.0,
                zero_count: 0.0,
                count,
                sum: count,
                positive_spans: vec![pulsus_model::Span {
                    offset: 0,
                    length: 1,
                }],
                negative_spans: vec![],
                positive_buckets: vec![count],
                negative_buckets: vec![],
                custom_values: vec![],
            },
        )
    }

    // -- issue #125 (AC5): the storage read-back emulation
    //    (`readback_hints`), pinned against `tsdb/chunkenc/histogram.go`
    //    `appendable`/`appendableGauge` + `histogram_meta.go`
    //    `counterResetHint` @ 40af9c2 --

    #[test]
    fn gauge_hinted_series_reads_back_all_gauge() {
        use CounterResetHint::Gauge;
        let hints = readback_hints(&[
            hist(0, 4.0, Gauge),
            hist(60_000, 7.0, Gauge),
            hist(120_000, 2.0, Gauge), // a drop never cuts a gauge chunk
        ]);
        assert_eq!(hints, vec![Gauge, Gauge, Gauge]);
    }

    #[test]
    fn monotone_counter_reads_back_unknown_then_not_counter_reset() {
        use CounterResetHint::{NotCounterReset, Unknown};
        let hints = readback_hints(&[
            hist(0, 4.0, Unknown),
            hist(60_000, 7.0, Unknown),
            hist(120_000, 9.0, Unknown),
        ]);
        assert_eq!(hints, vec![Unknown, NotCounterReset, NotCounterReset]);
    }

    /// A mid-series count drop cuts the chunk; the pinned issue-15346
    /// behaviour means the post-cut sample STILL reads back Unknown (the
    /// CounterReset header is not trusted across chunks).
    #[test]
    fn mid_series_reset_reads_back_unknown_at_the_reset_sample() {
        use CounterResetHint::{NotCounterReset, Unknown};
        let hints = readback_hints(&[
            hist(0, 4.0, Unknown),
            hist(60_000, 7.0, Unknown),
            hist(120_000, 2.0, Unknown), // count drop ⇒ cut
            hist(180_000, 3.0, Unknown),
        ]);
        assert_eq!(
            hints,
            vec![Unknown, NotCounterReset, Unknown, NotCounterReset]
        );
    }

    /// An explicit `counter_reset_hint:reset` literal cuts even without
    /// any count/bucket drop ("Always honor the explicit counter reset
    /// hint") and STILL reads back Unknown — explicit CR/NCR per-sample
    /// hints are deliberately NOT round-tripped (chunks store headers
    /// only). Non-vacuous: without the explicit-CR cut, sample 1 (a
    /// monotone increase) would read NotCounterReset.
    #[test]
    fn explicit_reset_hint_cuts_and_reads_back_unknown() {
        use CounterResetHint::{CounterReset, NotCounterReset, Unknown};
        let hints = readback_hints(&[
            hist(0, 4.0, Unknown),
            hist(60_000, 7.0, CounterReset), // no drop, hint-only cut
            hist(120_000, 9.0, Unknown),
        ]);
        assert_eq!(hints, vec![Unknown, Unknown, NotCounterReset]);
        // The NCR twin: an explicit not_reset is ignored by the appender
        // (full detection still runs; no cut on growth) AND is not
        // round-tripped — a FIRST sample with it reads Unknown.
        let hints = readback_hints(&[
            hist(0, 4.0, NotCounterReset),
            hist(60_000, 2.0, NotCounterReset), // real drop: NCR must NOT mask it
        ]);
        assert_eq!(hints, vec![Unknown, Unknown]);
    }

    /// A float sample (the grammar's `stale` marker included) ends the
    /// histogram chunk — the next histogram starts a fresh chunk and
    /// reads back Unknown.
    #[test]
    fn float_or_stale_interruption_cuts_the_chunk() {
        use CounterResetHint::{NotCounterReset, Unknown};
        let hints = readback_hints(&[
            hist(0, 4.0, Unknown),
            hist(60_000, 7.0, Unknown),
            Sample::float(120_000, f64::from_bits(STALE_NAN_BITS)),
            hist(180_000, 9.0, Unknown),
        ]);
        assert_eq!(
            hints,
            vec![Unknown, NotCounterReset, Unknown, Unknown],
            "post-stale histogram is first-in-chunk ⇒ Unknown"
        );
    }

    /// A gauge↔counter hint transition cuts in BOTH directions (the
    /// `mixed` corpus series shape): counter samples, one gauge sample,
    /// counter samples again.
    #[test]
    fn gauge_counter_transitions_cut_in_both_directions() {
        use CounterResetHint::{Gauge, NotCounterReset, Unknown};
        let hints = readback_hints(&[
            hist(0, 4.0, Unknown),
            hist(60_000, 7.0, Unknown),
            hist(120_000, 5.0, Gauge),   // counter chunk ⇒ gauge chunk
            hist(180_000, 8.0, Unknown), // gauge chunk ⇒ counter chunk
            hist(240_000, 9.0, Unknown),
        ]);
        assert_eq!(
            hints,
            vec![Unknown, NotCounterReset, Gauge, Unknown, NotCounterReset]
        );
    }

    /// A schema change cuts (the pin's `appendable` returns Unknown
    /// header without full detection).
    #[test]
    fn schema_change_cuts_the_chunk() {
        use CounterResetHint::Unknown;
        let mut wider = hist(60_000, 7.0, Unknown);
        wider.h.as_mut().unwrap().schema = 1;
        let hints = readback_hints(&[hist(0, 4.0, Unknown), wider]);
        assert_eq!(hints, vec![Unknown, Unknown]);
    }
}
