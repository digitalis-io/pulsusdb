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

use pulsus_model::{MatchOp, STALE_NAN_BITS};
use pulsus_promql::{FetchedSeries, Labels, QueryPlan, Sample, SeriesData};

use super::grammar::LoadSeries;
use super::series::SeqValue;

/// One loaded series: full label set (including `__name__`) plus its
/// samples, ascending by timestamp.
#[derive(Debug, Clone)]
struct StoredSeries {
    labels: BTreeMap<String, String>,
    samples: Vec<Sample>,
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
    /// append-to-storage behaviour across multiple `load` blocks.
    pub fn load(&mut self, step_ms: i64, series: &[LoadSeries]) -> Result<(), String> {
        for s in series {
            let mut samples = Vec::new();
            for (k, v) in s.values.iter().enumerate() {
                let t_ms = k as i64 * step_ms;
                match v {
                    SeqValue::Gap => {}
                    SeqValue::Stale => samples.push(Sample {
                        t_ms,
                        v: f64::from_bits(STALE_NAN_BITS),
                    }),
                    SeqValue::Value(v) => samples.push(Sample { t_ms, v: *v }),
                }
            }
            match self.series.iter_mut().find(|st| st.labels == s.labels) {
                Some(existing) => {
                    existing.samples.extend(samples);
                    existing.samples.sort_by_key(|s| s.t_ms);
                }
                None => self.series.push(StoredSeries {
                    labels: s.labels.clone(),
                    samples,
                }),
            }
        }
        Ok(())
    }

    /// Resolves and windows every selector of `plan` — the driver's stand-in
    /// for the resolve+fetch layer.
    pub fn fetch(&self, plan: &QueryPlan) -> Result<SeriesData, String> {
        let mut data = SeriesData::new();
        for spec in &plan.selectors {
            let (lower_excl, upper_incl) = spec.fetch_window(&plan.params);
            let mut fetched = Vec::new();
            for (idx, stored) in self.series.iter().enumerate() {
                let name = stored.labels.get("__name__").map(String::as_str);
                if name != Some(spec.metric_name.as_str()) {
                    continue;
                }
                let mut matched = true;
                for m in &spec.matchers {
                    let value = stored.labels.get(&m.key).map(String::as_str).unwrap_or("");
                    if !matcher_matches(&m.op, &m.value, value)? {
                        matched = false;
                        break;
                    }
                }
                if !matched {
                    continue;
                }
                let samples: Vec<Sample> = stored
                    .samples
                    .iter()
                    .copied()
                    .filter(|s| s.t_ms > lower_excl && s.t_ms <= upper_incl)
                    .collect();
                fetched.push(FetchedSeries {
                    fingerprint: idx as u64,
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
