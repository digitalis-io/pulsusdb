//! Synthetic in-memory storage for the corpus driver: `load` blocks
//! accumulate series (base epoch `T0 = 0 ms`, upstream
//! `testStartTime = time.Unix(0,0)`), `clear` wipes them, and
//! [`TestStorage::fetch`] replicates `pulsus-read::metrics::exec`'s
//! per-selector match-and-window step against a [`QueryPlan`] — matcher
//! semantics (`Eq`/`Neq`/`Re`/`Nre`; a `Re`/`Nre` pattern is compiled
//! through the *same expression production uses*,
//! `pulsus_re2::compile_user_regex_anchored(&pulsus_re2::re2_pattern_to_rust(p))`
//! — RE2's reading of the pattern, fully anchored `^(?:pat)$` — never a
//! bare `regex::Regex::new`, which reads `\d`/`\w`/`\s` as Unicode and
//! rejects a malformed brace run that RE2 takes as a literal (issue #278;
//! issue #317 is the rewrite itself). The production sites this mirrors are
//! `crates/pulsus-read/src/metrics/labels.rs:274` (the concrete-name path)
//! and `:620` (the cached path); a missing label is matched as `""`,
//! exactly like Prometheus's `labels.Matcher`) plus the left-open
//! right-closed [`SelectorSpec::fetch_window`] bounds. The evaluator itself
//! is the real `pulsus_promql::evaluate` — this store only stands in for
//! the ClickHouse fetch layer, keeping the whole replay hermetic.
//!
//! # What this stand-in substitutes, and what covers each stage instead
//!
//! Every stage between a corpus directive and the answer it compares. One
//! row per hidden concern, never a compound one, so a single annotation can
//! be true of the whole row. The annotation vocabulary is closed to three
//! forms and machine-checked by
//! `crates/pulsus-promql/tests/promqltest_corpus.rs::the_substitution_inventory_annotates_every_row`
//! — see that test's own doc comment for exactly what the check proves,
//! what it does not, and who does.
//!
//! // --- SUBSTITUTION INVENTORY BEGIN ---
//!
//! | stage / hidden concern | annotation |
//! |---|---|
//! | parse | Nothing substituted |
//! | plan (`plan(&expr, params)`) | Nothing substituted |
//! | `PlanParams` construction | Nothing substituted |
//! | series resolution — cold / stale / warm cache fallback routing | Covered by: crates/pulsus-read/tests/live_metrics_cache.rs::a_cold_cache_falls_back_to_sql_with_the_same_result_a_warm_cache_would_give, crates/pulsus-read/tests/live_metrics_cache.rs::stale_cache_degrades_to_sql_identical_to_ground_truth_and_a_fresh_refresh, crates/pulsus-read/tests/live_metrics_cache.rs::warm_cache_and_sql_fallback_return_identical_results |
//! | series resolution — the `labels` JSON round-trip | Covered by: crates/pulsus-read/tests/live_metrics_cache.rs::a_quote_and_backslash_bearing_label_key_round_trips_identically_on_both_paths |
//! | series resolution — metric-name fan-out cap, `QueryTooBroad(MetricFanout)` | Covered by: crates/pulsus-read/tests/live_discovery_fallback.rs::degraded_regex_name_discovery_over_the_fanout_cap_is_query_too_broad, crates/pulsus-server/tests/prom_api_live.rs::prom_api_name_regex_discovery_over_the_fanout_cap_is_422_execution |
//! | series resolution — info-family cardinality cap, `QueryTooBroad(InfoCardinality)` | Covered by: crates/pulsus-read/tests/live_metrics_engine.rs::info_cardinality_cap_rejects_over_cap_before_materialization, crates/pulsus-read/tests/live_metrics_engine.rs::info_cardinality_cap_rejects_over_cap_on_the_degraded_sql_fallback_path |
//! | series resolution — `OverCardinality` → SQL fallback | Covered by: crates/pulsus-read/src/metrics/labels.rs::tests::a_match_exceeding_cache_max_series_falls_back_to_sql_not_a_giant_in_list, crates/pulsus-read/src/metrics/labels.rs::tests::multi_metric_total_series_over_cache_max_series_is_unresolvable<br>`Limit:` hermetic only — the four live metrics suites in `pulsus-read` set `cache_max_series: 50_000` explicitly and `prom_api_live.rs` inherits the same value from the config default (`crates/pulsus-config/src/model.rs:456`), so no live suite can trip this cap. |
//! | **matcher semantics (the defect)** | Covered by: crates/pulsus-promql/tests/promqltest/corpus/proof/m7_selector_regex_re2.test, crates/pulsus-read/tests/live_metrics_engine.rs::selector_regex_matches_prometheus_on_cold_and_warm_resolution<br>`Limit:` both artifacts are created by issue #278; before it landed this row read `No suite covers this stage`. |
//! | sample fetch — window rendering (the `unix_milli <= end` right edge) | Covered by: crates/pulsus-read/tests/live_metrics_engine.rs::count_by_job_up_is_lookback_correct_and_excludes_a_silent_series |
//! | sample fetch — `Float64`/`Gorilla` value round-trip, ingest → storage | Covered by: crates/pulsus-write/tests/metric_ingest_float_roundtrip.rs::otlp_json_metrics_store_the_nearest_representable_f64_bits |
//! | sample fetch — value decode, storage → engine | Covered by: crates/pulsus-read/tests/live_metrics_engine.rs::rate_end_to_end_against_real_samples<br>`Limit:` answer-level, not bit-level. `assert_eq!` on `f64` compares by VALUE, and a passing value comparison is evidence about bits in neither direction: it accepts `+0.0` against `-0.0`, whose bits differ (`0x0000000000000000` vs `0x8000000000000000`), and it rejects a `NaN` against itself, whose bits are identical. The two spellings that do make a check bit-level are `to_bits` and `reinterpretAsUInt64`. |
//! | sample fetch — duplicate / replayed rows at the same `(metric_name, fingerprint, unix_milli)` | No suite covers this stage |
//! | sample fetch — `max_samples` budget | Covered by: crates/pulsus-read/tests/live_metrics_engine.rs::sample_budget_rejects_over_cap_fetch_and_admits_exactly_at_cap |
//! | histogram storage — the 13 `metric_hist_samples` value columns round-trip through ClickHouse | Covered by: crates/pulsus-schema/tests/live_hist_schema.rs::native_histogram_row_round_trips_losslessly_exponential_and_nhcb, crates/pulsus-schema/tests/live_hist_schema.rs::counter_reset_hint_column_is_additive_uint8_default_zero<br>`Limit:` the first asserts 12 of the 13 columns field-for-field on two samples — an exponential one with populated zero threshold/count and **both** positive and negative spans and bucket deltas, and an NHCB one with populated `custom_values`; the second covers the 13th, `counter_reset_hint` (UInt8, `DEFAULT 0`, a row inserted without it reads back 0). |
//! | histogram fetch — read-side decode of those columns into the engine's histogram type | Covered by: crates/pulsus-read/tests/live_metrics_engine.rs::dual_read_merges_and_decodes_histogram_samples_end_to_end |
//! | histogram ingest — the writer's encode of a `NativeHistogram` into those columns | Covered by: crates/pulsus-write/tests/live_metric_hist_writer.rs::native_exp_histogram_round_trips_absolute_counts_through_clickhouse<br>`Limit:` asserts schema, count, sum, positive spans, positive bucket deltas and the reset hint; zero threshold, zero count, the negative fields and `custom_values` are left at their defaults and are not asserted, so the writer emitting a populated negative side or NHCB custom values is uncovered. |
//! | evaluate (`evaluate(&query_plan, &data)`) | Nothing substituted |
//! | response marshalling (`pulsus-server` JSON) | Covered by: crates/pulsus-server/tests/api_conformance.rs::prom_query_string_literal_renders_result_type_string_live_case, crates/pulsus-server/tests/prom_api_live.rs::prom_api_serves_discovery_and_query_against_real_clickhouse<br>`Limit:` the first pins the JSON envelope byte-exactly on a **selector-free** string query — the *query* touches no tables, but the *setup* is real: `spawn_ready` (`crates/pulsus-server/tests/api_conformance.rs:299-321`) spawns the actual `pulsusdb` binary against a live ClickHouse database and blocks until `/ready` returns 200. The real result-body path is the second citation. |
//!
//! // --- SUBSTITUTION INVENTORY END ---

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
    /// Issue #155: per-sample start timestamps aligned 1:1 with
    /// `samples` (`0` = unset — the upstream sentinel). Always
    /// materialized (zeros when no `@st` line ever bound to this
    /// series) so multi-`load` merges stay a plain pair-sort;
    /// `has_st` gates whether `fetch` exposes the channel at all.
    st: Vec<i64>,
    /// Whether ANY `load` block bound an `@st` line to this series —
    /// a plain `load` fetches `start_ts: None` end-to-end (AC5).
    has_st: bool,
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

    /// Applies one `load <step>`/`load_with_nhcb <step>` block: sample
    /// `k` of a series lands at `t = k * step_ms`. A series whose labels
    /// already exist gets its new samples appended (then re-sorted),
    /// matching upstream's append-to-storage behaviour across multiple
    /// `load` blocks. After every merge+sort the storage READ-BACK hints
    /// are recomputed over the whole series ([`readback_hints`], issue
    /// #125). With `with_nhcb` set (issue #154), the block's classic
    /// series are appended as-is FIRST, then the [`super::nhcb`]
    /// conversion's output series are appended through the SAME
    /// merge+readback path (classic first, converted second — the
    /// oracle's `append` then `appendCustomHistogram` order,
    /// test.go:917-931); the readback recompute over the merged result
    /// gives the converted histograms the same #125 hint parity as any
    /// loaded counter series.
    pub fn load(
        &mut self,
        step_ms: i64,
        series: &[LoadSeries],
        with_nhcb: bool,
    ) -> Result<(), String> {
        // The block's built classic series, retained for the NHCB
        // conversion (cloned only under `with_nhcb`; the conversion
        // needs the whole block after the per-series appends consumed
        // the originals).
        let mut classic: Vec<(BTreeMap<String, String>, Vec<Sample>)> = Vec::new();
        for s in series {
            let mut samples = Vec::new();
            let mut sts: Vec<i64> = Vec::new();
            for (k, v) in s.values.iter().enumerate() {
                let t_ms = k as i64 * step_ms;
                // Issue #155: the bound `@st` offset for this value
                // position (upstream `cmd.set`, test.go:905-906: `s.ST =
                // tsMs + stVals[i].Value` for a non-omitted item; `0` =
                // unset).
                let st =
                    s.st.as_ref()
                        .and_then(|st| st[k])
                        .map(|offset| t_ms + offset)
                        .unwrap_or(0);
                match v {
                    SeqValue::Gap => {}
                    SeqValue::Stale => {
                        samples.push(Sample::float(t_ms, f64::from_bits(STALE_NAN_BITS)));
                        sts.push(st);
                    }
                    SeqValue::Value(v) => {
                        samples.push(Sample::float(t_ms, *v));
                        sts.push(st);
                    }
                    // `load` ignores hint_set; the hint VALUE rides inside
                    // the histogram (an explicit gauge/reset drives the
                    // chunk-cut emulation below).
                    //
                    // Issue #155: the ST is FORCED to 0 on histogram
                    // samples — the pin's tsdb has no histogram-ST
                    // support ("TODO: start timestamps doesn't work for
                    // histograms yet, because the tsdb support is
                    // missing", start_timestamps.test:126,:151), so the
                    // engine reads histogram STs back as 0; this store is
                    // the emulation point of that gap, mirroring
                    // upstream's own storage/eval split (the eval-side
                    // hist ST checks are still implemented per the pin).
                    SeqValue::Histogram(h, _) => {
                        samples.push(Sample::hist(t_ms, h.clone()));
                        sts.push(0);
                    }
                }
            }
            if with_nhcb {
                classic.push((s.labels.clone(), samples.clone()));
            }
            self.append_series(&s.labels, samples, sts, s.st.is_some());
        }
        if with_nhcb {
            for (labels, samples) in super::nhcb::convert_block(&classic)? {
                // Converted series never carry the `@st` channel
                // (upstream appends them with `sampleST{Sample: s}` —
                // ST = 0 = unset, test.go:1023).
                let sts = vec![0; samples.len()];
                self.append_series(&labels, samples, sts, false);
            }
        }
        Ok(())
    }

    /// Merges one built series into the store: pair-sorted `(sample,
    /// st)` append with a whole-series readback-hint recompute — the
    /// single append path shared by classic and NHCB-converted series.
    fn append_series(
        &mut self,
        labels: &BTreeMap<String, String>,
        samples: Vec<Sample>,
        sts: Vec<i64>,
        has_st: bool,
    ) {
        match self.series.iter_mut().find(|st| st.labels == *labels) {
            Some(existing) => {
                // Pair-sort `(sample, st)` by timestamp so the aligned
                // channel survives a multi-`load` merge (issue #155);
                // a side without `@st` already materialized zeros.
                let mut pairs: Vec<(Sample, i64)> = existing
                    .samples
                    .drain(..)
                    .zip(existing.st.drain(..))
                    .chain(samples.into_iter().zip(sts))
                    .collect();
                pairs.sort_by_key(|(s, _)| s.t_ms);
                (existing.samples, existing.st) = pairs.into_iter().unzip();
                existing.readback = readback_hints(&existing.samples);
                existing.has_st |= has_st;
            }
            None => {
                let readback = readback_hints(&samples);
                self.series.push(StoredSeries {
                    labels: labels.clone(),
                    samples,
                    readback,
                    st: sts,
                    has_st,
                });
            }
        }
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
            // Issue #278: compiled ONCE per selector, before the scan.
            let name_regexes = compile_matcher_regexes(&spec.name_matchers)?;
            let regexes = compile_matcher_regexes(&spec.matchers)?;
            let mut fetched = Vec::new();
            for (idx, stored) in self.series.iter().enumerate() {
                let name = stored.labels.get("__name__").map(String::as_str);
                if let Some(want) = &spec.metric_name
                    && name != Some(want.as_str())
                {
                    continue;
                }
                let mut matched = true;
                for (m, re) in spec.name_matchers.iter().zip(&name_regexes) {
                    // Absent `__name__` matches as `""`, like any label.
                    if !matcher_matches(re.as_ref(), &m.op, &m.value, name.unwrap_or("")) {
                        matched = false;
                        break;
                    }
                }
                for (m, re) in spec.matchers.iter().zip(&regexes) {
                    if !matched {
                        break;
                    }
                    let value = stored.labels.get(&m.key).map(String::as_str).unwrap_or("");
                    if !matcher_matches(re.as_ref(), &m.op, &m.value, value) {
                        matched = false;
                    }
                }
                if !matched {
                    continue;
                }
                // Issue #155: window `(sample, st)` PAIRS so the aligned
                // ST channel survives the fetch-window slice.
                let mut samples: Vec<Sample> = Vec::new();
                let mut sts: Vec<i64> = Vec::new();
                for ((s, hint), st) in stored
                    .samples
                    .iter()
                    .zip(&stored.readback)
                    .zip(&stored.st)
                    .filter(|((s, _), _)| s.t_ms > lower_excl && s.t_ms <= upper_incl)
                {
                    // Issue #125: what the engine sees is the STORAGE
                    // read-back hint, not the literal's — explicit
                    // NCR/CR per-sample hints are deliberately not
                    // round-tripped (chunks store only headers).
                    let mut s = s.clone();
                    if let Some(h) = s.h.as_mut() {
                        h.counter_reset_hint = *hint;
                    }
                    samples.push(s);
                    sts.push(*st);
                }
                fetched.push(FetchedSeries {
                    fingerprint: idx as u64,
                    metric_name: name.map(str::to_string),
                    // `Labels::new` drops `__name__` itself.
                    labels: Labels::new(stored.labels.iter().map(|(k, v)| (k.clone(), v.clone()))),
                    samples,
                    // A series never touched by `@st` fetches `None` —
                    // the production shape (AC5).
                    start_ts: stored.has_st.then_some(sts),
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

/// Issue #278: the ONE expression production uses, and the only place in
/// this driver that compiles a *user* selector pattern.
///
/// Mirrors `crates/pulsus-read/src/metrics/labels.rs:274` (the
/// concrete-name path) and `:620` (the cached path) exactly:
/// `compile_user_regex_anchored` supplies the `^(?:pat)$` anchoring and
/// the shared compile budget, and `re2_pattern_to_rust` supplies RE2's
/// reading of the pattern (issue #317). Do NOT pre-anchor here — the
/// anchoring is inside `compile_user_regex_anchored`.
///
/// Before this existed the store called `regex::Regex::new` on the raw
/// pattern, a second, private implementation of matcher semantics that
/// disagreed with Prometheus *and* with our own engine: it read
/// `\d`/`\w`/`\s` as Unicode classes (so `a\wb` matched `aµb`) and
/// rejected `a{,3}`, which RE2 takes as the literal text `a{,3}`.
fn compile_selector_regex(pattern: &str) -> Result<regex::Regex, String> {
    pulsus_re2::compile_user_regex_anchored(&pulsus_re2::re2_pattern_to_rust(pattern))
        .map_err(|e| format!("invalid selector regex {pattern:?}: {e}"))
}

/// Compiles the `Re`/`Nre` patterns of `matchers` once, in matcher order —
/// `None` for `Eq`/`Neq`, which need no regex. Called once per selector by
/// [`TestStorage::fetch`], never once per stored series (before issue #278
/// the compile sat inside the per-series scan, `O(series x matchers)`).
fn compile_matcher_regexes(
    matchers: &[pulsus_model::LabelMatcher],
) -> Result<Vec<Option<regex::Regex>>, String> {
    matchers
        .iter()
        .map(|m| match m.op {
            MatchOp::Eq | MatchOp::Neq => Ok(None),
            MatchOp::Re | MatchOp::Nre => compile_selector_regex(&m.value).map(Some),
        })
        .collect()
}

/// Prometheus matcher semantics against one label value. `re` is the
/// pre-compiled pattern for this matcher (`None` for `Eq`/`Neq`); a
/// missing label matches as the empty string (handled by the caller).
fn matcher_matches(re: Option<&regex::Regex>, op: &MatchOp, pattern: &str, value: &str) -> bool {
    match op {
        MatchOp::Eq => value == pattern,
        MatchOp::Neq => value != pattern,
        MatchOp::Re | MatchOp::Nre => {
            let is_match = re
                .expect("compile_matcher_regexes supplies a regex for every Re/Nre matcher")
                .is_match(value);
            match op {
                MatchOp::Re => is_match,
                _ => !is_match,
            }
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

    // -- issue #155 (AC5): the ST channel through load + fetch --

    /// A `rate(m[10m])` instant plan at `at_ms` — one selector whose
    /// fetch window `(at-10m, at]` covers every test sample.
    fn rate_plan(at_ms: i64) -> QueryPlan {
        let expr = pulsus_promql::parse("rate(m[10m])").expect("valid query");
        pulsus_promql::plan(
            &expr,
            pulsus_promql::PlanParams {
                start_ms: at_ms,
                end_ms: at_ms,
                step_ms: 60_000,
                lookback_ms: pulsus_promql::DEFAULT_LOOKBACK_MS,
                experimental_functions: false,
            },
        )
        .expect("plannable query")
    }

    fn m_labels() -> BTreeMap<String, String> {
        BTreeMap::from([("__name__".to_string(), "m".to_string())])
    }

    /// AC5 half 1: a PLAIN `load` (no `@st` line anywhere) fetches
    /// `start_ts: None` end-to-end — the production shape; ST-free
    /// queries take the unchanged code path.
    #[test]
    fn plain_load_fetches_start_ts_none_end_to_end() {
        let mut store = TestStorage::new();
        store
            .load(
                60_000,
                &[LoadSeries {
                    labels: m_labels(),
                    values: vec![SeqValue::Value(1.0), SeqValue::Value(2.0)],
                    st: None,
                }],
                false,
            )
            .unwrap();
        let plan = rate_plan(300_000);
        let data = store.fetch(&plan).unwrap();
        let fetched = data.get(plan.selectors[0].id);
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].samples.len(), 2);
        assert_eq!(fetched[0].start_ts, None);
    }

    /// AC5 half 2: an `@st`-bound load fetches `Some` with `st = t +
    /// offset` per non-omitted position, `0` at `_` positions, and `0`
    /// FORCED on histogram samples (the pin's tsdb gap — the store is
    /// the emulation point).
    #[test]
    fn st_load_fetches_computed_start_ts_with_zero_for_omitted_and_histogram_samples() {
        let mut store = TestStorage::new();
        let h = hist(0, 4.0, CounterResetHint::Unknown).h.unwrap();
        store
            .load(
                60_000,
                &[LoadSeries {
                    labels: m_labels(),
                    values: vec![
                        SeqValue::Value(1.0),           // t=0, `_` ST
                        SeqValue::Value(2.0),           // t=60k, ST=-30s
                        SeqValue::Value(3.0),           // t=120k, ST=-0ms
                        SeqValue::Histogram(*h, false), // t=180k, ST forced 0
                    ],
                    st: Some(vec![None, Some(-30_000), Some(0), Some(-1)]),
                }],
                false,
            )
            .unwrap();
        let plan = rate_plan(300_000);
        let data = store.fetch(&plan).unwrap();
        let fetched = data.get(plan.selectors[0].id);
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].samples.len(), 4);
        assert_eq!(
            fetched[0].start_ts,
            Some(vec![0, 30_000, 120_000, 0]),
            "`_` ⇒ 0; t+offset otherwise; histogram STs forced to 0"
        );
    }

    /// The fetch window slices `(sample, st)` pairs together: a window
    /// dropping the first sample drops its ST too.
    #[test]
    fn fetch_windows_sample_and_st_pairs_together() {
        let mut store = TestStorage::new();
        store
            .load(
                60_000,
                &[LoadSeries {
                    labels: m_labels(),
                    values: vec![
                        SeqValue::Value(1.0),
                        SeqValue::Value(2.0),
                        SeqValue::Value(3.0),
                    ],
                    st: Some(vec![None, Some(-30_000), Some(-1_000)]),
                }],
                false,
            )
            .unwrap();
        // `rate(m[10m])` at 1_000_000: the fetch window `(at - range -
        // lookback, at]` = (100_000, 1_000_000] keeps only the t=120_000
        // sample (t=0 and t=60_000 fall at/below the left-open bound).
        let plan = rate_plan(1_000_000);
        let data = store.fetch(&plan).unwrap();
        let fetched = data.get(plan.selectors[0].id);
        assert_eq!(fetched.len(), 1);
        assert_eq!(
            fetched[0]
                .samples
                .iter()
                .map(|s| s.t_ms)
                .collect::<Vec<_>>(),
            vec![120_000]
        );
        assert_eq!(fetched[0].start_ts, Some(vec![119_000]));
    }

    /// Issue #278: the vertical-tab case, which cannot live in a `.test`
    /// corpus file without an invisible control byte in the diff.
    ///
    /// RE2's `\s` is exactly `[\t\n\f\r ]` — five characters, **no**
    /// U+000B VERTICAL TAB (`re2/parse.cc`'s perl class table; our own
    /// rewrite pins the same set, see
    /// `crates/pulsus-re2/src/re2_syntax.rs`). The Rust `regex` crate's
    /// `\s` is the Unicode `White_Space` property, which DOES include
    /// U+000B. So `a\sb` must NOT match `a\u{000B}b`.
    ///
    /// The two live U+00A0 / U+0020 cases beside it are in
    /// `corpus/proof/m7_selector_regex_re2.test`; this one is here only
    /// because the byte is unreviewable in a text fixture.
    #[test]
    fn a_vertical_tab_is_not_re2_whitespace() {
        let re = compile_selector_regex(r"a\sb").expect(r"`a\sb` compiles");
        assert!(
            !re.is_match("a\u{000B}b"),
            "RE2's \\s is [\\t\\n\\f\\r ] and excludes U+000B VERTICAL TAB; the Rust \
             crate's Unicode \\s would match it"
        );
        // The four characters RE2's `\s` DOES accept in the middle,
        // plus the plain space, all match — so the assertion above is a
        // statement about U+000B, not about a regex that matches nothing.
        for c in ['\t', '\n', '\u{000C}', '\r', ' '] {
            assert!(
                re.is_match(&format!("a{c}b")),
                "RE2's \\s must accept {:?}",
                c
            );
        }
    }

    /// Issue #278, the same divergence on the other three constructs, as
    /// unit-level companions to the corpus rows: `\w`/`\d` are ASCII-only
    /// under RE2, and a malformed brace run is a literal, not a
    /// repetition (which the Rust crate rejects outright).
    #[test]
    fn perl_classes_are_ascii_and_a_malformed_brace_run_is_a_literal() {
        let w = compile_selector_regex(r"a\wb").expect("compiles");
        assert!(w.is_match("awb") && w.is_match("a3b"));
        assert!(
            !w.is_match("a\u{00B5}b"),
            "U+00B5 MICRO SIGN is not ASCII \\w"
        );
        assert!(!w.is_match("a\u{0663}b"), "U+0663 is not ASCII \\w");

        let d = compile_selector_regex(r"a\db").expect("compiles");
        assert!(d.is_match("a3b"));
        assert!(
            !d.is_match("a\u{0663}b"),
            "U+0663 ARABIC-INDIC DIGIT THREE is not ASCII \\d"
        );

        // `regex::Regex::new("^(?:a{,3})$")` — what this store used to do
        // — is an ERROR here, which aborted the whole corpus file.
        let brace = compile_selector_regex("a{,3}").expect("RE2 reads `a{,3}` as a literal");
        assert!(brace.is_match("a{,3}"));
        assert!(!brace.is_match("aaa"), "it is a literal, not a repetition");
    }
}
