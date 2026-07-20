//! `info()` — the experimental metadata-join function (issue #82,
//! M6-05b), ported from Prometheus v3.13.0 @ 40af9c2 `promql/info.go`.
//! Enriches a base instant vector with data labels from the `*_info`
//! family (default `target_info`), matching on the hard-coded identifying
//! labels `["instance", "job"]` (`info.go:34` — mirrored verbatim, no
//! config surface, per the #82 Q2 adjudication).
//!
//! Upstream fetches the info series *at eval time*, narrowed to the
//! identifying-label values observed on the non-ignored base matrix
//! (`fetchInfoSeries`'s per-label `MatchRegexp` narrowing). Our
//! pre-fetch/pure-eval split fetches the whole in-window info family
//! through one ordinary synthetic [`crate::plan::SelectorSpec`] instead
//! (PK-pruned on the effective `__name__` matchers, arg1's data matchers
//! pushed down), and this module reconstructs upstream's narrowing
//! **observationally** before any dedup/join (#82 plan v2 Δ1 + the
//! round-2 adjudication):
//!
//! 1. effective `__name__` matchers (plan-time, [`effective_info_name_matchers`]);
//! 2. `__name__` stripped from the data matchers (structural at plan
//!    time — the planner routes `__name__` matchers to the name channel,
//!    so `data_matchers` never contains them: the
//!    `removeNameFromDataLabelMatchers` port, `info.go:155`);
//! 3. ignore-set: base series whose *retained* name matches ALL effective
//!    name matchers pass through unenriched (`info.go:57-72`; membership
//!    is a pure function of the name, so it is evaluated per series
//!    rather than materialized as a hash set);
//! 4. `id_lbl_values`: the present, non-empty identifying-label values of
//!    the non-ignored base series, built ONCE over the full evaluated
//!    arg0 horizon (`eval::prepare_info` — never per step; round-2
//!    finding 2);
//! 5. the `info.go:183` short-circuit: an empty `id_lbl_values` means the
//!    info vector does not participate at all (an ID-less base can never
//!    be enriched by an ID-less info row);
//! 6. eligibility: an info series participates only if, for EVERY
//!    identifying label with a non-empty `id_lbl_values` entry set, it
//!    CARRIES that label with an allowed value — absence is not a
//!    wildcard (absent ≡ `""` fails upstream's value regexp; round-2
//!    finding 1 as adjudicated). Runs BEFORE dedup, so unrelated
//!    `(instance, job)` pairs can never raise a spurious duplicate;
//! 7. dedup per signature, newest-original-timestamp wins; an equal-
//!    timestamp pair is the pinned duplicate-series error (`info.go:401`);
//! 8. join: signature = `__name__=<infoName>` + the present identifying
//!    labels; cross-metric conflicting data label errors
//!    (`info.go:446`); include only labels named by the data matchers
//!    (when any), skip labels already on the base; when no info series
//!    matched, drop the base series iff some data matcher rejects the
//!    empty string (`info.go:459-475`).

use std::collections::{BTreeMap, BTreeSet, HashMap};

use pulsus_model::{LabelMatcher, MatchOp};

use crate::error::PromqlError;
use crate::value::{InstantSample, Labels};

/// The labels considered identifying for info metrics — upstream's
/// hard-coded `identifyingLabels` (`info.go:34`), mirrored verbatim.
pub(crate) fn identifying_labels() -> [&'static str; 2] {
    ["instance", "job"]
}

/// The default info-metric family (`info.go:32`).
pub(crate) const TARGET_INFO: &str = "target_info";

/// Port of `effectiveInfoNameMatchers` (`info.go:88-102`): any positive
/// (`Eq`/`Re`) matcher present → all matchers as-is; only negatives →
/// prepend a synthetic `__name__=~".+_info"`; none → a single
/// `__name__="target_info"` equality matcher.
pub(crate) fn effective_info_name_matchers(matchers: Vec<LabelMatcher>) -> Vec<LabelMatcher> {
    if matchers
        .iter()
        .any(|m| matches!(m.op, MatchOp::Eq | MatchOp::Re))
    {
        return matchers;
    }
    if !matchers.is_empty() {
        let mut out = Vec::with_capacity(matchers.len() + 1);
        out.push(LabelMatcher {
            key: "__name__".to_string(),
            op: MatchOp::Re,
            value: ".+_info".to_string(),
        });
        out.extend(matchers);
        return out;
    }
    vec![LabelMatcher {
        key: "__name__".to_string(),
        op: MatchOp::Eq,
        value: TARGET_INFO.to_string(),
    }]
}

/// One step-resolved info series: its real metric name (the fetch
/// layer's per-series channel), its `__name__`-free labels, and the
/// resolved sample's ORIGINAL timestamp — the newest-wins dedup key
/// (upstream encodes it through the float value; we carry it directly).
#[derive(Debug, Clone)]
pub(crate) struct InfoSeriesAtStep {
    pub metric_name: String,
    pub labels: Labels,
    pub orig_t_ms: i64,
}

/// Compiled-regex memo for one evaluation scope (a `combine` call or one
/// `prepare_info` horizon walk): each distinct `Re`/`Nre` pattern
/// compiles once, never per series. The `label_replace` per-step
/// recompilation precedent, bounded by the query's own matcher count.
#[derive(Debug, Default)]
pub(crate) struct MatcherCache {
    compiled: HashMap<String, regex::Regex>,
}

impl MatcherCache {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Prometheus matcher semantics: regexes fully anchored `^(?:pat)$`
    /// (the `concrete_name_matches`/corpus-store convention), a missing
    /// label matches as `""` (handled by the caller). The error branch is
    /// unreachable through `parse()`, which validates matcher regexes —
    /// kept total rather than trusting that upstream invariant.
    fn matches(&mut self, m: &LabelMatcher, value: &str) -> Result<bool, PromqlError> {
        Ok(match m.op {
            MatchOp::Eq => value == m.value,
            MatchOp::Neq => value != m.value,
            MatchOp::Re | MatchOp::Nre => {
                let re = match self.compiled.get(&m.value) {
                    Some(re) => re,
                    None => {
                        let re = regex::Regex::new(&format!("^(?:{})$", m.value)).map_err(|e| {
                            PromqlError::LabelSet {
                                detail: format!(
                                    "invalid matcher regex in info(): {:?}: {e}",
                                    m.value
                                ),
                            }
                        })?;
                        self.compiled.entry(m.value.clone()).or_insert(re)
                    }
                };
                let is_match = re.is_match(value);
                if m.op == MatchOp::Re {
                    is_match
                } else {
                    !is_match
                }
            }
        })
    }
}

/// `true` iff `value` matches EVERY matcher in `ms` — the ignore-set
/// membership test (`info.go:57-72`, all-effective-matchers AND) and the
/// per-group data-matcher test share this shape.
pub(crate) fn matches_all(
    cache: &mut MatcherCache,
    ms: &[LabelMatcher],
    value: &str,
) -> Result<bool, PromqlError> {
    for m in ms {
        if !cache.matches(m, value)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Steps 5+6 of the module doc's pipeline (the `:183` empty-`id_lbl_values`
/// short-circuit is the CALLER's job — see both call sites — this is only
/// the per-candidate eligibility test once that guard has already passed):
/// `name` must match every effective `__name__` matcher, `labels` must
/// satisfy every data matcher, and for every identifying label carrying a
/// non-empty `id_lbl_values` entry the candidate must CARRY that label
/// with an in-set value (absence is not a wildcard — round-2 finding 1).
///
/// Issue #82 (retroactive re-review, Option B perf fix): hoisted out of
/// `combine`'s own loop so `eval::prepare_info` can apply the identical
/// label-only half of this test ONCE per horizon, over the raw fetched
/// series, before any per-step staleness resolution — `combine` keeps
/// calling this too (now over an already-narrowed set), so there is one
/// definition of "eligible," never two that could drift.
pub(crate) fn is_eligible_info_candidate(
    cache: &mut MatcherCache,
    name: &str,
    labels: &Labels,
    id_lbl_values: &BTreeMap<String, BTreeSet<String>>,
    name_matchers: &[LabelMatcher],
    data_matchers: &[(String, Vec<LabelMatcher>)],
) -> Result<bool, PromqlError> {
    if !matches_all(cache, name_matchers, name)? {
        return Ok(false);
    }
    for (key, ms) in data_matchers {
        let value = labels.get(key).unwrap_or("");
        if !matches_all(cache, ms, value)? {
            return Ok(false);
        }
    }
    for (label, allowed) in id_lbl_values {
        match labels.get(label) {
            Some(v) if allowed.contains(v) => {}
            // Absent ≡ "" fails upstream's value regexp — absence is NOT
            // a wildcard (round-2 finding 1).
            _ => return Ok(false),
        }
    }
    Ok(true)
}

/// Renders one info series as upstream `labels.Labels.String()` does for
/// the duplicate-series error: `{name="value", ...}` sorted by name with
/// the `__name__` signature component merged in, values Go-quoted
/// ([`super::quote::go_quote`] — the #70 byte-parity port).
fn render_info_series(metric_name: &str, labels: &Labels) -> String {
    let mut pairs: Vec<(&str, &str)> = labels
        .0
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    pairs.push(("__name__", metric_name));
    pairs.sort();
    let mut out = String::from("{");
    for (i, (k, v)) in pairs.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(k);
        out.push('=');
        out.push_str(&super::quote::go_quote(v));
    }
    out.push('}');
    out
}

/// The dedup signature: `__name__=<infoName>` plus the PRESENT
/// identifying labels (upstream `sigFunction`'s
/// `MatchLabels(true, identifyingLabels...)` — an absent label simply
/// does not contribute).
type Signature = (String, Labels);

fn signature(metric_name: &str, labels: &Labels, id_keys: &[String]) -> Signature {
    (metric_name.to_string(), labels.only(id_keys))
}

/// One evaluation step's info join — steps 5–8 of the module-doc
/// pipeline over the step's base vector and step-resolved info vector.
/// `id_lbl_values` is the HORIZON-WIDE allowed-value map
/// ([`super::prepare_info`] builds it once from the full non-ignored
/// arg0 matrix — round-2 finding 2). Output is name-keeping AND clears
/// the delayed name-removal verdict: the pin constructs fresh output
/// `Sample`/`Series` values without `DropName` on every path
/// (`combineWithInfoVector`'s ignored-passthrough, enriched, and
/// fallback appends; `combineWithInfoSeries`'s series assembly), so a
/// name-dropping arg0 re-emerges from `info()` with its retained name
/// KEPT — pinned by the `info(abs(metric))` proof case, verified against
/// the pinned upstream engine (issue #82 code review round 1).
pub(crate) fn combine(
    base: Vec<InstantSample>,
    info: Vec<InfoSeriesAtStep>,
    id_lbl_values: &BTreeMap<String, BTreeSet<String>>,
    name_matchers: &[LabelMatcher],
    data_matchers: &[(String, Vec<LabelMatcher>)],
    t_ms: i64,
) -> Result<Vec<InstantSample>, PromqlError> {
    // `combineWithInfoVector`'s own guard (info.go:371): nothing can
    // match an empty base.
    if base.is_empty() {
        return Ok(Vec::new());
    }

    let mut cache = MatcherCache::new();
    let id_keys: Vec<String> = identifying_labels().iter().map(|s| s.to_string()).collect();

    // Steps 5+6: the :183 short-circuit, then the eligibility filter —
    // both BEFORE dedup, so an out-of-set or absent-ID info series can
    // never raise a spurious duplicate/conflict. Issue #82 (retroactive
    // re-review, Option B): the SAME predicate now also runs once per
    // horizon, over the raw fetched series, in `eval::prepare_info` —
    // `is_eligible_info_candidate` is the single shared definition, so
    // this loop is a no-op re-check on an already-narrowed `info` in the
    // hot (live) path, never a second source of truth.
    let mut eligible: Vec<InfoSeriesAtStep> = Vec::new();
    if !id_lbl_values.is_empty() {
        for series in info {
            if is_eligible_info_candidate(
                &mut cache,
                &series.metric_name,
                &series.labels,
                id_lbl_values,
                name_matchers,
                data_matchers,
            )? {
                eligible.push(series);
            }
        }
    }

    // Step 7: newest-original-timestamp-wins dedup per signature
    // (`combineWithInfoVector`'s rightStrSigs loop); equal timestamps are
    // the pinned duplicate-series error (info.go:401).
    let mut by_sig: BTreeMap<Signature, InfoSeriesAtStep> = BTreeMap::new();
    for series in eligible {
        let sig = signature(&series.metric_name, &series.labels, &id_keys);
        match by_sig.entry(sig) {
            std::collections::btree_map::Entry::Vacant(e) => {
                e.insert(series);
            }
            std::collections::btree_map::Entry::Occupied(mut e) => {
                let existing = e.get();
                match existing.orig_t_ms.cmp(&series.orig_t_ms) {
                    std::cmp::Ordering::Greater => {} // keep the newer existing one
                    std::cmp::Ordering::Less => {
                        e.insert(series);
                    }
                    std::cmp::Ordering::Equal => {
                        return Err(PromqlError::LabelSet {
                            detail: format!(
                                "found duplicate series for info metric: existing {} @ {}, \
                                 new {} @ {}",
                                render_info_series(&existing.metric_name, &existing.labels),
                                existing.orig_t_ms,
                                render_info_series(&series.metric_name, &series.labels),
                                series.orig_t_ms,
                            ),
                        });
                    }
                }
            }
        }
    }

    // The distinct info-metric names present after dedup — the per-base
    // signature probe set (upstream `infoMetrics`). Sorted (BTreeSet) for
    // determinism; upstream iterates a Go map, but the outcome is
    // order-independent (a genuine cross-metric conflict errors under
    // every order, and non-conflicting labels commute).
    let info_names: BTreeSet<String> = by_sig.keys().map(|(name, _)| name.clone()).collect();

    // The empty-string fallback verdict (info.go:459-475), computed once:
    // when NO info series matched a base series, the series drops iff
    // some data matcher rejects the empty string.
    let mut all_matchers_match_empty = true;
    for (_, ms) in data_matchers {
        for m in ms {
            if !cache.matches(m, "")? {
                all_matchers_match_empty = false;
            }
        }
    }

    // Step 8: the per-base join.
    let mut out = Vec::with_capacity(base.len());
    for bs in base {
        // Step 3: the ignore-set — a pure function of the retained name
        // (upstream materializes the same predicate as a hash set).
        let retained = bs.metric_name.as_deref().unwrap_or("");
        if matches_all(&mut cache, name_matchers, retained)? {
            // Fresh output sample (upstream `Sample{Metric, F, H}` —
            // DropName cleared even on the ignored-passthrough path).
            out.push(InstantSample {
                labels: bs.labels,
                metric_name: bs.metric_name,
                drop_name: false,
                t_ms,
                v: bs.v,
                h: bs.h,
            });
            continue;
        }

        let base_id_labels = bs.labels.only(&id_keys);
        // The enh.lb builder: data labels accumulated across info
        // metrics, conflict-checked (info.go:446).
        let mut added: Vec<(String, String)> = Vec::new();
        let mut seen_any = false;
        for info_name in &info_names {
            let sig = (info_name.clone(), base_id_labels.clone());
            let Some(series) = by_sig.get(&sig) else {
                continue;
            };
            for (key, value) in &series.labels.0 {
                // Not among the specified data label matchers (when any
                // are specified) — key-presence only, exactly upstream.
                if !data_matchers.is_empty() && !data_matchers.iter().any(|(k, _)| k == key) {
                    continue;
                }
                // Conflict check BEFORE the base-label skip (upstream's
                // own order; empty accumulated values never conflict).
                if let Some((_, existing)) = added.iter_mut().find(|(k, _)| k == key) {
                    if !existing.is_empty() && existing != value {
                        return Err(PromqlError::LabelSet {
                            detail: format!("conflicting label: {key}"),
                        });
                    }
                    *existing = value.clone();
                    continue;
                }
                // Skip labels already on the base metric.
                if bs.labels.get(key).is_some() {
                    continue;
                }
                added.push((key.clone(), value.clone()));
            }
            seen_any = true;
        }

        if !seen_any && !all_matchers_match_empty {
            // No matching info series and a data matcher requires a
            // non-empty value: the base series is dropped.
            continue;
        }

        let mut labels = bs.labels;
        for (key, value) in added {
            labels.set(key, value);
        }
        out.push(InstantSample {
            labels,
            // Name-keeping construct: the retained name passes through;
            // the delayed verdict is CLEARED (fresh upstream output
            // samples carry no DropName — see the fn doc).
            metric_name: bs.metric_name,
            drop_name: false,
            t_ms,
            v: bs.v,
            h: bs.h,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eq(key: &str, value: &str) -> LabelMatcher {
        LabelMatcher {
            key: key.to_string(),
            op: MatchOp::Eq,
            value: value.to_string(),
        }
    }

    fn re(key: &str, value: &str) -> LabelMatcher {
        LabelMatcher {
            key: key.to_string(),
            op: MatchOp::Re,
            value: value.to_string(),
        }
    }

    fn nre(key: &str, value: &str) -> LabelMatcher {
        LabelMatcher {
            key: key.to_string(),
            op: MatchOp::Nre,
            value: value.to_string(),
        }
    }

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        Labels::new(pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())))
    }

    fn base_sample(name: &str, pairs: &[(&str, &str)], v: f64) -> InstantSample {
        InstantSample {
            labels: labels(pairs),
            metric_name: Some(name.to_string()),
            drop_name: false,
            t_ms: 0,
            v,
            h: None,
        }
    }

    fn info_series(name: &str, pairs: &[(&str, &str)], orig_t_ms: i64) -> InfoSeriesAtStep {
        InfoSeriesAtStep {
            metric_name: name.to_string(),
            labels: labels(pairs),
            orig_t_ms,
        }
    }

    fn id_values(entries: &[(&str, &[&str])]) -> BTreeMap<String, BTreeSet<String>> {
        entries
            .iter()
            .map(|(k, vs)| {
                (
                    k.to_string(),
                    vs.iter().map(|v| v.to_string()).collect::<BTreeSet<_>>(),
                )
            })
            .collect()
    }

    fn default_name_matchers() -> Vec<LabelMatcher> {
        effective_info_name_matchers(Vec::new())
    }

    // --- effective_info_name_matchers (AC3: the pinned three branches) ---

    #[test]
    fn effective_name_matchers_with_a_positive_matcher_return_as_is() {
        let input = vec![re("__name__", "target_.+"), nre("__name__", "build.*")];
        assert_eq!(effective_info_name_matchers(input.clone()), input);
    }

    #[test]
    fn effective_name_matchers_only_negative_prepend_the_synthetic_info_regex() {
        let input = vec![nre("__name__", "websvc_.+")];
        let out = effective_info_name_matchers(input);
        assert_eq!(
            out,
            vec![re("__name__", ".+_info"), nre("__name__", "websvc_.+")]
        );
    }

    #[test]
    fn effective_name_matchers_empty_default_to_target_info_equality() {
        assert_eq!(
            effective_info_name_matchers(Vec::new()),
            vec![eq("__name__", TARGET_INFO)]
        );
    }

    // --- eligibility (round-2 findings 1 + Δ1 step 6, before dedup) ---

    /// An info series carrying an identifying-label value outside the
    /// horizon-wide allowed set is dropped BEFORE dedup: two duplicate
    /// out-of-set series (same signature, equal timestamps) must not
    /// raise the duplicate error, and the in-set series still enriches.
    #[test]
    fn eligibility_drops_out_of_set_identifying_values_before_dedup() {
        let base = vec![base_sample(
            "metric",
            &[("instance", "a"), ("job", "1")],
            7.0,
        )];
        let info = vec![
            info_series(
                "target_info",
                &[("instance", "zzz"), ("job", "9"), ("data", "x")],
                0,
            ),
            info_series(
                "target_info",
                &[("instance", "zzz"), ("job", "9"), ("data", "y")],
                0,
            ),
            info_series(
                "target_info",
                &[("instance", "a"), ("job", "1"), ("data", "info")],
                0,
            ),
        ];
        let ids = id_values(&[("instance", &["a"]), ("job", &["1"])]);
        let out = combine(base, info, &ids, &default_name_matchers(), &[], 0).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].labels.get("data"), Some("info"));
    }

    /// Round-2 finding 1 (adjudicated): an info series MISSING an
    /// identifying label that has non-empty allowed values is excluded —
    /// absence is not a wildcard. The job-less info row would otherwise
    /// signature-match the job-less base series.
    #[test]
    fn eligibility_excludes_an_info_series_missing_a_constrained_identifying_label() {
        let base = vec![
            base_sample("metric", &[("instance", "a")], 1.0),
            base_sample("metric", &[("instance", "a"), ("job", "1")], 2.0),
        ];
        let info = vec![info_series(
            "custom_info",
            &[("instance", "a"), ("extra", "x")],
            0,
        )];
        let ids = id_values(&[("instance", &["a"]), ("job", &["1"])]);
        let name_matchers = vec![eq("__name__", "custom_info")];
        let out = combine(base, info, &ids, &name_matchers, &[], 0).unwrap();
        assert_eq!(out.len(), 2, "both base series pass through unenriched");
        for s in &out {
            assert_eq!(s.labels.get("extra"), None, "absence must not enrich");
        }
    }

    /// Δ1 step 5 (info.go:183): an empty horizon-wide id-value map means
    /// zero info participation — an ID-less base with an ID-less info
    /// row present does NOT enrich.
    #[test]
    fn empty_id_lbl_values_short_circuits_all_info_participation() {
        let base = vec![base_sample("metric", &[("l", "v")], 3.0)];
        let info = vec![info_series("custom_info", &[("enriched", "yes")], 0)];
        let ids = BTreeMap::new();
        let name_matchers = vec![eq("__name__", "custom_info")];
        let out = combine(base, info, &ids, &name_matchers, &[], 0).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].labels.get("enriched"), None);
    }

    /// The short-circuit still applies the empty-string fallback: a data
    /// matcher rejecting `""` drops the (non-ignored) base series.
    #[test]
    fn empty_id_lbl_values_with_a_non_empty_data_matcher_drops_the_base_series() {
        let base = vec![base_sample("metric", &[("l", "v")], 3.0)];
        let info = vec![info_series("custom_info", &[("enriched", "yes")], 0)];
        let ids = BTreeMap::new();
        let name_matchers = vec![eq("__name__", "custom_info")];
        let data = vec![("enriched".to_string(), vec![re("enriched", ".+")])];
        let out = combine(base, info, &ids, &name_matchers, &data, 0).unwrap();
        assert!(out.is_empty());
    }

    // --- Δ3: the exact pinned error strings ---

    /// The full duplicate-series message (info.go:401): both label sets
    /// (with the `__name__` signature component) and both original
    /// timestamps, deterministic.
    #[test]
    fn equal_timestamp_duplicate_info_series_error_carries_both_label_sets_and_timestamps() {
        let base = vec![base_sample(
            "metric",
            &[("instance", "a"), ("job", "1")],
            1.0,
        )];
        let info = vec![
            info_series(
                "target_info",
                &[("instance", "a"), ("job", "1"), ("data", "info")],
                60_000,
            ),
            info_series(
                "target_info",
                &[("instance", "a"), ("job", "1"), ("data", "updated")],
                60_000,
            ),
        ];
        let ids = id_values(&[("instance", &["a"]), ("job", &["1"])]);
        let err = combine(base, info, &ids, &default_name_matchers(), &[], 60_000).unwrap_err();
        assert_eq!(
            err.to_string(),
            "found duplicate series for info metric: existing \
             {__name__=\"target_info\", data=\"info\", instance=\"a\", job=\"1\"} @ 60000, \
             new {__name__=\"target_info\", data=\"updated\", instance=\"a\", job=\"1\"} @ 60000"
        );
    }

    /// `conflicting label: <name>` (info.go:446), byte-exact.
    #[test]
    fn conflicting_data_label_across_info_metrics_errors_with_the_pinned_message() {
        let base = vec![base_sample(
            "metric",
            &[("instance", "a"), ("job", "1")],
            1.0,
        )];
        let info = vec![
            info_series(
                "target_info",
                &[("instance", "a"), ("job", "1"), ("x", "from_target")],
                0,
            ),
            info_series(
                "build_info",
                &[("instance", "a"), ("job", "1"), ("x", "from_build")],
                0,
            ),
        ];
        let ids = id_values(&[("instance", &["a"]), ("job", &["1"])]);
        let name_matchers = vec![re("__name__", ".+_info")];
        let err = combine(base, info, &ids, &name_matchers, &[], 0).unwrap_err();
        assert_eq!(err.to_string(), "conflicting label: x");
    }

    // --- dedup + join semantics ---

    #[test]
    fn newer_original_timestamp_wins_the_dedup() {
        let base = vec![base_sample(
            "metric",
            &[("instance", "a"), ("job", "1")],
            5.0,
        )];
        let info = vec![
            info_series(
                "target_info",
                &[("instance", "a"), ("job", "1"), ("data", "old")],
                60_000,
            ),
            info_series(
                "target_info",
                &[("instance", "a"), ("job", "1"), ("data", "new")],
                120_000,
            ),
        ];
        let ids = id_values(&[("instance", &["a"]), ("job", &["1"])]);
        let out = combine(base, info, &ids, &default_name_matchers(), &[], 120_000).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].labels.get("data"), Some("new"));
    }

    #[test]
    fn ignored_base_series_pass_through_even_when_a_data_matcher_rejects_empty() {
        // An info-family base series is never enriched AND never dropped
        // by the fallback (upstream ignores it before the fallback runs).
        let base = vec![base_sample(
            "build_info",
            &[("instance", "a"), ("job", "1"), ("build_data", "build")],
            1.0,
        )];
        let ids = id_values(&[("instance", &["a"]), ("job", &["1"])]);
        let name_matchers = vec![re("__name__", ".+_info")];
        let data = vec![("another_data".to_string(), vec![re("another_data", ".+")])];
        let out = combine(base, Vec::new(), &ids, &name_matchers, &data, 0).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].metric_name.as_deref(), Some("build_info"));
    }

    #[test]
    fn data_labels_already_on_the_base_metric_are_skipped() {
        let base = vec![base_sample(
            "metric",
            &[("instance", "a"), ("job", "1"), ("data", "base")],
            1.0,
        )];
        let info = vec![info_series(
            "target_info",
            &[
                ("instance", "a"),
                ("job", "1"),
                ("data", "info"),
                ("extra", "e"),
            ],
            0,
        )];
        let ids = id_values(&[("instance", &["a"]), ("job", &["1"])]);
        let out = combine(base, info, &ids, &default_name_matchers(), &[], 0).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].labels.get("data"), Some("base"), "base value kept");
        assert_eq!(out[0].labels.get("extra"), Some("e"));
    }

    #[test]
    fn empty_base_short_circuits_to_an_empty_result() {
        let info = vec![info_series(
            "target_info",
            &[("instance", "a"), ("data", "info")],
            0,
        )];
        let ids = id_values(&[("instance", &["a"])]);
        let out = combine(Vec::new(), info, &ids, &default_name_matchers(), &[], 0).unwrap();
        assert!(out.is_empty());
    }

    /// Issue #82 code review round 1 (finding 2): the pin constructs
    /// fresh output samples without `DropName`, so a drop-marked base
    /// sample re-emerges with its retained name KEPT — on the enriched,
    /// fallback-passthrough, AND ignored-passthrough paths alike.
    #[test]
    fn output_is_name_keeping_and_clears_the_drop_verdict_on_every_path() {
        // Enriched path.
        let mut bs = base_sample("metric", &[("instance", "a")], 9.0);
        bs.drop_name = true;
        let info = vec![info_series(
            "target_info",
            &[("instance", "a"), ("data", "info")],
            0,
        )];
        let ids = id_values(&[("instance", &["a"])]);
        let out = combine(vec![bs], info, &ids, &default_name_matchers(), &[], 0).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].metric_name.as_deref(), Some("metric"));
        assert!(!out[0].drop_name, "enriched output clears the verdict");

        // Fallback-passthrough path (no matching info series).
        let mut bs = base_sample("metric", &[("instance", "b")], 9.0);
        bs.drop_name = true;
        let out = combine(
            vec![bs],
            Vec::new(),
            &id_values(&[("instance", &["b"])]),
            &default_name_matchers(),
            &[],
            0,
        )
        .unwrap();
        assert!(!out[0].drop_name, "fallback output clears the verdict");

        // Ignored-passthrough path (base series IS an info series).
        let mut bs = base_sample("target_info", &[("instance", "a"), ("data", "x")], 1.0);
        bs.drop_name = true;
        let out = combine(
            vec![bs],
            Vec::new(),
            &id_values(&[("instance", &["a"])]),
            &default_name_matchers(),
            &[],
            0,
        )
        .unwrap();
        assert!(!out[0].drop_name, "ignored passthrough clears the verdict");
    }
}
