//! [`LabelSet`]: the canonical, key-sorted label collection used for both
//! fingerprinting ([`crate::fingerprint`]) and canonical JSON serialization
//! (docs/architecture.md §2.2). Normalized-key collision semantics are
//! frozen by the issue #4 plan amendment: `from_normalized` is infallible
//! and lossy (deterministic, input-order-independent resolution), while
//! `try_from_normalized` rejects a collision outright for callers that must
//! not silently drop conflicting label data.

use std::collections::{BTreeMap, BTreeSet};

use thiserror::Error;

use crate::canonical::{SERVICE_NAME_LABEL, canonicalize_label_key};

/// Errors from constructing a [`LabelSet`] via the strict
/// [`LabelSet::try_from_normalized`] constructor.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum LabelError {
    /// Two or more distinct `(key, value)` entries share a normalized key —
    /// either because their original keys both normalize to the same
    /// label key (e.g. `service.name` and `service_name`), or because the
    /// same original key was supplied more than once with conflicting
    /// values. `originals` lists the distinct original keys involved,
    /// sorted.
    #[error(
        "label keys {originals:?} all normalize to \"{normalized}\": use \
         LabelSet::from_normalized for the deterministic lossy resolution, \
         or de-duplicate before calling try_from_normalized"
    )]
    NormalizationCollision {
        normalized: String,
        originals: Vec<String>,
    },
}

/// A key-sorted, key-unique set of labels. Two `LabelSet`s built from the
/// same logical content compare equal and serialize identically regardless
/// of the order their source data arrived in — this order-independence is
/// the invariant both fingerprint functions ([`crate::fingerprint`]) and
/// [`LabelSet::to_canonical_json`] depend on.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LabelSet {
    /// Sorted by key (ascending), one entry per key.
    entries: Vec<(String, String)>,
}

/// Drops every pair whose value is exactly `""`, leaving any same-named
/// non-empty pair in place — Prometheus' "an empty label value is the same as
/// an absent label" rule in its PAIR-WISE form.
///
/// This is `labels.Labels.WithoutEmpty()`
/// (`vendor/github.com/prometheus/prometheus/model/labels/labels_stringlabels.go:261-286
/// @ v3.7.4`), which Loki's log ingest applies to **stream labels**:
/// `syntax.ParseLabels` ends in `return ls.WithoutEmpty()`
/// (`pkg/logql/syntax/parser.go:279-296 @ v3.7.4`), with the reason given
/// inline — an empty value alters the label-set hash, so it must be normalized
/// away early on the write path or a stream's identity is not deterministic.
/// Every Loki log receiver reaches it through
/// `Distributor.parseStreamLabels`, which calls `syntax.ParseLabels` on the
/// stream's label literal (`pkg/distributor/distributor.go:1363-1375`, reached
/// from `:648 @ v3.7.4`) — including the OTLP one, whose translation renders
/// the promoted resource attributes back into exactly such a literal
/// (`pkg/loghttp/push/otlp.go:240-250 @ v3.7.4`).
///
/// Use this — NOT [`strip_empty_valued_labels`] — wherever a STREAM LABEL set
/// is built. The two differ only when the same name appears twice, which the
/// reference resolves differently at its two seams; see
/// [`strip_empty_valued_labels`] for the discriminating measurement.
///
/// Only an exactly-empty value is dropped. A whitespace-only value is kept
/// verbatim and nothing is trimmed — measured on `grafana/loki:3.7.4` at both
/// receivers: a Loki-push stream label `e=" "` and an OTLP index label
/// `deployment.environment=" "` both survive, each forming a stream distinct
/// from one that omits the label.
///
/// Deliberately NOT applied by the metrics paths: Prometheus, not Loki, is the
/// metrics reference, and its own rejection/normalization surface is governed
/// separately.
pub fn retain_non_empty_values(pairs: &mut Vec<(String, String)>) {
    // Borrowed scan first: the overwhelmingly common input has no empty value,
    // and `retain` on it would still walk every element shifting nothing.
    if !pairs.iter().any(|(_, value)| value.is_empty()) {
        return;
    }
    pairs.retain(|(_, value)| !value.is_empty());
}

/// Drops every pair whose NAME carries an empty value anywhere in `pairs` —
/// the same "empty means absent" rule in its BY-NAME form, applied to a RAW
/// pair list *before* any key canonicalization.
///
/// This is what Prometheus' `labels.Builder` does, and Loki's distributor runs
/// every entry's **structured metadata** through one
/// (`pkg/distributor/distributor.go:698-722 @ v3.7.4`): `NewBuilder` calls
/// `Reset`, which records the NAME of every empty-valued base label in `del`,
/// and `Labels()` then omits every base label carrying such a name
/// (`vendor/github.com/prometheus/prometheus/model/labels/labels_stringlabels.go:471-521`
/// — the file actually compiled: it is `//go:build !slicelabels &&
/// !dedupelabels`, and Loki builds with `-tags netgo` alone,
/// `Makefile:54-64 @ v3.7.4`). `Builder.Set(n, "")` is likewise
/// defined as `Del(n)` (`labels_common.go:187-192`), which is how a name that
/// the distributor *renames* into an existing one takes that one with it.
///
/// Use this — NOT [`retain_non_empty_values`] — wherever a STRUCTURED
/// METADATA set is built.
///
/// The discriminating case, measured against `grafana/loki:3.7.4`
/// (`b318f282`, `allow_structured_metadata: true`, `discover_log_levels:
/// false`, `discover_service_name: []`) — one duplicate name, one occurrence
/// empty, pushed both ways round:
///
/// | pushed | as structured metadata | as stream labels |
/// |---|---|---|
/// | `a=""` then `a="keep"` | no `a` at all | `a="keep"` |
/// | `a="keep"` then `a=""` | no `a` at all | `a="keep"` |
///
/// (Stream labels measured on the Loki-push protobuf transport: of the two
/// push encodings its label literal is the only one that carries a duplicate
/// name as far as the strip, because the JSON one decodes its label object
/// into a map first — both here and on the reference,
/// `pkg/loghttp/labels.go:24-40 @ v3.7.4`. The OTLP receiver's attribute list
/// can repeat a key too, and resolves it differently again; that seam is
/// documented at `otlp_logs::build_stream_labels`. Structured metadata keeps
/// duplicates on BOTH push encodings: protobuf as a repeated field, JSON
/// because the reference appends each object key to a slice,
/// `pkg/loghttp/query.go:182-203 @ v3.7.4`, exactly as we do.)
///
/// Only an exactly-empty value is dropped; a whitespace-only value is kept
/// verbatim (`{"a":" "}` round-trips as `a=" "`) and nothing is trimmed.
pub fn strip_empty_valued_labels(pairs: &mut Vec<(String, String)>) {
    // Fast path: the overwhelmingly common case has no empty value at all, so
    // it costs one borrowed scan and allocates nothing.
    if !pairs.iter().any(|(_, value)| value.is_empty()) {
        return;
    }
    // Cloned because `retain`'s closure cannot borrow `pairs` while it mutates
    // them. Only the empty-valued names are cloned, only on this rare path,
    // and the count is bounded by the caller's per-stream / per-entry cap.
    let empty_names: BTreeSet<String> = pairs
        .iter()
        .filter(|(_, value)| value.is_empty())
        .map(|(name, _)| name.clone())
        .collect();
    pairs.retain(|(name, _)| !empty_names.contains(name.as_str()));
}

/// Groups `pairs` by `normalize(key)`, deduplicating identical
/// `(original_key, value)` pairs within each group (a `BTreeSet` member is
/// unique by definition). Shared by every `LabelSet` constructor so the
/// grouping/collision logic is expressed exactly once.
fn group_by<I, F>(pairs: I, mut normalize: F) -> BTreeMap<String, BTreeSet<(String, String)>>
where
    I: IntoIterator<Item = (String, String)>,
    F: FnMut(&str) -> String,
{
    let mut groups: BTreeMap<String, BTreeSet<(String, String)>> = BTreeMap::new();
    for (key, value) in pairs {
        let normalized = normalize(&key);
        groups.entry(normalized).or_default().insert((key, value));
    }
    groups
}

/// Picks the winning `(original_key, value)` within a normalized-key group,
/// per the frozen rule (issue #4 plan amendment): the entry with the
/// lexicographically greatest original key, ties broken by the
/// lexicographically greatest value. `BTreeSet<(String, String)>` already
/// orders its members by exactly this tuple comparison (key first, then
/// value), so the winner is simply the maximum element.
fn winner(distinct: &BTreeSet<(String, String)>) -> &(String, String) {
    // Infallible invariant, not a runtime check: every group produced by
    // `group_by` has at least one member, because a `BTreeMap` entry is
    // only ever created together with its first `.insert(...)`.
    distinct
        .iter()
        .next_back()
        .expect("label group is non-empty by construction (group_by never creates an empty set)")
}

impl LabelSet {
    /// Infallible, lossy constructor for logs/metrics: canonicalizes every
    /// key (`[^a-zA-Z0-9_]` -> `_`, [`canonicalize_label_key`]) and
    /// resolves any resulting collision deterministically.
    ///
    /// Never fails — ingest must never drop an entire label set over a key
    /// collision. Returns the resolved `LabelSet` plus a `collision_count`
    /// of *losing* entries (the writer surfaces this as a metric).
    /// Identical duplicate `(key, value)` pairs collapse silently and are
    /// **not** counted as collisions.
    pub fn from_normalized<I>(pairs: I) -> (LabelSet, usize)
    where
        I: IntoIterator<Item = (String, String)>,
    {
        let groups = group_by(pairs, canonicalize_label_key);
        let mut collision_count = 0usize;
        let mut entries = Vec::with_capacity(groups.len());
        for (normalized, distinct) in groups {
            collision_count += distinct.len().saturating_sub(1);
            let (_original_key, value) = winner(&distinct);
            entries.push((normalized, value.clone()));
        }
        (LabelSet { entries }, collision_count)
    }

    /// Strict variant of [`LabelSet::from_normalized`]: rejects any
    /// normalized-key collision (more than one distinct `(key, value)` pair
    /// sharing a normalized key) instead of resolving it, returning
    /// [`LabelError::NormalizationCollision`]. `from_normalized`'s lossy
    /// resolution is still available for callers that must never fail.
    pub fn try_from_normalized<I>(pairs: I) -> Result<LabelSet, LabelError>
    where
        I: IntoIterator<Item = (String, String)>,
    {
        let groups = group_by(pairs, canonicalize_label_key);
        let mut entries = Vec::with_capacity(groups.len());
        for (normalized, distinct) in groups {
            if distinct.len() > 1 {
                let originals: BTreeSet<String> = distinct.iter().map(|(k, _)| k.clone()).collect();
                return Err(LabelError::NormalizationCollision {
                    normalized,
                    originals: originals.into_iter().collect(),
                });
            }
            let (_original_key, value) = winner(&distinct);
            entries.push((normalized, value.clone()));
        }
        Ok(LabelSet { entries })
    }

    /// Verbatim constructor for traces (docs/architecture.md §2.2): keys
    /// are never canonicalized. An exact duplicate key still resolves
    /// deterministically (greatest value wins, same tie-break rule as
    /// [`LabelSet::from_normalized`]) so `LabelSet`'s sorted/unique
    /// invariant always holds — but this is not a normalization collision,
    /// so no count is reported.
    pub fn from_verbatim<I>(pairs: I) -> LabelSet
    where
        I: IntoIterator<Item = (String, String)>,
    {
        let groups = group_by(pairs, |k| k.to_string());
        let mut entries = Vec::with_capacity(groups.len());
        for (key, distinct) in groups {
            let (_original_key, value) = winner(&distinct);
            entries.push((key, value.clone()));
        }
        LabelSet { entries }
    }

    /// The value for `key`, if present.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.entries
            .binary_search_by(|(k, _)| k.as_str().cmp(key))
            .ok()
            .map(|i| self.entries[i].1.as_str())
    }

    /// Derives the physical `service` column value (docs/architecture.md
    /// §2.3): the `service_name` label's value, or `""` if absent. This is
    /// the single function the writer and the planner both call so that a
    /// `{service_name="checkout"}` label, an OTel `service.name` attribute
    /// (normalized to `service_name` by [`LabelSet::from_normalized`]), and
    /// the physical `service` column all resolve to the identical string
    /// (issue #4 AC#3) — see the `normalization_chain` cases in
    /// `tests/golden.rs`.
    pub fn service(&self) -> &str {
        self.get(SERVICE_NAME_LABEL).unwrap_or("")
    }

    /// Iterates `(key, value)` pairs in sorted key order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.entries.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// Number of labels.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True if this label set has no labels.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Canonical JSON: sorted keys (guaranteed by `LabelSet`'s own
    /// invariant — iteration order is never re-derived here), `serde_json`
    /// string escaping. This is the exact string stored in
    /// `log_streams.labels` (docs/architecture.md §2.2); the
    /// `log_streams_idx` materialized view re-reads it via
    /// `JSONExtractKeysAndValues` rather than recomputing the fingerprint,
    /// so this key order and escaping must stay stable across releases.
    pub fn to_canonical_json(&self) -> String {
        let mut out = String::from("{");
        for (i, (k, v)) in self.entries.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            // `serde_json::to_string` on a `&str` cannot fail (no
            // NaN/Infinity-float or non-UTF-8 concern applies to strings):
            // infallible in practice, not a runtime-checked invariant.
            out.push_str(
                &serde_json::to_string(k)
                    .expect("label key is a valid UTF-8 &str: JSON string encoding cannot fail"),
            );
            out.push(':');
            out.push_str(
                &serde_json::to_string(v)
                    .expect("label value is a valid UTF-8 &str: JSON string encoding cannot fail"),
            );
        }
        out.push('}');
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pairs(items: &[(&str, &str)]) -> Vec<(String, String)> {
        items
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn from_verbatim_sorts_by_key_regardless_of_input_order() {
        let a = LabelSet::from_verbatim(pairs(&[("b", "2"), ("a", "1"), ("c", "3")]));
        let b = LabelSet::from_verbatim(pairs(&[("c", "3"), ("a", "1"), ("b", "2")]));
        assert_eq!(a, b);
        assert_eq!(
            a.iter().collect::<Vec<_>>(),
            vec![("a", "1"), ("b", "2"), ("c", "3")]
        );
    }

    #[test]
    fn from_verbatim_does_not_canonicalize_keys() {
        let set = LabelSet::from_verbatim(pairs(&[("resource.service.name", "checkout")]));
        assert_eq!(set.get("resource.service.name"), Some("checkout"));
        assert_eq!(set.get("resource_service_name"), None);
    }

    #[test]
    fn from_normalized_canonicalizes_before_sorting() {
        let (set, collisions) = LabelSet::from_normalized(pairs(&[("service.name", "checkout")]));
        assert_eq!(collisions, 0);
        assert_eq!(set.get("service_name"), Some("checkout"));
    }

    #[test]
    fn from_normalized_identical_duplicate_collapses_uncounted() {
        let (set, collisions) = LabelSet::from_normalized(pairs(&[
            ("service.name", "checkout"),
            ("service.name", "checkout"),
        ]));
        assert_eq!(collisions, 0);
        assert_eq!(set.len(), 1);
        assert_eq!(set.get("service_name"), Some("checkout"));
    }

    #[test]
    fn from_normalized_resolves_dot_vs_underscore_collision_by_greatest_original_key() {
        // "service_name" (0x5F '_') > "service.name" (0x2E '.') byte-wise,
        // so the "service_name"-keyed entry's value wins.
        let (set, collisions) = LabelSet::from_normalized(pairs(&[
            ("service.name", "from_dot"),
            ("service_name", "from_underscore"),
        ]));
        assert_eq!(collisions, 1);
        assert_eq!(set.get("service_name"), Some("from_underscore"));
    }

    #[test]
    fn from_normalized_collision_resolution_is_input_order_independent() {
        let (a, ca) = LabelSet::from_normalized(pairs(&[
            ("service.name", "from_dot"),
            ("service_name", "from_underscore"),
        ]));
        let (b, cb) = LabelSet::from_normalized(pairs(&[
            ("service_name", "from_underscore"),
            ("service.name", "from_dot"),
        ]));
        assert_eq!(a, b);
        assert_eq!(ca, cb);
    }

    #[test]
    fn from_normalized_same_original_key_conflicting_values_breaks_tie_by_value() {
        let (set, collisions) =
            LabelSet::from_normalized(pairs(&[("env", "prod"), ("env", "staging")]));
        assert_eq!(collisions, 1);
        // "staging" > "prod" byte-wise.
        assert_eq!(set.get("env"), Some("staging"));
    }

    #[test]
    fn try_from_normalized_rejects_dot_vs_underscore_collision() {
        let err = LabelSet::try_from_normalized(pairs(&[
            ("service.name", "from_dot"),
            ("service_name", "from_underscore"),
        ]))
        .unwrap_err();
        match err {
            LabelError::NormalizationCollision {
                normalized,
                originals,
            } => {
                assert_eq!(normalized, "service_name");
                assert_eq!(originals, vec!["service.name", "service_name"]);
            }
        }
    }

    #[test]
    fn try_from_normalized_accepts_identical_duplicates() {
        let set = LabelSet::try_from_normalized(pairs(&[
            ("service.name", "checkout"),
            ("service.name", "checkout"),
        ]))
        .expect("identical duplicates are not a collision");
        assert_eq!(set.len(), 1);
        assert_eq!(set.get("service_name"), Some("checkout"));
    }

    #[test]
    fn try_from_normalized_matches_lossy_resolution_when_no_collision() {
        let (lossy, collisions) = LabelSet::from_normalized(pairs(&[("service.name", "checkout")]));
        assert_eq!(collisions, 0);
        let strict = LabelSet::try_from_normalized(pairs(&[("service.name", "checkout")]))
            .expect("no collision");
        assert_eq!(lossy, strict);
    }

    #[test]
    fn get_returns_none_for_missing_key() {
        let set = LabelSet::from_verbatim(pairs(&[("a", "1")]));
        assert_eq!(set.get("missing"), None);
    }

    #[test]
    fn empty_label_set_has_empty_canonical_json() {
        let set = LabelSet::from_verbatim(Vec::new());
        assert!(set.is_empty());
        assert_eq!(set.to_canonical_json(), "{}");
    }

    #[test]
    fn to_canonical_json_sorts_keys_and_escapes_special_characters() {
        let set = LabelSet::from_verbatim(pairs(&[
            ("z_key", "line1\nline2"),
            ("a_key", "quote\"and\\backslash"),
            ("m_key", "café"),
        ]));
        assert_eq!(
            set.to_canonical_json(),
            "{\"a_key\":\"quote\\\"and\\\\backslash\",\"m_key\":\"café\",\"z_key\":\"line1\\nline2\"}"
        );
    }

    // -- empty-value strips (issue #259) -----------------------------------

    #[test]
    fn both_strips_remove_only_exactly_empty_values() {
        // Neither trims: a whitespace-only value is a value.
        let source = pairs(&[("a", ""), ("b", "v"), ("c", " "), ("d", "\t"), ("e", "0")]);
        let expected = pairs(&[("b", "v"), ("c", " "), ("d", "\t"), ("e", "0")]);

        let mut by_name = source.clone();
        strip_empty_valued_labels(&mut by_name);
        assert_eq!(by_name, expected);

        let mut pair_wise = source;
        retain_non_empty_values(&mut pair_wise);
        assert_eq!(pair_wise, expected);
    }

    /// The one case that tells the two apart, and the reason there are two of
    /// them. Loki's structured-metadata seam is a `labels.Builder`, whose
    /// `Reset` records the NAME of an empty-valued base label so `Labels()`
    /// omits every base label carrying it; its stream-label seam is
    /// `WithoutEmpty()`, which drops the empty PAIR and nothing else.
    ///
    /// Measured on `grafana/loki:3.7.4` (`b318f282`) with the same duplicate
    /// pushed both ways round: as structured metadata (either transport) no
    /// `a` survives; as protobuf stream labels `a="keep"` survives.
    #[test]
    fn by_name_strip_takes_the_non_empty_twin_where_pair_wise_keeps_it() {
        for source in [
            pairs(&[("a", ""), ("a", "keep"), ("z", "v")]),
            pairs(&[("a", "keep"), ("a", ""), ("z", "v")]),
        ] {
            let mut by_name = source.clone();
            strip_empty_valued_labels(&mut by_name);
            assert_eq!(by_name, pairs(&[("z", "v")]), "by-name, from {source:?}");

            let mut pair_wise = source.clone();
            retain_non_empty_values(&mut pair_wise);
            assert_eq!(
                pair_wise,
                pairs(&[("a", "keep"), ("z", "v")]),
                "pair-wise, from {source:?}"
            );
        }
    }

    #[test]
    fn both_strips_preserve_order_and_are_noops_without_empties() {
        for strip in [
            strip_empty_valued_labels as fn(&mut Vec<(String, String)>),
            retain_non_empty_values,
        ] {
            let mut p = pairs(&[("z", "1"), ("a", "2"), ("m", "3")]);
            let before = p.clone();
            strip(&mut p);
            assert_eq!(p, before, "input order is preserved, nothing removed");

            let mut all_empty = pairs(&[("a", ""), ("b", "")]);
            strip(&mut all_empty);
            assert!(all_empty.is_empty());

            let mut none: Vec<(String, String)> = Vec::new();
            strip(&mut none);
            assert!(none.is_empty());
        }
    }
}
