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
}
