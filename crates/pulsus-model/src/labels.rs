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
/// Use this — NOT [`resolve_structured_metadata`] — wherever a STREAM LABEL
/// set is built. The two differ whenever a name repeats or renames onto
/// another, which the reference resolves differently at its two seams; see
/// [`resolve_structured_metadata`] for the discriminating measurements.
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

/// Resolves one log entry's **structured metadata** the way the reference
/// resolves it: Prometheus' `labels.Builder`, driven by Loki's distributor
/// over the entry's pairs (`pkg/distributor/distributor.go:697-722 @ v3.7.4`
/// over
/// `vendor/github.com/prometheus/prometheus/model/labels/labels_stringlabels.go:454-521`
/// and `labels_common.go:163-200`). Input is the entry's pairs in WIRE order
/// under their RAW names; output is pairs whose names are
/// [`canonicalize_label_key`] fixed points and unique, so the caller's
/// [`LabelSet::from_normalized`] never reaches its own collision branch.
///
/// Use this — NOT [`retain_non_empty_values`] — wherever a STRUCTURED
/// METADATA set is built. Order is not part of the contract: the sole
/// consumer sorts, so the slow path's ascending order is a by-product of the
/// merge and the fast path returns wire order untouched.
///
/// # The builder, primitive by primitive
///
/// 1. `base` is the entry's pairs as `labels.Labels`: sorted by name with
///    **duplicates preserved** (`logproto.FromLabelAdaptersToLabels`,
///    `pkg/logproto/compat.go:59-86`, over `ScratchBuilder.Add`/`Sort`/
///    `Labels`, `labels_stringlabels.go:614-645` — `Add`'s own doc says a
///    repeated name yields a duplicate label).
/// 2. `NewBuilder` calls `Reset`, which records the RAW name of every
///    empty-valued base label in `del` (`distributor.go:700` ->
///    `labels_stringlabels.go:471-480` — the file actually compiled: it is
///    `//go:build !slicelabels && !dedupelabels`, and Loki builds with
///    `-tags netgo` alone, `Makefile:54-64 @ v3.7.4`).
/// 3. The loop **renames**: iff the normalized name differs from the raw one
///    it runs `Del(raw)` then `Set(normalized, value)`
///    (`distributor.go:707-710`).
/// 4. The loop **also** `Set`s when the value contains `utf8.RuneError`, with
///    every U+FFFD mapped to a space (`distributor.go:714-715`,
///    `removeInvalidUtf` at `:75-80`). Both branches can fire for one pair;
///    the second overwrites the first, as in Go.
/// 5. `Set(n, "")` is `Del(n)`; `Del` also removes `n` from `add`; `Set` does
///    not remove `n` from `del` (`labels_common.go:163-200`).
/// 6. `Labels()` merges (`distributor.go:722` ->
///    `labels_stringlabels.go:483-521`): a base entry whose name is in `del`
///    is dropped, an `add` entry **replaces** the FIRST base entry of the
///    same name, remaining `add` entries are inserted in sorted position.
/// 7. Consequence of 5 and 6, spelled out because it is the corner every
///    prose DESCRIPTION of this function has got wrong: **`del` drops BASE
///    entries only, so a `Set` outranks it.** An empty value deletes every
///    pair stored under its name *that the builder did not `Set`* — a rename
///    or a U+FFFD rewrite re-adds the name, in either wire order, because
///    `add` is emitted whether or not `del` holds that name. Measured on
///    `grafana/loki:3.7.4` (`b318f282`): `{a_b="", a_b="p\u{FFFD}"}` stores
///    `a_b="p "` and so does the reverse order, while the U+FFFD-free control
///    `{a_b="", a_b="p"}` stores nothing. Rows `g01`-`g05` of
///    `the_builder_emits_a_set_name_even_when_reset_deleted_it`.
///
/// So the tie-break is two-tier, not positional: **a pair that was `Set`
/// (renamed, or carrying U+FFFD) beats a pair that was not, wherever either
/// sits in wire order; among pairs `Set` onto one name the last wins; among
/// pairs never `Set` the reference keeps them all as duplicate labels.** That
/// is neither [`LabelSet::from_normalized`]'s greatest-original-key rule nor
/// plain last-write-wins over the wire list, and it is what makes both orders
/// of `{a.b="x", a_b="keep"}` store `a_b="x"` (issue #381).
///
/// Duplicate names, which the reference's `Labels()` can emit and our
/// key-unique `log_samples.structured_metadata` column cannot hold, collapse
/// **keeping the last**: the reference marshals its duplicates into a JSON
/// object (`pkg/loghttp/entry.go:233-244`), so a JSON consumer observes the
/// last one. That choice reproduces every measured duplicate row below.
///
/// # One function, because it is one builder
///
/// The empty-value rule of issue #259 is not a second rule folded in for
/// adjacency: `Reset` + `Del` and `Set` are primitives of the same builder,
/// and the model above derives all ten of that issue's measured rows —
/// asserted, unchanged, by
/// `loki_push::a_normalized_metadata_name_collision_with_one_empty_follows_the_references_builder`
/// and its JSON twin.
///
/// # Measured
///
/// Every row below was pushed as structured metadata to
/// `grafana/loki:3.7.4` (`buildinfo` `revision: b318f282`) and read back with
/// `X-Loki-Response-Encoding-Flags: categorize-labels`. The `stored` column
/// is the container's answer; each is asserted here by
/// the unit test `structured_metadata_resolution_is_the_references_builder`,
/// and
/// against a committed capture of the raw response bodies by
/// `pulsus-write/tests/structured_metadata_collisions.rs`.
///
/// | id | pushed, in this order | stored |
/// |---|---|---|
/// | c01 | `a.b=x`, `a_b=keep` | `a_b=x` — the renamed pair replaces the base twin |
/// | c02 | `a_b=keep`, `a.b=x` | `a_b=x` — same, wire order does not decide |
/// | c03 | `a.b=1`, `a-b=2` | `a_b=2` — both renamed, last `Set` wins |
/// | c04 | `a-b=2`, `a.b=1` | `a_b=1` — …in either order |
/// | c05 | `a_b=1`, `a_b=2` | `a_b=2` — neither `Set`: two base duplicates, last observed |
/// | c06 | `a_b=2`, `a_b=1` | `a_b=1` — …in either order |
/// | c07 | `a_b=1`, `a_b=2`, `a.b=9` | `a_b=2` — the `Set` replaces the FIRST duplicate only |
/// | c08 | `a.b=1`, `a_b=2`, `a.b=3` | `a_b=3` |
/// | c09 | `a.b=1`, `a_b=2`, `a-b=3` | `a_b=3` |
/// | c10 | `a-b=3`, `a_b=2`, `a.b=1` | `a_b=1` |
/// | c11 | `a.b=1`, `a.b=2` | `a_b=2` |
/// | c16 | `a_b=""`, `a.b=x`, `a-b=y` | `a_b=y` — `Reset` deletes the base name, both renames re-add |
/// | c17 | `a.b=x`, `a_b=keep`, `z=1` | `a_b=x`, `z=1` |
/// | c18 | `a.b=9`, `a_b=1`, `a_b=2` | `a_b=2` |
/// | f01 | `a.b=x`, `a_b=p\u{FFFD}` | `a_b="p "` — the U+FFFD `Set` outranks the rename |
/// | f02 | `a_b=p\u{FFFD}`, `a.b=x` | `a_b=x` — …and loses to a LATER `Set` |
/// | f03 | `a_b=p\u{FFFD}q` | `a_b="p q"` — a lone value rewrite, no collision |
/// | f04 | `a_b=1`, `a_b=p\u{FFFD}` | *the push is a 204 whose read is a 500* |
///
/// f04 is where `Labels()` emits TWO `a_b` entries — `"p "` from `add`, then
/// the untouched base duplicate — and the reference's own read path then
/// fails: `failed to parse series labels to categorize labels: 1:6: parse
/// error: invalid UTF-8 rune`. There is no consumer-observable reference
/// value, so ours is a choice (keep-last, `a_b="p\u{FFFD}"`), recorded as a
/// residual in docs/benchmarks/logs-differential-ledger.md.
///
/// # Residuals
///
/// - **`normalized` here is [`canonicalize_label_key`], not the reference's
///   `LabelNamer.Build`.** The rule is then statable against our own data —
///   the name a pair is actually STORED under — and the two agree wherever the
///   two renamings agree. Where they do not (`a..b` and `a__b` normalize to
///   `a_b` there and to `a__b` here; `9bad` gains a `key_` prefix there) the
///   collision GROUPS differ: measured, `{a..b="", a_b="keep"}` stores nothing
///   there and `a_b="keep"` here. That is the label-RENAMING divergence
///   already registered in docs/api.md §8.2 (issue #259), not a second rule;
///   see `protocols::label_name`'s
///   `sanitize_differs_from_our_storage_canonicalization`.
/// - **Go's sort is unstable.** `base` is ordered by `slices.SortFunc`
///   (`ScratchBuilder.Sort`, `labels_stringlabels.go:627-629`), which is
///   insertion sort up to 12 elements and pdqsort above, so when one canonical
///   name is repeated AND a rename lands on it the reference's own answer is
///   unspecified past 12 pairs in the entry. This function sorts stably; the
///   measured boundary is in the ledger.
///
/// Only an exactly-empty value is dropped; a whitespace-only value is a value
/// (`{"a":" "}` round-trips as `a=" "`) and nothing is trimmed.
///
/// A caller that has already canonicalized its keys (the OTLP scope path)
/// passes fixed points, for which no pair is renamed, `add` carries only the
/// U+FFFD rewrites, and the builder degenerates to a by-name delete plus
/// keep-last — which is the reference's own shape there, because its OTLP
/// translation runs `LabelNamer.Build` over every attribute key before the
/// distributor sees it (`pkg/loghttp/push/otlp.go:602-614 @ v3.7.4`).
pub fn resolve_structured_metadata(pairs: Vec<(String, String)>) -> Vec<(String, String)> {
    // Identity fast path, borrowed and allocation-light: the overwhelmingly
    // common entry (`trace_id`, `span_id`, …) has non-empty values, no
    // U+FFFD, canonical names and no repeat. `del` and `add` are then both
    // empty, so `Labels()` returns `base` — the same pairs, modulo an order
    // the caller re-derives anyway. A name is a `canonicalize_label_key`
    // fixed point exactly when every char is in its allow-list
    // (`canonical.rs:26-36`), which costs no allocation to test.
    let is_identity = {
        let mut seen: BTreeSet<&str> = BTreeSet::new();
        pairs.iter().all(|(name, value)| {
            !value.is_empty()
                && !value.contains('\u{FFFD}')
                && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
                && seen.insert(name.as_str())
        })
    };
    if is_identity {
        return pairs;
    }

    let canon: Vec<String> = pairs
        .iter()
        .map(|(name, _)| canonicalize_label_key(name))
        .collect();
    // `Reset`: the RAW name of every empty-valued base label.
    let mut deleted: BTreeSet<String> = pairs
        .iter()
        .filter(|(_, value)| value.is_empty())
        .map(|(name, _)| name.clone())
        .collect();
    // `add`. Go keeps an unordered slice and sorts it just before the merge;
    // a `BTreeMap` has the same `Set`/`Del` semantics (unique names, last
    // write overwrites) and hands the merge its sorted iteration for free.
    let mut added: BTreeMap<String, String> = BTreeMap::new();
    for (index, (raw, value)) in pairs.iter().enumerate() {
        let normalized = &canon[index];
        if normalized != raw {
            // `Del(raw)`. Its `add` half is provably a no-op here — every
            // `add` key is a fixed point and `raw` is not, or it would equal
            // `normalized` — but the delete half is what lets a rename
            // suppress its own base entry.
            added.remove(raw);
            deleted.insert(raw.clone());
            set(&mut added, &mut deleted, normalized, value.clone());
        }
        if value.contains('\u{FFFD}') {
            set(
                &mut added,
                &mut deleted,
                normalized,
                value.replace('\u{FFFD}', " "),
            );
        }
    }

    // `Labels()`: merge the sorted base with the sorted `add`. The sort is
    // stable, so equal names keep wire order (Go's is not — see the residual
    // in this function's docs).
    let mut order: Vec<usize> = (0..pairs.len()).collect();
    order.sort_by(|&a, &b| pairs[a].0.cmp(&pairs[b].0));
    let mut merged: Vec<(String, String)> = Vec::with_capacity(pairs.len() + added.len());
    let mut adds = added.into_iter().peekable();
    for index in order {
        let name = &pairs[index].0;
        if deleted.contains(name) {
            continue;
        }
        while adds.peek().is_some_and(|(n, _)| n < name) {
            merged.push(adds.next().expect("peeked"));
        }
        if adds.peek().is_some_and(|(n, _)| n == name) {
            // This base entry is REPLACED — and only this one, so a second
            // base entry of the same name survives as a duplicate (row c07).
            merged.push(adds.next().expect("peeked"));
            continue;
        }
        merged.push(pairs[index].clone());
    }
    merged.extend(adds);

    // The reference can emit duplicate names; our column is key-unique, and a
    // JSON consumer of the reference's object observes the last. `merged` is
    // sorted, so the duplicates are adjacent: one linear pass, keeping the
    // last of each run.
    merged.dedup_by(|later, earlier| {
        if later.0 == earlier.0 {
            std::mem::swap(later, earlier);
            true
        } else {
            false
        }
    });
    merged
}

/// The builder's `Set`, whose empty-value case is `Del`
/// (`labels_common.go:187-192 @ v3.7.4`).
fn set(
    added: &mut BTreeMap<String, String>,
    deleted: &mut BTreeSet<String>,
    name: &str,
    value: String,
) {
    if value.is_empty() {
        added.remove(name);
        deleted.insert(name.to_string());
    } else {
        added.insert(name.to_string(), value);
    }
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

    // -- the structured-metadata builder (issues #259, #381) ---------------

    /// The pair list `resolve_structured_metadata` returns, sorted, so a row's
    /// expectation reads as the set the caller will store.
    fn resolved(items: &[(&str, &str)]) -> Vec<(String, String)> {
        let mut out = resolve_structured_metadata(pairs(items));
        out.sort();
        out
    }

    #[test]
    fn both_rules_remove_only_exactly_empty_values() {
        // Neither trims: a whitespace-only value is a value.
        let source = pairs(&[("a", ""), ("b", "v"), ("c", " "), ("d", "\t"), ("e", "0")]);
        let expected = pairs(&[("b", "v"), ("c", " "), ("d", "\t"), ("e", "0")]);

        assert_eq!(
            resolved(&[("a", ""), ("b", "v"), ("c", " "), ("d", "\t"), ("e", "0")]),
            expected
        );

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
    fn the_builder_takes_the_non_empty_twin_where_the_pair_wise_strip_keeps_it() {
        for source in [
            [("a", ""), ("a", "keep"), ("z", "v")],
            [("a", "keep"), ("a", ""), ("z", "v")],
        ] {
            assert_eq!(
                resolved(&source),
                pairs(&[("z", "v")]),
                "builder, from {source:?}"
            );

            let mut pair_wise = pairs(&source);
            retain_non_empty_values(&mut pair_wise);
            assert_eq!(
                pair_wise,
                pairs(&[("a", "keep"), ("z", "v")]),
                "pair-wise, from {source:?}"
            );
        }
    }

    /// **The reference's builder, row by row.** Every case in
    /// [`resolve_structured_metadata`]'s doc table, with the expectation
    /// equal to the container's answer.
    ///
    /// Measured this session on `grafana/loki:3.7.4` (`buildinfo` reports
    /// `version 3.7.4`, `revision b318f282`), each row pushed as JSON
    /// structured metadata to `/loki/api/v1/push` — duplicate object keys
    /// emitted by hand, which the reference keeps as raw pairs
    /// (`pkg/loghttp/query.go:181-196 @ v3.7.4`) — and read back through
    /// `/loki/api/v1/query_range` with
    /// `X-Loki-Response-Encoding-Flags: categorize-labels`. The raw response
    /// bodies are committed at
    /// `pulsus-write/tests/fixtures/structured_metadata_collisions/capture.json`,
    /// which `structured_metadata_collisions.rs` re-derives these answers
    /// from and re-captures in CI; the rows below are the same measurement in
    /// the form this function's own callers see.
    ///
    /// The `#259` rows are the same builder and live in the same table for
    /// that reason (`e01`-`e10`): `Reset`, `Del` and `Set` are primitives of
    /// one function, not two rules that happen to be adjacent.
    #[test]
    fn structured_metadata_resolution_is_the_references_builder() {
        for (id, source, expected) in [
            // -- issue #381: which pair wins ------------------------------
            (
                "c01",
                &[("a.b", "x"), ("a_b", "keep")][..],
                &[("a_b", "x")][..],
            ),
            ("c02", &[("a_b", "keep"), ("a.b", "x")], &[("a_b", "x")]),
            ("c03", &[("a.b", "1"), ("a-b", "2")], &[("a_b", "2")]),
            ("c04", &[("a-b", "2"), ("a.b", "1")], &[("a_b", "1")]),
            ("c05", &[("a_b", "1"), ("a_b", "2")], &[("a_b", "2")]),
            ("c06", &[("a_b", "2"), ("a_b", "1")], &[("a_b", "1")]),
            (
                "c07",
                &[("a_b", "1"), ("a_b", "2"), ("a.b", "9")],
                &[("a_b", "2")],
            ),
            (
                "c08",
                &[("a.b", "1"), ("a_b", "2"), ("a.b", "3")],
                &[("a_b", "3")],
            ),
            (
                "c09",
                &[("a.b", "1"), ("a_b", "2"), ("a-b", "3")],
                &[("a_b", "3")],
            ),
            (
                "c10",
                &[("a-b", "3"), ("a_b", "2"), ("a.b", "1")],
                &[("a_b", "1")],
            ),
            ("c11", &[("a.b", "1"), ("a.b", "2")], &[("a_b", "2")]),
            (
                "c16",
                &[("a_b", ""), ("a.b", "x"), ("a-b", "y")],
                &[("a_b", "y")],
            ),
            (
                "c17",
                &[("a.b", "x"), ("a_b", "keep"), ("z", "1")],
                &[("a_b", "x"), ("z", "1")],
            ),
            (
                "c18",
                &[("a.b", "9"), ("a_b", "1"), ("a_b", "2")],
                &[("a_b", "2")],
            ),
            // -- issue #381: the U+FFFD `Set`, which decides f01/f02 ------
            (
                "f01",
                &[("a.b", "x"), ("a_b", "p\u{FFFD}")],
                &[("a_b", "p ")],
            ),
            (
                "f02",
                &[("a_b", "p\u{FFFD}"), ("a.b", "x")],
                &[("a_b", "x")],
            ),
            ("f03", &[("a_b", "p\u{FFFD}q")], &[("a_b", "p q")]),
            // -- issue #259: the same builder's `Reset`/`Del` half ---------
            ("e01", &[("a.b", ""), ("a_b", "keep")], &[]),
            ("e02", &[("a_b", "keep"), ("a.b", "")], &[]),
            ("e03", &[("a.b", "keep"), ("a_b", "")], &[("a_b", "keep")]),
            ("e04", &[("a_b", ""), ("a.b", "keep")], &[("a_b", "keep")]),
            ("e05", &[("a.b", ""), ("a.b", "keep")], &[("a_b", "keep")]),
            ("e06", &[("a.b", "keep"), ("a.b", "")], &[]),
            ("e07", &[("a.b", ""), ("c", "v")], &[("c", "v")]),
            (
                "e08",
                &[("a.b", "x"), ("a_b", ""), ("a_b", "keep")],
                &[("a_b", "x")],
            ),
            ("e09", &[("a", ""), ("a", "keep")], &[]),
            ("e10", &[("a", "keep"), ("a", "")], &[]),
        ] {
            assert_eq!(resolved(source), pairs(expected), "{id}: from {source:?}");
        }
    }

    /// **`del` drops base entries only, so a `Set` outranks it** — primitive
    /// 7 of [`resolve_structured_metadata`]'s docs, and the corner that makes
    /// "an empty value removes every pair stored under that name" false as
    /// stated. `Reset` seeds `del` from the base scan, a U+FFFD rewrite
    /// writes into `add`, and `Labels()` emits `add` whether or not `del`
    /// holds the name — so the rewritten pair survives, in either wire order.
    ///
    /// Every row measured this session on `grafana/loki:3.7.4` (`buildinfo`
    /// `3.7.4` / `b318f282`, `ci/logql/config.yaml`), pushed as JSON
    /// structured metadata with duplicate object keys emitted as text and
    /// read back with `X-Loki-Response-Encoding-Flags: categorize-labels`.
    /// `g02`/`g04` are the U+FFFD-free controls: without the `Set` the delete
    /// stands, which is what makes the other three discriminating rather than
    /// vacuous. `g03`/`g04` are the pair list the OTLP scope path builds for
    /// an empty-valued `scope_name` attribute beside a real scope name — the
    /// case `otlp_logs`'s
    /// `a_u_fffd_bearing_scope_name_survives_an_empty_valued_attribute_of_the_same_name`
    /// asserts end to end.
    #[test]
    fn the_builder_emits_a_set_name_even_when_reset_deleted_it() {
        for (id, source, expected) in [
            (
                "g01",
                &[("a_b", ""), ("a_b", "p\u{FFFD}")][..],
                &[("a_b", "p ")][..],
            ),
            ("g02", &[("a_b", ""), ("a_b", "p")], &[]),
            (
                "g03",
                &[("scope_name", ""), ("scope_name", "N\u{FFFD}")],
                &[("scope_name", "N ")],
            ),
            ("g04", &[("scope_name", ""), ("scope_name", "N")], &[]),
            (
                "g05",
                &[("a_b", "p\u{FFFD}"), ("a_b", "")],
                &[("a_b", "p ")],
            ),
        ] {
            assert_eq!(resolved(source), pairs(expected), "{id}: from {source:?}");
        }
    }

    /// Residual B of issue #381: `{a_b:"1", a_b:"p\u{FFFD}"}` is a push the
    /// reference accepts (204) and then cannot read back — its `Labels()`
    /// emits TWO `a_b` entries, the `add` rewrite `"p "` and the untouched
    /// base duplicate, and its own read path answers HTTP 500 `failed to
    /// parse series labels to categorize labels: 1:6: parse error: invalid
    /// UTF-8 rune` (measured, with and without the categorize header).
    ///
    /// There is no consumer-observable reference value, so this is OUR
    /// choice, asserted as such: the duplicate collapse keeps the last, which
    /// is the untouched base pair. Recorded in
    /// docs/benchmarks/logs-differential-ledger.md.
    #[test]
    fn a_duplicate_the_reference_cannot_serve_collapses_to_the_last_pair() {
        assert_eq!(
            resolved(&[("a_b", "1"), ("a_b", "p\u{FFFD}")]),
            pairs(&[("a_b", "p\u{FFFD}")])
        );
    }

    /// Residual A of issue #381, pinned on OUR side so the divergence has a
    /// stated boundary rather than a vague one. `base` is ordered by Go's
    /// `slices.SortFunc` (`ScratchBuilder.Sort`,
    /// `labels_stringlabels.go:627-629 @ v3.7.4`), which is insertion sort up
    /// to 12 elements and pdqsort above, so with one canonical name repeated
    /// AND a rename landing on it the reference's own answer stops being a
    /// function of wire order once the entry carries 13 pairs.
    ///
    /// Measured on `grafana/loki:3.7.4` (`b318f282`) with `k` copies of `a_b`
    /// followed by `a.b="REN"`: the container returns the LAST wire copy for
    /// k=3,5,8,11 (12 pairs or fewer) and `a_b="2"` for k=12,13,15,20 (13
    /// pairs or more). With no rename present it returns the last wire copy
    /// at k=13,20,40.
    ///
    /// This function sorts stably, so it returns the last wire copy at every
    /// k — asserted here at both sides of the boundary. It therefore agrees
    /// with the reference on every shape except "repeated canonical name + a
    /// rename onto it + at least 13 pairs", where the reference's own answer
    /// is unspecified. Recorded in
    /// docs/benchmarks/logs-differential-ledger.md.
    #[test]
    fn the_stable_sort_returns_the_last_wire_copy_on_both_sides_of_the_boundary() {
        for k in [11usize, 12] {
            let mut input: Vec<(String, String)> = (1..=k)
                .map(|i| ("a_b".to_string(), i.to_string()))
                .collect();
            input.push(("a.b".to_string(), "REN".to_string()));
            assert_eq!(
                resolve_structured_metadata(input),
                pairs(&[("a_b", &k.to_string())]),
                "k={k}: the last wire copy must win whatever the entry's width"
            );
        }
    }

    /// The residual of the label-RENAMING divergence registered in
    /// docs/api.md §8.2, pinned so it stays visible: the reference groups
    /// `a..b`, `a__b` and `a_b` under one name (`LabelNamer.Build` collapses
    /// consecutive invalid runes) while PulsusDB stores them as three, so the
    /// collision groups differently. Measured on `grafana/loki:3.7.4`:
    /// `{a..b="", a_b="keep"}` and `{a__b="", a_b="keep"}` both store NOTHING
    /// there, and `{9bad="", key_9bad="keep"}` likewise (`9bad` gains a
    /// `key_` prefix there). This is one divergence, not two: fixing the
    /// renaming fixes these rows with no change to the rule above.
    #[test]
    fn the_builder_groups_by_our_renaming_not_the_references() {
        for source in [
            [("a..b", ""), ("a_b", "keep")],
            [("a__b", ""), ("a_b", "keep")],
            [("9bad", ""), ("key_9bad", "keep")],
        ] {
            assert_eq!(
                resolve_structured_metadata(pairs(&source)).len(),
                1,
                "the non-empty twin survives here and does not on the reference: {source:?}"
            );
        }
    }

    #[test]
    fn the_builder_is_a_noop_without_empties_renames_or_repeats() {
        let p = pairs(&[("z", "1"), ("a", "2"), ("m", "3")]);
        assert_eq!(
            resolve_structured_metadata(p.clone()),
            p,
            "the identity fast path returns the input untouched, wire order included"
        );
        assert!(resolve_structured_metadata(pairs(&[("a", ""), ("b", "")])).is_empty());
        assert!(resolve_structured_metadata(Vec::new()).is_empty());
    }

    /// A naive, independently written transcription of `labels.Builder` as
    /// the distributor drives it (`pkg/distributor/distributor.go:697-722 @
    /// v3.7.4`) — `base` as a sorted list of pairs, `del` and `add` as plain
    /// `Vec`s with linear scans, and `Labels()` as the literal merge — with
    /// **no** fast path and no `BTreeMap`. Used only by
    /// [`the_fast_path_is_the_builders_identity_case`].
    fn naive_builder(input: &[(String, String)]) -> Vec<(String, String)> {
        // `Labels()` merges over a name-sorted base with duplicates kept.
        let mut base: Vec<(String, String)> = input.to_vec();
        base.sort_by(|a, b| a.0.cmp(&b.0));
        // `Reset`.
        let mut del: Vec<String> = base
            .iter()
            .filter(|(_, v)| v.is_empty())
            .map(|(n, _)| n.clone())
            .collect();
        let mut add: Vec<(String, String)> = Vec::new();
        fn set(add: &mut Vec<(String, String)>, del: &mut Vec<String>, n: &str, v: String) {
            if v.is_empty() {
                add.retain(|(an, _)| an != n);
                del.push(n.to_string());
            } else if let Some(slot) = add.iter_mut().find(|(an, _)| an == n) {
                slot.1 = v;
            } else {
                add.push((n.to_string(), v));
            }
        }
        for (raw, value) in input {
            let normalized = canonicalize_label_key(raw);
            if normalized != *raw {
                add.retain(|(an, _)| an != raw);
                del.push(raw.clone());
                set(&mut add, &mut del, &normalized, value.clone());
            }
            if value.contains('\u{FFFD}') {
                set(
                    &mut add,
                    &mut del,
                    &normalized,
                    value.replace('\u{FFFD}', " "),
                );
            }
        }
        add.sort_by(|a, b| a.0.cmp(&b.0));
        let mut out: Vec<(String, String)> = Vec::new();
        let mut a = 0usize;
        for (name, value) in &base {
            if del.iter().any(|d| d == name) {
                continue;
            }
            while a < add.len() && add[a].0 < *name {
                out.push(add[a].clone());
                a += 1;
            }
            if a < add.len() && add[a].0 == *name {
                out.push(add[a].clone());
                a += 1;
                continue;
            }
            out.push((name.clone(), value.clone()));
        }
        while a < add.len() {
            out.push(add[a].clone());
            a += 1;
        }
        // Duplicate names collapse keeping the LAST, as a JSON consumer of
        // the reference's duplicate-keyed object observes.
        let mut collapsed: Vec<(String, String)> = Vec::new();
        for pair in out {
            if let Some(slot) = collapsed.iter_mut().find(|(n, _)| *n == pair.0) {
                slot.1 = pair.1;
            } else {
                collapsed.push(pair);
            }
        }
        collapsed
    }

    /// The fast path is the builder's identity case, and the optimized merge
    /// is the naive one — over the exhaustive enumeration of every sequence
    /// of length 1..=3 drawn from four names x four values. The names cover a
    /// canonical fixed point, two distinct raw names renaming onto it and an
    /// unrelated key; the values cover two distinct non-empty ones, the empty
    /// one and a U+FFFD carrier, so every branch of both implementations is
    /// crossed with every other.
    ///
    /// Order is not part of the contract (the sole consumer sorts), so both
    /// sides are compared sorted.
    #[test]
    fn the_fast_path_is_the_builders_identity_case() {
        const NAMES: [&str; 4] = ["a_b", "a.b", "a-b", "c"];
        const VALUES: [&str; 4] = ["1", "2", "", "p\u{FFFD}"];
        let alphabet: Vec<(String, String)> = NAMES
            .iter()
            .flat_map(|n| VALUES.iter().map(move |v| (n.to_string(), v.to_string())))
            .collect();
        assert_eq!(alphabet.len(), 16);

        let mut cases = 0usize;
        let mut took_fast_path = 0usize;
        let mut sequences: Vec<Vec<(String, String)>> = vec![Vec::new()];
        for _ in 0..3 {
            sequences = sequences
                .iter()
                .flat_map(|prefix| {
                    alphabet.iter().map(move |pair| {
                        let mut next = prefix.clone();
                        next.push(pair.clone());
                        next
                    })
                })
                .collect();
            for input in &sequences {
                cases += 1;
                let mut ours = resolve_structured_metadata(input.clone());
                if ours == *input {
                    took_fast_path += 1;
                }
                ours.sort();
                let mut theirs = naive_builder(input);
                theirs.sort();
                assert_eq!(ours, theirs, "from {input:?}");
            }
        }
        // Asserted as a literal so a silent shrink of the enumeration fails:
        // 16 + 16^2 + 16^3.
        assert_eq!(cases, 4368, "the enumeration has shrunk");
        // Non-vacuity, and exact: only the fast path can return the input
        // itself. Every other branch shortens the list (an empty value or a
        // repeat) or rewrites a name or value, so `ours == *input` holds for
        // exactly the fast-path members — the sequences of DISTINCT canonical
        // names (`a_b`, `c`) over the two non-empty non-U+FFFD values. Length
        // 1: 2 names x 2 values = 4. Length 2: 2 orders x 2 x 2 values = 8.
        // Length 3: no third distinct canonical name, so none.
        assert_eq!(
            took_fast_path, 12,
            "the fast path was taken by a different set of cases than the enumeration implies"
        );
    }
}
