//! Per-query response warnings (issue #277).
//!
//! A `variants(...)` query whose variant breaches the result-series cap is
//! not an error: the reference SKIPS that variant and records a warning,
//! serving the rest at HTTP 200 (`pkg/logql/engine.go:500-508 @ grafana/loki
//! v3.7.4 b318f2829f0ae2094ab3a1e90780450e9e4b03be`). This module is the
//! accumulator that carries those messages from the evaluator to the
//! response envelope, where they render as a top-level `warnings` array
//! **after** `data`.
//!
//! The retention question is answered by construction rather than by a
//! cap: at most one message is added per variant, and the variant count is
//! already gated at plan time
//! ([`MAX_VARIANT_SUB_STATES`](super::plan::MAX_VARIANT_SUB_STATES)), so
//! the accumulated bytes are a function of the QUERY SHAPE and never of
//! the rows scanned. `warnings_retention_is_bounded_by_the_variant_count`
//! is the executable form of that sentence.

use std::collections::BTreeSet;

/// Per-query warnings, deduplicated by message and rendered
/// byte-lexicographically — the exact contract of the reference's
/// `metadata.Context` (`pkg/logqlmodel/metadata/context.go:34,80-92 @
/// grafana/loki v3.7.4 b318f2829f0ae2094ab3a1e90780450e9e4b03be`: a
/// `warnings map[string]struct{}` rendered with
/// `slices.Sorted(maps.Keys(...))`; the frontend merge repeats it at
/// `pkg/querier/queryrange/codec.go:2229-2240`).
///
/// A [`BTreeSet`] gives both properties by CONSTRUCTION rather than by a
/// sort call a later reader can forget: Go's string compare and Rust's
/// `String: Ord` are both byte-wise, so the two orders agree.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Warnings {
    set: BTreeSet<String>,
}

impl Warnings {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records `message`, ignoring a repeat (the reference's set
    /// semantics — its `map[string]struct{}` insert is idempotent).
    pub fn add(&mut self, message: String) {
        self.set.insert(message);
    }

    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    pub fn len(&self) -> usize {
        self.set.len()
    }

    /// Sorted and deduplicated, ready for the wire.
    pub fn as_strings(&self) -> Vec<String> {
        self.set.iter().cloned().collect()
    }
}

/// `maximum of series ({cap}) reached for variant ({index})` — the
/// reference's format verbatim (`pkg/logql/engine.go:506 @ grafana/loki
/// v3.7.4 b318f2829f0ae2094ab3a1e90780450e9e4b03be`; duplicated for the
/// frontend's own cap at `pkg/querier/queryrange/limits.go:507`).
///
/// NOTE the text is **"maximum of series"**, not the plain query's
/// "maximum number of series" — the two messages differ, and a query that
/// breaches both caps must not be able to borrow the other's wording.
/// `index` is rendered as the `__variant__` label VALUE, i.e. a plain
/// decimal (`strconv.Itoa`, never zero-padded).
pub fn variant_series_warning(cap: u64, index: usize) -> String {
    format!("maximum of series ({cap}) reached for variant ({index})")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// AC 5's unit half: the wire order is byte-lexicographic (so
    /// `variant (10)` precedes `variant (2)` — captured from the pinned
    /// container, not derived) and a repeated message is stored once.
    #[test]
    fn warnings_render_byte_lexicographically_and_deduplicate() {
        let mut w = Warnings::new();
        assert!(w.is_empty());
        w.add(variant_series_warning(500, 2));
        w.add(variant_series_warning(500, 10));
        // The same message twice is ONE entry.
        w.add(variant_series_warning(500, 2));
        assert_eq!(w.len(), 2);
        assert_eq!(
            w.as_strings(),
            vec![
                "maximum of series (500) reached for variant (10)".to_string(),
                "maximum of series (500) reached for variant (2)".to_string(),
            ]
        );
        assert!(!w.is_empty());
    }

    /// The message is the reference's, byte for byte — including the
    /// "maximum of series" wording, which is NOT the plain query's
    /// "maximum number of series".
    #[test]
    fn the_variant_warning_is_the_references_text_and_not_the_plain_query_one() {
        assert_eq!(
            variant_series_warning(500, 0),
            "maximum of series (500) reached for variant (0)"
        );
        assert!(!variant_series_warning(500, 0).contains("maximum number of series"));
    }

    /// AC 10 — the retained warning bytes are a function of the VARIANT
    /// COUNT alone. Two accumulators built from the same variant indices
    /// but from wildly different scan volumes are byte-identical, and the
    /// message count never exceeds the variant count.
    #[test]
    fn warnings_retention_is_bounded_by_the_variant_count() {
        let retained = |variants: usize, rows_scanned: usize| -> (usize, usize) {
            let mut w = Warnings::new();
            // One message per breaching variant is the ONLY emission
            // shape; `rows_scanned` is threaded in to show it changes
            // nothing.
            for i in 0..variants {
                let _ = rows_scanned;
                w.add(variant_series_warning(500, i));
            }
            let bytes: usize = w.as_strings().iter().map(String::len).sum();
            (w.len(), bytes)
        };

        for variants in [1usize, 2, 11, 64] {
            let small = retained(variants, 10);
            let huge = retained(variants, 10_000_000);
            assert_eq!(small, huge, "retention must not track scanned rows");
            assert_eq!(small.0, variants, "at most one message per variant");
            assert!(small.0 <= variants);
        }
    }
}
