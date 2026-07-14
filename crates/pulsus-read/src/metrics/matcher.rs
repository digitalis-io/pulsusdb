//! The issue #31 -> resolver contract. `LabelMatcher`/`MatchOp` are owned by
//! `pulsus-model` (issue #31 plan amendment §1: `pulsus-model -> pulsus-
//! promql -> pulsus-read` must stay acyclic, so the matcher type lives at
//! the bottom of that graph, not here) — re-exported so every caller in
//! this crate can keep writing `crate::metrics::LabelMatcher` without
//! reaching into `pulsus_model` directly. Under the lands-second-rebases
//! rule, issue #30 (this module) lands first; this file intentionally does
//! **not** define its own `LabelMatcher`/`MatchOp`, so #31 never needs to
//! remove a duplicate.
//!
//! `__name__` is never a [`LabelMatcher`]: every caller extracts it into its
//! own `metric_name` argument structurally (docs/schemas.md §2.1's
//! metric-scoped model), never carries it as a matcher.

pub use pulsus_model::{LabelMatcher, MatchOp};

/// The full data window a query needs answered, **including** lookback and
/// range-vector width — computed by issue #31, handed to
/// [`super::labels::SeriesResolver::resolve`]. This is the resolver's
/// correctness gate for the time-awareness invariant (docs/architecture.md
/// §5.2): a query's full data window, not just its displayed range, must
/// lie inside the cache window before the cache may answer it. Stays local
/// to `pulsus-read` (unlike `LabelMatcher`/`MatchOp`) — `pulsus-model` has
/// no reason to know about query windows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataWindow {
    pub start_ms: i64,
    pub end_ms: i64,
}

/// One `match[]` selector for the discovery endpoints (issue #32:
/// `/labels`, `/label/{name}/values`, `/series`) — deliberately looser than
/// [`pulsus_promql::SelectorSpec`]: `metric_name` is optional so a caller
/// can express "every series, unfiltered" (Prometheus's own `/labels`/
/// `/label/{name}/values` contract when `match[]` is omitted entirely,
/// docs/api.md §3.3) as the single filter `DiscoveryFilter { metric_name:
/// None, matchers: vec![] }`. `/series` requires at least one selector
/// (enforced by the caller, `pulsus-server`'s param parsing) with a
/// concrete metric name (the #31 planner's own structural contract — a
/// nameless/regex-`__name__` PromQL selector is `PromqlError::Unsupported`
/// before it ever reaches this type).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DiscoveryFilter {
    pub metric_name: Option<String>,
    pub matchers: Vec<LabelMatcher>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_window_carries_start_and_end_verbatim() {
        let w = DataWindow {
            start_ms: 10,
            end_ms: 20,
        };
        assert_eq!(w.start_ms, 10);
        assert_eq!(w.end_ms, 20);
    }

    #[test]
    fn label_matcher_and_match_op_are_the_pulsus_model_types() {
        // Structural assertion: this module must not shadow/duplicate the
        // `pulsus_model` types — it only re-exports them.
        let m: LabelMatcher = pulsus_model::LabelMatcher {
            key: "job".to_string(),
            op: MatchOp::Eq,
            value: "api".to_string(),
        };
        assert_eq!(m.op, pulsus_model::MatchOp::Eq);
    }
}
