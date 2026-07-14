//! Label matchers shared across the metrics read path (issue #30's label
//! cache, issue #31's PromQL planner). Pure data, no dependencies: this
//! type lives in `pulsus-model` — rather than `pulsus-read` or
//! `pulsus-promql` — specifically so both crates can depend on it without a
//! cycle (`pulsus-model -> pulsus-promql -> pulsus-read`, issue #31 plan
//! amendment §1). `__name__` is never represented as a [`LabelMatcher`]
//! here: the metric name is extracted structurally into its own
//! `metric_name` argument by every caller (docs/schemas.md §2.1's
//! metric-scoped model), never carried as a matcher.

/// One label matcher's comparison operator (PromQL `=`, `!=`, `=~`, `!~`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchOp {
    Eq,
    Neq,
    Re,
    Nre,
}

/// A single label matcher: `key <op> value`. `value` is a literal for
/// [`MatchOp::Eq`]/[`MatchOp::Neq`], a regex pattern for
/// [`MatchOp::Re`]/[`MatchOp::Nre`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelMatcher {
    pub key: String,
    pub op: MatchOp,
    pub value: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_matcher_carries_key_op_value_verbatim() {
        let m = LabelMatcher {
            key: "job".to_string(),
            op: MatchOp::Eq,
            value: "api".to_string(),
        };
        assert_eq!(m.key, "job");
        assert_eq!(m.op, MatchOp::Eq);
        assert_eq!(m.value, "api");
    }

    #[test]
    fn match_op_variants_are_distinct() {
        assert_ne!(MatchOp::Eq, MatchOp::Neq);
        assert_ne!(MatchOp::Re, MatchOp::Nre);
        assert_ne!(MatchOp::Eq, MatchOp::Re);
    }
}
