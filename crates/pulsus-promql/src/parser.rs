//! Thin re-export of the vendored, patched `promql-parser` crate (issue
//! #31, docs/decisions/0003) — the single import surface #32/#13 and this
//! crate's own `plan` module use, so no other module in this workspace
//! ever imports `promql_parser` directly.

use crate::error::PromqlError;

pub use promql_parser::label::{
    MatchOp as PLabelMatchOp, Matcher as PMatcher, Matchers as PMatchers,
};
pub use promql_parser::parser::value::ValueType;
pub use promql_parser::parser::{
    AggregateExpr, AtModifier, BinModifier, BinaryExpr, Call, DurationExpr, Expr, LabelModifier,
    MatrixSelector, NumberLiteral, Offset, ParenExpr, StringLiteral, SubqueryExpr, UnaryExpr,
    VectorMatchCardinality, VectorSelector, token,
};

/// Parses `q` into the vendored parser's AST. The `Err` case's `Display`
/// carries the parser's own error text verbatim — see [`PromqlError::Parse`].
pub fn parse(q: &str) -> Result<Expr, PromqlError> {
    promql_parser::parser::parse(q).map_err(PromqlError::Parse)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accepts_a_simple_selector() {
        assert!(parse("up").is_ok());
    }

    /// Issue #82 code review round 1 (finding 1): a deeply nested input
    /// whose only defect is an all-empty-matching selector now parses to
    /// a full AST before the deferred check (vendored PATCHES.md #6)
    /// rejects it — the rejection AND the rejected tree's destruction
    /// must be stack-safe. The vendored `parse()` walks iteratively and
    /// dismantles the rejected tree iteratively (`ast::dismantle`), so
    /// the deferred check adds no stack use beyond the generated LR
    /// parser's own pre-existing recursion bound: on a 2 MiB test-thread
    /// stack the GRAMMAR itself (instrumented, patch code never reached)
    /// overflows at 9_000 repetitions of this unit for fully VALID input
    /// (`-m-1`) exactly as for this rejected shape, and survives 8_000.
    /// This test pins that the whole deferred path (parse → walk →
    /// reject → dismantle) completes at 4_000 repetitions (~8k nested
    /// `Expr` nodes — half the measured grammar bound, kept there
    /// because the generated parser's runtime grows quadratically with
    /// depth in debug builds: 8_000 reps ≈ 12 s, 4_000 ≈ 2.4 s). `!=`
    /// rather than `=~` keeps the case fast (no per-matcher regex
    /// compilation) while still matching the empty string.
    #[test]
    fn deep_all_empty_matcher_input_is_rejected_and_destroyed_without_overflow() {
        let input = "(".to_string() + &r#"-{x!="a"}-1"#.repeat(4_000) + ")";
        let err = parse(&input).unwrap_err();
        assert!(
            err.to_string()
                .contains("vector selector must contain at least one non-empty matcher"),
            "{err}"
        );
    }

    #[test]
    fn parse_rejects_invalid_syntax_with_the_parser_own_message() {
        let err = parse("up{").unwrap_err();
        match err {
            PromqlError::Parse(msg) => assert!(!msg.is_empty()),
            other => panic!("expected Parse, got {other:?}"),
        }
    }
}
