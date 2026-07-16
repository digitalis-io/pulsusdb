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

    #[test]
    fn parse_rejects_invalid_syntax_with_the_parser_own_message() {
        let err = parse("up{").unwrap_err();
        match err {
            PromqlError::Parse(msg) => assert!(!msg.is_empty()),
            other => panic!("expected Parse, got {other:?}"),
        }
    }
}
