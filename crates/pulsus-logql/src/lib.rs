//! LogQL parser (M1 proof subset). See docs/architecture.md §5.3.
//!
//! A hand-written two-stage lexer (`lexer.rs`, tokens carrying byte-offset
//! spans) feeds a recursive-descent parser (`parser.rs`) that produces the
//! [`Expr`] AST — the stable contract the #11 planner/SQL generator
//! consumes. The M1 subset (docs/features.md §2) covers stream selectors
//! (`=`, `!=`, `=~`, `!~`), line filters (`|=`, `!=`, `|~`, `!~`), the
//! count/bytes-only range aggregations (`rate`, `count_over_time`,
//! `bytes_rate`, `bytes_over_time`), and vector aggregations (`sum`,
//! `avg`, `min`, `max`, `count`) with `by`/`without`. Every documented but
//! unimplemented M6 construct is recognized and named in
//! [`LogQlError::NotYetSupported`] rather than failing as a generic
//! syntax error. This crate is purely syntactic: no planning, no SQL
//! generation, no query evaluation, no regex compilation — see #11.
//!
//! This crate is `pulsus-logql`'s parser layer (docs/architecture.md
//! §1.1); the planner and SQL generator that consume [`Expr`] land in
//! this same crate in #11.

mod ast;
mod duration;
mod error;
mod lexer;
mod limits;
mod parser;
mod token;
pub mod walk;

pub use ast::{
    BINARY_OP_KEYWORDS, BinModifier, BinOp, CompareOp, DropKeepElem, Duration, Expr, Grouping,
    GroupingKind, LabelExtraction, LabelFilterExpr, LabelFilterScc, LabelFmt, LabelMatch,
    LineFilter, LineFilterOp, LineMatch, LogExpr, LogRange, MatchGroup, MatchOp, Matcher, MeNode,
    MeRef, MeSlot, MeSlotNode, MeVal, MetricExpr, MetricScc, NumericLiteral, ParserStage,
    REMAINING_UNSUPPORTED_STAGES, RangeAggOp, Stage, StreamSelector, UNWRAP_CONVERSIONS, Unwrap,
    VariantsExpr, VectorAggOp, VectorMatching, for_each_label_filter, for_each_metric_expr,
};
pub use error::LogQlError;
pub use limits::{MAX_QUERY_BYTES, MAX_QUERY_SPAN_HOURS, MAX_QUERY_SPAN_NS};
pub use parser::{parse, parse_selector};
pub use token::Span;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crate_compiles() {}

    #[test]
    fn parse_and_parse_selector_are_exported_and_callable() {
        assert!(parse(r#"{app="x"}"#).is_ok());
        assert!(parse_selector(r#"{app="x"}"#).is_ok());
    }
}
