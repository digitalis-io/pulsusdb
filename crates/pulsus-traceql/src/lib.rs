//! TraceQL parser (M4 search subset). See docs/architecture.md §5.4.
//!
//! A hand-written two-stage lexer (`lexer.rs`, tokens carrying byte-offset
//! spans) feeds a recursive-descent parser (`parser.rs`) that produces the
//! [`Query`] AST — the stable contract the T5 planner/SQL generator
//! consumes. The M4 subset (docs/features.md §4) covers scoped
//! (`span.`/`resource.`) and unscoped (`.attr`) attribute conditions, the
//! intrinsics `name`/`duration`/`status`/`kind` (`service` is *not* an
//! intrinsic — it is the `resource.service.name` attribute), all eight
//! comparison operators incl. regex, `&&`/`||` within a spanset and
//! across spansets (with parentheses), aggregate filters
//! (`count()`/`sum`/`avg`/`min`/`max`), and `select(...)`.
//!
//! Every documented-but-unimplemented construct — structural operators
//! (`>`/`>>`/`<`/`<<`/`~`), negation, arithmetic, `parent.`, bracketed
//! attributes, bare-attribute existence (M7), and the metrics pipeline
//! functions `rate`/`count_over_time`/`quantile_over_time`/
//! `histogram_over_time` (T7, via the additive [`PipelineStage`] growth
//! point) — is recognized and named in
//! [`TraceQlError::NotYetSupported`] rather than failing as a generic
//! syntax error; the frozen registry is [`BOUNDARY_CONSTRUCTS`].
//!
//! Duration literals follow the normative in-house grammar (docs/api.md
//! §4.2): an unsigned decimal number immediately followed by exactly one
//! unit from `{ns, us, µs, ms, s, m, h}` — no sign, no compound
//! literals, and fractional literals must resolve *exactly* to whole
//! nanoseconds.
//!
//! This crate is purely syntactic: no planning, no SQL generation, no
//! query evaluation, no regex compilation — the planner and SQL
//! generator that consume [`Query`] land in T5.

mod ast;
mod duration;
mod error;
mod lexer;
mod parser;
mod token;

pub use ast::{
    AggregateOp, AttrScope, BOUNDARY_CONSTRUCTS, BoolOp, ComparisonOp, Duration, Field, FieldExpr,
    Intrinsic, PipelineStage, Query, SpanKindValue, SpansetExpr, SpansetFilter, StatusValue, Value,
};
pub use error::TraceQlError;
pub use parser::parse;
pub use token::{Span, Token, TokenKind};

/// Exposed solely so the golden-corpus gate (`tests/corpus.rs`) can prove
/// every grammar-reachable [`TokenKind`] appears in at least one accept
/// case. Not a supported API surface.
#[doc(hidden)]
pub fn tokenize_for_corpus_gate(input: &str) -> Result<Vec<Token>, TraceQlError> {
    lexer::tokenize(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_is_exported_and_callable() {
        assert!(parse(r#"{ resource.service.name = "checkout" }"#).is_ok());
    }

    #[test]
    fn the_match_all_spanset_parses_to_an_empty_filter() {
        let query = parse("{}").unwrap();
        assert_eq!(
            query.spanset,
            SpansetExpr::Filter(SpansetFilter { body: None })
        );
        assert!(query.pipeline.is_empty());
    }
}
