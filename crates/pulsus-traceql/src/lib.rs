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
//! The structural operators `>` (child), `>>` (descendant), and `~`
//! (sibling) parse to [`SpansetExpr::Structural`] (issue #172): tighter
//! than `&&`/`||`, left-associative. Every documented-but-unimplemented
//! construct — the remaining structural operators (`<`/`<<`/`>=`/`<=`),
//! negation, arithmetic, `parent.`, bracketed attributes, bare-attribute
//! existence, the deferred metrics pipeline functions
//! (`avg`/`min`/`max`/`quantile`/`histogram` `_over_time`), and metrics
//! grouping `by` (all M7) — is recognized and named in
//! [`TraceQlError::NotYetSupported`] rather than failing as a generic
//! syntax error; the frozen registry is [`BOUNDARY_CONSTRUCTS`]. The
//! committed M4 metrics functions `rate()`/`count_over_time()` parse to
//! [`PipelineStage::Metric`] (issue #59, the additive growth-point fill).
//!
//! Duration literals follow the normative in-house grammar (docs/api.md
//! §4.2): an unsigned decimal number immediately followed by exactly one
//! unit from `{ns, us, µs, ms, s, m, h}` — no sign, no compound
//! literals, and fractional literals must resolve *exactly* to whole
//! nanoseconds.
//!
//! This crate is purely syntactic: no planning, no SQL generation, no
//! query evaluation — the planner and SQL generator that consume
//! [`Query`] land in T5. The one exception is the post-parse semantic
//! pass ([`validate`], issue #328), which compiles regex literals
//! through `pulsus-re2`.

mod ast;
mod duration;
mod error;
mod lexer;
mod parser;
mod token;
mod validate;

pub use ast::{
    AggregateOp, ArithOp, AttrScope, BOUNDARY_CONSTRUCTS, BoolOp, COMPARE_DEFAULT_TOP_N,
    ComparisonOp, Duration, Field, FieldExpr, FieldOp, HintValue, INTRINSIC_KIND_VALUES,
    INTRINSIC_STATUS_VALUES, Intrinsic, MetricFn, MetricHint, MetricStage, PipelineStage, Query,
    SecondStage, SpanKindValue, SpansetExpr, SpansetFilter, StatusValue, StructuralModifier,
    StructuralOp, UNARY_BINDING_POWER, UnaryOp, Value,
};
pub use error::TraceQlError;
pub use parser::parse;
pub use token::{Span, Token, TokenKind};
pub use validate::{VALIDATE_RULES, ValidateError, validate};

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
