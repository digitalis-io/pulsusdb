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

/// Whether `raw` is a valid TraceQL duration literal under the normative
/// in-house grammar (docs/api.md §4.2) — the module-doc rules verbatim,
/// including the exact-whole-nanoseconds requirement (`.5s` is valid,
/// `0.1ns` is not). This is the SINGLE SOURCE OF TRUTH for "is this
/// string a duration" outside the parser (issue #58 task-manager
/// amendment): consumers such as the tag-value type inference delegate
/// here rather than re-implementing the grammar, so no second
/// implementation exists to drift.
pub fn is_duration_literal(raw: &str) -> bool {
    duration::parse_duration(
        raw,
        Span {
            start: 0,
            end: raw.len(),
        },
    )
    .is_ok()
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

    #[test]
    fn is_duration_literal_agrees_with_the_normative_parser() {
        // Accepts: the grammar's own accept vectors, incl. the leading-dot
        // fraction (issue #58 round-2 review).
        for raw in ["2s", "100ms", "1.5s", "500µs", ".5s", "0.5s", "1h", "5m"] {
            assert!(is_duration_literal(raw), "{raw:?} must be a duration");
        }
        // Rejects: compound literals, unsupported units, inexact
        // fractional nanoseconds, missing units, signs.
        for raw in ["1h30m", "1d", "0.1ns", "5", "-1s", "", ".s", "5x"] {
            assert!(!is_duration_literal(raw), "{raw:?} must not be a duration");
        }
    }
}
