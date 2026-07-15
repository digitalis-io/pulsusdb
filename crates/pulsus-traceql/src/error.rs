//! `TraceQlError` — the taxonomy every lexer/parser failure resolves to.
//! Mirrors `pulsus-logql`'s error module style: `thiserror`, one variant
//! per distinct failure mode, each message carrying enough context (byte
//! offset, the exact construct name) to be actionable both in logs and in
//! the `400` query-error envelope (docs/api.md: "malformed queries with
//! parser position where available").

use thiserror::Error;

use crate::token::Span;

/// The nesting recursion guard (parenthesized spanset/field expressions).
/// Exceeding it is a parse error, never a stack overflow.
pub(crate) const MAX_DEPTH: usize = 64;

/// Errors from `pulsus-traceql`'s lexer and parser.
#[derive(Debug, Error)]
pub enum TraceQlError {
    /// A concrete token was found where a different one was expected.
    #[error("unexpected {found} at byte {}: expected {expected}", .span.start)]
    UnexpectedToken {
        found: String,
        expected: String,
        span: Span,
    },

    /// The query ended where more input was required. `span` is a
    /// zero-width span at end of input (`start == end == input.len()`),
    /// the same position/rendering convention every other variant uses,
    /// so callers can point at an exact byte offset even for a truncated
    /// query.
    #[error("unexpected end of query at byte {}: expected {expected}", .span.start)]
    UnexpectedEof { expected: String, span: Span },

    /// A recognized-but-unimplemented construct outside the committed M4
    /// search subset (structural operators, negation, arithmetic,
    /// `parent.`, bracketed attributes, bare-attribute existence — M7;
    /// metrics pipeline functions — T7). Names the construct so the
    /// caller can distinguish "not yet supported" from a genuine syntax
    /// error. The full registry is [`crate::ast::BOUNDARY_CONSTRUCTS`].
    #[error(
        "`{construct}` at byte {} is not yet supported (M4 TraceQL search subset — features.md §4)",
        .span.start
    )]
    NotYetSupported { construct: String, span: Span },

    /// A duration literal (`2s`, `1.5s`, ...) that is malformed (unknown
    /// or unsupported unit) or whose nanosecond total overflows `u64` —
    /// never wrapped/truncated. The grammar is normative in docs/api.md
    /// §4.2: unsigned decimal, exactly one unit from
    /// `{ns, us, µs, ms, s, m, h}`, no sign, no compound literals.
    #[error("invalid duration {raw:?} at byte {}: {reason}", .span.start)]
    InvalidDuration {
        raw: String,
        reason: String,
        span: Span,
    },

    /// A fractional duration literal whose value is not exactly a whole
    /// number of nanoseconds (`0.1ns`, `0.0000001ms`). Exact conversion
    /// only — no rounding, no truncation: silent precision loss in a
    /// query language is a correctness bug (docs/api.md §4.2).
    #[error(
        "duration {raw:?} at byte {} does not resolve to a whole number of nanoseconds",
        .span.start
    )]
    FractionalNanoseconds { raw: String, span: Span },

    /// A double-quoted or backtick string with no closing delimiter
    /// before the end of input.
    #[error("unterminated string starting at byte {}", .span.start)]
    UnterminatedString { span: Span },

    /// Nested expressions exceeded [`MAX_DEPTH`] levels.
    #[error("query nesting exceeds the {MAX_DEPTH} level limit")]
    RecursionLimitExceeded { span: Span },

    /// The full expression parsed successfully but did not consume the
    /// whole input.
    #[error("unexpected trailing input at byte {}", .span.start)]
    TrailingInput { span: Span },
}

impl TraceQlError {
    /// The byte-offset span every variant carries — surfaced by the query
    /// endpoints' `400 bad_data` error envelope as `position` (docs/api.md
    /// "Errors").
    pub fn span(&self) -> Span {
        match self {
            TraceQlError::UnexpectedToken { span, .. }
            | TraceQlError::UnexpectedEof { span, .. }
            | TraceQlError::NotYetSupported { span, .. }
            | TraceQlError::InvalidDuration { span, .. }
            | TraceQlError::FractionalNanoseconds { span, .. }
            | TraceQlError::UnterminatedString { span }
            | TraceQlError::RecursionLimitExceeded { span }
            | TraceQlError::TrailingInput { span } => *span,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn span() -> Span {
        Span { start: 3, end: 5 }
    }

    #[test]
    fn unexpected_token_message_names_the_offset_and_expectation() {
        let err = TraceQlError::UnexpectedToken {
            found: "'}'".to_string(),
            expected: "a value".to_string(),
            span: span(),
        };
        let msg = err.to_string();
        assert!(msg.contains("byte 3"));
        assert!(msg.contains("a value"));
    }

    #[test]
    fn unexpected_eof_message_names_the_end_of_input_offset() {
        let err = TraceQlError::UnexpectedEof {
            expected: "a spanset filter".to_string(),
            span: Span { start: 7, end: 7 },
        };
        let msg = err.to_string();
        assert!(msg.contains("byte 7"));
        assert!(msg.contains("a spanset filter"));
    }

    #[test]
    fn not_yet_supported_message_names_the_construct() {
        let err = TraceQlError::NotYetSupported {
            construct: "structural operator '>>'".to_string(),
            span: span(),
        };
        let msg = err.to_string();
        assert!(msg.contains("structural operator '>>'"));
        assert!(msg.contains("byte 3"));
    }

    #[test]
    fn invalid_duration_message_names_the_raw_literal_and_reason() {
        let err = TraceQlError::InvalidDuration {
            raw: "5x".to_string(),
            reason: "unknown unit".to_string(),
            span: span(),
        };
        let msg = err.to_string();
        assert!(msg.contains("5x"));
        assert!(msg.contains("unknown unit"));
    }

    #[test]
    fn fractional_nanoseconds_message_names_the_literal_and_offset() {
        let err = TraceQlError::FractionalNanoseconds {
            raw: "0.1ns".to_string(),
            span: span(),
        };
        let msg = err.to_string();
        assert!(msg.contains("0.1ns"));
        assert!(msg.contains("byte 3"));
        assert!(msg.contains("whole number of nanoseconds"));
    }

    #[test]
    fn recursion_limit_message_names_the_configured_limit() {
        let err = TraceQlError::RecursionLimitExceeded { span: span() };
        assert!(err.to_string().contains(&MAX_DEPTH.to_string()));
    }

    #[test]
    fn unterminated_string_message_names_the_offset() {
        let err = TraceQlError::UnterminatedString { span: span() };
        assert!(err.to_string().contains("byte 3"));
    }

    #[test]
    fn trailing_input_message_names_the_offset() {
        let err = TraceQlError::TrailingInput { span: span() };
        assert!(err.to_string().contains("byte 3"));
    }

    #[test]
    fn span_returns_the_carried_span_for_every_variant() {
        let cases = [
            TraceQlError::UnexpectedToken {
                found: "x".to_string(),
                expected: "y".to_string(),
                span: span(),
            },
            TraceQlError::UnexpectedEof {
                expected: "y".to_string(),
                span: span(),
            },
            TraceQlError::NotYetSupported {
                construct: "parent scope".to_string(),
                span: span(),
            },
            TraceQlError::InvalidDuration {
                raw: "5x".to_string(),
                reason: "bad".to_string(),
                span: span(),
            },
            TraceQlError::FractionalNanoseconds {
                raw: "0.1ns".to_string(),
                span: span(),
            },
            TraceQlError::UnterminatedString { span: span() },
            TraceQlError::RecursionLimitExceeded { span: span() },
            TraceQlError::TrailingInput { span: span() },
        ];
        for case in cases {
            assert_eq!(case.span(), span());
        }
    }
}
