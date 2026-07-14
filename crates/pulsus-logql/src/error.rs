//! `LogQlError` — the taxonomy every lexer/parser failure resolves to.
//! Follows `pulsus-schema::SchemaError`'s style: `thiserror`, one variant
//! per distinct failure mode, each message carrying enough context
//! (byte offset, the exact construct name) to be actionable both in logs
//! and in the `400` query-error envelope (docs/api.md: "malformed queries
//! with parser position where available").

use thiserror::Error;

use crate::token::Span;

/// The nested-vector-aggregation recursion guard (`sum(sum(sum(...)))`).
/// Exceeding it is a parse error, never a stack overflow (architect plan:
/// "Duration & recursion as panic vectors").
pub(crate) const MAX_DEPTH: usize = 64;

/// Errors from `pulsus-logql`'s lexer and parser.
#[derive(Debug, Error)]
pub enum LogQlError {
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
    /// so callers can point at an exact byte offset even for a
    /// truncated query.
    #[error("unexpected end of query at byte {}: expected {expected}", .span.start)]
    UnexpectedEof { expected: String, span: Span },

    /// A recognized-but-unimplemented M6 construct (docs/features.md §2
    /// "LogQL — parity (M6)"). Names the construct so the caller can
    /// distinguish "not yet supported" from a genuine syntax error.
    #[error(
        "`{construct}` at byte {} is not yet supported (M1 proof subset; parity lands in M6 — features.md §2)",
        .span.start
    )]
    NotYetSupported { construct: String, span: Span },

    /// A duration literal (`5m`, `1h30m`, ...) that is malformed or whose
    /// nanosecond total overflows `u64` — never wrapped/truncated.
    #[error("invalid duration {raw:?} at byte {}: {reason}", .span.start)]
    InvalidDuration {
        raw: String,
        reason: String,
        span: Span,
    },

    /// A double-quoted or backtick string with no closing delimiter
    /// before the end of input.
    #[error("unterminated string starting at byte {}", .span.start)]
    UnterminatedString { span: Span },

    /// `{}` with zero label matchers. Match-everything selectors that
    /// *do* have a matcher (e.g. `{app=~".*"}`) are syntactically valid
    /// here — rejecting those is a planner/cost concern, deferred to #11
    /// (task-manager resolution #2).
    #[error("empty stream selector: at least one label matcher is required")]
    EmptySelector { span: Span },

    /// Nested vector aggregations exceeded [`MAX_DEPTH`] levels.
    #[error("query nesting exceeds the {MAX_DEPTH} level limit")]
    RecursionLimitExceeded { span: Span },

    /// The full expression parsed successfully but did not consume the
    /// whole input.
    #[error("unexpected trailing input at byte {}", .span.start)]
    TrailingInput { span: Span },
}

impl LogQlError {
    /// The byte-offset span every variant carries — surfaced by #13's
    /// `400 bad_data` query-error envelope as `position` (docs/api.md
    /// "Errors": "400 for malformed queries with parser position where
    /// available").
    pub fn span(&self) -> Span {
        match self {
            LogQlError::UnexpectedToken { span, .. }
            | LogQlError::UnexpectedEof { span, .. }
            | LogQlError::NotYetSupported { span, .. }
            | LogQlError::InvalidDuration { span, .. }
            | LogQlError::UnterminatedString { span }
            | LogQlError::EmptySelector { span }
            | LogQlError::RecursionLimitExceeded { span }
            | LogQlError::TrailingInput { span } => *span,
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
        let err = LogQlError::UnexpectedToken {
            found: "'}'".to_string(),
            expected: "a string".to_string(),
            span: span(),
        };
        let msg = err.to_string();
        assert!(msg.contains("byte 3"));
        assert!(msg.contains("a string"));
    }

    #[test]
    fn unexpected_eof_message_names_the_end_of_input_offset() {
        let err = LogQlError::UnexpectedEof {
            expected: "a stream selector".to_string(),
            span: Span { start: 7, end: 7 },
        };
        let msg = err.to_string();
        assert!(msg.contains("byte 7"));
        assert!(msg.contains("a stream selector"));
    }

    #[test]
    fn not_yet_supported_message_names_the_construct() {
        let err = LogQlError::NotYetSupported {
            construct: "json".to_string(),
            span: span(),
        };
        let msg = err.to_string();
        assert!(msg.contains("json"));
        assert!(msg.contains("byte 3"));
    }

    #[test]
    fn invalid_duration_message_names_the_raw_literal_and_reason() {
        let err = LogQlError::InvalidDuration {
            raw: "5x".to_string(),
            reason: "unknown unit".to_string(),
            span: span(),
        };
        let msg = err.to_string();
        assert!(msg.contains("5x"));
        assert!(msg.contains("unknown unit"));
    }

    #[test]
    fn recursion_limit_message_names_the_configured_limit() {
        let err = LogQlError::RecursionLimitExceeded { span: span() };
        assert!(err.to_string().contains(&MAX_DEPTH.to_string()));
    }

    #[test]
    fn empty_selector_message_explains_the_rule() {
        let err = LogQlError::EmptySelector { span: span() };
        assert!(err.to_string().contains("at least one label matcher"));
    }

    #[test]
    fn unterminated_string_message_names_the_offset() {
        let err = LogQlError::UnterminatedString { span: span() };
        assert!(err.to_string().contains("byte 3"));
    }

    #[test]
    fn trailing_input_message_names_the_offset() {
        let err = LogQlError::TrailingInput { span: span() };
        assert!(err.to_string().contains("byte 3"));
    }

    #[test]
    fn span_returns_the_carried_span_for_every_variant() {
        let cases = [
            LogQlError::UnexpectedToken {
                found: "x".to_string(),
                expected: "y".to_string(),
                span: span(),
            },
            LogQlError::UnexpectedEof {
                expected: "y".to_string(),
                span: span(),
            },
            LogQlError::NotYetSupported {
                construct: "json".to_string(),
                span: span(),
            },
            LogQlError::InvalidDuration {
                raw: "5x".to_string(),
                reason: "bad".to_string(),
                span: span(),
            },
            LogQlError::UnterminatedString { span: span() },
            LogQlError::EmptySelector { span: span() },
            LogQlError::RecursionLimitExceeded { span: span() },
            LogQlError::TrailingInput { span: span() },
        ];
        for case in cases {
            assert_eq!(case.span(), span());
        }
    }
}
