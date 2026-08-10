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
///
/// **#272 does not remove this constant.** It converts the AST/plan
/// walks (`Debug`, `Clone`, `PartialEq`, `Hash`, `Display`, drop, and
/// the planner/executor walks) to iterative form; the PARSER's own
/// recursive descent is untouched, so this guard is still the only thing
/// bounding it. **#256 owns its removal**, and cannot simply raise it to
/// the query-text cap: 131,071 bytes of `(` is roughly 65,000 parser
/// recursion levels, far past any stack — the parser must be converted
/// or frame-bounded first.
pub(crate) const MAX_DEPTH: usize = 64;

/// The label-filter *parenthesis*-nesting recursion guard
/// (`| ((((x="1"))))`) — issue #255. Bounds paren-driven parse depth ONLY:
/// a flat `or`/`and` chain builds a left-deep tree at parse depth 1 and is
/// not bounded by this counter (surviving vector, issue #272).
///
/// Derived, not chosen: the binding measured abort threshold is a debug
/// build on tokio's default 2 MiB worker stack under the deepest legal
/// metric prefix (`sum(`x63) — 364 levels OK, 365 aborts, re-measured
/// with this guard's `depth` parameter in place; `364 / 4 = 91` — divided
/// by a safety factor of 4. That factor is in LEVELS; the accepted worst
/// case consumes 1,029 KiB of the 2 MiB stack (1.99x), because the metric
/// prefix alone costs a fixed 694 KiB.
///
/// **#272 does not remove this constant either.** It is a PARSER depth
/// guard, and #272 converts the AST/plan walks rather than the parser.
/// **#256 owns its removal**, under the same arithmetic recorded on
/// [`MAX_DEPTH`].
pub(crate) const LABEL_FILTER_MAX_DEPTH: usize = 91;

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

    /// An escape sequence the reference's string grammar does not define
    /// (`\d`, `\w`, `\q`, `\'`), or a malformed one it does (`\x8"`,
    /// `\400`, `\U00110000`) — issue #400. `escape` is the offending
    /// SOURCE text, from the backslash to wherever the scan stopped, so
    /// the message names what the user has to change.
    ///
    /// The reference raises this at its LEXER
    /// (`vendor/github.com/prometheus/prometheus/util/strutil/quote.go:66-231
    /// @ v3.7.4`, called from `pkg/logql/syntax/lex.go:198`), before any
    /// regex parser, so it applies to every construct carrying a string.
    /// **Only the `400` status is claimed against it, not the message
    /// text** — owner ruling on #246 (2026-07-26, 2026-08-08).
    #[error("invalid char escape {escape:?} at byte {}", .span.start)]
    InvalidCharEscape { escape: String, span: Span },

    /// A string literal whose decoded BYTES are not valid UTF-8 — the
    /// only way to reach it is a `\xHH`/`\NNN` escape above `0x7F` that
    /// no neighbouring escape completes (`"\xff"`), since Go's byte
    /// escapes are bytes and `"\xc3\xa9"` composes to `é`.
    ///
    /// **A deliberate narrowing** (issue #400, owner ruling 2026-08-10),
    /// ledgered as `logql-string-escape-non-utf8`: the reference serves
    /// such a pattern at its five `NewFastRegexMatcher` positions. It is
    /// unreachable as a MATCH here regardless, because no mounted ingest
    /// route can store invalid UTF-8 (`LogRow.body: String`,
    /// `pulsus-write/src/protocols/otlp_logs.rs:37-55`).
    #[error("string literal at byte {} decodes to bytes that are not valid UTF-8", .span.start)]
    NonUtf8StringLiteral { span: Span },

    /// `{}` with zero label matchers. Match-everything selectors that
    /// *do* have a matcher (e.g. `{app=~".*"}`) are syntactically valid
    /// here — rejecting those is a planner/cost concern, deferred to #11
    /// (task-manager resolution #2).
    #[error("empty stream selector: at least one label matcher is required")]
    EmptySelector { span: Span },

    /// Nested vector aggregations exceeded [`MAX_DEPTH`] levels, or
    /// label-filter parenthesis nesting exceeded
    /// [`LABEL_FILTER_MAX_DEPTH`] levels (issue #255). `limit` is the
    /// guard that fired, so the message names the right number.
    #[error("query nesting exceeds the {limit} level limit")]
    RecursionLimitExceeded { span: Span, limit: usize },

    /// The full expression parsed successfully but did not consume the
    /// whole input.
    #[error("unexpected trailing input at byte {}", .span.start)]
    TrailingInput { span: Span },

    /// A `topk`/`bottomk`/`approx_topk` `k` that is not an integer literal
    /// (issue #221; reference: `strconv.Atoi` in
    /// `mustNewVectorAggregationExpr`, pkg/logql/syntax/ast.go). The
    /// reference's message text is embedded VERBATIM (including its
    /// trailing `(raw,` fragment) so a differential substring gate is
    /// shared; PulsusDB appends its house `at byte N` position suffix.
    #[error("invalid parameter {op}({raw}, at byte {}", .span.start)]
    InvalidAggregationParam { op: String, raw: String, span: Span },

    /// The same `k`, integral but `<= 0` (issue #221;
    /// pkg/logql/syntax/ast.go — reference text verbatim, position suffix
    /// appended).
    #[error("invalid parameter (must be greater than 0) {op}({raw} at byte {}", .span.start)]
    AggregationParamNotPositive { op: String, raw: String, span: Span },

    /// `approx_topk` with a `by`/`without` clause (issue #221;
    /// pkg/logql/syntax/ast.go — reference text verbatim, position suffix
    /// appended).
    #[error("grouping not allowed for {op} aggregation at byte {}", .span.start)]
    GroupingNotAllowed { op: String, span: Span },

    /// The query source exceeded [`crate::MAX_QUERY_BYTES`]. The inner text
    /// is the reference's verbatim (grafana/loki v3.7.4
    /// `pkg/logql/syntax/parser.go:87`), the same treatment
    /// `InvalidAggregationParam` and its siblings already get. At exactly
    /// the cap this renders `(131072 > 131072)` — the reference's own
    /// artifact of printing `len > cap` while comparing `>=`, not a defect.
    /// No `at byte N` suffix (the reference reports line=col=0, and
    /// `EmptySelector`/`RecursionLimitExceeded` set the same house
    /// precedent); `span` is zero-width at 0 so the envelope's `position`
    /// stays well-formed.
    #[error("input size too long ({len} > {cap})")]
    QueryTooLong { len: usize, cap: usize, span: Span },

    /// An `offset` or `[range]` duration literal longer than
    /// [`crate::MAX_QUERY_SPAN_NS`] (issue #343, owner mandate). `what` is
    /// `"offset"` or `"range"`; `raw` is the literal AS WRITTEN — sign
    /// included, in the units the user sent — so the message shows what
    /// tripped it.
    ///
    /// Same shape as [`LogQlError::QueryTooLong`] (`X too long (got >
    /// cap)`), the same `400 bad_data`, and the cap is DERIVED from the
    /// constant rather than spelled again so the two cannot drift.
    #[error("{what} too long ({raw} > {cap_hours}h)")]
    SpanTooLong {
        what: &'static str,
        raw: String,
        cap_hours: i64,
        span: Span,
    },
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
            | LogQlError::InvalidCharEscape { span, .. }
            | LogQlError::NonUtf8StringLiteral { span }
            | LogQlError::EmptySelector { span }
            | LogQlError::RecursionLimitExceeded { span, .. }
            | LogQlError::TrailingInput { span }
            | LogQlError::InvalidAggregationParam { span, .. }
            | LogQlError::AggregationParamNotPositive { span, .. }
            | LogQlError::GroupingNotAllowed { span, .. }
            | LogQlError::QueryTooLong { span, .. }
            | LogQlError::SpanTooLong { span, .. } => *span,
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
        let err = LogQlError::RecursionLimitExceeded {
            span: span(),
            limit: MAX_DEPTH,
        };
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

    /// Issue #221: the non-integer-`k` message CONTAINS the reference's
    /// verbatim text (`invalid parameter approx_topk(2.5,` — including the
    /// trailing comma) plus the house position suffix.
    #[test]
    fn invalid_aggregation_param_message_embeds_the_reference_text() {
        let err = LogQlError::InvalidAggregationParam {
            op: "approx_topk".to_string(),
            raw: "2.5".to_string(),
            span: span(),
        };
        let msg = err.to_string();
        assert!(msg.contains("invalid parameter approx_topk(2.5,"), "{msg}");
        assert!(msg.contains("byte 3"), "{msg}");
    }

    /// Issue #221: the non-positive-`k` message CONTAINS the reference's
    /// verbatim text (`invalid parameter (must be greater than 0) topk(0`
    /// — no trailing comma) plus the house position suffix.
    #[test]
    fn aggregation_param_not_positive_message_embeds_the_reference_text() {
        let err = LogQlError::AggregationParamNotPositive {
            op: "topk".to_string(),
            raw: "0".to_string(),
            span: span(),
        };
        let msg = err.to_string();
        assert!(
            msg.contains("invalid parameter (must be greater than 0) topk(0"),
            "{msg}"
        );
        assert!(msg.contains("byte 3"), "{msg}");
    }

    /// Issue #221: the `approx_topk` grouping rejection CONTAINS the
    /// reference's verbatim text plus the house position suffix.
    #[test]
    fn grouping_not_allowed_message_embeds_the_reference_text() {
        let err = LogQlError::GroupingNotAllowed {
            op: "approx_topk".to_string(),
            span: span(),
        };
        let msg = err.to_string();
        assert!(
            msg.contains("grouping not allowed for approx_topk aggregation"),
            "{msg}"
        );
        assert!(msg.contains("byte 3"), "{msg}");
    }

    /// Issue #279: the over-cap message is the reference's verbatim
    /// `input size too long (<len> > <cap>)` (grafana/loki v3.7.4
    /// `pkg/logql/syntax/parser.go:87`) with NO `at byte N` suffix — the
    /// reference reports line=col=0 for this rejection.
    #[test]
    fn query_too_long_message_embeds_the_reference_text_with_no_position_suffix() {
        let err = LogQlError::QueryTooLong {
            len: 131_072,
            cap: 131_072,
            span: Span { start: 0, end: 0 },
        };
        let msg = err.to_string();
        assert_eq!(msg, "input size too long (131072 > 131072)");
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
            LogQlError::RecursionLimitExceeded {
                span: span(),
                limit: MAX_DEPTH,
            },
            LogQlError::TrailingInput { span: span() },
            LogQlError::InvalidAggregationParam {
                op: "topk".to_string(),
                raw: "2.5".to_string(),
                span: span(),
            },
            LogQlError::AggregationParamNotPositive {
                op: "topk".to_string(),
                raw: "0".to_string(),
                span: span(),
            },
            LogQlError::GroupingNotAllowed {
                op: "approx_topk".to_string(),
                span: span(),
            },
            LogQlError::QueryTooLong {
                len: 131_072,
                cap: 131_072,
                span: span(),
            },
        ];
        for case in cases {
            assert_eq!(case.span(), span());
        }
    }
}
