//! Recursive-descent parser over `&[Token]`. A `Cursor` tracks the
//! current position; two counters guard against unbounded nesting, both
//! resolving to `RecursionLimitExceeded` at [`crate::error::MAX_DEPTH`]:
//!
//! - a by-value `depth` counter threaded through the parenthesized
//!   spanset/field productions (bounds *parse-time* recursion), and
//! - a by-`&mut` `binary_nodes` counter incremented for every `&&`/`||`
//!   node at both expression levels (bounds the *constructed AST*, so a
//!   paren-free 100k-operand chain errors cleanly instead of building a
//!   boxed spine that would overflow the stack in `Display`/`Drop`).
//!
//! Together they cap any root-to-leaf AST path at under `2 × MAX_DEPTH`
//! (128) nested nodes, so the derived recursive `Debug`/`Display`/`Drop`
//! implementations are stack-safe by construction — no iterative `Drop`
//! is needed.
//!
//! Grammar (plan v2 F1 / v3 F5):
//!
//! ```text
//! Query          := SpansetExpr ("|" PipelineStage)*
//! SpansetExpr    := SpansetAnd ("||" SpansetAnd)*
//! SpansetAnd     := SpansetPrimary ("&&" SpansetPrimary)*
//! SpansetPrimary := SpansetFilter | "(" SpansetExpr ")"
//! SpansetFilter  := "{" FieldExpr? "}"
//! FieldExpr      := FieldAnd ("||" FieldAnd)*
//! FieldAnd       := FieldPrimary ("&&" FieldPrimary)*
//! FieldPrimary   := "(" FieldExpr ")" | Field CmpOp Value
//! PipelineStage  := "count" "(" ")" CmpOp Value
//!                 | ("avg"|"sum"|"min"|"max") "(" AggField ")" CmpOp Value
//!                 | "select" "(" Field { "," Field } ")"
//! ```
//!
//! Disambiguation of the dual-role `>`/`>=`/`<`/`<=` tokens (comparison
//! inside a field expression, structural operator between spansets) is
//! purely positional: field-level comparisons are fully consumed before
//! the closing `}`, so the spanset combination position only ever sees
//! `&&`/`||`/`|`/structural/EOF — the LogQL `!=` disambiguation
//! precedent.

use crate::ast::{
    self, AggregateOp, AttrScope, BoolOp, ComparisonOp, Field, FieldExpr, Intrinsic, MetricFn,
    PipelineStage, Query, SpanKindValue, SpansetExpr, SpansetFilter, StatusValue, Value,
};
use crate::duration;
use crate::error::{MAX_DEPTH, TraceQlError};
use crate::lexer;
use crate::token::{Span, Token, TokenKind};

/// Parses a full TraceQL search query into a [`Query`] — the T5 planner
/// contract.
pub fn parse(input: &str) -> Result<Query, TraceQlError> {
    let tokens = lexer::tokenize(input)?;
    let mut cursor = Cursor::new(&tokens);
    let mut binary_nodes = 0usize;
    let spanset = parse_spanset_expr(&mut cursor, 0, &mut binary_nodes)?;
    let mut pipeline = Vec::new();
    while matches!(cursor.peek().kind, TokenKind::Pipe) {
        cursor.advance();
        pipeline.push(parse_pipeline_stage(&mut cursor)?);
    }
    expect_eof(&cursor)?;
    Ok(Query { spanset, pipeline })
}

/// Charges one `&&`/`||` node against the query-wide binary-node budget
/// (shared across the spanset and field levels). `span` is the
/// operator's span, so an over-limit chain errors at the exact operator
/// that exceeded it.
fn charge_binary_node(binary_nodes: &mut usize, span: Span) -> Result<(), TraceQlError> {
    *binary_nodes += 1;
    if *binary_nodes >= MAX_DEPTH {
        Err(TraceQlError::RecursionLimitExceeded { span })
    } else {
        Ok(())
    }
}

fn expect_eof(cursor: &Cursor<'_>) -> Result<(), TraceQlError> {
    let tok = cursor.peek();
    if matches!(tok.kind, TokenKind::Eof) {
        Ok(())
    } else {
        Err(TraceQlError::TrailingInput { span: tok.span })
    }
}

/// A read-only cursor over the token stream. Tokens always end with
/// `Eof`, so `peek`/`peek_at` never index out of bounds.
struct Cursor<'a> {
    tokens: &'a [Token],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(tokens: &'a [Token]) -> Self {
        Cursor { tokens, pos: 0 }
    }

    fn peek_at(&self, ahead: usize) -> &Token {
        let idx = (self.pos + ahead).min(self.tokens.len() - 1);
        &self.tokens[idx]
    }

    fn peek(&self) -> &Token {
        self.peek_at(0)
    }

    fn peek2(&self) -> &Token {
        self.peek_at(1)
    }

    fn advance(&mut self) -> Token {
        let tok = self.peek().clone();
        if self.pos + 1 < self.tokens.len() {
            self.pos += 1;
        }
        tok
    }

    /// Consumes the current token if its kind matches `want` (payload
    /// ignored — this is only used for payload-free token kinds).
    fn expect(&mut self, want: &TokenKind, expected: &str) -> Result<Token, TraceQlError> {
        let tok = self.peek().clone();
        if std::mem::discriminant(&tok.kind) == std::mem::discriminant(want) {
            self.advance();
            Ok(tok)
        } else if matches!(tok.kind, TokenKind::Eof) {
            Err(TraceQlError::UnexpectedEof {
                expected: expected.to_string(),
                span: tok.span,
            })
        } else {
            Err(TraceQlError::UnexpectedToken {
                found: describe(&tok.kind),
                expected: expected.to_string(),
                span: tok.span,
            })
        }
    }

    fn expect_ident(&mut self, expected: &str) -> Result<(String, Span), TraceQlError> {
        let tok = self.peek().clone();
        match tok.kind {
            TokenKind::Ident(name) => {
                self.advance();
                Ok((name, tok.span))
            }
            TokenKind::Eof => Err(TraceQlError::UnexpectedEof {
                expected: expected.to_string(),
                span: tok.span,
            }),
            _ => Err(TraceQlError::UnexpectedToken {
                found: describe(&tok.kind),
                expected: expected.to_string(),
                span: tok.span,
            }),
        }
    }
}

/// A short human-readable description of a token for error messages.
fn describe(kind: &TokenKind) -> String {
    match kind {
        TokenKind::LBrace => "'{'".to_string(),
        TokenKind::RBrace => "'}'".to_string(),
        TokenKind::LParen => "'('".to_string(),
        TokenKind::RParen => "')'".to_string(),
        TokenKind::LBracket => "'['".to_string(),
        TokenKind::RBracket => "']'".to_string(),
        TokenKind::Comma => "','".to_string(),
        TokenKind::Dot => "'.'".to_string(),
        TokenKind::Eq => "'='".to_string(),
        TokenKind::Neq => "'!='".to_string(),
        TokenKind::Re => "'=~'".to_string(),
        TokenKind::Nre => "'!~'".to_string(),
        TokenKind::Gt => "'>'".to_string(),
        TokenKind::Gte => "'>='".to_string(),
        TokenKind::Lt => "'<'".to_string(),
        TokenKind::Lte => "'<='".to_string(),
        TokenKind::AndAnd => "'&&'".to_string(),
        TokenKind::OrOr => "'||'".to_string(),
        TokenKind::Pipe => "'|'".to_string(),
        TokenKind::Shr => "'>>'".to_string(),
        TokenKind::Shl => "'<<'".to_string(),
        TokenKind::Tilde => "'~'".to_string(),
        TokenKind::Bang => "'!'".to_string(),
        TokenKind::Amp => "'&'".to_string(),
        TokenKind::Plus => "'+'".to_string(),
        TokenKind::Minus => "'-'".to_string(),
        TokenKind::Star => "'*'".to_string(),
        TokenKind::Slash => "'/'".to_string(),
        TokenKind::Ident(s) => format!("identifier {s:?}"),
        TokenKind::String(s) => format!("string {s:?}"),
        TokenKind::Duration(s) => format!("duration {s:?}"),
        TokenKind::Number(s) => format!("number {s:?}"),
        TokenKind::Eof => "end of query".to_string(),
    }
}

/// `SpansetExpr := SpansetAnd ("||" SpansetAnd)*` — left-associative;
/// `&&` binds tighter than `||` at the spanset level, mirroring the
/// field level.
fn parse_spanset_expr(
    cursor: &mut Cursor<'_>,
    depth: usize,
    binary_nodes: &mut usize,
) -> Result<SpansetExpr, TraceQlError> {
    if depth >= MAX_DEPTH {
        return Err(TraceQlError::RecursionLimitExceeded {
            span: cursor.peek().span,
        });
    }
    let mut lhs = parse_spanset_and(cursor, depth, binary_nodes)?;
    while matches!(cursor.peek().kind, TokenKind::OrOr) {
        charge_binary_node(binary_nodes, cursor.peek().span)?;
        cursor.advance();
        let rhs = parse_spanset_and(cursor, depth, binary_nodes)?;
        lhs = SpansetExpr::Binary {
            op: BoolOp::Or,
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
        };
    }
    Ok(lhs)
}

fn parse_spanset_and(
    cursor: &mut Cursor<'_>,
    depth: usize,
    binary_nodes: &mut usize,
) -> Result<SpansetExpr, TraceQlError> {
    let mut lhs = parse_spanset_primary(cursor, depth, binary_nodes)?;
    loop {
        check_no_structural_op(cursor)?;
        if matches!(cursor.peek().kind, TokenKind::AndAnd) {
            charge_binary_node(binary_nodes, cursor.peek().span)?;
            cursor.advance();
            let rhs = parse_spanset_primary(cursor, depth, binary_nodes)?;
            lhs = SpansetExpr::Binary {
                op: BoolOp::And,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        } else {
            return Ok(lhs);
        }
    }
}

/// After a complete spanset operand, checks whether the next token is a
/// structural operator — valid Tempo, out of the committed M4 surface
/// (docs/features.md §4: "Structural operators are M7") — and names it.
/// This runs after *every* operand, so structural operators are caught
/// both at the top level and inside parentheses.
fn check_no_structural_op(cursor: &Cursor<'_>) -> Result<(), TraceQlError> {
    let tok = cursor.peek();
    let construct = match &tok.kind {
        TokenKind::Gt => "structural operator '>'",
        TokenKind::Shr => "structural operator '>>'",
        TokenKind::Lt => "structural operator '<'",
        TokenKind::Shl => "structural operator '<<'",
        TokenKind::Gte => "structural operator '>='",
        TokenKind::Lte => "structural operator '<='",
        TokenKind::Tilde => "structural operator '~'",
        TokenKind::Bang => "negation operator '!'",
        _ => return Ok(()),
    };
    Err(TraceQlError::NotYetSupported {
        construct: construct.to_string(),
        span: tok.span,
    })
}

/// `SpansetPrimary := SpansetFilter | "(" SpansetExpr ")"` — the paren
/// production plan v2 F1 added so `Display`'s full parenthesization
/// round-trips. Parens are structural only: no AST node.
fn parse_spanset_primary(
    cursor: &mut Cursor<'_>,
    depth: usize,
    binary_nodes: &mut usize,
) -> Result<SpansetExpr, TraceQlError> {
    let tok = cursor.peek().clone();
    match tok.kind {
        TokenKind::LBrace => Ok(SpansetExpr::Filter(parse_spanset_filter(
            cursor,
            depth,
            binary_nodes,
        )?)),
        TokenKind::LParen => {
            cursor.advance();
            let expr = parse_spanset_expr(cursor, depth + 1, binary_nodes)?;
            cursor.expect(&TokenKind::RParen, "')'")?;
            Ok(expr)
        }
        TokenKind::Bang => Err(TraceQlError::NotYetSupported {
            construct: "negation operator '!'".to_string(),
            span: tok.span,
        }),
        TokenKind::Eof => Err(TraceQlError::UnexpectedEof {
            expected: "a spanset filter ('{') or '('".to_string(),
            span: tok.span,
        }),
        _ => Err(TraceQlError::UnexpectedToken {
            found: describe(&tok.kind),
            expected: "a spanset filter ('{') or '('".to_string(),
            span: tok.span,
        }),
    }
}

/// `SpansetFilter := "{" FieldExpr? "}"` — `{}` is the MatchAll node
/// (task-manager adjudication 3).
fn parse_spanset_filter(
    cursor: &mut Cursor<'_>,
    depth: usize,
    binary_nodes: &mut usize,
) -> Result<SpansetFilter, TraceQlError> {
    cursor.expect(&TokenKind::LBrace, "'{'")?;
    if matches!(cursor.peek().kind, TokenKind::RBrace) {
        cursor.advance();
        return Ok(SpansetFilter { body: None });
    }
    let body = parse_field_expr(cursor, depth, binary_nodes)?;
    cursor.expect(&TokenKind::RBrace, "'}'")?;
    Ok(SpansetFilter { body: Some(body) })
}

/// `FieldExpr := FieldAnd ("||" FieldAnd)*` — `&&` binds tighter than
/// `||`, both left-associative.
fn parse_field_expr(
    cursor: &mut Cursor<'_>,
    depth: usize,
    binary_nodes: &mut usize,
) -> Result<FieldExpr, TraceQlError> {
    if depth >= MAX_DEPTH {
        return Err(TraceQlError::RecursionLimitExceeded {
            span: cursor.peek().span,
        });
    }
    let mut lhs = parse_field_and(cursor, depth, binary_nodes)?;
    while matches!(cursor.peek().kind, TokenKind::OrOr) {
        charge_binary_node(binary_nodes, cursor.peek().span)?;
        cursor.advance();
        let rhs = parse_field_and(cursor, depth, binary_nodes)?;
        lhs = FieldExpr::Binary {
            op: BoolOp::Or,
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
        };
    }
    Ok(lhs)
}

fn parse_field_and(
    cursor: &mut Cursor<'_>,
    depth: usize,
    binary_nodes: &mut usize,
) -> Result<FieldExpr, TraceQlError> {
    let mut lhs = parse_field_primary(cursor, depth, binary_nodes)?;
    loop {
        check_no_arithmetic_op(cursor)?;
        if matches!(cursor.peek().kind, TokenKind::AndAnd) {
            charge_binary_node(binary_nodes, cursor.peek().span)?;
            cursor.advance();
            let rhs = parse_field_primary(cursor, depth, binary_nodes)?;
            lhs = FieldExpr::Binary {
                op: BoolOp::And,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        } else {
            return Ok(lhs);
        }
    }
}

/// After a complete field-expression operand, checks whether the next
/// token starts an arithmetic expression — valid Tempo, out of M4 — and
/// names the operator.
fn check_no_arithmetic_op(cursor: &Cursor<'_>) -> Result<(), TraceQlError> {
    let tok = cursor.peek();
    let construct = match &tok.kind {
        TokenKind::Plus => "arithmetic operator '+'",
        TokenKind::Minus => "arithmetic operator '-'",
        TokenKind::Star => "arithmetic operator '*'",
        TokenKind::Slash => "arithmetic operator '/'",
        _ => return Ok(()),
    };
    Err(TraceQlError::NotYetSupported {
        construct: construct.to_string(),
        span: tok.span,
    })
}

/// `FieldPrimary := "(" FieldExpr ")" | Field CmpOp Value`. An
/// *attribute* with no comparison (`{ .foo }`) is valid Tempo (attribute
/// existence) but out of the M4 comparison-only surface →
/// `NotYetSupported` at the field's span (task-manager adjudication 3).
/// A bare *intrinsic* (`{ name }`) is not a future construct — it is
/// malformed grammar in every milestone, so it gets a plain positioned
/// missing-comparison error (round-2 adjudication 1, same rationale as
/// bare `parent`).
const COMPARISON_EXPECTED: &str =
    "a comparison operator ('=', '!=', '>', '>=', '<', '<=', '=~', '!~')";

fn parse_field_primary(
    cursor: &mut Cursor<'_>,
    depth: usize,
    binary_nodes: &mut usize,
) -> Result<FieldExpr, TraceQlError> {
    let tok = cursor.peek().clone();
    match tok.kind {
        TokenKind::LParen => {
            cursor.advance();
            let expr = parse_field_expr(cursor, depth + 1, binary_nodes)?;
            cursor.expect(&TokenKind::RParen, "')'")?;
            Ok(expr)
        }
        TokenKind::Bang => Err(TraceQlError::NotYetSupported {
            construct: "negation operator '!'".to_string(),
            span: tok.span,
        }),
        _ => {
            let (field, field_span) = parse_field(cursor)?;
            let op = match &cursor.peek().kind {
                TokenKind::Eq => ComparisonOp::Eq,
                TokenKind::Neq => ComparisonOp::Neq,
                TokenKind::Gt => ComparisonOp::Gt,
                TokenKind::Gte => ComparisonOp::Gte,
                TokenKind::Lt => ComparisonOp::Lt,
                TokenKind::Lte => ComparisonOp::Lte,
                TokenKind::Re => ComparisonOp::Re,
                TokenKind::Nre => ComparisonOp::Nre,
                TokenKind::RBrace
                | TokenKind::AndAnd
                | TokenKind::OrOr
                | TokenKind::RParen
                | TokenKind::Eof
                    if matches!(field, Field::Attribute { .. }) =>
                {
                    return Err(TraceQlError::NotYetSupported {
                        construct: "bare attribute expression".to_string(),
                        span: field_span,
                    });
                }
                TokenKind::Eof => {
                    return Err(TraceQlError::UnexpectedEof {
                        expected: COMPARISON_EXPECTED.to_string(),
                        span: cursor.peek().span,
                    });
                }
                other => {
                    let span = cursor.peek().span;
                    return Err(TraceQlError::UnexpectedToken {
                        found: describe(other),
                        expected: COMPARISON_EXPECTED.to_string(),
                        span,
                    });
                }
            };
            cursor.advance();
            let value = parse_value(cursor, &field)?;
            Ok(FieldExpr::Comparison { field, op, value })
        }
    }
}

/// `Field := Intrinsic | ("span"|"resource") "." DottedKey | "." DottedKey`.
/// A bare intrinsic keyword not followed by `.` resolves to the
/// intrinsic; `parent.` and bracketed attributes are recognized-but-M7;
/// a bare non-intrinsic word is an error (attributes must be scoped or
/// use the leading-`.` unscoped form). Returns the field plus its full
/// byte span.
fn parse_field(cursor: &mut Cursor<'_>) -> Result<(Field, Span), TraceQlError> {
    let tok = cursor.peek().clone();
    match &tok.kind {
        TokenKind::Dot => {
            cursor.advance();
            let (key, end) = parse_dotted_key(cursor)?;
            Ok((
                Field::Attribute {
                    scope: AttrScope::Unscoped,
                    key,
                },
                Span {
                    start: tok.span.start,
                    end,
                },
            ))
        }
        TokenKind::LBracket => Err(TraceQlError::NotYetSupported {
            construct: "bracketed attribute".to_string(),
            span: tok.span,
        }),
        TokenKind::Ident(name) => {
            let followed_by_dot = matches!(cursor.peek2().kind, TokenKind::Dot);
            // Only the `parent.` scope *syntax* is the recognized M7
            // construct; a bare `parent` is an ordinary unknown word and
            // falls through to the plain positioned error below.
            if name == "parent" && followed_by_dot {
                return Err(TraceQlError::NotYetSupported {
                    construct: "parent scope".to_string(),
                    span: tok.span,
                });
            }
            if (name == "span" || name == "resource") && followed_by_dot {
                let scope = if name == "span" {
                    AttrScope::Span
                } else {
                    AttrScope::Resource
                };
                cursor.advance(); // scope ident
                cursor.advance(); // '.'
                let (key, end) = parse_dotted_key(cursor)?;
                return Ok((
                    Field::Attribute { scope, key },
                    Span {
                        start: tok.span.start,
                        end,
                    },
                ));
            }
            if let Some(intrinsic) = Intrinsic::from_ident(name)
                && !followed_by_dot
            {
                cursor.advance();
                return Ok((Field::Intrinsic(intrinsic), tok.span));
            }
            Err(TraceQlError::UnexpectedToken {
                found: describe(&tok.kind),
                expected: "an intrinsic (name, duration, status, kind) or a scoped attribute \
                           (span., resource., or the unscoped . form)"
                    .to_string(),
                span: tok.span,
            })
        }
        TokenKind::Eof => Err(TraceQlError::UnexpectedEof {
            expected: "a field (intrinsic or attribute)".to_string(),
            span: tok.span,
        }),
        _ => Err(TraceQlError::UnexpectedToken {
            found: describe(&tok.kind),
            expected: "a field (intrinsic or attribute)".to_string(),
            span: tok.span,
        }),
    }
}

/// Parses the dotted key after a scope prefix: `Ident ("." Ident)*`,
/// e.g. `http.status_code`. Returns the joined key and the byte offset
/// just past its last segment. A `[` here is the bracketed-attribute
/// form — recognized, M7.
fn parse_dotted_key(cursor: &mut Cursor<'_>) -> Result<(String, usize), TraceQlError> {
    if matches!(cursor.peek().kind, TokenKind::LBracket) {
        return Err(TraceQlError::NotYetSupported {
            construct: "bracketed attribute".to_string(),
            span: cursor.peek().span,
        });
    }
    let (first, first_span) = cursor.expect_ident("an attribute name")?;
    let mut key = first;
    let mut end = first_span.end;
    while matches!(cursor.peek().kind, TokenKind::Dot)
        && matches!(cursor.peek2().kind, TokenKind::Ident(_))
    {
        cursor.advance(); // '.'
        let (segment, span) = cursor.expect_ident("an attribute name")?;
        key.push('.');
        key.push_str(&segment);
        end = span.end;
    }
    Ok((key, end))
}

/// Field-typed value parsing (plan v2 F4): the closed `status`/`kind`
/// keyword sets are enforced here with a position, `duration` requires a
/// duration literal (a bare number has no unit), `name` requires a
/// string, and attributes accept string/number/boolean/duration.
fn parse_value(cursor: &mut Cursor<'_>, field: &Field) -> Result<Value, TraceQlError> {
    match field {
        Field::Intrinsic(Intrinsic::Status) => {
            const EXPECTED: &str = "a status ('ok', 'error', or 'unset')";
            let tok = cursor.peek().clone();
            match &tok.kind {
                TokenKind::Ident(name) => match StatusValue::from_ident(name) {
                    Some(status) => {
                        cursor.advance();
                        Ok(Value::Status(status))
                    }
                    None => Err(TraceQlError::UnexpectedToken {
                        found: describe(&tok.kind),
                        expected: EXPECTED.to_string(),
                        span: tok.span,
                    }),
                },
                TokenKind::Eof => Err(TraceQlError::UnexpectedEof {
                    expected: EXPECTED.to_string(),
                    span: tok.span,
                }),
                _ => Err(TraceQlError::UnexpectedToken {
                    found: describe(&tok.kind),
                    expected: EXPECTED.to_string(),
                    span: tok.span,
                }),
            }
        }
        Field::Intrinsic(Intrinsic::Kind) => {
            const EXPECTED: &str =
                "a span kind ('internal', 'server', 'client', 'producer', or 'consumer')";
            let tok = cursor.peek().clone();
            match &tok.kind {
                TokenKind::Ident(name) => match SpanKindValue::from_ident(name) {
                    Some(kind) => {
                        cursor.advance();
                        Ok(Value::Kind(kind))
                    }
                    None => Err(TraceQlError::UnexpectedToken {
                        found: describe(&tok.kind),
                        expected: EXPECTED.to_string(),
                        span: tok.span,
                    }),
                },
                TokenKind::Eof => Err(TraceQlError::UnexpectedEof {
                    expected: EXPECTED.to_string(),
                    span: tok.span,
                }),
                _ => Err(TraceQlError::UnexpectedToken {
                    found: describe(&tok.kind),
                    expected: EXPECTED.to_string(),
                    span: tok.span,
                }),
            }
        }
        Field::Intrinsic(Intrinsic::Duration) => {
            let tok = cursor.peek().clone();
            match &tok.kind {
                TokenKind::Duration(raw) => {
                    cursor.advance();
                    Ok(Value::Duration(duration::parse_duration(raw, tok.span)?))
                }
                TokenKind::Number(_) => Err(TraceQlError::UnexpectedToken {
                    found: describe(&tok.kind),
                    expected: "a duration with a unit (e.g. 2s, 100ms)".to_string(),
                    span: tok.span,
                }),
                TokenKind::Eof => Err(TraceQlError::UnexpectedEof {
                    expected: "a duration literal (e.g. 2s, 100ms)".to_string(),
                    span: tok.span,
                }),
                _ => Err(TraceQlError::UnexpectedToken {
                    found: describe(&tok.kind),
                    expected: "a duration literal (e.g. 2s, 100ms)".to_string(),
                    span: tok.span,
                }),
            }
        }
        Field::Intrinsic(Intrinsic::Name) => {
            let tok = cursor.peek().clone();
            match tok.kind {
                TokenKind::String(value) => {
                    cursor.advance();
                    Ok(Value::String(value))
                }
                TokenKind::Eof => Err(TraceQlError::UnexpectedEof {
                    expected: "a string".to_string(),
                    span: tok.span,
                }),
                _ => Err(TraceQlError::UnexpectedToken {
                    found: describe(&tok.kind),
                    expected: "a string".to_string(),
                    span: tok.span,
                }),
            }
        }
        Field::Attribute { .. } => {
            const EXPECTED: &str = "a value (string, number, boolean, or duration)";
            let tok = cursor.peek().clone();
            match &tok.kind {
                TokenKind::String(value) => {
                    let value = value.clone();
                    cursor.advance();
                    Ok(Value::String(value))
                }
                TokenKind::Number(raw) => {
                    let raw = raw.clone();
                    cursor.advance();
                    Ok(Value::Number(raw))
                }
                TokenKind::Duration(raw) => {
                    let parsed = duration::parse_duration(raw, tok.span)?;
                    cursor.advance();
                    Ok(Value::Duration(parsed))
                }
                TokenKind::Ident(name) => match name.as_str() {
                    "true" => {
                        cursor.advance();
                        Ok(Value::Bool(true))
                    }
                    "false" => {
                        cursor.advance();
                        Ok(Value::Bool(false))
                    }
                    _ => Err(TraceQlError::UnexpectedToken {
                        found: describe(&tok.kind),
                        expected: EXPECTED.to_string(),
                        span: tok.span,
                    }),
                },
                TokenKind::Eof => Err(TraceQlError::UnexpectedEof {
                    expected: EXPECTED.to_string(),
                    span: tok.span,
                }),
                _ => Err(TraceQlError::UnexpectedToken {
                    found: describe(&tok.kind),
                    expected: EXPECTED.to_string(),
                    span: tok.span,
                }),
            }
        }
    }
}

/// `PipelineStage := Aggregate | Select | Metric` (plan v2 F5 / v3 F5;
/// issue #59 adds the zero-arity metrics stage). The deferred
/// `*_over_time` metrics functions are recognized here and rejected as
/// `NotYetSupported` (M7, task-manager adjudication 1 on issue #59), as
/// is metrics grouping `by` after a metric stage.
fn parse_pipeline_stage(cursor: &mut Cursor<'_>) -> Result<PipelineStage, TraceQlError> {
    let tok = cursor.peek().clone();
    let name = match &tok.kind {
        TokenKind::Ident(name) => name.clone(),
        TokenKind::Eof => {
            return Err(TraceQlError::UnexpectedEof {
                expected: "a pipeline stage (count, sum, avg, min, max, or select)".to_string(),
                span: tok.span,
            });
        }
        _ => {
            return Err(TraceQlError::UnexpectedToken {
                found: describe(&tok.kind),
                expected: "a pipeline stage (count, sum, avg, min, max, or select)".to_string(),
                span: tok.span,
            });
        }
    };

    if name == "select" {
        cursor.advance();
        return parse_select(cursor);
    }
    if let Some(op) = AggregateOp::from_ident(&name) {
        cursor.advance();
        return parse_aggregate(cursor, op);
    }
    if let Some(func) = MetricFn::from_ident(&name) {
        cursor.advance();
        return parse_metric(cursor, func);
    }
    if ast::UNSUPPORTED_METRIC_FNS.contains(&name.as_str()) {
        return Err(TraceQlError::NotYetSupported {
            construct: format!("metrics function '{name}'"),
            span: tok.span,
        });
    }
    Err(TraceQlError::UnexpectedToken {
        found: describe(&tok.kind),
        expected: "a pipeline stage (count, sum, avg, min, max, or select)".to_string(),
        span: tok.span,
    })
}

/// `count() Cmp Value` (zero-arity) or `avg|sum|min|max(AggField) Cmp
/// Value` (one-arity, numeric-aggregatable fields only) — every
/// malformed arity is a positioned error (plan v2 F5).
fn parse_aggregate(
    cursor: &mut Cursor<'_>,
    op: AggregateOp,
) -> Result<PipelineStage, TraceQlError> {
    cursor.expect(&TokenKind::LParen, "'('")?;
    let field = match op {
        AggregateOp::Count => {
            cursor.expect(&TokenKind::RParen, "')' (count() takes no argument)")?;
            None
        }
        _ => {
            if matches!(cursor.peek().kind, TokenKind::RParen) {
                let span = cursor.peek().span;
                return Err(TraceQlError::UnexpectedToken {
                    found: "')'".to_string(),
                    expected: "an aggregatable field (duration or an attribute)".to_string(),
                    span,
                });
            }
            let (field, field_span) = parse_field(cursor)?;
            if matches!(
                field,
                Field::Intrinsic(Intrinsic::Name)
                    | Field::Intrinsic(Intrinsic::Status)
                    | Field::Intrinsic(Intrinsic::Kind)
            ) {
                return Err(TraceQlError::UnexpectedToken {
                    found: format!("identifier {:?}", field.to_string()),
                    expected: "an aggregatable field (duration or an attribute)".to_string(),
                    span: field_span,
                });
            }
            cursor.expect(&TokenKind::RParen, "')'")?;
            Some(field)
        }
    };
    let cmp = parse_comparison_op(cursor)?;
    let value = parse_aggregate_value(cursor)?;
    Ok(PipelineStage::Aggregate {
        op,
        field,
        cmp,
        value,
    })
}

/// `Metric := ("rate"|"count_over_time") "(" ")"` — strictly zero-arity
/// (a stray argument is a positioned error). A trailing `by` is the
/// recognized-but-M7 metrics-grouping construct (issue #59 plan v2
/// delta 7), named rather than left to fail as generic trailing input.
fn parse_metric(cursor: &mut Cursor<'_>, func: MetricFn) -> Result<PipelineStage, TraceQlError> {
    cursor.expect(&TokenKind::LParen, "'('")?;
    if !matches!(cursor.peek().kind, TokenKind::RParen) {
        let tok = cursor.peek().clone();
        if matches!(tok.kind, TokenKind::Eof) {
            return Err(TraceQlError::UnexpectedEof {
                expected: format!("')' ({func}() takes no argument)"),
                span: tok.span,
            });
        }
        return Err(TraceQlError::UnexpectedToken {
            found: describe(&tok.kind),
            expected: format!("')' ({func}() takes no argument)"),
            span: tok.span,
        });
    }
    cursor.advance(); // ')'
    if let TokenKind::Ident(next) = &cursor.peek().kind
        && next == "by"
    {
        return Err(TraceQlError::NotYetSupported {
            construct: "metrics grouping 'by'".to_string(),
            span: cursor.peek().span,
        });
    }
    Ok(PipelineStage::Metric(func))
}

fn parse_comparison_op(cursor: &mut Cursor<'_>) -> Result<ComparisonOp, TraceQlError> {
    let tok = cursor.peek().clone();
    let op = match tok.kind {
        TokenKind::Eq => ComparisonOp::Eq,
        TokenKind::Neq => ComparisonOp::Neq,
        TokenKind::Gt => ComparisonOp::Gt,
        TokenKind::Gte => ComparisonOp::Gte,
        TokenKind::Lt => ComparisonOp::Lt,
        TokenKind::Lte => ComparisonOp::Lte,
        TokenKind::Re => ComparisonOp::Re,
        TokenKind::Nre => ComparisonOp::Nre,
        TokenKind::Eof => {
            return Err(TraceQlError::UnexpectedEof {
                expected: "a comparison operator".to_string(),
                span: tok.span,
            });
        }
        _ => {
            return Err(TraceQlError::UnexpectedToken {
                found: describe(&tok.kind),
                expected: "a comparison operator".to_string(),
                span: tok.span,
            });
        }
    };
    cursor.advance();
    Ok(op)
}

/// The right-hand side of an aggregate filter: a number (`count() > 3`)
/// or a duration (`avg(duration) > 100ms`).
fn parse_aggregate_value(cursor: &mut Cursor<'_>) -> Result<Value, TraceQlError> {
    let tok = cursor.peek().clone();
    match &tok.kind {
        TokenKind::Number(raw) => {
            let raw = raw.clone();
            cursor.advance();
            Ok(Value::Number(raw))
        }
        TokenKind::Duration(raw) => {
            let parsed = duration::parse_duration(raw, tok.span)?;
            cursor.advance();
            Ok(Value::Duration(parsed))
        }
        TokenKind::Eof => Err(TraceQlError::UnexpectedEof {
            expected: "a number or a duration".to_string(),
            span: tok.span,
        }),
        _ => Err(TraceQlError::UnexpectedToken {
            found: describe(&tok.kind),
            expected: "a number or a duration".to_string(),
            span: tok.span,
        }),
    }
}

/// `Select := "select" "(" Field { "," Field } ")"` — one or more fields;
/// empty `select()` is a positioned error (plan v3 F5).
fn parse_select(cursor: &mut Cursor<'_>) -> Result<PipelineStage, TraceQlError> {
    cursor.expect(&TokenKind::LParen, "'('")?;
    if matches!(cursor.peek().kind, TokenKind::RParen) {
        let span = cursor.peek().span;
        return Err(TraceQlError::UnexpectedToken {
            found: "')'".to_string(),
            expected: "a field (select() requires at least one field)".to_string(),
            span,
        });
    }
    let mut fields = Vec::new();
    loop {
        let (field, _) = parse_field(cursor)?;
        fields.push(field);
        if matches!(cursor.peek().kind, TokenKind::Comma) {
            cursor.advance();
            continue;
        }
        break;
    }
    cursor.expect(&TokenKind::RParen, "')'")?;
    Ok(PipelineStage::Select { fields })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `{ .a = 1 && .a = 1 && ... }` with `ops` field-level `&&`
    /// operators.
    fn flat_field_and_chain(ops: usize) -> String {
        let mut q = String::from("{ .a = 1");
        for _ in 0..ops {
            q.push_str(" && .a = 1");
        }
        q.push_str(" }");
        q
    }

    /// `{} || {} || ...` with `ops` spanset-level `||` operators.
    fn flat_spanset_or_chain(ops: usize) -> String {
        let mut q = String::from("{}");
        for _ in 0..ops {
            q.push_str(" || {}");
        }
        q
    }

    #[test]
    fn a_just_under_limit_flat_field_chain_parses() {
        // The budget admits MAX_DEPTH - 1 binary nodes.
        assert!(parse(&flat_field_and_chain(MAX_DEPTH - 1)).is_ok());
    }

    #[test]
    fn an_over_limit_flat_field_chain_is_a_clean_error() {
        let err = parse(&flat_field_and_chain(MAX_DEPTH)).unwrap_err();
        assert!(matches!(err, TraceQlError::RecursionLimitExceeded { .. }));
    }

    #[test]
    fn a_just_under_limit_flat_spanset_chain_parses() {
        assert!(parse(&flat_spanset_or_chain(MAX_DEPTH - 1)).is_ok());
    }

    #[test]
    fn an_over_limit_flat_spanset_chain_is_a_clean_error() {
        let err = parse(&flat_spanset_or_chain(MAX_DEPTH)).unwrap_err();
        assert!(matches!(err, TraceQlError::RecursionLimitExceeded { .. }));
    }

    #[test]
    fn the_binary_budget_is_shared_across_field_and_spanset_levels() {
        // 32 field-level nodes + 31 spanset-level nodes = 63 (under the
        // 64 budget) parses; one more spanset node tips it over.
        let under = format!("{}{}", flat_field_and_chain(32), " && {}".repeat(31));
        assert!(parse(&under).is_ok());
        let over = format!("{}{}", flat_field_and_chain(32), " && {}".repeat(32));
        let err = parse(&over).unwrap_err();
        assert!(matches!(err, TraceQlError::RecursionLimitExceeded { .. }));
    }

    #[test]
    fn the_over_limit_error_points_at_the_offending_operator() {
        let query = flat_field_and_chain(MAX_DEPTH);
        let err = parse(&query).unwrap_err();
        // The 64th `&&` is the one that exceeds the budget; its span
        // must sit inside the query and start on the operator.
        let span = err.span();
        assert_eq!(&query[span.start..span.end], "&&");
    }

    #[test]
    fn a_maximal_ast_survives_display_reparse_and_drop() {
        // AC5/Drop-safety proof at the limit: parse the deepest chain
        // the budget admits, render it (recursive Display), reparse the
        // rendering (round-trip), and drop both ASTs (recursive Drop) —
        // all without overflowing the stack.
        for query in [
            flat_field_and_chain(MAX_DEPTH - 1),
            flat_spanset_or_chain(MAX_DEPTH - 1),
        ] {
            let ast = parse(&query).unwrap();
            let rendered = ast.to_string();
            let reparsed = parse(&rendered).unwrap();
            assert_eq!(reparsed, ast);
            drop(reparsed);
            drop(ast);
        }
    }

    #[test]
    fn a_hundred_thousand_operand_chain_errors_without_overflow() {
        // The review's scenario: a paren-free 100k-operand chain must be
        // a clean positioned error, not a stack overflow.
        for query in [
            flat_field_and_chain(100_000),
            flat_spanset_or_chain(100_000),
        ] {
            let err = parse(&query).unwrap_err();
            assert!(matches!(err, TraceQlError::RecursionLimitExceeded { .. }));
        }
    }

    #[test]
    fn parent_with_a_dot_is_the_recognized_m7_scope() {
        let err = parse(r#"{ parent.foo = "x" }"#).unwrap_err();
        match err {
            TraceQlError::NotYetSupported { construct, .. } => {
                assert_eq!(construct, "parent scope");
            }
            other => panic!("unexpected {other}"),
        }
    }

    #[test]
    fn bare_parent_without_a_dot_is_a_plain_syntax_error() {
        let err = parse(r#"{ parent = "x" }"#).unwrap_err();
        match err {
            TraceQlError::UnexpectedToken { found, span, .. } => {
                assert!(found.contains("parent"), "found: {found}");
                assert_eq!(span.start, 2);
            }
            other => panic!("unexpected {other}"),
        }
    }

    #[test]
    fn a_bare_attribute_is_the_recognized_existence_boundary() {
        let err = parse("{ .foo }").unwrap_err();
        match err {
            TraceQlError::NotYetSupported { construct, .. } => {
                assert_eq!(construct, "bare attribute expression");
            }
            other => panic!("unexpected {other}"),
        }
    }

    #[test]
    fn a_bare_intrinsic_is_a_plain_missing_comparison_error() {
        // `{ name }` is malformed grammar in every milestone, not a
        // future construct (round-2 adjudication 1).
        for query in ["{ name }", "{ duration }", "{ status && .a = 1 }"] {
            let err = parse(query).unwrap_err();
            match err {
                TraceQlError::UnexpectedToken { expected, .. } => {
                    assert!(
                        expected.contains("comparison operator"),
                        "{query}: {expected}"
                    );
                }
                other => panic!("{query} -> unexpected {other}"),
            }
        }
    }

    #[test]
    fn a_bare_intrinsic_at_end_of_input_is_unexpected_eof() {
        let err = parse("{ kind").unwrap_err();
        assert!(matches!(err, TraceQlError::UnexpectedEof { .. }), "{err}");
    }

    #[test]
    fn rate_and_count_over_time_parse_to_the_metric_stage() {
        for (query, func) in [
            ("{} | rate()", MetricFn::Rate),
            ("{} | count_over_time()", MetricFn::CountOverTime),
        ] {
            let parsed = parse(query).unwrap();
            assert_eq!(parsed.pipeline, vec![PipelineStage::Metric(func)]);
        }
    }

    #[test]
    fn a_metric_fn_with_an_argument_is_a_positioned_arity_error() {
        let err = parse("{} | rate(5)").unwrap_err();
        match err {
            TraceQlError::UnexpectedToken { expected, span, .. } => {
                assert!(expected.contains("rate() takes no argument"), "{expected}");
                assert_eq!(span.start, 10, "points at the stray argument");
            }
            other => panic!("unexpected {other}"),
        }
    }

    #[test]
    fn a_metric_fn_cut_off_mid_call_is_unexpected_eof() {
        let err = parse("{} | rate(").unwrap_err();
        assert!(matches!(err, TraceQlError::UnexpectedEof { .. }), "{err}");
    }

    #[test]
    fn metrics_grouping_by_is_the_recognized_m7_boundary() {
        let query = "{} | rate() by (resource.service.name)";
        let err = parse(query).unwrap_err();
        match err {
            TraceQlError::NotYetSupported { construct, span } => {
                assert_eq!(construct, "metrics grouping 'by'");
                assert_eq!(&query[span.start..span.end], "by");
            }
            other => panic!("unexpected {other}"),
        }
    }

    #[test]
    fn deferred_over_time_functions_stay_positioned_not_yet_supported() {
        for name in [
            "avg_over_time",
            "min_over_time",
            "max_over_time",
            "quantile_over_time",
            "histogram_over_time",
        ] {
            let query = format!("{{}} | {name}()");
            let err = parse(&query).unwrap_err();
            match err {
                TraceQlError::NotYetSupported { construct, span } => {
                    assert_eq!(construct, format!("metrics function '{name}'"));
                    assert_eq!(span.start, 5, "{query}");
                }
                other => panic!("{query} -> unexpected {other}"),
            }
        }
    }
}
