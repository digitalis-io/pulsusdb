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
//! Query             := SpansetExpr ("|" PipelineStage)*
//! SpansetExpr       := SpansetAnd ("||" SpansetAnd)*
//! SpansetAnd        := SpansetStructural ("&&" SpansetStructural)*
//! SpansetStructural := SpansetPrimary ((">"|">>"|"~") SpansetPrimary)*
//! SpansetPrimary    := SpansetFilter | "(" SpansetExpr ")"
//! SpansetFilter     := "{" FieldExpr? "}"
//! FieldExpr         := FieldAnd ("||" FieldAnd)*
//! FieldAnd          := FieldPrimary ("&&" FieldPrimary)*
//! FieldPrimary      := "(" FieldExpr ")" | Field CmpOp Value
//! PipelineStage     := "count" "(" ")" CmpOp Value
//!                    | ("avg"|"sum"|"min"|"max") "(" AggField ")" CmpOp Value
//!                    | "select" "(" Field { "," Field } ")"
//! ```
//!
//! Structural operators (`>`/`>>`/`~`, issue #172) bind TIGHTER than
//! `&&`/`||` and are left-associative (`{a} && {b} > {c}` ≡
//! `{a} && ({b} > {c})`; `{a} > {b} > {c}` ≡ `({a} > {b}) > {c}`) — the
//! adjudicated precedence pin, frozen into the corpus goldens.
//!
//! Disambiguation of the dual-role `>`/`>=`/`<`/`<=` tokens (comparison
//! inside a field expression, structural operator between spansets) is
//! purely positional: field-level comparisons are fully consumed before
//! the closing `}`, so the spanset combination position only ever sees
//! `&&`/`||`/`|`/structural/EOF — the LogQL `!=` disambiguation
//! precedent.

use crate::ast::{
    AggregateOp, AttrScope, BoolOp, ComparisonOp, Field, FieldExpr, HintValue, Intrinsic, MetricFn,
    MetricHint, MetricStage, PipelineStage, Query, SecondStage, SpanKindValue, SpansetExpr,
    SpansetFilter, StatusValue, StructuralModifier, StructuralOp, Value,
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
        TokenKind::Colon => "':'".to_string(),
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
    let mut lhs = parse_spanset_structural(cursor, depth, binary_nodes)?;
    while matches!(cursor.peek().kind, TokenKind::AndAnd) {
        charge_binary_node(binary_nodes, cursor.peek().span)?;
        cursor.advance();
        let rhs = parse_spanset_structural(cursor, depth, binary_nodes)?;
        lhs = SpansetExpr::Binary {
            op: BoolOp::And,
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
        };
    }
    Ok(lhs)
}

/// `SpansetStructural := SpansetPrimary (StructOp SpansetPrimary)*` — all
/// fifteen structural relations (issue #172 `>`/`>>`/`~`; issue #183
/// completes the surface with `<`/`<<` and the negated/union modifiers):
/// tighter than `&&`/`||`, left-associative. Each structural node charges
/// the shared binary-node budget exactly like `&&`/`||`.
///
/// The operator is recognized by parser POSITION from one or two tokens:
/// a single `Gt`/`Shr`/`Lt`/`Shl`/`Tilde` is Plain, `Nre` (`!~`) is a
/// negated sibling, `Bang` + `{Gt,Shr,Lt,Shl}` is Negated, and
/// `Amp` + `{Gt,Shr,Lt,Shl,Tilde}` is Union. `>=`/`<=` between spansets
/// stay recognized-but-M7 boundaries (Tempo rejects them too).
fn parse_spanset_structural(
    cursor: &mut Cursor<'_>,
    depth: usize,
    binary_nodes: &mut usize,
) -> Result<SpansetExpr, TraceQlError> {
    let mut lhs = parse_spanset_primary(cursor, depth, binary_nodes)?;
    loop {
        let start = cursor.peek().span;
        let (op, modifier, tokens) = match &cursor.peek().kind {
            TokenKind::Gt => (StructuralOp::Child, StructuralModifier::Plain, 1),
            TokenKind::Shr => (StructuralOp::Descendant, StructuralModifier::Plain, 1),
            TokenKind::Lt => (StructuralOp::Parent, StructuralModifier::Plain, 1),
            TokenKind::Shl => (StructuralOp::Ancestor, StructuralModifier::Plain, 1),
            TokenKind::Tilde => (StructuralOp::Sibling, StructuralModifier::Plain, 1),
            TokenKind::Nre => (StructuralOp::Sibling, StructuralModifier::Negated, 1),
            TokenKind::Gte => {
                return Err(TraceQlError::NotYetSupported {
                    construct: "structural operator '>='".to_string(),
                    span: start,
                });
            }
            TokenKind::Lte => {
                return Err(TraceQlError::NotYetSupported {
                    construct: "structural operator '<='".to_string(),
                    span: start,
                });
            }
            TokenKind::Bang => match &cursor.peek2().kind {
                TokenKind::Gt => (StructuralOp::Child, StructuralModifier::Negated, 2),
                TokenKind::Shr => (StructuralOp::Descendant, StructuralModifier::Negated, 2),
                TokenKind::Lt => (StructuralOp::Parent, StructuralModifier::Negated, 2),
                TokenKind::Shl => (StructuralOp::Ancestor, StructuralModifier::Negated, 2),
                // A `!` not introducing a negated structural operator
                // (`!{…}` is Tempo-rejected) falls through to a generic
                // error at the outer levels.
                _ => return Ok(lhs),
            },
            TokenKind::Amp => match &cursor.peek2().kind {
                TokenKind::Gt => (StructuralOp::Child, StructuralModifier::Union, 2),
                TokenKind::Shr => (StructuralOp::Descendant, StructuralModifier::Union, 2),
                TokenKind::Lt => (StructuralOp::Parent, StructuralModifier::Union, 2),
                TokenKind::Shl => (StructuralOp::Ancestor, StructuralModifier::Union, 2),
                TokenKind::Tilde => (StructuralOp::Sibling, StructuralModifier::Union, 2),
                // A lone `&` (not `&&`, not a union structural op) is a
                // generic error downstream (the `lone_amp` corpus case).
                _ => return Ok(lhs),
            },
            _ => return Ok(lhs),
        };
        charge_binary_node(binary_nodes, start)?;
        for _ in 0..tokens {
            cursor.advance();
        }
        let rhs = parse_spanset_primary(cursor, depth, binary_nodes)?;
        lhs = SpansetExpr::Structural {
            op,
            modifier,
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
        };
    }
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
        // `logic.not` (issue #183): unary field negation binds tighter
        // than `&&`/`||` — a primary. `depth` bounds `!`-chain nesting
        // (`{ !!!…!.a }`) so the recursive walk never overflows the stack.
        TokenKind::Bang => {
            if depth >= MAX_DEPTH {
                return Err(TraceQlError::RecursionLimitExceeded { span: tok.span });
            }
            cursor.advance();
            let inner = parse_field_primary(cursor, depth + 1, binary_nodes)?;
            Ok(FieldExpr::Not(Box::new(inner)))
        }
        // A bare boolean static (`static.bare_boolean`, issue #183): a
        // lone `true`/`false` at field-primary position, not the scope of
        // a dotted attribute.
        TokenKind::Ident(ref name)
            if (name == "true" || name == "false")
                && !matches!(cursor.peek2().kind, TokenKind::Dot) =>
        {
            let b = name == "true";
            cursor.advance();
            Ok(FieldExpr::BoolStatic(b))
        }
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
            // `comparison.rhs_attribute` (issue #183): when the value
            // position begins a field (attribute or intrinsic) the RHS is
            // a `Field`, not a literal. Regex operators (`=~`/`!~`) never
            // accept a field RHS — they fall through to `parse_value`,
            // which rejects the field-start (Tempo rejects `{ .a =~ .b }`).
            if !matches!(op, ComparisonOp::Re | ComparisonOp::Nre) && rhs_begins_field(cursor) {
                let (rhs, _) = parse_field(cursor)?;
                return Ok(FieldExpr::FieldCompare {
                    lhs: field,
                    op,
                    rhs,
                });
            }
            let value = parse_value(cursor, &field)?;
            Ok(FieldExpr::Comparison { field, op, value })
        }
    }
}

/// Whether the token at the value position begins a `Field` right-hand
/// side (issue #183 `comparison.rhs_attribute`): the unscoped `.attr`
/// form, a `span.`/`resource.`/`parent.` scoped attribute, or a bare
/// intrinsic keyword. Boolean/status/kind value keywords (`true`, `ok`,
/// `server`, …) are NOT intrinsics, so they stay literal values.
fn rhs_begins_field(cursor: &Cursor<'_>) -> bool {
    match &cursor.peek().kind {
        TokenKind::Dot => true,
        TokenKind::Ident(name) => {
            if (name == "span" || name == "resource" || name == "parent")
                && matches!(cursor.peek2().kind, TokenKind::Dot)
            {
                true
            } else {
                Intrinsic::from_ident(name).is_some()
                    && !matches!(cursor.peek2().kind, TokenKind::Dot)
            }
        }
        _ => false,
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
            // Colon-scoped intrinsic (`span:childCount`, `trace:id`, …,
            // issue #184): `<scope> : <ident>`. A known scope+field pair
            // resolves to the normalized intrinsic; any unknown scope
            // (`event:`/`link:`/`instrumentation:`) or unknown field is a
            // GENERIC error — never a named boundary — so those constructs
            // keep their interim-generic disposition.
            if matches!(cursor.peek2().kind, TokenKind::Colon) {
                if let TokenKind::Ident(field) = &cursor.peek_at(2).kind
                    && let Some(intrinsic) = Intrinsic::from_scoped(name, field)
                {
                    let start = tok.span.start;
                    cursor.advance(); // scope ident
                    cursor.advance(); // ':'
                    let field_tok = cursor.advance(); // field ident
                    return Ok((
                        Field::Intrinsic(intrinsic),
                        Span {
                            start,
                            end: field_tok.span.end,
                        },
                    ));
                }
                return Err(TraceQlError::UnexpectedToken {
                    found: describe(&tok.kind),
                    expected: "a known scoped intrinsic (span:… or trace:…)".to_string(),
                    span: tok.span,
                });
            }
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
        // `duration` and `traceDuration`/`trace:duration` require a
        // duration literal (issue #184: the trace-wide duration is the
        // same value type as the span duration).
        Field::Intrinsic(Intrinsic::Duration | Intrinsic::TraceDuration) => {
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
        // String-valued intrinsics: `name` plus the issue #184 additions
        // `statusMessage`, `span:id`, `span:parentID`, `trace:id`,
        // `rootName`, `rootServiceName`. The operator (`=`/`!=`/`=~`/`!~`)
        // is validated downstream at leaf compilation; here the value must
        // be a string literal.
        Field::Intrinsic(
            Intrinsic::Name
            | Intrinsic::StatusMessage
            | Intrinsic::SpanId
            | Intrinsic::ParentId
            | Intrinsic::TraceId
            | Intrinsic::RootName
            | Intrinsic::RootServiceName,
        ) => {
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
        // Numeric span/trace properties: the nested-set intrinsics (issue
        // #181) and `span:childCount` (issue #184) compare against a bare
        // number (`< 0`, `> 2`). A regex string (`=~ "x"`) is a positioned
        // `UnexpectedToken` here — the value must be a number.
        Field::Intrinsic(
            Intrinsic::NestedSetParent
            | Intrinsic::NestedSetLeft
            | Intrinsic::NestedSetRight
            | Intrinsic::ChildCount,
        ) => {
            let tok = cursor.peek().clone();
            match &tok.kind {
                TokenKind::Number(raw) => {
                    let raw = raw.clone();
                    cursor.advance();
                    Ok(Value::Number(raw))
                }
                TokenKind::Eof => Err(TraceQlError::UnexpectedEof {
                    expected: "a number".to_string(),
                    span: tok.span,
                }),
                _ => Err(TraceQlError::UnexpectedToken {
                    found: describe(&tok.kind),
                    expected: "a number".to_string(),
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
    if is_metric_fn_name(&name) {
        cursor.advance();
        return parse_metric(cursor, &name);
    }
    if name == "topk" || name == "bottomk" {
        cursor.advance();
        return parse_second_stage(cursor, &name);
    }
    if name == "compare" {
        cursor.advance();
        return parse_compare(cursor);
    }
    Err(TraceQlError::UnexpectedToken {
        found: describe(&tok.kind),
        expected: "a pipeline stage (count, sum, avg, min, max, or select)".to_string(),
        span: tok.span,
    })
}

/// `Compare := "compare" "(" SpansetFilter ")"` (issue #182): the
/// `metrics.compare` construct. Its argument is a `{ … }` spanset filter
/// (the selection), not a field. The inner filter carries its own
/// (fresh, bounded) recursion budget.
fn parse_compare(cursor: &mut Cursor<'_>) -> Result<PipelineStage, TraceQlError> {
    cursor.expect(&TokenKind::LParen, "'('")?;
    let mut inner_nodes = 0usize;
    let selection = parse_spanset_filter(cursor, 0, &mut inner_nodes)?;
    cursor.expect(&TokenKind::RParen, "')'")?;
    let hints = parse_optional_with(cursor)?;
    Ok(PipelineStage::Compare {
        selection: Box::new(selection),
        hints,
    })
}

/// Whether `name` is a first-stage TraceQL metrics function (issue
/// #59/#182). `rate`/`count_over_time` are zero-arity; the `*_over_time`
/// family takes a numeric aggregation target (and `quantile_over_time`
/// trailing quantile literals).
fn is_metric_fn_name(name: &str) -> bool {
    matches!(
        name,
        "rate"
            | "count_over_time"
            | "sum_over_time"
            | "min_over_time"
            | "max_over_time"
            | "avg_over_time"
            | "quantile_over_time"
            | "histogram_over_time"
    )
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
            // Non-numerically-aggregatable intrinsics: the string/enum
            // fields plus every issue #184 trace-level/scoped intrinsic
            // (`avg(rootName)`, `sum(statusMessage)`, `max(span:childCount)`
            // — numeric aggregation of childCount/traceDuration is out of
            // scope). `duration`/`nestedSet*` stay aggregatable.
            if matches!(
                field,
                Field::Intrinsic(
                    Intrinsic::Name
                        | Intrinsic::Status
                        | Intrinsic::Kind
                        | Intrinsic::StatusMessage
                        | Intrinsic::ChildCount
                        | Intrinsic::SpanId
                        | Intrinsic::ParentId
                        | Intrinsic::TraceId
                        | Intrinsic::TraceDuration
                        | Intrinsic::RootName
                        | Intrinsic::RootServiceName
                )
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

/// `Metric := MetricFn [ "by" "(" Field { "," Field } ")" ]
///                     [ "with" "(" Hint { "," Hint } ")" ]` (issue
/// #59/#182). `rate()`/`count_over_time()` are strictly zero-arity; the
/// `*_over_time` family takes a numeric target field, and
/// `quantile_over_time` trailing quantile literals. Every malformed arity
/// is a positioned error.
fn parse_metric(cursor: &mut Cursor<'_>, name: &str) -> Result<PipelineStage, TraceQlError> {
    cursor.expect(&TokenKind::LParen, "'('")?;
    let func = match name {
        "rate" => {
            expect_no_metric_arg(cursor, name)?;
            MetricFn::Rate
        }
        "count_over_time" => {
            expect_no_metric_arg(cursor, name)?;
            MetricFn::CountOverTime
        }
        "sum_over_time" => MetricFn::SumOverTime(parse_metric_target(cursor)?),
        "min_over_time" => MetricFn::MinOverTime(parse_metric_target(cursor)?),
        "max_over_time" => MetricFn::MaxOverTime(parse_metric_target(cursor)?),
        "avg_over_time" => MetricFn::AvgOverTime(parse_metric_target(cursor)?),
        "histogram_over_time" => MetricFn::HistogramOverTime(parse_metric_target(cursor)?),
        "quantile_over_time" => {
            let field = parse_metric_target_keep_open(cursor)?;
            let quantiles = parse_quantile_list(cursor)?;
            MetricFn::QuantileOverTime { field, quantiles }
        }
        other => unreachable!("parse_metric dispatched on a non-metric name {other:?}"),
    };
    let by = parse_optional_by(cursor)?;
    let hints = parse_optional_with(cursor)?;
    let result_filter = parse_optional_result_filter(cursor)?;
    Ok(PipelineStage::Metric(MetricStage {
        func,
        by,
        hints,
        result_filter,
    }))
}

/// Parses an optional trailing metrics-result comparison (`… > 5`, issue
/// #182 — `metrics.result_comparison`): a comparison operator followed by
/// a number/duration, attached to the metric with no `|`. Regex operators
/// are not valid here. Returns `None` when no comparison follows.
fn parse_optional_result_filter(
    cursor: &mut Cursor<'_>,
) -> Result<Option<(ComparisonOp, Value)>, TraceQlError> {
    let op = match cursor.peek().kind {
        TokenKind::Eq => ComparisonOp::Eq,
        TokenKind::Neq => ComparisonOp::Neq,
        TokenKind::Gt => ComparisonOp::Gt,
        TokenKind::Gte => ComparisonOp::Gte,
        TokenKind::Lt => ComparisonOp::Lt,
        TokenKind::Lte => ComparisonOp::Lte,
        _ => return Ok(None),
    };
    cursor.advance();
    let value = parse_aggregate_value(cursor)?;
    Ok(Some((op, value)))
}

/// Consumes the closing `)` of a zero-arity metric function; a stray
/// argument (or EOF) is a positioned error.
fn expect_no_metric_arg(cursor: &mut Cursor<'_>, name: &str) -> Result<(), TraceQlError> {
    if matches!(cursor.peek().kind, TokenKind::RParen) {
        cursor.advance();
        return Ok(());
    }
    let tok = cursor.peek().clone();
    if matches!(tok.kind, TokenKind::Eof) {
        return Err(TraceQlError::UnexpectedEof {
            expected: format!("')' ({name}() takes no argument)"),
            span: tok.span,
        });
    }
    Err(TraceQlError::UnexpectedToken {
        found: describe(&tok.kind),
        expected: format!("')' ({name}() takes no argument)"),
        span: tok.span,
    })
}

/// Parses `Field ")"` — the single aggregation target of a `*_over_time`
/// function. An empty argument list is a positioned error.
fn parse_metric_target(cursor: &mut Cursor<'_>) -> Result<Field, TraceQlError> {
    let field = parse_metric_target_keep_open(cursor)?;
    cursor.expect(&TokenKind::RParen, "')'")?;
    Ok(field)
}

/// Parses the aggregation-target `Field` but leaves the cursor before the
/// closing `)` / next `,` — used by `quantile_over_time`, which follows
/// the field with a quantile list.
fn parse_metric_target_keep_open(cursor: &mut Cursor<'_>) -> Result<Field, TraceQlError> {
    if matches!(cursor.peek().kind, TokenKind::RParen) {
        let span = cursor.peek().span;
        return Err(TraceQlError::UnexpectedToken {
            found: "')'".to_string(),
            expected: "an aggregation target (duration or an attribute)".to_string(),
            span,
        });
    }
    let (field, _) = parse_field(cursor)?;
    Ok(field)
}

/// Parses `"," Number { "," Number } ")"` — one or more quantile literals
/// after a `quantile_over_time` target. At least one quantile is required.
fn parse_quantile_list(cursor: &mut Cursor<'_>) -> Result<Vec<Value>, TraceQlError> {
    let mut quantiles = Vec::new();
    cursor.expect(
        &TokenKind::Comma,
        "',' (quantile_over_time requires at least one quantile)",
    )?;
    loop {
        let tok = cursor.peek().clone();
        match &tok.kind {
            TokenKind::Number(raw) => {
                quantiles.push(Value::Number(raw.clone()));
                cursor.advance();
            }
            TokenKind::Eof => {
                return Err(TraceQlError::UnexpectedEof {
                    expected: "a quantile in [0, 1]".to_string(),
                    span: tok.span,
                });
            }
            _ => {
                return Err(TraceQlError::UnexpectedToken {
                    found: describe(&tok.kind),
                    expected: "a quantile in [0, 1]".to_string(),
                    span: tok.span,
                });
            }
        }
        if matches!(cursor.peek().kind, TokenKind::Comma) {
            cursor.advance();
            continue;
        }
        break;
    }
    cursor.expect(&TokenKind::RParen, "')'")?;
    Ok(quantiles)
}

/// Parses an optional trailing `by (field, ...)` grouping clause. Returns
/// the empty vector when no `by` follows (ungrouped).
fn parse_optional_by(cursor: &mut Cursor<'_>) -> Result<Vec<Field>, TraceQlError> {
    if !matches!(&cursor.peek().kind, TokenKind::Ident(n) if n == "by") {
        return Ok(Vec::new());
    }
    cursor.advance(); // 'by'
    cursor.expect(&TokenKind::LParen, "'('")?;
    if matches!(cursor.peek().kind, TokenKind::RParen) {
        let span = cursor.peek().span;
        return Err(TraceQlError::UnexpectedToken {
            found: "')'".to_string(),
            expected: "a grouping field (by() requires at least one field)".to_string(),
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
    Ok(fields)
}

/// Parses an optional trailing `with (key=value, ...)` hint clause.
/// Returns the empty vector when no `with` follows.
fn parse_optional_with(cursor: &mut Cursor<'_>) -> Result<Vec<MetricHint>, TraceQlError> {
    if !matches!(&cursor.peek().kind, TokenKind::Ident(n) if n == "with") {
        return Ok(Vec::new());
    }
    cursor.advance(); // 'with'
    cursor.expect(&TokenKind::LParen, "'('")?;
    if matches!(cursor.peek().kind, TokenKind::RParen) {
        let span = cursor.peek().span;
        return Err(TraceQlError::UnexpectedToken {
            found: "')'".to_string(),
            expected: "a hint (with() requires at least one key=value pair)".to_string(),
            span,
        });
    }
    let mut hints = Vec::new();
    loop {
        hints.push(parse_hint(cursor)?);
        if matches!(cursor.peek().kind, TokenKind::Comma) {
            cursor.advance();
            continue;
        }
        break;
    }
    cursor.expect(&TokenKind::RParen, "')'")?;
    Ok(hints)
}

/// `Hint := Ident "=" (Bool | Number | Duration | String)`.
fn parse_hint(cursor: &mut Cursor<'_>) -> Result<MetricHint, TraceQlError> {
    let (key, _) = cursor.expect_ident("a hint name (e.g. sample, exemplars)")?;
    cursor.expect(&TokenKind::Eq, "'=' (hints are key=value pairs)")?;
    let tok = cursor.peek().clone();
    let value = match &tok.kind {
        TokenKind::Ident(word) if word == "true" => HintValue::Bool(true),
        TokenKind::Ident(word) if word == "false" => HintValue::Bool(false),
        TokenKind::Number(raw) => HintValue::Number(raw.clone()),
        TokenKind::Duration(raw) => {
            let parsed = duration::parse_duration(raw, tok.span)?;
            cursor.advance();
            return Ok(MetricHint {
                key,
                value: HintValue::Duration(parsed),
            });
        }
        TokenKind::String(s) => HintValue::String(s.clone()),
        TokenKind::Eof => {
            return Err(TraceQlError::UnexpectedEof {
                expected: "a hint value (true, false, a number, a duration, or a string)"
                    .to_string(),
                span: tok.span,
            });
        }
        _ => {
            return Err(TraceQlError::UnexpectedToken {
                found: describe(&tok.kind),
                expected: "a hint value (true, false, a number, a duration, or a string)"
                    .to_string(),
                span: tok.span,
            });
        }
    };
    cursor.advance();
    Ok(MetricHint { key, value })
}

/// `SecondStage := ("topk"|"bottomk") "(" Number ")"` (issue #182): a
/// series-reduction operator over a first-stage metric's output.
fn parse_second_stage(cursor: &mut Cursor<'_>, name: &str) -> Result<PipelineStage, TraceQlError> {
    cursor.expect(&TokenKind::LParen, "'('")?;
    let tok = cursor.peek().clone();
    let n = match &tok.kind {
        TokenKind::Number(raw) => {
            let n = raw
                .parse::<u64>()
                .map_err(|_| TraceQlError::UnexpectedToken {
                    found: format!("number {raw:?}"),
                    expected: format!("a whole number of series ({name}(n))"),
                    span: tok.span,
                })?;
            cursor.advance();
            n
        }
        TokenKind::Eof => {
            return Err(TraceQlError::UnexpectedEof {
                expected: format!("a whole number of series ({name}(n))"),
                span: tok.span,
            });
        }
        _ => {
            return Err(TraceQlError::UnexpectedToken {
                found: describe(&tok.kind),
                expected: format!("a whole number of series ({name}(n))"),
                span: tok.span,
            });
        }
    };
    cursor.expect(&TokenKind::RParen, "')'")?;
    let stage = match name {
        "topk" => SecondStage::TopK(n),
        "bottomk" => SecondStage::BottomK(n),
        other => unreachable!("parse_second_stage dispatched on {other:?}"),
    };
    Ok(PipelineStage::MetricSecondStage(stage))
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

    /// The comparison field of a single-comparison spanset filter.
    fn only_field(q: &str) -> Field {
        match parse(q).expect("parse").spanset {
            SpansetExpr::Filter(SpansetFilter {
                body: Some(FieldExpr::Comparison { field, .. }),
            }) => field,
            other => panic!("{q}: expected a single comparison, got {other:?}"),
        }
    }

    #[test]
    fn colon_scoped_and_legacy_intrinsics_parse_to_normalized_variants() {
        // Every issue #184 construct (bare + scoped spellings) parses, and
        // the scoped/bare spellings that name the same field normalize onto
        // one variant.
        for (q, intrinsic) in [
            (r#"{ statusMessage = "boom" }"#, Intrinsic::StatusMessage),
            (
                r#"{ span:statusMessage = "boom" }"#,
                Intrinsic::StatusMessage,
            ),
            (r#"{ span:name = "checkout" }"#, Intrinsic::Name),
            ("{ span:duration > 100ms }", Intrinsic::Duration),
            ("{ span:status = error }", Intrinsic::Status),
            ("{ span:kind = server }", Intrinsic::Kind),
            (r#"{ span:id = "0a1b" }"#, Intrinsic::SpanId),
            (r#"{ span:parentID = "0a1b" }"#, Intrinsic::ParentId),
            ("{ span:childCount > 2 }", Intrinsic::ChildCount),
            ("{ trace:duration > 1s }", Intrinsic::TraceDuration),
            ("{ traceDuration > 1s }", Intrinsic::TraceDuration),
            (r#"{ trace:id = "0a1b" }"#, Intrinsic::TraceId),
            (r#"{ trace:rootName = "GET /" }"#, Intrinsic::RootName),
            (r#"{ rootName = "GET /" }"#, Intrinsic::RootName),
            (
                r#"{ trace:rootService = "gw" }"#,
                Intrinsic::RootServiceName,
            ),
            (r#"{ rootServiceName = "gw" }"#, Intrinsic::RootServiceName),
        ] {
            assert_eq!(only_field(q), Field::Intrinsic(intrinsic), "{q}");
        }
    }

    #[test]
    fn unknown_colon_scopes_are_generic_errors_not_named_boundaries() {
        // event:/link:/instrumentation: must stay GENERIC (interim-generic
        // disposition), never a NotYetSupported named boundary.
        for q in [
            r#"{ event:name = "exception" }"#,
            r#"{ link:spanID = "0a1b" }"#,
            r#"{ instrumentation:name = "otel" }"#,
            "{ span:bogus > 1 }",
        ] {
            match parse(q) {
                Err(TraceQlError::NotYetSupported { .. }) => {
                    panic!("{q}: must be a generic error, not a named boundary")
                }
                Err(_) => {}
                Ok(ast) => panic!("{q}: must not parse, got {ast:?}"),
            }
        }
    }

    #[test]
    fn aggregating_a_trace_level_or_scoped_intrinsic_is_a_positioned_error() {
        // AC9: numeric aggregation of these intrinsics is rejected at parse.
        for q in [
            r#"{} | avg(rootName)"#,
            r#"{} | sum(statusMessage)"#,
            r#"{} | max(span:childCount)"#,
            r#"{} | min(traceDuration)"#,
            r#"{} | avg(rootServiceName)"#,
        ] {
            assert!(
                matches!(parse(q), Err(TraceQlError::UnexpectedToken { .. })),
                "{q}: must be a positioned aggregation error"
            );
        }
    }

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

    // -- issue #172: structural operators ------------------------------

    fn filter_key(expr: &SpansetExpr) -> &str {
        match expr {
            SpansetExpr::Filter(SpansetFilter {
                body:
                    Some(FieldExpr::Comparison {
                        field: Field::Attribute { key, .. },
                        ..
                    }),
            }) => key,
            other => panic!("expected a single-attr filter, got {other:?}"),
        }
    }

    #[test]
    fn structural_operators_parse_to_structural_nodes() {
        for (query, op) in [
            ("{ .a = 1 } > { .b = 2 }", StructuralOp::Child),
            ("{ .a = 1 } >> { .b = 2 }", StructuralOp::Descendant),
            ("{ .a = 1 } ~ { .b = 2 }", StructuralOp::Sibling),
        ] {
            let parsed = parse(query).unwrap();
            match &parsed.spanset {
                SpansetExpr::Structural {
                    op: got, lhs, rhs, ..
                } => {
                    assert_eq!(*got, op, "{query}");
                    assert_eq!(filter_key(lhs), "a");
                    assert_eq!(filter_key(rhs), "b");
                }
                other => panic!("{query} -> expected Structural, got {other:?}"),
            }
        }
    }

    #[test]
    fn structural_binds_tighter_than_and_and_or() {
        // Adjudicated pin 1: `{a} && {b} > {c}` ≡ `{a} && ({b} > {c})`.
        let parsed = parse("{ .a = 1 } && { .b = 2 } > { .c = 3 }").unwrap();
        match &parsed.spanset {
            SpansetExpr::Binary {
                op: BoolOp::And,
                lhs,
                rhs,
            } => {
                assert_eq!(filter_key(lhs), "a");
                match rhs.as_ref() {
                    SpansetExpr::Structural {
                        op: StructuralOp::Child,
                        lhs,
                        rhs,
                        ..
                    } => {
                        assert_eq!(filter_key(lhs), "b");
                        assert_eq!(filter_key(rhs), "c");
                    }
                    other => panic!("expected the structural node under &&, got {other:?}"),
                }
            }
            other => panic!("expected && at the root, got {other:?}"),
        }
        // And under `||`.
        let parsed = parse("{ .a = 1 } > { .b = 2 } || { .c = 3 }").unwrap();
        match &parsed.spanset {
            SpansetExpr::Binary {
                op: BoolOp::Or,
                lhs,
                ..
            } => assert!(matches!(lhs.as_ref(), SpansetExpr::Structural { .. })),
            other => panic!("expected || at the root, got {other:?}"),
        }
    }

    #[test]
    fn chained_structural_is_left_associative() {
        // Adjudicated pin 1: `{a} > {b} >> {c}` ≡ `({a} > {b}) >> {c}`.
        let parsed = parse("{ .a = 1 } > { .b = 2 } >> { .c = 3 }").unwrap();
        match &parsed.spanset {
            SpansetExpr::Structural {
                op: StructuralOp::Descendant,
                lhs,
                rhs,
                ..
            } => {
                assert!(matches!(
                    lhs.as_ref(),
                    SpansetExpr::Structural {
                        op: StructuralOp::Child,
                        ..
                    }
                ));
                assert_eq!(filter_key(rhs), "c");
            }
            other => panic!("expected left-assoc structural chain, got {other:?}"),
        }
    }

    #[test]
    fn parentheses_override_structural_precedence() {
        // `({a} && {b}) > {c}` puts the && UNDER the structural node.
        let parsed = parse("({ .a = 1 } && { .b = 2 }) > { .c = 3 }").unwrap();
        match &parsed.spanset {
            SpansetExpr::Structural {
                op: StructuralOp::Child,
                lhs,
                rhs,
                ..
            } => {
                assert!(matches!(
                    lhs.as_ref(),
                    SpansetExpr::Binary {
                        op: BoolOp::And,
                        ..
                    }
                ));
                assert_eq!(filter_key(rhs), "c");
            }
            other => panic!("expected structural at the root, got {other:?}"),
        }
    }

    #[test]
    fn structural_nodes_charge_the_shared_binary_budget() {
        let mut q = String::from("{}");
        for _ in 0..MAX_DEPTH {
            q.push_str(" > {}");
        }
        let err = parse(&q).unwrap_err();
        assert!(matches!(err, TraceQlError::RecursionLimitExceeded { .. }));
        let mut under = String::from("{}");
        for _ in 0..MAX_DEPTH - 1 {
            under.push_str(" > {}");
        }
        assert!(parse(&under).is_ok());
    }

    #[test]
    fn remaining_structural_operators_stay_positioned_not_yet_supported() {
        // `<`/`<<` are implemented in issue #183; only `>=`/`<=` between
        // spansets remain recognized-but-M7 boundaries.
        for (query, construct) in [
            ("{ .a = 1 } >= { .b = 2 }", "structural operator '>='"),
            ("{ .a = 1 } <= { .b = 2 }", "structural operator '<='"),
        ] {
            let err = parse(query).unwrap_err();
            match err {
                TraceQlError::NotYetSupported {
                    construct: got,
                    span,
                } => {
                    assert_eq!(got, construct, "{query}");
                    assert_eq!(span.start, 11, "{query}");
                }
                other => panic!("{query} -> unexpected {other:?}"),
            }
        }
    }

    #[test]
    fn all_fifteen_structural_operators_parse_with_their_modifiers() {
        use StructuralModifier::*;
        use StructuralOp::*;
        for (query, want_op, want_mod) in [
            ("{ .a = 1 } < { .b = 2 }", Parent, Plain),
            ("{ .a = 1 } << { .b = 2 }", Ancestor, Plain),
            ("{ .a = 1 } !> { .b = 2 }", Child, Negated),
            ("{ .a = 1 } !>> { .b = 2 }", Descendant, Negated),
            ("{ .a = 1 } !< { .b = 2 }", Parent, Negated),
            ("{ .a = 1 } !<< { .b = 2 }", Ancestor, Negated),
            ("{ .a = 1 } !~ { .b = 2 }", Sibling, Negated),
            ("{ .a = 1 } &> { .b = 2 }", Child, Union),
            ("{ .a = 1 } &>> { .b = 2 }", Descendant, Union),
            ("{ .a = 1 } &< { .b = 2 }", Parent, Union),
            ("{ .a = 1 } &<< { .b = 2 }", Ancestor, Union),
            ("{ .a = 1 } &~ { .b = 2 }", Sibling, Union),
        ] {
            let parsed = parse(query).unwrap_or_else(|e| panic!("{query}: {e}"));
            match &parsed.spanset {
                SpansetExpr::Structural { op, modifier, .. } => {
                    assert_eq!(*op, want_op, "{query}");
                    assert_eq!(*modifier, want_mod, "{query}");
                }
                other => panic!("{query} -> expected Structural, got {other:?}"),
            }
            // Display round-trips through a reparse for every form.
            let reparsed = parse(&parsed.to_string()).unwrap_or_else(|e| panic!("{query}: {e}"));
            assert_eq!(reparsed, parsed, "{query}");
        }
    }

    #[test]
    fn nre_token_is_a_field_regex_and_a_structural_neg_sibling() {
        // `!~` inside `{…}` is a field regex; between spansets it is the
        // negated sibling — disambiguated purely by parser position.
        let field = parse(r#"{ .a !~ "x" }"#).unwrap();
        match &field.spanset {
            SpansetExpr::Filter(SpansetFilter {
                body: Some(FieldExpr::Comparison { op, .. }),
            }) => assert_eq!(*op, ComparisonOp::Nre),
            other => panic!("expected a field !~ comparison, got {other:?}"),
        }
        let structural = parse(r#"{ .a = 1 } !~ { .b = 2 }"#).unwrap();
        assert!(matches!(
            &structural.spanset,
            SpansetExpr::Structural {
                op: StructuralOp::Sibling,
                modifier: StructuralModifier::Negated,
                ..
            }
        ));
    }

    #[test]
    fn logic_not_parses_and_bare_boolean_statics_parse() {
        for query in ["{ !(.a = 1) }", "{ !(.a = 1 && .b = 2) }"] {
            let parsed = parse(query).unwrap_or_else(|e| panic!("{query}: {e}"));
            assert!(matches!(
                &parsed.spanset,
                SpansetExpr::Filter(SpansetFilter {
                    body: Some(FieldExpr::Not(_))
                })
            ));
            assert_eq!(parse(&parsed.to_string()).unwrap(), parsed, "{query}");
        }
        for (query, want) in [("{ true }", true), ("{ false }", false)] {
            let parsed = parse(query).unwrap();
            assert_eq!(
                parsed.spanset,
                SpansetExpr::Filter(SpansetFilter {
                    body: Some(FieldExpr::BoolStatic(want))
                })
            );
        }
    }

    #[test]
    fn field_vs_field_comparison_parses_and_regex_field_rhs_rejects() {
        for query in [
            r#"{ .a = .b }"#,
            r#"{ .a != span.b }"#,
            r#"{ .a > .b }"#,
            r#"{ duration = .b }"#,
            r#"{ .a = status }"#,
        ] {
            let parsed = parse(query).unwrap_or_else(|e| panic!("{query}: {e}"));
            match &parsed.spanset {
                SpansetExpr::Filter(SpansetFilter {
                    body: Some(FieldExpr::FieldCompare { .. }),
                }) => {}
                other => panic!("{query} -> expected FieldCompare, got {other:?}"),
            }
            assert_eq!(parse(&parsed.to_string()).unwrap(), parsed, "{query}");
        }
        // A regex against a field RHS is rejected (Tempo rejects it too).
        assert!(parse(r#"{ .a =~ .b }"#).is_err());
        // A spanset-level `!{…}` is a plain parse error (not a construct).
        assert!(matches!(
            parse(r#"!{ .a = 1 }"#),
            Err(TraceQlError::UnexpectedToken { .. })
        ));
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

    // -- issue #181: nested-set intrinsics --------------------------------

    #[test]
    fn nested_set_intrinsics_parse_to_numeric_comparisons() {
        for (query, intrinsic) in [
            ("{ nestedSetParent < 0 }", Intrinsic::NestedSetParent),
            ("{ nestedSetLeft > 0 }", Intrinsic::NestedSetLeft),
            ("{ nestedSetRight >= 1 }", Intrinsic::NestedSetRight),
        ] {
            let parsed = parse(query).unwrap();
            match &parsed.spanset {
                SpansetExpr::Filter(SpansetFilter {
                    body: Some(FieldExpr::Comparison { field, value, .. }),
                }) => {
                    assert_eq!(*field, Field::Intrinsic(intrinsic), "{query}");
                    assert!(matches!(value, Value::Number(_)), "{query}");
                }
                other => panic!("{query} -> unexpected {other:?}"),
            }
            // Display round-trips through a reparse.
            let reparsed = parse(&parsed.to_string()).unwrap();
            assert_eq!(reparsed, parsed, "{query}");
        }
    }

    #[test]
    fn nested_set_regex_string_is_a_positioned_unexpected_token() {
        let err = parse(r#"{ nestedSetLeft =~ "x" }"#).unwrap_err();
        match err {
            TraceQlError::UnexpectedToken { expected, span, .. } => {
                assert!(expected.contains("number"), "{expected}");
                // The string value sits after `nestedSetLeft =~ `.
                assert_eq!(
                    &r#"{ nestedSetLeft =~ "x" }"#[span.start..span.end],
                    r#""x""#
                );
            }
            other => panic!("unexpected {other}"),
        }
    }

    #[test]
    fn rate_and_count_over_time_parse_to_the_metric_stage() {
        for (query, func) in [
            ("{} | rate()", MetricFn::Rate),
            ("{} | count_over_time()", MetricFn::CountOverTime),
        ] {
            let parsed = parse(query).unwrap();
            assert_eq!(
                parsed.pipeline,
                vec![PipelineStage::Metric(MetricStage {
                    func,
                    by: vec![],
                    hints: vec![],
                    result_filter: None,
                })]
            );
        }
    }

    #[test]
    fn a_zero_arity_metric_fn_with_an_argument_is_a_positioned_arity_error() {
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
    fn over_time_functions_parse_with_their_aggregation_target() {
        for (query, want) in [
            (
                "{} | sum_over_time(duration)",
                MetricFn::SumOverTime(Field::Intrinsic(Intrinsic::Duration)),
            ),
            (
                "{} | min_over_time(duration)",
                MetricFn::MinOverTime(Field::Intrinsic(Intrinsic::Duration)),
            ),
            (
                "{} | max_over_time(duration)",
                MetricFn::MaxOverTime(Field::Intrinsic(Intrinsic::Duration)),
            ),
            (
                "{} | avg_over_time(duration)",
                MetricFn::AvgOverTime(Field::Intrinsic(Intrinsic::Duration)),
            ),
            (
                "{} | histogram_over_time(duration)",
                MetricFn::HistogramOverTime(Field::Intrinsic(Intrinsic::Duration)),
            ),
        ] {
            let parsed = parse(query).unwrap();
            assert_eq!(
                parsed.pipeline,
                vec![PipelineStage::Metric(MetricStage {
                    func: want,
                    by: vec![],
                    hints: vec![],
                    result_filter: None,
                })],
                "{query}"
            );
            assert_eq!(parse(&parsed.to_string()).unwrap(), parsed, "{query}");
        }
    }

    #[test]
    fn an_over_time_function_without_a_target_is_a_positioned_error() {
        let err = parse("{} | sum_over_time()").unwrap_err();
        assert!(matches!(err, TraceQlError::UnexpectedToken { .. }), "{err}");
    }

    #[test]
    fn quantile_over_time_parses_single_and_multiple_quantiles() {
        let parsed = parse("{} | quantile_over_time(duration, 0.5, 0.9, 0.99)").unwrap();
        assert_eq!(
            parsed.pipeline,
            vec![PipelineStage::Metric(MetricStage {
                func: MetricFn::QuantileOverTime {
                    field: Field::Intrinsic(Intrinsic::Duration),
                    quantiles: vec![
                        Value::Number("0.5".to_string()),
                        Value::Number("0.9".to_string()),
                        Value::Number("0.99".to_string()),
                    ],
                },
                by: vec![],
                hints: vec![],
                result_filter: None,
            })]
        );
        assert_eq!(parse(&parsed.to_string()).unwrap(), parsed);
    }

    #[test]
    fn quantile_over_time_without_a_quantile_is_a_positioned_error() {
        let err = parse("{} | quantile_over_time(duration)").unwrap_err();
        assert!(matches!(err, TraceQlError::UnexpectedToken { .. }), "{err}");
    }

    #[test]
    fn a_metric_by_grouping_parses_to_the_stage_grouping_keys() {
        let parsed = parse("{} | rate() by(resource.service.name)").unwrap();
        let PipelineStage::Metric(stage) = &parsed.pipeline[0] else {
            panic!("expected a metric stage, got {:?}", parsed.pipeline);
        };
        assert_eq!(
            stage.by,
            vec![Field::Attribute {
                scope: AttrScope::Resource,
                key: "service.name".to_string(),
            }]
        );
        assert_eq!(parse(&parsed.to_string()).unwrap(), parsed);
    }

    #[test]
    fn a_metric_by_with_no_field_is_a_positioned_error() {
        let err = parse("{} | rate() by()").unwrap_err();
        assert!(matches!(err, TraceQlError::UnexpectedToken { .. }), "{err}");
    }

    #[test]
    fn metric_with_hints_parse_bool_and_numeric_values() {
        let parsed = parse("{} | rate() with(sample=true, exemplars=100)").unwrap();
        let PipelineStage::Metric(stage) = &parsed.pipeline[0] else {
            panic!("expected a metric stage");
        };
        assert_eq!(
            stage.hints,
            vec![
                MetricHint {
                    key: "sample".to_string(),
                    value: HintValue::Bool(true),
                },
                MetricHint {
                    key: "exemplars".to_string(),
                    value: HintValue::Number("100".to_string()),
                },
            ]
        );
        assert_eq!(parse(&parsed.to_string()).unwrap(), parsed);
    }

    #[test]
    fn a_by_and_with_can_both_trail_a_metric() {
        let parsed =
            parse("{} | quantile_over_time(duration, 0.9) by(name) with(exemplars=true)").unwrap();
        let PipelineStage::Metric(stage) = &parsed.pipeline[0] else {
            panic!("expected a metric stage");
        };
        assert_eq!(stage.by, vec![Field::Intrinsic(Intrinsic::Name)]);
        assert_eq!(stage.hints.len(), 1);
        assert_eq!(parse(&parsed.to_string()).unwrap(), parsed);
    }

    #[test]
    fn topk_and_bottomk_parse_as_second_stages() {
        for (query, want) in [
            ("{} | rate() | topk(10)", SecondStage::TopK(10)),
            ("{} | rate() | bottomk(3)", SecondStage::BottomK(3)),
        ] {
            let parsed = parse(query).unwrap();
            assert_eq!(
                parsed.pipeline[1],
                PipelineStage::MetricSecondStage(want),
                "{query}"
            );
            assert_eq!(parse(&parsed.to_string()).unwrap(), parsed, "{query}");
        }
    }

    #[test]
    fn a_standalone_by_pipeline_stage_is_a_generic_error() {
        // `pipeline.by` (a top-level `| by(...)`, not a metric grouping)
        // stays a generic error, distinct from the metric `by(...)` clause.
        let err = parse("{} | by(resource.service.name)").unwrap_err();
        assert!(
            !matches!(err, TraceQlError::NotYetSupported { .. }),
            "standalone by() must be a generic error, got {err}"
        );
    }

    #[test]
    fn compare_parses_to_a_compare_stage_with_its_selection() {
        let parsed = parse(r#"{} | compare({ span.http.status_code = "500" })"#).unwrap();
        match &parsed.pipeline[..] {
            [PipelineStage::Compare { selection, hints }] => {
                assert!(selection.body.is_some(), "the selection filter is captured");
                assert!(hints.is_empty());
            }
            other => panic!("expected a compare stage, got {other:?}"),
        }
        assert_eq!(parse(&parsed.to_string()).unwrap(), parsed, "round-trips");
        // compare accepts trailing with() hints (e.g. exemplars).
        let with_ex = parse(r#"{} | compare({ .a = 1 }) with(exemplars=2)"#).unwrap();
        match &with_ex.pipeline[..] {
            [PipelineStage::Compare { hints, .. }] => assert_eq!(hints.len(), 1),
            other => panic!("expected compare with hints, got {other:?}"),
        }
        assert_eq!(parse(&with_ex.to_string()).unwrap(), with_ex, "round-trips");
    }

    #[test]
    fn a_metrics_result_comparison_attaches_to_the_metric_stage() {
        let parsed = parse("{} | rate() > 5").unwrap();
        match &parsed.pipeline[..] {
            [PipelineStage::Metric(stage)] => {
                assert_eq!(
                    stage.result_filter,
                    Some((ComparisonOp::Gt, Value::Number("5".to_string())))
                );
            }
            other => panic!("expected a metric stage with a result filter, got {other:?}"),
        }
        assert_eq!(parse(&parsed.to_string()).unwrap(), parsed, "round-trips");
        // A regex result comparison is not valid.
        assert!(parse(r#"{} | rate() =~ "5""#).is_err());
    }

    #[test]
    fn a_bare_with_after_a_spanset_is_a_generic_error() {
        // `hints.most_recent` probe: `with(...)` directly on a spanset
        // (no metric) stays a generic trailing-input error.
        let err = parse("{ .a = 1 } with(most_recent=true)").unwrap_err();
        assert!(
            !matches!(err, TraceQlError::NotYetSupported { .. }),
            "bare with() must be a generic error, got {err}"
        );
    }
}
