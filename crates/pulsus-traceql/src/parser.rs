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
//! Together they cap any root-to-leaf AST path at under
//! `MAX_DEPTH × (1 + LEVELS)` nested nodes — one nesting level admits at
//! most one node per precedence level on a right spine — so the derived
//! recursive `Debug`/`Display`/`Drop` implementations are stack-safe by
//! construction and no iterative `Drop` is needed.
//!
//! `depth` is charged where recursion is genuinely unbounded: parentheses,
//! unary prefix chains, and the RIGHT-associative `^` (whose RHS re-enters
//! at its own binding power, so `2^2^2^…` would otherwise recurse without
//! limit). A LEFT-associative RHS re-enters at a strictly higher binding
//! power and can therefore descend at most `LEVELS` frames, so charging it
//! would spend the budget on the shape of the precedence ladder rather
//! than on user nesting.
//!
//! Known gap, PRE-EXISTING and unchanged here (it predates the Stage B
//! collapse — `61dea2f` looped the arithmetic level uncharged in exactly
//! the same way): a paren-free left-associative ARITHMETIC chain
//! (`{ .a = 1+1+1+… }`) is bounded by neither counter, because the budget
//! counts `&&`/`||` only. Its left spine is as long as the input, and
//! `Display`/`Drop` walk it recursively. Widening the budget to cover it
//! would narrow the accept surface, so it is recorded rather than
//! quietly changed.
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
//! `&&` and `||` share ONE precedence level and are left-associative, at
//! both the spanset and the field level (issue #335 classes D10/D11). The
//! full table, tightest first: `^`, `* / %`, unary `-`, `+ -`, the
//! comparison operators, then `&&`/`||`. Every arithmetic level is
//! left-associative EXCEPT `^`, which is RIGHT-associative (D8), and
//! unary `-` sits BETWEEN `* / %` and `+ -` (D9). All four placements are
//! the reference's, read off its own parenthesised echo rather than
//! assumed; the captures and the whole accept-surface comparison live in
//! `tests/accept_surface/`. `^` additionally carries a deliberate VALUE
//! divergence (grouping agrees, the operator does not) — ledger row
//! `traceql-pow-integer-operand-swap`.
//!
//! Disambiguation of the dual-role `>`/`>=`/`<`/`<=` tokens (comparison
//! inside a field expression, structural operator between spansets) is
//! purely positional: field-level comparisons are fully consumed before
//! the closing `}`, so the spanset combination position only ever sees
//! `&&`/`||`/`|`/structural/EOF — the LogQL `!=` disambiguation
//! precedent.

use crate::ast::{
    AggregateOp, ArithOp, AttrScope, BoolOp, ComparisonOp, Field, FieldExpr, FieldOp, HintValue,
    Intrinsic, MetricFn, MetricHint, MetricStage, PipelineStage, Query, SecondStage, SpanKindValue,
    SpansetExpr, SpansetFilter, StatusValue, StructuralModifier, StructuralOp, UNARY_BINDING_POWER,
    UnaryOp, Value,
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
    // A trailing `with(...)` on a non-metric query carries search hints
    // (issue #185, `hints.most_recent`): `{ … } with(most_recent=true)`.
    let hints = parse_root_hints(&mut cursor)?;
    expect_eof(&cursor)?;
    Ok(Query {
        spanset,
        pipeline,
        hints,
    })
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
        TokenKind::Percent => "'%'".to_string(),
        TokenKind::Caret => "'^'".to_string(),
        TokenKind::Ident(s) => format!("identifier {s:?}"),
        TokenKind::String(s) => format!("string {s:?}"),
        TokenKind::Duration(s) => format!("duration {s:?}"),
        TokenKind::Number(s) => format!("number {s:?}"),
        TokenKind::Eof => "end of query".to_string(),
    }
}

/// The boolean operator a token introduces, if any. `&&` and `||` share
/// ONE precedence level and are left-associative (issue #335, classes
/// D10/D11) — `a || b && c` is `(a || b) && c`, not `a || (b && c)`.
///
/// That is the reference's grammar, verified black-box rather than
/// inferred: at the field level the reference echoes its own parse in a
/// type error (`{ .a = 1 || .b = 2 && "x" }` reports
/// `((.a = 1) || (.b = 2)) && \`x\``), and at the spanset level, where no
/// error channel exists, a result differential over pushed spans shows
/// `{A} || {B} && {C}` returning what `({A} || {B}) && {C}` returns.
/// Both captures live in `tests/accept_surface/matrix.json`.
fn bool_op_of(kind: &TokenKind) -> Option<BoolOp> {
    match kind {
        TokenKind::AndAnd => Some(BoolOp::And),
        TokenKind::OrOr => Some(BoolOp::Or),
        _ => None,
    }
}

/// `SpansetExpr := SpansetStructural (("&&" | "||") SpansetStructural)*`
/// — one precedence level, left-associative (see [`bool_op_of`]).
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
    let mut lhs = parse_spanset_structural(cursor, depth, binary_nodes)?;
    while let Some(op) = bool_op_of(&cursor.peek().kind) {
        charge_binary_node(binary_nodes, cursor.peek().span)?;
        cursor.advance();
        let rhs = parse_spanset_structural(cursor, depth, binary_nodes)?;
        lhs = SpansetExpr::Binary {
            op,
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

/// `FieldExpr := FieldPrimary (("&&" | "||") FieldPrimary)*` — one
/// precedence level, left-associative (see [`bool_op_of`]).
/// **The one climb** (issue #335 Stage B). Replaces the former
/// `parse_field_expr` / `parse_field_primary` / `parse_operand{,_bin,
/// _unary,_atom}` layering, which encoded operand SHAPE in the node kind
/// (`Comparison` vs `FieldCompare` vs `ArithCompare`) and therefore had
/// to look ahead — `rhs_begins_field`, `rhs_begins_arith` and the LHS
/// arithmetic peek — to decide which layer to enter. One node kind and
/// one precedence table need no lookahead at all.
///
/// Precedence, loosest first, matching the reference:
/// `&&`/`||` (one level) < comparison < `+ -` < unary `! -` < `* / %` < `^`.
fn parse_field_expr(
    cursor: &mut Cursor<'_>,
    depth: usize,
    binary_nodes: &mut usize,
) -> Result<FieldExpr, TraceQlError> {
    parse_field_bp(cursor, depth, binary_nodes, 0)
}

/// Precedence climbing: parse a prefix/atom, then absorb every infix
/// operator whose binding power is at least `min_bp`.
fn parse_field_bp(
    cursor: &mut Cursor<'_>,
    depth: usize,
    binary_nodes: &mut usize,
    min_bp: u8,
) -> Result<FieldExpr, TraceQlError> {
    if depth >= MAX_DEPTH {
        return Err(TraceQlError::RecursionLimitExceeded {
            span: cursor.peek().span,
        });
    }
    let mut lhs = parse_field_prefix(cursor, depth, binary_nodes)?;
    while let Some(op) = field_op_of(&cursor.peek().kind) {
        let bp = op.binding_power();
        if bp < min_bp {
            break;
        }
        // The budget counts `&&`/`||` ONLY, at both the field and spanset
        // levels — the pre-collapse meaning, preserved deliberately. The
        // climb sees comparison and arithmetic operators through the same
        // `field_op_of` table, so charging "every infix operator here"
        // reads natural and is wrong: it would spend the budget on the
        // `=` in each conjunct and reject a chain of ~32 comparisons that
        // parsed before. Widening a self-protection guard narrows the
        // accept surface.
        if matches!(op, FieldOp::Bool(_)) {
            charge_binary_node(binary_nodes, cursor.peek().span)?;
        }
        cursor.advance();
        // `= nil` / `!= nil` fold to `Exists` — ONLY after `=`/`!=` on a
        // field LHS, which is a decision about the operator and the LHS
        // already parsed, not a lookahead into the operand.
        if matches!(
            op,
            FieldOp::Cmp(ComparisonOp::Eq) | FieldOp::Cmp(ComparisonOp::Neq)
        ) && is_nil(cursor.peek())
            && let FieldExpr::Field(field) = &lhs
        {
            let field = field.clone();
            cursor.advance();
            lhs = FieldExpr::Exists {
                field,
                negated: matches!(op, FieldOp::Cmp(ComparisonOp::Eq)),
            };
            continue;
        }
        // Left-associative operators bind the RHS one level tighter;
        // right-associative (`^`) at the same level — which is also why
        // only the right-associative RHS charges `depth` (see the module
        // note): re-entering at the SAME binding power can recurse without
        // limit, re-entering higher cannot.
        let (next_bp, rhs_depth) = if op.is_right_assoc() {
            (bp, depth + 1)
        } else {
            (bp + 1, depth)
        };
        let rhs = parse_field_bp(cursor, rhs_depth, binary_nodes, next_bp)?;
        lhs = FieldExpr::Binary {
            op,
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
        };
    }
    Ok(lhs)
}

/// A prefix operator (`!`, `-`) or an atom.
fn parse_field_prefix(
    cursor: &mut Cursor<'_>,
    depth: usize,
    binary_nodes: &mut usize,
) -> Result<FieldExpr, TraceQlError> {
    let tok = cursor.peek().clone();
    let op = match tok.kind {
        TokenKind::Bang => UnaryOp::Not,
        TokenKind::Minus => UnaryOp::Neg,
        _ => return parse_field_atom(cursor, depth, binary_nodes),
    };
    if depth >= MAX_DEPTH {
        return Err(TraceQlError::RecursionLimitExceeded { span: tok.span });
    }
    cursor.advance();
    let expr = parse_field_bp(cursor, depth + 1, binary_nodes, UNARY_BINDING_POWER)?;
    Ok(FieldExpr::Unary {
        op,
        expr: Box::new(expr),
    })
}

/// An atom: a parenthesized expression, a literal, or a field.
///
/// **Context-free by construction** (issue #335 Stage B). The old
/// `parse_value(cursor, &field)` needed the LHS to type its operand; that
/// was doing two jobs. Typing bare idents is grammar and is done here
/// from the token alone — the ten reserved words (`ok`/`error`/`unset`,
/// the five span kinds, `true`/`false`) resolve to values without any
/// left context, which is how the reference's own grammar treats them
/// (measured: `{ .a = ok }`, `{ .a = server }` are reference 200s).
/// Rejecting a MISMATCHED pair is a constraint on the pair, not on the
/// atom, and lives in `validate.rs`.
fn parse_field_atom(
    cursor: &mut Cursor<'_>,
    depth: usize,
    binary_nodes: &mut usize,
) -> Result<FieldExpr, TraceQlError> {
    let tok = cursor.peek().clone();
    match &tok.kind {
        TokenKind::LParen => {
            cursor.advance();
            let expr = parse_field_expr(cursor, depth + 1, binary_nodes)?;
            cursor.expect(&TokenKind::RParen, "')'")?;
            Ok(expr)
        }
        TokenKind::String(v) => {
            let v = v.clone();
            cursor.advance();
            Ok(FieldExpr::Literal(Value::String(v)))
        }
        TokenKind::Number(raw) => {
            let raw = raw.clone();
            cursor.advance();
            Ok(FieldExpr::Literal(Value::Number(raw)))
        }
        TokenKind::Duration(raw) => {
            let parsed = duration::parse_duration(raw, tok.span)?;
            cursor.advance();
            Ok(FieldExpr::Literal(Value::Duration(parsed)))
        }
        TokenKind::Ident(name) => {
            if let Some(value) = static_value_of(name) {
                cursor.advance();
                return Ok(FieldExpr::Literal(value));
            }
            let (field, _) = parse_field(cursor)?;
            Ok(FieldExpr::Field(field))
        }
        TokenKind::Eof => Err(TraceQlError::UnexpectedEof {
            expected: FIELD_ATOM_EXPECTED.to_string(),
            span: tok.span,
        }),
        other => {
            // A field can also start with `.` or a scope token; defer to
            // `parse_field`, which owns those spellings and their errors.
            if field_start(other) {
                let (field, _) = parse_field(cursor)?;
                return Ok(FieldExpr::Field(field));
            }
            Err(TraceQlError::UnexpectedToken {
                found: describe(other),
                expected: FIELD_ATOM_EXPECTED.to_string(),
                span: tok.span,
            })
        }
    }
}

const FIELD_ATOM_EXPECTED: &str = "a field, a literal, or '('";

/// The reserved words that are VALUES wherever they appear — the
/// reference's static terminals. Resolved here, from the token alone,
/// because that is where the reference resolves them: `static` is a
/// terminal alternation plus one `IDENTIFIER` action block
/// (`pkg/traceql/expr.y:426-453` @ Tempo v3.0.2), with no left context.
///
/// Thirteen words: the three `status` keywords, the six `kind` keywords
/// (`unspecified` included since issue #335 Stage D1, class D13),
/// `true`/`false`, and the two integer bounds below.
fn static_value_of(name: &str) -> Option<Value> {
    if let Some(status) = StatusValue::from_ident(name) {
        return Some(Value::Status(status));
    }
    if let Some(kind) = SpanKindValue::from_ident(name) {
        return Some(Value::Kind(kind));
    }
    match name {
        "true" => Some(Value::Bool(true)),
        "false" => Some(Value::Bool(false)),
        // Issue #335 Stage D1, class D14. The reference's `static`
        // production resolves exactly these two identifiers to
        // `math.MinInt` / `math.MaxInt` inside its own action block
        // (`expr.y:442-452`), and errors `unknown identifier: …` on every
        // other one.
        //
        // **Why i64 and not isize.** `math.MinInt`/`math.MaxInt` are Go's
        // PLATFORM int, so the value is the build target's. The pinned
        // oracle image is `linux/amd64` (`/status/version` reports
        // `platform: linux/amd64`), i.e. a 64-bit build, so the reference
        // this repository is measured against resolves them to the 64-bit
        // bounds. That is what these constants match, and the reason is
        // written here rather than left for a reader to reconstruct from
        // an image tag.
        //
        // `Value::Number` carries the raw literal text, so the folded
        // integer both renders and re-parses unchanged — the identifier
        // spelling does NOT survive, exactly as at the reference, whose
        // `NewStaticInt` keeps no trace of how the value was written.
        "minInt" => Some(Value::Number(i64::MIN.to_string())),
        "maxInt" => Some(Value::Number(i64::MAX.to_string())),
        _ => None,
    }
}

/// A bare `Ident("nil")` in operand position — never a `Value` variant.
fn is_nil(tok: &Token) -> bool {
    matches!(&tok.kind, TokenKind::Ident(n) if n == "nil")
}

/// Whether `kind` can begin a field. `parse_field` owns these spellings
/// and their errors; this only decides whether to defer to it.
fn field_start(kind: &TokenKind) -> bool {
    matches!(kind, TokenKind::Dot | TokenKind::Ident(_))
}

/// Maps a token to an infix field operator.
fn field_op_of(kind: &TokenKind) -> Option<FieldOp> {
    Some(match kind {
        TokenKind::AndAnd => FieldOp::Bool(BoolOp::And),
        TokenKind::OrOr => FieldOp::Bool(BoolOp::Or),
        TokenKind::Eq => FieldOp::Cmp(ComparisonOp::Eq),
        TokenKind::Neq => FieldOp::Cmp(ComparisonOp::Neq),
        TokenKind::Gt => FieldOp::Cmp(ComparisonOp::Gt),
        TokenKind::Gte => FieldOp::Cmp(ComparisonOp::Gte),
        TokenKind::Lt => FieldOp::Cmp(ComparisonOp::Lt),
        TokenKind::Lte => FieldOp::Cmp(ComparisonOp::Lte),
        // `Re`/`Nre` are `=~`/`!~`. `Tilde` is the STRUCTURAL SIBLING
        // operator and is deliberately absent here — it is a spanset
        // operator, never a field one.
        TokenKind::Re => FieldOp::Cmp(ComparisonOp::Re),
        TokenKind::Nre => FieldOp::Cmp(ComparisonOp::Nre),
        TokenKind::Plus => FieldOp::Arith(ArithOp::Add),
        TokenKind::Minus => FieldOp::Arith(ArithOp::Sub),
        TokenKind::Star => FieldOp::Arith(ArithOp::Mul),
        TokenKind::Slash => FieldOp::Arith(ArithOp::Div),
        TokenKind::Percent => FieldOp::Arith(ArithOp::Mod),
        TokenKind::Caret => FieldOp::Arith(ArithOp::Pow),
        _ => return None,
    })
}

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
            if (name == "span"
                || name == "resource"
                || name == "instrumentation"
                || name == "event"
                || name == "link")
                && followed_by_dot
            {
                let scope = match name.as_str() {
                    "span" => AttrScope::Span,
                    "resource" => AttrScope::Resource,
                    "instrumentation" => AttrScope::Instrumentation,
                    "event" => AttrScope::Event,
                    _ => AttrScope::Link,
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
/// e.g. `http.status_code`. The leading segment may instead be a quoted
/// string (`span."attr with spaces"` — `scope.quoted`, issue #185) or a
/// bracketed string (`span.["foo bar"]` — `scope.bracketed`), both of
/// which carry an arbitrary key with spaces/punctuation. Returns the
/// joined key and the byte offset just past its last segment.
fn parse_dotted_key(cursor: &mut Cursor<'_>) -> Result<(String, usize), TraceQlError> {
    let (mut key, mut end) = match &cursor.peek().kind {
        // `span.["foo bar"]` — a bracketed key segment.
        TokenKind::LBracket => {
            cursor.advance(); // '['
            let tok = cursor.peek().clone();
            let TokenKind::String(s) = tok.kind else {
                return Err(match tok.kind {
                    TokenKind::Eof => TraceQlError::UnexpectedEof {
                        expected: "a quoted attribute name inside '[...]'".to_string(),
                        span: tok.span,
                    },
                    other => TraceQlError::UnexpectedToken {
                        found: describe(&other),
                        expected: "a quoted attribute name inside '[...]'".to_string(),
                        span: tok.span,
                    },
                });
            };
            cursor.advance(); // string
            let close = cursor.expect(&TokenKind::RBracket, "']'")?;
            (s, close.span.end)
        }
        // `span."attr with spaces"` — a quoted key segment.
        TokenKind::String(s) => {
            let s = s.clone();
            let span = cursor.peek().span;
            cursor.advance();
            (s, span.end)
        }
        _ => {
            let (first, first_span) = cursor.expect_ident("an attribute name")?;
            (first, first_span.end)
        }
    };
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
    // Spanset-level `by(...)` / `coalesce()` stages (issue #185): a
    // top-level `| by(...)` regroups the matched spans (distinct from the
    // metric `by(...)` clause); `| coalesce()` merges the spanset arrays.
    if name == "by" {
        cursor.advance();
        return parse_by_stage(cursor);
    }
    if name == "coalesce" {
        cursor.advance();
        return parse_coalesce_stage(cursor);
    }
    Err(TraceQlError::UnexpectedToken {
        found: describe(&tok.kind),
        expected: "a pipeline stage (count, sum, avg, min, max, or select)".to_string(),
        span: tok.span,
    })
}

/// `By := "by" "(" FieldExpr ")"` — the spanset-level grouping stage
/// (issue #185, `pipeline.by`; rewritten by issue #335 Stage D2, class
/// D16). Empty `by()` is a positioned error.
///
/// **Both halves of this signature are the reference's, and both were
/// wrong here before.** `groupOperation` is
/// `BY OPEN_PARENS fieldExpression CLOSE_PARENS` (`expr.y:177-179` @
/// Tempo v3.0.2):
///
/// * **ONE operand.** The production carries no `COMMA`, and
///   `fieldExpression` has none either, so `| by(.b, .c)` is a parse
///   error at the reference — measured `400`. We accepted it AND served
///   it, which is the worst shape an accept-surface divergence takes: the
///   query works, the user builds on it, and it is not portable to the
///   system we claim compatibility with. Withdrawn here, ledgered in
///   `docs/benchmarks/traces-differential-ledger.md`.
/// * **A FULL field expression.** `by(.b + .c)`, `by(-.b)`, `by(!.b)`,
///   `by((.b))` and `by(.b = 1)` are all reference `200`s that we
///   refused.
///
/// The METRICS `by(...)` is a different production sharing one keyword —
/// `attributeList` (`expr.y:195-198`) — and keeps its comma list; see
/// [`parse_optional_by`].
fn parse_by_stage(cursor: &mut Cursor<'_>) -> Result<PipelineStage, TraceQlError> {
    cursor.expect(&TokenKind::LParen, "'('")?;
    if matches!(cursor.peek().kind, TokenKind::RParen) {
        let span = cursor.peek().span;
        return Err(TraceQlError::UnexpectedToken {
            found: "')'".to_string(),
            expected: "a grouping key (by() requires exactly one field expression)".to_string(),
            span,
        });
    }
    let mut binary_nodes = 0usize;
    let key = parse_field_expr(cursor, 0, &mut binary_nodes)?;
    cursor.expect(&TokenKind::RParen, "')'")?;
    Ok(PipelineStage::By { key })
}

/// `Coalesce := "coalesce" "(" ")"` (issue #185, `pipeline.coalesce`):
/// zero-arity spanset-array merge. A stray argument is a positioned error.
fn parse_coalesce_stage(cursor: &mut Cursor<'_>) -> Result<PipelineStage, TraceQlError> {
    cursor.expect(&TokenKind::LParen, "'('")?;
    if matches!(cursor.peek().kind, TokenKind::RParen) {
        cursor.advance();
        return Ok(PipelineStage::Coalesce);
    }
    let tok = cursor.peek().clone();
    if matches!(tok.kind, TokenKind::Eof) {
        return Err(TraceQlError::UnexpectedEof {
            expected: "')' (coalesce() takes no argument)".to_string(),
            span: tok.span,
        });
    }
    Err(TraceQlError::UnexpectedToken {
        found: describe(&tok.kind),
        expected: "')' (coalesce() takes no argument)".to_string(),
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

/// `count() Cmp Value` (zero-arity) or `avg|sum|min|max(FieldExpr) Cmp
/// Value` (one-arity) — every malformed arity is a positioned error
/// (plan v2 F5).
///
/// **The argument is a full field expression** (issue #335 Stage C, D7).
/// The reference parses an ordinary operand there and decides legality
/// in its validator: `avg(span:childCount)`, `avg(trace:duration)`,
/// `avg(.a + 1)` and `avg((.a))` are all measured 200s against the
/// pinned digest, while `avg(1)` and `avg("x")` are 400s whose messages
/// name the parsed subexpression and carry no position — the semantic
/// signature. So the intrinsic blocklist that used to live here is gone;
/// `validate.rs` rule 11 holds the same rejections, and the search
/// planner holds the shapes it cannot execute.
///
/// The argument gets a FRESH recursion/`&&`-budget, like `compare()`'s
/// inner filter: it is a self-contained expression, not a continuation
/// of the spanset filter's.
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
            let mut inner_nodes = 0usize;
            let expr = parse_field_expr(cursor, 0, &mut inner_nodes)?;
            cursor.expect(&TokenKind::RParen, "')'")?;
            Some(expr)
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

/// The ROOT hint position, where a query may carry MORE THAN ONE
/// `with(...)` clause (issue #335 Stage D1, class D23).
///
/// `root: root hints` (`pkg/traceql/expr.y:134` @ Tempo v3.0.2) is
/// left-recursive, so `{ .a = 1 } with(a=1) with(b=2)` is a legal root
/// query at the reference and a 200 there, while our single call read the
/// second clause as trailing input and answered 400.
///
/// **The LAST clause wins; clauses do NOT concatenate.** The reference's
/// action is `yylex.(*lexer).expr.withHints($2)` and `withHints` is
/// `r.Hints = h` — a plain assignment, not an append
/// (`pkg/traceql/ast.go:112-115` @ v3.0.2). So the recursion's effect is
/// replacement: `with(a=1) with(b=2)` means `with(b=2)`, and only a
/// single clause's comma list (`hintList`, `expr.y:379-382`) accumulates.
/// This is worth stating because the intuitive reading — that repeated
/// clauses merge — is wrong, and shipping it would be a quiet semantic
/// divergence behind an accept-surface fix that looked complete.
///
/// Only this call site loops. The metric-stage and `compare()` hint
/// positions keep their single-clause shape: their grammar
/// (`metricsAggregation`, `expr.y:307-327`) has no such recursion, and
/// `| rate() with(a=1) with(b=2)` is accepted by both sides through the
/// existing single call because the trailing clause is the ROOT's.
fn parse_root_hints(cursor: &mut Cursor<'_>) -> Result<Vec<MetricHint>, TraceQlError> {
    let mut hints = Vec::new();
    loop {
        let clause = parse_optional_with(cursor)?;
        if clause.is_empty() {
            // `parse_optional_with` returns empty only when no `with`
            // follows — an empty `with()` is a positioned error there, so
            // this cannot loop forever on a well-formed clause.
            return Ok(hints);
        }
        hints = clause;
    }
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

/// What a hint value may be, in one place because three arms report it.
/// The list follows `static` (`expr.y:426-453` @ Tempo v3.0.2), which is
/// what the reference's `hint` production takes.
const HINT_VALUE_EXPECTED: &str =
    "a hint value (a status or kind keyword, true, false, a number, a duration, or a string)";

/// `Hint := Ident "=" Static` — the value is the whole `static`
/// production (`expr.y:371-373` @ Tempo v3.0.2), not a shorter list.
fn parse_hint(cursor: &mut Cursor<'_>) -> Result<MetricHint, TraceQlError> {
    let (key, _) = cursor.expect_ident("a hint name (e.g. sample, exemplars)")?;
    cursor.expect(&TokenKind::Eq, "'=' (hints are key=value pairs)")?;
    let tok = cursor.peek().clone();
    let value = match &tok.kind {
        // `hint: IDENTIFIER EQ static` (`expr.y:371-373` @ Tempo v3.0.2):
        // the value position is the WHOLE `static` production, so it is
        // resolved by the same function the field-expression atom uses
        // rather than by a shorter hand-written list. Before issue #335
        // Stage D1 this arm knew `true`/`false` only, which is why
        // `with(k=unspecified)` and `with(k=maxInt)` were 400s here and
        // 200s at the reference — and why `with(k=server)` was too,
        // though no probe happened to name it.
        TokenKind::Ident(word) => match static_value_of(word) {
            Some(Value::Bool(b)) => HintValue::Bool(b),
            Some(Value::Number(raw)) => HintValue::Number(raw),
            Some(Value::Status(s)) => HintValue::Status(s),
            Some(Value::Kind(k)) => HintValue::Kind(k),
            // `static_value_of` returns only the four kinds above.
            Some(Value::String(_) | Value::Duration(_)) | None => {
                return Err(TraceQlError::UnexpectedToken {
                    found: describe(&tok.kind),
                    expected: HINT_VALUE_EXPECTED.to_string(),
                    span: tok.span,
                });
            }
        },
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
                expected: HINT_VALUE_EXPECTED.to_string(),
                span: tok.span,
            });
        }
        _ => {
            return Err(TraceQlError::UnexpectedToken {
                found: describe(&tok.kind),
                expected: HINT_VALUE_EXPECTED.to_string(),
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
                body:
                    Some(FieldExpr::Binary {
                        op: FieldOp::Cmp(_),
                        lhs,
                        ..
                    }),
            }) => match *lhs {
                FieldExpr::Field(field) => field,
                other => panic!("{q}: expected a field LHS, got {other:?}"),
            },
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
        // An unknown colon field stays GENERIC, never a NotYetSupported named
        // boundary. (instrumentation:, event: and link: now resolve their
        // known fields — see instrumentation_scope_constructs_parse /
        // event_scope_constructs_parse / link_scope_constructs_parse; an
        // unknown field under any of them, like `link:bogus`, stays generic.)
        for q in ["{ link:bogus = \"x\" }", "{ span:bogus > 1 }"] {
            match parse(q) {
                Err(TraceQlError::NotYetSupported { .. }) => {
                    panic!("{q}: must be a generic error, not a named boundary")
                }
                Err(_) => {}
                Ok(ast) => panic!("{q}: must not parse, got {ast:?}"),
            }
        }
    }

    /// Issue #192 (PR-A): the three instrumentation-scope constructs parse —
    /// the `scope.instrumentation` attribute namespace plus the
    /// `instrumentation:name`/`instrumentation:version` intrinsics.
    #[test]
    fn instrumentation_scope_constructs_parse() {
        assert_eq!(
            only_field(r#"{ instrumentation.name = "otel" }"#),
            Field::Attribute {
                scope: AttrScope::Instrumentation,
                key: "name".to_string(),
            },
        );
        assert_eq!(
            only_field(r#"{ instrumentation:name = "otel" }"#),
            Field::Intrinsic(Intrinsic::InstrumentationName),
        );
        assert_eq!(
            only_field(r#"{ instrumentation:version = "1.4.2" }"#),
            Field::Intrinsic(Intrinsic::InstrumentationVersion),
        );
    }

    /// Issue #192 (PR-B): the three span-event constructs parse — the
    /// `scope.event` attribute namespace plus the `event:name`/
    /// `event:timeSinceStart` intrinsics (a string and a duration).
    #[test]
    fn event_scope_constructs_parse() {
        assert_eq!(
            only_field(r#"{ event.exception.type = "IOError" }"#),
            Field::Attribute {
                scope: AttrScope::Event,
                key: "exception.type".to_string(),
            },
        );
        assert_eq!(
            only_field(r#"{ event:name = "exception" }"#),
            Field::Intrinsic(Intrinsic::EventName),
        );
        assert_eq!(
            only_field(r#"{ event:timeSinceStart > 1ms }"#),
            Field::Intrinsic(Intrinsic::EventTimeSinceStart),
        );
    }

    /// Issue #192 (PR-C): the three span-link constructs parse — the
    /// `scope.link` attribute namespace plus the `link:spanID`/`link:traceID`
    /// intrinsics (both lowercase-hex id strings).
    #[test]
    fn link_scope_constructs_parse() {
        assert_eq!(
            only_field(r#"{ link.relation = "child_of" }"#),
            Field::Attribute {
                scope: AttrScope::Link,
                key: "relation".to_string(),
            },
        );
        assert_eq!(
            only_field(r#"{ link:spanID = "0a1b2c3d4e5f6071" }"#),
            Field::Intrinsic(Intrinsic::LinkSpanId),
        );
        assert_eq!(
            only_field(r#"{ link:traceID = "000102030405060708090a0b0c0d0e0f" }"#),
            Field::Intrinsic(Intrinsic::LinkTraceId),
        );
    }

    /// Issue #184 AC9 asserted that aggregating any trace-level or
    /// scoped intrinsic is a PARSE error. Issue #335 Stage C (D7) moves
    /// that decision to the validator, because the reference makes it
    /// there — and the reference's answer is not uniform across the
    /// group: `max(span:childCount)` and `min(traceDuration)` are 200s
    /// (numeric), `avg(rootName)` and friends are 400s (string). A parse
    /// rejection could not tell them apart, which is why the rule had to
    /// move rather than be re-tuned.
    ///
    /// The parse side keeps a claim of its own: the argument PARSES,
    /// carrying the field expression the validator then judges.
    #[test]
    fn every_intrinsic_aggregate_argument_now_parses_for_the_validator_to_judge() {
        for (q, want) in [
            (
                r#"{} | avg(rootName) > 1"#,
                Field::Intrinsic(Intrinsic::RootName),
            ),
            (
                r#"{} | sum(statusMessage) > 1"#,
                Field::Intrinsic(Intrinsic::StatusMessage),
            ),
            (
                r#"{} | max(span:childCount) > 1"#,
                Field::Intrinsic(Intrinsic::ChildCount),
            ),
            (
                r#"{} | min(traceDuration) > 1s"#,
                Field::Intrinsic(Intrinsic::TraceDuration),
            ),
            (
                r#"{} | avg(rootServiceName) > 1"#,
                Field::Intrinsic(Intrinsic::RootServiceName),
            ),
        ] {
            let ast = parse(q).unwrap_or_else(|e| panic!("{q}: must parse now, got {e}"));
            let [PipelineStage::Aggregate { field, .. }] = ast.pipeline.as_slice() else {
                panic!("{q}: expected one aggregate stage, got {:?}", ast.pipeline);
            };
            assert_eq!(
                field.as_ref(),
                Some(&FieldExpr::Field(want.clone())),
                "{q}: the argument must reach the validator intact"
            );
        }
        // A missing comparison is still a positioned parse error — the
        // aggregate's trailing `cmp value` is untouched by Stage C.
        assert!(matches!(
            parse("{} | avg(rootName)"),
            Err(TraceQlError::UnexpectedEof { .. })
        ));
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

    /// Only the RIGHT-associative RHS charges `depth`, and the asymmetry
    /// is load-bearing in BOTH directions. This test fails if the
    /// condition is dropped either way, so it cannot be satisfied by
    /// "charge always" or "charge never".
    ///
    /// - `^` re-enters the climb at its OWN binding power, so a paren-free
    ///   `2^2^2^…` recurses once per operator. Uncharged it would build a
    ///   right spine as long as the input and overflow the stack in
    ///   `Display`/`Drop`; it must error cleanly instead.
    /// - A left-associative RHS re-enters one level tighter, so it returns
    ///   after an atom: charging it costs a CONSTANT (one per operator on
    ///   the path, not one per chain element), which nonetheless eats the
    ///   nesting headroom. Measured against the pre-collapse parser at
    ///   `61dea2f`, the paren boundary is 63 accepted / 64 rejected;
    ///   charging the left RHS moves it to 61 / 62 and silently rejects
    ///   two nesting levels that used to parse.
    ///
    /// A flat left-associative chain is a POOR witness for the second leg
    /// and was tried first: `2 * 2 * … * 2` is iterative, so it parses
    /// under both variants. The boundary below is the discriminating one.
    #[test]
    fn only_the_right_associative_rhs_charges_the_depth_guard() {
        let pow_chain = format!("{{ .a = 2{} }}", " ^ 2".repeat(MAX_DEPTH + 2));
        assert!(
            matches!(
                parse(&pow_chain),
                Err(TraceQlError::RecursionLimitExceeded { .. })
            ),
            "a paren-free `^` chain recurses per operator and must be bounded"
        );

        let nested = |n: usize| format!("{{ {}.a = 1 && .b = 2{} }}", "(".repeat(n), ")".repeat(n));
        assert!(
            parse(&nested(MAX_DEPTH - 1)).is_ok(),
            "MAX_DEPTH - 1 paren levels parsed before the collapse and must still parse"
        );
        assert!(
            matches!(
                parse(&nested(MAX_DEPTH)),
                Err(TraceQlError::RecursionLimitExceeded { .. })
            ),
            "the boundary must not move outward either"
        );
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
                    Some(FieldExpr::Binary {
                        op: FieldOp::Cmp(_),
                        lhs,
                        ..
                    }),
            }) => match lhs.as_ref() {
                FieldExpr::Field(Field::Attribute { key, .. }) => key.as_str(),
                other => panic!("expected an attribute LHS, got {other:?}"),
            },
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

    /// Issue #335 classes D10/D11: `&&` and `||` are ONE precedence level,
    /// left-associative, at both the field and the spanset level — so the
    /// bare form must parse identically to the reference's parenthesised
    /// reading, and NOT to the `&&`-binds-tighter reading we had.
    #[test]
    fn and_and_or_share_one_left_associative_precedence_level() {
        for (bare, reference_reading, old_wrong_reading) in [
            (
                "{ .a = 1 || .b = 2 && .c = 3 }",
                "{ ((.a = 1) || (.b = 2)) && (.c = 3) }",
                "{ (.a = 1) || ((.b = 2) && (.c = 3)) }",
            ),
            (
                "{ .a = 1 } || { .b = 2 } && { .c = 3 }",
                "(({ .a = 1 } || { .b = 2 }) && { .c = 3 })",
                "({ .a = 1 } || ({ .b = 2 } && { .c = 3 }))",
            ),
            (
                "{ .a = 1 && .b = 2 || .c = 3 }",
                "{ ((.a = 1) && (.b = 2)) || (.c = 3) }",
                "{ ((.a = 1) && (.b = 2)) || (.c = 3) }",
            ),
        ] {
            let got = parse(bare).unwrap_or_else(|e| panic!("{bare:?}: {e}"));
            let want = parse(reference_reading).unwrap();
            assert_eq!(got, want, "{bare:?} must group like {reference_reading:?}");
            if reference_reading != old_wrong_reading {
                assert_ne!(
                    got,
                    parse(old_wrong_reading).unwrap(),
                    "{bare:?} must NOT group like {old_wrong_reading:?}"
                );
            }
        }
    }

    /// Issue #335 class D8: `^` is **RIGHT**-associative — `2 ^ 3 ^ 2`
    /// groups as `2 ^ (3 ^ 2)`. Pinned as a tree, not a number: this
    /// crate does not evaluate.
    ///
    /// **Established structurally, not from the value.** The reference
    /// folds `2^3` to 9 and `3^2` to 8, so the candidate groupings reduce
    /// to different single operations: `2 ^ 3 ^ 2` measures 64, `9 ^ 2`
    /// (left's second step) measures 512, `2 ^ 8` (right's second step)
    /// measures 64. The three-term form equals `2 ^ 8`, so it groups
    /// right — a conclusion that needs no model of what `^` computes.
    ///
    /// This test previously asserted LEFT associativity, derived from the
    /// reference value 64 alone. That derivation was wrong: the reference
    /// reaches 64 by right grouping combined with an operand-swapping
    /// integer `^` (ledger `traceql-pow-integer-operand-swap`), i.e. two
    /// errors cancelling. **A value can only ever pin a value.**
    #[test]
    fn pow_is_right_associative() {
        let got = parse("{ .a = 2 ^ 3 ^ 2 }").unwrap();
        assert_eq!(got, parse("{ .a = (2 ^ (3 ^ 2)) }").unwrap());
        assert_ne!(got, parse("{ .a = ((2 ^ 3) ^ 2) }").unwrap());
    }

    /// Issue #335 class D9: unary `-` binds LOOSER than `^` and `* / %`
    /// but tighter than `+ -`, so `-2 ^ 2` is `-(2 ^ 2)` (= -4), not
    /// `(-2) ^ 2` (= 4).
    #[test]
    fn unary_minus_binds_between_the_arithmetic_levels() {
        for (bare, tighter_level) in [
            ("{ .a = -2 ^ 2 }", "{ .a = -(2 ^ 2) }"),
            ("{ .a = -.b * 2 }", "{ .a = -(.b * 2) }"),
            ("{ .a = -.b / 2 }", "{ .a = -(.b / 2) }"),
            ("{ .a = -.b % 2 }", "{ .a = -(.b % 2) }"),
        ] {
            assert_eq!(
                parse(bare).unwrap(),
                parse(tighter_level).unwrap(),
                "{bare:?} must absorb the tighter arithmetic level"
            );
        }
        // …but `+`/`-` stay OUTSIDE the negation.
        for (bare, looser_level) in [
            ("{ .a = -2 + 3 }", "{ .a = (-2) + 3 }"),
            ("{ .a = -.b - 3 }", "{ .a = (-.b) - 3 }"),
        ] {
            assert_eq!(
                parse(bare).unwrap(),
                parse(looser_level).unwrap(),
                "{bare:?} must not absorb the looser arithmetic level"
            );
        }
    }

    /// Issue #335 class D2: every colon-scoped intrinsic is an ordinary
    /// operand, so all eighteen are legal as a comparison right-hand side
    /// (they were already legal on the left).
    #[test]
    fn every_colon_scoped_intrinsic_is_accepted_as_a_comparison_rhs() {
        for scoped in [
            "span:name",
            "span:duration",
            "span:status",
            "span:kind",
            "span:statusMessage",
            "span:childCount",
            "span:id",
            "span:parentID",
            "trace:id",
            "trace:duration",
            "trace:rootName",
            "trace:rootService",
            "instrumentation:name",
            "instrumentation:version",
            "event:name",
            "event:timeSinceStart",
            "link:spanID",
            "link:traceID",
        ] {
            let q = format!("{{ .a = {scoped} }}");
            let parsed = parse(&q).unwrap_or_else(|e| panic!("{q:?}: {e}"));
            assert!(
                matches!(
                    &parsed.spanset,
                    SpansetExpr::Filter(SpansetFilter {
                        body: Some(FieldExpr::Binary {
                            op: FieldOp::Cmp(_),
                            ..
                        }),
                    })
                ),
                "{q:?} must compare against an intrinsic RHS, got {parsed:?}"
            );
        }
        // Arithmetic and the other comparison operators reach it too.
        assert!(parse("{ .a >= span:duration }").is_ok());
        assert!(parse("{ .a = span:duration + 1 }").is_ok());
    }

    /// The predicate widened for D2 recognises only KNOWN colon pairs, so
    /// an unknown scope or field keeps its previous positioned error
    /// instead of being routed into the field parser.
    #[test]
    fn an_unknown_colon_pair_on_the_rhs_keeps_its_value_position_error() {
        for q in [
            r#"{ .a = foo:bar }"#,
            r#"{ .a = span:nope }"#,
            r#"{ .a = trace:childCount }"#,
        ] {
            let err = parse(q).unwrap_err();
            assert!(
                matches!(err, TraceQlError::UnexpectedToken { .. }),
                "{q:?} -> {err}"
            );
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
                body:
                    Some(FieldExpr::Binary {
                        op: FieldOp::Cmp(op),
                        ..
                    }),
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
                    body: Some(FieldExpr::Unary {
                        op: UnaryOp::Not,
                        ..
                    })
                })
            ));
            assert_eq!(parse(&parsed.to_string()).unwrap(), parsed, "{query}");
        }
        for (query, want) in [("{ true }", true), ("{ false }", false)] {
            let parsed = parse(query).unwrap();
            assert_eq!(
                parsed.spanset,
                SpansetExpr::Filter(SpansetFilter {
                    body: Some(FieldExpr::Literal(Value::Bool(want)))
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
                    body:
                        Some(FieldExpr::Binary {
                            op: FieldOp::Cmp(_),
                            rhs,
                            ..
                        }),
                }) if matches!(rhs.as_ref(), FieldExpr::Field(_)) => {}
                other => panic!("{query} -> expected a field-vs-field compare, got {other:?}"),
            }
            assert_eq!(parse(&parsed.to_string()).unwrap(), parsed, "{query}");
        }
        // A regex against a field RHS is still rejected, by the
        // VALIDATOR since the Stage B collapse — one operand grammar
        // cannot know which operator it is feeding, so the rule moved to
        // where the operator and the operand are both in scope. Measured
        // reference: 400 `invalid type for =~ or !~: .b`.
        let ast = parse(r#"{ .a =~ .b }"#).expect("parses after the collapse");
        assert_eq!(
            crate::validate(&ast).unwrap_err().rule_id(),
            "invalid-regex-operand"
        );
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
    fn a_bare_attribute_parses_to_an_existence_check() {
        // `existence.bare_attr` (issue #185): a bare attribute is now an
        // existence check, not a named boundary.
        let parsed = parse("{ .foo }").unwrap();
        assert_eq!(
            parsed.spanset,
            SpansetExpr::Filter(SpansetFilter {
                body: Some(FieldExpr::Field(Field::Attribute {
                    scope: AttrScope::Unscoped,
                    key: "foo".to_string(),
                })),
            })
        );
        assert_eq!(parse(&parsed.to_string()).unwrap(), parsed);
    }

    #[test]
    fn nil_comparisons_parse_to_existence_and_absence() {
        // `!= nil` ⇒ presence (Exists); `= nil` ⇒ absence (Not(Exists)).
        let present = parse("{ .a != nil }").unwrap();
        assert_eq!(
            present.spanset,
            SpansetExpr::Filter(SpansetFilter {
                body: Some(FieldExpr::Exists {
                    field: Field::Attribute {
                        scope: AttrScope::Unscoped,
                        key: "a".to_string(),
                    },
                    negated: false,
                }),
            })
        );
        assert_eq!(parse(&present.to_string()).unwrap(), present);
        let absent = parse("{ .a = nil }").unwrap();
        assert!(matches!(
            absent.spanset,
            SpansetExpr::Filter(SpansetFilter {
                body: Some(FieldExpr::Exists { negated: true, .. }),
            })
        ));
        assert_eq!(parse(&absent.to_string()).unwrap(), absent);
    }

    #[test]
    fn arithmetic_comparisons_parse_and_round_trip() {
        for query in [
            "{ .a = 1 + 2 }",
            "{ .a = 2 - 1 }",
            "{ .a = 2 * 3 }",
            "{ .a = 4 / 2 }",
            "{ .a = 5 % 2 }",
            "{ .a = 2 ^ 3 }",
            "{ .a = -1 }",
            "{ duration * 2 > 1s }",
        ] {
            let parsed = parse(query).unwrap_or_else(|e| panic!("{query}: {e}"));
            match &parsed.spanset {
                SpansetExpr::Filter(SpansetFilter {
                    body:
                        Some(FieldExpr::Binary {
                            op: FieldOp::Cmp(_),
                            lhs,
                            rhs,
                        }),
                }) if matches!(
                    lhs.as_ref(),
                    FieldExpr::Binary {
                        op: FieldOp::Arith(_),
                        ..
                    } | FieldExpr::Unary {
                        op: UnaryOp::Neg,
                        ..
                    }
                ) || matches!(
                    rhs.as_ref(),
                    FieldExpr::Binary {
                        op: FieldOp::Arith(_),
                        ..
                    } | FieldExpr::Unary {
                        op: UnaryOp::Neg,
                        ..
                    }
                ) => {}
                other => panic!("{query}: expected an arithmetic compare, got {other:?}"),
            }
            assert_eq!(parse(&parsed.to_string()).unwrap(), parsed, "{query}");
        }
    }

    #[test]
    fn quoted_and_bracketed_attribute_keys_parse_and_round_trip() {
        for (query, key) in [
            (r#"{ span."attr with spaces" = 1 }"#, "attr with spaces"),
            (r#"{ span.["foo bar"] = "x" }"#, "foo bar"),
            (r#"{ .["a b"] = "x" }"#, "a b"),
        ] {
            let field = only_field(query);
            match field {
                Field::Attribute { key: got, .. } => assert_eq!(got, key, "{query}"),
                other => panic!("{query}: expected an attribute, got {other:?}"),
            }
            let parsed = parse(query).unwrap();
            assert_eq!(parse(&parsed.to_string()).unwrap(), parsed, "{query}");
        }
    }

    #[test]
    fn spanset_by_and_coalesce_pipeline_stages_parse() {
        let by = parse("{ .a = 1 } | by(resource.service.name)").unwrap();
        assert!(matches!(by.pipeline[..], [PipelineStage::By { .. }]));
        assert_eq!(parse(&by.to_string()).unwrap(), by);
        let coalesce = parse("{ .a = 1 } | coalesce()").unwrap();
        assert_eq!(coalesce.pipeline, vec![PipelineStage::Coalesce]);
        assert_eq!(parse(&coalesce.to_string()).unwrap(), coalesce);
        // Empty by() stays a positioned error.
        assert!(matches!(
            parse("{ .a = 1 } | by()"),
            Err(TraceQlError::UnexpectedToken { .. })
        ));
    }

    #[test]
    fn a_trailing_with_on_a_spanset_carries_query_hints() {
        // `hints.most_recent` (issue #185): `{ … } with(most_recent=true)`.
        let parsed = parse("{ .a = 1 } with(most_recent=true)").unwrap();
        assert_eq!(
            parsed.hints,
            vec![MetricHint {
                key: "most_recent".to_string(),
                value: HintValue::Bool(true),
            }]
        );
        assert_eq!(parse(&parsed.to_string()).unwrap(), parsed);
    }

    /// A bare non-boolean body is REJECTED, and after the issue #335
    /// Stage B collapse the rejection is the validator's.
    ///
    /// It used to be a parse error ("expected a comparison operator"),
    /// which was our grammar's accident, not the reference's: the
    /// reference PARSES `{ name }` and then rejects it with `span filter
    /// field expressions must resolve to a boolean`. Same 400 on the wire,
    /// correct layer, and the message now says what is actually wrong.
    ///
    /// This lives in the parser's test module deliberately, next to the
    /// grammar it stopped being a rule of, so nobody re-adds the guard.
    #[test]
    fn a_bare_non_boolean_body_parses_and_the_validator_rejects_it() {
        for query in ["{ name }", "{ duration }", "{ status && .a = 1 }"] {
            let ast = parse(query).unwrap_or_else(|e| panic!("{query} must now parse: {e}"));
            let err = crate::validate(&ast).expect_err("must be rejected by the validator");
            assert!(
                matches!(
                    err.rule_id(),
                    "spanset-filter-not-boolean" | "type-mismatch"
                ),
                "{query} -> unexpected rule {}",
                err.rule_id()
            );
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
                    body:
                        Some(FieldExpr::Binary {
                            op: FieldOp::Cmp(_),
                            lhs,
                            rhs,
                        }),
                }) => {
                    assert_eq!(
                        **lhs,
                        FieldExpr::Field(Field::Intrinsic(intrinsic)),
                        "{query}"
                    );
                    assert!(
                        matches!(rhs.as_ref(), FieldExpr::Literal(Value::Number(_))),
                        "{query}"
                    );
                }
                other => panic!("{query} -> unexpected {other:?}"),
            }
            // Display round-trips through a reparse.
            let reparsed = parse(&parsed.to_string()).unwrap();
            assert_eq!(reparsed, parsed, "{query}");
        }
    }

    /// `{ nestedSetLeft =~ "x" }` — a regex against an int-typed intrinsic.
    ///
    /// Was a POSITIONED parse error (the old operand grammar knew the
    /// LHS's type and refused the string); is now the validator's
    /// positionless type rule, which is the reference's own signature for
    /// it: measured 400 `binary operations must operate on the same type:
    /// nestedSetLeft =~ `x``.
    #[test]
    fn nested_set_regex_string_is_a_validate_type_mismatch() {
        let ast = parse(r#"{ nestedSetLeft =~ "x" }"#).expect("parses after the collapse");
        assert_eq!(
            crate::validate(&ast).unwrap_err().rule_id(),
            "type-mismatch"
        );
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
    fn a_standalone_by_pipeline_stage_parses_to_the_by_stage() {
        // `pipeline.by` (issue #185): a top-level `| by(...)` is a spanset
        // grouping stage, distinct from the metric `by(...)` clause.
        let parsed = parse("{} | by(resource.service.name)").unwrap();
        assert_eq!(
            parsed.pipeline,
            vec![PipelineStage::By {
                key: FieldExpr::Field(Field::Attribute {
                    scope: AttrScope::Resource,
                    key: "service.name".to_string(),
                }),
            }]
        );
        assert_eq!(parse(&parsed.to_string()).unwrap(), parsed);
    }

    /// Issue #335 Stage D2, class D16. `groupOperation` is
    /// `BY '(' fieldExpression ')'` (`expr.y:177-179` @ Tempo v3.0.2):
    /// ONE operand, and a full field expression. Both halves are asserted
    /// here, because the old shape was wrong in both directions.
    #[test]
    fn the_spanset_by_takes_one_key_and_that_key_is_a_field_expression() {
        // Widened: every one of these is a reference 200 that we refused.
        for query in [
            "{ .a = 1 } | by(.b + .c)",
            "{ .a = 1 } | by(-.b)",
            "{ .a = 1 } | by(!.b)",
            "{ .a = 1 } | by((.b))",
            "{ .a = 1 } | by(.b = 1)",
        ] {
            let parsed = parse(query).unwrap_or_else(|e| panic!("{query}: {e}"));
            assert!(
                matches!(parsed.pipeline[..], [PipelineStage::By { .. }]),
                "{query}"
            );
            assert_eq!(parse(&parsed.to_string()).unwrap(), parsed, "{query}");
        }
        // Narrowed: the comma list is a reference parse error, and was a
        // shipped, SERVED accept here until this stage withdrew it.
        for query in ["{ .a = 1 } | by(.b, .c)", "{ .a = 1 } | by(.b, .c, name)"] {
            let err = parse(query).unwrap_err();
            assert!(
                matches!(err, TraceQlError::UnexpectedToken { .. }),
                "{query}: expected a positioned error, got {err}"
            );
        }
        // The METRICS by(...) is attributeList and keeps its comma list.
        let metrics = parse("{} | rate() by(.a, .b)").unwrap();
        assert!(matches!(metrics.pipeline[..], [PipelineStage::Metric(_)]));
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
    fn most_recent_is_the_only_recognized_but_arbitrary_hint_key() {
        // `hints.most_recent` (issue #185): the trailing `with(...)` accepts
        // any key=value pair; `most_recent=true` is the recognized use.
        let parsed = parse("{ .a = 1 } with(most_recent=true)").unwrap();
        assert_eq!(parsed.hints.len(), 1);
        assert_eq!(parsed.hints[0].key, "most_recent");
        // An empty with() is still a positioned error.
        assert!(matches!(
            parse("{ .a = 1 } with()"),
            Err(TraceQlError::UnexpectedToken { .. })
        ));
    }
}
