//! Recursive-descent parser over `&[Token]`. A `Cursor` tracks the
//! current position; a `depth` counter threaded through metric-expression
//! parsing guards against unbounded nesting (`sum(sum(sum(...)))`) —
//! [`crate::error::MAX_DEPTH`] levels return `RecursionLimitExceeded`
//! instead of overflowing the call stack. A second counter threaded
//! through the label-filter grammar guards its parenthesis recursion
//! (`| ((((x="1"))))`) the same way —
//! [`crate::error::LABEL_FILTER_MAX_DEPTH`] levels, issue #255. It bounds
//! **paren nesting only**: flat `or`/`and` chains are parsed iteratively
//! at depth 1 and are not counted.
//!
//! Before any of that, both public entry points run the #279 query-text
//! admission cap: input of [`crate::MAX_QUERY_BYTES`] (131,072) bytes or
//! more is rejected as `QueryTooLong` before tokenization, via the
//! [`crate::limits::CheckedQuery`] seam `lexer::tokenize` requires
//! (grafana/loki v3.7.4 `pkg/logql/syntax/parser.go:86`). Like the depth
//! guards, the cap is necessary but NOT sufficient for deep nesting —
//! 131,071 bytes of `(` is still far past the recursion limits above.
//!
//! Disambiguation of the overloaded `!=`/`!~` tokens (selector matcher,
//! line filter, or — `!=` only — a binary comparison) is purely
//! positional: the selector-matcher loop, the pipeline-stage loop, and
//! the binary-operator loop ([`peek_binop`], which runs only after a
//! complete metric primary) each own their token set, and none of them
//! overlap in when they run (architect plan amendments 1-3).

use crate::ast::{
    self, BinModifier, BinOp, CompareOp, DropKeepElem, Expr, Grouping, GroupingKind,
    LabelExtraction, LabelFilterExpr, LabelFmt, LabelMatch, LineFilter, LineFilterOp, LogExpr,
    LogRange, MatchOp, Matcher, MetricExpr, NumericLiteral, ParserStage, RangeAggOp, Stage,
    StreamSelector, Unwrap, VectorAggOp,
};
use crate::duration;
use crate::error::{LABEL_FILTER_MAX_DEPTH, LogQlError, MAX_DEPTH};
use crate::lexer;
use crate::limits::CheckedQuery;
use crate::token::{Span, Token, TokenKind};
use crate::walk;

/// Parses a full LogQL query into an [`Expr`] — the #11 planner contract.
pub fn parse(input: &str) -> Result<Expr, LogQlError> {
    let tokens = lexer::tokenize(CheckedQuery::new(input)?)?;
    let mut cursor = Cursor::new(&tokens);
    let expr = parse_expr(&mut cursor, 0)?;
    expect_eof(&cursor)?;
    Ok(expr)
}

/// Parses just a stream selector (`{label_matcher, ...}`) — the entry
/// point `/series` and `/label/{name}/values` (#13) use, since those
/// endpoints never see a full LogQL pipeline.
pub fn parse_selector(input: &str) -> Result<StreamSelector, LogQlError> {
    let tokens = lexer::tokenize(CheckedQuery::new(input)?)?;
    let mut cursor = Cursor::new(&tokens);
    let selector = parse_stream_selector(&mut cursor)?;
    expect_eof(&cursor)?;
    Ok(selector)
}

fn expect_eof(cursor: &Cursor<'_>) -> Result<(), LogQlError> {
    let tok = cursor.peek();
    if matches!(tok.kind, TokenKind::Eof) {
        Ok(())
    } else {
        Err(LogQlError::TrailingInput { span: tok.span })
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
    fn expect(&mut self, want: &TokenKind, expected: &str) -> Result<Token, LogQlError> {
        let tok = self.peek().clone();
        if std::mem::discriminant(&tok.kind) == std::mem::discriminant(want) {
            self.advance();
            Ok(tok)
        } else if matches!(tok.kind, TokenKind::Eof) {
            Err(LogQlError::UnexpectedEof {
                expected: expected.to_string(),
                span: tok.span,
            })
        } else {
            Err(LogQlError::UnexpectedToken {
                found: describe(&tok.kind),
                expected: expected.to_string(),
                span: tok.span,
            })
        }
    }

    fn expect_ident(&mut self) -> Result<(String, crate::token::Span), LogQlError> {
        let tok = self.peek().clone();
        match tok.kind {
            TokenKind::Ident(name) => {
                self.advance();
                Ok((name, tok.span))
            }
            TokenKind::Eof => Err(LogQlError::UnexpectedEof {
                expected: "an identifier".to_string(),
                span: tok.span,
            }),
            _ => Err(LogQlError::UnexpectedToken {
                found: describe(&tok.kind),
                expected: "an identifier".to_string(),
                span: tok.span,
            }),
        }
    }

    fn expect_string(&mut self) -> Result<(String, crate::token::Span), LogQlError> {
        let tok = self.peek().clone();
        match tok.kind {
            TokenKind::String(value) => {
                self.advance();
                Ok((value, tok.span))
            }
            TokenKind::Eof => Err(LogQlError::UnexpectedEof {
                expected: "a string".to_string(),
                span: tok.span,
            }),
            _ => Err(LogQlError::UnexpectedToken {
                found: describe(&tok.kind),
                expected: "a string".to_string(),
                span: tok.span,
            }),
        }
    }

    fn expect_duration(&mut self) -> Result<(String, crate::token::Span), LogQlError> {
        let tok = self.peek().clone();
        match tok.kind {
            TokenKind::Duration(raw) => {
                self.advance();
                Ok((raw, tok.span))
            }
            TokenKind::Eof => Err(LogQlError::UnexpectedEof {
                expected: "a duration (e.g. \"5m\")".to_string(),
                span: tok.span,
            }),
            _ => Err(LogQlError::UnexpectedToken {
                found: describe(&tok.kind),
                expected: "a duration (e.g. \"5m\")".to_string(),
                span: tok.span,
            }),
        }
    }

    fn expect_number(
        &mut self,
        expected: &str,
    ) -> Result<(String, crate::token::Span), LogQlError> {
        let tok = self.peek().clone();
        match tok.kind {
            TokenKind::Number(raw) => {
                self.advance();
                Ok((raw, tok.span))
            }
            TokenKind::Eof => Err(LogQlError::UnexpectedEof {
                expected: expected.to_string(),
                span: tok.span,
            }),
            _ => Err(LogQlError::UnexpectedToken {
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
        TokenKind::Eq => "'='".to_string(),
        TokenKind::Neq => "'!='".to_string(),
        TokenKind::Re => "'=~'".to_string(),
        TokenKind::Nre => "'!~'".to_string(),
        TokenKind::EqEq => "'=='".to_string(),
        TokenKind::Gt => "'>'".to_string(),
        TokenKind::Lt => "'<'".to_string(),
        TokenKind::Gte => "'>='".to_string(),
        TokenKind::Lte => "'<='".to_string(),
        TokenKind::Plus => "'+'".to_string(),
        TokenKind::Minus => "'-'".to_string(),
        TokenKind::Star => "'*'".to_string(),
        TokenKind::Slash => "'/'".to_string(),
        TokenKind::Percent => "'%'".to_string(),
        TokenKind::Caret => "'^'".to_string(),
        TokenKind::PipeExact => "'|='".to_string(),
        TokenKind::PipeMatch => "'|~'".to_string(),
        TokenKind::Pipe => "'|'".to_string(),
        TokenKind::Ident(s) => format!("identifier {s:?}"),
        TokenKind::Flag(s) => format!("flag \"--{s}\""),
        TokenKind::String(s) => format!("string {s:?}"),
        TokenKind::Duration(s) => format!("duration {s:?}"),
        TokenKind::Number(s) => format!("number {s:?}"),
        TokenKind::Eof => "end of query".to_string(),
    }
}

/// `Expr := LogExpr | MetricBinaryExpr`. A query starting with `{` is
/// always a log expression; a query starting with an identifier, a
/// number, or `(` is always a metric expression — an aggregation call, a
/// scalar literal, or a parenthesized/binary combination of those (issue
/// M6-10).
fn parse_expr(cursor: &mut Cursor<'_>, depth: usize) -> Result<Expr, LogQlError> {
    match &cursor.peek().kind {
        TokenKind::LBrace => Ok(Expr::Log(parse_log_expr(cursor)?)),
        TokenKind::Ident(_) | TokenKind::Number(_) | TokenKind::LParen => {
            let metric = parse_binary_expr(cursor, depth, 0, true)?;
            check_no_stray_filter_op(cursor)?;
            Ok(Expr::Metric(metric))
        }
        TokenKind::Eof => Err(LogQlError::UnexpectedEof {
            expected: "a stream selector or an aggregation function".to_string(),
            span: cursor.peek().span,
        }),
        _ => {
            let tok = cursor.peek().clone();
            Err(LogQlError::UnexpectedToken {
                found: describe(&tok.kind),
                expected: "a stream selector or an aggregation function".to_string(),
                span: tok.span,
            })
        }
    }
}

/// After a complete top-level metric expression, names a stray `!~` /
/// `|=` / `|~`: those are never binary operators in any LogQL milestone
/// (amendment 3) — a plain position-bearing `UnexpectedToken`, never
/// `NotYetSupported`.
fn check_no_stray_filter_op(cursor: &Cursor<'_>) -> Result<(), LogQlError> {
    let tok = cursor.peek();
    match &tok.kind {
        TokenKind::Nre | TokenKind::PipeExact | TokenKind::PipeMatch => {
            Err(LogQlError::UnexpectedToken {
                found: describe(&tok.kind),
                expected: "end of query".to_string(),
                span: tok.span,
            })
        }
        _ => Ok(()),
    }
}

/// Binary-operator recognition: `(op, precedence, right_assoc)` for the
/// token at cursor position, `None` when the token does not start a
/// binary operation. Precedence mirrors the upstream grammar: `or` <
/// `and`/`unless` < comparisons < `+ -` < `* / %` < `^` (right-
/// associative).
fn peek_binop(cursor: &Cursor<'_>) -> Option<(BinOp, u8, bool)> {
    let op = match &cursor.peek().kind {
        // The identifier-shaped operators come from the one recognition
        // table (`ast::BINARY_OP_KEYWORDS`) so the documented operator
        // inventory and the parser cannot drift.
        TokenKind::Ident(name) if ast::BINARY_OP_KEYWORDS.contains(&name.as_str()) => {
            match name.as_str() {
                "or" => BinOp::Or,
                "and" => BinOp::And,
                "unless" => BinOp::Unless,
                _ => return None,
            }
        }
        TokenKind::Ident(_) => return None,
        TokenKind::EqEq => BinOp::Eq,
        TokenKind::Neq => BinOp::Neq,
        TokenKind::Gt => BinOp::Gt,
        TokenKind::Gte => BinOp::Gte,
        TokenKind::Lt => BinOp::Lt,
        TokenKind::Lte => BinOp::Lte,
        TokenKind::Plus => BinOp::Add,
        TokenKind::Minus => BinOp::Sub,
        TokenKind::Star => BinOp::Mul,
        TokenKind::Slash => BinOp::Div,
        TokenKind::Percent => BinOp::Mod,
        TokenKind::Caret => BinOp::Pow,
        _ => return None,
    };
    let (prec, right_assoc) = match op {
        BinOp::Or => (1, false),
        BinOp::And | BinOp::Unless => (2, false),
        BinOp::Eq | BinOp::Neq | BinOp::Gt | BinOp::Gte | BinOp::Lt | BinOp::Lte => (3, false),
        BinOp::Add | BinOp::Sub => (4, false),
        BinOp::Mul | BinOp::Div | BinOp::Mod => (5, false),
        BinOp::Pow => (6, true),
    };
    Some((op, prec, right_assoc))
}

/// Precedence-climbing binary-operation layer over metric primaries
/// (issue M6-10). After each operator, the `bool` modifier is accepted on
/// comparisons only, followed by the optional vector-matching clause
/// (`on`/`ignoring` with an optional `group_left`/`group_right`), issue
/// #91.
///
/// `allow_variants` is the positional `variants(...)` acceptance rule
/// (issue #221): the reference's `variantsExpr` is an alternative of
/// `expr` (not of `metricExpr`), and `binOpExpr` is `expr OP expr` — so
/// `variants(...)` is legal exactly at the top level and in any operand
/// position of a TOP-LEVEL binary chain, and illegal inside `( … )`,
/// inside a vector aggregation, and inside another `variants(...)`
/// argument list. This flag is that rule; there is no other mechanism.
fn parse_binary_expr(
    cursor: &mut Cursor<'_>,
    depth: usize,
    min_prec: u8,
    allow_variants: bool,
) -> Result<MetricExpr, LogQlError> {
    if depth >= MAX_DEPTH {
        return Err(LogQlError::RecursionLimitExceeded {
            span: cursor.peek().span,
            limit: MAX_DEPTH,
        });
    }
    let mut lhs = parse_metric_primary(cursor, depth, allow_variants)?;
    while let Some((op, prec, right_assoc)) = peek_binop(cursor) {
        if prec < min_prec {
            break;
        }
        cursor.advance();
        let modifier = parse_bin_modifier(cursor, op)?;
        let next_min = if right_assoc { prec } else { prec + 1 };
        let rhs = parse_binary_expr(cursor, depth + 1, next_min, allow_variants)?;
        lhs = MetricExpr::Binary {
            op,
            modifier,
            lhs: walk::Child::new(lhs),
            rhs: walk::Child::new(rhs),
        };
    }
    Ok(lhs)
}

/// Parses the optional modifier(s) after a binary operator: an optional
/// `bool` (comparison operators only), then an optional vector-matching
/// clause `("on"|"ignoring") "(" labels ")"` with an optional trailing
/// `("group_left"|"group_right") ("(" labels ")")?` (issue #91). Returns
/// `None` when neither is present (byte-identical to the pre-#91
/// clause-free path). A `group_left`/`group_right` with no preceding
/// `on`/`ignoring` is a positional parse error — oracle-pinned: Loki
/// rejects it HTTP 400.
fn parse_bin_modifier(
    cursor: &mut Cursor<'_>,
    op: BinOp,
) -> Result<Option<BinModifier>, LogQlError> {
    let mut return_bool = false;
    if let TokenKind::Ident(name) = &cursor.peek().kind
        && name == "bool"
        && op.is_comparison()
    {
        cursor.advance();
        return_bool = true;
    }

    let matching = parse_vector_matching(cursor)?;
    if !return_bool && matching.is_none() {
        return Ok(None);
    }
    Ok(Some(BinModifier {
        return_bool,
        matching,
    }))
}

/// Parses an optional `("on"|"ignoring") "(" labels ")"` clause plus an
/// optional trailing `group_left`/`group_right` grouping (issue #91). A
/// bare `group_left`/`group_right` (no `on`/`ignoring` first) is rejected
/// as an unexpected token — matching the oracle's HTTP 400.
fn parse_vector_matching(
    cursor: &mut Cursor<'_>,
) -> Result<Option<ast::VectorMatching>, LogQlError> {
    let on = match &cursor.peek().kind {
        TokenKind::Ident(name) if name == "on" => true,
        TokenKind::Ident(name) if name == "ignoring" => false,
        TokenKind::Ident(name) if name == "group_left" || name == "group_right" => {
            let tok = cursor.peek();
            return Err(LogQlError::UnexpectedToken {
                found: format!("identifier {name:?}"),
                expected: "'on' or 'ignoring' before a grouping modifier".to_string(),
                span: tok.span,
            });
        }
        _ => return Ok(None),
    };
    cursor.advance();
    let labels = parse_label_list_parens(cursor)?;

    let group = match &cursor.peek().kind {
        TokenKind::Ident(name) if name == "group_left" || name == "group_right" => {
            let left = name == "group_left";
            cursor.advance();
            // The include-label list is optional: `group_left` or
            // `group_left(a, b)`.
            let includes = if matches!(cursor.peek().kind, TokenKind::LParen) {
                parse_label_list_parens(cursor)?
            } else {
                Vec::new()
            };
            Some(if left {
                ast::MatchGroup::Left(includes)
            } else {
                ast::MatchGroup::Right(includes)
            })
        }
        _ => None,
    };

    Ok(Some(ast::VectorMatching { on, labels, group }))
}

/// Parses a parenthesized comma-separated label-name list `"(" (ident
/// ("," ident)*)? ")"` — the shape shared by `on`/`ignoring`,
/// `group_left`/`group_right`, and `by`/`without` grouping. An empty list
/// `()` is allowed (`on()`).
fn parse_label_list_parens(cursor: &mut Cursor<'_>) -> Result<Vec<String>, LogQlError> {
    cursor.expect(&TokenKind::LParen, "'('")?;
    let mut labels = Vec::new();
    if !matches!(cursor.peek().kind, TokenKind::RParen) {
        loop {
            let (label, _) = cursor.expect_ident()?;
            labels.push(label);
            if matches!(cursor.peek().kind, TokenKind::Comma) {
                cursor.advance();
                continue;
            }
            break;
        }
    }
    cursor.expect(&TokenKind::RParen, "')'")?;
    Ok(labels)
}

/// A metric-expression primary: a parenthesized binary expression, a bare
/// scalar number literal, or an aggregation call. The `LParen` arm drops
/// `allow_variants` (issue #221): `(variants(…) of (…))` is a reference
/// 400 (`variantsExpr` is not a `metricExpr`, so it cannot parenthesize).
fn parse_metric_primary(
    cursor: &mut Cursor<'_>,
    depth: usize,
    allow_variants: bool,
) -> Result<MetricExpr, LogQlError> {
    match &cursor.peek().kind {
        TokenKind::LParen => {
            cursor.advance();
            let inner = parse_binary_expr(cursor, depth + 1, 0, false)?;
            cursor.expect(&TokenKind::RParen, "')'")?;
            Ok(inner)
        }
        TokenKind::Number(raw) => {
            let raw = raw.clone();
            cursor.advance();
            Ok(MetricExpr::Literal(raw))
        }
        _ => parse_metric_expr(cursor, depth, allow_variants),
    }
}

/// `LogExpr := StreamSelector (Stage)*` — the stage loop is greedy: line
/// filters chain with no separator (`{app="x"} |= "a" != "b" !~ "c"`);
/// a bare `|` introduces a parser stage, label filter, `line_format`,
/// `label_format`, or `unwrap` (issue M6-09). Any other token at stage
/// position ends the loop and control returns to the caller.
///
/// **Post-`unwrap` grammar rule (plan v3 delta 1):** the LogQL pipeline
/// allows only label filters after `unwrap` — a parser/format/line-filter
/// stage there is an `UnexpectedToken` naming the rule, so the invalid
/// ordering is unrepresentable in a parsed pipeline.
fn parse_log_expr(cursor: &mut Cursor<'_>) -> Result<LogExpr, LogQlError> {
    let selector = parse_stream_selector(cursor)?;
    let mut pipeline = Vec::new();
    let mut saw_unwrap = false;
    loop {
        let stage_span = cursor.peek().span;
        let line_filter_op = match &cursor.peek().kind {
            TokenKind::PipeExact => Some(LineFilterOp::Contains),
            TokenKind::Neq => Some(LineFilterOp::NotContains),
            TokenKind::PipeMatch => Some(LineFilterOp::Regex),
            TokenKind::Nre => Some(LineFilterOp::NotRegex),
            _ => None,
        };
        if let Some(op) = line_filter_op {
            if saw_unwrap {
                return Err(post_unwrap_stage_error(
                    describe(&cursor.peek().kind),
                    stage_span,
                ));
            }
            cursor.advance();
            let (value, value_is_ip) = parse_line_match(cursor, op)?;
            // `or`-chained alternatives (M8-LQ2 `linefilter.or`): greedily
            // consume `or <line-match>` — a line-level `or` only ever
            // follows a line-match value, so it never collides with the
            // metric-level `or` binary op (which appears after the range
            // closes, outside this log expression).
            let mut or_matches = Vec::new();
            while matches!(&cursor.peek().kind, TokenKind::Ident(n) if n == "or") {
                cursor.advance();
                let (v, is_ip) = parse_line_match(cursor, op)?;
                or_matches.push(ast::LineMatch { value: v, is_ip });
            }
            pipeline.push(Stage::LineFilter(LineFilter {
                op,
                value,
                value_is_ip,
                or_matches,
            }));
            continue;
        }
        if matches!(cursor.peek().kind, TokenKind::Pipe) {
            cursor.advance();
            let stage = parse_pipe_stage(cursor)?;
            match &stage {
                Stage::LabelFilter(_) => {}
                other if saw_unwrap => {
                    return Err(post_unwrap_stage_error(
                        format!("stage `{other}`"),
                        stage_span,
                    ));
                }
                Stage::Unwrap(_) => saw_unwrap = true,
                _ => {}
            }
            pipeline.push(stage);
            continue;
        }
        break;
    }
    Ok(LogExpr { selector, pipeline })
}

fn post_unwrap_stage_error(found: String, span: crate::token::Span) -> LogQlError {
    LogQlError::UnexpectedToken {
        found,
        expected: "a label filter (only label filters may follow `unwrap`)".to_string(),
        span,
    }
}

/// Parses one line-filter alternative: `ip("<spec>")` (M8-LQ2
/// `linefilter.ip`) when the next tokens are `ip` `(`, else a plain quoted
/// string. `ip(…)` is accepted only with `|=`/`!=` (matching the reference)
/// — under `|~`/`!~` it is an `UnexpectedToken`. Returns `(value, is_ip)`.
fn parse_line_match(
    cursor: &mut Cursor<'_>,
    op: LineFilterOp,
) -> Result<(String, bool), LogQlError> {
    if matches!(&cursor.peek().kind, TokenKind::Ident(n) if n == "ip")
        && matches!(cursor.peek2().kind, TokenKind::LParen)
    {
        let ip_tok = cursor.peek().clone();
        if !matches!(op, LineFilterOp::Contains | LineFilterOp::NotContains) {
            return Err(LogQlError::UnexpectedToken {
                found: describe(&ip_tok.kind),
                expected: "a string (ip() line filters require `|=` or `!=`)".to_string(),
                span: ip_tok.span,
            });
        }
        cursor.advance(); // `ip`
        cursor.expect(&TokenKind::LParen, "'('")?;
        let (value, _) = cursor.expect_string()?;
        cursor.expect(&TokenKind::RParen, "')'")?;
        Ok((value, true))
    } else {
        let (value, _) = cursor.expect_string()?;
        Ok((value, false))
    }
}

/// Dispatches the stage after a bare `|`: a stage keyword (`json`,
/// `logfmt`, `regexp`, `pattern`, `line_format`, `label_format`,
/// `unwrap`), a still-unsupported keyword (named `NotYetSupported`), or —
/// any other identifier / an opening paren — a label-filter expression.
fn parse_pipe_stage(cursor: &mut Cursor<'_>) -> Result<Stage, LogQlError> {
    let tok = cursor.peek().clone();
    match &tok.kind {
        TokenKind::Ident(name) => match name.as_str() {
            "json" => {
                cursor.advance();
                Ok(Stage::Parser(ParserStage::Json {
                    extractions: parse_extraction_list(cursor)?,
                }))
            }
            "logfmt" => {
                cursor.advance();
                let (strict, keep_empty) = parse_logfmt_flags(cursor)?;
                Ok(Stage::Parser(ParserStage::Logfmt {
                    strict,
                    keep_empty,
                    extractions: parse_extraction_list(cursor)?,
                }))
            }
            "regexp" => {
                cursor.advance();
                let (re, _) = cursor.expect_string()?;
                Ok(Stage::Parser(ParserStage::Regexp(re)))
            }
            "pattern" => {
                cursor.advance();
                let (p, _) = cursor.expect_string()?;
                Ok(Stage::Parser(ParserStage::Pattern(p)))
            }
            "line_format" => {
                cursor.advance();
                let (tmpl, _) = cursor.expect_string()?;
                Ok(Stage::LineFormat(tmpl))
            }
            "label_format" => {
                cursor.advance();
                Ok(Stage::LabelFormat(parse_label_format_list(cursor)?))
            }
            "unwrap" => {
                cursor.advance();
                Ok(Stage::Unwrap(parse_unwrap(cursor)?))
            }
            "unpack" => {
                cursor.advance();
                Ok(Stage::Unpack)
            }
            "decolorize" => {
                cursor.advance();
                Ok(Stage::Decolorize)
            }
            "drop" => {
                cursor.advance();
                Ok(Stage::Drop(parse_drop_keep_list(cursor)?))
            }
            "keep" => {
                cursor.advance();
                Ok(Stage::Keep(parse_drop_keep_list(cursor)?))
            }
            name if ast::REMAINING_UNSUPPORTED_STAGES.contains(&name) => {
                Err(LogQlError::NotYetSupported {
                    construct: name.to_string(),
                    span: tok.span,
                })
            }
            // Any other identifier at stage position starts a label
            // filter (e.g. `| status="500"`, `| status >= 500`).
            _ => Ok(Stage::LabelFilter(parse_label_filter_or(cursor, 0)?)),
        },
        TokenKind::LParen => Ok(Stage::LabelFilter(parse_label_filter_or(cursor, 0)?)),
        TokenKind::Eof => Err(LogQlError::UnexpectedEof {
            expected: "a pipeline stage".to_string(),
            span: tok.span,
        }),
        _ => Err(LogQlError::UnexpectedToken {
            found: describe(&tok.kind),
            expected: "a pipeline stage".to_string(),
            span: tok.span,
        }),
    }
}

/// `json`/`logfmt` extraction list: zero or more `label` /
/// `label="expression"` entries, comma-separated. A bare identifier is
/// shorthand for `label="label"`.
fn parse_extraction_list(cursor: &mut Cursor<'_>) -> Result<Vec<LabelExtraction>, LogQlError> {
    let mut out = Vec::new();
    while matches!(cursor.peek().kind, TokenKind::Ident(_)) {
        let (label, _) = cursor.expect_ident()?;
        let expression = if matches!(cursor.peek().kind, TokenKind::Eq)
            && matches!(cursor.peek2().kind, TokenKind::String(_))
        {
            cursor.advance();
            cursor.expect_string()?.0
        } else {
            label.clone()
        };
        out.push(LabelExtraction { label, expression });
        if matches!(cursor.peek().kind, TokenKind::Comma) {
            cursor.advance();
            continue;
        }
        break;
    }
    Ok(out)
}

/// Consumes any leading `--strict` / `--keep-empty` flag tokens after the
/// `logfmt` keyword (issue #200), returning `(strict, keep_empty)`. Flags
/// may appear in any order; a repeated flag is idempotent; any other flag
/// name is an `UnexpectedToken` (never a silent ignore).
fn parse_logfmt_flags(cursor: &mut Cursor<'_>) -> Result<(bool, bool), LogQlError> {
    let mut strict = false;
    let mut keep_empty = false;
    while let TokenKind::Flag(name) = &cursor.peek().kind {
        match name.as_str() {
            "strict" => strict = true,
            "keep-empty" => keep_empty = true,
            other => {
                let tok = cursor.peek();
                return Err(LogQlError::UnexpectedToken {
                    found: format!("flag \"--{other}\""),
                    expected: "'--strict' or '--keep-empty'".to_string(),
                    span: tok.span,
                });
            }
        }
        cursor.advance();
    }
    Ok((strict, keep_empty))
}

/// `drop`/`keep` element list: one or more `ident [<op> "value"]` entries,
/// comma-separated (issue #200). At least one element is required; the
/// optional value matcher accepts `=`/`!=`/`=~`/`!~`.
fn parse_drop_keep_list(cursor: &mut Cursor<'_>) -> Result<Vec<DropKeepElem>, LogQlError> {
    let mut out = Vec::new();
    loop {
        let (label, _) = cursor.expect_ident()?;
        let matcher = match cursor.peek().kind {
            TokenKind::Eq => Some(MatchOp::Eq),
            TokenKind::Neq => Some(MatchOp::Neq),
            TokenKind::Re => Some(MatchOp::Re),
            TokenKind::Nre => Some(MatchOp::Nre),
            _ => None,
        };
        let matcher = if let Some(op) = matcher {
            cursor.advance();
            let (value, _) = cursor.expect_string()?;
            Some(LabelMatch { op, value })
        } else {
            None
        };
        out.push(DropKeepElem { label, matcher });
        if matches!(cursor.peek().kind, TokenKind::Comma) {
            cursor.advance();
            continue;
        }
        break;
    }
    Ok(out)
}

/// `label_format` list: one or more `dst=src` (identifier RHS, a rename)
/// or `dst="<template>"` (string RHS) entries, comma-separated.
fn parse_label_format_list(cursor: &mut Cursor<'_>) -> Result<Vec<LabelFmt>, LogQlError> {
    let mut out = Vec::new();
    loop {
        let (dst, _) = cursor.expect_ident()?;
        cursor.expect(&TokenKind::Eq, "'='")?;
        let tok = cursor.peek().clone();
        match tok.kind {
            TokenKind::Ident(src) => {
                cursor.advance();
                out.push(LabelFmt::Rename { dst, src });
            }
            TokenKind::String(tmpl) => {
                cursor.advance();
                out.push(LabelFmt::Template { dst, tmpl });
            }
            TokenKind::Eof => {
                return Err(LogQlError::UnexpectedEof {
                    expected: "a source label or a template string".to_string(),
                    span: tok.span,
                });
            }
            _ => {
                return Err(LogQlError::UnexpectedToken {
                    found: describe(&tok.kind),
                    expected: "a source label or a template string".to_string(),
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
    Ok(out)
}

/// `unwrap <label>` or `unwrap <conversion>(<label>)` where the
/// conversion is one of `duration`, `duration_seconds`, `bytes`.
fn parse_unwrap(cursor: &mut Cursor<'_>) -> Result<Unwrap, LogQlError> {
    let (first, first_span) = cursor.expect_ident()?;
    if matches!(cursor.peek().kind, TokenKind::LParen) {
        if !ast::UNWRAP_CONVERSIONS.contains(&first.as_str()) {
            return Err(LogQlError::UnexpectedToken {
                found: format!("identifier {first:?}"),
                expected: "an unwrap conversion: 'duration', 'duration_seconds', or 'bytes'"
                    .to_string(),
                span: first_span,
            });
        }
        cursor.advance();
        let (label, _) = cursor.expect_ident()?;
        cursor.expect(&TokenKind::RParen, "')'")?;
        Ok(Unwrap {
            label,
            conversion: Some(first),
        })
    } else {
        Ok(Unwrap {
            label: first,
            conversion: None,
        })
    }
}

/// Label-filter boolean grammar, precedence-climbing: `or` binds loosest,
/// `and`/`,` bind tighter, parentheses group. `depth` counts parenthesis
/// nesting only (the sole recursion in this family, issue #255): `or`/
/// `and` terms are consumed iteratively and never increment it.
fn parse_label_filter_or(
    cursor: &mut Cursor<'_>,
    depth: usize,
) -> Result<LabelFilterExpr, LogQlError> {
    if depth >= LABEL_FILTER_MAX_DEPTH {
        return Err(LogQlError::RecursionLimitExceeded {
            span: cursor.peek().span,
            limit: LABEL_FILTER_MAX_DEPTH,
        });
    }
    let mut left = parse_label_filter_and(cursor, depth)?;
    while matches!(&cursor.peek().kind, TokenKind::Ident(name) if name == "or") {
        cursor.advance();
        let right = parse_label_filter_and(cursor, depth)?;
        left = LabelFilterExpr::Or(walk::Child::new(left), walk::Child::new(right));
    }
    Ok(left)
}

fn parse_label_filter_and(
    cursor: &mut Cursor<'_>,
    depth: usize,
) -> Result<LabelFilterExpr, LogQlError> {
    let mut left = parse_label_filter_factor(cursor, depth)?;
    loop {
        let is_and = match &cursor.peek().kind {
            TokenKind::Comma => true,
            TokenKind::Ident(name) if name == "and" => true,
            _ => false,
        };
        if !is_and {
            return Ok(left);
        }
        cursor.advance();
        let right = parse_label_filter_factor(cursor, depth)?;
        left = LabelFilterExpr::And(walk::Child::new(left), walk::Child::new(right));
    }
}

fn parse_label_filter_factor(
    cursor: &mut Cursor<'_>,
    depth: usize,
) -> Result<LabelFilterExpr, LogQlError> {
    if matches!(cursor.peek().kind, TokenKind::LParen) {
        cursor.advance();
        let inner = parse_label_filter_or(cursor, depth + 1)?;
        cursor.expect(&TokenKind::RParen, "')'")?;
        return Ok(inner);
    }
    parse_label_filter_predicate(cursor)
}

/// One `name <op> <rhs>` predicate, RHS-typed (plan v1): a string RHS is
/// a string matcher (`=`/`!=`/`=~`/`!~`), a number/duration RHS is a
/// numeric comparison (`==`/`=`/`!=`/`>`/`>=`/`<`/`<=`).
fn parse_label_filter_predicate(cursor: &mut Cursor<'_>) -> Result<LabelFilterExpr, LogQlError> {
    let (name, _) = cursor.expect_ident()?;
    let op_tok = cursor.peek().clone();
    cursor.advance();
    let rhs_tok = cursor.peek().clone();

    /// Which operator family the operator token belongs to.
    enum OpForms {
        /// `=`/`!=`: legal with both a string RHS (matcher) and a numeric
        /// RHS (comparison).
        Both { m: MatchOp, c: CompareOp },
        /// `=~`/`!~`: string RHS only.
        StringOnly(MatchOp),
        /// `==`/`>`/`>=`/`<`/`<=`: numeric RHS only.
        NumericOnly(CompareOp),
    }

    let forms = match op_tok.kind {
        TokenKind::Eq => OpForms::Both {
            m: MatchOp::Eq,
            c: CompareOp::Eq,
        },
        TokenKind::Neq => OpForms::Both {
            m: MatchOp::Neq,
            c: CompareOp::Neq,
        },
        TokenKind::Re => OpForms::StringOnly(MatchOp::Re),
        TokenKind::Nre => OpForms::StringOnly(MatchOp::Nre),
        TokenKind::EqEq => OpForms::NumericOnly(CompareOp::Eq),
        TokenKind::Gt => OpForms::NumericOnly(CompareOp::Gt),
        TokenKind::Gte => OpForms::NumericOnly(CompareOp::Gte),
        TokenKind::Lt => OpForms::NumericOnly(CompareOp::Lt),
        TokenKind::Lte => OpForms::NumericOnly(CompareOp::Lte),
        TokenKind::Eof => {
            return Err(LogQlError::UnexpectedEof {
                expected: "a label-filter operator".to_string(),
                span: op_tok.span,
            });
        }
        _ => {
            return Err(LogQlError::UnexpectedToken {
                found: describe(&op_tok.kind),
                expected:
                    "a label-filter operator ('=', '!=', '=~', '!~', '==', '>', '>=', '<', '<=')"
                        .to_string(),
                span: op_tok.span,
            });
        }
    };

    let numeric_rhs = |cursor: &mut Cursor<'_>| -> Result<NumericLiteral, LogQlError> {
        let tok = cursor.peek().clone();
        match tok.kind {
            TokenKind::Number(raw) => {
                cursor.advance();
                Ok(NumericLiteral::Number(raw))
            }
            TokenKind::Duration(raw) => {
                cursor.advance();
                Ok(NumericLiteral::DurationOrBytes(raw))
            }
            TokenKind::Eof => Err(LogQlError::UnexpectedEof {
                expected: "a number, duration, or bytes literal".to_string(),
                span: tok.span,
            }),
            _ => Err(LogQlError::UnexpectedToken {
                found: describe(&tok.kind),
                expected: "a number, duration, or bytes literal".to_string(),
                span: tok.span,
            }),
        }
    };

    match forms {
        OpForms::Both { m, c } => match &rhs_tok.kind {
            TokenKind::String(_) => {
                let (value, _) = cursor.expect_string()?;
                Ok(LabelFilterExpr::Match(Matcher { name, op: m, value }))
            }
            // `name = ip("…")` / `name != ip("…")` (M8-LQ2 `labelfilter.ip`).
            // Only `=`/`!=` accept an `ip()` RHS; `=~`/`!~`/numeric ops keep
            // rejecting it via their own arms below.
            TokenKind::Ident(n)
                if n == "ip" && matches!(cursor.peek2().kind, TokenKind::LParen) =>
            {
                cursor.advance(); // `ip`
                cursor.expect(&TokenKind::LParen, "'('")?;
                let (value, _) = cursor.expect_string()?;
                cursor.expect(&TokenKind::RParen, "')'")?;
                Ok(LabelFilterExpr::Ip {
                    name,
                    value,
                    negated: matches!(m, MatchOp::Neq),
                })
            }
            _ => {
                let rhs = numeric_rhs(cursor)?;
                Ok(LabelFilterExpr::Compare { name, op: c, rhs })
            }
        },
        OpForms::StringOnly(m) => {
            let (value, _) = cursor.expect_string()?;
            Ok(LabelFilterExpr::Match(Matcher { name, op: m, value }))
        }
        OpForms::NumericOnly(c) => {
            let rhs = numeric_rhs(cursor)?;
            Ok(LabelFilterExpr::Compare { name, op: c, rhs })
        }
    }
}

/// `StreamSelector := "{" (Matcher ("," Matcher)*)? "}"`, rejecting zero
/// matchers (`EmptySelector`) — match-everything selectors that *do* have
/// a matcher are accepted here; rejecting those is a planner concern.
fn parse_stream_selector(cursor: &mut Cursor<'_>) -> Result<StreamSelector, LogQlError> {
    let open = cursor.expect(&TokenKind::LBrace, "'{'")?;
    let mut matchers = Vec::new();
    if !matches!(cursor.peek().kind, TokenKind::RBrace) {
        loop {
            let (name, _) = cursor.expect_ident()?;
            let op_tok = cursor.peek().clone();
            let op = match op_tok.kind {
                TokenKind::Eq => MatchOp::Eq,
                TokenKind::Neq => MatchOp::Neq,
                TokenKind::Re => MatchOp::Re,
                TokenKind::Nre => MatchOp::Nre,
                TokenKind::Eof => {
                    return Err(LogQlError::UnexpectedEof {
                        expected: "'=', '!=', '=~', or '!~'".to_string(),
                        span: op_tok.span,
                    });
                }
                _ => {
                    return Err(LogQlError::UnexpectedToken {
                        found: describe(&op_tok.kind),
                        expected: "'=', '!=', '=~', or '!~'".to_string(),
                        span: op_tok.span,
                    });
                }
            };
            cursor.advance();
            let (value, _) = cursor.expect_string()?;
            matchers.push(Matcher { name, op, value });
            if matches!(cursor.peek().kind, TokenKind::Comma) {
                cursor.advance();
                continue;
            }
            break;
        }
    }
    cursor.expect(&TokenKind::RBrace, "'}'")?;
    if matchers.is_empty() {
        return Err(LogQlError::EmptySelector { span: open.span });
    }
    Ok(StreamSelector { matchers })
}

/// `MetricExpr := <range-agg-name> "(" (Number ",")? LogRange ")" |
/// <vector-agg-name> Grouping? "(" (Number ",")? BinaryExpr ")"
/// Grouping?` — dispatches on the leading identifier: implemented
/// range/vector aggregation names build the corresponding node; anything
/// else is an `UnexpectedToken` (the M6-10 aggregation set is complete —
/// no future-aggregation keyword table remains).
fn parse_metric_expr(
    cursor: &mut Cursor<'_>,
    depth: usize,
    allow_variants: bool,
) -> Result<MetricExpr, LogQlError> {
    if depth >= MAX_DEPTH {
        return Err(LogQlError::RecursionLimitExceeded {
            span: cursor.peek().span,
            limit: MAX_DEPTH,
        });
    }
    let tok = cursor.peek().clone();
    let name = match &tok.kind {
        TokenKind::Ident(name) => name.clone(),
        TokenKind::Eof => {
            return Err(LogQlError::UnexpectedEof {
                expected: "an aggregation function".to_string(),
                span: tok.span,
            });
        }
        _ => {
            return Err(LogQlError::UnexpectedToken {
                found: describe(&tok.kind),
                expected: "an aggregation function".to_string(),
                span: tok.span,
            });
        }
    };

    // `vector(<NUMBER>)` (issue #221): promotes a scalar to a vector result
    // (`{} => n`). Only a `NUMBER` arg — mirrors Loki v3.7.4's `vectorExpr`
    // grammar (`vector "(" NUMBER ")"`), which rejects an inner expression.
    if name == "vector" {
        cursor.advance();
        cursor.expect(&TokenKind::LParen, "'('")?;
        let (raw, _) = cursor.expect_number("the vector value (e.g. vector(0))")?;
        cursor.expect(&TokenKind::RParen, "')'")?;
        return Ok(MetricExpr::VectorFn(raw));
    }
    // `label_replace(<metricExpr>, "<dst>", "<replacement>", "<src>",
    // "<regex>")` (issue #276) — mirrors Loki v3.7.4's `labelReplaceExpr`
    // grammar rule (`LABEL_REPLACE "(" metricExpr "," STRING "," STRING
    // "," STRING "," STRING ")"`, pkg/logql/syntax/syntax.y). A
    // `metricExpr` alternative, so it is legal in EVERY metric position
    // (oracle-probed: inside `sum(...)`/`topk(...)`, parenthesized, as a
    // binary operand, and nested in another `label_replace` are all
    // reference 200s). The inner expression is a full binary-capable
    // metric expression; `variants` stays illegal inside the argument
    // list (`allow_variants = false` — the reference 400s
    // `label_replace(variants(...) of (...), ...)` with `syntax error:
    // unexpected ,`). Arity/argument-type mistakes fall out of the
    // `expect` calls as plain positional 400s, matching the reference's
    // `unexpected ), expecting ,` / `unexpected IDENTIFIER, expecting
    // STRING` class. The keyword is matched as the exact lowercase
    // identifier, consistent with every other PulsusDB keyword (the
    // reference's case-insensitive lexer is the workspace-wide
    // pre-existing gap, issue #221 plan §risk 6).
    if name == "label_replace" {
        cursor.advance();
        return parse_label_replace_call(cursor, depth);
    }
    // `variants(...) of (...)` (issue #221) — recognized ONLY in the
    // positions the reference grammar admits (`allow_variants`, see
    // `parse_binary_expr`). In a disallowed position the identifier falls
    // through to the ordinary `UnexpectedToken` below — a plain 400, the
    // same shape the reference's `syntax error: unexpected )` carries,
    // never `NotYetSupported`.
    if name == "variants" && allow_variants {
        cursor.advance();
        return parse_variants_expr(cursor, depth);
    }
    if let Some(op) = RangeAggOp::from_ident(&name) {
        cursor.advance();
        return parse_range_agg_call(cursor, op);
    }
    if let Some(op) = VectorAggOp::from_ident(&name) {
        cursor.advance();
        return parse_vector_agg_call(cursor, depth, op, tok.span);
    }
    Err(LogQlError::UnexpectedToken {
        found: describe(&tok.kind),
        expected: "an aggregation function".to_string(),
        span: tok.span,
    })
}

fn parse_range_agg_call(cursor: &mut Cursor<'_>, op: RangeAggOp) -> Result<MetricExpr, LogQlError> {
    cursor.expect(&TokenKind::LParen, "'('")?;
    // `quantile_over_time` is the only range aggregation with a leading
    // parameter (`quantile_over_time(0.95, {...}[5m])`); for every other
    // op the call opens directly with the log range, so a stray number
    // there fails as an `UnexpectedToken` from the selector parser.
    let param = if matches!(op, RangeAggOp::QuantileOverTime) {
        let (raw, _) = cursor.expect_number("the quantile parameter (e.g. 0.95)")?;
        cursor.expect(&TokenKind::Comma, "','")?;
        Some(raw)
    } else {
        None
    };
    let range = parse_log_range(cursor)?;
    cursor.expect(&TokenKind::RParen, "')'")?;
    Ok(MetricExpr::Range { op, range, param })
}

/// `LogRange := LogExpr "[" Duration "]"`.
fn parse_log_range(cursor: &mut Cursor<'_>) -> Result<LogRange, LogQlError> {
    let selector = parse_log_expr(cursor)?;
    cursor.expect(&TokenKind::LBracket, "'['")?;
    let (raw, span) = cursor.expect_duration()?;
    let range = duration::parse_duration(&raw, span)?;
    cursor.expect(&TokenKind::RBracket, "']'")?;
    Ok(LogRange {
        selector,
        range,
        unwrap: None, // M1 never populates the M6 `unwrap` stage
    })
}

/// `variants "(" metricExpr ("," metricExpr)* ")" "of" "(" logRange ")"`
/// (issue #221). The `variants` keyword itself has already been consumed.
///
/// An empty argument list needs NO special case: the first
/// `parse_binary_expr` sees `)` and produces the ordinary
/// `UnexpectedToken { found: "')'", expected: "an aggregation function" }`,
/// which is the same rejection shape the reference's `unexpected )`
/// carries. A trailing comma fails identically. Arguments parse with
/// `allow_variants = false`, so a nested `variants(...)` argument is the
/// same plain 400.
fn parse_variants_expr(cursor: &mut Cursor<'_>, depth: usize) -> Result<MetricExpr, LogQlError> {
    cursor.expect(&TokenKind::LParen, "'('")?;
    let mut variants = vec![parse_binary_expr(cursor, depth + 1, 0, false)?];
    while matches!(cursor.peek().kind, TokenKind::Comma) {
        cursor.advance();
        variants.push(parse_binary_expr(cursor, depth + 1, 0, false)?);
    }
    cursor.expect(&TokenKind::RParen, "')'")?;
    // The `of` keyword is matched as the exact lowercase identifier,
    // consistent with every other PulsusDB keyword (`sum`, `by`,
    // `unwrap`) — the reference's case-insensitive lexer is a
    // workspace-wide pre-existing gap, deliberately not special-cased
    // here (issue #221 plan §risk 6).
    match &cursor.peek().kind {
        TokenKind::Ident(name) if name == "of" => {
            cursor.advance();
        }
        other => {
            let span = cursor.peek().span;
            return Err(LogQlError::UnexpectedToken {
                found: describe(other),
                expected: "'of'".to_string(),
                span,
            });
        }
    }
    cursor.expect(&TokenKind::LParen, "'('")?;
    let range = parse_log_range(cursor)?;
    cursor.expect(&TokenKind::RParen, "')'")?;
    Ok(MetricExpr::Variants(walk::Child::new(ast::VariantsExpr {
        variants: walk::ChildVec::new(variants),
        range,
    })))
}

/// `label_replace "(" metricExpr "," STRING "," STRING "," STRING ","
/// STRING ")"` (issue #276). The keyword itself has already been
/// consumed. A separate function — NOT inlined into `parse_metric_expr`
/// — so its argument locals never enlarge the recursion-path frame the
/// #255/#293 pinned-stack gates budget.
fn parse_label_replace_call(
    cursor: &mut Cursor<'_>,
    depth: usize,
) -> Result<MetricExpr, LogQlError> {
    cursor.expect(&TokenKind::LParen, "'('")?;
    let inner = parse_binary_expr(cursor, depth + 1, 0, false)?;
    cursor.expect(&TokenKind::Comma, "','")?;
    let (dst, _) = cursor.expect_string()?;
    cursor.expect(&TokenKind::Comma, "','")?;
    let (replacement, _) = cursor.expect_string()?;
    cursor.expect(&TokenKind::Comma, "','")?;
    let (src, _) = cursor.expect_string()?;
    cursor.expect(&TokenKind::Comma, "','")?;
    let (regex, _) = cursor.expect_string()?;
    cursor.expect(&TokenKind::RParen, "')'")?;
    Ok(MetricExpr::LabelReplace {
        inner: walk::Child::new(inner),
        dst,
        replacement,
        src,
        regex,
    })
}

fn parse_vector_agg_call(
    cursor: &mut Cursor<'_>,
    depth: usize,
    op: VectorAggOp,
    op_span: Span,
) -> Result<MetricExpr, LogQlError> {
    let mut grouping = maybe_grouping(cursor)?;
    cursor.expect(&TokenKind::LParen, "'('")?;
    // `topk`/`bottomk`/`approx_topk` require a leading `k` parameter
    // (`topk(5, ...)`); for the parameterless aggregations the inner
    // expression may itself begin with a number (`sum(2 * rate(...))`),
    // so no-param ops go straight to the inner parse — a misplaced
    // `0.5,` there fails on the `,` as an `UnexpectedToken` (expected
    // `')'`).
    let raw_param = if op.takes_param() {
        let (raw, span) = cursor.expect_number("the k parameter (e.g. topk(5, ...))")?;
        cursor.expect(&TokenKind::Comma, "','")?;
        Some((raw, span))
    } else {
        None
    };
    // The aggregated operand is a full binary-capable metric expression
    // (`sum(rate(a) + rate(b))` — issue M6-10). `variants` is NOT legal
    // here (`sum(variants(…))` is a reference 400 — issue #221).
    let inner = parse_binary_expr(cursor, depth + 1, 0, false)?;
    cursor.expect(&TokenKind::RParen, "')'")?;
    if grouping.is_none() {
        grouping = maybe_grouping(cursor)?;
    }
    // Loki-exact `k` validation — the root-cause fix shared by
    // `topk`/`bottomk`/`approx_topk` (issue #221, adjudicated: `topk(0,
    // ...)` becomes a 400, no longer an empty 200). The reference runs
    // `strconv.Atoi` then the `> 0` check, then the `approx_topk`
    // grouping rejection, all in `mustNewVectorAggregationExpr`
    // (pkg/logql/syntax/ast.go), which fires at reduce time — i.e. only
    // after the whole call parsed — so the checks run here, after the
    // inner expression and the postfix grouping lookahead (a syntax
    // error inside the call still wins, exactly as in the reference).
    let param = match raw_param {
        Some((raw, span)) => {
            match raw.parse::<i64>() {
                Err(_) => {
                    return Err(LogQlError::InvalidAggregationParam {
                        op: op.to_string(),
                        raw,
                        span,
                    });
                }
                Ok(k) if k <= 0 => {
                    return Err(LogQlError::AggregationParamNotPositive {
                        op: op.to_string(),
                        raw,
                        span,
                    });
                }
                Ok(_) => {}
            }
            Some(raw)
        }
        None => None,
    };
    // `grouping not allowed for approx_topk aggregation` — after the
    // postfix lookahead so BOTH `approx_topk by(x)(k, ...)` and
    // `approx_topk(k, ...) by(x)` reject (reference: ast.go, gated on
    // `OpTypeApproxTopK && gr != nil` after the `k` checks).
    if matches!(op, VectorAggOp::ApproxTopk) && grouping.is_some() {
        return Err(LogQlError::GroupingNotAllowed {
            op: op.to_string(),
            span: op_span,
        });
    }
    Ok(MetricExpr::Vector {
        op,
        grouping,
        param,
        inner: walk::Child::new(inner),
    })
}

/// Looks ahead for `("by" | "without") "("` — Loki accepts grouping both
/// before (`sum by(l)(...)`) and after (`sum(...) by(l)`) the aggregated
/// expression; the parser accepts either and normalizes to one
/// `Grouping` value.
fn maybe_grouping(cursor: &mut Cursor<'_>) -> Result<Option<Grouping>, LogQlError> {
    let is_grouping_keyword = matches!(&cursor.peek().kind, TokenKind::Ident(name) if name == "by" || name == "without")
        && matches!(cursor.peek2().kind, TokenKind::LParen);
    if is_grouping_keyword {
        Ok(Some(parse_grouping(cursor)?))
    } else {
        Ok(None)
    }
}

fn parse_grouping(cursor: &mut Cursor<'_>) -> Result<Grouping, LogQlError> {
    let (name, span) = cursor.expect_ident()?;
    let kind = match name.as_str() {
        "by" => GroupingKind::By,
        "without" => GroupingKind::Without,
        _ => {
            return Err(LogQlError::UnexpectedToken {
                found: format!("identifier {name:?}"),
                expected: "'by' or 'without'".to_string(),
                span,
            });
        }
    };
    let labels = parse_label_list_parens(cursor)?;
    Ok(Grouping { kind, labels })
}
