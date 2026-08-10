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
use crate::limits::{CheckedQuery, MAX_QUERY_SPAN_HOURS, MAX_QUERY_SPAN_NS};
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

/// **The ONE case-folding point for LogQL keywords** (issue #339).
///
/// The reference's lexer resolves a keyword by looking its text up
/// case-insensitively, so `RATE(...)`, `SUM BY (...)`, `| JSON` and
/// `LABEL_REPLACE(...)` are all accepted there (probed against the pinned
/// v3.7.4 container: every keyword position returns 200 in upper, lower
/// and mixed case). PulsusDB used to compare each keyword with `==`
/// against its lowercase spelling, so every one of those was a 400 — a
/// whole-surface rejection divergence, not a per-construct one.
///
/// **Folding happens here and nowhere else, and it applies ONLY at
/// grammar positions that expect a keyword.** Identifier *payloads* — a
/// label name in a selector, a grouping list, a label filter, `drop`/
/// `keep`, a `label_format` source or destination, an `unwrap`
/// identifier — keep their original case, because the reference keeps
/// theirs: `| json | RATE = "R"` does NOT match a field named `rate`
/// (semantically probed, 0 hits vs 1 for the lowercase spelling), and
/// `sum by (Env)` groups on `Env`, not `env`. Folding an identifier
/// payload would silently change which series a query selects — strictly
/// worse than the rejection this fixes.
///
/// **Not ASCII-only** (issue #392). This paragraph used to argue that
/// `str::to_ascii_lowercase` was safe here because a non-ASCII
/// identifier could not lex — true only while the lexer was
/// `[A-Za-z_][A-Za-z0-9_]*`, and falsified the moment #392 gave it the
/// reference's Unicode rune set. The reference folds with Go
/// `strings.ToLower` at lex time (`pkg/logql/syntax/lex.go:226` @
/// v3.7.4), so the Kelvin-sign and dotless-i cases that comment
/// dismissed are real and measured: `| <U+212A>EEP ax` is a 200 there,
/// `| drop <U+212A>EEP` is `400 unexpected keep`, and
/// `sum by (İGNORING)` is `400 unexpected ignoring`.
/// [`crate::unicode_ident::fold_keyword_char`] carries the fold, with a
/// whole-code-space enumeration proving U+0130 and U+212A are the only
/// non-ASCII identifier runes that can reach an ASCII keyword.
fn kw(name: &str) -> String {
    name.chars()
        .map(crate::unicode_ident::fold_keyword_char)
        .collect()
}

/// `name` is the keyword `want`, compared the way the reference's lexer
/// compares it. `want` must already be lowercase — asserted by
/// [`tests::every_keyword_literal_in_this_file_is_lowercase`], since a
/// mixed-case `want` could never match a folded name.
///
/// Char-by-char rather than `kw(name) == want`, so the hot path — every
/// keyword probe in the parser — stays allocation-free (issue #392; it
/// was `eq_ignore_ascii_case` before, which is no longer the reference's
/// fold).
fn is_kw(name: &str, want: &str) -> bool {
    debug_assert_eq!(want, want.to_ascii_lowercase(), "keyword must be lowercase");
    let mut n = name.chars();
    let mut w = want.chars();
    loop {
        match (n.next(), w.next()) {
            (None, None) => return true,
            (Some(a), Some(b)) if crate::unicode_ident::fold_keyword_char(a) == b => {}
            _ => return false,
        }
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
        TokenKind::Ident(name) if ast::BINARY_OP_KEYWORDS.iter().any(|k| is_kw(name, k)) => {
            match kw(name).as_str() {
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
        && is_kw(name, "bool")
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
        TokenKind::Ident(name) if is_kw(name, "on") => true,
        TokenKind::Ident(name) if is_kw(name, "ignoring") => false,
        TokenKind::Ident(name) if is_kw(name, "group_left") || is_kw(name, "group_right") => {
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
        TokenKind::Ident(name) if is_kw(name, "group_left") || is_kw(name, "group_right") => {
            let left = is_kw(name, "group_left");
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
            while matches!(&cursor.peek().kind, TokenKind::Ident(n) if is_kw(n, "or")) {
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
    if matches!(&cursor.peek().kind, TokenKind::Ident(n) if is_kw(n, "ip"))
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
        TokenKind::Ident(name) => match kw(name).as_str() {
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
///
/// **A comma must be followed by another entry (issue #247).**
/// `labelExtractionExpressionList: labelExtractionExpression |
/// labelExtractionExpressionList COMMA labelExtractionExpression`
/// (`pkg/logql/syntax/syntax.y:318-321 @ v3.7.4`) — there is no
/// trailing-comma production. So the loop below is `loop` +
/// `expect_ident`, the shape [`parse_drop_keep_list`] and
/// [`parse_label_format_list`] already use, and a dangling comma dies in
/// `expect_ident` rather than silently ending the list. Measured on the
/// pinned container: `| logfmt a="b",`, `| logfmt a="b", | json` and
/// `| json a="b",` are all 400 (`unexpected $end, expecting IDENTIFIER`
/// for the first and third), as are `| label_format a=b,` and
/// `| drop a,` — the same rejection the two loops above already give.
/// The early return keeps a bare
/// `| json` / `| logfmt` (and `| logfmt --strict`) an empty list, which
/// is what the `LOGFMT`/`JSON` productions without a list express
/// (`syntax.y:264-267`, `:270`).
fn parse_extraction_list(cursor: &mut Cursor<'_>) -> Result<Vec<LabelExtraction>, LogQlError> {
    let mut out = Vec::new();
    if !matches!(cursor.peek().kind, TokenKind::Ident(_)) {
        return Ok(out);
    }
    loop {
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
        // The conversion name is a KEYWORD (folds); the label name it
        // wraps is an identifier payload (never folded) — issue #339.
        if !ast::UNWRAP_CONVERSIONS.iter().any(|c| is_kw(&first, c)) {
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
            conversion: Some(kw(&first)),
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
    while matches!(&cursor.peek().kind, TokenKind::Ident(name) if is_kw(name, "or")) {
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
            TokenKind::Ident(name) if is_kw(name, "and") => true,
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
                if is_kw(n, "ip") && matches!(cursor.peek2().kind, TokenKind::LParen) =>
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
    if is_kw(&name, "vector") {
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
    // STRING` class. The keyword is matched case-insensitively through
    // [`is_kw`], like every other PulsusDB keyword since issue #339 —
    // `LABEL_REPLACE(...)` is the case that surfaced that gap.
    if is_kw(&name, "label_replace") {
        cursor.advance();
        return parse_label_replace_call(cursor, depth);
    }
    // `variants(...) of (...)` (issue #221) — recognized ONLY in the
    // positions the reference grammar admits (`allow_variants`, see
    // `parse_binary_expr`). In a disallowed position the identifier falls
    // through to the ordinary `UnexpectedToken` below — a plain 400, the
    // same shape the reference's `syntax error: unexpected )` carries,
    // never `NotYetSupported`.
    if is_kw(&name, "variants") && allow_variants {
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
    let close = cursor.expect(&TokenKind::RParen, "')'")?;
    // Issue #344: a range aggregation takes its grouping POSTFIX ONLY.
    // Unlike a vector aggregation, the reference has no prefix production
    // here — `max_over_time by (fp) (…)` is `syntax error: unexpected by,
    // expecting (`, which is what `maybe_grouping` NOT being called before
    // the '(' above already produces.
    let grouping = maybe_grouping(cursor)?;
    // …and it admits one on only eight of the fifteen ops. This fires
    // before the planner's unwrap-compatibility check, matching the
    // reference's own ordering: `sum_over_time({a="b"}[5m]) by (fp)`
    // answers the grouping rejection, not `invalid aggregation
    // sum_over_time without unwrap`, and `bytes_over_time({a="b"} | unwrap
    // v [5m]) by (fp)` likewise answers the grouping rejection rather than
    // `… with unwrap` (both probed on v3.7.4).
    if grouping.is_some() && !op.allows_grouping() {
        return Err(LogQlError::GroupingNotAllowed {
            op: op.to_string(),
            span: close.span,
        });
    }
    Ok(MetricExpr::Range {
        op,
        range,
        param,
        grouping,
    })
}

/// `LogRange := LogExpr "[" Duration "]" ("offset" ["-"] Duration)?`.
///
/// **`offset` binds to the RANGE SELECTOR, never to the expression**
/// (issue #343). Measured against the pinned v3.7.4 container:
/// `rate({app="x"}[5m] offset 1h)` is a 200, while `rate({app="x"}[5m])
/// offset 1h` and `{app="x"} offset 1h` are both 400
/// `syntax error: unexpected offset`. Parsing it here — inside the range
/// selector — is what makes those two spellings errors for us too.
fn parse_log_range(cursor: &mut Cursor<'_>) -> Result<LogRange, LogQlError> {
    let selector = parse_log_expr(cursor)?;
    cursor.expect(&TokenKind::LBracket, "'['")?;
    let (raw, span) = cursor.expect_duration()?;
    let range = duration::parse_duration(&raw, span)?;
    // The 5-year rule, place 2 of 3 (issue #343, owner mandate):
    // `[2500000h]` is a 285-year window and is nonsense. See
    // [`check_span`].
    check_span("range", range.as_nanos(), &raw, span)?;
    cursor.expect(&TokenKind::RBracket, "']'")?;
    let offset_ns = parse_offset(cursor)?;
    Ok(LogRange {
        selector,
        range,
        unwrap: None, // M1 never populates the M6 `unwrap` stage
        offset_ns,
    })
}

/// The optional `offset ["-"] <duration>` suffix (issue #343).
///
/// Two boundaries, both MEASURED and both easy to "tidy" into a bug:
///
/// - **A NEGATIVE offset is ACCEPTED** and shifts the window FORWARD:
///   `rate({app="x"}[5m] offset -1h)` is a reference **200**. Rejecting
///   it as nonsense would be a divergence on the accept surface.
/// - **A bare `0` is REJECTED** while `0s` is fine: `offset 0` is a
///   reference **400** `syntax error: unexpected NUMBER, expecting
///   DURATION`. Accepting bare `0` "for symmetry" would over-accept —
///   the operand is a DURATION token, and `0` is a number.
///
/// Both fall out of requiring a duration token, so neither needs a
/// special case; they are stated because the next reader will wonder.
///
/// - **AN OVER-CAP MAGNITUDE IS REJECTED, NEVER CLAMPED**, in either
///   direction: `offset 43800h` and `offset -43800h` are the largest
///   accepted, one nanosecond more is a `400` ([`check_span`], the 5-year
///   rule, place 1 of 3). Clamping here would hand the planner an offset
///   the user never wrote — the same defect class issue #343 fixed one
///   layer down, where a saturating shift relocated the evaluation
///   window, and nothing downstream can detect an already-altered offset.
///
///   **The cap is OURS; the reference caps no offset magnitude here.**
///   MEASURED on the digest-pinned v3.7.4 oracle
///   (`grafana/loki@sha256:87f0a067…`, `/loki/api/v1/status/buildinfo`
///   reporting `3.7.4` / `b318f282` — that is the only path the
///   reference serves it on, registered twice at that one path:
///   `pkg/loki/loki.go:601 @ v3.7.4` on the internal server, guarded by
///   `if t.Cfg.InternalServer.Enable`, and `:603` unconditionally on the
///   public one; bare `/status/buildinfo` and `/api/v1/status/buildinfo`
///   are both a measured `404`), the reference's LEXER admits the whole `i64`
///   nanosecond domain, asymmetrically. The LEXER, said deliberately:
///   HTTP acceptance stops one value short of that band, and the
///   endpoint it drops is the next paragraph's subject, so this sentence
///   is not a wire-acceptance claim (issue #248 round 9; the same
///   sentence had already been narrowed the same way at three other
///   sites — `limits.rs`'s `MAX_QUERY_SPAN_NS`, `docs/features.md`'s
///   LogQL parity paragraph, and the `five-year-span-cap` ledger row —
///   and this was the fourth.
///
///   **FOUR SITES ARE AUDITED. THE SET IS NOT PROVEN CLOSED**, and a
///   fifth unqualified claim may exist in wording none of the sweeps
///   below matches. Rounds 9 and 10 wrote "those four are the whole
///   set"; the sweeps do not carry a universal, so the claim is
///   withdrawn (issue #248 round 11). They are written out verbatim
///   here, counts as of `3e04428`, so the next reader re-runs them
///   instead of trusting a number.
///   Sweep 1, the keyword: `git grep -lI five-year-span-cap` → 5 files,
///   whose fifth, `pulsus_read`'s `QuerySpanTooLong`, quantifies over no
///   band at all. CIRCULAR on its own — a file could describe the band
///   and never name the row.
///   Sweep 2, band markers, keyword-free:
///   `git grep -nI -iE 'offset' | grep -E 'i64|2562047|9223372036854775|43800|43801'`
///   → 88 lines in 27 files AT `3e04428`; the 23 not also in sweep 1 are
///   `pub offset_ns: i64`-class type declarations, PromQL/OTLP/template
///   arithmetic, and `b19_offset.test`'s
///   `offset-domain-edge-exact-arithmetic` block, read by hand.
///   Sweep 3, unbounded-acceptance phrases within ±12 lines of a line
///   matching `offset`, keyword-free. The matcher is INLINED into the
///   command, never held in a shell variable, so what is printed is what
///   runs in a shell that has never seen this file. An earlier revision
///   spelled the matcher out in prose and passed it as `"$PH"` without
///   showing the assignment: run as printed, `$PH` expands to the empty
///   regex, which every line matches, and the loop selects every file it
///   scans instead of 13 — 229 files, both at `42806d2` and over the tree
///   carrying this sentence, since the scanned set is `git grep -lI -i
///   offset` and this file was already in it (issue #248 round 12,
///   reproduced with `env -u PH`).
///   `for f in $(git grep -lI -i offset); do grep -i -C12 -E offset "$f" |
///   grep -qiE 'unbounded|uncapped|no cap|imposes no|admits|accepts (any|the|every)' &&
///   echo "$f"; done | sort -u`
///   → 13 files, the 9 not also in sweep 1 read by hand and carrying
///   no claim about the reference's offset band: a TraceQL cap, a
///   parser-vendoring note, `admits_instant`, a local named `uncapped`,
///   and three copies of "the only unbounded quantity left is where the
///   REQUEST sits", which is the other ledger row.
///
///   Three ways those sweeps fall short of closure, each reproducible.
///   NEITHER keyword-free sweep contains sweep 1's set: both MISS
///   `crates/pulsus-read/src/logql/error.rs`, so each drops a KNOWN
///   member. Round 10 charged that failure against a line-scoped variant
///   of sweep 3 only and offered sweep 3 as the repair; sweep 3 has it
///   too, and had it silently: round 10 gave sweep 2's residue as 22 of
///   27, which subtracts all five keyword files from it. Only four are
///   in it, so the residue is 23. Compute the intersection; do not
///   subtract an assumed containment.
///   Sweep 3's count is a function of its matcher — widening it by
///   `whole i64|entire|no limit` takes 13 → 20 — so no single number
///   from it means anything without the list beside it. And sweep 2
///   COUNTS ITS OWN DESCRIPTION: the line spelling out its markers,
///   above, matches it, so its total moves whenever this paragraph is
///   edited. That is the entire 87-vs-88 disagreement between rounds 10
///   and 11: 87 at `b3bd6c3`, 88 at `3e04428` — the one added line being
///   this sweep's own text — and neither round quoted the SHA it counted
///   at, which is why two correct measurements read as a contradiction.
///   Writing this paragraph out costs two more self-matches and saves
///   one, so over the tree carrying this sentence the same command
///   returns 89. Quote the SHA with the number or do not quote the
///   number.)
///   `offset 2562047h47m16s854ms775us807ns` (`i64::MAX`) → 200, one ns
///   more → 400 `syntax error: unexpected NUMBER, expecting DURATION`;
///   `offset -9223372036854775808ns` (`i64::MIN`) is the lexer's floor,
///   one ns more negative → 400 `syntax error: unexpected -, expecting
///   DURATION`.
///
///   Those are LEXER verdicts. On the WIRE the whole negative `i64` band
///   is a `200` SAVE `i64::MIN` itself, which a frontend that has not
///   already answered its neighbour REFUSES (issue #248 rounds 5 to 7; round 5
///   reported that `400` as unconditional, round 6's wording made the
///   `200` unconditional instead, and it is neither). The refusal is
///   `400 this data is no longer available, it is past now -
///   max_query_lookback (0s)`:
///   the value parses, and what refuses it is Go's `-offset` overflowing
///   at that one value inside the shard resolver's `through =
///   end.Add(-Offset)` while `from = start.Add(-(Interval + Offset))`
///   moves the other way, inverting the window
///   (`pkg/querier/queryrange/shard_resolver.go:94-104`,
///   rejected at `pkg/querier/limits/validation.go:92-94` @ v3.7.4).
///   Only a warm cache turns it into a `200`:
///   `cache_index_stats_results` (default true,
///   `pkg/querier/queryrange/roundtrip.go:66`) answers it from the
///   `i64::MIN + 1` entry — the two are one nanosecond apart and the
///   index-stats request is in milliseconds. Set that option to `false`
///   and the `400` stands in every probe order. It is an artefact of one
///   value rather than a bound — `i64::MIN + 1` is a `200` in any order,
///   with the cache on or off — but the accepted band this paragraph
///   describes therefore stops one value short of the domain. The
///   order-dependent probe table is in the `five-year-span-cap` ledger
///   row.
///
///   That asymmetry is not an accident of its own, and it is ONE
///   function's doing. The magnitude is lexed by `parseDuration` (v3.7.4
///   `pkg/logql/syntax/lex.go:326`), which tries the vendored
///   `model.ParseDuration` and falls back to Go's `time.ParseDuration`.
///   Both endpoints above are spelled in units `model` does not have —
///   its map is `ms`/`s`/`m`/`h`/`d`/`w`/`y`, no `us` and no `ns`
///   (`vendor/github.com/prometheus/common/model/time.go:189-200` @
///   v3.7.4) — and a leading `-` fails its leading-digit check (`:219`)
///   besides, so neither endpoint reaches `model`'s overflow branches at
///   all; the stdlib decides both. There the total accumulates in a
///   `uint64` the loop lets reach `1<<63` (`src/time/format.go:1707` at
///   go1.25.5, `:1717` at the go1.26.5 the reference is actually built
///   with), and a negative returns at `if neg {
///   return -Duration(d) }` (`:1711` / `:1721`) BEFORE the positive-only
///   `d > 1<<63-1` (`:1714` / `:1724`) — the two toolchains' arithmetic
///   is identical and the line numbers are the whole of the difference,
///   compared in
///   [`tests::both_duration_literals_cap_at_five_years_and_refuse_rather_than_clamp`].
///   Hence a ceiling of `i64::MAX` and a floor
///   of `i64::MIN`, one check on each side. Recorded because our cap
///   sits far inside it: the `i64` band, its negative endpoint aside, is
///   reference-accepted and PulsusDB-refused, ledgered as
///   `five-year-span-cap`. PAST that band the reference refuses too (the
///   three rows in
///   [`tests::both_duration_literals_cap_at_five_years_and_refuse_rather_than_clamp`]),
///   so there is no divergence out there to record.
///
/// The keyword goes through [`is_kw`] like every other keyword in this
/// file (issue #339's rule): the reference's lexer folds keywords, so
/// `OFFSET 1m` is a 200 there. A byte compare here made `offset` the one
/// keyword that did not fold — recorded as a reference-accept /
/// ours-reject row in the #339 census until this landed. Identifier
/// PAYLOADS still never fold; that asymmetry is unchanged.
fn parse_offset(cursor: &mut Cursor<'_>) -> Result<Option<i64>, LogQlError> {
    let is_offset_kw = matches!(&cursor.peek().kind, TokenKind::Ident(k) if is_kw(k, "offset"));
    if !is_offset_kw {
        return Ok(None);
    }
    cursor.advance();
    let negative = matches!(cursor.peek().kind, TokenKind::Minus);
    if negative {
        cursor.advance();
    }
    let (raw, span) = cursor.expect_duration()?;
    let nanos = duration::parse_duration(&raw, span)?.as_nanos();
    // The magnitude is checked BEFORE any conversion, so nothing can be
    // narrowed or clamped on the way in. The signed literal is echoed as
    // the user wrote it, sign included.
    let written = if negative {
        format!("-{raw}")
    } else {
        raw.clone()
    };
    let magnitude = check_span("offset", nanos, &written, span)?;
    // `magnitude <= MAX_QUERY_SPAN_NS` was just established, so negating
    // it cannot overflow.
    Ok(Some(if negative { -magnitude } else { magnitude }))
}

/// **The 5-year rule** (issue #343, owner mandate): nothing in a LogQL
/// query may span more than [`MAX_QUERY_SPAN_NS`] — 43,800 h = 5 × 365 d.
/// One check, used by both duration literals; the query's own
/// `start`-to-`end` span is the third place, enforced at the planner
/// against this same constant.
///
/// Returns the value as a validated `i64` on success, so a caller cannot
/// pass the check and then convert unsafely. Over the cap it is a `400`
/// echoing `written` — the literal AS SENT, in the user's own units —
/// never a clamped value: someone asking for a stupid number is told
/// plainly rather than silently handed a different answer.
///
/// **A deliberate divergence**, and a narrower one than this comment
/// used to claim: the reference DOES bound a query's span, through
/// `max_query_length` (default `721h`), over a window that includes the
/// `[range]` selector but not the offset. Retention is days to months,
/// so this refuses nothing a real deployment does. The re-measurement
/// and the source lines are on the `five-year-span-cap` ledger row.
fn check_span(
    what: &'static str,
    nanos: u64,
    written: &str,
    span: Span,
) -> Result<i64, LogQlError> {
    let too_long = || LogQlError::SpanTooLong {
        what,
        raw: written.to_string(),
        cap_hours: MAX_QUERY_SPAN_HOURS,
        span,
    };
    let ns = i64::try_from(nanos).map_err(|_| too_long())?;
    if ns > MAX_QUERY_SPAN_NS {
        return Err(too_long());
    }
    Ok(ns)
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
    // The `of` keyword is matched case-insensitively through [`is_kw`],
    // consistent with every other PulsusDB keyword (`sum`, `by`,
    // `unwrap`) since issue #339 closed the whole-surface folding gap.
    match &cursor.peek().kind {
        TokenKind::Ident(name) if is_kw(name, "of") => {
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
    let is_grouping_keyword = matches!(&cursor.peek().kind, TokenKind::Ident(name) if is_kw(name, "by") || is_kw(name, "without"))
        && matches!(cursor.peek2().kind, TokenKind::LParen);
    if is_grouping_keyword {
        Ok(Some(parse_grouping(cursor)?))
    } else {
        Ok(None)
    }
}

fn parse_grouping(cursor: &mut Cursor<'_>) -> Result<Grouping, LogQlError> {
    let (name, span) = cursor.expect_ident()?;
    let kind = match kw(&name).as_str() {
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

#[cfg(test)]
mod tests {
    use super::{MAX_QUERY_SPAN_HOURS, MAX_QUERY_SPAN_NS};
    use crate::error::LogQlError;

    /// [`is_kw`] compares `name` case-insensitively against a `want` that
    /// must already be lowercase — a mixed-case `want` would silently
    /// never match, and `debug_assert` only catches it if that call site
    /// is exercised. This is the static half: every keyword literal in
    /// this file, extracted from the source, must be lowercase.
    ///
    /// It is a census, not a spot check: it reads the file rather than a
    /// hand-maintained list, so a new `is_kw(..., "By")` fails here
    /// without anyone remembering to add it.
    #[test]
    fn every_keyword_literal_in_this_file_is_lowercase() {
        let src = include_str!("parser.rs");
        let mut offenders = Vec::new();
        let mut found = 0usize;
        for (n, line) in src.lines().enumerate() {
            // Strip comments first: this very test documents the shape
            // `is_kw(..., "By")` in prose, and a census that flagged its
            // own documentation would be noise, not a finding.
            let code = line.split_once("//").map_or(line, |(before, _)| before);
            let mut rest = code;
            while let Some(at) = rest.find("is_kw(") {
                rest = &rest[at + "is_kw(".len()..];
                // The keyword is the quoted literal argument.
                let Some(open) = rest.find('"') else { break };
                let after = &rest[open + 1..];
                let Some(close) = after.find('"') else { break };
                let literal = &after[..close];
                found += 1;
                if literal != literal.to_ascii_lowercase() {
                    offenders.push(format!("{}: is_kw(..., {literal:?})", n + 1));
                }
                rest = &after[close + 1..];
            }
        }
        // The extractor must actually be finding call sites: a rename
        // that made it match nothing would leave this green forever.
        assert!(
            found >= 15,
            "expected the parser's keyword call sites, found {found} — the extractor is stale"
        );
        assert!(
            offenders.is_empty(),
            "keyword literals must be lowercase (is_kw folds the INPUT, not the expectation):\n{}",
            offenders.join("\n")
        );
    }

    /// **The 5-year rule on both duration literals** (issue #343, owner
    /// mandate): `offset` and `[range]` each cap at
    /// [`MAX_QUERY_SPAN_NS`] — 43,800 h — and are REFUSED past it, never
    /// clamped. The third place, the query's own `start`-to-`end` span, is
    /// enforced at the planner against the same constant.
    ///
    /// The predecessor did `i64::try_from(nanos).unwrap_or(i64::MAX)`,
    /// which handed the planner an offset the user never wrote — the
    /// identical clamp-instead-of-handle defect this issue exists to fix,
    /// one layer above the code it fixed, and undetectable from below. The
    /// cap subsumes it: nothing past 43,800 h reaches a conversion at all.
    ///
    /// The offsets rejected here that the reference's own lexer ACCEPTS
    /// are reference **200**s — measured on the digest-pinned v3.7.4
    /// oracle `grafana/loki@sha256:87f0a067…`, buildinfo `3.7.4` /
    /// `b318f282`: `offset ±43801h` and
    /// `offset 2562047h47m16s854ms775us807ns` are all 200s, instant and
    /// range alike, because the offset cancels out of the window
    /// `max_query_length` is measured over. That is the ledgered
    /// `five-year-span-cap` divergence and this test is what pins it.
    ///
    /// The three out-of-`i64` literals in the refusal list below are NOT
    /// that. All three are measured reference `400`s at the LEXER on the
    /// same oracle: `9223372036854775808ns` and `18446744073709551615ns`
    /// both give `parse error at line 1, col 38: syntax error: unexpected
    /// NUMBER, expecting DURATION`, and `-9223372036854775809ns` gives
    /// `unexpected -, expecting DURATION`. Those rows are reject PARITY,
    /// not divergence — the paragraph above used to cover them with an
    /// "every".
    ///
    /// **Which branch refuses each — TWO branches over the three rows,
    /// not one and not three.**
    /// `parseDuration` (v3.7.4 `pkg/logql/syntax/lex.go:326`) tries the
    /// vendored `model.ParseDuration` and falls back to Go's
    /// `time.ParseDuration`. NEITHER of `model`'s two overflow checks is
    /// what fires here, which this comment used to say it was: its unit
    /// map is `ms`/`s`/`m`/`h`/`d`/`w`/`y` with no `ns` at all
    /// (`vendor/github.com/prometheus/common/model/time.go:189-200` @
    /// v3.7.4, prometheus/common v0.67.5), so a pure-`ns` literal stops
    /// at the unit lookup with `unknown unit "ns"` (`:240-242`) and
    /// never reaches `v > 1<<63/unit.mult` or `dur > 1<<63-1`
    /// (`:249-255`); the `-` form stops one check earlier still, at the
    /// leading-digit test (`:219`). All three are therefore decided by
    /// the stdlib, and the first row's branch is not the other two's.
    /// Line numbers below are `go1.25.5 src/time/format.go`'s; the
    /// TOOLCHAIN THE REFERENCE ITSELF USES IS go1.26.5, and that is now
    /// the version compared rather than assumed (issue #248 round 9 —
    /// an earlier wording generalised from go1.23 and go1.25.5, neither
    /// of which the reference builds with). Loki v3.7.4 declares
    /// `go 1.26.5` (`go.mod:3`, `cmd/loki/Dockerfile:1
    /// ARG GO_VERSION=1.26.5`) and the digest-pinned image's own binary
    /// agrees: `go version -m` on `/usr/bin/loki` extracted from
    /// `grafana/loki@sha256:87f0a067…` prints `go1.26.5` and
    /// `mod github.com/grafana/loki/v3 v3.0.0-20260722033256-b318f2829f0a`
    /// (the build-info endpoint cannot answer this. Measured on the
    /// pinned digest: `GET /loki/api/v1/status/buildinfo` → `200`
    /// `{"version":"3.7.4","revision":"b318f282",…,"goVersion":""}`.
    /// That path is the endpoint — the only one it is served on,
    /// registered at that one path twice: `pkg/loki/loki.go:601 @ v3.7.4`
    /// on the internal server, behind `if t.Cfg.InternalServer.Enable`,
    /// and `:603` unconditionally on the public one; bare
    /// `/status/buildinfo` and `/api/v1/status/buildinfo` are both `404`,
    /// which is what this comment named until issue #248 round 10.
    ///
    /// **Empty in the RELEASE build, which is the build we pin — not
    /// unpopulatable in principle.** The release `ldflags` set
    /// Branch/Version/Revision/BuildUser/BuildDate and NOT `GoVersion`
    /// (`Makefile:46-50` @ v3.7.4), and `versionHandler` reads the
    /// package var `build.GoVersion` directly
    /// (`pkg/loki/version_handler.go:12-20`,
    /// `pkg/util/build/build.go:14-21` @ v3.7.4) rather than
    /// `build.GetVersion()`, the accessor whose `init()` fills the field
    /// from `runtime.Version()` (`build.go:23-30`) — so nothing in a
    /// release build ever assigns it. That is a fact about the shipped
    /// binary, not about the tree: `-X …/pkg/util/build.GoVersion=…` at
    /// link time, or a plain assignment, populates it, and the
    /// reference's own `version_handler_test.go:20 @ v3.7.4` sets
    /// `build.GoVersion = "42"` and requires the handler to echo it
    /// (`:36` in the expected literal opened at `:30`, `assert.JSONEq`
    /// at `:40`) —
    /// RUN, not merely read: `go test ./pkg/loki/ -run
    /// '^TestVersionHandler$'` on the v3.7.4 checkout passes, under the
    /// go1.26.5 the module declares. Round 10 wrote "structurally so,
    /// not by accident of this image", which claims no build of the tree
    /// can populate it; a missing ldflag cannot show that, and that
    /// passing test refutes it. What the ldflag DOES establish is the
    /// only thing this comment needs — the release binary we pin
    /// reports it empty (issue #248 round 11)).
    /// Against go1.26.5's `src/time/format.go`:
    /// `leadingInt` is byte-identical to go1.25.5's and at the SAME
    /// lines (`:1554-1572`, ceiling `:1566`), `unitMap` is
    /// byte-identical (moved to `:1615`), and `ParseDuration` differs at
    /// ten lines, ALL of them error construction —
    /// `errors.New("time: … " + quote(orig))` became
    /// `&parseDurationError{…}`, whose `Error()` renders the identical
    /// string (`:1606-1613`). No arithmetic, no control flow: the three
    /// branches cited below sit unchanged at `:1717` / `:1721` /
    /// `:1724`, i.e. go1.25.5's `:1707` / `:1711` / `:1714` shifted by
    /// the ten lines that type occupies. Measured as well as read: nine
    /// literals — the three refusals and two discriminators below,
    /// `-9223372036854775808ns`, both `i64::MAX` spellings
    /// (`9223372036854775807ns` and
    /// `2562047h47m16s854ms775us807ns`) and the one-nanosecond-over
    /// `2562047h47m16s854ms775us808ns` — through `time.ParseDuration`
    /// under BOTH toolchains give byte-identical verdicts and error
    /// texts. So the branch does not turn on the toolchain, and that is
    /// now checked at the one toolchain that matters:
    ///
    /// - `9223372036854775808ns` is `1<<63` EXACTLY. `leadingInt`
    ///   admits it — its ceiling is `1<<63` itself (`:1566`) — and the
    ///   in-loop `d > 1<<63` (`:1707`) does not fire either. What
    ///   refuses it is the trailing POSITIVE-ONLY `d > 1<<63-1`
    ///   (`:1714`), which is also why the identical magnitude spelled
    ///   `-9223372036854775808ns` parses: `if neg { return -Duration(d) }`
    ///   (`:1711`) returns before that check.
    /// - `18446744073709551615ns` (`1<<64 - 1`) never reaches a unit at
    ///   all: `leadingInt` overflows on the digits at `x > 1<<63`
    ///   (`:1566`).
    /// - `-9223372036854775809ns` is that SAME `leadingInt` overflow, on
    ///   the magnitude, after `model` refused the leading `-`. So these
    ///   two share a branch and the first row does not.
    ///
    /// Which branch it is cannot be read off the wire — the lexer
    /// discards `parseDuration`'s error and emits NUMBER (or, for the
    /// `-` form, the `-` rune), so all of them surface as the one syntax
    /// error. Established by running the vendored file and the stdlib
    /// over these literals plus two discriminators that separate
    /// `leadingInt` from the unit lookup: `9223372036854775808x` gives
    /// `unknown unit "x"` (so `leadingInt` passed) while
    /// `18446744073709551615x` gives `invalid duration` (so it did not).
    ///
    /// The `[range]` half is NOT symmetric with it, which this comment
    /// used to imply by saying "both literals": on a range query the
    /// selector counts against `max_query_length` (`[720h]` over a `1h`
    /// request span is already a 400 there), and on an instant query the
    /// reference admits it and then decomposes it into per-hour
    /// subqueries that do not answer. Issue #248 round 5 re-measured
    /// both; the ledger row carries the table.
    #[test]
    fn both_duration_literals_cap_at_five_years_and_refuse_rather_than_clamp() {
        const CAP: i64 = MAX_QUERY_SPAN_NS;

        // --- `offset`: accepted up to the cap, either direction.
        for (query, want) in [
            (r#"count_over_time({app="x"}[5m] offset 43800h)"#, CAP),
            (r#"count_over_time({app="x"}[5m] offset -43800h)"#, -CAP),
            (
                r#"count_over_time({app="x"}[5m] offset 1h)"#,
                3_600_000_000_000,
            ),
            (r#"count_over_time({app="x"}[5m] offset 0s)"#, 0),
        ] {
            let expr = crate::parse(query).unwrap_or_else(|e| panic!("{query}: {e}"));
            assert_eq!(range_of(&expr).offset_ns, Some(want), "{query}");
        }

        // --- `offset`: refused past it, both signs, and refused rather
        // than narrowed for magnitudes that do not even fit `i64`.
        for query in [
            r#"count_over_time({app="x"}[5m] offset 43801h)"#,
            r#"count_over_time({app="x"}[5m] offset -43801h)"#,
            r#"count_over_time({app="x"}[5m] offset 2562047h)"#,
            r#"count_over_time({app="x"}[5m] offset 9223372036854775808ns)"#,
            r#"count_over_time({app="x"}[5m] offset 18446744073709551615ns)"#,
            r#"count_over_time({app="x"}[5m] offset -9223372036854775809ns)"#,
        ] {
            let err = crate::parse(query).expect_err("must be refused, never clamped");
            assert!(
                matches!(&err, LogQlError::SpanTooLong { what: "offset", .. }),
                "{query}: {err}"
            );
        }

        // --- `[range]`: the same cap, the same refusal.
        let expr = crate::parse(r#"count_over_time({app="x"}[43800h])"#).expect("at the cap");
        assert_eq!(range_of(&expr).range.as_nanos(), CAP as u64);
        for query in [
            r#"count_over_time({app="x"}[43801h])"#,
            r#"count_over_time({app="x"}[2500000h])"#,
            r#"count_over_time({app="x"}[9223372036854775808ns])"#,
        ] {
            let err = crate::parse(query).expect_err("must be refused");
            assert!(
                matches!(&err, LogQlError::SpanTooLong { what: "range", .. }),
                "{query}: {err}"
            );
        }

        // The message echoes the literal AS SENT, sign included, and
        // quotes the cap in hours derived from the constant.
        let err = crate::parse(r#"count_over_time({app="x"}[5m] offset -43801h)"#).unwrap_err();
        assert_eq!(err.to_string(), "offset too long (-43801h > 43800h)");
        let err = crate::parse(r#"count_over_time({app="x"}[43801h])"#).unwrap_err();
        assert_eq!(err.to_string(), "range too long (43801h > 43800h)");
    }

    /// `MAX_QUERY_SPAN_NS` IS five years, so the "(5 years)" every doc and
    /// ledger line says cannot quietly become false.
    #[test]
    fn the_span_cap_is_exactly_five_365_day_years() {
        assert_eq!(MAX_QUERY_SPAN_NS, 5 * 365 * 24 * 3_600_000_000_000);
        assert_eq!(MAX_QUERY_SPAN_HOURS, 43_800);
    }

    /// The `LogRange` of a single-range metric query, for the boundary
    /// test above. Kept local: it walks exactly the one shape that test
    /// builds and has no other caller.
    fn range_of(expr: &crate::ast::Expr) -> &crate::ast::LogRange {
        match expr {
            crate::ast::Expr::Metric(crate::ast::MetricExpr::Range { range, .. }) => range,
            other => panic!("expected a range aggregation, got {other:?}"),
        }
    }
}
