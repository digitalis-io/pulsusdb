//! Recursive-descent parser over `&[Token]`. A `Cursor` tracks the
//! current position; a `depth` counter threaded through metric-expression
//! parsing guards against unbounded nesting (`sum(sum(sum(...)))`) —
//! [`crate::error::MAX_DEPTH`] levels return `RecursionLimitExceeded`
//! instead of overflowing the call stack.
//!
//! Disambiguation of the overloaded `!=`/`!~` tokens (selector matcher,
//! line filter, or — `!=` only — an M6 binary comparison) is purely
//! positional: the selector-matcher loop, the pipeline-stage loop, and
//! the post-`MetricExpr` binary-op check each own their token set, and
//! none of them overlap in when they run (architect plan amendments 1-3).

use crate::ast::{
    self, CompareOp, Expr, Grouping, GroupingKind, LabelExtraction, LabelFilterExpr, LabelFmt,
    LineFilter, LineFilterOp, LogExpr, LogRange, MatchOp, Matcher, MetricExpr, NumericLiteral,
    ParserStage, RangeAggOp, Stage, StreamSelector, Unwrap, VectorAggOp,
};
use crate::duration;
use crate::error::{LogQlError, MAX_DEPTH};
use crate::lexer;
use crate::token::{Token, TokenKind};

/// Parses a full LogQL query into an [`Expr`] — the #11 planner contract.
pub fn parse(input: &str) -> Result<Expr, LogQlError> {
    let tokens = lexer::tokenize(input)?;
    let mut cursor = Cursor::new(&tokens);
    let expr = parse_expr(&mut cursor, 0)?;
    expect_eof(&cursor)?;
    Ok(expr)
}

/// Parses just a stream selector (`{label_matcher, ...}`) — the entry
/// point `/series` and `/label/{name}/values` (#13) use, since those
/// endpoints never see a full LogQL pipeline.
pub fn parse_selector(input: &str) -> Result<StreamSelector, LogQlError> {
    let tokens = lexer::tokenize(input)?;
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
        TokenKind::String(s) => format!("string {s:?}"),
        TokenKind::Duration(s) => format!("duration {s:?}"),
        TokenKind::Number(s) => format!("number {s:?}"),
        TokenKind::Eof => "end of query".to_string(),
    }
}

/// `Expr := LogExpr | MetricExpr`. A query starting with `{` is always a
/// log expression; a query starting with an identifier is always a
/// metric expression (a call to a range or vector aggregation function).
fn parse_expr(cursor: &mut Cursor<'_>, depth: usize) -> Result<Expr, LogQlError> {
    match &cursor.peek().kind {
        TokenKind::LBrace => Ok(Expr::Log(parse_log_expr(cursor)?)),
        TokenKind::Ident(_) => {
            let metric = parse_metric_expr(cursor, depth)?;
            check_no_binary_op(cursor)?;
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

/// After a complete top-level `MetricExpr`, checks whether the next token
/// starts an M6 binary operation (`+ - * / % ^ == != > < >= <= and or
/// unless`) and names it if so. `!~`/`|=`/`|~` are never binary operators
/// in any LogQL milestone (amendment 3) — those, if found here, are a
/// plain position-bearing `UnexpectedToken`, not `NotYetSupported`.
fn check_no_binary_op(cursor: &Cursor<'_>) -> Result<(), LogQlError> {
    let tok = cursor.peek();
    match &tok.kind {
        TokenKind::Plus
        | TokenKind::Minus
        | TokenKind::Star
        | TokenKind::Slash
        | TokenKind::Percent
        | TokenKind::Caret
        | TokenKind::EqEq
        | TokenKind::Neq
        | TokenKind::Gt
        | TokenKind::Lt
        | TokenKind::Gte
        | TokenKind::Lte => Err(LogQlError::NotYetSupported {
            construct: "binary operation".to_string(),
            span: tok.span,
        }),
        TokenKind::Ident(name) if ast::BINARY_OP_KEYWORDS.contains(&name.as_str()) => {
            Err(LogQlError::NotYetSupported {
                construct: "binary operation".to_string(),
                span: tok.span,
            })
        }
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
            let (value, _) = cursor.expect_string()?;
            pipeline.push(Stage::LineFilter(LineFilter { op, value }));
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
                Ok(Stage::Parser(ParserStage::Logfmt {
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
            name if ast::REMAINING_UNSUPPORTED_STAGES.contains(&name) => {
                Err(LogQlError::NotYetSupported {
                    construct: name.to_string(),
                    span: tok.span,
                })
            }
            // Any other identifier at stage position starts a label
            // filter (e.g. `| status="500"`, `| status >= 500`).
            _ => Ok(Stage::LabelFilter(parse_label_filter_or(cursor)?)),
        },
        TokenKind::LParen => Ok(Stage::LabelFilter(parse_label_filter_or(cursor)?)),
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
/// `and`/`,` bind tighter, parentheses group.
fn parse_label_filter_or(cursor: &mut Cursor<'_>) -> Result<LabelFilterExpr, LogQlError> {
    let mut left = parse_label_filter_and(cursor)?;
    while matches!(&cursor.peek().kind, TokenKind::Ident(name) if name == "or") {
        cursor.advance();
        let right = parse_label_filter_and(cursor)?;
        left = LabelFilterExpr::Or(Box::new(left), Box::new(right));
    }
    Ok(left)
}

fn parse_label_filter_and(cursor: &mut Cursor<'_>) -> Result<LabelFilterExpr, LogQlError> {
    let mut left = parse_label_filter_factor(cursor)?;
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
        let right = parse_label_filter_factor(cursor)?;
        left = LabelFilterExpr::And(Box::new(left), Box::new(right));
    }
}

fn parse_label_filter_factor(cursor: &mut Cursor<'_>) -> Result<LabelFilterExpr, LogQlError> {
    if matches!(cursor.peek().kind, TokenKind::LParen) {
        cursor.advance();
        let inner = parse_label_filter_or(cursor)?;
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

/// `MetricExpr := <range-agg-name> "(" LogRange ")" | <vector-agg-name>
/// Grouping? "(" MetricExpr ")" Grouping?` — dispatches on the leading
/// identifier: implemented range/vector aggregation names build the
/// corresponding node; every documented-but-unimplemented M6 aggregation
/// name resolves to a named `NotYetSupported`; anything else is an
/// `UnexpectedToken`.
fn parse_metric_expr(cursor: &mut Cursor<'_>, depth: usize) -> Result<MetricExpr, LogQlError> {
    if depth >= MAX_DEPTH {
        return Err(LogQlError::RecursionLimitExceeded {
            span: cursor.peek().span,
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

    if let Some(op) = RangeAggOp::from_ident(&name) {
        cursor.advance();
        return parse_range_agg_call(cursor, op);
    }
    if let Some(op) = VectorAggOp::from_ident(&name) {
        cursor.advance();
        return parse_vector_agg_call(cursor, depth, op);
    }
    if ast::FUTURE_RANGE_AGG.contains(&name.as_str())
        || ast::FUTURE_VECTOR_AGG.contains(&name.as_str())
    {
        return Err(LogQlError::NotYetSupported {
            construct: name,
            span: tok.span,
        });
    }
    Err(LogQlError::UnexpectedToken {
        found: describe(&tok.kind),
        expected: "an aggregation function".to_string(),
        span: tok.span,
    })
}

fn parse_range_agg_call(cursor: &mut Cursor<'_>, op: RangeAggOp) -> Result<MetricExpr, LogQlError> {
    cursor.expect(&TokenKind::LParen, "'('")?;
    let range = parse_log_range(cursor)?;
    cursor.expect(&TokenKind::RParen, "')'")?;
    Ok(MetricExpr::Range {
        op,
        range,
        param: None, // M1 never populates the M6 quantile_over_time parameter
    })
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

fn parse_vector_agg_call(
    cursor: &mut Cursor<'_>,
    depth: usize,
    op: VectorAggOp,
) -> Result<MetricExpr, LogQlError> {
    let mut grouping = maybe_grouping(cursor)?;
    cursor.expect(&TokenKind::LParen, "'('")?;
    let inner = parse_metric_expr(cursor, depth + 1)?;
    cursor.expect(&TokenKind::RParen, "')'")?;
    if grouping.is_none() {
        grouping = maybe_grouping(cursor)?;
    }
    Ok(MetricExpr::Vector {
        op,
        grouping,
        inner: Box::new(inner),
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
    Ok(Grouping { kind, labels })
}
