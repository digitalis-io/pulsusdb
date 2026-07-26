//! The LogQL AST — the stable contract the #11 planner/SQL generator
//! consumes (docs/architecture.md §5.3). Every type derives `Debug`,
//! `Clone`, `PartialEq`, `Eq`, `Hash` (the last for #11's plan-cache/dedup
//! and so the AST can key a cache; also load-bearing for the `Display`
//! round-trip test oracle: `parse(ast.to_string()) == ast`).
//!
//! `Stage` (M1: only `LineFilter`), `RangeAggOp`, and `VectorAggOp` are
//! the designated M6 growth points — parity lands as additive variants,
//! never a reshape of these types or their fields (architect plan: "AST
//! contract stability"). Issue M6-10 exercised those growth points (the
//! full over-time set on `RangeAggOp`; `stddev`/`stdvar`/`topk`/`bottomk`
//! on `VectorAggOp`) and added the adjudicated M6-10 growth points, all
//! additive: the `param` field on [`MetricExpr::Vector`] (the raw
//! `topk`/`bottomk` `k`), the [`MetricExpr::Literal`] variant (a bare
//! scalar number), and the [`MetricExpr::Binary`] variant with [`BinOp`]/
//! [`BinModifier`] (binary operations). Issue #91 extended
//! [`BinModifier`] with the `on`/`ignoring`/`group_left`/`group_right`
//! vector-matching clause ([`VectorMatching`]) — additive, though
//! [`BinModifier`] drops its `Copy` derive to own the label list.

use std::fmt;

/// A parsed LogQL query: either a log-stream query (`resultType: streams`
/// in the query API) or a metric query over a log range (`resultType:
/// vector`/`matrix`) — docs/api.md §2.1.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Expr {
    Log(LogExpr),
    Metric(MetricExpr),
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Log(e) => write!(f, "{e}"),
            Expr::Metric(e) => write!(f, "{e}"),
        }
    }
}

/// A stream selector plus its pipeline of stages. M1 implements only
/// `Stage::LineFilter`; M6 adds parsers, label filters, `line_format`,
/// `label_format`, and `unwrap` as additive `Stage` variants.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogExpr {
    pub selector: StreamSelector,
    pub pipeline: Vec<Stage>,
}

impl fmt::Display for LogExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.selector)?;
        for stage in &self.pipeline {
            write!(f, " {stage}")?;
        }
        Ok(())
    }
}

/// `{label_matcher, ...}` — never empty after a successful parse
/// (`LogQlError::EmptySelector`); match-everything rejection is a
/// planner/semantic concern deferred to #11 (task-manager resolution #2).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamSelector {
    pub matchers: Vec<Matcher>,
}

impl fmt::Display for StreamSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{")?;
        for (i, m) in self.matchers.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{m}")?;
        }
        write!(f, "}}")
    }
}

/// A single `name <op> "value"` label matcher. `value` is the raw pattern
/// for `Re`/`Nre` — this crate never compiles or validates regexes
/// (architect plan: "Regex not validated").
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Matcher {
    pub name: String,
    pub op: MatchOp,
    pub value: String,
}

impl fmt::Display for Matcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}{}", self.name, self.op, quote(&self.value))
    }
}

/// `=` `!=` `=~` `!~` inside a stream selector `{...}`. The same `!=`/`!~`
/// tokens are also line-filter operators and (for `!=` only) an M6 binary
/// comparison — disambiguated purely by parser position, never by the
/// lexer (architect plan amendments 1-3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MatchOp {
    Eq,
    Neq,
    Re,
    Nre,
}

impl fmt::Display for MatchOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            MatchOp::Eq => "=",
            MatchOp::Neq => "!=",
            MatchOp::Re => "=~",
            MatchOp::Nre => "!~",
        };
        write!(f, "{s}")
    }
}

/// A pipeline stage. M1 implemented only line filters; M6-09 adds the
/// parsers (`json`/`logfmt`/`regexp`/`pattern`), label filters,
/// `line_format`, `label_format`, and `unwrap` as additive variants, per
/// the AST-stability contract. `unwrap` is an ordered stage in the same
/// pipeline (the grammar allows only label filters after it — enforced by
/// the parser); the still-unimplemented stage keywords are listed in
/// [`REMAINING_UNSUPPORTED_STAGES`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Stage {
    LineFilter(LineFilter),
    Parser(ParserStage),
    LabelFilter(LabelFilterExpr),
    /// `| line_format "<template>"` — raw template text (e.g.
    /// `"{{.method}} {{.status}}"`); template validation/compilation is a
    /// `pulsus-read` concern (this crate stays purely syntactic).
    LineFormat(String),
    LabelFormat(Vec<LabelFmt>),
    Unwrap(Unwrap),
    /// `| unpack` — parse the line as a packed JSON object, promoting the
    /// `_entry` field back to the line and other string fields to labels
    /// (issue #200). No arguments.
    Unpack,
    /// `| decolorize` — strip ANSI SGR color escape sequences from the line
    /// (issue #200). No arguments.
    Decolorize,
    /// `| drop <elem> (, <elem>)*` — remove the listed labels (issue #200).
    Drop(Vec<DropKeepElem>),
    /// `| keep <elem> (, <elem>)*` — retain only the listed labels (issue
    /// #200).
    Keep(Vec<DropKeepElem>),
}

/// One element of a `drop`/`keep` label list: a bare label name, or a
/// label qualified by a value matcher (`level="info"`, `status=~"5.."`).
/// The matcher, when present, gates the drop/keep on the label's current
/// value (`pulsus-read` evaluates it — this crate stays purely syntactic).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DropKeepElem {
    pub label: String,
    pub matcher: Option<LabelMatch>,
}

impl fmt::Display for DropKeepElem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.label)?;
        if let Some(m) = &self.matcher {
            write!(f, "{}{}", m.op, quote(&m.value))?;
        }
        Ok(())
    }
}

/// A `<op> "value"` matcher qualifying a `drop`/`keep` element. `op` is a
/// stream-selector [`MatchOp`] (`=`/`!=`/`=~`/`!~`); `value` is the raw
/// pattern (regex bodies are never compiled here — the `Matcher` contract).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LabelMatch {
    pub op: MatchOp,
    pub value: String,
}

impl fmt::Display for Stage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Stage::LineFilter(lf) => write!(f, "{lf}"),
            Stage::Parser(p) => write!(f, "| {p}"),
            Stage::LabelFilter(lf) => write!(f, "| {lf}"),
            Stage::LineFormat(tmpl) => write!(f, "| line_format {}", quote(tmpl)),
            Stage::LabelFormat(fmts) => {
                write!(f, "| label_format ")?;
                for (i, lf) in fmts.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{lf}")?;
                }
                Ok(())
            }
            Stage::Unwrap(u) => match &u.conversion {
                Some(conv) => write!(f, "| unwrap {conv}({})", u.label),
                None => write!(f, "| unwrap {}", u.label),
            },
            Stage::Unpack => write!(f, "| unpack"),
            Stage::Decolorize => write!(f, "| decolorize"),
            Stage::Drop(elems) => write_drop_keep(f, "drop", elems),
            Stage::Keep(elems) => write_drop_keep(f, "keep", elems),
        }
    }
}

fn write_drop_keep(
    f: &mut fmt::Formatter<'_>,
    keyword: &str,
    elems: &[DropKeepElem],
) -> fmt::Result {
    write!(f, "| {keyword} ")?;
    for (i, e) in elems.iter().enumerate() {
        if i > 0 {
            write!(f, ", ")?;
        }
        write!(f, "{e}")?;
    }
    Ok(())
}

/// A parser stage: extracts labels from the (unmodified) log line. Regex
/// and pattern bodies are raw text here — validated/compiled in
/// `pulsus-read` (the "regex not validated" contract, same as `Matcher`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ParserStage {
    /// `| json` (full flatten) or `| json foo="a.b", bar` (targeted
    /// extractions; a bare identifier is shorthand for `foo="foo"`).
    Json { extractions: Vec<LabelExtraction> },
    /// `| logfmt`, optionally with the `--strict`/`--keep-empty` flags
    /// (issue #200) and/or `foo="source_key", bar` extractions. `strict`
    /// errors on a malformed line; `keep_empty` retains empty-value pairs
    /// (both are `pulsus-read` semantics — this crate only records them).
    Logfmt {
        strict: bool,
        keep_empty: bool,
        extractions: Vec<LabelExtraction>,
    },
    /// `| regexp "<re>"` — named capture groups become labels.
    Regexp(String),
    /// `| pattern "<p>"` — `<name>` captures between literal delimiters,
    /// `<_>` discards.
    Pattern(String),
}

impl fmt::Display for ParserStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn extraction_list(f: &mut fmt::Formatter<'_>, ex: &[LabelExtraction]) -> fmt::Result {
            for (i, e) in ex.iter().enumerate() {
                write!(
                    f,
                    "{}{}={}",
                    if i > 0 { ", " } else { " " },
                    e.label,
                    quote(&e.expression)
                )?;
            }
            Ok(())
        }
        match self {
            ParserStage::Json { extractions } => {
                write!(f, "json")?;
                extraction_list(f, extractions)
            }
            ParserStage::Logfmt {
                strict,
                keep_empty,
                extractions,
            } => {
                write!(f, "logfmt")?;
                // Flags render before the extraction list so the round-trip
                // oracle holds (`parse(ast.to_string()) == ast`).
                if *strict {
                    write!(f, " --strict")?;
                }
                if *keep_empty {
                    write!(f, " --keep-empty")?;
                }
                extraction_list(f, extractions)
            }
            ParserStage::Regexp(re) => write!(f, "regexp {}", quote(re)),
            ParserStage::Pattern(p) => write!(f, "pattern {}", quote(p)),
        }
    }
}

/// One `label="expression"` pair in a `json`/`logfmt` extraction list.
/// The expression is raw text (a JSON path for `json`, a source key for
/// `logfmt`) — interpreted in `pulsus-read`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LabelExtraction {
    pub label: String,
    pub expression: String,
}

/// A label-filter expression: string matchers, numeric comparisons, and
/// the `and`/`or`/`,` boolean mini-grammar (`,` and `and` both AND; `and`
/// binds tighter than `or`; parentheses group).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LabelFilterExpr {
    /// String form: `name =|!=|=~|!~ "value"`.
    Match(Matcher),
    /// Numeric form: `name ==|!=|>|>=|<|<= <number|duration|bytes>`.
    Compare {
        name: String,
        op: CompareOp,
        rhs: NumericLiteral,
    },
    /// IP form (M8-LQ2 `labelfilter.ip`): `name = ip("…")` /
    /// `name != ip("…")`. `value` is the raw CIDR/range/single-address
    /// text (matching is a `pulsus-read` concern); `negated` is the `!=`
    /// form.
    Ip {
        name: String,
        value: String,
        negated: bool,
    },
    And(Box<LabelFilterExpr>, Box<LabelFilterExpr>),
    Or(Box<LabelFilterExpr>, Box<LabelFilterExpr>),
}

impl LabelFilterExpr {
    /// Renders a child of a boolean node, parenthesizing nested boolean
    /// children so `Display` round-trips the exact tree shape (the parser
    /// is left-associative; an unparenthesized nested right child would
    /// re-associate on reparse).
    fn fmt_child(child: &LabelFilterExpr, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match child {
            LabelFilterExpr::And(..) | LabelFilterExpr::Or(..) => write!(f, "({child})"),
            leaf => write!(f, "{leaf}"),
        }
    }
}

impl fmt::Display for LabelFilterExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LabelFilterExpr::Match(m) => write!(f, "{m}"),
            LabelFilterExpr::Compare { name, op, rhs } => write!(f, "{name} {op} {rhs}"),
            LabelFilterExpr::Ip {
                name,
                value,
                negated,
            } => {
                let op = if *negated { "!=" } else { "=" };
                write!(f, "{name} {op} ip({})", quote(value))
            }
            LabelFilterExpr::And(a, b) => {
                Self::fmt_child(a, f)?;
                write!(f, " and ")?;
                Self::fmt_child(b, f)
            }
            LabelFilterExpr::Or(a, b) => {
                Self::fmt_child(a, f)?;
                write!(f, " or ")?;
                Self::fmt_child(b, f)
            }
        }
    }
}

/// Numeric label-filter comparison operators (`==` `!=` `>` `>=` `<`
/// `<=`). A `=` with a numeric RHS also parses as [`CompareOp::Eq`]
/// (canonical `Display` renders `==`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompareOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
}

impl fmt::Display for CompareOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            CompareOp::Eq => "==",
            CompareOp::Neq => "!=",
            CompareOp::Gt => ">",
            CompareOp::Gte => ">=",
            CompareOp::Lt => "<",
            CompareOp::Lte => "<=",
        };
        write!(f, "{s}")
    }
}

/// A numeric label-filter RHS, kept as raw text (this crate never parses
/// numbers/units — `Eq`/`Hash` stay derivable, and duration-vs-bytes
/// disambiguation is a `pulsus-read` concern).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NumericLiteral {
    /// A bare number token, e.g. `500`, `0.25`.
    Number(String),
    /// A unit-suffixed literal the lexer scanned as one duration-shaped
    /// token, e.g. `250ms`, `5KB`, `1MiB` — interpreted (duration vs
    /// bytes) by the shared unit parser in `pulsus-read`.
    DurationOrBytes(String),
}

impl fmt::Display for NumericLiteral {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NumericLiteral::Number(raw) | NumericLiteral::DurationOrBytes(raw) => {
                write!(f, "{raw}")
            }
        }
    }
}

/// One `label_format` operation: `dst=src` renames (identifier RHS),
/// `dst="<template>"` computes (string RHS).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LabelFmt {
    Rename { dst: String, src: String },
    Template { dst: String, tmpl: String },
}

impl fmt::Display for LabelFmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LabelFmt::Rename { dst, src } => write!(f, "{dst}={src}"),
            LabelFmt::Template { dst, tmpl } => write!(f, "{dst}={}", quote(tmpl)),
        }
    }
}

/// `("|=" | "!=" | "|~" | "!~") <line-match> (or <line-match>)*` — chains
/// with no separator between filters (`{app="x"} |= "a" != "b" !~ "c"`);
/// `!=`/`!~` carry no leading pipe. The head alternative is `value` +
/// `value_is_ip` (an `ip("…")` head renders `ip("…")` instead of a quoted
/// literal); each `or`-chained alternative is an [`LineMatch`]. `ip(…)` is
/// only accepted with `|=`/`!=` (M8-LQ2 `linefilter.ip`); the `or` chain is
/// M8-LQ2 `linefilter.or`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LineFilter {
    pub op: LineFilterOp,
    pub value: String,
    /// `true` when the head alternative is an `ip("…")` CIDR/range spec
    /// rather than a plain literal/regex value.
    pub value_is_ip: bool,
    /// Additional `or`-chained alternatives after the head (empty for a
    /// plain single-value filter). Each carries its own `is_ip` flag.
    pub or_matches: Vec<LineMatch>,
}

impl LineFilter {
    /// Iterates every alternative of this filter (head first, then each
    /// `or` alternative) as `(value, is_ip)`.
    pub fn alternatives(&self) -> impl Iterator<Item = (&str, bool)> + '_ {
        std::iter::once((self.value.as_str(), self.value_is_ip))
            .chain(self.or_matches.iter().map(|m| (m.value.as_str(), m.is_ip)))
    }
}

impl fmt::Display for LineFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ", self.op)?;
        fn render(f: &mut fmt::Formatter<'_>, value: &str, is_ip: bool) -> fmt::Result {
            if is_ip {
                write!(f, "ip({})", quote(value))
            } else {
                write!(f, "{}", quote(value))
            }
        }
        render(f, &self.value, self.value_is_ip)?;
        for m in &self.or_matches {
            write!(f, " or ")?;
            render(f, &m.value, m.is_ip)?;
        }
        Ok(())
    }
}

/// One `or`-chained alternative of a [`LineFilter`] (`|= "a" or "b"` /
/// `|= "a" or ip("…")`) — the head alternative lives on `LineFilter`
/// itself.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LineMatch {
    pub value: String,
    pub is_ip: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LineFilterOp {
    Contains,
    NotContains,
    Regex,
    NotRegex,
}

impl fmt::Display for LineFilterOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            LineFilterOp::Contains => "|=",
            LineFilterOp::NotContains => "!=",
            LineFilterOp::Regex => "|~",
            LineFilterOp::NotRegex => "!~",
        };
        write!(f, "{s}")
    }
}

/// A metric query: a range aggregation over a [`LogRange`], optionally
/// wrapped in vector aggregations, combined with other metric
/// expressions and scalar literals via binary operations (issue M6-10).
/// The M6-complete shape (`param`, `LogRange::unwrap`) was reserved in M1
/// so non-counting range aggregations and `unwrap` stayed additive
/// (architect plan amendment 1 §2); `Literal`/`Binary` and
/// `Vector::param` are the adjudicated M6-10 additive growth points (see
/// the module doc).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MetricExpr {
    Range {
        op: RangeAggOp,
        range: LogRange,
        /// Raw numeric literal (e.g. `"0.95"`), never parsed to `f64` here
        /// so the AST keeps its `Eq`/`Hash` derive (amendment 2 §1). Only
        /// `quantile_over_time` populates this.
        param: Option<String>,
    },
    Vector {
        op: VectorAggOp,
        grouping: Option<Grouping>,
        /// Raw numeric literal — the `topk`/`bottomk` `k` (issue M6-10),
        /// kept as raw text for the same `Eq`/`Hash` reason as
        /// `Range::param`. `None` for every other vector aggregation.
        param: Option<String>,
        inner: Box<MetricExpr>,
    },
    /// A bare scalar number (`2`, `0.95`) as a metric-expression operand
    /// (issue M6-10), raw text — parsed to `f64` by the planner.
    Literal(String),
    /// `vector(<scalar>)` (issue #221): promotes a scalar to a vector
    /// result (`{} => n`). Raw number text, kept for the same `Eq`/`Hash`
    /// reason as [`MetricExpr::Literal`]. Only a `NUMBER` arg (mirrors
    /// Loki v3.7.4's `vectorExpr` grammar, which rejects inner expressions).
    VectorFn(String),
    /// A binary operation between metric expressions (issue M6-10).
    /// `modifier` carries the `bool` comparison modifier and (issue #91)
    /// the `on()`/`ignoring()`/`group_left()`/`group_right()` vector-
    /// matching clause.
    Binary {
        op: BinOp,
        modifier: Option<BinModifier>,
        lhs: Box<MetricExpr>,
        rhs: Box<MetricExpr>,
    },
}

impl MetricExpr {
    /// Renders a binary operand, parenthesizing nested binary children so
    /// `Display` round-trips the exact tree shape (same convention as
    /// [`LabelFilterExpr::fmt_child`]: precedence/associativity would
    /// otherwise re-associate on reparse).
    fn fmt_operand(child: &MetricExpr, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match child {
            MetricExpr::Binary { .. } => write!(f, "({child})"),
            other => write!(f, "{other}"),
        }
    }
}

impl fmt::Display for MetricExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetricExpr::Range { op, range, param } => match param {
                Some(p) => write!(f, "{op}({p}, {range})"),
                None => write!(f, "{op}({range})"),
            },
            MetricExpr::Vector {
                op,
                grouping,
                param,
                inner,
            } => {
                match grouping {
                    Some(g) => write!(f, "{op} {g}(")?,
                    None => write!(f, "{op}(")?,
                }
                if let Some(p) = param {
                    write!(f, "{p}, ")?;
                }
                write!(f, "{inner})")
            }
            MetricExpr::Literal(raw) => write!(f, "{raw}"),
            MetricExpr::VectorFn(raw) => write!(f, "vector({raw})"),
            MetricExpr::Binary {
                op,
                modifier,
                lhs,
                rhs,
            } => {
                Self::fmt_operand(lhs, f)?;
                write!(f, " {op} ")?;
                if let Some(m) = modifier {
                    if m.return_bool {
                        write!(f, "bool ")?;
                    }
                    if let Some(vm) = &m.matching {
                        write!(f, "{vm} ")?;
                    }
                }
                Self::fmt_operand(rhs, f)
            }
        }
    }
}

/// The M6-10 binary operators: arithmetic, comparison, and the
/// `and`/`or`/`unless` set operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    And,
    Or,
    Unless,
}

impl BinOp {
    /// `true` for the six comparison operators — the only ones that
    /// accept the `bool` modifier.
    pub fn is_comparison(self) -> bool {
        matches!(
            self,
            BinOp::Eq | BinOp::Neq | BinOp::Gt | BinOp::Gte | BinOp::Lt | BinOp::Lte
        )
    }
}

impl fmt::Display for BinOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            BinOp::Add => "+",
            BinOp::Sub => "-",
            BinOp::Mul => "*",
            BinOp::Div => "/",
            BinOp::Mod => "%",
            BinOp::Pow => "^",
            BinOp::Eq => "==",
            BinOp::Neq => "!=",
            BinOp::Gt => ">",
            BinOp::Gte => ">=",
            BinOp::Lt => "<",
            BinOp::Lte => "<=",
            BinOp::And => "and",
            BinOp::Or => "or",
            BinOp::Unless => "unless",
        };
        write!(f, "{s}")
    }
}

/// Binary-operation modifier: `bool` (comparison 0/1 instead of
/// filtering) and, since issue #91, the optional
/// `on`/`ignoring`/`group_left`/`group_right` vector-matching clause.
///
/// No longer `Copy` (issue #91): `matching` owns a `Vec<String>` label
/// list. Both consumers pattern-match by reference (ast Display, the
/// planner's `build_metric_node`), so the derive was never load-bearing.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BinModifier {
    pub return_bool: bool,
    pub matching: Option<VectorMatching>,
}

/// The `on(...)`/`ignoring(...)` match-signature clause with an optional
/// `group_left`/`group_right` grouping (issue #91). Semantics are Loki's
/// (which mirror Prometheus's), oracle-pinned against `grafana/loki:3.4.2`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VectorMatching {
    /// `true` = `on(labels)` (restrict the signature to the listed
    /// labels); `false` = `ignoring(labels)` (drop the listed labels).
    pub on: bool,
    /// The `on`/`ignoring` label list (may be empty: `on()`).
    pub labels: Vec<String>,
    /// `group_left`/`group_right` many-side selection with its include
    /// labels; `None` for one-to-one matching. The parser only ever
    /// populates this when an `on`/`ignoring` clause precedes it — a bare
    /// `group_left`/`group_right` is a parse error (oracle: HTTP 400).
    pub group: Option<MatchGroup>,
}

/// The many side of a grouped match and its include-label list (copied
/// from the one side onto the many-side output). `Left` = `group_left`
/// (many-to-one, lhs is the many side); `Right` = `group_right`
/// (one-to-many, rhs is the many side).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MatchGroup {
    Left(Vec<String>),
    Right(Vec<String>),
}

impl fmt::Display for VectorMatching {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kw = if self.on { "on" } else { "ignoring" };
        write!(f, "{kw}(")?;
        for (i, l) in self.labels.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{l}")?;
        }
        write!(f, ")")?;
        if let Some(group) = &self.group {
            let (kw, labels) = match group {
                MatchGroup::Left(l) => ("group_left", l),
                MatchGroup::Right(l) => ("group_right", l),
            };
            write!(f, " {kw}")?;
            if !labels.is_empty() {
                write!(f, "(")?;
                for (i, l) in labels.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{l}")?;
                }
                write!(f, ")")?;
            }
        }
        Ok(())
    }
}

/// `LogExpr [duration]` — the operand of every range aggregation.
/// `unwrap` is retained-but-unused (issue M6-09 plan v3 delta 1): the
/// parser represents `| unwrap …` as an ordered [`Stage::Unwrap`] inside
/// `selector.pipeline` — so post-unwrap label filters keep their position
/// — and always leaves this field `None`. Kept so `LogRange` never
/// reshapes (the ast.rs additive-only freeze).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogRange {
    pub selector: LogExpr,
    pub range: Duration,
    pub unwrap: Option<Unwrap>,
}

impl fmt::Display for LogRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}[{}]", self.selector, self.range)
    }
}

/// `| unwrap <label>` / `| unwrap <conversion>(<label>)` — carried by
/// [`Stage::Unwrap`] (issue M6-09). `conversion` is
/// `Some("duration"|"duration_seconds"|"bytes")` for the wrapped forms.
/// Parse-only in M6-09: feeding the unwrapped value into range
/// aggregations is the M6-10 seam.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Unwrap {
    pub label: String,
    pub conversion: Option<String>,
}

/// Range aggregation functions. M1 implemented the four count/bytes-only
/// operations that the log rollup table can serve (docs/architecture.md
/// §5.3, §3.2); issue M6-10 added the full over-time set as new variants
/// only (the designated growth-point contract).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RangeAggOp {
    Rate,
    CountOverTime,
    BytesRate,
    BytesOverTime,
    SumOverTime,
    AvgOverTime,
    MinOverTime,
    MaxOverTime,
    StddevOverTime,
    StdvarOverTime,
    QuantileOverTime,
    FirstOverTime,
    LastOverTime,
    AbsentOverTime,
    /// `rate_counter({...} | unwrap x [5m])` — the reset-aware per-second
    /// increase over unwrapped counter values (issue M8-LQ3). Requires
    /// `unwrap`; client-aggregated like the other over-time reducers.
    RateCounter,
}

impl RangeAggOp {
    pub(crate) fn from_ident(name: &str) -> Option<Self> {
        match name {
            "rate" => Some(Self::Rate),
            "count_over_time" => Some(Self::CountOverTime),
            "bytes_rate" => Some(Self::BytesRate),
            "bytes_over_time" => Some(Self::BytesOverTime),
            "sum_over_time" => Some(Self::SumOverTime),
            "avg_over_time" => Some(Self::AvgOverTime),
            "min_over_time" => Some(Self::MinOverTime),
            "max_over_time" => Some(Self::MaxOverTime),
            "stddev_over_time" => Some(Self::StddevOverTime),
            "stdvar_over_time" => Some(Self::StdvarOverTime),
            "quantile_over_time" => Some(Self::QuantileOverTime),
            "first_over_time" => Some(Self::FirstOverTime),
            "last_over_time" => Some(Self::LastOverTime),
            "absent_over_time" => Some(Self::AbsentOverTime),
            "rate_counter" => Some(Self::RateCounter),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            RangeAggOp::Rate => "rate",
            RangeAggOp::CountOverTime => "count_over_time",
            RangeAggOp::BytesRate => "bytes_rate",
            RangeAggOp::BytesOverTime => "bytes_over_time",
            RangeAggOp::SumOverTime => "sum_over_time",
            RangeAggOp::AvgOverTime => "avg_over_time",
            RangeAggOp::MinOverTime => "min_over_time",
            RangeAggOp::MaxOverTime => "max_over_time",
            RangeAggOp::StddevOverTime => "stddev_over_time",
            RangeAggOp::StdvarOverTime => "stdvar_over_time",
            RangeAggOp::QuantileOverTime => "quantile_over_time",
            RangeAggOp::FirstOverTime => "first_over_time",
            RangeAggOp::LastOverTime => "last_over_time",
            RangeAggOp::AbsentOverTime => "absent_over_time",
            RangeAggOp::RateCounter => "rate_counter",
        }
    }
}

impl fmt::Display for RangeAggOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Vector aggregations. M1 implemented the five that need no extra
/// parameter; issue M6-10 added `stddev`/`stdvar` (reductions) and
/// `topk`/`bottomk` (per-step selections carrying a `k` parameter on
/// [`MetricExpr::Vector::param`]) as new variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VectorAggOp {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    Stddev,
    Stdvar,
    Topk,
    Bottomk,
    /// `approx_topk(k, ...)` — probabilistic top-k via a count-min sketch
    /// (issue #221). Parses exactly like `topk` except grouping is
    /// rejected (`grouping not allowed for approx_topk aggregation`) and
    /// the construct is instant-only (the planner rejects it for range
    /// queries).
    ApproxTopk,
    /// `sort(...)` / `sort_desc(...)` — order the instant result vector by
    /// value ascending/descending (issue M8-LQ3). A post-aggregation
    /// in-memory reordering, not a reduction; carries no parameter and no
    /// grouping.
    Sort,
    SortDesc,
}

impl VectorAggOp {
    pub(crate) fn from_ident(name: &str) -> Option<Self> {
        match name {
            "sum" => Some(Self::Sum),
            "avg" => Some(Self::Avg),
            "min" => Some(Self::Min),
            "max" => Some(Self::Max),
            "count" => Some(Self::Count),
            "stddev" => Some(Self::Stddev),
            "stdvar" => Some(Self::Stdvar),
            "topk" => Some(Self::Topk),
            "bottomk" => Some(Self::Bottomk),
            "approx_topk" => Some(Self::ApproxTopk),
            "sort" => Some(Self::Sort),
            "sort_desc" => Some(Self::SortDesc),
            _ => None,
        }
    }

    /// `true` for `topk`/`bottomk`/`approx_topk` — the k-selections that
    /// require a leading `k` parameter (`topk(5, ...)`).
    pub fn takes_param(self) -> bool {
        matches!(
            self,
            VectorAggOp::Topk | VectorAggOp::Bottomk | VectorAggOp::ApproxTopk
        )
    }

    fn as_str(self) -> &'static str {
        match self {
            VectorAggOp::Sum => "sum",
            VectorAggOp::Avg => "avg",
            VectorAggOp::Min => "min",
            VectorAggOp::Max => "max",
            VectorAggOp::Count => "count",
            VectorAggOp::Stddev => "stddev",
            VectorAggOp::Stdvar => "stdvar",
            VectorAggOp::Topk => "topk",
            VectorAggOp::Bottomk => "bottomk",
            VectorAggOp::ApproxTopk => "approx_topk",
            VectorAggOp::Sort => "sort",
            VectorAggOp::SortDesc => "sort_desc",
        }
    }
}

impl fmt::Display for VectorAggOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// `by(label, ...)` or `without(label, ...)`, accepted by the parser in
/// both the prefix (`sum by(l)(...)`) and postfix (`sum(...) by(l)`)
/// positions Loki allows, normalized to this one shape (architect plan:
/// "Grouping placement").
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Grouping {
    pub kind: GroupingKind,
    pub labels: Vec<String>,
}

impl fmt::Display for Grouping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.kind)?;
        for (i, label) in self.labels.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{label}")?;
        }
        write!(f, ")")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GroupingKind {
    By,
    Without,
}

impl fmt::Display for GroupingKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            GroupingKind::By => "by",
            GroupingKind::Without => "without",
        };
        write!(f, "{s}")
    }
}

/// A duration in nanoseconds — the `u64`-nanos newtype (task-manager
/// resolution #3), not `std::time::Duration`, so #11's step arithmetic
/// stays exact `u64` math with no `time`-feature dependency anywhere in
/// this crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Duration(u64);

impl Duration {
    pub(crate) fn from_nanos(nanos: u64) -> Self {
        Duration(nanos)
    }

    pub fn as_nanos(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 == 0 {
            return write!(f, "0s");
        }
        // Largest-unit-first greedy decomposition. Every remainder is
        // exactly representable (base unit is 1ns), so this always
        // terminates with the input value reproduced exactly — the
        // property the `Display`-round-trip test oracle relies on.
        const COMPONENTS: [(u64, &str); 7] = [
            (86_400_000_000_000, "d"),
            (3_600_000_000_000, "h"),
            (60_000_000_000, "m"),
            (1_000_000_000, "s"),
            (1_000_000, "ms"),
            (1_000, "us"),
            (1, "ns"),
        ];
        let mut remaining = self.0;
        for (unit_ns, suffix) in COMPONENTS {
            let count = remaining / unit_ns;
            if count > 0 {
                write!(f, "{count}{suffix}")?;
                remaining %= unit_ns;
            }
        }
        Ok(())
    }
}

/// Escapes a raw matcher/line-filter value into a double-quoted LogQL
/// string literal for canonical `Display` rendering — only `\` and `"`
/// need escaping for the round-trip oracle to hold.
fn quote(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            other => out.push(other),
        }
    }
    out.push('"');
    out
}

// Static keyword tables used by the parser to recognize M6 constructs it
// does not implement and name them in `LogQlError::NotYetSupported`
// (docs/features.md §2 "LogQL — parity (M6)"; architect plan amendment 1
// §3). `offset` is deliberately absent — it is a PromQL-ism with no LogQL
// grammar (amendment 1 §3 note).
//
// These three tables are `pub` (re-exported from `lib.rs`) SOLELY so the
// LogQL conformance harness (issue #191, `tests/conformance.rs`) can couple
// its construct-registry completeness check to the *canonical* accepted-syntax
// surface rather than hand-copying the entries. Visibility-only: no behaviour
// change, and `pulsus-logql` is an internal workspace crate so `pub` is
// workspace-visible. Hand-copying these tables into the test was a proven
// silent-gap source (adding an entry here made the parser accept new syntax
// with the test suite still green), so the coupling is the correct root fix.

/// Pipeline stage keywords still outside the implemented set (issue #200
/// flipped `unpack`/`drop`/`keep`/`decolorize` to first-class stages):
/// recognized after a bare `|` and named in `NotYetSupported`.
pub const REMAINING_UNSUPPORTED_STAGES: &[&str] = &["ip"];

/// The conversion functions `unwrap` accepts in its wrapped form.
pub const UNWRAP_CONVERSIONS: &[&str] = &["duration", "duration_seconds", "bytes"];

/// The identifier-shaped binary operators (`and`/`or`/`unless`) — since
/// M6-10 consumed by the precedence-climbing binary-op parser (the
/// symbolic operators `+ - * / % ^ == != > < >= <=` are their own token
/// kinds). `!~`/`|=`/`|~` are deliberately excluded — they are never
/// binary operators in any LogQL milestone.
pub const BINARY_OP_KEYWORDS: &[&str] = &["and", "or", "unless"];

// The vector-matching modifier keywords (`on`/`ignoring`/`group_left`/
// `group_right`) are recognized directly by the parser
// (`parse_vector_matching`) since issue #91 — no lookup table is needed,
// the clause is fully parsed into a [`VectorMatching`] rather than named
// `NotYetSupported`.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_display_round_trips_a_single_unit() {
        assert_eq!(Duration::from_nanos(5_000_000_000).to_string(), "5s");
    }

    #[test]
    fn duration_display_round_trips_a_compound_value() {
        // 1h30m5s in nanoseconds.
        let nanos = 3_600_000_000_000 + 30 * 60_000_000_000 + 5_000_000_000;
        assert_eq!(Duration::from_nanos(nanos).to_string(), "1h30m5s");
    }

    #[test]
    fn duration_display_of_zero_is_zero_seconds() {
        assert_eq!(Duration::from_nanos(0).to_string(), "0s");
    }

    #[test]
    fn matcher_display_quotes_and_escapes_the_value() {
        let m = Matcher {
            name: "app".to_string(),
            op: MatchOp::Eq,
            value: "a\"b\\c".to_string(),
        };
        assert_eq!(m.to_string(), r#"app="a\"b\\c""#);
    }

    #[test]
    fn range_agg_op_round_trips_through_from_ident_and_display() {
        for (name, op) in [
            ("rate", RangeAggOp::Rate),
            ("count_over_time", RangeAggOp::CountOverTime),
            ("bytes_rate", RangeAggOp::BytesRate),
            ("bytes_over_time", RangeAggOp::BytesOverTime),
            ("sum_over_time", RangeAggOp::SumOverTime),
            ("avg_over_time", RangeAggOp::AvgOverTime),
            ("min_over_time", RangeAggOp::MinOverTime),
            ("max_over_time", RangeAggOp::MaxOverTime),
            ("stddev_over_time", RangeAggOp::StddevOverTime),
            ("stdvar_over_time", RangeAggOp::StdvarOverTime),
            ("quantile_over_time", RangeAggOp::QuantileOverTime),
            ("first_over_time", RangeAggOp::FirstOverTime),
            ("last_over_time", RangeAggOp::LastOverTime),
            ("absent_over_time", RangeAggOp::AbsentOverTime),
            ("rate_counter", RangeAggOp::RateCounter),
        ] {
            assert_eq!(RangeAggOp::from_ident(name), Some(op));
            assert_eq!(op.to_string(), name);
        }
    }

    #[test]
    fn vector_agg_op_round_trips_through_from_ident_and_display() {
        for (name, op) in [
            ("sum", VectorAggOp::Sum),
            ("avg", VectorAggOp::Avg),
            ("min", VectorAggOp::Min),
            ("max", VectorAggOp::Max),
            ("count", VectorAggOp::Count),
            ("stddev", VectorAggOp::Stddev),
            ("stdvar", VectorAggOp::Stdvar),
            ("topk", VectorAggOp::Topk),
            ("bottomk", VectorAggOp::Bottomk),
            ("approx_topk", VectorAggOp::ApproxTopk),
            ("sort", VectorAggOp::Sort),
            ("sort_desc", VectorAggOp::SortDesc),
        ] {
            assert_eq!(VectorAggOp::from_ident(name), Some(op));
            assert_eq!(op.to_string(), name);
        }
    }

    #[test]
    fn unknown_identifiers_are_not_recognized_as_implemented_aggregations() {
        // `rate_counter`/`sort`/`sort_desc` now resolve (issue M8-LQ3);
        // genuinely-unknown idents must still return `None`.
        assert_eq!(RangeAggOp::from_ident("rate_bytes"), None);
        assert_eq!(VectorAggOp::from_ident("sortk"), None);
        assert_eq!(VectorAggOp::from_ident("sort_asc"), None);
    }

    #[test]
    fn only_the_k_selections_take_a_parameter() {
        for op in [
            VectorAggOp::Sum,
            VectorAggOp::Avg,
            VectorAggOp::Min,
            VectorAggOp::Max,
            VectorAggOp::Count,
            VectorAggOp::Stddev,
            VectorAggOp::Stdvar,
            VectorAggOp::Sort,
            VectorAggOp::SortDesc,
        ] {
            assert!(!op.takes_param());
        }
        assert!(VectorAggOp::Topk.takes_param());
        assert!(VectorAggOp::Bottomk.takes_param());
        assert!(VectorAggOp::ApproxTopk.takes_param());
    }

    #[test]
    fn only_the_six_comparison_binops_accept_bool() {
        for op in [
            BinOp::Eq,
            BinOp::Neq,
            BinOp::Gt,
            BinOp::Gte,
            BinOp::Lt,
            BinOp::Lte,
        ] {
            assert!(op.is_comparison());
        }
        for op in [
            BinOp::Add,
            BinOp::Sub,
            BinOp::Mul,
            BinOp::Div,
            BinOp::Mod,
            BinOp::Pow,
            BinOp::And,
            BinOp::Or,
            BinOp::Unless,
        ] {
            assert!(!op.is_comparison());
        }
    }
}
