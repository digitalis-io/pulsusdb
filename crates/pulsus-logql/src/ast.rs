//! The LogQL AST — the stable contract the #11 planner/SQL generator
//! consumes (docs/architecture.md §5.3). Every type derives `Debug`,
//! `Clone`, `PartialEq`, `Eq`, `Hash` (the last for #11's plan-cache/dedup
//! and so the AST can key a cache; also load-bearing for the `Display`
//! round-trip test oracle: `parse(ast.to_string()) == ast`).
//!
//! `Stage` (M1: only `LineFilter`), `RangeAggOp`, and `VectorAggOp` are
//! the designated M6 growth points — parity lands as additive variants,
//! never a reshape of these types or their fields (architect plan: "AST
//! contract stability").

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
        }
    }
}

/// A parser stage: extracts labels from the (unmodified) log line. Regex
/// and pattern bodies are raw text here — validated/compiled in
/// `pulsus-read` (the "regex not validated" contract, same as `Matcher`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ParserStage {
    /// `| json` (full flatten) or `| json foo="a.b", bar` (targeted
    /// extractions; a bare identifier is shorthand for `foo="foo"`).
    Json { extractions: Vec<LabelExtraction> },
    /// `| logfmt` or `| logfmt foo="source_key", bar`.
    Logfmt { extractions: Vec<LabelExtraction> },
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
            ParserStage::Logfmt { extractions } => {
                write!(f, "logfmt")?;
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

/// `("|=" | "!=" | "|~" | "!~") string` — chains with no separator
/// (`{app="x"} |= "a" != "b" !~ "c"`); `!=`/`!~` carry no leading pipe.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LineFilter {
    pub op: LineFilterOp,
    pub value: String,
}

impl fmt::Display for LineFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.op, quote(&self.value))
    }
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
/// wrapped in one or more vector aggregations. The M6-complete shape
/// (`param`, `LogRange::unwrap`) is reserved now so non-counting range
/// aggregations and `unwrap` are additive — M1 always leaves them `None`
/// (architect plan amendment 1 §2).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MetricExpr {
    Range {
        op: RangeAggOp,
        range: LogRange,
        /// Raw numeric literal (e.g. `"0.95"`), never parsed to `f64` here
        /// so the AST keeps its `Eq`/`Hash` derive (amendment 2 §1). Only
        /// `quantile_over_time` (M6) populates this; M1 always sets
        /// `None`.
        param: Option<String>,
    },
    Vector {
        op: VectorAggOp,
        grouping: Option<Grouping>,
        inner: Box<MetricExpr>,
    },
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
                inner,
            } => match grouping {
                Some(g) => write!(f, "{op} {g}({inner})"),
                None => write!(f, "{op}({inner})"),
            },
        }
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

/// Range aggregation functions. M1 implements the four count/bytes-only
/// operations that the log rollup table can serve (docs/architecture.md
/// §5.3, §3.2); M6 adds the full over-time set as new variants only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RangeAggOp {
    Rate,
    CountOverTime,
    BytesRate,
    BytesOverTime,
}

impl RangeAggOp {
    pub(crate) fn from_ident(name: &str) -> Option<Self> {
        match name {
            "rate" => Some(Self::Rate),
            "count_over_time" => Some(Self::CountOverTime),
            "bytes_rate" => Some(Self::BytesRate),
            "bytes_over_time" => Some(Self::BytesOverTime),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            RangeAggOp::Rate => "rate",
            RangeAggOp::CountOverTime => "count_over_time",
            RangeAggOp::BytesRate => "bytes_rate",
            RangeAggOp::BytesOverTime => "bytes_over_time",
        }
    }
}

impl fmt::Display for RangeAggOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Vector aggregations. M1 implements the five that need no extra
/// parameter; M6 adds `stddev`/`stdvar`/`topk`/`bottomk` as new variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VectorAggOp {
    Sum,
    Avg,
    Min,
    Max,
    Count,
}

impl VectorAggOp {
    pub(crate) fn from_ident(name: &str) -> Option<Self> {
        match name {
            "sum" => Some(Self::Sum),
            "avg" => Some(Self::Avg),
            "min" => Some(Self::Min),
            "max" => Some(Self::Max),
            "count" => Some(Self::Count),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            VectorAggOp::Sum => "sum",
            VectorAggOp::Avg => "avg",
            VectorAggOp::Min => "min",
            VectorAggOp::Max => "max",
            VectorAggOp::Count => "count",
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

/// The remaining `*_over_time` range aggregations.
pub(crate) const FUTURE_RANGE_AGG: &[&str] = &[
    "sum_over_time",
    "avg_over_time",
    "min_over_time",
    "max_over_time",
    "stddev_over_time",
    "stdvar_over_time",
    "quantile_over_time",
    "first_over_time",
    "last_over_time",
    "absent_over_time",
];

/// The remaining vector aggregations.
pub(crate) const FUTURE_VECTOR_AGG: &[&str] = &["stddev", "stdvar", "topk", "bottomk"];

/// Pipeline stage keywords still outside the implemented set after
/// M6-09 (which emptied the former `FUTURE_PARSERS`/
/// `FUTURE_STAGE_KEYWORDS` tables): recognized after a bare `|` and named
/// in `NotYetSupported`.
pub(crate) const REMAINING_UNSUPPORTED_STAGES: &[&str] =
    &["unpack", "drop", "keep", "decolorize", "distinct", "ip"];

/// The conversion functions `unwrap` accepts in its wrapped form.
pub(crate) const UNWRAP_CONVERSIONS: &[&str] = &["duration", "duration_seconds", "bytes"];

/// The exact expression-level binary-op recognition table (architect plan
/// amendment 2 §2, corrected by amendment 3): `+ - * / % ^ == != > < >=
/// <= and or unless`. `!~`/`|=`/`|~` are deliberately excluded — they are
/// never binary operators in any LogQL milestone.
pub(crate) const BINARY_OP_KEYWORDS: &[&str] = &["and", "or", "unless"];

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
        ] {
            assert_eq!(VectorAggOp::from_ident(name), Some(op));
            assert_eq!(op.to_string(), name);
        }
    }

    #[test]
    fn unknown_identifiers_are_not_recognized_as_implemented_aggregations() {
        assert_eq!(RangeAggOp::from_ident("sum_over_time"), None);
        assert_eq!(VectorAggOp::from_ident("topk"), None);
    }
}
