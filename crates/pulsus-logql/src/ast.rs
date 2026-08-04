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
use std::hash::{Hash, Hasher};
use std::mem;

use crate::walk::{self, Child, ChildVec, ChunkStack, Emit, Ref, Scc, Slot, Walk};

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
/// **Issue #272 — no `#[derive]`.** `And`/`Or`'s operands are
/// SCC-internal child slots spelled [`Child`], so a `#[derive]` here is a
/// compile error (C3). A flat `a or b or c …` chain parses at depth 1
/// into a LEFT-DEEP tree — the parenthesis-depth guard bounds paren
/// nesting only, never term width — so this type carried a reachable
/// crash vector until every edge below became iterative.
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
    And(Child<LabelFilterExpr>, Child<LabelFilterExpr>),
    Or(Child<LabelFilterExpr>, Child<LabelFilterExpr>),
}

// ---------------------------------------------------------------------
// SCC-1: `LabelFilterExpr` (issue #272)
// ---------------------------------------------------------------------
//
// A homogeneous SCC: every SCC-internal child of a `LabelFilterExpr` is a
// `LabelFilterExpr`, so the kinds are the trivial ones.

/// The zero-sized tag naming SCC-1 in [`walk::Scc`]'s type position.
#[derive(Debug, Clone, Copy)]
pub struct LabelFilterScc;

/// A same-kind, non-allocating `LabelFilterExpr` the parser can never
/// produce, written over a stolen child so the emptied parent's own drop
/// is free. `Ip` is chosen over `Match`/`Compare` because its fields are
/// two empty `String`s and a `bool` — nothing to allocate.
#[inline]
fn lf_placeholder() -> LabelFilterExpr {
    LabelFilterExpr::Ip {
        name: String::new(),
        value: String::new(),
        negated: false,
    }
}

impl Scc for LabelFilterScc {
    type Ref<'a> = walk::Ref<'a, LabelFilterExpr>;
    type Node<'a> = &'a LabelFilterExpr;
    type Slot<'a> = walk::Slot<'a, LabelFilterExpr>;
    type SlotNode<'a> = &'a mut LabelFilterExpr;
    type Val = LabelFilterExpr;

    #[inline]
    fn wrap<'a>(n: &'a LabelFilterExpr) -> walk::Ref<'a, LabelFilterExpr>
    where
        Self: 'a,
    {
        walk::Ref::new(n)
    }

    #[inline]
    fn open<'a>(r: walk::Ref<'a, LabelFilterExpr>, w: &Walk) -> &'a LabelFilterExpr
    where
        Self: 'a,
    {
        r.open(w)
    }

    #[inline]
    fn open_slot<'a>(s: walk::Slot<'a, LabelFilterExpr>, w: &Walk) -> &'a mut LabelFilterExpr
    where
        Self: 'a,
    {
        s.open(w)
    }

    #[inline]
    fn slot_node_ref<'x>(s: &'x &mut LabelFilterExpr) -> &'x LabelFilterExpr
    where
        Self: 'x,
    {
        s
    }

    #[inline]
    fn child<'a>(n: &'a LabelFilterExpr, i: usize) -> Option<walk::Ref<'a, LabelFilterExpr>>
    where
        Self: 'a,
    {
        match n {
            LabelFilterExpr::Match(_)
            | LabelFilterExpr::Compare { .. }
            | LabelFilterExpr::Ip { .. } => None,
            LabelFilterExpr::And(a, b) | LabelFilterExpr::Or(a, b) => match i {
                0 => Some(a.peek()),
                1 => Some(b.peek()),
                _ => None,
            },
        }
    }

    fn shallow_eq(a: &LabelFilterExpr, b: &LabelFilterExpr) -> bool {
        match (a, b) {
            (LabelFilterExpr::Match(a), LabelFilterExpr::Match(b)) => a == b,
            (
                LabelFilterExpr::Compare {
                    name: an,
                    op: ao,
                    rhs: ar,
                },
                LabelFilterExpr::Compare {
                    name: bn,
                    op: bo,
                    rhs: br,
                },
            ) => an == bn && ao == bo && ar == br,
            (
                LabelFilterExpr::Ip {
                    name: an,
                    value: av,
                    negated: ag,
                },
                LabelFilterExpr::Ip {
                    name: bn,
                    value: bv,
                    negated: bg,
                },
            ) => an == bn && av == bv && ag == bg,
            (LabelFilterExpr::And(..), LabelFilterExpr::And(..)) => true,
            (LabelFilterExpr::Or(..), LabelFilterExpr::Or(..)) => true,
            (LabelFilterExpr::Match(_), _)
            | (LabelFilterExpr::Compare { .. }, _)
            | (LabelFilterExpr::Ip { .. }, _)
            | (LabelFilterExpr::And(..), _)
            | (LabelFilterExpr::Or(..), _) => false,
        }
    }

    fn rebuild(n: &LabelFilterExpr, kids: &mut Vec<LabelFilterExpr>) -> LabelFilterExpr {
        match n {
            LabelFilterExpr::Match(m) => LabelFilterExpr::Match(m.clone()),
            LabelFilterExpr::Compare { name, op, rhs } => LabelFilterExpr::Compare {
                name: name.clone(),
                op: *op,
                rhs: rhs.clone(),
            },
            LabelFilterExpr::Ip {
                name,
                value,
                negated,
            } => LabelFilterExpr::Ip {
                name: name.clone(),
                value: value.clone(),
                negated: *negated,
            },
            // The tail of `kids` is [.., lhs, rhs].
            LabelFilterExpr::And(..) => {
                let b = drain_lf(kids);
                let a = drain_lf(kids);
                LabelFilterExpr::And(Child::new(a), Child::new(b))
            }
            LabelFilterExpr::Or(..) => {
                let b = drain_lf(kids);
                let a = drain_lf(kids);
                LabelFilterExpr::Or(Child::new(a), Child::new(b))
            }
        }
    }

    fn take_own_fields(s: &mut &mut LabelFilterExpr) {
        match &mut **s {
            // `And`/`Or` carry no own fields; the leaves are never
            // emptied (`dismantle` only empties nodes that have
            // children), which is what keeps a real `Ip` leaf
            // distinguishable from the placeholder.
            LabelFilterExpr::Match(_)
            | LabelFilterExpr::Compare { .. }
            | LabelFilterExpr::Ip { .. }
            | LabelFilterExpr::And(..)
            | LabelFilterExpr::Or(..) => {}
        }
    }

    fn steal_children(s: &mut &mut LabelFilterExpr, out: &mut ChunkStack<LabelFilterExpr>) {
        match &mut **s {
            LabelFilterExpr::Match(_)
            | LabelFilterExpr::Compare { .. }
            | LabelFilterExpr::Ip { .. } => {}
            LabelFilterExpr::And(a, b) | LabelFilterExpr::Or(a, b) => {
                // Right-to-left, so LIFO pop order is left-to-right.
                out.push(b.replace(lf_placeholder()));
                out.push(a.replace(lf_placeholder()));
            }
        }
    }

    #[inline]
    fn val_ref(v: &LabelFilterExpr) -> walk::Ref<'_, LabelFilterExpr> {
        walk::Ref::new(v)
    }

    #[inline]
    fn val_slot(v: &mut LabelFilterExpr) -> walk::Slot<'_, LabelFilterExpr> {
        walk::Slot::new(v)
    }
}

#[inline]
fn drain_lf(kids: &mut Vec<LabelFilterExpr>) -> LabelFilterExpr {
    match kids.pop() {
        Some(v) => v,
        // Unreachable by construction; clone path, never a `Drop` path.
        None => unreachable!("expected a `LabelFilterExpr` child value on the rebuild stack"),
    }
}

/// Visits every SCC-1 node reachable from `root`, pre-order, left to
/// right. The consumer never names a handle or a [`walk::Walk`].
pub fn for_each_label_filter<'a>(root: &'a LabelFilterExpr, f: impl FnMut(&'a LabelFilterExpr)) {
    walk::preorder::<LabelFilterScc>(root, f);
}

impl Drop for LabelFilterExpr {
    fn drop(&mut self) {
        #[cfg(test)]
        drop_order::note_visited_lf(self);
        walk::dismantle::<LabelFilterScc>(walk::Slot::new(self));
    }
}

impl Clone for LabelFilterExpr {
    fn clone(&self) -> Self {
        walk::clone_iter::<LabelFilterScc>(self)
    }
}

impl PartialEq for LabelFilterExpr {
    fn eq(&self, other: &Self) -> bool {
        walk::eq_iter::<LabelFilterScc>(self, other)
    }
}

impl Eq for LabelFilterExpr {}

mod lf_step {
    pub(super) const ENTER: u32 = 0;
    pub(super) const AFTER_FIRST: u32 = 1;
    pub(super) const AFTER_SECOND: u32 = 2;
    /// `Display`: entered as a boolean operand, which parenthesises a
    /// nested boolean child exactly as the old `fmt_child` did.
    pub(super) const OPERAND: u32 = 3;
    pub(super) const OPERAND_CLOSE: u32 = 4;
}

impl Hash for LabelFilterExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let done: Result<(), ()> = walk::emit::<LabelFilterScc, ()>(self, |n, step, _| {
            // The derive writes `mem::discriminant(self)` EXACTLY ONCE
            // per node, before its fields. A resumable frame is entered
            // once per child plus once, so the discriminant belongs to
            // the ENTER step alone — hashing it on every step would
            // write it three times for `And`/`Or` and silently diverge
            // from every caller written against the derive.
            if step == lf_step::ENTER {
                mem::discriminant(n).hash(state);
            }
            match n {
                LabelFilterExpr::Match(m) => {
                    if step == lf_step::ENTER {
                        m.hash(state);
                    }
                    Ok(Emit::Done)
                }
                LabelFilterExpr::Compare { name, op, rhs } => {
                    if step == lf_step::ENTER {
                        name.hash(state);
                        op.hash(state);
                        rhs.hash(state);
                    }
                    Ok(Emit::Done)
                }
                LabelFilterExpr::Ip {
                    name,
                    value,
                    negated,
                } => {
                    if step == lf_step::ENTER {
                        name.hash(state);
                        value.hash(state);
                        negated.hash(state);
                    }
                    Ok(Emit::Done)
                }
                LabelFilterExpr::And(a, b) | LabelFilterExpr::Or(a, b) => match step {
                    lf_step::ENTER => Ok(Emit::Descend {
                        next_step: lf_step::AFTER_FIRST,
                        child: a.peek(),
                        child_step: lf_step::ENTER,
                        child_level: 0,
                    }),
                    lf_step::AFTER_FIRST => Ok(Emit::Descend {
                        next_step: lf_step::AFTER_SECOND,
                        child: b.peek(),
                        child_step: lf_step::ENTER,
                        child_level: 0,
                    }),
                    _ => Ok(Emit::Done),
                },
            }
        });
        match done {
            Ok(()) => {}
            Err(()) => unreachable!("the hash step machine never fails"),
        }
    }
}

impl fmt::Debug for LabelFilterExpr {
    /// Byte-equivalent to the deleted `#[derive(Debug)]` in both modes.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let alt = f.alternate();
        walk::emit::<LabelFilterScc, fmt::Error>(self, |n, step, level| match n {
            LabelFilterExpr::Match(m) => {
                walk::dbg_open_tuple(f, "Match", alt, level)?;
                walk::dbg_own(f, m, alt, level + 1)?;
                walk::dbg_close_tuple(f, alt, level)?;
                Ok(Emit::Done)
            }
            LabelFilterExpr::Compare { name, op, rhs } => {
                walk::dbg_open_struct(f, "Compare", alt, level)?;
                f.write_str("name: ")?;
                walk::dbg_own(f, name, alt, level + 1)?;
                walk::dbg_sep(f, alt, level)?;
                f.write_str("op: ")?;
                walk::dbg_own(f, op, alt, level + 1)?;
                walk::dbg_sep(f, alt, level)?;
                f.write_str("rhs: ")?;
                walk::dbg_own(f, rhs, alt, level + 1)?;
                walk::dbg_close_struct(f, alt, level)?;
                Ok(Emit::Done)
            }
            LabelFilterExpr::Ip {
                name,
                value,
                negated,
            } => {
                walk::dbg_open_struct(f, "Ip", alt, level)?;
                f.write_str("name: ")?;
                walk::dbg_own(f, name, alt, level + 1)?;
                walk::dbg_sep(f, alt, level)?;
                f.write_str("value: ")?;
                walk::dbg_own(f, value, alt, level + 1)?;
                walk::dbg_sep(f, alt, level)?;
                f.write_str("negated: ")?;
                walk::dbg_own(f, negated, alt, level + 1)?;
                walk::dbg_close_struct(f, alt, level)?;
                Ok(Emit::Done)
            }
            LabelFilterExpr::And(a, b) | LabelFilterExpr::Or(a, b) => {
                let name = if matches!(n, LabelFilterExpr::And(..)) {
                    "And"
                } else {
                    "Or"
                };
                match step {
                    lf_step::ENTER => {
                        walk::dbg_open_tuple(f, name, alt, level)?;
                        Ok(Emit::Descend {
                            next_step: lf_step::AFTER_FIRST,
                            child: a.peek(),
                            child_step: lf_step::ENTER,
                            child_level: level + 1,
                        })
                    }
                    lf_step::AFTER_FIRST => {
                        walk::dbg_sep(f, alt, level)?;
                        Ok(Emit::Descend {
                            next_step: lf_step::AFTER_SECOND,
                            child: b.peek(),
                            child_step: lf_step::ENTER,
                            child_level: level + 1,
                        })
                    }
                    _ => {
                        walk::dbg_close_tuple(f, alt, level)?;
                        Ok(Emit::Done)
                    }
                }
            }
        })
    }
}

impl fmt::Display for LabelFilterExpr {
    /// The old `fmt_child`'s parenthesisation becomes explicit open/close
    /// tokens: a boolean child is entered at `OPERAND`, which writes `(`
    /// when the child is itself boolean, descends into that very node at
    /// `ENTER`, and closes on the way back.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        #[inline]
        fn is_bool(n: &LabelFilterExpr) -> bool {
            matches!(n, LabelFilterExpr::And(..) | LabelFilterExpr::Or(..))
        }
        walk::emit::<LabelFilterScc, fmt::Error>(self, |n, step, _| {
            if step == lf_step::OPERAND {
                if is_bool(n) {
                    f.write_str("(")?;
                }
                return Ok(Emit::Descend {
                    next_step: lf_step::OPERAND_CLOSE,
                    child: LabelFilterScc::wrap(n),
                    child_step: lf_step::ENTER,
                    child_level: 0,
                });
            }
            if step == lf_step::OPERAND_CLOSE {
                if is_bool(n) {
                    f.write_str(")")?;
                }
                return Ok(Emit::Done);
            }
            match n {
                LabelFilterExpr::Match(m) => {
                    write!(f, "{m}")?;
                    Ok(Emit::Done)
                }
                LabelFilterExpr::Compare { name, op, rhs } => {
                    write!(f, "{name} {op} {rhs}")?;
                    Ok(Emit::Done)
                }
                LabelFilterExpr::Ip {
                    name,
                    value,
                    negated,
                } => {
                    let op = if *negated { "!=" } else { "=" };
                    write!(f, "{name} {op} ip({})", quote(value))?;
                    Ok(Emit::Done)
                }
                LabelFilterExpr::And(a, b) | LabelFilterExpr::Or(a, b) => match step {
                    lf_step::ENTER => Ok(Emit::Descend {
                        next_step: lf_step::AFTER_FIRST,
                        child: a.peek(),
                        child_step: lf_step::OPERAND,
                        child_level: 0,
                    }),
                    lf_step::AFTER_FIRST => {
                        let sep = if matches!(n, LabelFilterExpr::And(..)) {
                            " and "
                        } else {
                            " or "
                        };
                        f.write_str(sep)?;
                        Ok(Emit::Descend {
                            next_step: lf_step::AFTER_SECOND,
                            child: b.peek(),
                            child_step: lf_step::OPERAND,
                            child_level: 0,
                        })
                    }
                    _ => Ok(Emit::Done),
                },
            }
        })
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
/// **Issue #272 — no `#[derive]`.** `Vector::inner`, `Binary::lhs`,
/// `Binary::rhs` and `Variants.0` are SCC-internal child slots spelled
/// [`Child`], which implements no derivable trait, so a `#[derive]` here
/// is a compile error (C3) and every per-child dispatch must go through
/// an iterative driver. `Debug`, `Clone`, `PartialEq`, `Eq`, `Hash`,
/// `Display` and `Drop` are hand-written below and are byte-equivalent to
/// what the derive produced.
pub enum MetricExpr {
    Range {
        op: RangeAggOp,
        range: LogRange,
        /// Raw numeric literal (e.g. `"0.95"`), never parsed to `f64` here
        /// so the AST keeps its `Eq`/`Hash` derive (amendment 2 §1). Only
        /// `quantile_over_time` populates this.
        param: Option<String>,
        /// `by (…)` / `without (…)` on the range aggregation itself
        /// (issue #344) — `max_over_time({app="x"} | unwrap v [5m]) by (fp)`.
        /// POSTFIX ONLY, and only for the eight ops
        /// [`RangeAggOp::allows_grouping`] admits; the reference rejects the
        /// prefix placement its vector aggregations accept, and rejects the
        /// other seven ops by name. `None` for every op that forbids it,
        /// so an illegal combination is unrepresentable after parsing.
        grouping: Option<Grouping>,
    },
    Vector {
        op: VectorAggOp,
        grouping: Option<Grouping>,
        /// Raw numeric literal — the `topk`/`bottomk` `k` (issue M6-10),
        /// kept as raw text for the same `Eq`/`Hash` reason as
        /// `Range::param`. `None` for every other vector aggregation.
        param: Option<String>,
        inner: Child<MetricExpr>,
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
        lhs: Child<MetricExpr>,
        rhs: Child<MetricExpr>,
    },
    /// `variants(<metricExpr>, …) of (<logRangeExpr>)` (issue #221): N
    /// metric extractors over ONE log range, each result tagged
    /// `__variant__="<index>"`. Modelled as a `MetricExpr` (not a new
    /// [`Expr`] arm) because the reference's `MultiVariantExpr` implements
    /// `SampleExpr` and is therefore a legal binary operand —
    /// `variants(…) of (…) + 1` is accepted upstream — while the
    /// positional `allow_variants` parser rule keeps it out of every
    /// nested position the reference grammar rejects (`sum(variants(…))`,
    /// `(variants(…))`, another `variants` argument list).
    Variants(Child<VariantsExpr>),
    /// `label_replace(<metricExpr>, "<dst>", "<replacement>", "<src>",
    /// "<regex>")` (issue #276) — mirrors Loki v3.7.4's `labelReplaceExpr`
    /// grammar rule (`pkg/logql/syntax/syntax.y`), a `metricExpr`
    /// alternative, so it composes in every metric position (inside
    /// vector aggregations, parentheses, binary operands, another
    /// `label_replace`). All four string arguments are raw text here —
    /// the regex is compiled (and its `^(?:…)$` anchoring applied) by
    /// `pulsus-read` at plan time, per this crate's "regex not validated"
    /// contract.
    LabelReplace {
        inner: Child<MetricExpr>,
        dst: String,
        replacement: String,
        src: String,
        regex: String,
    },
}

/// `variants(<metricExpr>, …) of (<logRangeExpr>)` (issue #221). Mirrors
/// the reference's `MultiVariantExpr`: N metric extractors over ONE log
/// range, each result tagged with an index-based `__variant__` label.
/// **Issue #272 — no `#[derive]`**: `variants` is an N-ary SCC-internal
/// child slot spelled [`ChildVec`]. Every trait below is hand-written and
/// iterative. `VariantsExpr` carries **no** `impl Drop`: after conversion
/// its only recursive edge is `MetricExpr::drop`, which is iterative.
pub struct VariantsExpr {
    /// At least one (the parser cannot produce an empty list); each is
    /// validated at PLAN time to be a range aggregation optionally
    /// wrapped in exactly ONE vector aggregation.
    pub variants: ChildVec<MetricExpr>,
    /// The single common log range — the ONLY selector/pipeline that
    /// selects and transforms data (a variant's own selector, line
    /// filters and parsers are dead syntax in the reference).
    pub range: LogRange,
}

// ---------------------------------------------------------------------
// SCC-2: `MetricExpr` + `VariantsExpr` (issue #272)
// ---------------------------------------------------------------------
//
// The two types are ONE strongly-connected component of the "contains"
// graph — `MetricExpr::Variants` holds a `VariantsExpr` and
// `VariantsExpr::variants` holds `MetricExpr`s — so they are walked as one
// heterogeneous node algebra rather than two self-typed ones.
//
// Ordering (normative): a node's children are its SCC-internal slots in
// DECLARATION order, left to right. `MetricExpr::Variants` has exactly ONE
// child (the `VariantsExpr` node); `VariantsExpr` has N children
// (`variants[0..n]` in index order) and `range` is an own field that
// FOLLOWS them, which is why the emitters interleave rather than emit
// "own fields then children".

/// The zero-sized tag naming SCC-2 in [`walk::Scc`]'s type position.
#[derive(Debug, Clone, Copy)]
pub struct MetricScc;

/// SCC-2's inert handle kind. Derives ONLY `Clone`/`Copy` — copying a
/// handle traverses nothing.
#[derive(Clone, Copy)]
pub enum MeRef<'a> {
    Expr(Ref<'a, MetricExpr>),
    Var(Ref<'a, VariantsExpr>),
}

/// SCC-2's opened node kind — what every driver callback receives.
#[derive(Clone, Copy)]
pub enum MeNode<'a> {
    Expr(&'a MetricExpr),
    Var(&'a VariantsExpr),
}

/// SCC-2's inert mutable handle kind.
pub enum MeSlot<'a> {
    Expr(Slot<'a, MetricExpr>),
    Var(Slot<'a, VariantsExpr>),
}

/// SCC-2's opened mutable node kind.
pub enum MeSlotNode<'a> {
    Expr(&'a mut MetricExpr),
    Var(&'a mut VariantsExpr),
}

/// SCC-2's owned kind.
pub enum MeVal {
    Expr(MetricExpr),
    Var(VariantsExpr),
}

/// A same-kind, non-allocating `MetricExpr` the parser can never produce,
/// written over a stolen child so the emptied parent's own drop is free.
#[inline]
fn me_placeholder() -> MetricExpr {
    MetricExpr::Literal(String::new())
}

/// The `VariantsExpr` placeholder: every `Vec` empty, so it allocates
/// nothing.
#[inline]
fn ve_placeholder() -> VariantsExpr {
    VariantsExpr {
        variants: ChildVec::new(Vec::new()),
        range: empty_log_range(),
    }
}

#[inline]
fn empty_log_range() -> LogRange {
    LogRange {
        selector: LogExpr {
            selector: StreamSelector {
                matchers: Vec::new(),
            },
            pipeline: Vec::new(),
        },
        range: Duration::from_nanos(0),
        unwrap: None,
        offset_ns: None,
    }
}

impl Scc for MetricScc {
    type Ref<'a> = MeRef<'a>;
    type Node<'a> = MeNode<'a>;
    type Slot<'a> = MeSlot<'a>;
    type SlotNode<'a> = MeSlotNode<'a>;
    type Val = MeVal;

    #[inline]
    fn wrap<'a>(n: MeNode<'a>) -> MeRef<'a>
    where
        Self: 'a,
    {
        match n {
            MeNode::Expr(e) => MeRef::Expr(Ref::new(e)),
            MeNode::Var(v) => MeRef::Var(Ref::new(v)),
        }
    }

    #[inline]
    fn open<'a>(r: MeRef<'a>, w: &Walk) -> MeNode<'a>
    where
        Self: 'a,
    {
        match r {
            MeRef::Expr(r) => MeNode::Expr(r.open(w)),
            MeRef::Var(r) => MeNode::Var(r.open(w)),
        }
    }

    #[inline]
    fn open_slot<'a>(s: MeSlot<'a>, w: &Walk) -> MeSlotNode<'a>
    where
        Self: 'a,
    {
        match s {
            MeSlot::Expr(s) => MeSlotNode::Expr(s.open(w)),
            MeSlot::Var(s) => MeSlotNode::Var(s.open(w)),
        }
    }

    #[inline]
    fn slot_node_ref<'x>(s: &'x MeSlotNode<'_>) -> MeNode<'x>
    where
        Self: 'x,
    {
        match s {
            MeSlotNode::Expr(e) => MeNode::Expr(e),
            MeSlotNode::Var(v) => MeNode::Var(v),
        }
    }

    #[inline]
    fn child<'a>(n: MeNode<'a>, i: usize) -> Option<MeRef<'a>>
    where
        Self: 'a,
    {
        match n {
            MeNode::Expr(MetricExpr::Range { .. })
            | MeNode::Expr(MetricExpr::Literal(_))
            | MeNode::Expr(MetricExpr::VectorFn(_)) => None,
            MeNode::Expr(MetricExpr::Vector { inner, .. }) => match i {
                0 => Some(MeRef::Expr(inner.peek())),
                _ => None,
            },
            MeNode::Expr(MetricExpr::Binary { lhs, rhs, .. }) => match i {
                0 => Some(MeRef::Expr(lhs.peek())),
                1 => Some(MeRef::Expr(rhs.peek())),
                _ => None,
            },
            MeNode::Expr(MetricExpr::Variants(v)) => match i {
                0 => Some(MeRef::Var(v.peek())),
                _ => None,
            },
            MeNode::Expr(MetricExpr::LabelReplace { inner, .. }) => match i {
                0 => Some(MeRef::Expr(inner.peek())),
                _ => None,
            },
            MeNode::Var(v) => v.variants.peek().get(i).map(MeRef::Expr),
        }
    }

    fn shallow_eq(a: MeNode<'_>, b: MeNode<'_>) -> bool {
        match (a, b) {
            (MeNode::Expr(a), MeNode::Expr(b)) => match (a, b) {
                (
                    MetricExpr::Range {
                        op: ao,
                        range: ar,
                        param: ap,
                        grouping: ag,
                    },
                    MetricExpr::Range {
                        op: bo,
                        range: br,
                        param: bp,
                        grouping: bg,
                    },
                ) => ao == bo && ar == br && ap == bp && ag == bg,
                (
                    MetricExpr::Vector {
                        op: ao,
                        grouping: ag,
                        param: ap,
                        ..
                    },
                    MetricExpr::Vector {
                        op: bo,
                        grouping: bg,
                        param: bp,
                        ..
                    },
                ) => ao == bo && ag == bg && ap == bp,
                (MetricExpr::Literal(a), MetricExpr::Literal(b)) => a == b,
                (MetricExpr::VectorFn(a), MetricExpr::VectorFn(b)) => a == b,
                (
                    MetricExpr::Binary {
                        op: ao,
                        modifier: am,
                        ..
                    },
                    MetricExpr::Binary {
                        op: bo,
                        modifier: bm,
                        ..
                    },
                ) => ao == bo && am == bm,
                (MetricExpr::Variants(_), MetricExpr::Variants(_)) => true,
                (
                    MetricExpr::LabelReplace {
                        dst: ad,
                        replacement: ap,
                        src: as_,
                        regex: ar,
                        ..
                    },
                    MetricExpr::LabelReplace {
                        dst: bd,
                        replacement: bp,
                        src: bs,
                        regex: br,
                        ..
                    },
                ) => ad == bd && ap == bp && as_ == bs && ar == br,
                (MetricExpr::Range { .. }, _)
                | (MetricExpr::Vector { .. }, _)
                | (MetricExpr::Literal(_), _)
                | (MetricExpr::VectorFn(_), _)
                | (MetricExpr::Binary { .. }, _)
                | (MetricExpr::Variants(_), _)
                | (MetricExpr::LabelReplace { .. }, _) => false,
            },
            (MeNode::Var(a), MeNode::Var(b)) => {
                a.variants.len() == b.variants.len() && a.range == b.range
            }
            (MeNode::Expr(_), MeNode::Var(_)) | (MeNode::Var(_), MeNode::Expr(_)) => false,
        }
    }

    fn rebuild(n: MeNode<'_>, kids: &mut Vec<MeVal>) -> MeVal {
        match n {
            MeNode::Expr(MetricExpr::Range {
                op,
                range,
                param,
                grouping,
            }) => MeVal::Expr(MetricExpr::Range {
                op: *op,
                range: range.clone(),
                param: param.clone(),
                grouping: grouping.clone(),
            }),
            MeNode::Expr(MetricExpr::Literal(raw)) => MeVal::Expr(MetricExpr::Literal(raw.clone())),
            MeNode::Expr(MetricExpr::VectorFn(raw)) => {
                MeVal::Expr(MetricExpr::VectorFn(raw.clone()))
            }
            MeNode::Expr(MetricExpr::Vector {
                op,
                grouping,
                param,
                ..
            }) => MeVal::Expr(MetricExpr::Vector {
                op: *op,
                grouping: grouping.clone(),
                param: param.clone(),
                inner: Child::new(drain_expr(kids)),
            }),
            MeNode::Expr(MetricExpr::Binary { op, modifier, .. }) => {
                // The tail of `kids` is [.., lhs, rhs].
                let rhs = drain_expr(kids);
                let lhs = drain_expr(kids);
                MeVal::Expr(MetricExpr::Binary {
                    op: *op,
                    modifier: modifier.clone(),
                    lhs: Child::new(lhs),
                    rhs: Child::new(rhs),
                })
            }
            MeNode::Expr(MetricExpr::Variants(_)) => {
                let v = match kids.pop() {
                    Some(MeVal::Var(v)) => v,
                    // Unreachable by construction: `Variants`' sole child
                    // is the `VariantsExpr` node. This is the clone path,
                    // never a `Drop` path, so a release panic is correct.
                    _ => unreachable!("`Variants`' child rebuilds to a `VariantsExpr`"),
                };
                MeVal::Expr(MetricExpr::Variants(Child::new(v)))
            }
            MeNode::Expr(MetricExpr::LabelReplace {
                dst,
                replacement,
                src,
                regex,
                ..
            }) => MeVal::Expr(MetricExpr::LabelReplace {
                inner: Child::new(drain_expr(kids)),
                dst: dst.clone(),
                replacement: replacement.clone(),
                src: src.clone(),
                regex: regex.clone(),
            }),
            MeNode::Var(v) => {
                let n = v.variants.len();
                let at = kids.len() - n;
                let mut out = Vec::with_capacity(n);
                for k in kids.drain(at..) {
                    match k {
                        MeVal::Expr(e) => out.push(e),
                        // Unreachable: every `VariantsExpr` child is a
                        // `MetricExpr`.
                        MeVal::Var(_) => {
                            unreachable!("a `VariantsExpr` child rebuilds to a `MetricExpr`")
                        }
                    }
                }
                MeVal::Var(VariantsExpr {
                    variants: ChildVec::new(out),
                    range: v.range.clone(),
                })
            }
        }
    }

    fn take_own_fields(s: &mut MeSlotNode<'_>) {
        match s {
            MeSlotNode::Expr(e) => match &mut **e {
                MetricExpr::Range { .. }
                | MetricExpr::Literal(_)
                | MetricExpr::VectorFn(_)
                | MetricExpr::Variants(_) => {}
                MetricExpr::Vector {
                    grouping, param, ..
                } => {
                    *grouping = None;
                    *param = None;
                }
                MetricExpr::Binary { modifier, .. } => {
                    *modifier = None;
                }
                MetricExpr::LabelReplace {
                    dst,
                    replacement,
                    src,
                    regex,
                    ..
                } => {
                    *dst = String::new();
                    *replacement = String::new();
                    *src = String::new();
                    *regex = String::new();
                }
            },
            MeSlotNode::Var(v) => {
                v.range = empty_log_range();
            }
        }
    }

    fn steal_children(s: &mut MeSlotNode<'_>, out: &mut ChunkStack<MeVal>) {
        match s {
            MeSlotNode::Expr(e) => match &mut **e {
                MetricExpr::Range { .. } | MetricExpr::Literal(_) | MetricExpr::VectorFn(_) => {}
                MetricExpr::Vector { inner, .. } => {
                    out.push(MeVal::Expr(inner.replace(me_placeholder())));
                }
                MetricExpr::Binary { lhs, rhs, .. } => {
                    // Right-to-left, so LIFO pop order is left-to-right.
                    out.push(MeVal::Expr(rhs.replace(me_placeholder())));
                    out.push(MeVal::Expr(lhs.replace(me_placeholder())));
                }
                MetricExpr::Variants(v) => {
                    out.push(MeVal::Var(v.replace(ve_placeholder())));
                }
                MetricExpr::LabelReplace { inner, .. } => {
                    out.push(MeVal::Expr(inner.replace(me_placeholder())));
                }
            },
            MeSlotNode::Var(v) => {
                let taken = v.variants.take();
                for e in taken.into_iter().rev() {
                    out.push(MeVal::Expr(e));
                }
            }
        }
    }

    #[inline]
    fn val_ref(v: &MeVal) -> MeRef<'_> {
        match v {
            MeVal::Expr(e) => MeRef::Expr(Ref::new(e)),
            MeVal::Var(v) => MeRef::Var(Ref::new(v)),
        }
    }

    #[inline]
    fn val_slot(v: &mut MeVal) -> MeSlot<'_> {
        match v {
            MeVal::Expr(e) => MeSlot::Expr(Slot::new(e)),
            MeVal::Var(v) => MeSlot::Var(Slot::new(v)),
        }
    }
}

/// Drains one `MetricExpr` off the tail of a rebuild value stack.
#[inline]
fn drain_expr(kids: &mut Vec<MeVal>) -> MetricExpr {
    match kids.pop() {
        Some(MeVal::Expr(e)) => e,
        // Unreachable by construction; clone path, never a `Drop` path.
        _ => unreachable!("expected a `MetricExpr` child value on the rebuild stack"),
    }
}

/// Visits every SCC-2 node reachable from `root`, pre-order, left to
/// right. The consumer never names a handle or a [`walk::Walk`].
pub fn for_each_metric_expr<'a>(root: &'a MetricExpr, f: impl FnMut(MeNode<'a>)) {
    walk::preorder::<MetricScc>(MeNode::Expr(root), f);
}

// ---------------------------------------------------------------------
// SCC-2: `Drop`
// ---------------------------------------------------------------------

impl Drop for MetricExpr {
    fn drop(&mut self) {
        #[cfg(test)]
        drop_order::note_visited_expr(self);
        walk::dismantle::<MetricScc>(MeSlot::Expr(Slot::new(self)));
    }
}

// ---------------------------------------------------------------------
// SCC-2: `Clone`, `PartialEq`, `Eq`
// ---------------------------------------------------------------------

impl Clone for MetricExpr {
    fn clone(&self) -> Self {
        match walk::clone_iter::<MetricScc>(MeNode::Expr(self)) {
            MeVal::Expr(e) => e,
            // Unreachable: the root kind is preserved by `rebuild`.
            MeVal::Var(_) => unreachable!("cloning a `MetricExpr` yields a `MetricExpr`"),
        }
    }
}

impl Clone for VariantsExpr {
    fn clone(&self) -> Self {
        match walk::clone_iter::<MetricScc>(MeNode::Var(self)) {
            MeVal::Var(v) => v,
            // Unreachable: the root kind is preserved by `rebuild`.
            MeVal::Expr(_) => unreachable!("cloning a `VariantsExpr` yields a `VariantsExpr`"),
        }
    }
}

impl PartialEq for MetricExpr {
    fn eq(&self, other: &Self) -> bool {
        walk::eq_iter::<MetricScc>(MeNode::Expr(self), MeNode::Expr(other))
    }
}

impl Eq for MetricExpr {}

impl PartialEq for VariantsExpr {
    fn eq(&self, other: &Self) -> bool {
        walk::eq_iter::<MetricScc>(MeNode::Var(self), MeNode::Var(other))
    }
}

impl Eq for VariantsExpr {}

// ---------------------------------------------------------------------
// SCC-2: `Hash`
// ---------------------------------------------------------------------
//
// The derive's write sequence, transcribed: `mem::discriminant(self)` for
// an enum, then each field in DECLARATION order; `Vec<T>` writes its
// length through `write_usize` (the stable spelling of
// `Hasher::write_length_prefix`) and then each element. `VariantsExpr`
// declares `variants` BEFORE `range`, so the length prefix and the N
// children are emitted before the own field that follows them.

/// Step indices for the hash step machine.
mod hash_step {
    pub(super) const ENTER: u32 = 0;
    /// After the sole child of `Vector`/`Variants`, or after the `lhs` of
    /// `Binary`.
    pub(super) const AFTER_FIRST: u32 = 1;
    /// After the `rhs` of `Binary`.
    pub(super) const AFTER_SECOND: u32 = 2;
    /// `VariantsExpr`: step `VAR_BASE + i` is "about to emit child `i`".
    pub(super) const VAR_BASE: u32 = 3;
}

fn hash_scc<H: Hasher>(root: MeNode<'_>, state: &mut H) {
    let done: Result<(), ()> = walk::emit::<MetricScc, ()>(root, |n, step, _depth| {
        match n {
            MeNode::Expr(e) => match e {
                MetricExpr::Range {
                    op,
                    range,
                    param,
                    grouping,
                } => {
                    mem::discriminant(e).hash(state);
                    op.hash(state);
                    range.hash(state);
                    param.hash(state);
                    grouping.hash(state);
                    Ok(Emit::Done)
                }
                MetricExpr::Literal(raw) => {
                    mem::discriminant(e).hash(state);
                    raw.hash(state);
                    Ok(Emit::Done)
                }
                MetricExpr::VectorFn(raw) => {
                    mem::discriminant(e).hash(state);
                    raw.hash(state);
                    Ok(Emit::Done)
                }
                MetricExpr::Vector {
                    op,
                    grouping,
                    param,
                    inner,
                } => {
                    if step == hash_step::ENTER {
                        mem::discriminant(e).hash(state);
                        op.hash(state);
                        grouping.hash(state);
                        param.hash(state);
                        Ok(Emit::Descend {
                            next_step: hash_step::AFTER_FIRST,
                            child: MeRef::Expr(inner.peek()),
                            child_step: hash_step::ENTER,
                            child_level: 0,
                        })
                    } else {
                        Ok(Emit::Done)
                    }
                }
                MetricExpr::Binary {
                    op,
                    modifier,
                    lhs,
                    rhs,
                } => match step {
                    hash_step::ENTER => {
                        mem::discriminant(e).hash(state);
                        op.hash(state);
                        modifier.hash(state);
                        Ok(Emit::Descend {
                            next_step: hash_step::AFTER_FIRST,
                            child: MeRef::Expr(lhs.peek()),
                            child_step: hash_step::ENTER,
                            child_level: 0,
                        })
                    }
                    hash_step::AFTER_FIRST => Ok(Emit::Descend {
                        next_step: hash_step::AFTER_SECOND,
                        child: MeRef::Expr(rhs.peek()),
                        child_step: hash_step::ENTER,
                        child_level: 0,
                    }),
                    _ => Ok(Emit::Done),
                },
                MetricExpr::Variants(v) => {
                    if step == hash_step::ENTER {
                        mem::discriminant(e).hash(state);
                        Ok(Emit::Descend {
                            next_step: hash_step::AFTER_FIRST,
                            child: MeRef::Var(v.peek()),
                            child_step: hash_step::ENTER,
                            child_level: 0,
                        })
                    } else {
                        Ok(Emit::Done)
                    }
                }
                MetricExpr::LabelReplace {
                    inner,
                    dst,
                    replacement,
                    src,
                    regex,
                } => {
                    // Declaration order: `inner` precedes the own string
                    // fields, so the child's hash is emitted between the
                    // discriminant and the strings — exactly the derive's
                    // field order.
                    if step == hash_step::ENTER {
                        mem::discriminant(e).hash(state);
                        Ok(Emit::Descend {
                            next_step: hash_step::AFTER_FIRST,
                            child: MeRef::Expr(inner.peek()),
                            child_step: hash_step::ENTER,
                            child_level: 0,
                        })
                    } else {
                        dst.hash(state);
                        replacement.hash(state);
                        src.hash(state);
                        regex.hash(state);
                        Ok(Emit::Done)
                    }
                }
            },
            MeNode::Var(v) => {
                let kids = v.variants.peek();
                if step == hash_step::ENTER {
                    // `impl Hash for [T]` writes the length prefix first.
                    state.write_usize(kids.len());
                }
                let i = if step == hash_step::ENTER {
                    0
                } else {
                    (step - hash_step::VAR_BASE) as usize
                };
                match kids.get(i) {
                    Some(c) => Ok(Emit::Descend {
                        next_step: hash_step::VAR_BASE + i as u32 + 1,
                        child: MeRef::Expr(c),
                        child_step: hash_step::ENTER,
                        child_level: 0,
                    }),
                    None => {
                        v.range.hash(state);
                        Ok(Emit::Done)
                    }
                }
            }
        }
    });
    match done {
        Ok(()) => {}
        // Unreachable: the step machine's error type is uninhabited in
        // practice — no step above returns `Err`.
        Err(()) => unreachable!("the hash step machine never fails"),
    }
}

impl Hash for MetricExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        hash_scc(MeNode::Expr(self), state);
    }
}

impl Hash for VariantsExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        hash_scc(MeNode::Var(self), state);
    }
}

// ---------------------------------------------------------------------
// SCC-2: `Debug` (both modes)
// ---------------------------------------------------------------------
//
// Byte-equivalent to `#[derive(Debug)]`: `Name { k: v, k: v }` /
// `Name(v)` compact, and the `debug_struct`/`debug_tuple`/`debug_list`
// alternate layouts, whose indentation is `4 * depth` — exactly the
// driver's frame depth. Non-recursive own fields are rendered through
// [`walk::PadWriter`] at that depth so a nested derived value indents as
// `debug_struct` would.

mod dbg_step {
    pub(super) const ENTER: u32 = 0;
    pub(super) const AFTER_FIRST: u32 = 1;
    pub(super) const AFTER_SECOND: u32 = 2;
    pub(super) const VAR_BASE: u32 = 3;
}

fn debug_scc(root: MeNode<'_>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let alt = f.alternate();
    walk::emit::<MetricScc, fmt::Error>(root, |n, step, depth| match n {
        MeNode::Expr(e) => match e {
            MetricExpr::Range {
                op,
                range,
                param,
                grouping,
            } => {
                walk::dbg_open_struct(f, "Range", alt, depth)?;
                f.write_str("op: ")?;
                walk::dbg_own(f, op, alt, depth + 1)?;
                walk::dbg_sep(f, alt, depth)?;
                f.write_str("range: ")?;
                walk::dbg_own(f, range, alt, depth + 1)?;
                walk::dbg_sep(f, alt, depth)?;
                f.write_str("param: ")?;
                walk::dbg_own(f, param, alt, depth + 1)?;
                walk::dbg_sep(f, alt, depth)?;
                f.write_str("grouping: ")?;
                walk::dbg_own(f, grouping, alt, depth + 1)?;
                walk::dbg_close_struct(f, alt, depth)?;
                Ok(Emit::Done)
            }
            MetricExpr::Literal(raw) => {
                walk::dbg_open_tuple(f, "Literal", alt, depth)?;
                walk::dbg_own(f, raw, alt, depth + 1)?;
                walk::dbg_close_tuple(f, alt, depth)?;
                Ok(Emit::Done)
            }
            MetricExpr::VectorFn(raw) => {
                walk::dbg_open_tuple(f, "VectorFn", alt, depth)?;
                walk::dbg_own(f, raw, alt, depth + 1)?;
                walk::dbg_close_tuple(f, alt, depth)?;
                Ok(Emit::Done)
            }
            MetricExpr::Vector {
                op,
                grouping,
                param,
                inner,
            } => {
                if step == dbg_step::ENTER {
                    walk::dbg_open_struct(f, "Vector", alt, depth)?;
                    f.write_str("op: ")?;
                    walk::dbg_own(f, op, alt, depth + 1)?;
                    walk::dbg_sep(f, alt, depth)?;
                    f.write_str("grouping: ")?;
                    walk::dbg_own(f, grouping, alt, depth + 1)?;
                    walk::dbg_sep(f, alt, depth)?;
                    f.write_str("param: ")?;
                    walk::dbg_own(f, param, alt, depth + 1)?;
                    walk::dbg_sep(f, alt, depth)?;
                    f.write_str("inner: ")?;
                    Ok(Emit::Descend {
                        next_step: dbg_step::AFTER_FIRST,
                        child: MeRef::Expr(inner.peek()),
                        child_step: dbg_step::ENTER,
                        child_level: depth + 1,
                    })
                } else {
                    walk::dbg_close_struct(f, alt, depth)?;
                    Ok(Emit::Done)
                }
            }
            MetricExpr::Binary {
                op,
                modifier,
                lhs,
                rhs,
            } => match step {
                dbg_step::ENTER => {
                    walk::dbg_open_struct(f, "Binary", alt, depth)?;
                    f.write_str("op: ")?;
                    walk::dbg_own(f, op, alt, depth + 1)?;
                    walk::dbg_sep(f, alt, depth)?;
                    f.write_str("modifier: ")?;
                    walk::dbg_own(f, modifier, alt, depth + 1)?;
                    walk::dbg_sep(f, alt, depth)?;
                    f.write_str("lhs: ")?;
                    Ok(Emit::Descend {
                        next_step: dbg_step::AFTER_FIRST,
                        child: MeRef::Expr(lhs.peek()),
                        child_step: dbg_step::ENTER,
                        child_level: depth + 1,
                    })
                }
                dbg_step::AFTER_FIRST => {
                    walk::dbg_sep(f, alt, depth)?;
                    f.write_str("rhs: ")?;
                    Ok(Emit::Descend {
                        next_step: dbg_step::AFTER_SECOND,
                        child: MeRef::Expr(rhs.peek()),
                        child_step: dbg_step::ENTER,
                        child_level: depth + 1,
                    })
                }
                _ => {
                    walk::dbg_close_struct(f, alt, depth)?;
                    Ok(Emit::Done)
                }
            },
            MetricExpr::Variants(v) => {
                if step == dbg_step::ENTER {
                    walk::dbg_open_tuple(f, "Variants", alt, depth)?;
                    Ok(Emit::Descend {
                        next_step: dbg_step::AFTER_FIRST,
                        child: MeRef::Var(v.peek()),
                        child_step: dbg_step::ENTER,
                        child_level: depth + 1,
                    })
                } else {
                    walk::dbg_close_tuple(f, alt, depth)?;
                    Ok(Emit::Done)
                }
            }
            MetricExpr::LabelReplace {
                inner,
                dst,
                replacement,
                src,
                regex,
            } => {
                if step == dbg_step::ENTER {
                    walk::dbg_open_struct(f, "LabelReplace", alt, depth)?;
                    f.write_str("inner: ")?;
                    Ok(Emit::Descend {
                        next_step: dbg_step::AFTER_FIRST,
                        child: MeRef::Expr(inner.peek()),
                        child_step: dbg_step::ENTER,
                        child_level: depth + 1,
                    })
                } else {
                    walk::dbg_sep(f, alt, depth)?;
                    f.write_str("dst: ")?;
                    walk::dbg_own(f, dst, alt, depth + 1)?;
                    walk::dbg_sep(f, alt, depth)?;
                    f.write_str("replacement: ")?;
                    walk::dbg_own(f, replacement, alt, depth + 1)?;
                    walk::dbg_sep(f, alt, depth)?;
                    f.write_str("src: ")?;
                    walk::dbg_own(f, src, alt, depth + 1)?;
                    walk::dbg_sep(f, alt, depth)?;
                    f.write_str("regex: ")?;
                    walk::dbg_own(f, regex, alt, depth + 1)?;
                    walk::dbg_close_struct(f, alt, depth)?;
                    Ok(Emit::Done)
                }
            }
        },
        MeNode::Var(v) => {
            let kids = v.variants.peek();
            if step == dbg_step::ENTER {
                walk::dbg_open_struct(f, "VariantsExpr", alt, depth)?;
                f.write_str("variants: ")?;
                if kids.is_empty() {
                    // `debug_list` renders an empty list as `[]` in both
                    // modes.
                    f.write_str("[]")?;
                    walk::dbg_sep(f, alt, depth)?;
                    f.write_str("range: ")?;
                    walk::dbg_own(f, &v.range, alt, depth + 1)?;
                    walk::dbg_close_struct(f, alt, depth)?;
                    return Ok(Emit::Done);
                }
                if alt {
                    f.write_str("[\n")?;
                    walk::write_pad(f, depth + 2)?;
                } else {
                    f.write_str("[")?;
                }
                return Ok(Emit::Descend {
                    next_step: dbg_step::VAR_BASE + 1,
                    child: MeRef::Expr(kids.get(0).unwrap_or_else(|| {
                        unreachable!("the non-empty branch always has a child at 0")
                    })),
                    child_step: dbg_step::ENTER,
                    child_level: depth + 2,
                });
            }
            let i = (step - dbg_step::VAR_BASE) as usize;
            match kids.get(i) {
                Some(c) => {
                    if alt {
                        f.write_str(",\n")?;
                        walk::write_pad(f, depth + 2)?;
                    } else {
                        f.write_str(", ")?;
                    }
                    Ok(Emit::Descend {
                        next_step: dbg_step::VAR_BASE + i as u32 + 1,
                        child: MeRef::Expr(c),
                        child_step: dbg_step::ENTER,
                        child_level: depth + 2,
                    })
                }
                None => {
                    if alt {
                        f.write_str(",\n")?;
                        walk::write_pad(f, depth + 1)?;
                        f.write_str("]")?;
                    } else {
                        f.write_str("]")?;
                    }
                    walk::dbg_sep(f, alt, depth)?;
                    f.write_str("range: ")?;
                    walk::dbg_own(f, &v.range, alt, depth + 1)?;
                    walk::dbg_close_struct(f, alt, depth)?;
                    Ok(Emit::Done)
                }
            }
        }
    })
}

impl fmt::Debug for MetricExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_scc(MeNode::Expr(self), f)
    }
}

impl fmt::Debug for VariantsExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_scc(MeNode::Var(self), f)
    }
}

// ---------------------------------------------------------------------
// SCC-2: `Display`
// ---------------------------------------------------------------------
//
// `fmt_operand`'s parenthesisation becomes explicit open/close tokens.
// A node reached as a binary operand is entered at `OPERAND`, which
// writes `(` when the node is itself a `Binary`, descends into the very
// same node at `ENTER`, and closes the parenthesis on the way back —
// exactly the pre-change `fmt_operand` predicate, one frame per operand
// rather than one call.

mod disp_step {
    pub(super) const ENTER: u32 = 0;
    pub(super) const AFTER_FIRST: u32 = 1;
    pub(super) const AFTER_SECOND: u32 = 2;
    pub(super) const OPERAND: u32 = 3;
    pub(super) const OPERAND_CLOSE: u32 = 4;
    pub(super) const VAR_BASE: u32 = 5;
}

#[inline]
fn is_binary(n: MeNode<'_>) -> bool {
    matches!(n, MeNode::Expr(MetricExpr::Binary { .. }))
}

fn display_scc(root: MeNode<'_>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    walk::emit::<MetricScc, fmt::Error>(root, |n, step, _depth| {
        if step == disp_step::OPERAND {
            if is_binary(n) {
                f.write_str("(")?;
            }
            return Ok(Emit::Descend {
                next_step: disp_step::OPERAND_CLOSE,
                child: MetricScc::wrap(n),
                child_step: disp_step::ENTER,
                child_level: 0,
            });
        }
        if step == disp_step::OPERAND_CLOSE {
            if is_binary(n) {
                f.write_str(")")?;
            }
            return Ok(Emit::Done);
        }
        match n {
            MeNode::Expr(e) => match e {
                MetricExpr::Range {
                    op,
                    range,
                    param,
                    grouping,
                } => {
                    match param {
                        Some(p) => write!(f, "{op}({p}, {range})")?,
                        None => write!(f, "{op}({range})")?,
                    }
                    // POSTFIX only — the reference rejects the prefix
                    // placement here (issue #344), so rendering it that way
                    // would produce a string that does not reparse there.
                    if let Some(g) = grouping {
                        write!(f, " {g}")?;
                    }
                    Ok(Emit::Done)
                }
                MetricExpr::Literal(raw) => {
                    write!(f, "{raw}")?;
                    Ok(Emit::Done)
                }
                MetricExpr::VectorFn(raw) => {
                    write!(f, "vector({raw})")?;
                    Ok(Emit::Done)
                }
                MetricExpr::Vector {
                    op,
                    grouping,
                    param,
                    inner,
                } => {
                    if step == disp_step::ENTER {
                        match grouping {
                            Some(g) => write!(f, "{op} {g}(")?,
                            None => write!(f, "{op}(")?,
                        }
                        if let Some(p) = param {
                            write!(f, "{p}, ")?;
                        }
                        Ok(Emit::Descend {
                            next_step: disp_step::AFTER_FIRST,
                            child: MeRef::Expr(inner.peek()),
                            child_step: disp_step::ENTER,
                            child_level: 0,
                        })
                    } else {
                        f.write_str(")")?;
                        Ok(Emit::Done)
                    }
                }
                MetricExpr::Binary {
                    op,
                    modifier,
                    lhs,
                    rhs,
                } => match step {
                    disp_step::ENTER => Ok(Emit::Descend {
                        next_step: disp_step::AFTER_FIRST,
                        child: MeRef::Expr(lhs.peek()),
                        child_step: disp_step::OPERAND,
                        child_level: 0,
                    }),
                    disp_step::AFTER_FIRST => {
                        write!(f, " {op} ")?;
                        if let Some(m) = modifier {
                            if m.return_bool {
                                write!(f, "bool ")?;
                            }
                            if let Some(vm) = &m.matching {
                                write!(f, "{vm} ")?;
                            }
                        }
                        Ok(Emit::Descend {
                            next_step: disp_step::AFTER_SECOND,
                            child: MeRef::Expr(rhs.peek()),
                            child_step: disp_step::OPERAND,
                            child_level: 0,
                        })
                    }
                    _ => Ok(Emit::Done),
                },
                MetricExpr::Variants(v) => {
                    if step == disp_step::ENTER {
                        Ok(Emit::Descend {
                            next_step: disp_step::AFTER_FIRST,
                            child: MeRef::Var(v.peek()),
                            child_step: disp_step::ENTER,
                            child_level: 0,
                        })
                    } else {
                        Ok(Emit::Done)
                    }
                }
                MetricExpr::LabelReplace {
                    inner,
                    dst,
                    replacement,
                    src,
                    regex,
                } => {
                    if step == disp_step::ENTER {
                        f.write_str("label_replace(")?;
                        // `ENTER`, not `OPERAND`: a binary inner needs no
                        // parentheses inside the call's own parens, and
                        // the round-trip oracle re-parses it identically.
                        Ok(Emit::Descend {
                            next_step: disp_step::AFTER_FIRST,
                            child: MeRef::Expr(inner.peek()),
                            child_step: disp_step::ENTER,
                            child_level: 0,
                        })
                    } else {
                        write!(
                            f,
                            ", {}, {}, {}, {})",
                            quote(dst),
                            quote(replacement),
                            quote(src),
                            quote(regex)
                        )?;
                        Ok(Emit::Done)
                    }
                }
            },
            MeNode::Var(v) => {
                let kids = v.variants.peek();
                if step == disp_step::ENTER {
                    f.write_str("variants(")?;
                    match kids.get(0) {
                        Some(c) => {
                            return Ok(Emit::Descend {
                                next_step: disp_step::VAR_BASE + 1,
                                child: MeRef::Expr(c),
                                child_step: disp_step::ENTER,
                                child_level: 0,
                            });
                        }
                        None => {
                            write!(f, ") of ({})", v.range)?;
                            return Ok(Emit::Done);
                        }
                    }
                }
                let i = (step - disp_step::VAR_BASE) as usize;
                match kids.get(i) {
                    Some(c) => {
                        f.write_str(", ")?;
                        Ok(Emit::Descend {
                            next_step: disp_step::VAR_BASE + i as u32 + 1,
                            child: MeRef::Expr(c),
                            child_step: disp_step::ENTER,
                            child_level: 0,
                        })
                    }
                    None => {
                        write!(f, ") of ({})", v.range)?;
                        Ok(Emit::Done)
                    }
                }
            }
        }
    })
}

impl fmt::Display for MetricExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        display_scc(MeNode::Expr(self), f)
    }
}

impl fmt::Display for VariantsExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        display_scc(MeNode::Var(self), f)
    }
}

/// Declares [`BinOp`], its rendering and its COMPLETE variant list from
/// one source (issue #293 review round 3).
///
/// A hand-maintained `ALL` slice beside a hand-written enum is two
/// sources: a wildcard-free `match` elsewhere forces an author to touch
/// the file when a variant is added, but that edit can satisfy the match
/// while leaving the variant out of the slice — and out of every census
/// and corpus that enumerates the operator space through it. Emitting
/// both from one invocation makes that omission unrepresentable.
macro_rules! bin_ops {
    ($($variant:ident => $symbol:literal,)+) => {
        /// The M6-10 binary operators: arithmetic, comparison, and the
        /// `and`/`or`/`unless` set operations.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum BinOp { $($variant),+ }

        impl BinOp {
            /// Every variant, in declaration order — emitted by the same
            /// invocation that declares them, so no enumeration driven by
            /// this slice can miss an operator the enum has.
            pub const ALL: &'static [BinOp] = &[$(BinOp::$variant),+];
        }

        impl fmt::Display for BinOp {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let s = match self { $(BinOp::$variant => $symbol),+ };
                write!(f, "{s}")
            }
        }
    };
}

bin_ops! {
    Add => "+",
    Sub => "-",
    Mul => "*",
    Div => "/",
    Mod => "%",
    Pow => "^",
    Eq => "==",
    Neq => "!=",
    Gt => ">",
    Gte => ">=",
    Lt => "<",
    Lte => "<=",
    And => "and",
    Or => "or",
    Unless => "unless",
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
    /// `offset <duration>` (issue #343) — shifts the evaluated window
    /// BACK by this much: the selector is evaluated over
    /// `(T - offset - range, T - offset]`.
    ///
    /// Signed NANOSECONDS rather than [`Duration`], which is `u64`,
    /// because **the reference accepts a NEGATIVE offset** and shifts the
    /// window forward. Measured against the pinned v3.7.4 container:
    /// `rate({app="x"}[5m] offset -1h)` is a 200. It looks like a bug and
    /// is not — do not "tidy" it into a rejection.
    pub offset_ns: Option<i64>,
}

impl fmt::Display for LogRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}[{}]", self.selector, self.range)?;
        if let Some(ns) = self.offset_ns {
            // Rendered from the ABSOLUTE value with an explicit sign, so a
            // negative offset round-trips (`Duration` is unsigned).
            let mag = Duration::from_nanos(ns.unsigned_abs());
            let sign = if ns < 0 { "-" } else { "" };
            write!(f, " offset {sign}{mag}")?;
        }
        Ok(())
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
    /// Resolves a range-aggregation KEYWORD. Case-insensitive, because
    /// the reference's lexer resolves keywords case-insensitively
    /// (issue #339: `RATE(...)`/`Rate(...)` are 200s there). Folding
    /// lives here rather than at the call sites so a new caller cannot
    /// reintroduce the case-sensitive lookup.
    pub(crate) fn from_ident(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
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

    /// Whether the reference admits a `by`/`without` clause on this range
    /// aggregation (issue #344). Captured by probing grafana/loki v3.7.4
    /// directly — every one of the fifteen ops, in both directions — NOT
    /// by reasoning about which ones look like they should: `sum_over_time`
    /// and `rate` over an unwrapped range are the trap, since both take a
    /// numeric sample stream and both are rejected. The eight that admit
    /// it are exactly the sample-selecting/summarising ops; the seven that
    /// do not answer
    /// `parse error : grouping not allowed for <op> aggregation`.
    pub fn allows_grouping(self) -> bool {
        match self {
            RangeAggOp::AvgOverTime
            | RangeAggOp::MinOverTime
            | RangeAggOp::MaxOverTime
            | RangeAggOp::StddevOverTime
            | RangeAggOp::StdvarOverTime
            | RangeAggOp::QuantileOverTime
            | RangeAggOp::FirstOverTime
            | RangeAggOp::LastOverTime => true,
            RangeAggOp::Rate
            | RangeAggOp::RateCounter
            | RangeAggOp::CountOverTime
            | RangeAggOp::BytesRate
            | RangeAggOp::BytesOverTime
            | RangeAggOp::SumOverTime
            | RangeAggOp::AbsentOverTime => false,
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
    /// Resolves a vector-aggregation KEYWORD — case-insensitive for the
    /// same reason as [`RangeAggOp::from_ident`] (issue #339).
    pub(crate) fn from_ident(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
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
// §3).
//
// This comment used to read: "`offset` is deliberately absent — it is a
// PromQL-ism with no LogQL grammar". **That was false, and it is the
// mechanism that hid the gap** (issue #343). The reference accepts
// `offset` on every range selector — measured against the pinned v3.7.4
// container — so the construct was not merely missing a registry row: we
// had written down that it does not exist, which excluded it from THIS
// table too, so it never even became a `NotYetSupported` error. A missing
// entry is an absence; a false one removes the construct from the place
// you would look for it.
//
// The detector, worth carrying: that claim was justified by where the
// construct CAME FROM ("a PromQL-ism"), not by what the reference's
// grammar accepts. A claim resting on a construct's origin is a claim
// nobody measured, and it reads exactly as confidently as one that was.
// Its neighbours here were audited against the container when this was
// found (`ip`, the unwrap conversions, the identifier-shaped binary
// operators) and all three hold — so the list was written from
// measurement and this single entry was not.
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

#[cfg(test)]
mod drop_order;
