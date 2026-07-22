// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::label::{Labels, METRIC_NAME, Matchers};
use crate::parser::token::{
    self, T_BOTTOMK, T_COUNT_VALUES, T_END, T_QUANTILE, T_START, T_TOPK, token_display,
};
use crate::parser::token::{Token, TokenId, TokenType};
use crate::parser::value::ValueType;
use crate::parser::{Function, FunctionArgs, MAX_CHARACTERS_PER_LINE, Prettier, indent};
use crate::util::{display_duration, escape_string};
use chrono::{DateTime, Utc};
use std::fmt::{self, Write};
use std::ops::Neg;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// LabelModifier acts as
///
/// # Aggregation Modifier
///
/// - Exclude means `ignoring`
/// - Include means `on`
///
/// # Vector Match Modifier
///
/// - Exclude means `without` removes the listed labels from the result vector,
///   while all other labels are preserved in the output.
///
/// - Include means `by` does the opposite and drops labels that are not listed in the by clause,
///   even if their label values are identical between all elements of the vector.
///
/// if empty listed labels, meaning no grouping
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LabelModifier {
    Include(Labels),
    Exclude(Labels),
}

impl LabelModifier {
    pub fn labels(&self) -> &Labels {
        match self {
            LabelModifier::Include(l) => l,
            LabelModifier::Exclude(l) => l,
        }
    }

    pub fn is_include(&self) -> bool {
        matches!(*self, LabelModifier::Include(_))
    }

    pub fn include(ls: Vec<&str>) -> Self {
        Self::Include(Labels::new(ls))
    }

    pub fn exclude(ls: Vec<&str>) -> Self {
        Self::Exclude(Labels::new(ls))
    }
}

/// The label list provided with the group_left or group_right modifier contains
/// additional labels from the "one"-side to be included in the result metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "ser", derive(serde::Serialize))]
#[cfg_attr(feature = "ser", serde(rename_all = "kebab-case"))]
pub enum VectorMatchCardinality {
    OneToOne,
    ManyToOne(Labels),
    OneToMany(Labels),
    ManyToMany, // logical/set binary operators
}

impl VectorMatchCardinality {
    pub fn labels(&self) -> Option<&Labels> {
        match self {
            VectorMatchCardinality::ManyToOne(l) => Some(l),
            VectorMatchCardinality::OneToMany(l) => Some(l),
            VectorMatchCardinality::ManyToMany => None,
            VectorMatchCardinality::OneToOne => None,
        }
    }

    pub fn many_to_one(ls: Vec<&str>) -> Self {
        Self::ManyToOne(Labels::new(ls))
    }

    pub fn one_to_many(ls: Vec<&str>) -> Self {
        Self::OneToMany(Labels::new(ls))
    }
}

/// VectorMatchFillValues contains the fill values to use for Vector matching
/// when one side does not find a match on the other side.
/// When a fill value is nil, no fill is applied for that side, and there
/// is no output for the match group if there is no match.
#[derive(Debug, Clone, PartialEq, Default)]
#[cfg_attr(feature = "ser", derive(serde::Serialize))]
pub struct VectorMatchFillValues {
    pub rhs: Option<f64>,
    pub lhs: Option<f64>,
}

impl VectorMatchFillValues {
    pub fn new(lhs: f64, rhs: f64) -> Self {
        Self {
            rhs: Some(rhs),
            lhs: Some(lhs),
        }
    }

    pub fn with_rhs(mut self, rhs: f64) -> Self {
        self.rhs = Some(rhs);
        self
    }

    pub fn with_lhs(mut self, lhs: f64) -> Self {
        self.lhs = Some(lhs);
        self
    }
}

/// Binary Expr Modifier
#[derive(Debug, Clone, PartialEq)]
pub struct BinModifier {
    /// The matching behavior for the operation if both operands are Vectors.
    /// If they are not this field is None.
    pub card: VectorMatchCardinality,

    /// on/ignoring on labels.
    /// like a + b, no match modifier is needed.
    pub matching: Option<LabelModifier>,
    /// If a comparison operator, return 0/1 rather than filtering.
    pub return_bool: bool,
    /// Fill-in values to use when a series from one side does not find a match
    /// on the other side.
    pub fill_values: VectorMatchFillValues,
}

impl fmt::Display for BinModifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = String::from(self.bool_str());

        if let Some(matching) = &self.matching {
            match matching {
                LabelModifier::Include(ls) => write!(s, "on ({ls}) ")?,
                LabelModifier::Exclude(ls) if !ls.is_empty() => write!(s, "ignoring ({ls}) ")?,
                _ => (),
            }
        }

        match &self.card {
            VectorMatchCardinality::ManyToOne(ls) => write!(s, "group_left ({ls}) ")?,
            VectorMatchCardinality::OneToMany(ls) => write!(s, "group_right ({ls}) ")?,
            _ => (),
        }

        if self.fill_values.rhs.is_some() || self.fill_values.lhs.is_some() {
            if self.fill_values.rhs == self.fill_values.lhs {
                let fill_value = self.fill_values.rhs.unwrap();
                write!(s, "fill ({fill_value}) ")?;
            } else {
                if let Some(fill_value) = self.fill_values.lhs {
                    write!(s, "fill_left ({fill_value}) ")?;
                }

                if let Some(fill_value) = self.fill_values.rhs {
                    write!(s, "fill_right ({fill_value}) ")?;
                }
            }
        }

        if s.trim().is_empty() {
            write!(f, "")
        } else {
            write!(f, " {}", s.trim_end()) // there is a leading space here
        }
    }
}

impl Default for BinModifier {
    fn default() -> Self {
        Self {
            card: VectorMatchCardinality::OneToOne,
            matching: None,
            return_bool: false,
            fill_values: VectorMatchFillValues::default(),
        }
    }
}

impl BinModifier {
    pub fn with_card(mut self, card: VectorMatchCardinality) -> Self {
        self.card = card;
        self
    }

    pub fn with_matching(mut self, matching: Option<LabelModifier>) -> Self {
        self.matching = matching;
        self
    }

    pub fn with_return_bool(mut self, return_bool: bool) -> Self {
        self.return_bool = return_bool;
        self
    }

    pub fn with_fill_values(mut self, fill_values: VectorMatchFillValues) -> Self {
        self.fill_values = fill_values;
        self
    }

    pub fn is_labels_joint(&self) -> bool {
        matches!(
            (self.card.labels(), &self.matching),
            (Some(labels), Some(matching)) if labels.is_joint(matching.labels())
        )
    }

    pub fn intersect_labels(&self) -> Option<Vec<String>> {
        if let Some(labels) = self.card.labels() {
            if let Some(matching) = &self.matching {
                return Some(labels.intersect(matching.labels()).labels);
            }
        };
        None
    }

    pub fn is_matching_on(&self) -> bool {
        matches!(&self.matching, Some(matching) if matching.is_include())
    }

    pub fn is_matching_labels_not_empty(&self) -> bool {
        matches!(&self.matching, Some(matching) if !matching.labels().is_empty())
    }

    pub fn bool_str(&self) -> &str {
        if self.return_bool { "bool " } else { "" }
    }
}

#[cfg(feature = "ser")]
pub(crate) fn serialize_grouping<S>(
    this: &Option<LabelModifier>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::SerializeMap;
    let mut map = serializer.serialize_map(Some(2))?;
    match this {
        Some(LabelModifier::Include(l)) => {
            map.serialize_entry("grouping", l)?;
            map.serialize_entry("without", &false)?;
        }
        Some(LabelModifier::Exclude(l)) => {
            map.serialize_entry("grouping", l)?;
            map.serialize_entry("without", &true)?;
        }
        None => {
            map.serialize_entry("grouping", &(vec![] as Vec<String>))?;
            map.serialize_entry("without", &false)?;
        }
    }

    map.end()
}

#[cfg(feature = "ser")]
pub(crate) fn serialize_bin_modifier<S>(
    this: &Option<BinModifier>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::SerializeMap;
    use serde_json::json;

    let mut map = serializer.serialize_map(Some(2))?;

    map.serialize_entry(
        "bool",
        &this.as_ref().map(|t| t.return_bool).unwrap_or(false),
    )?;
    if let Some(t) = this {
        if let Some(labels) = &t.matching {
            map.serialize_key("matching")?;

            match labels {
                LabelModifier::Include(labels) => {
                    let value = json!({
                        "card": t.card,
                        "include": [],
                        "labels": labels,
                        "on": true,
                        "fillValues": t.fill_values,
                    });
                    map.serialize_value(&value)?;
                }
                LabelModifier::Exclude(labels) => {
                    let value = json!({
                        "card": t.card,
                        "include": [],
                        "labels": labels,
                        "on": false,
                        "fillValues": t.fill_values,
                    });
                    map.serialize_value(&value)?;
                }
            }
        } else {
            let value = json!({
                "card": t.card,
                "include": [],
                "labels": [],
                "on": false,
                "fillValues": t.fill_values,
            });
            map.serialize_entry("matching", &value)?;
        }
    } else {
        map.serialize_entry("matching", &None::<bool>)?;
    }

    map.end()
}

#[cfg(feature = "ser")]
pub(crate) fn serialize_at_modifier<S>(
    this: &Option<AtModifier>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::SerializeMap;
    let mut map = serializer.serialize_map(Some(2))?;
    match this {
        Some(AtModifier::Start) => {
            map.serialize_entry("startOrEnd", &Some("start"))?;
            map.serialize_entry("timestamp", &None::<u128>)?;
        }
        Some(AtModifier::End) => {
            map.serialize_entry("startOrEnd", &Some("end"))?;
            map.serialize_entry("timestamp", &None::<u128>)?;
        }
        Some(AtModifier::At(time)) => {
            map.serialize_entry("startOrEnd", &None::<&str>)?;
            map.serialize_entry(
                "timestamp",
                &time
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or(Duration::ZERO)
                    .as_millis(),
            )?;
        }
        None => {
            map.serialize_entry("startOrEnd", &None::<&str>)?;
            map.serialize_entry("timestamp", &None::<u128>)?;
        }
    }

    map.end()
}

#[derive(Debug, Clone, PartialEq)]
pub enum Offset {
    Pos(Duration),
    Neg(Duration),
}

impl Offset {
    #[cfg(feature = "ser")]
    pub(crate) fn as_millis(&self) -> i128 {
        match self {
            Self::Pos(dur) => dur.as_millis() as i128,
            Self::Neg(dur) => -(dur.as_millis() as i128),
        }
    }

    #[cfg(feature = "ser")]
    pub(crate) fn serialize_offset<S>(
        offset: &Option<Self>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let value = offset.as_ref().map(|o| o.as_millis()).unwrap_or(0);
        serializer.serialize_i128(value)
    }
}

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Offset::Pos(dur) => write!(f, "{}", display_duration(dur)),
            Offset::Neg(dur) => write!(f, "-{}", display_duration(dur)),
        }
    }
}

/// A duration expression (PulsusDB patch, docs/decisions/0003 grammar
/// patch G1) — Prometheus v3.13.0's `DurationExpr` node (at the pinned
/// conformance SHA), self-contained rather than an [`Expr`] variant: it
/// only ever appears in the range-selector, subquery range/step, and
/// `offset` positions, carried in the `*_expr: Option<DurationExpr>`
/// fields next to the corresponding concrete field (upstream's dual
/// `Range`+`RangeExpr` model). A plain (possibly sign-folded) literal
/// resolves to the concrete field at parse time and never builds one of
/// these; every other form — arithmetic, `step()`, `range()`,
/// `min_of`/`max_of`, parentheses — parses into this tree and is resolved
/// against the query's step/range downstream.
///
/// `Number` values are seconds (upstream `NumberLiteral.Val`): a
/// `DURATION` lexeme is folded to `parse_duration(..).as_secs_f64()`.
/// `Wrapped` preserves explicit parentheses (upstream's `Wrapped` flag) so
/// `parse -> Display -> parse` round-trips to an equal tree.
#[derive(Debug, Clone, PartialEq)]
pub enum DurationExpr {
    /// A literal sub-expression, in seconds.
    Number(f64),
    /// `step()` — the query resolution step.
    Step,
    /// `range()` — the query range (`end - start`).
    Range,
    /// Unary `+`. Upstream folds unary plus away on some paths and keeps
    /// an `ADD`-with-nil-LHS node on others; this tree keeps it uniformly
    /// (and displays it) so round-trips are exact. Resolution is identity.
    Pos(Box<DurationExpr>),
    /// Unary `-`.
    Neg(Box<DurationExpr>),
    Add(Box<DurationExpr>, Box<DurationExpr>),
    Sub(Box<DurationExpr>, Box<DurationExpr>),
    Mul(Box<DurationExpr>, Box<DurationExpr>),
    Div(Box<DurationExpr>, Box<DurationExpr>),
    Mod(Box<DurationExpr>, Box<DurationExpr>),
    Pow(Box<DurationExpr>, Box<DurationExpr>),
    MinOf(Box<DurationExpr>, Box<DurationExpr>),
    MaxOf(Box<DurationExpr>, Box<DurationExpr>),
    /// An explicitly parenthesised sub-expression.
    Wrapped(Box<DurationExpr>),
}

impl DurationExpr {
    /// The folded literal seconds value of a parenthesised and/or
    /// unary-signed numeric literal (`(5m)`, `-(5)`, `-((0))`), or `None`
    /// for any genuinely computed expression. Upstream v3.13 keeps such a
    /// tree a `*NumberLiteral` all the way through
    /// (`paren_duration_expr`'s `$$ = $2` and
    /// `applyUnaryOpToDurationExpr`'s literal fold), so it stays subject
    /// to the *literal* guards (positivity, division/modulo by literal
    /// zero) and the literal nanosecond-rounding path — while the
    /// parenthesised form is still experimental-gated
    /// (`experimentalDurationExpr($2)` fires before the literal is
    /// unwrapped). This crate keeps the `Wrapped`/`Pos`/`Neg` metadata for
    /// Display round-trip fidelity and gate-presence checks; consumers use
    /// this accessor to recover the upstream literal semantics.
    pub fn literal_value(&self) -> Option<f64> {
        match self {
            DurationExpr::Number(v) => Some(*v),
            DurationExpr::Wrapped(e) | DurationExpr::Pos(e) => e.literal_value(),
            DurationExpr::Neg(e) => e.literal_value().map(|v| -v),
            _ => None,
        }
    }
}

impl fmt::Display for DurationExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Same rendering as `NumberLiteral` (upstream prints the
            // literal's value; a re-parse folds it back to the same
            // seconds value either way).
            DurationExpr::Number(val) => {
                if *val == f64::INFINITY {
                    write!(f, "Inf")
                } else if *val == f64::NEG_INFINITY {
                    write!(f, "-Inf")
                } else if f64::is_nan(*val) {
                    write!(f, "NaN")
                } else {
                    write!(f, "{val}")
                }
            }
            DurationExpr::Step => write!(f, "step()"),
            DurationExpr::Range => write!(f, "range()"),
            DurationExpr::Pos(e) => write!(f, "+{e}"),
            DurationExpr::Neg(e) => write!(f, "-{e}"),
            DurationExpr::Add(l, r) => write!(f, "{l} + {r}"),
            DurationExpr::Sub(l, r) => write!(f, "{l} - {r}"),
            DurationExpr::Mul(l, r) => write!(f, "{l} * {r}"),
            DurationExpr::Div(l, r) => write!(f, "{l} / {r}"),
            DurationExpr::Mod(l, r) => write!(f, "{l} % {r}"),
            DurationExpr::Pow(l, r) => write!(f, "{l} ^ {r}"),
            DurationExpr::MinOf(l, r) => write!(f, "min_of({l}, {r})"),
            DurationExpr::MaxOf(l, r) => write!(f, "max_of({l}, {r})"),
            DurationExpr::Wrapped(e) => write!(f, "({e})"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AtModifier {
    Start,
    End,
    /// at can be earlier than UNIX_EPOCH
    At(SystemTime),
}

impl fmt::Display for AtModifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AtModifier::Start => write!(f, "@ {}()", token_display(T_START)),
            AtModifier::End => write!(f, "@ {}()", token_display(T_END)),
            AtModifier::At(time) => {
                let d = time
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or(Duration::ZERO); // This should not happen
                write!(f, "@ {:.3}", d.as_secs_f64())
            }
        }
    }
}

impl TryFrom<TokenId> for AtModifier {
    type Error = String;

    fn try_from(id: TokenId) -> Result<Self, Self::Error> {
        match id {
            T_START => Ok(AtModifier::Start),
            T_END => Ok(AtModifier::End),
            _ => Err(format!(
                "invalid @ modifier preprocessor '{}', START or END is valid.",
                token::token_display(id)
            )),
        }
    }
}

impl TryFrom<Token> for AtModifier {
    type Error = String;

    fn try_from(token: Token) -> Result<Self, Self::Error> {
        AtModifier::try_from(token.id())
    }
}

impl TryFrom<NumberLiteral> for AtModifier {
    type Error = String;

    fn try_from(num: NumberLiteral) -> Result<Self, Self::Error> {
        AtModifier::try_from(num.val)
    }
}

impl TryFrom<Expr> for AtModifier {
    type Error = String;

    fn try_from(ex: Expr) -> Result<Self, Self::Error> {
        match ex {
            Expr::NumberLiteral(nl) => AtModifier::try_from(nl),
            _ => Err("invalid float value after @ modifier".into()),
        }
    }
}

impl TryFrom<f64> for AtModifier {
    type Error = String;

    fn try_from(secs: f64) -> Result<Self, Self::Error> {
        let err_info = format!("timestamp out of bounds for @ modifier: {secs}");

        if secs.is_nan() || secs.is_infinite() || secs >= f64::MAX || secs <= f64::MIN {
            return Err(err_info);
        }
        let milli = (secs * 1000f64).round().abs() as u64;

        let duration = Duration::from_millis(milli);
        let mut st = Some(SystemTime::UNIX_EPOCH);
        if secs.is_sign_positive() {
            st = SystemTime::UNIX_EPOCH.checked_add(duration);
        }
        if secs.is_sign_negative() {
            st = SystemTime::UNIX_EPOCH.checked_sub(duration);
        }

        st.map(Self::At).ok_or(err_info)
    }
}

/// EvalStmt holds an expression and information on the range it should
/// be evaluated on.
#[allow(rustdoc::broken_intra_doc_links)]
#[derive(Debug, Clone)]
pub struct EvalStmt {
    /// Expression to be evaluated.
    pub expr: Expr,

    /// The time boundaries for the evaluation. If start equals end an instant
    /// is evaluated.
    pub start: SystemTime,
    pub end: SystemTime,
    /// Time between two evaluated instants for the range [start:end].
    pub interval: Duration,
    /// Lookback delta to use for this evaluation.
    pub lookback_delta: Duration,
}

impl fmt::Display for EvalStmt {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "[{}] @ [{}, {}, {}, {}]",
            self.expr,
            DateTime::<Utc>::from(self.start).to_rfc3339(),
            DateTime::<Utc>::from(self.end).to_rfc3339(),
            display_duration(&self.interval),
            display_duration(&self.lookback_delta)
        )
    }
}

/// The byte offset of an expression node's first token in the query source
/// text — PulsusDB patch (see vendor/promql-parser/PATCHES.md, AST-metadata
/// patch class): the start half of Prometheus's `PositionRange`
/// (`promql/parser/posrange/posrange.go`), captured from the grmtools
/// `$span` in the producing grammar action. `None` means the node was
/// built by hand (test fixtures, `From` impls) rather than parsed.
///
/// **Position is metadata, not identity:** `PartialEq` compares equal
/// ALWAYS, so adding this field to an AST struct does not change that
/// struct's derived equality — hand-built-vs-parsed AST assertions
/// (this crate's and consumers') are unaffected.
#[derive(Clone, Copy, Default)]
pub struct AstPos(Option<usize>);

impl AstPos {
    /// A known start byte offset (parser-produced nodes).
    pub fn at(start: usize) -> Self {
        AstPos(Some(start))
    }

    /// The start byte offset, if the node came from the parser.
    pub fn start(&self) -> Option<usize> {
        self.0
    }
}

impl PartialEq for AstPos {
    /// Always true — see the type doc: position is metadata, not identity.
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for AstPos {}

impl fmt::Debug for AstPos {
    /// Compact single-line form (`AstPos(19)` / `AstPos(None)`) so
    /// `{:#?}` AST snapshots stay one line per node.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(start) => write!(f, "AstPos({start})"),
            None => write!(f, "AstPos(None)"),
        }
    }
}

/// Grammar:
/// ``` norust
/// <aggr-op> [without|by (<label list>)] ([parameter,] <vector expression>)
/// <aggr-op>([parameter,] <vector expression>) [without|by (<label list>)]
/// ```
///
/// parameter is only required for `count_values`, `quantile`, `topk` and `bottomk`.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "ser", derive(serde::Serialize))]
pub struct AggregateExpr {
    /// The used aggregation operation.
    pub op: TokenType,
    /// The Vector expression over which is aggregated.
    pub expr: Box<Expr>,
    /// Parameter used by some aggregators.
    pub param: Option<Box<Expr>>,
    /// modifier is optional for some aggregation operators, like sum.
    #[cfg_attr(feature = "ser", serde(flatten))]
    #[cfg_attr(feature = "ser", serde(serialize_with = "serialize_grouping"))]
    pub modifier: Option<LabelModifier>,
    /// Start byte offset of the aggregation op token (PulsusDB patch,
    /// PATCHES.md AST-metadata class).
    #[cfg_attr(feature = "ser", serde(skip))]
    pub pos: AstPos,
}

impl AggregateExpr {
    fn get_op_string(&self) -> String {
        let mut s = self.op.to_string();

        if let Some(modifier) = &self.modifier {
            // PulsusDB patch (docs/decisions/0003): always render the
            // `by (...)` clause, even when its label list is empty —
            // previously an *explicit* empty `by ()` collapsed to no
            // modifier at all on Display, so `parse -> Display -> parse`
            // turned `modifier: Some(Include([]))` into `modifier: None`,
            // an AST-shape round-trip failure on one of the M2 subset's own
            // constructs (aggregations with `by`/`without`). `without`
            // already rendered its empty form unconditionally; this makes
            // `by` symmetric with it — a leaf `Display` fix. See
            // vendor/promql-parser/PATCHES.md.
            match modifier {
                LabelModifier::Exclude(ls) => write!(s, " without ({ls}) ").unwrap(),
                LabelModifier::Include(ls) => write!(s, " by ({ls}) ").unwrap(),
            }
        }
        s
    }
}

impl fmt::Display for AggregateExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.get_op_string())?;

        write!(f, "(")?;
        if let Some(param) = &self.param {
            write!(f, "{param}, ")?;
        }
        write!(f, "{})", self.expr)?;

        Ok(())
    }
}

impl Prettier for AggregateExpr {
    fn format(&self, level: usize, max: usize) -> String {
        let mut s = format!("{}{}(\n", indent(level), self.get_op_string());
        if let Some(param) = &self.param {
            writeln!(s, "{},", param.pretty(level + 1, max)).unwrap();
        }
        writeln!(s, "{}", self.expr.pretty(level + 1, max)).unwrap();
        write!(s, "{})", indent(level)).unwrap();
        s
    }
}

/// UnaryExpr will negate the expr
#[derive(Debug, Clone, PartialEq)]
pub struct UnaryExpr {
    pub expr: Box<Expr>,
    /// Start byte offset of the sign token (PulsusDB patch, PATCHES.md
    /// AST-metadata class).
    pub pos: AstPos,
}

impl fmt::Display for UnaryExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "-{}", self.expr)
    }
}

impl Prettier for UnaryExpr {
    fn pretty(&self, level: usize, max: usize) -> String {
        format!(
            "{}-{}",
            indent(level),
            self.expr.pretty(level, max).trim_start()
        )
    }
}

#[cfg(feature = "ser")]
impl serde::Serialize for UnaryExpr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("op", "-")?;
        map.serialize_entry("expr", &self.expr)?;

        map.end()
    }
}

/// Grammar:
/// ``` norust
/// <vector expr> <bin-op> ignoring(<label list>) group_left(<label list>) <vector expr>
/// <vector expr> <bin-op> ignoring(<label list>) group_right(<label list>) <vector expr>
/// <vector expr> <bin-op> on(<label list>) group_left(<label list>) <vector expr>
/// <vector expr> <bin-op> on(<label list>) group_right(<label list>) <vector expr>
/// ```
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "ser", derive(serde::Serialize))]
pub struct BinaryExpr {
    /// The operation of the expression.
    pub op: TokenType,
    /// The operands on the left sides of the operator.
    pub lhs: Box<Expr>,
    /// The operands on the right sides of the operator.
    pub rhs: Box<Expr>,
    #[cfg_attr(feature = "ser", serde(flatten))]
    #[cfg_attr(feature = "ser", serde(serialize_with = "serialize_bin_modifier"))]
    pub modifier: Option<BinModifier>,
}

impl BinaryExpr {
    pub fn is_matching_on(&self) -> bool {
        matches!(&self.modifier, Some(modifier) if modifier.is_matching_on())
    }

    pub fn is_matching_labels_not_empty(&self) -> bool {
        matches!(&self.modifier, Some(modifier) if modifier.is_matching_labels_not_empty())
    }

    pub fn return_bool(&self) -> bool {
        matches!(&self.modifier, Some(modifier) if modifier.return_bool)
    }

    /// check if labels of card and matching are joint
    pub fn is_labels_joint(&self) -> bool {
        matches!(&self.modifier, Some(modifier) if modifier.is_labels_joint())
    }

    /// intersect labels of card and matching
    pub fn intersect_labels(&self) -> Option<Vec<String>> {
        self.modifier
            .as_ref()
            .and_then(|modifier| modifier.intersect_labels())
    }

    fn get_op_matching_string(&self) -> String {
        match &self.modifier {
            Some(modifier) => format!("{}{modifier}", self.op),
            None => self.op.to_string(),
        }
    }
}

impl fmt::Display for BinaryExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{} {} {}",
            self.lhs,
            self.get_op_matching_string(),
            self.rhs
        )
    }
}

impl Prettier for BinaryExpr {
    fn format(&self, level: usize, max: usize) -> String {
        format!(
            "{}\n{}{}\n{}",
            self.lhs.pretty(level + 1, max),
            indent(level),
            self.get_op_matching_string(),
            self.rhs.pretty(level + 1, max)
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "ser", derive(serde::Serialize))]
pub struct ParenExpr {
    pub expr: Box<Expr>,
    /// Start byte offset of the opening `(` (PulsusDB patch, PATCHES.md
    /// AST-metadata class).
    #[cfg_attr(feature = "ser", serde(skip))]
    pub pos: AstPos,
}

impl fmt::Display for ParenExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({})", self.expr)
    }
}

impl Prettier for ParenExpr {
    fn format(&self, level: usize, max: usize) -> String {
        format!(
            "{}(\n{}\n{})",
            indent(level),
            self.expr.pretty(level + 1, max),
            indent(level)
        )
    }
}

/// Grammar:
/// ```norust
/// <instant_query> '[' <range> ':' [<resolution>] ']' [ @ <float_literal> ] [ offset <duration> ]
/// ```
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "ser", derive(serde::Serialize))]
pub struct SubqueryExpr {
    pub expr: Box<Expr>,
    #[cfg_attr(feature = "ser", serde(serialize_with = "Offset::serialize_offset"))]
    pub offset: Option<Offset>,
    /// `Some` iff the `offset` was written as a non-literal duration
    /// expression (docs/decisions/0003 grammar patch G1); `offset` is then
    /// unset and the concrete value is resolved downstream.
    #[cfg_attr(feature = "ser", serde(skip))]
    pub offset_expr: Option<DurationExpr>,
    #[cfg_attr(feature = "ser", serde(flatten))]
    #[cfg_attr(feature = "ser", serde(serialize_with = "serialize_at_modifier"))]
    pub at: Option<AtModifier>,
    #[cfg_attr(
        feature = "ser",
        serde(serialize_with = "crate::util::duration::serialize_duration")
    )]
    pub range: Duration,
    /// `Some` iff the range was written as a non-literal duration
    /// expression; `range` is then `Duration::ZERO` (upstream's dual
    /// `Range`+`RangeExpr` model).
    #[cfg_attr(feature = "ser", serde(skip))]
    pub range_expr: Option<DurationExpr>,
    /// Default is the global evaluation interval.
    #[cfg_attr(
        feature = "ser",
        serde(serialize_with = "crate::util::duration::serialize_duration_opt")
    )]
    pub step: Option<Duration>,
    /// `Some` iff the step was written as a non-literal duration
    /// expression; `step` is then `None`.
    #[cfg_attr(feature = "ser", serde(skip))]
    pub step_expr: Option<DurationExpr>,
}

impl SubqueryExpr {
    fn get_time_suffix_string(&self) -> String {
        let step = match (&self.step_expr, &self.step) {
            (Some(e), _) => e.to_string(),
            (None, Some(step)) => display_duration(step),
            (None, None) => String::from(""),
        };
        let range = match &self.range_expr {
            Some(e) => e.to_string(),
            None => display_duration(&self.range),
        };

        let mut s = format!("[{range}:{step}]");

        if let Some(at) = &self.at {
            write!(s, " {at}").unwrap();
        }

        if let Some(offset) = &self.offset {
            write!(s, " offset {offset}").unwrap();
        }
        if let Some(e) = &self.offset_expr {
            write!(s, " offset {e}").unwrap();
        }
        s
    }
}

impl fmt::Display for SubqueryExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", self.expr, self.get_time_suffix_string())
    }
}

impl Prettier for SubqueryExpr {
    fn pretty(&self, level: usize, max: usize) -> String {
        format!(
            "{}{}",
            self.expr.pretty(level, max),
            self.get_time_suffix_string()
        )
    }
}

#[derive(Debug, Clone)]
pub struct NumberLiteral {
    pub val: f64,
    /// Start byte offset of the literal (or its collapsed sign — a
    /// parsed `-1` starts at the `-`; PulsusDB patch, PATCHES.md
    /// AST-metadata class).
    pub pos: AstPos,
}

impl NumberLiteral {
    pub fn new(val: f64) -> Self {
        Self {
            val,
            pos: AstPos::default(),
        }
    }
}

impl PartialEq for NumberLiteral {
    fn eq(&self, other: &Self) -> bool {
        self.val == other.val || self.val.is_nan() && other.val.is_nan()
    }
}

impl Eq for NumberLiteral {}

impl Neg for NumberLiteral {
    type Output = Self;

    fn neg(self) -> Self::Output {
        NumberLiteral {
            val: -self.val,
            pos: self.pos,
        }
    }
}

impl fmt::Display for NumberLiteral {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.val == f64::INFINITY {
            write!(f, "Inf")
        } else if self.val == f64::NEG_INFINITY {
            write!(f, "-Inf")
        } else if f64::is_nan(self.val) {
            write!(f, "NaN")
        } else {
            write!(f, "{}", self.val)
        }
    }
}

#[cfg(feature = "ser")]
impl serde::Serialize for NumberLiteral {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry("val", &self.to_string())?;

        map.end()
    }
}

impl Prettier for NumberLiteral {
    fn needs_split(&self, _max: usize) -> bool {
        false
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "ser", derive(serde::Serialize))]
pub struct StringLiteral {
    pub val: String,
    /// Start byte offset of the opening quote (PulsusDB patch, PATCHES.md
    /// AST-metadata class).
    #[cfg_attr(feature = "ser", serde(skip))]
    pub pos: AstPos,
}

impl fmt::Display for StringLiteral {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "\"{}\"", escape_string(&self.val))
    }
}

impl Prettier for StringLiteral {
    fn needs_split(&self, _max: usize) -> bool {
        false
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "ser", derive(serde::Serialize))]
pub struct VectorSelector {
    pub name: Option<String>,
    #[cfg_attr(feature = "ser", serde(flatten))]
    pub matchers: Matchers,
    #[cfg_attr(feature = "ser", serde(serialize_with = "Offset::serialize_offset"))]
    pub offset: Option<Offset>,
    /// `Some` iff the `offset` was written as a non-literal duration
    /// expression (docs/decisions/0003 grammar patch G1); `offset` is then
    /// unset and the concrete value is resolved downstream.
    #[cfg_attr(feature = "ser", serde(skip))]
    pub offset_expr: Option<DurationExpr>,
    #[cfg_attr(feature = "ser", serde(flatten))]
    #[cfg_attr(feature = "ser", serde(serialize_with = "serialize_at_modifier"))]
    pub at: Option<AtModifier>,
    /// `true` iff the selector carried the `anchored` extended-range
    /// modifier (docs/decisions/0003 grammar patch G3). Skipped from the
    /// `ser` JSON shape (the experimental gate lives at plan time).
    #[cfg_attr(feature = "ser", serde(skip))]
    pub anchored: bool,
    /// `true` iff the selector carried the `smoothed` extended-range
    /// modifier (docs/decisions/0003 grammar patch G3). Skipped from the
    /// `ser` JSON shape (the experimental gate lives at plan time).
    #[cfg_attr(feature = "ser", serde(skip))]
    pub smoothed: bool,
    /// Start byte offset of the selector's first token (PulsusDB patch,
    /// PATCHES.md AST-metadata class).
    #[cfg_attr(feature = "ser", serde(skip))]
    pub pos: AstPos,
}

impl VectorSelector {
    pub fn new(name: Option<String>, matchers: Matchers) -> Self {
        VectorSelector {
            name,
            matchers,
            offset: None,
            offset_expr: None,
            at: None,
            anchored: false,
            smoothed: false,
            pos: AstPos::default(),
        }
    }
}

impl Default for VectorSelector {
    fn default() -> Self {
        Self {
            name: None,
            matchers: Matchers::empty(),
            offset: None,
            offset_expr: None,
            at: None,
            anchored: false,
            smoothed: false,
            pos: AstPos::default(),
        }
    }
}

impl From<String> for VectorSelector {
    fn from(name: String) -> Self {
        VectorSelector {
            name: Some(name),
            offset: None,
            offset_expr: None,
            at: None,
            anchored: false,
            smoothed: false,
            matchers: Matchers::empty(),
            pos: AstPos::default(),
        }
    }
}

/// directly create an instant vector with only METRIC_NAME matcher.
///
/// # Examples
///
/// Basic usage:
///
/// ``` rust
/// use promql_parser::label::Matchers;
/// use promql_parser::parser::VectorSelector;
///
/// let vs = VectorSelector {
///     name: Some(String::from("foo")),
///     offset: None,
///     offset_expr: None,
///     at: None,
///     anchored: false,
///     smoothed: false,
///     matchers: Matchers::empty(),
///     pos: promql_parser::parser::ast::AstPos::default(),
/// };
///
/// assert_eq!(VectorSelector::from("foo"), vs);
/// ```
impl From<&str> for VectorSelector {
    fn from(name: &str) -> Self {
        VectorSelector::from(name.to_string())
    }
}

impl Neg for VectorSelector {
    type Output = UnaryExpr;

    fn neg(self) -> Self::Output {
        let ex = Expr::VectorSelector(self);
        UnaryExpr {
            expr: Box::new(ex),
            pos: AstPos::default(),
        }
    }
}

impl fmt::Display for VectorSelector {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(name) = &self.name {
            write!(f, "{name}")?;
        }
        let matchers = &self.matchers.to_string();
        if !matchers.is_empty() {
            write!(f, "{{{matchers}}}")?;
        }
        if let Some(at) = &self.at {
            write!(f, " {at}")?;
        }
        // Grammar patch G3: printer.go:413-418 emits the modifier after the
        // @ modifier and before offset (anchored takes precedence).
        if self.anchored {
            write!(f, " anchored")?;
        } else if self.smoothed {
            write!(f, " smoothed")?;
        }
        if let Some(offset) = &self.offset {
            write!(f, " offset {offset}")?;
        }
        if let Some(e) = &self.offset_expr {
            write!(f, " offset {e}")?;
        }
        Ok(())
    }
}

impl Prettier for VectorSelector {
    fn needs_split(&self, _max: usize) -> bool {
        false
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "ser", derive(serde::Serialize))]
pub struct MatrixSelector {
    #[cfg_attr(feature = "ser", serde(flatten))]
    pub vs: VectorSelector,
    #[cfg_attr(
        feature = "ser",
        serde(serialize_with = "crate::util::duration::serialize_duration")
    )]
    pub range: Duration,
    /// `Some` iff the range was written as a non-literal duration
    /// expression (docs/decisions/0003 grammar patch G1); `range` is then
    /// `Duration::ZERO` (upstream's dual `Range`+`RangeExpr` model).
    #[cfg_attr(feature = "ser", serde(skip))]
    pub range_expr: Option<DurationExpr>,
}

impl fmt::Display for MatrixSelector {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(name) = &self.vs.name {
            write!(f, "{name}")?;
        }

        let matchers = &self.vs.matchers.to_string();
        if !matchers.is_empty() {
            write!(f, "{{{matchers}}}")?;
        }

        match &self.range_expr {
            Some(e) => write!(f, "[{e}]")?,
            None => write!(f, "[{}]", display_duration(&self.range))?,
        }

        // Grammar patch G3: printer.go:280-305 emits the modifier after the
        // range and before the @ modifier (anchored takes precedence).
        if self.vs.anchored {
            write!(f, " anchored")?;
        } else if self.vs.smoothed {
            write!(f, " smoothed")?;
        }

        if let Some(at) = &self.vs.at {
            write!(f, " {at}")?;
        }

        if let Some(offset) = &self.vs.offset {
            write!(f, " offset {offset}")?;
        }

        if let Some(e) = &self.vs.offset_expr {
            write!(f, " offset {e}")?;
        }

        Ok(())
    }
}

impl Prettier for MatrixSelector {
    fn needs_split(&self, _max: usize) -> bool {
        false
    }
}

/// Call represents Prometheus Function.
/// Some functions have special cases:
///
/// ## exp
///
/// exp(v instant-vector) calculates the exponential function for all elements in v.
/// Special cases are:
///
/// ```promql
/// Exp(+Inf) = +Inf
/// Exp(NaN) = NaN
/// ```
///
/// ## ln
///
/// ln(v instant-vector) calculates the natural logarithm for all elements in v.
/// Special cases are:
///
/// ```promql
/// ln(+Inf) = +Inf
/// ln(0) = -Inf
/// ln(x < 0) = NaN
/// ln(NaN) = NaN
/// ```
///
/// TODO: support more special cases of function call
///
///  - acos()
///  - acosh()
///  - asin()
///  - asinh()
///  - atan()
///  - atanh()
///  - cos()
///  - cosh()
///  - sin()
///  - sinh()
///  - tan()
///  - tanh()
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "ser", derive(serde::Serialize))]
pub struct Call {
    pub func: Function,
    #[cfg_attr(feature = "ser", serde(flatten))]
    pub args: FunctionArgs,
    /// Start byte offset of the function-name token (PulsusDB patch,
    /// PATCHES.md AST-metadata class).
    #[cfg_attr(feature = "ser", serde(skip))]
    pub pos: AstPos,
}

impl fmt::Display for Call {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}({})", self.func.name, self.args)
    }
}

impl Prettier for Call {
    fn format(&self, level: usize, max: usize) -> String {
        format!(
            "{}{}(\n{}\n{})",
            indent(level),
            self.func.name,
            self.args.pretty(level + 1, max),
            indent(level)
        )
    }
}

/// Node for extending the AST. [Extension] won't be generate by this parser itself.
#[derive(Debug, Clone)]
pub struct Extension {
    pub expr: Arc<dyn ExtensionExpr>,
}

/// The interface for extending the AST with custom expression node.
pub trait ExtensionExpr: std::fmt::Debug + Send + Sync {
    fn as_any(&self) -> &dyn std::any::Any;

    fn name(&self) -> &str;

    fn value_type(&self) -> ValueType;

    fn children(&self) -> &[Expr];

    fn with_new_children(&self, children: Vec<Expr>) -> Arc<dyn ExtensionExpr>;
}

impl PartialEq for Extension {
    fn eq(&self, other: &Self) -> bool {
        format!("{self:?}") == format!("{other:?}")
    }
}

impl Eq for Extension {}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "ser", derive(serde::Serialize))]
#[cfg_attr(feature = "ser", serde(tag = "type", rename_all = "camelCase"))]
pub enum Expr {
    /// Aggregate represents an aggregation operation on a Vector.
    #[cfg_attr(feature = "ser", serde(rename = "aggregation"))]
    Aggregate(AggregateExpr),

    /// Unary represents a unary operation on another expression.
    /// Currently unary operations are only supported for Scalars.
    #[cfg_attr(feature = "ser", serde(rename = "unaryExpr"))]
    Unary(UnaryExpr),

    /// Binary represents a binary expression between two child expressions.
    #[cfg_attr(feature = "ser", serde(rename = "binaryExpr"))]
    Binary(BinaryExpr),

    /// Paren wraps an expression so it cannot be disassembled as a consequence
    /// of operator precedence.
    #[cfg_attr(feature = "ser", serde(rename = "parenExpr"))]
    Paren(ParenExpr),

    /// SubqueryExpr represents a subquery.
    Subquery(SubqueryExpr),

    /// NumberLiteral represents a number.
    NumberLiteral(NumberLiteral),

    /// StringLiteral represents a string.
    StringLiteral(StringLiteral),

    /// VectorSelector represents a Vector selection.
    VectorSelector(VectorSelector),

    /// MatrixSelector represents a Matrix selection.
    MatrixSelector(MatrixSelector),

    /// Call represents a function call.
    Call(Call),

    /// Extension represents an extension expression. It is for user to attach additional
    /// information to the AST. This parser won't generate Extension node.
    #[cfg_attr(feature = "ser", serde(skip))]
    Extension(Extension),
}

impl Expr {
    pub(crate) fn new_vector_selector(
        name: Option<String>,
        matchers: Matchers,
    ) -> Result<Self, String> {
        let vs = VectorSelector::new(name, matchers);
        Ok(Self::VectorSelector(vs))
    }

    pub(crate) fn new_unary_expr(expr: Expr) -> Result<Self, String> {
        match expr {
            Expr::StringLiteral(_) => Err("unary expression only allowed on expressions of type scalar or vector, got: string".into()),
            Expr::MatrixSelector(_) => Err("unary expression only allowed on expressions of type scalar or vector, got: matrix".into()),
            _ => Ok(-expr),
        }
    }

    pub(crate) fn new_subquery_expr(
        expr: Expr,
        range: Duration,
        range_expr: Option<DurationExpr>,
        step: Option<Duration>,
        step_expr: Option<DurationExpr>,
    ) -> Result<Self, String> {
        let se = Expr::Subquery(SubqueryExpr {
            expr: Box::new(expr),
            offset: None,
            offset_expr: None,
            at: None,
            range,
            range_expr,
            step,
            step_expr,
        });
        Ok(se)
    }

    pub(crate) fn new_paren_expr(expr: Expr) -> Result<Self, String> {
        let ex = Expr::Paren(ParenExpr {
            expr: Box::new(expr),
            pos: AstPos::default(),
        });
        Ok(ex)
    }

    /// NOTE: @ and offset is not set here.
    pub(crate) fn new_matrix_selector(
        expr: Expr,
        range: Duration,
        range_expr: Option<DurationExpr>,
    ) -> Result<Self, String> {
        match expr {
            Expr::VectorSelector(VectorSelector {
                offset: Some(_), ..
            })
            | Expr::VectorSelector(VectorSelector {
                offset_expr: Some(_),
                ..
            }) => Err("no offset modifiers allowed before range".into()),
            Expr::VectorSelector(VectorSelector { at: Some(_), .. }) => {
                Err("no @ modifiers allowed before range".into())
            }
            Expr::VectorSelector(vs) => {
                let ms = Expr::MatrixSelector(MatrixSelector {
                    vs,
                    range,
                    range_expr,
                });
                Ok(ms)
            }
            _ => Err("ranges only allowed for vector selectors".into()),
        }
    }

    pub(crate) fn at_expr(self, at: AtModifier) -> Result<Self, String> {
        let already_set_err = Err("@ <timestamp> may not be set multiple times".into());
        match self {
            Expr::VectorSelector(mut vs) => match vs.at {
                None => {
                    vs.at = Some(at);
                    Ok(Expr::VectorSelector(vs))
                }
                Some(_) => already_set_err,
            },
            Expr::MatrixSelector(mut ms) => match ms.vs.at {
                None => {
                    ms.vs.at = Some(at);
                    Ok(Expr::MatrixSelector(ms))
                }
                Some(_) => already_set_err,
            },
            Expr::Subquery(mut s) => match s.at {
                None => {
                    s.at = Some(at);
                    Ok(Expr::Subquery(s))
                }
                Some(_) => already_set_err,
            },
            _ => {
                Err("@ modifier must be preceded by an vector selector or matrix selector or a subquery".into())
            }
        }
    }

    /// set offset field for specified Expr, but CAN ONLY be set once.
    pub(crate) fn offset_expr(self, offset: Offset) -> Result<Self, String> {
        let already_set_err = Err("offset may not be set multiple times".into());
        match self {
            Expr::VectorSelector(mut vs) => match (&vs.offset, &vs.offset_expr) {
                (None, None) => {
                    vs.offset = Some(offset);
                    Ok(Expr::VectorSelector(vs))
                }
                _ => already_set_err,
            },
            Expr::MatrixSelector(mut ms) => match (&ms.vs.offset, &ms.vs.offset_expr) {
                (None, None) => {
                    ms.vs.offset = Some(offset);
                    Ok(Expr::MatrixSelector(ms))
                }
                _ => already_set_err,
            },
            Expr::Subquery(mut s) => match (&s.offset, &s.offset_expr) {
                (None, None) => {
                    s.offset = Some(offset);
                    Ok(Expr::Subquery(s))
                }
                _ => already_set_err,
            },
            _ => {
                Err("offset modifier must be preceded by an vector selector or matrix selector or a subquery".into())
            }
        }
    }

    /// set the offset *duration-expression* field for specified Expr
    /// (docs/decisions/0003 grammar patch G1 — upstream's
    /// `addOffsetExpr`), same set-once rule as [`Expr::offset_expr`].
    pub(crate) fn offset_dur_expr(self, offset_expr: DurationExpr) -> Result<Self, String> {
        let already_set_err = Err("offset may not be set multiple times".into());
        match self {
            Expr::VectorSelector(mut vs) => match (&vs.offset, &vs.offset_expr) {
                (None, None) => {
                    vs.offset_expr = Some(offset_expr);
                    Ok(Expr::VectorSelector(vs))
                }
                _ => already_set_err,
            },
            Expr::MatrixSelector(mut ms) => match (&ms.vs.offset, &ms.vs.offset_expr) {
                (None, None) => {
                    ms.vs.offset_expr = Some(offset_expr);
                    Ok(Expr::MatrixSelector(ms))
                }
                _ => already_set_err,
            },
            Expr::Subquery(mut s) => match (&s.offset, &s.offset_expr) {
                (None, None) => {
                    s.offset_expr = Some(offset_expr);
                    Ok(Expr::Subquery(s))
                }
                _ => already_set_err,
            },
            _ => {
                Err("offset modifier must be preceded by an vector selector or matrix selector or a subquery".into())
            }
        }
    }

    /// set the `anchored` extended-range modifier (docs/decisions/0003
    /// grammar patch G3 — upstream's `setAnchored`, parse.go:1078-1102,
    /// minus the experimental gate which lives at plan time).
    pub(crate) fn set_anchored(self) -> Result<Self, String> {
        let mutual_excl_err = "anchored and smoothed modifiers cannot be used together";
        match self {
            Expr::VectorSelector(mut vs) => {
                vs.anchored = true;
                if vs.smoothed {
                    return Err(mutual_excl_err.into());
                }
                Ok(Expr::VectorSelector(vs))
            }
            Expr::MatrixSelector(mut ms) => {
                ms.vs.anchored = true;
                if ms.vs.smoothed {
                    return Err(mutual_excl_err.into());
                }
                Ok(Expr::MatrixSelector(ms))
            }
            Expr::Subquery(_) => Err("anchored modifier is not supported for subqueries".into()),
            _ => Err("anchored modifier not implemented".into()),
        }
    }

    /// set the `smoothed` extended-range modifier (docs/decisions/0003
    /// grammar patch G3 — upstream's `setSmoothed`, parse.go:1105-1129,
    /// minus the experimental gate which lives at plan time).
    pub(crate) fn set_smoothed(self) -> Result<Self, String> {
        let mutual_excl_err = "anchored and smoothed modifiers cannot be used together";
        match self {
            Expr::VectorSelector(mut vs) => {
                vs.smoothed = true;
                if vs.anchored {
                    return Err(mutual_excl_err.into());
                }
                Ok(Expr::VectorSelector(vs))
            }
            Expr::MatrixSelector(mut ms) => {
                ms.vs.smoothed = true;
                if ms.vs.anchored {
                    return Err(mutual_excl_err.into());
                }
                Ok(Expr::MatrixSelector(ms))
            }
            Expr::Subquery(_) => Err("smoothed modifier is not supported for subqueries".into()),
            _ => Err("smoothed modifier not implemented".into()),
        }
    }

    pub(crate) fn new_call(func: Function, args: FunctionArgs) -> Result<Expr, String> {
        Ok(Expr::Call(Call {
            func,
            args,
            pos: AstPos::default(),
        }))
    }

    pub(crate) fn new_binary_expr(
        lhs: Expr,
        op: TokenId,
        modifier: Option<BinModifier>,
        rhs: Expr,
    ) -> Result<Expr, String> {
        let ex = BinaryExpr {
            op: TokenType::new(op),
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
            modifier,
        };
        Ok(Expr::Binary(ex))
    }

    pub(crate) fn new_aggregate_expr(
        op: TokenId,
        modifier: Option<LabelModifier>,
        args: FunctionArgs,
    ) -> Result<Expr, String> {
        let op = TokenType::new(op);
        if args.is_empty() {
            return Err(format!(
                "no arguments for aggregate expression '{op}' provided"
            ));
        }
        let mut desired_args_count = 1;
        let mut param = None;
        if op.is_aggregator_with_param() {
            desired_args_count = 2;
            param = args.first();
        }
        if args.len() != desired_args_count {
            return Err(format!(
                "wrong number of arguments for aggregate expression provided, expected {}, got {}",
                desired_args_count,
                args.len()
            ));
        }

        match args.last() {
            Some(expr) => Ok(Expr::Aggregate(AggregateExpr {
                op,
                expr,
                param,
                modifier,
                pos: AstPos::default(),
            })),
            None => Err(
                "aggregate operation needs a single instant vector parameter, but found none"
                    .into(),
            ),
        }
    }

    pub fn value_type(&self) -> ValueType {
        match self {
            Expr::Aggregate(_) => ValueType::Vector,
            Expr::Unary(ex) => ex.expr.value_type(),
            Expr::Binary(ex) => {
                if ex.lhs.value_type() == ValueType::Scalar
                    && ex.rhs.value_type() == ValueType::Scalar
                {
                    ValueType::Scalar
                } else {
                    ValueType::Vector
                }
            }
            Expr::Paren(ex) => ex.expr.value_type(),
            Expr::Subquery(_) => ValueType::Matrix,
            Expr::NumberLiteral(_) => ValueType::Scalar,
            Expr::StringLiteral(_) => ValueType::String,
            Expr::VectorSelector(_) => ValueType::Vector,
            Expr::MatrixSelector(_) => ValueType::Matrix,
            Expr::Call(ex) => ex.func.return_type,
            Expr::Extension(ex) => ex.expr.value_type(),
        }
    }

    /// only Some if expr is [Expr::NumberLiteral]
    pub(crate) fn scalar_value(&self) -> Option<f64> {
        match self {
            Expr::NumberLiteral(nl) => Some(nl.val),
            _ => None,
        }
    }

    /// The start byte offset of this expression in the query source text
    /// (PulsusDB patch, PATCHES.md AST-metadata class) — the start half
    /// of upstream Prometheus's `Expr.PositionRange()` (`promql/parser/
    /// ast.go`). Seven node kinds carry their own captured offset; the
    /// three wrapper kinds whose range starts at their inner expression
    /// recurse (`Binary` starts at its LHS, `MatrixSelector` at its
    /// vector selector, `Subquery` at its inner expression — upstream's
    /// own `PositionRange()` shapes). `None` for hand-built nodes and
    /// `Extension`.
    pub fn pos_start(&self) -> Option<usize> {
        match self {
            Expr::Aggregate(ex) => ex.pos.start(),
            Expr::Unary(ex) => ex.pos.start(),
            Expr::Binary(ex) => ex.lhs.pos_start(),
            Expr::Paren(ex) => ex.pos.start(),
            Expr::Subquery(ex) => ex.expr.pos_start(),
            Expr::NumberLiteral(ex) => ex.pos.start(),
            Expr::StringLiteral(ex) => ex.pos.start(),
            Expr::VectorSelector(ex) => ex.pos.start(),
            Expr::MatrixSelector(ex) => ex.vs.pos.start(),
            Expr::Call(ex) => ex.pos.start(),
            Expr::Extension(_) => None,
        }
    }

    /// Sets the start byte offset on the node whose position field
    /// defines this expression's start (the inverse of
    /// [`Expr::pos_start`]'s dispatch) — called by the producing grammar
    /// actions with `$span.start()`. For the recursive kinds this
    /// overwrites the start-defining descendant's own offset, which is
    /// exactly upstream's sign-collapse behaviour (`-1` is a
    /// `NumberLiteral` whose range starts at the `-`).
    pub(crate) fn with_pos_start(self, start: usize) -> Expr {
        match self {
            Expr::Aggregate(mut ex) => {
                ex.pos = AstPos::at(start);
                Expr::Aggregate(ex)
            }
            Expr::Unary(mut ex) => {
                ex.pos = AstPos::at(start);
                Expr::Unary(ex)
            }
            Expr::Binary(mut ex) => {
                ex.lhs = Box::new(ex.lhs.with_pos_start(start));
                Expr::Binary(ex)
            }
            Expr::Paren(mut ex) => {
                ex.pos = AstPos::at(start);
                Expr::Paren(ex)
            }
            Expr::Subquery(mut ex) => {
                ex.expr = Box::new(ex.expr.with_pos_start(start));
                Expr::Subquery(ex)
            }
            Expr::NumberLiteral(mut ex) => {
                ex.pos = AstPos::at(start);
                Expr::NumberLiteral(ex)
            }
            Expr::StringLiteral(mut ex) => {
                ex.pos = AstPos::at(start);
                Expr::StringLiteral(ex)
            }
            Expr::VectorSelector(mut ex) => {
                ex.pos = AstPos::at(start);
                Expr::VectorSelector(ex)
            }
            Expr::MatrixSelector(mut ex) => {
                ex.vs.pos = AstPos::at(start);
                Expr::MatrixSelector(ex)
            }
            Expr::Call(mut ex) => {
                ex.pos = AstPos::at(start);
                Expr::Call(ex)
            }
            ex @ Expr::Extension(_) => ex,
        }
    }

    pub fn prettify(&self) -> String {
        self.pretty(0, MAX_CHARACTERS_PER_LINE)
    }
}

impl From<String> for Expr {
    fn from(val: String) -> Self {
        Expr::StringLiteral(StringLiteral {
            val,
            pos: AstPos::default(),
        })
    }
}

impl From<&str> for Expr {
    fn from(s: &str) -> Self {
        Expr::StringLiteral(StringLiteral {
            val: s.into(),
            pos: AstPos::default(),
        })
    }
}

impl From<f64> for Expr {
    fn from(val: f64) -> Self {
        Expr::NumberLiteral(NumberLiteral::new(val))
    }
}

/// directly create an Expr::VectorSelector from instant vector
///
/// # Examples
///
/// Basic usage:
///
/// ``` rust
/// use promql_parser::label::Matchers;
/// use promql_parser::parser::{Expr, VectorSelector};
///
/// let name = String::from("foo");
/// let vs = VectorSelector::new(Some(name), Matchers::empty());
///
/// assert_eq!(Expr::VectorSelector(vs), Expr::from(VectorSelector::from("foo")));
/// ```
impl From<VectorSelector> for Expr {
    fn from(vs: VectorSelector) -> Self {
        Expr::VectorSelector(vs)
    }
}

impl Neg for Expr {
    type Output = Self;

    fn neg(self) -> Self::Output {
        match self {
            Expr::NumberLiteral(nl) => Expr::NumberLiteral(-nl),
            _ => Expr::Unary(UnaryExpr {
                expr: Box::new(self),
                pos: AstPos::default(),
            }),
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Expr::Aggregate(ex) => write!(f, "{ex}"),
            Expr::Unary(ex) => write!(f, "{ex}"),
            Expr::Binary(ex) => write!(f, "{ex}"),
            Expr::Paren(ex) => write!(f, "{ex}"),
            Expr::Subquery(ex) => write!(f, "{ex}"),
            Expr::NumberLiteral(ex) => write!(f, "{ex}"),
            Expr::StringLiteral(ex) => write!(f, "{ex}"),
            Expr::VectorSelector(ex) => write!(f, "{ex}"),
            Expr::MatrixSelector(ex) => write!(f, "{ex}"),
            Expr::Call(ex) => write!(f, "{ex}"),
            Expr::Extension(ext) => write!(f, "{ext:?}"),
        }
    }
}

impl Prettier for Expr {
    fn pretty(&self, level: usize, max: usize) -> String {
        match self {
            Expr::Aggregate(ex) => ex.pretty(level, max),
            Expr::Unary(ex) => ex.pretty(level, max),
            Expr::Binary(ex) => ex.pretty(level, max),
            Expr::Paren(ex) => ex.pretty(level, max),
            Expr::Subquery(ex) => ex.pretty(level, max),
            Expr::NumberLiteral(ex) => ex.pretty(level, max),
            Expr::StringLiteral(ex) => ex.pretty(level, max),
            Expr::VectorSelector(ex) => ex.pretty(level, max),
            Expr::MatrixSelector(ex) => ex.pretty(level, max),
            Expr::Call(ex) => ex.pretty(level, max),
            Expr::Extension(ext) => format!("{ext:?}"),
        }
    }
}

/// check_ast checks the validity of the provided AST. This includes type checking.
/// Recursively check correct typing for child nodes and raise errors in case of bad typing.
pub(crate) fn check_ast(expr: Expr) -> Result<Expr, String> {
    match expr {
        Expr::Binary(ex) => check_ast_for_binary_expr(ex),
        Expr::Aggregate(ex) => check_ast_for_aggregate_expr(ex),
        Expr::Call(ex) => check_ast_for_call(ex),
        Expr::Unary(ex) => check_ast_for_unary(ex),
        Expr::Subquery(ex) => check_ast_for_subquery(ex),
        Expr::VectorSelector(ex) => check_ast_for_vector_selector(ex),
        // PATCHES.md #6 (issue #82 v5): the eager empty-matcher operand
        // guard, so a paren-wrapped bare `{}` cannot smuggle past the
        // reductions below it (`(({}))`, etc.) into an accepting context.
        Expr::Paren(p) => {
            reject_empty_operand(&p.expr)?;
            Ok(Expr::Paren(p))
        }
        Expr::NumberLiteral(_) => Ok(expr),
        Expr::StringLiteral(_) => Ok(expr),
        // PATCHES.md #6 (issue #82 v5): a `MatrixSelector` is the one
        // depth-adding reduction whose empty-matcher leaf never routes
        // through the plain `expr: vector_selector` production (it is
        // built from `expr LEFT_BRACKET duration RIGHT_BRACKET`, so its
        // inner selector's own `check_ast` pass already ran — this arm
        // catches a range-wrapped bare selector, e.g. `rate({}[5m])`).
        Expr::MatrixSelector(ex) => check_ast_for_matrix_selector(ex),
        Expr::Extension(_) => Ok(expr),
    }
}

fn expect_type(
    expected: ValueType,
    actual: Option<ValueType>,
    context: &str,
) -> Result<bool, String> {
    match actual {
        Some(actual) => {
            if actual == expected {
                Ok(true)
            } else {
                Err(format!(
                    "expected type {expected} in {context}, got {actual}"
                ))
            }
        }
        None => Err(format!("expected type {expected} in {context}, got None")),
    }
}

/// the original logic is redundant in prometheus, and the following coding blocks
/// have been optimized for readability, but all logic SHOULD be covered.
fn check_ast_for_binary_expr(mut ex: BinaryExpr) -> Result<Expr, String> {
    // PATCHES.md #6 (issue #82 v5): the eager empty-matcher operand
    // guard on both sides.
    reject_empty_operand(&ex.lhs)?;
    reject_empty_operand(&ex.rhs)?;
    if !ex.op.is_operator() {
        return Err(format!(
            "binary expression does not support operator '{}'",
            ex.op
        ));
    }

    if ex.return_bool() && !ex.op.is_comparison_operator() {
        return Err("bool modifier can only be used on comparison operators".into());
    }

    if ex.op.is_comparison_operator()
        && ex.lhs.value_type() == ValueType::Scalar
        && ex.rhs.value_type() == ValueType::Scalar
        && !ex.return_bool()
    {
        return Err("comparisons between scalars must use BOOL modifier".into());
    }

    // For `on` matching, a label can only appear in one of the lists.
    // Every time series of the result vector must be uniquely identifiable.
    if ex.is_matching_on() && ex.is_labels_joint() {
        if let Some(labels) = ex.intersect_labels() {
            if let Some(label) = labels.first() {
                return Err(format!(
                    "label '{label}' must not occur in ON and GROUP clause at once"
                ));
            }
        };
    }

    if ex.op.is_set_operator() {
        if ex.lhs.value_type() == ValueType::Scalar || ex.rhs.value_type() == ValueType::Scalar {
            return Err(format!(
                "set operator '{}' not allowed in binary scalar expression",
                ex.op
            ));
        }

        if ex.lhs.value_type() == ValueType::Vector && ex.rhs.value_type() == ValueType::Vector {
            if let Some(ref modifier) = ex.modifier {
                if matches!(modifier.card, VectorMatchCardinality::OneToMany(_))
                    || matches!(modifier.card, VectorMatchCardinality::ManyToOne(_))
                {
                    return Err(format!("no grouping allowed for '{}' operation", ex.op));
                }
            };
        }

        match &mut ex.modifier {
            Some(modifier) => {
                if modifier.card == VectorMatchCardinality::OneToOne {
                    modifier.card = VectorMatchCardinality::ManyToMany;
                }
            }
            None => {
                ex.modifier =
                    Some(BinModifier::default().with_card(VectorMatchCardinality::ManyToMany));
            }
        }
    }

    if ex.lhs.value_type() != ValueType::Scalar && ex.lhs.value_type() != ValueType::Vector {
        return Err("binary expression must contain only scalar and instant vector types".into());
    }
    if ex.rhs.value_type() != ValueType::Scalar && ex.rhs.value_type() != ValueType::Vector {
        return Err("binary expression must contain only scalar and instant vector types".into());
    }

    if (ex.lhs.value_type() != ValueType::Vector || ex.rhs.value_type() != ValueType::Vector)
        && ex.is_matching_labels_not_empty()
    {
        return Err("vector matching only allowed between vectors".into());
    }

    Ok(Expr::Binary(ex))
}

fn check_ast_for_aggregate_expr(ex: AggregateExpr) -> Result<Expr, String> {
    // PATCHES.md #6 (issue #82 v5): the eager empty-matcher operand
    // guard on the aggregated expression and its optional param.
    reject_empty_operand(&ex.expr)?;
    if let Some(param) = &ex.param {
        reject_empty_operand(param)?;
    }
    if !ex.op.is_aggregator() {
        return Err(format!(
            "aggregation operator expected in aggregation expression but got '{}'",
            ex.op
        ));
    }

    expect_type(
        ValueType::Vector,
        Some(ex.expr.value_type()),
        "aggregation expression",
    )?;

    if matches!(ex.op.id(), T_TOPK | T_BOTTOMK | T_QUANTILE) {
        expect_type(
            ValueType::Scalar,
            ex.param.as_ref().map(|ex| ex.value_type()),
            "aggregation expression",
        )?;
    }

    if ex.op.id() == T_COUNT_VALUES {
        expect_type(
            ValueType::String,
            ex.param.as_ref().map(|ex| ex.value_type()),
            "aggregation expression",
        )?;
    }

    Ok(Expr::Aggregate(ex))
}

fn check_ast_for_call(ex: Call) -> Result<Expr, String> {
    let name = ex.func.name;

    check_call_arity(
        ex.func.arg_types.len(),
        ex.func.variadic,
        ex.args.len(),
        name,
    )?;

    // special cases from https://prometheus.io/docs/prometheus/latest/querying/functions
    if name.eq("exp") {
        if let Some(val) = ex.args.first().and_then(|ex| ex.scalar_value()) {
            if val.is_nan() || val.is_infinite() {
                return Ok(Expr::Call(ex));
            }
        }
    } else if name.eq("ln") || name.eq("log2") || name.eq("log10") {
        if let Some(val) = ex.args.first().and_then(|ex| ex.scalar_value()) {
            if val.is_nan() || val.is_infinite() || val <= 0.0 {
                return Ok(Expr::Call(ex));
            }
        }
    }

    // PATCHES.md #7 (issue #132): upstream-parity direct-selector check on
    // info()'s second argument (v3.13.0 parse.go:846-859). Only applies
    // when arg 1 is vector-typed: a non-vector arg falls through to
    // check_args_match_types so the type error stays the first error,
    // matching upstream's emission order (parse.go:848 runs first).
    if name == "info" {
        if let Some(arg) = ex.args.args.get(1) {
            if arg.value_type() == ValueType::Vector {
                match arg.as_ref() {
                    Expr::VectorSelector(vs) if vs.name.is_some() => {
                        return Err(
                            "expected label selectors only, got vector selector instead".into()
                        );
                    }
                    Expr::VectorSelector(_) => {}
                    _ => return Err("expected label selectors only".into()),
                }
            }
        }
    }

    // PATCHES.md #6 (issue #82 v5): the eager empty-matcher operand
    // guard on every call argument, EXCEPT `info()`'s second argument
    // (index 1 — 0-indexed, matching the deferred bypass's own
    // `bypass_second && i == 1` in `check_no_empty_selectors`) — the one
    // context where a bare `{}` is a legal label-selector-only operand.
    // `info()`'s own arg 0 is NOT exempt: `info({})` still rejects.
    for (i, arg) in ex.args.args.iter().enumerate() {
        if name == "info" && i == 1 {
            continue;
        }
        reject_empty_operand(arg)?;
    }

    check_args_match_types(&ex.args.args, &ex.func.arg_types, name)?;
    Ok(Expr::Call(ex))
}

fn check_call_arity(nargs: usize, variadic: i32, actual: usize, name: &str) -> Result<(), String> {
    if variadic == 0 {
        if nargs != actual {
            return Err(format!(
                "expected {nargs} argument(s) in call to '{name}', got {actual}"
            ));
        }
    } else {
        let na = nargs.saturating_sub(1);
        if na > actual {
            return Err(format!(
                "expected at least {na} argument(s) in call to '{name}', got {actual}"
            ));
        } else if variadic > 0 {
            let nargsmax = na + variadic as usize;
            if nargsmax < actual {
                return Err(format!(
                    "expected at most {nargsmax} argument(s) in call to '{name}', got {actual}"
                ));
            }
        }
    }
    Ok(())
}

fn check_args_match_types(
    args: &[Box<Expr>],
    arg_types: &[ValueType],
    name: &str,
) -> Result<(), String> {
    for (i, actual_arg) in args.iter().enumerate() {
        let expected_idx = if i < arg_types.len() {
            i
        } else {
            arg_types.len() - 1
        };
        expect_type(
            arg_types[expected_idx],
            Some(actual_arg.value_type()),
            &format!("call to function '{name}'"),
        )?;
    }
    Ok(())
}

fn check_ast_for_unary(ex: UnaryExpr) -> Result<Expr, String> {
    reject_empty_operand(&ex.expr)?;
    let value_type = ex.expr.value_type();
    if value_type != ValueType::Scalar && value_type != ValueType::Vector {
        return Err(format!(
            "unary expression only allowed on expressions of type scalar or vector, got {value_type}"
        ));
    }

    Ok(Expr::Unary(ex))
}

fn check_ast_for_subquery(ex: SubqueryExpr) -> Result<Expr, String> {
    reject_empty_operand(&ex.expr)?;
    let value_type = ex.expr.value_type();
    if value_type != ValueType::Vector {
        return Err(format!(
            "subquery is only allowed on vector, got {value_type} instead"
        ));
    }

    Ok(Expr::Subquery(ex))
}

/// `true` iff `expr` is a bare, name-less vector selector with no
/// non-empty matcher (PATCHES.md #6, issue #82 v5) — the same predicate
/// [`check_no_empty_selectors`]'s `selector_violates` applies, widened
/// from the old reduction-time arm's literal "zero matchers" check to
/// `Matchers::is_empty_matchers()` (a matcher set that only ever matches
/// `""`, e.g. `{x=~".*"}`, is equally "empty" — matching the deferred
/// walk exactly, so `sum({x=~".*"})` rejects eagerly too).
fn is_bare_empty_selector(expr: &Expr) -> bool {
    matches!(expr, Expr::VectorSelector(vs) if vs.name.is_none() && vs.matchers.is_empty_matchers())
}

/// The eager empty-matcher operand guard (PATCHES.md #6, issue #82 v5):
/// called from every depth-adding reduction's `check_ast_for_*` on each
/// of its immediate `Expr` operands, so a bare `{}` (or an
/// all-empty-matching selector) is rejected at the SHALLOWEST reduction
/// that wraps it — restoring the eager short-circuit the old
/// leaf-arm rejection provided, now context-aware (`info()`'s second
/// call argument is the one exempt position — see
/// [`check_ast_for_call`]).
fn reject_empty_operand(expr: &Expr) -> Result<(), String> {
    if is_bare_empty_selector(expr) {
        return Err("vector selector must contain at least one non-empty matcher".into());
    }
    Ok(())
}

/// The `MatrixSelector` eager check (PATCHES.md #6, issue #82 v5): a
/// `matrix_selector` reduces through `check_ast` at its own grammar
/// production (`promql.y:194`) — the innermost point at which a
/// range-wrapped bare selector (`{}[5m]`, and so `rate({}[5m])`,
/// `abs(rate({}[5m]))`, …) can be caught before any wrapping call or
/// operator reduces. Without this arm the empty selector hides behind a
/// type-valid `MatrixSelector` past every `VectorSelector`-only operand
/// guard, deferring rejection until the post-parse
/// [`check_no_empty_selectors`] walk — which only runs after the
/// generated parser has already built the (deep) tree, reopening the
/// stack-overflow hole `reject_empty_operand`'s eagerness otherwise
/// closes. Predicate mirrors [`is_bare_empty_selector`] verbatim (a
/// `MatrixSelector` is never `info()`'s second argument — that position
/// type-checks as an instant vector — so no bypass ever applies here).
fn check_ast_for_matrix_selector(ex: MatrixSelector) -> Result<Expr, String> {
    if ex.vs.name.is_none() && ex.vs.matchers.is_empty_matchers() {
        return Err("vector selector must contain at least one non-empty matcher".into());
    }
    Ok(Expr::MatrixSelector(ex))
}

fn check_ast_for_vector_selector(ex: VectorSelector) -> Result<Expr, String> {
    match ex.name {
        Some(ref name) => match ex.matchers.find_matcher_value(METRIC_NAME) {
            Some(val) => Err(format!(
                "metric name must not be set twice: '{name}' or '{val}'"
            )),
            None => Ok(Expr::VectorSelector(ex)),
        },
        // NOTE (PATCHES.md #6, issue #82 v5): a name-less selector's "at
        // least one non-empty matcher" rejection no longer lives here.
        // This reduction-time check used to run before any enclosing
        // context was known, which is exactly why it could never accept
        // upstream's one exempt context — `info()`'s second argument
        // (`VectorSelector.BypassEmptyMatcherCheck`, v3.13.0
        // parse.go:846-921), a label-selector-only position where an
        // all-empty-matching selector like `{data=~".*"}` — or, per issue
        // #82's re-review, the literal `{}` — is legal. The eager reject
        // now lives on every depth-adding reduction ONE LEVEL UP
        // (`reject_empty_operand`/`check_ast_for_matrix_selector`), which
        // stays context-aware (`check_ast_for_call` skips it for
        // `info()`'s second argument) while remaining just as eager: a
        // bare selector's own reduction here always succeeds, but nothing
        // wraps it without hitting a guard except the exempt position and
        // the true top-level bare `{}` (caught by the post-parse
        // [`check_no_empty_selectors`] backstop, which cannot overflow —
        // a top-level expression has no wrapping nesting). Measured
        // (PATCHES.md #6): this relocation does not reopen the pinned
        // deep fuzz-regression input's stack-overflow hole (`(-{}-1…`
        // ×10k, `parse.rs` `test_corner_fail_cases`) — it still returns
        // this exact `Err` with no overflow, since `-{}-1` is caught
        // eagerly at the innermost `unary_expr` reduction.
        _ => Ok(Expr::VectorSelector(ex)),
    }
}

/// Iteratively dismantles a rejected AST (PATCHES.md #6): the deferred
/// [`check_no_empty_selectors`] walk can reject a tree as deep as the
/// generated LR parser can build (measured ~8k `-{…}-1` units on a
/// 2 MiB stack before the GRAMMAR's own recursion — a pre-existing,
/// input-kind-independent bound — overflows first), and letting that
/// tree simply fall out of scope would recursively drop one stack frame
/// per nesting level. Children are moved out of their boxes onto an
/// explicit worklist so every shell drops shallowly — the rejection
/// path adds O(1) stack beyond the parser's own bound. Only the `Expr`
/// spine needs this: every other field (matchers, durations, literals)
/// has depth bounded by its own written form independent of expression
/// nesting.
pub(crate) fn dismantle(expr: Expr) {
    let mut work: Vec<Expr> = vec![expr];
    while let Some(node) = work.pop() {
        match node {
            Expr::Aggregate(agg) => {
                work.push(*agg.expr);
                if let Some(param) = agg.param {
                    work.push(*param);
                }
            }
            Expr::Unary(u) => work.push(*u.expr),
            Expr::Binary(b) => {
                work.push(*b.lhs);
                work.push(*b.rhs);
            }
            Expr::Paren(p) => work.push(*p.expr),
            Expr::Subquery(sq) => work.push(*sq.expr),
            Expr::Call(c) => work.extend(c.args.args.into_iter().map(|b| *b)),
            Expr::NumberLiteral(_)
            | Expr::StringLiteral(_)
            | Expr::VectorSelector(_)
            | Expr::MatrixSelector(_)
            | Expr::Extension(_) => {}
        }
    }
}

/// The deferred "vector selector must contain at least one non-empty
/// matcher" check (PATCHES.md #6): walks the finished tree iteratively
/// and rejects every name-less, all-empty-matching vector selector
/// EXCEPT one in `info()`'s second-argument position — the port of
/// upstream `BypassEmptyMatcherCheck` (v3.13.0 parse.go:846-921).
/// **Issue #82 v5:** every WRAPPED occurrence (operand of a unary/
/// binary/subquery/aggregate/paren/call, or a `MatrixSelector`) is now
/// caught EAGERLY by `reject_empty_operand`/`check_ast_for_matrix_selector`
/// at its own depth-adding reduction, before this walk ever runs — this
/// backstop is reached only for a bare top-level `{}` (no wrapping
/// nesting, so no overflow risk) and remains the sole place the
/// `info()`-arg-1 bypass is evaluated (a direct `VectorSelector` there
/// never hits an eager guard at all — see `check_ast_for_call`).
pub(crate) fn check_no_empty_selectors(expr: &Expr) -> Result<(), String> {
    fn selector_violates(name: &Option<String>, matchers: &Matchers) -> bool {
        // When name is None, a vector selector must contain at least one
        // non-empty matcher to prevent implicit selection of all metrics
        // (e.g. by a typo).
        name.is_none() && matchers.is_empty_matchers()
    }

    // (node, bypass): `bypass` is true only for info()'s second argument.
    let mut stack: Vec<(&Expr, bool)> = vec![(expr, false)];
    while let Some((node, bypass)) = stack.pop() {
        match node {
            Expr::VectorSelector(vs) => {
                if !bypass && selector_violates(&vs.name, &vs.matchers) {
                    return Err(
                        "vector selector must contain at least one non-empty matcher".into(),
                    );
                }
            }
            Expr::MatrixSelector(ms) => {
                // A matrix selector is never info()'s second argument
                // (type-checked as vector), so no bypass applies.
                if selector_violates(&ms.vs.name, &ms.vs.matchers) {
                    return Err(
                        "vector selector must contain at least one non-empty matcher".into(),
                    );
                }
            }
            Expr::Call(call) => {
                let bypass_second = call.func.name == "info";
                for (i, arg) in call.args.args.iter().enumerate() {
                    stack.push((arg, bypass_second && i == 1));
                }
            }
            Expr::Aggregate(agg) => {
                stack.push((&agg.expr, false));
                if let Some(param) = &agg.param {
                    stack.push((param, false));
                }
            }
            Expr::Binary(bin) => {
                stack.push((&bin.lhs, false));
                stack.push((&bin.rhs, false));
            }
            Expr::Paren(p) => stack.push((&p.expr, false)),
            Expr::Unary(u) => stack.push((&u.expr, false)),
            Expr::Subquery(sq) => stack.push((&sq.expr, false)),
            Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::Extension(_) => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::label::{MatchOp, Matcher};

    #[test]
    fn test_valid_at_modifier() {
        let cases = vec![
            // tuple: (seconds, elapsed milliseconds before or after UNIX_EPOCH)
            (0.0, 0),
            (1000.3, 1000300),    // after UNIX_EPOCH
            (1000.9, 1000900),    // after UNIX_EPOCH
            (1000.9991, 1000999), // after UNIX_EPOCH
            (1000.9999, 1001000), // after UNIX_EPOCH
            (-1000.3, 1000300),   // before UNIX_EPOCH
            (-1000.9, 1000900),   // before UNIX_EPOCH
        ];

        for (secs, elapsed) in cases {
            match AtModifier::try_from(secs).unwrap() {
                AtModifier::At(st) => {
                    if secs.is_sign_positive() || secs == 0.0 {
                        assert_eq!(
                            elapsed,
                            st.duration_since(SystemTime::UNIX_EPOCH)
                                .unwrap()
                                .as_millis()
                        )
                    } else if secs.is_sign_negative() {
                        assert_eq!(
                            elapsed,
                            SystemTime::UNIX_EPOCH
                                .duration_since(st)
                                .unwrap()
                                .as_millis()
                        )
                    }
                }
                _ => panic!(),
            }
        }

        assert_eq!(
            AtModifier::try_from(Expr::from(1.0)),
            AtModifier::try_from(1.0),
        );
    }

    #[test]
    fn test_invalid_at_modifier() {
        let cases = vec![
            f64::MAX,
            f64::MIN,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ];

        for secs in cases {
            assert!(AtModifier::try_from(secs).is_err())
        }

        assert_eq!(
            AtModifier::try_from(token::T_ADD),
            Err("invalid @ modifier preprocessor '+', START or END is valid.".into())
        );

        assert_eq!(
            AtModifier::try_from(Expr::from("string literal")),
            Err("invalid float value after @ modifier".into())
        );
    }

    #[test]
    fn test_binary_labels() {
        assert_eq!(
            &Labels::new(vec!["foo", "bar"]),
            LabelModifier::Include(Labels::new(vec!["foo", "bar"])).labels()
        );

        assert_eq!(
            &Labels::new(vec!["foo", "bar"]),
            LabelModifier::Exclude(Labels::new(vec!["foo", "bar"])).labels()
        );

        assert_eq!(
            &Labels::new(vec!["foo", "bar"]),
            VectorMatchCardinality::OneToMany(Labels::new(vec!["foo", "bar"]))
                .labels()
                .unwrap()
        );

        assert_eq!(
            &Labels::new(vec!["foo", "bar"]),
            VectorMatchCardinality::ManyToOne(Labels::new(vec!["foo", "bar"]))
                .labels()
                .unwrap()
        );

        assert_eq!(VectorMatchCardinality::OneToOne.labels(), None);
        assert_eq!(VectorMatchCardinality::ManyToMany.labels(), None);
    }

    #[test]
    fn test_neg() {
        assert_eq!(
            -VectorSelector::from("foo"),
            UnaryExpr {
                expr: Box::new(Expr::from(VectorSelector::from("foo"))),
                pos: AstPos::default(),
            }
        )
    }

    #[test]
    fn test_scalar_value() {
        assert_eq!(Some(1.0), Expr::from(1.0).scalar_value());
        assert_eq!(None, Expr::from("1.0").scalar_value());
    }

    #[test]
    fn test_at_expr() {
        assert_eq!(
            "@ <timestamp> may not be set multiple times",
            Expr::from(VectorSelector::from("foo"))
                .at_expr(AtModifier::try_from(1.0).unwrap())
                .and_then(|ex| ex.at_expr(AtModifier::try_from(1.0).unwrap()))
                .unwrap_err()
        );

        assert_eq!(
            "@ <timestamp> may not be set multiple times",
            Expr::new_matrix_selector(
                Expr::from(VectorSelector::from("foo")),
                Duration::from_secs(1),
                None,
            )
            .and_then(|ex| ex.at_expr(AtModifier::try_from(1.0).unwrap()))
            .and_then(|ex| ex.at_expr(AtModifier::try_from(1.0).unwrap()))
            .unwrap_err()
        );

        assert_eq!(
            "@ <timestamp> may not be set multiple times",
            Expr::new_subquery_expr(
                Expr::from(VectorSelector::from("foo")),
                Duration::from_secs(1),
                None,
                None,
                None,
            )
            .and_then(|ex| ex.at_expr(AtModifier::try_from(1.0).unwrap()))
            .and_then(|ex| ex.at_expr(AtModifier::try_from(1.0).unwrap()))
            .unwrap_err()
        )
    }

    #[test]
    fn test_offset_expr() {
        assert_eq!(
            "offset may not be set multiple times",
            Expr::from(VectorSelector::from("foo"))
                .offset_expr(Offset::Pos(Duration::from_secs(1000)))
                .and_then(|ex| ex.offset_expr(Offset::Pos(Duration::from_secs(1000))))
                .unwrap_err()
        );

        assert_eq!(
            "offset may not be set multiple times",
            Expr::new_matrix_selector(
                Expr::from(VectorSelector::from("foo")),
                Duration::from_secs(1),
                None,
            )
            .and_then(|ex| ex.offset_expr(Offset::Pos(Duration::from_secs(1000))))
            .and_then(|ex| ex.offset_expr(Offset::Pos(Duration::from_secs(1000))))
            .unwrap_err()
        );

        assert_eq!(
            "offset may not be set multiple times",
            Expr::new_subquery_expr(
                Expr::from(VectorSelector::from("foo")),
                Duration::from_secs(1),
                None,
                None,
                None,
            )
            .and_then(|ex| ex.offset_expr(Offset::Pos(Duration::from_secs(1000))))
            .and_then(|ex| ex.offset_expr(Offset::Pos(Duration::from_secs(1000))))
            .unwrap_err()
        );
    }

    #[test]
    fn test_expr_to_string() {
        let mut cases = vec![
            ("1", "1"),
            ("- 1", "-1"),
            ("+ 1", "1"),
            ("Inf", "Inf"),
            ("inf", "Inf"),
            ("+Inf", "Inf"),
            ("- Inf", "-Inf"),
            (".5", "0.5"),
            ("5.", "5"),
            ("123.4567", "123.4567"),
            ("5e-3", "0.005"),
            ("5e3", "5000"),
            ("0xc", "12"),
            ("0755", "493"),
            ("08", "8"),
            ("+5.5e-3", "0.0055"),
            ("-0755", "-493"),
            ("NaN", "NaN"),
            ("NAN", "NaN"),
            ("- 1^2", "-1 ^ 2"),
            ("+1 + -2 * 1", "1 + -2 * 1"),
            ("1 + 2/(3*1)", "1 + 2 / (3 * 1)"),
            ("foo*sum", "foo * sum"),
            ("foo * on(test,blub) bar", "foo * on (test, blub) bar"),
            // PulsusDB patch (docs/decisions/0003): `Matchers::Display` now
            // preserves parse order rather than re-sorting alphabetically —
            // these two cases' expected output changed from
            // `up{instance="in",job="hi"}...` (alphabetical) to
            // `up{job="hi",instance="in"}...` (parse order, matching the
            // input).
            (
                r#"up{job="hi", instance="in"} offset 5m @ 100"#,
                r#"up{job="hi",instance="in"} @ 100.000 offset 5m"#,
            ),
            (
                r#"up{job="hi", instance="in"}"#,
                r#"up{job="hi",instance="in"}"#,
            ),
            ("sum (up) by (job,instance)", "sum by (job, instance) (up)"),
            (
                "foo / on(test,blub) group_left(bar) bar",
                "foo / on (test, blub) group_left (bar) bar",
            ),
            (
                "foo / on(test,blub) group_right(bar) bar",
                "foo / on (test, blub) group_right (bar) bar",
            ),
            // Parse order == input order for both of these (unaffected in
            // substance, but no longer alphabetically re-sorted).
            (
                r#"foo{a="b",foo!="bar",test=~"test",bar!~"baz"}"#,
                r#"foo{a="b",foo!="bar",test=~"test",bar!~"baz"}"#,
            ),
            (
                r#"{__name__=~"foo.+",__name__=~".*bar"}"#,
                r#"{__name__=~"foo.+",__name__=~".*bar"}"#,
            ),
            (
                r#"test{a="b"}[5y] OFFSET 3d"#,
                r#"test{a="b"}[5y] offset 3d"#,
            ),
            (r#"{a="b"}[5y] OFFSET 3d"#, r#"{a="b"}[5y] offset 3d"#),
            (
                "sum(some_metric) without(and, by, avg, count, alert, annotations)",
                "sum without (and, by, avg, count, alert, annotations) (some_metric)",
            ),
            (
                r#"floor(some_metric{foo!="bar"})"#,
                r#"floor(some_metric{foo!="bar"})"#,
            ),
            (
                "sum(rate(http_request_duration_seconds[10m])) / count(rate(http_request_duration_seconds[10m]))",
                "sum(rate(http_request_duration_seconds[10m])) / count(rate(http_request_duration_seconds[10m]))",
            ),
            ("rate(some_metric[5m])", "rate(some_metric[5m])"),
            ("round(some_metric,5)", "round(some_metric, 5)"),
            (
                r#"absent(sum(nonexistent{job="myjob"}))"#,
                r#"absent(sum(nonexistent{job="myjob"}))"#,
            ),
            (
                "histogram_quantile(0.9,rate(http_request_duration_seconds_bucket[10m]))",
                "histogram_quantile(0.9, rate(http_request_duration_seconds_bucket[10m]))",
            ),
            (
                "histogram_quantile(0.9,sum(rate(http_request_duration_seconds_bucket[10m])) by(job,le))",
                "histogram_quantile(0.9, sum by (job, le) (rate(http_request_duration_seconds_bucket[10m])))",
            ),
            (
                r#"label_join(up{job="api-server",src1="a",src2="b",src3="c"}, "foo", ",", "src1", "src2", "src3")"#,
                r#"label_join(up{job="api-server",src1="a",src2="b",src3="c"}, "foo", ",", "src1", "src2", "src3")"#,
            ),
            (
                r#"min_over_time(rate(foo{bar="baz"}[2s])[5m:])[4m:3s] @ 100"#,
                r#"min_over_time(rate(foo{bar="baz"}[2s])[5m:])[4m:3s] @ 100.000"#,
            ),
            (
                r#"min_over_time(rate(foo{bar="baz"}[2s])[5m:])[4m:3s]"#,
                r#"min_over_time(rate(foo{bar="baz"}[2s])[5m:])[4m:3s]"#,
            ),
            (
                r#"min_over_time(rate(foo{bar="baz"}[2s])[5m:] offset 4m)[4m:3s]"#,
                r#"min_over_time(rate(foo{bar="baz"}[2s])[5m:] offset 4m)[4m:3s]"#,
            ),
            (
                "some_metric OFFSET 1m [10m:5s]",
                "some_metric offset 1m[10m:5s]",
            ),
            ("some_metric @123 [10m:5s]", "some_metric @ 123.000[10m:5s]"),
            ("some_metric <= 1ms", "some_metric <= 0.001"),
        ];

        // the following cases are from https://github.com/prometheus/prometheus/blob/main/promql/parser/printer_test.go
        let mut cases1 = vec![
            // PulsusDB patch (docs/decisions/0003): upstream Prometheus's
            // own `String()` collapses an explicit empty `by()` to no
            // modifier at all — this crate now deliberately diverges from
            // that one Display convention (rendering `by ()` explicitly
            // instead) to restore `parse -> Display -> parse` AST
            // round-trip fidelity, which is the property PulsusDB's own
            // corpus gate requires.
            (
                r#"sum by() (task:errors:rate10s{job="s"})"#,
                r#"sum by () (task:errors:rate10s{job="s"})"#,
            ),
            (
                r#"sum by(code) (task:errors:rate10s{job="s"})"#,
                r#"sum by (code) (task:errors:rate10s{job="s"})"#,
            ),
            (
                r#"sum without() (task:errors:rate10s{job="s"})"#,
                r#"sum without () (task:errors:rate10s{job="s"})"#,
            ),
            (
                r#"sum without(instance) (task:errors:rate10s{job="s"})"#,
                r#"sum without (instance) (task:errors:rate10s{job="s"})"#,
            ),
            (
                r#"topk(5, task:errors:rate10s{job="s"})"#,
                r#"topk(5, task:errors:rate10s{job="s"})"#,
            ),
            (
                r#"count_values("value", task:errors:rate10s{job="s"})"#,
                r#"count_values("value", task:errors:rate10s{job="s"})"#,
            ),
            ("a - on() c", "a - on () c"),
            ("a - on(b) c", "a - on (b) c"),
            ("a - on(b) group_left(x) c", "a - on (b) group_left (x) c"),
            (
                "a - on(b) group_left(x, y) c",
                "a - on (b) group_left (x, y) c",
            ),
            ("a - on(b) group_left c", "a - on (b) group_left () c"),
            ("a - ignoring(b) c", "a - ignoring (b) c"),
            ("a - ignoring() c", "a - c"),
            ("a + fill(-23) b", "a + fill (-23) b"),
            ("a + fill_left(-23) b", "a + fill_left (-23) b"),
            ("a + fill_right(42) b", "a + fill_right (42) b"),
            (
                "a + fill_left(-23) fill_right(42) b",
                "a + fill_left (-23) fill_right (42) b",
            ),
            (
                "a + on(b) group_left fill(-23) c",
                "a + on (b) group_left () fill (-23) c",
            ),
            ("up > bool 0", "up > bool 0"),
            ("a offset 1m", "a offset 1m"),
            ("a offset -7m", "a offset -7m"),
            (r#"a{c="d"}[5m] offset 1m"#, r#"a{c="d"}[5m] offset 1m"#),
            ("a[5m] offset 1m", "a[5m] offset 1m"),
            ("a[12m] offset -3m", "a[12m] offset -3m"),
            ("a[1h:5m] offset 1m", "a[1h:5m] offset 1m"),
            (r#"{__name__="a"}"#, r#"{__name__="a"}"#),
            (r#"a{b!="c"}[1m]"#, r#"a{b!="c"}[1m]"#),
            (r#"a{b=~"c"}[1m]"#, r#"a{b=~"c"}[1m]"#),
            (r#"a{b!~"c"}[1m]"#, r#"a{b!~"c"}[1m]"#),
            ("a @ 10", "a @ 10.000"),
            ("a[1m] @ 10", "a[1m] @ 10.000"),
            ("a @ start()", "a @ start()"),
            ("a @ end()", "a @ end()"),
            ("a[1m] @ start()", "a[1m] @ start()"),
            ("a[1m] @ end()", "a[1m] @ end()"),
        ];

        // the following cases copy the tests from the following: https://github.com/prometheus/prometheus/pull/9138
        let mut cases2 = vec![
            (
                r#"test{a="b"}[5y] OFFSET 3d"#,
                r#"test{a="b"}[5y] offset 3d"#,
            ),
            (
                r#"test{a="b"}[5m] OFFSET 3600"#,
                r#"test{a="b"}[5m] offset 1h"#,
            ),
            ("foo[3ms] @ 2.345", "foo[3ms] @ 2.345"),
            ("foo[4s180ms] @ 2.345", "foo[4s180ms] @ 2.345"),
            ("foo[4.18] @ 2.345", "foo[4s180ms] @ 2.345"),
            ("foo[4s18ms] @ 2.345", "foo[4s18ms] @ 2.345"),
            ("foo[4.018] @ 2.345", "foo[4s18ms] @ 2.345"),
            ("test[5]", "test[5s]"),
            ("some_metric[5m] @ 1m", "some_metric[5m] @ 60.000"),
            ("metric @ 100s", "metric @ 100.000"),
            ("metric @ 1m40s", "metric @ 100.000"),
            ("metric @ 100 offset 50", "metric @ 100.000 offset 50s"),
            ("metric offset 50 @ 100", "metric @ 100.000 offset 50s"),
            ("metric @ 0 offset -50", "metric @ 0.000 offset -50s"),
            ("metric offset -50 @ 0", "metric @ 0.000 offset -50s"),
            (
                r#"sum_over_time(metric{job="1"}[100] @ 100 offset 50)"#,
                r#"sum_over_time(metric{job="1"}[1m40s] @ 100.000 offset 50s)"#,
            ),
            (
                r#"sum_over_time(metric{job="1"}[100] offset 50s @ 100)"#,
                r#"sum_over_time(metric{job="1"}[1m40s] @ 100.000 offset 50s)"#,
            ),
            (
                r#"sum_over_time(metric{job="1"}[100] @ 100) + label_replace(sum_over_time(metric{job="2"}[100] @ 100), "job", "1", "", "")"#,
                r#"sum_over_time(metric{job="1"}[1m40s] @ 100.000) + label_replace(sum_over_time(metric{job="2"}[1m40s] @ 100.000), "job", "1", "", "")"#,
            ),
            (
                r#"sum_over_time(metric{job="1"}[100:1] offset 20 @ 100)"#,
                r#"sum_over_time(metric{job="1"}[1m40s:1s] @ 100.000 offset 20s)"#,
            ),
        ];

        cases.append(&mut cases1);
        cases.append(&mut cases2);
        for (input, expected) in cases {
            let expr = crate::parser::parse(input).unwrap();
            assert_eq!(expected, expr.to_string())
        }
    }

    #[test]
    fn test_vector_selector_to_string() {
        let cases = vec![
            (VectorSelector::default(), ""),
            (VectorSelector::from("foobar"), "foobar"),
            (
                {
                    let name = Some(String::from("foobar"));
                    let matchers = Matchers::one(Matcher::new(MatchOp::Equal, "a", "x"));
                    VectorSelector::new(name, matchers)
                },
                r#"foobar{a="x"}"#,
            ),
            (
                {
                    let matchers = Matchers::new(vec![
                        Matcher::new(MatchOp::Equal, "a", "x"),
                        Matcher::new(MatchOp::Equal, "b", "y"),
                    ]);
                    VectorSelector::new(None, matchers)
                },
                r#"{a="x",b="y"}"#,
            ),
            (
                {
                    let matchers =
                        Matchers::one(Matcher::new(MatchOp::Equal, METRIC_NAME, "foobar"));
                    VectorSelector::new(None, matchers)
                },
                r#"{__name__="foobar"}"#,
            ),
        ];

        for (vs, expect) in cases {
            assert_eq!(expect, vs.to_string())
        }
    }

    #[test]
    fn test_aggregate_expr_pretty() {
        let cases = vec![
            ("sum(foo)", "sum(foo)"),
            // PulsusDB patch (docs/decisions/0003): shares `get_op_string`
            // with `Display` — see the `cases1` comment above.
            (
                r#"sum by() (task:errors:rate10s{job="s"})"#,
                r#"sum by () (
  task:errors:rate10s{job="s"}
)"#,
            ),
            (
                r#"sum without(job,foo) (task:errors:rate10s{job="s"})"#,
                r#"sum without (job, foo) (
  task:errors:rate10s{job="s"}
)"#,
            ),
            (
                r#"sum(task:errors:rate10s{job="s"}) without(job,foo)"#,
                r#"sum without (job, foo) (
  task:errors:rate10s{job="s"}
)"#,
            ),
            (
                r#"sum by(job,foo) (task:errors:rate10s{job="s"})"#,
                r#"sum by (job, foo) (
  task:errors:rate10s{job="s"}
)"#,
            ),
            (
                r#"sum (task:errors:rate10s{job="s"}) by(job,foo)"#,
                r#"sum by (job, foo) (
  task:errors:rate10s{job="s"}
)"#,
            ),
            (
                r#"topk(10, ask:errors:rate10s{job="s"})"#,
                r#"topk(
  10,
  ask:errors:rate10s{job="s"}
)"#,
            ),
            (
                r#"sum by(job,foo) (sum by(job,foo) (task:errors:rate10s{job="s"}))"#,
                r#"sum by (job, foo) (
  sum by (job, foo) (
    task:errors:rate10s{job="s"}
  )
)"#,
            ),
            (
                r#"sum by(job,foo) (sum by(job,foo) (sum by(job,foo) (task:errors:rate10s{job="s"})))"#,
                r#"sum by (job, foo) (
  sum by (job, foo) (
    sum by (job, foo) (
      task:errors:rate10s{job="s"}
    )
  )
)"#,
            ),
            (
                r#"sum by(job,foo)
(sum by(job,foo) (task:errors:rate10s{job="s"}))"#,
                r#"sum by (job, foo) (
  sum by (job, foo) (
    task:errors:rate10s{job="s"}
  )
)"#,
            ),
            (
                r#"sum by(job,foo)
(sum(task:errors:rate10s{job="s"}) without(job,foo))"#,
                r#"sum by (job, foo) (
  sum without (job, foo) (
    task:errors:rate10s{job="s"}
  )
)"#,
            ),
            (
                r#"sum by(job,foo) # Comment 1.
(sum by(job,foo) ( # Comment 2.
task:errors:rate10s{job="s"}))"#,
                r#"sum by (job, foo) (
  sum by (job, foo) (
    task:errors:rate10s{job="s"}
  )
)"#,
            ),
        ];

        for (input, expect) in cases {
            let expr = crate::parser::parse(input);
            assert_eq!(expect, expr.unwrap().pretty(0, 10));
        }
    }

    #[test]
    fn test_binary_expr_pretty() {
        let cases = vec![
            ("a+b", "a + b"),
            (
                "a == bool 1",
                "  a
== bool
  1",
            ),
            (
                "a == 1024000",
                "  a
==
  1024000",
            ),
            (
                "a + ignoring(job) b",
                "  a
+ ignoring (job)
  b",
            ),
            (
                "foo_1 + foo_2",
                "  foo_1
+
  foo_2",
            ),
            (
                "foo_1 + foo_2 + foo_3",
                "    foo_1
  +
    foo_2
+
  foo_3",
            ),
            (
                "foo + baar + foo_3",
                "  foo + baar
+
  foo_3",
            ),
            (
                "foo_1 + foo_2 + foo_3 + foo_4",
                "      foo_1
    +
      foo_2
  +
    foo_3
+
  foo_4",
            ),
            (
                "foo_1 + ignoring(foo) foo_2 + ignoring(job) group_left foo_3 + on(instance) group_right foo_4",
                "      foo_1
    + ignoring (foo)
      foo_2
  + ignoring (job) group_left ()
    foo_3
+ on (instance) group_right ()
  foo_4",
            ),
        ];

        for (input, expect) in cases {
            let expr = crate::parser::parse(input);
            assert_eq!(expect, expr.unwrap().pretty(0, 10));
        }
    }

    #[test]
    fn test_call_expr_pretty() {
        let cases = vec![
            (
                "rate(foo[1m])",
                "rate(
  foo[1m]
)",
            ),
            (
                "sum_over_time(foo[1m])",
                "sum_over_time(
  foo[1m]
)",
            ),
            (
                "rate(long_vector_selector[10m:1m] @ start() offset 1m)",
                "rate(
  long_vector_selector[10m:1m] @ start() offset 1m
)",
            ),
            (
                "histogram_quantile(0.9, rate(foo[1m]))",
                "histogram_quantile(
  0.9,
  rate(
    foo[1m]
  )
)",
            ),
            (
                "histogram_quantile(0.9, rate(foo[1m] @ start()))",
                "histogram_quantile(
  0.9,
  rate(
    foo[1m] @ start()
  )
)",
            ),
            (
                "max_over_time(rate(demo_api_request_duration_seconds_count[1m])[1m:] @ start() offset 1m)",
                "max_over_time(
  rate(
    demo_api_request_duration_seconds_count[1m]
  )[1m:] @ start() offset 1m
)",
            ),
            (
                r#"label_replace(up{job="api-server",service="a:c"}, "foo", "$1", "service", "(.*):.*")"#,
                r#"label_replace(
  up{job="api-server",service="a:c"},
  "foo",
  "$1",
  "service",
  "(.*):.*"
)"#,
            ),
            (
                r#"label_replace(label_replace(up{job="api-server",service="a:c"}, "foo", "$1", "service", "(.*):.*"), "foo", "$1", "service", "(.*):.*")"#,
                r#"label_replace(
  label_replace(
    up{job="api-server",service="a:c"},
    "foo",
    "$1",
    "service",
    "(.*):.*"
  ),
  "foo",
  "$1",
  "service",
  "(.*):.*"
)"#,
            ),
        ];

        for (input, expect) in cases {
            let expr = crate::parser::parse(input);
            assert_eq!(expect, expr.unwrap().pretty(0, 10));
        }
    }

    #[test]
    fn test_paren_expr_pretty() {
        let cases = vec![
            ("(foo)", "(foo)"),
            (
                "(_foo_long_)",
                "(
  _foo_long_
)",
            ),
            (
                "((foo_long))",
                "(
  (foo_long)
)",
            ),
            (
                "((_foo_long_))",
                "(
  (
    _foo_long_
  )
)",
            ),
            (
                "(((foo_long)))",
                "(
  (
    (foo_long)
  )
)",
            ),
            ("(1 + 2)", "(1 + 2)"),
            (
                "(foo + bar)",
                "(
  foo + bar
)",
            ),
            (
                "(foo_long + bar_long)",
                "(
    foo_long
  +
    bar_long
)",
            ),
            (
                "(foo_long + bar_long + bar_2_long)",
                "(
      foo_long
    +
      bar_long
  +
    bar_2_long
)",
            ),
            (
                "((foo_long + bar_long) + bar_2_long)",
                "(
    (
        foo_long
      +
        bar_long
    )
  +
    bar_2_long
)",
            ),
            (
                "(1111 + 2222)",
                "(
    1111
  +
    2222
)",
            ),
            (
                "(sum_over_time(foo[1m]))",
                "(
  sum_over_time(
    foo[1m]
  )
)",
            ),
            (
                r#"(label_replace(up{job="api-server",service="a:c"}, "foo", "$1", "service", "(.*):.*"))"#,
                r#"(
  label_replace(
    up{job="api-server",service="a:c"},
    "foo",
    "$1",
    "service",
    "(.*):.*"
  )
)"#,
            ),
            (
                r#"(label_replace(label_replace(up{job="api-server",service="a:c"}, "foo", "$1", "service", "(.*):.*"), "foo", "$1", "service", "(.*):.*"))"#,
                r#"(
  label_replace(
    label_replace(
      up{job="api-server",service="a:c"},
      "foo",
      "$1",
      "service",
      "(.*):.*"
    ),
    "foo",
    "$1",
    "service",
    "(.*):.*"
  )
)"#,
            ),
            (
                r#"(label_replace(label_replace((up{job="api-server",service="a:c"}), "foo", "$1", "service", "(.*):.*"), "foo", "$1", "service", "(.*):.*"))"#,
                r#"(
  label_replace(
    label_replace(
      (
        up{job="api-server",service="a:c"}
      ),
      "foo",
      "$1",
      "service",
      "(.*):.*"
    ),
    "foo",
    "$1",
    "service",
    "(.*):.*"
  )
)"#,
            ),
        ];

        for (input, expect) in cases {
            let expr = crate::parser::parse(input);
            assert_eq!(expect, expr.unwrap().pretty(0, 10));
        }
    }

    #[test]
    fn test_unary_expr_pretty() {
        let cases = vec![
            ("-1", "-1"),
            ("-vector_selector", "-vector_selector"),
            (
                "(-vector_selector)",
                "(
  -vector_selector
)",
            ),
            (
                "-histogram_quantile(0.9,rate(foo[1m]))",
                "-histogram_quantile(
  0.9,
  rate(
    foo[1m]
  )
)",
            ),
            (
                "-histogram_quantile(0.99, sum by (le) (rate(foo[1m])))",
                "-histogram_quantile(
  0.99,
  sum by (le) (
    rate(
      foo[1m]
    )
  )
)",
            ),
            (
                "-histogram_quantile(0.9, -rate(foo[1m] @ start()))",
                "-histogram_quantile(
  0.9,
  -rate(
    foo[1m] @ start()
  )
)",
            ),
            (
                "(-histogram_quantile(0.9, -rate(foo[1m] @ start())))",
                "(
  -histogram_quantile(
    0.9,
    -rate(
      foo[1m] @ start()
    )
  )
)",
            ),
        ];

        for (input, expect) in cases {
            let expr = crate::parser::parse(input);
            assert_eq!(expect, expr.unwrap().pretty(0, 10));
        }
    }

    #[test]
    fn test_expr_pretty() {
        // Following queries have been taken from https://monitoring.mixins.dev/
        // PulsusDB patch (docs/decisions/0003): expected matcher order below
        // updated to parse-preserved order (was alphabetical) — see the
        // `Matchers` `Display` fix in `label/matcher.rs`.
        let cases = vec![
            (
                r#"(node_filesystem_avail_bytes{job="node",fstype!=""} / node_filesystem_size_bytes{job="node",fstype!=""} * 100 < 40 and predict_linear(node_filesystem_avail_bytes{job="node",fstype!=""}[6h], 24*60*60) < 0 and node_filesystem_readonly{job="node",fstype!=""} == 0)"#,
                r#"(
            node_filesystem_avail_bytes{job="node",fstype!=""}
          /
            node_filesystem_size_bytes{job="node",fstype!=""}
        *
          100
      <
        40
    and
        predict_linear(
          node_filesystem_avail_bytes{job="node",fstype!=""}[6h],
            24 * 60
          *
            60
        )
      <
        0
  and
      node_filesystem_readonly{job="node",fstype!=""}
    ==
      0
)"#,
            ),
            (
                r#"(node_filesystem_avail_bytes{job="node",fstype!=""} / node_filesystem_size_bytes{job="node",fstype!=""} * 100 < 20 and predict_linear(node_filesystem_avail_bytes{job="node",fstype!=""}[6h], 4*60*60) < 0 and node_filesystem_readonly{job="node",fstype!=""} == 0)"#,
                r#"(
            node_filesystem_avail_bytes{job="node",fstype!=""}
          /
            node_filesystem_size_bytes{job="node",fstype!=""}
        *
          100
      <
        20
    and
        predict_linear(
          node_filesystem_avail_bytes{job="node",fstype!=""}[6h],
            4 * 60
          *
            60
        )
      <
        0
  and
      node_filesystem_readonly{job="node",fstype!=""}
    ==
      0
)"#,
            ),
            (
                r#"(node_timex_offset_seconds > 0.05 and deriv(node_timex_offset_seconds[5m]) >= 0) or (node_timex_offset_seconds < -0.05 and deriv(node_timex_offset_seconds[5m]) <= 0)"#,
                r#"  (
        node_timex_offset_seconds
      >
        0.05
    and
        deriv(
          node_timex_offset_seconds[5m]
        )
      >=
        0
  )
or
  (
        node_timex_offset_seconds
      <
        -0.05
    and
        deriv(
          node_timex_offset_seconds[5m]
        )
      <=
        0
  )"#,
            ),
            (
                r#"1 - ((node_memory_MemAvailable_bytes{job="node"} or (node_memory_Buffers_bytes{job="node"} + node_memory_Cached_bytes{job="node"} + node_memory_MemFree_bytes{job="node"} + node_memory_Slab_bytes{job="node"}) ) / node_memory_MemTotal_bytes{job="node"})"#,
                r#"  1
-
  (
      (
          node_memory_MemAvailable_bytes{job="node"}
        or
          (
                  node_memory_Buffers_bytes{job="node"}
                +
                  node_memory_Cached_bytes{job="node"}
              +
                node_memory_MemFree_bytes{job="node"}
            +
              node_memory_Slab_bytes{job="node"}
          )
      )
    /
      node_memory_MemTotal_bytes{job="node"}
  )"#,
            ),
            (
                r#"min by (job, integration) (rate(alertmanager_notifications_failed_total{job="alertmanager", integration=~".*"}[5m]) / rate(alertmanager_notifications_total{job="alertmanager", integration="~.*"}[5m])) > 0.01"#,
                r#"  min by (job, integration) (
      rate(
        alertmanager_notifications_failed_total{job="alertmanager",integration=~".*"}[5m]
      )
    /
      rate(
        alertmanager_notifications_total{job="alertmanager",integration="~.*"}[5m]
      )
  )
>
  0.01"#,
            ),
            (
                r#"(count by (job) (changes(process_start_time_seconds{job="alertmanager"}[10m]) > 4) / count by (job) (up{job="alertmanager"})) >= 0.5"#,
                r#"  (
      count by (job) (
          changes(
            process_start_time_seconds{job="alertmanager"}[10m]
          )
        >
          4
      )
    /
      count by (job) (
        up{job="alertmanager"}
      )
  )
>=
  0.5"#,
            ),
        ];

        for (input, expect) in cases {
            let expr = crate::parser::parse(input);
            assert_eq!(expect, expr.unwrap().pretty(0, 10));
        }
    }

    #[test]
    fn test_step_invariant_pretty() {
        let cases = vec![
            ("a @ 1", "a @ 1.000"),
            ("a @ start()", "a @ start()"),
            ("vector_selector @ start()", "vector_selector @ start()"),
        ];

        for (input, expect) in cases {
            let expr = crate::parser::parse(input);
            assert_eq!(expect, expr.unwrap().pretty(0, 10));
        }
    }

    #[test]
    fn test_prettify() {
        // PulsusDB patch (docs/decisions/0003): expected matcher order below
        // updated to parse-preserved order (was alphabetical).
        let cases = vec![
            ("vector_selector", "vector_selector"),
            (
                r#"vector_selector{fooooooooooooooooo="barrrrrrrrrrrrrrrrrrr",barrrrrrrrrrrrrrrrrrr="fooooooooooooooooo",process_name="alertmanager"}"#,
                r#"vector_selector{fooooooooooooooooo="barrrrrrrrrrrrrrrrrrr",barrrrrrrrrrrrrrrrrrr="fooooooooooooooooo",process_name="alertmanager"}"#,
            ),
            (
                r#"matrix_selector{fooooooooooooooooo="barrrrrrrrrrrrrrrrrrr",barrrrrrrrrrrrrrrrrrr="fooooooooooooooooo",process_name="alertmanager"}[1y2w3d]"#,
                r#"matrix_selector{fooooooooooooooooo="barrrrrrrrrrrrrrrrrrr",barrrrrrrrrrrrrrrrrrr="fooooooooooooooooo",process_name="alertmanager"}[382d]"#,
            ),
        ];

        for (input, expect) in cases {
            assert_eq!(expect, crate::parser::parse(input).unwrap().prettify());
        }
    }

    #[test]
    fn test_eval_stmt_to_string() {
        // PulsusDB patch (docs/decisions/0003): expected matcher order below
        // updated to parse-preserved order (was alphabetical).
        let query = r#"http_requests_total{job="apiserver", handler="/api/comments"}[5m]"#;
        let start = "2024-10-08T07:15:00.022978+00:00";
        let end = "2024-10-08T07:15:30.012978+00:00";
        let expect = r#"[http_requests_total{job="apiserver",handler="/api/comments"}[5m]] @ [2024-10-08T07:15:00.022978+00:00, 2024-10-08T07:15:30.012978+00:00, 1m, 5m]"#;

        let stmt = EvalStmt {
            expr: crate::parser::parse(query).unwrap(),
            start: DateTime::parse_from_rfc3339(start)
                .unwrap()
                .with_timezone(&Utc)
                .into(),
            end: DateTime::parse_from_rfc3339(end)
                .unwrap()
                .with_timezone(&Utc)
                .into(),
            interval: Duration::from_secs(60),
            lookback_delta: Duration::from_secs(300),
        };

        assert_eq!(expect, stmt.to_string());
    }

    fn make_call(func_name: &str, arg_count: usize) -> Call {
        use crate::parser::function::get_function;
        let func =
            get_function(func_name).unwrap_or_else(|| panic!("unknown function: {func_name}"));
        let args: Vec<Box<Expr>> = (0..arg_count)
            .map(|_| Box::new(Expr::VectorSelector(VectorSelector::from("foo"))))
            .collect();
        Call {
            func,
            args: FunctionArgs { args },
            pos: AstPos::default(),
        }
    }

    #[test]
    fn test_call_arity_variadic_zero() {
        // floor: arg_types=[Vector], variadic=0 → exact 1 arg required
        assert!(check_ast(Expr::Call(make_call("floor", 1))).is_ok());

        let err = check_ast(Expr::Call(make_call("floor", 0))).unwrap_err();
        assert!(
            err.contains("expected 1 argument(s) in call to 'floor', got 0"),
            "{err}"
        );

        let err = check_ast(Expr::Call(make_call("floor", 2))).unwrap_err();
        assert!(
            err.contains("expected 1 argument(s) in call to 'floor', got 2"),
            "{err}"
        );
    }

    #[test]
    fn test_call_arity_bounded_variadic_single_arg_type() {
        // days_in_month: arg_types=[Vector], variadic=1 → min=0, max=1
        // 0 args is valid (default); only "too many" is enforced
        assert!(check_ast(Expr::Call(make_call("days_in_month", 1))).is_ok());

        let err = check_ast(Expr::Call(make_call("days_in_month", 2))).unwrap_err();
        assert!(
            err.contains("expected at most 1 argument(s) in call to 'days_in_month', got 2"),
            "{err}"
        );
    }

    #[test]
    fn test_call_arity_bounded_variadic_two_arg_types() {
        // round: arg_types=[Vector, Scalar], variadic=1 → min=1, max=2
        let err = check_ast(Expr::Call(make_call("round", 0))).unwrap_err();
        assert!(
            err.contains("expected at least 1 argument(s) in call to 'round', got 0"),
            "{err}"
        );

        let err = check_ast(Expr::Call(make_call("round", 3))).unwrap_err();
        assert!(
            err.contains("expected at most 2 argument(s) in call to 'round', got 3"),
            "{err}"
        );

        // info: arg_types=[Vector, Vector], variadic=1 → min=1, max=2
        let err = check_ast(Expr::Call(make_call("info", 0))).unwrap_err();
        assert!(
            err.contains("expected at least 1 argument(s) in call to 'info', got 0"),
            "{err}"
        );

        let err = check_ast(Expr::Call(make_call("info", 3))).unwrap_err();
        assert!(
            err.contains("expected at most 2 argument(s) in call to 'info', got 3"),
            "{err}"
        );
    }

    #[test]
    fn test_call_arity_bounded_variadic_large() {
        // histogram_quantiles: arg_types=[Vector, String, Scalar, Scalar], variadic=9 → min=3, max=12
        let err = check_ast(Expr::Call(make_call("histogram_quantiles", 2))).unwrap_err();
        assert!(
            err.contains("expected at least 3 argument(s) in call to 'histogram_quantiles', got 2"),
            "{err}"
        );

        let err = check_ast(Expr::Call(make_call("histogram_quantiles", 13))).unwrap_err();
        assert!(
            err.contains(
                "expected at most 12 argument(s) in call to 'histogram_quantiles', got 13"
            ),
            "{err}"
        );
    }

    #[test]
    fn test_call_arity_unbounded_variadic() {
        // label_join: arg_types=[Vector, String, String, String], variadic=-1 → min=3, no max
        let err = check_ast(Expr::Call(make_call("label_join", 2))).unwrap_err();
        assert!(
            err.contains("expected at least 3 argument(s) in call to 'label_join', got 2"),
            "{err}"
        );

        // sort_by_label: arg_types=[Vector, String], variadic=-1 → min=1, no max
        let err = check_ast(Expr::Call(make_call("sort_by_label", 0))).unwrap_err();
        assert!(
            err.contains("expected at least 1 argument(s) in call to 'sort_by_label', got 0"),
            "{err}"
        );
    }

    #[test]
    fn test_prettify_with_utf8_labels() {
        // Test that labels with special characters are properly quoted in display
        let cases = vec![
            // (input, expected_display)
            (r#"{"some.metric"}"#, r#"{__name__="some.metric"}"#),
            (
                r#"foo{"label.with.dots"="value"}"#,
                r#"foo{"label.with.dots"="value"}"#,
            ),
            (
                r#"bar{"label-with-dashes"="test"}"#,
                r#"bar{"label-with-dashes"="test"}"#,
            ),
            (
                r#"baz{"label:with:colons"="data"}"#,
                r#"baz{"label:with:colons"="data"}"#,
            ),
            (
                r#"sum by ("service.version", foo) ({"some.metric"})"#,
                r#"sum by ("service.version", foo) ({__name__="some.metric"})"#,
            ),
            (
                r#"sum by (`service.version`, foo) ({"some.metric"})"#,
                r#"sum by ("service.version", foo) ({__name__="some.metric"})"#,
            ),
            // Regular labels should not be quoted
            (r#"foo{job="web"}"#, r#"foo{job="web"}"#),
            (
                r#"bar{instance_id="server1"}"#,
                r#"bar{instance_id="server1"}"#,
            ),
        ];

        for (input, expected) in cases {
            let parsed = crate::parser::parse(input).unwrap();
            let prettified = parsed.prettify();
            assert_eq!(prettified, expected);
        }
    }

    #[test]
    fn test_prettify_escape_roundtrip() {
        // Queries with backslash escapes must survive parse → prettify → re-parse
        let cases = vec![
            // Escaped dot in regex matcher
            (
                r#"{__name__="up",service=~"flagd\\.evaluation\\.v1\\.Service"}"#,
                r#"{__name__="up",service=~"flagd\\.evaluation\\.v1\\.Service"}"#,
            ),
            // Escaped pipe
            (
                r#"{__name__="up",tag=~"a\\|b"}"#,
                r#"{__name__="up",tag=~"a\\|b"}"#,
            ),
            // Literal backslash in value
            (r#"{path="C:\\\\Windows"}"#, r#"{path="C:\\\\Windows"}"#),
            // Embedded double quote
            (r#"{msg="say \"hello\""}"#, r#"{msg="say \"hello\""}"#),
        ];

        for (input, expected) in &cases {
            let parsed = crate::parser::parse(input).unwrap();
            let prettified = parsed.prettify();
            assert_eq!(
                &prettified, expected,
                "prettify mismatch for input: {input}"
            );

            // Roundtrip: re-parsing the prettified output must succeed and produce the same result
            let reparsed = crate::parser::parse(&prettified).unwrap();
            assert_eq!(
                parsed.prettify(),
                reparsed.prettify(),
                "roundtrip failed for input: {input}"
            );
        }
    }
}
