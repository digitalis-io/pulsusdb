//! The span-scope predicate compiler (issues #588 and #589).
//!
//! One TraceQL filter body to one ClickHouse boolean over a `spans` row,
//! and the one place a window and a predicate compose
//! ([`span_membership_sql`]). Nothing here reads or writes: every entry
//! point is pure.
//!
//! **What it serves.** Every attribute scope — `span.`, `resource.`,
//! `instrumentation.`, `event.`, `link.` and the unscoped `.`; the
//! fourteen intrinsics a span row answers — `name`, `kind`, `status`,
//! `statusMessage`, `duration`, `span:id`, `span:parentID`, `trace:id`,
//! `instrumentation:name`, `instrumentation:version`, `event:name`,
//! `event:timeSinceStart`, `link:spanID`, `link:traceID`; a comparison of
//! two of these: span and instrumentation attributes, the ten span-row
//! intrinsics `name`, `statusMessage`, `span:id`, `span:parentID`,
//! `trace:id`, `instrumentation:name`, `instrumentation:version`,
//! `duration`, `status` and `kind`, resource attributes, and
//! `resource.service.name` — its string from the span row, every other arm
//! from the resource row ([`field_terms`]); event and link attributes, the
//! four event and link intrinsics and the unscoped `.k` — as sets, any
//! element, `!=` every element ([`Compiler::set_compare`]); event, link and
//! unscoped operands inside arithmetic and opposite any side, over every
//! tuple of their elements (at most two) ([`Compiler::occurrence_build`]);
//! arithmetic over those fields and literals ([`Compiler::arith_tuple`]), a
//! literal-only side folded ([`fold`]), and a boolean-valued side; and
//! `&&`, `||` and `!`. Every other field and construct is
//! [`PlanError::UnsupportedField`] naming itself and the issue that serves
//! it. The arms are exhaustive
//! with no wildcard, so a new `Intrinsic` or `AttrScope` variant fails to
//! compile here rather than falling into the wrong one.
//!
//! **Four rules worth reading before the code.**
//!
//! * A typed `JSON` subcolumn read is `Nullable`: ``attrs.`k`.:Int64`` is
//!   `Nullable(Int64)` for every row, and the value is `NULL` when the key
//!   is absent **or** stored at another type. So the typed read is
//!   coalesced and the negation is applied to the coalesced result —
//!   `NOT (coalesce(… = v, false) OR …)`. Without the `coalesce`,
//!   `false OR NULL` is `NULL`, `NOT NULL` is `NULL`, and a `WHERE` drops
//!   the row: measured on the worked fixture, that rendering answers
//!   nothing at all. With the negation pushed *inside* the arms it answers
//!   one row instead of eight. `!=` matching a span lacking the key is
//!   this product's documented semantics (`docs/api.md` §4.2) and a
//!   deliberate divergence recorded at
//!   `docs/TraceQL/functional-requirements.md`.
//! * **No intrinsic read is coalesced.** Every intrinsic column in
//!   `schema/schema.sql` is non-nullable, so there is no `NULL` to absorb
//!   and a `coalesce` there would be noise that reads as a rule.
//! * **A decimal integer literal does not go through `f64`.** `f64` loses
//!   integer exactness from `2^53` up, and all six comparison operators
//!   then answer differently; `maxInt` through `parse_num`/`render_num` is
//!   `9223372036854776000`, which ClickHouse types `UInt64`. An integer
//!   literal is exact from −2^127 to 2^128 − 1 and refused outside it.
//!   [`render_number`] and [`fold`] are where that is decided.
//! * **A resource negation is `NOT` outside the subquery, never inside
//!   it.** `NOT (R(p))` and `R(NOT p)` disagree for a span whose resource
//!   row is not visible; outside keeps `{ resource.k != v }` the exact
//!   complement of `{ resource.k = v }`.
//! * **An event or link negation is `NOT` outside `arrayExists`, never
//!   inside the lambda.** `{ event.k != v }` matches a span only when no
//!   element is `v` — a span lacking the key or carrying no elements
//!   included — which is the 2026-09-18 decision
//!   (`docs/benchmarks/traces-differential-ledger.md`,
//!   `traceql-event-link-operand-any-match`). Inside, it would match a span
//!   with any element that differs.

use std::fmt::Write as _;

use pulsus_clickhouse::json_column::escape_json_path;
use pulsus_traceql::{
    ArithOp, AttrScope, BoolOp, ComparisonOp, Duration, Field, FieldExpr, FieldOp, Intrinsic,
    UnaryOp, Value,
};

use crate::logql::escape;
use crate::traces::filter::{
    PlanError, anchored_regex_sql, flip_comparison, kind_code, parse_num, render_num, sql_op,
    status_code,
};
use crate::traces::window_sql::WindowSql;

use super::tracelevel::{TraceLeaf, child_leaf, trace_leaf, type_refusal};

/// A ClickHouse boolean over a `spans` row. Private fields, no public
/// constructor: the four `compile_*` functions are the only ways to obtain
/// one, so no later task can splice un-escaped query text into a
/// statement. The posture [`WindowSql`] already has.
///
/// `Debug` is derived so a case that expected a refusal can print what it
/// got instead; it is not a second way to read the text.
#[derive(Debug, Clone)]
pub struct SpanPredicate {
    sql: String,
    /// `Some(ctx.window)` when the text carries a resource subquery, which
    /// is bounded by that window's days.
    window: Option<WindowSql>,
    /// The messages of the statement's `!` demands, in pre-order; empty for
    /// most predicates.
    demands: Vec<String>,
    /// The per-trace scalar's elements the text reads (issue #594 part 1).
    trace_leaves: Vec<TraceLeaf>,
    /// Whether the text reads the scalar's outside buckets, element 1.
    child_counts: bool,
}

impl SpanPredicate {
    /// A spanset's membership (issue #593, [`super::structural`]), built
    /// from predicates this module compiled: `sql` composes their texts,
    /// `window` is the request's when any of them, or the composition,
    /// reads a window-bounded subquery, and `demands`, `trace_leaves` and
    /// `child_counts` are theirs.
    pub(super) fn composed(
        sql: String,
        window: Option<WindowSql>,
        demands: Vec<String>,
        trace_leaves: Vec<TraceLeaf>,
        child_counts: bool,
    ) -> Self {
        SpanPredicate {
            sql,
            window,
            demands,
            trace_leaves,
            child_counts,
        }
    }

    /// The per-trace scalar's elements the text reads (issue #594 part 1),
    /// each once.
    pub fn trace_leaves(&self) -> &[TraceLeaf] {
        &self.trace_leaves
    }

    /// Whether the text reads the per-trace scalar's outside buckets: it
    /// holds a `span:childCount` leaf.
    pub fn reads_child_counts(&self) -> bool {
        self.child_counts
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Whether this predicate may sit in a statement bounded by `w`: it
    /// carries no resource subquery, or was compiled for `w`.
    pub(crate) fn composes_with(&self, w: WindowSql) -> bool {
        self.window.is_none_or(|pw| pw == w)
    }

    /// The messages this predicate's `throwIf` demands raise. The server
    /// reports any `throwIf` as `Code: 395`, so a route turns that code
    /// into a `400` only when the message is one of these.
    pub fn demand_messages(&self) -> &[String] {
        &self.demands
    }
}

/// What a predicate needs from the statement it will sit in.
#[derive(Debug, Clone, Copy)]
pub struct PredicateCtx<'a> {
    pub window: WindowSql,
    /// The span table a `span:childCount` leaf reads (issue #594 part 1):
    /// unqualified in the live suite, `<db>.spans` in production.
    pub spans_table: &'a str,
    /// Unqualified in the live suite, `<db>.resources` in production. A
    /// trusted schema name, as `span_membership_sql`'s `spans_table` is.
    pub resources_table: &'a str,
}

/// Compiles one spanset filter body with no context. A `resource.` field
/// is refused: its subquery needs the request window.
///
/// A literal on the LEFT is accepted and the operator mirrored, so
/// `{ 500 <= span.http.response.status_code }` and
/// `{ span.http.response.status_code >= 500 }` are one rendering —
/// [`flip_comparison`] is the same reflection the shipped compiler uses.
pub fn compile_span_predicate(expr: &FieldExpr) -> Result<SpanPredicate, PlanError> {
    let mut c = Compiler::new(None);
    let e = c.render_expr(expr)?;
    Ok(c.finish(e))
}

/// [`compile_span_predicate`], in the statement's context.
pub fn compile_span_predicate_in(
    expr: &FieldExpr,
    ctx: &PredicateCtx<'_>,
) -> Result<SpanPredicate, PlanError> {
    let mut c = Compiler::new(Some(*ctx));
    let e = c.render_expr(expr)?;
    Ok(c.finish(e))
}

/// The leaf, for a caller that has already normalised to
/// `(field, op, value)` — `filter::compile_leaf`'s shape, so the two
/// compilers read against each other.
pub fn compile_span_leaf(
    field: &Field,
    op: ComparisonOp,
    value: &Value,
) -> Result<SpanPredicate, PlanError> {
    let mut c = Compiler::new(None);
    let e = c.leaf(field, op, value)?;
    Ok(c.finish(e))
}

/// [`compile_span_leaf`], in the statement's context.
pub fn compile_span_leaf_in(
    field: &Field,
    op: ComparisonOp,
    value: &Value,
    ctx: &PredicateCtx<'_>,
) -> Result<SpanPredicate, PlanError> {
    let mut c = Compiler::new(Some(*ctx));
    let e = c.leaf(field, op, value)?;
    Ok(c.finish(e))
}

/// Issue #591 part 2's section 3.2: `leaf` — a positive condition on one
/// `event.`/`link.` attribute or event and link intrinsic against a
/// literal-only side, or its presence — rendered as the predicate renders
/// it, with `arrayFirstIndex` where the predicate writes `arrayExists`: the
/// 1-based index of the first element whose own condition holds. `false`
/// when the condition holds on no span.
pub(super) fn element_index_in(
    leaf: &FieldExpr,
    ctx: &PredicateCtx<'_>,
) -> Result<String, PlanError> {
    let mut c = Compiler::new(Some(*ctx));
    c.head = Head::FirstIndex;
    c.render_expr(leaf)
}

/// Issue #591 part 2's section 3.5: `comparison`, holding one set field
/// once inside arithmetic, in select mode — the first element of the
/// field's set the comparison holds for, read through every binder outside
/// the loop. `false` when the comparison holds on no span.
pub(super) fn element_select_in(
    comparison: &FieldExpr,
    ctx: &PredicateCtx<'_>,
) -> Result<String, PlanError> {
    let mut c = Compiler::new(Some(*ctx));
    c.head = Head::Select;
    c.render_expr(comparison)
}

/// Issue #591 part 2's section 3.1: `V(k)`, the value of resource key
/// `key` at the span's own `resource_id`, read through `Q(k)`, the text
/// every resource operand of `key` reads.
pub(super) fn resource_value_in(key: &str, ctx: &PredicateCtx<'_>) -> Result<String, PlanError> {
    let mut c = Compiler::new(Some(*ctx));
    let q = c.resource_read(key)?;
    Ok(resource_value(&q))
}

/// Issue #591 part 2's section 3.3: `P_scope`, whether `scope` holds `key`
/// as the unscoped chain tests it ([`Compiler::chain_presence`]).
pub(super) fn chain_presence_in(
    scope: AttrScope,
    key: &str,
    ctx: &PredicateCtx<'_>,
) -> Result<String, PlanError> {
    let mut c = Compiler::new(Some(*ctx));
    c.chain_presence(scope, key)
}

/// The ONE place a window and a predicate compose. The live suite issues
/// it; `tests/traces_compile_v2.rs`'s `T-C4` freezes its text, so the
/// frozen text is what ran.
///
/// Issue it with `final = 1`, as every read on these tables is: `spans` is
/// a `ReplacingMergeTree` and a retried push collapses on the sorting key
/// only after a merge.
///
/// **A predicate compiled for another window panics.** A resource
/// subquery bounded by one window's days inside a statement bounded by
/// another is a wrong answer, not text to return.
///
/// **The returned column is aliased `id`, not `span_id`, and that is not a
/// cosmetic choice.** A select-list alias shadows a column of the same
/// name for the whole statement, the `WHERE` included, so
/// `… AS span_id … WHERE lower(hex(span_id)) < '…'` compares the HEX of
/// the hex text. Measured on 26.3.29.7 over a two-row `FixedString(8)`
/// table: `SELECT lower(hex(span_id)) AS span_id, lower(hex(span_id)) AS
/// twice` returns `0a1b2c3d4e5f60a1` beside
/// `30613162326333643465356636306131`, and the same statement with a
/// `lower(hex(span_id)) < '0a1b2c3d4e5f60a4'` bound answers nothing where
/// the un-aliased spelling answers one row. Any predicate reading
/// `span_id` — the whole `span:id` family — is wrong under that alias.
pub fn span_membership_sql(spans_table: &str, w: WindowSql, p: &SpanPredicate) -> String {
    assert!(
        p.composes_with(w),
        "span_membership_sql: the predicate was compiled for a different window"
    );
    format!(
        "SELECT lower(hex(span_id)) AS id\n\
         FROM {spans_table}\n\
         WHERE {time}\n\
         \x20 AND {bucket}\n\
         \x20 AND {day}\n\
         \x20 AND ({pred})\n\
         ORDER BY id",
        time = w.span_time_clause(),
        bucket = w.span_bucket_clause(),
        day = w.span_day_clause(),
        pred = p.sql(),
    )
}

// ---------------------------------------------------------------------
// the refusals
// ---------------------------------------------------------------------

/// The issue each deferred construct is served by.
const NESTED_AND_TRACE: &str = "#594";

/// The refusal every deferred construct takes: it names the construct and
/// the issue that serves it.
fn unsupported(construct: &str, target: &str) -> PlanError {
    PlanError::UnsupportedField(format!(
        "{construct} is not supported by the span-scope predicate compiler yet (issue {target})"
    ))
}

/// The four per-trace intrinsics as an operand (issue #594 part 1, D6):
/// refused, so today's engine answers until #602.
fn operand_refusal(intrinsic: Intrinsic) -> PlanError {
    PlanError::UnsupportedField(format!(
        "{intrinsic} as an operand is not supported by the search statement (issue #602)"
    ))
}

fn unsupported_intrinsic(intrinsic: Intrinsic, target: &str) -> PlanError {
    unsupported(&format!("{intrinsic}"), target)
}

/// A `resource.` or `.` field compiled with no [`PredicateCtx`]: both
/// read the resource table through a subquery bounded by the window.
fn needs_window(scope: AttrScope) -> PlanError {
    PlanError::UnsupportedField(format!(
        "the \"{scope}\" attribute scope needs the request window: compile it with \
         compile_span_predicate_in (issue #589)"
    ))
}

/// Arithmetic as the whole filter, or under `!`. The validator rejects it
/// first; this keeps the AST path total.
fn not_a_predicate() -> PlanError {
    PlanError::TypeMismatch("an arithmetic expression is not a predicate".to_string())
}

/// A boolean-valued node as an operand of arithmetic. The validator
/// rejects it first.
fn not_an_operand() -> PlanError {
    PlanError::TypeMismatch("a boolean-valued expression is not an arithmetic operand".to_string())
}

/// An integer literal outside −2^127…2^128 − 1, named with its sign
/// (decision 4 of the part-3c design, owner, 2026-10-06).
fn out_of_range(literal: &str) -> PlanError {
    PlanError::TypeMismatch(format!("integer literal out of range: {literal}"))
}

/// The most arithmetic nodes — binary operators and unary minus, after
/// folding — one predicate may hold. 64 nested `^` over 65 resource
/// operands render about 146,300 bytes and 30,200 syntax-tree elements,
/// inside the server's defaults; past about 165 nested nodes the server's
/// `max_ast_depth` of 1,000 refuses the statement.
const MAX_ARITH_NODES: usize = 64;

/// Whether `expr` is an arithmetic node — a binary arithmetic operator or
/// the unary minus, which is the same construct spelled as a prefix.
fn is_arithmetic(expr: &FieldExpr) -> bool {
    matches!(
        expr,
        FieldExpr::Binary {
            op: FieldOp::Arith(_),
            ..
        } | FieldExpr::Unary {
            op: UnaryOp::Neg,
            ..
        }
    )
}

/// The field under `!field`, or `None` for any other shape.
fn not_of_field(expr: &FieldExpr) -> Option<&Field> {
    match expr {
        FieldExpr::Unary {
            op: UnaryOp::Not,
            expr,
        } => match expr.as_ref() {
            FieldExpr::Field(f) => Some(f),
            _ => None,
        },
        _ => None,
    }
}

/// `(!operand) op literal` → the value `operand` must hold:
/// `= b` wants `!b`, `!= b` wants `b`, and anything else matches nothing
/// (`None`). The shipped rule, `filter.rs`'s `BoolMatch::of_comparison`.
fn bool_want(op: ComparisonOp, literal: &Value) -> Option<bool> {
    match (op, literal) {
        (ComparisonOp::Eq, Value::Bool(v)) => Some(!v),
        (ComparisonOp::Neq, Value::Bool(v)) => Some(*v),
        _ => None,
    }
}

// ---------------------------------------------------------------------
// the expression level
// ---------------------------------------------------------------------

/// The roots of the two span-row `JSON` columns an attribute is read from.
const ATTRS: &str = "attrs";
const SCOPE_ATTRS: &str = "scope_attrs";

/// The `JSON` element of the `events` and `links` arrays. On `spans` each
/// is an array of `JSON`, so ``events.attrs.`k`.:String`` is the typed
/// array of that one path, which reads that path only.
const EVENT_ATTRS: &str = "events.attrs";
const LINK_ATTRS: &str = "links.attrs";

/// The one resource key that is read from the span row: the writer stores
/// it in `spans.service`, with its arm in `spans.service_type`.
const SERVICE_NAME: &str = "service.name";

/// `Str`: the value was stored as a string, empty or not.
const SERVICE_IS_STRING: &str = "service_type = 'string'";
/// `P`: a value was stored, whatever its arm.
const SERVICE_PRESENT: &str = "service_type != ''";

/// One compilation: the context, whether a resource subquery was emitted,
/// and the `!` demands collected so far.
struct Compiler<'a> {
    ctx: Option<PredicateCtx<'a>>,
    uses_resources: bool,
    /// `(condition, message)` pairs in pre-order, each once.
    demands: Vec<(String, String)>,
    /// The arithmetic nodes emitted so far, across the whole predicate,
    /// for [`MAX_ARITH_NODES`].
    arith_nodes: usize,
    /// The current comparison's set operands, in pre-order, left side
    /// first (the part-3e design's section 5.1); saved and reset by each
    /// [`Compiler::compare`].
    occurrences: Vec<Occurrence>,
    /// The current comparison's scalar field leaves' presences, which
    /// `!=` over a set operand requires (decision 11 of the part-3e design).
    presences: Vec<String>,
    /// What the element loops render: the predicate, or one of the search
    /// projection's two element reads ([`element_index_in`],
    /// [`element_select_in`]).
    head: Head,
    /// The trace leaves compiled so far, each once (issue #594 part 1).
    trace_leaves: Vec<TraceLeaf>,
    /// Whether a `span:childCount` leaf was compiled.
    child_counts: bool,
}

/// The head of an element loop: `arrayExists` for the predicate; for the
/// search projection (issue #591 part 2), `arrayFirstIndex`, the first
/// element whose own condition holds, or select mode, the first element
/// itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Head {
    Exists,
    FirstIndex,
    Select,
}

impl Head {
    /// The function a leaf's element loop is rendered with.
    fn leaf_function(self) -> &'static str {
        match self {
            Head::Exists | Head::Select => "arrayExists",
            Head::FirstIndex => "arrayFirstIndex",
        }
    }
}

/// One set operand of a comparison, numbered `k` in pre-order: its element
/// variable `e<k>`, its set; a chain's variable `c<k>`, `C(k)`, and its
/// resource value's variable `cr<k>` with `V(k)`; and whether it is a `!F`
/// side, whose set must hold the key under `!=` (decision 14).
struct Occurrence {
    var: String,
    array: String,
    chain: Option<(String, String, (String, String))>,
    negated: bool,
}

/// The most set operands one comparison may hold (decision 13 of the
/// part-3e design, owner, 2026-10-06). Each one multiplies the work per
/// span by its elements: three 16-element set operands measured 487 µs a
/// span and 7.6 GB peak memory — 4,873 ms over 10,000 spans (26.3.29.7, 16
/// cores, `max_threads = 16`, `max_block_size = 65409`, `final = 1`,
/// `max_query_size = 8388608`, `max_execution_time = 20`, every other
/// setting the server default), against about 300 µs predicted from the
/// two-set rate of 75 ns a tuple.
const MAX_SET_OPERANDS: usize = 2;

/// Decision 13's refusal.
fn too_many_set_operands() -> PlanError {
    PlanError::UnsupportedField(format!(
        "a comparison with more than {MAX_SET_OPERANDS} event, link or unscoped operands is not \
         supported"
    ))
}

/// The set operands a side holds, each occurrence counted: a set field,
/// lone, under `!` or inside arithmetic. A boolean-valued node is a
/// comparison of its own and counts its own.
fn set_operands_in(expr: &FieldExpr) -> usize {
    match expr {
        FieldExpr::Field(field) => usize::from(is_set_field(field)),
        FieldExpr::Unary {
            op: UnaryOp::Not,
            expr: inner,
        } => match inner.as_ref() {
            FieldExpr::Field(field) => usize::from(is_set_field(field)),
            _ => 0,
        },
        FieldExpr::Unary {
            op: UnaryOp::Neg,
            expr: inner,
        } => set_operands_in(inner),
        FieldExpr::Binary {
            op: FieldOp::Arith(_),
            lhs,
            rhs,
        } => set_operands_in(lhs) + set_operands_in(rhs),
        FieldExpr::Literal(_)
        | FieldExpr::Exists { .. }
        | FieldExpr::Binary {
            op: FieldOp::Cmp(_) | FieldOp::Bool(_),
            ..
        } => 0,
    }
}

/// A lambda variable and the value it is bound to.
type Bind = (String, String);

/// Whether `var` is an arithmetic side's variable, `a1` or `a2`, whose
/// tuple may read a set operand's element and so is bound inside the
/// loops.
fn is_arith_var(var: &str) -> bool {
    matches!(var, "a1" | "a2")
}

impl<'a> Compiler<'a> {
    fn new(ctx: Option<PredicateCtx<'a>>) -> Self {
        Compiler {
            ctx,
            uses_resources: false,
            demands: Vec::new(),
            arith_nodes: 0,
            occurrences: Vec::new(),
            presences: Vec::new(),
            head: Head::Exists,
            trace_leaves: Vec::new(),
            child_counts: false,
        }
    }

    /// The predicate: `E` alone, or — with demands — every `throwIf`
    /// summed with `E`. `plus` does not short-circuit, so every demand is
    /// evaluated for every row that reaches the predicate, wherever its
    /// `!f` sits in `E`; inside `AND`/`OR` it would be skipped.
    fn finish(self, e: String) -> SpanPredicate {
        let sql = if self.demands.is_empty() {
            e
        } else {
            let mut sql = String::from("(");
            for (condition, message) in &self.demands {
                write!(
                    sql,
                    "throwIf({condition}, {}) + ",
                    escape::ch_string(message)
                )
                .expect("writing to a String cannot fail");
            }
            format!("{sql}toUInt8({e})) = 1")
        };
        SpanPredicate {
            sql,
            window: if self.uses_resources {
                self.ctx.map(|c| c.window)
            } else {
                None
            },
            demands: self.demands.into_iter().map(|(_, m)| m).collect(),
            trace_leaves: self.trace_leaves,
            child_counts: self.child_counts,
        }
    }

    /// `R(p)`: the spans whose resource row, within the window's days,
    /// satisfies `p`. Inline, so the whole predicate is one statement.
    fn resource_subquery(&mut self, p: &str) -> Result<String, PlanError> {
        let ctx = self.ctx.ok_or_else(|| needs_window(AttrScope::Resource))?;
        self.uses_resources = true;
        Ok(format!(
            "resource_id IN (SELECT resource_id FROM {} WHERE {} AND ({p}))",
            ctx.resources_table,
            ctx.window.resources_day_clause()
        ))
    }

    /// `Q(k)`: one uncorrelated scalar subquery over the resource rows in
    /// the window's days that hold `key` — their ids, their values and the
    /// set of types those values are stored as. Every occurrence of one
    /// key's text is the same, so the engine evaluates it once per
    /// statement.
    fn resource_read(&mut self, key: &str) -> Result<String, PlanError> {
        let ctx = self.ctx.ok_or_else(|| needs_window(AttrScope::Resource))?;
        self.uses_resources = true;
        let path = attr_path(ATTRS, key);
        Ok(format!(
            "(SELECT (groupArray(resource_id), groupArray({path}), \
             groupUniqArray(dynamicType({path}))) FROM {} WHERE {} AND \
             dynamicType({path}) != 'None')",
            ctx.resources_table,
            ctx.window.resources_day_clause()
        ))
    }

    fn demand(&mut self, condition: String, message: String) {
        let pair = (condition, message);
        if !self.demands.contains(&pair) {
            self.demands.push(pair);
        }
    }

    /// Every operand of `&&`/`||` is wrapped in one pair of parentheses, so
    /// the SQL grouping is the parse tree's and SQL's own `AND`-before-`OR`
    /// precedence never applies.
    fn render_expr(&mut self, expr: &FieldExpr) -> Result<String, PlanError> {
        match expr {
            FieldExpr::Binary {
                op: FieldOp::Cmp(op),
                lhs,
                rhs,
            } => self.compare(lhs, *op, rhs),
            FieldExpr::Binary {
                op: FieldOp::Bool(op),
                lhs,
                rhs,
            } => {
                let l = self.render_expr(lhs)?;
                let r = self.render_expr(rhs)?;
                Ok(match op {
                    BoolOp::And => format!("({l}) AND ({r})"),
                    BoolOp::Or => format!("({l}) OR ({r})"),
                })
            }
            FieldExpr::Binary {
                op: FieldOp::Arith(_),
                ..
            }
            | FieldExpr::Unary {
                op: UnaryOp::Neg, ..
            } => Err(not_a_predicate()),
            FieldExpr::Unary {
                op: UnaryOp::Not,
                expr: inner,
            } => match inner.as_ref() {
                FieldExpr::Field(field) => self.not_field(field, Some(false), false),
                e if is_arithmetic(e) => Err(not_a_predicate()),
                // Two-valued: every leaf this compiler emits is non-`NULL`,
                // so a span lacking the key matches `NOT (E)`.
                e => Ok(format!("NOT ({})", self.render_expr(e)?)),
            },
            // `{ span.k }` is TRUTHINESS, not presence: the reference matches
            // a span only where the value IS `true`, so this is plain
            // equality against the boolean literal and inherits the leaf's
            // rendering.
            FieldExpr::Field(field) => self.leaf(field, ComparisonOp::Eq, &Value::Bool(true)),
            // `{ true }` is exactly `{ }` and `{ false }` matches nothing. A
            // non-boolean static never reaches here from the wire —
            // `pulsus_traceql::validate` rejects it with the reference's own
            // message — and is refused so the AST-constructed path stays
            // total.
            FieldExpr::Literal(Value::Bool(b)) => Ok(if *b { "true" } else { "false" }.to_string()),
            FieldExpr::Literal(_) => Err(PlanError::TypeMismatch(
                "a non-boolean literal is not a predicate".to_string(),
            )),
            FieldExpr::Exists { field, negated } => self.exists(field, *negated),
        }
    }

    /// `!field`, alone (`want = Some(false)`) or as `(!field) op literal`.
    ///
    /// It matches a span whose value IS `want`; absent matches nothing; and
    /// a present non-boolean fails the whole statement with
    /// `expression (!<field>) expected a boolean`, as the shipped evaluator
    /// does. The failure is a demand, collected here and lifted out of the
    /// expression by [`Compiler::finish`].
    ///
    /// `all` is `(!field) != literal`: an event or link field then matches
    /// only when it holds the key and EVERY element is `want` — no element
    /// is its opposite (decision 7 of the part-3d design; the 2026-09-18
    /// rule of `traceql-event-link-operand-any-match`).
    fn not_field(
        &mut self,
        field: &Field,
        want: Option<bool>,
        all: bool,
    ) -> Result<String, PlanError> {
        let (scope, key) = match field {
            Field::Intrinsic(intrinsic) => {
                return Err(PlanError::TypeMismatch(format!(
                    "expression (!{intrinsic}) expected a boolean"
                )));
            }
            Field::Attribute { scope, key } => (*scope, key),
        };
        let (matches, condition) = self.not_parts(scope, key, want, all)?;
        self.demand(
            condition,
            format!("expression (!{scope}{key}) expected a boolean"),
        );
        Ok(matches)
    }

    /// `!<scope>key`'s two halves at one scope — what matches `want`, and
    /// the demand condition — with no demand registered, so the `.` chain
    /// can assemble its own from them.
    fn not_parts(
        &mut self,
        scope: AttrScope,
        key: &str,
        want: Option<bool>,
        all: bool,
    ) -> Result<(String, String), PlanError> {
        Ok(match scope {
            AttrScope::Span => not_attr(ATTRS, key, want),
            AttrScope::Instrumentation => not_attr(SCOPE_ATTRS, key, want),
            AttrScope::Event => not_element(EVENT_ATTRS, key, want, all),
            AttrScope::Link => not_element(LINK_ATTRS, key, want, all),
            AttrScope::Resource if key == SERVICE_NAME => {
                self.ctx.ok_or_else(|| needs_window(AttrScope::Resource))?;
                (
                    want.map_or_else(
                        || "false".to_string(),
                        |w| format!("(service_type = 'bool' AND service = '{w}')"),
                    ),
                    "service_type != '' AND service_type != 'bool'".to_string(),
                )
            }
            AttrScope::Resource => {
                let (matches, condition) = not_attr(ATTRS, key, want);
                let matches = match want {
                    Some(_) => self.resource_subquery(&matches)?,
                    None => matches,
                };
                (matches, self.resource_subquery(&condition)?)
            }
            AttrScope::Unscoped => self.not_chain(key, want, all)?,
        })
    }

    /// `!.key`: section 5.1's chain over each scope's `T` and over each
    /// scope's demand condition, both defaulting to `false`.
    fn not_chain(
        &mut self,
        key: &str,
        want: Option<bool>,
        all: bool,
    ) -> Result<(String, String), PlanError> {
        self.ctx.ok_or_else(|| needs_window(AttrScope::Unscoped))?;
        let mut t_args = Vec::with_capacity(2 * CHAIN.len());
        let mut c_args = Vec::with_capacity(2 * CHAIN.len());
        for scope in CHAIN {
            let present = self.chain_presence(scope, key)?;
            let (t, c) = self.not_parts(scope, key, want, all)?;
            t_args.push(present.clone());
            t_args.push(t);
            c_args.push(present);
            c_args.push(c);
        }
        let matches = match want {
            Some(_) => format!("multiIf({}, false)", t_args.join(", ")),
            None => "false".to_string(),
        };
        Ok((matches, format!("multiIf({}, false)", c_args.join(", "))))
    }

    /// `{ .k != nil }` (presence) and `{ .k = nil }` (absence).
    ///
    /// `dynamicType` and not a typed read: ``attrs.`k`.:String IS NOT NULL``
    /// answers only the rows that store a string, so it misses an integer, a
    /// boolean and an array. The two spellings' meanings differ, which is why
    /// the AST carries the negation rather than a canonical form.
    fn exists(&mut self, field: &Field, negated: bool) -> Result<String, PlanError> {
        let (scope, key) = match field {
            Field::Attribute { scope, key } => (*scope, key),
            Field::Intrinsic(_) => {
                return Err(PlanError::TypeMismatch(
                    "existence checks are only supported on attributes".to_string(),
                ));
            }
        };
        self.scoped_exists(scope, key, negated)
    }

    fn scoped_exists(
        &mut self,
        scope: AttrScope,
        key: &str,
        negated: bool,
    ) -> Result<String, PlanError> {
        let op = if negated { "=" } else { "!=" };
        match scope {
            AttrScope::Span => Ok(format!(
                "dynamicType({}) {op} 'None'",
                attr_path(ATTRS, key)
            )),
            AttrScope::Instrumentation => Ok(format!(
                "dynamicType({}) {op} 'None'",
                attr_path(SCOPE_ATTRS, key)
            )),
            // Any element holds the key. Absence is `NOT` outside, so a span
            // carrying no elements lacks it.
            AttrScope::Event => Ok(element_exists(
                EVENT_ATTRS,
                key,
                negated,
                self.head.leaf_function(),
            )),
            AttrScope::Link => Ok(element_exists(
                LINK_ATTRS,
                key,
                negated,
                self.head.leaf_function(),
            )),
            // Presence reads the arm, never the text: `service` is `''` for
            // an empty string and for no key alike.
            AttrScope::Resource if key == SERVICE_NAME => {
                self.ctx.ok_or_else(|| needs_window(AttrScope::Resource))?;
                Ok(if negated {
                    format!("NOT ({SERVICE_PRESENT})")
                } else {
                    SERVICE_PRESENT.to_string()
                })
            }
            // Absence is `NOT` outside the subquery, so a span whose
            // resource row is not visible counts as lacking the key.
            AttrScope::Resource => {
                let present = self.resource_subquery(&format!(
                    "dynamicType({}) != 'None'",
                    attr_path(ATTRS, key)
                ))?;
                Ok(if negated {
                    format!("NOT ({present})")
                } else {
                    present
                })
            }
            // Present at any scope of the chain.
            AttrScope::Unscoped => {
                self.ctx.ok_or_else(|| needs_window(AttrScope::Unscoped))?;
                let mut any = Vec::with_capacity(CHAIN.len());
                for scope in CHAIN {
                    any.push(format!("({})", self.chain_presence(scope, key)?));
                }
                let any = any.join(" OR ");
                Ok(if negated { format!("NOT ({any})") } else { any })
            }
        }
    }

    fn leaf(
        &mut self,
        field: &Field,
        op: ComparisonOp,
        value: &Value,
    ) -> Result<String, PlanError> {
        match field {
            Field::Attribute { scope, key } => self.scoped_leaf(*scope, key, op, value),
            Field::Intrinsic(
                i @ (Intrinsic::TraceDuration | Intrinsic::RootName | Intrinsic::RootServiceName),
            ) => {
                let (sql, leaf) = trace_leaf(*i, op, value)?;
                if !self.trace_leaves.contains(&leaf) {
                    self.trace_leaves.push(leaf);
                }
                Ok(sql)
            }
            Field::Intrinsic(Intrinsic::ChildCount) => self.child_count_leaf(op, value),
            Field::Intrinsic(intrinsic) => {
                intrinsic_leaf(*intrinsic, op, value, self.head.leaf_function())
            }
        }
    }

    /// `span:childCount op n` (issue #594 part 1). Its subquery reads the
    /// window's buckets, so the predicate is tied to the window, as a
    /// resource subquery ties it.
    fn child_count_leaf(&mut self, op: ComparisonOp, value: &Value) -> Result<String, PlanError> {
        let Value::Number(raw) = value else {
            return Err(type_refusal(Intrinsic::ChildCount, op, value));
        };
        if sql_op(op).is_none() {
            return Err(type_refusal(Intrinsic::ChildCount, op, value));
        }
        let ctx = self.ctx.ok_or_else(|| {
            PlanError::UnsupportedField(
                "span:childCount needs the request window: compile it with \
                 compile_span_predicate_in (issue #594)"
                    .to_string(),
            )
        })?;
        let sql = child_leaf(op, value, &render_number(raw)?, ctx.window, ctx.spans_table)?;
        self.uses_resources = true;
        self.child_counts = true;
        Ok(sql)
    }

    fn scoped_leaf(
        &mut self,
        scope: AttrScope,
        key: &str,
        op: ComparisonOp,
        value: &Value,
    ) -> Result<String, PlanError> {
        match scope {
            AttrScope::Span => attr_leaf(ATTRS, key, op, value),
            AttrScope::Instrumentation => attr_leaf(SCOPE_ATTRS, key, op, value),
            AttrScope::Resource => self.resource_leaf(key, op, value),
            AttrScope::Event => {
                element_leaf(EVENT_ATTRS, key, op, value, self.head.leaf_function())
            }
            AttrScope::Link => element_leaf(LINK_ATTRS, key, op, value, self.head.leaf_function()),
            AttrScope::Unscoped => self.unscoped_leaf(key, op, value),
        }
    }

    /// `resource.k <op> v`: the span-scope rendering inside `R(…)`, with a
    /// negated rendering's `NOT` moved outside it.
    fn resource_leaf(
        &mut self,
        key: &str,
        op: ComparisonOp,
        value: &Value,
    ) -> Result<String, PlanError> {
        self.ctx.ok_or_else(|| needs_window(AttrScope::Resource))?;
        if key == SERVICE_NAME {
            return service_name_leaf(op, value);
        }
        let (negated, positive) = attr_leaf_parts(ATTRS, key, op, value)?;
        let r = self.resource_subquery(&positive)?;
        Ok(if negated { format!("NOT ({r})") } else { r })
    }

    /// `.k <op> v`: the value is the first scope's in the chain that holds
    /// the key — `multiIf(P_span, X_span, …, P_instrumentation,
    /// X_instrumentation, D)` — and a span lacking it everywhere matches
    /// only a negated operator. A refusal is `span.k`'s.
    fn unscoped_leaf(
        &mut self,
        key: &str,
        op: ComparisonOp,
        value: &Value,
    ) -> Result<String, PlanError> {
        self.ctx.ok_or_else(|| needs_window(AttrScope::Unscoped))?;
        let (negated, _) = attr_leaf_parts(ATTRS, key, op, value)?;
        let mut args = Vec::with_capacity(2 * CHAIN.len());
        for scope in CHAIN {
            args.push(self.chain_presence(scope, key)?);
            args.push(if scope == AttrScope::Resource && key == SERVICE_NAME {
                self.service_name_in_chain(op, value)?
            } else {
                self.scoped_leaf(scope, key, op, value)?
            });
        }
        let default = if negated { "true" } else { "false" };
        Ok(format!("multiIf({}, {default})", args.join(", ")))
    }

    /// `P_sc`: whether `scope` holds `key`, as the chain tests it.
    ///
    /// At `service.name` the resource scope is present when the span row
    /// holds a string (the writer moves a non-empty string there, and stores
    /// every other arm in `resources.attrs`) or the resource row holds any
    /// value.
    fn chain_presence(&mut self, scope: AttrScope, key: &str) -> Result<String, PlanError> {
        if scope == AttrScope::Resource && key == SERVICE_NAME {
            let r = self.resource_subquery(&format!(
                "dynamicType({}) != 'None'",
                attr_path(ATTRS, SERVICE_NAME)
            ))?;
            return Ok(format!("({SERVICE_IS_STRING} OR {r})"));
        }
        self.scoped_exists(scope, key, false)
    }

    /// `X_resource` at `service.name` in the chain: a string is compared on
    /// the span row, gated on its arm, OR in the resource row; every other
    /// operand in the resource row alone. A negation goes outside both.
    fn service_name_in_chain(
        &mut self,
        op: ComparisonOp,
        value: &Value,
    ) -> Result<String, PlanError> {
        let (negated, p) = attr_leaf_parts(ATTRS, SERVICE_NAME, op, value)?;
        let r = self.resource_subquery(&p)?;
        let Value::String(s) = value else {
            return Ok(if negated { format!("NOT ({r})") } else { r });
        };
        let g = match positive_op(op).1 {
            ComparisonOp::Re => format!("match(service, {})", anchored_regex_sql(s)?),
            other => format!(
                "service {} {}",
                sql_op(other).expect("the six non-regex operators"),
                escape::ch_string(s)
            ),
        };
        let x = format!("(({SERVICE_IS_STRING} AND {g}) OR {r})");
        Ok(if negated { format!("NOT {x}") } else { x })
    }

    /// `L op R`, in the order of the part-3c design's section 5.1: the
    /// first rule that applies decides.
    ///
    /// 0. an integer literal out of range anywhere in either side is
    ///    refused;
    /// 1. `(!field) op literal` is [`Compiler::not_field`];
    /// 2. a lone field against a plain literal is the leaf;
    /// 3. a lone attribute or event and link intrinsic against a
    ///    literal-only side that folds to a constant is the leaf over that
    ///    constant, and a set operand ([`is_set_field`]) against one that
    ///    folds to no value is `false` (decision 8 of the part-3d design);
    /// 4. a deferred operand is refused, pre-order, left side first — after
    ///    the regex operators when both sides are lone fields, which is
    ///    part 3a's order; between the two, two lone fields of which either
    ///    is a set are [`Compiler::set_compare`] (the part-3d design's
    ///    section 5.6);
    /// 5. a literal-only subtree that folds to no value makes it `false`;
    /// 6. a regex operator takes two string literals and nothing else;
    /// 7. otherwise each side's arms ([`Compiler::side_arms`]) through
    ///    [`field_terms`], bound once each.
    ///
    /// An operand read from the resource row, and an arithmetic side, is
    /// bound once: the body is a lambda over its variable, applied to a
    /// one-element array holding the value, so every arm reads the
    /// variable rather than repeating the text.
    fn compare(
        &mut self,
        lhs: &FieldExpr,
        op: ComparisonOp,
        rhs: &FieldExpr,
    ) -> Result<String, PlanError> {
        // Each comparison numbers its own set operands and collects its own
        // presences: a boolean-valued side is a comparison of its own,
        // compiled in the middle of this one's.
        let occurrences = std::mem::take(&mut self.occurrences);
        let presences = std::mem::take(&mut self.presences);
        let compared = self.compare_in_frame(lhs, op, rhs);
        self.occurrences = occurrences;
        self.presences = presences;
        compared
    }

    /// [`Compiler::compare`] within the comparison's own frame.
    fn compare_in_frame(
        &mut self,
        lhs: &FieldExpr,
        op: ComparisonOp,
        rhs: &FieldExpr,
    ) -> Result<String, PlanError> {
        check_literals(lhs)?;
        check_literals(rhs)?;
        if let (Some(field), FieldExpr::Literal(value)) = (not_of_field(lhs), rhs) {
            return self.not_field(field, bool_want(op, value), op == ComparisonOp::Neq);
        }
        if let (FieldExpr::Literal(value), Some(field)) = (lhs, not_of_field(rhs)) {
            let op = flip_comparison(op);
            return self.not_field(field, bool_want(op, value), op == ComparisonOp::Neq);
        }
        match (lhs, rhs) {
            (FieldExpr::Field(field), FieldExpr::Literal(value)) => {
                return self.leaf(field, op, value);
            }
            (FieldExpr::Literal(value), FieldExpr::Field(field)) => {
                return self.leaf(field, flip_comparison(op), value);
            }
            _ => {}
        }
        for (field, other, op) in [(lhs, rhs, op), (rhs, lhs, flip_comparison(op))] {
            let FieldExpr::Field(field) = field else {
                continue;
            };
            if !folds_to_leaf(field) {
                continue;
            }
            match fold(other)? {
                Folded::Undefined if is_set_field(field) && literal_only(other) => {
                    return Ok("false".to_string());
                }
                folded => {
                    if let Some(value) = folded.constant() {
                        return self.leaf(field, op, &value);
                    }
                }
            }
        }
        let regex = matches!(op, ComparisonOp::Re | ComparisonOp::Nre);
        let two_fields = matches!((lhs, rhs), (FieldExpr::Field(_), FieldExpr::Field(_)));
        if two_fields && regex {
            return Err(PlanError::TypeMismatch(
                "a field-against-field comparison does not support regex operators".to_string(),
            ));
        }
        if let (FieldExpr::Field(l), FieldExpr::Field(r)) = (lhs, rhs)
            && (is_set_field(l) || is_set_field(r))
        {
            return self.set_compare(l, op, r);
        }
        self.refuse_deferred(lhs, false)?;
        self.refuse_deferred(rhs, false)?;
        // Decision 13's cap holds before a side folding to no value makes
        // the comparison `false`, so no shape of a comparison escapes it.
        if set_operands_in(lhs) + set_operands_in(rhs) > MAX_SET_OPERANDS {
            return Err(too_many_set_operands());
        }
        if fold(lhs)? == Folded::Undefined || fold(rhs)? == Folded::Undefined {
            return Ok("false".to_string());
        }
        if regex {
            return match (lhs, rhs) {
                (FieldExpr::Literal(Value::String(l)), FieldExpr::Literal(Value::String(r))) => {
                    string_column_text(&escape::ch_string(l), op, r)
                }
                (FieldExpr::Literal(_), _) | (_, FieldExpr::Literal(_)) => {
                    Err(PlanError::TypeMismatch(
                        "a regex operator against a static operand is not supported".to_string(),
                    ))
                }
                _ => Err(PlanError::TypeMismatch(
                    "a regex operator compares a field with a string literal".to_string(),
                )),
            };
        }
        let mut fields = 0usize;
        let Some(l) = self.side_arms(lhs, 1, &mut fields)? else {
            return Ok("false".to_string());
        };
        let Some(r) = self.side_arms(rhs, 2, &mut fields)? else {
            return Ok("false".to_string());
        };
        let body = field_terms(&l, op, &r);
        if !self.occurrences.is_empty() {
            return Ok(self.occurrence_build(op, &l, &r, body));
        }
        if body == "false" {
            return Ok(body);
        }
        Ok(bind_values(&l.binds, &r.binds, body))
    }

    /// The part-3e design's section 5.4: `body` inside the arithmetic
    /// sides' bindings, then one loop per set operand, last first — any
    /// tuple of their elements, or every tuple for `!=` (decision 10) —
    /// then, for `!=`, the presences of decision 11, then the chains'
    /// sets, then the lone resource fields' values and the chains'.
    ///
    /// With no term nothing matches under `= < <= > >=`; `!=` is built
    /// over a body of `false`, so it holds when a set is empty (decision 9).
    ///
    /// In select mode (issue #591 part 2's section 3.5) the loop is
    /// `arrayFirst`, the first element the comparison holds for, and each
    /// binder outside it reads that element: `arrayElement(arrayMap(…), 1)`.
    /// The arithmetic sides' binders inside the loop stay `arrayExists`:
    /// they are the loop's condition.
    fn occurrence_build(&self, op: ComparisonOp, l: &Arms, r: &Arms, body: String) -> String {
        let neq = op == ComparisonOp::Neq;
        let select = self.head == Head::Select;
        if body == "false" && !neq {
            return body;
        }
        let (arith, values): (Vec<Bind>, Vec<Bind>) = l
            .binds
            .iter()
            .chain(&r.binds)
            .cloned()
            .partition(|(var, _)| is_arith_var(var));
        let mut text = bind_values(&arith, &[], body);
        for occ in self.occurrences.iter().rev() {
            let each = if select {
                "arrayFirst"
            } else if neq {
                "arrayAll"
            } else {
                "arrayExists"
            };
            text = format!("{each}({} -> {text}, {})", occ.var, occ.array);
        }
        if neq {
            // The scalar leaves', then the chains', then the `!F` sets'.
            let chains = self
                .occurrences
                .iter()
                .filter_map(|o| o.chain.as_ref().map(|(c, _, _)| format!("notEmpty({c})")));
            let negated = self
                .occurrences
                .iter()
                .filter(|o| o.negated)
                .map(|o| format!("notEmpty({})", o.array));
            let mut presences: Vec<String> = Vec::new();
            for p in self.presences.iter().cloned().chain(chains).chain(negated) {
                if !presences.contains(&p) {
                    presences.push(p);
                }
            }
            if !presences.is_empty() {
                text = format!("({} AND {text})", presences.join(" AND "));
            }
        }
        for occ in self.occurrences.iter().rev() {
            if let Some((c, chain, _)) = &occ.chain {
                text = if select {
                    format!("arrayElement(arrayMap({c} -> {text}, [{chain}]), 1)")
                } else {
                    format!("arrayExists({c} -> {text}, [{chain}])")
                };
            }
        }
        let chain_values: Vec<(String, String)> = self
            .occurrences
            .iter()
            .filter_map(|o| o.chain.as_ref().map(|(_, _, value)| value.clone()))
            .collect();
        if select {
            select_values(&values, &chain_values, text)
        } else {
            bind_values(&values, &chain_values, text)
        }
    }

    /// Registers the set operand `field` as the comparison's next
    /// occurrence, `k` (the part-3e design's section 5.1), and returns its
    /// set, its element arms reading `e<k>` with no binding of their own.
    /// A third occurrence is refused (decision 13).
    fn register_occurrence(
        &mut self,
        field: &Field,
        negated: bool,
    ) -> Result<SetOperand, PlanError> {
        if self.occurrences.len() >= MAX_SET_OPERANDS {
            return Err(too_many_set_operands());
        }
        let k = self.occurrences.len() + 1;
        let cr = format!("cr{k}");
        let mut set = self
            .set_operand(field, k, &cr)?
            .expect("register_occurrence is called with a set operand");
        let value = set.arms.binds.pop();
        self.occurrences.push(Occurrence {
            var: set.var.clone(),
            array: set.array.clone(),
            chain: set
                .chain
                .clone()
                .map(|(c, chain)| (c, chain, value.expect("a chain reads its resource value"))),
            negated,
        });
        Ok(set)
    }

    /// Notes a scalar field leaf's presence for `!=` (decision 11).
    fn note_presence(&mut self, presence: Option<String>) {
        if let Some(p) = presence {
            self.presences.push(p);
        }
    }

    /// Section 5.1's row 4: the first operand of `expr` this part does not
    /// serve, pre-order, refused. A boolean-valued node is a comparison of
    /// its own, rendered — and refused — by [`Compiler::render_expr`],
    /// except inside arithmetic, where it is not an operand.
    fn refuse_deferred(&self, expr: &FieldExpr, in_arithmetic: bool) -> Result<(), PlanError> {
        match expr {
            FieldExpr::Field(field) => self.refuse_field(field),
            FieldExpr::Literal(_) => Ok(()),
            FieldExpr::Binary {
                op: FieldOp::Arith(_),
                lhs,
                rhs,
            } => {
                self.refuse_deferred(lhs, true)?;
                self.refuse_deferred(rhs, true)
            }
            FieldExpr::Unary {
                op: UnaryOp::Neg,
                expr: inner,
            } => self.refuse_deferred(inner, true),
            _ if in_arithmetic => Err(not_an_operand()),
            FieldExpr::Unary {
                op: UnaryOp::Not,
                expr: inner,
            } => match inner.as_ref() {
                FieldExpr::Field(field) => self.refuse_field(field),
                _ => Ok(()),
            },
            FieldExpr::Binary {
                op: FieldOp::Cmp(_) | FieldOp::Bool(_),
                ..
            }
            | FieldExpr::Exists { .. } => Ok(()),
        }
    }

    /// One operand's refusal, if this part does not serve it. **No
    /// wildcard arm**.
    fn refuse_field(&self, field: &Field) -> Result<(), PlanError> {
        match field {
            Field::Attribute { scope, .. } => match scope {
                AttrScope::Span
                | AttrScope::Instrumentation
                | AttrScope::Event
                | AttrScope::Link
                | AttrScope::Unscoped => Ok(()),
                AttrScope::Resource => self
                    .ctx
                    .map(|_| ())
                    .ok_or_else(|| needs_window(AttrScope::Resource)),
            },
            Field::Intrinsic(intrinsic) => match intrinsic {
                Intrinsic::Name
                | Intrinsic::StatusMessage
                | Intrinsic::SpanId
                | Intrinsic::ParentId
                | Intrinsic::TraceId
                | Intrinsic::InstrumentationName
                | Intrinsic::InstrumentationVersion
                | Intrinsic::Duration
                | Intrinsic::Status
                | Intrinsic::Kind
                | Intrinsic::EventName
                | Intrinsic::EventTimeSinceStart
                | Intrinsic::LinkSpanId
                | Intrinsic::LinkTraceId => Ok(()),
                Intrinsic::NestedSetParent
                | Intrinsic::NestedSetLeft
                | Intrinsic::NestedSetRight => {
                    Err(unsupported_intrinsic(*intrinsic, NESTED_AND_TRACE))
                }
                Intrinsic::ChildCount
                | Intrinsic::TraceDuration
                | Intrinsic::RootName
                | Intrinsic::RootServiceName => Err(operand_refusal(*intrinsic)),
            },
        }
    }

    /// One side's arms (the part-3c design's section 5.2), or `None` when
    /// the side has no value on any span, so the comparison is `false`.
    ///
    /// `n` is the side, 1 left and 2 right, which names an arithmetic
    /// side's variable `a<n>`; `fields` counts the field operands outside
    /// arithmetic so far, which names a resource operand's `r<n>`.
    fn side_arms(
        &mut self,
        side: &FieldExpr,
        n: usize,
        fields: &mut usize,
    ) -> Result<Option<Arms>, PlanError> {
        match side {
            FieldExpr::Field(field) => {
                *fields += 1;
                if is_set_field(field) {
                    return Ok(Some(self.register_occurrence(field, false)?.arms));
                }
                let var = format!("r{fields}");
                self.note_presence(scalar_presence(field, &var));
                self.operand_arms(field, var).map(Some)
            }
            FieldExpr::Literal(value) => literal_arms(value).map(Some),
            FieldExpr::Binary {
                op: FieldOp::Arith(_),
                ..
            }
            | FieldExpr::Unary {
                op: UnaryOp::Neg, ..
            } => {
                if let Some(arms) = fold(side)?.constant_arms() {
                    return Ok(Some(arms));
                }
                let Some(tuple) = self.arith_tuple(side)? else {
                    return Ok(None);
                };
                let var = format!("a{n}");
                Ok(Some(Arms {
                    i: Some(format!("tupleElement({var}, 1)")),
                    f: Some(format!("tupleElement({var}, 2)")),
                    nullable: true,
                    binds: vec![(var, tuple.text)],
                    ..Arms::default()
                }))
            }
            FieldExpr::Unary {
                op: UnaryOp::Not,
                expr: inner,
            } if matches!(inner.as_ref(), FieldExpr::Field(_)) => {
                let FieldExpr::Field(field) = inner.as_ref() else {
                    unreachable!("matched as a field above")
                };
                *fields += 1;
                self.not_operand(field, format!("r{fields}")).map(Some)
            }
            FieldExpr::Unary {
                op: UnaryOp::Not, ..
            }
            | FieldExpr::Binary {
                op: FieldOp::Cmp(_) | FieldOp::Bool(_),
                ..
            }
            | FieldExpr::Exists { .. } => {
                // Two-valued, as every predicate this compiler emits: a
                // span lacking the operand compares as `false`.
                let b = self.render_expr(side)?;
                Ok(Some(Arms {
                    b: Some(format!("({b})")),
                    ..Arms::default()
                }))
            }
        }
    }

    /// `!field` as a side: its boolean arm, `NULL` where the value is not
    /// a boolean, with `not_field`'s demand that a present non-boolean
    /// fails the statement. An `event.`, `link.` or `.` field is a set
    /// operand, each element negated (the part-3e design's section 5.3).
    fn not_operand(&mut self, field: &Field, var: String) -> Result<Arms, PlanError> {
        let (scope, key) = match field {
            Field::Intrinsic(intrinsic) => {
                return Err(PlanError::TypeMismatch(format!(
                    "expression (!{intrinsic}) expected a boolean"
                )));
            }
            Field::Attribute { scope, key } => (*scope, key),
        };
        let arms = match scope {
            AttrScope::Span | AttrScope::Instrumentation => {
                let root = if scope == AttrScope::Span {
                    ATTRS
                } else {
                    SCOPE_ATTRS
                };
                self.note_presence(Some(format!(
                    "dynamicType({}) != 'None'",
                    attr_path(root, key)
                )));
                Arms {
                    b: Some(format!(
                        "(NOT {}{})",
                        attr_path(root, key),
                        Read::Bool.suffix()
                    )),
                    nullable: true,
                    ..Arms::default()
                }
            }
            AttrScope::Resource if key == SERVICE_NAME => {
                self.ctx.ok_or_else(|| needs_window(AttrScope::Resource))?;
                // Held when the resource row holds the key (decision 11):
                // a boolean is stored there, not on the span row.
                let q = self.resource_read(key)?;
                self.note_presence(Some(format!(
                    "dynamicType({}) != 'None'",
                    resource_value(&q)
                )));
                Arms {
                    b: Some("if(service_type = 'bool', service = 'false', NULL)".to_string()),
                    nullable: true,
                    ..Arms::default()
                }
            }
            AttrScope::Resource => {
                let q = self.resource_read(key)?;
                self.note_presence(Some(format!("dynamicType({var}) != 'None'")));
                Arms {
                    b: Some(format!("(NOT dynamicElement({var}, 'Bool'))")),
                    nullable: true,
                    types: Some(format!("tupleElement({q}, 3)")),
                    binds: vec![(var, resource_value(&q))],
                    ..Arms::default()
                }
            }
            AttrScope::Event | AttrScope::Link | AttrScope::Unscoped => {
                let set = self.register_occurrence(field, true)?;
                Arms {
                    b: Some(format!("(NOT dynamicElement({}, 'Bool'))", set.var)),
                    nullable: true,
                    ..Arms::default()
                }
            }
        };
        let (_, condition) = self.not_parts(scope, key, None, false)?;
        self.demand(
            condition,
            format!("expression (!{scope}{key}) expected a boolean"),
        );
        Ok(arms)
    }

    /// One arithmetic value as a `(Nullable(Int256), Nullable(Float64))`
    /// tuple, at most one element non-`NULL` (the part-3c design's section
    /// 5.3); `None` when it has no value on any span. A subtree that folds
    /// to a constant is that constant and is not a node. Each node is
    /// counted, across the predicate, for [`MAX_ARITH_NODES`].
    ///
    /// A binary node binds its two children's tuples to `pl` and `pr` once
    /// each, so a node's text holds each child's text once and the
    /// statement grows linearly with the tree.
    fn arith_tuple(&mut self, expr: &FieldExpr) -> Result<Option<Tuple>, PlanError> {
        match fold(expr)? {
            Folded::Int(v) => return Ok(Some(Tuple::int(int_text(v), false))),
            Folded::Dur(v) => return Ok(Some(Tuple::int(int_text(v), true))),
            Folded::U128(v) => return Ok(Some(Tuple::int(u128_text(v), false))),
            Folded::Float(x) => {
                return Ok(Some(Tuple {
                    text: format!("tuple({NULL_INT}, {})", float_text(x)),
                    dur: false,
                }));
            }
            Folded::Undefined => return Ok(None),
            Folded::No => {}
        }
        match expr {
            FieldExpr::Binary {
                op: FieldOp::Arith(op),
                lhs,
                rhs,
            } => {
                self.count_arith_node()?;
                let Some(l) = self.arith_tuple(lhs)? else {
                    return Ok(None);
                };
                let Some(r) = self.arith_tuple(rhs)? else {
                    return Ok(None);
                };
                Ok(Some(binary_node(*op, &l, &r)))
            }
            FieldExpr::Unary {
                op: UnaryOp::Neg,
                expr: inner,
            } => {
                self.count_arith_node()?;
                let Some(x) = self.arith_tuple(inner)? else {
                    return Ok(None);
                };
                Ok(Some(Tuple {
                    text: format!(
                        "arrayElement(arrayMap(pl -> tuple({}, negate({L2})), [{}]), 1)",
                        neg_int(L1),
                        x.text
                    ),
                    dur: x.dur,
                }))
            }
            FieldExpr::Field(field) => self.arith_leaf(field),
            // A string, boolean, status or kind literal has no number.
            FieldExpr::Literal(_) => Ok(None),
            FieldExpr::Unary {
                op: UnaryOp::Not, ..
            }
            | FieldExpr::Binary {
                op: FieldOp::Cmp(_) | FieldOp::Bool(_),
                ..
            }
            | FieldExpr::Exists { .. } => Err(not_an_operand()),
        }
    }

    fn count_arith_node(&mut self) -> Result<(), PlanError> {
        self.arith_nodes += 1;
        if self.arith_nodes > MAX_ARITH_NODES {
            return Err(PlanError::UnsupportedField(format!(
                "an arithmetic expression of more than {MAX_ARITH_NODES} operations is not \
                 supported"
            )));
        }
        Ok(())
    }

    /// A field as an arithmetic leaf: its integer read widened to `Int256`
    /// and its float read. A set operand is the next occurrence, read per
    /// element (the part-3e design's section 5.2); a field leaf's presence
    /// is noted for `!=` (decision 11). **No wildcard arm**.
    fn arith_leaf(&mut self, field: &Field) -> Result<Option<Tuple>, PlanError> {
        if is_set_field(field) {
            return self.set_leaf(field).map(Some);
        }
        let attribute = |root: &str, key: &str| {
            let path = attr_path(root, key);
            Some(Tuple {
                text: format!(
                    "tuple(toInt256({path}{}), {path}{})",
                    Read::Int.suffix(),
                    Read::Float.suffix()
                ),
                dur: false,
            })
        };
        let intrinsic = match field {
            Field::Attribute { scope, key } => {
                return match scope {
                    AttrScope::Span | AttrScope::Instrumentation => {
                        let root = if *scope == AttrScope::Span {
                            ATTRS
                        } else {
                            SCOPE_ATTRS
                        };
                        self.note_presence(Some(format!(
                            "dynamicType({}) != 'None'",
                            attr_path(root, key)
                        )));
                        Ok(attribute(root, key))
                    }
                    // `resource.service.name`'s numeric arms are on the
                    // resource row, as every other resource key's are.
                    AttrScope::Resource => {
                        let q = self.resource_read(key)?;
                        self.note_presence(Some(format!(
                            "dynamicType({}) != 'None'",
                            resource_value(&q)
                        )));
                        Ok(Some(Tuple {
                            text: format!(
                                "arrayElement(arrayMap(pv -> tuple(toInt256(dynamicElement(pv, \
                                 'Int64')), dynamicElement(pv, 'Float64')), [{}]), 1)",
                                resource_value(&q)
                            ),
                            dur: false,
                        }))
                    }
                    AttrScope::Event | AttrScope::Link | AttrScope::Unscoped => {
                        unreachable!("a set operand is taken by set_leaf first")
                    }
                };
            }
            Field::Intrinsic(intrinsic) => *intrinsic,
        };
        match intrinsic {
            Intrinsic::Duration => Ok(Some(Tuple {
                text: format!("tuple(toInt256(duration_ns), {NULL_FLOAT})"),
                dur: true,
            })),
            // Not a number on any span.
            Intrinsic::Name
            | Intrinsic::StatusMessage
            | Intrinsic::SpanId
            | Intrinsic::ParentId
            | Intrinsic::TraceId
            | Intrinsic::InstrumentationName
            | Intrinsic::InstrumentationVersion
            | Intrinsic::Status
            | Intrinsic::Kind => Ok(None),
            Intrinsic::EventName
            | Intrinsic::EventTimeSinceStart
            | Intrinsic::LinkSpanId
            | Intrinsic::LinkTraceId => unreachable!("a set operand is taken by set_leaf first"),
            Intrinsic::NestedSetParent | Intrinsic::NestedSetLeft | Intrinsic::NestedSetRight => {
                Err(unsupported_intrinsic(intrinsic, NESTED_AND_TRACE))
            }
            Intrinsic::ChildCount
            | Intrinsic::TraceDuration
            | Intrinsic::RootName
            | Intrinsic::RootServiceName => Err(operand_refusal(intrinsic)),
        }
    }

    /// A set operand as an arithmetic leaf (the part-3e design's section
    /// 5.2), registered as the next occurrence `k`: an attribute or chain
    /// element's integer and float reads; `event:timeSinceStart`'s
    /// element, a duration; `event:name` and the link ids no number
    /// (decision 12).
    fn set_leaf(&mut self, field: &Field) -> Result<Tuple, PlanError> {
        let set = self.register_occurrence(field, false)?;
        let e = &set.var;
        let read = |r: Read| format!("dynamicElement({e}, '{}')", r.type_name());
        Ok(match field {
            Field::Attribute { .. } => Tuple {
                text: format!(
                    "tuple(toInt256({}), {})",
                    read(Read::Int),
                    read(Read::Float)
                ),
                dur: false,
            },
            Field::Intrinsic(Intrinsic::EventTimeSinceStart) => Tuple {
                text: format!("tuple(toInt256({e}), {NULL_FLOAT})"),
                dur: true,
            },
            Field::Intrinsic(_) => Tuple {
                text: format!("tuple({NULL_INT}, {NULL_FLOAT})"),
                dur: false,
            },
        })
    }

    /// One operand's arms: a typed read per class it can hold. `var` is
    /// the lambda variable a resource-row operand is bound to. **No
    /// wildcard arm**, so a new `Intrinsic` or `AttrScope` variant fails to
    /// compile here until it is classified.
    fn operand_arms(&mut self, field: &Field, var: String) -> Result<Arms, PlanError> {
        let intrinsic = match field {
            Field::Attribute { scope, key } => {
                return match scope {
                    AttrScope::Span => Ok(Arms::attribute(ATTRS, key)),
                    AttrScope::Instrumentation => Ok(Arms::attribute(SCOPE_ATTRS, key)),
                    // The writer keeps a non-empty string on the span row
                    // only, so the string arm is read there; every other
                    // arm from the resource row.
                    AttrScope::Resource if key == SERVICE_NAME => {
                        let q = self.resource_read(key)?;
                        Ok(Arms {
                            s: Some(format!("if({SERVICE_IS_STRING}, service, NULL)")),
                            s_on_span_row: true,
                            ..Arms::resource(&var, &q)
                        })
                    }
                    AttrScope::Resource => {
                        let q = self.resource_read(key)?;
                        Ok(Arms::resource(&var, &q))
                    }
                    AttrScope::Event | AttrScope::Link | AttrScope::Unscoped => {
                        unreachable!("every set operand is classified before operand_arms")
                    }
                };
            }
            Field::Intrinsic(intrinsic) => *intrinsic,
        };
        let string = |expr: String| Arms {
            s: Some(expr),
            ..Arms::default()
        };
        let hex = |column: IdColumn| string(format!("lower(hex({}))", column.name));
        match intrinsic {
            Intrinsic::Name => Ok(string("name".to_string())),
            Intrinsic::StatusMessage => Ok(string("status_message".to_string())),
            Intrinsic::SpanId => Ok(hex(IdColumn::SPAN_ID)),
            Intrinsic::ParentId => Ok(hex(IdColumn::PARENT_SPAN_ID)),
            Intrinsic::TraceId => Ok(hex(IdColumn::TRACE_ID)),
            Intrinsic::InstrumentationName => Ok(string("scope_name".to_string())),
            Intrinsic::InstrumentationVersion => Ok(string("scope_version".to_string())),
            Intrinsic::Duration => Ok(Arms {
                i: Some("duration_ns".to_string()),
                ..Arms::default()
            }),
            Intrinsic::Status => Ok(Arms {
                st: Some("status_code".to_string()),
                ..Arms::default()
            }),
            Intrinsic::Kind => Ok(Arms {
                kd: Some("kind".to_string()),
                ..Arms::default()
            }),
            Intrinsic::EventName
            | Intrinsic::EventTimeSinceStart
            | Intrinsic::LinkSpanId
            | Intrinsic::LinkTraceId => {
                unreachable!("every set operand is classified before operand_arms")
            }
            Intrinsic::NestedSetParent | Intrinsic::NestedSetLeft | Intrinsic::NestedSetRight => {
                Err(unsupported_intrinsic(intrinsic, NESTED_AND_TRACE))
            }
            Intrinsic::ChildCount
            | Intrinsic::TraceDuration
            | Intrinsic::RootName
            | Intrinsic::RootServiceName => Err(operand_refusal(intrinsic)),
        }
    }

    /// `L op R`, two lone fields of which at least one is a set (the
    /// part-3d design's section 5): each operand classified left before
    /// right — [`Compiler::set_operand`], else [`Compiler::operand_arms`] —
    /// so a refused or context-less operand is refused as part 3c refuses
    /// it; then one set against a scalar per element
    /// ([`element_compare`]) or two sets by their class lists
    /// ([`class_list_compare`]); then the binding of section 5.5: each
    /// chain inside the resource values, left outermost.
    fn set_compare(
        &mut self,
        lhs: &Field,
        op: ComparisonOp,
        rhs: &Field,
    ) -> Result<String, PlanError> {
        let l_set = self.set_operand(lhs, 1, "r1")?;
        let l_scalar = match l_set {
            Some(_) => None,
            None => Some(self.operand_arms(lhs, "r1".to_string())?),
        };
        let r_set = self.set_operand(rhs, 2, "r2")?;
        let r_scalar = match r_set {
            Some(_) => None,
            None => Some(self.operand_arms(rhs, "r2".to_string())?),
        };
        let pred = match (&l_set, &l_scalar, &r_set, &r_scalar) {
            (Some(l), _, Some(r), _) => class_list_compare(l, op, r),
            (Some(l), _, None, Some(r)) => {
                element_compare(l, op, r, scalar_presence(rhs, "r2").as_deref(), true)
            }
            (None, Some(l), Some(r), _) => {
                element_compare(r, op, l, scalar_presence(lhs, "r1").as_deref(), false)
            }
            _ => unreachable!("set_compare is called with at least one set operand"),
        };
        if pred == "false" {
            return Ok(pred);
        }
        let mut body = pred;
        for set in [&r_set, &l_set].into_iter().flatten() {
            if let Some((var, chain)) = &set.chain {
                body = format!("arrayExists({var} -> {body}, [{chain}])");
            }
        }
        let arms = |set: &Option<SetOperand>, scalar: &Option<Arms>| -> Vec<(String, String)> {
            match (set, scalar) {
                (Some(s), _) => s.arms.binds.clone(),
                (None, Some(a)) => a.binds.clone(),
                (None, None) => Vec::new(),
            }
        };
        Ok(bind_values(
            &arms(&l_set, &l_scalar),
            &arms(&r_set, &r_scalar),
            body,
        ))
    }

    /// The set `field` reads at position `n` (the part-3d design's section
    /// 5.1), or `None` for a scalar field; a chain's resource value is bound
    /// to `resource_var`. **No wildcard arm**.
    fn set_operand(
        &mut self,
        field: &Field,
        n: usize,
        resource_var: &str,
    ) -> Result<Option<SetOperand>, PlanError> {
        let var = format!("e{n}");
        let intrinsic = match field {
            Field::Attribute { scope, key } => {
                return match scope {
                    AttrScope::Span | AttrScope::Instrumentation | AttrScope::Resource => Ok(None),
                    AttrScope::Event => Ok(Some(SetOperand::attribute(EVENT_ATTRS, key, var))),
                    AttrScope::Link => Ok(Some(SetOperand::attribute(LINK_ATTRS, key, var))),
                    AttrScope::Unscoped => self.chain_set(key, n, resource_var).map(Some),
                };
            }
            Field::Intrinsic(intrinsic) => *intrinsic,
        };
        let intrinsic_set = |array: &str, count: &str, read: Read| {
            let element = Arms {
                s: (read == Read::Str).then(|| var.clone()),
                i: (read == Read::Int).then(|| var.clone()),
                ..Arms::default()
            };
            Some(SetOperand {
                array: array.to_string(),
                var: var.clone(),
                arms: element,
                count: count.to_string(),
                lists: vec![(read, array.to_string())],
                chain: None,
                presence: None,
            })
        };
        Ok(match intrinsic {
            Intrinsic::EventName => intrinsic_set("events.name", "length(events.name)", Read::Str),
            Intrinsic::EventTimeSinceStart => intrinsic_set(
                "arrayMap(t -> toInt128(t) - start_ns, events.time_ns)",
                "length(events.time_ns)",
                Read::Int,
            ),
            Intrinsic::LinkSpanId => intrinsic_set(
                "arrayMap(h -> lower(hex(h)), links.span_id)",
                "length(links.span_id)",
                Read::Str,
            ),
            Intrinsic::LinkTraceId => intrinsic_set(
                "arrayMap(h -> lower(hex(h)), links.trace_id)",
                "length(links.trace_id)",
                Read::Str,
            ),
            Intrinsic::Name
            | Intrinsic::StatusMessage
            | Intrinsic::SpanId
            | Intrinsic::ParentId
            | Intrinsic::TraceId
            | Intrinsic::InstrumentationName
            | Intrinsic::InstrumentationVersion
            | Intrinsic::Duration
            | Intrinsic::Status
            | Intrinsic::Kind
            | Intrinsic::NestedSetParent
            | Intrinsic::NestedSetLeft
            | Intrinsic::NestedSetRight
            | Intrinsic::ChildCount
            | Intrinsic::TraceDuration
            | Intrinsic::RootName
            | Intrinsic::RootServiceName => None,
        })
    }

    /// `.key` at position `n` as a set (the part-3d design's section 5.4):
    /// `c<n>` bound to `C(k)`, the elements of the first scope holding the
    /// key, which reads the resource value bound to `r<n>`.
    ///
    /// Each scope's values are concatenated into one array with a tag
    /// naming the scope, and the elements whose tag is `SC(k)`, the first
    /// scope holding the key, are kept. The engine's direct forms — a
    /// `multiIf` or `if` choosing among `Array(Dynamic)` values — fail on
    /// 26.3.29.7 with `Code: 36 … Variant type should have at least one
    /// nested type`. At `service.name` a string is taken from the span row
    /// and every other arm from the resource row, as part 3b's operand
    /// does; the arm not taken is tagged 9, which `SC(k)` never answers.
    ///
    /// The resource value is bound to `resource_var`: `r<n>` for part 3d's
    /// two lone fields, `cr<k>` for part 3e's occurrence `k`.
    fn chain_set(
        &mut self,
        key: &str,
        n: usize,
        resource_var: &str,
    ) -> Result<SetOperand, PlanError> {
        self.ctx.ok_or_else(|| needs_window(AttrScope::Unscoped))?;
        let q = self.resource_read(key)?;
        let r = resource_var.to_string();
        let (span, events, links, scope) = (
            attr_path(ATTRS, key),
            attr_path(EVENT_ATTRS, key),
            attr_path(LINK_ATTRS, key),
            attr_path(SCOPE_ATTRS, key),
        );
        let (resource_present, resource_values, resource_tags) = if key == SERVICE_NAME {
            (
                format!("({SERVICE_IS_STRING} OR dynamicType({r}) != 'None')"),
                format!("[CAST(CAST(service, 'String'), 'Dynamic')], [{r}]"),
                format!("[if({SERVICE_IS_STRING}, 2, 9)], [if({SERVICE_IS_STRING}, 9, 2)]"),
            )
        } else {
            (
                format!("dynamicType({r}) != 'None'"),
                format!("[{r}]"),
                "[2]".to_string(),
            )
        };
        let tag = format!(
            "multiIf(dynamicType({span}) != 'None', 1, {resource_present}, 2, arrayExists(d -> \
             dynamicType(d) != 'None', {events}), 3, arrayExists(d -> dynamicType(d) != 'None', \
             {links}), 4, dynamicType({scope}) != 'None', 5, 0)"
        );
        let chain = format!(
            "arrayFilter((d, g) -> g = {tag} AND dynamicType(d) != 'None', arrayConcat([{span}], \
             {resource_values}, {events}, {links}, [{scope}]), arrayConcat([1], {resource_tags}, \
             arrayMap(d -> 3, {events}), arrayMap(d -> 4, {links}), [5]))"
        );
        let c = format!("c{n}");
        let var = format!("e{n}");
        Ok(SetOperand {
            array: c.clone(),
            arms: Arms {
                binds: vec![(r, resource_value(&q))],
                ..element_arms(&var)
            },
            var,
            count: format!("length({c})"),
            lists: dynamic_lists(&c),
            presence: Some(format!("notEmpty({c})")),
            chain: Some((c, chain)),
        })
    }
}

/// The scopes of the unscoped chain, in the order a span's one value is
/// taken from (`docs/api.md`, the unscoped attribute).
pub(super) const CHAIN: [AttrScope; 5] = [
    AttrScope::Span,
    AttrScope::Resource,
    AttrScope::Event,
    AttrScope::Link,
    AttrScope::Instrumentation,
];

/// `!<root>.<key>`'s two halves: what matches `want` (`false` with none),
/// and the demand condition, a present non-boolean.
///
/// **The condition is two `!=`, never `NOT IN ('None', 'Bool')`**: on
/// 26.3.29.7 the `NOT IN` form threw `Code: 395` over a `LowCardinality`
/// set on data holding no non-boolean at all (issue #589).
fn not_attr(root: &str, key: &str, want: Option<bool>) -> (String, String) {
    let path = attr_path(root, key);
    (
        want.map_or_else(
            || "false".to_string(),
            |w| format!("coalesce({path}.:Bool = {w}, false)"),
        ),
        format!("dynamicType({path}) != 'None' AND dynamicType({path}) != 'Bool'"),
    )
}

/// `!event.key` / `!link.key`'s two halves, each any-match over the
/// elements: some element IS `want`, and some element holds a
/// non-boolean. With `all`, the match is every element: the key is held
/// and no element is `!want` (decision 7 of the part-3d design).
fn not_element(root: &str, key: &str, want: Option<bool>, all: bool) -> (String, String) {
    let path = attr_path(root, key);
    (
        want.map_or_else(
            || "false".to_string(),
            |w| {
                if all {
                    format!(
                        "(arrayExists(d -> dynamicType(d) != 'None', {path}) AND NOT \
                         arrayExists(b -> coalesce(b = {}, false), {path}.:Bool))",
                        !w
                    )
                } else {
                    format!("arrayExists(b -> coalesce(b = {w}, false), {path}.:Bool)")
                }
            },
        ),
        format!("arrayExists(d -> dynamicType(d) != 'None' AND dynamicType(d) != 'Bool', {path})"),
    )
}

/// `resource.service.name`, read from the span row: `service` holds the
/// value as text and `service_type` the arm it arrived as.
///
/// Every string and regex comparison is gated on the value having been
/// stored as a string, because `service` cannot tell `""` from no key, or
/// the integer `12345` from the string `"12345"`, and typed comparisons do
/// not cross types. A negation goes outside the gate. The refusals and the
/// non-string fold are [`untyped_string_leaf`]'s, unchanged.
fn service_name_leaf(op: ComparisonOp, value: &Value) -> Result<String, PlanError> {
    const NAME: &str = "resource.service.name";
    let (Value::String(_), Some((negated, positive_op))) = (
        value,
        match op {
            ComparisonOp::Eq => Some((false, ComparisonOp::Eq)),
            ComparisonOp::Neq => Some((true, ComparisonOp::Eq)),
            ComparisonOp::Re => Some((false, ComparisonOp::Re)),
            ComparisonOp::Nre => Some((true, ComparisonOp::Re)),
            ComparisonOp::Gt | ComparisonOp::Gte | ComparisonOp::Lt | ComparisonOp::Lte => None,
        },
    ) else {
        return untyped_string_leaf(NAME, "service", op, value);
    };
    let positive = untyped_string_leaf(NAME, "service", positive_op, value)?;
    let gated = format!("({SERVICE_IS_STRING} AND {positive})");
    Ok(if negated {
        format!("NOT {gated}")
    } else {
        gated
    })
}

// ---------------------------------------------------------------------
// the attribute path
// ---------------------------------------------------------------------

/// `<root>.` — `attrs.`, `scope_attrs.`, `events.attrs.` or
/// `links.attrs.` — then the stored JSON path,
/// backtick-quoted. Two escapes, in this order, and neither is optional.
///
/// 1. [`escape_json_path`] — `%` to `%25`, then `.` to `%2E`. It is the
///    **writer's** function: it is what put the path there, so the reader
///    must not have a second copy. A dot is the server's own nesting
///    separator, so `http.response.status_code` is stored as the single
///    path `http%2Eresponse%2Estatus_code`.
/// 2. [`escape::ch_ident`] — backtick-quote, escaping `` ` `` and `\`.
///    This is the first place in the product where a client-chosen OTLP
///    key becomes a SQL **identifier**; `ch_ident`'s own doc carries the
///    measurement.
pub(super) fn attr_path(root: &str, key: &str) -> String {
    format!("{root}.{}", escape::ch_ident(&escape_json_path(key)))
}

/// Any element holds the key. Absence is `NOT` outside, so a span carrying
/// no elements lacks it. `head` is the loop's function: `arrayExists`, or
/// `arrayFirstIndex` for the search projection's first element holding it.
fn element_exists(root: &str, key: &str, negated: bool, head: &str) -> String {
    let present = format!(
        "{head}(d -> dynamicType(d) != 'None', {})",
        attr_path(root, key)
    );
    if negated {
        format!("NOT {present}")
    } else {
        present
    }
}

/// One typed read of an attribute value: on a span row, the typed
/// subcolumn; in an element condition, the lambda variable bound to that
/// subcolumn's array.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Read {
    Str,
    Int,
    Float,
    Bool,
    StrArray,
    IntArray,
    FloatArray,
    BoolArray,
}

impl Read {
    fn suffix(self) -> &'static str {
        match self {
            Read::Str => ".:String",
            Read::Int => ".:Int64",
            Read::Float => ".:Float64",
            Read::Bool => ".:Bool",
            Read::StrArray => ".:`Array(Nullable(String))`",
            Read::IntArray => ".:`Array(Nullable(Int64))`",
            Read::FloatArray => ".:`Array(Nullable(Float64))`",
            Read::BoolArray => ".:`Array(Nullable(Bool))`",
        }
    }

    /// The type name a `Dynamic` value of this read is stored as, which is
    /// what `dynamicType` answers and `dynamicElement` takes.
    fn type_name(self) -> &'static str {
        match self {
            Read::Str => "String",
            Read::Int => "Int64",
            Read::Float => "Float64",
            Read::Bool => "Bool",
            Read::StrArray => "Array(Nullable(String))",
            Read::IntArray => "Array(Nullable(Int64))",
            Read::FloatArray => "Array(Nullable(Float64))",
            Read::BoolArray => "Array(Nullable(Bool))",
        }
    }

    fn var(self) -> &'static str {
        match self {
            Read::Str => "s",
            Read::Int => "i",
            Read::Float => "f",
            Read::Bool => "b",
            Read::StrArray => "sa",
            Read::IntArray => "ia",
            Read::FloatArray => "fa",
            Read::BoolArray => "ba",
        }
    }
}

// ---------------------------------------------------------------------
// field against field
// ---------------------------------------------------------------------

/// The classes a field-against-field operand can hold. `Status` and
/// `Kind` are their own classes, so against an attribute or any other
/// field they share no term.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Class {
    Str,
    Int,
    Float,
    Bool,
    Status,
    Kind,
}

impl Class {
    /// The typed read of a scalar of this class, if an attribute can hold
    /// one.
    fn scalar_read(self) -> Option<Read> {
        match self {
            Class::Str => Some(Read::Str),
            Class::Int => Some(Read::Int),
            Class::Float => Some(Read::Float),
            Class::Bool => Some(Read::Bool),
            Class::Status | Class::Kind => None,
        }
    }

    /// The typed read of an array whose elements are this class, if an
    /// attribute can hold one.
    fn array_read(self) -> Option<Read> {
        match self {
            Class::Str => Some(Read::StrArray),
            Class::Int => Some(Read::IntArray),
            Class::Float => Some(Read::FloatArray),
            Class::Bool => Some(Read::BoolArray),
            Class::Status | Class::Kind => None,
        }
    }

    /// Whether an ordered operator has a term for this class: a boolean, a
    /// status and a kind are compared by `=` and `!=` only.
    fn is_ordered(self) -> bool {
        match self {
            Class::Str | Class::Int | Class::Float => true,
            Class::Bool | Class::Status | Class::Kind => false,
        }
    }
}

/// One field-against-field operand: one SQL expression per class it can
/// hold, scalar and array, and whether any of them can be `NULL`.
///
/// A resource-row operand also carries `types`, the text of the set of
/// types its key is stored as in the window (`tupleElement(Q(k), 3)`),
/// which gates each arm. `binds` holds the lambda variable and the value it
/// is bound to, for a resource-row operand and an arithmetic side.
/// `s_on_span_row` marks `resource.service.name`, whose string arm is read
/// from the span row and so is not gated.
#[derive(Debug, Default)]
struct Arms {
    s: Option<String>,
    i: Option<String>,
    f: Option<String>,
    b: Option<String>,
    st: Option<String>,
    kd: Option<String>,
    sa: Option<String>,
    ia: Option<String>,
    fa: Option<String>,
    ba: Option<String>,
    nullable: bool,
    types: Option<String>,
    s_on_span_row: bool,
    binds: Vec<(String, String)>,
}

impl Arms {
    /// An attribute at `root`: the literal path's typed reads, scalar and
    /// array, every one `Nullable`.
    fn attribute(root: &str, key: &str) -> Arms {
        let path = attr_path(root, key);
        let read = |r: Read| Some(format!("{path}{}", r.suffix()));
        Arms {
            s: read(Read::Str),
            i: read(Read::Int),
            f: read(Read::Float),
            b: read(Read::Bool),
            st: None,
            kd: None,
            sa: read(Read::StrArray),
            ia: read(Read::IntArray),
            fa: read(Read::FloatArray),
            ba: read(Read::BoolArray),
            nullable: true,
            types: None,
            s_on_span_row: false,
            binds: Vec::new(),
        }
    }

    /// A resource attribute whose lookup is `q` (`Q(k)`): every class read
    /// from the value bound to `var`, which is the value at the span's own
    /// `resource_id` — `NULL` when the span's resource row is not among
    /// `q`'s, so every arm is `NULL` then.
    fn resource(var: &str, q: &str) -> Arms {
        let read = |r: Read| Some(format!("dynamicElement({var}, '{}')", r.type_name()));
        Arms {
            s: read(Read::Str),
            i: read(Read::Int),
            f: read(Read::Float),
            b: read(Read::Bool),
            st: None,
            kd: None,
            sa: read(Read::StrArray),
            ia: read(Read::IntArray),
            fa: read(Read::FloatArray),
            ba: read(Read::BoolArray),
            nullable: true,
            types: Some(format!("tupleElement({q}, 3)")),
            s_on_span_row: false,
            binds: vec![(var.to_string(), resource_value(q))],
        }
    }

    /// The gate of the scalar arm of `class`: whether any resource row
    /// holds a value of that type. `None` when the arm needs none.
    fn scalar_gate(&self, class: Class) -> Option<String> {
        if class == Class::Str && self.s_on_span_row {
            return None;
        }
        self.gate(class.scalar_read()?)
    }

    /// The gate of the array arm whose elements are `class`.
    fn array_gate(&self, class: Class) -> Option<String> {
        self.gate(class.array_read()?)
    }

    fn gate(&self, read: Read) -> Option<String> {
        let types = self.types.as_deref()?;
        Some(format!("has({types}, '{}')", read.type_name()))
    }

    fn scalar(&self, class: Class) -> Option<&str> {
        match class {
            Class::Str => self.s.as_deref(),
            Class::Int => self.i.as_deref(),
            Class::Float => self.f.as_deref(),
            Class::Bool => self.b.as_deref(),
            Class::Status => self.st.as_deref(),
            Class::Kind => self.kd.as_deref(),
        }
    }

    fn array(&self, element: Class) -> Option<&str> {
        match element {
            Class::Str => self.sa.as_deref(),
            Class::Int => self.ia.as_deref(),
            Class::Float => self.fa.as_deref(),
            Class::Bool => self.ba.as_deref(),
            Class::Status | Class::Kind => None,
        }
    }
}

/// `V(k)`: the value of the lookup `q` (`Q(k)`) at the span's own
/// `resource_id`, `NULL` when the span's resource row is not among `q`'s.
fn resource_value(q: &str) -> String {
    format!(
        "arrayElement(tupleElement({q}, 2), transform(resource_id, tupleElement({q}, 1), \
         arrayEnumerate(tupleElement({q}, 1)), 0))"
    )
}

/// The scalar pairs, in the order their terms are emitted.
const SCALAR_PAIRS: [(Class, Class); 8] = [
    (Class::Str, Class::Str),
    (Class::Int, Class::Int),
    (Class::Int, Class::Float),
    (Class::Float, Class::Int),
    (Class::Float, Class::Float),
    (Class::Bool, Class::Bool),
    (Class::Status, Class::Status),
    (Class::Kind, Class::Kind),
];

/// The `(scalar class, element class)` pairs of a scalar against an
/// array, in the order their terms are emitted.
const ARRAY_PAIRS: [(Class, Class); 6] = [
    (Class::Str, Class::Str),
    (Class::Int, Class::Int),
    (Class::Int, Class::Float),
    (Class::Float, Class::Int),
    (Class::Float, Class::Float),
    (Class::Bool, Class::Bool),
];

/// The integer against a float, and the only place that choice lives: the
/// integer side of every `Int`/`Float` pair, scalar and array, is rendered
/// here.
///
/// **Exact, by decision.** ClickHouse compares `Int64` with `Float64`
/// exactly, so `9007199254740993 > 9007199254740992.0` is true here. The
/// reference converts both sides to `float64` first, where the two are
/// equal; the other choice would be `toFloat64(e)`. Rounding is not a rule
/// a user can rely on, so this differs from the reference deliberately
/// (`docs/benchmarks/traces-differential-ledger.md`,
/// `traceql-field-compare-type-gate`). An integer pair never passes
/// through here.
fn mixed_int(expr: &str) -> String {
    expr.to_string()
}

/// One side of a pair: the integer side of a mixed pair through
/// [`mixed_int`], anything else as it is.
fn pair_side(class: Class, other: Class, expr: &str) -> String {
    if class == Class::Int && other == Class::Float {
        mixed_int(expr)
    } else {
        expr.to_string()
    }
}

/// A scalar against each element `x` of `array`, `l` and `r` in the
/// written order. Any element matches, except `!=`, where every element
/// must differ and the array must not be empty: a typed array subcolumn is
/// `[]` on every row whose value is not that array, and `arrayAll` over
/// `[]` is true.
fn array_term(op: ComparisonOp, sym: &str, l: &str, r: &str, array: &str) -> String {
    if op == ComparisonOp::Neq {
        format!("(notEmpty({array}) AND arrayAll(x -> coalesce({l} != {r}, false), {array}))")
    } else {
        format!("arrayExists(x -> coalesce({l} {sym} {r}, false), {array})")
    }
}

/// `term` under the gates present, left first.
fn gated(term: String, left: Option<String>, right: Option<String>) -> String {
    match (left, right) {
        (None, None) => term,
        (Some(g), None) | (None, Some(g)) => format!("if({g}, {term}, false)"),
        (Some(gl), Some(gr)) => format!("if({gl} AND {gr}, {term}, false)"),
    }
}

/// `L op R` over two operands' arms: one term per pair of classes both
/// sides carry, ORed, and `false` when there is none.
///
/// **Each type pair is its own term.** The engine's direct form, one
/// comparison of the two `Dynamic` values, fails the statement with
/// `NO_COMMON_TYPE` on a span holding a string against an integer. A pair
/// of different types has no term and so matches nothing, under every
/// operator — `!=` included, which is why `!=` is per term and never
/// `NOT (… = …)`. Two arrays have no term either: the reference fails the
/// query there, and one span holding two arrays would empty every result.
///
/// A scalar term is coalesced unless both operands are non-nullable,
/// which is the module's rule that no intrinsic read is coalesced.
///
/// A term with an arm read from the resource row is gated on that key's
/// type set, `if(<gate_L> AND <gate_R>, <term>, false)`. The gate never
/// changes an answer — it is false only when no resource row holds that
/// type, where the term is false anyway — but it is a constant, so the
/// engine folds every term no resource row can satisfy.
fn field_terms(l: &Arms, op: ComparisonOp, r: &Arms) -> String {
    let sym = sql_op(op).expect("the caller has already excluded the regex operators");
    let ordered = !matches!(op, ComparisonOp::Eq | ComparisonOp::Neq);
    let coalesced = l.nullable || r.nullable;
    let mut terms = Vec::new();
    for (lc, rc) in SCALAR_PAIRS {
        if ordered && !lc.is_ordered() {
            continue;
        }
        let (Some(a), Some(b)) = (l.scalar(lc), r.scalar(rc)) else {
            continue;
        };
        let c = format!("{} {sym} {}", pair_side(lc, rc, a), pair_side(rc, lc, b));
        let term = if coalesced {
            format!("coalesce({c}, false)")
        } else {
            c
        };
        terms.push(gated(term, l.scalar_gate(lc), r.scalar_gate(rc)));
    }
    // The array on the right, then the array on the left.
    for (c, e) in ARRAY_PAIRS {
        if ordered && !c.is_ordered() {
            continue;
        }
        if let (Some(scalar), Some(array)) = (l.scalar(c), r.array(e)) {
            let term = array_term(
                op,
                sym,
                &pair_side(c, e, scalar),
                &pair_side(e, c, "x"),
                array,
            );
            terms.push(gated(term, l.scalar_gate(c), r.array_gate(e)));
        }
    }
    for (c, e) in ARRAY_PAIRS {
        if ordered && !c.is_ordered() {
            continue;
        }
        if let (Some(array), Some(scalar)) = (l.array(e), r.scalar(c)) {
            let term = array_term(
                op,
                sym,
                &pair_side(e, c, "x"),
                &pair_side(c, e, scalar),
                array,
            );
            terms.push(gated(term, l.array_gate(e), r.scalar_gate(c)));
        }
    }
    if terms.is_empty() {
        "false".to_string()
    } else {
        format!("({})", terms.join(" OR "))
    }
}

/// The values a comparison binds — an operand read from the resource row,
/// an arithmetic side, a chain's resource value — left then right, each to
/// its variable once: the body is a lambda applied to one-element arrays.
fn bind_values(left: &[(String, String)], right: &[(String, String)], body: String) -> String {
    bind_with("arrayExists", left, right, body)
}

/// [`bind_values`] in select mode (issue #591 part 2's section 3.5): the
/// body is a value, not a condition, so it is mapped over the one-element
/// arrays and the one element read back.
fn select_values(left: &[(String, String)], right: &[(String, String)], body: String) -> String {
    let binds: Vec<&(String, String)> = left.iter().chain(right).collect();
    if binds.is_empty() {
        return body;
    }
    format!(
        "arrayElement({}, 1)",
        bind_with("arrayMap", left, right, body)
    )
}

fn bind_with(
    function: &str,
    left: &[(String, String)],
    right: &[(String, String)],
    body: String,
) -> String {
    let binds: Vec<&(String, String)> = left.iter().chain(right).collect();
    match binds.as_slice() {
        [] => body,
        [(v, value)] => format!("{function}({v} -> {body}, [{value}])"),
        many => {
            let vars: Vec<&str> = many.iter().map(|(v, _)| v.as_str()).collect();
            let values: Vec<String> = many.iter().map(|(_, value)| format!("[{value}]")).collect();
            format!(
                "{function}(({}) -> {body}, {})",
                vars.join(", "),
                values.join(", ")
            )
        }
    }
}

// ---------------------------------------------------------------------
// event, link and unscoped operands: sets
// ---------------------------------------------------------------------

/// Whether `field` is a set operand (the part-3d design's section 5.1): an
/// `event.`, `link.` or `.` attribute, or one of the four event and link
/// intrinsics. **No wildcard arm**.
fn is_set_field(field: &Field) -> bool {
    match field {
        Field::Attribute { scope, .. } => match scope {
            AttrScope::Event | AttrScope::Link | AttrScope::Unscoped => true,
            AttrScope::Span | AttrScope::Instrumentation | AttrScope::Resource => false,
        },
        Field::Intrinsic(intrinsic) => is_set_intrinsic(*intrinsic),
    }
}

/// The four event and link intrinsics. **No wildcard arm**.
fn is_set_intrinsic(intrinsic: Intrinsic) -> bool {
    match intrinsic {
        Intrinsic::EventName
        | Intrinsic::EventTimeSinceStart
        | Intrinsic::LinkSpanId
        | Intrinsic::LinkTraceId => true,
        Intrinsic::Name
        | Intrinsic::StatusMessage
        | Intrinsic::SpanId
        | Intrinsic::ParentId
        | Intrinsic::TraceId
        | Intrinsic::InstrumentationName
        | Intrinsic::InstrumentationVersion
        | Intrinsic::Duration
        | Intrinsic::Status
        | Intrinsic::Kind
        | Intrinsic::NestedSetParent
        | Intrinsic::NestedSetLeft
        | Intrinsic::NestedSetRight
        | Intrinsic::ChildCount
        | Intrinsic::TraceDuration
        | Intrinsic::RootName
        | Intrinsic::RootServiceName => false,
    }
}

/// Whether a lone `field` opposite a literal-only side that folds to a
/// constant is the leaf over that constant: any attribute, and the four
/// event and link intrinsics (decision 8 of the part-3d design).
fn folds_to_leaf(field: &Field) -> bool {
    match field {
        Field::Attribute { .. } => true,
        Field::Intrinsic(intrinsic) => is_set_intrinsic(*intrinsic),
    }
}

/// Whether `expr` holds literals and arithmetic only.
fn literal_only(expr: &FieldExpr) -> bool {
    match expr {
        FieldExpr::Literal(_) => true,
        FieldExpr::Binary {
            op: FieldOp::Arith(_),
            lhs,
            rhs,
        } => literal_only(lhs) && literal_only(rhs),
        FieldExpr::Unary {
            op: UnaryOp::Neg,
            expr: inner,
        } => literal_only(inner),
        FieldExpr::Field(_)
        | FieldExpr::Exists { .. }
        | FieldExpr::Unary {
            op: UnaryOp::Not, ..
        }
        | FieldExpr::Binary {
            op: FieldOp::Cmp(_) | FieldOp::Bool(_),
            ..
        } => false,
    }
}

/// Every typed read an attribute element can be, in the order the lists
/// of a set are built.
const ELEMENT_READS: [Read; 8] = [
    Read::Str,
    Read::Int,
    Read::Float,
    Read::Bool,
    Read::StrArray,
    Read::IntArray,
    Read::FloatArray,
    Read::BoolArray,
];

/// One set operand of the part-3d design's section 5.1.
///
/// `array` is `S`, iterated by `var`, `e<n>`; `arms` are the element's
/// typed reads, with a chain's resource value in `binds`; `count` is
/// `|S|` and `lists` its elements by type (section 5.3); `chain` is a
/// chain's variable `c<n>` and `C(k)`; `presence` the chain's own
/// `notEmpty(c<n>)`. An event, link or intrinsic set has no presence: an
/// empty one satisfies `!=` (decision 1).
struct SetOperand {
    array: String,
    var: String,
    arms: Arms,
    count: String,
    lists: Vec<(Read, String)>,
    chain: Option<(String, String)>,
    presence: Option<String>,
}

impl SetOperand {
    /// `event.key` or `link.key`: the elements holding the key.
    fn attribute(root: &str, key: &str, var: String) -> SetOperand {
        let path = attr_path(root, key);
        SetOperand {
            array: format!("arrayFilter(d -> dynamicType(d) != 'None', {path})"),
            arms: element_arms(&var),
            var,
            count: format!("arrayCount(d -> dynamicType(d) != 'None', {path})"),
            lists: dynamic_lists(&path),
            chain: None,
            presence: None,
        }
    }

    fn list(&self, read: Read) -> Option<&str> {
        self.lists
            .iter()
            .find(|(r, _)| *r == read)
            .map(|(_, text)| text.as_str())
    }
}

/// The arms of an element of a `Dynamic` array, bound to `var`: every
/// class read from it, every one `Nullable`.
fn element_arms(var: &str) -> Arms {
    let read = |r: Read| Some(format!("dynamicElement({var}, '{}')", r.type_name()));
    Arms {
        s: read(Read::Str),
        i: read(Read::Int),
        f: read(Read::Float),
        b: read(Read::Bool),
        st: None,
        kd: None,
        sa: read(Read::StrArray),
        ia: read(Read::IntArray),
        fa: read(Read::FloatArray),
        ba: read(Read::BoolArray),
        nullable: true,
        types: None,
        s_on_span_row: false,
        binds: Vec::new(),
    }
}

/// A `Dynamic` array's elements of each type, one list per type.
fn dynamic_lists(array: &str) -> Vec<(Read, String)> {
    ELEMENT_READS
        .iter()
        .map(|read| {
            let t = read.type_name();
            (
                *read,
                format!(
                    "arrayMap(d -> dynamicElement(d, '{t}'), arrayFilter(d -> dynamicType(d) = \
                     '{t}', {array}))"
                ),
            )
        })
        .collect()
}

/// A scalar operand's presence, which `!=` against a set requires (the
/// part-3d design's section 5.1): `var` is the variable a resource value
/// is bound to. An intrinsic has none. **No wildcard arm**.
fn scalar_presence(field: &Field, var: &str) -> Option<String> {
    match field {
        Field::Attribute { scope, key } => match scope {
            AttrScope::Span => Some(format!("dynamicType({}) != 'None'", attr_path(ATTRS, key))),
            AttrScope::Instrumentation => Some(format!(
                "dynamicType({}) != 'None'",
                attr_path(SCOPE_ATTRS, key)
            )),
            AttrScope::Resource if key == SERVICE_NAME => Some(format!(
                "({SERVICE_IS_STRING} OR dynamicType({var}) != 'None')"
            )),
            AttrScope::Resource => Some(format!("dynamicType({var}) != 'None'")),
            AttrScope::Event | AttrScope::Link | AttrScope::Unscoped => None,
        },
        Field::Intrinsic(_) => None,
    }
}

/// One set against a scalar (the part-3d design's section 5.2): part 3a's
/// terms between the element and the scalar. Any element, for every
/// operator but `!=`; `!=` every element, after the presences — the
/// chain's and the scalar's, in the written order — so an empty event or
/// link set satisfies it and an absent scalar or a chain held nowhere
/// does not. With no term, `false`, except that `!=` holds over an empty
/// set (decision 9 of the part-3d design).
fn element_compare(
    set: &SetOperand,
    op: ComparisonOp,
    scalar: &Arms,
    scalar_presence: Option<&str>,
    set_on_left: bool,
) -> String {
    let body = if set_on_left {
        field_terms(&set.arms, op, scalar)
    } else {
        field_terms(scalar, op, &set.arms)
    };
    // Decision 9: with no term, nothing matches under `= < <= > >=`, and
    // `!=` keeps its every-element rule over `false`, so it holds exactly
    // when the set is empty.
    if body == "false" && op != ComparisonOp::Neq {
        return body;
    }
    let (var, array) = (&set.var, &set.array);
    if op != ComparisonOp::Neq {
        return format!("arrayExists({var} -> {body}, {array})");
    }
    let presences: Vec<&str> = if set_on_left {
        [set.presence.as_deref(), scalar_presence]
    } else {
        [scalar_presence, set.presence.as_deref()]
    }
    .into_iter()
    .flatten()
    .collect();
    let all = format!("arrayAll({var} -> {body}, {array})");
    if presences.is_empty() {
        all
    } else {
        format!("({} AND {all})", presences.join(" AND "))
    }
}

/// Two sets (the part-3d design's section 5.3): every pair of elements,
/// one from each, by part 3a's type pairs over each set's lists of one
/// type. Any pair, for every operator but `!=`; `!=` every pair, by
/// counting the pairs that satisfy it against `|L| × |R|`, so no pairs
/// satisfies it. The count is exact: each element has one stored type and
/// each pair of types at most one term, so a pair of elements is counted
/// at most once, and a pair with no term — two arrays, a string against a
/// number — is not counted. With no term at all, `false`, except that
/// `!=` holds when either set is empty (decision 9).
///
/// Comparing each pair through part 3a's per-element terms costs about 94
/// ns a pair against 4.6 ns for the lists (26.3.29.7, the part-3d design's
/// section 3.2).
fn class_list_compare(l: &SetOperand, op: ComparisonOp, r: &SetOperand) -> String {
    let sym = sql_op(op).expect("the caller has already excluded the regex operators");
    let ordered = !matches!(op, ComparisonOp::Eq | ComparisonOp::Neq);
    let neq = op == ComparisonOp::Neq;
    let coalesced = l.arms.nullable || r.arms.nullable;
    let cmp = |lc: Class, rc: Class, a: &str, b: &str| {
        let c = format!("{} {sym} {}", pair_side(lc, rc, a), pair_side(rc, lc, b));
        if coalesced {
            format!("coalesce({c}, false)")
        } else {
            format!("({c})")
        }
    };
    let mut terms = Vec::new();
    for (lc, rc) in SCALAR_PAIRS {
        if ordered && !lc.is_ordered() {
            continue;
        }
        let (Some(lr), Some(rr)) = (lc.scalar_read(), rc.scalar_read()) else {
            continue;
        };
        let (Some(ll), Some(rl)) = (l.list(lr), r.list(rr)) else {
            continue;
        };
        terms.push(if neq {
            format!(
                "arraySum(p -> arrayCount(q -> {}, {rl}), {ll})",
                cmp(lc, rc, "p", "q")
            )
        } else {
            format!(
                "arrayExists(x -> arrayExists(y -> {}, {rl}), {ll})",
                cmp(lc, rc, "x", "y")
            )
        });
    }
    // An array element on the right: the left's scalar element against
    // each of its elements.
    for (c, e) in ARRAY_PAIRS {
        if ordered && !c.is_ordered() {
            continue;
        }
        let (Some(lr), Some(rr)) = (c.scalar_read(), e.array_read()) else {
            continue;
        };
        let (Some(ll), Some(rl)) = (l.list(lr), r.list(rr)) else {
            continue;
        };
        terms.push(if neq {
            format!(
                "arraySum(p -> arrayCount(q -> (notEmpty(q) AND arrayAll(x -> {}, q)), {rl}), \
                 {ll})",
                cmp(c, e, "p", "x")
            )
        } else {
            format!(
                "arrayExists(x -> arrayExists(w -> arrayExists(y -> {}, w), {rl}), {ll})",
                cmp(c, e, "x", "y")
            )
        });
    }
    // An array element on the left.
    for (c, e) in ARRAY_PAIRS {
        if ordered && !c.is_ordered() {
            continue;
        }
        let (Some(lr), Some(rr)) = (e.array_read(), c.scalar_read()) else {
            continue;
        };
        let (Some(ll), Some(rl)) = (l.list(lr), r.list(rr)) else {
            continue;
        };
        terms.push(if neq {
            format!(
                "arraySum(p -> arrayCount(q -> (notEmpty(p) AND arrayAll(x -> {}, p)), {rl}), \
                 {ll})",
                cmp(e, c, "x", "q")
            )
        } else {
            format!(
                "arrayExists(u -> arrayExists(x -> arrayExists(y -> {}, {rl}), u), {ll})",
                cmp(e, c, "x", "y")
            )
        });
    }
    // Decision 9: with no term, nothing matches under `= < <= > >=`, and
    // `!=` counts no pair, so it holds when either set is empty.
    if terms.is_empty() {
        if !neq {
            return "false".to_string();
        }
        terms.push("0".to_string());
    }
    if !neq {
        return format!("({})", terms.join(" OR "));
    }
    let all = format!("(({}) = {} * {})", terms.join(" + "), l.count, r.count);
    let presences: Vec<&str> = [l.presence.as_deref(), r.presence.as_deref()]
        .into_iter()
        .flatten()
        .collect();
    if presences.is_empty() {
        all
    } else {
        format!("({} AND {all})", presences.join(" AND "))
    }
}

// ---------------------------------------------------------------------
// arithmetic, folding and the literal-only side
// ---------------------------------------------------------------------

/// `NI` and `NF`: the typed `NULL`s of an arithmetic tuple's two elements.
const NULL_INT: &str = "CAST(NULL, 'Nullable(Int256)')";
const NULL_FLOAT: &str = "CAST(NULL, 'Nullable(Float64)')";

/// The two children of a binary node, bound to `pl` and `pr`: `l1`/`r1`
/// the integer elements, `l2`/`r2` the float elements.
const L1: &str = "tupleElement(pl, 1)";
const L2: &str = "tupleElement(pl, 2)";
const R1: &str = "tupleElement(pr, 1)";
const R2: &str = "tupleElement(pr, 2)";

/// One arithmetic value's tuple text, and whether a duration is in it —
/// which makes `/` a float division.
struct Tuple {
    text: String,
    dur: bool,
}

impl Tuple {
    /// An integer constant, `digits` its `I(v)`.
    fn int(digits: String, dur: bool) -> Tuple {
        Tuple {
            text: format!("tuple(toInt256({digits}), {NULL_FLOAT})"),
            dur,
        }
    }
}

/// `NEG(x)`: `-x` in `Int256`, with no value for the type's minimum — the
/// only value that is its own negation.
fn neg_int(x: &str) -> String {
    format!("if({x} != 0 AND negate({x}) = {x}, NULL, negate({x}))")
}

/// `l op r` over two tuples: `arrayElement(arrayMap((pl, pr) ->
/// tuple(<I>, <F>), [L], [R]), 1)`. The float element is computed only
/// when either side is a float (`H`), so an all-integer node never also
/// carries a float result.
fn binary_node(op: ArithOp, l: &Tuple, r: &Tuple) -> Tuple {
    let dur = l.dur || r.dur;
    let fl = format!("coalesce({L2}, toFloat64({L1}))");
    let fr = format!("coalesce({R2}, toFloat64({R1}))");
    let only_ints = format!("{L2} IS NULL AND {R2} IS NULL");
    let float = |sym: &str| format!("if({only_ints}, NULL, {fl} {sym} {fr})");
    let (int, float) = match op {
        ArithOp::Add => (widen(op), float("+")),
        ArithOp::Sub => (widen(op), float("-")),
        ArithOp::Mul => (widen(op), float("*")),
        // The reference's duration arithmetic is float: `3ms / 2ms` is 1.5.
        ArithOp::Div if dur => (NULL_INT.to_string(), format!("{fl} / {fr}")),
        ArithOp::Div => (int_divisor(op), float("/")),
        ArithOp::Mod => (int_divisor(op), float("%")),
        ArithOp::Pow => (
            int_pow(),
            format!("if({only_ints} AND coalesce({R1} >= 0, false), NULL, pow({fl}, {fr}))"),
        ),
    };
    Tuple {
        text: format!(
            "arrayElement(arrayMap((pl, pr) -> tuple({int}, {float}), [{}], [{}]), 1)",
            l.text, r.text
        ),
        dur,
    }
}

/// The integer element of `+`, `-` and `*`: exact in `Int256`, every
/// integer leaf having been widened to it with `toInt256`, and no value
/// for a result outside the type (decision 3 of the part-3c design: the
/// standing rule, be correct wherever we can). `+` and `-` wrap only when
/// the operands' signs say the result cannot have the sign it has. A
/// wrapped `*` differs from the `Float64` product by at least 2^256, far
/// beyond `1e-6` relative, while an exact one agrees within a few units in
/// the last place.
fn widen(op: ArithOp) -> String {
    match op {
        ArithOp::Add => format!(
            "if(({L1} > 0 AND {R1} > 0 AND {L1} + {R1} <= 0) OR ({L1} < 0 AND {R1} < 0 AND \
             {L1} + {R1} >= 0), NULL, {L1} + {R1})"
        ),
        ArithOp::Sub => format!(
            "if(({L1} >= 0 AND {R1} < 0 AND {L1} - {R1} < 0) OR ({L1} < 0 AND {R1} > 0 AND \
             {L1} - {R1} >= 0), NULL, {L1} - {R1})"
        ),
        ArithOp::Mul => format!(
            "if(abs(toFloat64({L1} * {R1}) - toFloat64({L1}) * toFloat64({R1})) <= 1e-6 * \
             abs(toFloat64({L1}) * toFloat64({R1})), {L1} * {R1}, NULL)"
        ),
        ArithOp::Div | ArithOp::Mod | ArithOp::Pow => {
            unreachable!("widen serves + - * only")
        }
    }
}

/// The integer element of `/` (truncating, `intDiv`) and `%` (the
/// dividend's sign). A zero divisor has no value, so the span does not
/// match (decision 1 of the part-3c design, owner, 2026-10-06; the
/// reference fails the query). `-1` is answered without dividing: the
/// type's minimum divided by `-1` raises in the engine. The divisor that
/// reaches `intDiv` or `%` is never 0 or `-1`, so no row can raise,
/// whichever rows the engine evaluates.
fn int_divisor(op: ArithOp) -> String {
    let safe = format!("if({R1} = 0 OR {R1} = -1, 1, {R1})");
    match op {
        ArithOp::Div => format!(
            "multiIf({R1} = 0, NULL, {R1} = -1, {}, intDiv({L1}, {safe}))",
            neg_int(L1)
        ),
        ArithOp::Mod => format!("multiIf({R1} = 0, NULL, {R1} = -1, {L1} * 0, {L1} % {safe})"),
        ArithOp::Add | ArithOp::Sub | ArithOp::Mul | ArithOp::Pow => {
            unreachable!("int_divisor serves / and % only")
        }
    }
}

/// The integer element of `^`: exact, by squaring over the exponent's
/// eight bits, in the written order (decision 2 of the part-3c design: the
/// standing rule, be correct wherever we can; the reference computes it
/// through `float64` with its operands swapped). A negative exponent has
/// no integer value, and the float element gives `pow`. Past 255 only a
/// base of 0, 1 or −1 stays inside `Int256`. A result outside the type
/// differs from `pow` by far more than `1e-6` relative and has no value.
/// `bitTest` takes the exponent through `toUInt8`: on an `Int256` it is
/// refused, and in this branch the exponent is 0…255.
fn int_pow() -> String {
    let pf = format!("pow(toFloat64({L1}), toFloat64({R1}))");
    let squaring = format!(
        "tupleElement(arrayFold((pa, pk) -> (if(bitTest(toUInt8(assumeNotNull({R1})), pk), \
         tupleElement(pa, 1) * tupleElement(pa, 2), tupleElement(pa, 1)), tupleElement(pa, 2) * \
         tupleElement(pa, 2)), range(8), (toInt256(1), assumeNotNull({L1}))), 1)"
    );
    format!(
        "if({L1} IS NULL OR {R1} IS NULL OR {R1} < 0, NULL, if({R1} >= 256, multiIf({L1} = 0, \
         0, {L1} = 1, 1, {L1} = -1, if({R1} % 2 = 0, 1, -1), NULL), arrayElement(arrayMap(pq \
         -> if(isFinite({pf}) AND abs(toFloat64(pq) - {pf}) <= 1e-6 * abs({pf}), pq, NULL), \
         [{squaring}]), 1)))"
    )
}

/// A literal-only subtree's value (the part-3c design's section 5.4).
/// `Undefined` is an integer `/` or `%` by zero: no value, exactly where
/// the tuple path answers none. `No` is a subtree that is not folded — it
/// holds a field, or folding could differ from the tuple path — and is
/// computed per span instead.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Folded {
    Int(i128),
    Dur(i128),
    U128(u128),
    Float(f64),
    Undefined,
    No,
}

impl Folded {
    /// The literal a constant compares as through part 1's leaf.
    fn constant(self) -> Option<Value> {
        match self {
            Folded::Int(v) | Folded::Dur(v) => Some(Value::Number(v.to_string())),
            Folded::U128(v) => Some(Value::Number(v.to_string())),
            Folded::Float(x) => Some(Value::Number(render_num(x))),
            Folded::Undefined | Folded::No => None,
        }
    }

    /// A constant's arms: `I(v)` or `F(x)`, never `NULL`.
    fn constant_arms(self) -> Option<Arms> {
        match self {
            Folded::Int(v) | Folded::Dur(v) => Some(Arms {
                i: Some(int_text(v)),
                ..Arms::default()
            }),
            Folded::U128(v) => Some(Arms {
                i: Some(u128_text(v)),
                ..Arms::default()
            }),
            Folded::Float(x) => Some(Arms {
                f: Some(float_text(x)),
                ..Arms::default()
            }),
            Folded::Undefined | Folded::No => None,
        }
    }

    fn as_f64(self) -> f64 {
        match self {
            Folded::Int(v) | Folded::Dur(v) => v as f64,
            Folded::U128(v) => v as f64,
            Folded::Float(x) => x,
            Folded::Undefined | Folded::No => unreachable!("only constants convert"),
        }
    }
}

/// Whether `raw` is a `Number` of decimal digits and nothing else.
fn is_digits(raw: &str) -> bool {
    !raw.is_empty() && raw.bytes().all(|b| b.is_ascii_digit())
}

/// A `Number` literal's value. Digits are exact up to 2^128 − 1 and
/// refused past it (decision 4 of the part-3c design, owner, 2026-10-06);
/// `-` and digits is `minInt`'s spelling; anything else is a float.
fn number_literal(raw: &str) -> Result<Folded, PlanError> {
    if is_digits(raw) {
        let m: u128 = raw.parse().map_err(|_| out_of_range(raw))?;
        return Ok(i128::try_from(m).map_or(Folded::U128(m), Folded::Int));
    }
    if raw.strip_prefix('-').is_some_and(is_digits) {
        return raw
            .parse::<i128>()
            .map(Folded::Int)
            .map_err(|_| out_of_range(raw));
    }
    Ok(Folded::Float(parse_num(raw)?))
}

/// `-` directly over a `Number` of digits: the lexer gives a literal's
/// sign as a unary minus, so this is the signed literal, exact down to
/// −2^127 and refused below it.
fn signed_literal(digits: &str) -> Result<Folded, PlanError> {
    let refused = || out_of_range(&format!("-{digits}"));
    let m: u128 = digits.parse().map_err(|_| refused())?;
    0i128
        .checked_sub_unsigned(m)
        .map(Folded::Int)
        .ok_or_else(refused)
}

/// The digits under a unary minus, when it is a signed literal.
fn negated_digits(expr: &FieldExpr) -> Option<&str> {
    match expr {
        FieldExpr::Unary {
            op: UnaryOp::Neg,
            expr: inner,
        } => match inner.as_ref() {
            FieldExpr::Literal(Value::Number(raw)) if is_digits(raw) => Some(raw),
            _ => None,
        },
        _ => None,
    }
}

/// Section 5.1's row 0: every integer literal anywhere in `expr`, read
/// with its sign, is inside −2^127…2^128 − 1.
fn check_literals(expr: &FieldExpr) -> Result<(), PlanError> {
    if let Some(digits) = negated_digits(expr) {
        return signed_literal(digits).map(|_| ());
    }
    match expr {
        FieldExpr::Literal(Value::Number(raw)) => number_literal(raw).map(|_| ()),
        FieldExpr::Unary { expr: inner, .. } => check_literals(inner),
        FieldExpr::Binary { lhs, rhs, .. } => {
            check_literals(lhs)?;
            check_literals(rhs)
        }
        FieldExpr::Field(_) | FieldExpr::Literal(_) | FieldExpr::Exists { .. } => Ok(()),
    }
}

/// Folds `expr` (the part-3c design's section 5.4). Integer steps are
/// `i128` checked operations; one that overflows is `No`, and the tuple
/// path computes it in `Int256`. A float result that is not finite is
/// `No` too, as is a float `^`: the libraries' `pow` may differ in the last
/// place.
fn fold(expr: &FieldExpr) -> Result<Folded, PlanError> {
    if let Some(digits) = negated_digits(expr) {
        return signed_literal(digits);
    }
    Ok(match expr {
        FieldExpr::Literal(Value::Number(raw)) => number_literal(raw)?,
        FieldExpr::Literal(Value::Duration(d)) => Folded::Dur(i128::from(d.as_nanos())),
        FieldExpr::Unary {
            op: UnaryOp::Neg,
            expr: inner,
        } => match fold(inner)? {
            Folded::Int(v) => v.checked_neg().map_or(Folded::No, Folded::Int),
            Folded::Dur(v) => v.checked_neg().map_or(Folded::No, Folded::Dur),
            Folded::Float(x) => Folded::Float(-x),
            Folded::Undefined => Folded::Undefined,
            Folded::U128(_) | Folded::No => Folded::No,
        },
        FieldExpr::Binary {
            op: FieldOp::Arith(op),
            lhs,
            rhs,
        } => fold_binary(*op, fold(lhs)?, fold(rhs)?),
        _ => Folded::No,
    })
}

fn fold_binary(op: ArithOp, l: Folded, r: Folded) -> Folded {
    use Folded::{Dur, Float, Int, No, U128, Undefined};
    match (l, r) {
        (Undefined, _) | (_, Undefined) => Undefined,
        (No | U128(_), _) | (_, No | U128(_)) => No,
        (Float(_), _) | (_, Float(_)) => {
            let (a, b) = (l.as_f64(), r.as_f64());
            let v = match op {
                ArithOp::Add => a + b,
                ArithOp::Sub => a - b,
                ArithOp::Mul => a * b,
                ArithOp::Div => a / b,
                ArithOp::Mod => a % b,
                ArithOp::Pow => return No,
            };
            if v.is_finite() { Float(v) } else { No }
        }
        (Int(a) | Dur(a), Int(b) | Dur(b)) => {
            let dur = matches!(l, Dur(_)) || matches!(r, Dur(_));
            let integer = |v: Option<i128>| match v {
                Some(v) if dur => Dur(v),
                Some(v) => Int(v),
                None => No,
            };
            match op {
                ArithOp::Add => integer(a.checked_add(b)),
                ArithOp::Sub => integer(a.checked_sub(b)),
                ArithOp::Mul => integer(a.checked_mul(b)),
                // By zero a duration's quotient is an infinity, which the
                // tuple path computes.
                ArithOp::Div if dur => {
                    if b == 0 {
                        No
                    } else {
                        Float(a as f64 / b as f64)
                    }
                }
                ArithOp::Div | ArithOp::Mod if b == 0 => Undefined,
                ArithOp::Div => integer(a.checked_div(b)),
                ArithOp::Mod => integer(a.checked_rem(b)),
                ArithOp::Pow => u32::try_from(b)
                    .ok()
                    .map_or(No, |e| integer(a.checked_pow(e))),
            }
        }
    }
}

/// `I(v)`: the digits, sign included, where a bare literal is an integer
/// type; otherwise through `toInt256`, because ClickHouse types a bare
/// integer literal past 2^64 − 1 as `Float64`, which rounds.
fn int_text(v: i128) -> String {
    if (i128::from(i64::MIN)..=i128::from(u64::MAX)).contains(&v) {
        v.to_string()
    } else {
        format!("toInt256('{v}')")
    }
}

fn u128_text(v: u128) -> String {
    if v <= u128::from(u64::MAX) {
        v.to_string()
    } else {
        format!("toInt256('{v}')")
    }
}

/// `F(x)`: through a string, so the sign of a zero is kept — `toFloat64(-0)`
/// is +0.
fn float_text(x: f64) -> String {
    format!("toFloat64('{}')", render_num(x))
}

/// A literal side's arms: a number or duration as a constant, a string,
/// boolean, status or kind as its own class. Never `NULL`.
fn literal_arms(value: &Value) -> Result<Arms, PlanError> {
    let folded = match value {
        Value::Number(raw) => number_literal(raw)?,
        Value::Duration(d) => Folded::Dur(i128::from(d.as_nanos())),
        Value::String(s) => {
            return Ok(Arms {
                s: Some(escape::ch_string(s)),
                ..Arms::default()
            });
        }
        Value::Bool(b) => {
            return Ok(Arms {
                b: Some(b.to_string()),
                ..Arms::default()
            });
        }
        Value::Status(v) => {
            return Ok(Arms {
                st: Some(status_code(*v).to_string()),
                ..Arms::default()
            });
        }
        Value::Kind(v) => {
            return Ok(Arms {
                kd: Some(kind_code(*v).to_string()),
                ..Arms::default()
            });
        }
    };
    Ok(folded
        .constant_arms()
        .expect("a number or duration literal is a constant"))
}

// ---------------------------------------------------------------------
// the attribute leaf
// ---------------------------------------------------------------------

/// A string equality: the `:String` arm and the array-membership arm,
/// both, because the compiler has no stored type for a key and
/// `docs/TraceQL/server-implementation.md` gives array membership its own
/// rule with no way to select it. The array arm is inert when the value is
/// not an `Array(Nullable(String))` — measured on the worked fixture, each
/// arm alone answers one of the two rows and neither answers both.
///
/// `=~`, `!~` and the four ordered operators read `:String` only: the
/// design names `= "v"` and nothing else, and an ordered comparison
/// against array elements has no stated meaning.
fn string_eq(string: &str, string_array: &str, literal: &str) -> String {
    let lit = escape::ch_string(literal);
    format!("(coalesce({string} = {lit}, false) OR has({string_array}, {lit}))")
}

/// The numeric pair, `Int64` before `Float64` in both directions.
///
/// The order is the one `docs/TraceQL/query-catalogue-accepted.md` carries
/// on all 48 of its coalesced numeric lines; the measurement script
/// disagrees with itself about it, so the catalogue is the authority and
/// the order is fixed once here.
fn numeric_pair(int: &str, float: &str, op: ComparisonOp, number: &str) -> String {
    let sym = sql_op(op).expect("the caller has already excluded the regex operators");
    format!("(coalesce({int} {sym} {number}, false) OR coalesce({float} {sym} {number}, false))")
}

/// `N` — the numeric literal's SQL text. **Integer first**: a `Number` of
/// digits, or `-` and digits, is `I(v)`, exact from −2^127 to 2^128 − 1 and
/// refused outside it; `parse_num` / `render_num` are used only for a
/// literal with a fraction, because the `f64` round trip does not preserve
/// an integer.
///
/// `minInt`/`maxInt` need no arm of their own: the parser turns each into
/// `Value::Number(i64::MIN.to_string())` / `i64::MAX.to_string()`, so they
/// arrive as ordinary decimal literals and take this same path.
fn render_number(raw: &str) -> Result<String, PlanError> {
    Ok(match number_literal(raw)? {
        Folded::Int(v) | Folded::Dur(v) => int_text(v),
        Folded::U128(v) => u128_text(v),
        Folded::Float(_) | Folded::Undefined | Folded::No => render_num(parse_num(raw)?),
    })
}

/// A duration literal's nanoseconds, which is the number it compares as —
/// not the digits the client wrote, so `> 1ns` and `> 1us` are different
/// comparisons. Every `u64` the parser accepts, as a bare literal.
fn duration_nanos(d: Duration) -> String {
    d.as_nanos().to_string()
}

/// One attribute comparison at `root`: `NOT ` + the positive rendering when
/// `negated`, the positive rendering alone otherwise.
fn attr_leaf(root: &str, key: &str, op: ComparisonOp, value: &Value) -> Result<String, PlanError> {
    let (negated, positive) = attr_leaf_parts(root, key, op, value)?;
    Ok(if negated {
        format!("NOT {positive}")
    } else {
        positive
    })
}

/// An attribute comparison's polarity and its positive rendering, kept
/// apart so a resource leaf can put the negation outside its subquery.
/// Negated: `!=` on a string, number, duration or boolean, and `!~`.
fn attr_leaf_parts(
    root: &str,
    key: &str,
    op: ComparisonOp,
    value: &Value,
) -> Result<(bool, String), PlanError> {
    let path = attr_path(root, key);
    leaf_parts(&mut |r| format!("{path}{}", r.suffix()), key, op, value)
}

/// `event.k` / `link.k`: any element passes the positive rendering, with
/// each typed read a lambda variable over that read's array. A negation
/// is `NOT` outside `arrayExists`.
fn element_leaf(
    root: &str,
    key: &str,
    op: ComparisonOp,
    value: &Value,
    head: &str,
) -> Result<String, PlanError> {
    let mut used: Vec<Read> = Vec::new();
    let (negated, body) = {
        let mut reader = |r: Read| {
            if !used.contains(&r) {
                used.push(r);
            }
            r.var().to_string()
        };
        leaf_parts(&mut reader, key, op, value)?
    };
    let path = attr_path(root, key);
    let vars: Vec<&str> = used.iter().map(|r| r.var()).collect();
    let args = if vars.len() > 1 {
        format!("({})", vars.join(", "))
    } else {
        vars.concat()
    };
    let arrays: Vec<String> = used
        .iter()
        .map(|r| format!("{path}{}", r.suffix()))
        .collect();
    let positive = format!("{head}({args} -> {body}, {})", arrays.join(", "));
    Ok(if negated {
        format!("NOT {positive}")
    } else {
        positive
    })
}

/// The one attribute-comparison rule, over a reader that renders each
/// typed read: a span-row path for [`attr_leaf_parts`], a lambda variable
/// for [`element_leaf`]. `key` names the attribute in the refusal.
fn leaf_parts(
    read: &mut dyn FnMut(Read) -> String,
    key: &str,
    op: ComparisonOp,
    value: &Value,
) -> Result<(bool, String), PlanError> {
    let rendered = match (op, value) {
        (ComparisonOp::Eq, Value::String(s)) => {
            (false, string_eq(&read(Read::Str), &read(Read::StrArray), s))
        }
        (ComparisonOp::Neq, Value::String(s)) => {
            (true, string_eq(&read(Read::Str), &read(Read::StrArray), s))
        }
        (
            ComparisonOp::Gt | ComparisonOp::Gte | ComparisonOp::Lt | ComparisonOp::Lte,
            Value::String(s),
        ) => (
            false,
            format!(
                "coalesce({} {} {}, false)",
                read(Read::Str),
                sql_op(op).expect("an ordered operator"),
                escape::ch_string(s)
            ),
        ),
        (ComparisonOp::Re, Value::String(s)) => (
            false,
            format!(
                "coalesce(match({}, {}), false)",
                read(Read::Str),
                anchored_regex_sql(s)?
            ),
        ),
        (ComparisonOp::Nre, Value::String(s)) => (
            true,
            format!(
                "coalesce(match({}, {}), false)",
                read(Read::Str),
                anchored_regex_sql(s)?
            ),
        ),
        (ComparisonOp::Neq, Value::Number(raw)) => (
            true,
            numeric_pair(
                &read(Read::Int),
                &read(Read::Float),
                ComparisonOp::Eq,
                &render_number(raw)?,
            ),
        ),
        (op, Value::Number(raw)) if sql_op(op).is_some() => (
            false,
            numeric_pair(
                &read(Read::Int),
                &read(Read::Float),
                op,
                &render_number(raw)?,
            ),
        ),
        (ComparisonOp::Neq, Value::Duration(d)) => (
            true,
            numeric_pair(
                &read(Read::Int),
                &read(Read::Float),
                ComparisonOp::Eq,
                &duration_nanos(*d),
            ),
        ),
        (op, Value::Duration(d)) if sql_op(op).is_some() => (
            false,
            numeric_pair(
                &read(Read::Int),
                &read(Read::Float),
                op,
                &duration_nanos(*d),
            ),
        ),
        (ComparisonOp::Eq, Value::Bool(b)) => (
            false,
            format!("coalesce({} = {b}, false)", read(Read::Bool)),
        ),
        (ComparisonOp::Neq, Value::Bool(b)) => (
            true,
            format!("(coalesce({} = {b}, false))", read(Read::Bool)),
        ),
        // The message `filter.rs` produces for the same pair, so
        // `{ span.k = error }` is the same `400` on both compilers.
        _ => {
            return Err(PlanError::TypeMismatch(format!(
                "attribute {key:?} does not support operator {op} on this value type"
            )));
        }
    };
    Ok(rendered)
}

// ---------------------------------------------------------------------
// the intrinsics
// ---------------------------------------------------------------------

/// **No wildcard arm**: a new `Intrinsic` variant fails to compile here
/// rather than falling silently into the wrong half.
fn intrinsic_leaf(
    intrinsic: Intrinsic,
    op: ComparisonOp,
    value: &Value,
    head: &str,
) -> Result<String, PlanError> {
    match intrinsic {
        Intrinsic::Name => string_column_leaf(intrinsic, "name", op, value),
        Intrinsic::StatusMessage => string_column_leaf(intrinsic, "status_message", op, value),
        Intrinsic::Kind => kind_leaf(op, value),
        Intrinsic::Status => status_leaf(op, value),
        Intrinsic::Duration => duration_leaf(op, value),
        Intrinsic::SpanId => id_leaf(intrinsic, IdColumn::SPAN_ID, op, value),
        Intrinsic::ParentId => id_leaf(intrinsic, IdColumn::PARENT_SPAN_ID, op, value),
        Intrinsic::TraceId => id_leaf(intrinsic, IdColumn::TRACE_ID, op, value),
        Intrinsic::InstrumentationName => {
            untyped_string_leaf(&intrinsic.to_string(), "scope_name", op, value)
        }
        Intrinsic::InstrumentationVersion => {
            untyped_string_leaf(&intrinsic.to_string(), "scope_version", op, value)
        }
        Intrinsic::EventName => event_name_leaf(op, value, head),
        Intrinsic::EventTimeSinceStart => time_since_start_leaf(op, value, head),
        Intrinsic::LinkSpanId => link_id_leaf(intrinsic, "links.span_id", op, value, head),
        Intrinsic::LinkTraceId => link_id_leaf(intrinsic, "links.trace_id", op, value, head),
        Intrinsic::NestedSetParent | Intrinsic::NestedSetLeft | Intrinsic::NestedSetRight => {
            Err(unsupported_intrinsic(intrinsic, NESTED_AND_TRACE))
        }
        Intrinsic::ChildCount
        | Intrinsic::TraceDuration
        | Intrinsic::RootName
        | Intrinsic::RootServiceName => unreachable!("taken by Compiler::leaf"),
    }
}

/// `name` and `statusMessage`: a non-nullable `String`/`LowCardinality`
/// column, so no `coalesce`.
///
/// **All eight operators**, which is where this compiler differs from the
/// shipped one. `{ name > "x" }` is a reference `200` — strings order —
/// and our own validator accepts it
/// (`crates/pulsus-traceql/src/validate.rs`); `filter.rs`'s
/// `string_op_leaf` answers `400` there, and that is strictness this
/// compiler does not inherit.
fn string_column_leaf(
    intrinsic: Intrinsic,
    column: &str,
    op: ComparisonOp,
    value: &Value,
) -> Result<String, PlanError> {
    let Value::String(s) = value else {
        return Err(PlanError::TypeMismatch(format!(
            "{intrinsic} requires a string value"
        )));
    };
    string_column_text(column, op, s)
}

/// `<column> <op> <s>` over a non-nullable string, under all eight
/// operators. An invalid pattern is refused here, at compile time: the
/// engine would fail the statement.
pub(super) fn string_column_text(
    column: &str,
    op: ComparisonOp,
    s: &str,
) -> Result<String, PlanError> {
    Ok(match op {
        ComparisonOp::Re => format!("match({column}, {})", anchored_regex_sql(s)?),
        ComparisonOp::Nre => format!("NOT match({column}, {})", anchored_regex_sql(s)?),
        other => format!(
            "{column} {} {}",
            sql_op(other).expect("the six non-regex operators"),
            escape::ch_string(s)
        ),
    })
}

/// `instrumentation:name` / `instrumentation:version`, and
/// `resource.service.name`'s refusals and non-string fold, which keep
/// `filter.rs`'s rule unchanged — the cross-type `=`/`!=` fold to `false`
/// included. `name` is the field as the messages spell it.
///
/// `docs/api.md` names these two fields (with `resource.service.name`) and
/// says of them that a cross-type `=~`/`!~` and **any ordered comparison
/// at those fields stay `400`**. That sentence names the fields and the
/// operators; the field-vs-field paragraph beside it names neither, so it
/// does not reach here.
fn untyped_string_leaf(
    name: &str,
    column: &str,
    op: ComparisonOp,
    value: &Value,
) -> Result<String, PlanError> {
    if !matches!(
        op,
        ComparisonOp::Eq | ComparisonOp::Neq | ComparisonOp::Re | ComparisonOp::Nre
    ) {
        return Err(PlanError::TypeMismatch(format!(
            "{name} supports only = != =~ !~"
        )));
    }
    match value {
        Value::String(s) => Ok(match op {
            ComparisonOp::Re => format!("match({column}, {})", anchored_regex_sql(s)?),
            ComparisonOp::Nre => format!("NOT match({column}, {})", anchored_regex_sql(s)?),
            other => format!(
                "{column} {} {}",
                sql_op(other).expect("= or !="),
                escape::ch_string(s)
            ),
        }),
        // A cross-type `=`/`!=` is a query the reference's own validator
        // accepts and answers, so it matches no span rather than being a
        // plan error. Both spellings fold to `false`, as `filter.rs`'s
        // `never_matching_leaf` does.
        _ if matches!(op, ComparisonOp::Eq | ComparisonOp::Neq) => Ok("false".to_string()),
        _ => Err(PlanError::TypeMismatch(format!(
            "{name} requires a string value"
        ))),
    }
}

fn kind_leaf(op: ComparisonOp, value: &Value) -> Result<String, PlanError> {
    let Value::Kind(k) = value else {
        return Err(PlanError::TypeMismatch(
            "kind requires a span-kind keyword".to_string(),
        ));
    };
    if !matches!(op, ComparisonOp::Eq | ComparisonOp::Neq) {
        return Err(PlanError::TypeMismatch(
            "kind supports only = and !=".to_string(),
        ));
    }
    Ok(format!(
        "kind {} {}",
        sql_op(op).expect("= or !="),
        kind_code(*k)
    ))
}

fn status_leaf(op: ComparisonOp, value: &Value) -> Result<String, PlanError> {
    let Value::Status(s) = value else {
        return Err(PlanError::TypeMismatch(
            "status requires ok|error|unset".to_string(),
        ));
    };
    if !matches!(op, ComparisonOp::Eq | ComparisonOp::Neq) {
        return Err(PlanError::TypeMismatch(
            "status supports only = and !=".to_string(),
        ));
    }
    Ok(format!(
        "status_code {} {}",
        sql_op(op).expect("= or !="),
        status_code(*s)
    ))
}

/// `duration` reads `duration_ns`, the stored column — not
/// `end_ns - start_ns`, which is the same number on the worked fixture and
/// so is not what distinguishes them.
///
/// The operand is checked BEFORE the operator, which is `filter.rs`'s own
/// order: that is what makes `{ duration =~ "x" }` answer "requires a
/// duration literal" on both compilers rather than two different messages
/// for one query.
fn duration_leaf(op: ComparisonOp, value: &Value) -> Result<String, PlanError> {
    let Value::Duration(d) = value else {
        return Err(PlanError::TypeMismatch(
            "duration requires a duration literal".to_string(),
        ));
    };
    let Some(sym) = sql_op(op) else {
        return Err(PlanError::TypeMismatch(
            "duration does not support regex operators".to_string(),
        ));
    };
    Ok(format!("duration_ns {sym} {}", duration_nanos(*d)))
}

// ---------------------------------------------------------------------
// the three id columns
// ---------------------------------------------------------------------

/// One id column: its name, its stored byte width, and whether the
/// raw-byte literal is wrapped back into the column's own `FixedString`.
///
/// **The wrap is on `trace_id` alone and it changes nothing measurable.**
/// It gives the right side the column's own `FixedString(16)` type.
/// `EXPLAIN indexes = 1` prints the same primary-key condition with and
/// without it (26.3.29.7, issue #589), and `toFixedString` over exactly
/// `width` bytes is the identity, so no answer moves either. `T-C9` pins
/// the text.
#[derive(Debug, Clone, Copy)]
struct IdColumn {
    name: &'static str,
    byte_width: usize,
    wrap_fixed_string: bool,
}

impl IdColumn {
    const SPAN_ID: IdColumn = IdColumn {
        name: "span_id",
        byte_width: 8,
        wrap_fixed_string: false,
    };
    const PARENT_SPAN_ID: IdColumn = IdColumn {
        name: "parent_span_id",
        byte_width: 8,
        wrap_fixed_string: false,
    };
    const TRACE_ID: IdColumn = IdColumn {
        name: "trace_id",
        byte_width: 16,
        wrap_fixed_string: true,
    };
}

/// `span:id`, `span:parentID`, `trace:id`.
///
/// Two rules, both answer-inverting if dropped:
///
/// * **The literal is lowercased for `=`/`!=` and for nothing else.** The
///   stored hex is lowercase, so an uppercase probe must be folded to
///   match it — but a regex may be deliberately case-sensitive, and an
///   ordered comparison is lexicographic on the hex TEXT, where `'0a'`
///   and `'0A'` are different bounds.
/// * **The raw-byte fast path carries `<op>`, not a hard-coded `=`.**
///
/// The width guard is the third: `unhex('')` is zero bytes and a
/// `FixedString` right-pads, so without it `{ span:parentID = "" }` equals
/// every root span instead of nothing.
fn id_leaf(
    intrinsic: Intrinsic,
    column: IdColumn,
    op: ComparisonOp,
    value: &Value,
) -> Result<String, PlanError> {
    let Value::String(raw) = value else {
        return Err(PlanError::TypeMismatch(format!(
            "{intrinsic} requires a string value"
        )));
    };
    let equality = matches!(op, ComparisonOp::Eq | ComparisonOp::Neq);
    let literal = if equality {
        raw.to_lowercase()
    } else {
        raw.clone()
    };
    let full_width_hex = literal.len() == 2 * column.byte_width
        && literal
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
    if equality && full_width_hex {
        let bytes = format!("unhex({})", escape::ch_string(&literal));
        let right = if column.wrap_fixed_string {
            format!("toFixedString({bytes}, {})", column.byte_width)
        } else {
            bytes
        };
        return Ok(format!(
            "{} {} {right}",
            column.name,
            sql_op(op).expect("= or !=")
        ));
    }
    let hex = format!("lower(hex({}))", column.name);
    Ok(match op {
        ComparisonOp::Re => format!("match({hex}, {})", anchored_regex_sql(&literal)?),
        ComparisonOp::Nre => format!("NOT match({hex}, {})", anchored_regex_sql(&literal)?),
        other => format!(
            "{hex} {} {}",
            sql_op(other).expect("the six non-regex operators"),
            escape::ch_string(&literal)
        ),
    })
}

// ---------------------------------------------------------------------
// the four event and link intrinsics
// ---------------------------------------------------------------------

/// `!=` to `=` and `!~` to `=~`, with whether it was negated; the other
/// operators unchanged.
fn positive_op(op: ComparisonOp) -> (bool, ComparisonOp) {
    match op {
        ComparisonOp::Neq => (true, ComparisonOp::Eq),
        ComparisonOp::Nre => (true, ComparisonOp::Re),
        ComparisonOp::Eq
        | ComparisonOp::Gt
        | ComparisonOp::Gte
        | ComparisonOp::Lt
        | ComparisonOp::Lte
        | ComparisonOp::Re => (false, op),
    }
}

/// Any element of `array`, bound to `var`, passes `body`; `NOT` outside
/// when negated. `head` is the loop's function: `arrayExists`, or
/// `arrayFirstIndex` for the search projection's first passing element.
fn any_element(var: &str, array: &str, negated: bool, body: &str, head: &str) -> String {
    let positive = format!("{head}({var} -> {body}, {array})");
    if negated {
        format!("NOT {positive}")
    } else {
        positive
    }
}

/// `event:name`: `name`'s rule over each event's name.
fn event_name_leaf(op: ComparisonOp, value: &Value, head: &str) -> Result<String, PlanError> {
    let (negated, pos) = positive_op(op);
    let body = string_column_leaf(Intrinsic::EventName, "n", pos, value)?;
    Ok(any_element("n", "events.name", negated, &body, head))
}

/// `event:timeSinceStart`: each event's time less the span's start, in
/// nanoseconds. `time_ns` is `UInt64` and `start_ns` `Int64`, so the
/// difference is taken as `Int128`: exact for every stored pair, and
/// negative for an event before its span. A bare number is nanoseconds.
/// The operand is checked before the operator, as `duration`'s is.
fn time_since_start_leaf(op: ComparisonOp, value: &Value, head: &str) -> Result<String, PlanError> {
    let nanos = match value {
        Value::Duration(d) => duration_nanos(*d),
        Value::Number(raw) => render_number(raw)?,
        _ => {
            return Err(PlanError::TypeMismatch(
                "event:timeSinceStart requires a duration or a number".to_string(),
            ));
        }
    };
    let (negated, pos) = positive_op(op);
    let Some(sym) = sql_op(pos) else {
        return Err(PlanError::TypeMismatch(
            "event:timeSinceStart does not support regex operators".to_string(),
        ));
    };
    Ok(any_element(
        "t",
        "events.time_ns",
        negated,
        &format!("toInt128(t) - start_ns {sym} {nanos}"),
        head,
    ))
}

/// `link:spanID` / `link:traceID`: each link's id as lowercase hex. The
/// literal is lowercased for `=`/`!=` only, as [`id_leaf`]'s is. A link id
/// is a `String` of any length with no index, so there is no raw-byte
/// fast path and no width to guard.
fn link_id_leaf(
    intrinsic: Intrinsic,
    array: &str,
    op: ComparisonOp,
    value: &Value,
    head: &str,
) -> Result<String, PlanError> {
    let Value::String(raw) = value else {
        return Err(PlanError::TypeMismatch(format!(
            "{intrinsic} requires a string value"
        )));
    };
    let (negated, pos) = positive_op(op);
    let body = match pos {
        ComparisonOp::Re => format!("match(lower(hex(h)), {})", anchored_regex_sql(raw)?),
        ComparisonOp::Eq => format!("lower(hex(h)) = {}", escape::ch_string(&raw.to_lowercase())),
        other => format!(
            "lower(hex(h)) {} {}",
            sql_op(other).expect("the four ordered operators"),
            escape::ch_string(raw)
        ),
    };
    Ok(any_element("h", array, negated, &body, head))
}
