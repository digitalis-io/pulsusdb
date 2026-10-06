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
//! `duration`, `status` and `kind`, and `resource.service.name`'s string
//! ([`field_terms`]); and `&&`, `||` and `!`. Every other field and
//! construct is
//! [`PlanError::UnsupportedField`] naming itself and the issue that
//! serves it. The arms are exhaustive with no wildcard, so a
//! new `Intrinsic` or `AttrScope` variant fails to compile here rather
//! than falling into the wrong one.
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
//!   `9223372036854776000`, which ClickHouse types `UInt64`. [`render_number`]
//!   is where that is decided.
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
    AttrScope, BoolOp, ComparisonOp, Duration, Field, FieldExpr, FieldOp, Intrinsic, UnaryOp, Value,
};

use crate::logql::escape;
use crate::traces::filter::{
    PlanError, anchored_regex_sql, flip_comparison, kind_code, parse_num, render_num, sql_op,
    status_code,
};
use crate::traces::window_sql::WindowSql;

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
}

impl SpanPredicate {
    pub fn sql(&self) -> &str {
        &self.sql
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
        p.window.is_none_or(|pw| pw == w),
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
const PART_3B: &str = "#589 part 3b";
const PART_3C: &str = "#589 part 3c";
const NESTED_AND_TRACE: &str = "#594";

/// The refusal every deferred construct takes: it names the construct and
/// the issue that serves it.
fn unsupported(construct: &str, target: &str) -> PlanError {
    PlanError::UnsupportedField(format!(
        "{construct} is not supported by the span-scope predicate compiler yet (issue {target})"
    ))
}

fn unsupported_intrinsic(intrinsic: Intrinsic, target: &str) -> PlanError {
    unsupported(&format!("{intrinsic}"), target)
}

fn unsupported_arithmetic() -> PlanError {
    unsupported("arithmetic in a span-scope predicate", PART_3C)
}

/// A `resource.` or `.` field compiled with no [`PredicateCtx`]: both
/// read the resource table through a subquery bounded by the window.
fn needs_window(scope: AttrScope) -> PlanError {
    PlanError::UnsupportedField(format!(
        "the \"{scope}\" attribute scope needs the request window: compile it with \
         compile_span_predicate_in (issue #589)"
    ))
}

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
}

impl<'a> Compiler<'a> {
    fn new(ctx: Option<PredicateCtx<'a>>) -> Self {
        Compiler {
            ctx,
            uses_resources: false,
            demands: Vec::new(),
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
            } => {
                if is_arithmetic(lhs) || is_arithmetic(rhs) {
                    return Err(unsupported_arithmetic());
                }
                if let (Some(field), FieldExpr::Literal(value)) = (not_of_field(lhs), rhs.as_ref())
                {
                    return self.not_field(field, bool_want(*op, value));
                }
                if let (FieldExpr::Literal(value), Some(field)) = (lhs.as_ref(), not_of_field(rhs))
                {
                    return self.not_field(field, bool_want(flip_comparison(*op), value));
                }
                match (lhs.as_ref(), rhs.as_ref()) {
                    (FieldExpr::Field(field), FieldExpr::Literal(value)) => {
                        self.leaf(field, *op, value)
                    }
                    (FieldExpr::Literal(value), FieldExpr::Field(field)) => {
                        self.leaf(field, flip_comparison(*op), value)
                    }
                    (FieldExpr::Field(lhs_field), FieldExpr::Field(rhs_field)) => {
                        self.field_compare(lhs_field, *op, rhs_field)
                    }
                    (FieldExpr::Literal(_), FieldExpr::Literal(_)) => {
                        Err(unsupported("a comparison between two literals", PART_3C))
                    }
                    _ => Err(unsupported(
                        "a comparison with a boolean-valued side other than `(!field) op literal`",
                        PART_3C,
                    )),
                }
            }
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
            } => Err(unsupported_arithmetic()),
            FieldExpr::Unary {
                op: UnaryOp::Not,
                expr: inner,
            } => match inner.as_ref() {
                FieldExpr::Field(field) => self.not_field(field, Some(false)),
                e if is_arithmetic(e) => Err(unsupported_arithmetic()),
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
    fn not_field(&mut self, field: &Field, want: Option<bool>) -> Result<String, PlanError> {
        let (scope, key) = match field {
            Field::Intrinsic(intrinsic) => {
                return Err(PlanError::TypeMismatch(format!(
                    "expression (!{intrinsic}) expected a boolean"
                )));
            }
            Field::Attribute { scope, key } => (*scope, key),
        };
        let (matches, condition) = self.not_parts(scope, key, want)?;
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
    ) -> Result<(String, String), PlanError> {
        Ok(match scope {
            AttrScope::Span => not_attr(ATTRS, key, want),
            AttrScope::Instrumentation => not_attr(SCOPE_ATTRS, key, want),
            AttrScope::Event => not_element(EVENT_ATTRS, key, want),
            AttrScope::Link => not_element(LINK_ATTRS, key, want),
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
            AttrScope::Unscoped => self.not_chain(key, want)?,
        })
    }

    /// `!.key`: section 5.1's chain over each scope's `T` and over each
    /// scope's demand condition, both defaulting to `false`.
    fn not_chain(&mut self, key: &str, want: Option<bool>) -> Result<(String, String), PlanError> {
        self.ctx.ok_or_else(|| needs_window(AttrScope::Unscoped))?;
        let mut t_args = Vec::with_capacity(2 * CHAIN.len());
        let mut c_args = Vec::with_capacity(2 * CHAIN.len());
        for scope in CHAIN {
            let present = self.chain_presence(scope, key)?;
            let (t, c) = self.not_parts(scope, key, want)?;
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
            AttrScope::Event => Ok(element_exists(EVENT_ATTRS, key, negated)),
            AttrScope::Link => Ok(element_exists(LINK_ATTRS, key, negated)),
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
            Field::Intrinsic(intrinsic) => intrinsic_leaf(*intrinsic, op, value),
        }
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
            AttrScope::Event => element_leaf(EVENT_ATTRS, key, op, value),
            AttrScope::Link => element_leaf(LINK_ATTRS, key, op, value),
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

    /// `L op R`, both sides fields. The regex operators are refused
    /// first, then each operand in turn, left before right; what remains
    /// is [`field_terms`] over the two operands' arms.
    fn field_compare(
        &mut self,
        lhs: &Field,
        op: ComparisonOp,
        rhs: &Field,
    ) -> Result<String, PlanError> {
        if matches!(op, ComparisonOp::Re | ComparisonOp::Nre) {
            return Err(PlanError::TypeMismatch(
                "a field-against-field comparison does not support regex operators".to_string(),
            ));
        }
        let l = self.operand_arms(lhs)?;
        let r = self.operand_arms(rhs)?;
        Ok(field_terms(&l, op, &r))
    }

    /// One operand's arms: a typed read per class it can hold. **No
    /// wildcard arm**, so a new `Intrinsic` or `AttrScope` variant fails to
    /// compile here until it is classified.
    fn operand_arms(&self, field: &Field) -> Result<Arms, PlanError> {
        let unsupported_operand = |what: String| {
            unsupported(
                &format!("a field-against-field comparison with {what}"),
                PART_3B,
            )
        };
        let intrinsic = match field {
            Field::Attribute { scope, key } => {
                return match scope {
                    AttrScope::Span => Ok(Arms::attribute(ATTRS, key)),
                    AttrScope::Instrumentation => Ok(Arms::attribute(SCOPE_ATTRS, key)),
                    AttrScope::Resource if key == SERVICE_NAME => {
                        self.ctx.ok_or_else(|| needs_window(AttrScope::Resource))?;
                        Ok(Arms {
                            s: Some(format!("if({SERVICE_IS_STRING}, service, NULL)")),
                            nullable: true,
                            ..Arms::default()
                        })
                    }
                    AttrScope::Resource
                    | AttrScope::Event
                    | AttrScope::Link
                    | AttrScope::Unscoped => {
                        Err(unsupported_operand(format!("a \"{scope}\" operand")))
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
            | Intrinsic::LinkTraceId => Err(unsupported_operand(intrinsic.to_string())),
            Intrinsic::NestedSetParent
            | Intrinsic::NestedSetLeft
            | Intrinsic::NestedSetRight
            | Intrinsic::ChildCount
            | Intrinsic::TraceDuration
            | Intrinsic::RootName
            | Intrinsic::RootServiceName => Err(unsupported_intrinsic(intrinsic, NESTED_AND_TRACE)),
        }
    }
}

/// The scopes of the unscoped chain, in the order a span's one value is
/// taken from (`docs/api.md`, the unscoped attribute).
const CHAIN: [AttrScope; 5] = [
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
/// non-boolean.
fn not_element(root: &str, key: &str, want: Option<bool>) -> (String, String) {
    let path = attr_path(root, key);
    (
        want.map_or_else(
            || "false".to_string(),
            |w| format!("arrayExists(b -> coalesce(b = {w}, false), {path}.:Bool)"),
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
fn attr_path(root: &str, key: &str) -> String {
    format!("{root}.{}", escape::ch_ident(&escape_json_path(key)))
}

/// Any element holds the key. Absence is `NOT` outside, so a span carrying
/// no elements lacks it.
fn element_exists(root: &str, key: &str, negated: bool) -> String {
    let present = format!(
        "arrayExists(d -> dynamicType(d) != 'None', {})",
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
        }
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
        terms.push(if coalesced {
            format!("coalesce({c}, false)")
        } else {
            c
        });
    }
    // The array on the right, then the array on the left.
    for (c, e) in ARRAY_PAIRS {
        if ordered && !c.is_ordered() {
            continue;
        }
        if let (Some(scalar), Some(array)) = (l.scalar(c), r.array(e)) {
            terms.push(array_term(
                op,
                sym,
                &pair_side(c, e, scalar),
                &pair_side(e, c, "x"),
                array,
            ));
        }
    }
    for (c, e) in ARRAY_PAIRS {
        if ordered && !c.is_ordered() {
            continue;
        }
        if let (Some(array), Some(scalar)) = (l.array(e), r.scalar(c)) {
            terms.push(array_term(
                op,
                sym,
                &pair_side(e, c, "x"),
                &pair_side(c, e, scalar),
                array,
            ));
        }
    }
    if terms.is_empty() {
        "false".to_string()
    } else {
        format!("({})", terms.join(" OR "))
    }
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

/// `N` — the numeric literal's SQL text. **Integer first**: `parse_num` /
/// `render_num` are used only for a literal that is not an `i64`, because
/// the `f64` round trip does not preserve one.
///
/// `minInt`/`maxInt` need no arm of their own: the parser turns each into
/// `Value::Number(i64::MIN.to_string())` / `i64::MAX.to_string()`, so they
/// arrive as ordinary decimal literals and take this same path.
fn render_number(raw: &str) -> Result<String, PlanError> {
    if let Ok(i) = raw.parse::<i64>() {
        return Ok(i.to_string());
    }
    Ok(render_num(parse_num(raw)?))
}

/// A duration literal's nanoseconds, which is the number it compares as —
/// not the digits the client wrote, so `> 1ns` and `> 1us` are different
/// comparisons.
fn duration_nanos(d: Duration) -> Result<String, PlanError> {
    i64::try_from(d.as_nanos())
        .map(|n| n.to_string())
        .map_err(|_| PlanError::TypeMismatch("duration literal exceeds the i64 range".to_string()))
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
    let positive = format!("arrayExists({args} -> {body}, {})", arrays.join(", "));
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
                &duration_nanos(*d)?,
            ),
        ),
        (op, Value::Duration(d)) if sql_op(op).is_some() => (
            false,
            numeric_pair(
                &read(Read::Int),
                &read(Read::Float),
                op,
                &duration_nanos(*d)?,
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
        Intrinsic::EventName => event_name_leaf(op, value),
        Intrinsic::EventTimeSinceStart => time_since_start_leaf(op, value),
        Intrinsic::LinkSpanId => link_id_leaf(intrinsic, "links.span_id", op, value),
        Intrinsic::LinkTraceId => link_id_leaf(intrinsic, "links.trace_id", op, value),
        Intrinsic::NestedSetParent
        | Intrinsic::NestedSetLeft
        | Intrinsic::NestedSetRight
        | Intrinsic::ChildCount
        | Intrinsic::TraceDuration
        | Intrinsic::RootName
        | Intrinsic::RootServiceName => Err(unsupported_intrinsic(intrinsic, NESTED_AND_TRACE)),
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
    Ok(format!("duration_ns {sym} {}", duration_nanos(*d)?))
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
/// when negated.
fn any_element(var: &str, array: &str, negated: bool, body: &str) -> String {
    let positive = format!("arrayExists({var} -> {body}, {array})");
    if negated {
        format!("NOT {positive}")
    } else {
        positive
    }
}

/// `event:name`: `name`'s rule over each event's name.
fn event_name_leaf(op: ComparisonOp, value: &Value) -> Result<String, PlanError> {
    let (negated, pos) = positive_op(op);
    let body = string_column_leaf(Intrinsic::EventName, "n", pos, value)?;
    Ok(any_element("n", "events.name", negated, &body))
}

/// `event:timeSinceStart`: each event's time less the span's start, in
/// nanoseconds. `time_ns` is `UInt64` and `start_ns` `Int64`, so the
/// difference is taken as `Int128`: exact for every stored pair, and
/// negative for an event before its span. A bare number is nanoseconds.
/// The operand is checked before the operator, as `duration`'s is.
fn time_since_start_leaf(op: ComparisonOp, value: &Value) -> Result<String, PlanError> {
    let nanos = match value {
        Value::Duration(d) => duration_nanos(*d)?,
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
    Ok(any_element("h", array, negated, &body))
}
