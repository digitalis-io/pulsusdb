//! The route-independent semantic pass (issue #328) — the port of the
//! reference's `traceql.Validate` (grafana/tempo v3.0.2 `validate.go:3`
//! → `ast_validate.go:21-319`, plus `ast_metrics.go:312-342/396-401/
//! 540-547` and `engine_metrics_compare.go:319-341`), run where the
//! reference's query-frontend middleware runs it
//! (`async_query_validator_middleware.go:51`): after parse, before
//! planning, on EVERY TraceQL query parameter — including search's
//! shadow `query`, which is validated and never executed.
//!
//! **Route independence is structural, not conventional.** This crate
//! cannot name `pulsus_read` (the reverse dependency edge is a cargo
//! cycle: `pulsus-read` depends on `pulsus-traceql`), so the planner's
//! context types (`PlanError`, `plan_search`, `SearchCtx`) are
//! unnameable here, and [`validate`]'s signature carries no route, no
//! parameter name and no request context to branch on. The planner is
//! stricter than the reference's validator in places (`{} | rate()` on
//! the search route), so reusing it for the shadow parameter would
//! manufacture a 400 the reference does not have; this pass accepts
//! `{} | rate()` exactly as the reference's validator does. What the
//! cycle does NOT prove is *separation* — a planner-only rule could
//! still be re-implemented here by hand; the behavioural allowlist in
//! `pulsus-read/tests/validate_is_not_the_planner.rs` and the
//! [`VALIDATE_RULES`] citation table are what stand against that, and
//! the residual (a copied rule narrower than every witness) is stated
//! there rather than claimed away.
//!
//! **Every accept/reject decision below is container-measured**, not
//! derived from reading source: the vectors in
//! `tests/conformance/validate-vectors.json` record the pinned
//! `grafana/tempo@sha256:aa8df8d0…` verdict per shape and are re-checked
//! live (fail-closed) in CI. Where our AST cannot represent the
//! reference's distinction, the divergence is measured and ledgered —
//! see the `divergence` rows in the vectors and
//! docs/benchmarks/traces-differential-ledger.md.
//!
//! # What is checked (the reference's own list, see [`VALIDATE_RULES`])
//!
//! 1. Binary operand types must match (`{ name = duration }` →
//!    [`ValidateError::TypeMismatch`]); an `Attribute`-typed operand
//!    matches anything. Applies to EVERY binary class — comparison,
//!    `&&`/`||` and arithmetic (`{ .a = 1 && "x" }`, `{ name * 2 }`).
//! 2. The operator must be legal for BOTH operand types
//!    (`{ status > ok }` → [`ValidateError::IllegalOperator`]), per
//!    class: comparison has a per-type operator set, `&&`/`||` want
//!    booleans, arithmetic wants numbers.
//! 3. A `=~`/`!~` RHS must be a string static whose pattern an
//!    RE2-family engine accepts — decided by
//!    [`pulsus_re2::re2_verdict`], with `Unknown` ACCEPTED (an
//!    over-rejection breaks queries the reference serves; the measured
//!    `Unknown`-and-reference-rejects residual is ledgered, owner
//!    #336).
//! 4. `= nil` is illegal on any intrinsic and on scoped
//!    `resource.service.name` ([`ValidateError::IntrinsicNotNil`];
//!    `!= nil` is accepted everywhere — measured).
//! 5. `quantile_over_time` quantile literals must lie in `[0, 1]`
//!    (inclusive — `0` and `1` both measured accepted).
//! 6. Metrics `by(...)` arity: at most 5 keys, at most 4 for
//!    `histogram_over_time`/`quantile_over_time` (measured: `by` of 5
//!    is accepted for `rate()` and rejected for `histogram_over_time`).
//! 7. `topk`/`bottomk` limits must be positive.
//! 8. `compare()` cannot be combined with any other pipeline stage
//!    (measured: `… | compare(…) | topk(3)` is the reference's own
//!    Validate error; `… | rate() | compare(…)` and
//!    `… | compare(…) | rate()` are parse errors THERE and parse HERE,
//!    so this check carries their rejection too — same 400 surface,
//!    different layer).
//! 9. A unary operator must be legal for its operand's type — `!` takes
//!    a boolean, `-` a number ([`ValidateError::IllegalUnaryOperator`];
//!    the reference words the unary failure in the SINGULAR, "the given
//!    type", against rule 2's plural).
//! 10. A spanset filter's body must RESOLVE to a boolean (`{ name }`,
//!     `{ 1 }`, `{ .a + 1 }` →
//!     [`ValidateError::SpansetFilterNotBoolean`]); an attribute-typed
//!     body is accepted, since its type is unknown until the span is read
//!     (`{ .a }` is truthiness).
//! 11. An aggregate's argument must resolve to a NUMBER type (or an
//!     attribute, whose type is unknown until the span is read —
//!     [`ValidateError::AggregateNotNumeric`]) **and** must reference
//!     the span ([`ValidateError::AggregateNotSpanReferencing`]). Two
//!     variants because the reference words them differently and the
//!     order between them is observable; one reference check, split the
//!     way `invalid-regex`/`invalid-regex-operand` already are.
//!
//! Rules 9 and 10, and the widening of rules 1 and 2 past comparison,
//! arrived with the issue #335 Stage B grammar collapse. Rule 11
//! arrived with Stage C, which made the aggregate argument a full field
//! expression (D7). They were not
//! missed before: they were UNREACHABLE while the layered parser could
//! not build the offending shapes, and the collapsed grammar builds them
//! exactly as the reference's grammar does. Every one is measured against
//! the pinned digest — see each variant's doc for the queries.
//!
//! Every OTHER check on the reference's list is unreachable here
//! because our parser is stricter — each has a parse-guard in the test
//! module that goes RED the moment the parser loosens, so the missing
//! check cannot go silently missing (tiers per issue #328 D4; the
//! sampled families are named as sampled).

use crate::ast::{
    AggregateOp, AttrScope, ComparisonOp, Field, FieldExpr, FieldOp, Intrinsic, MetricFn,
    MetricStage, PipelineStage, Query, SecondStage, SpansetExpr, SpansetFilter, UnaryOp, Value,
};
use pulsus_re2::{Re2Verdict, re2_verdict};
use thiserror::Error;

/// The reference's `maxGroupBys` (`engine_metrics.go:639`): the metrics
/// `by(...)` arity bound. `histogram_over_time`/`quantile_over_time`
/// spend one slot on their implicit bucket grouping, so their user
/// bound is one lower (measured: `by` of 5 keys is accepted for
/// `rate()` and rejected for `histogram_over_time`).
const MAX_GROUP_BYS: usize = 5;

/// A semantic rejection from [`validate`]. Messages are PulsusDB's own
/// (the reference's parser/validator wording has never been an
/// identity); the status, the accept/reject decision and the checked
/// surface are what match the reference, vector for vector.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ValidateError {
    /// `ast_validate.go:198-219` — both operands of a comparison must
    /// have matching types (an `Attribute` operand matches anything).
    #[error("binary operations must operate on the same type: {expr}")]
    TypeMismatch { expr: String },
    /// `ast_validate.go:198-219` via `enum_operators.go:77-121` — the
    /// operator must be valid for both operand types.
    #[error("illegal operation for the given types: {expr}")]
    IllegalOperator { expr: String },
    /// `ast_validate.go:221-245` — a `=~`/`!~` RHS string must be a
    /// pattern an RE2-family engine accepts. The `invalid regex`
    /// literal in this message is load-bearing (issue #328 D6).
    #[error("invalid regex {value:?}: {reason}")]
    InvalidRegex { value: String, reason: String },
    /// A `=~`/`!~` right-hand side must be a string literal.
    ///
    /// Before the issue #335 Stage B collapse this was unreachable: the
    /// layered parser refused a field RHS for a regex operator, so the
    /// validator needed no rule. The collapse removed that guard — one
    /// operand grammar cannot know the operator — which silently widened
    /// the accept surface until this rule landed. **Found by the
    /// dual-position token enumeration, not by a probe.**
    ///
    /// Measured against the pinned reference: `{ .a =~ .b }`,
    /// `{ .a !~ .b }` and `{ name =~ .b }` are 400 `invalid type for =~
    /// or !~: .b` — a SEMANTIC error (no position, names the
    /// subexpression), which is why the rule belongs here and not in the
    /// parser.
    #[error("invalid type for =~ or !~: {operand}")]
    InvalidRegexOperand { operand: String },
    /// A spanset filter's body must RESOLVE to a boolean.
    ///
    /// Before the Stage B collapse the layered parser could only build a
    /// boolean-shaped body, so no rule was needed. The collapsed grammar
    /// parses any field expression between the braces — `{ name }`,
    /// `{ 1 }`, `{ .a + 1 }` — which is the reference's grammar too; the
    /// reference rejects them in its validator, and so must we.
    ///
    /// An ATTRIBUTE-typed body is accepted: `{ .a }` and `{ -.a }` are
    /// reference 200s because the type is unknown until the span is read
    /// (`{ .a }` is truthiness). Only a body whose type is statically
    /// known and non-boolean is rejected.
    ///
    /// Measured, pinned reference: `{ name }`, `{ duration }`, `{ kind }`,
    /// `{ 1 }`, `{ "x" }`, `{ 1s }`, `{ ok }`, `{ server }`,
    /// `{ nestedSetLeft }`, `{ .a + 1 }`, `{ .a ^ 1 }`, `{ (1) }` and
    /// `{ !.a + 1 }` are 400 `span filter field expressions must resolve
    /// to a boolean: …`; `{ .a }`, `{ -.a }`, `{ !.a }`, `{ true }`,
    /// `{ (.a) }`, `{ .a - .b }` and `{ !(.a = 1) }` are 200s.
    #[error("span filter field expressions must resolve to a boolean: {expr}")]
    SpansetFilterNotBoolean { expr: String },
    /// A unary operator must be legal for its operand's type: `!` takes a
    /// boolean, `-` takes a number; an attribute takes either.
    ///
    /// The reference words the UNARY failure in the singular — "the given
    /// type" — against the plural of its binary sibling, which is why this
    /// is its own variant rather than a reuse of
    /// [`ValidateError::IllegalOperator`].
    ///
    /// Measured: `{ !1 }` → `illegal operation for the given type: !1`,
    /// and likewise `{ !name }`, `{ !"x" }`, `{ -name }`, `{ -true }`.
    /// `{ !.a }`, `{ -.a }` and `{ !true }` are 200s.
    #[error("illegal operation for the given type: {expr}")]
    IllegalUnaryOperator { expr: String },
    /// An aggregate argument must resolve to a NUMBER type; an
    /// attribute is accepted, its type being unknown until the span is
    /// read (issue #335 Stage C, rule 11).
    ///
    /// Before Stage C this was unreachable: the parser took a bare
    /// `Field` from a hand-maintained aggregatable-intrinsic allowlist,
    /// so no non-numeric argument could be built. The reference parses
    /// an ordinary field expression there and rejects the surplus in its
    /// validator — a POSITIONLESS error naming the whole aggregate,
    /// which is the semantic signature.
    ///
    /// Measured against the pinned digest (`{} | avg(…) > 1` unless a
    /// duration threshold is required), 400 `aggregate field expressions
    /// must resolve to a number type`: `avg("x")`, `avg(name)`,
    /// `avg(status)`, `avg(kind)`, `avg(statusMessage)`, `avg(span:id)`,
    /// `avg(span:parentID)`, `avg(trace:id)`, `avg(rootName)`,
    /// `avg(rootServiceName)`, `avg(event:name)`, `avg(link:spanID)`,
    /// `avg(link:traceID)`, `avg(true)`, `avg(.a = 1)`,
    /// `avg(.a && .b)`, `avg(.a =~ "x")`, `avg(.a = nil)`,
    /// `avg(.a != nil)`, `avg(status = ok)`, `min(status)`, `max(name)`,
    /// `min("x")`. 200s: `avg(.a)`, `avg((.a))`, `avg(!.a)`, `avg(-.a)`,
    /// `avg(.a + 1)`, `avg(.a * 2 + 1)`, `avg(duration)`,
    /// `avg(span:duration)`, `avg(span:childCount)`,
    /// `avg(trace:duration)`, `avg(traceDuration)`,
    /// `avg(event:timeSinceStart)`, `avg(nestedSetParent|Left|Right)`,
    /// `avg(instrumentation:name|version)` (the mirrored
    /// attribute-typing quirk, see [`field_type`]).
    #[error("aggregate field expressions must resolve to a number type: {expr}")]
    AggregateNotNumeric { expr: String },
    /// An aggregate argument must REFERENCE THE SPAN — a numeric
    /// expression built only from literals aggregates nothing (issue
    /// #335 Stage C, rule 11's second half).
    ///
    /// Checked AFTER the number-type half, and the order is observable:
    /// `avg("x")` reports the type, `avg(1)` the span reference.
    ///
    /// Measured, 400 `aggregate field expressions must reference the
    /// span`: `avg(1)`, `avg((1))`, `avg(-1)`, `avg(1s)`, `avg(-1s)`,
    /// `avg(1 + 2)`, `sum(1)`, `sum(-1)`. 200s: every argument
    /// containing a field, at any depth — `avg(.a + 1)`, `avg(1 + .a)`,
    /// `avg(-(-.a))`, `avg(nestedSetParent + nestedSetLeft)`.
    #[error("aggregate field expressions must reference the span: {expr}")]
    AggregateNotSpanReferencing { expr: String },
    /// `ast_validate.go:272-287` — `= nil` is illegal on any intrinsic
    /// and on scoped `resource.service.name`.
    #[error("{field} = nil is not valid: intrinsics cannot be nil")]
    IntrinsicNotNil { field: String },
    /// `ast_metrics.go:328-332` — quantile literals lie in `[0, 1]`.
    #[error("quantile must be between 0 and 1: {value}")]
    QuantileOutOfRange { value: String },
    /// `ast_metrics.go:320/:325/:338` (`maxGroupBys = 5`,
    /// `engine_metrics.go:639`).
    #[error("metrics group by {got} values exceeds the supported {max}")]
    TooManyGroupBys { got: usize, max: usize },
    /// `ast_metrics.go:396-401` — `topk`/`bottomk` limits are positive.
    #[error("limit must be greater than 0: {expr}")]
    NonPositiveLimit { expr: String },
    /// `ast_validate.go:43-48` — `compare()` combines with nothing.
    #[error("compare() cannot be combined with any other pipeline stage")]
    CompareWithSecondStage,
}

impl ValidateError {
    /// The rule id, one per variant — the exhaustive match is what makes
    /// a ninth rule uncompilable without a [`VALIDATE_RULES`] row and a
    /// reference citation (issue #328 D5′).
    pub fn rule_id(&self) -> &'static str {
        match self {
            ValidateError::TypeMismatch { .. } => "type-mismatch",
            ValidateError::IllegalOperator { .. } => "illegal-operator",
            ValidateError::InvalidRegex { .. } => "invalid-regex",
            ValidateError::InvalidRegexOperand { .. } => "invalid-regex-operand",
            ValidateError::SpansetFilterNotBoolean { .. } => "spanset-filter-not-boolean",
            ValidateError::IllegalUnaryOperator { .. } => "illegal-unary-operator",
            ValidateError::AggregateNotNumeric { .. } => "aggregate-not-numeric",
            ValidateError::AggregateNotSpanReferencing { .. } => "aggregate-not-span-referencing",
            ValidateError::IntrinsicNotNil { .. } => "intrinsic-not-nil",
            ValidateError::QuantileOutOfRange { .. } => "quantile-out-of-range",
            ValidateError::TooManyGroupBys { .. } => "too-many-group-bys",
            ValidateError::NonPositiveLimit { .. } => "non-positive-limit",
            ValidateError::CompareWithSecondStage => "compare-second-stage",
        }
    }
}

/// One row per implemented rule: the [`ValidateError::rule_id`] and its
/// reference citation (grafana/tempo v3.0.2). Driven by the exhaustive
/// `rule_id` match, and pinned both ways by the test module — a new
/// variant cannot compile without a row here.
pub const VALIDATE_RULES: &[(&str, &str)] = &[
    ("type-mismatch", "ast_validate.go:198-219"),
    (
        "illegal-operator",
        "ast_validate.go:198-219 via enum_operators.go:77-121",
    ),
    ("invalid-regex", "ast_validate.go:221-245"),
    (
        "invalid-regex-operand",
        "ast_validate.go:221-245 (the same rule's operand half)",
    ),
    // Cited by SYMBOL, not by line: these two rules were established from
    // the pinned container's responses (see each variant's doc for the
    // measured queries), and inventing line numbers to match the rows
    // above would dress a measurement up as a source reading.
    (
        "spanset-filter-not-boolean",
        "ast_validate.go SpansetFilter.validate (impliedType must be boolean or attribute)",
    ),
    (
        "illegal-unary-operator",
        "ast_validate.go UnaryOperation.validate via enum_operators.go binaryTypeValid/unary set",
    ),
    // Rule 11's two halves — one reference check, two messages, the same
    // symbol-level citation posture as the two rows above (the verdicts
    // and the ORDER between the halves are container-measured; see each
    // variant's doc for the queries).
    (
        "aggregate-not-numeric",
        "ast_validate.go Aggregate.validate (the impliedType must be numeric or attribute half)",
    ),
    (
        "aggregate-not-span-referencing",
        "ast_validate.go Aggregate.validate via ast.go referencesSpan (the span-reference half)",
    ),
    ("intrinsic-not-nil", "ast_validate.go:272-287"),
    ("quantile-out-of-range", "ast_metrics.go:328-332"),
    (
        "too-many-group-bys",
        "ast_metrics.go:320,325,338 (maxGroupBys = 5, engine_metrics.go:639)",
    ),
    ("non-positive-limit", "ast_metrics.go:396-401"),
    ("compare-second-stage", "ast_validate.go:43-48"),
];

/// The reference's semantic validation over a parsed [`Query`] — pure,
/// route-independent, run after parse and before any planning. `Ok(())`
/// means the reference's validator would accept the expression; an error
/// maps to its `invalid TraceQL query: …` 400.
pub fn validate(query: &Query) -> Result<(), ValidateError> {
    validate_spanset(&query.spanset)?;
    // Rule 8: `compare()` combines with nothing — measured: the
    // reference's own Validate rejects `compare() | topk(n)`, and its
    // GRAMMAR rejects a `compare()` beside any other stage
    // (`… | rate() | compare(…)` / `… | compare(…) | rate()` are parse
    // errors there and parse here), so one structural rule carries the
    // whole measured rejection surface.
    let has_compare = query
        .pipeline
        .iter()
        .any(|s| matches!(s, PipelineStage::Compare { .. }));
    if has_compare && query.pipeline.len() > 1 {
        return Err(ValidateError::CompareWithSecondStage);
    }
    for stage in &query.pipeline {
        validate_stage(stage)?;
    }
    Ok(())
}

fn validate_spanset(expr: &SpansetExpr) -> Result<(), ValidateError> {
    match expr {
        SpansetExpr::Filter(filter) => validate_filter(filter),
        SpansetExpr::Binary { lhs, rhs, .. } | SpansetExpr::Structural { lhs, rhs, .. } => {
            validate_spanset(lhs)?;
            validate_spanset(rhs)
        }
    }
}

fn validate_filter(filter: &SpansetFilter) -> Result<(), ValidateError> {
    let Some(body) = &filter.body else {
        return Ok(());
    };
    validate_field_expr(body)?;
    // The body must RESOLVE to a boolean. Order matters and is the
    // reference's: `{ !1 }` reports the illegal unary operator (from the
    // recursive walk above), not the non-boolean body, even though both
    // hold.
    match expr_type(body) {
        // Unknown until the span is read — `{ .a }` is truthiness.
        OperandType::Attribute | OperandType::Boolean => Ok(()),
        OperandType::String | OperandType::Numeric | OperandType::Status | OperandType::Kind => {
            Err(ValidateError::SpansetFilterNotBoolean {
                expr: format!("{{ {body} }}"),
            })
        }
    }
}

/// **Rejections that moved from the parser to here (issue #335 Stage B).**
///
/// The collapse deleted the layered parser, and with it every guard that
/// lived in a layer rather than in a rule. Each one below was a parse
/// error before Stage B and is a validate error after it. All were
/// measured against the pinned reference and all match its POSITIONLESS
/// semantic signature, which is what says the rejection belongs here.
///
/// | moved | before | after | reference |
/// |---|---|---|---|
/// | operand type mismatch (`{ status = "ok" }`, `{ name = 5 }`) | field-typed `parse_value` refused the literal | `check_comparison` | 400 `binary operations must operate on the same type` |
/// | regex operand (`{ .a =~ .b }`) | parser refused a field RHS for `=~`/`!~` | [`ValidateError::InvalidRegexOperand`] | 400 `invalid type for =~ or !~: .b` |
/// | non-boolean filter body (`{ name }`, `{ 1 }`, `{ .a + 1 }`) | no layer could build a non-boolean body | [`ValidateError::SpansetFilterNotBoolean`] | 400 `span filter field expressions must resolve to a boolean` |
/// | unary operand type (`{ !name }`, `{ -true }`, `{ !1 }`) | `!` was a spanset-filter form, `-` an operand-only form | [`ValidateError::IllegalUnaryOperator`] | 400 `illegal operation for the given type` (singular) |
/// | non-comparison operand types (`{ .a = 1 && "x" }`, `{ name * 2 }`) | logical operands were boolean-shaped layers; arithmetic operands a separate grammar | rules 1-2, now over every binary class | 400 `binary operations must operate on the same type` / `illegal operation for the given types` |
/// | aggregate argument (`{} \| avg(name) > 1`, `{} \| avg(1) > 1`) — issue #335 Stage C | `parse_aggregate` took a bare field off an aggregatable-intrinsic allowlist | [`ValidateError::AggregateNotNumeric`] / [`ValidateError::AggregateNotSpanReferencing`] (rule 11) | 400 `aggregate field expressions must resolve to a number type` / `… must reference the span` |
///
/// One further rejection appeared in Stage B that is NOT in this class,
/// and the difference matters at the re-pin: a non-boolean `!` operand
/// (`{ !.a }` where `.a` is a string) now fails the whole query, but it
/// is a per-span EVALUATION error in `pulsus-read`, not a validate rule —
/// the query parses and validates, and fails when a span disagrees with
/// it. It cannot move the parse axis, so it belongs in the ledger as a
/// deliberate behaviour change rather than on this list.
///
/// **Why this is a list and not a note.** On the WIRE axis these are
/// verdict-neutral: parse ∘ validate composes, and a rejection stays a
/// rejection wherever it happens. On the PARSE axis every one of them is
/// `reject → accept`. So at the accept-surface re-pin, each parse-axis
/// move must be matched against this list *in advance* — an unexplained
/// move stops the work, and "it is probably one of these" is not an
/// explanation. Add a row here the moment a guard moves, not afterwards.
fn validate_field_expr(expr: &FieldExpr) -> Result<(), ValidateError> {
    match expr {
        FieldExpr::Field(_) | FieldExpr::Literal(_) => Ok(()),
        // `!= nil` (presence) and `= nil` (absence). Rule 4 keys off the
        // NEGATED form, which is now the node's own flag rather than a
        // `Not(Exists(..))` shape — so the `{ !(name != nil) }`
        // over-rejection the old shape forced is gone by construction
        // (ledger `traceql-validate-nil-spelling-conflation` retires).
        FieldExpr::Exists { field, negated } => {
            if !*negated {
                return Ok(());
            }
            match field {
                Field::Intrinsic(_) => Err(ValidateError::IntrinsicNotNil {
                    field: field.to_string(),
                }),
                Field::Attribute { scope, key } => {
                    // Exhaustive over `AttrScope` ON PURPOSE (issue #328
                    // D4 tier 1): the reference's nil rule also names
                    // `parent.`-scoped fields, which our AST cannot
                    // represent — adding a variant stops this compiling
                    // and forces the decision here.
                    match scope {
                        AttrScope::Resource if key == "service.name" => {
                            Err(ValidateError::IntrinsicNotNil {
                                field: field.to_string(),
                            })
                        }
                        AttrScope::Span
                        | AttrScope::Resource
                        | AttrScope::Unscoped
                        | AttrScope::Instrumentation
                        | AttrScope::Event
                        | AttrScope::Link => Ok(()),
                    }
                }
            }
        }
        FieldExpr::Unary { op, expr: inner } => {
            validate_field_expr(inner)?;
            check_unary(*op, expr_type(inner), &expr.to_string())
        }
        FieldExpr::Binary { op, lhs, rhs } => {
            validate_field_expr(lhs)?;
            validate_field_expr(rhs)?;
            let rendered = format!("{lhs} {op} {rhs}");
            // Rule 1 (matching operand types) applies to EVERY binary
            // class, not just comparison: measured, `{ .a = 1 && "x" }` is
            // 400 `binary operations must operate on the same type` and
            // `{ name * 2 }` likewise.
            if !types_match(expr_type(lhs), expr_type(rhs)) {
                return Err(ValidateError::TypeMismatch { expr: rendered });
            }
            // Rule 2 (operator legality) is per class. Destructuring
            // rather than asking the operator what class it is: the
            // comparison arm hands us a `ComparisonOp` directly, so no
            // other operator can reach `op_valid` even by mistake.
            let cmp = match op {
                // `&&`/`||` take booleans; `+ - * / % ^` take numbers; an
                // attribute takes either. Measured: `{ !.a && 1 }` and
                // `{ .a - true }` are 400 `illegal operation for the given
                // types`, while `{ .a = 1s + 1 }` and `{ true && true }`
                // are 200s.
                FieldOp::Bool(_) => {
                    return check_class_operands(
                        expr_type(lhs),
                        expr_type(rhs),
                        OperandType::Boolean,
                        rendered,
                    );
                }
                FieldOp::Arith(_) => {
                    return check_class_operands(
                        expr_type(lhs),
                        expr_type(rhs),
                        OperandType::Numeric,
                        rendered,
                    );
                }
                FieldOp::Cmp(cmp) => *cmp,
            };
            check_comparison(expr_type(lhs), cmp, expr_type(rhs), &rendered)?;
            if matches!(cmp, ComparisonOp::Re | ComparisonOp::Nre) {
                // The RHS of a regex operator must be a string literal.
                let FieldExpr::Literal(Value::String(pattern)) = rhs.as_ref() else {
                    return Err(ValidateError::InvalidRegexOperand {
                        operand: rhs.to_string(),
                    });
                };
                // Rule 3: the pattern itself.
                check_regex(pattern)?;
            }
            Ok(())
        }
    }
}

fn validate_stage(stage: &PipelineStage) -> Result<(), ValidateError> {
    match stage {
        PipelineStage::Aggregate {
            op,
            field,
            cmp,
            value,
        } => {
            // The aggregate's result is numeric, so the trailing
            // comparison follows the numeric operator set: measured,
            // `{} | count() =~ 2` is a reference 400 (its grammar) and
            // parses here. The exhaustive match is the `AggregateOp`
            // half of the D4 tier-2 guard.
            match op {
                AggregateOp::Count
                | AggregateOp::Sum
                | AggregateOp::Avg
                | AggregateOp::Min
                | AggregateOp::Max => {}
            }
            // Rule 11 (issue #335 Stage C): the ARGUMENT, in the
            // reference's own order — the ordinary field-expression
            // rules first (`avg(!1)` is `illegal operation for the given
            // type: !1`, not an aggregate error), then the number type,
            // then the span reference (`avg("x")` reports the type,
            // `avg(1)` the reference). All three orderings measured.
            if let Some(arg) = field {
                validate_field_expr(arg)?;
                let rendered = format!("{op}({arg})");
                match expr_type(arg) {
                    OperandType::Numeric | OperandType::Attribute => {}
                    OperandType::String
                    | OperandType::Boolean
                    | OperandType::Status
                    | OperandType::Kind => {
                        return Err(ValidateError::AggregateNotNumeric { expr: rendered });
                    }
                }
                if !references_span(arg) {
                    return Err(ValidateError::AggregateNotSpanReferencing { expr: rendered });
                }
            }
            check_comparison(
                OperandType::Numeric,
                *cmp,
                value_type(value),
                &format!("{stage}"),
            )
        }
        PipelineStage::Select { .. } | PipelineStage::By { .. } | PipelineStage::Coalesce => Ok(()),
        PipelineStage::Metric(metric) => validate_metric_stage(metric),
        PipelineStage::MetricSecondStage(second) => match second {
            // Rule 7 — the exhaustive match doubles as the
            // `SecondStage` guard (a new variant must decide here).
            SecondStage::TopK(n) | SecondStage::BottomK(n) => {
                if *n == 0 {
                    return Err(ValidateError::NonPositiveLimit {
                        expr: second.to_string(),
                    });
                }
                Ok(())
            }
        },
        PipelineStage::Compare { selection, .. } => validate_filter(selection),
    }
}

fn validate_metric_stage(metric: &MetricStage) -> Result<(), ValidateError> {
    // Rule 6 — the exhaustive match doubles as the `MetricFn` guard.
    let max_group_bys = match &metric.func {
        MetricFn::HistogramOverTime(_) | MetricFn::QuantileOverTime { .. } => MAX_GROUP_BYS - 1,
        MetricFn::Rate
        | MetricFn::CountOverTime
        | MetricFn::SumOverTime(_)
        | MetricFn::MinOverTime(_)
        | MetricFn::MaxOverTime(_)
        | MetricFn::AvgOverTime(_) => MAX_GROUP_BYS,
    };
    if metric.by.len() > max_group_bys {
        return Err(ValidateError::TooManyGroupBys {
            got: metric.by.len(),
            max: max_group_bys,
        });
    }
    // Rule 5.
    if let MetricFn::QuantileOverTime { quantiles, .. } = &metric.func {
        for q in quantiles {
            if let Value::Number(raw) = q {
                let parsed: f64 = raw.parse().unwrap_or(f64::NAN);
                if !(0.0..=1.0).contains(&parsed) {
                    return Err(ValidateError::QuantileOutOfRange { value: raw.clone() });
                }
            }
        }
    }
    Ok(())
}

/// The operand-type lattice (`ast.go` `impliedType`, collapsed:
/// Int/Float/Duration all behave identically under both
/// `isMatchingOperand` and `binaryTypeValid`, so they are one `Numeric`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperandType {
    /// An attribute's type is unknown until the data is read — it
    /// matches every type and every operator.
    Attribute,
    String,
    Numeric,
    Boolean,
    Status,
    Kind,
}

/// `isMatchingOperand`: attributes match anything; everything else
/// matches itself.
fn types_match(a: OperandType, b: OperandType) -> bool {
    a == OperandType::Attribute || b == OperandType::Attribute || a == b
}

/// The per-type legal operator sets (`enum_operators.go:77-121`),
/// measured: `{ name > "x" }` is a reference 200 (strings order), `{
/// status > ok }` / `{ kind < consumer }` / `{ .a > true }` /
/// `{ .a =~ 1 }` are reference 400s.
fn op_valid(op: ComparisonOp, t: OperandType) -> bool {
    match t {
        OperandType::Attribute | OperandType::String => true,
        OperandType::Numeric => !matches!(op, ComparisonOp::Re | ComparisonOp::Nre),
        OperandType::Boolean | OperandType::Status | OperandType::Kind => {
            matches!(op, ComparisonOp::Eq | ComparisonOp::Neq)
        }
    }
}

/// Rule 2 for the non-comparison classes, whose legal operand set is a
/// single type (plus `Attribute`, which is every type): `&&`/`||` want
/// `Boolean`, the arithmetic operators want `Numeric`.
///
/// Reached only after [`types_match`], so the two operands are already
/// either equal or attribute-wild; both are still checked because an
/// attribute pairs with anything and must not launder its partner.
fn check_class_operands(
    lhs: OperandType,
    rhs: OperandType,
    want: OperandType,
    expr: String,
) -> Result<(), ValidateError> {
    let ok = |t: OperandType| t == OperandType::Attribute || t == want;
    if ok(lhs) && ok(rhs) {
        Ok(())
    } else {
        Err(ValidateError::IllegalOperator { expr })
    }
}

/// Rule 2 for the unary operators. `!` wants a boolean, `-` a number, and
/// an attribute satisfies either — its type is unknown until the span is
/// read, so `{ !.a }` and `{ -.a }` are both reference 200s.
fn check_unary(op: UnaryOp, operand: OperandType, expr: &str) -> Result<(), ValidateError> {
    let want = match op {
        UnaryOp::Not => OperandType::Boolean,
        UnaryOp::Neg => OperandType::Numeric,
    };
    if operand == OperandType::Attribute || operand == want {
        Ok(())
    } else {
        Err(ValidateError::IllegalUnaryOperator {
            expr: expr.to_string(),
        })
    }
}

/// Rules 1 and 2, in the reference's order (measured: `{ status > kind }`
/// reports the type mismatch, not the illegal operator).
fn check_comparison(
    lhs: OperandType,
    op: ComparisonOp,
    rhs: OperandType,
    expr: &str,
) -> Result<(), ValidateError> {
    if !types_match(lhs, rhs) {
        return Err(ValidateError::TypeMismatch {
            expr: expr.to_string(),
        });
    }
    if !op_valid(op, lhs) || !op_valid(op, rhs) {
        return Err(ValidateError::IllegalOperator {
            expr: expr.to_string(),
        });
    }
    Ok(())
}

/// Rule 3's pattern half: reject exactly when [`re2_verdict`] proves an
/// RE2-family engine rejects. `Unknown` is ACCEPTED — the residual
/// where the reference still rejects spans the `Unknown` class families
/// enumerated from `pulsus-re2`'s own return sites (measured per class:
/// lookarounds, out-of-table `\p{…}` properties, `\u`/`\U` escapes,
/// a trailing backslash, non-portable `(?…` heads, repetition beyond
/// `kMaxRepeat` or applied to a repetition, over-budget compilations) —
/// ledgered under `traceql-validate-re2-unknown-residual`, owner #336.
fn check_regex(pattern: &str) -> Result<(), ValidateError> {
    match re2_verdict(pattern) {
        Re2Verdict::Accepts | Re2Verdict::Unknown => Ok(()),
        Re2Verdict::Rejects => Err(ValidateError::InvalidRegex {
            value: pattern.to_string(),
            reason: "not a valid RE2 regular expression".to_string(),
        }),
    }
}

/// [`Field`] typing. `instrumentation:name`/`instrumentation:version`
/// fall through to the ATTRIBUTE type DELIBERATELY (issue #328 ruling
/// 2, the #240-note posture: the reference's `impliedType` has no arm
/// for them, so its validator accepts any operator/operand there —
/// measured: `{ instrumentation:name = duration }` is a reference 200
/// where `{ event:name = duration }` is a 400. Do NOT "fix" this
/// toward consistency; mirroring the quirk is the point, and the
/// planner still rejects what cannot execute).
fn field_type(field: &Field) -> OperandType {
    match field {
        Field::Attribute { .. } => OperandType::Attribute,
        Field::Intrinsic(intrinsic) => match intrinsic {
            Intrinsic::Name
            | Intrinsic::StatusMessage
            | Intrinsic::SpanId
            | Intrinsic::ParentId
            | Intrinsic::TraceId
            | Intrinsic::RootName
            | Intrinsic::RootServiceName
            | Intrinsic::EventName
            | Intrinsic::LinkSpanId
            | Intrinsic::LinkTraceId => OperandType::String,
            Intrinsic::Duration
            | Intrinsic::TraceDuration
            | Intrinsic::EventTimeSinceStart
            | Intrinsic::NestedSetParent
            | Intrinsic::NestedSetLeft
            | Intrinsic::NestedSetRight
            | Intrinsic::ChildCount => OperandType::Numeric,
            Intrinsic::Status => OperandType::Status,
            Intrinsic::Kind => OperandType::Kind,
            Intrinsic::InstrumentationName | Intrinsic::InstrumentationVersion => {
                OperandType::Attribute
            }
        },
    }
}

fn value_type(value: &Value) -> OperandType {
    match value {
        Value::String(_) => OperandType::String,
        Value::Number(_) => OperandType::Numeric,
        Value::Duration(_) => OperandType::Numeric,
        Value::Bool(_) => OperandType::Boolean,
        Value::Status(_) => OperandType::Status,
        Value::Kind(_) => OperandType::Kind,
    }
}

/// Rule 11's second half (`ast.go referencesSpan`): does the expression
/// name anything read off a span?
///
/// A FIELD does, at every scope and for every intrinsic — including the
/// trace-scoped ones. That is not an inference from the phrase but the
/// measured behaviour: `avg(trace:duration)` and `avg(traceDuration)`
/// are reference 200s under an error message that says "must reference
/// the span". A LITERAL does not, however it is spelled or nested
/// (`avg(1)`, `avg((1))`, `avg(-1)`, `avg(1s)`, `avg(1 + 2)` are 400s).
///
/// `Exists` is unreachable here in practice — it is boolean-typed, so
/// the number-type half rejects it first — but it names a field, so it
/// answers `true` rather than pretending the question has no answer.
fn references_span(expr: &FieldExpr) -> bool {
    match expr {
        FieldExpr::Field(_) | FieldExpr::Exists { .. } => true,
        FieldExpr::Literal(_) => false,
        FieldExpr::Unary { expr, .. } => references_span(expr),
        FieldExpr::Binary { lhs, rhs, .. } => references_span(lhs) || references_span(rhs),
    }
}

/// Field-expression typing — the reference's `impliedType`: negation
/// keeps its operand's type; a binary node takes the first non-attribute
/// operand's type; a comparison or logical node is boolean.
fn expr_type(expr: &FieldExpr) -> OperandType {
    match expr {
        FieldExpr::Field(field) => field_type(field),
        FieldExpr::Literal(value) => value_type(value),
        FieldExpr::Exists { .. } => OperandType::Boolean,
        FieldExpr::Unary { expr, .. } => expr_type(expr),
        FieldExpr::Binary { op, lhs, rhs } => {
            if op.is_boolean_valued() {
                return OperandType::Boolean;
            }
            let l = expr_type(lhs);
            if l != OperandType::Attribute {
                l
            } else {
                expr_type(rhs)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse;

    fn v(q: &str) -> Result<(), ValidateError> {
        validate(&parse(q).unwrap_or_else(|e| panic!("{q:?} must parse: {e}")))
    }

    /// One reject-table row: the query and the variant predicate.
    type RejectCase = (&'static str, fn(&ValidateError) -> bool);

    /// The reject table: every container-measured rejection, mapped to
    /// its variant (the vectors file carries the same shapes with the
    /// captured statuses; this is the unit-level half).
    #[test]
    fn measured_rejections_map_to_their_variants() {
        use ValidateError as E;
        let cases: &[RejectCase] = &[
            ("{ name = duration }", |e| {
                matches!(e, E::TypeMismatch { .. })
            }),
            ("{ status = kind }", |e| matches!(e, E::TypeMismatch { .. })),
            ("{ span:childCount = name }", |e| {
                matches!(e, E::TypeMismatch { .. })
            }),
            ("{ duration + 1s > name }", |e| {
                matches!(e, E::TypeMismatch { .. })
            }),
            ("{ status > kind }", |e| matches!(e, E::TypeMismatch { .. })),
            ("{ status > ok }", |e| {
                matches!(e, E::IllegalOperator { .. })
            }),
            ("{ kind < consumer }", |e| {
                matches!(e, E::IllegalOperator { .. })
            }),
            ("{ .a > true }", |e| matches!(e, E::IllegalOperator { .. })),
            ("{ .a =~ 1 }", |e| matches!(e, E::IllegalOperator { .. })),
            ("{} | count() =~ 2", |e| {
                matches!(e, E::IllegalOperator { .. })
            }),
            (r#"{ span.a =~ "[" }"#, |e| {
                matches!(e, E::InvalidRegex { .. })
            }),
            (r#"{ .a =~ "x)(y" }"#, |e| {
                matches!(e, E::InvalidRegex { .. })
            }),
            (r#"{ .a !~ "[a--b]" }"#, |e| {
                matches!(e, E::InvalidRegex { .. })
            }),
            (r#"{ .a =~ "a{2,1}" }"#, |e| {
                matches!(e, E::InvalidRegex { .. })
            }),
            // Rule 11 (issue #335 Stage C), both halves and their order.
            (r#"{} | avg("x") > 1"#, |e| {
                matches!(e, E::AggregateNotNumeric { .. })
            }),
            ("{} | avg(name) > 1", |e| {
                matches!(e, E::AggregateNotNumeric { .. })
            }),
            ("{} | min(status) > 1", |e| {
                matches!(e, E::AggregateNotNumeric { .. })
            }),
            ("{} | avg(.a = 1) > 1", |e| {
                matches!(e, E::AggregateNotNumeric { .. })
            }),
            ("{} | avg(1) > 1", |e| {
                matches!(e, E::AggregateNotSpanReferencing { .. })
            }),
            ("{} | avg(1s) > 1s", |e| {
                matches!(e, E::AggregateNotSpanReferencing { .. })
            }),
            ("{} | sum(-1) > 1", |e| {
                matches!(e, E::AggregateNotSpanReferencing { .. })
            }),
            ("{} | avg(1 + 2) > 1", |e| {
                matches!(e, E::AggregateNotSpanReferencing { .. })
            }),
            // The inner field-expression rules run FIRST: measured
            // `illegal operation for the given type: !1`, not an
            // aggregate error.
            ("{} | avg(!1) > 1", |e| {
                matches!(e, E::IllegalUnaryOperator { .. })
            }),
            ("{} | avg(-name) > 1", |e| {
                matches!(e, E::IllegalUnaryOperator { .. })
            }),
            (r#"{} | avg(1 + "x") > 1"#, |e| {
                matches!(e, E::TypeMismatch { .. })
            }),
            ("{ name = nil }", |e| matches!(e, E::IntrinsicNotNil { .. })),
            ("{ duration = nil }", |e| {
                matches!(e, E::IntrinsicNotNil { .. })
            }),
            ("{ status = nil }", |e| {
                matches!(e, E::IntrinsicNotNil { .. })
            }),
            ("{ span:status = nil }", |e| {
                matches!(e, E::IntrinsicNotNil { .. })
            }),
            ("{ span:childCount = nil }", |e| {
                matches!(e, E::IntrinsicNotNil { .. })
            }),
            ("{ resource.service.name = nil }", |e| {
                matches!(e, E::IntrinsicNotNil { .. })
            }),
            ("{} | quantile_over_time(duration, 1.5)", |e| {
                matches!(e, E::QuantileOutOfRange { .. })
            }),
            ("{} | rate() by(.a1, .a2, .a3, .a4, .a5, .a6)", |e| {
                matches!(e, E::TooManyGroupBys { got: 6, max: 5 })
            }),
            (
                "{} | histogram_over_time(duration) by(.a1, .a2, .a3, .a4, .a5)",
                |e| matches!(e, E::TooManyGroupBys { got: 5, max: 4 }),
            ),
            (
                "{} | quantile_over_time(duration, .5) by(.a1, .a2, .a3, .a4, .a5)",
                |e| matches!(e, E::TooManyGroupBys { got: 5, max: 4 }),
            ),
            ("{} | rate() | topk(0)", |e| {
                matches!(e, E::NonPositiveLimit { .. })
            }),
            ("{} | rate() | bottomk(0)", |e| {
                matches!(e, E::NonPositiveLimit { .. })
            }),
            ("{} | compare({ .a = 1 }) | topk(3)", |e| {
                matches!(e, E::CompareWithSecondStage)
            }),
            ("{} | rate() | compare({ .a = 1 })", |e| {
                matches!(e, E::CompareWithSecondStage)
            }),
            ("{} | compare({ .a = 1 }) | rate()", |e| {
                matches!(e, E::CompareWithSecondStage)
            }),
            // Recursion: the same checks fire nested under spanset
            // composition, structural relations and compare()'s inner
            // filter.
            ("{ name = duration } && { .b = 1 }", |e| {
                matches!(e, E::TypeMismatch { .. })
            }),
            ("{} >> { status > ok }", |e| {
                matches!(e, E::IllegalOperator { .. })
            }),
            ("{} | compare({ status > ok })", |e| {
                matches!(e, E::IllegalOperator { .. })
            }),
        ];
        for (q, matches_variant) in cases {
            let err = v(q).expect_err(&format!("{q:?} must be rejected"));
            assert!(matches_variant(&err), "{q:?} rejected as {err:?}");
        }
    }

    /// The accept table (v1 AC 3 plus the container-measured additions):
    /// everything here is a reference 200 and must validate `Ok` — the
    /// planner may still reject some of them on some routes, which is
    /// exactly the strictness this pass must not inherit.
    #[test]
    fn measured_accepts_validate_ok() {
        for q in [
            "{} | rate()",
            r#"{ name > "x" }"#,
            "{ .a = nil }",
            "{ .service.name = nil }",
            "{ span.service.name = nil }",
            "{ .a != nil }",
            "{ name != nil }",
            "{ resource.service.name != nil }",
            "{ duration = 1 + 2 }",
            "{ name = .a }",
            "{ .a = 1 } !~ { .b = 2 }",
            "{ duration + 1s > 2s }",
            "{ statusMessage = name }",
            "{ duration = traceDuration }",
            "{ kind = internal }",
            "{ status = error }",
            "{ status != ok }",
            r#"{ statusMessage =~ "x" }"#,
            r#"{ trace:id = "abc" }"#,
            r#"{ .a =~ "che.*" }"#,
            r#"{ .a =~ "a{bbb}c" }"#,
            r#"{ .a !~ "[]a]" }"#,
            "{} | quantile_over_time(duration, .5)",
            "{} | quantile_over_time(duration, 0)",
            "{} | quantile_over_time(duration, 1)",
            "{} | rate() by(.a1, .a2, .a3, .a4, .a5)",
            "{} | histogram_over_time(duration) by(.a1, .a2, .a3, .a4)",
            "{} | rate() | topk(3)",
            "{} | rate() | topk(3) | bottomk(2)",
            "{} | compare({ .a = 1 })",
            "{} | sum_over_time(name)",
            "{} | sum_over_time(nestedSetLeft)",
            "{} | rate() by(trace:id)",
            "{} | rate() > 5",
            "{} | avg(duration) > 1s",
            "{} | count() > 2",
            "{} | count() > 2 | select(name)",
            "{ duration > 1s } | rate()",
            "{ true }",
            // Rule 11's accept side (issue #335 Stage C, D7): the four
            // probes that closed, plus the shapes that establish the
            // rule's boundary — every one a measured reference 200.
            "{} | avg(span:childCount) > 1",
            "{} | avg(trace:duration) > 1s",
            "{} | avg(.a + 1) > 1",
            "{} | avg((.a)) > 1",
            "{} | avg(!.a) > 1",
            "{} | avg(-.a) > 1",
            "{} | avg(1 + .a) > 1",
            "{} | max(.a * 2) > 1",
            "{} | avg(nestedSetParent + nestedSetLeft) > 1",
            "{} | avg(event:timeSinceStart) > 1",
            "{} | avg(instrumentation:name) > 1",
        ] {
            assert_eq!(v(q), Ok(()), "{q:?} must validate Ok");
        }
    }

    /// The mirrored `instrumentation:` quirk (ruling 2, #240 posture —
    /// deliberate, do not "fix" toward consistency): the reference's
    /// `impliedType` has no arm for `instrumentation:name`/`:version`,
    /// so they fall to the attribute type and accept any operand —
    /// measured `{ instrumentation:name = duration }` 200 where
    /// `{ event:name = duration }` is 400.
    #[test]
    fn the_instrumentation_intrinsics_mirror_the_reference_type_quirk() {
        assert_eq!(v("{ instrumentation:name = duration }"), Ok(()));
        assert_eq!(v("{ instrumentation:version > duration }"), Ok(()));
        assert!(matches!(
            v("{ event:name = duration }"),
            Err(ValidateError::TypeMismatch { .. })
        ));
    }

    /// The `Unknown` residual is ACCEPTED (never over-reject), one
    /// representative per class family enumerated from `pulsus-re2`'s
    /// return sites. The consequences are measured per class in the
    /// capture vectors: some members are reference 200s (agreement —
    /// Go accepts `\Q…\E`, octal, named groups, `\<`-as-literal,
    /// in-table `\p{L}`), the rest are reference 400s — the ledgered
    /// divergence (`traceql-validate-re2-unknown-residual`, owner #336).
    #[test]
    fn unknown_verdicts_are_accepted_never_over_rejected() {
        for q in [
            r#"{ .a =~ "\\p{Alphabetic}" }"#,
            r#"{ .a =~ "\\p{L}" }"#,
            r#"{ .a =~ "[\\p{Alphabetic}]" }"#,
            r#"{ .a =~ "a{1001}" }"#,
            r#"{ .a =~ "a**" }"#,
            r#"{ .a =~ "a*??" }"#,
            r#"{ .a =~ "(?P<n>x)" }"#,
            r#"{ .a =~ "(?=x)" }"#,
            r#"{ .a =~ "(?<!x)" }"#,
            r#"{ .a =~ "(?x)a b" }"#,
            r#"{ .a =~ "a\\" }"#,
            r#"{ .a =~ "\\u{263A}" }"#,
            r#"{ .a =~ "\\<word\\>" }"#,
            r#"{ .a =~ "\\b{start}x" }"#,
            r#"{ .a =~ "\\Qa*\\E" }"#,
            r#"{ .a =~ "\\12" }"#,
            r#"{ .a =~ "(?:(?:(?:(?:[0-9a-f]{32}){32}){32}){32})" }"#,
        ] {
            assert_eq!(v(q), Ok(()), "{q:?} carries an Unknown-verdict pattern");
        }
    }

    /// The nil-spelling conflation is RETIRED (issue #335 Stage B): this
    /// pins the DISTINCTION, where its predecessor pinned the conflation.
    ///
    /// `= nil` / `!= nil` fold to one `Exists { field, negated }` node
    /// carrying the polarity as a flag, so the canonical and the
    /// double-negation spellings are different ASTs and the validator can
    /// tell them apart. Ledger row
    /// `traceql-validate-nil-spelling-conflation` closed with this.
    ///
    /// Both halves are asserted. AST inequality alone would pass if the
    /// nodes differed and the validator still rejected both; the verdicts
    /// alone would pass if the parser reintroduced the conflation and some
    /// other rule happened to accept.
    #[test]
    fn the_nil_spellings_are_no_longer_conflated() {
        let canonical = parse("{ name = nil }").expect("parses");
        let double_negation = parse("{ !(name != nil) }").expect("parses");
        assert_ne!(
            canonical, double_negation,
            "the spellings must be distinguishable, which is what retired the divergence"
        );
        assert!(matches!(
            v("{ name = nil }"),
            Err(ValidateError::IntrinsicNotNil { .. })
        ));
        // Measured 200s at the pinned digest, all previously over-rejected.
        for q in [
            "{ !(name != nil) }",
            "{ !(resource.service.name != nil) }",
            "{ name != nil }",
            "{ .a = nil }",
            "{ !(.a != nil) }",
        ] {
            assert!(
                v(q).is_ok(),
                "{q:?} is a reference 200 and must be accepted"
            );
        }
    }

    /// D5′: the rule table and the listed variants are the same set,
    /// both directions, and every id is unique.
    ///
    /// **Residual, stated because it bit once.** `instances` is a hand
    /// list, and a hand list cannot see a variant nobody added to it: the
    /// `invalid-regex-operand` rule shipped earlier in issue #335 Stage B
    /// with a `rule_id` arm and NO citation row, and this test stayed
    /// green because the variant was missing from both sides at once. The
    /// count assertion only catches a variant added to one side. Rust has
    /// no stable variant enumeration, so what stands behind this is the
    /// reverse direction below (every row must be produced by a listed
    /// instance) plus `tests/validate_corpus.rs`, which drives real
    /// queries through `validate` and maps the resulting `rule_id` to a
    /// vector — a rule with no citation row fails THERE the moment any
    /// query reaches it.
    #[test]
    fn every_rule_variant_has_exactly_one_citation_row() {
        let instances = [
            ValidateError::TypeMismatch {
                expr: String::new(),
            },
            ValidateError::IllegalOperator {
                expr: String::new(),
            },
            ValidateError::InvalidRegex {
                value: String::new(),
                reason: String::new(),
            },
            ValidateError::InvalidRegexOperand {
                operand: String::new(),
            },
            ValidateError::SpansetFilterNotBoolean {
                expr: String::new(),
            },
            ValidateError::IllegalUnaryOperator {
                expr: String::new(),
            },
            ValidateError::AggregateNotNumeric {
                expr: String::new(),
            },
            ValidateError::AggregateNotSpanReferencing {
                expr: String::new(),
            },
            ValidateError::IntrinsicNotNil {
                field: String::new(),
            },
            ValidateError::QuantileOutOfRange {
                value: String::new(),
            },
            ValidateError::TooManyGroupBys { got: 0, max: 0 },
            ValidateError::NonPositiveLimit {
                expr: String::new(),
            },
            ValidateError::CompareWithSecondStage,
        ];
        assert_eq!(instances.len(), VALIDATE_RULES.len());
        for e in &instances {
            assert_eq!(
                VALIDATE_RULES
                    .iter()
                    .filter(|(id, _)| *id == e.rule_id())
                    .count(),
                1,
                "{} must have exactly one citation row",
                e.rule_id()
            );
        }
        // The reverse direction: no citation row may name an id that no
        // listed variant produces.
        for (id, _) in VALIDATE_RULES {
            assert!(
                instances.iter().any(|e| e.rule_id() == *id),
                "citation row {id} names no variant"
            );
        }
    }

    /// D6: the `invalid regex` literal in the Display is load-bearing
    /// (the search-route test keeps asserting on it).
    #[test]
    fn the_invalid_regex_message_carries_the_literal() {
        let err = v(r#"{ .a =~ "[" }"#).expect_err("rejected");
        assert!(err.to_string().contains("invalid regex"), "{err}");
    }

    // -----------------------------------------------------------------
    // Parse guards (issue #328 D4): the reference checks NOT implemented
    // here because our parser is stricter. Each guard reddens the moment
    // the parser loosens, so the missing semantic check cannot go
    // silently missing.
    // -----------------------------------------------------------------

    /// The `AggregateOp` × `Intrinsic` product, exhaustively (both
    /// matches break the build on a new variant).
    ///
    /// **Was a D4 tier-2 PARSE guard; is now rule 11's product table**
    /// (issue #335 Stage C). The guard asserted that every non-`duration`
    /// intrinsic aggregate target stays a parse error, standing in for
    /// the reference check we had not ported. Rule 11 ports it, so the
    /// guard has done its job and is replaced — by the same exhaustive
    /// product, now asserting the reference's own disposition per cell
    /// rather than a blanket parse rejection.
    ///
    /// Every cell is container-measured (`{} | <op>(<intrinsic>) > 1`,
    /// the shadow `query=` route, pinned digest): the numeric intrinsics
    /// and the two `instrumentation:` ones are 200s, the string/status/
    /// kind ones are 400 `must resolve to a number type`, and
    /// `count(<anything>)` is a parse error on both sides.
    #[test]
    fn aggregate_over_every_intrinsic_matches_the_reference_type_rule() {
        let agg_token = |op: AggregateOp| match op {
            AggregateOp::Count => "count",
            AggregateOp::Sum => "sum",
            AggregateOp::Avg => "avg",
            AggregateOp::Min => "min",
            AggregateOp::Max => "max",
        };
        let intrinsic_token = |i: Intrinsic| match i {
            Intrinsic::Name => "name",
            Intrinsic::Duration => "duration",
            Intrinsic::Status => "status",
            Intrinsic::Kind => "kind",
            Intrinsic::NestedSetParent => "nestedSetParent",
            Intrinsic::NestedSetLeft => "nestedSetLeft",
            Intrinsic::NestedSetRight => "nestedSetRight",
            Intrinsic::StatusMessage => "statusMessage",
            Intrinsic::ChildCount => "span:childCount",
            Intrinsic::SpanId => "span:id",
            Intrinsic::ParentId => "span:parentID",
            Intrinsic::TraceId => "trace:id",
            Intrinsic::TraceDuration => "traceDuration",
            Intrinsic::RootName => "rootName",
            Intrinsic::RootServiceName => "rootServiceName",
            Intrinsic::InstrumentationName => "instrumentation:name",
            Intrinsic::InstrumentationVersion => "instrumentation:version",
            Intrinsic::EventName => "event:name",
            Intrinsic::EventTimeSinceStart => "event:timeSinceStart",
            Intrinsic::LinkSpanId => "link:spanID",
            Intrinsic::LinkTraceId => "link:traceID",
        };
        let aggs = [
            AggregateOp::Count,
            AggregateOp::Sum,
            AggregateOp::Avg,
            AggregateOp::Min,
            AggregateOp::Max,
        ];
        let intrinsics = [
            Intrinsic::Name,
            Intrinsic::Duration,
            Intrinsic::Status,
            Intrinsic::Kind,
            Intrinsic::NestedSetParent,
            Intrinsic::NestedSetLeft,
            Intrinsic::NestedSetRight,
            Intrinsic::StatusMessage,
            Intrinsic::ChildCount,
            Intrinsic::SpanId,
            Intrinsic::ParentId,
            Intrinsic::TraceId,
            Intrinsic::TraceDuration,
            Intrinsic::RootName,
            Intrinsic::RootServiceName,
            Intrinsic::InstrumentationName,
            Intrinsic::InstrumentationVersion,
            Intrinsic::EventName,
            Intrinsic::EventTimeSinceStart,
            Intrinsic::LinkSpanId,
            Intrinsic::LinkTraceId,
        ];
        for op in aggs {
            for i in intrinsics {
                let q = format!("{{}} | {}({}) = 1", agg_token(op), intrinsic_token(i));
                // `count()` is strictly zero-arity in BOTH grammars
                // (measured: `count(1)` is a reference parse error), so
                // the whole `count` row stays a parse error here.
                if op == AggregateOp::Count {
                    assert!(parse(&q).is_err(), "{q:?} must stay a parse error");
                    continue;
                }
                // Rule 11's type half, per the measured product: an
                // argument whose implied type is numeric or attribute
                // validates; every other intrinsic is
                // `aggregate-not-numeric`. The exhaustive match is what
                // forces a new `Intrinsic` variant to be classified.
                let accepted = match field_type(&Field::Intrinsic(i)) {
                    OperandType::Numeric | OperandType::Attribute => true,
                    OperandType::String
                    | OperandType::Boolean
                    | OperandType::Status
                    | OperandType::Kind => false,
                };
                let ast = parse(&q).unwrap_or_else(|e| {
                    panic!("{q:?} must PARSE after Stage C's widening, got {e}")
                });
                match validate(&ast) {
                    Ok(()) => assert!(accepted, "{q:?} validates but is a reference 400"),
                    Err(err) => {
                        assert!(
                            !accepted,
                            "{q:?} is a reference 200 but was rejected: {err}"
                        );
                        assert_eq!(
                            err.rule_id(),
                            "aggregate-not-numeric",
                            "{q:?} rejected under the wrong rule ({err})"
                        );
                    }
                }
            }
        }
    }

    /// D4 tier 2 — `StructuralOp` and `ComparisonOp` in the positions
    /// the reference validates and our grammar rejects (structural ops
    /// in field position, `ast_validate.go:139-149`; comparison/regex
    /// tokens between spansets). The exhaustive matches decide, variant
    /// by variant, which members are grammar-valid and excluded.
    #[test]
    fn structural_and_comparison_tokens_stay_rejected_outside_their_positions() {
        use crate::ast::StructuralOp;
        let structural = [
            StructuralOp::Child,
            StructuralOp::Descendant,
            StructuralOp::Parent,
            StructuralOp::Ancestor,
            StructuralOp::Sibling,
        ];
        for op in structural {
            // In FIELD position: `>`/`<` are comparison operators there
            // (grammar-valid), the rest must stay parse errors.
            let field_pos_valid = match op {
                StructuralOp::Child | StructuralOp::Parent => true,
                StructuralOp::Descendant | StructuralOp::Ancestor | StructuralOp::Sibling => false,
            };
            if !field_pos_valid {
                let q = format!("{{ .a {op} .b }}");
                assert!(parse(&q).is_err(), "{q:?} must stay a parse error");
            }
        }
        let comparisons = [
            ComparisonOp::Eq,
            ComparisonOp::Neq,
            ComparisonOp::Gt,
            ComparisonOp::Gte,
            ComparisonOp::Lt,
            ComparisonOp::Lte,
            ComparisonOp::Re,
            ComparisonOp::Nre,
        ];
        for op in comparisons {
            // Between SPANSETS: `>`/`<` are structural, `!~` is the
            // negated sibling — grammar-valid; the rest must stay
            // rejected (Gte/Lte as the frozen NotYetSupported boundary).
            let spanset_pos_valid = match op {
                ComparisonOp::Gt | ComparisonOp::Lt | ComparisonOp::Nre => true,
                ComparisonOp::Eq
                | ComparisonOp::Neq
                | ComparisonOp::Gte
                | ComparisonOp::Lte
                | ComparisonOp::Re => false,
            };
            if !spanset_pos_valid {
                let q = format!("{{}} {op} {{}}");
                assert!(parse(&q).is_err(), "{q:?} must stay a parse error");
            }
        }
    }

    /// D4 tier 3 — SAMPLED families, and said so: these shapes are
    /// unparseable for grammar reasons the sample does not isolate, so a
    /// future grammar loosening INSIDE one of these families could
    /// become parseable without reddening anything here. What each
    /// sample stands for is named; the honest limit is that it is a
    /// sample.
    #[test]
    fn sampled_parse_guards_for_the_remaining_reference_checks() {
        // (a) STILL unreachable: the parser rejects these, so the
        // reference's corresponding check has nothing to run on here. A
        // loosening of the grammar turns one of these into an accept and
        // this test goes red — which is the whole point of the list.
        for (q, family) in [
            // Scalar expressions and filters (ast_validate.go:151-162).
            (r#"{} | avg(duration) = "x""#, "non-numeric scalar operand"),
            (r#"{} | count() =~ "x""#, "regex scalar filter on a string"),
            // compare() extra arguments (engine_metrics_compare.go:
            // 319-341: topN/start/end). NOTE the reference ACCEPTS
            // `compare({…}, 10)` — we are stricter here, which is a
            // pre-existing divergence, not a check we mirror.
            ("{} | compare({ .a = 1 }, 10)", "compare() topN argument"),
            // parent. scope (ast_validate.go:250-267) — also tier 1: the
            // AttrScope match in validate_field_expr has no variant for
            // it.
            ("{ parent.a = 1 }", "parent scope"),
        ] {
            assert!(parse(q).is_err(), "{family}: {q:?} must stay a parse error");
        }

        // (b) MOVED to this pass by the issue #335 Stage B collapse. Each
        // was a parse error before and is a validate rule now; the
        // rejection is unchanged on the wire, so the entry moves rather
        // than disappearing. Asserting the RULE ID, not merely `is_err`:
        // "some error" would still pass if the collapse left a stray
        // parse error behind and the rule were never wired up.
        for (q, rule) in [
            ("{ name }", "spanset-filter-not-boolean"),
            (r#"{ name =~ .a }"#, "invalid-regex-operand"),
            (r#"{ .a + 1 =~ "x" }"#, "type-mismatch"),
        ] {
            let err = v(q).unwrap_err();
            assert_eq!(err.rule_id(), rule, "{q:?} must be caught by {rule}");
        }

        // (c) NOT a check on either side. These sat in the guard list
        // because our parser rejected them, and the list read that as
        // "the reference's check is unreachable" — but the reference
        // ACCEPTS all three (measured 200s), so what the guard actually
        // pinned was a divergence of ours. The collapse closed it.
        for q in ["{ 1 = 1 }", "{ -.a > 1 }", "{ -duration > 1s }"] {
            assert!(
                v(q).is_ok(),
                "{q:?} is a reference 200 and must be accepted"
            );
        }
    }
}
