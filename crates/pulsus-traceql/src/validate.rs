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
//!    matches anything.
//! 2. The operator must be legal for BOTH operand types
//!    (`{ status > ok }` → [`ValidateError::IllegalOperator`]).
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
//!
//! Every OTHER check on the reference's list is unreachable here
//! because our parser is stricter — each has a parse-guard in the test
//! module that goes RED the moment the parser loosens, so the missing
//! check cannot go silently missing (tiers per issue #328 D4; the
//! sampled families are named as sampled).

use crate::ast::{
    AggregateOp, AttrScope, ComparisonOp, Field, FieldExpr, Intrinsic, MetricFn, MetricStage,
    Operand, PipelineStage, Query, SecondStage, SpansetExpr, SpansetFilter, Value,
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
    match &filter.body {
        None => Ok(()),
        Some(body) => validate_field_expr(body),
    }
}

fn validate_field_expr(expr: &FieldExpr) -> Result<(), ValidateError> {
    match expr {
        FieldExpr::Comparison { field, op, value } => {
            check_comparison(
                field_type(field),
                *op,
                value_type(value),
                &format!("{field} {op} {value}"),
            )?;
            // Rule 3: the regex pattern itself. The RHS is a string
            // static by construction here (a non-string RHS already
            // failed the operator check: `=~` is illegal for every
            // non-string, non-attribute type, and an attribute RHS
            // cannot appear in `Comparison`).
            if let (ComparisonOp::Re | ComparisonOp::Nre, Value::String(pattern)) = (op, value) {
                check_regex(pattern)?;
            }
            Ok(())
        }
        FieldExpr::FieldCompare { lhs, op, rhs } => {
            // A field RHS never carries a regex (parse-guarded), so no
            // pattern check is reachable here.
            check_comparison(
                field_type(lhs),
                *op,
                field_type(rhs),
                &format!("{lhs} {op} {rhs}"),
            )
        }
        FieldExpr::ArithCompare { lhs, op, rhs } => {
            check_comparison(
                operand_type(lhs),
                *op,
                operand_type(rhs),
                &format!("{expr}"),
            )?;
            if let (ComparisonOp::Re | ComparisonOp::Nre, Operand::Literal(Value::String(p))) =
                (op, rhs)
            {
                check_regex(p)?;
            }
            Ok(())
        }
        FieldExpr::BoolStatic(_) => Ok(()),
        // `!= nil` (and the bare-attribute existence spelling) — accepted
        // everywhere, measured: `{ name != nil }` and
        // `{ resource.service.name != nil }` are both reference 200s.
        FieldExpr::Exists(_) => Ok(()),
        FieldExpr::Not(inner) => {
            // Rule 4. The parser normalizes `x = nil` to `Not(Exists(x))`,
            // so the nil rule keys off that shape. MEASURED CONFLATION,
            // ledgered (`traceql-validate-nil-spelling-conflation`): the
            // double-negation spelling `{ !(name != nil) }` produces the
            // same AST and is a reference 200 — rejecting here matches
            // the canonical `= nil` 400 and over-rejects only that
            // spelling; distinguishing them needs a parser/AST change
            // this issue does not make.
            if let FieldExpr::Exists(field) = inner.as_ref() {
                match field {
                    Field::Intrinsic(_) => {
                        return Err(ValidateError::IntrinsicNotNil {
                            field: field.to_string(),
                        });
                    }
                    Field::Attribute { scope, key } => {
                        // Exhaustive over `AttrScope` ON PURPOSE (issue
                        // #328 D4 tier 1): the reference's nil rule also
                        // names `parent.`-scoped fields, which our AST
                        // cannot represent — adding an `AttrScope`
                        // variant stops this compiling and forces the
                        // decision here.
                        match scope {
                            AttrScope::Resource if key == "service.name" => {
                                return Err(ValidateError::IntrinsicNotNil {
                                    field: field.to_string(),
                                });
                            }
                            AttrScope::Span
                            | AttrScope::Resource
                            | AttrScope::Unscoped
                            | AttrScope::Instrumentation
                            | AttrScope::Event
                            | AttrScope::Link => {}
                        }
                    }
                }
            }
            validate_field_expr(inner)
        }
        FieldExpr::Binary { lhs, rhs, .. } => {
            validate_field_expr(lhs)?;
            validate_field_expr(rhs)
        }
    }
}

fn validate_stage(stage: &PipelineStage) -> Result<(), ValidateError> {
    match stage {
        PipelineStage::Aggregate {
            op,
            field: _,
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

/// [`Operand`] typing — `Unary`/`BinaryOperation.impliedType`: negation
/// keeps its operand's type; arithmetic takes the first non-attribute
/// operand's type.
fn operand_type(operand: &Operand) -> OperandType {
    match operand {
        Operand::Field(field) => field_type(field),
        Operand::Literal(value) => value_type(value),
        Operand::Neg(inner) => operand_type(inner),
        Operand::Arith { lhs, rhs, .. } => {
            let l = operand_type(lhs);
            if l != OperandType::Attribute {
                l
            } else {
                operand_type(rhs)
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

    /// The nil-spelling conflation, pinned as the ledgered divergence it
    /// is (`traceql-validate-nil-spelling-conflation`): the parser
    /// normalizes `{ !(name != nil) }` onto the same AST as
    /// `{ name = nil }`, so the exotic double-negation spelling is
    /// over-rejected (reference 200) as the price of rejecting the
    /// canonical spelling (reference 400).
    #[test]
    fn the_nil_double_negation_spelling_is_conflated_and_rejected() {
        let canonical = parse("{ name = nil }").expect("parses");
        let exotic = parse("{ !(name != nil) }").expect("parses");
        assert_eq!(canonical, exotic, "the conflation this pin documents");
        assert!(matches!(
            v("{ !(name != nil) }"),
            Err(ValidateError::IntrinsicNotNil { .. })
        ));
    }

    /// D5′: the rule table and the variant set are the same set, both
    /// directions, and every id is unique.
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

    /// D4 tier 2 — the `AggregateOp` × `Intrinsic` product, exhaustively
    /// (both matches break the build on a new variant): the reference
    /// validates that aggregates reference the span
    /// (`ast_validate.go:63-70,86-109`); here every non-`duration`
    /// intrinsic aggregate target is a parse error, over the WHOLE
    /// product.
    #[test]
    fn aggregate_over_every_non_duration_intrinsic_stays_a_parse_error() {
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
                // The grammar's legal intrinsic aggregates: `duration`
                // and the numeric nested-set intrinsics under the
                // one-arity ops (`sum(nestedSetLeft) = 1` is measured a
                // reference 200 — span-referencing AND numeric, so no
                // guard applies); `count(anything)` is not.
                let numeric_target = matches!(
                    i,
                    Intrinsic::Duration
                        | Intrinsic::NestedSetParent
                        | Intrinsic::NestedSetLeft
                        | Intrinsic::NestedSetRight
                );
                if numeric_target && op != AggregateOp::Count {
                    continue;
                }
                let q = format!("{{}} | {}({}) = 1", agg_token(op), intrinsic_token(i));
                assert!(
                    parse(&q).is_err(),
                    "{q:?} now PARSES — the parser loosened; the reference's span-referencing \
                     aggregate validation (ast_validate.go:63-70,86-109) must be ported before \
                     this guard is relaxed"
                );
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
        for (q, family) in [
            // Spanset filters must resolve to a boolean
            // (ast_validate.go:111-137).
            ("{ name }", "non-boolean spanset filter"),
            ("{ 1 = 1 }", "literal-vs-literal comparison"),
            // Scalar expressions and filters (ast_validate.go:151-162).
            (r#"{} | avg(duration) = "x""#, "non-numeric scalar operand"),
            (r#"{} | count() =~ "x""#, "regex scalar filter on a string"),
            // Unary operators (ast_validate.go:164-196).
            ("{ -.a > 1 }", "unary minus in field position"),
            ("{ -duration > 1s }", "unary minus on an intrinsic"),
            // Regex with a non-static RHS (ast_validate.go:221-245).
            (r#"{ name =~ .a }"#, "regex against a field RHS"),
            (r#"{ .a + 1 =~ "x" }"#, "regex against an arithmetic LHS"),
            // compare() extra arguments (engine_metrics_compare.go:
            // 319-341: topN/start/end).
            ("{} | compare({ .a = 1 }, 10)", "compare() topN argument"),
            // parent. scope (ast_validate.go:250-267) — also tier 1: the
            // AttrScope match in validate_field_expr has no variant for
            // it.
            ("{ parent.a = 1 }", "parent scope"),
        ] {
            assert!(parse(q).is_err(), "{family}: {q:?} must stay a parse error");
        }
    }
}
