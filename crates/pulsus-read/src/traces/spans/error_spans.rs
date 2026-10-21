//! Which queries the error-span table answers (issue #601 part 4b).
//!
//! `error_spans` holds the error spans' non-attribute columns under the span
//! sort key, filled by a view from the landing table's span rows with
//! `status_code = 2`. A search or metrics statement whose query [`admits`]
//! reads it in place of `spans`: one walk, for both routes.

use pulsus_traceql::{
    AttrScope, BoolOp, ComparisonOp, Field, FieldExpr, FieldOp, Intrinsic, MetricFn, PipelineStage,
    Query, SpansetExpr, SpansetFilter, StatusValue, Value,
};

/// Whether `query` is answered from the error-span table: its spanset is
/// one filter with a body, that body's `&&` spine has a conjunct
/// `status = error`, every field it reads — in the body, `by()`,
/// `select()`, an aggregate's argument or the metrics target — is one the
/// table holds, and it is not `compare()`.
pub fn admits(query: &Query) -> bool {
    let SpansetExpr::Filter(SpansetFilter { body: Some(body) }) = &query.spanset else {
        return false;
    };
    has_error_conjunct(body) && reads_held_fields(body) && query.pipeline.iter().all(stage_admitted)
}

/// One pipeline stage reads only held fields. A further spanset filter and
/// `compare()` are not admitted.
fn stage_admitted(stage: &PipelineStage) -> bool {
    match stage {
        PipelineStage::Aggregate { field, .. } => field.as_ref().is_none_or(reads_held_fields),
        PipelineStage::Select { fields } => fields.iter().all(held),
        PipelineStage::By { key } => reads_held_fields(key),
        PipelineStage::Coalesce | PipelineStage::MetricSecondStage(_) => true,
        PipelineStage::Metric(metric) => {
            let target = match &metric.func {
                MetricFn::Rate | MetricFn::CountOverTime => None,
                MetricFn::SumOverTime(f)
                | MetricFn::MinOverTime(f)
                | MetricFn::MaxOverTime(f)
                | MetricFn::AvgOverTime(f)
                | MetricFn::HistogramOverTime(f)
                | MetricFn::QuantileOverTime { field: f, .. } => Some(f),
            };
            target.is_none_or(held) && metric.by.iter().all(held)
        }
        PipelineStage::Filter(_) | PipelineStage::Compare { .. } => false,
    }
}

/// The body's `&&` spine has a conjunct `status = error`.
fn has_error_conjunct(expr: &FieldExpr) -> bool {
    match expr {
        FieldExpr::Binary {
            op: FieldOp::Bool(BoolOp::And),
            lhs,
            rhs,
        } => has_error_conjunct(lhs) || has_error_conjunct(rhs),
        FieldExpr::Binary {
            op: FieldOp::Cmp(ComparisonOp::Eq),
            lhs,
            rhs,
        } => {
            matches!(**lhs, FieldExpr::Field(Field::Intrinsic(Intrinsic::Status)))
                && matches!(**rhs, FieldExpr::Literal(Value::Status(StatusValue::Error)))
        }
        _ => false,
    }
}

/// Every field `expr` reads is held.
fn reads_held_fields(expr: &FieldExpr) -> bool {
    match expr {
        FieldExpr::Field(field) | FieldExpr::Exists { field, .. } => held(field),
        FieldExpr::Literal(_) => true,
        FieldExpr::Unary { expr, .. } => reads_held_fields(expr),
        FieldExpr::Binary { lhs, rhs, .. } => reads_held_fields(lhs) && reads_held_fields(rhs),
    }
}

/// A field the error-span table, with `traces` beside it, answers: either
/// spelling of each intrinsic parses to one variant.
fn held(field: &Field) -> bool {
    match field {
        Field::Intrinsic(i) => matches!(
            i,
            Intrinsic::Status
                | Intrinsic::StatusMessage
                | Intrinsic::Kind
                | Intrinsic::Name
                | Intrinsic::Duration
                | Intrinsic::SpanId
                | Intrinsic::TraceId
                | Intrinsic::TraceDuration
                | Intrinsic::RootName
                | Intrinsic::RootServiceName
        ),
        Field::Attribute { scope, key } => *scope == AttrScope::Resource && key == "service.name",
    }
}
