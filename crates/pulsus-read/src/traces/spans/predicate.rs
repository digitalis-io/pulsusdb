//! The span-scope predicate compiler, part 1 (issue #588) — STUB.
//!
//! The type and the three entry points the cases of issue #588 are
//! written against. Every body here is a placeholder: the predicate
//! compiler renders the constant `false` and the membership statement
//! omits the day bound, so each case fails on its own assertion rather
//! than on a missing item.

use pulsus_traceql::{ComparisonOp, Field, FieldExpr, Value};

use crate::traces::filter::PlanError;
use crate::traces::window_sql::WindowSql;

/// A ClickHouse boolean over a `spans` row. Private field, no public
/// constructor: [`compile_span_predicate`] is the only way to obtain one,
/// so no later task can splice un-escaped query text into a statement.
/// The posture [`WindowSql`] already has.
///
/// `Debug` is derived so a case that expected a refusal can print what it
/// got instead; it is not a second way to read the text.
#[derive(Debug, Clone)]
pub struct SpanPredicate(String);

impl SpanPredicate {
    pub fn sql(&self) -> &str {
        &self.0
    }
}

/// Part 1 accepts exactly the shapes of the plan's sections 4 and 6;
/// every other shape is [`PlanError::UnsupportedField`] naming the
/// construct and issue #589.
pub fn compile_span_predicate(expr: &FieldExpr) -> Result<SpanPredicate, PlanError> {
    let _ = expr;
    Ok(SpanPredicate("false".to_string()))
}

/// The leaf, for a caller that has already normalised to
/// `(field, op, value)` — `filter::compile_leaf`'s shape, so the two
/// compilers read against each other.
pub fn compile_span_leaf(
    field: &Field,
    op: ComparisonOp,
    value: &Value,
) -> Result<SpanPredicate, PlanError> {
    let _ = (field, op, value);
    Ok(SpanPredicate("false".to_string()))
}

/// The ONE place a window and a predicate compose. The live suite issues
/// it; the compile suite freezes its text, so the frozen text is what ran.
pub fn span_membership_sql(spans_table: &str, w: WindowSql, p: &SpanPredicate) -> String {
    format!(
        "SELECT lower(hex(span_id)) AS span_id\n\
         FROM {spans_table}\n\
         WHERE {time}\n\
         \x20 AND {bucket}\n\
         \x20 AND ({pred})\n\
         ORDER BY span_id",
        time = w.span_time_clause(),
        bucket = w.span_bucket_clause(),
        pred = p.sql(),
    )
}
