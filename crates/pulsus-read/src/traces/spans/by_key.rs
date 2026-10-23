//! Issue #649: a `by()` key that is a field expression — its response
//! label, as the reference renders the stage, and its value.

use pulsus_traceql::{Field, FieldExpr, FieldOp, Intrinsic, UnaryOp, Value};

use super::fold::Key;
use super::predicate::{
    KeyStatic, PredicateCtx, compile_span_predicate_in, key_arith_in, key_static,
};
use super::projection::GroupKeySql;
use super::tracelevel::{KeyNames, KeyValues};
use crate::traces::PlanError;
use crate::traces::values::go_duration_string;

/// `by(<expr>)` as the reference's stage renders it: a literal-only
/// arithmetic subtree folded to its static; an operand that is neither a
/// static nor a field parenthesised; a string static in backticks.
pub fn label(expr: &FieldExpr) -> Result<String, PlanError> {
    Ok(format!("by({})", text(expr)?))
}

fn text(expr: &FieldExpr) -> Result<String, PlanError> {
    if let Some(s) = static_text(expr)? {
        return Ok(s);
    }
    Ok(match expr {
        FieldExpr::Field(f) => f.to_string(),
        FieldExpr::Literal(Value::String(s)) => format!("`{s}`"),
        FieldExpr::Literal(v) => v.to_string(),
        FieldExpr::Exists { field, negated } => {
            format!("{field} {} nil", if *negated { "=" } else { "!=" })
        }
        FieldExpr::Unary { op, expr: inner } => {
            let sym = match op {
                UnaryOp::Neg => "-",
                UnaryOp::Not => "!",
            };
            format!("{sym}{}", wrap(inner)?)
        }
        FieldExpr::Binary { op, lhs, rhs } => format!("{} {op} {}", wrap(lhs)?, wrap(rhs)?),
    })
}

fn wrap(expr: &FieldExpr) -> Result<String, PlanError> {
    if matches!(expr, FieldExpr::Field(_) | FieldExpr::Literal(_)) || static_text(expr)?.is_some() {
        text(expr)
    } else {
        Ok(format!("({})", text(expr)?))
    }
}

/// Whether `expr`'s value is a duration: a duration field or literal, or
/// `-` over one. Every binary node is an integer or a float.
fn duration_typed(expr: &FieldExpr) -> bool {
    match expr {
        FieldExpr::Field(Field::Intrinsic(
            Intrinsic::Duration | Intrinsic::TraceDuration | Intrinsic::EventTimeSinceStart,
        )) => true,
        FieldExpr::Literal(Value::Duration(_)) => true,
        FieldExpr::Unary {
            op: UnaryOp::Neg,
            expr,
        } => duration_typed(expr),
        _ => false,
    }
}

/// A folded arithmetic subtree's static text; `None` for anything else.
fn static_text(expr: &FieldExpr) -> Result<Option<String>, PlanError> {
    let arith = matches!(
        expr,
        FieldExpr::Literal(Value::Number(_) | Value::Duration(_))
            | FieldExpr::Unary {
                op: UnaryOp::Neg,
                ..
            }
            | FieldExpr::Binary {
                op: FieldOp::Arith(_),
                ..
            }
    );
    if !arith {
        return Ok(None);
    }
    Ok(key_static(expr)?.map(|s| match s {
        KeyStatic::Int(v) => v,
        KeyStatic::Dur(v) if duration_typed(expr) => {
            go_duration_string(i64::try_from(v).unwrap_or(i64::MAX))
        }
        KeyStatic::Dur(v) => go_float(v as f64),
        KeyStatic::Float(x) => go_float(x),
    }))
}

/// Go's `strconv.FormatFloat(x, 'g', -1, 64)`, with `.0` appended when it
/// holds neither `e` nor `.` (the reference's float static).
pub fn go_float(x: f64) -> String {
    let sci = format!("{x:e}");
    let (mantissa, exp) = sci.split_once('e').expect("{:e} writes an exponent");
    let exp: i32 = exp.parse().expect("an integer exponent");
    let s = if x != 0.0 && !(-4..6).contains(&exp) {
        let sign = if exp < 0 { '-' } else { '+' };
        format!("{mantissa}e{sign}{:02}", exp.abs())
    } else {
        format!("{x}")
    };
    if s.contains(['e', '.']) || s.contains("inf") || s.contains("NaN") {
        s
    } else {
        format!("{s}.0")
    }
}

/// The `by()` key of an expression that is not a bare field, its lookups
/// read by `names`; `top_sql` is `sql` until the caller sets it.
pub fn expression_key(
    expr: &FieldExpr,
    ctx: &PredicateCtx<'_>,
    names: KeyNames,
) -> Result<Key, PlanError> {
    let display = label(expr)?;
    let arithmetic = matches!(
        expr,
        FieldExpr::Binary {
            op: FieldOp::Arith(_),
            ..
        } | FieldExpr::Unary {
            op: UnaryOp::Neg,
            ..
        }
    );
    if !arithmetic {
        let p = compile_span_predicate_in(expr, ctx)?;
        let sql = GroupKeySql {
            value: format!("if({}, 'true', 'false')", p.sql()),
            value_type: Some("'Bool'".to_string()),
            values: Vec::new(),
        };
        return Ok(Key {
            display,
            top_sql: sql.clone(),
            sql,
            per_trace: false,
            needs: Some(p),
        });
    }
    let a = key_arith_in(expr, ctx, names).map_err(|e| match e {
        PlanError::UnsupportedField(m) if m.starts_with("by(): ") => {
            PlanError::UnsupportedField(format!("{display}: {}", &m["by(): ".len()..]))
        }
        other => other,
    })?;
    const IN_INT64: &str = "BETWEEN -9223372036854775808 AND 9223372036854775807";
    let i = "tupleElement(pk, 1)";
    let f = "tupleElement(pk, 2)";
    let (si, sf) = (
        "toString(assumeNotNull(tupleElement(pk, 1)))",
        "toString(assumeNotNull(tupleElement(pk, 2)))",
    );
    let pair = if duration_typed(expr) {
        format!("multiIf({i} {IN_INT64}, ({si}, 'Duration'), ('false', 'Bool'))")
    } else if a.dur {
        format!(
            "multiIf({i} IS NOT NULL, (toString(toFloat64(assumeNotNull({i}))), 'Float64'), \
             {f} IS NOT NULL, ({sf}, 'Float64'), ('false', 'Bool'))"
        )
    } else {
        format!(
            "multiIf({i} {IN_INT64}, ({si}, 'Int64'), {f} IS NOT NULL, ({sf}, 'Float64'), \
             ('false', 'Bool'))"
        )
    };
    let kv = match &a.tuple {
        Some(t) => format!("arrayElement(arrayMap(pk -> {pair}, [{t}]), 1)"),
        None => "('false', 'Bool')".to_string(),
    };
    let per_trace =
        matches!(a.values.first(), Some(KeyValues::Trace(_))) && only_trace_leaves(expr);
    let sql = GroupKeySql {
        value: format!("tupleElement({kv}, 1)"),
        value_type: Some(format!("tupleElement({kv}, 2)")),
        values: a.values,
    };
    Ok(Key {
        display,
        top_sql: sql.clone(),
        sql,
        per_trace,
        needs: None,
    })
}

/// Whether every field `expr` reads is one per trace.
fn only_trace_leaves(expr: &FieldExpr) -> bool {
    match expr {
        FieldExpr::Field(f) => matches!(f, Field::Intrinsic(Intrinsic::TraceDuration)),
        FieldExpr::Literal(_) => true,
        FieldExpr::Exists { .. } => false,
        FieldExpr::Unary { expr, .. } => only_trace_leaves(expr),
        FieldExpr::Binary { lhs, rhs, .. } => only_trace_leaves(lhs) && only_trace_leaves(rhs),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn label_of(q: &str) -> String {
        let query = pulsus_traceql::parse(q).expect("parses");
        let pulsus_traceql::PipelineStage::By { key } = &query.pipeline[0] else {
            panic!("{q}: not a by() stage");
        };
        label(key).expect("renders")
    }

    /// Every row was read from the pinned reference's response.
    #[test]
    fn the_label_is_the_references() {
        let mut wrong = Vec::new();
        for (q, want) in [
            ("{ } | by(span.dms / 1000)", "by(span.dms / 1000)"),
            ("{ } | by(span.dms + (1 + 2))", "by(span.dms + 3)"),
            ("{ } | by(span.dms + 2 * 3)", "by(span.dms + 6)"),
            ("{ } | by((span.dms + 2) * 3)", "by((span.dms + 2) * 3)"),
            ("{ } | by(span.dms + 7 / 2)", "by(span.dms + 3)"),
            ("{ } | by(span.dms + 7.0 / 2)", "by(span.dms + 3.5)"),
            ("{ } | by(span.dms * 1000000.0)", "by(span.dms * 1e+06)"),
            ("{ } | by(span.dms * 0.00001)", "by(span.dms * 1e-05)"),
            ("{ } | by(span.dms * 1.50)", "by(span.dms * 1.5)"),
            ("{ } | by(span.dms * 123456.0)", "by(span.dms * 123456.0)"),
            (
                "{ } | by(span.dms * 1234567.0)",
                "by(span.dms * 1.234567e+06)",
            ),
            ("{ } | by(span.dms * 0.0001)", "by(span.dms * 0.0001)"),
            ("{ } | by(span.dms + 1500ms)", "by(span.dms + 1.5s)"),
            ("{ } | by(span.dms + 90s)", "by(span.dms + 1m30s)"),
            ("{ } | by(span.dms + 3600s)", "by(span.dms + 1h0m0s)"),
            ("{ } | by(span.dms + 1us)", "by(span.dms + 1µs)"),
            ("{ } | by(span.dms * (1ms + 1ms))", "by(span.dms * 2e+06)"),
            ("{ } | by(span.dms > -1)", "by(span.dms > -1)"),
            ("{ } | by(span.dms - -1)", "by(span.dms - -1)"),
            ("{ } | by(-7 / span.k)", "by(-(7 / span.k))"),
            ("{ } | by(-span.dms)", "by(-span.dms)"),
            ("{ } | by(-(span.dms + 1))", "by(-(span.dms + 1))"),
            ("{ } | by(-duration)", "by(-duration)"),
            ("{ } | by(trace:duration / 2)", "by(traceDuration / 2)"),
            ("{ } | by(.dms / 1000)", "by(.dms / 1000)"),
            ("{ } | by(span.s = \"a\\\"b\")", "by(span.s = `a\"b`)"),
            ("{ } | by(span.s =~ \"x\")", "by(span.s =~ `x`)"),
            (
                "{ } | by(span.b || span.dms > 0)",
                "by(span.b || (span.dms > 0))",
            ),
            ("{ } | by(span.dms + 1 > 1000)", "by((span.dms + 1) > 1000)"),
            (
                "{ } | by(span.dms + 1 = 2 || span.b)",
                "by(((span.dms + 1) = 2) || span.b)",
            ),
            ("{ } | by(!(span.dms > 1000))", "by(!(span.dms > 1000))"),
            ("{ } | by(!span.b)", "by(!span.b)"),
            ("{ } | by(span.dms != nil)", "by(span.dms != nil)"),
            ("{ } | by(span.dms = nil)", "by(span.dms = nil)"),
            ("{ } | by(kind = server)", "by(kind = server)"),
            ("{ } | by(status = ok)", "by(status = ok)"),
            ("{ } | by(span.b = true)", "by(span.b = true)"),
            (
                "{ } | by(instrumentation:name = \"t\")",
                "by(instrumentation:name = `t`)",
            ),
            (
                "{ } | by(resource.service.name = \"x\")",
                "by(resource.service.name = `x`)",
            ),
            (
                "{ } | by(9223372036854775807 + span.dms)",
                "by(9223372036854775807 + span.dms)",
            ),
        ] {
            let got = label_of(q);
            if got != want {
                wrong.push(format!("{q}\n  want {want}\n  got  {got}"));
            }
        }
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    }
}
