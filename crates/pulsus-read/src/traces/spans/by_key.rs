//! Issue #649: a `by()` key that is a field expression — its response
//! label, as the reference renders the stage, and its value.

use pulsus_traceql::FieldExpr;

use crate::traces::PlanError;

/// Stub for the tests-first commit.
pub fn label(expr: &FieldExpr) -> Result<String, PlanError> {
    Ok(format!("by({expr})"))
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
