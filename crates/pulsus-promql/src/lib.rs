//! PromQL planner and hybrid evaluation engine. See docs/architecture.md
//! §5.1. Pure — no I/O, no ClickHouse; `pulsus-read::metrics::exec` is the
//! only crate that resolves/fetches and then calls into this one
//! (`plan` -> `evaluate`).

pub mod annotations;
pub mod error;
pub mod eval;
pub mod math;
pub mod parser;
pub mod plan;
pub mod value;

pub use annotations::{Annotation, AnnotationKind, Annotations, ForcedMonotonicityDetail};
pub use error::PromqlError;
pub use eval::{CancelToken, evaluate, evaluate_cancellable};
pub use math::KahanSum;
pub use parser::parse;
pub use plan::{
    AggOp, BinOp, DEFAULT_LOOKBACK_MS, FillValues, Group, Grouping, Matching, MathFn, OverTimeFn,
    OverTimeParamFn, PlanExpr, PlanParams, QueryPlan, RangeFn, ScalarFn, SelectorId, SelectorSpec,
    SetOp, plan, series_selector,
};
pub use value::{
    FetchedSeries, InstantSample, Labels, Point, QueryValue, RangeSeries, Sample, SeriesData,
};

/// True iff the (paren-stripped) root of `expr` is a call to one of the
/// four sort functions (`sort`/`sort_desc`/`sort_by_label`/
/// `sort_by_label_desc`) — issue #68 (M6-05): the server encoder skips
/// its deterministic label re-sort for a sort-rooted **instant** query so
/// the evaluator's value/label order survives on the wire; every other
/// query keeps the label-sorted output.
pub fn expr_is_sort_root(expr: &parser::Expr) -> bool {
    let mut e = expr;
    while let parser::Expr::Paren(p) = e {
        e = &p.expr;
    }
    matches!(
        e,
        parser::Expr::Call(call) if matches!(
            call.func.name,
            "sort" | "sort_desc" | "sort_by_label" | "sort_by_label_desc"
        )
    )
}

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles() {}

    /// Issue #68: the encoder-ordering gate keys off this predicate —
    /// paren-stripped root only, never a nested sort.
    #[test]
    fn expr_is_sort_root_matches_the_four_sort_functions_at_the_root_only() {
        for query in [
            "sort(up)",
            "sort_desc(up)",
            r#"sort_by_label(up, "job")"#,
            r#"sort_by_label_desc(up, "job")"#,
            "((sort(up)))",
        ] {
            let expr = crate::parse(query).unwrap();
            assert!(super::expr_is_sort_root(&expr), "{query}");
        }
        for query in ["up", "sum(sort(up))", "sort(up) + 0", "abs(up)", "1"] {
            let expr = crate::parse(query).unwrap();
            assert!(!super::expr_is_sort_root(&expr), "{query}");
        }
    }
}
