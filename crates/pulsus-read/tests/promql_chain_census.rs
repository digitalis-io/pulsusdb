//! Issue #548 check 2: **the chain model, against a second
//! implementation of the rule and against the reference's own
//! registries.**
//!
//! # Why a differential and not a list of shapes
//!
//! Five rounds of plan review produced five rules that are wrong, and
//! each was caught by the same instrument — a second implementation run
//! over shapes — not by the named rows, because a named list is a finite
//! set of shapes and a wrong rule only has to miss all of them. So the
//! corpus here is **derived**, from the committed function registry and
//! from the planner's own operator lists, and the comparison is against a
//! walker written from the rule's sentence.
//!
//! ```text
//!   registry-v3.13.json  ---+
//!   the 15 + 3 operators ---+--> 219 corpus entries --+--> pulsus_promql::plan
//!   21 named shapes      ---+                         |        |
//!                                                     |        +--> metrics::compile::chain_of
//!                                                     |        |          |
//!                                                     |        |   ordered stage names
//!                                                     |        |          |
//!                                                     |        +--> this file's walker
//!                                                     |                   |
//!                                                     +-- equal? ---------+
//! ```
//!
//! **Two checks, and neither replaces the other.** The identity
//! `chain_of(plan).len() == plan.selectors.len()` fixes the COUNT; the
//! ordered stage-name differential fixes the CONTENTS. Measured on the
//! plan's own corpus during review: three wrong rules returned the right
//! number of chains on all 218 planned entries while getting 19, 33 and
//! 134 link lists wrong.
//!
//! # The walker's independence, and where it stops
//!
//! [`walker::derive`] is written from the rule's sentence and from
//! `pulsus_promql`'s public plan tree. **It never calls
//! `metrics::compile`.** It is a different algorithm, not a
//! transcription: a post-order fold returning each node's entry set and a
//! map from entry to links, where `chain_of` flattens the tree into
//! pre-order, propagates entry sets through a parent array and orders
//! each chain by depth.
//!
//! Its match over `PlanExpr` has **no default arm**, which is stronger
//! than the panic-on-an-unmodelled-variant the plan asks for and serves
//! the same purpose: a corpus entry reaching a variant nobody modelled
//! stops the build rather than producing a comparison that passes for the
//! wrong reason.
//!
//! **Where it stops:** the walker's stage NAMES are a second
//! transcription of the same spellings, not a second derivation of them —
//! `AggOp::LimitK` is spelled `limitk` in both places because that is what
//! a user writes, and no mechanical rule produces it from the variant.
//! What the walker derives independently is the walk: which entry each
//! node belongs to, and in what order the links come out. A shared
//! mistake in the RULE is not closable by any check here, which is why
//! the rule's sentence is stated in the module doc of
//! `crates/pulsus-read/src/metrics/compile.rs` verbatim.
//!
//! # A refused entry is recorded, never skipped
//!
//! One derived entry is refused, and it is derived rather than invented:
//! the registry declares the metadata function with two vector slots, so
//! the generator emits a call with a selector in the second and the
//! parser refuses it. [`REFUSED`] carries that query with its refusal
//! text, every row is asserted to be reached, and an entry refused
//! without a row fails.

use std::collections::{BTreeMap, BTreeSet};

use pulsus_promql::plan::{
    AggOp, BinOp, DateFn, HistogramAccessorFn, MathFn, OverTimeFn, OverTimeParamFn, PlanExpr,
    QueryPlan, RangeFn, RangeSource, ScalarFn, SelectorId, SetOp,
};
use pulsus_promql::{DEFAULT_LOOKBACK_MS, PlanParams, parse};
use pulsus_read::metrics::compile::{chain_of, stage_names};

/// The metric every corpus entry selects.
const M: &str = "http_requests_total";
/// The second, distinct metric the two-selector families use.
const M2: &str = "http_errors_total";

const START_MS: i64 = 1_782_907_200_000;
const END_MS: i64 = 1_782_928_800_000;
const STEP_MS: i64 = 60_000;

fn plan_params() -> PlanParams {
    PlanParams {
        start_ms: START_MS,
        end_ms: END_MS,
        step_ms: STEP_MS,
        lookback_ms: DEFAULT_LOOKBACK_MS,
        experimental_functions: true,
    }
}

/// `Ok(plan)`, or the refusal text from the parser or the planner.
fn try_plan(q: &str) -> Result<QueryPlan, String> {
    let ast = parse(q).map_err(|e| format!("{e}"))?;
    pulsus_promql::plan(&ast, plan_params()).map_err(|e| format!("{e:?}"))
}

fn planned(q: &str) -> QueryPlan {
    try_plan(q).unwrap_or_else(|e| panic!("{q}: {e}"))
}

/// The chains as the code under test reports them: one entry per
/// selector-list position, each with its ordered stage names.
fn under_test(plan: &QueryPlan) -> Vec<(SelectorId, Vec<String>)> {
    chain_of(plan)
        .iter()
        .map(|c| (c.selector, stage_names(c)))
        .collect()
}

// ---------------------------------------------------------------------
// The corpus, derived
// ---------------------------------------------------------------------

mod corpus {
    use super::{M, M2};
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct Registry {
        functions: Vec<Function>,
        aggregation_operators: Vec<AggregationOperator>,
    }

    #[derive(Debug, Deserialize)]
    struct Function {
        name: String,
        arg_types: Vec<String>,
    }

    #[derive(Debug, Deserialize)]
    struct AggregationOperator {
        name: String,
    }

    /// One corpus entry: the family it was derived from, the query, and
    /// what the derivation predicts about it.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct Entry {
        pub family: Family,
        pub query: String,
        /// The number of chains the derivation predicts, where the family
        /// predicts one. `None` where the family makes no prediction and
        /// the identity plus the differential carry the entry.
        pub expect_chains: Option<usize>,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    pub enum Family {
        /// Every registry function over a plain selector.
        FunctionPlain,
        /// Every function with a matrix argument, again over a sub-query.
        FunctionSubquery,
        /// One entry per scalar position beside a vector or matrix slot.
        ScalarPosition,
        /// The operators, two distinct metrics.
        OperatorTwoMetrics,
        /// The operators, one selector.
        OperatorOneSelector,
        /// The operators, one metric twice.
        OperatorSameMetric,
        /// The aggregation operators.
        Aggregation,
        /// The named boundary shapes.
        Named,
    }

    fn registry() -> Registry {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(std::path::Path::parent)
            .expect("workspace root");
        let raw = std::fs::read_to_string(
            root.join("crates/pulsus-promql/tests/promqltest/coverage/registry-v3.13.json"),
        )
        .expect("read the committed registry");
        serde_json::from_str(&raw).expect("parse the committed registry")
    }

    /// The 15 binary operators and the 3 set operators, as a user writes
    /// them. Derived from the planner's own enums by
    /// `every_operator_of_the_planners_own_enums_is_in_the_operator_list`,
    /// which fails if either enum gains a variant.
    pub const BINARY_OPERATORS: [&str; 15] = [
        "+", "-", "*", "/", "%", "^", "atan2", "==", "!=", "<", "<=", ">", ">=", "</", ">/",
    ];
    pub const SET_OPERATORS: [&str; 3] = ["and", "or", "unless"];

    /// The literal each non-principal slot takes when the derivation is
    /// not putting a selector in it.
    fn literal(arg_type: &str) -> String {
        match arg_type {
            "scalar" => "0.5".to_string(),
            "string" => "\"l\"".to_string(),
            other => panic!("no literal for a {other} slot"),
        }
    }

    /// A call string with the selector in every vector and matrix slot
    /// and a literal in every other. `matrix_arg` is what a matrix slot
    /// takes — a range selector, or a sub-query.
    fn call(f: &Function, matrix_arg: &str) -> String {
        let args: Vec<String> = f
            .arg_types
            .iter()
            .map(|t| match t.as_str() {
                "vector" => M.to_string(),
                "matrix" => matrix_arg.to_string(),
                other => literal(other),
            })
            .collect();
        format!("{}({})", f.name, args.join(", "))
    }

    /// How many chains a call over a plain selector predicts: one per
    /// vector or matrix slot, because each takes its own selector.
    fn vector_slots(f: &Function) -> usize {
        f.arg_types
            .iter()
            .filter(|t| t.as_str() == "vector" || t.as_str() == "matrix")
            .count()
    }

    /// The 21 named boundary shapes, from revisions 2 to 5 of the plan.
    /// `m or m` is deliberately absent: the same-metric operator family
    /// derives it.
    pub fn named_shapes() -> Vec<String> {
        vec![
            "vector(1) and vector(2)".to_string(),
            format!("100 * {M}"),
            format!("{M} == bool 100"),
            format!("topk(scalar({M2}), {M})"),
            format!("abs({M} / {M2})"),
            format!("abs(abs({M} / {M2}))"),
            format!("sum({M} and {M2})"),
            format!("abs(info({M}))"),
            format!("abs({M} / 100)"),
            format!("sum(rate({M}[5m])) / sum(rate({M2}[5m]))"),
            format!("rate({M}[5m:1m])"),
            format!("abs(rate({M}[5m:1m]))"),
            format!("max_over_time({M}[5m:1m])"),
            format!("quantile_over_time(0.5, {M}[5m:1m])"),
            format!("absent_over_time({M}[5m:1m])"),
            format!("sum by (status) (rate({M}[5m:1m]))"),
            format!("rate({M}[5m])"),
            format!("sum({M} / {M})"),
            format!("{M} offset 5m / {M}"),
            format!("rate({M}[5m]) / rate({M}[10m])"),
            format!("topk(scalar({M}), {M})"),
        ]
    }

    /// The whole corpus, derived. Duplicates across families are kept:
    /// every assertion is per entry, so a repeated entry asserts the same
    /// thing twice, and removing it would make the published size depend
    /// on a dedup rule rather than on the derivations.
    pub fn build() -> Vec<Entry> {
        let reg = registry();
        let mut out = Vec::new();

        // 89 — every registry function over a plain selector.
        for f in &reg.functions {
            out.push(Entry {
                family: Family::FunctionPlain,
                query: call(f, &format!("{M}[5m]")),
                expect_chains: Some(vector_slots(f)),
            });
        }

        // 27 — every function with a matrix argument, over a sub-query.
        for f in reg
            .functions
            .iter()
            .filter(|f| f.arg_types.iter().any(|t| t.as_str() == "matrix"))
        {
            out.push(Entry {
                family: Family::FunctionSubquery,
                query: call(f, &format!("{M}[5m:1m]")),
                expect_chains: Some(vector_slots(f)),
            });
        }

        // 14 — one entry per scalar position beside a vector or matrix
        // slot. The selector goes in that position and a literal in every
        // other, which is what puts a selector into every non-principal
        // child slot the registry knows about.
        for f in &reg.functions {
            if vector_slots(f) == 0 {
                continue;
            }
            let scalar_positions: Vec<usize> = f
                .arg_types
                .iter()
                .enumerate()
                .filter(|(_, t)| t.as_str() == "scalar")
                .map(|(i, _)| i)
                .collect();
            for chosen in scalar_positions {
                let args: Vec<String> = f
                    .arg_types
                    .iter()
                    .enumerate()
                    .map(|(i, t)| match t.as_str() {
                        "vector" => M.to_string(),
                        "matrix" => format!("{M}[5m]"),
                        "scalar" if i == chosen => format!("scalar({M2})"),
                        other => literal(other),
                    })
                    .collect();
                out.push(Entry {
                    family: Family::ScalarPosition,
                    query: format!("{}({})", f.name, args.join(", ")),
                    expect_chains: Some(vector_slots(f) + 1),
                });
            }
        }

        // 18 — the operators over two distinct metrics.
        for op in BINARY_OPERATORS.iter().chain(SET_OPERATORS.iter()) {
            out.push(Entry {
                family: Family::OperatorTwoMetrics,
                query: format!("{M} {op} {M2}"),
                expect_chains: Some(2),
            });
        }

        // 18 — the operators over one selector. A set operator's operands
        // are both instant vectors, so its scalar side is `vector(1)`.
        for op in BINARY_OPERATORS {
            out.push(Entry {
                family: Family::OperatorOneSelector,
                query: format!("{M} {op} 100"),
                expect_chains: Some(1),
            });
        }
        for op in SET_OPERATORS {
            out.push(Entry {
                family: Family::OperatorOneSelector,
                query: format!("{M} {op} vector(1)"),
                expect_chains: Some(1),
            });
        }

        // 18 — the operators over ONE METRIC TWICE: the family a rule
        // that keys a chain on the metric name is invisible in.
        for op in BINARY_OPERATORS.iter().chain(SET_OPERATORS.iter()) {
            out.push(Entry {
                family: Family::OperatorSameMetric,
                query: format!("{M} {op} {M}"),
                expect_chains: Some(2),
            });
        }

        // 14 — the aggregation operators. The call form is derived by
        // asking the planner rather than from a hand list of which
        // operators take a parameter: the no-parameter form first, then
        // the scalar-parameter form, then the string-parameter one.
        for op in &reg.aggregation_operators {
            let forms = [
                format!("{}({M})", op.name),
                format!("{}(2, {M})", op.name),
                format!("{}(\"l\", {M})", op.name),
            ];
            let query = forms
                .iter()
                .find(|q| super::try_plan(q).is_ok())
                .unwrap_or_else(|| panic!("no derived call form plans for {}", op.name))
                .clone();
            out.push(Entry {
                family: Family::Aggregation,
                query,
                expect_chains: Some(1),
            });
        }

        // 21 — the named boundary shapes. Their own rows assert what each
        // one carries; here they make no count prediction of their own.
        for q in named_shapes() {
            out.push(Entry {
                family: Family::Named,
                query: q,
                expect_chains: None,
            });
        }

        out
    }
}

// ---------------------------------------------------------------------
// The independent walker
// ---------------------------------------------------------------------

/// A second implementation of the chain rule, written from its sentence
/// and from the planner's public plan tree.
///
/// > A chain is rooted at one entry of the planner's selector list, and
/// > there is exactly one chain per entry, always. A node is on entry
/// > `i`'s chain iff `i` is the only entry whose rows it consumes, where
/// > *consumes* is transitive: a node's entry set is the union of its
/// > children's.
///
/// It calls nothing in `pulsus_read::metrics::compile`.
mod walker {
    use super::*;

    /// What one node contributes: the entries it consumes, and the links
    /// already settled beneath it, per entry, innermost first.
    struct Node {
        entries: BTreeSet<SelectorId>,
        chains: BTreeMap<SelectorId, Vec<String>>,
    }

    /// One chain per selector-list entry, in list order.
    pub fn derive(plan: &QueryPlan) -> Vec<(SelectorId, Vec<String>)> {
        let node = fold(&plan.root);
        (0..plan.selectors.len())
            .map(|id| {
                let mut links = vec![format!("Select({id})")];
                if let Some(above) = node.chains.get(&id) {
                    links.extend(above.iter().cloned());
                }
                (id, links)
            })
            .collect()
    }

    fn fold(e: &PlanExpr) -> Node {
        let mut entries: BTreeSet<SelectorId> = own_entries(e);
        let mut chains: BTreeMap<SelectorId, Vec<String>> = BTreeMap::new();
        for child in kids(e) {
            let below = fold(child);
            entries.extend(below.entries);
            for (id, links) in below.chains {
                chains.entry(id).or_default().extend(links);
            }
        }
        // The rule: this node is on a chain when exactly one entry's rows
        // reach it. The push happens after the children's, which is what
        // makes each chain read innermost first.
        if entries.len() == 1
            && let Some(name) = link_name(e)
        {
            let id = *entries.iter().next().expect("one entry");
            chains.entry(id).or_default().push(name);
        }
        Node { entries, chains }
    }

    /// The entries a node consumes on its own account. `Absent`'s and
    /// `Timestamp`'s remembered selector ids are NOT read: each names a
    /// selector that is already the node's own argument. `Info`'s IS:
    /// the synthetic metadata selector is a list entry that appears
    /// nowhere in the tree.
    fn own_entries(e: &PlanExpr) -> BTreeSet<SelectorId> {
        let mut out = BTreeSet::new();
        match e {
            PlanExpr::Selector(id) => {
                out.insert(*id);
            }
            PlanExpr::RangeVector { source }
            | PlanExpr::RangeFn { source, .. }
            | PlanExpr::OverTime { source, .. }
            | PlanExpr::OverTimeParam { source, .. }
            | PlanExpr::AbsentOverTime { source } => {
                if let RangeSource::Selector(id) = source {
                    out.insert(*id);
                }
            }
            PlanExpr::Info { info_selector, .. } => {
                out.insert(*info_selector);
            }
            _ => {}
        }
        out
    }

    /// Every child expression, including the one reachable only through a
    /// sub-query source. **No default arm**: a 30th `PlanExpr` variant
    /// fails to compile here.
    fn kids(e: &PlanExpr) -> Vec<&PlanExpr> {
        fn sub(source: &RangeSource) -> Vec<&PlanExpr> {
            match source {
                RangeSource::Selector(_) => Vec::new(),
                RangeSource::Subquery(sq) => vec![sq.inner.as_ref()],
            }
        }
        match e {
            PlanExpr::Selector(_)
            | PlanExpr::Time
            | PlanExpr::Scalar(_)
            | PlanExpr::StringLiteral(_) => Vec::new(),
            PlanExpr::RangeVector { source }
            | PlanExpr::RangeFn { source, .. }
            | PlanExpr::OverTime { source, .. }
            | PlanExpr::AbsentOverTime { source } => sub(source),
            PlanExpr::OverTimeParam { source, args, .. } => {
                let mut v = sub(source);
                v.extend(args.iter().map(Box::as_ref));
                v
            }
            PlanExpr::Absent { arg, .. }
            | PlanExpr::Sort { arg, .. }
            | PlanExpr::SortByLabel { arg, .. }
            | PlanExpr::LabelReplace { arg, .. }
            | PlanExpr::LabelJoin { arg, .. }
            | PlanExpr::HistogramAccessor { arg, .. }
            | PlanExpr::Timestamp { arg, .. }
            | PlanExpr::ScalarOf { arg }
            | PlanExpr::VectorOf { arg } => vec![arg.as_ref()],
            PlanExpr::HistogramQuantile { quantile, expr, .. } => {
                vec![quantile.as_ref(), expr.as_ref()]
            }
            PlanExpr::HistogramQuantiles {
                expr, quantiles, ..
            } => {
                let mut v = vec![expr.as_ref()];
                v.extend(quantiles.iter().map(Box::as_ref));
                v
            }
            PlanExpr::HistogramFraction {
                lower, upper, expr, ..
            } => vec![lower.as_ref(), upper.as_ref(), expr.as_ref()],
            PlanExpr::Aggregate { expr, param, .. } => {
                let mut v = vec![expr.as_ref()];
                v.extend(param.iter().map(Box::as_ref));
                v
            }
            PlanExpr::CountValues { expr, .. } => vec![expr.as_ref()],
            PlanExpr::Binary { lhs, rhs, .. } | PlanExpr::SetOp { lhs, rhs, .. } => {
                vec![lhs.as_ref(), rhs.as_ref()]
            }
            PlanExpr::MathFn {
                arg, scalar_args, ..
            } => {
                let mut v = vec![arg.as_ref()];
                v.extend(scalar_args.iter().map(Box::as_ref));
                v
            }
            PlanExpr::ScalarFn { args, .. } => args.iter().map(Box::as_ref).collect(),
            PlanExpr::DateFn { arg, .. } => arg.iter().map(Box::as_ref).collect(),
            PlanExpr::Info { base, .. } => vec![base.as_ref()],
        }
    }

    /// The link a node becomes, or `None` for the five variants that are
    /// on no chain. **No default arm.**
    fn link_name(e: &PlanExpr) -> Option<String> {
        Some(match e {
            PlanExpr::RangeVector { .. } => "RangeVector".to_string(),
            PlanExpr::RangeFn { func, .. } => format!("RangeFn({})", range_fn(*func)),
            PlanExpr::OverTime { func, .. } => format!("OverTime({})", over_time(*func)),
            PlanExpr::OverTimeParam { func, .. } => {
                format!("OverTimeParam({})", over_time_param(*func))
            }
            PlanExpr::AbsentOverTime { .. } => "AbsentOverTime".to_string(),
            PlanExpr::Absent { .. } => "Absent".to_string(),
            PlanExpr::Sort { .. } => "Sort".to_string(),
            PlanExpr::SortByLabel { .. } => "SortByLabel".to_string(),
            PlanExpr::LabelReplace { .. } => "LabelReplace".to_string(),
            PlanExpr::LabelJoin { .. } => "LabelJoin".to_string(),
            PlanExpr::HistogramQuantile { .. } => "HistogramQuantile".to_string(),
            PlanExpr::HistogramQuantiles { .. } => "HistogramQuantiles".to_string(),
            PlanExpr::HistogramAccessor { func, .. } => {
                format!("HistogramAccessor({})", hist_accessor(*func))
            }
            PlanExpr::HistogramFraction { .. } => "HistogramFraction".to_string(),
            PlanExpr::Aggregate { op, .. } => format!("Aggregate({})", agg_op(*op)),
            PlanExpr::CountValues { .. } => "CountValues".to_string(),
            PlanExpr::Binary { op, .. } => format!("Binary({})", bin_op(*op)),
            PlanExpr::SetOp { op, .. } => format!("SetOp({})", set_op(*op)),
            PlanExpr::MathFn { func, .. } => format!("MathFn({})", math_fn(*func)),
            PlanExpr::ScalarFn { func, .. } => format!("ScalarFn({})", scalar_fn(*func)),
            PlanExpr::DateFn { func, .. } => format!("DateFn({})", date_fn(*func)),
            PlanExpr::Timestamp { .. } => "Timestamp".to_string(),
            PlanExpr::ScalarOf { .. } => "ScalarOf".to_string(),
            PlanExpr::VectorOf { .. } => "VectorOf".to_string(),
            PlanExpr::Selector(_)
            | PlanExpr::Time
            | PlanExpr::Scalar(_)
            | PlanExpr::StringLiteral(_)
            | PlanExpr::Info { .. } => return None,
        })
    }

    fn range_fn(f: RangeFn) -> &'static str {
        match f {
            RangeFn::Rate => "rate",
            RangeFn::Irate => "irate",
            RangeFn::Increase => "increase",
            RangeFn::Delta => "delta",
        }
    }

    fn over_time(f: OverTimeFn) -> &'static str {
        match f {
            OverTimeFn::Avg => "avg",
            OverTimeFn::Min => "min",
            OverTimeFn::Max => "max",
            OverTimeFn::Sum => "sum",
            OverTimeFn::Count => "count",
            OverTimeFn::Stddev => "stddev",
            OverTimeFn::Stdvar => "stdvar",
            OverTimeFn::Last => "last",
            OverTimeFn::Present => "present",
            OverTimeFn::Idelta => "idelta",
            OverTimeFn::Resets => "resets",
            OverTimeFn::Changes => "changes",
            OverTimeFn::Deriv => "deriv",
            OverTimeFn::First => "first",
            OverTimeFn::Mad => "mad",
            OverTimeFn::TsOfMin => "ts_of_min",
            OverTimeFn::TsOfMax => "ts_of_max",
            OverTimeFn::TsOfFirst => "ts_of_first",
            OverTimeFn::TsOfLast => "ts_of_last",
        }
    }

    fn over_time_param(f: OverTimeParamFn) -> &'static str {
        match f {
            OverTimeParamFn::Quantile => "quantile",
            OverTimeParamFn::PredictLinear => "predict_linear",
            OverTimeParamFn::DoubleExpSmoothing => "double_exponential_smoothing",
        }
    }

    fn hist_accessor(f: HistogramAccessorFn) -> &'static str {
        match f {
            HistogramAccessorFn::Count => "count",
            HistogramAccessorFn::Sum => "sum",
            HistogramAccessorFn::Avg => "avg",
            HistogramAccessorFn::StdDev => "stddev",
            HistogramAccessorFn::StdVar => "stdvar",
        }
    }

    fn agg_op(op: AggOp) -> &'static str {
        match op {
            AggOp::Sum => "sum",
            AggOp::Avg => "avg",
            AggOp::Min => "min",
            AggOp::Max => "max",
            AggOp::Count => "count",
            AggOp::Group => "group",
            AggOp::Topk => "topk",
            AggOp::Bottomk => "bottomk",
            AggOp::Stddev => "stddev",
            AggOp::Stdvar => "stdvar",
            AggOp::Quantile => "quantile",
            AggOp::LimitK => "limitk",
            AggOp::LimitRatio => "limit_ratio",
        }
    }

    fn bin_op(op: BinOp) -> &'static str {
        match op {
            BinOp::Add => "add",
            BinOp::Sub => "sub",
            BinOp::Mul => "mul",
            BinOp::Div => "div",
            BinOp::Mod => "mod",
            BinOp::Pow => "pow",
            BinOp::Atan2 => "atan2",
            BinOp::Eq => "eq",
            BinOp::Ne => "ne",
            BinOp::Lt => "lt",
            BinOp::Le => "le",
            BinOp::Gt => "gt",
            BinOp::Ge => "ge",
            BinOp::TrimUpper => "trim_upper",
            BinOp::TrimLower => "trim_lower",
        }
    }

    fn set_op(op: SetOp) -> &'static str {
        match op {
            SetOp::And => "and",
            SetOp::Or => "or",
            SetOp::Unless => "unless",
        }
    }

    fn math_fn(f: MathFn) -> &'static str {
        match f {
            MathFn::Abs => "abs",
            MathFn::Ceil => "ceil",
            MathFn::Floor => "floor",
            MathFn::Sqrt => "sqrt",
            MathFn::Sgn => "sgn",
            MathFn::Deg => "deg",
            MathFn::Rad => "rad",
            MathFn::Exp => "exp",
            MathFn::Ln => "ln",
            MathFn::Log2 => "log2",
            MathFn::Log10 => "log10",
            MathFn::Sin => "sin",
            MathFn::Cos => "cos",
            MathFn::Tan => "tan",
            MathFn::Asin => "asin",
            MathFn::Acos => "acos",
            MathFn::Atan => "atan",
            MathFn::Sinh => "sinh",
            MathFn::Cosh => "cosh",
            MathFn::Tanh => "tanh",
            MathFn::Asinh => "asinh",
            MathFn::Acosh => "acosh",
            MathFn::Atanh => "atanh",
            MathFn::Clamp => "clamp",
            MathFn::ClampMin => "clamp_min",
            MathFn::ClampMax => "clamp_max",
            MathFn::Round => "round",
        }
    }

    fn scalar_fn(f: ScalarFn) -> &'static str {
        match f {
            ScalarFn::Pi => "pi",
            ScalarFn::MaxOf => "max_of",
            ScalarFn::MinOf => "min_of",
        }
    }

    fn date_fn(f: DateFn) -> &'static str {
        match f {
            DateFn::Year => "year",
            DateFn::Month => "month",
            DateFn::DayOfMonth => "day_of_month",
            DateFn::DayOfWeek => "day_of_week",
            DateFn::DayOfYear => "day_of_year",
            DateFn::DaysInMonth => "days_in_month",
            DateFn::Hour => "hour",
            DateFn::Minute => "minute",
        }
    }
}

// ---------------------------------------------------------------------
// The refused entries
// ---------------------------------------------------------------------

/// Every corpus entry the parser or the planner refuses, with its
/// refusal. One row, and it is derived rather than invented: the registry
/// declares the metadata function with two vector slots.
const REFUSED: [(&str, &str); 1] = [(
    "info(http_requests_total, http_requests_total)",
    "expected label selectors only, got vector selector instead",
)];

// ---------------------------------------------------------------------
// The corpus's own shape
// ---------------------------------------------------------------------

/// The composition published on the issue, derived here rather than
/// typed: `89 + 27 + 14 + 18 + 18 + 18 + 14 + 21`.
#[test]
fn the_corpus_is_the_published_composition() {
    let entries = corpus::build();
    let mut counts: BTreeMap<corpus::Family, usize> = BTreeMap::new();
    for e in &entries {
        *counts.entry(e.family).or_default() += 1;
    }
    let published: [(corpus::Family, usize); 8] = [
        (corpus::Family::FunctionPlain, 89),
        (corpus::Family::FunctionSubquery, 27),
        (corpus::Family::ScalarPosition, 14),
        (corpus::Family::OperatorTwoMetrics, 18),
        (corpus::Family::OperatorOneSelector, 18),
        (corpus::Family::OperatorSameMetric, 18),
        (corpus::Family::Aggregation, 14),
        (corpus::Family::Named, 21),
    ];
    for (family, n) in published {
        assert_eq!(counts.get(&family).copied(), Some(n), "{family:?}");
    }
    assert_eq!(
        published.iter().map(|(_, n)| n).sum::<usize>(),
        219,
        "the published composition sums to the published size"
    );
    assert_eq!(entries.len(), 219, "the corpus cannot silently shrink");
}

/// The operator list the corpus derives three families from is the
/// planner's own: 15 binary operators and 3 set operators. A variant
/// added to either enum fails this, so the families cannot go stale.
#[test]
fn every_operator_of_the_planners_own_enums_is_in_the_operator_list() {
    let binary = [
        BinOp::Add,
        BinOp::Sub,
        BinOp::Mul,
        BinOp::Div,
        BinOp::Mod,
        BinOp::Pow,
        BinOp::Atan2,
        BinOp::Eq,
        BinOp::Ne,
        BinOp::Lt,
        BinOp::Le,
        BinOp::Gt,
        BinOp::Ge,
        BinOp::TrimUpper,
        BinOp::TrimLower,
    ];
    // No `_` arm: a 16th `BinOp` fails to build here.
    for op in binary {
        let written = match op {
            BinOp::Add => "+",
            BinOp::Sub => "-",
            BinOp::Mul => "*",
            BinOp::Div => "/",
            BinOp::Mod => "%",
            BinOp::Pow => "^",
            BinOp::Atan2 => "atan2",
            BinOp::Eq => "==",
            BinOp::Ne => "!=",
            BinOp::Lt => "<",
            BinOp::Le => "<=",
            BinOp::Gt => ">",
            BinOp::Ge => ">=",
            BinOp::TrimUpper => "</",
            BinOp::TrimLower => ">/",
        };
        assert!(
            corpus::BINARY_OPERATORS.contains(&written),
            "{written} is a BinOp the corpus does not carry"
        );
    }
    assert_eq!(corpus::BINARY_OPERATORS.len(), binary.len());
    for op in [SetOp::And, SetOp::Or, SetOp::Unless] {
        let written = match op {
            SetOp::And => "and",
            SetOp::Or => "or",
            SetOp::Unless => "unless",
        };
        assert!(corpus::SET_OPERATORS.contains(&written));
    }
    assert_eq!(corpus::SET_OPERATORS.len(), 3);
}

/// Every entry plans, or is recorded with its refusal — never skipped.
#[test]
fn every_corpus_entry_plans_or_is_recorded_with_its_refusal() {
    let entries = corpus::build();
    let mut reached: BTreeSet<&str> = BTreeSet::new();
    let mut refused = 0usize;
    for e in &entries {
        match try_plan(&e.query) {
            Ok(_) => {}
            Err(message) => {
                refused += 1;
                let row = REFUSED
                    .iter()
                    .find(|(q, _)| *q == e.query)
                    .unwrap_or_else(|| {
                        panic!("{}: refused with {message}, and no row records it", e.query)
                    });
                assert!(
                    message.contains(row.1),
                    "{}: refusal text moved.\n  recorded: {}\n   printed: {message}",
                    e.query,
                    row.1
                );
                reached.insert(row.0);
            }
        }
    }
    for (q, _) in REFUSED {
        assert!(reached.contains(q), "{q} is recorded as refused but plans");
    }
    assert_eq!(
        refused,
        REFUSED.len(),
        "one refused entry, and it is the one"
    );
    assert_eq!(refused, 1);
}

// ---------------------------------------------------------------------
// The two checks that carry the whole corpus
// ---------------------------------------------------------------------

/// **The identity.** `chain_of(plan).len() == plan.selectors.len()` on
/// every planned entry, with the roots `Select(0) … Select(n-1)` in list
/// order.
///
/// It is what a rule keyed on the metric name fails: `m / m` is two
/// entries, with two ids, two fetch windows that may differ and two
/// statements, and one chain would describe a read the engine does not
/// perform.
#[test]
fn every_chain_is_one_selector_list_entry() {
    let entries = corpus::build();
    let mut checked = 0usize;
    for e in &entries {
        let Ok(plan) = try_plan(&e.query) else {
            continue;
        };
        let chains = under_test(&plan);
        assert_eq!(
            chains.len(),
            plan.selectors.len(),
            "{}: one chain per selector-list entry",
            e.query
        );
        for (i, (id, links)) in chains.iter().enumerate() {
            assert_eq!(*id, i, "{}: chains are in list order", e.query);
            assert_eq!(
                links.first().map(String::as_str),
                Some(format!("Select({i})").as_str()),
                "{}: chain {i} is rooted at its own entry",
                e.query
            );
        }
        if let Some(n) = e.expect_chains {
            assert_eq!(
                chains.len(),
                n,
                "{}: the family predicts {n} chains",
                e.query
            );
        }
        checked += 1;
    }
    assert_eq!(checked, 218, "218 of the 219 entries plan");
}

/// **The differential.** On every planned entry, the chains and their
/// ordered stage names equal the independent walker's.
///
/// The identity above fixes the count and cannot see the contents: three
/// wrong rules measured during review returned the right number of chains
/// on all 218 while getting 19, 33 and 134 link lists wrong.
#[test]
fn every_corpus_query_agrees_with_the_independent_derivation() {
    let entries = corpus::build();
    let mut disagreements: Vec<String> = Vec::new();
    let mut compared = 0usize;
    for e in &entries {
        let Ok(plan) = try_plan(&e.query) else {
            continue;
        };
        let ours = under_test(&plan);
        let theirs = walker::derive(&plan);
        if ours != theirs {
            disagreements.push(format!(
                "{}\n   chain_of: {ours:?}\n     walker: {theirs:?}",
                e.query
            ));
        }
        compared += 1;
    }
    assert_eq!(compared, 218, "218 of the 219 entries plan");
    assert!(
        disagreements.is_empty(),
        "{} of {compared} entries disagree with the independent derivation:\n{}",
        disagreements.len(),
        disagreements.join("\n")
    );
}

// ---------------------------------------------------------------------
// The named rows — the cases a reader can check by eye
// ---------------------------------------------------------------------

/// The chains of `q`, as `(selector, ordered stage names)`.
fn chains_of(q: &str) -> Vec<(SelectorId, Vec<String>)> {
    under_test(&planned(q))
}

/// The stage names of chain `i`, as `&str`s, for a literal comparison.
fn links(chains: &[(SelectorId, Vec<String>)], i: usize) -> Vec<&str> {
    chains[i].1.iter().map(String::as_str).collect()
}

/// Criterion 3: **each selector-rooted stack becomes exactly one chain.**
#[test]
fn a_two_selector_expression_yields_one_chain_per_selector() {
    let c = chains_of(&format!("rate({M}[5m]) / on (instance) rate({M2}[5m])"));
    assert_eq!(c.len(), 2);
    assert_eq!(links(&c, 0), ["Select(0)", "RangeFn(rate)"]);
    assert_eq!(links(&c, 1), ["Select(1)", "RangeFn(rate)"]);

    let c = chains_of(&format!("sum by (status) (rate({M}[5m]))"));
    assert_eq!(c.len(), 1);
    assert_eq!(
        links(&c, 0),
        ["Select(0)", "RangeFn(rate)", "Aggregate(sum)"]
    );

    let c = chains_of(&format!("{M} * 100"));
    assert_eq!(c.len(), 1);
    assert_eq!(links(&c, 0), ["Select(0)", "Binary(mul)"]);

    // One metric written twice is TWO entries, not one.
    let c = chains_of(&format!("{M} / {M}"));
    assert_eq!(c.len(), 2, "two occurrences of one metric are two entries");
    assert_eq!(links(&c, 0), ["Select(0)"]);
    assert_eq!(links(&c, 1), ["Select(1)"]);
}

/// The metadata join is two chains of one link each: `info` always
/// consumes the synthetic metadata selector as well as its argument, so
/// it is on neither.
///
/// **`info(m)` alone cannot see the synthetic entry being dropped**, and
/// that is measured rather than assumed: forget it and the planner still
/// makes two entries, the `Info` node is a link in neither reading, and
/// both chains still read `Select(i)`. What sees it is a node ABOVE the
/// join, which stops consuming two entries and attaches — so the second
/// row here is `abs(info(m))`, and it is the row this test's own break
/// reddens.
#[test]
fn the_metadata_join_yields_two_chains_of_one_link() {
    let c = chains_of(&format!("info({M})"));
    assert_eq!(c.len(), 2);
    assert_eq!(links(&c, 0), ["Select(0)"]);
    assert_eq!(links(&c, 1), ["Select(1)"]);

    let c = chains_of(&format!("abs(info({M}))"));
    assert_eq!(c.len(), 2, "the join still consumes both entries");
    assert_eq!(
        links(&c, 0),
        ["Select(0)"],
        "no `MathFn` above a node that consumes two entries"
    );
    assert_eq!(links(&c, 1), ["Select(1)"]);
}

/// Criterion 8: the census's function row. The call string and the
/// expected count both come from the registry entry.
#[test]
fn every_registry_function_yields_the_chain_count_its_arg_types_predict() {
    let rows: Vec<corpus::Entry> = corpus::build()
        .into_iter()
        .filter(|e| e.family == corpus::Family::FunctionPlain)
        .collect();
    assert_eq!(rows.len(), 89, "the registry's own function count");
    let mut planned_rows = 0usize;
    for row in &rows {
        let Ok(plan) = try_plan(&row.query) else {
            continue;
        };
        assert_eq!(
            under_test(&plan).len(),
            row.expect_chains.expect("the family predicts a count"),
            "{}",
            row.query
        );
        planned_rows += 1;
    }
    assert_eq!(planned_rows, 88, "88 of the 89 plan; the 89th is recorded");
}

/// Criterion 8: every aggregation operator is one chain whose last link
/// is that operator's.
#[test]
fn every_aggregation_operator_is_one_chain_with_an_aggregate_link() {
    let rows: Vec<corpus::Entry> = corpus::build()
        .into_iter()
        .filter(|e| e.family == corpus::Family::Aggregation)
        .collect();
    assert_eq!(rows.len(), 14, "the registry's own operator count");
    for row in &rows {
        let c = chains_of(&row.query);
        assert_eq!(c.len(), 1, "{}", row.query);
        let last = c[0].1.last().expect("a link above the source").clone();
        assert!(
            last.starts_with("Aggregate(") || last == "CountValues",
            "{}: the last link is {last}",
            row.query
        );
    }
}

/// Criterion 8: an operator over two selectors is on **no** chain.
#[test]
fn every_operator_over_two_selectors_is_on_no_chain() {
    let rows: Vec<corpus::Entry> = corpus::build()
        .into_iter()
        .filter(|e| e.family == corpus::Family::OperatorTwoMetrics)
        .collect();
    assert_eq!(rows.len(), 18);
    for row in &rows {
        let c = chains_of(&row.query);
        assert_eq!(c.len(), 2, "{}", row.query);
        assert_eq!(links(&c, 0), ["Select(0)"], "{}", row.query);
        assert_eq!(links(&c, 1), ["Select(1)"], "{}", row.query);
    }
}

/// Criterion 8, the other side: an operator over ONE selector **is** on
/// its chain. A builder that always excluded an operator would pass the
/// row above and fails this one.
#[test]
fn every_operator_over_one_selector_is_on_its_chain() {
    let rows: Vec<corpus::Entry> = corpus::build()
        .into_iter()
        .filter(|e| e.family == corpus::Family::OperatorOneSelector)
        .collect();
    assert_eq!(rows.len(), 18);
    for row in &rows {
        let c = chains_of(&row.query);
        assert_eq!(c.len(), 1, "{}", row.query);
        let last = c[0].1.last().expect("a link above the source").clone();
        assert!(
            last.starts_with("Binary(") || last.starts_with("SetOp("),
            "{}: the last link is {last}",
            row.query
        );
    }
}

/// Criterion 8: one metric written twice is two entries, and the
/// operator is on neither — the family a rule keyed on the metric name is
/// invisible in.
#[test]
fn every_operator_over_one_metric_twice_is_two_chains() {
    let rows: Vec<corpus::Entry> = corpus::build()
        .into_iter()
        .filter(|e| e.family == corpus::Family::OperatorSameMetric)
        .collect();
    assert_eq!(rows.len(), 18);
    for row in &rows {
        let c = chains_of(&row.query);
        assert_eq!(c.len(), 2, "{}", row.query);
        assert_eq!(links(&c, 0), ["Select(0)"], "{}", row.query);
        assert_eq!(links(&c, 1), ["Select(1)"], "{}", row.query);
    }
}

/// Criterion 8: an operator over no selector yields no chain at all.
#[test]
fn an_operator_over_no_selector_yields_no_chain() {
    assert!(chains_of("vector(1) and vector(2)").is_empty());
}

/// Criterion 8: an aggregate whose PARAMETER bears a selector consumes
/// two entries, so it is off the chain on the same per-instance rule.
///
/// **The `scalar(...)` around the parameter IS on a chain**, and that is
/// the rule rather than an exception to it: it consumes one entry — the
/// parameter's own selector, which the planner lists FIRST because it is
/// written first — so it sits on that entry's chain, while the `topk`
/// above it consumes both and sits on neither.
#[test]
fn an_aggregate_whose_parameter_bears_a_selector_is_on_no_chain() {
    for q in [
        format!("topk(scalar({M2}), {M})"),
        // The same shape with one metric on both sides: still two
        // entries, still no aggregate link.
        format!("topk(scalar({M}), {M})"),
    ] {
        let c = chains_of(&q);
        assert_eq!(c.len(), 2, "{q}");
        assert_eq!(links(&c, 0), ["Select(0)", "ScalarOf"], "{q}");
        assert_eq!(links(&c, 1), ["Select(1)"], "{q}");
        for (i, (_, names)) in c.iter().enumerate() {
            assert!(
                !names.iter().any(|n| n.starts_with("Aggregate(")),
                "{q}: chain {i} carries an aggregate link: {names:?}"
            );
        }
    }
}

/// Criterion 8: **if a node is on no chain, no ancestor of it is on any
/// chain** — and below it each chain carries on.
#[test]
fn no_ancestor_of_an_off_chain_node_is_on_a_chain() {
    let rows: Vec<(String, Vec<Vec<&str>>)> = vec![
        (
            format!("abs({M} / {M2})"),
            vec![vec!["Select(0)"], vec!["Select(1)"]],
        ),
        (
            format!("abs(abs({M} / {M2}))"),
            vec![vec!["Select(0)"], vec!["Select(1)"]],
        ),
        (
            format!("sum({M} and {M2})"),
            vec![vec!["Select(0)"], vec!["Select(1)"]],
        ),
        (
            format!("abs(info({M}))"),
            vec![vec!["Select(0)"], vec!["Select(1)"]],
        ),
        (
            format!("abs({M} / 100)"),
            vec![vec!["Select(0)", "Binary(div)", "MathFn(abs)"]],
        ),
        (
            format!("sum(rate({M}[5m])) / sum(rate({M2}[5m]))"),
            vec![
                vec!["Select(0)", "RangeFn(rate)", "Aggregate(sum)"],
                vec!["Select(1)", "RangeFn(rate)", "Aggregate(sum)"],
            ],
        ),
    ];
    assert_eq!(rows.len(), 6);
    for (q, expected) in rows {
        let c = chains_of(&q);
        assert_eq!(c.len(), expected.len(), "{q}: chain count");
        for (i, want) in expected.iter().enumerate() {
            assert_eq!(&links(&c, i), want, "{q}: chain {i}");
        }
    }
}

/// Criterion 8: **a sub-query source keeps every link above it.** A
/// selector inside a sub-query is reachable only through the sub-query's
/// own field; skip that one arm and the range function above it, and
/// everything above that, falls off the chain.
///
/// The assertion is the exact ordered list, not the presence of a link:
/// the same slip has two plausible codings — an EMPTY chain, or
/// `Select(0)` alone — and a third attaches the right links in the wrong
/// order. Only the whole list catches all three.
#[test]
fn a_subquery_source_keeps_every_link_above_it() {
    let rows: Vec<(String, Vec<&str>)> = vec![
        (
            format!("rate({M}[5m:1m])"),
            vec!["Select(0)", "RangeFn(rate)"],
        ),
        (
            format!("abs(rate({M}[5m:1m]))"),
            vec!["Select(0)", "RangeFn(rate)", "MathFn(abs)"],
        ),
        (
            format!("max_over_time({M}[5m:1m])"),
            vec!["Select(0)", "OverTime(max)"],
        ),
        (
            format!("quantile_over_time(0.5, {M}[5m:1m])"),
            vec!["Select(0)", "OverTimeParam(quantile)"],
        ),
        (
            format!("absent_over_time({M}[5m:1m])"),
            vec!["Select(0)", "AbsentOverTime"],
        ),
        (
            format!("sum by (status) (rate({M}[5m:1m]))"),
            vec!["Select(0)", "RangeFn(rate)", "Aggregate(sum)"],
        ),
        // The contrast: four characters different, and it is the row a
        // mutation that reddens everything cannot be scored against.
        (format!("rate({M}[5m])"), vec!["Select(0)", "RangeFn(rate)"]),
    ];
    assert_eq!(rows.len(), 7);
    for (q, want) in rows {
        let c = chains_of(&q);
        assert_eq!(c.len(), 1, "{q}: one chain");
        assert_eq!(links(&c, 0), want, "{q}");
    }
}

/// The fifth holder of a range source cannot take a sub-query, so
/// "exactly four variants can" is an enumeration of the type rather than
/// a sample of queries.
#[test]
fn a_bare_subquery_at_the_root_is_refused_by_the_planner() {
    let err = try_plan(&format!("{M}[5m:1m]")).expect_err("refused");
    assert!(
        err.contains("subquery used outside a range-vector function argument"),
        "the planner's own refusal: {err}"
    );
}

/// Two entries sharing a metric name do not merely send the same
/// statement twice: an offset or a range moves the window, so they send
/// DIFFERENT statements. A model that merged them would name two
/// statements where four are sent.
#[test]
fn two_entries_sharing_a_metric_name_can_have_different_windows() {
    let plan = planned(&format!("{M} offset 5m / {M}"));
    assert_eq!(plan.selectors.len(), 2);
    let p = plan_params();
    let w0 = plan.selectors[0].fetch_window(&p);
    let w1 = plan.selectors[1].fetch_window(&p);
    assert_eq!(w0, (1_782_906_600_000, 1_782_928_500_000));
    assert_eq!(w1, (1_782_906_900_000, 1_782_928_800_000));
    assert_ne!(w0, w1, "the offset moves the window");
    assert_eq!(under_test(&plan).len(), 2);

    let plan = planned(&format!("rate({M}[5m]) / rate({M}[10m])"));
    assert_eq!(plan.selectors.len(), 2);
    let w0 = plan.selectors[0].fetch_window(&p);
    let w1 = plan.selectors[1].fetch_window(&p);
    assert_eq!(w0, (1_782_906_600_000, 1_782_928_800_000));
    assert_eq!(w1, (1_782_906_300_000, 1_782_928_800_000));
    assert_ne!(w0, w1, "the range moves the window");
    assert_eq!(under_test(&plan).len(), 2);

    // And the same metric twice with no modifier: two entries, one window.
    let plan = planned(&format!("sum({M} / {M})"));
    assert_eq!(plan.selectors.len(), 2);
    assert_eq!(
        plan.selectors[0].fetch_window(&p),
        plan.selectors[1].fetch_window(&p)
    );
    assert_eq!(under_test(&plan).len(), 2);
}
