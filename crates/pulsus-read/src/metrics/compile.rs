//! PromQL against the shared compile core (issue #548).
//!
//! **No statement changes and no answer changes.** The chain is built
//! from the planner's own tree, only when `X-Pulsus-Explain: 1` is set,
//! and rendered as one additive key on the metrics explain response.
//! `exec.rs` still fetches and evaluates exactly as it did; the core is
//! not modified — no new variant, no new wire word, no signature change.
//!
//! # The chain rule
//!
//! > **A chain is rooted at one entry of the planner's selector list** —
//! > [`pulsus_promql::QueryPlan::selectors`], indexed by
//! > [`SelectorId`] — **and [`chain_of`] returns exactly one chain per
//! > entry, always.** A node of the plan tree is on entry `i`'s chain
//! > **iff `i` is the only entry whose rows it consumes**, where
//! > *consumes* is transitive: a node's entry set is the union of its
//! > children's.
//! >
//! > **An entry is a list position, never a metric name.** Two
//! > occurrences of one metric are two entries, with two ids, two fetch
//! > windows that may differ, and two statements. Nothing about a chain's
//! > identity is derived from what a selector matches.
//!
//! That sentence is copied verbatim from the plan, so the sentence a
//! reviewer reads and the sentence this code implements are one string.
//! Two consequences follow from it and are worth stating because four
//! wrong rules were built that satisfy some of the rows and not the rule:
//!
//! * **If a node is on no chain, no ancestor of it is on any chain.** An
//!   ancestor consumes at least what its child consumed, so the exclusion
//!   is monotonic upward.
//! * **Below a node that is on no chain each chain carries on.**
//!   `sum(rate(m[5m])) / sum(rate(m2[5m]))` is two chains of three links;
//!   only the `/` is off.
//!
//! ```text
//!    abs( m / m2 )                      abs( m / 100 )
//!         |    \                             |    \
//!         |     2 entries below              |     1 entry below
//!         v                                  v
//!    the abs is on NO chain             the abs IS on m's chain
//!    2 chains, 1 link each              1 chain, 3 links
//!    no Binary, no MathFn               Select(0), Binary(div), MathFn(abs)
//! ```
//!
//! The two queries differ in one token.
//!
//! # A selector's read is TWO statements, so it is two parts
//!
//! Every selector that reads sends a float read and its complementary
//! histogram read. They are expressed the way the core already expresses
//! "one statement per source, merged in our process": a predicate that is
//! a disjunction whose two leaves name different sources, which
//! [`plan_of`] partitions into two parts with `cut: disjoint_sources` on
//! the second. One part would have the plan say one statement where two
//! are sent, on every PromQL query.
//!
//! # `Fidelity::Wider` on the source link
//!
//! [`pulsus_promql::SelectorSpec::fetch_window`] subtracts one lookback
//! unconditionally — *"deliberately conservative (over-fetches by up to
//! one lookback width for range-vector-only queries)"*
//! (`crates/pulsus-promql/src/plan.rs:180-186`). The rows the statement
//! returns are a superset of the rows the evaluator uses and the
//! evaluator re-selects per step, so `Wider` — the evaluator MUST
//! re-apply — is the true reading. **A pushed aggregate may therefore not
//! be a plain `GROUP BY` over the fetched rows**: it has to produce the
//! per-step grid, or it answers over the over-fetched window.
//!
//! # The window a lowered link reads
//!
//! **A window lowered into SQL reads [`SelectorSpec::fetch_window`]'s
//! milliseconds, never [`RequestBounds`]' nanoseconds.** The two agree
//! below `9_223_372_036_855` ms and part there: `i64::MAX / 1_000_000` is
//! `9_223_372_036_854`, and the accept surface admits a larger value
//! because `parse_time` clamps to `i64::MAX` milliseconds rather than
//! rejecting. [`request_bounds`] saturates rather than multiplying, so a
//! request the accept surface already lets through cannot abort the
//! process in a debug build; nothing in this piece reads those
//! nanoseconds.
//!
//! # What is deliberately not represented
//!
//! * **Label names in the column set.** [`ColSet`] holds the statement's
//!   columns (`fingerprint`, `unix_milli`, `value`); a PromQL group key
//!   is a *label*, which is not a column. So every residual effect here
//!   is the identity and no link may lower against a label name. The
//!   piece that lowers `by (l)` must first decide how a label enters the
//!   column set.
//! * **The chunk driver.** A selector resolving to more than
//!   `CHUNK_THRESHOLD` fingerprints sends one statement per chunk per
//!   table; the plan says `issue: once`. The core attaches a chunk driver
//!   only to a *seeded* part, and this seed is a fingerprint set our own
//!   cache resolved, not a value an earlier statement produced.
//! * **`Never`.** No PromQL link is classified [`Capability::Never`].
//!   Every candidate is "no SQL form written yet", which is what
//!   `NotYetLowered` says.
//! * **A link's payload, on the explain surface.** [`stage_names`] prints
//!   the link kind and its operator — `Aggregate(max)`, `RangeFn(rate)` —
//!   and not the grouping. Nothing here compares payloads beyond the
//!   stage name, and no payload reaches a statement in this piece; the
//!   first piece to build SQL from a payload owes a check on it.
//!
//! [`plan_of`]: crate::compile::plan::plan_of
//! [`ColSet`]: crate::compile::fold::ColSet
//! [`SelectorSpec::fetch_window`]: pulsus_promql::SelectorSpec::fetch_window

use pulsus_promql::plan::{
    AggOp, BinOp, DateFn, Grouping, HistogramAccessorFn, MathFn, OverTimeFn, OverTimeParamFn,
    PlanExpr, RangeFn, RangeSource, ScalarFn, SelectorId, SetOp,
};

use super::exec::MetricQueryParams;
use crate::compile::fold::{
    BlockReason, Capability, Col, ColSet, Fidelity, Lang, Lower, LowerCx, Name, Pred, Provenance,
    Relation, RequestBounds, Shape, SourceName, SourceTerm, lower_chain,
};
use crate::compile::plan::{
    HandoffCost, PlanConfig, PlanCx, PlanShape, SourceRef, plan_of as core_plan_of,
};
use crate::logql::error::ReadError;

// ---------------------------------------------------------------------
// Sources
// ---------------------------------------------------------------------

/// The float sample table.
pub const METRIC_SAMPLES: SourceRef = SourceRef("metric_samples");
/// The native-histogram sample table: the complementary read every
/// selector issues beside the float one, merged in our process.
pub const METRIC_HIST_SAMPLES: SourceRef = SourceRef("metric_hist_samples");

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PqlSource(pub SourceRef);

impl SourceName for PqlSource {
    fn source_ref(&self) -> SourceRef {
        self.0
    }

    fn named(s: SourceRef) -> Self {
        PqlSource(s)
    }
}

/// PromQL's shapes.
///
/// One variant today. The rows a selector's statements return are
/// samples; nothing this piece builds has any other shape. The piece that
/// lowers an aggregate adds the grid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PqlShape {
    Samples,
}

impl Shape for PqlShape {}

/// What WOULD cross between two PromQL parts: the fingerprint set.
/// Always empty today — no link hands off, so no part is ever seeded.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PqlHandoff(pub Vec<u64>);

// ---------------------------------------------------------------------
// The chain link
// ---------------------------------------------------------------------

/// PromQL's chain link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PqlLink {
    /// The source: one [`pulsus_promql::SelectorSpec`]'s two statements.
    /// The payload is the planner's selector-list position, which is what
    /// makes `plans[i]` traceable to a selector on the explain surface.
    Select(SelectorId),
    Node(NodeKind),
}

/// One link per plan-tree variant that can sit on a chain — 24 of the
/// planner's 29.
///
/// The five that are not here: `Selector` is the chain ROOT and becomes
/// [`PqlLink::Select`]; `Scalar`, `StringLiteral` and `Time` are leaves
/// that bear no selector, ever, so their entry set is empty and no chain
/// can carry them; `Info` always consumes two entries — its own argument
/// and the synthetic metadata selector — so it is on no chain by the
/// rule, not by an exception to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeKind {
    RangeVector,
    RangeFn(RangeFn),
    OverTime(OverTimeFn),
    OverTimeParam(OverTimeParamFn),
    AbsentOverTime,
    Absent,
    Sort,
    SortByLabel,
    LabelReplace,
    LabelJoin,
    HistogramQuantile,
    HistogramQuantiles,
    HistogramAccessor(HistogramAccessorFn),
    HistogramFraction,
    Aggregate(AggOp, Option<Grouping>),
    CountValues,
    Binary(BinOp),
    SetOp(SetOp),
    MathFn(MathFn),
    ScalarFn(ScalarFn),
    DateFn(DateFn),
    Timestamp,
    ScalarOf,
    VectorOf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pql;

#[derive(Debug)]
pub struct SelectLower;
#[derive(Debug)]
pub struct NodeLower;

static SELECT: SelectLower = SelectLower;
static NODE: NodeLower = NodeLower;

impl Lang for Pql {
    type Stage = PqlLink;
    type Source = PqlSource;
    type ColExpr = String;
    type Shape = PqlShape;
    type Handoff = PqlHandoff;
    type Err = ReadError;

    /// The ONE exhaustive match over the chain-link type. **No `_` arm**:
    /// a 25th [`NodeKind`] fails to compile here.
    fn lower_of(stage: &PqlLink) -> &'static dyn Lower<Pql> {
        match stage {
            PqlLink::Select(_) => &SELECT,
            PqlLink::Node(kind) => match kind {
                NodeKind::RangeVector
                | NodeKind::RangeFn(_)
                | NodeKind::OverTime(_)
                | NodeKind::OverTimeParam(_)
                | NodeKind::AbsentOverTime
                | NodeKind::Absent
                | NodeKind::Sort
                | NodeKind::SortByLabel
                | NodeKind::LabelReplace
                | NodeKind::LabelJoin
                | NodeKind::HistogramQuantile
                | NodeKind::HistogramQuantiles
                | NodeKind::HistogramAccessor(_)
                | NodeKind::HistogramFraction
                | NodeKind::Aggregate(_, _)
                | NodeKind::CountValues
                | NodeKind::Binary(_)
                | NodeKind::SetOp(_)
                | NodeKind::MathFn(_)
                | NodeKind::ScalarFn(_)
                | NodeKind::DateFn(_)
                | NodeKind::Timestamp
                | NodeKind::ScalarOf
                | NodeKind::VectorOf => &NODE,
            },
        }
    }

    // `source_of`, `handoff_key` and `handoff_bound` take the trait
    // defaults. No PromQL link reads a source the seed statement did not
    // read, so no cut is possible and `handoff_bound` returning `None` is
    // not reached.

    /// A fingerprint renders as up to 20 decimal digits (`u64::MAX`) plus
    /// the two-byte `", "` separator inside an `IN (…)` list, and
    /// `fingerprint IN ()` is the 17-byte frame. `ast_elements` counts the
    /// identifier, the `IN` function, the list and one literal per value.
    ///
    /// **`text_bytes` over-counts by exactly 2** — the last value carries
    /// no separator — and that is the safe direction: the core compares
    /// it against a ceiling, so over-counting can only make it cut
    /// earlier.
    fn handoff_cost(n: u64) -> HandoffCost {
        HandoffCost {
            text_bytes: 17 + n * 22,
            ast_elements: 3 + n,
        }
    }
}

// ---------------------------------------------------------------------
// The two dispatchers
// ---------------------------------------------------------------------

impl Lower<Pql> for SelectLower {
    /// Always lowers: the predicate is already in the seed, which is what
    /// the shipped statement builders render.
    fn capability(&self, _s: &PqlLink, _rel: &Relation<Pql>) -> Capability {
        Capability::Yes
    }

    /// The identity — the seed carries the whole read.
    fn apply(
        &self,
        _s: &PqlLink,
        rel: Relation<Pql>,
        _cx: &LowerCx<'_, Pql>,
    ) -> Result<Relation<Pql>, ReadError> {
        Ok(rel)
    }

    /// The seed is always applied, so there is no residual case: the
    /// effect is the identity, and this row asserts that rather than
    /// leaving the exemption silent.
    fn residual_effect(&self, _s: &PqlLink, rel: Relation<Pql>) -> Relation<Pql> {
        rel
    }

    /// `Wider`, unconditionally — the fetch window subtracts a lookback
    /// the evaluator re-applies. Claiming `Equivalent` would license the
    /// evaluator to skip re-filtering, which drops rows.
    fn fidelity(&self, _s: &PqlLink, _rel: &Relation<Pql>) -> Fidelity {
        Fidelity::Wider
    }
}

impl Lower<Pql> for NodeLower {
    /// No SQL form has been written for any of the 24 yet, which is what
    /// `NotYetLowered` says. None of them is `Never`: every candidate —
    /// `stddev`/`stdvar`, the binary-operator tree — is unwritten rather
    /// than unwritable, and a `Never` here would put a word on a public
    /// surface that no code can produce.
    fn capability(&self, _s: &PqlLink, _rel: &Relation<Pql>) -> Capability {
        Capability::No(BlockReason::NotYetLowered)
    }

    fn apply(
        &self,
        _s: &PqlLink,
        rel: Relation<Pql>,
        _cx: &LowerCx<'_, Pql>,
    ) -> Result<Relation<Pql>, ReadError> {
        Ok(rel)
    }

    /// **The identity, for every one of the 24.** No link rewrites a
    /// column's provenance, because no label name is in the column set:
    /// `cols` holds `fingerprint`, `unix_milli` and `value`, and a PromQL
    /// group key is a label. Asserted row by row rather than assumed —
    /// see `every_promql_link_states_its_residual_state_effect`.
    fn residual_effect(&self, _s: &PqlLink, rel: Relation<Pql>) -> Relation<Pql> {
        rel
    }
}

// ---------------------------------------------------------------------
// The seed
// ---------------------------------------------------------------------

/// One selector's read, as one predicate: the float read **or** the
/// histogram read, one leaf each, tagged with the table it reads.
///
/// A disjunction because that is what the two statements mean: they are
/// issued together, neither consumes the other, and their rows are merged
/// in our process — the core's own description of `Cut::DisjointSources`.
/// `Pred::disjoint_or_branches` then partitions them into two parts.
///
/// The three fragments come from [`super::sample_sql`]'s own producers,
/// so the leaf text and the statement text cannot disagree.
pub fn selector_pred(name: &str, window: &str, fps: &str) -> Pred {
    let text = format!("{name} AND {window} AND {fps}");
    Pred::leaf(text.clone(), METRIC_SAMPLES).or(Pred::leaf(text, METRIC_HIST_SAMPLES))
}

/// The seed relation a PromQL chain folds from.
pub fn seed_relation(predicate: Pred) -> Relation<Pql> {
    Relation {
        source: SourceTerm::Base(PqlSource(METRIC_SAMPLES)),
        predicate,
        projection: vec![
            (Name::from("fingerprint"), "fingerprint".to_string()),
            (Name::from("unix_milli"), "unix_milli".to_string()),
            (Name::from("value"), "value".to_string()),
        ],
        cols: ColSet::Closed(vec![
            Col {
                name: Name::from("fingerprint"),
                provenance: Provenance::Stored,
            },
            Col {
                name: Name::from("unix_milli"),
                provenance: Provenance::Stored,
            },
            Col {
                name: Name::from("value"),
                provenance: Provenance::Stored,
            },
        ]),
        grouping: None,
        ordering: None,
        limit: None,
        shape: PqlShape::Samples,
        exact: true,
        depth: 0,
        having: Vec::new(),
    }
}

// ---------------------------------------------------------------------
// The chain builder
// ---------------------------------------------------------------------

/// One chain, and the selector-list entry it is rooted at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chain {
    pub selector: SelectorId,
    pub links: Vec<PqlLink>,
}

/// Every chain of a planned query, one per selector-list entry, in the
/// planner's own order.
///
/// **ITERATIVE: one explicit worklist, and no function on this path calls
/// itself** — the rule `crates/pulsus-promql/src/limits.rs:52-70` states
/// for every walk over an accepted tree, whose depth may be
/// [`pulsus_promql::MAX_EXPR_DEPTH`] = 250.
///
/// The walk is three passes over a flattened pre-order of the tree:
/// collect the nodes with their parents and depths; propagate each node's
/// entry set into its parent, in reverse pre-order, so a node is
/// processed after every descendant; then take, for each entry, the nodes
/// whose set is exactly that entry, innermost first.
pub fn chain_of(plan: &pulsus_promql::QueryPlan) -> Vec<Chain> {
    let flat = flatten(&plan.root);
    let n = flat.nodes.len();
    let mut entries: Vec<Vec<SelectorId>> = flat.direct.clone();
    for i in (0..n).rev() {
        if let Some(p) = flat.parent[i] {
            let mine = entries[i].clone();
            for id in mine {
                if !entries[p].contains(&id) {
                    entries[p].push(id);
                }
            }
        }
    }

    (0..plan.selectors.len())
        .map(|id| {
            // The nodes on this entry's chain form a path from the
            // selector upward, so ordering them by depth descending is
            // ordering them innermost first. The index tie-break is
            // unreachable for a path and is there so the order is total.
            let mut on_chain: Vec<(usize, usize)> = (0..n)
                .filter(|&i| entries[i].len() == 1 && entries[i][0] == id)
                .map(|i| (flat.depth[i], i))
                .collect();
            on_chain.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
            let mut links = Vec::with_capacity(on_chain.len() + 1);
            links.push(PqlLink::Select(id));
            links.extend(
                on_chain
                    .into_iter()
                    .filter_map(|(_, i)| node_kind(flat.nodes[i]).map(PqlLink::Node)),
            );
            Chain {
                selector: id,
                links,
            }
        })
        .collect()
}

/// The plan tree flattened into pre-order, with each node's parent, its
/// depth, and the entries it consumes DIRECTLY (before any propagation).
struct Flat<'a> {
    nodes: Vec<&'a PlanExpr>,
    parent: Vec<Option<usize>>,
    depth: Vec<usize>,
    direct: Vec<Vec<SelectorId>>,
}

fn flatten(root: &PlanExpr) -> Flat<'_> {
    let mut flat = Flat {
        nodes: Vec::new(),
        parent: Vec::new(),
        depth: Vec::new(),
        direct: Vec::new(),
    };
    // (node, parent index, depth). A stack, not recursion.
    let mut stack: Vec<(&PlanExpr, Option<usize>, usize)> = vec![(root, None, 0)];
    let mut kids: Vec<&PlanExpr> = Vec::new();
    while let Some((node, parent, depth)) = stack.pop() {
        let i = flat.nodes.len();
        flat.nodes.push(node);
        flat.parent.push(parent);
        flat.depth.push(depth);
        flat.direct.push(direct_entries(node));
        kids.clear();
        children(node, &mut kids);
        // Pushed in reverse so the first child is popped first and the
        // flattened order is a genuine pre-order.
        for child in kids.iter().rev() {
            stack.push((child, Some(i), depth + 1));
        }
    }
    flat
}

/// The entries a node consumes **without** looking at its children.
///
/// `Absent::selector` and `Timestamp::bare_selector` are deliberately not
/// read here: each names a selector that is already the node's own
/// argument, so counting it would be a second spelling of one occurrence.
/// `Info::info_selector` IS read, because the synthetic metadata selector
/// is a selector-list entry that appears nowhere in the tree — which is
/// why `info(m)` consumes two entries and is on no chain.
fn direct_entries(e: &PlanExpr) -> Vec<SelectorId> {
    match e {
        PlanExpr::Selector(id) => vec![*id],
        PlanExpr::RangeVector { source }
        | PlanExpr::RangeFn { source, .. }
        | PlanExpr::OverTime { source, .. }
        | PlanExpr::OverTimeParam { source, .. }
        | PlanExpr::AbsentOverTime { source } => match source {
            RangeSource::Selector(id) => vec![*id],
            RangeSource::Subquery(_) => Vec::new(),
        },
        PlanExpr::Info { info_selector, .. } => vec![*info_selector],
        PlanExpr::Absent { .. }
        | PlanExpr::Sort { .. }
        | PlanExpr::SortByLabel { .. }
        | PlanExpr::LabelReplace { .. }
        | PlanExpr::LabelJoin { .. }
        | PlanExpr::HistogramQuantile { .. }
        | PlanExpr::HistogramQuantiles { .. }
        | PlanExpr::HistogramAccessor { .. }
        | PlanExpr::HistogramFraction { .. }
        | PlanExpr::Aggregate { .. }
        | PlanExpr::CountValues { .. }
        | PlanExpr::Binary { .. }
        | PlanExpr::SetOp { .. }
        | PlanExpr::MathFn { .. }
        | PlanExpr::ScalarFn { .. }
        | PlanExpr::Time
        | PlanExpr::DateFn { .. }
        | PlanExpr::Timestamp { .. }
        | PlanExpr::ScalarOf { .. }
        | PlanExpr::VectorOf { .. }
        | PlanExpr::Scalar(_)
        | PlanExpr::StringLiteral(_) => Vec::new(),
    }
}

/// A node's children, in written order. **No `_` arm**: a 30th
/// [`PlanExpr`] variant fails to compile here rather than silently
/// losing whatever it carries.
///
/// A sub-query's inner expression is reachable only through
/// [`RangeSource::Subquery`], so the four variants that hold a
/// `RangeSource` reach it through [`range_source_child`]. Skipping that
/// one arm stops the entry set propagating and drops every link above a
/// sub-query source off its chain.
fn children<'a>(e: &'a PlanExpr, out: &mut Vec<&'a PlanExpr>) {
    match e {
        PlanExpr::Selector(_)
        | PlanExpr::Time
        | PlanExpr::Scalar(_)
        | PlanExpr::StringLiteral(_) => {}
        PlanExpr::RangeVector { source } | PlanExpr::AbsentOverTime { source } => {
            range_source_child(source, out);
        }
        PlanExpr::RangeFn { source, .. } | PlanExpr::OverTime { source, .. } => {
            range_source_child(source, out);
        }
        PlanExpr::OverTimeParam { source, args, .. } => {
            range_source_child(source, out);
            out.extend(args.iter().map(Box::as_ref));
        }
        PlanExpr::Absent { arg, .. }
        | PlanExpr::Sort { arg, .. }
        | PlanExpr::SortByLabel { arg, .. }
        | PlanExpr::LabelReplace { arg, .. }
        | PlanExpr::LabelJoin { arg, .. }
        | PlanExpr::HistogramAccessor { arg, .. }
        | PlanExpr::Timestamp { arg, .. }
        | PlanExpr::ScalarOf { arg }
        | PlanExpr::VectorOf { arg } => out.push(arg),
        PlanExpr::HistogramQuantile { quantile, expr, .. } => {
            out.push(quantile);
            out.push(expr);
        }
        PlanExpr::HistogramQuantiles {
            expr, quantiles, ..
        } => {
            out.push(expr);
            out.extend(quantiles.iter().map(Box::as_ref));
        }
        PlanExpr::HistogramFraction {
            lower, upper, expr, ..
        } => {
            out.push(lower);
            out.push(upper);
            out.push(expr);
        }
        PlanExpr::Aggregate { expr, param, .. } => {
            out.push(expr);
            out.extend(param.iter().map(Box::as_ref));
        }
        PlanExpr::CountValues { expr, .. } => out.push(expr),
        PlanExpr::Binary { lhs, rhs, .. } | PlanExpr::SetOp { lhs, rhs, .. } => {
            out.push(lhs);
            out.push(rhs);
        }
        PlanExpr::MathFn {
            arg, scalar_args, ..
        } => {
            out.push(arg);
            out.extend(scalar_args.iter().map(Box::as_ref));
        }
        PlanExpr::ScalarFn { args, .. } => out.extend(args.iter().map(Box::as_ref)),
        PlanExpr::DateFn { arg, .. } => out.extend(arg.iter().map(Box::as_ref)),
        PlanExpr::Info { base, .. } => out.push(base),
    }
}

fn range_source_child<'a>(source: &'a RangeSource, out: &mut Vec<&'a PlanExpr>) {
    match source {
        RangeSource::Selector(_) => {}
        RangeSource::Subquery(sq) => out.push(&sq.inner),
    }
}

/// The link a plan-tree node becomes, or `None` for the five variants
/// that are on no chain. **No `_` arm.**
fn node_kind(e: &PlanExpr) -> Option<NodeKind> {
    Some(match e {
        PlanExpr::RangeVector { .. } => NodeKind::RangeVector,
        PlanExpr::RangeFn { func, .. } => NodeKind::RangeFn(*func),
        PlanExpr::OverTime { func, .. } => NodeKind::OverTime(*func),
        PlanExpr::OverTimeParam { func, .. } => NodeKind::OverTimeParam(*func),
        PlanExpr::AbsentOverTime { .. } => NodeKind::AbsentOverTime,
        PlanExpr::Absent { .. } => NodeKind::Absent,
        PlanExpr::Sort { .. } => NodeKind::Sort,
        PlanExpr::SortByLabel { .. } => NodeKind::SortByLabel,
        PlanExpr::LabelReplace { .. } => NodeKind::LabelReplace,
        PlanExpr::LabelJoin { .. } => NodeKind::LabelJoin,
        PlanExpr::HistogramQuantile { .. } => NodeKind::HistogramQuantile,
        PlanExpr::HistogramQuantiles { .. } => NodeKind::HistogramQuantiles,
        PlanExpr::HistogramAccessor { func, .. } => NodeKind::HistogramAccessor(*func),
        PlanExpr::HistogramFraction { .. } => NodeKind::HistogramFraction,
        PlanExpr::Aggregate { op, grouping, .. } => NodeKind::Aggregate(*op, grouping.clone()),
        PlanExpr::CountValues { .. } => NodeKind::CountValues,
        PlanExpr::Binary { op, .. } => NodeKind::Binary(*op),
        PlanExpr::SetOp { op, .. } => NodeKind::SetOp(*op),
        PlanExpr::MathFn { func, .. } => NodeKind::MathFn(*func),
        PlanExpr::ScalarFn { func, .. } => NodeKind::ScalarFn(*func),
        PlanExpr::DateFn { func, .. } => NodeKind::DateFn(*func),
        PlanExpr::Timestamp { .. } => NodeKind::Timestamp,
        PlanExpr::ScalarOf { .. } => NodeKind::ScalarOf,
        PlanExpr::VectorOf { .. } => NodeKind::VectorOf,
        // The five that are on no chain. `Selector` is the chain ROOT and
        // is emitted as `PqlLink::Select` by `chain_of`; the other four
        // have entry sets that are empty or of size two.
        PlanExpr::Selector(_)
        | PlanExpr::Time
        | PlanExpr::Scalar(_)
        | PlanExpr::StringLiteral(_)
        | PlanExpr::Info { .. } => return None,
    })
}

// ---------------------------------------------------------------------
// Names
// ---------------------------------------------------------------------

/// The language's own spelling of each chain link, in chain order — the
/// core has no way to name a `L::Stage`, so it takes these as a
/// parameter to `QueryPlan::shape`.
pub fn stage_names(chain: &Chain) -> Vec<String> {
    chain.links.iter().map(stage_name).collect()
}

/// The stage KIND and its operator, never a user literal — except the
/// selector INDEX, which is not a literal and is what ties a plan to its
/// selector.
fn stage_name(link: &PqlLink) -> String {
    match link {
        PqlLink::Select(i) => format!("Select({i})"),
        PqlLink::Node(kind) => match kind {
            NodeKind::RangeVector => "RangeVector".to_string(),
            NodeKind::RangeFn(f) => format!("RangeFn({})", range_fn_name(*f)),
            NodeKind::OverTime(f) => format!("OverTime({})", over_time_name(*f)),
            NodeKind::OverTimeParam(f) => format!("OverTimeParam({})", over_time_param_name(*f)),
            NodeKind::AbsentOverTime => "AbsentOverTime".to_string(),
            NodeKind::Absent => "Absent".to_string(),
            NodeKind::Sort => "Sort".to_string(),
            NodeKind::SortByLabel => "SortByLabel".to_string(),
            NodeKind::LabelReplace => "LabelReplace".to_string(),
            NodeKind::LabelJoin => "LabelJoin".to_string(),
            NodeKind::HistogramQuantile => "HistogramQuantile".to_string(),
            NodeKind::HistogramQuantiles => "HistogramQuantiles".to_string(),
            NodeKind::HistogramAccessor(f) => {
                format!("HistogramAccessor({})", histogram_accessor_name(*f))
            }
            NodeKind::HistogramFraction => "HistogramFraction".to_string(),
            NodeKind::Aggregate(op, _) => format!("Aggregate({})", agg_op_name(*op)),
            NodeKind::CountValues => "CountValues".to_string(),
            NodeKind::Binary(op) => format!("Binary({})", bin_op_name(*op)),
            NodeKind::SetOp(op) => format!("SetOp({})", set_op_name(*op)),
            NodeKind::MathFn(f) => format!("MathFn({})", math_fn_name(*f)),
            NodeKind::ScalarFn(f) => format!("ScalarFn({})", scalar_fn_name(*f)),
            NodeKind::DateFn(f) => format!("DateFn({})", date_fn_name(*f)),
            NodeKind::Timestamp => "Timestamp".to_string(),
            NodeKind::ScalarOf => "ScalarOf".to_string(),
            NodeKind::VectorOf => "VectorOf".to_string(),
        },
    }
}

fn range_fn_name(f: RangeFn) -> &'static str {
    match f {
        RangeFn::Rate => "rate",
        RangeFn::Irate => "irate",
        RangeFn::Increase => "increase",
        RangeFn::Delta => "delta",
    }
}

fn over_time_name(f: OverTimeFn) -> &'static str {
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

fn over_time_param_name(f: OverTimeParamFn) -> &'static str {
    match f {
        OverTimeParamFn::Quantile => "quantile",
        OverTimeParamFn::PredictLinear => "predict_linear",
        OverTimeParamFn::DoubleExpSmoothing => "double_exponential_smoothing",
    }
}

fn histogram_accessor_name(f: HistogramAccessorFn) -> &'static str {
    match f {
        HistogramAccessorFn::Count => "count",
        HistogramAccessorFn::Sum => "sum",
        HistogramAccessorFn::Avg => "avg",
        HistogramAccessorFn::StdDev => "stddev",
        HistogramAccessorFn::StdVar => "stdvar",
    }
}

fn agg_op_name(op: AggOp) -> &'static str {
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

fn bin_op_name(op: BinOp) -> &'static str {
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

fn set_op_name(op: SetOp) -> &'static str {
    match op {
        SetOp::And => "and",
        SetOp::Or => "or",
        SetOp::Unless => "unless",
    }
}

fn math_fn_name(f: MathFn) -> &'static str {
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

fn scalar_fn_name(f: ScalarFn) -> &'static str {
    match f {
        ScalarFn::Pi => "pi",
        ScalarFn::MaxOf => "max_of",
        ScalarFn::MinOf => "min_of",
    }
}

fn date_fn_name(f: DateFn) -> &'static str {
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

// ---------------------------------------------------------------------
// The entry point `exec` calls
// ---------------------------------------------------------------------

/// What `exec` records per selector while it builds the statements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectorRead {
    pub selector: SelectorId,
    pub pred: Pred,
}

/// The request's bounds, in the core's nanoseconds.
///
/// **`saturating_mul`, not `*`.** `parse_time` clamps to `i64::MAX`
/// milliseconds rather than rejecting
/// (`crates/pulsus-server/src/prom_api/params.rs:127`), so `start_ms ==
/// i64::MAX` is reachable from a request; a plain multiply panics in a
/// debug build — every test binary is one — and wraps in release. The
/// first millisecond value that does not fit is `9_223_372_036_855`.
///
/// Saturating is safe here **only because nothing reads these
/// nanoseconds**: no link's `capability` consults `bounds`, and `limit`
/// is always `None` because PromQL has no limit parameter, so
/// `inexact_limit_fires` is `false` for every PromQL plan by
/// construction. A window lowered into SQL reads
/// `SelectorSpec::fetch_window`'s milliseconds.
pub fn request_bounds(params: &MetricQueryParams) -> RequestBounds {
    RequestBounds {
        start_ns: params.start_ms.saturating_mul(1_000_000),
        end_ns: params.end_ms.saturating_mul(1_000_000),
        step_ns: Some(params.step_ms.saturating_mul(1_000_000)),
        limit: None,
    }
}

/// Every plan shape the explain surface carries: one per selector-list
/// entry **that reads**.
///
/// A selector that sends no statement — `up{__name__="down"}`, whose name
/// matchers exclude the concrete name — contributes no entry, because
/// `plan_of` always emits a source part and a plan with a source part
/// would say a statement was sent. So `plans.len()` can be less than
/// `selectors.len()`, and the mapping back to a selector is link 0's
/// `Select(i)` stage name.
pub fn plan_shapes(
    plan: &pulsus_promql::QueryPlan,
    reads: &[SelectorRead],
    params: &MetricQueryParams,
) -> Result<Vec<PlanShape>, ReadError> {
    let bounds = request_bounds(params);
    let config = PlanConfig::default();
    let cx = PlanCx {
        bounds: &bounds,
        config: &config,
    };
    let lower_cx = LowerCx::<Pql>::new(&bounds);
    let mut out = Vec::new();
    for chain in chain_of(plan) {
        let Some(read) = reads.iter().find(|r| r.selector == chain.selector) else {
            continue;
        };
        let names = stage_names(&chain);
        let lowering =
            lower_chain::<Pql>(&chain.links, seed_relation(read.pred.clone()), &lower_cx)?;
        let query_plan = core_plan_of::<Pql>(&chain.links, lowering, &cx)?;
        out.push(query_plan.shape(&names));
    }
    Ok(out)
}

// ---------------------------------------------------------------------
// Gates
// ---------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use crate::compile::plan::{Cut, Part};
    use crate::compile::testkit::{EffectRow, assert_every_residual_state_effect};
    use pulsus_promql::{DEFAULT_LOOKBACK_MS, PlanParams, parse};

    fn params() -> MetricQueryParams {
        MetricQueryParams {
            start_ms: 1_782_907_200_000,
            end_ms: 1_782_928_800_000,
            step_ms: 60_000,
        }
    }

    fn plan_params() -> PlanParams {
        PlanParams {
            start_ms: params().start_ms,
            end_ms: params().end_ms,
            step_ms: params().step_ms,
            lookback_ms: DEFAULT_LOOKBACK_MS,
            experimental_functions: true,
        }
    }

    fn planned(q: &str) -> pulsus_promql::QueryPlan {
        pulsus_promql::plan(&parse(q).expect("parse"), plan_params()).expect("plan")
    }

    fn pred(tag: &str) -> Pred {
        selector_pred(
            &format!("metric_name = '{tag}'"),
            "unix_milli > 1 AND unix_milli <= 2",
            "fingerprint IN (7)",
        )
    }

    /// Every one of the 24 `NodeKind`s, each carrying a payload where it
    /// has one — the rows of the residual-state-effect gate.
    fn every_node_kind() -> Vec<NodeKind> {
        vec![
            NodeKind::RangeVector,
            NodeKind::RangeFn(RangeFn::Rate),
            NodeKind::OverTime(OverTimeFn::Max),
            NodeKind::OverTimeParam(OverTimeParamFn::Quantile),
            NodeKind::AbsentOverTime,
            NodeKind::Absent,
            NodeKind::Sort,
            NodeKind::SortByLabel,
            NodeKind::LabelReplace,
            NodeKind::LabelJoin,
            NodeKind::HistogramQuantile,
            NodeKind::HistogramQuantiles,
            NodeKind::HistogramAccessor(HistogramAccessorFn::Count),
            NodeKind::HistogramFraction,
            NodeKind::Aggregate(AggOp::Max, None),
            NodeKind::CountValues,
            NodeKind::Binary(BinOp::Mul),
            NodeKind::SetOp(SetOp::And),
            NodeKind::MathFn(MathFn::Abs),
            NodeKind::ScalarFn(ScalarFn::Pi),
            NodeKind::DateFn(DateFn::Year),
            NodeKind::Timestamp,
            NodeKind::ScalarOf,
            NodeKind::VectorOf,
        ]
    }

    /// Criterion 4: **every link's residual state effect is asserted, not
    /// assumed.**
    ///
    /// 25 rows — one per `NodeKind` plus one for `Select` — every one
    /// `has_effect: false`, with two differing seeds each, so a row
    /// cannot be satisfied by an implementation that assigns a field
    /// where the row says it preserves one.
    #[test]
    fn every_promql_link_states_its_residual_state_effect() {
        let mut s1 = seed_relation(pred("a"));
        let mut s2 = seed_relation(pred("b"));
        // The two seeds differ in more than the predicate, so a row that
        // claims the identity is checked against two genuinely different
        // relations.
        s2.exact = false;
        s2.cols
            .set_provenance(&Name::from("value"), Provenance::EvaluatorOnly);
        s1.shape = PqlShape::Samples;
        let mut rows: Vec<EffectRow<Pql>> = vec![EffectRow {
            name: "Select",
            link: PqlLink::Select(0),
            s1: s1.clone(),
            s2: s2.clone(),
            e1: s1.clone(),
            e2: s2.clone(),
            effect_is_constant: false,
            has_effect: false,
        }];
        rows.extend(every_node_kind().into_iter().map(|kind| EffectRow {
            name: "Node",
            link: PqlLink::Node(kind),
            s1: s1.clone(),
            s2: s2.clone(),
            e1: s1.clone(),
            e2: s2.clone(),
            effect_is_constant: false,
            has_effect: false,
        }));
        assert_eq!(rows.len(), 25, "one row per link kind, plus Select");
        assert_every_residual_state_effect::<Pql>(&rows, 25);
    }

    /// Criterion 6: the request bounds **saturate rather than overflow**.
    ///
    /// `9_223_372_036_854` ms is the last value that converts exactly;
    /// `9_223_372_036_855` is the first that does not, and the two differ
    /// by one millisecond — the narrowest place the two readings part.
    #[test]
    fn the_request_bounds_saturate_rather_than_overflow() {
        let cases: [i64; 5] = [
            9_223_372_036_854,
            9_223_372_036_855,
            i64::MAX,
            i64::MIN,
            -9_223_372_036_855,
        ];
        for ms in cases {
            let b = request_bounds(&MetricQueryParams {
                start_ms: ms,
                end_ms: ms,
                step_ms: ms,
            });
            let expected = ms.saturating_mul(1_000_000);
            assert_eq!(b.start_ns, expected, "start_ns at {ms} ms");
            assert_eq!(b.end_ns, expected, "end_ns at {ms} ms");
            assert_eq!(b.step_ns, Some(expected), "step_ns at {ms} ms");
        }
        // The two readings, side by side at the boundary.
        assert_eq!(
            request_bounds(&MetricQueryParams {
                start_ms: 9_223_372_036_854,
                end_ms: 0,
                step_ms: 0
            })
            .start_ns,
            9_223_372_036_854_000_000,
            "the last millisecond value that converts exactly"
        );
        assert_eq!(
            request_bounds(&MetricQueryParams {
                start_ms: 9_223_372_036_855,
                end_ms: 0,
                step_ms: 0
            })
            .start_ns,
            i64::MAX,
            "the first millisecond value that does not fit"
        );
        // And the invariant the saturation rests on: PromQL has no limit
        // parameter, so no plan here can take the inexact-limit cut.
        assert_eq!(
            request_bounds(&params()).limit,
            None,
            "PromQL has no limit parameter"
        );
    }

    /// Criterion 7: `handoff_cost` **bounds the rendered list and never
    /// under-counts**, at the boundary rather than at a dramatic value.
    #[test]
    fn handoff_cost_bounds_the_rendered_fingerprint_list() {
        for n in [0usize, 1, 2, 500] {
            let fps = vec![u64::MAX; n];
            let rendered = super::super::sample_sql::render_fingerprint_list(&fps);
            let frame = "fingerprint IN ()".len();
            assert_eq!(frame, 17, "the frame this constant counts");
            let cost = Pql::handoff_cost(n as u64);
            let actual = (rendered.len() + frame) as u64;
            assert!(
                cost.text_bytes >= actual,
                "n={n}: {} must not under-count {actual}",
                cost.text_bytes
            );
            assert!(
                cost.text_bytes - actual <= 2,
                "n={n}: over-count {} exceeds the one missing separator",
                cost.text_bytes - actual
            );
            assert_eq!(cost.ast_elements, 3 + n as u64, "n={n}: ast elements");
        }
    }

    /// The worked query of the plan: one chain, two SQL parts and one
    /// engine part, with the aggregate residual.
    #[test]
    fn the_worked_query_yields_two_sql_parts_and_one_engine_part() {
        let plan = planned("max by (status) (http_requests_total{status=\"500\"})");
        let reads = vec![SelectorRead {
            selector: 0,
            pred: pred("http_requests_total"),
        }];
        let shapes = plan_shapes(&plan, &reads, &params()).expect("plan shapes");
        assert_eq!(shapes.len(), 1, "one plan per selector that reads");
        let json = serde_json::to_value(&shapes[0]).expect("serialize");
        assert_eq!(
            json["parts"][0]["name"], "metric_samples",
            "the float read opens the plan"
        );
        assert_eq!(json["parts"][0]["cut"], serde_json::Value::Null);
        assert_eq!(json["parts"][1]["name"], "metric_hist_samples");
        assert_eq!(json["parts"][1]["cut"]["why"], "disjoint_sources");
        assert_eq!(json["parts"][2]["kind"], "engine");
        assert_eq!(json["parts"][2]["links"], serde_json::json!([1]));
        assert_eq!(json["links"][0]["stage"], "Select(0)");
        assert_eq!(json["links"][0]["how"], "lowered");
        assert_eq!(json["links"][0]["fidelity"], "wider");
        assert_eq!(json["links"][1]["stage"], "Aggregate(max)");
        assert_eq!(json["links"][1]["how"], "residual");
        assert_eq!(json["links"][1]["why"], "not_yet_lowered");
        assert_eq!(json["parts"][0]["yields"], "candidates");
    }

    /// The dual read is two parts because the seed is a disjunction over
    /// two sources — stated against the core's own cut rather than
    /// against the rendered JSON.
    #[test]
    fn a_selectors_two_statements_are_two_sql_parts_cut_on_disjoint_sources() {
        let plan = planned("http_requests_total");
        let reads = vec![SelectorRead {
            selector: 0,
            pred: pred("http_requests_total"),
        }];
        let bounds = request_bounds(&params());
        let config = PlanConfig::default();
        let cx = PlanCx {
            bounds: &bounds,
            config: &config,
        };
        let chain = chain_of(&plan).remove(0);
        let lowering = lower_chain::<Pql>(
            &chain.links,
            seed_relation(reads[0].pred.clone()),
            &LowerCx::<Pql>::new(&bounds),
        )
        .expect("fold");
        let qp = core_plan_of::<Pql>(&chain.links, lowering, &cx).expect("plan");
        let sql: Vec<&crate::compile::plan::SqlPart<Pql>> = qp
            .parts
            .iter()
            .filter_map(|p| match p {
                Part::Sql(s) => Some(s.as_ref()),
                Part::Engine { .. } => None,
            })
            .collect();
        assert_eq!(sql.len(), 2, "a selector's read is two statements");
        assert_eq!(sql[0].rel.source_ref(), METRIC_SAMPLES);
        assert_eq!(sql[1].rel.source_ref(), METRIC_HIST_SAMPLES);
        assert!(sql[0].cut.is_none(), "the first part opens the plan");
        assert!(
            matches!(&sql[1].cut, Some(Cut::DisjointSources { sources })
                if sources == &[METRIC_SAMPLES, METRIC_HIST_SAMPLES]),
            "the second part's cut: {:?}",
            sql[1].cut
        );
    }

    /// A selector that sends no statement contributes no plan, and the
    /// remaining plans still name their own selector.
    #[test]
    fn a_selector_that_sends_no_statement_contributes_no_plan() {
        let plan = planned("http_requests_total and http_errors_total");
        assert_eq!(plan.selectors.len(), 2);
        let reads = vec![SelectorRead {
            selector: 1,
            pred: pred("http_errors_total"),
        }];
        let shapes = plan_shapes(&plan, &reads, &params()).expect("plan shapes");
        assert_eq!(shapes.len(), 1, "one selector read, so one plan");
        assert_eq!(shapes[0].links[0].stage, "Select(1)");
        let both = vec![
            SelectorRead {
                selector: 0,
                pred: pred("http_requests_total"),
            },
            SelectorRead {
                selector: 1,
                pred: pred("http_errors_total"),
            },
        ];
        let shapes = plan_shapes(&plan, &both, &params()).expect("plan shapes");
        assert_eq!(shapes.len(), 2);
        assert_eq!(shapes[0].links[0].stage, "Select(0)");
        assert_eq!(shapes[1].links[0].stage, "Select(1)");
    }

    /// A 250-link chain is built without aborting, and the depth cap
    /// rejects 251 before planning, so the builder never sees deeper.
    #[test]
    fn a_chain_at_the_depth_cap_is_built_and_one_deeper_is_refused() {
        let deep = format!("m{}", " * 1".repeat(249));
        let plan = planned(&deep);
        let chains = chain_of(&plan);
        assert_eq!(chains.len(), 1);
        assert_eq!(
            chains[0].links.len(),
            250,
            "one Select plus 249 Binary links"
        );
        let deeper = format!("m{}", " * 1".repeat(250));
        let err = parse(&deeper).expect_err("251 levels is refused before planning");
        assert!(
            format!("{err}").contains("nesting depth"),
            "the parser's own refusal: {err}"
        );
    }
}
