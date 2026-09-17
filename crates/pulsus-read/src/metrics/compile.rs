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
/// The rows a selector's statements return. Two shapes today, and the
/// shape is what the aggregate link's capability reads: the decision to
/// push rides in the DATA rather than in a flag the dispatcher consults,
/// so the explain surface changes exactly when the statement changes and
/// a query that is eligible but declines has an unchanged plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PqlShape {
    /// One row per stored sample — `fingerprint, unix_milli, value`.
    Samples,
    /// One row per RUN: a stretch of consecutive grid indices over which
    /// one group's answer does not change (issue #549). Set by
    /// [`seed_relation`] only for a selector whose grouped push was
    /// taken.
    GroupedRuns,
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

    /// The grouped instant read (issue #549) unions `metric_samples` and
    /// `metric_hist_samples` inside ONE statement, so the part named for
    /// the first also reads the second. Every other PromQL statement
    /// reads exactly one table and this returns nothing for it, which is
    /// what keeps the wire field absent from every plan but that one.
    fn also_reads(rel: &Relation<Pql>) -> Vec<SourceRef> {
        match rel.shape {
            PqlShape::GroupedRuns => vec![METRIC_HIST_SAMPLES],
            PqlShape::Samples => Vec::new(),
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
    /// One link lowers, and only over rows the grouped statement
    /// produced: `min`/`max`/`count`/`group`, when the relation's shape
    /// is [`PqlShape::GroupedRuns`] (issue #549). Every other link — and
    /// those four over ordinary sample rows — is `NotYetLowered`.
    ///
    /// **The shape is the condition, not the operator.** Reading only the
    /// operator would claim a lowering for `max by (status) (m)` on every
    /// route, including the one where the push declined on the threshold
    /// and the engine still does the reduction.
    ///
    /// None of the rest is `Never`: every candidate — `stddev`/`stdvar`,
    /// the binary-operator tree — is unwritten rather than unwritable,
    /// and a `Never` here would put a word on a public surface that no
    /// code can produce.
    fn capability(&self, s: &PqlLink, rel: &Relation<Pql>) -> Capability {
        if rel.shape == PqlShape::GroupedRuns
            && let PqlLink::Node(NodeKind::Aggregate(
                AggOp::Min | AggOp::Max | AggOp::Count | AggOp::Group,
                _,
            )) = s
        {
            return Capability::Yes;
        }
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

/// One pushed selector's read, as one predicate: the grouped statement
/// reads `metric_samples` and `metric_hist_samples` in ONE statement, so
/// it is one leaf naming the table the part is named for, with the other
/// carried additively by [`Pql::also_reads`] rather than by a second
/// branch (a disjunction here would split it into two parts, which is a
/// plan describing two statements where the engine sends one).
pub fn grouped_selector_pred(name: &str, window: &str, fps: &str) -> Pred {
    Pred::leaf(format!("{name} AND {window} AND {fps}"), METRIC_SAMPLES)
}

/// The seed relation a PromQL chain folds from.
pub fn seed_relation(predicate: Pred, shape: PqlShape) -> Relation<Pql> {
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
        shape,
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

/// How many selector-list entries' rows reach a node.
///
/// **Three cases and not a set**, because the rule asks exactly one
/// question — *is `i` the only entry whose rows this node consumes?* —
/// and that question is answered by `One(i)` alone. Carrying the whole
/// set instead made the union O(entries) per edge and made the chain
/// builder scan every node once per entry: a 4,096-selector query, which
/// the accept surface admits at 43,940 bytes, did 33,550,336 node-filter
/// checks (code review round 2). The merge below is O(1) and the
/// information dropped — WHICH several entries reach a `Many` node — is
/// information no rule here reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Owner {
    /// No entry's rows reach this node: a scalar leaf, `time()`.
    None,
    /// Exactly one, so the node is on that entry's chain.
    One(SelectorId),
    /// Two or more, so the node is on no chain — and, because the merge
    /// is monotonic, neither is any ancestor of it.
    Many,
}

impl Owner {
    /// The union, as the rule reads it. `None` is the identity; two
    /// different `One`s are `Many`; `Many` absorbs.
    fn merge(self, other: Owner) -> Owner {
        match (self, other) {
            (Owner::None, o) | (o, Owner::None) => o,
            (Owner::One(a), Owner::One(b)) if a == b => Owner::One(a),
            _ => Owner::Many,
        }
    }
}

/// Every chain of a planned query, one per selector-list entry, in the
/// planner's own order.
///
/// **ITERATIVE: one explicit worklist, and no function on this path calls
/// itself** — the rule `crates/pulsus-promql/src/limits.rs:52-70` states
/// for every walk over an accepted tree, whose depth may be
/// [`pulsus_promql::MAX_EXPR_DEPTH`] = 250.
///
/// **Linear in the tree, not quadratic in the selector list.** Four
/// passes, each over the nodes or over the entries and never over both:
///
/// ```text
///   flatten          the nodes and their parents, in pre-order   O(nodes)
///   own              each node's Owner, folded into its parent   O(nodes)
///   bucket           each One node filed under its entry         O(nodes)
///   build            one chain per entry, from its bucket only   O(entries + nodes)
/// ```
///
/// The third pass is what removes the per-entry scan: **every node is
/// placed at most once**, so the buckets hold no more than `nodes`
/// entries between them however many selectors the query has. **One
/// bucket holds at most [`pulsus_promql::MAX_EXPR_DEPTH`] entries**,
/// because a chain is a path from a selector to the root and the parser
/// caps the depth — so even a scan of one bucket against itself is
/// bounded by a constant rather than by the tree.
///
/// Every collection here is [`Counted`], construction included, so a pass
/// anyone adds **over these collections** is charged for what it reads or
/// writes whether or not they know the gate exists. A pass over a copy
/// taken out of one of them is not — see the bound published on
/// `the_explain_walk_is_linear_in_the_tree`, which is where the limits of
/// the counting are stated.
pub fn chain_of(plan: &pulsus_promql::QueryPlan) -> Vec<Chain> {
    let flat = flatten(&plan.root);
    let n = flat.nodes.len();

    // Pass 2: each node's owner, folded upward. Reverse pre-order, so a
    // node is merged into its parent after every one of its descendants
    // has been merged into it.
    let mut owner = flat.direct;
    for i in (0..n).rev() {
        let (Some(mine), Some(Some(p))) = (owner.get(i).copied(), flat.parent.get(i).copied())
        else {
            continue;
        };
        let merged = owner.get(p).copied().unwrap_or(Owner::None).merge(mine);
        owner.set(p, merged);
    }

    // Pass 3: one bucket per entry, filled in ONE pass over the nodes.
    // Pre-order visits an ancestor before its descendants, so each
    // bucket comes out ordered by increasing depth.
    let mut buckets: Counted<Vec<Vec<usize>>> =
        Counted::new((0..plan.selectors.len()).map(|_| Vec::new()).collect());
    for (i, own) in owner.iter().enumerate() {
        if let Owner::One(id) = own {
            buckets.push_into(*id, i);
        }
    }

    // Pass 4: one chain per entry, reading only that entry's bucket. The
    // nodes on a chain form a path from the selector upward, so reversing
    // the bucket's increasing depth is ordering them innermost first.
    let mut out = Vec::with_capacity(plan.selectors.len());
    for (id, bucket) in buckets.into_counted_inners().enumerate() {
        let mut links = Vec::with_capacity(bucket.len() + 1);
        links.push(PqlLink::Select(id));
        links.extend(
            bucket
                .into_iter_counted()
                .rev()
                .filter_map(|i| flat.nodes.get(i).and_then(|node| node_kind(node)))
                .map(PqlLink::Node),
        );
        out.push(Chain {
            selector: id,
            links,
        });
    }
    out
}

// ---------------------------------------------------------------------
// The instrument
// ---------------------------------------------------------------------

/// Collections whose every element access THROUGH THEM is charged.
///
/// **This is the whole instrument, and its point is that nobody has to
/// know it is here.** Two earlier versions did not have that property
/// and were beaten twice: the first counted at call sites a person had
/// to remember to write, so a restored scan sat green (code review round
/// 3); the second charged only the FINISHED collections, so quadratic
/// work added inside the construction pass — a scan of everything pushed
/// so far, right after each push — moved no count at all (round 4).
///
/// So the rule is now stated as a property of the walk rather than of
/// any one collection:
///
/// > **Every collection the walk reads back is a [`Counted`], and every
/// > collection that is not is write-only.** The first half is what
/// > charges a scan OVER THOSE COLLECTIONS; the second is what makes
/// > "not counted" mean "cannot hold a scan". Both halves are checked by
/// > `every_collection_the_walk_reads_back_is_counted`, which parses
/// > this file.
///
/// The collection inside [`Counted`] is private to this module, so from
/// outside there is **no way to reach an element at all** except through
/// an accessor below, and every accessor charges — including the ones
/// that BUILD it, because a construction pass is part of the walk.
mod counted {
    use std::ops::{Deref, DerefMut};

    /// A collection whose elements can only be reached, or added, by
    /// charging for them. `C` is `Vec<T>` throughout: the walk builds
    /// its own collections and is HANDED one built the same way, so
    /// there is no borrowed-slice form and nothing that could hand a
    /// plain slice back.
    pub(crate) struct Counted<C>(C);

    impl<C> Counted<C> {
        pub(crate) fn new(inner: C) -> Self {
            Self(inner)
        }
    }

    impl<T> Counted<Vec<T>> {
        pub(crate) fn empty() -> Self {
            Self(Vec::new())
        }

        /// Appends one element. Charged: **a push is part of the work**,
        /// which is what the round-4 probe exploited when only finished
        /// collections were counted.
        #[inline]
        pub(crate) fn push(&mut self, v: T) {
            super::charge(1);
            self.0.push(v);
        }

        /// Appends every element of `it`, charged one by one.
        pub(super) fn extend(&mut self, it: impl Iterator<Item = T>) {
            for v in it {
                self.push(v);
            }
        }

        /// Removes and returns the last element. Charged.
        #[inline]
        pub(super) fn pop(&mut self) -> Option<T> {
            super::charge(1);
            self.0.pop()
        }

        /// Empties the collection, charged for what it drops.
        pub(super) fn clear(&mut self) {
            super::charge(self.0.len() as u64);
            self.0.clear();
        }

        /// Consumes the collection, charged for every element handed
        /// over — so draining it is not a way to read it for free.
        pub(super) fn into_iter_counted(self) -> impl DoubleEndedIterator<Item = T> {
            super::charge(self.0.len() as u64);
            self.0.into_iter()
        }
    }

    impl<T, C: Deref<Target = [T]>> Counted<C> {
        /// How many elements there are. **Not charged**: a length is not
        /// an element access, and charging it would make the bound
        /// depend on how often a loop asks how long something is.
        /// Measured by the review: nested loops over `0..len()` whose
        /// sums escape through `black_box` move no count, which is the
        /// limit this gate states rather than hides.
        pub(super) fn len(&self) -> usize {
            self.0.len()
        }

        /// One element, or `None` past the end. Charged.
        #[inline]
        pub(super) fn get(&self, i: usize) -> Option<&T> {
            super::charge(1);
            self.0.get(i)
        }

        /// Every element, charged one at a time as they are yielded — so
        /// a scan that stops early pays for what it read and no more.
        pub(super) fn iter<'a>(&'a self) -> impl Iterator<Item = &'a T>
        where
            T: 'a,
        {
            self.0.iter().inspect(|_| super::charge(1))
        }
    }

    impl<T: Copy, C: DerefMut<Target = [T]>> Counted<C> {
        /// Replaces one element. Charged: a write is an access.
        #[inline]
        pub(super) fn set(&mut self, i: usize, v: T) {
            super::charge(1);
            self.0[i] = v;
        }

        /// Replaces one element if there is one there. Charged once
        /// whether or not there is, because the reach is the work.
        #[inline]
        pub(super) fn try_set(&mut self, i: usize, v: T) {
            super::charge(1);
            if let Some(slot) = self.0.get_mut(i) {
                *slot = v;
            }
        }
    }

    impl<T> Counted<Vec<Vec<T>>> {
        /// Appends to the inner collection at `i`. Charged once for
        /// reaching it, which is what a per-entry scan over the outer
        /// collection would pay per entry.
        #[inline]
        pub(super) fn push_into(&mut self, i: usize, v: T) {
            super::charge(1);
            if let Some(inner) = self.0.get_mut(i) {
                inner.push(v);
            }
        }

        /// Consumes the outer collection, handing each inner one over
        /// **still counted** — so a bucket cannot be read for free by
        /// draining the collection that holds it.
        pub(super) fn into_counted_inners(self) -> impl Iterator<Item = Counted<Vec<T>>> {
            super::charge(self.0.len() as u64);
            self.0.into_iter().map(Counted::new)
        }
    }
}

use counted::Counted;

/// What [`plan_shapes`] is handed: the reads, **already metered**.
///
/// **The type is the boundary, and that is the point.** A plain
/// `&[SelectorRead]` parameter could be aliased in one line —
/// `let raw = reads;`, a shared slice being `Copy` — and searched per
/// chain with the access counter and the source rule both green, which is
/// round 2's defect restored (code review round 5). There is no plain
/// slice to alias here: the signature never offers one, and [`Counted`]
/// hands out no way to get one back — it has no `Deref`, so even
/// `&*reads` does not compile.
pub(crate) type SelectorReads = Counted<Vec<SelectorRead>>;

/// Charges `n` element accesses.
///
/// **Test-only, and nothing at all in every other build** — measured on
/// both built libraries, and the artifact is named in the notes because
/// a symbol query that looked at the wrong one would read exactly like
/// an absence. The cost claim is a property a check has to be able to
/// fail, and a wall clock is not one: a clock is not scale-invariant and
/// measures the machine rather than the algorithm.
#[cfg(test)]
fn charge(n: u64) {
    WORK.with(|c| c.set(c.get() + n));
}

#[cfg(not(test))]
#[inline(always)]
fn charge(_n: u64) {}

#[cfg(test)]
thread_local! {
    static WORK: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Runs `f` and reports the element accesses charged while it ran.
#[cfg(test)]
fn work_of<T>(f: impl FnOnce() -> T) -> (T, u64) {
    WORK.with(|c| c.set(0));
    let out = f();
    (out, WORK.with(std::cell::Cell::get))
}

/// The plan tree flattened into pre-order, with each node's parent and
/// the entry it consumes DIRECTLY (before any propagation).
///
/// **No depth column.** The build pass reverses each bucket rather than
/// sorting it, because pre-order already files a bucket by increasing
/// depth, so a depth per node would be a column nothing reads.
struct Flat<'a> {
    nodes: Counted<Vec<&'a PlanExpr>>,
    parent: Counted<Vec<Option<usize>>>,
    /// Never [`Owner::Many`]: no node names two entries of its own
    /// account, so a `Many` can only ever arise from the merge.
    direct: Counted<Vec<Owner>>,
}

/// The nodes in pre-order.
///
/// **The collections are counted while they are being BUILT**, not only
/// once they are finished. That is what the round-4 probe found missing:
/// a scan of everything pushed so far, placed right after a push, cost
/// nothing on any of four widths while the construction vectors were
/// plain. Reaching an element of `nodes`, `parent`, `direct`, `stack` or
/// `kids` charges here exactly as it does in the passes above.
fn flatten(root: &PlanExpr) -> Flat<'_> {
    let mut nodes: Counted<Vec<&PlanExpr>> = Counted::empty();
    let mut parent: Counted<Vec<Option<usize>>> = Counted::empty();
    let mut direct: Counted<Vec<Owner>> = Counted::empty();
    // (node, parent index). A stack, not recursion.
    let mut stack: Counted<Vec<(&PlanExpr, Option<usize>)>> = Counted::empty();
    stack.push((root, None));
    let mut kids: Counted<Vec<&PlanExpr>> = Counted::empty();
    while let Some((node, parent_of)) = stack.pop() {
        let i = nodes.len();
        nodes.push(node);
        parent.push(parent_of);
        direct.push(direct_owner(node));
        kids.clear();
        children(node, &mut kids);
        // Pushed in reverse so the first child is popped first and the
        // flattened order is a genuine pre-order.
        let arity = kids.len();
        for k in (0..arity).rev() {
            if let Some(child) = kids.get(k).copied() {
                stack.push((child, Some(i)));
            }
        }
    }
    Flat {
        nodes,
        parent,
        direct,
    }
}

/// The entry a node consumes **without** looking at its children.
///
/// At most one, always — which is why the return type is an [`Owner`]
/// that is never `Many` here.
///
/// `Absent::selector` and `Timestamp::bare_selector` are deliberately not
/// read: each names a selector that is already the node's own argument,
/// so counting it would be a second spelling of one occurrence.
/// `Info::info_selector` IS read, because the synthetic metadata selector
/// is a selector-list entry that appears nowhere in the tree — which is
/// why `info(m)` consumes two entries and is on no chain.
fn direct_owner(e: &PlanExpr) -> Owner {
    match e {
        PlanExpr::Selector(id) => Owner::One(*id),
        PlanExpr::RangeVector { source }
        | PlanExpr::RangeFn { source, .. }
        | PlanExpr::OverTime { source, .. }
        | PlanExpr::OverTimeParam { source, .. }
        | PlanExpr::AbsentOverTime { source } => match source {
            RangeSource::Selector(id) => Owner::One(*id),
            RangeSource::Subquery(_) => Owner::None,
        },
        PlanExpr::Info { info_selector, .. } => Owner::One(*info_selector),
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
        | PlanExpr::StringLiteral(_) => Owner::None,
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
fn children<'a>(e: &'a PlanExpr, out: &mut Counted<Vec<&'a PlanExpr>>) {
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

fn range_source_child<'a>(source: &'a RangeSource, out: &mut Counted<Vec<&'a PlanExpr>>) {
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
    /// The rows this selector's statements return. [`PqlShape::Samples`]
    /// for the sample fetch; [`PqlShape::GroupedRuns`] when the grouped
    /// push was taken for this selector (issue #549) — which is the ONE
    /// input the aggregate link's capability reads.
    pub shape: PqlShape,
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
pub(crate) fn plan_shapes(
    plan: &pulsus_promql::QueryPlan,
    reads: &SelectorReads,
    params: &MetricQueryParams,
) -> Result<Vec<PlanShape>, ReadError> {
    let bounds = request_bounds(params);
    let config = PlanConfig::default();
    let cx = PlanCx {
        bounds: &bounds,
        config: &config,
    };
    let lower_cx = LowerCx::<Pql>::new(&bounds);
    // The reads, indexed by the entry they belong to, built once. A
    // linear search per chain made this quadratic in the selector count
    // on a request an untrusted caller can send (code review round 2):
    // 4,096 selectors did 8,390,656 comparisons.
    //
    // **The reads arrive counted**, so there is no plain slice in scope to
    // alias and search per chain — the shape that beat the shadowing this
    // replaced (code review round 5). The index is counted for the same
    // reason.
    let mut by_selector: Counted<Vec<Option<&SelectorRead>>> =
        Counted::new(vec![None; plan.selectors.len()]);
    for read in reads.iter() {
        by_selector.try_set(read.selector, Some(read));
    }
    let mut out = Vec::new();
    for chain in chain_of(plan) {
        let Some(read) = by_selector.get(chain.selector).copied().flatten() else {
            continue;
        };
        let names = stage_names(&chain);
        let lowering = lower_chain::<Pql>(
            &chain.links,
            seed_relation(read.pred.clone(), read.shape.clone()),
            &lower_cx,
        )?;
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

    /// Issue #549 criterion 4: **the queries that keep today's route
    /// keep today's plan, byte for byte.**
    ///
    /// The shape is taken from [`crate::metrics::grouped::shape_of`],
    /// not chosen by this test — which is what makes each mutation below
    /// reach it. **Every one was run** (review round 1 named one that did
    /// not):
    ///
    /// ```text
    ///   mutation                               query it turns eligible    case
    ///   widen the pushed set to Stddev         stddev by (status) (m)     1
    ///   accept ANY child, taking the plan's    max by (status) (abs(m))   3
    ///     single selector
    /// ```
    ///
    /// **`max by (status) (rate(m[5m]))` is case 2 and no mutation of the
    /// direct-child check reaches it.** Lifting that check alone leaves it
    /// declined, because a range selector is refused independently a few
    /// lines further down — measured in review round 1, where the named
    /// break stayed green. Two protections decline it and removing one
    /// changes nothing, so case 2 is kept for the shape it pins and case
    /// 3 is what the child check is tested by: `abs(…)` is a non-selector
    /// child whose selector is still a plain INSTANT one, so the child
    /// check is the only thing declining it.
    ///
    /// The flag is ON in the configuration this test builds, so a decline
    /// here is a decline on the query's shape and not on the flag.
    #[test]
    fn every_unpushed_query_keeps_todays_plan_byte_for_byte() {
        let cases: [(&str, &str); 3] = [
            (
                "stddev by (status) (http_requests_total{status=\"500\"})",
                r#"{"parts":[{"kind":"sql","name":"metric_samples","issue":"once","cut":null,"seed":null,"yields":"candidates"},{"kind":"sql","name":"metric_hist_samples","issue":"once","cut":{"why":"disjoint_sources","sources":["metric_samples","metric_hist_samples"]},"seed":null,"yields":"candidates"},{"kind":"engine","links":[1]}],"links":[{"i":0,"part":0,"stage":"Select(0)","how":"lowered","fidelity":"wider"},{"i":1,"part":2,"stage":"Aggregate(stddev)","how":"residual","why":"not_yet_lowered"}]}"#,
            ),
            (
                "max by (status) (rate(http_requests_total{status=\"500\"}[5m]))",
                r#"{"parts":[{"kind":"sql","name":"metric_samples","issue":"once","cut":null,"seed":null,"yields":"candidates"},{"kind":"sql","name":"metric_hist_samples","issue":"once","cut":{"why":"disjoint_sources","sources":["metric_samples","metric_hist_samples"]},"seed":null,"yields":"candidates"},{"kind":"engine","links":[1,2]}],"links":[{"i":0,"part":0,"stage":"Select(0)","how":"lowered","fidelity":"wider"},{"i":1,"part":2,"stage":"RangeFn(rate)","how":"residual","why":"not_yet_lowered"},{"i":2,"part":2,"stage":"Aggregate(max)","how":"residual","why":"not_yet_lowered"}]}"#,
            ),
            (
                "max by (status) (abs(http_requests_total{status=\"500\"}))",
                r#"{"parts":[{"kind":"sql","name":"metric_samples","issue":"once","cut":null,"seed":null,"yields":"candidates"},{"kind":"sql","name":"metric_hist_samples","issue":"once","cut":{"why":"disjoint_sources","sources":["metric_samples","metric_hist_samples"]},"seed":null,"yields":"candidates"},{"kind":"engine","links":[1,2]}],"links":[{"i":0,"part":0,"stage":"Select(0)","how":"lowered","fidelity":"wider"},{"i":1,"part":2,"stage":"MathFn(abs)","how":"residual","why":"not_yet_lowered"},{"i":2,"part":2,"stage":"Aggregate(max)","how":"residual","why":"not_yet_lowered"}]}"#,
            ),
        ];
        for (query, want) in cases {
            let shapes = rendered_plan(query);
            assert_eq!(shapes.len(), 1, "{query}: one selector that reads");
            let got = serde_json::to_string(&shapes[0]).expect("serialize");
            assert_eq!(got, want, "{query}: the plan moved");
        }
    }

    /// Issue #549: a PUSHED selector is ONE SQL part, named for
    /// `metric_samples` and additively naming the histogram table it
    /// reads inside the same statement — with no `disjoint_sources` cut,
    /// because there is no second statement to cut from, and no engine
    /// part, because the aggregate link lowers.
    #[test]
    fn a_pushed_selector_is_one_part_that_also_reads_the_histogram_table() {
        let shapes = rendered_plan("max by (status) (http_requests_total{status=\"500\"})");
        assert_eq!(shapes.len(), 1);
        assert_eq!(
            serde_json::to_string(&shapes[0]).expect("serialize"),
            r#"{"parts":[{"kind":"sql","name":"metric_samples","also_reads":["metric_hist_samples"],"issue":"once","cut":null,"seed":null,"yields":"candidates"}],"links":[{"i":0,"part":0,"stage":"Select(0)","how":"lowered","fidelity":"wider"},{"i":1,"part":0,"stage":"Aggregate(max)","how":"lowered","fidelity":"wider"}]}"#
        );
    }

    /// The plan shapes for `query`, with each selector's seed shape taken
    /// from `grouped::shape_of` — the same decision `exec` takes, so a
    /// change to the eligibility rule reaches these literals.
    fn rendered_plan(query: &str) -> Vec<PlanShape> {
        let plan = planned(query);
        let cfg = grouped_test_config();
        let shape = crate::metrics::grouped::shape_of(&plan, &plan_params(), &cfg);
        let mut reads = SelectorReads::empty();
        for (i, _) in plan.selectors.iter().enumerate() {
            let pushed = shape.as_ref().is_some_and(|s| s.selector == i);
            reads.push(SelectorRead {
                selector: i,
                pred: if pushed {
                    grouped_selector_pred(
                        "metric_name = 'http_requests_total'",
                        "unix_milli > 1 AND unix_milli <= 2",
                        "fingerprint IN (7)",
                    )
                } else {
                    pred("http_requests_total")
                },
                shape: if pushed {
                    PqlShape::GroupedRuns
                } else {
                    PqlShape::Samples
                },
            });
        }
        plan_shapes(&plan, &reads, &params()).expect("plan shapes")
    }

    /// The engine configuration these tests plan against: the grouped
    /// push ON, so a decline is a decline on the query and not on the
    /// flag.
    fn grouped_test_config() -> crate::metrics::MetricsConfig {
        crate::metrics::MetricsConfig {
            db: "d".to_string(),
            samples_table: "metric_samples".to_string(),
            hist_samples_table: "metric_hist_samples".to_string(),
            series_table: "metric_series".to_string(),
            metadata_table: "metric_metadata".to_string(),
            experimental_functions: true,
            max_metric_fanout: 1_000,
            max_cache_scan: 200_000,
            max_info_series: 100_000,
            max_samples: 50_000_000,
            distributed: false,
            read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
            grouped_push: true,
        }
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
        let mut s1 = seed_relation(pred("a"), PqlShape::Samples);
        let mut s2 = seed_relation(pred("b"), PqlShape::Samples);
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
        let mut reads = SelectorReads::empty();
        reads.push(SelectorRead {
            selector: 0,
            pred: pred("http_requests_total"),
            shape: PqlShape::Samples,
        });
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
        let mut reads = SelectorReads::empty();
        reads.push(SelectorRead {
            selector: 0,
            pred: pred("http_requests_total"),
            shape: PqlShape::Samples,
        });
        let bounds = request_bounds(&params());
        let config = PlanConfig::default();
        let cx = PlanCx {
            bounds: &bounds,
            config: &config,
        };
        let chain = chain_of(&plan).remove(0);
        let lowering = lower_chain::<Pql>(
            &chain.links,
            seed_relation(
                reads.get(0).expect("one read").pred.clone(),
                PqlShape::Samples,
            ),
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
        let mut reads = SelectorReads::empty();
        reads.push(SelectorRead {
            selector: 1,
            pred: pred("http_errors_total"),
            shape: PqlShape::Samples,
        });
        let shapes = plan_shapes(&plan, &reads, &params()).expect("plan shapes");
        assert_eq!(shapes.len(), 1, "one selector read, so one plan");
        assert_eq!(shapes[0].links[0].stage, "Select(1)");
        let mut both = SelectorReads::empty();
        both.push(SelectorRead {
            selector: 0,
            pred: pred("http_requests_total"),
            shape: PqlShape::Samples,
        });
        both.push(SelectorRead {
            selector: 1,
            pred: pred("http_errors_total"),
            shape: PqlShape::Samples,
        });
        let shapes = plan_shapes(&plan, &both, &params()).expect("plan shapes");
        assert_eq!(shapes.len(), 2);
        assert_eq!(shapes[0].links[0].stage, "Select(0)");
        assert_eq!(shapes[1].links[0].stage, "Select(1)");
    }

    /// A balanced sum of `terms` selectors: 4,096 of them is a query the
    /// accept surface admits, because the depth cap is on DEPTH and a
    /// balanced sum of 4,096 terms is twelve deep.
    fn wide_query(terms: usize) -> String {
        let mut level: Vec<String> = (0..terms).map(|i| format!("m{i}")).collect();
        while level.len() > 1 {
            level = level
                .chunks(2)
                .map(|pair| match pair {
                    [a, b] => format!("({a} + {b})"),
                    [a] => a.clone(),
                    _ => unreachable!("chunks(2) yields one or two"),
                })
                .collect();
        }
        level.pop().expect("one root")
    }

    /// **The whole explain build is linear in the tree, not quadratic in
    /// the selector list.**
    ///
    /// A 4,096-selector query is an ACCEPTED request from an untrusted
    /// caller, so the cost of building its plan is reachable. Before this
    /// was fixed the builder scanned every node once per entry and, on
    /// that query, examined 33,550,336 nodes, with a further 8,390,656
    /// comparisons to find each chain's recorded read (issue #548, code
    /// review round 2).
    ///
    /// **It measures `plan_shapes`, not `chain_of`**, because the read
    /// lookup lives there: a gate over the walk alone could not see
    /// `reads.iter().find` come back, and did not (code review round 3).
    ///
    /// **What is counted is element ACCESSES, charged by the collections
    /// themselves.** Nothing here and nothing in the walk calls a
    /// counter: `Counted` charges on `get`, `set` and each element an
    /// `iter` yields, so a scan anyone adds **through those accessors**
    /// pays for what it reads without having to remember anything. A scan
    /// over a copy taken out of one of them does not; that is the bound
    /// below, and it is the limit of the technique. The previous version counted
    /// at hand-written call sites and stayed green when either quadratic
    /// path was restored without them.
    ///
    /// Three assertions. The bound is the ceiling; the ratio is what
    /// says LINEAR rather than "small on this machine", because accesses
    /// per node plus entry must not grow with the width; and the link
    /// total bounds what the per-chain callees are handed. Counted
    /// rather than timed — a wall clock measures the machine, and a
    /// bound on one would be neither scale-invariant nor reproducible on
    /// another.
    ///
    /// # What this gate does NOT see
    ///
    /// Named here rather than left to be found, because a check whose
    /// limits are unstated gets read as covering everything. The list is
    /// what six rounds of review established, and the first item is the
    /// limit of the technique rather than a gap in this use of it.
    ///
    /// * **Work over a COPY taken out of a counted collection.** One
    ///   metered pass buys an unmetered buffer:
    ///
    ///   ```text
    ///   let raw = reads.iter().collect::<Vec<_>>();   // charged once, per element
    ///   for chain in … {                              // then free, for ever
    ///       raw.iter().find(|r| r.selector == chain.selector);
    ///   }
    ///   ```
    ///
    ///   Measured: both checks green and all four access rows at their
    ///   published values. The same materialisation works for the
    ///   flattened nodes, the buckets and the read index.
    ///
    ///   **The escape is an UNCHARGED copy, not a copy.** A copy whose
    ///   iteration still charges is seen — measured both ways at this
    ///   head, on the shape above:
    ///
    ///   ```text
    ///   raw.iter().inspect(|_| charge(1)).find(…)   RED, 4,175 over a bound of 3,056
    ///   raw.iter().find(…)                          green
    ///   ```
    ///
    ///   So the true statement is the narrow one: **this counter cannot
    ///   see reads over a copy that is no longer charged.** And what
    ///   cannot be closed by counting harder is exactly that case — the
    ///   copy is an ordinary value the counter has no hold on, so whether
    ///   it charges is the writer's choice rather than the collection's.
    ///   It is why this gate is the last one built for this claim rather
    ///   than the sixth of a series.
    /// * **Work that touches no collection.** Nested loops over
    ///   `0..len()` whose sums escape through `black_box` move no count.
    ///   `len()` is deliberately free; arithmetic over indices is
    ///   invisible.
    /// * **Allocation and string cost.** Not charged. A change that kept
    ///   the access count and doubled the bytes would pass.
    /// * **The per-chain callees' internals** — `stage_names`,
    ///   `lower_chain`, `plan_of`, `shape`. What they are HANDED is
    ///   bounded below; what they do with it is their own business, and
    ///   two of them belong to the shared core, which this module does
    ///   not instrument.
    /// * **The planner's own `QueryPlan`.** The walk borrows it and does
    ///   not own it: `plan.selectors` is a plain `Vec` on someone else's
    ///   struct, and a scan of it per node would be charged nothing. The
    ///   walk reads its length and its root, and nothing else.
    /// * **A collection the walk adds that is not a `Counted`** — which
    ///   is what `every_collection_the_walk_reads_back_is_counted` is
    ///   for, and that rule's own bound is published on it and is narrow.
    ///
    /// **So what does this gate guarantee?** Exactly one thing: a scan
    /// written *against the counted collections* is priced, and the three
    /// shapes that reached production review — a pass over every node per
    /// entry, a search of the reads per chain, and a scan of what has
    /// been pushed so far during construction — each redden it at the
    /// narrowest width. That is worth having and it is not a proof of
    /// total work.
    /// # The figures
    ///
    /// They are in [`ROWS`] below, **asserted exactly**, not written in
    /// this comment. A table in prose beside a test cannot go wrong
    /// loudly: changing one of these numbers here left the test green,
    /// which is a table that looks authoritative and is not (code review
    /// round 6). A change to the charged surface now reddens this test
    /// and prints both numbers.
    ///
    /// The denominator in the ratio is nodes PLUS entries, which is the
    /// one the `O(nodes + entries)` claim names; per tree node the same
    /// figures are about 1.5 times these and are a different quantity.
    #[test]
    fn the_explain_walk_is_linear_in_the_tree() {
        /// `(entries, nodes, query bytes, element accesses)` — what the
        /// walk costs at four widths, as the shipped code produces it.
        /// The counter is deterministic: one charge per accessor call,
        /// one thread, no dependence on the allocator, so these are
        /// equalities rather than ceilings and they reproduced on two
        /// machines.
        const ROWS: [(usize, usize, usize, u64); 4] = [
            (64, 127, 497, 2_095),
            (256, 511, 2_189, 8_431),
            (1_024, 2_047, 9_125, 33_775),
            (4_096, 8_191, 39_845, 135_151),
        ];

        let mut rows: Vec<(usize, usize, usize, u64)> = Vec::new();
        for terms in ROWS.map(|r| r.0) {
            let q = wide_query(terms);
            let plan = planned(&q);
            assert_eq!(plan.selectors.len(), terms, "{terms}: entries");
            // One recorded read per entry, so every chain finds one and
            // the lookup is exercised once per chain — the path the
            // round-2 index replaced a linear search on.
            let mut reads = SelectorReads::empty();
            for selector in 0..terms {
                reads.push(SelectorRead {
                    selector,
                    pred: pred("http_requests_total"),
                    shape: PqlShape::Samples,
                });
            }
            let (shapes, work) =
                work_of(|| plan_shapes(&plan, &reads, &params()).expect("plan shapes"));
            assert_eq!(
                shapes.len(),
                terms,
                "{terms}: one plan per entry that reads"
            );
            // Every operator over two entries is on no chain, so each
            // chain here is its source link alone.
            assert_eq!(shapes[0].links.len(), 1, "{terms}");
            assert_eq!(shapes[0].links[0].stage, "Select(0)", "{terms}");
            assert_eq!(
                shapes[terms - 1].links[0].stage,
                format!("Select({})", terms - 1),
                "{terms}"
            );
            // A balanced sum of `terms` selectors is `terms` leaves and
            // `terms - 1` operators.
            let nodes = 2 * terms - 1;
            let bound = 16 * (nodes + terms) as u64;
            assert!(
                work <= bound,
                "{terms} entries, {nodes} nodes, {} query bytes: building the plans took {work} \
                 element accesses, over the bound of {bound}. A pass over every node inside the \
                 per-entry loop, or a search of the reads per chain, is what puts it there.",
                q.len()
            );
            // **What the per-chain callees are handed is linear too.**
            // `stage_names`, `lower_chain`, `plan_of` and `shape` are
            // called once per chain and read only that chain's links,
            // and the links total no more than one per entry plus one
            // per node. Their own internals are not charged here — that
            // is named below as a limit — but the INPUT they get cannot
            // grow faster than the tree.
            let links: usize = shapes.iter().map(|s| s.links.len()).sum();
            assert!(
                links <= terms + nodes,
                "{terms} entries, {nodes} nodes: the chains carry {links} links between them,                  more than one per entry plus one per node, so the callees are handed more than                  linear work"
            );
            rows.push((terms, nodes, q.len(), work));
        }
        assert_eq!(
            rows,
            ROWS.to_vec(),
            "the walk's cost moved. Each row is (entries, nodes, query bytes, element accesses); \
             if the charged surface changed on purpose, the new numbers go here and the reason \
             goes in the notes."
        );
        // Per NODE PLUS ENTRY — the same denominator the bound uses and
        // the same one the `O(nodes + entries)` claim names. Divided by
        // tree nodes alone the figures are about 1.5 times these, which
        // is a different quantity and not the one claimed.
        let per_node_plus_entry = |&(terms, nodes, _, work): &(usize, usize, usize, u64)| {
            work as f64 / (nodes + terms) as f64
        };
        let (narrow, wide) = (
            per_node_plus_entry(&rows[0]),
            per_node_plus_entry(&rows[rows.len() - 1]),
        );
        assert!(
            wide <= narrow * 1.05,
            "accesses per (node + entry) grew from {narrow} at {} entries to {wide} at {}              entries: {rows:?}",
            rows[0].0,
            rows[rows.len() - 1].0
        );
    }

    /// **Every collection the walk reads back is a [`Counted`], and every
    /// collection that is not is write-only.**
    ///
    /// This is the half of the linearity claim that a measurement cannot
    /// make. The access counter charges what goes through `Counted`; it
    /// says nothing about a plain collection somebody adds tomorrow.
    ///
    /// The rule:
    ///
    /// > In the walk's functions, a local binding of a plain collection
    /// > may appear only as its own initialiser, as the receiver of
    /// > `push` or `extend`, or as a bare identifier. Any other use —
    /// > indexing, `iter`, `get`, `len`, a `for` loop over it, taking a
    /// > reference to it — is a READ, and a collection the walk reads
    /// > must be a `Counted`.
    ///
    /// The collections it permits are **published in [`WRITE_ONLY`] and
    /// asserted exactly**, so adding a fourth is a change to a list a
    /// reader sees in the diff rather than something discovered the day
    /// it breaks.
    ///
    /// # What this rule does NOT refuse, measured
    ///
    /// Twenty shapes, each a genuine unmetered read of a plain collection
    /// inside the walk, all twenty compiled into `chain_of` in **one**
    /// run: **it refuses 5 of them.**
    ///
    /// ```text
    ///   REFUSED      1 a plain binding read          `let v: Vec<usize> = …; v.len()`
    ///   (recognised, 4 a closure capture             `let f = || v.len();`
    ///    and the    13 a boxed slice from a Vec      `let v: Box<[usize]> = Vec::new().into_boxed_slice();`
    ///    read       19 a `ref` pattern               `let ref v = vec![0usize];`
    ///    reported)  20 a nested function's local     `fn inner() { let v: Vec<usize> = …; v.len() }`
    ///
    ///   RECOGNISED,  2 rebinding                     `let w = v; w.len()`
    ///   permitted    5 `format!`                     `format!("{v:?}")`  — the use is inside a macro
    ///   (the         6 a match binding               `let w = match … { _ => v }; w.len()`
    ///    binding     7 `&*name`                      `let s: &[usize] = &*v;`
    ///    is seen,    8 a helper, by move             `helper(v)`
    ///    the read    9 UFCS, by move                 `Vec::into_iter(v)`
    ///    is not)
    ///
    ///   UNRECOGNISED 3 a tuple field                 `let t = (Vec::new(), 0); t.0.len()`
    ///   (not seen   10 `VecDeque`                   11 `HashMap`              12 an array
    ///    as a       14 an inferred `collect()`       15 a type alias          16 a Vec from a function
    ///    collection 17 a tuple pattern               18 a method on a struct
    ///    at all)
    /// ```
    ///
    /// **The number is a property of the twenty shapes, not a constant**,
    /// and the two words for what happens to a shape are not the same
    /// one. A shape is **recognised** when this rule sees a plain
    /// collection at all, and **refused** when it also reports a read of
    /// it; the table above is 5 refused, 6 recognised and permitted, 9
    /// unrecognised. The round-6 review ran its own twenty, and its run
    /// **recognised** nine — its binding census failed first, so the read
    /// assertion never ran and nothing was refused; its own diagnostic
    /// named four. Different code, and the first of the two questions
    /// rather than the second. What both runs establish is the same
    /// thing: as a way of refusing unmetered reads this rule is weak, and
    /// widening it has been beaten by the next shape every time it was
    /// tried.
    ///
    /// **So this is not what keeps the walk honest, and the linearity
    /// claim does not rest on it.** What keeps the walk honest is that
    /// there is nothing unmetered in scope to read: the tree collections
    /// are built counted, the buckets hand out counted inners, and the
    /// walk's INPUT arrives counted, so a plain slice cannot be aliased
    /// out of a parameter. This rule is the backstop for the one case
    /// that remains — a plain collection someone writes inside the walk —
    /// and it refuses the obvious form of that and not the ingenious
    /// ones.
    ///
    /// **What is outside this rule entirely**, so that nobody reads a
    /// guarantee that is not on offer:
    ///
    /// * the fifteen shapes above that it does not refuse;
    /// * a scan written in a helper function the walk calls, since the
    ///   rule is per function and a bare identifier is a move;
    /// * the planner's own `QueryPlan`, which the walk borrows and does
    ///   not own: `plan.selectors` is a plain `Vec` on someone else's
    ///   struct. The walk reads its length and its root, and nothing
    ///   else.
    ///
    /// Parsed with a syntax tree rather than matched textually, because a
    /// token rule cannot see a method call inside a macro body or a
    /// generic call.
    #[test]
    fn every_collection_the_walk_reads_back_is_counted() {
        use syn::visit::Visit;

        /// **The plain collections the walk is permitted to hold**, each
        /// write-only and moved out. Asserted exactly, so a fourth is a
        /// visible change to this list and not a silent permission.
        const WRITE_ONLY: [&str; 3] = ["chain_of::links", "chain_of::out", "plan_shapes::out"];

        /// The functions that make up the walk: everything reachable
        /// from `plan_shapes` that holds a collection of its own.
        const WALK: [&str; 5] = [
            "plan_shapes",
            "chain_of",
            "flatten",
            "children",
            "range_source_child",
        ];
        const ALLOWED: [&str; 2] = ["push", "extend"];

        fn named(e: &syn::Expr, name: &str) -> bool {
            matches!(e, syn::Expr::Path(p) if p.path.is_ident(name))
        }

        struct Uses<'a> {
            name: &'a str,
            bad: Vec<String>,
        }

        impl<'ast> Visit<'ast> for Uses<'_> {
            fn visit_expr(&mut self, e: &'ast syn::Expr) {
                match e {
                    syn::Expr::MethodCall(c) if named(&c.receiver, self.name) => {
                        let m = c.method.to_string();
                        if !ALLOWED.contains(&m.as_str()) {
                            self.bad.push(format!("{}.{m}(…)", self.name));
                        }
                        // The receiver is accounted for; only the
                        // arguments are still to visit.
                        for a in &c.args {
                            self.visit_expr(a);
                        }
                        return;
                    }
                    syn::Expr::Index(i) if named(&i.expr, self.name) => {
                        self.bad.push(format!("{}[…]", self.name));
                    }
                    syn::Expr::Reference(r) if named(&r.expr, self.name) => {
                        self.bad.push(format!("&{}", self.name));
                    }
                    syn::Expr::ForLoop(f) if named(&f.expr, self.name) => {
                        self.bad.push(format!("for … in {}", self.name));
                    }
                    _ => {}
                }
                syn::visit::visit_expr(self, e);
            }
        }

        /// Every `let` in a function body, at any depth.
        struct Locals(Vec<syn::Local>);

        impl<'ast> Visit<'ast> for Locals {
            fn visit_local(&mut self, l: &'ast syn::Local) {
                self.0.push(l.clone());
                syn::visit::visit_local(self, l);
            }
        }

        /// The binding's name, when it is a PLAIN collection — a `Vec`
        /// in its type or its initialiser, and no `Counted` in either.
        fn plain_collection(local: &syn::Local) -> Option<String> {
            let (name, ty) = match &local.pat {
                syn::Pat::Ident(i) => (i.ident.to_string(), None),
                syn::Pat::Type(t) => match &*t.pat {
                    syn::Pat::Ident(i) => (i.ident.to_string(), Some(&*t.ty)),
                    _ => return None,
                },
                _ => return None,
            };
            let ty_text = ty
                .map(|t| quote::quote!(#t).to_string())
                .unwrap_or_default();
            let init_text = local
                .init
                .as_ref()
                .map(|i| {
                    let e = &*i.expr;
                    quote::quote!(#e).to_string()
                })
                .unwrap_or_default();
            let counted = ty_text.contains("Counted") || init_text.contains("Counted");
            let plain = ty_text.contains("Vec <")
                || init_text.starts_with("Vec ::")
                || init_text.starts_with("vec !");
            (plain && !counted).then_some(name)
        }

        let file = syn::parse_file(include_str!("compile.rs")).expect("this file parses");
        let mut seen: Vec<&str> = Vec::new();
        let mut examined: Vec<String> = Vec::new();
        let mut problems: Vec<String> = Vec::new();
        for item in &file.items {
            let syn::Item::Fn(f) = item else { continue };
            let name = f.sig.ident.to_string();
            let Some(which) = WALK.iter().find(|w| **w == name) else {
                continue;
            };
            seen.push(which);
            // EVERY local in the body, not only the ones at the top
            // level: a scratch collection added inside a loop is the
            // likeliest place for a scan, and a check that looked only
            // at the outermost statements would not see it.
            let mut locals = Locals(Vec::new());
            locals.visit_block(&f.block);
            for local in locals.0 {
                let Some(binding) = plain_collection(&local) else {
                    continue;
                };
                examined.push(format!("{name}::{binding}"));
                let mut uses = Uses {
                    name: &binding,
                    bad: Vec::new(),
                };
                uses.visit_block(&f.block);
                for bad in uses.bad {
                    problems.push(format!(
                        "{name}: `{binding}` is a plain collection and is READ as `{bad}`. A \
                         collection the walk reads must be a `Counted`, or the access counter \
                         cannot see a scan over it."
                    ));
                }
            }
        }
        assert_eq!(
            seen.len(),
            WALK.len(),
            "the walk's functions were not all found; this check parsed {seen:?} of {WALK:?}"
        );
        examined.sort();
        assert_eq!(
            examined, WRITE_ONLY,
            "the walk's plain collections are not the ones this check publishes. Every one of \
             them is write-only and moved out; a new one belongs in `WRITE_ONLY` with that said \
             of it, so that permitting it is a line in the diff."
        );
        assert!(
            problems.is_empty(),
            "{} of the walk's plain collections are read:\n{}\nexamined: {examined:?}",
            problems.len(),
            problems.join("\n")
        );
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
