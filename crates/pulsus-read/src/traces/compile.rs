//! TraceQL against the shared compile core (issue #492, wave 1).
//!
//! **Compiled and unwired.** [`super::search_plan`] does not call
//! anything here, so no TraceQL statement moves. What lands is the chain
//! link set, the [`Lang`] impl, and the two rules the design rests on:
//! `Emit` is `Never` **and served by its own SQL part**, and no regex leaf
//! may claim [`Fidelity::Equivalent`].
//!
//! **`Emit` is `Never`, so a lowered TraceQL search is two statements,
//! not one.** The response's root summary is read trace-wide with **no
//! time predicate** — the true root may predate the search window — and
//! `TraceSearchResult.root` is not optional, so every search response
//! needs it. `Never` is the right classification and it does not mean the
//! evaluator does the work: the way the evaluator owns that link is to
//! send a second statement, so the plan builder gives it an SQL part.
//! "Cannot be lowered into THIS statement" and "is not SQL" are different
//! claims, and only the first is made here.

use pulsus_traceql::{
    ComparisonOp, FieldExpr, FieldOp, PipelineStage, SpansetExpr, SpansetFilter, UnaryOp,
};

use super::filter::PlanError;
use crate::compile::fold::{
    BlockReason, Capability, Col, ColSet, Fidelity, Lang, Lower, LowerCx, Name, NeverReason, Pred,
    Provenance, Relation, Shape, SourceName, SourceTerm,
};
use crate::compile::plan::{HandoffCost, PlanCx, SeedBound, SourceRef};

// ---------------------------------------------------------------------
// Sources
// ---------------------------------------------------------------------

/// The span table, read inside the request's time window.
pub const TRACE_SPANS: SourceRef = SourceRef("trace_spans");
/// The attribute index.
pub const TRACE_ATTRS_IDX: SourceRef = SourceRef("trace_attrs_idx");
/// The winners' root summary: the SAME table, read trace-wide with **no
/// time predicate**, which is why it is a different source and not the
/// same one twice. A window-bounded statement cannot produce that answer:
/// a search whose window begins after the root span starts still reports
/// that root's name, start and duration.
pub const TRACE_SPANS_ROOT: SourceRef = SourceRef("trace_spans:root");

/// The key the winners' root read is seeded on.
pub const TRACE_ID: &str = "trace_id";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TqlSource(pub SourceRef);

impl SourceName for TqlSource {
    fn source_ref(&self) -> SourceRef {
        self.0
    }
}

/// TraceQL's shapes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TqlShape {
    Spans,
    Traces,
    Groups(Name),
}

impl Shape for TqlShape {}

/// What crosses between two TraceQL parts: the winners' trace ids,
/// bounded by the request `limit`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TqlHandoff(pub Vec<[u8; 16]>);

/// TraceQL's chain link: the seven `PipelineStage` variants, plus the
/// selector and the three synthesised links.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TqlLink {
    Source(Box<SpansetExpr>),
    Pipe(PipelineStage),
    Order,
    Limit(u32),
    Emit,
}

/// The marker the core is generic over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tql;

// ---------------------------------------------------------------------
// The regex-dialect rule
// ---------------------------------------------------------------------

/// Which string comparison a leaf compiles to — the discriminant of the
/// search planner's own `StrOp`, which is all the fidelity rule reads.
///
/// The `StrOp`-to-`StrOpKind` bridge lives in this module's test rather
/// than here, because nothing in production holds a compiled `StrOp` at
/// the point a fidelity verdict is wanted; it is written there as an
/// exhaustive `match` with no `_` arm, so a new `StrOp` variant fails to
/// build a binary `cargo test --workspace` builds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrOpKind {
    Eq,
    Neq,
    Re,
    Nre,
}

impl StrOpKind {
    /// The AST operator's compiled shape, or `None` for an operator no
    /// string leaf accepts.
    pub fn of_comparison(op: ComparisonOp) -> Option<Self> {
        match op {
            ComparisonOp::Eq => Some(StrOpKind::Eq),
            ComparisonOp::Neq => Some(StrOpKind::Neq),
            ComparisonOp::Re => Some(StrOpKind::Re),
            ComparisonOp::Nre => Some(StrOpKind::Nre),
            ComparisonOp::Gt | ComparisonOp::Gte | ComparisonOp::Lt | ComparisonOp::Lte => None,
        }
    }

    /// **The one place the rule is stated.**
    ///
    /// A regex leaf may never be [`Fidelity::Equivalent`], and the reason
    /// is measured rather than assumed: on this path one pattern gets two
    /// readings. The generator renders the pattern into SQL, where the
    /// database reads it as RE2; our own evaluator re-checks the same
    /// pattern with the raw Rust `regex` crate, with no dialect rewrite
    /// applied anywhere under `traces/`. The two disagree on `\d`, `\w`
    /// and `\s`: over the subject `٤` (U+0664), RE2 does not match and
    /// the Rust crate does.
    ///
    /// **`Wider` is the answer, and it is not a repair.** `Wider` means
    /// `orig ⟹ sql`; on those subjects the SQL reading is the NARROWER
    /// one, so the candidate is discarded in the database and there is
    /// nothing left for the evaluator to re-apply the link to. Measured
    /// end to end through our own write and read paths: two traces, one
    /// span each, named `٤` and `4`; `{ name =~ "\d" }` emits
    /// `match(name, '^(?:\d)$')` and returns ONE trace, the ASCII one,
    /// while the raw Rust crate matches both subjects. The divergence is
    /// a parity defect that ships today and is recorded, not repaired
    /// here; what this rule prevents is a compiler decision built on top
    /// of it.
    pub fn fidelity(self) -> Fidelity {
        match self {
            StrOpKind::Eq | StrOpKind::Neq => Fidelity::Equivalent,
            StrOpKind::Re | StrOpKind::Nre => Fidelity::Wider,
        }
    }
}

/// Whether any leaf of a selector compiles to a regex comparison — the
/// selector-level reading of [`StrOpKind::fidelity`], and not a second
/// statement of the rule: it asks [`StrOpKind::fidelity`] for every leaf
/// it finds.
pub fn selector_fidelity(expr: &SpansetExpr) -> Fidelity {
    fn walk_field(e: &FieldExpr, out: &mut Fidelity) {
        match e {
            FieldExpr::Field(_) | FieldExpr::Literal(_) | FieldExpr::Exists { .. } => {}
            FieldExpr::Unary {
                op: UnaryOp::Not | UnaryOp::Neg,
                expr,
            } => walk_field(expr, out),
            FieldExpr::Binary { op, lhs, rhs } => {
                if let FieldOp::Cmp(c) = op
                    && let Some(kind) = StrOpKind::of_comparison(*c)
                    && kind.fidelity() == Fidelity::Wider
                {
                    *out = Fidelity::Wider;
                }
                walk_field(lhs, out);
                walk_field(rhs, out);
            }
        }
    }
    fn walk_filter(f: &SpansetFilter, out: &mut Fidelity) {
        if let Some(body) = &f.body {
            walk_field(body, out);
        }
    }
    fn walk(e: &SpansetExpr, out: &mut Fidelity) {
        match e {
            SpansetExpr::Filter(f) => walk_filter(f, out),
            SpansetExpr::Binary { lhs, rhs, .. } | SpansetExpr::Structural { lhs, rhs, .. } => {
                walk(lhs, out);
                walk(rhs, out);
            }
        }
    }
    let mut out = Fidelity::Equivalent;
    walk(expr, &mut out);
    out
}

// ---------------------------------------------------------------------
// The dispatchers
// ---------------------------------------------------------------------

macro_rules! dispatchers {
    ($($name:ident),+ $(,)?) => {
        $(
            #[derive(Debug)]
            pub struct $name;
        )+
    };
}

dispatchers!(
    SourceLower,
    AggregateLower,
    SelectLower,
    ByLower,
    CoalesceLower,
    NotASearchLinkLower,
    OrderLower,
    LimitLower,
    EmitLower,
);

static SOURCE: SourceLower = SourceLower;
static AGGREGATE: AggregateLower = AggregateLower;
static SELECT: SelectLower = SelectLower;
static BY: ByLower = ByLower;
static COALESCE: CoalesceLower = CoalesceLower;
static NOT_A_SEARCH_LINK: NotASearchLinkLower = NotASearchLinkLower;
static ORDER: OrderLower = OrderLower;
static LIMIT: LimitLower = LimitLower;
static EMIT: EmitLower = EmitLower;

impl Lang for Tql {
    type Stage = TqlLink;
    type Source = TqlSource;
    type ColExpr = String;
    type Shape = TqlShape;
    type Handoff = TqlHandoff;
    type Err = PlanError;

    /// The ONE exhaustive match over the chain-link type. **No `_` arm**:
    /// adding a `PipelineStage` variant fails to compile here.
    fn lower_of(stage: &TqlLink) -> &'static dyn Lower<Tql> {
        match stage {
            TqlLink::Source(_) => &SOURCE,
            TqlLink::Pipe(p) => match p {
                PipelineStage::Aggregate { .. } => &AGGREGATE,
                PipelineStage::Select { .. } => &SELECT,
                PipelineStage::By { .. } => &BY,
                PipelineStage::Coalesce => &COALESCE,
                // Not chain links on the search route: the planner
                // answers 400 for all three, so the chain builder never
                // constructs one. They are dispatched rather than
                // wildcarded so the match stays exhaustive.
                PipelineStage::Metric(_)
                | PipelineStage::MetricSecondStage(_)
                | PipelineStage::Compare { .. } => &NOT_A_SEARCH_LINK,
            },
            TqlLink::Order => &ORDER,
            TqlLink::Limit(_) => &LIMIT,
            TqlLink::Emit => &EMIT,
        }
    }

    fn source_of(stage: &TqlLink, rel: &Relation<Tql>) -> SourceRef {
        match stage {
            TqlLink::Emit => TRACE_SPANS_ROOT,
            _ => rel.source_ref(),
        }
    }

    fn handoff_key(stage: &TqlLink, _rel: &Relation<Tql>) -> Option<Name> {
        match stage {
            TqlLink::Emit => Some(Name::from(TRACE_ID)),
            _ => None,
        }
    }

    fn handoff_bound(stage: &TqlLink, _rel: &Relation<Tql>, cx: &PlanCx<'_>) -> Option<SeedBound> {
        match stage {
            TqlLink::Emit => Some(SeedBound::RequestLimit(cx.bounds.limit?)),
            _ => None,
        }
    }

    /// A trace id renders as `unhex('<32 hex>')` inside an `IN (…)` list:
    /// 41 characters plus `", "`, and two AST elements — the call and its
    /// string literal. The 48-byte constant is the
    /// `trace_id IN ()` frame.
    fn handoff_cost(n: u64) -> HandoffCost {
        HandoffCost {
            text_bytes: 48 + n * 43,
            ast_elements: 4 + n * 2,
        }
    }
}

// ---------------------------------------------------------------------
// Per-link rules
// ---------------------------------------------------------------------

impl Lower<Tql> for SourceLower {
    /// Always lowers, possibly partially: an unlowerable leaf contributes
    /// `1` and clears `exact`.
    fn capability(&self, _s: &TqlLink, _rel: &Relation<Tql>) -> Capability {
        Capability::Yes
    }
    fn apply(
        &self,
        _s: &TqlLink,
        rel: Relation<Tql>,
        _cx: &LowerCx<'_, Tql>,
    ) -> Result<Relation<Tql>, PlanError> {
        Ok(rel)
    }
    /// The seed is always applied, so there is no residual case: the
    /// effect is the identity, and this row asserts that rather than
    /// leaving the exemption silent.
    fn residual_effect(&self, _s: &TqlLink, rel: Relation<Tql>) -> Relation<Tql> {
        rel
    }
    /// **A regex leaf is never `Equivalent`** — see [`StrOpKind::fidelity`],
    /// which this delegates to for every leaf.
    fn fidelity(&self, s: &TqlLink, _rel: &Relation<Tql>) -> Fidelity {
        match s {
            TqlLink::Source(expr) => selector_fidelity(expr),
            _ => Fidelity::Wider,
        }
    }
}

impl Lower<Tql> for AggregateLower {
    /// `exact` **and** `grouping.is_none()`.
    ///
    /// An aggregate over a superset is not merely wide, it is wrong:
    /// `max()` can exceed the true maximum and admit a trace that should
    /// not match, `min()` errs the other way, and `count()` inflates.
    /// Measured, the cost of getting this wrong is 333 qualifying traces
    /// becoming 1,000.
    fn capability(&self, _s: &TqlLink, rel: &Relation<Tql>) -> Capability {
        if rel.shape != TqlShape::Spans {
            return Capability::No(BlockReason::ShapeMismatch);
        }
        if !rel.exact {
            return Capability::No(BlockReason::NotExact);
        }
        if rel.grouping.is_some() {
            return Capability::No(BlockReason::ShapeMismatch);
        }
        Capability::No(BlockReason::NotYetLowered)
    }
    fn apply(
        &self,
        _s: &TqlLink,
        rel: Relation<Tql>,
        _cx: &LowerCx<'_, Tql>,
    ) -> Result<Relation<Tql>, PlanError> {
        Ok(rel)
    }
    /// **Shape unchanged** — whatever the fold has accumulated, not reset
    /// to `Spans`; **clears `exact`**, because the evaluator will drop
    /// traces the SQL returned.
    fn residual_effect(&self, _s: &TqlLink, mut rel: Relation<Tql>) -> Relation<Tql> {
        rel.exact = false;
        rel
    }
}

impl Lower<Tql> for ByLower {
    fn capability(&self, _s: &TqlLink, rel: &Relation<Tql>) -> Capability {
        if rel.shape != TqlShape::Spans {
            return Capability::No(BlockReason::ShapeMismatch);
        }
        if !rel.exact {
            return Capability::No(BlockReason::NotExact);
        }
        if rel.grouping.is_some() {
            return Capability::No(BlockReason::ShapeMismatch);
        }
        Capability::No(BlockReason::NotYetLowered)
    }
    fn apply(
        &self,
        _s: &TqlLink,
        rel: Relation<Tql>,
        _cx: &LowerCx<'_, Tql>,
    ) -> Result<Relation<Tql>, PlanError> {
        Ok(rel)
    }
    /// **Shape unchanged**; clears `exact`; records the key as an
    /// evaluator-owned group consumer, so a later `Aggregate` refuses.
    fn residual_effect(&self, s: &TqlLink, mut rel: Relation<Tql>) -> Relation<Tql> {
        if let TqlLink::Pipe(PipelineStage::By { key }) = s {
            rel.cols
                .set_provenance(&Name::new(key.to_string()), Provenance::EvaluatorOnly);
        }
        rel.exact = false;
        rel
    }
}

impl Lower<Tql> for CoalesceLower {
    /// Two rows in one dispatcher, and one rule rather than two special
    /// cases: with a preceding `By` the grouping slot is occupied and
    /// lowering means WRAPPING; with none it is the identity and costs
    /// nothing.
    fn capability(&self, _s: &TqlLink, rel: &Relation<Tql>) -> Capability {
        if rel.grouping.is_none() {
            // The identity: it contributes no SQL and cannot fail.
            return Capability::Yes;
        }
        Capability::No(BlockReason::NotYetLowered)
    }
    fn apply(
        &self,
        _s: &TqlLink,
        rel: Relation<Tql>,
        _cx: &LowerCx<'_, Tql>,
    ) -> Result<Relation<Tql>, PlanError> {
        Ok(rel)
    }
    /// Grouped: shape unchanged, clears `exact`. Ungrouped: the identity,
    /// and this dispatcher is where that exemption is checked rather than
    /// assumed.
    fn residual_effect(&self, _s: &TqlLink, mut rel: Relation<Tql>) -> Relation<Tql> {
        if rel.grouping.is_some() {
            rel.exact = false;
        }
        rel
    }
    fn fidelity(&self, _s: &TqlLink, _rel: &Relation<Tql>) -> Fidelity {
        Fidelity::Equivalent
    }
}

impl Lower<Tql> for SelectLower {
    /// **No exactness precondition** — projecting a column onto rows the
    /// evaluator will drop is harmless.
    fn capability(&self, s: &TqlLink, rel: &Relation<Tql>) -> Capability {
        if let TqlLink::Pipe(PipelineStage::Select { fields }) = s {
            for f in fields {
                if rel.cols.resolve(&Name::new(f.to_string())).is_none() {
                    return Capability::No(BlockReason::NameNotResolvable);
                }
            }
        }
        Capability::No(BlockReason::NotYetLowered)
    }
    fn apply(
        &self,
        _s: &TqlLink,
        rel: Relation<Tql>,
        _cx: &LowerCx<'_, Tql>,
    ) -> Result<Relation<Tql>, PlanError> {
        Ok(rel)
    }
    /// `cols` unchanged — no existing column moves — and the selected
    /// fields become an evaluator-owned projection.
    fn residual_effect(&self, s: &TqlLink, mut rel: Relation<Tql>) -> Relation<Tql> {
        if let TqlLink::Pipe(PipelineStage::Select { fields }) = s {
            for f in fields {
                rel.cols
                    .set_provenance(&Name::new(f.to_string()), Provenance::EvaluatorOnly);
            }
        }
        rel
    }
}

impl Lower<Tql> for NotASearchLinkLower {
    /// Unreachable by construction: the chain builder never makes one,
    /// because the shipped planner answers `400` for all three metrics
    /// stages on the search route. Classified `Never` so that nobody
    /// later reads it as unfinished work.
    fn capability(&self, _s: &TqlLink, _rel: &Relation<Tql>) -> Capability {
        Capability::Never(NeverReason::NotASearchLink)
    }
    fn apply(
        &self,
        _s: &TqlLink,
        rel: Relation<Tql>,
        _cx: &LowerCx<'_, Tql>,
    ) -> Result<Relation<Tql>, PlanError> {
        Ok(rel)
    }
    fn residual_effect(&self, _s: &TqlLink, rel: Relation<Tql>) -> Relation<Tql> {
        rel
    }
}

impl Lower<Tql> for OrderLower {
    /// `exact` — and this is the part easy to miss. The TraceQL sort key
    /// is `max(matched-span timestamp)`, so over a superset the **order**
    /// is wrong, not just the set. LogQL's `Order` does NOT inherit this
    /// precondition, which is why it is per-link rather than global.
    fn capability(&self, _s: &TqlLink, rel: &Relation<Tql>) -> Capability {
        if !rel.exact {
            return Capability::No(BlockReason::NotExact);
        }
        Capability::No(BlockReason::NotYetLowered)
    }
    fn apply(
        &self,
        _s: &TqlLink,
        rel: Relation<Tql>,
        _cx: &LowerCx<'_, Tql>,
    ) -> Result<Relation<Tql>, PlanError> {
        Ok(rel)
    }
    fn residual_effect(&self, _s: &TqlLink, mut rel: Relation<Tql>) -> Relation<Tql> {
        rel.ordering = None;
        rel
    }
}

impl Lower<Tql> for LimitLower {
    fn capability(&self, _s: &TqlLink, rel: &Relation<Tql>) -> Capability {
        if rel.ordering.is_none() {
            return Capability::No(BlockReason::OrderingNotEstablished);
        }
        Capability::No(BlockReason::NotYetLowered)
    }
    fn apply(
        &self,
        s: &TqlLink,
        mut rel: Relation<Tql>,
        _cx: &LowerCx<'_, Tql>,
    ) -> Result<Relation<Tql>, PlanError> {
        if let TqlLink::Limit(n) = s {
            rel.limit = Some(u64::from(*n));
        }
        Ok(rel)
    }
    fn residual_effect(&self, _s: &TqlLink, mut rel: Relation<Tql>) -> Relation<Tql> {
        rel.limit = None;
        rel
    }
}

impl Lower<Tql> for EmitLower {
    fn capability(&self, _s: &TqlLink, _rel: &Relation<Tql>) -> Capability {
        Capability::Never(NeverReason::NeedsUnwindowedRootRead)
    }
    fn apply(
        &self,
        _s: &TqlLink,
        rel: Relation<Tql>,
        _cx: &LowerCx<'_, Tql>,
    ) -> Result<Relation<Tql>, PlanError> {
        Ok(rel)
    }
    /// Records the winners' root read as the evaluator's — and the way
    /// the evaluator owns it is to send a second statement, which is what
    /// the plan builder turns this into.
    fn residual_effect(&self, _s: &TqlLink, mut rel: Relation<Tql>) -> Relation<Tql> {
        rel.cols
            .set_provenance(&Name::from("root"), Provenance::EvaluatorOnly);
        rel
    }
}

/// The seed relation a TraceQL chain folds from.
pub fn seed_relation(source: SourceRef) -> Relation<Tql> {
    Relation {
        source: SourceTerm::Base(TqlSource(source)),
        predicate: Pred::True,
        projection: vec![(Name::from(TRACE_ID), TRACE_ID.to_string())],
        cols: ColSet::Closed(vec![
            Col {
                name: Name::from(TRACE_ID),
                provenance: Provenance::Stored,
            },
            Col {
                name: Name::from("name"),
                provenance: Provenance::Stored,
            },
        ]),
        grouping: None,
        ordering: None,
        limit: None,
        shape: TqlShape::Spans,
        exact: true,
        depth: 0,
    }
}

// ---------------------------------------------------------------------
// Gates
// ---------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::super::search_plan::StrOp;
    use super::*;
    use crate::compile::fold::{Grouping, Ordering, SortDir, SqlExpr};
    use crate::compile::testkit::{EffectRow, assert_every_residual_state_effect};
    use pulsus_traceql::{AggregateOp, Field, Value};

    fn parse_selector(q: &str) -> SpansetExpr {
        pulsus_traceql::parse(q)
            .unwrap_or_else(|e| panic!("{q}: {e}"))
            .spanset
    }

    fn pipe(q: &str) -> PipelineStage {
        pulsus_traceql::parse(q)
            .unwrap_or_else(|e| panic!("{q}: {e}"))
            .pipeline
            .into_iter()
            .next()
            .unwrap_or_else(|| panic!("{q} has no pipeline stage"))
    }

    /// The search planner's compiled string comparison, classified.
    /// **No `_` arm**: adding a `StrOp` variant fails to build this
    /// binary, which `cargo test --workspace` builds.
    fn kind_of(op: &StrOp) -> StrOpKind {
        match op {
            StrOp::Eq => StrOpKind::Eq,
            StrOp::Neq => StrOpKind::Neq,
            StrOp::Re(_) => StrOpKind::Re,
            StrOp::Nre(_) => StrOpKind::Nre,
        }
    }

    /// Issue #492 acceptance criterion 12, hermetic half: **no regex leaf
    /// claims `Fidelity::Equivalent`.**
    ///
    /// The `match` in [`kind_of`] has no `_` arm, so a new `StrOp`
    /// variant fails to build this test rather than slipping through
    /// unclassified. The two regex arms assert the LITERAL
    /// `Fidelity::Wider` rather than a value the rule under test
    /// produced.
    #[test]
    fn no_regex_leaf_claims_equivalent_fidelity() {
        let re = pulsus_re2::compile_user_regex_anchored(r"\d").expect("test pattern compiles");
        for op in [StrOp::Eq, StrOp::Neq, StrOp::Re(re.clone()), StrOp::Nre(re)] {
            let kind = kind_of(&op);
            match kind {
                StrOpKind::Eq | StrOpKind::Neq => {
                    assert_eq!(kind.fidelity(), Fidelity::Equivalent, "{kind:?}");
                }
                StrOpKind::Re | StrOpKind::Nre => {
                    assert_eq!(
                        kind.fidelity(),
                        Fidelity::Wider,
                        "{kind:?}: a regex leaf is read by TWO engines with different dialects, \
                         and the SQL reading is the NARROWER one — claiming equivalence would \
                         invert the lattice invariant and DROP rows"
                    );
                }
            }
        }

        // The same rule as the selector link sees it: a `=~` anywhere in
        // the selector makes the whole link `Wider`.
        for (q, want) in [
            (
                r#"{ resource.service.name = "checkout" }"#,
                Fidelity::Equivalent,
            ),
            (r#"{ name =~ "\\d" }"#, Fidelity::Wider),
            (r#"{ name !~ "\\d" }"#, Fidelity::Wider),
            (
                r#"{ resource.service.name = "checkout" && name =~ "a.*" }"#,
                Fidelity::Wider,
            ),
            (
                r#"{ resource.service.name = "checkout" || span.foo =~ "b" }"#,
                Fidelity::Wider,
            ),
        ] {
            assert_eq!(selector_fidelity(&parse_selector(q)), want, "{q}");
        }

        // And through the fold, which is where the verdict has its
        // consequence: a `Wider` link clears `exact`, so no later link
        // that needs exactness can lower behind a regex leaf.
        let bounds = crate::compile::fold::RequestBounds {
            start_ns: 0,
            end_ns: 1,
            step_ns: None,
            limit: Some(20),
        };
        let cx = LowerCx::<Tql>::new(&bounds);
        for (q, want_exact) in [
            (r#"{ resource.service.name = "checkout" }"#, true),
            (r#"{ name =~ "\\d" }"#, false),
        ] {
            let chain = vec![TqlLink::Source(Box::new(parse_selector(q)))];
            let lowering =
                crate::compile::fold::lower_chain::<Tql>(&chain, seed_relation(TRACE_SPANS), &cx)
                    .expect("fold");
            assert_eq!(lowering.rel.exact, want_exact, "{q}");
        }
    }

    // --- the residual state effects -----------------------------------

    fn base(shape: TqlShape) -> Relation<Tql> {
        let mut rel = seed_relation(TRACE_SPANS);
        rel.shape = shape;
        rel
    }

    fn grouped(mut rel: Relation<Tql>) -> Relation<Tql> {
        rel.grouping = Some(Grouping {
            keys: vec!["name".to_string()],
        });
        rel
    }

    fn ordered(mut rel: Relation<Tql>, key: &str) -> Relation<Tql> {
        rel.ordering = Some(Ordering {
            keys: vec![(key.to_string(), SortDir::Desc)],
        });
        rel
    }

    fn limited(mut rel: Relation<Tql>, n: u64) -> Relation<Tql> {
        rel.limit = Some(n);
        rel
    }

    fn evaluator_owned(mut rel: Relation<Tql>, name: &str) -> Relation<Tql> {
        rel.cols
            .set_provenance(&Name::new(name), Provenance::EvaluatorOnly);
        rel
    }

    fn not_exact(mut rel: Relation<Tql>) -> Relation<Tql> {
        rel.exact = false;
        rel
    }

    /// Issue #492: every TraceQL link's residual state effect is the one
    /// the design record states, and the two whose stated effect is
    /// *none* assert that the effect IS the identity — so the exemption
    /// is itself a check rather than a silence.
    ///
    /// **Nine rows**: seven links with a stated effect (`Aggregate`,
    /// `By`, grouped `Coalesce`, `Select`, `Order`, `Limit`, `Emit`) and
    /// two whose effect is none (`Source`, and `Coalesce` with no
    /// preceding `By`).
    #[test]
    fn every_residual_state_effect_is_the_one_the_document_states() {
        let sel = parse_selector(r#"{ resource.service.name = "checkout" }"#);
        let agg = pipe(r#"{ .a = "1" } | max(duration) > 1s"#);
        let by = pipe(r#"{ .a = "1" } | by(name)"#);
        let coalesce = pipe(r#"{ .a = "1" } | coalesce()"#);
        let select = pipe(r#"{ .a = "1" } | select(span.http.method)"#);

        // Two seeds per row. They differ in `shape` and in every field
        // the row's effect column names as unchanged or retained; where
        // the effect is only observable on a field with a particular
        // value, BOTH seeds carry that value, so assertion 4 can see the
        // effect on each.
        let rows: Vec<EffectRow<Tql>> = vec![
            EffectRow {
                name: "Source",
                link: TqlLink::Source(Box::new(sel)),
                s1: base(TqlShape::Spans),
                s2: base(TqlShape::Traces),
                e1: base(TqlShape::Spans),
                e2: base(TqlShape::Traces),
                effect_is_constant: false,
                has_effect: false,
            },
            EffectRow {
                name: "Aggregate",
                link: TqlLink::Pipe(agg),
                s1: base(TqlShape::Spans),
                s2: base(TqlShape::Traces),
                e1: not_exact(base(TqlShape::Spans)),
                e2: not_exact(base(TqlShape::Traces)),
                effect_is_constant: false,
                has_effect: true,
            },
            EffectRow {
                name: "By",
                link: TqlLink::Pipe(by),
                s1: base(TqlShape::Spans),
                s2: base(TqlShape::Traces),
                e1: not_exact(evaluator_owned(base(TqlShape::Spans), "name")),
                e2: not_exact(evaluator_owned(base(TqlShape::Traces), "name")),
                effect_is_constant: false,
                has_effect: true,
            },
            EffectRow {
                name: "Coalesce, after a By",
                link: TqlLink::Pipe(coalesce.clone()),
                s1: grouped(base(TqlShape::Spans)),
                s2: grouped(base(TqlShape::Groups(Name::from("name")))),
                e1: not_exact(grouped(base(TqlShape::Spans))),
                e2: not_exact(grouped(base(TqlShape::Groups(Name::from("name"))))),
                effect_is_constant: false,
                has_effect: true,
            },
            EffectRow {
                name: "Coalesce, with no preceding By",
                link: TqlLink::Pipe(coalesce),
                s1: base(TqlShape::Spans),
                s2: base(TqlShape::Traces),
                e1: base(TqlShape::Spans),
                e2: base(TqlShape::Traces),
                effect_is_constant: false,
                has_effect: false,
            },
            EffectRow {
                name: "Select",
                link: TqlLink::Pipe(select),
                s1: base(TqlShape::Spans),
                s2: base(TqlShape::Traces),
                e1: evaluator_owned(base(TqlShape::Spans), "span.http.method"),
                e2: evaluator_owned(base(TqlShape::Traces), "span.http.method"),
                effect_is_constant: false,
                has_effect: true,
            },
            EffectRow {
                name: "Order",
                link: TqlLink::Order,
                s1: ordered(base(TqlShape::Spans), "bound_ts"),
                s2: ordered(base(TqlShape::Traces), "trace_id"),
                e1: base(TqlShape::Spans),
                e2: base(TqlShape::Traces),
                effect_is_constant: false,
                has_effect: true,
            },
            EffectRow {
                name: "Limit",
                link: TqlLink::Limit(20),
                s1: limited(base(TqlShape::Spans), 20),
                s2: limited(base(TqlShape::Traces), 21),
                e1: base(TqlShape::Spans),
                e2: base(TqlShape::Traces),
                effect_is_constant: false,
                has_effect: true,
            },
            EffectRow {
                name: "Emit",
                link: TqlLink::Emit,
                s1: base(TqlShape::Spans),
                s2: base(TqlShape::Traces),
                e1: evaluator_owned(base(TqlShape::Spans), "root"),
                e2: evaluator_owned(base(TqlShape::Traces), "root"),
                effect_is_constant: false,
                has_effect: true,
            },
        ];
        assert_every_residual_state_effect::<Tql>(&rows, 9);
    }

    /// `Emit` is `Never`, AND the plan builder gives it its own SQL part
    /// rather than folding it into the engine part. Those are two
    /// different claims and the second is the one the previous shape of
    /// this design got wrong.
    #[test]
    fn emit_is_never_and_is_served_by_its_own_sql_part() {
        let bounds = crate::compile::fold::RequestBounds {
            start_ns: 0,
            end_ns: 1,
            step_ns: None,
            limit: Some(20),
        };
        let chain = vec![
            TqlLink::Source(Box::new(parse_selector(
                r#"{ resource.service.name = "checkout" }"#,
            ))),
            TqlLink::Order,
            TqlLink::Limit(20),
            TqlLink::Emit,
        ];
        let cx = LowerCx::<Tql>::new(&bounds);
        let lowering =
            crate::compile::fold::lower_chain::<Tql>(&chain, seed_relation(TRACE_SPANS), &cx)
                .expect("fold");
        assert_eq!(
            lowering.how[3],
            crate::compile::fold::Disposition::Residual(
                crate::compile::fold::ResidualReason::Never(NeverReason::NeedsUnwindowedRootRead)
            )
        );

        let config = crate::compile::plan::PlanConfig::default();
        let plan = crate::compile::plan::plan_of::<Tql>(
            &chain,
            lowering,
            &PlanCx {
                bounds: &bounds,
                config: &config,
            },
        )
        .expect("plan");
        let emit_part = plan.links[3].part;
        let crate::compile::plan::Part::Sql(p) = &plan.parts[emit_part] else {
            panic!(
                "Emit must land in an SQL part, not an engine part: {:?}",
                plan.parts
            )
        };
        assert_eq!(
            p.cut,
            Some(crate::compile::plan::Cut::SourceHandoff {
                source: TRACE_SPANS_ROOT,
                key: Name::from(TRACE_ID),
            })
        );
        assert_eq!(
            p.seed.as_ref().map(|s| s.bound),
            Some(SeedBound::RequestLimit(20)),
            "seeded by the winners' trace ids, bounded by the request limit"
        );
        assert_eq!(p.issue, crate::compile::plan::Issue::Once);
    }

    /// A physical `=` leaf still classifies through the same rule, so the
    /// `Equivalent` half of `StrOpKind::fidelity` is exercised by
    /// something other than its own arm list.
    #[test]
    fn a_string_equality_leaf_is_equivalent_and_a_regex_one_is_not() {
        let re = pulsus_re2::compile_user_regex_anchored("a.*").expect("compiles");
        assert_eq!(kind_of(&StrOp::Eq).fidelity(), Fidelity::Equivalent);
        assert_eq!(
            kind_of(&StrOp::Re(re)).fidelity(),
            Fidelity::Wider,
            "the two engines read the pattern differently"
        );
    }

    /// Unused-import guard for the two AST types the row table names.
    #[test]
    fn the_row_table_names_real_ast_shapes() {
        let agg = pipe(r#"{ .a = "1" } | max(duration) > 1s"#);
        let PipelineStage::Aggregate { op, value, .. } = &agg else {
            panic!("{agg:?}")
        };
        assert_eq!(*op, AggregateOp::Max);
        assert!(matches!(value, Value::Duration(_)));
        let sel = pipe(r#"{ .a = "1" } | select(span.http.method)"#);
        let PipelineStage::Select { fields } = &sel else {
            panic!("{sel:?}")
        };
        let f: &Field = &fields[0];
        assert_eq!(f.to_string(), "span.http.method");
        let _ = SqlExpr::new("unused-in-production");
    }
}
