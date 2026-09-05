//! LogQL against the shared compile core (issue #492, wave 1).
//!
//! **Compiled and unwired.** [`super::plan`] does not call anything here,
//! so no LogQL statement moves: what this module buys is the second
//! language the core has to fit, made a compiler result rather than a
//! reading.
//!
//! **LogQL's chain link is not its AST stage enum.** `pulsus_logql::Stage`
//! has ten variants and carries **none** of the window, the range
//! aggregation, the vector aggregation, the ordering, the limit or the
//! response builder — while `Unwrap` **is** one of the ten, because the
//! parser represents `| unwrap …` as an ordered stage inside the
//! pipeline and always leaves `LogRange::unwrap` `None`. So `Unwrap`
//! reaches the chain through [`LqlLink::Pipe`], in its written position,
//! which is the whole reason the parser puts it there — and a separate
//! `Unwrap` link would be a second spelling of one construct.
//!
//! **What lowers here is exactly what lowers today.** The selector, and a
//! pushable line filter over a stored line. Every other link answers
//! [`Capability::No`] with the reason the design record's link table
//! states, which is what makes the three walk-agreement gates in
//! [`super::plan`]'s test module reproduce the shipped functions rather
//! than a larger lowerable set nobody has written the SQL for.

use pulsus_logql::{
    DropKeepElem, LabelFilterExpr, LabelFmt, ParserStage, RangeAggOp, Stage, VectorAggOp,
};

use crate::compile::fold::{
    BlockReason, Capability, Col, ColSet, Fidelity, Lang, Lower, LowerCx, Name, NeverReason, Pred,
    Provenance, Relation, Shape, SourceName, SqlExpr,
};
use crate::compile::plan::{HandoffCost, SourceRef};

// ---------------------------------------------------------------------
// The language's own types
// ---------------------------------------------------------------------

/// The three tables a LogQL read walks, in the order it walks them.
pub const LOG_STREAMS_IDX: SourceRef = SourceRef("log_streams_idx");
pub const LOG_STREAMS: SourceRef = SourceRef("log_streams");
pub const LOG_SAMPLES: SourceRef = SourceRef("log_samples");

/// The name a fingerprint handoff is keyed on.
pub const FINGERPRINT: &str = "fingerprint";

/// The stored line. `body`'s provenance is what makes a line filter after
/// a line-rewriting stage residual, and it is the whole of that rule.
pub const BODY: &str = "body";

/// One structured-metadata key, and the expression that reads it.
///
/// `log_samples.structured_metadata` is a `String` holding flat JSON
/// (schema migration id 21, `crates/pulsus-schema/src/catalog.rs`), so a
/// key inside it is a name that is not a column and resolves to an
/// extraction over one — the same shape the shipped metrics read path
/// already renders for `labels`
/// (`crates/pulsus-read/src/metrics/series_where.rs:333`).
///
/// **Why the seed carries one at all.** Without a resolvable
/// structured-metadata name, [`LabelFilterLower::capability`] refuses
/// every label filter at its name-resolution guard and never reaches the
/// rule under it, so a change to that rule is invisible to every gate —
/// a fixture that cannot fail on the input it is given (issue #492, code
/// review round 19). With this column present, `| level="error"` resolves
/// and the answer comes from the rule.
///
/// **It moves no SQL.** No label filter lowers in wave 1, so the
/// expression is never emitted; the walk-agreement gates measure that.
pub const LEVEL: &str = "level";
pub const LEVEL_EXPR: &str = "JSONExtractString(structured_metadata, 'level')";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LqlSource(pub SourceRef);

impl SourceName for LqlSource {
    fn source_ref(&self) -> SourceRef {
        self.0
    }

    fn named(s: SourceRef) -> Self {
        LqlSource(s)
    }
}

/// LogQL's shapes. Not a shared enum: one enum over both languages would
/// be a union with per-language invalid states.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LqlShape {
    Lines,
    Samples,
    Series,
}

impl Shape for LqlShape {}

/// What crosses between two LogQL parts: the resolved fingerprint list,
/// bounded by `DEFAULT_MAX_STREAMS`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LqlHandoff(pub Vec<u64>);

/// LogQL's chain link.
///
/// `param` is `Option<String>` and not `Option<f64>` because the AST
/// keeps the `quantile_over_time` and `topk` parameters as **raw text** so
/// it can derive `Eq`/`Hash`; parsing to `f64` is the planner's job, and
/// doing it in the link would move a parse error out of the one place
/// that reports it (`parse_vector_agg_params`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LqlLink {
    /// The stream selector, lowered by the predicate lattice rather than
    /// by the stage fold.
    Source,
    Pipe(Stage),
    Window {
        range_ns: i64,
        step_ns: i64,
        offset_ns: i64,
        grid_start_ns: i64,
    },
    RangeAgg {
        op: RangeAggOp,
        grouping: Option<pulsus_logql::Grouping>,
        param: Option<String>,
    },
    VectorAgg {
        op: VectorAggOp,
        grouping: Option<pulsus_logql::Grouping>,
        param: Option<String>,
    },
    LabelReplace {
        dst: String,
        replacement: String,
        src: String,
        regex: String,
    },
    Order,
    Limit(u32),
    Emit,
}

/// The marker the core is generic over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lql;

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
    LineFilterLower,
    ParserLower,
    LabelFilterLower,
    LineFormatLower,
    LabelFormatLower,
    UnwrapLower,
    UnpackLower,
    DecolorizeLower,
    DropLower,
    KeepLower,
    WindowLower,
    RangeAggLower,
    VectorAggLower,
    LabelReplaceLower,
    OrderLower,
    LimitLower,
    EmitLower,
);

static SOURCE: SourceLower = SourceLower;
static LINE_FILTER: LineFilterLower = LineFilterLower;
static PARSER: ParserLower = ParserLower;
static LABEL_FILTER: LabelFilterLower = LabelFilterLower;
static LINE_FORMAT: LineFormatLower = LineFormatLower;
static LABEL_FORMAT: LabelFormatLower = LabelFormatLower;
static UNWRAP: UnwrapLower = UnwrapLower;
static UNPACK: UnpackLower = UnpackLower;
static DECOLORIZE: DecolorizeLower = DecolorizeLower;
static DROP: DropLower = DropLower;
static KEEP: KeepLower = KeepLower;
static WINDOW: WindowLower = WindowLower;
static RANGE_AGG: RangeAggLower = RangeAggLower;
static VECTOR_AGG: VectorAggLower = VectorAggLower;
static LABEL_REPLACE: LabelReplaceLower = LabelReplaceLower;
static ORDER: OrderLower = OrderLower;
static LIMIT: LimitLower = LimitLower;
static EMIT: EmitLower = EmitLower;

impl Lang for Lql {
    type Stage = LqlLink;
    type Source = LqlSource;
    type ColExpr = String;
    type Shape = LqlShape;
    type Handoff = LqlHandoff;
    type Err = super::ReadError;

    /// The ONE exhaustive match over the chain-link type. **No `_` arm**:
    /// adding an `LqlLink` variant, or a `Stage` variant, fails to
    /// compile here.
    fn lower_of(stage: &LqlLink) -> &'static dyn Lower<Lql> {
        match stage {
            LqlLink::Source => &SOURCE,
            LqlLink::Pipe(s) => match s {
                Stage::LineFilter(_) => &LINE_FILTER,
                // The four parser forms share one dispatcher because the
                // design record states one rule for all four; each still
                // has its own row in the residual-effect gate.
                Stage::Parser(ParserStage::Json { .. })
                | Stage::Parser(ParserStage::Logfmt { .. })
                | Stage::Parser(ParserStage::Regexp(_))
                | Stage::Parser(ParserStage::Pattern(_)) => &PARSER,
                Stage::LabelFilter(_) => &LABEL_FILTER,
                Stage::LineFormat(_) => &LINE_FORMAT,
                Stage::LabelFormat(_) => &LABEL_FORMAT,
                Stage::Unwrap(_) => &UNWRAP,
                Stage::Unpack => &UNPACK,
                Stage::Decolorize => &DECOLORIZE,
                Stage::Drop(_) => &DROP,
                Stage::Keep(_) => &KEEP,
            },
            LqlLink::Window { .. } => &WINDOW,
            LqlLink::RangeAgg { .. } => &RANGE_AGG,
            LqlLink::VectorAgg { .. } => &VECTOR_AGG,
            LqlLink::LabelReplace { .. } => &LABEL_REPLACE,
            LqlLink::Order => &ORDER,
            LqlLink::Limit(_) => &LIMIT,
            LqlLink::Emit => &EMIT,
        }
    }

    /// **No LogQL link hands off to another source in wave 1**, and that
    /// is read off the design record's own link table rather than
    /// decided here: every LogQL row's continuation column says *none*,
    /// including `Emit`'s — *"the response is built from rows the last
    /// SQL part already returned"* — and the only continuation the table
    /// gives a LogQL link is the `Limit` row's inexact-limit page loop,
    /// which is not a source handoff.
    ///
    /// The design's prose separately names LogQL's three-statement
    /// structure (`log_streams_idx` → `log_streams` / `log_samples`,
    /// three statements and two cuts) as a shipped instance of the
    /// source-handoff cut. **It does not say which links carry those two
    /// cuts**, and inventing an assignment here would put a claim in the
    /// code that no row of the table supports. Flagged rather than
    /// guessed; the TraceQL side, whose `Emit` row states its
    /// continuation explicitly, is implemented.
    fn source_of(_stage: &LqlLink, rel: &Relation<Lql>) -> SourceRef {
        rel.source_ref()
    }

    /// A fingerprint renders as an unsigned decimal inside an `IN (…)`
    /// list: at most 20 digits plus `", "`, and one AST element per
    /// literal. The 32-byte constant is the `fingerprint IN ()` frame.
    fn handoff_cost(n: u64) -> HandoffCost {
        HandoffCost {
            text_bytes: 32 + n * 22,
            ast_elements: 4 + n,
        }
    }
}

// ---------------------------------------------------------------------
// Per-link rules
// ---------------------------------------------------------------------

/// The line's provenance, or `Stored` when the set has not recorded one.
fn body_provenance(rel: &Relation<Lql>) -> Provenance {
    rel.cols
        .provenance(&Name::from(BODY))
        .cloned()
        .unwrap_or(Provenance::Stored)
}

/// Marks the line as rewritten by a stage whose SQL form has not been
/// written, so a later link that reads the line finds nothing to lower
/// against.
///
/// **This is the one place wave 1 departs from the design record's §7.1
/// rows for `Decolorize`/`Unpack`**, which state `Computed(expr)`. The
/// expression does not exist yet: nothing renders the SGR strip or the
/// `_entry` unwrap into SQL, and a `Computed(expr)` with no expression
/// would make a following line filter lower against something that is
/// never emitted — which moves the measured zero the three walk-agreement
/// gates assert. When the wave that writes those expressions lands, this
/// becomes `Computed(expr)` and the zero moves deliberately.
fn mark_line_rewritten(mut rel: Relation<Lql>) -> Relation<Lql> {
    rel.cols
        .set_provenance(&Name::from(BODY), Provenance::EvaluatorOnly);
    rel
}

impl Lower<Lql> for SourceLower {
    fn capability(&self, _s: &LqlLink, _rel: &Relation<Lql>) -> Capability {
        Capability::Yes
    }
    fn apply(
        &self,
        _s: &LqlLink,
        rel: Relation<Lql>,
        _cx: &LowerCx<'_, Lql>,
    ) -> Result<Relation<Lql>, super::ReadError> {
        Ok(rel)
    }
    /// The seed is always applied, so there is no residual case.
    fn residual_effect(&self, _s: &LqlLink, rel: Relation<Lql>) -> Relation<Lql> {
        rel
    }
    fn fidelity(&self, _s: &LqlLink, _rel: &Relation<Lql>) -> Fidelity {
        Fidelity::Equivalent
    }
}

impl Lower<Lql> for LineFilterLower {
    /// Two conditions, and both are the shipped rule rather than a new
    /// one: the line must resolve to a SQL expression, and the filter
    /// must be one [`super::plan::is_pushable_line_filter`] accepts — an
    /// `ip()` alternative renders no predicate the body skip indexes
    /// could prune with.
    fn capability(&self, s: &LqlLink, rel: &Relation<Lql>) -> Capability {
        let LqlLink::Pipe(Stage::LineFilter(lf)) = s else {
            return Capability::No(BlockReason::NotYetLowered);
        };
        if body_provenance(rel).resolve(&Name::from(BODY)).is_none() {
            return Capability::No(BlockReason::BodyNotStored);
        }
        if !super::plan::is_pushable_line_filter(lf) {
            return Capability::No(BlockReason::NotPushable);
        }
        Capability::Yes
    }
    fn apply(
        &self,
        s: &LqlLink,
        mut rel: Relation<Lql>,
        _cx: &LowerCx<'_, Lql>,
    ) -> Result<Relation<Lql>, super::ReadError> {
        let LqlLink::Pipe(Stage::LineFilter(lf)) = s else {
            return Ok(rel);
        };
        let fragment = super::predicate::line_filter(lf)?;
        rel.predicate = rel
            .predicate
            .clone()
            .and(Pred::leaf(fragment.as_sql(), rel.source_ref()));
        Ok(rel)
    }
    /// It removes lines in the evaluator, so the SQL result is a
    /// superset.
    fn residual_effect(&self, _s: &LqlLink, mut rel: Relation<Lql>) -> Relation<Lql> {
        rel.exact = false;
        rel
    }
    /// A pushed line filter is a substring or regex search over the same
    /// line the evaluator reads, with no guard term.
    fn fidelity(&self, _s: &LqlLink, _rel: &Relation<Lql>) -> Fidelity {
        Fidelity::Equivalent
    }
}

impl Lower<Lql> for ParserLower {
    fn capability(&self, _s: &LqlLink, rel: &Relation<Lql>) -> Capability {
        if body_provenance(rel).resolve(&Name::from(BODY)).is_none() {
            return Capability::No(BlockReason::BodyNotStored);
        }
        Capability::No(BlockReason::NotYetLowered)
    }
    fn apply(
        &self,
        _s: &LqlLink,
        rel: Relation<Lql>,
        _cx: &LowerCx<'_, Lql>,
    ) -> Result<Relation<Lql>, super::ReadError> {
        Ok(rel)
    }
    /// `cols` is still widened, but with an EVALUATOR-ONLY open source:
    /// a following label filter goes residual instead of lowering against
    /// a name SQL cannot see. It does **not** clear `exact` — a parse
    /// failure keeps the line with an `__error__` label, so a parser
    /// removes no line.
    fn residual_effect(&self, s: &LqlLink, mut rel: Relation<Lql>) -> Relation<Lql> {
        let id = match s {
            LqlLink::Pipe(Stage::Parser(ParserStage::Json { .. })) => "parser:json",
            LqlLink::Pipe(Stage::Parser(ParserStage::Logfmt { .. })) => "parser:logfmt",
            LqlLink::Pipe(Stage::Parser(ParserStage::Regexp(_))) => "parser:regexp",
            LqlLink::Pipe(Stage::Parser(ParserStage::Pattern(_))) => "parser:pattern",
            _ => "parser",
        };
        rel.cols = rel.cols.widen(std::rc::Rc::new(EvaluatorOnlyLabels(
            crate::compile::fold::OpenSourceId(id),
        )));
        rel
    }
}

/// An open column source that resolves nothing: the names a residual
/// parser produced exist, and SQL cannot see any of them.
#[derive(Debug)]
pub struct EvaluatorOnlyLabels(pub crate::compile::fold::OpenSourceId);

impl crate::compile::fold::OpenSource for EvaluatorOnlyLabels {
    fn resolve(&self, _name: &Name) -> Option<SqlExpr> {
        None
    }
    fn id(&self) -> crate::compile::fold::OpenSourceId {
        self.0
    }
}

impl Lower<Lql> for LabelFilterLower {
    /// Every referenced name must resolve in `cols`.
    ///
    /// **Wave 1 lowers no label filter at all**, and that is a deliberate
    /// stopping point rather than an oversight. The design record's
    /// fidelity section makes a filter over a structured-metadata key
    /// `Equivalent`, which would let the `Limit` link lower and turn the
    /// sample read from a page loop into one statement — a real
    /// improvement, and one that makes the model disagree with the
    /// shipped `has_unpushed_dropping_stage`, whose answer for *any*
    /// label filter is "drops lines in-engine". The three walk-agreement
    /// gates in [`super::plan`]'s test module assert that the model and
    /// the shipped walks agree exactly, so the two cannot both hold until
    /// the wave that moves the shipped planner with it.
    fn capability(&self, s: &LqlLink, rel: &Relation<Lql>) -> Capability {
        if let LqlLink::Pipe(Stage::LabelFilter(expr)) = s {
            let mut resolvable = true;
            pulsus_logql::for_each_label_filter(expr, |node: &LabelFilterExpr| {
                let name = match node {
                    LabelFilterExpr::Match(m) => Some(m.name.as_str()),
                    LabelFilterExpr::Compare { name, .. } | LabelFilterExpr::Ip { name, .. } => {
                        Some(name.as_str())
                    }
                    LabelFilterExpr::And(_, _) | LabelFilterExpr::Or(_, _) => None,
                };
                if let Some(name) = name
                    && rel.cols.resolve(&Name::from(name)).is_none()
                {
                    resolvable = false;
                }
            });
            if !resolvable {
                return Capability::No(BlockReason::NameNotResolvable);
            }
        }
        Capability::No(BlockReason::NotYetLowered)
    }
    fn apply(
        &self,
        _s: &LqlLink,
        rel: Relation<Lql>,
        _cx: &LowerCx<'_, Lql>,
    ) -> Result<Relation<Lql>, super::ReadError> {
        Ok(rel)
    }
    /// It drops lines in the evaluator.
    fn residual_effect(&self, _s: &LqlLink, mut rel: Relation<Lql>) -> Relation<Lql> {
        rel.exact = false;
        rel
    }
}

impl Lower<Lql> for LineFormatLower {
    /// A Go text/template has no SQL form here. `No`, not `Never`.
    fn capability(&self, _s: &LqlLink, _rel: &Relation<Lql>) -> Capability {
        Capability::No(BlockReason::NotYetLowered)
    }
    fn apply(
        &self,
        _s: &LqlLink,
        rel: Relation<Lql>,
        _cx: &LowerCx<'_, Lql>,
    ) -> Result<Relation<Lql>, super::ReadError> {
        Ok(rel)
    }
    /// Sets the line's provenance with NO resolvable expression, so every
    /// later link needing the line goes residual. It does **not** clear
    /// `exact` — it removes no line.
    fn residual_effect(&self, _s: &LqlLink, rel: Relation<Lql>) -> Relation<Lql> {
        mark_line_rewritten(rel)
    }
}

impl Lower<Lql> for LabelFormatLower {
    fn capability(&self, _s: &LqlLink, _rel: &Relation<Lql>) -> Capability {
        Capability::No(BlockReason::NotYetLowered)
    }
    fn apply(
        &self,
        _s: &LqlLink,
        rel: Relation<Lql>,
        _cx: &LowerCx<'_, Lql>,
    ) -> Result<Relation<Lql>, super::ReadError> {
        Ok(rel)
    }
    /// `cols` rewritten with evaluator-only provenance for each rewritten
    /// name; `exact` untouched.
    fn residual_effect(&self, s: &LqlLink, mut rel: Relation<Lql>) -> Relation<Lql> {
        if let LqlLink::Pipe(Stage::LabelFormat(fmts)) = s {
            for f in fmts {
                rel.cols
                    .set_provenance(&Name::new(label_fmt_dst(f)), Provenance::EvaluatorOnly);
            }
        }
        rel
    }
}

/// The destination label one `| label_format` element writes.
fn label_fmt_dst(f: &LabelFmt) -> String {
    match f {
        LabelFmt::Rename { dst, .. } | LabelFmt::Template { dst, .. } => dst.clone(),
    }
}

impl Lower<Lql> for UnwrapLower {
    fn capability(&self, _s: &LqlLink, _rel: &Relation<Lql>) -> Capability {
        Capability::No(BlockReason::NotYetLowered)
    }
    fn apply(
        &self,
        _s: &LqlLink,
        rel: Relation<Lql>,
        _cx: &LowerCx<'_, Lql>,
    ) -> Result<Relation<Lql>, super::ReadError> {
        Ok(rel)
    }
    /// **Shape unchanged** — stated as preservation like every other row,
    /// because a state rule that is only true by grace of a parser
    /// restriction (the parser refuses a second `unwrap`) breaks silently
    /// when the restriction moves. The sample source becomes
    /// evaluator-owned, which is what makes a following range
    /// aggregation, whose input shape is `Samples`, refuse.
    fn residual_effect(&self, s: &LqlLink, mut rel: Relation<Lql>) -> Relation<Lql> {
        if let LqlLink::Pipe(Stage::Unwrap(u)) = s {
            rel.cols
                .set_provenance(&Name::new(u.label.clone()), Provenance::EvaluatorOnly);
        }
        rel
    }
}

impl Lower<Lql> for UnpackLower {
    fn capability(&self, _s: &LqlLink, _rel: &Relation<Lql>) -> Capability {
        Capability::No(BlockReason::NotYetLowered)
    }
    fn apply(
        &self,
        _s: &LqlLink,
        rel: Relation<Lql>,
        _cx: &LowerCx<'_, Lql>,
    ) -> Result<Relation<Lql>, super::ReadError> {
        Ok(rel)
    }
    /// Rewrites the line, and the promoted labels arrive as an open
    /// source the evaluator owns.
    fn residual_effect(&self, _s: &LqlLink, rel: Relation<Lql>) -> Relation<Lql> {
        let mut rel = mark_line_rewritten(rel);
        rel.cols = rel.cols.widen(std::rc::Rc::new(EvaluatorOnlyLabels(
            crate::compile::fold::OpenSourceId("unpack:labels"),
        )));
        rel
    }
}

impl Lower<Lql> for DecolorizeLower {
    fn capability(&self, _s: &LqlLink, _rel: &Relation<Lql>) -> Capability {
        Capability::No(BlockReason::NotYetLowered)
    }
    fn apply(
        &self,
        _s: &LqlLink,
        rel: Relation<Lql>,
        _cx: &LowerCx<'_, Lql>,
    ) -> Result<Relation<Lql>, super::ReadError> {
        Ok(rel)
    }
    fn residual_effect(&self, _s: &LqlLink, rel: Relation<Lql>) -> Relation<Lql> {
        mark_line_rewritten(rel)
    }
}

impl Lower<Lql> for DropLower {
    fn capability(&self, _s: &LqlLink, _rel: &Relation<Lql>) -> Capability {
        Capability::No(BlockReason::NotYetLowered)
    }
    fn apply(
        &self,
        _s: &LqlLink,
        rel: Relation<Lql>,
        _cx: &LowerCx<'_, Lql>,
    ) -> Result<Relation<Lql>, super::ReadError> {
        Ok(rel)
    }
    /// `cols` rewritten with the named labels REMOVED, so a later filter
    /// on a dropped name refuses; `exact` untouched.
    fn residual_effect(&self, s: &LqlLink, mut rel: Relation<Lql>) -> Relation<Lql> {
        if let LqlLink::Pipe(Stage::Drop(elems)) = s {
            rel.cols = rel.cols.without(&elem_names(elems));
        }
        rel
    }
}

impl Lower<Lql> for KeepLower {
    fn capability(&self, _s: &LqlLink, _rel: &Relation<Lql>) -> Capability {
        Capability::No(BlockReason::NotYetLowered)
    }
    fn apply(
        &self,
        _s: &LqlLink,
        rel: Relation<Lql>,
        _cx: &LowerCx<'_, Lql>,
    ) -> Result<Relation<Lql>, super::ReadError> {
        Ok(rel)
    }
    /// As `Drop`, COMPLEMENTED. The two share the payload type
    /// `Vec<DropKeepElem>`, which is why the dispatcher takes the stage:
    /// the payload alone cannot say which link it belongs to.
    fn residual_effect(&self, s: &LqlLink, mut rel: Relation<Lql>) -> Relation<Lql> {
        if let LqlLink::Pipe(Stage::Keep(elems)) = s {
            rel.cols = rel.cols.only(&elem_names(elems));
        }
        rel
    }
}

fn elem_names(elems: &[DropKeepElem]) -> Vec<Name> {
    elems.iter().map(|e| Name::new(e.label.clone())).collect()
}

impl Lower<Lql> for WindowLower {
    fn capability(&self, _s: &LqlLink, _rel: &Relation<Lql>) -> Capability {
        Capability::No(BlockReason::NotYetLowered)
    }
    fn apply(
        &self,
        _s: &LqlLink,
        rel: Relation<Lql>,
        _cx: &LowerCx<'_, Lql>,
    ) -> Result<Relation<Lql>, super::ReadError> {
        Ok(rel)
    }
    /// Records the bucketing as evaluator-owned, so a following
    /// aggregation cannot lower.
    fn residual_effect(&self, _s: &LqlLink, mut rel: Relation<Lql>) -> Relation<Lql> {
        rel.cols
            .set_provenance(&Name::from("__bucket"), Provenance::EvaluatorOnly);
        rel
    }
}

impl Lower<Lql> for RangeAggLower {
    /// `AbsentOverTime` is `Never`: the answer is a statement about rows
    /// that are **absent**, so there is no row to compute it from.
    fn capability(&self, s: &LqlLink, _rel: &Relation<Lql>) -> Capability {
        if let LqlLink::RangeAgg {
            op: RangeAggOp::AbsentOverTime,
            ..
        } = s
        {
            return Capability::Never(NeverReason::NoRowToComputeFrom);
        }
        Capability::No(BlockReason::NotYetLowered)
    }
    fn apply(
        &self,
        _s: &LqlLink,
        rel: Relation<Lql>,
        _cx: &LowerCx<'_, Lql>,
    ) -> Result<Relation<Lql>, super::ReadError> {
        Ok(rel)
    }
    /// **Shape unchanged** — `Lines` whenever the `Unwrap` above went
    /// residual, which is the case its own row describes; clears `exact`.
    fn residual_effect(&self, _s: &LqlLink, mut rel: Relation<Lql>) -> Relation<Lql> {
        rel.exact = false;
        rel
    }
}

impl Lower<Lql> for VectorAggLower {
    fn capability(&self, _s: &LqlLink, _rel: &Relation<Lql>) -> Capability {
        Capability::No(BlockReason::NotYetLowered)
    }
    fn apply(
        &self,
        _s: &LqlLink,
        rel: Relation<Lql>,
        _cx: &LowerCx<'_, Lql>,
    ) -> Result<Relation<Lql>, super::ReadError> {
        Ok(rel)
    }
    /// Retains the prior series state; clears `exact`.
    fn residual_effect(&self, _s: &LqlLink, mut rel: Relation<Lql>) -> Relation<Lql> {
        rel.exact = false;
        rel
    }
}

impl Lower<Lql> for LabelReplaceLower {
    fn capability(&self, _s: &LqlLink, _rel: &Relation<Lql>) -> Capability {
        Capability::No(BlockReason::NotYetLowered)
    }
    fn apply(
        &self,
        _s: &LqlLink,
        rel: Relation<Lql>,
        _cx: &LowerCx<'_, Lql>,
    ) -> Result<Relation<Lql>, super::ReadError> {
        Ok(rel)
    }
    /// Retains series state and **clears `exact`**, because at range it
    /// can REMOVE series: colliding post-rewrite label sets merge into
    /// one whose points repeat per grid timestamp. Clearing is also the
    /// conservative side — `exact` only ever blocks a later link from
    /// lowering, never permits an unsound one.
    fn residual_effect(&self, s: &LqlLink, mut rel: Relation<Lql>) -> Relation<Lql> {
        if let LqlLink::LabelReplace { dst, .. } = s {
            rel.cols
                .set_provenance(&Name::new(dst.clone()), Provenance::EvaluatorOnly);
        }
        rel.exact = false;
        rel
    }
}

/// The column a LogQL read is ordered by.
pub const TIMESTAMP_NS: &str = "timestamp_ns";

impl Lower<Lql> for OrderLower {
    /// A LogQL sort key is the row's own timestamp, which no dropped row
    /// changes, so ordering a superset and then dropping rows leaves the
    /// surviving order correct — **no exactness precondition**, unlike
    /// TraceQL's, where the sort key is `max(matched-span timestamp)` and
    /// a superset makes the ORDER wrong rather than just the set. The
    /// asymmetry is the reason the precondition is per-link rather than
    /// global.
    fn capability(&self, _s: &LqlLink, rel: &Relation<Lql>) -> Capability {
        if rel.cols.resolve(&Name::from(TIMESTAMP_NS)).is_none() {
            return Capability::No(BlockReason::NameNotResolvable);
        }
        Capability::Yes
    }
    fn apply(
        &self,
        _s: &LqlLink,
        mut rel: Relation<Lql>,
        _cx: &LowerCx<'_, Lql>,
    ) -> Result<Relation<Lql>, super::ReadError> {
        rel.ordering = Some(crate::compile::fold::Ordering {
            keys: vec![(
                TIMESTAMP_NS.to_string(),
                crate::compile::fold::SortDir::Desc,
            )],
        });
        Ok(rel)
    }
    fn residual_effect(&self, _s: &LqlLink, mut rel: Relation<Lql>) -> Relation<Lql> {
        rel.ordering = None;
        rel
    }
    /// An `ORDER BY` over the row's own timestamp means exactly what the
    /// response's ordering contract means.
    fn fidelity(&self, _s: &LqlLink, _rel: &Relation<Lql>) -> Fidelity {
        Fidelity::Equivalent
    }
}

impl Lower<Lql> for LimitLower {
    /// `ordering.is_some()` **and `exact`** — a `LIMIT` over a superset
    /// loses rows a residual link would have kept.
    fn capability(&self, _s: &LqlLink, rel: &Relation<Lql>) -> Capability {
        if rel.ordering.is_none() {
            return Capability::No(BlockReason::OrderingNotEstablished);
        }
        if !rel.exact {
            return Capability::No(BlockReason::NotExact);
        }
        Capability::Yes
    }
    fn apply(
        &self,
        s: &LqlLink,
        mut rel: Relation<Lql>,
        _cx: &LowerCx<'_, Lql>,
    ) -> Result<Relation<Lql>, super::ReadError> {
        if let LqlLink::Limit(n) = s {
            rel.limit = Some(u64::from(*n));
        }
        Ok(rel)
    }
    /// Leaves `limit` unset, which is today's oversample path.
    fn residual_effect(&self, _s: &LqlLink, mut rel: Relation<Lql>) -> Relation<Lql> {
        rel.limit = None;
        rel
    }
    /// A `LIMIT n` over rows the predicate already means exactly returns
    /// exactly the first `n` of them.
    fn fidelity(&self, _s: &LqlLink, _rel: &Relation<Lql>) -> Fidelity {
        Fidelity::Equivalent
    }
}

impl Lower<Lql> for EmitLower {
    fn capability(&self, _s: &LqlLink, _rel: &Relation<Lql>) -> Capability {
        Capability::Never(NeverReason::ResponseBuild)
    }
    fn apply(
        &self,
        _s: &LqlLink,
        rel: Relation<Lql>,
        _cx: &LowerCx<'_, Lql>,
    ) -> Result<Relation<Lql>, super::ReadError> {
        Ok(rel)
    }
    /// Records the response build as the evaluator's.
    fn residual_effect(&self, _s: &LqlLink, mut rel: Relation<Lql>) -> Relation<Lql> {
        rel.cols
            .set_provenance(&Name::from("__response"), Provenance::EvaluatorOnly);
        rel
    }
}

/// Parses `{service_name="x"} <atom>` and returns the single pipeline
/// stage, so that every payload a gate uses comes from the real parser
/// rather than from a hand-built AST. Test-only, and crate-visible
/// because the walk-agreement gates in [`super::plan`] build their corpus
/// from the same atoms.
#[cfg(test)]
pub(crate) fn parsed_stage(atom: &str) -> Stage {
    let q = format!(r#"{{service_name="x"}} {atom}"#);
    let expr = pulsus_logql::parse(&q).unwrap_or_else(|e| panic!("{q}: {e}"));
    let pulsus_logql::Expr::Log(log) = expr else {
        panic!("{q} is not a log expression")
    };
    log.pipeline
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("{q} has no stage"))
}

/// The seed relation a LogQL chain folds from: the resolved selector over
/// the stream index, with the stored line, the sort key and one
/// structured-metadata key ([`LEVEL`]) in the column set.
pub fn seed_relation() -> Relation<Lql> {
    Relation {
        source: crate::compile::fold::SourceTerm::Base(LqlSource(LOG_STREAMS_IDX)),
        predicate: Pred::True,
        projection: vec![(Name::from(FINGERPRINT), FINGERPRINT.to_string())],
        cols: ColSet::Closed(vec![
            Col {
                name: Name::from(BODY),
                provenance: Provenance::Stored,
            },
            Col {
                name: Name::from(TIMESTAMP_NS),
                provenance: Provenance::Stored,
            },
            Col {
                name: Name::from(LEVEL),
                provenance: Provenance::Computed(SqlExpr::new(LEVEL_EXPR)),
            },
        ]),
        grouping: None,
        ordering: None,
        limit: None,
        shape: LqlShape::Lines,
        exact: true,
        depth: 0,
    }
}

// ---------------------------------------------------------------------
// Gates
// ---------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::parsed_stage as stage;
    use super::*;
    use crate::compile::fold::{
        Disposition, Grouping, Lowering, Ordering, RequestBounds, ResidualReason, SortDir,
        lower_chain,
    };
    use crate::compile::plan::{Cut, Issue, Part, PlanConfig, PlanCx, plan_of};
    use crate::compile::testkit::{EffectRow, assert_every_residual_state_effect};

    fn bounds(limit: Option<u32>) -> RequestBounds {
        RequestBounds {
            start_ns: 0,
            end_ns: 1,
            step_ns: None,
            limit,
        }
    }

    fn fold(chain: &[LqlLink], b: &RequestBounds) -> Lowering<Lql> {
        let cx = LowerCx::<Lql>::new(b);
        lower_chain::<Lql>(chain, seed_relation(), &cx).expect("fold")
    }

    fn plan(
        chain: &[LqlLink],
        b: &RequestBounds,
        config: &PlanConfig,
    ) -> crate::compile::plan::QueryPlan<Lql> {
        plan_of::<Lql>(chain, fold(chain, b), &PlanCx { bounds: b, config }).expect("plan")
    }

    // --- the residual state effects -----------------------------------

    fn base(shape: LqlShape) -> Relation<Lql> {
        let mut rel = seed_relation();
        rel.shape = shape;
        rel
    }

    fn with_exact(mut rel: Relation<Lql>, exact: bool) -> Relation<Lql> {
        rel.exact = exact;
        rel
    }

    fn with_col(mut rel: Relation<Lql>, name: &str, p: Provenance) -> Relation<Lql> {
        rel.cols.set_provenance(&Name::from(name), p);
        rel
    }

    fn widened(mut rel: Relation<Lql>, id: &'static str) -> Relation<Lql> {
        rel.cols = rel.cols.widen(std::rc::Rc::new(EvaluatorOnlyLabels(
            crate::compile::fold::OpenSourceId(id),
        )));
        rel
    }

    fn ordered(mut rel: Relation<Lql>, key: &str) -> Relation<Lql> {
        rel.ordering = Some(Ordering {
            keys: vec![(key.to_string(), SortDir::Desc)],
        });
        rel
    }

    fn limited(mut rel: Relation<Lql>, n: u64) -> Relation<Lql> {
        rel.limit = Some(n);
        rel
    }

    fn without_col(mut rel: Relation<Lql>, name: &str) -> Relation<Lql> {
        rel.cols = rel.cols.without(&[Name::from(name)]);
        rel
    }

    fn only_cols(mut rel: Relation<Lql>, names: &[&str]) -> Relation<Lql> {
        rel.cols = rel
            .cols
            .only(&names.iter().map(|n| Name::from(*n)).collect::<Vec<_>>());
        rel
    }

    /// Issue #492: every LogQL link's residual state effect is the one
    /// the design record states.
    ///
    /// **Twenty rows** — thirteen `Pipe` rows (the line filter, the four
    /// parser forms, the label filter, `line_format`, `label_format`,
    /// `unwrap`, `unpack`, `decolorize`, `drop`, `keep`) and seven
    /// synthesised links (`Window`, `RangeAgg`, `VectorAgg`,
    /// `LabelReplace`, `Order`, `Limit`, `Emit`).
    #[test]
    fn every_residual_state_effect_is_the_one_the_document_states() {
        let dropkeep = stage("| drop level");
        let keep = stage("| keep body");
        let label_format = stage(r#"| label_format dst=src"#);
        let unwrap = stage("| unwrap latency");

        let mut rows: Vec<EffectRow<Lql>> = Vec::new();

        // A line filter removes lines in the evaluator, so it clears
        // `exact`. Both seeds carry `exact = true`, or the clearing would
        // be invisible on one of them.
        rows.push(EffectRow {
            name: "LineFilter",
            link: LqlLink::Pipe(stage(r#"|= ip("10.0.0.0/8")"#)),
            s1: with_exact(base(LqlShape::Lines), true),
            s2: with_exact(base(LqlShape::Samples), true),
            e1: with_exact(base(LqlShape::Lines), false),
            e2: with_exact(base(LqlShape::Samples), false),
            effect_is_constant: false,
            has_effect: true,
        });

        // The four parser forms: `cols` is still widened, with an
        // evaluator-only open source, and `exact` is NOT cleared — a
        // parse failure keeps the line with an `__error__` label, so a
        // parser removes no line. The seeds therefore differ in `exact`.
        for (name, atom, id) in [
            ("Parser(Json)", "| json", "parser:json"),
            ("Parser(Logfmt)", "| logfmt", "parser:logfmt"),
            ("Parser(Regexp)", r#"| regexp "(?P<a>.*)""#, "parser:regexp"),
            ("Parser(Pattern)", r#"| pattern "<a>""#, "parser:pattern"),
        ] {
            rows.push(EffectRow {
                name,
                link: LqlLink::Pipe(stage(atom)),
                s1: with_exact(base(LqlShape::Lines), true),
                s2: with_exact(base(LqlShape::Samples), false),
                e1: widened(with_exact(base(LqlShape::Lines), true), id),
                e2: widened(with_exact(base(LqlShape::Samples), false), id),
                effect_is_constant: false,
                has_effect: true,
            });
        }

        rows.push(EffectRow {
            name: "LabelFilter",
            link: LqlLink::Pipe(stage(r#"| level="error""#)),
            s1: with_exact(base(LqlShape::Lines), true),
            s2: with_exact(base(LqlShape::Samples), true),
            e1: with_exact(base(LqlShape::Lines), false),
            e2: with_exact(base(LqlShape::Samples), false),
            effect_is_constant: false,
            has_effect: true,
        });

        // A line rewrite leaves the line with no resolvable expression
        // and does NOT clear `exact` — it removes no line — so the seeds
        // differ in `exact` as well as in shape.
        for (name, atom) in [
            ("LineFormat", r#"| line_format "{{.msg}}""#),
            ("Decolorize", "| decolorize"),
        ] {
            rows.push(EffectRow {
                name,
                link: LqlLink::Pipe(stage(atom)),
                s1: with_exact(base(LqlShape::Lines), true),
                s2: with_exact(base(LqlShape::Samples), false),
                e1: with_col(
                    with_exact(base(LqlShape::Lines), true),
                    BODY,
                    Provenance::EvaluatorOnly,
                ),
                e2: with_col(
                    with_exact(base(LqlShape::Samples), false),
                    BODY,
                    Provenance::EvaluatorOnly,
                ),
                effect_is_constant: false,
                has_effect: true,
            });
        }

        rows.push(EffectRow {
            name: "LabelFormat",
            link: LqlLink::Pipe(label_format),
            s1: with_exact(base(LqlShape::Lines), true),
            s2: with_exact(base(LqlShape::Samples), false),
            e1: with_col(
                with_exact(base(LqlShape::Lines), true),
                "dst",
                Provenance::EvaluatorOnly,
            ),
            e2: with_col(
                with_exact(base(LqlShape::Samples), false),
                "dst",
                Provenance::EvaluatorOnly,
            ),
            effect_is_constant: false,
            has_effect: true,
        });

        // Shape unchanged — stated as PRESERVATION, not as an assignment
        // of `Lines`, because that is only true today by grace of the
        // parser refusing a second `unwrap`.
        rows.push(EffectRow {
            name: "Unwrap",
            link: LqlLink::Pipe(unwrap),
            s1: base(LqlShape::Lines),
            s2: base(LqlShape::Samples),
            e1: with_col(base(LqlShape::Lines), "latency", Provenance::EvaluatorOnly),
            e2: with_col(
                base(LqlShape::Samples),
                "latency",
                Provenance::EvaluatorOnly,
            ),
            effect_is_constant: false,
            has_effect: true,
        });

        rows.push(EffectRow {
            name: "Unpack",
            link: LqlLink::Pipe(stage("| unpack")),
            s1: base(LqlShape::Lines),
            s2: base(LqlShape::Samples),
            e1: widened(
                with_col(base(LqlShape::Lines), BODY, Provenance::EvaluatorOnly),
                "unpack:labels",
            ),
            e2: widened(
                with_col(base(LqlShape::Samples), BODY, Provenance::EvaluatorOnly),
                "unpack:labels",
            ),
            effect_is_constant: false,
            has_effect: true,
        });

        // `cols` narrowed, `exact` untouched: the seeds differ in both,
        // and both carry the label the payload names or the narrowing
        // would be invisible.
        rows.push(EffectRow {
            name: "Drop",
            link: LqlLink::Pipe(dropkeep),
            s1: with_col(
                with_exact(base(LqlShape::Lines), true),
                "level",
                Provenance::Stored,
            ),
            s2: with_col(
                with_exact(base(LqlShape::Samples), false),
                "level",
                Provenance::Stored,
            ),
            e1: without_col(
                with_col(
                    with_exact(base(LqlShape::Lines), true),
                    "level",
                    Provenance::Stored,
                ),
                "level",
            ),
            e2: without_col(
                with_col(
                    with_exact(base(LqlShape::Samples), false),
                    "level",
                    Provenance::Stored,
                ),
                "level",
            ),
            effect_is_constant: false,
            has_effect: true,
        });

        rows.push(EffectRow {
            name: "Keep",
            link: LqlLink::Pipe(keep),
            s1: with_col(
                with_exact(base(LqlShape::Lines), true),
                "level",
                Provenance::Stored,
            ),
            s2: with_col(
                with_exact(base(LqlShape::Samples), false),
                "level",
                Provenance::Stored,
            ),
            e1: only_cols(
                with_col(
                    with_exact(base(LqlShape::Lines), true),
                    "level",
                    Provenance::Stored,
                ),
                &[BODY],
            ),
            e2: only_cols(
                with_col(
                    with_exact(base(LqlShape::Samples), false),
                    "level",
                    Provenance::Stored,
                ),
                &[BODY],
            ),
            effect_is_constant: false,
            has_effect: true,
        });

        rows.push(EffectRow {
            name: "Window",
            link: LqlLink::Window {
                range_ns: 300_000_000_000,
                step_ns: 60_000_000_000,
                offset_ns: 0,
                grid_start_ns: 0,
            },
            s1: base(LqlShape::Lines),
            s2: base(LqlShape::Samples),
            e1: with_col(base(LqlShape::Lines), "__bucket", Provenance::EvaluatorOnly),
            e2: with_col(
                base(LqlShape::Samples),
                "__bucket",
                Provenance::EvaluatorOnly,
            ),
            effect_is_constant: false,
            has_effect: true,
        });

        rows.push(EffectRow {
            name: "RangeAgg",
            link: LqlLink::RangeAgg {
                op: RangeAggOp::CountOverTime,
                grouping: None,
                param: None,
            },
            s1: with_exact(base(LqlShape::Lines), true),
            s2: with_exact(base(LqlShape::Samples), true),
            e1: with_exact(base(LqlShape::Lines), false),
            e2: with_exact(base(LqlShape::Samples), false),
            effect_is_constant: false,
            has_effect: true,
        });

        rows.push(EffectRow {
            name: "VectorAgg",
            link: LqlLink::VectorAgg {
                op: VectorAggOp::Sum,
                grouping: None,
                param: None,
            },
            s1: with_exact(base(LqlShape::Series), true),
            s2: with_exact(base(LqlShape::Samples), true),
            e1: with_exact(base(LqlShape::Series), false),
            e2: with_exact(base(LqlShape::Samples), false),
            effect_is_constant: false,
            has_effect: true,
        });

        rows.push(EffectRow {
            name: "LabelReplace",
            link: LqlLink::LabelReplace {
                dst: "dst".to_string(),
                replacement: "$1".to_string(),
                src: "src".to_string(),
                regex: "(.*)".to_string(),
            },
            s1: with_exact(base(LqlShape::Series), true),
            s2: with_exact(base(LqlShape::Samples), true),
            e1: with_col(
                with_exact(base(LqlShape::Series), false),
                "dst",
                Provenance::EvaluatorOnly,
            ),
            e2: with_col(
                with_exact(base(LqlShape::Samples), false),
                "dst",
                Provenance::EvaluatorOnly,
            ),
            effect_is_constant: false,
            has_effect: true,
        });

        rows.push(EffectRow {
            name: "Order",
            link: LqlLink::Order,
            s1: ordered(base(LqlShape::Lines), "timestamp_ns"),
            s2: ordered(base(LqlShape::Series), "t"),
            e1: base(LqlShape::Lines),
            e2: base(LqlShape::Series),
            effect_is_constant: false,
            has_effect: true,
        });

        rows.push(EffectRow {
            name: "Limit",
            link: LqlLink::Limit(100),
            s1: limited(base(LqlShape::Lines), 100),
            s2: limited(base(LqlShape::Series), 101),
            e1: base(LqlShape::Lines),
            e2: base(LqlShape::Series),
            effect_is_constant: false,
            has_effect: true,
        });

        rows.push(EffectRow {
            name: "Emit",
            link: LqlLink::Emit,
            s1: base(LqlShape::Lines),
            s2: base(LqlShape::Series),
            e1: with_col(
                base(LqlShape::Lines),
                "__response",
                Provenance::EvaluatorOnly,
            ),
            e2: with_col(
                base(LqlShape::Series),
                "__response",
                Provenance::EvaluatorOnly,
            ),
            effect_is_constant: false,
            has_effect: true,
        });

        assert_every_residual_state_effect::<Lql>(&rows, 20);
    }

    /// Issue #492: `Drop` and `Keep` share the payload type
    /// `Vec<DropKeepElem>`, so the payload alone cannot say which link it
    /// belongs to — the dispatcher has to be reached through the STAGE.
    ///
    /// The three assertions a per-row table cannot make: the two links
    /// reach different dispatchers, each effect equals its own literal,
    /// and the two literals differ from each other — computed from the
    /// two literals rather than from the two dispatchers, so it cannot be
    /// satisfied by both sides collapsing together.
    #[test]
    fn drop_and_keep_dispatch_differently_on_the_same_payload_type() {
        // ONE shared payload, taken apart from two parsed stages so the
        // elements are the parser's own.
        let Stage::Drop(payload) = stage("| drop level") else {
            panic!("not a drop stage")
        };
        let drop_link = LqlLink::Pipe(Stage::Drop(payload.clone()));
        let keep_link = LqlLink::Pipe(Stage::Keep(payload.clone()));
        assert_eq!(payload.len(), 1);
        assert_eq!(payload[0].label, "level");

        let d = Lql::lower_of(&drop_link);
        let k = Lql::lower_of(&keep_link);
        assert!(
            !std::ptr::eq(d as *const dyn Lower<Lql>, k as *const dyn Lower<Lql>),
            "Drop and Keep must not reach the same dispatcher"
        );

        let seed = with_col(base(LqlShape::Lines), "level", Provenance::Stored);
        let expected_drop = without_col(seed.clone(), "level");
        let expected_keep = only_cols(seed.clone(), &["level"]);

        assert_eq!(d.residual_effect(&drop_link, seed.clone()), expected_drop);
        assert_eq!(k.residual_effect(&keep_link, seed.clone()), expected_keep);
        assert_ne!(
            expected_drop, expected_keep,
            "the two literal effects must differ, computed from the literals and not from the \
             dispatchers"
        );
    }

    /// Issue #492 acceptance criterion 4: a residual filter between two
    /// pushable ones does NOT cut. The fold continues and the later
    /// filter contributes to the SAME statement.
    ///
    /// The query is `{service_name="ipcase"} |= "CONN_REFUSED"
    /// |= ip("10.0.0.0/8") |= "pod-044"`. Withdrawing this rule is the
    /// measured 20.6× metered-byte regression.
    #[test]
    fn a_residual_filter_between_two_pushable_ones_does_not_cut() {
        let b = bounds(Some(100));
        let config = PlanConfig::default();

        let with_ip = vec![
            LqlLink::Source,
            LqlLink::Pipe(stage(r#"|= "CONN_REFUSED""#)),
            LqlLink::Pipe(stage(r#"|= ip("10.0.0.0/8")"#)),
            LqlLink::Pipe(stage(r#"|= "pod-044""#)),
            LqlLink::Order,
            LqlLink::Limit(100),
            LqlLink::Emit,
        ];
        let p = plan(&with_ip, &b, &config);

        // Both literal filters lowered, into part 0, with no cut.
        for i in [1usize, 3] {
            assert_eq!(
                p.links[i].how,
                Disposition::Lowered(Fidelity::Equivalent),
                "link {i} is a literal line filter and must lower"
            );
            assert_eq!(
                p.links[i].part, 0,
                "link {i} contributes to the FIRST statement"
            );
        }
        // The `ip()` filter is residual and is engine work.
        assert_eq!(
            p.links[2].how,
            Disposition::Residual(ResidualReason::Blocked(BlockReason::NotPushable))
        );
        assert!(
            matches!(p.parts[p.links[2].part], Part::Engine { .. }),
            "the address filter is engine work, not a statement: {:?}",
            p.parts
        );
        // No line filter OPENS a statement. The only cut in the plan is
        // the inexact-limit one, which is not a second statement — it is
        // the same statement issued once per page, and it is there
        // because the residual filter cleared `exact`, which is exactly
        // today's `fetch_until_limit`.
        for part in &p.parts {
            if let Part::Sql(s) = part
                && let Some(cut) = &s.cut
            {
                assert_eq!(
                    *cut,
                    Cut::InexactLimit,
                    "a line filter must not open a statement"
                );
            }
        }
        assert_eq!(
            p.parts.iter().filter(|x| matches!(x, Part::Sql(_))).count(),
            1,
            "one statement: {:?}",
            p.parts
        );

        // The control, and it is what makes the assertions above about
        // the RULE rather than about this chain: dropping the residual
        // filter changes no statement count.
        let without_ip = vec![
            LqlLink::Source,
            LqlLink::Pipe(stage(r#"|= "CONN_REFUSED""#)),
            LqlLink::Pipe(stage(r#"|= "pod-044""#)),
            LqlLink::Order,
            LqlLink::Limit(100),
            LqlLink::Emit,
        ];
        let q = plan(&without_ip, &b, &config);
        let sql = |pl: &crate::compile::plan::QueryPlan<Lql>| {
            pl.parts
                .iter()
                .filter(|p| matches!(p, Part::Sql(_)))
                .count()
        };
        assert_eq!(
            sql(&p),
            sql(&q),
            "the unpushable filter adds no statement: {:?} vs {:?}",
            p.parts,
            q.parts
        );
    }

    /// Issue #492 acceptance criterion 5: **fidelity decides the issue
    /// count.**
    ///
    /// A chain whose lowered links are all `Equivalent` keeps `exact`, so
    /// the `Limit` link lowers and the last statement carries the
    /// request's `LIMIT` and is sent once. One `Wider` or residual link
    /// clears `exact`, the `Limit` link refuses, and the same statement
    /// becomes a page loop.
    ///
    /// **Deviation from the plan's wording, and it is deliberate.** The
    /// criterion names the structured-metadata label filter as the
    /// `Equivalent` half. Wave 1 lowers no label filter at all
    /// ([`LabelFilterLower::capability`] says why), so the half that
    /// exercises the mechanism here is the pushable line filter, which
    /// genuinely lowers today and genuinely returns `Equivalent`. The
    /// mechanism under test is the same one.
    #[test]
    fn an_equivalent_filter_lets_the_limit_lower_and_a_wider_one_does_not() {
        let b = bounds(Some(100));
        let config = PlanConfig::default();

        let equivalent = vec![
            LqlLink::Source,
            LqlLink::Pipe(stage(r#"|= "CONN_REFUSED""#)),
            LqlLink::Order,
            LqlLink::Limit(100),
            LqlLink::Emit,
        ];
        assert_eq!(
            plan(&equivalent, &b, &config)
                .parts
                .iter()
                .filter(|p| matches!(p, Part::Sql(_)))
                .count(),
            1,
            "a LogQL plan is one statement plus engine work in wave 1"
        );
        let lowering = fold(&equivalent, &b);
        assert!(lowering.rel.exact, "an equivalent link keeps `exact`");
        assert_eq!(
            lowering.how[3],
            Disposition::Lowered(Fidelity::Equivalent),
            "the Limit link lowers, so the request LIMIT enters the statement"
        );
        assert_eq!(lowering.rel.limit, Some(100), "the request LIMIT entered");
        let p = plan(&equivalent, &b, &config);
        let last = p
            .parts
            .iter()
            .rposition(|x| matches!(x, Part::Sql(_)))
            .expect("an SQL part");
        let Part::Sql(sp) = &p.parts[last] else {
            unreachable!()
        };
        assert_eq!(sp.issue, Issue::Once, "one statement, not a page loop");

        let wider = vec![
            LqlLink::Source,
            LqlLink::Pipe(stage(r#"|= ip("10.0.0.0/8")"#)),
            LqlLink::Order,
            LqlLink::Limit(100),
            LqlLink::Emit,
        ];
        let lowering = fold(&wider, &b);
        assert!(
            !lowering.rel.exact,
            "a residual dropping link clears `exact`"
        );
        assert_eq!(
            lowering.how[3],
            Disposition::Residual(ResidualReason::Blocked(BlockReason::NotExact)),
            "the Limit link refuses over a superset"
        );
        assert_eq!(lowering.rel.limit, None, "the request LIMIT did NOT enter");
        let p = plan(&wider, &b, &config);
        let last = p
            .parts
            .iter()
            .rposition(|x| matches!(x, Part::Sql(_)))
            .expect("an SQL part");
        let Part::Sql(sp) = &p.parts[last] else {
            unreachable!()
        };
        assert!(
            matches!(
                sp.issue,
                Issue::PerSeed(crate::compile::plan::Driver::Keyset { .. })
            ),
            "the same statement, issued once per page: {:?}",
            sp.issue
        );
    }

    /// The synthesised `Grouping` slot type is exercised, so the seed
    /// builders above cannot silently stop compiling against it.
    #[test]
    fn a_grouping_slot_is_part_of_the_accumulated_state() {
        let mut rel = seed_relation();
        assert!(rel.grouping.is_none());
        rel.grouping = Some(Grouping {
            keys: vec!["level".to_string()],
        });
        assert_eq!(rel.grouping.as_ref().map(|g| g.keys.len()), Some(1));
    }
}
