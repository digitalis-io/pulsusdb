//! The compiler's output is a PLAN, not a statement (issue #492, wave 1).
//!
//! The fold's output was one relation, one disposition per link and a
//! boundary kind. **That cannot say what a request actually does**: the
//! design record's worked query sends 1,110 statements, and a type with
//! no field that could hold a number other than one described a SQL
//! statement — the winners' root read — as work in our own engine. A type
//! that cannot represent what the system already does is wrong
//! independently of any new requirement.
//!
//! So the compiler emits a plan: an ordered list of [`Part`]s, each part
//! either one SQL statement or work in our own process, with the value
//! set that crosses between two parts named, typed and bounded
//! ([`Seed`]).
//!
//! **The plan already exists in the shipped code; what was missing is a
//! type that can say so.** The committed TraceQL SQL goldens are written
//! per part and index the repeated ones — `== phase1 generator[0] ==`,
//! `== phase2 hydration (sample batch) ==`, `== phase2 membership[0] ==`,
//! `== root hydration (sample winners) ==` in
//! `crates/pulsus-read/tests/golden/traces_search/worked_example.sql`.
//! That file is this type drawn by hand, one case at a time.

use serde::Serialize;

use super::fold::{
    BoundaryOutput, Disposition, Fidelity, Lang, Lowering, Name, Relation, RequestBounds,
    ResidualReason, branch_sources_union,
};

// ---------------------------------------------------------------------
// Sources, bounds and costs
// ---------------------------------------------------------------------

/// Names one readable source — a table, or a table plus the projection
/// the planner would read it through.
///
/// [`super::fold::Relation::source`] is a `SourceTerm`, which is either a
/// base source or a wrapped relation; `SourceRef` is the comparable
/// identity [`Lang::source_of`] answers with, so that "a different
/// source" is an equality the core can decide without knowing either
/// language.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceRef(pub &'static str);

impl SourceRef {
    pub fn as_str(&self) -> &'static str {
        self.0
    }
}

impl std::fmt::Display for SourceRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

/// Rendered size of a seed, in query-text bytes and in database AST
/// elements — the two ceilings a statement has to stay under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct HandoffCost {
    pub text_bytes: u64,
    pub ast_elements: u64,
}

/// Where a seed's plan-time upper bound comes from. Every admissible
/// seed has one: a request parameter, a config field, or a named
/// constant. A link whose bound is `None` does not get a cut at all,
/// which is what stops a plan from shipping rewritten lines back to the
/// database after a stage that rewrites the line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeedBound {
    RequestLimit(u32),
    Config { name: &'static str, value: u64 },
    Constant { name: &'static str, value: u64 },
}

impl SeedBound {
    pub fn value(&self) -> u64 {
        match self {
            SeedBound::RequestLimit(n) => u64::from(*n),
            SeedBound::Config { value, .. } | SeedBound::Constant { value, .. } => *value,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            SeedBound::RequestLimit(_) => "request_limit",
            SeedBound::Config { .. } => "config",
            SeedBound::Constant { .. } => "constant",
        }
    }

    pub fn name(&self) -> Option<&'static str> {
        match self {
            SeedBound::RequestLimit(_) => None,
            SeedBound::Config { name, .. } | SeedBound::Constant { name, .. } => Some(name),
        }
    }
}

/// The two measured ceilings, and the paging parameters a keyset driver
/// uses. Defaults are the shipped constants, so a plan built with
/// `PlanConfig::default()` is bounded by exactly what the executor is
/// bounded by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanConfig {
    /// The database's AST-element ceiling. 32,768 literal ids is
    /// 1,409,081 query bytes and is refused with
    /// `Code: 168. DB::Exception: AST is too big. Maximum: 50000.`;
    /// raising `max_query_size` does not help (ADR 0008 D3, measured).
    pub max_ast_elements: u64,
    /// The rendered-SQL-text ceiling —
    /// [`crate::querytext::MAX_QUERY_TEXT_BYTES`].
    pub max_query_text_bytes: u64,
    /// Rows drawn per keyset page.
    pub keyset_page_rows: u32,
    /// The over-fetch factor a keyset page applies when the request's
    /// `LIMIT` could not enter the statement.
    pub keyset_over_fetch: u32,
}

impl Default for PlanConfig {
    fn default() -> Self {
        Self {
            max_ast_elements: 50_000,
            max_query_text_bytes: crate::querytext::MAX_QUERY_TEXT_BYTES,
            keyset_page_rows: 1_000,
            keyset_over_fetch: 10,
        }
    }
}

/// What the plan builder may read: the request's bounds, and the reader
/// config the seed bounds come from. It carries no connection and
/// performs no query — every rule in this module is decided at plan
/// time, in O(1) or O(log n), with no round trip.
#[derive(Debug, Clone, Copy)]
pub struct PlanCx<'a> {
    pub bounds: &'a RequestBounds,
    pub config: &'a PlanConfig,
}

// ---------------------------------------------------------------------
// The plan object
// ---------------------------------------------------------------------

/// Why a part is its own statement and not folded into the previous one.
///
/// **CLOSED: these four are the whole set.** Each is derived from one of
/// exactly two things a single statement cannot do — read a second source
/// keyed by its own result, or hold more than fits — plus the two forms
/// of "more than fits": the seed's size, and the answer's when the
/// `LIMIT` cannot enter. What would falsify the closure: a query in
/// either language whose correct plan has two SQL parts and no cut in
/// this list. An exhaustive `match` makes a fifth cut a build failure
/// rather than a silent addition; no test can discover that a fifth is
/// *needed*.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cut {
    /// The next read is over a different source, keyed by this one's
    /// result. ADR 0008 D3 forbids expressing that as a subquery, on
    /// measurement: `trace_id IN (<20 literal ids>)` reads 100 granules
    /// and 819,200 rows where `trace_id IN (SELECT … LIMIT 20)` reads
    /// 1,205 and 9,871,360 for the same 20 traces.
    SourceHandoff { source: SourceRef, key: Name },
    /// The seed does not fit in one statement.
    HandoffExceedsBound { cost: HandoffCost },
    /// An `OR` whose sides resolve against different sources. One
    /// `WHERE` cannot hold them, ADR 0008 D2 bans the common-table form
    /// on measurement, and the union form is a second statement merged in
    /// our process.
    DisjointSources { sources: Vec<SourceRef> },
    /// The request's `LIMIT` cannot enter the statement.
    InexactLimit,
}

impl Cut {
    /// The wire word, shared by the explain renderer and the design
    /// record's cut table.
    pub fn why(&self) -> &'static str {
        match self {
            Cut::SourceHandoff { .. } => "source_handoff",
            Cut::HandoffExceedsBound { .. } => "handoff_exceeds_bound",
            Cut::DisjointSources { .. } => "disjoint_sources",
            Cut::InexactLimit => "inexact_limit",
        }
    }
}

/// How many times a statement is sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Issue {
    /// Sent once.
    Once,
    /// Sent once per seed drawn from `driver`, until the driver stops.
    PerSeed(Driver),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Driver {
    /// The seed set is bounded but too large to write into one
    /// statement, so it is sent in chunks. `chunk` is chosen so the
    /// rendered statement stays under both ceilings.
    Chunks { bound: u64, chunk: u64 },
    /// The request's LIMIT could not enter the statement, so pages are
    /// drawn, each resuming from the previous page's last sort key, until
    /// the limit fills, the window is exhausted, or a byte budget is
    /// spent. This is today's keyset loop, named.
    Keyset { page_rows: u32, over_fetch: u32 },
}

impl Issue {
    pub fn wire(&self) -> &'static str {
        match self {
            Issue::Once => "once",
            Issue::PerSeed(Driver::Chunks { .. }) => "per_seed:chunks",
            Issue::PerSeed(Driver::Keyset { .. }) => "per_seed:keyset",
        }
    }
}

/// A value set crossing from one part to the next. Always materialised
/// values, never a subquery (ADR 0008 D3), and always bounded.
pub struct Seed<L: Lang + ?Sized> {
    pub from_part: usize,
    /// The language's own handoff type — trace ids, fingerprints, a
    /// keyset cursor. At plan time this is the EMPTY handoff: the plan
    /// describes the crossing, and the executor fills the values in.
    pub values: L::Handoff,
    /// The plan-time upper bound on how many values can be in it, and
    /// where that bound comes from.
    pub bound: SeedBound,
}

/// One SQL statement of a plan.
pub struct SqlPart<L: Lang + ?Sized> {
    /// The clause-slot term this statement renders from (ADR 0008 D1).
    pub rel: Relation<L>,
    /// What this statement consumes from the part before it. `None` only
    /// for a part that opens the plan.
    pub seed: Option<Seed<L>>,
    /// What it produces for the part after it.
    pub yields: BoundaryOutput<L>,
    /// How many times the statement is sent.
    pub issue: Issue,
    /// Why this is its own statement and not folded into the previous
    /// one.
    pub cut: Option<Cut>,
}

/// One part of a plan: a statement, or work in our own process.
pub enum Part<L: Lang + ?Sized> {
    /// Boxed because an [`SqlPart`] carries a whole [`Relation`] and an
    /// engine part carries two `usize`s; without the indirection every
    /// engine part in the vector would pay the statement's width.
    Sql(Box<SqlPart<L>>),
    /// Work in our own process: the residual links, applied in chain
    /// order. `links` indexes [`QueryPlan::links`].
    Engine { links: std::ops::Range<usize> },
}

/// One link's place in the plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LinkOutcome {
    /// Index into [`QueryPlan::parts`].
    pub part: usize,
    /// Unchanged from the fold.
    pub how: Disposition,
}

/// The compiler's output for one request. This — not SQL text, and not a
/// boundary index — is what an executor consumes. Never empty.
pub struct QueryPlan<L: Lang + ?Sized> {
    pub parts: Vec<Part<L>>,
    /// One entry per chain link, in chain order, so that every link in
    /// the user's pipeline can be traced to the part that runs it.
    pub links: Vec<LinkOutcome>,
}

// ---------------------------------------------------------------------
// The four recognisers
// ---------------------------------------------------------------------

/// The handoff facts a link supplies, once all three answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handoff {
    pub source: SourceRef,
    pub key: Name,
    pub bound: SeedBound,
}

/// Every cut rule that fires on one situation, in the order [`plan_of`]
/// consults them; `plan_of` takes the first.
///
/// The rules are separate so that "this input produces exactly this cut
/// and no other rule fires on it" is a statement a test can make, rather
/// than a property of the order they happen to be written in.
pub fn cuts_firing<L: Lang + ?Sized + 'static>(
    rel: &Relation<L>,
    handoff: Option<&Handoff>,
    cx: &PlanCx<'_>,
) -> Vec<Cut> {
    let mut out = Vec::new();
    if let Some(sources) = rel.predicate.disjoint_or_sources() {
        out.push(Cut::DisjointSources { sources });
    }
    if let Some(h) = handoff {
        // The two handoff rules are exclusive by construction: a seed
        // that fits in one statement is a source handoff, and one that
        // does not is the stronger reason. Overlapping them would make
        // "exactly one rule fires on this input" unstateable.
        let cost = L::handoff_cost(h.bound.value());
        if cost.text_bytes > cx.config.max_query_text_bytes
            || cost.ast_elements > cx.config.max_ast_elements
        {
            out.push(Cut::HandoffExceedsBound { cost });
        } else {
            out.push(Cut::SourceHandoff {
                source: h.source,
                key: h.key.clone(),
            });
        }
    }
    if inexact_limit_fires(rel, cx) {
        out.push(Cut::InexactLimit);
    }
    out
}

/// `request.limit.is_some() && rel.limit.is_none() && !rel.exact` after
/// the fold — the request's `LIMIT` could not enter the statement, so
/// pages are drawn until the limit fills.
pub fn inexact_limit_fires<L: Lang + ?Sized>(rel: &Relation<L>, cx: &PlanCx<'_>) -> bool {
    cx.bounds.limit.is_some() && rel.limit.is_none() && !rel.exact
}

/// The largest chunk of `bound` values whose rendered statement stays
/// under both ceilings. Binary search over [`Lang::handoff_cost`], which
/// is O(1) — no statement is rendered and no round trip is made.
fn chunk_for<L: Lang + ?Sized + 'static>(bound: u64, cx: &PlanCx<'_>) -> u64 {
    let fits = |n: u64| {
        let c = L::handoff_cost(n);
        c.text_bytes <= cx.config.max_query_text_bytes
            && c.ast_elements <= cx.config.max_ast_elements
    };
    if fits(bound) {
        return bound;
    }
    let (mut lo, mut hi) = (1u64, bound);
    while lo < hi {
        let mid = lo + (hi - lo).div_ceil(2);
        if fits(mid) {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }
    lo.max(1)
}

// ---------------------------------------------------------------------
// plan_of
// ---------------------------------------------------------------------

/// Partitions a completed fold into parts.
///
/// **The RULES live here, in the core; the FACTS come from [`Lang`].**
/// The plan builder never asks a link whether to cut — it asks what the
/// link reads, what key reaches it and how big the crossing would be, and
/// applies its own four rules.
///
/// **Three shapes that must NOT cut**, stated as rules because they are
/// the expensive mistakes:
///
/// 1. A residual link mid-pipeline does not cut. The fold continued and a
///    later link contributed to the *same* statement; withdrawing that is
///    a measured 20.6× metered-byte regression.
/// 2. A part may not be seeded by a value our own engine computed per
///    row. Every admissible seed is bounded by a plan-time constant, and
///    [`Lang::handoff_bound`] returning `None` is what refuses the cut.
/// 3. A predicate that engages no index does not cut and is not declined.
///
/// **`chain` is a parameter, which the design record's signature did not
/// carry.** Two of the three facts the builder needs — `source_of` and
/// `handoff_bound` — take the link, and [`Lowering`] holds only the
/// accumulated relation and the per-link dispositions. Nothing else about
/// the signature moves; the chain is borrowed and never mutated.
///
/// # Panics
///
/// If `chain` and `lowering.how` disagree in length. They come from one
/// [`super::fold::lower_chain`] call over one chain, which is the
/// documented invariant.
pub fn plan_of<L: Lang + ?Sized + 'static>(
    chain: &[L::Stage],
    lowering: Lowering<L>,
    cx: &PlanCx<'_>,
) -> Result<QueryPlan<L>, L::Err> {
    let Lowering { rel, how } = lowering;
    assert_eq!(
        chain.len(),
        how.len(),
        "plan_of: the chain and the fold's dispositions must come from one lower_chain call"
    );

    let mut parts: Vec<Part<L>> = Vec::new();

    // --- the source part(s) -----------------------------------------
    //
    // A disjunction whose sides resolve against different sources is one
    // statement per source, merged in our process — which is what the
    // shipped TraceQL planner's `generator_sqls` vector already does.
    let branches = rel.predicate.disjoint_or_branches();
    match &branches {
        Some(bs) if bs.len() > 1 => {
            // The SAME derivation `cuts_firing` uses, so the cut's
            // source list and the parts built beside it cannot disagree.
            let sources: Vec<SourceRef> = branch_sources_union(bs);
            for (i, (_, pred)) in bs.iter().enumerate() {
                let mut branch_rel = rel.clone();
                branch_rel.predicate = pred.clone();
                parts.push(Part::Sql(Box::new(SqlPart {
                    yields: boundary_output(&branch_rel),
                    rel: branch_rel,
                    seed: None,
                    issue: Issue::Once,
                    cut: if i == 0 {
                        None
                    } else {
                        Some(Cut::DisjointSources {
                            sources: sources.clone(),
                        })
                    },
                })));
            }
        }
        _ => {
            parts.push(Part::Sql(Box::new(SqlPart {
                yields: boundary_output(&rel),
                rel: rel.clone(),
                seed: None,
                issue: Issue::Once,
                cut: None,
            })));
        }
    }
    let mut last_sql_part = parts.len() - 1;

    // --- the links --------------------------------------------------
    let mut links: Vec<LinkOutcome> = Vec::with_capacity(how.len());
    // The open run of residual links that will become one engine part,
    // as (first link index, the part index it is promised).
    let mut engine_run: Option<(usize, usize)> = None;

    for (i, (stage, disp)) in chain.iter().zip(how.iter()).enumerate() {
        match disp {
            Disposition::Lowered(_) => {
                // Rule 1: a residual link mid-pipeline does not cut, so a
                // lowered link after one still belongs to the statement
                // the fold accumulated.
                links.push(LinkOutcome {
                    part: 0,
                    how: *disp,
                });
            }
            Disposition::Residual(_) => {
                let handoff = handoff_for::<L>(stage, &rel, cx);
                match handoff {
                    Some(h) => {
                        // The evaluator's way of owning this link is to
                        // send a second statement: it gets its own SQL
                        // part, not an engine part.
                        if let Some((start, part)) = engine_run.take() {
                            debug_assert_eq!(part, parts.len());
                            parts.push(Part::Engine { links: start..i });
                        }
                        let cut = cuts_firing::<L>(&rel, Some(&h), cx).into_iter().next();
                        let mut part_rel = rel.clone();
                        // The second read is over a different source and
                        // carries no predicate of its own: it is keyed on
                        // the seed alone.
                        part_rel.predicate = super::fold::Pred::True;
                        part_rel.limit = None;
                        let bound = h.bound;
                        let issue = match cut {
                            Some(Cut::HandoffExceedsBound { .. }) => {
                                Issue::PerSeed(Driver::Chunks {
                                    bound: bound.value(),
                                    chunk: chunk_for::<L>(bound.value(), cx),
                                })
                            }
                            _ => Issue::Once,
                        };
                        parts.push(Part::Sql(Box::new(SqlPart {
                            yields: boundary_output(&part_rel),
                            rel: part_rel,
                            seed: Some(Seed {
                                from_part: last_sql_part,
                                values: L::Handoff::default(),
                                bound,
                            }),
                            issue,
                            cut,
                        })));
                        last_sql_part = parts.len() - 1;
                        links.push(LinkOutcome {
                            part: last_sql_part,
                            how: *disp,
                        });
                    }
                    None => {
                        let (_, part) = *engine_run.get_or_insert((i, parts.len()));
                        links.push(LinkOutcome { part, how: *disp });
                    }
                }
            }
        }
    }
    if let Some((start, _)) = engine_run.take() {
        parts.push(Part::Engine {
            links: start..how.len(),
        });
    }

    // --- the request's LIMIT ----------------------------------------
    //
    // It changes how many times the last statement is sent, not which
    // statements there are: the same SQL part, issued once per page.
    if inexact_limit_fires(&rel, cx) {
        let page_rows = cx.config.keyset_page_rows;
        let over_fetch = cx.config.keyset_over_fetch;
        // Only a part sent ONCE becomes a page loop. A part already
        // issued once per seed chunk is bounded by the chunk driver, and
        // overwriting it would drop the bound the executor needs.
        if let Some(Part::Sql(p)) = parts.get_mut(last_sql_part)
            && matches!(p.issue, Issue::Once)
        {
            p.issue = Issue::PerSeed(Driver::Keyset {
                page_rows,
                over_fetch,
            });
            // `cut` answers "why is this its own statement"; when the
            // part already has an answer — it reads a second source —
            // that answer stands and only the issue count moves.
            if p.cut.is_none() {
                p.cut = Some(Cut::InexactLimit);
            }
        }
    }

    Ok(QueryPlan { parts, links })
}

/// The three facts a source-handoff cut needs, all three or nothing.
fn handoff_for<L: Lang + ?Sized + 'static>(
    stage: &L::Stage,
    rel: &Relation<L>,
    cx: &PlanCx<'_>,
) -> Option<Handoff> {
    let source = L::source_of(stage, rel);
    if source == rel.source_ref() {
        return None;
    }
    let key = L::handoff_key(stage, rel)?;
    let bound = L::handoff_bound(stage, rel, cx)?;
    Some(Handoff { source, key, bound })
}

/// The boundary kind, a function of the FINAL accumulated `shape` and
/// `exact` — never of where any prefix ended.
fn boundary_output<L: Lang + ?Sized>(rel: &Relation<L>) -> BoundaryOutput<L> {
    let h = L::Handoff::default();
    if rel.grouping.is_some() && rel.ordering.is_some() && rel.limit.is_some() {
        BoundaryOutput::Reduced(h)
    } else if rel.exact {
        BoundaryOutput::Exact(h)
    } else {
        BoundaryOutput::Candidates(h)
    }
}

// ---------------------------------------------------------------------
// The inspectable projection
// ---------------------------------------------------------------------

/// The plan, projected to strings, integers and enums.
///
/// **Not generic.** One renderer serves all three languages and the core
/// stays free of the wire format; the explain surface carries this as one
/// additive sibling key, `data.explain.plan` (docs/api.md §2.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PlanShape {
    pub parts: Vec<PartShape>,
    pub links: Vec<LinkShape>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum PartShape {
    Sql(SqlPartShape),
    Engine(EnginePartShape),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SqlPartShape {
    pub kind: &'static str,
    pub name: String,
    pub issue: &'static str,
    pub cut: Option<CutShape>,
    pub seed: Option<SeedShape>,
    pub yields: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EnginePartShape {
    pub kind: &'static str,
    pub links: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CutShape {
    pub why: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost: Option<HandoffCost>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SeedShape {
    pub from: usize,
    pub bound: BoundShape,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BoundShape {
    pub kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<&'static str>,
    pub value: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LinkShape {
    pub i: usize,
    pub part: usize,
    pub stage: String,
    pub how: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fidelity: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub why: Option<&'static str>,
}

impl<L: Lang + ?Sized> QueryPlan<L> {
    /// The plan projected for the explain surface. `stage_names` is the
    /// language's own spelling of each chain link, in chain order — the
    /// core has no way to name a `L::Stage`.
    pub fn shape(&self, stage_names: &[String]) -> PlanShape {
        let parts = self
            .parts
            .iter()
            .map(|p| match p {
                Part::Sql(s) => PartShape::Sql(SqlPartShape {
                    kind: "sql",
                    name: s.rel.source_ref().as_str().to_string(),
                    issue: s.issue.wire(),
                    cut: s.cut.as_ref().map(cut_shape),
                    seed: s.seed.as_ref().map(|seed| SeedShape {
                        from: seed.from_part,
                        bound: BoundShape {
                            kind: seed.bound.kind(),
                            name: seed.bound.name(),
                            value: seed.bound.value(),
                        },
                    }),
                    yields: s.yields.kind(),
                }),
                Part::Engine { links } => PartShape::Engine(EnginePartShape {
                    kind: "engine",
                    links: links.clone().collect(),
                }),
            })
            .collect();
        let links = self
            .links
            .iter()
            .enumerate()
            .map(|(i, l)| {
                let (how, fidelity, why) = match l.how {
                    Disposition::Lowered(Fidelity::Equivalent) => {
                        ("lowered", Some("equivalent"), None)
                    }
                    Disposition::Lowered(Fidelity::Wider) => ("lowered", Some("wider"), None),
                    Disposition::Residual(ResidualReason::Blocked(b)) => {
                        ("residual", None, Some(block_wire(b)))
                    }
                    Disposition::Residual(ResidualReason::Never(n)) => {
                        ("residual", None, Some(never_wire(n)))
                    }
                };
                LinkShape {
                    i,
                    part: l.part,
                    stage: stage_names.get(i).cloned().unwrap_or_default(),
                    how,
                    fidelity,
                    why,
                }
            })
            .collect();
        PlanShape { parts, links }
    }
}

fn cut_shape(cut: &Cut) -> CutShape {
    match cut {
        Cut::SourceHandoff { source, key } => CutShape {
            why: cut.why(),
            source: Some(source.as_str().to_string()),
            key: Some(key.to_string()),
            sources: Vec::new(),
            cost: None,
        },
        Cut::HandoffExceedsBound { cost } => CutShape {
            why: cut.why(),
            source: None,
            key: None,
            sources: Vec::new(),
            cost: Some(*cost),
        },
        Cut::DisjointSources { sources } => CutShape {
            why: cut.why(),
            source: None,
            key: None,
            sources: sources.iter().map(|s| s.as_str().to_string()).collect(),
            cost: None,
        },
        Cut::InexactLimit => CutShape {
            why: cut.why(),
            source: None,
            key: None,
            sources: Vec::new(),
            cost: None,
        },
    }
}

fn block_wire(b: super::fold::BlockReason) -> &'static str {
    use super::fold::BlockReason as B;
    match b {
        B::NotYetLowered => "not_yet_lowered",
        B::BodyNotStored => "body_not_stored",
        B::NameNotResolvable => "name_not_resolvable",
        B::NotExact => "not_exact",
        B::ShapeMismatch => "shape_mismatch",
        B::OrderingNotEstablished => "ordering_not_established",
        B::NotPushable => "not_pushable",
    }
}

fn never_wire(n: super::fold::NeverReason) -> &'static str {
    use super::fold::NeverReason as N;
    match n {
        N::NeedsUnwindowedRootRead => "needs_unwindowed_root_read",
        N::StructuralRelation => "structural_relation",
        N::NestedSetNumbering => "nested_set_numbering",
        N::TraceLevelIntrinsic => "trace_level_intrinsic",
        N::WholeQueryTypeFailure => "whole_query_type_failure",
        N::NoRowToComputeFrom => "no_row_to_compute_from",
        N::ResponseBuild => "response_build",
        N::NotASearchLink => "not_a_search_link",
    }
}

// ---------------------------------------------------------------------
// Hand-written `Clone`/`PartialEq`/`Eq`/`Debug` for the `L`-generic types
// (`#[derive]` would add an `L: Clone` bound; see `fold.rs`).
// ---------------------------------------------------------------------

impl<L: Lang + ?Sized> Clone for Seed<L> {
    fn clone(&self) -> Self {
        Self {
            from_part: self.from_part,
            values: self.values.clone(),
            bound: self.bound,
        }
    }
}

impl<L: Lang + ?Sized> std::fmt::Debug for Seed<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Seed")
            .field("from_part", &self.from_part)
            .field("values", &self.values)
            .field("bound", &self.bound)
            .finish()
    }
}

impl<L: Lang + ?Sized> PartialEq for Seed<L> {
    fn eq(&self, other: &Self) -> bool {
        self.from_part == other.from_part
            && self.values == other.values
            && self.bound == other.bound
    }
}

impl<L: Lang + ?Sized> Eq for Seed<L> {}

impl<L: Lang + ?Sized> Clone for SqlPart<L> {
    fn clone(&self) -> Self {
        Self {
            rel: self.rel.clone(),
            seed: self.seed.clone(),
            yields: self.yields.clone(),
            issue: self.issue,
            cut: self.cut.clone(),
        }
    }
}

impl<L: Lang + ?Sized> std::fmt::Debug for SqlPart<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlPart")
            .field("rel", &self.rel)
            .field("seed", &self.seed)
            .field("yields", &self.yields)
            .field("issue", &self.issue)
            .field("cut", &self.cut)
            .finish()
    }
}

impl<L: Lang + ?Sized> PartialEq for SqlPart<L> {
    fn eq(&self, other: &Self) -> bool {
        self.rel == other.rel
            && self.seed == other.seed
            && self.yields == other.yields
            && self.issue == other.issue
            && self.cut == other.cut
    }
}

impl<L: Lang + ?Sized> Eq for SqlPart<L> {}

impl<L: Lang + ?Sized> Clone for Part<L> {
    fn clone(&self) -> Self {
        match self {
            Part::Sql(p) => Part::Sql(p.clone()),
            Part::Engine { links } => Part::Engine {
                links: links.clone(),
            },
        }
    }
}

impl<L: Lang + ?Sized> std::fmt::Debug for Part<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Part::Sql(p) => f.debug_tuple("Sql").field(p).finish(),
            Part::Engine { links } => f.debug_struct("Engine").field("links", links).finish(),
        }
    }
}

impl<L: Lang + ?Sized> PartialEq for Part<L> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Part::Sql(a), Part::Sql(b)) => a == b,
            (Part::Engine { links: a }, Part::Engine { links: b }) => a == b,
            _ => false,
        }
    }
}

impl<L: Lang + ?Sized> Eq for Part<L> {}

impl<L: Lang + ?Sized> Clone for QueryPlan<L> {
    fn clone(&self) -> Self {
        Self {
            parts: self.parts.clone(),
            links: self.links.clone(),
        }
    }
}

impl<L: Lang + ?Sized> std::fmt::Debug for QueryPlan<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryPlan")
            .field("parts", &self.parts)
            .field("links", &self.links)
            .finish()
    }
}

impl<L: Lang + ?Sized> PartialEq for QueryPlan<L> {
    fn eq(&self, other: &Self) -> bool {
        self.parts == other.parts && self.links == other.links
    }
}

impl<L: Lang + ?Sized> Eq for QueryPlan<L> {}

// ---------------------------------------------------------------------
// Gates
// ---------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::compile::fold::{
        BlockReason, Capability, Col, ColSet, Fidelity, Lower, LowerCx, NeverReason, Pred,
        Provenance, Shape, SourceName, SourceTerm, lower_chain,
    };

    // --- a minimal language, so the CORE's rules are tested on their
    // --- own rather than through either real one -----------------------

    const IDX: SourceRef = SourceRef("idx");
    const ROWS: SourceRef = SourceRef("rows");
    const OTHER: SourceRef = SourceRef("other");

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestSource(&'static str);

    impl SourceName for TestSource {
        fn source_ref(&self) -> SourceRef {
            SourceRef(self.0)
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestShape(&'static str);

    impl Shape for TestShape {}

    #[derive(Debug, Clone, PartialEq, Eq, Default)]
    struct TestHandoff(Vec<u64>);

    /// One link per behaviour the plan builder has a rule for.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum TestStage {
        /// Lowers.
        Pushable,
        /// Residual, and reads no other source.
        Residual,
        /// Residual, reads another source, and its seed is bounded.
        BoundedHandoff,
        /// Residual, reads another source, and its seed is NOT bounded —
        /// the shape that must not open a part.
        UnboundedHandoff,
        /// Residual, reads another source, and its seed is bounded by a
        /// named constant too large to render in one statement.
        BigHandoff,
    }

    struct Tl;

    struct PushableLower;
    struct ResidualLower;

    impl Lower<Tl> for PushableLower {
        fn capability(&self, _s: &TestStage, _rel: &Relation<Tl>) -> Capability {
            Capability::Yes
        }
        fn apply(
            &self,
            _s: &TestStage,
            mut rel: Relation<Tl>,
            _cx: &LowerCx<'_, Tl>,
        ) -> Result<Relation<Tl>, ()> {
            rel.predicate = rel.predicate.clone().and(Pred::leaf("pushed = 1", IDX));
            Ok(rel)
        }
        fn residual_effect(&self, _s: &TestStage, rel: Relation<Tl>) -> Relation<Tl> {
            rel
        }
        fn fidelity(&self, _s: &TestStage, _rel: &Relation<Tl>) -> Fidelity {
            Fidelity::Equivalent
        }
    }

    impl Lower<Tl> for ResidualLower {
        fn capability(&self, s: &TestStage, _rel: &Relation<Tl>) -> Capability {
            match s {
                TestStage::Residual => Capability::No(BlockReason::NotYetLowered),
                _ => Capability::Never(NeverReason::NeedsUnwindowedRootRead),
            }
        }
        fn apply(
            &self,
            _s: &TestStage,
            rel: Relation<Tl>,
            _cx: &LowerCx<'_, Tl>,
        ) -> Result<Relation<Tl>, ()> {
            Ok(rel)
        }
        fn residual_effect(&self, _s: &TestStage, mut rel: Relation<Tl>) -> Relation<Tl> {
            rel.exact = false;
            rel
        }
    }

    static PUSHABLE: PushableLower = PushableLower;
    static RESIDUAL: ResidualLower = ResidualLower;

    impl Lang for Tl {
        type Stage = TestStage;
        type Source = TestSource;
        type ColExpr = String;
        type Shape = TestShape;
        type Handoff = TestHandoff;
        type Err = ();

        fn lower_of(stage: &TestStage) -> &'static dyn Lower<Tl> {
            match stage {
                TestStage::Pushable => &PUSHABLE,
                TestStage::Residual
                | TestStage::BoundedHandoff
                | TestStage::UnboundedHandoff
                | TestStage::BigHandoff => &RESIDUAL,
            }
        }

        fn source_of(stage: &TestStage, rel: &Relation<Tl>) -> SourceRef {
            match stage {
                TestStage::BoundedHandoff | TestStage::UnboundedHandoff | TestStage::BigHandoff => {
                    OTHER
                }
                _ => rel.source_ref(),
            }
        }

        fn handoff_key(stage: &TestStage, _rel: &Relation<Tl>) -> Option<Name> {
            match stage {
                TestStage::BoundedHandoff | TestStage::UnboundedHandoff | TestStage::BigHandoff => {
                    Some(Name::from("id"))
                }
                _ => None,
            }
        }

        fn handoff_bound(
            stage: &TestStage,
            _rel: &Relation<Tl>,
            cx: &PlanCx<'_>,
        ) -> Option<SeedBound> {
            match stage {
                TestStage::BoundedHandoff => Some(SeedBound::RequestLimit(cx.bounds.limit?)),
                TestStage::BigHandoff => Some(SeedBound::Constant {
                    name: "TEST_BIG_SEED",
                    value: 40_000,
                }),
                _ => None,
            }
        }

        /// 40 bytes and 2 AST elements per rendered value — the shape of a
        /// literal id list, and enough for the two ceilings to bite at a
        /// bound a test can name.
        fn handoff_cost(n: u64) -> HandoffCost {
            HandoffCost {
                text_bytes: 32 + n * 40,
                ast_elements: 8 + n * 2,
            }
        }
    }

    fn seed_rel() -> Relation<Tl> {
        Relation {
            source: SourceTerm::Base(TestSource("idx")),
            predicate: Pred::True,
            projection: vec![(Name::from("id"), "id".to_string())],
            cols: ColSet::Closed(vec![Col {
                name: Name::from("id"),
                provenance: Provenance::Stored,
            }]),
            grouping: None,
            ordering: None,
            limit: None,
            shape: TestShape("rows"),
            exact: true,
            depth: 0,
        }
    }

    fn bounds(limit: Option<u32>) -> RequestBounds {
        RequestBounds {
            start_ns: 0,
            end_ns: 1,
            step_ns: None,
            limit,
        }
    }

    fn plan(chain: &[TestStage], b: &RequestBounds, config: &PlanConfig) -> QueryPlan<Tl> {
        let lcx = LowerCx::<Tl>::new(b);
        let lowering = lower_chain::<Tl>(chain, seed_rel(), &lcx).expect("test fold");
        plan_of::<Tl>(chain, lowering, &PlanCx { bounds: b, config }).expect("test plan")
    }

    /// Issue #492 acceptance criterion 1.
    #[test]
    fn a_plan_is_never_empty_and_every_link_names_a_part() {
        let config = PlanConfig::default();
        for chain in [
            vec![],
            vec![TestStage::Pushable],
            vec![TestStage::Residual],
            vec![
                TestStage::Pushable,
                TestStage::Residual,
                TestStage::Pushable,
                TestStage::BoundedHandoff,
            ],
            vec![TestStage::UnboundedHandoff, TestStage::Residual],
        ] {
            let p = plan(&chain, &bounds(Some(20)), &config);
            assert!(
                !p.parts.is_empty(),
                "chain {chain:?}: a plan is never empty"
            );
            assert_eq!(
                p.links.len(),
                chain.len(),
                "chain {chain:?}: one entry per link"
            );
            for (i, l) in p.links.iter().enumerate() {
                assert!(
                    l.part < p.parts.len(),
                    "chain {chain:?}: link {i} names part {} of {}",
                    l.part,
                    p.parts.len()
                );
            }
        }
    }

    /// Issue #492 acceptance criterion 2: the four cuts are closed and in
    /// bijection with their recognisers. The `match` below has no `_`
    /// arm, so a fifth `Cut` variant fails to build this test rather than
    /// being added in silence.
    #[test]
    fn every_cut_variant_is_produced_by_exactly_one_rule() {
        let config = PlanConfig::default();
        // A bound big enough to blow the AST-element ceiling: 50,000/2.
        let over = SeedBound::Constant {
            name: "over",
            value: 40_000,
        };
        let under = SeedBound::RequestLimit(20);

        for want in [
            Cut::SourceHandoff {
                source: OTHER,
                key: Name::from("id"),
            },
            Cut::HandoffExceedsBound {
                cost: <Tl as Lang>::handoff_cost(over.value()),
            },
            Cut::DisjointSources {
                sources: vec![IDX, ROWS],
            },
            Cut::InexactLimit,
        ] {
            // One constructed situation per variant, built so that the
            // three rules it is NOT about cannot fire on it.
            let (rel, handoff, b) = match &want {
                Cut::SourceHandoff { source, key } => (
                    seed_rel(),
                    Some(Handoff {
                        source: *source,
                        key: key.clone(),
                        bound: under,
                    }),
                    bounds(None),
                ),
                Cut::HandoffExceedsBound { .. } => (
                    seed_rel(),
                    Some(Handoff {
                        source: OTHER,
                        key: Name::from("id"),
                        bound: over,
                    }),
                    bounds(None),
                ),
                Cut::DisjointSources { .. } => {
                    let mut rel = seed_rel();
                    rel.predicate = Pred::leaf("a = 1", IDX).or(Pred::leaf("b = 2", ROWS));
                    (rel, None, bounds(None))
                }
                Cut::InexactLimit => {
                    let mut rel = seed_rel();
                    rel.exact = false;
                    (rel, None, bounds(Some(20)))
                }
            };
            let cx = PlanCx {
                bounds: &b,
                config: &config,
            };
            let fired = cuts_firing::<Tl>(&rel, handoff.as_ref(), &cx);
            assert_eq!(
                fired,
                vec![want.clone()],
                "exactly one rule fires, and it produces {want:?}"
            );
        }
    }

    // -----------------------------------------------------------------
    // Issue #492, code review rounds 19 and 20: three ways the
    // disjoint-source partition asked the wrong question.
    //
    // Three defects in one function, so three gates, each of which fails
    // on its own break and stays green under the other two:
    //
    // * dropping the distribution (a branch carries only its own side)
    //   fails ONLY `a_conjunct_written_beside_the_disjunction_...`;
    // * keying a branch on `sources().first()` fails ONLY
    //   `a_branch_reading_two_sources_...`, whose keys collapse to one
    //   and whose partition then disappears;
    // * comparing the keys as ordered `Vec`s rather than as sets fails
    //   ONLY `equal_source_sets_reached_in_different_orders_...`.
    // -----------------------------------------------------------------

    /// A conjunct that sits outside the disjunction belongs to every
    /// branch: `(a ∨ b) ∧ t` is `(a ∧ t) ∨ (b ∧ t)`.
    ///
    /// The tenant spelling is the point. A branch relation whose
    /// predicate is `a` alone reads every tenant's rows, and the partition
    /// is the object the emitter will render statements from.
    ///
    /// This gate asserts the branch PREDICATES only, and the conjunct is
    /// written AFTER the disjunction, so that it is insensitive to the
    /// source-key half: under a first-source key the two branches still
    /// key on `idx` and `rows`, the partition survives and the predicates
    /// are unchanged. The keys are `a_branch_reading_two_sources_...`'s
    /// subject, and each half therefore reddens one gate and not the
    /// other.
    #[test]
    fn a_conjunct_written_beside_the_disjunction_reaches_every_branch() {
        let tenant = Pred::leaf("tenant_id = 7", IDX);
        let p = Pred::leaf("a = 1", IDX)
            .or(Pred::leaf("b = 2", ROWS))
            .and(tenant.clone());

        let bs = p
            .disjoint_or_branches()
            .expect("a disjunction over two sources under a conjunction partitions");
        assert_eq!(bs.len(), 2, "one branch per side: {bs:?}");
        let preds: Vec<&Pred> = bs.iter().map(|(_, pred)| pred).collect();
        assert_eq!(
            preds,
            vec![
                &Pred::leaf("a = 1", IDX).and(tenant.clone()),
                &Pred::leaf("b = 2", ROWS).and(tenant.clone()),
            ],
            "every branch carries the tenant conjunct, in its written position"
        );

        // And the same through `plan_of`, which is where the branch
        // relations are actually built (the site the finding names).
        let config = PlanConfig::default();
        let b = bounds(None);
        let mut rel = seed_rel();
        rel.predicate = p.clone();
        let plan = plan_of::<Tl>(
            &[],
            Lowering {
                rel,
                how: Vec::new(),
            },
            &PlanCx {
                bounds: &b,
                config: &config,
            },
        )
        .expect("plan");
        let sql: Vec<&SqlPart<Tl>> = plan
            .parts
            .iter()
            .filter_map(|p| match p {
                Part::Sql(s) => Some(&**s),
                Part::Engine { .. } => None,
            })
            .collect();
        assert_eq!(sql.len(), 2, "one statement per branch: {:?}", plan.parts);
        for (i, part) in sql.iter().enumerate() {
            assert_eq!(
                part.rel.predicate, bs[i].1,
                "branch relation {i} carries the whole predicate that applies to it"
            );
        }
    }

    /// A branch that reads two sources is ONE branch — an `AND` over two
    /// sources is one statement's job — and its key names both of them.
    ///
    /// This gate is insensitive to the distribution half: the spine here
    /// holds nothing but the disjunction, so there is no conjunct to
    /// distribute and a build that never distributes leaves it green.
    #[test]
    fn a_branch_reading_two_sources_stays_one_branch_keyed_on_both() {
        let spanning = Pred::leaf("a = 1", IDX).and(Pred::leaf("b = 2", ROWS));
        let p = spanning.clone().or(Pred::leaf("c = 3", IDX));

        let bs = p.disjoint_or_branches().expect(
            "the two sides read different source SETS ({idx, rows} and {idx}), so this \
             partitions — a key taken from the first source alone collapses them to one and \
             loses the partition entirely",
        );
        assert_eq!(bs.len(), 2, "the spanning side is one branch: {bs:?}");
        assert_eq!(
            bs[0],
            (vec![IDX, ROWS], spanning),
            "the spanning branch keeps both leaves and names both sources"
        );
        assert_eq!(bs[1], (vec![IDX], Pred::leaf("c = 3", IDX)));
        // The invariant that makes the key checkable rather than
        // stipulated: a branch's key IS its predicate's source list.
        for (key, pred) in &bs {
            assert_eq!(key, &pred.sources(), "branch key vs predicate sources");
        }
        assert_eq!(
            p.disjoint_or_sources(),
            Some(vec![IDX, ROWS]),
            "the cut names every source the partition spans"
        );

        // `plan_of` names the same set on the cut it attaches.
        let config = PlanConfig::default();
        let b = bounds(None);
        let mut rel = seed_rel();
        rel.predicate = p.clone();
        let plan = plan_of::<Tl>(
            &[],
            Lowering {
                rel,
                how: Vec::new(),
            },
            &PlanCx {
                bounds: &b,
                config: &config,
            },
        )
        .expect("plan");
        let cuts: Vec<&Cut> = plan
            .parts
            .iter()
            .filter_map(|p| match p {
                Part::Sql(s) => s.cut.as_ref(),
                Part::Engine { .. } => None,
            })
            .collect();
        assert_eq!(
            cuts,
            vec![&Cut::DisjointSources {
                sources: vec![IDX, ROWS]
            }],
            "one cut, naming both sources: {:?}",
            plan.parts
        );
    }

    /// Two branches that read the SAME set of sources are not a
    /// partition, however that set was spelled — the qualifying question
    /// is about a set, so the comparison must be about a set.
    ///
    /// The two branches here differ in nothing but the order their
    /// sources are first seen: `{idx, rows, other}` reached as
    /// `[idx, rows, other]` and as `[idx, other, rows]`. Comparing the
    /// keys with `Vec` equality — which is what round 19 shipped — makes
    /// those two distinct keys and partitions a disjunction that reads
    /// one set of sources on both sides.
    ///
    /// **Why three sources and not the two-source spelling.** The
    /// smallest form of this is `[idx, rows]` against `[rows, idx]`, but
    /// that input ALSO reddens under the `sources().first()` break —
    /// `idx` and `rows` are different first sources, so it cannot tell
    /// "the comparison carries order" apart from "the key is wrong", and
    /// the one-break-one-gate separation the previous round established
    /// would be lost. Holding the first source equal and permuting the
    /// rest leaves the first-source key blind to this input, so this gate
    /// answers for the comparison alone.
    #[test]
    fn equal_source_sets_reached_in_different_orders_are_not_a_partition() {
        let left = Pred::leaf("a = 1", IDX)
            .and(Pred::leaf("b = 2", ROWS))
            .and(Pred::leaf("c = 3", OTHER));
        let right = Pred::leaf("d = 4", IDX)
            .and(Pred::leaf("e = 5", OTHER))
            .and(Pred::leaf("f = 6", ROWS));
        // The premise the gate rests on, asserted rather than assumed:
        // same set, different first-seen order, same first source.
        assert_eq!(left.sources(), vec![IDX, ROWS, OTHER]);
        assert_eq!(right.sources(), vec![IDX, OTHER, ROWS]);
        assert_ne!(left.sources(), right.sources(), "the orders differ");
        assert_eq!(
            left.sources().iter().copied().collect::<BTreeSet<_>>(),
            right.sources().iter().copied().collect::<BTreeSet<_>>(),
            "the sets do not"
        );
        assert_eq!(
            left.sources().first(),
            right.sources().first(),
            "and the first source is the same, so a first-source key \
             cannot distinguish these two branches"
        );

        let p = left.or(right);
        assert_eq!(
            p.disjoint_or_branches(),
            None,
            "both sides read {{idx, rows, other}}; one statement holds them"
        );
        assert_eq!(p.disjoint_or_sources(), None);

        // And no `DisjointSources` cut through `plan_of`, which is where
        // a spurious partition would become a second statement.
        let config = PlanConfig::default();
        let b = bounds(None);
        let mut rel = seed_rel();
        rel.predicate = p.clone();
        let plan = plan_of::<Tl>(
            &[],
            Lowering {
                rel,
                how: Vec::new(),
            },
            &PlanCx {
                bounds: &b,
                config: &config,
            },
        )
        .expect("plan");
        let sql = plan
            .parts
            .iter()
            .filter(|p| matches!(p, Part::Sql(_)))
            .count();
        assert_eq!(sql, 1, "one statement, not two: {:?}", plan.parts);
        assert!(
            !plan.parts.iter().any(|p| matches!(
                p,
                Part::Sql(s) if matches!(s.cut, Some(Cut::DisjointSources { .. }))
            )),
            "no disjoint-source cut: {:?}",
            plan.parts
        );
    }

    /// What the flattening does and does not reach, pinned so the doc
    /// comment on [`Pred::disjoint_or_branches`] has a failure mode.
    ///
    /// A disjunction nested under another `OR` IS partitioned, into all
    /// of its branches, because `flatten_or` flattens the whole `OR`
    /// spine before the qualifying question is asked. A disjunction under
    /// a `Not` is not: `Not` is opaque to both spines, and branches taken
    /// from inside it would merge as `¬a ∨ ¬b` where the query asked for
    /// `¬a ∧ ¬b`.
    #[test]
    fn a_nested_or_is_flattened_and_a_disjunction_under_a_not_is_left_whole() {
        let nested =
            Pred::leaf("a = 1", IDX).or(Pred::leaf("b = 2", ROWS).or(Pred::leaf("c = 3", IDX)));
        let bs = nested
            .disjoint_or_branches()
            .expect("the flattened spine reads two source sets");
        assert_eq!(
            bs,
            vec![
                (vec![IDX], Pred::leaf("a = 1", IDX)),
                (vec![ROWS], Pred::leaf("b = 2", ROWS)),
                (vec![IDX], Pred::leaf("c = 3", IDX)),
            ],
            "the nesting is flattened: three branches, not two"
        );

        let negated = Pred::Not(Box::new(
            Pred::leaf("a = 1", IDX).or(Pred::leaf("b = 2", ROWS)),
        ));
        assert_eq!(
            negated.disjoint_or_branches(),
            None,
            "a disjunction under a `Not` is one statement's whole predicate"
        );
    }

    /// Issue #492 acceptance criterion 3: a seed with no plan-time bound
    /// refuses the cut.
    #[test]
    fn an_unbounded_handoff_refuses_a_cut_and_leaves_the_links_in_the_engine_part() {
        let config = PlanConfig::default();
        let b = bounds(Some(20));

        let refused = plan(
            &[TestStage::Pushable, TestStage::UnboundedHandoff],
            &b,
            &config,
        );
        let sql_parts = refused
            .parts
            .iter()
            .filter(|p| matches!(p, Part::Sql(_)))
            .count();
        let engine_parts = refused
            .parts
            .iter()
            .filter(|p| matches!(p, Part::Engine { .. }))
            .count();
        assert_eq!(sql_parts, 1, "an unbounded seed opens no second statement");
        assert_eq!(engine_parts, 1, "the link stays in the engine part");
        assert!(
            matches!(refused.parts[1], Part::Engine { .. }),
            "parts: {:?}",
            refused.parts
        );

        // The control: the SAME chain with a bounded seed DOES cut, so
        // the assertion above is about the bound and not about the shape.
        let taken = plan(
            &[TestStage::Pushable, TestStage::BoundedHandoff],
            &b,
            &config,
        );
        assert_eq!(
            taken
                .parts
                .iter()
                .filter(|p| matches!(p, Part::Sql(_)))
                .count(),
            2,
            "a bounded seed opens a second statement: {:?}",
            taken.parts
        );
        let Part::Sql(second) = &taken.parts[1] else {
            panic!("parts: {:?}", taken.parts)
        };
        assert_eq!(
            second.cut,
            Some(Cut::SourceHandoff {
                source: OTHER,
                key: Name::from("id")
            })
        );
        assert_eq!(second.seed.as_ref().map(|s| s.from_part), Some(0));
        assert_eq!(
            second.seed.as_ref().map(|s| s.bound),
            Some(SeedBound::RequestLimit(20))
        );
    }

    /// A residual link between two lowered ones does not open a part —
    /// the fold continues and the later link contributes to the SAME
    /// statement. Withdrawing this rule is the measured metered-byte
    /// regression the fold's shape exists to prevent.
    #[test]
    fn a_residual_link_between_two_lowered_ones_does_not_open_a_part() {
        let config = PlanConfig::default();
        let p = plan(
            &[
                TestStage::Pushable,
                TestStage::Residual,
                TestStage::Pushable,
            ],
            &bounds(None),
            &config,
        );
        assert_eq!(
            p.parts.iter().filter(|p| matches!(p, Part::Sql(_))).count(),
            1,
            "one statement: {:?}",
            p.parts
        );
        assert_eq!(p.links[0].part, 0);
        assert_eq!(p.links[2].part, 0, "the link AFTER the residual one");
        assert_ne!(p.links[1].part, 0, "the residual link is engine work");
    }

    /// A seed too large to render in one statement is chunked, and the
    /// chunk is the largest that stays under both ceilings.
    #[test]
    fn an_oversized_seed_becomes_a_chunked_issue_under_both_ceilings() {
        let config = PlanConfig::default();
        // No request limit, so the inexact-limit rule cannot fire and
        // the issue this asserts is the chunking one.
        let b = bounds(None);
        let p = plan(&[TestStage::BigHandoff], &b, &config);
        let Part::Sql(second) = &p.parts[1] else {
            panic!("parts: {:?}", p.parts)
        };
        let Issue::PerSeed(Driver::Chunks { bound, chunk }) = second.issue else {
            panic!("issue: {:?}", second.issue)
        };
        assert_eq!(bound, 40_000);
        let fits = <Tl as Lang>::handoff_cost(chunk);
        assert!(fits.ast_elements <= config.max_ast_elements);
        assert!(fits.text_bytes <= config.max_query_text_bytes);
        let one_more = <Tl as Lang>::handoff_cost(chunk + 1);
        assert!(
            one_more.ast_elements > config.max_ast_elements
                || one_more.text_bytes > config.max_query_text_bytes,
            "the chunk is the LARGEST that fits: {chunk} then {one_more:?}"
        );
    }
}
