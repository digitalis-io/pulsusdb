//! The fold: one linear chain of links, folded left, with a per-link
//! disposition and an accumulated relation (issue #492, wave 1).
//!
//! The chain is
//!
//! ```text
//! Source -> S1 -> S2 -> ... -> Sn -> Order -> Limit -> Emit
//!            \___ the language's own stages ___/   \__ synthesised __/
//! ```
//!
//! and the three synthesised links are ordinary links, which is the whole
//! answer to "position matters for some stages": a `LIMIT` is lowerable
//! only when an ordering is already established, and that is a
//! precondition on **accumulated state** rather than a rule about
//! `LIMIT`.
//!
//! > **A link either lowers, or becomes residual and the fold continues.
//! > Blocking is emergent from accumulated state — column provenance,
//! > shape, `exact` — never from position.**
//!
//! The residual link still applies its **state effect** ([`Lower::residual_effect`]).
//! That is the part that makes blocking work: a `line_format` that does
//! not lower still rewrites `body`'s provenance, so a later line filter
//! finds nothing to lower against and becomes residual too. Nothing is
//! skipped silently. Which provenance it rewrites to — [`Provenance::Computed`]
//! or [`Provenance::EvaluatorOnly`] — is decided by whether a SQL
//! expression for the rewrite has been written; see [`Provenance`].

use std::fmt;
use std::marker::PhantomData;
use std::rc::Rc;

use super::plan::{PlanCx, SeedBound, SourceRef};

// ---------------------------------------------------------------------
// Small shared vocabulary
// ---------------------------------------------------------------------

/// A column or label name, in the language's own spelling.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Name(String);

impl Name {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for Name {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl fmt::Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// One SQL expression fragment, already rendered in the database's own
/// dialect. The core never builds one and never parses one — fragment
/// construction is per-language work (the sharing boundary), and this
/// type exists so the core can carry a fragment without reading it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SqlExpr(pub String);

impl SqlExpr {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SqlExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Where a column's value comes from — the fact that derives blocking
/// rather than restating it.
///
/// A stage that rewrites a column records one of two things, and which
/// one is decided by whether a SQL expression for that rewrite exists:
///
/// * **It exists** — the stage records [`Provenance::Computed`] carrying
///   the expression, [`Provenance::resolve`] answers `Some`, and a later
///   link that reads the column lowers against the rewritten expression
///   instead of the stored one.
/// * **It does not exist** — the stage records
///   [`Provenance::EvaluatorOnly`], `resolve` answers `None`, and a later
///   link that reads the column has nothing to lower against and becomes
///   residual.
///
/// **In wave 1 every rewriting stage in both languages takes the second
/// case**, `line_format` and `label_format` as expected and `decolorize`
/// and `unpack` by an approved deviation from the design record's rows,
/// which name the first for those two. Nothing renders the SGR strip or
/// the `_entry` unwrap into SQL yet, and a `Computed` whose expression
/// does not exist would let a following filter lower against something no
/// planner emits — which moves the measured zero the walk-agreement gates
/// assert. `logql::compile::mark_line_rewritten` is the one place that
/// records it and carries the same note; when the wave that writes those
/// expressions lands, that call becomes `Computed` and the zero moves
/// deliberately.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Provenance {
    /// The stored column, readable as it is.
    Stored,
    /// A stage computed it, and SQL can reproduce the computation.
    Computed(SqlExpr),
    /// A stage computed it and SQL cannot reproduce the computation, so
    /// only the evaluator knows the value.
    EvaluatorOnly,
}

impl Provenance {
    /// The SQL expression this column resolves to, if any. `None` is the
    /// answer that makes a later link residual.
    pub fn resolve(&self, stored: &Name) -> Option<SqlExpr> {
        match self {
            Provenance::Stored => Some(SqlExpr::new(stored.as_str())),
            Provenance::Computed(e) => Some(e.clone()),
            Provenance::EvaluatorOnly => None,
        }
    }
}

/// One known column and where its value comes from.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Col {
    pub name: Name,
    pub provenance: Provenance,
}

impl Col {
    pub fn stored(name: impl Into<Name>) -> Self {
        Self {
            name: name.into(),
            provenance: Provenance::Stored,
        }
    }

    pub fn computed(name: impl Into<Name>, expr: SqlExpr) -> Self {
        Self {
            name: name.into(),
            provenance: Provenance::Computed(expr),
        }
    }

    pub fn evaluator_only(name: impl Into<Name>) -> Self {
        Self {
            name: name.into(),
            provenance: Provenance::EvaluatorOnly,
        }
    }
}

impl From<&str> for Col {
    fn from(s: &str) -> Self {
        Col::stored(Name::from(s))
    }
}

/// The identity of an open column source, so that [`ColSet`] can compare
/// two of them without the core knowing what either resolves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OpenSourceId(pub &'static str);

/// A set of column names that is not known at plan time — a LogQL
/// parser's labels, a TraceQL attribute — each of which can resolve a
/// name to a SQL expression, or refuse.
///
/// `Debug` is a supertrait and [`OpenSource::id`] is an identity, because
/// [`ColSet`] must be `Clone + PartialEq + Eq + Debug` and `#[derive]`
/// cannot see through `dyn`.
pub trait OpenSource: fmt::Debug {
    /// `Some(expr)` if this name is resolvable to SQL here; `None` if the
    /// only way to know its value is to run the stage in the evaluator.
    fn resolve(&self, name: &Name) -> Option<SqlExpr>;
    fn id(&self) -> OpenSourceId;
}

/// The accumulated column set.
///
/// **This is not an accommodation for one language.** A TraceQL
/// attribute (`.foo`, `span.bar`) is a name that is not a column and
/// resolves to an attribute-index read; a LogQL `| json` label is a name
/// that is not a column and resolves to a JSON extraction over the line.
/// They are one concept.
///
/// `Rc`, not `Box`: `ColSet` must be `Clone`, and a boxed trait object is
/// not.
#[derive(Debug, Clone)]
pub enum ColSet {
    Closed(Vec<Col>),
    Open {
        known: Vec<Col>,
        from: Vec<Rc<dyn OpenSource>>,
    },
}

impl ColSet {
    pub fn known(&self) -> &[Col] {
        match self {
            ColSet::Closed(k) | ColSet::Open { known: k, .. } => k,
        }
    }

    pub fn known_mut(&mut self) -> &mut Vec<Col> {
        match self {
            ColSet::Closed(k) | ColSet::Open { known: k, .. } => k,
        }
    }

    /// The provenance recorded for `name`, if the set knows it.
    pub fn provenance(&self, name: &Name) -> Option<&Provenance> {
        self.known()
            .iter()
            .find(|c| &c.name == name)
            .map(|c| &c.provenance)
    }

    /// Replaces `name`'s provenance, adding the column if it is new.
    /// This is the whole of a rewriting stage's residual state effect.
    pub fn set_provenance(&mut self, name: &Name, p: Provenance) {
        let known = self.known_mut();
        match known.iter_mut().find(|c| &c.name == name) {
            Some(c) => c.provenance = p,
            None => known.push(Col {
                name: name.clone(),
                provenance: p,
            }),
        }
    }

    /// `Some(expr)` if this name resolves to SQL here — a known column
    /// with a resolvable provenance, or an open source that answers.
    /// `None` is what makes a link that reads the name residual.
    pub fn resolve(&self, name: &Name) -> Option<SqlExpr> {
        if let Some(c) = self.known().iter().find(|c| &c.name == name) {
            return c.provenance.resolve(name);
        }
        match self {
            ColSet::Closed(_) => None,
            ColSet::Open { from, .. } => from.iter().find_map(|s| s.resolve(name)),
        }
    }

    /// Widens the set with an open source.
    pub fn widen(self, source: Rc<dyn OpenSource>) -> Self {
        match self {
            ColSet::Closed(known) => ColSet::Open {
                known,
                from: vec![source],
            },
            ColSet::Open { known, mut from } => {
                from.push(source);
                ColSet::Open { known, from }
            }
        }
    }

    /// Removes the named columns — `| drop`'s effect.
    pub fn without(mut self, names: &[Name]) -> Self {
        self.known_mut().retain(|c| !names.contains(&c.name));
        self
    }

    /// Keeps only the named columns — `| keep`'s effect, the complement
    /// of [`ColSet::without`].
    pub fn only(mut self, names: &[Name]) -> Self {
        self.known_mut().retain(|c| names.contains(&c.name));
        self
    }
}

impl PartialEq for ColSet {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ColSet::Closed(a), ColSet::Closed(b)) => a == b,
            (ColSet::Open { known: a, from: fa }, ColSet::Open { known: b, from: fb }) => {
                a == b && fa.len() == fb.len() && fa.iter().zip(fb).all(|(x, y)| x.id() == y.id())
            }
            _ => false,
        }
    }
}

impl Eq for ColSet {}

// ---------------------------------------------------------------------
// The predicate lattice (design record §2.4)
// ---------------------------------------------------------------------

/// The predicate under construction, as a boolean tree rather than text.
///
/// The one invariant is **`orig ⟹ sql`** — the emitted predicate is
/// implied by the expression it came from, so the SQL result is always a
/// superset of the true match set. `exact` (on [`Relation`]) additionally
/// asserts `orig ⟺ sql`.
///
/// Every leaf carries the [`SourceRef`] it reads, which is what lets the
/// plan builder recognise an `OR` whose two sides resolve against
/// different sources — one `WHERE` cannot hold them.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Pred {
    /// `1` — the seed, and what an unlowerable conjunct becomes.
    True,
    Leaf {
        sql: SqlExpr,
        source: SourceRef,
    },
    And(Box<Pred>, Box<Pred>),
    Or(Box<Pred>, Box<Pred>),
    Not(Box<Pred>),
}

impl Pred {
    pub fn leaf(sql: impl Into<String>, source: SourceRef) -> Self {
        Pred::Leaf {
            sql: SqlExpr::new(sql),
            source,
        }
    }

    /// Conjoins one more term. Dropping a conjunct is safe (it widens);
    /// dropping a disjunct is not, which is why there is no `or` helper
    /// that silently discards a side.
    pub fn and(self, other: Pred) -> Self {
        Pred::And(Box::new(self), Box::new(other))
    }

    pub fn or(self, other: Pred) -> Self {
        Pred::Or(Box::new(self), Box::new(other))
    }

    /// Every distinct source any leaf reads, in first-seen order.
    pub fn sources(&self) -> Vec<SourceRef> {
        let mut out = Vec::new();
        self.collect_sources(&mut out);
        out
    }

    fn collect_sources(&self, out: &mut Vec<SourceRef>) {
        match self {
            Pred::True => {}
            Pred::Leaf { source, .. } => {
                if !out.contains(source) {
                    out.push(*source);
                }
            }
            Pred::And(a, b) | Pred::Or(a, b) => {
                a.collect_sources(out);
                b.collect_sources(out);
            }
            Pred::Not(a) => a.collect_sources(out),
        }
    }

    /// The disjoint-source partition of this predicate: one entry per
    /// branch of the first qualifying disjunction on the top-level
    /// conjunctive spine, each carrying **the whole predicate that
    /// branch's statement applies** and **every source that predicate
    /// reads**.
    ///
    /// A disjunction qualifies when its branches do not all read the same
    /// set of sources — one `WHERE` cannot hold two sides that live in
    /// different tables, while an `AND` over two sources is one
    /// statement's job and needs no partition.
    ///
    /// **Every other conjunct on the spine is distributed into each
    /// branch**, in its written position, because `t ∧ (a ∨ b)` is
    /// `(t ∧ a) ∨ (t ∧ b)`: a branch carrying only `a` would mean less
    /// than the query did, and a dropped tenant conjunct is a cross-tenant
    /// read the moment a statement is rendered from it (issue #492, code
    /// review round 19).
    ///
    /// **A branch that spans sources stays one branch**, keyed on all of
    /// them: the statement for it reads every source its predicate names,
    /// which is the same "an `AND` over two sources is one statement"
    /// rule. Keying on the first source alone silently lost the rest —
    /// the second half of the same finding. The key is therefore always
    /// [`Pred::sources`] of the branch's own predicate, which is an
    /// equality a test can assert.
    ///
    /// **Where this stops, and why it is `None` rather than a partial
    /// answer.** A disjunction nested under another `OR`, or under a
    /// `Not`, is not partitioned: an `OR` sibling cannot be carried into a
    /// branch the way a conjunct can, and `¬(a ∨ b)` is `¬a ∧ ¬b`, a
    /// conjunction. `None` leaves one statement carrying the whole
    /// predicate, which is conservative; recursing there would return
    /// branches that mean less than the query.
    pub fn disjoint_or_branches(&self) -> Option<Vec<(Vec<SourceRef>, Pred)>> {
        let mut spine: Vec<&Pred> = Vec::new();
        self.collect_conjuncts(&mut spine);
        for (i, conjunct) in spine.iter().enumerate() {
            if !matches!(conjunct, Pred::Or(_, _)) {
                continue;
            }
            let mut branches = Vec::new();
            flatten_or(conjunct, &mut branches);
            let keyed: Vec<(Vec<SourceRef>, Pred)> = branches
                .into_iter()
                .map(|branch| {
                    // The spine rebuilt with `branch` in the
                    // disjunction's place: every other conjunct kept, in
                    // the order it was written.
                    let whole = spine
                        .iter()
                        .enumerate()
                        .fold(None::<Pred>, |acc, (j, term)| {
                            let term = if j == i {
                                branch.clone()
                            } else {
                                (*term).clone()
                            };
                            Some(match acc {
                                None => term,
                                Some(a) => a.and(term),
                            })
                        })
                        .expect("the spine holds at least the disjunction itself");
                    (whole.sources(), whole)
                })
                .collect();
            let mut distinct: Vec<&Vec<SourceRef>> = Vec::new();
            for (k, _) in &keyed {
                if !distinct.contains(&k) {
                    distinct.push(k);
                }
            }
            if distinct.len() > 1 {
                return Some(keyed);
            }
        }
        None
    }

    /// The distinct sources of [`Pred::disjoint_or_branches`], in
    /// first-seen order.
    pub fn disjoint_or_sources(&self) -> Option<Vec<SourceRef>> {
        self.disjoint_or_branches()
            .as_deref()
            .map(branch_sources_union)
    }

    /// Flattens this predicate's `AND` spine. [`Pred::True`] is the
    /// conjunction's identity and contributes nothing; `Not` is opaque,
    /// so a disjunction under one is a conjunct and never a partition.
    fn collect_conjuncts<'a>(&'a self, out: &mut Vec<&'a Pred>) {
        match self {
            Pred::True => {}
            Pred::And(a, b) => {
                a.collect_conjuncts(out);
                b.collect_conjuncts(out);
            }
            other => out.push(other),
        }
    }
}

/// Every source named by any branch of a partition, in first-seen order —
/// the one derivation of `Cut::DisjointSources`' source list, so the cut
/// and the parts built beside it can never name different sets.
pub fn branch_sources_union(branches: &[(Vec<SourceRef>, Pred)]) -> Vec<SourceRef> {
    let mut seen: Vec<SourceRef> = Vec::new();
    for (sources, _) in branches {
        for s in sources {
            if !seen.contains(s) {
                seen.push(*s);
            }
        }
    }
    seen
}

/// Flattens a right-/left-nested `OR` spine into its branches.
fn flatten_or(p: &Pred, out: &mut Vec<Pred>) {
    match p {
        Pred::Or(a, b) => {
            flatten_or(a, out);
            flatten_or(b, out);
        }
        other => out.push(other.clone()),
    }
}

impl fmt::Display for Pred {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Pred::True => f.write_str("1"),
            Pred::Leaf { sql, .. } => f.write_str(sql.as_str()),
            Pred::And(a, b) => write!(f, "{a} AND {b}"),
            Pred::Or(a, b) => write!(f, "({a} OR {b})"),
            Pred::Not(a) => write!(f, "NOT ({a})"),
        }
    }
}

// ---------------------------------------------------------------------
// The accumulated relation
// ---------------------------------------------------------------------

/// A shape is per-language and the core knows only that two of them can
/// be compared: TraceQL's are spans / traces / groups, LogQL's are lines
/// / samples / series. One enum over both would be a union with
/// per-language invalid states, and every `match` on it would acquire
/// unreachable arms.
pub trait Shape: Clone + Eq + fmt::Debug {}

/// How a language names one readable source, so the core can ask a
/// relation which source it reads without knowing the language.
pub trait SourceName {
    fn source_ref(&self) -> SourceRef;
}

/// `Base(source)`, or a relation wrapped as a subquery when a clause slot
/// collides (ADR 0008 D1's wrap rule).
///
/// `Clone`/`PartialEq`/`Debug` are written by hand rather than derived,
/// here and on every other type generic over `L`: a derive would add an
/// `L: Clone` bound, and `L` is a marker that is never a value.
pub enum SourceTerm<L: Lang + ?Sized> {
    Base(L::Source),
    Wrapped(Box<Relation<L>>),
}

impl<L: Lang + ?Sized> SourceTerm<L> {
    pub fn source_ref(&self) -> SourceRef {
        match self {
            SourceTerm::Base(s) => s.source_ref(),
            SourceTerm::Wrapped(inner) => inner.source.source_ref(),
        }
    }
}

/// A grouping slot. `Some` is what makes a second grouping stage wrap
/// rather than overwrite.
pub struct Grouping<L: Lang + ?Sized> {
    pub keys: Vec<L::ColExpr>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SortDir {
    Asc,
    Desc,
}

pub struct Ordering<L: Lang + ?Sized> {
    pub keys: Vec<(L::ColExpr, SortDir)>,
}

/// The SQL under construction, as an algebra term rather than text.
/// Rendering happens once, at the boundary (ADR 0008).
///
/// **`cols` is part of the accumulated state**, not of the projection: a
/// stage widens, narrows or rewrites the set of names the *next* stage
/// may read, and that is a different thing from the columns the statement
/// returns. Every "cols unchanged / widened / narrowed" row in the design
/// record's link tables is a statement about this field.
pub struct Relation<L: Lang + ?Sized> {
    pub source: SourceTerm<L>,
    /// An OVER-APPROXIMATING conjunction: `orig ⟹ sql`.
    pub predicate: Pred,
    pub projection: Vec<(Name, L::ColExpr)>,
    /// The names a later link may read, and where each one's value comes
    /// from.
    pub cols: ColSet,
    pub grouping: Option<Grouping<L>>,
    pub ordering: Option<Ordering<L>>,
    pub limit: Option<u64>,
    pub shape: L::Shape,
    /// Does `predicate` mean exactly what the chain so far means?
    pub exact: bool,
    /// Subquery nesting, for ADR 0008's wrap rule.
    pub depth: u8,
}

impl<L: Lang + ?Sized> Relation<L> {
    /// The identity of the source this relation reads — what
    /// [`Lang::source_of`] is compared against.
    pub fn source_ref(&self) -> SourceRef {
        self.source.source_ref()
    }
}

// ---------------------------------------------------------------------
// Capability, disposition, fidelity
// ---------------------------------------------------------------------

/// Why a link that is lowerable in principle did not lower here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BlockReason {
    /// No SQL form has been written for this link yet.
    NotYetLowered,
    /// The link reads the line and the line is no longer a stored column.
    BodyNotStored,
    /// A name the link reads does not resolve to SQL in the accumulated
    /// column set.
    NameNotResolvable,
    /// The accumulated predicate is a superset, and this link's answer
    /// over a superset would be wrong rather than merely wide.
    NotExact,
    /// The link's input shape does not match the accumulated shape.
    ShapeMismatch,
    /// The link needs an ordering that no earlier link established.
    OrderingNotEstablished,
    /// The link renders no predicate any index could serve, and the
    /// evaluator owns it (a LogQL `ip()` line filter).
    NotPushable,
}

/// Why a link can never lower, in any state. Documentation carried in
/// the type, so that nobody later reads one of these as unfinished work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NeverReason {
    /// The response's root summary is read trace-wide with no time
    /// bound, because the true root may predate the search window.
    NeedsUnwindowedRootRead,
    /// The relation holds between two spans of one trace, over a span
    /// set our own batching defines.
    StructuralRelation,
    /// A modified-preorder numbering computed per trace at query time;
    /// no stored column carries it.
    NestedSetNumbering,
    /// Resolved from a co-load that is deliberately trace-wide and
    /// unwindowed; a window-bounded statement cannot read those rows.
    TraceLevelIntrinsic,
    /// One row's type must fail the whole request, and SQL evaluates row
    /// by row.
    WholeQueryTypeFailure,
    /// The answer is a statement about rows that are absent, so there is
    /// no row to compute it from.
    NoRowToComputeFrom,
    /// The response builder.
    ResponseBuild,
    /// Not a chain link on this route at all — the shipped planner
    /// refuses the stage before any chain is built.
    NotASearchLink,
}

/// A link's answer, given what has accumulated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Capability {
    Yes,
    /// Lowerable in principle, not here.
    No(BlockReason),
    /// Not lowerable in any state, ever.
    Never(NeverReason),
}

/// What the SQL a link contributed MEANS, relative to the link.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Fidelity {
    /// `orig <=> sql`. The evaluator must NOT re-apply this link.
    Equivalent,
    /// `orig => sql`. The evaluator MUST re-apply this link.
    Wider,
}

/// Why a link did not lower. ONE variant per [`Capability`] outcome that
/// is not `Yes`-and-taken, so the fold's arms and this enum are in
/// bijection and neither can gain a case without the other failing to
/// compile.
///
/// There is no `Policy` variant. A per-link cost hook was deleted from
/// [`Lang`] — its inputs cannot answer the question it names, and no
/// measured case in the design record is one where declining wins — and
/// with nothing able to construct it, a `Policy` variant would be a case
/// with no producer: dead code shaped like a decision point, and the
/// quiet half of a broken bijection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResidualReason {
    /// `Capability::No(_)` — lowerable in principle, not in this state.
    Blocked(BlockReason),
    /// `Capability::Never(_)` — documentation, not control flow.
    Never(NeverReason),
}

/// One link's outcome. A boundary index cannot express this, because the
/// lowered links need not be a prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Disposition {
    Lowered(Fidelity),
    Residual(ResidualReason),
}

/// What crosses the boundary, and what the evaluator may assume.
///
/// The kind is a function of the FINAL accumulated `shape` and `exact` —
/// not of where any prefix ended. These stopped being the fold's return
/// value and became a field of the SQL part that produced them
/// ([`super::plan::SqlPart::yields`]), because a request is not one
/// statement and the fold had no way to say which statement a kind
/// belonged to.
pub enum BoundaryOutput<L: Lang + ?Sized> {
    /// Superset rows — the evaluator MUST re-filter and owns the rest of
    /// the chain.
    Candidates(L::Handoff),
    /// Exact rows — the evaluator MUST NOT re-filter.
    Exact(L::Handoff),
    /// Key-grouped, ordered and limited — at most `limit` rows.
    Reduced(L::Handoff),
}

impl<L: Lang + ?Sized> BoundaryOutput<L> {
    /// The wire word for this kind, shared by the explain renderer and
    /// the design record's cap table.
    pub fn kind(&self) -> &'static str {
        match self {
            BoundaryOutput::Candidates(_) => "candidates",
            BoundaryOutput::Exact(_) => "exact",
            BoundaryOutput::Reduced(_) => "reduced",
        }
    }
}

// ---------------------------------------------------------------------
// The language interface
// ---------------------------------------------------------------------

/// The request bounds every link and every plan rule may read. No
/// connection, no query: every rule in this module and in
/// [`super::plan`] is decided at plan time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestBounds {
    pub start_ns: i64,
    pub end_ns: i64,
    pub step_ns: Option<i64>,
    pub limit: Option<u32>,
}

/// What a link may read while it lowers.
#[derive(Debug)]
pub struct LowerCx<'a, L: Lang + ?Sized> {
    pub bounds: &'a RequestBounds,
    _lang: PhantomData<fn(&L)>,
}

impl<'a, L: Lang + ?Sized> LowerCx<'a, L> {
    pub fn new(bounds: &'a RequestBounds) -> Self {
        Self {
            bounds,
            _lang: PhantomData,
        }
    }
}

/// One language's contribution to the shared core.
///
/// The core owns every RULE; a language supplies only the FACTS the core
/// cannot know — which source a link would read, how big its handoff can
/// get, and what that handoff costs to render.
///
/// **There is no cost hook.** A per-link `should_lower` boolean with a
/// `true` default was specified and is deleted: its inputs cannot answer
/// the question it names — no schema, no cardinality, no request bound —
/// it is per link where the real decision compares whole plans, and no
/// measured case in the design record is one where declining wins.
pub trait Lang {
    /// The language's CHAIN LINK, which is not the same type as its AST
    /// stage enum: LogQL's carries the window and the two aggregation
    /// levels, which are not stage variants at all.
    type Stage;
    type Source: Clone + Eq + fmt::Debug + SourceName;
    /// A SQL column expression fragment.
    type ColExpr: Clone + Eq + fmt::Debug;
    type Shape: Shape;
    /// What crosses between two parts — trace ids, fingerprints, a
    /// keyset cursor. `Default` is the EMPTY handoff: a plan describes
    /// the crossing at plan time, and the executor fills the values in.
    type Handoff: Clone + Eq + fmt::Debug + Default;
    type Err;

    /// The ONE exhaustive match per language over the chain-link type,
    /// written with no `_` arm so that adding a link variant fails to
    /// compile here. Returns a stateless `&'static` dispatcher.
    fn lower_of(stage: &Self::Stage) -> &'static dyn Lower<Self>
    where
        Self: 'static;

    /// Which source this link would read, given what has accumulated.
    /// Returning something other than `rel.source_ref()` is what the core
    /// recognises as a source-handoff cut.
    fn source_of(_stage: &Self::Stage, rel: &Relation<Self>) -> SourceRef {
        rel.source_ref()
    }

    /// The key the differing source is reachable by — the name a
    /// previous part's result seeds the next statement on.
    ///
    /// `None` means the source is not reachable from what this relation
    /// projects, and the core then refuses the cut. Default `None`, so a
    /// language that does not hand off cannot accidentally acquire one.
    fn handoff_key(_stage: &Self::Stage, _rel: &Relation<Self>) -> Option<Name> {
        None
    }

    /// The plan-time upper bound on a seed's cardinality, and where that
    /// bound comes from. `None` means unbounded, and the core then
    /// REFUSES the cut and leaves the links in the engine part — which is
    /// what stops a plan from shipping rewritten lines back to the
    /// database after a stage that rewrites the line.
    fn handoff_bound(
        _stage: &Self::Stage,
        _rel: &Relation<Self>,
        _cx: &PlanCx<'_>,
    ) -> Option<SeedBound> {
        None
    }

    /// Rendered size of a seed of `n` values, in query-text bytes and in
    /// database AST elements, so the core can apply its two ceilings
    /// without rendering the statement. O(1), no round trip.
    fn handoff_cost(n: u64) -> super::plan::HandoffCost;
}

/// One link's lowering rules. The stage is passed in: a dispatcher that
/// never receives it cannot see the needle, the template, the label name
/// or the operator — and `Drop` and `Keep` share a payload type, so the
/// payload alone cannot say which link it belongs to.
pub trait Lower<L: Lang + ?Sized> {
    fn capability(&self, stage: &L::Stage, rel: &Relation<L>) -> Capability;

    /// Contributes SQL and updates state. Called only when the link
    /// lowers.
    fn apply(
        &self,
        stage: &L::Stage,
        rel: Relation<L>,
        cx: &LowerCx<'_, L>,
    ) -> Result<Relation<L>, L::Err>;

    /// Updates state ONLY, contributing no SQL. Called when the link is
    /// RESIDUAL. This is what makes blocking emergent rather than
    /// positional.
    fn residual_effect(&self, stage: &L::Stage, rel: Relation<L>) -> Relation<L>;

    /// What the SQL this link contributed MEANS, relative to the link.
    /// Called only where `capability` answered `Yes` and the link was
    /// taken.
    ///
    /// Default [`Fidelity::Wider`] — the conservative side, and today's
    /// behaviour for every link, so a link whose author has not thought
    /// about it cannot make the plan wrong; it can only make it no better
    /// than today.
    fn fidelity(&self, _stage: &L::Stage, _rel: &Relation<L>) -> Fidelity {
        Fidelity::Wider
    }
}

// ---------------------------------------------------------------------
// The fold
// ---------------------------------------------------------------------

/// The fold's own output. It is NOT the compiler's output:
/// [`super::plan::plan_of`] consumes it and produces a
/// [`super::plan::QueryPlan`], which is what an executor would see.
pub struct Lowering<L: Lang + ?Sized> {
    pub rel: Relation<L>,
    /// One entry per link, in chain order.
    pub how: Vec<Disposition>,
}

/// Folds a chain left, with no early return.
///
/// Every link is asked against the relation the link before it left
/// behind; a link that does not lower still applies its state effect and
/// the fold carries on. Returning at the first refusal — computing a
/// longest lowerable prefix — is a measured regression against what
/// ships today and is what this shape exists to prevent.
pub fn lower_chain<L: Lang + ?Sized + 'static>(
    chain: &[L::Stage],
    seed: Relation<L>,
    cx: &LowerCx<'_, L>,
) -> Result<Lowering<L>, L::Err> {
    let mut rel = seed;
    let mut how = Vec::with_capacity(chain.len());
    for stage in chain {
        let lw = L::lower_of(stage);
        match lw.capability(stage, &rel) {
            Capability::Yes => {
                let f = lw.fidelity(stage, &rel);
                rel = lw.apply(stage, rel, cx)?;
                rel.exact &= matches!(f, Fidelity::Equivalent);
                how.push(Disposition::Lowered(f));
            }
            Capability::No(reason) => {
                rel = lw.residual_effect(stage, rel);
                how.push(Disposition::Residual(ResidualReason::Blocked(reason)));
            }
            Capability::Never(reason) => {
                rel = lw.residual_effect(stage, rel);
                how.push(Disposition::Residual(ResidualReason::Never(reason)));
            }
        }
    }
    Ok(Lowering { rel, how })
}

// ---------------------------------------------------------------------
// Hand-written `Clone`/`PartialEq`/`Eq`/`Debug` for the `L`-generic types
//
// `#[derive]` would add an `L: Clone` (or `L: Debug`, `L: PartialEq`)
// bound. `L` is a marker type that is never constructed and never held,
// so those bounds are unsatisfiable in practice and would make the types
// unusable. Every impl below is the derive's body with the bound removed.
// ---------------------------------------------------------------------

impl<L: Lang + ?Sized> Clone for SourceTerm<L> {
    fn clone(&self) -> Self {
        match self {
            SourceTerm::Base(s) => SourceTerm::Base(s.clone()),
            SourceTerm::Wrapped(r) => SourceTerm::Wrapped(r.clone()),
        }
    }
}

impl<L: Lang + ?Sized> fmt::Debug for SourceTerm<L> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SourceTerm::Base(s) => f.debug_tuple("Base").field(s).finish(),
            SourceTerm::Wrapped(r) => f.debug_tuple("Wrapped").field(r).finish(),
        }
    }
}

impl<L: Lang + ?Sized> PartialEq for SourceTerm<L> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (SourceTerm::Base(a), SourceTerm::Base(b)) => a == b,
            (SourceTerm::Wrapped(a), SourceTerm::Wrapped(b)) => a == b,
            _ => false,
        }
    }
}

impl<L: Lang + ?Sized> Eq for SourceTerm<L> {}

impl<L: Lang + ?Sized> Clone for Grouping<L> {
    fn clone(&self) -> Self {
        Self {
            keys: self.keys.clone(),
        }
    }
}

impl<L: Lang + ?Sized> fmt::Debug for Grouping<L> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Grouping")
            .field("keys", &self.keys)
            .finish()
    }
}

impl<L: Lang + ?Sized> PartialEq for Grouping<L> {
    fn eq(&self, other: &Self) -> bool {
        self.keys == other.keys
    }
}

impl<L: Lang + ?Sized> Eq for Grouping<L> {}

impl<L: Lang + ?Sized> Clone for Ordering<L> {
    fn clone(&self) -> Self {
        Self {
            keys: self.keys.clone(),
        }
    }
}

impl<L: Lang + ?Sized> fmt::Debug for Ordering<L> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ordering")
            .field("keys", &self.keys)
            .finish()
    }
}

impl<L: Lang + ?Sized> PartialEq for Ordering<L> {
    fn eq(&self, other: &Self) -> bool {
        self.keys == other.keys
    }
}

impl<L: Lang + ?Sized> Eq for Ordering<L> {}

impl<L: Lang + ?Sized> Clone for Relation<L> {
    fn clone(&self) -> Self {
        Self {
            source: self.source.clone(),
            predicate: self.predicate.clone(),
            projection: self.projection.clone(),
            cols: self.cols.clone(),
            grouping: self.grouping.clone(),
            ordering: self.ordering.clone(),
            limit: self.limit,
            shape: self.shape.clone(),
            exact: self.exact,
            depth: self.depth,
        }
    }
}

impl<L: Lang + ?Sized> fmt::Debug for Relation<L> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Relation")
            .field("source", &self.source)
            .field("predicate", &self.predicate)
            .field("projection", &self.projection)
            .field("cols", &self.cols)
            .field("grouping", &self.grouping)
            .field("ordering", &self.ordering)
            .field("limit", &self.limit)
            .field("shape", &self.shape)
            .field("exact", &self.exact)
            .field("depth", &self.depth)
            .finish()
    }
}

impl<L: Lang + ?Sized> PartialEq for Relation<L> {
    fn eq(&self, other: &Self) -> bool {
        self.source == other.source
            && self.predicate == other.predicate
            && self.projection == other.projection
            && self.cols == other.cols
            && self.grouping == other.grouping
            && self.ordering == other.ordering
            && self.limit == other.limit
            && self.shape == other.shape
            && self.exact == other.exact
            && self.depth == other.depth
    }
}

impl<L: Lang + ?Sized> Eq for Relation<L> {}

impl<L: Lang + ?Sized> Clone for BoundaryOutput<L> {
    fn clone(&self) -> Self {
        match self {
            BoundaryOutput::Candidates(h) => BoundaryOutput::Candidates(h.clone()),
            BoundaryOutput::Exact(h) => BoundaryOutput::Exact(h.clone()),
            BoundaryOutput::Reduced(h) => BoundaryOutput::Reduced(h.clone()),
        }
    }
}

impl<L: Lang + ?Sized> fmt::Debug for BoundaryOutput<L> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BoundaryOutput::Candidates(h) => f.debug_tuple("Candidates").field(h).finish(),
            BoundaryOutput::Exact(h) => f.debug_tuple("Exact").field(h).finish(),
            BoundaryOutput::Reduced(h) => f.debug_tuple("Reduced").field(h).finish(),
        }
    }
}

impl<L: Lang + ?Sized> PartialEq for BoundaryOutput<L> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (BoundaryOutput::Candidates(a), BoundaryOutput::Candidates(b))
            | (BoundaryOutput::Exact(a), BoundaryOutput::Exact(b))
            | (BoundaryOutput::Reduced(a), BoundaryOutput::Reduced(b)) => a == b,
            _ => false,
        }
    }
}

impl<L: Lang + ?Sized> Eq for BoundaryOutput<L> {}

impl<L: Lang + ?Sized> Clone for Lowering<L> {
    fn clone(&self) -> Self {
        Self {
            rel: self.rel.clone(),
            how: self.how.clone(),
        }
    }
}

impl<L: Lang + ?Sized> fmt::Debug for Lowering<L> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Lowering")
            .field("rel", &self.rel)
            .field("how", &self.how)
            .finish()
    }
}

impl<L: Lang + ?Sized> PartialEq for Lowering<L> {
    fn eq(&self, other: &Self) -> bool {
        self.rel == other.rel && self.how == other.how
    }
}

impl<L: Lang + ?Sized> Eq for Lowering<L> {}
