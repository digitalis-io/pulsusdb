# Query lowering: compiling pipeline stages to SQL

A LogQL or TraceQL query is a **selector** followed by a **pipeline of stages**. Every stage can
be evaluated in `pulsus-server` over rows ClickHouse has already sent, and some can instead be
compiled into the SQL we send, so ClickHouse does the work next to the data and returns less.
This document describes the mechanism that decides which — the shared lowering core — and applies
it to both languages.

The decision it deliberately leaves out is the SQL shape several lowered stages compose into.
That is [ADR 0008](decisions/0008-sql-composition-for-lowered-pipelines.md).

Related: [architecture.md §5.3](architecture.md) (LogQL) and [§5.4](architecture.md) (TraceQL) for
the read paths this sits inside; [schemas.md §3.2 and §4.2](schemas.md) for the generated SQL;
[api.md §2.1 and §4.2](api.md) for the response contracts a lowered query must still satisfy.

**What this design is required to do.** The requirement is **one generic architecture with shared
code patterns for both LogQL and TraceQL, designed together** — not two designs sequenced. It has
to cover **multi-stage pipelines**, not a single stage at a time, and it has to carry **query
optimisation**: which stages compile into the SQL, in what shape, and what that saves on the hop
that is billed. The requirement is recorded on
[#492](https://github.com/digitalis-io/pulsusdb/issues/492) and on
[#507](https://github.com/digitalis-io/pulsusdb/issues/507); #492 carries the shared core and #507
the LogQL stage inventory and its measurements. Three obligations follow and every section below is
answerable against them: the core is exercised by **both** stage sets, a stage from each fits
through the same interface **without changing it**, and where the sharing stops is stated with
reasons rather than left as a gap (§6).

**What the compiler emits.** Not one statement: a **plan** — an ordered list of parts, each part
either one SQL statement or work in our own engine, with the value set that crosses between
parts named, typed and bounded. §2.7 is the plan object and the four **cuts** that are the only ways
a plan gets a second SQL part. That is not an ambition: §9.2's worked request already sends **1,128**
statements, and an earlier form of this design had no field that could hold a number other than one.

**Status.** The core and the TraceQL side are designed and measured. The LogQL side is
**described but not measured here** — its inventory belongs to
[#507](https://github.com/digitalis-io/pulsusdb/issues/507) and §7 is structured to receive it.
The stage-by-stage statement text for both languages, with the answer each query must return, is
[query-to-sql.md](query-to-sql.md).
§10 states exactly what is demonstrated, what a compiler has now disproved and repaired, and what
the first implementation wave settles — including one **defect this design corrects** (the
pipeline's written order is observable on the reference and ignored here) and one **regression it
avoided** (§9.6: the fold's original stopping rule cost 20.6× more metered bytes than what ships
today, on an ordinary query).

---

## 1. Why this exists

**The lowering boundary is computed four times today, by hand, and none of the four can be
reused.**

LogQL computes it in three separate walks over the same stage list, in
[`crates/pulsus-read/src/logql/plan.rs`](../crates/pulsus-read/src/logql/plan.rs):

| function | line | what it computes |
|---|---|---|
| `compile_line_filters` | 3052 | the lowered prefix's predicates — walks the stages and `break`s at `LineFormat`, `Decolorize` or `Unpack` |
| `has_unpushed_dropping_stage` | 1655 | whether the boundary is short of the end — tracks `seen_line_format`, returns `true` at a `LabelFilter` or a post-rewrite line filter |
| `metric_pipeline_construct` | 1680 | the **first** stage that cannot lower, as a `&'static str` reason — a `find_map` over the ten stage variants |

Those are three projections of one traversal. `metric_pipeline_construct` is exactly "capability,
returning the first refusal and its reason"; `has_unpushed_dropping_stage` is "did the boundary
fall short"; `compile_line_filters` is "the predicate the fold accumulated".

Underneath them, `is_pushable_line_filter` (`plan.rs:3086`) carries a doc comment that states the
problem in the codebase's own words — *"the single source of truth for 'does this line filter push
down to SQL, or must it run in the client pipeline?' … so the two paths never drift"* — and it has
five call sites across three files: `plan.rs:1668`, `plan.rs:1686`, `plan.rs:3060`,
`pipeline.rs:1019`, `exec.rs:2261`.

TraceQL computes the same thing a fourth time and shares none of it:
[`filter::collect`](../crates/pulsus-read/src/traces/filter.rs) (line 2327) walks a boolean tree
choosing candidate generators, and
[`plan_pipeline`](../crates/pulsus-read/src/traces/search_plan.rs) (line 1083) walks the pipeline.

So a shared core is not an abstraction invented for a hypothetical future. It is the fourth
hand-written copy being replaced by the thing all four already are.

**And the cost of not having it was measurable.** TraceQL's spanset aggregate had no SQL path at
all when this record was written: `PlannedAggregate` was built at `search_plan.rs:1218` and read at
exactly one place, `search_eval.rs:2420`. Every matching span was therefore transported and then
discarded. (Issue #492 part 4 gave `min(duration)`, `max(duration)` and `count()` over a
single attribute-equality selector a `HAVING` in the generator statement; every other aggregate
shape still has no SQL path.) On corpus
C1 (§9), `{ .service.namespace = "prod" } | max(duration) > 1s` at `limit=20` costs **1,128
sequential round trips** and moves **77,572,021 result bytes**; of that, the ~11 KB the client
receives is all that was wanted (11,340 B, measured on C2 — see §9.1).

**The same answer lowered is four statements, not one: 4 round trips.** The compiled generator is
the first; the window-bounded hydration read and the membership read survive lowering, because
`spanSets[].matched` and `spanSets[].spans[]` are written unconditionally
(`crates/pulsus-server/src/traces_api/search_response.rs:428-430`, `:507-512`); the winners' root
read is the fourth and stays residual in **every** chain, because the root summary is read
trace-wide with no time predicate (§3.1's `Emit` row, §5). §9.2's own round-trip formula
`1 + 2·ceil(k/32) + 1` agrees: after lowering every candidate the generator returns already
qualifies, so `k` is the request's `limit` of 20 and the count is 4.

> **The lowered totals are measured.** An earlier revision published a lowered cost of `43,636 B`
> tagged `seed + root only`, computed from a two-statement model that left out the window-bounded
> hydration read and the membership read; every ratio derived from it inherited the omission. That
> figure, its tag and its ratios are **superseded by the §9.2 re-measurement** and appear nowhere
> else in this record or in [the hops diagram](diagrams/query-lowering-hops.svg), which is redrawn
> from the same artefact. [`query-to-sql.md`](query-to-sql.md) names the figure and the tag once, at
> its line 5084, in a paragraph that identifies them as superseded by the §9.2 re-measurement; that
> paragraph explains why a historical example in it still quotes them.
> §9.2b measures all four lowered statements and §9.2's comparison table divides the two whole
> requests: **212,986 B** against **77,572,021 B**, a saving of **364×**.

> **One figure is still an unverified survivor.** The client's `11,340 B` was measured on **corpus
> C2** (§9.1) — the OTLP-ingest corpus — and part 8 did not rebuild C2, so it is carried forward
> still flagged: nothing suggests it is wrong and nobody has re-measured it. **The peak-memory pair
> has left this list.** §9.2b re-measures it on C1 as **167,629,277** / **190,817,746 B** with a
> **1.14×**, and the re-measurement corrects what the smaller figure was *of*: the earlier revision
> called it "the loop's maximum", and the loop's maximum is 26,084,032 B. The larger of the two is
> the phase-1 generator's peak, on both sides of the comparison.

![Bytes per hop, evaluated against lowered](diagrams/query-lowering-hops.svg)

**What it is worth on LogQL, including the case that is worth least.** Measured by
[#507](https://github.com/digitalis-io/pulsusdb/issues/507) on its own corpus, metered-hop bytes
today against lowered:

| query | metered ratio |
|---|---|
| `{service_name="checkout"} \| json \| level="error"` | 42.1× |
| `{service_name="checkout"} \| json \| status >= 500` | 13.8× |
| `{service_name="checkout"} \| json \| line_format "{{.msg}}" \|= "pod-044"` | 68.9× |
| `{service_name="checkout"} \| trace_id="740eda9f12aec8e8"` | ≈24,800× |
| `sum by (level) (count_over_time({service_name="checkout"} \| json [1m]))` | 1,420× |
| **`sum by (trace_id) (count_over_time({service_name="checkout"} \| json [1m]))`** | **7.8×** |

**The last row is the one to design against, because it is the default shape.** Grouping by a
parser-derived high-cardinality label makes the output proportional to the input — 400,277 groups,
18 MB across the hop — and #507 captured the reference returning one series per distinct parsed
label set for an ungrouped `unwrap`, so high cardinality is what a user gets without asking for
it. 7.8× is still worth having, and it is **180× less** than the grouped-by-`level` row above it.

**A design justified on 24,800× that delivers 7.8× on what people actually run has been sold on
the wrong number.** The 24,800× row is real — it is a structured-metadata filter that today burns
127 round trips and about 53 GiB of reads **to return an empty partial answer** — but it is the
best case, not the expected one. Two consequences follow and both are load-bearing: a bound on the
number of groups is not optional (§8), and the case for this work rests on the round-trip collapse
and the 7.8×–70× band, not on its maximum.


---

## 2. The model

### 2.1 The pipeline as data

**Today there is nothing to fold over, and that is the root cause rather than an inconvenience.**
`plan_pipeline` sorts stages into buckets — `aggregates`, `select_fields`, `group_by`, `coalesce`,
`post_stages` — so a `SearchPlan` records *which* stages a query has and, except for
`post_stages`, not *in what order*. LogQL keeps its `Vec<Stage>` but walks it three times.

The intermediate representation is one linear chain, and **three of its links are synthesised
rather than written by the user**:

```
Source -> S1 -> S2 -> ... -> Sn -> Order -> Limit -> Emit
           \___ the language's own stages ___/   \__ synthesised __/
```

- `Source` carries the selector — a TraceQL `SpansetExpr` tree or a LogQL `StreamSelector` —
  lowered by the boolean lattice of §2.4, not by the stage fold.
- `S1..Sn` are the language's stages, **in order**.
- `Order` is the response's ordering contract, `Limit` the request's cap, `Emit` the response
  builder.

Making the last three ordinary links is the whole answer to "position matters for some stages": a
`LIMIT` is lowerable only when the rows are key-grouped and an ordering is already established,
and that is a precondition on accumulated state rather than a rule about `LIMIT`.

### 2.2 Capability, conditional on accumulated state

```rust
// crates/pulsus-read/src/compile/ — generic over the language.
// (Named `compile`, not `lower`: §6, "Where the code lives".)

pub trait Lang {
    /// The language's CHAIN LINK, which is not the same type as its AST
    /// stage enum. TraceQL's is `pulsus_traceql::PipelineStage` plus the
    /// three synthesised links; LogQL's is `LqlLink` (§7.1), of which
    /// `pulsus_logql::Stage` is ONE arm — the window and the two
    /// aggregation levels are not `Stage` variants.
    type Stage;
    type Source;                // which table(s), and how the selector lowered
    type ColExpr: Clone;        // a SQL column expression fragment
    type Shape: Shape;          // §2.3 — NOT a shared enum, see §6
    type Handoff;               // what crosses the boundary (trace ids | fingerprints)
    type Err;

    /// The ONE exhaustive match per language over the STAGE type. It is
    /// specified with no `_` arm, so that once wave 1 writes it, adding a
    /// stage variant will fail to compile here. It is not specified as the
    /// only such site: §11.3's two stage-variant gates —
    /// `every_logql_stage_variant_has_a_row_in_the_lowering_document` and
    /// `every_traceql_pipeline_stage_variant_has_a_row_in_the_lowering_document`,
    /// which do not exist at base either — are to be a second exhaustive match
    /// over the same type, on purpose (§11.5). Returns a stateless
    /// `&'static` dispatcher, which needs `Self: 'static` here and on the
    /// fold (R3b).
    fn lower_of(stage: &Self::Stage) -> &'static dyn Lower<Self> where Self: 'static;

    // `should_lower` — a per-link boolean cost hook with a `true`
    // default — is REMOVED from this trait. Its inputs cannot answer the
    // question it names, and no measured case in this document or in
    // query-to-sql.md is one where declining wins; §2.7.5 gives the
    // argument and what would falsify it. What replaces it is three
    // FACTS the plan builder needs and the core cannot know. The core
    // owns every RULE in §2.7; a language supplies only these.

    /// Which source this link would read, given what has accumulated.
    /// Returning something other than `rel.source` is what the core
    /// recognises as §2.7.2's source-handoff cut. Default: `rel.source`,
    /// i.e. no cut.
    fn source_of(_stage: &Self::Stage, rel: &Relation<Self>) -> SourceRef;

    /// The plan-time upper bound on a seed's cardinality, and where that
    /// bound comes from. `None` means unbounded, and the core then
    /// REFUSES the cut and leaves the links in the engine part
    /// (§2.7.6, rule 2).
    fn handoff_bound(_stage: &Self::Stage, rel: &Relation<Self>, cx: &PlanCx<'_>)
        -> Option<SeedBound>;

    /// Rendered size of a seed of `n` values, in query-text bytes and in
    /// database AST elements, so the core can apply §2.7.3's two ceilings
    /// without rendering the statement. O(1), no round trip.
    fn handoff_cost(n: u64) -> HandoffCost;
}

pub enum Capability {
    Yes,
    /// Lowerable in principle, not here. Carries why, so the boundary
    /// explains itself instead of being inferred.
    No(BlockReason),
    /// Not lowerable in any state, ever (§5). The type is what stops these
    /// being mistaken for unfinished work.
    Never(NeverReason),
}

pub trait Lower<L: Lang + ?Sized> {
    /// The stage is passed in. A dispatcher that never receives it cannot
    /// see the needle, the template, the label name or the operator (R3).
    fn capability(&self, stage: &L::Stage, rel: &Relation<L>) -> Capability;
    /// Contributes SQL and updates state. Called only when the link lowers.
    fn apply(&self, stage: &L::Stage, rel: Relation<L>, cx: &LowerCx<'_, L>)
        -> Result<Relation<L>, L::Err>;
    /// Updates state ONLY, contributing no SQL. Called when the link is
    /// RESIDUAL. This is what makes blocking emergent rather than
    /// positional, and it is the whole of R5's repair.
    fn residual_effect(&self, stage: &L::Stage, rel: Relation<L>) -> Relation<L>;
    /// What the SQL this link contributed MEANS, relative to the link
    /// (§2.7.7). Called only where `capability` answered `Yes` and the
    /// link was taken. Default `Wider` — the conservative side, and
    /// today's behaviour for every link.
    fn fidelity(&self, _stage: &L::Stage, _rel: &Relation<L>) -> Fidelity { Fidelity::Wider }
}

/// The SQL under construction, as an algebra term rather than text.
/// Rendering happens once, at the boundary (ADR 0008).
pub struct Relation<L: Lang + ?Sized> {
    pub source: SourceTerm<L>,        // Base(L::Source) | Wrapped(Box<Relation<L>>)
    pub predicate: Pred,              // an OVER-APPROXIMATING conjunction (§2.4)
    pub projection: Vec<(Name, L::ColExpr)>,
    pub grouping: Option<Grouping<L>>,
    pub ordering: Option<Ordering<L>>,
    pub limit: Option<u64>,
    pub shape: L::Shape,
    pub exact: bool,                  // does `predicate` mean exactly what the selector means?
    pub depth: u8,                    // subquery nesting, for ADR 0008's wrap rule
}
```

**`exact` is the field that makes capability conditional on what precedes it**, and it comes from
a correctness argument with a measured consequence. A candidate generator is *allowed* to be a
superset, because the evaluator re-filters afterwards. An aggregate is not: `max()` over a superset
can exceed the true maximum and admit a row that should not match, `min()` errs the other way, and
`count()` inflates. So an aggregate's `capability` is `No(NotExact)` when `!rel.exact`, and every
later stage that reads group membership inherits the precondition without restating it.

Measured on C1, the cost of getting this wrong is **333 qualifying traces becoming 1,000** (§9.3).

`Order` inherits the same precondition **in TraceQL**, and that is the part easy to miss: the
TraceQL sort key is `max(matched-span timestamp)`, so over a superset the **order** is wrong even
when the set is re-filtered. Measured on C1: sort key `…044006000` against `…044009000` for the same
trace. **LogQL's `Order` does not inherit it**, and the asymmetry is the reason the precondition is
per-link rather than global: a LogQL sort key is the row's own timestamp, which no dropped row
changes, so ordering a superset and then dropping rows leaves the surviving order correct. §7.1's
`Order` row therefore requires only that the ordering columns are in the projection, while §7.1's
`Limit` row does require `exact` — a `LIMIT` over a superset loses rows a residual link would have
kept.

**This scope rule matches the reference.** Tempo's `Aggregate.evaluate` iterates `ss.Spans` — the
spanset's spans, i.e. the matched set, not the trace's spans — and `aggregateCount` is
`len(ss.Spans)` (`pkg/traceql/ast_execute.go:243-280` @ grafana/tempo v3.0.2,
`0c4b926d09234186de39833e9c7ecb5b7614c8b9`). A span whose aggregated value is nil is skipped, and
a spanset with no non-nil value is dropped rather than emitted as zero — which is what our
`aggregate_value` (`search_eval.rs:1839`) returning `None` already does.

### 2.3 Shape composition, and the open column set

Each stage declares the input shape it accepts and the output shape it produces; the fold checks
they match. **A stage whose input shape does not match the accumulated shape returns `No`, and the
link becomes residual — a property of the chain, not of the stage. The fold does not stop; the
next link is asked against the state this one left behind.**

The part that decides whether the model is right is **columns**, because a LogQL parser adds
labels whose names are not known at plan time:

```rust
pub enum ColSet {
    Closed(Vec<Name>),
    /// Known columns, plus open sets each of which can resolve a name to
    /// a SQL expression — or refuse.
    ///
    /// Not `Box`: `Shape: Clone` forces `ColSet: Clone`, and a boxed
    /// trait object is not `Clone` (R1/R2 below). `Arc` and not `Rc`
    /// because a `QueryPlan` holding one is carried across an `.await` —
    /// see `OpenSource` below.
    Open { known: Vec<Name>, from: Vec<Arc<dyn OpenSource>> },
}

/// `Debug` is a supertrait and `id()` is an identity, because `ColSet`
/// must be `Clone + PartialEq + Eq + Debug` and `#[derive]` cannot see
/// through `dyn`. `ColSet`'s `PartialEq` is written by hand over `id()`.
///
/// `Send + Sync` because the language's own plan object holds a
/// `QueryPlan` across an `.await` — the TraceQL search handler builds one
/// before it acquires a connection and still holds it when the last row
/// arrives — and an axum handler's future must be `Send`. Measured: with
/// `Rc` and no bounds, `pulsus-server` does not compile, twice, with
/// `the trait bound … {search}: Handler<_, _> is not satisfied`
/// (issue #492 part 3). One implementor exists in the tree and it is
/// trivially `Send + Sync`.
pub trait OpenSource: std::fmt::Debug + Send + Sync {
    /// `Some(expr)` if this name is resolvable to SQL here; `None` if the
    /// only way to know its value is to run the stage in the evaluator.
    fn resolve(&self, name: &Name) -> Option<SqlExpr>;
    fn id(&self) -> OpenSourceId;
}
```

**This is not an accommodation for LogQL — TraceQL already needs it.** A TraceQL attribute
(`.foo`, `span.bar`) is a name that is not a column and resolves to a `trace_attrs_idx` semi-join
or value read. A LogQL `| json` label is a name that is not a column and resolves to a JSON
extraction over `body`. They are one concept, and writing them against one type made the model
smaller rather than larger.

`ColSet` also carries **provenance** per column — whether a column is the stored one or an
expression some stage computed. That single fact derives a rule LogQL currently writes down by
hand (§7.1).

### 2.4 Partial lowering, and the one invariant

A selector with three conditions where two can be lowered is the common case in both languages,
not an edge. Lowering a boolean expression returns a pair `(sql, exact)` under one invariant:

> **`orig ⟹ sql`** — the emitted predicate is implied by the expression it came from, so the SQL
> result is always a superset of the true match set. `exact` additionally asserts `orig ⟺ sql`.

| node | `sql` | `exact` |
|---|---|---|
| leaf, lowerable | its predicate | `true` |
| leaf, not lowerable | `1` | `false` |
| `a AND b` | `sql_a AND sql_b` | `exact_a && exact_b` |
| `a OR b` | `sql_a OR sql_b` | `exact_a && exact_b` |
| `NOT a`, `exact_a` | `NOT sql_a` | `true` |
| `NOT a`, `!exact_a` | `1` | `false` |

Three things this settles that a per-shape rule gets wrong:

- **Dropping a conjunct is safe; dropping a disjunct is not.** Under `AND` an unlowerable leaf
  becomes `1` and disappears — still a superset. Under `OR` it makes the whole disjunction `1`,
  which is correct and useless; lowering only the other branch would give a **subset** and
  silently lose rows. TraceQL already encodes this by hand — `filter::collect` requires *both*
  sides' generators for `a || b` — and LogQL encodes the same thing for `or` groups inside a line
  filter. The lattice is that rule generalised, not a new one.
- **`NOT` of an over-approximation is an under-approximation**, so negation must refuse unless its
  operand was exact. That is the one place the obvious rule breaks the invariant, and it is where
  a hand-written pushdown loses rows.
- **So: always lower maximally, and compute `exact` separately.** Pushing more conjuncts strictly
  reduces rows crossing the metered hop, and it never changes whether a later stage can lower — a
  later aggregate is blocked by `!exact` regardless of how much of the selector was pushed. The two
  facts are independent, which is why "partially, or not at all" has a clean answer: **partially,
  always; exactness is computed, not chosen.**

Cost evidence for the maximal choice, on C1: pushing the second conjunct of
`key='http.method' AND val='GET'` moves the read from 1,225 granules to 210 — 5.8x — and even a
conjunct that prunes nothing still removes rows before they cross the hop we pay for.

### 2.5 The fold, and why it is not a prefix

**This section was wrong in the first version of this document, and the correction is the most
important thing in it.** The fold originally returned at the first refusal, computing a *longest
lowerable prefix*. That is a measured regression against what ships today, and the measurement is
in §9.6.

The shipped `compile_line_filters` does not stop at a link it cannot lower — it **skips** that link
and carries on. Over 3,375 enumerated LogQL chains, the prefix model disagrees with shipped
behaviour on **715**; a narrower repair that skips only a non-lowerable line filter still
disagrees on **463**; and the model below disagrees on **0**.

**What that 0 covers, stated wherever the number appears.** The comparison is the **ordered list of
line-filter values the model would conjoin** against the ordered list a **transcription** of
`compile_line_filters` (`crates/pulsus-read/src/logql/plan.rs:3052`) emits — not against emitted
SQL, and not against a running server. So 0 means the two agree on **which filters push and in what
order**, over that atom set at that chain length. It does **not** cover the operator each filter
renders, the escaping, the rest of the statement, or any stage the atom set does not contain. Two
of the three shipped walks are not compared at all by it; §11.2 nominates one **wave-1** gate per
walk — `logql::plan::tests::the_model_reproduces_compile_line_filters_ordered_predicate_list`,
`logql::plan::tests::exact_after_the_fold_agrees_with_has_unpushed_dropping_stage` and
`logql::plan::tests::the_first_residual_pipe_link_agrees_with_metric_pipeline_construct`, none of
which exists at base — so the claim's domain and the check's domain are the same set.

> **A link either lowers, or becomes residual and the fold continues. Blocking is emergent from
> accumulated state — column provenance, shape, `exact` — never from position.**

The residual link still **applies its state effect**. That is the part that makes blocking work: a
`line_format` that does not lower still marks `body` as `Computed`, so a later line filter finds
no stored column to lower against and becomes residual too. Nothing is skipped silently.

```rust
/// Why a link did not lower. ONE variant per `Capability` outcome that is
/// not `Yes`-and-taken, so the fold's arms and this enum are in bijection
/// and neither can gain a case without the other failing to compile.
pub enum ResidualReason {
    /// `Capability::No(_)` — lowerable in principle, not in this state.
    Blocked(BlockReason),
    /// `Capability::Never(_)` — documentation, not control flow (below).
    Never(NeverReason),
}

/// One link's outcome. `Boundary { lowered: usize }` cannot express this,
/// because the lowered links need not be a prefix.
pub enum Disposition {
    /// Carries how faithful the SQL this link contributed is (§2.7.7).
    Lowered(Fidelity),
    Residual(ResidualReason),
}

/// The fold's own output. It is NOT the compiler's output any more:
/// `plan_of` (§2.7.1) consumes it and produces a `QueryPlan`, which is
/// what the executor sees.
pub struct Lowering<L: Lang + ?Sized> {
    pub rel: Relation<L>,
    /// One entry per link, in chain order.
    pub how: Vec<Disposition>,
}

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
```

**This fold was compiled, and the previous one did not compile — but the interface has moved since,
and the transcripts below were produced BEFORE it moved.** The fold as it stood at the round-15
design review was written to a single file together with §2.2's `Lang`/`Lower`/`Capability`/`Relation`
and §2.3's `ColSet`, given a four-link LogQL-shaped chain, and built and run with
`rustc --edition 2021` from probe sources in that architect's session scratchpad.

> **What that compile does and does not cover, stated because this revision changed the types it
> compiled.** It covers the traversal, the three-way `Capability` match and the residual rule. It
> does **not** cover `Fidelity`, the three fact-suppliers of §2.2, the removal of
> `ResidualReason::Policy`, or anything in §2.7 — all of which were added after that build and
> **have not been compiled by anything**. The transcripts below therefore print `Lowered` where the
> type above now reads `Lowered(Fidelity)`, and their `how` lines carry no fidelity payload: they
> are quoted as what that probe printed, not as what this interface prints. Nothing else in them
> is affected, because the residual rule they execute never reads the payload. The probe sources
> are **no longer on the machine** — the session scratchpad they lived in has been cleared — so
> these three blocks cannot be re-run as they stand, and §10 records re-establishing them, against
> the interface as it now reads, as a **wave 1** obligation. **The three blocks below are what the commands
in them printed, whole**, re-run on this tree at `2f78c53`. Where one is trimmed or normalised, the
command that does it is part of the block — break A's writes `rustc`'s output to a file and trims
*that*, because piping `rustc` into `head` closes the pipe and turns its exit code from **1** into
**141**; and break B's pipes the panic through `sed` to replace the **process id** with `PID`,
because that field is the one thing in these three blocks that changes on every run. `pipefail`
is what keeps break B's reported `exit=` the binary's **101** and not `sed`'s 0. Run twice
back to back, the three blocks now `diff` clean against their reruns; without the `sed` the panic
line alone differs.

```
$ rustc --edition 2021 -o 492-fold-repaired.bin 492-fold-repaired.rs; echo "rustc exit=$?"; ./492-fold-repaired.bin 2>&1; echo "exit=$?"
rustc exit=0
how       = [Lowered, Residual(Blocked(NotYetLowered)), Residual(Blocked(BodyNotStored)), Residual(Never(NeedsUnwindowedRootRead))]
body      = Computed(SqlExpr("template({{.msg}})"))
predicate = 1 AND position(body, 'CONN_REFUSED') > 0
RESIDUAL RULE HOLDS: refused link applied its state effect; the next link saw it
exit=0
```

The chain is `|= "CONN_REFUSED"`, `| line_format "{{.msg}}"`, `|= "pod-044"`, `Emit`, and the run is
the residual rule executed rather than asserted in prose: **link 2 is refused, still applies its
state effect, and link 3 sees it and goes residual too**, so `pod-044` never reaches the predicate.
The previous version of this block did not compile at all: `Capability::No { reason, .. }` against
`No(BlockReason)` is `error[E0026]: variant Capability::No does not have a field named reason`, and
with that pattern corrected the match was `error[E0004]: non-exhaustive patterns:
Capability::Never(_) not covered` — so as written, a refused link such as `LineFormat` could not
reach `residual_effect` at all.

**Two deliberate breaks confirm the check is not vacuous.** Break A deletes the `Never` arm from the
fold, and the compiler refuses it:

```
$ rustc --edition 2021 -o 492-fold-breakA.bin 492-fold-breakA.rs 2> a.err; echo "rustc exit=$?"; head -2 a.err; echo "(the remaining $(( $(wc -l < a.err) - 2 )) lines are E0004's explanation and the aborting/explain notes)"
rustc exit=1
error[E0004]: non-exhaustive patterns: `Capability::Never(_)` not covered
   --> 492-fold-breakA.rs:156:15
(the remaining 22 lines are E0004's explanation and the aborting/explain notes)
```

Break B leaves the fold alone and makes `LineFormat::residual_effect` return the relation unchanged.
It compiles, runs, prints the wrong answer **before** it fails, and then panics on the `assert_eq!`
— process exit **101**:

```
$ set -o pipefail; rustc --edition 2021 -o 492-fold-breakB.bin 492-fold-breakB.rs; echo "rustc exit=$?"; ./492-fold-breakB.bin 2>&1 | sed -E "s/^thread 'main' \([0-9]+\)/thread 'main' (PID)/"; echo "exit=$?"
rustc exit=0
how       = [Lowered, Residual(Blocked(NotYetLowered)), Lowered, Residual(Never(NeedsUnwindowedRootRead))]
body      = Stored
predicate = 1 AND position(body, 'CONN_REFUSED') > 0 AND position(body, 'pod-044') > 0

thread 'main' (PID) panicked at 492-fold-breakB.rs:316:5:
assertion `left == right` failed: link 2 is refused, still applies its state effect, and link 3 sees it
  left: [Lowered, Residual(Blocked(NotYetLowered)), Lowered, Residual(Never(NeedsUnwindowedRootRead))]
 right: [Lowered, Residual(Blocked(NotYetLowered)), Residual(Blocked(BodyNotStored)), Residual(Never(NeedsUnwindowedRootRead))]
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
exit=101
```

That run's `predicate` line is a line filter pushed into SQL **after** a `line_format`, which is
exactly the rule `compile_line_filters` breaks at today (`plan.rs:3067`). Break B's panic traces to the unchanged
provenance and nothing incidental: with `body` left `Stored`, link 3's `capability` answers `Yes`,
so it lowers.

**Every exit code this document states was re-run, not carried.** `rustc` exits **1** on a compile
failure, never 101 — an earlier version of this section said 101 and was wrong. The exit codes are:
the probe builds at 0 and runs at 0; break A fails to build at **1**; break B builds at **0** and
its run panics at **101**; a `cargo nextest` selector matching nothing exits **4** (§11); and
`cargo nextest --test <name>` naming a test target that does not exist fails at target selection at
**101** (§11.3).

**`Never` is documentation, not control flow.** It records that no future work will lower a link;
it does not stop the fold. A `Never` link becomes residual like any other and applies its state
effect. Conflating the two is what produced the regression.

**Soundness of continuing past a residual link.** A residual link is applied by the evaluator, so
the lowered SQL must remain a sound over-approximation without it. That holds for the same reason
dropping a conjunct holds in §2.4 — a row-removing link only widens the result — and it fails
exactly when a residual link would change what a later lowered link observes. It does not need a
separate rule: the residual link's state effect is what removes the later link's ability to lower.
A residual `by()` leaves the shape ungrouped, so a following aggregate that lowered would compute
per-trace instead of per-group; the shape it reads is the shape it gets, and it refuses.

![Lowering as a per-link disposition over the whole chain, on four pipelines](diagrams/query-lowering-boundary.svg)

**Greedy is the default, and it is a stated consequence of the cost model rather than a hook.**
Lowering one more stage always removes a round trip and never adds one. It can add rows read: the
lowered form loses the client-side early termination the TraceQL loop has, and the crossover is at
about one batch of `BATCH_TRACES` = 32 candidates
(`crates/pulsus-read/src/traces/exec.rs:115`). The accepted worst case is bounded by one key-range
scan — which is exactly what the phase-1 generator already costs — so there is no chain on which
greedy lowering costs more than one generator's read. §2.7.5 gives the argument in full, together
with the two measurements that looked like counterexamples and are not, and what would falsify it.

**The one real per-language cost regime is not a policy either.** LogQL's `fetch_until_limit`
keyset paging (`crates/pulsus-read/src/logql/plan.rs:1625`, field at `:80`) means compiling a
dropping stage changes the *paging strategy*, not only the byte count — and what decides that is
whether the compiled predicate is **equivalent** to the link or merely **wider** than it. That is a
property of the SQL a link contributed, so §2.7.7 makes the link say it, as `Fidelity`, rather than
leaving it to a boolean nobody can make return `false`.

### 2.6 What crosses, and what the evaluator may assume

```rust
pub enum BoundaryOutput<L: Lang + ?Sized> {
    /// superset rows — the evaluator MUST re-filter and owns the rest of the chain.
    Candidates(L::Handoff),
    /// exact rows — the evaluator MUST NOT re-filter. Re-filtering would be
    /// harmless; the assertion is what keeps the two paths honest.
    Exact(L::Handoff),
    /// key-grouped, ordered and limited — at most `limit` rows.
    Reduced(L::Handoff),
}
```

The output kind is a function of the FINAL accumulated `shape` and `exact` — not of where any
prefix ended — and it is the single thing the evaluator's precondition is written against. The
evaluator receives `Lowering::how` alongside it and applies exactly the links marked `Residual`,
in chain order. All three are bounded, which is what §8 uses to place the result-size limit.

**These three kinds stop being the fold's return value.** They become a field of the SQL part that
produced them — `SqlPart::yields` (§2.7.1) — because a request is not one statement and the fold
had no way to say which statement a kind belonged to. The kinds themselves, and the rule that
derives them from the final accumulated `shape` and `exact`, are unchanged.

**And what crosses between parts is a `Seed`, which is materialised values, never a subquery
(ADR 0008 D3), and always bounded.** It crosses from one part in every case but one: a seed drawn
from the MERGE of several source statements names all of them (§2.7.4). The bound is not a nicety: an unbounded seed is what turns a
plan into a mechanism that ships rewritten rows back to the database, so a link whose
`handoff_bound` (§2.2) answers `None` does not get a cut at all (§2.7.6, rule 2).

---

### 2.7 The compiler's output is a PLAN, not a statement

**§2.7 is built, and since issue #492 part 3 it RUNS on a served route.** The types below are
`crates/pulsus-read/src/compile/plan.rs`; `traces::search_plan::plan_search` builds one plan per
TraceQL search request, `traces::exec::batch_attrs` walks the resulting chain instead of six
hand-written index loops, and `X-Pulsus-Explain: 1` on the search route returns the plan's shape as
`data.explain.plan` (docs/api.md §2.1, §4.2). **No SQL moved**: every statement is still rendered by
the shipped builders, all 83 SQL goldens are byte-unchanged and `PINNED_SQL_CORPUS` is still
`0x5b8b_80d7_38cb_049b`. Nothing in §2.7 compiles a query stage yet; what it now does is say WHICH
statements a request sends and why each is its own statement. §10 records what that establishes and
what it does not.

#### 2.7.1 The plan object

The fold's output was one relation, one disposition per link and a boundary kind. **That cannot say
what a request actually does.** §9.2's worked query sends **1,128 statements**, and the fold's output
type had no field that could hold a number other than one; worse, a SQL statement — the winners'
root read — was being described as work in our own engine. A type that cannot represent what the
system already does is wrong independently of any new requirement.

So the compiler emits a plan: an ordered list of parts, each part either one SQL statement or work
in our own engine, with the value set that crosses between parts named, typed and bounded.

```rust
// crates/pulsus-read/src/compile/plan.rs

/// The compiler's output for one request. This — not SQL text, and not a
/// boundary index — is what the executor consumes. Never empty.
pub struct QueryPlan<L: Lang + ?Sized> {
    pub parts: Vec<Part<L>>,
    /// One entry per chain link, in chain order, so that every link in
    /// the user's pipeline can be traced to the part that runs it.
    pub links: Vec<LinkOutcome>,
}

pub struct LinkOutcome {
    /// Index into `QueryPlan::parts`.
    pub part: usize,
    /// Unchanged from the fold (§2.5).
    pub how: Disposition,
}

pub enum Part<L: Lang + ?Sized> {
    /// Boxed: an `SqlPart` carries a whole `Relation` and an engine part
    /// carries two `usize`s, so without the indirection every engine part
    /// in the vector would pay the statement's width.
    Sql(Box<SqlPart<L>>),
    /// Work in our own process: the residual links, applied in chain
    /// order. `links` indexes `QueryPlan::links`.
    Engine { links: std::ops::Range<usize> },
}

pub struct SqlPart<L: Lang + ?Sized> {
    /// The clause-slot term this statement renders from (ADR 0008 D1).
    pub rel: Relation<L>,
    /// What this statement consumes from the part or parts before it.
    /// `None` for a part that OPENS the plan — and a plan can open with
    /// SEVERAL, one per source of a disjunction, none of which consumes
    /// anything (issue #492 part 3, code review round 2).
    pub seed: Option<Seed<L>>,
    /// What it produces for the part after it — §2.6's three kinds.
    pub yields: BoundaryOutput<L>,
    /// How many times the statement is sent.
    pub issue: Issue,
    /// Why this is its own statement and not folded into the previous
    /// one. `None` only for the FIRST SQL part — and unlike `seed` above
    /// that really is only the first, because the second and later
    /// branches of a disjunction each carry `Cut::DisjointSources`.
    /// Measured over the 56 committed search goldens: the set of parts
    /// with no cut is `[0]` in all 56, while the set with no seed is
    /// `[0]` in 48 and `[0, 1]` in 8.
    pub cut: Option<Cut>,
}

/// A value set crossing from one part — or from several merged — to the
/// next. Always materialised values, never a subquery (ADR 0008 D3), and
/// always bounded.
pub struct Seed<L: Lang + ?Sized> {
    /// EVERY part whose result the values are drawn from, in plan order.
    /// A list because a seed can be a MERGE: a TraceQL search disjoining
    /// across two tables opens with two statements and hydrates their
    /// merged candidate set, and one index would credit one of the two.
    pub from_parts: Vec<usize>,
    /// The language's own handoff type — trace ids, fingerprints, a
    /// keyset cursor. Unchanged: this is `L::Handoff` (§2.2).
    pub values: L::Handoff,
    /// The plan-time upper bound on how many values can be in it, and
    /// where that bound comes from. A seed with no such bound is not
    /// admissible and the cut is refused (§2.7.6, rule 2).
    pub bound: SeedBound,
}

pub enum Issue {
    /// Sent AT MOST once, and no driver is attached.
    Once,
    /// Sent once per seed drawn from `driver`, until the driver stops.
    PerSeed(Driver),
}

pub enum Driver {
    /// The seed set is bounded but too large to write into one
    /// statement, so it is sent in chunks. `chunk` is the SMALLER of
    /// what the two ceilings of §2.7.3 admit and what the language
    /// batches at.
    Chunks { bound: u64, chunk: u64 },
    /// The request's LIMIT could not enter the statement, so pages are
    /// drawn, each resuming from the previous page's last sort key,
    /// until the limit fills, the window is exhausted, or a byte budget
    /// is spent. This is today's `stage3_keyset` loop, named.
    Keyset { page_rows: u32, over_fetch: u32 },
}

/// Why a part is its own statement and not folded into the previous one.
/// See §2.7.9 for what the set rests on and for the one measured shape it
/// does not cover, and §11.3's `every_cut_variant_has_a_row_in_the_design_record`
/// — **wave 1**, and it does not exist at base — makes a fifth a build
/// failure rather than a silent addition.
pub enum Cut {
    /// §2.7.2 — the next read is over a different source, keyed by this
    /// one's result.
    SourceHandoff { source: SourceRef, key: Name },
    /// §2.7.3 — the seed does not fit in one statement.
    HandoffExceedsBound { cost: HandoffCost },
    /// §2.7.4 — an `OR` whose sides resolve against different sources.
    DisjointSources { sources: Vec<SourceRef> },
    /// §2.7.5 — the request's `LIMIT` cannot enter the statement.
    InexactLimit,
}

/// Names one readable source — a table, or a table plus the projection
/// the planner would read it through. `Relation::source` (§2.2) is a
/// `SourceTerm`, which is either a `Base(L::Source)` or a wrapped
/// relation; `SourceRef` is the comparable identity `source_of` answers
/// with, so that "a different source" is an equality the core can decide
/// without knowing either language.
pub struct SourceRef(&'static str);

/// What the plan builder may read: the request's limit, window and step,
/// and the reader config the seed bounds come from. It carries no
/// connection and performs no query — every rule in §2.7 is decided at
/// plan time, in O(1), with no round trip.
pub struct PlanCx<'a> { /* request bounds + reader config */ _p: std::marker::PhantomData<&'a ()> }

pub struct HandoffCost { pub text_bytes: u64, pub ast_elements: u64 }

pub enum SeedBound {
    RequestLimit(u32),
    Config { name: &'static str, value: u64 },
    Constant { name: &'static str, value: u64 },
}

/// Partitions a completed fold into parts. The RULES live here, in the
/// core; the FACTS come from `Lang` (§2.2). The plan builder never asks a
/// link whether to cut — it asks what the link reads and how big the
/// crossing would be, and applies the four rules of §2.7.2 to §2.7.5.
pub fn plan_of<L: Lang + ?Sized + 'static>(
    chain: &[L::Stage],
    lowering: Lowering<L>,
    cx: &PlanCx<'_>,
) -> Result<QueryPlan<L>, L::Err>;
```

`BoundaryOutput`, `Relation`, `Disposition`, `ResidualReason` and `Capability` keep the definitions
§2.2 to §2.6 give them. `Lowering<L>` becomes an internal value the plan builder consumes rather
than the compiler's public output.

**`Issue::Once` means AT MOST once, not exactly once** (issue #492 part 3). A plan is a plan-time
answer and the executor sends fewer statements when the data runs out. Measured live on a 7-span
corpus: `{ traceDuration > 2s }` and `{ span:childCount > 2 }` each issued their generator, their
hydration and their co-load and then **no root read at all**, because nothing matched and there were
no winners to summarise.

Nothing branches on the stronger reading. The variant's consumers were enumerated by renaming it at
its declaration and reading the compiler's error list, then rewriting all seven and re-running
`cargo check --workspace --all-targets` to exit 0 — which is what turned seven into *all* of them,
since `cargo check` stops at the first crate that fails and would otherwise never have reached the
packages downstream of `pulsus-read`. Of the seven, five construct the value, two are test
assertions on the wire word, and exactly one is a production branch: the inexact-limit rewrite in
`plan_of`, whose own comment reads `Once` as **no driver is attached** rather than *this will
execute*. Overwriting it is equally safe at zero executions and at one. Had any of the seven read it
as a guaranteed execution, a separate variant would have been owed and that site would have been a
defect; none does, so the repair is this sentence and the type is unchanged.

**The plan already exists in the shipped code; what is missing is a type that can say so.** The
committed TraceQL SQL goldens are written per part and index the repeated ones —
`== phase1 generator[0] ==`, `== phase2 hydration (sample batch) ==`, `== phase2 membership[0] ==`,
`== root hydration (sample winners) ==` in
`crates/pulsus-read/tests/golden/traces_search/worked_example.sql`. That file is the plan object
drawn by hand, one case at a time.

#### 2.7.2 `Cut::SourceHandoff` — the next read is over a different source, keyed by this one's result

A **cut** is the only way a plan gets a second SQL part. There are four, each decided from something
the planner already holds — no probe, no round trip, no statistics. **They were declared CLOSED and
they are not**: §2.7.9 records the measured shape none of the four explains.

**Recognised by:** `L::source_of(stage, &rel) != rel.source`, where the differing source is
reachable by a key `rel` projects. ADR 0008 D3 forbids expressing that as a subquery, on
measurement.

**Two shipped instances, and they are the whole of today's multi-statement structure.**

- LogQL resolves the selector to fingerprints over `log_streams_idx`
  (`crates/pulsus-read/src/logql/sql.rs:246`), then reads `log_streams` and `log_samples` filtered
  on `fingerprint IN (…)` (`sql.rs:489`, `sql.rs:538`). Three statements, two cuts. The seed is the
  fingerprint list, bounded by `DEFAULT_MAX_STREAMS = 100_000`
  (`crates/pulsus-read/src/logql/params.rs:121`).
- The TraceQL search response's root summary is read trace-wide with **no time bound**, and
  `TraceSearchResult.root` is not optional (`crates/pulsus-read/src/traces/exec.rs:386`,
  `crates/pulsus-read/src/traces/search_sql.rs:361`). The seed is the winners' trace ids, bounded
  by the request `limit`.

**This is the case §2.6's earlier form got structurally wrong.** `Emit` is `Never`, so §2.5's fold
makes it residual and "the evaluator owns it" — but the evaluator's way of owning it is **to send a
second SQL statement**. A plan that calls that "work in our engine" misdescribes what the request
does. The `Never` classification is correct and stays; what changes is that `plan_of` reads the
link's `source_of` and emits an `Sql` part, not an `Engine` part.

#### 2.7.3 `Cut::HandoffExceedsBound` — the seed does not fit in one statement

**Recognised by:** rendering the seed's bound against two measured ceilings, at plan time, in O(1)
— the same shape as `ensure_query_text_fits` (`crates/pulsus-read/src/querytext.rs`), no round trip.

| ceiling | value | where |
|---|---|---|
| database AST elements | 50,000 — 32,768 literal ids is 1,409,081 query bytes and is refused with `Code: 168. DB::Exception: AST is too big. Maximum: 50000.`; raising `max_query_size` does not help | ADR 0008 D3, measured |
| rendered SQL text | 8 MiB, `422 query_too_broad` | `MAX_QUERY_TEXT_BYTES`, `crates/pulsus-read/src/querytext.rs:52` |

Over either, the part becomes `Issue::PerSeed(Driver::Chunks { .. })`. That is today's phase-2 batch
loop named for what it is: `BATCH_TRACES = 32` (`crates/pulsus-read/src/traces/exec.rs:115`).

**And the chunk is 32 because the language says so, not because a ceiling says so** (issue #492
part 3). The ceilings above answer *what a statement CAN hold*; they do not answer *what the
executor sends*. Measured for a TraceQL search whose candidate seed is bounded at 100,000:

```
handoff_cost(100_000) = HandoffCost { text_bytes: 4_300_048, ast_elements: 200_004 }
handoff_cost( 24_998) = HandoffCost { text_bytes: 1_074_962, ast_elements:  50_000 }
handoff_cost( 24_999) = HandoffCost { text_bytes: 1_075_005, ast_elements:  50_002 }
```

so the AST ceiling binds at **24,998** — and the executor batches **32**. A plan reporting 24,998
would describe a batch no statement this tree has ever sent. `PlanConfig::seed_chunk_rows` carries
the language's own batch and `chunk_for` returns the smaller of the two; TraceQL passes
`Some(BATCH_TRACES)` and `None` — the default — leaves the ceilings deciding, which is what every
language did before. Pinned by `compile::plan::tests::a_language_supplied_chunk_wins_over_the_ceiling`
and `traces::compile::tests::the_phase_two_chunk_is_the_batch_constant`.

#### 2.7.4 `Cut::DisjointSources` — the disjuncts do not resolve against one source

**Recognised by:** the lattice of §2.4 already walks the boolean tree; the cut fires when an `OR`
node's two sides return different `source_of`. One `WHERE` cannot hold them, ADR 0008 D2 bans the
common-table form on measurement, and the union form is a second statement merged in our process.

**Shipped instance, and the part of it this cut does NOT cover.**
`SearchPlan::generator_sqls` is a `Vec<String>`, deduped and appended per disjunct, and executed one
at a time in the phase-1 loop (`crates/pulsus-read/src/traces/exec.rs`, module header lines 9-10). A
structural query registers two (`search_plan.rs` test
`structural_registers_both_operands_generators_and_probes`).

**It is not all of `generator_sqls`.** Measured over the 56 committed search goldens: eleven send two
phase-1 generators, and for **eight** of them the two read different tables — those eight are this
cut. For the other three (`nested_boolean`, `structural_sibling`, `structural_descendant`) both
generators read ONE table, so no `OR` over differing sources exists and this cut does not fire; the
planner sends two statements anyway. `filter.rs::collect`'s rule is about **completeness** — `a || b`
needs both sides' sets, because a match may satisfy either — and completeness is not a statement
about sources: one `WHERE` could hold `nested_boolean`'s two. Whether one `WHERE` over an `OR` prunes
as well as two ranked reads is a pushdown measurement nobody has taken, and it is owed by the part
that compiles a generator, not by the part whose contract is that no SQL moves. The three cases are a
frozen, named exception, asserted as an EQUALITY by
`traces_search_plan_parts::the_generator_fan_out_exception_is_exactly_these_three`, so a fourth
cannot join them silently.

**The literal SQL is committed and byte-frozen, so this is not a prediction.**
`crates/pulsus-read/tests/golden/traces_search/mixed_or.sql` is
`{ duration > 2s || span.foo = "x" }`, and it holds **two** phase-1 generators, one per source:

```sql
== phase1 generator[0] ==
SELECT trace_id, max(timestamp_ns) AS bound_ts
FROM trace_spans
WHERE timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000
  AND (duration_ns > 2000000000)
GROUP BY trace_id
ORDER BY bound_ts DESC, trace_id ASC
LIMIT 100001

== phase1 generator[1] ==
SELECT trace_id, max(timestamp_ns) AS bound_ts
FROM trace_attrs_idx
WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15')
  AND timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000
  AND (key = 'foo' AND val = 'x' AND scope = 'span')
GROUP BY trace_id
ORDER BY bound_ts DESC, trace_id ASC
LIMIT 100001
```

`duration` is a physical column of `trace_spans`; `span.foo` is a row of `trace_attrs_idx`. One
`WHERE` cannot hold both, so the shipped planner already emits two statements and merges them —
and this is `Cut::DisjointSources` with its recogniser (`source_of` differs across the `OR` node),
its `Issue::Once` on each part, and its literal SQL, in the tree today. The file is byte-frozen by
`the_sql_golden_corpus_matches_its_committed_digest`, which **exists** and prints `Starting 1 test`
at exit 0 (§11.1), so the quotation above cannot drift without that gate reddening.

**This narrows a rule §2.4 already carried and did not bound.** The lattice says `a || b` becomes
`sql_a OR sql_b` in one statement. That is right **when both sides read the same source**, and wrong
when they do not: `resource.service.name` is a physical column of `trace_spans`
(`crates/pulsus-schema/src/catalog.rs:358`, ordered by `(trace_id, timestamp_ns)`) while
`span.http.method` is a row of `trace_attrs_idx` (`catalog.rs:382`, ordered by
`(key, val, scope, timestamp_ns, trace_id, span_id)`). A disjunction over one of each is reachable,
not theoretical.

#### 2.7.5 `Cut::InexactLimit` — the request's `LIMIT` cannot enter the statement; and why compiling is greedy

**Recognised by:** `request.limit.is_some() && rel.limit.is_none() && !rel.exact` after the fold.
The last SQL part becomes `Issue::PerSeed(Driver::Keyset { .. })` — **unless its seed is already
bounded by the request's own `LIMIT`**, in which case it sits DOWNSTREAM of the limit rather than
being the loop that fills it, and no driver is attached (issue #492 part 3, D4). Every TraceQL
search's last statement is that shape: the winners' root read, seeded by at most `limit` trace ids
and issued once after the limit is satisfied. Measured over the 56 committed search goldens, no part
carries `Issue::PerSeed(Driver::Keyset { .. })` and none carries `Cut::InexactLimit`; the shipped
instance below is LogQL's, and it is the only one.

**Shipped instance:** `StreamsPlan::fetch_until_limit` (`crates/pulsus-read/src/logql/plan.rs:80`,
set at `:1625` from `has_unpushed_dropping_stage`, `:1655`), and when it is set the read is one
statement per page through `stage3_keyset` (`crates/pulsus-read/src/logql/sql.rs:625`) with
`scan_limit = result_limit × reader.logql_pipeline_scan_factor`. §2.7.7 is what can turn this cut
off.

**And this is where the greedy question is answered — once, here, rather than by a hook.** Under the
cost model of §9.1 — bytes counted per hop, the client hop and the `pulsus-server`↔database hop
metered, compute and database memory fixed — a link that can become SQL always should. It removes
rows from the metered hop, it never adds a round trip, and the only costs it can add are database
CPU and database memory. **Two measurements say the two natural objections are wrong, and both cut
the same way:**

- A LogQL predicate over a parser-produced name prunes **no granule at all**: over 3,000,000 rows
  `EXPLAIN indexes=1` lists `MinMax`, `Partition` and `PrimaryKey` and **no `Skip` section**; the
  primary key cuts 367 granules to 124 and nothing cuts further. It is still worth compiling: page
  density went from 250 matching entries per 1,000-row page to 1,000, so the page loop needs a
  quarter of the rounds and moves a quarter of the bytes for the same answer
  ([query-to-sql.md](query-to-sql.md) part 9, *Measured*).
- A TraceQL regular-expression leaf reads **245× the rows** of the equality form for an identical
  one-row answer (1,225 granules / 10,023,040 rows against 5 / 40,960 — §3.4). Not compiling it does
  not make the read narrower; it moves the same 10,035,200 rows across the metered hop instead of
  one.

So there is no measured case in either document where declining wins, and the honest form of that is
a stated consequence of the cost model, not a hook nobody can make return `false`. **What would
falsify it:** one query where a compiled link increases metered-hop bytes or round trips.
`should_lower` is deleted for that reason (§2.2), and `ResidualReason::Policy` goes with it, because
with no hook nothing could construct it and a variant with no producer is dead code shaped like a
decision point.

#### 2.7.6 Three shapes that must NOT cut, stated as rules because they are the expensive mistakes

1. **A residual link mid-pipeline does not cut.** The fold continues and a later link contributes to
   the *same* statement. Withdrawing this is the measured **20.6×** metered-byte regression of §9.6
   and it stands untouched. The boundary diagram's pipeline D is the shape.
2. **A part may not be seeded by a value our own engine computed per row.** Under the cost model
   (§9.1) such a seed crosses the metered hop twice and its size grows with the rows read. **Every
   admissible seed is bounded by a plan-time constant** — the request `limit`, `DEFAULT_MAX_STREAMS`,
   `reader.traceql_max_candidates`, `BATCH_TRACES` — and `L::handoff_bound` returning `None` is what
   refuses the cut. This is what stops a plan from shipping rewritten lines back to the database
   after a stage that rewrites the line.
3. **A predicate that engages no index does not cut and is not declined.** §2.7.5 measures why.

#### 2.7.7 `Fidelity` — what the compiled SQL means, relative to the link

Whether LogQL's third part is one statement carrying the request `LIMIT` or an iterated keyset loop
turns on whether the compiled predicate is **equivalent** to the link or merely **wider** than it.
§7.1's rows state that as a table cell; it is a property of the SQL a link contributed, so the link
is what says it (`Lower::fidelity`, §2.2):

```rust
pub enum Fidelity {
    /// `orig <=> sql`. The evaluator must NOT re-apply this link.
    Equivalent,
    /// `orig => sql`. The evaluator MUST re-apply this link.
    Wider,
}
```

The fold gains one line — `rel.exact &= matches!(f, Fidelity::Equivalent);` (§2.5) — and its
traversal, its arms and its bijection with `ResidualReason` are otherwise untouched.

**This settles [query-to-sql.md](query-to-sql.md)'s open question 5, which today costs a page loop.**
A filter over a structured-metadata key compiles to `JSONExtractString(structured_metadata, 'k') = 'v'`
over a stored column our own encoder writes and our own flat reader reads
(`crates/pulsus-read/src/logql/labels.rs:157-189`), with no guard and no ambiguity: that is
`Equivalent`, so `Limit` may lower, so the read is one statement rather than `stage3_keyset`'s loop.
A filter over a **parser-produced** name is `Wider` by construction — its predicate carries guard
terms that keep lines SQL cannot decide — so the loop stays. One mechanism, two answers, and neither
is a rule anyone has to remember.

**Why `Wider` is the default and not `Equivalent`:** `Wider` is exactly today's behaviour on every
link, so a link whose author has not thought about it cannot make the plan wrong — it can only make
it no better than today.

#### 2.7.8 One assumption `Fidelity` must NOT make: that a pipeline has one regex dialect

**A `Fidelity::Equivalent` verdict on a regex leaf would be a claim that two regular-expression
engines read the pattern the same way, and on the TraceQL path they do not.** This is recorded here
because §2.7.7's mechanism is exactly where the mistake would be made, and because it is measured
rather than suspected.

Our other two languages rewrite a user pattern into the Rust `regex` crate's dialect before
compiling it, so that the crate reads it the way RE2 does — `pulsus_re2::re2_pattern_to_rust`,
applied at `crates/pulsus-read/src/metrics/labels.rs:274` and `:620`,
`crates/pulsus-read/src/metrics/re2_authority.rs:89` and `crates/pulsus-read/src/logql/plan.rs:171`.
**The TraceQL path applies it nowhere.** `git grep -n re2_pattern_to_rust -- crates/pulsus-read/src/traces/ crates/pulsus-traceql/src/`
returns no line; `search_plan.rs:942` compiles the **raw** pattern with
`pulsus_re2::compile_user_regex_anchored(pat)`, which is `^(?:pat)$` built by
`regex::RegexBuilder` with a size budget and no rewrite
(`crates/pulsus-re2/src/compile_budget.rs:343`).

So one pattern gets two readings. Measured — the Rust side by a probe calling the two functions
directly, the RE2 side on ClickHouse **26.3.29.7** (a newer patch than the `26.3.17.110` every cost
figure in §9 and ADR 0008 was taken on; only the regex semantics below were taken on it):

| pattern | subject | rewritten to | ClickHouse RE2 — Phase 1 | raw Rust crate — Phase 2 |
|---|---|---|---|---|
| `\d` | `٤` U+0664 | `[0-9]` | **no** | **yes** |
| `\d` | `4` | `[0-9]` | yes | yes |
| `\w` | `é` U+00E9 | `[0-9A-Za-z_]` | **no** | **yes** |
| `\s` | U+00A0 | `[\t\n\f\r ]` | **no** | **yes** |

`a{2}`, `a}` and `\pL` agree on both sides, so the split is the three ASCII-class shorthands rather
than everything.

**Which leaves are exposed, and which are not.** An **attribute** regex is evaluated only in
ClickHouse, through `match(val, …)` (`crates/pulsus-read/src/traces/filter.rs:810`), so it has one
dialect and one reading. The exposed set is the leaves `plan_physical` and `plan_trace_ctx` compile
a `StrOp::Re`/`Nre` for (`search_plan.rs:961-1040`) — `name`, `service`, `statusMessage`,
`span:id`, `span:parentID`, `instrumentation:name`, `instrumentation:version`, `rootName` and
`rootServiceName` — because those are re-checked in our process at `search_plan.rs:201-202` after a
generator has already selected on them. The committed golden
`crates/pulsus-read/tests/golden/traces_search/service_regex.sql` shows the Phase-1 half for one of
them: `{ resource.service.name =~ "check.*" }` renders `match(val, '^(?:check.*)$')`.

**Three rules follow, and they bind wave 1.**

1. **No regex leaf may return `Fidelity::Equivalent`** unless the pattern is one the two engines
   provably read alike. `Wider` is the default and is the safe answer here — but note it is only
   safe in one direction. `Wider` means `orig ⟹ sql`; on the table above the SQL reading is
   **narrower** than our own evaluator's, so if our evaluator is taken as `orig` the invariant of
   §2.4 is **violated, not merely loosened**, and rows are lost rather than over-returned.
2. **Which reading is authoritative is not this document's to decide.** The reference is Go and
   therefore RE2, which is the ClickHouse side; that makes our Phase-2 evaluator the divergent half
   and the defect ours. Recorded, not fixed here.
3. **A cut may not be justified by "the evaluator re-applies it".** §2.7.6's rules are about where
   work happens; this is about whether the two places agree, and for these nine leaves they do not.

**What this does NOT establish.** The components were measured separately and the coupling was read,
not run: no request was sent end to end through both phases with one of these patterns. Component
agreement is not an end-to-end measurement, and the query that would settle it is named in the plan
on [#492](https://github.com/digitalis-io/pulsusdb/issues/492). It is also not this design's defect
to repair — it ships today, independently of anything here.

#### 2.7.9 What the four cuts rest on, and what would falsify it

The argument that the four were **closed** is that each is derived from one of exactly two things a
single statement cannot do — read a second source keyed by its own result, or hold more than fits —
plus the two forms of "more than fits": the seed's size (§2.7.3), and the answer's when the `LIMIT`
cannot enter (§2.7.5). **What would falsify it:** a query in either language whose correct plan has
two SQL parts and no cut in the list.

**That witness exists, it is committed, and the closure claim is therefore withdrawn** (issue #492
part 3). `crates/pulsus-read/tests/golden/traces_search/nested_boolean.sql` is
`{ (.a = "1" || .b = "2") && (.c = "3" || .d = "4") }` and it holds two phase-1 generator
statements, **both reading `trace_attrs_idx`**; `structural_sibling.sql` is the same shape and
`structural_descendant.sql` is the same shape against `trace_spans`. Two SQL parts, and no cut in
the set of four explains the second: `Cut::SourceHandoff` needs a different source,
`Cut::DisjointSources` needs an `OR` whose sides read different sources, `Cut::HandoffExceedsBound`
needs a seed that does not fit, and `Cut::InexactLimit` is about the answer's size. See §2.7.4 for
the rule that produces them and why it is not a rule about sources.

**No fifth cut is added here, and the reason is not caution.** Choosing between two ranked reads and
one `OR`ed read changes what SQL is sent; part 3's whole contract is that no SQL moves, so that
choice cannot be made inside it without destroying the one property that makes a moved golden
unambiguously a defect. The measurement — does one `WHERE` over an `OR` prune as well as two ranked
reads — is owed by the part that compiles a generator, and the three cases are frozen as a named
exception with the reason attached until then.

§11.3's `every_cut_variant_has_a_row_in_the_design_record` — **wave 1**, and it does not exist at
base — makes a fifth cut a build failure rather than a silent addition, but no gate can discover
that a fifth is *needed*: this one was found by building the plan and comparing it against what the
goldens render, which is what
`traces_search_plan_parts::the_plan_sql_parts_match_the_sections_each_golden_case_renders` now does
on every run.

**The measurements this rests on, and which of them we have.**

| measurement | have it? |
|---|---|
| does a predicate touch a table's key prefix, and is there a skip index on the column | **yes**, statically, from `crates/pulsus-schema/src/catalog.rs` |
| the request's limit, window and step | **yes** |
| a seed's plan-time upper bound | **yes** — every one is a request parameter, a config field or a named constant |
| a seed's rendered size against the two ceilings | **yes**, O(1), no round trip |
| how many rows a predicate will match — its selectivity | **no.** There is no statistics catalogue, and the only two shipped ways to get a number are round-trip probes: the regular-expression matcher `count()` probe (`crates/pulsus-read/src/logql/sql.rs:283`) and the grouping cardinality pre-flight. **No rule in §2.7 may depend on selectivity**, and none does |
| the per-row cost of a database-side expression against the cost of transporting the row | **no.** Nothing measures it. Under the cost model of §9.1 it does not matter; if that model is ever revised this is the first number needed |
| behaviour across shards | **out of scope** by owner ruling on [#492](https://github.com/digitalis-io/pulsusdb/issues/492) |
| behaviour at 1 TB | **no** — [#25](https://github.com/digitalis-io/pulsusdb/issues/25) |

---

## 3. TraceQL against the model

### 3.1 The complete TraceQL link set

**Enumerated from the AST, not from this design's needs.** `PipelineStage`
([`crates/pulsus-traceql/src/ast.rs`](../crates/pulsus-traceql/src/ast.rs), line 981) has exactly
**eight** variants; all eight are below, together with `Source` and the three synthesised links of
§2.1. Every row states the **residual state effect** — what the link applies to the accumulated
`Relation` when it does *not* lower (§2.5) — because a link with no stated effect is a link whose
blocking behaviour the reader has to infer.

#### Payload validation runs BEFORE the fold, and the rejection governs

**A disposition in the table below is only ever reached by a payload the shipped planner accepts.**
`plan_pipeline` (`crates/pulsus-read/src/traces/search_plan.rs:1083`) refuses several payloads of
`Aggregate`, `By` and `Select` with `PlanError`, which
`crates/pulsus-server/src/traces_api/error.rs:304` maps to **`400`** with
`Content-Type: text/plain; charset=utf-8` (`:270-277`). Without this rule the design would be a
silent widening of the accept surface: a payload the shipped code refuses would instead become a
residual link and be *answered*. So:

> **For every payload `plan_pipeline` rejects, the rejection governs and the disposition is
> unreachable.** The chain builder validates the payload first and returns the same `PlanError`;
> no link is constructed, no `Relation` exists, and the request is the same `400` it is today.
> Nothing in this document widens what a query may mean — issue #492's "not in scope: changing
> what the queries mean".

Every rejection, with the body captured from `plan_search` on this tree at `2f78c53`. The last
column says whether a **parsed and validated** query can reach the arm at all, or whether the
parser or `pulsus_traceql::validate` refuses first — a parser-shadowed arm is defence in depth and
cannot be reached by any request:

| variant | rejected payload | `400` body, verbatim | reachable from a parsed+validated query |
|---|---|---|---|
| `Aggregate` | regex comparison operator (`search_plan.rs:1161`) | `type mismatch: aggregate filters do not support regex operators` | **no** — `validate` answers `illegal operation for the given types: count() =~ 2` |
| `Aggregate` | `count()` given a field (`:1189`) | `type mismatch: count() takes no field` | **no** — parse error `expected ')' (count() takes no argument)` |
| `Aggregate` | a one-arity op given no field (`:1194`) | ``type mismatch: `<op>`() requires a field`` | **no** — parse error `expected an aggregatable field (duration or an attribute)` |
| `Aggregate` | a non-numeric intrinsic argument (`:1200`) | `type mismatch: span:childCount is not numerically aggregatable` | **yes** — `{ .service.namespace = "prod" } \| max(span:childCount) > 1` |
| `Aggregate` | a composite argument expression (`:1212`) | `type mismatch: max((.a + .b)) is not an executable aggregation source: only a bare duration or attribute can be aggregated` | **yes** — `… \| max(.a + .b) > 1` |
| `Aggregate` | a duration threshold on a non-duration aggregate (`:1057`) | `type mismatch: aggregate comparisons require a numeric (or duration, for duration aggregates) threshold` | **yes** — `… \| max(.a) > 1s` and `… \| count() > 1s` |
| `Aggregate` | a non-finite numeric threshold (`:1046`) | `type mismatch: not a finite number: "999…"` | **yes** — `… \| max(.a) > <310 nines>`. The arm parses the raw literal as `f64` and filters on `is_finite`, so any decimal integer literal above `f64::MAX` reaches it; **measured** at 309, 310 and 320 digits, all three rejected here, while a 320-digit *fraction* is finite and plans. `nan`, `inf`, `1e400` and a leading `-` are refused by the lexer, but they are not the only spelling |
| `By` | a composite key expression (`:1125`) | `type mismatch: by((.a + .b)) is not a group key this engine can execute: a grouping key must resolve to a single per-span value, so it must be an attribute or an intrinsic` | **yes** — `… \| by(.a + .b) \| count() > 1` |
| `By` | a span-event / span-link intrinsic key (`:1435`) | `unsupported field: by(event:name): grouping by a span-event / span-link intrinsic is not supported (a span carries a collection of events/links, so there is no single group value)` | **yes** — `… \| by(event:name) \| count() > 1` |
| `Select` | a nested-set intrinsic (`:1277`) | `type mismatch: select() of a nested-set intrinsic is not supported` | **yes** — `… \| select(nestedSetLeft)` |
| `Select` | one of the twelve trace-level / scoped / event / link intrinsics (`:1322`) | `type mismatch: select() of this intrinsic is not supported` | **yes** — `… \| select(rootName)` |
| `Filter` | a mid-pipeline spanset OPERATION rather than a single filter | `type mismatch: ({ .b = 2 } && { .c = 3 }) is not executable as a pipeline stage: a ``|`` stage must be a single { ... } filter, not a cross-spanset or structural operation` | **yes** — `{ .a = 1 } \| { .b = 2 } && { .c = 3 }`. The reference's pipeline element is a full spanset expression, so the parser accepts it and the planner decides |

`Coalesce` is zero-arity and has no payload to reject. `Metric`, `MetricSecondStage` and `Compare`
are rejected whole rather than by payload and are already "not in the chain" below.

**Three of the twelve rows are unreachable, and the fourth was not.** An earlier revision of this
table marked the non-finite numeric threshold parser-shadowed on the strength of `nan`, `inf` and
`1e400` all being refused by the lexer. They are — but a long decimal literal is not, and
`{ .service.namespace = "prod" } | max(.a) > <320 nines>` parses, validates and returns
`400 type mismatch: not a finite number: "999…"` from `search_plan.rs:1046`, whose rule is
`raw.parse::<f64>()` filtered on `is_finite()`, `search_plan.rs:1042` to `:1046`. **An unreachability
claim is a universal over inputs**, so each of the four was re-checked by constructing the input
that would defeat it rather than by reading the lexer: three spellings each for the regex-operator,
`count()`-with-field and one-arity-without-field rows, and ten for the numeric threshold, including
309, 310 and 320 digits, a negative, a 320-digit *fraction* (which is finite and **plans**),
scientific notation, and the bare `nan` and `inf` words. The probe is `492-r6-probe-shadowing.rs`
in the architect's session scratchpad, run on this tree at `2f78c53`; §7.1's four shadowed rows were
re-checked the same way, with three to eight spellings each, and all four held.

| link | accepts → produces | precondition to lower | residual state effect | disposition | continuation |
|---|---|---|---|---|---|
| `Source` — `SpansetExpr` (`ast.rs:99`) | — → `Spans` | none; lowers by §2.4's lattice | n/a — the seed is always applied. An unlowerable leaf contributes `1` and clears `exact` | **always lowers, possibly partially** | *none*, unless the selector is a disjunction over two sources — then `Cut::DisjointSources` (§2.7.4) |
| `Hydrate` (synthesised, `traces/compile.rs:169`) | any → same shape | **never lowers** (`No(NotYetLowered)`) — the batch hydration read is a second statement over a different source keyed by this statement's result, and no SQL form has been written that would put it INTO the seed statement. That is what `NotYetLowered` says here, as against `Never`: nothing about the read is impossible, only unwritten | **none — the identity.** The read adds rows the evaluator consults; it rewrites no column's provenance and narrows no predicate. The row asserts the identity rather than leaving the exemption silent | never lowers | **`Cut::SourceHandoff`** (§2.7.2) — source `trace_spans` (hydration), key `trace_id`, `SeedBound::Config { reader.traceql_max_candidates }` |
| `Membership(i)` (synthesised, `:172`) | any → same shape | never lowers (`No(NotYetLowered)`), same reason as `Hydrate`. `i` indexes `SearchPlan::probes`, so one attribute probe is one link and one statement | **none — the identity** | never lowers | **`Cut::SourceHandoff`** — source `trace_attrs_idx` (membership), key `trace_id`, `SeedBound::Config` |
| `AggValues(i)` (synthesised, `:175`) | any → same shape | never lowers (`No(NotYetLowered)`). `i` indexes `SearchPlan::agg_fields`: one aggregate operand's `val_num` batch read | **none — the identity** | never lowers | **`Cut::SourceHandoff`** — source `trace_attrs_idx` (values), key `trace_id`, `SeedBound::Config` |
| `SelectValues(i)` (synthesised, `:178`) | any → same shape | never lowers (`No(NotYetLowered)`). `i` indexes `SearchPlan::select_attrs`: one `select()` field's `val` batch read. It shares a source with `AggValues` and is a separate link because it is a separate statement | **none — the identity** | never lowers | **`Cut::SourceHandoff`** — source `trace_attrs_idx` (values), key `trace_id`, `SeedBound::Config` |
| `EventSet(i)` (synthesised, `:181`) | any → same shape | never lowers (`No(NotYetLowered)`). `i` indexes `SearchPlan::event_sets`: one span-event / span-link value-set batch read | **none — the identity** | never lowers | **`Cut::SourceHandoff`** — source `trace_attrs_idx` (event sets), key `trace_id`, `SeedBound::Config` |
| `TraceCtx` (synthesised, `:183`) | any → same shape | **`Never(TraceLevelIntrinsic)`**, and the reason is the co-load's REACH rather than a missing SQL form: the trace-context read is deliberately trace-wide and unwindowed, so `traceDuration`, `rootName` and `rootServiceName` evaluate full-trace-exact whatever the search window is. A window-bounded statement cannot read those rows, in any state | **none — the identity** | **never lowers, in any state** | **`Cut::SourceHandoff`** — source `trace_spans` (trace context), key `trace_id`, `SeedBound::Config` |
| `ChildCount` (synthesised, `:185`) | any → same shape | **`Never(TraceLevelIntrinsic)`**, same reason: `span:childCount` is counted over the whole trace, not over the window | **none — the identity** | never lowers, in any state | **`Cut::SourceHandoff`** — source `trace_spans` (child counts), key `trace_id`, `SeedBound::Config` |
| `Structural` (synthesised, `:187`) | any → same shape | **`Never(StructuralRelation)`** — the relation holds between two spans of one trace, over a span set our own batching defines. Nothing in the seed statement's row scope can decide it | **clears `exact`** — the generators are the superset union of both operands' sets and the relation is applied afterwards, so the SQL means strictly more than the query | never lowers, in any state | *none* — the link reads no new source, so there is no handoff and no second part |
| `NestedSet` (synthesised, `:189`) | any → same shape | **`Never(NestedSetNumbering)`** — a modified-preorder numbering computed per trace at query time; no stored column carries it | **clears `exact`** | never lowers, in any state | *none* — same reason |
| `BoolTruth` (synthesised, `:192`) | any → same shape | **`Never(WholeQueryTypeFailure)`** — one row's type must fail the WHOLE request (a present non-boolean operand under `!` is an error for the query, not a non-match for the span) and SQL evaluates row by row | **clears `exact`** | never lowers, in any state | *none* — same reason |
| `Aggregate { op, field, cmp, value }` (`ast.rs:994`) | `Spans` → `Traces` | `exact`, **no fragment already in this statement's `HAVING`**, and a fragment `aggregate_having_sql` will render for this (aggregate, operator) pair and this grouping | **shape unchanged** — whatever the fold has accumulated, not reset to `Spans`; **clears `exact`** — the evaluator will drop traces the SQL returned | conditional, **over the accepted payload set only** (above) | *none* |
| `By { key }` (`ast.rs:1046`) | `Spans` → `Groups{key}` | never lowers (`No(NotYetLowered)`) — the evaluator builds the span sets | **shape unchanged**; records the key as an evaluator-owned group consumer; and then EITHER records `grouping` and leaves `exact` alone, when the key renders on this generator's source and the slot is free and the relation is still exact, OR clears `exact` | never lowers | *none* |
| `Coalesce` (`ast.rs:1049`), after a `By` | `Groups` → `Spans` | the level carries no `HAVING` — then the grouping slot is FREED. With a `HAVING` it refuses: the aggregate selected groups, and the spans it selected are not recoverable | **shape unchanged** — `Groups` in the ordinary case, but `Spans` if the preceding `By` was itself residual; clears `exact` when it refuses | conditional | *none* |
| `Coalesce`, with no preceding `By` | `Spans` → `Spans` | none — the identity | none | **always lowers**, contributing no SQL | *none* |
| `Select { fields }` (`ast.rs:1024`) | any → same shape | **never lowers.** `apply` returns the relation unchanged and `capability` has no `Yes` arm, so field resolution decides only which `BlockReason` is reported: `select(name)` reports `NotYetLowered` and every attribute spelling reports `NameNotResolvable`, because a TraceQL seed's `ColSet` is `Closed([trace_id, name])`. Measured on both seed sources by `traces::compile::tests::select_refuses_and_names_its_reason_per_field`. **No exactness precondition** — projecting a column onto rows the evaluator will drop would be harmless | **wider `cols`**: no existing column moves, and `set_provenance` ADDS the selected field as `EvaluatorOnly` (`compile/fold.rs:242`), which the effect table already expects (`traces/compile.rs:1745`) | **never lowers** — the two refusal reasons are the only outcomes, and `NameNotResolvable` is what the explain surface renders (`compile/plan.rs:879`) for every spelling a client writes | *none* here; a left join would need an ADR 0008 clause that does not exist — [query-to-sql.md](query-to-sql.md) open question 4, and §9.8 measured the join and refused it |
| `Filter(SpansetExpr)` (`ast.rs:1021`, issue #492 item 9) | `Spans` → `Spans` | **never lowers** (`No(NotYetLowered)`) — and the reason is soundness, not unfinished work. Pushing the filter as a `WHERE` conjunct is WRONG whenever the leading spanset is not a single filter: for `{ .tag = "x" } && { name = "a" } \| { .tag = "y" }` the qualifying span is supplied by the RIGHT operand, so `val = 'y'` ANDed onto the left leaf's `trace_attrs_idx` generator matches nothing and the trace is dropped. It would also favour one spelling over the identical `{A && B}`, which does not push its second leaf | **shape unchanged**; **clears `exact`** — the evaluator will drop spans, and traces, that the SQL returned | never lowers | *none*. It does decide WHICH generator statement phase 1 sends — `filter::collect`'s `&&` fold continued across the pipe, so `{A} \| {B}` sends the statement `{A && B}` sends — but that is a choice among statements the query already implies, not a fragment added to one |
| `Metric(MetricStage)` (`ast.rs:1055`) | — | **not a search-path link.** `plan_pipeline` answers `400` (`search_plan.rs:1854`) | n/a | **not in the chain** — the metrics routes compile it in full already (`metrics_sql.rs:90`) | n/a |
| `MetricSecondStage(SecondStage)` (`ast.rs:1059`) | — | `400` on search (`search_plan.rs:1861`) | n/a | not in the chain | n/a |
| `Compare { .. }` (`ast.rs:1071`) | — | `400` on search (`search_plan.rs:1867`) | n/a | not in the chain | n/a |
| `Order` (synthesised) | `Traces` → `Traces` | `exact` — over a superset the sort **key** is wrong, not just the set (§2.2) | leaves `ordering` unset | conditional | *none* |
| `Limit(n)` (synthesised) | `Traces` → `Traces` | `ordering.is_some()` | leaves `limit` unset | conditional | *none* |
| `Emit` (synthesised) | `Traces` \| `Groups` → answer | none — see below | records the winners' root read as the evaluator's | **must go residual**: `Never(NeedsUnwindowedRootRead)` | **served by a second SQL part, not by the evaluator** — `Cut::SourceHandoff` (§2.7.2), seeded by the winners' trace ids, `SeedBound::RequestLimit`, `Issue::Once` |

Four consequences fall out of the table rather than being written down.

- **`By` after `Aggregate` is `No`**, because `Aggregate` produced `Traces` and `By` accepts
  `Spans`. `By` becomes residual and the fold carries on to the links after it.
- **`Coalesce` after `By` lowers by wrapping**, because its grouping slot is occupied, while
  `Coalesce` with no preceding `By` is the identity and costs nothing — one rule, not two special
  cases.
- **`Emit` is `Never`, and a lowered TraceQL search is four statements, not one.** The root summary
  is read trace-wide with **no time predicate** (the true root may predate the search window —
  [schemas.md §4.2](schemas.md)), and `TraceSearchResult.root` is not optional
  (`crates/pulsus-read/src/traces/exec.rs:385`), so every search response needs it. That is exactly
  why the winners' root read exists today (`exec.rs:2063`), and lowering does not remove it: it
  removes the 1,108 round trips between it and the generator. The window-bounded hydration read and
  the membership read survive lowering for their own reason — `spanSets[].matched` and
  `spanSets[].spans[]` are written unconditionally
  (`crates/pulsus-server/src/traces_api/search_response.rs:428-430`, `:507-512`) — so the count is
  the generator plus those two plus the root read. §1, §4 and §9.2 count **4**; the hops diagram
  still counts 2 and says so on its own face, until part 8 redraws it.
- **The three metrics variants are not chain links at all on this route.** They are listed so the
  enumeration is complete against the AST rather than against the search planner's subset; a reader
  checking `PipelineStage` against this table finds every variant.
- **The table is complete against `TqlLink`, not only against `PipelineStage`, and that is new in
  part 8.** It used to carry rows for 5 of the enum's 15 variants — `Source`, `Pipe(_)`, `Order`,
  `Limit` and `Emit` — while calling itself the complete link set, and three of the missing ten
  (`Hydrate`, `Membership(n)`, `SelectValues(n)`) appear in **every** rendered plan on the search
  route. `every_traceql_chain_link_has_a_row_in_the_lowering_document` enumerates the enum
  exhaustively with no `_` arm, so a sixteenth variant fails to build rather than going unlisted.

### 3.2 Group 1 — cannot be lowered

See §5, which covers both languages.

### 3.3 Group 2 — could be lowered, has not been

[`metrics_sql`](../crates/pulsus-read/src/traces/metrics_sql.rs) already compiles a `{...}` filter
body to SQL — `compile_filter_predicate` (line 90) → `render_expr` (200) → `lower_leaf` (354),
with attribute leaves lowered by `semi_join_sql` (490). **The search path does not call it.**
Every row below is "this compiler already handles the leaf; nothing wraps its output in the rest
of the pipeline."

| class | what it costs today | ranked from |
|---|---|---|
| **spanset aggregate** (`count`/`sum`/`avg`/`min`/`max`) | the whole two-phase loop: 1,128 round trips, 77,572,021 metered bytes, 5,795,940,946 rows read (§9.2) | **measured on C1** |
| **`by()` regrouping** | adds no query of its own; its saving is the same loop collapse when the selector is lowerable | argued — it adds no read |
| **`select()` projection** | one extra read per batch; +4.6 KiB per request and one extra round trip. **Measured and refused** in §9.8: for the query whose only attribute-index read is the `select()` value read there is nothing to merge it with, and putting an attribute value into a `trace_spans` statement is a join | measured on C2 (issue #478); the refusal measured on §9.8's corpus |
| **field-vs-field comparison** `{ .a = .b }` | **four** `attr_values_sql` reads per batch, not two — each attribute operand is interned into `select_attrs` *and* into `agg_fields` (`plan_operand`, `search_plan.rs:1358-1359`), so a two-operand leaf reads both values twice. One whole request on C6: 37 statements and 300,984,841 rows read when 1 trace in 10 matches, **3,127 statements and 25,904,824,756 rows read** when 1 in 1,000 does (§9.7) | **measured on C6** |
| **cross-field arithmetic** `{ .a * 2 > .b }` | the same four reads per batch; 347 statements and 2,869,590,609 rows read for a request matching 9,000 traces (§9.7) | **measured on C6** |
| **event/link set comparison** `{ .a = event:name }` | one `event_set_sql` co-load per batch **plus the scalar operand's two value reads**; 2,502 statements and 13,995,704,756 rows read (§9.7) | **measured on C6** |
| **negated attribute leaf** `{ .a != "5" }` | drops the generator to the empty-predicate time-range superset (`GenClass::TimeRange`, `filter.rs:102`) and adds no read of its own, so the window's whole span scan is the cost: 4 statements, 12,097,152 rows read, 1,482 granules (§9.7) | **measured on C6** |

**Every group-2 class shares one saving mechanism** — collapsing the phase-2 loop — so the classes
differ mainly in whether they *block* the collapse, not in how much each would save alone. §9.7
measures what the collapse is worth for the four **measured on C6** rows, and it is not one number:
the same
query text saves about **1.2x** on the metered hop when 1 trace in 10 matches and **390x – 460x**
when 1 in 1,000 does — a range because four takes of the same pair disagree on the metered column.
**The saving is a function of selectivity, not of the class.**

**A row left this table, and it is a correction rather than a re-ranking.** `{ name != "x" }` was
listed here as widening the candidate generator to the whole window. It does not: the predicate is
rendered into a bounded span scan. It is a group-3 class — lowered already, prunes nothing — and it
is now in §3.4 with the measurement that says so. The construct that *does* produce the
empty-predicate time-range superset is the negated **attribute** leaf, which is the fourth row
above. The two are separate constructs and the record had them crossed.

### 3.4 Group 3 — lowered already, prunes nothing

`trace_attrs_idx` is `ORDER BY (key, val, scope, timestamp_ns, trace_id, span_id)`
([`catalog.rs`](../crates/pulsus-schema/src/catalog.rs), line 370). `val` is the second key column,
so a predicate on it prunes only if it is a **range**. `val = 'x'` is a point range;
`match(val, …)` and `val_num <op> n` are neither, so pruning stops at `key` and every row carrying
that key inside the window is read. `GenClass::AttrKeyScan` (`filter.rs:95`) already names this
correctly — the cost has just never been written down.

Corpus C1, 5-day window, `trace_attrs_idx` at 6,110 granules:

| leaf | SQL fragment | granules | rows read | rows that match |
|---|---|---|---|---|
| `{ span.http.method = "GET" }` | `key='http.method' AND val='GET' AND scope='span'` | **210** | 1,720,320 | 1,666,667 |
| `{ span.http.method =~ "GET" }` | `key='http.method' AND match(val,'^(?:GET)$') AND scope='span'` | **1,225** | 10,035,200 | 1,666,667 |
| `{ span.http.method =~ "GE.*" }` | `… match(val,'^(?:GE.*)$') …` | **1,225** | 10,035,200 | 1,666,667 |
| `{ span.http.status_code >= 500 }` | `key='http.status_code' AND val_num >= 500 AND scope='span'` | **1,225** | 10,035,200 | 2,500,000 |
| `{ span.user.id = "u-4242" }` | `key='user.id' AND val='u-4242' AND scope='span'` | **5** | 40,960 | 1 |
| `{ span.user.id =~ "u-4242" }` | `key='user.id' AND match(val,'^(?:u-4242)$') AND scope='span'` | **1,225** | 10,023,040 | 1 |

The last pair is the sharpest statement of the group: **identical one-row answer, 245x the rows
read.** Being lowered and being cheap are separate claims.

Three more entries in this group:

- **The negated physical leaf `{ name != "x" }`**, which this document listed under §3.3 until
  issue #492 part 6 measured it. `compile_leaf` sends a physical predicate through
  `spans_generator_for` (`filter.rs:2296`), which always returns `GenClass::SpanScan` with the
  predicate rendered into the `WHERE`. There is no widening to the whole window and there is
  nothing left to lower. On corpus C6 (§9.7) the negated form and the positive form select the
  **same granules** and read the **same rows**:

  | generator statement | result rows | rows read | granules | result bytes |
  |---|---|---|---|---|
  | `… AND (name != 'op-0') …` | 100,001 | 10,000,000 | **1,226/1,226** | 4,767,944 |
  | `… AND (name = 'op-0') …` | 4,000 | 10,000,000 | **1,226/1,226** | 97,672 |

  `EXPLAIN indexes = 1` prints the same three index blocks for both, and the `name` predicate
  appears in none of them — only the time bound does:

  ```
  Condition: and((timestamp_ns in (-Inf, 1700432001000000000]),
                 (timestamp_ns in [1699999999000000001, +Inf)))
  Parts: 6/6
  Granules: 1226/1226
  Search Algorithm: generic exclusion search
  ```

  Same reads, 49x the result bytes. The cost is the candidate rows themselves, and they genuinely
  match — a negation over 2,500 span names matches 2,499 of them. Lowering cannot help a predicate
  that is already in the statement and already selects everything.
  `a_negated_physical_leaf_keeps_its_predicate_a_negated_attribute_leaf_does_not`
  (`crates/pulsus-read/tests/traceql_group2_selector_generators.rs`) goes red if the generator
  stops being a `SpanScan` carrying its own predicate, and the same test asserts the negated
  attribute leaf's `GenClass::TimeRange` beside it, because a true sentence about one of them is
  what made the other one wrong.

- **The phase-2 candidate restriction `trace_id IN (…)` on `trace_spans`** prunes badly, and the
  rule that predicts it is §9.4.
- **`PREWHERE service = '…'` in the phase-1 generator** is a no-op relative to `WHERE`: both forms
  select 25 of 1,230 granules and read 204,800 rows, at 1,501,542 bytes off the file system †.
  `optimize_move_to_prewhere` is on by default, so the `WHERE` spelling is moved anyway. The
  pruning that does happen comes from the `service_time` projection, not from the keyword. Keep
  the `PREWHERE` — it is explicit and version-independent — but stop counting it as an
  optimisation.

---

## 4. Worked example

`{ .service.namespace = "prod" } | max(duration) > 1s`, `limit=20`, 5-day window.

**Today** — one generator, then 563 iterations of two queries, then one root read:

```sql
-- phase 1, once
SELECT trace_id, max(timestamp_ns) AS bound_ts
FROM trace_attrs_idx
WHERE date >= toDate('…') AND date <= toDate('…')
  AND timestamp_ns > … AND timestamp_ns <= …
  AND (key = 'service.namespace' AND val = 'prod')
GROUP BY trace_id ORDER BY bound_ts DESC, trace_id ASC LIMIT 100001

-- phase 2, per batch of 32 candidates, 563 times, serially
SELECT trace_id, span_id, parent_id, <9 capped/plain columns>
FROM trace_spans
WHERE trace_id IN (unhex('…'), … 32 of them)
  AND timestamp_ns > … AND timestamp_ns <= …
ORDER BY trace_id ASC, timestamp_ns ASC, span_id ASC
LIMIT 10001 BY trace_id

SELECT DISTINCT trace_id, span_id
FROM trace_attrs_idx
WHERE date >= … AND date <= …
  AND (key = 'service.namespace' AND val = 'prod' AND scope = 'resource')
  AND timestamp_ns > … AND timestamp_ns <= …
  AND trace_id IN (unhex('…'), … the same 32)
```

**Lowered** — **four statements**: the lowered generator below, the window-bounded per-batch
hydration read, the membership read for the selector's one attribute condition, and the same
winners' root read the evaluator still owns because `Emit` is `Never` (§3.1). The last three are
unchanged from today, so only the first is shown in full:

```sql
SELECT trace_id, max(timestamp_ns) AS sort_key
FROM trace_attrs_idx
WHERE date >= toDate('…') AND date <= toDate('…')
  AND timestamp_ns > … AND timestamp_ns <= …
  AND key = 'service.namespace' AND val = 'prod' AND scope = 'resource'
GROUP BY trace_id
HAVING max(duration_ns) > 1000000000
ORDER BY sort_key DESC, trace_id ASC
LIMIT 20
```

```sql
-- the winners' root read, unchanged (`search_sql.rs:345`): 20 literal ids,
-- no time predicate and no row cap, because the true root may predate the
-- search window
SELECT trace_id, span_id, parent_id, <byte-capped service>, <byte-capped name>,
       timestamp_ns, duration_ns
FROM trace_spans
WHERE trace_id IN (unhex('…'), … the 20 winners)
```

`trace_attrs_idx` carries `timestamp_ns` and `duration_ns` denormalised per attribute row, so for
a single-attribute-leaf selector with a `duration`- or `count`-sourced aggregate **the attribute
index covers the whole query** — no join, no subquery, no second table.

**Four round trips, not one, and that is the number every other section quotes.** 1,128 → **4**;
77,572,021 B → **212,986 B**; 5,795,940,946 rows → **19,988,480**; 707,689 granules → **2,440**.
Both sides of every ratio include the root read, so the comparison is like for like — today's
1,128 round trips include it too (§9.2). **All four lowered totals are measured over all four
lowered statements** (§9.2b), the window-bounded hydration read and the membership read included.
An earlier revision of this paragraph published `seed + root only` totals — the generator's row
added to the root read's, with the two surviving phase-2 reads left out. Those are
superseded by the §9.2 re-measurement.

---

## 5. What can never be lowered, and why

These return `Capability::Never`, and the distinction from `No` is carried in the type so that
nobody later reads them as unfinished work.

| construct | why SQL does not have the information |
|---|---|
| **structural relations** `>` `>>` `<` `<<` `~` and their `!`/`&` forms | the relation holds between two spans of one trace and is evaluated over the **hydrated** span set — window-bounded and truncated at `MAX_SPANS_PER_TRACE` = 10,000 (`exec.rs:119`). The answer is a function of our own batching, so a SQL form would have to reproduce a limit that only the client-side query defines |
| **the nested-set numbering** `nestedSetLeft`, `nestedSetRight`, and `nestedSetParent` outside the root sentinel | a modified-preorder numbering computed per trace at query time from the `parent_id` forest; no stored column carries it. The root sentinel **is** expressible and is already lowered (`metrics_sql.rs:414`) |
| **trace-level intrinsics** `traceDuration`, `rootName`, `rootServiceName`, `span:childCount` | resolved from a co-load that is deliberately trace-wide with **no time predicate**, because the true root may predate the window. A window-bounded statement cannot read those rows at all. Already refused on the metrics path for this reason (`lower_leaf`, `metrics_sql.rs:354`) |
| **the `!` operator's whole-query type failure** | `{ !.a }` against a present non-boolean must fail the entire request, not skip the span. SQL evaluates row by row and cannot turn one row's type into a request-level refusal. The matching half is expressible, the failure half is not, and they are one leaf (`LeafEval::BoolTruth`, `filter.rs:369`) |
| **`Emit` on the traces search route** | the response's root summary is read trace-wide and unwindowed, the same reason as the trace-level intrinsics — and `TraceSearchResult.root` is not optional (`crates/pulsus-read/src/traces/exec.rs:386`), so this is unconditional on that route, not a case that sometimes arises. **`Never` is the right classification and it does not mean the evaluator does the work**: the way the evaluator owns this link is to send a second statement, so `plan_of` gives it its own SQL part (`Cut::SourceHandoff`, §2.7.2). "Cannot be lowered into THIS statement" and "is not SQL" are different claims, and only the first is made here |

**Cross-attribute comparison is deliberately not in this table.** `{ .a = .b }` compares two rows
of the attribute index sharing a `(trace_id, span_id)`; the information is present, and the SQL
that decides it is a **per-span pre-grouping** — `GROUP BY trace_id, span_id` with the comparison
in a `HAVING`. **No join**: ADR 0008 authorises none ("A clause these rules do not name: the
join"), and none is needed. So it is a group-2 class the core can reach later (§3.3). Calling it
impossible would be wrong — what it is not is *cheap*, and §9.7 measures how far from cheap: on
the attribute index as it is ordered today the pre-grouping needs about 10.5 GiB of aggregation
state on a 10,000,000-span window, against a shipped 512 MiB generator ceiling, so issue #492
part 6 measured it and refused it rather than shipping it.

**LogQL's candidate `Never` class was settled by [#507](https://github.com/digitalis-io/pulsusdb/issues/507),
and two of the three are not `Never`.** The class was "a link whose output depends on an in-engine
rewrite of the line". The question was whether a SQL expression can reproduce the rewrite, and it
was answered by running each expression on a container rather than by reading:

| link | SQL | verdict |
|---|---|---|
| `decolorize` | `replaceRegexpAll(body, '\x1B\[[0-9;]*m', '')` — our own implementation is the single pattern `DECOLORIZE_PATTERN` applied with `replace_all` (`crates/pulsus-read/src/logql/pipeline.rs`) | **`No(NotYetLowered)`**, not `Never` |
| `unpack` | `if(JSONHas(body,'_entry'), JSONExtractString(body,'_entry'), body)` — the line becomes the packed object's `_entry` when present, otherwise unchanged | **`No`**, not `Never`, for the line. Its label promotion is the open-column-set case and needs no new mechanism |
| `line_format` | a Go text/template evaluated per line | still open; #507 treats it as producing `Computed` only when the chain is residual from there |

So `decolorize` and `unpack` produce `Provenance::Computed(expr)` rather than blocking, and a
following line filter lowers **against the rewritten expression**. That is a strictly larger
lowerable set than this document's first version assumed, and it is only reachable because a
residual link still applies its state effect (§2.5).

**One caveat #507 owns and this document must not pre-empt:** the reference matches a line filter
after `decolorize` against the **raw** line, which our tree appears not to do. If that holds on
both sides it is a user-visible divergence, and the SQL above must reproduce whichever behaviour
is ratified — not whichever is convenient. #507 is measuring our side.

---

## 6. Where the sharing stops, and why

Forcing a common abstraction over things that genuinely differ is worse than two clean mechanisms.
The boundary below is part of the design, not an admission.

**Two different claims are made below and they are not the same strength.** "Generic by
construction" means the type system or the fold enforces it: a language cannot supply a variant of
it, and getting it wrong is a build failure. "Per-language work" means each language writes its own
and the core only fixes the shape of the obligation. Everything in the second column is **asserted**
generic in the sense that it is expected to fit; only the first column is generic in the sense that
it cannot fail to.

| mechanism | generic **by construction** — what the core enforces | **per-language work** — what each language supplies |
|---|---|---|
| the chain | `&[L::Stage]` folded left, and `Order`/`Limit`/`Emit` synthesised as ordinary links | the link type itself (`PipelineStage` + 3, or `LqlLink`) and the chain builder that produces it |
| capability | `Capability`'s three outcomes, evaluated against the **accumulated** `Relation`, and their bijection with `ResidualReason` | the rule each link answers with |
| dispositions | the fold applies `apply` on `Lowered` and **`residual_effect` on every other outcome**, for every link, with no early return — compiled (§2.5) | what each link's `residual_effect` *does* |
| shapes | `L::Shape: Eq`, and the requirement that a stage's input shape match the accumulated one | the shape lattice: `Spans`/`Traces`/`Groups` against `Lines`/`Samples`/`Series` |
| columns | `ColSet`, the `OpenSource` resolver, and per-column provenance | which open sources exist and what each resolves |
| predicates | the `orig ⟹ sql` lattice including `NOT`-refuses-unless-exact | every SQL *fragment*: predicates, column expressions, escaping |
| composition | `Relation` as a clause-slot term and ADR 0008's wrap-on-slot-collision rule; the renderer **skeleton** | fragment construction, regex handling, time-bucket expressions |
| the boundary | `BoundaryOutput`'s three kinds as `SqlPart::yields`, the `Seed` that crosses between parts, and the cap placement that follows (§8) | the `Handoff` type and the evaluator that consumes it |
| plan shape | the four cuts (§2.7.2–§2.7.5), the three must-not-cut rules (§2.7.6), and the refusal of any cut whose seed has no plan-time bound | the three facts of §2.2 — `source_of`, `handoff_bound`, `handoff_cost` — and each link's `fidelity` |
| errors | `L::Err` as an associated type | the error taxonomy and its HTTP mapping |

**Nothing moved from the right column to the left by argument.** One row moved by compiling: the
fold's guarantee that a residual link still gets `residual_effect` was previously asserted in prose
and did not compile (§2.5); it now compiles, so it is in the left column. The rest of the right
column stays there and is named as per-language work rather than described as shared.

**Three things that are deliberately not shared:**

1. **`Shape` is an associated type, not a shared enum.** TraceQL's shapes are `Spans`/`Traces`/
   `Groups`; LogQL's are `Lines`/`Samples`/`Series`. One enum over both is a union with
   per-language invalid states, and every `match` on it acquires unreachable arms. The core knows
   only that shapes are `Eq` and must match across a stage boundary.
2. **The renderer is shared only as a skeleton.** Clause slots and nesting are common; fragment
   construction is not, and must not be — LogQL's escaping, regex handling and time-bucket
   expressions have nothing to do with TraceQL's.
3. **The plan-shape FACTS are per language; the plan-shape RULES are not.** There is no cost
   policy hook: §2.7.5 answers the greedy question once, for both languages, as a consequence of
   §9.1's cost model, and §2.7.7 handles the one place the two languages genuinely differ — LogQL's
   keyset paging — through `Fidelity`, which is a property of a link's SQL rather than a policy.
   What a language still supplies is what the core cannot know: which source a link would read, how
   big its handoff can get, and what that handoff costs to render.

**Where the code lives.** Both read paths are already modules of **one crate** —
`crates/pulsus-read/src/logql/` and `crates/pulsus-read/src/traces/`, with
`crates/pulsus-read/src/metrics/` for PromQL. The core is a sibling module,
`crates/pulsus-read/src/compile/` — **named `compile`, not `lower`**, settled by owner ruling on
[#492](https://github.com/digitalis-io/pulsusdb/issues/492) and recorded as
[query-to-sql.md](query-to-sql.md)'s open question 3: that document avoids the term throughout, and
a word kept out of the prose has no business entering the tree as a path and as module identifiers.
The per-language impls are `crates/pulsus-read/src/logql/compile.rs` and
`crates/pulsus-read/src/traces/compile.rs`. The core **introduces no new dependency edge**:
`crates/pulsus-read/Cargo.toml` already depends on `pulsus-logql` and `pulsus-traceql`, and the
core depends on neither, being generic over `Lang`. A separate crate would be the wrong call — it
would need both AST crates or a third set of types, for no gain.

One constraint follows from an existing workaround in that same manifest: `clickhouse` is a
**direct** dependency because its `#[derive(Row)]` macro expands to unqualified `clickhouse::…`
paths. The lesson transfers — **the core must export traits and plain types and no derive macro**,
or every future consumer inherits a direct dependency it did not ask for.

---

## 7. LogQL against the model

**This section is read from source and carries no measurement. The inventory and its numbers are
[#507](https://github.com/digitalis-io/pulsusdb/issues/507)'s** — measuring LogQL stages here
would produce a second set of figures that disagreed with that work.

### 7.1 The complete LogQL link set

**LogQL's chain link is not its AST stage enum, and this document said otherwise.** The previous
version equated `L::Stage` with `pulsus_logql::Stage`. That is wrong in one direction and, in the
place #507 wrote it, wrong in the other: `Stage` (`crates/pulsus-logql/src/ast.rs:133`) has ten
variants and carries **none** of the window, the range aggregation, the vector aggregation, the
ordering, the limit or the response builder — while `Unwrap` **is** one of the ten. `LogRange`'s own
`unwrap` field is *"retained-but-unused … the parser represents `| unwrap …` as an ordered
`Stage::Unwrap` inside `selector.pipeline` … and always leaves this field `None`"*
(`ast.rs:2294-2299`, `parser.rs:1298`, and the defence-in-depth comment at
`crates/pulsus-read/src/logql/plan.rs:1753`). So `Unwrap` reaches the chain through `Pipe`, in its
written position — which is the whole reason the parser puts it there, so post-`unwrap` label
filters keep theirs — and a separate `Unwrap` link would be a second spelling of one construct.

```rust
// crates/pulsus-read/src/logql/compile.rs

/// LogQL's chain link. `pulsus_logql::Stage` is ONE arm, carrying all ten
/// of its variants in the pipeline's own order; the window and the two
/// aggregation levels live in `LogRange` (`ast.rs:2301`) and `MetricExpr`
/// (`ast.rs:939`) and are synthesised into links by the chain builder.
pub enum LqlLink {
    Pipe(pulsus_logql::Stage),
    Window { range_ns: i64, step_ns: i64, offset_ns: i64, grid_start_ns: i64 },
    RangeAgg  { op: pulsus_logql::RangeAggOp,  grouping: Option<pulsus_logql::Grouping>, param: Option<String> },
    VectorAgg { op: pulsus_logql::VectorAggOp, grouping: Option<pulsus_logql::Grouping>, param: Option<String> },
    LabelReplace { dst: String, replacement: String, src: String, regex: String },
    Order,
    Limit(u32),
    Emit,
}
```

`param` is `Option<String>` and not `Option<f64>` because the AST keeps the `quantile_over_time`
and `topk` parameters as **raw text** so it can derive `Eq`/`Hash` (`ast.rs:943-946`, `:959-962`);
parsing to `f64` is the planner's job and doing it in the link would move a parse error out of the
one place that reports it.

The chain a metric query lowers is

```
Source -> Pipe(Stage) x n -> Window -> RangeAgg -> (VectorAgg | LabelReplace) x m -> Order -> Limit -> Emit
```

**`LabelReplace` is one of the *m* post-`RangeAgg` links, interleaved with the `VectorAgg` levels
rather than fixed before or after them.** `MetricExpr::LabelReplace`
(`crates/pulsus-logql/src/ast.rs:1002`) carries an `inner: Child<MetricExpr>` and is a `metricExpr`
alternative, so it composes in every metric position; an earlier version of this diagram omitted it
while the table below carried it, and the two disagreed.

**The *m* links cannot be derived from `unwrap_vector_aggs`, and the sentence that said they could
was wrong twice over.** `unwrap_vector_aggs_into` (`plan.rs:2179`) descends the spine and
`ControlFlow::Break`s at the first non-`Vector` `MetricExpr` — `LabelReplace` included — so it
never sees a level below one. Worse, a query carrying a `label_replace` **never produces a
`MetricPlan` at all**: `plan_metric_expr` (`plan.rs:1088`) routes on that same base, and only a
`MetricExpr::Range` base reaches `metric_plan`; everything else becomes `Plan::MetricBinary` over a
`MetricNode` tree, which has no `vector_aggs` field to reverse. Measured on this tree at `2f78c53`
through the real planner:

```
sum by (a) (label_replace(topk(3, count_over_time({service_name="svc-lr"} | logfmt [5m])), "a", "$1", "src", "(.*)"))
  -> Plan::MetricBinary  (no vector_aggs field)
     VectorAgg { aggs: [(Sum, By ["a"], None)], inner: LabelReplace { .. inner: Leaf(MetricPlan { .. }) } }

topk(2, sum by (region) (count_over_time({service_name="metrics-c"} | logfmt [1m])))
  -> Plan::Metric  vector_aggs=[(Topk, None, Some(2.0)), (Sum, By ["region"], None)]
```

The second line also shows the direction: `vector_aggs` is stored **outer-first**
(`MetricPlan::vector_aggs`, `plan.rs:343-348`) and the evaluator applies it innermost-first with a
`.rev()` walk (`post_agg.rs:3008-3011`).

**So the builder walks the `MetricExpr` spine and emits both link kinds, innermost first** — the
shape `build_metric_node` (`plan.rs:1171`) already uses: a pre-order descent emitting one `PlanOp`
per spine node, consumed in reverse. "Take `unwrap_vector_aggs`' list and reverse it" is not a
sufficient builder and must not be implemented as one.

`Pipe(Stage::Unwrap(u))` sits at its written position among the *n*. A log query is the same chain
with no `Window`, no aggregation levels and no `Unwrap` — which `plan.rs:1616` enforces as a `400`
(`` `unwrap` is only valid inside a range aggregation (e.g. sum_over_time({...} | unwrap x [5m])) ``,
captured from the planner), since an unwrapped value means nothing outside a range aggregation.

#### Payload validation runs BEFORE the fold here too

The rule §3.1 states for TraceQL is not a TraceQL rule: **for every payload the LogQL planner
rejects, the rejection governs and the disposition below is unreachable.** `ReadError::PipelineInvalid`
maps to **`400`** with `Content-Type: text/plain; charset=utf-8` and `X-Content-Type-Options: nosniff`
(`crates/pulsus-server/src/logs_api/error.rs:212`, `:147-157`); the body is the bare reason
(`crates/pulsus-read/src/logql/error.rs:771-772`).

**How this table is derived, because the previous one was transcribed and missed two rejections a
user can reach today.** The enumeration is over a literal scope: **every `ReadError::` construction
in `crates/pulsus-read/src/logql/plan.rs` above `mod tests` (`plan.rs:3206`)**, which is 25 sites at
`2f78c53`, listed by `grep -n 'ReadError::[A-Z]' crates/pulsus-read/src/logql/plan.rs`. Every one of
the 25 is either a row below or is excluded beneath the table with its reason, so completeness is a
property of that grep and not of anyone's reading. Each row's body **and its reachability** were
then produced by sending a query through `logql::plan::plan` on this tree at `2f78c53` — the probe
is `492-r4-probe-logql-rejections.rs` in the architect's session scratchpad, and its printed output
is the source of every cell. The previous table was built by reading the `format!` strings, and
reading missed `plan.rs:1227` and `plan.rs:1392` entirely, cited two sites one and five lines off,
and folded a helper with four distinct message bodies into a single row.

| link | rejected payload | reached by | `400` body, verbatim |
|---|---|---|---|
| `VectorAgg` | a bare scalar literal as the aggregated operand (`plan.rs:1227`) | `sum(1)` · `topk(2, 1)` · `sum by (x) (1)` | `a vector aggregation cannot aggregate a bare scalar literal` |
| `VectorAgg` | `sort`/`sort_desc` carrying a grouping clause (`plan.rs:1490`) | `sort by (x) (count_over_time({service_name="checkout"}[5m]))` | `` `sort` does not accept a grouping clause `` |
| `VectorAgg` | `approx_topk` on a range query (`plan.rs:1501`) | `approx_topk(3, count_over_time({service_name="checkout"}[5m]))` **as a range query** | `count min sketches are only supported on instant queries` |
| `VectorAgg` | an op that takes `k`, given none (`plan.rs:1508`) | **nothing — parser-shadowed.** `topk(count_over_time({service_name="checkout"}[5m]))` is refused by the parser: `unexpected identifier "count_over_time" at byte 5: expected the k parameter (e.g. topk(5, ...))` | `` `<op>` requires a k parameter (e.g. <op>(5, ...)) `` — unreachable |
| `VectorAgg` | an op that takes no parameter, given one (`plan.rs:1513`) | **nothing — parser-shadowed.** `sum(3, count_over_time({service_name="checkout"}[5m]))` is refused: `unexpected ',' at byte 5: expected ')'` | `` `<op>` takes no parameter `` — unreachable |
| `VectorAgg` | a `k` that is not a finite number (`plan.rs:1466`, from `plan.rs:1506`) | **nothing — parser-shadowed.** `topk(<320 nines>, count_over_time({service_name="checkout"}[5m]))` is refused: `invalid parameter topk(…)` | `` invalid `<op>` parameter "…" `` — unreachable |
| `RangeAgg` | a quantile that is not a finite number (`plan.rs:1466`, from `plan.rs:1796`) | `quantile_over_time(<320 nines>, {service_name="checkout"} \| unwrap latency [5m])` | `invalid quantile parameter "999…"` |
| `RangeAgg` | an op that requires `unwrap`, without one (`plan.rs:1778`) | `sum_over_time({service_name="checkout"}[5m])` | `invalid aggregation sum_over_time without unwrap` |
| `RangeAgg` | an op that forbids `unwrap`, with one (`plan.rs:1783`) | `count_over_time({service_name="checkout"} \| unwrap latency [5m])` | `invalid aggregation count_over_time with unwrap` |
| `RangeAgg` | `quantile_over_time` with no quantile (`plan.rs:1801`) | **nothing — parser-shadowed.** `quantile_over_time({service_name="checkout"} \| unwrap latency [5m])` is refused: `unexpected '{' at byte 19: expected the quantile parameter (e.g. 0.95)` | `quantile_over_time requires a quantile parameter` — unreachable |
| `Pipe(Stage::Unwrap)` | an `unwrap` in a **log** query (`plan.rs:1616`) | `{service_name="checkout"} \| unwrap latency` | `` `unwrap` is only valid inside a range aggregation (e.g. sum_over_time({...} \| unwrap x [5m])) `` |
| `LabelReplace` | a **scalar** operand (`plan.rs:1392`) | `label_replace(1, "d", "$1", "src", "(.*)")` · `label_replace(1 + 2, …)` · `sum(label_replace(1, …))` | `label_replace requires a vector operand, got a scalar expression` |
| `LabelReplace` | a regex using a group flag RE2 does not have (`plan.rs:167`) | `label_replace(count_over_time({service_name="checkout"}[5m]), "d", "$1", "src", "(?x)a")` | ``invalid regex in label_replace: a `(?x`/`(?u`/`(?R` group flag RE2 does not have: `(?x)a` `` |
| `LabelReplace` | a regex that does not compile (`plan.rs:176`) | `label_replace(count_over_time({service_name="checkout"}[5m]), "d", "$1", "src", "a(")` | `invalid regex in label_replace: regex parse error: … error: unclosed group` |

**14 rows over 13 of the 25 construction sites.** `plan.rs:1466` is a shared helper with **four**
call sites and therefore four distinct message bodies; two of them — `plan.rs:1506` and
`plan.rs:1796` — are reached from a chain link and get a row each, which is why 13 sites give 14
rows. The helper's other two call sites, `plan.rs:1254` (`invalid scalar literal "…"`) and
`plan.rs:1263` (`invalid vector() value "…"`), belong to `MetricExpr::Literal` and `VectorFn`,
which the synthesised-link table marks **not in the chain**; both were confirmed reachable by the
probe, so they are excluded by that marking rather than by an assumption that nothing reaches them.

**Four of the fourteen rows are unreachable** — the parser refuses the payload first — and they are
kept as rows rather than deleted, because a later parser change would make them reachable and a
deleted row would not be there to notice.

**The other 12 construction sites, and why each is not a row.** This list exists so the grep above
**partitions** the 25 rather than merely covering part of them: 13 + 12 = 25, with no site in both
lists and none in neither.

- `plan.rs:1075` (`QuerySpanTooLong`), `plan.rs:1106` and `plan.rs:1725` (`InvalidStep`),
  `plan.rs:2364`, `plan.rs:2542`, `plan.rs:2558` (`QueryTooBroad`) — **request parameters and
  resource guards, not a link payload.** They refuse the request before any chain exists.
- `plan.rs:2601` — one `reject` closure returned from three conditions (`plan.rs:2609`,
  `plan.rs:2618`, `plan.rs:2627`; those three are call sites, not constructions, so they are not
  among the 25). It states the shape a `variants(…)` operand must have, and `Variants` is
  **out of scope, named** in the synthesised-link table above. Reachable, and checked:
  `variants(sum(topk(2, count_over_time({service_name="checkout"}[5m])))) of ({service_name="checkout"}[5m])`
  returns `variant 0 must be a range aggregation, optionally wrapped in one vector aggregation …`.
- `plan.rs:2677`, `plan.rs:2682`, `plan.rs:2691` — the same three range-aggregation arity
  rejections as the rows above, re-checked on the `variants(…)` path. Identical bodies, identical
  links, reached only through a construct that is not in the chain.
- `plan.rs:2985` (`ContradictoryMatchers`) and `plan.rs:3015` (`EmptyMatcherSet`) — the
  **selector's** payload. `Source` "always lowers" and has no rejectable payload of its own; the
  selector is refused before a chain is built. Recorded rather than assumed:
  `{service_name="checkout", service_name="other"}` does return
  `matchers are contradictory: the selector can never match a stream`, while `{service_name=~".*"}`
  **plans** — so `plan.rs:3015` was not reached by the obvious candidate and is excluded on the
  link argument, not on a demonstration.

§3.1's TraceQL table was enumerated the same way in the previous round, in the source direction over
`crates/pulsus-read/src/traces/search_plan.rs`, and came back complete at eleven rows; it is not
re-derived here. Issue #492 item 9 added the twelfth row with the arm it describes — the
mid-pipeline spanset OPERATION — so the table is twelve rows and the enumeration above covers
eleven of them.

**The five parameter rejections stay the planner's, and the link must not re-implement them.**
`parse_vector_agg_params` (`plan.rs:1480`) is the sole producer of parsed aggregation parameters and
says so in its own doc comment; that is why `RangeAgg::param` and `VectorAgg::param` are
`Option<String>` copied verbatim from the AST and the link performs no parse and no validation.

#### The ten `Stage` variants, each as a `Pipe` link

| link | accepts → produces | precondition to lower | residual state effect | disposition | continuation |
|---|---|---|---|---|---|
| `LineFilter(lf)` (`ast.rs:134`) | `Lines` → `Lines` | `body` provenance resolves to a SQL expression — `Stored`, or `Computed(e)` from `decolorize`/`unpack` (§5) — **and** `is_pushable_line_filter(lf)` (`plan.rs:3086`: no `ip()` alternative) | **clears `exact`**: it removes lines in the evaluator, so the SQL result is a superset | conditional | *none* |
| `Parser(Json { extractions })` (`ast.rs:237`) | `Lines` → `Lines`, `cols` widened by an open source over `body` | `body`'s provenance is expressible | `cols` still widened, but with an **evaluator-only** open source whose `resolve` answers `None`, so a following `LabelFilter` goes residual instead of lowering against a name SQL cannot see. Does **not** clear `exact` | conditional | *none* |
| `Parser(Logfmt { strict, keep_empty, extractions })` (`ast.rs:242`) | as above | as above | as above | conditional | *none* |
| `Parser(Regexp(re))` (`ast.rs:248`) | as above, names = capture groups | as above, and the pattern expressible | as above | conditional | *none* |
| `Parser(Pattern(p))` (`ast.rs:251`) | as above, names = `<name>` captures | as above | as above | conditional | *none* |
| `LabelFilter(expr)` (`ast.rs:136`) | `Lines` → `Lines` | every referenced name resolves in `cols`, and the comparison is expressible | **clears `exact`** — it drops lines in the evaluator | conditional | *none*. **Fidelity `Wider`** over a parser-produced name (its predicate carries guard terms SQL cannot decide), **`Equivalent`** over a structured-metadata key — which is what decides whether the `Limit` link may lower (§2.7.7) |
| `LineFormat(tpl)` (`ast.rs:140`) | `Lines` → `Lines`, `body` → `Computed` | none today: a Go text/template has no SQL form here | sets `body` provenance `Computed` with **no resolvable expression**, so every later link needing the line goes residual. Does **not** clear `exact` — it removes no lines | **must go residual** today: `No(NotYetLowered)`, not `Never` (§5) | *none* |
| `LabelFormat(fmts)` (`ast.rs:141`) | `Lines` → `Lines`, `cols` rewritten | every source name resolves and the template is a rename or a constant | `cols` rewritten with evaluator-only provenance for each rewritten name; `exact` untouched | conditional | *none* |
| `Unwrap(u)` (`ast.rs:142`) | `Lines` → `Samples{value}` | `u.label` resolves in `cols` and `u.conversion` is expressible | **shape unchanged** — always `Lines` today, because the parser refuses a second `unwrap` (§11.2b), but stated as preservation like every other row, because a state rule that is only true by grace of a parser restriction breaks silently when the restriction moves — and the sample source becomes evaluator-owned — which is what makes a following `RangeAgg`, whose input shape is `Samples`, refuse | conditional | *none* |
| `Unpack` (`ast.rs:146`) | `Lines` → `Lines`, `body` → `Computed`, labels promoted | the `_entry` rewrite is expressible (§5) | sets `body` `Computed(expr)`; promoted labels arrive as an open source | conditional | *none* |
| `Decolorize` (`ast.rs:149`) | `Lines` → `Lines`, `body` → `Computed` | the SGR strip is expressible (§5) | sets `body` `Computed(expr)` | conditional, with the open caveat in §5 | *none* |
| `Drop(elems)` (`ast.rs:151`) | `Lines` → `Lines`, `cols` narrowed | every named label resolves; any value matcher is expressible | `cols` rewritten with the named labels removed, so a later filter on a dropped name refuses; `exact` untouched | conditional | *none* |
| `Keep(elems)` (`ast.rs:154`) | `Lines` → `Lines`, `cols` narrowed to the complement | as `Drop` | as `Drop`, complemented | conditional. **Shares the payload type `Vec<DropKeepElem>` with `Drop`**, which is why the dispatcher takes the stage (R3) | *none* |

#### The synthesised links, and the seven `MetricExpr` variants

| link | source | accepts → produces | precondition to lower | residual state effect | disposition | continuation |
|---|---|---|---|---|---|---|
| `Source` | the stream selector (`LogQL`'s `{…}`) | — → `Lines` | none; the seed is lowered by the predicate lattice rather than by the stage fold, so it always emits | **none — the identity.** The seed is always applied, so there is no residual case, and the row asserts the identity rather than leaving the exemption silent (`logql/compile.rs:314-317`) | **always lowers**, `Fidelity::Equivalent` | *none* |
| `Window` | `LogRange` (`ast.rs:2301`) + the request step | `Lines`\|`Samples` → the same, bucketed | the origin-shifted bucket expression is emittable and the offset is representable | records the bucketing as evaluator-owned, so a following aggregation cannot lower | conditional | *none* |
| `RangeAgg` | `MetricExpr::Range` (`ast.rs:940`) | `Samples` → `Series{by}` | `exact`, the `Window` lowered, and `__error__` either filtered or carried in the grouping | **shape unchanged** — `Lines` whenever the `Unwrap` above went residual, which is the case its own row describes; clears `exact` | conditional. `AbsentOverTime` is `Never`: the answer is a statement about rows that are **absent**, so there is no row to compute it from | *none* |
| `VectorAgg`, one link per level | `MetricExpr::Vector` (`ast.rs:956`) | `Series` → `Series` | the prior level lowered and the grouping is expressible | retains the prior series state; clears `exact` | conditional | *none* |
| `LabelReplace` | `MetricExpr::LabelReplace` (`ast.rs:1002`) | `Series` → `Series` | none today — see below | retains series state; **clears `exact`**, because at range it can REMOVE series: colliding post-rewrite label sets merge (`post_agg.rs:3307`, `merge_matrix_collisions`) | must go residual today: `No(NotYetLowered)` | *none* |
| `Order` | the request direction | `Lines`\|`Series` → same | the ordering columns are in the projection | leaves `ordering` unset | conditional | *none* |
| `Limit(n)` | the request limit | `Lines`\|`Series` → same | `ordering.is_some()` **and `exact`** — a `LIMIT` over a superset loses rows a residual link would have kept | leaves `limit` unset, which is today's oversample path | conditional | **the same SQL part, issued once per page** when `!exact` — `Cut::InexactLimit` (§2.7.5), `Issue::PerSeed(Driver::Keyset)`, which is today's `fetch_until_limit` loop. `Issue::Once` with the `LIMIT` in the statement when every earlier link was `Fidelity::Equivalent` |
| `Emit` | the response builder | → answer | none | records the response build as the evaluator's | **must go residual** | *none* on the LogQL routes: the response is built from rows the last SQL part already returned |
| `MetricExpr::Literal` (`ast.rs:967`), `VectorFn` (`:972`) | — | — | scalar leaves, not chain links | n/a | not in the chain | n/a |
| `MetricExpr::Binary` (`ast.rs:977`), `Variants` (`:992`) | — | — | **trees, not chains.** A left fold cannot represent two operands | n/a | out of scope, named | n/a |

**Why `LabelReplace` clears `exact`, and why a grouped SQL form cannot replace it.** An earlier
version of this row said it "removes no series". At **range** it does: colliding label sets merge
into one series whose points repeat per grid timestamp, by a k-way *stable* merge that preserves
the duplicates (`crates/pulsus-read/src/logql/post_agg.rs:3307-3350`). The committed corpus carries
the case with both sides, captured from the pinned reference (grafana/loki:3.7.4, capture
2026-08-02 — `crates/pulsus-read/tests/logqltest/corpus/b16_label_replace.test:1-12`):

- `r1` (`:252-256`) — the operand alone, `label_replace(count_over_time({service_name="svc-lr"} | logfmt [5m]), "dst", "v-$1", "src", "source-value-(.*)")`, returns **four** series;
- `r2` (`:261-262`) — `label_replace(sum by (src) (count_over_time({service_name="svc-lr"} | logfmt [5m])), "src", "same", "src", "(.*)")` returns **one**: `{src="same"} 30s 1 30s 1 30s 1 30s 1 60s 1 60s 1 60s 1 60s 1`;
- `c14` (`:198-202`) — the same collision at **instant** returns four duplicate samples instead.

Four series to one, with four points at each of two timestamps. A `GROUP BY` on the rewritten key
would give one point per timestamp, so the construct does not lower to a grouped query — that is
the obstacle, not an absence of a SQL spelling for the rewrite. And because the residual link
removes series the SQL still returns, the SQL is a superset: `exact` is cleared, on the same rule
as every other row-removing residual link (§2.4). Clearing it is also the conservative side —
`exact` only ever blocks a later link from lowering, never permits an unsound one — which matters
because the merge happens at range and not at instant.

#### Three rules the model derives rather than restates

- **`compile_line_filters`' `break`.** It does two things: it skips a non-pushable filter, and it
  `break`s at `LineFormat | Decolorize | Unpack` (`plan.rs:3067`). The first is the
  `is_pushable_line_filter` precondition above. The second is not a special case here — it is
  `body`'s provenance turning `Computed`. The documented *exception* falls out too: a filter after a
  **parser** still lowers, because *"parsers read but never rewrite the line"* (`plan.rs:3039`), so
  `body` stays `Stored`.
- **`has_unpushed_dropping_stage` is `!exact` on a `Lines` shape.** That function
  (`plan.rs:1655`) decides `fetch_until_limit` (`plan.rs:1625`), and it returns `true` for exactly
  the links this table clears `exact` on — a label filter, a line filter after a line rewrite, a
  non-pushable line filter — and `false` for parsers and `label_format`, which its own doc comment
  calls non-dropping because *"a parse failure keeps the line with an `__error__` label; fan-out
  only regroups"* (`plan.rs:1648-1654`). The oversample is not a separate concept: it is what the
  `Limit` link does when `exact` is false.
- **`metric_pipeline_construct`'s first refusal** (`plan.rs:1680`) is the index of the first
  `Pipe` link this table marks residual under the capability set that ships today, and its
  `&'static str` is that link's `BlockReason`.

Each of the three is a test, not a claim — §11 nominates one per walk, so that the number quoted in
§9.6 is not the only thing checked.

### 7.2 Groups 1, 2 and 3

- **Group 1 (cannot be lowered):** §5's final paragraph states the candidate class and its
  open question.
- **Group 2 (could be, has not been):** the stage list `metric_pipeline_construct` (`plan.rs:1680`)
  already enumerates as blocking — `json`, `logfmt`, `regexp`, `pattern`, label filter,
  `line_format`, `label_format`, `unwrap`, `unpack`, `decolorize`, `drop`, `keep`, and the `ip()`
  line filter. **Which of those are lowerable, in what SQL, and what each saves is #507's
  inventory.** This section is where it lands.
- **Group 3 (lowered already, prunes nothing):** LogQL's line filters lower to `LIKE`/`match`
  predicates backed by the body skip indexes ([architecture.md §5.3](architecture.md)). Whether a
  given filter shape actually prunes granules is a measurement, and it is #507's.

---

## 8. Where the result-size limit lives

When the boundary moves per query, the enforcement point moves with it. That cannot be per-stage
special-casing — and it does not have to be, because **the cap is a property of the boundary's
output kind**, and each kind has exactly one bound:

**The cap is keyed on the SQL part, not on the request.** Each `Part::Sql` picks its cap from its
own `yields` (§2.7.1), and a part whose `issue` is `PerSeed` applies that cap **per issue** against
a cumulative request-scoped budget — which is exactly what `HYDRATION_BYTE_BUDGET`
(`crates/pulsus-read/src/traces/exec.rs:145`) and `reader.logql_scan_budget_bytes` already do across
today's loops. A plan with three SQL parts therefore has three enforcement points, not one, and
saying which is which is the whole reason the cap table is keyed this way.

| `SqlPart::yields` | what crosses | the cap that applies |
|---|---|---|
| `Candidates` | up to `reader.traceql_max_candidates` keys | the candidate cap — today's mechanism, unchanged |
| `Exact` | hydrated rows | `max_result_bytes` (`crates/pulsus-read/src/traces/exec.rs:152-160`) plus the `HYDRATION_BYTE_BUDGET` retention counter (`exec.rs:145`) — today's mechanism, unchanged |
| `Reduced` | at most `limit` rows | **nothing bounds the grouping that produced them** — the gap |

So the placement question is answered once, structurally. The gap needs `max_rows_to_group_by`
with `group_by_overflow_mode = 'throw'`, mapped to the existing 422 taxonomy; the metrics path
already carries the analogous `max_rows_in_set`/`max_bytes_in_set` pair with
`set_overflow_mode = 'throw'` (`TRACE_METRICS_MAX_SET_ROWS`, `exec.rs:197`), so both the mechanism
and the error class exist.

This also corrects a natural misreading: `max_result_bytes` is **not** "unreachable on the lowered
path". It is not the cap for the `Reduced` kind, and it still applies unchanged for `Exact` and
`Candidates`.

**What a breach does is a separate question from where it is detected**, and both languages
already have an incompleteness channel, so the option space is wider than "refuse or lie":

- the traces search route signals truncation with `metrics.completedJobs < metrics.totalJobs`
  (`search_response.rs:504`, and [api.md §4.2](api.md));
- the LogQL streams route signals byte-budget truncation with `stats.pulsus_partial`
  ([api.md §2.1](api.md)).

[schemas.md §4.2](schemas.md) still describes the retired `partial` response flag and is stale on
this point.

---

## 9. The measurements

### 9.1 Corpora, and what moves with what

**C1 — the corpus every figure in this document is taken on unless stated.** 10,000,000 spans /
1,000,000 traces / 10 spans each / 5 whole days / 50 services, 45 of them
`service.namespace = "prod"` so 900,000 traces match / 2,500 span names including the empty
string, a single character, `a/b/c`, `{braced}`, `café-op` and `GETTING_STARTED` / 5 attribute
rows per span, so 50,000,000 `trace_attrs_idx` rows. Exactly 1,000 traces carry one 2.001 s span,
200 on each of the five days; every other span is 1.000001–2.0 ms. Generated by
`INSERT … SELECT FROM numbers_mt()` against the schema `catalog.rs` migrations 16 and 17 render,
plus the additive `status_message` and `scope_name`/`scope_version` ALTERs. **`payload` is
empty**, so bytes read off the file system are not comparable with C2's; granules, rows, result
bytes and peak memory are. After `OPTIMIZE … FINAL`: `trace_spans` 5 parts / 1,230 granules / Wide;
`trace_attrs_idx` 5 parts / 6,110 granules / Wide.

**C1 is built by a committed generator as of part 8, and that is what makes §9.2 reproducible.**
`cargo xtask bench traces-lowering` (`xtask/src/bench/traces_lowering.rs`) writes every row of it
from `numbers_mt()` over an index, with no PRNG, so the corpus is a function of the constants this
section states and nothing else. After its own `OPTIMIZE … FINAL` it reports **5 parts / 1,230
granules** for `trace_spans` and **5 parts / 6,110 granules** for `trace_attrs_idx` — the same
shape recorded above from the earlier, uncommitted build, which is a corroboration rather than a
restatement: the two builds were made from different code four months apart. The reproduction
command is in [the M4 traces read-path report](benchmarks/m4-traces-read-path.md).

**C2 — the corpus of [#478](https://github.com/digitalis-io/pulsusdb/issues/478).** The same
shape at the same scale, but pushed as OTLP/JSON through the product ingest path with real
payloads, on both PulsusDB and the pinned reference. Used here only where a figure is attributed
to it.

**C3 — the four-span behavioural corpus of §10.** One trace, four spans, three named `a` and one
named `b`, under one resource `service.name`, no span attributes at all. Pushed as identical
OTLP/JSON to both PulsusDB and the pinned reference (`grafana/tempo` v3.0.2, revision
`0c4b926d0`, confirmed from the running container's `/status/version`). It exists to make two
pipeline spellings disagree if order is honoured, and it uses **physical columns only** so that
the answer cannot depend on attribute storage. Nothing about scale is claimed from it.

**On the binary used for the PulsusDB side of C3.** It was built from commit `0677f40`, not from
`7f6de8e`. That is stated rather than glossed because it is exactly the kind of gap that produces
a confident wrong result. The two commits differ in 23 files; the four that decide this
measurement — `traces/search_plan.rs`, `traces/search_eval.rs`, `traces_api/search_response.rs`
and the `pulsus-traceql` AST and validator — are **byte-identical** between them
(`git diff 7f6de8e 0677f40 --` over those paths is empty). Of the files that do differ,
`traces/exec.rs` differs only inside the tag-values path (`TagValue`, `list_tag_values`), which
`/api/traces/v1/search` never calls, and `pulsus-schema/src/catalog.rs` differs only by an ALTER
on `trace_attrs_idx`, which a physical-column query never reads. A build from `7f6de8e` was not
made because a code review was compiling against the shared cargo home at the time and a
competing build would have failed it for machine reasons.

**The cost model, stated because every ratio in this document counts by it, and it is a premise
rather than a measurement.** Bytes are counted **per hop**. The `pulsus-server` ↔ ClickHouse hop and
the client hop cross a zone boundary in a multi-zone deployment and are **metered**; the
parts ↔ ClickHouse hop is local storage and is not. Compute is a fixed cost and bandwidth is not, so
spending CPU or database memory to move fewer bytes across a metered hop is the direction this
design takes. Nothing here is measured — it is the assumption under which the measurements below
are read, and both diagrams label their hops by it.

**What moves with storage layout.** Both layouts were constructed from identical rows — same DDL,
same projection, same skip index, differing only in `min_bytes_for_wide_part` — and measured:

| quantity | Wide | Compact | moves with layout? |
|---|---|---|---|
| granules selected (32-id batch) | 30 | 30 | **no** |
| rows read | 245,760 | 245,760 | **no** |
| decoded bytes | 8,293,268 | 14,604,381 | **yes, +76%** |
| bytes off the file system | 4,652,336 | 4,094,796 | **yes, −12%** |
| result bytes | 119,081 | 119,081 | **no** |
| peak query memory | 20,605,633 | 20,606,177 | **no** |

Every layout-sensitive figure in this document is marked **†**. Under a cost model that counts
bytes crossing a process boundary, layout is neutral — which is a finding, not an absence of one:
the design does not have to choose a layout.

### 9.2 The worked query, per stage

**Every figure in this section and in §9.2b is read out of
[`docs/benchmarks/data/traces-lowering-92.json`](benchmarks/data/traces-lowering-92.json), which
holds one `system.query_log` row per statement — 1,132 rows, one per `query_id`, no summaries.**
The corpus, the two forms and the retained rows are produced by
`cargo xtask bench traces-lowering` (`xtask/src/bench/traces_lowering.rs`); the reproduction
command is in [the M4 traces read-path report](benchmarks/m4-traces-read-path.md). Before this
re-measurement the section's figures rested on a run whose corpus was committed nowhere and whose
rows had been discarded, so nothing in this repository could re-derive one of them.
`every_figure_section_9_2_states_is_the_one_the_artefact_holds`
(`crates/pulsus-read/tests/query_lowering_doc_gate.rs`) totals the artefact's rows and compares
every cell below against its total.

Corpus C1, one request, driven serially exactly as the phase-2 loop (`exec.rs:1973`) and its three
serial reads (`exec.rs:2204`, `2054`, `2079`) drive it. Byte columns are raw `system.query_log`
byte counts; `decoded †` is `read_bytes` and `off file system †` is
`ProfileEvents['ReadBufferFromFileDescriptorReadBytes']`. The `granules (avg)` column is the mean
`ProfileEvents['SelectedMarks']` per statement, with the observed range beside it; its **total**
row is the sum over every statement, not a mean.

| stage | queries | rows read | decoded † | off file system † | granules (avg) | result bytes |
|---|---|---|---|---|---|---|
| phase-1 generator | 1 | 9,052,160 | 307,228,160 | 78,482,522 | 1,105 (min 1,105, max 1,105) | 4,767,944 |
| phase-2 hydration | 563 | 689,769,042 | 17,546,569,141 | 6,270,799,741 | 149.9 (min 129, max 161) | 68,260,769 |
| phase-2 membership | 563 | 5,096,366,080 | 91,782,941,344 | 11,888,515,258 | 1,105 (min 1,105, max 1,105) | 4,485,300 |
| winners' root read | 1 | 753,664 | 13,345,772 | 8,886,238 | 92 (min 92, max 92) | 58,008 |
| **total** | **1,128** | **5,795,940,946** | **109,650,084,417** | **18,246,683,759** | **707,689** | **77,572,021** |

The two byte totals rendered for reading: **102.12 GiB** decoded and **16.99 GiB** off the file
system.

### 9.2b The lowered request, per stage

The same request with the spanset aggregate compiled into the phase-1 generator's `HAVING` (issue
[#492](https://github.com/digitalis-io/pulsusdb/issues/492) part 4). One plan, two statements for
phase 1: `generator_sqls[0]` carries the `HAVING`, and `SearchPlan::generator_fallback_sql` is the
byte-identical statement without it. The table above is driven from the second, this one from the
first; **the hydration, membership and root statements are the same builders in both**, so the
whole difference between the two tables follows from how many candidates phase 1 returns.

| stage | queries | rows read | decoded † | off file system † | granules (avg) | result bytes |
|---|---|---|---|---|---|---|
| lowered generator | 1 | 9,052,160 | 379,317,760 | 83,844,774 | 1,105 (min 1,105, max 1,105) | 24,584 |
| lowered hydration | 1 | 1,130,496 | 28,818,128 | 15,084,501 | 138 (min 138, max 138) | 114,074 |
| lowered membership | 1 | 9,052,160 | 164,678,336 | 24,111,699 | 1,105 (min 1,105, max 1,105) | 16,320 |
| winners' root read | 1 | 753,664 | 13,345,772 | 8,886,238 | 92 (min 92, max 92) | 58,008 |
| **total** | **4** | **19,988,480** | **586,159,996** | **131,927,212** | **2,440** | **212,986** |

The two byte totals rendered for reading: **559.01 MiB** decoded and **125.82 MiB** off the file
system.

**The lowered generator is the more expensive statement, and that is the point.** It reads the same
9,052,160 rows and selects the same 1,105 granules as the unlowered one, but it also reads
`duration_ns` to evaluate the `HAVING`, so it decodes 379,317,760 bytes against 307,228,160 and
peaks at 190,817,746 B of database memory against the unlowered generator's 167,629,277 B:
**1.14×** = 190,817,746 / 167,629,277. Paying that on one statement is what removes 1,124 others.
Every ratio this section prints is written that way — the value, then the division it comes from —
so a reader can check it without leaving the paragraph, and
`every_ratio_in_section_9_2_is_the_quotient_of_two_printed_figures` checks every one of them.

**Both memory figures are phase-1 generators', and an earlier revision said otherwise.** It
published `169,311,055 B` as *"the loop's maximum"*. It is not a loop figure at all. Over the same
request there are two different maxima and this revision names which is which:

| quantity | scope | this measurement |
|---|---|---|
| the unlowered phase-1 generator's peak | one statement, `max(memory_usage)` over the single `(current, generator)` row | **167,629,277 B** |
| the phase-2 loop's peak | 1,126 statements, `max(memory_usage)` over every `(current, hydration)` and `(current, membership)` row | **26,084,032 B** |
| the lowered phase-1 generator's peak | one statement, `max(memory_usage)` over the single `(lowered, generator)` row | **190,817,746 B** |

The loop's real peak is **6.4 times smaller** than the figure that was labelled as its maximum, and
the number of that size belongs to the generator. **Nothing but the retained rows could have shown
this**: a total, or a single published maximum, looks the same whichever subset it was taken over.
That is the argument for committing one row per statement rather than a summary, and it is why
`memory_usage` is the one quantity in this section with no accumulator — it is a maximum over a
*named* subset, never a total, and the check that reads this artefact refuses to sum it.

**Where the round trips go.** The round-trip count is `1 + 2·ceil(k/32) + 1` where `k` is the index
of the `limit`-th qualifying candidate in `bound_ts DESC` order. On C1 as this harness builds it a
trace qualifies when it carries the one 2.001 s span, which is one trace in a thousand, and 45 of
the 50 services are `prod`, so the 20th qualifying candidate is at `k = 18,000`; `ceil(18000/32)`
is 563 and the count is 1,128. Lowered, the generator returns only qualifying candidates, so `k` is
the request's `limit` of 20, one batch serves it and the count is 4.

| | round trips | rows read | granules | result bytes |
|---|---|---|---|---|
| today | 1,128 | 5,795,940,946 | 707,689 | 77,572,021 |
| lowered | **4** | **19,988,480** | **2,440** | **212,986** |
| ratio | **282×** | **290×** | **290×** | **364×** |

Both rows count the **whole request**, the winners' root read included, so the ratios compare like
with like. `every_ratio_in_section_9_2_is_the_quotient_of_two_printed_figures` divides the two rows
above and refuses any printed ratio that is not within half of its last printed place.

**§9.7 is a second table of this kind and it is not this one.** It measures a *refused* push — the
per-span pre-grouping four group-2 selector classes would need — on a different corpus, with the
instrument settings stated beside every figure. Anyone re-taking this section should read §9.7
first, so part 6's numbers are found rather than taken again.

**The root read is why the lowered side is 4 and not 1.** It is not an artefact of the measurement:
`Emit` is `Never` (§3.1) because the root summary is trace-wide and unwindowed, so no chain removes
it; and the window-bounded hydration read and the membership read survive lowering because
`spanSets[].matched` and `spanSets[].spans[]` are written unconditionally. The earlier revision of
this section published a lowered total of `43,636 B` computed from **two** statements — the
generator and the root read — and every ratio derived from it. That figure and its ratios are
superseded by the §9.2 re-measurement: the four rows of §9.2b are what a lowered request costs,
measured, and the `354×` above is the saving.

**Our own process does not grow while it holds the matched set.** `pulsus-server` resident memory
stayed at 383 MiB, unchanged from idle, measured on corpus C2. The cost of the two-phase loop is on
the metered hop and in the database, not in our heap — which is why every ratio above is counted in
bytes and round trips rather than in memory. Carried over from the hops diagram, where it was the
only figure this document did not also state; **not re-measured in this revision**, because C2 was
not rebuilt.

**The membership read is the dominant term, it is batch-independent, and this is now stated over
every batch rather than most of them.** All **563** membership reads selected exactly 1,105
granules and read exactly 9,052,160 rows — the artefact holds all 563 rows and they agree to the
byte, so the per-read unit and the phase total are two readings of the same evidence rather than
one number and a multiplication. The earlier revision could only say "553 of the 554", and had to
flag its own phase total as *not independent evidence*, because the run's rows had not been kept.
Retaining one row per statement is the whole reason that flag is gone.

The read is batch-independent because asking about a different 32 candidates changes nothing:
`trace_id` is the fifth key column of `trace_attrs_idx` and the fourth, `timestamp_ns`, is left as
the whole request window, so every batch scans the same `(key, val)` prefix. Narrowing that
predicate to the batch's own span range collapses the same read to a handful of granules with an
identical answer. That is a separate optimisation from anything in this document and it does not
need the lowering core.

**What is reproducible here, and what is not — and the two questions are different.** Re-running
the harness against the SAME corpus is one question; rebuilding the corpus and re-running is
another, and only the first was measured when this section was first written.

**The block below is generated from
[`docs/benchmarks/data/traces-lowering-92-rebuilds.tsv`](benchmarks/data/traces-lowering-92-rebuilds.tsv)**
— the table and the sentences alike — and
`the_rebuild_block_is_the_one_the_dataset_produces` compares it byte for byte. Two earlier
revisions of this paragraph stated these figures in prose and both carried one that was wrong;
gating the table alone was not enough, because the sentences beside a table are where the next
wrong number goes. There is nothing here for a person to write a number into.

<!-- generated from traces-lowering-92-rebuilds.tsv -->

Each cell reads *statements moved, of 1,132* / *largest per-statement change*.

| column | S | A | C |
|---|---|---|---|
| `selected_marks` | 0 / — | 0 / — | 1 / 0.71% |
| `result_bytes` | 0 / — | 0 / — | 0 / — |
| `read_rows` | 0 / — | 147 / 0.013% | 147 / 0.689% |
| `read_bytes` | 0 / — | 147 / 0.013% | 147 / 0.647% |
| `read_compressed_bytes` | 440 / 0.19% | 977 / 0.013% | 996 / 0.234% |
| `fd_read_bytes` | 561 / 2.53% | 1124 / 2.5% | 1125 / 3.15% |
| `memory_usage` | 982 / 25.8% | 1132 / 18.6% | 1105 / 19.9% |

**S** is the same corpus, run twice by this harness.

**A** is a corpus rebuilt from the committed generator, measured by this harness.

**C** is a corpus rebuilt on another host by issue #492 part 8's second code review; not re-measured here.

**B** is a corpus rebuilt on another host by issue #492 part 8's first code review, which reported group totals only, so it appears in no column above: `selected_marks` +1 granule, hydration group; `result_bytes` +2,323 bytes, hydration group.

Changes over the whole unlowered request, where an observation published one: S `fd_read_bytes` 0.03%; A `read_rows` −418 rows, −0.00001%; A `read_bytes` +11,418 bytes, +0.00001%. Every other cell above was published per statement only.

On the same corpus, 4 of the 7 columns did not move on a single statement: `selected_marks`, `result_bytes`, `read_rows`, `read_bytes`.

Rebuild C differs from rebuild A on 6 of the 7 columns; the one it does not differ on is `result_bytes`.

Of the 7 columns, **none** is one every rebuild that recorded it found unmoved.

An earlier revision of this section said a rebuild is expected to land within rebuild A's figures. That expectation was written before rebuild C, and rebuild C did not meet it. 4 observations exist now — A, B, C, S — and no band is established across them: what they establish is that these columns vary, not by how much. A re-runner should expect their numbers to differ from the committed artefact without reading the difference as a defect.

<!-- end generated -->

> **These two sections were reconstructed, and the reconstruction cannot be verified.** While part 8
> was being reviewed, `git checkout` was used to revert a break with two document rewrites
> uncommitted, and both were destroyed — this reproducibility block and §12.3. They were rewritten
> from what their author remembered writing. **No blob of the destroyed text was retained**, so
> nobody can compare the reconstruction against it: a reader can check the current text for internal
> consistency and against the datasets, and that is all. A code review checking it for internal
> consistency is what found that one paragraph said every rebuild agreed on a column another
> paragraph said one rebuild had moved. **A reconstruction that cannot be verified is a different
> thing from one that has been**, and this note exists so a later reader does not mistake the second
> for the first. Both blocks are generated from committed datasets now, which is a guarantee about
> the present text and says nothing about what the destroyed text contained.

**The mechanism is adaptive granularity.** `index_granularity_bytes` is 10 MiB on these tables, so a
granule holds as many rows as fit in that many bytes rather than a fixed 8,192. The corpus is
anchored to the day it is built, so `timestamp_ns` carries different absolute values between builds;
under `CODEC(DoubleDelta, ZSTD)` those compress differently, the byte size of a row shifts, and a
granule boundary moves. A moved boundary changes how many rows a `trace_id IN (…)` read touches, and
that is what the row and byte columns above are showing.

**So §9.2's figures are gated against the committed artefact and against nothing else.**
`every_figure_section_9_2_states_is_the_one_the_artefact_holds` compares the document with
`docs/benchmarks/data/traces-lowering-92.json`, which is one measurement kept row by row. A rebuild
is a second measurement of the same design on a different corpus build, and the two are not required
to agree.

Both byte counters are recorded in the artefact for every statement and neither is inferred from the
other; the memory figures §9.2b prints are one run's, which is why they carry no accumulator and no
ratio gate beyond the one printed beside them.

### 9.3 The correctness consequence, measured

`{ span.http.method = "GET" } | max(duration) > 1s` on C1: **333 traces qualify** under the
matched-span scope the reference and our evaluator both use, and **1,000** under a whole-trace
scope. The top trace's sort key differs too — `…044006000` against `…044009000` — so a wrong
lowering gets both the set and the order wrong.

### 9.4 The primary-key pruning rule

Uniform-random `trace_id` values over C1, `trace_spans` at 5 parts and 1,230 granules:

| N ids | 1 | 2 | 4 | 8 | 16 | 32 | 64 | 128 | 256 | 1024 | 4096 |
|---|---|---|---|---|---|---|---|---|---|---|---|
| granules selected | 5 | 10 | 20 | 40 | 79 | **155** | 296 | 545 | 816 | 1207 | 1225 |
| rule `G·(1−(1−P/G)^N)` | 5 | 10 | 20 | 40 | 80 | 151 | 297 | 502 | 799 | 1211 | 1230 |

**Granules ≈ `G·(1 − (1 − P/G)^N)`**, where `G` is the granules in the window and `P` the number of
parts. Within 9% at every point, exact at both ends. Two consequences decide emitted shapes:

- **A single point lookup costs `P` granules, not 1.** A trace's spans live in one partition, but
  the primary key carries no partition information, so one granule per part is selected.
- **Pruning saturates at `N ≈ G/P`.** Here `G/P = 246`, and at `N = 256` the query already reads
  66% of the window. At the emitted `BATCH_TRACES = 32` a batch reads 12.6% of the window, 563
  times — the loop reads the table 69x over.

`EXPLAIN indexes = 1` names the mechanism and the count together for the emitted 32-id batch:
`Granules: 149/1225`, `Ranges: 119`, `Search Algorithm: generic exclusion search`. An `IN`-set on
the leading key column does not get binary search. The `service_time` projection, by contrast,
does: `Granules: 25/1225`, `Search Algorithm: binary search`, 204,800 rows read for 200,000
matching.

**An `EXPLAIN` showing an index selected is not evidence that it pruned.** Every claim in this
section quotes granule counts, not index names.

### 9.5 The instrument, and the traps that decide a figure

Every figure is one `system.query_log` row per `query_id`. The instrument was validated in states
where it must fail: a `query_id` that does not exist returns no row rather than a plausible one,
and a query with no key predicate reports 0 granules while a narrowed one reports 5, so it
discriminates rather than always printing a small number.

**ClickHouse 26.3 — the version CI's `schema-it` job runs — enables `use_query_condition_cache` by
default.** It remembers which granules a condition matched, so `SelectedMarks` for one query
depends on what ran before it. A first pass of these measurements reported **1 granule on a warm
cache and 1,105 on a cold one, for the same SQL text and the same 32 candidate ids.** Every figure
here was re-taken with the cache disabled. **Any measurement of granule counts must set
`use_query_condition_cache = 0`, or it will report the wrong number** — including any that a later
wave asserts on.

A second trap, also measured: a `LIMIT` with no `ORDER BY` is cancelled on early termination and
writes **no `QueryFinish` row at all**. A measurement reading `query_log` must fail on a missing row,
never treat it as zero cost.

**A third trap, and it is the one that decided an architectural question.** Our reader sends
`max_block_size = 4096` on every search statement — `TRACE_SEARCH_MAX_BLOCK_ROWS: u64 = 4096`
(`crates/pulsus-read/src/traces/exec.rs:173`), set in `search_settings` (`:2836`) and inherited by
`generator_settings` (`:2869`). ClickHouse 26.3.29.7's own default is **65,409**
(`SELECT value, default FROM system.settings WHERE name = 'max_block_size'` prints `65409 65409`).
A measurement taken at the server default is a measurement of a system we do not run, and the
setting moves two different figures in opposite directions:

```
peak query memory, ONE statement (issue #492 part 6's pushed pre-grouping on a span-ordered
attribute index, full 10,000,000-span window, optimize_aggregation_in_order = 1)

  0                          512 MiB ceiling                            1200 MiB
  |----------------------------------|----------------------------------------|
              228.7 MiB                              1,068.3 MiB
        max_block_size = 4096                   max_block_size = 65,409
        (what our reader sends)                 (ClickHouse's default)
             COMPLETES                            REFUSED, Code 241

result_bytes, ONE generator statement -- same SQL, same rows read, same granules, only the
LIMIT moved. Condition cache dropped before every run; TSV output; ClickHouse 26.3.29.7.

  rows returned      at 4,096     at 65,409     4,096 higher by
        4,096         196,616       196,616             0     0%
        4,097         197,136       196,616           520     0.26%
        8,192         393,232       393,224             8     0.002%
       12,288         589,848       393,224       196,624    33.3%
       16,384         786,464       786,440            24     0.003%
       20,000         884,776       786,440        98,336    11.1%
       32,768       1,572,928     1,572,872            56     0.004%
       50,000       2,383,976     1,572,872       811,104    34.0%
       65,409       3,047,552     1,572,872     1,474,680    48.4%
       65,410       3,047,552     1,573,392     1,474,160    48.4%
      100,001       4,767,944     3,145,744     1,622,200    34.0%
```

**The gap is a function of the result size, and it is not monotonic.** At 65,409 the figure only
moves at the doublings -- 196,616, 393,224, 786,440, 1,572,872, 3,145,744 -- while at 4,096 it
climbs roughly 48 bytes per row, so the distance between the two settings depends on where the
result size sits between two doublings: **0.002% just above one (8,192 rows), 48.4% just below the
next (65,409 rows)**. The phase-1 generator returns 100,001 rows, where the gap is **34.0%**; at
4,097 rows, the first result above one block, it is **520 bytes, 0.26%**. So "the default reads
about a third low" is a statement about the 100,001-row result and about no other result size, and
this table, not that sentence, is what a re-take should be compared against.

So a re-take at the default **refuses a statement the shipped reader would run**, and it moves the
metered column by anywhere between 0% and 48% on the same statement. Two competent
measurements of §9.7's headline figure landed a factor of 4.7 apart for exactly this reason, and
neither was wrong about what it measured. `search_settings_pin_the_layer_1_budget_contract`
(`crates/pulsus-read/src/traces/exec.rs:5254`) is what keeps 4,096 shipped: it asserts that the
rendered search settings contain the substring `max_block_size` and the substring `4096` — as two
independent substring checks, not bound to each other, so it would not catch a different value
arriving beside a stray `4096`.

**A fourth trap, and it is inside the method the third one invites.** The obvious way to establish
the block size a figure was taken at is to read it back out of the query log. That method is blind
in exactly one direction. `system.query_log` records only the settings a statement **changed**, so
`Settings['max_block_size']` is **empty** for a statement submitted at 65,409 — and equally empty
for a statement that sent no block size at all. Reading the log therefore cannot tell *"65,409 was
sent"* from *"nothing was sent"*, which is the very pair the third trap turns on. Measured on
ClickHouse 26.3.29.7, one statement per case:

```
$ curl -sS "$B?query_id=blk_4096&max_block_size=4096"   --data-binary "SELECT count() FROM numbers(100000)"
$ curl -sS "$B?query_id=blk_65409&max_block_size=65409" --data-binary "SELECT count() FROM numbers(100000)"
$ curl -sS "$B?query_id=blk_absent"                     --data-binary "SELECT count() FROM numbers(100000)"
$ curl -sS "$B" --data-binary "SYSTEM FLUSH LOGS"
$ curl -sS "$B" --data-binary "SELECT query_id, Settings['max_block_size'] AS logged,
    has(mapKeys(Settings),'max_block_size') AS present FROM system.query_log
    WHERE query_id LIKE 'blk_%' AND type = 'QueryFinish' ORDER BY query_id FORMAT TSVWithNames"

query_id      logged  present
blk_4096      4096    1
blk_65409             0
blk_absent            0
```

The submitted value is not hiding in another column either: `SELECT * FROM system.query_log WHERE
query_id = 'blk_65409' AND type = 'QueryFinish' FORMAT Vertical` matches `65409` on `query_id` and
`initial_query_id` and on nothing else, and those two carry it only because the id was named after
the setting.

**So the instrument is established from what was submitted, and the query log confirms only the
non-default case.** Pair each `query_id` with the `max_block_size` the statement was sent with —
the URL parameter, or the constant the reader binary sends — and use `Settings['max_block_size']`
as a *confirmation* where the value is non-default. Every 4,096 figure in §9.7 is confirmed that
way, and it is a real confirmation: 4,096 is not the default, so an empty cell there would mean the
setting never arrived. Nothing at 65,409 can be confirmed that way; the curve above is established
from the parameter each statement was sent with, alongside `SELECT value, default FROM
system.settings WHERE name = 'max_block_size'`, which prints `65409 65409`. **An empty
`Settings['max_block_size']` is not evidence that the default was used** — it is the absence of
evidence either way.

**No wall-clock figure in this document carries a claim.** The machine carried other work
throughout and its load average moved from 3.42 to 34.24; two readings of the same query forty
minutes apart differed 5.6x with identical counters. Timing for the worked query, taken on C2 at
load ≤ 1.41, is on [#478](https://github.com/digitalis-io/pulsusdb/issues/478).

---

### 9.6 The stopping rule: what the first version of this document got wrong

The fold originally returned at the first refusal. Measured on **#507**'s LogQL corpus, against the
shipped `compile_line_filters` (`crates/pulsus-read/src/logql/plan.rs:3052`) transcribed as the
oracle, over every chain of length 3 built from 15 concrete LogQL atoms parsed with our real
parser — an equality on the **ordered** list of pushed predicates, not a count and not a
containment:

**The scope of these three numbers, and it is narrower than it looks.** The oracle is a
**transcription** of `compile_line_filters`, not the shipped function and not emitted SQL, so the
comparison is over **which line-filter values are pushed and in what order** — 15 atoms, chains of
length 3, 3,375 ordered triples. A `0` therefore means the two walks select the same filters in the
same order; it says nothing about the operator each one renders, the escaping, the rest of the
statement, the other two hand-written walks, or any stage outside the atom set. It is a real
result — the negative controls below show it is not vacuous — and it is one projection of one
walk. §11.2 nominates the **wave-1**
`logql::plan::tests::the_model_reproduces_compile_line_filters_ordered_predicate_list`, which
replaces the transcription with the function itself, and two more that cover the other two walks;
none of the three exists at base — each selector prints `Starting 0 tests` and exits 4.

| model | chains | chains where the oracle pushes ≥ 1 | mismatches |
|---|---|---|---|
| return at the first refusal (this document's first version) | 3,375 | 1,827 | **715** |
| skip only a non-lowerable *line filter*, else return | 3,375 | 1,827 | **463** |
| every link that cannot lower becomes residual, still applies its state effect, and the fold continues | 3,375 | 1,827 | **0** |

And the cost is not theoretical. `{service_name="checkout"} |= "CONN_REFUSED" |= ip("10.0.0.1") |= "pod-044"`
— three line filters, the middle one not pushable — on a corpus where the two literal filters
together match 400 of 2,000,000 rows in the key range and the first alone matches 40,000:

| | round trips | rows to the server | metered bytes | rows read |
|---|---|---|---|---|
| what ships today | **1** | 400 | **169,707** | 204,800 |
| the first version's fold, stopping at the `ip()` link | **10** | 10,000 | **3,501,588** | 24,133,632 |

**20.6× more bytes on the metered hop, 10× the round trips, 118× the rows read — a regression
produced by an architecture whose entire purpose is to move fewer bytes.** An ordinary
three-filter query, not a constructed one.

The corrected model is §2.5. It was validated by breaking it twice and watching the enumeration go
red: dropping the `or_matches` half of the pushability test gave 511 mismatches, and forgetting
that `decolorize` rewrites the line gave 171; restoring gave 0.

**Attribution.** This finding is #507's, obtained by transcribing this document's §2 interface
verbatim into a throwaway crate and writing the LogQL link set against it. It is the reason §10
now records the two-language fit as **disproved** rather than unproven.

### 9.7 The four group-2 selector classes, measured — and why none of them was lowered

Issue #492 part 6 measured the four §3.3 rows that had never been measured: the two field-vs-field
comparisons, the cross-field arithmetic form, and the event-set comparison. It built the SQL each
one would need, ran it beside the path that ships today, and **lowered none of them**. This section
is that record. Nothing in it is asserted by a test and nothing in it runs in CI — the corpus is
71,000,000 rows on a developer instance and was dropped when the measurements were finished — so
**the recipe below, not any single figure, is the deliverable**.

The one thing that did move into code is a correction: `{ name != "x" }` was in §3.3 and is now in
§3.4, because it is lowered already and prunes nothing.

#### The instrument, before any figure

Every number in this section is one `system.query_log` row per `query_id`, on **ClickHouse
26.3.29.7**. Read §9.5's four traps first — the fourth is about how the block size on each
caption below was established, and it is why those captions say what was **submitted**. The settings are part of each claim, so they are named
beside the figures they govern rather than once here, and this list is the index:

| setting | value used | why it decides a figure here |
|---|---|---|
| `max_block_size` | **4096** | the shipped value (`exec.rs:173`). At ClickHouse's own default, 65,409, the same statement peaks at **1,068.3 MiB** instead of **228.7 MiB** — across the 512 MiB ceiling — and the same statement's `result_bytes` moves by between 0% and 48% depending on the result size (§9.5's curve). Every figure below names the block size it was taken at |
| `use_query_condition_cache` | **0**, or the cache dropped before each request | otherwise a repeat read reports an order of magnitude fewer rows (§9.5's first trap). Two routes, below |
| `optimize_aggregation_in_order` | **1**, named on the rows that need it | it is what lets the span-ordered index stream the aggregation instead of holding a hash table over every span-group. On the current index order it buys nothing, because `(trace_id, span_id)` is not a prefix of that sorting key |
| `max_memory_usage` | **536870912** | the shipped `reader.traceql_generator_max_memory_bytes` (`crates/pulsus-config/src/model.rs:518`), applied by `generator_settings` (`exec.rs:2869`) |
| `max_bytes_before_external_group_by` | **0** | shipped: the generator throws rather than spilling (`exec.rs:2869`) |
| `max_rows_to_read` | **50000000** shipped, **200000000** in the raised-budget rows | `reader.traceql_scan_budget_rows` (`model.rs:515`), carried with `read_overflow_mode = throw` by `search_settings` (`exec.rs:2830-2836`) |
| `min_bytes_for_wide_part` | **10485760** | pinned in the corpus recipe so the part format is reproducible; ClickHouse's own 26.3 default happens to be the same value, and neither trace `CREATE TABLE` pins it |

**The rule this section follows: every metered figure carries its instrument beside the number.**
Not a preamble saying "everything below is at 4,096" — this section had exactly that sentence and it
did not hold. Its first `{ .a = .c }` stage table was a **65,409** take sitting under a 4,096
heading, and two of its five rows are values that statement set produces only at the server default:
the phase-1 generator's `3,145,744` (**4,767,944** at 4,096) and the winners' root read's `92,352`
(**97,016** at 4,096). The wrong instrument is invisible in a number; it is visible only in a label,
so each table below states, in its own caption: the block size, the condition-cache state, whether a
memory ceiling was applied, and **who issued the statements** — our reader binary, or a hand-issued
statement. Where a figure was re-taken while writing this section, the re-take and its instrument
are printed beside the original rather than replacing it. **A caption's block size is what the
statement was sent with**, paired to its `query_id`; §9.5's fourth trap is why it cannot be
established the other way round.

**And one arithmetic check that catches this class without knowing anything about ClickHouse.** A
stage table decomposes a request, so **within one take** its rows must sum to that request's own
totals, in every column. The 4,096 stage table below sums to **123,448,989** metered bytes, to
**3,127** statements and to **3,162,449** granules, and all three are the `{ .a = .c }` row of the
request table. The 65,409 table it replaces summed to 119,881,011 bytes against the same request
row — a 3,567,978-byte disagreement between two tables on the same page, which nobody had added up.

**The same addition then caught a second thing, and it is not an instrument error.** The stage table
and the request table are takes on two *different builds* of C6, so setting them side by side is
also a reproduction test. Between those two takes, one column fails it: the stage table's rows-read
column sums to 25,904,811,940 against a request row of 25,904,824,756 printed a few lines above it.
Two further builds have been measured since, and all five takes are printed below. **Rows read and
metered bytes both move between builds** — rows read across a span of 122,674, metered bytes across
5,362,175 — while statements and granules are identical on all five. **So the equality claimed under the stage table, which is an equality
between two takes, is over statements and granules. Rows read and metered bytes are recorded there
rather than checked, and they are outside it because they were measured to be, not because
excluding them was convenient.**

`search_settings_pin_the_layer_1_budget_contract` (`crates/pulsus-read/src/traces/exec.rs:5254`)
is what keeps 4,096 shipped, and it is worth knowing exactly how much it keeps: it asserts that the
rendered search settings contain the substring `max_block_size` and the substring `4096`, as two
independent checks that are not bound to each other. It would not catch a different block size
arriving beside a stray `4096` elsewhere in the settings.

**What the instrument rule was applied to, and what it did not reach.** After the mixed stage table
was found, C6 was rebuilt from the five statements below and these were re-measured on one
instrument, all at `max_block_size = 4096` with the condition cache dropped:

| re-measured | outcome |
|---|---|
| the eight structural quantities and the six selectivities | exact |
| the five whole requests of the table below | metered bytes exact on four of five; `{ .a = .b }` 219 bytes high; statement and granule counts exact on all five. That was one re-take against one earlier build; a later build moved `{ .a = .c }`'s metered total by 5,362,175 bytes, so metered bytes is recorded and not checked (see the five-take note below the request table) |
| the `{ .a = .c }` stage decomposition | **two rows were 65,409 readings**; the table is replaced and now sums to its own request total |
| the `{ .a = .c }` push side | statements, rows read and granules exact; metered bytes a fourth take, outside the earlier three |
| the span-ordered copy's size and the group-3 pruning pair | exact — 451,383,963 bytes, 98,305 / 12 and 49,152 / 6 against 71,000,000 / 8,670 |
| the row-budget refusal | reproduces; its `current rows` figure does not, and is now labelled |

**Not re-measured, and still carrying the instrument recorded when they were taken:** the 30-run
peak-memory table, the 390,000 / 395,000 crossover bracket, the push side of the other four classes,
and the numeric-only `HAVING` control. An instrument label on those rows says what the take
recorded, not what a re-take confirmed, and the rows say which.

**Two routes to `use_query_condition_cache = 0`, and they are not interchangeable.** ClickHouse 26.3
ships the setting **on** (`system.settings` prints `value 1, default 1, changed 0` — it is not a
container configuration), and `ALTER USER default SETTINGS use_query_condition_cache = 0` is refused
with `Code: 495 … in users_xml because this storage is readonly`. So either

- issue every statement with `?use_query_condition_cache=0`, or, when the statements are issued by
  our own reader rather than by hand, connect it as a SQL-defined user:

      CREATE USER p6 IDENTIFIED WITH no_password
        SETTINGS use_query_condition_cache = 0 CHANGEABLE_IN_READONLY;
      GRANT CURRENT GRANTS ON *.* TO p6;   -- GRANT ALL is refused: `default` lacks GRANT OPTION

  then point `clickhouse.auth` at it and check each statement's logged `Settings` carries the `0`;

- or run `SYSTEM DROP QUERY CONDITION CACHE` before each request, which is what the last take used.
  The two are not the same measurement: dropping the cache before a request against carrying it over
  from the previous one moves the metered column by 0.75% on `{ .a * 2 < .c }` — **17,940,201**
  dropped against **17,806,317** carried.

**And one rule about instruments, learnt the expensive way here.** Two takes of the peak-memory
figure disagreed by 4.7x and each was internally consistent. They became comparable only when the
corpus was rebuilt **from the other side's own build scripts**, database name substituted and
nothing else changed: with a shared corpus the statement counts, rows read and granule counts
matched to the digit, the disagreement narrowed to one column and one setting, and the setting was
`max_block_size`. **When two measurements of the same quantity disagree, rebuild the corpus from
the other side's scripts and re-take both figures on one instrument before attributing the
difference to anything else.**

#### Corpus C6 — the recipe, which is the part that has to survive

C6 is 10,000,000 spans / 1,000,000 traces / 10 spans each / 432,000 s from `1700000000000000000`
across 6 dates / 50 services / 2,500 span names, with **7** attribute rows per span plus 1,000,000
event-intrinsic rows = **71,000,000** `trace_attrs_idx` rows. Keys: `service.namespace` (resource),
`http.method` (span), `a` (span, numeric), `b` (equal to `a` on 1 span in 100), `c` (equal to `a` on
1 span in 10,000), `s1` (span, text), `s2` (equal to `s1` on 1 span in 10,000), and `name` under
`scope = 'event:intrinsic'` on 1 span in 10, equal to `s1`'s value on 1 span in 50.

**The schema is exactly these five statements, in this order, and no other migration.** Not the
`_dist` twins (36/38/40), not `trace_tag_catalog` (18/41), not the `span_name_day` projection
(42/43).

| # | migration | what it is | `catalog.rs` |
|---|---|---|---|
| 1 | 16 | `CREATE TABLE … trace_spans` | `339-361` |
| 2 | 35 | `ALTER … ADD COLUMN IF NOT EXISTS status_message` | `742-745` |
| 3 | 37 | `ALTER … ADD COLUMN … scope_name, scope_version` | `779-783` |
| 4 | 17 | `CREATE TABLE … trace_attrs_idx` | `369-385` |
| 5 | 39 | `ALTER … ADD COLUMN IF NOT EXISTS val_type` | `816-819` |

The additive-`ALTER` order is the shipped build order, not a convenience: `catalog.rs:1657` asserts
`"status_message must arrive via the additive ALTER (id 35), not id 16's CREATE"` and `:1708` the
same for `scope_name`. A corpus with those columns written inline into the `CREATE` is **not the
schema we run**, and that ambiguity is why the statements are printed rather than described.

Each `CREATE` is transcribed with exactly three edits: `{{db}}` becomes the corpus database,
`{{on_cluster}}` becomes empty, the `TTL …` line is **deleted** (C6's timestamps are in 2023), and
the trailing `SETTINGS ttl_only_drop_parts = 1` becomes

    SETTINGS ttl_only_drop_parts = 1, min_bytes_for_wide_part = 10485760, min_rows_for_wide_part = 0

The two `ALTER`s are transcribed with no edit but `{{db}}` and `{{on_cluster}}`.

Spans — 10,000,000 rows:

```sql
INSERT INTO trace_spans SELECT
  reinterpretAsFixedString(toUInt64(intDiv(number,10))) || reinterpretAsFixedString(toUInt64(0)),
  reinterpretAsFixedString(toUInt64(number)),
  reinterpretAsFixedString(toUInt64(0)),
  concat('op-', toString(number % 2500)),
  concat('svc-', toString(intDiv(number,10) % 50)),
  toInt64(1700000000000000000 + intDiv(number,10) * 432000000 + (number % 10) * 1000000),
  toInt64(if(intDiv(number,10) % 1000 = 0 AND number % 10 = 0, 2001000000, 1000001 + (number % 1000000))),
  toInt8(number % 3), toInt8(1 + number % 5), toInt8(0), '', '', '', ''
FROM numbers_mt(10000000)
```

Attributes — one pass per key, each 10,000,000 rows, with `<KEY>`, `<VAL>`, `<SCOPE>` and `<TYPE>`
from the table below:

```sql
INSERT INTO trace_attrs_idx SELECT
  toDate(fromUnixTimestamp64Nano(toInt64(1700000000000000000 + intDiv(number,10) * 432000000 + (number % 10) * 1000000))),
  <KEY>, <VAL>, <SCOPE>, toFloat64OrNull(<VAL>),
  toInt64(1700000000000000000 + intDiv(number,10) * 432000000 + (number % 10) * 1000000),
  reinterpretAsFixedString(toUInt64(intDiv(number,10))) || reinterpretAsFixedString(toUInt64(0)),
  reinterpretAsFixedString(toUInt64(number)),
  toInt64(if(intDiv(number,10) % 1000 = 0 AND number % 10 = 0, 2001000000, 1000001 + (number % 1000000))),
  <TYPE>
FROM numbers_mt(10000000)
```

| `<KEY>` | `<SCOPE>` | `<VAL>` | `<TYPE>` |
|---|---|---|---|
| `'service.namespace'` | `'resource'` | `if(intDiv(number,10) % 50 < 45, 'prod', 'staging')` | `'string'` |
| `'http.method'` | `'span'` | `['GET','POST','PUT','DELETE','PATCH','HEAD'][1 + number % 6]` | `'string'` |
| `'a'` | `'span'` | `toString(number % 1000)` | `'int'` |
| `'b'` | `'span'` | `if(number % 100 = 0, toString(number % 1000), toString((number % 1000) + 1))` | `'int'` |
| `'c'` | `'span'` | `if(number % 10000 = 0, toString(number % 1000), toString((number % 1000) + 1))` | `'int'` |
| `'s1'` | `'span'` | `concat('t-', toString(number % 1000))` | `'string'` |
| `'s2'` | `'span'` | `if(number % 10000 = 0, concat('t-', toString(number % 1000)), concat('z-', toString(number % 1000)))` | `'string'` |

Event intrinsics — 1,000,000 rows, one per tenth span:

```sql
INSERT INTO trace_attrs_idx SELECT
  toDate(fromUnixTimestamp64Nano(toInt64(1700000000000000000 + number * 432000000))),
  'name',
  concat('t-', toString(if((number*10) % 50 = 0, (number*10) % 1000, ((number*10) % 1000) + 3))),
  'event:intrinsic', NULL,
  toInt64(1700000000000000000 + number * 432000000),
  reinterpretAsFixedString(toUInt64(number)) || reinterpretAsFixedString(toUInt64(0)),
  reinterpretAsFixedString(toUInt64(number*10)),
  toInt64(1000001), 'string'
FROM numbers_mt(1000000)
```

Then `OPTIMIZE TABLE … FINAL` on both tables.

**What a rebuild has to match: eight structural quantities and six selectivities.** These reproduced
exactly on every build that took them — **five** independent full builds of the recipe above, by
four different hands (two of the five are the same one), and the structural half again on the four
span-only repeat builds below.

```
trace_spans      6 parts   1,232 marks   10,000,000 rows   Wide
trace_attrs_idx  6 parts   8,676 marks   71,000,000 rows   Wide

.a = .b            100,000 spans   100,000 traces
.a = .c              1,000 spans     1,000 traces
.s1 = .s2            1,000 spans     1,000 traces
.a * 2 < .c          9,000 spans     9,000 traces
.s2 = event:name     1,000 spans     1,000 traces
name = 'op-0'        4,000 spans     4,000 traces
```

The awkward one is `.a * 2 < .c`, which is 9,000 and not 10,000: `a = n % 1000` and
`c = (n % 1000) + 1` except on the 1,000 spans where `n % 10000 = 0`, and there `c = a = 0`, so
`0 < 0` excludes them. A rebuild reporting 10,000 has transcribed the `c` generator wrongly. That is
why the selectivities are the check.

**The byte totals are recorded and are not a check.** Three builds of the identical pinned recipe,
compared column by column — same rows, same order, same granules, same part format, and an identical
content hash over all 14 columns (`4829404233030457425`) — gave three different `trace_spans` byte
totals, and six further builds gave six more, nine distinct values in all:

```
trace_spans bytes on disk -- recorded, not a check
build set               bytes on disk                                 spread within the set
three by one person     211,538,791 / 211,564,845 / 211,376,088             188,757
  (the content hash was measured on these three)
three by another        211,366,295 / 211,506,324 / 211,503,142             140,029
one at implementation   211,335,774
one at review           211,446,731
one at the correction   211,506,914
                            overall range 211,335,774 .. 211,564,845        229,071
```

Every column's compressed size moves between builds; the thread count is not pinned, and pinning it
would make it a different recipe from the one that produced these figures. `trace_attrs_idx` came
out at 1,128,726,045 on four builds, 1,128,725,599 on one and 1,128,726,121 on one — 522 bytes
apart at the widest. **A rebuild that differs from these figures by tens of kilobytes has
reproduced**; the eight structural quantities and the six selectivities are what a rebuild is
checked against.

One further note about running our own reader against a trace-only corpus: it logs a
`metric_series` label-cache refresh warning about once a minute. That is expected and is not a
symptom of anything.

#### What the four classes cost today

Whole requests, `GET /api/traces/v1/search?q=<query>&start=1699999999&end=1700432001&limit=20`,
issued to the reader binary built at `fe0d98fe` in `mode: reader` against C6, with the request's
statements attributed from `system.query_log`. Every one answered `200` with 20 traces. Metered
bytes are `result_bytes + length(query)` summed over every statement of the request — both
directions of the `pulsus-server` ↔ ClickHouse hop, which is the cost model §9.1 states.
**Instrument:** every statement issued by our reader binary, which sends `max_block_size = 4096`,
and each statement's `Settings['max_block_size']` in `system.query_log` carries the `4096` back —
a confirmation, because 4,096 is not the server default (§9.5's fourth trap: at the default the
log would be empty and would prove nothing); condition cache dropped before each request; no
`optimize_aggregation_in_order`; the shipped memory and row budgets in force. **This is the first
take of these requests. The `{ .a = .c }` row has since been taken four more times, on three
further builds; see the note below for what did and did not come back.**

| query | traces matching | statements | rows read | granules | metered bytes |
|---|---|---|---|---|---|
| `{ .a = .b }` | 100,000 | 37 | 300,984,841 | 36,744 | 6,186,101 |
| `{ .a = .c }` | 1,000 | **3,127** | **25,904,824,756** | 3,162,449 | 123,448,989 |
| `{ .s1 = .s2 }` | 1,000 | 3,127 | 25,945,817,524 | 3,167,453 | 103,993,640 |
| `{ .a * 2 < .c }` | 9,000 | 347 | 2,869,590,609 | 350,316 | 17,940,201 |
| `{ .s2 = event:name }` | 1,000 | 2,502 | 13,995,704,756 | 1,708,699 | 96,025,490 |

**The request table above was re-taken while this section was being corrected**, one run per class
on a fresh build of C6, with each statement's submitted block size paired to its `query_id` and the
`4096` confirmed in `system.query_log`. Four of the five metered figures came back **to the byte**;
`{ .a = .b }` came back 219 bytes higher, 6,186,320 against 6,186,101 (0.0035%). Statement counts
and granule counts came back exactly on all five. Rows read came back to five significant figures
and not to the digit — 25,904,811,940 against 25,904,824,756 for `{ .a = .c }`, 12,816 rows lower,
and the same 12,816 on two other classes. **Do not read that metered agreement as a general
property.** It is an agreement between two builds; a fourth build, in the note below, moved
`{ .a = .c }`'s metered total by 5,362,175 bytes.

**For `{ .a = .c }`, rows read and metered bytes are recorded, not checks.** That request has been
taken five times: once on each of four separate builds of C6 from the identical pinned recipe, and a
second time on the fourth build against the same corpus with no rebuild in between. Builds one and
two were made while writing this section; builds three and four were made independently, each in a
run of its own. Every take was issued by the reader binary at `max_block_size = 4096` with the
condition cache dropped before the request.

```
{ .a = .c }, whole request -- five takes on four builds
statements and granules are the checks; rows read and metered bytes are recorded

take                          build                   statements  rows read       granules   metered bytes
first (the request table)     implementation build        3,127   25,904,824,756  3,162,449     123,448,989
correction re-take            the correction build        3,127   25,904,811,940  3,162,449     123,448,989
  (the stage table below)
third, by another party       a third build               3,127   25,904,806,780  3,162,449     123,448,989
fourth, by another party      a fourth build              3,127   25,904,929,454  3,162,449     128,811,164
fourth repeated, no rebuild   the same fourth build       3,127   25,904,929,454  3,162,449     128,810,945
```

**What reproduces, and what does not.**

- **Statements and granules reproduce to the digit** — 3,127 and 3,162,449 on all five takes across
  four builds. **These two are the checks.**
- **Rows read varies between builds, and within the one build that was run twice it did not vary at
  all.** The first three builds fall in a 17,976-row band out of 25.9 billion; the fourth is 104,698
  above the top of that band, 5.8 times the band's own width, so the four span 122,674. The fourth
  build is the only one taken twice, and both of its runs returned 25,904,929,454 — the same digits.
- **Metered bytes varies between builds as well, and within a build it is close but not identical.**
  Three builds gave 123,448,989 and the fourth gave 128,811,164, a difference of 5,362,175. The
  fourth build's two runs differ from each other by 219 bytes, so unlike rows read this quantity
  does not settle even on one unchanged corpus. All of that movement is in `result_bytes`.
  The query-text half is **5,593,287** on every take that recorded the split, so the metered totals
  decompose as 117,855,702 + 5,593,287, 123,217,877 + 5,593,287 and 123,217,658 + 5,593,287.
  128,810,945 also appears in the push-comparison table further down, as one of three earlier takes
  of this same total; those were separate runs and nothing here attributes the match.

**The difference between builds is not attributed.** The recipe's byte totals above record that a
rebuild moves every column's compressed size — but the granule *counts* are identical across all
five takes, so nothing here establishes the mechanism, and no take's figures are preferred over
another's. **A rebuild is checked against the eight structural quantities, the six selectivities,
and the statement and granule counts here. It is not checked against rows read or metered bytes.**
Each of those two has already been seen both to repeat a printed value and to change: rows read
came back to the digit on the fourth build's two runs and gave a different value on each of the four
builds; metered bytes gave the same 123,448,989 on three builds and differed by 219 between the
fourth build's two runs. **Neither outcome is compared against anything here, so a further take of
either settles nothing in this paragraph.**

**Where the cost is.** For `{ .a = .c }`, per stage. **Instrument: the correction re-take — one
request issued by the reader binary, sent at `max_block_size = 4096` on every one of its 3,127
statements and confirmed per statement in `Settings['max_block_size']`, condition cache dropped
before the request. This is its own take, not a decomposition of the request-table run above**, and
the rows-read column is the second line of the note above.

| stage | statements | rows read | granules | result bytes | query-text bytes |
|---|---|---|---|---|---|
| phase-1 generator | 1 | 10,043,392 | 1,226 | 4,767,944 | 302 |
| phase-2 hydration | 625 | 785,346,468 | 96,108 | 71,705,392 | 1,320,625 |
| phase-2 `val_num` value reads | 1,250 | 12,554,240,000 | 1,532,500 | 19,816,250 | 2,120,000 |
| phase-2 `val` value reads | 1,250 | 12,554,240,000 | 1,532,500 | 21,469,100 | 2,151,250 |
| winners' root read | 1 | 942,080 | 115 | 97,016 | 1,110 |
| **request** | **3,127** | **25,904,811,940** | **3,162,449** | **117,855,702** | **5,593,287** |

The last row is the sum of the five above it. On **statements** and **granules** it also equals the
`{ .a = .c }` row of the request table above — 3,127 and 3,162,449. **That equality is the check
that catches an instrument mixed into a stage table**, and it is why the row is printed. It matched
on metered bytes as well, `117,855,702 + 5,593,287 = 123,448,989`, but the note above records a
fourth build on which metered bytes is 5,362,175 higher, so that column is recorded here and is not
part of the check. On **rows read** it does not equal the request row and is not claimed to: these
two takes differ there by 12,816 rows, and two later takes differ again.

**This table replaces a 65,409 one.** The version of it that first appeared here carried
`3,145,744 / 323` for the generator and `92,352 / 1,131` for the root read; the same two statements
at 4,096 give `4,767,944` and `97,016` (§9.5's curve is the same effect, measured across eleven
result sizes). Those rows summed to 119,881,011 against a request total of 123,448,989 printed four
lines above them, and neither number was wrong about what it measured.

**All 2,500 value reads read exactly 10,043,392 rows and exactly 1,226 granules** — measured as
`min = max` with `uniqExact(read_rows) = 1`, not inferred from a total that happens to divide (the
trap §9.2 records against itself). That is the whole `key = 'a'` (or `'c'`) partition, once per
read, and it equals the phase-1 generator's own read. `trace_id` is the fifth column of `ORDER BY
(key, val, scope, timestamp_ns, trace_id, span_id)` (`catalog.rs:382`), so a batch's
`trace_id IN (32 ids)` prunes nothing inside it.

**And §9.2's cheap fix does not apply.** §9.2 records that narrowing the *membership* read's
`timestamp_ns` to the batch's own span range collapses it 221x. On the *value* read for the same
batch, window narrowed from 432,002 s to the batch's own 13.401 s:

    read_rows 10,043,392 -> 10,043,392, granules 1,226 -> 1,226, result 320 rows -> 320 rows

A membership read fixes `(key, val, scope)`, so `timestamp_ns` is the next key column and the time
bound has something to prune. A value read fixes only `key` and leaves `val` free, so there is
nothing inside for the time bound to reach. These classes cannot be rescued cheaply.

#### The SQL the push would be

The generator's own predicate and its `GROUP BY trace_id` are unchanged — so `bound_ts`, the
candidate sort key, is unchanged — and a `trace_id IN (…)` sub-statement pre-groups by
`(trace_id, span_id)` and applies a range-and-type test. **No join**: ADR 0008 authorises none and
none is needed. Written out for `{ .a = .c }`; the other four differ only in the keys and the
`HAVING`:

```sql
SELECT trace_id, max(timestamp_ns) AS bound_ts
FROM <trace_attrs_idx | the span-ordered copy>
WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-19')
  AND timestamp_ns > 1699999999000000000 AND timestamp_ns <= 1700432001000000000
  AND (key = 'a')
  AND trace_id IN (
    SELECT trace_id FROM <same table>
    WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-19')
      AND timestamp_ns > 1699999999000000000 AND timestamp_ns <= 1700432001000000000
      AND key IN ('a', 'c')
    GROUP BY trace_id, span_id
    HAVING countIf(key = 'a') > 0 AND countIf(key = 'c') > 0 AND (
        (countIf(key = 'a' AND isNotNull(val_num)) > 0 AND countIf(key = 'c' AND isNotNull(val_num)) > 0
         AND maxIf(val_num, key = 'a') >= minIf(val_num, key = 'c')
         AND minIf(val_num, key = 'a') <= maxIf(val_num, key = 'c'))
     OR (countIf(key = 'a' AND isNotNull(val_num)) = 0 AND countIf(key = 'c' AND isNotNull(val_num)) = 0
         AND maxIf(if(length(val) <= 8192, val, substringUTF8(val, 1, 2048)), key = 'a')
             >= minIf(if(length(val) <= 8192, val, substringUTF8(val, 1, 2048)), key = 'c')
         AND minIf(if(length(val) <= 8192, val, substringUTF8(val, 1, 2048)), key = 'a')
             <= maxIf(if(length(val) <= 8192, val, substringUTF8(val, 1, 2048)), key = 'c')))
  )
GROUP BY trace_id ORDER BY bound_ts DESC, trace_id ASC LIMIT 100001
```

**Three details of that `HAVING` are not decoration, and each was measured.** A future push that
drops any of them answers differently from the evaluator.

- **Compare `val_num`, not `val`, and gate on `isNotNull(val_num)`.** The `f64` rounding happens at
  ingest — `numeric_val_num` (`crates/pulsus-write/src/protocols/otlp_traces.rs:712`) is
  `val.parse::<f64>().filter(is_finite)` — so both sides of the comparison already read the rounded
  number and there is no unrounded side to disagree with. But the `val` String still holds the
  original text:

      stored val            val_num           back to UInt64
      9007199254740992      9007199254740992  9007199254740992
      9007199254740993      9007199254740992  9007199254740992   <- rounds onto its neighbour
      9007199254740994      9007199254740994  9007199254740994
      9007199254740995      9007199254740996  9007199254740996

  A span with `.a = "9007199254740993"` and `.b = "9007199254740992"` **matches `{ .a = .b }`
  today**. A pushed form comparing `val` would answer `false` there — a lost row under `=` and an
  admitted one under `!=` — and nothing downstream re-applies the leaf when the generator is the
  only filter, so a lost row is a wrong answer. The control pair
  `9007199254740994` / `9007199254740995` is unequal under both readings and so cannot mask it.
- **Render the same byte cap on both operands.** `byte_cap_expr`
  (`crates/pulsus-read/src/traces/search_sql.rs:64`) renders
  `if(length(val) <= 8192, val, substringUTF8(val, 1, 2048))`; `length` counts **bytes** and
  `substringUTF8` counts **code points**. Measured: two 8,192-byte values differing in the last byte
  compare unequal (`eq_at_8192 = 0`, both compared in full), and two 8,193-byte values agreeing on
  the first 2,048 code points and differing at byte 8,193 compare **equal** (`eq_at_8193 = 1`, both
  truncated). `repeat('<3-byte char>', 3000)` — 9,000 bytes, 3,000 characters — caps to 2,048
  characters and 6,144 bytes. Comparing raw `val` disagrees at exactly 8,193 bytes; comparing a
  character-counted cap disagrees on any multi-byte value above 2,048 characters.
- **Use a range test, not `anyIf`.** One `(trace_id, span_id, key)` can carry two rows, and the
  evaluator reads `any(val_num)` and `any(val)` (`attr_values_sql`, `search_sql.rs:325`), which is
  an **arbitrary** choice among them. On a three-row fixture (`a` = 5, `a` = 7, `b` = 7 on one span)
  `any(val_num)` for `key = 'a'` returned `5` on ten runs across `max_threads` 1–4 — arbitrary, and
  here stable — so `{ .a = .b }` on that span is decided by which row `any` picked. A pushed `anyIf`
  could pick the other row and **drop** a trace the evaluator keeps. The range test is a superset for
  every choice `any` could make: on that fixture `min_a = 5`, `max_a = 7`, `min_b = max_b = 7`, and
  `maxIf(val_num, key='a') >= minIf(val_num, key='b') AND minIf(val_num, key='a') <= maxIf(val_num, key='b')`
  is `1`.

**The push is exact.** On all five classes its candidate set equals the arithmetic ground truth —
100,000 / 1,000 / 1,000 / 9,000 / 1,000 traces, no extra candidates and none missed — and the first
20 trace ids are the same ids in the same order as today's path (`36420F00…`, `583E0F00…`,
`583E0F00…`, `DC410F00…`, `583E0F00…`). **So the refusal below is about resources alone.**

#### What the push would save on the metered hop, and why one number will not do

Push side: the pushed phase-1 statement above, then the server's own phase-2 statement text taken
from `system.query_log` for the same query with only the `trace_id` list substituted. The memory
ceiling is lifted for this table, because under the shipped ceiling none of these statements runs at
all on the current index order.

**Instrument, and it is not the same on both columns.** The **today** column is a whole request
issued by the reader binary, every statement at `max_block_size = 4096`. The **push** column is
seven statements issued **by hand** — the push does not exist in our binary — on the **current**
index order, with `max_memory_usage = 0` and `max_bytes_before_external_group_by = 0`, condition
cache dropped. The three takes that built this table did not record the push side's block size.
The fourth take, below, does.

| query | today: stmts / metered bytes | push: stmts / rows read / granules / metered bytes | saving |
|---|---|---|---|
| `{ .a = .b }` | 37 / 6,186,101 | 7 / 72,646,656 / 8,868 / 5,025,292 | **1.2x** |
| `{ .a = .c }` | 3,127 / 123,448,989 | 7 / 72,704,000 / 8,875 / 277,773 | **390x – 460x** |
| `{ .s1 = .s2 }` | 3,127 / 103,993,640 | 7 / 72,785,920 / 8,885 / 249,160 | **420x – 440x** |
| `{ .a * 2 < .c }` | 347 / 17,940,201 | 7 / 72,720,384 / 8,877 / 668,425 | **27x – 28x** |
| `{ .s2 = event:name }` | 2,502 / 96,025,490 | 6 / 44,638,208 / 5,449 / 231,450 | **410x – 450x** |

**The ratios are printed to two significant figures as a range, deliberately, and the raw integers
are printed beside them so a reader can recompute any take's own quotient.** Three careful takes of
this table, on three instruments, disagree by up to 8.1% on the metered column while agreeing to the
digit on statement counts and granules — so the entire disagreement is `result_bytes`. (A fourth
take, described below, agrees on statement counts and granules and comes within five significant
figures on rows read.)
Printing four digits of a quotient that four takes cannot reproduce to two would be false
precision, and the decision here does not turn on whether the saving is 390x or 460x.

- The **push** column agreed between 0.00% and 1.14% across the first three takes: `{ .a = .b }`
  gave 5,025,441 / 5,025,378 / 5,025,292, and `{ .a = .c }` gave **277,922** / **280,931** /
  **277,773**. A **fourth** take of the `{ .a = .c }` push side, this time with its instrument
  recorded — seven hand-issued statements, current index order, `max_block_size = 4096`, ceiling
  lifted, cache dropped — gave **318,640**, which is 13.4% above the highest of those three and
  outside their spread. Its statement count (7), rows read (**72,704,000**) and granules (**8,875**)
  reproduced the table's own figures **to the digit**, so once again the whole disagreement is
  `result_bytes`. The same seven statements re-issued at 65,409 gave 311,129, so the push column is
  block-size sensitive too, by 2.4% here. That is why the `{ .a = .c }` saving now reads
  **390x – 460x** rather than 440x – 460x.
- The **today** column differs by 0.5% to 8.1% of the smaller reading: narrowest `{ .a = .b }`
  6,218,236 against 6,186,101, widest `{ .s2 = event:name }` 103,776,196 against 96,025,490. For
  `{ .a = .c }` the three takes that built this table are **128,810,945**, 128,819,521 and
  **123,448,989**. The five-take note under the request table above measures the same quantity on
  one pinned instrument and finds it moving there too, so this column's spread is not only an
  instrument effect.
- **What is attributed:** whether the query condition cache was dropped before the request moves
  the metered column, 17,940,201 dropped against 17,806,317 carried — 0.75%, measured on
  `{ .a * 2 < .c }` only. Applying that figure to the other four classes would be an argument, not
  a measurement.
- **What is not attributed:** about 3% on `{ .a = .c }` and up to about 7% on the event class, and
  now the fourth take's 13.4% on the push side of `{ .a = .c }`. That is unexplained, not noise. The
  fourth take did land outside the range printed above, the range widened from 440x to 390x, and the
  conclusion did not change.

**Two things fall out, and both matter more than the endpoints.** The saving is a function of
**selectivity**, not of the class: the same query text at 1 trace in 10 saves 1.2x and at 1 in 1,000
saves 390x – 460x. And the memory cost below is a function of **window size**, not of selectivity —
it is the same 10.5 GiB for the 1-in-10 and the 1-in-1,000 form.

#### Why none of it was lowered: the per-span pre-grouping does not fit

Relating two attribute values of one span needs a per-span grouping, and its aggregation state is
`O(spans in the search window)`. On the attribute index as it is ordered today, peak query memory
for the pushed phase-1 statement, from `system.query_log`, MiB, min–max over the reps.

**Instrument:** hand-issued statements on the current index order, no memory ceiling applied
(`max_memory_usage = 0`, `max_bytes_before_external_group_by = 0`), `use_query_condition_cache = 0`,
three repetitions per point. **The 625,000-span row deliberately spans both block sizes** — its 30
runs are five forms × two block sizes × three reps — and the 10,000,000-span row is at
**`max_block_size = 4096`**. Block size is the one instrument that does not move this figure, which
is why the two are pooled and why the pooling is stated rather than assumed; the sentence under the
table is the measurement that licenses it.

| spans in window | `{ .a = .b }` | `{ .a = .c }` | `{ .s1 = .s2 }` | `{ .a * 2 < .c }` | `{ .s2 = event:name }` |
|---|---|---|---|---|---|
| 625,000 | 800.7–801.6 | 800.7–801.8 | 800.7–801.7 | 276.1–276.4 | 318.8–392.3 |
| 10,000,000 | 10,526.6–10,777.5 | 10,539.2–10,649.5 | 10,532.1–10,642.1 | 4,314.1–4,372.0 | 5,436.2–6,666.8 |

The 625,000-span row is 30 runs — five forms, two block sizes, three repetitions — and the maximum
over all 30 is **801.8 MiB**. At 312,500 spans the same design spans **167.4–469.5 MiB** across the
five forms; the per-form split at that point was not printed. On this index order the block size
does not move the figure: 10,651.8 against 10,650.2 MiB for the same statement at 4,096 and at
65,409, and a later re-take of `{ .a = .c }` gave 10,650.2 MiB twice at 4,096 and 10,661.9 MiB at
65,409 — a 0.1% spread across four readings and two block sizes, against the 2.9x spread the *form*
of the statement produces. The hash table over roughly 10,000,000 groups dominates, and the
per-thread block buffers do not.

**Peak memory is not one number, and treating it as one is what produced two irreconcilable
readings of it.** At 625,000 spans the answer is anywhere between 276 MiB and 802 MiB depending on
which of the five forms is running — a factor of 2.9 — because the number of aggregate states per
span-group differs: 12 states, four of them `String`, for a field-vs-field equality; 4 states, none
of them `String`, for the arithmetic form. Per span-group at the full window the five forms cost
1,129 / 1,116 / 1,104 / 454 / 571 bytes.

Against that, `generator_settings` (`exec.rs:2869`) applies `max_memory_usage = 536870912` — the
shipped `reader.traceql_generator_max_memory_bytes` (`model.rs:518`) — with
`max_bytes_before_external_group_by = 0`, so the statement throws rather than spilling:

```
Code: 241. DB::Exception: Query memory limit exceeded: would use 515.11 MiB
(attempt to allocate chunk of 4.10 MiB), maximum: 512.00 MiB:
While executing AggregatingTransform. (MEMORY_LIMIT_EXCEEDED) (version 26.3.29.7 (official build))
```

`map_trace_generator_error` (`exec.rs:701`) classifies code 241 first, and `read_error_parts`
(`crates/pulsus-server/src/traces_api/error.rs:366`) answers `422`. Executed rather than reasoned —
same binary, same corpus, same query, the only change being
`reader.traceql_generator_max_memory_bytes`:

```
default (536870912):
  HTTP/1.1 200 OK
  content-type: application/json
  -> 20 traces, first 583e0f00000000000000000000000000

16777216:
  HTTP/1.1 422 Unprocessable Entity
  content-type: text/plain; charset=utf-8
  content-length: 80

  query too broad: trace search generator memory budget of 16777216 bytes exceeded
```

(no trailing newline; at the shipped default the number in the body reads `536870912`). **A query
that answers 200 today would answer 422.**

**Where the boundary falls, measured by bisection rather than interpolated.** Same statements under
the shipped `max_memory_usage = 536870912` and `max_bytes_before_external_group_by = 0`, at
`max_block_size = 4096`, window narrowed by `timestamp_ns` until it holds exactly the stated number
of spans (counted, not assumed):

    390,000 spans  10 of 10 complete at 4,096, and 10 of 10 at 65,409
    395,000 spans  the peaks straddle the ceiling -- see below

**At 395,000 spans the peaks fall into two clusters 24 MiB apart with the ceiling between them, and
which cluster a run lands in was not explained.** Completions peak at 482.1–486.3 MiB and refusals
at 508.0–512.0 MiB. Four blocks of runs at that window:

    25 runs, first block                                          15 completed / 10 refused
    40 runs (mark, uncompressed, primary-index and condition
            caches dropped before three of the five blocks)       40 completed /  0 refused
    25 runs replaying the earlier block order                     25 completed /  0 refused
    25 runs at max_block_size = 65,409                            25 completed /  0 refused
    23 runs on a second instrument                                 0 completed / 23 refused

So a run-to-run flip was observed once and did not reproduce, and a second instrument sat in the
high cluster for 23 consecutive runs. **It is recorded as two clusters, not as a rate**: the
mechanism that selects one is not established, and neither instrument could force the other's
result. Nothing in the recommendation rests on it.

**Two rescues on the current index order, both measured, both refused.** `{ .a = .c }` pushed, full
window, three reps. **Instrument: hand-issued, `max_block_size = 4096` except where the row says
otherwise, no memory ceiling on the first three rows, `use_query_condition_cache = 0`.**

```
current index order, plain                             10,647–10,760 MiB   read 30,130,176  marks 3,678
current index order + optimize_aggregation_in_order=1  10,634–10,780 MiB   -- no improvement
   (its own paired plain run, same three reps:         10,506–10,629 MiB)
current index order + in_order at max_block_size=4096   0 of 3 complete, peak 511.6 MiB
```

In-order aggregation buys nothing here because the grouping key `(trace_id, span_id)` is not a
prefix of `ORDER BY (key, val, scope, timestamp_ns, trace_id, span_id)`, so the aggregation cannot
stream. A numeric-only `HAVING` — the cheapest per-span comparison that could be written — costs
5,894–6,027 MiB on this order. **Six statements were tried at the shipped ceiling and block size on
the current index order — the five classes and the numeric-only `HAVING` — and all six refuse. That
is six, not all.**

#### The one thing that does fit, and the two changes it needs

The same statement against an attribute table ordered `(trace_id, span_id, key)` streams the
aggregation, and then it fits. The attribution took two rounds and one setting:

```
span-ordered index, optimize_aggregation_in_order = 1, full 10,000,000-span window, { .a = .c }

max_block_size    uncapped peak (3 reps)         under max_memory_usage = 536870912
65,409 (server)   1130.6 / 1107.0 / 1068.3 MiB   0 of 3 -- Code 241 at 520.60 / 514.65 / 516.05 MiB
 4,096 (ours)      248.1 /  232.9 /  228.7 MiB   3 of 3 COMPLETE at 230.2 / 222.0 / 225.1 MiB
```

**Our reader sends 4,096, so the shipped answer is the second row.** All five classes, span-ordered
table, `optimize_aggregation_in_order = 1`, `max_block_size = 4096`, shipped memory ceiling, full
window, three reps:

| class | capped peaks (MiB) | rows read | marks | current index order, same window |
|---|---|---|---|---|
| `{ .a = .b }` | 242.6 / 238.4 / 231.7 | 112,197,568 | 13,699 | 0 of 3, `Code: 241` |
| `{ .a = .c }` | 249.6 / 222.6 / 240.9 | 80,658,368 | 9,849 | 0 of 3, `Code: 241` |
| `{ .s1 = .s2 }` | 248.4 / 231.0 / 243.9 | 80,658,368 | 9,849 | 0 of 3, `Code: 241` |
| `{ .a * 2 < .c }` | 76.3 / 77.8 / 79.3 | 91,520,960 | 11,175 | 0 of 3, `Code: 241` |
| `{ .s2 = event:name }` | 274.8 / 250.1 / 247.6 | 80,658,368 | 9,849 | 0 of 3, `Code: 241` |

Exact on all five: the candidate set equals the arithmetic ground truth — 100,000 / 1,000 / 1,000 /
9,000 / 1,000 traces, none extra and none missed — and the first 20 ids are the same ids in the same
order as on the current index order. The same numeric-only `HAVING` control that costs 5,894–6,027
MiB on the current order costs 77.7–85.6 MiB here, which is what establishes that the index order,
not the statement, is what changed.

**And then a second shipped budget refuses it, which nobody had looked at.** `generator_settings`
inherits `search_settings`, so the pushed statement carries **four** throw budgets and not one:
rows read, bytes read, result bytes and memory. On the span-ordered table at the full window with
everything shipped:

```
Code: 158. DB::Exception: Limit for rows or bytes to read exceeded,
max rows: 50.00 million, current rows: 51.31 million: While executing MergeTreeSelect…
```

`map_trace_read_error` turns 158 into `TooBroadReason::TraceScanBudgetRows` (`exec.rs:666`) and
`ReadError::QueryTooBroad(_)` is `422` (`traces_api/error.rs:366`), so **"it fits" is false at the
level a user experiences** until `reader.traceql_scan_budget_rows` is raised as well. All five
classes trip it, at 51.31 / 51.31 / 55.75 / 53.30 / 52.88 million rows in the order of the table
above.

**`current rows` is a property of the run, not of the class.** It is how far the read had got when
the limit tripped, so it is not reproducible: three consecutive runs of the identical `{ .a = .c }`
statement — same instrument, condition cache dropped before each — printed **51.31, 51.31 and
53.30** million. The five numbers above are one reading each and should be read as "somewhere just
above 50 million", never compared class against class. What *is* reproducible is the refusal itself
and the rows the statement would read if allowed: 80,658,368 for `{ .a = .c }`, re-measured to the
digit on a later build.

With that one budget at 200,000,000 and every other setting shipped, three reps each:

| class | peaks (MiB) | rows read | result rows (the right answer) |
|---|---|---|---|
| `{ .a = .b }` | 229.9 / 229.9 / 231.0 | 112,197,568 | 100,000 |
| `{ .a = .c }` | 225.8 / 216.1 / 227.8 | 80,658,368 | 1,000 |
| `{ .s1 = .s2 }` | 234.7 / 237.1 / 239.8 | 80,658,368 | 1,000 |
| `{ .a * 2 < .c }` | 69.7 / 74.0 / 76.1 | 91,520,960 | 9,000 |
| `{ .s2 = event:name }` | 254.7 / 255.9 / 231.7 | 80,658,368 | 1,000 |

**Note what that budget is bounding.** Today's `{ .a = .c }` request reads 25,904,824,756 rows
across 3,127 statements and passes, because the budget is **per statement**. (That rows figure, and
every repetition of it below, is the first of the five takes recorded above; the five span 122,674
rows and nothing in the argument turns on which one is used.) The push reads
80,658,368 rows in **one** statement and is refused. The budget bounds a statement, and the class it
refuses is the one that replaced three thousand statements with one.

#### Where this leaves the read path

Today the generator prunes on `key` — a group-3 leaf reaches 6 to 12 marks of 8,676 — and the cost
is not the generator, it is the phase-2 loop: 3,127 statements and 25.9 billion rows read for one
`{ .a = .c }` request. The push replaces that loop with one statement. On the current index order it
keeps the `key` prune (30,130,176 rows) but cannot hold the per-span state (about 10.5 GiB). On the
span-ordered order it holds the state (216–275 MiB) and loses the `key` prune, so it reads every
attribute row in the window — 80,658,368 in that one statement, about 123 million across the whole
seven-statement request, against today's 25,904,824,756. Nothing moves client-side either way: the
comparison is a `HAVING` on ClickHouse in both.

**So the four classes are not blocked by the compiler. They are blocked by the attribute index order
and by one budget** — a schema change and a configuration change, neither of which is part 6's to
make.

Two claims here are scale-dependent and are not asserted: where the row budget binds relative to
memory at production volume, and whether 80,658,368 rows per generator statement is acceptable at
1 TB. Those belong to [#25](https://github.com/digitalis-io/pulsusdb/issues/25).

#### Where this measurement stops

- **Nothing here is checkable in CI, and the corpus is gone.** C6 was 71,000,000 rows on a developer
  instance and was dropped. No fixture reproduces it, no xtask scenario generates it, and no cargo
  test reads this section. That is why the recipe above, not any figure, is the deliverable.
- **The two tests this part shipped prove only what the compiler emits.**
  `crates/pulsus-read/tests/traceql_group2_selector_generators.rs` asserts that a negated `name`
  leaf keeps its predicate in a bounded span scan while a negated attribute leaf does not, and that
  no generator predicate contains a nested statement. Neither can observe a memory ceiling, and
  neither would notice if any figure above were wrong.
- **One corpus, one attribute density.** Seven attribute rows per span and one window shape. A
  denser corpus crosses the memory ceiling at fewer spans and trips the row budget sooner.
- **The span-ordered index was measured at one window** — the full 10,000,000 spans — so its
  memory-versus-window curve is argued, not measured: in-order aggregation streams, so the state is
  bounded by the per-thread block buffers rather than by the number of groups, while rows read grow
  linearly with the window. That is the argument for the row budget, not memory, binding first at
  production volume, and establishing that ordering belongs to #25.
- **The 395,000-span flip's mechanism is not established**, and about 3% of the metered-bytes
  disagreement between takes is not attributed.
- **The push side of the saving table is our statement at our settings, but it was not issued by our
  binary**, because the push does not exist in our binary. The phase-2 text was taken from
  `system.query_log` with only the id list substituted.
- **The argument that no lower-memory form exists on the current index order is an argument.**
  Relating two rows of one span needs a join (ADR 0008 authorises none, and a hash join materialises
  the same per-span state), a correlated subquery (ClickHouse has none in this position), or a
  per-span grouping. It is falsified by any statement that decides `{ .a = .b }` per span under
  512 MiB on a 10,000,000-span window at `max_block_size = 4096`. Three rescues were tried —
  in-order aggregation, the span-ordered index, and the two combined — and the storage figures
  below. That is three, not all.

#### The span-ordered attribute index — a proposal for scheduling, not part of this work

**What it is.** An **additional** `trace_attrs_idx`-shaped table ordered `(trace_id, span_id, key)`,
alongside the existing `ORDER BY (key, val, scope, timestamp_ns, trace_id, span_id)`
(`crates/pulsus-schema/src/catalog.rs:382`) — **not instead of it**.

**What it costs to store.** **451,383,963** bytes for the same **71,000,000** rows, on top of the
existing key-ordered `trace_attrs_idx` — which is **1,128,726,045** bytes on four builds of the
recipe and **1,128,725,599** on another — so **+40%** against either denominator, plus a second
write of every attribute row on ingest. The copy's byte total repeated exactly on **four**
independent builds, and the quotient agrees to four digits against both recorded denominators:
**39.990569%** against 1,128,726,045 and **39.990584%** against 1,128,725,599, a disagreement in
the seventh digit. That is why this one byte figure is quoted where the corpus's own totals are
not; the recipe above records every build of that table and its spread. **Instrument: `sum(bytes_on_disk)` over
`system.parts` for active parts after `OPTIMIZE … FINAL`, the same statement that produced the
corpus totals.**

**Why it cannot replace the key-ordered index.** Group 3's leaves prune on `key`, which is not the
leading column of the span-ordered table. **Instrument: hand-issued `SELECT count()`, condition
cache dropped before each statement (without that, granule counts are meaningless — §9.5's first
trap), `max_block_size = 4096`; all four rows re-measured to the digit on a later independent
build.**

| leaf | key-ordered | span-ordered | |
|---|---|---|---|
| `key='http.method' AND val='GET' AND scope='span'` | 98,305 rows / 12 marks | 71,000,000 rows / 8,670 marks | 722x |
| `key='a' AND val='42' AND scope='span'` | 49,152 rows / 6 marks | 71,000,000 rows / 8,670 marks | **1,444x** |

Same answers on both tables — 1,666,667 and 10,000 matching rows — at 722x and 1,444x the rows read.
So this is a second copy of the attribute rows, not a re-ordering of the existing one.

**The budget it needs alongside.** `reader.traceql_scan_budget_rows`, raised from 50,000,000
(`crates/pulsus-config/src/model.rs:515`) to cover the window's attribute rows; **200,000,000** was
measured. Without it every one of the five classes returns
`Code: 158. DB::Exception: Limit for rows or bytes to read exceeded, max rows: 50.00 million,
current rows: …` — the trailing figure is where the read had got when the limit tripped and varies
run to run on one class (51.31, 51.31, 53.30 million on three runs of `{ .a = .c }`) — and the user
sees `422`.

**What it buys.** For the four selector classes: 7 statements and 80,658,368 rows read in place of
3,127 statements and 25,904,824,756 rows, and a metered hop between 1.2x at 1 matching trace in 10
and 390x – 460x at 1 in 1,000 — a function of selectivity, and the endpoints are the take-to-take
range, not a precision claim.

**This is a proposal for the owner to schedule, and nothing more.** Part 6 does not build the table,
does not change a configuration default, and **files nothing**.

---

### 9.8 `select()` is refused, and the measurement that refuses it

Issue #492 part 7 asked whether a `select()` projection can be compiled into the statements a
TraceQL search already sends. It cannot, for the query the scope enumeration names, and part 7
changes no production line. This section is the measurement, so that the round which amends
[ADR 0008](decisions/0008-sql-composition-for-lowered-pipelines.md) starts from figures rather than
from an argument.

**The finding first, because it is the one an amendment has to meet.** The per-query join form — the
shape that justifies "replaces one statement per batch with one statement per query" — does not
survive the shipped generator memory ceiling. At `max_memory_usage = 536870912`, the shipped
`reader.traceql_generator_max_memory_bytes` (`crates/pulsus-config/src/model.rs:518`, applied by
`generator_settings`, `crates/pulsus-read/src/traces/exec.rs:2869`), it refused on all three takes,
`exception_code` 241, 721 marks selected, no rows out. **The refusal is asserted on `Code: 241` and
`512.00 MiB`, and on nothing else.** Everything else in the message is a record, and the three
bodies below differ from each other in all four of the ways it can: the "would use" figure takes two
values across these three takes, the chunk figure takes two, the execution site takes two — the
aggregate on two takes and a storage read on the third — and a part path appears on one take of the
three. All three takes are the same statement at the same ceiling on the same build.

```text
take 1, build 81e37dde-8239-405e-a5ab-fddd4c68a5de:
Code: 241. DB::Exception: Query memory limit exceeded: would use 514.04 MiB (attempt to allocate chunk of 4.00 MiB), maximum: 512.00 MiB: While executing AggregatingTransform. (MEMORY_LIMIT_EXCEEDED) (version 26.3.29.7 (official build))
take 2, build 81e37dde-8239-405e-a5ab-fddd4c68a5de:
Code: 241. DB::Exception: Query memory limit exceeded: would use 514.04 MiB (attempt to allocate chunk of 4.00 MiB), maximum: 512.00 MiB: While executing AggregatingTransform. (MEMORY_LIMIT_EXCEEDED) (version 26.3.29.7 (official build))
take 3, build 81e37dde-8239-405e-a5ab-fddd4c68a5de:
Code: 241. DB::Exception: Query memory limit exceeded: would use 514.05 MiB (attempt to allocate chunk of 4.04 MiB), maximum: 512.00 MiB: (while reading column trace_id): (while reading from part /var/lib/clickhouse/store/3c6/3c6209c2-899e-4088-8041-c0bb02613ec1/20231117_9_12_2/ in table arch492p7.trace_attrs_idx (3c6209c2-899e-4088-8041-c0bb02613ec1) located on disk default of type local, from mark 24 with max_rows_to_read = 4096, offset = 0): While executing MergeTreeSelect(pool: ReadPool, algorithm: Thread). (MEMORY_LIMIT_EXCEEDED) (version 26.3.29.7 (official build))
```

**The statement those three bodies come from.** The capture statement printed under "The
instrument" below selects `exception_code` but not the body, so the bodies were read by a second
`system.query_log` statement, run after the same flush loop against the same build:

```sql
SELECT ql.query_id, ql.exception
FROM system.query_log AS ql
WHERE ql.type = 'ExceptionWhileProcessing'
  AND startsWith(ql.query_id, 'p7c_t4_join_ceiling_')
ORDER BY ql.query_id
```

It returns three rows, one per take. They are the only rows the capture found with a non-zero
`exception_code`: of the 42 takes that capture returned, the three ceiling takes of `t4_join` are
the only ones that failed. `'p7c_t4_join_ceiling_'` is this section's `query_id` prefix followed by
the statement name, so a re-taker changes the same string literal here as in the capture statement.

**These three bodies are this build's own output, read from the container that produced the
figures below.** They are not an illustration of the shape such a body has: the statement above was
run against the measurement container while it was still up, and its three rows were copied from
the terminal into this section. The copying step itself is checked by nothing: the container is
gone and its terminal was not saved. What can be shown is that the three takes the statement above
matches are in the capture, and failed there. These are three rows of the capture output file — the
scratch file the next paragraph describes — with its header, read out of that file by
`(head -1 <capture file>; grep '^p7c_t4_join_ceiling_' <capture file>) | cut -f1-5,8 | column -t` —
the `memory_usage` and `result_bytes` columns are cut, because they are records and this section
does not publish them:

```text
query_id               exception_code  marks  read_rows  result_rows  build_uuid
p7c_t4_join_ceiling_1  241             721    2551808    0            81e37dde-8239-405e-a5ab-fddd4c68a5de
p7c_t4_join_ceiling_2  241             721    2592768    0            81e37dde-8239-405e-a5ab-fddd4c68a5de
p7c_t4_join_ceiling_3  241             721    2699264    0            81e37dde-8239-405e-a5ab-fddd4c68a5de
```

Those are the three `query_id`s in full, and they are the row the tables below summarise as
`t4_join at ceiling`: 721 marks, `exception_code` 241, no rows out, three takes, on build
`81e37dde-8239-405e-a5ab-fddd4c68a5de`. `read_rows` is a record and differs across the three. This
does not show that the bodies above were copied from that container — nothing here shows that — only
that the three takes they are attributed to exist in the capture and failed the way the bodies say.

**The body statement's stdout was not kept.** The capture statement's output was written to a file
at capture time — measurement scratch, not in this repository — and the `marks`, `read_rows`,
`rows out` and `exception_code` columns of the take tables below were read out of that file. The
body statement's output was not written anywhere: it was read off the terminal, and the block at the
top of this section is the only copy of it that survives. The container has been removed, so it
cannot be read again; a re-taker builds the corpus afresh and gets three new bodies, differing in
the ways listed under "Varies" below.

What was checked instead is the statement. The SQL printed above is byte-for-byte the file the
measurement submitted, and it was run once more against a different server, on three throwaway
queries made to fail on purpose under the same `query_id` spelling, to show it returns one row per
take with a body in it rather than merely parsing. **Those throwaway bodies are not the
measurement's, are not printed in this document, and are not the source of any figure here.** The
three above were already in this file at commit `b97d6853`, which predates that run.

**The same statement with only the join removed runs.** The control — `t4_control` below, which is
`t4_join` with the `LEFT JOIN (…) AS sel ON …` block and the `sel.v AS sel_method` projection taken
out and nothing else changed — answered 20 rows at 476 marks with `exception_code` 0, on all three
takes at the same ceiling. **The breach is the join's, isolated by removing only the join.** A run
where both fail, or both succeed, means the corpus is not the one this recipe builds.

The corpus this happened on holds 10,000,000 `trace_attrs_idx` rows and 2,000,000 `trace_spans`
rows, which is what the physical-layout statement printed under "The corpus" below returned.
Code 241 on a generator read maps to `TooBroadReason::TraceGeneratorMemory`
(`map_trace_generator_error`, `crates/pulsus-read/src/traces/exec.rs:701`) and the request answers
**422**. Table 4 is the whole measurement.

#### The build these figures come from

- build `81e37dde-8239-405e-a5ab-fddd4c68a5de`

That is the `system.tables.uuid` of `arch492p7.trace_spans` on the container the figures below were
taken on, and it is not typed by hand: the capture statement returns it in the same row as the
figures, so a take carries the identity of the build it ran on. `CREATE TABLE` generates it, so a
rebuild of the same recipe gets a different one. The container ran ClickHouse **26.3.29.7**
(`SELECT version()`), held one build of the recipe printed below, and was removed when the
measurement finished.

The third error body above carries `3c6209c2-899e-4088-8041-c0bb02613ec1`. A `Code: 241` that fails
inside a storage read prints the part path, and the part path carries the UUID of the table being
read; a failure in the aggregate has no part path, which is why two of the three bodies do not carry
one. **That UUID corroborates nothing.** The capture statement returns `trace_spans`'s UUID, not
`trace_attrs_idx`'s, so no saved output outside the body carries that value, and the container that
could have been asked for the mapping has been removed. It is repeated here because it is part of
the body; the build label above rests on the capture statement alone.

**Nothing here checks that a take table carries the right UUID.** No check covers a mistyped or
swapped label. What makes it unlikely rather than impossible is that the UUID is copied out of the
capture statement's own output rather than written down from memory.

#### The instrument

Every batch statement was submitted over HTTP, by hand rather than by the reader, with exactly these
settings, and the wire figure is that response piped to `wc -c`:

```text
default_format=RowBinaryWithNamesAndTypes&max_block_size=4096&use_query_condition_cache=0
&max_rows_to_read=200000000&max_bytes_to_read=8589934592&read_overflow_mode=throw
&max_result_bytes=1073741824&result_overflow_mode=throw&max_memory_usage=8589934592
&max_bytes_before_external_group_by=0
```

`max_block_size=4096` is the shipped `TRACE_SEARCH_MAX_BLOCK_ROWS`
(`crates/pulsus-read/src/traces/exec.rs:173`). `use_query_condition_cache=0` is in the instrument so
that a second take of a statement cannot be served in part from work an earlier take left behind;
this section does not measure what that setting is worth, it holds it fixed. Table 4's ceiling takes
are the same string with `max_memory_usage=536870912`.

**Wire bytes are measured outside the query log and are not `result_bytes`.** `result_bytes` is the
in-memory block size of the result, not what crosses the metered hop, and on the hydration-against-
join pair of table 3 the two moved in opposite directions on this build. This section publishes no
`result_bytes` figure; the capture statement above returns the column for anyone who wants to look.
Every byte figure below is `wc -c` over
`default_format=RowBinaryWithNamesAndTypes` — the format the reader decodes
(`crates/pulsus-clickhouse/src/lib.rs:3`) — with no query log involved.

The granule figures come from this capture statement. It selects the build's `trace_spans` UUID in
the same row as the figures:

```sql
SELECT ql.query_id,
       ql.exception_code,
       ql.ProfileEvents['SelectedMarks'] AS marks,
       ql.read_rows, ql.result_rows, ql.memory_usage, ql.result_bytes,
       (SELECT uuid FROM system.tables
        WHERE database='arch492p7' AND name='trace_spans') AS build_uuid
FROM system.query_log AS ql
WHERE ql.type IN ('QueryFinish','ExceptionWhileProcessing')
  AND startsWith(ql.query_id, 'p7c_')
ORDER BY ql.query_id
```

`'p7c_'` is the prefix on the takes this section publishes: each `query_id` is that prefix, the
statement name and the take number. Anyone re-taking these figures changes that one string literal
to their own prefix.

**`SYSTEM FLUSH LOGS` is not a barrier, and one capture can come back short.** Run the flush, wait,
capture, and repeat until the row count stops growing; read the figures off the last run. A take
table built from a single capture can be missing takes with nothing on the screen to say so, which
is the same hazard as a query that matched nothing looking like a query that found nothing. The
takes below were read off a capture whose row count had stopped growing, at 42 rows.

**The instrument, validated where it must fail.** This returns `0`, and a harness that reads a
figure out of an empty result must abort rather than print a plausible number:

```sql
SELECT count() AS rows_for_a_query_id_that_does_not_exist
FROM system.query_log
WHERE type='QueryFinish' AND query_id='v5_no_such_take_0000'
```

**And validated where it must discriminate.** `a1` with its key predicate deleted — `a1_nokey`,
printed in table 1 — reads every granule in the table: 1,225 marks against `a1`'s 226. The mark
figure is decided by the key predicate, not by the batch of trace ids.

#### `<the 32>`, the batch every read below is taken over

Every read below is one batch of 32 trace ids over the whole five-day window, which is what the
shipped `BATCH_TRACES = 32` loop sends (`crates/pulsus-read/src/traces/exec.rs:117`). **`<the 32>`
means exactly this list, in this order, wherever it appears in a statement below**, and it is the
only substitution any statement in this section carries:

```text
'T000000000000099','T000000000006299','T000000000012599','T000000000018799','T000000000025099','T000000000031299','T000000000037501','T000000000043751','T000000000050001','T000000000056251','T000000000062501','T000000000068751','T000000000075001','T000000000081251','T000000000087501','T000000000093751','T000000000100001','T000000000106251','T000000000112501','T000000000118751','T000000000125001','T000000000131251','T000000000137501','T000000000143751','T000000000150001','T000000000156251','T000000000162501','T000000000168751','T000000000175001','T000000000181251','T000000000187501','T000000000193751'
```

which is what this generator prints, one id per row:

```sql
WITH number*6250 + if(number<6,if(number%2=0,99,49),1) AS n
SELECT toFixedString(concat('T',leftPad(toString(n),15,'0')),16) FROM numbers(32)
```

#### The corpus, as the text that builds it

Run this through `clickhouse-client --multiquery --queries-file`. It is not runnable over HTTP: a
multi-statement body answers `SYNTAX_ERROR`.

```sql
DROP DATABASE IF EXISTS arch492p7;
CREATE DATABASE arch492p7;

CREATE TABLE arch492p7.trace_spans (
  trace_id FixedString(16), span_id FixedString(8), parent_id FixedString(8),
  name LowCardinality(String), service LowCardinality(String),
  timestamp_ns Int64 CODEC(DoubleDelta, ZSTD(1)),
  duration_ns Int64 CODEC(T64, ZSTD(1)), status_code Int8, kind Int8,
  payload_type Int8, payload String CODEC(ZSTD(3)),
  shared UInt8 DEFAULT 0,
  status_message String DEFAULT '',
  scope_name LowCardinality(String) DEFAULT '',
  scope_version LowCardinality(String) DEFAULT '',
  INDEX idx_duration duration_ns TYPE minmax GRANULARITY 4,
  PROJECTION service_time (SELECT * ORDER BY (service, timestamp_ns)),
  PROJECTION span_name_day (SELECT toDate(fromUnixTimestamp64Nano(timestamp_ns)) AS d, name, count() GROUP BY d, name)
) ENGINE=MergeTree
PARTITION BY toDate(fromUnixTimestamp64Nano(timestamp_ns))
ORDER BY (trace_id,timestamp_ns)
TTL toDateTime(fromUnixTimestamp64Nano(timestamp_ns)) + INTERVAL 10000 DAY DELETE
SETTINGS ttl_only_drop_parts=1;

CREATE TABLE arch492p7.trace_attrs_idx (
  date Date, key LowCardinality(String), val String,
  scope LowCardinality(String), val_num Nullable(Float64), timestamp_ns Int64,
  trace_id FixedString(16), span_id FixedString(8), duration_ns Int64,
  val_type LowCardinality(String) DEFAULT ''
) ENGINE=ReplacingMergeTree
PARTITION BY date
ORDER BY (key,val,scope,timestamp_ns,trace_id,span_id)
TTL toDateTime(fromUnixTimestamp64Nano(timestamp_ns)) + INTERVAL 10000 DAY DELETE
SETTINGS ttl_only_drop_parts=1;

CREATE VIEW arch492p7.p7_span_seed AS
SELECT number AS span_n,
  toFixedString(concat('T',leftPad(toString(intDiv(number,10)),15,'0')),16) AS trace_id,
  toFixedString(leftPad(toString(number),8,'0'),8) AS span_id,
  if(number%10=0,toFixedString('',8),
     toFixedString(leftPad(toString(number-1),8,'0'),8)) AS parent_id,
  concat('span-',toString(number%10)) AS name, 'checkout' AS service,
  toInt64(1699920000000000000 + intDiv(number,400000)*86400000000000
          + (number%400000)*100000000) AS timestamp_ns,
  toInt64(1000000+(number%10000)) AS duration_ns,
  toInt8(arrayElement([0,1,2,2,0],toUInt32(number%5+1))) AS status_code,
  toInt8(number%6) AS kind, toInt8(0) AS payload_type, '' AS payload,
  toUInt8(0) AS shared, '' AS status_message,
  'review-scope' AS scope_name, '1.0' AS scope_version
FROM numbers_mt(2000000);

-- PIN (a): TWO inserts, split exactly here. The one-insert form's mark count is
-- not stable across rebuilds; the pinned split gives 255.
INSERT INTO arch492p7.trace_spans
SELECT * EXCEPT span_n FROM arch492p7.p7_span_seed
WHERE span_n%400000<200000;
INSERT INTO arch492p7.trace_spans
SELECT * EXCEPT span_n FROM arch492p7.p7_span_seed
WHERE span_n%400000>=200000;
DROP VIEW arch492p7.p7_span_seed;

INSERT INTO arch492p7.trace_attrs_idx
WITH intDiv(number,5) AS span_n, intDiv(span_n,10) AS trace_n,
 number%5 AS attr_n,
 toInt64(1699920000000000000 + intDiv(span_n,400000)*86400000000000
         + (span_n%400000)*100000000) AS ts,
 arrayElement(['service.namespace','tenant','http.method','http.status_code','k4'],
              toUInt32(attr_n+1)) AS attr_key,
 multiIf(
   -- PIN (b): 91/89/90/90/90 by date, 90% overall. Uniform 90 gives 225 / 470.
   attr_n=0,if((intDiv(trace_n,40000)=0 AND trace_n%100<91)
              OR (intDiv(trace_n,40000)=1 AND trace_n%100<89)
              OR (intDiv(trace_n,40000)>=2 AND trace_n%100<90),'prod','dev'),
   attr_n=1,concat('tenant-',toString(trace_n%50)),
   -- PIN (c): this assignment decides the attribute statements' wire bytes.
   attr_n=2,arrayElement(['GET','POST','PUT','DELETE','PATCH'],toUInt32(span_n%5+1)),
   attr_n=3,toString(arrayElement([200,400,404,500,503],toUInt32(span_n%5+1))),
   concat('v',toString(span_n%100))) AS attr_val
SELECT toDate(fromUnixTimestamp64Nano(ts)), attr_key, attr_val,
 if(attr_n IN (0,1),'resource','span'),
 if(attr_n=3,toNullable(toFloat64(attr_val)),NULL), ts,
 toFixedString(concat('T',leftPad(toString(trace_n),15,'0')),16),
 toFixedString(leftPad(toString(span_n),8,'0'),8),
 toInt64(1000000+(span_n%10000)), if(attr_n=3,'int','string')
FROM numbers_mt(10000000);
OPTIMIZE TABLE arch492p7.trace_spans FINAL;
OPTIMIZE TABLE arch492p7.trace_attrs_idx FINAL;
```

Three pins in that text decide figures below. **(a)** the span insert is split into two, exactly
where the recipe splits it; a single insert lands its part boundaries wherever the threads finish
and its mark count is not stable across rebuilds. **(b)** the `service.namespace` split is
91/89/90/90/90 by date rather than a uniform 90%, which is what makes `a1` 226 marks rather than
225. **(c)** which HTTP method lands on which span decides the attribute statements' wire bytes and
nothing else — marks and rows do not move with it.

**Pin (b)'s comment names a figure this build did not produce.** `225 / 470` comes from the
uniform-90 counterfactual corpus, built and measured while the recipe was being pinned and published
at [`5572477064`](https://github.com/digitalis-io/pulsusdb/issues/492#issuecomment-5572477064),
[`5573379164`](https://github.com/digitalis-io/pulsusdb/issues/492#issuecomment-5573379164) and
[`5574039261`](https://github.com/digitalis-io/pulsusdb/issues/492#issuecomment-5574039261). It is
kept in the recipe because it is what tells a re-taker which corpus they built. No figure this
section publishes comes from it.

**A `trace_spans` without `status_message` cannot run table 3 at all.** The shipped hydration
statement projects that column (`hydration_sql`,
`crates/pulsus-read/src/traces/search_sql.rs:230`, and any `== phase2 hydration ==` section in the
committed goldens), so a reduced span shape fails with `UNKNOWN_IDENTIFIER` before the query starts
rather than returning a wrong number. That is the good failure, but only if the recipe carries the
column — which is why it carries the full shipped span shape: migration 16 plus `shared`,
`status_message`, `scope_name`/`scope_version` and the `span_name_day` projection.

**What that recipe produced here.** The physical layout:

```sql
SELECT table, sum(rows), count(), sum(marks), groupArrayDistinct(part_type)
FROM system.parts WHERE active AND database='arch492p7'
GROUP BY table ORDER BY table
```

```text
trace_attrs_idx  10000000  5  1230  ['Wide']
trace_spans       2000000  5   255  ['Wide']
```

Read that statement once the `OPTIMIZE … FINAL`s have settled. Taken immediately after the recipe
returned it answered `trace_attrs_idx 10000000 8 1234`, because a merge's source parts were still
marked active; the three reads taken afterwards all gave the five parts above.

And the per-date split that shows pin (b) landed:

```sql
SELECT date, countIf(val='prod') FROM arch492p7.trace_attrs_idx
WHERE key='service.namespace' GROUP BY date ORDER BY date
```

```text
2023-11-14  364000      2023-11-16  360000
2023-11-15  356000      2023-11-17  360000
                        2023-11-18  360000
```

A corpus answering `360000` five times is the uniform-90 one, and that is how to tell the two apart
before running anything else.

#### The statement register

Every statement any figure in this section comes from, and where it is printed in full.

```text
statement            printed in                     what it produces a figure for
the id generator     "`<the 32>`" above             the batch every read is taken over
the capture          "The instrument" above         every mark, rows-read, rows-out and
                                                    exception-code figure below
the error-body read  the top of this section        the three `Code: 241` bodies
the zero-row check   "The instrument" above         the instrument's own empty answer
the recipe           "The corpus" above             the corpus
physical layout      "The corpus" above             parts, rows, marks, part type
per-date prod        "The corpus" above             the per-date `prod` counts
a1                   table 1                        226 / 1,851,392 / 260 / 6,289
a1_nokey             table 1                        1,225 / 10,000,000 / 320 / 7,729
a2                   table 1                        245 / 2,007,040 / 320 / 11,651
a3                   table 1                        471 / 3,858,432 / 320 / 11,985
a4_widened           table 1                        495 / 4,055,040 / 320 / 11,985
t2_agg               table 2                        250 / 2,048,000 / 320 / 11,918
t2_select            table 2                        250 / 2,048,000 / 320 / 10,307
t2_merged            table 2                        250 / 2,048,000 / 320 / 13,207
t3_hyd               table 3                        32 / 262,144 / 320 / 27,106
t3_join              table 3                        277 / 2,269,184 / 320 / 31,036
t3_no_alias          table 3                        277 / 2,269,184 / 320 / 31,028
t4_join              table 4                        721 marks, at both ceilings
t4_control           table 4                        476 marks, 20 rows out
answer agreement     "The answers agree" below      260 / 320 / 320 and four zeros
the witness          "The join-free merge" below    two rows against three
```

A figure whose statement is not in that list is not published in this section. In particular this
section publishes no `result_bytes` figure and no peak-memory figure, and it compares nothing
against C1's corpus.

#### Table 1 — two attribute-index reads over DIFFERENT keys

`{ .service.namespace = "prod" } | select(span.http.method)` sends a membership read and a value
read over one batch. **a1**, the membership read:

```sql
SELECT DISTINCT trace_id, span_id
FROM arch492p7.trace_attrs_idx
WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-18')
  AND (key = 'service.namespace' AND val = 'prod')
  AND timestamp_ns > 1699919999999999999 AND timestamp_ns <= 1700345598900000000
  AND trace_id IN (<the 32>)
```

**a1_nokey** — the same statement with its key predicate deleted. It is here as the instrument's
discrimination check, not as a form anything would send:

```sql
SELECT DISTINCT trace_id, span_id
FROM arch492p7.trace_attrs_idx
WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-18')
  AND timestamp_ns > 1699919999999999999 AND timestamp_ns <= 1700345598900000000
  AND trace_id IN (<the 32>)
```

**a2**, the `select()` value read:

```sql
SELECT trace_id, span_id,
       any(if(length(val) <= 8192, val, substringUTF8(val, 1, 2048))) AS v,
       any(val_type) AS t
FROM arch492p7.trace_attrs_idx
WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-18')
  AND key = 'http.method' AND scope = 'span'
  AND timestamp_ns > 1699919999999999999 AND timestamp_ns <= 1700345598900000000
  AND trace_id IN (<the 32>)
GROUP BY trace_id, span_id
```

**a3**, the join-free merge: one statement carrying both predicates as a disjunction, with a
presence count for the membership side and an `anyIf` for the value side:

```sql
SELECT trace_id, span_id,
       countIf(key = 'service.namespace' AND val = 'prod') > 0 AS matched,
       anyIf(if(length(val) <= 8192, val, substringUTF8(val, 1, 2048)),
             key = 'http.method' AND scope = 'span') AS v,
       anyIf(val_type, key = 'http.method' AND scope = 'span') AS t
FROM arch492p7.trace_attrs_idx
WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-18')
  AND ((key = 'service.namespace' AND val = 'prod')
       OR (key = 'http.method' AND scope = 'span'))
  AND timestamp_ns > 1699919999999999999 AND timestamp_ns <= 1700345598900000000
  AND trace_id IN (<the 32>)
GROUP BY trace_id, span_id
```

**a4_widened**, the form the design record rejected: the same merge with the two predicates widened
into `key IN (…)`, which throws the `val` prune away:

```sql
SELECT trace_id, span_id,
       countIf(key = 'service.namespace' AND val = 'prod') > 0 AS matched,
       anyIf(if(length(val) <= 8192, val, substringUTF8(val, 1, 2048)),
             key = 'http.method' AND scope = 'span') AS v,
       anyIf(val_type, key = 'http.method' AND scope = 'span') AS t
FROM arch492p7.trace_attrs_idx
WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-18')
  AND (key IN ('service.namespace','http.method'))
  AND timestamp_ns > 1699919999999999999 AND timestamp_ns <= 1700345598900000000
  AND trace_id IN (<the 32>)
GROUP BY trace_id, span_id
```

```text
                                                marks   rows read   rows out   wire bytes   stmts
a1    membership                                  226   1,851,392        260        6,289
a2    select values                               245   2,007,040        320       11,651
        today, the pair                           471   3,858,432        320       17,940       2
a3    merged as one disjunction                   471   3,858,432        320       11,985       1
a4_widened  merged, key IN (…)                    495   4,055,040        320       11,985       1
a1_nokey    a1 with no key predicate            1,225  10,000,000        320        7,729
```

`471 = 226 + 245` and `3,858,432 = 1,851,392 + 2,007,040`, exactly. **The disjunctive form keeps the
`val` prune** — the branch that has it is written out in full instead of being widened away — so it
reads exactly what the two statements it replaces read, in one statement, and costs 33.2% fewer
bytes on the metered hop. Widening to `key IN (…)` selects 495 marks instead: **that** is the form
the design record rejected, and it is not the only join-free one. This is the same phenomenon part 4
measured on the phase-1 generators (`148 = 25 + 123`, `traces_search_plan_parts.rs:48-54`),
reproduced one layer down.

#### Table 2 — two attribute-index reads over the SAME key

`| by(span.foo)` and a right-hand-side attribute operand send an aggregate `val_num` read and a
`select()` `val` read over one `(key, scope)` prefix. **t2_agg**:

```sql
SELECT trace_id, span_id, any(val_num) AS v, any(val_type) AS t
FROM arch492p7.trace_attrs_idx
WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-18')
  AND key = 'http.status_code'
  AND scope = 'span'
  AND isNotNull(val_num)
  AND timestamp_ns > 1699919999999999999 AND timestamp_ns <= 1700345598900000000
  AND trace_id IN (<the 32>)
GROUP BY trace_id, span_id
```

**t2_select**:

```sql
SELECT trace_id, span_id, any(if(length(val) <= 8192, val, substringUTF8(val, 1, 2048))) AS v, any(val_type) AS t
FROM arch492p7.trace_attrs_idx
WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-18')
  AND key = 'http.status_code'
  AND scope = 'span'
  AND timestamp_ns > 1699919999999999999 AND timestamp_ns <= 1700345598900000000
  AND trace_id IN (<the 32>)
GROUP BY trace_id, span_id
```

**t2_merged**, the two in one statement:

```sql
SELECT trace_id, span_id,
       anyIf(val_num, isNotNull(val_num)) AS n,
       any(if(length(val) <= 8192, val, substringUTF8(val, 1, 2048))) AS v,
       any(val_type) AS t
FROM arch492p7.trace_attrs_idx
WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-18')
  AND key = 'http.status_code'
  AND scope = 'span'
  AND timestamp_ns > 1699919999999999999 AND timestamp_ns <= 1700345598900000000
  AND trace_id IN (<the 32>)
GROUP BY trace_id, span_id
```

```text
                                                marks   rows read   rows out   wire bytes   stmts
t2_agg      aggregate values                      250   2,048,000        320       11,918
t2_select   select values                         250   2,048,000        320       10,307
              today, the pair                     500   4,096,000        320       22,225       2
t2_merged   merged into one statement             250   2,048,000        320       13,207       1
```

**Half the marks, half the rows, 40.6% fewer bytes, one round trip instead of two.**

#### Table 3 — the join, per batch

**t3_hyd**, the shipped hydration render over the same batch:

```sql
SELECT trace_id, span_id, parent_id,
       if(length(service) <= 8192, service, substringUTF8(service, 1, 2048)) AS service,
       if(length(name) <= 8192, name, substringUTF8(name, 1, 2048)) AS name,
       timestamp_ns, duration_ns, status_code,
       if(length(status_message) <= 8192, status_message, substringUTF8(status_message, 1, 2048)) AS status_message,
       kind,
       if(length(scope_name) <= 8192, scope_name, substringUTF8(scope_name, 1, 2048)) AS scope_name,
       if(length(scope_version) <= 8192, scope_version, substringUTF8(scope_version, 1, 2048)) AS scope_version
FROM arch492p7.trace_spans
WHERE trace_id IN (<the 32>)
  AND timestamp_ns > 1699919999999999999 AND timestamp_ns <= 1700345598900000000
ORDER BY trace_id ASC, timestamp_ns ASC, span_id ASC
LIMIT 10001 BY trace_id
```

**t3_join**, that statement as the left side and a2 as the right side:

```sql
SELECT h.trace_id, h.span_id, h.parent_id, h.service, h.name, h.timestamp_ns, h.duration_ns, h.status_code, h.status_message, h.kind, h.scope_name, h.scope_version, sel.v AS sel_v, sel.t AS sel_t
FROM (
SELECT trace_id, span_id, parent_id, if(length(service) <= 8192, service, substringUTF8(service, 1, 2048)) AS service, if(length(name) <= 8192, name, substringUTF8(name, 1, 2048)) AS name, timestamp_ns, duration_ns, status_code, if(length(status_message) <= 8192, status_message, substringUTF8(status_message, 1, 2048)) AS status_message, kind, if(length(scope_name) <= 8192, scope_name, substringUTF8(scope_name, 1, 2048)) AS scope_name, if(length(scope_version) <= 8192, scope_version, substringUTF8(scope_version, 1, 2048)) AS scope_version
FROM arch492p7.trace_spans
WHERE trace_id IN (<the 32>)
  AND timestamp_ns > 1699919999999999999 AND timestamp_ns <= 1700345598900000000
ORDER BY trace_id ASC, timestamp_ns ASC, span_id ASC
LIMIT 10001 BY trace_id
) AS h
LEFT JOIN (
SELECT trace_id, span_id,
       any(if(length(val) <= 8192, val, substringUTF8(val, 1, 2048))) AS v,
       any(val_type) AS t
FROM arch492p7.trace_attrs_idx
WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-18')
  AND key = 'http.method' AND scope = 'span'
  AND timestamp_ns > 1699919999999999999 AND timestamp_ns <= 1700345598900000000
  AND trace_id IN (<the 32>)
GROUP BY trace_id, span_id
) AS sel ON sel.trace_id = h.trace_id AND sel.span_id = h.span_id
```

**t3_no_alias** — the same statement with the two output aliases gone. It is here because the marks
and the rows do not move with the projection text while the byte figure does, by the eight
characters the header loses:

```sql
SELECT h.trace_id, h.span_id, h.parent_id, h.service, h.name, h.timestamp_ns, h.duration_ns, h.status_code, h.status_message, h.kind, h.scope_name, h.scope_version, sel.v, sel.t
FROM (
SELECT trace_id, span_id, parent_id, if(length(service) <= 8192, service, substringUTF8(service, 1, 2048)) AS service, if(length(name) <= 8192, name, substringUTF8(name, 1, 2048)) AS name, timestamp_ns, duration_ns, status_code, if(length(status_message) <= 8192, status_message, substringUTF8(status_message, 1, 2048)) AS status_message, kind, if(length(scope_name) <= 8192, scope_name, substringUTF8(scope_name, 1, 2048)) AS scope_name, if(length(scope_version) <= 8192, scope_version, substringUTF8(scope_version, 1, 2048)) AS scope_version
FROM arch492p7.trace_spans
WHERE trace_id IN (<the 32>)
  AND timestamp_ns > 1699919999999999999 AND timestamp_ns <= 1700345598900000000
ORDER BY trace_id ASC, timestamp_ns ASC, span_id ASC
LIMIT 10001 BY trace_id
) AS h
LEFT JOIN (
SELECT trace_id, span_id,
       any(if(length(val) <= 8192, val, substringUTF8(val, 1, 2048))) AS v,
       any(val_type) AS t
FROM arch492p7.trace_attrs_idx
WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-18')
  AND key = 'http.method' AND scope = 'span'
  AND timestamp_ns > 1699919999999999999 AND timestamp_ns <= 1700345598900000000
  AND trace_id IN (<the 32>)
GROUP BY trace_id, span_id
) AS sel ON sel.trace_id = h.trace_id AND sel.span_id = h.span_id
```

```text
                                                marks   rows read   rows out   wire bytes   stmts
t3_hyd      hydration, the same 32 ids            32     262,144        320       27,106
a2          select values                        245   2,007,040        320       11,651
              today, the pair                    277   2,269,184        320       38,757       2
t3_join     hydration LEFT JOIN a2               277   2,269,184        320       31,036       1
t3_no_alias the same, output aliases dropped     277   2,269,184        320       31,028       1
```

`277 = 32 + 245` and `2,269,184 = 262,144 + 2,007,040`, exactly. Same marks, same rows, 19.9% fewer
bytes, one statement instead of two — **per batch**. That is the join the design record's saving
would actually buy, and it is not the one the record documents.

#### Table 4 — the join per QUERY, which is the form the record documents

The design record's worked example puts the selector inline as a subquery so the whole request is
one statement. **t4_join**:

```sql
SELECT s.trace_id, s.span_id, sel.v AS sel_method
FROM arch492p7.trace_spans AS s
LEFT JOIN (
  SELECT trace_id, span_id, any(if(length(val) <= 8192, val, substringUTF8(val, 1, 2048))) AS v
  FROM arch492p7.trace_attrs_idx
  WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-18')
    AND key = 'http.method' AND scope = 'span'
    AND timestamp_ns > 1699919999999999999 AND timestamp_ns <= 1700345598900000000
  GROUP BY trace_id, span_id
) AS sel ON sel.trace_id = s.trace_id AND sel.span_id = s.span_id
WHERE s.timestamp_ns > 1699919999999999999 AND s.timestamp_ns <= 1700345598900000000
  AND (s.trace_id, s.span_id) IN (
    SELECT trace_id, span_id FROM arch492p7.trace_attrs_idx
    WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-18')
      AND key = 'service.namespace' AND val = 'prod' AND scope = 'resource'
      AND timestamp_ns > 1699919999999999999 AND timestamp_ns <= 1700345598900000000)
ORDER BY s.trace_id ASC, s.span_id ASC
LIMIT 20
```

**t4_control** — the same statement with the join block and the `sel.v AS sel_method` projection
removed, and nothing else changed:

```sql
SELECT s.trace_id, s.span_id
FROM arch492p7.trace_spans AS s
WHERE s.timestamp_ns > 1699919999999999999 AND s.timestamp_ns <= 1700345598900000000
  AND (s.trace_id, s.span_id) IN (
    SELECT trace_id, span_id FROM arch492p7.trace_attrs_idx
    WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-18')
      AND key = 'service.namespace' AND val = 'prod' AND scope = 'resource'
      AND timestamp_ns > 1699919999999999999 AND timestamp_ns <= 1700345598900000000)
ORDER BY s.trace_id ASC, s.span_id ASC
LIMIT 20
```

```text
                                    marks   exception_code   rows out   takes
t4_join    at 8589934592              721                0         20   3 of 3
t4_control at 536870912               476                0         20   3 of 3
t4_join    at 536870912               721              241          0   3 of 3
```

The extra 245 marks the joined form selects are exactly the whole `key='http.method' AND
scope='span'` prefix over the window, measured alone as a2's 245: the per-query form's right side
carries no candidate restriction at all. Bounding it is what makes the join per batch, which is
table 3.

#### Reproduces — the checks, with their takes

Every quantity here took the same value on every take. **A re-take that differs means the recipe is
wrong**: say so rather than editing the figure. Three takes of each statement on the one build named
above, unless the row says otherwise.

```text
build 81e37dde-8239-405e-a5ab-fddd4c68a5de
statement            marks   read_rows    rows out   takes
a1                     226   1,851,392         260   3
a1_nokey             1,225  10,000,000         320   3
a2                     245   2,007,040         320   3
a3                     471   3,858,432         320   3
a4_widened             495   4,055,040         320   3
t2_agg                 250   2,048,000         320   3
t2_select              250   2,048,000         320   3
t2_merged              250   2,048,000         320   3
t3_hyd                  32     262,144         320   3
t3_join                277   2,269,184         320   3
t3_no_alias            277   2,269,184         320   3
t4_join    at 8 GiB    721   (a record)         20   3, exception_code 0
t4_control at ceiling  476   (a record)         20   3, exception_code 0
t4_join    at ceiling  721   (a record)          0   3, exception_code 241

physical layout   trace_spans      2,000,000 rows / 5 parts / 255 marks / Wide
                  trace_attrs_idx 10,000,000 rows / 5 parts / 1,230 marks / Wide
per-date prod     364,000  356,000  360,000  360,000  360,000
identities        471 = 226 + 245          3,858,432 = 1,851,392 + 2,007,040
                  277 =  32 + 245          2,269,184 =   262,144 + 2,007,040
                  721 = 476 + 245
answer agreement  260 / 320 / 320 rows, all four symmetric differences 0
the witness       2 rows from a2's shape, 3 from a3's
the instrument    a `query_id` that does not exist returns 0 rows
```

**The identity alone is not the check**, which is why both sides of every row above are asserted.
On the uniform-90 corpus — pin (b) undone — the marks are different and the identity still holds;
that corpus was built and measured while the recipe was being pinned, and its figures are at the
three comment ids cited under the recipe. It was not rebuilt here.

`read_rows` on the three table-4 forms is in the record below rather than here. **Measured:** those
three differ take to take, while the eleven batch statements above repeated exactly. **Argued:**
the difference is `LIMIT 20` stopping the read at a thread boundary, which the batch statements have
no equivalent of, so they read every granule they select. What would falsify the argument is a
table-4 form whose `read_rows` repeats exactly over many takes, or a batch statement whose
`read_rows` moves.

#### Varies — the records, every take

These are published as their takes and are checked by nothing. **A take that differs is added to the
record; it is not a failure.** No bound, no range, no tolerance and no invariant is stated on any of
them.

Wire bytes, `wc -c` over `default_format=RowBinaryWithNamesAndTypes`, three takes each on the one
build named above. They agreed on all three takes here; that is what was observed, not a property
being claimed:

```text
build 81e37dde-8239-405e-a5ab-fddd4c68a5de
statement       take 1   take 2   take 3
a1               6,289    6,289    6,289
a1_nokey         7,729    7,729    7,729
a2              11,651   11,651   11,651
a3              11,985   11,985   11,985
a4_widened      11,985   11,985   11,985
t2_agg          11,918   11,918   11,918
t2_select       10,307   10,307   10,307
t2_merged       13,207   13,207   13,207
t3_hyd          27,106   27,106   27,106
t3_join         31,036   31,036   31,036
t3_no_alias     31,028   31,028   31,028
```

Table 4's `read_rows`, each take:

```text
build 81e37dde-8239-405e-a5ab-fddd4c68a5de
form                     take 1      take 2      take 3
t4_join    at 8 GiB     3,973,120   4,026,368   4,009,984
t4_control at ceiling   3,448,832   3,489,792   3,338,240
t4_join    at ceiling   2,551,808   2,592,768   2,699,264
```

`memory_usage` and `result_bytes` are records too, and this section publishes neither. The capture
statement returns both, in the columns of those names, for anyone who wants them; on this build
`t3_join` reported a different `memory_usage` and a different `result_bytes` on its first take from
the two after it, which is why a bound on either would be worthless.

The error body is a record beyond `Code: 241` and `512.00 MiB`. The three bodies at the top of this
section are one build's three takes of one statement: the "would use" figure takes two values across
them, the chunk figure takes two, and the execution site takes two — two takes failed in the
aggregate and one in a storage read, and only the storage-read take carries a part path. That part
path carries a container-local table UUID, so it cannot repeat across builds at all. That run's
stdout was not kept, so the block at the top of this section is the only copy of those three
bodies; the note beside them says so, and gives the statement they came from.

#### The answers agree

The merged form of table 1 must return the same answers as the two statements it replaces. This
statement inlines a1, a2 and a3 verbatim and counts the symmetric differences in both directions,
on the membership set and on the projection:

```sql
WITH
  a1 AS (
SELECT DISTINCT trace_id, span_id
FROM arch492p7.trace_attrs_idx
WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-18')
  AND (key = 'service.namespace' AND val = 'prod')
  AND timestamp_ns > 1699919999999999999 AND timestamp_ns <= 1700345598900000000
  AND trace_id IN (<the 32>)
  ),
  a2 AS (
SELECT trace_id, span_id,
       any(if(length(val) <= 8192, val, substringUTF8(val, 1, 2048))) AS v,
       any(val_type) AS t
FROM arch492p7.trace_attrs_idx
WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-18')
  AND key = 'http.method' AND scope = 'span'
  AND timestamp_ns > 1699919999999999999 AND timestamp_ns <= 1700345598900000000
  AND trace_id IN (<the 32>)
GROUP BY trace_id, span_id
  ),
  a3 AS (
SELECT trace_id, span_id,
       countIf(key = 'service.namespace' AND val = 'prod') > 0 AS matched,
       anyIf(if(length(val) <= 8192, val, substringUTF8(val, 1, 2048)),
             key = 'http.method' AND scope = 'span') AS v,
       anyIf(val_type, key = 'http.method' AND scope = 'span') AS t
FROM arch492p7.trace_attrs_idx
WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-18')
  AND ((key = 'service.namespace' AND val = 'prod')
       OR (key = 'http.method' AND scope = 'span'))
  AND timestamp_ns > 1699919999999999999 AND timestamp_ns <= 1700345598900000000
  AND trace_id IN (<the 32>)
GROUP BY trace_id, span_id
  )
SELECT (SELECT count() FROM a1)                                        AS a1_rows,
       (SELECT count() FROM a2)                                        AS a2_rows,
       (SELECT count() FROM a3)                                        AS a3_rows,
       (SELECT count() FROM (SELECT trace_id, span_id FROM a1
                             EXCEPT SELECT trace_id, span_id FROM a3 WHERE matched)) AS a1_minus_a3,
       (SELECT count() FROM (SELECT trace_id, span_id FROM a3 WHERE matched
                             EXCEPT SELECT trace_id, span_id FROM a1))              AS a3_minus_a1,
       (SELECT count() FROM (SELECT trace_id, span_id, v, t FROM a2
                             EXCEPT SELECT trace_id, span_id, v, t FROM a3))        AS a2_minus_a3,
       (SELECT count() FROM (SELECT trace_id, span_id, v, t FROM a3
                             EXCEPT SELECT trace_id, span_id, v, t FROM a2))        AS a3_minus_a2
```

It answered, `TSVWithNames`:

```text
a1_rows	a2_rows	a3_rows	a1_minus_a3	a3_minus_a1	a2_minus_a3	a3_minus_a2
260	320	320	0	0	0	0
```

The merged form's `matched` set is the membership statement's row set and its
`(trace_id, span_id, v, t)` projection is the value statement's, in both directions.

#### The ADR's rule sentence is wider than the decision it records, and this part does not change it

ADR 0008 is titled "SQL composition for lowered query pipelines" and its three rules govern how a
lowered pipeline composes. The rule added on 2026-09-02 is written without that qualifier — "no
emitted SQL may contain a join until this ADR is amended to name the clause"
(`docs/decisions/0008-sql-composition-for-lowered-pipelines.md:201`, the same claim in the summary
at line 11). Six committed goldens carry a join today and **none is planned by the compile core**:

- `traces_graph/clustered_local_join.sql` and `traces_graph/single_node.sql`, one join line each,
  from `service_graph_sql` (`crates/pulsus-read/src/traces/graph_sql.rs:92`, `INNER JOIN` at 109),
  called from `crates/pulsus-read/src/traces/exec.rs:1693` and nowhere else.
- `traces_metrics/compare_status.sql` and `traces_metrics/compare_status_window.sql`, seven join
  lines each. Six of the seven come from `metrics_compare_sql`
  (`crates/pulsus-read/src/traces/metrics_sql.rs:1189`, `LEFT JOIN` at 1253 and `INNER JOIN` at
  1257), which builds one string holding both joins and feeds it to the cross-tab and the probe,
  and which `metrics_plan.rs` calls three times (`:607`, `:619`, `:631`). The seventh comes from
  `metrics_compare_exemplar_range_sql` (`metrics_sql.rs:1380`, `INNER JOIN` at 1425, called at
  `metrics_plan.rs:646`).
- `traces_metrics_base/compare_status.sql` and `traces_metrics_base/compare_status_window.sql`,
  four join lines each. These are **historic**: each is byte-identical to
  `git show 2f78c53:crates/pulsus-read/tests/golden/traces_metrics/` at the same file name, they carry no
  exemplars section, and no test regenerates them. They are not unmoored from today's builder —
  `every_instant_side_section_is_byte_identical_to_base` ties two of their four join lines to the
  current file's bytes, and `the_declared_inverse_restores_every_moved_section_to_its_base_bytes`
  inverts the cross-tab section under three timestamp substitutions, none of which touches a `JOIN`
  line.

All six come from hand-written builders on routes the compile core classifies `Never` —
`NotASearchLinkLower::capability`, `crates/pulsus-read/src/traces/compile.rs:1246-1254` — so they
are not lowered pipelines and the decision never reached them. The sentence reaches further than the
decision it records: a drafting fault in the record, not shipped code breaking a rule. **The wording
belongs to the amendment round ADR 0008 already reserves.** This part records the fact, scopes its
gate to the compiled route's corpus, and pins the six by name so a seventh anywhere in the tree
fails — `no_planned_search_statement_contains_a_join`,
`crates/pulsus-read/tests/golden_sql_freeze.rs`, whose doc comment carries this same record beside
the list.

#### What this measures, and what it decides

**The projection cannot compile without a join for a query whose only attribute-index read is its
own; it can, join-free, for a query that already sends a second one — and that second thing is a
different mechanism, not `select()` lowering.**

`{ resource.service.name = "checkout" } | select(span.http.method)` sends four statements:
`trace_spans` (the generator), `trace_spans:hydration`, `trace_attrs_idx:values` and
`trace_spans:root`. Exactly one of them reads the attribute index, and it is the `select()` value
read itself. There is nothing to merge it with. The other three read `trace_spans`, and putting an
attribute value into a `trace_spans` statement means reading a second table inside one statement,
which is a join. `Relation` has no join slot (`crates/pulsus-read/src/compile/fold.rs:623`), so a
stage cannot contribute one without a type change, and ADR 0008 names no join clause. So part 7
records the refusal and lowers nothing.

That is a statement about the query, not about `select()` in general — which is why the refusal is
pinned by three tests rather than asserted here.
[`the_named_select_query_reads_the_attribute_index_exactly_once`](../crates/pulsus-read/tests/traceql_select_projection_refusal.rs)
fails if the named query ever grows a second attribute-index read, or loses one of the other three
statements. `exactly_one_committed_select_case_has_no_merge_partner`, in the same file, asserts both
name lists, so it fails in either direction: if the named case gains a merge partner, or if any of
the other six committed `select()` cases loses its. `select_refuses_and_names_its_reason_per_field`
(`crates/pulsus-read/src/traces/compile.rs`, in `mod tests`) pins the dispatcher itself, on both
seed sources and all six field spellings.

**And the join the design record documents does not survive the shipped ceiling**, which is the top
of this section. The form that does survive is the per-batch one, table 3, and that is one statement
per batch — not one per query.

#### The join-free merge is a different mechanism, and it is proposed later work on #492

Tables 1 and 2 measure a real saving — a third of the metered bytes on one shape, half the marks on
another — and it is **not `select()` lowering**. It is two phase-2 attribute-index reads sharing one
SQL part, and it applies to a query with no `select()` in it at all. It is proposed as its own part
on issue #492, after part 8, and it is not scheduled work until the owner schedules it.

Its three prerequisites, each with what a taker must read first:

1. **`plan_of`'s rule 2 must change.** Today every residual link with a handoff gets its own SQL
   part, unconditionally (`crates/pulsus-read/src/compile/plan.rs:543`, the
   `Disposition::Residual(_)` arm: "The evaluator's way of owning this link is to send a second
   statement: it gets its own SQL part, not an engine part"). Merging two of them means a rule
   saying when two handoffs over the same source and window share one part.
2. **The presence-count discriminator.** A bare `anyIf` maps "the span carries the key with an empty
   value" and "the span carries no such row" onto the same output row. `val_type` cannot tell them
   apart: migration 39 added it with `DEFAULT ''` and pre-existing rows read back `''`
   (`crates/pulsus-schema/src/catalog.rs:813`), and `StoredType::from_stored` maps `''` to `Unknown`
   (`crates/pulsus-read/src/traces/search_eval.rs:166`). The merged statement must carry
   `countIf(key = … AND scope = …) > 0` as its own column, which is what a3 does.

   Measured, on a five-row witness. Span `0000000A` carries `service.namespace='prod'` and
   `http.method='GET'`; `0000000B` carries only `service.namespace='prod'`; `0000000C` carries
   `service.namespace='prod'` and `http.method` stored as the empty string with `val_type='string'`:

   ```sql
   DROP DATABASE IF EXISTS arch492p7w;
   CREATE DATABASE arch492p7w;
   CREATE TABLE arch492p7w.trace_attrs_idx (
     date Date, key LowCardinality(String), val String,
     scope LowCardinality(String), val_num Nullable(Float64), timestamp_ns Int64,
     trace_id FixedString(16), span_id FixedString(8), duration_ns Int64,
     val_type LowCardinality(String) DEFAULT ''
   ) ENGINE=ReplacingMergeTree
   PARTITION BY date
   ORDER BY (key,val,scope,timestamp_ns,trace_id,span_id);

   INSERT INTO arch492p7w.trace_attrs_idx
     (date, key, val, scope, val_num, timestamp_ns, trace_id, span_id, duration_ns, val_type) VALUES
     ('2023-11-14','service.namespace','prod','resource',NULL,1700000000000000000,'T000000000000001','0000000A',1000000,'string'),
     ('2023-11-14','http.method','GET','span',NULL,1700000000000000000,'T000000000000001','0000000A',1000000,'string'),
     ('2023-11-14','service.namespace','prod','resource',NULL,1700000000000000000,'T000000000000001','0000000B',1000000,'string'),
     ('2023-11-14','service.namespace','prod','resource',NULL,1700000000000000000,'T000000000000001','0000000C',1000000,'string'),
     ('2023-11-14','http.method','','span',NULL,1700000000000000000,'T000000000000001','0000000C',1000000,'string');

   -- C1: a2's shape over the witness
   SELECT span_id,
          any(if(length(val) <= 8192, val, substringUTF8(val, 1, 2048))) AS v,
          any(val_type) AS t
   FROM arch492p7w.trace_attrs_idx
   WHERE key = 'http.method' AND scope = 'span'
   GROUP BY trace_id, span_id
   ORDER BY span_id;

   -- C2: a3's shape over the witness
   SELECT span_id,
          countIf(key = 'service.namespace' AND val = 'prod') > 0 AS matched,
          anyIf(if(length(val) <= 8192, val, substringUTF8(val, 1, 2048)),
                key = 'http.method' AND scope = 'span') AS v,
          anyIf(val_type, key = 'http.method' AND scope = 'span') AS t
   FROM arch492p7w.trace_attrs_idx
   WHERE (key = 'service.namespace' AND val = 'prod')
      OR (key = 'http.method' AND scope = 'span')
   GROUP BY trace_id, span_id
   ORDER BY span_id;
   ```

   The answer, `TSVRaw`, C1 then C2 — two rows against three:

   ```text
   0000000A	GET	string
   0000000C		string
   0000000A	1	GET	string
   0000000B	1		
   0000000C	1		string
   ```

   Under a2's shape span `0000000B` is absent. Under a bare `anyIf` merge it gains an `http.method`
   whose value is the empty string, which is a **different** answer and not a wider one: nothing
   downstream re-applies a presence test, because `ProjectionValue::SelectValue`'s only guard is the
   map lookup itself (`crates/pulsus-read/src/traces/search_eval.rs:2417`) and the merged form makes
   that lookup succeed. `0000000C` is the case that makes this a boundary rather than a rule about
   null: an attribute that IS present with an empty value must stay present. A test built on
   `val = 'GET'` against absent passes on a build that gets this wrong; the two values a test must
   use are the pair that differ by the least — `val = ''` with `val_type = 'string'`, against no row
   at all.
3. **The with-value membership arm's row-count decision.** `membership_sql`'s `with_value` arm
   projects `SELECT DISTINCT trace_id, span_id, v, t`, and its own documentation records that a span
   carrying one key at one text under two stored types yields **two** rows where the non-value arm
   yields one — "stated rather than guarded", because our ingest cannot produce it
   (`crates/pulsus-read/src/traces/search_sql.rs:275-285`). A merged form is
   `GROUP BY trace_id, span_id`, which collapses that to one row. Whether the collapse is accepted
   is a decision the later part must take itself: the arm came from #479 and its `val_type` column
   from #510, and both issues are closed, so nothing open owns it.

#### What this measurement cannot see

One node, one container, one build of one recipe, a warm page cache, and uniform-random trace ids.
Nothing here is a scale claim; the 1 TB behaviour is [#25](https://github.com/digitalis-io/pulsusdb/issues/25).
It cannot see the containers of earlier rounds, which are gone, so a quantity published there is
cited by comment id rather than re-measured. And nothing in this section observes a TraceQL request
end to end: what it measures is the SQL, and what pins the plan is the three tests named above.

## 10. Status: what is demonstrated, what is not

This document describes a design. Its evidence is uneven and the unevenness is the point of this
section.

**Wired, and labelled as wired (issue #492 part 3).** The plan object is no longer a specification:
`plan_search` builds one on every TraceQL search request and the executor consults it. What that
establishes is that the model can describe a real request — the part sequence the plan produces
equals the statement sequence each of the 56 committed goldens renders, case by case
(`traces_search_plan_parts::the_plan_sql_parts_match_the_sections_each_golden_case_renders`). What
it does **not** establish is that anything is faster or narrower, and it is not meant to: the
contract of that part is that **no SQL moves**, so that when a later part does move a statement, a
moved golden is unambiguously a defect in the renderer rather than an unattributable mix of the two.
Nothing about pushdown, pruning or bytes on the wire is settled by it.

**Four claims in this document were measurably false once the plan was built, and are corrected in
place rather than annotated.** The plan named the seed's table on every part after the first; the
phase-2 chunk was the rendering ceiling (24,998) instead of the batch the executor sends (32); a
regex-free multi-leaf selector was called `Equivalent` where its generator is a documented superset;
and the keyset page loop landed on the winners' root read, which is issued once. The first two show
on the explain surface the moment it is rendered, the third inverts `orig ⟹ sql` into `orig ⟺ sql`
and can DROP rows, and the fourth describes a loop that does not exist. §2.7.3, §2.7.4 and §2.7.9
carry the corrections.

**Demonstrated by measurement.** Everything in §9, on corpus C1: the per-stage decomposition, the
333-against-1,000 correctness consequence, the granule tables for groups 2 and 3, the pruning
rule, and ADR 0008's three composition measurements. These are counters from
`system.query_log`, load-independent, and re-runnable.

**Measured, refused, and no SQL moved (issue #492 part 7).** §9.8 measured whether a `select()`
projection can be compiled into the statements a TraceQL search already sends. It cannot, for the
query the scope enumeration names, whose only attribute-index read is the `select()` value read
itself; and the per-query join form the design record documents refuses at the shipped generator
memory ceiling, where the same statement with only the join removed succeeds. Part 7 lowers nothing,
changes no production line and moves no golden — what it ships is the record, plus three tests that
fail if the record's checkable sentences stop being true. Its figures come from one build of the
recipe §9.8 prints, on a container that no longer exists, and none of them is checkable in CI.

**Read from source and labelled as read.** The four hand-written boundary computations of §1 and
their call sites; the shape and residual-effect columns of §3.1 and §7.1; the `Never` reasons of §5;
the response channels of §8. Reading gives the rule and cannot give the size of an effect, which is
why every cost claim is a measurement instead.

**Run against the shipped planner, and labelled as run.** Every `400` body in §3.1's and §7.1's
payload-rejection tables was produced by calling `plan_search` and `logql::plan::plan` on this tree
at `2f78c53` and printing the error — not transcribed from the `format!` strings, and not observed
on the wire: the status code, the content type and the two headers are **read** from
`crates/pulsus-server/src/traces_api/error.rs:270-304` and
`crates/pulsus-server/src/logs_api/error.rs:147-212`. The same run established which arms are
reachable at all: **three** of the twelve TraceQL arms and **four** of the fourteen LogQL arms are
shadowed by the parser or by `pulsus_traceql::validate`, which no reading of the planner would have
shown. The TraceQL count was four until this revision, when the input that defeats the fourth was
constructed (§3.1); the LogQL count was written as "three of ten" against a table that had already
become fourteen rows with four shadowed (§7.1).
`unwrap_vector_aggs`' behaviour at `label_replace`, and the routing consequence, are from the same
run.

**Compiled here.** §2.5's fold, together with §2.2's `Lang`/`Lower`/`Capability`/`Relation` and
§2.3's `ColSet`, builds and runs under `rustc --edition 2021`, and the residual rule is an
assertion in that program rather than a sentence in this one. **What that establishes and what it
does not:** the instantiation is a four-link LogQL-*shaped* one written for the probe, not the real
`LqlLink` over the real AST, so it establishes that the fold's contract type-checks and that a
refused link's state effect reaches the next link — not that thirteen real links fit. The second
claim is still #507's, still against a transcription, and still open below. The previous version of §2.5 **did
not compile** (`E0026`, then `E0004` on the missing `Never` arm), which means the mechanism this
document rests on — a refused link still applying its state effect — was unreachable as written.
Two deliberate breaks show the check discriminates. Both the output and the break results are in
§2.5.

**Disproved by compiling — the two-language fit did NOT hold as first written.** The previous
version of this section said the fit was "not established" and warned that an interface fitting on
paper might need widening the moment a real implementation was written.
[#507](https://github.com/digitalis-io/pulsusdb/issues/507) transcribed §2's interface verbatim
into a throwaway crate depending only on `pulsus-logql` and wrote the LogQL link set against it.
It did need widening, in five places:

| finding | what broke | repair, now in this document |
|---|---|---|
| **R1** | `Vec<OpenSource>` names a trait as a type — `E0782` | `Vec<Arc<dyn OpenSource>>`; not `Box`, because `Shape: Clone` forces `ColSet: Clone`, and `Arc` rather than `Rc` because the plan crosses an `.await` (§2.3) |
| **R2** | `Clone`/`Eq`/`Debug` cannot be derived through `dyn` — `E0277` ×4, `E0369` | `OpenSource` gains a `Debug` supertrait and `id()`; `ColSet`'s `PartialEq` is hand-written (§2.3). **Not a LogQL accommodation** — the TraceQL sketch clones `cols` too and hits the same wall |
| **R3** | the dispatcher never received the stage, so no payload could reach it; and `Drop`/`Keep` share the payload type `Vec<DropKeepElem>`, so per-payload impls collide — `E0119` | `capability`/`apply` take `&L::Stage` (§2.2) |
| **R3b** | a `&'static` dispatcher needs `Self: 'static` — `E0310` | bound added on `lower_of` and the fold (§2.2, §2.5) |
| **R5** | the fold returned at the first refusal, which is a **measured regression** against shipped behaviour | the fold is a per-link disposition, not a prefix (§2.5, §9.6) |

With R1–R3b applied, #507 reports the full ten-`Stage` link set — `Unwrap` among the ten, not
beside them — plus `Window`, `RangeAgg`, `VectorAgg`, `LabelReplace`, `Order`, `Limit` and `Emit`
compiling against the core with **no further change**. So the interface is now
**fitted against a second language by a compiler**, which is a materially stronger statement than
the one this section used to make — and it was reached by the check this section demanded rather
than by anyone reading their own code back.

**R5 is the one that matters, and it is not a type error.** It is the design being wrong about
what a pipeline is. Four of the five findings would have cost a coder an afternoon; R5 would have
shipped a 20.6× byte regression on an ordinary three-filter query, and been noticed — if at all —
as "queries got slower".

**Corrected in this revision, and listed so the corrections are reviewable as corrections.** The
`Never`-arm break exits **1**, not 101 (§2.5) — a stated exit code that had never been re-run.
§3.1 and §7.1 gained payload validation ahead of the fold, because without it a disposition could
have carried a query the shipped planner refuses with `400`. §7.1's chain gained `LabelReplace`,
which its own table already carried; its builder derivation was replaced, because
`unwrap_vector_aggs` breaks at that construct and a query containing one produces no `MetricPlan`
at all; and its `LabelReplace` row's "removes no series" was replaced by the measured 4-series-to-1
range collision. The boundary diagram's pipeline D was drawn to the end of its chain and its
enumeration caption stopped calling a transcription "the shipped function". §11.2b now nominates
two gates covering all 28 effects, replacing what an earlier revision of that section nominated;
this section states no count for that revision and defers to §11.2b, which records that no retained
artefact contains it —
`logql::compile::tests::every_residual_state_effect_is_the_one_the_document_states` and
`traces::compile::tests::every_residual_state_effect_is_the_one_the_document_states`, both **wave 1**
and neither at base — and §11.5's "no compile-failure harness exists" was false and is gone.

**Corrected again in this revision, and the first three are the same defect at three sizes.**

1. **The document claimed that §11 gates ran when they do not exist.** §11.5 said the compile-failure item —
   §11.3's `every_lql_link_variant_has_a_row_in_the_lowering_document`, **wave 1** — "became a
   real gate", that adding a link variant "fails to build that binary … so the property is enforced
   on every CI run", and headed itself "and that IS gated". The binary does not exist and every
   selector naming it exits **101**. §11.0 is new and states the rule that prevents it: every gate
   claim carries a measured count or the wave that writes it, nothing carries neither, and the
   whole set is listed with its measured `Starting N test…` line. At that revision four of §11's
   gates existed and eighteen did not; the current totals are in §11.0.
2. **A clause withdrawn in one place was still asserted in another.** §2.2's `lower_of` doc comment
   said an added variant "fails to compile here **and nowhere else**" while §11.5 withdrew exactly
   that clause. Deleted at its source. The other four withdrawals in this document — the
   `pub(super)` placement, the 16,598× ratio, the "removes no series" row, and the diagram caption
   calling a transcription the shipped function — were each re-checked for a surviving assertion
   and none has one.
3. **Two reachable LogQL rejections were missing, and the method that missed them is replaced.**
   `plan.rs:1227` (a vector aggregation over a bare scalar literal) and `plan.rs:1392`
   (`label_replace` over a scalar operand) are both reachable today. §7.1's table is now derived
   from a literal scope — every `ReadError::` construction in `plan.rs` above `mod tests`, 25 sites
   — with each site either a row or an excluded site with its reason, and every body and every
   reachability verdict produced by running a query rather than by reading a `format!` string. It
   went from 10 rows to **14**, and it also corrected two `file:line` citations and split one
   helper with four distinct bodies.
4. **Five residual-effect rows stated a literal shape where they meant preservation**, and every one
   of the five named the stage's usual *input* shape — so `Aggregate` "stays `Spans`" while the
   boundary diagram invokes it on `Groups`, and `RangeAgg` "stays `Samples`" while its own `Unwrap`
   row describes the `Lines` case. All five now say **shape unchanged**. The gate that would have
   frozen them is fixed too, before **wave 1** writes it: §11.2b's rows carry **two** seeds, not
   one.
5. **The hops diagram asserted three things the prose did not** — a specific vendor's datasource as
   the client, a `pulsus-server` resident-memory figure, and "every one of the 554 batches" where
   §9.2 said 553 at the time. The client label is generic, the memory figure is in §9.2, the batch
   count matches §9.2, and §11.3 records that none of the gates it nominates would have caught any of
   the three.

**Corrected in the revision after that, and four of the five are one defect: a check whose domain
was smaller than the claim it was asked to support.**

1. **The sweep behind §11.0's "0 unbacked" was blind to five verbs and to its own unit.** It worked
   on blank-line blocks, so a table cell backed a whole paragraph run and a code fence was one unit,
   and its hand-written verb list had no *fails to compile*, *asserts*, *proves*, *establishes* or
   *compares*. Five current-tense gate claims sat outside the 22-gate table where it could not see
   them. Its replacement, a sentence-unit two-tier sweep, was beaten twice more — by a ninth claim
   found only in its printed residue, and by a seventh constructed on demand — so **§11.0 no longer
   keys on the verb at all**: it enumerated the inventory as it then stood — 22 gates, four
   measured and eighteen **wave 1** — and
   audits every mention of one, which is verb-independent, tense-independent and
   phrasing-independent. The verb sweep is kept as a
   backstop, because reading its residue is what found three claims no verb list contained.
2. **§3.1 said a non-finite numeric threshold was parser-shadowed. It is not.**
   `{ .service.namespace = "prod" } | max(.a) > <320 nines>` parses, validates and reaches
   `search_plan.rs:1046`. The row is corrected to **reachable**, and **every other shadowing claim
   in this document was re-checked by constructing the input that would defeat it** rather than by
   reading the lexer — four TraceQL rows with three to ten spellings each and four LogQL rows with
   three to eight. The other seven held. Two stale counts fell out of it: §10 said "four of the
   eleven TraceQL arms and three of the ten LogQL arms" where the truth was three of eleven and four
   of fourteen. (The TraceQL table has since gained a twelfth arm — §3.1's `Filter` row, issue #492
   item 9 — and it is reachable, so §10 now reads three of twelve.)
3. **Three of the 22 gates then in the inventory had no seed-provenance row, and the diagram row
   said "two" where there are three.** §11.0b now has a row for every gate, each cell parenthesises
   how many gates it covers, the counts sum to the inventory total, and every row repeats §11.0's
   at-base state — so the two tables check each other instead of one being the other quoted twice.
4. **The hops diagram carried three MORE picture-only assertions**, found only when the method
   changed from "look for figures" to "enumerate every text node in both files". §11.3 states the
   method and the count.
5. **§9.2's phase-2 total was exactly the per-read unit times the batch count**, which is what a
   measured total and a derived total both look like, so it could not corroborate the per-read
   figure beside it. The claim was weakened to "553 of the 554" and the total was labelled as not
   independent evidence. **Part 8 discharged the re-measurement it owed:** §9.2 is now computed
   from one retained `system.query_log` row per statement
   ([`docs/benchmarks/data/traces-lowering-92.json`](benchmarks/data/traces-lowering-92.json)), all
   563 membership reads agree to the byte, and the flag is gone.

**Changed by the plan-object revision, and none of it is compiled.** The compiler's output became a
**plan** rather than a relation-plus-dispositions (§2.7.1); the four cuts, the three must-not-cut
rules and `Fidelity` were added (§2.7.2–§2.7.7); `should_lower` and `ResidualReason::Policy` were
deleted and three fact-suppliers put in their place (§2.2); §3.1's, §5's and §7.1's link tables
gained a **continuation** column; §6's cost row became a plan-shape row; §8's cap table is keyed on
the SQL part; and the core module is `compile/`, not `lower/`, by owner ruling. **Everything named
in that sentence was written after the build recorded above and has been compiled by nothing.** The
probe sources are gone from the machine, so the three transcripts in §2.5 cannot be re-run as they
stand — they are quoted for what they printed against the earlier interface, and re-establishing
them against the interface as it now reads is a **wave 1** obligation.

**What this revision re-took, and what it did not.** **Re-taken at `acf44c49`:** every
`Starting N tests` line and every exit code in §11, with `cargo-nextest 0.9.143`, including the
whole-set absence check and its two-name control — the workspace holds 6,949 tests there against
6,851 at `2f78c53`, and `pulsus-read`'s lib 1,208 against 1,190. **Not re-taken:** the gate-mention
checker's buckets, which are the round-15 run over a 22-row inventory and whose scripts are gone
(§11.0); the six rustdoc `compile_fail` fences of §11.5; and the payload-rejection bodies of §3.1
and §7.1, the shadowing probes, and §9's corpus measurements, all of which stay dated to `2f78c53`
in the sentences that carry them.

**Citations.** Every `file:line` in this document was printed and read at `2f78c53`. The citations
§2.7, §5, §6 and §8 introduce or move were re-printed at `acf44c49`; **the rest were not**, and
several are known to have drifted — `search_plan.rs`'s `generator_sqls` from `:456` to `:594`, its
per-disjunct append from `:1681` to `:2157-2158`, `search_eval.rs`'s `apply_post_stages` from
`:2464` to `:2498`, `exec.rs`'s `BATCH_TRACES` from `:114` to `:115`, and `search_sql.rs`'s root
read from `:345` to `:361`. **Wave 1 owes a re-print of every citation at the commit it lands on.**

**Still not established.** #507's crate is outside the tree and compiles against a transcription,
not against `crates/pulsus-read/src/compile/`. The core does not exist yet, so nothing has compiled
the repaired interface **and** a real language implementation in one build. That is what wave 1
closes, and until it does this section stays as it is.

**Both diagrams carried the same false assertion, and both are redrawn.** A picture asserts a design
without being read as a claim, which is why §11.3 gates them — and why a stale one is worse than a
stale paragraph. The defect: the boundary diagram's legend read *"RESIDUAL — no SQL, but still
applies its state effect"*, and its pipeline-A caption read *"The evaluator runs link 5: the
winners' root read"*. Under §2.7.2 that link is residual **and** is served by a second SQL part, so
the legend was false for `Emit` and the caption misdescribed what the request does — precisely the
defect the plan object exists to correct, sitting in the picture that teaches the design.

What changed, so the redraw is reviewable as a redraw:

| artefact | before | after |
|---|---|---|
| boundary legend | two entries; the residual one read *"no SQL, but still applies its state effect"* | **three** entries: that one now reads *"no SQL **here**, …"*, and a new blue dashed one reads *"RESIDUAL, and served by its OWN SQL part — a cut"* |
| boundary panel A, link 5 | amber `res` box, subtitle *"root read is unwindowed"* | blue `res2` box, subtitle *"cut: SourceHandoff"* |
| boundary panel A caption | *"The evaluator runs link 5: the winners' root read…"* | *"part 0 yields Reduced … seeding part 1 … Link 5 is residual AND is part 1 — the evaluator does not run it."* |
| boundary panels B, C | *"exact was cleared by link 2"* / *"all asked, all residual"* | *"…; Emit cuts"* on both, plus one line: `Never` says a link cannot join **this** statement, not that no statement serves it |
| boundary panel D | *"residual"* | *"residual, no cut"* — LogQL's `Emit` opens no second statement, and the caption now says so |
| hops panel title | *"one lowered statement, plus the winners' root read"* | *"a plan of 2 SQL parts, the second seeded by the first's 20 trace ids"* |
| hops caption / `<desc>` | *"2 statements."* / *"stays residual in every chain"* | *"2 SQL parts."* / the same clause the legend gained |

**No number moved in part 4's edit.** The hops diagram's counters and the 553-of-554 batch note
were left exactly as they stood, and so was every box and arrow position; the boundary diagram went
from 87 text nodes to 90 (one legend entry, two caption lines). Both files parse as XML. **Part 8
then redrew the hops diagram from the retained rows**, so none of those counters survives; the
boundary diagram is untouched by part 8.

**The table above is a record of two past edits.** Part 4 (issue #492) marked the hops diagram and
moved no number: the canvas grew from 620 to 764 px, the ratio panel moved down 144 px to make room
for the marker, every existing `<title>`, `<desc>` and `<text>` node kept its content, and `<desc>`
gained a superseded sentence. **Part 8 then redrew it** against §9.2 and §9.2b: the marker node and
every figure it named are gone, the canvas is back to 620 px, and
`the_hops_diagram_and_the_document_agree_on_the_lowered_request` now compares the drawing's lowered
round-trip count and result-byte total against §9.2b's rather than leaving them to be read.

**The three diagram gates are still wave 1 and are still owed** —
`the_hops_diagram_and_the_document_agree_on_the_lowered_request`,
`the_boundary_diagram_names_only_links_the_document_defines` and
`every_boundary_diagram_pipeline_carries_the_three_synthesised_links`, none of which exists at base;
each selector exits 101 against a test target that does not exist (§11.0). **None of the three would
have caught what was just corrected**, and that is the point of recording it: they compare the
lowered round-trip count, the result-byte total, the link labels and the three synthesised links,
and this defect was in a legend and two captions. A fourth gate that would have caught it — one
asserting that every link the document gives a **continuation** is drawn in the class that says so —
is what `every_chain_link_row_states_a_continuation` becomes if it is written against the diagram as
well as the tables. Wave 1 should write it that way.

**What the first implementation wave settles.** It delivers the core **with R1–R3b and R5 already
applied** — they are not wave-1 discoveries — and the TraceQL aggregate as its first lowered link.
It also lands the LogQL `Lang` impl and link set **compiled and unwired**: not called from
`plan.rs`, no LogQL SQL emitted, no LogQL behaviour changed. The gates that make that worth having
are §11, by name and by selector — four of them exist and the other twenty-one are **wave 1**.

**That re-measurement has landed.** §9.2's phase-2 membership row used to state a total that was
exactly the per-read unit times the batch count, which is what both a measured and a derived total
look like, so the total could not corroborate the per-read figure beside it. Part 8 re-measured the
phase-2 loop on C1 and **retained one raw `system.query_log` row per statement** —
[`docs/benchmarks/data/traces-lowering-92.json`](benchmarks/data/traces-lowering-92.json), 1,132
rows, no summaries — so the per-read unit and the phase total are now two readings of the same
evidence. All 563 membership reads selected 1,105 granules and read 9,052,160 rows, so the "553 of
the 554" is replaced by a statement over every batch. The drawing carried the same two claims in a
text node of its own and is redrawn from the same artefact. Three of its text runs used to overflow
the canvas — measured at `right` 1255.6, 1276.2 and 1344.7 against a canvas of 1120 — and the
redraw is where that was fixed.

**Two behaviours change with the chain, and both were measured rather than ruled on.** Corpus
**C3**: one trace, four spans, three named `a` and one named `b`, under one resource
`service.name`, pushed as identical OTLP/JSON to both sides. The query uses **physical columns
only** (`service` and `name`), so it exercises no attribute storage. Both sides queried at the
same window with `limit=20`.

**1. Pipeline order — the reference honours it, we ignore it. This is a defect on our side, and
the chain fixes it.** Not a divergence we would be introducing: a divergence is what it would have
been had the reference ignored order too.

| | `\| by(name) \| count() > 2` | `\| count() > 2 \| by(name)` |
|---|---|---|
| **PulsusDB** | 2 spanSets, 4 spans, `[by(name)=a]` `[by(name)=b]` | 2 spanSets, 4 spans, **identical** |
| **the reference** | **1 spanSet, 3 spans**, `[by(name)=a, count()=3]` | **2 spanSets, 4 spans**, `[count()=4, by(name)=a]` `[count()=4, by(name)=b]` |

The reference distinguishes the two orders three ways at once: the number of spanSets, the
`count()` **value** (3 computed per group against 4 computed per trace), and the **order of the
spanSet attributes**, which records the execution order directly. We return the same answer to
both spellings, because the aggregate loop (`search_eval.rs:2420`) always runs before
`apply_post_stages` (`search_eval.rs:2464`) — the bucket representation of §2.1 showing through.

So this document is describing a **correction**, and the implementing wave must land it as one —
with its own test and its own changelog line — rather than letting it arrive silently inside an
architecture change, where nobody reviews it as a fix.

**An adjacent divergence surfaced in the same four requests and is not this document's to fix:**
the reference attaches a `count()` attribute to each spanSet (and appends `by(…)`/`count()` in
execution order); we emit only `by(…)`. That is a response-shape gap independent of ordering, and
it is reported on [#492](https://github.com/digitalis-io/pulsusdb/issues/492) rather than absorbed
here.

**2. `partial` — accepted, and it moves us toward the reference rather than away.** The reference
reports `completedJobs == totalJobs` on every shape measured on C3 — plain filter, the aggregate
shape, an aggregate matching nothing, `by()`-then-`count()`, and `limit=1` — so it calls these
queries **complete**, and it has no candidate-cap analogue to report.

Our signal is our own: with `reader.traceql_max_candidates` set to 2 against 5 matching traces,
our response returns 2 traces and **omits `completedJobs`**. The answer a user asked for was never
incomplete; our intermediate candidate list was truncated. Lowering removes the intermediate step,
so the response says complete, which is both true and what the reference already says.

**Client-visible change, to be recorded in [api.md §4.2](api.md) by the wave that ships it:**
lowered queries return `metrics.completedJobs = 1` where the two-phase path omitted it, so a
client sees fewer partial answers. Nothing that consumed the old signal breaks — a client reading
`completedJobs < totalJobs` as "incomplete" simply sees fewer such responses, and none of them
was ever an incomplete *answer*.

**Not covered anywhere in this document.** Single node, one corpus shape, warm page cache; no
cold-start figure and no cross-shard figure. The shard-to-shard hop is out of scope by owner
ruling and is neither measured nor estimated. Behaviour at 1 TB is
[#25](https://github.com/digitalis-io/pulsusdb/issues/25).

---

## 11. The tests this design nominates

A design that names behaviours and no test selectors cannot be checked before the code exists. Each
gate below is named with the **`cargo nextest` selector that selects it**, its binary, and two
states: what it printed **at base** — commit `acf44c49`, a measurement that stays as one — and what
the tree says **today**.

> **Part 8 re-derived the `today` column, and it is why that column exists.** At base four of the 25
> gates ran and twenty-one were wave 1, and this section stated that in a dozen places. It went on
> stating it: **27 of the 28 selectors the record names now resolve to a definition in the tree**,
> and the one that does not is `no_such_test_name_at_all_zzz`, this record's own negative control.
> `every_gate_the_record_names_exists_or_is_marked_absent` reads the `today` column and the tree and
> fails when they disagree, so the state cannot go stale again without something saying so. The
> `at base` column is untouched: it was a measurement at a named commit and it is still true of that
> commit. **The prose below is written in the tense of that measurement**; where it says "wave 1
> writes this", read it as what was owed at base, and read the `today` column for what happened.

**Two things about the selector form, both measured on this tree at `acf44c49` with
`cargo-nextest 0.9.143`.** An integration test's function name carries **no module prefix** and is
selected by its bare name. A **lib unit test** needs its **full module path**: the bare name selects
zero. And a selector that matches nothing **exits 4** while printing `Starting 0 tests` — where
`cargo test` would exit 0 and print green — so a gate that has not been written fails
loudly rather than passing quietly.

**Both blocks below are what the commands in them printed**, re-run on this tree at `acf44c49`
with `CARGO_TARGET_DIR` outside the source tree. The `grep` is part of each command rather
than applied afterwards, and `pipefail` is what makes the reported `exit=` `cargo`'s and not
`grep`'s — without it the second command reports `exit=0`, because the `grep` matched.

```
$ set -o pipefail; cargo nextest run -p pulsus-read --lib -E 'test(=traces::exec::tests::search_settings_pin_the_layer_1_budget_contract)' 2>&1 | grep -E '^ +Starting|^error'; echo "exit=$?"
    Starting 1 test across 1 binary (1207 tests skipped)
exit=0

$ set -o pipefail; cargo nextest run -p pulsus-read --lib -E 'test(=search_settings_pin_the_layer_1_budget_contract)' 2>&1 | grep -E '^ +Starting|^error'; echo "exit=$?"
    Starting 0 tests across 1 binary (1208 tests skipped)
error: no tests to run
exit=4
```

A third form is worth stating because it fails in a different place: `--lib` against a crate with
no library target (`pulsus-server`) does not select zero tests — it fails at **target selection**,
printing `error: no library targets found in package \`pulsus-server\`` and exiting **101**. So does
`--test <name>` naming a test binary that does not exist (§11.3). Exit 4 means "the selector matched
nothing"; exit 101 means "the thing you selected from does not exist". Two different failures, and a
gate that has not been written must produce the first, not the second.

**Do not read the selection count out of `nextest list --message-format json`.** Its `test-count`
field is the **binary's** test count, not the selection's: with the zero-matching selector above it
reports `1208`, the same number the run prints as *skipped*. **And do not use `list` to establish an
absence at all**: measured at `acf44c49`, `cargo nextest list` exits **0** on a selector matching
nothing, where `run` exits 4 (§11.0). The empty-selection guarantee this document relies on belongs
to `run`, and every reading here is from `run`.

**Read-only configuration noise from unrelated tooling.** In some sandboxes every `cargo`,
`rustc` and `nextest` invocation here is preceded by an unrelated tool's read-only-filesystem
warning about its own config file, before any output of its own. It is not this workspace's, it
changes no test membership and no exit code, and it must not be read as a failure — check the
`Starting N tests` line and the exit code, which are the only two things this document reads off a
selector.

### 11.0 Every gate claim carries a measured count or a wave

**A gate that exists is named with its selector and the `Starting N test…` line that selector
prints today. A gate that does not exist yet says so in the same sentence and names the wave that
writes it. No sentence in these four artefacts says a check is enforced, became a gate, is caught,
or fails a build unless running it today produces that result.** These artefacts name **25** gates: **four exist and
twenty-one are wave 1**. The four were run at `acf44c49` — three print `Starting 1 test` and the live
`query_log` binary prints `Starting 14 tests` — and the twenty-one **do not exist**. The table below gives
the measured count or the wave for every one. The `search_settings_pin_…` pair below it is a
demonstration of selector *form*, not a gate.

This rule exists because an earlier revision broke it three times in one section. §11.5 said the
compile-failure item — §11.3's `every_lql_link_variant_has_a_row_in_the_lowering_document`, **wave 1** —
"became a **real gate**", that adding a link variant "**fails to build that
binary** … so the property is enforced on every CI run", and headed itself "and that IS gated" —
while `crates/pulsus-read/tests/query_lowering_doc_gate.rs` does not exist and every selector
naming it exits **101**. Specifying a gate that does not exist yet is correct in a design;
**claiming it exists is not**, and the two readings are a few words apart.

**Auditable in one pass, and the audit is keyed on the gate identifier, not on the verb.** **Four**
mechanisms were written for this rule before the one described below, and all four were blind:
three verb sweeps, then one that keyed on the subject through a designation vocabulary. The first
reported "16 gate-claiming blocks, 0 without evidence" and was wrong two ways at once, both of them
the defect the rule exists for — a claim checked against a smaller domain than the one it names:

- its **unit** was the blank-line block, so a table's `at base` cell counted as evidence for every
  sentence in the same paragraph run, and a whole fenced code block was one unit; and
- its **verb list** was hand-written, and did not contain *fails to compile*, *asserts*, *proves*,
  *establishes* or *compares* — five verbs carrying five current-tense gate claims that sat outside
  the 22-gate table and were therefore invisible to it.

Its replacement moved to the sentence and widened the verbs. That one's **own first version** caught
only four of the six sentences it was written to catch; its second version missed a ninth claim,
found only by reading what it had *not* flagged; and a reviewer then constructed a **seventh**
unflagged claim on demand. **A verb list cannot enumerate the ways English asserts that something
runs, and it fails open** — a verb outside the list is passed in silence, so each round produced a
new spelling and a new patch to the list.

**So the audit keys on the gate's own identifier, because the identifiers are enumerable and the
ways English can qualify a noun are not.** The inventory holds exactly **25** rows — four that
exist and twenty-one that are **wave 1** — and the six tables in §§11.1–11.4 derive them. **§11.4 carries one further row that is not one of the 25**: `no_planned_search_statement_contains_a_join`, added by issue #492 part 7, which exists and prints `Starting 1 test` on this tree. It is marked as such where it stands, and every count in this section and in §11.0b is of the 25. **It held
22 when the checker below was last run**, and the three added since are §11.3's
`every_cut_variant_has_a_row_in_the_design_record`, `every_chain_link_row_states_a_continuation`
and `the_plan_shape_json_keys_match_the_api_document`, all **wave 1** and none of them at base.
`492-r10-gate-mention-check.py`, in the architect's session scratchpad, finds **every mention of a
gate anywhere in these four artefacts** — which is verb-independent, tense-independent and
phrasing-independent — and asks two questions of each:

1. **Accounting.** Which of the 22 does this mention denote? A mention that denotes none is a
   failure, whatever verb the sentence uses.
2. **Agreement.** Does the sentence agree with those rows' `at base` status? A unit mentioning a
   gate that is **wave 1** must carry the wave or the at-base evidence in the same unit; one
   mentioning a gate that **exists** must carry its measured count or exit code. A unit mentioning
   both kinds is checked for the **wave** evidence only, which is limit 5 below.

**A designation vocabulary was tried for the accounting question, and it is gone because it
failed.** An earlier revision mapped 41 prose designations — *diagram*, *effect*, *wave-1* and
thirty-eight more, each of them a modifier of the head noun — onto inventory rows, matching
wherever the designation ended at that noun. A
review then wrote **"The diagram gate for colour semantics passes today; wave 1 only documents
it"** into §11.3 and the checker exited **0**: the designation `diagram gate` matched, and
§11.3's three real diagram rows — `the_hops_diagram_and_the_document_agree_on_the_lowered_request`,
`the_boundary_diagram_names_only_links_the_document_defines` and
`every_boundary_diagram_pipeline_carries_the_three_synthesised_links`, all **wave 1** — absorbed a claim
about a **fourth** gate that does not exist, while the words *wave 1* four tokens later supplied
the evidence. The defect is not the entry list, and lengthening it
would not have helped. **A modifier that follows the head noun narrows the subject, and English
post-modification cannot be enumerated any more than the verbs could** — so a checker that reads
only as far as the head noun will go on mapping an unknown subject onto the nearest known one.

**So a mention that qualifies the noun must name its subject by identifier.** Each mention is
classified in this order — the compound rule reads the word to the **right** of the mention, and the
two rules below it read only its left context:

- **compound** — the word modifies another noun (*gate claim*, *gate table*, *gate row*, *gate
  noun*, *gate count*, *gate cell*, *gate inventory*, *gate identifier*, *gate list*, *gate
  selector*, *gate mention*, *gate accounting*, *gate state*, *gate sweep*, *gate word*, *gate
  evidence*, *gate vocabulary*, *gate claiming*), or is `env-gated`, which is the other sense of the
  word. Not a reference to any of the 22.
- **generic** — every word between the mention and the nearest preceding determiner is closed-class
  (*the, this, that, these, those, both, a, an, any, such, another, every, each, all, no, its,
  their, same, other, old, new, only, own, first, second, third, last, next, following*, a numeral,
  a possessive, or a `§` anchor). The mention denotes the inventory rows its own section owns, plus
  those of any section it cites by number. Resolving to no row at all is `UNRESOLVED`, and a
  failure.
- **qualified** — anything else: a content word modifies the noun, so the mention picks out
  something in particular. It resolves **only** by an exact, word-bounded occurrence of one of the
  22 inventory identifiers **in the same unit**, and to exactly those rows. A qualified mention
  whose unit names no identifier is `UNACCOUNTED`, and a failure — it is not mapped onto a near
  neighbour. **An unrecognised subject is itself the finding**: either a gate is missing from the
  inventory, or the sentence names something that is not one of the 22.

**It fails closed**, which is the property the verb sweep did not have: every list in it is a
closed class, and an occurrence matching none of them is *reported*, not skipped. A phrasing nobody
anticipated appears in the report instead of slipping past it.

**There are no exemptions.** An earlier revision exempted mentions inside **headings**, on the
ground that a heading has nowhere to carry a count, and units carrying an explicit **historical**
marker (*an earlier revision*, *used to*, *was false*). A review beat both with one sentence each,
both of them about §11.3's three diagram gates `the_hops_diagram_and_the_document_agree_on_the_lowered_request`,
`the_boundary_diagram_names_only_links_the_document_defines` and
`every_boundary_diagram_pipeline_carries_the_three_synthesised_links` — all **wave 1**, none at base:
`### The diagram gates pass today` exited **0** as a heading, and *"The diagram gates pass today; a
previous revision described only their names"* exited **0** because a marker in its second clause
covered the live assertion in its first. Both exemptions are removed rather than narrowed. The six
headings and the seven historical sentences now carry their own wave or measured evidence in the
same unit, like everything else, and the checker prints no bucket that it does not also check.

**What this still admits. The checker is FROZEN at this behaviour and is not to be rebuilt.** Four
mechanisms preceded it and each was defeated in the round after it shipped — three verb sweeps, then
a designation vocabulary — so the seven cases below are not a to-do list. They are the stated limit,
and what covers the rest is a person reading the document. Each was measured by injecting the
sentence into these artefacts and running both checks, not argued. The runner is
`492-r14-limits.py` in the architect's session scratchpad; the tree, the checker, the sweep and
a temporary root are all arguments to it, so it runs against a tree without being edited.

1. **A marker in the unit is not a consistency check.** *"`the_hops_diagram_and_the_document_agree_on_the_lowered_request`,
   which wave 1 writes, already rejects malformed rows"* is `AGREES` to the identifier check, which
   exits **0**: the identifier resolves, the unit carries the wave, and no string match sees that the
   rest of the sentence contradicts it. The retained sweep matches no claim verb in it — `rejects` is
   in neither of its two verb tiers — so it does not report it and exits **0** too.
2. **A modifier AFTER the head noun still narrows the subject, and neither check sees it.** *"The
   gates for colour semantics pass today; wave 1 only documents them"* exits **0** on both, inside
   §11.3: the mention itself is generic and resolves to that section's rows while the trailing
   modifier stays invisible. An earlier revision of this list said what survives is strictly
   narrower — that a sentence could no longer claim a gate outside the inventory **runs**, only
   that one is planned. That was false, and the sentence just quoted is the counterexample: it
   asserts that a gate outside the inventory passes **today**.
3. **A possessive head noun is read as a compound and skipped.** *"The diagram gate's state is
   enforced today"* is not reported by the identifier check, which exits **0**: the possessive is
   followed by a noun, which puts the mention in the compound class — so a gate outside the
   inventory can be asserted to run today. The retained sweep does flag that spelling; *"The
   diagram gate's state is red the moment a label changes today"* exits **0** on both.
4. **The retained sweep is case-sensitive.** *"the test asserts the parser refuses them"* is
   `FLAGGED` by the sweep at exit 1; the same sentence with a capital `T` is not reported by it and
   the sweep exits **0**. Its subject patterns are compiled without a case-insensitive flag, so
   capitalising a first letter stops it flagging — and a capital first letter is the normal spelling
   of an English sentence. The identifier check reports neither spelling and exits **0** on both,
   because the sentence names no gate the inventory carries — which is limit 7.
5. **A unit that mentions a wave-1 gate and an existing one together is checked for the wave
   evidence only.** *"`query_log_gates` does not exist at base, and neither does
   `the_golden_sql_corpus_contains_no_with_clause`"* is `AGREES` to the identifier check at exit
   **0**, although the first exists and prints `Starting 14 tests`. The retained sweep matches no
   claim verb in it, so it does not report it and exits **0**.
6. **A qualified mention resolves to whatever identifiers its unit happens to carry, even when they
   name a different gate.** *"`the_hops_diagram_and_the_document_agree_on_the_lowered_request` is
   wave 1, and the colour-semantics gates are too"* is `AGREES` to the identifier check at exit
   **0**, the uninventoried subject resolved onto the hops-diagram row; the retained sweep matches
   no claim verb in it, so it does not report it and exits **0**. This one is not hypothetical.
   Before that revision §11.2b attributed the four variant gates' job to all of §11.3's, in a
   sentence that also named
   `the_document_states_the_residual_effect_counts_the_gates_assert` — **wave 1** — and the mention
   passed by resolving onto that row; split the same words into two sentences and it is reported
   `UNACCOUNTED`.
7. **A claim with no gate word and no identifier is invisible to this check by construction.** *"the
   test asserts the parser refuses them"* names nothing the inventory carries, so it does not report
   it and exits 0 — while the retained sweep prints `FLAGGED` and exits 1, subject to limit 4. That
   is the whole reason the sweep is kept.

**The two evidence patterns are read out of this document rather than written twice.** The checker
parses the fenced block below and compiles its two regular expressions from it, so the phrases this
section states and the phrases it accepts are the same strings by construction — an earlier
revision printed eight historical phrases in prose while its script accepted eighteen, and nothing
could have caught that but reading both. `MEAS_EV` names the **measurement** and never the verdict:
an earlier revision accepted the bare word *passes*, so *"The golden corpus gates fail today; a toy
example passes"* exited **0** on evidence about a different subject in the same sentence — where
§11.1's `the_sql_golden_corpus_has_exactly_its_committed_membership`,
`the_sql_golden_corpus_matches_its_committed_digest` and
`skip_block_conditions_are_captured_and_blocks_do_not_swallow_each_other` each print `Starting 1 test`.

```gate-evidence-patterns
WAVE_EV=wave 1|wave-1|Starting 0 tests|exits? 4|exits? 101|does not exist|do not exist|not exist|no such target|has not been written|nothing at base|neither exists|none of them exists|none of the three exists|nothing asserts them today
MEAS_EV=Starting \d+ tests?|exits? 0
```

It sorts every unit into buckets and prints the first three in full. **The counts below are the
round-15 run, over a 22-row inventory, and they have NOT been re-run on this revision — the scripts
are no longer on the machine.** They are quoted as a dated result, not as a property of the text as
it now stands:

| bucket | meaning | count, round 15 |
|---|---|---|
| `UNACCOUNTED` | a qualified mention whose unit names none of the inventory identifiers | **0** |
| `UNRESOLVED` | a generic mention resolving to no row at all — a section that owns none, citing none | **0** |
| `DISAGREES` | a mention whose unit contradicts, or fails to carry, its rows' `at base` state | **0** |
| `AGREES` | a mention whose unit carries the wave or the measured evidence for its rows | **112** |
| `COMPOUND`, `GENERIC-INDEF` | not references to any particular one of the inventory | **60** |

**159** mentions in all, at round 15. Only the first three rows were properties, and all three were
zero; the rest are a snapshot that moves whenever prose is added — and prose has been added since.

> **The plan-object revision was NOT checked by this script, and could not have been.**
> `492-r10-gate-mention-check.py`, `492-r6-gate-claim-sweep.py` and `492-r14-limits.py` lived in a
> session scratchpad that has been cleared; `find /tmp /home/hayato -name '492-r*-*.py'` returns
> nothing. The standing owner ruling is that the checker is frozen and that the remaining
> verification is a person reading the document; this revision does not rebuild it, and does not
> claim its buckets. **Every `Starting N tests` line and every exit code in §11 WAS re-taken**, at
> `acf44c49` with `cargo-nextest 0.9.143`, and they are the readings printed below — the prose
> audit is the only part that is dated.

**What this checker rests on, said here so the next person does not have to find it.** It derives
the inventory rows and their states from this document, but it finds them by **four exact header
strings** — the three gate-table headers (`| gate | selector | binary | at base |` and the two `-E`
forms) and §11.0b's seed header, which it skips as a header row and which the companion inventory
check sums the parenthesised counts from — plus the fence info string `gate-evidence-patterns`
above, and it reads a row as *existing* by the literal `Starting (?:1|14) tests?`. Change a header's spelling
and that table drops out of the inventory in silence; add a gate whose count is neither 1 nor 14
and its row reads as not existing. Neither falsifies what it prints today, because §11.0b
enumerates the same inventory independently from its own counts and the two tables cross-check; but the
anchoring is hand-written, and it is the first thing to look at if the two ever disagree.

**This checker has never been run against a revision whose defects it was written to catch, and that
is the largest gap in its evidence.** These four artefacts are untracked, so no earlier revision of
them is in the repository. What is retained outside it is the text as it stood before the round-8
edits, the edit scripts for each round, and the round-7 reviewer's reversal script, which together
could in principle reconstruct the round-5 text — but that reconstruction has not been run against
this checker. A replay was attempted instead, from thirteen claim sentences taken from those
scripts' replaced strings, and it is **not** evidence and is not cited here: the sentences in it
were shortened by hand, so not one of the thirteen matches the string it was taken from, and what it
exercises is a paraphrase. The evidence for this checker is exactly two things — the buckets it
prints on these four artefacts as they stand, and the seven injected cases above. It has not been
shown to catch anything it was not shown catching there.

**The verb sweep is retained as a backstop, not as the mechanism.** `492-r6-gate-claim-sweep.py`, in
the same place, still sorts every unit into `FLAGGED` / `BACKED` / `MARKED` / `RESIDUE` by a hand-written
two-tier verb list, and its `RESIDUE` — every unit that names a gate and matches no claim
verb — is printed under `--residue` and read as a list. That reading is what turned up "the gates
**are** that argument as tests", "pushability **gets** its own gate" and "`Drop` and `Keep` already
**have** one row each", none of which any verb list contained and each of which now names **wave 1**
in the sentence itself. That is why the sweep is kept even though it is no longer what the rule is
checked with. **The two are complementary in both directions, and each direction has a case that
reproduces.** Injected into these artefacts: *"the test asserts the parser refuses them"* and
*"adding a stage variant fails to compile here"* name no gate at all, so the identifier-keyed check
does not report them and exits 0 while the sweep prints `FLAGGED` and exits 1; *"The diagram gate
for colour semantics passes today; wave 1 only documents it"* and *"The diagram gates, three of
them, pass at base"* name a subject the inventory does not carry — unlike §11.3's
`the_hops_diagram_and_the_document_agree_on_the_lowered_request`,
`the_boundary_diagram_names_only_links_the_document_defines` and
`every_boundary_diagram_pipeline_carries_the_three_synthesised_links`, which are **wave 1** — so
the sweep does not report them and exits 0 while the identifier-keyed check prints `UNACCOUNTED`
and exits 1. An earlier revision
claimed the inversion at two specific lines of a superseded file instead, and that claim did not
reproduce — the identifier-keyed check reports both lines. Neither script is committed: a checker
for a document that will be deleted when the code lands is more apparatus than the check is
worth.

**Every selector this document names, re-run on this tree at `acf44c49` with `cargo-nextest 0.9.143`**, with
`CARGO_TARGET_DIR` outside the tree. The runner is `492-r4-run-selectors.sh` in the same place.

| selector | target | `Starting …` | exit | state |
|---|---|---|---|---|
| `the_sql_golden_corpus_has_exactly_its_committed_membership` | `--test golden_sql_freeze` | `Starting 1 test across 1 binary (1 test skipped)` | 0 | **exists, passes** |
| `the_sql_golden_corpus_matches_its_committed_digest` | `--test golden_sql_freeze` | `Starting 1 test across 1 binary (1 test skipped)` | 0 | **exists, passes** |
| `skip_block_conditions_are_captured_and_blocks_do_not_swallow_each_other` | `--test explain_indexes` | `Starting 1 test across 1 binary (33 tests skipped)` | 0 | **exists, passes** |
| `traces::exec::tests::search_settings_pin_the_layer_1_budget_contract` | `--lib` | `Starting 1 test across 1 binary (1207 tests skipped)` | 0 | **exists, passes** — the selector-form demonstration below |
| `search_settings_pin_the_layer_1_budget_contract` (bare) | `--lib` | `Starting 0 tests across 1 binary (1208 tests skipped)` | 4 | selects nothing **by design** — the same test, wrong form |
| the six §11.2 gates | `--lib` | `Starting 0 tests across 1 binary (1208 tests skipped)` each | 4 each | **do not exist — wave 1** |
| the three §11.2b gates | `--lib` | `Starting 0 tests across 1 binary (1208 tests skipped)` each | 4 each | **do not exist — wave 1** |
| the §11.3 gates — **eight** before the plan-object revision, **eleven** since it added three; all eleven re-measured at `acf44c49` | `--test query_lowering_doc_gate` | `error: no test target named \`query_lowering_doc_gate\` in \`pulsus-read\` package` | 101 each | **binary does not exist — wave 1**; re-checked at `acf44c49` by `git ls-files` and by `ls`, both of which report no such file |
| `the_golden_sql_corpus_contains_no_with_clause` | `--test golden_sql_freeze` | `Starting 0 tests across 1 binary (2 tests skipped)` | 4 | **does not exist — wave 1** |
| the `query_log` live half (whole binary) | `--test query_log_gates` | `Starting 14 tests across 1 binary` | 0 | **exists** — but see §11.4: it self-skips green |

**Four of the 25 gates exist; twenty-one do not exist and are wave 1.** The absence was checked
wider than the runs above, and over the **whole** set rather than a sample of it: one discovery
expression naming all **21** wave-1 gates by their **full test-function names** — so subsumption is
trivial rather than argued — run at `acf44c49` with `cargo-nextest 0.9.143`:

```
$ cargo nextest run --workspace -E 'test(=…) + test(=…) + … 21 names …'
    Starting 0 tests across 221 binaries (6949 tests skipped)
error: no tests to run
exit=4

$ cargo nextest run --workspace -E '…the same 21… + test(=the_sql_golden_corpus_matches_its_committed_digest) + test(=skip_block_conditions_are_captured_and_blocks_do_not_swallow_each_other)'
    Starting 2 tests across 221 binaries (6947 tests skipped)
exit=0
```

**The control is what makes the zero mean something.** A filter that silently matched nothing would
have produced the same `Starting 0 tests`; adding two names that must match turns it into
`Starting 2`, so the expression is discriminating rather than inert. The two runs also cross-check
the population: `0 + 6949` and `2 + 6947` are both **6,949**, which is the workspace test count at
`acf44c49` — it was 6,851 at `2f78c53`, and the difference is the five commits in between.

**`run`, not `list` — and this is a trap worth stating.** The empty-selection guarantee is on
`nextest run`. `cargo nextest list` **exits 0** on a selector matching nothing, measured here:

```
$ cargo nextest list -p pulsus-read --lib -E 'test(=no_such_test_name_at_all_zzz)'; echo $?
0
$ cargo nextest run  -p pulsus-read --lib -E 'test(=no_such_test_name_at_all_zzz)'
    Starting 0 tests across 1 binary (1208 tests skipped)
error: no tests to run
exit=4
```

So an absence check written with `list` would report success whether or not it was measuring
anything. Every reading in §11 is from `run`.

Two more absences, by a different instrument so that neither is the only witness:
`git grep -w <name> -- crates/` returns **no line** for any of the 21, while the same command over
the three existing gates returns one file each; and `git ls-files` for `query_lowering`,
`compile.rs` and `/compile/` returns nothing, with
`crates/pulsus-read/tests/query_lowering_doc_gate.rs` absent from disk.

**Why wave 1 for all twenty-one.** §10 states what the first implementation wave delivers: the core
with R1–R3b and R5 applied, the TraceQL aggregate as its first lowered link, and the LogQL `Lang`
impl and link set compiled and unwired. `crates/pulsus-read/src/traces/compile.rs` and
`crates/pulsus-read/src/logql/compile.rs` both come into existence there, which is the first moment
any of these can be written at all. **The coder owes their red output**: each must be
shown failing before it is made to pass, and this document's `at base` column must be replaced by
the count the same selector prints once wave 1 lands.

### 11.0b Where each of §11's 25 gates gets its expected answer — four existed at base, twenty-one were **wave 1** then, and twenty-four exist today

A gate seeded from one example would assert that the example is correct. If the example is wrong,
such a gate makes the error permanent and looks like coverage while doing it — so every row below
states whether its seed is **independently established** or **assumed**, and the assumed ones say
what they therefore cannot discover. **All 25 gates §11.0 counts have a row here**: of the three
missing from an earlier revision, `logql::plan::tests::a_refused_line_format_marks_the_body_computed_and_the_next_filter_residual`,
**wave 1**, is the third row from the end and the live `query_log_gates` half, which exists and
prints `Starting 14 tests` at exit 0, is the last, while
`every_boundary_diagram_pipeline_carries_the_three_synthesised_links`, **wave 1**, is the third gate
of the diagram row — which is why that row covers **three** gates, not two. The parenthesised count in each `gate` cell sums to **25**, matching
§11.0's total, and the `at base` column repeats §11.0's exists-or-wave answer for every one — so
the two tables are a cross-check on each other rather than one table quoted twice.

| gate | at base | what seeds its expected answer | independent? |
|---|---|---|---|
| §11.1 golden corpus membership and digest (2) | **exist**, `Starting 1 test` each, exit 0 | the committed golden `.sql` files, produced by the shipped renderer under a prior issue and frozen | **yes** — the gate compares this tree against bytes it did not produce |
| §11.1 `EXPLAIN` skip-block reader (1) | **exists**, `Starting 1 test`, exit 0 | its committed fixture | **yes**, same |
| §11.2 walks 1–3 (3) | **wave 1**, `Starting 0 tests`, exit 4 each | the 3,375 chains of §9.6, with the **shipped** functions as oracle | **yes for traversal; no for pushability** — the model and all three walks call the same `is_pushable_line_filter`, so that half is one producer wearing two hats. Stated again at §11.2 below, because it is the limit that matters |
| §11.2 the first-refusal negative control (1) | **wave 1**, `Starting 0 tests`, exit 4 | the same corpus, expectation `!= 0` | **yes** — direction-neutral: once written it would redden if the fold silently became a prefix again, whatever the new count is |
| §11.2 pushability (1) | **wave 1**, `Starting 0 tests`, exit 4 | eight hand-written rows | **no.** The AST flag columns were captured from the real parser, so they cannot drift; the **decision** column is this document's statement of the rule. Nothing outside this repository defines it — SQL pushdown is ours, not the reference's — so the gate freezes the rule and cannot discover the rule is wrong. That is a review obligation, not a test's |
| §11.2b residual effects, LogQL and TraceQL (2) | **wave 1**, `Starting 0 tests`, exit 4 each | two seeds and two literals per row (below) | **the literals: no**, they are this document's claim about each link. **The preserve-against-assign property: yes**, because the two seeds differ in the fields the row claims to leave alone and the relation between the two literals is asserted independently of the implementation |
| §11.2b `Drop`/`Keep` (1) | **wave 1**, `Starting 0 tests`, exit 4 | one payload, two literals | as above, plus `expected_drop != expected_keep` computed from the two literals rather than from the two dispatchers |
| §11.3's four variant gates — `every_logql_stage_variant_has_a_row_in_the_lowering_document`, `every_traceql_pipeline_stage_variant_has_a_row_in_the_lowering_document`, `every_lql_link_variant_has_a_row_in_the_lowering_document`, `every_traceql_chain_link_has_a_row_in_the_lowering_document` (4) | **wave 1**, exit 101, no such target | the AST enums against this document's tables | **yes** — two independent producers: the compiler's variant list, and this text |
| §11.3's `the_document_states_the_residual_effect_counts_the_gates_assert` (1) | **wave 1**, exit 101, no such target | this document's tables against the gates' own row lists | **no** — both sides are written here. It is a consistency gate, and a wrong count agreed on twice still passes |
| §11.3 the **three** diagram gates — `the_hops_diagram_and_the_document_agree_on_the_lowered_request`, `the_boundary_diagram_names_only_links_the_document_defines`, `every_boundary_diagram_pipeline_carries_the_three_synthesised_links` (3) | **wave 1**, exit 101, no such target | the diagrams' text against this document's | **no**, same reason — and two rounds running have found assertions in the hops diagram the prose did not carry: three in the previous round (a named client product, a resident-memory figure, and "every one of the 554 batches" where §9.2 said 553 at the time) and three more in this one (`heap of 20`, `renders 20 rows`, `bounded by limit`). **None of the six would have been caught by any of the three gates**, because they compare only the lowered round-trip count, the result-byte total, the link labels and the three synthesised links |
| §11.2's `logql::plan::tests::a_refused_line_format_marks_the_body_computed_and_the_next_filter_residual` (1) | **wave 1**, `Starting 0 tests`, exit 4 | the `Computed`/residual pair this document states for `LineFormat` and the following line filter (§7.1), written as literals in the test | **no** — both sides are this document's claim about the language. It is the §11.2b limit in a smaller frame: it freezes the stated behaviour and cannot discover that the stated behaviour is wrong. What it *can* discover is a fold that drops the effect, which is the §2.5 regression it exists for |
| §11.3's `every_cut_variant_has_a_row_in_the_design_record` (1) | **wave 1**, exit 101, no such target | an exhaustive `match` over `Cut` on one side, §2.7's headings on the other | **yes** — two independent producers, the compiler's variant list and this text. What it cannot discover is that a **fifth** cut is needed; §2.7.9 says what would falsify the closure argument, and no gate can |
| §11.3's `every_chain_link_row_states_a_continuation` (1) | **wave 1**, exit 101, no such target | this document's three link tables against the four `Cut` variants | **no** — both sides are written here. It catches a row with no continuation cell and a continuation naming a cut that does not exist; it cannot discover that a stated continuation is the wrong one |
| §11.3's `the_plan_shape_json_keys_match_the_api_document` (1) | **wave 1**, exit 101, no such target | `QueryPlan::shape()`'s rendered keys against [api.md](api.md)'s `data.explain.plan` block | **yes** — the keys come from a serializer and the expectation from a committed document in another directory, so neither produces the other |
| §11.4 no-`WITH` (1) | **wave 1**, `Starting 0 tests`, exit 4 | the golden corpus | **yes, and vacuous until wave 2** — at base the corpus contains no lowered SQL at all, so the gate would be green over a population containing none of the case it exists for |
| §11.4 the live `query_log` half (1) | **exists**, `Starting 14 tests`, exit 0 — and see §11.4: worthless locally | the round-trip and metered-byte counters ClickHouse writes for our own queries | **yes for the counters, and nothing at base** — `system.query_log` is written by the database, not by us, so the numbers are not ours to get wrong; but the ratios it checks are this document's, and locally the binary self-skips green without `PULSUS_TEST_CLICKHOUSE`, so its only real evidence is the `schema-it` CI job (§11.4) |

### 11.1 The three gates that existed at base and must not move — three of the four; each prints `Starting 1 test` and exits 0, then and today

| gate | selector | binary | at base | today |
|---|---|---|---|---|
| the SQL golden corpus keeps its membership | `-E 'test(=the_sql_golden_corpus_has_exactly_its_committed_membership)'` | `crates/pulsus-read/tests/golden_sql_freeze.rs` | `Starting 1 test`, passes | **exists** |
| the SQL golden corpus keeps its digest | `-E 'test(=the_sql_golden_corpus_matches_its_committed_digest)'` | same | `Starting 1 test`, passes | **exists** |
| the `EXPLAIN` skip-block reader still discriminates | `-E 'test(=skip_block_conditions_are_captured_and_blocks_do_not_swallow_each_other)'` | `crates/pulsus-read/tests/explain_indexes.rs` | `Starting 1 test`, passes | **exists** |

Wave 1 emits no SQL, so the first two must stay green **unchanged** — measured green today,
`Starting 1 test` each, exit 0 (§11.0). When the fold is wired, the
goldens and `PINNED_SQL_CORPUS` (`crates/pulsus-read/tests/golden_sql_freeze.rs:168`) move in the
same commit.

### 11.2 The gates that reproduce each hand-written walk — all **wave 1** at base, none of them there then; all six exist today

§1's argument is that the boundary is computed four times by hand. The gates **wave 1** writes are
to be that argument as tests: the model must reproduce **each** walk, not just the one §9.6
measured. None of them exists at base.

These are **lib unit tests**, because `compile_line_filters` is `pub(crate)`
(`crates/pulsus-read/src/logql/plan.rs:3052`) and `has_unpushed_dropping_stage` (`:1655`) and
`metric_pipeline_construct` (`:1680`) are private — an integration test cannot call any of them.
**They go in `plan.rs`'s existing `mod tests` (`plan.rs:3206`), and no production item is widened
for them.** That module is a child of `logql::plan`, so it already reaches both private functions —
directly, and again through its `use super::*` (`plan.rs:3209`). An earlier version of this section
offered a second option — **wave 1** writes them wherever they go — moving the gates to
`logql::compile`'s test module with the two functions raised to `pub(super)`. That option is **withdrawn**: the widening was never needed, and a design
that offers two placements has not decided.

| gate | selector (`-E`) | at base | today |
|---|---|---|---|
| walk 1: the model's ordered pushed-filter list equals `compile_line_filters`' own — the real function, not a transcription — over the 3,375 chains of §9.6 | `test(=logql::plan::tests::the_model_reproduces_compile_line_filters_ordered_predicate_list)` | `Starting 0 tests`, exit 4 — **wave 1** | **exists** |
| the same suite recomputes the first-refusal fold's mismatch count and asserts it is **not** 0, so the regression cannot silently return | `test(=logql::plan::tests::a_first_refusal_fold_still_mismatches_the_shipped_walk)` | `Starting 0 tests`, exit 4 — **wave 1** | **exists** |
| walk 2: `!exact` on a `Lines` shape after the fold equals `has_unpushed_dropping_stage` on the same pipeline, over the same corpus | `test(=logql::plan::tests::exact_after_the_fold_agrees_with_has_unpushed_dropping_stage)` | `Starting 0 tests`, exit 4 — **wave 1** | **exists** |
| walk 3: the first `Pipe` link the fold marks residual is the stage `metric_pipeline_construct` names, and the reason maps to its `&'static str` | `test(=logql::plan::tests::the_first_residual_pipe_link_agrees_with_metric_pipeline_construct)` | `Starting 0 tests`, exit 4 — **wave 1** | **exists** |
| the residual rule as behaviour: a refused `line_format` marks `body` `Computed`, and the next line filter is residual because of it | `test(=logql::plan::tests::a_refused_line_format_marks_the_body_computed_and_the_next_filter_residual)` | `Starting 0 tests`, exit 4 — **wave 1** | **exists** |

The gate this table used to carry — `logql::compile::tests::drop_and_keep_dispatch_differently_on_the_same_payload_type`,
**wave 1** — has **moved to §11.2b**, where each side gets its own literal expectation. It is listed there and not here, so there is one gate of that name and not two.

Walks 2 and 3 are what make the §9.6 number stop being the only evidence: they cover label filters,
line rewrites, parsers and `label_format` — every stage the 15-atom set does not contain.

**What these three gates will NOT establish once wave 1 has written them, because they share a
helper — today they establish nothing, because they do not exist.** All three walks call
`is_pushable_line_filter` (`crates/pulsus-read/src/logql/plan.rs:3086`) — `plan.rs:3060`,
`plan.rs:1668`, `plan.rs:1686` — and so does the model's `LineFilter::capability` (§7.1). The
sharing is deliberate and stays: that function's doc comment calls itself *"the single source of
truth for 'does this line filter push down to SQL, or must it run in the client pipeline?' … so the
two paths never drift"*, and a model that computed pushability itself would be the second producer
the comment exists to prevent. But the consequence has to be said rather than left implicit:

> The three agreement gates — `logql::plan::tests::the_model_reproduces_compile_line_filters_ordered_predicate_list`,
> `logql::plan::tests::exact_after_the_fold_agrees_with_has_unpushed_dropping_stage` and
> `logql::plan::tests::the_first_residual_pipe_link_agrees_with_metric_pipeline_construct` — once **wave 1** has
> written them, will establish that the model's
> **traversal and accumulated state** reproduce each hand-written walk. They will establish
> **nothing** about pushability itself: if `is_pushable_line_filter` were wrong, both sides would be
> wrong together and all three would stay green. At base none of the three exists — each selector
> prints `Starting 0 tests` and exits 4 — so today they establish nothing at all.

So pushability is to get its own gate — also **wave 1**, `Starting 0 tests`, exit 4 at base — and
it is to reach its answer without the helper: the expected answers are to be literals written in the
test, one per parsed query, never values the helper produced.

| gate | selector (`-E`) | at base | today |
|---|---|---|---|
| the pushability rule matches a hand-written table of parsed line filters | `test(=logql::plan::tests::the_pushability_rule_matches_a_hand_written_table)` | `Starting 0 tests`, exit 4 — **wave 1** | **exists** |

The table, with every row's AST flags captured from the real parser on this tree at `2f78c53`. It
is chosen adversarially: two rows spell an IP address without being an `ip()` filter, and two put
the `ip()` on the `or` side rather than the head.

| query | `value_is_ip` | `or_matches` | expected |
|---|---|---|---|
| `{service_name="checkout"} \|= "x"` | `false` | `[]` | **pushable** |
| ``{service_name="checkout"} \|~ `1\.2\.3\.4` `` | `false` | `[]` | **pushable** — a regex that spells an IP is not an `ip()` filter |
| `{service_name="checkout"} \|= "ip("` | `false` | `[]` | **pushable** — a literal that spells the call is not the call |
| `{service_name="checkout"} \|= ip("10.0.0.1")` | `true` | `[]` | **not pushable** |
| `{service_name="checkout"} != ip("10.0.0.1")` | `true` | `[]` | **not pushable** |
| `{service_name="checkout"} \|= "x" or "y"` | `false` | `[("y", false)]` | **pushable** |
| `{service_name="checkout"} \|= "x" or ip("10.0.0.1")` | `false` | `[("10.0.0.1", true)]` | **not pushable** — the `ip()` is not the head |
| `{service_name="checkout"} \|= ip("10.0.0.1") or "x"` | `true` | `[("x", false)]` | **not pushable** |

Two shapes that do **not** reach the rule. The gate **wave 1** writes is to assert that the parser
refuses them, so that a later parser change cannot quietly add a case the table does not cover; the
two parser answers below were captured on this tree at `2f78c53`, but nothing asserts them today:

| query | parser answer, verbatim |
|---|---|
| `{service_name="checkout"} \|~ "1\.2\.3\.4"` | `invalid char escape "\." at byte 31` — a double-quoted LogQL string has no `\.` escape; the backtick form above is the spelling that parses |
| `{service_name="checkout"} !~ "x" or ip("::1")` | ``unexpected identifier "ip" at byte 36: expected a string (ip() line filters require `\|=` or `!=`)`` |

**Composition, stated once so the loop is visible — for what wave 1 is to build, not for what runs
today.** The gates for walks 1–3 — **wave 1** — are specified to prove *model traversal == shipped
traversal* with pushability held in common; `logql::plan::tests::the_pushability_rule_matches_a_hand_written_table`,
also **wave 1**, is specified to prove *pushability == a literal table*.
Together they would cover the model's pushability decision; neither half would cover it alone, and
this document does not claim otherwise. All four are **wave 1**: each selector prints
`Starting 0 tests` and exits 4 at base, so the composition is a specification and covers nothing
yet.

### 11.2b Every residual state effect — gated in wave 1, and all three gates exist today

§2.5's whole repair is that a residual link **still applies its state effect**. §3.1 and §7.1 state
that effect for every link. An earlier version of this section nominated other gates that did not
touch every one of those effects, all in one language — **wave 1** writes the three that replace
them, and none of the three exists at base — so the design's central mechanism was under-checked,
and the `Drop`/`Keep` pair passed if either side alone was neutered. **This document states no count
for that version, here or in §10, because no retained artefact contains it: how many gates it
nominated, and how many of those effects they touched, cannot be checked — the three that replace
them are wave 1, and none of the three exists at base.** Nothing else said here about that version
can be checked against an artefact either — including "under-checked" just above, and the sentence
about the earlier `Drop`/`Keep` revision below.

**Every link with a stated residual state effect gets a row.** Counted off the document's own
tables: **11** in §3.1 (`Structural`, `NestedSet`, `BoolTruth`, `Aggregate`, `By`, grouped
`Coalesce`, `Select`, `Filter`, `Order`, `Limit`, `Emit`) and **20** in §7.1 — 13 `Pipe` rows
(`LineFilter`, the four `Parser` forms, `LabelFilter`, `LineFormat`, `LabelFormat`, `Unwrap`,
`Unpack`, `Decolorize`, `Drop`, `Keep`) and 7 synthesised (`Window`, `RangeAgg`, `VectorAgg`,
`LabelReplace`, `Order`, `Limit`, `Emit`) — **31** effects in all. The **ten** chain links whose
stated effect is *none* — §3.1's `Source`, `Coalesce` with no preceding `By` and seven per-batch
reads (`Hydrate`, the four indexed phase-2 reads and the two trace-wide co-loads), and **§7.1's
`Source`, which part 8 added: `LqlLink::Source` had no row at all, so §7.1 called itself the
complete LogQL link set while omitting a variant, the same defect §3.1 carried** — are to get a row
too, asserting the effect **is** the identity, so the exemption is itself a check
rather than a silence. So
`logql::compile::tests::every_residual_state_effect_is_the_one_the_document_states` is specified to
carry **21** rows and
`traces::compile::tests::every_residual_state_effect_is_the_one_the_document_states` **20** — in
**wave 1**, which writes both; neither exists at base.

**Part 8 closed the gap this paragraph used to describe.** Before it, §3.1 enumerated 5 of
`TqlLink`'s 15 variants, so this section's derivation from §3.1 came to ten while the shipped test
carried twenty-one, and the eleven-row difference had to be explained in prose. §3.1 now carries a
row for every variant, so the derivation is **20**, and the shipped test's **21** is that plus
**one more row for `By`** — the shipped test gives `By` a row per key branch, one key that renders
and one that does not, where §3.1 gives it a single row. Twenty plus one. The shipped row count is
gated — `assert_every_residual_state_effect::<Tql>(&rows, 21)` in
`crates/pulsus-read/src/traces/compile.rs` — and
`the_document_states_the_residual_effect_counts_the_gates_assert` now gates the derivation against
it, so neither side is prose alone.

The five other cells reading `n/a` or `none` belong to rows the tables mark **not in the chain** —
§3.1's `Metric`, `MetricSecondStage` and `Compare`, and §7.1's `MetricExpr::Literal`/`VectorFn` and
`Binary`/`Variants` — and are excluded by that marking, not by silence.

| gate | selector (`-E`) | at base | today |
|---|---|---|---|
| every LogQL link's residual state effect is the one §7.1 states, and none of them is the identity | `test(=logql::compile::tests::every_residual_state_effect_is_the_one_the_document_states)` | `Starting 0 tests`, exit 4 — **wave 1** | **exists** |
| the same for TraceQL against §3.1 | `test(=traces::compile::tests::every_residual_state_effect_is_the_one_the_document_states)` | `Starting 0 tests`, exit 4 — **wave 1** | **exists** |
| `Drop` and `Keep` reach different dispatchers and different effects on the same `Vec<DropKeepElem>` | `test(=logql::compile::tests::drop_and_keep_dispatch_differently_on_the_same_payload_type)` | `Starting 0 tests`, exit 4 — **wave 1** | **exists** |

All three gates above — **wave 1** writes them — are to be lib unit tests in the modules that will
define the impls
(`crates/pulsus-read/src/logql/compile.rs` and `crates/pulsus-read/src/traces/compile.rs`), so that they
reach every `Lower` impl without widening anything. Neither those modules nor the gates exist at
base: **wave 1** creates the modules and writes the gates, and all three selectors print
`Starting 0 tests` and exit 4 today.

**The neutering is to be executed, not described — wave 1 writes it.** The gate carries a wrapper whose `residual_effect` is
the identity and which delegates everything else:

```rust
/// The neutered dispatcher: `residual_effect` returns the relation
/// unchanged. Comparing a real dispatcher against this one IS the break
/// the review asked for, run in-process for every row on every run —
/// no source edit, no rebuild, and no row that can be forgotten.
struct Neutered<'a, L: Lang + ?Sized>(&'a dyn Lower<L>);

impl<L: Lang + ?Sized> Lower<L> for Neutered<'_, L> {
    fn capability(&self, s: &L::Stage, rel: &Relation<L>) -> Capability { self.0.capability(s, rel) }
    fn apply(&self, s: &L::Stage, rel: Relation<L>, cx: &LowerCx<'_, L>)
        -> Result<Relation<L>, L::Err> { self.0.apply(s, rel, cx) }
    fn residual_effect(&self, _s: &L::Stage, rel: Relation<L>) -> Relation<L> { rel }
}
```

**Two seeds per row, not one — and that is the part that matters.** The first draft of this section
gave each row **one** seed and **one** literal `expected`. That shape would not catch a wrong row,
it would **freeze it**. If the seed's `shape` is `Spans` and the row claims the effect leaves the shape
`Spans`, then an implementation that *preserves* the accumulated shape and one that *assigns*
`Spans` produce the same relation, and the gate **wave 1** writes would pass on both while looking
like coverage. The
gate does not exist at base — both selectors print `Starting 0 tests` and exit 4 — so **wave 1** is
still the cheapest place to fix its shape, which is why it is fixed here rather than after.

That is not hypothetical. §3.1's `Aggregate` and `By` rows said "shape stays `Spans`" and the
boundary diagram invokes `Aggregate` on `Groups`; §7.1's `RangeAgg` row said "shape stays
`Samples`" while its own `Unwrap` row describes the case where the accumulated shape is `Lines`.
Five rows across the two tables named a literal shape, and every one of the five named the stage's
usual **input** shape. A gate seeded once would have made all five permanent — which is why the two-seed shape is settled
here, before **wave 1** writes it.

**The chains that reach four of the five on a different shape were run, not argued** — parser and
validator only, since what is at stake is whether the chain shape is constructible. The probe is
`492-r4-probe-shapes.rs` in the architect's session scratchpad, run on this tree at `2f78c53`:

| chain | reaches | on accumulated shape | result |
|---|---|---|---|
| `{ .service.namespace = "prod" } \| by(name) \| count() > 2` | `Aggregate` residual | `Groups` | PARSES + VALIDATES |
| `{ .service.namespace = "prod" } \| by(name) \| by(.tier) \| count() > 2` | the second `By` residual (`grouping.is_some()`) | `Groups` | PARSES + VALIDATES |
| `{ .service.namespace = "prod" && trace:duration > 5s } \| by(name) \| coalesce()` | grouped `Coalesce` after a **residual** `By` (the partial source cleared `exact`, §2.4) | `Spans` | PARSES + VALIDATES |
| `sum_over_time({service_name="checkout"} \| line_format "{{.x}}" \| unwrap latency [5m])` | `RangeAgg` after a residual `Unwrap` | `Lines` | PARSES |
| `sum_over_time({service_name="checkout"} \| unwrap a \| unwrap b [5m])` | a second `Unwrap` | — | **PARSE-REFUSED**: `unexpected stage \`\| unwrap b\` at byte 51: expected a label filter (only label filters may follow \`unwrap\`)` |

The last row is why §7.1's `Unwrap` keeps `Lines` as a parenthetical rather than as the rule: it is
true today only because the parser refuses the chain that would falsify it. They are corrected
above, and the gate **wave 1** writes is specified so the same mistake cannot be re-frozen:

Each row therefore carries seeds `S₁` and `S₂` and literals `E₁` and `E₂`, and asserts:

1. `assert_ne!(S₁, S₂)` — the two seeds really are different, so a row cannot be satisfied by
   supplying the same seed twice.
2. `assert_eq!(real.residual_effect(link, S₁.clone()), E₁)` and the same for `S₂`/`E₂`. Both
   literals are whole `Relation`s **written in the test** from this document's row, never computed
   by the code under test. This catches a *wrong* effect — once **wave 1** has written it; today
   both selectors print `Starting 0 tests` and exit 4.
3. `assert_eq!(E₁ == E₂, row.effect_is_constant)`, where `effect_is_constant` is a literal `bool`
   the row declares. It is `false` for every row whose effect **preserves** a field the two seeds
   differ in, and `true` only where the effect genuinely resets a field to a constant. This is the
   assertion a single seed cannot make, and it is what would turn "shape unchanged" from a phrase
   into a property — in **wave 1**, which writes it; at base the selector exits 4.
4. for the **28** rows with a stated effect, on **both** seeds,
   `assert_ne!(real.residual_effect(link, Sᵢ.clone()), Neutered(real).residual_effect(link, Sᵢ.clone()))`
   — this is to catch a *missing* effect, and it is the neutering. Like assertion 2 it catches
   nothing until **wave 1** writes it.
5. for the **2** rows whose stated effect is none, the same comparison with `assert_eq!` on both
   seeds — the exemption checked rather than assumed.

**Which fields the two seeds must differ in.** Stated once so no row can pick a convenient pair:
**`S₁` and `S₂` differ in `shape`, plus every field the row's effect column names as unchanged or
retained.** A row saying `cols` is unchanged needs seeds differing in `cols`; one saying `exact` is
untouched needs seeds differing in `exact`; `ordering`, `limit`, `source`, `predicate` and `depth`
likewise. A row that names nothing as unchanged still gets two seeds differing in `shape`.

30 rows across the two gates **wave 1** writes, 60 seed evaluations, 28 rows carrying assertion 4 on
both seeds. The
wrapper cannot silently pass: if `Neutered::residual_effect` were ever made to delegate, all 28
would fail assertion 4 at once, which is the loudest possible failure.

**`Drop`/`Keep` is fixed by giving each side its own literal.** `Drop` and `Keep` are each given a
row of their own in `logql::compile::tests::every_residual_state_effect_is_the_one_the_document_states` —
**wave 1** writes that gate too — so `logql::compile::tests::drop_and_keep_dispatch_differently_on_the_same_payload_type`
is not a duplicate of those rows but the two
assertions a per-row table cannot make — that the two links reach *different dispatchers*, and that
their two literal effects differ from each other. It asserts, on one shared
`Vec<DropKeepElem>` payload `P`: that `lower_of(&Pipe(Stage::Drop(P)))` and
`lower_of(&Pipe(Stage::Keep(P)))` are **not the same dispatcher**; that `Drop`'s effect equals its
own literal `expected_drop`; that `Keep`'s equals its own literal `expected_keep`; and that
`expected_drop != expected_keep` — computed from the two literals, so it cannot be satisfied by both
sides collapsing together. Neutering either side alone fails that side's own assertion. An earlier
revision of this gate — `logql::compile::tests::drop_and_keep_dispatch_differently_on_the_same_payload_type`,
**wave 1**, and it does not exist at base — specified a single "they differ" comparison; that
comparison is removed, because it was exactly the relational check that two neutered sides
satisfy.

**Completeness, and where it stops. None of this exists at base; wave 1 writes all of it.** The row
list is to be enumerated by an exhaustive `match` over the link type with no `_` arm, so that once
the gate exists, adding a variant will fail to build it. That forces a *name* for the new link; it
does not by itself force a *row*. The closure is to be the count, all of it in **wave 1**: the LogQL gate is to assert it
has **21** rows and the TraceQL gate **20**, and §11.3's
`the_document_states_the_residual_effect_counts_the_gates_assert` is to assert the same numbers read
from this document's own tables. §11.3's four variant gates —
`every_logql_stage_variant_has_a_row_in_the_lowering_document`,
`every_traceql_pipeline_stage_variant_has_a_row_in_the_lowering_document`,
`every_lql_link_variant_has_a_row_in_the_lowering_document` and
`every_traceql_chain_link_has_a_row_in_the_lowering_document`, all **wave 1** and none of them at
base — are to assert that every AST and chain-link variant has a row in the document. All of that
is **wave 1**; at base each of those
selectors exits 4 or 101. So once wave 1 has landed them, a new variant will redden
`every_lql_link_variant_has_a_row_in_the_lowering_document` first, then
`the_document_states_the_residual_effect_counts_the_gates_assert`, then
`logql::compile::tests::every_residual_state_effect_is_the_one_the_document_states` — three reds, each
naming the next thing to do. Today it reddens nothing, because none of the three has been written.

**One implementation hazard, because it bit the parse that produced the numbers above.** Cells in
these tables contain **escaped pipes** — `` `Traces` \| `Groups` → answer ``, `` `Lines`\|`Series` ``
— so a splitter on `|` shifts those rows by a column and silently reads the *precondition* cell as
the *effect* cell. The first parse written for this section did exactly that and reported §3.1's
`Emit` effect as `none — see below`. Split on `(?<!\\)\|`.

**And the three link tables are one column wider than they were.** §3.1's, §7.1's ten-`Stage` and
§7.1's synthesised-link tables each gained a **continuation** column, appended after `disposition`
so that no existing column index moved. Every parse in §11.2b and §11.3 — including
`the_document_states_the_residual_effect_counts_the_gates_assert`, which reads the
residual-effect counts **11**/**12** and **20**/**3** out of these same tables — must be written
against the widened tables and must index the effect column from the left, never from the right.
Adding the column moved no count; **part 8's rows did**, and the four numbers above are that
section's counts after it.

What none of these will see, once **wave 1** has written them, is a row whose **literal `expected`
values are BOTH wrong in the same
way**. Two seeds would catch a preserve-against-assign confusion, because that confusion shows up as a
disagreement between `E₁` and `E₂`, but a row whose stated effect is simply the wrong effect will be
written into both literals and agree with itself. That is the document's claim about the language,
and it is settled by review, not by the gate **wave 1** writes.

### 11.3 The document and its diagrams — gated in wave 1, and all eleven gates exist today

A diagram asserts a design without being read as a claim, and this one has now carried four
contradictions across three rounds: a lowered request drawn as one round trip while the text made
`Emit` residual; a link in a diagram that appears in no table; a pipeline **truncated** before its
three synthesised links; and a caption calling the §9.6 oracle "the shipped `compile_line_filters`"
where §9.6 says it is a transcription of it. All four are mechanically detectable, so **wave 1** is
to gate them rather than leave them merely corrected. The gates go in a new integration test with
bare names, `crates/pulsus-read/tests/query_lowering_doc_gate.rs`, which **does not exist at base**:
every selector naming it exits 101 (§11.0). None of these eleven gates exists today; **wave 1** writes all eleven.
The last three are new in the plan-object revision and gate the three things that revision added: the
closure of `Cut`, the continuation column on the three link tables, and the `data.explain.plan` key
set against [api.md](api.md).

| gate | selector (`-E`) | at base | today |
|---|---|---|---|
| every `pulsus_logql::Stage` variant has a row in §7.1 — to be enumerated by an exhaustive `match` with no `_` arm, so that adding a variant will fail to build here | `test(=every_logql_stage_variant_has_a_row_in_the_lowering_document)` | exit **101**, no such target — **wave 1** | **exists** |
| every `pulsus_traceql::PipelineStage` variant has a row in §3.1, same construction | `test(=every_traceql_pipeline_stage_variant_has_a_row_in_the_lowering_document)` | exit **101**, no such target — **wave 1** | **exists** |
| every `LqlLink` variant has a row in §7.1, same construction — this is where adding a link variant will redden, once wave 1 has written it | `test(=every_lql_link_variant_has_a_row_in_the_lowering_document)` | exit **101**, no such target — **wave 1** | **exists** |
| every `TqlLink` variant — all **15** of them, by an exhaustive `match` with no `_` arm, so adding a sixteenth fails to build here — has a row in §3.1. The LogQL sibling is the same shape: `LqlLink` has exactly **9** variants and §7.1 carries a row for each. An earlier revision specified this gate as a hand list of twelve (the eight `PipelineStage` variants plus `Source`, `Order`, `Limit`, `Emit`), which would have passed over a §3.1 carrying 5 of the 15 | `test(=every_traceql_chain_link_has_a_row_in_the_lowering_document)` | exit **101**, no such target — **wave 1** | **exists** |
| §3.1 carries exactly **11** rows with a residual state effect and **12** without; §7.1 carries exactly **20** and **3** — the counts §11.2b's two gates assert against their own row lists | `test(=the_document_states_the_residual_effect_counts_the_gates_assert)` | exit **101**, no such target — **wave 1** | **exists** |
| the hops diagram's lowered round-trip count and result-byte total equal §9.2's | `test(=the_hops_diagram_and_the_document_agree_on_the_lowered_request)` | exit **101**, no such target — **wave 1** | **exists** |
| every link label in the boundary diagram's pipelines is a link this document defines | `test(=the_boundary_diagram_names_only_links_the_document_defines)` | exit **101**, no such target — **wave 1** | **exists** |
| every pipeline drawn in the boundary diagram ends in `Order`, `Limit` and `Emit`, because every chain does | `test(=every_boundary_diagram_pipeline_carries_the_three_synthesised_links)` | exit **101**, no such target — **wave 1** | **exists** |
| every `Cut` variant has a row in §2.7 — an exhaustive `match` over `Cut` with no `_` arm on one side, a parse of §2.7's headings on the other, so a fifth cut is a build failure rather than a silent addition | `test(=every_cut_variant_has_a_row_in_the_design_record)` | exit **101**, no such target — **wave 1** | **exists** |
| every row of §3.1's and §7.1's three link tables states a continuation, and every continuation naming a cut names one of the four | `test(=every_chain_link_row_states_a_continuation)` | exit **101**, no such target — **wave 1** | **exists** |
| every key `QueryPlan::shape()` renders is a key [api.md](api.md) documents for `data.explain.plan`, and no other | `test(=the_plan_shape_json_keys_match_the_api_document)` | exit **101**, no such target — **wave 1** | **exists** |

**A twelfth gate exists as of part 4 and is not one of the eleven.**
`the_hops_diagram_marks_its_superseded_figures_on_its_own_face` asserts only that the drawing
carries its superseded marker while it still carries two-statement figures. It is **not**
`the_hops_diagram_and_the_document_agree_on_the_lowered_request`, which compares the drawing's
lowered round-trip count and result-byte total against §9.2 and **cannot go green until part 8
redraws**: §9.2 counts 4 and the drawing counts 2, deliberately. A thirteenth,
`the_record_flags_the_two_survivors_nobody_re_measured`, asserts that this record still flags the
two figures nobody re-measured; it is not one of the eleven either.

**Pre-measured only as far as they can be.** These eight name a test binary that does not exist
either, so `cargo nextest run -p pulsus-read --test query_lowering_doc_gate -E '…'` fails at target
selection — re-run on this tree at `acf44c49`, printing
`error: command \`cargo test --no-run … --test query_lowering_doc_gate\` exited with code 101` and
exiting **101**, not at test selection and not at 4. What is established now is the selector *form*
— bare names, because they are integration tests — and that the count is 0. The gates in §11.2 are
different: their binary (`pulsus-read`'s lib) exists, so each was re-run at `acf44c49` and each
printed `Starting 0 tests across 1 binary (1208 tests skipped)` and exited **4**.

**What these eleven will not see, once wave 1 has written them — none of them runs today.** The
three gates §11.3 nominates for the diagrams — **wave 1**, none at base — are specified to compare
**text**, so a label the diagram spells differently from the document would read as undefined. The
gate **wave 1** writes for that — `every_boundary_diagram_pipeline_carries_the_three_synthesised_links` — is
specified to close one specific omission — a pipeline missing `Order`/`Limit`/`Emit` — because that omission has now happened; a link the
document defines and the diagram omits *anywhere else* is still invisible. Nor can they check that a
box's colour matches the disposition the table gives it, or that a caption's prose describes what
the caption's numbers were measured against — those bindings run through SVG geometry and English,
not through any string a test compares. Once wave 1 writes them they will catch a link that exists
nowhere, a truncated pipeline and a number that disagrees; they will not certify the picture.

**Two rounds running have proved that limit is real rather than theoretical, and the second round
found more than the first — which is a fact about the METHOD, not about the diagram.** The previous
round re-read the hops diagram looking for figures and found **three** picture-only assertions: the
generic client labelled as a specific vendor's datasource, a `pulsus-server` resident-memory figure
the document did not state, and "on every one of the 554 batches" where §9.2 said **553** at the time. This
round enumerated **every text node in both files** — `<title>`, `<desc>` and every `<text>`: **44**
nodes in the hops diagram (1 + 1 + 42) and **87** in the boundary (1 + 1 + 85), which is the literal
and complete set of things an SVG can assert — and found **three more** in the hops diagram, all of them missed before because each earlier pass had searched for the
*kind* of thing the pass before it found: `evaluator + heap of 20` (true of
`crates/pulsus-read/src/traces/exec.rs:1968`, but stated nowhere in the prose), `renders 20 rows`,
and "it is bounded by limit, not by candidates". All three are removed; the derived `1.12×` memory
ratio now shows the division it comes from; and the cost model the `METERED` labels depend on is
written into §9.1 as a **premise**, since it was the one thing the pictures asserted that the prose
had never stated at all. The boundary diagram's 87 nodes came back clean. **None of the six would
have been caught by any of the three gates §11.3 nominates for the diagrams** — **wave 1** writes
them — which are specified to
compare the lowered round-trip count, the result-byte total, the link labels and the three
synthesised links, and none of the six is any of those.

### 11.4 The gates ADR 0008 nominates — one **wave 1**, one that exists and prints `Starting 14 tests` at exit 0, and one added by part 7 that is not one of §11.0's 25

| gate | selector (`-E`) | binary | at base | today |
|---|---|---|---|---|
| no emitted SQL contains a `WITH` clause (ADR 0008 D2) | `test(=the_golden_sql_corpus_contains_no_with_clause)` | `crates/pulsus-read/tests/golden_sql_freeze.rs` | `Starting 0 tests`, exit 4 — **wave 1** | **exists** |
| the `query_log` half of the same rule, and the round-trip and metered-byte ratios | — | `crates/pulsus-read/tests/query_log_gates.rs` | `Starting 14 tests`, 14 passed, exit 0 — **exists**, but see below | — |
| no statement the compile core plans contains a join (ADR 0008's added rule, scoped to the compiled route's corpus) — **added by issue #492 part 7, not one of §11.0's 25** | `test(=no_planned_search_statement_contains_a_join)` | `crates/pulsus-read/tests/golden_sql_freeze.rs` | **exists**, `Starting 1 test across 1 binary (3 tests skipped)`, 1 passed, exit 0 | **exists** |

The second binary exists and is **env-gated**, which is exactly the trap: run here at `acf44c49`
with `PULSUS_TEST_CLICKHOUSE` unset it printed `Starting 14 tests across 1 binary` and
`14 tests run: 14 passed, 0 skipped`, exit 0 — **and that green is not evidence of anything**,
because each test self-skips internally when the variable is absent. The binary exists — run here it printed `Starting 14 tests` — and CI runs the whole of it with
`cargo test -p pulsus-read --test query_log_gates` in the `schema-it` job, with the variable set
(`.github/workflows/ci.yml:1302-1306`). A local run without it proves nothing, and this document
does not count it.

**The first is vacuous until wave 2 even after wave 1 writes it.** At base the golden corpus
contains no lowered SQL at all, so a no-`WITH` assertion over it would be green over a population
holding none of the case it exists for. It becomes a real check only when a wave emits wrapped
statements into the corpus (ADR 0008 D1), and this document does not count it before that.

**The third is vacuous in the same direction, and says so on its own face.** No statement the
compile core plans contains a join today, and none can: `Relation` has no join slot
(`compile/fold.rs:623`), so a stage cannot contribute one without a type change. Over the
`traces_search/` corpus the assertion is therefore green over a population holding none of the case
it exists for, exactly as the `WITH` row says of itself, and it becomes a real check only if
something writes a join. What it is **not** vacuous about is the other half of its body: the six
committed goldens outside that corpus which carry a join today are pinned as an EQUALITY, so a
seventh anywhere in the golden tree fails it. That half is why the gate walks the golden root rather
than `CORPORA` — two of the six sit in `traces_metrics_base/`, which `CORPORA` does not contain, so
the digest gate above cannot see them either. §9.8 carries the record of all six.

### 11.5 Adding a link variant is to be a build failure — wave 1 made it one

An earlier version of this section said "no crate in this workspace has a compile-failure harness"
and left the check to be run by hand. **That claim was false.** The workspace already has one, in
the form this repository uses for exactly this purpose: rustdoc `compile_fail` fences, in
`crates/pulsus-read/src/logql/predicate.rs` (`:257`, `:295`, `:332`, `:373`) and
`crates/pulsus-read/src/logql/sql.rs` (`:181`, `:195`), with a module doc that sets the bar for them
— *"a fence is only worth what its REMOVAL TEST is worth"* (`predicate.rs:92`) — and a measured
caveat that the annotated error code is not checked at all (`predicate.rs:87-91`, issue #286).
Doctests are not run by `nextest`; CI runs them separately as `cargo test --workspace --doc`
(`.github/workflows/ci.yml:126`, whose own comment says *"nextest never runs doctests"*). Re-run on
this tree at `2f78c53`, and NOT re-taken at `acf44c49`: `cargo test --doc -p pulsus-read` lists all six fences by file and line and
reports `test result: ok. 6 passed; 0 failed`, exit 0.

**What that mechanism reaches, measured rather than argued.** A probe crate with a `pub enum` of two
variants, a function whose `match` over it has no `_` arm, a compiling fence containing the same
exhaustive `match`, and a `compile_fail` fence containing a short one, built with `rustc` and run
with `rustdoc --test`:

| state | the compiling fence | the `compile_fail` fence | `rustdoc --test` exit |
|---|---|---|---|
| baseline | ok | ok | 0 |
| a variant added, the function updated, the fence not | **FAILED** — `error[E0004]: non-exhaustive patterns: &Lnk::C not covered` | ok | **101** |
| a `_` arm put in the function, the enum unchanged | ok | ok | 0 |
| a variant added **and** a `_` arm put in the function **and** the fence updated | ok | ok | 0 |

Three things follow, and all three are decisions rather than observations:

1. **The trigger half belongs in an exhaustive match, not in a fence — and nothing carries it yet.**
   Row 2 shows the trigger is the *exhaustive match with no `_` arm*, wherever it lives; the fence
   is just one place to put it. §11.3 **specifies** that match in
   `crates/pulsus-read/tests/query_lowering_doc_gate.rs` — **wave 1** writes both the file and the
   gates — and this revision adds `every_lql_link_variant_has_a_row_in_the_lowering_document` and
   its TraceQL twin so the *chain link* types are covered and not only the AST stage enums. **That file does not exist at base**:
   run on this tree at `acf44c49`, every selector naming it prints
   `error: no test target named \`query_lowering_doc_gate\` in \`pulsus-read\` package` and exits
   **101** (§11.0). Once **wave 1** writes it, adding an `LqlLink` variant will fail to build a
   binary `cargo test --workspace` builds, and the property will hold on every CI run. Until then it
   holds nowhere, and this is a specification, not a gate. An earlier revision of this section said
   the item — `every_lql_link_variant_has_a_row_in_the_lowering_document`, **wave 1** — "became a
   **real gate**" and that the property "is enforced on every CI run"; both were false when
   written, and correcting them is the whole of §11.0.
2. **A `compile_fail` fence is not added, because it would be vacuous.** Rows 1–4 show it never
   moves: no production change turns it green or red. Adding it would fail this repository's own
   stated bar for fences, which is the removal test — so it is refused for the same reason
   `predicate.rs` refuses to count an entailed fence as a gate.
3. **"and nowhere else" is withdrawn from the claim, because the design itself makes it false.**
   Row 2 is the proof: a second exhaustive site is intended on purpose, and §11.3's
   `every_lql_link_variant_has_a_row_in_the_lowering_document` **will be** that second site once
   **wave 1** writes it — at base it does not exist and its
   selector exits 101, so today there is no second site and the clause was false for a different
   reason as well. A function body is not observable from a doctest or from any test (rows 3 and 4), so
   whether `lower_of` carries a `_` arm cannot be asserted directly at all.

**What will catch a `_` arm, one variant late — also wave 1.** §11.2b gives every link its own row
asserting its own literal residual effect. A `_` arm routes at least two links to one dispatcher, so
the first link that reaches the wrong one fails its row. That is a real consequence and the
only one — §11.2b's `logql::compile::tests::every_residual_state_effect_is_the_one_the_document_states`
and `traces::compile::tests::every_residual_state_effect_is_the_one_the_document_states` carry it, and
like everything else in §11.2b they do not exist at base: both selectors print
`Starting 0 tests across 1 binary (1208 tests skipped)` and exit 4. The syntactic property "no `_`
arm in `lower_of`" is a review obligation on the diff, stated here rather than gated, because
nothing in this repository can see it.

**Nothing in §11.5 is running today.** The four-state probe below was run and its results are
measurements; the *repository consequence* drawn from them is a specification for wave 1. The
distinction is the one §11.0 exists to keep.

---

## 12. The end state

**What this section is.** §§1–11 describe a design and the checks it nominates. This section states
the **end state** the design reaches: what is permanently outside SQL, and why each of those things
is permanent rather than unfinished. It exists because "never lowers" and "has not been lowered
yet" were being written the same way in two records, and a permanence claim that nobody has to
justify is a claim nobody re-checks.

`every_never_reason_variant_is_named_in_the_end_state`
(`crates/pulsus-read/tests/query_lowering_doc_gate.rs`) reads `NeverReason` out of
`crates/pulsus-read/src/compile/fold.rs` and requires a row below for every variant. `NeverReason`
has exactly **eight** variants, and a ninth permanent reason cannot be added to the compiler
without being written down here.

### 12.1 Every permanent reason, and what it rules out

`Capability::Never(reason)` is the compiler's own word for *not lowerable in any state, ever* —
distinct from `Capability::No(reason)`, which means *lowerable in principle, not here*. The two take
byte-identical paths in the fold (`crates/pulsus-read/src/compile/fold.rs:960-967`) and differ only
in the reason string the explain surface renders, so nothing about a request changes with the
choice. What changes is what a reader is entitled to conclude.

| `NeverReason` | what it rules out | why no state can change it |
|---|---|---|
| `NeedsUnwindowedRootRead` | folding the winners' root read into the seed statement | the true root may start before the search window, so the root summary is read trace-wide with **no time predicate**, and `TraceSearchResult.root` is not optional (`crates/pulsus-read/src/traces/exec.rs:385`). A window-bounded statement cannot produce it, whatever has accumulated |
| `StructuralRelation` | pushing `>`, `>>`, `<`, `<<`, `~` into the seed statement | the relation holds between two spans of one trace, over a span set our own batching defines. Nothing in the seed statement's row scope can decide it |
| `NestedSetNumbering` | pushing the modified-preorder numbering | it is computed per trace at query time; no stored column carries it, so there is nothing for SQL to read |
| `TraceLevelIntrinsic` | pushing `traceDuration`, `rootName`, `rootServiceName` or `span:childCount` | they resolve from co-loads that are deliberately trace-wide and unwindowed, so they evaluate full-trace-exact whatever the search window is. A window-bounded statement cannot read those rows, in any state |
| `WholeQueryTypeFailure` | pushing a `!`-operand truthiness leaf | one row's type must fail the **whole** request — a present non-boolean operand under `!` is an error for the query, not a non-match for the span — and SQL evaluates row by row |
| `NoRowToComputeFrom` | pushing an answer about rows that are **absent** | `absent_over_time` is a statement about the empty set; there is no row to compute it from, so no `WHERE` and no `HAVING` can express it |
| `ResponseBuild` | pushing the response builder | the answer's shape is JSON assembled in our process, not a relation |
| `NotASearchLink` | anything at all, on this route | the shipped planner answers `400` for the stage before a chain is built, so no link is constructed and there is nothing to lower |

**What this table does not claim.** That the list is complete for all time. It is complete against
the enum, which is what the check enforces; whether a ninth permanent reason exists is a design
question, and the record's answer is that a new one must arrive with a row here and an argument in
it.

### 12.2 The one thing this section does not settle

Two records disagree about **six constructs**, and the disagreement is not a typo in either.
[`query-to-sql.md`](query-to-sql.md) marks each of them *cannot become SQL* or *never becomes SQL* —
permanence, by that document's own vocabulary table — while the shipped fit answers
`Capability::No(...)`, which is the opposite claim:

| construct | the record marks | the fit answers |
|---|---|---|
| `\| line_format "…"` (general) | *cannot become SQL* | `No(NotYetLowered)` |
| `\| label_format dst="{{…}}"` | *cannot become SQL* | `No(NotYetLowered)` |
| `\| unwrap duration(x)` / `bytes(x)` | *cannot become SQL* | `No(NotYetLowered)` |
| `sum by (<parsed label>) (…)` | *cannot become SQL* | `No(NotYetLowered)` |
| `\|= ip("10.0.0.0/8")` | *never becomes SQL* | `No(NotPushable)` |
| `\| { … }` written after another stage | *never becomes SQL* | `No(NotYetLowered)` |

**The wire cost of resolving it either way is one string on one route.** `set_plan` has exactly one
non-test caller, so the `plan` key reaches a response only from the TraceQL search executor: the
five LogQL constructs' `Capability` is invisible to every client on every option, and the only
user-visible consequence is `links[].why` for a query carrying a mid-pipeline `{ … }` filter. `how`
stays `"residual"` on every option, so the evaluator still runs the stage and the traces returned do
not change.

**It is left open deliberately.** Deciding it is a design call, not an implementation one, and it is
recorded here rather than settled so that the two records cannot drift further apart while nobody
is looking. The check the resolution will need —
`every_permanence_marked_row_is_never_in_the_fit` — is nominated in §11.3 and is **not written**,
because until the decision is made there is no state for it to assert: today it would fail against
either half of the record.

### 12.3 The citations, and the hole that is enumerated rather than papered over

The design record cites source files by line number, and nothing derived those citations until
part 8: moving `search_plan.rs:1854` to `:2854` in [`query-to-sql.md`](query-to-sql.md) and running
`cargo nextest run --workspace` exited 0 with no failing test.

> **This section was reconstructed and the reconstruction cannot be verified.** See the note in
> §9.2b: `git checkout` destroyed the uncommitted text of both this section and that one, no blob
> of it was retained, and what stands here was rewritten from memory. It can be checked for
> internal consistency and against the datasets; it cannot be compared with what it replaced.

**Every figure in this section is generated, not written down.** An earlier revision stated its
census in prose and six of the numbers were wrong at the head; the revision after that derived the
table cells and left the sentences beside them, and a code review changed a prose count with every
suite staying green. Gating prose by pattern is not the fix — numbers in English are unbounded, so
a pattern that catches today's sentences misses tomorrow's and looks like coverage while doing it.
The block below, tables and sentences alike, is rendered from the two citation datasets by
`every_figure_section_12_3_states_is_the_one_the_datasets_hold`, which compares it byte for byte.

<!-- generated from the citation datasets -->

| quantity | at this revision |
|---|---|
| citation occurrences in the five artefacts | 609 |
| of those, citing a bare basename | 477 |
| `(document, token)` pairs the rule resolves | 318 |
| occurrences those resolved pairs cover | 477 |
| `(document, token)` pairs it cannot resolve | 83 |
| occurrences those frozen pairs cover | 132 |
| resolved rows anchored on a token the citing prose prints | 154 |
| resolved rows anchored on a snapshot of the cited line | 164 |

| reason it cannot be resolved | pairs | what it means |
|---|---|---|
| `ambiguous_basename` | 74 | the basename matches several tracked files and the citing line prints no identifier that separates them |
| `blank_target_line` | 4 | the cited line exists and is **empty**, so there is nothing to anchor on |
| `not_a_tracked_file` | 2 | the citation names a throwaway probe that was never committed, which §10 records deliberately |
| `occurrences_disagree` | 3 | the record cites the token more than once in one document and the rule answers differently for two of those occurrences |

| the reviewed verdict on a fallback disagreement | cases |
|---|---|
| the fallback answers a file the citing prose does not describe | 5 |
| the fallback is right and the anchor rule points elsewhere | 3 |
| the sentence describes both candidates, so neither answer is wrong | 1 |

| anchor kind | what a row of that kind can show |
|---|---|
| `prose` | a token the citing prose prints, so the claim and its evidence are reviewable side by side |
| `line` | a snapshot of the cited line, taken because the citing prose prints no such token: it detects the line moving or changing and cannot show the citation means the right thing |

Of the 609 citation occurrences the five artefacts make, 477 name a bare basename. The rule resolves 318 `(document, token)` pairs covering 477 occurrences, and cannot resolve 83 covering 132. Of the resolved rows, 154 are anchored on a token the citing prose prints and 164 on a snapshot of the cited line.

The language fallback and the anchor rule disagree on 9 citations, all of them read one at a time. 5 are citations where the fallback answers a file the citing prose does not describe, which is why it is not applied.

The citations pointing at an empty line are `crates/pulsus-read/src/traces/exec.rs:1968` (in `docs/query-lowering.md`), `search_plan.rs:1042` (in `docs/query-lowering.md`), `traces/exec.rs:114` (cited from 2 documents).

The citations the rule answers differently for two occurrences of are `labels.rs:363` (in `docs/query-to-sql.md`), `sql.rs:489` (in `docs/query-to-sql.md`), `sql.rs:996` (in `docs/query-to-sql.md`).

The citations where the fallback answers a file the citing prose does not describe are `exec.rs:2830-2836` in `docs/query-lowering.md`, `exec.rs:2869` in `docs/query-lowering.md`, `exec.rs:701` in `docs/query-lowering.md`, `labels.rs:157-189` in `docs/query-to-sql.md`. Each is named with its reasoning in `REVIEWED_FALLBACK_DIVERGENCES`, and the test prints them when it runs.

<!-- end generated -->

**These counts move when this section is edited**, because this section cites source files too and
a citation it makes is a citation like any other. Some of the occurrences the block counts are ones
§12.3 added when it began naming the tokens it is about, which is content rather than drift — and
it is why the block is generated rather than typed. Nothing outside the block states one of its
numbers, so there is no second copy to fall out of step.

**The rule lives in `crates/pulsus-read/tests/design_record_drift_gate.rs`, in
`resolve_citation`, and it is the only implementation.** An earlier revision generated the datasets
from a script beside the repository and checked them with a second reader written in the test; the
two drifted on two citations, which is the two-implementations problem in miniature. An `#[ignore]`d
test regenerates both datasets from the one rule.

**The frozen pairs carry a reason each**, in
`crates/pulsus-read/tests/design_record_unresolvable_citations.tsv`. The block above lists the
reasons, counts them and says what each one means: a list of row labels beside a generated table is
a second copy of the table's own labels, and this section has already had one go stale.

**`occurrences_disagree` is a category part 8 did not expect to need.** An earlier revision assumed
a token names one target wherever it is written, so one occurrence with evidence settled the
others, and the check kept the first answer and discarded the rest — which meant it was not
comparing the set. A code review found tokens where the answers differ — the block above counts
them under `occurrences_disagree`. Two contradictory answers are not an answer, so they are frozen
rather than settled by whichever occurrence came first, and the check now computes each key's
verdict over **every** occurrence of it.

`every_citation_in_the_design_record_has_a_row` runs the rule over every citation and compares its
verdict with the two datasets **in every direction**: a citation covered by neither is a hole; a
citation covered by both is covered by neither rule; a resolved row whose citation stops resolving
fails; a frozen row whose citation **starts** resolving fails, naming the file it now resolves to
and saying to move the row. That last direction is what makes freezing a set honest rather than a
place to put inconvenient citations, and an earlier revision promised it and did not have it.

#### The fallback that was rejected, and the cases that rejected it

The obvious next rule for the frozen pairs is the enclosing section's language: a `plan.rs` citation in a
LogQL section means `logql/plan.rs`. **It is not applied, and the reason is a set of citations
anyone can read**, counted in the block above —
`the_language_fallback_disagrees_with_the_anchor_rule_only_where_a_person_has_ruled` finds every
citation where the fallback and the anchor rule disagree, and requires each to carry a verdict a
person reached by reading the citing prose against both candidate files. The block above lists the
verdicts and counts them.

Wrong answers on a rule whose whole job is to say which file a citation means are why it is not
applied; the block above counts them and names them. They are LogQL sections citing the **TraceQL**
executor, where the fallback answers `logql/exec.rs`, which carries nothing of the kind, and a
sentence about the label encoder answered with `logql/labels.rs`.

> **No percentage is published here, and an earlier revision of this section published two.** The
> first, **18%**, came from an experiment that was never committed and counted a case as a
> disagreement when the rule had no candidate in the preferred family at all — a rule that declines
> is not a rule that answers wrongly. The second, **8.26%**, was committed and re-runnable but
> **measured against itself**: it treated `resolve_citation` as the truth, and `resolve_citation` is
> the other rule under test. Reading the cases one at a time showed some where the **anchor rule**
> is the one pointing at the wrong file — the ones now frozen as `occurrences_disagree`. A rate
> computed that way says how often two rules differ, not how often either is wrong. **Named cases
> with their reasoning are worth more than a percentage measured against itself**, and the test
> asserts the set of disagreements is exactly the set that has been read, so a new one cannot
> appear without a person reading it.

**What would close the hole, stated as work rather than promised.** Each of those frozen citing
lines needs to print an identifier the cited line carries — the same rule the resolved ones satisfy
— after
a reading of the cited line against the claim beside it. The `occurrences_disagree` ones are
already read: the review established that their citing prose describes `logql/labels.rs`,
`logql/sql.rs` and `logql/sql.rs`, and those citations need path-qualifying to say so. The `blank_target_line` rows are a smaller job of the same kind: they are citations pointing at
nothing, and each needs a line number that means something. None of it is part 8's.

**Running the regenerator is not a way to make a red check green.** The `line` and `anchor` of a
resolved row are what the DOCUMENT claims, so a target that moves means the record's citation is
stale and a person has to re-read it; re-running the regenerator would rewrite the claim to match
whatever the source had become. The count dataset is the other way round — its `line` column is
derived from an anchor, so regenerating it is the correct response to a document re-wrap. The diff
is the review in both cases.

**What a resolved row can and cannot show** depends on which kind of anchor it carries, and the
block above says what each kind can show and how many rows carry it. The dataset's `anchor_kind`
column is what records the difference per row, so it is visible rather than assumed away.
