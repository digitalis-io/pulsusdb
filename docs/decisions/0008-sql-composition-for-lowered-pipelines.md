# ADR 0008: SQL composition for lowered query pipelines

Status: **Accepted** (2026-09-01), extended 2026-09-02
Issue: [#492](https://github.com/digitalis-io/pulsusdb/issues/492) (compile pipeline stages to SQL instead of evaluating them after transport)

**What the 2026-09-02 extension added, and what it did not.** D1, D2 and D3 are unchanged, and every
measurement below is the original one, re-quoted and not re-taken. What is new is the framing: the
compiler's output is a **plan** of parts rather than one statement, so the Context now says which of
these rules governs *within* a part and which governs *between* two, and names the four cuts —
recording that two of them are recognised from D3's and D2's measurements. And one rule is added
that these three decisions never made: **no emitted SQL may contain a join until this ADR names the
clause**, stated before field selection is built rather than after.
Related: [#507](https://github.com/digitalis-io/pulsusdb/issues/507) (the LogQL stage inventory and its measurements), [#25](https://github.com/digitalis-io/pulsusdb/issues/25) (the 1 TB reference run)

## Context

[query-lowering.md](../query-lowering.md) introduces a shared lowering core: a LogQL or TraceQL
pipeline is folded left, each link contributing to a relational term, and the links that lower
become SQL sent to ClickHouse. The lowered links need not be a prefix — a link that cannot
lower becomes residual and the fold continues (query-lowering.md §2.5). The core deliberately leaves one thing
unanswered, because it is not a property of any stage: **when several stages have lowered, what
SQL shape do they compose into?**

**What the compiler emits is a plan, and these rules govern one SQL part of it.** The compiler's
output is an ordered list of parts, each either one SQL statement or work in our own engine
(query-lowering.md §2.7.1). D1 below decides the shape *within* one SQL part; D3 decides how a value
set crosses *between* two parts; and the boundary between the two is a **cut**, of which there are
exactly four (query-lowering.md §2.7.2–§2.7.5). Two of those four are recognised from measurements
recorded in this ADR, so this document is an input to a compiler decision and not only a constraint
on how SQL is written:

| cut | what in this ADR decides it |
|---|---|
| `Cut::SourceHandoff` — the next read is over a different source, keyed by this one's result | **D3.** A subquery handoff reads 12× the rows of a materialised one, so the second source is a second statement rather than a nested `SELECT` |
| `Cut::HandoffExceedsBound` — the seed does not fit in one statement | **D3's measured ceiling.** 32,768 literal ids is 1,409,081 query bytes and is refused at `Code: 168 … AST is too big. Maximum: 50000`, so a seed larger than the ceiling is sent in chunks |
| `Cut::DisjointSources` — an `OR` whose sides read different sources | **D2.** The common-table form is rejected on measurement, so the union is a second statement merged in our process |
| `Cut::InexactLimit` — the request's `LIMIT` cannot enter the statement | not decided here; it is decided by the link's `Fidelity` (query-lowering.md §2.7.7) |

`Cut::InexactLimit` is listed so the table is the whole of the four rather than the subset this ADR
happens to decide. Recording only three would leave a reader to infer that a cut with no row here
does not exist.

Three shapes are available and the choice is hard to reverse once several stages depend on it.
A pipeline of four stages might become four nested subqueries, or a chain of common table
expressions, or one `SELECT` whose clause slots have been filled in turn. The stages themselves
do not care — they contribute a predicate, a projection or a grouping. The composition is a single
global decision, and it decides how the query performs when a later stage needs an earlier
stage's output more than once.

The standing query-performance mandate makes this a measured decision rather than a stylistic one:
the read path is the product, and a composition that silently re-executes a subtree would be a
performance regression nobody could see in the emitted SQL.

## Methodology

Measured against corpus **C1**, described in [query-lowering.md §9](../query-lowering.md), on
ClickHouse `26.3.17.110` in a container started for this work. Every figure is one
`system.query_log` row per `query_id`, with the trace read path's own settings
([`search_settings`](../../crates/pulsus-read/src/traces/exec.rs), line 2459) plus
`use_query_condition_cache = 0` — see [query-lowering.md §9](../query-lowering.md) for why that
setting is mandatory for any granule measurement on ClickHouse 26.3.

Wall-clock time is **not** used for any claim here. The machine carried other work throughout and
its load average moved from 3.42 to 34.24; two readings of the same query forty minutes apart
differed 5.6x with identical counters. Every decision below rests on counters, which are
load-independent.

## Decision

**D1. Within one SQL part: one `SELECT` accumulating clause slots, wrapped in a subquery exactly
when a stage needs a slot that is already filled.**

`Relation::wrap()` moves the current term into the `FROM` position, clears `grouping`, `ordering`
and `limit`, keeps `shape` and `exact`, and increments a nesting depth. No stage decides its own
SQL shape; the rule is mechanical and lives in one place.

Chosen because the single statement is measurably the cheapest form when it applies, and wrapping
costs nothing when it does not:

| form | granules | rows read | decoded bytes † | peak memory |
|---|---|---|---|---|
| one accumulating statement | 1,105 | 9,052,160 | 388,545,000 | 191,787,342 |
| identical semantics, wrapped one level per stage (3 nestings) | 1,105 | 9,052,160 | 388,545,000 | 191,795,995 |

Identical on every counter; peak memory differs by 0.005%. ClickHouse flattens the nesting. So the
wrap rule can be applied whenever a slot collides without a cost argument attached to it.

**D2. Common table expressions are rejected.** A CTE referenced twice is executed twice.

| form | granules | rows read | decoded bytes † |
|---|---|---|---|
| CTE referenced **once** | 1,105 | 9,052,160 | 388,545,000 |
| CTE referenced **twice** | 2,210 | **18,104,321** | 777,090,001 |
| the same subquery written out twice (control) | 2,210 | **18,104,321** | 777,090,001 |
| one statement, both predicates as `countIf` (control) | 1,105 | **9,052,160** | 388,545,000 |

The CTE and the written-out subquery agree **to the byte**, and both are exactly twice the single
reference. ClickHouse substitutes a CTE textually rather than materialising it, so the construct
buys nothing over the subquery it would replace and hides the cost at precisely the point a reader
would expect it removed — a stage needing an earlier stage's output as a set rather than a stream.

**This is to be enforced rather than merely preferred, and today it is neither: wave 1 writes the
first half and wave 2 is the first wave it can fail on.** The rule is that no emitted SQL may
contain a `WITH` clause. D2's gate has two halves — one **wave 1**, one existing — both nominated by name in
[query-lowering.md §11.4](../query-lowering.md).

`the_golden_sql_corpus_contains_no_with_clause` over the committed golden corpus
(`crates/pulsus-read/tests/golden_sql_freeze.rs`) **does not exist**: run at `2f78c53` its selector
prints `Starting 0 tests across 1 binary (2 tests skipped)` and exits 4. **Wave 1** writes it. Even
then it is vacuous until a wave emits lowered SQL into that corpus, because at base the corpus holds
none of the case the rule is about.

The `system.query_log` half in `crates/pulsus-read/tests/query_log_gates.rs` **does** exist — run at
`2f78c53` the binary prints `Starting 14 tests across 1 binary` and `14 passed`, exit 0 — but that
green is worthless locally: it is env-gated on `PULSUS_TEST_CLICKHOUSE=1` and each test self-skips
without it. CI runs it with the variable set in the `schema-it` job
(`.github/workflows/ci.yml:1302-1306`). A local run without that variable self-skips green, so the
`Starting 14 tests` and `14 passed` recorded above are not evidence of anything.

**D3. A set crossing to the evaluator is handed over as materialised values, never as a subquery.**

When the lowered SQL has run and the evaluator must read rows for the keys it selected, the
key set is sent as a literal `IN` list, not as a nested `SELECT`. This is not a hypothetical
handoff: `Emit` is `Never` (query-lowering.md §3.1), so **every** lowered TraceQL search performs
the winners' root read, and D3 decides how its key set is written. Measured on that read for 20
traces:

| handoff | granules | rows read |
|---|---|---|
| `trace_id IN (<20 literal ids>)` | 100 | **819,200** |
| `trace_id IN (SELECT … LIMIT 20)` | 1,205 | **9,871,360** |

12x the rows for the same 20 traces, because the subquery form leaves the key condition with an
unknown set and it degrades to a scan of the window.

This is why `Limit` is a link in the chain rather than post-processing: it is what makes the
handoff small enough to materialise. The ceiling is real and was measured rather than assumed —
32,768 literal ids is 1,409,081 query bytes and the server refuses it with
`Code: 168. DB::Exception: AST is too big. Maximum: 50000.` Raising `max_query_size` does not
help; the limit is on AST elements. A handoff that cannot be materialised means the `Limit` link
did not lower, and the handoff is a candidate set bounded by its own cap instead.

**Two consequences the plan object makes explicit.**

- **The two ceilings are checked at plan time, in O(1), and decide how a part is issued.** A seed
  rendering over either — 50,000 AST elements, or the 8 MiB query-text ceiling
  `MAX_QUERY_TEXT_BYTES` (`crates/pulsus-read/src/querytext.rs:52`), which answers
  `422 query_too_broad` — makes the part `Issue::PerSeed(Driver::Chunks { .. })` rather than
  `Issue::Once`. No round trip and no probe: the language supplies `handoff_cost(n)` and the core
  compares (query-lowering.md §2.2, §2.7.3). Today's phase-2 batch loop at `BATCH_TRACES = 32` is
  that driver, unnamed.
- **A seed with no plan-time upper bound is not admissible, and the cut is refused rather than
  taken.** Every seed this design admits is bounded by a request parameter, a config field or a
  named constant — the request `limit`, `DEFAULT_MAX_STREAMS`, `reader.traceql_max_candidates`,
  `BATCH_TRACES`. A seed whose size grows with the rows read would cross the metered hop twice and
  grow with the read, which is the opposite of what this work is for; `Lang::handoff_bound`
  returning `None` is what refuses it (query-lowering.md §2.7.6, rule 2).

† **Moves with part layout.** Decoded bytes and bytes read off the file system move with Compact
against Wide storage; granules, rows read, result bytes and peak memory do not. Both layouts were
constructed from identical rows and measured — see [query-lowering.md §9](../query-lowering.md).
The three decisions above rest on granules and rows read, which are layout-invariant, so none of
them turns on a layout-sensitive figure.

## Consequences

- The renderer is a single function over a `Relation`, and the only structural choice it makes is
  where to wrap. That is what makes the emitted SQL reviewable: a golden file shows the whole
  composition, not a stage's opinion about it.
- A stage that genuinely needs a shared subexpression evaluated once has no mechanism available
  in SQL here, and must instead be residual. That is a real limitation of D2 and it is
  accepted: the alternative measured no better.
- D3 couples the size of the handoff to whether the `Limit` link lowered. A pipeline whose `Limit`
  is residual hands over a candidate set bounded by `reader.traceql_max_candidates` instead, which
  is today's model and is unchanged.
- These figures were taken on a single node with a warm page cache and one corpus shape. Nothing
  here is a scale claim; behaviour at 1 TB is [#25](https://github.com/digitalis-io/pulsusdb/issues/25).
- **D3's measured ceiling is now an input to a compiler decision, not only a constraint on how a
  handoff is written.** It decides `Cut::HandoffExceedsBound` and therefore whether a SQL part is
  issued once or per chunk (query-lowering.md §2.7.3).

## A clause these rules do not name: the join

**These three decisions cover exactly three things** — one accumulating `SELECT` with a wrap rule
(D1), the ban on the common-table form (D2), and how a key set crosses from one SQL part to the next
(D3). **A join is none of them, and this ADR does not authorise one.**

That is not academic, and the stage that needs it is already identified.
[query-to-sql.md](../query-to-sql.md) §2.7.3 decides TraceQL field selection — `| select(.foo)` — as
a **left join** whose right side is `trace_attrs_idx` restricted to `key = 'foo'`, one value per
span, projected as an extra column. It chose that on a measurement rather than a preference: the
alternative that stays inside D1 is to widen the selector's own `key` predicate to
`key IN ('service.namespace', 'foo')` and pick the values apart with `anyIf`, and widening loses the
`val` prune, because `val` is the second column of the ordering key. `key = 'service.namespace' AND
val = 'prod'` read **14 of 74** granules; `key IN ('service.namespace', 'foo')` read **51 of 74** —
3.6 times as many. The join keeps the two-column prune and replaces one statement **per batch** with
one per query.

**So the rule, stated before anything is built rather than after:**

> **No emitted SQL may contain a join until this ADR is amended to name the clause.** The amendment
> owes three things, none of which the three decisions above supply: which slot a join occupies in
> the accumulating `SELECT`; what D1's wrap rule does when a later stage needs a slot the join has
> already filled; and whether the join's right side counts against D3's two ceilings, since it is a
> second source read inside one statement rather than a key set crossing between two.

`Relation` (query-lowering.md §2.2) has **no join slot**, which is what makes this enforceable rather
than a request: a stage cannot contribute a join without a type change, and a type change is a
reviewable act. Field selection is therefore **not** in the first implementation wave, and whoever
adds it amends this ADR first. This is recorded as [query-to-sql.md](../query-to-sql.md)'s open
question 4 and is settled here only as far as "not without an amendment".

## Alternatives rejected

**Nested subqueries as the default shape** (one level per stage, unconditionally). Rejected not
on cost — D1's table shows the nesting is free — but because it makes the emitted SQL's depth a
function of the pipeline's length rather than of what the pipeline actually needs, so a reviewer
reading a golden file cannot tell a real slot collision from an artefact of the composition rule.
The wrap-on-collision rule produces the same performance and a legible artefact.

**Common table expressions.** Rejected on measurement (D2): a second reference costs a second full
execution, identical to writing the subquery out twice.

**A temporary table or `MATERIALIZED` CTE for shared subexpressions.** Not measured and not
adopted. It would make the read path stateful, would need a lifecycle and a cleanup path on every
error, and would change what a read is allowed to do to the database. If a future stage set makes
shared subexpressions common enough to matter, that is the point to measure it — this ADR does not
foreclose it, and D2's gates — `the_golden_sql_corpus_contains_no_with_clause`, **wave 1**, and
`query_log_gates`, which exists and prints `Starting 14 tests` at exit 0 — would need an explicit
carve-out recorded here.
