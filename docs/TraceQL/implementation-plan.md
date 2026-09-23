# TraceQL storage and query: the implementation, broken into tasks

Fourth of the documents under `docs/TraceQL/`, and the only one that talks
about the order of work rather than the design. It takes
`functional-requirements.md` (requirements, data, queries, the 75 test cases),
`sql-schema.md` (tables, SQL, measurements) and `server-implementation.md`
(write path, compiler, what is kept, replaced, deleted) as settled, and says
how the tree gets from the six trace tables it ships today to the five the
design describes, one merge at a time.

**The design governs.** Nothing here changes a design decision, and where this
document appears to differ from one, the design is right and this document is
the thing to correct. Where the design left a choice open, §4 takes it and says
so; where a choice needs a measurement that does not exist yet, §5 names it and
names the task whose own plan takes it. Three places where the design's own
scope leaves a behaviour unserved are recorded in §7 as gaps, not closed here.

---

## 1. The rule every task obeys

| the rule | what it means here |
|---|---|
| it lands on its own | one branch, one pull request, the five required checks green — `ci`, `wire-baseline-freeze`, `conformance-evidence-debt`, `schema-it`, `schema-it-cluster` |
| the tree works after it | every existing test passes at that point, or the task's own list names the existing assertion it moves and why; each task below says what still reads and writes the old tables when it is done |
| one implementer, one sitting | no task holds a second one open, and no task waits on a decision another task has not taken |
| tests first | the implementer commits the tests alone — failing on an assertion, with empty stubs where a type does not exist yet — pastes that run, then commits the code that turns them green |
| nothing to migrate | the product has never been released, so no task copies data, and no task carries a cutover protocol |

**Reversibility.** Tasks 1 to 19 add; only task 20 removes. Every task before
it can be reverted by reverting its commit, because the old tables and the old
code are still there and still serving. Task 20 is the point of no return and
is deliberately last.

## 2. The shape of the transition

```
   task 2        the five new tables exist, empty
                 -----------------------------------------------
   task 4        every push writes BOTH stores
                 old six tables  <-- ingest -->  new five tables
                 -----------------------------------------------
   tasks 5-17    one route at a time moves its reads to the new tables
                 fetch, then search, then metrics, then tags, then the graph
                 -----------------------------------------------
   tasks 18-19   the whole corpus, and the reference comparison, against the
                 compiler that now ships
                 -----------------------------------------------
   task 20       the old six tables, the old compiler, the old evaluator
                 and the old half of the write path are deleted
```

Between task 4 and task 20 both stores hold the same spans, apart from the two
cases §2.1 states. That is what makes a route switchable one at a time: a route
reading the new tables and a route still reading the old ones answer over the
same data.

**The search route switches shape by shape, not all at once.** Task 9 puts a
fork in `TraceEngine::search`: a query whose plan the new compiler covers is
answered from the new statement, and every other query is answered by the
engine that serves it today. Tasks 10 to 12 move shapes across the fork until
the old side is empty. The fork is mechanical, not a judgement call — task 9
commits an inventory file listing all 141 accepted corpus queries with the
route each takes and the side that route serves it from, and D6 gives the
number of rows each later task moves. Task 20 deletes the fork.

### 2.1 What the two stores do not agree on while both exist

Two things, and the first is the reason the ingest suppression is in task 4
rather than later.

**(a) A retry separated by more than the suppression window.**

| case | the old tables | the new tables |
|---|---|---|
| the same request body sent twice, inside the suppression window | not stored twice — task 4's suppression is upstream of both inserts | not stored twice |
| the same body sent twice, outside the window | stored twice | collapses on the sorting key; `final = 1` makes the answer exact before the merge |

So for the retries the corpus and the fixture send, the two paths agree from
task 4 onward. A retry separated by more than the suppression window is counted
twice by a route still on the old tables and once by a route already moved.
That residue exists from task 4 to task 20, it is stated here rather than
designed away, and no test asserts the old side's answer to it.

**(b) A push whose inserts do not all commit.** Each target table flushes on its
own generation — that is the shipped model, stated at
`crates/pulsus-write/src/writer/trace.rs:9-18` — and task 4 takes the trace
writer from two targets to six. A sync caller's `200` resolves only when every
generation is durable, so a client is never told a partial write succeeded; it
is told the push failed. The targets that did commit stay committed, and
`PushDedup` marks a claim `Confirmed` as soon as **any** target committed
(`crates/pulsus-write/src/writer/push_dedup.rs:1087-1091`), so a retry inside
the suppression window is suppressed and answered with the original's failure.
Until the client retries **outside** the window, one store can hold spans the
other lacks.

```
   push  --+--> trace_spans        committed
           |
           +--> trace_attrs_idx    committed
           |
           +--> spans              NOT committed   <-- the two stores now differ
           |
           +--> resources          committed
           +--> tag_names          committed
           +--> tag_values         committed

   client sees:   an error, not 200
   retry inside the window:   suppressed, answered with the same error
   retry outside the window:  re-sends the same block
                              trace_spans drops it  (deduplicate_insert)
                              spans collapses on the sorting key
                              -> the two stores agree again
```

What bounds it:

- the registration-shaped targets heal by themselves: a `Poisoned` flush returns
  its rows to the backfill backlog (`crates/pulsus-write/src/writer/backfill.rs`)
  and the cache is **not** promoted, because promotion happens only in
  `on_flush_success` (`crates/pulsus-write/src/writer/registration.rs:16-19`);
- the span-shaped targets do not heal, and the retry outside the window restores
  equivalence rather than doubling, because `span_insert_settings()`
  (`crates/pulsus-write/src/writer/trace.rs:49-55`) pins `deduplicate_insert` on
  the old table and the new table's `ReplacingMergeTree` key collapses the repeat;
- the case is already counted: `pulsus_ingest_dedup_mixed_outcome_total`
  (`crates/pulsus-server/src/ops.rs:256`), which task 4 extends to the trace
  signal.

Task 4's cases inject a failure into each target in turn and assert exactly this
sequence. No test asserts an answer taken while the two stores differ.

## 3. The order, and what depends on what

```
   1  window rule          (independent; task 9 must follow it)

   2  five tables --> 3  row encoder --> 4  dual write
                                              |
          +---------------+-------------------+
          |               |                   |
     5  fetch      6  predicates p1     17  service graph
                         |
                   7  predicates p2 ---> 16  tags
                         |
                   8  search statement
                         |
                   9  search route fork          (after 1 and 8)
                         |
            +------------+------------+
            |            |            |
        10  by()   11  structural  12  trace-level
            |                         |
        13  slice plan            14  metrics     (after 7 and 12)
                                      |
                                 15  compare()

   every one of 10 .. 17 -------> 18  corpus catalogue
                                        |
                                   19  the reference comparison
                                        |
                                   20  delete the old path
```

| task | must follow | why |
|---|---|---|
| 1 | — | it changes the shipped path only |
| 2 | — | it adds tables nothing reads |
| 3 | 2 | the round-trip test needs the tables |
| 4 | 3 | the writer inserts the rows the encoder builds |
| 5, 6, 17 | 4 | they read data, so the data has to be there |
| 7 | 6 | part 2 extends part 1's predicate type |
| 8 | 7 | the statement embeds a predicate |
| 9 | 1, 8 | the fork serves answers, so the window rule must already be the one rule |
| 10, 11, 12 | 9 | each moves rows across the fork |
| 13 | 10 | its acceptance case runs a `select()` over errors, which task 10 adds |
| 14 | 12 | the metrics statement embeds a predicate (task 7) and one served metrics query filters on `nestedSetParent < 0`, whose lowering is task 12's — `explore_root_rate_by_service` in `crates/pulsus-traceql/tests/corpus/grafana/` |
| 15 | 14 | `compare()` is a metrics shape and shares its window and settings |
| 16 | 7 | a narrowed tag-value read compiles the request's `q`, including `resource.…` conditions, which is task 7 |
| 18 | 10–17 | it runs the whole corpus through the shipped compiler |
| 19 | 18 | it compares the shipped routes with the reference, so every route must already be on the new tables |
| 20 | 5, 19 | nothing may be deleted while something still reaches it, and task 5 owns the fetch path task 20 removes |

**What runs at the same time as what.** One pair, and the reason there is only
one is `crates/pulsus-read/src/traces/exec.rs`:

| pair | disjoint because |
|---|---|
| 5 and 6 | task 5 is `traces/spans/{fetch,rows}.rs`, `traces/exec.rs`, `traces_api/assemble.rs` and cases in `traces_api_v2_live.rs` (whose CI step task 1 added); task 6 is `traces/spans/predicate.rs`, `traces/window_sql.rs`, two new CI steps and cases in `traces_compile_v2.rs` and `traces_query_v2_live.rs` |

Every other pair shares a file, and the sharing is stated rather than wished
away:

| file | tasks that edit it |
|---|---|
| `crates/pulsus-read/src/traces/exec.rs` | 5, 9, 10, 11, 12, 13, 14, 15, 16, 17, 20 — every task that puts a route on a new statement |
| `crates/pulsus-read/src/traces/spans/search.rs` | 8, 10, 12, 13 |
| `crates/pulsus-read/src/traces/spans/predicate.rs` | 6, 7 |
| `crates/pulsus-read/src/traces/spans/metrics.rs` | 14, 15 |
| `crates/pulsus-read/tests/traces_route_inventory.tsv` | 9, 10, 11, 12, 14, 15, 20 |
| `crates/pulsus-read/tests/traces_query_v2_live.rs` | 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 |
| `crates/pulsus-server/tests/traces_api_v2_live.rs` | 1, 4, 5, 9, 13, 14, 16, 17, 18, 20 |
| `crates/pulsus-read/tests/traces_compile_v2.rs` | 1, 6, 7, 8 |
| `crates/pulsus-schema/tests/live_traces_v2.rs` | 2, 4, 20 |
| `crates/pulsus-write/tests/trace_rows_v2.rs` | 3, 4, 20 |
| `crates/pulsus-read/tests/golden_sql_freeze.rs` | 1, 8, 14, 17, 20 — the corpus counts and the digest |
| `docs/api.md` | 1, 5, 12, 15 — under D9, a different §4 subsection each |
| `docs/benchmarks/traces-differential-ledger.md` | 1, 5, 12, 15 — one row each |
| `.github/workflows/ci.yml` | 1 (two D7 steps), 2, 3, 6 (one D7 step each), 18 and 19 (one scheduled job each) |

`exec.rs` is 5,909 lines and each read task adds one method to it. The rule, if
two tasks are ever run together despite the table above: add the new method at
the end of the `impl TraceEngine` block, never in the middle, so the conflict is
a two-line one.

## 4. Decisions taken here

These are choices the design leaves open. They are taken now so no task plan
re-takes them, and each says what it costs.

| # | the choice | the decision | the cost |
|---|---|---|---|
| D1 | where the new read code lives | a new directory `crates/pulsus-read/src/traces/spans/`, one file per route — `predicate.rs`, `search.rs`, `structural.rs`, `tracelevel.rs`, `numbering.rs`, `metrics.rs`, `tags.rs`, `graph.rs`, `fetch.rs`, `rows.rs`, `mod.rs`. The old files keep their names until task 20 deletes them | two compilers in the tree between tasks 6 and 20, and no rename lands at the end. The cost is **not** that each file is written once: §3's table gives the three new source files that more than one task edits, and `exec.rs` is edited by eleven |
| D2 | the three `CREATE FUNCTION` helpers in `measure/schema.sql` | none of them ships. `tqd_kv2json` and `tqd_kv2json_skip` exist to load the staging table and have no production caller. `tqd_unescape` is rendered inline as `replaceAll(replaceAll(<expr>, '%2E', '.'), '%25', '%')` wherever a stored path is turned back into an OTLP key | a user-defined function is server-wide, not per database, so it cannot be created or dropped with a tenant's schema and the controller has no mechanism for one. Inlining costs two function calls in the two statements that render a key: the `compare()` key universe and the tag-name read |
| D3 | how the new tables shard | `spans` and `traces` carry `Family::Traces` and a `_dist` wrapper, as `trace_spans` does today. `resources`, `tag_names` and `tag_values` follow `trace_tag_catalog`'s shipped pattern: `family: None`, `Replication::Global`, no `_dist` wrapper | a Global table is written from every shard and converges through its Replacing engine, which is the pattern already in the tree (`crates/pulsus-schema/src/catalog.rs`, migration 18) |
| D4 | the migration ids | new, appended after 63; no existing migration is amended, and **none is removed**. The old 38 trace migrations and 4 trace views stay recorded for good; task 20 appends `DROP` migrations instead | the amendment window closed with issue #498 (`catalog.rs:16-25`), and the controller applies only the records that remain (`controller.rs:108-110`) — deleting a record would leave the table standing in every database that had already reconciled it |
| D5 | where the corpus-scale storage figures are asserted | R2's ≤ 45 B/span, its ≤ 10% index overhead and R3's ≥ 6× are asserted in the required checks on **corpus c1**, whose whole recipe is §4.1 below and which the test generates from a fixed seed. The 2,000,064-span figures stay in `measure/results/` and are re-measured by a scheduled run of `measure/run_all.sh` | a bytes-per-span figure is a property of the corpus, so a test asserting one has to state the corpus it holds for. §4.1 also says which way each term of the model moves between c1 and g1, so a c1 pass is not read as a g1 pass |
| D6 | the shape of the search fork | one inventory file, `crates/pulsus-read/tests/traces_route_inventory.tsv`, **one row per query the parser and the validator accept — all 141**, with three columns: the query's name, the route it reaches (`search`, `metrics`), and the side that route serves it from (`new`, `old`, `refused`). The 3 the planner refuses carry `refused` from task 9 to task 20 and never move. §4.2 gives the row count each task moves; task 20 deletes the file and the fork | the inventory is a committed count a reviewer reads, instead of a claim in a comment that the fork is shrinking. Its domain is stated once, here, and §4.2's numbers add up to 138 + 3 |
| D7 | the five live test files | exactly the five `functional-requirements.md` §8 names, and no more: `crates/pulsus-schema/tests/live_traces_v2.rs`, `crates/pulsus-write/tests/trace_rows_v2.rs`, `crates/pulsus-read/tests/traces_compile_v2.rs`, `crates/pulsus-read/tests/traces_query_v2_live.rs`, `crates/pulsus-server/tests/traces_api_v2_live.rs`. The first task that needs a file adds it **and its one step** to `.github/workflows/ci.yml`; every later task adds cases to a file that already has a step | five new steps in `schema-it` across twenty tasks, rather than twenty |
| D8 | the fixtures the live tests seed | the worked fixture of `functional-requirements.md` §6.1, fixture E of `measure/fixture/make_edge_fixture.py`, fixture M of task 14, fixture G of task 17, fixture P of task 13 and corpus c1 of §4.1 are written once, in Rust, in `crates/pulsus-read/tests/fixtures/`, and shared by every live test that needs them. Their expected answers are the literal tables in §6.1, §8.3, `measure/edge_checks.sh` and the task sections below | one seeding routine to review rather than a dozen, and the expected answers stay the ones the design wrote down |
| D9 | what happens to `docs/api.md` | each task that changes a client-visible behaviour edits the §4 section it changes, in the same commit. No task leaves a documentation edit to a later one | a route whose behaviour moved and whose documentation did not is the defect this rule exists for |
| D10 | `max_recursive_cte_evaluation_depth` for the structural climb | the statement carries `PULSUS_TRACEQL_MAX_DEPTH + 1` — 65 at the shipped default — and the server refuses nothing on account of the database setting | the climb's own bound has to trip first. If the database's limit were the lower of the two, ClickHouse would abort the statement and the route would answer a database error instead of returning the `overflow` row that carries `unresolved`, which is exactly the answer `T-A10` and `T-A11` require. One above the climb's own bound is the smallest value with that property |

### 4.1 Corpus c1 — the corpus D5's storage assertions hold for

Generated in `crates/pulsus-read/tests/fixtures/`, from the constant seed
`20260923`, with no environment input, so two runs produce the same bytes:

| parameter | c1 | g1 | why this value |
|---|---:|---:|---|
| spans | 50,000 | 2,000,064 | 50,000 × 620 B/span is about 31 MB of OTLP bodies, and it leaves the 1,000-span trace at 2% of the corpus rather than 20% |
| traces | 1,761 | 70,413 | one trace of 1,000 spans and 1,760 of 27 or 28, so the mean is 28.4 — g1's ratio, which is the model's `k` |
| largest trace | 1,000 spans | 1,000 spans | the shape that breaks per-trace grouping |
| services | 24 | 24 | unchanged |
| distinct resources | 68 | 68 | unchanged, so `41.5/k` and the resource table's fixed cost are unchanged |
| resource attributes per resource | 15 | 15 | unchanged |
| span attributes per span | 5 on every span, a 6th on 45% of them — mean 5.45 | 5.45 | the `1.99·A` term of the model |
| rows in `tag_values` | 7,600 | 304,070 | 0.152 per span, g1's ratio, so the `5.6·V/spans` term is unchanged |
| days | 1 | 1 | the storage figures are per-day-invariant |
| request bodies | 102 | 4,073 | one body per service per trace group |
| bodies sent twice | 1 | 40 | so R11's suppression is exercised by the same corpus |
| OTLP bytes on the wire | about 31 MB | 1,239,967,396 | 620 B/span, g1's measured rate |

**What a c1 pass does and does not establish.** The model of
`sql-schema.md` §2.1 is `23.2 + 1.99·A + 41.5/k + 5.6·V/spans` bytes per span.
`A` (attributes per span), `k` (spans per trace) and `V/spans` (distinct values
per span) are held at g1's values above, so every term of the model is the same
at both scales and a c1 figure is comparable with g1's 37.275. What c1 cannot
establish is anything that depends on part count or on column entropy at scale:
the compression ratio is measured after `OPTIMIZE TABLE spans FINAL`, so the
ratio is read off merged parts at both scales, and that is the term to re-check
in the scheduled `run_all.sh` run rather than to trust from c1.

**Why the 2M-span figures are not a required check**, with the figure rather
than an adjective: g1 is 1,239,967,396 bytes of OTLP request bodies, it is not
a committed artifact, and a required check would have to generate and push it
on every pull request. c1 is about 31 MB on the same rate.

### 4.2 What each task moves across the fork

The inventory's domain is D6's: 141 rows, one per query under
`crates/pulsus-traceql/tests/corpus/accept/` and `grafana/`. The 12 queries of
`measure/catalogue-extra.tsv` are **not** corpus queries and are not rows of
this file; they are task 18's, which runs them the same way.

| task | rows it moves to `new` | which shapes | running total `new` |
|---|---:|---|---:|
| 9 | 84 | every query whose plan is one filter chain: attributes, intrinsics, arithmetic, regex, existence, scopes, boolean and spanset combinators, hints, static keywords, string escapes, and the two nested-set comparisons that compile to `true` | 84 |
| 10 | 14 | `by()`, `coalesce()`, `select()`, the five aggregate filters, and a `{…}` filter as a later stage | 98 |
| 11 | 13 | the structural-operator queries — thirteen of them, between them reaching the fifteen forms — including the one that also carries `\| count()` | 111 |
| 12 | 8 | `trace:duration`, `trace:rootName`, `trace:rootService` and their legacy spellings, `span:childCount`, `nestedSetParent < 0` | 119 |
| 13 | 0 | the slice plan changes how a search is executed, not which shapes are covered — a stated zero, not an omission | 119 |
| 14 | 16 | every metrics shape but `compare()` | 135 |
| 15 | 3 | the three `compare()` queries | 138 |
| — | 3 | the three the planner refuses stay `refused` throughout | 141 rows |

The counts were derived by classifying every row of
`docs/TraceQL/query-catalogue-accepted.md` and subtracting the 12 of
`measure/catalogue-extra.tsv`; each task's own test asserts its number, so a
misclassification here fails that task rather than surviving it.

## 5. Decisions a task's own plan must take

Each of these needs a measurement or a source read that has not been done, and
each is named against the task that must take it before writing code.

| # | task | the question | why it cannot be settled here |
|---|---|---|---|
| Q1 | 3 | **Can the vendored driver write a `JSON` column, and in which form?** `vendor/clickhouse/src/rowbinary/validation.rs:624-635` accepts a `JSON` column against a serde `Str`/`String` and nothing else; a `serde_bytes` field maps only to `DataTypeNode::String`. The binary form the design names (`server-implementation.md` §2.3) is a path count followed by `(path, type tag, value)` triples, which is not a length-prefixed string. **This stays `[unverified]` in every document until task 3's probe runs**; it is settled in code, not on paper | every insert in `measure/` went through `INSERT … SELECT CAST(<text> AS JSON)` from a staging table (`measure/schema.sql:100-111`), so the encoding through our own driver has never been exercised. Task 3's plan states the probe, runs it, and pastes the result. If the binary form needs a driver change, that change is part of task 3 and is named in its plan |
| Q2 | 3 | **Is `json_type_escape_dots_in_keys` needed at all?** The measurement set it to 1 on every insert (`measure/apply_schema.py:16`) because it loaded text JSON. The writer escapes `%` then `.` itself, so a flat key carries no dot by the time ClickHouse sees it, while a nested object's structural dots must survive | the setting's interaction with the binary form is not recorded anywhere in the artifact. The probe is one span carrying a flat key `a.b` and one carrying a nested `{"a":{"b":1}}`, each read back by the query `sql-schema.md` §3.1 names |
| Q5 | 1 | **Does the `compare()` differential against the reference still hold once the selection window moves?** `crates/pulsus-read/tests/compare_value_differential.rs` compares our `compare()` answer with the reference's for `{} \| compare({ status = error })`, which passes no `start`/`end` arguments, so task 1 should not move it | "should not" is an argument. Task 1's plan runs that suite and pastes the result rather than reasoning about it |
| Q6 | 3 | **Where does an `AnyValue` with no arm set go?** `sql-schema.md` §3.3 covers the empty object, the bytes value and the unrenderable array, and does not cover an `AnyValue` carrying no value at all. The two candidates are: the key is absent, or the key goes to `attrs_other`. The second makes `{ .k != nil }` answer `false` for a key the sender did send, because that predicate reads `dynamicType(attrs.k)` | the design's table does not reach it and no measurement produced one. Task 3's plan states the choice, its effect on `{ .k != nil }` and `{ .k = nil }`, and — if it diverges from the reference — the ledger row and the `docs/api.md` §4.2 entry |

Two questions that might look open are not. `resources.day` is settled by the
approved DDL:
`measure/schema.sql:46-56` declares `day Date` and `PARTITION BY day`, and
`sql-schema.md:905-907` requires all three retained tables to be partitioned by
day. The recursion setting is now D10.

## 6. The tasks

Each section gives: what it changes, what it must not break, what still uses
the old path when it is done, its test cases, and what makes it done. A test
case named `T-xx` is the row of that id in `functional-requirements.md` §8,
which carries its literal input and its literal expected value; a case with a
name and no `T-` id is new here and is written out in full.

---

### Task 1 — One window rule on the shipped path

Requirement R9. The only task that changes an answer without touching a new
table, and the reason it is first: every later task inherits one window
convention instead of three.

**Changes**

- `crates/pulsus-read/src/traces/search_sql.rs:124` — `bounds` returns
  `WindowSql::start_closed_end_open`.
- `crates/pulsus-read/src/traces/tags_sql.rs` — the two store-backed reads
  (`span_name_values_sql` at `:253-270`, `attr_values_narrowed_sql` at
  `:282-312`) take the half-open window at nanosecond precision **in addition
  to** the day bound, and the day bound is rendered from the last included
  nanosecond instead of from the window's end.
- `crates/pulsus-read/src/traces/metrics_sql.rs:1365` — `compare()`'s
  `sel_window` takes `start_closed_end_open`.
- `crates/pulsus-read/src/traces/tags_sql.rs`'s own unit tests — three byte-exact
  assertions move: `unnarrowed_span_name_sql_is_byte_exact` (`:436`),
  `narrowed_span_name_sql_is_byte_exact` (`:450`) and
  `narrowed_attr_values_sql_is_byte_exact` (`:471`). Each is rewritten to the
  new literal text; a reviewer checks the only difference is the added time
  clause and the day literal.
- `crates/pulsus-read/tests/traces_tags_explain.rs:909-1035` —
  `span_name_projection_is_selected_and_prunes` is replaced by
  `span_name_read_prunes_on_the_row_bound`. **This is a deliberate loss of a
  performance property in exchange for a correct answer**, and it is the one
  place in this task where an existing assertion cannot simply be re-rendered:
  the `span_name_day` projection is keyed on the day expression, so a
  `timestamp_ns` predicate defeats it — the reason the shipped read carries no
  such predicate is written in `tags_sql.rs:245-247`. `T-T6` requires the read
  to exclude a name whose only span is earlier the same day, which the day
  bound alone cannot do. The replacement keeps the property the test exists
  for — that the read prunes — by comparing the bounded statement's selected
  granules with the same statement with both time clauses removed, out of the
  same denominator, and requiring strictly fewer. The projection itself is
  deleted by task 20 (`server-implementation.md` §6, "Deleted"), so this is
  early rather than new.
- `crates/pulsus-read/tests/golden/traces_search/*.sql` (75 files) — regenerated,
  and the diff read.
- `crates/pulsus-read/tests/golden/traces_metrics/compare_status_window.sql` —
  regenerated. It pins `timestamp_ns > 1700000005000000000 AND timestamp_ns <=
  1700000008000000000`, which is `compare()`'s selection window at line 13 and
  in each of the five sections below it.
  `crates/pulsus-read/tests/golden/traces_metrics_base/compare_status_window.sql`
  is **not** touched: it is the pre-#477 historic copy, no test regenerates it
  and none pins its bytes (`golden_sql_freeze.rs:727-742`).
- `crates/pulsus-read/tests/golden_sql_freeze.rs` — `PINNED_SQL_CORPUS`
  (`:465`, today `0xfb65_48d6_14d0_6004`) takes its new value, and the
  regeneration history in that file's doc comment gains its entry. `CORPORA`'s
  two counts do not move: 75 and 28 files, none added, none removed.
- `docs/api.md` §4.2, §4.3, §4.4 — the three bound statements.
- `docs/benchmarks/traces-differential-ledger.md` — **one new row**, for
  `compare()`'s window only. There is no row to correct for the search window:
  the difference was never recorded as one.
- `.github/workflows/ci.yml` — under D7, the first two of the five steps:
  `cargo test -p pulsus-server --test traces_api_v2_live` and
  `cargo test -p pulsus-read --test traces_compile_v2`, both in `schema-it`
  beside the existing trace steps at `:1957-1962`.

**Must not break** — the service-graph window (`graph_sql.rs:65-67`) and the
metrics evaluation window (`metrics_sql.rs:86-114`) are already half-open and
are not touched. The per-step range selector inside a metrics query
(`metrics_plan.rs:473-482`) keeps its right-closed instants and is out of scope.
`crates/pulsus-read/tests/traces_metrics_explain.rs` and the rest of
`traces_tags_explain.rs` pass unchanged.

**Still on the old path afterwards** — everything. Six tables, one compiler,
one evaluator; only the bound moved.

**Test cases**

| case | file | what it pins |
|---|---|---|
| `T-B1`, `T-B2` | `crates/pulsus-server/tests/traces_api_v2_live.rs` | a span at exactly `start` is returned, one at exactly `end` is not. Fixture B of §8.2 |
| `T-B3` | same | the day bound is rendered from the last included nanosecond: a search ending at the next midnight reads one day's partitions, not two |
| `T-B5` | `crates/pulsus-read/tests/traces_compile_v2.rs` | one search, one fetch, one tag-value read, one metrics range query, one service-graph request — every time clause is `>= s AND < e`. Against the **shipped** column name at this point; task 8 re-asserts it against `start_ns`. The graph and metrics halves pass before the change and must pass after |
| `T-B6` | `crates/pulsus-server/tests/traces_api_v2_live.rs` | **guard.** The `gw → checkout` edge is absent because its server span starts at exactly `end` |
| `T-B7` | same | `/api/v2/search/tag/span.http.route/values?q={span.tenant="t1"}&start=1790038800&end=1790121600` answers exactly `["/inside"]` — `/only-before` is an hour before the window opens on the window's first day |
| `T-B8` | same | a span at exactly `compare()`'s `end` argument counts in the **baseline**, not the selection |
| `T-T6` | same | `/api/v2/search/tag/name/values` over the same fixture answers only the in-window names. It fails today because the unnarrowed read is day-widened |
| `span_name_read_prunes_on_the_row_bound` | `crates/pulsus-read/tests/traces_tags_explain.rs` | the rewritten case: the statement's selected granules against the same statement with both time clauses removed, same denominator, strictly fewer. The plan pastes both figures |
| `the reference comparison still holds` | `crates/pulsus-read/tests/compare_value_differential.rs` | not a new case: the existing suite is run against a live reference and its output pasted, answering Q5 |

`T-B4` — the bucket bound — is **not** in this task: the bound it names is over
the new span table's sort key. It is task 8's.

**Done when**

1. `T-B1`, `T-B2`, `T-B3`, `T-B5`, `T-B7`, `T-B8`, `T-T6` and
   `span_name_read_prunes_on_the_row_bound` pass, and `T-B6` still passes.
2. The 75 search goldens and `traces_metrics/compare_status_window.sql` are
   regenerated and every moved line is a window operator or a day literal — a
   reviewer reads the diff and finds nothing else — and `PINNED_SQL_CORPUS` is
   updated in the same commit.
3. The three byte-exact `tags_sql.rs` assertions carry the new text, and the
   replaced explain case carries its two granule figures.
4. `docs/api.md` §4.2, §4.3 and §4.4 state `start <= ts < end`, and the §4.3
   sentence about widening to every UTC day is gone.
5. `docs/benchmarks/traces-differential-ledger.md` has exactly one new row, for
   `compare()`, naming the endpoint.
6. `cargo test --workspace` is green, the two new CI steps are in `schema-it`,
   and the reference differential suites are run live and their output pasted.

---

### Task 2 — The five tables and the view

**Changes**

- `crates/pulsus-schema/src/catalog.rs` — five `Migration` records appended
  after id 63 (`spans`, `resources`, `traces`, `tag_names`, `tag_values`),
  their `_dist` records where D3 gives one, and one `MvDef` for `traces_mv`.
  DDL transcribed from `measure/schema.sql:16-99` with the repository's tokens
  (`{{db}}`, `{{on_cluster}}`, `{{retention_days}}`). `resources` carries
  `day Date` and `PARTITION BY day` exactly as that file declares them —
  `sql-schema.md:905-907` requires all three retained tables to be partitioned
  by day, so there is no choice to take here.
- `crates/pulsus-schema/src/controller.rs` — `TTL_STMTS` gains six statements,
  in the saturating form the array already uses:
  - `ALTER TABLE {{db}}.spans{{on_cluster}} MODIFY TTL toDateTime(least(intDiv(start_ns, 1000000000) + {{retention_days}} * 86400, 4294967295)) DELETE;`
  - `ALTER TABLE {{db}}.spans{{on_cluster}} MODIFY SETTING ttl_only_drop_parts = 1;`
  - the same pair for `resources` and for `traces`, whose time column is a
    `Date`, so the seconds expression is `toUInt32(day) * 86400`.

  `tag_names` and `tag_values` get **no** statement, because `docs/api.md` §4.3
  requires catalog entries to outlive span retention.
- `crates/pulsus-schema/src/render.rs` — the `_dist` wrapper set follows D3.
- `crates/pulsus-server/src/chconfig.rs` — the new table names, `_dist`-aware,
  added to `TraceReadConfig` and to the writer's table set. Nothing reads them
  yet.
- `crates/pulsus-server/src/ops.rs` — per-table operational metrics for the new
  tables, beside the old ones.
- `.github/workflows/ci.yml` — one step in `schema-it` running
  `cargo test -p pulsus-schema --test live_traces_v2`, and one assertion added
  to the existing `schema-it-cluster` leg.

**Must not break** — the six old tables, their views, their `_dist` wrappers
and every existing schema test. `mvs_are_exactly_the_expected_set` in
`catalog.rs` gains one name and keeps the other six.

**Still on the old path afterwards** — everything. The new tables exist and are
empty.

**Test cases**

| case | file | what it pins |
|---|---|---|
| `T-S5` | `crates/pulsus-schema/tests/live_traces_v2.rs` | every column of `spans` carries a compression codec — none empty in `system.columns` |
| `T-S6` | same | applying the schema twice is a no-op: no error, no column re-added |
| `T-R1` | same | `ALTER TABLE spans DROP PARTITION` on the older of two days: the day is gone, `system.mutations` and `system.merges` are empty, the other day is intact |
| `T-R2` | same | **two literal assertions.** (a) the rendered `spans` TTL statement is byte-for-byte `ALTER TABLE <db>.spans MODIFY TTL toDateTime(least(intDiv(start_ns, 1000000000) + 7 * 86400, 4294967295)) DELETE;` at `PULSUS_RETENTION_DAYS = 7`; (b) for a span at `4294943999000000000` ns — 2106-02-06T23:59:59Z, the top of the admitted domain — `SELECT toDateTime(least(intDiv(4294943999000000000, 1000000000) + 7 * 86400, 4294967295))` answers `2106-02-07 06:28:15`, the clamp, not a wrapped 1970 instant |
| `the sorting key is the one the design states` | same | `SELECT primary_key, sorting_key FROM system.tables WHERE database = <db> AND name = 'spans'` — the pattern `live_traces.rs:504-510` already uses, because a per-column Boolean cannot carry an expression or an order. `sorting_key` is `intDiv(start_ns, 300000000000), trace_id, start_ns, span_id, kind`; `engine_full` contains `index_granularity = 2048`. The same query over `traces` gives `trace_id` and `index_granularity = 1024`, and over `resources` gives `service, resource_id`. If ClickHouse renders an expression differently from the text above, the assertion takes the rendered text and this document's value is corrected in the same commit, with the observed text pasted. It fails today because the tables do not exist |
| `the catalogs carry no TTL` | same | `system.tables.engine_full` for `tag_names` and `tag_values` contains no `TTL`, while `spans`, `resources` and `traces` each do. It fails today because the tables do not exist |
| `the clustered form` | `crates/pulsus-schema/tests/live_cluster.rs` | `spans_dist` and `traces_dist` exist with sharding key `cityHash64(trace_id)`; `resources`, `tag_names` and `tag_values` exist on every shard and have **no** `_dist` twin — the shape `trace_tag_catalog` already has at `:291-301` |

**Done when**

1. The seven cases above pass; `cargo test -p pulsus-schema` is green.
2. The two CI steps are in place.
3. `docs/schemas.md` gains the five tables' DDL, beside the six that are still
   there.
4. No existing migration record is amended or removed — a reviewer diffs
   `catalog.rs` and sees only appended records.

---

### Task 3 — The span row encoder, and the JSON column through the driver

The task that answers Q1, Q2 and Q6 before anything depends on them. Pure code
plus one live round-trip; no route and no writer wiring changes.

**The test seam, because there is no compiler and no route yet.** Every case
below inserts rows with the new encoder directly and reads them back with a
literal SQL statement written out in this section. None of them goes through
`TraceEngine`, a `q=` parameter or an HTTP route — those arrive in tasks 5 to
17, and each re-asserts the same behaviour through the route it adds.

**Changes**

- `crates/pulsus-write/src/writer/rows.rs` — the new row shapes: one span row,
  one resource row, one tag-name row, one tag-value row. `TraceSpanRow` and
  `TraceAttrRow` stay where they are.
- `crates/pulsus-write/src/protocols/otlp_traces.rs` — the decoder is kept; a
  new encoder turns a decoded span into the new row. The payload-blob and
  attribute-row builders stay until task 20.
- `vendor/clickhouse/` — only if Q1's probe says the driver cannot write the
  column. If it can, this line is empty and the plan says so.

**Must not break** — `trace_ingest_fidelity.rs`, `trace_ingest_roundtrip.rs`
and the payload builder they pin. The new encoder is additional.

**Still on the old path afterwards** — everything. The encoder has no caller in
production code.

**Test cases**

| case | file | what it pins |
|---|---|---|
| `T-A3` | `crates/pulsus-write/tests/trace_rows_v2.rs` | insert a span with the flat key `a.b` and one with the nested value `{"a":{"b":1}}`; ``SELECT count() FROM spans WHERE coalesce(attrs.`a%2Eb`.:Int64 = 1, false)`` answers **1**, and ``SELECT count() FROM spans WHERE coalesce(attrs.a.b.:Int64 = 1, false)`` answers **1** for the other span, never the same row for both |
| `T-A4` | same | an attribute literally named `a%2Eb` stores path `a%252Eb`: ``SELECT count() FROM spans WHERE dynamicType(attrs.`a%252Eb`) != 'None'`` is 1 and ``…attrs.`a%2Eb`…`` is 0 for that span, and `replaceAll(replaceAll('a%252Eb', '%2E', '.'), '%25', '%')` returns `a%2Eb` |
| `T-A5` | same | one span carrying `k="first"` and `k="second"`: the insert succeeds and ``SELECT attrs.`k`.:String FROM spans …`` is `first` |
| `T-A6` | same | attributes `+Inf`, `-Inf` and `NaN` written through the driver, not through `CAST(<text> AS JSON)`. ``SELECT count() FROM spans WHERE coalesce(attrs.`k`.:Float64 > 500, false)`` answers **1** — the `+Inf` span; ``… .:Float64 < -500 …`` answers 1; ``… isNaN(attrs.`k`.:Float64) …`` answers 1; and `toTypeName` of each read is `Nullable(Float64)`. **This is Q1's probe** |
| `T-S2` | same | ingest one span with `service.name = "checkout"`: ``SELECT count() FROM resources WHERE dynamicType(attrs.`service%2Ename`) != 'None'`` is **0**, and `SELECT service FROM spans` is `checkout` |
| `T-W5` | same | 1,000 spans of one resource in one day produce exactly 1 row in `resources` |
| `the resource id is 128 bits and stable` | same | five assertions, each a pair of encoded spans. (1) byte-identical resources → the same `resource_id`. (2) the **same pairs in a different order** → the same `resource_id`; this is the invariant `crates/pulsus-model/src/labels.rs:38-42` already states for a label set, and an identity built over the encoded bytes in arrival order fails it. (3) one attribute **value** changed → a different id. (4) one attribute **key** changed → a different id. (5) the same pairs with a different `schema_url` → a different id, because the design makes the schema url part of the identity (`server-implementation.md:60-62`). `system.columns.type` for `spans.resource_id` is `UInt128`. It fails today because no such identity exists |
| `every OTLP value kind round-trips` | same | one span per row of the table below, each read back with the statement given. The list is the seven `AnyValue` arms plus the four composites the design's §3.3 table singles out; a twelfth input, an `AnyValue` with **no** arm set, is Q6's and the plan states the answer it took |

The value-kind table, which is the case's whole expected result:

| input `AnyValue` | stored where | read back by |
|---|---|---|
| `string_value: "s"` | ``attrs.`k`.:String`` | ``SELECT attrs.`k`.:String`` → `s` |
| `bool_value: true` | ``attrs.`k`.:Bool`` | → `true` |
| `int_value: 7` | ``attrs.`k`.:Int64`` | → `7` |
| `double_value: 1.5` | ``attrs.`k`.:Float64`` | → `1.5` |
| `array_value: ["a","b"]` | ``attrs.`k`.:`Array(Nullable(String))` `` | ``has(…, 'b')`` → 1 |
| `array_value: [1,2]` | ``attrs.`k`.:`Array(Nullable(Int64))` `` | ``has(…, 2)`` → 1 |
| `array_value: ["a",1]` | `attrs`, a JSON array | `dynamicType` is not `'None'`, and `toString` of the value is `['a',1]` |
| `array_value: [bytes]` | `attrs_other` | ``dynamicType(attrs.`k`)`` is `'None'`; `attrs_other` contains the key |
| `kvlist_value: {"a": {"b": 1}}` (non-empty) | nested paths in `attrs` | ``SELECT attrs.`k`.a.b.:Int64`` → `1`, and ``dynamicType(attrs.`k`)`` is not `'None'` |
| `kvlist_value: {}` (empty) | `attrs_other` | ``dynamicType(attrs.`k`)`` is `'None'`; `attrs_other` contains the key |
| `bytes_value` | `attrs_other` | same |
| no arm set | Q6 | the plan states it, with its effect on `{ .k != nil }` |

**Done when**

1. The eight cases pass against the tables task 2 created, every one of them
   through a direct insert and a literal `SELECT`.
2. The plan's answer to Q1, Q2 and Q6 is pasted: the probe, its output, and —
   if the driver needed a change — what changed and what else reads that code.
   Q1 stays written `[unverified]` in every document until this run exists.
3. `.github/workflows/ci.yml` gains one step running
   `cargo test -p pulsus-write --test trace_rows_v2`.
4. No production caller of the new encoder exists: a reviewer greps for it and
   finds only tests.

---

### Task 4 — Dual write

**Changes**

- `crates/pulsus-write/src/writer/trace.rs` — the writer gains the four new
  target tables beside its two, and the module doc's consistency model is
  rewritten for six targets. Two caches, both promoted **only** in
  `on_flush_success` and returned to a backlog in `on_flush_poisoned`, which is
  the shipped registration pattern (`writer/registration.rs:16-19`,
  `writer/backfill.rs:1-40`): a `(resource_id, day)` set for `resources`, and a
  `(scope, key[, value, type])` set for the two catalogs.
- `crates/pulsus-write/src/writer/backfill.rs` — the three new registration row
  shapes join the generic backlog, the way `TraceAttrRow` already has. Their
  `on_healed` hook promotes membership, which is safe for a pure set.
- `crates/pulsus-write/src/writer/{mod,table,config,metrics}.rs` — the table
  list and per-table metrics grow; nothing is removed.
- `crates/pulsus-write/src/writer/push_dedup.rs` — a `trace_identity` beside
  `log_identity` (`writer/mod.rs:454`) and `metric_identity`
  (`writer/metric.rs:565`).
- `crates/pulsus-write/src/ingest/traces.rs` and `writer/trace.rs:186-189` — the
  trace writer stops passing `dedup: None` and takes the suppression index,
  upstream of **all six** inserts, which is what keeps the two stores agreeing
  (§2.1a).
- `crates/pulsus-server/src/ops.rs` — the dedup counters gain the `trace`
  signal, beside the two they already carry at `:256`.
- `docs/configuration.md:100-102` — the three `PULSUS_INGEST_DEDUP*` rows are
  **already there** and the settings are **already parsed**
  (`crates/pulsus-config/src/env.rs:58-60`, `:310-317`). No new setting is
  added and no new row. The edit is to the wording of those three rows, which
  today say "entries or samples": they gain the trace push.
- The `traces_mv` view created in task 2 starts receiving rows.

**Must not break** — the old two-table write path, its flush semantics, its
backpressure and its metrics; and `span_insert_settings()`
(`writer/trace.rs:49-55`), whose two block-deduplication settings are what makes
a repeated identical block a no-op on the old side. The new span insert carries
the same pair, for the same reason.

**Still on the old path afterwards** — every read. Both stores hold the same
data except in the two cases §2.1 states.

**Test cases**

| case | file | what it pins |
|---|---|---|
| `T-W1` | `crates/pulsus-write/tests/trace_rows_v2.rs` | the fixture's six bodies with one sent twice, in one insert block: `count()` is 9 and `count() FINAL` is 9 |
| `T-W2` | same | the same body in two separate inserts: `count() FINAL` is 9 before any merge |
| `T-W3` | same | one span id with kinds 2 and 3 — a shared span — is 2 rows after `FINAL` |
| `T-W6` | same | break the per-trace view, then insert: the insert fails and no span is stored without its index row |
| `the suppression covers traces` | `crates/pulsus-server/tests/traces_api_v2_live.rs` | the same OTLP body posted twice inside the window stores its spans once in **both** stores: `count()` on `trace_spans` and on `spans` are equal and are the body's span count. It fails today because the trace route passes `dedup: None` (`writer/trace.rs:186-189`) |
| `a failed target does not promote its cache` | `crates/pulsus-write/tests/trace_rows_v2.rs` | six sub-cases, one per target. For each, the target's inserter is made to fail `Poisoned` for one push of 100 spans over 3 resources and 12 catalog keys, and the assertions are: (a) the push does **not** answer `200`; (b) the five other targets hold their rows; (c) if the failed target was `resources`, `tag_names` or `tag_values`, its cache holds **no** key from that push, so the next push re-emits the rows; (d) after the backlog heals, the failed target holds exactly the rows the others imply — 3 resource rows, 12 name rows. A cache promoted at admission rather than on flush success fails (c) and then (d) |
| `a retry outside the window restores equivalence` | same | the same push, one target failed, then the same bytes re-sent after `PULSUS_INGEST_DEDUP_WINDOW` has elapsed: `count() FINAL` on `trace_spans` and on `spans` are both 100. Without `deduplicate_insert` on the old side the old table holds 200 |
| `a retry inside the window is suppressed and reports the failure` | same | the same push, one target failed, the same bytes re-sent inside the window: nothing is stored by the retry, the retry is answered with the first push's failure, and `pulsus_ingest_dedup_mixed_outcome_total{signal="trace"}` is 1. This is §2.1b, asserted rather than described |
| `T-S3`, `T-S4` | `crates/pulsus-schema/tests/live_traces_v2.rs` | on corpus c1 (§4.1): total bytes per span ≤ 45, non-span tables ≤ 10% of the span table, and the span table's compression ratio ≥ 6 after `OPTIMIZE TABLE spans FINAL` |
| `T-W4` | `crates/pulsus-schema/tests/live_cluster.rs` | load c1 into replica 1, `SYSTEM SYNC REPLICA` on replica 2, then read replica 2's `system.part_log`: bytes fetched per span against the active part bytes per span, within 5%. A ratio, so it holds at c1's scale as well as g1's; g1 measured 34.965 against 34.923, which is 1.0012× |

**Done when**

1. The ten cases pass.
2. A push writes both stores: a live test reads the same span count from
   `trace_spans` and from `spans` after one c1 load.
3. The three `PULSUS_INGEST_DEDUP*` rows in `docs/configuration.md` name the
   trace push, and no new setting was added.
4. The write-path metrics name both table sets, and `ops.rs` reports both,
   including the dedup counters under the `trace` signal.

---

### Task 5 — The trace fetch on the new tables

The smallest read, and the one that exercises every column of the span row.

**Changes**

- `crates/pulsus-read/src/traces/spans/fetch.rs` — the §5.4 statement: the
  trace's extent from `traces`, the spans by key, the distinct resources in the
  same statement; **and the fallback** of `server-implementation.md:207`, for a
  trace not yet in the per-trace table. When the extent subquery returns no
  row, a second statement reads the span table over the **request's** window
  instead of the trace's extent. That is the second of §3.5's four
  multi-statement cases, it happens only inside the view's flush interval, and
  `T-Q1` counts 2 for it.
- `crates/pulsus-read/src/traces/spans/rows.rs` — the row shape it decodes.
- `crates/pulsus-read/src/traces/exec.rs` — `fetch_by_id` issues the new
  statement, and the fallback when the first answers nothing.
- `crates/pulsus-server/src/traces_api/assemble.rs` — builds the OTLP response
  from columns instead of decoding a stored payload, and puts the service name
  back into the resource it renders.

**Must not break** — the fetch response as an **OTLP value**. It is not the same
bytes: the design states at `server-implementation.md:50-56` that two things are not
preserved byte for byte, neither of them part of the OTLP data model —
attribute **order** (the JSON column returns keys sorted) and a **duplicate key
inside one span** (the first value is kept, which is the rule `docs/api.md`
§4.2 already states). Every golden and conformance assertion that pins
attribute order inside a fetched span is therefore regenerated in this task,
the diff read, and each moved line shown to be a reordering and not a changed
value. The `(span_id, kind)` de-duplication and the canonical span order stay
exactly as they are.

**Still on the old path afterwards** — search, metrics, tags, the service
graph. `trace_spans.payload` is still written and is now read by nothing.

**Test cases**

| case | file | what it pins |
|---|---|---|
| `T-W7` | `crates/pulsus-server/tests/traces_api_v2_live.rs` | a span carrying every field, an event, a link and all five value types: fetched back and compared as an OTLP value, equal, attribute order not significant |
| `the fallback answers a trace the view has not indexed` | same | insert one trace's spans with the `traces_mv` view detached, so `traces` holds no row for it, then `GET /api/v2/traces/<id>?start=…&end=…`: the response carries the same spans as the same fetch with the view attached, and `system.query_log` shows **2** statements for that request's `query_id` prefix, against 1 when the index row exists. Without the fallback the response is empty and the status is `404` |
| `T-Q3` (fetch half) | same | response bytes against `sum(byteSize(*))` over that trace's rows with `final = 1`: within R6's 2× |
| `T-Q1` (fetch rows) | same | one statement per fetch request whose trace is indexed, counted from `system.query_log` by the request's `query_id` prefix; two for the fallback case above |
| `the shared span survives the fetch` | same | a Zipkin shared span — one span id, kinds 2 and 3 — comes back as two spans, in `(start_ns, span_id, kind)` order |
| `the fetch response is equal as an OTLP value` | `crates/pulsus-server/tests/traces_api_live.rs` | the existing fetch suite passes, with the attribute-order assertions regenerated and the diff read. A value that moved, rather than a key that reordered, fails it |

**Done when**

1. The five new cases pass and `traces_api_live.rs` passes with only
   attribute-order lines regenerated.
2. `system.query_log` shows one statement for an indexed fetch and two for the
   fallback.
3. A grep shows `assemble.rs` no longer decodes `payload`.
4. `docs/api.md` §4.1 gains one sentence: within a span, attribute order is not
   significant and is not preserved. `docs/benchmarks/traces-differential-ledger.md`
   gains its row.

---

### Task 6 — The predicate compiler, part 1

Span attributes and intrinsics. The type that every later read task embeds.

**Changes**

- `crates/pulsus-read/src/traces/spans/predicate.rs` — a `Query` leaf to one
  SQL boolean expression: the typed subcolumn read, `coalesce(…, false)` on
  every typed read with negation expressed over the result, the `Int64` and
  `Float64` variants for a numeric comparison, the `Bool` variant, array
  membership through `has(…)`, truthiness, presence and absence, the anchored
  regex, the static keywords including `minInt` and `maxInt`, and the intrinsic
  columns — `name`, `kind`, `status`, `statusMessage`, `duration`, `span:id`,
  `span:parentID`, `trace:id`, and `instrumentation:name` / `instrumentation:version`,
  which are the `scope_name` / `scope_version` columns
  (`server-implementation.md:164`).
- `crates/pulsus-read/src/traces/window_sql.rs` — the clauses are
  parameterised by the column they render over. Today they hard-code
  `timestamp_ns`, `date` and `bucket`; the new table's are `start_ns` and the
  expression `intDiv(start_ns, 300000000000)`, and `resources`/`traces` carry
  `day`. The one-value rule of §5.1 is unchanged: the row bound, the bucket
  bound and the day bound are all rendered from `last_included_ns`.

**Must not break** — `filter.rs`, which still serves every shipped route.
`window_sql`'s existing renderings must be byte-identical afterwards: the
existing goldens are the check.

**Still on the old path afterwards** — every route. The new predicate compiler
has no production caller.

**Test cases**

| case | file | what it pins |
|---|---|---|
| `T-A1` | `crates/pulsus-read/tests/traces_query_v2_live.rs` | `{ span.http.response.status_code != 200 }` on the §6.1 fixture returns 8 spans — the seven lacking the key plus `…0006` (F19). Without `coalesce` it returns 0 |
| `T-A2` | same | `{ … >= 500 }` → `…0001` (F4); `{ … = "200" }` → `…0006` (F5); `{ … = 200 }` → none |
| `T-A16` | same | `{ span.app.cache.hit }` matches only a value that **is** `true` |
| `T-A17`, `T-A18` | same | `{ span.app.tags != nil }` matches spans carrying the key whatever the value; `= nil` matches those not carrying it |
| `F3`, `F6`, `F7`, `F8`, `F9`, `F14`, `F20` | same | the §6.1 rows: `status = error` → `…0001, …0005`; a double comparison → `…0003`; array membership → `…0003`; a false bool → `…0001`; `duration > 1s` → `…0008`; a span name → `…0006, …0009`; `kind = consumer` → `…0008` |
| `the static keywords are literals` | same | `{ .a < maxInt }` and `{ .a = minInt }` render `9223372036854775807` and `-9223372036854775808` in both the `Int64` and the `Float64` variant, and answer the catalogue's rows 130 and 131 |
| `the window renders over the new column` | `crates/pulsus-read/tests/traces_compile_v2.rs` | for the `T-B1` window — `start = 1790094846486853636`, `end = 1790094846486853637` — the emitted text carries, literally: `start_ns >= 1790094846486853636 AND start_ns < 1790094846486853637`; `intDiv(start_ns, 300000000000) BETWEEN 5966982 AND 5966982`; and `toDate(fromUnixTimestamp64Nano(start_ns)) >= toDate('2026-09-22') AND toDate(fromUnixTimestamp64Nano(start_ns)) <= toDate('2026-09-22')`. Every one of the three is rendered from `end - 1 = 1790094846486853636`. If the renderer shapes a clause differently from the text above, the assertion takes the rendered text and this document's value is corrected in the same commit, with the observed text pasted |
| `the old renderings did not move` | `crates/pulsus-read/tests/golden_sql_freeze.rs` | not a new case: every existing golden passes unchanged after `window_sql` is parameterised |

**Done when**

1. The cases above pass, seeded with the §6.1 fixture (D8).
2. The existing goldens pass unchanged and `PINNED_SQL_CORPUS` does not move.
3. `.github/workflows/ci.yml` gains the step for
   `cargo test -p pulsus-read --test traces_query_v2_live`, the last of D7's five.
4. No production caller of `spans/predicate.rs` exists yet.

---

### Task 7 — The predicate compiler, part 2

**Changes** — `crates/pulsus-read/src/traces/spans/predicate.rs` gains:
resource conditions as `resource_id IN (SELECT resource_id FROM resources …)`;
event and link conditions as `arrayExists` over the event/link `attrs`;
the event and link intrinsics `event:name`, `event:timeSinceStart`,
`link:spanID` and `link:traceID` as array predicates over the `events` and
`links` columns (`server-implementation.md:165-166`); instrumentation-scope
conditions over `scope_attrs`; the unscoped `.k` chain span → resource → event
→ link → instrumentation; arithmetic `+ - * / % ^` and unary `-` at the
parser's precedence; a comparison whose right side is a field; and the boolean
combinators.

**Must not break** — part 1's output for a query that uses no part-2 construct:
the task 6 tests are the check.

**Still on the old path afterwards** — every route.

**Test cases**

| case | file | what it pins |
|---|---|---|
| `T-A7` | `crates/pulsus-read/tests/traces_query_v2_live.rs` | `{ span.app.items.count + span.app.discount.ratio > 3 }` and `{ span.app.items.count > span.app.discount.ratio }` each return exactly `…0003`, and each is one statement |
| `F2`, `F18` | same | `{ resource.service.name = "payment" }` → `…0005, …0006`; `{ resource.k8s.pod.name = "payment-a" }` → the same two |
| `F10`, `F11` | same | `{ event.exception.type = "java.lang.IllegalStateException" }` → `…0005`; `{ link:traceID = "111…1" }` → `…0008` |
| `F17` | same | `{ .app.user.id = "u-1" }` → `…0001`, through the unscoped chain |
| `the four event and link intrinsics` | same | on the catalogue fixture: `{ event:name = "exception" }` → `…0004`; `{ event:timeSinceStart > 1ms }` → `…0004`; `{ link:spanID = "0a1b2c3d4e5f6071" }` → `…0004`; `{ link:traceID = "000102030405060708090a0b0c0d0e0f" }` → `…0004`. `event:timeSinceStart` is `event.time_ns − start_ns`, so a rendering that compares the raw event time answers 0 rows |
| `the unscoped chain reads the scopes in order` | same | fixture E's trace carrying the same key on a span and on its resource with different values: the span's value wins. It fails without the ordered `multiIf` |
| `T-T9` (compile half) | `crates/pulsus-read/tests/traces_compile_v2.rs` | an `instrumentation.k` condition compiles to a read of `scope_attrs`, not of `attrs` |

**Done when**

1. The cases pass; task 6's cases still pass.
2. Every row of `server-implementation.md` §3.2 whose rule names a leaf
   predicate has a case in task 6 or task 7, and the plan carries the
   row-to-case map as a committed table in the task's own plan comment, so a
   reviewer checks every row is claimed rather than being told it is.

---

### Task 8 — The search statement

**Changes** — `crates/pulsus-read/src/traces/spans/search.rs`: the §5.2
statement. The top-K as a **scalar** subquery so it runs once; the detail read
keyed on `(bucket, trace_id)`; the left join to `traces` for the root, extent
and duration; the ordering `last DESC, trace_id ASC`; the spanset cap `spss`
with `matched` reported from the uncapped count.

**Must not break** — nothing: still no production caller.

**Still on the old path afterwards** — every route, including search.

**Test cases**

| case | file | what it pins |
|---|---|---|
| `T-B4` | `crates/pulsus-read/tests/traces_compile_v2.rs` | the statement carries `intDiv(start_ns, 300000000000) BETWEEN 5966982 AND 5966982` for the `T-B1` window |
| `the top-K runs once` | `crates/pulsus-read/tests/traces_query_v2_live.rs` | the statement's `system.query_log` `read_rows` for one search over corpus c1 is within 5% of the single-pass count — the single-pass count being `SELECT count() FROM spans` over the same window, read in the same test — not twice it. Written as a CTE over g1 it read 4,098,432 rows against 2,098,370; this is that difference, asserted as a ratio so it holds at c1's scale |
| `T-C3` | same | a trace with five matching spans and `spss=3`: `matched` is 5, the spanset holds 3 |
| `T-C4` | same | two traces whose newest matched spans share a timestamp come back newest first, `trace_id` ascending |
| `F1` | same | `{}` over the §6.1 fixture returns all nine spans, newest trace first |
| `the detail read does not re-scan the window` | same | `read_rows` for the detail pass is at most the number of spans in the twenty traces it returned — that number read in the same test as `SELECT count() FROM spans WHERE trace_id IN (<the twenty>)` — and strictly less than the window's span count |

**Done when**

1. The cases pass.
2. The statement's shape is frozen as a golden under
   `crates/pulsus-read/tests/golden/traces_spans_search/`, one file per shape
   in the plan's list, registered in `golden_sql_freeze.rs`'s `CORPORA` — which
   grows from two entries to three — with its own count, its own digest, and
   the total at `golden_sql_freeze.rs:597` moved by exactly the number of files
   added.

---

### Task 9 — The search route fork

The first task after which a user's search can be answered from the new
tables.

**Changes**

- `crates/pulsus-read/src/traces/exec.rs` — `search` asks one function whether
  the plan is covered; if it is, it issues task 8's statement and decodes the
  new row; otherwise it calls the engine that serves it today.
- `crates/pulsus-read/src/traces/spans/mod.rs` — the coverage predicate,
  written over the plan's own shape, not over the query text.
- `crates/pulsus-read/tests/traces_route_inventory.tsv` — D6's committed
  inventory: 141 rows, `name`, `route`, `side`.
- `docs/api.md` — no change. The answers must not move.

**Must not break** — every existing search test, golden and corpus case. Both
sides answer over the same data, so a query moving across the fork must not
change its answer. That is what the corpus suites check.

**Still on the old path afterwards** — the shapes the inventory marks `old`:
`by()`, `select()`, aggregate filters, structural operators, trace-level
intrinsics, `span:childCount`, the nested-set comparisons other than the two
that compile to `true`, and the broad-search slice plan. Metrics, tags and the
service graph are untouched.

**Test cases**

| case | file | what it pins |
|---|---|---|
| `the inventory is complete and exact` | `crates/pulsus-read/tests/traces_route_inventory.rs` | the file has exactly one row per query under `crates/pulsus-traceql/tests/corpus/accept/` and `grafana/` — **141**, no more and no fewer — the `route` column is the route the API dispatches that query to, the `side` column is what the coverage predicate returns, and the three the planner refuses carry `refused`. After this task, 84 rows are `new` and 54 are `old` (§4.2). It fails if a corpus query is added and the file is not |
| `an answer does not move across the fork` | `crates/pulsus-read/tests/traces_query_v2_live.rs` | for every query the inventory marks `new`, the old engine and the new statement are both run over the same seeded fixture and their span id lists are compared element by element. It fails if the two disagree on any one |
| `F1`–`F20` through the route | `crates/pulsus-server/tests/traces_api_v2_live.rs` | the §6.1 table, asked over HTTP, with the literal span ids of that table |
| `T-C1` | `crates/pulsus-read/tests/traces_query_v2_live.rs` | the eighteen filters of §6.2 against the counts `measure/ground_truth.py` computes from the corpus file, on corpus c1 |
| `T-Q2` | same | `{ status = error }` over c1 with `limit=20`: the rows the reader receives from ClickHouse are at most 20 — the answer's own size — not a span count. Today the engine hydrates 32 traces per batch and evaluates spans in Rust |
| `T-Q3` (search half) | `crates/pulsus-server/tests/traces_api_v2_live.rs` | a search response is ≤ 8 KB whatever it matched: asserted for `{}` and for `{ status = error }` over c1, the widest two shapes in the corpus |

**Done when**

1. The inventory file is committed with 141 rows, 84 `new`, 54 `old`, 3
   `refused`, and its test passes.
2. The cross-check of old against new is green for every `new` row, and its
   output — the number of queries compared — is pasted.
3. Every existing search suite passes unchanged.

---

### Task 10 — `by()`, `coalesce()`, `select()`, aggregates and a later filter stage

**Changes** — `crates/pulsus-read/src/traces/spans/search.rs` gains the group
key rendered as **text beside its stored type**, the first-appearance ordering
from `min((start_ns, span_id))`, the missing-key rule, `coalesce()` dropping
the key, `select()`'s projected slots, `| count()`/`sum`/`avg`/`min`/`max` as
the first pass's `HAVING`, and a `{...}` filter as a later pipeline element as
a predicate of the detail pass. `crates/pulsus-read/src/traces/exec.rs` widens
the coverage predicate.

**Must not break** — the ungrouped statement of task 8.

**Still on the old path afterwards** — structural operators, trace-level
intrinsics, `span:childCount`, the nested-set comparisons; metrics, tags, graph.

**Test cases**

| case | file | what it pins |
|---|---|---|
| `T-A8` | `crates/pulsus-read/tests/traces_query_v2_live.rs` | `{ resource.service.name = "checkout" } \| by(span.rpc.method)`: one spanset per distinct **(value, stored type)** pair, groups in first-appearance order, no spanset for a span lacking the key, one statement |
| `the integer and the double do not merge` | same | the catalogue fixture's trace carrying `a = 1` (int) on one span and `a = 1.0` (double) on another: `{} \| by(span.a)` answers `('1', 'int', 1 span)` and `('1', 'double', 1 span)`. Grouping on the label alone gives one group of two spans — and grouping on the bare read does not run at all, `Code: 44` |
| `T-A19` | same | `{ resource.service.name = "checkout" } \| count() > 5 \| { status = error }` is **one** statement |
| `coalesce drops the key` | same | the same query with `\| by(span.rpc.method) \| coalesce()` answers exactly what the ungrouped query answers |
| `the top-K stays per trace` | same | a trace with many groups does not crowd another trace out of the twenty: a fixture with one trace in six groups and twenty-one traces in one, asking for twenty, returns twenty distinct traces |
| `the inventory moved fourteen rows` | `crates/pulsus-read/tests/traces_route_inventory.rs` | the fourteen rows §4.2 names are `new`, and the file's `new` count is **98**. It fails if a row moves that this task did not implement, and if one this task implemented did not move |

**Done when**

1. The cases pass and the inventory's `new` count is 98.
2. `by(<expression>)` still answers `400` — `T-A15`'s search rows still pass.
   §7 records that this refusal is a gap against the reference, not a
   correctness decision, and names who closes it.

---

### Task 11 — Structural operators

**Changes** — `crates/pulsus-read/src/traces/spans/structural.rs`: the five
relations in three modifiers. The three non-transitive ones as a set test
inside a per-trace group; the two transitive ones as the bounded recursive
climb, carrying `max_recursive_cte_evaluation_depth = PULSUS_TRACEQL_MAX_DEPTH + 1`
(D10); the relation-specific union partner sets; the candidate restriction that
differs for the negated forms; the `overflow` row as **its own row** of the
result; `PULSUS_TRACEQL_MAX_DEPTH` (default 64) as a new setting, with its row
in `docs/configuration.md`. `crates/pulsus-read/src/traces/exec.rs` widens the
coverage predicate.

**Must not break** — task 10's statements.

**Still on the old path afterwards** — trace-level intrinsics, `childCount`,
the nested-set comparisons; metrics, tags, graph.

**Test cases**

| case | file | what it pins |
|---|---|---|
| `T-A9` | `crates/pulsus-read/tests/traces_query_v2_live.rs` | all fifteen forms on the §6.1 fixture, with the literal span id list the design gives for each |
| `T-A9b` | same | fixture E's `ee06`: `&>` → `A2, B1`; `&<` → `A1, B1`; `&~` → `A3, B2`; the three plain forms → `B1`, `B1`, `B2`. One partner expression for all three answers `A1, A2, B1` twice and `B2` once |
| `T-A10` | same | the 65-span chain's leaf matches with `unresolved = 0`; the 66-span chain's leaf does not match and the statement's overflow row carries a non-zero `unresolved`, which the route turns into `422 query_too_broad` — never `200` with an empty spanset, and never a database error, which is what D10's setting is for |
| `T-A11` | same | a two-span cycle terminates and reports `unresolved > 0`; and with the over-bound chain **alone** in the database, 0 match rows and `unresolved = 1` |
| `a trace with no A span answers the negated forms` | same | fixture E's `ee07` is returned by `!>`, `!<`, `!~`, `!>>` and `!<<`, and by none of the others |
| `the inventory moved thirteen rows` | `crates/pulsus-read/tests/traces_route_inventory.rs` | the thirteen structural rows are `new` and the file's `new` count is **111** |

**Done when**

1. The six cases pass, with the expected answers taken from
   `measure/edge_checks.sh` and `results/fixture-structural.tsv`.
2. `PULSUS_TRACEQL_MAX_DEPTH` is in the known-environment list, in
   `docs/configuration.md`, and in `server-implementation.md` §4's list — which
   already names it.
3. The inventory's `new` count is 111.

---

### Task 12 — Trace-level intrinsics, `span:childCount`, and the nested-set shapes

**Changes**

- `crates/pulsus-read/src/traces/spans/tracelevel.rs`: `trace:duration`,
  `trace:rootName`, `trace:rootService` and their legacy spellings joined from
  `traces`; `span:childCount` from a per-`(trace_id, parent_span_id)` count
  joined back; `nestedSetParent < 0` as the root anti-join with **no**
  numbering; `nestedSetLeft > 0` and `nestedSetRight >= 1` as `true` (already
  task 9's, and re-asserted here); every other nested-set comparison as the two
  statements of §3.5.
- `crates/pulsus-read/src/traces/spans/numbering.rs` — **a new module, not a
  reference to the old one.** The Euler tour the reader uses to number the
  candidate traces is carried across from `search_eval.rs:2085-2139` into the
  new tree in this task, with its rules for same-instant siblings, an unstored
  parent and a cycle intact. Task 20 then deletes `search_eval.rs` whole
  instead of having to keep 7,559 lines alive for 55 of them.
- `crates/pulsus-read/src/traces/exec.rs` widens the coverage predicate.

**Must not break** — the numbering rule itself, which the carried-across
implementation already gets right for same-instant siblings, an unstored
parent, and a cycle. The old module keeps its copy and its callers until task
20; the two are byte-compared by the case below.

**Still on the old path afterwards** — the slice plan; metrics, tags, graph.

**Test cases**

| case | file | what it pins |
|---|---|---|
| `T-C5` | `crates/pulsus-read/tests/traces_query_v2_live.rs` | a trace whose root is outside the request window: `trace:duration` is `3602000000000` ns and `trace:rootService` is `loadgen`, and `{ trace:rootService = "loadgen" && trace:duration > 1h }` returns it. A window-only computation gives 2 s and `cart` |
| `T-A20` | same | `{ span:childCount > 3 }`, and the numbering `root 1/8/-1`, `A 2/5/1`, `C 3/4/2`, `B 6/7/1`; a 1,000-span trace has a maximum right of exactly 2,000 |
| `T-A21` | same | `{ nestedSetParent < 0 }` over c1 returns one span per trace, and the statement carries **no** recursive CTE |
| `T-A22` | same | the same query over fixture E returns exactly `ee01:01, ee01:05, ee02:04, ee03:01, ee04:01, ee06:01, ee07:01` — the orphan is returned, a span inside a pure cycle is not |
| `T-A23` | same | on the cyclic trace, the numbering gives the promoted member `parent = -1`, which is where the two paths differ |
| `T-Q2b` | same | `{ resource.service.name = "checkout" && nestedSetLeft > 5 }` issues two statements and hands the reader at most the trace cap × `MAX_SPANS_PER_TRACE` span rows, never a window's |
| `the carried numbering is the same function` | `crates/pulsus-read/tests/traces_query_v2_live.rs` | for each of fixture E's seven traces, `spans::numbering` and `search_eval`'s numbering produce identical `(left, right, parent)` triples, span by span. It fails if the carry-across changed a rule, and it is deleted with `search_eval.rs` in task 20 |
| `the inventory moved eight rows` | `crates/pulsus-read/tests/traces_route_inventory.rs` | the eight rows §4.2 names are `new` and the file's `new` count is **119**. Those eight are `intrinsic_nested_set_parent_lt`, `intrinsic_root_name_legacy`, `intrinsic_root_service_name_legacy`, `intrinsic_span_child_count`, `intrinsic_trace_duration`, `intrinsic_trace_duration_legacy`, `intrinsic_trace_root_name`, `intrinsic_trace_root_service` |

**Done when**

1. The eight cases pass.
2. `docs/api.md` §4.2 gains the entry for the cycle difference, and
   `docs/benchmarks/traces-differential-ledger.md` gains its row.
3. The inventory's `old` side holds no nested-set or trace-level query, and its
   `new` count is 119.

---

### Task 13 — The newest-slice-first search plan

**Changes** — `crates/pulsus-read/src/traces/spans/search.rs`: the first pass
bounded to a slice, doubling, stopping when the request's `limit` traces are in
hand; the rule below; the statement-count bound `⌈log₂(window / 5 min)⌉ + 1`.
`crates/pulsus-read/src/traces/exec.rs` issues the loop.

**The rule, and where it comes from.** `server-implementation.md` §3.5 says the
compiler uses the slice plan "only when the first slice's own match count says
the filter is broad". One statement cannot both count the first slice and
decide the whole-window shape, so the decision costs the first slice's own
statement and is taken **once**:

```
   K   = ceil(window / 5 min)          how many 5-minute slices the window holds
   L   = the request's limit           20 by default

   statement 1: the whole search over the newest 5 minutes -> n distinct traces

     n >= L          -> answer from this slice                      1 statement
     n * K >= 2 * L  -> double the slice and repeat until L traces  <= ceil(log2 K) + 1
     otherwise       -> one more statement, over the whole window   2 statements
```

The middle test is derived, not tuned. At the first slice's density the plan
expects to fill `L` traces at a slice of about `5 min · L / n`; doubling reads
about twice that, against `window` for the single statement. Doubling is the
cheaper plan exactly when `2 · 5 min · L / n < window`, which rearranges to
`n · K > 2 · L`. At `K = 36` (three hours) and `L = 20` that is `n >= 2`: a
filter matching nothing or one trace in the newest five minutes goes straight
to the whole window.

**Must not break** — the answer. A trace found in a newer slice always outranks
one found only in an older slice, and the second pass still covers the whole
window through the per-trace extents.

**Still on the old path afterwards** — metrics, tags, the service graph.

**Fixture P**, seeded by the shared fixture module (D8). Window
`start = 1790035200`, `end = 1790046000` — three hours, so `K = 36`. Forty-three
traces, one span each, trace ids `0000…0001` through `0000…002b` in ascending
start order:

| traces | ids | starts | attributes |
|---|---|---|---|
| 20 | `…0001`–`…0014` | `1790035200 + 300·i` s, i = 0…19 | service `checkout`, `status = error`, `span.http.response.status_code = 500` |
| 10 | `…0015`–`…001e` | `1790044800 + 60·i` s, i = 0…9 | the same |
| 6 | `…001f`–`…0024` | `1790045400 + 10·i` s, i = 0…5 | the same |
| 6 | `…0025`–`…002a` | `1790045700 + 10·i` s, i = 0…5 | the same; `…0029` and `…002a` also carry `span.app.slice.probe = true` |
| 1 | `…002b` | `1790035200` s | service `payment`, `status = unset`, `span.app.user.id = "u-10013"` |

**Test cases**

| case | file | what it pins |
|---|---|---|
| `the sliced answer is the literal twenty` | `crates/pulsus-read/tests/traces_query_v2_live.rs` | over fixture P with `limit=20`, each of `{}`, `{ resource.service.name = "checkout" }`, `{ span.http.response.status_code >= 500 }` and `{ status = error } \| select(name)` returns, in this order: `…002a, …0029, …0028, …0027, …0026, …0025, …0024, …0023, …0022, …0021, …0020, …001f, …001e, …001d, …001c, …001b, …001a, …0019, …0018, …0017`. The expectation is the list, computed from the fixture table above, not the other path's answer |
| `the broad search slices three times` | same | for those four queries `system.query_log` shows **3** statements: the 5-minute slice finds 6 traces (6 < 20, and 6·36 ≥ 40, so it doubles), the 10-minute slice finds 12, the 20-minute slice finds 22 and the newest 20 are returned |
| `the rare filter does not slice` | same | `{ span.app.user.id = "u-10013" }` returns exactly `…002b` and issues **2** statements: the newest slice finds 0 traces, `0 · 36 < 40`, so the second statement covers the whole window. The count is two and not one, because the decision itself costs the slice it reads |
| `the statement count is bounded` | same | `{ span.app.slice.probe }` returns exactly `…002a, …0029` and issues **7** statements: the newest slice finds 2 traces, `2 · 36 ≥ 40`, so the plan doubles 5 → 10 → 20 → 40 → 80 → 160 → 180 minutes, which is `⌈log₂ 36⌉ + 1 = 7`, the bound §3.5 states |
| `T-Q1` (search rows) | `crates/pulsus-server/tests/traces_api_v2_live.rs` | every search request in `measure/api_requests.tsv` issues the number of statements that file states in its `statements` column |
| `the inventory moved nothing` | `crates/pulsus-read/tests/traces_route_inventory.rs` | the `new` count is still **119**: the slice plan changes how a search runs, not which shapes are covered |

**Done when**

1. The six cases pass.
2. `server-implementation.md` §3.5's first row is the shipped rule — the plan
   quotes the condition the code applies and shows it is the rule above.
3. The inventory is unchanged.

---

### Task 14 — Metrics on the new tables

**Changes** — `crates/pulsus-read/src/traces/spans/metrics.rs`: the §5.6
statement. One row per series, not one per point; the right-closed step label
`(intDiv(start_ns - 1, 60000000000) + 1) * 60000`; exemplars from
`argMax((trace_id, span_id, duration_ns), (duration_ns, span_id))` in the same
pass; `topk`/`bottomk` ordering the finished series inside the statement; the
trailing metrics-result comparison as a `HAVING`; and the plan-time refusals,
carried across from `metrics_plan.rs:1102-1121` into the new module so that
task 20 can delete the old one. `TraceEngine::metrics_range` and
`metrics_instant` issue it.

**What this task does not build, and why.** The design's §5.6 also describes
grouping by a resource attribute "resolved through `resource_id` and joined
afterwards". No served query reaches that path: the planner admits exactly one
`by` key and only `resource.service.name`, which is the `spans.service` column,
not a resource attribute read. Building the join now would ship a code path no
test can execute. It arrives with the change that opens the other grouping keys
— §7 names that gap and who carries it.

**Must not break** — the plan-time `400`s. `T-A15` is the guard. §7 records
that those refusals are a gap against the reference, not a correctness
decision.

**Still on the old path afterwards** — `compare()`, tags, the service graph.

**Fixture M**, seeded by the shared fixture module (D8). Six spans, one trace,
request window `start = 1789999980`, `end = 1790000040`, `step = 60s`, so every
span falls in the single bucket labelled `1790000040000`:

| span | service | `start_ns` | `duration_ns` | status |
|---|---|---:|---:|---|
| `…0001` | checkout | 1790000000000000000 | 1000000000 | ok |
| `…0002` | checkout | 1790000005000000000 | 3000000000 | error |
| `…0003` | checkout | 1790000010000000000 | 2000000000 | ok |
| `…0004` | payment | 1790000015000000000 | 4000000000 | ok |
| `…0005` | payment | 1790000020000000000 | 5000000000 | ok |
| `…0006` | frontend | 1790000025000000000 | 6000000000 | ok |

**Fixture M2** is fixture M plus `…0007`, service `frontend`,
`start_ns = 1790000030000000000`, `duration_ns = 6000000000` — the same
duration as `…0006` and a larger span id.

**Test cases**

| case | file | what it pins |
|---|---|---|
| `the five over-time aggregates` | `crates/pulsus-read/tests/traces_query_v2_live.rs` | over fixture M, one point at `t = 1790000040000` for each: `{} \| count_over_time()` → **6**; `\| sum_over_time(duration)` → **21000000000**; `\| min_over_time(duration)` → **1000000000**; `\| max_over_time(duration)` → **6000000000**; `\| avg_over_time(duration)` → **3500000000**. An aggregate applied to the wrong column, or a bucket label off by one step, fails every one |
| `the quantile` | same | two assertions. (a) over a variant of fixture M in which all six durations are `2000000000`, `\| quantile_over_time(duration, 0.5)` and `\| quantile_over_time(duration, 0.99)` both answer **2000000000** — a count rendered in place of a quantile answers 6. (b) over fixture M itself, `quantile_over_time(duration, 0)` answers **1000000000** and `quantile_over_time(duration, 1)` answers **6000000000**, the two levels at which the digest is exact |
| `the histogram buckets` | same | over fixture M, `{} \| histogram_over_time(duration)` answers exactly four series at `t = 1790000040000`: `1073741824` → 1, `2147483648` → 1, `4294967296` → 2, `8589934592` → 2. The series label is `toUInt64(roundToExp2(duration_ns - 1)) * 2`, so a rendering that drops the `- 1` puts `…0003`'s 2 s in `4294967296` and fails |
| `the instant query` | same | `/api/metrics/query_instant` with the same query and window answers the one bucket's value: `count_over_time()` → **6** |
| `the trailing comparison` | same | `{} \| count_over_time() > 5` returns the one series; `> 6` returns none. A comparison applied before the aggregation returns the series in both |
| `T-A12` | same | `{ } \| rate() by (resource.service.name)` with exemplars over fixture M: one exemplar per bucket per series, each naming a `(trace:id, span:id)` that exists, and the same exemplar on a second run. The `checkout` series' exemplar is `…0002` (3 s, the longest of the three), `payment`'s is `…0005` (5 s), `frontend`'s is `…0006` |
| `the exemplar is the longest span and the span id breaks the tie` | same | over fixture M2 the `frontend` series' exemplar is **`…0007`**, not `…0006`: `argMax(…, (duration_ns, span_id))` takes the larger span id when the durations are equal. A stable but wrong choice — the first row, or the smaller id — passes `T-A12` and fails here |
| `grouping by the service name` | same | `{} \| count_over_time() by (resource.service.name)` over fixture M answers `checkout` → 3, `payment` → 2, `frontend` → 1 |
| `T-A13` | `crates/pulsus-server/tests/traces_api_v2_live.rs` | `{ } \| count_over_time() by (resource.service.name) \| topk(3)` over the §6.1 fixture returns exactly `frontend` 3, `accounting` 2, `checkout` 2 — `payment` also has 2 and is absent; `bottomk(3)` returns `accounting`, `checkout`, `payment` and drops `frontend` |
| `F21` | same | `{} \| count_over_time()` over the §6.1 fixture window answers **9**, not 11 |
| `T-C2` | same | now asserted **whole**: all twenty-four rows of §6.1 — F1 through F21 and B1 through B3 — with their literal span ids. Every one of them is served from the new tables after this task |
| `T-A15` (metrics rows) | same | **guard.** The five metrics requests whose "today" column reads `400` still answer `400` with the §4 envelope; the message text is not asserted |
| `T-Q3` (metrics half) | same | ≤ 24 bytes per point per series |
| `T-X4` | same | **guard.** At `PULSUS_TRACEQL_READ_MAX_MEMORY_BYTES = 1048576` the served metrics query answers `422 query_too_broad` naming `reader.traceql_read_max_memory_bytes`, and `200` at the default |
| `the inventory moved sixteen rows` | `crates/pulsus-read/tests/traces_route_inventory.rs` | the sixteen `metrics` rows that are not `compare()` are `new`, and the file's `new` count is **135** |

**Done when**

1. The cases pass; `T-A15` and `T-X4` passed before the change and still pass.
2. The 28 goldens under `crates/pulsus-read/tests/golden/traces_metrics/` are
   **unchanged** and `PINNED_SQL_CORPUS` does not move: they pin
   `metrics_sql.rs`, which this task does not edit and task 20 deletes. The new
   statement gets its own golden directory,
   `crates/pulsus-read/tests/golden/traces_spans_metrics/`, registered in
   `golden_sql_freeze.rs`'s `CORPORA` with its own count and its own digest.
3. The inventory's `new` count is 135.

---

### Task 15 — `compare()` on the new tables

**Changes** — `crates/pulsus-read/src/traces/spans/metrics.rs` gains the §5.6
comparison: per-`(scope, key, value, type)` counts for each side, **counted per
span**, over span, resource, instrumentation-scope, event and link attributes
plus the eleven intrinsics, with `kind` and `status` rendered as the keywords
the API returns, `topN` applied **in the statement** per key and per side, and
`span:id` omitted. The fixed 25-key well-known set and the `*_total`
denominators stay with the response layer.

**Must not break** — the response envelope, and the `key=nil` emission for a
well-known key absent from the data.

**Still on the old path afterwards** — tags and the service graph.

**Test cases**

| case | file | what it pins |
|---|---|---|
| `T-A14` | `crates/pulsus-read/tests/traces_query_v2_live.rs` | fixture E's `ee07`: `resource k8s.pod.name=pod-x` → **1 / 2**; `intrinsic statusMessage=boom` → 1 / 0; `intrinsic kind=server` → 6 / 2, the keyword not the code; `event name=exception` → 1 / 0; `link spanId=0000000000000009` → 1 / 0 |
| `every scope appears on both sides` | same | the two comparisons of `measure/catalogue-extra.tsv` — selecting `.b = 2` and selecting `.a = 1` — between them put events, links and instrumentation attributes on the selection in one and on the baseline in the other |
| `topN is per key and per side` | same | a fixture with eleven values under one key and two under another, `topN = 10`: the second key's two values are both returned. A global `LIMIT` after the counts drops the second key entirely |
| `span:id is absent` | same | the key set carries the eleven intrinsics and does not carry `span:id` |
| `the differential still holds` | `crates/pulsus-read/tests/compare_value_differential.rs` | not a new case: the existing comparison against a live reference is run and its output pasted |
| `the inventory moved three rows` | `crates/pulsus-read/tests/traces_route_inventory.rs` | `metrics_compare`, `metrics_compare_topn` and `metrics_compare_window` are `new`, the file's `new` count is **138**, and the only rows left not `new` are the three `refused` |

**Done when**

1. The five new cases pass and the differential suite is run live.
2. `docs/api.md` §4.4 carries the `span:id` entry, and
   `docs/benchmarks/traces-differential-ledger.md` its row.
3. The inventory is 138 `new` and 3 `refused`.

---

### Task 16 — Tags on the two catalogs

**Changes** — `crates/pulsus-read/src/traces/spans/tags.rs`: names from
`tag_names` (time-less), unnarrowed values from `tag_values` (time-less, typed
per value), narrowed values from the store bounded by the window — which
compiles the request's `q` through tasks 6 and 7's predicate compiler, and is
why this task follows task 7 — the `name` intrinsic from the store bounded by
the window, and every other intrinsic from the static vocabulary with no
statement at all. `crates/pulsus-read/src/traces/exec.rs` — `list_tag_names`,
`list_tag_values` and `list_span_name_values` issue the new statements.

**Must not break** — `docs/api.md` §4.3, which does not change. The caps
(`TAG_NAMES_MAX = 10_000`, `TAG_VALUES_MAX = 1_000`) and their `truncated`
flags stay, in **both** value paths.

**Still on the old path afterwards** — the service graph, if task 17 has not
landed.

**Test cases**

| case | file | what it pins |
|---|---|---|
| `T-T1`, `T-T2` | `crates/pulsus-server/tests/traces_api_v2_live.rs` | **guards.** A name is listed although its span is outside the window, and still listed after the span's day is dropped |
| `T-T3` | same | one key `k` carrying the 1,000 values `v0000` … `v0999`: `/api/v2/search/tag/k/values` with no `q` returns exactly those 1,000, in ascending order, each typed `string`, with `truncated` **false**; `system.query_log`'s `tables` column for that request names `tag_values` and no span table |
| `T-T4` | same | **guard.** `q={resource.service.name="cart"}` narrows to that service's values and reads the span store |
| `T-T5` | same | **guard.** `k` as int 8080 in one span and string `"8080"` in another gives two entries, `int` and `string`, same text |
| `T-T6` | same | `/tag/name/values` returns only the in-window names, on the `[start, end)` rule — re-asserted here against the new statement; task 1 asserted it against the shipped one |
| `T-T7` | same | **guard.** `/tag/status/values` returns `ok`, `error`, `unset` typed `keyword`, with **zero** statements |
| `T-T8` | same | **guard.** 10,001 names and 1,001 values cap at 10,000 and 1,000 with `truncated: true` |
| `T-T9` | same | a scope attribute `otel.scope.build = "release"` is listed under `scope=instrumentation` and its value is returned |
| `T-Q3` (tag half) | same | a tag-name response and a tag-value response are each ≤ 4 KB, asserted on the 10,000-name and 1,000-value capped responses — the largest either route can return |
| `the catalogs carry all five scopes` | `crates/pulsus-read/tests/traces_query_v2_live.rs` | after seeding the fixture, `tag_names` holds a row in each of `span`, `resource`, `event`, `link` and `instrumentation` |

**Done when**

1. The ten cases pass; the six guards passed before and still pass.
2. The unnarrowed value read touches no span table — checked from
   `system.query_log`'s `tables` column, not from the statement text.

---

### Task 17 — The service graph on the span table

**Changes** — `crates/pulsus-read/src/traces/spans/graph.rs`: the §5.7
statement, a self-join of the span table over the window with **two** branches
— the ordinary one pairing a server span to its parent, and the shared one
pairing it to the client span with the same id, selected by
``coalesce(attrs.`zipkin%2Eshared`.:Bool, false)``. One row more than the
1,000-edge cap so the response can set `truncated`.
`crates/pulsus-read/src/traces/exec.rs` — `service_graph` issues it. The route
takes no `q`, so this task needs nothing from the predicate compiler.

**Must not break** — the graph window, which is already `[start, end)` and
stays so; `T-B6` is the guard. The edge ledger `trace_edges` is still written
and is now read by nothing.

**Still on the old path afterwards** — nothing on the read side.

**Fixture G**, seeded by the shared fixture module (D8), in one window:

| span | service | kind | parent | `duration_ns` | status | note |
|---|---|---|---|---:|---|---|
| `c1` | svc-a | client (3) | — | — | ok | |
| `s1` | svc-b | server (2) | `c1` | 10000000 | ok | the ordinary pair |
| `c2` | svc-a | client (3) | — | — | ok | |
| `c2` | svc-c | server (2) | — | 30000000 | error | the **same span id**, `zipkin.shared = true` |
| `p1` | svc-a | producer (4) | — | — | ok | |
| `q1` | svc-b | consumer (5) | `p1` | 20000000 | ok | the messaging pair |

**Test cases**

| case | file | what it pins |
|---|---|---|
| `T-C6` | `crates/pulsus-server/tests/traces_api_v2_live.rs` | the rpc edges over fixture G are exactly two, `svc-a → svc-b` and `svc-a → svc-c`, and every field is asserted: `calls` 1 and 1; `failed` **0** and **1**, because the shared pair's server span carries `status = error`; `quantiles_ns` `[10000000, 10000000, 10000000]` and `[30000000, 30000000, 30000000]`, the p50/p95/p99 of one sample each. A single-branch join returns only the first edge; a `failed` counted over the client span alone answers 0 and 0; a quantile taken over the client span answers nulls |
| `T-C7` | same | fixture G's producer/consumer pair adds a third edge, `svc-a → svc-b` with `connectionType` `messaging`, `calls` 1, `failed` 0, `quantiles_ns` `[20000000, 20000000, 20000000]` — differing from the rpc edge between the same two services only in `connectionType`. A statement that groups without the connection type answers two edges instead of three |
| `T-B6` | same | **guard.** The edge whose server span starts at exactly `end` is absent |
| `the cap sets truncated` | same | 1,001 distinct edges in the window: 1,000 returned and `truncated` set |

**Done when**

1. The four cases pass.
2. The graph route issues one statement, counted from `system.query_log`.
3. The two existing goldens under `crates/pulsus-read/tests/golden/traces_graph/`
   are **unchanged**: they pin `graph_sql.rs`'s builder, which still exists and
   is deleted by task 20. The new statement gets its own golden under
   `crates/pulsus-read/tests/golden/traces_spans_graph/`, registered in
   `golden_sql_freeze.rs` and added by name to
   `JOINS_OUTSIDE_THE_COMPILED_SEARCH_CORPUS` (`:680-692`), which grows from
   seven entries to eight — a self-join is a join, and that list is what keeps
   an eighth from appearing unnoticed.

---

### Task 18 — The corpus catalogue against the shipped compiler

`query-catalogue.md` records that all 138 served corpus queries and the 12 of
`measure/catalogue-extra.tsv` agree with an independent interpreter. Those
statements came from `measure/catalogue_render.py`, which is the design's
renderer, not this repository's compiler. This task points the same comparison
at the compiler that now ships.

**Changes**

- `xtask/src/traceql_catalogue.rs` and a subcommand in `xtask/src/main.rs` — a
  command that, for each corpus query, prints the statement the route would
  issue. `xtask` is the location, not `crates/pulsus-read/src/bin/`: `xtask`
  exists and already hosts the bench harnesses (`xtask/src/bench`,
  `xtask/src/ch_bench`), and `crates/pulsus-read/src/bin/` does not exist. A
  document used to derive disjoint file sets cannot leave this open. The route
  already has `search_explained`; this extends it to every
  route.
- `docs/TraceQL/measure/catalogue.py` — takes the statements from that command
  instead of from `catalogue_render.py`. `catalogue_interp.py`, the independent
  interpreter, is untouched: it is what the comparison is against.
- `.github/workflows/ci.yml` — one **scheduled or manually dispatched** job, not
  a required check: it needs the catalogue fixture, a container and a Python
  interpreter, and it runs the whole corpus.

**Must not break** — `catalogue_interp.py`. If the interpreter is edited to
agree with the compiler, the comparison stops being one.

**Test cases**

| case | file | what it pins |
|---|---|---|
| `T-C8` | the catalogue job | for each of the 138 served corpus queries and the 12 extra ones, the statement the **route** issues runs and its rows equal the interpreter's, and the membership statement does too: 138 of 138 and 12 of 12 on both comparisons |
| `T-Q2` (R5's whole-of-`measure/sql/` form) | the same job | for every statement the job issues, the rows returned to the reader are the answer's own size — at most `limit` traces for a search, at most series × points for a metrics query — never a span count. The §8 table's narrower `T-Q2` is task 9's; this is the form R5's test column states |
| `T-C9` | `crates/pulsus-server/tests/traces_api_v2_live.rs` | **guard.** All 50 refusals — 47 from the parser and the validator, 3 from the planner — answer `400` with the §4 envelope, with the message the corpus golden pins for the 47. The three planner refusals are recorded in §7 as gaps against the reference; this case pins today's behaviour so that closing one of them is a visible, deliberate change rather than a silent one |
| `the comparison can go red` | the catalogue job | the rule `duration_unit_render` of `measure/perturbations.tsv:76` is applied **to the shipped compiler**, in a scratch copy of the tree: a millisecond becomes 10³ nanoseconds instead of 10⁶. `{ duration >= 100ms }` then compiles to `duration_ns >= 100000`. On the catalogue fixture the correct answer to that query is 32 spans (`query-catalogue-accepted.md` row 34) and `{ duration >= 500µs }` is 33 (row 37), so the planted compiler answers at least 33 where the interpreter answers 32, and the comparison must report `duration_gte` as a membership disagreement. The rule, the diff and the run are pasted. A comparison nobody has seen fail is not one |

**Done when**

1. `T-C8` reports 138 of 138 and 12 of 12, and the run is pasted.
2. The planted-error run is pasted and names `duration_gte` among its
   disagreements.
3. `docs/TraceQL/query-catalogue.md` gains a sentence saying the statements now
   come from the shipped compiler, and which revision produced the recorded run.

---

### Task 19 — The reference comparison, measured

Requirement R10 and `T-P1`. It is its own task because its harness is not the
catalogue's and its answer is a distribution, not an equality.

**Changes**

- `docs/TraceQL/measure/fetch_compare.py`, `docs/TraceQL/measure/http_bench.py`
  — pointed at the shipped routes rather than at the design's statements, with
  their methodology unchanged: one unmeasured run, then five measured, median
  reported; the fetch comparison interleaves the two stores inside one run and
  reports the distribution, because the reference's fetch time moves between
  sessions.
- `.github/workflows/ci.yml` — one **scheduled or manually dispatched** job. It
  needs two containers and corpus g1, so it is not a required check, for the
  reason D5 gives with the figure.
- `docs/TraceQL/functional-requirements.md` §6.3 — the measured column is
  re-stated from this run, with its date, and the carried-forward note updated.

**Must not break** — the rule §6.3 states, which is the warm median below the
reference's for every shape outside the exempt class. The exempt class is
searches whose filter matches a large share of the window, where the reference
returns the first twenty traces it finds and this design returns the twenty
newest.

**Still on the old path afterwards** — nothing on the read side. The old tables
are still written and still present.

**Test cases**

| case | file | what it pins |
|---|---|---|
| `T-P1` | the benchmark job | §6.3 lists fifteen shapes, and each one ends in exactly one of four states. **Ten are compared**, and for each the warm median is below the reference's, interleaved, on one machine, same corpus. **One is exempt**: `{ rootServiceName = … && traceDuration > 2s }`, the search class §6.3 names. **One has no counterpart**: the service graph, for which the reference has no endpoint, so its figure is reported alone. **Three cannot be compared at all** while the metrics-grouping refusal stands, because our route answers `400` — `quantile_over_time(duration, …) by (span.http.route)`, instant `avg_over_time(duration) by (name)` and `{ } \| rate() by (resource.k8s.pod.name)`; the job reports them `refused`, never as passes. 10 + 1 + 1 + 3 = 15, and the job prints the four counts so a reader sees no shape was dropped |
| `the exempt shape is reported, not hidden` | the same job | `{ rootServiceName = … && traceDuration > 2s }` is measured and reported beside the reference's figure with the exempt marker, so a reader sees the one shape the rule does not cover and why |

**Done when**

1. The job runs, its output is pasted, and §6.3's table carries this run's
   figures and its date.
2. Every shape is one of: below the reference, exempt with its reason, or
   `refused` with the issue that would enable it.

---

### Task 20 — Delete the old path

The only task that removes anything, and the only irreversible one.

**Changes**

- `crates/pulsus-schema/src/catalog.rs` — **appended `DROP` migrations, and not
  one record removed.** The controller applies only the records that remain
  (`controller.rs:108-110`) and `reconcile_mvs` only iterates `MVS`
  (`controller.rs:334-335`), so deleting the 38 trace migrations and the 4
  `MvDef`s would leave every already-reconciled database holding all six tables
  and all four views for good. The catalogue's own policy says the same:
  amendment closed with issue #498 (`catalog.rs:16-25`). What is appended, after
  the ids task 2 added:
  - one `DROP VIEW IF EXISTS {{db}}.<name>{{on_cluster}}` per old trace view —
    `trace_tag_catalog_mv`, `trace_edges_mv`, `trace_recent_mv`,
    `trace_error_spans_mv` — and the four `MvDef`s are removed from `MVS` in
    the same commit, because a view that has been dropped must not be recreated
    by the next reconcile;
  - one `DROP TABLE IF EXISTS {{db}}.<name>{{on_cluster}}` per old trace table —
    `trace_spans`, `trace_attrs_idx`, `trace_tag_catalog`, `trace_edges`,
    `trace_recent`, `trace_error_spans` — and one per `_dist` twin, cluster-gated
    the way `Ddl::Dist` already is;
  - the matching entries are removed from `TTL_STMTS`, whose statements would
    otherwise `ALTER` a table that is gone.

  **Key the old set on the table name, not on `family`**: 36 of the 38 old
  records carry `Some(Family::Traces)`, and ids 18 and 41 — `trace_tag_catalog` —
  carry `family: None` (`server-implementation.md:306-324`). A change that
  reasons about the trace family alone misses those two.
- `crates/pulsus-read/src/traces/` — deleted: `search_plan.rs`, `search_eval.rs`,
  `search_sql.rs`, `filter.rs`, `compile.rs`, `metrics_plan.rs`, `metrics_sql.rs`,
  `tags_sql.rs`, `tag_narrow.rs`, `graph_sql.rs`, `sql.rs`, the old shapes in
  `rows.rs`, and the fork of task 9 with its inventory file. **Kept**, because
  the new tree uses them: `window_sql.rs` (parameterised by task 6),
  `metrics_result.rs` and `log2_histogram.rs` (response shapes),
  `dispatch.rs`, and `exec.rs` itself, which now holds only new-path methods.
  Every function the deleted files owned and the new tree still needs was
  carried across by the task that needed it — the numbering by task 12, the
  plan-time metrics refusals by task 14 — so this task creates no module and
  ports no logic.
- `crates/pulsus-write/src/writer/trace.rs` — **shrunk, not deleted.**
  `crates/pulsus-server/src/serve.rs` constructs and owns `TraceWriter` at
  `:23`, `:103` and `:450`; the file keeps its name and its type and loses its
  two old targets. The same for `ingest/traces.rs`. What goes: `TraceSpanRow`
  and `TraceAttrRow` in `writer/rows.rs`, the payload builder and the
  attribute-row builder in `protocols/otlp_traces.rs`, and the `TraceAttrRow`
  arm of `writer/backfill.rs`.
- `crates/pulsus-server/src/chconfig.rs`, `ops.rs` — the old table names.
- `crates/pulsus-model/src/time.rs` — the comments describing the old trace
  tables' timestamp needs. The admitted domain rule itself stays.
- Every test file and golden that pins an old-path statement, including the 75
  `traces_search` goldens, the 28 `traces_metrics` goldens, `CORPORA` and
  `PINNED_SQL_CORPUS` in `golden_sql_freeze.rs`, and the two
  `traces_metrics_base/` historic copies, which have nothing left to be the
  history of.

**Must not break** — the API. Every conformance corpus, the accept surface, the
comparison suites against a live reference, and every `docs/api.md` §4
behaviour.

**Still on the old path afterwards** — nothing.

**Test cases**

| case | file | what it pins |
|---|---|---|
| `T-S1` | `crates/pulsus-schema/tests/live_traces_v2.rs` | now asserted **whole**: the set of columns holding attribute values is exactly `spans.attrs`, `spans.scope_attrs`, `spans.attrs_other`, the `attrs` inside `spans.events` and `spans.links`, `resources.attrs`, `resources.attrs_other`, `tag_values.value` — and nothing else. It cannot pass before this task, because `trace_attrs_idx.val` and `trace_spans.payload` are still there |
| `T-Q1` | `crates/pulsus-server/tests/traces_api_v2_live.rs` | now asserted **whole**: every request in `measure/api_requests.tsv` issues the number of statements its `statements` column gives. That file holds 65 requests: 59 state one statement, 2 state two, and 5 state `-` and name no route, so they are not asserted. Counted with `/usr/bin/awk -F'\t' '!/^#/ && $1 != "statement" {print $5}' docs/TraceQL/measure/api_requests.tsv \| sort \| uniq -c` |
| `T-X1`–`T-X5` | `crates/pulsus-write/tests/trace_rows_v2.rs`, `crates/pulsus-server/tests/traces_api_v2_live.rs` | **guards, re-pointed.** Each of the five protections is asserted against the **new** path by name, not inherited from an old-route test: the 256 MiB request expansion ceiling and its sweep, `MAX_ANYVALUE_DEPTH = 32`, the scan-row budget on a new-table statement, `PULSUS_TRACEQL_READ_MAX_MEMORY_BYTES` on a new-table statement, and the admitted timestamp domain on the new writer. Each names the setting in its `422`/`400` body, so a statement that never received the setting fails rather than passing on the old route's guard |
| `the old tables are dropped, not merely unlisted` | `crates/pulsus-schema/tests/live_traces_v2.rs` | apply the schema to a database that was reconciled **before** this task — the test creates one by applying the previous migration set first — then apply the new set: `system.tables` for that database holds none of the six old names, none of their `_dist` twins and none of the four views, and `schema_migrations` still records the original 38 and 4. Removing the records instead of appending drops fails this on the first half; amending a record fails the second |
| `no code names an old trace table` | `crates/pulsus-read/tests/traces_route_inventory.rs`, rewritten as a source sweep | the script below exits 0. Everything outside `docs/` is searched; `docs/` is excluded because it carries the history |

The sweep, written out because a description of one is not one:

```sh
#!/usr/bin/env bash
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"

mapfile -d '' -t files < <(git ls-files -z -- ':!docs/')
if [ "${#files[@]}" -eq 0 ]; then
  echo "empty file list: the sweep searched nothing" >&2
  exit 2
fi

hits=$(/usr/bin/grep -nE \
  'trace_spans|trace_attrs_idx|trace_tag_catalog|trace_edges|trace_recent|trace_error_spans' \
  -- "${files[@]}" < /dev/null || true)

if [ -n "$hits" ]; then
  printf '%s\n' "$hits" >&2
  echo "an old trace table is still named outside docs/" >&2
  exit 1
fi
exit 0
```

Why it is written this way: it changes to the repository root first, so it can
be run from anywhere; it exits 2 on an empty file list rather than letting
`grep` read standard input and hang; it takes its input from `/dev/null` for
the same reason; it does not use `set -e`, because a no-match `grep` exits 1
and the interaction between `errexit` and an `&&` list is exactly the sort of
thing that turns a passing sweep into a sweep that never ran; and it prints
every hit, because a bare exit status cannot be acted on.

**It has been seen to go both ways.** Run against this tree today it exits 1
and names `.github/workflows/ci.yml:1611` among its hits, which is correct —
the old tables are still here until this task. Run with the pattern replaced by
a token the tree does not contain it exits 0. Both runs are the check on the
check, and task 20's implementer repeats them.

**Done when**

1. All five cases above pass, and `T-S1` and `T-Q1` pass in their whole form.
2. `cargo test --workspace` is green and the five required checks are green.
3. `docs/schemas.md` §4 describes five tables, not eleven.
4. The line count removed is stated in the pull request body and matches
   `server-implementation.md` §6's table: about 34,876 lines of read path and
   about 3,300 of write path, less the files §6 of this document keeps.

## 7. What this breakdown does not settle

Stated here rather than found later.

### 7.1 Three behaviours the design leaves refused, which the reference serves

These are **not** correctness decisions and this document does not present them
as ones. In each case the reference accepts the query, its engine has a defined
answer, and PulsusDB replies `400`. The design at `cf7d3416` records all three
under a "today" column and states that it changes neither the parser, the
validator nor the planner (`query-catalogue-refused.md:4-5`), so each is a gap
this replacement carries forward unchanged rather than one it introduces or
closes.

| the query | what we answer | what the reference does | where |
|---|---|---|---|
| `{ .a = 1 } \| by(.b + .c)` | `400` | `GroupOperation.evaluate` runs any `FieldExpression` per span and groups on its value; the grammar's `groupOperation` takes a `fieldExpression`, not an attribute | `pkg/traceql/ast_execute.go:14-54` and `pkg/traceql/expr.y:177-178` @ `v3.0.2` (`0c4b926d09234186de39833e9c7ecb5b7614c8b9`) |
| `{ .a = 1 } \| { .b = 2 } && { .c = 3 }` | `400` | `spansetPipeline PIPE spansetExpression` is a grammar production, so a spanset operation is a legal pipeline stage; `SpansetOperation.evaluate`'s `OpSpansetAnd` arm emits the unique spans of both sides, for a trace where both sides matched | `pkg/traceql/expr.y:170` and `:210`, `pkg/traceql/ast_execute.go:75` and `:92` @ the same tag |
| `\| rate() by (<any key but resource.service.name>)`, more than one key, a grouped quantile or histogram, a non-duration aggregation target | `400` | served | the design measures our own storage answering them — `rate() by(resource.k8s.pod.name)` at 55 ms, `quantile_over_time(…) by(span.http.route)` at 42 ms (`server-implementation.md:187`) |

**Why they are not closed here.** Each is a change to the accept surface: a
query that answers `400` today would answer `200`, which moves the conformance
corpus, `docs/api.md` §4, the differential ledger and `T-A15`/`T-C9`. None of
the three has an approved SQL shape — `sql-schema.md` §5.2's group key is "an
attribute or an intrinsic rendered as text beside its stored type", and there
is no statement in §5 for a spanset operation used as a pipeline stage. A
document whose first rule is that it changes no design decision cannot invent
three. Closing them is a behaviour change with its own plan, its own review and
its own ledger rows, and it should not ride inside a storage replacement whose
merge gate is "every existing test still passes".

**What they cost, stated rather than implied.** The metrics-grouping refusal is
not free in this work: three of the fifteen shapes in the design's own
benchmark table (`functional-requirements.md` §6.3) are shapes our route
answers `400`, so task 19 cannot compare them with the reference at all and
reports them `refused`. That is the clearest measure of the gap available
without closing it.

**Who settles them.** The owner, by scheduling them; task 14's plan and task
18's plan each name the open issue that carries the gap they touch, so neither
deferral is owned by nothing. Until such an issue exists, `T-A15` and `T-C9`
pin today's answers so that closing a gap is a visible, deliberate change.

### 7.2 The rest

| what | why it is not settled | who settles it |
|---|---|---|
| the size of each task in hours | no task here has been implemented, so "one sitting" is a judgement from the file sets and the case counts, not a measurement | the first three tasks' actual duration; if task 6 or 8 overruns, the split point is between the intrinsics and the attributes |
| whether the driver can write the JSON column | Q1, and it stays written `[unverified]` in every document until task 3's probe runs. Every insert behind the design's measurements went through `CAST(<text> AS JSON)` from a staging table, never through the vendored driver. It is settled in code by task 3's implementer, not on paper here, and nothing is reordered around it | task 3's plan, with the probe pasted |
| where an `AnyValue` with no arm set goes | Q6. `sql-schema.md` §3.3 does not reach it | task 3, which states the choice and its effect on `{ .k != nil }` |
| whether the `+Inf` sentence in `sql-schema.md` §3.3 has a producer | a case-insensitive search of `docs/TraceQL/measure/` for `nan`, `infinity`, `isinf` and `1e400` matches only the word "nanosecond". No committed script inserts a non-finite float | task 3, which makes `T-A6` that producer |
| the fork's cost between tasks 9 and 20 | two compilers are in the tree for eleven tasks, and a query's answer depends on which side serves it. §2.1 states both behaviours that differ between the sides | task 20 removes it; nothing else can |
| whether the corpus-scale figures still hold | the 2,000,064-span numbers were measured once, on one machine, on 2026-09-23. No task here re-measures them; §4.1 says which terms of the model corpus c1 does and does not reach | the scheduled run of `measure/run_all.sh`, which D5 keeps outside the required checks |
| the reference comparison suites run per task | they are run per task and their output pasted, but no task here predicts what they will say | each task's own run |
| the occurrence counts in `measure/claim-domains.txt` | that file records how many numbers each sweep rule faces across the documents under `docs/TraceQL/`, and this is an eighth document. Its header still says seven and its four counts are the seven-document counts. **It is left stale deliberately.** Nothing in the required checks reads it — a search of `.github` and `ci` for `claims_check.py` and `claim-domains.txt` returns nothing — so it breaks no check; `measure/claims_check.py:473-486` compares it against the documents and exits 1 while they disagree, and `measure/run_all.sh:185-189` is what invokes that check | the next authorised run of the measurement suite: `claim_domains.py` rewrites the file. No task here regenerates it, and no task here is blocked by it |
