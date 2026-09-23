# TraceQL storage and query: the implementation, broken into tasks

Fourth of the documents under `docs/TraceQL/`, and the only one that talks
about the order of work rather than the design. It takes
`functional-requirements.md` (requirements, data, queries, the 75 test cases),
`sql-schema.md` (tables, SQL, measurements) and `server-implementation.md`
(write path, compiler, what is kept, replaced, deleted) as settled, and says
how the tree gets from the six trace tables it ships today to the five the
design describes, one merge at a time.

Nothing here changes a design decision. Where the design left a choice open,
§4 takes it and says so; where a choice needs a measurement that does not
exist yet, §5 names it and names the task whose own plan takes it.

---

## 1. The rule every task obeys

| the rule | what it means here |
|---|---|
| it lands on its own | one branch, one pull request, the five required checks green — `ci`, `wire-baseline-freeze`, `conformance-evidence-debt`, `schema-it`, `schema-it-cluster` |
| the tree works after it | every existing test passes at that point; each task below says what still reads and writes the old tables when it is done |
| one implementer, one sitting | no task holds a second one open, and no task waits on a decision another task has not taken |
| tests first | the implementer commits the tests alone — failing on an assertion, with empty stubs where a type does not exist yet — pastes that run, then commits the code that turns them green |
| nothing to migrate | the product has never been released, so no task copies data, and no task carries a cutover protocol |

**Reversibility.** Tasks 1 to 18 add; only task 19 removes. Every task before
it can be reverted by reverting its commit, because the old tables and the old
code are still there and still serving. Task 19 is the point of no return and
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
   task 19       the old six tables, the old compiler, the old evaluator
                 and the second half of the write path are deleted
```

Between task 4 and task 19 both stores hold the same spans. That is what makes
a route switchable one at a time: a route reading the new tables and a route
still reading the old ones answer over the same data.

**The search route switches shape by shape, not all at once.** Task 9 puts a
fork in `TraceEngine::search`: a query whose plan the new compiler covers is
answered from the new statement, and every other query is answered by the
engine that serves it today. Tasks 10 to 13 move shapes across the fork until
the old side is empty. The fork is mechanical, not a judgement call — task 9
commits an inventory file listing all 141 accepted corpus queries with the side
each takes, and each later task moves a stated number of rows across it. Task
19 deletes the fork.

### 2.1 What the two stores do not agree on while both exist

One thing, and it is the reason the ingest suppression is in task 4 rather
than later:

| case | the old tables | the new tables |
|---|---|---|
| the same request body sent twice, inside the suppression window | not stored twice — task 4's suppression is upstream of both inserts | not stored twice |
| the same body sent twice, outside the window | stored twice | collapses on the sorting key; `final = 1` makes the answer exact before the merge |

So for the retries the corpus and the fixture send, the two paths agree from
task 4 onward. A retry separated by more than the suppression window is
counted twice by a route still on the old tables and once by a route already
moved. That residue exists from task 4 to task 19, it is stated here rather
than designed away, and no test asserts the old side's answer to it.

## 3. The order, and what depends on what

```
   1  window rule ------------------------------------------------+
                                                                  |
   2  five tables --> 3  row encoder --> 4  dual write --+         |
                                                          |        |
                        +---------------------------------+        |
                        |                                          |
              +---------+-----------+--------------+               |
              |                     |              |               |
   5  fetch   6  predicates p1   16  tags      17  service graph    |
              |                                                    |
   7  predicates p2                                                |
              |                                                    |
   8  search statement                                             |
              |                                                    |
   9  search route fork <-------------------------------------------+
              |
      +-------+-------+----------+-----------+
      |       |       |          |           |
  10 by()  11 struct 12 trace-   13 slice   14 metrics --> 15 compare()
                        level             plan
      |       |       |          |           |              |
      +-------+-------+----------+-----------+--------------+
                        |
             18  corpus catalogue
                        |
             19  delete the old path
```

| task | must follow | why |
|---|---|---|
| 1 | — | it changes the shipped path only |
| 2 | — | it adds tables nothing reads |
| 3 | 2 | the round-trip test needs the tables |
| 4 | 3 | the writer inserts the rows the encoder builds |
| 5, 6, 16, 17 | 4 | they read data, so the data has to be there |
| 7 | 6 | part 2 extends part 1's predicate type |
| 8 | 7 | the statement embeds a predicate |
| 9 | 1, 8 | the fork serves answers, so the window rule must already be the one rule |
| 10, 11, 12, 13 | 9 | each moves rows across the fork |
| 14 | 7 | the metrics statement embeds a predicate; it does not need the search statement |
| 15 | 14 | `compare()` is a metrics shape and shares its window and settings |
| 18 | 10–17 | it runs the whole corpus through the shipped compiler |
| 19 | 18 | nothing may be deleted while something still reaches it |

**Pairs that can run at the same time.** File sets, not intentions:

| pair | disjoint because |
|---|---|
| 1 and 2 | task 1 is `crates/pulsus-read/src/traces/{search_sql,tags_sql,metrics_sql}.rs` and goldens; task 2 is `crates/pulsus-schema/` and `crates/pulsus-server/src/chconfig.rs` |
| 5 and 6 | task 5 is `traces/spans/fetch.rs` plus `traces_api/assemble.rs`; task 6 is `traces/spans/predicate.rs` |
| 16 and 17 | `traces/spans/tags.rs` against `traces/spans/graph.rs` |
| 11 and 12 | `traces/spans/structural.rs` against `traces/spans/tracelevel.rs` |

**The one file almost everything touches** is
`crates/pulsus-read/src/traces/exec.rs` (5,909 lines): each read task adds one
method to it. Two tasks running at once will conflict there textually even when
their logic does not. The rule for a pair running together: add the new method
at the end of the `impl TraceEngine` block, never in the middle, so the conflict
is a two-line one.

## 4. Decisions taken here

These are choices the design leaves open. They are taken now so no task plan
re-takes them, and each says what it costs.

| # | the choice | the decision | the cost |
|---|---|---|---|
| D1 | where the new read code lives | a new directory `crates/pulsus-read/src/traces/spans/`, one file per route — `predicate.rs`, `search.rs`, `structural.rs`, `tracelevel.rs`, `metrics.rs`, `tags.rs`, `graph.rs`, `fetch.rs`, `rows.rs`. The old files keep their names until task 19 deletes them | two compilers in the tree between tasks 6 and 19; in exchange no file is rewritten twice and no rename lands at the end |
| D2 | the three `CREATE FUNCTION` helpers in `measure/schema.sql` | none of them ships. `tqd_kv2json` and `tqd_kv2json_skip` exist to load the staging table and have no production caller. `tqd_unescape` is rendered inline as `replaceAll(replaceAll(<expr>, '%2E', '.'), '%25', '%')` wherever a stored path is turned back into an OTLP key | a user-defined function is server-wide, not per database, so it cannot be created or dropped with a tenant's schema and the controller has no mechanism for one. Inlining costs two function calls in the two statements that render a key: the `compare()` key universe and the tag-name read |
| D3 | how the new tables shard | `spans` and `traces` carry `Family::Traces` and a `_dist` wrapper, as `trace_spans` does today. `resources`, `tag_names` and `tag_values` follow `trace_tag_catalog`'s shipped pattern: `family: None`, `Replication::Global`, no `_dist` wrapper | a Global table is written from every shard and converges through its Replacing engine, which is the pattern already in the tree (`crates/pulsus-schema/src/catalog.rs`, migration 18) |
| D4 | the migration ids | new, appended after 63; no existing migration is amended. The old 38 trace migrations and 4 trace views stay recorded until task 19 | the amendment window is closed (`catalog.rs` header), and appending keeps the checksum guard meaningful |
| D5 | where the corpus-scale storage figures are asserted | R2's ≤ 45 B/span and R3's ≥ 6× are asserted in the required checks on a CI-scale corpus **generated with g1's stated parameters** — 5.45 span attributes per span, 15 resource attributes, 28.4 spans per trace, 24 services — with those parameters recorded in the test. The 2,000,064-span figures stay where they are, in `measure/results/`, and are re-measured by a scheduled run, not a required check | a bytes-per-span figure is a property of the corpus, so a test asserting one has to state the corpus it holds for. A 2M-span corpus does not fit a required check's budget |
| D6 | the shape of the search fork | one inventory file, `crates/pulsus-read/tests/traces_route_inventory.tsv`, one row per accepted corpus query with the side it takes. Task 9 commits it; tasks 10–13 each move a stated number of rows; task 19 deletes the file and the fork | the inventory is a committed count that a reviewer reads, instead of a claim in a comment that the fork is shrinking |
| D7 | the five live test files | exactly the five `functional-requirements.md` §8 names, and no more: `crates/pulsus-schema/tests/live_traces_v2.rs`, `crates/pulsus-write/tests/trace_rows_v2.rs`, `crates/pulsus-read/tests/traces_compile_v2.rs`, `crates/pulsus-read/tests/traces_query_v2_live.rs`, `crates/pulsus-server/tests/traces_api_v2_live.rs`. The first task that needs a file adds it **and its one step** to `.github/workflows/ci.yml`; every later task adds cases to a file that already has a step | five new steps in `schema-it` across nineteen tasks, rather than nineteen |
| D8 | the two fixtures the live tests seed | the worked fixture of `functional-requirements.md` §6.1 and fixture E of `measure/fixture/make_edge_fixture.py` are written once, in Rust, in `crates/pulsus-read/tests/fixtures/`, and shared by every live test that needs them. Their expected answers are the literal tables in §6.1, §8.3 and `measure/edge_checks.sh` | one seeding routine to review rather than nine, and the expected answers stay the ones the design wrote down |
| D9 | what happens to `docs/api.md` | each task that changes a client-visible behaviour edits the §4 section it changes, in the same commit. No task leaves a documentation edit to a later one | a route whose behaviour moved and whose documentation did not is the defect this rule exists for |

## 5. Decisions a task's own plan must take

Each of these needs a measurement or a source read that has not been done, and
each is named against the task that must take it before writing code.

| # | task | the question | why it cannot be settled here |
|---|---|---|---|
| Q1 | 3 | **Can the vendored driver write a `JSON` column, and in which form?** `vendor/clickhouse/src/rowbinary/validation.rs:629` accepts a `JSON` column against a serde `Str`/`String` and nothing else; a `serde_bytes` field maps only to `DataTypeNode::String`. The binary form the design names (`server-implementation.md` §2.3) is a path count followed by `(path, type tag, value)` triples, which is not a length-prefixed string | every insert in `measure/` went through `INSERT … SELECT CAST(<text> AS JSON)` from a staging table (`measure/schema.sql:100-111`), so the encoding through our own driver has never been exercised. Task 3's plan states the probe, runs it, and pastes the result: a span carrying `+Inf`, `-Inf` and `NaN` written through the driver and read back by `{ .k > 500 }`. If the binary form needs a driver change, that change is part of task 3 and is named in its plan |
| Q2 | 3 | **Is `json_type_escape_dots_in_keys` needed at all?** The measurement set it to 1 on every insert (`measure/apply_schema.py:15`) because it loaded text JSON. The writer escapes `%` then `.` itself, so a flat key carries no dot by the time ClickHouse sees it, while a nested object's structural dots must survive | the setting's interaction with the binary form is not recorded anywhere in the artifact. The probe is one span carrying a flat key `a.b` and one carrying a nested `{"a":{"b":1}}`, each read back by the query `sql-schema.md` §3.1 names |
| Q3 | 2 | **Does `resources` need a `day` column, or does the partition expression suffice?** `measure/schema.sql:109` writes `day` explicitly and the narrowed tag read prunes on it | the measurement's table has the column; whether the shipped DDL keeps it is a schema decision with a storage cost the task can measure directly |
| Q4 | 11 | **What `max_recursive_cte_evaluation_depth` does the climb need, and what does the server refuse above?** `sql-schema.md` §5.9 says the numbering statement carries the setting; §5.8's climb is bounded at 64 links | the numbering statement is not in the query path (§5.9), so the setting's value for the climb has not been established |
| Q5 | 14 | **Does the `compare()` differential against the reference still hold once the selection window moves?** `crates/pulsus-read/tests/compare_value_differential.rs` compares our `compare()` answer with the reference's for `{} \| compare({ status = error })`, which passes no `start`/`end` arguments, so task 1 should not move it | "should not" is an argument. Task 1's plan runs that suite and pastes the result rather than reasoning about it |

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
  `:282-312`) take the half-open window at nanosecond precision; the widening
  to every UTC day the window touches goes.
- `crates/pulsus-read/src/traces/metrics_sql.rs:1365` — `compare()`'s
  `sel_window` takes `start_closed_end_open`.
- `crates/pulsus-read/tests/golden/traces_search/*.sql` (75 files) and any tag
  golden that carries a window — regenerated, and the diff read.
- `docs/api.md` §4.2, §4.3, §4.4 — the three bound statements.
- `docs/benchmarks/traces-differential-ledger.md` — **one new row**, for
  `compare()`'s window only. There is no row to correct for the search window:
  the difference was never recorded as one.

**Must not break** — the service-graph window (`graph_sql.rs:65-67`) and the
metrics evaluation window (`metrics_sql.rs:86-114`) are already half-open and
are not touched. The per-step range selector inside a metrics query
(`metrics_plan.rs:473-482`) keeps its right-closed instants and is out of scope.

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
| `the reference comparison still holds` | `crates/pulsus-read/tests/compare_value_differential.rs` | not a new case: the existing suite is run against a live reference and its output pasted, answering Q5 |

`T-B4` — the bucket bound — is **not** in this task: the bound it names is over
the new span table's sort key. It is task 8's.

**Done when**

1. `T-B1`, `T-B2`, `T-B3`, `T-B5`, `T-B7`, `T-B8` pass and `T-B6` still passes.
2. The 75 search goldens are regenerated and every moved line is a window
   operator or a day literal — a reviewer reads the diff and finds nothing else.
3. `docs/api.md` §4.2, §4.3 and §4.4 state `start <= ts < end`, and the §4.3
   sentence about widening to every UTC day is gone.
4. `docs/benchmarks/traces-differential-ledger.md` has exactly one new row, for
   `compare()`, naming the endpoint.
5. `cargo test --workspace` is green, and the reference differential suites are
   run live and their output pasted.

---

### Task 2 — The five tables and the view

**Changes**

- `crates/pulsus-schema/src/catalog.rs` — five `Migration` records appended
  after id 63 (`spans`, `resources`, `traces`, `tag_names`, `tag_values`),
  their `_dist` records where D3 gives one, and one `MvDef` for `traces_mv`.
  DDL transcribed from `measure/schema.sql` with the repository's tokens
  (`{{db}}`, `{{on_cluster}}`, `{{retention_days}}`).
- `crates/pulsus-schema/src/controller.rs` — the TTL alterations name the new
  tables; `tag_names` and `tag_values` are excluded from TTL, because
  `docs/api.md` §4.3 requires catalog entries to outlive span retention.
- `crates/pulsus-schema/src/render.rs` — the `_dist` wrapper set follows D3.
- `crates/pulsus-server/src/chconfig.rs` — the new table names, `_dist`-aware,
  added to `TraceReadConfig` and to the writer's table set. Nothing reads them
  yet.
- `crates/pulsus-server/src/ops.rs` — per-table operational metrics for the new
  tables, beside the old ones.

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
| `T-R2` | same | a span at 2106-02-06 renders the clamped TTL, with no overflow past that date |
| `the sorting key is the one the design states` | same | `system.columns.is_in_sorting_key` over `spans` returns exactly `intDiv(start_ns, 300000000000)`, `trace_id`, `start_ns`, `span_id`, `kind` in that order, and the index granularity is 2048. It fails today because the table does not exist |
| `the catalogs carry no TTL` | same | `system.tables.engine_full` for `tag_names` and `tag_values` contains no `TTL`, while `spans`, `resources` and `traces` each do. It fails today because the tables do not exist |
| `the clustered form` | `crates/pulsus-schema/tests/live_cluster.rs` | `spans_dist` and `traces_dist` exist with sharding key `cityHash64(trace_id)`; `resources`, `tag_names` and `tag_values` exist on every shard and have **no** `_dist` twin — the shape `trace_tag_catalog` already has at `:291-301` |

**Done when**

1. The seven cases above pass; `cargo test -p pulsus-schema` is green.
2. `.github/workflows/ci.yml` gains one step in `schema-it` running
   `cargo test -p pulsus-schema --test live_traces_v2`, and one assertion in
   `schema-it-cluster`.
3. `docs/schemas.md` gains the five tables' DDL, beside the six that are still
   there.
4. No existing migration record is amended — a reviewer diffs `catalog.rs` and
   sees only appended records.

---

### Task 3 — The span row encoder, and the JSON column through the driver

The task that answers Q1 and Q2 before anything depends on them. Pure code
plus one live round-trip; no route and no writer wiring changes.

**Changes**

- `crates/pulsus-write/src/writer/rows.rs` — the new row shapes: one span row,
  one resource row, one tag-name row, one tag-value row. `TraceSpanRow` and
  `TraceAttrRow` stay where they are.
- `crates/pulsus-write/src/protocols/otlp_traces.rs` — the decoder is kept; a
  new encoder turns a decoded span into the new row. The payload-blob and
  attribute-row builders stay until task 19.
- `vendor/clickhouse/` — only if Q1's probe says the driver cannot write the
  column. If it can, this line is empty and the plan says so.

**Must not break** — `trace_ingest_fidelity.rs`, `trace_ingest_roundtrip.rs`
and the payload builder they pin. The new encoder is additional.

**Still on the old path afterwards** — everything. The encoder has no caller in
production code.

**Test cases**

| case | file | what it pins |
|---|---|---|
| `T-A3` | `crates/pulsus-write/tests/trace_rows_v2.rs` | a span with a flat key `a.b` and a span with a nested `{"a":{"b":1}}`: `{ span.a.b = 1 }` returns only the flat one |
| `T-A4` | same | an attribute literally named `a%2Eb` stores path `a%252Eb` and reads back as `a%2Eb` |
| `T-A5` | same | one span carrying `k="first"` and `k="second"`: the insert succeeds and the stored value is `first` |
| `T-A6` | same | attributes `+Inf`, `-Inf` and `NaN` are stored as `Float64`, and `{ .k > 500 }` matches the `+Inf` one. **This is Q1's probe**: it goes through the driver, not through `CAST(<text> AS JSON)` |
| `T-S2` | same | ingest one span with `service.name = "checkout"`: 0 rows of `resources` carry a `service%2Ename` path, and `spans.service` does |
| `T-W5` | same | 1,000 spans of one resource in one day produce exactly 1 row in `resources` |
| `the resource id is 128 bits and stable` | same | two spans whose resources differ in one attribute value get different `resource_id`; two spans with byte-identical resources get the same one; the width is `UInt128`. It fails today because no such identity exists |
| `every OTLP value kind round-trips` | same | one span carrying a string, an int, a double, a bool, a string array, a bytes value, an empty object and a nested object: each reads back as the design's §3.3 table says, with the bytes value and the empty object in `attrs_other` |

**Done when**

1. The eight cases pass against the tables task 2 created.
2. The plan's answer to Q1 and Q2 is pasted: the probe, its output, and — if
   the driver needed a change — what changed and what else reads that code.
3. `.github/workflows/ci.yml` gains one step running
   `cargo test -p pulsus-write --test trace_rows_v2`.
4. No production caller of the new encoder exists: a reviewer greps for it and
   finds only tests.

---

### Task 4 — Dual write

**Changes**

- `crates/pulsus-write/src/writer/trace.rs` — the writer gains the four new
  target tables beside its two. The resource cache keyed `(resource_id, day)`
  and the catalog cache keyed `(scope, key[, value, type])`, both the shape
  `MetricWriter`'s metadata cache already has.
- `crates/pulsus-write/src/writer/{mod,table,config,metrics}.rs` — the table
  list and per-table metrics grow; nothing is removed.
- `crates/pulsus-write/src/ingest/traces.rs` — the `PULSUS_INGEST_DEDUP`
  suppression of issue #494 extended to the trace route. It sits **upstream of
  both** inserts, which is what keeps the two stores agreeing (§2.1).
- `crates/pulsus-config/` — the trace suppression's settings join the known
  environment list; `docs/configuration.md` gains their rows.
- The `traces_mv` view created in task 2 starts receiving rows.

**Must not break** — the old two-table write path, its flush semantics, its
backpressure and its metrics. A failing new-table insert must fail the push,
the way the old ones do.

**Still on the old path afterwards** — every read. Both stores hold the data.

**Test cases**

| case | file | what it pins |
|---|---|---|
| `T-W1` | `crates/pulsus-write/tests/trace_rows_v2.rs` | the fixture's six bodies with one sent twice, in one insert block: `count()` is 9 and `count() FINAL` is 9 |
| `T-W2` | same | the same body in two separate inserts: `count() FINAL` is 9 before any merge |
| `T-W3` | same | one span id with kinds 2 and 3 — a shared span — is 2 rows after `FINAL` |
| `T-W6` | same | break the per-trace view, then insert: the insert fails and no span is stored without its index row |
| `the suppression covers traces` | `crates/pulsus-server/tests/traces_api_v2_live.rs` | the same OTLP body posted twice inside the window stores its spans once in **both** stores: `count()` on `trace_spans` and on `spans` are equal and are the body's span count. It fails today because the trace route has no suppression (`writer/trace.rs:187-189`) |
| `T-S3`, `T-S4` | `crates/pulsus-schema/tests/live_traces_v2.rs` | on the corpus D5 defines: total bytes per span ≤ 45, non-span tables ≤ 10% of the span table, and the span table's compression ratio ≥ 6 |

**Done when**

1. The seven cases pass.
2. A push writes both stores: a live test reads the same span count from
   `trace_spans` and from `spans` after one corpus load.
3. `docs/configuration.md` carries the trace suppression's settings.
4. The write-path metrics name both table sets, and `ops.rs` reports both.

---

### Task 5 — The trace fetch on the new tables

The smallest read, and the one that exercises every column of the span row.

**Changes**

- `crates/pulsus-read/src/traces/spans/fetch.rs` — the §5.4 statement: the
  trace's extent from `traces`, the spans by key, the distinct resources in the
  same statement.
- `crates/pulsus-read/src/traces/spans/rows.rs` — the row shape it decodes.
- `crates/pulsus-read/src/traces/exec.rs` — `fetch_by_id` issues the new
  statement.
- `crates/pulsus-server/src/traces_api/assemble.rs` — builds the OTLP response
  from columns instead of decoding a stored payload, and puts the service name
  back into the resource it renders.

**Must not break** — every golden and every conformance test that pins the
fetch response bytes. The response is the same bytes; only where they come from
changes. The `(span_id, kind)` de-duplication and the canonical output order
stay.

**Still on the old path afterwards** — search, metrics, tags, the service
graph. `trace_spans.payload` is still written and is now read by nothing.

**Test cases**

| case | file | what it pins |
|---|---|---|
| `T-W7` | `crates/pulsus-server/tests/traces_api_v2_live.rs` | a span carrying every field, an event, a link and all five value types: fetched back and compared as an OTLP value, equal, attribute order not significant |
| `T-Q3` (fetch half) | same | response bytes against `sum(byteSize(*))` over that trace's rows with `final = 1`: within R6's 2× |
| `T-Q1` (fetch row) | same | one statement per fetch request, counted from `system.query_log` by the request's `query_id` prefix |
| `the shared span survives the fetch` | same | a Zipkin shared span — one span id, kinds 2 and 3 — comes back as two spans, in `(start_ns, span_id, kind)` order |
| `the fetch response is byte-identical` | `crates/pulsus-server/tests/traces_api_live.rs` | not a new case: the existing fetch suite must pass unchanged, which is what says the response did not move |

**Done when**

1. The four new cases pass and `traces_api_live.rs` passes unchanged.
2. `system.query_log` shows one statement for a fetch.
3. A grep shows `assemble.rs` no longer decodes `payload`, and
   `docs/api.md` §4.1 is unchanged — because the response is unchanged.

---

### Task 6 — The predicate compiler, part 1

Span attributes and intrinsics. The type that every later read task embeds.

**Changes**

- `crates/pulsus-read/src/traces/spans/predicate.rs` — a `Query` leaf to one
  SQL boolean expression: the typed subcolumn read, `coalesce(…, false)` on
  every typed read with negation expressed over the result, the `Int64` and
  `Float64` variants for a numeric comparison, the `Bool` variant, array
  membership through `has(…)`, truthiness, presence and absence, the anchored
  regex, and the intrinsic columns (`name`, `kind`, `status`, `statusMessage`,
  `duration`, `span:id`, `span:parentID`, `trace:id`).
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
| `T-A1` | `crates/pulsus-read/tests/traces_query_v2_live.rs` | `{ span.http.response.status_code != 200 }` on the §6.1 fixture returns 8 spans — the seven lacking the key plus `…0006`. Without `coalesce` it returns 0 |
| `T-A2` | same | `{ … >= 500 }` → `…0001`; `{ … = "200" }` → `…0006`; `{ … = 200 }` → none |
| `T-A16` | same | `{ span.app.cache.hit }` matches only a value that **is** `true` |
| `T-A17`, `T-A18` | same | `{ span.app.tags != nil }` matches spans carrying the key whatever the value; `= nil` matches those not carrying it |
| `F6`, `F7`, `F8`, `F3`, `F9`, `F14`, `F20` | same | the §6.1 rows: a double comparison, array membership, a false bool, `status = error`, `duration > 1s`, a span name, `kind = consumer` — each returning the literal span ids of that table |
| `the window renders over the new column` | `crates/pulsus-read/tests/traces_compile_v2.rs` | the emitted text carries `start_ns >= <s> AND start_ns < <e>` and `intDiv(start_ns, 300000000000) BETWEEN <lo> AND <hi>`, with `<hi>` from `end - 1` |
| `the old renderings did not move` | `crates/pulsus-read/tests/golden_sql_freeze.rs` | not a new case: every existing golden passes unchanged after `window_sql` is parameterised |

**Done when**

1. The cases above pass, seeded with the §6.1 fixture (D8).
2. The existing goldens pass unchanged.
3. `.github/workflows/ci.yml` gains steps for
   `cargo test -p pulsus-read --test traces_compile_v2` and
   `--test traces_query_v2_live`.
4. No production caller of `spans/predicate.rs` exists yet.

---

### Task 7 — The predicate compiler, part 2

**Changes** — `crates/pulsus-read/src/traces/spans/predicate.rs` gains:
resource conditions as `resource_id IN (SELECT resource_id FROM resources …)`;
event and link conditions as `arrayExists` over the event/link `attrs`;
instrumentation-scope conditions over `scope_attrs`; the unscoped `.k` chain
span → resource → event → link → instrumentation; arithmetic `+ - * / % ^` and
unary `-` at the parser's precedence; a comparison whose right side is a field;
and the boolean combinators.

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
| `the unscoped chain reads the scopes in order` | same | fixture E's trace carrying the same key on a span and on its resource with different values: the span's value wins. It fails without the ordered `multiIf` |
| `T-T9` (compile half) | `crates/pulsus-read/tests/traces_compile_v2.rs` | an `instrumentation.k` condition compiles to a read of `scope_attrs`, not of `attrs` |

**Done when**

1. The cases pass; task 6's cases still pass.
2. Every construct in `server-implementation.md` §3.2 whose row names a leaf
   predicate has a case here or in task 6 — the plan lists the row-to-case map,
   and a reviewer checks every row is claimed.

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
| `the top-K runs once` | `crates/pulsus-read/tests/traces_query_v2_live.rs` | the statement's `system.query_log` `read_rows` for one search over the seeded corpus is within 5% of the single-pass count, not twice it. Written as a CTE it read 4,098,432 rows against 2,098,370; this is that difference |
| `T-C3` | same | a trace with five matching spans and `spss=3`: `matched` is 5, the spanset holds 3 |
| `T-C4` | same | two traces whose newest matched spans share a timestamp come back newest first, `trace_id` ascending |
| `F1` | same | `{}` over the §6.1 fixture returns all nine spans, newest trace first |
| `the detail read does not re-scan the window` | same | `read_rows` for the detail pass is bounded by the twenty traces' spans, not by the window's |

**Done when**

1. The cases pass.
2. The statement's shape is frozen as a golden under
   `crates/pulsus-read/tests/golden/traces_spans_search/`, one file per shape
   in the plan's list.

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
  inventory: one row per accepted corpus query, with `new` or `old`.
- `docs/api.md` — no change. The answers must not move.

**Must not break** — every existing search test, golden and corpus case. Both
sides answer over the same data, so a query moving across the fork must not
change its answer. That is what the corpus suites check.

**Still on the old path afterwards** — the shapes the inventory marks `old`:
`by()`, `select()`, aggregate filters, structural operators, trace-level
intrinsics, `span:childCount`, nested-set comparisons, and the broad-search
slice plan. Metrics, tags and the service graph are untouched.

**Test cases**

| case | file | what it pins |
|---|---|---|
| `the inventory is complete and exact` | `crates/pulsus-read/tests/traces_route_inventory.rs` | every query under `crates/pulsus-traceql/tests/corpus/accept/` and `grafana/` that the API serves has exactly one row, and the side the row states is the side the coverage predicate returns. It fails if a query is added and the file is not |
| `an answer does not move across the fork` | `crates/pulsus-read/tests/traces_query_v2_live.rs` | for every query the inventory marks `new`, the old engine and the new statement are both run over the same seeded fixture and their span id lists are compared element by element. It fails if the two disagree on any one |
| `F1`–`F20` through the route | `crates/pulsus-server/tests/traces_api_v2_live.rs` | the §6.1 table, asked over HTTP, with the literal span ids of that table |
| `T-C1` | `crates/pulsus-read/tests/traces_query_v2_live.rs` | the eighteen filters of §6.2 against the counts `measure/ground_truth.py` computes from the corpus file, on the corpus D5 defines |

**Done when**

1. The inventory file is committed and its test passes.
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
a predicate of the detail pass. The inventory moves the matching rows to `new`.

**Must not break** — the ungrouped statement of task 8.

**Still on the old path afterwards** — structural operators, trace-level
intrinsics, `span:childCount`, nested-set comparisons, the slice plan;
metrics, tags, graph.

**Test cases**

| case | file | what it pins |
|---|---|---|
| `T-A8` | `crates/pulsus-read/tests/traces_query_v2_live.rs` | `{ resource.service.name = "checkout" } \| by(span.rpc.method)`: one spanset per distinct **(value, stored type)** pair, groups in first-appearance order, no spanset for a span lacking the key, one statement |
| `the integer and the double do not merge` | same | the catalogue fixture's trace carrying `a = 1` (int) on one span and `a = 1.0` (double) on another: `{} \| by(span.a)` answers `('1', 'int', 1 span)` and `('1', 'double', 1 span)`. Grouping on the label alone gives one group of two spans — and grouping on the bare read does not run at all, `Code: 44` |
| `T-A19` | same | `{ resource.service.name = "checkout" } \| count() > 5 \| { status = error }` is **one** statement |
| `coalesce drops the key` | same | the same query with `\| by(span.rpc.method) \| coalesce()` answers exactly what the ungrouped query answers |
| `the top-K stays per trace` | same | a trace with many groups does not crowd another trace out of the twenty: a fixture with one trace in six groups and twenty-one traces in one, asking for twenty, returns twenty distinct traces |

**Done when**

1. The cases pass and the inventory's `new` count rises by the number the plan
   states.
2. `by(<expression>)` still answers `400` — `T-A15`'s search rows still pass.

---

### Task 11 — Structural operators

**Changes** — `crates/pulsus-read/src/traces/spans/structural.rs`: the five
relations in three modifiers. The three non-transitive ones as a set test
inside a per-trace group; the two transitive ones as the bounded recursive
climb; the relation-specific union partner sets; the candidate restriction that
differs for the negated forms; the `overflow` row as **its own row** of the
result; `PULSUS_TRACEQL_MAX_DEPTH` (default 64) as a new setting, with its row
in `docs/configuration.md`.

**Must not break** — task 10's statements.

**Still on the old path afterwards** — trace-level intrinsics, `childCount`,
nested-set comparisons, the slice plan; metrics, tags, graph.

**Test cases**

| case | file | what it pins |
|---|---|---|
| `T-A9` | `crates/pulsus-read/tests/traces_query_v2_live.rs` | all fifteen forms on the §6.1 fixture, with the literal span id list the design gives for each |
| `T-A9b` | same | fixture E's `ee06`: `&>` → `A2, B1`; `&<` → `A1, B1`; `&~` → `A3, B2`; the three plain forms → `B1`, `B1`, `B2`. One partner expression for all three answers `A1, A2, B1` twice and `B2` once |
| `T-A10` | same | the 65-span chain's leaf matches with `unresolved = 0`; the 66-span chain's leaf does not match and the statement's overflow row carries a non-zero `unresolved`, which the route turns into `422 query_too_broad` — never `200` with an empty spanset |
| `T-A11` | same | a two-span cycle terminates and reports `unresolved > 0`; and with the over-bound chain **alone** in the database, 0 match rows and `unresolved = 1` |
| `a trace with no A span answers the negated forms` | same | fixture E's `ee07` is returned by `!>`, `!<`, `!~`, `!>>` and `!<<`, and by none of the others |

**Done when**

1. The five cases pass, with the expected answers taken from
   `measure/edge_checks.sh` and `results/fixture-structural.tsv`.
2. `PULSUS_TRACEQL_MAX_DEPTH` is in the known-environment list, in
   `docs/configuration.md`, and in `server-implementation.md` §4's list — which
   already names it.
3. Q4 is answered in the plan, with the setting's value and the statement that
   needed it.

---

### Task 12 — Trace-level intrinsics, `span:childCount`, and the nested-set shapes

**Changes** — `crates/pulsus-read/src/traces/spans/tracelevel.rs`:
`trace:duration`, `trace:rootName`, `trace:rootService` joined from `traces`;
`span:childCount` from a per-`(trace_id, parent_span_id)` count joined back;
`nestedSetParent < 0` as the root anti-join with **no** numbering;
`nestedSetLeft > 0` and `nestedSetRight >= 1` as `true`; every other nested-set
comparison as the two statements of §3.5, with the reader numbering the
candidate traces using the Euler tour it already carries
(`search_eval.rs:2085-2139`).

**Must not break** — the numbering rule itself, which the retained
implementation already gets right for same-instant siblings, an unstored
parent, and a cycle.

**Still on the old path afterwards** — the slice plan; metrics, tags, graph.

**Test cases**

| case | file | what it pins |
|---|---|---|
| `T-C5` | `crates/pulsus-read/tests/traces_query_v2_live.rs` | a trace whose root is outside the request window: `trace:duration` is `3602000000000` ns and `trace:rootService` is `loadgen`, and `{ trace:rootService = "loadgen" && trace:duration > 1h }` returns it. A window-only computation gives 2 s and `cart` |
| `T-A20` | same | `{ span:childCount > 3 }`, and the numbering `root 1/8/-1`, `A 2/5/1`, `C 3/4/2`, `B 6/7/1`; a 1,000-span trace has a maximum right of exactly 2,000 |
| `T-A21` | same | `{ nestedSetParent < 0 }` over the corpus returns one span per trace, and the statement carries **no** recursive CTE |
| `T-A22` | same | the same query over fixture E returns exactly `ee01:01, ee01:05, ee02:04, ee03:01, ee04:01, ee06:01, ee07:01` — the orphan is returned, a span inside a pure cycle is not |
| `T-A23` | same | on the cyclic trace, the numbering gives the promoted member `parent = -1`, which is where the two paths differ |
| `T-Q2b` | same | `{ resource.service.name = "checkout" && nestedSetLeft > 5 }` issues two statements and hands the reader at most the trace cap × `MAX_SPANS_PER_TRACE` span rows, never a window's |

**Done when**

1. The six cases pass.
2. `docs/api.md` §4.2 gains the entry for the cycle difference, and
   `docs/benchmarks/traces-differential-ledger.md` gains its row.
3. The inventory's `old` side no longer holds a nested-set query.

---

### Task 13 — The newest-slice-first search plan

**Changes** — `crates/pulsus-read/src/traces/spans/search.rs`: the first pass
bounded to a 5-minute slice, doubling, stopping when twenty traces are in hand;
the rule that decides whether to use it, from the first slice's own match
count; the statement-count bound `⌈log₂(window / 5 min)⌉ + 1`.

**Must not break** — the answer. A trace found in a newer slice always outranks
one found only in an older slice, and the second pass still covers the whole
window through the per-trace extents.

**Still on the old path afterwards** — metrics, tags, the service graph.

**Test cases**

| case | file | what it pins |
|---|---|---|
| `the sliced answer equals the whole-window answer` | `crates/pulsus-read/tests/traces_query_v2_live.rs` | for each of `{}`, a service filter, `span.http.response.status_code >= 500` and a `select()` over errors, the twenty trace ids the sliced plan returns equal, in order, the twenty the single statement returns |
| `the rare filter does not slice` | same | `{ span.app.user.id = "u-10013" }` issues **one** statement, because the first slice's match count says the filter is not broad. Sliced it costs 6 statements |
| `the statement count is bounded` | same | over a 3-hour window the sliced plan issues at most `⌈log₂(180/5)⌉ + 1` = 7 statements, counted from `system.query_log` |
| `T-Q1` (search rows) | `crates/pulsus-server/tests/traces_api_v2_live.rs` | every search request in `measure/api_requests.tsv` issues the number of statements that file states |

**Done when**

1. The four cases pass.
2. `server-implementation.md` §3.5's first row is the shipped rule — the plan
   quotes the condition the code applies and it is the one that table states.

---

### Task 14 — Metrics on the new tables

**Changes** — `crates/pulsus-read/src/traces/spans/metrics.rs`: the §5.6
statement. One row per series, not one per point; the right-closed step label;
grouping by a resource attribute resolved through `resource_id` and joined
afterwards; exemplars from `argMax((trace_id, span_id, duration_ns),
(duration_ns, span_id))` in the same pass; `topk`/`bottomk` ordering the
finished series inside the statement; the trailing metrics-result comparison as
a `HAVING`. `TraceEngine::metrics_range` and `metrics_instant` issue it.

**Must not break** — the plan-time `400`s. `metrics_plan.rs:1102-1121` still
admits exactly one `by` key and only `resource.service.name`; enabling more is
a separate change with its own issue. `T-A15` is the guard.

**Still on the old path afterwards** — `compare()`, tags, the service graph.

**Test cases**

| case | file | what it pins |
|---|---|---|
| `T-A12` | `crates/pulsus-read/tests/traces_query_v2_live.rs` | `{ } \| rate() by (resource.service.name)` with exemplars: one exemplar per bucket per series, each naming a `(trace:id, span:id)` that exists, and the same exemplar on a second run |
| `T-A13` | `crates/pulsus-server/tests/traces_api_v2_live.rs` | `{ } \| count_over_time() by (resource.service.name) \| topk(3)` over the §6.1 fixture returns exactly `frontend` 3, `accounting` 2, `checkout` 2 — `payment` also has 2 and is absent; `bottomk(3)` returns `accounting`, `checkout`, `payment` and drops `frontend` |
| `F21` | same | `{} \| count_over_time()` over the fixture window answers **9**, not 11 |
| `T-A15` (metrics rows) | same | **guard.** The five metrics requests whose "today" column reads `400` still answer `400` with the §4 envelope; the message text is not asserted |
| `T-Q3` (metrics half) | same | ≤ 24 bytes per point per series |
| `T-X4` | same | **guard.** At `PULSUS_TRACEQL_READ_MAX_MEMORY_BYTES = 1048576` the served metrics query answers `422 query_too_broad` naming `reader.traceql_read_max_memory_bytes`, and `200` at the default |

**Done when**

1. The cases pass; `T-A15` and `T-X4` passed before the change and still pass.
2. The metrics goldens are regenerated and the diff read.

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

**Done when**

1. The four new cases pass and the differential suite is run live.
2. `docs/api.md` §4.4 carries the `span:id` entry, and
   `docs/benchmarks/traces-differential-ledger.md` its row.

---

### Task 16 — Tags on the two catalogs

**Changes** — `crates/pulsus-read/src/traces/spans/tags.rs`: names from
`tag_names` (time-less), unnarrowed values from `tag_values` (time-less, typed
per value), narrowed values from the store bounded by the window, the `name`
intrinsic from the store bounded by the window, and every other intrinsic from
the static vocabulary with no statement at all.

**Must not break** — `docs/api.md` §4.3, which does not change. The caps
(`TAG_NAMES_MAX = 10_000`, `TAG_VALUES_MAX = 1_000`) and their `truncated`
flags stay, in **both** value paths.

**Still on the old path afterwards** — the service graph, if task 17 has not
landed.

**Test cases**

| case | file | what it pins |
|---|---|---|
| `T-T1`, `T-T2` | `crates/pulsus-server/tests/traces_api_v2_live.rs` | **guards.** A name is listed although its span is outside the window, and still listed after the span's day is dropped |
| `T-T3` | same | one key with 1,000 values: `/tag/{k}/values` with no `q` is answered from `tag_values` and reads no span table |
| `T-T4` | same | **guard.** `q={resource.service.name="cart"}` narrows to that service's values and reads the span store |
| `T-T5` | same | **guard.** `k` as int 8080 in one span and string `"8080"` in another gives two entries, `int` and `string`, same text |
| `T-T6` | same | `/tag/name/values` returns only the in-window names, on the `[start, end)` rule |
| `T-T7` | same | **guard.** `/tag/status/values` returns `ok`, `error`, `unset` typed `keyword`, with **zero** statements |
| `T-T8` | same | **guard.** 10,001 names and 1,001 values cap at 10,000 and 1,000 with `truncated: true` |
| `T-T9` | same | a scope attribute `otel.scope.build = "release"` is listed under `scope=instrumentation` and its value is returned |
| `the catalogs carry all five scopes` | `crates/pulsus-read/tests/traces_query_v2_live.rs` | after seeding the fixture, `tag_names` holds a row in each of `span`, `resource`, `event`, `link` and `instrumentation` |

**Done when**

1. The nine cases pass; the six guards passed before and still pass.
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

**Must not break** — the graph window, which is already `[start, end)` and
stays so; `T-B6` is the guard. The edge ledger `trace_edges` is still written
and is now read by nothing.

**Still on the old path afterwards** — nothing on the read side except the
shapes the fork still marks `old`, if any remain.

**Test cases**

| case | file | what it pins |
|---|---|---|
| `T-C6` | `crates/pulsus-server/tests/traces_api_v2_live.rs` | one ordinary pair and one shared pair: **two** rpc edges, `svc-a → svc-b` and `svc-a → svc-c`, one call each. A single-branch join returns only the first |
| `T-C7` | same | one rpc pair and one messaging pair between the same two services: two edges differing only in `connectionType` |
| `T-B6` | same | **guard.** The edge whose server span starts at exactly `end` is absent |
| `the cap sets truncated` | same | 1,001 distinct edges in the window: 1,000 returned and `truncated` set |

**Done when**

1. The four cases pass.
2. The graph route issues one statement, counted from `system.query_log`.

---

### Task 18 — The corpus catalogue against the shipped compiler

`query-catalogue.md` records that all 138 served corpus queries and the 12 of
`measure/catalogue-extra.tsv` agree with an independent interpreter. Those
statements came from `measure/catalogue_render.py`, which is the design's
renderer, not this repository's compiler. This task points the same comparison
at the compiler that now ships.

**Changes**

- `crates/pulsus-read/src/bin/` or `xtask/` — a command that, for each corpus
  query, prints the statement the route would issue. The route already has
  `search_explained`; this extends it to every route.
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
| `T-C9` | `crates/pulsus-server/tests/traces_api_v2_live.rs` | **guard.** All 50 refusals — 47 from the parser and the validator, 3 from the planner — answer `400` with the §4 envelope, with the message the corpus golden pins for the 47 |
| `the comparison can go red` | the catalogue job | one rule of `measure/perturbations.tsv` is changed on the compiler side only, in a scratch copy, and the comparison is required to report a disagreement. The rule and the run are pasted. A comparison nobody has seen fail is not one |

**Done when**

1. `T-C8` reports 138 of 138 and 12 of 12, and the run is pasted.
2. The planted-error run is pasted and shows the comparison going red.
3. `docs/TraceQL/query-catalogue.md` gains a sentence saying the statements now
   come from the shipped compiler, and which revision produced the recorded run.

---

### Task 19 — Delete the old path

The only task that removes anything, and the only irreversible one.

**Changes**

- `crates/pulsus-schema/src/catalog.rs` — the 38 trace migrations (ids 16–20
  and 31–63) and the 4 trace views. **Key the set on the table name, not on
  `family`**: 36 of the 38 carry `Some(Family::Traces)`, and ids 18 and 41 —
  `trace_tag_catalog` — carry `family: None`. A change that deletes only the
  trace family leaves those two behind while removing the table they manage.
- `crates/pulsus-read/src/traces/` — `search_plan.rs`, `search_eval.rs`,
  `search_sql.rs`, `filter.rs`, `compile.rs`, `metrics_plan.rs`,
  `metrics_sql.rs`, `tags_sql.rs`, `tag_narrow.rs`, `graph_sql.rs`, `sql.rs`
  and the old `rows.rs` shapes, and the fork of task 9 with its inventory file.
- `crates/pulsus-write/src/writer/trace.rs`, `ingest/traces.rs`,
  `writer/rows.rs`'s `TraceSpanRow`/`TraceAttrRow`, the payload builder and the
  attribute-row builder in `protocols/otlp_traces.rs`, and the attrs backfill
  path in `writer/backfill.rs`.
- `crates/pulsus-server/src/chconfig.rs`, `ops.rs` — the old table names.
- `crates/pulsus-model/src/time.rs` — the comments describing the old trace
  tables' timestamp needs. The admitted domain rule itself stays.
- Every test file and golden that pins an old-path statement.

**Must not break** — the API. Every conformance corpus, the accept surface, the
comparison suites against a live reference, and every `docs/api.md` §4
behaviour.

**Still on the old path afterwards** — nothing.

**Test cases**

| case | file | what it pins |
|---|---|---|
| `T-S1` | `crates/pulsus-schema/tests/live_traces_v2.rs` | now asserted **whole**: the set of columns holding attribute values is exactly `spans.attrs`, `spans.scope_attrs`, `spans.attrs_other`, the `attrs` inside `spans.events` and `spans.links`, `resources.attrs`, `resources.attrs_other`, `tag_values.value` — and nothing else. It cannot pass before this task, because `trace_attrs_idx.val` and `trace_spans.payload` are still there |
| `T-Q1` | `crates/pulsus-server/tests/traces_api_v2_live.rs` | now asserted **whole**: every request in `measure/api_requests.tsv` issues the number of statements that file gives — 1 for every request but the four of §3.5 |
| `T-X1`–`T-X5` | `crates/pulsus-write/tests/trace_rows_v2.rs`, `crates/pulsus-server/tests/traces_api_v2_live.rs` | **guards.** The five protections still hold with the old code gone: the 256 MiB request expansion ceiling and its sweep, the `MAX_ANYVALUE_DEPTH = 32` limit, the scan-row budget, the read memory ceiling, the admitted timestamp domain |
| `the trace migration set is exactly the 38 and the 4` | `crates/pulsus-schema/tests/live_traces_v2.rs` | after the deletion, no `Migration` and no `MvDef` names an old trace table, and the count removed is 38 and 4. Keying on `family` alone leaves ids 18 and 41 and fails this |
| `no code names an old trace table` | `crates/pulsus-read/tests/traces_route_inventory.rs`, rewritten as a source sweep | a search over `git ls-files` for the six old table names, run from the repository root with the file list non-empty and standard input from `/dev/null`, returns only `docs/` history |

**Done when**

1. All five cases above pass, and `T-S1` and `T-Q1` pass in their whole form.
2. `cargo test --workspace` is green and the five required checks are green.
3. `docs/schemas.md` §4 describes five tables, not eleven.
4. The line count removed is stated in the pull request body and matches
   `server-implementation.md` §6's table: about 34,876 lines of read path and
   about 3,300 of write path.

## 7. What this breakdown does not settle

Stated here rather than found later.

| what | why it is not settled | who settles it |
|---|---|---|
| the size of each task in hours | no task here has been implemented, so "one sitting" is a judgement from the file sets and the case counts, not a measurement | the first three tasks' actual duration; if task 6 or 8 overruns, the split point is between the intrinsics and the attributes |
| whether the driver can write the JSON column | Q1. Every insert behind the design's measurements went through `CAST(<text> AS JSON)` from a staging table, never through the vendored driver | task 3's plan, with the probe pasted |
| whether the `+Inf` sentence in `sql-schema.md` §3.3 has a producer | a case-insensitive search of `docs/TraceQL/measure/` for `nan`, `infinity`, `isinf` and `1e400` matches only the word "nanosecond". No committed script inserts a non-finite float | task 3, which makes `T-A6` that producer |
| the fork's cost between tasks 9 and 19 | two compilers are in the tree for ten tasks, and a query's answer depends on which side serves it. §2.1 states the one behaviour that differs between the sides | task 19 removes it; nothing else can |
| whether the corpus-scale figures still hold | the 2,000,064-span numbers were measured once, on one machine, on 2026-09-23. No task here re-measures them | the scheduled run of `measure/run_all.sh`, which D5 keeps outside the required checks |
| the reference comparison suites | they are run per task and their output pasted, but no task here predicts what they will say | each task's own run |
| the occurrence counts in `measure/claim-domains.txt` | that file records how many numbers each sweep rule faces across the documents under `docs/TraceQL/`, and this is an eighth document. Its header still says seven, and its four counts are the seven-document counts. Nothing in the required checks reads it; `measure/claims_check.py` compares it against the documents and exits 1 while they disagree | whoever next runs the measurement suite: `claim_domains.py` rewrites the file, and the `doc_count` claim in `functional-requirements.md` — already moved to 8 here — is the one number in it that this change could compute without re-running the sweep |
