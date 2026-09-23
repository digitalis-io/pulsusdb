# TraceQL storage and query: functional requirements

This document says what the trace store must do, in numbers a review can check.
It is the first of the 7 documents under `docs/TraceQL/`:

| document | subject |
|---|---|
| `docs/TraceQL/functional-requirements.md` | the requirements, the data, the queries, the benchmark, the test cases |
| `docs/TraceQL/sql-schema.md` | the tables, the SQL each query compiles to, the measurements |
| `docs/TraceQL/server-implementation.md` | the write path, the compiler, what is kept, replaced, deleted |
| `docs/TraceQL/query-catalogue.md` | **every query in the repository's TraceQL corpus**: 138 the API serves, each with the statement the route issues, the membership statement and its literal answer, and 50 it refuses — 47 at parse or validation, 3 at plan time — each with its status and reason |
| `docs/TraceQL/query-catalogue-accepted.md` | generated: the served queries, one row each |
| `docs/TraceQL/query-catalogue-refused.md` | generated: the refusals, one row each |
| `docs/TraceQL/measure/README.md` | the scripts, the order to run them in, what a second run reproduces, and what is checked about the numbers above |

Every figure was produced by a script committed under `docs/TraceQL/measure/`,
against ClickHouse 26.3.29.7 and against the pinned reference build
`grafana/tempo:3.0.2@sha256:cda87c21…` (`deploy/e2e/compose.single.yaml:158`),
both limited to 4 CPUs and 6 GB on one machine. Raw outputs are in
`docs/TraceQL/measure/results/`; `measure/README.md` gives the order to run them
in, from a clean checkout.

**Which figures are current, and which are carried forward.** Every corpus-scale
figure in these documents comes from the run of 2026-09-23 that wrote `results/`.
Revisions made after that run re-derive the counted claims against the tree with
`measure/claims_check.py` and re-run the catalogue over its fixture; they do
**not** re-measure the corpus. `measure/README.md`, under "What this revision
re-derived, and what it carries forward", says which is which. A figure whose
producer is `run_all.sh` is as old as the last `run_all.sh`.

---

## 1. Why the store is being rebuilt

Six trace tables ship today: `trace_spans`, `trace_attrs_idx`,
`trace_tag_catalog`, `trace_edges`, `trace_recent`, `trace_error_spans`. Each
arrived with an issue that needed a faster path, and each stored another copy of
data already stored.

Measured on corpus `g1` (§4), loaded through the PulsusDB server at `5500145d`
and through the reference, from the same 4,073 OTLP request bodies:

| store | bytes on disk | bytes per span | rows written per span |
|---|---:|---:|---:|
| PulsusDB today, six tables | 1,508,935,285 | **754.44** | 22.64 |
| the reference, three active blocks | 209,324,594 | 104.66 | — |
| this design, five tables | 74,551,799 | **37.275** | 1.187 |

Today's bytes, per span: the attribute index 493.43, the span table 236.27 (the
OTLP payload blob 58.12, the five attribute arrays about 90, two re-sorted
projections 63.6), the service-graph ledger 22.49, the tag catalog 1.14, the
recency table 0.93, the error table 0.19. An attribute value is stored **three
times** — in the payload blob, in the span row's arrays, and as a row of the
attribute index — and a fourth time, deduplicated, in the tag catalog.

Three query shapes on today's server, same corpus, same machine
(`results/g1-today.tsv`). These three were measured when the corpus was first
built and are carried forward twice over: the 2026-09-23 run had no server of
the old design to re-measure them against, and no run since has measured
anything at corpus scale. Their input is `measure/baseline/g1-today.tsv`, which
carries its own date and command:

| query | response time | statements per request | rows read |
|---|---:|---:|---:|
| `{ rootServiceName = "loadgen" && traceDuration > 2s }` | 75,369 ms | 3,685.5 | 731,880,713 |
| `{ … } >> { … }` (descendant) | 5,069 ms | 185.8 | 36,223,690 |
| `{ resource.service.name = "payment" && status = error }` | 496 ms | 22.0 | 3,921,164 |

`| rate() by (span.http.route)` — any grouping key that is not the service name
— answers `400` today.

## 2. Two decisions taken by the owner while this was being written

**Object storage is out of scope (2026-09-22).** Two requirements were withdrawn
by the owner and are recorded here so a later reader does not think they were
missed:

> "we need to be able to leverage object store offloading of data"
> "once files are offloaded to object storage, it is no longer replicated like
> it would be while being hosted on clickhouse with replication factor"

The design is therefore single-tier: span data lives on local disks and is
replicated the way ClickHouse replicates anything. There is no hot/cold split,
no object-storage volume, no per-shard cold owner and no cutover protocol — and
so no place for the double-counting a copy-then-drop cutover would have had to
prevent. §3 keeps what the measurement established before the withdrawal.

**The search window matches the reference at both ends (2026-09-22).** The rule
is `start <= ts < end`, on every read path. §5.5 states it and tests it.

## 3. What was established about object storage before the requirement was withdrawn

Measured on two ClickHouse 26.3.29.7 replicas of one shard sharing a keeper,
over an S3-compatible store (`measure/offload.sh`), on a table whose parts are
79,007,851 bytes per replica. Kept because it cost real work and the next person
should not repeat it:

| mechanism | copies in the store | evidence |
|---|---:|---|
| replicated table on an S3 disk, zero-copy off | **2** | 98 objects, 158,020,906 bytes |
| the same, zero-copy on | 1 | 50 objects, 79,010,454 bytes |
| local hot volume with `TTL … TO VOLUME`, zero-copy off / on | **2** / 1 | 78,646,696 / 39,323,348 bytes after the day's drop |
| replicated table on a `plain_rewritable` disk | **2** | two disjoint 84,278,977-byte object sets, one per replica |
| the shared-storage engine | — | `Code: 56 … Unknown table engine SharedMergeTree` |

`allow_remote_fs_zero_copy_replication` is tier `Experimental` in 26.3.29.7 and
its description is *"Don't use this setting in production, because it is not
ready."*; with it on, `FREEZE`, `DETACH` and `FETCH PARTITION` answer `Code: 344`
(`disable_*_for_zero_copy_replication`, default 1). A single-copy tier would
therefore have needed either that setting or one server per shard owning the
data — which is where the withdrawn requirement stood when it was withdrawn.

## 4. What the design must do

Each requirement gives the number, and the command or test that can return the
other answer. "g1" is the corpus of §5.

| # | requirement | the check that could fail |
|---|---|---|
| R1 | **A value is stored once.** A span attribute is stored once per span, in `spans.attrs`; a resource attribute once per distinct resource per day, in `resources.attrs`; an event or link attribute inside its own event or link. No payload blob, no attribute index, no per-span copy anywhere. The two catalogs and the per-trace table are indexes: each holds one row per **distinct** key, value or trace, never one per span, and together they stay inside R2. | `T-S1`/`T-S2`: a query over `system.columns` and over `resources` that fails if any column outside the list holds attribute values, and if any resource row repeats the service name. Measured: 68 resource rows, **0** carrying `service.name` |
| R2 | **≤ 45 bytes per span on disk** for the whole store, and the non-span tables **≤ 10%** of the span table. | `T-S3`: measured **37.275 B/span**, index overhead **6.605%** |
| R3 | **Compression ratio ≥ 6×** on the span table. | `T-S4`: measured **6.977×** (243.9 → 34.965 B/span); today 4.53× |
| R4 | **One statement per API request**, except the four cases `server-implementation.md` §3.5 names, each with its count and reason. | `T-Q1`: a live test that issues each API request and counts rows in `system.query_log` for that request's `query_id` prefix; it fails if the count exceeds the number the case table gives. Today's counts, for contrast: 2.2 to 3,685.5 |
| R5 | **The database does the work.** Every statement returns final answer rows: filtering, grouping, top-K, aggregation, quantiles, histogram bucketing, structural evaluation, exemplar selection and per-trace evaluation all run in ClickHouse. **One exception, measured**: the nested-set numbering for a comparison other than the two shapes `server-implementation.md` §3.2 answers directly. Numbering in SQL re-reads the span set once per recursion step, which over this corpus exhausted a 6 GB server after 2 m 12 s; the reader numbers the candidate traces instead — at most the trace cap × `MAX_SPANS_PER_TRACE` spans, hydrated by one 35 ms statement. | `T-Q2`: for each statement in `measure/sql/`, the test asserts the reader performs no per-span work — the row count returned is the answer's own size (≤ `limit` traces, ≤ series × points), not a span count |
| R6 | **Bytes returned**: ≤ 8 KB for a search whatever it matched; ≤ 24 bytes per point per series for a metrics range query; ≤ 4 KB for tag names or values; for a trace fetch, **≤ 2× the uncompressed stored bytes of that trace's rows**, which is the denominator `T-Q3` computes with `byteSize`. | `T-Q3`: measured 546–4,926 B per search; 10.3–19.8 B per point; 100–1,445 B for tags; fetch 11,699 B against 11,708 (1.00×) and 335,128 against 589,696 (0.57×), the denominators being `sum(byteSize(*))` over that trace's rows with `final = 1`, which is the expression `measure/run_all.sh` runs and `results/r6-denominators.tsv` records |
| R7 | **A span crosses a zone once per further replica**, and no read path reads a column twice in one statement. | `T-W4`: `measure/replication_bytes.sh` reads replica 2's `system.part_log`; it fails if the bytes fetched per span exceed the stored part bytes per span by more than **5%** — one compressed copy and its part metadata, never a second copy. Measured **34.965 B/span** fetched against **34.923** stored, which is 1.0012× |
| R8 | **Retention is a partition drop**: one `ALTER TABLE … DROP PARTITION` per table per day, no `ALTER … DELETE`, no row rewrite, and no merge scheduled by it. | `T-R1`: `measure/retention.sh` — measured **0.068 s** for a day of 2,000,064 spans, 0 mutations, 0 merges |
| R9 | **One window rule**: `start <= ts < end` wherever a time window selects spans, with the day-partition bound and the bucket bound rendered from the same last-included nanosecond. Three windows change (search, the store-backed tag reads, `compare()`'s `start`/`end` arguments); the metrics evaluation window and both halves of the service graph are already half-open and must stay so; a metrics range selector's own `(aS − step, aE]` instants are not a request window and do not change. §4.1 is the whole inventory, read off the code. | `T-B1`–`T-B8`: `measure/boundary.sh`, one case per changed window and one regression case for the two that do not change |
| R10 | **Faster than the reference**, warm, interleaved, on the same machine and data, for every shape except the search class §6.3 names. | `T-P1`: `measure/fetch_compare.py` and `measure/http_bench.py`; measured below |
| R11 | **Exact answers.** A retried push is counted once; structural and trace-level queries evaluate over the whole trace; typed comparisons do not cross types. | `T-C1`–`T-C6`: the 18 corpus filters and the fixture |
| R12 | **The protections survive the replacement**: the 256 MiB request expansion bound, the attribute nesting depth limit, the scan-row and result-byte budgets, the admitted timestamp domain, and the recursive-climb depth bound. | `T-X1`–`T-X5`: one case per protection sending the breaching input and asserting the status and body |

### 4.1 Every window, and what the ruling changes

The owner's ruling (2026-09-22) is one rule for every time window that selects
spans: `start <= ts < end`. This table is read off the code, one row per window,
with the file and lines that decide it. Three of the eight are already
half-open, so the ruling costs them nothing; saying so is the point of the
table, because "make them all consistent" applied to the wrong three would move
answers nobody asked to move.

| window | today, and where it is decided | under the rule | the edit |
|---|---|---|---|
| search span selection | `ts > start AND ts <= end` — `crates/pulsus-read/src/traces/search_sql.rs:106-124` | `[start, end)` | `search_sql.rs:124` takes `WindowSql::start_closed_end_open`; `docs/api.md` §4.2. Every `golden/traces_search/*.sql` moves |
| service graph, both halves | **already `[start, end)`** — `crates/pulsus-read/src/traces/graph_sql.rs:65-67` | unchanged | none. The graph half of `T-B5` is the regression half: that assertion holds before the change and must hold after, so the case fails if the graph is "tidied" into the search convention. `T-B6` is the guard that covers the same window end to end |
| metrics evaluation window | **already `[start, end)`** — `crates/pulsus-read/src/traces/metrics_sql.rs:86-114` | unchanged | none |
| the per-step range selector inside a metrics query | the instants `(aS − step, aE]`, rendered as `[aS − step + 1, aE + 1)` — `crates/pulsus-read/src/traces/metrics_plan.rs:473-482` | unchanged, and **out of scope** | none. This is not the request's window: it is what a range selector means in the query language, the same right-closed instant set Prometheus and the reference define, and it is computed from the already-half-open request window. Changing it would move every metrics value by one step edge |
| `compare()`'s `start`/`end` arguments | `ts > start AND ts <= end` — `metrics_sql.rs:1355-1365`, `docs/api.md` §4.4 | `[start, end)` | `metrics_sql.rs:1365`; `docs/api.md` §4.4; a ledger row, because the reference defines this one as right-closed (`pkg/traceql/engine_metrics_compare.go:98-110` @ v3.0.2, `spanStartTime > start && <= end`) |
| tag **values**, the store-backed reads (`q`-narrowed values, and the `name` intrinsic) | the window is **widened to every UTC day it touches** — `docs/api.md` §4.3 | `[start, end)` at nanosecond precision, with the day partition pruned from the same last-included nanosecond | `docs/api.md` §4.3's "widened to every UTC day" sentence, and the tag-value read |
| tag **names** | time-less: `start`/`end` are accepted and ignored — `docs/api.md` §4.3 | unchanged — a name is not a span selection, and the catalog has no timestamp column | none |
| trace by id | no window: the per-trace extent locates the spans | unchanged | none |

One more place renders a window, and it is in this branch rather than in the
product: the statements `measure/make_sql.py` generates. Three of them derived
the day-partition bound from the window's `end` where the file's own rule says
the last included nanosecond, `end - 1`; that is corrected here, and every
generated statement now renders the row bound, the bucket bound and the day
bound from the same value.

Three of the rows are behaviour a client can see, and each gets its own case
with literal values in §8: the search boundary at both ends (`T-B1`, `T-B2`),
a tag value that exists only before the window opens on the window's first day
(`T-B7`), and a span at exactly `compare()`'s `end` (`T-B8`).

### R1 stated precisely

A value may be stored once per **distinct thing it describes**, not once per span
that mentions it:

- a **span** attribute: once per span;
- a **resource** attribute: once per distinct resource per day. The span row
  carries a 128-bit `resource_id` (1.49 B/span compressed) instead. On g1 the 68
  distinct resources cost 6,412 bytes in total, against 27,983,608 bytes —
  13.99 B/span — when the same attributes sit inline on every span (measured on
  `l_resource_inline`, `measure/layouts.sql`);
- the **service name** is stored once, as the `spans.service` column, and is
  **removed** from `resources.attrs`; the reader puts it back into the resource
  it renders. Verified: 0 of 68 resource rows carry a `service.name` path;
- a **derived** row is allowed if it is an index and stays inside R2: the
  per-trace table (1.46 B/span), the tag-name catalog (48 rows) and the
  tag-value catalog (304,070 rows, 0.846 B/span — one row per distinct
  `(scope, key, value, type)`, which `docs/api.md` §4.3 requires to exist and to
  be time-less).

## 5. The data, stated as parameters

Corpus **g1** — `measure/gen_corpus.py`, seed 20260922, which emits both the
OTLP/JSON request bodies and the storage rows from one seeded stream, so both
stores receive the same spans.

| parameter | g1 | what it stands for |
|---|---:|---|
| spans | 2,000,064 | three hours of one shop-like system |
| traces | 70,413 | 28.4 spans per trace on average |
| spans in the largest trace | 1,000 | 60% of traces are 3–10 spans, 1% are 200–1,000 |
| services | 24, 4 pods each | 68 distinct resources |
| span attributes per span | 5.45 | HTTP, gRPC, database, messaging and business keys |
| resource attributes per span | 15 | including `k8s.pod.name`, `cloud.availability_zone` |
| distinct values of the widest key | 50,000 (`app.user.id`) | a high-cardinality attribute |
| attribute types present | string, int, double, bool, string array | every OTLP scalar kind; bytes and key-value lists are covered by `sql-schema.md` §3.3 |
| events | 70,716 | exception events carry a 25-frame stack trace |
| links | 1,963 | a consumer span linking a producer in another trace |
| error spans | 13,436 (0.67%) | `status = error` |
| clock skew | ±20 ms per service instance | children may start before their parent |
| retried pushes | 40 bodies, 19,669 spans (`dup_bodies`, `dup_spans` in `results/corpus-summary.json`) | the same bytes sent twice. A store that keeps both copies ends up with 2,019,733 spans where this design has 2,000,064; `measure/run_all.sh` requires the reference to report exactly that number before it measures anything, so a reference that still holds an earlier run's spans stops the run instead of being compared |
| OTLP bytes | 1,239,967,396 | 4,073 request bodies |

Two shapes the corpus contains on purpose, because they break naive designs: a
1,000-span trace, and traces whose spans arrive in several bodies, one per
service, minutes apart — so a trace is never whole in one insert.

## 6. The queries, and the answers they must give

### 6.1 The worked fixture — literal data, literal answers

`measure/fixture/make_fixture.py` writes three traces, nine spans, six request
bodies, one of which is **sent twice**.

```
trace 1 (…1111)                                   trace 2 (…2222)   trace 3 (…3333)
  …0001 frontend  GET /cart      SERVER  error      …0007 frontend    …0008 accounting  CONSUMER  2 s
    http.request.method="GET"  (string)               GET /health       link -> trace 1, span …0005
    http.response.status_code=500  (int)               status 200       …0009 accounting  SELECT ledger
    app.user.id="u-1"          app.cache.hit=false
    …0002 frontend  checkout.Create  CLIENT
      …0003 checkout  checkout.Create  SERVER
        app.items.count=3 (int)  app.discount.ratio=0.25 (double)  app.tags=["gold","eu"]
        …0004 checkout  payment.Charge  CLIENT
          …0005 payment  payment.Charge  SERVER  error
            event "exception": exception.type="java.lang.IllegalStateException"
            …0006 payment  SELECT ledger  CLIENT
              db.system.name="postgresql"   http.response.status_code="200"  (STRING, not int)
```

Answers from both stores (`measure/fixture/fixture_answers.py`,
`results/fixture-answers.tsv`). Nineteen of the twenty query cases are
byte-identical; the two marked differ, and both are the correct answer:

| # | query | the answer (span ids) | the reference |
|---|---|---|---|
| F1 | `{}` | all nine spans, newest trace first | same |
| F2 | `{ resource.service.name = "payment" }` | `…0005, …0006` | same |
| F3 | `{ status = error }` | `…0001, …0005` | same |
| F4 | `{ span.http.response.status_code >= 500 }` | `…0001` — the int 500; **not** `…0006`, whose value is the string `"200"` | same |
| F5 | `{ span.http.response.status_code = "200" }` | `…0006` | same |
| F6 | `{ span.app.discount.ratio > 0.2 }` | `…0003` | same |
| F7 | `{ span.app.tags = "gold" }` (array membership) | `…0003` | same |
| F8 | `{ span.app.cache.hit = false }` | `…0001` | same |
| F9 | `{ duration > 1s }` | `…0008` | same |
| F10 | `{ event.exception.type = "java.lang.IllegalStateException" }` | `…0005` | same |
| F11 | `{ link:traceID = "111…1" }` | `…0008` | same |
| F12 | `{ …frontend } >> { …payment && status = error }` | `…0005` | same |
| F13 | `{ …checkout } > { …payment }` | `…0005` | same |
| F14 | `{ name = "SELECT ledger" }` | `…0006, …0009` | same |
| F15 | `{ rootServiceName = "accounting" }` | `…0008, …0009` | same |
| F16 | `{ traceDuration > 1s }` | `…0008, …0009` | same |
| F17 | `{ .app.user.id = "u-1" }` (unscoped) | `…0001` | same |
| F18 | `{ resource.k8s.pod.name = "payment-a" }` | `…0005, …0006` | same |
| F19 | `{ span.http.response.status_code != 200 }` | **eight spans**: the seven lacking the key, plus `…0006` | **`…0001` only** |
| F20 | `{ kind = consumer }` | `…0008` | same |
| F21 | `{} \| count_over_time()` over the window | **9** | **11** |
| B1 | a span whose timestamp is exactly `start` | returned (**1**) | start is inclusive |
| B2 | a span whose timestamp is exactly `end` | not returned (**0**) | end is exclusive |
| B3 | the same span, window one nanosecond wider | returned (**1**) | — |

- **F21, the retried push.** The body carrying `…0001` and `…0002` was sent
  twice; the reference stores and counts both copies, so a panel reads 11 where
  9 were sent. This design stores the span once (the repeat collapses on the
  sorting key) and every read carries `final = 1`. At corpus scale: the
  reference counts 113,201 `checkout` spans against the corpus's 112,701, and
  the 40 retried bodies hold exactly 500 `checkout` spans.
- **F19, `!=` on an attribute.** PulsusDB's rule (`docs/api.md` §4.2) is that
  `!=` also matches a span lacking the key; the reference requires the key. On
  g1: 29,253 spans carry the key with a value other than 200 and 1,708,628 lack
  it; this design answers their sum, 1,737,881, and the reference answers
  29,531 — 29,253 plus its own retried copies.

### 6.2 Answers must equal the corpus, not the other store

`measure/agreement.py` counts, for eighteen filters, what each store reports;
`measure/ground_truth.py` counts the same eighteen in the corpus file. **This
design equals the corpus on all eighteen** (`results/g1-agreement.tsv`), including
`{ checkout } > { payment }` at 8,596 and the descendant query at 178. The
reference is higher on every ordinary filter by its retried copies and lower on
the two structural counts (7,905 and 168 in this run, 7,855 and 166 in the
first); its own search returns those traces when asked for them directly, so the
cause was not established and is recorded as unexplained.

### 6.3 The benchmark

**The definition.** The same corpus into both stores from the same 4,073 bodies.
Both containers `--cpus 4 --memory 6g` on one machine. Each query runs once
unmeasured, then five times; the median is reported. The trace-fetch comparison
**interleaves** the two stores within one run and reports a distribution, because
the reference's fetch time varies between sessions (`measure/fetch_compare.py`).
**Cold** for ClickHouse drops the mark, uncompressed, query-condition and
primary-index caches and reads with direct I/O; the reference's caches cannot be
dropped equivalently, so cold numbers are reported for this design only and the
requirement is stated warm-to-warm.

**The rule.** Warm median below the reference's for every shape except searches
whose filter matches a large share of the window, where the reference returns
the first twenty traces it finds and stops while this design returns the twenty
**newest** — a stricter answer, and the one `docs/api.md` §4.2 requires.

| shape | this design, warm | cold | the reference | today |
|---|---:|---:|---:|---:|
| trace by id, 20 spans (interleaved, 21 reps, p50) | **35 ms** | 168 | 53 ms | 6 ms |
| trace by id, 1,000 spans (interleaved, 21 reps, p50) | **42 ms** | 130 | 102 ms | 17 ms |
| `{ } \| rate() by (resource.service.name)` | **38 ms** | 44 | 249 ms | 181 ms |
| `quantile_over_time(duration, …) by (span.http.route)` | **40 ms** | 56 | 190 ms | `400` |
| `{ status = error } \| count_over_time() by (…service.name)` | **17 ms** | 27 | 62 ms | 52 ms |
| `histogram_over_time(duration)` | **26 ms** | 37 | 163 ms | 125 ms |
| instant `avg_over_time(duration) by (name)` | **27 ms** | 45 | 210 ms | `400` |
| `{ } \| rate() by (resource.k8s.pod.name)` | **44 ms** | 46 | 445 ms | `400` |
| tag names, span scope (the catalog read, `c11`) | **2 ms** | 2 | 5 ms | 4 ms |
| tag values for one key (the catalog read, `c12`) | **2 ms** | 5 | 3 ms | 4 ms |
| tag values narrowed by a query (`t03`) | **27 ms** | 36 | 30 ms | 108 ms |
| `{ span.app.user.id = "u-10013" }` | **44 ms** | 67 | 139 ms | 26 ms |
| `{ rootServiceName = … && traceDuration > 2s }` | **78 ms** | 168 | 26 ms¹ | 75,369 ms |
| `\| compare({ status = error })` | **1,191 ms** | 1,428 | 1,229 ms | — |
| the service graph | **209 ms** | 261 | no endpoint | 323 ms |

¹ A search, so it falls in the exempt class: the reference stops at the first
twenty traces.

**Why the tag catalogs exist**, in the same units: answering the same two
questions by scanning the span table costs **392 ms** for the span-scope names
(`t01`) and **44 ms** for one key's values (`t02`), against 2 ms from the
catalogs. Those two statements are in the suite for that comparison, not as the
design's tag path.

**The exempt class, measured** (`results/comparison.tsv`,
`results/g1-new-design-sliced.tsv`): `{}` 62 ms whole-window, **34 ms** with the
newest-slice-first plan in 2 statements, against the reference's 9 ms; a service
search 36/31 against 10; `status_code >= 500` 47/32 against 19; `select()` 36/32
against 12. The one shape where the loop costs more than it saves is the rare
point filter: `{ span.app.user.id = "u-10013" }` is 44 ms whole-window and 116 ms
sliced over 6 statements, which is why the compiler uses the loop only when the
first slice's own match count says the filter is broad.

The reference's own answer to `{}` is not the newest twenty, and is not the same
answer twice: in three consecutive calls it returned **none** of the corpus's
newest twenty traces, and the twenty it did return ranked 52,635th to 68,876th
by recency in the first call, 16,583rd to 51,614th in the second, and
52,635th to 68,876th in the third (`measure/broad_search_check.py`,
`results/g1-broad-search-check.tsv`). That is what the exception buys: this
design returns the twenty newest, every time.

**Bytes returned**: a search 546–4,926 bytes here against 4,122–33,970 from the
reference; a 180-step metrics query 2,346–185,352 against 7,380–680,516.

### 6.4 The whole corpus, not a selection

The benchmark set below is chosen for cost. **Correctness is answered over the
repository's own corpus instead**: all 188 queries of
`crates/pulsus-traceql/tests/corpus/` are catalogued in
`docs/TraceQL/query-catalogue.md`.

| | |
|---|---:|
| the parser and the validator accept | 141 |
| of those, the API refuses at plan time (`400`) | **3** |
| the API serves | **138** |
| whose route statement's answer equals the independent check | **138** |
| whose membership answer equals the independent check | **138** |
| the parser or the validator refuses (`400`) | 47 |

The three the planner refuses are `{ .a = 1 } \| by(.b + .c)`,
`{ duration > 100 }` and `{ .a = 1 } \| { .b = 2 } && { .c = 3 }`; the storage
answers all three, and the refusals are the shipped planner's, which this design
does not change. `docs/TraceQL/query-catalogue.md` names each with its refusal
site.

**The catalogue fixture is built so that a wrong statement gives a different
answer from a right one, and that is checked rather than claimed.**
`measure/perturbations.tsv` lists one rule per row — a status code, the scope
order an unscoped read uses, the climb's depth bound, the child relation, the
spanset cap, the log2 bucket, `compare()`'s type key, the exemplar's choice,
and so on. `measure/perturb_check.py` changes each one **on one side only**, in
a scratch copy, runs the whole catalogue against it, and requires the comparison
to go red. `results/perturbations.tsv` is the run.

Two things fall out of it, and neither is visible from an agreement count:

- **the two sides are independent.** A rule changed in the renderer and the same
  rule changed in the interpreter each produce a disagreement, because the only
  thing the two share is the parser.
- **the rules the corpus cannot tell apart are named.** A row marked
  `not_discriminated` is a rule no corpus query exercises on any fixture — for
  example `topk` against `bottomk`, which the corpus asks for only where the
  fixture has one series. Those rows are listed in
  `docs/TraceQL/query-catalogue.md` with the reason, rather than counted as
  passing.

Three of the fixture's six traces exist for the same purpose: a chain
alternating the two sides of a structural operator, a sibling pair under a
parent of neither side, a descendant three links below its nearest matching
ancestor, a span whose parent is not stored, a two-span cycle, and a resource
carrying a key its own span carries with a different value. Adding the first set
moved three catalogue answers and each was a defect — the numbering behind
`nestedSetParent < 0` missed the orphan and the cycle, a pipeline stage after a
structural operator was dropped, and a sibling union re-added its own hit.
`measure/fixture/make_edge_fixture.py` carries the same shapes at corpus scale
for the generated statements, with 28 expected answers written down in
`measure/edge_checks.sh`.

### 6.5 The query set

Search: `{}`; a service; a service and `status = error`; an integer attribute
`>= 500`; `duration > 2s && kind = server`; a database attribute with a span
name; a regex on a route; a high-cardinality equality; a descendant relation; a
child relation; an event attribute; `select()` across scopes; `| count() > 5`;
two trace-level intrinsics; an unscoped attribute with a resource attribute;
arithmetic on two attributes; a field-against-field comparison; an aggregate
over a projected value; an intrinsic against an attribute; `| by()` on an
attribute; `span:childCount`; nested-set depth.
Tags: names by scope (catalog); values for one key (catalog); values narrowed by
a query; values for the `name` intrinsic (window-bounded).
Metrics: `rate() by` service; `quantile_over_time … by` a span attribute;
`count_over_time() by` service over errors; `histogram_over_time(duration)`; an
instant `avg_over_time by (name)`; `rate() by` a resource attribute; exemplars;
`topk`; `compare()`.
Fetch: a 20-span and a 1,000-span trace. Service graph: the whole window.

## 7. The API surface this design covers

From `crates/pulsus-server/src/traces_api/mod.rs` (native and compat routers) and
the reference's `pkg/api/http.go:69-86` @ grafana/tempo v3.0.2 (`0c4b926d`).

| route | what it answers | covered by |
|---|---|---|
| `GET /api/traces/v1/trace/{id}` (+ `/json`, compat `/api/traces/{id}`, `/tempo/api/traces/{id}`) | the whole trace as OTLP | `sql-schema.md` §5.4 |
| `GET /api/v2/traces/{id}` (compat) | the same, v2 envelope | the same statement |
| `GET /api/traces/v1/search` (compat `/api/search`) | TraceQL search: every accepted filter, pipeline and structural form | `sql-schema.md` §5.2–5.3, `server-implementation.md` §3.2–3.4 |
| `GET /api/traces/v1/tags`, `/api/v2/search/tags` | tag names by scope | `sql-schema.md` §5.5 |
| `GET /api/traces/v1/tag/{tag}/values`, `/api/v2/search/tag/{tag}/values` | tag values, narrowed or not, and the static vocabularies | `sql-schema.md` §5.5 |
| `GET /api/traces/v1/metrics/query_range`, `/api/traces/v1/metrics/query` (+ compat) | TraceQL metrics, with exemplars, `topk`/`bottomk` and `compare()` | `sql-schema.md` §5.6 |
| `GET /api/traces/v1/service_graph` | the PulsusDB-native service graph | `sql-schema.md` §5.7 |
| `GET /api/echo` (compat) | a constant; no storage | unchanged |
| `POST /v1/traces`; `POST /api/v2/spans`, `/tempo/spans` (compat) | ingest | `server-implementation.md` §2 |

The reference additionally serves `/api/mcp`, `/api/status/buildinfo`,
`/status/usage-stats` and a gRPC streaming search; PulsusDB does not serve the
first three from trace storage, and the streaming service is out of scope here,
as it is today.

## 8. Test cases

**75 cases**: 6 schema and storage plus 2 retention (§8.1), 8 window
(§8.2), 4 statement-count and pushdown plus 24 compiler constructs (§8.3), 9 tag
(§8.4), 7 write path (§8.5), 5 protection (§8.6), and 10 corpus-scale (§8.7), the
last two of those being the whole corpus catalogue. The count is the number of
`T-` rows in this section, and every id is distinct.

The coder writes them first and runs them against the unchanged tree, with empty
stubs where a type or function does not exist yet, so that a case which is meant
to fail fails on an assertion rather than on a compile error. **Each case is one
of two kinds**, and the kind says what that first run must show:

| kind | on the unchanged tree | why it is here |
|---|---|---|
| **new** (59 cases) | **fails**, on an assertion | it asserts behaviour this change introduces |
| **guard** (16 cases) | **passes**, and must still pass afterwards | it asserts behaviour that is staying exactly as it is, so that the change cannot move it by accident |

The 16 guards are `T-B6`, `T-A15`, `T-T1`, `T-T2`, `T-T4`, `T-T5`, `T-T7`,
`T-T8`, `T-C3`, `T-C4`, `T-C9`, `T-X1`, `T-X2`, `T-X3`, `T-X4` and `T-X5`. Every
other case is new. `T-B5` is **not** a guard even though part of it is: it
compiles five reads and asserts one window form for all of them, and the graph
and metrics windows already have that form while the search and tag-value ones
do not, so the case fails as a whole. Its row says which two are the regression
half.

**The first run settles this, not this document.** The coder pastes the run
against the unchanged tree. A `new` case that passes, or a `guard` case that
fails, is a defect in this table: the coder reports that row and the table is
corrected. No test is rewritten to make the split come out right.

Files
follow the tree's homes: schema tests in `crates/pulsus-schema/tests/live_traces_v2.rs`,
write-path tests in `crates/pulsus-write/tests/trace_rows_v2.rs`, compiler tests
in `crates/pulsus-read/tests/traces_compile_v2.rs`, live query tests in
`crates/pulsus-read/tests/traces_query_v2_live.rs`, API tests in
`crates/pulsus-server/tests/traces_api_v2_live.rs`. Every case gives one literal
input and one expected result; none of them leaves a choice to the coder.

### 8.1 Schema and storage

| case | setup (literal) | assertion | expected | on the unchanged tree |
|---|---|---|---|---|
| `T-S1` | apply the schema | the set of columns whose type holds attribute values, from `system.columns` | exactly `spans.attrs`, `spans.scope_attrs`, `spans.attrs_other`, the `attrs` inside `spans.events` and `spans.links`, `resources.attrs`, `resources.attrs_other`, `tag_values.value` | `trace_attrs_idx.val`, `trace_spans.payload` and `trace_spans.attr_val` also hold them |
| `T-S2` | ingest one span with `service.name = "checkout"` | `countIf` over `resources` for a `service%2Ename` path, and `spans.service` | 0 resource rows carry it; the span column does | the resource JSON carries it until the writer strips it |
| `T-S3` | load corpus g1 | total bytes / 2,000,064, and non-span bytes / span bytes | ≤ 45 B/span and ≤ 10% (measured 37.275 and 6.605%) | today 754.44 B/span |
| `T-S4` | load corpus g1 | `sum(data_uncompressed_bytes)/sum(data_compressed_bytes)` on `spans` | ≥ 6 (measured 6.977) | today 4.53 |
| `T-S5` | apply the schema | `system.columns.compression_codec` for every column of `spans` | none empty | the table does not exist |
| `T-S6` | apply the schema twice | the second application | no error, no column re-added | the migration set is new |
| `T-R1` | load two days, `ALTER TABLE spans DROP PARTITION` the older | elapsed, `system.mutations`, `system.merges`, remaining rows | the day is gone, 0 mutations, 0 merges, the other day intact (measured 0.068 s for 2,000,064 spans) | the table does not exist |
| `T-R2` | a span on 2106-02-06 | the rendered TTL expression | the clamped form, no overflow past 2106-02-06 | new DDL |

### 8.2 The window rule

Every case here uses one fixture, `B`, so the coder writes it once: four spans
in one trace `b0000000000000000000000000000001`, service `checkout`, in the UTC
day 2026-09-22.

| span | id | start (ns) | what it is for |
|---|---|---|---|
| b1 | `…0001` | `1790094846486853636` | the search boundary, both ends |
| b2 | `…0002` | `1790121599999999999` | `23:59:59.999999999`, the last nanosecond of the day |
| b3 | `…0003` | `1790035230000000000` | `00:00:30`, before the tag-value window opens |
| b4 | `…0004` | `1790038800000000000` | `01:00:00`, inside it |

b3 carries `span.http.route = "/only-before"`, b4 carries
`span.http.route = "/inside"`, and both carry `span.tenant = "t1"`.

A `start` or `end` whose magnitude is at least 1e12 is read as nanoseconds and
anything smaller as seconds (`crates/pulsus-server/src/traces_api/params.rs:161-171`),
which is why some requests below carry nanoseconds and some seconds.

| case | setup (literal) | assertion | expected | on the unchanged tree |
|---|---|---|---|---|
| `T-B1` | fixture B | `GET /api/search?q={}&start=1790094846486853636&end=1790094846486853637` (nanoseconds) | b1 is returned | the search window renders `ts > start`, so a span at exactly `start` is dropped |
| `T-B2` | fixture B | the same search with `start=1790094846486853635&end=1790094846486853636` | b1 is **not** returned | the search window renders `ts <= end`, so a span at exactly `end` is returned |
| `T-B3` | fixture B | search `start=1790121599999999999&end=1790121600000000000` (to the next midnight) | b2 is returned, and the statement's `WHERE` carries `day >= toDate(…1790121599999999999…) AND day <= toDate(…1790121600000000000 - 1…)`, both resolving to `2026-09-22` | the day bound is rendered from `end` rather than from the last included nanosecond, so it reads `2026-09-23` as well |
| `T-B4` | any search window | the emitted SQL text | contains `intDiv(start_ns, 300000000000) BETWEEN 5966982 AND 5966982` for the `T-B1` window — the bucket bound rendered from `start` and from `end - 1` | the compiler emits no bucket bound at all, so the sort key's leading column cannot prune |
| `T-B5` | compile one search, one trace fetch, one tag-value read, one metrics range query and one service-graph request | every time clause in the emitted SQL | all of the form `start_ns >= <s> AND start_ns < <e>` | search and the store-backed tag read use other conventions. **This case is a regression case for the graph and the metrics window**: both are already half-open (`graph_sql.rs:65-67`, `metrics_sql.rs:86-114`), so those two assertions pass before the change and must keep passing after it |
| `T-B6` | fixture B, plus a client span at `1790094846486853630` in service `gw` whose child server span is b1 | `GET /api/traces/v1/service_graph?start=1790094846486853630&end=1790094846486853636` | the `gw → checkout` edge is **absent**, because b1 starts at exactly `end` | **guard**: passes today — the graph window is already `[start, end)` (`crates/pulsus-read/src/traces/graph_sql.rs:65-67`, `WindowSql::start_closed_end_open`). It is here so that making the four conventions one does not quietly widen the graph |
| `T-B7` | fixture B | `GET /api/v2/search/tag/span.http.route/values?q={span.tenant="t1"}&start=1790038800&end=1790121600` (seconds, so the window opens at `01:00:00`) | the values are exactly `["/inside"]`: `/only-before` is **not** returned | the store-backed read is widened to every UTC day the window touches, so b3's value comes back although b3 is an hour before the window opens |
| `T-B8` | fixture B | `GET /api/metrics/query_range?q={span.tenant="t1"} \| compare({span.http.route="/inside"}, 10, 1790035230000000000, 1790038800000000000)&start=1790035200&end=1790121600&step=3600s` | b4, whose start is exactly the `end` argument, counts in the **baseline**: the series `__meta_type="baseline"` carries `span.http.route="/inside"` with 1, and the `selection` side does not | `compare()`'s selection window is right-closed, so b4 counts in the selection |

### 8.3 The compiler

Statement counts and pushdown:

| case | setup (literal) | assertion | expected | on the unchanged tree |
|---|---|---|---|---|
| `T-Q1` | every request in `measure/api_requests.tsv` — one line per statement in `measure/sql/`, with its method, path, literal query string and the count it may issue. 60 of the 65 statements name a route, and one further row is a request with no statement of its own (the unscoped tag-value read, which is §3.5's third case); the five without a route say in that file why — two are cost comparisons of the same query, and three are the numbering the query path does not run | rows in `system.query_log` whose `query_id` carries the request's prefix | the `statements` column of that file: 1 for every request but the four of `server-implementation.md` §3.5 | today 2.2–3,685.5 per request |
| `T-Q2` | `{ status = error }` over 2,000,064 spans | rows the reader receives from ClickHouse | ≤ `limit` rows | today hydrates 32 traces per batch and evaluates spans in Rust |
| `T-Q2b` | `{ resource.service.name = "checkout" && nestedSetLeft > 5 }` over the same corpus — the one shape R5 excepts | the statements issued, and the rows the reader receives | two statements (`c21`'s shape is the second), and at most the trace cap × `MAX_SPANS_PER_TRACE` span rows — 924 on this corpus — never a window's spans | the numbering is new, and doing it in SQL over the window exhausts the server |
| `T-Q3` | each benchmark shape | response bytes, and for a fetch `sum(byteSize(*))` over that trace's rows with `final = 1` | within R6 (measured: 11,699 against 11,708, and 335,128 against 589,696) | no statement to measure |

One case per accepted construct. Each names the query, the fixture and the
answer; `measure/sql/<file>.sql` is the statement it must compile to.

| case | query (on the fixture of §6.1) | expected | statement |
|---|---|---|---|
| `T-A1` | `{ span.http.response.status_code != 200 }` | 8 spans: the seven lacking the key plus `…0006` | every typed read wrapped in `coalesce(…, false)`; without it the answer is 0 rows |
| `T-A2` | `{ span.http.response.status_code >= 500 }`, `{ … = "200" }`, `{ … = 200 }` | `…0001`; `…0006`; none | `s04` |
| `T-A3` | a span with `a.b = 1` and another with a nested `a: {b: 1}`; `{ span.a.b = 1 }` | only the dotted one | path escaping |
| `T-A4` | an attribute literally named `a%2Eb` | stored path `a%252Eb`; the key read back is `a%2Eb` | the `%`-first escape |
| `T-A5` | one span carrying `k="first"` and `k="second"` | stored value `first`; the insert succeeds | ClickHouse refuses a duplicate JSON path |
| `T-A6` | attributes `+Inf`, `-Inf`, `NaN`; `{ .k > 500 }` | stored as `Float64`; `+Inf` matches | text JSON cannot parse them |
| `T-A7` | the fixture of §6.1, whose span `…0003` is the only one carrying two numeric attributes (`span.app.items.count` = int 3, `span.app.discount.ratio` = double 0.25): `{ span.app.items.count + span.app.discount.ratio > 3 }`, and the field-against-field `{ span.app.items.count > span.app.discount.ratio }` | the span ids each returns, and the statement count | each returns exactly **`…0003`** — 3 + 0.25 is 3.25, and 3 > 0.25 — and each is **one** statement. Every other span lacks one or both keys, and each typed read sits inside `coalesce(…, false)`, so it is false rather than null (measured on that fixture) | `c01`, `c02` |
| `T-A8` | `GET /api/traces/v1/search?q={ resource.service.name = "checkout" } \| by(span.rpc.method)` — a **search** pipeline, which serves attribute group keys today (`crates/pulsus-read/src/traces/search_plan.rs:2010-2020` and `:2356`) | one spanset per **distinct (value, stored type) pair**, groups in first-appearance order, no spanset for spans lacking the key, and one statement in `system.query_log` | the groups of `measure/sql/c05_by_attribute.sql` on g1, and `statements = 1`. The type is part of the group because `docs/api.md` §4.2 renders an attribute by-key in the arm the sender stored it as, so an integer `1` and a double `1.0` carry the same label and are two groups: the catalogue fixture has that pair on one trace and the statement answers `('1', 'int')` and `('1', 'double')` (`by_attribute_types` in `measure/catalogue-extra.tsv`). **Grouping inside a metrics query is a different surface and stays `400`** — that is `T-A15` |
| `T-A9` | the fifteen structural forms with `A = { resource.service.name = "checkout" }`, `B = { resource.service.name = "payment" }` | `>`: `…0005`; `!>`: `…0006`; `&>`: `…0004, …0005`; `<`: none; `!<`: `…0005, …0006`; `&<`: none; `~`: none; `!~`: `…0005, …0006`; `&~`: none; `>>`: `…0005, …0006`; `!>>`: none; `&>>`: `…0003, …0004, …0005, …0006`; `<<`: none; `!<<`: `…0005, …0006`; `&<<`: none | `st01`–`st15`; the answers are produced by `measure/fixture/structural_answers.py` and committed at `results/fixture-structural.tsv`. **This fixture does not separate the three union partner rules** — its answers are the same under a wrong rule and a right one — which is what `T-A9b` is for |
| `T-A9b` | fixture E's `ee06…`: `P → A2 → B1 → A1` beside `P → P2 → {A3, B2}`, with A = `resource.service.name = "frontend"` and B = `resource.service.name = "payment" && status = error` | the spans each union form returns | `&>` → `A2, B1`; `&<` → `A1, B1`; `&~` → `A3, B2`; and the three plain forms → `B1`, `B1`, `B2` | one partner expression for all three relations answered `A1, A2, B1`, `A1, A2, B1` and `B2` — measured, and the reason `measure/edge_checks.sh` exists |
| `T-A10` | fixture E's two chains, which sit either side of the bound: `ee03…` is 65 spans, so its leaf is exactly `PULSUS_TRACEQL_MAX_DEPTH` = 64 parent links below the root; `ee04…` is 66 spans, one link further. The root matches A, the leaf matches B; `{A} >> {B}` | which leaf matches, and the overflow row | `ee03`'s leaf **matches** with `unresolved = 0` for it; `ee04`'s leaf does **not** match and the statement's `overflow` row carries a non-zero `unresolved`, which the route turns into **`422 query_too_broad`** — never a `200` with an empty spanset | `measure/edge_checks.sh` checks `st10_descendant_plain` and its overflow count; the previous statement followed 65 links and matched `ee04` too |
| `T-A11` | fixture E's `ee05…`: two spans that are each other's parent, one matching A and one matching B; `{A} >> {B}` | termination, and the overflow row | the statement terminates, and the `overflow` row carries `unresolved > 0`. **And with nothing else in the database it still does**: `edge_checks.sh` loads the over-bound chain alone into its own database and asserts 0 match rows and `unresolved = 1`, because a count carried as a column of the matches disappears with the last match — measured, the previous statement returned no rows at all there | `measure/edge_checks.sh`, checks `deep_no_match` and `deep_overflow` |
| `T-A12` | `{ } \| rate() by (resource.service.name)` with exemplars | one exemplar per bucket per series, each naming a `(trace:id, span:id)` that exists | `c09` |
| `T-A13` | the fixture of §6.1, whose nine spans are three `frontend`, two `checkout`, two `payment` and two `accounting`: `GET /api/metrics/query_range?q={ } \| count_over_time() by (resource.service.name) \| topk(3)&start=<fixture start, seconds>&end=<start + 10>&step=60s` | which series come back, and each series' value | exactly three: `frontend` **3**, `accounting` **2**, `checkout` **2**. `payment` also has 2 and is **absent** — the three largest totals are chosen in ClickHouse and the tie at 2 is broken by the label ascending. The same query with `bottomk(3)` returns `accounting`, `checkout` and `payment`, and drops `frontend`: that pair is what tells the two orders apart, and a single-series fixture cannot (measured on that fixture). `count_over_time()` rather than `rate()` so the expected values are counts and not a per-second division by the step | `c10` |
| `T-A14` | fixture E — `measure/fixture/make_edge_fixture.py`, whose trace `ee07…` is one resource `k8s.pod.name = "pod-x"` under one selection span and two baseline spans — and the statement `measure/sql/c08_compare.sql` for `{ resource.service.name = "payment" } \| compare({ status = error })` | the selection and baseline counts of five rows, and the key set | `resource k8s.pod.name=pod-x` → **1 / 2** (a resource shared across the two sides is counted per span, not per resource: the previous statement answered 1 / 0); `intrinsic statusMessage=boom` → 1 / 0; `intrinsic kind=server` → 6 / 2, the keyword rather than the stored code; `event name=exception` → 1 / 0; `link spanId=0000000000000009` → 1 / 0. The key set carries all five attribute scopes and the eleven intrinsics of `server-implementation.md` §3.2, and does **not** carry `span:id` | `measure/edge_checks.sh` runs exactly these five and writes `results/edge-checks.tsv`; the previous statement reported five keys in total |
| `T-A15` | one request per row of `server-implementation.md` §3.2 whose **"today"** column reads `400`. On `/api/metrics/query_range`: `{} \| rate() by(span.http.route)`, `{} \| rate() by(resource.service.name, span.http.route)`, `{} \| quantile_over_time(duration, .9) by(span.http.route)`, `{} \| histogram_over_time(duration) by(span.http.route)`, `{} \| avg_over_time(span.http.request.body.size)`. On `/api/traces/v1/search`: `{ .a = 1 } \| { .b = 2 } > { .c = 3 }` and `{ .a = 1 } \| { .b = 2 } && { .c = 3 }` | the HTTP status and the response envelope | `400` and the §4 error envelope. **guard**: every one of these answers `400` today — the metrics keys at `crates/pulsus-read/src/traces/metrics_plan.rs:1102-1121` (`resolve_by_keys` admits `resource.service.name` and nothing else), the two search stages at `crates/pulsus-read/src/traces/search_plan.rs:1945-1956`, so the case must pass against the unchanged tree and keep passing. **The message text is not asserted** — this project matches status and envelope, never wording | the table marks them, and nothing must enable them by accident |
| `T-A16` | `{ span.app.cache.hit }` (truthiness) | only spans whose value **is** `true`; a `false` value does not match | `c16` |
| `T-A17` | `{ span.app.tags != nil }` (presence) | spans carrying the key, whatever the value | `c17` |
| `T-A18` | `{ span.app.tags = nil }` (absence) | spans not carrying the key | `c18` |
| `T-A19` | `{ resource.service.name = "checkout" } \| count() > 5 \| { status = error }` (a filter as a later pipeline element) | one statement: the aggregate becomes the first pass's `HAVING`, the later filter a predicate of the detail pass | `c19` |
| `T-A20` | `{ span:childCount > 3 }`, and `nestedSetLeft`/`nestedSetRight`/`nestedSetParent` on the fixture tree root→A→C, root→B | childCount from the parent aggregate; the numbering `root 1/8/-1`, `A 2/5/1`, `C 3/4/2`, `B 6/7/1`, and for a 1,000-span trace a maximum right of exactly 2,000 | `c06`, `c14`, `c15`, verified by `measure/nested_set_check.sh` |
| `T-A21` | `{ nestedSetParent < 0 }` over corpus g1, whose 70,413 traces hold no orphan and no cycle | the number of spans returned, and the statement text | 70,413 — one per trace — and the statement carries **no** recursive CTE: a root is a span with no stored parent (`c20`) | a statement that numbered the window first was killed by the server's memory ceiling after 2 m 12 s |
| `T-A22` | the same query over fixture E, which holds an orphan (`ee01…05`, whose parent id names a span that is not stored) and a two-span cycle (`ee02…01` / `ee02…02`) | which spans are returned | exactly `ee01:01, ee01:05, ee02:04, ee03:01, ee04:01, ee06:01, ee07:01` — the **orphan is returned**; a span inside a pure cycle is **not**, which is the one place the window shortcut differs from the numbering and is `docs/api.md`'s ledger row for it | a `parent_span_id = ''` test alone misses the orphan |
| `T-A23` | the numbering statement over the same cyclic trace (`measure/edge_checks.sh`, check `nested_ee02_detail`) | the `parent` of each span | the promoted cycle member carries `parent = -1` there, which is what the retained implementation does. The two answers differ only for a span inside a pure cycle, and only because numbering a whole window cannot be afforded | the numbering is new |

### 8.4 Tags

| case | setup (literal) | assertion | expected | on the unchanged tree |
|---|---|---|---|---|
| `T-T1` | a span older than the window asked for | `/tags?start&end` | the name is listed; the window is ignored | **guard**: passes today — name discovery is already time-less: `start`/`end` are parsed and ignored (`crates/pulsus-server/src/traces_api/params.rs:753-758`), and a windowed catalog would not list it |
| `T-T2` | drop the span day, keep the catalog | `/tags` | the name is still listed | **guard**: passes today — `trace_tag_catalog` is created with no partition key and no timestamp column (`crates/pulsus-schema/src/catalog.rs:394-404`, migration 18); a day-partitioned catalog would lose it |
| `T-T3` | one key with 1,000 values | `/tag/{k}/values` with no `q` | answered from `tag_values`; the plan reads no span table | a scan is 336 ms against 1 ms |
| `T-T4` | `q={resource.service.name="cart"}` | the value list and the plan | only that service's values; reads `spans` | **guard**: passes today — `q` narrowing already reads the span store: `crates/pulsus-read/src/traces/tags_sql.rs:253-270` (`span_name_values_sql`) puts the narrowing clauses in the `WHERE` over the span table, and `:282-312` (`attr_values_narrowed_sql`) narrows with `(trace_id, span_id) IN (SELECT … FROM <spans> …)`. It must survive the replacement |
| `T-T5` | `k` as int 8080 in one span and string `"8080"` in another | the two entries | two entries, `int` and `string`, same text | **guard**: passes today — the shipped catalog already carries `val_type` per value (`crates/pulsus-schema/src/catalog.rs:862-870`, migration 41) |
| `T-T6` | names in and outside the window for `/tag/name/values` | the value list | only the in-window names, on the `[start, end)` rule of §4.1 | today the read is day-widened |
| `T-T7` | `/tag/status/values` | the response and the statement count | the three keywords typed `keyword`, zero statements | **guard**: passes today — the static vocabulary reads nothing, and must keep doing so. `crates/pulsus-server/src/traces_api/tags.rs:134-157` sends every intrinsic but `name` to `TagValueSource::Vocabulary`; `:211-214` answers those from `ValuesSource::Static` without asking the engine at all, which is why the statement count is zero; `crates/pulsus-server/src/traces_api/intrinsics.rs:61-63` produces the list. **The three values are `"ok"`, `"error"` and `"unset"` at `crates/pulsus-traceql/src/ast.rs:858-864`** (`StatusValue::as_str`), gathered into `INTRINSIC_STATUS_VALUES` by the `const` block at `:952-960`, which computes the list from the enum rather than transcribing it. **`keyword` is applied at `crates/pulsus-server/src/traces_api/tags_response.rs:202`**, where a `Static` answer renders every value with `KEYWORD_TYPE` (`intrinsics.rs:34`) |
| `T-T8` | 10,001 names and 1,001 values | the responses | capped at 10,000 and 1,000, `truncated: true` | **guard**: passes today — the caps are the API's and must not move with the catalogs. `crates/pulsus-read/src/traces/exec.rs:133` is `TAG_NAMES_MAX = 10_000`, read at `:1887` as `LIMIT cap + 1` and truncated with its flag at `:1903-1905`. `:138` is `TAG_VALUES_MAX = 1_000`, read the same way at `:1948`, `:1970` and `:2003`, and truncated in **both** value paths: `:2018-2019` for the span-name values and **`:2046-2047` for the attribute values**, each setting `truncated` from the pre-truncation length |
| `T-T9` | a scope attribute `otel.scope.build = "release"` | `/tags?scope=instrumentation` and `/tag/instrumentation.otel.scope.build/values` | the name and the value are listed | the catalog write must cover the fifth scope, as `measure/schema.sql` now does |

### 8.5 The write path

| case | setup (literal) | assertion | expected | on the unchanged tree |
|---|---|---|---|---|
| `T-W1` | the fixture's six bodies, one sent twice, in one insert block | `count()` and `count() FINAL` | 9 and 9 | today stores 11 |
| `T-W2` | the same body in two separate inserts | `count() FINAL` before any merge | 9 | no dedup key today |
| `T-W3` | one span id with kinds 2 and 3 (a shared span) | rows after `FINAL` | 2 | `kind` must be in the sorting key |
| `T-W4` | insert the corpus into replica 1, `SYSTEM SYNC REPLICA` on replica 2 | replica 2's `part_log` fetched bytes / 2,000,064, against the active part bytes per span | within 5% — measured **34.965** fetched against **34.923** stored, 1.0012×. The excess is part metadata and checksums, not a second copy of a column; a ratio near 2 would be one | — |
| `T-W5` | 1,000 spans of one resource in one day | rows in `resources` | 1 | the writer cache is new |
| `T-W6` | break the per-trace view, then insert | the insert | fails; no span stored without its index row | matches today's behaviour and must be kept |
| `T-W7` | a span carrying every field, an event, a link, and all five value types | fetch it back and compare as an OTLP value | equal; attribute order is not significant | today compares against the payload blob |

### 8.6 Protections

Each names the literal breach and the configured bound it breaches.

| case | setup (literal) | assertion | expected | on the unchanged tree |
|---|---|---|---|---|
| `T-X1` | `POST /v1/traces` carrying `N` spans, each with one string attribute whose value is 1,048,576 ASCII bytes, for `N` = 248, 249, … 264. The ceiling is `MAX_EXPANDED_BYTES` = 4 × `MAX_DECOMPRESSED_BYTES` = 268,435,456 bytes and the test is `>` (`crates/pulsus-write/src/protocols/otlp_traces.rs:135` and `:948`), and the charge is the attribute's **wire** length plus the per-span row overhead — so the flip is near 256 spans but not at a number this document can compute, which is why the case sweeps | for each `N`: the status, and the row count in `spans` afterwards | there is exactly one `N*` in the sweep with `200` at `N* - 1` and `400` at `N*`; every `N` above `N*` also gives `400`, with `google.rpc.Status` code 3 and no rows stored; every `N` below gives `200`. The test records `N*` | **guard**: passes today — the ceiling and the sweep's monotone flip are today's behaviour (`crates/pulsus-write/src/protocols/otlp_traces.rs:135`, `MAX_EXPANDED_BYTES`, and the `>` test in `charge_budget` at `:946-950`). It is here because the charge has to keep covering a row shape that changes, and `N*` is recorded on both runs; `N*` itself may move |
| `T-X2` | one attribute whose `AnyValue` nests 33 levels, against `MAX_ANYVALUE_DEPTH = 32` (`crates/pulsus-write/src/protocols/otlp_depth.rs:47`) | status | `400`, as today | **guard**: passes today — the depth limit is the decoder's: `crates/pulsus-write/src/protocols/otlp_depth.rs:47` is `MAX_ANYVALUE_DEPTH = 32` and `:132-139` is the check that returns the oversize error the moment a child would pass it. The new JSON path writer must not reach past it |
| `T-X3` | `PULSUS_TRACEQL_SCAN_BUDGET_ROWS = 1000`, then `{}` over the 2,000,064-span corpus | status and body | `422 query_too_broad` | **guard**: passes today — the budget is `max_rows_to_read` with `read_overflow_mode = throw` (`crates/pulsus-read/src/traces/exec.rs:386-389` and `:2984-2986`), and code 158 maps to `422` (`:746-750`); it must stay attached when the statements are replaced |
| `T-X4` | `PULSUS_TRACEQL_READ_MAX_MEMORY_BYTES = 1048576`, then the **served** metrics query `{ } \| rate() by (resource.service.name)` over the fixture of §6.1; then the same query at the shipped default | the status and body of each | `422 query_too_broad` naming `reader.traceql_read_max_memory_bytes` at the low ceiling, and `200` at the default. Measured: at a 1 MiB ceiling a single-part scan of a 34-row table already asks ClickHouse for 1.17 MiB and raises code 241, so the breach does not depend on how large the corpus is | **guard**: passes today — `metrics_settings` is built on `search_settings`, which carries `max_memory_usage` (`crates/pulsus-read/src/traces/exec.rs:3002` and `:3110-3119`), and `map_trace_metrics_error` hands code 241 to `map_trace_read_error` (`crates/pulsus-read/src/traces/exec.rs:702-713` and `:733-745`), which is `TraceReadMemory` → `422`. **The query has to be one the planner serves**: this case used to name `by (resource.k8s.pod.name)`, which is a plan-time `400` (`metrics_plan.rs:1102-1121`), so no ceiling could ever be reached and the case could not pass |
| `T-X5` | one span at `2106-02-07T00:00:00Z` and one at `2106-02-06T23:59:59Z` | ingest result for each | the first rejected, the second stored | **guard**: passes today — the admitted domain is the writer's (`crates/pulsus-write/src/protocols/otlp_traces.rs:484-495`, the last UTC day fully inside the storage range), and the new partition and TTL depend on it |

### 8.7 Correctness at corpus scale

| case | setup (literal) | assertion | expected | on the unchanged tree |
|---|---|---|---|---|
| `T-C1` | corpus g1 | each of the eighteen filters of §6.2 against `ground_truth.py` | equal on all eighteen | the compiler is new |
| `T-C2` | the fixture | the trace and span ids for **F1 through F21 and B1 through B3** — all twenty-four rows of §6.1 | §6.1's table | same |
| `T-C3` | a trace with five matching spans, `spss=3` | `matched` and the spanset length | 5 and 3 | **guard**: passes today — the `spss` contract is `docs/api.md` §4.2's: `crates/pulsus-read/src/traces/search_eval.rs:3212` caps each spanset's spans at `spss`, and `:3222-3226` reports `matched` from the uncapped `set.spans.len()`. It must survive |
| `T-C4` | two traces whose newest matched spans share a timestamp | the order | newest first, `trace_id` ascending | **guard**: passes today — the ordering contract is unchanged: `impl Ord for HeapEntry` at `crates/pulsus-read/src/traces/exec.rs:838-847` compares the sort key reversed and breaks a tie on ascending trace id |
| `T-C5` | one trace `c5000000000000000000000000000001`: a root span in service `loadgen`, name `GET /cart`, at `1790035200000000000` lasting 1 s, and a child in service `cart` at `1790038800000000000` lasting 2 s — 1 h 0 m 2 s of trace extent. The request window is `start=1790038800&end=1790042400` (seconds), which contains the child and not the root | `trace:duration` and `trace:rootService` on the returned trace, and the query `{ trace:rootService = "loadgen" && trace:duration > 1h }` | `3602000000000` ns and `loadgen`; the query returns the trace | both values come from the per-trace table, which aggregates the whole trace rather than the part inside the window; a window-only computation gives 2 s and `cart` |
| `T-C6` | one ordinary pair (client `c1` svc-a → server `s1` svc-b, parent `c1`) and one **shared** pair (client `c2` svc-a and a server span with the **same id** `c2`, svc-c, carrying `zipkin.shared`) | the service-graph edges | two rpc edges, `svc-a → svc-b` and `svc-a → svc-c`, one call each | the single-branch join misses the shared pair; measured by `measure/shared_span_edges.sh` |
| `T-C7` | one rpc pair and one messaging pair between the same two services | the edges | two edges differing only in `connectionType` | today's statement grouped them together |
| `T-P1` | both stores, same corpus, interleaved | warm medians per shape | below the reference for every shape outside §6.3's exempt class | this design does not exist yet |
| `T-C8` | every query in `crates/pulsus-traceql/tests/corpus/accept/` and `grafana/` that the API serves (138 of 141; the other three are `T-C9`'s), **and the 12 of `measure/catalogue-extra.tsv`** — the queries no corpus query's rule reaches, among them the attribute group key whose statement the database rejected | for each: the statement the route issues runs and its rows equal the independent interpreter's, and the membership statement does too | **138 of 138 and 12 of 12** on both comparisons, as `docs/TraceQL/query-catalogue.md` records, which counts the rows of that file rather than repeating a number | the compiler is new; `measure/catalogue.py` is the runner |
| `T-C9` | every query the API refuses: the 47 of `reject/`, `unsupported/` and `validate_reject/`, and the three of `accept/` the planner refuses — `{ .a = 1 } \| by(.b + .c)`, `{ duration > 100 }` and `{ .a = 1 } \| { .b = 2 } && { .c = 3 }` | the status and body | `400` and `docs/api.md` §4's envelope for all 50, with the message the corpus's golden pins for the 47 | **guard**: passes today — the parser, the validator and the planner do not change, and all 50 refusals must survive. The three plan-time sites are `crates/pulsus-read/src/traces/search_plan.rs:1945-1956` (a stage that is not one filter), `:2011-2017` (a group key that is not a field) and `crates/pulsus-read/src/traces/filter.rs:1710-1715` (`duration` against a bare number); `measure/planner_dispositions.tsv` is the shipped planner's own answer for every one of them |

## 9. What stays exactly as it is

The parser, the API handlers and their response shapes, the error envelope, the
comparison tests against the running reference and the TraceQL conformance
corpora define correctness and are not part of this change. The semantics that
belong to the compiler rather than to storage are carried over unchanged:
attribute scope precedence, `!=` matching a missing key, the `spss` selection
rule, the ordering contract, the tag contract of `docs/api.md` §4.3, and the
response envelopes. The semantics that do change are these, and §4.1 is the inventory for the first:

- **The window bound**, by the owner's decision of 2026-09-22: `start <= ts <
  end` wherever a window selects spans. Three windows move — the search window
  (`WindowSql::start_open_end_closed` at `crates/pulsus-read/src/traces/search_sql.rs:124`
  is replaced by the half-open constructor the metrics routes already use), the
  store-backed tag-value reads, and `compare()`'s `start`/`end` arguments at
  `metrics_sql.rs:1365`. `docs/api.md` §4.2, §4.3 and §4.4 state those bounds
  and are updated with them. The metrics evaluation window and both halves of
  the service graph are already half-open and are not touched.
  **There is no ledger row to correct for the search window**: searching all 72
  rows of `docs/benchmarks/traces-differential-ledger.md` for the bound finds
  none, so the difference was never recorded as a divergence — it is being
  removed before it ever was. `compare()`'s window is the other way round: the
  reference defines it as right-closed
  (`pkg/traceql/engine_metrics_compare.go:98-110` @ v3.0.2), so moving it to the
  one rule **adds** a ledger row.
- **`{ nestedSetParent < 0 }` over a window answers "no stored parent"**, so a
  span inside a pure cycle is not returned, while the numbering — computed over
  candidate traces — promotes one member of each cyclic component to a root. The
  alternative is to number the window, measured at 2 m 12 s and a dead 6 GB
  server against 437 ms. An orphan is a root on both paths. A ledger row and a
  `docs/api.md` §4.2 entry, with `sql-schema.md` §5.9 for the measurement.
- **`compare()` omits `span:id`** from its key universe, which the reference
  includes. A span id is unique per span, so grouping by it can only return
  `topN` arbitrary spans; the reference's own reason for the six intrinsics it
  skips — "the cardinality isn't useful" — applies to it exactly. A ledger row
  and a `docs/api.md` §4.4 entry, with `sql-schema.md` §5.6 for the reasoning.

## 10. Open questions

None. The object-storage requirements were withdrawn (§2) and the window bound
was decided (§2, R9).
