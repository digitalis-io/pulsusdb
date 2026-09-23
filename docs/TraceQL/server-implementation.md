# TraceQL in the PulsusDB server: write path, compiler, and what changes

Third of three, with `docs/TraceQL/functional-requirements.md` (requirements,
data, benchmark, test cases) and `docs/TraceQL/sql-schema.md` (tables, SQL,
measurements). This one says what PulsusDB does, what ClickHouse does, which
accepted query construct compiles to what, and which files change.

---

## 1. What runs where

```
   OTLP / Zipkin                 PulsusDB                         ClickHouse
   -------------                 --------                         ----------
   POST /v1/traces  -->  decode, validate, bound            +--> spans      one row per span
                         resource -> 128-bit id, cached     |    resources  one row per resource per day
                         attributes -> typed JSON paths     |    traces     a view fills it
                         catalog rows behind a cache -------+    tag_names, tag_values

   GET /api/traces/v1/...
                    -->  parse (unchanged)
                         compile to ONE statement --------->     filter, group, top-K, aggregate,
                         shape the JSON  <-----------------      quantiles, structural evaluation,
                                                                 exemplars, per-trace evaluation
```

PulsusDB evaluates nothing per span on the read path. Storage is single-tier on
local disks: the object-storage requirements were withdrawn by the owner on
2026-09-22, so there is no tiering component, no cold owner and no cutover.

## 2. The write path

### 2.1 One span, one row

| OTLP | stored |
|---|---|
| `trace_id`, `span_id`, `parent_span_id` | `FixedString(16)`/`FixedString(8)`, raw bytes |
| start and end | `start_ns`, `duration_ns = end − start` |
| `name`, `kind`, `status.code`, `status.message` | columns |
| `trace_state`, `flags`, the three dropped counts | columns |
| span attributes | `attrs`, one typed JSON path per key |
| events, links | arrays of tuples, each with its own `attrs` |
| scope name, version, attributes | `scope_name`, `scope_version`, `scope_attrs` |
| resource attributes | a 128-bit `resource_id`, plus one row in `resources` |
| the service name | the `spans.service` column **only** — it is removed from the resource's attributes and put back by the reader |

There is no payload blob: a fetch rebuilds the OTLP message from the columns.
That is where today's 58.12 B/span of payload goes.

**Fidelity.** Everything OTLP carries is stored. Two things are not preserved
byte for byte, and neither is part of the OTLP data model: **attribute order**
(attributes are a map; the JSON column returns keys sorted, as the reference's
own dedicated columns do) and a **duplicate key inside one span** (the first
value is kept, which is the rule `docs/api.md` §4.2 already states, and which
ClickHouse requires — two identical paths in one JSON value are refused).
`T-W7` compares a round trip as an OTLP value.

### 2.2 Resources, catalogs and their caches

`resource_id` is `sipHash128` over the resource's encoded attributes and schema
url — 128 bits, the width issue #498 settled on for stream and series identity,
for the same reason. The writer keeps a small map of `(resource_id, day)` and
`(scope, key[, value, type])` it has already written, so a resource row is
written once per day and a catalog row once per process lifetime, the way
metric metadata already works. On g1: 68 resource rows, 48 name rows, 304,070
value rows, against 2,000,064 spans.

### 2.3 Attributes: paths, types, and the binary encoding

- **Path**: `%` → `%25`, then `.` → `%2E`, applied by the writer and by the
  compiler, so `a.b` and a literal `a%2Eb` stay distinct (`sql-schema.md` §3.1).
- **Type**: the OTLP `AnyValue` kind picks the stored variant, so TraceQL's type
  rules fall out of the storage instead of being re-implemented above it.
- **Encoding**: RowBinary's **binary** JSON form — a count, then
  `(path, type tag, value)` — not text. Text cannot carry `NaN` or `±Inf`, the
  binary form skips server-side parsing, and a stored `+Inf` answers `> 500`
  with 1.
- **What JSON cannot hold** — a bytes value, an empty object, an array with no
  JSON rendering — goes to `attrs_other`, a string holding the OTLP `AnyValue`
  for those keys only. Each value is still stored exactly once.

### 2.4 A retried push is stored once

Three layers: the writer's suppression window (`PULSUS_INGEST_DEDUP`, issue
#494, extended to traces — a change of scope, not of mechanism); the
`ReplacingMergeTree` key ending in `span_id, kind`, which collapses a repeat in
the same block immediately and a later one at the next merge; and `final = 1` on
every read, which makes the answer exact in between. Today's trace path has none
of the three: the same 40 retried bodies stored 19,669 extra span rows.

### 2.5 The view

`traces_mv` aggregates each insert block into one row per `(day, trace)`. It
performs no join and no lookup, so it adds no read to the write path. A failing
view fails the insert, as today's derived tables do, so a span is never stored
without its index row (`T-W6`).

### 2.6 What the write path costs

| | today | this design |
|---|---:|---:|
| rows written per span | 22.64 | 1.187 |
| bytes on disk per span | 754.44 | 37.275 |
| bytes per span on the insert hop (LZ4, measured on the same rows) | — | 57.58 |
| bytes per span fetched by each further replica (measured) | 754 | **34.965** |

## 3. The compiler

### 3.1 The window

One rule, `start <= ts < end`, on every read path (owner decision, 2026-09-22).
The row bound, the bucket bound and the day-partition bound are rendered from
one value — the last nanosecond the window includes — in one place, the existing
`window_sql` module, which exists because rendering them separately loses rows
silently. The bucket bound is required: ClickHouse does not derive it, and
without it a windowed read touches every granule (980/980 against 26/980).

### 3.2 Every accepted construct, and the SQL it compiles to

The vocabulary is `crates/pulsus-traceql/src/ast.rs` — 21 intrinsics, 5
structural operators × 3 modifiers, 8 pipeline stages, 8 metric functions, 2
second-stage operators, 5 aggregate operators — and `docs/api.md` §4.2–§4.4.
"Statement" names a file in `measure/sql/` that runs and is timed. Every rule in
this table is also applied to the whole corpus in
`docs/TraceQL/query-catalogue.md`: of the 141 queries the parser and the
validator accept, the API serves **138** and refuses **3** at plan time, and
each of the 138 has its own two statements — the one the route issues and the
membership query — both run, both answers checked against an independent
interpreter. The corpus belongs to the parser and covers the grammar, so **12 rules
in this table have no corpus query that reaches them**; those 12 queries
are `measure/catalogue-extra.tsv`, each named with the rule it decides, and they
are run and checked the same way. **4 of the 12 are the row below on
`| by(<field>)`**: no corpus query groups by an attribute, and the statement that
shape produced did not run at all.

The three refusals are `by_expression_key`, `duration_unitless` and
`pipeline_spanset_operation`; the rows below that read `400` are why, and
`measure/planner_dispositions.tsv` is the shipped planner's own answer for all
141, captured by the probe `measure/README.md` prints. The **today**
column says what the shipped API does with the construct, so nothing is enabled
or lost by accident: `served` must keep working, `400` must keep refusing.

| construct | today | compiles to | statement |
|---|---|---|---|
| `span.k = "s"`, `!=`, `=~`, `!~` | served | the key's typed subcolumn, each typed read inside `coalesce(…, false)`; regex anchored `^(?:…)$` | `s07`, fixture F5/F19 |
| `span.k <op> <number>` | served | the `Int64` **and** `Float64` variants, both coalesced | `s04` |
| `span.k = true` / `false` | served | the `Bool` variant | fixture F8 |
| `span.k = "v"` on an array value | served | ``has(attrs.`k`.:`Array(Nullable(String))`, 'v')`` | fixture F7 |
| `{ span.k }` — a bare field, **truthiness** | served | ``coalesce(attrs.`k`.:Bool = true, false)`` — the value must *be* true, which is not presence | `c16` |
| `{ span.k != nil }` — presence | served | ``dynamicType(attrs.`k`) != 'None'`` | `c17` |
| `{ span.k = nil }` — absence | served | ``dynamicType(attrs.`k`) = 'None'`` | `c18` |
| `resource.k <op> v` | served | `resource_id IN (SELECT resource_id FROM resources WHERE …)` | fixture F18, `t03` |
| `event.k`, `link.k` | served | ``arrayExists`` over ``events.attrs.`k``` / ``links.attrs.`k``` | `s11`, fixture F10/F11 |
| `instrumentation.k` | served | ``scope_attrs.`k``` | same rule as `span.k`; the fifth catalog scope is `T-T9` |
| unscoped `.k` | served | a `multiIf` chain, span → resource → event → link → instrumentation | `s15`, fixture F17 |
| `name`, `kind`, `status`, `statusMessage`, `duration` | served | the column of that name | `s05`, `s06`, fixture F3/F9/F20 |
| `span:id`, `span:parentID` | served | `span_id` / `parent_span_id` | column comparison on the raw bytes |
| `trace:id` | served | `trace_id`, the sort key's second column | key read |
| `trace:duration`, `trace:rootName`, `trace:rootService` | served | the per-trace table, joined on `trace_id` | `s14`, fixture F15/F16 |
| `span:childCount` | served | a per-`(trace_id, parent_span_id)` count joined back to the span | `c06` |
| `nestedSetParent < 0` — a root | served | **no numbering**: a root of the hydrated forest is a span whose parent is not stored, which is one anti-join over the window (`sql-schema.md` §5.9) | `c20`, 437 ms over 2,000,064 spans |
| `nestedSetLeft > 0`, `nestedSetRight >= 1` | served | **no numbering**: the numbering starts at 1, so every stored span satisfies them | the ordinary search statement |
| any other `nestedSetLeft` / `nestedSetRight` / `nestedSetParent` comparison | served | two statements: the search without the condition, then the candidate traces hydrated whole (`c21`, 35 ms for 20 traces), and the reader numbers those spans with the retained Euler tour. Numbering a window in SQL is not possible at corpus scale — measured, it exhausted a 6 GB server after 2 m 12 s | `c21`, §3.5's second case; the rule itself is checked by `c14`, `c15`, `measure/nested_set_check.sh` and the catalogue |
| `instrumentation:name`, `instrumentation:version` | served | the `scope_name` / `scope_version` columns, which the span row carries and the tag catalog records under the `instrumentation` scope | `t01`, `T-T9` |
| `event:name`, `event:timeSinceStart` | served | the events array; `timeSinceStart` is `event.time_ns − start_ns` | array predicate |
| `link:spanID`, `link:traceID` | served | the links array | fixture F11 |
| arithmetic `+ - * / % ^`, unary `-` | served | SQL arithmetic over the typed reads, at the parser's precedence | `c01` |
| a comparison whose right side is a field | served | both sides typed reads in one predicate | `c02`, `c04` |
| `&&`, `\|\|`, `!`, parentheses | served | SQL boolean over the same predicates | every search statement |
| static keywords (`true`, `false`, `ok`/`error`/`unset`, the six kinds, `minInt`, `maxInt`) | served | literals | fixture F3/F20 |
| `{A} > {B}`, `< `, `~`, each plain, `!` and `&` | served | one grouped pass over the spans matching either side; the relation is a set test inside the group | `st01`–`st09` |
| `{A} >> {B}`, `<<`, each plain, `!` and `&` | served | the bounded recursive climb of `sql-schema.md` §5.8. The climb follows at most `PULSUS_TRACEQL_MAX_DEPTH` parent links, and the count of what it could not resolve is its **own row** of the result, so the reader sees it even when nothing matched — which is the case it exists for | `st10`–`st15`, `s09`, `measure/edge_checks.sh` |
| `\| count()`, `sum`, `avg`, `min`, `max` as a spanset filter | served | `HAVING` on the first pass | `s13`, `c03` |
| `\| select(...)` | served | extra projected slots in the spanset tuple | `s12` |
| a `{...}` filter as a **later** pipeline element | served | one statement: the earlier aggregate is the first pass's `HAVING`, the later filter a predicate of the detail pass | `c19` |
| `\| by(<field>)` in a **search** pipeline — an intrinsic or an attribute | served | a second grouping key, **rendered as text beside its stored type**: an attribute read is a `Dynamic` value, which ClickHouse refuses as a `GROUP BY` key (code 44), and the label alone would merge an integer `1` with a double `1.0`, which `docs/api.md` §4.2 renders in different arms. An intrinsic key and `resource.service.name` carry no type column — the type follows from the query. One spanset per group, in **first-appearance order**, and no spanset for a span lacking the key (`sql-schema.md` §5.2). Attribute keys included: `search_plan.rs:2010-2020` plans any `Field`, and `plan_group_key` has an `Field::Attribute` arm at `:2356` | `c05`, and `by_span_attribute` / `by_attribute_types` / `by_missing_attribute` / `by_name_order` in `measure/catalogue-extra.tsv` |
| `\| by(<expression>)` in a search pipeline — `by(.b + .c)` | **`400`** | not compiled. A group key must resolve to one value per span, so it must be an attribute or an intrinsic; the refusal is `crates/pulsus-read/src/traces/search_plan.rs:2011-2017` | the corpus's `by_expression_key` |
| `\| coalesce()` | served | drops the group key `by()` added, re-grouping to one spanset per trace — a shape change on the same statement | the `c05` statement without its group key |
| `rate`, `count_over_time`, `sum/min/max/avg_over_time`, `quantile_over_time`, `histogram_over_time` | served | one time-bucketed `GROUP BY`, one row per series | `m01`–`m05` |
| exemplars (default on; hint, then parameter, then 100; ceiling 100) | served | `argMax` inside the same pass, one per bucket per series, carrying `(trace:id, span:id, value)` | `c09` |
| `\| topk(n)`, `\| bottomk(n)` | served | the finished series ordered by their total, inside the same statement | `c10` |
| `\| compare({...}[, topN[, start, end]])` | served | one pass producing per-`(scope, key, value, type)` counts for each side, **counted per span**, over span, resource, instrumentation-scope, event and link attributes and the intrinsics the reference reports (`name`, `kind`, `status`, `statusMessage`, `instrumentation:name`, `instrumentation:version`, `trace:rootName`, `trace:rootService`, `event:name`, `link:traceId`, `link:spanId`), with `kind` and `status` rendered as the keywords the API returns. `topN` (default 10) is applied **in the statement**, per key and per side, before any cap. Two things stay with the response layer, and neither can lose a stored count: the fixed 25-key well-known set `docs/api.md` §4.4 requires, for keys **absent** from the data, which emit `key=nil`; and the `*_total` denominators. `span:id` is deliberately omitted — see the ledger row below | `c08`, and the five literal counts of `T-A14` |
| a trailing metrics-result comparison (`… > 5`) | served | a `HAVING` on the series' samples | the metric's own statement |
| `with(...)` hints | served | parsed as today; `exemplars` sets the budget, `sample` returns the exact superset, no other hint changes a read | — |
| a `duration` intrinsic compared with a bare number — `{ duration > 100 }` | **`400`** | not compiled. `duration` takes a duration literal; the refusal is `crates/pulsus-read/src/traces/filter.rs:1710-1715`. The parser accepts the query, so it sits under `accept/` in the corpus and the route answers `400` all the same | the corpus's `duration_unitless` |
| a structural or cross-spanset operation used as a `\|` **stage** — `{ .a = 1 } \| { .b = 2 } > { .c = 3 }` | **`400`** | not compiled. The stage must be one `{ … }` filter; the refusal is `crates/pulsus-read/src/traces/search_plan.rs:1945-1956` and ledger row `traceql-midpipeline-spanset-operation-unsupported`. A single filter as a later stage IS served, one row above | `T-A15` |
| `by(<key>)` in a **metrics** query, for any key but `resource.service.name`; more than one key; grouped quantile and histogram; a non-duration aggregation target | **`400`** | the storage answers them — `rate() by(resource.k8s.pod.name)` measured at 55 ms, `quantile_over_time(…) by(span.http.route)` at 42 ms — but enabling them is a behaviour change with its own issue. The refusal is `crates/pulsus-read/src/traces/metrics_plan.rs:1102-1121`, which admits exactly one key and only that one | `m02`, `m05`, `m06` show the statements; `T-A15` pins that the route still answers `400` |

### 3.3 A search

The three reads of `sql-schema.md` §5.2, with the top-K as a **scalar** subquery
so it is computed once: written as an ordinary CTE it ran twice, 4,098,432 rows
against 2,098,370.

### 3.4 Trace by id, tags, metrics, service graph

`sql-schema.md` §5.4–§5.7. The tag routes keep `docs/api.md` §4.3 exactly: names
are time-less from `tag_names`, an unnarrowed value lookup reads `tag_values`, a
narrowed one reads the store, `name` values are window-bounded, and the other
intrinsics answer from the static vocabulary without reading anything.

### 3.5 Where one statement is not possible

| case | statements | why |
|---|---:|---|
| a broad search whose filter matches much of the window | 2, up to `⌈log₂(window / 5 min)⌉ + 1` | the newest-slice-first plan: each statement is the whole search over its slice, and the loop stops when 20 traces are in hand. Measured: 95,397 rows against 2,000,064, and 34 ms against 62. The compiler uses it only when the first slice's own match count says the filter is broad — for a rare filter it costs 6 statements and 116 ms against 1 and 44 ms |
| a trace fetch for a trace not yet in the per-trace table | 2 | the fallback reads the span table over the request's window instead of the trace's extent; it happens only inside the view's flush interval |
| a nested-set comparison other than `nestedSetParent < 0` or `nestedSetLeft`/`nestedSetRight` against 0 | 2 | the first statement is the search without that condition, the second hydrates the candidate traces whole so the reader can number them. Numbering in SQL re-reads the span set once per recursion step, which over the 3-hour corpus exhausted a 6 GB server after 2 m 12 s; over the candidate traces the hydration is 35 ms and the numbering is the Euler tour the reader already carries |
| a tag-value request whose `q` does not narrow to one scope | 2 | one statement per scope read, merged by the reader, because the scopes live in different columns |

Everything else — every search shape in the benchmark, every metrics shape, both
tag catalogs, the fetch, the service graph, every construct in §3.2 — is one
statement. `T-Q1` counts them from `system.query_log` and fails if any request
exceeds the number in this table.

## 4. Budgets and protections

Unchanged in kind, re-pointed at the new statements: the per-query scan-row and
result-byte budgets, `max_bytes_before_external_group_by` on the grouping
passes, the `IN`-set bounds on the key sets, the 256 MiB request expansion
bound, the attribute nesting depth limit, and the admitted timestamp domain
(1970-01-01 … 2106-02-06, which the daily partition and the TTL both depend on).
A breach is the same `422 query_too_broad`, or the same ingest `400`, as today.

Each protection's literal bound, so `T-X1`–`T-X5` can be written without a
decision: the request expansion ceiling is a fixed **256 MiB** (268,435,456
bytes, `400` with `google.rpc.Status` code 3); attribute nesting is
**`MAX_ANYVALUE_DEPTH = 32`** (`crates/pulsus-write/src/protocols/otlp_depth.rs:47`,
`400`); the scan budget is `PULSUS_TRACEQL_SCAN_BUDGET_ROWS` (default
50,000,000, `422 query_too_broad`); the read memory ceiling is
`PULSUS_TRACEQL_READ_MAX_MEMORY_BYTES` (default 8,589,934,592, `422`); and the
admitted timestamp domain is 1970-01-01 … 2106-02-06 (per-span rejection).

The new shapes add one bound of their own: **`PULSUS_TRACEQL_MAX_DEPTH`**,
default 64, bounds the recursive climb: it follows at most that many parent
links, and the statement returns an `overflow` row carrying `unresolved` rather
than truncating in silence, so a chain deeper than the bound and a cycle both
answer `422` — including when the result holds no matches at all, which is when
a count carried as a column of the matches would be lost (`sql-schema.md` §5.8,
`measure/recursion_bound.sh`, `measure/edge_checks.sh`, `T-A10`/`T-A11`). The key set built from the per-trace table is bounded by the
candidate cap, as the slice loop is bounded by its statement count.

## 5. Cross-zone traffic

Per span, at replication factor 2, one shard:

```
   client --620 B--> PulsusDB --57.6 B--> ClickHouse --34.97 B--> the other replica
   (OTLP, g1)                 (LZ4 RowBinary)          (the compressed part, measured)
                                                       merges are local: 0
```

- 620 B/span is g1's OTLP/JSON on the wire (1,239,967,396 bytes for 2,000,064
  spans); protobuf is smaller.
- 57.552 B/span is the same rows under LZ4 (`l_lz4` in `measure/layouts.sql`);
  the client compresses inserts with LZ4 by default
  (`vendor/clickhouse/src/compression/mod.rs:41-45`).
- 34.965 B/span is what replica 2 actually fetched, read from its
  `system.part_log` (`measure/replication_bytes.sh`), against 34.923 stored —
  1.0012×, the excess being part metadata rather than a second copy of a column.
  Today's engine ships 754 B/span, because it writes 22.6 rows per span.
- On the read path what crosses is the answer: 546–4,926 bytes for a search,
  2,346–185,352 for a metrics range query, one row for a fetch. A clustered
  search returns twenty rows per shard to the coordinator, not spans.

## 6. What is kept, replaced, deleted

Every count below is read at one named revision of this repository —
`5500145d`, recorded in `measure/source-revision.txt` — and not from a working
copy. That is the state "today" means throughout these documents: six trace
tables, the corpus loaded through the server at that revision, the directories
as they stand there. `measure/claims_check.py` reads them there with `git show`
and `git ls-tree`, so the same clone gives the same number whether or not it has
pulled since.

### Kept — these define correctness and do not move

| path | lines | why |
|---|---:|---|
| `crates/pulsus-traceql/src/` (8 files) | 8,696 | the language: lexer, parser, AST, validation, durations, errors |
| `crates/pulsus-server/src/traces_api/` (18 files) | 13,223 | routes, parameters, negotiation, error envelope, response shaping |
| `crates/pulsus-traceql/tests/` (396 files) | — | conformance vectors, the accept surface, the comparison tests against the running reference, the query corpora |
| `docs/api.md` §4, §8 | — | the API contract, including the §4.3 tag contract, does not change |

`traces_api/assemble.rs` is the one kept file that changes internally: it builds
the OTLP response from columns instead of decoding stored payloads, and it puts
the service name back into the resource it renders. Its output, and every golden
that pins it, stays the same.

### Replaced

| path | lines | by what |
|---|---:|---|
| `crates/pulsus-read/src/traces/` (18 files) | 34,876 | the compiler of §3: one statement per shape. The two-phase candidate engine, the per-leaf generators and the client-side evaluator (`search_eval.rs`, 7,559 lines) go with it |
| `crates/pulsus-write/src/writer/trace.rs`, `ingest/traces.rs` | 680 | the row builder of §2: one row per span, no payload, no attribute rows, plus the resource and catalog caches |
| `crates/pulsus-write/src/protocols/otlp_traces.rs` | 2,622 | kept as a decoder; the part that renders payload blobs and attribute rows becomes the typed-path encoder |
| `crates/pulsus-write/src/writer/rows.rs` | — | `TraceSpanRow`/`TraceAttrRow` become the single new span row plus the resource and catalog rows |
| `crates/pulsus-write/src/writer/mod.rs`, `table.rs`, `config.rs`, `metrics.rs`, `backfill.rs` | — | the trace writer's table list, its two-generation flush and its per-table metrics collapse to one span table plus three small ones; the attrs backfill path disappears with the index |
| the trace half of `crates/pulsus-schema/src/catalog.rs` (38 trace migrations) and its 4 trace materialized views | — | five `CREATE TABLE`s and one view. No data migration: nothing has shipped |
| `crates/pulsus-schema/src/controller.rs` | — | the trace TTL alterations it emits now name the new tables, and the time-less catalogs are excluded from TTL |
| `crates/pulsus-schema/src/render.rs` | — | the `_dist` wrapper rendering follows the new table set and sharding keys |
| `crates/pulsus-server/src/chconfig.rs` | — | the trace table names it supplies |
| `crates/pulsus-server/src/ops.rs` | — | per-table operational metrics follow the new table set |
| `crates/pulsus-model/src/time.rs` | — | its comments describe the old trace-table timestamp needs; the admitted domain rule stays, the wording follows |

**The replaced migration set, exactly.** Every `Migration` in `MIGRATIONS` whose
`name` is a trace table, and every `MvDef` in `MVS` whose name is one — at
`5500145d`, 38 and 4:

```
  MIGRATIONS   ids 16 .. 20  and  31 .. 63        38 records, and no other id in
                                                  either run belongs to another family
                 of those, ids 18 and 41          trace_tag_catalog   family: None
                 the other 36                     family: Some(Family::Traces)
  MVS          trace_tag_catalog_mv   trace_edges_mv
               trace_recent_mv        trace_error_spans_mv
```

Do not key the set on `family: Some(Family::Traces)`. Thirty-six of the
thirty-eight carry it; `trace_tag_catalog`'s two, ids 18 and 41, carry
`family: None`, and a change that replaces only the trace family leaves those
two behind while deleting the table they manage. `measure/claims_check.py`
derives both counts by parsing the two arrays and keying on the name, which is
the set this table means.

### Deleted

`trace_attrs_idx`; `trace_tag_catalog` and its view (replaced by the two
catalogs, which keep the same contract); `trace_edges` and its view;
`trace_recent` and its view; `trace_error_spans` and its view; on the span
table, the `payload` column, the five attribute arrays, the alignment
constraint, the three projections (`service_time`, `name_time`,
`span_name_day`), the `idx_duration` skip index, and the `_dist` twins of all of
them.

That is 493.43 + 22.49 + 1.14 + 0.93 + 0.19 bytes per span of tables, plus
58.12 of payload, about 90 of arrays and 63.6 of projections inside the span
table.

## 7. Risks, and what would show this wrong

| risk | what it would look like | what to measure |
|---|---|---|
| far more distinct attribute keys than g1's 48 | the JSON column spills past `max_dynamic_paths` (1,024 per part) into shared data, and a filter on a rare key stops being a single subcolumn read | bytes read for a key filter as the key count grows; the setting is per table. The 1,024 is measured, not quoted: `measure/json_paths_default.sh` offers one part 2,000 distinct paths and reads back 1,024 kept and 976 pushed to shared data, with a column declared `JSON(max_dynamic_paths=8)` in the same result reading 8 |
| a key whose values are unique per span | `tag_values` grows linearly with spans — the same unbounded growth today's `trace_tag_catalog` has | rows in `tag_values` per span; a per-key cap is the fix, and it is future work, not claimed here |
| very wide traces (10⁴–10⁵ spans) | the recursive climb and the per-trace grouping grow with the trace | the descendant query on such a corpus; `PULSUS_TRACEQL_MAX_DEPTH` and the candidate cap are the backstop |
| `compare()` on a wide attribute space | 1,191 ms here against the reference's 1,229, reading 20,071,121 rows for 2,000,064 spans — one row per attribute per span, which is what the shape means | its cost as attributes per span grows |
| `final = 1` under a heavy insert rate | read latency rising with unmerged parts | the 41 → 68 ms measurement of `sql-schema.md` §4 is its shape; merge settings are the lever |

**What would falsify the storage claim**: a corpus where this layout stores more
than 45 bytes per span, or where the index tables exceed 10% of the span table.
The model in `sql-schema.md` §2.1 predicts 23.2 + 1.99·A + 41.5/k + 5.6·V/spans
bytes per span; a corpus with 20 span attributes per span and 10 spans per trace
should land near 70 B/span, and if it lands near 200 the model is wrong.

## 8. Order of work

`docs/TraceQL/implementation-plan.md` breaks this into independently mergeable
tasks, with the dependency graph, what still reads the old tables at each
point, and the test cases each task is judged by. The seven steps below are the
sequence it expands; where the two differ in detail, the implementation plan is
the one a coder works from, because it is the one that says how the tree keeps
working in between.

The coder writes the test cases of `functional-requirements.md` §8 first, runs
them red against the unchanged tree, and then:

1. the five `CREATE TABLE`s and the view, replacing the 38 trace migrations and
   the 4 trace materialized views named in §6 by id and by name;
2. the write path: typed paths, resource ids and caches, the binary JSON
   encoding, catalog writes, retry suppression extended to traces;
3. the trace fetch, which is the smallest read and exercises the whole row;
4. the compiler: the window renderer, then §3.2's table, top to bottom;
5. structural operators, trace-level intrinsics and the nested-set numbering;
6. the tag routes against the §4.3 contract;
7. delete the old tables, the old compiler and the old evaluator.

Steps 3 to 6 are also checked by tests that already exist: the conformance
corpora, the accept surface, and the comparison tests against the running
reference.
