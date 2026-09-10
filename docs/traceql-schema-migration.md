# Changing the trace tables

**What this is.** The design for the ClickHouse tables that hold traces: what we
store today, what is wrong with it in numbers, what we would store instead, and
what each query costs before and after.

**Who it is for.** Someone who has not read the code and will not open it. Every
term is defined where it first appears. Every structural claim points at a file
and a line in this repository. Every number says whether it was **derived** (from
those files, on paper) or **measured** (on a running ClickHouse).

**Nothing has ever shipped.** No users, no deployments, no stored data. So this is
not a data migration. It is a decision about which `CREATE TABLE` statements the
product ships with. §8 says what that means in practice.

    this tree            86081ef1eaceaad37d8ede1f5cf47b46a9ce41ec
    ClickHouse           26.3 is the live target (.github/workflows/ci.yml:567-572)
    every derived number Appendix B's calculator, at Appendix A's parameters

---

## 0. The change in one page

| | today | proposed |
|---|---|---|
| tables holding attributes | 2 — one row per span, plus `A` index rows per span | 1 — the attributes ride the span's own row as arrays |
| tables that answer "which traces hold value V" | `trace_attrs_idx`, one row per **span** | `trace_attr_traces`, one row per **trace** |
| client `INSERT`s per batch of spans | 2, on two independent flush generations | 1 |
| materialized views on the trace family | 2 | 5 |
| query shapes with no sorted path | 4 of 9 | **1 of 9** — the metrics range query keeps its full-window scan, and §3.5 says why it cannot be pre-aggregated exactly |
| storage | 1047.9 B/span, 7.34 TB at 10⁹ spans/day and 7-day retention | **625.7 B/span, 4.38 TB** — **−40%** |
| merge work per span | 9856 LZ4-equivalent B per merge level | **5341** — **−46%** |
| bytes the writer sends ClickHouse | 1838 raw B/span in 2 statements | **1038 in 1** — **−44%** |
| SQL statements a one-condition search issues | 4 … 6252 | **3 … 3127** |

Every number in that table is derived. The parameters they depend on are in Appendix A;
substitute your own and Appendix B's calculator recomputes them.

**The read that was reported here as getting worse, restated.** A search batch's
phase-2 read was given as 2.2× the bytes it reads today. That figure was taken by
repeating an identical statement. ClickHouse 26.3 defaults
`use_query_condition_cache = 1`, which memoises the granules a condition selected, and a
search batch's statement carries a different 32-trace-id list every time, and a list
that has not been seen before is a miss. **How often a real deployment repeats a list is
not measured**; what was measured is that a fresh list misses even with the cache warm
from a previous one. On a first-seen batch with
`use_query_condition_cache = 0`, today's two statements read `15,809,612 + 48,523,078 =`
**64,332,690** bytes and the new single statement reads **30,144,320**: the new form reads
**0.47×**, not 2.2×. (26.3.29.7; `use_query_condition_cache=0`, `optimize_move_to_prewhere=1`, `max_block_size=65409`, `max_threads=auto(16)`; 3 reps, zero spread; corpus C1) The earlier total printed here, 63,987,313, was
arithmetic on figures this document has since replaced. In the warm
regime the same corpus reproduces the direction, at 2.39×. §4 Q1 carries both
readings, the corpus and the full instrument.

**Whether the original measurement was itself a warm reading is not established.** It was
taken on a different corpus of a different physical size and its per-statement settings
were not recorded, so the cache explanation accounts for the reversal seen here without
proving what happened there.

---

## 1. What we store today

### 1.1 The tables, drawn

```
 trace_spans                                MergeTree      catalog.rs:335-364
 PARTITION BY toDate(ts)   ORDER BY (trace_id, timestamp_ns)
 +------------------------------------------------------------------+
 | trace_id FixedString(16)   <- sort key 1: a trace's spans are     |
 |                               stored next to each other           |
 | span_id / parent_id  FixedString(8)                               |
 | name / service       LowCardinality(String)                       |
 | timestamp_ns Int64 CODEC(DoubleDelta, ZSTD(1))  <- sort key 2     |
 | duration_ns  Int64 CODEC(T64, ZSTD(1))                            |
 | status_code / kind / payload_type  Int8                           |
 | shared UInt8            (migration 31, catalog.rs:648-658)        |
 | status_message String   (migration 35, catalog.rs:738-748)        |
 | scope_name / scope_version LC (migration 37, catalog.rs:775-786)  |
 | payload String CODEC(ZSTD(3))     <- the whole OTLP span, again   |
 | INDEX idx_duration duration_ns minmax GRANULARITY 4               |
 +------------------------------------------------------------------+
 | PROJECTION service_time  SELECT *  ORDER BY (service, ts)         |
 |     `-- SELECT * means the payload is stored a SECOND time        |
 | PROJECTION span_name_day (day, name, count())   (migration 42/43) |
 +------------------------------------------------------------------+

 trace_attrs_idx                     ReplacingMergeTree   catalog.rs:365-388
 PARTITION BY date
 ORDER BY (key, val, scope, timestamp_ns, trace_id, span_id)
 +------------------------------------------------------------------+
 | date Date | key LC | val String | scope LC  <- the sorted prefix  |
 | val_num Nullable(Float64)   <- val's f64 parse, when finite       |
 | val_type LC     (migration 39, catalog.rs:812-822)                |
 | timestamp_ns Int64          <- no codec                           |
 | trace_id FixedString(16)    <- 16 bytes, never beside a copy      |
 | span_id  FixedString(8)                                           |
 | duration_ns Int64           <- a second copy of the span's        |
 +------------------------------------------------------------------+
   ONE ROW PER ATTRIBUTE PER SPAN.

 trace_tag_catalog                   ReplacingMergeTree   catalog.rs:393-407
 ORDER BY (scope, key, val, val_type)
   no PARTITION BY, no time column, NO TTL
   (controller.rs:479-480 says so in words: "a bounded catalog and
   carries no TTL"; it is absent from TTL_STMTS, controller.rs:436-472)
   fed by trace_tag_catalog_mv (catalog.rs:965-969):
        SELECT scope, key, val, val_type FROM trace_attrs_idx
        -- no GROUP BY

 trace_edges                         ReplacingMergeTree   catalog.rs:692-716
 PARTITION BY date   ORDER BY (side, trace_id, span_id)
   fed by trace_edges_mv (catalog.rs:985-1000)
```

### 1.2 One span, and every row it produces

Take one span. Four attributes, which is fewer than a real one carries — the
worked parameter is 20 (Appendix A) — but four fits on the page.

```
  trace_id   4bf92f3577b34da6a3ce929d0e0e4736
  span_id    00f067aa0ba902b7      parent_id  b7ad6b7169203331
  service    checkout              name       GET /pay
  start      1700000000000000000   (2023-11-14T22:13:20Z)
  duration   2500000000 ns         status     2 (error)   kind  3 (client)
  payload    the OTLP protobuf bytes of this span, ~400 bytes

  attributes
    resource  service.name              = "checkout"   (OTLP string)
    resource  deployment.environment    = "prod"       (OTLP string)
    span      http.status_code          = 500          (OTLP int)
    span      http.method               = "GET"        (OTLP string)
```

Resource attributes belong to a whole batch of spans, but the writer copies them
onto **every** span it produces (`otlp_traces.rs:487-503`, the loop over
`[(resource, …), (span, …), (instrumentation, …)]`).

**That loop is not the whole of what the writer emits.** `otlp_traces.rs:505-607` emits,
per span, two more families of index row:

```
   per span EVENT   (otlp_traces.rs:505-556)
     event:name           scope event:intrinsic   val = the event name
     event:timeSinceStart scope event:intrinsic   val_num = event.time - span.start, ns
     one row per event attribute, scope `event`, verbatim key

   per span LINK    (otlp_traces.rs:558-607)
     link:spanID          scope link:intrinsic    val = lowercase hex
     link:traceID         scope link:intrinsic    val = lowercase hex
     one row per link attribute, scope `link`, verbatim key
```

Seven scopes reach `trace_attrs_idx`, not three: `resource`, `span`,
`instrumentation`, `event`, `event:intrinsic`, `link`, `link:intrinsic`. Every one of
them has the same row shape — key, scope, val, val_type, val_num — so every one of them
rides the five arrays of §3.1 unchanged. **A writer change that moves only the first
loop makes `event:name`, `event:timeSinceStart`, `link:spanID`, `link:traceID` and every
event and link attribute unsearchable.**

The worked span below carries no events and no links, which is why its row count is
four. So this one span produces:

```
 trace_spans        1 base row
                  + 1 row in the service_time projection  (a FULL copy,
                      payload included, because the projection is SELECT *)
                  + a count bump in span_name_day

 trace_attrs_idx    4 rows:

   date       key                      val         scope     val_num  val_type
   ---------- ------------------------ ----------- --------- -------- --------
   2023-11-14 service.name             checkout    resource  NULL     string
   2023-11-14 deployment.environment   prod        resource  NULL     string
   2023-11-14 http.status_code         500         span      500      int
   2023-11-14 http.method              GET         span      NULL     string

   ... and every one of those four rows ALSO carries
       timestamp_ns  1700000000000000000
       trace_id      4bf92f3577b34da6a3ce929d0e0e4736     16 bytes
       span_id       00f067aa0ba902b7                      8 bytes
       duration_ns   2500000000

 trace_tag_catalog  4 rows written by the materialized view, one per attribute
                    row, with no GROUP BY - deduplicated only later, by merges

 trace_edges        1 row (kind 3 is a client half)

 ---------------------------------------------------------------
 11 physical rows written for one span, and the 24 bytes that name
 the span - trace_id + span_id - are written 5 times.
```

`val_num` is `val.parse::<f64>()` when the result is finite, else NULL
(`otlp_traces.rs:712-714`). It is set from the **text**, whatever OTLP type the
sender declared, which is why the string `"500"` under a different key would also
carry `val_num = 500`.

### 1.3 What that costs

Per span, compressed, at Appendix A's parameters:

| where the bytes are | B/span | share |
|---|---|---|
| `trace_spans` base row | 122.9 | 11.7% |
| `service_time` projection (`SELECT *`, so a second payload) | 137.6 | 13.1% |
| **`trace_attrs_idx`** | **787.3** | **75.1%** |
| `trace_tag_catalog` | ≈0 | ≈0% |
| **total** | **1047.9** | 7.34 TB at 10⁹ spans/day, 7-day retention |

Derived. The 75% depends on `A`, the number of attributes a span carries:

| `A` | 5 | 8 | 10 | **20** | 40 | 60 |
|---|---|---|---|---|---|---|
| index share of the family | 43.0% | 54.7% | 60.2% | **75.1%** | 85.8% | 90.1% |

---

## 2. What is wrong with it

### 2.1 Three quarters of storage is one table, and most of that table is names

One `trace_attrs_idx` row is 39.4 compressed bytes at Appendix A's parameters.
24 of them — `trace_id` 16 plus `span_id` 8 — are the **name of the span the row
points at**, and the sort order `(key, val, scope, timestamp_ns, trace_id,
span_id)` guarantees those bytes never sit beside a copy of themselves, so they
do not compress.

```
   one index row, 39.4 compressed bytes

   [ val 4.7 ][ num 1.7 ][ ts 5.0 ][  trace_id 16.0  ][ span_id 8.0 ][ dur 4.0 ]
                                   |<-------- 24.0 = 61% ---------->|

   paid A = 20 times per span  ->  480 B/span = 46% of the whole family
```

Derived. **Measured** on a 2,000,000-span corpus with `A` = 8, where the same
quantity is 79.7% of the table once the timestamp is counted with it:

| column of `trace_attrs_idx` | bytes on disk | share |
|---|---|---|
| `trace_id` | 222,378,166 | 38.7% |
| `span_id` | 128,552,749 | 22.4% |
| `timestamp_ns` | 106,926,535 | 18.6% |
| `duration_ns` | 98,157,457 | 17.1% |
| `val` | 11,271,295 | 2.0% |
| `val_num` | 6,694,459 | 1.2% |
| `date` + `key` + `scope` + `val_type` | 369,059 | 0.06% |

**Can the 24 bytes be made narrower?** No. A reference to one span among `2¹²⁸`
possible ids needs 116 bits even when the ids are sorted inside a run of 10⁴ rows.
That is 14.5 bytes against the 16 we pay, and no compression codec beats it. **The
number of rows has to fall, not their width.**

### 2.2 Four of the nine query shapes have no sorted path

A ClickHouse table has one sort order. Our eight shapes want six. Ranked by our
own planner, lower is better (`filter.rs:89-103`):

| rank | class | what reaches it | rows read in a 1-hour window |
|---|---|---|---|
| 0 | `AttrEq` | an attribute string equality | 1.04·10⁶ — a sorted seek |
| 1 | `ServiceEq` | `resource.service.name = "x"` | 8.33·10⁵ — the `service_time` projection |
| 2 | `AttrKeyScan` | a numeric or regex attribute | 2.08·10⁷ — one key's whole slice |
| 3 | `Duration` | `duration > 2s` | ≤ 4.17·10⁷ |
| **4** | **`SpanScan`** | **`name = "…"`, `status = error`** | **4.17·10⁷ — the whole window** |
| **5** | **`TimeRange`** | **`{}`, or only negations** | **4.17·10⁷ — the whole window** |

`status`, `name` and the empty query `{}` are the three things the Grafana traces
search form puts in front of a user before they type anything, and all three land
in rank 4 or 5. `docs/schemas.md:705` already names the class: *"no selective
index — window-bounded, budget-limited"*.

The proportions of a real query mix were derived separately, by reading what the
Grafana traces datasource plugin generates (that reading is not reproduced here).
Its result: **84–97% of the rows a search reads come from shapes with no sorted
path**, 93.5% at the worked point; and **about 5% come from the attribute index
that costs 75% of the storage.**

### 2.3 The tag dropdown's scan grows with the age of the deployment

`trace_tag_catalog` has no time column, no partition key and no TTL
(`catalog.rs:393-407`; `controller.rs:479-480`). Every distinct
`(scope, key, val, val_type)` ever ingested stays in it for ever.

```
   rows the dropdown scans

   today       K_tot  = every tuple the deployment has EVER seen, unbounded
   bounded     K_d·W  = tuples produced in the query window

   worked: 10^6 against 10^4 - a factor of 100, and it grows every day
```

And the request that reads it already computes a window and throws it away. The
values route parses `start`/`end`, defaulting to `traceql_tag_lookback` = 24 h
(`config/model.rs:516`, `traces_api/tags.rs:161-164`), then calls
`tag_values_sql` (`tags_sql.rs:118-127`), which emits no time predicate at all —
because the table it reads has no time column.

### 2.4 The narrowed dropdown is a join over the window

Open a tag-value dropdown while a service filter is set and the read becomes a
semi-join between the two tables at **day** grain
(`tags_sql.rs:282-312`, chosen at `exec.rs:1825-1866`):

```sql
SELECT DISTINCT val, val_type
FROM trace_attrs_idx
WHERE key = 'http.status_code' AND scope IN ('event','instrumentation','link','resource','span')
  AND date >= toDate('2023-11-14') AND date <= toDate('2023-11-15')
  AND (trace_id, span_id) IN (
    SELECT trace_id, span_id
    FROM trace_spans
    WHERE toDate(fromUnixTimestamp64Nano(timestamp_ns)) >= toDate('2023-11-14')
      AND toDate(fromUnixTimestamp64Nano(timestamp_ns)) <= toDate('2023-11-15')
      AND service = 'cart')
ORDER BY val, val_type LIMIT 1001
```

**Measured**: 2,138,112 rows and 74.6 MB at 2,000,000 spans; 21,037,056 rows and
734 MB at 20,000,000. It cannot be answered off the catalog, because
`trace_tag_catalog_mv` reads `trace_attrs_idx`, which has no `service` column
(`catalog.rs:965-969`, `catalog.rs:365-388`).

### 2.5 Three ways a failed write leaves the two tables disagreeing

The writer sends two `INSERT`s on two independent flush generations
(`writer/trace.rs:9-19`, and `admit_batch` at `:220-304` appends to two separate
buffers drained by two separate tasks). A reader can therefore see a span without
its attribute rows during the settle window. That window is temporary and the
module documents it. These three are not temporary:

| # | what fails | where | what is left behind |
|---|---|---|---|
| 1 | the `trace_attrs_idx` insert is **definitely** not committed; its rows go to a bounded in-memory backlog, and a row that would push the backlog over `backfill_max_bytes` is dropped and counted | `writer/trace.rs:137-185`; `backfill.rs:189-201`; the backlog's byte cap, `backfill.rs:78-90` | the span is stored; its attributes never arrive. It is fetchable by id and **invisible to attribute search, permanently** |
| 2 | the backlog's own re-insert returns `InsertUncertain`, or any deterministic error | `backfill.rs:214-220` — both branches remove the entry and count it abandoned, never retried | as above |
| 3 | the **`trace_spans`** insert fails, definitely or uncertainly. `trace_spans` passes `on_flush_poisoned: None` (`writer/trace.rs:172`) — it is the structural append-only exclusion (`backfill.rs:23-28`), so nothing ever replays it | `writer/table.rs:367-434`, which spools the rows to disk as an audit record and settles the generation with an error | the attribute rows are stored; the span is not. A search generates that trace as a candidate and its hydration returns nothing |

In all three the client is told the write failed. What it is not told is *which
half* survived.

### 2.6 The payload is compressed with ZSTD(3), twice

`service_time` is `SELECT *` (`catalog.rs:353-355`), so the `payload` column is
stored a second time and re-compressed on every merge. Of the 9856
LZ4-equivalent bytes a span costs per merge level, 8000 are those two ZSTD(3)
passes. Nothing reads `payload` from the projection: the only statement that
selects it is the trace-by-id point read, and that filters on `trace_id`, which
is sort key 1 of the **base** table (`sql.rs:16-26`).

---

## 3. The new structure

Three moves, and one small addition.

```
  1. the attributes ride the span's own row, as five aligned arrays
  2. the value-sorted table drops to TRACE grain: one row per
     (attribute value, trace, 5-minute bucket) instead of per span
  3. three of the four shapes with no sorted path get one
  4. the tag catalog gains a date, a service, a partition and a TTL

  and one thing deliberately NOT done: the metrics range query is not
  pre-aggregated. Section 3.5.
```

### 3.1 The tables, drawn

```
 trace_spans                          MergeTree, SAME engine, SAME order
 PARTITION BY toDate(ts)   ORDER BY (trace_id, timestamp_ns)
 +------------------------------------------------------------------+
 | ... every column it has today, unchanged ...                      |
 |                                                                   |
 | + attr_key    Array(LowCardinality(String))   }                   |
 | + attr_scope  Array(LowCardinality(String))   }  same length,     |
 | + attr_val    Array(String)                   }  position i is    |
 | + attr_type   Array(LowCardinality(String))   }  one attribute    |
 | + attr_num    Array(Nullable(Float64))        }                   |
 | CONSTRAINT attr_arrays_aligned CHECK the five lengths are equal   |
 +------------------------------------------------------------------+
 | PROJECTION service_time  <the 14 non-payload columns>             |
 |                          ORDER BY (service, timestamp_ns)         |
 |     `-- named columns, NOT SELECT *: no payload, no arrays        |
 | PROJECTION name_time     the same 14, ORDER BY (name, timestamp_ns)|
 | PROJECTION span_name_day unchanged                                |
 +------------------------------------------------------------------+

 trace_attr_traces                       AggregatingMergeTree    NEW
 PARTITION BY date
 ORDER BY (key, val, scope, bucket, trace_id, val_type)
 +------------------------------------------------------------------+
 | date Date | key LC | val String | scope LC   <- the sorted prefix |
 | bucket   UInt32   <- intDiv(timestamp_ns, 300000000000)           |
 | trace_id FixedString(16)                                          |
 | val_type LowCardinality(String)                                   |
 | val_num  Nullable(Float64)                                        |
 | ts_max   SimpleAggregateFunction(max, Int64)                      |
 | dur_max  SimpleAggregateFunction(max, Int64)                      |
 | dur_min  SimpleAggregateFunction(min, Int64)                      |
 +------------------------------------------------------------------+
   ONE ROW PER (value, trace, 5-minute bucket).
   REPLACES trace_attrs_idx, which is deleted.

 trace_tag_catalog                       ReplacingMergeTree
 PARTITION BY date   ORDER BY (scope, key, service, val, val_type)
 TTL date + <retention> DAY
 + date Date  + service LowCardinality(String)

 trace_error_spans                       ReplacingMergeTree      NEW
 PARTITION BY date   ORDER BY (timestamp_ns, trace_id, span_id)
   date, trace_id, span_id, timestamp_ns, duration_ns, service, name, kind
   fed by an MV: WHERE status_code = 2


 trace_recent                            AggregatingMergeTree    NEW
 PARTITION BY date   ORDER BY (bucket, trace_id)
   date, bucket UInt32, trace_id, ts_max SimpleAggregateFunction(max, Int64)

 trace_edges                             unchanged, byte for byte
```

The writer sends **one** `INSERT`, into `trace_spans`. Every other table is a
materialized view over it. `trace_attr_traces` is fed by:

```sql
CREATE MATERIALIZED VIEW trace_attr_traces_mv TO trace_attr_traces AS
SELECT toDate(fromUnixTimestamp64Nano(timestamp_ns))        AS date,
       key, val, scope, val_type, val_num,
       toUInt32(intDiv(timestamp_ns, 300000000000))         AS bucket,
       trace_id,
       max(timestamp_ns) AS ts_max,
       max(duration_ns)  AS dur_max,
       min(duration_ns)  AS dur_min
FROM trace_spans
ARRAY JOIN attr_key AS key, attr_scope AS scope, attr_val AS val,
           attr_type AS val_type, attr_num AS val_num
GROUP BY date, key, val, scope, val_type, val_num, bucket, trace_id;
```

Nothing in that view is untried. An `ARRAY JOIN` inside a materialized view is
what `log_streams_idx_mv` does (`catalog.rs:934-942`). A `GROUP BY` inside one,
feeding `SimpleAggregateFunction` columns of an `AggregatingMergeTree`, is what
`log_metrics_*_mv` does into `log_metrics_*` (`catalog.rs:945-953`, table at
`catalog.rs:266-281`). And the two **together in one view** — `ARRAY JOIN` over
`trace_spans`'s arrays plus a `GROUP BY` — was built and run for the tag catalog
in the measured one-table corpus, where it produced the same 1,094,467 rows as
the two-table build.

**What has never been run is that combination writing `SimpleAggregateFunction`
columns**, which is what `ts_max`, `dur_max` and `dur_min` are. That is the first
thing to try if this design is taken — §11 P1.

The whole shape is also what the logs family already does: `log_streams_idx` is
sorted `(key, val, fingerprint)` with one row per `(key, val, stream)`, never one
per sample (`catalog.rs:227-234`).

### 3.2 The same span, and every row it now produces

```
 trace_spans   1 base row - the same columns as before, plus

   attr_key   ['service.name','deployment.environment','http.status_code','http.method']
   attr_scope ['resource',   'resource',              'span',            'span']
   attr_val   ['checkout',   'prod',                  '500',             'GET']
   attr_type  ['string',     'string',                'int',             'string']
   attr_num   [NULL,         NULL,                    500,               NULL]

   A span carrying one event and one link appends, to the SAME five arrays and in
   the writer's existing emission order (otlp_traces.rs:505-607):

   attr_key   [... , 'name',            'timeSinceStart',  'db.system', 'spanID',        'traceID',       'rel']
   attr_scope [... , 'event:intrinsic', 'event:intrinsic', 'event',     'link:intrinsic','link:intrinsic','link']
   attr_val   [... , 'cache.miss',      '120000000',       'redis',     '00f0…02b7',     '4bf9…4736',     'child']
   attr_type  [... , 'string',          'int',             'string',    'string',        'string',        'string']
   attr_num   [... , NULL,              120000000,         NULL,        NULL,            NULL,            NULL]

   All seven scopes, one array set. No scope needs a column of its own.

             + 1 row in service_time   (14 columns; no payload, no arrays)
             + 1 row in name_time      (the same 14)
             + a count bump in span_name_day

 trace_attr_traces   4 rows for this span, and they COLLAPSE with the
                     other spans of the same trace in the same bucket:

   key                     val       scope     bucket    trace_id      ts_max
   ----------------------- --------- --------- --------- ------------- ------
   service.name            checkout  resource  5666666   4bf9...4736   ...
   deployment.environment  prod      resource  5666666   4bf9...4736   ...
   http.status_code        500       span      5666666   4bf9...4736   ...
   http.method             GET       span      5666666   4bf9...4736   ...

   The trace's other 11 spans re-emit `service.name = checkout` and
   `deployment.environment = prod`; every one of those collapses into the
   SAME row. That is the whole saving.

 trace_tag_catalog   <= 4 rows PER INSERTED BLOCK, not per span
 trace_edges         1 row       (unchanged)
 trace_error_spans   1 row       (status_code = 2)
 trace_recent        1 row per (trace, bucket) - shared by all 12 spans
```

### 3.3 Why a bucket, and why in that position

Two things pull against each other. Collapsing rows to the trace needs the
per-span timestamp **out** of the sort key, because a merge only collapses rows
that agree on the whole key. Pruning a sub-day window needs a time column **in**
it.

```
   today   ORDER BY (key, val, scope, timestamp_ns, trace_id, span_id)
                                      ^^^^^^^^^^^^ position 4, one per span

   new     ORDER BY (key, val, scope, bucket,       trace_id, val_type)
                                      ^^^^^^ position 4, one per 5 minutes
```

The bucket sits **exactly where the timestamp sits today**, so pruning behaves
exactly as it does today: a ClickHouse sort-key column at position 4 can prune
only when positions 1–3 are each bound to a single value, so an equality search
prunes on time in both designs and a range or regex search on `val` prunes on
neither. Nothing gets worse; the collapse becomes possible.

A trace whose spans straddle a bucket edge produces two rows instead of one. The
fraction is `trace_duration / B` — about 0.3% for one-second traces at
`B` = 5 minutes.

The bucket column is `UInt32`, and it cannot overflow: ingest rejects any span
whose UTC day falls before 1970-01-01 or after 2106-02-06
(`otlp_traces.rs:465-486`), so `intDiv(timestamp_ns, 3·10¹¹)` lies in
`[0, 1.43·10⁷]` against a `UInt32` ceiling of 4.29·10⁹.

### 3.4 Every new table is safe against a duplicated span row, and one would not have been

Spans are written at least once and never deduplicated. `trace_spans` is a plain
`MergeTree` (`catalog.rs:335-364`). Our own writer never replays a block whose
commit fate is unknown — a failure after the bytes are sent is classified and
never retried, *"the one hard invariant this crate enforces"*
(`writer/table.rs:313-321`) — but nothing stops a client resending the same
spans. So the read path counts spans as `uniqExact(trace_id, span_id)` rather
than `count()`, and says why in words: *"at-least-once replays must never inflate
a bucket"* (`metrics_sql.rs:9-12`).

Every table in §3.1 is checked against that:

| table | what a duplicated span row does to it | safe? |
|---|---|---|
| `trace_spans` arrays | the probe reads the span's own row; both copies answer the same | **yes** |
| `trace_attr_traces` | the duplicate emits the same `(key, val, scope, bucket, trace_id)` tuple, which collapses into the same row; `max`/`min` are unchanged by repeating a value | **yes** |
| `trace_recent` | same shape, `max(ts_max)` | **yes** |
| `trace_error_spans` | `ReplacingMergeTree` on `(timestamp_ns, trace_id, span_id)` collapses the duplicate, and the read is `GROUP BY trace_id, max(timestamp_ns)` anyway | **yes** |
| `trace_tag_catalog` | `ReplacingMergeTree`, read with `DISTINCT` | **yes** |
| `name_time`, `service_time` | projections hold exactly the base table's rows; the read is `GROUP BY trace_id, max(timestamp_ns)` | **yes** |

### 3.5 What was designed, priced, and then rejected: a metrics rollup

A pre-aggregated table keyed `(bucket, service, name, status_code, kind)` would
turn the metrics range query from a full-window scan into a small read — 1.80·10⁶
rows instead of 4.17·10⁷ at one-minute buckets. **It is not in this design, and
the reason is §3.4.**

```
   the query today            uniqExact(trace_id, span_id)   counts DISTINCT spans
   the rollup would give      sum(count)                     counts ROWS

   one span written twice ->  today  1        rollup  2
```

The exact form is `uniqExactState(trace_id, span_id)`, and an exact
distinct-count state has to remember every distinct value it has seen — so it is
the size of the data it was meant to summarise, and the rollup saves nothing.
(That last step is asserted from what exactness requires, not read: ClickHouse is
not checked out on this machine. §11.1.) There is no third option:
**the rollup cannot answer `rate()` or `count_over_time()` with today's answer,
so it does not answer them.** The rule this follows is exact-or-refuse, and
refusing is always available.

What it would have cost, for whoever revisits this:

| rollup bucket `B_r` | `count`/`sum`/`min`/`max` | plus a latency sketch |
|---|---|---|
| 60 s | 0.38 B/span | +51.8 B/span |
| 300 s | 0.08 B/span | +10.4 B/span |
| 900 s | 0.03 B/span | +3.5 B/span |

at 3·10⁴ distinct groups. The sketch is 99% of it, and it would be 7.6% of the
whole family at one-minute buckets. Two things would make the rollup possible:
an ingest path that guarantees each span row is written exactly once, or a
metrics answer that is defined on rows rather than on distinct spans. Both are
decisions about the product, not about storage.

### 3.6 Where this departs from the analysis it came from, and why

Three changes, each with the number that motivated it. A fourth — dropping the
metrics rollup entirely — is §3.5.

| the option document says | this design says | why |
|---|---|---|
| six arrays, including `attr_val_i64 Array(Nullable(Int64))` | **five arrays.** No integer array | An exact-integer column would make some comparisons above 2⁵³ answer differently from today's `Nullable(Float64)`. That is a change of answer, and it belongs to whoever decides to make it, not to a storage change. Five arrays cost 130.3 compressed B/span against 156.3 — and give byte-identical answers |
| `trace_tag_catalog ORDER BY (service, scope, key, val, val_type)` | **`ORDER BY (scope, key, service, val, val_type)`** | Under the first order, a dropdown that names **no** service — 26.5% of the requests an investigation makes, on the query mix of §2.2 — loses its `(scope, key)` prefix prune entirely, because `service` leads and is unbound. Under the second, the un-narrowed read keeps the prune it has today and the service-narrowed read gains a `(scope, key, service)` seek. The cost is a sort over a small slice for the un-narrowed shape, which today came free from the storage order |
| `trace_error_spans ORDER BY (date, service, timestamp_ns)` | **`ORDER BY (timestamp_ns, trace_id, span_id)`**, `PARTITION BY date` | `date` is already the partition key, so leading the sort key with it prunes nothing extra — and with `service` in position 2 and unbound, a bare `{status = error}` cannot prune on time inside the day partition. It would read a whole day for a one-hour question: **24× more rows than needed at `W` = 1 h** |

The option document's recommendation — remove the waste first, then move the
index to trace grain, then add the sorted paths — is followed, minus the rollup.
What also changes is that with nothing shipped there is no reason to do the rest
as three separate migrations (§8).

---

## 4. The nine queries, before and after

The "before" SQL is copied from committed golden files under
`crates/pulsus-read/tests/golden/`, which are byte-frozen against the builders.
The "after" SQL is what the same builders would produce against the new tables.

**Corpus C1, which every `[M]` figure below names.** 2,000,000 spans,
166,667 traces of 12 spans, 8 attributes per span, three hours from
`1700000000000000000`, two UTC day partitions. `spans_old` and `spans_new` are built by
the **same** eight `INSERT … SELECT … FROM numbers(<lo>, 250000)` statements at
`max_insert_threads = 1, max_threads = 1, max_block_size = 65409`; `payload` is
`repeat(substring(concat(four sipHash128 hex digests of the row number), 1, 100), 4)`,
so it is a deterministic function of the row number. Then `OPTIMIZE … FINAL` on every
table. **Its `payload` compresses 15.91× (816,000,000 raw to 51,292,209 on disk), well
above Appendix A's `Z_p` = 4**, which matters to §5's storage row and to nothing else —
no query below selects `payload` except Q5.

**The 32 phase-1 ids the batch measurements use, published in full** so no digest
convention is needed. They are what
`SELECT trace_id, max(timestamp_ns) AS b FROM spans_old PREWHERE service='svc-3'
WHERE timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000
GROUP BY trace_id ORDER BY b DESC, trace_id ASC LIMIT 32` returns on C1:

```
e948da06ce241975afd4e7d6d8026e69  8c11154d86e5ecc4f66baeb8ea447adb  9df0d7515aeace453503bddf600aca04
1c79709c5be7ffc7fe1a832e722a8833  13da092c5e695c684a3d3b6a4ae743c5  402b54a8ec604dd8fc052c393828ff15
8e502fc32e168f44af9c6916d4a9d1a8  c8c082bbe2d42e4c18a741027d34c216  c6f6e8e2c97e473467c0a265f667c3a3
222a8051aa1a84749987ab0bfad92689  97d6879b0b0b8f9827b6f7efdc9f5fa7  1a455236dbfb4407b3d013c7ba6bdcd3
f80de147c42435823b4e96699b8afdc5  146b61bfdef2b3d29a720272dbe958ea  abd288af4affe9e83b53053f04df3b83
e2c39f07adf2e0b9aff0f34dd398540f  f08ad33a05ecd064ba5e0accd813acfd  da8b361d2ce618f22025bd94d1fd1d82
45d9a911923b434a4fa9bca1690cb45a  5d61ef3b9eb18d1dcca590ef9f6cb107  71c84282136fe0109c1585ec6fa5eb9f
3b2a92eb06b89df450d0d05550fbe0d6  3a8465006c3aeff328e2d7bd1e0b1ba4  85391f991789a2dd742df3f04b2c8e9f
d1246c2c66ba8a932e37eba813769ca7  dfa13942a69d591baaf61bac0fa72a52  4bf1b1119bb647e06653179c0afd4596
24609a98fcd348e4cd3a2820e33e3a09  9e9795059aa3513254c06c9e7906e053  ff3237ca295901ba3abfdff1c20e38bd
8f1b096e54ca056d661e37ff9c76ca41  0141358bb246527351994c56ce011868
```

`SHA256` of that list joined by single commas, computed in ClickHouse **and by
`printf %s | sha256sum` on the same bytes**, is
`73a0e7b4ba4cc136686150904074d6d902318cb598e867a5c0f870e338fad75b`.

**C1 is pinned by a construction script, not by prose.** The script creates both span
tables, runs the sixteen inserts and the two `OPTIMIZE … FINAL`, and ends by printing one
digest over the physical layout every counter in §4 depends on:

```sql
SELECT lower(hex(SHA256(arrayStringConcat(groupArray(
         concat(table,'|',name,'|',toString(rows),'|',toString(marks))), ';'))))
FROM (SELECT table, name, rows, marks FROM system.parts
      WHERE database='c1' AND active AND table IN ('spans_old','spans_new')
      ORDER BY table, name)
```

On 26.3.29.7 it prints
`e58c2eb30291fa579a90fe947f3327776424ffcce2d999507c327c5f39b9d1a0`, over this layout:

    spans_new  20231114_1_5_1  1,185,089 rows  148 marks
    spans_new  20231115_6_9_1    814,911 rows  102 marks
    spans_old  20231114_1_5_1  1,185,089 rows  148 marks
    spans_old  20231115_6_9_1    814,911 rows  102 marks

**A different digest is a different corpus, and that is what a granule disagreement
is.** C1's window runs from 22:13:20 on one UTC day to 01:13:20 on the next, so
`PARTITION BY toDate(…)` gives **two parts** and 148 + 102 = 250 marks; a full-window read
selects 248 of them. A corpus whose spans fall inside one UTC day is **one part** of
⌈2,000,000 / 8,192⌉ = 245 marks, and the same query then reports 245. Neither is wrong;
they are different corpora, and the digest is how to tell.

**One earlier attribution is withdrawn.** An earlier version of this section explained a
non-reproducing hydration counter — 453,824 / 15,464,235 — as an artefact of building the
old span table with a single `INSERT … SELECT` and a random payload. Two independent
reconstructions of that build produced 455,036 / 15,580,652 and 451,852 / 15,416,907;
neither is the figure the explanation was offered for. **The construction accounts for the
mark counts 246/492, which both reconstructions reproduced, and not for the hydration
counter. That counter's corpus no longer exists and its cause is unknown.**

### Q0 — `{}`, the query the search form sends before you type anything

```sql
-- today            golden/traces_search/existence_absent.sql:5-10 has this shape
SELECT trace_id, max(timestamp_ns) AS bound_ts
FROM trace_spans
WHERE timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000
GROUP BY trace_id ORDER BY bound_ts DESC, trace_id ASC LIMIT 100001

-- new
SELECT trace_id, max(ts_max) AS bound_ts
FROM trace_recent
WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15')
  AND bucket >= 5666666 AND bucket <= 5666702
  AND ts_max > 1700000000000000000
GROUP BY trace_id ORDER BY bound_ts DESC, trace_id ASC LIMIT 100001
```

**There is no `ts_max <= <end>` bound, and there cannot be one.** `ts_max` is the newest
span of the trace *in that bucket*. When the window ends inside a bucket, a trace with
in-window spans and a later span in the same bucket has `ts_max > end`, and the
predicate drops the row before the `GROUP BY` — so the trace disappears from an answer
today's query returns. Measured on the 2,000,000-span corpus of §4's table, window
`(1700000000000000000, 1700005400000000000]`, whose end falls inside bucket 5666684,
`use_query_condition_cache = 0`:

    today's Q0                                  83,334 traces
    with `AND ts_max <= <end>`                  83,317 traces   <- 17 traces LOST
    without it                                  84,877 traces   <- a superset, as §6.1 wants

    (26.3.29.7; use_query_condition_cache=0, optimize_move_to_prewhere=1, max_block_size=65409, max_threads=auto(16); 3 reps, zero spread; corpus C1)

The witness is trace `009c0bde7c7bcbcdd0952fd56bbb74bc`:

    its spans      1700005399913600000            <- inside the window
                   1700005400013600000 … 1700005401013600000   (11 more, after the end)
    trace_recent   bucket 5666684, ts_max 1700005401013600000

Q2 below already omits the bound, for the same reason.

Reads **3.48·10⁶ trace rows instead of 4.17·10⁷ span rows — 12× fewer**, and that
is guaranteed by row counts alone. **Measured** at 11.96×: today
2,000,000 / 48,000,408 / 248 marks, new 167,277 / 5,018,350 / 22 marks, both returning
100,001 rows (26.3.29.7; `use_query_condition_cache=0`, `optimize_move_to_prewhere=1`, `max_block_size=65409`, `max_threads=auto(16)`; 3 reps, zero spread; corpus C1).

**The 144× does not happen.** §11 P4 asked whether the read can stop at the newest
bucket. `EXPLAIN indexes = 1` on the statement above reports `Granules: 22/22` — the
whole window — and `optimize_aggregation_in_order = 1`, that plus
`optimize_read_in_order = 1, max_threads = 1`, and
`query_plan_optimize_lazy_materialization = 1` each read the same 167,277 rows and 22
marks (26.3.29.7; `use_query_condition_cache=0`, `optimize_move_to_prewhere=1`, `max_block_size=65409`, `max_threads=auto(16)`; 3 reps, zero spread; corpus C1), each setting added to that instrument. `GROUP BY trace_id` is not a prefix of `(bucket, trace_id)` and the `ORDER BY` is
on an aggregate, so the `LIMIT` cannot engage before the aggregation. The corpus can
exercise the limit: it holds 166,667 traces against a 100,001 cap. **12× is the whole
of it.**

### Q1 — a service, an attribute and a duration

`{ resource.service.name = "checkout" && span.http.status_code >= 500 && duration > 2s }`

Phase 1 finds candidate traces. It is **byte-identical** in both designs
(`golden/traces_search/worked_example.sql:5-11`):

```sql
SELECT trace_id, max(timestamp_ns) AS bound_ts
FROM trace_spans PREWHERE service = 'checkout'
WHERE timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000
GROUP BY trace_id ORDER BY bound_ts DESC, trace_id ASC LIMIT 100001
```

Phase 2 takes 32 candidates at a time (`exec.rs:117`). **Today that is two
statements per batch** — one to fetch the spans, one to ask the index which of
them carry the attribute:

```sql
-- today, statement 1 of 2   (search_sql.rs:230-252)
SELECT trace_id, span_id, parent_id, <byte-capped service>, <byte-capped name>,
       timestamp_ns, duration_ns, status_code, <byte-capped status_message>, kind,
       <byte-capped scope_name>, <byte-capped scope_version>
FROM trace_spans
WHERE trace_id IN (…32 ids…)
  AND timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000
ORDER BY trace_id ASC, timestamp_ns ASC, span_id ASC
LIMIT 10001 BY trace_id

-- today, statement 2 of 2   (search_sql.rs:286-312)
SELECT DISTINCT trace_id, span_id, <byte-capped val> AS v, val_type AS t
FROM trace_attrs_idx
WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15')
  AND (key = 'http.status_code' AND val_num >= 500 AND scope = 'span')
  AND timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000
  AND trace_id IN (…the same 32 ids…)
```

**New: the membership read becomes a column of the first statement.**

```sql
SELECT trace_id, span_id, parent_id, <byte-capped service>, <byte-capped name>,
       timestamp_ns, duration_ns, status_code, <byte-capped status_message>, kind,
       <byte-capped scope_name>, <byte-capped scope_version>,
       arrayExists((key, scope, val_num) ->
                   (key = 'http.status_code' AND val_num >= 500 AND scope = 'span'),
                   attr_key, attr_scope, attr_num) AS probe0
FROM trace_spans
PREWHERE trace_id IN (…32 ids…)
WHERE timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000
ORDER BY trace_id ASC, timestamp_ns ASC, span_id ASC
LIMIT 10001 BY trace_id
```

**The lambda body is the existing predicate string, unaltered.**
`search_plan.rs:661` already carries `probe_predicates: Vec<String>`, documented as
"Each probe's pre-escaped **positive** predicate", built by `membership_predicate`
(`search_plan.rs:1077`) against the column names `key`, `scope`, `val`, `val_num`.
Naming the lambda parameters after those columns makes the string reusable verbatim —
measured on ClickHouse 26.3.29.7 for a numeric range, a string equality, a
`match(val, …)` regex and a bare key-existence predicate, all four accepted and all four
returning 1 on a span that carries the attribute.

**Only the arrays the predicate names are passed**, because a column a statement does
not read costs it nothing. Measured on the corpus below, same statement, the metrics
form of the probe:

    lambda over (key, scope, val_num)             288,001,456 bytes   144/162/147 ms
    lambda over (key, scope, val)                 376,157,016 bytes   172/167/169 ms
    lambda over (key, scope, val, val_num)        536,157,016 bytes   214/221/239 ms

**The probe is POSITIVE and stays positive. Negation is not done in SQL.**
`search_eval.rs:1213-1216` evaluates a leaf as `member != *negated`, so `{ span.k != "x" }`
is a positive `k = 'x'` probe whose result the reader inverts. Rendering the negation
inside `arrayExists` instead changes the answer on three of five cases. Measured:

    span's attributes        today, and required   arrayExists(val != 'x')   NOT arrayExists(val = 'x')
    []            (no key)            1                      0                        1
    ['x']                             0                      0                        0
    ['x','y']                         0                      1                        0
    ['y']                             1                      1                        1
    ['j' = 'x']   (other key)         1                      0                        1

An absent key must match `!= "x"` and a span carrying both `x` and `y` must not.
`arrayExists` over a negated element predicate gets both backwards, because it asks
"does SOME element differ" where the question is "does NO element match".
`arrayAll(NOT positive)` is equivalent to `NOT arrayExists(positive)` and returns the
required column on all five; either renders the same answer, and neither belongs in the
SQL, because the reader already holds the `negated` flag.

**Two of the eight search builders disappear; one is retargeted, not deleted.**

| builder | after | why |
|---|---|---|
| `membership_sql` (`search_sql.rs:286`) | **deleted** — becomes `probe0` above | the result is one `UInt8` per span row |
| `attr_values_sql` (`:325`) | **deleted** — becomes two columns per read field | it is SCALAR: one value per (span, key). `arrayFirstIndex(…) AS i0`, then `attr_num[i0]` / `<byte-capped> attr_val[i0]` and `attr_type[i0]` from the SAME element. One capped string per field per row, which is the row shape the hydration read already has |
| `event_set_sql` (`:397`) | **retargeted to `trace_spans` with an `ARRAY JOIN`**, still its own statement | it is MULTI-VALUED, and its own doc comment (`search_sql.rs:366-380`, issue #351) records why a row-per-value shape replaced an aggregate one: "An ARRAY column is an unbounded number of capped strings in ONE row … phase-2 reads carry no `max_memory_usage`". Projecting `arrayFilter(…)` as a column would put that shape back. `ARRAY JOIN` over the span row reproduces the row-per-value read exactly, on the granules the batch already selects |

`root_sql`, `trace_ctx_sql` and `child_count_sql` (`:428, 468, 492`) read `trace_spans`
by `trace_id IN` and are untouched.

**Which value a duplicated key yields, and why the rule is the one below.** A span may
carry the same key twice. Today `attr_values_sql` reads `any(val)` /
`any(val_num)` over a `GROUP BY (trace_id, span_id)`, which picks arbitrarily and is not
stable across merges. The array form picks by position, so a rule has to be chosen, and
the reference already has one:

```
   the reference, tempodb/encoding/vparquet4/block_traceql.go:128-152 @ v3.0.2
     scoped    scan the scope's attribute slice IN ORDER, return the FIRST match

   the reference, block_traceql.go:248-275 @ v3.0.2
     unscoped  try scopes in this order and take the first that has the name:
               span -> resource -> event -> link -> instrumentation
```

So a stored span in sender order `[7, 5]` answers **7**, not 5. Today's `any()` returned
5 in one measurement; that is not a contract, it is whichever row the aggregate reached
first. **This design adopts the reference's rule, and it is a change of answer** — a
ledger row and a differential test, not a silent improvement.

**The locate and the type test are two steps, and putting them in one breaks the rule.**
An earlier draft rendered the numeric read as
`arrayFirstIndex((key, scope, val_num) -> key = K AND scope = S AND isNotNull(val_num), …)`,
folding today's `AND isNotNull(val_num)` filter into the search. On a span carrying
`k = 'bad'` then `k = '5'` that skips past the first match and returns index 2, value 5 —
neither first-match nor the reference's rule. Measured:

    arrays ['k','k'] / ['span','span'] / ['bad','5'] / ['string','int'] / [NULL, 5]

      arrayFirstIndex((key, scope) -> key='k' AND scope='span', …)                 -> 1
      arrayFirstIndex((key, scope, val_num) -> … AND isNotNull(val_num), …)        -> 2   WRONG

**Locate on `(key, scope)` alone, then read that element:**

      i0 = arrayFirstIndex((key, scope) -> key='k' AND scope='span', attr_key, attr_scope)
      attr_val [i0] -> 'bad'      attr_type[i0] -> 'string'      attr_num[i0] -> NULL

A NULL numeric read means the located element is not numeric, which is the same thing
today's `isNotNull(val_num)` filter expresses by returning no row for that span — so the
reader treats a NULL `v{n}` exactly as it treats an absent `agg_values` entry. **Where the
two differ is which element they look at**, and that difference belongs in the same ledger
row as the duplicate-key rule.

Both forms were measured on 26.3.29.7:

    scoped     arrayFirstIndex((key, scope) -> key = K AND scope = S, attr_key, attr_scope)
               on ['7','5'] returned index 1, value 7, kind int

    unscoped   five arrayFirstIndex calls, taken in the precedence order above:
               attrs ['resource','link','span','event','instrumentation'] chose 'span'
               attrs ['link','resource','event']                          chose 'resource'

**The byte cost of the batch, measured on a first-seen batch.** Corpus: 2,000,000 spans,
166,667 traces of 12 spans, 8 attributes per span, three hours from
`1700000000000000000`. **Both span tables are built by the same eight
`INSERT … SELECT … FROM numbers(<lo>, 250000)` statements at
`max_insert_threads = 1, max_threads = 1, max_block_size = 65409`, with a payload that is
a deterministic function of the row number**, then `OPTIMIZE … FINAL`. That construction
is what makes the counters reproducible: an earlier corpus built `spans_old` with a
single `INSERT … SELECT` and used `randomPrintableASCII`, and its hydration counters and
several mark counts did not reproduce on a rebuild.

Instrument: ClickHouse 26.3.29.7, `use_query_condition_cache = 0`, `optimize_move_to_prewhere = 1`, `max_block_size = 65409`, `max_threads = auto(16)`, five repetitions,
zero counter spread. The 32 ids are the first batch phase 1 returns for
`service = 'svc-3'`, first `e948da06ce241975afd4e7d6d8026e69`, last
`0141358bb246527351994c56ce011868`:

| statement | read_rows | read_bytes | marks | ms |
|---|---|---|---|---|
| today, hydration | 449,736 | 15,809,612 | 56 | 16/16/15/15/15 |
| today, membership | 2,015,232 | 48,523,078 | 246 | 13/13/11/13/12 |
| **today, both** | | **64,332,690** | | |
| new, one statement, `WHERE` | 449,736 | 32,529,648 | 56 | 22/22/21/22/20 |
| **new, one statement, `PREWHERE`** | 449,736 | **30,144,320** | 56 | 23/22/22/20/21 |

**0.47×, not 2.2×.** The 2.2× reading is the warm one. Same corpus, same statements,
instrument as above but `use_query_condition_cache = 1`, after
`SYSTEM DROP QUERY CONDITION CACHE`, three consecutive runs each:

| statement | run 1 | runs 2 and 3 |
|---|---|---|
| today, hydration | 449,736 / 15,809,996 / 56, hits 0 misses 4 | 216,780 / 10,218,940 / 27, hits 4 misses 0 |
| today, membership | 2,015,232 / 48,523,078 / 246, hits 0 misses 4 | 24,576 / 845,198 / 3, hits 4 misses 0 |
| new, `PREWHERE` | 449,736 / 30,145,517 / 56, hits 0 misses 4 | 216,780 / 26,417,997 / 27, hits 4 misses 0 |

`11,064,138` against `26,417,997` — **2.39×**. A fresh 32-id list goes straight back to
246 marks with the cache warm from the previous list. `use_query_condition_cache`
defaults to 1 on this server and `git grep -n use_query_condition_cache -- crates/*/src`
finds no production pin. **How often a real deployment issues the same 32-id list twice
is not measured**; what is measured is that a list not seen before does not hit.

**The `PREWHERE` remedy is close to a no-op**, and §11 P2 asked the wrong question of it.
`optimize_move_to_prewhere = 1` already moves the whole `WHERE` into the prewhere pass by
itself — `EXPLAIN actions = 1` on the `WHERE` form prints
`Prewhere filter column: and(greater(timestamp_ns, …), lessOrEquals(timestamp_ns, …), in(trace_id, <32-element set>))`,
and the explicit-`PREWHERE` form prints only the `in(trace_id, …)` half. The 7.33%
difference is which conditions sit in that pass, not whether there is one.

**What the batch's cost actually turns on is how selective the probed value is.** Same
corpus, same batch, instrument as the first-seen table above, three repetitions, zero
spread:

    probe                                              read_rows  read_bytes   marks
    key='http.status_code' AND val_num >= 500          2,015,232  48,523,078    246
    key='http.method' AND val='GET'                      516,096   9,380,321     63
    key='request.id' AND val='r-1234567'                  16,384     294,928      2

    whole batch, today (hydration 15,809,612 + membership) vs new (PREWHERE 30,144,320)
    numeric range                64,332,690  ->  30,144,320   0.47x
    string eq, 4 distinct vals   25,189,933  ->  30,144,320   1.20x
    string eq, unique per span   16,104,540  ->  30,144,320   1.87x

### Q2 — an attribute-only search: `{ span.http.status_code >= 500 }`

```sql
-- today   golden/traces_search/val_num_range.sql:5-12, byte for byte
SELECT trace_id, max(timestamp_ns) AS bound_ts
FROM trace_attrs_idx
WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15')
  AND timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000
  AND (key = 'http.status_code' AND val_num >= 500 AND scope = 'span')
GROUP BY trace_id ORDER BY bound_ts DESC, trace_id ASC LIMIT 100001

-- new: the same shape, against the trace-grained table
SELECT trace_id, max(ts_max) AS bound_ts
FROM trace_attr_traces
WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15')
  AND (key = 'http.status_code' AND val_num >= 500 AND scope = 'span')
  AND bucket >= 5666666 AND bucket <= 5666702
  AND ts_max > 1700000000000000000
GROUP BY trace_id ORDER BY bound_ts DESC, trace_id ASC LIMIT 100001
```

The absent `ts_max <= <end>` is the same rule Q0 states: an upper bound on a
bucket-grained maximum drops traces the window contains.

Same sorted prefix, same seek, **2.5× fewer rows** — the trace-grain collapse
factor `d` (Appendix A). **Measured** at 2.96×: 2,015,232 rows / 49,578,272 bytes /
246 marks against 679,936 / 19,685,496 / 83, both returning 100,001 rows. Uncapped, the
new candidate set is a strict superset of the old — 166,664 traces become 166,667,
**0 lost and 3 gained**.

### Q3a — `{ status = error }`

```sql
-- today   golden/traces_search/status_only.sql:5-11, byte for byte
SELECT trace_id, max(timestamp_ns) AS bound_ts
FROM trace_spans
WHERE timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000
  AND (status_code = 2)
GROUP BY trace_id ORDER BY bound_ts DESC, trace_id ASC LIMIT 100001

-- new
SELECT trace_id, max(timestamp_ns) AS bound_ts
FROM trace_error_spans
WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15')
  AND timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000
GROUP BY trace_id ORDER BY bound_ts DESC, trace_id ASC LIMIT 100001
```

Whole-window scan becomes a time-ordered read of a table that holds only the
error spans: **100× fewer rows** at `σ_err` = 1%. **Measured** at exactly 100×:
2,000,000 rows / 50,000,488 bytes / 248 marks against 20,000 / 520,016 / 3, both
returning the same 19,999 traces — identical sets, not a superset (26.3.29.7; `use_query_condition_cache=0`, `optimize_move_to_prewhere=1`, `max_block_size=65409`, `max_threads=auto(16)`; 3 reps, zero spread; corpus C1).

### Q3b — `{ name = "GET /pay" }`

```sql
-- today: identical text on both designs; only the plan moves
SELECT trace_id, max(timestamp_ns) AS bound_ts
FROM trace_spans
WHERE timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000
  AND (name = 'GET /pay')
GROUP BY trace_id ORDER BY bound_ts DESC, trace_id ASC LIMIT 100001
```

Today no table in the family is sorted by `name`, so this reads the whole window.
The `name_time` projection re-sorts every row by `(name, timestamp_ns)`, and
ClickHouse's optimiser selects it for this predicate without the SQL changing:
**50× fewer rows** at `σ_name` = 2%. **Measured** at 35×, where one span name in fifty
is 2% of 2,000,000 spans but a granule holds 8,192 rows:
2,000,000 / 50,000,488 / 248 against 57,142 / 1,428,566 / 7, both returning the same
39,998 traces (26.3.29.7; `use_query_condition_cache=0`, `optimize_move_to_prewhere=1`, `max_block_size=65409`, `max_threads=auto(16)`; 3 reps, zero spread; corpus C1). `EXPLAIN indexes = 1`, same instrument, prints
`ReadFromMergeTree (name_time)` and, on its `PrimaryKey` block, `Granules: 7/248` — the
projection is selected rather than assumed. **The `PrimaryKey` block is the one to read:
the same output also prints `Granules: 248/248` twice above it, for the MinMax and
Partition blocks.**

### Q4 — the tag dropdown

```sql
-- Q4a values for one key, today   (tags_sql.rs:118-127) - no time bound at all
SELECT DISTINCT val, val_type FROM trace_tag_catalog
WHERE key = 'http.status_code' AND scope = 'span'
ORDER BY val, val_type LIMIT 1001

-- Q4a values for one key, new     - one added line
SELECT DISTINCT val, val_type FROM trace_tag_catalog
WHERE key = 'http.status_code' AND scope = 'span'
  AND date >= toDate('2023-11-14') AND date <= toDate('2023-11-15')
ORDER BY val, val_type LIMIT 1001

-- Q4c values narrowed by a service, today (tags_sql.rs:282-312): the semi-join
--    of §2.4 - measured 2,138,112 rows / 74.6 MB at 2M spans
-- Q4c values narrowed by a service, new: a primary-key seek, no join
SELECT DISTINCT val, val_type FROM trace_tag_catalog
WHERE scope = 'span' AND key = 'http.status_code' AND service = 'cart'
  AND date >= toDate('2023-11-14') AND date <= toDate('2023-11-15')
ORDER BY val, val_type LIMIT 1001
```

A dropdown narrowed by something other than a service still has no cheap path —
§10.

### Q5 — trace by id

```sql
-- identical in both designs   sql.rs:16-26, byte-frozen against docs/schemas.md §4.2
SELECT trace_id, span_id, parent_id, payload_type, kind, payload
FROM trace_spans WHERE trace_id = unhex('4bf92f3577b34da6a3ce929d0e0e4736')
```

**The id above is illustrative and is not in the corpus** — it returns 0 result rows,
16,384 read rows, 262,160 bytes, 2 marks, identically on both shapes (26.3.29.7; `use_query_condition_cache=0`, `optimize_move_to_prewhere=1`, `max_block_size=65409`, `max_threads=auto(16)`; 3 reps, zero spread; corpus C1).
Re-measured with an id that IS in the corpus,
`000018d4dfe8a7a22d8f5b96d0ac2759`: **8,192 rows / 136,124 bytes / 1 mark / 12 result
rows, identical on both shapes** (26.3.29.7; `use_query_condition_cache=0`, `optimize_move_to_prewhere=1`, `max_block_size=65409`, `max_threads=auto(16)`; 3 reps, zero spread; corpus C1). `EXPLAIN indexes = 1` reports
`PrimaryKey … Parts: 1/2, Granules: 1/248`. This is the
latency-critical read and nothing here touches it.

**The earlier figure `1 granule of 245, 8,192 rows, 132,564 bytes` was taken on a corpus
this document no longer describes and by a method it no longer accepts. It is withdrawn,
not carried.**

### Q6 — a metrics query: `{ duration > 1s } | rate() by(resource.service.name)`

```sql
-- today   golden/traces_metrics/rate_by_service.sql:5-11, byte for byte
SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns - 1),
       INTERVAL 60000000000 NANOSECOND)) + 60000 AS t, service AS g0,
       uniqExact(trace_id, span_id) AS n
FROM trace_spans
WHERE timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
  AND duration_ns > 1000000000
GROUP BY t, g0 ORDER BY t ASC, g0

-- new: THE SAME STATEMENT. This shape is not pre-aggregated, and
-- section 3.5 gives the reason: sum(count) over a rollup and
-- uniqExact(trace_id, span_id) over the spans differ the moment one
-- span row is written twice.
```

**Unchanged, deliberately.** This is the one shape of the nine that keeps its
full-window scan. §3.5 prices the rollup that was rejected and names the two
things that would make it possible.

A metrics query with an attribute filter is a semi-join today
(`golden/traces_metrics/attr_semi_join.sql`); the attribute test becomes inline:

```sql
-- today
… AND (trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx
      WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15')
        AND timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
        AND key = 'http.status_code' AND val_num >= 500 AND scope = 'span')
-- new
… AND arrayExists((key, scope, val_num) ->
                  (key = 'http.status_code' AND val_num >= 500 AND scope = 'span'),
                  attr_key, attr_scope, attr_num)
```

**The 20,000,000-span pair that stood here — "3.9× more bytes … 1.7–2.3× faster
(1436/1543/1627 ms against 2442/3659/3479 ms)" — is withdrawn.** Its method was not
recorded, the corpus no longer exists, and the same two statements re-measured at
2,000,000 spans give a different byte ratio. It is not replaced by an estimate.

**Measured at 2,000,000 spans.** Instrument: ClickHouse 26.3.29.7,
`use_query_condition_cache = 0`, `optimize_move_to_prewhere = 1`,
`max_block_size = 65409`, `max_threads = auto(16)`, three repetitions, zero counter
spread:

| statement | read_rows | read_bytes | marks | result rows | ms |
|---|---|---|---|---|---|
| Q6, `\| rate() by(service)`, today | 2,000,000 | 82,001,456 | 248 | 3,620 | 83/80/74 |
| Q6, same statement, new tables | 2,000,000 | 82,001,456 | 248 | 3,620 | 77/82/84 |
| Q6b, the semi-join, today | 4,015,232 | 121,706,304 | 494 | 181 | 429/416/458 |
| Q6b, the inline probe, new | 2,000,000 | 288,001,456 | 248 | 181 | 154/153/144 |

So at this corpus size the inline form reads **2.37×** the bytes and is **2.7–3.0×
faster**.

**Where the earlier 34,000,000 and 240,000,000 came from.** They were taken through a
`SELECT count() FROM ( <the statement> )` wrapper. `EXPLAIN header = 1` shows what that
costs: the wrapper's `ReadFromMergeTree` header is `timestamp_ns, service`, the bare
statement's is `timestamp_ns, service, trace_id, span_id`. The optimiser drops the two id
columns because nothing outside the subquery reads `uniqExact(trace_id, span_id)`.

The arithmetic, so it is not asserted as rounder than it is:

    bare Q6                                       82,001,456
    the same statement inside count()             34,000,000
    difference                                    48,001,456

    measured on their own, same corpus and instrument
      SELECT uniqExact(trace_id) FROM spans_old   32,000,320
      SELECT uniqExact(span_id)  FROM spans_old   16,000,320
      SELECT service, count() … GROUP BY service   2,000,000
                                                  ----------
      trace_id + span_id                          48,000,640

**The residue is 816 bytes, and it is not attributed.** The mechanism is established —
the wrapper does not read the two id columns — the exact total is not, and no round
`24 × 2,000,000` identity holds. **A counter taken through a wrapper is a counter for the
wrapper.** Every `[M]` figure in this document was taken by running the statement itself.

### Q7 — the service graph

Reads `trace_edges`, which this design does not touch. The statement is
byte-identical (`golden/traces_graph/single_node.sql`). **The earlier counters,
1,516,384 rows and 65,764,280 bytes, are withdrawn: their method was not recorded and
their corpus no longer exists.** Measured on the corpus above, where the edge ledger holds
1,200,000 half-rows: 1,216,384 rows / 51,624,512 bytes / 149 marks, returning 2 edges
(26.3.29.7; `use_query_condition_cache=0`, `optimize_move_to_prewhere=1`, `max_block_size=65409`, `max_threads=auto(16)`; 3 reps, zero spread; corpus C1).

---

## 5. What it costs and what it saves

Per dimension. **[D]** = derived from the files cited, by the calculator in
Appendix B. **[M]** = measured on ClickHouse 26.3 at 2,000,000 and 20,000,000
spans.

| # | dimension | today | new | change | |
|---|---|---|---|---|---|
| 1 | storage, B/span, at `A` = 20 | 1047.9 | **625.7** | **−40.3%** | [D] |
| 1 | storage, B/span, **measured** at `A` = 8 and `Z_p` = 15.91 on corpus C1; `sum(bytes_on_disk)` over `system.parts` after `OPTIMIZE … FINAL`, both span tables built identically, ClickHouse 26.3.29.7 | 306.6 | **212.9** | **−30.6%** | [M] |
| 1 | … the payload component of each, so `Z_p` can be substituted: today 180,471,375 B (base 51,292,209 + a second copy of 129,179,166 in `service_time`), new 51,292,209 B (its two projections carry none). Payload-free: today 216.4 B/span, new 187.2 B/span | | | | [M] |
| 1 | storage at 10⁹ spans/day, 7 days | 7.34 TB | **4.38 TB** | −2.96 TB | [D] |
| 2 | rows read, `{}` | 4.17·10⁷ | 3.48·10⁶ | **÷12** | [D] |
| 2 | rows read, `{status = error}` | 4.17·10⁷ | 4.17·10⁵ | **÷100** | [D] |
| 2 | rows read, `{name = "…"}` | 4.17·10⁷ | 8.33·10⁵ | **÷50** | [D] |
| 2 | rows read, `\| rate() by(service)` | 4.17·10⁷ | 4.17·10⁷ | **unchanged** — §3.5 | [D] |
| 2 | rows read, attribute search | 2.08·10⁷ | 8.33·10⁶ | **÷2.5** | [D] |
| 2 | rows read, the tag dropdown | 10⁶ and rising with deployment age | 10⁴ | **÷100, and bounded** | [D] |
| 2 | rows read, the narrowed dropdown (26.3.29.7; `use_query_condition_cache=0`, `optimize_move_to_prewhere=1`, `max_block_size=65409`, `max_threads=auto(16)`; 3 reps, zero spread; corpus C1) | 2,129,920 rows / 72,453,720 B / 260 marks | 16,384 / 51,826 / 2 | **÷130 on rows, ÷1398 on bytes**, service-narrowed only. Both return `DELETE POST PUT` | [M] |
| 2 | rows read, trace-by-id and the service graph (26.3.29.7; `use_query_condition_cache=0`, `optimize_move_to_prewhere=1`, `max_block_size=65409`, `max_threads=auto(16)`; 3 reps, zero spread; corpus C1) | Q5 8,192 / 136,124 / 1; Q7 1,216,384 / 51,624,512 / 149 | Q5 identical; Q7 untouched table | **identical** | [M] |
| 3 | bytes, storage → reader, one search batch, **first-seen** (26.3.29.7; `use_query_condition_cache=0`, `optimize_move_to_prewhere=1`, `max_block_size=65409`, `max_threads=auto(16)`; 3 reps, zero spread; corpus C1), 5 reps | 64,332,690 | **30,144,320** | **−53%** | [M] |
| 3 | … the same batch, **repeated identically** (26.3.29.7; `use_query_condition_cache=1`, `optimize_move_to_prewhere=1`, `max_block_size=65409`, `max_threads=auto(16)`; corpus C1; runs 2 and 3 after `SYSTEM DROP QUERY CONDITION CACHE`) | 11,064,138 | 26,417,997 | +139% | [M] |
| 3 | bytes, writer → ClickHouse | 1838 raw B/span, 2 statements | 1038, 1 | **−43.5%** | [D] |
| 3 | rows crossing to every replica (the catalog is `Replication::Global`, `catalog.rs:406`) | 20 per span | ≤ distinct tuples per block | **≈500×** | [D] |
| 3 | bytes, reader → client | — | — | **unchanged** — set by the API response shape, not by storage | [D] |
| 4 | SQL statements, one-condition search, `M`=20 | 4 | **3** | −25% | [D] |
| 4 | … a search that compares an `event:`/`link:` intrinsic against another field | | **keeps its extra per-batch statement** — §4 Q1 says why the multi-valued read cannot become a column | [D] |
| 4 | … at the candidate ceiling | 6252 | **3127** | −50% | [D] |
| 5 | ClickHouse CPU | tracks the uncompressed bytes of the selected columns | | ÷2.5 on attribute search; ÷2.1 on a first-seen search batch; 2.7–3.0× **faster** on an attribute metrics query at 2M (26.3.29.7; `use_query_condition_cache=0`, `optimize_move_to_prewhere=1`, `max_block_size=65409`, `max_threads=auto(16)`; 3 reps, zero spread; corpus C1). The 20,000,000-span claim is withdrawn with the figures it rested on | [M] |
| 6 | our own CPU | 66 statements, 28,384 rows decoded at `M`=1000 | 34 statements, 25,312 rows | **strictly lower** | [D] |
| 7 | disk read work | tracks the compressed bytes of the selected columns | | as row 2 and row 3 | [D] |
| 8 | merge, LZ4-equivalent B/span/level | 9856 | **5341** | **−45.8%** | [D] |
| 8 | write wall time, 20,000,000 spans, four takes | 387.6 / 500.4 / 441.3 / 337.4 s | 200.1 / 272.8 / 239.7 / 304.0 s | one statement was faster in **all nine takes** at both corpus sizes, margin 1.02×–2.48× | [M] |

The write-time takes were taken on a machine carrying a load average between 11
and 29 from other work. No ratio is claimed; the direction is what all nine
agree on.

### 5.1 Where the storage goes

```
   today  1047.9 B/span                    new  625.7 B/span
   one # is about 36 bytes

   base            122.9 ###               base + arrays      253.3 #######
   service_time    137.6 ####              service_time        37.6 #
   trace_attrs_idx 787.3 ######################  name_time     37.6 #
                                           trace_attr_traces  294.8 ########
                                           error table          0.5
                                           recency              1.9
```

### 5.2 Where the two designs cross over

| the parameter | crossover | which side wins |
|---|---|---|
| `A_t`, distinct attribute values per trace | the new layout stops being smaller at `A_t` = **246.5**. `A_t` can never exceed `A·S` = 240 | **the new layout is smaller at every parameter value.** Even in the degenerate case where no attribute value repeats anywhere in a trace it is 1027.9 B/span against 1047.9 |
| `σ_err`, the fraction of spans in error | `trace_error_spans` costs `σ_err·(37.6 + 16)` B/span and reads `σ_err·N_W` rows. It stops being cheaper than the base table at `σ_err` = 1 | at 1% it is 0.5 B/span for a 100× read reduction; a deployment where most spans are errors gets neither |
| `σ_name`, the fraction of spans sharing one span name | `name_time` costs a flat 37.6 B/span and reads `σ_name·N_W` rows | it is the most expensive of the three additions and the only one that is a full re-sorted copy. At `σ_name` = 1 — one span name in the whole deployment — it buys nothing and still costs 37.6 |
| `n_k`, attribute-index **rows** a batch's membership read touches | the search batch's byte cost crosses at `n_k` ≈ **9.5·10⁵** | below it, two statements read fewer bytes. **Measured on a first-seen batch, §4 Q1's corpus and instrument: `n_k` = 2,015,232 rows** — above the crossover, so the new design **wins** this dimension there. The earlier reading of 24,576 rows is what the same statement returns on a repeat, once the query-condition cache has memoised its granules; a list not seen before does not hit, measured with the cache warm from another list. `n_k` moves with the probed value's selectivity: 2,015,232 rows for a numeric range, 516,096 for a four-value string equality, 16,384 for a value unique to one span |

---

## 6. The data contract

**What a value is, on each side.** Types printed with `toTypeName`, not inferred
from names.

| value | today | new | first disagreement | verdict |
|---|---|---|---|---|
| attribute key | `LowCardinality(String)` column | `Array(LowCardinality(String))`, element reads as `String` | none — string equality is byte equality either way | **agree** |
| attribute scope | `LowCardinality(String)` | element reads as `String` | none | **agree** |
| attribute value text | `String` | `String` | none | **agree** |
| declared OTLP type | `LowCardinality(String)` | element reads as `String` | none | **agree** |
| numeric value | `Nullable(Float64)` | `Nullable(Float64)` | none — same width, same NULL rule, same `toFloat64OrNull` source | **agree** |
| the comparison `val_num >= 500` | `Nullable(UInt8)` | `Nullable(UInt8)` | none | **agree** |
| the PROBE's result, as the reader sees it | a row present or absent in the membership set | `arrayExists(…)`, printed `UInt8` | none on the row set: `arrayExists` returns 0 exactly where the column form returns NULL, and a `WHERE` treats NULL as 0. Measured for `>=`, `!=` and `=` against a NULL element, all three 0 | **agree** |
| the probe under NEGATION | positive probe, reader inverts (`search_eval.rs:1213-1216`, `member != *negated`) | **must stay exactly that** | negating inside `arrayExists` differs on an absent key and on a multi-valued key — §4 Q1's five-case table | **agree only if the negation stays out of the SQL** |
| the value a DUPLICATED key yields | `any(val)` / `any(val_num)` over `GROUP BY (trace_id, span_id)` — arbitrary, not stable across merges | **locate on `(key, scope)` only, then read that element**; scope precedence span → resource → event → link → instrumentation | on `['7','5']`: today returned 5 in one measurement, the new form returns 7. On `['bad','5']`: today's numeric read returns 5, the new form returns NULL, because the FIRST match is not numeric | **CHANGED, deliberately.** The new rule is the reference's, `block_traceql.go:128-152, 248-275 @ v3.0.2`; today's has no contract. Needs a ledger row and a differential test |
| `val_num`'s determinant | — | `(scope, key, val)`, **not `val` alone** | `link:spanID` = `'0000000000000001'` stores `val_num = NULL` while the same text under an attribute key stores `1.0` (`otlp_traces.rs:558-607` sets `val_num: None` unconditionally for both link intrinsics) | **determined**, and all three columns are in the sorting key |
| `timestamp_ns`, `duration_ns` | `Int64` nanoseconds | `Int64` nanoseconds | none | **agree** |
| the bucket | — | `UInt32` | ingest bounds `timestamp_ns` to `[0, 4.29·10¹⁸]` (`otlp_traces.rs:465-486`), so the bucket is `≤ 1.43·10⁷` against a ceiling of 4.29·10⁹ | **cannot overflow** |

**Measured, not argued**: on a 2,000,000-span corpus, all 6,000,000 non-NULL
numeric attribute values compared bitwise equal between the two layouts —
`countIf(a.bits = b.bits)` returned 6,000,000 of 6,000,000, reading the bits with
`reinterpretAsUInt64`.

**The 2⁵³ boundary is exactly where it is today.** `val.parse::<f64>()`
(`otlp_traces.rs:712-714`) already rounds `9007199254740993` to
`9007199254740992` before anything is stored, and the new layout parses the same
text with the same function into the same `Float64`. This design neither improves
nor worsens that, **and that is why there are five arrays and not six**: adding
`attr_val_i64 Array(Nullable(Int64))` would make integer comparisons above 2⁵³
answer differently from today, which is a change of answer, not a change of
storage. If that answer should change, it should change on its own.

### 6.1 Which rows does each query return, before and after?

| kind of stored value | example | today | new | same rows? |
|---|---|---|---|---|
| a string attribute | `http.method = "GET"` | index row per span, `val = 'GET'` | one row per (value, trace, bucket), `val = 'GET'` | candidates a **superset**; final answer **identical** |
| a numeric attribute | `http.status_code >= 500` | `val_num >= 500` on the index | `val_num >= 500` on the trace-grained table | same, same `f64` semantics |
| text that looks numeric but was sent as a string | `build.id = "500"` | `val_num` is set from the text regardless of the declared type | same function, same column | **identical** |
| an event or link intrinsic | `event:name`, `link:spanID` | its own scope, one row per span | same tuple, trace grain | as the first row |
| the tag dropdown's rows | any key ever ingested | every tuple ever seen | tuples seen in the retention window | **changed, deliberately** — a value last seen 400 days ago stops appearing. That is what every other endpoint already does |

**Where the two candidate generators first disagree**, as a case rather than a
description:

```
   window  (lo, hi]
   |----------------------|
       span X at t < lo, carries http.status_code = 500   } same trace T
       span Y at t in (lo, hi], does NOT carry it         }

   today       T is NOT a candidate (no index row inside the window)
   new         T IS a candidate     (ts_max > lo, its bucket overlaps)
   phase 2     hydrates T, evaluates the test over T's in-window spans,
               finds no match -> T is dropped
   answer      IDENTICAL
```

The superset is bounded by one bucket, not two:

```
   buckets overlapping (lo, hi]   |--B--|--B--|--B--|--B--|
   the window                         (lo................hi]

   leading edge   a trace whose matching spans are ALL before lo has
                  ts_max <= lo and is filtered out       -> no widening
   trailing edge  a trace whose matching spans are all after hi is
                  admitted                               -> the widening

   candidates cover at most  W + B  of span-time instead of W
        factor  1 + B/W   =  x2.00 at W = B = 5 min
                          =  x1.08 at W = 1 h, B = 5 min
```

**The superset holds only if no upper time bound is applied to a bucket-grained
maximum.** §4 Q0 measures what happens when one is: 17 of 83,334 traces disappear, which
is a wrong answer rather than a wider one. Neither Q0 nor Q2 carries such a bound.

**Two places where "more candidates" is not free.** Both need a test.

1. `traceql_max_candidates = 100_000` (`config/model.rs:514`). A query sitting
   just under the ceiling today can now hit it, and hitting it is reported as a
   partial result. The test uses a corpus of exactly 100,000 candidate traces for
   one value, one query at `W = B` and one at `W = B/2`, and asserts the returned
   trace set and the partial flag on both designs.
2. `bound_ts` becomes `max(ts_max)` over overlapping buckets rather than
   `max(timestamp_ns)` over in-window matching spans, so it is **≥** today's
   value. It is still an upper bound on the trace's sort key, so early
   termination stays correct — the search stops later, never earlier. It does
   change the **order** candidates are consumed in, and therefore which ones are
   dropped when the ceiling engages. The test uses two traces whose `bound_ts`
   differ by one nanosecond, one of them with a matching span before the window,
   and asserts the returned order.

### 6.2 One pushdown is lost, and the answer does not move

Three aggregate conditions are pushed into the candidate generator today
(`compile.rs:451-462`): `count() > n` as `uniqExact(span_id)`, and
`max(duration) > t` / `min(duration) < t` as `max(duration_ns)` /
`min(duration_ns)`.

| pushdown | new | why |
|---|---|---|
| `max(duration) > t` | **keeps pushing**, against `dur_max` | the aggregate is carried on the row |
| `min(duration) < t` | **keeps pushing**, against `dur_min` | as above |
| `count() > n` | **stops pushing** | it is `uniqExact(span_id)` and there is no `span_id` at trace grain |

Losing a pushdown does not change an answer. The condition is re-evaluated over
the hydrated spans either way; the pushed form only narrows the candidate list,
and the plan already keeps a byte-for-byte fallback statement with nothing pushed
(`search_plan.rs:3072-3092`, used at `exec.rs:2163-2191`). The effect is more
candidates, not a different result.

`by()` grouping already refuses to push whenever the generator is not
`trace_spans` (`compile.rs:560-562`), so an attribute-generated search behaves
exactly as it does today. Its reason improves: today the refusal is needed
because `trace_attrs_idx` is a `ReplacingMergeTree` whose sort key omits
`duration_ns`, so a merge picks one of two values arbitrarily; on the new table
those values are explicit `min`/`max` aggregates and the arbitrary choice is gone.
The refusal stands on the simpler ground that there is no span row to group.

### 6.3 One class of wrong answer disappears, and a wider one takes its place

Today a span counts as matching an attribute if and only if an index row exists
for it — and §2.5 lists three ways the two tables can permanently disagree about
that. In the new design the attribute test reads the span's own row, so **that
particular disagreement cannot occur**: the arrays are columns of the span row, and a
span row that exists carries its attributes.

**§11 P3 predicted that this removed the class. It does not.** P3's prediction was that
a materialized view which throws fails the whole `INSERT`, so nothing is stored rather
than half. Measured on ClickHouse 26.3.29.7, stock config, `async_insert = 0` (the
writer's pin, `client.rs:137`), with a view built to throw on one row of a two-row block:

    client                             Code: 395 … while pushing to view mv_throw
    SELECT count() FROM src            2
    system.parts                       20231114_1_1_0   2 rows   active = 1
    SELECT count() FROM tgt            0

The span rows are stored in an active part and are visible to `SELECT`; the throwing
view's target is empty.

**An earlier version of this section said the boundary is "source committed, no view
committed". That is wrong, and the correct statement is worse.** Measured with one
throwing view and **three** healthy sibling views, on twenty fresh databases, stock
config with `async_insert = 0`, `parallel_view_processing = 0`,
`materialized_views_ignore_errors = 0`, one two-row block per trial of which the second
row makes the view throw:

    outcome (src / throwing target / healthy b / healthy c / healthy d)   trials
    2 / 0 / 0 / 0 / 0     no healthy sibling committed                      14
    2 / 0 / 0 / 2 / 0     one committed                                      2
    2 / 0 / 2 / 0 / 0     one committed                                      1
    2 / 0 / 0 / 0 / 2     one committed                                      1
    2 / 0 / 2 / 2 / 2     all three committed                                2

What holds on **every** trial: the source rows are committed, and the throwing view's own
target is empty. What does **not** hold: that the other views commit nothing. **Each
healthy sibling independently may or may not commit, and which ones do varies between
runs of the identical statement.** So the failure leaves **partial derived state**, not
no derived state.

**How many trials it takes to see it.** 6 of 20 trials showed at least one healthy
sibling committing, so a single trial misses it about 70% of the time. At that rate nine
trials give about 95% and thirteen about 99%. Twenty trials pin the rate itself only to
roughly 12–54%, so those trial counts are a working figure, not a measured bound.

The failure therefore leaves the span fetchable by id and **absent from
`trace_attr_traces`, `trace_recent`, `trace_error_spans` and `trace_tag_catalog`** —
invisible to every search shape. Today's equivalent failure (§2.5 row 1) removes only
the attribute index. `trace_spans` passes `on_flush_poisoned: None`
(`writer/trace.rs:172`), the structural append-only exclusion (`backfill.rs:23-28`), so
nothing replays it.

One further reading, same server: with the source at
`non_replicated_deduplication_window = 100`, inserting the identical block twice left the
source at 1 row and moved the view target from 2 rows to 4. A retry after a view failure
does re-run the views even where the source block deduplicates — and it writes view rows
a second time, which is what §3.4's collapse rules absorb.

**The remedy is not in this document.** It is a decision about the write path: a repair
pass over spans whose derived rows are absent, `materialized_views_ignore_errors` plus an
explicit repair, view bodies that cannot throw, or accepting and documenting it.

---

## 7. What we did not measure

Stated here rather than left to be found.

**P1 to P4 have been read since this table was written; §11 carries what each returned.**
What remains unmeasured:

| not measured | why it matters |
|---|---|
| whether the source-committed/no-view-committed boundary holds for all **five** proposed views | it was established for one throwing view beside one healthy view. §6.3 |
| `d`, the trace-grain collapse factor, on real traces | it scales the whole index saving. §11 P5 |
| the byte cost of the `event_set_sql` read after it moves to an `ARRAY JOIN` over `trace_spans` | it is the one phase-2 read that stays a separate statement. §4 Q1 |
| the scalar value read (`arrayFirstIndex` + element extraction) against today's `attr_values_sql` | the shape is bounded by construction; the byte cost is not measured |
| whether `Array(LowCardinality(String))` and `Array(Nullable(Float64))` insert through our own writer | `metric_hist_samples` proves `Array(Int32)`/`Array(Float64)` from a `Vec` field (`catalog.rs:492-498`, `rows.rs:437, 443`); the low-cardinality and nullable element types have no precedent in this repository |
| the cost of the drop/add/materialise interval on `service_time` (§8 ids 55–57) on a populated table | during it, a `resource.service.name` search falls back to a base-table scan. Empty on a fresh database |
| the clustered path beyond column presence | §8's twins were measured on a single-node `Distributed('default', …)`: the column appears and the insert lands. Multi-shard routing, `cityHash64(trace_id)` co-sharding of the four new wrappers, and a clustered read were not measured |
| storage at Appendix A's `Z_p` = 4 | §5's measured storage row was taken at `Z_p` = 15.91 and carries its payload component so the figure can be re-derived at another `Z_p`; it was not re-run at 4 |
| concurrency, and ClickHouse's mark, uncompressed and query-condition caches | every figure here is one request on an idle server; a repeated query is cheaper than this says |
| the write path's own CPU | building five array fields instead of `A` separate rows is almost certainly cheaper, and is not counted |
| the eleven compression ratios in Appendix A | they are judgement. Every worked byte figure moves with them; the **signs** of the crossovers in §5.2 survive the whole stated range, the magnitudes do not |
| whether each new table really is safe against a duplicated span row (§3.4) | it is derived from the engine's own collapse rules and from `max`/`min` being unchanged by repeating a value, not observed. §11 P11 is the reading, and it is one insert repeated |

---

## 8. How the change happens

**Nothing has ever shipped. There is no data to move and no compatibility to
keep.** That is not a detail; it removes most of the work.

| what a live deployment would need | what we need |
|---|---|
| an `ALTER` sequence that keeps queries answering throughout | nothing — the tables are created in their final form |
| a backfill for existing parts | nothing — there are no existing parts |
| a dual-read period while the new index fills | nothing |
| a rollback plan for stored data | nothing |
| a staged rollout, one option at a time | one change |

**Amending migration 16 and 18 in place is not currently permitted.** Three places say
the migration catalogue is append-only and that the window for in-place amendment closed:

> Migrations are idempotent, and append-only from the first tagged release onward —
> in-place amendment of an already-listed migration was permitted only pre-release (the
> trace-index scope amendment, issue #54, was the last such window; see schemas.md §6)
> — `docs/architecture.md:96`

> the trace-index scope amendment (issue #54) was the last such amendment window
> — `docs/schemas.md:882`

> issue #54's scope amendment of migrations 17/18 + `trace_tag_catalog_mv` was the last
> such amendment window (task-manager ruling on #54) — `crates/pulsus-schema/src/catalog.rs:16-23`

There has been no tagged release, so the policy's stated trigger has not fired; what
closed the window was a ruling. Reopening it needs another ruling and an edit to all
three places.

**The change does not need that ruling.** It is expressible entirely as new,
append-only migrations, and the sequence was run end to end on ClickHouse 26.3.29.7
against a `trace_spans` built from today's migration 16 plus migrations 31/35/37/42/43
and populated with 50,000 rows. Every statement returned HTTP 200 with an empty body,
and the row count was 50,000 before and after:

```
  id  scope              statement
  --  -----------------  --------------------------------------------------------------
  44  PerShard           ALTER TABLE trace_spans ADD COLUMN IF NOT EXISTS attr_key    Array(LowCardinality(String))
  45  PerShard, CLUSTER  ALTER TABLE trace_spans_dist ADD COLUMN IF NOT EXISTS attr_key    Array(LowCardinality(String))
  46  PerShard           ALTER TABLE trace_spans ADD COLUMN IF NOT EXISTS attr_scope  Array(LowCardinality(String))
  47  PerShard, CLUSTER  ALTER TABLE trace_spans_dist ADD COLUMN IF NOT EXISTS attr_scope  Array(LowCardinality(String))
  48  PerShard           ALTER TABLE trace_spans ADD COLUMN IF NOT EXISTS attr_val    Array(String)
  49  PerShard, CLUSTER  ALTER TABLE trace_spans_dist ADD COLUMN IF NOT EXISTS attr_val    Array(String)
  50  PerShard           ALTER TABLE trace_spans ADD COLUMN IF NOT EXISTS attr_type   Array(LowCardinality(String))
  51  PerShard, CLUSTER  ALTER TABLE trace_spans_dist ADD COLUMN IF NOT EXISTS attr_type   Array(LowCardinality(String))
  52  PerShard           ALTER TABLE trace_spans ADD COLUMN IF NOT EXISTS attr_num    Array(Nullable(Float64))
  53  PerShard, CLUSTER  ALTER TABLE trace_spans_dist ADD COLUMN IF NOT EXISTS attr_num    Array(Nullable(Float64))
  54  PerShard           ALTER TABLE trace_spans ADD CONSTRAINT IF NOT EXISTS attr_arrays_aligned CHECK …
                         (no _dist twin — see below)
  55  PerShard           ALTER TABLE trace_spans DROP PROJECTION IF EXISTS service_time
  56  PerShard           ALTER TABLE trace_spans ADD PROJECTION IF NOT EXISTS service_time (<14 named columns> ORDER BY (service, timestamp_ns))
  57  PerShard           ALTER TABLE trace_spans MATERIALIZE PROJECTION service_time
  58  PerShard           ALTER TABLE trace_spans ADD PROJECTION IF NOT EXISTS name_time    (<the same 14>  ORDER BY (name, timestamp_ns))
  59  PerShard           ALTER TABLE trace_spans MATERIALIZE PROJECTION name_time
  60  Global             DROP TABLE IF EXISTS trace_tag_catalog        <- PARTITION BY and ORDER BY
  61  Global             CREATE TABLE trace_tag_catalog (<the new shape>)  cannot be ALTERed
  62  PerShard, CLUSTER  DROP TABLE IF EXISTS trace_attrs_idx_dist
  63  PerShard           DROP TABLE IF EXISTS trace_attrs_idx
  64  PerShard           CREATE TABLE trace_attr_traces   (AggregatingMergeTree)
  65  PerShard, Dist     CREATE TABLE trace_attr_traces_dist   AS trace_attr_traces
                         ENGINE = Distributed('{cluster}', {db}, trace_attr_traces, cityHash64(trace_id))
  66  PerShard           CREATE TABLE trace_error_spans   (ReplacingMergeTree)
  67  PerShard, Dist     CREATE TABLE trace_error_spans_dist   … cityHash64(trace_id)
  68  PerShard           CREATE TABLE trace_recent        (AggregatingMergeTree)
  69  PerShard, Dist     CREATE TABLE trace_recent_dist        … cityHash64(trace_id)
```

`PerShard, CLUSTER` is `Ddl::StaticClusterOnly`: skipped and unrecorded on a single node,
applied the first time clustering is enabled. `PerShard, Dist` is `Ddl::Dist`, rendered
by `render::dist_ddl_template` from `Family::Traces`'s single sharding expression, so all
four trace wrappers co-shard on `cityHash64(trace_id)` and every read joins shard-locally
(§7). `Global` is the catalogue's one cluster-wide replica set, no wrapper.

**One grammar note, because every example in this section is a statement someone will
paste.** `SETTINGS` goes **before** `VALUES` in an `INSERT`, or in the HTTP query string.
After `VALUES` it is parsed as row data and rejected. Measured on 26.3.29.7:

    INSERT INTO t VALUES (1) SETTINGS async_insert=0   HTTP 400  Code: 27  Cannot parse input: expected '(' before: 'SETTINGS …
    INSERT INTO t SETTINGS async_insert=0 VALUES (2)   HTTP 200
    INSERT INTO t VALUES (3)   with ?async_insert=0    HTTP 200

The rejection code depends on where the parser gives up — `Code: 27` here, `Code: 62`
where the row text differs — but it is an HTTP 400 either way, and it is easy to read as
"the setting was applied and the insert failed" when the setting was never seen.

**The odd ids are not decoration.** `trace_spans_dist` is
`CREATE TABLE … AS trace_spans`, which copies the column list at creation and **does not
inherit a later base-table `ALTER`**. Migration 32's own comment says so, and in cluster
mode every read and write goes through the wrapper. Measured on 26.3.29.7 with a
single-node `Distributed('default', …)`:

    after the base ALTERs only
      local columns        ['attr_key','attr_num']
      distributed columns  []
      INSERT INTO trace_spans_dist (…, attr_key, attr_num) VALUES (…)
        -> Code: 16. DB::Exception: No such column attr_key in table … (NO_SUCH_COLUMN_IN_TABLE)

    after the cluster-only twins
      distributed columns  ['attr_key','attr_num']
      the same INSERT       -> 200, and the row lands in the LOCAL table

**The `CONSTRAINT` gets no twin, and does not need one.** Measured:
`ALTER TABLE trace_spans_dist ADD CONSTRAINT …` returns
`Code: 48 … not supported by storage Distributed (NOT_IMPLEMENTED)`. And the base-table
constraint fires on a wrapper insert anyway — a misaligned row sent to
`trace_spans_dist` returned
`Code: 469 … Constraint attr_arrays_aligned for table <db>.trace_spans is violated at row 1`.
Projections likewise exist only on the base table.

`ADD PROJECTION` followed by `MATERIALIZE PROJECTION` is the pattern migrations 42/43
already use — **but 42/43 add a projection that did not exist, where 55–57 first drop one
that is serving reads.** Between 55 and 57 a `resource.service.name` search falls back to
a base-table scan: a correct answer, a slower one, for as long as the materialise takes.
On a fresh database that interval is empty. The named-column `service_time` requires
`shared`, `status_message`, `scope_name` and `scope_version` to exist, which they do by
the time a new id runs — 31/35/37 have already applied.

**Three of these statements destroy data, and the policy does not stop them.** The
append-only rule constrains mutation of an already-listed migration entry; it says nothing
about what a *new* entry may contain. So ids 60, 62 and 63 are formally allowed and would
drop a populated catalogue and a populated attribute index. **"There is no data to keep"
is the issue's premise, not a property the sequence checks.**

### 8.1 What happens to rows that already exist

Measured, and it is not a migration. Starting from a database reconciled by the
repository's own initialiser (migrations 16/31/35/37/42/43) and populated with 50,000
spans, then running ids 44–69:

    SELECT count(), countIf(all five array lengths = 0) FROM trace_spans
      -> 50000, 50000

    uniqExact(trace_id) in trace_spans                          50000
    SELECT count() FROM trace_recent                                0
    the `{}` generator over trace_recent                            0 traces
    a trace-by-id read for one of those traces                      1 row

**"Returned by no search of any shape" is wrong, and here is the exact split.** Measured
on the same database, by running each shape's generator:

    STILL ANSWER  (read trace_spans' own columns)
      { resource.service.name = "s" }      50,000 traces
      { name = "n" }                       50,000 traces
      { duration > 0 }                     50,000 spans
      trace by id                               1 row
      | rate() by(service)                      1 group

    RETURN NOTHING
      {}                       -> trace_recent        0
      { span.k >= 500 }        -> trace_attr_traces   0
      { status = error }       -> trace_error_spans   0
      the tag dropdown         -> trace_tag_catalog   0
      any attribute condition in phase 2  -> probe0 is 0 on every span, because the
                                             arrays are empty
      { span.k >= 500 } | rate()          -> 0

So a surviving span is reachable by **service, span name, duration and id**, and
unreachable by **the empty search, every attribute condition, `status = error` and the tag
dropdown**. **Two shapes change character** — `{}` and `{ status = error }` both read
`trace_spans` today and both move to a derived table, so both go from answering to
returning nothing. The rest of the "return nothing" column never read `trace_spans`
directly in the first place. Their attributes were
in `trace_attrs_idx`, which id 63 drops.

**Two ways to fix it exist, and both were run.** An earlier version of this section said
there is none; that was false.

| option | measured | cost |
|---|---|---|
| a preaggregated `ENGINE = Join` lookup keyed `(trace_id, span_id)`, then `ALTER TABLE trace_spans UPDATE attr_* = joinGet(…) WHERE length(attr_key) = 0` | `HTTP 200`; arrays populated and aligned | rewrites every affected part; needs the lookup table resident; **the mutation is not transactional with anything** |
| `CREATE TABLE rebuilt AS trace_spans; INSERT INTO rebuilt SELECT … ANY LEFT JOIN <the grouped index>; EXCHANGE TABLES` | `HTTP 200`; 1 row in, 1 row out, aligned | a second full copy on disk, and the exchange has to be coordinated with writers |

**Three things about them that are not obvious and were measured.**

1. **`groupArray` over a `Nullable` column silently drops NULLs and misaligns the
   arrays.** Building the lookup as `groupArray(val_num)` produced `attr_key` of length 2
   against `attr_num` of length 1. Aggregating the whole tuple —
   `groupArray((key, scope, val, val_type, val_num))` then `arrayMap(x -> x.5, …)` —
   preserves the NULL and the alignment: `['j','k']` against `[NULL, 7]`.
2. **A `CHECK` constraint does not run during `ALTER TABLE … UPDATE`.** With
   `attr_arrays_aligned` on the table, a mutation setting `attr_num = [1.0]` against a
   two-element `attr_key` returned `HTTP 200` and left `length(attr_key)=2`,
   `length(attr_num)=1`. The identical row **as an `INSERT`** was rejected with
   `Code: 469 … Constraint attr_arrays_aligned … is violated at row 1`. **A mutation can
   create rows a later insert would reject**, so any backfill must check alignment itself
   after running.
3. **Sender order cannot be recovered.** `trace_attrs_idx` stores no element position
   (`catalog.rs:370-384`), so a backfill must impose an order of its own. Under §4 Q1's
   rule — first match in stored order — a backfilled span's answer for a duplicated key is
   whatever order the backfill chose, not the sender's. **So the rule is defined over two
   populations, and the boundary between them is not visible in the data**: live rows
   answer in the sender's order, backfilled rows in the backfill's. A design that runs a
   backfill has to either accept that a duplicated key answers differently on either side
   of the cut-over, or add an element-position column to the index before backfilling —
   which is a change to a table this design deletes, so in practice it means accepting
   it and saying so.

Under the issue's premise — no tagged release, no deployments, CI databases created fresh
per run — none of this arises. **The premise should be
checked rather than assumed**: `run_init` already refuses on a server below the minimum
version (`controller.rs`'s `check_version`, called at `:89`), and the same place can
refuse when `trace_spans` is non-empty at the point ids 44–69 would first apply, naming
the remedy (drop the database and re-reconcile) in the error.

**That count must be cluster-wide, not local.** A count of the local `trace_spans`
reports 0 on a shard that holds none of the rows, while another shard holds them, and the
sequence then runs and destroys the search state of rows it never saw. So:

    cluster mode   SELECT count() FROM clusterAllReplicas('<cluster>', <db>, trace_spans)
    single node    SELECT count() FROM <db>.trace_spans

Measured on 26.3.29.7 with the rows placed where the local count cannot see them: local
`0`, `clusterAllReplicas` `1`.

The MV list and `TTL_STMTS` change either way:

```
  amend    the MV list    trace_tag_catalog_mv now reads trace_spans with an
                          ARRAY JOIN and a GROUP BY; three new views
  amend    TTL_STMTS      controller.rs:436 is `[&str; 14]`. It loses the two
                          trace_attrs_idx statements and gains a MODIFY TTL and a
                          MODIFY SETTING for each of trace_attr_traces,
                          trace_recent, trace_error_spans and trace_tag_catalog:
                          14 - 2 + 8 = 20. The doc comment at controller.rs:479-480,
                          "a bounded catalog and carries no TTL", stops being true
                          of trace_tag_catalog and changes with it
```

**Two things do need care even with no data.**

1. **The ordering of work, which is about review, not about data.** Two changes
   are independent of everything else and make no read anywhere slower: the
   catalog gaining `date`, a partition, a TTL and a `GROUP BY` in its view, and
   `service_time` losing `SELECT *`. Together they return 100 B/span and bound
   the one quantity in the system that has no bound today, and they can be
   reviewed on their own. Everything else is one change, because the arrays, the
   trace-grain table and the deletion of `trace_attrs_idx` are the same change
   seen from three sides. (The option analysis also priced a codec on
   `trace_attrs_idx.timestamp_ns` at 80 B/span. That saving does not survive
   here: the column it applies to is on a table this design deletes.)
2. **The read path changes with the schema, in the same commit.** Two SQL
   builders are deleted (`search_sql.rs:286, 325`) and one is retargeted
   (`:397` — §4 Q1 says why it cannot become a column); the hydration builder gains an
   `arrayExists` column per attribute leaf and a value column pair per read field; and
   the tag builders gain a `date` and a `service` clause. A schema that ships ahead of
   the builders answers nothing.

---

## 9. What could go wrong, and what would tell us early

| risk | what it would look like | the early signal |
|---|---|---|
| ~~the `SimpleAggregateFunction` half of the new view is rejected~~ | — | **read: it is not.** §11 P1 |
| ~~a search batch's 2.2× byte cost is structural~~ | — | **read: on a first-seen batch there is no 2.2×.** The cost of that batch turns on how selective the probed value is, and it is worse than today only for a highly selective probe. §4 Q1 |
| a materialized view throws and leaves the span stored with **some** derived rows and not others | a trace answers some search shapes and not others, and which ones varies between runs of the identical write | **read: this happens, non-deterministically.** §6.3's twenty trials. No remedy is chosen here |
| a probe's negation is rendered inside `arrayExists` rather than left to the reader | `{ span.k != "x" }` starts matching spans that carry `k = "x"` and stops matching spans with no `k` | §4 Q1's five-case table is the test. Three of the five cases go wrong |
| the writer moves only the resource/span/instrumentation loop | `event:name`, `event:timeSinceStart`, `link:spanID`, `link:traceID` and every event and link attribute stop being searchable | §1.2. `otlp_traces.rs:505-607` is a second and third emission site with the same row shape |
| the base-table `ALTER`s ship without their `_dist` twins | single-node CI is green; the first clustered insert fails with `Code: 16 NO_SUCH_COLUMN_IN_TABLE` | §8. Single-node execution cannot see it — the check has to be a clustered insert |
| the duplicate-key rule is left to `arrayFirstIndex` without being stated | `avg`, `select` and `by` change answer on a span that repeats a key, silently | §4 Q1. The rule is the reference's; it is a change of answer and needs a ledger row |
| ids 44–69 run against a database that already holds trace rows | those spans stay reachable by service, name, duration and id, and unreachable by the empty search, every attribute condition, `status = error` and the tag dropdown | §8.1, which also gives two working backfills |
| the precondition that guards that state counts the LOCAL table | a shard holding none of the rows reports 0 and lets the sequence run | §8. The count has to be `clusterAllReplicas`; measured local 0 against cluster-wide 1 |
| a backfill is written as a mutation and trusted to be checked | `CHECK` constraints do not run during `ALTER … UPDATE`; a mutation can leave arrays of unequal length that an `INSERT` would reject | §8.1. Measured `HTTP 200` with `length(attr_key)=2`, `length(attr_num)=1`, and `Code: 469` for the same row inserted |
| five materialized views instead of two make ingest slower than the measurements suggest | insert wall time rises rather than falls | the nine write takes in §5 already measured the two-view case against the one-INSERT case; two of the three new views are a narrow filter and a narrow group. `trace_attr_traces_mv` is the one that expands and groups `A` rows per span, and it is the one to measure on its own. Measure insert wall time with each view added in turn |
| the metrics range query stays the slowest shape and someone adds a rollup later without re-checking §3.5 | `rate()` starts under-counting or over-counting after a client resends spans | §3.5 states the property the rollup must have. Any future rollup is checked against the duplicate table in §3.4 before it is built |
| wider candidate sets push queries into the 100,000 ceiling that did not hit it before | responses turn partial | the ceiling is already reported to the client; the two tests in §6.1 pin the behaviour at the boundary |
| the whole trace family loses its stored history at cut-over | — | there is no history. Nothing is deployed |

---

## 10. What this does not do

- **A dropdown narrowed by an attribute** rather than by a service still has no
  cheap path. With no span-grained attribute index, the attribute half of the
  narrowing becomes an `ARRAY JOIN` over the window. On the query mix of §2.2
  that is 23% of the rows an investigation reads, and it is unchanged. It is the one place this design is
  structurally worse than today, and it is not priced here.
- **It does not remove a derived table.** `trace_tag_catalog` has to stay:
  answering the dropdown off the span table costs 3,256 MB against the catalog's
  43 KB (measured, at 2,000,000 spans). "One INSERT" is not "one table".
- **It does not speed up metrics range queries at all.** `| rate()`,
  `| count_over_time()` and every other metrics function still scan the window.
  §3.5 says why a rollup would give a different answer, and what would have to
  change for one to be possible.
- **The error table does not help `status = unset`**, which matches most spans.
- **It does not shrink the index below one row per distinct value per trace.** If
  every attribute value were unique on every span, the row count would not fall
  at all; only the row width would, which is why §5.2 shows the layout still
  smaller in that degenerate case.
- **It does not make the numeric comparison exact.** §6.
- **It records nothing about which attribute keys people search on**, so the
  question "which keys deserve their own column" still cannot be answered from
  anything this repository stores.

---

## 11. What a measurement would have to show for this to be wrong

Ordered by how much rests on them. Each names the number this document predicts
and the reading that refutes it.

| # | prediction | how to read it | refuted if |
|---|---|---|---|
| **P1** — **READ, not refuted** | a view doing `ARRAY JOIN` **and** `GROUP BY` can write `SimpleAggregateFunction` columns of an `AggregatingMergeTree`, producing one row per (value, trace, bucket) after merge | create it; insert two blocks holding the same trace; compare `SELECT count()` and the `ts_max`/`dur_max`/`dur_min` values before and after `OPTIMIZE … FINAL` against the expected distinct-tuple count and the expected aggregates | the view is rejected, or the post-merge count is not the distinct-tuple count, or an aggregate column holds anything but the max/min over the collapsed rows. **Outcome:** all three statements accepted on 26.3.29.7; 14 rows across two parts before `OPTIMIZE … FINAL`, 11 after, against 11 distinct tuples computed from the span table; `countIf(ts_max/dur_max/dur_min disagree)` = 0 over 11 compared rows. Ran identically under `async_insert` 0 and 1 |
| **P2** — **READ; the stopping test itself was defective** | the search batch's 2.2× byte cost falls materially under `PREWHERE trace_id IN (…)`, and today's membership read is expensive on real data | the same batch statement with `WHERE` and with `PREWHERE`, comparing `read_bytes`; then `EXPLAIN indexes = 1` and the membership read's granule selection on a corpus with **high-cardinality** attribute values | **This row compared a mark count against a row count.** §5.2's 24,576 is a number of ROWS — three granules of 8,192 — and this row asked whether `SelectedMarks` stays near it. The two quantities are three orders of magnitude apart and the rule could never be met. Restated: *refuted if `read_bytes` does not fall AND the membership read still selects about 3 marks on a first-seen batch.* **Materiality is now a number, not a word:** the remedy is material if the `PREWHERE` form's `read_bytes` is **at most 0.80×** the `WHERE` form's — a 20% fall, a fifth of the excess the single statement carries. **Outcome:** 30,144,320 / 32,529,648 = **0.927** (26.3.29.7; `use_query_condition_cache=0`, `optimize_move_to_prewhere=1`, `max_block_size=65409`, `max_threads=auto(16)`; corpus C1), 5 reps, zero spread — so the remedy is refuted at that threshold, and at any threshold below 0.93. The membership read selects **246 marks / 2,015,232 rows** on a first-seen batch, not 3 / 24,576. The regression the row exists to price is not there on a first-seen batch |
| **P3** — **READ, REFUTED, and the refutation is wider than first recorded** | a materialized view that throws fails the whole `INSERT`, so nothing is stored rather than half | insert a block through a view built to throw; check whether the source part exists | the source part is written and only the view's target is missing. **Outcome: the source part is written, the throwing view's target is empty, and the OTHER views commit or not, non-deterministically.** Twenty trials with three healthy siblings: 14 committed none, 4 committed exactly one, 2 committed all three. §6.3 has the distribution. The first reading of this row saw one healthy sibling and one throwing view and concluded "no view committed"; with a single sibling that outcome appears about 70% of the time |
| **P4** — **READ, not refuted; the conditional half is refuted** | `{}` reads 12× fewer rows guaranteed, and 144× if the read can stop at the newest bucket | `EXPLAIN indexes = 1` and `read_rows` for the `trace_recent` statement in §4 Q0 | `read_rows` is not below the span-table figure. **Outcome:** 167,277 against 2,000,000 — 11.96×, so not refuted. The 144× does not occur: `Granules: 22/22`, and three optimiser settings each read the same 167,277 rows and 22 marks. §4 Q0 has the detail |
| **P5** | `d`, the trace-grain collapse factor, is ≈2.5 on real traces | on one hour of real traffic: `count() / uniqExact((trace_id, scope, key, val))` over the expanded attribute rows | `d` < 1.3, at which point the index saving is a width saving only and the storage case weakens from −40% to roughly −20% |
| **P6** | storage is 1047.9 → 625.7 B/span | build both schemas from one source table, `OPTIMIZE … FINAL`, `sum(bytes_on_disk)` from `system.parts`, on two corpora with `A_t` at both ends of its range | the new schema is not smaller on a corpus with `A_t` ≥ 200 |
| **P7** | merge CPU falls ≈46%, because one ZSTD(3) pass over `payload` disappears | `OPTIMIZE … FINAL` both schemas over the same rows; `sum(ProfileEvents['OSCPUVirtualTimeMicroseconds'])` from `system.part_log` where `event_type = 'MergeParts'` | the new schema's merge CPU exceeds today's by more than 10% on a corpus with `P_b` ≥ 300 |
| **P8** | the candidate set is a superset of today's by at most `1 + B/W`, and the answer is identical | run every committed search golden against both schemas on one corpus at `W = B` and `W = 12B`; compare returned trace ids **and** candidate counts from `system.query_log` | any golden returns a different trace set, or the candidate count grows by more than `1 + B/W` |
| **P9** | trace-by-id, the service graph and a bare-column metrics query are **identical** on every counter | `read_rows`, `SelectedMarks`, `OSCPUVirtualTimeMicroseconds`, `NetworkSendBytes` on both schemas | any differs by more than the run-to-run spread |
| **P10** | statements per search are `2 + (1+P)·⌈C/32⌉` today and `2 + ⌈C/32⌉` after | count `QueryFinish` rows in `system.query_log` for one request | the count is not 4 for a one-batch, one-condition search today, or not 3 after |
| **P11** | **every table in §3.4 gives the same answer when a span row is written twice** | insert one block; record the answer to each of the nine queries in §4; insert the byte-identical block again; record again | any of the nine answers differs. That would mean a table in §3.4's safe column is not safe, and it is the same defect §3.5 rejected the rollup for |

**Reading any of these against the DDL of §3.1 needs a current timestamp.** Every table
in §3.1 carries `TTL … + INTERVAL <retention> DAY DELETE` with
`ttl_only_drop_parts = 1`. A fixture using this document's worked timestamp,
`1700000000000000000` (2023-11-14), is older than any plausible retention, and ClickHouse
drops it **at insert time**, not at merge time. Measured on 26.3.29.7 with the server date
2026-09-10 and a 30-day TTL:

    INSERT … VALUES (toDate('2023-11-14'), 5666666, unhex('4bf9…4736'), 1700000000000000000)
    SELECT count()                 0          <- immediately, before any OPTIMIZE
    system.parts   20231114_1_1_1  1 row  active=0
                   20231114_1_1_3  0 rows active=1

So a reading either omits the TTL clause from its throwaway DDL, or derives its
timestamps from the clock. **`toInt64(now64(9))` is the wrong way to do the second**: it
returns seconds — `1789033657` where the nanosecond value is `1789033657670830494` — and
a row built from it is 1970-dated and dropped in the same way, silently.
`toUnixTimestamp64Nano(now64(9))` is the correct form, verified to survive the same TTL.

### 11.1 Where the proof stops

**Derived from files in this repository, checkable without running anything.**
Every column, type, codec, sort key, partition key, projection column list and
materialized view in §1 (`catalog.rs:227-234, 244-256, 266-281, 335-407,
648-936, 934-1000`); every statement and its `SELECT` list (`search_sql.rs:184,
230, 286, 325, 397, 428, 468, 492`; `tags_sql.rs:89, 118, 253, 282`;
`sql.rs:16-26`; and the committed goldens); the batch arithmetic (`exec.rs:117`,
`config/model.rs:514-516`); which aggregates push down and what they read
(`compile.rs:431-462, 560-562`); the write path's failure modes
(`writer/trace.rs:9-19, 137-185, 172`; `writer/table.rs:367-434`;
`backfill.rs:23-28, 189-201, 214-220`); the wire framing
(`vendor/clickhouse/src/rowbinary/ser.rs:129, 137, 146, 222`) and that the
storage-to-reader hop is LZ4-framed (`pulsus-clickhouse/src/pool.rs:695` →
`vendor/clickhouse/Cargo.toml:49` `default = ["lz4"]` →
`vendor/clickhouse/src/query.rs:221-231`, which appends `compress=1`).

**Measured elsewhere, and marked [M].** Two ClickHouse 26.3 corpora, 2,000,000
and 20,000,000 spans, one synthetic generator, one machine under load.
`bytes_on_disk` reproduced across three complete rebuilds; no bound claimed.

**Assumed, all named in Appendix A.** Eleven compression ratios and the
ZSTD(3)-to-LZ4 cost ratio. Every worked byte figure moves with them.

**Argued, with what would falsify each.**

- *That the identity cannot be made narrower* rests on the entropy of a sorted
  subset. It would be wrong if trace ids were not uniform — if some deployment's
  ids carried structure a codec could find. OTLP promises uniqueness, not
  randomness.
- *That the bucket in position 4 preserves today's pruning exactly* rests on
  reading the two sort keys and on how a ClickHouse key condition uses a column
  at position 4. **ClickHouse is not checked out on this machine**, so its key
  condition code was not read. P2 is the same gap seen from the other side.
- *That the five arrays can carry every scope the writer emits* rests on all seven scopes
  having the same row shape — key, scope, val, val_type, val_num (`otlp_traces.rs:487-607`).
  It would be wrong if any scope needed a field the others do not have. Read, not run:
  no ingest path has been exercised end to end into the arrays.
- *That the scalar value read can replace `attr_values_sql`* rests on the read being
  scalar — one value per (span, key) — so `arrayFirstIndex` plus one element yields the
  row shape the hydration read already has. Measured that the SQL works
  (`arrayFirstIndex` over four aligned arrays returned index 3, skipping a NULL element,
  with the element's own `val_type`); not measured against today's `any(val_num)`, which
  picks arbitrarily where the new form picks the sender's first.
- *That no statement needs an attribute array from a projection* rests on
  enumerating the builders in `search_sql.rs` (8), `tags_sql.rs` (4), `sql.rs`
  (1) and `graph_sql.rs` (1), plus the structural fact that every attribute
  value in `metrics_sql.rs` arrives through a join against the attribute table.
  For that file's builders that is **an argument about the file, not a reading
  of every one of them.**
- *That five arrays give byte-identical answers to today's two columns* rests on
  both sides parsing the same text with the same function, and it was measured
  bitwise on 6,000,000 values (§6). It fails if any future path populates
  `attr_num` from something other than the stored text.

**A figure taken through a wrapper is not a figure.** Every `[M]` counter in this
document was taken by running the statement itself, on the corpus defined at the head of
§4, with that section's instrument. Figures whose method was not recorded and whose corpus
no longer exists — the 20,000,000-span Q6b byte and timing ratio, and Q7's original
1,516,384 / 65,764,280 — are **withdrawn rather than carried**, and are not replaced by
estimates.

**Three things are established more narrowly than an earlier version of this document
said.**

- The wrapper's byte difference is 48,001,456, and `trace_id` + `span_id` measured
  separately are 48,000,640. The mechanism is established from `EXPLAIN header = 1`; the
  816-byte residue is not attributed.
- The query-condition cache misses on a 32-id list not seen before, measured with the
  cache warm from a different list. **How often a real deployment repeats a list is not
  measured**, so "a production batch never repeats" is not a claim this document makes.
- **The source-committed / no-view-committed boundary is withdrawn.** It came from one
  throwing view beside one healthy view, an outcome that appears about 70% of the time
  with a single sibling. With three siblings and twenty trials the failure leaves
  **partial** derived state, non-deterministically. §6.3 carries the distribution and
  what it takes to observe it.
- The clustered path is measured for **column presence and one insert** through a
  single-node `Distributed`. Multi-shard routing, `cityHash64(trace_id)` co-sharding of
  the four new wrappers, and a clustered read were not measured here.
- **The corpus-construction attribution is withdrawn.** It accounts for the mark counts
  and not for the hydration counter it was offered for; that counter's corpus no longer
  exists and its cause is unknown. §4's corpus block records both reconstructions.

**The stopping rule.** A new version of this document is warranted by a finding
that **moves a crossover in §5.2** — the `A_t` at which the storage sign flips,
the `n_k` at which the batch read flips, or the `σ_err`/`σ_name` at which the two
added sorted paths stop paying — or that changes an answer in §6, or that finds
a table in §3.4 which is not in fact safe against a duplicated span row, or that
changes which rows a query in §4 returns. A finding that moves a worked value while leaving
those where they are is recorded, not re-argued: the worked column illustrates an
expression, and the expression is the deliverable.

---

## Appendix A — parameters

Every symbol used above. The worked column is one plausible deployment;
substitute your own and Appendix B recomputes everything.

### The workload

| symbol | meaning | range | worked |
|---|---|---|---|
| `N` | spans ingested per day | 10⁷ – 10¹⁰ | 10⁹ |
| `S` | spans per trace | 3 – 200 | 12 |
| `n_svc` | distinct services in one trace | 1 – 20 | 4 |
| `A_r` | resource + scope attributes per span — the same tuple on every span of a service | 3 – 30 | 12 |
| `A_s` | span + event + link attributes per span | 1 – 40 | 8 |
| `A` | `A_r + A_s`, attributes per span | 5 – 60 | 20 |
| `ζ` | of the `A_s`, the fraction whose `(scope, key, value)` is distinct within its trace | 0.1 – 1.0 | 0.5 |
| `A_t` | distinct `(scope, key, value)` tuples in one trace = `A_r·n_svc + A_s·S·ζ` | 24 – 240 | 96 |
| `d` | trace-grain collapse factor = `A·S / A_t` | 1.0 – 10 | 2.5 |
| `I`, `F` | of `A`, how many are OTLP int / double | 0 – `A` | 4, 2 |
| `L_v` | mean bytes of an attribute value's text | 3 – 60 | 14 |
| `P_b` | OTLP protobuf bytes of one span (the `payload` column) | 150 – 4000 | 400 |
| `K_d` | distinct `(scope, key, val, val_type)` tuples produced in one day | 10² – 10⁷ | 10⁴ |
| `K_tot` | distinct tuples the catalog holds over the deployment's whole life | ≥ `K_d` | 10⁶ |
| `R` | retention, days | 7 – 30 | 7 |
| `n_grp` | distinct `(service, name, status, kind)` combinations — used **only** to price the rollup §3.5 rejected | 10³ – 10⁶ | 3·10⁴ |

Where `A_t` comes from, since the whole index saving rests on it:

```
   one trace, S = 12 spans across n_svc = 4 services

   resource attrs  A_r = 12 per span x 12 spans = 144 rows -> 12 x 4  = 48 tuples
   span attrs      A_s =  8 per span x 12 spans =  96 rows -> 96 x 0.5 = 48 tuples
                                     A x S = 240 rows      -> A_t      = 96 tuples

                                        d = 240 / 96 = 2.5
```

### The query

| symbol | meaning | range | worked |
|---|---|---|---|
| `W` | query window | 5 min – `R` | 1 hour |
| `N_W` | spans in the window = `N·W` | — | 4.17·10⁷ |
| `M` | result limit, traces | 20 – 1000 | 20 |
| `P` | distinct attribute conditions in the query | 0 – 5 | 1 |
| `f_k` | fraction of spans carrying the probed key | 10⁻⁶ – 1 | 0.5 |
| `σ_svc`, `σ_name`, `σ_err` | fraction of spans matching a service / a span name / `status = error` | 10⁻⁶ – 1 | 0.02, 0.02, 0.01 |
| `B` | bucket width in the new index | 60 s – 1 h | 300 s |
| `Q_b`, `B_r` | bytes of one latency sketch state, and the rollup bucket width — both used **only** to price the rollup §3.5 rejected | 200 – 3000; 15 s – 15 min | 1200; 60 s |

`M` = 20 and `W` = 1 hour are the Grafana traces datasource's own defaults, read
from its source; the pointer is not reproduced here.

### Fixed by this repository, not assumed

| what | value | where |
|---|---|---|
| phase-2 batch | 32 candidate traces | `exec.rs:117` |
| candidate ceiling | 100,000 | `config/model.rs:514` |
| scan budget | 50,000,000 rows | `config/model.rs:515` |
| tag lookback default | 24 hours | `config/model.rs:516` |
| spans per trace cap | `LIMIT 10001 BY trace_id` | `exec.rs:122` |
| tag name / value caps | 10,000 / 1,000 | `exec.rs:130, 135` |
| storage → reader wire format | RowBinary, LZ4-framed | `pool.rs:695` → `vendor/clickhouse/Cargo.toml:49` → `vendor/clickhouse/src/query.rs:221-231` |
| shard key | `cityHash64(trace_id)` | `render.rs:55-57` |
| pushed aggregates | `uniqExact(span_id)`, `max(duration_ns)`, `min(duration_ns)` | `compile.rs:451-462` |

### Assumed compression ratios

Three columns name a codec (`catalog.rs:346-347, 351`): `timestamp_ns
CODEC(DoubleDelta, ZSTD(1))`, `duration_ns CODEC(T64, ZSTD(1))`, `payload
CODEC(ZSTD(3))`. Everything else takes the server default, LZ4.

| symbol | what | range | worked |
|---|---|---|---|
| `Z_p` | ZSTD(3) on OTLP protobuf | 1.0 – 8.0 | 4 |
| `Z_v` | LZ4 on attribute value text | 1.5 – 12 | 3 |
| `Z_ts0` | LZ4 alone on a sorted `Int64` nanosecond timestamp | 1.2 – 2.5 | 1.6 |
| `Z_ts1` | DoubleDelta + ZSTD(1) on a sorted `Int64` timestamp | 5 – 15 | 8 |
| `Z_tsu` | LZ4 on an **unsorted** `Int64` nanosecond timestamp | 1.0 – 2.0 | 1.3 |
| `Z_dur` | T64 + ZSTD(1) on durations | 1.2 – 6 | 2 |
| `Z_num` | LZ4 on a mostly-NULL `Nullable(Float64)` | 1.5 – 4 | 2 |
| `Z_bkt` | Delta + ZSTD on a sorted `UInt32` bucket | 5 – 30 | 12 |
| `ρ` | ZSTD(3) compression cost ÷ LZ4 compression cost, per byte | 5 – 20 | 10 |

Two structural facts used throughout, which are not assumptions:

- **A random 16-byte `trace_id` compresses only where consecutive rows repeat
  it.** `trace_spans ORDER BY (trace_id, timestamp_ns)` puts a trace's `S` spans
  together, so the base table pays about `16/S`. A table sorted by attribute
  value pays the full 16 on every row.
- **ClickHouse is columnar.** A column a statement does not name costs it
  nothing. That is why adding five array columns is free for every query that
  does not read them, and it is **measured**: `{status = error}` reads 50,000,504
  bytes with the arrays present and 50,000,504 without.

---

## Appendix B — the calculator

Every derived figure above comes from this. It builds the new total twice — as a
direct sum and as a delta from today — and compares them, so an arithmetic slip
exits non-zero.

```python
# --- workload -----------------------------------------------------------
S, n_svc, A_r, A_s, zeta = 12, 4, 12, 8, 0.5
A, I, F, Lv, Pb, N, R = A_r + A_s, 4, 2, 14, 400, 1e9, 7
sigma_err, sigma_name, sigma_svc, f_k = 0.01, 0.02, 0.02, 0.5
n_grp, B, B_r, t_trace = 3e4, 300.0, 60.0, 1.0   # n_grp/B_r: the REJECTED rollup
W = 3600.0                                   # query window, seconds
M, sigma_match, P = 20, 1.0, 1
# --- assumed compression ------------------------------------------------
Zp, Zv, Zts0, Zts1, Ztsu, Zdur, Znum, Zbkt, rho = 4, 3, 1.6, 8, 1.3, 2, 2, 12, 10
Qb = 1200.0                                  # bytes of one quantilesTDigest state

A_t = A_r*n_svc + A_s*S*zeta                 # distinct (scope,key,val) per trace
d   = A*S/A_t
rps = A_t/S                                  # new-index rows per span

# --- per-span compressed bytes ------------------------------------------
base_scalars = 16/S + 16 + 0.2 + 8/Zts1 + 8/Zdur + 0.4
base     = base_scalars + Pb/Zp
proj     = base_scalars - 16/S + 16          # a projection re-sorts: full 16
svc_star = proj + Pb/Zp                      # today's SELECT * projection

# five aligned arrays: key, scope, val, type, num. NOT six: a second, integer
# numeric array would move answers above 2^53 (see the data contract).
arr      = 3 + A*Lv/Zv + (I+F)*8/Znum + A/Znum
arr_raw  = 40 + A*(13+Lv)
arr6     = arr + I*8/Znum + A/Znum
arr6_raw = 48 + A*(22+Lv)

idx_today  = Lv/Zv + ((I+F)/A)*8/Znum + 1/Znum + 8/Zts0 + 16 + 8 + 8/Zdur
idx_new    = Lv/Zv + ((I+F)/A)*8/Znum + 1/Znum + 4/Zbkt + 16 + 8/Ztsu + 2*8/Zdur
idx_new_fp = idx_new - 16 + 4.0              # 40-bit fingerprint knob
idx_raw    = 2+1+(1+Lv)+1+4+16+1+9+8+8+8     # one new index row, uncompressed

err_raw    = 2+16+8+8+8+1+1+1                # one trace_error_spans row, raw
err_tbl    = sigma_err*(proj + 16)
# The rollup row, split: the four plain aggregates, and the OPTIONAL quantile
# sketch, which is 99% of it.
rollup_raw   = 8+2+2+1+1 + 8 + 8+8+8            # without the sketch
rollup_row   = 0.5 + 0.2 + 0.1 + 2 + 6          # compressed, without the sketch
rollup_b     = n_grp*(86400.0/B_r)*rollup_row/N
sketch_b     = n_grp*(86400.0/B_r)*Qb/N         # a t-digest state barely compresses
recent_raw = 2+4+16+8                        # one trace_recent row, raw
recent_row = 0.0 + 4/Zbkt + 16 + 8/Ztsu      # compressed: date~0, bucket, id, ts
recent_b   = recent_row*(1 + t_trace/B)/S

TODAY   = base + svc_star + A*idx_today
STEP0   = base + proj + A*idx_today          # only: payload out of service_time
STEP1   = STEP0 - A*(8/Zts0 - 8/Zts1)        # + a codec on the index timestamp
STEP2   = (base + arr) + proj + rps*idx_new
STEP3   = STEP2 + proj + err_tbl
NEW     = STEP3 + recent_b
chk = (TODAY - Pb/Zp + arr - A*idx_today + rps*idx_new
       + proj + err_tbl + recent_b)
assert abs(NEW-chk) < 0.01, "decomposition vs direct sum: %.2f != %.2f" % (NEW, chk)
```

Its output at Appendix A's parameters:

```text
A=20  A_t=96  d=2.50  new index rows per span 8.00
arrays: 5 -> 130.3 compressed / 580 raw   (6 would be 156.3 / 768)
one index row: today 39.37 | new 36.85 | new with a 40-bit fingerprint 24.85

  today                                 1047.9 B/span    +0.0%  7.34 TB at N=1e9 R=7
  payload out of service_time            947.9 B/span    -9.5%  6.64 TB at N=1e9 R=7
    + a codec on the index timestamp     867.9 B/span   -17.2%  6.08 TB at N=1e9 R=7
  attributes on the span row             585.7 B/span   -44.1%  4.10 TB at N=1e9 R=7
    + the two sorted paths               623.8 B/span   -40.5%  4.37 TB at N=1e9 R=7
    + the recency index = THE PROPOSAL   625.7 B/span   -40.3%  4.38 TB at N=1e9 R=7

  today, per table:
    trace_spans base               122.9   11.7%
    service_time (SELECT *)        137.6   13.1%
    trace_attrs_idx                787.3   75.1%
    trace_tag_catalog                0.0    0.0%
  new, per table:
    trace_spans base + arrays      253.3   40.5%
    service_time                    37.6    6.0%
    name_time                       37.6    6.0%
    trace_attr_traces              294.8   47.1%
    trace_error_spans                0.5    0.1%
    trace_recent                     1.9    0.3%
    trace_tag_catalog                0.0    0.0%

  attribute-index share of today's family, across A:
    A= 5   index   196.8 of   457.4 = 43.0%
    A= 8   index   314.9 of   575.5 = 54.7%
    A=10   index   393.7 of   654.2 = 60.2%
    A=20   index   787.3 of  1047.9 = 75.1%
    A=40   index  1574.7 of  1835.2 = 85.8%
    A=60   index  2362.0 of  2622.5 = 90.1%

  storage crossover: the new layout stops being smaller at A_t = 246.5;
    A_t can never exceed A*S = 240, so it is smaller at EVERY parameter value.
    at the degenerate A_t = A*S = 240 it is still 1027.9 B/span against 1047.9

  the metrics rollup that was REJECTED (section 3.5), priced anyway,
  at n_grp = 3e+04 - it is not in the total above:
    B_r =   60s   count/sum/min/max   0.38 B/span   + a latency sketch   51.84 B/span
    B_r =  300s   count/sum/min/max   0.08 B/span   + a latency sketch   10.37 B/span
    B_r =  900s   count/sum/min/max   0.03 B/span   + a latency sketch    3.46 B/span
    it would read n_grp*W/B_r = 1.80e+06 rows for a 1 h query against 4.17e+07

  merge, raw B/span/level     today   2656   new   1741   -34.5%
  merge, LZ4-equivalent       today   9856   new   5341   -45.8%
  client INSERT, raw B/span   today   1838 in 2 statements   new   1038 in 1   -43.5%
  catalog rows the MV writes per span   today 20   new  <= distinct tuples per block

  W = 1 h  ->  N_W = 4.167e+07 spans, 3.472e+06 traces in the window
    Q0  {}                           today 4.167e+07   new 3.484e+06   x12.0
    Q1  service+attr+dur, phase 1    today 8.333e+05   new 8.333e+05   x1.0
    Q2  attribute only               today 2.083e+07   new 8.333e+06   x2.5
    Q3a {status = error}             today 4.167e+07   new 4.167e+05   x100.0
    Q3b {name = "GET /pay"}          today 4.167e+07   new 8.333e+05   x50.0
    Q6a | rate() by(service)         today 4.167e+07   new 4.167e+07   x1.0
    Q4a/Q4b the tag dropdown         today 1.000e+06   new 1.000e+04   x100
    Q5 trace by id / Q7 service graph      identical statements, identical tables

  {} against trace_recent: 3.484e+06 trace rows vs 4.167e+07 span rows = x12.0 guaranteed
    ... x144 only if the read stops at the newest bucket (2.89e+05 traces per 300s bucket)

  statements per search, one attribute condition, M=20, sigma_match=1.00:
    today  2 + (1+P)*ceil(C_used/32) = 4      new  2 + ceil(C_used/32) = 3
    M=1000 sigma_match=1.00  batches   32   today    66   new    34
    M=1000 sigma_match=0.25  batches  125   today   252   new   127
    at the candidate ceiling (100000/32 = 3125 batches)  today  6252   new  3127
```
