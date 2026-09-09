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

**The one thing that gets worse**, stated up front: one phase of a search reads
2.2× the bytes it reads today. That is measured, not derived — §4 Q1 has the
numbers and the reason. §9 says what would make it a reason to stop.

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
`[(resource, …), (span, …), (instrumentation, …)]`). So this one span produces:

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
  AND ts_max > 1700000000000000000 AND ts_max <= 1700010800000000000
GROUP BY trace_id ORDER BY bound_ts DESC, trace_id ASC LIMIT 100001
```

Reads **3.48·10⁶ trace rows instead of 4.17·10⁷ span rows — 12× fewer**, and that
is guaranteed by row counts alone. It is 144× fewer if ClickHouse can stop at the
newest bucket rather than reading the whole window; that is unverified and is
§11 P4.

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

**New: one statement.** The attribute test becomes a column of the first one:

```sql
SELECT trace_id, span_id, parent_id, <byte-capped service>, <byte-capped name>,
       timestamp_ns, duration_ns, status_code, <byte-capped status_message>, kind,
       <byte-capped scope_name>, <byte-capped scope_version>,
       arrayExists((k, s, n) -> k = 'http.status_code' AND s = 'span' AND n >= 500,
                   attr_key, attr_scope, attr_num) AS probe0
FROM trace_spans
PREWHERE trace_id IN (…32 ids…)
WHERE timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000
ORDER BY trace_id ASC, timestamp_ns ASC, span_id ASC
LIMIT 10001 BY trace_id
```

Three of the eight search builders disappear — `membership_sql`,
`attr_values_sql` and `event_set_sql` (`search_sql.rs:286, 325, 397`), all of
which read the attribute table once per batch. `root_sql`, `trace_ctx_sql` and
`child_count_sql` (`:428, 468, 492`) read `trace_spans` by `trace_id IN` and are
untouched.

**This is the one read that gets worse.** Measured, per batch of 32 traces on a
2,000,000-span corpus: two statements read 10,092,369 bytes; one statement with
the inline test reads 21,952,638 — **2.2×**. Wall time was 22/19/18 ms against
23/22/25 ms, which on a loaded machine is a wash. The reason is granule locality:

```
   the batch wants   32 trace ids x 12 spans          =     384 spans
   the batch reads   55 granules of 245, 8192 rows    = 229,321 rows
                     ^-- 32 random trace ids scatter across 55 granules,
                         and a column is read one whole granule at a time

   so the test reads attr_key + attr_scope + attr_num for 229,321 rows
   to answer a question about 384 of them.
```

The `PREWHERE trace_id IN (…)` above is the intended remedy — today's builder
uses `WHERE` — and it is unmeasured. §11 P2.

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

Same sorted prefix, same seek, **2.5× fewer rows** — the trace-grain collapse
factor `d` (Appendix A).

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
error spans: **100× fewer rows** at `σ_err` = 1%.

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
**50× fewer rows** at `σ_name` = 2%.

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

Measured identical on every schema shape tested: 1 granule of 245, 8,192 rows,
132,564 bytes. This is the latency-critical read and nothing here touches it.

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
… AND arrayExists((k, s, n) -> k = 'http.status_code' AND s = 'span' AND n >= 500,
                  attr_key, attr_scope, attr_num)
```

**Measured** at 20,000,000 spans: the inline form reads 3.9× more bytes and is
**1.7–2.3× faster** (1436/1543/1627 ms against 2442/3659/3479 ms), because
building a hash table over 20 million `(trace_id, span_id)` tuples costs more
than reading the arrays. Both numbers are worth quoting; only one flatters the
change.

### Q7 — the service graph

Reads `trace_edges`, which this design does not touch. The statement is
byte-identical (`golden/traces_graph/single_node.sql`); measured 1,516,384 rows
and 65,764,280 bytes on both shapes.

---

## 5. What it costs and what it saves

Per dimension. **[D]** = derived from the files cited, by the calculator in
Appendix B. **[M]** = measured on ClickHouse 26.3 at 2,000,000 and 20,000,000
spans.

| # | dimension | today | new | change | |
|---|---|---|---|---|---|
| 1 | storage, B/span | 1047.9 | **625.7** | **−40.3%** | [D] |
| 1 | storage at 10⁹ spans/day, 7 days | 7.34 TB | **4.38 TB** | −2.96 TB | [D] |
| 2 | rows read, `{}` | 4.17·10⁷ | 3.48·10⁶ | **÷12** | [D] |
| 2 | rows read, `{status = error}` | 4.17·10⁷ | 4.17·10⁵ | **÷100** | [D] |
| 2 | rows read, `{name = "…"}` | 4.17·10⁷ | 8.33·10⁵ | **÷50** | [D] |
| 2 | rows read, `\| rate() by(service)` | 4.17·10⁷ | 4.17·10⁷ | **unchanged** — §3.5 | [D] |
| 2 | rows read, attribute search | 2.08·10⁷ | 8.33·10⁶ | **÷2.5** | [D] |
| 2 | rows read, the tag dropdown | 10⁶ and rising with deployment age | 10⁴ | **÷100, and bounded** | [D] |
| 2 | rows read, the narrowed dropdown | 2,138,112 at 2M / 21,037,056 at 20M | a key seek | **≫100×**, service-narrowed only | [M] |
| 2 | rows read, trace-by-id and the service graph | — | — | **identical** | [M] |
| 3 | bytes, storage → reader, one search batch | 10,092,369 | 21,952,638 | **+117%** | [M] |
| 3 | bytes, writer → ClickHouse | 1838 raw B/span, 2 statements | 1038, 1 | **−43.5%** | [D] |
| 3 | rows crossing to every replica (the catalog is `Replication::Global`, `catalog.rs:406`) | 20 per span | ≤ distinct tuples per block | **≈500×** | [D] |
| 3 | bytes, reader → client | — | — | **unchanged** — set by the API response shape, not by storage | [D] |
| 4 | SQL statements, one-condition search, `M`=20 | 4 | **3** | −25% | [D] |
| 4 | … at the candidate ceiling | 6252 | **3127** | −50% | [D] |
| 5 | ClickHouse CPU | tracks the uncompressed bytes of the selected columns | | ÷2.5 on attribute search; ×2.2 on a search batch; 1.7–2.3× **faster** on an attribute metrics query at 20M | [M] |
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
| `n_k`, attribute-index rows a batch's membership read touches | the search batch's byte cost crosses at `n_k` ≈ **9.5·10⁵** | below it, two statements read fewer bytes. **Measured on the test corpus: 24,576** — so on that corpus the new design loses this one dimension. §11 P2 is the reading that decides it on real data |

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

### 6.3 One class of wrong answer disappears

Today a span counts as matching an attribute if and only if an index row exists
for it — and §2.5 lists three ways the two tables can permanently disagree about
that. In the new design the test reads the span's own row, so **that disagreement
cannot occur.**

---

## 7. What we did not measure

Stated here rather than left to be found.

| not measured | why it matters |
|---|---|
| the `PREWHERE trace_id IN (…)` remedy for Q1 phase 2 | it is the fix for the one dimension that gets worse. §11 P2 |
| whether a view doing `ARRAY JOIN` and `GROUP BY` can feed `SimpleAggregateFunction` columns | the `ARRAY JOIN` + `GROUP BY` combination itself **was** measured, feeding the tag catalog in the one-table corpus; writing merge-time aggregates through it was not. §11 P1 |
| whether a throwing materialized view leaves the source part written | it decides whether "one INSERT" really removes the partial-write class or moves it. ClickHouse is not checked out on this machine and could not be read. §11 P3 |
| whether `{}` can be answered from the newest buckets without reading the whole window | the difference between ÷12 and ÷144. §11 P4 |
| `d`, the trace-grain collapse factor, on real traces | it scales the whole index saving. §11 P5 |
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

So the migration list is `CREATE TABLE`. Concretely, in
`crates/pulsus-schema/src/catalog.rs`:

```
  amend    migration 16   trace_spans: the five arrays, the CONSTRAINT, and
                          service_time as a named column list instead of SELECT *
  delete   migration 17   trace_attrs_idx
  delete   migrations     20 and 40 - the `_dist` wrapper and the `val_type`
           17/20/39/40    ALTER of a table that no longer exists
  amend    migration 18   trace_tag_catalog: + date + service,
           and 41         PARTITION BY date, the new ORDER BY. Migration 41's
                          `val_type` ALTER folds into the CREATE, because there
                          are no existing parts for it to be additive over
  add                     trace_attr_traces, trace_error_spans, trace_recent
  amend    the MV list    trace_tag_catalog_mv now reads trace_spans with an
                          ARRAY JOIN; three new views
  amend    TTL_STMTS      controller.rs:436-472 gains the new tables and loses
                          the two trace_attrs_idx statements
```

The migration ids and the checksum machinery exist and are unchanged; this is a
different set of statements through the same mechanism.

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
2. **The read path changes with the schema, in the same commit.** Three SQL
   builders are deleted (`search_sql.rs:286, 325, 397`), one gains an
   `arrayExists` column, and the tag builders gain a `date` and a `service`
   clause. A schema that ships ahead of the builders answers nothing.

---

## 9. What could go wrong, and what would tell us early

| risk | what it would look like | the early signal |
|---|---|---|
| the `SimpleAggregateFunction` half of the new view is rejected by ClickHouse | `CREATE MATERIALIZED VIEW` fails at `--mode init` | try it first, before anything is built. §11 P1 — it is one statement, and the `ARRAY JOIN` + `GROUP BY` half is already measured working |
| a search batch's 2.2× byte cost is structural and `PREWHERE` does not help | `read_bytes` on the hydration statement does not fall | §11 P2, one `EXPLAIN` and one `read_bytes` comparison. If it does not fall, the arrays still pay for themselves on storage, ingest and merge, and the option to keep a span-grained index for the probe alone is still open |
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
| **P1** | a view doing `ARRAY JOIN` **and** `GROUP BY` can write `SimpleAggregateFunction` columns of an `AggregatingMergeTree`, producing one row per (value, trace, bucket) after merge. The `ARRAY JOIN` + `GROUP BY` half is already measured working, into a `ReplacingMergeTree` | create it; insert two blocks holding the same trace; compare `SELECT count()` and the `ts_max`/`dur_max`/`dur_min` values before and after `OPTIMIZE … FINAL` against the expected distinct-tuple count and the expected aggregates | the view is rejected, or the post-merge count is not the distinct-tuple count, or an aggregate column holds anything but the max/min over the collapsed rows. **Cheapest reading here and the one to take first** |
| **P2** | the search batch's 2.2× byte cost falls materially under `PREWHERE trace_id IN (…)`, and today's membership read is expensive on real data (`n_k` in the millions, against 24,576 on the test corpus) | the same batch statement with `WHERE` and with `PREWHERE`, comparing `read_bytes`; then `EXPLAIN indexes = 1` and `SelectedMarks` for today's membership read on a corpus with **high-cardinality** attribute values | `read_bytes` does not fall, **and** `SelectedMarks` on the membership read stays near 24,576. Then the batch regression is permanent and real |
| **P3** | a materialized view that throws fails the whole `INSERT`, so nothing is stored rather than half | insert a block through a view built to throw; check whether the source part exists | the source part is written and only the view's target is missing. Then the partial-write class of §2.5 has moved rather than gone, and §6.3 is overstated |
| **P4** | `{}` reads 12× fewer rows guaranteed, and 144× if the read can stop at the newest bucket | `EXPLAIN indexes = 1` and `read_rows` for the `trace_recent` statement in §4 Q0 | `read_rows` is not below the span-table figure — then the recency table is worthless and is dropped, at a cost of 1.9 B/span |
| **P5** | `d`, the trace-grain collapse factor, is ≈2.5 on real traces | on one hour of real traffic: `count() / uniqExact((trace_id, scope, key, val))` over the expanded attribute rows | `d` < 1.3, at which point the index saving is a width saving only and the storage case weakens from −40% to roughly −20% |
| **P6** | storage is 1047.9 → 625.7 B/span | build both schemas from one source table, `OPTIMIZE … FINAL`, `sum(bytes_on_disk)` from `system.parts`, on two corpora with `A_t` at both ends of its range | the new schema is not smaller on a corpus with `A_t` ≥ 200 |
| **P7** | merge CPU falls ≈46%, because one ZSTD(3) pass over `payload` disappears | `OPTIMIZE … FINAL` both schemas over the same rows; `sum(ProfileEvents['OSCPUVirtualTimeMicroseconds'])` from `system.part_log` where `event_type = 'MergeParts'` | the new schema's merge CPU exceeds today's by more than 10% on a corpus with `P_b` ≥ 300 |
| **P8** | the candidate set is a superset of today's by at most `1 + B/W`, and the answer is identical | run every committed search golden against both schemas on one corpus at `W = B` and `W = 12B`; compare returned trace ids **and** candidate counts from `system.query_log` | any golden returns a different trace set, or the candidate count grows by more than `1 + B/W` |
| **P9** | trace-by-id, the service graph and a bare-column metrics query are **identical** on every counter | `read_rows`, `SelectedMarks`, `OSCPUVirtualTimeMicroseconds`, `NetworkSendBytes` on both schemas | any differs by more than the run-to-run spread |
| **P10** | statements per search are `2 + (1+P)·⌈C/32⌉` today and `2 + ⌈C/32⌉` after | count `QueryFinish` rows in `system.query_log` for one request | the count is not 4 for a one-batch, one-condition search today, or not 3 after |
| **P11** | **every table in §3.4 gives the same answer when a span row is written twice** | insert one block; record the answer to each of the nine queries in §4; insert the byte-identical block again; record again | any of the nine answers differs. That would mean a table in §3.4's safe column is not safe, and it is the same defect §3.5 rejected the rollup for |

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

**The stopping rule.** A new version of this document is warranted by a finding
that **moves a crossover in §5.2** — the `A_t` at which the storage sign flips,
the `n_k` at which the batch read flips, or the `σ_err`/`σ_name` at which the two
added sorted paths stop paying — or that changes an answer in §6, or that finds
a table in §3.4 which is not in fact safe against a duplicated span row. A finding that moves a worked value while leaving
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
