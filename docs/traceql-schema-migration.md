# Changing the trace tables

**What this is.** The design for the ClickHouse tables that hold traces: what we
store today, what is wrong with it in numbers, what we would store instead, and
what each query costs before and after.

**Who it is for.** Someone who has not read the code and will not open it. A term is
defined where it first appears. A structural claim points at a file, and at a line range
where the claim is about particular lines — 109 such ranges, all quoted in Appendix C,
and twelve places that name a file without one. **Every figure says whether it was
derived (from those files, on paper) or measured (on a running ClickHouse)**: §5's cost
table marks each row `[D]` or `[M]`, and a figure outside that table says in words which
it is and what produced it.

**Nothing has ever shipped.** No users, no deployments, no stored data. So this is
not a data migration. It is a decision about which `CREATE TABLE` statements the
product ships with. §8 says what that means in practice.

    this tree            86081ef1eaceaad37d8ede1f5cf47b46a9ce41ec — where the readings were taken
    citations pinned at  8f3348e88a1ff5033d7be623fabbbfd24b189e59 — the tip of main when
                         Appendix C was written.  The body cites 109 line ranges and
                         Appendix C quotes all 109.  Every range into CODE is byte-identical
                         between the two revisions, so no reading of the code has gone stale;
                         the two exceptions are document lines, marked where they appear,
                         and they are the citations round one corrected
    ClickHouse           26.3 is the live target (.github/workflows/ci.yml:567-572)
    every derived number Appendix B's calculator, at Appendix A's parameters, or stated
                         arithmetic on what it prints.  It prints the output block beside
                         it: run it and compare
    every code citation  a path from the repository root, and **Appendix C quotes the
                         lines**, so a reading can be checked without opening the tree
    every measured number the corpus and the server version sit beside it, and so do the
                         settings wherever they were recorded.  One row's were not: §5's
                         write wall time says so in the row itself, and it is the only
                         [M] row in the document with no per-statement settings behind it.
                         Corpus C1 is built by the script in §4 and pinned by a content
                         digest; §7 lists what is not measured, including the figures
                         whose corpus is not published

---

## 0. The change in one page

| | today | proposed |
|---|---|---|
| tables holding attributes | 2 — one row per span, plus `A` index rows per span | 1 — the attributes ride the span's own row as arrays |
| tables that answer "which traces hold value V" | `trace_attrs_idx`, one row per **span** | `trace_attr_traces`, one row per **trace** |
| client `INSERT`s per batch of spans | 2, on two independent flush generations | 1 |
| materialized views on the trace family | 2 | 5 |
| query shapes with no sorted path | 4 of 9 | **1 of 9** — the metrics range query keeps its full-window scan. §3.5 prices the exact rollup that would remove it: built, measured, same answer, 16.30 B/span, not taken |
| storage | 1047.9 B/span, 7.34 TB at 10⁹ spans/day and 7-day retention | **625.7 B/span, 4.38 TB** — **−40% in the worked model** (Appendix A's parameters). Two readings qualify it and both are in §5: substituting the identity columns' measured compressibility gives **−35.5%**, and the two families built and measured on corpus C1 give **−30.6%** at that corpus's `A` = 8 |
| merge work per span | 9856 LZ4-equivalent B per merge level | **5341** — **−46%** |
| bytes the writer sends ClickHouse | 1838 raw B/span in 2 statements | **1038 in 1** — **−44%** |
| SQL statements a one-condition search issues | 4 … 6252 | **3 … 3127** |

**The figures in that table are derived** — Appendix B's calculator at Appendix A's
parameters, so substitute your own and it recomputes them — **with two exceptions, both
marked in the cells themselves**: the storage row's −30.6%, which is the two families
built and measured on corpus C1 (§5), and the rollup's 16.30 B/span, which is §3.5's own
measurement.

**The read that was reported here as getting worse, restated.** A search batch's
phase-2 read was given as 2.2× the bytes it reads today. That figure was taken by
repeating an identical statement. ClickHouse 26.3 defaults
`use_query_condition_cache = 1`, which memoises the granules a condition selected, and a
search batch's statement carries a different 32-trace-id list every time, and a list
that has not been seen before is a miss. **How often a real deployment repeats a list is
not measured**; what was measured is that a fresh list misses even with the cache warm
from a previous one. On a first-seen batch with
`use_query_condition_cache = 0`, today's two statements read `15,809,612 + 48,571,546 =`
**64,381,158** bytes and the new single statement reads **30,144,320**: the new form reads
**0.47×**, not 2.2×. (26.3.29.7; `use_query_condition_cache=0`, `optimize_move_to_prewhere=1`, `max_block_size=65409`, `max_threads=auto(16)`; five repetitions, zero counter spread; corpus C1 with the index table of §2.1) Two earlier totals printed here, 63,987,313 and
64,332,690, were arithmetic on figures this document has since replaced. In the warm
regime the same corpus reproduces the direction, at 2.38×. §4 Q1 carries both
readings, the corpus and the full instrument.

**Whether the original measurement was itself a warm reading is not established.** It was
taken on a different corpus of a different physical size and its per-statement settings
were not recorded, so the cache explanation accounts for the reversal seen here without
proving what happened there.

---

## 1. What we store today

### 1.1 The tables, drawn

```
 trace_spans                                MergeTree      crates/pulsus-schema/src/catalog.rs:335-364
 PARTITION BY toDate(ts)   ORDER BY (trace_id, timestamp_ns)
 +------------------------------------------------------------------+
 | trace_id FixedString(16)   <- sort key 1: a trace's spans are     |
 |                               stored next to each other           |
 | span_id / parent_id  FixedString(8)                               |
 | name / service       LowCardinality(String)                       |
 | timestamp_ns Int64 CODEC(DoubleDelta, ZSTD(1))  <- sort key 2     |
 | duration_ns  Int64 CODEC(T64, ZSTD(1))                            |
 | status_code / kind / payload_type  Int8                           |
 | shared UInt8            (migration 31, crates/pulsus-schema/src/catalog.rs:648-658)        |
 | status_message String   (migration 35, crates/pulsus-schema/src/catalog.rs:738-748)        |
 | scope_name / scope_version LC (migration 37, crates/pulsus-schema/src/catalog.rs:775-786)  |
 | payload String CODEC(ZSTD(3))     <- the whole OTLP span, again   |
 | INDEX idx_duration duration_ns minmax GRANULARITY 4               |
 +------------------------------------------------------------------+
 | PROJECTION service_time  SELECT *  ORDER BY (service, ts)         |
 |     `-- SELECT * means the payload is stored a SECOND time        |
 | PROJECTION span_name_day (day, name, count())   (migration 42/43) |
 +------------------------------------------------------------------+

 trace_attrs_idx                     ReplacingMergeTree   crates/pulsus-schema/src/catalog.rs:365-388
 PARTITION BY date
 ORDER BY (key, val, scope, timestamp_ns, trace_id, span_id)
 +------------------------------------------------------------------+
 | date Date | key LC | val String | scope LC  <- the sorted prefix  |
 | val_num Nullable(Float64)   <- val's f64 parse, when finite       |
 | val_type LC     (migration 39, crates/pulsus-schema/src/catalog.rs:812-822)                |
 | timestamp_ns Int64          <- no codec                           |
 | trace_id FixedString(16)    <- 16 raw bytes; 5.65 on disk, §2.1  |
 | span_id  FixedString(8)                                           |
 | duration_ns Int64           <- a second copy of the span's        |
 +------------------------------------------------------------------+
   ONE ROW PER ATTRIBUTE PER SPAN.

 trace_tag_catalog                   ReplacingMergeTree   crates/pulsus-schema/src/catalog.rs:393-407
 ORDER BY (scope, key, val, val_type)
   no PARTITION BY, no time column, NO TTL
   (crates/pulsus-schema/src/controller.rs:479-480 says so in words: "a bounded catalog and
   carries no TTL"; it is absent from TTL_STMTS, crates/pulsus-schema/src/controller.rs:436-472)
   fed by trace_tag_catalog_mv (crates/pulsus-schema/src/catalog.rs:965-969):
        SELECT scope, key, val, val_type FROM trace_attrs_idx
        -- no GROUP BY

 trace_edges                         ReplacingMergeTree   crates/pulsus-schema/src/catalog.rs:692-716
 PARTITION BY date   ORDER BY (side, trace_id, span_id)
   fed by trace_edges_mv (crates/pulsus-schema/src/catalog.rs:985-1000)
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
onto **every** span it produces (`crates/pulsus-write/src/protocols/otlp_traces.rs:510-526`, the loop over
`[(resource, …), (span, …), (instrumentation, …)]`).

**That loop is not the whole of what the writer emits.** `crates/pulsus-write/src/protocols/otlp_traces.rs:528-630` emits,
per span, two more families of index row:

```
   per span EVENT   (crates/pulsus-write/src/protocols/otlp_traces.rs:528-579)
     event:name           scope event:intrinsic   val = the event name
     event:timeSinceStart scope event:intrinsic   val_num = event.time - span.start, ns
     one row per event attribute, scope `event`, verbatim key

   per span LINK    (crates/pulsus-write/src/protocols/otlp_traces.rs:581-630)
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
 count them:   1 base
             + 1 service_time projection row
             + 1 span_name_day aggregate row
             + 4 trace_attrs_idx
             + 4 trace_tag_catalog
             + 1 trace_edges
             = 12 physical rows written for one span.

 and the 24 bytes that name the span - trace_id + span_id - are
 written in 7 of those 12: the base row, the service_time copy,
 the 4 index rows and the edge row.  span_name_day and the catalog
 rows carry neither.
```

An earlier version of this block said 11 rows and 5 copies. Both were
undercounts: the `span_name_day` aggregate row was left out of the first, and
the edge row out of the second.

`val_num` is `val.parse::<f64>()` when the result is finite, else NULL
(`crates/pulsus-write/src/protocols/otlp_traces.rs:752-754`). It is set from the **text**, whatever OTLP type the
sender declared, which is why the string `"500"` under a different key would also
carry `val_num = 500`.

### 1.3 What that costs

Per span, compressed, at Appendix A's parameters:

| where the bytes are | B/span | share |
|---|---|---|
| `trace_spans` base row | 122.93 | 11.7% |
| `service_time` projection (`SELECT *`, so a second payload) | 137.60 | 13.1% |
| **`trace_attrs_idx`** | **787.33** | **75.1%** |
| `trace_tag_catalog` | ≈0 | ≈0% |
| **total** | **1047.9** | 7.34 TB at 10⁹ spans/day, 7-day retention |

**Two decimals in that table, because one does not add up.** `122.9 + 137.6 + 787.3` is
1047.8 against a total of 1047.9. The unrounded components are 122.933, 137.600 and
787.333 and they sum to 1047.87; Appendix B carries them at full precision. The 0.07
residual is in the first and third rows, and every other decomposition in this document
is rounded from the same unrounded figures.

Derived. The 75% depends on `A`, the number of attributes a span carries:

| `A` | 5 | 8 | 10 | **20** | 40 | 60 |
|---|---|---|---|---|---|---|
| index share of the family | 43.0% | 54.7% | 60.2% | **75.1%** | 85.8% | 90.1% |

---

## 2. What is wrong with it

### 2.1 Three quarters of storage is one table, and most of that table is names

One `trace_attrs_idx` row is 39.4 compressed bytes at Appendix A's parameters.
24 of them — `trace_id` 16 plus `span_id` 8 — are the **name of the span the row
points at**, and the worked model prices them at their full width.

**The assumption behind that price, stated as an assumption.** The model assumes a
workload in which no two rows adjacent under `(key, val, scope, timestamp_ns, trace_id,
span_id)` carry the same identity bytes. **Three things in the shipped system break it,
and all three are ordinary:**

- **two spans of one trace sharing an attribute value** sort together under that key,
  which is the whole of what the collapse factor `d` counts — at Appendix A's
  parameters a trace's rows for one value are `d` = 2.5 deep. Their `trace_id` bytes are
  identical and adjacent; their `span_id` bytes differ;
- **one span repeating a key at the same scope with two different values.** §4 Q1
  establishes that a span may carry a key more than once — that is the whole of the
  duplicate-key rule. Two such rows differ in `val` and agree on everything else, so they
  sort adjacently and **both identity columns are adjacent copies of themselves**, from
  one delivery, with no replay. Measured on 26.3.29.7 against the committed DDL: a span
  carrying `k = 'x'` and `k = 'y'` leaves **two rows after `OPTIMIZE … FINAL`**, adjacent
  in sort order, `uniqExact((trace_id, span_id))` = 1 across them.

  **The same-value variant does not survive to be measured, and an earlier version of
  this list named it instead.** Two rows identical in all six sort columns collapse in the
  insert's own optimisation pass; with `optimize_on_insert = 0` they survive the insert as
  two and collapse at `OPTIMIZE … FINAL`. Since the figures below are taken after
  `OPTIMIZE … FINAL`, that variant is transient input and can explain nothing in them;
- **an allowed replay.** The table is a `ReplacingMergeTree` keyed on all six sort
  columns, so re-delivering a span writes a byte-identical row that sits next to its
  twin until a merge collapses it. That is not a fault state; it is the at-least-once
  delivery the write path is built for (§3.4).

**So the 16 bytes are a worked-model price, not a floor**, and measured they compress:
by how much depends on how often one trace id lands inside one granule, and the table
below is the reading. **Every figure in it is taken after `OPTIMIZE … FINAL`**, so what
it prices is what survives a merge — the first and second cases above — and not anything
transient.

```
   one index row, 39.4 compressed bytes

   [ val 4.7 ][ num 1.7 ][ ts 5.0 ][  trace_id 16.0  ][ span_id 8.0 ][ dur 4.0 ]
                                   |<-------- 24.0 = 61% ---------->|

   paid A = 20 times per span  ->  480 B/span = 46% of the whole family
```

Derived. **Measured** on corpus C1 — the build published in §4, with the index
table built from C1's own span rows by the statement below so that the whole
corpus is one script. ClickHouse 26.3.29.7, `OPTIMIZE … FINAL`, `A` = 8,
16,000,000 index rows. One reading: `system.parts_columns` sums bytes on disk,
which is a stored quantity, not a timing, so there is no spread to report.

```sql
-- these are the last three statements of §4's corpus script, quoted here
CREATE TABLE c1.attrs_old (
  date Date, key LowCardinality(String), val String, scope LowCardinality(String),
  val_num Nullable(Float64), timestamp_ns Int64, trace_id FixedString(16),
  span_id FixedString(8), duration_ns Int64, val_type LowCardinality(String) DEFAULT ''
) ENGINE = MergeTree PARTITION BY date
ORDER BY (key, val, scope, timestamp_ns, trace_id, span_id) SETTINGS ttl_only_drop_parts = 1;

INSERT INTO c1.attrs_old
SELECT toDate(fromUnixTimestamp64Nano(timestamp_ns)), k, v, sc, vn,
       timestamp_ns, trace_id, span_id, duration_ns, vt
FROM c1.spans_new
ARRAY JOIN attr_key AS k, attr_val AS v, attr_scope AS sc, attr_num AS vn, attr_type AS vt;

OPTIMIZE TABLE c1.attrs_old FINAL;
```

| column of `trace_attrs_idx` | bytes on disk | B/row | share |
|---|---|---|---|
| `span_id` | 128,558,879 | 8.03 | 37.44% |
| `trace_id` | 90,473,312 | 5.65 | 26.35% |
| `timestamp_ns` | 90,093,417 | 5.63 | 26.23% |
| `duration_ns` | 22,524,063 | 1.41 | 6.56% |
| `val` | 10,668,963 | 0.67 | 3.11% |
| `val_num` | 650,440 | 0.04 | 0.19% |
| `date` + `key` + `scope` + `val_type` | 377,107 | 0.02 | 0.11% |
| **total** | **343,412,406** | **21.46** | |

`trace_id` + `span_id` is **63.8%** of the table, and 90.0% once the timestamp is
counted with them. **The drawing above and the measurement disagree about one
number, and the difference is a parameter, not an error.** The drawing prices
`trace_id` at its full 16 bytes because Appendix A's workload has each trace id
appearing `A·S` times in a window of 10⁴ rows; on C1 each id appears 96 times
and LZ4 finds those repeats, so the stored cost is 5.65. Everything derived from
`idx_today` moves with that ratio — §7 carries it and says which way.

**Can the 24 bytes be made narrower? Measured, not asserted.** Seven codecs were
attempted on `trace_id` and `span_id` in that exact sort order; five were accepted,
each into its own table loaded from the same rows and each `OPTIMIZE … FINAL`, and
two were rejected by the server. The figure is the two columns' bytes on disk over
16,000,000 rows:

| codec on both columns | trace_id | span_id | both, B/row |
|---|---|---|---|
| `NONE` | 256,054,974 | 128,054,948 | 24.01 |
| `LZ4` — what ships today | 90,473,312 | 128,558,879 | **13.69** |
| `LZ4HC(9)` | 90,395,454 | 128,558,639 | 13.68 |
| `ZSTD(1)` | 77,075,483 | 128,074,488 | **12.82** |
| `ZSTD(9)` | 77,052,216 | 128,074,488 | 12.82 |
| `Delta, ZSTD(1)` | rejected | | `Code: 36` — Delta takes types of size 1, 2, 4, 8 |
| `T64, ZSTD(1)` | rejected | | `Code: 431` — T64 does not take `FixedString(16)` |

So the claim an earlier version of this section made — *no compression codec beats
14.5 bytes* — is false twice over on this corpus. The shipped default already
stores the pair at 13.69 B/row, and `ZSTD(1)` takes it to 12.82, a further 6.3%
(13,882,220 bytes over the table). The 14.5-byte figure is an information bound on
**one** id drawn uniformly from `2¹²⁸` and sorted within a run of 10⁴ — a bound on
the incompressible case, which C1 is not, because its ids repeat.

**What survives, and the scope of it.** Even at the cheapest of the seven codecs
tried, the two identity columns are **62.3%** of the table (205,149,971 of
329,530,186 once the swap is applied to the table above), and they are paid `A`
times per span. The codec change is worth 13,882,220 bytes — 4.0% of the index
table, 3.3% of the two C1 tables together (423,339,156 bytes) — against the 40% of
the family the row-count change in §3 is worth.

**Stated as measured, not as a law:** *of the seven codecs tried, on this corpus,
in this sort order, the best narrows the pair by 6.3% and the design's row-count
change is worth an order of magnitude more.* An earlier version of this line read
"the number of rows has to fall, not their width", which is a claim about every
possible encoding and is not what was run — a column-level re-encoding this
document did not try (a dictionary over trace ids, a per-part id table, a shorter
synthetic id) is not covered by seven codecs. §11.1 carries the one such idea the
document has priced: a 40-bit fingerprint, which the calculator prices at 24.85
bytes a row against 36.85. Taking any of them is not part of this design; §7 lists
it.

### 2.2 Four of the nine query shapes have no sorted path

A ClickHouse table has one sort order. Our eight shapes want six. Ranked by our
own planner, lower is better (`crates/pulsus-read/src/traces/filter.rs:89-103`):

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
in rank 4 or 5. `docs/schemas.md:790` already names the class: *"no selective
index — window-bounded, budget-limited"*.

The proportions of a real query mix were derived separately, by reading what the
Grafana traces datasource plugin generates (that reading is not reproduced here).
Its result: **84–97% of the rows a search reads come from shapes with no sorted
path**, 93.5% at the worked point; and **about 5% come from the attribute index
that costs 75% of the storage.**

### 2.3 The tag dropdown's scan grows with the age of the deployment

`trace_tag_catalog` has no time column, no partition key and no TTL
(`crates/pulsus-schema/src/catalog.rs:393-407`; `crates/pulsus-schema/src/controller.rs:479-480`). Every distinct
`(scope, key, val, val_type)` ever ingested stays in it for ever.

```
   rows the dropdown scans

   today       K_tot  = every tuple the deployment has EVER seen, unbounded
   bounded     K_d·W  = tuples produced in the query window

   worked: 10^6 against 10^4 - a factor of 100, and it grows every day
```

And the request that reads it already computes a window and throws it away. The
values route parses `start`/`end`, defaulting to `traceql_tag_lookback` = 24 h
(`crates/pulsus-config/src/model.rs:537`, `crates/pulsus-server/src/traces_api/tags.rs:161-164`), then calls
`tag_values_sql` (`crates/pulsus-read/src/traces/tags_sql.rs:118-127`), which emits no time predicate at all —
because the table it reads has no time column.

### 2.4 The narrowed dropdown is a join over the window

Open a tag-value dropdown while a service filter is set and the read becomes a
semi-join between the two tables at **day** grain
(`crates/pulsus-read/src/traces/tags_sql.rs:282-312`, chosen at `crates/pulsus-read/src/traces/exec.rs:1825-1866`):

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

**Measured** on corpus C1 with the index table of §2.1, ClickHouse 26.3.29.7,
`use_query_condition_cache = 0`, `optimize_move_to_prewhere = 1`,
`max_block_size = 65409`, `max_threads = auto(16)`, `key = 'http.status_code'`,
`service = 'svc-3'`, the statement above run exactly as printed. **The reading
depends on one thing outside the statement, and that is why it is given twice:**

| the span table the sub-select reads | read_rows | read_bytes | marks |
|---|---|---|---|
| C1's `spans_old` as the §4 script builds it — **no projections** | 4,015,232 | 132,515,311 | 494 |
| the same rows with `service_time` added, which is what ships | 2,129,920 | 70,298,975 | 260 |

Both return `400, 500, 503`. The second is the production shape and is the figure
§5 and §4 Q4c carry; the first is what a reader who rebuilds C1 from the published
script and runs this statement will see, because that script creates no
projections. An earlier version of this section gave 2,138,112 rows / 74.6 MB with
no instrument beside it, and a second pair at 20,000,000 spans whose corpus is
withdrawn (§5); both are replaced by the table above. It cannot be answered off
the catalog, because
`trace_tag_catalog_mv` reads `trace_attrs_idx`, which has no `service` column
(`crates/pulsus-schema/src/catalog.rs:965-969`, `crates/pulsus-schema/src/catalog.rs:365-388`).

### 2.5 Three ways a failed write leaves the two tables disagreeing

The writer sends two `INSERT`s on two independent flush generations
(`crates/pulsus-write/src/writer/trace.rs:9-19`, and `admit_batch` at `:220-304` appends to two separate
buffers drained by two separate tasks). A reader can therefore see a span without
its attribute rows during the settle window. That window is temporary and the
module documents it. These three are not temporary:

| # | what fails | where | what is left behind |
|---|---|---|---|
| 1 | the `trace_attrs_idx` insert is **definitely** not committed; its rows go to a bounded in-memory backlog, and a row that would push the backlog over `backfill_max_bytes` is dropped and counted | `crates/pulsus-write/src/writer/trace.rs:137-185`; `crates/pulsus-write/src/writer/backfill.rs:189-201`; the backlog's byte cap, `crates/pulsus-write/src/writer/backfill.rs:78-90` | the span is stored; its attributes never arrive. It is fetchable by id and **invisible to attribute search, permanently** |
| 2 | the backlog's own re-insert returns `InsertUncertain`, or any deterministic error | `crates/pulsus-write/src/writer/backfill.rs:214-220` — both branches remove the entry and count it abandoned, never retried | as above |
| 3 | the **`trace_spans`** insert fails, definitely or uncertainly. `trace_spans` passes `on_flush_poisoned: None` (`crates/pulsus-write/src/writer/trace.rs:172`) — it is the structural append-only exclusion (`crates/pulsus-write/src/writer/backfill.rs:23-28`), so nothing ever replays it | `crates/pulsus-write/src/writer/table.rs:367-434`, which spools the rows to disk as an audit record and settles the generation with an error | the attribute rows are stored; the span is not. A search generates that trace as a candidate and its hydration returns nothing |

In all three the client is told the write failed. What it is not told is *which
half* survived.

### 2.6 The payload is compressed with ZSTD(3), twice

`service_time` is `SELECT *` (`crates/pulsus-schema/src/catalog.rs:353-355`), so the `payload` column is
stored a second time and re-compressed on every merge. Of the 9856
LZ4-equivalent bytes a span costs per merge level, 8000 are those two ZSTD(3)
passes. Nothing reads `payload` from the projection: the only statement that
selects it is the trace-by-id point read, and that filters on `trace_id`, which
is sort key 1 of the **base** table (`crates/pulsus-read/src/traces/sql.rs:16-26`).

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
what `log_streams_idx_mv` does (`crates/pulsus-schema/src/catalog.rs:934-942`). A `GROUP BY` inside one,
feeding `SimpleAggregateFunction` columns of an `AggregatingMergeTree`, is what
`log_metrics_*_mv` does into `log_metrics_*` (`crates/pulsus-schema/src/catalog.rs:945-953`, table at
`crates/pulsus-schema/src/catalog.rs:266-281`). And the two **together in one view** — `ARRAY JOIN` over
`trace_spans`'s arrays plus a `GROUP BY` — was built and run for the tag catalog,
where it produced the same rows as the two-table build. Re-run on corpus C1 with
the index table of §2.1, ClickHouse 26.3.29.7 — **as a comparison of the two row
SETS, because equal counts are not equal rows:**

```sql
WITH a AS (SELECT DISTINCT scope, key, val, val_type FROM c1.attrs_old),
     b AS (SELECT DISTINCT sc AS scope, k AS key, v AS val, vt AS val_type
           FROM c1.spans_new
           ARRAY JOIN attr_key AS k, attr_val AS v, attr_scope AS sc, attr_type AS vt)
SELECT (SELECT count() FROM a),
       (SELECT count() FROM b),
       (SELECT count() FROM (SELECT * FROM a EXCEPT SELECT * FROM b)),
       (SELECT count() FROM (SELECT * FROM b EXCEPT SELECT * FROM a))
```

    rows through the index          2,166,747
    rows off the span row's arrays  2,166,747
    in the index build, not the array build          0
    in the array build, not the index build          0

**The digest, with the serialisation it is taken over**, because a hash of a set means
nothing without one. Each row becomes `scope`, `key`, `val`, `val_type` joined by single
tab characters; the rows are sorted ascending as strings; the sorted list is joined by
single newlines; `SHA256` is taken over those bytes and printed lowercase hexadecimal.
As a statement, run once per side:

```sql
SELECT lower(hex(SHA256(arrayStringConcat(
         arraySort(groupArray(concat(scope, '\t', key, '\t', val, '\t', val_type))), '\n'))))
FROM (SELECT DISTINCT scope, key, val, val_type FROM c1.attrs_old)
```

    both sides   8f0971e15e588e0413f979fc63947140e690bb388a8132a557915df1cab9a7cb

An earlier version of this passage gave 1,094,467 for both, on a corpus that is
not C1 and is not published, and a later one compared only the two counts. Equal
counts would have been satisfied by two different sets of the same size; the two
`EXCEPT` counts and the digest are what establish the claim.

**One mechanical fact about this view, measured because it bit the build in §5.** Its
`SELECT` lists columns in a different order from the target table's columns, and that is
safe because a `TO` view matches **by name** — verified on 26.3.29.7 with a two-column
target and a view listing them reversed. The same body used as `INSERT … SELECT` with no
column list matches by **position** and fails with `Code: 48`. Any backfill names its
columns.

**What has never been run is that combination writing `SimpleAggregateFunction`
columns**, which is what `ts_max`, `dur_max` and `dur_min` are. That is the first
thing to try if this design is taken — §11 P1.

The whole shape is also what the logs family already does: `log_streams_idx` is
sorted `(key, val, fingerprint)` with one row per `(key, val, stream)`, never one
per sample (`crates/pulsus-schema/src/catalog.rs:227-234`).

### 3.2 The same span, and every row it now produces

```
 trace_spans   1 base row - the same columns as before, plus

   attr_key   ['service.name','deployment.environment','http.status_code','http.method']
   attr_scope ['resource',   'resource',              'span',            'span']
   attr_val   ['checkout',   'prod',                  '500',             'GET']
   attr_type  ['string',     'string',                'int',             'string']
   attr_num   [NULL,         NULL,                    500,               NULL]

   A span carrying one event and one link appends, to the SAME five arrays and in
   the writer's existing emission order (crates/pulsus-write/src/protocols/otlp_traces.rs:528-630):

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
(`crates/pulsus-write/src/protocols/otlp_traces.rs:481-502`), so `intDiv(timestamp_ns, 3·10¹¹)` lies in
`[0, 1.43·10⁷]` against a `UInt32` ceiling of 4.29·10⁹.

### 3.4 Every new table is safe against a duplicated span row, and one would not have been

Spans are written at least once and never deduplicated. `trace_spans` is a plain
`MergeTree` (`crates/pulsus-schema/src/catalog.rs:335-364`). Our own writer never replays a block whose
commit fate is unknown — a failure after the bytes are sent is classified and
never retried, *"the one hard invariant this crate enforces"*
(`crates/pulsus-write/src/writer/table.rs:313-321`) — but nothing stops a client resending the same
spans. So the read path counts spans as `uniqExact(trace_id, span_id)` rather
than `count()`, and says why in words: *"at-least-once replays must never inflate
a bucket"* (`crates/pulsus-read/src/traces/metrics_sql.rs:9-12`).

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
what follows is the price, not an impossibility.**

The obvious form of it is wrong, and that much is unchanged:

```
   the query today            uniqExact(trace_id, span_id)   counts DISTINCT spans
   a count-keeping rollup     sum(count)                     counts ROWS

   one span written twice ->  today  1        rollup  2
```

The exact form is `uniqExactState((trace_id, span_id))`. An earlier version of
this section said that such a state "is the size of the data it was meant to
summarise, and the rollup saves nothing", and said plainly that this was asserted
rather than run. **It has now been built and run, and the assertion is wrong.**

Built on corpus C1 — 2,000,000 spans, three hours — at `B_r` = 60 s, keyed exactly
as above, ClickHouse 26.3.29.7:

```sql
CREATE TABLE c1.rollup_exact (
  bucket_ns Int64, service LowCardinality(String), name LowCardinality(String),
  status_code Int8, kind Int8,
  spans AggregateFunction(uniqExact, Tuple(FixedString(16), FixedString(8))),
  cnt SimpleAggregateFunction(sum, UInt64), dur_sum SimpleAggregateFunction(sum, UInt64),
  dur_min SimpleAggregateFunction(min, Int64), dur_max SimpleAggregateFunction(max, Int64)
) ENGINE = AggregatingMergeTree
PARTITION BY toDate(fromUnixTimestamp64Nano(bucket_ns))
ORDER BY (bucket_ns, service, name, status_code, kind);

INSERT INTO c1.rollup_exact SELECT
  intDiv(timestamp_ns, 60000000000)*60000000000 AS bucket_ns, service, name, status_code, kind,
  uniqExactState((trace_id, span_id)), count(), sum(duration_ns), min(duration_ns), max(duration_ns)
FROM c1.spans_new GROUP BY bucket_ns, service, name, status_code, kind;
OPTIMIZE TABLE c1.rollup_exact FINAL;
```

**What it gave.**

| | |
|---|---|
| rows | 54,300 |
| bytes on disk | 32,592,559 = **16.30 B/span** |
| of which the exact state | 32,181,063 = 16.09 B/span, **98.7%** |
| the four plain aggregates and the five key columns | 0.21 B/span |
| the four base columns the metrics statement reads today (`span_id` 8.035, `trace_id` 1.599, `timestamp_ns` 0.972, `service` 0.217) | 10.82 B/span |

**The answer is identical.** `SELECT intDiv(bucket_ns, 60000000000), service,
uniqExactMerge(spans) … GROUP BY …` and the same query against `spans_new` with
`uniqExact((trace_id, span_id))` produce byte-identical result sets — `sha256` of
the TSV agrees, `3377317c4a7fdca4…` on both.

**Instrument for the read comparison below**: ClickHouse 26.3.29.7,
`use_query_condition_cache = 0`, `max_block_size = 65409`, three repetitions at each of
`max_threads` = 1, 4 and 16, corpus C1. `read_rows` and `read_bytes` are identical at
every setting; **memory is not, and its ordering reverses**:

| `max_threads` | full scan, peak memory | rollup, peak memory | which is higher | full scan, ms | rollup, ms |
|---|---|---|---|---|---|
| 1 | 127,640,933 – 127,641,093 | 142,020,704 – 142,022,400 | the rollup | 416 / 474 / 475 | 98 / 104 / 154 |
| 4 | 137,638,581 – 141,724,728 | 145,086,633 – 147,361,597 | the rollup | 144 / 146 / 160 | 75 / 81 / 85 |
| 16 | 188,430,810 – 196,099,808 | 145,086,633 – 151,557,949 | **the full scan** | 95 / 95 / 107 | 70 / 89 / 90 |

An earlier version of this section printed one memory pair with no thread setting beside
it, which is the pair at `max_threads` = 4 and is the setting at which the rollup looks
worst. The honest summary is that peak memory is of the same order either way and which
is larger depends on the thread count, while the read is 5.8× fewer bytes at every
setting.

**And it is duplicate-safe, which is the property §3.4 rejected the plain rollup
for.** The rollup rows for one interval — `WHERE timestamp_ns < 1700000000000000000 +
600000000000`, the corpus's first ten minutes, which is 111,015 of its 2,000,000 spans —
were inserted a second time by re-running the same `INSERT … SELECT` with that predicate
added:

    sum(cnt)              2,000,000  ->  2,111,015     <- counts ROWS, inflates
    uniqExactMerge(spans) 2,000,000  ->  2,000,000     <- counts DISTINCT SPANS
    the base table                       2,000,000

**So the honest statement is a price, not an impossibility:**

| | rollup, exact | the full scan today |
|---|---|---|
| rows read for the 3-hour query | 54,300 | 2,000,000 |
| bytes read | 11,348,700 | 66,001,296 – 66,001,328 |
| peak memory | see the table above — it depends on `max_threads` and the ordering reverses | |
| storage added | 16.30 B/span | — |

5.8× fewer bytes read, the same answer, at 16.30 B/span of extra storage — 2.6% of
the 625.7 B/span this design arrives at. The state is one entry per distinct span,
so it scales with `N` and not with `n_grp`; a wider grouping key would not make it
smaller, and a narrower one would not either.

**This design still does not take it**, and the reason is now a choice rather than
a wall: one query shape of nine, **16.30 B/span on C1** — every span contributes an
entry to the state whether or not anyone runs a metrics query, and the figure itself is
one corpus's, not a rate checked at a second scale (§7) — and a second write path to
keep consistent with the span table. Whoever revisits it has the numbers above rather than an assertion.
The rule the rest of this document follows is exact-or-refuse, and refusing is
always available.

What it would have cost, for whoever revisits this:

| rollup bucket `B_r` | `count`/`sum`/`min`/`max` | plus a latency sketch |
|---|---|---|
| 60 s | 0.38 B/span | +51.8 B/span |
| 300 s | 0.08 B/span | +10.4 B/span |
| 900 s | 0.03 B/span | +3.5 B/span |

at 3·10⁴ distinct groups — the count-keeping form, not the exact one. The sketch is
99% of that form, and it would be 7.6% of the whole family at one-minute buckets.

An earlier version of this paragraph ended: *two things would make the rollup
possible — an ingest path that guarantees each span row is written exactly once, or
a metrics answer defined on rows rather than on distinct spans.* **There is a third,
and it is the one measured above:** keep the distinct-span state itself, at
16.30 B/span. The first two remain the ways to get the cheap 0.38 B/span form.

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

**Corpus C1, which every `[M]` figure below names except one** — §5's write wall time,
whose 20,000,000-span corpus is not this one and is not published, as that row says.
C1 is 2,000,000 spans,
166,667 traces (166,666 of 12 spans and one of 8 — 2,000,000 does not divide
by 12), 8 attributes per span, three hours from
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

**C1 is pinned by the construction itself, not by prose and not by a layout digest.**
An earlier version of this section published only the final digest query; reconstructing
the build from the surrounding prose produced 146/101 marks and a different digest, which
is what an unpublished construction is worth. The script, in full:

```bash
#!/bin/bash
set -eu
CH="${1:?usage: $0 <clickhouse-http-endpoint>}"   # no default: pass the endpoint
q() { curl -sS --data-binary @- "$CH/?database=c1&max_insert_threads=1&max_threads=1&max_block_size=65409&max_execution_time=3600"; }
curl -sS --data-binary "DROP DATABASE IF EXISTS c1" "$CH/" >/dev/null
curl -sS --data-binary "CREATE DATABASE c1" "$CH/" >/dev/null
for T in spans_old spans_new; do
  EXTRA=""
  [ "$T" = spans_new ] && EXTRA=",
    attr_key Array(LowCardinality(String)), attr_scope Array(LowCardinality(String)),
    attr_val Array(String), attr_type Array(LowCardinality(String)), attr_num Array(Nullable(Float64))"
  q <<EOF
CREATE TABLE c1.$T (
  trace_id FixedString(16), span_id FixedString(8), parent_id FixedString(8),
  name LowCardinality(String), service LowCardinality(String),
  timestamp_ns Int64 CODEC(DoubleDelta, ZSTD(1)), duration_ns Int64 CODEC(T64, ZSTD(1)),
  status_code Int8, kind Int8, payload_type Int8, shared UInt8, status_message String,
  scope_name LowCardinality(String), scope_version LowCardinality(String),
  payload String CODEC(ZSTD(3))$EXTRA
) ENGINE = MergeTree
PARTITION BY toDate(fromUnixTimestamp64Nano(timestamp_ns))
ORDER BY (trace_id, timestamp_ns) SETTINGS ttl_only_drop_parts = 1
EOF
done
for i in 0 1 2 3 4 5 6 7; do LO=$((i*250000))
 for T in spans_new spans_old; do
  COLS=""
  [ "$T" = spans_new ] && COLS=",
  ['service.name','deployment.environment','k8s.cluster','http.method','http.status_code','http.target','user.id','request.id'],
  ['resource','resource','resource','span','span','span','span','span'],
  [concat('svc-', toString(intDiv(n,3) % 20)), 'prod', 'eu-west-1',
   ['GET','POST','PUT','DELETE'][(n % 4) + 1], ['200','400','500','503'][(n % 4) + 1],
   concat('/api/v1/r', toString(n % 50)), concat('u-', toString(intDiv(n,12))), concat('r-', toString(n))],
  ['string','string','string','string','int','string','string','string'],
  [NULL,NULL,NULL,NULL, toFloat64([200,400,500,503][(n % 4) + 1]), NULL,NULL,NULL]"
  q <<EOF
INSERT INTO c1.$T SELECT
  reinterpretAsFixedString(sipHash128(intDiv(n,12))), reinterpretAsFixedString(sipHash64(n)),
  reinterpretAsFixedString(sipHash64(intDiv(n,12)*12)),
  concat('GET /op/', toString(n % 50)), concat('svc-', toString(intDiv(n,3) % 20)),
  toInt64(1700000000000000000 + intDiv(n,12)*64800000 + (n%12)*100000000),
  toInt64(100000 + (n % 997) * 3000000),
  if(n % 100 = 0, toInt8(2), toInt8(0)), toInt8(n % 5), toInt8(0), toUInt8(0), '', 'scope', '1.0',
  repeat(substring(concat(lower(hex(sipHash128(n))), lower(hex(sipHash128(n+1))),
                          lower(hex(sipHash128(n+2))), lower(hex(sipHash128(n+3)))), 1, 100), 4)$COLS
FROM (SELECT number AS n FROM numbers($LO, 250000))
EOF
 done
done
for T in spans_old spans_new; do curl -sS --data-binary "OPTIMIZE TABLE c1.$T FINAL" "$CH/?database=c1&max_execution_time=3600" >/dev/null; done
# The attribute index the "today" side of every measurement reads, built from the
# span rows above so that the whole corpus is one script (§2.1 quotes these three).
q <<'EOF'
CREATE TABLE c1.attrs_old (
  date Date, key LowCardinality(String), val String, scope LowCardinality(String),
  val_num Nullable(Float64), timestamp_ns Int64, trace_id FixedString(16),
  span_id FixedString(8), duration_ns Int64, val_type LowCardinality(String) DEFAULT ''
) ENGINE = MergeTree PARTITION BY date
ORDER BY (key, val, scope, timestamp_ns, trace_id, span_id) SETTINGS ttl_only_drop_parts = 1
EOF
q <<'EOF'
INSERT INTO c1.attrs_old
SELECT toDate(fromUnixTimestamp64Nano(timestamp_ns)), k, v, sc, vn,
       timestamp_ns, trace_id, span_id, duration_ns, vt
FROM c1.spans_new
ARRAY JOIN attr_key AS k, attr_val AS v, attr_scope AS sc, attr_num AS vn, attr_type AS vt
EOF
curl -sS --data-binary "OPTIMIZE TABLE c1.attrs_old FINAL" "$CH/?database=c1&max_execution_time=3600" >/dev/null
```

**The identity is content, not layout.** A digest over parts, rows and marks alone does
not distinguish two corpora whose queries behave differently. Measured: a second corpus
built by the same script with the span name changed from `n % 50` to `n % 51` produced the
**identical** layout digest `e58c2eb3…` and a different `{name = …}` granule count. The
identity below covers both:

```sql
WITH
  (SELECT arrayStringConcat(groupArray(concat(table,'|',name,'|',toString(rows),'|',toString(marks))), ';')
     FROM (SELECT table, name, rows, marks FROM system.parts
           WHERE database='c1' AND active AND table IN ('spans_old','spans_new')
           ORDER BY table, name)) AS layout,
  (SELECT arrayStringConcat(arraySort(groupArray(concat(k,'=',v))), ';') FROM (
     SELECT 'a_rows' AS k, toString(count()) AS v FROM spans_new
     UNION ALL SELECT 'b_traces',   toString(uniqExact(trace_id))        FROM spans_new
     UNION ALL SELECT 'c_names',    toString(uniqExact(name))            FROM spans_new
     UNION ALL SELECT 'd_services', toString(uniqExact(service))         FROM spans_new
     UNION ALL SELECT 'e_errors',   toString(countIf(status_code = 2))   FROM spans_new
     UNION ALL SELECT 'f_tsmin',    toString(min(timestamp_ns))          FROM spans_new
     UNION ALL SELECT 'g_tsmax',    toString(max(timestamp_ns))          FROM spans_new
     UNION ALL SELECT 'h_cells',    toString(sum(length(attr_key)))      FROM spans_new
     UNION ALL SELECT 'i_sumdur',   toString(sum(duration_ns))           FROM spans_new
     UNION ALL SELECT 'j_ckold',    toString(sum(sipHash64(trace_id, timestamp_ns, name, service, status_code, duration_ns))) FROM spans_old
     UNION ALL SELECT 'k_cknew',    toString(sum(sipHash64(trace_id, timestamp_ns, name, service, status_code, duration_ns))) FROM spans_new
  )) AS content
SELECT lower(hex(SHA256(concat(layout, '#', content)))) AS c1_identity
```

    C1                                  fa2b69757f6737a368868c47cfd210e8eeb7043d0c8ea7277c2ae10a01ef1581
    the same script, name n % 51        5119ae707931f577f7d8c29d3e8526bf569bb46a46f6930a8d84b4ab9e68fa0e
    layout-only digest, BOTH corpora    e58c2eb30291fa579a90fe947f3327776424ffcce2d999507c327c5f39b9d1a0

    spans_new  20231114_1_5_1  1,185,089 rows  148 marks
    spans_new  20231115_6_9_1    814,911 rows  102 marks
    spans_old  20231114_1_5_1  1,185,089 rows  148 marks
    spans_old  20231115_6_9_1    814,911 rows  102 marks

**The mark arithmetic, corrected.** `system.parts.marks` counts one terminal mark per
part. C1's window runs 22:13:20 → 01:13:20 UTC, so `PARTITION BY toDate(…)` gives **two
parts** with 148 + 102 = 250 marks, which is `(148−1) + (102−1) = 248` readable granules —
the figure `EXPLAIN` and `SelectedMarks` report. A corpus inside one UTC day is one part
of `ceil(2,000,000 / 8,192) = 245` readable granules, which `system.parts` records as
**246** marks. An earlier version of this section equated the two counts.

### Q0 — `{}`, the query the search form sends before you type anything

```sql
-- today            crates/pulsus-read/tests/golden/traces_search/existence_absent.sql:5-10 has this shape
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
(`crates/pulsus-read/tests/golden/traces_search/worked_example.sql:5-11`):

```sql
SELECT trace_id, max(timestamp_ns) AS bound_ts
FROM trace_spans PREWHERE service = 'checkout'
WHERE timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000
GROUP BY trace_id ORDER BY bound_ts DESC, trace_id ASC LIMIT 100001
```

Phase 2 takes 32 candidates at a time (`crates/pulsus-read/src/traces/exec.rs:117`). **Today that is two
statements per batch** — one to fetch the spans, one to ask the index which of
them carry the attribute:

```sql
-- today, statement 1 of 2   (crates/pulsus-read/src/traces/search_sql.rs:230-252)
SELECT trace_id, span_id, parent_id, <byte-capped service>, <byte-capped name>,
       timestamp_ns, duration_ns, status_code, <byte-capped status_message>, kind,
       <byte-capped scope_name>, <byte-capped scope_version>
FROM trace_spans
WHERE trace_id IN (…32 ids…)
  AND timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000
ORDER BY trace_id ASC, timestamp_ns ASC, span_id ASC
LIMIT 10001 BY trace_id

-- today, statement 2 of 2   (crates/pulsus-read/src/traces/search_sql.rs:286-312)
SELECT DISTINCT trace_id, span_id, <byte-capped val> AS v, val_type AS t
FROM trace_attrs_idx
WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15')
  AND (key = 'http.status_code' AND val_num >= 500 AND scope = 'span')
  AND timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000
  AND trace_id IN (…the same 32 ids…)
```

**New: the membership read becomes a column of the first statement.**

```sql
WITH arrayFirstIndex((key, scope) -> key = 'http.status_code' AND scope = 'span',
                     attr_key, attr_scope) AS i0
SELECT trace_id, span_id, parent_id, <byte-capped service>, <byte-capped name>,
       timestamp_ns, duration_ns, status_code, <byte-capped status_message>, kind,
       <byte-capped scope_name>, <byte-capped scope_version>,
       (i0 != 0) AND ifNull(attr_num[i0] >= 500, 0) AS probe0
FROM trace_spans
PREWHERE trace_id IN (…32 ids…)
WHERE timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000
ORDER BY trace_id ASC, timestamp_ns ASC, span_id ASC
LIMIT 10001 BY trace_id
```

**Locate, then test — and an earlier version of this section got that wrong.** It
rendered the probe as
`arrayExists((key, scope, val_num) -> (key = … AND val_num >= 500 AND scope = …), …)`,
which asks *does SOME element satisfy the predicate*. That contradicts the rule this
document settles three paragraphs below: a span has **one** value for an attribute, the
the first stored element within the highest-precedence scope that is present, and every
operation has to use that one. The two forms disagree
on exactly the spans that repeat a key. Measured, on a fixture of eight spans:

| the span's stored attributes | the value it HAS | `arrayExists(… = 'x')` | locate-then-test | correct |
|---|---|---|---|---|
| no `k` | absent | 0 | 0 | 0 |
| `k='x'` | x | 1 | 1 | 1 |
| `k='x'`, `k='y'` | x | 1 | 1 | 1 |
| **`k='y'`, `k='x'`** | **y** | **1** | **0** | **0** |
| `k='y'` | y | 0 | 0 | 0 |
| `j='x'` only | absent | 0 | 0 | 0 |
| **`resource.k='x'`, `span.k='y'`, unscoped `.k`** | **y** | **1** | **0** | **0** |
| **`n='7'`, `n='5'`, filter `span.n = 5`** | **7** | **1** | **0** | **0** |

Three of the eight disagree, and in each the `arrayExists` form returns a span whose
own value does not satisfy the query — the span would then be rendered with the other
value by `select()`. **Filtering and reading now follow one rule.**

**All three of those rows are changes of answer against what ships today**, because
`arrayExists` is what today's membership read amounts to: a span with a matching entry
anywhere is in the set. Counted as **rows of the fixture the answer moves on, three**;
counted as **kinds of shape that move it, two** — a key repeated inside one scope, and a
key present at two scopes under an unscoped condition — with the third row being the
first kind seen through a negation. The ledger row below is written against the two
kinds and names all three shapes, so neither count is left to be inferred.

**The locate is the same expression the value read uses**, so there is one rule and one
place it is written: `arrayFirstIndex` over `(key, scope)` for a scoped condition, and
for an unscoped one the five-scope chain, in the order span → resource → event → link →
instrumentation:

```sql
WITH arrayFirstIndex((k, s) -> k = 'k' AND s = 'span',            attr_key, attr_scope) AS i_span,
     arrayFirstIndex((k, s) -> k = 'k' AND s = 'resource',        attr_key, attr_scope) AS i_res,
     arrayFirstIndex((k, s) -> k = 'k' AND s = 'event',           attr_key, attr_scope) AS i_evt,
     arrayFirstIndex((k, s) -> k = 'k' AND s = 'link',            attr_key, attr_scope) AS i_lnk,
     arrayFirstIndex((k, s) -> k = 'k' AND s = 'instrumentation', attr_key, attr_scope) AS i_ins,
     if(i_span != 0, i_span,
        if(i_res != 0, i_res,
           if(i_evt != 0, i_evt,
              if(i_lnk != 0, i_lnk, i_ins)))) AS i0
SELECT (i0 != 0) AND ifNull(attr_num[i0] = 400, 0) AS probe0 …
```

**The five shapes it has to get right, and what it returned on each.** One row per
shape, run on 26.3.29.7:

| the span's stored `k` | element the chain picks | the value it resolves to | `arrayFirstIndex(k -> k = 'k')` alone would pick | `= 400` | `= 600` |
|---|---|---|---|---|---|
| no `k` at all | **0** | absent | 0 | 0 | 0 |
| `span.k = 400` | 1 | 400, `int` | 1 | 1 | 0 |
| `span.k = 400` then `span.k = 600` | 1 | 400, `int` | 1 | 1 | 0 |
| **`resource.k = 600` then `span.k = 400`** | **2** | **400, `int`** | **1 — which is `600`, the wrong answer** | 1 | 0 |
| `span.k = 'bad'` (a string) then `span.k = 400` | 1 | `bad`, `string`, numeric NULL | 1 | **0** | 0 |

Row four is why the rule cannot be stated as "first in stored order": plain array order
picks the `resource` entry, and the rule picks the `span` one that comes after it. Row
five is the other end of the same rule — the located element is not numeric, so a
numeric test is **false** rather than skipping to the next element.

**Two of the five scopes are not single-valued, and the rule carves them out.** `span`,
`resource` and `instrumentation` are maps: a span has one `k` at each, and a repeat is a
sender bug the locate resolves deterministically. `event` and `link` are not — a span
carries one `exception.type` per EVENT and one `spanID` per LINK — so "the element the
span's `k` resolves to" is not a thing that exists there. Applying locate-then-test to
them would make `{ event:name = "evZ" }` stop matching a span whose events are
`evX, evY, evZ`, which is what the shipped live assertion
`literal-event-name-unchanged` requires it to match. **At those two scopes the condition
locates the first MATCHING element instead** — `arrayFirstIndex((k, s, v) -> k = … AND
s = 'event' AND <test on v>, attr_key, attr_scope, attr_val)` — so the span matches when
ANY element does, and the reader's single inversion turns that into the all-match rule
the owner's 2026-08-05 ruling already settled for the field-vs-field form. The same
alias then serves the fused value, so a multi-valued condition projects the element that
satisfied it.

That makes the unscoped chain's five arms two kinds rather than one: at a single-valued
scope the arm is `present ? <test on the located element> : next`, and at `event` or
`link` it is `present ? <first matching element found> : next`. The chain's ORDER is
unchanged — span, resource, event, link, instrumentation, first scope PRESENT — and so
is the value it fuses. Issue #557 ships exactly this, and its criterion 8 freezes the
event and link answers in both directions as live assertions.

**It costs nothing on this corpus.** The two forms read the same columns over the same
granules; five repetitions of each, zero counter spread, instrument as the table below:

    arrayExists form,   PREWHERE   449,736 rows / 30,144,320 bytes / 56 marks
    locate-then-test,   PREWHERE   449,736 rows / 30,144,320 bytes / 56 marks
    locate-then-test,   WHERE      449,736 rows / 32,529,648 bytes / 56 marks

**C1 cannot tell the two apart**, which is why the fixture above exists:
`countIf(arrayCount(k -> k = 'http.status_code', attr_key) > 1)` over `spans_new`
returns **0** — no span in C1 repeats a key — and both forms return the same
1,000,000 matching spans.

**The predicate string is still the one the planner rendered, and it is still
positive.** `crates/pulsus-read/src/traces/search_plan.rs:661` carries
`probe_predicates: Vec<String>`, documented as "Each probe's pre-escaped **positive**
predicate", built by `membership_predicate`
(`crates/pulsus-read/src/traces/search_plan.rs:1077`) against the column names `key`,
`scope`, `val`, `val_num`. What changes is where the string is spent: the
`key`/`scope` conjuncts become the locate, and the value conjunct becomes the test on
the located element. Splitting it that way is what the planner must render — the
predicate is no longer reusable as one opaque string, and that is a cost this change
carries.

**What the reference does here, and why we do not copy it.** Its value path returns the
first match (`AttributeFor`, quoted below). Its **condition** path is a different
mechanism: `createAttributeIterator`
(`tempodb/encoding/vparquet4/block_traceql.go:2981 @ v3.0.2`) builds per-column
predicates over the attribute rows and joins them, so a pushed-down condition matches a
span if **any** of its attribute entries satisfies it. On a span that repeats a key
those two disagree with each other — the same defect measured above. We resolve the
value once and use it everywhere, which is the rule §6 states and the rule this SQL
now implements.

**Only the arrays the condition names are read**, because a column a statement does
not read costs it nothing. Measured on the corpus below, same statement, the metrics
form of the probe, in the earlier `arrayExists` rendering:

    arrays read: key, scope, val_num              288,001,456 bytes   144/162/147 ms
    arrays read: key, scope, val                  376,157,016 bytes   172/167/169 ms
    arrays read: key, scope, val, val_num         536,157,016 bytes   214/221/239 ms

**The locate-then-test form reads the same set and the same bytes.** Its locate touches
`attr_key` and `attr_scope`, and the test touches whichever value array the predicate
names — for the metrics probe, `attr_num`. Re-measured at `max_threads = 16`, three
repetitions: 2,000,000 rows / **288,001,456 bytes**, the same figure as the first row
above.

**The probe is POSITIVE and stays positive. Negation is not done in SQL.**
`crates/pulsus-read/src/traces/search_eval.rs:1213-1216` evaluates a leaf as
`member != *negated`, so `{ span.k != "x" }` is a positive `k = 'x'` probe whose result
the reader inverts. That stays exactly as it is; only what the positive probe computes
changes, from "some element matches" to "the resolved element matches".

**The six negation cases, and the one the locate rule changes.** `probe0` is the
positive column; the returned answer is `NOT probe0`:

    the span's stored k        it HAS   probe0 today   probe0 new   { k != "x" } today / new
    (no k)                     absent        0             0               1    /   1
    ['x']                      x             1             1               0    /   0
    ['x','y']                  x             1             1               0    /   0
    ['y','x']                  y             1             0               0    /  *1*
    ['y']                      y             0             0               1    /   1
    j='x' only                 absent        0             0               1    /   1

Five of the six are unchanged. The one that moves is `['y','x']`: its value is `y`, so
`k != "x"` is true, and today's answer of 0 is the same inconsistency the filter table
above measures, seen through a negation. **It is a change of answer and it shares the
duplicate-key ledger row.**

Rendering the negation **inside** the array function instead is a third thing and is
still refused. `arrayExists(val != 'x')` asks "does SOME element differ" where the
question is "does the resolved element differ", and it gets the absent-key and
two-value cases backwards. The reader already holds the `negated` flag; nothing about
negation belongs in the SQL.

**Two of the eight search builders disappear; one is retargeted, not deleted.**

| builder | after | why |
|---|---|---|
| `membership_sql` (`crates/pulsus-read/src/traces/search_sql.rs:286`) | **deleted** — becomes `probe0` above | the result is one `UInt8` per span row |
| `attr_values_sql` (`:325`) | **deleted** — becomes two columns per read field | it is SCALAR: one value per (span, key). `arrayFirstIndex(…) AS i0`, then `attr_num[i0]` / `<byte-capped> attr_val[i0]` and `attr_type[i0]` from the SAME element. One capped string per field per row, which is the row shape the hydration read already has |
| `event_set_sql` (`:397`) | **retargeted to `trace_spans` with an `ARRAY JOIN`**, still its own statement | it is MULTI-VALUED, and its own doc comment (`crates/pulsus-read/src/traces/search_sql.rs:366-380`, issue #351) records why a row-per-value shape replaced an aggregate one: "An ARRAY column is an unbounded number of capped strings in ONE row … phase-2 reads carry no `max_memory_usage`". Projecting `arrayFilter(…)` as a column would put that shape back. `ARRAY JOIN` over the span row reproduces the row-per-value read exactly, on the granules the batch already selects |

`root_sql`, `trace_ctx_sql` and `child_count_sql` (`:428, 468, 492`) read `trace_spans`
by `trace_id IN` and are untouched.

**Which value a duplicated key yields, and why the rule is the one below.** A span may
carry the same key twice. Today `attr_values_sql` reads `any(val)` /
`any(val_num)` over a `GROUP BY (trace_id, span_id)`, which picks arbitrarily and is not
stable across merges. The array form picks by position, so a rule has to be chosen, and
the reference already has one:

**What the right answer is, worked out before looking at what anyone else does.**
A duplicated key arrives from a sender that repeated it — OTLP's span attributes are a
repeated `KeyValue` list, and nothing in the protocol prevents two entries with the same
key. Five rules are available, and four can be eliminated without reference to any other
implementation:

| rule | why not |
|---|---|
| **arbitrary** — today's `any()` over a `GROUP BY` | the same query on the same rows can answer differently after a merge. A user cannot predict it and cannot reproduce a screenshot. This is the one property a query engine may not have |
| **refuse the query** | one malformed span would empty a dashboard that is otherwise fine, and the person reading it cannot fix the sender. A bad span must cost its own row, never the query |
| **return every value** | it changes the type of every attribute read from one value to a set, for a case that is a sender bug, and it makes `avg()`, `by()` and `select()` each need a second rule for how to reduce the set |
| **last in stored order** | deterministic, so it is a real candidate; see below |
| **first in stored order**, within the scope the query resolves to | deterministic; see below |

Both surviving rules are deterministic and both are reproducible from what we store,
because the arrays are written in the order the writer walks the sender's list and
`payload` keeps that same order. The tie-break is **which one makes every surface of
this product agree**: the trace-by-id read returns `payload`, and a client rendering it
shows the sender's list in order, first occurrence first. Choosing *first* makes the
search answer, the projected value and the rendered payload name the same value.
Choosing *last* would make a user see 7 in one panel and 5 in another. So the rule is
**the first stored element within the highest-precedence scope that is present**, chosen
on that ground.

**Two halves, and the second is not "first".** A scoped condition — `span.k` — has one
scope to look in, and there the rule is the first stored element carrying the key. An
unscoped one — `.k` — first chooses a scope, by the precedence span → resource → event →
link → instrumentation, and only then takes the first element within it. **Those two
steps can disagree with plain array order**, and the case that shows it is a span that
carries the key at `resource` before it carries it at `span`: measured on the five
shapes below, the first element carrying the key is number 1 at `resource`, and the rule
resolves to number 2 at `span`. Saying only "first in stored order" describes the scoped
half and gets the unscoped half wrong.

**The reference lands in the same place, and here is its source rather than a
paraphrase of it.** Read at the pinned tag, `v3.0.2`, commit
`0c4b926d09234186de39833e9c7ecb5b7614c8b9`:

```go
// tempodb/encoding/vparquet4/block_traceql.go:128-151 @ v3.0.2
func (s *span) AttributeFor(a traceql.Attribute) (traceql.Static, bool) {
	find := func(a traceql.Attribute, attrs []attrVal) *traceql.Static {
		...
		for i := range attrs {
			if attrs[i].a == a {
				return &attrs[i].s
			}
		}
		return nil
	}
```

```go
// tempodb/encoding/vparquet4/block_traceql.go:249-280 @ v3.0.2
	// name search in span, resource, link, and event to give precedence to span
	// we don't need to do a name search at the trace level b/c it is intrinsics only
	if len(s.spanAttrs) > 0 {
		if attr := findName(a.Name, s.spanAttrs); attr != nil {
			return *attr, true
		}
	}

	if len(s.resourceAttrs) > 0 { ... findName(a.Name, s.resourceAttrs) ... }
	if len(s.eventAttrs) > 0 { ... findName(a.Name, s.eventAttrs) ... }
	if len(s.linkAttrs) > 0 { ... findName(a.Name, s.linkAttrs) ... }
	if len(s.instrumentationAttrs) > 0 { ... findName(a.Name, s.instrumentationAttrs) ... }

	return traceql.StaticNil, false
```

The first block is the scoped rule: a linear scan over the scope's slice that returns
at the first element whose attribute matches. The second is the unscoped one, in the
order the code runs it — span, resource, event, link, instrumentation — and its own
comment says the order exists to give span precedence. So the two agree with the rule
derived above, and the derivation did not need them.

So a stored span in sender order `[7, 5]` answers **7**, not 5. Today's `any()` returned
5 in one measurement; that is not a contract, it is whichever row the aggregate reached
first. **This is a change of answer, and it is recorded in the tree rather than only
here:**

| where | what it says |
|---|---|
| `docs/benchmarks/traces-differential-ledger.md`, `traceql-attribute-resolves-to-one-element` | the rule, why it was chosen before the reference was consulted, the reference's two disagreeing paths, the three fixture rows that move, and which differential cases are affected |
| `docs/api.md` §4.2 | what the route does today and what it will do, in the paragraph next to the projection rules, marked as a decision rather than as shipped behaviour |
| `crates/pulsus-read/src/traces/search_eval.rs`, `dual_scope_membership_satisfies_an_unscoped_negation_correctly` | a doc comment on the test that pins the OLD cross-scope answer, naming the ledger entry and the assertion the change will move. The assertion is deliberately left alone: it describes the shipped engine |
| `e2e/src/traces_corpus.rs`, `unscoped_str` | the differential corpus's own expectation helper resolves unscoped keys **resource-first**, the opposite of the precedence above. No case can see it today because the corpus's resource and span key sets are disjoint; a comment now says what must change if a case is added that can |

**No differential case needs an exemption**, and that is a statement about the corpus
rather than a hope: resource scope carries `run_id`, `env` and `region`, span scope
carries `http.status_code`, `cache_hit`, `sample_ratio` and `tier`, no key appears at
both, and no generated span carries one key twice — so no case, including the two
unscoped ones, can separate the old rule from the new.

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
166,667 traces (166,666 of 12 spans and one of 8 — 2,000,000 does not divide
by 12), 8 attributes per span, three hours from
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
| today, hydration | 449,736 | 15,809,612 | 56 | 25/17/17/16/15 |
| today, membership | 2,015,232 | 48,571,546 | 246 | 16/12/12/12/14 |
| **today, both** | | **64,381,158** | | |
| new, one statement, `WHERE` | 449,736 | 32,529,648 | 56 | 38/20/21/41/24 |
| **new, one statement, `PREWHERE`** | 449,736 | **30,144,320** | 56 | 20/20/20/20/20 |

**The membership row's byte count moved between two versions of this document, and
the reason is not the one first given.** An earlier version said the index table had
been rebuilt. It had not: the build is deterministic and reproduces to the byte
(16,000,000 rows, 343,412,406 bytes on disk, §2.1). **The whole difference is
`with_value`** — whether the statement projects the matched value. Measured on the
same table, the same 32 ids, three repetitions each, zero spread:

    SELECT DISTINCT trace_id, span_id                                   48,523,078 bytes
    SELECT DISTINCT trace_id, span_id, <byte-capped val> AS v,
                    val_type AS t                                       48,571,546 bytes
                                                    both 2,015,232 rows / 246 marks

**Which of the two production sends is not a choice made at the call site**, and that
is why the wrong explanation survived a round: the builder takes `with_value` as an
argument (`crates/pulsus-read/src/traces/search_sql.rs:286`), but the caller passes
`self.probe_values[probe_idx]`
(`crates/pulsus-read/src/traces/search_plan.rs:888-896`), and that vector is filled at
plan time by `projection_value`
(`crates/pulsus-read/src/traces/search_plan.rs:2308-2352`), which sets it **true** for
exactly four predicate classes — `Regex`, `Num`, `KeyExists`, `NumExpr` — because those
are the ones whose matched value the response needs and cannot take from the query's own
literal. Q1's probe is `val_num >= 500`, a `Num`, so production sends the **with-value**
form and **48,571,546 is the figure for the statement printed above**. The batch total
is 15,809,612 + 48,571,546 = 64,381,158.

The five `ms` readings are this machine's and are given as a spread, not as a ratio;
nothing below rests on them.

**0.47×, not 2.2×.** The 2.2× reading is the warm one. Same corpus, same statements,
instrument as above but `use_query_condition_cache = 1`, after
`SYSTEM DROP QUERY CONDITION CACHE`, three consecutive runs each:

| statement | run 1 | runs 2 and 3 |
|---|---|---|
| today, hydration | 449,736 / 15,809,996 / 56, hits 0 misses 4 | 216,780 / 10,218,940 / 27, hits 4 misses 0 |
| today, membership | 2,015,232 / 48,571,546 / 246, hits 0 misses 4 | 24,576 / 893,618 / 3, hits 4 misses 0 |
| new, `PREWHERE` | 449,736 / 30,145,517 / 56, hits 0 misses 4 | 216,780 / 26,417,997 / 27, hits 4 misses 0 |

`11,112,558` against `26,417,997` — **2.38×**. A fresh 32-id list goes straight back to
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
    key='http.status_code' AND val_num >= 500          2,015,232  48,571,546    246
    key='http.method' AND val='GET'                      516,096   9,382,050     63
    key='request.id' AND val='r-1234567'                  16,384     294,928      2

    whole batch, today (hydration 15,809,612 + membership) vs new (PREWHERE 30,144,320)
    numeric range                64,381,158  ->  30,144,320   0.47x
    string eq, 4 distinct vals   25,191,662  ->  30,144,320   1.20x
    string eq, unique per span   16,104,540  ->  30,144,320   1.87x

### Q2 — an attribute-only search: `{ span.http.status_code >= 500 }`

```sql
-- today   crates/pulsus-read/tests/golden/traces_search/val_num_range.sql:5-12, byte for byte
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
-- today   crates/pulsus-read/tests/golden/traces_search/status_only.sql:5-11, byte for byte
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
-- Q4a values for one key, today   (crates/pulsus-read/src/traces/tags_sql.rs:118-127) - no time bound at all
SELECT DISTINCT val, val_type FROM trace_tag_catalog
WHERE key = 'http.status_code' AND scope = 'span'
ORDER BY val, val_type LIMIT 1001

-- Q4a values for one key, new     - one added line
SELECT DISTINCT val, val_type FROM trace_tag_catalog
WHERE key = 'http.status_code' AND scope = 'span'
  AND date >= toDate('2023-11-14') AND date <= toDate('2023-11-15')
ORDER BY val, val_type LIMIT 1001

-- Q4c values narrowed by a service, today (crates/pulsus-read/src/traces/tags_sql.rs:282-312): the semi-join
--    of §2.4 - measured 2,129,920 rows / 260 marks on C1 with service_time present;
--    4,015,232 / 494 without it.  §2.4 carries both and the instrument
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
-- identical in both designs   crates/pulsus-read/src/traces/sql.rs:16-26, byte-frozen against docs/schemas.md §4.2
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
-- today   crates/pulsus-read/tests/golden/traces_metrics/rate_by_service.sql:5-11, byte for byte
SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns - 1),
       INTERVAL 60000000000 NANOSECOND)) + 60000 AS t, service AS g0,
       uniqExact(trace_id, span_id) AS n
FROM trace_spans
WHERE timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
  AND duration_ns > 1000000000
GROUP BY t, g0 ORDER BY t ASC, g0

-- new: THE SAME STATEMENT. This shape is not pre-aggregated, and
-- section 3.5 gives the price: sum(count) over a rollup and
-- uniqExact(trace_id, span_id) over the spans differ the moment one
-- span row is written twice.
```

**Unchanged, deliberately.** This is the one shape of the nine that keeps its
full-window scan. §3.5 prices the exact rollup that would change that — built on
C1, same answer, duplicate-safe, 16.30 B/span — and says why this design does not
take it.

A metrics query with an attribute filter is a semi-join today
(`crates/pulsus-read/tests/golden/traces_metrics/attr_semi_join.sql`); the attribute test becomes inline:

```sql
-- today
… AND (trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx
      WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15')
        AND timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
        AND key = 'http.status_code' AND val_num >= 500 AND scope = 'span')
-- new: the same locate-then-test column as the search batch, so the metrics
--      filter and the search filter answer a duplicated key the same way
WITH arrayFirstIndex((key, scope) -> key = 'http.status_code' AND scope = 'span',
                     attr_key, attr_scope) AS i0
… AND ((i0 != 0) AND ifNull(attr_num[i0] >= 500, 0))
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

So at this corpus size the inline form reads **2.37×** the bytes and finishes in
**2.7–3.0× less wall time**. **Wall time is not CPU, and here the two disagree**: the
same pair re-measured with `ProfileEvents['OSCPUVirtualTimeMicroseconds']` at
`max_threads = 16`, three repetitions, gives the semi-join 943–998 ms of CPU and the
inline form **1191–1324 ms** — the inline form spends about **1.25× more** CPU and
returns sooner because it spreads that work over the threads while the semi-join's
sub-select does not. §5 row 5 carries both readings. An earlier version of this
paragraph reported the wall-time ratio under the heading "ClickHouse CPU".

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
wrapper.** Every query counter in this document was taken by running the statement
itself, never by reading one out of a wrapper around it.

### Q7 — the service graph

Reads `trace_edges`, which this design does not touch. The statement is
byte-identical (`crates/pulsus-read/tests/golden/traces_graph/single_node.sql`). **The earlier counters,
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
| 1 | storage, B/span, **the worked model at `A` = 20** — Appendix A's parameters, not a measurement. It prices the identity columns at their full width; the measured row below and the sensitivity in §7 say what happens when they compress | 1047.9 | **625.7** | **−40.3% at those parameters; −35.5% at C1's measured identity cost** | [D] |
| 1 | storage, B/span, **measured** at `A` = 8 and `Z_p` = 15.91 on corpus C1, ClickHouse 26.3.29.7, `sum(bytes_on_disk)` over `system.parts` after `OPTIMIZE … FINAL`. **The build is published below this table**, and `trace_edges` is excluded from both sides because it is byte-identical in both (35,073,475 against 35,073,272 — the same rows through the same statement) | 306.5 | **212.8** | **−30.6%** | [M] |
| 1 | … the payload component of each, so `Z_p` can be substituted: today 180,471,375 B (base 51,292,209 + a second copy of 129,179,166 in `service_time`), new 51,292,209 B (its two projections carry none). Payload-free: today 216.4 B/span, new 187.2 B/span | | | | [M] |
| 1 | storage at 10⁹ spans/day, 7 days | 7.34 TB | **4.38 TB** | −2.96 TB | [D] |
| 2 | rows read, `{}` | 4.17·10⁷ | 3.48·10⁶ | **÷12** | [D] |
| 2 | rows read, `{status = error}` | 4.17·10⁷ | 4.17·10⁵ | **÷100** | [D] |
| 2 | rows read, `{name = "…"}` | 4.17·10⁷ | 8.33·10⁵ | **÷50** | [D] |
| 2 | rows read, `\| rate() by(service)` | 4.17·10⁷ | 4.17·10⁷ | **unchanged** — §3.5 | [D] |
| 2 | rows read, attribute search | 2.08·10⁷ | 8.33·10⁶ | **÷2.5** | [D] |
| 2 | rows read, the tag dropdown | 10⁶ and rising with deployment age | 10⁴ | **÷100, and bounded** | [D] |
| 2 | rows read, the narrowed dropdown (26.3.29.7; `use_query_condition_cache=0`, `optimize_move_to_prewhere=1`, `max_block_size=65409`, `max_threads=auto(16)`; 3 reps, zero spread; corpus C1, span table carrying `service_time`) | 2,129,920 rows / 72,453,720 B / 260 marks | 16,384 / 51,826 / 2 | **÷130 on rows, ÷1398 on bytes**, service-narrowed only. Both return `DELETE POST PUT`. §2.4 re-takes the today side on the published index build — same rows and marks, 70,298,975 B — and gives the reading without the projection as well | [M] |
| 2 | rows read, trace-by-id and the service graph (26.3.29.7; `use_query_condition_cache=0`, `optimize_move_to_prewhere=1`, `max_block_size=65409`, `max_threads=auto(16)`; 3 reps, zero spread; corpus C1) | Q5 8,192 / 136,124 / 1; Q7 1,216,384 / 51,624,512 / 149 | Q5 identical; Q7 untouched table | **identical** | [M] |
| 3 | bytes, storage → reader, one search batch, **first-seen** (26.3.29.7; `use_query_condition_cache=0`, `optimize_move_to_prewhere=1`, `max_block_size=65409`, `max_threads=auto(16)`; **five repetitions**, zero counter spread; corpus C1 with the index table of §2.1) | 64,381,158 | **30,144,320** | **−53%** | [M] |
| 3 | … the same batch, **repeated identically** (26.3.29.7; `use_query_condition_cache=1`, `optimize_move_to_prewhere=1`, `max_block_size=65409`, `max_threads=auto(16)`; corpus C1; runs 2 and 3 after `SYSTEM DROP QUERY CONDITION CACHE`) | 11,064,138 | 26,417,997 | +139% | [M] |
| 3 | bytes, writer → ClickHouse | 1838 raw B/span, 2 statements | 1038, 1 | **−43.5%** | [D] |
| 3 | rows crossing to every replica (the catalog is `Replication::Global`, `crates/pulsus-schema/src/catalog.rs:406`) | 20 per span | ≤ distinct tuples per block | **≈500×** | [D] |
| 3 | bytes, reader → client | — | — | **unchanged** — set by the API response shape, not by storage | [D] |
| 4 | SQL statements, one-condition search, `M`=20 | 4 | **3** | −25% | [D] |
| 4 | … a search that compares an `event:`/`link:` intrinsic against another field | | **keeps its extra per-batch statement** — §4 Q1 says why the multi-valued read cannot become a column | [D] |
| 4 | … at the candidate ceiling | 6252 | **3127** | −50% | [D] |
| 5 | **ClickHouse CPU, measured** — `ProfileEvents['OSCPUVirtualTimeMicroseconds']` from `system.query_log`, not inferred from bytes (26.3.29.7; `use_query_condition_cache=0`, `optimize_move_to_prewhere=1`, `max_block_size=65409`, `max_threads=16`; 3 reps; corpus C1) | attribute search 62–81 ms; search batch 68–78 ms (hydration 21–25 + membership 46–53); attribute metrics query 943–998 ms | attribute search 46–63 ms; search batch 25–27 ms; attribute metrics query **1191–1324 ms** | **it does not track bytes, and one of the three goes the other way**: the batch falls ≈2.7×, the attribute search ≈1.5×, and the metrics query's CPU **rises ≈1.25×** while its wall time falls ≈3× (400–445 ms → 134–140 ms) because the inline form parallelises where the semi-join does not. An earlier version of this row asserted that CPU "tracks the uncompressed bytes" and reported the metrics figure as a CPU ratio when it was a wall-time one | [M] |
| 6 | our own CPU | 66 statements, 28,384 rows decoded at `M`=1000 | 34 statements, 25,312 rows | **strictly fewer statements and strictly fewer decoded rows.** That is what was counted. Our process's CPU was **not** measured — no reader was run against either schema — and an earlier version of this row called the counts "strictly lower" CPU. §7 carries it | [D] |
| 7 | disk read work | tracks the compressed bytes of the selected columns | | as row 2 and row 3 | [D] |
| 8 | merge, LZ4-equivalent B/span/level | 9856 | **5341** | **−45.8%** | [D] |
| 8 | write wall time, 20,000,000 spans, four takes. **Instrument, stated in full because it is weaker than every other row here:** the corpus is a 20,000,000-span build, not C1, and it is not published — §5's own 20,000,000-span readings are withdrawn elsewhere in this row set, and this one survives only as a direction; the machine carried a load average between 11 and 29 from other work; ClickHouse 26.3.29.7; no per-statement settings were recorded | 387.6 / 500.4 / 441.3 / 337.4 s | 200.1 / 272.8 / 239.7 / 304.0 s | one statement was faster in **all nine takes** at both corpus sizes, margin 1.02×–2.48×. **No ratio is claimed and none should be read off these numbers**; what nine of nine takes support is the sign | [M] |

The write-time takes were taken on a machine carrying a load average between 11
and 29 from other work. No ratio is claimed; the direction is what all nine
agree on.

**The storage build, published.** Row 1's measured pair is the two table families built
from corpus C1 and compared. Both sides carry the same 2,000,000 spans; the old side's
index is the `attrs_old` of §2.1; every table is `OPTIMIZE … FINAL` before it is
measured. **The script below is complete and literal** — it names every column, it has
no placeholder, and it is what produced the numbers that follow. It takes the same
endpoint argument as §4's corpus script and it runs after it.

```bash
#!/bin/bash
set -eu
CH="${1:?usage: $0 <clickhouse-http-endpoint>}"
q(){ curl -sS --data-binary @- "$CH/?database=c1&max_execution_time=7200&max_insert_threads=1&max_threads=4&max_block_size=65409"; }
for T in o_spans o_catalog o_edges n_spans n_attr_traces n_error n_recent n_catalog n_edges; do
  q <<< "DROP TABLE IF EXISTS c1.$T" >/dev/null
done

SPAN_COLS="trace_id, span_id, parent_id, name, service, timestamp_ns, duration_ns,
  status_code, kind, payload_type, shared, status_message, scope_name, scope_version, payload"
SPAN_DDL="trace_id FixedString(16), span_id FixedString(8), parent_id FixedString(8),
  name LowCardinality(String), service LowCardinality(String),
  timestamp_ns Int64 CODEC(DoubleDelta, ZSTD(1)), duration_ns Int64 CODEC(T64, ZSTD(1)),
  status_code Int8, kind Int8, payload_type Int8, shared UInt8 DEFAULT 0,
  status_message String DEFAULT '', scope_name LowCardinality(String) DEFAULT '',
  scope_version LowCardinality(String) DEFAULT '', payload String CODEC(ZSTD(3))"
PROJ14="duration_ns, kind, name, parent_id, payload_type, scope_name, scope_version,
  service, shared, span_id, status_code, status_message, timestamp_ns, trace_id"
DAY_PROJ="PROJECTION span_name_day (SELECT toDate(fromUnixTimestamp64Nano(timestamp_ns)) AS d,
    name, count() GROUP BY d, name)"
EDGE_DDL="date Date, side UInt8, trace_id FixedString(16), span_id FixedString(8),
  pair_id FixedString(8), conn_type LowCardinality(String),
  timestamp_ns Int64 CODEC(DoubleDelta, ZSTD(1)), service LowCardinality(String),
  duration_ns Int64 CODEC(T64, ZSTD(1)), failed UInt8"
EDGE_SEL="SELECT toDate(fromUnixTimestamp64Nano(timestamp_ns)) AS date,
  toUInt8(kind IN (2, 5)) AS side, trace_id, span_id,
  if(kind IN (3, 4) OR shared = 1, span_id, parent_id) AS pair_id,
  if(kind IN (2, 3), 'rpc', 'messaging') AS conn_type, timestamp_ns, service, duration_ns,
  toUInt8(status_code = 2) AS failed
  FROM c1.spans_new
  WHERE kind IN (3, 4)
     OR (kind IN (2, 5) AND (shared = 1 OR parent_id != toFixedString(unhex('0000000000000000'), 8)))"

# ---- the family that ships today ------------------------------------------
q <<EOF >/dev/null
CREATE TABLE c1.o_spans ($SPAN_DDL,
  INDEX idx_duration duration_ns TYPE minmax GRANULARITY 4,
  PROJECTION service_time (SELECT * ORDER BY (service, timestamp_ns)),
  $DAY_PROJ
) ENGINE = MergeTree
PARTITION BY toDate(fromUnixTimestamp64Nano(timestamp_ns))
ORDER BY (trace_id, timestamp_ns) SETTINGS ttl_only_drop_parts = 1
EOF
q <<EOF >/dev/null
INSERT INTO c1.o_spans ($SPAN_COLS) SELECT $SPAN_COLS FROM c1.spans_old
EOF
q <<'EOF' >/dev/null
CREATE TABLE c1.o_catalog (
  scope LowCardinality(String), key LowCardinality(String), val String,
  val_type LowCardinality(String)
) ENGINE = ReplacingMergeTree ORDER BY (scope, key, val, val_type)
EOF
q <<'EOF' >/dev/null
INSERT INTO c1.o_catalog (scope, key, val, val_type) SELECT scope, key, val, val_type FROM c1.attrs_old
EOF
q <<EOF >/dev/null
CREATE TABLE c1.o_edges ($EDGE_DDL) ENGINE = ReplacingMergeTree
PARTITION BY date ORDER BY (side, trace_id, span_id) SETTINGS ttl_only_drop_parts = 1
EOF
q <<EOF >/dev/null
INSERT INTO c1.o_edges $EDGE_SEL
EOF

# ---- the family this document proposes ------------------------------------
q <<EOF >/dev/null
CREATE TABLE c1.n_spans ($SPAN_DDL,
  attr_key Array(LowCardinality(String)), attr_scope Array(LowCardinality(String)),
  attr_val Array(String), attr_type Array(LowCardinality(String)),
  attr_num Array(Nullable(Float64)),
  CONSTRAINT attr_arrays_aligned CHECK length(attr_key) = length(attr_scope)
    AND length(attr_key) = length(attr_val) AND length(attr_key) = length(attr_type)
    AND length(attr_key) = length(attr_num),
  INDEX idx_duration duration_ns TYPE minmax GRANULARITY 4,
  PROJECTION service_time (SELECT $PROJ14 ORDER BY (service, timestamp_ns)),
  PROJECTION name_time    (SELECT $PROJ14 ORDER BY (name, timestamp_ns)),
  $DAY_PROJ
) ENGINE = MergeTree
PARTITION BY toDate(fromUnixTimestamp64Nano(timestamp_ns))
ORDER BY (trace_id, timestamp_ns) SETTINGS ttl_only_drop_parts = 1
EOF
q <<EOF >/dev/null
INSERT INTO c1.n_spans ($SPAN_COLS, attr_key, attr_scope, attr_val, attr_type, attr_num)
SELECT $SPAN_COLS, attr_key, attr_scope, attr_val, attr_type, attr_num FROM c1.spans_new
EOF
q <<'EOF' >/dev/null
CREATE TABLE c1.n_attr_traces (
  date Date, key LowCardinality(String), val String, scope LowCardinality(String),
  bucket UInt32, trace_id FixedString(16), val_type LowCardinality(String),
  val_num Nullable(Float64),
  ts_max SimpleAggregateFunction(max, Int64), dur_max SimpleAggregateFunction(max, Int64),
  dur_min SimpleAggregateFunction(min, Int64)
) ENGINE = AggregatingMergeTree PARTITION BY date
ORDER BY (key, val, scope, bucket, trace_id, val_type) SETTINGS ttl_only_drop_parts = 1
EOF
q <<'EOF' >/dev/null
INSERT INTO c1.n_attr_traces
  (date, key, val, scope, val_type, val_num, bucket, trace_id, ts_max, dur_max, dur_min)
SELECT toDate(fromUnixTimestamp64Nano(timestamp_ns)) AS date,
       key, val, scope, val_type, val_num,
       toUInt32(intDiv(timestamp_ns, 300000000000)) AS bucket, trace_id,
       max(timestamp_ns), max(duration_ns), min(duration_ns)
FROM c1.spans_new
ARRAY JOIN attr_key AS key, attr_scope AS scope, attr_val AS val,
           attr_type AS val_type, attr_num AS val_num
GROUP BY date, key, val, scope, val_type, val_num, bucket, trace_id
EOF
q <<'EOF' >/dev/null
CREATE TABLE c1.n_error (
  date Date, trace_id FixedString(16), span_id FixedString(8),
  timestamp_ns Int64 CODEC(DoubleDelta, ZSTD(1)), duration_ns Int64 CODEC(T64, ZSTD(1)),
  service LowCardinality(String), name LowCardinality(String), kind Int8
) ENGINE = ReplacingMergeTree PARTITION BY date
ORDER BY (timestamp_ns, trace_id, span_id) SETTINGS ttl_only_drop_parts = 1
EOF
q <<'EOF' >/dev/null
INSERT INTO c1.n_error (date, trace_id, span_id, timestamp_ns, duration_ns, service, name, kind)
SELECT toDate(fromUnixTimestamp64Nano(timestamp_ns)), trace_id, span_id, timestamp_ns,
       duration_ns, service, name, kind
FROM c1.spans_new WHERE status_code = 2
EOF
q <<'EOF' >/dev/null
CREATE TABLE c1.n_recent (
  date Date, bucket UInt32, trace_id FixedString(16),
  ts_max SimpleAggregateFunction(max, Int64)
) ENGINE = AggregatingMergeTree PARTITION BY date ORDER BY (bucket, trace_id)
SETTINGS ttl_only_drop_parts = 1
EOF
q <<'EOF' >/dev/null
INSERT INTO c1.n_recent (date, bucket, trace_id, ts_max)
SELECT toDate(fromUnixTimestamp64Nano(timestamp_ns)) AS date,
       toUInt32(intDiv(timestamp_ns, 300000000000)) AS bucket, trace_id, max(timestamp_ns)
FROM c1.spans_new GROUP BY date, bucket, trace_id
EOF
q <<'EOF' >/dev/null
CREATE TABLE c1.n_catalog (
  date Date, scope LowCardinality(String), key LowCardinality(String),
  service LowCardinality(String), val String, val_type LowCardinality(String)
) ENGINE = ReplacingMergeTree PARTITION BY date
ORDER BY (scope, key, service, val, val_type) SETTINGS ttl_only_drop_parts = 1
EOF
q <<'EOF' >/dev/null
INSERT INTO c1.n_catalog (date, scope, key, service, val, val_type)
SELECT toDate(fromUnixTimestamp64Nano(timestamp_ns)) AS date, scope, key, service, val, val_type
FROM c1.spans_new
ARRAY JOIN attr_key AS key, attr_scope AS scope, attr_val AS val, attr_type AS val_type
GROUP BY date, scope, key, service, val, val_type
EOF
q <<EOF >/dev/null
CREATE TABLE c1.n_edges ($EDGE_DDL) ENGINE = ReplacingMergeTree
PARTITION BY date ORDER BY (side, trace_id, span_id) SETTINGS ttl_only_drop_parts = 1
EOF
q <<EOF >/dev/null
INSERT INTO c1.n_edges $EDGE_SEL
EOF
for T in o_spans o_catalog o_edges n_spans n_attr_traces n_error n_recent n_catalog n_edges; do
  q <<< "OPTIMIZE TABLE c1.$T FINAL" >/dev/null
done
q <<'EOF'
SELECT table, sum(rows) AS rows, sum(bytes_on_disk) AS bytes,
       round(sum(bytes_on_disk)/2000000.0, 2) AS b_per_span
FROM system.parts
WHERE database = 'c1' AND active AND table IN
  ('o_spans','attrs_old','o_catalog','o_edges','n_spans','n_attr_traces','n_catalog','n_error','n_recent','n_edges')
GROUP BY table ORDER BY table FORMAT TSV
EOF
q <<'EOF'
SELECT 'old, trace_edges excluded' AS side, sum(bytes_on_disk) AS bytes,
       round(sum(bytes_on_disk)/2000000.0, 1) AS b_per_span
FROM system.parts WHERE database='c1' AND active AND table IN ('o_spans','attrs_old','o_catalog')
UNION ALL
SELECT 'new, trace_edges excluded', sum(bytes_on_disk), round(sum(bytes_on_disk)/2000000.0, 1)
FROM system.parts WHERE database='c1' AND active
  AND table IN ('n_spans','n_attr_traces','n_catalog','n_error','n_recent')
FORMAT TSV
EOF
```

**One thing to get right when rebuilding it by hand, measured because it bit this
build.** The views of §3.1 select their columns in a different order from the target
tables' column order, and that is safe **only because a `TO` view matches by name** —
verified on 26.3.29.7 with a two-column target and a view whose `SELECT` lists them
reversed: the values land under the right names. An `INSERT … SELECT` matches by
**position** instead unless it names its columns, so the same bodies used as backfills
fail with `Code: 48 … while converting source column val_num to destination column
trace_id`. Every `INSERT` above names its columns for that reason.

**What it gave**, run from the block above extracted out of this document, against a
database holding only corpus C1 and the `attrs_old` of §2.1:

```text
table             rows        bytes on disk   B/span
attrs_old         16,000,000    343,412,406    171.71
n_attr_traces      6,505,828    160,928,751     80.46
n_catalog          2,667,639     11,860,643      5.93
n_edges            1,200,000     35,073,241     17.54
n_error               20,000        501,117      0.25
n_recent             167,277      3,707,783      1.85
n_spans            2,000,000    248,538,636    124.27
o_catalog          2,166,747      8,737,786      4.37
o_edges            1,200,000     35,073,281     17.54
o_spans            2,000,000    260,840,197    130.42
```

```text
old, trace_edges excluded    612,990,389      306.5 B/span
new, trace_edges excluded    425,536,930      212.8 B/span

425,536,930 / 612,990,389 = 0.6942        -30.6%
trace_edges is excluded from both sides because it is the same table fed by the
same statement on both: 35,073,281 against 35,073,241, a 40-byte difference in
how the two runs' blocks happened to fall.
```

**The same script run six times does not give the same bytes, and the printed B/span
can cross a rounding boundary.** Six complete rebuilds from the block above — four here
and two taken independently by a reviewer on their own server and corpus build — same
script, nothing else changed:

    run   old side, bytes   B/span    new side, bytes   B/span    ratio     where
    1       612,990,389      306.5      425,536,930      212.8     0.6942    here
    2       613,004,438      306.5      425,450,382    **212.7**   0.6940    here
    3       613,023,509      306.5      425,543,663      212.8     0.6942    here
    4       613,014,196      306.5      425,541,572      212.8     0.6942    here
    5       613,005,595      306.5      425,428,616    **212.7**   0.6940    independent
    6       613,011,384      306.5      425,528,267      212.8     0.6942    independent

The old side spans 33,120 bytes across the six (0.005%) and the new side 115,047
(0.027%) — merge and block boundaries land differently each time, and the lowest new-side
total of the six came from the independent runs, which is why this table is not a bound.
**Every run gives −30.6% and every run gives 306.5 on the old side; the new side prints
212.7 on two runs and 212.8 on four**, because 212.75 sits inside that spread. Row counts
were identical on every table on every run. So the ratio is the figure to carry, and the
one-decimal B/span is the figure that can move by a digit. **No bound is claimed**: six
runs say what the variation looked like, not what it cannot exceed.

−30.6%. The row that predicts it is the worked model's −40.3%; the gap between the two
is the identity-column compressibility §7 carries, and this corpus's `A` = 8 against the
model's 20.

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
| `A_t`, distinct attribute values per trace | the index swap alone stops being smaller at `A_t` = **246.5**, computed at `A` = 20 and `S` = 12. `A_t` cannot exceed `A·S`, which is 240 **at those parameters** | **at `A` = 20 the new layout is smaller at every `A_t`**: the degenerate case, where no attribute value repeats anywhere in a trace, is 1027.9 B/span against 1047.9. That is a statement about `A`, not a universal one. `A·S` < the crossover reduces to **`A` < 27.5**, independent of `S`, and Appendix A allows `A` up to 60 — at `A` = 60 a trace whose values never repeat costs **86.2 B/span more** in the new layout. An earlier version of this row read the 240 as a universal ceiling |
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
| the PROBE's result, as the reader sees it | a row present or absent in the membership set | `(i0 != 0) AND ifNull(<test on the located element>, 0)`, printed `UInt8` | **the row set moves on one class and only on it**: a span that carries the probed key more than once, or carries it at two scopes under an unscoped condition. There the membership set answers "some entry matched" and the column answers "the entry this span resolves to matched". Measured on the eight-span fixture in §4 Q1: three of eight differ. Everywhere else they agree, and `ifNull(…, 0)` keeps a NULL element reading as 0 exactly where the membership form returned no row | **differs, deliberately — the duplicate-key ledger row covers it** |
| the probe under NEGATION | positive probe, reader inverts (`crates/pulsus-read/src/traces/search_eval.rs:1213-1216`, `member != *negated`) | **must stay exactly that** | negating inside the array function differs on an absent key and on a multi-valued key — §4 Q1's six-case table. The inversion itself is unchanged; the positive column it inverts is the locate-then-test one, so `['y','x']` under `!= "x"` moves from 0 to 1 | **agree on five of six; the sixth is the duplicate-key change** |
| the value a DUPLICATED key yields | `any(val)` / `any(val_num)` over `GROUP BY (trace_id, span_id)` — arbitrary, not stable across merges | **locate on `(key, scope)` only, then read that element**; scope precedence span → resource → event → link → instrumentation | on `['7','5']`: today returned 5 in one measurement, the new form returns 7. On `['bad','5']`: today's numeric read returns 5, the new form returns NULL, because the FIRST match is not numeric | **CHANGED, deliberately.** The rule is **the first stored element within the highest-precedence scope that is present** — first-in-stored-order is the scoped half of it only — derived in §4 Q1 from what the alternatives cost a user and then checked against the reference, whose value path does the same: `tempodb/encoding/vparquet4/block_traceql.go:128-151` and `:249-280 @ v3.0.2`, quoted there. Its condition path does not, which is the divergence recorded in `docs/api.md` and in `docs/benchmarks/traces-differential-ledger.md`. Today's behaviour has no contract |
| `val_num`'s determinant | — | `(scope, key, val)`, **not `val` alone** | `link:spanID` = `'0000000000000001'` stores `val_num = NULL` while the same text under an attribute key stores `1.0` (`crates/pulsus-write/src/protocols/otlp_traces.rs:581-630` sets `val_num: None` unconditionally for both link intrinsics) | **determined**, and all three columns are in the sorting key |
| `timestamp_ns`, `duration_ns` | `Int64` nanoseconds | `Int64` nanoseconds | none | **agree** |
| the bucket | — | `UInt32` | ingest bounds `timestamp_ns` to `[0, 4.29·10¹⁸]` (`crates/pulsus-write/src/protocols/otlp_traces.rs:481-502`), so the bucket is `≤ 1.43·10⁷` against a ceiling of 4.29·10⁹ | **cannot overflow** |

**Measured, not argued**: on a 2,000,000-span corpus, all 6,000,000 non-NULL
numeric attribute values compared bitwise equal between the two layouts —
`countIf(a.bits = b.bits)` returned 6,000,000 of 6,000,000, reading the bits with
`reinterpretAsUInt64`.

**The 2⁵³ boundary is exactly where it is today.** `val.parse::<f64>()`
(`crates/pulsus-write/src/protocols/otlp_traces.rs:752-754`) already rounds `9007199254740993` to
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
| **a span that carries the probed key twice** | `span.n = "7"` then `span.n = "5"`, filter `{ span.n = 5 }` | the membership row for the second entry exists, so the span **matches** — and `select(span.n)` then renders whichever entry `any()` reached | the span resolves to `7`, so it does **not** match, and `select(span.n)` renders `7` | **changed, deliberately.** Today's two answers contradict each other; the new pair agrees. §4 Q1's fixture moves on three rows, of two kinds — this one, and the next — and the third row is this kind under a negation. One ledger row covers filter, negation and read |
| **a span that carries the probed key at two scopes, under an unscoped condition** | `resource.k = "x"` and `span.k = "y"`, filter `{ .k = "x" }` | matches — the unscoped probe unions the scopes | does not match — `.k` resolves to `"y"` by the precedence span → resource → event → link → instrumentation | **changed, deliberately**, same ledger row. `crates/pulsus-read/src/traces/search_eval.rs:3656` pins today's union behaviour and moves with it |

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

1. `traceql_max_candidates = 100_000` (`crates/pulsus-config/src/model.rs:535`). A query sitting
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
(`crates/pulsus-read/src/traces/compile.rs:451-462`): `count() > n` as `uniqExact(span_id)`, and
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
(`crates/pulsus-read/src/traces/search_plan.rs:3072-3092`, used at `crates/pulsus-read/src/traces/exec.rs:2163-2191`). The effect is more
candidates, not a different result.

`by()` grouping already refuses to push whenever the generator is not
`trace_spans` (`crates/pulsus-read/src/traces/compile.rs:560-562`), so an attribute-generated search behaves
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
writer's pin, `crates/pulsus-clickhouse/src/client.rs:137`), with a view built to throw on one row of a two-row block:

    client                             Code: 395 … while pushing to view mv_throw
    SELECT count() FROM src            2
    system.parts                       20231114_1_1_0   2 rows   active = 1
    SELECT count() FROM tgt            0

**This section records a measured distribution and states no invariant beyond the one
that survived every trial.** Two earlier versions of it stated a rule — first "no view
committed", then "the source is always committed" — and both were refuted by more trials.

Topology: one throwing view and **three** healthy sibling views over the same source
table, one two-row block per trial whose second row makes the view throw, a fresh
database per trial. Stock config with `async_insert = 0`,
`parallel_view_processing = 0`, `materialized_views_ignore_errors = 0`, ClickHouse
26.3.29.7. Every insert returned `HTTP 500`, `Code: 395`.

    src / throwing / b / c / d      20 trials    300 trials
    0 / 0 / 0 / 0 / 0                      0            3
    2 / 0 / 0 / 0 / 0                     14          246
    2 / 0 / 2 / 0 / 0                      1            7
    2 / 0 / 0 / 2 / 0                      2            6
    2 / 0 / 0 / 0 / 2                      1           11
    2 / 0 / 2 / 2 / 0                      0            6
    2 / 0 / 2 / 0 / 2                      0            8
    2 / 0 / 0 / 2 / 2                      0            6
    2 / 0 / 2 / 2 / 2                      2            7
    per-sibling commits b/c/d              -    28/25/32

**One thing held on every one of the 300 trials: the throwing view's own target is
empty.** Nothing else did.

- **Source-present is not an invariant.** 3 of 300 trials left the source table empty as
  well, and those rows were still absent three seconds later. An assertion that the span
  rows survive rejects real server behaviour.
- **The siblings are not independent.** All-three-commit came out 7 times against about
  0.25 expected from the marginals `28/300, 25/300, 32/300`. Whatever couples them is not
  measured here.
- **No trial-count rule is derived from this.** An earlier version computed "9 trials for
  95%" from a three-sibling rate applied to a one-sibling experiment; that inference is
  withdrawn, and no replacement is offered. What the table supports is that a single
  trial commonly shows the all-empty outcome and that more trials show others.

**What the failure leaves is per-target state, and which targets varies.** A span whose
write failed this way may be present or absent; each derived table may or may not hold
its rows; the search shapes that answer for it follow from which targets happen to be
populated. Today's equivalent failure (§2.5 row 1) removes only the attribute index.
`trace_spans` passes `on_flush_poisoned: None` (`crates/pulsus-write/src/writer/trace.rs:172`), the structural
append-only exclusion (`crates/pulsus-write/src/writer/backfill.rs:23-28`), so nothing replays it.

One further reading, same server: with the source at
`non_replicated_deduplication_window = 100`, inserting the identical block twice left the
source at 1 row and moved the view target from 2 rows to 4. A retry after a view failure
does re-run the views even where the source block deduplicates — and it writes view rows
a second time, which is what §3.4's collapse rules absorb.

**The decision has been taken: the behaviour is accepted as it is, and written down
here. No machinery is built for it** — no insert-setting change, no replay path in the
writer, no rebuild-on-demand, no repair pass. The reason given is that it is reached
when a limit is hit or a disk fills, and it has not been reached.

**The accepted contract, in the words a reader needs.**

| | |
|---|---|
| what the caller is told | the insert **fails**: `HTTP 500`, `Code: 395 … while pushing to view`, on every one of the 300 trials |
| what the caller is **not** told | which targets survived |
| what is guaranteed | the throwing view's own target holds nothing for that block — 300 of 300 |
| what is not guaranteed | anything else. The source rows were present in 297 of 300 and absent in 3. The three healthy sibling views committed 28, 25 and 32 times out of 300, and all three together 7 times against about 0.25 if they were independent, so they are not independent and the coupling is not explained |
| what a retry does | it re-runs the views even where the source block deduplicates, so derived rows can be written a second time. §3.4's collapse rules are what absorb that, and they are the reason every derived table in §3.1 is a `ReplacingMergeTree` or an `AggregatingMergeTree` keyed so a repeat is idempotent |
| what a reader sees meanwhile | a span that is fetchable by id but missing from one derived table answers some query shapes and not others. §8.1 splits which |

**So the failure is visible to the writer and invisible to the reader**, and that is the
part to carry forward: a client that receives the `500` and retries gets a consistent
database; a client that receives the `500` and gives up leaves one block's derived rows
partly written, with no record of which.

---

## 7. What we did not measure

Stated here rather than left to be found.

**P1 to P4 have been read since this table was written; §11 carries what each returned.**
What remains unmeasured:

| not measured | why it matters |
|---|---|
| **why the per-target outcomes are distributed the way §6.3 records** | 300 trials give the distribution and explain none of it. The siblings are not independent — all-three came out 7 against about 0.25 from the marginals — and 3 trials left the source table empty too. Neither the coupling nor the empty-source cases is explained. There is no boundary rule to hold or fail for five views; there is a distribution whose mechanism is unknown |
| `d`, the trace-grain collapse factor, on real traces | it scales the whole index saving. §11 P5 |
| the byte cost of the `event_set_sql` read after it moves to an `ARRAY JOIN` over `trace_spans` | it is the one phase-2 read that stays a separate statement. §4 Q1 |
| the scalar value read (`arrayFirstIndex` + element extraction) against today's `attr_values_sql` | the shape is bounded by construction; the byte cost is not measured |
| whether `Array(LowCardinality(String))` and `Array(Nullable(Float64))` insert through our own writer | `metric_hist_samples` proves `Array(Int32)`/`Array(Float64)` from a `Vec` field (`crates/pulsus-schema/src/catalog.rs:492-498`, `crates/pulsus-write/src/writer/rows.rs:437, 443`); the low-cardinality and nullable element types have no precedent in this repository |
| the cost of the drop/add/materialise interval on `service_time` (§8 ids 44–46) on a populated table | during it, a `resource.service.name` search falls back to a base-table scan. Empty on a fresh database |
| the clustered path beyond column presence | §8's twins were measured on a single-node `Distributed('default', …)`: the column appears and the insert lands. Multi-shard routing, `cityHash64(trace_id)` co-sharding of the four new wrappers, and a clustered read were not measured |
| storage at Appendix A's `Z_p` = 4 | §5's measured storage row was taken at `Z_p` = 15.91 and carries its payload component so the figure can be re-derived at another `Z_p`; it was not re-run at 4 |
| concurrency, and ClickHouse's mark, uncompressed and query-condition caches | every figure here is one request on an idle server; a repeated query is cheaper than this says |
| the write path's own CPU | building five array fields instead of `A` separate rows is almost certainly cheaper, and is not counted |
| the eleven compression ratios in Appendix A | they are judgement. Every worked byte figure moves with them; the **signs** of the crossovers in §5.2 survive the whole stated range, the magnitudes do not |
| whether each new table really is safe against a duplicated span row (§3.4) | it is derived from the engine's own collapse rules and from `max`/`min` being unchanged by repeating a value, not observed. §11 P11 is the reading, and it is one insert repeated |
| **a dropdown narrowed by an attribute rather than by a service** (§10) | §10 calls it the one place this design is structurally worse than today and says in the same sentence that it is not priced. It is still not priced: no statement was written for it and nothing was run |
| **the query mix behind "84–97% of rows read come from shapes with no sorted path", 93.5% at the worked point, and "about 5% from the attribute index"** (§2.2) | those three numbers carry the case for the whole change, and the reading that produced them — of what the dashboard datasource generates — is not reproduced in this document and cannot be checked from it |
| **that the identity columns are incompressible** | the model prices `trace_id` at its full 16 bytes per index row. On C1 it costs **5.65** (§2.1), because each id repeats 96 times there. Substituting C1's measured `trace_id` 5.65 and `span_id` 8.03 into `idx_today` and `idx_new` gives today 841.5 B/span and the new design 542.9 — **−35.5% instead of −40.3%**. The sign and the order of magnitude survive; the headline percentage is the thing that moves, and it moves with how often a trace id repeats inside a granule |
| **C1 carries no projections** | the published corpus script creates plain `MergeTree` tables. Any statement that could be served by `service_time` therefore reads differently on C1 than in production: measured, the narrowed dropdown reads 4,015,232 rows without it and 2,129,920 with it (§2.4). Every C1 figure for a statement that filters on `service` is a without-projection figure unless the row says otherwise |
| **the per-column raw-byte rule in Appendix B** | the merge and client-`INSERT` rows are derived from an assumed native-block width per column — fixed types at their width, `String` as one length byte plus `L_v`, `LowCardinality` as a one- or two-byte dictionary index, `Nullable` plus one. Those widths are judgement, not measurement, and the four figures move with them |
| **the corpus behind the write wall-time row** (§5 row 8) | it is a 20,000,000-span build that is not published and not C1, taken on a machine carrying other work. It supports a direction and no ratio, and it is the only row in §5 whose corpus cannot be rebuilt from this document |
| **the corpus behind the earlier catalog row count** | §3.1's "1,094,467 rows either way" came from a corpus that is not C1 and is not published. The claim was re-run on C1 (2,166,747 both ways) and it is the re-run that is stated; the earlier figure is not reproducible |
| **whether the reference behaves the way its source reads** | §4 Q1 now quotes the two functions at the pinned tag rather than paraphrasing them, but nothing here was observed on a running instance of it. A conformance suite is where that belongs, not a design record |
| **the exact rollup at another scale** (§3.5) | it was built and measured on C1: 16.30 B/span, 98.7% of it the distinct-span state, the same answer as the full scan, duplicate-safe. The state holds one entry per distinct span, so it should scale with `N` and not with the grouping key — that last step is an argument, and it was not run at a second corpus size. C1's 54,300 groups are also not Appendix A's `n_grp` = 3·10⁴ per day |
| **our own CPU** (§5 row 6) | the row counts statements and decoded rows, both strictly lower. **No reader was run against either schema**, so the CPU claim those counts are a proxy for is unmeasured on both sides. The database's own CPU *is* measured, in row 5, and it does not track bytes — which is the reason not to assume the reader's does either |
| **the rollup's peak memory beyond three thread settings** | §3.5 measures `max_threads` 1, 4 and 16 on one idle machine, and the ordering between the rollup and the full scan reverses inside that range. Nothing was measured under concurrency, on another machine, or at another `max_block_size`, and no rule is offered for where the crossover sits |
| **that the published DDL is the DDL the product would create** | §5's two families were built by typing the `CREATE TABLE` statements of §1.1 and §3.1 into a container, not by running the repository's own initialiser. A difference between what this document prints and what the migration catalogue renders would not show up in that measurement — §8's statement sequence is what checks it, and it was run separately |
| **the with-projection reading in §2.4** | it is a copy of C1's span table with `service_time` added by hand, because the published corpus script creates no projections. It matches the shipped shape in the one respect the statement depends on, and it was not produced by the product's own path |
| **the locate-then-test probe for the other three predicate classes** | §4 Q1 measures bytes for one shape — a numeric range — and checks the answers for a string equality, a numeric equality and an unscoped condition on an eight-span fixture. A regex probe and a bare key-existence probe are rendered the same way and were not measured at all |
| **the changed answers at corpus scale** | no span in C1 repeats a key (`countIf(arrayCount(k -> k = 'http.status_code', attr_key) > 1)` returns 0), and no C1 span carries one key at two scopes. The three answer changes §6.1 records are therefore measured on an eight-span fixture and on nothing larger |

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

> **Historical, 2026-09-17.** The window was reopened by a ruling on issue #498, for the
> reason the policy already states: there is no tagged release and no persistent
> deployment, so a `CREATE` can still be edited where it stands. That issue widened
> `fingerprint` to `UInt128` in migrations 4, 5, 6, 7, 8, 9, 23 and 29. **The analysis
> below and its conclusion are unchanged** — the span change is expressible as
> append-only migrations and needed no amendment, which is why it took none. What has
> moved is the surrounding statement of policy: the inventory is **four** places, this
> passage included, and the window's latest occupant is issue #498 rather than issue #54.

**Amending migration 16 and 18 in place is not currently permitted.** Three places say
the migration catalogue is append-only and that the window for in-place amendment closed:

> Migrations are idempotent, and append-only from the first tagged release onward —
> in-place amendment of an already-listed migration was permitted only pre-release (the
> trace-index scope amendment, issue #54, was the last such window; see docs/schemas.md §6)
> — `docs/architecture.md:96`

> the trace-index scope amendment (issue #54) was the last such amendment window
> — `docs/schemas.md:967`

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
  id    scope              statement
  ----  -----------------  --------------------------------------------------------------
  next  PerShard           ALTER TABLE trace_spans ADD COLUMN IF NOT EXISTS attr_key    Array(LowCardinality(String))
  next  PerShard, CLUSTER  ALTER TABLE trace_spans_dist ADD COLUMN IF NOT EXISTS attr_key    Array(LowCardinality(String))
  next  PerShard           ALTER TABLE trace_spans ADD COLUMN IF NOT EXISTS attr_scope  Array(LowCardinality(String))
  next  PerShard, CLUSTER  ALTER TABLE trace_spans_dist ADD COLUMN IF NOT EXISTS attr_scope  Array(LowCardinality(String))
  next  PerShard           ALTER TABLE trace_spans ADD COLUMN IF NOT EXISTS attr_val    Array(String)
  next  PerShard, CLUSTER  ALTER TABLE trace_spans_dist ADD COLUMN IF NOT EXISTS attr_val    Array(String)
  next  PerShard           ALTER TABLE trace_spans ADD COLUMN IF NOT EXISTS attr_type   Array(LowCardinality(String))
  next  PerShard, CLUSTER  ALTER TABLE trace_spans_dist ADD COLUMN IF NOT EXISTS attr_type   Array(LowCardinality(String))
  next  PerShard           ALTER TABLE trace_spans ADD COLUMN IF NOT EXISTS attr_num    Array(Nullable(Float64))
  next  PerShard, CLUSTER  ALTER TABLE trace_spans_dist ADD COLUMN IF NOT EXISTS attr_num    Array(Nullable(Float64))
  next  PerShard           ALTER TABLE trace_spans ADD CONSTRAINT IF NOT EXISTS attr_arrays_aligned CHECK …
                           (no _dist twin — see below)
  44    PerShard           ALTER TABLE trace_spans DROP PROJECTION IF EXISTS service_time
  45    PerShard           ALTER TABLE trace_spans ADD PROJECTION IF NOT EXISTS service_time (<14 named columns> ORDER BY (service, timestamp_ns))
  46    PerShard           ALTER TABLE trace_spans MATERIALIZE PROJECTION service_time
  47    PerShard           ALTER TABLE trace_spans ADD PROJECTION IF NOT EXISTS name_time    (<the same 14>  ORDER BY (name, timestamp_ns))
  48    PerShard           ALTER TABLE trace_spans MATERIALIZE PROJECTION name_time
  next  Global             DROP TABLE IF EXISTS trace_tag_catalog        <- see the note below
  next  Global             CREATE TABLE trace_tag_catalog (<the new shape>)
  next  PerShard, CLUSTER  DROP TABLE IF EXISTS trace_attrs_idx_dist
  next  PerShard           DROP TABLE IF EXISTS trace_attrs_idx
  next  PerShard           CREATE TABLE trace_attr_traces   (AggregatingMergeTree)
  next  PerShard, Dist     CREATE TABLE trace_attr_traces_dist   AS trace_attr_traces
                           ENGINE = Distributed('{cluster}', {db}, trace_attr_traces, cityHash64(trace_id))
  next  PerShard           CREATE TABLE trace_error_spans   (ReplacingMergeTree)
  next  PerShard, Dist     CREATE TABLE trace_error_spans_dist   … cityHash64(trace_id)
  next  PerShard           CREATE TABLE trace_recent        (AggregatingMergeTree)
  next  PerShard, Dist     CREATE TABLE trace_recent_dist        … cityHash64(trace_id)
```

**The five projection statements have shipped, as ids 44–48** (issue #555, the first part of this
record to land). Every other row reads `next`: it takes the next free id when its own part lands,
and which id that is cannot be decided here. An earlier version of this table numbered all
twenty-six in one run, 44–69, which reads as a commitment the build order has already broken once —
the projections shipped before the arrays. Renumbering the remaining twenty-one to 49–69 would be a
second guess at the same thing, so they say `next` instead. The ids are identity, not sequence;
nothing reads 44 as `attr_key`.

`PerShard, CLUSTER` is `Ddl::StaticClusterOnly`: skipped and unrecorded on a single node,
applied the first time clustering is enabled. `PerShard, Dist` is `Ddl::Dist`, rendered
by `render::dist_ddl_template` from `Family::Traces`'s single sharding expression, so all
four trace wrappers co-shard on `cityHash64(trace_id)` and every read joins shard-locally
(§7). `Global` is the catalogue's one cluster-wide replica set, no wrapper.

**Why the tag catalogue is dropped and recreated rather than altered.** The catalogue's new shape changes
both `PARTITION BY` and the leading columns of `ORDER BY`. Neither is reachable by an
`ALTER`, and that is established by enumerating every place a capability can live rather
than by trying one statement. Measured on 26.3.29.7:

    ALTER TABLE t MODIFY ORDER BY (scope, key, d, val)   -- a prefix change
      Code: 36  Primary key must be a prefix of the sorting key …
    ALTER TABLE t MODIFY ORDER BY (scope, key, val, d)   -- appending an existing column
      Code: 36  Existing column d is used in the expression that was added to the sorting key
    ALTER TABLE t MODIFY PARTITION BY d
      Code: 62  Syntax error … Expected …          (not grammar at all)
    ALTER TABLE t MODIFY SETTING partition_by = 'd'
      Code: 115 Unknown setting 'partition_by'

    settings           `system.settings` and `system.merge_tree_settings` matching
                       `order_by`/`partition` are query-planner and merge-scheduling
                       settings; none re-keys or re-partitions a table
    schema providers   `format_schema_source` supplies a schema to a FORMAT; it does not
                       touch table storage
    table functions,   none re-keys an existing MergeTree in place
    engines, views,
    formats
    SQL statements     `MODIFY PRIMARY KEY` -> Code 62; `MODIFY SAMPLE BY` -> Code 36;
    and grammar        `ATTACH PARTITION FROM`, `REPLACE PARTITION FROM` and
                       `MOVE PARTITION TO TABLE` between differently ordered tables all
                       -> Code 36, "Tables have different ordering"

**The enumeration is per claim, not a fixed list.** `ALTER … MODIFY TTL` and
`EXCHANGE TABLES` are capabilities exposed by SQL grammar and belong to none of the
classes above; a search that stopped at those classes would have missed them. What the
rule requires is that a capability claim names the surfaces it searched and why that set
is the relevant one for the claim — here, every surface that could re-key or re-partition
an existing part.

What **is** available is a rebuild plus `EXCHANGE TABLES`, which is what §8.1's second
backfill uses. With no data to keep, the drop is the same thing at lower cost.

**One grammar note, because the executable examples below are statements someone will
paste.** (The migration table above is schematic — it abbreviates column lists and shapes
— and is read against §3.1's DDL, not pasted.) `SETTINGS` goes **before** `VALUES` in an `INSERT`, or in the HTTP query string.
After `VALUES` it is parsed as row data and rejected. Measured on 26.3.29.7:

    INSERT INTO t VALUES (1) SETTINGS async_insert=0        HTTP 400  Code: 27  Cannot parse input: expected '(' before: 'SETTINGS …
    INSERT INTO t (a) VALUES ( SETTINGS async_insert=0       HTTP 400  Code: 62  Cannot parse expression of type UInt8 here: SETTINGS …
    INSERT INTO t SETTINGS async_insert=0 VALUES (2)         HTTP 200
    INSERT INTO t VALUES (3)   with ?async_insert=0          HTTP 200
    INSERT INTO t VALUES (4); SETTINGS async_insert=0        HTTP 200   <- the row lands, the setting is DROPPED
    INSERT INTO t VALUES (5); THIS IS IGNORED                HTTP 200   <- so does anything else after the semicolon

The rejection code depends on where the parser gives up: `Code: 27` when the row is
complete and the clause is read as another row, `Code: 62` when the row is incomplete and
the clause is read as a missing expression. Both are HTTP 400, and both read like "the
setting was applied and the insert failed" when the setting was never seen.

**The semicolon case is the dangerous one, because it returns 200.** A semicolon ends the
`VALUES` input over HTTP and everything after it is ignored — including a `SETTINGS`
clause meant to be applied. Measured with a setting whose effect is visible:

    INSERT INTO t2 SETTINGS max_partitions_per_insert_block=1 VALUES ('2023-11-14',1),('2023-11-15',2)
      -> HTTP 500  Code: 252  Too many partitions for single INSERT block
    the same statement with the setting as a query parameter
      -> HTTP 500  Code: 252
    INSERT INTO t2 VALUES ('2023-11-14',5),('2023-11-15',6); SETTINGS max_partitions_per_insert_block=1
      -> HTTP 200, two rows stored

So a statement that looks like it carries a setting, returns success, and stores its rows
may have run with none of it applied.

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
already use — **but 42/43 add a projection that did not exist, where 44–46 first drop one
that is serving reads.** Between 44 and 46 a `resource.service.name` search falls back to
a base-table scan: a correct answer, a slower one, for as long as the materialise takes.
On a fresh database that interval is empty. The named-column `service_time` requires
`shared`, `status_message`, `scope_name` and `scope_version` to exist, which they do by
the time a new id runs — 31/35/37 have already applied.

**Three of these statements destroy data, and the policy does not stop them.** The
append-only rule constrains mutation of an already-listed migration entry; it says nothing
about what a *new* entry may contain. So the catalogue drop and the two attribute-index drops
are formally allowed and would drop a populated catalogue and a populated attribute index. **"There is no data to keep"
is the issue's premise, not a property the sequence checks.**

### 8.1 What happens to rows that already exist

Measured, and it is not a migration. Starting from a database reconciled by the
repository's own initialiser (migrations 16/31/35/37/42/43) and populated with 50,000
spans, then running the remaining statements of §8:

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
in `trace_attrs_idx`, which the index drop removes.

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
3. **Sender order IS recoverable, from a column this design keeps.** An earlier version
   of this list said it was not, on the premise that only `trace_attrs_idx` could carry
   it and that table has no element-position column (`crates/pulsus-schema/src/catalog.rs:370-384`). That premise
   was wrong. `trace_spans.payload` holds a self-contained `TracesData` — this span with
   its own resource and scope, prost-encoded, both schema URLs kept — built by
   `build_payload` at `crates/pulsus-write/src/protocols/otlp_traces.rs:694-714`. **The sender's attribute order is inside
   it, for every stored span, on the table this change keeps.**

   So there are two backfills, not one, and they differ in exactly this:

   | backfill | attribute order | mechanism |
   |---|---|---|
   | from `trace_attrs_idx` | **imposed by the backfill.** A duplicated key can answer differently from a live row | pure SQL: the `joinGet` mutation or the rebuild above |
   | from `payload` | **the sender's, exactly.** A backfilled row answers identically to a live one | **a bounded server-side scan.** The `format` table function decodes a stored `String` and preserves element order. See below for what it takes |

   **An earlier version of this section said the second was "not expressible in SQL". It
   is.** That claim rested on a search of `system.functions`, which is the wrong place to
   look: a capability can live in a scalar function, a table function, a table engine, a
   view, or a **format**, and protobuf decoding lives in the last two. Measured on
   26.3.29.7:

       system.functions      no scalar protobuf decoder      (the search that produced the false claim)
       system.formats        Protobuf, ProtobufList, ProtobufSingle
       table functions       file, format, input, url, values
         file(...)           Code 107
         url(...)            bad URI, HTTP 500
         input(...)          Code 477 outside an INSERT
         format(...)         WORKS

       SELECT * FROM format(ProtobufSingle, 'x UInt8', unhex('0801'))                     -> 1
       SELECT * FROM format(ProtobufSingle, 'x UInt8', (SELECT payload FROM t WHERE id=1)) -> 1
       SELECT * FROM format(ProtobufSingle, 'x Array(UInt32)', unhex('0a020705'))          -> [7,5]
       SELECT * FROM format(ProtobufSingle, 'x Array(UInt32)', unhex('0a020507'))          -> [5,7]

   **Element order survives**, which is the whole point of decoding the payload at all.

   Two conditions the recipe has to meet, both measured:

   1. **Bulk decoding needs correct varint framing.** `ProtobufSingle` takes one message.
      `Protobuf` takes a length-delimited stream, and the blob can be built server-side
      from a stored column. The obvious `concat(char(length(p)), p)` is a **one-byte**
      varint and breaks above 127 bytes — on a 400-byte payload it returns
      `Code: 32 … Attempt to read after eof`. The two-byte form
      `char(bitOr(bitAnd(len,127),128), intDiv(len,128))` decoded the same payload and
      returned its 397 content bytes.
   2. **The generated schema numbers fields sequentially from 1.**
      `structureToProtobufSchema` builds nested `message`s with `repeated` fields, so the
      shape of `TracesData` is expressible — but a structure only decodes OTLP correctly
      where OTLP's own field numbers line up with the generated ones. **That end-to-end
      decode of a real span payload is not demonstrated here**; the mechanism is, and the
      framing is.

   **So the cost of the exact backfill is not settled here, and this states the condition
   rather than the answer.** What is established: the decode mechanism exists, it reads a
   stored column, it preserves element order, and bulk framing works with a correct varint.
   What is not: that a schema with OTLP's own field numbers decodes a real stored span
   payload, and that a batched rewrite runs within a stated memory and row budget.

       IF   (1) a committed `.proto` artefact whose every nested field number equals the
                encoded message's decodes a stored `trace_spans.payload` written by the
                repository's own OTLP path, for a span carrying one key twice, and yields
                the two values in the sender's order;
       AND  (2) a batched rewrite of 2,000,000 spans completes under these limits:
                batch                  <= 100,000 rows and <= 256 MiB of framed payload
                peak server memory     <= 4 GiB, read from system.query_log's
                                          memory_usage for every statement in the run
                permitted failures     0 statements returning non-200
                retry rule             none; the first non-200 aborts the run
       THEN the exact backfill is a bounded server-side scan and its cost is the measured
            wall time and bytes of that run.
       UNTIL both, its cost is unknown and an application re-ingest remains the only
       demonstrated route.

   Condition (1) is constructible today: the writer builds the self-contained payload at
   `crates/pulsus-write/src/protocols/otlp_traces.rs:697` and a live test already decodes the stored column
   (`crates/pulsus-write/tests/trace_ingest_roundtrip.rs:291`). Condition (2) has not been
   attempted.

   **Under the issue's premise neither backfill runs.** If one ever does, choosing the
   cheap one is choosing to let duplicated keys answer differently on either side of the
   cut-over, and that is a choice rather than a limitation.

**This question is settled, and the answer is that it does not arise.** Nothing is
deployed and there are no users, so there is no stored trace data anywhere that anyone
needs to keep; a developer with spans in a local database drops them. So **nothing is
built for it**: no backfill from the index table, no backfill from the payload, and no
refusal to start over a populated `trace_spans`. An earlier version of this section
proposed that refusal — a non-empty check beside `run_init`'s existing version check —
and worked out that the count would have to be cluster-wide rather than local. That
proposal is withdrawn; the readings above and below it stay, because they are readings.

What they are worth, now that nothing is built on them: the split of what survives the
remaining statements of §8 and what does not is what a developer sees before dropping the database; the two
backfill mechanisms are recorded as having been tried and found to work, for whoever
faces this question after there is data to keep; and the finding that a `CHECK`
constraint does not run during a mutation is a fact about the engine that outlives this
section.

The MV list and `TTL_STMTS` change either way:

```
  amend    the MV list    trace_tag_catalog_mv now reads trace_spans with an
                          ARRAY JOIN and a GROUP BY; three new views
  amend    TTL_STMTS      crates/pulsus-schema/src/controller.rs:436 is `[&str; 14]`. It loses the two
                          trace_attrs_idx statements and gains a MODIFY TTL and a
                          MODIFY SETTING for each of trace_attr_traces,
                          trace_recent, trace_error_spans and trace_tag_catalog:
                          14 - 2 + 8 = 20. The doc comment at crates/pulsus-schema/src/controller.rs:479-480,
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
   builders are deleted (`crates/pulsus-read/src/traces/search_sql.rs:286, 325`) and one is retargeted
   (`:397` — §4 Q1 says why it cannot become a column); the hydration builder gains **one
   resolved-element predicate column per attribute leaf** and a value column pair per read
   field; and the tag builders gain a `date` and a `service` clause. A schema that ships
   ahead of the builders answers nothing.

   **The predicate column is not `arrayExists`**, and this is the one place in §8 where
   getting it wrong changes answers rather than performance. Each leaf **locates one
   element and tests that element** — for a scoped condition

       arrayFirstIndex((key, scope) -> key = K AND scope = S, attr_key, attr_scope) AS i0
       (i0 != 0) AND <the value test applied to element i0>

   and for an unscoped one the five-index chain of §4 Q1, taking scopes span → resource →
   event → link → instrumentation and testing the element the chain lands on. §4 Q1
   measures three fixture spans on which `arrayExists` and this form give different
   answers, and §6.1 records the two kinds of change against what ships. An implementer
   who builds `arrayExists` here has built the semantics §4 argues against.

---

## 9. What could go wrong, and what would tell us early

| risk | what it would look like | the early signal |
|---|---|---|
| ~~the `SimpleAggregateFunction` half of the new view is rejected~~ | — | **read: it is not.** §11 P1 |
| ~~a search batch's 2.2× byte cost is structural~~ | — | **read: on a first-seen batch there is no 2.2×.** The cost of that batch turns on how selective the probed value is, and it is worse than today only for a highly selective probe. §4 Q1 |
| a materialized view throws and leaves the span stored with **some** derived rows and not others | a trace answers some search shapes and not others, and which ones varies between runs of the identical write | **read: this happens, non-deterministically.** §6.3's twenty trials. No remedy is chosen here |
| a probe's negation is rendered inside the array function rather than left to the reader | `{ span.k != "x" }` starts matching spans that carry `k = "x"` and stops matching spans with no `k` | §4 Q1's six-case negation table is the test |
| a leaf's predicate column is built as `arrayExists` rather than as locate-then-test | a span whose resolved value does not satisfy the query is returned, and then rendered with the value that does not match it | §4 Q1's eight-span fixture is the test: three of its eight rows separate the two forms. §8 says which column to build |
| the writer moves only the resource/span/instrumentation loop | `event:name`, `event:timeSinceStart`, `link:spanID`, `link:traceID` and every event and link attribute stop being searchable | §1.2. `crates/pulsus-write/src/protocols/otlp_traces.rs:528-630` is a second and third emission site with the same row shape |
| the base-table `ALTER`s ship without their `_dist` twins | single-node CI is green; the first clustered insert fails with `Code: 16 NO_SUCH_COLUMN_IN_TABLE` | §8. Single-node execution cannot see it — the check has to be a clustered insert |
| the duplicate-key rule is left to `arrayFirstIndex` without being stated | `avg`, `select` and `by` change answer on a span that repeats a key, silently | §4 Q1 states the rule and why it is the right one; it is a change of answer and needs a ledger row |
| the remaining statements of §8 run against a database that already holds trace rows | those spans stay reachable by service, name, duration and id, and unreachable by the empty search, every attribute condition, `status = error` and the tag dropdown | **not a risk this design carries.** §8.1: there is no deployment and no data to keep, so the answer is to drop the database. Nothing refuses, nothing backfills |
| a backfill is written as a mutation and trusted to be checked | `CHECK` constraints do not run during `ALTER … UPDATE`; a mutation can leave arrays of unequal length that an `INSERT` would reject | §8.1. Measured `HTTP 200` with `length(attr_key)=2`, `length(attr_num)=1`, and `Code: 469` for the same row inserted |
| five materialized views instead of two make ingest slower than the measurements suggest | insert wall time rises rather than falls | the nine write takes in §5 already measured the two-view case against the one-INSERT case; two of the three new views are a narrow filter and a narrow group. `trace_attr_traces_mv` is the one that expands and groups `A` rows per span, and it is the one to measure on its own. Measure insert wall time with each view added in turn |
| the metrics range query stays the slowest shape and someone adds a rollup later without re-checking §3.5 | `rate()` starts under-counting or over-counting after a client resends spans | §3.5 states the property the rollup must have and shows one form that has it — a distinct-span state, measured duplicate-safe at 16.30 B/span. Any future rollup is checked against the duplicate table in §3.4 before it is built |
| wider candidate sets push queries into the 100,000 ceiling that did not hit it before | responses turn partial | the ceiling is already reported to the client; the two tests in §6.1 pin the behaviour at the boundary |
| the whole trace family loses its stored history at cut-over | — | there is no history. Nothing is deployed |

---

## 10. What this does not do

- **A dropdown narrowed by an attribute** rather than by a service still has no
  cheap path. With no span-grained attribute index, the attribute half of the
  narrowing becomes an `ARRAY JOIN` over the window. On the query mix of §2.2 that is
  23% of the rows an investigation reads — a **derived** share, from the same unreproduced
  mix reading §7 lists, not a measurement — and it is unchanged. It is the one place this
  design is structurally worse than today, and it is not priced here.
- **It does not remove a derived table.** `trace_tag_catalog` has to stay: answering
  the dropdown off the span table reads the whole window where the catalog answers from
  a primary-key seek. Measured on corpus C1 with the two tables of §5's published build
  (26.3.29.7; `use_query_condition_cache=0`, `optimize_move_to_prewhere=1`,
  `max_block_size=65409`, `max_threads=16`; three repetitions, zero spread), both
  statements returning the same four values `200 400 500 503`, all `int`:

  | answering `values for http.status_code` | read_rows | read_bytes | marks |
  |---|---|---|---|
  | off the span row's arrays, `ARRAY JOIN` over the window | 2,000,000 | 360,155,560 | 245 |
  | off `trace_tag_catalog`, a primary-key seek | 16,384 | 52,172 | 2 |

  **6,903× the bytes**, for the same four values. An earlier version of this line gave
  `3,256 MB against 43 KB` with no statement, no settings and no repetition count behind
  it; those two figures are withdrawn and replaced by the table above. "One INSERT" is
  not "one table".
- **It does not speed up metrics range queries at all.** `| rate()`,
  `| count_over_time()` and every other metrics function still scan the window.
  §3.5 prices the exact rollup — built and measured, same answer, 16.30 B/span —
  and says why this design does not take it.
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
| **P3** — **READ, REFUTED, and the refutation is wider than first recorded** | a materialized view that throws fails the whole `INSERT`, so nothing is stored rather than half | insert a block through a view built to throw; check whether the source part exists | the source part is written and only the view's target is missing. **Outcome, over 300 trials: one thing held every time — the throwing view's own target is empty. Nothing else did**, including the source part, which was absent in 3 of 300. §6.3 has the distribution. This row's prediction is refuted; no replacement rule is stated |
| **P4** — **READ, not refuted; the conditional half is refuted** | `{}` reads 12× fewer rows guaranteed, and 144× if the read can stop at the newest bucket | `EXPLAIN indexes = 1` and `read_rows` for the `trace_recent` statement in §4 Q0 | `read_rows` is not below the span-table figure. **Outcome:** 167,277 against 2,000,000 — 11.96×, so not refuted. The 144× does not occur: `Granules: 22/22`, and three optimiser settings each read the same 167,277 rows and 22 marks. §4 Q0 has the detail |
| **P5** | `d`, the trace-grain collapse factor, is ≈2.5 on real traces | on one hour of real traffic: `count() / uniqExact((trace_id, scope, key, val))` over the expanded attribute rows | `d` < 1.3, at which point the index saving is a width saving only and the storage case weakens from −40% to roughly −20% |
| **P6** | storage is 1047.9 → 625.7 B/span | build both schemas from one source table, `OPTIMIZE … FINAL`, `sum(bytes_on_disk)` from `system.parts`, on two corpora with `A_t` at both ends of its range | the new schema is not smaller on a corpus with `A_t` ≥ 200 |
| **P7** | merge CPU falls ≈46%, because one ZSTD(3) pass over `payload` disappears | `OPTIMIZE … FINAL` both schemas over the same rows; `sum(ProfileEvents['OSCPUVirtualTimeMicroseconds'])` from `system.part_log` where `event_type = 'MergeParts'` | the new schema's merge CPU exceeds today's by more than 10% on a corpus with `P_b` ≥ 300 |
| **P8** | the candidate set is a superset of today's by at most `1 + B/W`, and the answer is identical | run every committed search golden against both schemas on one corpus at `W = B` and `W = 12B`; compare returned trace ids **and** candidate counts from `system.query_log` | any golden returns a different trace set, or the candidate count grows by more than `1 + B/W` |
| **P9** | trace-by-id, the service graph and a bare-column metrics query are **identical** on every counter | `read_rows`, `SelectedMarks`, `OSCPUVirtualTimeMicroseconds`, `NetworkSendBytes` on both schemas | any differs by more than the run-to-run spread |
| **P10** | statements per search are `2 + (1+P)·⌈C/32⌉` today and `2 + ⌈C/32⌉` after | count `QueryFinish` rows in `system.query_log` for one request | the count is not 4 for a one-batch, one-condition search today, or not 3 after |
| **P11** | **every table in §3.4 gives the same answer when a span row is written twice** | insert one block; record the answer to each of the nine queries in §4; insert the byte-identical block again; record again | any of the nine answers differs. That would mean a table in §3.4's safe column is not safe, and it is the same defect §3.5 rejected the rollup for |

| **P12** | **the resolved-value rule holds on every read path**: for a span that carries one key twice, and for a span that carries one key at two scopes, the filter, the negation, the scalar read and the grouping key all answer from the SAME element — the first stored element within the highest-precedence scope present, taking scopes in the order span → resource → event → link → instrumentation | the eight-span fixture of §4 Q1, ingested through our own writer rather than written as literals, then four requests per case: `{ span.n = 5 }`, `{ span.n != 5 }`, a `select(span.n)` pipe and a `by(span.n)` pipe; and the same four unscoped against a span carrying `resource.k` and `span.k` | any of the four disagrees with the others on the same span. **This is the reading the SQL correction in §4 Q1 exists for, and it is the one C1 cannot give**: no span in C1 repeats a key, so the corpus answers both forms identically and only a fixture separates them. Not yet run through the writer; run as SQL over a hand-built table, where three of the eight spans separate the two forms |

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
materialized view in §1 (`crates/pulsus-schema/src/catalog.rs:227-234, 244-256, 266-281, 335-407,
648-936, 934-1000`); every statement and its `SELECT` list (`crates/pulsus-read/src/traces/search_sql.rs:184,
230, 286, 325, 397, 428, 468, 492`; `crates/pulsus-read/src/traces/tags_sql.rs:89, 118, 253, 282`;
`crates/pulsus-read/src/traces/sql.rs:16-26`; and the committed goldens); the batch arithmetic (`crates/pulsus-read/src/traces/exec.rs:117`,
`crates/pulsus-config/src/model.rs:535-537`); which aggregates push down and what they read
(`crates/pulsus-read/src/traces/compile.rs:431-462, 560-562`); the write path's failure modes
(`crates/pulsus-write/src/writer/trace.rs:9-19, 137-185, 172`; `crates/pulsus-write/src/writer/table.rs:367-434`;
`crates/pulsus-write/src/writer/backfill.rs:23-28, 189-201, 214-220`); the wire framing
(`vendor/clickhouse/src/rowbinary/ser.rs:129, 137, 146, 222`) and that the
storage-to-reader hop is LZ4-framed (`crates/pulsus-clickhouse/src/pool.rs:695` →
`vendor/clickhouse/Cargo.toml:49` `default = ["lz4"]` →
`vendor/clickhouse/src/query.rs:221-231`, which appends `compress=1`).

**Measured elsewhere, and marked [M].** Two ClickHouse 26.3 corpora, 2,000,000
and 20,000,000 spans, one synthetic generator, one machine under load.
`bytes_on_disk` reproduced across three complete rebuilds; no bound claimed. The
2,000,000-span corpus is C1, published in §4 and pinned by the content identity
digest `fa2b6975…`; the index table it is read with is published in §2.1. **The
20,000,000-span corpus is neither published nor rebuildable from this document**,
and the only row that still rests on it is §5's write wall time, which claims a
direction and no ratio.

**Assumed, all named in Appendix A.** Eleven compression ratios and the
ZSTD(3)-to-LZ4 cost ratio. Every worked byte figure moves with them.

**Argued, with what would falsify each.**

- *That the identity cannot be made narrower* **is withdrawn: it was run, and it is
  wrong.** Seven codecs on `trace_id` and `span_id` in the index's own sort order
  (§2.1): the shipped `LZ4` stores the pair at 13.69 B/row — already under the
  14.5-byte entropy figure the argument rested on — and `ZSTD(1)` takes it to 12.82.
  The entropy figure is a bound on ids that repeat nowhere; C1's repeat 96 times each,
  and the codec finds them. What the section now claims is the measured 62.3% share
  and the 4.0% a codec is worth against the 40% a row-count change is worth.
- *That the bucket in position 4 preserves today's pruning exactly* rests on
  reading the two sort keys and on how a ClickHouse key condition uses a column
  at position 4. **ClickHouse is not checked out on this machine**, so its key
  condition code was not read. P2 is the same gap seen from the other side.
- *That the five arrays can carry every scope the writer emits* rests on all seven scopes
  having the same row shape — key, scope, val, val_type, val_num (`crates/pulsus-write/src/protocols/otlp_traces.rs:510-630`).
  It would be wrong if any scope needed a field the others do not have. Read, not run:
  no ingest path has been exercised end to end into the arrays.
- *That the scalar value read can replace `attr_values_sql`* rests on the read being
  scalar — one value per (span, key) — so one located element yields the row shape the
  hydration read already has. **The locator is over `(key, scope)` and nothing else**,
  which is §4 Q1's rule; an earlier version of this line cited a measurement of a
  locator that also required `isNotNull(val_num)`, and that is the form §4 Q1 names as
  wrong. Re-measured with the right one, on four spans:

      the span's stored k                     located   val     val_type   attr_num
      j=1, k=400                                 2       400      int        400
      k=400 then k=600                           1       400      int        400
      k='bad' (string) then k=400                1       bad      string     NULL
      no k at all                                0       —        —          NULL

  The third row is the whole of it: the located element is not numeric, so the numeric
  read is NULL and a numeric test on it is false. The locator that folds in
  `isNotNull(val_num)` returns element 2 on that span — `400 / int / 400` — which is a
  different value for the same attribute on the same span, and it is the value §4 Q1
  refuses. Not measured against today's `any(val_num)`, which picks arbitrarily where
  this form picks the element the span resolves to.
- *That no statement needs an attribute array from a projection* rests on
  enumerating the builders in `crates/pulsus-read/src/traces/search_sql.rs` (8), `crates/pulsus-read/src/traces/tags_sql.rs` (4), `crates/pulsus-read/src/traces/sql.rs`
  (1) and `crates/pulsus-read/src/traces/graph_sql.rs` (1), plus the structural fact that every attribute
  value in `crates/pulsus-read/src/traces/metrics_sql.rs` arrives through a join against the attribute table.
  For that file's builders that is **an argument about the file, not a reading
  of every one of them.**
- *That five arrays give byte-identical answers to today's two columns* rests on
  both sides parsing the same text with the same function, and it was measured
  bitwise on 6,000,000 values (§6). It fails if any future path populates
  `attr_num` from something other than the stored text.

**A figure taken through a wrapper is not a figure.** Every **query** counter in this
document was taken by running the statement itself rather than a wrapper around it, and
each carries the corpus and the settings it was taken with — §4's instrument for the
query counters in §§2, 4 and 5, §3.5's own thread sweep for the rollup, and
`sum(bytes_on_disk)` over `system.parts` for the storage readings in §2.1 and §5, which
are stored quantities and take no query settings. **One `[M]` row is weaker than that and
says so in itself**: §5's write wall time, on an unpublished 20,000,000-span corpus with
no per-statement settings recorded, claiming a direction and no ratio. Figures whose
method was not recorded and whose corpus no longer exists — the 20,000,000-span Q6b byte
and timing ratio, and Q7's original 1,516,384 / 65,764,280 — are **withdrawn rather than
carried**, and are not replaced by estimates.

**Three things are established more narrowly than an earlier version of this document
said.**

- The wrapper's byte difference is 48,001,456, and `trace_id` + `span_id` measured
  separately are 48,000,640. The mechanism is established from `EXPLAIN header = 1`; the
  816-byte residue is not attributed.
- The query-condition cache misses on a 32-id list not seen before, measured with the
  cache warm from a different list. **How often a real deployment repeats a list is not
  measured**, so "a production batch never repeats" is not a claim this document makes.
- **Every boundary rule this document has stated for a throwing view is withdrawn.**
  "No view committed" was refuted by three-sibling trials; "the source is always
  committed" was refuted at 3 of 300. §6.3 now records the distribution and asserts one
  thing only: the throwing view's own target was empty on 300 of 300 trials. The coupling
  between siblings, the cause of the empty-source cases, and any trial count for
  detection are **not** established here.
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
| phase-2 batch | 32 candidate traces | `crates/pulsus-read/src/traces/exec.rs:117` |
| candidate ceiling | 100,000 | `crates/pulsus-config/src/model.rs:535` |
| scan budget | 50,000,000 rows | `crates/pulsus-config/src/model.rs:536` |
| tag lookback default | 24 hours | `crates/pulsus-config/src/model.rs:537` |
| spans per trace cap | `LIMIT 10001 BY trace_id` | `crates/pulsus-read/src/traces/exec.rs:122` |
| tag name / value caps | 10,000 / 1,000 | `crates/pulsus-read/src/traces/exec.rs:130, 135` |
| storage → reader wire format | RowBinary, LZ4-framed | `crates/pulsus-clickhouse/src/pool.rs:695` → `vendor/clickhouse/Cargo.toml:49` → `vendor/clickhouse/src/query.rs:221-231` |
| shard key | `cityHash64(trace_id)` | `crates/pulsus-schema/src/render.rs:55-57` |
| pushed aggregates | `uniqExact(span_id)`, `max(duration_ns)`, `min(duration_ns)` | `crates/pulsus-read/src/traces/compile.rs:451-462` |

### Assumed compression ratios

Three columns name a codec (`crates/pulsus-schema/src/catalog.rs:346-347, 351`): `timestamp_ns
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

### Assumed raw widths

The merge and client-`INSERT` rows of §5 count **uncompressed** bytes, by this
rule. It is judgement, not measurement, and those four figures move with it (§7).

| column kind | raw bytes | worked |
|---|---|---|
| a fixed-width type | its width | `trace_id` 16, `span_id` 8, `Int64` 8, `Int8` 1 |
| `String` | 1 length byte + the text | `val` at `L_v` = 14 → 15; an empty `status_message` → 1 |
| `LowCardinality(String)` | its dictionary index only | 1 byte under 256 distinct values, 2 above — `name` and `key` take 2, `service`, `scope`, `scope_name`, `scope_version` and `val_type` take 1 |
| `Nullable(X)` | `X` + 1 | `val_num` → 9 |

At the worked parameters that gives a span row's 14 non-payload columns as **58**
raw bytes, one of today's index rows as **69**, one new index row as **73** and one
catalog row as **18**.

Two structural facts used throughout, which are not assumptions:

- **A random 16-byte `trace_id` compresses only where consecutive rows repeat
  it.** `trace_spans ORDER BY (trace_id, timestamp_ns)` puts a trace's `S` spans
  together, so the base table pays about `16/S`. A table sorted by attribute
  value pays the full 16 on every row — **and that last clause is the worked
  assumption, not a fact: it holds when a trace id appears once in a granule, and
  C1 measures 5.65 because each of its ids appears 96 times (§2.1, §7).**
- **ClickHouse is columnar.** A column a statement does not name costs it
  nothing. That is why adding five array columns is free for every query that
  does not read them, and it is **measured**: `{status = error}` reads 50,000,504
  bytes with the arrays present and 50,000,504 without.

---

## Appendix B — the calculator

Every derived figure above comes from this. It builds the new total twice — as a
direct sum and as a delta from today — and compares them, so an arithmetic slip
exits non-zero, **and it prints the block below**: save it as `calc.py`, run
`python3 calc.py`, and the output that follows is what you get, byte for byte.
An earlier version of this appendix carried the output without the statements that
produce it.

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

# --- raw bytes on the wire and in a merge --------------------------------
# One row's UNCOMPRESSED bytes, by the rule ClickHouse's native block uses:
#   fixed-width column        its width
#   String                    1 length byte + its text
#   LowCardinality(String)    its dictionary index only: 1 byte under 256
#                             distinct values, 2 above
#   Nullable(X)               X + 1
K_d, K_tot = 1e4, 1e6                        # Appendix A, the catalog parameters
sc_raw   = (16+8+8            # trace_id, span_id, parent_id
            + 2 + 1           # name (LC, > 256 distinct), service (LC, < 256)
            + 8 + 8           # timestamp_ns, duration_ns
            + 1+1+1+1         # status_code, kind, payload_type, shared
            + 1               # status_message, empty String
            + 1 + 1)          # scope_name, scope_version LowCardinality
base_raw = sc_raw + Pb
proj_old_raw = base_raw                      # SELECT * carries the payload
proj_new_raw = sc_raw                        # the 14 named columns: no payload, no arrays
idx_raw_today = (2                           # date
                 + 2 + (1+Lv) + 1            # key (LC, > 256), val (String), scope (LC)
                 + 9                         # val_num Nullable(Float64)
                 + 8 + 16 + 8 + 8)           # timestamp_ns, trace_id, span_id, duration_ns
cat_raw  = 2 + (1+Lv) + 1                    # one trace_tag_catalog row: key, val, scope
                                             # (val_type is LC, 1 byte, inside the 2)
merge_raw_today = base_raw + proj_old_raw + A*idx_raw_today + A*cat_raw
merge_raw_new   = (base_raw + arr_raw) + 2*proj_new_raw + rps*idx_raw \
                  + sigma_err*err_raw + recent_raw*(1 + t_trace/B)/S
lz4_today = merge_raw_today + (rho-1)*Pb*2   # two ZSTD(3) passes over the payload
lz4_new   = merge_raw_new   + (rho-1)*Pb     # one
ins_today = base_raw + A*idx_raw_today
ins_new   = base_raw + arr_raw

# --- the reads ------------------------------------------------------------
N_W = N*(W/86400.0)
trW = N_W/S
q0_new = trW*(1 + t_trace/B)
bkt = trW*B/W

# --- the crossover --------------------------------------------------------
# idx_today/idx_new hold (I+F)/A fixed at the worked ratio: I and F are stated
# as counts "of A", so a sweep over A keeps the FRACTION, not the count.
def idx_t(a): return Lv/Zv + ((I+F)/A)*8/Znum + 1/Znum + 8/Zts0 + 16 + 8 + 8/Zdur
def idx_n(a): return Lv/Zv + ((I+F)/A)*8/Znum + 1/Znum + 4/Zbkt + 16 + 8/Ztsu + 2*8/Zdur
def arr_of(a): return 3 + a*Lv/Zv + (I+F)*8/Znum + a/Znum
def At_cross(a, s): return s*(svc_star + a*idx_t(a) - arr_of(a) - proj)/idx_n(a)
lo, hi = 1.0, 1000.0                          # A where A*S == At_cross: S cancels
for _ in range(200):
    mid = (lo+hi)/2
    lo, hi = (mid, hi) if At_cross(mid, S)/S > mid else (lo, mid)
A_star = (lo+hi)/2
degenerate = (base + arr) + proj + (A*S/S)*idx_new

# --- the statements -------------------------------------------------------
import math
def stmts(m, sm):
    b = math.ceil((m/sm)/32)
    return b, 2 + (1+P)*b, 2 + b

# --- output ---------------------------------------------------------------
def tb(b): return b*N*R/1e12
print("A=%d  A_t=%d  d=%.2f  new index rows per span %.2f" % (A, A_t, d, rps))
print("arrays: 5 -> %.1f compressed / %d raw   (6 would be %.1f / %d)" % (arr, arr_raw, arr6, arr6_raw))
print("one index row: today %.2f | new %.2f | new with a 40-bit fingerprint %.2f"
      % (idx_today, idx_new, idx_new_fp))
print()
for lab, v in (("today", TODAY), ("payload out of service_time", STEP0),
               ("  + a codec on the index timestamp", STEP1),
               ("attributes on the span row", STEP2),
               ("  + the two sorted paths", STEP3),
               ("  + the recency index = THE PROPOSAL", NEW)):
    print("  %-36s%8.1f B/span%+8.1f%%  %.2f TB at N=1e9 R=%d"
          % (lab, v, 100.0*(v-TODAY)/TODAY, tb(v), R))
print()
print("  today, per table:")
for n_, v in (("trace_spans base", base), ("service_time (SELECT *)", svc_star),
              ("trace_attrs_idx", A*idx_today), ("trace_tag_catalog", 0.0)):
    print("    %-30s%6.1f%7.1f%%" % (n_, v, 100.0*v/TODAY))
print("  new, per table:")
for n_, v in (("trace_spans base + arrays", base+arr), ("service_time", proj),
              ("name_time", proj), ("trace_attr_traces", rps*idx_new),
              ("trace_error_spans", err_tbl), ("trace_recent", recent_b),
              ("trace_tag_catalog", 0.0)):
    print("    %-30s%6.1f%7.1f%%" % (n_, v, 100.0*v/NEW))
print()
print("  attribute-index share of today's family, across A:")
for a in (5, 8, 10, 20, 40, 60):
    tot = base + svc_star + a*idx_t(a)
    print("    A=%2d   index %7.1f of %7.1f = %.1f%%" % (a, a*idx_t(a), tot, 100.0*a*idx_t(a)/tot))
print()
print("  storage crossover, the index swap alone: the new layout stops being smaller")
print("    at A_t = %.1f, computed at A = %d, S = %d.  A_t cannot exceed A*S = %d, so"
      % (At_cross(A, S), A, S, A*S))
print("    at THESE parameters it is smaller at every A_t: the degenerate A_t = A*S")
print("    gives %.1f B/span against %.1f." % (degenerate, TODAY))
print("    That is a statement about A, not a universal one.  A*S < the crossover")
print("    reduces to A < %.1f, independent of S, and Appendix A allows A up to 60." % A_star)
print("    Above A = %.1f a trace whose attribute values never repeat is bigger in the" % A_star)
print("    new layout, by %.1f B/span at A = 60, S = 12."
      % ((base + arr_of(60)) + proj + 60*idx_n(60) - (base + svc_star + 60*idx_t(60))))
print()
print("  the metrics rollup that was REJECTED (section 3.5), priced anyway,")
print("  at n_grp = %.0e - it is not in the total above:" % n_grp)
for br in (60.0, 300.0, 900.0):
    print("    B_r = %4.0fs   count/sum/min/max %6.2f B/span   + a latency sketch %7.2f B/span"
          % (br, n_grp*(86400.0/br)*rollup_row/N, n_grp*(86400.0/br)*Qb/N))
print("    it would read n_grp*W/B_r = %.2e rows for a 1 h query against %.2e" % (n_grp*W/B_r, N_W))
print()
print("  merge, raw B/span/level     today %6.0f   new %6.0f   %.1f%%"
      % (merge_raw_today, merge_raw_new, 100.0*(merge_raw_new-merge_raw_today)/merge_raw_today))
print("  merge, LZ4-equivalent       today %6.0f   new %6.0f   %.1f%%"
      % (lz4_today, lz4_new, 100.0*(lz4_new-lz4_today)/lz4_today))
print("  client INSERT, raw B/span   today %6.0f in 2 statements   new %6.0f in 1   %.1f%%"
      % (ins_today, ins_new, 100.0*(ins_new-ins_today)/ins_today))
print("  catalog rows the MV writes per span   today %d   new  <= distinct tuples per block" % A)
print()
print("  W = 1 h  ->  N_W = %.3e spans, %.3e traces in the window" % (N_W, trW))
for lab, t_, n_ in (("Q0  {}", N_W, q0_new),
                    ("Q1  service+attr+dur, phase 1", sigma_svc*N_W, sigma_svc*N_W),
                    ("Q2  attribute only", f_k*N_W, f_k*N_W/d),
                    ("Q3a {status = error}", N_W, sigma_err*N_W),
                    ('Q3b {name = "GET /pay"}', N_W, sigma_name*N_W),
                    ("Q6a | rate() by(service)", N_W, N_W)):
    print("    %-32s today %.3e   new %.3e   x%.1f" % (lab, t_, n_, t_/n_))
print("    %-32s today %.3e   new %.3e   x%d" % ("Q4a/Q4b the tag dropdown", K_tot, K_d, K_tot/K_d))
print("    Q5 trace by id / Q7 service graph      identical statements, identical tables")
print()
print("  {} against trace_recent: %.3e trace rows vs %.3e span rows = x%.1f guaranteed"
      % (q0_new, N_W, N_W/q0_new))
print("    ... x%.0f only if the read stops at the newest bucket (%.2e traces per %.0fs bucket)"
      % (N_W/bkt, bkt, B))
print()
b0, t0, n0 = stmts(M, sigma_match)
print("  statements per search, one attribute condition, M=%d, sigma_match=%.2f:" % (M, sigma_match))
print("    today  2 + (1+P)*ceil(C_used/32) = %d      new  2 + ceil(C_used/32) = %d" % (t0, n0))
for m_, sm_ in ((1000, 1.0), (1000, 0.25)):
    b_, t_, n_ = stmts(m_, sm_)
    print("    M=%d sigma_match=%.2f  batches %4d   today %5d   new %5d" % (m_, sm_, b_, t_, n_))
b_ = 100000//32
print("    at the candidate ceiling (100000/32 = %d batches)  today %5d   new %5d"
      % (b_, 2+(1+P)*b_, 2+b_))
```

Its output at Appendix A's parameters, `python3 calc.py`:

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

  storage crossover, the index swap alone: the new layout stops being smaller
    at A_t = 246.5, computed at A = 20, S = 12.  A_t cannot exceed A*S = 240, so
    at THESE parameters it is smaller at every A_t: the degenerate A_t = A*S
    gives 1027.9 B/span against 1047.9.
    That is a statement about A, not a universal one.  A*S < the crossover
    reduces to A < 27.5, independent of S, and Appendix A allows A up to 60.
    Above A = 27.5 a trace whose attribute values never repeat is bigger in the
    new layout, by 86.2 B/span at A = 60, S = 12.

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

---

## Appendix C — the cited lines

Every claim this document makes about what the code does today names a file and a
line range. **This appendix quotes those lines**, so a reading can be checked
against the code without leaving the document.

**Completeness, stated exactly and counted twice.** The body carries **109** citations
that name a line range, over 33 files, and all 109 are quoted below. It also carries
**12** citations that name a file with no line range — they point at a whole builder, a
whole golden file, or a document rather than at a reading — and those are not quoted
here:

    crates/pulsus-read/src/traces/graph_sql.rs
    crates/pulsus-read/src/traces/metrics_sql.rs
    crates/pulsus-read/src/traces/search_eval.rs
    crates/pulsus-read/src/traces/search_sql.rs
    crates/pulsus-read/src/traces/sql.rs
    crates/pulsus-read/src/traces/tags_sql.rs
    crates/pulsus-read/tests/golden/traces_graph/single_node.sql
    crates/pulsus-read/tests/golden/traces_metrics/attr_semi_join.sql
    docs/api.md
    docs/benchmarks/traces-differential-ledger.md
    docs/schemas.md
    e2e/src/traces_corpus.rs

**Two earlier counts were wrong, and each was wrong in its own way.** The first said 97
because the script that produced it matched three file extensions and dropped five
golden `.sql` files and three document lines. The second said 108 and 9 because it did
not resolve a **shorthand** citation — a path given once and then continued with a bare
`` `:N` ``, as in "`crates/pulsus-write/src/writer/trace.rs:9-19`, and `admit_batch` at
`` `:220-304` ``" and "`crates/pulsus-schema/src/controller.rs`'s `check_version`, called
at `` `:89` ``". Six such shorthands appear in the body; resolving them adds
`crates/pulsus-write/src/writer/trace.rs:220-304` and
`crates/pulsus-schema/src/controller.rs:89` to the quoted set at the time. That second
citation has since gone: the passage that made it proposed a refusal to start over a
populated table, and a later ruling withdrew that passage (§8.1), so the count is 109 and
the controller is cited only by its two other ranges. The file-level list grew to 12 when
the duplicate-key divergence was recorded in two documents and two source files (§4 Q1).

**Pinned at `8f3348e8`** — the tip of `main` when this appendix was written. The
line numbers are that revision's and will drift; the quoted bytes are what the
reading rests on.

**Checked, not assumed:** every range was compared byte for byte between `86081ef1` —
the commit this branch is cut from, and the revision the readings were taken at — and
`8f3348e8`. **Every code range is identical**, so no reading in this document has gone
stale under it. The two `docs/schemas.md` lines are the exception and are marked where
they appear: they are the citations round one corrected, and they differ between the two
revisions because the file moved under them.

**What is quoted.** A range of 25 lines or fewer is quoted in full. A longer one is
quoted with its comments and blank lines removed and the first 30 remaining lines
shown; the header of each block says so and gives the full range, so the elision is
visible rather than silent.

**The quoted lines are verbatim source.** Where a quoted comment names a file by its
bare name, that is the source's own text, not a citation made by this document. This
document's own citations are the paths outside these blocks, and every one of them is
repository-relative.

### `.github/workflows/ci.yml`

**`:567-572`**

```yaml
  567        - name: Start ClickHouse 26.3
  568          run: |
  569            docker run -d --name pulsus-ch-schema-it -p 19123:8123 -p 19000:9000 \
  570              -v "$GITHUB_WORKSPACE"/ci/clickhouse-cluster/users.d/network.xml:/etc/clickhouse-server/users.d/zz-network.xml:ro \
  571              clickhouse/clickhouse-server:26.3
  572            for _ in $(seq 1 30); do
```

### `crates/pulsus-schema/src/catalog.rs`

**`:16-23`**

```rust
   16  //! **Amendment policy:** migrations are append-only from the first tagged
   17  //! release onward. In-place amendment of an already-listed migration was
   18  //! permitted only pre-release (no tagged release, no persistent
   19  //! deployments, CI databases created fresh per run), and issue #54's scope
   20  //! amendment of migrations 17/18 + `trace_tag_catalog_mv` was the last such
   21  //! amendment window (task-manager ruling on #54). Developers with a local
   22  //! schema created before that amendment must drop and re-reconcile it —
   23  //! the checksum drift guard ([`MigrationScope::Checksum`]) correctly
```

**`:227-234`**

```rust
  227              "CREATE TABLE IF NOT EXISTS {{db}}.log_streams_idx{{on_cluster}} (\n\
  228                   month        Date,\n\
  229                   key          LowCardinality(String),\n\
  230                   val          String,\n\
  231                   fingerprint  UInt64\n\
  232               ) ENGINE = ReplacingMergeTree\n\
  233               PARTITION BY month\n\
  234               ORDER BY (key, val, fingerprint);",
```

**`:244-256`**

```rust
  244              "CREATE TABLE IF NOT EXISTS {{db}}.log_samples{{on_cluster}} (\n\
  245                   service       LowCardinality(String),\n\
  246                   fingerprint   UInt64,\n\
  247                   timestamp_ns  Int64   CODEC(DoubleDelta, ZSTD(1)),\n\
  248                   severity      Int8    DEFAULT 0,\n\
  249                   body          String  CODEC(ZSTD(1)),\n\
  250                   INDEX idx_body_tokens body TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1,\n\
  251                   INDEX idx_body_ngrams body TYPE ngrambf_v1(4, 32768, 3, 0) GRANULARITY 1,\n\
  252                   INDEX idx_severity severity TYPE minmax GRANULARITY 4\n\
  253               ) ENGINE = MergeTree\n\
  254               PARTITION BY toDate(fromUnixTimestamp64Nano(timestamp_ns))\n\
  255               ORDER BY (service, fingerprint, timestamp_ns)\n\
  256               TTL toDateTime(fromUnixTimestamp64Nano(timestamp_ns)) + INTERVAL {{retention_days}} DAY DELETE\n\
```

**`:266-281`**

```rust
  266      Migration {
  267          id: 9,
  268          name: "log_metrics_{{log_rollup_suffix}}",
  269          family: Some(Family::Logs),
  270          ddl: Ddl::Static(
  271              "CREATE TABLE IF NOT EXISTS {{db}}.log_metrics_{{log_rollup_suffix}}{{on_cluster}} (\n\
  272                   fingerprint  UInt64,\n\
  273                   bucket_ns    Int64,\n\
  274                   count        SimpleAggregateFunction(sum, UInt64),\n\
  275                   bytes        SimpleAggregateFunction(sum, UInt64)\n\
  276               ) ENGINE = AggregatingMergeTree\n\
  277               PARTITION BY toDate(fromUnixTimestamp64Nano(bucket_ns))\n\
  278               ORDER BY (fingerprint, bucket_ns);",
  279          ),
  280          scope: MigrationScope::ConfigName,
  281          replication: Replication::PerShard,
```

**`:335-364`**  (30 lines in the range; comments and blanks elided, 30 shown)

```rust
  335      Migration {
  336          id: 16,
  337          name: "trace_spans",
  338          family: Some(Family::Traces),
  339          ddl: Ddl::Static(
  340              "CREATE TABLE IF NOT EXISTS {{db}}.trace_spans{{on_cluster}} (\n\
  341                   trace_id      FixedString(16),\n\
  342                   span_id       FixedString(8),\n\
  343                   parent_id     FixedString(8),\n\
  344                   name          LowCardinality(String),\n\
  345                   service       LowCardinality(String),\n\
  346                   timestamp_ns  Int64  CODEC(DoubleDelta, ZSTD(1)),\n\
  347                   duration_ns   Int64  CODEC(T64, ZSTD(1)),\n\
  348                   status_code   Int8,\n\
  349                   kind          Int8,\n\
  350                   payload_type  Int8,\n\
  351                   payload       String CODEC(ZSTD(3)),\n\
  352                   INDEX idx_duration duration_ns TYPE minmax GRANULARITY 4,\n\
  353                   PROJECTION service_time (\n\
  354                       SELECT * ORDER BY (service, timestamp_ns)\n\
  355                   )\n\
  356               ) ENGINE = MergeTree\n\
  357               PARTITION BY toDate(fromUnixTimestamp64Nano(timestamp_ns))\n\
  358               ORDER BY (trace_id, timestamp_ns)\n\
  359               TTL toDateTime(fromUnixTimestamp64Nano(timestamp_ns)) + INTERVAL {{retention_days}} DAY DELETE\n\
  360               SETTINGS ttl_only_drop_parts = 1;",
  361          ),
  362          scope: MigrationScope::Checksum,
  363          replication: Replication::PerShard,
  364      },
```

**`:335-407`**  (73 lines in the range; comments and blanks elided, 30 shown)

```rust
  335      Migration {
  336          id: 16,
  337          name: "trace_spans",
  338          family: Some(Family::Traces),
  339          ddl: Ddl::Static(
  340              "CREATE TABLE IF NOT EXISTS {{db}}.trace_spans{{on_cluster}} (\n\
  341                   trace_id      FixedString(16),\n\
  342                   span_id       FixedString(8),\n\
  343                   parent_id     FixedString(8),\n\
  344                   name          LowCardinality(String),\n\
  345                   service       LowCardinality(String),\n\
  346                   timestamp_ns  Int64  CODEC(DoubleDelta, ZSTD(1)),\n\
  347                   duration_ns   Int64  CODEC(T64, ZSTD(1)),\n\
  348                   status_code   Int8,\n\
  349                   kind          Int8,\n\
  350                   payload_type  Int8,\n\
  351                   payload       String CODEC(ZSTD(3)),\n\
  352                   INDEX idx_duration duration_ns TYPE minmax GRANULARITY 4,\n\
  353                   PROJECTION service_time (\n\
  354                       SELECT * ORDER BY (service, timestamp_ns)\n\
  355                   )\n\
  356               ) ENGINE = MergeTree\n\
  357               PARTITION BY toDate(fromUnixTimestamp64Nano(timestamp_ns))\n\
  358               ORDER BY (trace_id, timestamp_ns)\n\
  359               TTL toDateTime(fromUnixTimestamp64Nano(timestamp_ns)) + INTERVAL {{retention_days}} DAY DELETE\n\
  360               SETTINGS ttl_only_drop_parts = 1;",
  361          ),
  362          scope: MigrationScope::Checksum,
  363          replication: Replication::PerShard,
  364      },
```

**`:346-347`**

```rust
  346                   timestamp_ns  Int64  CODEC(DoubleDelta, ZSTD(1)),\n\
  347                   duration_ns   Int64  CODEC(T64, ZSTD(1)),\n\
```

**`:351-351`**

```rust
  351                   payload       String CODEC(ZSTD(3)),\n\
```

**`:353-355`**

```rust
  353                   PROJECTION service_time (\n\
  354                       SELECT * ORDER BY (service, timestamp_ns)\n\
  355                   )\n\
```

**`:365-388`**

```rust
  365      Migration {
  366          id: 17,
  367          name: "trace_attrs_idx",
  368          family: Some(Family::Traces),
  369          ddl: Ddl::Static(
  370              "CREATE TABLE IF NOT EXISTS {{db}}.trace_attrs_idx{{on_cluster}} (\n\
  371                   date          Date,\n\
  372                   key           LowCardinality(String),\n\
  373                   val           String,\n\
  374                   scope         LowCardinality(String),\n\
  375                   val_num       Nullable(Float64),\n\
  376                   timestamp_ns  Int64,\n\
  377                   trace_id      FixedString(16),\n\
  378                   span_id       FixedString(8),\n\
  379                   duration_ns   Int64\n\
  380               ) ENGINE = ReplacingMergeTree\n\
  381               PARTITION BY date\n\
  382               ORDER BY (key, val, scope, timestamp_ns, trace_id, span_id)\n\
  383               TTL toDateTime(fromUnixTimestamp64Nano(timestamp_ns)) + INTERVAL {{retention_days}} DAY DELETE\n\
  384               SETTINGS ttl_only_drop_parts = 1;",
  385          ),
  386          scope: MigrationScope::Checksum,
  387          replication: Replication::PerShard,
  388      },
```

**`:370-384`**

```rust
  370              "CREATE TABLE IF NOT EXISTS {{db}}.trace_attrs_idx{{on_cluster}} (\n\
  371                   date          Date,\n\
  372                   key           LowCardinality(String),\n\
  373                   val           String,\n\
  374                   scope         LowCardinality(String),\n\
  375                   val_num       Nullable(Float64),\n\
  376                   timestamp_ns  Int64,\n\
  377                   trace_id      FixedString(16),\n\
  378                   span_id       FixedString(8),\n\
  379                   duration_ns   Int64\n\
  380               ) ENGINE = ReplacingMergeTree\n\
  381               PARTITION BY date\n\
  382               ORDER BY (key, val, scope, timestamp_ns, trace_id, span_id)\n\
  383               TTL toDateTime(fromUnixTimestamp64Nano(timestamp_ns)) + INTERVAL {{retention_days}} DAY DELETE\n\
  384               SETTINGS ttl_only_drop_parts = 1;",
```

**`:393-407`**

```rust
  393      Migration {
  394          id: 18,
  395          name: "trace_tag_catalog",
  396          family: None,
  397          ddl: Ddl::Static(
  398              "CREATE TABLE IF NOT EXISTS {{db}}.trace_tag_catalog{{on_cluster}} (\n\
  399                   scope  LowCardinality(String),\n\
  400                   key    LowCardinality(String),\n\
  401                   val    String\n\
  402               ) ENGINE = ReplacingMergeTree\n\
  403               ORDER BY (scope, key, val);",
  404          ),
  405          scope: MigrationScope::Checksum,
  406          replication: Replication::Global,
  407      },
```

**`:406-406`**

```rust
  406          replication: Replication::Global,
```

**`:492-498`**

```rust
  492                   pos_span_offsets   Array(Int32)   CODEC(ZSTD(1)),\n\
  493                   pos_span_lengths   Array(UInt32)  CODEC(ZSTD(1)),\n\
  494                   pos_bucket_deltas  Array(Int64)   CODEC(ZSTD(1)),\n\
  495                   neg_span_offsets   Array(Int32)   CODEC(ZSTD(1)),\n\
  496                   neg_span_lengths   Array(UInt32)  CODEC(ZSTD(1)),\n\
  497                   neg_bucket_deltas  Array(Int64)   CODEC(ZSTD(1)),\n\
  498                   custom_values      Array(Float64) CODEC(ZSTD(1))\n\
```

**`:648-658`**

```rust
  648      Migration {
  649          id: 31,
  650          name: "trace_spans",
  651          family: Some(Family::Traces),
  652          ddl: Ddl::Static(
  653              "ALTER TABLE {{db}}.trace_spans{{on_cluster}}\n\
  654               ADD COLUMN IF NOT EXISTS shared UInt8 DEFAULT 0;",
  655          ),
  656          scope: MigrationScope::Checksum,
  657          replication: Replication::PerShard,
  658      },
```

**`:648-936`**  (289 lines in the range; comments and blanks elided, 30 shown)

```rust
  648      Migration {
  649          id: 31,
  650          name: "trace_spans",
  651          family: Some(Family::Traces),
  652          ddl: Ddl::Static(
  653              "ALTER TABLE {{db}}.trace_spans{{on_cluster}}\n\
  654               ADD COLUMN IF NOT EXISTS shared UInt8 DEFAULT 0;",
  655          ),
  656          scope: MigrationScope::Checksum,
  657          replication: Replication::PerShard,
  658      },
  665      Migration {
  666          id: 32,
  667          name: "trace_spans",
  668          family: Some(Family::Traces),
  669          ddl: Ddl::StaticClusterOnly(
  670              "ALTER TABLE {{db}}.trace_spans{{dist_suffix}}{{on_cluster}}\n\
  671               ADD COLUMN IF NOT EXISTS shared UInt8 DEFAULT 0;",
  672          ),
  673          scope: MigrationScope::Checksum,
  674          replication: Replication::PerShard,
  675      },
  692      Migration {
  693          id: 33,
  694          name: "trace_edges",
  695          family: Some(Family::Traces),
  696          ddl: Ddl::Static(
  697              "CREATE TABLE IF NOT EXISTS {{db}}.trace_edges{{on_cluster}} (\n\
  698                   date          Date,\n\
  699                   side          UInt8,\n\
```

**`:692-716`**

```rust
  692      Migration {
  693          id: 33,
  694          name: "trace_edges",
  695          family: Some(Family::Traces),
  696          ddl: Ddl::Static(
  697              "CREATE TABLE IF NOT EXISTS {{db}}.trace_edges{{on_cluster}} (\n\
  698                   date          Date,\n\
  699                   side          UInt8,\n\
  700                   trace_id      FixedString(16),\n\
  701                   span_id       FixedString(8),\n\
  702                   pair_id       FixedString(8),\n\
  703                   conn_type     LowCardinality(String),\n\
  704                   timestamp_ns  Int64  CODEC(DoubleDelta, ZSTD(1)),\n\
  705                   service       LowCardinality(String),\n\
  706                   duration_ns   Int64  CODEC(T64, ZSTD(1)),\n\
  707                   failed        UInt8\n\
  708               ) ENGINE = ReplacingMergeTree\n\
  709               PARTITION BY date\n\
  710               ORDER BY (side, trace_id, span_id)\n\
  711               TTL toDateTime(fromUnixTimestamp64Nano(timestamp_ns)) + INTERVAL {{retention_days}} DAY DELETE\n\
  712               SETTINGS ttl_only_drop_parts = 1;",
  713          ),
  714          scope: MigrationScope::Checksum,
  715          replication: Replication::PerShard,
  716      },
```

**`:738-748`**

```rust
  738      Migration {
  739          id: 35,
  740          name: "trace_spans",
  741          family: Some(Family::Traces),
  742          ddl: Ddl::Static(
  743              "ALTER TABLE {{db}}.trace_spans{{on_cluster}}\n\
  744               ADD COLUMN IF NOT EXISTS status_message String DEFAULT '';",
  745          ),
  746          scope: MigrationScope::Checksum,
  747          replication: Replication::PerShard,
  748      },
```

**`:775-786`**

```rust
  775      Migration {
  776          id: 37,
  777          name: "trace_spans",
  778          family: Some(Family::Traces),
  779          ddl: Ddl::Static(
  780              "ALTER TABLE {{db}}.trace_spans{{on_cluster}}\n\
  781               ADD COLUMN IF NOT EXISTS scope_name LowCardinality(String) DEFAULT '',\n\
  782               ADD COLUMN IF NOT EXISTS scope_version LowCardinality(String) DEFAULT '';",
  783          ),
  784          scope: MigrationScope::Checksum,
  785          replication: Replication::PerShard,
  786      },
```

**`:812-822`**

```rust
  812      Migration {
  813          id: 39,
  814          name: "trace_attrs_idx",
  815          family: Some(Family::Traces),
  816          ddl: Ddl::Static(
  817              "ALTER TABLE {{db}}.trace_attrs_idx{{on_cluster}}\n\
  818               ADD COLUMN IF NOT EXISTS val_type LowCardinality(String) DEFAULT '';",
  819          ),
  820          scope: MigrationScope::Checksum,
  821          replication: Replication::PerShard,
  822      },
```

**`:934-942`**

```rust
  934          name: "log_streams_idx_mv",
  935          tmpl: "CREATE MATERIALIZED VIEW {{db}}.log_streams_idx_mv{{on_cluster}} TO {{db}}.log_streams_idx AS\n\
  936                 SELECT\n\
  937                     month,\n\
  938                     kv.1 AS key,\n\
  939                     kv.2 AS val,\n\
  940                     fingerprint\n\
  941                 FROM {{db}}.log_streams\n\
  942                 ARRAY JOIN JSONExtractKeysAndValues(labels, 'String') AS kv;",
```

**`:934-1000`**  (67 lines in the range; comments and blanks elided, 30 shown)

```rust
  934          name: "log_streams_idx_mv",
  935          tmpl: "CREATE MATERIALIZED VIEW {{db}}.log_streams_idx_mv{{on_cluster}} TO {{db}}.log_streams_idx AS\n\
  936                 SELECT\n\
  937                     month,\n\
  938                     kv.1 AS key,\n\
  939                     kv.2 AS val,\n\
  940                     fingerprint\n\
  941                 FROM {{db}}.log_streams\n\
  942                 ARRAY JOIN JSONExtractKeysAndValues(labels, 'String') AS kv;",
  943      },
  944      MvDef {
  945          name: "log_metrics_{{log_rollup_suffix}}_mv",
  946          tmpl: "CREATE MATERIALIZED VIEW {{db}}.log_metrics_{{log_rollup_suffix}}_mv{{on_cluster}} TO {{db}}.log_metrics_{{log_rollup_suffix}} AS\n\
  947                 SELECT\n\
  948                     fingerprint,\n\
  949                     intDiv(timestamp_ns, {{log_rollup_ns}}) * {{log_rollup_ns}} AS bucket_ns,\n\
  950                     count() AS count,\n\
  951                     sum(length(body)) AS bytes\n\
  952                 FROM {{db}}.log_samples\n\
  953                 GROUP BY fingerprint, bucket_ns;",
  954      },
  964      MvDef {
  965          name: "trace_tag_catalog_mv",
  966          tmpl: "CREATE MATERIALIZED VIEW {{db}}.trace_tag_catalog_mv{{on_cluster}} TO {{db}}.trace_tag_catalog AS\n\
  967                 SELECT scope, key, val, val_type\n\
  968                 FROM {{db}}.trace_attrs_idx;",
  969      },
  984      MvDef {
  985          name: "trace_edges_mv",
  986          tmpl: "CREATE MATERIALIZED VIEW {{db}}.trace_edges_mv{{on_cluster}} TO {{db}}.trace_edges AS\n\
```

**`:945-953`**

```rust
  945          name: "log_metrics_{{log_rollup_suffix}}_mv",
  946          tmpl: "CREATE MATERIALIZED VIEW {{db}}.log_metrics_{{log_rollup_suffix}}_mv{{on_cluster}} TO {{db}}.log_metrics_{{log_rollup_suffix}} AS\n\
  947                 SELECT\n\
  948                     fingerprint,\n\
  949                     intDiv(timestamp_ns, {{log_rollup_ns}}) * {{log_rollup_ns}} AS bucket_ns,\n\
  950                     count() AS count,\n\
  951                     sum(length(body)) AS bytes\n\
  952                 FROM {{db}}.log_samples\n\
  953                 GROUP BY fingerprint, bucket_ns;",
```

**`:965-969`**

```rust
  965          name: "trace_tag_catalog_mv",
  966          tmpl: "CREATE MATERIALIZED VIEW {{db}}.trace_tag_catalog_mv{{on_cluster}} TO {{db}}.trace_tag_catalog AS\n\
  967                 SELECT scope, key, val, val_type\n\
  968                 FROM {{db}}.trace_attrs_idx;",
  969      },
```

**`:985-1000`**

```rust
  985          name: "trace_edges_mv",
  986          tmpl: "CREATE MATERIALIZED VIEW {{db}}.trace_edges_mv{{on_cluster}} TO {{db}}.trace_edges AS\n\
  987                 SELECT\n\
  988                     toDate(fromUnixTimestamp64Nano(timestamp_ns)) AS date,\n\
  989                     toUInt8(kind IN (2, 5)) AS side,\n\
  990                     trace_id,\n\
  991                     span_id,\n\
  992                     if(kind IN (3, 4) OR shared = 1, span_id, parent_id) AS pair_id,\n\
  993                     if(kind IN (2, 3), 'rpc', 'messaging') AS conn_type,\n\
  994                     timestamp_ns,\n\
  995                     service,\n\
  996                     duration_ns,\n\
  997                     toUInt8(status_code = 2) AS failed\n\
  998                 FROM {{db}}.trace_spans\n\
  999                 WHERE kind IN (3, 4)\n\
 1000                    OR (kind IN (2, 5) AND (shared = 1 OR parent_id != toFixedString(unhex('0000000000000000'), 8)));",
```

### `crates/pulsus-schema/src/controller.rs`

**`:436-436`**

```rust
  436  const TTL_STMTS: [&str; 14] = [
```

**`:436-472`**  (37 lines in the range; comments and blanks elided, 23 shown)

```rust
  436  const TTL_STMTS: [&str; 14] = [
  437      "ALTER TABLE {{db}}.metric_samples{{on_cluster}} MODIFY TTL \
  438       toDateTime(least(intDiv(unix_milli, 1000) + {{retention_days}} * 86400, 4294967295)) DELETE;",
  439      "ALTER TABLE {{db}}.metric_samples{{on_cluster}} MODIFY SETTING ttl_only_drop_parts = 1;",
  440      "ALTER TABLE {{db}}.log_samples{{on_cluster}} MODIFY TTL \
  441       toDateTime(least(intDiv(timestamp_ns, 1000000000) + {{retention_days}} * 86400, 4294967295)) DELETE;",
  442      "ALTER TABLE {{db}}.log_samples{{on_cluster}} MODIFY SETTING ttl_only_drop_parts = 1;",
  443      "ALTER TABLE {{db}}.trace_spans{{on_cluster}} MODIFY TTL \
  444       toDateTime(least(intDiv(timestamp_ns, 1000000000) + {{retention_days}} * 86400, 4294967295)) DELETE;",
  445      "ALTER TABLE {{db}}.trace_spans{{on_cluster}} MODIFY SETTING ttl_only_drop_parts = 1;",
  446      "ALTER TABLE {{db}}.trace_attrs_idx{{on_cluster}} MODIFY TTL \
  447       toDateTime(least(intDiv(timestamp_ns, 1000000000) + {{retention_days}} * 86400, 4294967295)) DELETE;",
  448      "ALTER TABLE {{db}}.trace_attrs_idx{{on_cluster}} MODIFY SETTING ttl_only_drop_parts = 1;",
  449      "ALTER TABLE {{db}}.metric_hist_samples{{on_cluster}} MODIFY TTL \
  450       toDateTime(least(intDiv(unix_milli, 1000) + {{retention_days}} * 86400, 4294967295)) DELETE;",
  451      "ALTER TABLE {{db}}.metric_hist_samples{{on_cluster}} MODIFY SETTING ttl_only_drop_parts = 1;",
  458      "ALTER TABLE {{db}}.trace_edges{{on_cluster}} MODIFY TTL \
  459       toDateTime(least(intDiv(timestamp_ns, 1000000000) + {{retention_days}} * 86400, 4294967295)) DELETE;",
  460      "ALTER TABLE {{db}}.trace_edges{{on_cluster}} MODIFY SETTING ttl_only_drop_parts = 1;",
  469      "ALTER TABLE {{db}}.log_patterns{{on_cluster}} MODIFY TTL \
  470       toDateTime(least(intDiv(bucket_ns, 1000000000) + {{retention_days}} * 86400, 4294967295)) DELETE;",
  471      "ALTER TABLE {{db}}.log_patterns{{on_cluster}} MODIFY SETTING ttl_only_drop_parts = 1;",
  472  ];
```

**`:479-480`**

```rust
  479  /// adjudication on issue #53; `trace_tag_catalog` is a bounded catalog
  480  /// and carries no TTL). `ALTER
```

### `crates/pulsus-write/src/protocols/otlp_traces.rs`

**`:465-486`**

```rust
  465          None => {
  466              // The timestamp is representable as `i64` ns but its day falls
  467              // outside the storage-safe range: before 1970-01-01, or past
  468              // day 49_709 (2106-02-06) — the last UTC day fully inside the
  469              // 32-bit DateTime domain the trace tables' delete-TTL evaluates
  470              // in (issue #131; days 49_710..=65_535 would partition
  471              // correctly but wrap in the TTL expression, and later days fall
  472              // outside the `Date` range entirely). Saturating would orphan
  473              // or silently early-expire the span, so it is rejected
  474              // wholesale into partial success.
  475              reject_span(
  476                  out,
  477                  format!(
  478                      "span {:?}: start_time_unix_nano {} is outside the supported \
  479                       storage time range (1970-01-01 to 2106-02-06 UTC)",
  480                      diag_snippet(&span.name, DIAG_SNIPPET_MAX_BYTES),
  481                      span.start_time_unix_nano
  482                  ),
  483              );
  484              return Ok(());
  485          }
  486      };
```

**`:487-503`**

```rust
  487      for (scope, attrs) in [
  488          (SCOPE_RESOURCE, resource_attrs),
  489          (SCOPE_SPAN, span.attributes.as_slice()),
  490          (SCOPE_INSTRUMENTATION, scope_attrs),
  491      ] {
  492          for kv in attrs {
  493              out.attrs.push(attr_record(
  494                  kv,
  495                  scope,
  496                  date,
  497                  timestamp_ns,
  498                  trace_id,
  499                  span_id,
  500                  duration_ns,
  501              ));
  502          }
  503      }
```

**`:487-607`**  (121 lines in the range; comments and blanks elided, 30 shown)

```rust
  487      for (scope, attrs) in [
  488          (SCOPE_RESOURCE, resource_attrs),
  489          (SCOPE_SPAN, span.attributes.as_slice()),
  490          (SCOPE_INSTRUMENTATION, scope_attrs),
  491      ] {
  492          for kv in attrs {
  493              out.attrs.push(attr_record(
  494                  kv,
  495                  scope,
  496                  date,
  497                  timestamp_ns,
  498                  trace_id,
  499                  span_id,
  500                  duration_ns,
  501              ));
  502          }
  503      }
  512      for event in &span.events {
  515          out.attrs.push(AttrRecord {
  516              date,
  517              key: EVENT_INTRINSIC_NAME_KEY.to_string(),
  518              scope: SCOPE_EVENT_INTRINSIC.to_string(),
  519              val: event.name.clone(),
  521              val_type: AttrValueType::String,
  522              val_num: numeric_val_num(&event.name),
  523              timestamp_ns,
  524              trace_id,
  525              span_id,
  526              duration_ns,
  527          });
```

**`:505-556`**  (52 lines in the range; comments and blanks elided, 30 shown)

```rust
  512      for event in &span.events {
  515          out.attrs.push(AttrRecord {
  516              date,
  517              key: EVENT_INTRINSIC_NAME_KEY.to_string(),
  518              scope: SCOPE_EVENT_INTRINSIC.to_string(),
  519              val: event.name.clone(),
  521              val_type: AttrValueType::String,
  522              val_num: numeric_val_num(&event.name),
  523              timestamp_ns,
  524              trace_id,
  525              span_id,
  526              duration_ns,
  527          });
  530          let time_since_start =
  531              resolve_time_since_start_ns(span.start_time_unix_nano, event.time_unix_nano);
  532          out.attrs.push(AttrRecord {
  533              date,
  534              key: EVENT_INTRINSIC_TIME_SINCE_START_KEY.to_string(),
  535              scope: SCOPE_EVENT_INTRINSIC.to_string(),
  536              val: time_since_start.to_string(),
  538              val_type: AttrValueType::Int,
  539              val_num: Some(time_since_start as f64),
  540              timestamp_ns,
  541              trace_id,
  542              span_id,
  543              duration_ns,
  544          });
  545          for kv in &event.attributes {
  546              out.attrs.push(attr_record(
  547                  kv,
```

**`:505-607`**  (103 lines in the range; comments and blanks elided, 30 shown)

```rust
  512      for event in &span.events {
  515          out.attrs.push(AttrRecord {
  516              date,
  517              key: EVENT_INTRINSIC_NAME_KEY.to_string(),
  518              scope: SCOPE_EVENT_INTRINSIC.to_string(),
  519              val: event.name.clone(),
  521              val_type: AttrValueType::String,
  522              val_num: numeric_val_num(&event.name),
  523              timestamp_ns,
  524              trace_id,
  525              span_id,
  526              duration_ns,
  527          });
  530          let time_since_start =
  531              resolve_time_since_start_ns(span.start_time_unix_nano, event.time_unix_nano);
  532          out.attrs.push(AttrRecord {
  533              date,
  534              key: EVENT_INTRINSIC_TIME_SINCE_START_KEY.to_string(),
  535              scope: SCOPE_EVENT_INTRINSIC.to_string(),
  536              val: time_since_start.to_string(),
  538              val_type: AttrValueType::Int,
  539              val_num: Some(time_since_start as f64),
  540              timestamp_ns,
  541              trace_id,
  542              span_id,
  543              duration_ns,
  544          });
  545          for kv in &event.attributes {
  546              out.attrs.push(attr_record(
  547                  kv,
```

**`:558-607`**  (50 lines in the range; comments and blanks elided, 30 shown)

```rust
  565      for link in &span.links {
  568          out.attrs.push(AttrRecord {
  569              date,
  570              key: LINK_INTRINSIC_SPAN_ID_KEY.to_string(),
  571              scope: SCOPE_LINK_INTRINSIC.to_string(),
  572              val: hex_lower(&link.span_id),
  574              val_type: AttrValueType::String,
  575              val_num: None,
  576              timestamp_ns,
  577              trace_id,
  578              span_id,
  579              duration_ns,
  580          });
  583          out.attrs.push(AttrRecord {
  584              date,
  585              key: LINK_INTRINSIC_TRACE_ID_KEY.to_string(),
  586              scope: SCOPE_LINK_INTRINSIC.to_string(),
  587              val: hex_lower(&link.trace_id),
  589              val_type: AttrValueType::String,
  590              val_num: None,
  591              timestamp_ns,
  592              trace_id,
  593              span_id,
  594              duration_ns,
  595          });
  596          for kv in &link.attributes {
  597              out.attrs.push(attr_record(
  598                  kv,
  599                  SCOPE_LINK,
  600                  date,
```

**`:654-674`**

```rust
  654  /// The self-contained single-`ResourceSpans` `TracesData` payload for one
  655  /// span (the pinned T2/T3 contract — see the module doc): this span, its
  656  /// own resource + scope, and both original schema URLs, `prost`-encoded.
  657  fn build_payload(
  658      span: &Span,
  659      resource: Option<&Resource>,
  660      resource_spans: &ResourceSpans,
  661      scope_spans: &ScopeSpans,
  662  ) -> Vec<u8> {
  663      TracesData {
  664          resource_spans: vec![ResourceSpans {
  665              resource: resource.cloned(),
  666              scope_spans: vec![ScopeSpans {
  667                  scope: scope_spans.scope.clone(),
  668                  spans: vec![span.clone()],
  669                  schema_url: scope_spans.schema_url.clone(),
  670              }],
  671              schema_url: resource_spans.schema_url.clone(),
  672          }],
  673      }
  674      .encode_to_vec()
```

**`:657-657`**

```rust
  657  fn build_payload(
```

**`:712-714`**

```rust
  712  fn numeric_val_num(val: &str) -> Option<f64> {
  713      val.parse::<f64>().ok().filter(|n| n.is_finite())
  714  }
```

### `crates/pulsus-read/src/traces/filter.rs`

**`:89-103`**

```rust
   89  pub enum GenClass {
   90      /// Attr string/bool equality — `(key, val[, scope])` prefix.
   91      AttrEq = 0,
   92      /// `resource.service.name =` — `service_time` projection PREWHERE.
   93      ServiceEq = 1,
   94      /// Attr numeric / regex — key-only `(key)` prefix scan.
   95      AttrKeyScan = 2,
   96      /// `duration <op>` — `idx_duration` minmax within the projection.
   97      Duration = 3,
   98      /// `name`/`status`/`kind` predicates — bounded time-window span scan.
   99      SpanScan = 4,
  100      /// No positive leaf (negations / `{}` match-all) — the complete
  101      /// time-range superset, bounded by the scan budget.
  102      TimeRange = 5,
  103  }
```

### `docs/schemas.md`

**`:720-720`**  — this is one of the two citations round one corrected; the branch base's line holds different text, which is what made the correction necessary

```markdown
  720  | `name`/`status`/`kind` | `trace_spans` time-window scan + predicate | no selective index — window-bounded, budget-limited |
```

**`:897-897`**  — this is one of the two citations round one corrected; the branch base's line holds different text, which is what made the correction necessary

```markdown
  897  **Migration amendment policy:** the migration catalog (`pulsus-schema`'s `catalog.rs`, recorded per-id in `schema_migrations`) is append-only from the first tagged release onward. In-place amendment of an already-listed migration was permitted only pre-release (no tagged release, no persistent deployments, CI databases created fresh per run); the trace-index scope amendment (issue #54) was the last such amendment window. A local database created before a pre-release amendment must be dropped and re-reconciled — the per-id checksum drift guard refuses to touch the stale tables.
```

### `crates/pulsus-config/src/model.rs`

**`:514-514`**

```rust
  514              traceql_max_candidates: 100_000,
```

**`:514-516`**

```rust
  514              traceql_max_candidates: 100_000,
  515              traceql_scan_budget_rows: 50_000_000,
  516              traceql_tag_lookback: HumanDuration(Duration::from_secs(24 * 3_600)),
```

**`:515-515`**

```rust
  515              traceql_scan_budget_rows: 50_000_000,
```

**`:516-516`**

```rust
  516              traceql_tag_lookback: HumanDuration(Duration::from_secs(24 * 3_600)),
```

### `crates/pulsus-server/src/traces_api/tags.rs`

**`:161-164`**

```rust
  161  /// The window the values routes read over when the client supplies none.
  162  pub(super) fn tag_lookback_ns(state: &AppState) -> i64 {
  163      i64::try_from(state.config.reader.traceql_tag_lookback.0.as_nanos()).unwrap_or(i64::MAX)
  164  }
```

### `crates/pulsus-read/src/traces/tags_sql.rs`

**`:89-89`**

```rust
   89  pub fn tag_names_sql(scope_literal: Option<&str>, limit: usize) -> String {
```

**`:118-118`**

```rust
  118  pub fn tag_values_sql(key_literal: &str, scope_literal: Option<&str>, limit: usize) -> String {
```

**`:118-127`**

```rust
  118  pub fn tag_values_sql(key_literal: &str, scope_literal: Option<&str>, limit: usize) -> String {
  119      let mut sql =
  120          format!("SELECT DISTINCT val, val_type\nFROM {CATALOG_TABLE}\nWHERE key = {key_literal}");
  121      match scope_literal {
  122          Some(scope) => sql.push_str(&format!(" AND scope = {scope}")),
  123          None => sql.push_str(&format!(" AND scope IN {ATTR_SCOPES_IN}")),
  124      }
  125      sql.push_str(&format!("\nORDER BY val, val_type\nLIMIT {limit}"));
  126      sql
  127  }
```

**`:253-253`**

```rust
  253  pub fn span_name_values_sql(
```

**`:282-282`**

```rust
  282  pub fn attr_values_narrowed_sql(
```

**`:282-312`**  (31 lines in the range; comments and blanks elided, 30 shown)

```rust
  282  pub fn attr_values_narrowed_sql(
  283      ctx: SpanFilterCtx<'_>,
  284      key_literal: &str,
  285      scope_literal: Option<&str>,
  286      days: DaySpan,
  287      terms: &[NarrowTerm],
  288      limit: usize,
  289  ) -> String {
  290      let mut sql = format!(
  291          "SELECT DISTINCT val, val_type\nFROM {}\nWHERE key = {key_literal}",
  292          ctx.attrs_table
  293      );
  294      match scope_literal {
  295          Some(scope) => sql.push_str(&format!(" AND scope = {scope}")),
  296          None => sql.push_str(&format!(" AND scope IN {ATTR_SCOPES_IN}")),
  297      }
  298      sql.push_str(&format!("\n  AND {}", attrs_day_clause(days)));
  299      let mut spans = format!(
  300          "SELECT trace_id, span_id\n    FROM {}\n    WHERE {}",
  301          ctx.spans_table,
  302          spans_day_clause(days)
  303      );
  304      for clause in term_clauses(ctx, terms, days) {
  305          spans.push_str(&format!("\n      AND {clause}"));
  306      }
  307      sql.push_str(&format!(
  308          "\n  AND (trace_id, span_id) IN (\n    {spans}\n  )"
  309      ));
  310      sql.push_str(&format!("\nORDER BY val, val_type\nLIMIT {limit}"));
  311      sql
```

### `crates/pulsus-read/src/traces/exec.rs`

**`:117-117`**

```rust
  117  pub const BATCH_TRACES: usize = 32;
```

**`:122-122`**

```rust
  122  pub const MAX_SPANS_PER_TRACE: usize = 10_000;
```

**`:130-130`**

```rust
  130  pub const TAG_NAMES_MAX: usize = 10_000;
```

**`:135-135`**

```rust
  135  pub const TAG_VALUES_MAX: usize = 1_000;
```

**`:1825-1866`**  (42 lines in the range; comments and blanks elided, 30 shown)

```rust
 1825      pub async fn list_tag_values(
 1826          &self,
 1827          key: &str,
 1828          scope: Option<&str>,
 1829          req: TagValuesRequest<'_>,
 1830      ) -> Result<TagValues, ReadError> {
 1831          let key_literal = crate::logql::escape::ch_string(key);
 1832          let scope_literal = scope.map(crate::logql::escape::ch_string);
 1833          let narrowing = req.narrowing();
 1834          let (sql, settings) = if narrowing.is_empty() {
 1835              (
 1836                  super::tags_sql::tag_values_sql(
 1837                      &key_literal,
 1838                      scope_literal.as_deref(),
 1839                      TAG_VALUES_MAX + 1,
 1840                  ),
 1841                  catalog_settings(&self.config),
 1842              )
 1843          } else {
 1854              (
 1855                  super::tags_sql::attr_values_narrowed_sql(
 1856                      self.span_filter_ctx(),
 1857                      &key_literal,
 1858                      scope_literal.as_deref(),
 1859                      req.days(),
 1860                      narrowing.terms(),
 1861                      TAG_VALUES_MAX + 1,
 1862                  ),
 1863                  metrics_settings(&self.config),
 1864              )
```

**`:2163-2191`**  (29 lines in the range; comments and blanks elided, 14 shown)

```rust
 2172              let rows: Vec<CandidateRow> = match attempt {
 2173                  Ok(rows) => rows,
 2174                  Err(ReadError::QueryTooBroad(TooBroadReason::TraceGeneratorMemory { .. }))
 2175                      if gen_idx == 0 && plan.generator_fallback_sql().is_some() =>
 2176                  {
 2177                      let fallback = plan
 2178                          .generator_fallback_sql()
 2179                          .expect("checked by the guard above");
 2186                      budget.release(phase1_charged - before_attempt);
 2187                      phase1_charged = before_attempt;
 2188                      charge_explain(
 2189                          &mut explain,
 2190                          &mut budget,
 2191                          "phase1_candidate_generator_fallback",
```

### `crates/pulsus-write/src/writer/trace.rs`

**`:9-19`**

```rust
    9  //! **Consistency model**: `trace_spans` and `trace_attrs_idx` flush
   10  //! independently on two separate generations — no cross-table atomic
   11  //! insert (the same eventual-consistency model `LogWriter`/`MetricWriter`'s
   12  //! module docs accept). The `join_generations` wait guarantees a sync
   13  //! caller never receives a false success: `admit_flush`'s `200` resolves
   14  //! only once this admission's spans *and* attrs generations are both
   15  //! durable, or it gets an `Err`. A concurrent reader can still observe a
   16  //! span durable without its attr rows during the settle window — legal;
   17  //! the TraceQL read path (T4+) intersects the index against the payload
   18  //! table and tolerates a lagging index row exactly as the log path
   19  //! tolerates a lagging stream registration.
```

**`:137-185`**  (49 lines in the range; comments and blanks elided, 30 shown)

```rust
  146          let attrs_backlog = Arc::new(Mutex::new(RegistrationBacklog::<TraceAttrRow>::new(
  147              runtime.backfill_max_bytes,
  148          )));
  149          let attrs_backlog_for_hook = attrs_backlog.clone();
  150          let attrs_backfill_metrics = metrics.attrs_backfill.clone();
  151          let on_attrs_flush_poisoned: table::FlushPoisonedHook<TraceAttrRow> =
  152              Arc::new(move |rows: &[TraceAttrRow]| {
  153                  backfill::enqueue_failed(&attrs_backlog_for_hook, &attrs_backfill_metrics, rows);
  154              });
  155          let attrs_inserter_for_backfill = attrs_inserter.clone();
  156          let attrs_table_for_backfill = tables.attrs.clone();
  162          let spans_ctx = TableContext {
  163              table: tables.spans,
  164              buffer: spans.clone(),
  165              notify: spans_notify.clone(),
  166              inserter: spans_inserter,
  167              runtime: runtime.clone(),
  168              table_metrics: metrics.spans.clone(),
  169              spool: spool.clone(),
  170              queued_bytes: queued_bytes.clone(),
  171              on_flush_success: None,
  172              on_flush_poisoned: None,
  173          };
  174          let attrs_ctx = TableContext {
  175              table: tables.attrs,
  176              buffer: attrs.clone(),
  177              notify: attrs_notify.clone(),
  178              inserter: attrs_inserter,
  179              runtime: runtime.clone(),
  180              table_metrics: metrics.attrs.clone(),
```

**`:172-172`**

```rust
  172              on_flush_poisoned: None,
```

**`:220-304`**  (85 lines in the range; comments and blanks elided, 30 shown)

```rust
  220      fn admit_batch(
  221          &self,
  222          batch: ParsedTraces,
  223          with_waiters: bool,
  224      ) -> Result<Vec<oneshot::Receiver<Result<(), WriteError>>>, Backpressure> {
  225          if self.shared.shutting_down.load(Ordering::Acquire) {
  226              return Err(Backpressure);
  227          }
  229          self.shared
  230              .metrics
  231              .rejected_total
  232              .fetch_add(batch.rejected, Ordering::Relaxed);
  237          let span_bytes: u64 = batch.spans.iter().map(TraceSpanRow::est_source_bytes).sum();
  238          let attr_bytes: u64 = batch.attrs.iter().map(TraceAttrRow::est_source_bytes).sum();
  239          let total_bytes = span_bytes + attr_bytes;
  243          super::reserve_queued_bytes(
  244              &self.shared.queued_bytes,
  245              &self.shared.metrics.backpressure_total,
  246              total_bytes,
  247              self.shared.runtime.queue_bytes_limit,
  248          )?;
  250          if self.shared.shutting_down.load(Ordering::Acquire) {
  251              self.shared
  252                  .queued_bytes
  253                  .fetch_sub(total_bytes, Ordering::AcqRel);
  254              return Err(Backpressure);
  255          }
  258          let span_rows: Vec<TraceSpanRow> = batch.spans.iter().map(TraceSpanRow::from).collect();
  259          let attr_rows: Vec<TraceAttrRow> = batch.attrs.iter().map(TraceAttrRow::from).collect();
  261          let mut receivers = Vec::new();
```

### `crates/pulsus-write/src/writer/backfill.rs`

**`:23-28`**

```rust
   23  //! The append-only tables (`log_samples`, `metric_samples`,
   24  //! `metric_hist_samples`, `trace_spans`) are structurally excluded: their
   25  //! `TableContext`s pass `on_flush_poisoned: None`, and each backlog is
   26  //! typed to exactly one registration row shape bound to one table name —
   27  //! cross-table replay is unrepresentable (#9 applies in full to every
   28  //! sample/span/rollup target).
```

**`:78-90`**

```rust
   78  /// Bounded, keyed backlog of Poisoned-flush registration rows awaiting
   79  /// re-insert. Keyed dedup on [`BackfillRow::backfill_key`]: an existing
   80  /// key is replaced iff the incoming version is larger; a new key that
   81  /// would exceed `max_bytes` is rejected and counted dropped.
   82  pub(crate) struct RegistrationBacklog<R: BackfillRow> {
   83      entries: HashMap<R::Key, R>,
   84      /// Sum of `backfill_bytes` over `entries`.
   85      bytes: u64,
   86      max_bytes: u64,
   87  }
   88  
   89  impl<R: BackfillRow> RegistrationBacklog<R> {
   90      pub(crate) fn new(max_bytes: u64) -> Self {
```

**`:189-201`**

```rust
  189  pub(crate) fn enqueue_failed<R: BackfillRow>(
  190      backlog: &Mutex<RegistrationBacklog<R>>,
  191      metrics: &BackfillMetrics,
  192      rows: &[R],
  193  ) {
  194      let mut guard = backlog.lock().expect("registration backlog mutex poisoned");
  195      let (accepted, dropped) = guard.enqueue(rows);
  196      metrics
  197          .enqueued_total
  198          .fetch_add(accepted, Ordering::Relaxed);
  199      metrics.dropped_total.fetch_add(dropped, Ordering::Relaxed);
  200      metrics.pending.store(guard.len() as u64, Ordering::Relaxed);
  201  }
```

**`:214-220`**

```rust
  214  /// - `InsertUncertain` → **terminal**: version-checked remove,
  215  ///   `abandoned_total += removed`, warn-log — commit fate unknown, never
  216  ///   retried (#9 discipline);
  217  /// - any other (deterministic) error → version-checked remove,
  218  ///   `abandoned_total += removed` (no poison spin; a poison-spool record
  219  ///   of the abandoned rows exists iff the generation's spool write
  220  ///   succeeded — residual R5 otherwise).
```

### `crates/pulsus-write/src/writer/table.rs`

**`:313-321`**

```rust
  313  
  314  /// Inserts `rows`, retrying only *pre-send* retryable failures
  315  /// (`ChError::is_retryable`) with exponential backoff and full jitter, up
  316  /// to `runtime.retry_max_attempts`. `insert_block` downgrades every
  317  /// *post-send* retryable failure to `ChError::InsertUncertain` before it
  318  /// ever reaches this function (see `pulsus_clickhouse::ChClient::
  319  /// insert_block`'s doc comment) — that path is classified but never
  320  /// retried, per the one hard invariant this crate enforces
  321  /// (docs/schemas.md §2.2/§8: replaying a partially-committed block
```

**`:367-434`**  (68 lines in the range; comments and blanks elided, 30 shown)

```rust
  367  async fn finish_generation<R>(
  368      ctx: &TableContext<R>,
  369      generation: Generation<R>,
  370      outcome: FlushOutcome,
  371      started: Instant,
  372  ) where
  373      R: ChRow + SpoolEncode + Send + Sync,
  374  {
  375      match outcome {
  376          FlushOutcome::Ok => {
  377              if let Some(hook) = &ctx.on_flush_success {
  378                  hook(&generation.rows);
  379              }
  380              ctx.table_metrics.record_flush(
  381                  generation.rows.len() as u64,
  382                  generation.bytes,
  383                  started.elapsed(),
  384              );
  385              ctx.queued_bytes
  386                  .fetch_sub(generation.bytes, Ordering::AcqRel);
  387              generation.settle(Ok(()));
  388          }
  389          FlushOutcome::Uncertain(msg) => {
  390              if let Err(spool_err) = ctx
  391                  .spool
  392                  .write(SpoolKind::Uncertain, &ctx.table, &generation.rows, &msg)
  393                  .await
  394              {
  395                  ctx.table_metrics
  396                      .spool_write_failures_total
```

### `crates/pulsus-read/src/traces/sql.rs`

**`:16-26`**

```rust
   16  pub fn point_read_sql(spans_table: &str, hex32: &str) -> String {
   17      debug_assert!(
   18          hex32.len() == 32 && hex32.bytes().all(|b| b.is_ascii_hexdigit()),
   19          "hex32 must be caller-validated 32-char hex, got {hex32:?}"
   20      );
   21      format!(
   22          "SELECT trace_id, span_id, parent_id, payload_type, kind, payload\n\
   23           FROM {spans_table}\n\
   24           WHERE trace_id = unhex('{hex32}')"
   25      )
   26  }
```

### `crates/pulsus-read/src/traces/metrics_sql.rs`

**`:9-12`**

```rust
    9  //! Counting is always `uniqExact(trace_id, span_id)` (plan v2 delta 1:
   10  //! at-least-once replays must never inflate a bucket — this is exactly
   11  //! T5's `(trace_id, span_id)` logical-span identity, carried flat here
   12  //! because `span_id` is trace-local).
```

### `crates/pulsus-read/tests/golden/traces_search/existence_absent.sql`

**`:5-10`**

```sql
    5  SELECT trace_id, max(timestamp_ns) AS bound_ts
    6  FROM trace_spans
    7  WHERE timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000
    8  GROUP BY trace_id
    9  ORDER BY bound_ts DESC, trace_id ASC
   10  LIMIT 100001
```

### `crates/pulsus-read/tests/golden/traces_search/worked_example.sql`

**`:5-11`**

```sql
    5  SELECT trace_id, max(timestamp_ns) AS bound_ts
    6  FROM trace_spans
    7  PREWHERE service = 'checkout'
    8  WHERE timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000
    9  GROUP BY trace_id
   10  ORDER BY bound_ts DESC, trace_id ASC
   11  LIMIT 100001
```

### `crates/pulsus-read/src/traces/search_sql.rs`

**`:184-184`**

```rust
  184  pub fn generator_sql(
```

**`:230-230`**

```rust
  230  pub fn hydration_sql(
```

**`:230-252`**

```rust
  230  pub fn hydration_sql(
  231      spans_table: &str,
  232      trace_ids: &[[u8; 16]],
  233      window: TimeWindow,
  234      max_spans_per_trace: usize,
  235  ) -> String {
  236      format!(
  237          "SELECT trace_id, span_id, parent_id, {}, {}, timestamp_ns, duration_ns, \
  238           status_code, {}, kind, {}, {}\n\
  239           FROM {spans_table}\n\
  240           WHERE {}\n  AND {}\n\
  241           ORDER BY trace_id ASC, timestamp_ns ASC, span_id ASC\n\
  242           LIMIT {} BY trace_id",
  243          byte_capped("service"),
  244          byte_capped("name"),
  245          byte_capped("status_message"),
  246          byte_capped("scope_name"),
  247          byte_capped("scope_version"),
  248          trace_id_in(trace_ids),
  249          time_clause(window),
  250          max_spans_per_trace + 1
  251      )
  252  }
```

**`:286-286`**

```rust
  286  pub fn membership_sql(
```

**`:286-312`**  (27 lines in the range; comments and blanks elided, 24 shown)

```rust
  286  pub fn membership_sql(
  287      attrs_table: &str,
  288      predicate: &str,
  289      trace_ids: &[[u8; 16]],
  290      window: TimeWindow,
  291      with_value: bool,
  292  ) -> String {
  293      let projection = if with_value {
  294          format!(
  295              "trace_id, span_id, {} AS v, val_type AS t",
  296              byte_cap_expr("val")
  297          )
  298      } else {
  299          "trace_id, span_id".to_string()
  300      };
  301      format!(
  302          "SELECT DISTINCT {projection}\n\
  303           FROM {attrs_table}\n\
  304           WHERE {}\n  AND ({predicate})\n  AND {}\n  AND {}",
  305          date_clause(window),
  306          time_clause(window),
  307          trace_id_in(trace_ids)
  308      )
  309  }
```

**`:325-325`**

```rust
  325  pub fn attr_values_sql(
```

**`:366-380`**

```rust
  366  ///
  367  /// **NO aggregate — deliberately, and this is the memory contract, not a
  368  /// style choice.** The first cut of this read used
  369  /// `groupUniqArray(...) GROUP BY trace_id, span_id`, and it broke the
  370  /// Layer-1 residual bound this module's own contract states: "at most
  371  /// `TRACE_SEARCH_MAX_BLOCK_ROWS` rows × (fixed-width columns + string
  372  /// columns each capped at [`TRACE_STR_COL_CAP`] bytes at the source) —
  373  /// never a-priori row-unbounded" (`traces::exec` module doc,
  374  /// docs/schemas.md §7). An ARRAY column is an unbounded number of capped
  375  /// strings in ONE row, so a single span with enough distinct event names
  376  /// made both the server-side aggregate state and the client's decoded row
  377  /// grow without any of that bound applying — and phase-2 reads carry no
  378  /// `max_memory_usage` (only phase-1 generators do), so a server-side
  379  /// blow-up would have surfaced as a 500 rather than the required 422.
  380  ///
```

**`:397-397`**

```rust
  397  pub fn event_set_sql(
```

**`:428-428`**

```rust
  428  pub fn root_sql(spans_table: &str, trace_ids: &[[u8; 16]]) -> String {
```

**`:468-468`**

```rust
  468  pub fn trace_ctx_sql(spans_table: &str, trace_ids: &[[u8; 16]]) -> String {
```

**`:492-492`**

```rust
  492  pub fn child_count_sql(spans_table: &str, trace_ids: &[[u8; 16]]) -> String {
```

### `crates/pulsus-read/src/traces/search_plan.rs`

**`:661-661`**

```rust
  661      pub(crate) probe_predicates: Vec<String>,
```

**`:888-896`**

```rust
  888      pub fn membership_sql_for(&self, probe_idx: usize, trace_ids: &[[u8; 16]]) -> String {
  889          search_sql::membership_sql(
  890              &self.attrs_table,
  891              &self.probe_predicates[probe_idx],
  892              trace_ids,
  893              self.window,
  894              self.probe_values[probe_idx],
  895          )
  896      }
```

**`:1077-1077`**

```rust
 1077  fn membership_predicate(probe: &AttrProbe) -> Result<String, PlanError> {
```

**`:2308-2352`**  (45 lines in the range; comments and blanks elided, 30 shown)

```rust
 2308  fn projection_value(
 2309      leaf: &PlannedLeafEval,
 2310      probes: &[AttrProbe],
 2311      probe_values: &mut [bool],
 2312  ) -> Option<ProjectionValue> {
 2313      match leaf {
 2314          PlannedLeafEval::Physical(p) => match p {
 2315              PhysicalEval::Name { .. } => Some(ProjectionValue::Name),
 2316              PhysicalEval::Service { .. } => Some(ProjectionValue::Service),
 2317              PhysicalEval::Status { .. } => Some(ProjectionValue::Status),
 2318              PhysicalEval::Kind { .. } => Some(ProjectionValue::Kind),
 2319              PhysicalEval::StatusMessage { .. } => Some(ProjectionValue::StatusMessage),
 2320              PhysicalEval::ParentIdHex { .. } => Some(ProjectionValue::ParentIdHex),
 2321              PhysicalEval::InstrumentationName { .. } => Some(ProjectionValue::ScopeName),
 2322              PhysicalEval::InstrumentationVersion { .. } => Some(ProjectionValue::ScopeVersion),
 2326              PhysicalEval::Duration { .. } | PhysicalEval::SpanIdHex { .. } => None,
 2327          },
 2330          PlannedLeafEval::Attr { negated: true, .. } => None,
 2331          PlannedLeafEval::Attr {
 2332              probe_idx,
 2333              negated: false,
 2334          } => match &probes[*probe_idx].pred {
 2337              ValuePred::StringEq(v) => Some(ProjectionValue::ProbeLiteral {
 2338                  text: v.clone(),
 2339                  literal_type: StoredType::String,
 2340              }),
 2341              ValuePred::BoolEq(b) => Some(ProjectionValue::ProbeLiteral {
 2342                  text: b.to_string(),
 2343                  literal_type: StoredType::Bool,
 2344              }),
```

**`:3072-3092`**

```rust
 3072      let pushed_having: Option<String> = match lowering.rel.having.as_slice() {
 3073          [frag] if generators.len() == 1 => Some(frag.clone()),
 3074          _ => None,
 3075      };
 3076      // Issue #492 part 5: the fallback is captured BEFORE the re-render,
 3077      // so it is the statement this query sends with nothing pushed, byte
 3078      // for byte. The executor runs it when the pushed statement raises
 3079      // ClickHouse code 241 — the only thing `generator_settings`'
 3080      // `max_memory_usage` raises — so a query that answers `200` without
 3081      // the pushdown still answers `200` with it.
 3082      let generator_fallback_sql = pushed_having.as_ref().map(|_| generator_sqls[0].clone());
 3083      if let Some(frag) = &pushed_having {
 3084          generator_sqls[0] = search_sql::generator_sql(
 3085              &generators[0].1,
 3086              window,
 3087              ctx.filter.spans_table,
 3088              ctx.filter.attrs_table,
 3089              ctx.max_candidates,
 3090              Some(frag),
 3091          );
 3092      }
```

### `crates/pulsus-read/src/traces/search_eval.rs`

**`:1213-1216`**

```rust
 1213          PlannedLeafEval::Attr { probe_idx, negated } => {
 1214              let member =
 1215                  env.attrs.membership[*probe_idx].contains(&(env.ctx.trace_id, span.span_id));
 1216              member != *negated
```

**`:3656-3656`**

```rust
 3656      fn dual_scope_membership_satisfies_an_unscoped_negation_correctly() {
```

### `crates/pulsus-read/tests/golden/traces_search/val_num_range.sql`

**`:5-12`**

```sql
    5  SELECT trace_id, max(timestamp_ns) AS bound_ts
    6  FROM trace_attrs_idx
    7  WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15')
    8    AND timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000
    9    AND (key = 'http.status_code' AND val_num >= 500 AND scope = 'span')
   10  GROUP BY trace_id
   11  ORDER BY bound_ts DESC, trace_id ASC
   12  LIMIT 100001
```

### `crates/pulsus-read/tests/golden/traces_search/status_only.sql`

**`:5-11`**

```sql
    5  SELECT trace_id, max(timestamp_ns) AS bound_ts
    6  FROM trace_spans
    7  WHERE timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000
    8    AND (status_code = 2)
    9  GROUP BY trace_id
   10  ORDER BY bound_ts DESC, trace_id ASC
   11  LIMIT 100001
```

### `crates/pulsus-read/tests/golden/traces_metrics/rate_by_service.sql`

**`:5-11`**

```sql
    5  SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns - 1), INTERVAL 60000000000 NANOSECOND)) + 60000 AS t, service AS g0,
    6         uniqExact(trace_id, span_id) AS n
    7  FROM trace_spans
    8  WHERE timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
    9    AND duration_ns > 1000000000
   10  GROUP BY t, g0
   11  ORDER BY t ASC, g0
```

### `crates/pulsus-read/src/traces/compile.rs`

**`:431-462`**  (32 lines in the range; comments and blanks elided, 26 shown)

```rust
  431  pub fn aggregate_having_sql(stage: &PipelineStage, group_key: Option<&str>) -> Option<String> {
  432      let PipelineStage::Aggregate {
  433          op,
  434          field,
  435          cmp,
  436          value,
  437      } = stage
  438      else {
  439          return None;
  440      };
  447      let is_duration = matches!(
  448          field,
  449          Some(FieldExpr::Field(Field::Intrinsic(Intrinsic::Duration)))
  450      );
  451      let (scalar, map_agg, arg, wrapper) = match (op, cmp) {
  452          (AggregateOp::Count, ComparisonOp::Gt | ComparisonOp::Gte) if field.is_none() => {
  453              ("uniqExact(span_id)", "uniqExactMap", "span_id", "arrayMax")
  454          }
  455          (AggregateOp::Max, ComparisonOp::Gt | ComparisonOp::Gte) if is_duration => {
  456              ("max(duration_ns)", "maxMap", "duration_ns", "arrayMax")
  457          }
  458          (AggregateOp::Min, ComparisonOp::Lt | ComparisonOp::Lte) if is_duration => {
  459              ("min(duration_ns)", "minMap", "duration_ns", "arrayMin")
  460          }
  461          _ => return None,
  462      };
```

**`:451-462`**

```rust
  451      let (scalar, map_agg, arg, wrapper) = match (op, cmp) {
  452          (AggregateOp::Count, ComparisonOp::Gt | ComparisonOp::Gte) if field.is_none() => {
  453              ("uniqExact(span_id)", "uniqExactMap", "span_id", "arrayMax")
  454          }
  455          (AggregateOp::Max, ComparisonOp::Gt | ComparisonOp::Gte) if is_duration => {
  456              ("max(duration_ns)", "maxMap", "duration_ns", "arrayMax")
  457          }
  458          (AggregateOp::Min, ComparisonOp::Lt | ComparisonOp::Lte) if is_duration => {
  459              ("min(duration_ns)", "minMap", "duration_ns", "arrayMin")
  460          }
  461          _ => return None,
  462      };
```

**`:560-562`**

```rust
  560  pub(crate) fn group_key_sql(field: &Field, source: SourceRef) -> Option<String> {
  561      if source != TRACE_SPANS {
  562          return None;
```

### `crates/pulsus-clickhouse/src/client.rs`

**`:137-137`**

```rust
  137              .set("async_insert", 0)
```

### `crates/pulsus-write/src/writer/rows.rs`

**`:437-437`**

```rust
  437      pub pos_span_offsets: Vec<i32>,
```

**`:443-443`**

```rust
  443      pub custom_values: Vec<f64>,
```

### `docs/architecture.md`

**`:96-96`**

```markdown
   96  All DDL is owned by `pulsus-schema` as templated SQL (parameters: database, cluster clause, engine family, storage policy). Migrations are idempotent, and append-only from the first tagged release onward — in-place amendment of an already-listed migration was permitted only pre-release (the trace-index scope amendment, issue #54, was the last such window; see schemas.md §6); a `schema_migrations` bookkeeping table records applied statements. When `PULSUS_CLUSTER` is set, `MergeTree` families swap to `ReplicatedMergeTree` equivalents and `*_dist` Distributed tables are created. Raw sample/span tables partition **daily** (short TTL, whole-part drops); series/index/tier tables partition **monthly**.
```

### `crates/pulsus-write/tests/trace_ingest_roundtrip.rs`

**`:281-281`**

```rust
  281      for row in &rows {
```

### `vendor/clickhouse/src/rowbinary/ser.rs`

**`:129-129`**

```rust
  129      fn serialize_str(self, v: &str) -> Result<()> {
```

**`:137-137`**

```rust
  137      fn serialize_bytes(self, v: &[u8]) -> Result<()> {
```

**`:146-146`**

```rust
  146      fn serialize_none(self) -> Result<()> {
```

**`:222-222`**

```rust
  222      fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
```

### `crates/pulsus-clickhouse/src/pool.rs`

**`:695-695`**

```rust
  695          clickhouse::Client::default()
```

### `vendor/clickhouse/Cargo.toml`

**`:49-49`**

```toml
   49  default = ["lz4"]
```

### `vendor/clickhouse/src/query.rs`

**`:221-231`**

```rust
  221          if self.client.compression.is_enabled() {
  222              #[cfg(feature = "zstd")]
  223              if matches!(self.client.compression, crate::Compression::Zstd(_)) {
  224                  pairs.append_pair(settings::ENABLE_HTTP_COMPRESSION, "1");
  225              } else {
  226                  pairs.append_pair(settings::COMPRESS, "1");
  227              }
  228  
  229              #[cfg(not(feature = "zstd"))]
  230              pairs.append_pair(settings::COMPRESS, "1");
  231          }
```

### `crates/pulsus-schema/src/render.rs`

**`:55-57`**

```rust
   55              Family::Metrics => "cityHash64(metric_name, fingerprint)",
   56              Family::Logs => "fingerprint",
   57              Family::Traces => "cityHash64(trace_id)",
```
