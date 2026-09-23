# TraceQL storage: the tables, the SQL, and what each costs

Companion to `docs/TraceQL/functional-requirements.md` (requirements, data,
benchmark, test cases) and `docs/TraceQL/server-implementation.md` (write path
and compiler). Every number was produced by a script in `docs/TraceQL/measure/`
on ClickHouse 26.3.29.7 limited to 4 CPUs and 6 GB, over corpus g1 — 2,000,064
spans, 70,413 traces, three hours, 24 services — in the run of 2026-09-23 that
wrote `measure/results/`. Revisions of this document made after that run compare
its counted claims against the tree and against `results/`; they do not
re-measure the corpus, and `measure/README.md` says what that separation is. Where the reference is quoted it
is the pinned build `grafana/tempo:3.0.2@sha256:cda87c21…`
(`deploy/e2e/compose.single.yaml:158`) on the same machine, the same corpus and
the same 4 CPUs and 6 GB.

Storage is **single-tier**: local disks, replicated the way ClickHouse
replicates anything. The object-storage requirements were withdrawn by the owner
on 2026-09-22 (`functional-requirements.md` §2), so there is no hot/cold split
and no offload path here.

---

## 1. Five tables

```
     spans        one row per span, everything the span carries
       key: (5-min bucket, trace_id, start_ns, span_id, kind)   granule 2048
       partition: the UTC day of start_ns
       attrs: a JSON column - one stored subcolumn per attribute key, typed

     resources    one row per DISTINCT resource per day; spans carry a 128-bit id
       key: (service, resource_id)      service.name is NOT repeated inside attrs

     traces       one row per (day, trace): extent, root, services   granule 1024
       key: trace_id

     tag_names    one row per distinct (scope, key)          48 rows for g1
     tag_values   one row per distinct (scope, key, value, type)   304,070 rows
       both time-less and without a TTL, which is what docs/api.md 4.3 requires
```

No attribute index, no payload blob, no edge ledger, no recency table, no error
table, no re-sorted projections.

The DDL is `measure/schema.sql`, which is what every measurement ran against;
the staging table it builds from is `measure/staging.sql`, loaded by
`measure/load_staging.sh`. Clustered, each table becomes its `Replicated*`
engine with the macros the schema controller already renders, sharded by
`cityHash64(trace_id)` so a trace is whole on one shard.

### 1.1 Why each part of the sort key is there

```
   intDiv(start_ns, 300000000000)   every read is time-bounded; 5 minutes is
          the time bucket           fine enough that an hour window over-reads
                                    at most 5 minutes at each end, and a trace
                                    stays inside one bucket (measured: 1.0003
                                    buckets per trace)
   trace_id                         a trace's spans are contiguous, so a fetch,
                                    a hydration and a per-trace evaluation are
                                    key reads rather than scans
   start_ns, span_id, kind          a stable order inside a trace, and the rest
                                    of the ReplacingMergeTree key: a retried
                                    span collapses, a Zipkin shared span (one
                                    id, two kinds) does not
```

`service` is deliberately **not** in the key. It was measured there — it prunes
a service-scoped read to that service's granules — and §5.8 gives both sides
with numbers; the trace-centric reads won, because their penalty grows with the
number of services in the deployment while the service prune's benefit does not
extend to anything else.

## 2. What it costs to store

Measured after one merge per table (`system.parts`, `system.columns`):

| table | rows | bytes on disk | bytes per span |
|---|---:|---:|---:|
| `spans` | 2,000,064 | 69,932,464 | 34.965 |
| `traces` | 70,413 | 2,919,750 | 1.460 |
| `tag_values` | 304,070 | 1,691,983 | 0.846 |
| `resources` | 68 | 6,412 | 0.003 |
| `tag_names` | 48 | 1,190 | 0.001 |
| **total** | 2,374,663 | **74,551,799** | **37.275** |

Non-span tables are **6.605%** of the span table. Against today's six tables on
the same bodies: 1,508,935,285 bytes, 754.44 per span (753.79 after a full
merge). Against the reference's three active blocks: 209,324,594 bytes, 104.66
per span.

The span table, column by column (compressed bytes per span):

| column | B/span | column | B/span |
|---|---:|---|---:|
| `attrs` (5.45 values per span) | 10.870 | `name` | 0.937 |
| `span_id` | 8.004 | `events` | 0.669 |
| `parent_span_id` | 4.015 | `trace_id` | 0.596 |
| `start_ns` | 3.674 | `service` | 0.430 |
| `duration_ns` | 3.595 | `kind` | 0.252 |
| `resource_id` | 1.495 | everything else | < 0.2 |

`span_id` is 8 random bytes and does not compress; it is the largest irreducible
cost, which is the sign the rest is near the floor. The table compresses
**6.977×** (244.0 B/span uncompressed → 34.97); today's tables 4.53×.

### 2.1 The model, so a reader can substitute their own workload

Let `A` be span attributes per span, `R` resource attributes per resource, `k`
spans per trace, `d` distinct resources per day, `V` distinct attribute values
in retention.

| dimension | expression | g1 |
|---|---|---|
| span row, fixed part | ids, times, name, kind, status, resource id ≈ 23 B/span | 23.2 |
| span attributes | ≈ `A` × 1.99 B — one typed subcolumn per key over a sorted run | 10.87 |
| events and links | their own attributes at the same rate | 0.71 |
| resources | 1.49 B/span for the id, plus `d` × `R` per day — **not** × spans | 1.49 + 0.003 |
| per-trace index | ≈ 41.5 B per trace ⇒ 41.5/`k` per span | 1.46 |
| tag catalogs | ≈ `V` × 5.6 B, and one row per distinct key | 0.85 |
| **total** | ≈ 23.2 + 1.99·`A` + 41.5/`k` + 5.6·`V`/spans | **37.275** |

`A` moves the total; `V` is the one term that grows with cardinality rather than
volume, and it is the price of the time-less value catalog `docs/api.md` §4.3
requires. Today's layout charges the same attribute about 24 B/span/value
(493 B/span for 20.4 values), because it stores it three times and carries
`trace_id`, `span_id`, `timestamp_ns` and `duration_ns` beside each copy.

**Where the value catalog is not bounded.** `tag_values` grows with distinct
values, exactly as today's `trace_tag_catalog` does, and neither has a
cardinality bound — a key whose values are unique per span grows it linearly.
That is today's shipped behaviour and today's shipped risk; the design does not
change it, and the case is listed as future work rather than claimed solved.

## 3. How an attribute is stored

### 3.1 One stored subcolumn per key, typed

`attrs` is ClickHouse's `JSON` type: each key becomes its own subcolumn and each
value keeps its OTLP type (`Int64`, `Float64`, `Bool`, `String`, `Array(...)`).
A filter on one key reads that key's subcolumn and nothing else.

A key is written with `.` escaped, because ClickHouse reads a dot as nesting:

```
   OTLP key                   stored path            read as
   http.response.status_code  http%2Eresponse%2E…    attrs.`http%2Eresponse%2Estatus_code`
   a.b   (a dotted key)       a%2Eb                  distinct from the nested object {"a":{"b":…}}
   a%2Eb (a literal percent)  a%252Eb                distinct from a.b
```

ClickHouse's own escape (`json_type_escape_dots_in_keys = 1`) is **not**
injective: `a%2Eb` and `a.b` both become `a%2Eb`, and the second insert fails
with `Duplicate path found during parsing JSON object`. The write path therefore
escapes `%` as `%25` first; the compiler renders the same two substitutions, and
the reader reverses them (`tqd_unescape` in `measure/schema.sql`).

### 3.2 Typed comparison, and the rule that nearly went wrong

```sql
-- { span.http.response.status_code >= 500 }
coalesce(attrs.`http%2Eresponse%2Estatus_code`.:Int64   >= 500, false)
OR coalesce(attrs.`http%2Eresponse%2Estatus_code`.:Float64 >= 500, false)
```

**`coalesce` is load-bearing.** A missing variant reads NULL and `NOT NULL` is
NULL, so the obvious rendering of `!=` — `NOT (x = 200 OR y = 200)` — returned
**0 rows** where the correct answer is 1,737,881. Every compiled comparison
wraps each typed read in `coalesce(…, false)` and expresses negation over the
result; `T-A1` pins it.

### 3.3 What JSON cannot hold, and where it goes instead

Measured against 26.3.29.7 by inserting each case:

| value | text JSON | this design |
|---|---|---|
| duplicate key in one span | insert **fails** | the write path keeps the first value, which is the scope-precedence rule already documented |
| `NaN`, `±Inf`, `1e400` | insert **fails** | written through RowBinary's **binary** JSON encoding, which carries a typed `Float64`; verified: a stored `+Inf` answers `> 500` with 1 |
| empty object | the key disappears | goes to `attrs_other` |
| bytes value, an array with no JSON rendering | not representable | `attrs_other`, a string holding the OTLP `AnyValue` for those keys only |
| nested object | becomes real nested paths | kept, and distinct from the escaped dotted key |

`attrs_other` is empty for every span in g1 and costs 676 bytes in total.

## 4. Reads never double-count a retried span

The span table is a `ReplacingMergeTree` whose key ends in `span_id, kind`, and
every read carries `final = 1`.

- A retry inside **one** insert block collapses on the spot: the fixture's
  duplicated body stores 9 rows, not 11.
- A retry in a **later** block collapses at the next merge; `final = 1` makes
  the answer exact before that merge. Measured with 1% of spans duplicated in an
  unmerged part: `rate() by service` answers 2,020,027 rows without it and
  2,000,064 with it, at 41 ms against 68 ms over twelve runs each.
- Once the part is merged the setting costs nothing measurable: twenty
  alternating pairs on a single active part gave equal medians (32 ms each way).

## 5. The SQL each query shape compiles to

Generated by `measure/make_sql.py` into `measure/sql/`. Those are the shapes the
benchmark times; for the statement **every** query in the repository's TraceQL
corpus compiles to, see `docs/TraceQL/query-catalogue.md` and the two files per
served query in `measure/catalogue-sql/` — the statement the route issues and
the membership query the catalogue's answer column comes from. Measurements are the
median of five warm and three cold runs (`results/g1-new-design.tsv`);
"returned" is the RowBinary body, which is what crosses to PulsusDB.

### 5.1 One window rule, rendered three ways from one value

```sql
WHERE start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000
  AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985
```

The rule is `start <= ts < end` (owner decision, 2026-09-22), and it holds
wherever a window selects spans: search, the store-backed tag reads, the metrics
evaluation window, `compare()`'s own `start`/`end` arguments and both halves of
the service graph. Three of those are already half-open in the tree and change
nothing; `functional-requirements.md` §4.1 is the inventory, one row per window,
read off the code, and it also says which window is **not** covered by the rule:
the per-step range selector inside a metrics query keeps the right-closed
instants the query language defines for it. The
**row bound**, the **bucket bound** and the day-partition bound are rendered from one value —
the last nanosecond the window includes, `end - 1` — because when they are
rendered separately one can be narrower than the other and answers lose rows
silently; the existing `window_sql` module exists for exactly that reason and
gains a third rendering.

The bucket bound is not redundant: ClickHouse does **not** derive a key
condition on `intDiv(start_ns, …)` from a condition on `start_ns`. Measured on
this table: **980 of 980** granules without it, **26 of 980** with it.

### 5.2 Search: one statement, three reads

```sql
WITH
    1790084801000000000 AS s, 1790095601000000000 AS e,
    (SELECT (groupArray(trace_id), groupArray(keys))
     FROM (SELECT trace_id, max(start_ns) AS last,
                  groupUniqArray(intDiv(start_ns, 300000000000)) AS keys
           FROM spans
           WHERE start_ns >= s AND start_ns < e
             AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985
             AND (service = 'checkout')
           GROUP BY trace_id
           ORDER BY last DESC, trace_id ASC
           LIMIT 20)) AS top                                  -- (1) the answer's traces
SELECT m.trace_id, t.root_service, t.root_name, t.start_ns, t.end_ns - t.start_ns,
       m.last, m.matched, m.spans
FROM (SELECT trace_id, max(start_ns) AS last, count() AS matched,
             arraySlice(arraySort(x -> (x.2, x.1),
                        groupArray((span_id, start_ns, duration_ns, service))), 1, 3) AS spans
      FROM spans
      WHERE (intDiv(start_ns, 300000000000), trace_id) IN                      -- (2) exact keys
            (SELECT arrayJoin(arrayFlatten(arrayMap((t, ks) -> arrayMap(k -> (k, t), ks),
                                                     top.1, top.2))))
        AND start_ns >= s AND start_ns < e
        AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985
        AND (service = 'checkout')
      GROUP BY trace_id) AS m
LEFT JOIN (SELECT trace_id, min(start_ns) AS start_ns, max(end_ns) AS end_ns,
                  max(root_service) AS root_service, max(root_name) AS root_name
           FROM traces WHERE trace_id IN (SELECT arrayJoin(top.1)) GROUP BY trace_id) AS t  -- (3)
      USING trace_id
ORDER BY m.last DESC, m.trace_id ASC
```

- **The top-K is computed once**, as a scalar subquery. Written as an ordinary
  CTE it ran twice — 4,098,432 rows read against 2,098,370 — because ClickHouse
  inlines CTEs and only a scalar is computed once.
- **The detail read is by key**, so the spans of twenty traces are fetched
  without touching the window again.
- **The answer is final**: one row per trace with the root, the extent, the
  matched count and the capped spanset. 3,118 bytes for twenty traces.

**With `| by(<field>)` the detail pass gains a second grouping key** and the
trace row carries one spanset per group instead of one:

```sql
FROM (SELECT trace_id, max(last) AS last, sum(matched) AS matched,
             arrayMap(x -> (x.2, x.3, x.4), arraySort(x -> x.1,
                      groupArray((first, grp, grp_type, spans)))) AS groups
      FROM (SELECT trace_id,
                   toString(<the group field>) AS grp,       -- an ATTRIBUTE key only:
                   <its stored type>          AS grp_type,   -- an intrinsic's type is the query's
                   max(start_ns) AS last, count() AS matched,
                   min((start_ns, span_id)) AS first,
                   arraySlice(arraySort(x -> (x.2, x.1),
                              groupArray((span_id, start_ns, duration_ns))), 1, 3) AS spans
            FROM spans
            WHERE <the same three reads> AND <the group key is present>
            GROUP BY trace_id, grp, grp_type)
      GROUP BY trace_id) AS m
```

An intrinsic key drops `grp_type` from all three places, and the groups tuple is
`(grp, spans)`.

```text
{} | by(span.a) on the catalogue fixture, trace 1111…

  span …0001   a = 1     stored Int64      ->  ('1', 'int',    1 span)
  span …0003   a = 1.0   stored Float64    ->  ('1', 'double', 1 span)

  the label alone: one group of two spans, and no arm to render it in
```

**`<the group field>` is rendered as TEXT beside its STORED TYPE** —
`toString(…) AS grp, <the type> AS grp_type`, both in the `GROUP BY`. Two rules
meet there:

* An attribute read is a `Dynamic` value and ClickHouse refuses those as
  grouping keys outright (code 44, *"Data types Variant/Dynamic are not allowed
  in GROUP BY keys"*), so a statement that groups on the bare read does not run
  at all.
* `docs/api.md` §4.2 renders an attribute group key **in the arm the sender
  stored it as**, and an integer `1` and a double `1.0` have the same text. A
  statement that groups on the label alone therefore merges two groups the API
  keeps apart, and leaves the response layer no way to choose the arm. The
  catalogue fixture has exactly that pair on one trace, and the statement
  answers `('1', 'int', 1 span)` and `('1', 'double', 1 span)`.

The type column is for an **attribute** key only: an intrinsic's type follows
from the intrinsic, and `resource.service.name` is always a string, so for those
the group is the label alone.

This is not a detail. Review round 5 found the catalogue rendering the bare
read, so **every** `by(<an attribute>)` search — a shape §3.2 documents as served
— was a statement the database rejected, and no corpus query groups by an
attribute, so nothing noticed. `measure/catalogue-extra.tsv` now carries the four
queries that reach the rule.

The cost table below was measured on `measure/sql/c05_by_attribute.sql`, which
casts the label and does **not** read the type; that statement and its figures
are unchanged. What the type column adds is one read of `dynamicType(attrs.k)` —
the discriminator ClickHouse already reads to answer `attrs.k` at all — and one
more `GROUP BY` key over a short string. That is an argument from what the two
statements read, not a second measurement.

Three more rules are in that shape, and each is one a simpler statement gets
wrong: the groups come back in **first-appearance order**, which is
`min((start_ns, span_id))` per group and not the group value's own order; a span
**lacking the key gets no spanset**, which is the `AND <present>`; and the top-K
that chooses the twenty traces is still per trace, not per group, so a trace with
many groups does not crowd out another trace. `| coalesce()` drops the key again,
which is the ungrouped statement above.

Five of these are rows of `measure/perturbations.tsv` — first-appearance order,
the missing-key rule, the label's rendering, the stored type beside it and
`coalesce()` — each with a query that tells a wrong rule from the right one. The top-K is not a row and cannot be:
the subquery that chooses the twenty traces is the same one the ungrouped
statement issues, and the group key does not appear in it at all, so there is
nothing there to change.

| search shape | rows read | returned | warm | cold | reference |
|---|---:|---:|---:|---:|---:|
| `{}` | 2,045,122 | 3,001 | 62 ms | 75 | 9 ms |
| a service | 2,048,194 | 3,118 | 36 | 56 | 10 |
| a service and `status = error` | 2,060,229 | 2,287 | 33 | 60 | 36 |
| `span.http.response.status_code >= 500` | 2,040,002 | 2,557 | 47 | 68 | 19 |
| `duration > 2s && kind = server` | 2,054,417 | 2,151 | 36 | 66 | 18 |
| a regex on `span.http.route` | 2,043,074 | 2,638 | 46 | 74 | 9 |
| `span.app.user.id = "u-10013"` | 2,012,354 | 546 | **44** | 67 | 139 |
| an event attribute | 2,045,122 | 2,428 | 43 | 55 | 13 |
| `select()` across scopes | 2,045,122 | 3,699 | 36 | 62 | 12 |
| `\| count() > 5` | 2,053,314 | 3,543 | 43 | 58 | 13 |
| arithmetic on two attributes | 2,038,978 | 2,239 | 50 | 63 | — |
| a field-against-field comparison | 2,047,170 | 2,490 | 55 | 63 | — |
| an aggregate over a projected value | 2,045,122 | 3,426 | 67 | 72 | — |
| `\| by(span.rpc.method)` | 3,073,217 | 4,926 | 90 | 148 | — |
| trace-level intrinsics | 1,246,002 | 2,962 | 78 | 168 | 26 |
| an unscoped and a resource attribute | 2,050,650 | 2,615 | 113 | 145 | 19 |

### 5.3 Broad searches read the newest slice first

The twenty newest traces are in the newest minutes, so PulsusDB runs the same
statement with its **first pass bounded to a slice** — 5 minutes, then 10, then
20, doubling — and stops when twenty traces are in hand. The answer is
identical: a trace found in a newer slice always outranks one found only in an
older slice, and the second pass still covers the whole window through the
per-trace extents.

| shape | one statement | newest-slice-first |
|---|---|---|
| `{}` | 2,045,122 rows, 62 ms | **95,397 rows, 34 ms**, 2 statements |
| a service | 2,048,194 rows, 36 ms | **98,469 rows, 31 ms**, 2 statements |
| `status_code >= 500` | 2,040,002 rows, 47 ms | **90,277 rows, 32 ms**, 2 statements |
| `select()` over errors | 2,045,122 rows, 36 ms | **95,397 rows, 32 ms**, 2 statements |
| `span.app.user.id = …` (rare) | 2,012,354 rows, **44 ms** | 2,247,021 rows, 116 ms, 6 statements |

The last row is why the loop is conditional: for a filter matching almost
nothing the slices are pure overhead. `server-implementation.md` §3.5 states the
rule the compiler applies and the statement bound.

### 5.4 Trace by id

```sql
WITH (SELECT (min(start_ns), max(end_ns)) FROM traces WHERE trace_id = unhex('…')) AS ext
SELECT groupArray((span_id, parent_span_id, start_ns, duration_ns, service, resource_id, name,
                   kind, status_code, status_message, trace_state, flags, scope_name,
                   scope_version, scope_attrs, attrs, attrs_other, dropped_attrs, events,
                   dropped_events, links, dropped_links)) AS spans,
       (SELECT groupArray((resource_id, attrs, attrs_other, dropped_attrs, schema_url))
        FROM resources WHERE … resource_id IN (the trace's resources)) AS resources
FROM spans
WHERE (intDiv(start_ns, 300000000000), trace_id) IN
      (SELECT (k, unhex('…')) FROM (SELECT arrayJoin(range(intDiv(ext.1, 300000000000),
                                                            intDiv(ext.2, 300000000000) + 1)) AS k))
```

One statement, one row: the spans and the distinct resources they point at, so a
resource crosses the wire once rather than once per span.

| | rows read | returned | warm | cold | the reference (interleaved p50) |
|---|---:|---:|---:|---:|---:|
| 20-span trace | 5,190 | 11,699 | **32 ms** | 168 | 53 ms |
| 1,000-span trace | 9,286 | 335,128 | **40 ms** | 130 | 102 ms |

The reference column is the interleaved p50 of 21 repetitions
(`measure/fetch_compare.py`, `results/g1-fetch-compare.tsv`), where this design
was 35 and 42 ms against 53 and 102. Interleaving is what makes the comparison
mean anything: the reference's fetch time moves between sessions — 53 and 102 ms
in this run, 61 and 112 in the one before, 66 and 121 before that — while this
design stayed inside 30–43 ms in every session.

### 5.5 Tags, and the contract they must keep

`docs/api.md` §4.3 is authoritative and unchanged: name discovery is **time-less**
(`start`/`end` accepted and ignored, entries may outlive span retention), an
**unnarrowed** value lookup is a catalog read, a **narrowed** one reads the store,
`name` is the one intrinsic whose values come from the store bounded by the
window, and every other intrinsic answers from a static vocabulary without
reading anything.

```sql
-- names, by scope: the time-less catalog
SELECT scope, key FROM tag_names FINAL
WHERE scope IN ('span','resource','event','link','instrumentation')
ORDER BY scope, key LIMIT 10001

-- values for one key, unnarrowed: the time-less catalog, typed per value
SELECT value, val_type FROM tag_values FINAL
WHERE scope = 'span' AND key = 'http.route' ORDER BY value LIMIT 1001

-- values narrowed by a query: the store, bounded by the window
SELECT toString(v) AS value, dynamicType(v) AS type
FROM (SELECT attrs.`k8s%2Epod%2Ename` AS v FROM resources
      WHERE … resource_id IN (SELECT DISTINCT resource_id FROM spans WHERE <window> AND service = 'cart'))
GROUP BY value, type ORDER BY value

-- the `name` intrinsic: the store, bounded by the window
SELECT DISTINCT name FROM spans WHERE <window> ORDER BY name LIMIT 1001
```

| shape | rows read | returned | warm | cold | reference |
|---|---:|---:|---:|---:|---:|
| names, five scopes | 48 | 1,123 | **1 ms** | 2 | 4 ms |
| values for one key, unnarrowed | 8,192 | 1,445 | **1 ms** | 2 | 3 ms |
| values narrowed by a query | 2,000,132 | 100 | **24 ms** | 36 | 30 ms |
| `name` values, window-bounded | 2,000,064 | 6,547 | **15 ms** | 22 | — |

Scanning the span table for names instead — `distinctJSONPaths(attrs)` over the
window — reads 741,332,805 bytes and takes 336 ms, which is why the catalog
exists. The catalogs cost 1,190 and 1,691,983 bytes for the whole corpus, and
they carry **all five API scopes**: on g1, 28 span names, 15 resource, 4 event,
1 link, and the instrumentation names of whatever scope attributes a sender
sends (the corpus sends none; the fixture sends one, `otel.scope.build`).

**The window applies to the store-backed reads only.** Names and unnarrowed
values are time-less, by `docs/api.md` §4.3. The two reads that do touch the
store — a `q`-narrowed value list and the `name` intrinsic — use §5.1's rule,
`[start, end)`, at nanosecond precision rather than the day widening the API
document describes today; that sentence is one of the edits
`functional-requirements.md` §4.1 lists, and `T-B7` is its boundary case.

The reference answered the unnarrowed values query with an **empty list** while
reporting 3,621,235 bytes inspected, and its span-scope name list omits four
keys the corpus contains (`http.request.method`, `http.route`, `server.address`,
`url.path`); this design returns the complete lists, which is what the contract
says.

### 5.6 Metrics

```sql
SELECT series, groupArray((t, v)) AS points
FROM (SELECT service AS series,
             (intDiv(start_ns - 1, 60000000000) + 1) * 60000 AS t,   -- right-closed step label
             count() AS v
      FROM spans
      WHERE start_ns >= <snapped start> AND start_ns < <snapped end>
        AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985
      GROUP BY series, t ORDER BY t)
GROUP BY series ORDER BY series
```

One row per series, not one per point, and with `with(exemplars=…)` one exemplar
per bucket per series in the same pass —
`argMax((trace_id, span_id, duration_ns), (duration_ns, span_id))`, so the
exemplar is the bucket's longest span and the span id breaks a tie, which makes
the answer the same on every run. `with(sample=…)` and every other hint change
no read at all: the shipped planner accepts them and returns the exact superset
(`crates/pulsus-read/src/traces/metrics_plan.rs:1093`).

Grouping by a **resource** attribute
aggregates by `resource_id` first and joins the small resource table afterwards,
so the group key is resolved once per resource rather than once per span.
Exemplars come from the same pass (`argMax` over the bucket), `topk`/`bottomk`
order the finished series inside the same statement, and `compare()` produces the
two attribute distributions in one statement.

| metrics shape | rows read | returned | warm | cold | reference | today |
|---|---:|---:|---:|---:|---:|---:|
| `rate() by (resource.service.name)` | 2,000,064 | 46,120 | **38 ms** | 44 | 249 ms | 181 ms |
| `quantile_over_time(…) by (span.http.route)` | 2,000,064 | 28,578 | **40** | 56 | 190 | `400` |
| `count_over_time() by (…)` over errors | 2,000,064 | 37,264 | **17** | 27 | 62 | 52 |
| `histogram_over_time(duration)` | 2,000,064 | 38,843 | **26** | 37 | 163 | 125 |
| instant `avg_over_time by (name)` | 2,000,064 | 2,346 | **27** | 45 | 210 | `400` |
| `rate() by (resource.k8s.pod.name)` | 2,000,132 | 185,352 | **44** | 46 | 445 | `400` |
| with exemplars | 2,000,064 | 235,706 | 90 | 91 | — | — |
| `topk(3)` over the series | 2,000,064 | 8,140 | 42 | 53 | — | — |
| `compare()` | 20,071,121 | 34,950 | **1,191** | 1,428 | 1,229 ms¹ | — |

¹ The median of three calls of the same query against the pinned reference
build, which returned 412 series in 311,470 bytes. Today's engine serves
`compare()` too; this run did not time it, so that cell is empty rather than
guessed.

`compare()` is the one shape that reads every attribute of every span — it is a
distribution over all of them by definition — and it is the slowest statement in
the set at **1,191 ms**. The row count says why: 20,071,121 rows for 2,000,064
spans, because each span contributes one row per attribute it carries in each of
the five scopes plus one per intrinsic. The earlier statement in this file read
2,000,064 and answered in 478 ms; it also answered wrongly, counting a shared
resource once instead of once per span and reporting five keys instead of
fifteen.

Three properties of that statement are worth stating, because a simpler one
fails each (`measure/make_sql.py`, the `compare` function, and the five checks
of `measure/edge_checks.sh`):

- it counts **spans**. A resource is shared by many spans, and each span is in
  the selection or the baseline on its own account; grouping the resource
  attributes by `resource_id` puts a shared resource wholly on one side.
  Measured on a resource under one selection span and two baseline spans: 1 and
  2, where grouping by resource gave 1 and 0.
- its key universe is **every attribute the span exposes** — span, resource,
  instrumentation-scope, event and link — plus the intrinsics the reference
  reports: `name`, `kind`, `status`, `statusMessage`, `instrumentation:name`,
  `instrumentation:version`, `trace:rootName`, `trace:rootService`,
  `event:name`, `link:traceId`, `link:spanId`. `kind` and `status` render as the
  keywords the API returns, not as the stored codes. The reference walks the
  same set (`tempodb/encoding/vparquet4/block_traceql.go:107-125` @ v3.0.2) and
  skips duration-typed values and six intrinsics
  (`pkg/traceql/engine_metrics_compare.go:143-159`). **PulsusDB also omits
  `span:id`**, which the reference includes: it is unique per span, so it can
  only return `topN` arbitrary spans — the reference's own reason for the six it
  skips is that "the cardinality isn't useful". Ledger row and `docs/api.md`
  entry.
- `topN` is applied **in the statement**, per key and per side, before any cap.
  A global `LIMIT` after the counts drops whole keys, and no response layer can
  put back a row the statement never returned.

All three are checked on the catalogue fixture as well as at corpus scale, by a
**pair** of comparisons in `measure/catalogue-extra.tsv`. `compare_both_sides`
selects the spans carrying `.b = 2`: its selection and baseline are both
non-empty, and a resource carried by spans on both sides is counted 9 against 6.
`compare_selection_scopes` selects `.a = 1` instead, which is where the fixture's
events, links and instrumentation attributes are — in the first query those three
scopes appear on the baseline only, and in the second on the selection only, so
between them **every scope appears on both sides** and a rule that put one
scope's rows on the wrong side cannot hide in a scope that only ever has one.
`measure/perturbations.tsv` drops the resource scope, renders the stored codes
instead of the keywords, counts a resource once instead of once per span,
replaces the per-key `topN` with one cap, and forces every event row to the
baseline — each on one side at a time, each of which the comparison now
reports. Until round 5 the catalogue's own
comparison statement was a reduced one on both sides: span attributes and three
intrinsics, with a single global `LIMIT`, so two reduced answers agreed and
established nothing about the statement above.

A value carries its stored type, because a value is counted per type: integer
`1` and double `1.0` render as the same text and are two values.

**What `quantile_over_time` returns, and to what precision.** The aggregate is
`quantilesTDigest`, whose ClickHouse return type is **`Float32`** — measured,
not assumed:

```
SELECT toTypeName(quantilesTDigest(0.9)(v)[1]) FROM (SELECT toInt64(1) AS v)
-> Float32
```

A duration in nanoseconds therefore comes back with about seven significant
digits. The table below is the step between representable values at each
magnitude — the largest error the return type can introduce, before t-digest's
own approximation:

| the duration | Float32 step | as a share |
|---|---:|---:|
| 1 ms (10⁶ ns) | 0.0625 ns | 6·10⁻⁸ |
| 1 s (10⁹ ns) | 64 ns | 6·10⁻⁸ |
| 1 h (3.6·10¹² ns) | 262,144 ns | 7·10⁻⁸ |

That is far inside what a latency panel reads, and it is what the shipped code
already returns (`crates/pulsus-read/src/traces/metrics_sql.rs:994`), so it
stays. It is written down because it is the one place in the read path where a
stored `Int64` comes back through a narrower type: the statements wrap the
result in `toFloat64` so the value's own digits are printed rather than
`Float32`'s shortest spelling of them, which recovers no precision and hides
none.

### 5.7 The service graph

An edge is `(client, server, connectionType)`, the window is `[start, end)` like
the metrics routes, and the statement asks for one row more than the documented
1,000-edge cap so the response can set `truncated`:

```sql
SELECT c.service AS client, s.service AS server,
       if(c.kind = 3, 'rpc', 'messaging') AS connection_type,
       count() AS calls,
       countIf(s.status_code = 2 OR c.status_code = 2) AS failed,
       CAST(quantilesTDigest(0.5, 0.95, 0.99)(s.duration_ns) AS Array(Float64)) AS quantiles_ns
FROM (SELECT trace_id, span_id, service, status_code, kind
      FROM spans WHERE <window> AND kind IN (3, 4)) AS c
INNER JOIN (SELECT trace_id, parent_span_id, service, status_code, duration_ns, kind
            FROM spans WHERE <window> AND kind IN (2, 5)) AS s
      ON s.trace_id = c.trace_id AND s.parent_span_id = c.span_id
WHERE (c.kind = 3 AND s.kind = 2) OR (c.kind = 4 AND s.kind = 5)
GROUP BY client, server, connection_type
ORDER BY calls DESC, client ASC, server ASC, connection_type ASC
LIMIT 1001
```

195 ms warm, 1,233 bytes returned, against today's 323 ms over a 22.49 B/span
ledger this design deletes.

**A Zipkin shared span carries both halves under one span id**, so the statement
has a second join branch: the ordinary branch pairs a server span to its parent,
the shared branch pairs it to the client span with the **same** id, selected by
``coalesce(attrs.`zipkin%2Eshared`.:Bool, false)``. Measured on one ordinary
pair and one shared pair (`measure/shared_span_edges.sh`): two rpc edges,
`svc-a → svc-b` and `svc-a → svc-c`, one call each — the single-branch form
returns only the first. `T-C6` is that case; `T-C7` is the rpc/messaging pair
that must not merge.

### 5.8 Structural operators: all fifteen forms

Five operators — child `>`, parent `<`, sibling `~`, descendant `>>`, ancestor
`<<` — each in three modifiers: plain, negated `!`, and union `&` (which returns
both sides of every holding pair). Each has a statement in `measure/sql/`, run
on the corpus with `A = { resource.service.name = "frontend" }` and
`B = { resource.service.name = "payment" && status = error }`:

| form | statement | rows read | warm |
|---|---|---:|---:|
| `>` / `!>` / `&>` | `st01` / `st02` / `st03` | 2,000,064 each | 74 / 77 / 73 ms |
| `<` / `!<` / `&<` | `st04` / `st05` / `st06` | 2,000,064 each | 80 / 68 / 68 ms |
| `~` / `!~` / `&~` | `st07` / `st08` / `st09` | 2,000,064 each | 106 / 108 / 97 ms |
| `>>` / `!>>` / `&>>` | `st10` / `st11` / `st12` | 9.9M / 10.3M / 14.0M | 994 / 965 / 1,325 ms |
| `<<` / `!<<` / `&<<` | `st13` / `st14` / `st15` | 6.6M / 6.9M / 8.8M | 594 / 612 / 874 ms |

The three non-transitive operators need only the spans matching either side,
grouped by trace: the relation is a set test inside the group. The two
transitive ones climb, and the row counts are the climb's repeated reads of the
candidate traces' spans.

`measure/sql/s09_descendant.sql` is the same relation as `st10` in the tuned
form the compiler emits for a search — it carries the spanset projection and the
per-trace header — and runs in **396 ms** against `st10`'s 994, because it
climbs from the B spans only and reads each candidate trace once more, rather
than materialising the pair set.

**The literal answers of all fifteen forms on the worked fixture** are produced
by `measure/fixture/structural_answers.py` and committed at
`results/fixture-structural.tsv`; they are the expected values of test case
`T-A9`.

```sql
WITH RECURSIVE climb AS (
    SELECT trace_id, span_id AS seed, parent_span_id AS cur, 0 AS depth
    FROM spans WHERE <the candidate traces' keys> AND <window> AND (B)
    UNION ALL
    SELECT c.trace_id, c.seed, x.parent_span_id, c.depth + 1
    FROM climb AS c
    INNER JOIN (SELECT trace_id, span_id, parent_span_id
                FROM spans WHERE <the candidate traces' keys> AND <window>) AS x
        ON x.trace_id = c.trace_id AND x.span_id = c.cur
    WHERE c.depth < 64 - 1)          -- PULSUS_TRACEQL_MAX_DEPTH parent links
```

**The bound is a number of parent links, and the row that reports it is its
own row.** A row of `climb` at depth `d` has already followed `d + 1` links, so
expanding rows up to `depth = 63` follows at most 64 — the bound — and an
ancestor 65 links up is out of reach. Written `c.depth < 64` the climb follows
65, which is the off-by-one a 66-span chain shows: measured, that chain matched.

```sql
overflow AS (SELECT count() AS unresolved
             FROM climb AS c
             INNER JOIN (SELECT trace_id, span_id, parent_span_id
                         FROM spans WHERE <keys> AND <window>) AS x
                 ON x.trace_id = c.trace_id AND x.span_id = c.cur
             WHERE c.depth = 64 - 1 AND x.parent_span_id != toFixedString('', 8))
```

The count is a row of the result, not a column of the matches:

```sql
SELECT * FROM (
    (SELECT 'match' AS row_kind, trace_id, …  FROM (<the matching spans>)
     GROUP BY trace_id ORDER BY last DESC, trace_id ASC LIMIT 20)
    UNION ALL
    (SELECT 'overflow', …, (SELECT unresolved FROM overflow)))
```

Carried as a scalar in the `SELECT` list of the grouped query it disappears
when nothing matches, which is exactly the case it exists for. Measured: a
database holding only a 67-span chain returned **0 rows** from the earlier
shape and returns **1 overflow row with `unresolved = 1`** from this one. A
non-zero `unresolved` is the reader's signal to answer `422 query_too_broad`
rather than an answer it cannot stand behind.

| trace | matched | unresolved | the route answers |
|---|---:|---:|---|
| 65 spans, the leaf exactly 64 links below the A root | the leaf | 0 | the match |
| 66 spans, the leaf 65 links below | none | ≥ 1 | `422` |
| two spans that are each other's parent | none | ≥ 1 | `422` |
| the 66-span chain alone in a database | none, **0 rows of matches** | 1 | `422` |

`measure/edge_checks.sh` runs all four against
`measure/fixture/make_edge_fixture.py`, with the expected answers written down.
Without the bound the cycle does not terminate; without the overflow row the
first three are indistinguishable from "no match".

**The union modifier's partner set is relation-specific**, and one expression
for all three relations is wrong. Union returns both sides of every pair the
relation holds for, so:

    A > B  (child)    the partner of a hit is its PARENT among the A spans
    A < B  (parent)   the partners are the A spans whose parent is the hit
    A ~ B  (sibling)  the partners are the other A children of the hit's parent

Measured on `ee06` of the edge fixture — `P → A2 → B1 → A1` beside
`P → P2 → {A3, B2}`:

| form | the answer | one partner expression for all three gave |
|---|---|---|
| `A &> B` | `A2, B1` | `A1, A2, B1` — A1 is a child of the hit, not its parent |
| `A &< B` | `A1, B1` | `A1, A2, B1` |
| `A &~ B` | `A3, B2` | `B2` — the sibling partner was never added |

**A trace with no A span at all still answers the negated forms.** The
reference evaluates `!>>` with an empty left operand and `falseForAll`, so
every B span qualifies (`pkg/traceql/ast_execute.go:114-119` and
`tempodb/encoding/vparquet4/block_traceql.go:296-325` @ v3.0.2). The candidate
restriction is therefore relation-dependent: the plain and union forms need a
span of each side in the trace, the negated forms only a B span. Trace `ee07`
of the edge fixture holds B spans and no A span, and `edge_checks.sh` asserts it
is returned by `!>`, `!<`, `!~`, `!>>` and `!<<` and by none of the others.

**The alternative that was measured and rejected**: per-trace arrays with
pointer jumping inside `arrayFold`. Identical answer (same checksum), 2,721 ms
and 840 MB against 162 ms and 7.3 MB, because ClickHouse replicates a captured
array once per element inside a lambda, making `indexOf(ids, parent)` quadratic
in the spans of a trace. Today's engine answers the same query in 5,069 ms with
185.8 statements.

### 5.9 Nested-set numbering

`nestedSetLeft`, `nestedSetRight` and `nestedSetParent` are computed by a walk
of the trace, in the retained entry/exit convention — one counter incremented on
entry and on exit, so n spans occupy 1..2n:

```
   dfs position r, depth d, subtree size s
   left   = 2 * r - 1 - d
   right  = left + 2 * s - 1
   parent = the parent's left, and -1 at a root
```

`r` is the order of each span's root path, and `s` is the number of spans whose
path has this span's path as a prefix.

**The numbering is total over the stored spans, and does not depend on the order
rows come back in.** That is not a detail: a client asks for root spans by
writing `{ nestedSetParent < 0 }` — both `grafana/explore_root_rate_by_service`
and `grafana/explore_root_rate_sample` in the corpus do — so a span left
unnumbered is a span missing from an answer. Three shapes decide it, and the
retained implementation
(`crates/pulsus-read/src/traces/search_eval.rs:2085-2139`) settles each:

| shape | the rule | what a naive walk does |
|---|---|---|
| two children of one parent with the **same** `start_ns` | the walk carries `(start_ns, span_id)`, so sibling order is total, and a subtree's rows are exactly the rows whose path begins with its own | ordering on `start_ns` alone leaves the order undefined and lets one sibling's prefix match the other's rows |
| a span whose **parent is not stored** in the window | it is a root of the hydrated forest, seeded like any root | seeding only `parent_span_id = ''` never reaches it: it and its subtree are unnumbered |
| a **cycle** | no member is a forest root, so the walk cannot reach any of them; each unnumbered component's first span by `(start_ns, span_id)` is promoted to a root, keeps `parent = -1`, and the walk from it stops when it would revisit a span already on its path | the whole component is unnumbered |

In SQL the promotion needs no iteration. After the forest walk, every remaining
span's parent is also remaining, so the spans that can reach a span X are
exactly X's own parent chain; X is promoted when X is the smallest
`(start_ns, span_id)` on that chain, which is one more bounded climb. The walk's
bound is `MAX_SPANS_PER_TRACE` (10,000, `crates/pulsus-read/src/traces/exec.rs:125`),
deeper than ClickHouse's default recursive-CTE depth, so the statement carries
`max_recursive_cte_evaluation_depth`. Any span the two passes still do not
number is counted in `unnumbered`, a column of the one row the statement
returns, so it cannot be lost.

Verified against a tree whose answer is written out by hand
(`measure/nested_set_check.sh`):

| span | left | right | parent |
|---|---:|---:|---:|
| root | 1 | 8 | −1 |
| A | 2 | 5 | 1 |
| C (under A) | 3 | 4 | 2 |
| B | 6 | 7 | 1 |

and against the two awkward traces of `measure/fixture/make_edge_fixture.py`,
where the same numbers are produced twice — once by this SQL and once by the
independent Python of `measure/catalogue_interp.py`, which reads the fixture rows
directly — and agree span for span:

| trace | spans | numbering | roots | unnumbered | the earlier statement gave |
|---|---:|---|---:|---:|---|
| a root, two children at the same instant, a grandchild, an orphan | 5 | 1..10 | 2 | 0 | 4 spans, 1..9, 1 root |
| a two-span cycle with a child hanging off it, beside a well-formed root | 5 | 1..10 | 2 | 0 | 2 spans, 1..4, 1 root |

and on the corpus: the 1,000-span trace numbers 1..**2,000** with 1,000 distinct
left values and one root (`c14`); the 20-span trace 1..**40** (`c15`).

#### What the query path does, and what it does not

**Numbering a whole window is not possible at this scale, and the query path
never does it.** ClickHouse inlines a CTE at each reference and re-reads it on
each iteration of a recursive one, so the walk reads the span set tens of times.
Measured on the 3-hour corpus (2,000,064 spans, 70,413 traces), a statement
numbering every trace in the window was killed by the server's memory ceiling:

```
Code: 241 … (total) memory limit exceeded: would use 5.04 GiB … maximum: 5.40 GiB
real 2m12.744s
```

and numbering ONE 1,000-span trace reads 14.9M rows for 3.0 s (`c14`). So the
design answers the three shapes a client sends like this:

| the query | how it is answered | measured |
|---|---|---|
| `{ nestedSetParent < 0 }` — both Grafana queries in the corpus | **no numbering**: a root of the hydrated forest is a span with no stored parent, which is one anti-join (`c20`) | **437 ms** warm over the window, 4.0M rows read, and the count it returns is 70,413 — exactly the number of traces in the window |
| `{ nestedSetLeft > 0 }`, `{ nestedSetRight >= 1 }` | **no numbering**: the numbering starts at 1, so every stored span satisfies them | the ordinary search statement |
| any other nested-set comparison | the search runs without the nested-set condition, a second statement hydrates the candidate traces whole (`c21`), and the reader numbers those spans with the retained Euler tour it already carries | **35 ms** for 20 candidate traces, 924 spans, 44,352 bytes returned |

**Where the shortcut and the numbering differ, and why that is the right trade.**
A span inside a *pure cycle* has a stored parent, so the anti-join does not
return it, while the numbering promotes one member of each cyclic component to a
root with `parent = -1`. Returning it from the window shortcut would mean
numbering the window, which is the thing that cannot be afforded — 2 m 12 s and a
dead server against 437 ms. So the difference is stated rather than closed: an
orphan **is** a root on both paths (measured on the edge fixture, which returns
`ee01:05`), and a span inside a cycle is a root only where the numbering is
actually computed. A cycle is malformed data — a span cannot be its own ancestor
— and this is the one shape where the two disagree. It gets a ledger row and a
`docs/api.md` §4.2 entry.

That last row is one of `server-implementation.md` §3.5's four cases — two
statements, named, with its reason. The bound is the search's own: at most the
trace cap × `MAX_SPANS_PER_TRACE` spans are numbered, never a window.

The SQL numbering above is not dead: it is how the rule is checked. `c14` and
`c15` number one corpus trace; `measure/nested_set_check.sh` numbers a
hand-computed tree; and `measure/edge_checks.sh` numbers the two awkward traces
of fixture E — a root with two same-instant siblings, a grandchild and an
orphan, and a two-span cycle with a child hanging off it — against the four
answers written out in that script
(`results/edge-checks.tsv`, rows `nested_ee01_detail` … `nested_ee02_totals`).
So the rule the reader implements is verified against an independent
statement of itself on the shapes that break a naive walk, without that
statement being in the query path.

**The catalogue does not number.** The corpus's three nested-set queries are
exactly the three shapes §3.2 answers without a numbering — `nestedSetParent < 0`
is the root anti-join, `nestedSetLeft > 0` and `nestedSetRight >= 1` are `true` —
so **none of the three nested-set query pairs** in `measure/catalogue-sql/`
carries a recursive CTE, and the catalogue's interpreter answers the root test
from the stored parent for the same reason the statement does. Six other files
there do carry one — `structural_shl`, `structural_shr` and
`structural_precedence`, each with its membership twin — where the recursion is
the bounded climb of §5.8 and not a numbering.


## 6. Retention

One `ALTER TABLE … DROP PARTITION` per table per day. No `ALTER … DELETE`, no row
rewrite, nothing to merge. Measured on a day holding 2,000,064 spans
(`measure/retention.sh`):

```
before: 1 part, 2,000,064 rows, 69,152,282 bytes
drop:   0.068 s
after:  0 parts, 0 rows; 0 mutations scheduled; 0 merges running
```

`spans`, `resources` and `traces` are partitioned by day and drop together;
`tag_names` and `tag_values` are time-less and are not dropped, which is what
`docs/api.md` §4.3 requires of the catalog ("catalog entries can therefore
outlive the 7-day span retention").

## 7. Clustered

| table | sharding key | why |
|---|---|---|
| `spans`, `traces` | `cityHash64(trace_id)` | a trace is whole on one shard, so per-trace grouping, the structural climb and the fetch are shard-local |
| `resources`, `tag_names`, `tag_values` | replicated to every shard | tiny, and read by every shard's join or dropdown |

A search's first pass aggregates per shard and the coordinator merges twenty
rows per shard; the detail read and the per-trace lookup are shard-local because
they are keyed by `trace_id`. `distributed_product_mode = 'local'` is injected
for the `IN` subqueries exactly as the current reader does. Replication moves
one compressed copy of each part to each further replica: measured **34.965
bytes per span** fetched by the second replica (`measure/replication_bytes.sh`)
against **34.923** stored — 1.0012×, the excess being part metadata rather than
a second copy of any column.

## 8. The layouts that were measured, and why this one

All six alternatives are built by `measure/layouts.sql` from the same staging
table, so only the layout differs, and `measure/run_all.sh` benchmarks each of
them — and the shipped one — on the five shapes that discriminate
(`results/layout-comparison.tsv`). Warm medians of five repetitions, one
unmeasured first:

| layout | B/span | fetch, 20 spans | fetch, 1,000 spans | service-scoped quantiles | instant by name | narrowed tag values |
|---|---:|---:|---:|---:|---:|---:|
| **this design**: trace first, granule 2048 | **34.965** | **32 ms** | **44** | 38 | 29 | 26 |
| trace first, granule 8192 | 34.578 | 35 | 40 | 41 | 22 | 25 |
| trace first, granule 1024 | 35.096 | 32 | 36 | 42 | 22 | 30 |
| trace first, granule 512 | 35.404 | 29 | 35 | 44 | 30 | 28 |
| service first, granule 8192 | 33.244 | 60 | 94 | **30** | **19** | **15** |
| resources inline (no resource id) | 46.488 | n/a | n/a | 40 | 27 | n/a |
| the same rows under LZ4 | 57.552 | 25 | 33 | 34 | 21 | 21 |

The four `n/a` cells are recorded with the server's own message in
`results/layout-comparison.tsv`: the resources-inline table carries only the
columns the resource question needs, so the two fetch shapes, which project the
whole span row, and the narrowed tag read, which joins the resource table, have
nothing to run against.

- **Trace first beats service first on every trace-centric read** — the 20-span
  fetch is 32 ms against 60, and the 1,000-span fetch 44 against 94 — and
  service first beats it on service-scoped reads by about 1.3 to 1.5×. The service prune's advantage grows with
  the number of services (24 in g1); the fetch penalty does not shrink, and the
  fetch is the shape R10 is measured on against the reference. This design takes
  the fetch.
- **Granule 2048** is the knee: 512 buys 3 ms on the small fetch for 1.3% more
  storage and four times the primary-key memory (93,872 bytes against 23,528 on
  this corpus), and costs 6 ms on the service-scoped quantiles.
- **LZ4 instead of ZSTD** on the same rows is 57.552 B/span against 34.965 — 65%
  more storage for 3 to 7 ms on a read. That is not a storage option; it prices
  the insert hop, where the client compresses a RowBinary block with LZ4 by
  default.
- **Resources inline** costs 46.488 B/span against 34.965: the `resource` column
  alone is 13.991 B/span where the 128-bit `resource_id` is 1.495, and it answers
  the resource-scoped shapes no faster (40 ms against 38).

## 9. Where one requirement is traded against another

| trade | measured cost | why this side |
|---|---|---|
| trace_id before service in the sort key | service-scoped metrics 42 ms instead of 27 | the fetch, the hydration and the structural climb are key reads instead of scans; R10 is measured on the fetch |
| `final = 1` on every read | 68 ms against 41 while a retried part is unmerged; nothing once merged | a retried push must not be counted twice |
| no attribute index | an equality on a 50,000-value key is a windowed subcolumn scan: 43 ms against today's 26 ms | that index is 493 B/span, thirteen times the whole new layout |
| a time-less value catalog | 0.846 B/span, and it grows with distinct values | `docs/api.md` §4.3 requires an unnarrowed value lookup to be a catalog read |
| 5-minute buckets in the key | an hour window may over-read 5 minutes at each end | a trace stays inside one bucket, so per-trace work stays local |
