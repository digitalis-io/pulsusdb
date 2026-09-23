#!/usr/bin/env python3
"""Writes the SQL each measured query shape compiles to, one file per shape,
into docs/TraceQL/measure/sql/. These are hand-compiled statements in exactly
the form docs/TraceQL/sql-schema.md proposes the compiler emits.
Usage: make_sql.py DB START_NS END_NS [OUTDIR [TRACE_ID ...]]"""
import sys, os
DB, S, E = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
OUT = (sys.argv[4] if len(sys.argv) > 4
       else os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sql'))
B = 300_000_000_000          # the bucket width in the sort key, 5 minutes
MAXDEPTH = 64                # PULSUS_TRACEQL_MAX_DEPTH: the climb's bound
# The window's bucket bound is rendered explicitly beside the row bound:
# ClickHouse does not derive a key condition on intDiv(start_ns, B) from a
# condition on start_ns (measured: 246/246 granules without it, 8/246 with).
# One window rule everywhere: start inclusive, end exclusive. The bucket bound
# and the day partition are rendered from the SAME last-included nanosecond,
# E - 1, so a row bound and a prune can never get out of step.
BW = f"intDiv(start_ns, {B}) BETWEEN {S // B} AND {(E - 1) // B}"
os.makedirs(OUT, exist_ok=True)

def p(key):                  # an OTLP attribute key as a stored JSON path
    return '`' + key.replace('%', '%25').replace('.', '%2E') + '`'

def search(name, F, proj='', having='', limit=20, spss=3):
    extra = (', ' + proj) if proj else ''
    sql = f"""WITH
    {S} AS s, {E} AS e,
    (SELECT (groupArray(trace_id), groupArray(keys))
     FROM (SELECT trace_id, max(start_ns) AS last,
                  groupUniqArray(intDiv(start_ns, {B})) AS keys
           FROM {DB}.spans
           WHERE start_ns >= s AND start_ns < e AND {BW}
             AND ({F})
           GROUP BY trace_id{(chr(10) + '           HAVING ' + having) if having else ''}
           ORDER BY last DESC, trace_id ASC
           LIMIT {limit})) AS top
SELECT m.trace_id, t.root_service, t.root_name, t.start_ns, t.end_ns - t.start_ns AS trace_duration_ns,
       m.last, m.matched, m.spans
FROM (SELECT trace_id, max(start_ns) AS last, count() AS matched,
             arraySlice(arraySort(x -> (x.2, x.1), groupArray((span_id, start_ns, duration_ns{extra}))), 1, {spss}) AS spans
      FROM {DB}.spans
      WHERE (intDiv(start_ns, {B}), trace_id) IN
            (SELECT arrayJoin(arrayFlatten(arrayMap((t, ks) -> arrayMap(k -> (k, t), ks), top.1, top.2))))
        AND start_ns >= s AND start_ns < e AND {BW}
        AND ({F})
      GROUP BY trace_id) AS m
LEFT JOIN (SELECT trace_id, min(start_ns) AS start_ns, max(end_ns) AS end_ns,
                  max(root_service) AS root_service, max(root_name) AS root_name
           FROM {DB}.traces
           WHERE trace_id IN (SELECT arrayJoin(top.1))
           GROUP BY trace_id) AS t USING trace_id
ORDER BY m.last DESC, m.trace_id ASC
"""
    open(f'{OUT}/{name}.sql', 'w').write(sql)

def res_in(pred):            # resource ids whose attributes satisfy pred, for the window's days
    return (f"resource_id IN (SELECT resource_id FROM {DB}.resources WHERE day >= toDate(fromUnixTimestamp64Nano({S})) "
            f"AND day <= toDate(fromUnixTimestamp64Nano({E} - 1)) AND {pred})")

search('s01_empty', '1')
search('s02_service', "service = 'checkout'", "service")
search('s03_service_error', "service = 'payment' AND status_code = 2", "service, status_code")
search('s04_status_code_ge_500', f"attrs.{p('http.response.status_code')}.:Int64 >= 500 OR attrs.{p('http.response.status_code')}.:Float64 >= 500",
       f"attrs.{p('http.response.status_code')}")
search('s05_duration_kind', "duration_ns > 2000000000 AND kind = 2", "kind")
search('s06_db_and_name', f"attrs.{p('db.system.name')}.:String = 'postgresql' AND name = 'SELECT shop'", f"name, attrs.{p('db.system.name')}")
search('s07_route_regex', f"match(attrs.{p('http.route')}.:String, '^(?:/api/auth/.*)$')", f"attrs.{p('http.route')}")
search('s08_user_point', f"attrs.{p('app.user.id')}.:String = 'u-10013'", f"attrs.{p('app.user.id')}")
search('s11_event_attr', f"arrayExists(x -> x = 'java.lang.IllegalStateException', events.attrs.{p('exception.type')}.:String)")
search('s12_select', "status_code = 2", f"attrs.{p('http.route')}, resource_id")
search('s13_count_gt', "service = 'checkout'", "service", having='count() > 5')
k = p('app.cache.hit')
unscoped = (f"multiIf(dynamicType(attrs.{k}) != 'None', attrs.{k}.:Bool = true, "
            f"{res_in(f'dynamicType(attrs.{k}) != {chr(39)}None{chr(39)}')}, {res_in(f'attrs.{k}.:Bool = true')}, "
            f"arrayExists(x -> dynamicType(x) != 'None', events.attrs.{k}), dynamicElement(arrayFirst(x -> dynamicType(x) != 'None', events.attrs.{k}), 'Bool') = true, "
            f"arrayExists(x -> dynamicType(x) != 'None', links.attrs.{k}), dynamicElement(arrayFirst(x -> dynamicType(x) != 'None', links.attrs.{k}), 'Bool') = true, "
            f"scope_attrs.{k}.:Bool = true)")
search('s15_unscoped_and_resource', f"({unscoped}) AND {res_in(p('k8s.pod.name') and 'attrs.' + p('k8s.pod.name') + '.:String = ' + chr(39) + 'cart-7d9f8b-00002' + chr(39))}",
       f"attrs.{k}, resource_id")
# trace-level intrinsics: candidates from the per-trace table
search('s14_trace_level', f"trace_id IN (SELECT trace_id FROM {DB}.traces WHERE day >= toDate(fromUnixTimestamp64Nano({S})) - 1 "
       f"AND day <= toDate(fromUnixTimestamp64Nano({E} - 1)) GROUP BY trace_id "
       f"HAVING max(root_service) = 'loadgen' AND max(end_ns) - min(start_ns) > 2000000000)")
print('ok')

# ---- structural: descendant, a recursive climb from each B span to the root ----
A, Bf = "service = 'frontend'", "service = 'payment' AND status_code = 2"
KEYS = (f"(SELECT arrayJoin(arrayFlatten(arrayMap(c -> arrayMap(k -> (k, c.1), "
        f"range(toInt64(intDiv(greatest(c.3, s), {B})), toInt64(intDiv(least(c.4, e), {B})) + 1)), cand))))")
open(f'{OUT}/s09_descendant.sql', 'w').write(f"""WITH RECURSIVE
    {S} AS s, {E} AS e,
    -- candidates: traces with at least one A span and one B span in the window,
    -- with the services and extent that locate all their spans by key
    (SELECT groupArray((trace_id, [], ts, te))
     FROM (SELECT trace_id, min(start_ns) AS ts, max(end_ns) AS te
           FROM {DB}.traces
           WHERE trace_id IN (SELECT trace_id FROM {DB}.spans
                              WHERE start_ns >= s AND start_ns < e AND {BW} AND (({A}) OR ({Bf}))
                              GROUP BY trace_id
                              HAVING countIf({A}) > 0 AND countIf({Bf}) > 0)
           GROUP BY trace_id)) AS cand,
    -- one row per (B span, ancestor reached so far); a row stops when it reaches an A span or the root
    climb AS (
        SELECT trace_id, span_id AS b, start_ns AS b_start, duration_ns AS b_dur, parent_span_id AS cur,
               toUInt8(0) AS found, 0 AS depth
        FROM {DB}.spans
        WHERE (intDiv(start_ns, {B}), trace_id) IN {KEYS}
          AND start_ns >= s AND start_ns < e AND {BW} AND ({Bf})
        UNION ALL
        SELECT c.trace_id, c.b, c.b_start, c.b_dur, x.parent_span_id, toUInt8(x.a), c.depth + 1
        FROM climb AS c
        INNER JOIN (SELECT trace_id, span_id, parent_span_id, ({A}) AS a
                    FROM {DB}.spans
                    WHERE (intDiv(start_ns, {B}), trace_id) IN {KEYS}
                      AND start_ns >= s AND start_ns < e AND {BW}) AS x
            ON x.trace_id = c.trace_id AND x.span_id = c.cur
        WHERE c.found = 0 AND c.depth < {MAXDEPTH})
SELECT trace_id, last, matched, spans, unresolved
FROM (SELECT trace_id, max(b_start) AS last, count() AS matched,
             arraySlice(arraySort(x -> (x.2, x.1), groupArray((b, b_start, b_dur))), 1, 3) AS spans,
             0 AS unresolved
      FROM (SELECT DISTINCT trace_id, b, b_start, b_dur FROM climb WHERE found = 1)
      GROUP BY trace_id
      UNION ALL
      -- a climb that reached the bound with a parent still to visit did not
      -- finish: the reader turns any such row into 422 rather than answering
      SELECT trace_id, toInt64(0), toUInt64(0), [], count() AS unresolved
      FROM climb
      WHERE found = 0 AND depth >= {MAXDEPTH} AND cur != toFixedString('', 8)
      GROUP BY trace_id)
ORDER BY unresolved DESC, last DESC, trace_id ASC
LIMIT 20
""")

# ---- structural: child ----
A2, B2 = "service = 'checkout'", "service = 'payment'"
open(f'{OUT}/s10_child.sql', 'w').write(f"""WITH {S} AS s, {E} AS e
SELECT trace_id, arrayMax(arrayMap(x -> x.2, hit)) AS last, length(hit) AS matched,
       arraySlice(arraySort(x -> (x.2, x.1), hit), 1, 3) AS spans
FROM (SELECT trace_id,
             groupArrayIf(span_id, {A2}) AS aids,
             arrayFilter(x -> has(aids, x.4), groupArrayIf((span_id, start_ns, duration_ns, parent_span_id), {B2})) AS hit
      FROM {DB}.spans
      WHERE start_ns >= s AND start_ns < e AND {BW} AND (({A2}) OR ({B2}))
      GROUP BY trace_id
      HAVING length(hit) > 0)
ORDER BY last DESC, trace_id ASC
LIMIT 20
""")

W = f"start_ns >= {S} AND start_ns < {E} AND {BW}"
# ---- tags ----
open(f'{OUT}/t01_tag_names_span.sql', 'w').write(
    f"SELECT arrayJoin(distinctJSONPaths(attrs)) AS path FROM {DB}.spans WHERE {W} ORDER BY path\n")
open(f'{OUT}/t02_tag_values_route.sql', 'w').write(f"""SELECT toString(v) AS value, dynamicType(v) AS type
FROM (SELECT attrs.{p('http.route')} AS v FROM {DB}.spans WHERE {W} AND dynamicType(v) != 'None')
GROUP BY value, type
ORDER BY value
LIMIT 5000
""")
open(f'{OUT}/t03_tag_values_filtered.sql', 'w').write(f"""SELECT toString(v) AS value, dynamicType(v) AS type
FROM (SELECT attrs.{p('k8s.pod.name')} AS v FROM {DB}.resources
      WHERE day >= toDate(fromUnixTimestamp64Nano({S})) AND day <= toDate(fromUnixTimestamp64Nano({E} - 1))
        AND resource_id IN (SELECT DISTINCT resource_id FROM {DB}.spans WHERE {W} AND service = 'cart')
        AND dynamicType(v) != 'None')
GROUP BY value, type
ORDER BY value
""")

# ---- metrics: range queries, 60 s step, right-closed buckets, one row per series ----
STEP = 60_000_000_000
S0 = (S // STEP) * STEP; E0 = -(-E // STEP) * STEP
MW = f"start_ns >= {S0} AND start_ns < {E0} AND intDiv(start_ns, {B}) BETWEEN {S0 // B} AND {(E0 - 1) // B}"
T = f"(intDiv(start_ns - 1, {STEP}) + 1) * {STEP // 1_000_000}"
def series(name, where, by, value, final=''):
    open(f'{OUT}/{name}.sql', 'w').write(f"""SELECT series, groupArray((t, v)) AS points
FROM (SELECT {by} AS series, {T} AS t, {value} AS v
      FROM {DB}.spans{final}
      WHERE {MW} AND ({where})
      GROUP BY series, t
      ORDER BY t)
GROUP BY series
ORDER BY series
""")
series('m01_rate_by_service', '1', 'service', 'count()')
series('m01b_rate_by_service_final', '1', 'service', 'count()', final=' FINAL')
series('m01c_rate_by_service_uniq', '1', 'service', 'uniqExact(trace_id, span_id)')
series('m02_quantiles_by_route', "service = 'frontend' AND kind = 2", f"attrs.{p('http.route')}.:String",
       'quantilesTDigest(0.5, 0.95, 0.99)(duration_ns)')
series('m03_errors_by_service', 'status_code = 2', 'service', 'count()')
series('m04_histogram', 'kind = 2 AND duration_ns >= 2', 'pow(2, ceil(log2(duration_ns)))', 'count()')
# by a resource attribute: aggregate per resource id, then map ids to the attribute and merge
open(f'{OUT}/m06_rate_by_resource_attr.sql', 'w').write(f"""SELECT pod AS series, groupArray((t, v)) AS points
FROM (SELECT r.pod, a.t, sum(a.n) AS v
      FROM (SELECT resource_id, {T} AS t, count() AS n
            FROM {DB}.spans WHERE {MW} GROUP BY resource_id, t) AS a
      INNER JOIN (SELECT resource_id, any(attrs.{p('k8s.pod.name')}.:String) AS pod FROM {DB}.resources
                  WHERE day >= toDate(fromUnixTimestamp64Nano({S0})) AND day <= toDate(fromUnixTimestamp64Nano({E0} - 1))
                  GROUP BY resource_id) AS r USING resource_id
      GROUP BY r.pod, a.t
      ORDER BY a.t)
GROUP BY series
ORDER BY series
""")
# instant query: one value per series over the whole window
open(f'{OUT}/m05_instant_avg_by_name.sql', 'w').write(f"""SELECT name, avg(duration_ns) AS v
FROM {DB}.spans
WHERE {W} AND service = 'frontend'
GROUP BY name
ORDER BY name
""")

# ---- service graph, straight from the span table ---------------------------
# docs/api.md 4.5: both halves are bounded [start, end) like the metrics routes,
# an edge is keyed (client, server, connectionType), and the response carries a
# truncated flag, so the statement asks for SERVICE_GRAPH_MAX_EDGES + 1 rows.
# Two join branches, because a Zipkin shared span carries BOTH halves under one
# span id: the ordinary branch pairs a server span to its parent, the shared
# branch pairs it to the client span with the same id.
GW = f"start_ns >= {S} AND start_ns < {E} AND intDiv(start_ns, {B}) BETWEEN {S // B} AND {(E - 1) // B}"
SHARED = f"coalesce(attrs.{p('zipkin.shared')}.:Bool, false)"
open(f'{OUT}/g01_service_graph.sql', 'w').write(f"""SELECT client, server, connection_type, count() AS calls,
       countIf(failed) AS failed,
       CAST(quantilesTDigest(0.5, 0.95, 0.99)(duration_ns) AS Array(Float64)) AS quantiles_ns
FROM (
    SELECT c.service AS client, s.service AS server,
           if(c.kind = 3, 'rpc', 'messaging') AS connection_type,
           (s.status_code = 2 OR c.status_code = 2) AS failed, s.duration_ns AS duration_ns
    FROM (SELECT trace_id, span_id, service, status_code, kind
          FROM {DB}.spans WHERE {GW} AND kind IN (3, 4)) AS c
    INNER JOIN (SELECT trace_id, parent_span_id, service, status_code, duration_ns, kind
                FROM {DB}.spans WHERE {GW} AND kind IN (2, 5) AND NOT {SHARED}) AS s
        ON s.trace_id = c.trace_id AND s.parent_span_id = c.span_id
    WHERE (c.kind = 3 AND s.kind = 2) OR (c.kind = 4 AND s.kind = 5)
    UNION ALL
    -- the shared-span branch: one span id carries the client and server halves
    SELECT c.service, s.service, if(c.kind = 3, 'rpc', 'messaging'),
           (s.status_code = 2 OR c.status_code = 2), s.duration_ns
    FROM (SELECT trace_id, span_id, service, status_code, kind
          FROM {DB}.spans WHERE {GW} AND kind IN (3, 4)) AS c
    INNER JOIN (SELECT trace_id, span_id, service, status_code, duration_ns, kind
                FROM {DB}.spans WHERE {GW} AND kind IN (2, 5) AND {SHARED}) AS s
        ON s.trace_id = c.trace_id AND s.span_id = c.span_id
    WHERE (c.kind = 3 AND s.kind = 2) OR (c.kind = 4 AND s.kind = 5))
GROUP BY client, server, connection_type
ORDER BY calls DESC, client ASC, server ASC, connection_type ASC
LIMIT 1001
""")

# ---- trace by id: ONE row, the spans and the trace's resources ----
def by_id(name, tid):
    open(f'{OUT}/{name}.sql', 'w').write(f"""WITH (SELECT (min(start_ns), max(end_ns))
      FROM {DB}.traces WHERE trace_id = toFixedString(unhex('{tid}'), 16)) AS ext
SELECT groupArray((span_id, parent_span_id, start_ns, duration_ns, service, resource_id, name, kind,
                   status_code, status_message, trace_state, flags, scope_name, scope_version, scope_attrs,
                   attrs, attrs_other, dropped_attrs, events, dropped_events, links, dropped_links)) AS spans,
       (SELECT groupArray((resource_id, attrs, attrs_other, dropped_attrs, schema_url))
        FROM (SELECT resource_id, any(attrs) AS attrs, any(attrs_other) AS attrs_other,
                     any(dropped_attrs) AS dropped_attrs, any(schema_url) AS schema_url
              FROM {DB}.resources
              WHERE day >= toDate(fromUnixTimestamp64Nano(ext.1)) AND day <= toDate(fromUnixTimestamp64Nano(ext.2))
                AND resource_id IN (SELECT DISTINCT resource_id FROM {DB}.spans
                                    WHERE (intDiv(start_ns, {B}), trace_id) IN
                                          (SELECT (k, toFixedString(unhex('{tid}'), 16)) FROM (SELECT arrayJoin(range(intDiv(ext.1, {B}), intDiv(ext.2, {B}) + 1)) AS k)))
              GROUP BY resource_id)) AS resources
FROM {DB}.spans
WHERE (intDiv(start_ns, {B}), trace_id) IN
      (SELECT (k, toFixedString(unhex('{tid}'), 16)) FROM (SELECT arrayJoin(range(intDiv(ext.1, {B}), intDiv(ext.2, {B}) + 1)) AS k))
""")
by_id('b01_trace_by_id_20', '50FB0CD99260AC2A15D0A6F208126742')
by_id('b02_trace_by_id_1000', '9E0AE95131B5BEDBEEA2C9EB5234F1EC')
print('ok2')

# ---- the accepted constructs the benchmark set did not exercise -------------
# One statement per construct, so every cell of the coverage table in
# docs/TraceQL/server-implementation.md 3.2 has a statement that runs.
def p2(k): return p(k)

# arithmetic on two attributes, and a field-against-field comparison
search('c01_arithmetic', f"coalesce(attrs.{p('http.request.body.size')}.:Int64, 0) + coalesce(attrs.{p('app.items.count')}.:Int64, 0) > 4000",
       f"attrs.{p('http.request.body.size')}, attrs.{p('app.items.count')}")
search('c02_field_vs_field', f"coalesce(attrs.{p('http.request.body.size')}.:Int64 > attrs.{p('app.items.count')}.:Int64, false)",
       f"attrs.{p('http.request.body.size')}, attrs.{p('app.items.count')}")
# a spanset aggregate over a projected value, and an intrinsic against an attribute
search('c03_aggregate_value', "service = 'frontend'", "duration_ns", having=f"avg(duration_ns) > 100000000")
search('c04_intrinsic_vs_attr', f"coalesce(duration_ns > attrs.{p('app.items.count')}.:Int64 * 1000000, false)", "duration_ns")

# | by(<attribute>): one spanset per group, which is one row per (trace, group)
open(f'{OUT}/c05_by_attribute.sql', 'w').write(f"""WITH {S} AS s, {E} AS e,
    (SELECT groupArray(trace_id) FROM (SELECT trace_id, max(start_ns) AS last FROM {DB}.spans
      WHERE {W} AND service = 'checkout' GROUP BY trace_id ORDER BY last DESC, trace_id ASC LIMIT 20)) AS ids
SELECT trace_id, grp, count() AS matched, max(start_ns) AS last,
       arraySlice(arraySort(x -> (x.2, x.1), groupArray((span_id, start_ns, duration_ns))), 1, 3) AS spans
FROM (SELECT trace_id, span_id, start_ns, duration_ns, toString(attrs.{p('rpc.method')}) AS grp
      FROM {DB}.spans
      WHERE (intDiv(start_ns, {B}), trace_id) IN (SELECT (arrayJoin(range(toInt64({S // B}), toInt64({(E - 1) // B}) + 1)), arrayJoin(ids)))
        AND {W} AND service = 'checkout')
GROUP BY trace_id, grp
ORDER BY last DESC, trace_id ASC, grp ASC
""")

# span:childCount, from the parent column of the same traces
open(f'{OUT}/c06_child_count.sql', 'w').write(f"""WITH {S} AS s, {E} AS e
SELECT trace_id, span_id, children
FROM (SELECT trace_id, span_id, start_ns,
             countIf(1) OVER (PARTITION BY trace_id, parent_span_id) AS siblings
      FROM {DB}.spans WHERE {W}) AS x
INNER JOIN (SELECT trace_id, parent_span_id AS span_id, count() AS children
            FROM {DB}.spans WHERE {W} AND parent_span_id != toFixedString('', 8)
            GROUP BY trace_id, parent_span_id
            HAVING children > 3) AS c USING (trace_id, span_id)
ORDER BY start_ns DESC, span_id ASC
LIMIT 20
""")

# nested-set numbering: depth and parent index, computed by the same climb the
# descendant operator uses, over the spans of one trace
open(f'{OUT}/c07_nested_set.sql', 'w').write(f"""WITH RECURSIVE
    seed AS (SELECT trace_id, span_id, parent_span_id, start_ns, 0 AS depth
             FROM {DB}.spans
             WHERE (intDiv(start_ns, {B}), trace_id) IN
                   (SELECT (arrayJoin(range(toInt64({S // B}), toInt64({(E - 1) // B}) + 1)), toFixedString(unhex('50FB0CD99260AC2A15D0A6F208126742'), 16)))
               AND {W} AND parent_span_id = toFixedString('', 8)
             UNION ALL
             SELECT c.trace_id, c.span_id, c.parent_span_id, c.start_ns, p.depth + 1
             FROM {DB}.spans AS c
             INNER JOIN seed AS p ON p.trace_id = c.trace_id AND p.span_id = c.parent_span_id
             WHERE (intDiv(c.start_ns, {B}), c.trace_id) IN
                   (SELECT (arrayJoin(range(toInt64({S // B}), toInt64({(E - 1) // B}) + 1)), toFixedString(unhex('50FB0CD99260AC2A15D0A6F208126742'), 16)))
               AND c.start_ns >= {S} AND c.start_ns < {E})
SELECT depth, count() AS spans FROM seed GROUP BY depth ORDER BY depth
""")

# | compare(): the attribute distribution of a selection against a baseline.
#
# Three things about this statement are the answer to a wrong one it replaces.
#
# 1. It counts SPANS, not resources. A resource is shared by many spans, and a
#    span is in the selection or the baseline on its own account; grouping the
#    resource attributes by resource_id (what this generator used to do) put a
#    shared resource wholly on the selection side and left the baseline empty.
#    Trace ee07 of `fixture/make_edge_fixture.py` is one resource under one
#    selection span and two baseline spans, and tells the two apart.
# 2. The key universe is every attribute the span exposes plus the intrinsics
#    the reference reports, not three intrinsics. The reference walks the whole
#    attribute set (`tempodb/encoding/vparquet4/block_traceql.go:107-125` @
#    v3.0.2 - trace, resource, instrumentation, span, event and link
#    attributes) and skips only duration-typed values and six intrinsics
#    (`pkg/traceql/engine_metrics_compare.go:143-159`): span start time, trace
#    id, parent id and the three nested-set values.
#    PulsusDB also leaves out `span:id`, which the reference includes: it is
#    unique per span, so it can only return `topN` arbitrary spans, and the
#    reference's own comment gives "the cardinality isn't useful" as the reason
#    for the six it skips. Ledger row and `docs/api.md` entry: "compare() omits
#    span:id".
# 3. topN is per key AND per side, applied before any cap on the statement.
#    A global LIMIT after the counts drops whole keys and cannot be repaired by
#    the response layer: the rows are already gone.
#
# The value carries its stored type, because compare() counts a value per type:
# integer 1 and double 1.0 render the same text and are different values.
COMPARE_TOPN = 10

def compare(name, scope_filter, selection):
    def kv_rows(src, scope, expr):
        """(key, raw value) pairs out of one JSON column, as its own branch."""
        return f"""    SELECT sel, '{scope}' AS scope, kv.1 AS key,
           if(startsWith(kv.2, '"'), JSONExtractString(kv.2), kv.2) AS value,
           multiIf(startsWith(kv.2, '"'), 'string', kv.2 IN ('true', 'false'), 'bool',
                   match(kv.2, '[.eE]'), 'double', 'int') AS type
    FROM {src}
    ARRAY JOIN JSONExtractKeysAndValuesRaw(toString({expr})) AS kv"""

    base = f"""(SELECT {selection} AS sel, name, kind, status_code, status_message,
               scope_name, scope_version, attrs, scope_attrs, events, links,
               resource_id, service, trace_id
        FROM {DB}.spans WHERE {W} AND {scope_filter})"""
    body = f"""WITH base AS {base},
     res AS (SELECT resource_id, any(attrs) AS rattrs FROM {DB}.resources GROUP BY resource_id),
     tr AS (SELECT trace_id, max(root_service) AS root_service, max(root_name) AS root_name
            FROM {DB}.traces GROUP BY trace_id),
     kv AS (
{kv_rows('base', 'span', 'attrs')}
    UNION ALL
{kv_rows('base', 'instrumentation', 'scope_attrs')}
    UNION ALL
{kv_rows('(SELECT b.sel AS sel, r.rattrs AS rattrs FROM base AS b INNER JOIN res AS r USING (resource_id))', 'resource', 'rattrs')}
    UNION ALL
{kv_rows('(SELECT sel, ev.3 AS eattrs FROM base ARRAY JOIN events AS ev)', 'event', 'eattrs')}
    UNION ALL
{kv_rows('(SELECT sel, lk.5 AS lattrs FROM base ARRAY JOIN links AS lk)', 'link', 'lattrs')}
    UNION ALL
    -- the service name lives on the span row, not inside the resource JSON (R1)
    SELECT sel, 'resource', 'service.name', service, 'string' FROM base
    UNION ALL
    SELECT sel, 'intrinsic', k, v, 'string' FROM
        (SELECT sel, ['name', 'kind', 'status', 'statusMessage',
                      'instrumentation:name', 'instrumentation:version'] AS ks,
                [toString(name),
                 -- the stored codes render as the keywords the API returns
                 -- (`crates/pulsus-read/src/traces/search_eval.rs:2318-2337`)
                 arrayElement(['unspecified', 'internal', 'server', 'client', 'producer', 'consumer'],
                              least(toInt32(kind), 5) + 1),
                 arrayElement(['unset', 'ok', 'error'], least(toInt32(status_code), 2) + 1),
                 status_message, toString(scope_name), toString(scope_version)] AS vs
         FROM base)
    ARRAY JOIN ks AS k, vs AS v
    UNION ALL
    SELECT sel, 'intrinsic', k, v, 'string' FROM
        (SELECT b.sel AS sel, ['trace:rootService', 'trace:rootName'] AS ks,
                [toString(t.root_service), toString(t.root_name)] AS vs
         FROM base AS b INNER JOIN tr AS t USING (trace_id))
    ARRAY JOIN ks AS k, vs AS v
    UNION ALL
    SELECT sel, 'event', 'name', toString(ev.2), 'string' FROM base ARRAY JOIN events AS ev
    UNION ALL
    SELECT sel, 'link', k, v, 'string' FROM
        (SELECT sel, ['traceId', 'spanId'] AS ks,
                [lower(hex(lk.1)), lower(hex(lk.2))] AS vs FROM base ARRAY JOIN links AS lk)
    ARRAY JOIN ks AS k, vs AS v)
SELECT scope, key, value, type, side, n
FROM (SELECT scope, key, value, type, if(sel, 'selection', 'baseline') AS side, count() AS n,
             row_number() OVER (PARTITION BY scope, key, side ORDER BY count() DESC, value ASC) AS rn
      FROM kv GROUP BY scope, key, value, type, side)
WHERE rn <= {COMPARE_TOPN}
ORDER BY scope, key, side, n DESC, value
"""
    open(f'{OUT}/{name}.sql', 'w').write(body)

compare('c08_compare', "service = 'payment'", 'status_code = 2')

# metrics with exemplars: one sampled (trace, span, value) per bucket per series
open(f'{OUT}/c09_exemplars.sql', 'w').write(f"""SELECT series, groupArray((t, v)) AS points, groupArray((t, ex_trace, ex_span, ex_v)) AS exemplars
FROM (SELECT service AS series, {T} AS t, count() AS v,
             argMax(lower(hex(trace_id)), start_ns) AS ex_trace,
             argMax(lower(hex(span_id)), start_ns) AS ex_span,
             toFloat64(argMax(duration_ns, start_ns)) AS ex_v
      FROM {DB}.spans
      WHERE {MW}
      GROUP BY series, t
      ORDER BY t)
GROUP BY series ORDER BY series
""")

# topk over a range metric: the second stage runs on the series, in the database
open(f'{OUT}/c10_topk.sql', 'w').write(f"""SELECT series, points
FROM (SELECT series, groupArray((t, v)) AS points, sum(v) AS total
      FROM (SELECT service AS series, {T} AS t, count() AS v
            FROM {DB}.spans WHERE {MW} GROUP BY series, t ORDER BY t)
      GROUP BY series)
ORDER BY total DESC, series ASC
LIMIT 3
""")

# tag names and unnarrowed tag values: the time-less catalogs of docs/api.md 4.3
open(f'{OUT}/c11_tag_names_catalog.sql', 'w').write(
    f"SELECT scope, key FROM {DB}.tag_names FINAL WHERE scope IN ('span','resource','event','link','instrumentation') ORDER BY scope, key LIMIT 10001\n")
open(f'{OUT}/c12_tag_values_catalog.sql', 'w').write(
    f"SELECT value, val_type FROM {DB}.tag_values FINAL WHERE scope = 'span' AND key = 'http.route' ORDER BY value LIMIT 1001\n")
# the one intrinsic whose values come from the store, bounded by the window
open(f'{OUT}/c13_name_values.sql', 'w').write(
    f"SELECT DISTINCT name FROM {DB}.spans WHERE {W} ORDER BY name LIMIT 1001\n")
print('ok3')

# nested-set numbering for one trace, in the retained entry/exit convention: one
# counter is incremented on entry and on exit, so n spans occupy 1..2n.
#   left   = 2 * dfs_position - 1 - depth
#   right  = left + 2 * subtree_size - 1
#   parent = the parent's left, and -1 at a root
#
# The numbering is TOTAL over the stored spans of the trace and does not depend
# on the order rows come back in. Three shapes make that a real constraint, and
# `fixture/make_edge_fixture.py` holds one trace for each:
#
#   same-time siblings  two children of one parent with the SAME start_ns. The
#                       walk carries (start_ns, span_id) pairs, not start_ns, so
#                       the order is total and a subtree's rows are exactly the
#                       rows whose path begins with its own.
#   an orphan           a span whose parent is not stored in the window. It is a
#                       root of the hydrated forest, the same rule the retained
#                       implementation applies
#                       (`crates/pulsus-read/src/traces/search_eval.rs:2100-2118`).
#   a cycle             every span of a cycle has a stored parent, so no member
#                       is a forest root and the forest walk never reaches it.
#                       The retained implementation promotes the ascending-first
#                       unvisited span to a root and drains again
#                       (`search_eval.rs:2124-2139`). A span X is promoted
#                       exactly when no earlier span reaches it, and within the
#                       unvisited set the spans that reach X are its own parent
#                       chain - so X is promoted iff X is the smallest
#                       (start_ns, span_id) on its parent closure. That is one
#                       more bounded climb, and it needs no iteration.
#
# The walk is bounded by MAX_SPANS_PER_TRACE (`crates/pulsus-read/src/traces/
# exec.rs:125`), which is deeper than ClickHouse's default recursive-CTE depth,
# so the statement carries the setting it needs. Any span the two passes still
# do not number is counted in `unnumbered`, which is a column of the one row
# this statement returns, so it cannot be lost.
MAXSPANS = 10_000

def nested_set(name, tid, detail=False):
    """`detail` returns one row per span instead of the aggregate, which is how
    the awkward-topology traces are checked span by span."""
    final = ("""SELECT lower(hex(span_id)) AS span, nested_set_left AS left,
       nested_set_right AS right, nested_set_parent AS parent, depth
FROM""" if detail else """SELECT count() AS spans, max(depth) AS max_depth,
       min(nested_set_left) AS min_left, max(nested_set_right) AS max_right,
       uniqExact(nested_set_left) AS distinct_left,
       countIf(nested_set_parent < 0) AS roots,
       (SELECT count() FROM sp WHERE span_id NOT IN (SELECT span_id FROM tour)) AS unnumbered
FROM""")
    order = '\nORDER BY left ASC' if detail else ''
    open(f'{OUT}/{name}.sql', 'w').write(f"""WITH RECURSIVE
    {S} AS s, {E} AS e,
    keys AS (SELECT (arrayJoin(range(toInt64({S // B}), toInt64({(E - 1) // B}) + 1)), toFixedString(unhex('{tid}'), 16)) AS k),
    sp AS (SELECT span_id, parent_span_id, start_ns FROM {DB}.spans
           WHERE (intDiv(start_ns, {B}), trace_id) IN (SELECT k FROM keys)
             AND start_ns >= s AND start_ns < e),
    roots AS (SELECT span_id, parent_span_id, start_ns FROM sp
              WHERE parent_span_id = toFixedString('', 8)
                 OR parent_span_id NOT IN (SELECT span_id FROM sp)),
    walk AS (
        SELECT span_id, parent_span_id, [(start_ns, span_id)] AS path, 0 AS depth, 0 AS phase
        FROM roots
        UNION ALL
        SELECT c.span_id, c.parent_span_id, arrayConcat(p.path, [(c.start_ns, c.span_id)]),
               p.depth + 1, p.phase
        FROM sp AS c INNER JOIN walk AS p ON p.span_id = c.parent_span_id
        WHERE p.depth < {MAXSPANS} AND NOT has(arrayMap(x -> x.2, p.path), c.span_id)),
    un AS (SELECT span_id, parent_span_id, start_ns FROM sp
           WHERE span_id NOT IN (SELECT span_id FROM walk)),
    up AS (
        SELECT span_id AS x, parent_span_id AS cur, [(start_ns, span_id)] AS seen,
               (start_ns, span_id) AS mk, 0 AS d
        FROM un
        UNION ALL
        SELECT u.x, n.parent_span_id, arrayConcat(u.seen, [(n.start_ns, n.span_id)]),
               least(u.mk, (n.start_ns, n.span_id)), u.d + 1
        FROM up AS u INNER JOIN un AS n ON n.span_id = u.cur
        WHERE u.d < {MAXSPANS} AND NOT has(arrayMap(y -> y.2, u.seen), n.span_id)),
    promoted AS (SELECT u.span_id AS span_id, u.parent_span_id AS parent_span_id, u.start_ns AS start_ns
                 FROM un AS u
                 INNER JOIN (SELECT x, min(mk) AS mk FROM up GROUP BY x) AS c ON c.x = u.span_id
                 WHERE c.mk = (u.start_ns, u.span_id)),
    walk2 AS (
        SELECT span_id, parent_span_id, [(start_ns, span_id)] AS path, 0 AS depth, 1 AS phase
        FROM promoted
        UNION ALL
        SELECT c.span_id, c.parent_span_id, arrayConcat(p.path, [(c.start_ns, c.span_id)]),
               p.depth + 1, p.phase
        FROM un AS c INNER JOIN walk2 AS p ON p.span_id = c.parent_span_id
        WHERE p.depth < {MAXSPANS} AND NOT has(arrayMap(x -> x.2, p.path), c.span_id)),
    tour AS (SELECT span_id, parent_span_id, depth, path, phase FROM walk
             UNION ALL
             SELECT span_id, parent_span_id, depth, path, phase FROM walk2),
    ordered AS (SELECT span_id, parent_span_id, depth, path, phase,
                       row_number() OVER (ORDER BY phase ASC, path ASC) AS r
                FROM tour),
    sized AS (SELECT o.span_id AS span_id, o.parent_span_id AS parent_span_id,
                     o.depth AS depth, o.r AS r, o.phase AS phase, count() AS subtree
              FROM ordered AS o
              INNER JOIN ordered AS d
                  ON d.phase = o.phase AND arraySlice(d.path, 1, length(o.path)) = o.path
              GROUP BY o.span_id, o.parent_span_id, o.depth, o.r, o.phase),
    numbered AS (SELECT span_id, parent_span_id, depth, subtree, phase,
                        2 * r - 1 - depth AS nested_set_left,
                        nested_set_left + 2 * subtree - 1 AS nested_set_right
                 FROM sized)
{final} (SELECT n.span_id AS span_id, n.depth AS depth, n.nested_set_left AS nested_set_left,
             n.nested_set_right AS nested_set_right,
             if(n.parent_span_id = toFixedString('', 8)
                OR n.parent_span_id NOT IN (SELECT span_id FROM ordered)
                OR n.span_id IN (SELECT span_id FROM promoted),
                -1, p.nested_set_left) AS nested_set_parent
      FROM numbered AS n
      LEFT JOIN numbered AS p ON p.span_id = n.parent_span_id){order}
SETTINGS max_recursive_cte_evaluation_depth = {MAXSPANS + 1}
""")

nested_set('c14_nested_set_numbering', '9E0AE95131B5BEDBEEA2C9EB5234F1EC')
nested_set('c15_nested_set_small', '50FB0CD99260AC2A15D0A6F208126742')
# any further trace ids on the command line get their own numbering statement,
# which is how `edge_checks.sh` numbers the awkward-topology traces
for _tid in sys.argv[5:]:
    nested_set(f'nested_{_tid.lower()}', _tid.upper())
    nested_set(f'nested_{_tid.lower()}_detail', _tid.upper(), detail=True)

# ---- the two nested-set shapes the query path actually uses ----------------
# Numbering every trace in a window is not possible at corpus scale: the
# recursive walk re-reads the span set once per iteration, and over the 3-hour
# corpus it exhausted a 6 GB server after 2 m 12 s (measured). The query path
# therefore never numbers a window. Two of the three shapes clients send need no
# numbering at all, and this is the first:
#   { nestedSetParent < 0 }  =  a root of the hydrated forest  =  no stored
#   parent. `grafana/explore_root_rate_by_service` and
#   `grafana/explore_root_rate_sample` in the corpus are both this shape.
open(f'{OUT}/c20_roots.sql', 'w').write(f"""WITH {S} AS s, {E} AS e
SELECT trace_id, span_id, start_ns, duration_ns
FROM {DB}.spans AS x
WHERE {W}
  AND (x.parent_span_id = toFixedString('', 8)
       OR (x.trace_id, x.parent_span_id) NOT IN
          (SELECT trace_id, span_id FROM {DB}.spans WHERE {W}))
ORDER BY start_ns DESC
LIMIT 20
""")
# and this is the second pass of the third shape: any other nested-set
# comparison runs the search without it, then hydrates the candidate traces
# whole, and the reader numbers those spans with the retained Euler tour.
open(f'{OUT}/c21_hydrate_candidates.sql', 'w').write(f"""WITH {S} AS s, {E} AS e,
    (SELECT (groupArray(trace_id), groupArray(keys))
     FROM (SELECT trace_id, max(start_ns) AS last,
                  groupUniqArray(intDiv(start_ns, {B})) AS keys
           FROM {DB}.spans
           WHERE {W} AND service = 'checkout'
           GROUP BY trace_id
           ORDER BY last DESC, trace_id ASC
           LIMIT 20)) AS top
SELECT trace_id, span_id, parent_span_id, start_ns, duration_ns
FROM {DB}.spans
WHERE (intDiv(start_ns, {B}), trace_id) IN
      (SELECT arrayJoin(arrayFlatten(arrayMap((t, ks) -> arrayMap(k -> (k, t), ks), top.1, top.2))))
  AND {W}
""")

# ---- the condition forms the review found missing --------------------------
search('c16_truthiness', f"coalesce(attrs.{p('app.cache.hit')}.:Bool = true, false)", f"attrs.{p('app.cache.hit')}")
search('c17_presence', f"dynamicType(attrs.{p('app.tags')}) != 'None'", f"attrs.{p('app.tags')}")
search('c18_absence', f"dynamicType(attrs.{p('app.tags')}) = 'None'")
# a filter written as a LATER pipeline element, after an aggregate stage:
# the aggregate is the HAVING of the first pass, the later filter a predicate of
# the detail pass, both inside one statement
search('c19_mid_pipeline_filter', "service = 'checkout'", "service, status_code", having="count() > 5")

# ---- all fifteen structural forms: 5 operators x 3 modifiers ----------------
# A = { resource.service.name = "frontend" }, B = { resource.service.name = "payment" }
# plain   : the B spans the relation holds for
# negated : the B spans it does not hold for
# union   : both sides of each holding pair
SA, SB = "service = 'frontend'", "service = 'payment' AND status_code = 2"

# The union modifier returns BOTH sides of every pair the relation holds for, so
# its partner set is relation-specific: the parent of a child hit, the children
# of a parent hit, the other children of a sibling hit's parent. One expression
# for all three (what this generator used to emit) adds the wrong A span and
# drops the sibling — `fixture/make_edge_fixture.py` trace ee06 tells them apart.
#   tuple fields: 1 span_id, 2 start_ns, 3 duration_ns, 4 parent_span_id
PARTNER = {
    'child':   "arrayFilter(y -> has(arrayMap(x -> x.4, b_hit), y.1), a_spans)",
    'parent':  "arrayFilter(y -> has(arrayMap(x -> x.1, b_hit), y.4), a_spans)",
    'sibling': ("arrayFilter(y -> has(arrayMap(x -> x.4, b_hit), y.4) "
                "AND NOT has(arrayMap(x -> x.1, b_hit), y.1), a_spans)"),
}

def struct_set(name, keep, union=None):
    """child / parent / sibling: one grouped pass over the spans matching either
    side. `union` is the relation name when the union modifier is asked for."""
    body = f"""WITH {S} AS s, {E} AS e
SELECT trace_id, arrayMax(arrayMap(x -> x.2, hit)) AS last, length(hit) AS matched,
       arraySlice(arraySort(x -> (x.2, x.1), hit), 1, 3) AS spans
FROM (SELECT trace_id,
             groupArrayIf(span_id, {SA}) AS a_ids,
             groupArrayIf(parent_span_id, {SA}) AS a_par,
             groupArrayIf((span_id, start_ns, duration_ns, parent_span_id), {SA}) AS a_spans,
             groupArrayIf((span_id, start_ns, duration_ns, parent_span_id), {SB}) AS b_spans,
             {keep} AS b_hit,
             {f"arrayConcat(b_hit, {PARTNER[union]})" if union else "b_hit"} AS hit
      FROM {DB}.spans
      WHERE {W} AND (({SA}) OR ({SB}))
      GROUP BY trace_id
      HAVING length(hit) > 0)
ORDER BY last DESC, trace_id ASC
LIMIT 20
"""
    open(f'{OUT}/{name}.sql', 'w').write(body)

# child: a B span whose direct parent matches A
struct_set('st01_child_plain',  "arrayFilter(x -> has(a_ids, x.4), b_spans)")
struct_set('st02_child_neg',    "arrayFilter(x -> NOT has(a_ids, x.4), b_spans)")
struct_set('st03_child_union',  "arrayFilter(x -> has(a_ids, x.4), b_spans)", union='child')
# parent: a B span that is the direct parent of an A span
struct_set('st04_parent_plain', "arrayFilter(x -> has(a_par, x.1), b_spans)")
struct_set('st05_parent_neg',   "arrayFilter(x -> NOT has(a_par, x.1), b_spans)")
struct_set('st06_parent_union', "arrayFilter(x -> has(a_par, x.1), b_spans)", union='parent')
# sibling: a B span sharing a parent with an A span, and not that A span itself
struct_set('st07_sibling_plain', "arrayFilter(x -> has(a_par, x.4) AND NOT has(a_ids, x.1), b_spans)")
struct_set('st08_sibling_neg',   "arrayFilter(x -> NOT (has(a_par, x.4) AND NOT has(a_ids, x.1)), b_spans)")
struct_set('st09_sibling_union', "arrayFilter(x -> has(a_par, x.4) AND NOT has(a_ids, x.1), b_spans)", union='sibling')

def struct_climb(name, direction, mode):
    """descendant / ancestor: the bounded recursive climb, over the candidate
    traces the compiled plan restricts to (sql/s09_descendant.sql).

    The restriction is relation-dependent, and getting it wrong loses rows.
    A trace with no A span at all answers the NEGATED form with every one of
    its B spans - the reference evaluates the operation with an empty left
    operand and `falseForAll`, so every B span qualifies
    (`pkg/traceql/ast_execute.go:114-119` and
    `tempodb/encoding/vparquet4/block_traceql.go:296-325` @ v3.0.2). For the
    plain and union forms an empty left operand yields nothing, so there both
    sides must be present. Trace ee07 of `fixture/make_edge_fixture.py` holds
    B spans and no A span, and tells the two rules apart."""
    seed_filter, reach = (SB, SA) if direction == 'descendant' else (SA, SB)
    # both forms return B spans: the descendant form seeds from them, the
    # ancestor form reaches them.
    cand_having = (f"countIf({SB}) > 0" if mode == 'neg'
                   else f"countIf({SA}) > 0 AND countIf({SB}) > 0")
    cand = f"""(SELECT groupArray((trace_id, ts, te)) FROM
        (SELECT trace_id, min(start_ns) AS ts, max(end_ns) AS te FROM {DB}.traces
         WHERE trace_id IN (SELECT trace_id FROM {DB}.spans WHERE {W} AND (({SA}) OR ({SB}))
                            GROUP BY trace_id HAVING {cand_having})
         GROUP BY trace_id))"""
    keys = (f"(SELECT arrayJoin(arrayFlatten(arrayMap(c -> arrayMap(k -> (k, c.1), "
            f"range(toInt64(intDiv(greatest(c.2, {S}), {B})), toInt64(intDiv(least(c.3, {E} - 1), {B})) + 1)), cand))))")
    inwin = f"(intDiv(start_ns, {B}), trace_id) IN {keys} AND {W}"
    stmt = f"""WITH RECURSIVE {cand} AS cand,
    climb AS (
        SELECT trace_id, span_id AS seed, parent_span_id AS cur, 0 AS depth
        FROM {DB}.spans WHERE {inwin} AND ({seed_filter})
        UNION ALL
        SELECT c.trace_id, c.seed, x.parent_span_id, c.depth + 1
        FROM climb AS c
        INNER JOIN (SELECT trace_id, span_id, parent_span_id FROM {DB}.spans WHERE {inwin}) AS x
            ON x.trace_id = c.trace_id AND x.span_id = c.cur
        WHERE c.depth < {MAXDEPTH} - 1),
    pairs AS (SELECT DISTINCT c.trace_id AS trace_id, c.seed AS seed, c.cur AS other
              FROM climb AS c
              INNER JOIN (SELECT trace_id, span_id FROM {DB}.spans WHERE {inwin} AND ({reach})) AS r
                  ON r.trace_id = c.trace_id AND r.span_id = c.cur),
    overflow AS (SELECT count() AS unresolved
                 FROM climb AS c
                 INNER JOIN (SELECT trace_id, span_id, parent_span_id FROM {DB}.spans WHERE {inwin}) AS x
                     ON x.trace_id = c.trace_id AND x.span_id = c.cur
                 WHERE c.depth = {MAXDEPTH} - 1 AND x.parent_span_id != toFixedString('', 8))
"""
    # which spans the form returns
    seed_side = f"""SELECT trace_id, span_id, start_ns, duration_ns FROM {DB}.spans
              WHERE {inwin} AND ({seed_filter}) AND (trace_id, span_id) IN (SELECT trace_id, seed FROM pairs)"""
    other_side = f"""SELECT trace_id, span_id, start_ns, duration_ns FROM {DB}.spans
              WHERE {inwin} AND ({reach}) AND (trace_id, span_id) IN (SELECT trace_id, other FROM pairs)"""
    # descendant returns the seed (the B span); ancestor returns the visited B span
    hit = seed_side if direction == 'descendant' else other_side
    miss_all = (f"SELECT trace_id, span_id, start_ns, duration_ns FROM {DB}.spans WHERE {inwin} AND "
                f"({seed_filter if direction == 'descendant' else reach})")
    if mode == 'plain':
        rows = hit
    elif mode == 'neg':
        rows = f"""SELECT trace_id, span_id, start_ns, duration_ns FROM ({miss_all})
              WHERE (trace_id, span_id) NOT IN (SELECT trace_id, span_id FROM ({hit}))"""
    else:
        partner = other_side if direction == 'descendant' else seed_side
        rows = f"""SELECT trace_id, span_id, start_ns, duration_ns FROM ({hit})
              UNION ALL
              SELECT trace_id, span_id, start_ns, duration_ns FROM ({partner})"""
    # The overflow count is its own row, not a column of the matches: a climb
    # cut by the bound must reach the reader even when nothing matched, and a
    # scalar in the SELECT list of a grouped query disappears with the last
    # group. The reader turns a non-zero `unresolved` into 422 whatever else
    # the statement returned.
    stmt += f"""SELECT * FROM (
    (SELECT 'match' AS row_kind, trace_id, max(start_ns) AS last, count() AS matched,
            arraySlice(arraySort(x -> (x.2, x.1), groupArray((span_id, start_ns, duration_ns))), 1, 3) AS spans,
            toUInt64(0) AS unresolved
     FROM ({rows})
     GROUP BY trace_id
     ORDER BY last DESC, trace_id ASC
     LIMIT 20)
    UNION ALL
    (SELECT 'overflow', toFixedString('', 16), toInt64(0), toUInt64(0),
            CAST([], 'Array(Tuple(FixedString(8), Int64, Int64))'),
            (SELECT unresolved FROM overflow)))
"""
    open(f'{OUT}/{name}.sql', 'w').write(stmt)

struct_climb('st10_descendant_plain', 'descendant', 'plain')
struct_climb('st11_descendant_neg',   'descendant', 'neg')
struct_climb('st12_descendant_union', 'descendant', 'union')
struct_climb('st13_ancestor_plain',   'ancestor',   'plain')
struct_climb('st14_ancestor_neg',     'ancestor',   'neg')
struct_climb('st15_ancestor_union',   'ancestor',   'union')
print('ok-structural')
