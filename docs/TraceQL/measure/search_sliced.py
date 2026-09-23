#!/usr/bin/env python3
"""The newest-slice-first search loop PulsusDB runs for a spanset filter: one
statement per slice, newest first, slice widths 5m, 10m, 20m, ... until LIMIT
traces are found or the window is exhausted. Each statement is the single-
statement search of sql/s02_service.sql with the FIRST pass bounded to the
slice (and to traces not yet returned) and the SECOND pass bounded to the whole
window, located by the per-trace table's extent and services. The answer is
exactly the whole-window answer (see docs/TraceQL/sql-schema.md).
Usage: search_sliced.py CH_URL DB START_NS END_NS NAME FILTER [PROJ] [REPS]
Prints: name statements rows_read bytes_returned ms_median"""
import sys, time, statistics, urllib.request
ch, DB, S, E, name, F = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), sys.argv[5], sys.argv[6]
proj = sys.argv[7] if len(sys.argv) > 7 else ''
reps = int(sys.argv[8]) if len(sys.argv) > 8 else 5
RUN = f'{time.time_ns()}'
B, K, SPSS, FIRST = 300_000_000_000, 20, 3, 300_000_000_000
extra = (', ' + proj) if proj else ''
def stmt(lo, hi, seen):
    excl = f"AND trace_id NOT IN ({','.join(seen)})" if seen else ''
    return f"""WITH {S} AS s, {E} AS e, {lo} AS lo, {hi} AS hi,
    (SELECT groupArray(trace_id) FROM (SELECT trace_id, max(start_ns) AS last FROM {DB}.spans
      WHERE start_ns >= lo AND start_ns < hi AND intDiv(start_ns, {B}) BETWEEN {lo // B} AND {(hi - 1) // B} AND ({F}) {excl}
      GROUP BY trace_id ORDER BY last DESC, trace_id ASC LIMIT {K})) AS ids,
    (SELECT groupArray((trace_id, services, ts, te, rs, rn)) FROM (SELECT trace_id, groupUniqArrayArray(services) AS services,
      min(start_ns) AS ts, max(end_ns) AS te, max(root_service) AS rs, max(root_name) AS rn
      FROM {DB}.traces WHERE trace_id IN (SELECT arrayJoin(ids)) GROUP BY trace_id)) AS tr
SELECT hex(trace_id) AS tid, max(start_ns) AS last, count() AS matched,
       arraySlice(arraySort(x -> (x.2, x.1), groupArray((span_id, start_ns, duration_ns{extra}))), 1, {SPSS}) AS spans,
       any(arrayFirst(x -> x.1 = trace_id, tr)) AS header
FROM {DB}.spans
WHERE (intDiv(start_ns, {B}), service, trace_id) IN
      (SELECT arrayJoin(arrayFlatten(arrayMap(c -> arrayFlatten(arrayMap(k -> arrayMap(sv -> (k, sv, c.1), c.2),
              range(toInt64(intDiv(greatest(c.3, s), {B})), toInt64(intDiv(least(c.4, e), {B})) + 1))), tr))))
  AND start_ns >= s AND start_ns < e AND intDiv(start_ns, {B}) BETWEEN {S // B} AND {(E - 1) // B} AND ({F})
GROUP BY trace_id
ORDER BY last DESC, trace_id ASC
FORMAT TSV SETTINGS final = 1"""
def q(sql, qid):
    return urllib.request.urlopen(urllib.request.Request(f'{ch}/?query_id={qid}', data=sql.encode()), timeout=300).read()
def run(tag):
    found, seen, n, ids, size = [], [], 0, [], 0
    hi, width = E, FIRST
    while len(found) < K and hi > S:
        lo = max(S, hi - width)
        qid = f'tqd-sl-{RUN}-{name}-{tag}-{n}'; ids.append(qid)
        out = q(stmt(lo, hi, seen), qid); size += len(out)
        rows = [l for l in out.decode(errors='replace').split('\n') if l]
        for r in rows:
            tid = r.split('\t')[0]
            found.append(tid)
        seen += [f"unhex('{r.split(chr(9))[0]}')" for r in rows]
        n += 1; hi = lo; width *= 2
    return n, ids, size
times = []
for i in range(reps + 1):
    t0 = time.perf_counter(); n, ids, size = run(i); ms = (time.perf_counter() - t0) * 1000
    if i: times.append(ms)
q('SYSTEM FLUSH LOGS', 'tqd-flush-' + str(time.time()))
rr = q(f"SELECT sum(read_rows) FROM system.query_log WHERE type='QueryFinish' AND query_id IN ({','.join(repr(x) for x in ids)}) FORMAT TSV", 'tqd-ql-' + str(time.time())).decode().strip()
print(f'{name}\t{n}\t{rr}\t{size}\t{statistics.median(times):.0f}')
