#!/usr/bin/env python3
"""For every query of the worked fixture, prints the answer the compiled SQL
gives on the proposed schema and the answer the reference gives over HTTP, as
trace id -> matched span ids. Usage:
  fixture_answers.py CH_URL DB TEMPO_URL START_S END_S"""
import json, sys, urllib.request, urllib.parse
ch, DB, tempo, S, E = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), int(sys.argv[5])
SNS, ENS, B = S * 10**9, E * 10**9, 300_000_000_000
W = f"start_ns >= {SNS} AND start_ns < {ENS} AND intDiv(start_ns, {B}) BETWEEN {SNS // B} AND {(ENS - 1) // B}"
def p(k): return '`' + k.replace('%', '%25').replace('.', '%2E') + '`'
def q(sql):
    return urllib.request.urlopen(urllib.request.Request(ch, data=(sql + ' SETTINGS final = 1').encode()), timeout=300).read().decode().strip()
def ours(pred, table='spans'):
    sql = (f"SELECT concat(lower(hex(trace_id)), ':', arrayStringConcat(arraySort(groupArray(lower(hex(span_id)))), ',')) "
           f"FROM {DB}.{table} WHERE {W} AND ({pred}) GROUP BY trace_id ORDER BY max(start_ns) DESC FORMAT TSV")
    return ' | '.join(q(sql).split('\n')) or '(none)'
def ref(tq):
    u = tempo + '/api/search?' + urllib.parse.urlencode({'q': tq, 'start': S, 'end': E, 'limit': 20, 'spss': 20})
    d = json.load(urllib.request.urlopen(u, timeout=300))
    out = []
    for t in d.get('traces', []):
        ids = sorted({s['spanID'] for ss in t.get('spanSets', []) for s in ss.get('spans', [])})
        out.append(f"{t['traceID'].rjust(32, '0')}:{','.join(ids)}")
    return ' | '.join(out) or '(none)'
CASES = [
 ('F1  {}', '{}', '1'),
 ('F2  service', '{ resource.service.name = "payment" }', "service = 'payment'"),
 ('F3  status error', '{ status = error }', 'status_code = 2'),
 ('F4  int >= 500', '{ span.http.response.status_code >= 500 }',
  f"coalesce(attrs.{p('http.response.status_code')}.:Int64 >= 500, false) OR coalesce(attrs.{p('http.response.status_code')}.:Float64 >= 500, false)"),
 ('F5  string "200"', '{ span.http.response.status_code = "200" }', f"attrs.{p('http.response.status_code')}.:String = '200'"),
 ('F6  float > 0.2', '{ span.app.discount.ratio > 0.2 }',
  f"coalesce(attrs.{p('app.discount.ratio')}.:Float64 > 0.2, false) OR coalesce(attrs.{p('app.discount.ratio')}.:Int64 > 0.2, false)"),
 ('F7  array member', '{ span.app.tags = "gold" }', f"has(attrs.{p('app.tags')}.:`Array(Nullable(String))`, 'gold')"),
 ('F8  bool false', '{ span.app.cache.hit = false }', f"attrs.{p('app.cache.hit')}.:Bool = false"),
 ('F9  duration > 1s', '{ duration > 1s }', 'duration_ns > 1000000000'),
 ('F10 event attribute', '{ event.exception.type = "java.lang.IllegalStateException" }',
  f"arrayExists(x -> x = 'java.lang.IllegalStateException', events.attrs.{p('exception.type')}.:String)"),
 ('F11 link intrinsic', '{ link:traceID = "11111111111111111111111111111111" }',
  "arrayExists(x -> x = unhex('11111111111111111111111111111111'), links.trace_id)"),
 ('F12 descendant', '{ resource.service.name = "frontend" } >> { resource.service.name = "payment" && status = error }', None),
 ('F13 child', '{ resource.service.name = "checkout" } > { resource.service.name = "payment" }', None),
 ('F14 name', '{ name = "SELECT ledger" }', "name = 'SELECT ledger'"),
 ('F15 root service', '{ rootServiceName = "accounting" }',
  f"trace_id IN (SELECT trace_id FROM {DB}.traces GROUP BY trace_id HAVING max(root_service) = 'accounting')"),
 ('F16 trace duration', '{ traceDuration > 1s }',
  f"trace_id IN (SELECT trace_id FROM {DB}.traces GROUP BY trace_id HAVING max(end_ns) - min(start_ns) > 1000000000)"),
 ('F17 unscoped', '{ .app.user.id = "u-1" }',
  f"multiIf(dynamicType(attrs.{p('app.user.id')}) != 'None', attrs.{p('app.user.id')}.:String = 'u-1', "
  f"resource_id IN (SELECT resource_id FROM {DB}.resources WHERE dynamicType(attrs.{p('app.user.id')}) != 'None'), "
  f"resource_id IN (SELECT resource_id FROM {DB}.resources WHERE attrs.{p('app.user.id')}.:String = 'u-1'), false)"),
 ('F18 resource attribute', '{ resource.k8s.pod.name = "payment-a" }',
  f"resource_id IN (SELECT resource_id FROM {DB}.resources WHERE attrs.{p('k8s.pod.name')}.:String = 'payment-a')"),
 ('F19 int != 200', '{ span.http.response.status_code != 200 }',
  f"NOT (coalesce(attrs.{p('http.response.status_code')}.:Int64 = 200, false) OR coalesce(attrs.{p('http.response.status_code')}.:Float64 = 200, false))"),
 ('F20 kind consumer', '{ kind = consumer }', 'kind = 5'),
]
print('case\tpulsus\treference')
for name, tq, pred in CASES:
    if pred is None:
        if 'descendant' in name:
            sql = (f"SELECT concat(lower(hex(trace_id)), ':', lower(hex(span_id))) FROM {DB}.spans AS b WHERE {W} "
                   f"AND b.service = 'payment' AND b.status_code = 2 AND b.trace_id IN "
                   f"(SELECT trace_id FROM {DB}.spans WHERE {W} AND service = 'frontend') FORMAT TSV")
        else:
            sql = (f"SELECT concat(lower(hex(b.trace_id)), ':', lower(hex(b.span_id))) FROM {DB}.spans AS b "
                   f"WHERE {W} AND b.service = 'payment' AND (b.trace_id, b.parent_span_id) IN "
                   f"(SELECT trace_id, span_id FROM {DB}.spans WHERE {W} AND service = 'checkout') FORMAT TSV")
        mine = ' | '.join(q(sql).split('\n')) or '(none)'
    else:
        mine = ours(pred)
    print(f'{name}\t{mine}\t{ref(tq)}')
# the window's two edges: start is inclusive, end is exclusive (owner decision,
# 2026-09-22). The root span starts exactly at the window's lower bound.
def at(lo, hi, span):
    sql = (f"SELECT count() FROM {DB}.spans WHERE start_ns >= {lo} AND start_ns < {hi} "
           f"AND intDiv(start_ns, {B}) BETWEEN {lo // B} AND {(hi - 1) // B} "
           f"AND span_id = unhex('{span}') FORMAT TSV")
    return q(sql)
root = int(q(f"SELECT min(start_ns) FROM {DB}.spans FORMAT TSV"))
last = int(q(f"SELECT max(start_ns) FROM {DB}.spans FORMAT TSV"))
rid = q(f"SELECT lower(hex(span_id)) FROM {DB}.spans WHERE start_ns = {root} LIMIT 1 FORMAT TSV")
lid = q(f"SELECT lower(hex(span_id)) FROM {DB}.spans WHERE start_ns = {last} LIMIT 1 FORMAT TSV")
print(f'B1 span exactly at start\t{at(root, root + 1, rid)} (must be 1)\treference: start is inclusive')
print(f'B2 span exactly at end\t{at(root, last, lid)} (must be 0)\treference: end is exclusive')
print(f'B3 that same span one ns later\t{at(root, last + 1, lid)} (must be 1)\t-')

# span counts, which is where the retried push shows
cnt = q(f"SELECT count() FROM {DB}.spans WHERE {W} FORMAT TSV")
u = tempo + '/api/metrics/query_range?' + urllib.parse.urlencode({'q': '{} | count_over_time()', 'start': S, 'end': E, 'step': '60s'})
d = json.load(urllib.request.urlopen(u, timeout=300))
print(f'F21 span count\t{cnt}\t{sum(int(s.get("value", 0)) for x in d.get("series", []) for s in x.get("samples", []))}')
