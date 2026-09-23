#!/usr/bin/env python3
"""Compares, for each filter in the benchmark, the number of spans the
reference counts with `| count_over_time()` against the number the compiled
SQL predicate counts over the same window. Any difference is a semantic
difference between the compiled form and the reference, and is reported.
Usage: agreement.py TEMPO_URL CH_URL DB START_S END_S"""
import sys, json, urllib.request, urllib.parse
tempo, ch, DB, S, E = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), int(sys.argv[5])
SNS, ENS, B = S * 10**9, E * 10**9, 300_000_000_000
W = f"start_ns >= {SNS} AND start_ns < {ENS} AND intDiv(start_ns, {B}) BETWEEN {SNS // B} AND {(ENS - 1) // B}"
def p(k): return '`' + k.replace('%', '%25').replace('.', '%2E') + '`'
CASES = [
 ('service', '{ resource.service.name = "checkout" }', "service = 'checkout'"),
 ('service_error', '{ resource.service.name = "payment" && status = error }', "service = 'payment' AND status_code = 2"),
 ('status_code_ge_500', '{ span.http.response.status_code >= 500 }',
  f"attrs.{p('http.response.status_code')}.:Int64 >= 500 OR attrs.{p('http.response.status_code')}.:Float64 >= 500"),
 ('duration_kind', '{ duration > 2s && kind = server }', "duration_ns > 2000000000 AND kind = 2"),
 ('db_and_name', '{ span.db.system.name = "postgresql" && name = "SELECT shop" }',
  f"attrs.{p('db.system.name')}.:String = 'postgresql' AND name = 'SELECT shop'"),
 ('route_regex', '{ span.http.route =~ "/api/auth/.*" }', f"match(attrs.{p('http.route')}.:String, '^(?:/api/auth/.*)$')"),
 ('user_point', '{ span.app.user.id = "u-10013" }', f"attrs.{p('app.user.id')}.:String = 'u-10013'"),
 ('event_attr', '{ event.exception.type = "java.lang.IllegalStateException" }',
  f"arrayExists(x -> x = 'java.lang.IllegalStateException', events.attrs.{p('exception.type')}.:String)"),
 ('bool_true', '{ span.app.cache.hit = true }', f"attrs.{p('app.cache.hit')}.:Bool = true"),
 ('float_gt', '{ span.app.discount.ratio > 0.4 }',
  f"attrs.{p('app.discount.ratio')}.:Float64 > 0.4 OR attrs.{p('app.discount.ratio')}.:Int64 > 0.4"),
 ('int_ne', '{ span.http.response.status_code != 200 }',
  f"NOT (coalesce(attrs.{p('http.response.status_code')}.:Int64 = 200, false) OR coalesce(attrs.{p('http.response.status_code')}.:Float64 = 200, false))"),
 ('array_attr', '{ span.app.tags = "gold" }',
  f"has(attrs.{p('app.tags')}.:`Array(Nullable(String))`, 'gold')"),
 ('kind_producer', '{ kind = producer }', "kind = 4"),
 ('status_unset', '{ status = unset }', "status_code = 0"),
 ('name_eq', '{ name = "SELECT shop" }', "name = 'SELECT shop'"),
 ('resource_attr', '{ resource.k8s.pod.name = "cart-7d9f8b-00002" }',
  f"resource_id IN (SELECT resource_id FROM {DB}.resources WHERE attrs.{p('k8s.pod.name')}.:String = 'cart-7d9f8b-00002')"),
 ('child', '{ resource.service.name = "checkout" } > { resource.service.name = "payment" }', None),
 ('descendant', '{ resource.service.name = "frontend" } >> { resource.service.name = "payment" && status = error }', None),
]
def tempo_count(q):
    u = tempo + '/api/metrics/query_range?' + urllib.parse.urlencode(
        {'q': q + ' | count_over_time()', 'start': S, 'end': E, 'step': '600s'})
    d = json.load(urllib.request.urlopen(u, timeout=600))
    return sum(int(s.get('value', 0)) for x in d.get('series', []) for s in x.get('samples', []))
def ch_count(pred):
    sql = f"SELECT count() FROM {DB}.spans WHERE {W} AND ({pred}) SETTINGS final = 1"
    return int(urllib.request.urlopen(urllib.request.Request(ch, data=sql.encode()), timeout=600).read())
print('case\treference\tpulsus\tagree')
for name, q, pred in CASES:
    t = tempo_count(q)
    if pred is None:
        if name == 'child':
            sql = (f"SELECT sum(length(arrayFilter(x -> has(aids, x), bpar))) FROM (SELECT groupArrayIf(span_id, service = 'checkout') AS aids, "
                   f"groupArrayIf(parent_span_id, service = 'payment') AS bpar FROM {DB}.spans WHERE {W} AND service IN ('checkout','payment') GROUP BY trace_id)")
        else:
            sql = open(sys.path[0] + '/sql/s09_descendant.sql').read().replace('LIMIT 20', '')
            sql = f"SELECT sum(matched) FROM ({sql})"
        c = int(urllib.request.urlopen(urllib.request.Request(ch, data=(sql + ' SETTINGS final = 1').encode()), timeout=600).read())
    else:
        c = ch_count(pred)
    print(f'{name}\t{t}\t{c}\t{"yes" if t == c else "NO"}')
