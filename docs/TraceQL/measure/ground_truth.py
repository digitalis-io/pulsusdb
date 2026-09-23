#!/usr/bin/env python3
"""Counts, directly from the generated corpus, the spans each benchmark filter
must match. The corpus file is what was sent to both stores, so it settles any
disagreement between them. Case names match agreement.py's.
Usage: ground_truth.py SPANS_JSONL"""
import json, sys
src = sys.argv[1]
parent, svc, st = {}, {}, {}
c = dict.fromkeys(
    ['spans', 'service', 'service_error', 'status_code_ge_500', 'duration_kind', 'db_and_name',
     'route_regex', 'user_point', 'event_attr', 'bool_true', 'float_gt', 'int_ne', 'array_attr',
     'kind_producer', 'status_unset', 'name_eq', 'resource_attr', 'key_absent', 'child', 'descendant'], 0)
def attr(a, k):
    for kk, t, v in a:
        if kk == k: return (t, v)
    return None
for line in open(src):
    o = json.loads(line)
    key = (o['trace_id'], o['span_id'])
    parent[key] = o['parent_span_id']; svc[key] = o['service']; st[key] = o['status_code']
    a, r = o['attrs'], o['resource']
    c['spans'] += 1
    c['service'] += o['service'] == 'checkout'
    c['service_error'] += o['service'] == 'payment' and o['status_code'] == 2
    sc = attr(a, 'http.response.status_code')
    c['status_code_ge_500'] += bool(sc) and sc[0] in ('i', 'd') and sc[1] >= 500
    c['int_ne'] += not (bool(sc) and sc[0] in ('i', 'd') and sc[1] == 200)
    c['key_absent'] += sc is None
    c['duration_kind'] += (o['end_ns'] - o['start_ns']) > 2_000_000_000 and o['kind'] == 2
    db = attr(a, 'db.system.name')
    c['db_and_name'] += bool(db) and db[1] == 'postgresql' and o['name'] == 'SELECT shop'
    rt = attr(a, 'http.route')
    c['route_regex'] += bool(rt) and rt[1].startswith('/api/auth/')
    u = attr(a, 'app.user.id')
    c['user_point'] += bool(u) and u[1] == 'u-10013'
    c['event_attr'] += any(attr(ea, 'exception.type') == ('s', 'java.lang.IllegalStateException')
                           for _, _, ea in o['events'])
    ch = attr(a, 'app.cache.hit')
    c['bool_true'] += ch == ('b', True)
    dr = attr(a, 'app.discount.ratio')
    c['float_gt'] += bool(dr) and dr[0] in ('d', 'i') and dr[1] > 0.4
    tg = attr(a, 'app.tags')
    c['array_attr'] += bool(tg) and 'gold' in tg[1]
    c['kind_producer'] += o['kind'] == 4
    c['status_unset'] += o['status_code'] == 0
    c['name_eq'] += o['name'] == 'SELECT shop'
    pod = attr(r, 'k8s.pod.name')
    c['resource_attr'] += bool(pod) and pod[1] == 'cart-7d9f8b-00002'
for k, s in svc.items():
    if s == 'payment':
        p = (k[0], parent[k])
        if svc.get(p) == 'checkout': c['child'] += 1
        if st[k] == 2:
            cur = p
            while cur in svc:
                if svc[cur] == 'frontend': c['descendant'] += 1; break
                cur = (cur[0], parent[cur])
print('case\tcorpus')
for k, v in c.items(): print(f'{k}\t{v}')
