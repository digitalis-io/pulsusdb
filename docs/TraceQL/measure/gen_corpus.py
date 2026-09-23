#!/usr/bin/env python3
"""Deterministic trace corpus for docs/TraceQL/.

Writes, from ONE pass over one seeded random stream:
  <out>/otlp/NNNNNN.json   OTLP/JSON ExportTraceServiceRequest bodies, one
                           resource per body, in arrival order (for the
                           reference and for PulsusDB's /v1/traces)
  <out>/spans.jsonl        one flat row per span (for ClickHouse staging)
  <out>/summary.json       the corpus's own counts

Usage: gen_corpus.py OUT_DIR END_UNIX_SECONDS [SPANS_TARGET] [SEED]
The window is the 3 hours before END_UNIX_SECONDS.
"""
import json, os, random, sys, hashlib

out, end_s = sys.argv[1], int(sys.argv[2])
target = int(sys.argv[3]) if len(sys.argv) > 3 else 2_000_000
seed = int(sys.argv[4]) if len(sys.argv) > 4 else 20260922
R = random.Random(seed)
WINDOW_NS = 3 * 3600 * 10**9
END_NS = end_s * 10**9
START_NS = END_NS - WINDOW_NS
BATCH = 500          # spans per OTLP request body, per service instance
DUP_EVERY = 100      # every 100th body is sent twice (a client retry)

# ---- topology --------------------------------------------------------------
# service -> (protocol, callees). 'pg'/'redis' are databases, 'kafka' a topic.
TOPO = {
    'frontend':       ('http', ['cart', 'catalog', 'recommendation', 'checkout', 'ad', 'currency', 'auth', 'search']),
    'checkout':       ('grpc', ['cart', 'payment', 'shipping', 'email', 'currency', 'fraud', 'inventory', 'kafka']),
    'cart':           ('grpc', ['redis']),
    'catalog':        ('grpc', ['pg']),
    'recommendation': ('grpc', ['catalog', 'redis']),
    'payment':        ('grpc', ['fraud', 'pg']),
    'shipping':       ('http', ['quote']),
    'quote':          ('http', []),
    'email':          ('http', []),
    'currency':       ('grpc', []),
    'fraud':          ('grpc', ['pg', 'redis']),
    'inventory':      ('grpc', ['pg']),
    'ad':             ('grpc', ['redis']),
    'auth':           ('http', ['pg', 'redis']),
    'search':         ('http', ['catalog', 'pg']),
    'accounting':     ('kafka', ['pg']),
    'notification':   ('kafka', ['email']),
    'loadgen':        ('http', ['frontend']),
    'image-provider': ('http', []),
    'feature-flags':  ('grpc', ['pg']),
    'reviews':        ('grpc', ['pg', 'catalog']),
    'geo':            ('http', []),
    'tax':            ('grpc', []),
    'loyalty':        ('grpc', ['pg', 'redis']),
}
SERVICES = list(TOPO)
LANG = ['go', 'java', 'python', 'nodejs', 'dotnet', 'rust']
PODS = 4
ZONES = ['eu-west-1a', 'eu-west-1b', 'eu-west-1c']
OPS = {s: [f'/api/{s}/{w}' for w in ['get', 'list', 'create', 'update', 'delete', 'search', 'health', 'batch']] for s in SERVICES}
RPC_METHODS = ['Get', 'List', 'Create', 'Update', 'Delete', 'Check', 'Stream', 'Batch']
SQL = [f'SELECT id, name, price FROM products WHERE id = $1 /* t{i} */' for i in range(6)] + \
      [f'UPDATE orders SET state = $1 WHERE order_id = $2 /* t{i} */' for i in range(4)] + \
      [f'INSERT INTO ledger (order_id, amount, currency, ts) VALUES ($1, $2, $3, $4) /* t{i} */' for i in range(3)]
UA = [f'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{120+i}.0.0.0 Safari/537.36' for i in range(20)] + \
     [f'okhttp/4.{i}.0' for i in range(10)]
STACKS = []
for i in range(15):
    frames = ''.join(f'\n\tat com.shop.{SERVICES[i % len(SERVICES)]}.Handler{j}.process(Handler{j}.java:{100 + 7 * j + i})' for j in range(25))
    STACKS.append((f'java.lang.IllegalStateException', f'order {i} rejected: inventory unavailable', 'java.lang.IllegalStateException: inventory unavailable' + frames))
STATUS_HTTP = [200] * 90 + [201] * 3 + [204] * 2 + [301, 400, 404, 404, 500]

def hexid(n):
    return ''.join('%02x' % R.getrandbits(8) for _ in range(n))

class Inst:
    def __init__(self, svc, i):
        self.svc, self.i = svc, i
        lang = LANG[SERVICES.index(svc) % len(LANG)]
        self.skew = R.randint(-20_000_000, 20_000_000)   # clock skew, ns
        self.resource = [
            ('service.name', 's', svc), ('service.namespace', 's', 'shop'),
            ('service.version', 's', f'1.{SERVICES.index(svc) % 7}.{i}'),
            ('service.instance.id', 's', hashlib.md5(f'{svc}{i}'.encode()).hexdigest()),
            ('deployment.environment.name', 's', 'prod'),
            ('k8s.namespace.name', 's', 'shop'), ('k8s.pod.name', 's', f'{svc}-7d9f8b-{i:05d}'),
            ('k8s.node.name', 's', f'node-{(SERVICES.index(svc) + i) % 12:02d}'),
            ('host.name', 's', f'{svc}-7d9f8b-{i:05d}'), ('cloud.region', 's', 'eu-west-1'),
            ('cloud.availability_zone', 's', ZONES[i % 3]),
            ('telemetry.sdk.language', 's', lang), ('telemetry.sdk.name', 's', 'opentelemetry'),
            ('telemetry.sdk.version', 's', '1.38.0'), ('process.pid', 'i', 1000 + i),
        ]
        self.scope = (f'io.opentelemetry.{TOPO[svc][0]}', '2.9.0')
        self.buf = []

INSTS = {s: [Inst(s, i) for i in range(PODS)] for s in SERVICES}

def size_class():
    x = R.random()
    if x < 0.60: return R.randint(3, 10)
    if x < 0.90: return R.randint(10, 40)
    if x < 0.99: return R.randint(40, 200)
    return R.randint(200, 1000)

def attrs_for(kind, proto, svc, callee, op, status_http, trace_ctx):
    a = []
    if proto == 'http':
        route = op
        a += [('http.request.method', 's', R.choice(['GET'] * 6 + ['POST'] * 3 + ['PUT', 'DELETE'])),
              ('http.route', 's', route), ('http.response.status_code', 'i', status_http),
              ('url.path', 's', route.replace('/get', f'/get/{R.randint(1, 20000)}')),
              ('network.protocol.version', 's', '1.1')]
        if kind == 2:
            a += [('user_agent.original', 's', R.choice(UA)),
                  ('client.address', 's', f'10.{R.randint(0, 19)}.{R.randint(0, 255)}.{R.randint(1, 254)}'),
                  ('server.address', 's', f'{svc}.shop.svc'),
                  ('http.request.body.size', 'i', R.randint(0, 4096))]
        else:
            a += [('server.address', 's', f'{callee}.shop.svc'), ('server.port', 'i', 8080)]
    elif proto == 'grpc':
        a += [('rpc.system', 's', 'grpc'), ('rpc.service', 's', f'shop.{(callee if kind == 3 else svc)}.v1.Service'),
              ('rpc.method', 's', R.choice(RPC_METHODS)), ('rpc.grpc.status_code', 'i', 0 if status_http < 500 else 13)]
        if kind == 3:
            a += [('server.address', 's', f'{callee}.shop.svc'), ('server.port', 'i', 50051)]
    elif proto == 'db':
        q = R.choice(SQL)
        a += [('db.system.name', 's', 'postgresql' if callee == 'pg' else 'redis'),
              ('db.namespace', 's', 'shop'), ('db.operation.name', 's', q.split(' ')[0]),
              ('db.query.text', 's', q if callee == 'pg' else f'GET cart:{R.randint(1, 50000)}'),
              ('server.address', 's', f'{callee}.shop.svc')]
    elif proto == 'kafka':
        a += [('messaging.system', 's', 'kafka'), ('messaging.destination.name', 's', 'orders'),
              ('messaging.operation.type', 's', 'send' if kind == 4 else 'process'),
              ('messaging.kafka.offset', 'i', R.randint(0, 10**9))]
    if R.random() < 0.3:
        a.append(('app.user.id', 's', trace_ctx['user']))
    if trace_ctx['order'] and svc in ('checkout', 'payment', 'shipping', 'accounting'):
        a.append(('app.order.id', 's', trace_ctx['order']))
    if R.random() < 0.15:
        a.append(('app.cache.hit', 'b', R.random() < 0.7))
    if R.random() < 0.10:
        a.append(('app.items.count', 'i', R.randint(1, 30)))
    if R.random() < 0.05:
        a.append(('app.discount.ratio', 'd', round(R.random() * 0.5, 4)))
    if R.random() < 0.02:
        a.append(('app.tags', 'as', [R.choice(['gold', 'new', 'eu', 'mobile']) for _ in range(R.randint(1, 3))]))
    return a

def make_trace(t0):
    n = size_class()
    ctx = {'user': f'u-{R.randint(1, 50000)}', 'order': (f'ord-{hexid(6)}' if R.random() < 0.3 else '')}
    tid = hexid(16)
    root_svc = 'loadgen' if R.random() < 0.05 else ('accounting' if R.random() < 0.03 else 'frontend')
    root_dur = int(R.lognormvariate(18.6, 1.1))           # median ~120 ms
    spans = []
    def add(svc, kind, parent, start, dur, name, proto, callee, status_http, err):
        inst = R.choice(INSTS[svc])
        sp = {'trace_id': tid, 'span_id': hexid(8), 'parent': parent, 'svc': svc, 'inst': inst,
              'kind': kind, 'start': start + inst.skew, 'dur': max(dur, 1000), 'name': name,
              'attrs': attrs_for(kind, proto, svc, callee, name, status_http, ctx),
              'err': err, 'events': [], 'links': []}
        if err:
            et, em, st = R.choice(STACKS)
            sp['events'].append((start + dur // 2, 'exception', [('exception.type', 's', et), ('exception.message', 's', em), ('exception.stacktrace', 's', st)]))
        if R.random() < 0.03:
            sp['events'].append((start + dur // 3, 'retry', [('retry.count', 'i', R.randint(1, 3))]))
        spans.append(sp)
        return sp
    op = R.choice(OPS[root_svc])
    code = R.choice(STATUS_HTTP)
    root = add(root_svc, 5 if root_svc == 'accounting' else 2, '', t0, root_dur, op, TOPO[root_svc][0], None, code, code >= 500)
    if root_svc == 'accounting':
        root['links'].append((hexid(16), hexid(8), [('messaging.kafka.offset', 'i', R.randint(0, 10**9))]))
    frontier = [root]
    while len(spans) < n and frontier:
        parent = R.choice(frontier)
        svc = parent['svc']
        callees = TOPO[svc][1]
        pstart, pdur = parent['start'] - parent['inst'].skew, parent['dur']
        cstart = pstart + int(pdur * R.random() * 0.3)
        cdur = int(pdur * (0.2 + 0.6 * R.random()))
        x = R.random()
        if not callees or x < 0.15:
            add(svc, 1, parent['span_id'], cstart, cdur, f'{svc}.internal.{R.choice(RPC_METHODS).lower()}', 'internal', None, 200, False)
            if not callees:
                frontier.remove(parent) if parent is not root else None
            continue
        callee = R.choice(callees)
        code = R.choice(STATUS_HTTP)
        err = code >= 500 and R.random() < 0.6
        if callee in ('pg', 'redis'):
            add(svc, 3, parent['span_id'], cstart, cdur, ('SELECT shop' if callee == 'pg' else 'GET'), 'db', callee, code, err)
            continue
        if callee == 'kafka':
            add(svc, 4, parent['span_id'], cstart, cdur // 4, 'orders send', 'kafka', None, 200, False)
            continue
        proto = TOPO[callee][0] if TOPO[callee][0] != 'kafka' else 'grpc'
        cname = (f'shop.{callee}.v1.Service/{R.choice(RPC_METHODS)}' if proto == 'grpc' else R.choice(['GET', 'POST']))
        c = add(svc, 3, parent['span_id'], cstart, cdur, cname, proto, callee, code, err)
        s = add(callee, 2, c['span_id'], cstart + cdur // 20, int(cdur * 0.9), (cname if proto == 'grpc' else R.choice(OPS[callee])), proto, None, code, err)
        frontier.append(s)
    # an error below makes the root an error with probability 0.5
    if any(s['err'] for s in spans) and not root['err'] and R.random() < 0.5:
        root['err'] = True
    return spans

def av(t, v):
    if t == 's': return {'stringValue': v}
    if t == 'i': return {'intValue': str(v)}
    if t == 'd': return {'doubleValue': v}
    if t == 'b': return {'boolValue': v}
    if t == 'as': return {'arrayValue': {'values': [{'stringValue': x} for x in v]}}

def kv(lst):
    return [{'key': k, 'value': av(t, v)} for k, t, v in lst]

def typed(lst):
    return [[k, t, v] for k, t, v in lst]

os.makedirs(f'{out}/otlp', exist_ok=True)
rows = open(f'{out}/spans.jsonl', 'w')
nbody = [0]
# `dup_spans` is what a store that keeps a retried push twice ends up holding
# beyond `spans`: the spans inside the 40 bodies that are sent a second time.
# The reference is such a store, so the visibility check in run_all.sh expects
# exactly `spans + dup_spans` there and refuses anything else.
stats = {'spans': 0, 'traces': 0, 'bodies': 0, 'dup_bodies': 0, 'dup_spans': 0,
         'error_spans': 0, 'events': 0, 'links': 0,
         'max_spans_per_trace': 0, 'attr_values': 0, 'window_start_ns': START_NS, 'window_end_ns': END_NS}

def flush(inst):
    if not inst.buf: return
    body = {'resourceSpans': [{'resource': {'attributes': kv(inst.resource)},
             'scopeSpans': [{'scope': {'name': inst.scope[0], 'version': inst.scope[1]}, 'spans': inst.buf}]}]}
    text = json.dumps(body, separators=(',', ':'))
    reps = 2 if nbody[0] % DUP_EVERY == DUP_EVERY - 1 else 1
    for _ in range(reps):
        with open(f'{out}/otlp/{stats["bodies"]:06d}.json', 'w') as f: f.write(text)
        stats['bodies'] += 1
    stats['dup_bodies'] += reps - 1
    stats['dup_spans'] += (reps - 1) * len(inst.buf)
    nbody[0] += 1
    inst.buf = []

# traces start uniformly over the window, in order; the last 10 minutes start
# no new traces so that every trace ends inside the window
t = START_NS
gap = (WINDOW_NS - 600 * 10**9) // max(1, target // 28)
while stats['spans'] < target:
    t += R.randint(0, 2 * gap)
    if t >= END_NS - 600 * 10**9: t = START_NS + R.randint(0, WINDOW_NS - 600 * 10**9)
    spans = make_trace(t)
    stats['traces'] += 1
    stats['max_spans_per_trace'] = max(stats['max_spans_per_trace'], len(spans))
    for s in spans:
        code = 2 if s['err'] else 0
        o = {'traceId': s['trace_id'], 'spanId': s['span_id'], 'name': s['name'], 'kind': s['kind'],
             'startTimeUnixNano': str(s['start']), 'endTimeUnixNano': str(s['start'] + s['dur']),
             'attributes': kv(s['attrs']), 'status': ({'code': 2, 'message': 'upstream failed'} if code == 2 else {})}
        if s['parent']: o['parentSpanId'] = s['parent']
        if s['events']: o['events'] = [{'timeUnixNano': str(tm), 'name': nm, 'attributes': kv(at)} for tm, nm, at in s['events']]
        if s['links']: o['links'] = [{'traceId': lt, 'spanId': ls, 'attributes': kv(la)} for lt, ls, la in s['links']]
        inst = s['inst']
        inst.buf.append(o)
        rows.write(json.dumps({
            'trace_id': s['trace_id'], 'span_id': s['span_id'], 'parent_span_id': s['parent'],
            'name': s['name'], 'kind': s['kind'], 'start_ns': s['start'], 'end_ns': s['start'] + s['dur'],
            'status_code': code, 'status_message': 'upstream failed' if code == 2 else '',
            'service': s['svc'], 'resource': typed(inst.resource), 'scope_name': inst.scope[0], 'scope_version': inst.scope[1], 'scope_attrs': [],
            'attrs': typed(s['attrs']),
            'events': [[tm, nm, typed(at)] for tm, nm, at in s['events']],
            'links': [[lt, ls, typed(la)] for lt, ls, la in s['links']],
        }, separators=(',', ':')) + '\n')
        stats['spans'] += 1
        stats['error_spans'] += code == 2
        stats['events'] += len(s['events'])
        stats['links'] += len(s['links'])
        stats['attr_values'] += len(s['attrs'])
        if len(inst.buf) >= BATCH: flush(inst)
for s in SERVICES:
    for inst in INSTS[s]: flush(inst)
rows.close()
json.dump(stats, open(f'{out}/summary.json', 'w'), indent=1)
print(json.dumps(stats))
