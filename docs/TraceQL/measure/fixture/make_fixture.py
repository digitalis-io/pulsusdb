#!/usr/bin/env python3
"""Writes the worked fixture of docs/TraceQL/functional-requirements.md: three
traces, nine spans, every attribute type, one event, one link, one retried
push. Emits OTLP/JSON bodies under <out>/otlp/ (the second copy of body 0 is
the retry) so the same bytes can be sent to any store.
Usage: make_fixture.py OUT_DIR BASE_UNIX_SECONDS"""
import json, os, sys
out, base = sys.argv[1], int(sys.argv[2])
NS = 10**9
def res(svc, pod, extra=()):
    a = [('service.name', 's', svc), ('deployment.environment.name', 's', 'prod'), ('k8s.pod.name', 's', pod)]
    return list(a) + list(extra)
def av(t, v):
    return {'s': lambda: {'stringValue': v}, 'i': lambda: {'intValue': str(v)}, 'd': lambda: {'doubleValue': v},
            'b': lambda: {'boolValue': v}, 'as': lambda: {'arrayValue': {'values': [{'stringValue': x} for x in v]}}}[t]()
def kv(l): return [{'key': k, 'value': av(t, v)} for k, t, v in l]
def span(tid, sid, pid, name, kind, t0, dur, attrs, status=0, events=(), links=()):
    o = {'traceId': tid, 'spanId': sid, 'name': name, 'kind': kind,
         'startTimeUnixNano': str(base * NS + t0), 'endTimeUnixNano': str(base * NS + t0 + dur),
         'attributes': kv(attrs)}
    if pid: o['parentSpanId'] = pid
    if status: o['status'] = {'code': status, 'message': 'boom'}
    if events: o['events'] = [{'timeUnixNano': str(base * NS + t), 'name': n, 'attributes': kv(a)} for t, n, a in events]
    if links: o['links'] = [{'traceId': lt, 'spanId': ls, 'attributes': kv(la)} for lt, ls, la in links]
    return o
T1, T2, T3 = '1' * 32, '2' * 32, '3' * 32
S = lambda n: f'{n:016x}'
bodies = []
def body(svc, pod, spans, extra=()):
    bodies.append({'resourceSpans': [{'resource': {'attributes': kv(res(svc, pod, extra))},
                   'scopeSpans': [{'scope': {'name': 'io.opentelemetry.http', 'version': '2.9.0',
                                            'attributes': kv([('otel.scope.build', 's', 'release')])},
                                  'spans': spans}]}]})
# trace 1: frontend -> checkout -> payment(error, exception event) and a db span
body('frontend', 'frontend-a', [
    span(T1, S(1), '', 'GET /cart', 2, 0, 500_000_000, [('http.request.method','s','GET'), ('http.route','s','/cart'),
         ('http.response.status_code','i',500), ('app.user.id','s','u-1'), ('app.cache.hit','b',False)], status=2),
    span(T1, S(2), S(1), 'checkout.Create', 3, 10_000_000, 400_000_000, [('rpc.system','s','grpc')])])
body('checkout', 'checkout-a', [
    span(T1, S(3), S(2), 'checkout.Create', 2, 20_000_000, 380_000_000, [('rpc.system','s','grpc'), ('app.items.count','i',3),
         ('app.discount.ratio','d',0.25), ('app.tags','as',['gold','eu'])]),
    span(T1, S(4), S(3), 'payment.Charge', 3, 30_000_000, 300_000_000, [('rpc.system','s','grpc')])])
body('payment', 'payment-a', [
    span(T1, S(5), S(4), 'payment.Charge', 2, 40_000_000, 280_000_000,
         [('rpc.system','s','grpc'), ('payment.amount','d',12.5), ('payment.currency','s','EUR')], status=2,
         events=[(200_000_000, 'exception', [('exception.type','s','java.lang.IllegalStateException'),
                                             ('exception.message','s','no funds')])]),
    span(T1, S(6), S(5), 'SELECT ledger', 3, 60_000_000, 120_000_000,
         [('db.system.name','s','postgresql'), ('db.query.text','s','SELECT 1'), ('http.response.status_code','s','200')])])
# trace 2: a clean frontend trace, no payment
body('frontend', 'frontend-b', [
    span(T2, S(7), '', 'GET /health', 2, 1_000_000_000, 5_000_000,
         [('http.request.method','s','GET'), ('http.route','s','/health'), ('http.response.status_code','i',200)])])
# trace 3: a consumer span linked to trace 1, 2 seconds long, clock skew of -1 ms
body('accounting', 'accounting-a', [
    span(T3, S(8), '', 'orders process', 5, 2_000_000_000 - 1_000_000, 2_000_000_000,
         [('messaging.system','s','kafka'), ('messaging.destination.name','s','orders')],
         links=[(T1, S(5), [('link.kind','s','producer')])]),
    span(T3, S(9), S(8), 'SELECT ledger', 3, 2_100_000_000, 50_000_000,
         [('db.system.name','s','postgresql'), ('db.query.text','s','SELECT 2')])])
os.makedirs(f'{out}/otlp', exist_ok=True)
n = 0
for i, b in enumerate(bodies):
    text = json.dumps(b, separators=(',', ':'))
    open(f'{out}/otlp/{n:03d}.json', 'w').write(text); n += 1
    if i == 0:                      # the retried push: the same bytes again
        open(f'{out}/otlp/{n:03d}.json', 'w').write(text); n += 1
print(json.dumps({'bodies': n, 'retried': 1, 'traces': 3, 'spans': sum(len(b['resourceSpans'][0]['scopeSpans'][0]['spans']) for b in bodies),
                  'window_start_s': base, 'window_end_s': base + 10}))
