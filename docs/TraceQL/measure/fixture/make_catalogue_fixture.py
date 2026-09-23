#!/usr/bin/env python3
"""The fixture docs/TraceQL/query-catalogue.md answers on: spans in six traces,
carrying every attribute key, scope, intrinsic and value type the TraceQL corpus
at crates/pulsus-traceql/tests/corpus/ mentions, so that a catalogue entry's
answer is a list of spans rather than a uniform empty set.

Three things the layout is built for, each because a fixture without it lets a
wrong rule pass:

1. **The structural topology** (traces four and five): a chain that alternates
   the two sides, so the child union, the parent union and the descendant union
   each return a different pair; a sibling pair under a parent of neither side;
   a span whose parent is not stored; a two-span cycle.
2. **A descendant two links past the nearest edge** (trace four,
   `A4 -> N1 -> N2 -> B4`): a direct-child rule and a recursive climb answer
   `{A} >> {B}` differently, and a climb bounded at one link loses B4.
3. **A positive answer for every rule the corpus exercises** (trace six and the
   additions to trace four): arithmetic values, the escaped names, a bool the
   truthiness rule must read, a span with four children, a trace with four error
   spans, a sibling pair that survives a `| count() > 1` stage, and one resource
   carrying a key its span also carries with a different value, so the unscoped
   lookup ORDER decides the answer. `docs/TraceQL/query-catalogue.md` lists the
   four corpus queries that stay empty on both sides, with the reason.

Writes OTLP/JSON bodies under <out>/otlp/ and the staging rows at
<out>/spans.jsonl. Usage: make_catalogue_fixture.py OUT_DIR BASE_UNIX_SECONDS"""
import json, os, sys
out, base = sys.argv[1], int(sys.argv[2])
NS = 10**9
T1 = '11' * 16                      # trace one: the attribute-carrying chain
T2 = '22' * 16                      # trace two: events, links, scope, a root name
T3 = '33' * 16                      # trace three: a span carrying nothing
T4 = '44' * 16                      # trace four: the structural topology
T5 = '55' * 16                      # trace five: a two-span cycle
T6 = '66' * 16                      # trace six: the values every rule needs
LINK_TRACE = '000102030405060708090a0b0c0d0e0f'
LINK_SPAN = '0a1b2c3d4e5f6071'
def sid(n): return f'{n:016x}'
rows = []
def span(trace, n, parent, name, kind, t_off, dur, service, res, attrs,
         status=0, msg='', events=(), links=(), scope=('io.opentelemetry.http', '2.9.0'), scope_attrs=()):
    rows.append({
        'trace_id': trace, 'span_id': sid(n), 'parent_span_id': sid(parent) if parent else '',
        'name': name, 'kind': kind,
        'start_ns': base * NS + t_off, 'end_ns': base * NS + t_off + dur,
        'status_code': status, 'status_message': msg, 'service': service,
        'resource': res, 'scope_name': scope[0], 'scope_version': scope[1],
        'scope_attrs': [list(a) for a in scope_attrs],
        'attrs': [list(a) for a in attrs],
        'events': [[e[0], e[1], [list(a) for a in e[2]]] for e in events],
        'links': [[l[0], l[1], [list(a) for a in l[2]]] for l in links],
    })
RES_CHECKOUT = [['service.name', 's', 'checkout'], ['deployment.environment', 's', 'prod'],
                ['k8s.pod.name', 's', 'checkout-a']]
RES_GW = [['service.name', 's', 'gw'], ['deployment.environment', 's', 'dev'], ['k8s.pod.name', 's', 'gw-a']]
RES_PLAIN = [['service.name', 's', 'plain'], ['deployment.environment', 's', 'staging'], ['k8s.pod.name', 's', 'plain-a']]

# trace 1: one span carrying nearly every key the corpus names, and two below it
span(T1, 1, None, 'GET /api/orders', 2, 0, 3 * NS, 'checkout', RES_CHECKOUT, [
        ['a', 'i', 1], ['b', 'i', 2], ['foo', 's', 'f'], ['env', 's', 'prod'],
        ['success', 'b', True], ['retries', 'i', 1], ['retried', 'b', True],
        ['http.status_code', 'i', 500], ['http.method', 's', 'POST'],
        ['http.url', 's', '/api/orders/17'], ['http.route', 's', '/api/v2/list'],
        ['bytes', 'i', 1500], ['client.timeout', 'i', 5 * NS],
        ['attr with spaces', 'i', 1], ['foo bar', 's', 'x'],
     ], status=2, msg='boom')
span(T1, 2, 1, 'b', 3, 100 * 10**6, 2 * NS, 'checkout', RES_CHECKOUT,
     [['b', 'i', 2], ['c', 'i', 3], ['bytes', 'i', 900], ['retries', 'i', 3]])
span(T1, 3, 2, 'c', 1, 200 * 10**6, 1 * NS, 'checkout', RES_CHECKOUT,
     [['c', 'i', 3], ['d', 'i', 4], ['a', 'd', 1.0]])
# trace 2: a root name and service, an event, a link, scope attributes
span(T2, 4, None, 'GET /', 2, 10 * NS, 3 * NS, 'gw', RES_GW,
     [['a', 'i', 1], ['d', 'i', 4], ['http.status_code', 'i', 200], ['env', 's', 'dev']],
     events=[(base * NS + 10 * NS + 2 * 10**6, 'exception',
              [['exception.type', 's', 'IOError'], ['exception.message', 's', 'io']])],
     links=[(LINK_TRACE, LINK_SPAN, [['relation', 's', 'child_of']])],
     scope=('otel', '1.0'), scope_attrs=[['name', 's', 'otel'], ['otel.scope.build', 's', 'release']])
span(T2, 5, 4, 'GET /api/orders', 3, 10 * NS + 5 * 10**6, 1 * NS, 'gw', RES_GW,
     [['a', 'i', 1], ['b', 'i', 2], ['http.method', 's', 'GET'], ['http.url', 's', '/api/v3/list']],
     scope=('otel', '1.0'))
span(T2, 6, 5, 'child', 5, 10 * NS + 10 * 10**6, 500 * 10**6, 'gw', RES_GW,
     [['c', 'i', 3], ['success', 'b', False]], scope=('otel', '1.0'))
# trace 3: a span with no attributes at all, and one with only a numeric double
span(T3, 7, None, 'bare', 1, 20 * NS, 100 * 10**6, 'plain', RES_PLAIN, [])
span(T3, 8, 7, 'bare child', 4, 20 * NS + 10 * 10**6, 50 * 10**6, 'plain', RES_PLAIN,
     [['a', 'd', 2.5], ['retries', 'i', 5]], status=1)
# trace 4: P -> A2 -> B1 -> A1 and P -> P2 -> {A3, B2}, plus a span whose parent
# is not stored. A = `.a = 1`, B = `.b = 2`, and P and P2 carry neither, so
#   child   -> B1        child union   -> A2, B1
#   parent  -> B1        parent union  -> A1, B1
#   sibling -> B2        sibling union -> A3, B2
RES_EDGE = [['service.name', 's', 'edge'], ['deployment.environment', 's', 'prod'],
            ['k8s.pod.name', 's', 'edge-a']]
span(T4, 20, None, 'P', 2, 30 * NS, 5 * NS, 'edge', RES_EDGE, [])
span(T4, 21, 20, 'A2', 1, 30 * NS + 10**6, 1 * NS, 'edge', RES_EDGE, [['a', 'i', 1]])
span(T4, 22, 21, 'B1', 1, 30 * NS + 2 * 10**6, 1 * NS, 'edge', RES_EDGE, [['b', 'i', 2]])
span(T4, 23, 22, 'A1', 1, 30 * NS + 3 * 10**6, 1 * NS, 'edge', RES_EDGE, [['a', 'i', 1]])
span(T4, 24, 20, 'P2', 1, 30 * NS + 4 * 10**6, 1 * NS, 'edge', RES_EDGE, [])
span(T4, 25, 24, 'A3', 1, 30 * NS + 5 * 10**6, 1 * NS, 'edge', RES_EDGE, [['a', 'i', 1]])
span(T4, 26, 24, 'B2', 1, 30 * NS + 6 * 10**6, 1 * NS, 'edge', RES_EDGE, [['b', 'i', 2]])
span(T4, 27, 999, 'orphan', 1, 30 * NS + 7 * 10**6, 1 * NS, 'edge', RES_EDGE, [['b', 'i', 2]])
# a second B sibling under P2, so the sibling relation returns two spans in one
# trace and `{ .a = 1 } ~ { .b = 2 } | count() > 1` has something to keep; P2
# now has three children, which is what `{ span:childCount > 2 }` asks for
span(T4, 36, 24, 'B3', 1, 30 * NS + 8 * 10**6, 1 * NS, 'edge', RES_EDGE, [['b', 'i', 2]])
# A4 -> N1 -> N2 -> B4: B4 is a descendant of an A span three links up and the
# child of neither side, so `>` and `>>` differ, and a climb bounded at one
# link loses it
span(T4, 32, 20, 'A4', 1, 30 * NS + 9 * 10**6, 1 * NS, 'edge', RES_EDGE, [['a', 'i', 1]])
span(T4, 37, 32, 'N1', 1, 30 * NS + 10 * 10**6, 1 * NS, 'edge', RES_EDGE, [])
span(T4, 38, 37, 'N2', 1, 30 * NS + 11 * 10**6, 1 * NS, 'edge', RES_EDGE, [])
span(T4, 33, 38, 'B4', 1, 30 * NS + 12 * 10**6, 1 * NS, 'edge', RES_EDGE, [['b', 'i', 2]])
# trace 5: a two-span cycle, one span on each side
span(T5, 30, 31, 'cyc-a', 1, 40 * NS, 1 * NS, 'edge', RES_EDGE, [['a', 'i', 1]])
span(T5, 31, 30, 'cyc-b', 1, 40 * NS + 10**6, 1 * NS, 'edge', RES_EDGE, [['b', 'i', 2]])

# trace 6: one span per rule the other five traces answer empty. Four of its
# spans carry `status = error`, so `{ status = error } | count() > 3` keeps this
# trace and no other; RES_SHADOW carries `http.status_code` as a RESOURCE
# attribute while span 47 carries a different value for the same key at span
# scope, so an unscoped read answers 500 or 200 according to the order it tries
# the scopes in.
RES_SHADOW = [['service.name', 's', 'shadow'], ['deployment.environment', 's', 'prod'],
              ['k8s.pod.name', 's', 'shadow-a'], ['http.status_code', 'i', 200]]
NAME_HEX = 'Az'                                   # "\x41\x7a"
NAME_OCTAL = 'A\n'                                # "\101\012"
NAME_UNICODE = '\u00e9\u65e5\U0001F600'            # "\u00e9\u65e5\U0001F600"
NAME_SHORT = 'col1\tcol2\n"quoted" \\ bell\a vt\v bs\b ff\f cr\r'
span(T6, 40, None, 'checkout', 0, 50 * NS, 1, 'plain', RES_PLAIN, [['a', 'i', 3]])
span(T6, 41, 40, NAME_HEX, 1, 50 * NS + 10**6, 1 * NS, 'plain', RES_PLAIN,
     [['a', 'i', 8]], status=2)
span(T6, 42, 40, NAME_OCTAL, 1, 50 * NS + 2 * 10**6, 1 * NS, 'plain', RES_PLAIN,
     [['a', 'i', 2], ['b', 'i', 2]], status=2)
span(T6, 43, 40, NAME_UNICODE, 1, 50 * NS + 3 * 10**6, 1 * NS, 'plain', RES_PLAIN,
     [['a', 'i', 6]], status=2)
span(T6, 44, 40, NAME_SHORT, 1, 50 * NS + 4 * 10**6, 1 * NS, 'plain', RES_PLAIN,
     [['a', 'i', -1]], status=2)
# 2^30 ns exactly, so the log2 bucket rule can be wrong by one bucket and show
# it; and two children of its own, so `childCount > 2` and `childCount >= 2`
# differ somewhere
span(T6, 45, 40, 'b', 1, 50 * NS + 5 * 10**6, 1073741824, 'plain', RES_PLAIN,
     [['a', 'i', 1], ['foo', 'b', True]])
span(T6, 49, 45, 'b-child-1', 1, 50 * NS + 8 * 10**6, 1 * NS, 'plain', RES_PLAIN, [])
span(T6, 50, 45, 'b-child-2', 1, 50 * NS + 9 * 10**6, 1 * NS, 'plain', RES_PLAIN, [])
span(T6, 46, 40, 'min', 1, 50 * NS + 6 * 10**6, 1 * NS, 'plain', RES_PLAIN,
     [['a', 'i', -9223372036854775808]])
span(T6, 47, 40, 'shadow', 1, 50 * NS + 7 * 10**6, 1 * NS, 'shadow', RES_SHADOW,
     [['http.status_code', 'i', 500]])
# every value here matches the corpus's regexes SOMEWHERE INSIDE it and not from
# end to end, so an unanchored rendering and an anchored one differ: `GET.*`
# finds GET inside `xGETy`, `/api/.*` finds `/api/` inside `x/api/z`,
# `/api/v\d+/.*` finds its match inside `z/api/v2/x`, and `dev|test` finds `dev`
# inside `development`, which is what `!~` then has to decide about
RES_REGEX = [['service.name', 's', 'regex'], ['deployment.environment', 's', 'development'],
             ['k8s.pod.name', 's', 'regex-a']]
span(T6, 48, 40, 'xGETy', 1, 50 * NS + 10 * 10**6, 1 * NS, 'regex', RES_REGEX,
     [['http.url', 's', 'x/api/z'], ['http.route', 's', 'z/api/v2/x']])

os.makedirs(f'{out}/otlp', exist_ok=True)
def av(t, v):
    return ({'stringValue': v} if t == 's' else {'intValue': str(v)} if t == 'i'
            else {'doubleValue': v} if t == 'd' else {'boolValue': v})
def kv(l): return [{'key': k, 'value': av(t, v)} for k, t, v in l]
bodies, by_res = [], {}
for r in rows:
    by_res.setdefault((json.dumps(r['resource']), r['scope_name'], r['scope_version'],
                       json.dumps(r['scope_attrs'])), []).append(r)
for (res, sn, sv, sa), rs in by_res.items():
    spans = []
    for r in rs:
        o = {'traceId': r['trace_id'], 'spanId': r['span_id'], 'name': r['name'], 'kind': r['kind'],
             'startTimeUnixNano': str(r['start_ns']), 'endTimeUnixNano': str(r['end_ns']),
             'attributes': kv(r['attrs'])}
        if r['parent_span_id']: o['parentSpanId'] = r['parent_span_id']
        if r['status_code']: o['status'] = {'code': r['status_code'], 'message': r['status_message']}
        if r['events']: o['events'] = [{'timeUnixNano': str(t), 'name': n, 'attributes': kv(a)} for t, n, a in r['events']]
        if r['links']: o['links'] = [{'traceId': a, 'spanId': b, 'attributes': kv(c)} for a, b, c in r['links']]
        spans.append(o)
    scope = {'name': sn, 'version': sv}
    if json.loads(sa): scope['attributes'] = kv(json.loads(sa))
    bodies.append({'resourceSpans': [{'resource': {'attributes': kv(json.loads(res))},
                                      'scopeSpans': [{'scope': scope, 'spans': spans}]}]})
for i, b in enumerate(bodies):
    open(f'{out}/otlp/{i:03d}.json', 'w').write(json.dumps(b, separators=(',', ':')))
with open(f'{out}/spans.jsonl', 'w') as f:
    for r in rows: f.write(json.dumps(r, separators=(',', ':')) + '\n')
print(json.dumps({'spans': len(rows), 'traces': len({r['trace_id'] for r in rows}), 'bodies': len(bodies),
                  'window_start_s': base, 'window_end_s': base + 60,
                  'link_trace': LINK_TRACE, 'link_span': LINK_SPAN}))
