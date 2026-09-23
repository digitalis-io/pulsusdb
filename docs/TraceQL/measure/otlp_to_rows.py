#!/usr/bin/env python3
"""Maps OTLP/JSON request bodies to the staging rows the schema is loaded from
— the same mapping docs/TraceQL/server-implementation.md gives for the write
path: one row per span, the resource kept whole (it is stored once per distinct
resource), attribute values kept typed.
Usage: otlp_to_rows.py DIR > spans.jsonl"""
import json, os, sys
d = sys.argv[1]
def val(v):
    if 'stringValue' in v: return ('s', v['stringValue'])
    if 'intValue' in v: return ('i', int(v['intValue']))
    if 'doubleValue' in v: return ('d', float(v['doubleValue']))
    if 'boolValue' in v: return ('b', bool(v['boolValue']))
    if 'arrayValue' in v: return ('as', [val(x)[1] for x in v['arrayValue'].get('values', [])])
    return ('s', '')
def attrs(l):
    out, seen = [], set()
    for a in l or []:
        if a['key'] in seen: continue      # a duplicate key keeps the first value
        seen.add(a['key']); t, v = val(a['value']); out.append([a['key'], t, v])
    return out
for f in sorted(os.listdir(f'{d}/otlp')):
    b = json.load(open(f'{d}/otlp/{f}'))
    for rs in b['resourceSpans']:
        r = attrs(rs.get('resource', {}).get('attributes'))
        svc = next((v for k, t, v in r if k == 'service.name'), '')
        for ss in rs.get('scopeSpans', []):
            sc = ss.get('scope', {})
            for sp in ss.get('spans', []):
                print(json.dumps({
                    'trace_id': sp['traceId'], 'span_id': sp['spanId'], 'parent_span_id': sp.get('parentSpanId', ''),
                    'name': sp.get('name', ''), 'kind': sp.get('kind', 0),
                    'start_ns': int(sp['startTimeUnixNano']), 'end_ns': int(sp['endTimeUnixNano']),
                    'status_code': sp.get('status', {}).get('code', 0),
                    'status_message': sp.get('status', {}).get('message', ''),
                    'service': svc, 'resource': r, 'scope_name': sc.get('name', ''), 'scope_version': sc.get('version', ''),
                    'scope_attrs': attrs(sc.get('attributes')),
                    'attrs': attrs(sp.get('attributes')),
                    'events': [[int(e['timeUnixNano']), e.get('name', ''), attrs(e.get('attributes'))] for e in sp.get('events', [])],
                    'links': [[l['traceId'], l['spanId'], attrs(l.get('attributes'))] for l in sp.get('links', [])],
                }, separators=(',', ':')))
