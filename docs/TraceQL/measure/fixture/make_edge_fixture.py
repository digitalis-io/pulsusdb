#!/usr/bin/env python3
"""The awkward-topology fixture: the traces on which a wrong statement and a
right one give different answers. Every trace here exists to discriminate one
rule, and `edge_checks.sh` states the expected answer for each.

    ee01  same-time siblings and an orphan   nested-set determinism, totality
    ee02  a two-span cycle beside a tree      nested-set totality
    ee03  a chain of 65 spans                 the climb reaches the bound
    ee04  a chain of 66 spans                 the climb is cut: overflow
    ee05  a two-span cycle                    the climb is cut: overflow
    ee06  P -> A2 -> B1 -> A1, P -> P2 -> {A3, B2}
                                              child/parent/sibling union partners
    ee07  one resource under three spans      compare() counts spans, not resources

Usage: make_edge_fixture.py OUTDIR BASE_EPOCH_SECONDS
Writes OUTDIR/spans.jsonl in the staging shape `load_staging.sh` reads.
"""
import json, os, sys

OUT, BASE = sys.argv[1], int(sys.argv[2])
os.makedirs(OUT, exist_ok=True)
B0 = BASE * 1_000_000_000
rows = []

A_SVC, B_SVC, N_SVC = 'frontend', 'payment', 'gateway'   # the A side, the B side, neutral


def span(trace, sid, parent, svc, off_s, *, name='op', kind=2, status=0, msg='',
         attrs=None, res=None, events=None, links=None, scope=('io.pulsus.edge', '1.0.0')):
    rows.append({
        'trace_id': trace, 'span_id': sid, 'parent_span_id': parent, 'name': name,
        'kind': kind, 'start_ns': B0 + off_s * 1_000_000_000,
        'end_ns': B0 + off_s * 1_000_000_000 + 1_000_000_000,
        'status_code': status, 'status_message': msg, 'service': svc,
        'resource': res or [['service.name', 's', svc], ['k8s.pod.name', 's', svc + '-pod']],
        'scope_name': scope[0], 'scope_version': scope[1], 'scope_attrs': [],
        'attrs': attrs or [], 'events': events or [], 'links': links or [],
    })


def sid(n):  return f'{n:016x}'


# Trace ids end in a non-zero byte on purpose. ClickHouse trims trailing zero
# bytes when it compares a FixedString with a String, so an id ending in zeros
# hides whether a statement compares trace ids as FixedString(16) or not.
def tid(n):  return f'ee{n:02d}' + '0' * 26 + '11'


T1 = tid(1)
span(T1, sid(1), '',      N_SVC, 0)          # root
span(T1, sid(2), sid(1),  N_SVC, 1)          # two children at the SAME instant
span(T1, sid(3), sid(1),  N_SVC, 1)
span(T1, sid(4), sid(2),  N_SVC, 2)
span(T1, sid(5), 'ffffffffffffffff', N_SVC, 3)   # an orphan: its parent is not stored

T2 = tid(2)
span(T2, sid(1), sid(2),  N_SVC, 1)          # C1 <-> C2 is a cycle
span(T2, sid(2), sid(1),  N_SVC, 2)
span(T2, sid(3), sid(2),  N_SVC, 3)          # hanging off the cycle
span(T2, sid(4), '',      N_SVC, 0)          # a well-formed root beside it
span(T2, sid(5), sid(4),  N_SVC, 1)

# A chain of n spans: the deepest is n-1 parent links below the root. The A side
# is the root, the B side the deepest span, so the climb must cross n-1 links.
def chain(trace, n):
    span(trace, sid(1), '', A_SVC, 0)
    for i in range(2, n + 1):
        span(trace, sid(i), sid(i - 1), B_SVC if i == n else N_SVC, min(i, 50),
             status=2 if i == n else 0)

chain(tid(3), 65)      # 64 links: the bound is reached
chain(tid(4), 66)      # 65 links: one past the bound

T5 = tid(5)            # a cycle with both sides in it
span(T5, sid(1), sid(2), A_SVC, 1)
span(T5, sid(2), sid(1), B_SVC, 2, status=2)

T6 = tid(6)
span(T6, sid(1), '',     N_SVC, 0)                  # P
span(T6, sid(2), sid(1), A_SVC, 1, name='A2')       # A2
span(T6, sid(3), sid(2), B_SVC, 2, name='B1', status=2)
span(T6, sid(4), sid(3), A_SVC, 3, name='A1')
span(T6, sid(5), sid(1), N_SVC, 1, name='P2')       # P2
span(T6, sid(6), sid(5), A_SVC, 2, name='A3')
span(T6, sid(7), sid(5), B_SVC, 3, name='B2', status=2)

T7 = tid(7)            # one resource under a selection span and two baseline spans
R = [['service.name', 's', B_SVC], ['k8s.pod.name', 's', 'pod-x']]
span(T7, sid(1), '',     B_SVC, 0, name='checkout', status=2, msg='boom', res=R,
     attrs=[['http.route', 's', '/pay']],
     events=[[B0, 'exception', [['exception.type', 's', 'IOError']]]],
     links=[['0' * 32, sid(9), []]])
span(T7, sid(2), sid(1), B_SVC, 1, name='checkout', res=R, attrs=[['http.route', 's', '/pay']])
span(T7, sid(3), sid(1), B_SVC, 2, name='checkout', res=R, attrs=[['http.route', 's', '/ok']])

with open(os.path.join(OUT, 'spans.jsonl'), 'w') as f:
    for r in rows:
        f.write(json.dumps(r, separators=(',', ':')) + '\n')
print(json.dumps({'spans': len(rows), 'base_ns': B0,
                  'traces': sorted({r['trace_id'] for r in rows})}))
