#!/usr/bin/env python3
"""The TraceQL catalogue against the shipped compiler (issue #600).

`cargo run -p xtask -- traceql-catalogue` runs every query of the two accepted
directories of crates/pulsus-traceql/tests/corpus/ and every row of
`catalogue-extra.tsv` through the route's own planner and engine and writes one
JSON line per query: its disposition, the answer, and the statements the engine
issued with the rows each returned (`system.query_log`). This script compares
each search answer but one, whole and typed, with `catalogue_interp.py`, which
walks the parsed query over the fixture rows; `intrinsic_event_time_since_start`
waits for #600's compiler-fix part (`LATER` below). The two sides share nothing: the statements
are the compiler's and the parser here is the interpreter's own.

Usage: catalogue.py RUN_JSONL FIXTURE_JSONL START_NS END_NS
Exit status 0 only when every count below is what `query-catalogue.md` states.
"""
import json, os, sys
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import catalogue_interp as CI
from catalogue_parse import parse
from catalogue_render import refusal
from catalogue_interp import Fixture, Interp

RUN, FIXTURE, S, E = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
LIMIT, CUT, SPSS, ALL = 20, 2, 3, 1000
I = Interp(Fixture(FIXTURE))

# A query whose right answer on this fixture is empty, with the reason; any
# other empty answer is reported, because empty against empty proves nothing.
EMPTY_REASONS = {
    'intrinsic_span_id': 'a stored span id is sixteen hex characters, never the four of "0a1b"',
    'intrinsic_colon_space_after': 'the same query written `span: id`',
    'intrinsic_span_parent_id': 'a stored parent span id is sixteen hex characters',
    'intrinsic_trace_id': 'a stored trace id is thirty-two hex characters',
    'field_cross_type': 'every span holding both keys holds them at two types; a cross-type '
                        '`!=` that matched would return spans `…01` and `…2d`',
}
CAPS = {'answer': LIMIT, 'cut': CUT, 'full': LIMIT}
# Compared in #600's compiler-fix part: which of several matching events or
# links the response projects.
LATER = {'intrinsic_event_time_since_start'}
# `docs/api.md` §4.2: a duration `avg()` truncates. Trace `11…`'s four checkout
# spans last 7 s and 3 ns in all, so their average is 1.75 s and 0.75 ns:
# truncated it is `1.75s`, rounded up or to the nearest `1.750000001s`. Checked
# on the route's answer itself, so the two sides cannot agree on another rule.
PINNED = {'pipeline_avg_duration': ['avg(duration)', ['stringValue', '1.75s']]}


def interp_answer(q, limit, spss):
    CI.LIMIT, CI.SPSS = limit, spss
    try:
        return I.search_answer(q)
    finally:
        CI.LIMIT, CI.SPSS = LIMIT, SPSS


def rendered(answer):
    """the answer as the response carries it: a grouped trace's flat
    `matched` and `spans` are not rendered (`docs/api.md` §4.2)"""
    for t in answer['traces']:
        if t['groups'] is not None:
            t['matched'], t['spans'] = None, None
    return answer


def bounded(r):
    """`T-Q2`: each request's statements stayed under the ceiling, and each
    returned at most the request's `limit` rows"""
    st = r['statements']
    return all(st.get(k) and not r['overflow'].get(k) and all(s[1] <= cap for s in st[k])
               for k, cap in CAPS.items())


VECTOR_BAD = []
for line in open(f'{HERE}/parse_vectors.tsv'):
    if line.startswith('#') or not line.strip(): continue
    name, src, tree, _ = line.rstrip('\n').split('\t')
    if json.loads(json.dumps(parse(src))) != json.loads(tree): VECTOR_BAD.append(name)

tally = {g: {'served': 0, 'answer': 0, 'membership': 0} for g in ('corpus', 'design')}
planner, mismatched, errors, unbounded, unexplained, missing_reason, notes = [], [], [], [], [], [], []
metrics = later = 0
# Every row of catalogue-extra.tsv is a query of the run; the added searches
# expected to agree are those rows less its metrics queries.
EXTRA_ROWS = sum(1 for l in open(f'{HERE}/catalogue-extra.tsv')
                 if l.strip() and not l.startswith('#'))
design_seen = design_other = 0
pinned_bad, pinned_seen = [], set()
for line in open(RUN):
    r = json.loads(line)
    q = parse(r['traceql'])
    if r['group'] == 'design':
        design_seen += 1
        design_other += r['disposition'] != 'served' or r['route'] == 'metrics' or r['name'] in LATER
    if (refusal(q) is None) != (r['disposition'] == 'served'):
        mismatched.append(r['name'])
    if r['disposition'] != 'served':
        if r['disposition'] == 'plan_error': planner.append(r['name'])
        continue
    if r['route'] == 'metrics':              # compared in part 1b
        metrics += 1
        continue
    if r['name'] in LATER:
        later += 1
        continue
    t = tally['design' if r['group'] == 'design' else 'corpus']
    t['served'] += 1
    if 'error' in r:
        errors.append((r['name'], r['error'])); continue
    if not bounded(r): unbounded.append(r['name'])
    res = r['result']
    if r['name'] in PINNED:
        pinned_seen.add(r['name'])
        if not any(PINNED[r['name']] in g['attrs'] for t in res['answer']['traces']
                   for g in t['groups'] or []):
            pinned_bad.append(r['name'])
    a = (rendered(res['answer']) == rendered(interp_answer(q, LIMIT, SPSS))
         and rendered(res['cut']) == rendered(interp_answer(q, CUT, SPSS)))
    want_all = rendered(interp_answer(q, LIMIT, ALL))
    m = rendered(res['full']) == want_all
    t['answer'] += a
    t['membership'] += m
    if not (a and m):
        notes.append((r['name'], r['traceql'], a, m, json.dumps(res['full'])[:300], json.dumps(want_all)[:300]))
    empty = not want_all['traces']
    if empty and r['name'] not in EMPTY_REASONS: unexplained.append(r['name'])
    if not empty and r['name'] in EMPTY_REASONS: missing_reason.append(r['name'])

c, d = tally['corpus'], tally['design']
print(f"corpus searches served                   {c['served']}")
print(f"  answers / memberships that agree        {c['answer']} / {c['membership']}")
print(f"catalogue-extra.tsv searches served      {d['served']}")
print(f"  answers / memberships that agree        {d['answer']} / {d['membership']}")
print(f"metrics queries served, compared in 1b   {metrics}")
print(f"searches for the compiler-fix part       {later}")
print(f"refused by the planner                   {len(planner)}: {', '.join(sorted(planner))}")
print(f"disposition rule against the planner     {len(mismatched)} mismatches")
print(f"statements that failed                   {len(errors)}")
print(f"requests over the answer's size (T-Q2)   {len(unbounded)}")
print(f"parse vectors that differ                {len(VECTOR_BAD)}")
print(f"pinned values that differ or are absent  {len(pinned_bad) + len(set(PINNED) - pinned_seen)}")
print(f"empty answers without a reason           {len(unexplained)}")
print(f"reasons for an answer that is not empty  {len(missing_reason)}")
for n, src, a, m, got, want in notes:
    print(f'  disagreement: {n} {src!r} answer={a} membership={m}\n    route:       {got}\n    interpreter: {want}')
for n, e in errors: print(f'  statement failed: {n}: {e[:160]}')
for n in unbounded + unexplained + missing_reason + mismatched + VECTOR_BAD + pinned_bad + sorted(set(PINNED) - pinned_seen):
    print(f'  see: {n}')
ok = (c == {'served': 118, 'answer': 118, 'membership': 118}
      and design_seen == EXTRA_ROWS and d['served'] == EXTRA_ROWS - design_other
      and d == {'served': d['served'], 'answer': d['served'], 'membership': d['served']}
      and metrics == 24 and later == 1
      and sorted(planner) == ['by_expression_key', 'duration_unitless', 'pipeline_spanset_operation']
      and pinned_seen == set(PINNED)
      and not (mismatched or errors or unbounded or unexplained or missing_reason or VECTOR_BAD
               or pinned_bad))
sys.exit(0 if ok else 1)
