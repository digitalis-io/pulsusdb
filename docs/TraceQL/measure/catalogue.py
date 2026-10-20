#!/usr/bin/env python3
"""The TraceQL catalogue against the shipped compiler (issue #600).

`cargo run -p xtask -- traceql-catalogue` runs every query of the two accepted
directories of crates/pulsus-traceql/tests/corpus/ and every row of
`catalogue-extra.tsv` through the route's own planner and engine and writes one
JSON line per query: its disposition, the answer, and the statements the engine
issued with the rows each returned (`system.query_log`). This script compares
each search answer and each metrics range answer but `compare()`'s, whole and
typed, with `catalogue_interp.py`, which walks
the parsed query over the fixture rows. The two sides share nothing: the statements
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
    'metrics_result_comparison': 'no bucket of `{ .a = 1 }` holds more than 9 spans a minute, '
                                 '0.15 per second, so no sample passes `> 5`',
}
CAPS = {'answer': LIMIT, 'cut': CUT, 'full': LIMIT}
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


def metrics_bounded(r):
    """`T-Q2`, metrics: 1 to 100 statements, no sentinel row, and none returning
    more rows than the answer has series (one when it has none)"""
    st = r['statements'].get('answer') or []
    n = max(len(r['result']['series']), 1)
    return 1 <= len(st) and not r['overflow'].get('answer') and all(s[1] <= n for s in st)


def bounded(r):
    """`T-Q2`: each request's statements stayed under the ceiling, and each
    returned at most the request's `limit` rows"""
    st = r['statements']
    return all(st.get(k) and not r['overflow'].get(k) and all(s[1] <= cap for s in st[k])
               for k, cap in CAPS.items())


# The interpreter's two older entry points no longer answer a metrics query:
# `range_answer` is the one rule, and an entry point that still answered one
# would be a second, unchecked copy of it (#600 part 1b-1).
LEGACY_ANSWERING = []
for entry in (I.answer, I.api_answer):
    try:
        entry(parse('{} | rate()'))
        LEGACY_ANSWERING.append(entry.__name__)
    except NotImplementedError:
        pass

VECTOR_BAD = []
for line in open(f'{HERE}/parse_vectors.tsv'):
    if line.startswith('#') or not line.strip(): continue
    name, src, tree, _ = line.rstrip('\n').split('\t')
    if json.loads(json.dumps(parse(src))) != json.loads(tree): VECTOR_BAD.append(name)

tally = {g: {'served': 0, 'answer': 0, 'membership': 0} for g in ('corpus', 'design')}
planner, mismatched, errors, unbounded, unexplained, missing_reason, notes = [], [], [], [], [], [], []
metrics = {g: {'served': 0, 'answer': 0} for g in ('corpus', 'design')}
compare_later, metric_notes = [], []
# Every row of catalogue-extra.tsv is a query of the run; the added searches
# expected to agree are those rows less its metrics queries.
EXTRA_ROWS = sum(1 for l in open(f'{HERE}/catalogue-extra.tsv')
                 if l.strip() and not l.startswith('#'))
# ... and the added metrics answers compared here are its rows with a metrics
# stage and no `compare()`
EXTRA_METRICS = sum(1 for l in open(f'{HERE}/catalogue-extra.tsv')
                    if l.strip() and not l.startswith('#')
                    for p in [parse(l.split('\t')[1])['pipeline']]
                    if any(st['k'] == 'metric' for st in p) and not any(st['k'] == 'compare' for st in p))
design_seen = design_other = 0
pinned_bad, pinned_seen = [], set()
for line in open(RUN):
    r = json.loads(line)
    q = parse(r['traceql'])
    if r['group'] == 'design':
        design_seen += 1
        design_other += r['disposition'] != 'served' or r.get('route') == 'metrics'
    if (refusal(q) is None) != (r['disposition'] == 'served'):
        mismatched.append(r['name'])
    if r['disposition'] != 'served':
        if r['disposition'] == 'plan_error': planner.append(r['name'])
        continue
    if r['route'] == 'metrics':
        if any(st['k'] == 'compare' for st in q['pipeline']):
            compare_later.append(r['name']); continue       # #600 part 1b-2
        mt = metrics['design' if r['group'] == 'design' else 'corpus']
        mt['served'] += 1
        if 'error' in r:
            errors.append((r['name'], r['error'])); continue
        if not metrics_bounded(r): unbounded.append(r['name'])
        want = I.range_answer(q, S, E)
        mt['answer'] += r['result'] == want
        if r['result'] != want:
            metric_notes.append((r['name'], r['traceql'], json.dumps(r['result'])[:600], json.dumps(want)[:600]))
        if not want['series'] and r['name'] not in EMPTY_REASONS: unexplained.append(r['name'])
        if want['series'] and r['name'] in EMPTY_REASONS: missing_reason.append(r['name'])
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
cm, dm = metrics['corpus'], metrics['design']
print(f"corpus metrics range answers that agree   {cm['answer']} / {cm['served']}")
print(f"catalogue-extra.tsv metrics that agree    {dm['answer']} / {dm['served']}")
print(f"compare() queries, compared in 1b-2       {len(compare_later)}")
print(f"refused by the planner                   {len(planner)}: {', '.join(sorted(planner))}")
print(f"disposition rule against the planner     {len(mismatched)} mismatches")
print(f"statements that failed                   {len(errors)}")
print(f"requests over the answer's size (T-Q2)   {len(unbounded)}")
print(f"parse vectors that differ                {len(VECTOR_BAD)}")
print(f"older entry points answering metrics     {len(LEGACY_ANSWERING)}")
print(f"pinned values that differ or are absent  {len(pinned_bad) + len(set(PINNED) - pinned_seen)}")
print(f"empty answers without a reason           {len(unexplained)}")
print(f"reasons for an answer that is not empty  {len(missing_reason)}")
for n, src, a, m, got, want in notes:
    print(f'  disagreement: {n} {src!r} answer={a} membership={m}\n    route:       {got}\n    interpreter: {want}')
for n, src, got, want in metric_notes:
    print(f'  disagreement: {n} {src!r}\n    route:       {got}\n    interpreter: {want}')
for n, e in errors: print(f'  statement failed: {n}: {e[:160]}')
for n in unbounded + unexplained + missing_reason + mismatched + VECTOR_BAD + LEGACY_ANSWERING + pinned_bad + sorted(set(PINNED) - pinned_seen):
    print(f'  see: {n}')
ok = (c == {'served': 119, 'answer': 119, 'membership': 119}
      and design_seen == EXTRA_ROWS and d['served'] == EXTRA_ROWS - design_other
      and d == {'served': d['served'], 'answer': d['served'], 'membership': d['served']}
      and cm == {'served': 16, 'answer': 16} and dm == {'served': EXTRA_METRICS, 'answer': EXTRA_METRICS}
      and len(compare_later) == 5
      and sorted(planner) == ['by_expression_key', 'duration_unitless', 'pipeline_spanset_operation']
      and pinned_seen == set(PINNED)
      and not (mismatched or errors or unbounded or unexplained or missing_reason or VECTOR_BAD
               or LEGACY_ANSWERING
               or pinned_bad))
sys.exit(0 if ok else 1)
