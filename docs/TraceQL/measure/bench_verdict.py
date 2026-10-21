#!/usr/bin/env python3
"""The verdict of a bench.sh run on functional-requirements.md §6.3's rule.

Reads OUT/paired.tsv (http_bench.py paired) and OUT/fetch.tsv
(fetch_compare.py, the 20-span trace's two lines and then the largest trace's),
prints one row per §6.3 shape and then each state's count, and writes the same
as Markdown to OUT/summary.md. Exits 0 only for 11 `below`, 2 `recorded`,
1 `exempt`, 1 `no counterpart` and nothing else. The two trace-by-id rows are
`recorded`: both medians are reported and their order passes or fails
nothing. A compared or exempt shape either side answers with nothing
(`items` 0), or g01 with no edge, is `not like for like`.

Usage: bench_verdict.py OUT  |  bench_verdict.py --self-test
"""
import sys

# §6.3's fifteen shapes, in its table's order: (name, how it is judged)
SHAPES = [
    ('fetch_trace_20', 'recorded'),
    ('fetch_trace_largest', 'recorded'),
    ('m01_rate_by_service', 'compared'),
    ('m02_quantiles_by_route', 'compared'),
    ('m03_errors_by_service', 'compared'),
    ('m04_histogram', 'compared'),
    ('m05_instant_avg_by_name', 'compared'),
    ('m06_rate_by_resource_attr', 'compared'),
    ('t01_tag_names_span', 'compared'),
    ('t02_tag_values_route', 'compared'),
    ('t03_tag_values_filtered', 'compared'),
    ('s08_user_point', 'compared'),
    ('s14_trace_level', 'exempt'),
    ('c08_compare', 'compared'),
    ('g01_service_graph', 'no counterpart'),
]
STATES = ['below', 'not below', 'recorded', 'exempt', 'no counterpart', 'refused', 'reference failed',
          'not like for like']
PASS = {'below': 11, 'recorded': 2, 'exempt': 1, 'no counterpart': 1}


def verdict(paired_text, fetch_text):
    """(rows, counts): rows are (shape, pulsus_ms, reference_ms, pulsus_bytes,
    reference_bytes, state). A shape missing from an input is refused on that
    side's account."""
    halves = {}            # shape -> {side: (status, ms, bytes)}, side 'p' or 'r'
    answered = {}          # shape -> {side: items}, from paired's p_items and r_items
    for line in paired_text.splitlines():
        c = line.split('\t')
        if len(c) < 6 or not c[0]: continue
        h = halves.setdefault(c[0], {})
        h['p'] = (c[1], c[4], c[2])
        if len(c) >= 11 and c[6] != '-':
            h['r'] = (c[6], c[9], c[7])
        for side, i in (('p', 11), ('r', 12)):
            if len(c) > i and c[i].isdigit():
                answered.setdefault(c[0], {})[side] = int(c[i])
    fetch = [dict(kv.split('=', 1) for kv in line.split('\t')[1:] if '=' in kv)
             | {'side': line.split('\t')[0], 'state': line.split('\t')[-1]}
             for line in fetch_text.splitlines() if line.strip()]
    for shape, at in (('fetch_trace_20', 0), ('fetch_trace_largest', 2)):
        h = halves.setdefault(shape, {})
        for side, i, who in (('p', at, 'PulsusDB'), ('r', at + 1, 'the reference')):
            if i < len(fetch) and fetch[i]['side'] == who and 'p50' in fetch[i]:
                h[side] = ('200', fetch[i]['p50'], fetch[i].get('bytes', ''))
            elif i < len(fetch) and fetch[i]['side'] == who and fetch[i]['state'] == 'answered':
                h[side] = ('200', '-', '-')      # every call answered 200; the other side failed
    rows, counts = [], {s: 0 for s in STATES}
    for shape, kind in SHAPES:
        h = halves.get(shape, {})
        p, r = h.get('p'), h.get('r')
        n = answered.get(shape, {})
        if p is None or p[0] != '200':
            state = 'refused'
        elif kind == 'no counterpart':
            state = 'not like for like' if n.get('p') == 0 else 'no counterpart'
        elif r is None or r[0] != '200':
            state = 'reference failed'
        elif 0 in (n.get('p'), n.get('r')):
            state = 'not like for like'
        elif kind in ('exempt', 'recorded'):
            state = kind
        else:
            state = 'below' if float(p[1]) < float(r[1]) else 'not below'
        counts[state] += 1
        rows.append((shape, p[1] if p else '-', r[1] if r else '-', p[2] if p else '-', r[2] if r else '-', state))
    return rows, counts


def passes(counts):
    return {k: v for k, v in counts.items() if v} == PASS


COUNT_LINES = [('compared below', 'below'), ('recorded', 'recorded'), ('exempt', 'exempt'),
               ('no counterpart', 'no counterpart'), ('refused', 'refused'), ('not below', 'not below'), ('reference failed', 'reference failed'),
               ('not like for like', 'not like for like')]


def write(out, rows, counts):
    """the TSV on stdout, the same as Markdown in OUT/summary.md"""
    head = ('shape', 'pulsus_ms', 'reference_ms', 'pulsus_bytes', 'reference_bytes', 'state')
    for r in (head, *rows):
        print('\t'.join(r))
    print()
    for label, state in COUNT_LINES:
        print(f'{label}\t{counts[state]}')
    print(f'verdict\t{"pass" if passes(counts) else "fail"}')
    with open(f'{out}/summary.md', 'w') as f:
        f.write('## TraceQL against the reference: the shipped routes, warm medians\n\n')
        f.write('| ' + ' | '.join(head) + ' |\n|---|---:|---:|---:|---:|---|\n')
        for r in rows:
            f.write('| ' + ' | '.join(r) + ' |\n')
        f.write('\n' + '; '.join(f'{label} {counts[state]}' for label, state in COUNT_LINES) + '\n\n')
        f.write(f'**{"pass" if passes(counts) else "fail"}**: the rule needs 11 compared below, 2 recorded, '
                '1 exempt, 1 with no counterpart and nothing else.\n')


def self_test():
    good_p = {name: (f'{name}\t200\t1000\t1.0\t5.0\t9.0\t200\t2000\t2.0\t10.0\t20.0\t3\t3\t1.0\t100\t1000')
              for name, _ in SHAPES if not name.startswith('fetch_')}
    good_p['g01_service_graph'] = 'g01_service_graph\t200\t1000\t1.0\t5.0\t9.0\t-\t-\t-\t-\t-\t3\t-\t1.0\t100\t1000'
    f20p = 'PulsusDB\ttrace=aaaaaaaa\treps=21\tmin=1\tp50=4\tp90=6\tmax=9\tbytes=15000\n'
    f20r = 'the reference\ttrace=aaaaaaaa\treps=21\tmin=2\tp50=8\tp90=12\tmax=20\tbytes=16000\n'
    fbigp = 'PulsusDB\ttrace=bbbbbbbb\treps=21\tmin=1\tp50=14\tp90=16\tmax=19\tbytes=800000\n'
    fbigr = 'the reference\ttrace=bbbbbbbb\treps=21\tmin=2\tp50=28\tp90=32\tmax=40\tbytes=900000\n'
    good_f = f20p + f20r + fbigp + fbigr
    # every row's state on the good inputs, written out here rather than read
    # from SHAPES, so a kind changed there is a failed case
    good_states = {
        'fetch_trace_20': 'recorded', 'fetch_trace_largest': 'recorded',
        'm01_rate_by_service': 'below', 'm02_quantiles_by_route': 'below', 'm03_errors_by_service': 'below',
        'm04_histogram': 'below', 'm05_instant_avg_by_name': 'below', 'm06_rate_by_resource_attr': 'below',
        't01_tag_names_span': 'below', 't02_tag_values_route': 'below', 't03_tag_values_filtered': 'below',
        's08_user_point': 'below', 's14_trace_level': 'exempt', 'c08_compare': 'below',
        'g01_service_graph': 'no counterpart',
    }

    def run(p, f=good_f):
        return verdict('\n'.join(p.values()) + '\n', f)

    def states_are(rows, **changed):
        """every row's state is the good one, except those named"""
        want = good_states | {k: v for k, v in changed.items()}
        return len(rows) == 15 and {r[0]: r[5] for r in rows} == want

    fails = []

    def check(case, ok):
        print(f'{"ok  " if ok else "FAIL"} {case}')
        if not ok: fails.append(case)

    rows, counts = run(good_p)
    check('all fifteen good: counts 11/2/1/1/0, pass',
          states_are(rows) and passes(counts)
          and (counts.get('below'), counts.get('recorded'), counts.get('exempt'), counts.get('no counterpart'),
               counts.get('refused')) == (11, 2, 1, 1, 0))

    p = dict(good_p); p['m01_rate_by_service'] = 'm01_rate_by_service\t200\t1000\t1.0\t10.0\t19.0\t200\t2000\t2.0\t10.0\t20.0'
    rows, counts = run(p)
    check('equal medians: not below, fails', states_are(rows, m01_rate_by_service='not below') and not passes(counts))

    p = dict(good_p); p['m02_quantiles_by_route'] = 'm02_quantiles_by_route\t400\t60\t1.0\t5.0\t9.0\t200\t2000\t2.0\t10.0\t20.0'
    rows, counts = run(p)
    check('PulsusDB 400 on m02: refused, count 1, fails',
          states_are(rows, m02_quantiles_by_route='refused') and counts.get('refused') == 1 and not passes(counts))

    p = dict(good_p); p['t03_tag_values_filtered'] = 't03_tag_values_filtered\t200\t1000\t1.0\t5.0\t9.0'
    rows, counts = run(p)
    check("t03's reference half missing: reference failed, fails",
          states_are(rows, t03_tag_values_filtered='reference failed') and not passes(counts))

    p = dict(good_p); p['s14_trace_level'] = 's14_trace_level\t200\t1000\t1.0\t5.0\t9.0\t500\t80\t2.0\t10.0\t20.0'
    rows, counts = run(p)
    check('the reference 500 on s14: reference failed, fails',
          states_are(rows, s14_trace_level='reference failed') and not passes(counts))

    p = dict(good_p); del p['g01_service_graph']
    rows, counts = run(p)
    check('g01 absent: refused, fails', states_are(rows, g01_service_graph='refused') and not passes(counts))

    # the two trace-by-id rows are recorded: their order passes and fails nothing
    rows, counts = run(good_p, f20p.replace('p50=4', 'p50=80') + f20r + fbigp + fbigr)
    check("PulsusDB's p50 above the reference's on fetch_trace_20: recorded, pass",
          states_are(rows) and next(r[1] for r in rows if r[0] == 'fetch_trace_20') == '80' and passes(counts))

    # fetch.tsv is read by position: PulsusDB's line and then the reference's,
    # the 20-span trace's two and then the largest trace's
    rows, counts = run(good_p, f20p + f20r + fbigr)
    check("no PulsusDB line for fetch_trace_largest: refused, fails",
          states_are(rows, fetch_trace_largest='refused') and not passes(counts))

    # without the reference's first line every later line moves up one, so the
    # largest trace's PulsusDB line is not where it is read and that row is refused too
    rows, counts = run(good_p, f20p + fbigp + fbigr)
    check("no reference line for fetch_trace_20: reference failed, fails",
          states_are(rows, fetch_trace_20='reference failed', fetch_trace_largest='refused') and not passes(counts))

    # a fetch that failed on one side: fetch_compare.py writes the failed side
    # `failed` and the other `answered` (every call it was given answered 200,
    # untimed); the row is that side's failure, not the other's
    rows, counts = run(good_p, 'PulsusDB\ttrace=aaaaaaaa\tanswered\n'
                               'the reference\ttrace=aaaaaaaa\tfailed\n' + fbigp + fbigr)
    check("the reference's fetch failed on fetch_trace_20: reference failed, fails",
          states_are(rows, fetch_trace_20='reference failed') and not passes(counts))
    rows, counts = run(good_p, f20p + f20r + 'PulsusDB\ttrace=bbbbbbbb\tfailed\n'
                               'the reference\ttrace=bbbbbbbb\tanswered\n')
    check("PulsusDB's fetch failed on fetch_trace_largest: refused, fails",
          states_are(rows, fetch_trace_largest='refused') and not passes(counts))

    # an answer with nothing in it is not a like-for-like comparison,
    # whichever side gave it and however fast
    for name, row in (
        ('t02_tag_values_route', 't02_tag_values_route\t200\t1000\t1.0\t5.0\t9.0\t200\t55\t2.0\t10.0\t20.0\t58\t0\t1.0\t100\t1000'),
        ('s14_trace_level', 's14_trace_level\t200\t1000\t1.0\t5.0\t9.0\t200\t2000\t2.0\t10.0\t20.0\t0\t20\t1.0\t100\t1000'),
        ('g01_service_graph', 'g01_service_graph\t200\t1000\t1.0\t5.0\t9.0\t-\t-\t-\t-\t-\t0\t-\t1.0\t100\t1000'),
    ):
        p = dict(good_p); p[name] = row
        rows, counts = run(p)
        check(f'{name} with an empty side: not like for like, count 1, fails',
              states_are(rows, **{name: 'not like for like'}) and counts.get('not like for like') == 1
              and not passes(counts))

    print(f'bench_verdict.py self-test: {14 - len(fails)} of 14 cases as expected')
    return 1 if fails else 0


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-test']:
        sys.exit(self_test())
    out = sys.argv[1]
    rows, counts = verdict(open(f'{out}/paired.tsv').read(), open(f'{out}/fetch.tsv').read())
    write(out, rows, counts)
    sys.exit(0 if passes(counts) else 1)
