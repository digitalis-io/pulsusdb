#!/usr/bin/env python3
"""The verdict of a bench.sh run on functional-requirements.md §6.3's rule.

Reads OUT/paired.tsv (http_bench.py paired) and OUT/fetch.tsv
(fetch_compare.py, the 20-span trace's two lines and then the largest trace's),
prints one row per §6.3 shape and then each state's count, and writes the same
as Markdown to OUT/summary.md. Exits 0 only for 13 `below`, 1 `exempt`,
1 `no counterpart` and nothing else.

Usage: bench_verdict.py OUT  |  bench_verdict.py --self-test
"""
import sys

# §6.3's fifteen shapes, in its table's order: (name, how it is judged)
SHAPES = [
    ('fetch_trace_20', 'compared'),
    ('fetch_trace_largest', 'compared'),
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
STATES = ['below', 'not below', 'exempt', 'no counterpart', 'refused', 'reference failed']
PASS = {'below': 13, 'exempt': 1, 'no counterpart': 1}


def verdict(paired_text, fetch_text):
    """(rows, counts): rows are (shape, pulsus_ms, reference_ms, pulsus_bytes,
    reference_bytes, state). A shape missing from an input is refused on that
    side's account."""
    halves = {}            # shape -> {side: (status, ms, bytes)}, side 'p' or 'r'
    for line in paired_text.splitlines():
        c = line.split('\t')
        if len(c) < 6 or not c[0]: continue
        h = halves.setdefault(c[0], {})
        h['p'] = (c[1], c[4], c[2])
        if len(c) >= 11 and c[6] != '-':
            h['r'] = (c[6], c[9], c[7])
    fetch = [dict(kv.split('=', 1) for kv in line.split('\t')[1:] if '=' in kv) | {'side': line.split('\t')[0]}
             for line in fetch_text.splitlines() if line.strip()]
    for shape, at in (('fetch_trace_20', 0), ('fetch_trace_largest', 2)):
        h = halves.setdefault(shape, {})
        for side, i, who in (('p', at, 'PulsusDB'), ('r', at + 1, 'the reference')):
            if i < len(fetch) and fetch[i]['side'] == who and 'p50' in fetch[i]:
                h[side] = ('200', fetch[i]['p50'], fetch[i].get('bytes', ''))
    rows, counts = [], {s: 0 for s in STATES}
    for shape, kind in SHAPES:
        h = halves.get(shape, {})
        p, r = h.get('p'), h.get('r')
        if p is None or p[0] != '200':
            state = 'refused'
        elif kind == 'no counterpart':
            state = 'no counterpart'
        elif r is None or r[0] != '200':
            state = 'reference failed'
        elif kind == 'exempt':
            state = 'exempt'
        else:
            state = 'below' if float(p[1]) < float(r[1]) else 'not below'
        counts[state] += 1
        rows.append((shape, p[1] if p else '-', r[1] if r else '-', p[2] if p else '-', r[2] if r else '-', state))
    return rows, counts


def passes(counts):
    return {k: v for k, v in counts.items() if v} == PASS


COUNT_LINES = [('compared below', 'below'), ('exempt', 'exempt'), ('no counterpart', 'no counterpart'),
               ('refused', 'refused'), ('not below', 'not below'), ('reference failed', 'reference failed')]


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
        f.write(f'**{"pass" if passes(counts) else "fail"}**: the rule needs 13 compared below, 1 exempt, '
                '1 with no counterpart and nothing else.\n')


def self_test():
    good_p = {name: (f'{name}\t200\t1000\t1.0\t5.0\t9.0\t200\t2000\t2.0\t10.0\t20.0\t1.0\t100\t1000')
              for name, _ in SHAPES if not name.startswith('fetch_')}
    good_p['g01_service_graph'] = 'g01_service_graph\t200\t1000\t1.0\t5.0\t9.0\t-\t-\t-\t-\t-\t1.0\t100\t1000'
    good_f = ('PulsusDB\ttrace=aaaaaaaa\treps=21\tmin=1\tp50=4\tp90=6\tmax=9\tbytes=15000\n'
              'the reference\ttrace=aaaaaaaa\treps=21\tmin=2\tp50=8\tp90=12\tmax=20\tbytes=16000\n'
              'PulsusDB\ttrace=bbbbbbbb\treps=21\tmin=1\tp50=14\tp90=16\tmax=19\tbytes=800000\n'
              'the reference\ttrace=bbbbbbbb\treps=21\tmin=2\tp50=28\tp90=32\tmax=40\tbytes=900000\n')

    def run(p):
        return verdict('\n'.join(p.values()) + '\n', good_f)

    def state(rows, name):
        return next((r[5] for r in rows if r[0] == name), None)

    fails = []

    def check(case, ok):
        print(f'{"ok  " if ok else "FAIL"} {case}')
        if not ok: fails.append(case)

    rows, counts = run(good_p)
    check('all fifteen good: pass', passes(counts) and len(rows) == 15)

    p = dict(good_p); p['m01_rate_by_service'] = 'm01_rate_by_service\t200\t1000\t1.0\t10.0\t19.0\t200\t2000\t2.0\t10.0\t20.0'
    rows, counts = run(p)
    check('equal medians: not below, fails', state(rows, 'm01_rate_by_service') == 'not below' and not passes(counts))

    p = dict(good_p); p['m02_quantiles_by_route'] = 'm02_quantiles_by_route\t400\t60\t1.0\t5.0\t9.0\t200\t2000\t2.0\t10.0\t20.0'
    rows, counts = run(p)
    check('PulsusDB 400 on m02: refused, count 1, fails',
          state(rows, 'm02_quantiles_by_route') == 'refused' and counts.get('refused') == 1 and not passes(counts))

    p = dict(good_p); p['t03_tag_values_filtered'] = 't03_tag_values_filtered\t200\t1000\t1.0\t5.0\t9.0'
    rows, counts = run(p)
    check("t03's reference half missing: reference failed, fails",
          state(rows, 't03_tag_values_filtered') == 'reference failed' and not passes(counts))

    p = dict(good_p); p['s14_trace_level'] = 's14_trace_level\t200\t1000\t1.0\t5.0\t9.0\t500\t80\t2.0\t10.0\t20.0'
    rows, counts = run(p)
    check('the reference 500 on s14: reference failed, fails',
          state(rows, 's14_trace_level') == 'reference failed' and not passes(counts))

    p = dict(good_p); del p['g01_service_graph']
    rows, counts = run(p)
    check('g01 absent: refused, fails', state(rows, 'g01_service_graph') == 'refused' and not passes(counts))

    print(f'bench_verdict.py self-test: {6 - len(fails)} of 6 cases as expected')
    return 1 if fails else 0


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-test']:
        sys.exit(self_test())
    out = sys.argv[1]
    rows, counts = verdict(open(f'{out}/paired.tsv').read(), open(f'{out}/fetch.tsv').read())
    write(out, rows, counts)
    sys.exit(0 if passes(counts) else 1)
