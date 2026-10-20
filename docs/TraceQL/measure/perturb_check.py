#!/usr/bin/env python3
"""Changes one rule at a time and requires the catalogue's comparison to notice.

`docs/TraceQL/query-catalogue.md` claims three things this script is the check on:

1. **The interpreter's rules are checked.** Since #600 the other side is the
   shipped route, whose answers the saved run holds, so a rule changed in
   `catalogue_interp.py` must produce a disagreement. A row on
   `catalogue_render.py` changes the design renderer's statements, which the
   catalogue no longer runs (only its disposition rule is read), so it ends
   `clean`.
2. **The shared parser is checked too.** A rule changed THERE moves both answers
   together, so no comparison can see it; `measure/parse_vectors.tsv` holds the
   tree each rule must produce, written out by hand, and the rows below whose
   file is `catalogue_parse.py` are what says that check bites. Round 5 found
   the hole: a wrong `ms` multiplier in the shared module left every answer
   agreeing, so the multipliers moved to the two sides and the rest of the
   module is covered by the vectors.
3. **The queries discriminate.** A rule no query reaches is a rule the catalogue
   does not check, whatever its agreement count says. A row marked
   `not_discriminated` is exactly that, and the catalogue document has to say
   why. Round 5 found eight, and the query that reaches the first of them —
   `by(<an attribute>)` — turned out not to run at all, so
   `measure/catalogue-extra.tsv` now carries one query per rule the corpus does
   not reach.

Each row of `perturbations.tsv` names a file, a string to replace and what is
expected. The replacement is made in a scratch copy — the committed files are
never written — and `catalogue.py` is run in that copy over a saved run of
`catalogue.sh`: the route's answers stay as the run recorded them, and the
changed rule moves only this side's. The unchanged copy must pass first.

Usage: perturb_check.py RUN_JSONL FIXTURE_JSONL START_NS END_NS WORK_DIR

RUN_JSONL and FIXTURE_JSONL are `catalogue.sh`'s `catalogue-run.jsonl` and
`cat/spans.jsonl`; START_NS and END_NS the window it ran over. Writes
`results/perturbations.tsv`; exits 0 only when every row ends as it expects.
"""
import json, os, re, shutil, subprocess, sys

HERE = os.path.dirname(os.path.abspath(__file__))
RUN, FIXTURE, S, E, WORK = sys.argv[1:6]
RUN, FIXTURE, WORK = os.path.abspath(RUN), os.path.abspath(FIXTURE), os.path.abspath(WORK)
os.makedirs(WORK, exist_ok=True)

COPY = ['catalogue_parse.py', 'catalogue_render.py', 'catalogue_interp.py', 'catalogue.py',
        'catalogue-extra.tsv', 'parse_vectors.tsv']

rows = []
for line in open(f'{HERE}/perturbations.tsv'):
    if line.startswith('#') or line.startswith('id\t') or not line.strip(): continue
    pid, side, f, expect, rule, find, repl = line.rstrip('\n').split('\t')
    rows.append((pid, side, f, expect, rule, json.loads(find), json.loads(repl)))

# Every anchor is checked BEFORE the first run, and all of them are reported at
# once: an anchor that has gone stale because the file it points into was edited
# is a defect in this file, not a property of the design.
stale = [(pid, f, open(f'{HERE}/{f}').read().count(find))
         for pid, side, f, expect, rule, find, repl in rows]
stale = [x for x in stale if x[2] != 1]
if stale:
    for pid, f, n in stale:
        print(f'anchor appears {n} times, not once: {pid} in {f}')
    print(f'perturbations {len(rows)}, failures {len(stale)} (no run attempted)')
    sys.exit(1)


def count(out, label):
    m = re.search(rf'^{re.escape(label)}\s+(\d+)', out, re.M)
    return int(m.group(1)) if m else 0


def run_one(name, f=None, find=None, repl=None):
    """`catalogue.py` in a scratch copy with one rule changed: None when it
    crashed, else (exit status, counts)"""
    d = f'{WORK}/{name}'
    shutil.rmtree(d, ignore_errors=True); os.makedirs(d)
    for n in COPY: shutil.copy(f'{HERE}/{n}', f'{d}/{n}')
    if f is not None:
        src = open(f'{d}/{f}').read()
        open(f'{d}/{f}', 'w').write(src.replace(find, repl))
    p = subprocess.run([sys.executable, '-I', f'{d}/catalogue.py', RUN, FIXTURE, S, E],
                       capture_output=True, text=True, timeout=600)
    shutil.rmtree(d)
    if 'Traceback' in p.stderr or p.returncode not in (0, 1):
        return None, p.stderr.strip().split('\n')[-1][:160]
    notes = [l.split()[1:3] for l in p.stdout.splitlines() if l.startswith('  disagreement: ')]
    metric = sum(1 for n in notes if n[1] in ('(range)', '(instant)'))
    return p.returncode, {'search': len(notes) - metric, 'metrics': metric,
                          'errors': count(p.stdout, 'statements that failed'),
                          'mismatch': count(p.stdout, 'disposition rule against the planner'),
                          'vectors': count(p.stdout, 'parse vectors that differ')}


base, why = run_one('unchanged')
if base != 0:
    print(f'the unchanged catalogue does not pass over this run: {why or "exit 1"}')
    sys.exit(1)

out = [('id', 'side', 'expect', 'search_disagree', 'metrics_disagree', 'statement_errors',
        'disposition_mismatch', 'parse_vector_mismatch', 'outcome', 'verdict', 'rule')]
fails = 0
for pid, side, f, expect, rule, find, repl in rows:
    status, c = run_one(pid, f, find, repl)
    if status is None:
        cols, outcome = ['-'] * 5, 'crashed'
    else:
        cols = [str(c[k]) for k in ('search', 'metrics', 'errors', 'mismatch', 'vectors')]
        outcome = 'noticed' if status else 'clean'
    want = 'noticed' if expect == 'red' else 'clean'
    verdict = 'PASS' if outcome == want else 'FAIL'
    fails += verdict == 'FAIL'
    out.append((pid, side, expect, *cols, outcome, verdict, rule))
    print('\t'.join(out[-1]), flush=True)

with open(f'{HERE}/results/perturbations.tsv', 'w') as fh:
    for r in out: fh.write('\t'.join(r) + '\n')
print(f'perturbations {len(rows)}, failures {fails}')
sys.exit(0 if fails == 0 else 1)
