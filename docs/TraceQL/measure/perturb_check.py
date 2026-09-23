#!/usr/bin/env python3
"""Changes one rule at a time and requires the catalogue's comparison to notice.

`docs/TraceQL/query-catalogue.md` claims three things this script is the check on:

1. **The two sides are independent.** `catalogue_render.py` and
   `catalogue_interp.py` share the parser and nothing else, so a rule changed on
   EITHER side must produce a disagreement. Review round 4 found the opposite:
   one table served both sides, the reviewer changed `STATUS['error']` from 2 to
   1, and the catalogue still reported every answer as agreeing.
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
never written — and the whole catalogue is run against it.

Usage: perturb_check.py CH_URL DB CORPUS_DIR FIXTURE_JSONL WORK_DIR START_NS END_NS
"""
import json, os, shutil, subprocess, sys

HERE = os.path.dirname(os.path.abspath(__file__))
CH, DB, CORPUS, FIXTURE, WORK, S, E = sys.argv[1:8]
os.makedirs(WORK, exist_ok=True)

COPY = ['catalogue_parse.py', 'catalogue_render.py', 'catalogue_interp.py', 'catalogue.py',
        'validate_messages.tsv', 'planner_dispositions.tsv', 'catalogue-extra.tsv',
        'parse_vectors.tsv']

rows = []
for line in open(f'{HERE}/perturbations.tsv'):
    if line.startswith('#') or line.startswith('id\t') or not line.strip(): continue
    pid, side, f, expect, rule, find, repl = line.rstrip('\n').split('\t')
    rows.append((pid, side, f, expect, rule, json.loads(find), json.loads(repl)))

# Every anchor is checked BEFORE the first run, and all of them are reported at
# once: an anchor that has gone stale because the file it points into was edited
# is a defect in this file, not a property of the design, and finding them one
# nine-second run at a time hides how many there are.
stale = [(pid, f, open(f'{HERE}/{f}').read().count(find))
         for pid, side, f, expect, rule, find, repl in rows]
stale = [x for x in stale if x[2] != 1]
if stale:
    for pid, f, n in stale:
        print(f'anchor appears {n} times, not once: {pid} in {f}')
    print(f'perturbations {len(rows)}, failures {len(stale)} (no run attempted)')
    sys.exit(1)


def run_one(pid, f, find, repl):
    d = f'{WORK}/{pid}/measure'
    if os.path.exists(f'{WORK}/{pid}'): shutil.rmtree(f'{WORK}/{pid}')
    os.makedirs(f'{d}/results'); os.makedirs(f'{d}/catalogue-sql')
    for n in COPY: shutil.copy(f'{HERE}/{n}', f'{d}/{n}')
    src = open(f'{d}/{f}').read()
    if src.count(find) != 1:
        return 'anchor-not-unique', {}
    open(f'{d}/{f}', 'w').write(src.replace(find, repl))
    p = subprocess.run([sys.executable, f'{d}/catalogue.py', CH, DB, CORPUS, FIXTURE, d, S, E],
                       capture_output=True, text=True, timeout=1800)
    try:
        counts = json.load(open(f'{d}/results/catalogue.json'))['counts']
    except Exception:
        shutil.rmtree(f'{WORK}/{pid}')
        return 'crashed', {'stderr': p.stderr.strip().split('\n')[-1][:120]}
    shutil.rmtree(f'{WORK}/{pid}')
    return None, counts

out = [('id', 'side', 'expect', 'api_disagree', 'membership_disagree', 'statement_errors',
        'disposition_mismatch', 'parse_vector_mismatch', 'outcome', 'verdict', 'rule')]
fails = 0
for pid, side, f, expect, rule, find, repl in rows:
    bad, counts = run_one(pid, f, find, repl)
    if bad:
        api = mem = err = mis = vec = '-'
        outcome = bad
    else:
        served = counts['served_total']
        api = served - counts['api_agree_total']
        mem = served - counts['membership_agree_total']
        err = served - counts['ran_total']
        mis = counts['disposition_mismatches']
        vec = counts['parse_vector_mismatches']
        outcome = 'noticed' if (api or mem or err or mis or vec) else 'clean'
    want = 'noticed' if expect == 'red' else 'clean'
    verdict = 'PASS' if outcome == want else 'FAIL'
    if verdict == 'FAIL': fails += 1
    out.append((pid, side, expect, str(api), str(mem), str(err), str(mis), str(vec),
                outcome, verdict, rule))
    print('\t'.join(out[-1]), flush=True)

with open(f'{HERE}/results/perturbations.tsv', 'w') as fh:
    for r in out: fh.write('\t'.join(r) + '\n')
print(f'perturbations {len(rows)}, failures {fails}')
sys.exit(0 if fails == 0 else 1)
