#!/usr/bin/env python3
"""Runs the catalogue again and checks it reproduced every file it wrote,
except the one wall-clock column.

`measure/README.md` says the catalogue's documents, its SQL and its result JSON
come back byte-identical on a second run apart from `ms`. This is that claim,
asked of the files — and asked in a way that cannot pass by looking at nothing:

* **it runs the catalogue itself**, so "the run happened" is part of the check
  rather than an assumption about what happened earlier in the session;
* **the file list is the run's own**, `results/catalogue-outputs.txt`, which
  `catalogue.py` writes as it writes each file. There is no path list here;
* that list is **deleted before the catalogue is started**, so it can only be
  there afterwards if this run wrote it. An earlier version compared its
  modification time against the moment the run began, and a one-second
  allowance let a catalogue that did nothing pass after the old list was
  touched (review round 8). A file that is not there cannot be back-dated;
* anything `git` reports as modified under `docs/TraceQL/` that the run did not
  write is reported too, because that is something else moving.

Two files carry the timing reading, and only there is a difference allowed:
`results/catalogue.json`'s `statement_ms` member, and the last cell of each
table row in `query-catalogue-accepted.md`. **Both are masked in the text, not
in a parsed structure**: an earlier version decoded the JSON and re-encoded it,
which accepted a change of indentation as timing-only (review round 8).

A previous version of this check took a hand-written path list, which silently
omitted `query-catalogue-accepted.md` — one of the two files the run changes —
and reported success on an empty list.

Usage, with the same arguments `catalogue.py` takes:

    reproduce_check.py CH_URL DB CORPUS_DIR FIXTURE_JSONL OUT_DIR START_NS END_NS [GIT_REVISION]

The second run leaves the working tree differing in the `ms` column; restore it
with `git checkout -- docs/TraceQL` when the check has passed. If the catalogue
fails, the file list it was to write is absent — `git checkout` brings that back
too.
"""
import os, re, subprocess, sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, '..', '..', '..'))
ARGS = sys.argv[1:8]
REV = sys.argv[8] if len(sys.argv) > 8 else 'HEAD'
if len(ARGS) != 7:
    sys.exit(__doc__)
OUT = os.path.abspath(ARGS[4].rstrip('/'))
MANIFEST = f'{OUT}/results/catalogue-outputs.txt'


def git(*a):
    return subprocess.run(['git', '-C', REPO, *a], capture_output=True, text=True, check=True).stdout


def at_revision(rev, path):
    """the committed bytes, or None where the revision does not carry the file"""
    r = subprocess.run(['git', '-C', REPO, 'show', f'{rev}:{path}'], capture_output=True, text=True)
    return r.stdout if r.returncode == 0 else None


# Deleted first: a list that is there afterwards was written by this run, and a
# run that writes nothing leaves nothing to mistake for success.
if os.path.exists(MANIFEST):
    os.remove(MANIFEST)
run = subprocess.run([sys.executable, f'{HERE}/catalogue.py', *ARGS], cwd=REPO,
                     capture_output=True, text=True)
tail = [l for l in run.stdout.strip().split('\n') if l.strip()]
print(f'catalogue exit {run.returncode}: {tail[-1] if tail else "(no output)"}')
if run.returncode != 0:
    print(run.stdout[-2000:], run.stderr[-2000:], sep='\n')
    sys.exit(1)

if not os.path.exists(MANIFEST):
    sys.exit(f'the catalogue wrote no file list at {MANIFEST}: it wrote nothing at all')
listed = [l.strip() for l in open(MANIFEST) if l.strip()]
if not listed:
    sys.exit(f'{MANIFEST} is empty: the catalogue wrote nothing')

ROW_MS = re.compile(r'\| *\d+ *\|\s*$')


JSON_MS = re.compile(r'("statement_ms":\s*)\d+')


def blind(path, text):
    """the file with its one timing reading masked — in the TEXT, so every other
    byte still has to match, indentation and key order included"""
    if path.endswith('results/catalogue.json'):
        return JSON_MS.sub(r'\1ms', text)
    if path.endswith('query-catalogue-accepted.md'):
        return '\n'.join(ROW_MS.sub('| ms |', l) for l in text.split('\n'))
    return text


TIMED = ('results/catalogue.json', 'query-catalogue-accepted.md')
differ, missing, fresh = [], [], []
for p in listed:
    full = os.path.join(REPO, p)
    if not os.path.exists(full):
        missing.append(p); continue
    was = at_revision(REV, p)
    if was is None:
        fresh.append(p); continue
    if blind(p, was) != blind(p, open(full).read()):
        differ.append(p)
changed = {l[3:].strip() for l in git('status', '--porcelain', '--', 'docs/TraceQL').splitlines() if l.strip()}
stray = sorted(changed - set(listed))

for p in sorted(missing): print('MISSING  ', p)
for p in sorted(fresh): print(f'NEW       (not in {REV})', p)
for p in sorted(differ): print('DIFFERS  ', p)
for p in stray: print('NOT THIS RUN\'S', p, '- modified, and the catalogue did not write it')
for p in listed:
    if p.endswith(TIMED) and p not in differ and p not in missing and p not in fresh:
        print('same      (timing blinded)', p)
print(f'files the run wrote {len(listed)}, of them timing-blinded {sum(1 for p in listed if p.endswith(TIMED))}, '
      f'missing {len(missing)}, new since {REV} {len(fresh)}, '
      f'differing beyond the timing column {len(differ)}, '
      f'modified but not written by the run {len(stray)}')
sys.exit(0 if not (missing or differ or stray or fresh) else 1)
