#!/usr/bin/env python3
"""How big each candidate domain is for "which numbers in these documents are
counts of a set in this repository".

The question decides one design: whether the judgement can be swept out of the
prose mechanically, or has to be written down. `claims_check.py` sweeps two
narrow forms and requires everything else to be registered by hand, and this
script is the measurement behind that split — the population each wider rule
would face, with the rule printed beside it so a reader can re-run it.

It reads every Markdown document under `docs/TraceQL`, found on the filesystem
from this script's own location rather than from the index, so it reads the same
set whether or not those documents have been committed yet. Usage, from anywhere:

  claim_domains.py            print the table
  claim_domains.py --write    write it to measure/claim-domains.txt

`claims_check.py` re-runs these rules and fails when the committed file
disagrees with the tree, so the populations cannot go stale behind a document
edit. They are not restated in any document: a sentence counting the numbers in
the document it sits in changes itself when it is written.
"""
import glob, os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, '..', '..', '..'))
WORDS = ('one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|'
         'fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty')
NOUNS = ('files|cases|claims|queries|rules|statements|alternatives|guards|rows|entries|'
         'tables|columns|windows|requirements|readers|spans|traces|steps|parts|scripts|'
         'documents|sections|tests|vectors|keys|shards|replicas|partitions|merges|mutations')

RULES = [
    ('every standalone number',
     r'(?<![\w.])\d[\d,]*(?![\w.])'),
    ('every bold span holding a number',
     r'\*\*[^*]*\d[^*]*\*\*'),
    ('a number then a countable noun',
     rf'(?<![\w.])(?:\d[\d,]*|{WORDS})\s+(?:\*\*)?(?:{NOUNS})\b'),
    ('SWEPT: a parenthesised count, or a file count',
     rf'\((?:\d[\d,]*|{WORDS}) [a-z][a-z -]*[a-z]\)|(?<![\w.])(?:\d[\d,]*|{WORDS}) files?\b'),
]


OUT = os.path.join(HERE, 'claim-domains.txt')


def documents():
    """every Markdown document under docs/TraceQL, as repository-relative paths.

    The first version asked the index (`git ls-files`). That reads the documents
    only once somebody has committed them, and it read NOTHING while this design
    sat in a working copy as untracked files — a state the run refuses loudly
    below rather than reporting an empty domain (review round 10, item 14). The
    filesystem is the set either way, and it also sees a document added and not
    yet staged, which is exactly when an unchecked number gets in.
    """
    root = os.path.join(REPO, 'docs/TraceQL')
    return sorted(os.path.relpath(p, REPO)
                  for p in glob.glob(os.path.join(root, '**', '*.md'), recursive=True))


def report():
    """the table, as text — no head, no date, so two runs over one tree agree"""
    docs = documents()
    if not docs:
        raise SystemExit('no TraceQL document found under docs/TraceQL: run this against a checkout')
    text = {d: ' '.join(open(os.path.join(REPO, d)).read().split()) for d in docs}
    lines = [f'{len(docs)} documents under docs/TraceQL:']
    lines += [f'    {d}' for d in docs]
    lines += ['', f'{"rule":46} {"occurrences":>11}']
    for name, pat in RULES:
        rx = re.compile(pat)
        lines.append(f'{name:46} {sum(len(rx.findall(text[d])) for d in docs):11}')
    lines += ['', 'the rules, as written:']
    for name, pat in RULES:
        lines += [f'    {name}', f'        {pat}']
    return '\n'.join(lines) + '\n'


def main():
    text = report()
    if '--write' in sys.argv:
        open(OUT, 'w').write(text)
        print(f'wrote {os.path.relpath(OUT, REPO)}')
    else:
        sys.stdout.write(text)
    return 0


if __name__ == '__main__':
    sys.exit(main())
