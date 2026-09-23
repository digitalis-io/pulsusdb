#!/usr/bin/env python3
"""Every counted claim in `measure/claims.tsv`, asked of the thing it counts.

A sentence that says how many of something there is, or that lists a set, is the
defect this branch has produced more than any other: five readers that were
twelve, a case count of 45 that was 68 and then 70 that was 73, ten design
queries that were twelve, seven files read inside a run that were eight. Each was
found by a search, each search was narrower than the claim, and each fix was
another hand-kept snapshot.

So the documents are not the authority for a count. `measure/claims.tsv` names
each claim, the document it is made in, the pattern that finds it, and the
derivation that answers it from the tree. This script runs them.

Three properties this file is built to keep, each because the first version
lacked it (review round 8):

* **every occurrence is compared, not the first.** A claim repeated in three
  places is three comparisons; a pattern that matches nothing is a failure, not
  a silent pass.
* **nothing printed is a literal.** The number of claims checked is the number of
  rows executed, so deleting a rule changes the total rather than hiding in it.
* **a registry row and a derivation cannot drift apart.** A row naming a
  derivation that does not exist fails, and a derivation no row uses fails.

What it does NOT do: decide which numbers in a document are set counts. That
judgement is `claims.tsv` itself — a counted claim nobody has registered is
unchecked, and a reader who wants to know what is checked reads that file.

Usage, from anywhere:  claims_check.py [--list]
"""
import glob, json, os, re, subprocess, sys
from collections import Counter

# the sweep rules live in one file, used by the script that reports their
# populations and by the check below; no bytecode is left in the source tree
sys.dont_write_bytecode = True
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import claim_domains  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, '..', '..', '..'))
DOCS = os.path.join(REPO, 'docs/TraceQL')
SCRIPTS = ['docs/TraceQL/measure/*.py', 'docs/TraceQL/measure/*.sh', 'docs/TraceQL/measure/fixture/*.py']
problems = []

# Counts of what ships TODAY — the files in a kept directory, the migrations the
# design replaces — are read at the revision named in `measure/source-revision.txt`,
# never from whatever the working copy happens to be at.
#
# Review round 10 read `crates/pulsus-schema/src/catalog.rs` in a checkout two
# commits behind the default branch, found 32 trace-family migrations where the
# design says 36, and reported the design wrong. The design was right; the
# checkout was stale, and nothing in the run could say so. A working copy is not
# a fact about the product, and a number derived from one follows it in silence.
#
# The price is that these counts describe the baseline the design was written
# against rather than the tip of the branch. That is what the documents mean —
# `server-implementation.md` section 6 is what is kept, replaced and deleted
# relative to a known state — and the revision is named in the document so a
# reader can see which state.


def read(p):
    return open(p).read()


def flat(p):
    """the file with its line breaks made spaces, so a sentence that wrapped is
    still one sentence to a pattern"""
    return ' '.join(read(p).split())


def git(*a):
    return subprocess.run(['git', '-C', REPO, *a], capture_output=True, text=True, check=True,
                          stdin=subprocess.DEVNULL).stdout


BASELINE = read(f'{HERE}/source-revision.txt').split()[0]
if subprocess.run(['git', '-C', REPO, 'cat-file', '-e', f'{BASELINE}^{{commit}}'],
                  capture_output=True, stdin=subprocess.DEVNULL).returncode != 0:
    raise SystemExit(f'the baseline revision {BASELINE} named in measure/source-revision.txt is not in '
                     f'this clone: fetch it, or run this against a clone of the repository. Nothing '
                     f'below can be derived without it.')


def at_baseline(rel):
    """one file, as it stands at the baseline revision"""
    return git('show', f'{BASELINE}:{rel}')


def files_at_baseline(rel):
    """the paths under `rel` at the baseline revision"""
    return git('ls-tree', '-r', '--name-only', BASELINE, '--', rel).split()


def repo_files(patterns):
    """repository-relative paths matching shell patterns, from the FILESYSTEM.

    Not `git ls-files`: this design sat in a working copy as untracked files for
    a whole review round, and every rule keyed on the index then enumerated the
    empty set — which is a silent pass for a sweep, not a failure. The
    filesystem is the same set once the documents are committed, and it is the
    right set before that.
    """
    out = []
    for pat in patterns:
        out += [os.path.relpath(p, REPO) for p in glob.glob(os.path.join(REPO, pat))]
    return sorted(out)


WORDS = {w: i for i, w in enumerate(
    'zero one two three four five six seven eight nine ten eleven twelve thirteen fourteen '
    'fifteen sixteen seventeen eighteen nineteen twenty'.split())}


def as_number(text):
    """a claim's number, however the sentence spells it"""
    t = text.strip().lower().replace(',', '')
    return str(WORDS[t]) if t in WORDS else t


def rows_of(path):
    return [l for l in read(path).split('\n') if l.strip() and not l.startswith('#')]


def sole(values, what):
    """the one value they all share; anything else is the failure, reported here
    rather than silently reduced to a number"""
    seen = set(values)
    if len(seen) == 1:
        return seen.pop()
    problems.append(f'{what}: expected one value across the set, the tree has {sorted(seen)}')
    return -1


def tsv_cell(path, row_key, column):
    """one cell of a committed result table, found by its first column"""
    lines = [l.split('\t') for l in rows_of(path)]
    head = lines[0]
    for l in lines[1:]:
        if l[0] == row_key:
            return l[head.index(column)]
    problems.append(f'{path}: no row named {row_key}')
    return -1


def lines_in(rel):
    return len(read(os.path.join(REPO, rel)).split('\n')) - 1


def _array(text, decl, what):
    """the body of one `pub const NAME: &[T] = &[ ... ];` array"""
    start = text.find(decl)
    if start < 0:
        problems.append(f'catalog.rs no longer declares {what}: {decl}')
        return ''
    return text[start:text.index('\n];', start)]


def trace_catalogue():
    """the migrations and the materialized views that name a trace table.

    Parsed out of the `MIGRATIONS` and `MVS` arrays, at the baseline revision.
    Two things the first version got wrong, and why the parse is worth its size:

    * it searched the WHOLE FILE for `family: Some(Family::Traces),`, which also
      matches the unit tests below the arrays. Four test assertions carry that
      text, so the same expression reads 36 inside the array and 40 over the
      file, and which one it returns depends on where the array happens to end.
    * it counted a FAMILY where the document names a SET. `trace_tag_catalog`
      has two migrations — ids 18 and 41 — and both carry `family: None`. A
      coder told to replace "the trace-family migrations" would leave those two
      behind while deleting the table they manage. The set the design replaces
      is every migration whose NAME is a trace table, which is 38, not 36.
    """
    src = at_baseline('crates/pulsus-schema/src/catalog.rs')
    migs = re.findall(r'\n    Migration \{(.*?)\n    \},', _array(
        src, 'pub const MIGRATIONS: &[Migration] = &[', 'MIGRATIONS'), re.S)
    mvs = re.findall(r'\n    MvDef \{(.*?)\n    \},', _array(
        src, 'pub const MVS: &[MvDef] = &[', 'MVS'), re.S)
    if not migs or not mvs:
        problems.append('catalog.rs: the MIGRATIONS/MVS arrays parsed to '
                        f'{len(migs)} and {len(mvs)} records — the shape has changed')
    named = lambda recs: [re.search(r'name: "([^"]+)"', r).group(1) for r in recs]
    return ([n for n in named(migs) if n.startswith('trace_')],
            [n for n in named(mvs) if n.startswith('trace_')])


TRACE_MIGRATIONS, TRACE_MVS = trace_catalogue()


# ------------------------------------------------------------- derivations --
FR = os.path.join(DOCS, 'functional-requirements.md')
CATALOGUE = json.load(open(f'{HERE}/results/catalogue.json'))
COUNTS = CATALOGUE['counts']
CASE_ROWS = [l for l in read(FR).split('\n') if re.match(r'^\| `T-[A-Z]', l)]
CASE_IDS = [re.match(r'^\| `(T-[A-Za-z0-9]+)`', l).group(1) for l in CASE_ROWS]
# A row is a guard iff it carries the bold marker. Which column that sits in
# differs between the tables of §8, so the marker is the rule rather than a
# position, and a row that merely says the word does not count.
GUARDS = [i for i, l in zip(CASE_IDS, CASE_ROWS) if '**guard**' in l]
REQUESTS = [r.split('\t') for r in rows_of(f'{HERE}/api_requests.tsv')][1:]
SQL_FILES = {os.path.basename(p)[:-4] for p in glob.glob(f'{HERE}/sql/*.sql')}


# The §8 preamble names each family by what it tests, and every case id carries
# its family letter, so each prose term is derived on its own. Summing the terms
# and comparing the sum with the total — which is what the first version did —
# passes an error in one term that another term cancels (review round 8).
FAMILIES = {'schema and storage': 'S', 'retention': 'R', 'window': 'B',
            'statement-count and pushdown': 'Q', 'compiler constructs': 'A', 'tag': 'T',
            'write path': 'W', 'protection': 'X', 'corpus-scale': 'CP'}
FAMILY_OF = Counter(re.match(r'^\| `T-([A-Z])', l).group(1) for l in CASE_ROWS)

MARKER_TEST = 'crates/pulsus-model/tests/doc_verification_markers.rs'


def storage_total(column, places=None):
    """one cell of the totals block of results/storage.tsv.

    That file holds two blocks: one row per table, then one row of totals under
    its own header. `tsv_cell` finds a row by its first column and cannot reach
    the second block, so the totals were restated in three documents and drifted
    in all three — 74,551,799 bytes stored, 74,555,945 in one document and
    74,561,477 in another, and no result file carrying either.
    """
    lines = [l.split('\t') for l in rows_of(f'{HERE}/results/storage.tsv')]
    for head, row in zip(lines, lines[1:]):
        if column in head and len(row) == len(head):
            v = row[head.index(column)]
            return v if places is None else f'{float(v):.{places}f}'
    problems.append(f'results/storage.tsv has no totals column named {column}')
    return -1


def catalogue_sql_per_query():
    """the files per served query in catalogue-sql/ — over the SERVED SET.

    The first version counted the files sharing each basename present in the
    directory and required one value. That is true of an empty directory, and it
    stays true when both files of one query are deleted: every remaining counter
    is still 2 (review round 10, item 3). So the denominator is the set of served
    queries the catalogue run recorded, and a name in one set and not the other
    is the failure.
    """
    served = ({e['name'] for e in CATALOGUE['accepted']}
              - set(CATALOGUE['planner_refused']))
    per = Counter(os.path.basename(f).split('.')[0] for f in glob.glob(f'{HERE}/catalogue-sql/*.sql'))
    missing, extra = sorted(served - set(per)), sorted(set(per) - served)
    if missing or extra:
        problems.append(f'catalogue-sql/ and the served queries of results/catalogue.json are not the '
                        f'same set: served with no files {missing[:5]}, files with no served query '
                        f'{extra[:5]} ({len(missing)} and {len(extra)} in all)')
    return sole([per[n] for n in sorted(served)], 'the files per served query in catalogue-sql/')


DERIVE = {
    'corpus_parsed': lambda: COUNTS['parsed'],
    'corpus_planner_refused': lambda: COUNTS['refused_by_planner'],
    'corpus_served': lambda: COUNTS['served'],
    'corpus_api_agree': lambda: COUNTS['api_agree'],
    'corpus_membership_agree': lambda: COUNTS['membership_agree'],
    'corpus_parser_refused': lambda: COUNTS['refused_before_planning'],
    'corpus_refused_total': lambda: COUNTS['refused_before_planning'] + COUNTS['refused_by_planner'],
    'corpus_all': lambda: COUNTS['parsed'] + COUNTS['refused_before_planning'],
    'design_queries': lambda: len(rows_of(f'{HERE}/catalogue-extra.tsv')),
    'cases_total': lambda: len(CASE_ROWS),
    'cases_guard': lambda: len(GUARDS),
    'cases_new': lambda: len(CASE_ROWS) - len(GUARDS),
    'layout_alternatives': lambda: read(f'{HERE}/layouts.sql').count('CREATE TABLE'),
    'sql_statements': lambda: len(SQL_FILES),
    'sql_statements_with_route': lambda: sum(1 for r in REQUESTS
                                             if r[0] in SQL_FILES and len(r) > 2 and r[2].strip() != '-'),
    'traceql_src_files': lambda: len(files_at_baseline('crates/pulsus-traceql/src')),
    'traces_api_files': lambda: len(files_at_baseline('crates/pulsus-server/src/traces_api')),
    'read_traces_files': lambda: len(files_at_baseline('crates/pulsus-read/src/traces')),
    # the trace test tree named in the kept table: one directory, so the
    # sentence and the derivation cannot mean different sets. It held `379`
    # from the first commit of this branch and the tree has never had 379
    # (review round 9).
    'traceql_tests_files': lambda: len(files_at_baseline('crates/pulsus-traceql/tests')),
    'trace_migrations': lambda: len(TRACE_MIGRATIONS),
    'trace_mvs': lambda: len(TRACE_MVS),
    # The two entries this design needs in the repository's marker registry,
    # which is the whole of what it changes outside docs/TraceQL. They went
    # missing once already, while a branch was being deleted, and nothing here
    # could see it: the count is the difference between the working file and the
    # baseline, so removing either entry fails the run.
    'marker_entries_added': lambda: (
        read(os.path.join(REPO, MARKER_TEST)).count('file: "docs/TraceQL/')
        - at_baseline(MARKER_TEST).count('file: "docs/TraceQL/')),
    'catalogue_sql_per_query': lambda: catalogue_sql_per_query(),
    'resource_rows': lambda: tsv_cell(f'{HERE}/results/storage.tsv', 'resources', 'rows'),
    'tag_names_rows': lambda: tsv_cell(f'{HERE}/results/storage.tsv', 'tag_names', 'rows'),
    'tag_values_rows': lambda: tsv_cell(f'{HERE}/results/storage.tsv', 'tag_values', 'rows'),
    'corpus_spans': lambda: json.load(open(f'{HERE}/results/corpus-summary.json'))['spans'],
    # measured, not quoted from anybody's documentation: one insert of 2,000 rows
    # each carrying a distinct path, into a column declared `JSON` with no
    # parameter. The control in the same result declares the parameter as 8 and
    # reads 8, which is what says the figure is the setting and not the probe.
    'json_max_dynamic_paths': lambda: tsv_cell(f'{HERE}/results/json-paths-default.tsv',
                                               'per_part_one_insert', 'dynamic'),
    'store_total_bytes': lambda: storage_total('total_bytes'),
    'store_bytes_per_span': lambda: storage_total('bytes_per_span', 3),
    'store_index_overhead_pct': lambda: storage_total('index_overhead_pct', 3),
    'store_rows_per_span': lambda: storage_total('rows_per_span', 3),
    'store_total_rows': lambda: storage_total('total_rows'),
    'counted_shapes_rows': lambda: len(rows_of(f'{HERE}/counted-shapes.tsv')),
    # the index at the head of functional-requirements.md named three documents
    # and listed four, while there are seven. The set is the filesystem's.
    'doc_count': lambda: len(claim_domains.documents()),
    # The registry's own size, stated in the README. Removing a row and its
    # derivation together is otherwise the one edit this file cannot see: the
    # totals it prints would change, and nothing would fail. With this rule it
    # takes an edit in two places, which is what every other claim here costs.
    'claims_rows': lambda: len(rows_of(f'{HERE}/claims.tsv')),
}
for _name, _letters in FAMILIES.items():
    DERIVE[f'cases_{_name.replace(" ", "_").replace("-", "_")}'] = (
        lambda ls=_letters: sum(FAMILY_OF[c] for c in ls))


# ------------------------------------------------------------ the registry --
registry = [r.split('\t') for r in rows_of(f'{HERE}/claims.tsv')]
used = set()
checked = 0
# the character span of every number the registry compared, per document. The
# sweep below uses it to tell a number that IS checked from one that is not.
compared = {}
for row in registry:
    if len(row) != 4:
        problems.append(f'claims.tsv: a row has {len(row)} fields, expected 4: {row[0] if row else row}')
        continue
    cid, key, doc, pattern = row
    if key not in DERIVE:
        problems.append(f'{cid}: claims.tsv names the derivation `{key}`, which does not exist')
        continue
    used.add(key)
    want = DERIVE[key]()
    text = flat(os.path.join(DOCS, doc))
    found = list(re.finditer(pattern, text))
    if not found:
        problems.append(f'{cid}: the pattern found nothing in {doc} — the claim has moved or been '
                        f'rewritten, and nothing is comparing it:  {pattern}')
        continue
    for m in found:
        checked += 1
        compared.setdefault(doc, set()).add(m.span(1))
        got = m.group(1)
        if as_number(str(got)) != str(want):
            problems.append(f'{cid}: {doc} says {got}, the tree says {want}')
for key in sorted(set(DERIVE) - used):
    problems.append(f'the derivation `{key}` exists and no row of claims.tsv uses it')

checked += 1
if sum(sum(FAMILY_OF[c] for c in ls) for ls in FAMILIES.values()) != len(CASE_ROWS):
    problems.append(f'the §8 families account for '
                    f'{sum(sum(FAMILY_OF[c] for c in ls) for ls in FAMILIES.values())} case rows, '
                    f'and there are {len(CASE_ROWS)}: a family has no prose term')

# --------------------------------------------------- the two SET claims ----
checked += 1
if len(set(CASE_IDS)) != len(CASE_IDS):
    problems.append(f'test case ids: {len(CASE_IDS) - len(set(CASE_IDS))} are used twice')
listed = re.findall(r'The \d+ guards are ((?:`T-[A-Za-z0-9]+`(?:, | and )?)+)\.', flat(FR))
checked += 1
if len(listed) != 1:
    problems.append(f'the guard list sentence in §8 was found {len(listed)} times, expected once')
else:
    named = re.findall(r'`(T-[A-Za-z0-9]+)`', listed[0])
    # the claim is about the SET; the sentence groups the ids by family rather
    # than by the order the rows appear in, and that is not a defect
    if set(named) != set(GUARDS):
        problems.append('the guard list in §8 is not the set of rows marked **guard**: '
                        f'listed and not marked {sorted(set(named) - set(GUARDS))}, '
                        f'marked and not listed {sorted(set(GUARDS) - set(named))}')
    if len(named) != len(set(named)):
        problems.append('the guard list in §8 names the same case twice')

# every mention of the results directory, classified in a committed table. The
# pattern is the directory NAME, not `results/`: `R=$HERE/results` names it too,
# and the first version of this rule missed both of those (review round 8).
found = Counter()
for rel in repo_files(SCRIPTS):
    for line in read(os.path.join(REPO, rel)).split('\n'):
        if re.search(r'\bresults\b|\$R/|\{R\}/', line):
            found[(rel[len('docs/TraceQL/measure/'):], ' '.join(line.split()))] += 1
table = Counter((r.split('\t')[0], r.split('\t')[3]) for r in rows_of(f'{HERE}/results-inventory.tsv'))
checked += len(table)
for key in sorted(found - table):
    problems.append(f'results-inventory.tsv does not carry this line of {key[0]}, which names the '
                    f'results directory — classify it as a read or a write:  {key[1][:100]}')
for key in sorted(table - found):
    problems.append(f'results-inventory.tsv carries a line of {key[0]} that is no longer there:  {key[1][:100]}')

# ------------------------------------------------- the count-shaped sweep --
# A registry cannot see the claim nobody put in it. Two counts had been wrong
# since this branch began and neither was registered: `the trace test tree (379
# files)` — a tree that has never had 379 tracked files — and `(42 trace
# migrations)`, where the file defines 36 (review round 9).
#
# So the two forms a count over the repository is written in here are swept out
# of every tracked TraceQL document, and every occurrence must be either a
# number the registry above compared, or a row of `counted-shapes.tsv` saying
# what it is instead. An occurrence that is neither stops the run.
#
# What this does NOT reach, stated so a reader does not read it as more: a count
# written in any other form — `the eight files of the parser`, `one hundred and
# forty-one queries` — is outside both patterns and is checked only if it is
# registered. The domain is narrow on purpose: it is the shape a count takes
# when it qualifies a path or a set, and it is small enough that the
# hand-classified part stays readable. The run prints how many it swept, so that
# figure is not kept in this comment where it would go stale.
WORD = '|'.join(w for w in WORDS if w != 'zero')
SHAPES = [re.compile(rf'\((\d[\d,]*|{WORD}) [a-z][a-z -]*[a-z]\)'),
          re.compile(rf'(?<![\w.])(\d[\d,]*|{WORD}) files?\b')]
swept = Counter()
sweep_total = 0
for rel in claim_domains.documents():
    doc = rel[len('docs/TraceQL/'):]
    text = flat(os.path.join(REPO, rel))
    already = compared.get(doc, set())
    # An occurrence is a NUMBER, identified by where it sits, not a match: both
    # forms find `(8 files)` and that is one claim, not two.
    here = {}
    for rx in SHAPES:
        for m in rx.finditer(text):
            here.setdefault(m.span(1), m.group(0))
    sweep_total += len(here)
    for span, got in sorted(here.items()):
        if span in already:
            continue              # the registry compared this very number
        swept[(doc, got)] += 1
shapes = Counter((r.split('\t')[0], r.split('\t')[1]) for r in rows_of(f'{HERE}/counted-shapes.tsv'))
checked += len(shapes)
for key in sorted(swept - shapes):
    problems.append(f'counted-shapes.tsv does not carry `{key[1]}` in {key[0]}, and no claims.tsv row '
                    f'compares that number — register it, or say here what it is instead')
for key in sorted(shapes - swept):
    problems.append(f'counted-shapes.tsv carries `{key[1]}` in {key[0]}, which is no longer there')

# ----------------------------------------- the storage table, cell for cell --
# sql-schema.md section 2 restates `results/storage.tsv` as a Markdown table.
# Restating a result is how it drifts: the byte column of that table disagreed
# with the committed result by 4,139 bytes on `spans`, and no committed result
# carried the number the document printed. The rows are compared here rather
# than registered one claim at a time, because the claim is the TABLE.
STORAGE_ROWS = {r.split('\t')[0]: r.split('\t')[1:] for r in rows_of(f'{HERE}/results/storage.tsv')}
checked += 1
_table = re.findall(r'\| `(spans|traces|tag_values|resources|tag_names)` \| ([\d,]+) \| ([\d,]+) \| ([\d.]+) \|',
                    flat(os.path.join(DOCS, 'sql-schema.md')))
if len(_table) != 5:
    problems.append(f'sql-schema.md section 2: found {len(_table)} of the 5 storage rows — the table has '
                    f'been rewritten and nothing is comparing it with results/storage.tsv')
for _name, _rows, _bytes, _per in _table:
    want = STORAGE_ROWS.get(_name)
    if want is None:
        problems.append(f'results/storage.tsv has no row for `{_name}`')
        continue
    got = [as_number(_rows), as_number(_bytes), as_number(_per)]
    # b_per_span is printed to three places in the document and stored as
    # whatever the database returned; compare at the document's precision
    exp = [want[0], want[1], f'{float(want[2]):.3f}'.rstrip('0').rstrip('.')]
    if got[:2] != exp[:2] or f'{float(got[2]):.3f}' != f'{float(exp[2]):.3f}':
        problems.append(f'sql-schema.md section 2 row `{_name}` says {got}, results/storage.tsv says {exp}')

# The measured populations behind the design decision in measure/README.md are
# a file, not a sentence, because a sentence counting the numbers in the
# document it sits in changes itself when it is written. The file has to be
# refreshed whenever a document changes, and this is what says so.
checked += 1
if read(f'{HERE}/claim-domains.txt') != claim_domains.report():
    problems.append('claim-domains.txt no longer matches the documents: refresh it with '
                    'measure/claim_domains.py --write')

if '--list' in sys.argv:
    for row in registry:
        if len(row) == 4 and row[1] in DERIVE:
            print(f'  {row[0]:34} {row[1]:28} = {DERIVE[row[1]]()}')
for p in problems:
    print('CLAIM', p)
print(f'claims checked {checked} over {len(registry)} registry rows, {len(DERIVE)} derivations, '
      f'{sweep_total} count-shaped occurrences swept, '
      f'failing {len(problems)}')
sys.exit(0 if not problems else 1)
