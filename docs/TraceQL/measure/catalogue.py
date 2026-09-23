#!/usr/bin/env python3
"""Builds docs/TraceQL/query-catalogue*.md from the TraceQL corpus at
crates/pulsus-traceql/tests/corpus/.

For every query the parser and the validator accept, it asks three questions
and answers each by running something:

1. **Does the API serve it?** `catalogue_render.refusal` gives the disposition
   from the rules of `server-implementation.md` §3.2, and
   `measure/planner_dispositions.tsv` gives the SHIPPED planner's own answer for
   the same 141 queries. The run fails if the two disagree anywhere.
2. **What does the request return?** `<name>.sql` is the statement the route
   issues — the trace envelope, the series envelope or `compare()`'s
   distributions — and its rows are compared with `Interp.api_answer`.
3. **Which spans match?** `<name>.membership.sql` is the narrower question the
   catalogue's answer column shows, compared with `Interp.answer`.

Both statements are run exactly as they are written to disk. The two sides
share `catalogue_parse` and nothing else.

Usage: catalogue.py CH_URL DB CORPUS_DIR FIXTURE_JSONL OUT_DIR START_NS END_NS
"""
import glob, json, os, re, sys, time, urllib.request
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from catalogue_parse import parse
from catalogue_render import ApiStatement, TwoStatements, api_statement, refusal
from catalogue_interp import Fixture, Interp

ch, DB, CORPUS, FIXTURE, OUT, S, E = (sys.argv[1].rstrip('/'), sys.argv[2], sys.argv[3].rstrip('/'),
                                      sys.argv[4], sys.argv[5].rstrip('/'), int(sys.argv[6]), int(sys.argv[7]))

# Every file this run writes, recorded as it is written and listed at the end in
# `results/catalogue-outputs.txt`. `measure/reproduce_check.py` compares exactly
# that list, so the set of files it checks comes from the run rather than from a
# list somebody kept: a file this script starts writing is checked the day it
# appears, and a check that looked at nothing cannot report success.
REPO = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', '..'))
WROTE = []


def wrote(path):
    WROTE.append(os.path.relpath(os.path.abspath(path), REPO))
    return path
st = ApiStatement(DB, S, E)
fx = Fixture(FIXTURE); interp = Interp(fx)
os.makedirs(f'{OUT}/catalogue-sql', exist_ok=True)

# The numbering walk is bounded by MAX_SPANS_PER_TRACE, which is deeper than
# ClickHouse's own default for a recursive CTE, so every statement carries it.
SETTINGS = ('SETTINGS final = 1, json_type_escape_dots_in_keys = 1, '
            'max_recursive_cte_evaluation_depth = 10001')
# `<name>.sql` returns a whole API answer, so it is read as JSON: a spanset is
# an array of tuples and a 64-bit integer must stay a number rather than
# becoming a quoted string.
API_SETTINGS = SETTINGS + ', output_format_json_quote_64bit_integers = 0'

# A query whose right answer on this fixture is empty. Every one is listed with
# the reason it cannot be made non-empty; the run reports any OTHER empty answer
# as unexplained, because an empty-against-empty comparison establishes nothing
# (review round 4, finding 5).
EMPTY_REASONS = {
    'intrinsic_span_id': 'a stored span id is eight bytes, so `lower(hex(span_id))` is sixteen '
                         'characters and never equals the two-byte literal `"0a1b"`',
    'intrinsic_colon_space_after': 'the same query written `span: id`; the literal is two bytes',
    'intrinsic_span_parent_id': 'a stored parent span id is eight bytes; the literal is two',
    'intrinsic_trace_id': 'a stored trace id is sixteen bytes, so its hex is thirty-two '
                          'characters and never equals `"0a1b"`',
}

# A rule `measure/perturbations.tsv` marks `not_discriminated`: no corpus query
# can tell the right rule from a wrong one, on any fixture. Each is named here
# with the reason, and the run reports any that is not.
UNDISCRIMINATED_REASONS = {}
# A rule `measure/perturbations.tsv` marks `not_discriminated`: no query the
# catalogue runs can tell the right rule from a wrong one. Each would be named
# here with the reason, and the run reports any that is not.
#
# There are none. Review round 5 found eight, all of the same shape — the rule
# was real and no corpus query reached it — and one of the eight was hiding a
# defect: `by(<an attribute>)` put a `Dynamic` value straight into `GROUP BY`,
# which ClickHouse refuses, so a search this design documents as served did not
# run at all. `measure/catalogue-extra.tsv` now carries one query per rule the
# corpus does not reach, so every row of `perturbations.tsv` is expected to go
# red and an exemption has to be argued for rather than inherited.


def esc(t): return t.replace('|', '\\|').replace('\n', ' ')
def code(t):
    """a markdown code span that survives a backtick inside it"""
    t = esc(t)
    if not t.strip(): return '*(the empty query)*'
    return f'``{t}``' if '`' in t else f'`{t}`'

def run(sql):
    body = sql if 'FORMAT' in sql else sql + ' FORMAT TSV'
    req = urllib.request.Request(ch + '/?final=1&json_type_escape_dots_in_keys=1', data=body.encode())
    t0 = time.perf_counter()
    try:
        out = urllib.request.urlopen(req, timeout=300).read().decode()
        return out, (time.perf_counter() - t0) * 1000, None
    except urllib.error.HTTPError as e:
        return '', (time.perf_counter() - t0) * 1000, e.read().decode()[:200].strip().replace('\n', ' ')

TSV_ESC = {'n': '\n', 't': '\t', 'r': '\r', '0': '\0', 'b': '\b', 'f': '\f',
           "'": "'", '\\': '\\'}

def untsv(x):
    """one TSV cell as the bytes it stands for.

    A value can hold a tab or a newline — the fixture has a span name that holds
    both — and ClickHouse writes those back escaped. Comparing the escaped text
    with the interpreter's own characters is a difference in the transport, not
    in the answer, so it is undone here rather than being written into either
    side's rules."""
    if '\\' not in x: return x
    out, i = [], 0
    while i < len(x):
        if x[i] == '\\' and i + 1 < len(x):
            out.append(TSV_ESC.get(x[i + 1], x[i + 1])); i += 2
        else:
            out.append(x[i]); i += 1
    return ''.join(out)


NUMRE = re.compile(r'-?\d+(\.\d+)?([eE][-+]?\d+)?$')

def canon_text(x):
    """a TSV cell, put into the same alphabet"""
    return canon(float(x)) if NUMRE.match(x) else x

def canon(x):
    """One alphabet for both sides. A whole number is its digits whatever type
    carried it, and any other double is its shortest round-trip spelling, so no
    two distinct doubles collapse."""
    if isinstance(x, bool) or x is None: return x
    if isinstance(x, int): return str(x)
    if isinstance(x, float):
        return str(int(x)) if x.is_integer() and abs(x) < 2 ** 53 else repr(x)
    if isinstance(x, (list, tuple)): return [canon(v) for v in x]
    return x

def group_of(p): return os.path.basename(os.path.dirname(p))
corpus_files = sorted(glob.glob(f'{CORPUS}/accept/*.traceql')) + sorted(glob.glob(f'{CORPUS}/grafana/*.traceql'))
accepted = [(os.path.basename(f)[:-len('.traceql')], group_of(f), open(f).read().strip())
            for f in corpus_files]
refused = (sorted(glob.glob(f'{CORPUS}/reject/*.traceql')) + sorted(glob.glob(f'{CORPUS}/unsupported/*.traceql'))
           + sorted(glob.glob(f'{CORPUS}/validate_reject/*.traceql')))

# The queries the corpus does not contain. The corpus belongs to the parser and
# was written to cover the grammar, so some rules of `server-implementation.md`
# §3.2 have no query that reaches them and the comparison could not tell a right
# rule from a wrong one for any of them (review round 5). `catalogue-extra.tsv`
# names each with the rule it decides — the count is that file's row count and is
# not repeated here; they are counted separately from the corpus's queries and
# answered exactly the same way.
DESIGN, design_why = [], {}
for _l in open(f'{OUT}/catalogue-extra.tsv'):
    if _l.startswith('#') or not _l.strip(): continue
    _n, _q, _why = _l.rstrip('\n').split('\t')
    DESIGN.append((_n, 'design', _q.strip()))
    design_why[_n] = _why

# The parser is the only module the two sides share, so a wrong rule there moves
# both answers together (review round 5). `parse_vectors.tsv` is the second
# check: one line per rule, with the tree it must produce, written out by hand.
vector_bad = []
for _l in open(f'{OUT}/parse_vectors.tsv'):
    if _l.startswith('#') or not _l.strip(): continue
    _n, _q, _want, _why = _l.rstrip('\n').split('\t')
    try:
        _got = json.loads(json.dumps(parse(_q)))
    except Exception as _e:
        vector_bad.append((_n, _want, f'{type(_e).__name__}: {_e}')); continue
    if _got != json.loads(_want):
        vector_bad.append((_n, _want, json.dumps(_got, ensure_ascii=False, separators=(',', ':'))))

# The shipped planner's own disposition for each of them, captured by the probe
# `measure/README.md` prints.
SHIPPED = {}
for _l in open(f'{OUT}/planner_dispositions.tsv'):
    if _l.strip():
        _p = _l.rstrip('\n').split('\t')
        SHIPPED[_p[0]] = (_p[1], _p[2], _p[3] if len(_p) > 3 else '')

rows, served, plan_refused, mismatched, unexplained = [], [], [], [], []
for name, grp, src in accepted + DESIGN:
    rec = {'name': name, 'group': grp, 'traceql': src}
    if grp == 'design': rec['why'] = design_why[name]
    q = parse(src)
    why = refusal(q)
    route, shipped_kind, shipped_msg = SHIPPED.get(name, ('?', '?', ''))
    rec['route'] = route
    # the disposition rule of §3.2 against the shipped planner's own answer
    if (why is None) != (shipped_kind == 'served'):
        mismatched.append((name, why, shipped_kind, shipped_msg))
    if why is not None:
        rec['refused_by_planner'] = why
        rec['planner_message'] = shipped_msg
        plan_refused.append(rec); rows.append(rec); continue

    stmt, _ = api_statement(st, q)
    ans_sql = st.answer_sql(q)
    stmt_f = stmt + '\n' + API_SETTINGS + '\nFORMAT JSONCompact\n'
    ans_f = ans_sql + '\n' + SETTINGS + '\n'
    open(wrote(f'{OUT}/catalogue-sql/{name}.sql'), 'w').write(stmt_f)
    open(wrote(f'{OUT}/catalogue-sql/{name}.membership.sql'), 'w').write(ans_f)

    # --- 2. the statement the route issues, against the independent answer ----
    out, ms, err = run(stmt_f)
    rec['statement_ms'] = round(ms)
    rec['statement_error'] = err
    got_api = json.loads(out)['data'] if not err else None
    want_api = interp.api_answer(q)
    rec['api_rows'] = len(got_api) if got_api is not None else None
    rec['api_agree'] = (got_api is not None and canon(got_api) == canon(want_api))
    if not rec['api_agree']:
        rec['api_got'] = json.dumps(got_api)[:300]
        rec['api_want'] = json.dumps(want_api)[:300]

    # --- 3. which spans match -------------------------------------------------
    aout, ams, aerr = run(ans_f)
    rec['answer_sql'] = ans_sql
    rec['answer_error'] = aerr
    got = [l for l in aout.split('\n') if l.strip()]
    want = interp.answer(q)
    is_compare = any(x['k'] == 'compare' for x in q['pipeline'])
    if is_compare:
        gs = []
        for l in got:
            c = [untsv(x) for x in l.split('\t')]
            if len(c) == 6: gs.append((f'{c[0]}/{c[1]}={c[2]}:{c[3]}', c[4], c[5]))
        rec['answer'] = '; '.join(f'{k} {side}={n}' for k, side, n in gs[:6]) + (' …' if len(gs) > 6 else '')
        rec['expected'] = '; '.join(f'{k} {side}={n}' for k, side, n in want[:6]) + (' …' if len(want) > 6 else '')
        rec['agree'] = gs == [tuple(x) for x in want]
    elif isinstance(want, list) and want and isinstance(want[0], tuple):      # a metric
        gs = [tuple(untsv(x) for x in l.split('\t')) for l in got]
        wn = [(canon(g), str(t), canon(v)) for g, t, v in want]
        gn = [(canon_text(g), t, canon_text(v)) for g, t, v in gs] if gs else []
        rec['answer'] = '; '.join(f'{g}@{t}={v}' for g, t, v in gn) or '(no series)'
        rec['expected'] = '; '.join(f'{g}@{t}={v}' for g, t, v in wn) or '(no series)'
        rec['agree'] = gn == wn
    else:
        gs = sorted(untsv(x.split('\t')[0]) for x in got)
        wn = sorted(want)
        rec['answer'] = ', '.join('…' + x[-4:] for x in gs) or '(no spans)'
        rec['expected'] = ', '.join('…' + x[-4:] for x in wn) or '(no spans)'
        rec['agree'] = gs == wn
    if aerr: rec['agree'] = False
    if rec['answer'] in ('(no spans)', '(no series)') and name not in EMPTY_REASONS:
        unexplained.append(name)
    served.append(rec); rows.append(rec)

VALIDATE_MSG = {}
for _l in open(f'{OUT}/validate_messages.tsv'):
    if _l.strip():
        _q, _m = _l.rstrip('\n').split('\t', 1)
        VALIDATE_MSG[_q] = _m

PLAIN = {'RecursionLimitExceeded': 'nested past the parser\'s depth limit',
         'TrailingInput': 'input left over after the end of the query',
         'UnterminatedString': 'a string literal with no closing quote'}

ref_rows = []
for f in refused:
    name = os.path.basename(f)[:-len('.traceql')]
    golden = open(f[:-len('.traceql')] + '.golden').read().strip()
    kind = golden.split('{')[0].strip() or golden.split('\n')[0].strip()
    found = re.search(r'found:\s*"(.*)"', golden)
    expected = re.search(r'expected:\s*"(.*)"', golden)
    span = re.search(r'start:\s*(\d+)', golden)
    construct = re.search(r'construct:\s*"(.*)"', golden)
    raw = re.search(r'raw:\s*"(.*)"', golden)
    why = re.search(r'reason:\s*"(.*)"', golden)
    ref_rows.append({'name': name, 'group': group_of(f), 'traceql': open(f).read().strip(),
                     'kind': kind, 'found': found.group(1) if found else '',
                     'expected': expected.group(1) if expected else '',
                     'construct': construct.group(1) if construct else '',
                     'raw': raw.group(1) if raw else '',
                     'why': why.group(1).replace('\\"', '"') if why else '',
                     'offset': span.group(1) if span else ''})

# the rules the corpus cannot tell apart, read back from the perturbation run
undisc, undisc_missing = [], []
try:
    for _l in open(f'{OUT}/results/perturbations.tsv'):
        _p = _l.rstrip('\n').split('\t')
        if len(_p) < 11 or _p[2] != 'not_discriminated': continue
        if _p[0] in UNDISCRIMINATED_REASONS:
            undisc.append((_p[0], _p[10], UNDISCRIMINATED_REASONS[_p[0]]))
        else:
            undisc_missing.append(_p[0])
except FileNotFoundError:
    pass
for n in undisc_missing: print('  a rule marked not_discriminated with no reason recorded:', n)
undisc_rows = ('\n'.join(f'| `{i}` | {esc(r)} | {esc(w)} |' for i, r, w in undisc)
               or '| — | — | *(none: every perturbation is noticed by some query here)* |')

def tally(rs):
    return {'served': len(rs), 'ran': sum(1 for r in rs if r.get('statement_error') is None),
            'api_agree': sum(1 for r in rs if r.get('api_agree')),
            'membership_agree': sum(1 for r in rs if r.get('agree'))}

corpus_rows = [r for r in rows if r['group'] != 'design']
c_srv, d_srv = [r for r in served if r['group'] != 'design'], [r for r in served if r['group'] == 'design']
c, d, t = tally(c_srv), tally(d_srv), tally(served)
counts = {'parsed': len(corpus_rows), 'refused_by_planner': len(plan_refused),
          'served': c['served'], 'ran': c['ran'], 'api_agree': c['api_agree'],
          'membership_agree': c['membership_agree'],
          'design': len(DESIGN), 'design_served': d['served'], 'design_ran': d['ran'],
          'design_api_agree': d['api_agree'], 'design_membership_agree': d['membership_agree'],
          'served_total': t['served'], 'ran_total': t['ran'],
          'api_agree_total': t['api_agree'], 'membership_agree_total': t['membership_agree'],
          'refused_before_planning': len(ref_rows),
          'disposition_mismatches': len(mismatched), 'unexplained_empty': len(unexplained),
          'undiscriminated_without_reason': len(undisc_missing),
          'parse_vector_mismatches': len(vector_bad)}
api_agree, mem_agree, ran = c['api_agree'], c['membership_agree'], c['ran']
json.dump({'counts': counts, 'accepted': rows, 'refused': ref_rows,
           'planner_refused': [r['name'] for r in plan_refused],
           'empty_with_reason': sorted(EMPTY_REASONS),
           'undiscriminated_without_reason': undisc_missing,
           'unexplained_empty': unexplained,
           'parse_vector_mismatches': [n for n, _, _ in vector_bad]},
          open(wrote(f'{OUT}/results/catalogue.json'), 'w'), indent=1)
print(f'queries the parser and validator accept   {counts["parsed"]}')
print(f'of those, the API refuses at plan time    {len(plan_refused)}')
print(f'queries the API serves                    {c["served"]}')
print(f'their statements that ran                 {c["ran"]}')
print(f'API answers that agree                    {c["api_agree"]}')
print(f'membership answers that agree             {c["membership_agree"]}')
print(f'design queries the corpus does not have   {counts["design"]}')
print(f'  of those served, and their statements   {d["served"]}, {d["ran"]} ran')
print(f'  their API and membership answers agree  {d["api_agree"]}, {d["membership_agree"]}')
print(f'parser trees against the written vectors  {len(vector_bad)} mismatches')
print(f'disposition rule vs shipped planner       {len(mismatched)} mismatches')
print(f'empty on both sides without a reason      {len(unexplained)}')
print(f'rules not discriminated, with no reason   {len(undisc_missing)}')
print(f'refused before planning                   {len(ref_rows)}')
for n, want, got in vector_bad:
    print('  parse vector mismatch:', n, '| written:', want[:160], '| parser:', got[:160])
for n, why, kind, msg in mismatched:
    print('  disposition mismatch:', n, '| rule:', why, '| shipped:', kind, msg[:80])
for r in served:
    if r.get('statement_error'): print('  statement error:', r['name'], r['statement_error'][:110])
    elif not r.get('api_agree'):
        print('  API disagreement:', r['name'], '| sql:', r.get('api_got'), '| interpreter:', r.get('api_want'))
    if not r.get('agree') and not r.get('statement_error'):
        print('  membership disagreement:', r['name'], '| sql:', r.get('answer'), '| interpreter:', r.get('expected'),
              ('| ' + r['answer_error'][:80]) if r.get('answer_error') else '')
for n in unexplained: print('  empty on both sides, no reason recorded:', n)

# ------------------------------------------------------------- the documents --
def abbreviate(sql):
    """the unscoped chain is the same five branches every time: name it once"""
    m = re.match(r"\(?multiIf\(dynamicType\(attrs\.`([^`]+)`\) != 'None', (.*)", sql, re.S)
    if not m: return sql
    key = m.group(1)
    first = m.group(2)
    depth, i = 0, 0
    for i, ch_ in enumerate(first):
        if ch_ == '(': depth += 1
        elif ch_ == ')':
            if depth == 0: break
            depth -= 1
        elif ch_ == ',' and depth == 0: break
    span_branch = first[:i].strip()
    return f'unscoped({key}) -> span: {span_branch}; then resource, event, link, instrumentation (server-implementation.md 3.2)'
def shape_key(sql):
    """the statement with its literals blanked, so queries that differ only in a
    literal can be counted as one shape"""
    s_ = re.sub(r"'(?:[^']|'')*'", "'?'", sql)
    return re.sub(r'\b\d+\b', 'N', s_)

DOCDIR = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
shapes = {}
for r in served:
    if 'answer_sql' in r: shapes.setdefault(shape_key(r['answer_sql']), []).append(r['name'])
dups = {k: v for k, v in shapes.items() if len(v) > 1}

nspans = len(fx.rows)
ntraces = len({r['trace_id'] for r in fx.rows})
fam = {}
for r in served:
    n = r['name']
    key = ('the rules the corpus does not reach' if r['group'] == 'design' else
           'arithmetic' if n.startswith('arith') else
           'attribute comparison' if n.startswith(('attr_', 'scope_', 'bracketed', 'bare_')) else
           'duration literals' if n.startswith('duration') else
           'existence and truthiness' if n.startswith('existence') else
           'intrinsics' if n.startswith('intrinsic') else
           'static keywords' if n.startswith('static') else
           'string escapes' if n.startswith('string_') else
           'structural operators' if n.startswith('structural') else
           'spanset operations' if n.startswith('spanset') else
           'pipeline stages' if n.startswith(('pipeline', 'select', 'by_')) else
           'metrics' if n.startswith('metrics') else
           'hints' if n.startswith('hints') else
           'field expressions' if n.startswith(('field_', 'rhs_', 'negation', 'match_all')) else
           'client-generated' if r['group'] == 'grafana' else 'other')
    fam[key] = fam.get(key, 0) + 1
body = '\n'.join(f'| {k} | {v} |' for k, v in sorted(fam.items(), key=lambda kv: -kv[1]))
design_rows = ('\n'.join(f'| `{r["name"]}` | {code(r["traceql"])} | {esc(r["why"])} |'
                         for r in rows if r['group'] == 'design')
               or '| — | — | — |')
empty_rows = '\n'.join(f'| `{n}` | {code(next(r["traceql"] for r in rows if r["name"] == n))} | {esc(EMPTY_REASONS[n])} |'
                       for n in sorted(EMPTY_REASONS))
plan_rows = '\n'.join(
    f'| `{r["name"]}` | {code(r["traceql"])} | `400` | {esc(r["refused_by_planner"])} |'
    for r in sorted(plan_refused, key=lambda x: x['name']))

with open(wrote(f'{DOCDIR}/query-catalogue.md'), 'w') as f:
    f.write(f'''# The TraceQL query catalogue

Every query in the repository's TraceQL corpus, with the SQL it compiles to
against the schema of `docs/TraceQL/sql-schema.md` and the literal answer it
gives, or — where the query is refused — the status and the reason.

The corpus is `crates/pulsus-traceql/tests/corpus/`: {len(accepted)} queries the
parser and the validator accept ({len(glob.glob(f'{CORPUS}/accept/*.traceql'))} under `accept/`,
{len(glob.glob(f'{CORPUS}/grafana/*.traceql'))} under `grafana/`, which are what
the dashboard client actually sends) and {len(ref_rows)} they refuse
({len(glob.glob(f'{CORPUS}/reject/*.traceql'))} `reject/`,
{len(glob.glob(f'{CORPUS}/unsupported/*.traceql'))} `unsupported/`,
{len(glob.glob(f'{CORPUS}/validate_reject/*.traceql')) } `validate_reject/`). It
came from the reference's own suite and from captured client traffic, so it
covers shapes nobody writes by hand.

| | |
|---|---:|
| queries the parser and the validator accept | **{counts['parsed']}** |
| of those, the API refuses at plan time, `400` | **{counts['refused_by_planner']}** |
| queries the API serves | **{counts['served']}** |
| of those, with a statement that runs | **{counts['ran']}** |
| whose **answer** equals the independent check | **{counts['api_agree']}** |
| whose **membership** equals the independent check | **{counts['membership_agree']}** |
| queries the parser or the validator refuses, `400` | **{counts['refused_before_planning']}** |
| queries added here, which the corpus does not contain | **{counts['design']}** |
| of those, served, with a statement that runs | **{counts['design_served']}**, **{counts['design_ran']}** |
| whose **answer** and **membership** equal the check | **{counts['design_api_agree']}**, **{counts['design_membership_agree']}** |

- `docs/TraceQL/query-catalogue-accepted.md` — the {counts['served_total']}
  served queries, each with its membership SQL and its answer: the
  {counts['served']} of the corpus and the {counts['design']} added here.
- `docs/TraceQL/query-catalogue-refused.md` — the
  {counts['refused_before_planning'] + counts['refused_by_planner']} refusals:
  {counts['refused_before_planning']} the parser or the validator turns away and
  {counts['refused_by_planner']} the planner does, each with the status and the
  reason.
- `docs/TraceQL/measure/catalogue-sql/` — two statements per served query, both
  run exactly as the file stands: `<name>.sql` is **the statement the route
  issues**, returning the answer a client receives; `<name>.membership.sql`
  returns the ids of the matching spans, which is this catalogue's answer
  column. Each file ends with the settings it needs, so it can be pasted into a
  client as it is.

## How this was produced, and what the check proves

`docs/TraceQL/measure/catalogue.py` reads the corpus, renders each query with
the rules of `docs/TraceQL/server-implementation.md` §3.2 written out once each
(`catalogue_render.py`), **runs both statements** against ClickHouse, and
compares each with an interpreter that walks the same parsed query over the
fixture rows (`catalogue_interp.py`).

**The two sides share the parser (`catalogue_parse.py`) and nothing else.** Not
the status or kind codes, not the order an unscoped read tries the scopes in,
not the climb's depth bound, not the shape of an answer: each is written once on
each side, from the document or the standard that defines it.
`measure/perturb_check.py` is the check on that claim — it changes one rule at a
time, on one side at a time, and requires the comparison to go red for each;
`results/perturbations.tsv` is its output, and `docs/TraceQL/measure/README.md`
says how to run it.

**A wrong rule in the shared parser would move both answers together**, so the
comparison cannot see one, and the parser is checked a second way instead:
`measure/parse_vectors.tsv` holds one line per rule it decides — an escape, the
precedence, a keyword spelling, `minInt`, the unit a duration is written with —
with the tree that rule must produce, written out by hand from the grammar. This
run found **{counts['parse_vector_mismatches']}** trees that differ. What a unit
is worth in nanoseconds is no longer in that module at all: the parser hands over
the digits and the spelling, and each side multiplies with its own table, because
changing the shared `ms` multiplier from 10⁶ to 10³ left every answer agreeing
(review round 5).

**The disposition is checked against the shipped planner, not asserted.**
`catalogue_render.refusal` implements §3.2's `400` rows;
`measure/planner_dispositions.tsv` holds the shipped parser, validator and
planner's own answer for all {counts['parsed']} of these queries, captured by the
probe `measure/README.md` prints. This run found
**{counts['disposition_mismatches']} disagreements** between the two.

**The reference is not consulted here, and that is deliberate.** The corpus's
keys are synthetic — `.a`, `.b`, `span.retried` — so a second store would answer
every one of them with an empty set, which proves nothing. The authority for an
answer is the fixture data itself, read by the interpreter. Where the two stores
*can* be compared on the same spans, they are, in
`docs/TraceQL/functional-requirements.md` §6.1 and §6.2.

The fixture is `docs/TraceQL/measure/fixture/make_catalogue_fixture.py`:
{nspans} spans in {ntraces} traces carrying every key, scope, intrinsic and value
type the corpus mentions, so an answer is a list of spans rather than a uniform
empty set. Answers name spans by the last four hex digits of their id.

Three of its six traces exist so that a wrong rule and a right one differ: a
chain that alternates the two sides of an operator (`P -> A2 -> B1 -> A1`)
beside a sibling pair under a parent of neither side; a descendant three links
below its nearest matching ancestor, so a direct-child rule and a bounded climb
answer `>>` differently; a span whose parent is not stored; a two-span cycle;
and one resource carrying a key its own span also carries with a different
value, so the unscoped lookup **order** decides the answer.

Reproduce with:

```
fixture/make_catalogue_fixture.py $WORK/cat 1790000000
load_staging.sh   $CH tqd_cat cat
apply_schema.py   $CH tqd_cat
catalogue.py $CH tqd_cat crates/pulsus-traceql/tests/corpus $WORK/cat/spans.jsonl \\
             docs/TraceQL/measure 1790000000000000000 1790000060000000000
```

## What the design cannot serve

Of the {counts['parsed']} queries the parser and the validator accept, the API
refuses **{counts['refused_by_planner']}** at plan time and this design keeps
every one of those refusals. They are not defects in the schema — the storage
answers all three — they are behaviour the shipped planner declines, each with
its own refusal site:

| query | TraceQL | status | the reason |
|---|---|---|---|
{plan_rows}

Of the {counts['served_total']} queries it serves, {counts['served_total'] - counts['ran_total']}
failed to run and {counts['served_total'] - counts['api_agree_total']} returned an
answer other than the independent check's. Two limits of the design are stated
rather than counted, because they are properties of the rules rather than of any
query:

1. **A nested-set comparison other than the three shapes §3.2 answers directly**
   needs two statements (`server-implementation.md` §3.5). No corpus query asks
   for one: the corpus's three nested-set queries are `nestedSetParent < 0`,
   `nestedSetLeft > 0` and `nestedSetRight >= 1`, which compile to the root
   anti-join and to `true`.
2. **An unscoped read in VALUE position** — inside `select()`, `by()` or a
   field-against-field comparison — resolves the span scope and then the
   instrumentation scope, and stops. In predicate position it resolves all five
   scopes in the documented order. A resource, event or link value in value
   position needs a join or an array read; no corpus query asks for one.

### The {counts['design']} queries the corpus does not contain

The corpus belongs to the parser and was written to cover the grammar, so some
rules of `server-implementation.md` §3.2 have no query that reaches them — and a
rule no query reaches is a rule this catalogue does not check, whatever its
agreement count says. `measure/perturb_check.py` names them: it changes one rule
at a time and reports the ones the comparison cannot tell apart. Round 5 of the
review found eight, and one of them was hiding a query the route could not serve
at all: `by(<an attribute>)` put the attribute's `Dynamic` value straight into
`GROUP BY`, which ClickHouse refuses (code 44), so an accepted, documented search
failed against the database. These queries are the answer to that: one per rule,
run and compared exactly as the corpus's are.

| query | TraceQL | the rule it decides |
|---|---|---|
{design_rows}

### What this check cannot detect

Five things, stated because the counts above do not say them. **This list is a
judgement, and it is the one claim on this page nothing checks.** The numbers
and sets `measure/claims.tsv` registers are derived from the thing they count
and compared with the document by `measure/claims_check.py`, which fails the run
when they differ; a number nobody registered is compared only if it is written
in one of the two shapes that file sweeps for, and otherwise it is not compared
at all. No program can tell you that a limit is missing from a list of limits.
Read the list below as what somebody thought of, not as a closed set.

1. **A rule both sides get wrong the same way.** The renderer is this design's
   statement and the interpreter is this design's meaning, so the comparison
   asks whether the statement says what the design means — not whether the
   design is right. Two other checks cover part of that: the disposition rule is
   checked against the shipped planner for every query
   (`measure/planner_dispositions.tsv`), and `functional-requirements.md` §6.1
   and §6.2 compare answers with the reference on the same spans.
2. **A rule nothing here names.** `measure/perturb_check.py` changes the rules
   `measure/perturbations.tsv` lists, one at a time, and nothing else: it does
   not enumerate the renderer's branches or the interpreter's, so the count of
   rules is a count of what somebody wrote down. A branch that no query here
   reaches **and** that no row names is invisible, and its absence looks exactly
   like a rule that was checked. The shared parser has the same shape:
   `measure/parse_vectors.tsv` is a list, not a proof of completeness, and a
   token shape none of its queries uses moves both sides together unseen.
3. **A value shape the fixture has no instance of.** The fixture carries string,
   integer, double and boolean attributes; it carries **no array-valued
   attribute**, so the array branch of every typed rule is unexercised. The
   worked fixture of `functional-requirements.md` §6.1 has one
   (`span.app.tags`), and F7 is its answer.
4. **Everything above the database.** Both sides are compared by sending SQL
   straight to the server and reading the rows back; **no HTTP request is made
   anywhere in this check**. The route, the response serialisation and the
   envelopes of `docs/api.md` §4.2 and §4.4 are therefore outside it — including
   the two parts of `compare()` that are deliberately the response layer's: the
   fixed 25-key well-known set, which emits `key=nil` for a key **absent** from
   the data, and the `*_total` denominators (`server-implementation.md` §3.2).
   What is checked is that neither can lose a stored count, because the
   statement's own `topN` is per key and per side.
5. **Anything at corpus scale, and anything timed.** The statements in
   `measure/sql/` are a different producer (`measure/make_sql.py`) and are not
   compared with the interpreter at all; they are checked against the corpus and
   the reference in §6.2. The `ms` column here exists to show the statement ran.

### The rules no query can tell apart

These are the rules that are left: a perturbation of one changes no answer on any
query here, and each says why.

| perturbation | the rule | why no query can tell it apart |
|---|---|---|
{undisc_rows}

### The queries whose right answer is empty

An empty answer on both sides establishes nothing, so each one is named here
with the reason it cannot be made non-empty on any fixture. This run found
**{counts['unexplained_empty']}** empty answers that are not on this list.

| query | TraceQL | why no fixture can answer it |
|---|---|---|
{empty_rows}

Four behaviours are worth stating plainly, because they are properties of the
design rather than of a query:

1. **`resource.service.name` is answered from the span's `service` column**, for
   comparison, regex and presence alike. The resource row does not repeat the
   service name (R1), so a presence test against the resource JSON would answer
   `false` for every span. The catalogue found this: `explore_root_rate_by_service`
   returned no series until the rule was written down.
2. **`compare()` groups by value *and stored type*.** An integer `1` and a
   double `1.0` render as the same text in ClickHouse, so grouping on text alone
   merges them; the API's own rule keeps a value distinct per type. The
   statement therefore carries the type from `JSONAllPathsWithTypes`.
3. **`| select()`, `| coalesce()` and `with(...)` do not change which spans
   match**; `| by()` does not either, but it changes the shape of the answer,
   and `<name>.sql` shows that shape. The catalogue's answer column is
   membership, so those queries share their answer with the filter they wrap.
   `with(sample=true)` changes no read at all — the shipped planner accepts it
   and returns the exact superset (`metrics_plan.rs:1093`).
4. **The transitive structural operators run bounded.** Every `>>`/`<<` statement
   carries `depth < PULSUS_TRACEQL_MAX_DEPTH` and reports what it could not
   resolve, as `sql-schema.md` §5.8 describes.

## What the corpus covers

| family | queries |
|---|---:|
{body}

## Shapes that differ only in a literal

{len(dups)} groups of queries compile to the same membership statement with a
different literal; the largest are listed here, and every member still has its
own SQL file and its own answer.

| statement shape | queries |
|---|---|
''')
    for k, v in sorted(dups.items(), key=lambda kv: -len(kv[1]))[:12]:
        f.write(f'| {len(v)} queries | {", ".join("`" + x + "`" for x in sorted(v))} |\n')

with open(wrote(f'{DOCDIR}/query-catalogue-accepted.md'), 'w') as f:
    f.write(f'''# The TraceQL query catalogue: the {counts['served_total']} queries the API serves

One row per query the API serves: the {counts['served']} of
`crates/pulsus-traceql/tests/corpus/accept/` and `grafana/`, and the
{counts['design']} of `measure/catalogue-extra.tsv`, which are the queries no
corpus query's rule reaches (`query-catalogue.md` lists them with the rule each
decides). The {counts['refused_by_planner']} the planner refuses are in
`query-catalogue-refused.md`. **SQL** is the part of the
membership statement this query contributes — the predicate, or the aggregate
for a metric — and the whole statement is
`docs/TraceQL/measure/catalogue-sql/<name>.membership.sql`, while
`<name>.sql` is the statement the route issues for the same query; both were run
as they stand, and both answers were compared with the independent interpreter
(`docs/TraceQL/query-catalogue.md`). **Answer** is the membership answer on the
catalogue fixture. **API rows** is how many rows the route's own statement
returned. Spans are named by the last four digits of their id. **ms** is one run
of the route's statement against the {nspans}-span fixture and moves by a few
milliseconds between runs; it is there to show the statement ran, not as a
performance figure. The timings that carry a requirement are in
`docs/TraceQL/functional-requirements.md` §6.3. The answer column does not move.

| # | name | TraceQL | SQL | answer | API rows | ms |
|---:|---|---|---|---|---:|---:|
''')
    for i, r in enumerate(sorted(served, key=lambda x: x['name']), 1):
        sql = r.get('answer_sql', '')
        m = re.search(r'WHERE .*?AND \((.*)\) ORDER BY span', sql, re.S)
        short = m.group(1) if m else sql
        short = abbreviate(short)
        if len(short) > 260: short = short[:260] + ' …'
        f.write(f"| {i} | `{r['name']}` | {code(r['traceql'])} | {code(short)} | "
                f"{esc(r.get('answer','-'))} | {r.get('api_rows','-')} | {r.get('statement_ms','-')} |\n")

with open(wrote(f'{DOCDIR}/query-catalogue-refused.md'), 'w') as f:
    f.write(f'''# The TraceQL query catalogue: the {len(ref_rows) + len(plan_refused)} refused queries

The response is `docs/api.md` §4's envelope in every case — **`400`**,
`text/plain`, the message and nothing else — and this design changes neither the
parser, the validator nor the planner, so it changes no refusal.

## {len(ref_rows)} the parser or the validator refuses

One row per query in `crates/pulsus-traceql/tests/corpus/reject/`,
`unsupported/` and `validate_reject/`. These never reach the storage. The byte
offset is inside the message.

The reason column comes from three places, one per group. For `reject/` and
`unsupported/` it is the failure the corpus's own `.golden` file pins, quoted
from it. For `validate_reject/` the golden holds the parsed query — those four
parse and the semantic pass refuses them — so the reason is the message
`pulsus_traceql::validate` returns, captured into
`docs/TraceQL/measure/validate_messages.tsv` by running the validator over the
four queries; `docs/TraceQL/measure/README.md` gives the test that regenerates
that file. A validator rejection carries no byte offset, which is why the last
column is `-` for those four. One query in the corpus spans two lines
(`reject/string_raw_newline`); its line break is written `\\n` in the table so
the row stays one row.

| # | group | name | TraceQL | status | the reason the golden pins | at byte |
|---:|---|---|---|---|---|---:|
''')
    for i, r in enumerate(ref_rows, 1):
        if r['group'] == 'validate_reject':
            reason = VALIDATE_MSG.get(r['traceql'], r['kind'])
        elif r['construct']:
            reason = f"not yet supported: {r['construct']}"
        elif r['raw']:
            reason = (f"the duration {r['raw']} is not valid: {r['why']}" if r['why']
                      else f"the duration {r['raw']} is not a whole number of nanoseconds")
        elif r['kind'] in PLAIN:
            reason = PLAIN[r['kind']]
        else:
            reason = r['expected'] and f"expected {r['expected']}" or r['kind']
            if r['found']: reason += f", found {r['found']}"
        q_ = r['traceql'].replace('\n', '\\n')
        if len(q_) > 90:
            q_ = q_[:90] + ' … (' + str(len(q_)) + ' bytes, in full in the corpus file)'
        f.write(f"| {i} | `{r['group']}` | `{r['name']}` | {code(q_)} | `400` | {esc(reason)} | {r['offset'] or '-'} |\n")
    f.write(f'''
## {len(plan_refused)} the planner refuses

These parse and validate, so they sit under `accept/`, and the route answers
`400` all the same. The reason column is the rule
`docs/TraceQL/server-implementation.md` §3.2 states; the message column is what
the shipped planner itself returned when the probe of `measure/README.md` ran it
(`measure/planner_dispositions.tsv`).

| # | name | TraceQL | status | the rule | the shipped planner's message |
|---:|---|---|---|---|---|
''')
    for i, r in enumerate(sorted(plan_refused, key=lambda x: x['name']), 1):
        f.write(f"| {i} | `{r['name']}` | {code(r['traceql'])} | `400` | {esc(r['refused_by_planner'])} | "
                f"{esc(r.get('planner_message', ''))} |\n")
MANIFEST = f'{OUT}/results/catalogue-outputs.txt'
wrote(MANIFEST)
with open(MANIFEST, 'w') as f:
    f.write('\n'.join(sorted(WROTE)) + '\n')
print(f'documents written, {len(WROTE)} files listed in results/catalogue-outputs.txt')

ok = (counts['ran_total'] == counts['served_total']
      and counts['api_agree_total'] == counts['served_total']
      and counts['membership_agree_total'] == counts['served_total']
      and counts['design_served'] == counts['design']
      and counts['parse_vector_mismatches'] == 0
      and counts['disposition_mismatches'] == 0
      and counts['unexplained_empty'] == 0
      and counts['undiscriminated_without_reason'] == 0)
sys.exit(0 if ok else 1)
