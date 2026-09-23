#!/usr/bin/env python3
"""The TraceQL-to-SQL rules of `docs/TraceQL/server-implementation.md` §3.2,
written out once each.

It is not a compiler: it is one method per rule, applied to the corpus at
`crates/pulsus-traceql/tests/corpus/`. For each accepted query it produces two
statements — the one the API issues (`api_statement`) and the span-id membership
query the catalogue's answer column shows (`Statement.answer_sql`) — and
`catalogue.py` runs both and checks both against `catalogue_interp.py`.

**This module and `catalogue_interp.py` share `catalogue_parse` and nothing
else.** Every constant below is this side's own: its own status and kind codes,
its own unscoped scope order, its own depth bounds. `measure/perturb_check.py`
changes one of them at a time, on one side at a time, and requires the
comparison to go red each time.
"""
import re


B = 300_000_000_000          # the sort key's bucket width

# The renderer's own semantic tables. Derived here from the sources named, not
# imported from the interpreter and not shared with it.
#
#   status: OTLP `Status.StatusCode` — UNSET 0, OK 1, ERROR 2
#           (opentelemetry-proto trace/v1/trace.proto)
#   kind:   OTLP `Span.SpanKind` — UNSPECIFIED 0, INTERNAL 1, SERVER 2,
#           CLIENT 3, PRODUCER 4, CONSUMER 5 (same file)
#   order:  the scope order an unscoped `.k` tries, docs/api.md §4.2
#   bounds: PULSUS_TRACEQL_MAX_DEPTH (64) and MAX_SPANS_PER_TRACE (10 000)
STATUS_CODE = {'unset': 0, 'ok': 1, 'error': 2}
KIND_CODE = {'unspecified': 0, 'internal': 1, 'server': 2, 'client': 3,
             'producer': 4, 'consumer': 5}
UNSCOPED_ORDER = ('span', 'resource', 'event', 'link', 'instrumentation')
MAX_DEPTH = 64               # PULSUS_TRACEQL_MAX_DEPTH: the structural climb's bound
MAX_SPANS = 10_000           # MAX_SPANS_PER_TRACE: the numbering walk's bound

# What each duration unit is worth in nanoseconds. The parser hands over the
# digits and the unit's SPELLING; this table is this side's own, because a wrong
# multiplier shared with the interpreter moves both answers together and the
# comparison stays green (review round 5).
DURATION_NS = {'ns': 1, 'us': 1000, 'µs': 1000, 'ms': 10 ** 6, 's': 10 ** 9,
               'm': 60 * 10 ** 9, 'h': 3600 * 10 ** 9}

# The value of `kind` and `status` as the API renders them, which is what
# `compare()` counts: the keyword, not the stored code. Read off this side's own
# code tables, so a perturbation of those tables moves this too.
KIND_KEYWORDS = [k for k, _ in sorted(KIND_CODE.items(), key=lambda kv: kv[1])]
STATUS_KEYWORDS = [k for k, _ in sorted(STATUS_CODE.items(), key=lambda kv: kv[1])]

# The intrinsics `compare()` counts, beside every attribute of every scope
# (`server-implementation.md` §3.2, `sql-schema.md` §5.6): eleven, and `span:id`
# is deliberately not among them.
COMPARE_INTRINSICS = ('name', 'kind', 'status', 'statusMessage',
                      'instrumentation:name', 'instrumentation:version',
                      'trace:rootName', 'trace:rootService',
                      'event:name', 'link:traceId', 'link:spanId')


def duration_ns(e):
    """a parsed duration literal in nanoseconds"""
    return int(round(float(e['v']) * DURATION_NS[e['unit']]))

LIMIT = 20                   # the search route's trace limit
SPSS = 3                     # spans per spanset
DEFAULT_EXEMPLARS = 100      # exemplars are on by default, with 100 the ceiling


class TwoStatements(Exception):
    """`server-implementation.md` §3.5's third case: a nested-set comparison
    other than the three shapes §3.2 answers directly needs the search without
    that condition and then the candidate traces hydrated whole, so the reader
    can number them. No query in the corpus asks for one."""



# ------------------------------------------------------------------ render --
def path(key):
    """an OTLP key as the stored JSON path: % first, then ."""
    return '`' + key.replace('%', '%25').replace('.', '%2E') + '`'

class Render:
    """The rules of docs/TraceQL/server-implementation.md 3.2, one method each."""
    def __init__(self, db): self.db = db

    # --- values -------------------------------------------------------------
    def lit_sql(self, e):
        t, v = e['t'], e['v']
        if t == 'string': return "'" + str(v).replace('\\', '\\\\').replace("'", "\\'") + "'"
        if t == 'duration': return str(duration_ns(e))
        if t == 'number': return str(v)
        if t == 'bool': return 'true' if v else 'false'
        if t == 'status': return str(STATUS_CODE[v])
        if t == 'kind': return str(KIND_CODE[v])
        raise ValueError(f'no literal rendering for {e}')

    def attr_typed(self, e, variant):
        s, k = e['scope'], e['key']
        if s == 'span': return f'attrs.{path(k)}.:{variant}'
        if s == 'instrumentation': return f'scope_attrs.{path(k)}.:{variant}'
        raise ValueError('typed read only for span/instrumentation scopes')

    def attr_present(self, e):
        s, k = e['scope'], e['key']
        if s == 'span': return f"dynamicType(attrs.{path(k)}) != 'None'"
        if s == 'instrumentation': return f"dynamicType(scope_attrs.{path(k)}) != 'None'"
        if s == 'resource':
            # the service name is stored once, as the span's own column (R1), so
            # its presence is answered there and not from the resource JSON
            if k == 'service.name': return "service != ''"
            return (f"has((SELECT groupArray(resource_id) FROM {self.db}.resources "
                    f"WHERE dynamicType(attrs.{path(k)}) != 'None'), resource_id)")
        if s == 'event': return f"arrayExists(x -> dynamicType(x) != 'None', events.attrs.{path(k)})"
        if s == 'link': return f"arrayExists(x -> dynamicType(x) != 'None', links.attrs.{path(k)})"
        if s == 'unscoped':
            parts = [self.attr_present({'k': 'attr', 'scope': sc, 'key': k}) for sc in UNSCOPED_ORDER]
            return '(' + ' OR '.join(parts) + ')'
        raise ValueError(s)

    # --- a comparison between an attribute and a literal --------------------
    def attr_cmp(self, a, op, lit):
        s, k = a['scope'], a['key']
        t = lit['t']
        sqlop = {'=': '=', '!=': '!=', '<': '<', '<=': '<=', '>': '>', '>=': '>='}.get(op)
        if s == 'resource':
            if k == 'service.name':
                if op in ('=~', '!~'):
                    m = f"match(service, '^(?:{lit['v']})$')"
                    return f'NOT {m}' if op == '!~' else m
                return f"service {sqlop} {self.lit_sql(lit)}"
            inner = self.attr_cmp({'k': 'attr', 'scope': 'span', 'key': k}, op, lit)
            return (f"has((SELECT groupArray(resource_id) FROM {self.db}.resources WHERE {inner}), resource_id)")
        if s in ('event', 'link'):
            col = 'events' if s == 'event' else 'links'
            if op in ('=~', '!~'):
                inner = f"match(x, '^(?:{lit['v']})$')"
                base = f"arrayExists(x -> {inner}, {col}.attrs.{path(k)}.:String)"
            elif t == 'string':
                base = f"arrayExists(x -> x {('=' if op in ('=','!=') else sqlop)} {self.lit_sql(lit)}, {col}.attrs.{path(k)}.:String)"
            else:
                base = (f"arrayExists(x -> x {('=' if op in ('=','!=') else sqlop)} {self.lit_sql(lit)}, "
                        f"{col}.attrs.{path(k)}.:Int64)")
            return f"NOT ({base})" if op in ('!=', '!~') else base
        if s == 'unscoped':
            # span -> resource -> event -> link -> instrumentation, first present wins
            branches = []
            for sc in UNSCOPED_ORDER:
                sub = {'k': 'attr', 'scope': sc, 'key': k}
                branches.append((self.attr_present(sub), self.attr_cmp(sub, op, lit)))
            # docs/api.md 4.2: != and !~ also match a span that lacks the key
            out = 'true' if op in ('!=', '!~') else 'false'
            for cond, val in reversed(branches):
                out = f"multiIf({cond}, {val}, {out})"
            return out
        # span or instrumentation: the typed reads
        if op in ('=~', '!~'):
            base = f"match({self.attr_typed(a, 'String')}, '^(?:{lit['v']})$')"
            return f"NOT (coalesce({base}, false))" if op == '!~' else f"coalesce({base}, false)"
        if t == 'string':
            base = f"coalesce({self.attr_typed(a, 'String')} {sqlop if op not in ('!=',) else '='} {self.lit_sql(lit)}, false)"
            return f"NOT ({base})" if op == '!=' else base
        if t == 'bool':
            base = f"coalesce({self.attr_typed(a, 'Bool')} = {self.lit_sql(lit)}, false)"
            return f"NOT (coalesce({self.attr_typed(a, 'Bool')} = {self.lit_sql(lit)}, false))" if op == '!=' else base
        # numeric or duration: both numeric variants
        o = '=' if op == '!=' else sqlop
        both = (f"coalesce({self.attr_typed(a, 'Int64')} {o} {self.lit_sql(lit)}, false) "
                f"OR coalesce({self.attr_typed(a, 'Float64')} {o} {self.lit_sql(lit)}, false)")
        return f"NOT ({both})" if op == '!=' else f"({both})"

    # --- intrinsics ---------------------------------------------------------
    TRACE_LEVEL = {'traceDuration': 'duration', 'trace:duration': 'duration',
                   'rootName': 'root_name', 'trace:rootName': 'root_name',
                   'rootServiceName': 'root_service', 'trace:rootService': 'root_service'}
    def intrinsic_cmp(self, i, op, lit):
        n = i['name']
        sqlop = {'=': '=', '!=': '!=', '<': '<', '<=': '<=', '>': '>', '>=': '>='}.get(op, op)
        if n in ('name', 'span:name'):
            if op in ('=~', '!~'):
                base = f"match(name, '^(?:{lit['v']})$')"
                return f'NOT {base}' if op == '!~' else base
            return f"name {sqlop} {self.lit_sql(lit)}"
        if n in ('kind', 'span:kind'): return f"kind {sqlop} {self.lit_sql(lit)}"
        if n in ('status', 'span:status'): return f"status_code {sqlop} {self.lit_sql(lit)}"
        if n in ('statusMessage', 'span:statusMessage'): return f"status_message {sqlop} {self.lit_sql(lit)}"
        if n in ('duration', 'span:duration'): return f"duration_ns {sqlop} {self.lit_sql(lit)}"
        if n == 'span:id': return f"lower(hex(span_id)) {sqlop} {self.lit_sql(lit)}"
        if n == 'span:parentID': return f"lower(hex(parent_span_id)) {sqlop} {self.lit_sql(lit)}"
        if n == 'trace:id': return f"lower(hex(trace_id)) {sqlop} {self.lit_sql(lit)}"
        if n in ('childCount', 'span:childCount'):
            return (f"(trace_id, span_id) IN (SELECT trace_id, parent_span_id FROM {self.db}.spans "
                    f"WHERE parent_span_id != toFixedString('', 8) GROUP BY trace_id, parent_span_id "
                    f"HAVING count() {sqlop} {self.lit_sql(lit)})")
        if n in self.TRACE_LEVEL:
            col = self.TRACE_LEVEL[n]
            if col == 'duration':
                having = f"max(end_ns) - min(start_ns) {sqlop} {self.lit_sql(lit)}"
            else:
                having = f"max({col}) {sqlop} {self.lit_sql(lit)}"
            return (f"trace_id IN (SELECT trace_id FROM {self.db}.traces GROUP BY trace_id HAVING {having})")
        if n == 'event:name':
            return f"arrayExists(x -> x.2 {sqlop} {self.lit_sql(lit)}, events)"
        if n == 'event:timeSinceStart':
            return f"arrayExists(x -> x.1 - start_ns {sqlop} {self.lit_sql(lit)}, events)"
        if n == 'link:spanID':
            return f"arrayExists(x -> lower(hex(x.2)) {sqlop} {self.lit_sql(lit)}, links)"
        if n == 'link:traceID':
            return f"arrayExists(x -> lower(hex(x.1)) {sqlop} {self.lit_sql(lit)}, links)"
        if n == 'instrumentation:name': return f"scope_name {sqlop} {self.lit_sql(lit)}"
        if n == 'instrumentation:version': return f"scope_version {sqlop} {self.lit_sql(lit)}"
        if n in ('nestedSetLeft', 'nestedSetRight', 'nestedSetParent'):
            return self.nested_set_cmp(n, op, sqlop, lit)
        raise ValueError(f'no rendering for intrinsic {n}')

    def roots_pred(self):
        """A root of the hydrated forest: a span whose parent is not stored.
        `server-implementation.md` §3.2 and `sql-schema.md` §5.9 — one anti-join
        over the window, no numbering (statement `c20`). This is the one place
        the window shortcut differs from the Euler numbering: a span inside a
        pure cycle has a stored parent, so it is not a root here, while the
        numbering promotes one member of the cycle. That difference is stated
        in `functional-requirements.md` §9 and carries a ledger row."""
        return (f"(parent_span_id = toFixedString('', 8) OR (trace_id, parent_span_id) NOT IN "
                f"(SELECT trace_id, span_id FROM {self.db}.spans WHERE {self.W}))")

    def nested_set_cmp(self, n, op, sqlop, lit):
        """The three comparisons that need no numbering, and the one that does."""
        v = str(lit['v']) if lit['t'] == 'number' else None
        if n == 'nestedSetParent' and op == '<' and v == '0':
            return self.roots_pred()
        # the numbering starts at 1, so every stored span satisfies these
        if n in ('nestedSetLeft', 'nestedSetRight') and (
                (op == '>' and v == '0') or (op == '>=' and v in ('0', '1'))):
            return 'true'
        col = {'nestedSetLeft': 'nested_set_left', 'nestedSetRight': 'nested_set_right',
               'nestedSetParent': 'nested_set_parent'}[n]
        return f"NESTED::{col} {sqlop} {self.lit_sql(lit)}"

    def field_value(self, e):
        """a field in value position, for by()/select() and field-vs-field."""
        if e['k'] == 'lit': return self.lit_sql(e)
        if e['k'] == 'attr':
            if e['scope'] == 'resource' and e['key'] == 'service.name': return 'service'
            if e['scope'] == 'span': return f'attrs.{path(e["key"])}'
            if e['scope'] == 'instrumentation': return f'scope_attrs.{path(e["key"])}'
            if e['scope'] == 'unscoped':
                # the value of an unscoped read: the first scope present, in the
                # documented order. span and instrumentation values live on the
                # span row; a resource, event or link value in this position
                # needs the resource join or an array read, and the catalogue
                # says so where a query asks for one.
                k = e['key']
                return (f"multiIf(dynamicType(attrs.{path(k)}) != 'None', attrs.{path(k)}, "
                        f"dynamicType(scope_attrs.{path(k)}) != 'None', scope_attrs.{path(k)}, NULL)")
            raise ValueError(f'value position for scope {e["scope"]} needs a join')
        if e['k'] == 'intrinsic':
            n = e['name']
            simple = {'name': 'name', 'span:name': 'name', 'kind': 'kind', 'span:kind': 'kind',
                      'status': 'status_code', 'span:status': 'status_code',
                      'duration': 'duration_ns', 'span:duration': 'duration_ns',
                      'statusMessage': 'status_message', 'span:statusMessage': 'status_message'}
            if n in simple: return simple[n]
        if e['k'] == 'bin':
            return f"({self.field_value(e['l'])} {e['op']} {self.field_value(e['r'])})"
        raise ValueError(f'no value rendering for {e}')

    # --- a whole field expression as a predicate ----------------------------
    def pred(self, e):
        if e is None: return '1'
        k = e['k']
        if k == 'lit' and e['t'] == 'bool': return 'true' if e['v'] else 'false'
        if k == 'unary' and e['op'] == '!': return f"NOT ({self.pred(e['e'])})"
        if k == 'bin' and e['op'] in ('&&', '||'):
            j = 'AND' if e['op'] == '&&' else 'OR'
            return f"({self.pred(e['l'])} {j} {self.pred(e['r'])})"
        if k == 'attr':      # a bare attribute is truthiness, not presence
            return f"coalesce({self.attr_typed(e, 'Bool')} = true, false)" if e['scope'] in ('span', 'instrumentation') \
                   else self.pred({'k': 'bin', 'op': '=', 'l': e, 'r': {'k': 'lit', 't': 'bool', 'v': True}})
        if k == 'bin' and e['op'] in ('=', '!=', '<', '<=', '>', '>=', '=~', '!~'):
            l, r, op = e['l'], e['r'], e['op']
            if r['k'] == 'lit' and r['t'] == 'nil':
                present = self.attr_present(l) if l['k'] == 'attr' else f"{self.field_value(l)} IS NOT NULL"
                return present if op == '!=' else f"NOT ({present})"
            if l['k'] == 'lit' and l['t'] == 'nil':
                return self.pred({'k': 'bin', 'op': op, 'l': r, 'r': l})
            if r['k'] == 'bin' and r['op'] in ('+', '-', '*', '/', '%', '^'):
                r = {'k': 'lit', 't': 'number', 'v': self.fold(r)}
            if l['k'] == 'attr' and r['k'] == 'lit': return self.attr_cmp(l, op, r)
            if l['k'] == 'intrinsic' and r['k'] == 'lit': return self.intrinsic_cmp(l, op, r)
            if l['k'] == 'lit' and r['k'] in ('attr', 'intrinsic'):
                flip = {'<': '>', '>': '<', '<=': '>=', '>=': '<='}.get(op, op)
                return self.pred({'k': 'bin', 'op': flip, 'l': r, 'r': l})
            # field against field
            lv, rv = self.field_value(l), self.field_value(r)
            o = {'=': '=', '!=': '!=', '<': '<', '<=': '<=', '>': '>', '>=': '>='}[op]
            return f"coalesce({lv} {o} {rv}, false)"
        if k == 'unary' and e['op'] == '-':
            raise ValueError('a negated value is not a predicate')
        raise ValueError(f'no predicate rendering for {e}')

    def fold(self, e):
        """constant arithmetic, folded at compile time as the parser's precedence says"""
        if e['k'] == 'lit': return duration_ns(e) if e['t'] == 'duration' else e['v']
        if e['k'] == 'unary' and e['op'] == '-': return -float(self.fold(e['e']))
        l, r = float(self.fold(e['l'])), float(self.fold(e['r']))
        v = {'+': l + r, '-': l - r, '*': l * r, '/': l / r, '%': l % r, '^': l ** r}[e['op']]
        return int(v) if v == int(v) else v

# --------------------------------------------------------------- statements --
class Statement(Render):
    """Builds, for one parsed query, the statement the compiler emits and the
    membership query the catalogue checks the answer with."""
    def __init__(self, db, s_ns, e_ns):
        super().__init__(db)
        self.s, self.e = s_ns, e_ns
        self.W = (f"start_ns >= {s_ns} AND start_ns < {e_ns} "
                  f"AND intDiv(start_ns, {B}) BETWEEN {s_ns // B} AND {(e_ns - 1) // B}")

    # The numbering rule of `docs/TraceQL/sql-schema.md` §5.9, over every trace
    # in the window at once. It is total: a span whose parent is not stored is a
    # root of the hydrated forest, and a span a cycle leaves unvisited is
    # promoted to a root, so `nestedSetParent < 0` - the way a client asks for
    # root spans - answers the same set the retained implementation answers
    # (`crates/pulsus-read/src/traces/search_eval.rs:2085-2139`). The walk
    # carries (start_ns, span_id), not start_ns, so two children that start at
    # the same instant get a defined order and a subtree's rows are exactly the
    # rows whose path begins with its own.
    NESTED_CTE = """WITH RECURSIVE
    sp AS (SELECT trace_id, span_id, parent_span_id, start_ns FROM {db}.spans WHERE {W}),
    roots AS (SELECT trace_id, span_id, parent_span_id, start_ns FROM sp
              WHERE parent_span_id = toFixedString('', 8)
                 OR (trace_id, parent_span_id) NOT IN (SELECT trace_id, span_id FROM sp)),
    walk AS (SELECT trace_id, span_id, parent_span_id, [(start_ns, span_id)] AS path,
                    0 AS depth, 0 AS phase
             FROM roots
             UNION ALL
             SELECT c.trace_id, c.span_id, c.parent_span_id,
                    arrayConcat(p.path, [(c.start_ns, c.span_id)]), p.depth + 1, p.phase
             FROM sp AS c INNER JOIN walk AS p
                 ON p.trace_id = c.trace_id AND p.span_id = c.parent_span_id
             WHERE p.depth < {maxspans} AND NOT has(arrayMap(x -> x.2, p.path), c.span_id)),
    un AS (SELECT trace_id, span_id, parent_span_id, start_ns FROM sp
           WHERE (trace_id, span_id) NOT IN (SELECT trace_id, span_id FROM walk)),
    up AS (SELECT trace_id, span_id AS x, parent_span_id AS cur,
                  [(start_ns, span_id)] AS seen, (start_ns, span_id) AS mk, 0 AS d
           FROM un
           UNION ALL
           SELECT u.trace_id, u.x, n.parent_span_id,
                  arrayConcat(u.seen, [(n.start_ns, n.span_id)]),
                  least(u.mk, (n.start_ns, n.span_id)), u.d + 1
           FROM up AS u INNER JOIN un AS n ON n.trace_id = u.trace_id AND n.span_id = u.cur
           WHERE u.d < {maxspans} AND NOT has(arrayMap(y -> y.2, u.seen), n.span_id)),
    promoted AS (SELECT u.trace_id AS trace_id, u.span_id AS span_id,
                        u.parent_span_id AS parent_span_id, u.start_ns AS start_ns
                 FROM un AS u
                 INNER JOIN (SELECT trace_id, x, min(mk) AS mk FROM up GROUP BY trace_id, x) AS c
                     ON c.trace_id = u.trace_id AND c.x = u.span_id
                 WHERE c.mk = (u.start_ns, u.span_id)),
    walk2 AS (SELECT trace_id, span_id, parent_span_id, [(start_ns, span_id)] AS path,
                     0 AS depth, 1 AS phase
              FROM promoted
              UNION ALL
              SELECT c.trace_id, c.span_id, c.parent_span_id,
                     arrayConcat(p.path, [(c.start_ns, c.span_id)]), p.depth + 1, p.phase
              FROM un AS c INNER JOIN walk2 AS p
                  ON p.trace_id = c.trace_id AND p.span_id = c.parent_span_id
              WHERE p.depth < {maxspans} AND NOT has(arrayMap(x -> x.2, p.path), c.span_id)),
    tour AS (SELECT trace_id, span_id, parent_span_id, depth, path, phase FROM walk
             UNION ALL
             SELECT trace_id, span_id, parent_span_id, depth, path, phase FROM walk2),
    ordered AS (SELECT trace_id, span_id, parent_span_id, depth, path, phase,
                       row_number() OVER (PARTITION BY trace_id ORDER BY phase ASC, path ASC) AS r
                FROM tour),
    sized AS (SELECT o.trace_id AS trace_id, o.span_id AS span_id, o.parent_span_id AS parent_span_id,
                     o.depth AS depth, o.r AS r, count() AS subtree
              FROM ordered AS o INNER JOIN ordered AS d
                  ON d.trace_id = o.trace_id AND d.phase = o.phase
                     AND arraySlice(d.path, 1, length(o.path)) = o.path
              GROUP BY o.trace_id, o.span_id, o.parent_span_id, o.depth, o.r),
    numbered AS (SELECT trace_id, span_id, parent_span_id, 2 * r - 1 - depth AS nested_set_left,
                        nested_set_left + 2 * subtree - 1 AS nested_set_right FROM sized),
    nested AS (SELECT n.trace_id AS trace_id, n.span_id AS span_id, n.nested_set_left AS nested_set_left,
                      n.nested_set_right AS nested_set_right,
                      if(n.parent_span_id = toFixedString('', 8)
                         OR (n.trace_id, n.parent_span_id) NOT IN (SELECT trace_id, span_id FROM ordered)
                         OR (n.trace_id, n.span_id) IN (SELECT trace_id, span_id FROM promoted),
                         -1, p.nested_set_left) AS nested_set_parent
               FROM numbered AS n LEFT JOIN numbered AS p
                   ON p.trace_id = n.trace_id AND p.span_id = n.parent_span_id)
"""

    def needs_nested(self, sql): return 'NESTED::' in sql

    def with_nested(self, body_sql, pred):
        """a statement whose predicate reads the nested-set numbers"""
        cte = self.NESTED_CTE.format(db=self.db, W=self.W, s=self.s, e=self.e, maxspans=MAX_SPANS)
        p = pred.replace('NESTED::', 'n.')
        return cte + body_sql.format(pred=p)

    # --- the membership query, which the interpreter is checked against ------
    def answer_sql(self, q):
        sp, pipe = q['spanset'], q['pipeline']
        cmpst = next((s for s in pipe if s['k'] == 'compare'), None)
        if cmpst: return self.compare_answer(q, cmpst)
        metric = next((s for s in pipe if s['k'] == 'metric'), None)
        if metric: return self.metric_answer(q, metric)
        stages = [st for st in pipe if st['k'] in ('filter_stage', 'aggregate')]
        # With no stage to apply, a structural or spanset-operation query IS its
        # own answer. With one, the stage applies to the spanset the operation
        # produced, so the answer is built from that set as a membership
        # predicate - dropping the stage (what this used to do) answers a
        # different query: `{ .a = 1 } ~ { .b = 2 } | count() > 1` returned the
        # one sibling pair on trace four of the catalogue fixture, where count()
        # is 1 and the query returns nothing.
        if sp['k'] == 'struct' and not stages: return self.struct_answer(sp)
        if sp['k'] in ('sp_and', 'sp_or') and not stages: return self.spanset_op_answer(sp)
        if sp['k'] == 'struct':
            pred = (f"has((SELECT groupArray((trace_id, span_id)) FROM "
                    f"({self.struct_member_set(sp)})), (trace_id, span_id))")
        elif sp['k'] in ('sp_and', 'sp_or'):
            pred = self.sp_member(sp)[0]
        else:
            pred = self.pred(sp['body'])
        extra = []
        for st in pipe:
            if st['k'] == 'filter_stage':
                inner = st['spanset']
                if inner['k'] == 'filter':
                    extra.append(self.pred(inner['body']))
                elif inner['k'] == 'struct':
                    extra.append(f"has((SELECT groupArray((trace_id, span_id)) FROM "
                                 f"({self.struct_member_set(inner)})), (trace_id, span_id))")
                else:
                    extra.append(self.sp_member(inner)[0])
            elif st['k'] == 'aggregate':
                agg = {'count': 'count()', 'avg': f"avg({self.field_value(st['arg'])})" if st['arg'] else 'avg(duration_ns)',
                       'sum': f"sum({self.field_value(st['arg'])})" if st['arg'] else 'sum(duration_ns)',
                       'min': f"min({self.field_value(st['arg'])})" if st['arg'] else 'min(duration_ns)',
                       'max': f"max({self.field_value(st['arg'])})" if st['arg'] else 'max(duration_ns)'}[st['op']]
                if st['op'] != 'count' and st['arg'] is not None and st['arg']['k'] == 'attr':
                    agg = f"{st['op']}(toFloat64OrNull(toString({self.field_value(st['arg'])})))"
                cmpv = self.lit_sql(st['value']) if st['value']['k'] == 'lit' else self.field_value(st['value'])
                extra.append(f"trace_id IN (SELECT trace_id FROM {self.db}.spans WHERE {self.W} AND ({pred}) "
                             f"GROUP BY trace_id HAVING {agg} {st['cmp']} {cmpv})")
        where = ' AND '.join([f'({pred})'] + [f'({x})' for x in extra])
        body = ("SELECT lower(hex(span_id)) AS span FROM {db}.spans AS s WHERE {W} AND ({pred}) ORDER BY span FORMAT TSV")
        if self.needs_nested(where):
            inner = ("SELECT lower(hex(s.span_id)) AS span FROM {db}.spans AS s "
                     "INNER JOIN nested AS n ON n.trace_id = s.trace_id AND n.span_id = s.span_id "
                     "WHERE " + self.W + " AND ({pred}) ORDER BY span FORMAT TSV").replace('{db}', self.db)
            return self.with_nested(inner, where)
        return body.format(db=self.db, W=self.W, pred=where)

    def operand_pred(self, x):
        if x['k'] == 'filter': return self.pred(x['body'])
        if x['k'] == 'struct':
            return (f"has((SELECT groupArray((trace_id, span_id)) FROM ({self.struct_member_set(x)})), "
                    f"(trace_id, span_id))")
        return self.sp_member(x)[0]

    _depth = 0
    def struct_answer(self, sp):
        Statement._depth += 1
        d = Statement._depth
        A = self.operand_pred(sp['l'])
        Bp = self.operand_pred(sp['r'])
        op, mod = sp['op'], sp['mod']
        if op in ('child', 'parent', 'sibling'):
            ids, par, asp, bsp = f'a_ids{d}', f'a_par{d}', f'a_spans{d}', f'b_spans{d}'
            keep = {'child': f"arrayFilter(x -> has({ids}, x.2), {bsp})",
                    'parent': f"arrayFilter(x -> has({par}, x.1), {bsp})",
                    'sibling': f"arrayFilter(x -> has({par}, x.2) AND NOT has({ids}, x.1), {bsp})"}[op]
            if mod == 'neg':
                keep = {'child': f"arrayFilter(x -> NOT has({ids}, x.2), {bsp})",
                        'parent': f"arrayFilter(x -> NOT has({par}, x.1), {bsp})",
                        'sibling': f"arrayFilter(x -> NOT (has({par}, x.2) AND NOT has({ids}, x.1)), {bsp})"}[op]
            partner = {'child':   f"arrayFilter(y -> has(arrayMap(x -> x.2, {keep}), y.1), {asp})",
                       'parent':  f"arrayFilter(y -> has(arrayMap(x -> x.1, {keep}), y.2), {asp})",
                       'sibling': f"arrayFilter(y -> has(arrayMap(x -> x.2, {keep}), y.2) "
                                  f"AND NOT has(arrayMap(x -> x.1, {keep}), y.1), {asp})"}[op]
            hit = keep if mod != 'union' else f"arrayConcat({keep}, {partner})"
            return (f"SELECT lower(hex(arrayJoin(arrayMap(x -> x.1, hit{d})))) AS span FROM ("
                    f"SELECT trace_id, groupArrayIf(span_id, {A}) AS {ids}, "
                    f"groupArrayIf(parent_span_id, {A}) AS {par}, "
                    f"groupArrayIf((span_id, parent_span_id), {A}) AS {asp}, "
                    f"groupArrayIf((span_id, parent_span_id), {Bp}) AS {bsp}, {hit} AS hit{d} "
                    f"FROM {self.db}.spans WHERE {self.W} AND (({A}) OR ({Bp})) GROUP BY trace_id) "
                    f"ORDER BY span FORMAT TSV")
        # descendant / ancestor: the bounded climb
        seed, reach = (Bp, A) if op == 'descendant' else (A, Bp)
        cte = (f"WITH RECURSIVE climb AS ("
               f"SELECT trace_id, span_id AS seed, parent_span_id AS cur, 0 AS depth "
               f"FROM {self.db}.spans WHERE {self.W} AND ({seed}) "
               f"UNION ALL "
               f"SELECT c.trace_id, c.seed, x.parent_span_id, c.depth + 1 FROM climb AS c "
               f"INNER JOIN (SELECT trace_id, span_id, parent_span_id FROM {self.db}.spans WHERE {self.W}) AS x "
               f"ON x.trace_id = c.trace_id AND x.span_id = c.cur WHERE c.depth < {MAX_DEPTH}), "
               f"pairs AS (SELECT DISTINCT c.trace_id AS trace_id, c.seed AS seed, c.cur AS other FROM climb AS c "
               f"INNER JOIN (SELECT trace_id, span_id FROM {self.db}.spans WHERE {self.W} AND ({reach})) AS r "
               f"ON r.trace_id = c.trace_id AND r.span_id = c.cur) ")
        side = 'seed' if op == 'descendant' else 'other'
        other = 'other' if op == 'descendant' else 'seed'
        keepf = seed if op == 'descendant' else reach
        hit = (f"SELECT lower(hex(span_id)) AS span FROM {self.db}.spans WHERE {self.W} AND ({keepf}) "
               f"AND (trace_id, span_id) IN (SELECT trace_id, {side} FROM pairs)")
        if mod == 'plain': body = hit
        elif mod == 'neg':
            body = (f"SELECT lower(hex(span_id)) AS span FROM {self.db}.spans WHERE {self.W} AND ({keepf}) "
                    f"AND (trace_id, span_id) NOT IN (SELECT trace_id, {side} FROM pairs)")
        else:
            partner = reach if op == 'descendant' else seed
            body = (hit + f" UNION ALL SELECT lower(hex(span_id)) AS span FROM {self.db}.spans "
                    f"WHERE {self.W} AND ({partner}) AND (trace_id, span_id) IN "
                    f"(SELECT trace_id, {other} FROM pairs)")
        return cte + f"SELECT span FROM ({body}) ORDER BY span FORMAT TSV"

    # a spanset expression, recursively: the span-level membership predicate and
    # the trace-level qualification each operand imposes
    def sp_member(self, sp):
        if sp['k'] == 'filter':
            p = self.pred(sp['body'])
            return p, [f"trace_id IN (SELECT trace_id FROM {self.db}.spans WHERE {self.W} AND ({p}))"]
        if sp['k'] == 'struct':
            sub = self.struct_member_set(sp)
            m = (f"has((SELECT groupArray((trace_id, span_id)) FROM ({sub})), (trace_id, span_id))")
            return m, [f"has((SELECT groupArray(trace_id) FROM ({sub})), trace_id)"]
        lm, lq = self.sp_member(sp['l'])
        rm, rq = self.sp_member(sp['r'])
        if sp['k'] == 'sp_or':
            return f"(({lm}) OR ({rm}))", []          # either side may carry the trace
        both = ' AND '.join(f'({x})' for x in lq + rq)
        return f"((({lm}) OR ({rm})) AND ({both}))", lq + rq

    def struct_member_set(self, sp):
        """the structural result as a (trace_id, span_id) set, for embedding"""
        sql = self.struct_answer(sp)
        sql = sql.replace('lower(hex(span_id)) AS span', 'trace_id, span_id')
        sql = re.sub(r'lower\(hex\(arrayJoin\(arrayMap\(x -> x\.1, (hit\d+)\)\)\)\) AS span',
                     r'trace_id, arrayJoin(arrayMap(x -> x.1, \1)) AS span_id', sql)
        sql = sql.replace('SELECT span FROM (', 'SELECT trace_id, span_id FROM (')
        return sql.replace(' ORDER BY span FORMAT TSV', '')

    def spanset_op_answer(self, sp):
        m, _ = self.sp_member(sp)
        return (f"SELECT lower(hex(span_id)) AS span FROM {self.db}.spans WHERE {self.W} "
                f"AND ({m}) ORDER BY span FORMAT TSV")

    def metric_post(self, q, metric, base, cols='series, t, v'):
        """the stages that run on the finished series: a threshold, and topk/bottomk"""
        out = f"SELECT {cols} FROM ({base})"
        if 'cmp' in metric:
            op, val = metric['cmp']
            out = f"SELECT {cols} FROM ({base}) WHERE v {op} {self.lit_sql(val)}"
        second = next((x for x in q['pipeline'] if x['k'] == 'second'), None)
        if second:
            order = 'DESC' if second['fn'] == 'topk' else 'ASC'
            out = (f"SELECT {cols} FROM ({out}) WHERE series IN "
                   f"(SELECT series FROM (SELECT series, sum(v) AS total FROM ({base}) GROUP BY series) "
                   f"ORDER BY total {order}, series ASC LIMIT {second['n']})")
        return out + " ORDER BY series, t FORMAT TSV"

    # --- compare() ----------------------------------------------------------
    #
    # One row per (scope, key, value, type, side). The universe is every
    # attribute of all five scopes plus the eleven intrinsics of
    # `server-implementation.md` §3.2 — `span:id` deliberately not among them —
    # each counted PER SPAN, with `topN` applied per key and per side inside the
    # statement. A reduced version of this statement agreed with a reduced
    # interpreter and established nothing (review round 5, finding 3).
    JSON_VALUE = "if(startsWith(kv.2, '\"'), JSONExtractString(kv.2), kv.2)"
    KEY_PATH = "replaceAll(replaceAll(kv.1, '%', '%25'), '.', '%2E')"

    def type_word(self, x):
        """The stored type of one value, in the words the answer carries.

        The writer produces five kinds (`measure/schema.sql`'s `tqd_kv2json`):
        `String`, `Int64`, `Float64`, `Bool` and an array. Anything else falls
        through as ClickHouse's own name for it, so an unexpected type shows up
        as itself rather than as a wrong word."""
        return (f"transform({x}, ['String', 'Int64', 'Float64', 'Bool'], "
                f"['string', 'int', 'double', 'bool'], "
                f"if(startsWith({x}, 'Array'), 'array', {x}))")

    def attr_rows(self, scope, source, col):
        """every key of one JSON column, with its value's text and its STORED
        type — read from `JSONAllPathsWithTypes`, not guessed from the text, so
        an integer `1` and a double `1.0` stay two values"""
        return (f"SELECT sel, '{scope}' AS scope, kv.1 AS key, {self.JSON_VALUE} AS value, "
                f"{self.type_word(f'tmap[{self.KEY_PATH}]')} AS type "
                f"FROM (SELECT sel, {col} AS j, "
                f"CAST(JSONAllPathsWithTypes({col}) AS Map(String, String)) AS tmap FROM {source}) "
                f"ARRAY JOIN JSONExtractKeysAndValuesRaw(toString(j)) AS kv")

    def compare_answer(self, q, stage):
        """| compare({...}): the per-attribute distribution of the selection
        against the baseline"""
        outer = self.pred(q['spanset']['body']) if q['spanset']['k'] == 'filter' else '1'
        inner = self.pred(stage['inner']['body']) if stage['inner']['k'] == 'filter' else '1'
        topn = int(stage['args'][0]) if stage['args'] else 10
        win = self.W
        if len(stage['args']) >= 3:
            s0, e0 = int(stage['args'][1]), int(stage['args'][2])
            sel_win = f"start_ns >= {s0} AND start_ns < {e0}"
        else:
            sel_win = '1'
        kinds = "[" + ", ".join(f"'{k}'" for k in KIND_KEYWORDS) + "]"
        statuses = "[" + ", ".join(f"'{k}'" for k in STATUS_KEYWORDS) + "]"
        parts = [
            self.attr_rows('span', 'base', 'attrs'),
            self.attr_rows('instrumentation', 'base', 'scope_attrs'),
            # a resource is shared by many spans and each span is on its own
            # side, so the join is per span and the count is per span
            self.attr_rows('resource',
                           '(SELECT b.sel AS sel, r.rattrs AS rattrs FROM base AS b '
                           'INNER JOIN res AS r USING (resource_id))', 'rattrs'),
            self.attr_rows('event', '(SELECT sel, ev.3 AS eattrs FROM base ARRAY JOIN events AS ev)',
                           'eattrs'),
            self.attr_rows('link', '(SELECT sel, lk.5 AS lattrs FROM base ARRAY JOIN links AS lk)',
                           'lattrs'),
            # the service name is the span row's own column, not a key inside
            # the resource JSON (R1)
            "SELECT sel, 'resource', 'service.name', service, 'string' FROM base",
            # the per-span intrinsics, `kind` and `status` as the keywords the
            # API returns rather than as the stored codes
            "SELECT sel, 'intrinsic', k, v, 'string' FROM ("
            "SELECT sel, ['name', 'kind', 'status', 'statusMessage', "
            "'instrumentation:name', 'instrumentation:version'] AS ks, "
            f"[toString(name), arrayElement({kinds}, least(toInt32(kind), {len(KIND_KEYWORDS) - 1}) + 1), "
            f"arrayElement({statuses}, least(toInt32(status_code), {len(STATUS_KEYWORDS) - 1}) + 1), "
            "status_message, toString(scope_name), toString(scope_version)] AS vs "
            "FROM base) ARRAY JOIN ks AS k, vs AS v",
            # the two trace-level intrinsics, from the per-trace table
            "SELECT sel, 'intrinsic', k, v, 'string' FROM ("
            "SELECT b.sel AS sel, ['trace:rootService', 'trace:rootName'] AS ks, "
            "[toString(t.root_service), toString(t.root_name)] AS vs "
            "FROM base AS b INNER JOIN tr AS t USING (trace_id)) ARRAY JOIN ks AS k, vs AS v",
            "SELECT sel, 'event', 'name', toString(ev.2), 'string' FROM base ARRAY JOIN events AS ev",
            "SELECT sel, 'link', k, v, 'string' FROM ("
            "SELECT sel, ['traceId', 'spanId'] AS ks, "
            "[lower(hex(lk.1)), lower(hex(lk.2))] AS vs FROM base ARRAY JOIN links AS lk) "
            "ARRAY JOIN ks AS k, vs AS v",
        ]
        return (f"WITH base AS (SELECT ({inner}) AND ({sel_win}) AS sel, name, kind, status_code, "
                f"status_message, scope_name, scope_version, attrs, scope_attrs, events, links, "
                f"resource_id, service, trace_id "
                f"FROM {self.db}.spans WHERE {win} AND ({outer})), "
                f"res AS (SELECT resource_id, any(attrs) AS rattrs FROM {self.db}.resources "
                f"GROUP BY resource_id), "
                f"tr AS (SELECT trace_id, max(root_service) AS root_service, max(root_name) AS root_name "
                f"FROM {self.db}.traces GROUP BY trace_id), "
                f"kv AS ({' UNION ALL '.join(parts)}) "
                f"SELECT scope, key, value, type, side, n FROM ("
                f"SELECT scope, key, value, type, if(sel, 'selection', 'baseline') AS side, "
                f"count() AS n, row_number() OVER (PARTITION BY scope, key, side "
                f"ORDER BY count() DESC, value ASC, type ASC) AS rn "
                f"FROM kv GROUP BY scope, key, value, type, side) "
                f"WHERE rn <= {topn} "
                f"ORDER BY scope, key, side, n DESC, value, type FORMAT TSV")

    def metric_base(self, q, metric, exemplars=False):
        """the bucketed pass: one row per (series, bucket), optionally carrying
        the bucket's exemplar. `argMax` over `(duration_ns, span_id)` picks one
        span per bucket per series and the tie-break makes it deterministic —
        `server-implementation.md` §3.2 says one exemplar per bucket per series,
        carrying `(trace:id, span:id, value)`."""
        sp = q['spanset']
        pred = self.pred(sp['body']) if sp['k'] == 'filter' else '1'
        by = metric.get('by')
        group = self.field_value(by) if by is not None else "''"
        fn = metric['fn']
        step = 60 * 10**9
        qarg = 0.9
        if fn == 'quantile_over_time' and len(metric['args']) > 1:
            qarg = float(metric['args'][1]['v'])
        val = {'rate': 'count()', 'count_over_time': 'count()',
               'sum_over_time': 'sum(duration_ns)', 'min_over_time': 'min(duration_ns)',
               'max_over_time': 'max(duration_ns)', 'avg_over_time': 'avg(duration_ns)',
               'quantile_over_time': f'toFloat64(quantilesTDigest({qarg})(duration_ns)[1])',
               # `quantilesTDigest` returns Float32 (`SELECT toTypeName(...)`), so the
               # answer carries about seven significant digits: at a one-second duration
               # the step between representable values is 64 ns. `toFloat64` widens the
               # already-rounded value so the text is the whole number it stands for
               # rather than Float32's shortest spelling of it; it recovers no precision.
               'histogram_over_time': 'count()'}[fn]
        grp = f"toString({group})"
        guard = ''
        if fn == 'histogram_over_time':
            # the log2 bucket rule the shipped code pushes down
            # (`metrics_sql.rs:1002`, `log2_histogram.rs:62-70`): the smallest
            # power of two at or above the duration, and a span shorter than
            # 2 ns has no bucket. The label here is the bucket in NANOSECONDS;
            # rendering it as float seconds is response shaping
            # (`log2_histogram::bucket_seconds`), not a storage question.
            grp = "toString(toUInt64(roundToExp2(duration_ns - 1)) * 2)"
            guard = ' AND duration_ns >= 2'
        exsel = (", argMax((lower(hex(trace_id)), lower(hex(span_id)), duration_ns), "
                 "(duration_ns, span_id)) AS ex" if exemplars else '')
        return (f"SELECT {grp} AS series, (intDiv(start_ns - 1, {step}) + 1) * {step // 10**6} AS t, "
                f"{val} AS v{exsel} FROM {self.db}.spans WHERE {self.W}{guard} AND ({pred}) "
                f"GROUP BY series, t")

    def metric_answer(self, q, metric):
        """the flat (series, bucket, value) statement the catalogue's answer
        column is read from: the same bucketed pass, without the envelope"""
        if self.needs_nested(self.pred(q['spanset']['body'])
                             if q['spanset']['k'] == 'filter' else '1'):
            raise TwoStatements('a nested-set comparison needs the two-statement path')
        return self.metric_post(q, metric, self.metric_base(q, metric))

# --------------------------------------------- the dispositions that are 400 --
DURATION_INTRINSICS = ('duration', 'span:duration')


def _walk(e):
    """every node of a field expression"""
    if not isinstance(e, dict): return
    yield e
    for k in ('l', 'r', 'e'):
        if k in e: yield from _walk(e[k])


def _spanset_filters(sp):
    if sp is None: return
    if sp['k'] == 'filter':
        yield sp['body']
    elif sp['k'] in ('sp_and', 'sp_or', 'struct'):
        yield from _spanset_filters(sp['l']); yield from _spanset_filters(sp['r'])


def refusal(q):
    """The reason the API answers `400`, or None.

    One rule per row of `docs/TraceQL/server-implementation.md` §3.2 whose
    **today** column reads `400`, plus the two the shipped planner adds. Each
    names the file that refuses it; `measure/planner_dispositions.tsv` is the
    same question asked of the shipped planner itself for all 141 accepted
    corpus queries, and `catalogue.py` fails if this function and that file
    disagree on any of them.
    """
    for st in q['pipeline']:
        # `{A} | {B} && {C}` — search_plan.rs:1950-1956
        if st['k'] == 'filter_stage' and st['spanset']['k'] != 'filter':
            return ('a `|` stage must be a single { ... } filter, not a '
                    'cross-spanset or structural operation')
        # `| by(.b + .c)` — search_plan.rs:2011-2017
        if st['k'] == 'by' and st['key']['k'] not in ('attr', 'intrinsic'):
            return ('a grouping key must resolve to a single per-span value, so it '
                    'must be an attribute or an intrinsic')
    # `{ duration > 100 }` — filter.rs:1710-1715
    bodies = list(_spanset_filters(q['spanset']))
    bodies += [s['spanset']['body'] for s in q['pipeline']
               if s['k'] == 'filter_stage' and s['spanset']['k'] == 'filter']
    for body in bodies:
        for n in _walk(body):
            if n.get('k') != 'bin' or n['op'] not in ('=', '!=', '<', '<=', '>', '>='):
                continue
            for a, b in ((n['l'], n['r']), (n['r'], n['l'])):
                if (a.get('k') == 'intrinsic' and a['name'] in DURATION_INTRINSICS
                        and b.get('k') == 'lit' and b['t'] != 'duration'):
                    return 'duration requires a duration literal'
    metric = next((s for s in q['pipeline'] if s['k'] == 'metric'), None)
    if metric is not None or any(s['k'] == 'compare' for s in q['pipeline']):
        # metrics_plan.rs:417-424
        if q['spanset']['k'] != 'filter':
            return ('cross-spanset and structural expressions are not supported by '
                    'metrics queries')
    if metric is not None:
        by = metric.get('by')
        # metrics_plan.rs:1102-1121
        if by is not None and not (by.get('k') == 'attr' and by.get('scope') == 'resource'
                                   and by.get('key') == 'service.name'):
            return 'a metrics grouping key other than resource.service.name'
        if by is not None and metric['fn'] in ('quantile_over_time', 'histogram_over_time'):
            return 'a grouped quantile or histogram'
        args = metric['args']
        if metric['fn'] not in ('rate', 'count_over_time') and args:
            a = args[0]
            if not (a.get('k') == 'intrinsic' and a['name'] in DURATION_INTRINSICS):
                return 'an aggregation target other than duration'
    return None


# ------------------------------------------------- the statement the API issues --
class ApiStatement(Statement):
    """`Statement`, plus the whole statement each route issues.

    `Statement.answer_sql` answers "which spans match", which is what the
    catalogue's answer column shows. This class answers "what does the route
    return" — the trace envelope of `sql-schema.md` §5.2 with its groups,
    projections and spanset caps, the series envelope of §5.6 with its
    exemplars, or `compare()`'s two distributions — because that is the
    statement whose rows a client sees (review round 4, finding 2).
    """

    # --- a projected or grouped field ---------------------------------------
    def projected(self, f):
        return self.field_value(f)

    def group_value(self, key):
        """The label a `by()` group carries: the value's TEXT.

        A group key is a `GROUP BY` key, and an attribute read is a `Dynamic`
        value — ClickHouse refuses those as grouping keys outright (code 44,
        "Data types Variant/Dynamic are not allowed in GROUP BY keys"), so a
        statement that groups on the bare read does not run at all. The label a
        client receives is text in any case (`sql-schema.md` §5.2), so the key
        is rendered as text here, which is what the corpus-scale statement
        `measure/sql/c05_by_attribute.sql` does too."""
        return f'toString({self.field_value(key)})'

    def group_type(self, key):
        """The stored type a `by()` group carries beside its label, or None
        where the type is a property of the QUERY rather than of the data.

        `docs/api.md` §4.2: an attribute group key renders in the arm the sender
        stored it as — `int` as `intValue`, `float` as `doubleValue`, a string
        that reads as a number still as `stringValue`. An integer `1` and a
        double `1.0` have the SAME label, so a statement that groups on the
        label alone merges two groups the API keeps apart and leaves the
        response layer with no way to choose the arm. An intrinsic's type
        follows from the intrinsic itself, and `resource.service.name` is always
        a string, so neither needs a column."""
        if key['k'] != 'attr': return None
        if key['scope'] == 'resource' and key['key'] == 'service.name': return None
        return self.type_word(f'dynamicType({self.field_value(key)})')

    def group_present(self, key):
        if key['k'] == 'attr':
            return f" AND ({self.attr_present(key)})"
        return ''

    def pipeline_parts(self, q):
        """(member predicate, HAVING, group key or None, select fields)"""
        sp, pipe = q['spanset'], q['pipeline']
        if sp['k'] == 'filter':
            member = self.pred(sp['body'])
        elif sp['k'] == 'struct':
            member = (f"has((SELECT groupArray((trace_id, span_id)) FROM "
                      f"({self.struct_member_set(sp)})), (trace_id, span_id))")
        else:
            member, _ = self.sp_member(sp)
        group, proj, having = None, [], ''
        for s in pipe:
            if s['k'] == 'filter_stage':
                member = f"({member}) AND ({self.pred(s['spanset']['body'])})"
            elif s['k'] == 'aggregate':
                agg = self.agg_sql(s)
                val = (self.lit_sql(s['value']) if s['value']['k'] == 'lit'
                       else self.field_value(s['value']))
                having = f"\n           HAVING {agg} {s['cmp']} {val}"
            elif s['k'] == 'by':
                group = s['key']
            elif s['k'] == 'coalesce':
                # `| by(k) | coalesce()` drops the key k added; with no `by()`
                # before it there is nothing to drop and the statement is the
                # ungrouped one (server-implementation.md §3.2)
                group = None
            elif s['k'] == 'select':
                proj = s['fields']
        return member, having, group, proj

    def agg_sql(self, s):
        if s['op'] == 'count':
            return 'count()'
        if s['arg'] is not None and s['arg']['k'] == 'attr':
            return f"{s['op']}(toFloat64OrNull(toString({self.field_value(s['arg'])})))"
        return f"{s['op']}({self.field_value(s['arg']) if s['arg'] else 'duration_ns'})"

    # --- search --------------------------------------------------------------
    def search_statement(self, q):
        member, having, group, proj = self.pipeline_parts(q)
        if self.needs_nested(member):
            raise TwoStatements(member)
        projsql = ''.join(', ' + self.projected(f) for f in proj)
        tup = f"(lower(hex(span_id)), start_ns, duration_ns{projsql})"
        top = (f"(SELECT (groupArray(trace_id), groupArray(keys))\n"
               f"     FROM (SELECT trace_id, max(start_ns) AS last,\n"
               f"                  groupUniqArray(intDiv(start_ns, {B})) AS keys\n"
               f"           FROM {self.db}.spans\n"
               f"           WHERE {self.W}\n"
               f"             AND ({member})\n"
               f"           GROUP BY trace_id{having}\n"
               f"           ORDER BY last DESC, trace_id ASC\n"
               f"           LIMIT {LIMIT})) AS top")
        keys = (f"(intDiv(start_ns, {B}), trace_id) IN\n"
                f"            (SELECT arrayJoin(arrayFlatten(arrayMap((t, ks) -> "
                f"arrayMap(k -> (k, t), ks), top.1, top.2))))")
        join = (f"LEFT JOIN (SELECT trace_id, min(start_ns) AS start_ns, max(end_ns) AS end_ns,\n"
                f"                  max(root_service) AS root_service, max(root_name) AS root_name\n"
                f"           FROM {self.db}.traces\n"
                f"           WHERE trace_id IN (SELECT arrayJoin(top.1))\n"
                f"           GROUP BY trace_id) AS t USING trace_id")
        if group is None:
            detail = (f"(SELECT trace_id, max(start_ns) AS last, count() AS matched,\n"
                      f"             arraySlice(arraySort(x -> (x.2, x.1), groupArray({tup})), 1, {SPSS}) AS spans\n"
                      f"      FROM {self.db}.spans\n"
                      f"      WHERE {keys}\n"
                      f"        AND {self.W}\n"
                      f"        AND ({member})\n"
                      f"      GROUP BY trace_id) AS m")
            sel = ("SELECT lower(hex(m.trace_id)) AS trace_id, t.root_service, t.root_name, t.start_ns,\n"
                   "       t.end_ns - t.start_ns AS trace_duration_ns, m.last, m.matched, m.spans")
        else:
            gt = self.group_type(group)
            tsel = f", {gt} AS grp_type" if gt else ''
            tkey = ', grp_type' if gt else ''
            gtup = '(first, grp, grp_type, spans)' if gt else '(first, grp, spans)'
            gmap = 'x -> (x.2, x.3, x.4)' if gt else 'x -> (x.2, x.3)'
            inner = (f"SELECT trace_id, {self.group_value(group)} AS grp{tsel},\n"
                     f"                   max(start_ns) AS last, count() AS matched,\n"
                     f"                   min((start_ns, span_id)) AS first,\n"
                     f"                   arraySlice(arraySort(x -> (x.2, x.1), groupArray({tup})), 1, {SPSS}) AS spans\n"
                     f"            FROM {self.db}.spans\n"
                     f"            WHERE {keys}\n"
                     f"              AND {self.W}\n"
                     f"              AND ({member}){self.group_present(group)}\n"
                     f"            GROUP BY trace_id, grp{tkey}")
            detail = (f"(SELECT trace_id, max(last) AS last, sum(matched) AS matched,\n"
                      f"             arrayMap({gmap}, arraySort(x -> x.1,\n"
                      f"                      groupArray({gtup}))) AS groups\n"
                      f"      FROM ({inner})\n"
                      f"      GROUP BY trace_id) AS m")
            sel = ("SELECT lower(hex(m.trace_id)) AS trace_id, t.root_service, t.root_name, t.start_ns,\n"
                   "       t.end_ns - t.start_ns AS trace_duration_ns, m.last, m.matched, m.groups")
        return (f"WITH\n    {self.s} AS s, {self.e} AS e,\n    {top}\n{sel}\n"
                f"FROM {detail}\n{join}\nORDER BY m.last DESC, m.trace_id ASC")

    # --- metrics -------------------------------------------------------------
    def exemplar_budget(self, q):
        """How many exemplars the statement collects.

        Exemplars are **on by default** (`server-implementation.md` §3.2): the
        query's own hint wins, then the HTTP parameter, then 100, and 100 is the
        ceiling (`metrics_plan.rs:1069-1095`). The catalogue has no HTTP
        request, so it renders the hint if there is one and the default
        otherwise — which is what a route issues for a request that says
        nothing. `with(exemplars=false)` is `0` and turns the column off. Every
        other hint is accepted and changes no read, `sample` included
        (`metrics_plan.rs:1093`)."""
        for k, v in q['hints']:
            if k == 'exemplars':
                if v == 'true': return DEFAULT_EXEMPLARS
                if v == 'false': return 0
                try: return min(int(float(v)), DEFAULT_EXEMPLARS)
                except ValueError: return 0
        return DEFAULT_EXEMPLARS

    def metric_statement(self, q, metric):
        ex = self.exemplar_budget(q) > 0
        base = self.metric_base(q, metric, exemplars=ex)
        cols = 'series, t, v, ex' if ex else 'series, t, v'
        flat = self.metric_post(q, metric, base, cols).replace(' ORDER BY series, t FORMAT TSV',
                                                              ' ORDER BY series, t')
        if ex:
            return (f"SELECT series, groupArray((t, v)) AS points,\n"
                    f"       groupArray((t, ex.1, ex.2, ex.3)) AS exemplars\n"
                    f"FROM ({flat})\nGROUP BY series ORDER BY series")
        return (f"SELECT series, groupArray((t, v)) AS points\n"
                f"FROM ({flat})\nGROUP BY series ORDER BY series")

    def compare_statement(self, q, stage):
        return self.compare_answer(q, stage).replace(' FORMAT TSV', '')


def api_statement(st, q):
    """(the statement the API issues, None) or (None, the reason it is a 400)."""
    why = refusal(q)
    if why is not None:
        return None, why
    cmpst = next((s for s in q['pipeline'] if s['k'] == 'compare'), None)
    if cmpst is not None:
        return st.compare_statement(q, cmpst), None
    metric = next((s for s in q['pipeline'] if s['k'] == 'metric'), None)
    if metric is not None:
        return st.metric_statement(q, metric), None
    return st.search_statement(q), None
