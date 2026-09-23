#!/usr/bin/env python3
"""The independent side of the catalogue's check: the fixture rows, and an
interpreter that walks a parsed TraceQL query over them.

It shares `catalogue_parse` with `catalogue_render.py` and nothing else. Every
rule that decides an answer is written here again, from the sources named
beside it, so that a mistake on one side shows up as a disagreement:

- the status and kind codes, from OTLP's own `trace.proto`;
- the order an unscoped `.k` tries the scopes in, from `docs/api.md` §4.2;
- the structural climb's depth bound;
- what the search route returns — the trace envelope of
  `docs/TraceQL/sql-schema.md` §5.2 — and what a metric, a grouping stage and
  `compare()` return.

`measure/perturb_check.py` changes one rule at a time, on one side at a time,
and requires the comparison to go red each time. A change made HERE must go red
just as a change made in the renderer does; that is what says the two sides are
not the same reasoning written twice.
"""
import json, re

# This side's own tables. Same sources as the renderer's, read again rather
# than imported: an import is what made a shared-encoding defect invisible
# (review round 4, finding 4).
STATUS_CODE = {'unset': 0, 'ok': 1, 'error': 2}
KIND_CODE = {'unspecified': 0, 'internal': 1, 'server': 2, 'client': 3,
             'producer': 4, 'consumer': 5}
UNSCOPED_ORDER = ('span', 'resource', 'event', 'link', 'instrumentation')
MAX_DEPTH = 64
LIMIT = 20
SPSS = 3
DEFAULT_EXEMPLARS = 100
# What each duration unit is worth in nanoseconds; the parser hands over the
# digits and the unit's spelling, and this table is this side's own.
DURATION_NS = {'ns': 1, 'us': 1000, 'µs': 1000, 'ms': 10 ** 6, 's': 10 ** 9,
               'm': 60 * 10 ** 9, 'h': 3600 * 10 ** 9}
# `kind` and `status` as the API renders them, from this side's own code tables.
KIND_KEYWORDS = [k for k, _ in sorted(KIND_CODE.items(), key=lambda kv: kv[1])]
STATUS_KEYWORDS = [k for k, _ in sorted(STATUS_CODE.items(), key=lambda kv: kv[1])]
# The stored type each fixture value has, in the words the statement's `type`
# column carries.
VALUE_TYPE = {'s': 'string', 'i': 'int', 'd': 'double', 'b': 'bool', 'as': 'array'}


def duration_ns(e):
    """a parsed duration literal in nanoseconds"""
    return int(round(float(e['v']) * DURATION_NS[e['unit']]))


def value_text(t, v):
    """The text a stored value renders as — the JSON token ClickHouse writes
    back for it, which is what `compare()` counts and what a group label
    carries. A whole double is its digits with no fractional part (`1.0` is
    written back as `1`), a boolean is `true`/`false`, and a string is itself."""
    if t == 'b': return 'true' if v else 'false'
    if t == 'd': return f'{v:g}'
    return str(v)


def label_text(v):
    """The text a group or series label carries, from a value this interpreter
    already holds as a Python object rather than as a stored (type, value)
    pair."""
    if isinstance(v, bool): return 'true' if v else 'false'
    if isinstance(v, float): return f'{v:g}'
    return str(v)

# ------------------------------------------------------------- interpreter --
class Fixture:
    """The fixture rows, read from the generator's own output, with the derived
    maps an evaluation needs. This is the independent side of the check: it
    walks the parsed query directly and never sees the SQL."""
    def __init__(self, path):
        self.rows = [json.loads(l) for l in open(path)]
        for r in self.rows:
            r['attrs_map'] = {k: (t, v) for k, t, v in r['attrs']}
            r['res_map'] = {k: (t, v) for k, t, v in r['resource']}
            r['scope_map'] = {k: (t, v) for k, t, v in r.get('scope_attrs', [])}
            r['dur'] = r['end_ns'] - r['start_ns']
        self.by_id = {(r['trace_id'], r['span_id']): r for r in self.rows}
        self.children = {}
        for r in self.rows:
            self.children.setdefault((r['trace_id'], r['parent_span_id']), []).append(r)
        self.traces = {}
        for r in self.rows:
            t = self.traces.setdefault(r['trace_id'], {'start': r['start_ns'], 'end': r['end_ns'],
                                                       'root_name': '', 'root_service': ''})
            t['start'] = min(t['start'], r['start_ns']); t['end'] = max(t['end'], r['end_ns'])
            if not r['parent_span_id']:       # the view's maxIf over the roots
                t['root_name'] = max(t['root_name'], r['name'])
                t['root_service'] = max(t['root_service'], r['service'])
        self.nested = self._number()
    def _number(self):
        """The nested-set numbering, written as the retained implementation
        describes it (`crates/pulsus-read/src/traces/search_eval.rs:2085-2139`):
        an Euler tour of the hydrated forest in ascending (start_ns, span_id)
        order, a span whose parent is not stored counting as a root, and any
        span a cycle left unvisited promoted to a root in the same order. One
        counter is incremented on entry and on exit, so n spans occupy 1..2n."""
        out = {}
        for tid in {r['trace_id'] for r in self.rows}:
            rows = sorted([r for r in self.rows if r['trace_id'] == tid],
                          key=lambda r: (r['start_ns'], r['span_id']))
            ids = {r['span_id'] for r in rows}
            kids = {}
            for r in rows:
                if r['parent_span_id'] and r['parent_span_id'] in ids:
                    kids.setdefault(r['parent_span_id'], []).append(r)
            for v in kids.values():
                v.sort(key=lambda r: (r['start_ns'], r['span_id']))
            paths, promoted = {}, set()

            def walk(r, p):
                paths[r['span_id']] = p + [(r['start_ns'], r['span_id'])]
                for c in kids.get(r['span_id'], []):
                    if c['span_id'] not in paths:
                        walk(c, paths[r['span_id']])

            for r in rows:                       # the forest's roots, in order
                if ((not r['parent_span_id'] or r['parent_span_id'] not in ids)
                        and r['span_id'] not in paths):
                    walk(r, [])
            for r in rows:                       # then what a cycle left behind
                if r['span_id'] not in paths:
                    promoted.add(r['span_id'])
                    walk(r, [])
            order = list(paths)                  # the tour, in visit order
            for i, sid in enumerate(order, 1):
                depth = len(paths[sid]) - 1
                sub = sum(1 for o in order if paths[o][:len(paths[sid])] == paths[sid])
                left = 2 * i - 1 - depth
                out[(tid, sid)] = {'left': left, 'right': left + 2 * sub - 1}
            for r in rows:
                par = r['parent_span_id']
                out[(tid, r['span_id'])]['parent'] = (
                    -1 if r['span_id'] in promoted or not par or par not in paths
                    else out[(tid, par)]['left'])
        return out
def f32(x):
    """The value as the metrics quantile returns it.

    `quantile_over_time` is computed by t-digest, whose ClickHouse aggregate
    returns **Float32** — about seven significant digits, so at a one-second
    duration the step between representable values is 64 ns. An answer this
    interpreter computes exactly is only observable to that precision, so it is
    stated at that precision here."""
    import struct
    return struct.unpack('f', struct.pack('f', float(x)))[0]


def log2_bucket_ns(dur):
    """The `histogram_over_time` bucket: the smallest power of two nanoseconds
    at or above the duration, and nothing at all below 2 ns.

    The reference's `Log2Bucketize` (`pkg/traceql/engine_metrics.go:2038-2046 @
    v3.0.2`) bit-scans `v - 1`; written here over Python integers, which is the
    same arithmetic without the word width. The label is the bucket in
    nanoseconds; the response layer renders it as float seconds
    (`log2_histogram::bucket_seconds`)."""
    if dur < 2: return None
    return 1 << (dur - 1).bit_length()


def bucket_label_ms(start_ns, step_ns):
    """The instant that CLOSES the bucket a span falls in, in milliseconds:
    the metrics window is left-closed and right-open and a range selector's
    instants are right-closed, so a span at exactly a step edge belongs to the
    bucket that edge ends (`sql-schema.md` §5.6)."""
    return ((start_ns - 1) // step_ns + 1) * step_ns // 10 ** 6


class Interp:
    def __init__(self, fx): self.fx = fx
    def lit(self, e):
        t, v = e['t'], e['v']
        if t == 'number': return float(v) if ('.' in str(v) or 'e' in str(v).lower()) else int(v)
        if t == 'duration': return duration_ns(e)
        if t == 'status': return STATUS_CODE[v]
        if t == 'kind': return KIND_CODE[v]
        return v
    def fold(self, e):
        if e['k'] == 'lit': return self.lit(e)
        if e['k'] == 'unary' and e['op'] == '-': return -self.fold(e['e'])
        l, r = self.fold(e['l']), self.fold(e['r'])
        v = {'+': l + r, '-': l - r, '*': l * r, '/': l / r, '%': l % r, '^': l ** r}[e['op']]
        return int(v) if float(v) == int(v) else v
    def attr(self, row, scope, key):
        """the value a span HAS for this key at this scope, or None"""
        if scope == 'span': return row['attrs_map'].get(key)
        if scope == 'resource': return row['res_map'].get(key)
        if scope == 'instrumentation': return row['scope_map'].get(key)
        if scope == 'event':
            for _, _, at in row['events']:
                for k, t, v in at:
                    if k == key: return (t, v)
            return None
        if scope == 'link':
            for _, _, at in row['links']:
                for k, t, v in at:
                    if k == key: return (t, v)
            return None
        for sc in UNSCOPED_ORDER:                      # unscoped: the first scope present
            got = self.attr(row, sc, key)
            if got is not None: return got
        return None
    def child_count(self, row):
        """the spans whose parent is this span"""
        return len(self.fx.children.get((row['trace_id'], row['span_id']), []))

    def intrinsic(self, row, name):
        t = self.fx.traces[row['trace_id']]
        n = self.fx.nested[(row['trace_id'], row['span_id'])]
        return {
            'name': row['name'], 'span:name': row['name'],
            'kind': row['kind'], 'span:kind': row['kind'],
            'status': row['status_code'], 'span:status': row['status_code'],
            'statusMessage': row['status_message'], 'span:statusMessage': row['status_message'],
            'duration': row['dur'], 'span:duration': row['dur'],
            'span:id': row['span_id'], 'span:parentID': row['parent_span_id'],
            'trace:id': row['trace_id'],
            'childCount': self.child_count(row), 'span:childCount': self.child_count(row),
            'traceDuration': t['end'] - t['start'], 'trace:duration': t['end'] - t['start'],
            'rootName': t['root_name'], 'trace:rootName': t['root_name'],
            'rootServiceName': t['root_service'], 'trace:rootService': t['root_service'],
            'instrumentation:name': row['scope_name'], 'instrumentation:version': row['scope_version'],
            'nestedSetLeft': n['left'], 'nestedSetRight': n['right'],
            # `nestedSetParent < 0` asks whether the span is a root, and the
            # API answers it from the stored parent rather than from a
            # numbering (functional-requirements.md §9, sql-schema.md §5.9):
            # a span inside a pure cycle HAS a stored parent and is therefore
            # not a root, which is the one place this differs from the Euler
            # tour's `promoted` rule.
            'nestedSetParent': (-1 if not row['parent_span_id']
                                or (row['trace_id'], row['parent_span_id']) not in self.fx.by_id
                                else self.fx.nested[(row['trace_id'], row['parent_span_id'])]['left']),
        }.get(name, '__unsupported__')
    def cmp(self, op, lhs, rhs):
        if lhs is None: return op in ('!=', '!~')       # a missing key matches != and !~
        if op == '=~': return bool(re.fullmatch(rhs, str(lhs)))
        if op == '!~': return not re.fullmatch(rhs, str(lhs))
        if isinstance(lhs, bool) != isinstance(rhs, bool): 
            if isinstance(lhs, bool) or isinstance(rhs, bool): return op == '!='
        if isinstance(lhs, str) != isinstance(rhs, str): return op == '!='
        try:
            return {'=': lhs == rhs, '!=': lhs != rhs, '<': lhs < rhs, '<=': lhs <= rhs,
                    '>': lhs > rhs, '>=': lhs >= rhs}[op]
        except TypeError:
            return op == '!='
    def value(self, row, e):
        if e['k'] == 'lit': return self.lit(e)
        if e['k'] == 'attr':
            got = self.attr(row, e['scope'], e['key'])
            return None if got is None else got[1]
        if e['k'] == 'intrinsic': return self.intrinsic(row, e['name'])
        if e['k'] == 'bin' and e['op'] in ('+', '-', '*', '/', '%', '^'): return self.fold(e)
        if e['k'] == 'unary' and e['op'] == '-': return -self.value(row, e['e'])
        raise ValueError(f'no value for {e}')
    def pred(self, row, e):
        if e is None: return True
        k = e['k']
        if k == 'lit' and e['t'] == 'bool': return e['v']
        if k == 'unary' and e['op'] == '!': return not self.pred(row, e['e'])
        if k == 'bin' and e['op'] == '&&': return self.pred(row, e['l']) and self.pred(row, e['r'])
        if k == 'bin' and e['op'] == '||': return self.pred(row, e['l']) or self.pred(row, e['r'])
        if k == 'attr':
            got = self.attr(row, e['scope'], e['key'])
            return got is not None and got[0] == 'b' and got[1] is True
        if k == 'bin':
            l, r, op = e['l'], e['r'], e['op']
            # the per-event and per-link intrinsics are existential: they ask
            # whether the span carries an event or a link that satisfies them
            if l['k'] == 'intrinsic' and l['name'] in ('event:name', 'event:timeSinceStart',
                                                       'link:spanID', 'link:traceID'):
                rv = self.value(row, r)
                n = l['name']
                if n == 'event:name': vals = [ev[1] for ev in row['events']]
                elif n == 'event:timeSinceStart': vals = [ev[0] - row['start_ns'] for ev in row['events']]
                elif n == 'link:spanID': vals = [lk[1] for lk in row['links']]
                else: vals = [lk[0] for lk in row['links']]
                return any(self.cmp(op, v, rv) for v in vals)
            if r['k'] == 'lit' and r['t'] == 'nil':
                present = self.value(row, l) is not None
                return present if op == '!=' else not present
            if l['k'] == 'lit' and l['t'] == 'nil':
                return self.pred(row, {'k': 'bin', 'op': op, 'l': r, 'r': l})
            # field against field: the value-position rule (see `value_field`)
            both_fields = l['k'] in ('attr', 'intrinsic') and r['k'] in ('attr', 'intrinsic')
            read = self.value_field if both_fields else self.value
            lv = read(row, l)
            rv = (self.fold(r) if (r['k'] == 'bin' and r['op'] in '+-*/%^')
                  else read(row, r))
            if lv == '__unsupported__' or rv == '__unsupported__': return False
            return self.cmp(op, lv, rv)
        raise ValueError(f'no predicate for {e}')

    # --- a whole query, the independent side of the check --------------------
    def answer(self, q):
        sp, pipe = q['spanset'], q['pipeline']
        cmpst = next((s for s in pipe if s['k'] == 'compare'), None)
        if cmpst: return self.compare_answer(q, cmpst)
        metric = next((s for s in pipe if s['k'] == 'metric'), None)
        if metric: return self.metric_answer(q, metric)
        return sorted(r['span_id'] for r in self.matched_rows(q))

    def matched_rows(self, q):
        """the span rows the query keeps, after every membership stage"""
        sp, pipe = q['spanset'], q['pipeline']
        hits = self.spanset(sp)
        for st in pipe:
            if st['k'] == 'filter_stage':
                inner = self.spanset(st['spanset'])
                hits = [r for r in hits if r in inner]
            elif st['k'] == 'aggregate':
                by_trace = {}
                for r in hits: by_trace.setdefault(r['trace_id'], []).append(r)
                keep = set()
                for tid, rs in by_trace.items():
                    if st['op'] == 'count': v = len(rs)
                    else:
                        vals = []
                        for r in rs:
                            x = self.value(r, st['arg']) if st['arg'] else r['dur']
                            if isinstance(x, (int, float)) and not isinstance(x, bool): vals.append(x)
                        if not vals: continue
                        v = {'avg': sum(vals) / len(vals), 'sum': sum(vals),
                             'min': min(vals), 'max': max(vals)}[st['op']]
                    if self.cmp(st['cmp'], v, self.value(rs[0], st['value'])): keep.add(tid)
                hits = [r for r in hits if r['trace_id'] in keep]
        return hits

    def spanset(self, sp):
        if sp['k'] == 'filter':
            return [r for r in self.fx.rows if self.pred(r, sp['body'])]
        if sp['k'] == 'sp_or':
            l, r = self.spanset(sp['l']), self.spanset(sp['r'])
            ids = {x['span_id'] for x in l} | {x['span_id'] for x in r}
            return [x for x in self.fx.rows if x['span_id'] in ids]
        if sp['k'] == 'sp_and':
            l, r = self.spanset(sp['l']), self.spanset(sp['r'])
            traces = {x['trace_id'] for x in l} & {x['trace_id'] for x in r}
            ids = {x['span_id'] for x in l} | {x['span_id'] for x in r}
            return [x for x in self.fx.rows if x['span_id'] in ids and x['trace_id'] in traces]
        if sp['k'] == 'struct':
            return self.structural(sp)
        raise ValueError(sp['k'])

    def ancestors(self, row, bound=MAX_DEPTH):
        out, cur, d = [], row['parent_span_id'], 0
        while cur and d < bound:
            p = self.fx.by_id.get((row['trace_id'], cur))
            if p is None: break
            out.append(p); cur = p['parent_span_id']; d += 1
        return out

    def structural(self, sp):
        A = self.spanset(sp['l']); Bs = self.spanset(sp['r'])
        aid = {(x['trace_id'], x['span_id']) for x in A}
        apar = {(x['trace_id'], x['parent_span_id']) for x in A}
        pairs = []
        for b in Bs:
            if sp['op'] == 'child':
                partners = [a for a in A if a['trace_id'] == b['trace_id'] and a['span_id'] == b['parent_span_id']]
            elif sp['op'] == 'parent':
                partners = [a for a in A if a['trace_id'] == b['trace_id'] and a['parent_span_id'] == b['span_id']]
            elif sp['op'] == 'sibling':
                partners = [a for a in A if a['trace_id'] == b['trace_id']
                            and a['parent_span_id'] == b['parent_span_id'] and a['span_id'] != b['span_id']]
            elif sp['op'] == 'descendant':
                anc = {(x['trace_id'], x['span_id']) for x in self.ancestors(b)}
                partners = [a for a in A if (a['trace_id'], a['span_id']) in anc]
            else:  # ancestor: b is an ancestor of an A span
                partners = [a for a in A if (b['trace_id'], b['span_id']) in
                            {(x['trace_id'], x['span_id']) for x in self.ancestors(a)}]
            if partners: pairs.append((b, partners))
        if sp['mod'] == 'plain': hit = [b for b, _ in pairs]
        elif sp['mod'] == 'neg':
            keep = {b['span_id'] for b, _ in pairs}
            hit = [b for b in Bs if b['span_id'] not in keep]
        else:
            ids = {b['span_id'] for b, _ in pairs} | {a['span_id'] for _, ps in pairs for a in ps}
            hit = [r for r in self.fx.rows if r['span_id'] in ids]
        return hit

    # --- compare() ----------------------------------------------------------
    #
    # Written from `server-implementation.md` §3.2 and `sql-schema.md` §5.6, not
    # from the statement: every attribute of all five scopes, plus eleven
    # intrinsics (`span:id` is deliberately not one of them), counted per SPAN,
    # with `topN` applied per key and per side. A shared reduction on both sides
    # is what review round 5 found here.
    def compare_keys(self, r):
        """every (scope, key, value, type) this span contributes"""
        out = []
        for k, t, v in r['attrs']:
            out.append(('span', k, value_text(t, v), VALUE_TYPE[t]))
        for k, t, v in r.get('scope_attrs', []):
            out.append(('instrumentation', k, value_text(t, v), VALUE_TYPE[t]))
        for k, t, v in r['resource']:
            # the service name is the span row's own column and is not repeated
            # inside the stored resource JSON, so it is counted once, below
            if k == 'service.name': continue
            out.append(('resource', k, value_text(t, v), VALUE_TYPE[t]))
        out.append(('resource', 'service.name', r['service'], 'string'))
        for _, _, at in r['events']:
            for k, t, v in at:
                out.append(('event', k, value_text(t, v), VALUE_TYPE[t]))
        for _, _, at in r['links']:
            for k, t, v in at:
                out.append(('link', k, value_text(t, v), VALUE_TYPE[t]))
        tr = self.fx.traces[r['trace_id']]
        for k, v in (('name', r['name']),
                     ('kind', KIND_KEYWORDS[r['kind']]),
                     ('status', STATUS_KEYWORDS[r['status_code']]),
                     ('statusMessage', r['status_message']),
                     ('instrumentation:name', r['scope_name']),
                     ('instrumentation:version', r['scope_version']),
                     ('trace:rootService', tr['root_service']),
                     ('trace:rootName', tr['root_name'])):
            out.append(('intrinsic', k, str(v), 'string'))
        for _, name, _ in r['events']:
            out.append(('event', 'name', str(name), 'string'))
        for tid, sid, _ in r['links']:
            out.append(('link', 'traceId', tid, 'string'))
            out.append(('link', 'spanId', sid, 'string'))
        return out

    def compare_tuples(self, q, stage):
        outer = self.spanset(q['spanset'])
        sel_rows = set(id(r) for r in self.spanset(stage['inner']))
        topn = int(stage['args'][0]) if stage['args'] else 10
        if len(stage['args']) >= 3:
            s0, e0 = int(stage['args'][1]), int(stage['args'][2])
            in_win = lambda r: s0 <= r['start_ns'] < e0
        else:
            in_win = lambda r: True
        counts = {}
        for r in outer:
            side = 'selection' if (id(r) in sel_rows and in_win(r)) else 'baseline'
            for scope, k, text, ty in self.compare_keys(r):
                key = (scope, k, text, ty, side)
                counts[key] = counts.get(key, 0) + 1
        rows = [(sc, k, v, ty, side, n) for (sc, k, v, ty, side), n in counts.items()]
        order = lambda x: (x[0], x[1], x[4], -x[5], x[2], x[3])
        # topN is per key and per side, inside the statement, before any cap
        rank, keep = {}, []
        for row in sorted(rows, key=order):
            sc, k, v, ty, side, n = row
            part = (sc, k, side)          # topN is per key and per side
            i = rank.get(part, 0) + 1
            rank[part] = i
            if i <= topn: keep.append(row)
        return [((sc, k, v, ty), side, n) for sc, k, v, ty, side, n in keep]

    def compare_answer(self, q, stage):
        """the same rows, as the catalogue's answer column shows them"""
        return [(f'{sc}/{k}={v}:{ty}', side, str(n))
                for (sc, k, v, ty), side, n in self.compare_tuples(q, stage)]

    def metric_answer(self, q, metric):
        sp = q['spanset']
        rows = self.spanset(sp)
        step = 60 * 10**9
        out = {}
        for r in rows:
            if metric['fn'] == 'histogram_over_time':
                g = log2_bucket_ns(r['dur'])
                if g is None: continue        # a span shorter than 2 ns has no bucket
                g = str(g)
            elif metric.get('by') is not None:
                v = self.value_field(r, metric['by'])
                g = '' if v is None else label_text(v)
            else:
                g = ''
            t = bucket_label_ms(r['start_ns'], step)
            out.setdefault((g, t), []).append(r)
        fn = metric['fn']
        qarg = float(metric['args'][1]['v']) if fn == 'quantile_over_time' and len(metric['args']) > 1 else 0.9
        res = []
        for (g, t), rs in sorted(out.items()):
            durs = sorted(x['dur'] for x in rs)
            v = {'rate': len(rs), 'count_over_time': len(rs), 'histogram_over_time': len(rs),
                 'sum_over_time': sum(durs), 'min_over_time': min(durs), 'max_over_time': max(durs),
                 'avg_over_time': sum(durs) / len(durs),
                 'quantile_over_time': f32(durs[min(len(durs) - 1, int(qarg * len(durs)))])}[fn]
            res.append((g, t, v))
        if 'cmp' in metric:
            op, val = metric['cmp']
            th = self.lit(val)
            res = [x for x in res if self.cmp(op, x[2], th)]
        second = next((x for x in q['pipeline'] if x['k'] == 'second'), None)
        if second:
            totals = {}
            for g, t, v in res: totals[g] = totals.get(g, 0) + v
            order = sorted(totals, key=lambda g: (-totals[g], g)) if second['fn'] == 'topk' \
                    else sorted(totals, key=lambda g: (totals[g], g))
            keep = set(order[:second['n']])
            res = [x for x in res if x[0] in keep]
        return res

    # ------------------------------------------ what the route itself returns --
    #
    # The independent side of the check review round 4 asked for: not "which
    # spans match" but "what does the request answer". Each shape below is
    # written from the document that defines it — the trace envelope of
    # `sql-schema.md` §5.2, the series envelope of §5.6, `compare()`'s two
    # distributions — and never from the SQL that `catalogue_render.py` emits.

    def value_field(self, row, e):
        """A field in VALUE position: `select(...)`, `by(...)`, and either side
        of a field-against-field comparison.

        The documented rule is narrower here than in predicate position: the
        span and instrumentation scopes live on the span row, while a resource,
        event or link value needs a join or an array read, so an unscoped read
        in value position resolves span, then instrumentation, then nothing
        (`server-implementation.md` §3.2; `query-catalogue.md` states it as a
        limitation and names the corpus queries it touches)."""
        if e['k'] == 'lit': return self.lit(e)
        if e['k'] == 'attr':
            s_, k = e['scope'], e['key']
            if s_ == 'resource' and k == 'service.name': return row['service']
            if s_ == 'span':
                got = row['attrs_map'].get(k); return None if got is None else got[1]
            if s_ == 'instrumentation':
                got = row['scope_map'].get(k); return None if got is None else got[1]
            if s_ == 'unscoped':
                for sc in ('span', 'instrumentation'):
                    got = self.attr(row, sc, k)
                    if got is not None: return got[1]
                return None
            raise ValueError(f'value position for scope {s_} needs a join')
        if e['k'] == 'intrinsic':
            v = self.intrinsic(row, e['name'])
            if v == '__unsupported__': raise ValueError(f'no value for {e}')
            return v
        raise ValueError(f'no value for {e}')

    def group_value_type(self, row, key):
        """The stored type of a `by()` group's value, or None where the type is
        a property of the QUERY rather than of the data.

        `docs/api.md` §4.2 renders an attribute group key in the arm the sender
        stored it as, and an integer `1` and a double `1.0` carry the same
        label — so the label alone is not the group. An intrinsic's type follows
        from the intrinsic, and `resource.service.name` is always a string."""
        if key['k'] != 'attr': return None
        s_, k = key['scope'], key['key']
        if s_ == 'resource' and k == 'service.name': return None
        if s_ == 'unscoped':
            for sc in ('span', 'instrumentation'):
                got = self.attr(row, sc, k)
                if got is not None: return VALUE_TYPE[got[0]]
            return None
        got = {'span': row['attrs_map'], 'instrumentation': row['scope_map']}[s_].get(k)
        return None if got is None else VALUE_TYPE[got[0]]

    def group_and_proj(self, q):
        group, proj = None, []
        for st in q['pipeline']:
            if st['k'] == 'by': group = st['key']
            elif st['k'] == 'coalesce': group = None
            elif st['k'] == 'select': proj = st['fields']
        return group, proj

    def api_answer(self, q):
        """The rows the request's own statement must return."""
        cmpst = next((s for s in q['pipeline'] if s['k'] == 'compare'), None)
        if cmpst: return self.compare_rows(q, cmpst)
        metric = next((s for s in q['pipeline'] if s['k'] == 'metric'), None)
        if metric: return self.metric_rows(q, metric)
        return self.search_rows(q)

    def span_tuple(self, r, proj):
        return ([r['span_id'], r['start_ns'], r['dur']]
                + [self.value_field(r, f) for f in proj])

    def search_rows(self, q):
        """`sql-schema.md` §5.2: the newest LIMIT traces, each with its root,
        its extent from the per-trace table, the matched count and the spanset
        capped at SPSS spans in (start, id) order."""
        group, proj = self.group_and_proj(q)
        rows = self.matched_rows(q)
        by_trace = {}
        for r in rows: by_trace.setdefault(r['trace_id'], []).append(r)
        out = []
        for tid, rs in by_trace.items():
            t = self.fx.traces[tid]
            last = max(r['start_ns'] for r in rs)
            head = [tid, t['root_service'], t['root_name'], t['start'], t['end'] - t['start'], last]
            if group is None:
                spans = sorted(rs, key=lambda r: (r['start_ns'], r['span_id']))[:SPSS]
                out.append(head + [len(rs), [self.span_tuple(r, proj) for r in spans]])
                continue
            groups = {}
            for r in rs:
                g = self.value_field(r, group)
                if g is None: continue                 # no spanset for a span lacking the key
                t = self.group_value_type(r, group)
                key = (label_text(g),) if t is None else (label_text(g), t)
                groups.setdefault(key, []).append(r)
            if not groups: continue
            ordered = sorted(groups.items(),
                             key=lambda kv: min((r['start_ns'], r['span_id']) for r in kv[1]))
            matched = sum(len(v) for v in groups.values())
            out.append(head[:5] + [max(max(r['start_ns'] for r in v) for v in groups.values()),
                                   matched,
                                   [list(g) + [[self.span_tuple(r, proj)
                                                for r in sorted(v, key=lambda r: (r['start_ns'], r['span_id']))[:SPSS]]]
                                    for g, v in ordered]])
        out.sort(key=lambda row: (-row[5], row[0]))
        return out[:LIMIT]

    def exemplar_budget(self, q):
        for k, v in q['hints']:
            if k == 'exemplars':
                if v == 'true': return DEFAULT_EXEMPLARS
                if v == 'false': return 0
                try: return min(int(float(v)), DEFAULT_EXEMPLARS)
                except ValueError: return 0
        return DEFAULT_EXEMPLARS

    def metric_rows(self, q, metric):
        """`sql-schema.md` §5.6: one row per series, its points in bucket order,
        and — when `with(exemplars=…)` asks for them — one exemplar per bucket
        per series, the span with the largest duration in that bucket."""
        flat = self.metric_answer(q, metric)
        want_ex = self.exemplar_budget(q) > 0
        ex_by = {}
        if want_ex:
            step = 60 * 10**9
            for r in self.matched_rows(q):
                t = bucket_label_ms(r['start_ns'], step)
                g = self.metric_series_key(r, metric)
                cur = ex_by.get((g, t))
                if cur is None or (r['dur'], r['span_id']) > (cur['dur'], cur['span_id']):
                    ex_by[(g, t)] = r
        out = {}
        for g, t, v in flat: out.setdefault(g, []).append((t, v))
        rows = []
        for g in sorted(out):
            points = [[t, v] for t, v in out[g]]
            if want_ex:
                ex = [[t, ex_by[(g, t)]['trace_id'], ex_by[(g, t)]['span_id'], ex_by[(g, t)]['dur']]
                      for t, _ in out[g]]
                rows.append([g, points, ex])
            else:
                rows.append([g, points])
        return rows

    def metric_series_key(self, r, metric):
        if metric['fn'] == 'histogram_over_time':
            b = log2_bucket_ns(r['dur'])
            return None if b is None else str(b)
        by = metric.get('by')
        if by is None: return ''
        v = self.value_field(r, by)
        return '' if v is None else label_text(v)

    def compare_rows(self, q, stage):
        """`compare()`'s own statement: one row per (scope, key, value, type,
        side) with that side's count, ordered as §5.6 states."""
        return [[sc, k, v, ty, side, n] for (sc, k, v, ty), side, n in self.compare_tuples(q, stage)]
