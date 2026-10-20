#!/usr/bin/env python3
"""The independent side of the catalogue's check: the fixture rows, and an
interpreter that walks a parsed TraceQL query over them.

The other side is the shipped route, whose answers `catalogue.sh` records.
Every rule that decides an answer is written here from the sources named
beside it, so that a mistake on either side shows up as a disagreement:

- the status and kind codes, from OTLP's own `trace.proto`;
- the order an unscoped `.k` tries the scopes in, from `docs/api.md` §4.2;
- the structural climb's depth bound;
- what the search route returns — the trace envelope of
  `docs/TraceQL/sql-schema.md` §5.2 — and what a metric, a grouping stage and
  `compare()` return.

`measure/perturb_check.py` changes one rule here at a time and requires the
comparison to go red each time.
"""
import decimal, json, math, re

# This side's own tables, read from their sources rather than imported from
# the product: an import is what made a shared-encoding defect invisible
# (review round 4, finding 4).
STATUS_CODE = {'unset': 0, 'ok': 1, 'error': 2}
KIND_CODE = {'unspecified': 0, 'internal': 1, 'server': 2, 'client': 3,
             'producer': 4, 'consumer': 5}
UNSCOPED_ORDER = ('span', 'resource', 'event', 'link', 'instrumentation')
# a comparison read the other way round, the field moved to the left
FLIP = {'<': '>', '<=': '>=', '>': '<', '>=': '<='}
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
VALUE_TYPE = {'s': 'string', 'i': 'int', 'd': 'double', 'b': 'bool', 'a': 'array'}


def duration_ns(e):
    """a parsed duration literal in nanoseconds"""
    return int(round(float(e['v']) * DURATION_NS[e['unit']]))


# ------------------------------------------------------------- interpreter --
class Fixture:
    """The fixture rows, read from the generator's own output, with the derived
    maps an evaluation needs. This is the independent side of the check: it
    walks the parsed query directly and never sees the SQL."""
    def __init__(self, path):
        self.rows = [json.loads(l) for l in open(path)]
        for r in self.rows:
            # `docs/api.md` §4.2: an array whose elements are of two types has
            # no TraceQL type and is missing (#600 part 1b-3)
            r['attrs'] = [a for a in r['attrs'] if a[1] != 'a' or len({e[0] for e in a[2]}) <= 1]
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
                                                       'root_name': '', 'root_service': '',
                                                       'root_key': None})
            t['start'] = min(t['start'], r['start_ns']); t['end'] = max(t['end'], r['end_ns'])
            # The root is ONE span, the earliest root by (start_ns, span_id),
            # and both values come from it (issue #591 part 1).
            if not r['parent_span_id']:
                key = (r['start_ns'], r['span_id'])
                if t['root_key'] is None or key < t['root_key']:
                    t['root_key'] = key
                    t['root_name'] = r['name']
                    t['root_service'] = r['service']
        self.nested = self._number()
    def _number(self):
        """`docs/api.md` §4.2, "Nested-set intrinsics": the Euler-tour
        numbering from each span with no parent id, siblings by start time
        then span id, one counter per trace incremented on entry and on exit;
        an orphan, a cycle member and every span below them number 0. A root's
        `nestedSetParent` is -1, any other numbered span's is its parent's
        `nestedSetLeft`."""
        out = {}
        for tid in {r['trace_id'] for r in self.rows}:
            rows = sorted([r for r in self.rows if r['trace_id'] == tid],
                          key=lambda r: (r['start_ns'], r['span_id']))
            kids = {}
            for r in rows:
                if r['parent_span_id']:
                    kids.setdefault(r['parent_span_id'], []).append(r)
            n = [0]

            def walk(r, parent_left):
                n[0] += 1
                left = n[0]
                for c in kids.get(r['span_id'], []):
                    walk(c, left)
                n[0] += 1
                out[(tid, r['span_id'])] = {'left': left, 'right': n[0], 'parent': parent_left}

            for r in rows:
                if not r['parent_span_id']:
                    walk(r, -1)
            for r in rows:
                out.setdefault((tid, r['span_id']), {'left': 0, 'right': 0, 'parent': 0})
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


def engine_double_text(x):
    """A double as the engine's `toString` writes it inside an array: the
    shortest digits that read back as the same double, positional while the
    decimal point falls within 21 digits of them and 6 zeros after it, else
    `<digits>e<exponent>` with no `+`; `-0`, `inf`, `-inf`, `nan` as such
    (measured on the server, #600 part 1b-3)"""
    x = float(x)
    if x != x: return 'nan'
    if x in (float('inf'), float('-inf')): return 'inf' if x > 0 else '-inf'
    if x == 0: return '-0' if math.copysign(1, x) < 0 else '0'
    sign = '-' if x < 0 else ''
    # repr is the shortest round-trip form; the point falls `n` digits after
    # the first of its significant digits
    d = decimal.Decimal(repr(abs(x))).as_tuple()
    n = len(d.digits) + d.exponent
    digits = ''.join(map(str, d.digits)).rstrip('0')
    k = len(digits)
    if k <= n <= 21: return sign + digits + '0' * (n - k)
    if 0 < n <= 21: return sign + digits[:n] + '.' + digits[n:]
    if -6 < n <= 0: return sign + '0.' + '0' * -n + digits
    return sign + digits[0] + ('.' + digits[1:] if k > 1 else '') + 'e' + str(n - 1)


def kind(v):
    """a value's type class for the field-vs-field type gate"""
    return 'bool' if isinstance(v, bool) else 'number' if isinstance(v, (int, float)) else 'string'


def arm(t, v):
    """`docs/api.md` §4.2: a typed value as the response renders it, its
    protojson arm and text — a string `stringValue`, an integer `intValue`
    in decimal, a boolean `boolValue`, a double `doubleValue` in full
    decimal notation with no exponent and no trailing `.0`"""
    if t == 'string': return ['stringValue', v]
    # an array as the JSON text of its elements (#600 part 1b-3)
    if t == 'array': return ['stringValue', json.dumps([e for _, e in v], separators=(',', ':'))]
    if t == 'int': return ['intValue', str(v)]
    if t == 'bool': return ['boolValue', 'true' if v else 'false']
    v = float(v)
    if v != v: return ['doubleValue', 'NaN']
    if v in (float('inf'), float('-inf')): return ['doubleValue', 'Infinity' if v > 0 else '-Infinity']
    if v == int(v): return ['doubleValue', ('-' if str(v).startswith('-') else '') + str(abs(int(v)))]
    return ['doubleValue', format(decimal.Decimal(repr(v)), 'f')]


def go_duration(ns):
    """Go's `time.Duration.String()`: under a second in the largest of ns, µs,
    ms with the fraction's trailing zeros dropped; else h, m and seconds"""
    if ns == 0: return '0s'
    sign, u = ('-' if ns < 0 else ''), abs(ns)
    def frac(v, digits):
        whole, part = divmod(v, 10 ** digits)
        tail = str(part).rjust(digits, '0').rstrip('0') if digits else ''
        return str(whole) + ('.' + tail if tail else '')
    if u < 10 ** 9:
        unit, digits = ('ns', 0) if u < 10 ** 3 else ('µs', 3) if u < 10 ** 6 else ('ms', 6)
        return sign + frac(u, digits) + unit
    secs, rest = divmod(u, 10 ** 9)
    h, m, sec = secs // 3600, secs // 60 % 60, secs % 60
    text = frac(sec * 10 ** 9 + rest, 9) + 's'
    if secs >= 60: text = str(m) + 'm' + text
    if secs >= 3600: text = str(h) + 'h' + text
    return sign + text


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
            'nestedSetParent': n['parent'],
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
    def elements(self, row, f):
        """the values of an event or link field, one per element holding it;
        None for any other field"""
        if f['k'] == 'intrinsic':
            return {'event:name': lambda: [e[1] for e in row['events']],
                    'event:timeSinceStart': lambda: [e[0] - row['start_ns'] for e in row['events']],
                    'link:spanID': lambda: [k[1] for k in row['links']],
                    'link:traceID': lambda: [k[0] for k in row['links']]}.get(f['name'], lambda: None)()
        if f['k'] == 'attr' and f['scope'] in ('event', 'link'):
            items = row['events'] if f['scope'] == 'event' else row['links']
            return [v for *_, at in items for k, _, v in at if k == f['key']]
        return None

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
            # `docs/api.md` §4.2, "Which value a span *has*": an event or link
            # field is decided per element — a condition matches when any
            # element holding the field satisfies it, `!=` and `!~` when every
            # one does
            if self.elements(row, l) is None and self.elements(row, r) is not None:
                l, r, op = r, l, FLIP.get(op, op)          # the field on the left
            vals = self.elements(row, l)
            if vals is not None and not (r['k'] == 'lit' and r['t'] == 'nil'):
                rv = self.value(row, r)
                if op in ('!=', '!~'): return all(self.cmp(op, v, rv) for v in vals)
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
            # `docs/api.md` §4.2, "Field-vs-field comparison": an absent key
            # on either side, or operands of two types, is no match for every
            # operator, `!=` included
            if both_fields and (lv is None or rv is None or kind(lv) != kind(rv)): return False
            return self.cmp(op, lv, rv)
        raise ValueError(f'no predicate for {e}')

    # --- a whole query, the independent side of the check --------------------
    def answer(self, q):
        sp, pipe = q['spanset'], q['pipeline']
        cmpst = next((s for s in pipe if s['k'] == 'compare'), None)
        if cmpst: raise NotImplementedError('compare(): compare_answer (#600 part 1b-2)')
        metric = next((s for s in pipe if s['k'] == 'metric'), None)
        if metric: raise NotImplementedError('metrics: range_answer (#600 part 1b-1)')
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
                partners = [a for a in A if a['trace_id'] == b['trace_id'] and b['parent_span_id']
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

    # ------------------------------------------ what the route itself returns --
    #
    # The independent side of the check review round 4 asked for: not "which
    # spans match" but "what does the request answer". Each shape below is
    # written from the document that defines it — the trace envelope of
    # `sql-schema.md` §5.2, the series envelope of §5.6, `compare()`'s two
    # distributions — and never from the shipped compiler's SQL.

    def value_field(self, row, e):
        """A field in VALUE position: `select(...)`, `by(...)`, and either side
        of a field-against-field comparison: the one value the span has
        (`docs/api.md` §4.2, "Which value a span *has*"), the same as in
        predicate position."""
        if e['k'] == 'lit': return self.lit(e)
        if e['k'] == 'attr':
            if e['scope'] == 'resource' and e['key'] == 'service.name':
                return row['service']
            got = self.attr(row, e['scope'], e['key'])
            return None if got is None else got[1]
        if e['k'] == 'intrinsic':
            v = self.intrinsic(row, e['name'])
            if v == '__unsupported__': raise ValueError(f'no value for {e}')
            return v
        raise ValueError(f'no value for {e}')

    def api_answer(self, q):
        """The rows the request's own statement must return."""
        cmpst = next((s for s in q['pipeline'] if s['k'] == 'compare'), None)
        if cmpst: raise NotImplementedError('compare(): compare_answer (#600 part 1b-2)')
        metric = next((s for s in q['pipeline'] if s['k'] == 'metric'), None)
        if metric: raise NotImplementedError('metrics: range_answer (#600 part 1b-1)')
        return self.search_answer(q)

    # `docs/api.md` §4.2, "The matched-span projection": the fields the span
    # summary already carries, and `span:childCount`, are never projected.
    NOT_PROJECTED = {'duration', 'span:duration', 'traceDuration', 'trace:duration',
                     'rootServiceName', 'trace:rootService', 'rootName', 'trace:rootName',
                     'trace:id', 'span:id', 'childCount', 'span:childCount'}
    # ... and only a field stored as a span-row column projects under `!=` and
    # `!~`: these, and `resource.service.name`.
    COLUMNS = {'name', 'status', 'kind', 'statusMessage', 'parentID',
               'instrumentation:name', 'instrumentation:version'}

    def last_attr(self, row, scope, key):
        """the last element of an event or link scope holding `key`, or None"""
        items = row['events'] if scope == 'event' else row['links']
        held = [(t, v) for *_, at in items for k, t, v in at if k == key]
        return held[-1] if held else None

    def wire(self, row, f, leaf=None, last=False):
        """`(identity, key, type, value)` of field `f` on `row` as the response
        carries it, or None when the span holds no value: the bare key, the
        stored type, `status` and `kind` as keywords. An event or link field
        is the LAST element satisfying `leaf` (`docs/api.md` §4.2, "Which
        value a span *has*"); with no `leaf`, the last element holding the key
        when `last` (the projection) and the first otherwise (a `by()` key)."""
        if f['k'] == 'attr':
            if f['key'] == 'service.name' and f['scope'] == 'resource':
                return ('resource', 'service.name'), 'service.name', arm('string', row['service'])
            if f['scope'] in ('event', 'link') and leaf is not None:
                items = row['events'] if f['scope'] == 'event' else row['links']
                rv = self.value(row, leaf['r'])
                hits = [(k, t, v) for *_, at in items for k, t, v in at
                        if k == f['key'] and self.cmp(leaf['op'], v, rv)]
                if not hits: return None
                k, t, v = hits[-1]
                return (f['scope'], k), k, arm(VALUE_TYPE[t], v)
            scopes = UNSCOPED_ORDER if f['scope'] == 'unscoped' else (f['scope'],)
            for sc in scopes:
                got = (self.last_attr(row, sc, f['key']) if last and sc in ('event', 'link')
                       else self.attr(row, sc, f['key']))
                if got is not None:
                    return (sc, f['key']), f['key'], arm(VALUE_TYPE[got[0]], got[1])
            return None
        n = f['name'].replace('span:', '') if f['name'] in (
            'span:name', 'span:status', 'span:kind', 'span:statusMessage', 'span:duration') else f['name']
        vals = self.elements(row, f)
        if vals is not None:
            rv = None if leaf is None else self.value(row, leaf['r'])
            vals = [v for v in vals if leaf is None or self.cmp(leaf['op'], v, rv)]
            if not vals: return None
            v = vals[-1] if leaf is not None or last else vals[0]
            return ('intrinsic', n), n, arm('int' if n == 'event:timeSinceStart' else 'string', v)
        if n in ('duration', 'traceDuration', 'trace:duration'):
            return ('intrinsic', n), n, arm('string', go_duration(self.intrinsic(row, n)))
        v = self.intrinsic(row, n)
        if n == 'status': v = STATUS_KEYWORDS[v]
        if n == 'kind': v = KIND_KEYWORDS[v]
        if v == '__unsupported__': return None
        return ('intrinsic', n), n, arm('int' if isinstance(v, int) else 'string', v)

    @staticmethod
    def fields_of(e):
        """the distinct fields a comparison's operands name"""
        if e['k'] in ('attr', 'intrinsic'):
            return [json.dumps(e, sort_keys=True)]
        if e['k'] == 'bin' and e['op'] in '+-*/%^': return Interp.fields_of(e['l']) + Interp.fields_of(e['r'])
        if e['k'] == 'unary' and e['op'] == '-': return Interp.fields_of(e['e'])
        if e['k'] == 'lit': return []
        return [None]                                  # a comparison used as an operand

    def projected(self, row, q):
        """`docs/api.md` §4.2, "The matched-span projection" and "What a
        negation projects": the response's `name` and `attributes` for `row`.
        Every single-field condition of the query's filters that matched the
        span, in the order written, then every `select()` field; a field once."""
        out, seen, name = [], set(), [False]

        def add(f, leaf=None):
            if f['k'] == 'intrinsic' and f['name'] in self.NOT_PROJECTED: return
            if f['k'] == 'intrinsic' and f['name'] in ('name', 'span:name'):
                name[0] = True; return
            w = self.wire(row, f, leaf, last=True)
            if w is None or w[0] in seen: return
            seen.add(w[0]); out.append([w[1], w[2]])

        def walk(e, odd):
            if e is None or e['k'] == 'lit': return
            if e['k'] == 'unary' and e['op'] == '!': return walk(e['e'], not odd)
            if e['k'] == 'attr':                       # truthiness
                if not odd and self.pred(row, e): add(e)
                return
            if e['k'] != 'bin': return
            if e['op'] in ('&&', '||'):
                walk(e['l'], odd); walk(e['r'], odd); return
            l, r, op = e['l'], e['r'], e['op']
            if l['k'] == 'lit' and l['t'] == 'nil': l, r = r, l
            if r['k'] == 'lit' and r['t'] == 'nil':  # presence projects, absence does not
                if (op == '!=') != odd and self.value(row, l) is not None: add(l)
                return
            fs = set(self.fields_of(l) + self.fields_of(r))
            if odd or len(fs) != 1 or None in fs or not self.pred(row, e): return
            f = json.loads(next(iter(fs)))
            column = (f['k'] == 'intrinsic' and f['name'].replace('span:', '') in self.COLUMNS) or (
                f['k'] == 'attr' and f['key'] == 'service.name' and f['scope'] == 'resource')
            if op in ('!=', '!~') and not column: return
            if self.elements(row, f) is not None and self.elements(row, l) is None:
                e = {'k': 'bin', 'op': FLIP.get(op, op), 'l': r, 'r': l}
            add(f, e if self.elements(row, f) is not None else None)

        def spansets(sp):
            if sp['k'] == 'filter': walk(sp['body'], False)
            elif sp['k'] in ('sp_and', 'sp_or', 'struct'): spansets(sp['l']); spansets(sp['r'])
        spansets(q['spanset'])
        for st in q['pipeline']:
            if st['k'] == 'filter_stage': spansets(st['spanset'])
            if st['k'] == 'select':
                for f in st['fields']: add(f)
        return (row['name'] if name[0] and row['name'] else None), out

    def span_answer(self, r, q):
        name, attrs = self.projected(r, q)
        return {'id': r['span_id'], 'start': r['start_ns'], 'dur': r['dur'], 'name': name, 'attrs': attrs}

    def group_key(self, r, key):
        """the `by()` key's response attribute: `by(<key>)`, typed (`docs/api.md` §4.2)"""
        label = 'by(' + (key['name'] if key['k'] == 'intrinsic' else
                         ('.' if key['scope'] == 'unscoped' else key['scope'] + '.') + key['key']) + ')'
        w = self.wire(r, key)
        return [label, ['stringValue', 'nil'] if w is None else w[2]]

    def field_text(self, f):
        """a field as the query renders it: `.k`, `span.k`, an intrinsic's name"""
        if f['k'] == 'intrinsic':
            return {'span:duration': 'duration', 'span:name': 'name', 'span:status': 'status',
                    'span:kind': 'kind'}.get(f['name'], f['name'])
        return ('.' if f['scope'] == 'unscoped' else f['scope'] + '.') + f['key']

    def aggregate(self, st, spans):
        """`docs/api.md` §4.2: an aggregate stage's value over a spanSet, as
        `(the number the comparison reads, the typed attribute value)`, or
        None when no span contributes"""
        if st['op'] == 'count': return len(spans), arm('int', len(spans))
        arg = st['arg']
        if arg['k'] == 'intrinsic' and self.field_text(arg) == 'duration':
            vals = [r['dur'] for r in spans]
            v = {'min': min(vals), 'max': max(vals), 'sum': sum(vals),
                 'avg': sum(vals) // len(vals)}[st['op']]
            return v, arm('string', go_duration(v))
        got = [self.attr(r, arg['scope'], arg['key']) for r in spans]
        got = [g for g in got if g is not None and g[0] in ('i', 'd')]
        if not got: return None
        if st['op'] in ('min', 'max'):
            t, v = (min if st['op'] == 'min' else max)(got, key=lambda g: float(g[1]))
            return v, arm(VALUE_TYPE[t], v)
        total = sum(float(g[1]) for g in got)
        if st['op'] == 'sum':
            return total, (arm('int', int(total)) if all(g[0] == 'i' for g in got) else arm('double', total))
        return total / len(got), arm('double', total / len(got))

    def pipeline_fold(self, q, rs):
        """`docs/api.md` §4.2, "The pipeline is one ordered fold": each stage
        maps the trace's spanSets, each `[attributes, spans]`, to new ones"""
        order = lambda r: (r['start_ns'], r['span_id'])
        sets = [[[], sorted(rs, key=order)]]
        for st in q['pipeline']:
            if st['k'] == 'filter_stage':
                keep = {id(r) for r in self.spanset(st['spanset'])}
                sets = [[a, [r for r in sp if id(r) in keep]] for a, sp in sets]
            elif st['k'] == 'aggregate':
                out = []
                for a, sp in sets:
                    v = self.aggregate(st, sp)
                    if v is not None and self.cmp(st['cmp'], v[0], self.value(sp[0], st['value'])):
                        key = st['op'] + '(' + (self.field_text(st['arg']) if st['arg'] else '') + ')'
                        out.append([a + [[key, v[1]]], sp])
                sets = out
            elif st['k'] == 'by':
                out = []
                for a, sp in sets:
                    gs = {}
                    for r in sp: gs.setdefault(json.dumps(self.group_key(r, st['key'])), []).append(r)
                    out += [[a + [json.loads(k)], v] for k, v in gs.items()]
                sets = out
            elif st['k'] == 'coalesce':
                sets = [[[], sorted({id(r): r for _, sp in sets for r in sp}.values(), key=order)]]
            sets = [x for x in sets if x[1]]
        return sets

    def search_answer(self, q):
        """`docs/api.md` §4.2: the newest LIMIT traces by their newest matching
        span, before any pipeline stage, each with its root and extent, and its spanSets: one without
        attributes is the flat `matched` and `spans`, capped at SPSS in
        (start, id) order; any other is a group with its own count and cap."""
        by_trace = {}
        for r in self.spanset(q['spanset']): by_trace.setdefault(r['trace_id'], []).append(r)
        out = []
        for tid, rs in by_trace.items():
            sets = self.pipeline_fold(q, rs)
            if not sets: continue
            t = self.fx.traces[tid]
            tr = {'id': tid, 'root_service': t['root_service'], 'root_name': t['root_name'],
                  'start': t['start'], 'dur': t['end'] - t['start'], 'matched': None,
                  'spans': None, 'groups': None}
            if len(sets) == 1 and not sets[0][0]:
                tr['matched'] = len(sets[0][1])
                tr['spans'] = [self.span_answer(r, q) for r in sets[0][1][:SPSS]]
            else:
                tr['groups'] = [{'attrs': a, 'matched': len(sp),
                                 'spans': [self.span_answer(r, q) for r in sp[:SPSS]]} for a, sp in sets]
            out.append((max(r['start_ns'] for r in rs), tr))
        out.sort(key=lambda x: (-x[0], x[1]['id']))
        return {'partial': False, 'traces': [tr for _, tr in out[:LIMIT]]}

    def exemplar_budget(self, q):
        for k, v in q['hints']:
            if k == 'exemplars':
                if v == 'true': return DEFAULT_EXEMPLARS
                if v == 'false': return 0
                try: return min(int(float(v)), DEFAULT_EXEMPLARS)
                except ValueError: return 0
        return DEFAULT_EXEMPLARS

    # `docs/api.md` §4.4, "Grouping": a missing value of these keys is a `nil`
    # label, of any other key no label; one key missing alone is `nil` either way
    WELL_KNOWN = {('resource', k) for k in ('service.name', 'cluster', 'namespace', 'pod',
                  'container', 'k8s.cluster.name', 'k8s.namespace.name', 'k8s.pod.name',
                  'k8s.container.name')} | {('span', k) for k in ('http.status_code',
                  'http.method', 'http.url')}
    LABEL_TYPE = {'s': 'string', 'i': 'int', 'd': 'double', 'b': 'bool'}

    def series_key(self, r, f):
        """`(sort key, label)` of `by(f)` on span `r`: the label `<scope>.<key>`
        (`.<key>` unscoped) with the value in its stored type; a missing value
        `nil`, sorted as the empty text (`docs/api.md` §4.4)"""
        if f['k'] == 'intrinsic':                      # a string intrinsic: `name`
            v = self.intrinsic(r, f['name'])
            return (v, 's'), (f['name'], ('string', v))
        name = ('.' if f['scope'] == 'unscoped' else f['scope'] + '.') + f['key']
        if f['scope'] == 'resource' and f['key'] == 'service.name':
            return (r['service'], 's'), (name, ('string', r['service']))
        got = self.attr(r, f['scope'], f['key'])
        if got is None:
            code = 'N' if (f['scope'], f['key']) in self.WELL_KNOWN else 'n'
            return ('', code), (name, ('string', 'nil'))
        t, v = got
        if t == 'a':
            # each array is its own series labelled `nil` by the engine's text
            # of it, `['x','y']`, so two arrays of one text share a series
            # (`docs/api.md` §4.4; #600 part 1b-3)
            text = '[' + ','.join(f"'{e}'" if et == 's' else engine_double_text(e) if et == 'd'
                                  else str(e) for et, e in v) + ']'
            return (text, 'a'), (name, ('string', 'nil'))
        text = ('true' if v else 'false') if t == 'b' else str(v)
        return (text, t), (name, (self.LABEL_TYPE[t], v))

    def range_answer(self, q, start, end, step=60 * 10**9):
        """`docs/api.md` §4.4: the `query_range` answer of a metrics query that
        is not `compare()`, as the command writes it."""
        metric = next(s for s in q['pipeline'] if s['k'] == 'metric')
        second = next((s for s in q['pipeline'] if s['k'] == 'second'), None)
        fn = metric['fn']
        dense = fn in ('rate', 'count_over_time', 'histogram_over_time', 'quantile_over_time')
        # a quantile's levels, in the query's order, each its own series
        levels = [float(a['v']) for a in metric['args'][1:]] if fn == 'quantile_over_time' else [None]
        # "Bucketing": snapped outward; labels S … E; label L holds (L − step, L]
        S, E = start // step * step, -(-end // step) * step
        grid = [(S + i * step) // 10**6 for i in range((E - S) // step + 1)]
        arg = metric['args'][0] if metric['args'] and fn not in ('rate', 'count_over_time') else None

        def target(r):
            """the target in its unit: `duration` in seconds, an attribute's
            Int64 or Float64 as a double, `span:childCount`; None: not counted"""
            if arg is None or arg['k'] == 'intrinsic' and arg['name'] in ('duration', 'span:duration'):
                return r['dur'] / 1e9
            if arg['k'] == 'intrinsic':                    # span:childCount
                return float(self.child_count(r))
            got = self.attr(r, arg['scope'], arg['key'])
            return float(got[1]) if got and got[0] in ('i', 'd') else None
        cells = {}
        for r in self.spanset(q['spanset']):
            if not S - step < r['start_ns'] <= E: continue
            if fn.endswith('_over_time') and fn != 'count_over_time' and target(r) is None: continue
            if fn == 'histogram_over_time':
                if r['dur'] < 2: continue
                b = 1 << (r['dur'] - 1).bit_length()       # the log2 bucket, ns
                key = ((b,), (('__bucket', ('double', b / 1e9)),))
            elif 'by' in metric:
                sk, label = self.series_key(r, metric['by'])
                key = (sk, (label,))
            elif fn == 'quantile_over_time':
                key = ((), ())                          # its `p` label is its only one
            else:
                key = ((), (('__name__', ('string', fn)),))
            t = ((r['start_ns'] - 1) // step + 1) * step // 10**6
            cells.setdefault(key, {}).setdefault(t, []).append(r)
        # an ungrouped count or rate no span reaches, not ranked, is one series
        # of zeros (a trailing comparison may then remove them) (r2)
        if not cells and 'by' not in metric and fn in ('rate', 'count_over_time') and not second:
            cells[((), (('__name__', ('string', fn)),))] = {}

        def value(rs, level=None):
            # a duration aggregates in nanoseconds and converts once
            # (`float64(ns)/1e9`); any other target aggregates its own number
            dur = arg is None or arg['k'] == 'intrinsic' and arg['name'] in ('duration', 'span:duration')
            d = [x['dur'] for x in rs] if dur else [target(x) for x in rs]
            k = 1e9 if dur else 1
            if fn == 'quantile_over_time':
                # the exact quantile: the ceil(p * n)-th smallest duration (at
                # least the first), as the 32-bit float the t-digest returns
                d = sorted(d)
                return f32(d[max(1, math.ceil(level * len(d))) - 1]) / k
            return {'rate': lambda: len(rs) / (step / 1e9), 'count_over_time': lambda: float(len(rs)),
                    'histogram_over_time': lambda: float(len(rs)),
                    'sum_over_time': lambda: float(sum(d)) / k,
                    'min_over_time': lambda: float(min(d)) / k,
                    'max_over_time': lambda: float(max(d)) / k,
                    'avg_over_time': lambda: (sum(d) / len(d)) / k}[fn]()
        # dense: every label, 0 where empty; the value aggregations: occupied only
        pts = [((key[0] + (i,), key[1] + ((('p', ('double', lv)),) if lv is not None else ())),
                t, value(by_t[t], lv) if t in by_t else 0.0, by_t.get(t, []))
               for key, by_t in cells.items() for i, lv in enumerate(levels)
               for t in grid if dense or t in by_t]
        if fn == 'quantile_over_time':
            # an exemplar sits on the `p` series whose value at its own bucket
            # is nearest its duration, ties to the first (`docs/api.md` §4.4)
            at = {(p[0][0][:-1], p[1]): [] for p in pts}
            for p in pts: at[(p[0][0][:-1], p[1])].append(p)
            def placed(p):
                if not p[3]: return []
                top = max(p[3], key=lambda x: (x['dur'], x['span_id']))
                own = top['dur'] / 1e9
                best = min(at[(p[0][0][:-1], p[1])], key=lambda o: (abs(o[2] - own), o[0][0][-1]))
                return p[3] if best[0] == p[0] else []
            pts = [(k, t, v, placed((k, t, v, rs))) for k, t, v, rs in pts]
        if 'cmp' in metric:
            op, lit = metric['cmp']
            th = self.lit(lit) / 1e9 if lit.get('t') == 'duration' else self.lit(lit)
            pts = [p for p in pts if self.cmp(op, p[2], th)]
        if second:                     # per timestamp, after the comparison, ties by labels
            keep = []
            for t in grid:
                at = sorted((p for p in pts if p[1] == t),
                            key=lambda p: (-p[2] if second['fn'] == 'topk' else p[2], p[0][0]))
                keep += at[:second['n']]
            pts = keep
        budget = self.exemplar_budget(q)
        series, cand = {}, []
        for key, t, v, rs in pts:
            s = series.setdefault(key, {'labels': [[k, list(x)] for k, x in key[1]],
                                        'samples': [], 'exemplars': []})
            s['samples'].append([t, v])
            if rs and budget:          # the longest span, the larger id on a tie
                top = max(rs, key=lambda x: (x['dur'], x['span_id']))
                cand.append(((t, key[0]), s, {'labels': [['trace:id', ['string', top['trace_id']]]],
                             'value': v if dense and fn != 'quantile_over_time' else target(top),
                             'ts': top['start_ns'] // 10**6}))
        # over the budget, an even stride in bucket order, then series order
        # (`docs/api.md` §4.4): candidate `x` of `T` is kept when
        # `c = ⌈x·b/T⌉ < b` and `⌊c·T/b⌋ = x` (#600 part 1b-2)
        cand.sort(key=lambda c: c[0])
        T = len(cand)
        for x, (_, s, e) in enumerate(cand):
            c = -(-x * budget // T)
            if T <= budget or (c < budget and c * T // budget == x):
                s['exemplars'].append(e)
        out = []
        for key in sorted(series, key=lambda k: k[0]):
            s = series[key]
            s['samples'].sort()
            s['exemplars'].sort(key=lambda e: e['ts'])
            out.append(s)
        return {'series': out}

    def instant_answer(self, q, start, end, step=60 * 10**9):
        """`docs/api.md` §4.4, "The instant `query` form": one bucket over the
        snapped `[S, E)`, reported at `E`; `rate` divides by `E − S` seconds;
        no exemplars. An ungrouped count or rate is always one series; any
        other shape only the series some span reaches."""
        metric = next(s for s in q['pipeline'] if s['k'] == 'metric')
        second = next((s for s in q['pipeline'] if s['k'] == 'second'), None)
        fn = metric['fn']
        S, E = start // step * step, -(-end // step) * step
        levels = [float(a['v']) for a in metric['args'][1:]] if fn == 'quantile_over_time' else [None]
        arg = metric['args'][0] if metric['args'] and fn not in ('rate', 'count_over_time') else None
        dur = arg is None or arg['k'] == 'intrinsic' and arg['name'] in ('duration', 'span:duration')

        def target(r):
            if dur: return r['dur'] / 1e9
            if arg['k'] == 'intrinsic': return float(self.child_count(r))
            got = self.attr(r, arg['scope'], arg['key'])
            return float(got[1]) if got and got[0] in ('i', 'd') else None
        groups = {}
        for r in self.spanset(q['spanset']):
            if not S <= r['start_ns'] < E: continue
            if fn.endswith('_over_time') and fn != 'count_over_time' and target(r) is None: continue
            if fn == 'histogram_over_time':
                if r['dur'] < 2: continue
                b = 1 << (r['dur'] - 1).bit_length()
                key = ((b,), (('__bucket', ('double', b / 1e9)),))
            elif 'by' in metric:
                sk, label = self.series_key(r, metric['by'])
                key = (sk, (label,))
            elif fn == 'quantile_over_time':
                key = ((), ())
            else:
                key = ((), (('__name__', ('string', fn)),))
            groups.setdefault(key, []).append(r)
        if not groups and 'by' not in metric and fn in ('rate', 'count_over_time'):
            groups[((), (('__name__', ('string', fn)),))] = []

        def value(rs, level):
            d = [x['dur'] for x in rs] if dur else [target(x) for x in rs]
            k = 1e9 if dur else 1
            if fn == 'quantile_over_time':
                d = sorted(d)
                return f32(d[max(1, math.ceil(level * len(d))) - 1]) / k
            return {'rate': lambda: len(rs) / ((E - S) / 1e9), 'count_over_time': lambda: float(len(rs)),
                    'histogram_over_time': lambda: float(len(rs)),
                    'sum_over_time': lambda: float(sum(d)) / k, 'min_over_time': lambda: float(min(d)) / k,
                    'max_over_time': lambda: float(max(d)) / k,
                    'avg_over_time': lambda: (sum(d) / len(d)) / k}[fn]()
        pts = [((key[0] + (i,), key[1] + ((('p', ('double', lv)),) if lv is not None else ())), value(rs, lv))
               for key, rs in groups.items() for i, lv in enumerate(levels)]
        if 'cmp' in metric:
            op, lit = metric['cmp']
            th = self.lit(lit) / 1e9 if lit.get('t') == 'duration' else self.lit(lit)
            pts = [p for p in pts if self.cmp(op, p[1], th)]
        if second:
            pts = sorted(pts, key=lambda p: (-p[1] if second['fn'] == 'topk' else p[1], p[0][0]))[:second['n']]
        return {'series': [{'labels': [[k, list(x)] for k, x in key[1]], 'samples': [[E // 10**6, v]],
                            'exemplars': []} for key, v in sorted(pts, key=lambda p: p[0][0])]}

    # `docs/api.md` §4.4, "`compare()`'s keys": the 25 keys always answered
    COMPARE_KNOWN = ('name', 'kind', 'status', 'statusMessage', 'rootName', 'rootServiceName',
                     'instrumentation:name', 'instrumentation:version', 'resource.service.name',
                     'resource.cluster', 'resource.container', 'resource.namespace', 'resource.pod',
                     'resource.k8s.cluster.name', 'resource.k8s.container.name',
                     'resource.k8s.namespace.name', 'resource.k8s.pod.name', 'span.http.method',
                     'span.http.request.method', 'span.http.route', 'span.http.status_code',
                     'span.http.url', 'span.server.address', 'span.url.path', 'span.url.route')

    def compare_pairs(self, r):
        """every `(key, value, stored type)` a span counts once in `compare()`:
        its span, instrumentation and resource attributes of a TraceQL type
        and eight intrinsics; no event or link key (C9, `docs/api.md` §4.4)"""
        ty = {'s': 'string', 'i': 'int', 'd': 'double', 'b': 'bool'}
        out = [('span.' + k, v, ty[t]) for k, t, v in r['attrs'] if t in ty]
        out += [('instrumentation.' + k, v, ty[t]) for k, t, v in r.get('scope_attrs', []) if t in ty]
        out += [('resource.' + k, v, ty[t]) for k, t, v in r['resource'] if t in ty and k != 'service.name']
        out.append(('resource.service.name', r['service'], 'string'))
        tr = self.fx.traces[r['trace_id']]
        for k, v in (('name', r['name']), ('kind', KIND_KEYWORDS[r['kind']]),
                     ('status', STATUS_KEYWORDS[r['status_code']]), ('statusMessage', r['status_message']),
                     ('instrumentation:name', r['scope_name']), ('instrumentation:version', r['scope_version']),
                     ('rootName', tr['root_name']), ('rootServiceName', tr['root_service'])):
            out.append((k, v, 'string'))
        return out

    def compare_answer(self, q, start, end, instant, step=60 * 10**9):
        """`docs/api.md` §4.4, `compare()`: per key, its kept values per side,
        then for a well-known key `key=nil` per side, then `<side>_total`."""
        st = next(s for s in q['pipeline'] if s['k'] == 'compare')
        topn = int(st['args'][0]) if st['args'] else 10
        win = (int(st['args'][1]), int(st['args'][2])) if len(st['args']) >= 3 else (0, 0)
        S, E = start // step * step, -(-end // step) * step
        grid = [E // 10**6] if instant else [(S + i * step) // 10**6 for i in range((E - S) // step + 1)]
        sel_ids = {id(r) for r in self.spanset(st['inner'])}
        pop = [{}, {}]; sums = {}; vals = {}; best = {}
        for r in self.spanset(q['spanset']):
            if instant and not S <= r['start_ns'] < E: continue
            if not instant and not S - step < r['start_ns'] <= E: continue
            side = int(id(r) in sel_ids and (win == (0, 0) or win[0] < r['start_ns'] <= win[1]))
            t = E // 10**6 if instant else ((r['start_ns'] - 1) // step + 1) * step // 10**6
            pop[side][t] = pop[side].get(t, 0) + 1
            for k, v, ty in self.compare_pairs(r):
                sums.setdefault(k, [{}, {}])[side][t] = sums.setdefault(k, [{}, {}])[side].get(t, 0) + 1
                cell = vals.setdefault((side, k, v, ty), {})
                cell[t] = cell.get(t, 0) + 1
                cur = best.get((side, k, t))
                if cur is None or (r['dur'], r['span_id']) > (cur['dur'], cur['span_id']):
                    best[(side, k, t)] = r

        def text(v, ty):
            return ('true' if v else 'false') if ty == 'bool' else (format(v, 'g') if ty == 'double' else str(v))
        kept = {}
        for (side, k, v, ty), cell in vals.items():
            kept.setdefault((side, k), []).append((-sum(cell.values()), text(v, ty), ty, v, cell))
        series = []
        meta = ('baseline', 'selection')
        for k in sorted(set(sums) | set(self.COMPARE_KNOWN)):
            for side in (0, 1):
                # `topN` by count, ties by value text then type; listed by value text then type
                top = sorted(kept.get((side, k), []), key=lambda x: x[:3])[:topn]
                for _, _, ty, v, cell in sorted(top, key=lambda x: x[1:3]):
                    series.append(([['__meta_type', ['string', meta[side]]], [k, [ty, v]]], cell, None))
            of_key = sums.get(k, [{}, {}])
            known = k in self.COMPARE_KNOWN
            if known:
                for side in (0, 1):
                    series.append(([['__meta_type', ['string', meta[side]]], [k, ['string', 'nil']]],
                                   {t: n - of_key[side].get(t, 0) for t, n in pop[side].items()}, None))
            for side in (0, 1):
                series.append(([['__meta_type', ['string', meta[side] + '_total']], [k, ['string', 'nil']]],
                               pop[side] if known else of_key[side], (side, k)))
        out = []
        for labels, cell, ex_key in series:
            if not any(cell.values()): continue
            samples = [[t, float(cell.get(t, 0))] for t in grid] if not instant else \
                      [[t, float(n)] for t, n in sorted(cell.items())]
            out.append({'labels': labels, 'samples': samples, 'exemplars': [], '_ex': ex_key})
        budget = self.exemplar_budget(q)
        if not instant and budget:
            # each key's candidates — per side, key and bucket the longest span
            # carrying it — go onto its side's total; then an even stride over
            # the whole answer, keys in order, sides baseline first
            index = {s['_ex']: s for s in out if s['_ex']}
            cand = []
            for side in (0, 1):
                for k in sorted(sums):
                    s = index.get((side, k))
                    for t in sorted(sums[k][side]):
                        top = best[(side, k, t)]
                        if s is None: continue
                        v = dict((a, b) for a, b in s['samples']).get(t)
                        cand.append((s, {'labels': [['trace:id', ['string', top['trace_id']]]], 'value': v,
                                         'ts': top['start_ns'] // 10**6}))
            if len(cand) > budget:
                cand = [cand[i * len(cand) // budget] for i in range(budget)]
            for s, e in cand: s['exemplars'].append(e)
        for s in out: del s['_ex']
        return {'series': out}
