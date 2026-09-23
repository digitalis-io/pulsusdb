#!/usr/bin/env python3
"""The TraceQL parser the catalogue's two sides share, and the ONLY thing they
share.

`docs/TraceQL/query-catalogue.md` checks each rendered statement against an
interpreter that walks the same query over the fixture rows. For that check to
mean anything the two sides must not share a semantic rule: a shared status
code, a shared scope order or a shared depth bound moves both answers together
and the comparison stays green (review round 4, finding 4). So this module
holds SYNTAX only — how the text becomes a tree — and nothing that decides an
answer:

| here | not here |
|---|---|
| the token shapes, the precedence, the AST | the numeric code a status or kind keyword stands for |
| the KEYWORD SPELLINGS a scope, status or kind may use | the ORDER an unscoped read tries the scopes in |
| the escape rules inside a string literal | the recursive climb's depth bound |
| the unit SPELLINGS a duration literal may be written with | how many nanoseconds each of those units is |
| | how any of it becomes SQL, or an answer |

A status keyword parses to `{'t': 'status', 'v': 'error'}` — the NAME. A
duration parses to `{'t': 'duration', 'v': '1.5', 'unit': 'ms'}` — the digits
and the unit's SPELLING. The renderer (`catalogue_render.py`) and the
interpreter (`catalogue_interp.py`) each map those to a number from their own
table, and `measure/perturb_check.py` perturbs one table at a time and requires
the comparison to go red for each.

A rule that stayed here would move both sides together and the comparison would
stay green, so this module is checked a second way: `measure/parse_vectors.tsv`
holds one line per rule with the tree it must produce, written out by hand, and
`measure/catalogue.py` fails if any of them parses to something else. That is
what makes a change to an escape rule, to the precedence or to `minInt` visible;
the comparison cannot see it.
"""
import re

# ---------------------------------------------------------------- tokenizer --
TOKEN = re.compile(r"""
    (?P<ws>\s+)
  | (?P<bstring>`[^`]*`)
  | (?P<string>"(?:\\.|[^"\\])*")
  | (?P<duration>-?(?:\d+\.\d*|\.\d+|\d+)\s*(?:ns|us|µs|ms|s|m|h)\b)
  | (?P<number>-?(?:\d+\.\d*|\.\d+|\d+)(?:[eE][-+]?\d+)?)
  | (?P<op><<|>>|!<<|!>>|&<<|&>>|!<|!>|&<|&>|!~|&~|&&|\|\||>=|<=|!=|=~|=|<|>|~|\+|-|\*|/|%|\^|!|\||\(|\)|\{|\}|\[|\]|,|:|\.)
  | (?P<ident>[A-Za-z_][A-Za-z0-9_]*)
""", re.X)

def lex(src):
    out, i = [], 0
    while i < len(src):
        m = TOKEN.match(src, i)
        if not m:
            raise ValueError(f'cannot tokenize at {i}: {src[i:i+20]!r}')
        i = m.end()
        kind = m.lastgroup
        if kind != 'ws':
            out.append((kind, m.group()))
    out.append(('eof', ''))
    return out

# The unit spellings a duration literal may be written with. The SPELLINGS are
# lexical and belong here; what each one is worth in nanoseconds decides an
# answer, so it is written once on each side (`catalogue_render.DURATION_NS`,
# `catalogue_interp.DURATION_NS`). Review round 5 found the multipliers here:
# changing `ms` from 10**6 to 10**3 moved both sides together and the whole
# catalogue still reported complete agreement.
DUR_UNITS = frozenset({'ns', 'us', 'µs', 'ms', 's', 'm', 'h'})

# Keyword SPELLINGS only. What `error` or `server` stands for is a semantic
# question and each side answers it from its own table.
# Sets, not sequences: a sequence here would be an order the two sides share,
# and the order an unscoped read tries the scopes in is exactly the kind of rule
# that must be written once on each side.
STATUS_NAMES = frozenset({'unset', 'ok', 'error'})
KIND_NAMES = frozenset({'unspecified', 'internal', 'server', 'client', 'producer', 'consumer'})
SCOPE_NAMES = frozenset({'span', 'resource', 'event', 'link', 'instrumentation'})

INTRINSICS = {
    'name', 'duration', 'status', 'kind', 'statusMessage', 'childCount',
    'nestedSetLeft', 'nestedSetRight', 'nestedSetParent',
    'rootName', 'rootServiceName', 'traceDuration',
    'span:id', 'span:parentID', 'span:name', 'span:kind', 'span:status',
    'span:statusMessage', 'span:duration', 'span:childCount',
    'trace:id', 'trace:duration', 'trace:rootName', 'trace:rootService',
    'event:name', 'event:timeSinceStart', 'link:spanID', 'link:traceID',
    'instrumentation:name', 'instrumentation:version',
}

def unquote(tok):
    if tok.startswith('`'):
        return tok[1:-1]
    body = tok[1:-1]
    out, i = [], 0
    while i < len(body):
        c = body[i]
        if c != '\\':
            out.append(c); i += 1; continue
        n = body[i + 1]; i += 2
        if n == 'x': out.append(chr(int(body[i:i+2], 16))); i += 2
        elif n == 'u': out.append(chr(int(body[i:i+4], 16))); i += 4
        elif n == 'U': out.append(chr(int(body[i:i+8], 16))); i += 8
        elif n in '01234567': out.append(chr(int(body[i-1:i+2], 8))); i += 2
        else: out.append({'n': '\n', 't': '\t', 'r': '\r', 'a': '\a', 'b': '\b',
                          'f': '\f', 'v': '\v', '\\': '\\', '"': '"', "'": "'"}.get(n, n))
    return ''.join(out)

# ------------------------------------------------------------------- parser --
class P:
    def __init__(self, src):
        self.src = src; self.t = lex(src); self.i = 0
    def peek(self, k=0): return self.t[self.i + k]
    def next(self): self.i += 1; return self.t[self.i - 1]
    def at(self, val): return self.peek()[1] == val
    def eat(self, val):
        if self.at(val): self.next(); return True
        return False
    def expect(self, val):
        if not self.eat(val): raise ValueError(f'expected {val!r} at {self.peek()!r} in {self.src!r}')

    # query := spanset_expr pipeline* hint*
    def query(self):
        e = self.spanset_expr()
        pipeline = []
        while self.at('|'):
            self.next()
            pipeline.append(self.stage())
        hints = []
        while self.peek()[1] == 'with' and self.peek(1)[1] == '(':
            self.next(); self.expect('(')
            h = []
            while not self.at(')'):
                key = self.next()[1]
                self.expect('=')
                h.append((key, self.value_token()))
                self.eat(',')
            self.expect(')')
            hints = h
        return {'spanset': e, 'pipeline': pipeline, 'hints': hints}

    def value_token(self):
        k, v = self.next()
        if k == 'string': return unquote(v)
        if k == 'duration': return v
        return v

    # spanset_expr := structural chain of filters, with && || and parens
    def spanset_expr(self):
        return self.spanset_or()
    def spanset_or(self):
        l = self.spanset_and()
        while self.at('||'):
            self.next(); r = self.spanset_and(); l = {'k': 'sp_or', 'l': l, 'r': r}
        return l
    def spanset_and(self):
        l = self.structural()
        while self.at('&&'):
            self.next(); r = self.structural(); l = {'k': 'sp_and', 'l': l, 'r': r}
        return l
    STRUCT = {'>': 'child', '>>': 'descendant', '<': 'parent', '<<': 'ancestor', '~': 'sibling'}
    def structural(self):
        l = self.spanset_atom()
        while True:
            v = self.peek()[1]
            mod, op = 'plain', None
            if v in self.STRUCT: op = self.STRUCT[v]
            elif v.startswith('!') and v[1:] in self.STRUCT: op, mod = self.STRUCT[v[1:]], 'neg'
            elif v.startswith('&') and v[1:] in self.STRUCT: op, mod = self.STRUCT[v[1:]], 'union'
            if not op: return l
            self.next()
            r = self.spanset_atom()
            l = {'k': 'struct', 'op': op, 'mod': mod, 'l': l, 'r': r}
    def spanset_atom(self):
        if self.eat('('):
            e = self.spanset_expr(); self.expect(')'); return e
        self.expect('{')
        if self.eat('}'): return {'k': 'filter', 'body': None}
        body = self.expr()
        self.expect('}')
        return {'k': 'filter', 'body': body}

    # pipeline stages
    def stage(self):
        v = self.peek()[1]
        if v == '{' or v == '(':
            return {'k': 'filter_stage', 'spanset': self.spanset_expr()}
        name = self.next()[1]
        if name in ('count', 'avg', 'sum', 'min', 'max') and self.at('('):
            self.expect('(')
            arg = None if self.at(')') else self.expr()
            self.expect(')')
            op = self.next()[1]
            val = self.expr()
            return {'k': 'aggregate', 'op': name, 'arg': arg, 'cmp': op, 'value': val}
        if name == 'by':
            self.expect('('); key = self.expr(); self.expect(')')
            return {'k': 'by', 'key': key}
        if name == 'select':
            self.expect('('); fields = [self.expr()]
            while self.eat(','): fields.append(self.expr())
            self.expect(')')
            return {'k': 'select', 'fields': fields}
        if name == 'coalesce':
            self.expect('('); self.expect(')')
            return {'k': 'coalesce'}
        if name in ('rate', 'count_over_time', 'sum_over_time', 'min_over_time', 'max_over_time',
                    'avg_over_time', 'quantile_over_time', 'histogram_over_time'):
            self.expect('(')
            args = []
            if not self.at(')'):
                args.append(self.expr())
                while self.eat(','): args.append(self.expr())
            self.expect(')')
            st = {'k': 'metric', 'fn': name, 'args': args}
            if self.peek()[1] == 'by' and self.peek(1)[1] == '(':
                self.next(); self.expect('('); st['by'] = self.expr(); self.expect(')')
            if self.peek()[0] == 'op' and self.peek()[1] in ('>', '<', '>=', '<=', '=', '!='):
                op = self.next()[1]; st['cmp'] = (op, self.expr())
            return st
        if name in ('topk', 'bottomk'):
            self.expect('('); n = self.next()[1]; self.expect(')')
            return {'k': 'second', 'fn': name, 'n': int(n)}
        if name == 'compare':
            self.expect('(')
            inner = self.spanset_expr()
            args = []
            while self.eat(','): args.append(self.next()[1])
            self.expect(')')
            return {'k': 'compare', 'inner': inner, 'args': args}
        raise ValueError(f'unknown pipeline stage {name!r} in {self.src!r}')

    # field expressions, precedence: || && < cmp < +- < unary < */% < ^
    def expr(self): return self.p_or()
    def p_or(self):
        l = self.p_and()
        while self.at('||'):
            self.next(); l = {'k': 'bin', 'op': '||', 'l': l, 'r': self.p_and()}
        return l
    def p_and(self):
        l = self.p_cmp()
        while self.at('&&'):
            self.next(); l = {'k': 'bin', 'op': '&&', 'l': l, 'r': self.p_cmp()}
        return l
    CMP = ('=', '!=', '=~', '!~', '<', '<=', '>', '>=')
    def p_cmp(self):
        l = self.p_add()
        while self.peek()[1] in self.CMP and self.peek()[0] == 'op':
            op = self.next()[1]; l = {'k': 'bin', 'op': op, 'l': l, 'r': self.p_add()}
        return l
    def p_add(self):
        l = self.p_unary()
        while self.peek()[1] in ('+', '-') and self.peek()[0] == 'op':
            op = self.next()[1]; l = {'k': 'bin', 'op': op, 'l': l, 'r': self.p_unary()}
        return l
    def p_unary(self):
        if self.peek()[1] in ('-', '!') and self.peek()[0] == 'op':
            op = self.next()[1]; return {'k': 'unary', 'op': op, 'e': self.p_unary()}
        return self.p_mul()
    def p_mul(self):
        l = self.p_pow()
        while self.peek()[1] in ('*', '/', '%') and self.peek()[0] == 'op':
            op = self.next()[1]; l = {'k': 'bin', 'op': op, 'l': l, 'r': self.p_pow()}
        return l
    def p_pow(self):
        l = self.atom()
        if self.peek()[1] == '^' and self.peek()[0] == 'op':
            self.next(); return {'k': 'bin', 'op': '^', 'l': l, 'r': self.p_pow()}
        return l
    def atom(self):
        k, v = self.peek()
        if v == '(':
            self.next(); e = self.expr(); self.expect(')'); return e
        if k == 'string': self.next(); return {'k': 'lit', 't': 'string', 'v': unquote(v)}
        if k == 'bstring': self.next(); return {'k': 'lit', 't': 'string', 'v': unquote(v)}
        if k == 'duration':
            self.next()
            m = re.match(r'(-?[\d.]+)\s*(ns|us|µs|ms|s|m|h)', v)
            assert m.group(2) in DUR_UNITS
            return {'k': 'lit', 't': 'duration', 'v': m.group(1), 'unit': m.group(2)}
        if k == 'number': self.next(); return {'k': 'lit', 't': 'number', 'v': v}
        if v == '.' or v in SCOPE_NAMES or k == 'ident':
            return self.field()
        raise ValueError(f'unexpected {v!r} in {self.src!r}')

    def field(self):
        k, v = self.peek()
        # unscoped .key  /  scope.key  /  scope:intrinsic  /  bare intrinsic or keyword
        if v == '.':
            self.next(); return {'k': 'attr', 'scope': 'unscoped', 'key': self.key_path()}
        if v in SCOPE_NAMES and self.peek(1)[1] in ('.', '[', ':'):
            scope = self.next()[1]
            if self.eat(':'):
                name = self.next()[1]
                return {'k': 'intrinsic', 'name': f'{scope}:{name}'}
            if self.at('['):
                self.next(); key = unquote(self.next()[1]); self.expect(']')
                return {'k': 'attr', 'scope': scope, 'key': key}
            self.expect('.')
            if self.at('['):
                self.next(); key = unquote(self.next()[1]); self.expect(']')
                return {'k': 'attr', 'scope': scope, 'key': key}
            if self.peek()[0] == 'string':
                return {'k': 'attr', 'scope': scope, 'key': unquote(self.next()[1])}
            return {'k': 'attr', 'scope': scope, 'key': self.key_path()}
        name = self.next()[1]
        if name in ('true', 'false'): return {'k': 'lit', 't': 'bool', 'v': name == 'true'}
        if name == 'nil': return {'k': 'lit', 't': 'nil', 'v': None}
        if name in STATUS_NAMES and name != 'name': return {'k': 'lit', 't': 'status', 'v': name}
        if name in KIND_NAMES: return {'k': 'lit', 't': 'kind', 'v': name}
        if name in ('minInt', 'maxInt'):
            return {'k': 'lit', 't': 'number', 'v': '-9223372036854775808' if name == 'minInt' else '9223372036854775807'}
        if self.at(':'):
            self.next(); sub = self.next()[1]; return {'k': 'intrinsic', 'name': f'{name}:{sub}'}
        if name in INTRINSICS: return {'k': 'intrinsic', 'name': name}
        return {'k': 'attr', 'scope': 'unscoped', 'key': name}

    def key_path(self):
        parts = [self.next()[1]]
        while self.at('.') and self.peek(1)[0] == 'ident':
            self.next(); parts.append(self.next()[1])
        return '.'.join(parts)

def parse(src):
    p = P(src.strip())
    q = p.query()
    if p.peek()[0] != 'eof':
        raise ValueError(f'trailing input at {p.peek()!r} in {src!r}')
    return q
