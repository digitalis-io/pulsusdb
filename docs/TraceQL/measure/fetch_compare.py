#!/usr/bin/env python3
"""Trace by id over the shipped routes, PulsusDB against the reference, many
repetitions, reported as a distribution: the reference's fetch time varies
between sessions, so a single median is not enough. The two stores are
interleaved, alternating which goes first, so neither gets the warm side of a
session effect. Every call, the unmeasured first included, must answer 200 with
a body, or the script exits 1 naming the side and the cause. The side is also
named on a fixed stderr line, `failed side: PulsusDB` or `failed side: the
reference`, and stdout then carries the trace's two lines in their usual order:
the failed side's ends `failed`, the other side's `answered` (every call it was
given answered 200, untimed) or `not called`. A call that ends without a
status (nothing listening, a timeout, a body cut short) is its side's failure
in the same way, and so is a first body that does not decode on the protobuf
wire. The two first bodies must hold the same number of spans, more than 0, or
it exits 1 naming `not like for like`, with nothing on stdout.
Usage: fetch_compare.py PULSUS_URL REFERENCE_URL TRACE_ID_HEX REPS  |  fetch_compare.py --self-test"""
import sys, time, statistics, urllib.request, urllib.error

# what the trace datasource requests
ACCEPT = {'Accept': 'application/protobuf'}


def varint(b, i):
    r = s = 0
    while True:
        c = b[i]; i += 1; r |= (c & 0x7f) << s; s += 7
        if c < 0x80: return r, i


def fields(b):
    """(field, payload) of every length-delimited field of a message;
    other wire types are skipped"""
    i, out = 0, []
    while i < len(b):
        key, i = varint(b, i)
        f, wt = key >> 3, key & 7
        if wt == 2:
            n, i = varint(b, i); out.append((f, b[i:i + n])); i += n
        elif wt == 0:
            _, i = varint(b, i)
        elif wt == 1: i += 8
        elif wt == 5: i += 4
        else: raise ValueError(f'wire type {wt}')
    return out


def span_count(body):
    """the spans of a `TracesData`: field 1 `ResourceSpans`, its field 2
    `ScopeSpans`, their field 2 `Span`; `None` when the body is not one"""
    try:
        return sum(1 for f, rs in fields(body) if f == 1
                   for g, ss in fields(rs) if g == 2
                   for h, _ in fields(ss) if h == 2)
    except (ValueError, IndexError):
        return None


class Failed(Exception):
    """one call of `side` did not answer 200 with a body"""
    def __init__(self, side, message):
        super().__init__(message); self.side = side


def timed(side, url, size, first=None):
    """wall time of one GET in ms; records the body length in size[side],
    and the body in first[side] when `first` is given"""
    t0 = time.perf_counter()
    try:
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=ACCEPT), timeout=120) as r:
                body = r.read(); status = r.status
        except urllib.error.HTTPError as e:
            body = e.read(); status = e.code
    except Exception as e:     # no status: refused, closed, timed out, cut short
        raise Failed(side, f'{side}: {type(e).__name__} from {url}: {e}')
    ms = (time.perf_counter() - t0) * 1000
    if status != 200:
        raise Failed(side, f'{side}: status {status} from {url}: {body[:200]!r}')
    if not body:
        raise Failed(side, f'{side}: empty body with status 200 from {url}')
    size[side] = len(body)
    if first is not None:
        if span_count(body) is None:
            raise Failed(side, f'{side}: a body that is not a trace from {url}: {body[:200]!r}')
        first[side] = body
    return ms


def main(pulsus, ref, tid, reps):
    p_url = f'{pulsus.rstrip("/")}/api/traces/v1/trace/{tid.lower()}'
    r_url = f'{ref.rstrip("/")}/api/traces/{tid.lower()}'
    size, first = {}, {}
    tp = lambda: timed('PulsusDB', p_url, size)
    tr = lambda: timed('the reference', r_url, size)
    a, b = [], []
    try:
        timed('PulsusDB', p_url, size, first); timed('the reference', r_url, size, first)   # unmeasured
        for i in range(reps):
            if i % 2 == 0: a.append(tp()); b.append(tr())
            else: b.append(tr()); a.append(tp())
    except Failed as e:
        # a side that answered a call has a body length in `size`
        for name in ('PulsusDB', 'the reference'):
            state = 'failed' if name == e.side else 'answered' if name in size else 'not called'
            print(f'{name}\ttrace={tid[:8]}\t{state}', flush=True)
        print(f'failed side: {e.side}', file=sys.stderr)
        sys.exit(str(e))
    n = {side: span_count(body) for side, body in first.items()}
    if n['PulsusDB'] != n['the reference'] or not n['PulsusDB']:
        sys.exit(f'not like for like: trace {tid}: PulsusDB answers {n["PulsusDB"]} spans, '
                 f'the reference {n["the reference"]}')
    for name, xs in (('PulsusDB', sorted(a)), ('the reference', sorted(b))):
        print(f'{name}\ttrace={tid[:8]}\treps={reps}\tmin={xs[0]:.0f}\tp50={statistics.median(xs):.0f}'
              f'\tp90={xs[int(len(xs)*0.9)-1]:.0f}\tmax={xs[-1]:.0f}\tbytes={size[name]}', flush=True)


def stub(answer):
    """A server on 127.0.0.1, on a port the OS picks; answer(n) gives the status
    and body of its n-th call (from 1), and optionally a third item: the
    Content-Length to declare, so a longer one ends the read short."""
    import http.server, threading
    seen = [0]
    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            seen[0] += 1
            status, body, *declared = answer(seen[0])
            self.send_response(status)
            self.send_header('Content-Length', str(declared[0] if declared else len(body))); self.end_headers()
            self.wfile.write(body)
        def log_message(self, *a): pass
    srv = http.server.ThreadingHTTPServer(('127.0.0.1', 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return f'http://127.0.0.1:{srv.server_address[1]}', srv


def closed_port():
    """a URL on 127.0.0.1 where nothing listens: a port the OS gave out, closed again"""
    import socket
    s = socket.socket(); s.bind(('127.0.0.1', 0)); port = s.getsockname()[1]; s.close()
    return f'http://127.0.0.1:{port}', None


def traces_data(spans):
    """a `TracesData` holding `spans` empty spans in one resource and scope,
    on the protobuf wire: field 1 `ResourceSpans`, its field 2 `ScopeSpans`,
    its field 2 `Span`"""
    def ld(field, payload):
        assert len(payload) < 128
        return bytes([(field << 3) | 2, len(payload)]) + payload
    return ld(1, ld(2, b''.join(ld(2, b'') for _ in range(spans))))


def self_test():
    import subprocess
    one = traces_data(1)
    good = lambda n: (200, one)
    cases = [
        ('PulsusDB 500 on its third call', lambda n: (500, b'') if n == 3 else (200, one), good,
         1, ['PulsusDB', '500']),
        ('the reference 200 with an empty body on its first call', good, lambda n: (200, b'') if n == 1 else (200, one),
         1, ['the reference', 'empty body']),
        ('a good pair', good, good, 0, []),
        ('two spans against one', lambda n: (200, traces_data(2)), good, 1, ['not like for like']),
        ('no span on either side', lambda n: (200, traces_data(0)), lambda n: (200, traces_data(0)),
         1, ['not like for like']),
    ]
    # a failure names its side on a fixed stderr line, and stdout carries the
    # trace's two slot lines: the failed side `failed`, the other `answered`
    # (every call it was given answered 200) or `not called`
    slots = [
        ('the reference 404 on its first call', good, lambda n: (404, b'') if n == 1 else (200, one),
         'failed side: the reference',
         ['PulsusDB\ttrace=aaaaaaaa\tanswered', 'the reference\ttrace=aaaaaaaa\tfailed']),
        ('PulsusDB 500 on its first call', lambda n: (500, b'') if n == 1 else (200, one), good,
         'failed side: PulsusDB',
         ['PulsusDB\ttrace=aaaaaaaa\tfailed', 'the reference\ttrace=aaaaaaaa\tnot called']),
        ('PulsusDB 500 on its third call', lambda n: (500, b'') if n == 3 else (200, one), good,
         'failed side: PulsusDB',
         ['PulsusDB\ttrace=aaaaaaaa\tfailed', 'the reference\ttrace=aaaaaaaa\tanswered']),
        # a call that ends without a status, and a body that does not decode,
        # are that side's failure as a status is
        ('nothing listening on the reference port', good, 'closed',
         'failed side: the reference',
         ['PulsusDB\ttrace=aaaaaaaa\tanswered', 'the reference\ttrace=aaaaaaaa\tfailed']),
        ('nothing listening on the PulsusDB port', 'closed', good,
         'failed side: PulsusDB',
         ['PulsusDB\ttrace=aaaaaaaa\tfailed', 'the reference\ttrace=aaaaaaaa\tnot called']),
        ('the reference body cut short of its Content-Length on its third call', good,
         lambda n: (200, one, len(one) + 100) if n == 3 else (200, one),
         'failed side: the reference',
         ['PulsusDB\ttrace=aaaaaaaa\tanswered', 'the reference\ttrace=aaaaaaaa\tfailed']),
        ('a PulsusDB body that does not decode', lambda n: (200, b'\xff'), good,
         'failed side: PulsusDB',
         ['PulsusDB\ttrace=aaaaaaaa\tfailed', 'the reference\ttrace=aaaaaaaa\tnot called']),
    ]
    # every malformed form, at each level the count walks (TracesData,
    # ResourceSpans, ScopeSpans), is a body that does not decode, charged to
    # the side that sent it
    def ld(field, payload):
        n, size = len(payload), b''
        while True:
            size += bytes([(n & 0x7f) | (0x80 if n > 0x7f else 0)]); n >>= 7
            if not n: break
        return bytes([(field << 3) | 2]) + size + payload
    malformed = [
        ('a length past the end, in TracesData', b'\x0a\x05\x12'),
        ('a length past the end, in ResourceSpans', ld(1, b'\x12\x05\x12')),
        ('a length past the end, in ScopeSpans', ld(1, ld(2, b'\x12\x05'))),
        ('a varint off the end, in TracesData', b'\x0a\x80'),
        ('a varint off the end, in ScopeSpans', ld(1, ld(2, b'\x08\x80'))),
        ('an 11-byte varint, in TracesData', one + b'\x08' + b'\x80' * 10 + b'\x00'),
        ('an 11-byte varint, in ResourceSpans', ld(1, b'\x08' + b'\x80' * 10 + b'\x00')),
        ('wire type 3, a group start, in TracesData', one + b'\x0b'),
        ('wire type 4, a group end, in ResourceSpans', ld(1, b'\x0c')),
        ('wire type 6, in ScopeSpans', ld(1, ld(2, b'\x0e'))),
        ('wire type 7, in TracesData', one + b'\x0f'),
        ('a fixed64 cut short, trailing in TracesData', one + b'\x09\x00'),
        ('a fixed32 cut short, trailing in ScopeSpans', ld(1, ld(2, ld(2, b'') + b'\x0d\x00'))),
        ('a key cut short, trailing in ResourceSpans', ld(1, ld(2, ld(2, b'')) + b'\x80')),
        ('a length past the end, in TracesData, with no byte after it', b'\x0a\x01'),
    ]
    for i, (form, body) in enumerate(malformed):
        bad = lambda n, body=body: (200, body)
        if i % 2 == 0:
            slots.append((f'{form}, from PulsusDB', bad, good, 'failed side: PulsusDB',
                          ['PulsusDB\ttrace=aaaaaaaa\tfailed', 'the reference\ttrace=aaaaaaaa\tnot called']))
        else:
            slots.append((f'{form}, from the reference', good, bad, 'failed side: the reference',
                          ['PulsusDB\ttrace=aaaaaaaa\tanswered', 'the reference\ttrace=aaaaaaaa\tfailed']))
    fails = 0
    # a well-formed body still counts every span: two resources, three scopes,
    # seven spans with content, and a field of every other wire type at each level
    other = b'\x18\x96\x01' + b'\x21' + bytes(8) + b'\x2d' + bytes(4)
    span = ld(1, bytes(16)) + ld(5, b'GET /cart')
    scope = lambda n: ld(1, ld(1, b'lib')) + other + b''.join(ld(2, span) for _ in range(n)) + ld(3, b'url')
    body = (ld(1, ld(1, b'res-a') + ld(2, scope(2)) + other + ld(2, scope(3)))
            + other + ld(1, ld(2, scope(2)) + ld(3, b'url')))
    ok = span_count(body) == 7 and span_count(traces_data(20)) == 20
    print(f'{"ok  " if ok else "FAIL"} a well-formed nested body: {span_count(body)} spans of 7, '
          f'{span_count(traces_data(20))} of 20')
    fails += not ok
    for name, pa, ra, want, words in cases:
        (p, ps), (r, rs) = stub(pa), stub(ra)
        run = subprocess.run([sys.executable, __file__, p, r, 'aaaaaaaabbbbbbbbccccccccdddddddd', '3'],
                             capture_output=True, text=True, timeout=60)
        ps.shutdown(); rs.shutdown()
        ok = run.returncode == want and all(w in run.stderr for w in words)
        if want == 0:
            lines = run.stdout.splitlines()
            ok = ok and len(lines) == 2 and lines[0].startswith('PulsusDB\t') and lines[1].startswith('the reference\t')
        print(f'{"ok  " if ok else "FAIL"} {name}: exit {run.returncode}, expected {want}; {run.stderr.strip()[-200:]}')
        fails += not ok
    for name, pa, ra, marker, want_lines in slots:
        (p, ps), (r, rs) = (closed_port() if a == 'closed' else stub(a) for a in (pa, ra))
        run = subprocess.run([sys.executable, __file__, p, r, 'aaaaaaaabbbbbbbbccccccccdddddddd', '3'],
                             capture_output=True, text=True, timeout=60)
        for srv in (ps, rs):
            if srv: srv.shutdown()
        ok = (run.returncode == 1 and marker in run.stderr.splitlines()
              and run.stdout.splitlines() == want_lines)
        print(f'{"ok  " if ok else "FAIL"} {name}: exit {run.returncode}; stdout {run.stdout.splitlines()}; '
              f'{run.stderr.strip()[-200:]}')
        fails += not ok
    total = len(cases) + len(slots) + 1
    print(f'fetch_compare.py self-test: {total - fails} of {total} cases as expected')
    return 1 if fails else 0


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-test']:
        sys.exit(self_test())
    main(sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]))
