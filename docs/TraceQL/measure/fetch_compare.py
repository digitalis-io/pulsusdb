#!/usr/bin/env python3
"""Trace by id over the shipped routes, PulsusDB against the reference, many
repetitions, reported as a distribution: the reference's fetch time varies
between sessions, so a single median is not enough. The two stores are
interleaved, alternating which goes first, so neither gets the warm side of a
session effect. Every call, the unmeasured first included, must answer 200 with
a body, or the script exits 1 naming the side and the cause.
Usage: fetch_compare.py PULSUS_URL REFERENCE_URL TRACE_ID_HEX REPS  |  fetch_compare.py --self-test"""
import sys, time, statistics, urllib.request, urllib.error

# what the trace datasource requests
ACCEPT = {'Accept': 'application/protobuf'}


def timed(side, url, size):
    """wall time of one GET in ms; records the body length in size[side]"""
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=ACCEPT), timeout=120) as r:
            body = r.read(); status = r.status
    except urllib.error.HTTPError as e:
        body = e.read(); status = e.code
    ms = (time.perf_counter() - t0) * 1000
    if status != 200:
        sys.exit(f'{side}: status {status} from {url}: {body[:200]!r}')
    if not body:
        sys.exit(f'{side}: empty body with status 200 from {url}')
    size[side] = len(body)
    return ms


def main(pulsus, ref, tid, reps):
    p_url = f'{pulsus.rstrip("/")}/api/traces/v1/trace/{tid.lower()}'
    r_url = f'{ref.rstrip("/")}/api/traces/{tid.lower()}'
    size = {}
    tp = lambda: timed('PulsusDB', p_url, size)
    tr = lambda: timed('the reference', r_url, size)
    tp(); tr()                                   # unmeasured
    a, b = [], []
    for i in range(reps):
        if i % 2 == 0: a.append(tp()); b.append(tr())
        else: b.append(tr()); a.append(tp())
    for name, xs in (('PulsusDB', sorted(a)), ('the reference', sorted(b))):
        print(f'{name}\ttrace={tid[:8]}\treps={reps}\tmin={xs[0]:.0f}\tp50={statistics.median(xs):.0f}'
              f'\tp90={xs[int(len(xs)*0.9)-1]:.0f}\tmax={xs[-1]:.0f}\tbytes={size[name]}', flush=True)


def stub(answer):
    """A server on 127.0.0.1, on a port the OS picks; answer(n) gives the status
    and body of its n-th call (from 1)."""
    import http.server, threading
    seen = [0]
    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            seen[0] += 1
            status, body = answer(seen[0])
            self.send_response(status); self.send_header('Content-Length', str(len(body))); self.end_headers()
            self.wfile.write(body)
        def log_message(self, *a): pass
    srv = http.server.ThreadingHTTPServer(('127.0.0.1', 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return f'http://127.0.0.1:{srv.server_address[1]}', srv


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
    fails = 0
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
    print(f'fetch_compare.py self-test: {len(cases) - fails} of {len(cases)} cases as expected')
    return 1 if fails else 0


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-test']:
        sys.exit(self_test())
    main(sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]))
