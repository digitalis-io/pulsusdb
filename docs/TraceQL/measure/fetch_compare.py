#!/usr/bin/env python3
"""Trace-by-id, both stores, many repetitions, reported as a distribution: the
reference's cost depends on how many blocks it must check, so a single median
is not enough. Usage: fetch_compare.py CH_URL DB TEMPO_URL TRACE_ID_HEX REPS"""
import sys, time, statistics, urllib.request
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


def self_test():
    import subprocess
    good = lambda n: (200, b'trace')
    cases = [
        ('PulsusDB 500 on its third call', lambda n: (500, b'') if n == 3 else (200, b'trace'), good,
         1, ['PulsusDB', '500']),
        ('the reference 200 with an empty body on its first call', good, lambda n: (200, b'') if n == 1 else (200, b'trace'),
         1, ['the reference', 'empty body']),
        ('a good pair', good, good, 0, []),
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
    print(f'fetch_compare.py self-test: {3 - fails} of 3 cases as expected')
    return 1 if fails else 0


if sys.argv[1:] == ['--self-test']:
    sys.exit(self_test())
ch, DB, tempo, tid, reps = sys.argv[1].rstrip('/'), sys.argv[2], sys.argv[3].rstrip('/'), sys.argv[4], int(sys.argv[5])
sql = open(f'{sys.path[0]}/sql/b01_trace_by_id_20.sql').read()
sql = sql.replace('50FB0CD99260AC2A15D0A6F208126742', tid.upper()).replace('tqd_g1', DB)
def t_ch():
    t0 = time.perf_counter()
    urllib.request.urlopen(urllib.request.Request(f'{ch}/?default_format=RowBinary&final=1', data=sql.encode()), timeout=120).read()
    return (time.perf_counter() - t0) * 1000
def t_ref():
    t0 = time.perf_counter()
    urllib.request.urlopen(f'{tempo}/api/traces/{tid.lower()}', timeout=120).read()
    return (time.perf_counter() - t0) * 1000
# interleaved, alternating which store goes first, so neither gets the warm
# side of a session effect
t_ch(); t_ref()
a, b = [], []
for i in range(reps):
    if i % 2 == 0: a.append(t_ch()); b.append(t_ref())
    else: b.append(t_ref()); a.append(t_ch())
for name, xs in (('this design', sorted(a)), ('the reference', sorted(b))):
    print(f'{name}\ttrace={tid[:8]}\treps={reps}\tmin={xs[0]:.0f}\tp50={statistics.median(xs):.0f}\tp90={xs[int(len(xs)*0.9)-1]:.0f}\tmax={xs[-1]:.0f}')
