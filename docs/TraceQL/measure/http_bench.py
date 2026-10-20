#!/usr/bin/env python3
"""Times the benchmark's queries over HTTP against the reference, against
PulsusDB, or against both (`paired`), and (for PulsusDB) reads from
ClickHouse's query_log how many statements each request issued and how many
bytes ClickHouse sent back.

Usage: http_bench.py reference|pulsus BASE_URL START_S END_S REPS [CH_URL]
       http_bench.py paired PULSUS_URL REFERENCE_URL START_S END_S REPS [CH_URL]
       http_bench.py --self-test
Prints TSV: name status bytes ms_min ms_median ms_max [statements rows_read ch_bytes_sent];
paired: name p_status p_bytes p_min p_med p_max r_status r_bytes r_min r_med r_max [p_statements p_rows_read p_ch_bytes_sent]
Each shape runs once unmeasured, then REPS times; paired runs one unmeasured call
per store, then REPS on each, the two stores alternating which goes first. A
status is the first non-200 of any call to the shape, the unmeasured one
included, else 200. b01/b02 fetch the traces BENCH_T20 and BENCH_TBIG when set."""
import os, sys, time, json, urllib.request, urllib.parse, urllib.error, statistics

T20 = os.environ.get('BENCH_T20') or '50fb0cd99260ac2a15d0a6f208126742'
TBIG = os.environ.get('BENCH_TBIG') or '9e0ae95131b5bedbeea2c9eb5234f1ec'


def queries(native, S, E):
    """(name, path, params) for every shape, on PulsusDB's paths or the reference's"""
    SEARCH = '/api/traces/v1/search' if native else '/api/search'
    TAGS = '/api/traces/v1/tags' if native else '/api/v2/search/tags'
    TAGV = (lambda t: f'/api/traces/v1/tag/{t}/values') if native else (lambda t: f'/api/v2/search/tag/{t}/values')
    QR = '/api/traces/v1/metrics/query_range' if native else '/api/metrics/query_range'
    QI = '/api/traces/v1/metrics/query' if native else '/api/metrics/query'
    TR = (lambda i: f'/api/traces/v1/trace/{i}') if native else (lambda i: f'/api/traces/{i}')
    w = {'start': S, 'end': E}
    def s(q): return (SEARCH, dict(w, q=q, limit='20', spss='3'))
    Q = [
     ('s01_empty', *s('{}')),
     ('s02_service', *s('{ resource.service.name = "checkout" }')),
     ('s03_service_error', *s('{ resource.service.name = "payment" && status = error }')),
     ('s04_status_code_ge_500', *s('{ span.http.response.status_code >= 500 }')),
     ('s05_duration_kind', *s('{ duration > 2s && kind = server }')),
     ('s06_db_and_name', *s('{ span.db.system.name = "postgresql" && name = "SELECT shop" }')),
     ('s07_route_regex', *s('{ span.http.route =~ "/api/auth/.*" }')),
     ('s08_user_point', *s('{ span.app.user.id = "u-10013" }')),
     ('s09_descendant', *s('{ resource.service.name = "frontend" } >> { resource.service.name = "payment" && status = error }')),
     ('s10_child', *s('{ resource.service.name = "checkout" } > { resource.service.name = "payment" }')),
     ('s11_event_attr', *s('{ event.exception.type = "java.lang.IllegalStateException" }')),
     ('s12_select', *s('{ status = error } | select(span.http.route, resource.k8s.pod.name)')),
     ('s13_count_gt', *s('{ resource.service.name = "checkout" } | count() > 5')),
     ('s14_trace_level', *s('{ rootServiceName = "loadgen" && traceDuration > 2s }')),
     ('s15_unscoped_and_resource', *s('{ .app.cache.hit = true && resource.k8s.pod.name = "cart-7d9f8b-00002" }')),
     ('t01_tag_names_span', TAGS, dict(w, scope='span')),
     ('t02_tag_values_route', TAGV('span.http.route'), dict(w)),
     ('t03_tag_values_filtered', TAGV('resource.k8s.pod.name'), dict(w, q='{ resource.service.name = "cart" }')),
     ('m01_rate_by_service', QR, dict(w, q='{ } | rate() by (resource.service.name)', step='60s')),
     ('m02_quantiles_by_route', QR, dict(w, q='{ resource.service.name = "frontend" && kind = server } | quantile_over_time(duration, .5, .95, .99) by (span.http.route)', step='60s')),
     ('m03_errors_by_service', QR, dict(w, q='{ status = error } | count_over_time() by (resource.service.name)', step='60s')),
     ('m04_histogram', QR, dict(w, q='{ kind = server } | histogram_over_time(duration)', step='60s')),
     ('m05_instant_avg_by_name', QI, dict(w, q='{ resource.service.name = "frontend" } | avg_over_time(duration) by (name)')),
     ('m06_rate_by_resource_attr', QR, dict(w, q='{ } | rate() by (resource.k8s.pod.name)', step='60s')),
     ('c08_compare', QR, dict(w, q='{ resource.service.name = "payment" } | compare({ status = error })', step='3600s')),
     ('b01_trace_by_id_20', TR(T20), {}),
     ('b02_trace_by_id_1000', TR(TBIG), {}),
    ]
    if native:
        Q.append(('g01_service_graph', '/api/traces/v1/service_graph', dict(w)))
    return Q


def call(base, path, params):
    """(ms, status, body) of one GET"""
    url = base + path + ('?' + urllib.parse.urlencode(params) if params else '')
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers={'Accept': 'application/json'}), timeout=300) as r:
            body = r.read(); status = r.status
    except urllib.error.HTTPError as e:
        body = e.read(); status = e.code
    return (time.perf_counter() - t0) * 1000, status, body


class Side:
    """one store's calls to one shape: the measured times, the last body's
    size, and the first non-200 status with its body"""
    def __init__(self, base, path, params):
        self.base, self.path, self.params = base, path, params
        self.times, self.status, self.size, self.bad_body = [], 200, None, None
    def run(self, measured):
        ms, status, body = call(self.base, self.path, self.params)
        if measured: self.times.append(ms)
        self.size = len(body)
        if status != 200 and self.status == 200:
            self.status, self.bad_body = status, body
    def cells(self):
        t = self.times
        return [str(self.status), str(self.size), f'{min(t):.1f}', f'{statistics.median(t):.1f}', f'{max(t):.1f}']


def chq(ch, sql):
    return urllib.request.urlopen(urllib.request.Request(ch, data=sql.encode()), timeout=120).read().decode()


def ch_cells(ch, t_start, n):
    """statements, rows read and bytes ClickHouse sent, per call, since t_start"""
    chq(ch, 'SYSTEM FLUSH LOGS')
    r = chq(ch, f"SELECT count(), sum(read_rows), sum(ProfileEvents['NetworkSendBytes']) FROM system.query_log "
                f"WHERE type = 'QueryFinish' AND event_time >= toDateTime({int(t_start)}) AND query_id NOT LIKE 'tqd-%' "
                f"AND query NOT ILIKE '%system.query_log%' AND query NOT ILIKE 'SYSTEM FLUSH%' AND current_database != 'system' FORMAT TSV").split('\t')
    return [f'{int(r[0]) / n:.1f}', f'{int(r[1]) / n:.0f}', f'{int(r[2]) / n:.0f}']


def why(side):
    return side.bad_body[:200].decode(errors='replace').replace('\n', ' ') if side.bad_body is not None else ''


def single(base, shapes, reps, ch=None):
    """one store: the first call warms and is not counted"""
    rows = []
    for name, path, params in shapes:
        x = Side(base, path, params)
        t_start = time.time()
        for i in range(reps + 1):
            x.run(measured=i > 0)
        c = x.cells()
        line = [name, c[0], c[1], f'{min(x.times):.0f}', f'{statistics.median(x.times):.0f}', f'{max(x.times):.0f}']
        if ch: line += ch_cells(ch, t_start, reps + 1)
        if x.status != 200: line.append(why(x))
        rows.append(line)
        print('\t'.join(line), flush=True)
    return rows


def paired(p_base, r_base, shapes, reps, ch=None):
    """One row per shape, both stores alternating which goes first.
    shapes: (name, p_path, p_params, r_path or None, r_params)."""
    rows = []
    for name, pp, pq, rp, rq in shapes:
        p = Side(p_base, pp, pq)
        r = Side(r_base, rp, rq) if rp is not None else None
        t_start = time.time()
        p.run(False)
        if r: r.run(False)                          # one unmeasured call per store
        for i in range(reps):
            first, second = (p, r) if i % 2 == 0 else (r, p)
            for x in (first, second):
                if x: x.run(True)
        line = [name] + p.cells() + (r.cells() if r else ['-'] * 5)
        if ch: line += ch_cells(ch, t_start, reps + 1)
        bad = '; '.join(f'{who}: {why(x)}' for who, x in (('PulsusDB', p), ('the reference', r)) if x and x.status != 200)
        if bad: line.append(bad)
        rows.append(line)
        print('\t'.join(line), flush=True)
    return rows


def stub(answer):
    """A server on 127.0.0.1, on a port the OS picks; answer(path, n) gives the
    status and body of the n-th call (from 1) to that path."""
    import http.server, threading
    seen = {}
    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            path = self.path.split('?')[0]
            seen[path] = seen.get(path, 0) + 1
            status, body = answer(path, seen[path])
            self.send_response(status); self.send_header('Content-Length', str(len(body))); self.end_headers()
            self.wfile.write(body)
        def log_message(self, *a): pass
    srv = http.server.ThreadingHTTPServer(('127.0.0.1', 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return f'http://127.0.0.1:{srv.server_address[1]}', srv


def self_test():
    import tempfile
    one = b'{"traces":[{"traceID":"aa"}]}'
    def ours(path, n):
        if path == '/flaky' and n == 1: return 400, b'no'
        if path == '/search': return 200, one
        return 200, b'{}'
    def theirs(path, n):
        if path == '/search': return 200, b'{"traces":[]}'
        return 200, b'{}'
    p, ps = stub(ours)
    r, rs = stub(theirs)
    bodies = tempfile.mkdtemp(prefix='http_bench_bodies.')
    os.environ['BENCH_BODIES'] = bodies
    rows = {row[0]: row for row in paired(p, r, [('flaky', '/flaky', {}, '/flaky', {}),
                                                   ('steady', '/steady', {}, '/steady', {}),
                                                   ('s99_empty_reference', '/search', {}, '/search', {})], 3)}
    del os.environ['BENCH_BODIES']
    fails = 0
    for name, want in (('flaky', '400'), ('steady', '200')):
        got = rows[name][1] if name in rows else None
        ok = got == want
        print(f'{"ok  " if ok else "FAIL"} {name}: p_status {got}, expected {want}')
        fails += not ok
    # a search the reference answers with no trace: its row counts 1 trace
    # for PulsusDB and 0 for the reference, and both first bodies are kept
    row = rows.get('s99_empty_reference', [])
    got = (row[11] if len(row) > 11 else None, row[12] if len(row) > 12 else None)
    ok = got == ('1', '0')
    print(f'{"ok  " if ok else "FAIL"} s99_empty_reference: (p_items, r_items) {got}, expected (\'1\', \'0\')')
    fails += not ok
    kept = {}
    for side in ('pulsus', 'reference'):
        path = os.path.join(bodies, f's99_empty_reference.{side}')
        kept[side] = open(path, 'rb').read() if os.path.exists(path) else None
    ok = kept == {'pulsus': one, 'reference': b'{"traces":[]}'}
    print(f'{"ok  " if ok else "FAIL"} s99_empty_reference: bodies written under BENCH_BODIES {kept}')
    fails += not ok
    import shutil; shutil.rmtree(bodies)
    ps.shutdown(); rs.shutdown()
    print(f'http_bench.py self-test: {4 - fails} of 4 cases as expected')
    return 1 if fails else 0


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-test']:
        sys.exit(self_test())
    flavor = sys.argv[1]
    if flavor == 'paired':
        pb, rb, S, E, reps = sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], int(sys.argv[6])
        ch = sys.argv[7] if len(sys.argv) > 7 else None
        rq = {name: (path, params) for name, path, params in queries(False, S, E)}
        shapes = [(name, path, params) + rq.get(name, (None, None)) for name, path, params in queries(True, S, E)]
        paired(pb, rb, shapes, reps, ch)
    elif flavor in ('reference', 'pulsus'):
        base, S, E, reps = sys.argv[2], sys.argv[3], sys.argv[4], int(sys.argv[5])
        ch = sys.argv[6] if len(sys.argv) > 6 else None
        single(base, queries(flavor == 'pulsus', S, E), reps, ch)
    else:
        sys.exit(__doc__)
