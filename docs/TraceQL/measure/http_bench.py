#!/usr/bin/env python3
"""Times the benchmark's queries over HTTP against the reference or against
PulsusDB, and (for PulsusDB) reads from ClickHouse's query_log how many
statements each request issued and how many bytes ClickHouse sent back.
Usage: http_bench.py reference|pulsus BASE_URL START_S END_S REPS [CH_URL]
Prints TSV: name status bytes ms_min ms_median ms_max [statements rows_read ch_bytes_sent]"""
import sys, time, json, urllib.request, urllib.parse, statistics
flavor, base, S, E, reps = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], int(sys.argv[5])
ch = sys.argv[6] if len(sys.argv) > 6 else None
native = flavor == 'pulsus'
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
 ('b01_trace_by_id_20', TR('50fb0cd99260ac2a15d0a6f208126742'), {}),
 ('b02_trace_by_id_1000', TR('9e0ae95131b5bedbeea2c9eb5234f1ec'), {}),
]
if native:
    Q.append(('g01_service_graph', '/api/traces/v1/service_graph', dict(w)))
def chq(sql):
    return urllib.request.urlopen(urllib.request.Request(ch, data=sql.encode()), timeout=120).read().decode()
for name, path, params in Q:
    url = base + path + ('?' + urllib.parse.urlencode(params) if params else '')
    times = []; status = size = None
    t_start = time.time()
    for i in range(reps + 1):                  # the first run warms and is not counted
        t0 = time.perf_counter()
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={'Accept': 'application/json'}), timeout=300) as r:
                body = r.read(); status = r.status
        except urllib.error.HTTPError as e:
            body = e.read(); status = e.code
        ms = (time.perf_counter() - t0) * 1000
        if i: times.append(ms)
        size = len(body)
    line = [name, str(status), str(size), f'{min(times):.0f}', f'{statistics.median(times):.0f}', f'{max(times):.0f}']
    if ch:
        chq('SYSTEM FLUSH LOGS')
        r = chq(f"SELECT count(), sum(read_rows), sum(ProfileEvents['NetworkSendBytes']) FROM system.query_log "
                f"WHERE type = 'QueryFinish' AND event_time >= toDateTime({int(t_start)}) AND query_id NOT LIKE 'tqd-%' "
                f"AND query NOT ILIKE '%system.query_log%' AND query NOT ILIKE 'SYSTEM FLUSH%' AND current_database != 'system' FORMAT TSV").split('\t')
        n = reps + 1
        line += [f'{int(r[0]) / n:.1f}', f'{int(r[1]) / n:.0f}', f'{int(r[2]) / n:.0f}']
    if status != 200: line.append(body[:200].decode(errors='replace').replace('\n', ' '))
    print('\t'.join(line), flush=True)
