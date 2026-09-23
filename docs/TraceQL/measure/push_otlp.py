#!/usr/bin/env python3
"""POST every OTLP/JSON body in DIR/otlp/, in file-name order, to URL.
Usage: push_otlp.py URL DIR [WORKERS]. URL may be the receiver's base
(http://host:4318) or the full path (http://host:4318/v1/traces); the OTLP
traces path is appended when it is absent. Exits non-zero on any non-2xx."""
import os, sys, urllib.request, concurrent.futures, time
url, d = sys.argv[1], sys.argv[2]
if not url.rstrip('/').endswith('/v1/traces'):
    url = url.rstrip('/') + '/v1/traces' 
workers = int(sys.argv[3]) if len(sys.argv) > 3 else 4
files = sorted(os.listdir(f'{d}/otlp'))
if not files: sys.exit('no bodies')
def post(name):
    body = open(f'{d}/otlp/{name}', 'rb').read()
    for attempt in range(20):
        req = urllib.request.Request(url, data=body, headers={'Content-Type': 'application/json'})
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return r.status, len(body)
        except urllib.error.HTTPError as e:
            if e.code in (429, 503): time.sleep(0.5 * (attempt + 1)); continue
            raise SystemExit(f'{name}: HTTP {e.code} {e.read()[:300]!r}')
    raise SystemExit(f'{name}: gave up')
t0 = time.time(); n = b = 0
with concurrent.futures.ThreadPoolExecutor(workers) as ex:
    for st, ln in ex.map(post, files):
        n += 1; b += ln
print(f'posted {n} bodies, {b} bytes, {time.time() - t0:.1f}s')
