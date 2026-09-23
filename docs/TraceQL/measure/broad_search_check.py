#!/usr/bin/env python3
"""What the reference's broad search actually returns: whether its twenty traces
are the twenty newest, and where they sit in the recency order the API contract
requires. Usage: broad_search_check.py CH_URL DB TEMPO_URL START_S END_S"""
import json, sys, urllib.request, urllib.parse
ch, DB, tempo, S, E = sys.argv[1].rstrip('/'), sys.argv[2], sys.argv[3].rstrip('/'), int(sys.argv[4]), int(sys.argv[5])
u = tempo + '/api/search?' + urllib.parse.urlencode({'q': '{}', 'start': S, 'end': E, 'limit': 20})
ref = {t['traceID'].rjust(32, '0').upper() for t in json.load(urllib.request.urlopen(u, timeout=300))['traces']}
def q(sql):
    return urllib.request.urlopen(urllib.request.Request(ch + '/', data=sql.encode()), timeout=300).read().decode()
newest = set(q(f"SELECT hex(trace_id) FROM (SELECT trace_id, max(start_ns) m FROM {DB}.spans "
               f"GROUP BY trace_id ORDER BY m DESC, trace_id ASC LIMIT 20) FORMAT TSV").split())
ids = "','".join(sorted(ref))
n, lo, hi = q(f"SELECT count(), min(rank), max(rank) FROM (SELECT hex(trace_id) h, row_number() OVER "
              f"(ORDER BY max(start_ns) DESC, trace_id ASC) rank FROM {DB}.spans GROUP BY trace_id) "
              f"WHERE h IN ('{ids}') FORMAT TSV").split()
print(f'reference traces returned\t{len(ref)}')
print(f'of the newest twenty\t{len(ref & newest)}')
print(f'their recency ranks\t{lo}..{hi}')
