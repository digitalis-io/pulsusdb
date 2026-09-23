#!/usr/bin/env python3
"""Trace-by-id, both stores, many repetitions, reported as a distribution: the
reference's cost depends on how many blocks it must check, so a single median
is not enough. Usage: fetch_compare.py CH_URL DB TEMPO_URL TRACE_ID_HEX REPS"""
import sys, time, statistics, urllib.request
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
