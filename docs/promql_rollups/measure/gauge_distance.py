# Distance from raw of functions answered from 60 s and 720 s level rows.
# usage: gauge_distance.py INPUT_DIR SCRAPE_SECONDS (reads raw<S>.tsv, lv60_<S>.tsv, lv720_<S>.tsv, kinds.tsv)
import sys, math, bisect, statistics
from collections import defaultdict
D0 = 1792713600000
DAY = 86400000
base = sys.argv[1]

def load_raw(p):
    d = defaultdict(lambda: ([], []))
    for line in open(p):
        f, t, v = line.split('\t'); d[f][0].append(int(t)); d[f][1].append(float(v))
    return d

def load_lv(p):
    d = defaultdict(list)
    for line in open(p):
        f, te, tl, last, mn, mx, sm, ct = line.split('\t')
        d[f].append((int(te), int(tl), float(last), float(mn), float(mx), float(sm), int(ct)))
    return d

def extrap(ts, vs, lo, hi, counter, is_rate):
    # the reference's extrapolated rate/increase/delta (window (lo, hi], ms)
    n = len(ts)
    if n < 2: return None
    res = vs[-1] - vs[0]
    if counter:
        for a, b in zip(vs, vs[1:]):
            if b < a: res += a
    dstart = (ts[0] - lo) / 1000; dend = (hi - ts[-1]) / 1000
    sampled = (ts[-1] - ts[0]) / 1000; avg = sampled / (n - 1)
    if counter and res > 0 and vs[0] >= 0:
        dz = sampled * (vs[0] / res)
        if dz < dstart: dstart = dz
    thr = avg * 1.1; ext = sampled
    ext += dstart if dstart < thr else avg / 2
    ext += dend if dend < thr else avg / 2
    f = ext / sampled
    if is_rate: f /= (hi - lo) / 1000
    return res * f

def irate(ts, vs):
    if len(ts) < 2: return None
    a, b = vs[-2], vs[-1]
    d = b - a if b >= a else b
    return d / ((ts[-1] - ts[-2]) / 1000)

def window(ts, lo, hi):
    return bisect.bisect_right(ts, lo), bisect.bisect_right(ts, hi)

raw = load_raw(f'{base}/raw{sys.argv[2]}.tsv')
kinds = dict(l.strip().split('\t') for l in open(f'{base}/kinds.tsv'))
out = defaultdict(list); examples = {}
for lvl, rng in ((60, 300000), (60, 3600000), (720, 3600000)):
    lv = load_lv(f'{base}/lv{lvl}_{sys.argv[2]}.tsv')
    step = lvl * 1000
    for fp, (ts, vs) in raw.items():
        k = kinds[fp]; rows = lv[fp]
        te = [r[0] for r in rows]; tl = [r[1] for r in rows]
        order = sorted(range(len(rows)), key=lambda i: tl[i]); tls = [tl[i] for i in order]
        T = D0 + rng
        while T <= D0 + DAY:
            lo = T - rng
            a, b = window(ts, lo, T); rts, rvs = ts[a:b], vs[a:b]
            ra, rb = window(te, lo, T); agg = [r for r in rows[ra:rb] if r[6] > 0]
            la, lb = window(tls, lo, T); lp = [rows[i] for i in order[la:lb]]
            pts = [r[1] for r in lp]; pvs = [r[2] for r in lp]
            pairs = []
            if k.startswith('c'):
                pairs.append(('rate', extrap(rts, rvs, lo, T, True, True), extrap(pts, pvs, lo, T, True, True)))
                pairs.append(('irate', irate(rts, rvs), irate(pts, pvs)))
                pairs.append(('resets', sum(1 for x, y in zip(rvs, rvs[1:]) if y < x), sum(1 for x, y in zip(pvs, pvs[1:]) if y < x)))
            else:
                g = [v for v in rvs if not math.isnan(v)]
                if g and agg:
                    pairs.append(('max_over_time', max(g), max(r[4] for r in agg)))
                    pairs.append(('min_over_time', min(g), min(r[3] for r in agg)))
                    pairs.append(('sum_over_time', sum(g), sum(r[5] for r in agg)))
                    pairs.append(('avg_over_time', sum(g) / len(g), sum(r[5] / r[6] for r in agg) / len(agg)))
                    pairs.append(('delta', extrap(rts, rvs, lo, T, False, False), extrap(pts, pvs, lo, T, False, False)))
            if rvs and pvs:
                pairs.append(('last_over_time', rvs[-1], pvs[-1]))
            for name, x, y in pairs:
                if x is None or y is None: continue
                key = (lvl, rng // 1000, k if name in ('rate', 'irate', 'resets') else 'g', name)
                e = abs(y - x) / abs(x) if x != 0 else (0.0 if y == 0 else float('inf'))
                out[key].append(e)
                if key not in examples or e > examples[key][0]:
                    examples[key] = (e, fp, T, x, y)
            T += step
for key in sorted(out):
    es = sorted(out[key]); n = len(es)
    ex = examples[key]
    print(f"{key[0]}s level, range {key[1]}s, {key[2]}, {key[3]}: n={n} exact={sum(1 for e in es if e == 0)} median={es[n//2]:.3g} p99={es[int(n*0.99)]:.3g} max={es[-1]:.3g} | worst fp={ex[1][:12]} T={ex[2]} raw={ex[3]:.6g} rollup={ex[4]:.6g}")
