# Counter rate error per level: last-only pseudo-samples (the rejected rule) against the reset-corrected inc column (the chosen rule).
# usage: counter_distance.py SCRAPE_SECONDS DAYS
import sys, bisect, hashlib, statistics
IV = int(sys.argv[1]) * 1000            # scrape interval, ms
DAYS = int(sys.argv[2])
D0 = 1792713600000
LEVELS = [5000, 60000, 720000, 7200000, 86400000]
RANGES = {5000: 60000, 60000: 300000, 720000: 3600000, 7200000: 36000000, 86400000: 5 * 86400000}
EVAL_DAYS = {5000: 1, 60000: 1, 720000: DAYS, 7200000: DAYS, 86400000: DAYS}

def h(*a):
    return int.from_bytes(hashlib.blake2b(repr(a).encode(), digest_size=8).digest(), 'little')

def series(k):
    ph = h(k) % IV
    n = DAYS * 86400000 // IV
    period = 2 if k >= 100 else 500 + k % 300
    ts = [D0 + i * IV + ph + (h(k, i) % 201) - 100 for i in range(n)]
    vs = [float(i % period) + (1 if period == 2 else 0) for i in range(n)]
    return ts, vs

def corrected_prefix(vs):
    p = [0.0]
    for i in range(1, len(vs)):
        a, b = vs[i - 1], vs[i]
        p.append(p[-1] + (b - a if b >= a else b))
    return p

def base_level(ts, vs, R):
    # one row per bucket: t_end, t_last, last, inc (reset-corrected, from the previous sample if it is within 120 s)
    rows = []; cur = None
    for i, (t, v) in enumerate(zip(ts, vs)):
        te = ((t - 1) // R + 1) * R
        if i > 0 and t - ts[i - 1] <= 120000 + R:
            a = vs[i - 1]; d = v - a if v >= a else v
        else:
            d = 0.0
        if cur is None or cur[0] != te:
            if cur: rows.append(tuple(cur))
            cur = [te, t, v, d]
        else:
            cur[1] = t; cur[2] = v; cur[3] += d
    rows.append(tuple(cur))
    return rows

def up(rows, R):
    out = []; cur = None
    for te, tl, last, inc in rows:
        ce = ((te - 1) // R + 1) * R
        if cur is None or cur[0] != ce:
            if cur: out.append(tuple(cur))
            cur = [ce, tl, last, inc]
        else:
            cur[1] = tl; cur[2] = last; cur[3] += inc
    out.append(tuple(cur))
    return out

def extrap(n, t0, tn, v0, res, lo, hi):
    # the reference's extrapolated rate, given the corrected increase res
    if n < 2: return None
    dstart = (t0 - lo) / 1000; dend = (hi - tn) / 1000
    sampled = (tn - t0) / 1000; avg = sampled / (n - 1)
    if res > 0 and v0 >= 0:
        dz = sampled * (v0 / res)
        if dz < dstart: dstart = dz
    thr = avg * 1.1; ext = sampled
    ext += dstart if dstart < thr else avg / 2
    ext += dend if dend < thr else avg / 2
    return res * (ext / sampled) / ((hi - lo) / 1000)

def rate_raw(ts, vs, P, lo, hi):
    a = bisect.bisect_right(ts, lo); b = bisect.bisect_right(ts, hi)
    if b - a < 2: return None
    return extrap(b - a, ts[a], ts[b - 1], vs[a], P[b - 1] - P[a], lo, hi)

stats = {}
for k in list(range(0, 100)) + list(range(100, 120)):
    ts, vs = series(k); P = corrected_prefix(vs)
    rows = base_level(ts, vs, LEVELS[0]); lv = {LEVELS[0]: rows}
    for i in range(1, len(LEVELS)):
        lv[LEVELS[i]] = up(lv[LEVELS[i - 1]], LEVELS[i])
    kind = 'fast' if k >= 100 else 'normal'
    for R in LEVELS:
        r = lv[R]; tl = [x[1] for x in r]; lasts = [x[2] for x in r]
        cum = []; c = 0.0
        for j, x in enumerate(r):
            c = x[2] if j == 0 else c + x[3]
            cum.append(c)
        Pl = corrected_prefix(lasts)
        rng = RANGES[R]; T = D0 + rng; end = D0 + EVAL_DAYS[R] * 86400000
        while T <= end:
            lo = T - rng
            x = rate_raw(ts, vs, P, lo, T)
            a = bisect.bisect_right(tl, lo); b = bisect.bisect_right(tl, T)
            if x is not None and x != 0 and b - a >= 2:
                old = extrap(b - a, tl[a], tl[b - 1], lasts[a], Pl[b - 1] - Pl[a], lo, T)
                # inc of row a covers (previous row's last, row a's last], which lies partly before the window: start from row a's value
                new = extrap(b - a, tl[a], tl[b - 1], lasts[a], cum[b - 1] - cum[a], lo, T)
                # the chosen read rule at 720 s and coarser: the first row also gives a start point at the bucket's start, valued before its inc
                start = max(lo + 1, r[a][0] - R + 1)
                new2 = extrap(b - a + 1, start, tl[b - 1], cum[a] - r[a][3], cum[b - 1] - cum[a] + r[a][3], lo, T)
                key = (R, kind)
                s = stats.setdefault(key, {'old': [], 'new': [], 'new2': []})
                s['old'].append(abs(old - x) / abs(x)); s['new'].append(abs(new - x) / abs(x)); s['new2'].append(abs(new2 - x) / abs(x))
            T += R
for (R, kind), s in sorted(stats.items()):
    for m in ('old', 'new', 'new2'):
        e = sorted(s[m]); n = len(e)
        print(f"scrape {IV // 1000}s level {R // 1000}s range {RANGES[R] // 1000}s {kind:6} { {'old': 'last only', 'new': 'inc', 'new2': 'inc+start'}[m]:9} n={n} exact={sum(1 for v in e if v == 0)} median={e[n // 2]:.3g} p99={e[min(n - 1, int(n * 0.99))]:.3g} max={e[-1]:.3g}")
