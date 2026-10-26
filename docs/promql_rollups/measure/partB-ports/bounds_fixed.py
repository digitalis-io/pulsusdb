# Port measurement for part B's bounds: d = |a - b| / |b| (|a| when b = 0), p99 by nearest rank.
import sys, bisect, hashlib, math
IV = int(sys.argv[1]) * 1000; DAYS = 14
D0 = 1792713600000
def h(*a): return int.from_bytes(hashlib.blake2b(repr(a).encode(), digest_size=8).digest(), 'little')
def series(k, kind):
    ph = h(k) % IV; n = DAYS * 86400000 // IV
    period = 2 if kind == 'fast' else 500 + k % 300
    stop = D0 + (h(k, 's') % (DAYS * 86400000 - 86400000)) + 43200000
    ts, vs = [], []
    for i in range(n):
        t = D0 + i * IV + ph + (h(k, i) % 201) - 100
        if kind == 'stopped' and t > stop: break
        if kind == 'sparse' and h(k, i, 'x') % 10 < 3: continue
        ts.append(t)
        vs.append(math.sin(i / 50) * 100 if kind == 'gauge' else float(i % period) + (1 if kind == 'fast' else 0))
    return ts, vs
def corr_prefix(vs):
    p = [0.0]
    for i in range(1, len(vs)):
        a, b = vs[i-1], vs[i]; p.append(p[-1] + (b - a if b >= a else b))
    return p
def base(ts, vs, R):
    rows = []; cur = None
    for i, (t, v) in enumerate(zip(ts, vs)):
        te = ((t - 1) // R + 1) * R
        d = (v - vs[i-1] if v >= vs[i-1] else v) if i > 0 and t - ts[i-1] <= 86400000 else 0.0
        if cur is None or cur[0] != te:
            if cur: rows.append(tuple(cur))
            cur = [te, t, v, d, v, 1]
        else: cur[1] = t; cur[2] = v; cur[3] += d; cur[4] += v; cur[5] += 1
    if cur: rows.append(tuple(cur))
    return rows
def up(rows, R):
    out = []; cur = None
    for te, tl, last, inc, sm, ct in rows:
        ce = ((te - 1) // R + 1) * R
        if cur is None or cur[0] != ce:
            if cur: out.append(tuple(cur))
            cur = [ce, tl, last, inc, sm, ct]
        else: cur[1] = tl; cur[2] = last; cur[3] += inc; cur[4] += sm; cur[5] += ct
    if cur: out.append(tuple(cur))
    return out
def extrap(n, t0, tn, v0, res, lo, hi):
    if n < 2: return None
    ds = (t0 - lo) / 1000; de = (hi - tn) / 1000; sampled = (tn - t0) / 1000
    if sampled <= 0: return None
    avg = sampled / (n - 1)
    if res > 0 and v0 >= 0:
        dz = sampled * (v0 / res)
        if dz < ds: ds = dz
    thr = avg * 1.1; ext = sampled
    ext += ds if ds < thr else avg / 2
    ext += de if de < thr else avg / 2
    return res * (ext / sampled) / ((hi - lo) / 1000)
def dist(a, b): return abs(a - b) / abs(b) if b != 0 else abs(a)
stats = {}
for kind in ('sparse',):
    for k in range(40):
        ts, vs = series(k, kind); P = corr_prefix(vs); S = [0.0]
        for v in vs: S.append(S[-1] + v)
        lv = {60000: base(ts, vs, 60000)}
        for a, b in ((60000, 720000), (720000, 7200000), (7200000, 86400000)): lv[b] = up(lv[a], b)
        for R, r in lv.items():
            if kind == 'gauge':
                te = [x[0] for x in r]; rng = 5 * R; T = D0 + rng
                while T <= D0 + DAYS * 86400000:
                    lo = T - rng; a = bisect.bisect_right(ts, lo); b = bisect.bisect_right(ts, T)
                    i0 = bisect.bisect_right(te, lo); i1 = bisect.bisect_right(te, T)
                    if b > a and i1 > i0:
                        raw = (S[b] - S[a]) / (b - a)
                        roll = sum(x[4] / x[5] for x in r[i0:i1]) / (i1 - i0)
                        stats.setdefault((kind, R, 'avg'), []).append(dist(roll, raw))
                    T += R
                continue
            pts = []; c = 0.0
            for j, x in enumerate(r):
                c = x[2] if j == 0 else c + x[3]
                if R >= 720000: pts.append((x[0] - R + 1, c - x[3]))
                pts.append((x[1], c))
            pts.sort(); pt = [p[0] for p in pts]
            rng = 5 * R; T = D0 + rng
            while T <= D0 + DAYS * 86400000:
                lo = T - rng; a = bisect.bisect_right(ts, lo); b = bisect.bisect_right(ts, T)
                raw = extrap(b - a, ts[a], ts[b-1], vs[a], P[b-1] - P[a], lo, T) if b - a >= 2 else None
                q0 = bisect.bisect_right(pt, lo); q1 = bisect.bisect_right(pt, T)
                roll = extrap(q1 - q0, pts[q0][0], pts[q1-1][0], pts[q0][1], pts[q1-1][1] - pts[q0][1], lo, T) if q1 - q0 >= 2 else None
                if raw is not None and roll is not None:
                    stats.setdefault((kind, R, 'rate'), []).append(dist(roll, raw))
                elif (raw is None) != (roll is None):
                    stats.setdefault((kind, R, 'rate_presence_differs'), []).append(1)
                T += R
for key in sorted(stats):
    e = sorted(stats[key]); n = len(e); p99 = e[max(0, math.ceil(0.99 * n) - 1)]
    print(f"scrape {IV//1000}s {key[0]:8} level {key[1]//1000:>6}s {key[2]:22} n={n} p99={p99:.3g} max={e[-1]:.3g}")
