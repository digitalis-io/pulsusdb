# rate[1h] at step 12 m on a 720 s route (a start point per row), the 720 s watermark W k steps before the
# window end, raw after W folded into the counter as read, not thinned.
import sys, bisect, hashlib, math
IV = 15000; DAYS = 2; D0 = 1792713600000
def h(*a): return int.from_bytes(hashlib.blake2b(repr(a).encode(), digest_size=8).digest(), 'little')
def series(k, kind):
    ph = h(k) % IV; n = DAYS * 86400000 // IV; period = 500 + k % 300
    ts, vs = [], []
    for i in range(n):
        t = D0 + i * IV + ph + (h(k, i) % 201) - 100
        if kind == 'stop' and t > D0 + 86400000 + 450000 + h(k, 's') % 3600000: break
        ts.append(t); vs.append(float(i % period))
    return ts, vs
def corr(a, b): return b - a if b >= a else b
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
def rate_pts(pts, lo, hi):
    w = [p for p in pts if lo < p[0] <= hi]
    if len(w) < 2: return None
    return extrap(len(w), w[0][0], w[-1][0], w[0][1], w[-1][1] - w[0][1], lo, hi)
R = 720000; RNG = 3600000; STEP = 720000
stats = {}
for kind in ('normal', 'stop'):
    for k in range(30):
        ts, vs = series(k, kind)
        rows = []; cur = None
        for i, (t, v) in enumerate(zip(ts, vs)):
            te = ((t - 1) // R + 1) * R; d = corr(vs[i-1], v) if i else 0.0
            if cur is None or cur[0] != te:
                if cur: rows.append(cur)
                cur = [te, t, v, d]
            else: cur[1] = t; cur[2] = v; cur[3] += d
        rows.append(cur); te_list = [r[0] for r in rows]
        T = D0 + 86400000 - 7200000 if kind == 'stop' else D0 + 7200000
        TE = D0 + 86400000 + 7200000 if kind == 'stop' else D0 + DAYS * 86400000 - 7200000
        while T <= TE:
            lo = T - RNG
            a = bisect.bisect_right(ts, lo); b = bisect.bisect_right(ts, T)
            if b - a >= 2:
                P = sum(corr(vs[i-1], vs[i]) for i in range(a + 1, b))
                raw = extrap(b - a, ts[a], ts[b-1], vs[a], P, lo, T)
                for ks in range(0, 6):
                    W = T - ks * STEP
                    j = bisect.bisect_right(te_list, W); j0 = bisect.bisect_right(te_list, lo - RNG)
                    pts = []; c = None
                    for r in rows[j0:j]:
                        if c is None: c = r[2]; pts.append((r[0] - R + 1, c - r[3]))
                        else: c += r[3]; pts.append((r[0] - R + 1, c - r[3]))
                        pts.append((r[1], c))
                    p = rows[j-1][2] if j > 0 else None
                    for i in range(bisect.bisect_right(ts, W), b):
                        c = vs[i] if c is None else c + corr(p, vs[i]); p = vs[i]; pts.append((ts[i], c))
                    x = rate_pts(pts, lo, T)
                    if raw:
                        d = 1.0 if x is None else abs(x - raw) / abs(raw)
                        stats.setdefault((kind, ks), []).append(d)
            T += STEP
for key in sorted(stats):
    e = sorted(stats[key]); n = len(e); p99 = e[max(0, math.ceil(0.99 * n) - 1)]
    print(f"720s route {key[0]:7} watermark {key[1]} steps before the end n={n} p99={p99:.4g} max={e[-1]:.3g}")
