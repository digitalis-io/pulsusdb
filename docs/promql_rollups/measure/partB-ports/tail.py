# rate[5m] at step 60 s on a 60 s level whose watermark W is k minutes before the window end;
# the raw tail after W as read (every sample) or thinned to its last sample per 60 s bucket.
import sys, bisect, hashlib, math
IV = int(sys.argv[1]) * 1000; DAYS = 2
D0 = 1792713600000
def h(*a): return int.from_bytes(hashlib.blake2b(repr(a).encode(), digest_size=8).digest(), 'little')
def series(k, kind):
    ph = h(k) % IV; n = DAYS * 86400000 // IV
    period = 2 if kind == 'fast' else 500 + k % 300
    ts, vs = [], []
    for i in range(n):
        t = D0 + i * IV + ph + (h(k, i) % 201) - 100
        if kind == 'sparse' and h(k, i, 'x') % 10 < 3: continue
        ts.append(t); vs.append(float(i % period) + (1 if kind == 'fast' else 0))
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
R = 60000; RNG = 300000
stats = {}
for kind in ('normal', 'fast', 'sparse'):
    for k in range(30):
        ts, vs = series(k, kind)
        # 60 s rows: (t_end, t_last, last, inc) with inc from the previous sample (no age limit here)
        rows = []; cur = None
        for i, (t, v) in enumerate(zip(ts, vs)):
            te = ((t - 1) // R + 1) * R; d = corr(vs[i-1], v) if i else 0.0
            if cur is None or cur[0] != te:
                if cur: rows.append(cur)
                cur = [te, t, v, d]
            else: cur[1] = t; cur[2] = v; cur[3] += d
        rows.append(cur)
        te_list = [r[0] for r in rows]
        # raw counter (reset-free) values, for the routed tail
        T = D0 + 2 * 3600000
        while T <= D0 + DAYS * 86400000 - 3600000:
            lo = T - RNG
            a = bisect.bisect_right(ts, lo); b = bisect.bisect_right(ts, T)
            if b - a >= 2:
                P = 0.0
                for i in range(a + 1, b): P += corr(vs[i-1], vs[i])
                raw = extrap(b - a, ts[a], ts[b-1], vs[a], P, lo, T)
                for kmin in (0, 1, 2, 3, 4, 5):
                    W = T - kmin * 60000
                    j = bisect.bisect_right(te_list, W)
                    # level points up to W: c from the window's start region
                    j0 = bisect.bisect_right(te_list, lo - RNG)
                    pts = []; c = None
                    for r in rows[j0:j]:
                        c = r[2] if c is None else c + r[3]
                        pts.append((r[1], c))
                    i0 = bisect.bisect_right(ts, W)
                    p = rows[j-1][2] if j > 0 else None
                    full = list(pts); thin = list(pts); cc = c
                    lastb = None
                    for i in range(i0, b):
                        cc = (vs[i] if cc is None else cc + corr(p, vs[i])) if p is not None or cc is None else cc
                        p = vs[i]
                        full.append((ts[i], cc))
                        bk = ((ts[i] - 1) // R + 1) * R
                        if thin and lastb == bk: thin[-1] = (ts[i], cc)
                        else: thin.append((ts[i], cc))
                        lastb = bk
                    for name, ps in (('as read', full), ('thinned', thin)):
                        x = rate_pts(ps, lo, T)
                        if x is not None and raw:
                            stats.setdefault((kind, kmin, name), []).append(abs(x - raw) / abs(raw))
            T += 60000
for key in sorted(stats):
    e = sorted(stats[key]); n = len(e); p99 = e[max(0, math.ceil(0.99 * n) - 1)]
    print(f"scrape {IV//1000}s {key[0]:7} window ends W+{key[1]}m {key[2]:8} n={n} p99={p99:.4g} max={e[-1]:.3g}")
