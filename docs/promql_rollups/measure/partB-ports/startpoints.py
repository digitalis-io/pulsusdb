# Counter rate from level rows: one start point per window, at the window's first row, against one start point per row,
# on normal, stopped and sparse counters. Window (lo, T], range 5 x res, step res.
import sys, bisect, hashlib
IV = int(sys.argv[1]) * 1000; DAYS = int(sys.argv[2])
D0 = 1792713600000
LEVELS = [60000, 720000, 7200000]
def h(*a): return int.from_bytes(hashlib.blake2b(repr(a).encode(), digest_size=8).digest(), 'little')
def series(k, kind):
    ph = h(k) % IV; n = DAYS * 86400000 // IV; period = 500 + k % 300
    stop = D0 + (h(k, 's') % (DAYS * 86400000 - 86400000)) + 43200000
    ts, vs = [], []
    for i in range(n):
        t = D0 + i * IV + ph + (h(k, i) % 201) - 100
        if kind == 'stopped' and t > stop: break
        if kind == 'sparse' and h(k, i, 'x') % 10 < 3: continue
        ts.append(t); vs.append(float(i % period))
    return ts, vs, (stop if kind == 'stopped' else None)
def corr_prefix(vs):
    p = [0.0]
    for i in range(1, len(vs)):
        a, b = vs[i-1], vs[i]; p.append(p[-1] + (b - a if b >= a else b))
    return p
def base(ts, vs, R):
    rows = []; cur = None
    for i, (t, v) in enumerate(zip(ts, vs)):
        te = ((t - 1) // R + 1) * R
        d = (v - vs[i-1] if v >= vs[i-1] else v) if i > 0 and t - ts[i-1] <= 120000 + R else 0.0
        if cur is None or cur[0] != te:
            if cur: rows.append(tuple(cur))
            cur = [te, t, v, d]
        else: cur[1] = t; cur[2] = v; cur[3] += d
    if cur: rows.append(tuple(cur))
    return rows
def up(rows, R):
    out = []; cur = None
    for te, tl, last, inc in rows:
        ce = ((te - 1) // R + 1) * R
        if cur is None or cur[0] != ce:
            if cur: out.append(tuple(cur))
            cur = [ce, tl, last, inc]
        else: cur[1] = tl; cur[2] = last; cur[3] += inc
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
stats = {}
for kind in ('normal', 'stopped', 'sparse'):
    for k in range(60):
        ts, vs, stop = series(k, kind); P = corr_prefix(vs)
        lv = {60000: base(ts, vs, 60000)}
        lv[720000] = up(lv[60000], 720000); lv[7200000] = up(lv[720000], 7200000)
        for R in (720000, 7200000):
            r = lv[R]; tl = [x[1] for x in r]; c = []; cc = 0.0
            for j, x in enumerate(r):
                cc = x[2] if j == 0 else cc + x[3]; c.append(cc)
            # per-row start points: (t_end - R + 1, c - inc) and (t_last, c)
            pts = []
            for j, x in enumerate(r):
                if j > 0: pts.append((x[0] - R + 1, c[j] - x[3]))
                else: pts.append((x[0] - R + 1, c[j] - x[3]))
                pts.append((x[1], c[j]))
            pts.sort(); pt = [p[0] for p in pts]
            rng = 5 * R; T = D0 + rng
            while T <= D0 + DAYS * 86400000:
                lo = T - rng
                a = bisect.bisect_right(ts, lo); b = bisect.bisect_right(ts, T)
                raw = extrap(b - a, ts[a], ts[b-1], vs[a], P[b-1] - P[a], lo, T) if b - a >= 2 else None
                i0 = bisect.bisect_right(tl, lo); i1 = bisect.bisect_right(tl, T)
                if raw and i1 - i0 >= 1:
                    # per-window rule: one start point at max(bucket start, lo) + 1 for the window's first row
                    st = max(lo, r[i0][0] - R) + 1
                    per_window = extrap(i1 - i0 + 1, st, tl[i1-1], c[i0] - r[i0][3], c[i1-1] - c[i0] + r[i0][3], lo, T)
                    q0 = bisect.bisect_right(pt, lo); q1 = bisect.bisect_right(pt, T)
                    per_row = extrap(q1 - q0, pts[q0][0], pts[q1-1][0], pts[q0][1], pts[q1-1][1] - pts[q0][1], lo, T) if q1 - q0 >= 2 else None
                    for name, x in (('per_window', per_window), ('per_row', per_row)):
                        if x is None: continue
                        stats.setdefault((kind, R, name), []).append(abs(x - raw) / abs(raw))
                T += R
for key in sorted(stats):
    e = sorted(stats[key]); n = len(e)
    print(f"scrape {IV//1000}s {key[0]:8} level {key[1]//1000}s {key[2]:10} n={n} median={e[n//2]:.3g} p99={e[min(n-1, int(n*0.99))]:.3g} max={e[-1]:.3g}")
