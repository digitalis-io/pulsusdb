-- case: by_nested_parent
-- q: {} | rate() by(nestedSetParent)

== range ==
SELECT g, o, points, exemplars, 1 AS f
FROM (SELECT g, o, arraySort(groupArrayIf(((29856666 + p) * 60000, v), NOT none)) AS points,
             arraySort(e -> e.2, groupArrayIf((ex.1, ex.2, v), has AND (T <= 100 OR (intDiv(x * 100 + T - 1, T) < 100 AND intDiv(intDiv(x * 100 + T - 1, T) * T, 100) = x)))) AS exemplars
      FROM (SELECT none, g, o, p, v, ex, ex.2 != 0 AS has,
       row_number() OVER (PARTITION BY has ORDER BY p, g, o) - 1 AS x,
       countIf(has) OVER () AS T
FROM (SELECT g, o, empty(cells) AS none, c.1 AS p, c.2 AS v, if(isNaN(v), CAST((toFixedString('', 16), 0, 0), 'Tuple(FixedString(16), Int64, Int64)'), c.3) AS ex
FROM (SELECT g, o, groupArray((p, v, ex)) AS cells, throwIf(count() OVER () > 1000, 'metrics series cap') AS over
FROM (SELECT arrayMap(x -> if(x.2 IN ('n', 'N'), ('', x.2), x), [(toString(nb.np), 'i')]) AS g, toUInt64(0) AS o, intDiv(start_ns - 1, 60000000000) + 1 - 29856666 AS p, toFloat64(count()) / 60 AS v, argMax((trace_id, start_ns, duration_ns), (duration_ns, span_id)) AS ex
FROM spans
LEFT JOIN (WITH (SELECT groupArray((k, trace_id)) FROM (SELECT trace_id, arrayJoin(bk) AS k FROM (SELECT trace_id, groupUniqArrayArray(buckets) AS bk FROM traces WHERE day >= toDate(fromUnixTimestamp64Nano(1791313500000000001), 'UTC') AND day <= toDate(fromUnixTimestamp64Nano(1791497220000000000), 'UTC') GROUP BY trace_id HAVING max(last_start_ns) >= 1791399900000000001 AND min(start_ns) <= 1791410820000000000))) AS nested_keys SELECT trace_id, sp.1 AS span_id, nl, nr, np FROM (SELECT trace_id, s, n, lft, rgt,
       arrayMap((l, p) -> if(l = 0, 0, p), lft,
                arrayMap(x -> x.2, arraySort(x -> x.1, arrayFilter(x -> x.1 > 0,
                  arrayMap((a, b) -> if(a.2 = 1, (a.3, b.4), (0, 0)), pl, arrayFill(z -> z.2 = 0, pl)))))) AS par
FROM (SELECT trace_id, s, n, pidx, lft, rgt,
             arraySort(arrayConcat([(toUInt32(0), 0, toUInt32(0), toInt64(-1))],
                                   arrayMap((v, j) -> (j, 0, j, v), lft, arrayEnumerate(lft)),
                                   arrayMap((k, j) -> (k, 1, j, toInt64(0)), pidx, arrayEnumerate(pidx)))) AS pl
      FROM (SELECT trace_id, s, n, pidx,
                   arraySlice(tour, 1, n) AS enter,
                   toInt64(2 * arrayCount(t -> t.1 = 2 * n + 1, enter)) AS m2,
                   arrayMap(t -> if(t.1 = 2 * n + 1, m2 - t.2 + 1, 0), enter) AS lft,
                   arrayMap((t, e) -> if(t.1 = 2 * n + 1, m2 - e.2 + 1, 0), enter, arraySlice(tour, n + 1, n)) AS rgt
            FROM (SELECT trace_id, s, n, pidx,
                         arrayFold((acc, x) -> arrayMap(c -> arrayMap((t, y) -> (y.1, toUInt32(t.2 + y.2)), acc,
                                                        arrayMap(y -> (y.2, y.3), arraySort(y -> y.1, arrayFilter((y, k) -> k.2 = 1,
                                                          arrayMap((k, f) -> (k.3, f.4, f.5), c, arrayFill(z -> z.2 = 0, c)), c)))),
                                              [arraySort((v, k) -> k,
                                                 arrayConcat(arrayMap((t, j) -> (j, 0, toUInt32(0), t.1, t.2), acc, arrayEnumerate(acc)),
                                                             arrayMap((t, j) -> (t.1, 1, j, toUInt32(0), toUInt32(0)), acc, arrayEnumerate(acc))),
                                                 arrayConcat(arrayMap(j -> toUInt64(j) * 2, arrayEnumerate(acc)),
                                                             arrayMap(t -> toUInt64(t.1) * 2 + 1, acc)))])[1],
                                   range(toUInt32(ceil(log2(2 * n + 2)))),
                                   arrayConcat(arrayMap((f, i) -> (toUInt32(if(f > 0, f, n + i)), toUInt32(1)), fc, arrayEnumerate(fc)),
                                               arrayMap((sb, p, root) -> (toUInt32(if(sb > 0, sb, if(p > 0, n + p, if(root, 2 * n + 1, 2 * n + 2)))), toUInt32(1)), nsib, pidx, isroot),
                                               [(toUInt32(2 * n + 1), toUInt32(0)), (toUInt32(2 * n + 2), toUInt32(0))])) AS tour
                  FROM (SELECT trace_id, s, n, pidx, isroot,
                               arraySort((g, i) -> (g, i), grp, arrayEnumerate(grp)) AS qg,
                               arraySort((i, g) -> (g, i), arrayEnumerate(grp), grp) AS q,
                               arraySort((v, k) -> k,
                                         arrayMap((g, g2, nq) -> if(g = g2, nq, 0), qg,
                                                  arrayPushBack(arrayPopFront(qg), toInt64(-9223372036854775807)),
                                                  arrayPushBack(arrayPopFront(q), toUInt32(0))), q) AS nsib,
                               arraySort(arrayConcat(arrayMap(i -> (toInt64(i), 0, i), arrayEnumerate(grp)),
                                                     arrayMap((k, j) -> (k, 1, j), grp, arrayEnumerate(grp)))) AS fcc,
                               arrayMap(x -> x.2, arraySort(x -> x.1, arrayFilter(x -> x.1 > 0,
                                 arrayMap((a, b) -> if(a.2 = 0, (a.3, if(b.2 = 1 AND b.1 = a.1, b.3, 0)), (0, 0)),
                                          fcc, arrayReverseFill(x -> x.2 = 1, fcc))))) AS fc
                        FROM (SELECT trace_id, s, n, pidx, isroot,
                                     arrayMap((p, root, i) -> if(p > 0, toInt64(p), if(root, 0, -toInt64(i))), pidx, isroot, arrayEnumerate(pidx)) AS grp
                              FROM (SELECT trace_id, s, toUInt32(length(s)) AS n,
                                           arrayMap(x -> x.2 = toFixedString('', 8), s) AS isroot,
                                           arraySort(arrayConcat(arrayMap((x, j) -> (x.2, 0, j), s, arrayEnumerate(s)),
                                                                 arrayMap((x, j) -> (x.1, 1, j), s, arrayEnumerate(s)))) AS pc,
                                           arrayMap(x -> x.2, arraySort(x -> x.1, arrayFilter(x -> x.1 > 0,
                                             arrayMap((a, b) -> if(a.2 = 0, (a.3, if(b.2 = 1 AND b.1 = a.1, b.3, 0)), (0, 0)),
                                                      pc, arrayReverseFill(x -> x.2 = 1, pc))))) AS pidx0,
                                           arrayMap((v, root) -> if(root, 0, v), pidx0, isroot) AS pidx
                                    FROM (SELECT trace_id, arraySort(x -> (x.3, x.1), groupArray((span_id, parent_span_id, start_ns))) AS s
                                          FROM spans
                                          WHERE (intDiv(start_ns, 300000000000), trace_id) IN (SELECT arrayJoin(nested_keys))
                                          GROUP BY trace_id
                                          SETTINGS max_block_size = 256)))))))) ARRAY JOIN s AS sp, lft AS nl, rgt AS nr, par AS np) AS nb USING (trace_id, span_id)
WHERE start_ns >= 1791399900000000001 AND start_ns < 1791410820000000001
  AND intDiv(start_ns, 300000000000) BETWEEN intDiv(1791399900000000001, 300000000000) AND intDiv(1791410820000000000, 300000000000)
  AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') >= toDate('2026-10-07') AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') <= toDate('2026-10-07')
  AND (true)
GROUP BY g, o, p)
GROUP BY g, o)
LEFT ARRAY JOIN cells AS c
WHERE over = 0))
      GROUP BY g, o)
ORDER BY g, o

== instant ==
SELECT g, o, [(toInt64(1791410820000), v)] AS points, CAST([], 'Array(Tuple(FixedString(16), Int64, Float64))') AS exemplars, 0 AS f
FROM (SELECT g, o, v FROM (SELECT g, o, v, throwIf(count() OVER () > 1000, 'metrics series cap') AS over FROM (SELECT arrayMap(x -> if(x.2 IN ('n', 'N'), ('', x.2), x), [(toString(nb.np), 'i')]) AS g, toUInt64(0) AS o, toFloat64(count()) / 10860 AS v
FROM spans
LEFT JOIN (WITH (SELECT groupArray((k, trace_id)) FROM (SELECT trace_id, arrayJoin(bk) AS k FROM (SELECT trace_id, groupUniqArrayArray(buckets) AS bk FROM traces WHERE day >= toDate(fromUnixTimestamp64Nano(1791313560000000000), 'UTC') AND day <= toDate(fromUnixTimestamp64Nano(1791497219999999999), 'UTC') GROUP BY trace_id HAVING max(last_start_ns) >= 1791399960000000000 AND min(start_ns) <= 1791410819999999999))) AS nested_keys SELECT trace_id, sp.1 AS span_id, nl, nr, np FROM (SELECT trace_id, s, n, lft, rgt,
       arrayMap((l, p) -> if(l = 0, 0, p), lft,
                arrayMap(x -> x.2, arraySort(x -> x.1, arrayFilter(x -> x.1 > 0,
                  arrayMap((a, b) -> if(a.2 = 1, (a.3, b.4), (0, 0)), pl, arrayFill(z -> z.2 = 0, pl)))))) AS par
FROM (SELECT trace_id, s, n, pidx, lft, rgt,
             arraySort(arrayConcat([(toUInt32(0), 0, toUInt32(0), toInt64(-1))],
                                   arrayMap((v, j) -> (j, 0, j, v), lft, arrayEnumerate(lft)),
                                   arrayMap((k, j) -> (k, 1, j, toInt64(0)), pidx, arrayEnumerate(pidx)))) AS pl
      FROM (SELECT trace_id, s, n, pidx,
                   arraySlice(tour, 1, n) AS enter,
                   toInt64(2 * arrayCount(t -> t.1 = 2 * n + 1, enter)) AS m2,
                   arrayMap(t -> if(t.1 = 2 * n + 1, m2 - t.2 + 1, 0), enter) AS lft,
                   arrayMap((t, e) -> if(t.1 = 2 * n + 1, m2 - e.2 + 1, 0), enter, arraySlice(tour, n + 1, n)) AS rgt
            FROM (SELECT trace_id, s, n, pidx,
                         arrayFold((acc, x) -> arrayMap(c -> arrayMap((t, y) -> (y.1, toUInt32(t.2 + y.2)), acc,
                                                        arrayMap(y -> (y.2, y.3), arraySort(y -> y.1, arrayFilter((y, k) -> k.2 = 1,
                                                          arrayMap((k, f) -> (k.3, f.4, f.5), c, arrayFill(z -> z.2 = 0, c)), c)))),
                                              [arraySort((v, k) -> k,
                                                 arrayConcat(arrayMap((t, j) -> (j, 0, toUInt32(0), t.1, t.2), acc, arrayEnumerate(acc)),
                                                             arrayMap((t, j) -> (t.1, 1, j, toUInt32(0), toUInt32(0)), acc, arrayEnumerate(acc))),
                                                 arrayConcat(arrayMap(j -> toUInt64(j) * 2, arrayEnumerate(acc)),
                                                             arrayMap(t -> toUInt64(t.1) * 2 + 1, acc)))])[1],
                                   range(toUInt32(ceil(log2(2 * n + 2)))),
                                   arrayConcat(arrayMap((f, i) -> (toUInt32(if(f > 0, f, n + i)), toUInt32(1)), fc, arrayEnumerate(fc)),
                                               arrayMap((sb, p, root) -> (toUInt32(if(sb > 0, sb, if(p > 0, n + p, if(root, 2 * n + 1, 2 * n + 2)))), toUInt32(1)), nsib, pidx, isroot),
                                               [(toUInt32(2 * n + 1), toUInt32(0)), (toUInt32(2 * n + 2), toUInt32(0))])) AS tour
                  FROM (SELECT trace_id, s, n, pidx, isroot,
                               arraySort((g, i) -> (g, i), grp, arrayEnumerate(grp)) AS qg,
                               arraySort((i, g) -> (g, i), arrayEnumerate(grp), grp) AS q,
                               arraySort((v, k) -> k,
                                         arrayMap((g, g2, nq) -> if(g = g2, nq, 0), qg,
                                                  arrayPushBack(arrayPopFront(qg), toInt64(-9223372036854775807)),
                                                  arrayPushBack(arrayPopFront(q), toUInt32(0))), q) AS nsib,
                               arraySort(arrayConcat(arrayMap(i -> (toInt64(i), 0, i), arrayEnumerate(grp)),
                                                     arrayMap((k, j) -> (k, 1, j), grp, arrayEnumerate(grp)))) AS fcc,
                               arrayMap(x -> x.2, arraySort(x -> x.1, arrayFilter(x -> x.1 > 0,
                                 arrayMap((a, b) -> if(a.2 = 0, (a.3, if(b.2 = 1 AND b.1 = a.1, b.3, 0)), (0, 0)),
                                          fcc, arrayReverseFill(x -> x.2 = 1, fcc))))) AS fc
                        FROM (SELECT trace_id, s, n, pidx, isroot,
                                     arrayMap((p, root, i) -> if(p > 0, toInt64(p), if(root, 0, -toInt64(i))), pidx, isroot, arrayEnumerate(pidx)) AS grp
                              FROM (SELECT trace_id, s, toUInt32(length(s)) AS n,
                                           arrayMap(x -> x.2 = toFixedString('', 8), s) AS isroot,
                                           arraySort(arrayConcat(arrayMap((x, j) -> (x.2, 0, j), s, arrayEnumerate(s)),
                                                                 arrayMap((x, j) -> (x.1, 1, j), s, arrayEnumerate(s)))) AS pc,
                                           arrayMap(x -> x.2, arraySort(x -> x.1, arrayFilter(x -> x.1 > 0,
                                             arrayMap((a, b) -> if(a.2 = 0, (a.3, if(b.2 = 1 AND b.1 = a.1, b.3, 0)), (0, 0)),
                                                      pc, arrayReverseFill(x -> x.2 = 1, pc))))) AS pidx0,
                                           arrayMap((v, root) -> if(root, 0, v), pidx0, isroot) AS pidx
                                    FROM (SELECT trace_id, arraySort(x -> (x.3, x.1), groupArray((span_id, parent_span_id, start_ns))) AS s
                                          FROM spans
                                          WHERE (intDiv(start_ns, 300000000000), trace_id) IN (SELECT arrayJoin(nested_keys))
                                          GROUP BY trace_id
                                          SETTINGS max_block_size = 256)))))))) ARRAY JOIN s AS sp, lft AS nl, rgt AS nr, par AS np) AS nb USING (trace_id, span_id)
WHERE start_ns >= 1791399960000000000 AND start_ns < 1791410820000000000
  AND intDiv(start_ns, 300000000000) BETWEEN intDiv(1791399960000000000, 300000000000) AND intDiv(1791410819999999999, 300000000000)
  AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') >= toDate('2026-10-07') AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') <= toDate('2026-10-07')
  AND (true)
GROUP BY g, o)) WHERE over = 0)
ORDER BY g, o
