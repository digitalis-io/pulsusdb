-- case: by_root_service
-- q: {} | rate() by(trace:rootService)

== range ==
SELECT g, o, points, exemplars, 1 AS f
FROM (SELECT g, o, arraySort(groupArrayIf(((29856666 + p) * 60000, v), NOT none)) AS points,
             arraySort(e -> e.2, groupArrayIf((ex.1, ex.2, v), has AND (T <= 100 OR (intDiv(x * 100 + T - 1, T) < 100 AND intDiv(intDiv(x * 100 + T - 1, T) * T, 100) = x)))) AS exemplars
      FROM (SELECT none, g, o, p, v, ex, ex.2 != 0 AS has,
       row_number() OVER (PARTITION BY has ORDER BY p, g, o) - 1 AS x,
       countIf(has) OVER () AS T
FROM (SELECT g, o, empty(cells) AS none, c.1 AS p, c.2 AS v, if(isNaN(v), CAST((toFixedString('', 16), 0, 0), 'Tuple(FixedString(16), Int64, Int64)'), c.3) AS ex
FROM (SELECT g, o, groupArray((p, v, ex)) AS cells, throwIf(count() OVER () > 1000, 'metrics series cap') AS over
FROM (SELECT arrayMap(x -> if(x.2 IN ('n', 'N'), ('', x.2), x), [(tv.rs, 's')]) AS g, toUInt64(0) AS o, intDiv(start_ns - 1, 60000000000) + 1 - 29856666 AS p, toFloat64(count()) / 60 AS v, argMax((trace_id, start_ns, duration_ns), (duration_ns, span_id)) AS ex
FROM spans
LEFT JOIN (SELECT trace_id, max(end_ns) - min(start_ns) AS td, min(root) AS r, if(r.1 = 0, r.5, '') AS rn, if(r.1 = 0, r.4, '') AS rs FROM traces WHERE day >= toDate(fromUnixTimestamp64Nano(1791313500000000001), 'UTC') AND day <= toDate(fromUnixTimestamp64Nano(1791497220000000000), 'UTC') GROUP BY trace_id) AS tv USING trace_id
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
FROM (SELECT g, o, v FROM (SELECT g, o, v, throwIf(count() OVER () > 1000, 'metrics series cap') AS over FROM (SELECT arrayMap(x -> if(x.2 IN ('n', 'N'), ('', x.2), x), [(tv.rs, 's')]) AS g, toUInt64(0) AS o, toFloat64(count()) / 10860 AS v
FROM spans
LEFT JOIN (SELECT trace_id, max(end_ns) - min(start_ns) AS td, min(root) AS r, if(r.1 = 0, r.5, '') AS rn, if(r.1 = 0, r.4, '') AS rs FROM traces WHERE day >= toDate(fromUnixTimestamp64Nano(1791313560000000000), 'UTC') AND day <= toDate(fromUnixTimestamp64Nano(1791497219999999999), 'UTC') GROUP BY trace_id) AS tv USING trace_id
WHERE start_ns >= 1791399960000000000 AND start_ns < 1791410820000000000
  AND intDiv(start_ns, 300000000000) BETWEEN intDiv(1791399960000000000, 300000000000) AND intDiv(1791410819999999999, 300000000000)
  AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') >= toDate('2026-10-07') AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') <= toDate('2026-10-07')
  AND (true)
GROUP BY g, o)) WHERE over = 0)
ORDER BY g, o
