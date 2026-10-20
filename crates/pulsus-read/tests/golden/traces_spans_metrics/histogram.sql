-- case: histogram
-- q: {} | histogram_over_time(duration)

== range ==
SELECT g, o, points, exemplars, 1 AS f
FROM (SELECT g, o, arraySort(groupArrayIf(((29856666 + p) * 60000, v), NOT none)) AS points,
             arraySort(e -> e.2, groupArrayIf((ex.1, ex.2, v), has AND (T <= 100 OR (intDiv(x * 100 + T - 1, T) < 100 AND intDiv(intDiv(x * 100 + T - 1, T) * T, 100) = x)))) AS exemplars
      FROM (SELECT none, g, o, p, v, ex, ex.2 != 0 AS has,
       row_number() OVER (PARTITION BY has ORDER BY p, g, o) - 1 AS x,
       countIf(has) OVER () AS T
FROM (SELECT g, o, empty(cells) AS none, c.1 AS p, c.2 AS v, if(isNaN(v), CAST((toFixedString('', 16), 0, 0), 'Tuple(FixedString(16), Int64, Int64)'), c.3) AS ex
FROM (SELECT g, o, groupArray((p, v, ex)) AS cells
FROM (SELECT CAST([], 'Array(Tuple(String, String))') AS g, toUInt64(roundToExp2(duration_ns - 1)) * 2 AS o, intDiv(start_ns - 1, 60000000000) + 1 - 29856666 AS p, toFloat64(count()) AS v, argMax((trace_id, start_ns, duration_ns), (duration_ns, span_id)) AS ex
FROM (SELECT * FROM spans
WHERE start_ns >= 1791399900000000001 AND start_ns < 1791410820000000001
  AND intDiv(start_ns, 300000000000) BETWEEN intDiv(1791399900000000001, 300000000000) AND intDiv(1791410820000000000, 300000000000)
  AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') >= toDate('2026-10-07') AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') <= toDate('2026-10-07')
  AND (true) AND duration_ns >= 2
LIMIT 1 BY trace_id, start_ns, span_id, kind)
GROUP BY g, o, p)
GROUP BY g, o)
LEFT ARRAY JOIN cells AS c))
      GROUP BY g, o)
ORDER BY g, o

== instant ==
SELECT g, o, [(toInt64(1791410820000), v)] AS points, CAST([], 'Array(Tuple(FixedString(16), Int64, Float64))') AS exemplars, 0 AS f
FROM (SELECT CAST([], 'Array(Tuple(String, String))') AS g, toUInt64(roundToExp2(duration_ns - 1)) * 2 AS o, toFloat64(count()) AS v
FROM (SELECT * FROM spans
WHERE start_ns >= 1791400000000000000 AND start_ns < 1791410800000000000
  AND intDiv(start_ns, 300000000000) BETWEEN intDiv(1791400000000000000, 300000000000) AND intDiv(1791410799999999999, 300000000000)
  AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') >= toDate('2026-10-07') AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') <= toDate('2026-10-07')
  AND (true) AND duration_ns >= 2
LIMIT 1 BY trace_id, start_ns, span_id, kind)
GROUP BY g, o)
ORDER BY g, o
