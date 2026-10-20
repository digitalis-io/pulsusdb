-- case: rate_by_service_topk
-- q: {} | rate() by(resource.service.name) | topk(3)

== range ==
SELECT g, o, points, exemplars, 0 AS f
FROM (SELECT g, o, arraySort(groupArrayIf(((29856666 + p) * 60000, v), NOT none)) AS points,
             arraySort(e -> e.2, groupArrayIf((ex.1, ex.2, v), has AND (T <= 100 OR (intDiv(x * 100 + T - 1, T) < 100 AND intDiv(intDiv(x * 100 + T - 1, T) * T, 100) = x)))) AS exemplars
      FROM (SELECT none, g, o, p, v, ex, ex.2 != 0 AS has,
       row_number() OVER (PARTITION BY has ORDER BY p, g, o) - 1 AS x,
       countIf(has) OVER () AS T
FROM (SELECT 0 AS none, g, o, p, v, ex FROM (SELECT g, o, p, v, ex, row_number() OVER (PARTITION BY p ORDER BY v DESC, g, o) AS r
FROM (SELECT g, o, p, v, ex FROM (SELECT g, o, c.1 AS p, c.2 AS v, c.3 AS ex
FROM (SELECT g, o, groupArray((p, v, ex)) AS cells, throwIf(count() OVER () > 1000, 'metrics series cap') AS over
FROM (SELECT arrayMap(x -> if(x.2 IN ('n', 'N'), ('', x.2), x), [if(service_type = 'string', (toString(service), 's'), ('', 'N'))]) AS g, toUInt64(0) AS o, intDiv(start_ns - 1, 60000000000) + 1 - 29856666 AS p, toFloat64(count()) / 60 AS v, argMax((trace_id, start_ns, duration_ns), (duration_ns, span_id)) AS ex
FROM spans
WHERE start_ns >= 1791399900000000001 AND start_ns < 1791410820000000001
  AND intDiv(start_ns, 300000000000) BETWEEN intDiv(1791399900000000001, 300000000000) AND intDiv(1791410820000000000, 300000000000)
  AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') >= toDate('2026-10-07') AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') <= toDate('2026-10-07')
  AND (true)
GROUP BY g, o, p)
GROUP BY g, o)
ARRAY JOIN cells AS c
WHERE over = 0)
ORDER BY g, o, p WITH FILL FROM 0 TO 182)) WHERE r <= 3))
      GROUP BY g, o)
WHERE length(points) > 0
ORDER BY g, o

== instant ==
SELECT g, o, [(toInt64(1791410820000), v)] AS points, CAST([], 'Array(Tuple(FixedString(16), Int64, Float64))') AS exemplars, 0 AS f
FROM (SELECT g, o, v FROM (SELECT g, o, v, row_number() OVER (ORDER BY v DESC, g, o) AS r FROM (SELECT g, o, v FROM (SELECT g, o, v, throwIf(count() OVER () > 1000, 'metrics series cap') AS over FROM (SELECT arrayMap(x -> if(x.2 IN ('n', 'N'), ('', x.2), x), [if(service_type = 'string', (toString(service), 's'), ('', 'N'))]) AS g, toUInt64(0) AS o, toFloat64(count()) / 10800 AS v
FROM spans
WHERE start_ns >= 1791400000000000000 AND start_ns < 1791410800000000000
  AND intDiv(start_ns, 300000000000) BETWEEN intDiv(1791400000000000000, 300000000000) AND intDiv(1791410799999999999, 300000000000)
  AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') >= toDate('2026-10-07') AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') <= toDate('2026-10-07')
  AND (true)
GROUP BY g, o)) WHERE over = 0)) WHERE r <= 3)
ORDER BY g, o
