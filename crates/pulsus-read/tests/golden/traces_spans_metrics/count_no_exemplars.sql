-- case: count_no_exemplars
-- q: {} | count_over_time() by(resource.service.name) with(exemplars=false)

== range ==
SELECT g, o, points, exemplars, 1 AS f
FROM (SELECT g, o, arraySort(groupArrayIf(((29856666 + p) * 60000, v), NOT none)) AS points,
             CAST([], 'Array(Tuple(FixedString(16), Int64, Float64))') AS exemplars
      FROM (SELECT none, g, o, p, v, ex FROM (SELECT g, o, empty(cells) AS none, c.1 AS p, c.2 AS v, if(isNaN(v), CAST((toFixedString('', 16), 0, 0), 'Tuple(FixedString(16), Int64, Int64)'), c.3) AS ex
FROM (SELECT g, o, groupArray((p, v, ex)) AS cells, throwIf(count() OVER () > 1000, 'metrics series cap') AS over
FROM (SELECT arrayMap(x -> if(x.2 IN ('n', 'N'), ('', x.2), x), [if(service_type = 'string', (toString(service), 's'), ('', 'N'))]) AS g, toUInt64(0) AS o, intDiv(start_ns - 1, 60000000000) + 1 - 29856666 AS p, toFloat64(count()) AS v, CAST((toFixedString('', 16), 0, 0), 'Tuple(FixedString(16), Int64, Int64)') AS ex
FROM spans
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
FROM (SELECT g, o, v FROM (SELECT g, o, v, throwIf(count() OVER () > 1000, 'metrics series cap') AS over FROM (SELECT arrayMap(x -> if(x.2 IN ('n', 'N'), ('', x.2), x), [if(service_type = 'string', (toString(service), 's'), ('', 'N'))]) AS g, toUInt64(0) AS o, toFloat64(count()) AS v
FROM spans
WHERE start_ns >= 1791399960000000000 AND start_ns < 1791410820000000000
  AND intDiv(start_ns, 300000000000) BETWEEN intDiv(1791399960000000000, 300000000000) AND intDiv(1791410819999999999, 300000000000)
  AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') >= toDate('2026-10-07') AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') <= toDate('2026-10-07')
  AND (true)
GROUP BY g, o)) WHERE over = 0)
ORDER BY g, o
