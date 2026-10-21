-- case: quantile_by_route
-- q: {} | quantile_over_time(duration, 0.5, 0.9, 0.99) by(span.http.route)

== range ==
SELECT g, o, points, exemplars, 1 AS f
FROM (SELECT g, o, arraySort(groupArrayIf(((29856666 + p) * 60000, v), NOT none)) AS points,
             arraySort(e -> e.2, groupArrayIf((ex.1, ex.2, ex.3 / 1000000000), has AND (T <= 100 OR (intDiv(x * 100 + T - 1, T) < 100 AND intDiv(intDiv(x * 100 + T - 1, T) * T, 100) = x)))) AS exemplars
      FROM (SELECT none, g, o, p, v, ex, ex.2 != 0 AS has,
       row_number() OVER (PARTITION BY has ORDER BY p, g, o) - 1 AS x,
       countIf(has) OVER () AS T
FROM (SELECT g, o, empty(cells) AS none, c.1 AS p, c.2 AS v, if(isNaN(v), CAST((toFixedString('', 16), 0, 0), 'Tuple(FixedString(16), Int64, Int64)'), c.3) AS ex
FROM (SELECT g, o, groupArray((p, v, ex)) AS cells, throwIf(count() OVER () > 1000, 'metrics series cap') AS over
FROM (SELECT g, toUInt64(qi) AS o, p, q AS v,
       if(qi = indexOf(arrayMap(x -> abs(x - x0.3 / 1000000000), qq), arrayMin(arrayMap(x -> abs(x - x0.3 / 1000000000), qq))), x0, CAST((toFixedString('', 16), 0, 0), 'Tuple(FixedString(16), Int64, Int64)')) AS ex
FROM (SELECT arrayMap(x -> if(x.2 IN ('n', 'N'), ('', x.2), x), [(toString(attrs.`http%2Eroute`), multiIf(dynamicType(attrs.`http%2Eroute`) = 'Int64', 'i', dynamicType(attrs.`http%2Eroute`) = 'Float64', 'd', dynamicType(attrs.`http%2Eroute`) = 'Bool', 'b', dynamicType(attrs.`http%2Eroute`) = 'String', 's', NOT (dynamicType(attrs.`http%2Eroute`) IN ('String', 'Int64', 'Float64', 'Bool', 'Array(Nullable(String))', 'Array(Nullable(Int64))', 'Array(Nullable(Float64))', 'Array(Nullable(Bool))')), 'n', 'a'))]) AS g, intDiv(start_ns - 1, 60000000000) + 1 - 29856666 AS p,
             arrayMap(x -> if(isFinite(x), x / 1000000000, 0), CAST(quantilesTDigest(0.5, 0.9, 0.99)(duration_ns) AS Array(Float64))) AS qq,
             argMax((trace_id, start_ns, duration_ns), (duration_ns, span_id)) AS x0
      FROM (SELECT * FROM spans
      WHERE start_ns >= 1791399900000000001 AND start_ns < 1791410820000000001
        AND intDiv(start_ns, 300000000000) BETWEEN intDiv(1791399900000000001, 300000000000) AND intDiv(1791410820000000000, 300000000000)
        AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') >= toDate('2026-10-07') AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') <= toDate('2026-10-07')
        AND (true)
      LIMIT 1 BY trace_id, start_ns, span_id, kind)
      GROUP BY g, p)
ARRAY JOIN qq AS q, arrayEnumerate(qq) AS qi)
GROUP BY g, o)
LEFT ARRAY JOIN cells AS c
WHERE over = 0))
      GROUP BY g, o)
ORDER BY g, o

== instant ==
SELECT g, o, [(toInt64(1791410820000), v)] AS points, CAST([], 'Array(Tuple(FixedString(16), Int64, Float64))') AS exemplars, 0 AS f
FROM (SELECT g, o, v FROM (SELECT g, o, v, throwIf(count() OVER () > 1000, 'metrics series cap') AS over FROM (SELECT g, toUInt64(qi) AS o, q AS v FROM (SELECT arrayMap(x -> if(x.2 IN ('n', 'N'), ('', x.2), x), [(toString(attrs.`http%2Eroute`), multiIf(dynamicType(attrs.`http%2Eroute`) = 'Int64', 'i', dynamicType(attrs.`http%2Eroute`) = 'Float64', 'd', dynamicType(attrs.`http%2Eroute`) = 'Bool', 'b', dynamicType(attrs.`http%2Eroute`) = 'String', 's', NOT (dynamicType(attrs.`http%2Eroute`) IN ('String', 'Int64', 'Float64', 'Bool', 'Array(Nullable(String))', 'Array(Nullable(Int64))', 'Array(Nullable(Float64))', 'Array(Nullable(Bool))')), 'n', 'a'))]) AS g, arrayMap(x -> if(isFinite(x), x / 1000000000, 0), CAST(quantilesTDigest(0.5, 0.9, 0.99)(duration_ns) AS Array(Float64))) AS qq
FROM (SELECT * FROM spans
WHERE start_ns >= 1791400000000000000 AND start_ns < 1791410800000000000
  AND intDiv(start_ns, 300000000000) BETWEEN intDiv(1791400000000000000, 300000000000) AND intDiv(1791410799999999999, 300000000000)
  AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') >= toDate('2026-10-07') AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') <= toDate('2026-10-07')
  AND (true)
LIMIT 1 BY trace_id, start_ns, span_id, kind)
GROUP BY g)
ARRAY JOIN qq AS q, arrayEnumerate(qq) AS qi)) WHERE over = 0)
ORDER BY g, o
