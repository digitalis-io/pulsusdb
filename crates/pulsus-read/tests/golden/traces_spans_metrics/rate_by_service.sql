-- case: rate_by_service
-- q: {} | rate() by(resource.service.name)

== range ==
SELECT g, o, points, exemplars, 1 AS f
FROM (SELECT g, o, arraySort(groupArrayIf(((29856666 + p) * 60000, v), NOT none)) AS points,
             arraySort(e -> e.2, groupArrayIf((ex.1, ex.2, v), has AND (T <= 100 OR (intDiv(x * 100 + T - 1, T) < 100 AND intDiv(intDiv(x * 100 + T - 1, T) * T, 100) = x)))) AS exemplars
      FROM (SELECT none, g, o, p, v, ex, ex.2 != 0 AS has,
       row_number() OVER (PARTITION BY has ORDER BY p, g, o) - 1 AS x,
       countIf(has) OVER () AS T
FROM (SELECT g, o, empty(cells) AS none, c.1 AS p, c.2 AS v, if(isNaN(v), CAST((toFixedString('', 16), 0, 0), 'Tuple(FixedString(16), Int64, Int64)'), c.3) AS ex
FROM (SELECT g, o, groupArray((p, v, ex)) AS cells, throwIf(count() OVER () > 1000, 'metrics series cap') AS over
FROM (SELECT arrayMap(x -> if(x.2 IN ('n', 'N'), ('', x.2), x), [if(service_type = 'string', (toString(service), 's'), if(((transform(resource_id, tupleElement((SELECT (groupArray(resource_id), groupArray((toString(attrs.`service%2Ename`), multiIf(dynamicType(attrs.`service%2Ename`) = 'Int64', 'i', dynamicType(attrs.`service%2Ename`) = 'Float64', 'd', dynamicType(attrs.`service%2Ename`) = 'Bool', 'b', dynamicType(attrs.`service%2Ename`) = 'String', 's', 'a')))) FROM resources WHERE org_id = '' AND day >= toDate(fromUnixTimestamp64Nano(1791399900000000001), 'UTC') AND day <= toDate(fromUnixTimestamp64Nano(1791410820000000000), 'UTC') AND dynamicType(attrs.`service%2Ename`) IN ('String', 'Int64', 'Float64', 'Bool', 'Array(Nullable(String))', 'Array(Nullable(Int64))', 'Array(Nullable(Float64))', 'Array(Nullable(Bool))')) AS rk_87e43de65a15c0a3, 1), arrayEnumerate(tupleElement(rk_87e43de65a15c0a3, 1)), 0) AS ri_87e43de65a15c0a3) != 0), arrayElement(tupleElement(rk_87e43de65a15c0a3, 2), ri_87e43de65a15c0a3), ('', 'N')))]) AS g, toUInt64(0) AS o, intDiv(start_ns - 1, 60000000000) + 1 - 29856666 AS p, toFloat64(uniqExact(trace_id, start_ns, span_id, kind)) / 60 AS v, argMax((trace_id, start_ns, duration_ns), (duration_ns, span_id)) AS ex
FROM spans
WHERE org_id = '' AND start_ns >= 1791399900000000001 AND start_ns < 1791410820000000001
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
FROM (SELECT g, o, v FROM (SELECT g, o, v, throwIf(count() OVER () > 1000, 'metrics series cap') AS over FROM (SELECT arrayMap(x -> if(x.2 IN ('n', 'N'), ('', x.2), x), [if(service_type = 'string', (toString(service), 's'), if(((transform(resource_id, tupleElement((SELECT (groupArray(resource_id), groupArray((toString(attrs.`service%2Ename`), multiIf(dynamicType(attrs.`service%2Ename`) = 'Int64', 'i', dynamicType(attrs.`service%2Ename`) = 'Float64', 'd', dynamicType(attrs.`service%2Ename`) = 'Bool', 'b', dynamicType(attrs.`service%2Ename`) = 'String', 's', 'a')))) FROM resources WHERE org_id = '' AND day >= toDate(fromUnixTimestamp64Nano(1791400000000000000), 'UTC') AND day <= toDate(fromUnixTimestamp64Nano(1791410799999999999), 'UTC') AND dynamicType(attrs.`service%2Ename`) IN ('String', 'Int64', 'Float64', 'Bool', 'Array(Nullable(String))', 'Array(Nullable(Int64))', 'Array(Nullable(Float64))', 'Array(Nullable(Bool))')) AS rk_0e4274514fbf373c, 1), arrayEnumerate(tupleElement(rk_0e4274514fbf373c, 1)), 0) AS ri_0e4274514fbf373c) != 0), arrayElement(tupleElement(rk_0e4274514fbf373c, 2), ri_0e4274514fbf373c), ('', 'N')))]) AS g, toUInt64(0) AS o, toFloat64(uniqExact(trace_id, start_ns, span_id, kind)) / 10800 AS v
FROM spans
WHERE org_id = '' AND start_ns >= 1791400000000000000 AND start_ns < 1791410800000000000
  AND intDiv(start_ns, 300000000000) BETWEEN intDiv(1791400000000000000, 300000000000) AND intDiv(1791410799999999999, 300000000000)
  AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') >= toDate('2026-10-07') AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') <= toDate('2026-10-07')
  AND (true)
GROUP BY g, o)) WHERE over = 0)
ORDER BY g, o
