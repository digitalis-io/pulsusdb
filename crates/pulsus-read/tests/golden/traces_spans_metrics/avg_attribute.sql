-- case: avg_attribute
-- q: {} | avg_over_time(span.http.request.body.size)

== range ==
SELECT g, o, points, exemplars, 0 AS f
FROM (SELECT g, o, arraySort(groupArrayIf(((29856666 + p) * 60000, v), NOT none)) AS points,
             arraySort(e -> e.2, groupArrayIf((ex.1, ex.2, ex.3), has AND (T <= 100 OR (intDiv(x * 100 + T - 1, T) < 100 AND intDiv(intDiv(x * 100 + T - 1, T) * T, 100) = x)))) AS exemplars
      FROM (SELECT none, g, o, p, v, ex, ex.2 != 0 AS has,
       row_number() OVER (PARTITION BY has ORDER BY p, g, o) - 1 AS x,
       countIf(has) OVER () AS T
FROM (SELECT g, o, empty(cells) AS none, c.1 AS p, c.2 AS v, if(isNaN(v), CAST((toFixedString('', 16), 0, 0), 'Tuple(FixedString(16), Int64, Float64)'), c.3) AS ex
FROM (SELECT CAST([], 'Array(Tuple(String, String))') AS g, toUInt64(0) AS o, groupArray((p, v, ex)) AS cells
FROM (SELECT CAST([], 'Array(Tuple(String, String))') AS g, toUInt64(0) AS o, intDiv(start_ns - 1, 60000000000) + 1 - 29856666 AS p, avg(assumeNotNull(w_x)) AS v, argMax((trace_id, start_ns, assumeNotNull(w_x)), (duration_ns, span_id)) AS ex
FROM (SELECT *, if(dynamicType(attrs.`http%2Erequest%2Ebody%2Esize`) = 'Int64', toFloat64(dynamicElement(attrs.`http%2Erequest%2Ebody%2Esize`, 'Int64')), if(dynamicType(attrs.`http%2Erequest%2Ebody%2Esize`) = 'Float64', dynamicElement(attrs.`http%2Erequest%2Ebody%2Esize`, 'Float64'), NULL)) AS w_x, dynamicElement(attrs.`http%2Erequest%2Ebody%2Esize`, 'Int64') AS w_i FROM spans
WHERE start_ns >= 1791399900000000001 AND start_ns < 1791410820000000001
  AND intDiv(start_ns, 300000000000) BETWEEN intDiv(1791399900000000001, 300000000000) AND intDiv(1791410820000000000, 300000000000)
  AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') >= toDate('2026-10-07') AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') <= toDate('2026-10-07')
  AND (true) AND if(dynamicType(attrs.`http%2Erequest%2Ebody%2Esize`) = 'Int64', toFloat64(dynamicElement(attrs.`http%2Erequest%2Ebody%2Esize`, 'Int64')), if(dynamicType(attrs.`http%2Erequest%2Ebody%2Esize`) = 'Float64', dynamicElement(attrs.`http%2Erequest%2Ebody%2Esize`, 'Float64'), NULL)) IS NOT NULL
LIMIT 1 BY trace_id, start_ns, span_id, kind)
GROUP BY p))
LEFT ARRAY JOIN cells AS c))
      GROUP BY g, o)
WHERE length(points) > 0
ORDER BY g, o

== instant ==
SELECT g, o, [(toInt64(1791410820000), v)] AS points, CAST([], 'Array(Tuple(FixedString(16), Int64, Float64))') AS exemplars, 0 AS f
FROM (SELECT CAST([], 'Array(Tuple(String, String))') AS g, toUInt64(0) AS o, avg(assumeNotNull(w_x)) AS v
FROM (SELECT *, if(dynamicType(attrs.`http%2Erequest%2Ebody%2Esize`) = 'Int64', toFloat64(dynamicElement(attrs.`http%2Erequest%2Ebody%2Esize`, 'Int64')), if(dynamicType(attrs.`http%2Erequest%2Ebody%2Esize`) = 'Float64', dynamicElement(attrs.`http%2Erequest%2Ebody%2Esize`, 'Float64'), NULL)) AS w_x, dynamicElement(attrs.`http%2Erequest%2Ebody%2Esize`, 'Int64') AS w_i FROM spans
WHERE start_ns >= 1791400000000000000 AND start_ns < 1791410800000000000
  AND intDiv(start_ns, 300000000000) BETWEEN intDiv(1791400000000000000, 300000000000) AND intDiv(1791410799999999999, 300000000000)
  AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') >= toDate('2026-10-07') AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') <= toDate('2026-10-07')
  AND (true) AND if(dynamicType(attrs.`http%2Erequest%2Ebody%2Esize`) = 'Int64', toFloat64(dynamicElement(attrs.`http%2Erequest%2Ebody%2Esize`, 'Int64')), if(dynamicType(attrs.`http%2Erequest%2Ebody%2Esize`) = 'Float64', dynamicElement(attrs.`http%2Erequest%2Ebody%2Esize`, 'Float64'), NULL)) IS NOT NULL
LIMIT 1 BY trace_id, start_ns, span_id, kind)
HAVING count() > 0)
ORDER BY g, o
