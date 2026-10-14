-- case: avg_unscoped
-- q: { .a = 1 } | avg_over_time(duration)

== range ==
SELECT g, points, exemplars, 0 AS f
FROM (SELECT g, any(o) AS o, arraySort(groupArrayIf(((29856666 + p) * 60000, v), NOT none)) AS points,
             arraySort(e -> e.2, groupArrayIf((ex.1, ex.2, v), has AND (T <= 100 OR (intDiv(x * 100 + T - 1, T) < 100 AND intDiv(intDiv(x * 100 + T - 1, T) * T, 100) = x)))) AS exemplars
      FROM (SELECT none, g, o, p, v, ex, ex.2 != 0 AS has,
       row_number() OVER (PARTITION BY has ORDER BY p, o, g) - 1 AS x,
       countIf(has) OVER () AS T
FROM (SELECT g, o, empty(cells) AS none, c.1 AS p, c.2 AS v, if(isNaN(v), CAST((toFixedString('', 16), 0, 0), 'Tuple(FixedString(16), Int64, Int64)'), c.3) AS ex
FROM (SELECT '' AS g, 0 AS o, groupArray((p, v, ex)) AS cells
FROM (SELECT '' AS g, 0 AS o, intDiv(start_ns - 1, 60000000000) + 1 - 29856666 AS p, avg(duration_ns) / 1000000000 AS v, argMax((trace_id, start_ns, duration_ns), (duration_ns, span_id)) AS ex
FROM spans
WHERE start_ns >= 1791399900000000001 AND start_ns < 1791410820000000001
  AND intDiv(start_ns, 300000000000) BETWEEN intDiv(1791399900000000001, 300000000000) AND intDiv(1791410820000000000, 300000000000)
  AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') >= toDate('2026-10-07') AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') <= toDate('2026-10-07')
  AND (multiIf(dynamicType(attrs.`a`) != 'None', (coalesce(attrs.`a`.:Int64 = 1, false) OR coalesce(attrs.`a`.:Float64 = 1, false)), resource_id IN (SELECT resource_id FROM resources WHERE day >= toDate(fromUnixTimestamp64Nano(1791399900000000001), 'UTC') AND day <= toDate(fromUnixTimestamp64Nano(1791410820000000000), 'UTC') AND (dynamicType(attrs.`a`) != 'None')), resource_id IN (SELECT resource_id FROM resources WHERE day >= toDate(fromUnixTimestamp64Nano(1791399900000000001), 'UTC') AND day <= toDate(fromUnixTimestamp64Nano(1791410820000000000), 'UTC') AND ((coalesce(attrs.`a`.:Int64 = 1, false) OR coalesce(attrs.`a`.:Float64 = 1, false)))), arrayExists(d -> dynamicType(d) != 'None', events.attrs.`a`), arrayExists((i, f) -> (coalesce(i = 1, false) OR coalesce(f = 1, false)), events.attrs.`a`.:Int64, events.attrs.`a`.:Float64), arrayExists(d -> dynamicType(d) != 'None', links.attrs.`a`), arrayExists((i, f) -> (coalesce(i = 1, false) OR coalesce(f = 1, false)), links.attrs.`a`.:Int64, links.attrs.`a`.:Float64), dynamicType(scope_attrs.`a`) != 'None', (coalesce(scope_attrs.`a`.:Int64 = 1, false) OR coalesce(scope_attrs.`a`.:Float64 = 1, false)), false))
GROUP BY p))
LEFT ARRAY JOIN cells AS c))
      GROUP BY g)
WHERE length(points) > 0
ORDER BY o, g

== instant ==
SELECT g, [(toInt64(1791410820000), v)] AS points, CAST([], 'Array(Tuple(FixedString(16), Int64, Float64))') AS exemplars, 0 AS f
FROM (SELECT '' AS g, 0 AS o, avg(duration_ns) / 1000000000 AS v
FROM spans
WHERE start_ns >= 1791399960000000000 AND start_ns < 1791410820000000000
  AND intDiv(start_ns, 300000000000) BETWEEN intDiv(1791399960000000000, 300000000000) AND intDiv(1791410819999999999, 300000000000)
  AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') >= toDate('2026-10-07') AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') <= toDate('2026-10-07')
  AND (multiIf(dynamicType(attrs.`a`) != 'None', (coalesce(attrs.`a`.:Int64 = 1, false) OR coalesce(attrs.`a`.:Float64 = 1, false)), resource_id IN (SELECT resource_id FROM resources WHERE day >= toDate(fromUnixTimestamp64Nano(1791399960000000000), 'UTC') AND day <= toDate(fromUnixTimestamp64Nano(1791410819999999999), 'UTC') AND (dynamicType(attrs.`a`) != 'None')), resource_id IN (SELECT resource_id FROM resources WHERE day >= toDate(fromUnixTimestamp64Nano(1791399960000000000), 'UTC') AND day <= toDate(fromUnixTimestamp64Nano(1791410819999999999), 'UTC') AND ((coalesce(attrs.`a`.:Int64 = 1, false) OR coalesce(attrs.`a`.:Float64 = 1, false)))), arrayExists(d -> dynamicType(d) != 'None', events.attrs.`a`), arrayExists((i, f) -> (coalesce(i = 1, false) OR coalesce(f = 1, false)), events.attrs.`a`.:Int64, events.attrs.`a`.:Float64), arrayExists(d -> dynamicType(d) != 'None', links.attrs.`a`), arrayExists((i, f) -> (coalesce(i = 1, false) OR coalesce(f = 1, false)), links.attrs.`a`.:Int64, links.attrs.`a`.:Float64), dynamicType(scope_attrs.`a`) != 'None', (coalesce(scope_attrs.`a`.:Int64 = 1, false) OR coalesce(scope_attrs.`a`.:Float64 = 1, false)), false))
HAVING count() > 0)
ORDER BY o, g
