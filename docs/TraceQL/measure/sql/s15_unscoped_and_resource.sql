WITH
    1790084801000000000 AS s, 1790095601000000000 AS e,
    (SELECT (groupArray(trace_id), groupArray(keys))
     FROM (SELECT trace_id, max(start_ns) AS last,
                  groupUniqArray(intDiv(start_ns, 300000000000)) AS keys
           FROM tqd_g1.spans
           WHERE start_ns >= s AND start_ns < e AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985
             AND ((multiIf(dynamicType(attrs.`app%2Ecache%2Ehit`) != 'None', attrs.`app%2Ecache%2Ehit`.:Bool = true, resource_id IN (SELECT resource_id FROM tqd_g1.resources WHERE day >= toDate(fromUnixTimestamp64Nano(1790084801000000000)) AND day <= toDate(fromUnixTimestamp64Nano(1790095601000000000 - 1)) AND dynamicType(attrs.`app%2Ecache%2Ehit`) != 'None'), resource_id IN (SELECT resource_id FROM tqd_g1.resources WHERE day >= toDate(fromUnixTimestamp64Nano(1790084801000000000)) AND day <= toDate(fromUnixTimestamp64Nano(1790095601000000000 - 1)) AND attrs.`app%2Ecache%2Ehit`.:Bool = true), arrayExists(x -> dynamicType(x) != 'None', events.attrs.`app%2Ecache%2Ehit`), dynamicElement(arrayFirst(x -> dynamicType(x) != 'None', events.attrs.`app%2Ecache%2Ehit`), 'Bool') = true, arrayExists(x -> dynamicType(x) != 'None', links.attrs.`app%2Ecache%2Ehit`), dynamicElement(arrayFirst(x -> dynamicType(x) != 'None', links.attrs.`app%2Ecache%2Ehit`), 'Bool') = true, scope_attrs.`app%2Ecache%2Ehit`.:Bool = true)) AND resource_id IN (SELECT resource_id FROM tqd_g1.resources WHERE day >= toDate(fromUnixTimestamp64Nano(1790084801000000000)) AND day <= toDate(fromUnixTimestamp64Nano(1790095601000000000 - 1)) AND attrs.`k8s%2Epod%2Ename`.:String = 'cart-7d9f8b-00002'))
           GROUP BY trace_id
           ORDER BY last DESC, trace_id ASC
           LIMIT 20)) AS top
SELECT m.trace_id, t.root_service, t.root_name, t.start_ns, t.end_ns - t.start_ns AS trace_duration_ns,
       m.last, m.matched, m.spans
FROM (SELECT trace_id, max(start_ns) AS last, count() AS matched,
             arraySlice(arraySort(x -> (x.2, x.1), groupArray((span_id, start_ns, duration_ns, attrs.`app%2Ecache%2Ehit`, resource_id))), 1, 3) AS spans
      FROM tqd_g1.spans
      WHERE (intDiv(start_ns, 300000000000), trace_id) IN
            (SELECT arrayJoin(arrayFlatten(arrayMap((t, ks) -> arrayMap(k -> (k, t), ks), top.1, top.2))))
        AND start_ns >= s AND start_ns < e AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985
        AND ((multiIf(dynamicType(attrs.`app%2Ecache%2Ehit`) != 'None', attrs.`app%2Ecache%2Ehit`.:Bool = true, resource_id IN (SELECT resource_id FROM tqd_g1.resources WHERE day >= toDate(fromUnixTimestamp64Nano(1790084801000000000)) AND day <= toDate(fromUnixTimestamp64Nano(1790095601000000000 - 1)) AND dynamicType(attrs.`app%2Ecache%2Ehit`) != 'None'), resource_id IN (SELECT resource_id FROM tqd_g1.resources WHERE day >= toDate(fromUnixTimestamp64Nano(1790084801000000000)) AND day <= toDate(fromUnixTimestamp64Nano(1790095601000000000 - 1)) AND attrs.`app%2Ecache%2Ehit`.:Bool = true), arrayExists(x -> dynamicType(x) != 'None', events.attrs.`app%2Ecache%2Ehit`), dynamicElement(arrayFirst(x -> dynamicType(x) != 'None', events.attrs.`app%2Ecache%2Ehit`), 'Bool') = true, arrayExists(x -> dynamicType(x) != 'None', links.attrs.`app%2Ecache%2Ehit`), dynamicElement(arrayFirst(x -> dynamicType(x) != 'None', links.attrs.`app%2Ecache%2Ehit`), 'Bool') = true, scope_attrs.`app%2Ecache%2Ehit`.:Bool = true)) AND resource_id IN (SELECT resource_id FROM tqd_g1.resources WHERE day >= toDate(fromUnixTimestamp64Nano(1790084801000000000)) AND day <= toDate(fromUnixTimestamp64Nano(1790095601000000000 - 1)) AND attrs.`k8s%2Epod%2Ename`.:String = 'cart-7d9f8b-00002'))
      GROUP BY trace_id) AS m
LEFT JOIN (SELECT trace_id, min(start_ns) AS start_ns, max(end_ns) AS end_ns,
                  max(root_service) AS root_service, max(root_name) AS root_name
           FROM tqd_g1.traces
           WHERE trace_id IN (SELECT arrayJoin(top.1))
           GROUP BY trace_id) AS t USING trace_id
ORDER BY m.last DESC, m.trace_id ASC
