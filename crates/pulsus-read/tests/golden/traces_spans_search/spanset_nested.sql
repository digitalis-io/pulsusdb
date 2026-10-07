-- case: spanset_nested
-- q: { span.a = 1 } || { span.b = 2 } && { span.c = 3 } limit=20 spss=3
WITH (SELECT (groupArray(trace_id), groupArray(keys))
      FROM (SELECT trace_id, max(f1) AS c1, max(f2) AS c2, max(f3) AS c3, greatest(if(((c1 OR c2) AND c3), maxIf(start_ns, f1), 0), if(((c1 OR c2) AND c3), maxIf(start_ns, f2), 0), if(((c1 OR c2) AND c3), maxIf(start_ns, f3), 0)) AS last,
                   groupUniqArray(intDiv(start_ns, 300000000000)) AS keys
            FROM (SELECT trace_id, start_ns, ((coalesce(attrs.`a`.:Int64 = 1, false) OR coalesce(attrs.`a`.:Float64 = 1, false))) AS f1, ((coalesce(attrs.`b`.:Int64 = 2, false) OR coalesce(attrs.`b`.:Float64 = 2, false))) AS f2, ((coalesce(attrs.`c`.:Int64 = 3, false) OR coalesce(attrs.`c`.:Float64 = 3, false))) AS f3
                  FROM spans
                  WHERE start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000
                    AND intDiv(start_ns, 300000000000) BETWEEN intDiv(1790084801000000000, 300000000000) AND intDiv(1790095600999999999, 300000000000)
                    AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') >= toDate('2026-09-22') AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') <= toDate('2026-09-22')
                    AND (f1 OR f2 OR f3))
            GROUP BY trace_id
            HAVING ((c1 OR c2) AND c3)
            ORDER BY last DESC, trace_id ASC
            LIMIT 20)) AS top
SELECT m.trace_id AS trace_id, t.root_service AS root_service, t.root_name AS root_name,
       t.start_ns AS start_ns, t.end_ns - t.start_ns AS duration_ns,
       m.last AS last, m.matched AS matched, m.spans AS spans
FROM (SELECT trace_id, arrayMax(arrayMap(x -> x.2, sp)) AS last, length(sp) AS matched,
             arraySlice(arraySort(x -> (x.2, x.1), sp), 1, 3) AS spans
      FROM (SELECT trace_id, max(f1) AS c1, max(f2) AS c2, max(f3) AS c3,
                   arrayMap(x -> (x.1, x.2, x.3, x.4, x.5),
                            arrayFilter(x -> (x.6 AND ((c1 OR c2) AND c3)) OR (x.7 AND ((c1 OR c2) AND c3)) OR (x.8 AND ((c1 OR c2) AND c3)),
                                        groupArray((span_id, start_ns, duration_ns, service, proj, f1, f2, f3)))) AS sp
            FROM (SELECT trace_id, span_id, start_ns, duration_ns, service, arrayFilter(x -> x.1 != 0, [multiIf((coalesce(attrs.`a`.:Int64 = 1, false) OR coalesce(attrs.`a`.:Float64 = 1, false)), (1, if(startsWith(dynamicType(attrs.`a`), 'Array'), toJSONString(arrayMap(x -> (toString(dynamicType(x)), toString(x)), CAST(attrs.`a`, 'Array(Dynamic)'))), if(length(toString(attrs.`a`)) <= 8192, toString(attrs.`a`), substringUTF8(toString(attrs.`a`), 1, 2048))), toString(dynamicType(attrs.`a`))), (0, '', '')), multiIf((coalesce(attrs.`b`.:Int64 = 2, false) OR coalesce(attrs.`b`.:Float64 = 2, false)), (2, if(startsWith(dynamicType(attrs.`b`), 'Array'), toJSONString(arrayMap(x -> (toString(dynamicType(x)), toString(x)), CAST(attrs.`b`, 'Array(Dynamic)'))), if(length(toString(attrs.`b`)) <= 8192, toString(attrs.`b`), substringUTF8(toString(attrs.`b`), 1, 2048))), toString(dynamicType(attrs.`b`))), (0, '', '')), multiIf((coalesce(attrs.`c`.:Int64 = 3, false) OR coalesce(attrs.`c`.:Float64 = 3, false)), (3, if(startsWith(dynamicType(attrs.`c`), 'Array'), toJSONString(arrayMap(x -> (toString(dynamicType(x)), toString(x)), CAST(attrs.`c`, 'Array(Dynamic)'))), if(length(toString(attrs.`c`)) <= 8192, toString(attrs.`c`), substringUTF8(toString(attrs.`c`), 1, 2048))), toString(dynamicType(attrs.`c`))), (0, '', ''))]) AS proj, ((coalesce(attrs.`a`.:Int64 = 1, false) OR coalesce(attrs.`a`.:Float64 = 1, false))) AS f1, ((coalesce(attrs.`b`.:Int64 = 2, false) OR coalesce(attrs.`b`.:Float64 = 2, false))) AS f2, ((coalesce(attrs.`c`.:Int64 = 3, false) OR coalesce(attrs.`c`.:Float64 = 3, false))) AS f3
                  FROM spans
                  WHERE (intDiv(start_ns, 300000000000), trace_id) IN
                        (SELECT arrayJoin(arrayFlatten(arrayMap((t, ks) -> arrayMap(k -> (k, t), ks), top.1, top.2))))
                    AND start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000
                    AND intDiv(start_ns, 300000000000) BETWEEN intDiv(1790084801000000000, 300000000000) AND intDiv(1790095600999999999, 300000000000)
                    AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') >= toDate('2026-09-22') AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') <= toDate('2026-09-22')
                    AND (f1 OR f2 OR f3))
            GROUP BY trace_id)) AS m
LEFT JOIN (SELECT trace_id, min(start_ns) AS start_ns, max(end_ns) AS end_ns,
                  min(root) AS r, if(r.1 = 0, if(length(r.4) <= 8192, r.4, substringUTF8(r.4, 1, 2048)), '') AS root_service, if(r.1 = 0, if(length(r.5) <= 8192, r.5, substringUTF8(r.5, 1, 2048)), '') AS root_name
           FROM traces
           WHERE trace_id IN (SELECT arrayJoin(top.1))
           GROUP BY trace_id) AS t USING trace_id
ORDER BY last DESC, trace_id ASC
