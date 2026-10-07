-- case: literal_and_stored
-- q: { span.app.user.id = "u-10013" || span.app.discount.ratio > 0.45 } limit=20 spss=3
WITH (SELECT (groupArray(trace_id), groupArray(keys))
      FROM (SELECT trace_id, max(start_ns) AS last,
                   groupUniqArray(intDiv(start_ns, 300000000000)) AS keys
            FROM spans
            WHERE start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000
              AND intDiv(start_ns, 300000000000) BETWEEN intDiv(1790084801000000000, 300000000000) AND intDiv(1790095600999999999, 300000000000)
              AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') >= toDate('2026-09-22') AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') <= toDate('2026-09-22')
              AND (((coalesce(attrs.`app%2Euser%2Eid`.:String = 'u-10013', false) OR has(attrs.`app%2Euser%2Eid`.:`Array(Nullable(String))`, 'u-10013'))) OR ((coalesce(attrs.`app%2Ediscount%2Eratio`.:Int64 > 0.45, false) OR coalesce(attrs.`app%2Ediscount%2Eratio`.:Float64 > 0.45, false))))
            GROUP BY trace_id
            ORDER BY last DESC, trace_id ASC
            LIMIT 20)) AS top
SELECT m.trace_id AS trace_id, t.root_service AS root_service, t.root_name AS root_name,
       t.start_ns AS start_ns, t.end_ns - t.start_ns AS duration_ns,
       m.last AS last, m.matched AS matched, m.spans AS spans
FROM (SELECT trace_id, max(start_ns) AS last, count() AS matched,
             arraySlice(arraySort(x -> (x.2, x.1),
                        groupArray((span_id, start_ns, duration_ns, service, arrayFilter(x -> x.1 != 0, [multiIf((coalesce(attrs.`app%2Euser%2Eid`.:String = 'u-10013', false) OR has(attrs.`app%2Euser%2Eid`.:`Array(Nullable(String))`, 'u-10013')), (1, if(length('u-10013') <= 8192, 'u-10013', substringUTF8('u-10013', 1, 2048)), 'String'), (0, '', '')), multiIf((coalesce(attrs.`app%2Ediscount%2Eratio`.:Int64 > 0.45, false) OR coalesce(attrs.`app%2Ediscount%2Eratio`.:Float64 > 0.45, false)), (2, if(startsWith(dynamicType(attrs.`app%2Ediscount%2Eratio`), 'Array'), toJSONString(arrayMap(x -> (toString(dynamicType(x)), toString(x)), CAST(attrs.`app%2Ediscount%2Eratio`, 'Array(Dynamic)'))), if(length(toString(attrs.`app%2Ediscount%2Eratio`)) <= 8192, toString(attrs.`app%2Ediscount%2Eratio`), substringUTF8(toString(attrs.`app%2Ediscount%2Eratio`), 1, 2048))), toString(dynamicType(attrs.`app%2Ediscount%2Eratio`))), (0, '', ''))])))), 1, 3) AS spans
      FROM spans
      WHERE (intDiv(start_ns, 300000000000), trace_id) IN
            (SELECT arrayJoin(arrayFlatten(arrayMap((t, ks) -> arrayMap(k -> (k, t), ks), top.1, top.2))))
        AND start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000
        AND intDiv(start_ns, 300000000000) BETWEEN intDiv(1790084801000000000, 300000000000) AND intDiv(1790095600999999999, 300000000000)
        AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') >= toDate('2026-09-22') AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') <= toDate('2026-09-22')
        AND (((coalesce(attrs.`app%2Euser%2Eid`.:String = 'u-10013', false) OR has(attrs.`app%2Euser%2Eid`.:`Array(Nullable(String))`, 'u-10013'))) OR ((coalesce(attrs.`app%2Ediscount%2Eratio`.:Int64 > 0.45, false) OR coalesce(attrs.`app%2Ediscount%2Eratio`.:Float64 > 0.45, false))))
      GROUP BY trace_id) AS m
LEFT JOIN (SELECT trace_id, min(start_ns) AS start_ns, max(end_ns) AS end_ns,
                  min(root) AS r, if(r.1 = 0, if(length(r.4) <= 8192, r.4, substringUTF8(r.4, 1, 2048)), '') AS root_service, if(r.1 = 0, if(length(r.5) <= 8192, r.5, substringUTF8(r.5, 1, 2048)), '') AS root_name
           FROM traces
           WHERE trace_id IN (SELECT arrayJoin(top.1))
           GROUP BY trace_id) AS t USING trace_id
ORDER BY last DESC, trace_id ASC
