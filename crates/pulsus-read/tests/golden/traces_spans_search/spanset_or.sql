-- case: spanset_or
-- q: { resource.service.name = "frontend" } || { span.http.response.status_code >= 500 } limit=20 spss=3
WITH (SELECT (groupArray(trace_id), groupArray(keys))
      FROM (SELECT trace_id, max(f1) AS c1, max(f2) AS c2, greatest(maxIf(start_ns, f1), maxIf(start_ns, f2)) AS last,
                   groupUniqArray(intDiv(start_ns, 300000000000)) AS keys
            FROM (SELECT trace_id, start_ns, ((service_type = 'string' AND service = 'frontend')) AS f1, ((coalesce(attrs.`http%2Eresponse%2Estatus_code`.:Int64 >= 500, false) OR coalesce(attrs.`http%2Eresponse%2Estatus_code`.:Float64 >= 500, false))) AS f2
                  FROM spans
                  WHERE start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000
                    AND intDiv(start_ns, 300000000000) BETWEEN intDiv(1790084801000000000, 300000000000) AND intDiv(1790095600999999999, 300000000000)
                    AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') >= toDate('2026-09-22') AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') <= toDate('2026-09-22')
                    AND (f1 OR f2))
            GROUP BY trace_id
            HAVING (c1 OR c2)
            ORDER BY last DESC, trace_id ASC
            LIMIT 20)) AS top
SELECT m.trace_id AS trace_id, t.root_service AS root_service, t.root_name AS root_name,
       t.start_ns AS start_ns, t.end_ns - t.start_ns AS duration_ns,
       m.last AS last, m.matched AS matched, m.spans AS spans
FROM (SELECT trace_id, arrayMax(arrayMap(x -> x.2, sp)) AS last, length(sp) AS matched,
             arraySlice(arraySort(x -> (x.2, x.1), sp), 1, 3) AS spans
      FROM (SELECT trace_id, max(f1) AS c1, max(f2) AS c2,
                   arrayMap(x -> (x.1, x.2, x.3, x.4, x.5),
                            arrayFilter(x -> x.6 OR x.7,
                                        groupArray((span_id, start_ns, duration_ns, service, proj, f1, f2)))) AS sp
            FROM (SELECT trace_id, span_id, start_ns, duration_ns, service, arrayFilter(x -> x.1 != 0, [multiIf((service_type = 'string' AND service = 'frontend'), (1, if(length(toString(service)) <= 8192, toString(service), substringUTF8(toString(service), 1, 2048)), 'String'), (0, '', '')), multiIf((coalesce(attrs.`http%2Eresponse%2Estatus_code`.:Int64 >= 500, false) OR coalesce(attrs.`http%2Eresponse%2Estatus_code`.:Float64 >= 500, false)), (2, if(startsWith(dynamicType(attrs.`http%2Eresponse%2Estatus_code`), 'Array'), toJSONString(arrayMap(x -> (toString(dynamicType(x)), toString(x)), CAST(attrs.`http%2Eresponse%2Estatus_code`, 'Array(Dynamic)'))), if(length(toString(attrs.`http%2Eresponse%2Estatus_code`)) <= 8192, toString(attrs.`http%2Eresponse%2Estatus_code`), substringUTF8(toString(attrs.`http%2Eresponse%2Estatus_code`), 1, 2048))), toString(dynamicType(attrs.`http%2Eresponse%2Estatus_code`))), (0, '', ''))]) AS proj, ((service_type = 'string' AND service = 'frontend')) AS f1, ((coalesce(attrs.`http%2Eresponse%2Estatus_code`.:Int64 >= 500, false) OR coalesce(attrs.`http%2Eresponse%2Estatus_code`.:Float64 >= 500, false))) AS f2
                  FROM spans
                  WHERE (intDiv(start_ns, 300000000000), trace_id) IN
                        (SELECT arrayJoin(arrayFlatten(arrayMap((t, ks) -> arrayMap(k -> (k, t), ks), top.1, top.2))))
                    AND start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000
                    AND intDiv(start_ns, 300000000000) BETWEEN intDiv(1790084801000000000, 300000000000) AND intDiv(1790095600999999999, 300000000000)
                    AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') >= toDate('2026-09-22') AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') <= toDate('2026-09-22')
                    AND (f1 OR f2))
            GROUP BY trace_id)) AS m
LEFT JOIN (SELECT trace_id, min(start_ns) AS start_ns, max(end_ns) AS end_ns,
                  min(root) AS r, if(r.1 = 0, if(length(r.4) <= 8192, r.4, substringUTF8(r.4, 1, 2048)), '') AS root_service, if(r.1 = 0, if(length(r.5) <= 8192, r.5, substringUTF8(r.5, 1, 2048)), '') AS root_name
           FROM traces
           WHERE trace_id IN (SELECT arrayJoin(top.1))
           GROUP BY trace_id) AS t USING trace_id
ORDER BY last DESC, trace_id ASC
