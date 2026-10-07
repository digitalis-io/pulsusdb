-- case: match_all
-- q: {} limit=20 spss=3
WITH (SELECT (groupArray(trace_id), groupArray(keys))
      FROM (SELECT trace_id, max(start_ns) AS last,
                   groupUniqArray(intDiv(start_ns, 300000000000)) AS keys
            FROM spans
            WHERE start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000
              AND intDiv(start_ns, 300000000000) BETWEEN intDiv(1790084801000000000, 300000000000) AND intDiv(1790095600999999999, 300000000000)
              AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') >= toDate('2026-09-22') AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') <= toDate('2026-09-22')
              AND (true)
            GROUP BY trace_id
            ORDER BY last DESC, trace_id ASC
            LIMIT 20)) AS top
SELECT m.trace_id AS trace_id, t.root_service AS root_service, t.root_name AS root_name,
       t.start_ns AS start_ns, t.end_ns - t.start_ns AS duration_ns,
       m.last AS last, m.matched AS matched, m.spans AS spans
FROM (SELECT trace_id, max(start_ns) AS last, count() AS matched,
             arraySlice(arraySort(x -> (x.2, x.1),
                        groupArray((span_id, start_ns, duration_ns, service, CAST([], 'Array(Tuple(UInt8, String, String))')))), 1, 3) AS spans
      FROM spans
      WHERE (intDiv(start_ns, 300000000000), trace_id) IN
            (SELECT arrayJoin(arrayFlatten(arrayMap((t, ks) -> arrayMap(k -> (k, t), ks), top.1, top.2))))
        AND start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000
        AND intDiv(start_ns, 300000000000) BETWEEN intDiv(1790084801000000000, 300000000000) AND intDiv(1790095600999999999, 300000000000)
        AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') >= toDate('2026-09-22') AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') <= toDate('2026-09-22')
        AND (true)
      GROUP BY trace_id) AS m
LEFT JOIN (SELECT trace_id, min(start_ns) AS start_ns, max(end_ns) AS end_ns,
                  min(root) AS r, if(r.1 = 0, if(length(r.4) <= 8192, r.4, substringUTF8(r.4, 1, 2048)), '') AS root_service, if(r.1 = 0, if(length(r.5) <= 8192, r.5, substringUTF8(r.5, 1, 2048)), '') AS root_name
           FROM traces
           WHERE trace_id IN (SELECT arrayJoin(top.1))
           GROUP BY trace_id) AS t USING trace_id
ORDER BY last DESC, trace_id ASC
