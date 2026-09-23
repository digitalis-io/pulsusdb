WITH
    1790084801000000000 AS s, 1790095601000000000 AS e,
    (SELECT (groupArray(trace_id), groupArray(keys))
     FROM (SELECT trace_id, max(start_ns) AS last,
                  groupUniqArray(intDiv(start_ns, 300000000000)) AS keys
           FROM tqd_g1.spans
           WHERE start_ns >= s AND start_ns < e AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985
             AND (coalesce(attrs.`http%2Erequest%2Ebody%2Esize`.:Int64, 0) + coalesce(attrs.`app%2Eitems%2Ecount`.:Int64, 0) > 4000)
           GROUP BY trace_id
           ORDER BY last DESC, trace_id ASC
           LIMIT 20)) AS top
SELECT m.trace_id, t.root_service, t.root_name, t.start_ns, t.end_ns - t.start_ns AS trace_duration_ns,
       m.last, m.matched, m.spans
FROM (SELECT trace_id, max(start_ns) AS last, count() AS matched,
             arraySlice(arraySort(x -> (x.2, x.1), groupArray((span_id, start_ns, duration_ns, attrs.`http%2Erequest%2Ebody%2Esize`, attrs.`app%2Eitems%2Ecount`))), 1, 3) AS spans
      FROM tqd_g1.spans
      WHERE (intDiv(start_ns, 300000000000), trace_id) IN
            (SELECT arrayJoin(arrayFlatten(arrayMap((t, ks) -> arrayMap(k -> (k, t), ks), top.1, top.2))))
        AND start_ns >= s AND start_ns < e AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985
        AND (coalesce(attrs.`http%2Erequest%2Ebody%2Esize`.:Int64, 0) + coalesce(attrs.`app%2Eitems%2Ecount`.:Int64, 0) > 4000)
      GROUP BY trace_id) AS m
LEFT JOIN (SELECT trace_id, min(start_ns) AS start_ns, max(end_ns) AS end_ns,
                  max(root_service) AS root_service, max(root_name) AS root_name
           FROM tqd_g1.traces
           WHERE trace_id IN (SELECT arrayJoin(top.1))
           GROUP BY trace_id) AS t USING trace_id
ORDER BY m.last DESC, m.trace_id ASC
