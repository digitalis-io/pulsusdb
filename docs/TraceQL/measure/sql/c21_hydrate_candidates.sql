WITH 1790084801000000000 AS s, 1790095601000000000 AS e,
    (SELECT (groupArray(trace_id), groupArray(keys))
     FROM (SELECT trace_id, max(start_ns) AS last,
                  groupUniqArray(intDiv(start_ns, 300000000000)) AS keys
           FROM tqd_g1.spans
           WHERE start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985 AND service = 'checkout'
           GROUP BY trace_id
           ORDER BY last DESC, trace_id ASC
           LIMIT 20)) AS top
SELECT trace_id, span_id, parent_span_id, start_ns, duration_ns
FROM tqd_g1.spans
WHERE (intDiv(start_ns, 300000000000), trace_id) IN
      (SELECT arrayJoin(arrayFlatten(arrayMap((t, ks) -> arrayMap(k -> (k, t), ks), top.1, top.2))))
  AND start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985
