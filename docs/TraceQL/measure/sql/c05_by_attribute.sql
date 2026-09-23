WITH 1790084801000000000 AS s, 1790095601000000000 AS e,
    (SELECT groupArray(trace_id) FROM (SELECT trace_id, max(start_ns) AS last FROM tqd_g1.spans
      WHERE start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985 AND service = 'checkout' GROUP BY trace_id ORDER BY last DESC, trace_id ASC LIMIT 20)) AS ids
SELECT trace_id, grp, count() AS matched, max(start_ns) AS last,
       arraySlice(arraySort(x -> (x.2, x.1), groupArray((span_id, start_ns, duration_ns))), 1, 3) AS spans
FROM (SELECT trace_id, span_id, start_ns, duration_ns, toString(attrs.`rpc%2Emethod`) AS grp
      FROM tqd_g1.spans
      WHERE (intDiv(start_ns, 300000000000), trace_id) IN (SELECT (arrayJoin(range(toInt64(5966949), toInt64(5966985) + 1)), arrayJoin(ids)))
        AND start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985 AND service = 'checkout')
GROUP BY trace_id, grp
ORDER BY last DESC, trace_id ASC, grp ASC
