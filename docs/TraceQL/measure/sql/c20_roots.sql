WITH 1790084801000000000 AS s, 1790095601000000000 AS e
SELECT trace_id, span_id, start_ns, duration_ns
FROM tqd_g1.spans AS x
WHERE start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985
  AND (x.parent_span_id = toFixedString('', 8)
       OR (x.trace_id, x.parent_span_id) NOT IN
          (SELECT trace_id, span_id FROM tqd_g1.spans WHERE start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985))
ORDER BY start_ns DESC
LIMIT 20
