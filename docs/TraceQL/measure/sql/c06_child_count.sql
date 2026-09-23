WITH 1790084801000000000 AS s, 1790095601000000000 AS e
SELECT trace_id, span_id, children
FROM (SELECT trace_id, span_id, start_ns,
             countIf(1) OVER (PARTITION BY trace_id, parent_span_id) AS siblings
      FROM tqd_g1.spans WHERE start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985) AS x
INNER JOIN (SELECT trace_id, parent_span_id AS span_id, count() AS children
            FROM tqd_g1.spans WHERE start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985 AND parent_span_id != toFixedString('', 8)
            GROUP BY trace_id, parent_span_id
            HAVING children > 3) AS c USING (trace_id, span_id)
ORDER BY start_ns DESC, span_id ASC
LIMIT 20
