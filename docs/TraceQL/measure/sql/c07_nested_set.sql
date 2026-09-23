WITH RECURSIVE
    seed AS (SELECT trace_id, span_id, parent_span_id, start_ns, 0 AS depth
             FROM tqd_g1.spans
             WHERE (intDiv(start_ns, 300000000000), trace_id) IN
                   (SELECT (arrayJoin(range(toInt64(5966949), toInt64(5966985) + 1)), toFixedString(unhex('50FB0CD99260AC2A15D0A6F208126742'), 16)))
               AND start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985 AND parent_span_id = toFixedString('', 8)
             UNION ALL
             SELECT c.trace_id, c.span_id, c.parent_span_id, c.start_ns, p.depth + 1
             FROM tqd_g1.spans AS c
             INNER JOIN seed AS p ON p.trace_id = c.trace_id AND p.span_id = c.parent_span_id
             WHERE (intDiv(c.start_ns, 300000000000), c.trace_id) IN
                   (SELECT (arrayJoin(range(toInt64(5966949), toInt64(5966985) + 1)), toFixedString(unhex('50FB0CD99260AC2A15D0A6F208126742'), 16)))
               AND c.start_ns >= 1790084801000000000 AND c.start_ns < 1790095601000000000)
SELECT depth, count() AS spans FROM seed GROUP BY depth ORDER BY depth
