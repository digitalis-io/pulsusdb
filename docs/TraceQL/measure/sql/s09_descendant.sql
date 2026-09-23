WITH RECURSIVE
    1790084801000000000 AS s, 1790095601000000000 AS e,
    -- candidates: traces with at least one A span and one B span in the window,
    -- with the services and extent that locate all their spans by key
    (SELECT groupArray((trace_id, [], ts, te))
     FROM (SELECT trace_id, min(start_ns) AS ts, max(end_ns) AS te
           FROM tqd_g1.traces
           WHERE trace_id IN (SELECT trace_id FROM tqd_g1.spans
                              WHERE start_ns >= s AND start_ns < e AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985 AND ((service = 'frontend') OR (service = 'payment' AND status_code = 2))
                              GROUP BY trace_id
                              HAVING countIf(service = 'frontend') > 0 AND countIf(service = 'payment' AND status_code = 2) > 0)
           GROUP BY trace_id)) AS cand,
    -- one row per (B span, ancestor reached so far); a row stops when it reaches an A span or the root
    climb AS (
        SELECT trace_id, span_id AS b, start_ns AS b_start, duration_ns AS b_dur, parent_span_id AS cur,
               toUInt8(0) AS found, 0 AS depth
        FROM tqd_g1.spans
        WHERE (intDiv(start_ns, 300000000000), trace_id) IN (SELECT arrayJoin(arrayFlatten(arrayMap(c -> arrayMap(k -> (k, c.1), range(toInt64(intDiv(greatest(c.3, s), 300000000000)), toInt64(intDiv(least(c.4, e), 300000000000)) + 1)), cand))))
          AND start_ns >= s AND start_ns < e AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985 AND (service = 'payment' AND status_code = 2)
        UNION ALL
        SELECT c.trace_id, c.b, c.b_start, c.b_dur, x.parent_span_id, toUInt8(x.a), c.depth + 1
        FROM climb AS c
        INNER JOIN (SELECT trace_id, span_id, parent_span_id, (service = 'frontend') AS a
                    FROM tqd_g1.spans
                    WHERE (intDiv(start_ns, 300000000000), trace_id) IN (SELECT arrayJoin(arrayFlatten(arrayMap(c -> arrayMap(k -> (k, c.1), range(toInt64(intDiv(greatest(c.3, s), 300000000000)), toInt64(intDiv(least(c.4, e), 300000000000)) + 1)), cand))))
                      AND start_ns >= s AND start_ns < e AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985) AS x
            ON x.trace_id = c.trace_id AND x.span_id = c.cur
        WHERE c.found = 0 AND c.depth < 64)
SELECT trace_id, last, matched, spans, unresolved
FROM (SELECT trace_id, max(b_start) AS last, count() AS matched,
             arraySlice(arraySort(x -> (x.2, x.1), groupArray((b, b_start, b_dur))), 1, 3) AS spans,
             0 AS unresolved
      FROM (SELECT DISTINCT trace_id, b, b_start, b_dur FROM climb WHERE found = 1)
      GROUP BY trace_id
      UNION ALL
      -- a climb that reached the bound with a parent still to visit did not
      -- finish: the reader turns any such row into 422 rather than answering
      SELECT trace_id, toInt64(0), toUInt64(0), [], count() AS unresolved
      FROM climb
      WHERE found = 0 AND depth >= 64 AND cur != toFixedString('', 8)
      GROUP BY trace_id)
ORDER BY unresolved DESC, last DESC, trace_id ASC
LIMIT 20
