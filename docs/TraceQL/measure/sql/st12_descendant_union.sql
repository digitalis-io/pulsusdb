WITH RECURSIVE (SELECT groupArray((trace_id, ts, te)) FROM
        (SELECT trace_id, min(start_ns) AS ts, max(end_ns) AS te FROM tqd_g1.traces
         WHERE trace_id IN (SELECT trace_id FROM tqd_g1.spans WHERE start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985 AND ((service = 'frontend') OR (service = 'payment' AND status_code = 2))
                            GROUP BY trace_id HAVING countIf(service = 'frontend') > 0 AND countIf(service = 'payment' AND status_code = 2) > 0)
         GROUP BY trace_id)) AS cand,
    climb AS (
        SELECT trace_id, span_id AS seed, parent_span_id AS cur, 0 AS depth
        FROM tqd_g1.spans WHERE (intDiv(start_ns, 300000000000), trace_id) IN (SELECT arrayJoin(arrayFlatten(arrayMap(c -> arrayMap(k -> (k, c.1), range(toInt64(intDiv(greatest(c.2, 1790084801000000000), 300000000000)), toInt64(intDiv(least(c.3, 1790095601000000000 - 1), 300000000000)) + 1)), cand)))) AND start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985 AND (service = 'payment' AND status_code = 2)
        UNION ALL
        SELECT c.trace_id, c.seed, x.parent_span_id, c.depth + 1
        FROM climb AS c
        INNER JOIN (SELECT trace_id, span_id, parent_span_id FROM tqd_g1.spans WHERE (intDiv(start_ns, 300000000000), trace_id) IN (SELECT arrayJoin(arrayFlatten(arrayMap(c -> arrayMap(k -> (k, c.1), range(toInt64(intDiv(greatest(c.2, 1790084801000000000), 300000000000)), toInt64(intDiv(least(c.3, 1790095601000000000 - 1), 300000000000)) + 1)), cand)))) AND start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985) AS x
            ON x.trace_id = c.trace_id AND x.span_id = c.cur
        WHERE c.depth < 64 - 1),
    pairs AS (SELECT DISTINCT c.trace_id AS trace_id, c.seed AS seed, c.cur AS other
              FROM climb AS c
              INNER JOIN (SELECT trace_id, span_id FROM tqd_g1.spans WHERE (intDiv(start_ns, 300000000000), trace_id) IN (SELECT arrayJoin(arrayFlatten(arrayMap(c -> arrayMap(k -> (k, c.1), range(toInt64(intDiv(greatest(c.2, 1790084801000000000), 300000000000)), toInt64(intDiv(least(c.3, 1790095601000000000 - 1), 300000000000)) + 1)), cand)))) AND start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985 AND (service = 'frontend')) AS r
                  ON r.trace_id = c.trace_id AND r.span_id = c.cur),
    overflow AS (SELECT count() AS unresolved
                 FROM climb AS c
                 INNER JOIN (SELECT trace_id, span_id, parent_span_id FROM tqd_g1.spans WHERE (intDiv(start_ns, 300000000000), trace_id) IN (SELECT arrayJoin(arrayFlatten(arrayMap(c -> arrayMap(k -> (k, c.1), range(toInt64(intDiv(greatest(c.2, 1790084801000000000), 300000000000)), toInt64(intDiv(least(c.3, 1790095601000000000 - 1), 300000000000)) + 1)), cand)))) AND start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985) AS x
                     ON x.trace_id = c.trace_id AND x.span_id = c.cur
                 WHERE c.depth = 64 - 1 AND x.parent_span_id != toFixedString('', 8))
SELECT * FROM (
    (SELECT 'match' AS row_kind, trace_id, max(start_ns) AS last, count() AS matched,
            arraySlice(arraySort(x -> (x.2, x.1), groupArray((span_id, start_ns, duration_ns))), 1, 3) AS spans,
            toUInt64(0) AS unresolved
     FROM (SELECT trace_id, span_id, start_ns, duration_ns FROM (SELECT trace_id, span_id, start_ns, duration_ns FROM tqd_g1.spans
              WHERE (intDiv(start_ns, 300000000000), trace_id) IN (SELECT arrayJoin(arrayFlatten(arrayMap(c -> arrayMap(k -> (k, c.1), range(toInt64(intDiv(greatest(c.2, 1790084801000000000), 300000000000)), toInt64(intDiv(least(c.3, 1790095601000000000 - 1), 300000000000)) + 1)), cand)))) AND start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985 AND (service = 'payment' AND status_code = 2) AND (trace_id, span_id) IN (SELECT trace_id, seed FROM pairs))
              UNION ALL
              SELECT trace_id, span_id, start_ns, duration_ns FROM (SELECT trace_id, span_id, start_ns, duration_ns FROM tqd_g1.spans
              WHERE (intDiv(start_ns, 300000000000), trace_id) IN (SELECT arrayJoin(arrayFlatten(arrayMap(c -> arrayMap(k -> (k, c.1), range(toInt64(intDiv(greatest(c.2, 1790084801000000000), 300000000000)), toInt64(intDiv(least(c.3, 1790095601000000000 - 1), 300000000000)) + 1)), cand)))) AND start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985 AND (service = 'frontend') AND (trace_id, span_id) IN (SELECT trace_id, other FROM pairs)))
     GROUP BY trace_id
     ORDER BY last DESC, trace_id ASC
     LIMIT 20)
    UNION ALL
    (SELECT 'overflow', toFixedString('', 16), toInt64(0), toUInt64(0),
            CAST([], 'Array(Tuple(FixedString(8), Int64, Int64))'),
            (SELECT unresolved FROM overflow)))
