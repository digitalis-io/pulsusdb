WITH 1790084801000000000 AS s, 1790095601000000000 AS e
SELECT trace_id, arrayMax(arrayMap(x -> x.2, hit)) AS last, length(hit) AS matched,
       arraySlice(arraySort(x -> (x.2, x.1), hit), 1, 3) AS spans
FROM (SELECT trace_id,
             groupArrayIf(span_id, service = 'checkout') AS aids,
             arrayFilter(x -> has(aids, x.4), groupArrayIf((span_id, start_ns, duration_ns, parent_span_id), service = 'payment')) AS hit
      FROM tqd_g1.spans
      WHERE start_ns >= s AND start_ns < e AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985 AND ((service = 'checkout') OR (service = 'payment'))
      GROUP BY trace_id
      HAVING length(hit) > 0)
ORDER BY last DESC, trace_id ASC
LIMIT 20
