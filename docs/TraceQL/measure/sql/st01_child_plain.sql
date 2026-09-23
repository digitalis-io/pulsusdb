WITH 1790084801000000000 AS s, 1790095601000000000 AS e
SELECT trace_id, arrayMax(arrayMap(x -> x.2, hit)) AS last, length(hit) AS matched,
       arraySlice(arraySort(x -> (x.2, x.1), hit), 1, 3) AS spans
FROM (SELECT trace_id,
             groupArrayIf(span_id, service = 'frontend') AS a_ids,
             groupArrayIf(parent_span_id, service = 'frontend') AS a_par,
             groupArrayIf((span_id, start_ns, duration_ns, parent_span_id), service = 'frontend') AS a_spans,
             groupArrayIf((span_id, start_ns, duration_ns, parent_span_id), service = 'payment' AND status_code = 2) AS b_spans,
             arrayFilter(x -> has(a_ids, x.4), b_spans) AS b_hit,
             b_hit AS hit
      FROM tqd_g1.spans
      WHERE start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985 AND ((service = 'frontend') OR (service = 'payment' AND status_code = 2))
      GROUP BY trace_id
      HAVING length(hit) > 0)
ORDER BY last DESC, trace_id ASC
LIMIT 20
