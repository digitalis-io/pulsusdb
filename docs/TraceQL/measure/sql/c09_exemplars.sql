SELECT series, groupArray((t, v)) AS points, groupArray((t, ex_trace, ex_span, ex_v)) AS exemplars
FROM (SELECT service AS series, (intDiv(start_ns - 1, 60000000000) + 1) * 60000 AS t, count() AS v,
             argMax(lower(hex(trace_id)), start_ns) AS ex_trace,
             argMax(lower(hex(span_id)), start_ns) AS ex_span,
             toFloat64(argMax(duration_ns, start_ns)) AS ex_v
      FROM tqd_g1.spans
      WHERE start_ns >= 1790084760000000000 AND start_ns < 1790095620000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985
      GROUP BY series, t
      ORDER BY t)
GROUP BY series ORDER BY series
