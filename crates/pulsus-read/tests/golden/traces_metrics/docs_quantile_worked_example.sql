-- case: docs_quantile_worked_example
-- q: { resource.service.name = "checkout" } | quantile_over_time(duration, 0.5, 0.9, 0.99, 1.0)

== range (query_range) ==
SELECT t, CAST(quantilesTDigest(0.5, 0.9, 0.99, 1)(val) AS Array(Float64)) AS qs
FROM (
  SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns - 1), INTERVAL 60000000000 NANOSECOND)) + 60000 AS t, trace_id, span_id,
         any(duration_ns) AS val
  FROM trace_spans
  PREWHERE service = 'checkout'
  WHERE timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
  GROUP BY t, trace_id, span_id
)
GROUP BY t
ORDER BY t ASC

== instant (query) ==
SELECT CAST(quantilesTDigest(0.5, 0.9, 0.99, 1)(val) AS Array(Float64)) AS qs
FROM (
  SELECT trace_id, span_id, any(duration_ns) AS val
  FROM trace_spans
  PREWHERE service = 'checkout'
  WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
  GROUP BY trace_id, span_id
)

== exemplars ==
SELECT t, ex, CAST(quantilesTDigestMerge(0.5, 0.9, 0.99, 1)(st) OVER () AS Array(Float64)) AS qs
FROM (
  SELECT t, groupArraySample(1, 1)(tuple(trace_id, ts, val)) AS ex,
         quantilesTDigestState(0.5, 0.9, 0.99, 1)(val) AS st
  FROM (
    SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns - 1), INTERVAL 60000000000 NANOSECOND)) + 60000 AS t, trace_id, span_id,
           any(duration_ns) AS val, any(timestamp_ns) AS ts
    FROM trace_spans
    PREWHERE service = 'checkout'
    WHERE timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
    GROUP BY t, trace_id, span_id
  )
  GROUP BY t
)
ORDER BY t ASC
