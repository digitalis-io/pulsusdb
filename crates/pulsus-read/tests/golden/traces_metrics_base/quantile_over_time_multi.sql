-- case: quantile_over_time_multi
-- q: {} | quantile_over_time(duration, 0.5, 0.9, 0.99)

== range (query_range) ==
SELECT t, CAST(quantilesTDigest(0.5, 0.9, 0.99)(val) AS Array(Float64)) AS qs
FROM (
  SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns), INTERVAL 60000 MILLISECOND)) AS t, trace_id, span_id,
         any(duration_ns) AS val
  FROM trace_spans
  WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
  GROUP BY t, trace_id, span_id
)
GROUP BY t
ORDER BY t ASC

== instant (query) ==
SELECT CAST(quantilesTDigest(0.5, 0.9, 0.99)(val) AS Array(Float64)) AS qs
FROM (
  SELECT trace_id, span_id, any(duration_ns) AS val
  FROM trace_spans
  WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
  GROUP BY trace_id, span_id
)
