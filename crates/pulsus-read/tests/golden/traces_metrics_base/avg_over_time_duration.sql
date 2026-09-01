-- case: avg_over_time_duration
-- q: {} | avg_over_time(duration)

== range (query_range) ==
SELECT t, toFloat64(avg(val)) AS v
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
SELECT toFloat64(avg(val)) AS v
FROM (
  SELECT trace_id, span_id, any(duration_ns) AS val
  FROM trace_spans
  WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
  GROUP BY trace_id, span_id
)
