-- case: docs_histogram_worked_example
-- q: { resource.service.name = "checkout" } | histogram_over_time(duration)

== range (query_range) ==
SELECT t, toUInt64(roundToExp2(val - 1)) * 2 AS bucket, count() AS n
FROM (
  SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns), INTERVAL 60000 MILLISECOND)) AS t, trace_id, span_id,
         any(duration_ns) AS val
  FROM trace_spans
  PREWHERE service = 'checkout'
  WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
  GROUP BY t, trace_id, span_id
)
WHERE val >= 2
GROUP BY t, bucket
ORDER BY t ASC, bucket ASC

== instant (query) ==
SELECT toUInt64(roundToExp2(val - 1)) * 2 AS bucket, count() AS n
FROM (
  SELECT trace_id, span_id, any(duration_ns) AS val
  FROM trace_spans
  PREWHERE service = 'checkout'
  WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
  GROUP BY trace_id, span_id
)
WHERE val >= 2
GROUP BY bucket
ORDER BY bucket ASC
