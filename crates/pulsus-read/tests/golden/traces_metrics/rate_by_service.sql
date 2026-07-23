-- case: rate_by_service
-- q: { duration > 1s } | rate() by(resource.service.name)

== range (query_range) ==
SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns), INTERVAL 60000 MILLISECOND)) AS t, service AS g0,
       uniqExact(trace_id, span_id) AS n
FROM trace_spans
WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
  AND duration_ns > 1000000000
GROUP BY t, g0
ORDER BY t ASC, g0

== instant (query) ==
SELECT service AS g0, uniqExact(trace_id, span_id) AS n
FROM trace_spans
WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
  AND duration_ns > 1000000000
GROUP BY g0
ORDER BY g0

== series probe ==
SELECT count() AS n FROM (
  SELECT service AS g0
  FROM trace_spans
  WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
    AND duration_ns > 1000000000
  GROUP BY g0
  LIMIT 1001
)
