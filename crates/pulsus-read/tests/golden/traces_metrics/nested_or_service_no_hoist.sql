-- case: nested_or_service_no_hoist
-- q: { (resource.service.name = "a" || resource.service.name = "b") && duration > 1s } | rate()

== range (query_range) ==
SELECT toUnixTimestamp(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns), INTERVAL 60 SECOND)) AS t,
       uniqExact(trace_id, span_id) AS n
FROM trace_spans
WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
  AND ((service = 'a' OR service = 'b') AND duration_ns > 1000000000)
GROUP BY t
ORDER BY t ASC

== instant (query) ==
SELECT uniqExact(trace_id, span_id) AS n
FROM trace_spans
WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
  AND ((service = 'a' OR service = 'b') AND duration_ns > 1000000000)
