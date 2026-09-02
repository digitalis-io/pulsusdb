-- case: nested_set_root_rate
-- q: { nestedSetParent < 0 } | rate()

== range (query_range) ==
SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns), INTERVAL 60000 MILLISECOND)) AS t,
       uniqExact(trace_id, span_id) AS n
FROM trace_spans
WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
  AND parent_id = toFixedString(unhex('0000000000000000'), 8)
GROUP BY t
ORDER BY t ASC

== instant (query) ==
SELECT uniqExact(trace_id, span_id) AS n
FROM trace_spans
WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
  AND parent_id = toFixedString(unhex('0000000000000000'), 8)
