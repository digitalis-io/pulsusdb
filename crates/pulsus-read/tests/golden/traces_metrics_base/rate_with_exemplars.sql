-- case: rate_with_exemplars
-- q: {} | rate() with(exemplars=3)

== range (query_range) ==
SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns), INTERVAL 60000 MILLISECOND)) AS t,
       uniqExact(trace_id, span_id) AS n
FROM trace_spans
WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
GROUP BY t
ORDER BY t ASC

== instant (query) ==
SELECT uniqExact(trace_id, span_id) AS n
FROM trace_spans
WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000

== exemplars ==
SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns), INTERVAL 60000 MILLISECOND)) AS t,
       groupArraySample(3, 1)(tuple(trace_id, timestamp_ns)) AS ex
FROM trace_spans
WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
GROUP BY t
ORDER BY t ASC
