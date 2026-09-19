-- case: mixed_boolean
-- q: { (span.foo = "x" || duration > 2s) && status = error } | rate()

== range (query_range) ==
WITH arrayFirstIndex((k, s) -> k = 'foo' AND s = 'span', attr_key, attr_scope) AS pi0
SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns - 1), INTERVAL 60000000000 NANOSECOND)) + 60000 AS t,
       uniqExact(trace_id, span_id) AS n
FROM trace_spans
WHERE timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
  AND ((((pi0 != 0) AND attr_val[pi0] = 'x') OR duration_ns > 2000000000) AND status_code = 2)
GROUP BY t
ORDER BY t ASC

== instant (query) ==
WITH arrayFirstIndex((k, s) -> k = 'foo' AND s = 'span', attr_key, attr_scope) AS pi0
SELECT uniqExact(trace_id, span_id) AS n
FROM trace_spans
WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
  AND ((((pi0 != 0) AND attr_val[pi0] = 'x') OR duration_ns > 2000000000) AND status_code = 2)

== exemplars ==
WITH arrayFirstIndex((k, s) -> k = 'foo' AND s = 'span', attr_key, attr_scope) AS pi0
SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns - 1), INTERVAL 60000000000 NANOSECOND)) + 60000 AS t,
       groupArraySample(1, 1)(tuple(trace_id, timestamp_ns)) AS ex
FROM trace_spans
WHERE timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
  AND ((((pi0 != 0) AND attr_val[pi0] = 'x') OR duration_ns > 2000000000) AND status_code = 2)
GROUP BY t
ORDER BY t ASC
