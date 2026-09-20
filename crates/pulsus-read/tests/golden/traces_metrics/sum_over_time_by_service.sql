-- case: sum_over_time_by_service
-- q: { span.env = "prod" } | sum_over_time(duration) by(resource.service.name)

== range (query_range) ==
SELECT t, g0, toFloat64(sum(val)) AS v
FROM (
  WITH arrayFirstIndex((k, s) -> k = 'env' AND s = 'span', attr_key, attr_scope) AS pi0
  SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns - 1), INTERVAL 60000000000 NANOSECOND)) + 60000 AS t, service AS g0, trace_id, span_id,
         any(duration_ns) AS val
  FROM trace_spans
  WHERE timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
    AND ((pi0 != 0) AND attr_val[pi0] = 'prod')
  GROUP BY t, g0, trace_id, span_id
)
GROUP BY t, g0
ORDER BY t ASC, g0

== instant (query) ==
SELECT g0, toFloat64(sum(val)) AS v
FROM (
  WITH arrayFirstIndex((k, s) -> k = 'env' AND s = 'span', attr_key, attr_scope) AS pi0
  SELECT service AS g0, trace_id, span_id, any(duration_ns) AS val
  FROM trace_spans
  WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
    AND ((pi0 != 0) AND attr_val[pi0] = 'prod')
  GROUP BY g0, trace_id, span_id
)
GROUP BY g0
ORDER BY g0

== series probe ==
SELECT count() AS n FROM (
  WITH arrayFirstIndex((k, s) -> k = 'env' AND s = 'span', attr_key, attr_scope) AS pi0
  SELECT service AS g0
  FROM trace_spans
  WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
    AND ((pi0 != 0) AND attr_val[pi0] = 'prod')
  GROUP BY g0
  LIMIT 1001
)

== range series probe ==
SELECT count() AS n FROM (
  WITH arrayFirstIndex((k, s) -> k = 'env' AND s = 'span', attr_key, attr_scope) AS pi0
  SELECT service AS g0
  FROM trace_spans
  WHERE timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
    AND ((pi0 != 0) AND attr_val[pi0] = 'prod')
  GROUP BY g0
  LIMIT 1001
)

== exemplars ==
WITH arrayFirstIndex((k, s) -> k = 'env' AND s = 'span', attr_key, attr_scope) AS pi0
SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns - 1), INTERVAL 60000000000 NANOSECOND)) + 60000 AS t, service AS g0,
       groupArraySample(1, 1)(tuple(trace_id, timestamp_ns)) AS ex
FROM trace_spans
WHERE timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
  AND ((pi0 != 0) AND attr_val[pi0] = 'prod')
GROUP BY t, g0
ORDER BY t ASC, g0
