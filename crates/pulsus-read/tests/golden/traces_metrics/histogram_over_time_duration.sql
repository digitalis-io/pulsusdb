-- case: histogram_over_time_duration
-- q: { span.http.status_code >= 500 } | histogram_over_time(duration)

== range (query_range) ==
SELECT t, toUInt64(roundToExp2(val - 1)) * 2 AS bucket, count() AS n
FROM (
  WITH arrayFirstIndex((k, s) -> k = 'http.status_code' AND s = 'span', attr_key, attr_scope) AS pi0
  SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns - 1), INTERVAL 60000000000 NANOSECOND)) + 60000 AS t, trace_id, span_id,
         any(duration_ns) AS val
  FROM trace_spans
  WHERE timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
    AND ((pi0 != 0) AND ifNull(attr_num[pi0] >= 500, 0))
  GROUP BY t, trace_id, span_id
)
WHERE val >= 2
GROUP BY t, bucket
ORDER BY t ASC, bucket ASC

== instant (query) ==
SELECT toUInt64(roundToExp2(val - 1)) * 2 AS bucket, count() AS n
FROM (
  WITH arrayFirstIndex((k, s) -> k = 'http.status_code' AND s = 'span', attr_key, attr_scope) AS pi0
  SELECT trace_id, span_id, any(duration_ns) AS val
  FROM trace_spans
  WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
    AND ((pi0 != 0) AND ifNull(attr_num[pi0] >= 500, 0))
  GROUP BY trace_id, span_id
)
WHERE val >= 2
GROUP BY bucket
ORDER BY bucket ASC

== exemplars ==
SELECT t, toUInt64(roundToExp2(val - 1)) * 2 AS bucket, groupArraySample(1, 1)(tuple(trace_id, ts)) AS ex
FROM (
  WITH arrayFirstIndex((k, s) -> k = 'http.status_code' AND s = 'span', attr_key, attr_scope) AS pi0
  SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns - 1), INTERVAL 60000000000 NANOSECOND)) + 60000 AS t, trace_id, span_id,
         any(duration_ns) AS val, any(timestamp_ns) AS ts
  FROM trace_spans
  WHERE timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
    AND ((pi0 != 0) AND ifNull(attr_num[pi0] >= 500, 0))
  GROUP BY t, trace_id, span_id
)
WHERE val >= 2
GROUP BY t, bucket
ORDER BY t ASC, bucket ASC
