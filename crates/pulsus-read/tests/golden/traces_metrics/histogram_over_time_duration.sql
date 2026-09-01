-- case: histogram_over_time_duration
-- q: { span.http.status_code >= 500 } | histogram_over_time(duration)

== range (query_range) ==
SELECT t, toUInt64(roundToExp2(val - 1)) * 2 AS bucket, count() AS n
FROM (
  SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns - 1), INTERVAL 60000000000 NANOSECOND)) + 60000 AS t, trace_id, span_id,
         any(duration_ns) AS val
  FROM trace_spans
  WHERE timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
    AND (trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001 AND key = 'http.status_code' AND val_num >= 500 AND scope = 'span')
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
  WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
    AND (trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000 AND key = 'http.status_code' AND val_num >= 500 AND scope = 'span')
  GROUP BY trace_id, span_id
)
WHERE val >= 2
GROUP BY bucket
ORDER BY bucket ASC

== exemplars ==
SELECT t, toUInt64(roundToExp2(val - 1)) * 2 AS bucket, groupArraySample(1, 1)(tuple(trace_id, ts)) AS ex
FROM (
  SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns - 1), INTERVAL 60000000000 NANOSECOND)) + 60000 AS t, trace_id, span_id,
         any(duration_ns) AS val, any(timestamp_ns) AS ts
  FROM trace_spans
  WHERE timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
    AND (trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001 AND key = 'http.status_code' AND val_num >= 500 AND scope = 'span')
  GROUP BY t, trace_id, span_id
)
WHERE val >= 2
GROUP BY t, bucket
ORDER BY t ASC, bucket ASC
