-- case: sum_over_time_by_service
-- q: { span.env = "prod" } | sum_over_time(duration) by(resource.service.name)

== range (query_range) ==
SELECT t, g0, toFloat64(sum(val)) AS v
FROM (
  SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns - 1), INTERVAL 60000000000 NANOSECOND)) + 60000 AS t, service AS g0, trace_id, span_id,
         any(duration_ns) AS val
  FROM trace_spans
  WHERE timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
    AND (trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001 AND key = 'env' AND val = 'prod' AND scope = 'span')
  GROUP BY t, g0, trace_id, span_id
)
GROUP BY t, g0
ORDER BY t ASC, g0

== instant (query) ==
SELECT g0, toFloat64(sum(val)) AS v
FROM (
  SELECT service AS g0, trace_id, span_id, any(duration_ns) AS val
  FROM trace_spans
  WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
    AND (trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000 AND key = 'env' AND val = 'prod' AND scope = 'span')
  GROUP BY g0, trace_id, span_id
)
GROUP BY g0
ORDER BY g0

== series probe ==
SELECT count() AS n FROM (
  SELECT service AS g0
  FROM trace_spans
  WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
    AND (trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000 AND key = 'env' AND val = 'prod' AND scope = 'span')
  GROUP BY g0
  LIMIT 1001
)

== range series probe ==
SELECT count() AS n FROM (
  SELECT service AS g0
  FROM trace_spans
  WHERE timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
    AND (trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001 AND key = 'env' AND val = 'prod' AND scope = 'span')
  GROUP BY g0
  LIMIT 1001
)

== exemplars ==
SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns - 1), INTERVAL 60000000000 NANOSECOND)) + 60000 AS t, service AS g0,
       groupArraySample(1, 1)(tuple(trace_id, timestamp_ns)) AS ex
FROM trace_spans
WHERE timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
  AND (trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001 AND key = 'env' AND val = 'prod' AND scope = 'span')
GROUP BY t, g0
ORDER BY t ASC, g0
