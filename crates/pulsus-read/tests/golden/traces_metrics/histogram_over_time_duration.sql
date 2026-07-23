-- case: histogram_over_time_duration
-- q: { span.http.status_code >= 500 } | histogram_over_time(duration)

== range (query_range) ==
SELECT t, [countIf(val <= 1024), countIf(val <= 8192), countIf(val <= 65536), countIf(val <= 524288), countIf(val <= 4194304), countIf(val <= 33554432), countIf(val <= 268435456), countIf(val <= 1073741824), countIf(val <= 2147483648), countIf(val <= 4294967296), countIf(val <= 17179869184), countIf(val <= 68719476736), countIf(val <= 274877906944), countIf(val <= 1099511627776)] AS bkts
FROM (
  SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns), INTERVAL 60000 MILLISECOND)) AS t, trace_id, span_id,
         any(duration_ns) AS val
  FROM trace_spans
  WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
    AND (trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000 AND key = 'http.status_code' AND val_num >= 500 AND scope = 'span')
  GROUP BY t, trace_id, span_id
)
GROUP BY t
ORDER BY t ASC

== instant (query) ==
SELECT [countIf(val <= 1024), countIf(val <= 8192), countIf(val <= 65536), countIf(val <= 524288), countIf(val <= 4194304), countIf(val <= 33554432), countIf(val <= 268435456), countIf(val <= 1073741824), countIf(val <= 2147483648), countIf(val <= 4294967296), countIf(val <= 17179869184), countIf(val <= 68719476736), countIf(val <= 274877906944), countIf(val <= 1099511627776)] AS bkts
FROM (
  SELECT trace_id, span_id, any(duration_ns) AS val
  FROM trace_spans
  WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
    AND (trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000 AND key = 'http.status_code' AND val_num >= 500 AND scope = 'span')
  GROUP BY trace_id, span_id
)
