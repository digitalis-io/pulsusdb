-- case: mixed_boolean
-- q: { (span.foo = "x" || duration > 2s) && status = error } | rate()

== range (query_range) ==
SELECT toUnixTimestamp(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns), INTERVAL 60 SECOND)) AS t,
       uniqExact(trace_id, span_id) AS n
FROM trace_spans
WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
  AND (((trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000 AND key = 'foo' AND val = 'x' AND scope = 'span') OR duration_ns > 2000000000) AND status_code = 2)
GROUP BY t
ORDER BY t ASC

== instant (query) ==
SELECT uniqExact(trace_id, span_id) AS n
FROM trace_spans
WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
  AND (((trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000 AND key = 'foo' AND val = 'x' AND scope = 'span') OR duration_ns > 2000000000) AND status_code = 2)
