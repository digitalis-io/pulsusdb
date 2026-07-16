-- case: negated_attr
-- q: { span.env != "prod" } | count_over_time()

== range (query_range) ==
SELECT toUnixTimestamp(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns), INTERVAL 60 SECOND)) AS t,
       uniqExact(trace_id, span_id) AS n
FROM trace_spans
WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
  AND (trace_id, span_id) NOT IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000 AND key = 'env' AND val = 'prod' AND scope = 'span')
GROUP BY t
ORDER BY t ASC

== instant (query) ==
SELECT uniqExact(trace_id, span_id) AS n
FROM trace_spans
WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
  AND (trace_id, span_id) NOT IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000 AND key = 'env' AND val = 'prod' AND scope = 'span')
