-- case: compare_status
-- q: { resource.service.name = "checkout" } | compare({ span.http.status_code = "500" })

== compare cross-tab (query_range) ==
SELECT t, akey, aval, countIf(is_sel = 0) AS base_n, countIf(is_sel) AS sel_n
FROM (
  SELECT t, is_sel, kv.1 AS akey, kv.2 AS aval FROM (
    SELECT t, is_sel, arrayJoin([('name', i_name), ('kind', transform(i_kind, [0, 1, 2, 3, 4, 5], ['unspecified', 'internal', 'server', 'client', 'producer', 'consumer'], 'unspecified')), ('status', transform(i_status, [0, 1, 2], ['unset', 'ok', 'error'], 'unset')), ('resource.service.name', i_service)]) AS kv
    FROM (
  SELECT t, trace_id, span_id, any(i_name) AS i_name, any(i_kind) AS i_kind, any(i_status) AS i_status, any(i_service) AS i_service, max(is_sel) AS is_sel
  FROM (
  SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns), INTERVAL 60000 MILLISECOND)) AS t, trace_id, span_id, name AS i_name, kind AS i_kind, status_code AS i_status, service AS i_service, ((trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000 AND key = 'http.status_code' AND val = '500' AND scope = 'span')) AS is_sel
    FROM trace_spans
    PREWHERE service = 'checkout'
  WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
  )
  GROUP BY t, trace_id, span_id
    )
  )
  UNION ALL
  SELECT b.t AS t, b.is_sel AS is_sel, concat(a.scope, '.', a.key) AS akey, a.val AS aval
  FROM (
  SELECT t, trace_id, span_id, any(i_name) AS i_name, any(i_kind) AS i_kind, any(i_status) AS i_status, any(i_service) AS i_service, max(is_sel) AS is_sel
  FROM (
  SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns), INTERVAL 60000 MILLISECOND)) AS t, trace_id, span_id, name AS i_name, kind AS i_kind, status_code AS i_status, service AS i_service, ((trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000 AND key = 'http.status_code' AND val = '500' AND scope = 'span')) AS is_sel
    FROM trace_spans
    PREWHERE service = 'checkout'
  WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
  )
  GROUP BY t, trace_id, span_id
  ) b
  INNER JOIN (
    SELECT DISTINCT trace_id, span_id, scope, key, val FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
  ) a ON b.trace_id = a.trace_id AND b.span_id = a.span_id
)
GROUP BY t, akey, aval
ORDER BY t ASC, akey, aval

== compare totals (query_range) ==
SELECT t, countIf(is_sel = 0) AS base_total, countIf(is_sel) AS sel_total
FROM (
  SELECT t, trace_id, span_id, any(i_name) AS i_name, any(i_kind) AS i_kind, any(i_status) AS i_status, any(i_service) AS i_service, max(is_sel) AS is_sel
  FROM (
  SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns), INTERVAL 60000 MILLISECOND)) AS t, trace_id, span_id, name AS i_name, kind AS i_kind, status_code AS i_status, service AS i_service, ((trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000 AND key = 'http.status_code' AND val = '500' AND scope = 'span')) AS is_sel
    FROM trace_spans
    PREWHERE service = 'checkout'
  WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
  )
  GROUP BY t, trace_id, span_id
)
GROUP BY t
ORDER BY t ASC

== compare series probe ==
SELECT toUInt64(pairs * 2 + keys * 4 + 100) AS n FROM (
  SELECT count() AS pairs, uniqExact(akey) AS keys FROM (
  SELECT akey, aval FROM (
  SELECT t, is_sel, kv.1 AS akey, kv.2 AS aval FROM (
    SELECT t, is_sel, arrayJoin([('name', i_name), ('kind', transform(i_kind, [0, 1, 2, 3, 4, 5], ['unspecified', 'internal', 'server', 'client', 'producer', 'consumer'], 'unspecified')), ('status', transform(i_status, [0, 1, 2], ['unset', 'ok', 'error'], 'unset')), ('resource.service.name', i_service)]) AS kv
    FROM (
  SELECT t, trace_id, span_id, any(i_name) AS i_name, any(i_kind) AS i_kind, any(i_status) AS i_status, any(i_service) AS i_service, max(is_sel) AS is_sel
  FROM (
  SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns), INTERVAL 60000 MILLISECOND)) AS t, trace_id, span_id, name AS i_name, kind AS i_kind, status_code AS i_status, service AS i_service, ((trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000 AND key = 'http.status_code' AND val = '500' AND scope = 'span')) AS is_sel
    FROM trace_spans
    PREWHERE service = 'checkout'
  WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
  )
  GROUP BY t, trace_id, span_id
    )
  )
  UNION ALL
  SELECT b.t AS t, b.is_sel AS is_sel, concat(a.scope, '.', a.key) AS akey, a.val AS aval
  FROM (
  SELECT t, trace_id, span_id, any(i_name) AS i_name, any(i_kind) AS i_kind, any(i_status) AS i_status, any(i_service) AS i_service, max(is_sel) AS is_sel
  FROM (
  SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns), INTERVAL 60000 MILLISECOND)) AS t, trace_id, span_id, name AS i_name, kind AS i_kind, status_code AS i_status, service AS i_service, ((trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000 AND key = 'http.status_code' AND val = '500' AND scope = 'span')) AS is_sel
    FROM trace_spans
    PREWHERE service = 'checkout'
  WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
  )
  GROUP BY t, trace_id, span_id
  ) b
  INNER JOIN (
    SELECT DISTINCT trace_id, span_id, scope, key, val FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
  ) a ON b.trace_id = a.trace_id AND b.span_id = a.span_id
) GROUP BY akey, aval LIMIT 1001
)
)
