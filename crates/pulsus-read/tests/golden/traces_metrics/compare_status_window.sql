-- case: compare_status_window
-- q: { resource.service.name = "checkout" } | compare({ span.http.status_code = "500" }, 3, 1700000005000000000, 1700000008000000000)

== compare cross-tab (query_range) ==
SELECT t, akey, aval, countIf(is_sel = 0) AS base_n, countIf(is_sel) AS sel_n
FROM (
  SELECT t, is_sel, kv.1 AS akey, kv.2 AS aval FROM (
    SELECT t, is_sel, arrayJoin([('name', i_name), ('kind', transform(i_kind, [0, 1, 2, 3, 4, 5], ['unspecified', 'internal', 'server', 'client', 'producer', 'consumer'], 'unspecified')), ('status', transform(i_status, [0, 1, 2], ['unset', 'ok', 'error'], 'unset')), ('resource.service.name', i_service), ('statusMessage', i_status_message), ('instrumentation:name', i_scope_name), ('instrumentation:version', i_scope_version), ('rootName', r.root_name), ('rootServiceName', r.root_service)]) AS kv
    FROM (
  SELECT t, trace_id, span_id, any(i_name) AS i_name, any(i_kind) AS i_kind, any(i_status) AS i_status, any(i_service) AS i_service, any(i_status_message) AS i_status_message, any(i_scope_name) AS i_scope_name, any(i_scope_version) AS i_scope_version, max(is_sel) AS is_sel
  FROM (
  SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns - 1), INTERVAL 60000000000 NANOSECOND)) + 60000 AS t, trace_id, span_id, name AS i_name, kind AS i_kind, status_code AS i_status, service AS i_service, status_message AS i_status_message, scope_name AS i_scope_name, scope_version AS i_scope_version, (((trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001 AND key = 'http.status_code' AND val = '500' AND scope = 'span')) AND timestamp_ns > 1700000005000000000 AND timestamp_ns <= 1700000008000000000) AS is_sel
    FROM trace_spans
    PREWHERE service = 'checkout'
  WHERE timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
  )
  GROUP BY t, trace_id, span_id
    ) b
    LEFT JOIN (
  SELECT trace_id, argMin(if(length(name) <= 8192, name, substringUTF8(name, 1, 2048)), (toUInt8(parent_id != toFixedString(unhex('0000000000000000'), 8)), timestamp_ns, span_id)) AS root_name, argMin(if(length(service) <= 8192, service, substringUTF8(service, 1, 2048)), (toUInt8(parent_id != toFixedString(unhex('0000000000000000'), 8)), timestamp_ns, span_id)) AS root_service
  FROM trace_spans
  WHERE trace_id IN (SELECT DISTINCT trace_id FROM (
  SELECT t, trace_id, span_id, any(i_name) AS i_name, any(i_kind) AS i_kind, any(i_status) AS i_status, any(i_service) AS i_service, any(i_status_message) AS i_status_message, any(i_scope_name) AS i_scope_name, any(i_scope_version) AS i_scope_version, max(is_sel) AS is_sel
  FROM (
  SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns - 1), INTERVAL 60000000000 NANOSECOND)) + 60000 AS t, trace_id, span_id, name AS i_name, kind AS i_kind, status_code AS i_status, service AS i_service, status_message AS i_status_message, scope_name AS i_scope_name, scope_version AS i_scope_version, (((trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001 AND key = 'http.status_code' AND val = '500' AND scope = 'span')) AND timestamp_ns > 1700000005000000000 AND timestamp_ns <= 1700000008000000000) AS is_sel
    FROM trace_spans
    PREWHERE service = 'checkout'
  WHERE timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
  )
  GROUP BY t, trace_id, span_id
  ))
  GROUP BY trace_id
    ) r ON b.trace_id = r.trace_id
  )
  UNION ALL
  SELECT b.t AS t, b.is_sel AS is_sel, concat(a.scope, '.', a.key) AS akey, a.val AS aval
  FROM (
  SELECT t, trace_id, span_id, any(i_name) AS i_name, any(i_kind) AS i_kind, any(i_status) AS i_status, any(i_service) AS i_service, any(i_status_message) AS i_status_message, any(i_scope_name) AS i_scope_name, any(i_scope_version) AS i_scope_version, max(is_sel) AS is_sel
  FROM (
  SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns - 1), INTERVAL 60000000000 NANOSECOND)) + 60000 AS t, trace_id, span_id, name AS i_name, kind AS i_kind, status_code AS i_status, service AS i_service, status_message AS i_status_message, scope_name AS i_scope_name, scope_version AS i_scope_version, (((trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001 AND key = 'http.status_code' AND val = '500' AND scope = 'span')) AND timestamp_ns > 1700000005000000000 AND timestamp_ns <= 1700000008000000000) AS is_sel
    FROM trace_spans
    PREWHERE service = 'checkout'
  WHERE timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
  )
  GROUP BY t, trace_id, span_id
  ) b
  INNER JOIN (
    SELECT DISTINCT trace_id, span_id, scope, key, val FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
  ) a ON b.trace_id = a.trace_id AND b.span_id = a.span_id
)
GROUP BY t, akey, aval
ORDER BY t ASC, akey, aval

== compare totals (query_range) ==
SELECT t, countIf(is_sel = 0) AS base_total, countIf(is_sel) AS sel_total
FROM (
  SELECT t, trace_id, span_id, any(i_name) AS i_name, any(i_kind) AS i_kind, any(i_status) AS i_status, any(i_service) AS i_service, any(i_status_message) AS i_status_message, any(i_scope_name) AS i_scope_name, any(i_scope_version) AS i_scope_version, max(is_sel) AS is_sel
  FROM (
  SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns - 1), INTERVAL 60000000000 NANOSECOND)) + 60000 AS t, trace_id, span_id, name AS i_name, kind AS i_kind, status_code AS i_status, service AS i_service, status_message AS i_status_message, scope_name AS i_scope_name, scope_version AS i_scope_version, (((trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001 AND key = 'http.status_code' AND val = '500' AND scope = 'span')) AND timestamp_ns > 1700000005000000000 AND timestamp_ns <= 1700000008000000000) AS is_sel
    FROM trace_spans
    PREWHERE service = 'checkout'
  WHERE timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
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
    SELECT t, is_sel, arrayJoin([('name', i_name), ('kind', transform(i_kind, [0, 1, 2, 3, 4, 5], ['unspecified', 'internal', 'server', 'client', 'producer', 'consumer'], 'unspecified')), ('status', transform(i_status, [0, 1, 2], ['unset', 'ok', 'error'], 'unset')), ('resource.service.name', i_service), ('statusMessage', i_status_message), ('instrumentation:name', i_scope_name), ('instrumentation:version', i_scope_version), ('rootName', r.root_name), ('rootServiceName', r.root_service)]) AS kv
    FROM (
  SELECT t, trace_id, span_id, any(i_name) AS i_name, any(i_kind) AS i_kind, any(i_status) AS i_status, any(i_service) AS i_service, any(i_status_message) AS i_status_message, any(i_scope_name) AS i_scope_name, any(i_scope_version) AS i_scope_version, max(is_sel) AS is_sel
  FROM (
  SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns), INTERVAL 60000 MILLISECOND)) AS t, trace_id, span_id, name AS i_name, kind AS i_kind, status_code AS i_status, service AS i_service, status_message AS i_status_message, scope_name AS i_scope_name, scope_version AS i_scope_version, (((trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000 AND key = 'http.status_code' AND val = '500' AND scope = 'span')) AND timestamp_ns > 1700000005000000000 AND timestamp_ns <= 1700000008000000000) AS is_sel
    FROM trace_spans
    PREWHERE service = 'checkout'
  WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
  )
  GROUP BY t, trace_id, span_id
    ) b
    LEFT JOIN (
  SELECT trace_id, argMin(if(length(name) <= 8192, name, substringUTF8(name, 1, 2048)), (toUInt8(parent_id != toFixedString(unhex('0000000000000000'), 8)), timestamp_ns, span_id)) AS root_name, argMin(if(length(service) <= 8192, service, substringUTF8(service, 1, 2048)), (toUInt8(parent_id != toFixedString(unhex('0000000000000000'), 8)), timestamp_ns, span_id)) AS root_service
  FROM trace_spans
  WHERE trace_id IN (SELECT DISTINCT trace_id FROM (
  SELECT t, trace_id, span_id, any(i_name) AS i_name, any(i_kind) AS i_kind, any(i_status) AS i_status, any(i_service) AS i_service, any(i_status_message) AS i_status_message, any(i_scope_name) AS i_scope_name, any(i_scope_version) AS i_scope_version, max(is_sel) AS is_sel
  FROM (
  SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns), INTERVAL 60000 MILLISECOND)) AS t, trace_id, span_id, name AS i_name, kind AS i_kind, status_code AS i_status, service AS i_service, status_message AS i_status_message, scope_name AS i_scope_name, scope_version AS i_scope_version, (((trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000 AND key = 'http.status_code' AND val = '500' AND scope = 'span')) AND timestamp_ns > 1700000005000000000 AND timestamp_ns <= 1700000008000000000) AS is_sel
    FROM trace_spans
    PREWHERE service = 'checkout'
  WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
  )
  GROUP BY t, trace_id, span_id
  ))
  GROUP BY trace_id
    ) r ON b.trace_id = r.trace_id
  )
  UNION ALL
  SELECT b.t AS t, b.is_sel AS is_sel, concat(a.scope, '.', a.key) AS akey, a.val AS aval
  FROM (
  SELECT t, trace_id, span_id, any(i_name) AS i_name, any(i_kind) AS i_kind, any(i_status) AS i_status, any(i_service) AS i_service, any(i_status_message) AS i_status_message, any(i_scope_name) AS i_scope_name, any(i_scope_version) AS i_scope_version, max(is_sel) AS is_sel
  FROM (
  SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns), INTERVAL 60000 MILLISECOND)) AS t, trace_id, span_id, name AS i_name, kind AS i_kind, status_code AS i_status, service AS i_service, status_message AS i_status_message, scope_name AS i_scope_name, scope_version AS i_scope_version, (((trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000 AND key = 'http.status_code' AND val = '500' AND scope = 'span')) AND timestamp_ns > 1700000005000000000 AND timestamp_ns <= 1700000008000000000) AS is_sel
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

== compare range series probe ==
SELECT toUInt64(pairs * 2 + keys * 4 + 100) AS n FROM (
  SELECT count() AS pairs, uniqExact(akey) AS keys FROM (
  SELECT akey, aval FROM (
  SELECT t, is_sel, kv.1 AS akey, kv.2 AS aval FROM (
    SELECT t, is_sel, arrayJoin([('name', i_name), ('kind', transform(i_kind, [0, 1, 2, 3, 4, 5], ['unspecified', 'internal', 'server', 'client', 'producer', 'consumer'], 'unspecified')), ('status', transform(i_status, [0, 1, 2], ['unset', 'ok', 'error'], 'unset')), ('resource.service.name', i_service), ('statusMessage', i_status_message), ('instrumentation:name', i_scope_name), ('instrumentation:version', i_scope_version), ('rootName', r.root_name), ('rootServiceName', r.root_service)]) AS kv
    FROM (
  SELECT t, trace_id, span_id, any(i_name) AS i_name, any(i_kind) AS i_kind, any(i_status) AS i_status, any(i_service) AS i_service, any(i_status_message) AS i_status_message, any(i_scope_name) AS i_scope_name, any(i_scope_version) AS i_scope_version, max(is_sel) AS is_sel
  FROM (
  SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns - 1), INTERVAL 60000000000 NANOSECOND)) + 60000 AS t, trace_id, span_id, name AS i_name, kind AS i_kind, status_code AS i_status, service AS i_service, status_message AS i_status_message, scope_name AS i_scope_name, scope_version AS i_scope_version, (((trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001 AND key = 'http.status_code' AND val = '500' AND scope = 'span')) AND timestamp_ns > 1700000005000000000 AND timestamp_ns <= 1700000008000000000) AS is_sel
    FROM trace_spans
    PREWHERE service = 'checkout'
  WHERE timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
  )
  GROUP BY t, trace_id, span_id
    ) b
    LEFT JOIN (
  SELECT trace_id, argMin(if(length(name) <= 8192, name, substringUTF8(name, 1, 2048)), (toUInt8(parent_id != toFixedString(unhex('0000000000000000'), 8)), timestamp_ns, span_id)) AS root_name, argMin(if(length(service) <= 8192, service, substringUTF8(service, 1, 2048)), (toUInt8(parent_id != toFixedString(unhex('0000000000000000'), 8)), timestamp_ns, span_id)) AS root_service
  FROM trace_spans
  WHERE trace_id IN (SELECT DISTINCT trace_id FROM (
  SELECT t, trace_id, span_id, any(i_name) AS i_name, any(i_kind) AS i_kind, any(i_status) AS i_status, any(i_service) AS i_service, any(i_status_message) AS i_status_message, any(i_scope_name) AS i_scope_name, any(i_scope_version) AS i_scope_version, max(is_sel) AS is_sel
  FROM (
  SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns - 1), INTERVAL 60000000000 NANOSECOND)) + 60000 AS t, trace_id, span_id, name AS i_name, kind AS i_kind, status_code AS i_status, service AS i_service, status_message AS i_status_message, scope_name AS i_scope_name, scope_version AS i_scope_version, (((trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001 AND key = 'http.status_code' AND val = '500' AND scope = 'span')) AND timestamp_ns > 1700000005000000000 AND timestamp_ns <= 1700000008000000000) AS is_sel
    FROM trace_spans
    PREWHERE service = 'checkout'
  WHERE timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
  )
  GROUP BY t, trace_id, span_id
  ))
  GROUP BY trace_id
    ) r ON b.trace_id = r.trace_id
  )
  UNION ALL
  SELECT b.t AS t, b.is_sel AS is_sel, concat(a.scope, '.', a.key) AS akey, a.val AS aval
  FROM (
  SELECT t, trace_id, span_id, any(i_name) AS i_name, any(i_kind) AS i_kind, any(i_status) AS i_status, any(i_service) AS i_service, any(i_status_message) AS i_status_message, any(i_scope_name) AS i_scope_name, any(i_scope_version) AS i_scope_version, max(is_sel) AS is_sel
  FROM (
  SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns - 1), INTERVAL 60000000000 NANOSECOND)) + 60000 AS t, trace_id, span_id, name AS i_name, kind AS i_kind, status_code AS i_status, service AS i_service, status_message AS i_status_message, scope_name AS i_scope_name, scope_version AS i_scope_version, (((trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001 AND key = 'http.status_code' AND val = '500' AND scope = 'span')) AND timestamp_ns > 1700000005000000000 AND timestamp_ns <= 1700000008000000000) AS is_sel
    FROM trace_spans
    PREWHERE service = 'checkout'
  WHERE timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
  )
  GROUP BY t, trace_id, span_id
  ) b
  INNER JOIN (
    SELECT DISTINCT trace_id, span_id, scope, key, val FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
  ) a ON b.trace_id = a.trace_id AND b.span_id = a.span_id
) GROUP BY akey, aval LIMIT 1001
)
)

== exemplars ==
SELECT t, is_sel, akey, groupArraySample(1, 1)(tuple(trace_id, ts)) AS ex
FROM (
  SELECT t, is_sel, trace_id, ts, arrayJoin(['name', 'kind', 'status', 'resource.service.name', 'statusMessage', 'instrumentation:name', 'instrumentation:version', 'rootName', 'rootServiceName']) AS akey
  FROM (
  SELECT t, trace_id, span_id, any(ts) AS ts, max(is_sel) AS is_sel
    FROM (
  SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns - 1), INTERVAL 60000000000 NANOSECOND)) + 60000 AS t, trace_id, span_id, timestamp_ns AS ts, (((trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001 AND key = 'http.status_code' AND val = '500' AND scope = 'span')) AND timestamp_ns > 1700000005000000000 AND timestamp_ns <= 1700000008000000000) AS is_sel
      FROM trace_spans
      PREWHERE service = 'checkout'
  WHERE timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
    )
    GROUP BY t, trace_id, span_id
  )
  UNION ALL
  SELECT b.t AS t, b.is_sel AS is_sel, b.trace_id AS trace_id, b.ts AS ts, concat(a.scope, '.', a.key) AS akey
  FROM (
  SELECT t, trace_id, span_id, any(ts) AS ts, max(is_sel) AS is_sel
    FROM (
  SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns - 1), INTERVAL 60000000000 NANOSECOND)) + 60000 AS t, trace_id, span_id, timestamp_ns AS ts, (((trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001 AND key = 'http.status_code' AND val = '500' AND scope = 'span')) AND timestamp_ns > 1700000005000000000 AND timestamp_ns <= 1700000008000000000) AS is_sel
      FROM trace_spans
      PREWHERE service = 'checkout'
  WHERE timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
    )
    GROUP BY t, trace_id, span_id
  ) b
  INNER JOIN (
    SELECT DISTINCT trace_id, span_id, scope, key FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
  ) a ON b.trace_id = a.trace_id AND b.span_id = a.span_id
)
GROUP BY t, is_sel, akey
ORDER BY t ASC, is_sel, akey
