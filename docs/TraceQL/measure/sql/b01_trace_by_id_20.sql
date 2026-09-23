WITH (SELECT (min(start_ns), max(end_ns))
      FROM tqd_g1.traces WHERE trace_id = toFixedString(unhex('50FB0CD99260AC2A15D0A6F208126742'), 16)) AS ext
SELECT groupArray((span_id, parent_span_id, start_ns, duration_ns, service, resource_id, name, kind,
                   status_code, status_message, trace_state, flags, scope_name, scope_version, scope_attrs,
                   attrs, attrs_other, dropped_attrs, events, dropped_events, links, dropped_links)) AS spans,
       (SELECT groupArray((resource_id, attrs, attrs_other, dropped_attrs, schema_url))
        FROM (SELECT resource_id, any(attrs) AS attrs, any(attrs_other) AS attrs_other,
                     any(dropped_attrs) AS dropped_attrs, any(schema_url) AS schema_url
              FROM tqd_g1.resources
              WHERE day >= toDate(fromUnixTimestamp64Nano(ext.1)) AND day <= toDate(fromUnixTimestamp64Nano(ext.2))
                AND resource_id IN (SELECT DISTINCT resource_id FROM tqd_g1.spans
                                    WHERE (intDiv(start_ns, 300000000000), trace_id) IN
                                          (SELECT (k, toFixedString(unhex('50FB0CD99260AC2A15D0A6F208126742'), 16)) FROM (SELECT arrayJoin(range(intDiv(ext.1, 300000000000), intDiv(ext.2, 300000000000) + 1)) AS k)))
              GROUP BY resource_id)) AS resources
FROM tqd_g1.spans
WHERE (intDiv(start_ns, 300000000000), trace_id) IN
      (SELECT (k, toFixedString(unhex('50FB0CD99260AC2A15D0A6F208126742'), 16)) FROM (SELECT arrayJoin(range(intDiv(ext.1, 300000000000), intDiv(ext.2, 300000000000) + 1)) AS k))
