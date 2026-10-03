WITH (SELECT (count(), min(start_ns), max(last_start_ns),
              groupUniqArrayArray(4096)(buckets))
      FROM tqd_g1.traces
      WHERE trace_id = toFixedString(unhex('9E0AE95131B5BEDBEEA2C9EB5234F1EC'), 16)) AS ext,
     ifNull(ext.1, 0)  AS idx_rows,
     ifNull(ext.2, 0)  AS ext_lo,
     ifNull(ext.3, 0)  AS ext_hi,
     ifNull(ext.4, []) AS bk,
     (SELECT (groupArray((span_id, parent_span_id, start_ns, end_ns, service,
                          resource_id, name, kind, status_code, status_message,
                          trace_state, flags, scope_name, scope_version, scope_attrs,
                          attrs, attrs_other, dropped_attrs, events, dropped_events,
                          links, dropped_links,
                          scope_schema_url, scope_dropped_attrs, scope_attrs_other)),
              groupUniqArray((service, resource_id)))
      FROM tqd_g1.spans
      WHERE (intDiv(start_ns, 300000000000), trace_id) IN
            (SELECT (k, toFixedString(unhex('9E0AE95131B5BEDBEEA2C9EB5234F1EC'), 16))
             FROM (SELECT arrayJoin(bk) AS k))
        AND length(bk) < 4096) AS sp
SELECT idx_rows               AS index_rows,
       toUInt32(length(bk))   AS bucket_count,
       sp.1                   AS spans,
       (SELECT groupArray((resource_id, attrs, attrs_other, dropped_attrs,
                           schema_url, entity_refs))
        FROM (SELECT resource_id,
                     any(attrs) AS attrs, any(attrs_other) AS attrs_other,
                     any(dropped_attrs) AS dropped_attrs, any(schema_url) AS schema_url,
                     any(entity_refs) AS entity_refs
              FROM tqd_g1.resources
              WHERE day >= toDate(fromUnixTimestamp64Nano(ext_lo), 'UTC') AND day <= toDate(fromUnixTimestamp64Nano(ext_hi), 'UTC')
                AND (service, resource_id) IN (SELECT arrayJoin(sp.2))
              GROUP BY resource_id)) AS resources
