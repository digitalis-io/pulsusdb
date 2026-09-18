-- case: spanset_by_service
-- q: { .a = "1" } | by(resource.service.name)

== phase1 generator[0] ==
SELECT trace_id, max(timestamp_ns) AS bound_ts
FROM trace_attrs_idx
WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15')
  AND timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000
  AND (key = 'a' AND val = '1')
GROUP BY trace_id
ORDER BY bound_ts DESC, trace_id ASC
LIMIT 100001

== phase2 hydration (sample batch) ==
WITH arrayFirstIndex((k, s) -> k = 'a' AND s = 'span', attr_key, attr_scope) AS pi0s,
     arrayFirstIndex((k, s) -> k = 'a' AND s = 'resource', attr_key, attr_scope) AS pi0r,
     arrayFirstIndex((k, s) -> k = 'a' AND s = 'event', attr_key, attr_scope) AS pi0e,
     arrayFirstIndex((k, s) -> k = 'a' AND s = 'link', attr_key, attr_scope) AS pi0l,
     arrayFirstIndex((k, s) -> k = 'a' AND s = 'instrumentation', attr_key, attr_scope) AS pi0i,
     arrayFirstIndex((k, s, v) -> k = 'a' AND s = 'event' AND v = '1', attr_key, attr_scope, attr_val) AS pm0e,
     arrayFirstIndex((k, s, v) -> k = 'a' AND s = 'link' AND v = '1', attr_key, attr_scope, attr_val) AS pm0l
SELECT trace_id, span_id, parent_id, if(length(service) <= 8192, service, substringUTF8(service, 1, 2048)) AS service, if(length(name) <= 8192, name, substringUTF8(name, 1, 2048)) AS name, timestamp_ns, duration_ns, status_code, if(length(status_message) <= 8192, status_message, substringUTF8(status_message, 1, 2048)) AS status_message, kind, if(length(scope_name) <= 8192, scope_name, substringUTF8(scope_name, 1, 2048)) AS scope_name, if(length(scope_version) <= 8192, scope_version, substringUTF8(scope_version, 1, 2048)) AS scope_version,
       [if(pi0s != 0, attr_val[pi0s] = '1', if(pi0r != 0, attr_val[pi0r] = '1', if(pi0e != 0, pm0e != 0, if(pi0l != 0, pm0l != 0, if(pi0i != 0, attr_val[pi0i] = '1', 0)))))] AS attr_slot
FROM trace_spans
WHERE trace_id IN (unhex('000102030405060708090a0b0c0d0e0f'), unhex('101112131415161718191a1b1c1d1e1f'))
  AND timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000
ORDER BY trace_id ASC, timestamp_ns ASC, span_id ASC
LIMIT 10001 BY trace_id

== by() cardinality probe ==
SELECT count() AS n FROM (
  SELECT service AS g0
  FROM trace_spans
  WHERE timestamp_ns >= 1700000000000000000 AND timestamp_ns < 1700010800000000000 AND ((trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') AND timestamp_ns >= 1700000000000000000 AND timestamp_ns < 1700010800000000000 AND key = 'a' AND val = '1'))
  GROUP BY g0
  LIMIT 1001
)

== root hydration (sample winners) ==
SELECT trace_id, span_id, parent_id, if(length(service) <= 8192, service, substringUTF8(service, 1, 2048)) AS service, if(length(name) <= 8192, name, substringUTF8(name, 1, 2048)) AS name, timestamp_ns, duration_ns
FROM trace_spans
WHERE trace_id IN (unhex('000102030405060708090a0b0c0d0e0f'))
