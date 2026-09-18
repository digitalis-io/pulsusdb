-- case: issue492_unscoped_attr_with_max_duration
-- q: { .k = "v" } | max(duration) > 1s

== phase1 generator[0] ==
SELECT trace_id, max(timestamp_ns) AS bound_ts
FROM trace_attrs_idx
WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15')
  AND timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000
  AND (key = 'k' AND val = 'v')
GROUP BY trace_id
HAVING max(duration_ns) > 1000000000
ORDER BY bound_ts DESC, trace_id ASC
LIMIT 100001

== phase2 hydration (sample batch) ==
WITH arrayFirstIndex((k, s) -> k = 'k' AND s = 'span', attr_key, attr_scope) AS pi0s,
     arrayFirstIndex((k, s) -> k = 'k' AND s = 'resource', attr_key, attr_scope) AS pi0r,
     arrayFirstIndex((k, s) -> k = 'k' AND s = 'event', attr_key, attr_scope) AS pi0e,
     arrayFirstIndex((k, s) -> k = 'k' AND s = 'link', attr_key, attr_scope) AS pi0l,
     arrayFirstIndex((k, s) -> k = 'k' AND s = 'instrumentation', attr_key, attr_scope) AS pi0i,
     arrayFirstIndex((k, s, v) -> k = 'k' AND s = 'event' AND v = 'v', attr_key, attr_scope, attr_val) AS pm0e,
     arrayFirstIndex((k, s, v) -> k = 'k' AND s = 'link' AND v = 'v', attr_key, attr_scope, attr_val) AS pm0l
SELECT trace_id, span_id, parent_id, if(length(service) <= 8192, service, substringUTF8(service, 1, 2048)) AS service, if(length(name) <= 8192, name, substringUTF8(name, 1, 2048)) AS name, timestamp_ns, duration_ns, status_code, if(length(status_message) <= 8192, status_message, substringUTF8(status_message, 1, 2048)) AS status_message, kind, if(length(scope_name) <= 8192, scope_name, substringUTF8(scope_name, 1, 2048)) AS scope_name, if(length(scope_version) <= 8192, scope_version, substringUTF8(scope_version, 1, 2048)) AS scope_version,
       [if(pi0s != 0, attr_val[pi0s] = 'v', if(pi0r != 0, attr_val[pi0r] = 'v', if(pi0e != 0, pm0e != 0, if(pi0l != 0, pm0l != 0, if(pi0i != 0, attr_val[pi0i] = 'v', 0)))))] AS attr_probe
FROM trace_spans
WHERE trace_id IN (unhex('000102030405060708090a0b0c0d0e0f'), unhex('101112131415161718191a1b1c1d1e1f'))
  AND timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000
ORDER BY trace_id ASC, timestamp_ns ASC, span_id ASC
LIMIT 10001 BY trace_id

== root hydration (sample winners) ==
SELECT trace_id, span_id, parent_id, if(length(service) <= 8192, service, substringUTF8(service, 1, 2048)) AS service, if(length(name) <= 8192, name, substringUTF8(name, 1, 2048)) AS name, timestamp_ns, duration_ns
FROM trace_spans
WHERE trace_id IN (unhex('000102030405060708090a0b0c0d0e0f'))
