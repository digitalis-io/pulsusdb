-- case: worked_example
-- q: { resource.service.name = "checkout" && span.http.status_code >= 500 && duration > 2s }

== phase1 generator[0] ==
SELECT trace_id, max(timestamp_ns) AS bound_ts
FROM trace_spans
PREWHERE service = 'checkout'
WHERE timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000
GROUP BY trace_id
ORDER BY bound_ts DESC, trace_id ASC
LIMIT 100001

== phase2 hydration (sample batch) ==
WITH arrayFirstIndex((k, s) -> k = 'http.status_code' AND s = 'span', attr_key, attr_scope) AS pi0
SELECT trace_id, span_id, parent_id, if(length(service) <= 8192, service, substringUTF8(service, 1, 2048)) AS service, if(length(name) <= 8192, name, substringUTF8(name, 1, 2048)) AS name, timestamp_ns, duration_ns, status_code, if(length(status_message) <= 8192, status_message, substringUTF8(status_message, 1, 2048)) AS status_message, kind, if(length(scope_name) <= 8192, scope_name, substringUTF8(scope_name, 1, 2048)) AS scope_name, if(length(scope_version) <= 8192, scope_version, substringUTF8(scope_version, 1, 2048)) AS scope_version,
       [(pi0 != 0) AND ifNull(attr_num[pi0] >= 500, 0)] AS attr_slot,
       [if(length(attr_val[pi0]) <= 8192, attr_val[pi0], substringUTF8(attr_val[pi0], 1, 2048))] AS attr_slot_val,
       [attr_type[pi0]] AS attr_slot_type
FROM trace_spans
WHERE trace_id IN (unhex('000102030405060708090a0b0c0d0e0f'), unhex('101112131415161718191a1b1c1d1e1f'))
  AND timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000
ORDER BY trace_id ASC, timestamp_ns ASC, span_id ASC
LIMIT 10001 BY trace_id

== root hydration (sample winners) ==
SELECT trace_id, span_id, parent_id, if(length(service) <= 8192, service, substringUTF8(service, 1, 2048)) AS service, if(length(name) <= 8192, name, substringUTF8(name, 1, 2048)) AS name, timestamp_ns, duration_ns
FROM trace_spans
WHERE trace_id IN (unhex('000102030405060708090a0b0c0d0e0f'))
