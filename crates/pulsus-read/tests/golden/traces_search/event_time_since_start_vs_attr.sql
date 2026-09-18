-- case: event_time_since_start_vs_attr
-- q: { .a > event:timeSinceStart }

== phase1 generator[0] ==
SELECT trace_id, max(timestamp_ns) AS bound_ts
FROM trace_attrs_idx
WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15')
  AND timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000
  AND (key = 'a')
GROUP BY trace_id
ORDER BY bound_ts DESC, trace_id ASC
LIMIT 100001

== phase2 hydration (sample batch) ==
WITH arrayFirstIndex((k, s) -> k = 'a' AND s = 'span', attr_key, attr_scope) AS fs0s,
     arrayFirstIndex((k, s) -> k = 'a' AND s = 'resource', attr_key, attr_scope) AS fs0r,
     arrayFirstIndex((k, s) -> k = 'a' AND s = 'event', attr_key, attr_scope) AS fs0e,
     arrayFirstIndex((k, s) -> k = 'a' AND s = 'link', attr_key, attr_scope) AS fs0l,
     arrayFirstIndex((k, s) -> k = 'a' AND s = 'instrumentation', attr_key, attr_scope) AS fs0i,
     arrayFirstIndex((k, s) -> k = 'a' AND s = 'span', attr_key, attr_scope) AS fa0s,
     arrayFirstIndex((k, s) -> k = 'a' AND s = 'resource', attr_key, attr_scope) AS fa0r,
     arrayFirstIndex((k, s) -> k = 'a' AND s = 'event', attr_key, attr_scope) AS fa0e,
     arrayFirstIndex((k, s) -> k = 'a' AND s = 'link', attr_key, attr_scope) AS fa0l,
     arrayFirstIndex((k, s) -> k = 'a' AND s = 'instrumentation', attr_key, attr_scope) AS fa0i
SELECT trace_id, span_id, parent_id, if(length(service) <= 8192, service, substringUTF8(service, 1, 2048)) AS service, if(length(name) <= 8192, name, substringUTF8(name, 1, 2048)) AS name, timestamp_ns, duration_ns, status_code, if(length(status_message) <= 8192, status_message, substringUTF8(status_message, 1, 2048)) AS status_message, kind, if(length(scope_name) <= 8192, scope_name, substringUTF8(scope_name, 1, 2048)) AS scope_name, if(length(scope_version) <= 8192, scope_version, substringUTF8(scope_version, 1, 2048)) AS scope_version,
       [if(fs0s != 0, 1, if(fs0r != 0, 1, if(fs0e != 0, 1, if(fs0l != 0, 1, if(fs0i != 0, 1, 0))))), if(fa0s != 0, 1, if(fa0r != 0, 1, if(fa0e != 0, 1, if(fa0l != 0, 1, if(fa0i != 0, 1, 0))))), arrayCount((k, s) -> k = 'timeSinceStart' AND s = 'event:intrinsic', attr_key, attr_scope) != 0] AS attr_slot,
       [if(fs0s != 0, if(length(attr_val[fs0s]) <= 8192, attr_val[fs0s], substringUTF8(attr_val[fs0s], 1, 2048)), if(fs0r != 0, if(length(attr_val[fs0r]) <= 8192, attr_val[fs0r], substringUTF8(attr_val[fs0r], 1, 2048)), if(fs0e != 0, if(length(attr_val[fs0e]) <= 8192, attr_val[fs0e], substringUTF8(attr_val[fs0e], 1, 2048)), if(fs0l != 0, if(length(attr_val[fs0l]) <= 8192, attr_val[fs0l], substringUTF8(attr_val[fs0l], 1, 2048)), if(fs0i != 0, if(length(attr_val[fs0i]) <= 8192, attr_val[fs0i], substringUTF8(attr_val[fs0i], 1, 2048)), ''))))), '', ''] AS attr_slot_val,
       [if(fs0s != 0, attr_type[fs0s], if(fs0r != 0, attr_type[fs0r], if(fs0e != 0, attr_type[fs0e], if(fs0l != 0, attr_type[fs0l], if(fs0i != 0, attr_type[fs0i], ''))))), if(fa0s != 0, attr_type[fa0s], if(fa0r != 0, attr_type[fa0r], if(fa0e != 0, attr_type[fa0e], if(fa0l != 0, attr_type[fa0l], if(fa0i != 0, attr_type[fa0i], ''))))), ''] AS attr_slot_type,
       CAST([NULL, if(fa0s != 0, attr_num[fa0s], if(fa0r != 0, attr_num[fa0r], if(fa0e != 0, attr_num[fa0e], if(fa0l != 0, attr_num[fa0l], if(fa0i != 0, attr_num[fa0i], NULL))))), toFloat64(arrayCount((k, s) -> k = 'timeSinceStart' AND s = 'event:intrinsic', attr_key, attr_scope))] AS Array(Nullable(Float64))) AS attr_slot_num
FROM trace_spans
WHERE trace_id IN (unhex('000102030405060708090a0b0c0d0e0f'), unhex('101112131415161718191a1b1c1d1e1f'))
  AND timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000
ORDER BY trace_id ASC, timestamp_ns ASC, span_id ASC
LIMIT 10001 BY trace_id

== phase2 event set[0] ==
SELECT trace_id, span_id, arrayJoin(arrayFilter((n, k, s) -> k = 'timeSinceStart' AND s = 'event:intrinsic' AND isNotNull(n), attr_num, attr_key, attr_scope)) AS v
FROM (
  SELECT trace_id, span_id, attr_key, attr_scope, attr_num
  FROM trace_spans
  WHERE trace_id IN (unhex('000102030405060708090a0b0c0d0e0f'), unhex('101112131415161718191a1b1c1d1e1f'))
    AND timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000
  ORDER BY trace_id ASC, timestamp_ns ASC, span_id ASC
  LIMIT 10000 BY trace_id
)

== root hydration (sample winners) ==
SELECT trace_id, span_id, parent_id, if(length(service) <= 8192, service, substringUTF8(service, 1, 2048)) AS service, if(length(name) <= 8192, name, substringUTF8(name, 1, 2048)) AS name, timestamp_ns, duration_ns
FROM trace_spans
WHERE trace_id IN (unhex('000102030405060708090a0b0c0d0e0f'))
