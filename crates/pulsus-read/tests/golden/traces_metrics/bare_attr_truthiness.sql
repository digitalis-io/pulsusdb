-- case: bare_attr_truthiness
-- q: { .flag } | rate()

== range (query_range) ==
WITH arrayFirstIndex((k, s) -> k = 'flag' AND s = 'span', attr_key, attr_scope) AS pi0s,
     arrayFirstIndex((k, s) -> k = 'flag' AND s = 'resource', attr_key, attr_scope) AS pi0r,
     arrayFirstIndex((k, s) -> k = 'flag' AND s = 'event', attr_key, attr_scope) AS pi0e,
     arrayFirstIndex((k, s) -> k = 'flag' AND s = 'link', attr_key, attr_scope) AS pi0l,
     arrayFirstIndex((k, s) -> k = 'flag' AND s = 'instrumentation', attr_key, attr_scope) AS pi0i,
     arrayFirstIndex((k, s, v) -> k = 'flag' AND s = 'event' AND v = 'true', attr_key, attr_scope, attr_val) AS pm0e,
     arrayFirstIndex((k, s, v) -> k = 'flag' AND s = 'link' AND v = 'true', attr_key, attr_scope, attr_val) AS pm0l
SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns - 1), INTERVAL 60000000000 NANOSECOND)) + 60000 AS t,
       uniqExact(trace_id, span_id) AS n
FROM trace_spans
WHERE timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
  AND (if(pi0s != 0, attr_val[pi0s] = 'true', if(pi0r != 0, attr_val[pi0r] = 'true', if(pi0e != 0, pm0e != 0, if(pi0l != 0, pm0l != 0, if(pi0i != 0, attr_val[pi0i] = 'true', 0))))))
GROUP BY t
ORDER BY t ASC

== instant (query) ==
WITH arrayFirstIndex((k, s) -> k = 'flag' AND s = 'span', attr_key, attr_scope) AS pi0s,
     arrayFirstIndex((k, s) -> k = 'flag' AND s = 'resource', attr_key, attr_scope) AS pi0r,
     arrayFirstIndex((k, s) -> k = 'flag' AND s = 'event', attr_key, attr_scope) AS pi0e,
     arrayFirstIndex((k, s) -> k = 'flag' AND s = 'link', attr_key, attr_scope) AS pi0l,
     arrayFirstIndex((k, s) -> k = 'flag' AND s = 'instrumentation', attr_key, attr_scope) AS pi0i,
     arrayFirstIndex((k, s, v) -> k = 'flag' AND s = 'event' AND v = 'true', attr_key, attr_scope, attr_val) AS pm0e,
     arrayFirstIndex((k, s, v) -> k = 'flag' AND s = 'link' AND v = 'true', attr_key, attr_scope, attr_val) AS pm0l
SELECT uniqExact(trace_id, span_id) AS n
FROM trace_spans
WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
  AND (if(pi0s != 0, attr_val[pi0s] = 'true', if(pi0r != 0, attr_val[pi0r] = 'true', if(pi0e != 0, pm0e != 0, if(pi0l != 0, pm0l != 0, if(pi0i != 0, attr_val[pi0i] = 'true', 0))))))

== exemplars ==
WITH arrayFirstIndex((k, s) -> k = 'flag' AND s = 'span', attr_key, attr_scope) AS pi0s,
     arrayFirstIndex((k, s) -> k = 'flag' AND s = 'resource', attr_key, attr_scope) AS pi0r,
     arrayFirstIndex((k, s) -> k = 'flag' AND s = 'event', attr_key, attr_scope) AS pi0e,
     arrayFirstIndex((k, s) -> k = 'flag' AND s = 'link', attr_key, attr_scope) AS pi0l,
     arrayFirstIndex((k, s) -> k = 'flag' AND s = 'instrumentation', attr_key, attr_scope) AS pi0i,
     arrayFirstIndex((k, s, v) -> k = 'flag' AND s = 'event' AND v = 'true', attr_key, attr_scope, attr_val) AS pm0e,
     arrayFirstIndex((k, s, v) -> k = 'flag' AND s = 'link' AND v = 'true', attr_key, attr_scope, attr_val) AS pm0l
SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns - 1), INTERVAL 60000000000 NANOSECOND)) + 60000 AS t,
       groupArraySample(1, 1)(tuple(trace_id, timestamp_ns)) AS ex
FROM trace_spans
WHERE timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001
  AND (if(pi0s != 0, attr_val[pi0s] = 'true', if(pi0r != 0, attr_val[pi0r] = 'true', if(pi0e != 0, pm0e != 0, if(pi0l != 0, pm0l != 0, if(pi0i != 0, attr_val[pi0i] = 'true', 0))))))
GROUP BY t
ORDER BY t ASC
