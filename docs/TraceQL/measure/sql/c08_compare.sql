WITH base AS (SELECT status_code = 2 AS sel, name, kind, status_code, status_message,
               scope_name, scope_version, attrs, scope_attrs, events, links,
               resource_id, service, trace_id
        FROM tqd_g1.spans WHERE start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985 AND service = 'payment'),
     res AS (SELECT resource_id, any(attrs) AS rattrs FROM tqd_g1.resources GROUP BY resource_id),
     tr AS (SELECT trace_id, max(root_service) AS root_service, max(root_name) AS root_name
            FROM tqd_g1.traces GROUP BY trace_id),
     kv AS (
    SELECT sel, 'span' AS scope, kv.1 AS key,
           if(startsWith(kv.2, '"'), JSONExtractString(kv.2), kv.2) AS value,
           multiIf(startsWith(kv.2, '"'), 'string', kv.2 IN ('true', 'false'), 'bool',
                   match(kv.2, '[.eE]'), 'double', 'int') AS type
    FROM base
    ARRAY JOIN JSONExtractKeysAndValuesRaw(toString(attrs)) AS kv
    UNION ALL
    SELECT sel, 'instrumentation' AS scope, kv.1 AS key,
           if(startsWith(kv.2, '"'), JSONExtractString(kv.2), kv.2) AS value,
           multiIf(startsWith(kv.2, '"'), 'string', kv.2 IN ('true', 'false'), 'bool',
                   match(kv.2, '[.eE]'), 'double', 'int') AS type
    FROM base
    ARRAY JOIN JSONExtractKeysAndValuesRaw(toString(scope_attrs)) AS kv
    UNION ALL
    SELECT sel, 'resource' AS scope, kv.1 AS key,
           if(startsWith(kv.2, '"'), JSONExtractString(kv.2), kv.2) AS value,
           multiIf(startsWith(kv.2, '"'), 'string', kv.2 IN ('true', 'false'), 'bool',
                   match(kv.2, '[.eE]'), 'double', 'int') AS type
    FROM (SELECT b.sel AS sel, r.rattrs AS rattrs FROM base AS b INNER JOIN res AS r USING (resource_id))
    ARRAY JOIN JSONExtractKeysAndValuesRaw(toString(rattrs)) AS kv
    UNION ALL
    SELECT sel, 'event' AS scope, kv.1 AS key,
           if(startsWith(kv.2, '"'), JSONExtractString(kv.2), kv.2) AS value,
           multiIf(startsWith(kv.2, '"'), 'string', kv.2 IN ('true', 'false'), 'bool',
                   match(kv.2, '[.eE]'), 'double', 'int') AS type
    FROM (SELECT sel, ev.3 AS eattrs FROM base ARRAY JOIN events AS ev)
    ARRAY JOIN JSONExtractKeysAndValuesRaw(toString(eattrs)) AS kv
    UNION ALL
    SELECT sel, 'link' AS scope, kv.1 AS key,
           if(startsWith(kv.2, '"'), JSONExtractString(kv.2), kv.2) AS value,
           multiIf(startsWith(kv.2, '"'), 'string', kv.2 IN ('true', 'false'), 'bool',
                   match(kv.2, '[.eE]'), 'double', 'int') AS type
    FROM (SELECT sel, lk.5 AS lattrs FROM base ARRAY JOIN links AS lk)
    ARRAY JOIN JSONExtractKeysAndValuesRaw(toString(lattrs)) AS kv
    UNION ALL
    -- the service name lives on the span row, not inside the resource JSON (R1)
    SELECT sel, 'resource', 'service.name', service, 'string' FROM base
    UNION ALL
    SELECT sel, 'intrinsic', k, v, 'string' FROM
        (SELECT sel, ['name', 'kind', 'status', 'statusMessage',
                      'instrumentation:name', 'instrumentation:version'] AS ks,
                [toString(name),
                 -- the stored codes render as the keywords the API returns
                 -- (`crates/pulsus-read/src/traces/search_eval.rs:2318-2337`)
                 arrayElement(['unspecified', 'internal', 'server', 'client', 'producer', 'consumer'],
                              least(toInt32(kind), 5) + 1),
                 arrayElement(['unset', 'ok', 'error'], least(toInt32(status_code), 2) + 1),
                 status_message, toString(scope_name), toString(scope_version)] AS vs
         FROM base)
    ARRAY JOIN ks AS k, vs AS v
    UNION ALL
    SELECT sel, 'intrinsic', k, v, 'string' FROM
        (SELECT b.sel AS sel, ['trace:rootService', 'trace:rootName'] AS ks,
                [toString(t.root_service), toString(t.root_name)] AS vs
         FROM base AS b INNER JOIN tr AS t USING (trace_id))
    ARRAY JOIN ks AS k, vs AS v
    UNION ALL
    SELECT sel, 'event', 'name', toString(ev.2), 'string' FROM base ARRAY JOIN events AS ev
    UNION ALL
    SELECT sel, 'link', k, v, 'string' FROM
        (SELECT sel, ['traceId', 'spanId'] AS ks,
                [lower(hex(lk.1)), lower(hex(lk.2))] AS vs FROM base ARRAY JOIN links AS lk)
    ARRAY JOIN ks AS k, vs AS v)
SELECT scope, key, value, type, side, n
FROM (SELECT scope, key, value, type, if(sel, 'selection', 'baseline') AS side, count() AS n,
             row_number() OVER (PARTITION BY scope, key, side ORDER BY count() DESC, value ASC) AS rn
      FROM kv GROUP BY scope, key, value, type, side)
WHERE rn <= 10
ORDER BY scope, key, side, n DESC, value
