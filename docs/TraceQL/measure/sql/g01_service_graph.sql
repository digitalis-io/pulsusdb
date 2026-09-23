SELECT client, server, connection_type, count() AS calls,
       countIf(failed) AS failed,
       CAST(quantilesTDigest(0.5, 0.95, 0.99)(duration_ns) AS Array(Float64)) AS quantiles_ns
FROM (
    SELECT c.service AS client, s.service AS server,
           if(c.kind = 3, 'rpc', 'messaging') AS connection_type,
           (s.status_code = 2 OR c.status_code = 2) AS failed, s.duration_ns AS duration_ns
    FROM (SELECT trace_id, span_id, service, status_code, kind
          FROM tqd_g1.spans WHERE start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985 AND kind IN (3, 4)) AS c
    INNER JOIN (SELECT trace_id, parent_span_id, service, status_code, duration_ns, kind
                FROM tqd_g1.spans WHERE start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985 AND kind IN (2, 5) AND NOT coalesce(attrs.`zipkin%2Eshared`.:Bool, false)) AS s
        ON s.trace_id = c.trace_id AND s.parent_span_id = c.span_id
    WHERE (c.kind = 3 AND s.kind = 2) OR (c.kind = 4 AND s.kind = 5)
    UNION ALL
    -- the shared-span branch: one span id carries the client and server halves
    SELECT c.service, s.service, if(c.kind = 3, 'rpc', 'messaging'),
           (s.status_code = 2 OR c.status_code = 2), s.duration_ns
    FROM (SELECT trace_id, span_id, service, status_code, kind
          FROM tqd_g1.spans WHERE start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985 AND kind IN (3, 4)) AS c
    INNER JOIN (SELECT trace_id, span_id, service, status_code, duration_ns, kind
                FROM tqd_g1.spans WHERE start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985 AND kind IN (2, 5) AND coalesce(attrs.`zipkin%2Eshared`.:Bool, false)) AS s
        ON s.trace_id = c.trace_id AND s.span_id = c.span_id
    WHERE (c.kind = 3 AND s.kind = 2) OR (c.kind = 4 AND s.kind = 5))
GROUP BY client, server, connection_type
ORDER BY calls DESC, client ASC, server ASC, connection_type ASC
LIMIT 1001
