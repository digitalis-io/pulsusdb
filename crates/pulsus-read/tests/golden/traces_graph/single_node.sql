-- case: single_node
-- edges_table: trace_edges

SELECT
    c.service AS client,
    s.service AS server,
    s.conn_type AS conn_type,
    count() AS calls,
    countIf(greatest(s.failed, c.failed) = 1) AS failed,
    CAST(quantilesTDigest(0.5, 0.95, 0.99)(s.duration_ns) AS Array(Float64)) AS quantiles_ns
FROM
(
    SELECT trace_id, span_id, any(pair_id) AS pair_id, any(conn_type) AS conn_type,
           any(service) AS service, max(duration_ns) AS duration_ns, max(failed) AS failed
    FROM trace_edges
    WHERE side = 1 AND date >= toDate('2023-11-14') AND date <= toDate('2023-11-15')
      AND timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
    GROUP BY trace_id, span_id
) AS s
INNER JOIN
(
    SELECT trace_id, pair_id, any(conn_type) AS conn_type,
           any(service) AS service, max(failed) AS failed
    FROM trace_edges
    WHERE side = 0 AND date >= toDate('2023-11-14') AND date <= toDate('2023-11-15')
      AND timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000
    GROUP BY trace_id, pair_id
) AS c
ON c.trace_id = s.trace_id AND c.pair_id = s.pair_id AND c.conn_type = s.conn_type
GROUP BY client, server, conn_type
ORDER BY calls DESC, client ASC, server ASC
LIMIT 1001
