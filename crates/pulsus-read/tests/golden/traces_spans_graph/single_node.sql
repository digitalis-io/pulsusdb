-- case: single_node
-- target: none

SELECT client, server, conn_type, sum(calls) AS calls, sum(failed) AS failed,
       CAST(quantilesTDigestMerge(0.5, 0.95, 0.99)(q) AS Array(Float64)) AS quantiles_ns
FROM (
WITH (SELECT arraySort(groupArray(minute)) FROM service_edges FINAL WHERE conn_type = '') AS ms,
     ms[1] AS first,
     ms[arrayFirstIndex(i -> i = length(ms) OR ms[i + 1] - ms[i] != 300, arrayEnumerate(ms))] AS last,
     greatest(1699999980000000000, toInt64(toUnixTimestamp(first)) * 1000000000 - 240000000000) AS A,
     least(1700010840000000000, toInt64(toUnixTimestamp(last)) * 1000000000 + 60000000000) AS B,
     if(A < B, if(A > 1699999980000000000, A + 60000000000, 1699999980000000000), 1700010840000000000) AS h1,
     if(A < B, if(B < 1700010840000000000, B - 60000000000, 1700010840000000000), 1700010840000000000) AS t0
SELECT client, server, conn_type, calls, failed, q
FROM service_edges FINAL
WHERE conn_type IN ('rpc', 'messaging')
  AND minute >= toDateTime(intDiv(A, 1000000000), 'UTC') AND minute < toDateTime(intDiv(B, 1000000000), 'UTC')
  AND client_minute >= toDateTime(intDiv(A, 1000000000), 'UTC') AND client_minute < toDateTime(intDiv(B, 1000000000), 'UTC')
UNION ALL
SELECT c.1 AS client, s.2 AS server, if(c.3 = 3, 'rpc', 'messaging') AS conn_type,
       count() AS calls, countIf(s.3 = 2 OR c.2 = 2) AS failed,
       quantilesTDigestState(0.5, 0.95, 0.99)(s.4) AS q
FROM (SELECT trace_id,
             if(kind IN (3, 4) OR attrs.`zipkin%2Eshared`.:String = 'true', span_id, parent_span_id) AS k,
             anyIf((service, status_code, kind, start_ns), kind IN (3, 4)) AS c,
             countIf(kind IN (3, 4)) AS nc,
             groupArrayIf((span_id, service, status_code, duration_ns, kind, start_ns), kind IN (2, 5)) AS ss
      FROM spans
      WHERE ((start_ns >= 1699999980000000000 AND start_ns < h1
              AND intDiv(start_ns, 300000000000) >= 5666666 AND intDiv(start_ns, 300000000000) <= intDiv(h1 - 1, 300000000000))
          OR (start_ns >= t0 AND start_ns < 1700010840000000000
              AND intDiv(start_ns, 300000000000) >= intDiv(t0, 300000000000) AND intDiv(start_ns, 300000000000) <= 5666702))
        AND kind IN (2, 3, 4, 5)
      GROUP BY trace_id, k
      HAVING nc > 0)
ARRAY JOIN arrayDistinct(ss) AS s
WHERE ((c.3 = 3 AND s.5 = 2) OR (c.3 = 4 AND s.5 = 5))
  AND NOT (s.6 >= A AND s.6 < B AND c.4 >= A AND c.4 < B)
GROUP BY client, server, conn_type
)
GROUP BY client, server, conn_type
ORDER BY calls DESC, client ASC, server ASC, conn_type ASC
LIMIT 1001
