SELECT pod AS series, groupArray((t, v)) AS points
FROM (SELECT r.pod, a.t, sum(a.n) AS v
      FROM (SELECT resource_id, (intDiv(start_ns - 1, 60000000000) + 1) * 60000 AS t, count() AS n
            FROM tqd_g1.spans WHERE start_ns >= 1790084760000000000 AND start_ns < 1790095620000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985 GROUP BY resource_id, t) AS a
      INNER JOIN (SELECT resource_id, any(attrs.`k8s%2Epod%2Ename`.:String) AS pod FROM tqd_g1.resources
                  WHERE day >= toDate(fromUnixTimestamp64Nano(1790084760000000000)) AND day <= toDate(fromUnixTimestamp64Nano(1790095620000000000 - 1))
                  GROUP BY resource_id) AS r USING resource_id
      GROUP BY r.pod, a.t
      ORDER BY a.t)
GROUP BY series
ORDER BY series
