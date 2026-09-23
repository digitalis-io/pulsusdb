SELECT series, groupArray((t, v)) AS points
FROM (SELECT service AS series, (intDiv(start_ns - 1, 60000000000) + 1) * 60000 AS t, count() AS v
      FROM tqd_g1.spans FINAL
      WHERE start_ns >= 1790084760000000000 AND start_ns < 1790095620000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985 AND (1)
      GROUP BY series, t
      ORDER BY t)
GROUP BY series
ORDER BY series
