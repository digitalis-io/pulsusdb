SELECT name, avg(duration_ns) AS v
FROM tqd_g1.spans
WHERE start_ns >= 1790084801000000000 AND start_ns < 1790095601000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966949 AND 5966985 AND service = 'frontend'
GROUP BY name
ORDER BY name
