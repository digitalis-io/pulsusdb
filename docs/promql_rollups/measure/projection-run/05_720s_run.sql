INSERT INTO arch18r12_m.metric_samples_720s
SELECT 0 AS kind, org_id, fingerprint, (intDiv(te - 1, 720000) + 1) * 720000 AS t_end,
  max(tl) AS t_last, argMax(lv, tl) AS last,
  minIf(mn, ct > 0) AS min, maxIf(mx, ct > 0) AS max,
  sum(sm) AS sum, toUInt32(sum(ct)) AS count, sum(ic) AS inc, toInt64(1792940000000) AS built_at
FROM (SELECT org_id, fingerprint, t_end AS te, t_last AS tl, last AS lv, min AS mn, max AS mx,
             sum AS sm, count AS ct, inc AS ic
      FROM arch18r12_m.metric_samples_60s
      WHERE kind = 0
        AND intDiv(t_end - 1, 720000) BETWEEN intDiv(1792937520000, 720000) AND intDiv(1792938240000 - 1, 720000)
        AND t_end > 1792937520000 AND t_end <= 1792938240000)
GROUP BY org_id, fingerprint, t_end
UNION ALL
SELECT 1, '', toUInt128(0), r.2, r.1, 0., 0., 0., 0., 0, 0., toInt64(1792940000000) FROM system.one
ARRAY JOIN arrayFilter(x -> x.1 < x.2,
    [(toInt64(1792937520000), least(toInt64(1792938240000), (intDiv(toInt64(1792937520000), 86400000) + 1) * 86400000)),
     (greatest(toInt64(1792937520000), (intDiv(toInt64(1792937520000), 86400000) + 1) * 86400000), toInt64(1792938240000))]) AS r
SETTINGS max_threads = 2, max_memory_usage = 4294967296, max_insert_threads = 1, max_insert_block_size = 100000000, min_insert_block_size_rows = 100000000, min_insert_block_size_bytes = 10000000000
