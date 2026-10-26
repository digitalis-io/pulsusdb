INSERT INTO arch18r12_m.metric_samples_60s
SELECT 0 AS kind, org_id, fingerprint, (intDiv(unix_milli - 1, 60000) + 1) * 60000 AS t_end,
  max(unix_milli) AS t_last, argMax(value, (unix_milli, bits)) AS last,
  minIf(value, NOT isNaN(value)) AS min, maxIf(value, NOT isNaN(value)) AS max,
  sumIf(value, NOT isNaN(value)) AS sum, toUInt32(countIf(NOT isNaN(value))) AS count,
  sum(d) AS inc, toInt64(1792941704000) AS built_at
FROM (
  SELECT org_id, fingerprint, unix_milli, bits, value,
    if(q IS NULL OR isNaN(value) OR isNaN(assumeNotNull(q)), 0.,
       if(value >= assumeNotNull(q), value - assumeNotNull(q), value)) AS d
  FROM (
    SELECT org_id, fingerprint, unix_milli, bits, reinterpretAsFloat64(bits) AS value,
      lagInFrame(toNullable(reinterpretAsFloat64(bits))) OVER (PARTITION BY org_id, fingerprint
          ORDER BY unix_milli, bits ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS q
    FROM (SELECT DISTINCT org_id, fingerprint, unix_milli, reinterpretAsUInt64(value) AS bits
          FROM arch18r12_m.metric_landing
          WHERE kind = 0
            AND intDiv(unix_milli, 300000) BETWEEN intDiv(1792936800000 - 120000, 300000) AND intDiv(1792940400000, 300000)
            AND unix_milli > 1792936800000 - 120000 AND unix_milli <= 1792940400000
            AND fingerprint BETWEEN toUInt128(0) AND toUInt128('340282366920938463463374607431768211455')))
  WHERE unix_milli > 1792936800000)
GROUP BY org_id, fingerprint, t_end
UNION ALL
SELECT 1, '', toUInt128(0), r.2, r.1, 0., 0., 0., 0., 0, 0., toInt64(1792941704000) FROM system.one
ARRAY JOIN arrayFilter(x -> x.1 < x.2,
    [(toInt64(1792936800000), least(toInt64(1792940400000), (intDiv(toInt64(1792936800000), 86400000) + 1) * 86400000)),
     (greatest(toInt64(1792936800000), (intDiv(toInt64(1792936800000), 86400000) + 1) * 86400000), toInt64(1792940400000))]) AS r
SETTINGS max_threads = 2, max_memory_usage = 4294967296, max_insert_threads = 1, max_insert_block_size = 100000000, min_insert_block_size_rows = 100000000, min_insert_block_size_bytes = 10000000000
