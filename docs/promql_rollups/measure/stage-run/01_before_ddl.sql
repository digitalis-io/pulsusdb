CREATE TABLE IF NOT EXISTS m800_a.metric_samples_60s
(
    kind UInt8,
    org_id LowCardinality(String),
    fingerprint UInt128 CODEC(ZSTD(1)),
    t_end Int64 CODEC(DoubleDelta, ZSTD(1)),
    t_last Int64 CODEC(DoubleDelta, ZSTD(1)),
    last Float64 CODEC(Gorilla(8), ZSTD(1)),
    min Float64 CODEC(Gorilla(8), ZSTD(1)),
    max Float64 CODEC(Gorilla(8), ZSTD(1)),
    sum Float64 CODEC(Gorilla(8), ZSTD(1)),
    count UInt32 CODEC(T64, ZSTD(1)),
    inc Float64 CODEC(Gorilla(8), ZSTD(1)),
    flags UInt8 CODEC(T64, ZSTD(1)),
    built_at Int64 CODEC(DoubleDelta, ZSTD(1))
    , PROJECTION by_parent (SELECT * ORDER BY (intDiv(t_end - 1, 720000), org_id, fingerprint, t_end))
)
ENGINE = MergeTree
PARTITION BY toDate(fromUnixTimestamp64Milli(t_end - 1), 'UTC')
ORDER BY (org_id, fingerprint, t_end)
TTL toDateTime(least((intDiv(t_end - 1, 86400000) + 1) * 86400 + (7 * 86400), 4294967295))
SETTINGS ttl_only_drop_parts = 1, index_granularity = 1024, non_replicated_deduplication_window = 10000
COMMENT 'rollup_res_ms=60000 rollup_from_ms=10800000 counter_buckets=5';

CREATE MATERIALIZED VIEW m800_a.metric_samples_60s_mv REFRESH EVERY 1 MINUTE APPEND TO m800_a.metric_samples_60s
AS WITH toUInt8(1) AS builder, toUInt256(1) AS n_shards, toUInt256(0) AS shard_ix,
     toUInt256('340282366920938463463374607431768211456') AS space,
     intDiv(space, greatest(n_shards, toUInt256(1))) AS slice_w,
     slice_w * shard_ix AS slice_lo,
     if(shard_ix + 1 = n_shards, space - 1, slice_w * (shard_ix + 1) - 1) AS slice_hi,
     toInt64(toUnixTimestamp64Milli(now64())) AS now_ms,
     (SELECT (lo, hi, lo < hi) FROM (
        SELECT intDiv(n - 150000, 60000) * 60000 AS ready,
               intDiv(n - 6 * 3600000 + 720000 + 60000 - 1, 60000) * 60000 AS oldest,
               if(w = 0, ready - 60000, greatest(w, oldest)) AS lo,
               least(ready, lo + 5 * 60000) AS hi
        FROM (SELECT toInt64(toUnixTimestamp64Milli(now64())) AS n, max(t_end) AS w
              FROM m800_a.metric_samples_60s WHERE org_id = '' AND fingerprint = 0 AND kind = 1))) AS plan,
     tupleElement(plan, 1) AS lo,
     tupleElement(plan, 2) AS hi,
     builder = 1 AND tupleElement(plan, 3) AS work
SELECT toUInt8(0) AS kind, org_id, fingerprint, (intDiv(unix_milli - 1, 60000) + 1) * 60000 AS t_end,
  maxIf(unix_milli, bits != 9218868437227405314) AS t_last, argMaxIf(value, (unix_milli, bits), bits != 9218868437227405314) AS last,
  minIf(value, NOT isNaN(value)) AS min, maxIf(value, NOT isNaN(value)) AS max,
  sumIf(value, NOT isNaN(value)) AS sum, toUInt32(countIf(NOT isNaN(value))) AS count,
  sum(d) AS inc, toUInt8(if(argMax(bits, (unix_milli, bits)) = 9218868437227405314, 1, 0) + if(countIf(isNaN(value) AND bits != 9218868437227405314) > 0, 2, 0)) AS flags, any(now_ms) AS built_at
FROM (
  SELECT org_id, fingerprint, unix_milli, bits, value,
    if(q IS NULL OR isNaN(value) OR isNaN(assumeNotNull(q)), 0.,
       if(value >= assumeNotNull(q), value - assumeNotNull(q), value)) AS d
  FROM (
    SELECT org_id, fingerprint, unix_milli, bits, value,
      if(isNull(q0), if(unix_milli - tp0 <= 86400000, p0, NULL), if(unix_milli - t0 <= 86400000, q0, NULL)) AS q
    FROM (
      SELECT org_id, fingerprint, unix_milli, bits, reinterpretAsFloat64(bits) AS value,
        anyLast(if(bits = 9218868437227405314, NULL, toNullable(reinterpretAsFloat64(bits)))) OVER (PARTITION BY org_id, fingerprint ORDER BY unix_milli, bits ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS q0,
        anyLast(if(bits = 9218868437227405314, NULL, toNullable(unix_milli))) OVER (PARTITION BY org_id, fingerprint ORDER BY unix_milli, bits ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS t0
      FROM (SELECT DISTINCT org_id, fingerprint, unix_milli, reinterpretAsUInt64(value) AS bits
FROM m800_a.metric_landing
            WHERE work AND kind = 0
              AND intDiv(unix_milli, 300000) BETWEEN intDiv(lo - 120000, 300000) AND intDiv(hi, 300000)
              AND unix_milli > lo - 120000 AND unix_milli <= hi
              AND fingerprint BETWEEN toUInt128(slice_lo) AND toUInt128(slice_hi))) AS w
    LEFT JOIN (
      SELECT org_id, fingerprint,
             toNullable(argMaxIf(last, t_last, t_last > 0)) AS p0,
             toNullable(maxIf(t_last, t_last > 0)) AS tp0
      FROM m800_a.metric_samples_60s
      WHERE kind = 0 AND t_end <= lo AND t_end > lo - 86400000
        AND (org_id, fingerprint) IN (SELECT org_id, fingerprint
FROM m800_a.metric_landing
                              WHERE work AND kind = 0
                                AND intDiv(unix_milli, 300000) BETWEEN intDiv(lo - 120000, 300000) AND intDiv(hi, 300000)
                                AND unix_milli > lo - 120000 AND unix_milli <= hi
                                AND fingerprint BETWEEN toUInt128(slice_lo) AND toUInt128(slice_hi)
                              GROUP BY org_id, fingerprint HAVING minIf(unix_milli, reinterpretAsUInt64(value) != 9218868437227405314) > lo)
      GROUP BY org_id, fingerprint) AS z USING (org_id, fingerprint))
  WHERE unix_milli > lo)
GROUP BY org_id, fingerprint, t_end
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), r.2, r.1, 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), now_ms
FROM system.one
ARRAY JOIN if(work, arrayMap(d -> (greatest(lo, toInt64(d) * 86400000), least(hi, (toInt64(d) + 1) * 86400000)),
                               range(toUInt64(intDiv(lo, 86400000)), toUInt64(intDiv(hi - 1, 86400000) + 1))),
                    CAST([], 'Array(Tuple(Int64, Int64))')) AS r
SETTINGS max_threads = 2, max_memory_usage = 4294967296, max_execution_time = 50,
         max_insert_threads = 1, max_insert_block_size = 100000000, min_insert_block_size_rows = 100000000, min_insert_block_size_bytes = 10000000000;

CREATE TABLE IF NOT EXISTS m800_a.metric_samples_720s
(
    kind UInt8,
    org_id LowCardinality(String),
    fingerprint UInt128 CODEC(ZSTD(1)),
    t_end Int64 CODEC(DoubleDelta, ZSTD(1)),
    t_last Int64 CODEC(DoubleDelta, ZSTD(1)),
    last Float64 CODEC(Gorilla(8), ZSTD(1)),
    min Float64 CODEC(Gorilla(8), ZSTD(1)),
    max Float64 CODEC(Gorilla(8), ZSTD(1)),
    sum Float64 CODEC(Gorilla(8), ZSTD(1)),
    count UInt32 CODEC(T64, ZSTD(1)),
    inc Float64 CODEC(Gorilla(8), ZSTD(1)),
    flags UInt8 CODEC(T64, ZSTD(1)),
    built_at Int64 CODEC(DoubleDelta, ZSTD(1))
    , PROJECTION by_parent (SELECT * ORDER BY (intDiv(t_end - 1, 7200000), org_id, fingerprint, t_end))
)
ENGINE = MergeTree
PARTITION BY toDate(fromUnixTimestamp64Milli(t_end - 1), 'UTC')
ORDER BY (org_id, fingerprint, t_end)
TTL toDateTime(least((intDiv(t_end - 1, 86400000) + 1) * 86400 + (7 * 86400), 4294967295))
SETTINGS ttl_only_drop_parts = 1, index_granularity = 1024, non_replicated_deduplication_window = 10000
COMMENT 'rollup_res_ms=720000 rollup_from_ms=57600000 counter_buckets=5';

CREATE MATERIALIZED VIEW m800_a.metric_samples_720s_mv REFRESH EVERY 12 MINUTE OFFSET 240 SECOND APPEND TO m800_a.metric_samples_720s
AS WITH toUInt8(1) AS builder, toUInt256(1) AS n_shards, toUInt256(0) AS shard_ix,
     toUInt256('340282366920938463463374607431768211456') AS space,
     intDiv(space, greatest(n_shards, toUInt256(1))) AS slice_w,
     slice_w * shard_ix AS slice_lo,
     if(shard_ix + 1 = n_shards, space - 1, slice_w * (shard_ix + 1) - 1) AS slice_hi,
     toInt64(toUnixTimestamp64Milli(now64())) AS now_ms,
     (SELECT (b - 720000, least(intDiv(c.2, 720000) * 720000, b + 9 * 720000), c.2 >= b) FROM (
        SELECT arrayFirst(m -> greatest(intDiv(m.1 + 720000 - 1, 720000) * 720000 + 720000, (intDiv(w, 720000) + 1) * 720000) <= m.2, u) AS c,
               greatest(intDiv(c.1 + 720000 - 1, 720000) * 720000 + 720000, (intDiv(w, 720000) + 1) * 720000) AS b
        FROM (SELECT any(w0) AS w,
                     arrayFold((acc, x) -> if(empty(acc) OR x.1 > acc[-1].2, arrayPushBack(acc, x), arrayPushBack(arrayPopBack(acc), (acc[-1].1, greatest(acc[-1].2, x.2)))),
                               arraySort(groupArray((t_last, t_end))), CAST([], 'Array(Tuple(Int64, Int64))')) AS u
              FROM (SELECT (SELECT max(t_end) FROM m800_a.metric_samples_720s WHERE org_id = '' AND fingerprint = 0 AND kind = 1) AS w0, t_last, t_end
                    FROM m800_a.metric_samples_60s WHERE org_id = '' AND fingerprint = 0 AND kind = 1)
              WHERE t_end > w0))) AS plan,
     tupleElement(plan, 1) AS lo,
     tupleElement(plan, 2) AS hi,
     builder = 1 AND tupleElement(plan, 3) AS work
SELECT toUInt8(0) AS kind, org_id, fingerprint, (intDiv(te - 1, 720000) + 1) * 720000 AS t_end,
  max(tl) AS t_last, argMax(lv, tl) AS last,
  minIf(mn, ct > 0) AS min, maxIf(mx, ct > 0) AS max,
  sum(sm) AS sum, toUInt32(sum(ct)) AS count, sum(ic) AS inc, toUInt8(bitAnd(argMax(fl, te), 1) + bitAnd(groupBitOr(fl), 2)) AS flags, any(now_ms) AS built_at
FROM (SELECT org_id, fingerprint, t_end AS te, t_last AS tl, last AS lv, min AS mn, max AS mx,
             sum AS sm, count AS ct, inc AS ic, flags AS fl
      FROM m800_a.metric_samples_60s
      WHERE work AND kind = 0
        AND intDiv(t_end - 1, 720000) BETWEEN intDiv(lo, 720000) AND intDiv(hi - 1, 720000)
        AND t_end > lo AND t_end <= hi)
GROUP BY org_id, fingerprint, t_end
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), r.2, r.1, 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), now_ms
FROM system.one
ARRAY JOIN if(work, arrayMap(d -> (greatest(lo, toInt64(d) * 86400000), least(hi, (toInt64(d) + 1) * 86400000)),
                               range(toUInt64(intDiv(lo, 86400000)), toUInt64(intDiv(hi - 1, 86400000) + 1))),
                    CAST([], 'Array(Tuple(Int64, Int64))')) AS r
SETTINGS max_threads = 2, max_memory_usage = 4294967296, max_execution_time = 50,
         max_insert_threads = 1, max_insert_block_size = 100000000, min_insert_block_size_rows = 100000000, min_insert_block_size_bytes = 10000000000;

CREATE TABLE IF NOT EXISTS m800_a.metric_samples_7200s
(
    kind UInt8,
    org_id LowCardinality(String),
    fingerprint UInt128 CODEC(ZSTD(1)),
    t_end Int64 CODEC(DoubleDelta, ZSTD(1)),
    t_last Int64 CODEC(DoubleDelta, ZSTD(1)),
    last Float64 CODEC(Gorilla(8), ZSTD(1)),
    min Float64 CODEC(Gorilla(8), ZSTD(1)),
    max Float64 CODEC(Gorilla(8), ZSTD(1)),
    sum Float64 CODEC(Gorilla(8), ZSTD(1)),
    count UInt32 CODEC(T64, ZSTD(1)),
    inc Float64 CODEC(Gorilla(8), ZSTD(1)),
    flags UInt8 CODEC(T64, ZSTD(1)),
    built_at Int64 CODEC(DoubleDelta, ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toDate(fromUnixTimestamp64Milli(t_end - 1), 'UTC')
ORDER BY (org_id, fingerprint, t_end)
TTL toDateTime(least((intDiv(t_end - 1, 86400000) + 1) * 86400 + (7 * 86400), 4294967295))
SETTINGS ttl_only_drop_parts = 1, index_granularity = 1024, non_replicated_deduplication_window = 10000
COMMENT 'rollup_res_ms=7200000 rollup_from_ms=604800000 counter_buckets=5';

CREATE MATERIALIZED VIEW m800_a.metric_samples_7200s_mv REFRESH EVERY 2 HOUR OFFSET 300 SECOND APPEND TO m800_a.metric_samples_7200s
AS WITH toUInt8(1) AS builder, toUInt256(1) AS n_shards, toUInt256(0) AS shard_ix,
     toUInt256('340282366920938463463374607431768211456') AS space,
     intDiv(space, greatest(n_shards, toUInt256(1))) AS slice_w,
     slice_w * shard_ix AS slice_lo,
     if(shard_ix + 1 = n_shards, space - 1, slice_w * (shard_ix + 1) - 1) AS slice_hi,
     toInt64(toUnixTimestamp64Milli(now64())) AS now_ms,
     (SELECT (b - 7200000, least(intDiv(c.2, 7200000) * 7200000, b + 9 * 7200000), c.2 >= b) FROM (
        SELECT arrayFirst(m -> greatest(intDiv(m.1 + 7200000 - 1, 7200000) * 7200000 + 7200000, (intDiv(w, 7200000) + 1) * 7200000) <= m.2, u) AS c,
               greatest(intDiv(c.1 + 7200000 - 1, 7200000) * 7200000 + 7200000, (intDiv(w, 7200000) + 1) * 7200000) AS b
        FROM (SELECT any(w0) AS w,
                     arrayFold((acc, x) -> if(empty(acc) OR x.1 > acc[-1].2, arrayPushBack(acc, x), arrayPushBack(arrayPopBack(acc), (acc[-1].1, greatest(acc[-1].2, x.2)))),
                               arraySort(groupArray((t_last, t_end))), CAST([], 'Array(Tuple(Int64, Int64))')) AS u
              FROM (SELECT (SELECT max(t_end) FROM m800_a.metric_samples_7200s WHERE org_id = '' AND fingerprint = 0 AND kind = 1) AS w0, t_last, t_end
                    FROM m800_a.metric_samples_720s WHERE org_id = '' AND fingerprint = 0 AND kind = 1)
              WHERE t_end > w0))) AS plan,
     tupleElement(plan, 1) AS lo,
     tupleElement(plan, 2) AS hi,
     builder = 1 AND tupleElement(plan, 3) AS work
SELECT toUInt8(0) AS kind, org_id, fingerprint, (intDiv(te - 1, 7200000) + 1) * 7200000 AS t_end,
  max(tl) AS t_last, argMax(lv, tl) AS last,
  minIf(mn, ct > 0) AS min, maxIf(mx, ct > 0) AS max,
  sum(sm) AS sum, toUInt32(sum(ct)) AS count, sum(ic) AS inc, toUInt8(bitAnd(argMax(fl, te), 1) + bitAnd(groupBitOr(fl), 2)) AS flags, any(now_ms) AS built_at
FROM (SELECT org_id, fingerprint, t_end AS te, t_last AS tl, last AS lv, min AS mn, max AS mx,
             sum AS sm, count AS ct, inc AS ic, flags AS fl
      FROM m800_a.metric_samples_720s
      WHERE work AND kind = 0
        AND intDiv(t_end - 1, 7200000) BETWEEN intDiv(lo, 7200000) AND intDiv(hi - 1, 7200000)
        AND t_end > lo AND t_end <= hi)
GROUP BY org_id, fingerprint, t_end
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), r.2, r.1, 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), now_ms
FROM system.one
ARRAY JOIN if(work, arrayMap(d -> (greatest(lo, toInt64(d) * 86400000), least(hi, (toInt64(d) + 1) * 86400000)),
                               range(toUInt64(intDiv(lo, 86400000)), toUInt64(intDiv(hi - 1, 86400000) + 1))),
                    CAST([], 'Array(Tuple(Int64, Int64))')) AS r
SETTINGS max_threads = 2, max_memory_usage = 4294967296, max_execution_time = 50,
         max_insert_threads = 1, max_insert_block_size = 100000000, min_insert_block_size_rows = 100000000, min_insert_block_size_bytes = 10000000000;
