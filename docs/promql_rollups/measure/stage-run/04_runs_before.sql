-- m800_a unmerged 720 ten r1
INSERT INTO m800_a.metric_samples_720s (kind, org_id, fingerprint, t_end, t_last, built_at) VALUES (1, '', 0, 1794250800000, 1794250080000, 1);

-- m800_a unmerged 720 ten r1
INSERT INTO m800_a.metric_samples_720s
WITH toUInt8(1) AS builder, toUInt256(1) AS n_shards, toUInt256(0) AS shard_ix,
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

-- m800_a unmerged 720 ten r2
INSERT INTO m800_a.metric_samples_720s (kind, org_id, fingerprint, t_end, t_last, built_at) VALUES (1, '', 0, 1794250800000, 1794250080000, 1);

-- m800_a unmerged 720 ten r2
INSERT INTO m800_a.metric_samples_720s
WITH toUInt8(1) AS builder, toUInt256(1) AS n_shards, toUInt256(0) AS shard_ix,
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

-- m800_a unmerged 720 ten r3
INSERT INTO m800_a.metric_samples_720s (kind, org_id, fingerprint, t_end, t_last, built_at) VALUES (1, '', 0, 1794250800000, 1794250080000, 1);

-- m800_a unmerged 720 ten r3
INSERT INTO m800_a.metric_samples_720s
WITH toUInt8(1) AS builder, toUInt256(1) AS n_shards, toUInt256(0) AS shard_ix,
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

-- m800_a unmerged 720 one r1
INSERT INTO m800_a.metric_samples_720s (kind, org_id, fingerprint, t_end, t_last, built_at) VALUES (1, '', 0, 1794257280000, 1794256560000, 1);

-- m800_a unmerged 720 one r1
INSERT INTO m800_a.metric_samples_720s
WITH toUInt8(1) AS builder, toUInt256(1) AS n_shards, toUInt256(0) AS shard_ix,
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

-- m800_a unmerged 720 one r2
INSERT INTO m800_a.metric_samples_720s (kind, org_id, fingerprint, t_end, t_last, built_at) VALUES (1, '', 0, 1794257280000, 1794256560000, 1);

-- m800_a unmerged 720 one r2
INSERT INTO m800_a.metric_samples_720s
WITH toUInt8(1) AS builder, toUInt256(1) AS n_shards, toUInt256(0) AS shard_ix,
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

-- m800_a unmerged 720 one r3
INSERT INTO m800_a.metric_samples_720s (kind, org_id, fingerprint, t_end, t_last, built_at) VALUES (1, '', 0, 1794257280000, 1794256560000, 1);

-- m800_a unmerged 720 one r3
INSERT INTO m800_a.metric_samples_720s
WITH toUInt8(1) AS builder, toUInt256(1) AS n_shards, toUInt256(0) AS shard_ix,
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

-- m800_a unmerged 720 full: until a run writes no run row
INSERT INTO m800_a.metric_samples_720s
WITH toUInt8(1) AS builder, toUInt256(1) AS n_shards, toUInt256(0) AS shard_ix,
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

-- m800_a unmerged 7200 ten r1
INSERT INTO m800_a.metric_samples_7200s (kind, org_id, fingerprint, t_end, t_last, built_at) VALUES (1, '', 0, 1794182400000, 1794175200000, 1);

-- m800_a unmerged 7200 ten r1
INSERT INTO m800_a.metric_samples_7200s
WITH toUInt8(1) AS builder, toUInt256(1) AS n_shards, toUInt256(0) AS shard_ix,
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

-- m800_a unmerged 7200 ten r2
INSERT INTO m800_a.metric_samples_7200s (kind, org_id, fingerprint, t_end, t_last, built_at) VALUES (1, '', 0, 1794182400000, 1794175200000, 1);

-- m800_a unmerged 7200 ten r2
INSERT INTO m800_a.metric_samples_7200s
WITH toUInt8(1) AS builder, toUInt256(1) AS n_shards, toUInt256(0) AS shard_ix,
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

-- m800_a unmerged 7200 ten r3
INSERT INTO m800_a.metric_samples_7200s (kind, org_id, fingerprint, t_end, t_last, built_at) VALUES (1, '', 0, 1794182400000, 1794175200000, 1);

-- m800_a unmerged 7200 ten r3
INSERT INTO m800_a.metric_samples_7200s
WITH toUInt8(1) AS builder, toUInt256(1) AS n_shards, toUInt256(0) AS shard_ix,
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

-- m800_a unmerged 7200 one r1
INSERT INTO m800_a.metric_samples_7200s (kind, org_id, fingerprint, t_end, t_last, built_at) VALUES (1, '', 0, 1794247200000, 1794240000000, 1);

-- m800_a unmerged 7200 one r1
INSERT INTO m800_a.metric_samples_7200s
WITH toUInt8(1) AS builder, toUInt256(1) AS n_shards, toUInt256(0) AS shard_ix,
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

-- m800_a unmerged 7200 one r2
INSERT INTO m800_a.metric_samples_7200s (kind, org_id, fingerprint, t_end, t_last, built_at) VALUES (1, '', 0, 1794247200000, 1794240000000, 1);

-- m800_a unmerged 7200 one r2
INSERT INTO m800_a.metric_samples_7200s
WITH toUInt8(1) AS builder, toUInt256(1) AS n_shards, toUInt256(0) AS shard_ix,
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

-- m800_a unmerged 7200 one r3
INSERT INTO m800_a.metric_samples_7200s (kind, org_id, fingerprint, t_end, t_last, built_at) VALUES (1, '', 0, 1794247200000, 1794240000000, 1);

-- m800_a unmerged 7200 one r3
INSERT INTO m800_a.metric_samples_7200s
WITH toUInt8(1) AS builder, toUInt256(1) AS n_shards, toUInt256(0) AS shard_ix,
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

-- m800_a merged 720 ten r1
INSERT INTO m800_a.metric_samples_720s (kind, org_id, fingerprint, t_end, t_last, built_at) VALUES (1, '', 0, 1794250800000, 1794250080000, 1);

-- m800_a merged 720 ten r1
INSERT INTO m800_a.metric_samples_720s
WITH toUInt8(1) AS builder, toUInt256(1) AS n_shards, toUInt256(0) AS shard_ix,
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

-- m800_a merged 720 ten r2
INSERT INTO m800_a.metric_samples_720s (kind, org_id, fingerprint, t_end, t_last, built_at) VALUES (1, '', 0, 1794250800000, 1794250080000, 1);

-- m800_a merged 720 ten r2
INSERT INTO m800_a.metric_samples_720s
WITH toUInt8(1) AS builder, toUInt256(1) AS n_shards, toUInt256(0) AS shard_ix,
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

-- m800_a merged 720 ten r3
INSERT INTO m800_a.metric_samples_720s (kind, org_id, fingerprint, t_end, t_last, built_at) VALUES (1, '', 0, 1794250800000, 1794250080000, 1);

-- m800_a merged 720 ten r3
INSERT INTO m800_a.metric_samples_720s
WITH toUInt8(1) AS builder, toUInt256(1) AS n_shards, toUInt256(0) AS shard_ix,
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

-- m800_a merged 720 one r1
INSERT INTO m800_a.metric_samples_720s (kind, org_id, fingerprint, t_end, t_last, built_at) VALUES (1, '', 0, 1794257280000, 1794256560000, 1);

-- m800_a merged 720 one r1
INSERT INTO m800_a.metric_samples_720s
WITH toUInt8(1) AS builder, toUInt256(1) AS n_shards, toUInt256(0) AS shard_ix,
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

-- m800_a merged 720 one r2
INSERT INTO m800_a.metric_samples_720s (kind, org_id, fingerprint, t_end, t_last, built_at) VALUES (1, '', 0, 1794257280000, 1794256560000, 1);

-- m800_a merged 720 one r2
INSERT INTO m800_a.metric_samples_720s
WITH toUInt8(1) AS builder, toUInt256(1) AS n_shards, toUInt256(0) AS shard_ix,
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

-- m800_a merged 720 one r3
INSERT INTO m800_a.metric_samples_720s (kind, org_id, fingerprint, t_end, t_last, built_at) VALUES (1, '', 0, 1794257280000, 1794256560000, 1);

-- m800_a merged 720 one r3
INSERT INTO m800_a.metric_samples_720s
WITH toUInt8(1) AS builder, toUInt256(1) AS n_shards, toUInt256(0) AS shard_ix,
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

-- m800_a merged 720 full: until a run writes no run row
INSERT INTO m800_a.metric_samples_720s
WITH toUInt8(1) AS builder, toUInt256(1) AS n_shards, toUInt256(0) AS shard_ix,
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

-- m800_a merged 7200 ten r1
INSERT INTO m800_a.metric_samples_7200s (kind, org_id, fingerprint, t_end, t_last, built_at) VALUES (1, '', 0, 1794182400000, 1794175200000, 1);

-- m800_a merged 7200 ten r1
INSERT INTO m800_a.metric_samples_7200s
WITH toUInt8(1) AS builder, toUInt256(1) AS n_shards, toUInt256(0) AS shard_ix,
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

-- m800_a merged 7200 ten r2
INSERT INTO m800_a.metric_samples_7200s (kind, org_id, fingerprint, t_end, t_last, built_at) VALUES (1, '', 0, 1794182400000, 1794175200000, 1);

-- m800_a merged 7200 ten r2
INSERT INTO m800_a.metric_samples_7200s
WITH toUInt8(1) AS builder, toUInt256(1) AS n_shards, toUInt256(0) AS shard_ix,
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

-- m800_a merged 7200 ten r3
INSERT INTO m800_a.metric_samples_7200s (kind, org_id, fingerprint, t_end, t_last, built_at) VALUES (1, '', 0, 1794182400000, 1794175200000, 1);

-- m800_a merged 7200 ten r3
INSERT INTO m800_a.metric_samples_7200s
WITH toUInt8(1) AS builder, toUInt256(1) AS n_shards, toUInt256(0) AS shard_ix,
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

-- m800_a merged 7200 one r1
INSERT INTO m800_a.metric_samples_7200s (kind, org_id, fingerprint, t_end, t_last, built_at) VALUES (1, '', 0, 1794247200000, 1794240000000, 1);

-- m800_a merged 7200 one r1
INSERT INTO m800_a.metric_samples_7200s
WITH toUInt8(1) AS builder, toUInt256(1) AS n_shards, toUInt256(0) AS shard_ix,
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

-- m800_a merged 7200 one r2
INSERT INTO m800_a.metric_samples_7200s (kind, org_id, fingerprint, t_end, t_last, built_at) VALUES (1, '', 0, 1794247200000, 1794240000000, 1);

-- m800_a merged 7200 one r2
INSERT INTO m800_a.metric_samples_7200s
WITH toUInt8(1) AS builder, toUInt256(1) AS n_shards, toUInt256(0) AS shard_ix,
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

-- m800_a merged 7200 one r3
INSERT INTO m800_a.metric_samples_7200s (kind, org_id, fingerprint, t_end, t_last, built_at) VALUES (1, '', 0, 1794247200000, 1794240000000, 1);

-- m800_a merged 7200 one r3
INSERT INTO m800_a.metric_samples_7200s
WITH toUInt8(1) AS builder, toUInt256(1) AS n_shards, toUInt256(0) AS shard_ix,
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
