CREATE TABLE IF NOT EXISTS arch18r12_m.metric_samples_60s
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
    built_at Int64 CODEC(DoubleDelta, ZSTD(1)),
    PROJECTION by_parent (SELECT * ORDER BY (intDiv(t_end - 1, 720000), org_id, fingerprint, t_end))
)
ENGINE = MergeTree
PARTITION BY toDate(fromUnixTimestamp64Milli(t_end - 1), 'UTC')
ORDER BY (org_id, fingerprint, t_end)
TTL toDateTime(least(intDiv(t_end - 1, 86400000) * 86400 + (30 * 86400), 4294967295))
SETTINGS ttl_only_drop_parts = 1, index_granularity = 1024, non_replicated_deduplication_window = 10000
COMMENT 'rollup_res_ms=60000'
