CREATE TABLE IF NOT EXISTS arch18r12_m.metric_landing
(
    org_id LowCardinality(String),
    received_ms Int64 CODEC(DoubleDelta, ZSTD(1)),
    kind UInt8 CODEC(ZSTD(1)),
    metric_name LowCardinality(String),
    fingerprint UInt128 CODEC(Delta(8), ZSTD(1)),
    unix_milli Int64 CODEC(DoubleDelta, ZSTD(1)),
    value Float64 CODEC(Gorilla(8), ZSTD(1)),
    labels String CODEC(ZSTD(5)),
    value_type UInt8 CODEC(ZSTD(1)),
    metric_type LowCardinality(String),
    help String CODEC(ZSTD(1)),
    unit String CODEC(ZSTD(1)),
    updated_ns Int64 CODEC(DoubleDelta, ZSTD(1)),
    hist_schema Int8 CODEC(ZSTD(1)),
    hist_zero_threshold Float64 CODEC(Gorilla(8), ZSTD(1)),
    hist_zero_count UInt64 CODEC(T64, ZSTD(1)),
    hist_count UInt64 CODEC(T64, ZSTD(1)),
    hist_sum Float64 CODEC(Gorilla(8), ZSTD(1)),
    hist_pos_span_offsets Array(Int32) CODEC(ZSTD(1)),
    hist_pos_span_lengths Array(UInt32) CODEC(ZSTD(1)),
    hist_pos_bucket_deltas Array(Int64) CODEC(ZSTD(1)),
    hist_neg_span_offsets Array(Int32) CODEC(ZSTD(1)),
    hist_neg_span_lengths Array(UInt32) CODEC(ZSTD(1)),
    hist_neg_bucket_deltas Array(Int64) CODEC(ZSTD(1)),
    hist_custom_values Array(Float64) CODEC(ZSTD(1)),
    hist_counter_reset_hint UInt8 CODEC(ZSTD(1)),
    ex_trace_id FixedString(16) CODEC(ZSTD(1)),
    ex_span_id FixedString(8) CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toStartOfHour(fromUnixTimestamp64Milli(received_ms))
ORDER BY (kind, intDiv(unix_milli, 300000), org_id, metric_name, fingerprint, unix_milli)
TTL toDateTime(least(intDiv(received_ms, 1000) + (6 * 3600), 4294967295))
SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192, min_columns_to_activate_adaptive_write_buffer = 1, non_replicated_deduplication_window = 10000;
