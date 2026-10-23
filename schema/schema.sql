-- The pulsusdb schema. `schema/schema.sh` applies it; nothing else does.
--
-- ONE FILE, TWO VARIANTS. A line prefixed `--@single` is used on a single
-- node and a line prefixed `--@cluster` on a cluster; every other line is
-- used by both. A statement that exists in one variant only carries the
-- prefix on all of its lines.
--
-- TOKENS ARE DOUBLE-BRACE. `{{db}}`, `{{on_cluster}}` and the rest are
-- substituted by the script and by `pulsus-schema`'s own renderer.
-- `{shard}` and `{replica}` are ClickHouse's own macros and must reach the
-- server as those exact characters: never write a single-brace token here,
-- and never add a substitution that matches one.
--
-- A REPLICATION PATH NAMES ITS OWN TABLE. The paths are written out rather
-- than derived, so a wrong one is a per-table error. Nineteen tables take
-- `/clickhouse/tables/{shard}/` and six take `/clickhouse/tables/all/`
-- (docs/architecture.md §3). `tests/schema_file.rs` holds the relation.
--
-- A STATEMENT ENDS AT A LINE ENDING IN A SEMICOLON, and no other character
-- in this file is one: the HTTP interface refuses a multi-statement body,
-- so the script splits on that and sends one statement per request.
--
-- THERE ARE NO MIGRATIONS. A schema change is an edit here and another run
-- of the script, which drops the database and builds it again.

CREATE DATABASE IF NOT EXISTS {{db}}{{on_cluster}} COMMENT 'retention_days={{retention_days}}';

CREATE TABLE IF NOT EXISTS {{db}}.log_landing{{on_cluster}}
(
    org_id LowCardinality(String),
    event_id UUID DEFAULT generateUUIDv7(),
    received_ms Int64 CODEC(DoubleDelta, ZSTD(1)),
    kind UInt8 CODEC(ZSTD(1)),
    service LowCardinality(String),
    fingerprint UInt128 CODEC(Delta(8), ZSTD(1)),
    timestamp_ns Int64 CODEC(DoubleDelta, ZSTD(1)),
    severity Int8 CODEC(ZSTD(1)),
    body String CODEC(ZSTD(1)),
    structured_metadata String CODEC(ZSTD(1)),
    month Date CODEC(ZSTD(1)),
    labels String CODEC(ZSTD(5)),
    updated_ns Int64 CODEC(DoubleDelta, ZSTD(1)),
    pattern String CODEC(ZSTD(1)),
    pattern_count UInt64 CODEC(T64, ZSTD(1)), pattern_level LowCardinality(String)
)
--@single  ENGINE = MergeTree
--@cluster ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{{db}}.log_landing', '{replica}')
PARTITION BY toStartOfHour(fromUnixTimestamp64Milli(received_ms))
ORDER BY (org_id, kind, service, fingerprint, timestamp_ns)
TTL toDateTime(least(intDiv(received_ms, 1000) + ({{log_landing_retention_hours}} * 3600), 4294967295))
--@single  SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192, min_columns_to_activate_adaptive_write_buffer = 1, non_replicated_deduplication_window = {{log_dedup_window}}{{storage_policy}};
--@cluster SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192, min_columns_to_activate_adaptive_write_buffer = 1, replicated_deduplication_window = {{log_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.log_metrics_{{log_rollup_suffix}}{{on_cluster}}
(
    org_id LowCardinality(String),
    fingerprint UInt128,
    bucket_ns Int64,
    count SimpleAggregateFunction(sum, UInt64),
    bytes SimpleAggregateFunction(sum, UInt64),
    count_at SimpleAggregateFunction(sum, UInt64),
    bytes_at SimpleAggregateFunction(sum, UInt64),
    PROJECTION by_time (SELECT org_id, bucket_ns, fingerprint ORDER BY (org_id, bucket_ns, fingerprint))
)
--@single  ENGINE = AggregatingMergeTree
--@cluster ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/{{db}}.log_metrics_{{log_rollup_suffix}}', '{replica}')
PARTITION BY toDate(fromUnixTimestamp64Nano(bucket_ns))
ORDER BY (org_id, fingerprint, bucket_ns)
TTL toDateTime(least(intDiv(bucket_ns, 1000000000) + ({{retention_days}} * 86400), 4294967295))
--@single  SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192, deduplicate_merge_projection_mode = 'rebuild', lightweight_mutation_projection_mode = 'rebuild', non_replicated_deduplication_window = {{log_dedup_window}}{{storage_policy}};
--@cluster SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192, deduplicate_merge_projection_mode = 'rebuild', lightweight_mutation_projection_mode = 'rebuild', replicated_deduplication_window = {{log_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.log_patterns{{on_cluster}}
(
    org_id LowCardinality(String),
    fingerprint UInt128,
    bucket_ns Int64,
    pattern String CODEC(ZSTD(1)), level LowCardinality(String),
    count SimpleAggregateFunction(sum, UInt64)
)
--@single  ENGINE = AggregatingMergeTree
--@cluster ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/{{db}}.log_patterns', '{replica}')
PARTITION BY toDate(fromUnixTimestamp64Nano(bucket_ns))
ORDER BY (org_id, fingerprint, bucket_ns, pattern, level)
TTL toDateTime(least(intDiv(bucket_ns, 1000000000) + ({{retention_days}} * 86400), 4294967295))
--@single  SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192, non_replicated_deduplication_window = {{log_dedup_window}}{{storage_policy}};
--@cluster SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192, replicated_deduplication_window = {{log_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.log_samples{{on_cluster}}
(
    org_id LowCardinality(String),
    service LowCardinality(String),
    fingerprint UInt128,
    timestamp_ns Int64 CODEC(DoubleDelta, ZSTD(1)),
    severity Int8 DEFAULT 0,
    body String CODEC(ZSTD(1)),
    structured_metadata String DEFAULT '',
    INDEX idx_body_ngrams body TYPE text(tokenizer = ngrams(4)),
    INDEX idx_severity severity TYPE minmax GRANULARITY 4
)
--@single  ENGINE = MergeTree
--@cluster ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{{db}}.log_samples', '{replica}')
PARTITION BY toDate(fromUnixTimestamp64Nano(timestamp_ns))
ORDER BY (org_id, service, fingerprint, timestamp_ns)
TTL toDateTime(least(intDiv(timestamp_ns, 1000000000) + ({{retention_days}} * 86400), 4294967295))
--@single  SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192, min_columns_to_activate_adaptive_write_buffer = 1, non_replicated_deduplication_window = {{log_dedup_window}}{{storage_policy}};
--@cluster SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192, min_columns_to_activate_adaptive_write_buffer = 1, replicated_deduplication_window = {{log_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.log_streams{{on_cluster}}
(
    org_id LowCardinality(String),
    month Date,
    fingerprint UInt128,
    service LowCardinality(String),
    labels String CODEC(ZSTD(5)),
    updated_ns Int64
)
--@single  ENGINE = ReplacingMergeTree(updated_ns)
--@cluster ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{{db}}.log_streams', '{replica}', updated_ns)
PARTITION BY month
ORDER BY (org_id, fingerprint)
--@single  SETTINGS index_granularity = 8192, non_replicated_deduplication_window = {{log_dedup_window}}{{storage_policy}};
--@cluster SETTINGS index_granularity = 8192, replicated_deduplication_window = {{log_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.log_streams_idx{{on_cluster}}
(
    org_id LowCardinality(String),
    month Date,
    key LowCardinality(String),
    val String,
    fingerprint UInt128
)
--@single  ENGINE = ReplacingMergeTree
--@cluster ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{{db}}.log_streams_idx', '{replica}')
PARTITION BY month
ORDER BY (org_id, key, val, fingerprint)
--@single  SETTINGS index_granularity = 8192, non_replicated_deduplication_window = {{log_dedup_window}}{{storage_policy}};
--@cluster SETTINGS index_granularity = 8192, replicated_deduplication_window = {{log_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.metric_exemplars{{on_cluster}}
(
    org_id LowCardinality(String),
    fingerprint UInt128 CODEC(ZSTD(1)),
    unix_milli Int64 CODEC(DoubleDelta, ZSTD(1)),
    value Float64 CODEC(Gorilla(8), ZSTD(1)),
    trace_id FixedString(16) CODEC(ZSTD(1)),
    span_id FixedString(8) CODEC(ZSTD(1)),
    labels String CODEC(ZSTD(5))
)
--@single  ENGINE = MergeTree
--@cluster ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{{db}}.metric_exemplars', '{replica}')
PARTITION BY toDate(fromUnixTimestamp64Milli(unix_milli))
ORDER BY (org_id, fingerprint, unix_milli)
TTL toDateTime(least(intDiv(unix_milli, 1000) + ({{retention_days}} * 86400), 4294967295))
--@single  SETTINGS ttl_only_drop_parts = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 1, index_granularity = 1024, min_columns_to_activate_adaptive_write_buffer = 1, non_replicated_deduplication_window = {{metrics_dedup_window}}{{storage_policy}};
--@cluster SETTINGS ttl_only_drop_parts = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 1, index_granularity = 1024, min_columns_to_activate_adaptive_write_buffer = 1, replicated_deduplication_window = {{metrics_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.metric_hist_samples{{on_cluster}}
(
    org_id LowCardinality(String),
    fingerprint UInt128 CODEC(ZSTD(1)),
    unix_milli Int64 CODEC(DoubleDelta, ZSTD(1)),
    schema Int8 CODEC(ZSTD(1)),
    zero_threshold Float64 CODEC(Gorilla(8), ZSTD(1)),
    zero_count UInt64 CODEC(T64, ZSTD(1)),
    count UInt64 CODEC(T64, ZSTD(1)),
    sum Float64 CODEC(Gorilla(8), ZSTD(1)),
    pos_span_offsets Array(Int32) CODEC(ZSTD(1)),
    pos_span_lengths Array(UInt32) CODEC(ZSTD(1)),
    pos_bucket_deltas Array(Int64) CODEC(ZSTD(1)),
    neg_span_offsets Array(Int32) CODEC(ZSTD(1)),
    neg_span_lengths Array(UInt32) CODEC(ZSTD(1)),
    neg_bucket_deltas Array(Int64) CODEC(ZSTD(1)),
    custom_values Array(Float64) CODEC(ZSTD(1)),
    counter_reset_hint UInt8 DEFAULT 0
)
--@single  ENGINE = MergeTree
--@cluster ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{{db}}.metric_hist_samples', '{replica}')
PARTITION BY toDate(fromUnixTimestamp64Milli(unix_milli))
ORDER BY (org_id, fingerprint, unix_milli)
TTL toDateTime(least(intDiv(unix_milli, 1000) + ({{retention_days}} * 86400), 4294967295))
--@single  SETTINGS ttl_only_drop_parts = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 1, index_granularity = 8192, min_columns_to_activate_adaptive_write_buffer = 1, non_replicated_deduplication_window = {{metrics_dedup_window}}{{storage_policy}};
--@cluster SETTINGS ttl_only_drop_parts = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 1, index_granularity = 8192, min_columns_to_activate_adaptive_write_buffer = 1, replicated_deduplication_window = {{metrics_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.metric_labels{{on_cluster}}
(
    org_id LowCardinality(String),
    metric_name LowCardinality(String),
    fingerprint UInt128 CODEC(Delta(8), ZSTD(1)),
    labels String CODEC(ZSTD(5)),
    first_seen SimpleAggregateFunction(min, Int64) CODEC(ZSTD(1)),
    last_seen SimpleAggregateFunction(max, Int64) CODEC(ZSTD(1))
)
--@single  ENGINE = AggregatingMergeTree
--@cluster ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/{{db}}.metric_labels', '{replica}')
ORDER BY (org_id, metric_name, fingerprint)
-- `labels` is outside the key and not an aggregate. It is a function of
-- `fingerprint`, so a merge never chooses between two different values;
-- 26.8 refuses such a column unless the table allows it.
--@single  SETTINGS allow_dimensions_outside_sorting_key = 1, index_granularity = 8192, min_columns_to_activate_adaptive_write_buffer = 1, non_replicated_deduplication_window = {{metrics_dedup_window}}{{storage_policy}};
--@cluster SETTINGS allow_dimensions_outside_sorting_key = 1, index_granularity = 8192, min_columns_to_activate_adaptive_write_buffer = 1, replicated_deduplication_window = {{metrics_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.metric_label_index{{on_cluster}}
(
    org_id LowCardinality(String),
    key LowCardinality(String),
    value String CODEC(ZSTD(1)),
    fingerprint UInt128 CODEC(ZSTD(1))
)
--@single  ENGINE = ReplacingMergeTree
--@cluster ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{{db}}.metric_label_index', '{replica}')
ORDER BY (org_id, key, value, fingerprint)
--@single  SETTINGS index_granularity = 8192, min_columns_to_activate_adaptive_write_buffer = 1, non_replicated_deduplication_window = {{metrics_dedup_window}}{{storage_policy}};
--@cluster SETTINGS index_granularity = 8192, min_columns_to_activate_adaptive_write_buffer = 1, replicated_deduplication_window = {{metrics_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.metric_label_values{{on_cluster}}
(
    org_id LowCardinality(String),
    key LowCardinality(String),
    value String CODEC(ZSTD(1))
)
--@single  ENGINE = ReplacingMergeTree
--@cluster ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{{db}}.metric_label_values', '{replica}')
ORDER BY (org_id, key, value)
--@single  SETTINGS index_granularity = 8192, non_replicated_deduplication_window = {{metrics_dedup_window}}{{storage_policy}};
--@cluster SETTINGS index_granularity = 8192, replicated_deduplication_window = {{metrics_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.metric_landing{{on_cluster}}
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
--@single  ENGINE = MergeTree
--@cluster ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{{db}}.metric_landing', '{replica}')
PARTITION BY toStartOfHour(fromUnixTimestamp64Milli(received_ms))
ORDER BY (org_id, kind, metric_name, fingerprint, unix_milli)
TTL toDateTime(least(intDiv(received_ms, 1000) + ({{metrics_landing_retention_hours}} * 3600), 4294967295))
--@single  SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192, min_columns_to_activate_adaptive_write_buffer = 1, non_replicated_deduplication_window = {{metrics_dedup_window}}{{storage_policy}};
--@cluster SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192, min_columns_to_activate_adaptive_write_buffer = 1, replicated_deduplication_window = {{metrics_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.metric_metadata{{on_cluster}}
(
    org_id LowCardinality(String),
    metric_name LowCardinality(String),
    metric_type LowCardinality(String),
    help String,
    unit String,
    updated_ns Int64
)
--@single  ENGINE = ReplacingMergeTree(updated_ns)
--@cluster ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/all/{{db}}.metric_metadata', '{replica}', updated_ns)
ORDER BY (org_id, metric_name, metric_type, help, unit)
TTL toDateTime(least(intDiv(updated_ns, 1000000000) + ({{retention_days}} * 86400), 4294967295))
--@single  SETTINGS index_granularity = 8192, non_replicated_deduplication_window = {{metrics_dedup_window}}{{storage_policy}};
--@cluster SETTINGS index_granularity = 8192, replicated_deduplication_window = {{metrics_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.metric_samples{{on_cluster}}
(
    org_id LowCardinality(String),
    fingerprint UInt128 CODEC(ZSTD(1)),
    unix_milli Int64 CODEC(DoubleDelta, ZSTD(1)),
    value Float64 CODEC(Gorilla(8), ZSTD(1))
)
--@single  ENGINE = MergeTree
--@cluster ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{{db}}.metric_samples', '{replica}')
PARTITION BY toDate(fromUnixTimestamp64Milli(unix_milli))
ORDER BY (org_id, fingerprint, unix_milli)
TTL toDateTime(least(intDiv(unix_milli, 1000) + ({{retention_days}} * 86400), 4294967295))
--@single  SETTINGS ttl_only_drop_parts = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 1, index_granularity = 8192, min_columns_to_activate_adaptive_write_buffer = 1, non_replicated_deduplication_window = {{metrics_dedup_window}}{{storage_policy}};
--@cluster SETTINGS ttl_only_drop_parts = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 1, index_granularity = 8192, min_columns_to_activate_adaptive_write_buffer = 1, replicated_deduplication_window = {{metrics_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.metric_series{{on_cluster}}
(
    org_id LowCardinality(String),
    day Date,
    fingerprint UInt128 CODEC(ZSTD(1)),
    metric_name LowCardinality(String),
    hours SimpleAggregateFunction(groupBitOr, UInt32)
)
--@single  ENGINE = AggregatingMergeTree
--@cluster ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/{{db}}.metric_series', '{replica}')
PARTITION BY day
ORDER BY (org_id, fingerprint)
TTL toDateTime(least((toUInt64(toUInt16(day)) + 1 + {{retention_days}}) * 86400, 4294967295))
-- `metric_name` is outside the key and not an aggregate. It is a function
-- of `fingerprint`, so a merge never chooses between two different values;
-- 26.8 refuses such a column unless the table allows it.
--@single  SETTINGS allow_dimensions_outside_sorting_key = 1, ttl_only_drop_parts = 1, index_granularity = 8192, non_replicated_deduplication_window = {{metrics_dedup_window}}{{storage_policy}};
--@cluster SETTINGS allow_dimensions_outside_sorting_key = 1, ttl_only_drop_parts = 1, index_granularity = 8192, replicated_deduplication_window = {{metrics_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.resources{{on_cluster}}
(
    day Date CODEC(ZSTD(1)),
    resource_id UInt128 CODEC(ZSTD(1)),
    service LowCardinality(String) CODEC(ZSTD(1)),
    attrs JSON CODEC(ZSTD(1)),
    attrs_other String CODEC(ZSTD(1)),
    dropped_attrs UInt32 CODEC(ZSTD(1)),
    schema_url String CODEC(ZSTD(1)),
    entity_refs String CODEC(ZSTD(1))
)
--@single  ENGINE = ReplacingMergeTree
--@cluster ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/all/{{db}}.resources', '{replica}')
PARTITION BY day
ORDER BY (service, resource_id)
TTL toDateTime(least(((toUInt32(day) + 1) * 86400) + ({{retention_days}} * 86400), 4294967295))
--@single  SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, non_replicated_deduplication_window = {{trace_dedup_window}}{{storage_policy}};
--@cluster SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, replicated_deduplication_window = {{trace_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.spans{{on_cluster}}
(
    trace_id FixedString(16) CODEC(ZSTD(1)),
    span_id FixedString(8) CODEC(ZSTD(1)),
    parent_span_id FixedString(8) CODEC(ZSTD(1)),
    start_ns Int64 CODEC(Delta(8), ZSTD(1)),
    duration_ns Int64 CODEC(T64, ZSTD(1)),
    service LowCardinality(String) CODEC(ZSTD(1)),
    resource_id UInt128 CODEC(ZSTD(1)),
    name LowCardinality(String) CODEC(ZSTD(1)),
    kind Int32 CODEC(ZSTD(1)),
    status_code Int32 CODEC(ZSTD(1)),
    status_message String CODEC(ZSTD(1)),
    trace_state String CODEC(ZSTD(1)),
    flags UInt32 CODEC(ZSTD(1)),
    scope_name LowCardinality(String) CODEC(ZSTD(1)),
    scope_version LowCardinality(String) CODEC(ZSTD(1)),
    scope_attrs JSON CODEC(ZSTD(1)),
    attrs JSON CODEC(ZSTD(1)),
    attrs_other String CODEC(ZSTD(1)),
    dropped_attrs UInt32 CODEC(ZSTD(1)),
    events Array(Tuple(
        time_ns UInt64,
        name LowCardinality(String),
        attrs JSON,
        attrs_other String,
        dropped_attrs UInt32)) CODEC(ZSTD(1)),
    dropped_events UInt32 CODEC(ZSTD(1)),
    links Array(Tuple(
        trace_id String,
        span_id String,
        trace_state String,
        flags UInt32,
        attrs JSON,
        attrs_other String,
        dropped_attrs UInt32)) CODEC(ZSTD(1)),
    dropped_links UInt32 CODEC(ZSTD(1)),
    scope_schema_url String CODEC(ZSTD(1)),
    scope_dropped_attrs UInt32 CODEC(ZSTD(1)),
    scope_attrs_other String CODEC(ZSTD(1)),
    end_ns UInt64 CODEC(Delta(8), ZSTD(1)),
    service_type LowCardinality(String) CODEC(ZSTD(1)),
    INDEX idx_service service TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_name name TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_resource resource_id TYPE bloom_filter(0.01) GRANULARITY 1{{trace_attr_indexes}}
)
--@single  ENGINE = ReplacingMergeTree
--@cluster ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{{db}}.spans', '{replica}')
PARTITION BY toDate(fromUnixTimestamp64Nano(start_ns), 'UTC')
ORDER BY (intDiv(start_ns, 300000000000), trace_id, start_ns, span_id, kind)
TTL toDateTime(least(intDiv(start_ns, 1000000000) + ({{retention_days}} * 86400), 4294967295))
--@single  SETTINGS ttl_only_drop_parts = 1, index_granularity = 2048, min_columns_to_activate_adaptive_write_buffer = 1, non_replicated_deduplication_window = {{trace_dedup_window}}{{storage_policy}};
--@cluster SETTINGS ttl_only_drop_parts = 1, index_granularity = 2048, min_columns_to_activate_adaptive_write_buffer = 1, replicated_deduplication_window = {{trace_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.tag_names{{on_cluster}}
(
    scope LowCardinality(String) CODEC(ZSTD(1)),
    key String CODEC(ZSTD(1))
)
--@single  ENGINE = ReplacingMergeTree
--@cluster ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/all/{{db}}.tag_names', '{replica}')
ORDER BY (scope, key)
--@single  SETTINGS index_granularity = 8192, non_replicated_deduplication_window = {{trace_dedup_window}}{{storage_policy}};
--@cluster SETTINGS index_granularity = 8192, replicated_deduplication_window = {{trace_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.tag_values{{on_cluster}}
(
    scope LowCardinality(String) CODEC(ZSTD(1)),
    key String CODEC(ZSTD(1)),
    value String CODEC(ZSTD(1)),
    val_type LowCardinality(String) CODEC(ZSTD(1))
)
--@single  ENGINE = ReplacingMergeTree
--@cluster ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/all/{{db}}.tag_values', '{replica}')
ORDER BY (scope, key, value, val_type)
--@single  SETTINGS index_granularity = 8192, non_replicated_deduplication_window = {{trace_dedup_window}}{{storage_policy}};
--@cluster SETTINGS index_granularity = 8192, replicated_deduplication_window = {{trace_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.trace_landing{{on_cluster}}
(
    event_id UUID DEFAULT generateUUIDv7(),
    received_ms Int64 CODEC(DoubleDelta, ZSTD(1)),
    row_kind UInt8 CODEC(ZSTD(1)),
    trace_id FixedString(16) CODEC(ZSTD(1)),
    span_id FixedString(8) CODEC(ZSTD(1)),
    parent_span_id FixedString(8) CODEC(ZSTD(1)),
    start_ns Int64 CODEC(Delta(8), ZSTD(1)),
    duration_ns Int64 CODEC(T64, ZSTD(1)),
    resource_id UInt128 CODEC(ZSTD(1)),
    name LowCardinality(String) CODEC(ZSTD(1)),
    kind Int32 CODEC(ZSTD(1)),
    status_code Int32 CODEC(ZSTD(1)),
    status_message String CODEC(ZSTD(1)),
    trace_state String CODEC(ZSTD(1)),
    flags UInt32 CODEC(ZSTD(1)),
    scope_name LowCardinality(String) CODEC(ZSTD(1)),
    scope_version LowCardinality(String) CODEC(ZSTD(1)),
    scope_attrs JSON CODEC(ZSTD(1)),
    events Array(Tuple(
        time_ns UInt64,
        name LowCardinality(String),
        attrs JSON,
        attrs_other String,
        dropped_attrs UInt32)) CODEC(ZSTD(1)),
    dropped_events UInt32 CODEC(ZSTD(1)),
    links Array(Tuple(
        trace_id String,
        span_id String,
        trace_state String,
        flags UInt32,
        attrs JSON,
        attrs_other String,
        dropped_attrs UInt32)) CODEC(ZSTD(1)),
    dropped_links UInt32 CODEC(ZSTD(1)),
    service LowCardinality(String) CODEC(ZSTD(1)),
    attrs JSON CODEC(ZSTD(1)),
    attrs_other String CODEC(ZSTD(1)),
    dropped_attrs UInt32 CODEC(ZSTD(1)),
    day Date CODEC(ZSTD(1)),
    schema_url String CODEC(ZSTD(1)),
    tag_scope LowCardinality(String) CODEC(ZSTD(1)),
    tag_key String CODEC(ZSTD(1)),
    tag_value String CODEC(ZSTD(1)),
    tag_type LowCardinality(String) CODEC(ZSTD(1)),
    scope_schema_url String CODEC(ZSTD(1)),
    scope_dropped_attrs UInt32 CODEC(ZSTD(1)),
    scope_attrs_other String CODEC(ZSTD(1)),
    end_ns UInt64 CODEC(Delta(8), ZSTD(1)),
    entity_refs String CODEC(ZSTD(1)),
    service_type LowCardinality(String) CODEC(ZSTD(1))
)
--@single  ENGINE = MergeTree
--@cluster ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{{db}}.trace_landing', '{replica}')
PARTITION BY toStartOfHour(fromUnixTimestamp64Milli(received_ms))
ORDER BY (row_kind, trace_id, start_ns, span_id, kind, tag_key, tag_value)
TTL toDateTime(least(intDiv(received_ms, 1000) + ({{trace_landing_retention_hours}} * 3600), 4294967295))
--@single  SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, async_insert = 0, index_granularity = 8192, non_replicated_deduplication_window = {{trace_dedup_window}}{{storage_policy}};
--@cluster SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, async_insert = 0, index_granularity = 8192, replicated_deduplication_window = {{trace_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.traces{{on_cluster}}
(
    day Date CODEC(ZSTD(1)),
    trace_id FixedString(16) CODEC(ZSTD(1)),
    start_ns SimpleAggregateFunction(min, Int64) CODEC(ZSTD(1)),
    end_ns SimpleAggregateFunction(max, Int64) CODEC(ZSTD(1)),
    root SimpleAggregateFunction(min, Tuple(UInt8, Int64, FixedString(8), String, String)) CODEC(ZSTD(1)),
    services SimpleAggregateFunction(groupUniqArrayArray, Array(String)) CODEC(ZSTD(1)),
    last_start_ns SimpleAggregateFunction(max, Int64) CODEC(Delta(8), ZSTD(1)),
    buckets SimpleAggregateFunction(groupUniqArrayArray(4096), Array(Int64)) CODEC(ZSTD(1))
)
--@single  ENGINE = AggregatingMergeTree
--@cluster ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/{{db}}.traces', '{replica}')
PARTITION BY day
ORDER BY trace_id
TTL toDateTime(least(intDiv(last_start_ns, 1000000000) + ({{retention_days}} * 86400), 4294967295))
--@single  SETTINGS index_granularity = 1024, ttl_only_drop_parts = 1, non_replicated_deduplication_window = {{trace_dedup_window}}{{storage_policy}};
--@cluster SETTINGS index_granularity = 1024, ttl_only_drop_parts = 1, replicated_deduplication_window = {{trace_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

--@cluster CREATE TABLE IF NOT EXISTS {{db}}.log_metrics_{{log_rollup_suffix}}{{dist_suffix}}{{on_cluster}} AS {{db}}.log_metrics_{{log_rollup_suffix}}
--@cluster ENGINE = Distributed('{{cluster}}', '{{db}}', 'log_metrics_{{log_rollup_suffix}}', cityHash64(fingerprint));

--@cluster CREATE TABLE IF NOT EXISTS {{db}}.log_patterns{{dist_suffix}}{{on_cluster}} AS {{db}}.log_patterns
--@cluster ENGINE = Distributed('{{cluster}}', '{{db}}', 'log_patterns', cityHash64(fingerprint));

--@cluster CREATE TABLE IF NOT EXISTS {{db}}.log_samples{{dist_suffix}}{{on_cluster}} AS {{db}}.log_samples
--@cluster ENGINE = Distributed('{{cluster}}', '{{db}}', 'log_samples', cityHash64(fingerprint));

--@cluster CREATE TABLE IF NOT EXISTS {{db}}.log_streams_idx{{dist_suffix}}{{on_cluster}} AS {{db}}.log_streams_idx
--@cluster ENGINE = Distributed('{{cluster}}', '{{db}}', 'log_streams_idx', cityHash64(fingerprint));

--@cluster CREATE TABLE IF NOT EXISTS {{db}}.log_streams{{dist_suffix}}{{on_cluster}} AS {{db}}.log_streams
--@cluster ENGINE = Distributed('{{cluster}}', '{{db}}', 'log_streams', cityHash64(fingerprint));

--@cluster CREATE TABLE IF NOT EXISTS {{db}}.metric_exemplars{{dist_suffix}}{{on_cluster}} AS {{db}}.metric_exemplars
--@cluster ENGINE = Distributed('{{cluster}}', '{{db}}', 'metric_exemplars', cityHash64(fingerprint));

--@cluster CREATE TABLE IF NOT EXISTS {{db}}.metric_hist_samples{{dist_suffix}}{{on_cluster}} AS {{db}}.metric_hist_samples
--@cluster ENGINE = Distributed('{{cluster}}', '{{db}}', 'metric_hist_samples', cityHash64(fingerprint));

--@cluster CREATE TABLE IF NOT EXISTS {{db}}.metric_labels{{dist_suffix}}{{on_cluster}} AS {{db}}.metric_labels
--@cluster ENGINE = Distributed('{{cluster}}', '{{db}}', 'metric_labels', cityHash64(fingerprint));

--@cluster CREATE TABLE IF NOT EXISTS {{db}}.metric_label_index{{dist_suffix}}{{on_cluster}} AS {{db}}.metric_label_index
--@cluster ENGINE = Distributed('{{cluster}}', '{{db}}', 'metric_label_index', cityHash64(fingerprint));

--@cluster CREATE TABLE IF NOT EXISTS {{db}}.metric_samples{{dist_suffix}}{{on_cluster}} AS {{db}}.metric_samples
--@cluster ENGINE = Distributed('{{cluster}}', '{{db}}', 'metric_samples', cityHash64(fingerprint));

--@cluster CREATE TABLE IF NOT EXISTS {{db}}.metric_series{{dist_suffix}}{{on_cluster}} AS {{db}}.metric_series
--@cluster ENGINE = Distributed('{{cluster}}', '{{db}}', 'metric_series', cityHash64(fingerprint));

--@cluster CREATE TABLE IF NOT EXISTS {{db}}.spans{{dist_suffix}}{{on_cluster}} AS {{db}}.spans
--@cluster ENGINE = Distributed('{{cluster}}', '{{db}}', 'spans', cityHash64(trace_id))
--@cluster SETTINGS fsync_after_insert = 1, fsync_directories = 1;

--@cluster CREATE TABLE IF NOT EXISTS {{db}}.traces{{dist_suffix}}{{on_cluster}} AS {{db}}.traces
--@cluster ENGINE = Distributed('{{cluster}}', '{{db}}', 'traces', cityHash64(trace_id))
--@cluster SETTINGS fsync_after_insert = 1, fsync_directories = 1;

CREATE MATERIALIZED VIEW {{db}}.log_metrics_{{log_rollup_suffix}}_mv{{on_cluster}} TO {{db}}.log_metrics_{{log_rollup_suffix}}
AS SELECT
    org_id AS org_id,
    fingerprint AS fingerprint,
    intDiv(timestamp_ns, {{log_rollup_ns}}) * {{log_rollup_ns}} AS bucket_ns,
    count() AS count,
    sum(length(body)) AS bytes,
    countIf(timestamp_ns = bucket_ns) AS count_at,
    sumIf(length(body), timestamp_ns = bucket_ns) AS bytes_at
FROM {{db}}.log_landing
WHERE kind = 0
GROUP BY
    org_id,
    fingerprint,
    bucket_ns;

CREATE MATERIALIZED VIEW {{db}}.log_patterns_mv{{on_cluster}} TO {{db}}.log_patterns
AS SELECT
    org_id AS org_id,
    fingerprint AS fingerprint,
    timestamp_ns AS bucket_ns,
    pattern AS pattern, pattern_level AS level,
    pattern_count AS count
FROM {{db}}.log_landing
WHERE kind = 2;

CREATE MATERIALIZED VIEW {{db}}.log_samples_mv{{on_cluster}} TO {{db}}.log_samples
AS SELECT
    org_id AS org_id,
    service AS service,
    fingerprint AS fingerprint,
    timestamp_ns AS timestamp_ns,
    severity AS severity,
    body AS body,
    structured_metadata AS structured_metadata
FROM {{db}}.log_landing
WHERE kind = 0;

CREATE MATERIALIZED VIEW {{db}}.log_streams_idx_mv{{on_cluster}} TO {{db}}.log_streams_idx
AS SELECT
    org_id AS org_id,
    month,
    kv.1 AS key,
    kv.2 AS val,
    fingerprint
FROM {{db}}.log_landing
ARRAY JOIN JSONExtractKeysAndValues(labels, 'String') AS kv
WHERE kind = 1;

CREATE MATERIALIZED VIEW {{db}}.log_streams_mv{{on_cluster}} TO {{db}}.log_streams
AS SELECT
    org_id AS org_id,
    month AS month,
    fingerprint AS fingerprint,
    service AS service,
    labels AS labels,
    updated_ns AS updated_ns
FROM {{db}}.log_landing
WHERE kind = 1;

CREATE MATERIALIZED VIEW {{db}}.metric_exemplars_mv{{on_cluster}} TO {{db}}.metric_exemplars
AS SELECT
    org_id AS org_id,
    fingerprint AS fingerprint,
    unix_milli AS unix_milli,
    value AS value,
    ex_trace_id AS trace_id,
    ex_span_id AS span_id,
    labels AS labels
FROM {{db}}.metric_landing
WHERE kind = 4;

CREATE MATERIALIZED VIEW {{db}}.metric_hist_samples_mv{{on_cluster}} TO {{db}}.metric_hist_samples
AS SELECT
    org_id AS org_id,
    fingerprint AS fingerprint,
    unix_milli AS unix_milli,
    hist_schema AS schema,
    hist_zero_threshold AS zero_threshold,
    hist_zero_count AS zero_count,
    hist_count AS count,
    hist_sum AS sum,
    hist_pos_span_offsets AS pos_span_offsets,
    hist_pos_span_lengths AS pos_span_lengths,
    hist_pos_bucket_deltas AS pos_bucket_deltas,
    hist_neg_span_offsets AS neg_span_offsets,
    hist_neg_span_lengths AS neg_span_lengths,
    hist_neg_bucket_deltas AS neg_bucket_deltas,
    hist_custom_values AS custom_values,
    hist_counter_reset_hint AS counter_reset_hint
FROM {{db}}.metric_landing
WHERE kind = 1;

CREATE MATERIALIZED VIEW {{db}}.metric_labels_mv{{on_cluster}} TO {{db}}.metric_labels
AS SELECT
    org_id AS org_id,
    metric_name AS metric_name,
    fingerprint AS fingerprint,
    labels AS labels,
    unix_milli AS first_seen,
    unix_milli AS last_seen
FROM {{db}}.metric_landing
WHERE kind = 2;

CREATE MATERIALIZED VIEW {{db}}.metric_label_index_mv{{on_cluster}} TO {{db}}.metric_label_index
AS SELECT
    org_id AS org_id,
    kv.1 AS key,
    kv.2 AS value,
    fingerprint AS fingerprint
FROM {{db}}.metric_landing
ARRAY JOIN JSONExtractKeysAndValues(labels, 'String') AS kv
WHERE kind = 2;

CREATE MATERIALIZED VIEW {{db}}.metric_label_values_mv{{on_cluster}} TO {{db}}.metric_label_values
AS SELECT DISTINCT
    org_id AS org_id,
    kv.1 AS key,
    kv.2 AS value
FROM {{db}}.metric_landing
ARRAY JOIN JSONExtractKeysAndValues(labels, 'String') AS kv
WHERE kind = 2;

CREATE MATERIALIZED VIEW {{db}}.metric_metadata_mv{{on_cluster}} TO {{db}}.metric_metadata
AS SELECT
    org_id AS org_id,
    metric_name AS metric_name,
    metric_type AS metric_type,
    help AS help,
    unit AS unit,
    updated_ns AS updated_ns
FROM {{db}}.metric_landing
WHERE kind = 3;

CREATE MATERIALIZED VIEW {{db}}.metric_samples_mv{{on_cluster}} TO {{db}}.metric_samples
AS SELECT
    org_id AS org_id,
    fingerprint AS fingerprint,
    unix_milli AS unix_milli,
    value AS value
FROM {{db}}.metric_landing
WHERE kind = 0;

CREATE MATERIALIZED VIEW {{db}}.metric_series_mv{{on_cluster}} TO {{db}}.metric_series
AS SELECT
    org_id AS org_id,
    toDate(fromUnixTimestamp64Milli(unix_milli), 'UTC') AS day,
    fingerprint AS fingerprint,
    metric_name AS metric_name,
    toUInt32(bitShiftLeft(toUInt32(1), toHour(fromUnixTimestamp64Milli(unix_milli), 'UTC'))) AS hours
FROM {{db}}.metric_landing
WHERE kind = 2;

CREATE MATERIALIZED VIEW {{db}}.resources_mv{{on_cluster}} TO {{db}}.resources
AS SELECT
    day AS day,
    resource_id AS resource_id,
    service AS service,
    attrs AS attrs,
    attrs_other AS attrs_other,
    dropped_attrs AS dropped_attrs,
    schema_url AS schema_url,
    entity_refs AS entity_refs
FROM {{db}}.trace_landing
WHERE row_kind = 1;

CREATE MATERIALIZED VIEW {{db}}.spans_mv{{on_cluster}} TO {{db}}.spans{{route_suffix}}
AS SELECT
    trace_id AS trace_id,
    span_id AS span_id,
    parent_span_id AS parent_span_id,
    start_ns AS start_ns,
    duration_ns AS duration_ns,
    service AS service,
    resource_id AS resource_id,
    name AS name,
    kind AS kind,
    status_code AS status_code,
    status_message AS status_message,
    trace_state AS trace_state,
    flags AS flags,
    scope_name AS scope_name,
    scope_version AS scope_version,
    scope_attrs AS scope_attrs,
    attrs AS attrs,
    attrs_other AS attrs_other,
    dropped_attrs AS dropped_attrs,
    events AS events,
    dropped_events AS dropped_events,
    links AS links,
    dropped_links AS dropped_links,
    scope_schema_url AS scope_schema_url,
    scope_dropped_attrs AS scope_dropped_attrs,
    scope_attrs_other AS scope_attrs_other,
    end_ns AS end_ns,
    service_type AS service_type
FROM {{db}}.trace_landing
WHERE row_kind = 0;

CREATE MATERIALIZED VIEW {{db}}.tag_names_mv{{on_cluster}} TO {{db}}.tag_names
AS SELECT
    tag_scope AS scope,
    tag_key AS key
FROM {{db}}.trace_landing
WHERE row_kind = 2;

CREATE MATERIALIZED VIEW {{db}}.tag_values_mv{{on_cluster}} TO {{db}}.tag_values
AS SELECT
    tag_scope AS scope,
    tag_key AS key,
    tag_value AS value,
    tag_type AS val_type
FROM {{db}}.trace_landing
WHERE row_kind = 3;

CREATE TABLE IF NOT EXISTS {{db}}.service_edges{{on_cluster}}
(
    minute DateTime('UTC') CODEC(Delta, ZSTD(1)),
    client_minute DateTime('UTC') CODEC(Delta, ZSTD(1)),
    client LowCardinality(String) CODEC(ZSTD(1)),
    server LowCardinality(String) CODEC(ZSTD(1)),
    conn_type LowCardinality(String) CODEC(ZSTD(1)),
    calls UInt64 CODEC(ZSTD(1)),
    failed UInt64 CODEC(ZSTD(1)),
    q AggregateFunction(quantilesTDigest(0.5, 0.95, 0.99), Int64) CODEC(ZSTD(1)),
    ver UInt64 CODEC(ZSTD(1))
)
--@single  ENGINE = ReplacingMergeTree(ver)
--@cluster ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{{db}}.service_edges', '{replica}', ver)
PARTITION BY toDate(minute)
ORDER BY (conn_type, minute, client_minute, client, server)
TTL toDateTime(least(toUInt32(minute) - if(conn_type = '', 240, 0) + ({{retention_days}} * 86400), 4294967295))
SETTINGS index_granularity = 1024, ttl_only_drop_parts = 1{{storage_policy}};

CREATE MATERIALIZED VIEW {{db}}.service_edges_mv{{on_cluster}} REFRESH EVERY 1 MINUTE APPEND TO {{db}}.service_edges
--@single AS WITH (SELECT arraySort(groupArray(minute)) FROM merge('{{db}}', '^service_edges$') FINAL WHERE conn_type = '') AS ms,
--@cluster AS WITH (SELECT groupArray((replica_path, log_pointer > log_max_index OR (log_pointer = 0 AND log_max_index = 0), zookeeper_path, log_pointer)) FROM system.replicas WHERE database = '{{db}}' AND table IN ('spans', 'service_edges')) AS logs,
--@cluster (SELECT groupArray((name, numChildren, value)) FROM system.zookeeper WHERE path IN (SELECT arrayJoin(arrayMap(x -> x.1, logs))) AND name IN ('queue', 'is_lost')) AS nodes,
--@cluster (SELECT groupArray((path, numChildren)) FROM system.zookeeper WHERE path IN (SELECT arrayJoin(arrayMap(x -> x.3, arrayFilter(x -> position(x.1, '.spans/') > 0 AND NOT x.2, logs)))) AND name = 'log') AS loglen,
--@cluster (SELECT groupArray((toUInt64OrZero(substring(name, 5)), ctime, extract(value, '^format version: \\d+\ncreate_time: [^\n]*\nsource replica: [^\n]*\nblock_id: [^\n]*\n(?:log_entry_id: [^\n]*\n)?([A-Za-z_]+)\n'), extract(value, '\nsource replica: ([^\n]*)\n'))) FROM (SELECT name, ctime, value FROM system.zookeeper WHERE path IN (SELECT arrayJoin(arrayMap(x -> x.3 || '/log', arrayFilter(x -> position(x.1, '.spans/') > 0 AND NOT x.2 AND length(loglen) = 1 AND loglen[1].2 <= 10000, logs)))) LIMIT 10001) SETTINGS max_execution_time = 10) AS logents,
--@cluster length(logs) = 2 AND arrayAll(x -> x.2 OR (position(x.1, '.spans/') > 0 AND length(loglen) = 1 AND loglen[1].2 <= 10000 AND length(logents) <= 10000), logs) AND arrayCount(n -> n.1 = 'is_lost' AND n.3 = '0', nodes) = 2 AND arrayCount(n -> n.1 = 'queue' AND n.2 <= 1000, nodes) = 2 AS listable,
--@cluster (SELECT groupArray((position(path, '.service_edges/') > 0, extract(value, '^format version: \\d+\ncreate_time: [^\n]*\nsource replica: [^\n]*\nblock_id: [^\n]*\n(?:log_entry_id: [^\n]*\n)?([A-Za-z_]+)\n'), parseDateTimeBestEffortOrZero(extract(value, '\ncreate_time: ([^\n]*)\n'), serverTimezone()), extract(value, '\n(?:get|attach)\n([^\n]+)'), extract(value, '\nsource replica: ([^\n]*)\n'))) FROM system.zookeeper WHERE path IN (SELECT arrayJoin(arrayMap(x -> x.1 || if(listable, '/queue', '/log_pointer'), logs))) SETTINGS max_execution_time = 10) AS entries,
--@cluster (SELECT groupArray((partition_id, min_block_number, max_block_number)) FROM system.parts WHERE database = '{{db}}' AND table = 'service_edges' AND active AND has(arrayMap(e -> extract(e.4, '^([^_]+)_[0-9]+_[0-9]+_[0-9]+(?:_[0-9]+)?$'), arrayFilter(e -> e.1, entries)), partition_id)) AS local_edges,
--@cluster listable AND arrayCount(e -> e.1, entries) <= 1000 AND arrayCount(e -> NOT e.1, entries) <= 1000 AND arrayAll(e -> NOT e.1 OR e.2 = 'merge' OR (e.2 IN ('get', 'attach') AND e.5 != '' AND arrayExists(p -> p.1 = extract(e.4, '^([^_]+)_[0-9]+_[0-9]+_[0-9]+(?:_[0-9]+)?$') AND p.2 <= toInt64OrZero(extract(e.4, '^[^_]+_([0-9]+)_')) AND toInt64OrZero(extract(e.4, '^[^_]+_[0-9]+_([0-9]+)_')) <= p.3, local_edges)), entries) AS edges_ok,
--@cluster (SELECT arraySort(groupArray(minute)) FROM merge('{{db}}', '^service_edges$') FINAL WHERE conn_type = '' AND edges_ok) AS ms,
     ms[arrayFirstIndex(i -> i = length(ms) OR ms[i + 1] - ms[i] != 300, arrayEnumerate(ms))] AS wm,
     toStartOfInterval(now('UTC') - INTERVAL 3 MINUTE, INTERVAL 5 MINUTE) AS ready,
     (SELECT toDateTime(min(toDate(parseDateTimeBestEffort(partition))), 'UTC') FROM system.parts WHERE database = '{{db}}' AND table = 'spans' AND active) AS oldest,
     if(wm = 0, if(oldest = 0, ready - INTERVAL 5 MINUTE, oldest), wm + INTERVAL 1 MINUTE) AS lo,
--@single  least(ready, lo + INTERVAL 5 MINUTE) AS hi,
--@cluster least(ready, lo + INTERVAL 5 MINUTE) AS hi0,
--@cluster (SELECT groupArray((partition_id, min_block_number, max_block_number)) FROM system.parts WHERE database = '{{db}}' AND table = 'spans' AND active AND has(arrayMap(e -> extract(e.4, '^([^_]+)_[0-9]+_[0-9]+_[0-9]+(?:_[0-9]+)?$'), arrayFilter(e -> NOT e.1, entries)), partition_id)) AS local_named,
--@cluster edges_ok AND arrayAll(x -> x.2 OR arrayAll(e -> e.1 < x.4 OR e.3 = 'merge' OR (e.3 = 'get' AND e.4 != '' AND e.2 >= hi0 + INTERVAL 3 MINUTE + INTERVAL 10 SECOND), logents), logs) AND arrayAll(e -> e.1 OR e.2 = 'merge' OR (e.2 IN ('get', 'attach') AND e.5 != '' AND e.4 != '' AND toUnixTimestamp(e.3) > 0 AND (arrayExists(p -> p.1 = extract(e.4, '^([^_]+)_[0-9]+_[0-9]+_[0-9]+(?:_[0-9]+)?$') AND p.2 <= toInt64OrZero(extract(e.4, '^[^_]+_([0-9]+)_')) AND toInt64OrZero(extract(e.4, '^[^_]+_[0-9]+_([0-9]+)_')) <= p.3, local_named) OR (e.2 = 'get' AND e.3 >= hi0 + INTERVAL 3 MINUTE + INTERVAL 10 SECOND))), entries) AS caught_up,
--@cluster if(caught_up, hi0, lo) AS hi,
     toInt64(toUnixTimestamp(lo)) * 1000000000 AS slo,
     toInt64(toUnixTimestamp(hi)) * 1000000000 AS shi,
     toUInt64(toUnixTimestamp64Milli(now64())) AS v
SELECT * FROM (
SELECT toDateTime(intDiv(s.6, 60000000000) * 60, 'UTC') AS minute, toDateTime(intDiv(c.4, 60000000000) * 60, 'UTC') AS client_minute,
       c.1 AS client, s.2 AS server, if(c.3 = 3, 'rpc', 'messaging') AS conn_type,
       count() AS calls, countIf(s.3 = 2 OR c.2 = 2) AS failed,
       quantilesTDigestState(0.5, 0.95, 0.99)(s.4) AS q, v AS ver
FROM (SELECT trace_id,
        if(kind IN (3, 4) OR attrs.`zipkin%2Eshared`.:String = 'true', span_id, parent_span_id) AS k,
        anyIf((service, status_code, kind, start_ns), kind IN (3, 4)) AS c,
        countIf(kind IN (3, 4)) AS nc,
        groupArrayIf((span_id, service, status_code, duration_ns, kind, start_ns), kind IN (2, 5)) AS ss
 FROM merge('{{db}}', '^spans$')
 WHERE start_ns >= slo - 60000000000 AND start_ns < shi + 60000000000
   AND intDiv(start_ns, 300000000000) BETWEEN intDiv(slo - 60000000000, 300000000000) AND intDiv(shi + 59999999999, 300000000000)
   AND ((kind IN (2, 5) AND start_ns >= slo AND start_ns < shi) OR kind IN (3, 4))
 GROUP BY trace_id, k
 HAVING nc > 0)
ARRAY JOIN arrayDistinct(ss) AS s
WHERE ((c.3 = 3 AND s.5 = 2) OR (c.3 = 4 AND s.5 = 5)) AND shi > slo
GROUP BY minute, client_minute, client, server, conn_type
UNION ALL
SELECT hi - INTERVAL 1 MINUTE, hi - INTERVAL 1 MINUTE, '', '', '', 0, 0, quantilesTDigestState(0.5, 0.95, 0.99)(toInt64(0)), v
FROM system.one
HAVING hi > lo)
ORDER BY conn_type = '', minute, client_minute, client, server, conn_type
SETTINGS max_threads = 4, max_memory_usage = 4294967296, max_rows_to_read = {{graph_job_max_rows}}, read_overflow_mode = 'throw', max_execution_time = 50,
         max_insert_threads = 1, max_insert_block_size = 100000000, min_insert_block_size_rows = 100000000, min_insert_block_size_bytes = 10000000000;

CREATE MATERIALIZED VIEW {{db}}.traces_mv{{on_cluster}} TO {{db}}.traces{{route_suffix}}
AS SELECT
    toDate(fromUnixTimestamp64Nano(s), 'UTC') AS day,
    trace_id,
    s AS start_ns,
    e AS end_ns,
    r AS root,
    sv AS services,
    ls AS last_start_ns,
    bk AS buckets
FROM
(
    SELECT
        trace_id,
        min(start_ns) AS s,
        max(toInt64(least(toUInt64(start_ns) + toUInt64(duration_ns), 9223372036854775807))) AS e,
        min((toUInt8(parent_span_id != toFixedString('', 8)), start_ns, span_id, if(service_type = 'string', toString(service), ''), toString(name))) AS r,
        groupUniqArrayIf(toString(service), service_type = 'string') AS sv,
        max(start_ns) AS ls,
        groupUniqArray(4096)(intDiv(start_ns, 300000000000)) AS bk
    FROM {{db}}.trace_landing
    WHERE row_kind = 0
    GROUP BY trace_id
);

CREATE TABLE IF NOT EXISTS {{db}}.tag_bucket_values{{on_cluster}}
(
    bucket Int64 CODEC(ZSTD(1)),
    service LowCardinality(String) CODEC(ZSTD(1)),
    scope LowCardinality(String) CODEC(ZSTD(1)),
    key String CODEC(ZSTD(1)),
    vals AggregateFunction(groupUniqArray(101), Tuple(String, LowCardinality(String))) CODEC(ZSTD(1))
)
--@single  ENGINE = AggregatingMergeTree
--@cluster ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/all/{{db}}.tag_bucket_values', '{replica}')
PARTITION BY toDate(fromUnixTimestamp64Nano(bucket * 300000000000), 'UTC')
ORDER BY (service, scope, key, bucket)
TTL toDateTime(least(intDiv(bucket * 300000000000, 1000000000) + ({{retention_days}} * 86400), 4294967295))
--@single  SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192, non_replicated_deduplication_window = {{trace_dedup_window}}{{storage_policy}};
--@cluster SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192, replicated_deduplication_window = {{trace_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.error_spans{{on_cluster}}
(
    trace_id FixedString(16) CODEC(ZSTD(1)),
    span_id FixedString(8) CODEC(ZSTD(1)),
    start_ns Int64 CODEC(Delta(8), ZSTD(1)),
    duration_ns Int64 CODEC(T64, ZSTD(1)),
    end_ns UInt64 CODEC(Delta(8), ZSTD(1)),
    service LowCardinality(String) CODEC(ZSTD(1)),
    service_type LowCardinality(String) CODEC(ZSTD(1)),
    resource_id UInt128 CODEC(ZSTD(1)),
    name LowCardinality(String) CODEC(ZSTD(1)),
    kind Int32 CODEC(ZSTD(1)),
    status_code Int32 CODEC(ZSTD(1)),
    status_message String CODEC(ZSTD(1))
)
--@single  ENGINE = ReplacingMergeTree
--@cluster ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{{db}}.error_spans', '{replica}')
PARTITION BY toDate(fromUnixTimestamp64Nano(start_ns), 'UTC')
ORDER BY (intDiv(start_ns, 300000000000), trace_id, start_ns, span_id, kind)
TTL toDateTime(least(intDiv(start_ns, 1000000000) + ({{retention_days}} * 86400), 4294967295))
--@single  SETTINGS ttl_only_drop_parts = 1, index_granularity = 2048, min_columns_to_activate_adaptive_write_buffer = 1, non_replicated_deduplication_window = {{trace_dedup_window}}{{storage_policy}};
--@cluster SETTINGS ttl_only_drop_parts = 1, index_granularity = 2048, min_columns_to_activate_adaptive_write_buffer = 1, replicated_deduplication_window = {{trace_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

--@cluster CREATE TABLE IF NOT EXISTS {{db}}.error_spans{{dist_suffix}}{{on_cluster}} AS {{db}}.error_spans
--@cluster ENGINE = Distributed('{{cluster}}', '{{db}}', 'error_spans', cityHash64(trace_id))
--@cluster SETTINGS fsync_after_insert = 1, fsync_directories = 1;

CREATE MATERIALIZED VIEW {{db}}.error_spans_mv{{on_cluster}} TO {{db}}.error_spans{{route_suffix}}
AS SELECT trace_id, span_id, start_ns, duration_ns, end_ns, service, service_type, resource_id, name, kind,
          status_code, status_message
FROM {{db}}.trace_landing
WHERE row_kind = 0 AND status_code = 2;

CREATE MATERIALIZED VIEW {{db}}.tag_bucket_values_mv{{on_cluster}} TO {{db}}.tag_bucket_values
AS SELECT intDiv(start_ns, 300000000000) AS bucket, service, 'span' AS scope, pv.1 AS key,
          groupUniqArrayState(101)(e) AS vals
FROM {{db}}.trace_landing
ARRAY JOIN arrayZip(mapKeys(JSONAllPathsWithTypes(attrs)), mapValues(JSONAllPathsWithTypes(attrs)),
                    JSONAllValues(attrs)) AS pv
ARRAY JOIN multiIf(
    pv.2 = 'String',  [(pv.3, 'string')],
    pv.2 = 'Int64',   [(pv.3, 'int')],
    pv.2 = 'Float64', [(pv.3, 'float')],
    pv.2 = 'Bool',    [(pv.3, 'bool')],
    pv.2 = 'Array(Nullable(String))',  arrayMap(x -> (assumeNotNull(x), 'string'), CAST(pv.3, 'Array(Nullable(String))')),
    pv.2 = 'Array(Nullable(Int64))',   arrayMap(x -> (toString(assumeNotNull(x)), 'int'), CAST(pv.3, 'Array(Nullable(Int64))')),
    pv.2 = 'Array(Nullable(Float64))', arrayMap(x -> (toString(assumeNotNull(x)), 'float'), CAST(pv.3, 'Array(Nullable(Float64))')),
    pv.2 = 'Array(Nullable(Bool))',    arrayMap(x -> (toString(assumeNotNull(x)), 'bool'), CAST(pv.3, 'Array(Nullable(Bool))')),
    CAST([], 'Array(Tuple(String, String))')) AS e
WHERE row_kind = 0 AND service_type = 'string'
GROUP BY bucket, service, scope, key;

CREATE TABLE IF NOT EXISTS {{db}}.service_bucket_resources{{on_cluster}}
(
    bucket Int64 CODEC(ZSTD(1)),
    service LowCardinality(String) CODEC(ZSTD(1)),
    resource_id UInt128 CODEC(ZSTD(1))
)
--@single  ENGINE = ReplacingMergeTree
--@cluster ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/all/{{db}}.service_bucket_resources', '{replica}')
PARTITION BY toDate(fromUnixTimestamp64Nano(bucket * 300000000000), 'UTC')
ORDER BY (service, bucket, resource_id)
TTL toDateTime(least(intDiv(bucket * 300000000000, 1000000000) + ({{retention_days}} * 86400), 4294967295))
--@single  SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192, non_replicated_deduplication_window = {{trace_dedup_window}}{{storage_policy}};
--@cluster SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192, replicated_deduplication_window = {{trace_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE MATERIALIZED VIEW {{db}}.service_bucket_resources_mv{{on_cluster}} TO {{db}}.service_bucket_resources
AS SELECT intDiv(start_ns, 300000000000) AS bucket, service, resource_id
FROM {{db}}.trace_landing
WHERE row_kind = 0 AND service_type = 'string'
GROUP BY bucket, service, resource_id;
