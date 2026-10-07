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
-- `/clickhouse/tables/{shard}/` and five take `/clickhouse/tables/all/`
-- (docs/architecture.md §3). `tests/schema_file.rs` holds the relation.
--
-- A STATEMENT ENDS AT A LINE ENDING IN A SEMICOLON, and no other character
-- in this file is one: the HTTP interface refuses a multi-statement body,
-- so the script splits on that and sends one statement per request.
--
-- THERE ARE NO MIGRATIONS. A schema change is an edit here and another run
-- of the script, which drops the database and builds it again.

CREATE DATABASE IF NOT EXISTS {{db}}{{on_cluster}};

CREATE TABLE IF NOT EXISTS {{db}}.log_landing{{on_cluster}}
(
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
    pattern_count UInt64 CODEC(T64, ZSTD(1))
)
--@single  ENGINE = MergeTree
--@cluster ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{{db}}.log_landing', '{replica}')
PARTITION BY toStartOfHour(fromUnixTimestamp64Milli(received_ms))
ORDER BY (kind, service, fingerprint, timestamp_ns)
TTL toDateTime(least(intDiv(received_ms, 1000) + ({{log_landing_retention_hours}} * 3600), 4294967295))
--@single  SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192, non_replicated_deduplication_window = {{log_dedup_window}}{{storage_policy}};
--@cluster SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192, replicated_deduplication_window = {{log_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.log_metrics_{{log_rollup_suffix}}{{on_cluster}}
(
    fingerprint UInt128,
    bucket_ns Int64,
    count SimpleAggregateFunction(sum, UInt64),
    bytes SimpleAggregateFunction(sum, UInt64)
)
--@single  ENGINE = AggregatingMergeTree
--@cluster ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/{{db}}.log_metrics_{{log_rollup_suffix}}', '{replica}')
PARTITION BY toDate(fromUnixTimestamp64Nano(bucket_ns))
ORDER BY (fingerprint, bucket_ns)
--@single  SETTINGS index_granularity = 8192, non_replicated_deduplication_window = {{log_dedup_window}}{{storage_policy}};
--@cluster SETTINGS index_granularity = 8192, replicated_deduplication_window = {{log_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.log_patterns{{on_cluster}}
(
    fingerprint UInt128,
    bucket_ns Int64,
    pattern String CODEC(ZSTD(1)),
    count SimpleAggregateFunction(sum, UInt64)
)
--@single  ENGINE = AggregatingMergeTree
--@cluster ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/{{db}}.log_patterns', '{replica}')
PARTITION BY toDate(fromUnixTimestamp64Nano(bucket_ns))
ORDER BY (fingerprint, bucket_ns, pattern)
TTL toDateTime(least(intDiv(bucket_ns, 1000000000) + ({{retention_days}} * 86400), 4294967295))
--@single  SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192, non_replicated_deduplication_window = {{log_dedup_window}}{{storage_policy}};
--@cluster SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192, replicated_deduplication_window = {{log_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.log_samples{{on_cluster}}
(
    service LowCardinality(String),
    fingerprint UInt128,
    timestamp_ns Int64 CODEC(DoubleDelta, ZSTD(1)),
    severity Int8 DEFAULT 0,
    body String CODEC(ZSTD(1)),
    structured_metadata String DEFAULT '',
    INDEX idx_body_tokens body TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1,
    INDEX idx_body_ngrams body TYPE ngrambf_v1(4, 32768, 3, 0) GRANULARITY 1,
    INDEX idx_severity severity TYPE minmax GRANULARITY 4
)
--@single  ENGINE = MergeTree
--@cluster ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{{db}}.log_samples', '{replica}')
PARTITION BY toDate(fromUnixTimestamp64Nano(timestamp_ns))
ORDER BY (service, fingerprint, timestamp_ns)
TTL toDateTime(least(intDiv(timestamp_ns, 1000000000) + ({{retention_days}} * 86400), 4294967295))
--@single  SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192, non_replicated_deduplication_window = {{log_dedup_window}}{{storage_policy}};
--@cluster SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192, replicated_deduplication_window = {{log_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.log_streams{{on_cluster}}
(
    month Date,
    fingerprint UInt128,
    service LowCardinality(String),
    labels String CODEC(ZSTD(5)),
    updated_ns Int64
)
--@single  ENGINE = ReplacingMergeTree(updated_ns)
--@cluster ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{{db}}.log_streams', '{replica}', updated_ns)
PARTITION BY month
ORDER BY fingerprint
--@single  SETTINGS index_granularity = 8192, non_replicated_deduplication_window = {{log_dedup_window}}{{storage_policy}};
--@cluster SETTINGS index_granularity = 8192, replicated_deduplication_window = {{log_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.log_streams_idx{{on_cluster}}
(
    month Date,
    key LowCardinality(String),
    val String,
    fingerprint UInt128
)
--@single  ENGINE = ReplacingMergeTree
--@cluster ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{{db}}.log_streams_idx', '{replica}')
PARTITION BY month
ORDER BY (key, val, fingerprint)
--@single  SETTINGS index_granularity = 8192, non_replicated_deduplication_window = {{log_dedup_window}}{{storage_policy}};
--@cluster SETTINGS index_granularity = 8192, replicated_deduplication_window = {{log_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.metric_hist_samples{{on_cluster}}
(
    fingerprint UInt128 CODEC(Delta(8), ZSTD(1)),
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
ORDER BY (fingerprint, unix_milli)
TTL toDateTime(least(intDiv(unix_milli, 1000) + ({{retention_days}} * 86400), 4294967295))
--@single  SETTINGS ttl_only_drop_parts = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 1, index_granularity = 8192, non_replicated_deduplication_window = {{metrics_dedup_window}}{{storage_policy}};
--@cluster SETTINGS ttl_only_drop_parts = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 1, index_granularity = 8192, replicated_deduplication_window = {{metrics_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.metric_labels{{on_cluster}}
(
    metric_name LowCardinality(String),
    fingerprint UInt128 CODEC(Delta(8), ZSTD(1)),
    labels String CODEC(ZSTD(5)),
    first_seen SimpleAggregateFunction(min, Int64) CODEC(ZSTD(1)),
    last_seen SimpleAggregateFunction(max, Int64) CODEC(ZSTD(1))
)
--@single  ENGINE = AggregatingMergeTree
--@cluster ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/{{db}}.metric_labels', '{replica}')
ORDER BY (metric_name, fingerprint)
--@single  SETTINGS index_granularity = 8192, non_replicated_deduplication_window = {{metrics_dedup_window}}{{storage_policy}};
--@cluster SETTINGS index_granularity = 8192, replicated_deduplication_window = {{metrics_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.metric_landing{{on_cluster}}
(
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
    hist_counter_reset_hint UInt8 CODEC(ZSTD(1))
)
--@single  ENGINE = MergeTree
--@cluster ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{{db}}.metric_landing', '{replica}')
PARTITION BY toStartOfHour(fromUnixTimestamp64Milli(received_ms))
ORDER BY (kind, metric_name, fingerprint, unix_milli)
TTL toDateTime(least(intDiv(received_ms, 1000) + ({{metrics_landing_retention_hours}} * 3600), 4294967295))
--@single  SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192, non_replicated_deduplication_window = {{metrics_dedup_window}}{{storage_policy}};
--@cluster SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192, replicated_deduplication_window = {{metrics_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.metric_metadata{{on_cluster}}
(
    metric_name LowCardinality(String),
    metric_type LowCardinality(String),
    help String,
    unit String,
    updated_ns Int64
)
--@single  ENGINE = ReplacingMergeTree(updated_ns)
--@cluster ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/all/{{db}}.metric_metadata', '{replica}', updated_ns)
ORDER BY metric_name
--@single  SETTINGS index_granularity = 8192, non_replicated_deduplication_window = {{metrics_dedup_window}}{{storage_policy}};
--@cluster SETTINGS index_granularity = 8192, replicated_deduplication_window = {{metrics_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.metric_samples{{on_cluster}}
(
    fingerprint UInt128 CODEC(Delta(8), ZSTD(1)),
    unix_milli Int64 CODEC(DoubleDelta, ZSTD(1)),
    value Float64 CODEC(Gorilla(8), ZSTD(1))
)
--@single  ENGINE = MergeTree
--@cluster ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{{db}}.metric_samples', '{replica}')
PARTITION BY toDate(fromUnixTimestamp64Milli(unix_milli))
ORDER BY (fingerprint, unix_milli)
TTL toDateTime(least(intDiv(unix_milli, 1000) + ({{retention_days}} * 86400), 4294967295))
--@single  SETTINGS ttl_only_drop_parts = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 1, index_granularity = 8192, non_replicated_deduplication_window = {{metrics_dedup_window}}{{storage_policy}};
--@cluster SETTINGS ttl_only_drop_parts = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 1, index_granularity = 8192, replicated_deduplication_window = {{metrics_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.metric_series{{on_cluster}}
(
    day Date,
    fingerprint UInt128 CODEC(ZSTD(1)),
    metric_name LowCardinality(String),
    hours SimpleAggregateFunction(groupBitOr, UInt32)
)
--@single  ENGINE = AggregatingMergeTree
--@cluster ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/{{db}}.metric_series', '{replica}')
PARTITION BY day
ORDER BY fingerprint
TTL toDateTime(least((toUInt64(toUInt16(day)) + 1 + {{retention_days}}) * 86400, 4294967295))
--@single  SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192, non_replicated_deduplication_window = {{metrics_dedup_window}}{{storage_policy}};
--@cluster SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192, replicated_deduplication_window = {{metrics_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

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
    service_type LowCardinality(String) CODEC(ZSTD(1))
)
--@single  ENGINE = ReplacingMergeTree
--@cluster ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{{db}}.spans', '{replica}')
PARTITION BY toDate(fromUnixTimestamp64Nano(start_ns), 'UTC')
ORDER BY (intDiv(start_ns, 300000000000), trace_id, start_ns, span_id, kind)
TTL toDateTime(least(intDiv(start_ns, 1000000000) + ({{retention_days}} * 86400), 4294967295))
--@single  SETTINGS ttl_only_drop_parts = 1, index_granularity = 2048, non_replicated_deduplication_window = {{trace_dedup_window}}{{storage_policy}};
--@cluster SETTINGS ttl_only_drop_parts = 1, index_granularity = 2048, replicated_deduplication_window = {{trace_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}};

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

CREATE TABLE IF NOT EXISTS {{db}}.trace_attrs_idx{{on_cluster}}
(
    date Date,
    key LowCardinality(String),
    val String,
    scope LowCardinality(String),
    val_num Nullable(Float64),
    timestamp_ns Int64,
    trace_id FixedString(16),
    span_id FixedString(8),
    duration_ns Int64,
    val_type LowCardinality(String) DEFAULT ''
)
--@single  ENGINE = ReplacingMergeTree
--@cluster ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{{db}}.trace_attrs_idx', '{replica}')
PARTITION BY date
ORDER BY (key, val, scope, timestamp_ns, trace_id, span_id)
TTL toDateTime(least(intDiv(timestamp_ns, 1000000000) + ({{retention_days}} * 86400), 4294967295))
SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.trace_edges{{on_cluster}}
(
    date Date,
    side UInt8,
    trace_id FixedString(16),
    span_id FixedString(8),
    pair_id FixedString(8),
    conn_type LowCardinality(String),
    timestamp_ns Int64 CODEC(DoubleDelta, ZSTD(1)),
    service LowCardinality(String),
    duration_ns Int64 CODEC(T64, ZSTD(1)),
    failed UInt8
)
--@single  ENGINE = ReplacingMergeTree
--@cluster ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{{db}}.trace_edges', '{replica}')
PARTITION BY date
ORDER BY (side, trace_id, span_id)
TTL toDateTime(least(intDiv(timestamp_ns, 1000000000) + ({{retention_days}} * 86400), 4294967295))
SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.trace_error_spans{{on_cluster}}
(
    date Date,
    trace_id FixedString(16),
    span_id FixedString(8),
    timestamp_ns Int64 CODEC(DoubleDelta, ZSTD(1)),
    duration_ns Int64 CODEC(T64, ZSTD(1)),
    service LowCardinality(String),
    name LowCardinality(String),
    kind Int8
)
--@single  ENGINE = ReplacingMergeTree
--@cluster ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{{db}}.trace_error_spans', '{replica}')
PARTITION BY date
ORDER BY (timestamp_ns, trace_id, span_id)
TTL toDateTime(least(intDiv(timestamp_ns, 1000000000) + ({{retention_days}} * 86400), 4294967295))
--@single  SETTINGS ttl_only_drop_parts = 1, non_replicated_deduplication_window = {{trace_dedup_window}}, index_granularity = 8192{{storage_policy}};
--@cluster SETTINGS ttl_only_drop_parts = 1, replicated_deduplication_window = {{trace_dedup_window}}, index_granularity = 8192{{storage_policy}};

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

CREATE TABLE IF NOT EXISTS {{db}}.trace_recent{{on_cluster}}
(
    date Date,
    bucket UInt32,
    trace_id FixedString(16),
    ts_max SimpleAggregateFunction(max, Int64) CODEC(T64, ZSTD(1)),
    ts_min SimpleAggregateFunction(min, Int64) CODEC(T64, ZSTD(1))
)
--@single  ENGINE = AggregatingMergeTree
--@cluster ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/{{db}}.trace_recent', '{replica}')
PARTITION BY date
ORDER BY (bucket, trace_id)
TTL toDateTime(least(intDiv(ts_max, 1000000000) + ({{retention_days}} * 86400), 4294967295))
--@single  SETTINGS ttl_only_drop_parts = 1, non_replicated_deduplication_window = {{trace_dedup_window}}, index_granularity = 8192{{storage_policy}};
--@cluster SETTINGS ttl_only_drop_parts = 1, replicated_deduplication_window = {{trace_dedup_window}}, index_granularity = 8192{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.trace_spans{{on_cluster}}
(
    trace_id FixedString(16),
    span_id FixedString(8),
    parent_id FixedString(8),
    name LowCardinality(String),
    service LowCardinality(String),
    timestamp_ns Int64 CODEC(DoubleDelta, ZSTD(1)),
    duration_ns Int64 CODEC(T64, ZSTD(1)),
    status_code Int8,
    kind Int8,
    payload_type Int8,
    payload String CODEC(ZSTD(3)),
    shared UInt8 DEFAULT 0,
    status_message String DEFAULT '',
    scope_name LowCardinality(String) DEFAULT '',
    scope_version LowCardinality(String) DEFAULT '',
    attr_key Array(LowCardinality(String)),
    attr_scope Array(LowCardinality(String)),
    attr_val Array(String),
    attr_type Array(LowCardinality(String)),
    attr_num Array(Nullable(Float64)),
    INDEX idx_duration duration_ns TYPE minmax GRANULARITY 4,
    CONSTRAINT attr_arrays_aligned CHECK (length(attr_key) = length(attr_scope)) AND (length(attr_key) = length(attr_val)) AND (length(attr_key) = length(attr_type)) AND (length(attr_key) = length(attr_num)),
    PROJECTION span_name_day
    (
        SELECT
            toDate(fromUnixTimestamp64Nano(timestamp_ns)) AS d,
            name,
            count()
        GROUP BY
            d,
            name
    ),
    PROJECTION service_time
    (
        SELECT
            duration_ns,
            kind,
            name,
            parent_id,
            payload_type,
            scope_name,
            scope_version,
            service,
            shared,
            span_id,
            status_code,
            status_message,
            timestamp_ns,
            trace_id
        ORDER BY
            service,
            timestamp_ns
    ),
    PROJECTION name_time
    (
        SELECT
            duration_ns,
            kind,
            name,
            parent_id,
            payload_type,
            scope_name,
            scope_version,
            service,
            shared,
            span_id,
            status_code,
            status_message,
            timestamp_ns,
            trace_id
        ORDER BY
            name,
            timestamp_ns
    )
)
--@single  ENGINE = MergeTree
--@cluster ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{{db}}.trace_spans', '{replica}')
PARTITION BY toDate(fromUnixTimestamp64Nano(timestamp_ns))
ORDER BY (trace_id, timestamp_ns)
TTL toDateTime(least(intDiv(timestamp_ns, 1000000000) + ({{retention_days}} * 86400), 4294967295))
SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192{{storage_policy}};

CREATE TABLE IF NOT EXISTS {{db}}.trace_tag_catalog{{on_cluster}}
(
    scope LowCardinality(String),
    key LowCardinality(String),
    val String,
    val_type LowCardinality(String)
)
--@single  ENGINE = ReplacingMergeTree
--@cluster ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/all/{{db}}.trace_tag_catalog', '{replica}')
PRIMARY KEY (scope, key, val)
ORDER BY (scope, key, val, val_type)
SETTINGS index_granularity = 8192{{storage_policy}};

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

--@cluster CREATE TABLE IF NOT EXISTS {{db}}.metric_hist_samples{{dist_suffix}}{{on_cluster}} AS {{db}}.metric_hist_samples
--@cluster ENGINE = Distributed('{{cluster}}', '{{db}}', 'metric_hist_samples', cityHash64(fingerprint));

--@cluster CREATE TABLE IF NOT EXISTS {{db}}.metric_labels{{dist_suffix}}{{on_cluster}} AS {{db}}.metric_labels
--@cluster ENGINE = Distributed('{{cluster}}', '{{db}}', 'metric_labels', cityHash64(fingerprint));

--@cluster CREATE TABLE IF NOT EXISTS {{db}}.metric_samples{{dist_suffix}}{{on_cluster}} AS {{db}}.metric_samples
--@cluster ENGINE = Distributed('{{cluster}}', '{{db}}', 'metric_samples', cityHash64(fingerprint));

--@cluster CREATE TABLE IF NOT EXISTS {{db}}.metric_series{{dist_suffix}}{{on_cluster}} AS {{db}}.metric_series
--@cluster ENGINE = Distributed('{{cluster}}', '{{db}}', 'metric_series', cityHash64(fingerprint));

--@cluster CREATE TABLE IF NOT EXISTS {{db}}.spans{{dist_suffix}}{{on_cluster}} AS {{db}}.spans
--@cluster ENGINE = Distributed('{{cluster}}', '{{db}}', 'spans', cityHash64(trace_id))
--@cluster SETTINGS fsync_after_insert = 1, fsync_directories = 1;

--@cluster CREATE TABLE IF NOT EXISTS {{db}}.trace_attrs_idx{{dist_suffix}}{{on_cluster}} AS {{db}}.trace_attrs_idx
--@cluster ENGINE = Distributed('{{cluster}}', '{{db}}', 'trace_attrs_idx', cityHash64(trace_id));

--@cluster CREATE TABLE IF NOT EXISTS {{db}}.trace_edges{{dist_suffix}}{{on_cluster}} AS {{db}}.trace_edges
--@cluster ENGINE = Distributed('{{cluster}}', '{{db}}', 'trace_edges', cityHash64(trace_id));

--@cluster CREATE TABLE IF NOT EXISTS {{db}}.trace_error_spans{{dist_suffix}}{{on_cluster}} AS {{db}}.trace_error_spans
--@cluster ENGINE = Distributed('{{cluster}}', '{{db}}', 'trace_error_spans', cityHash64(trace_id));

--@cluster CREATE TABLE IF NOT EXISTS {{db}}.trace_recent{{dist_suffix}}{{on_cluster}} AS {{db}}.trace_recent
--@cluster ENGINE = Distributed('{{cluster}}', '{{db}}', 'trace_recent', cityHash64(trace_id));

--@cluster CREATE TABLE IF NOT EXISTS {{db}}.trace_spans{{dist_suffix}}{{on_cluster}} AS {{db}}.trace_spans
--@cluster ENGINE = Distributed('{{cluster}}', '{{db}}', 'trace_spans', cityHash64(trace_id));

--@cluster CREATE TABLE IF NOT EXISTS {{db}}.traces{{dist_suffix}}{{on_cluster}} AS {{db}}.traces
--@cluster ENGINE = Distributed('{{cluster}}', '{{db}}', 'traces', cityHash64(trace_id))
--@cluster SETTINGS fsync_after_insert = 1, fsync_directories = 1;

DROP VIEW IF EXISTS {{db}}.log_metrics_{{log_rollup_suffix}}_mv{{on_cluster}};
CREATE MATERIALIZED VIEW {{db}}.log_metrics_{{log_rollup_suffix}}_mv{{on_cluster}} TO {{db}}.log_metrics_{{log_rollup_suffix}}
AS SELECT
    fingerprint AS fingerprint,
    intDiv(timestamp_ns, {{log_rollup_ns}}) * {{log_rollup_ns}} AS bucket_ns,
    count() AS count,
    sum(length(body)) AS bytes
FROM {{db}}.log_landing
WHERE kind = 0
GROUP BY
    fingerprint,
    bucket_ns;

DROP VIEW IF EXISTS {{db}}.log_patterns_mv{{on_cluster}};
CREATE MATERIALIZED VIEW {{db}}.log_patterns_mv{{on_cluster}} TO {{db}}.log_patterns
AS SELECT
    fingerprint AS fingerprint,
    timestamp_ns AS bucket_ns,
    pattern AS pattern,
    pattern_count AS count
FROM {{db}}.log_landing
WHERE kind = 2;

DROP VIEW IF EXISTS {{db}}.log_samples_mv{{on_cluster}};
CREATE MATERIALIZED VIEW {{db}}.log_samples_mv{{on_cluster}} TO {{db}}.log_samples
AS SELECT
    service AS service,
    fingerprint AS fingerprint,
    timestamp_ns AS timestamp_ns,
    severity AS severity,
    body AS body,
    structured_metadata AS structured_metadata
FROM {{db}}.log_landing
WHERE kind = 0;

DROP VIEW IF EXISTS {{db}}.log_streams_idx_mv{{on_cluster}};
CREATE MATERIALIZED VIEW {{db}}.log_streams_idx_mv{{on_cluster}} TO {{db}}.log_streams_idx
AS SELECT
    month,
    kv.1 AS key,
    kv.2 AS val,
    fingerprint
FROM {{db}}.log_landing
ARRAY JOIN JSONExtractKeysAndValues(labels, 'String') AS kv
WHERE kind = 1;

DROP VIEW IF EXISTS {{db}}.log_streams_mv{{on_cluster}};
CREATE MATERIALIZED VIEW {{db}}.log_streams_mv{{on_cluster}} TO {{db}}.log_streams
AS SELECT
    month AS month,
    fingerprint AS fingerprint,
    service AS service,
    labels AS labels,
    updated_ns AS updated_ns
FROM {{db}}.log_landing
WHERE kind = 1;

DROP VIEW IF EXISTS {{db}}.metric_hist_samples_mv{{on_cluster}};
CREATE MATERIALIZED VIEW {{db}}.metric_hist_samples_mv{{on_cluster}} TO {{db}}.metric_hist_samples
AS SELECT
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

DROP VIEW IF EXISTS {{db}}.metric_labels_mv{{on_cluster}};
CREATE MATERIALIZED VIEW {{db}}.metric_labels_mv{{on_cluster}} TO {{db}}.metric_labels
AS SELECT
    metric_name AS metric_name,
    fingerprint AS fingerprint,
    labels AS labels,
    unix_milli AS first_seen,
    unix_milli AS last_seen
FROM {{db}}.metric_landing
WHERE kind = 2;

DROP VIEW IF EXISTS {{db}}.metric_metadata_mv{{on_cluster}};
CREATE MATERIALIZED VIEW {{db}}.metric_metadata_mv{{on_cluster}} TO {{db}}.metric_metadata
AS SELECT
    metric_name AS metric_name,
    metric_type AS metric_type,
    help AS help,
    unit AS unit,
    updated_ns AS updated_ns
FROM {{db}}.metric_landing
WHERE kind = 3;

DROP VIEW IF EXISTS {{db}}.metric_samples_mv{{on_cluster}};
CREATE MATERIALIZED VIEW {{db}}.metric_samples_mv{{on_cluster}} TO {{db}}.metric_samples
AS SELECT
    fingerprint AS fingerprint,
    unix_milli AS unix_milli,
    value AS value
FROM {{db}}.metric_landing
WHERE kind = 0;

DROP VIEW IF EXISTS {{db}}.metric_series_mv{{on_cluster}};
CREATE MATERIALIZED VIEW {{db}}.metric_series_mv{{on_cluster}} TO {{db}}.metric_series
AS SELECT
    toDate(fromUnixTimestamp64Milli(unix_milli), 'UTC') AS day,
    fingerprint AS fingerprint,
    metric_name AS metric_name,
    toUInt32(bitShiftLeft(toUInt32(1), toHour(fromUnixTimestamp64Milli(unix_milli), 'UTC'))) AS hours
FROM {{db}}.metric_landing
WHERE kind = 2;

DROP VIEW IF EXISTS {{db}}.resources_mv{{on_cluster}};
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

DROP VIEW IF EXISTS {{db}}.spans_mv{{on_cluster}};
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

DROP VIEW IF EXISTS {{db}}.tag_names_mv{{on_cluster}};
CREATE MATERIALIZED VIEW {{db}}.tag_names_mv{{on_cluster}} TO {{db}}.tag_names
AS SELECT
    tag_scope AS scope,
    tag_key AS key
FROM {{db}}.trace_landing
WHERE row_kind = 2;

DROP VIEW IF EXISTS {{db}}.tag_values_mv{{on_cluster}};
CREATE MATERIALIZED VIEW {{db}}.tag_values_mv{{on_cluster}} TO {{db}}.tag_values
AS SELECT
    tag_scope AS scope,
    tag_key AS key,
    tag_value AS value,
    tag_type AS val_type
FROM {{db}}.trace_landing
WHERE row_kind = 3;

DROP VIEW IF EXISTS {{db}}.trace_edges_mv{{on_cluster}};
CREATE MATERIALIZED VIEW {{db}}.trace_edges_mv{{on_cluster}} TO {{db}}.trace_edges
AS SELECT
    toDate(fromUnixTimestamp64Nano(timestamp_ns)) AS date,
    toUInt8(kind IN (2, 5)) AS side,
    trace_id,
    span_id,
    if((kind IN (3, 4)) OR (shared = 1), span_id, parent_id) AS pair_id,
    if((kind IN (2, 3)), 'rpc', 'messaging') AS conn_type,
    timestamp_ns,
    service,
    duration_ns,
    toUInt8(status_code = 2) AS failed
FROM {{db}}.trace_spans
WHERE (kind IN (3, 4)) OR ((kind IN (2, 5)) AND ((shared = 1) OR (parent_id != toFixedString(unhex('0000000000000000'), 8))));

DROP VIEW IF EXISTS {{db}}.trace_error_spans_mv{{on_cluster}};
CREATE MATERIALIZED VIEW {{db}}.trace_error_spans_mv{{on_cluster}} TO {{db}}.trace_error_spans
AS SELECT
    toDate(fromUnixTimestamp64Nano(timestamp_ns)) AS date,
    trace_id,
    span_id,
    timestamp_ns,
    duration_ns,
    service,
    name,
    kind
FROM {{db}}.trace_spans
WHERE status_code = 2;

DROP VIEW IF EXISTS {{db}}.trace_recent_mv{{on_cluster}};
CREATE MATERIALIZED VIEW {{db}}.trace_recent_mv{{on_cluster}} TO {{db}}.trace_recent
AS SELECT
    toDate(fromUnixTimestamp64Nano(timestamp_ns)) AS date,
    toUInt32(intDiv(timestamp_ns, 300000000000)) AS bucket,
    trace_id,
    max(timestamp_ns) AS ts_max,
    min(timestamp_ns) AS ts_min
FROM {{db}}.trace_spans
GROUP BY
    date,
    bucket,
    trace_id;

DROP VIEW IF EXISTS {{db}}.trace_tag_catalog_mv{{on_cluster}};
CREATE MATERIALIZED VIEW {{db}}.trace_tag_catalog_mv{{on_cluster}} TO {{db}}.trace_tag_catalog
AS SELECT
    scope,
    key,
    val,
    val_type
FROM {{db}}.trace_attrs_idx;

DROP VIEW IF EXISTS {{db}}.traces_mv{{on_cluster}};
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
        min((toUInt8(parent_span_id != toFixedString('', 8)), start_ns, span_id, toString(service), toString(name))) AS r,
        groupUniqArray(toString(service)) AS sv,
        max(start_ns) AS ls,
        groupUniqArray(4096)(intDiv(start_ns, 300000000000)) AS bk
    FROM {{db}}.trace_landing
    WHERE row_kind = 0
    GROUP BY trace_id
);
