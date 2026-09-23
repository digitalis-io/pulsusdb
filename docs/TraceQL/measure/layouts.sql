-- The layouts that were measured before the schema of schema.sql was chosen,
-- and the alternatives docs/TraceQL/sql-schema.md 5.4 reports. Each is built
-- from the same staging table, so only the layout differs. Run with
-- json_type_escape_dots_in_keys = 1 after schema.sql has created tqd_g1.spans.

-- A. the shipped layout's sort key with the service first (the alternative that
--    prunes a service-scoped read but costs a trace fetch more granules)
CREATE TABLE tqd_g1.l_service_first AS tqd_g1.spans
ENGINE = ReplacingMergeTree
PARTITION BY toDate(fromUnixTimestamp64Nano(start_ns))
ORDER BY (intDiv(start_ns, 300000000000), service, trace_id, start_ns, span_id, kind)
SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192;
INSERT INTO tqd_g1.l_service_first SELECT * FROM tqd_g1.spans;

-- B. the shipped sort key at the default granule, to price the granule alone
CREATE TABLE tqd_g1.l_trace_first_8192 AS tqd_g1.spans
ENGINE = ReplacingMergeTree
PARTITION BY toDate(fromUnixTimestamp64Nano(start_ns))
ORDER BY (intDiv(start_ns, 300000000000), trace_id, start_ns, span_id, kind)
SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192;
INSERT INTO tqd_g1.l_trace_first_8192 SELECT * FROM tqd_g1.spans;

-- C. the resource attributes inline on every span, instead of a resource id
CREATE TABLE tqd_g1.l_resource_inline (
    trace_id FixedString(16) CODEC(ZSTD(1)), span_id FixedString(8) CODEC(ZSTD(1)),
    parent_span_id FixedString(8) CODEC(ZSTD(1)), start_ns Int64 CODEC(Delta, ZSTD(1)),
    duration_ns Int64 CODEC(T64, ZSTD(1)), service LowCardinality(String) CODEC(ZSTD(1)),
    resource JSON CODEC(ZSTD(1)), name LowCardinality(String) CODEC(ZSTD(1)),
    kind UInt8 CODEC(ZSTD(1)), status_code UInt8 CODEC(ZSTD(1)), attrs JSON CODEC(ZSTD(1))
) ENGINE = ReplacingMergeTree
PARTITION BY toDate(fromUnixTimestamp64Nano(start_ns))
ORDER BY (intDiv(start_ns, 300000000000), trace_id, start_ns, span_id, kind)
SETTINGS ttl_only_drop_parts = 1, index_granularity = 2048;
INSERT INTO tqd_g1.l_resource_inline
SELECT unhex(trace_id), unhex(span_id), if(parent_span_id = '', toFixedString('', 8), unhex(parent_span_id)),
       start_ns, end_ns - start_ns, service, CAST(tqd_kv2json(resource) AS JSON), name, kind, status_code,
       CAST(tqd_kv2json(attrs) AS JSON)
FROM tqd_g1.raw;

-- D. the shipped layout under LZ4 instead of ZSTD, which prices the insert hop:
--    the client compresses a RowBinary block with LZ4 by default
CREATE TABLE tqd_g1.l_lz4 AS tqd_g1.spans
ENGINE = ReplacingMergeTree
PARTITION BY toDate(fromUnixTimestamp64Nano(start_ns))
ORDER BY (intDiv(start_ns, 300000000000), trace_id, start_ns, span_id, kind)
SETTINGS ttl_only_drop_parts = 1, index_granularity = 2048;
ALTER TABLE tqd_g1.l_lz4 MODIFY COLUMN trace_id FixedString(16) CODEC(LZ4), MODIFY COLUMN span_id FixedString(8) CODEC(LZ4),
    MODIFY COLUMN parent_span_id FixedString(8) CODEC(LZ4), MODIFY COLUMN start_ns Int64 CODEC(LZ4),
    MODIFY COLUMN duration_ns Int64 CODEC(LZ4), MODIFY COLUMN service LowCardinality(String) CODEC(LZ4),
    MODIFY COLUMN resource_id UInt128 CODEC(LZ4), MODIFY COLUMN name LowCardinality(String) CODEC(LZ4),
    MODIFY COLUMN kind UInt8 CODEC(LZ4), MODIFY COLUMN status_code UInt8 CODEC(LZ4),
    MODIFY COLUMN attrs JSON CODEC(LZ4), MODIFY COLUMN events Array(Tuple(time_ns Int64, name LowCardinality(String), attrs JSON, dropped_attrs UInt32)) CODEC(LZ4),
    MODIFY COLUMN links Array(Tuple(trace_id FixedString(16), span_id FixedString(8), trace_state String, flags UInt32, attrs JSON, dropped_attrs UInt32)) CODEC(LZ4);
INSERT INTO tqd_g1.l_lz4 SELECT * FROM tqd_g1.spans;

-- E and F. the shipped sort key at two smaller granules, which is the knee the
--    document reports for the trace fetch
CREATE TABLE tqd_g1.l_trace_first_1024 AS tqd_g1.spans
ENGINE = ReplacingMergeTree
PARTITION BY toDate(fromUnixTimestamp64Nano(start_ns))
ORDER BY (intDiv(start_ns, 300000000000), trace_id, start_ns, span_id, kind)
SETTINGS ttl_only_drop_parts = 1, index_granularity = 1024;
INSERT INTO tqd_g1.l_trace_first_1024 SELECT * FROM tqd_g1.spans;

CREATE TABLE tqd_g1.l_trace_first_512 AS tqd_g1.spans
ENGINE = ReplacingMergeTree
PARTITION BY toDate(fromUnixTimestamp64Nano(start_ns))
ORDER BY (intDiv(start_ns, 300000000000), trace_id, start_ns, span_id, kind)
SETTINGS ttl_only_drop_parts = 1, index_granularity = 512;
INSERT INTO tqd_g1.l_trace_first_512 SELECT * FROM tqd_g1.spans;

OPTIMIZE TABLE tqd_g1.l_trace_first_1024 FINAL;
OPTIMIZE TABLE tqd_g1.l_trace_first_512 FINAL;
OPTIMIZE TABLE tqd_g1.l_service_first FINAL;
OPTIMIZE TABLE tqd_g1.l_trace_first_8192 FINAL;
OPTIMIZE TABLE tqd_g1.l_resource_inline FINAL;
OPTIMIZE TABLE tqd_g1.l_lz4 FINAL;
