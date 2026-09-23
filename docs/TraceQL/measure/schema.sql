-- The proposed trace schema (docs/TraceQL/sql-schema.md), single-node form, in
-- database tqd_g1, built from the staging table of staging.sql. Run with
-- json_type_escape_dots_in_keys = 1. The replicated form differs only in the
-- engine names (Replicated*) and the object-storage settings of the document.

-- An attribute list [key, type, value] -> a JSON object with native JSON types:
-- string -> "..", int -> 123, double -> 1.5 (a whole double keeps ".0"),
-- bool -> true, array -> [..]. `skip` drops one key (the service name, which the
-- span row already carries as a column).
CREATE FUNCTION IF NOT EXISTS tqd_kv2json AS (a) -> concat('{', arrayStringConcat(arrayMap(x -> concat(toJSONString(x.1), ':', if(x.2 = 'd' AND NOT match(x.3, '[.eE]'), x.3 || '.0', x.3)), arrayMap(e -> (JSONExtractString(e, 1), JSONExtractString(e, 2), JSONExtractRaw(e, 3)), JSONExtractArrayRaw(a))), ','), '}');
CREATE FUNCTION IF NOT EXISTS tqd_kv2json_skip AS (a, skip) -> concat('{', arrayStringConcat(arrayMap(x -> concat(toJSONString(x.1), ':', if(x.2 = 'd' AND NOT match(x.3, '[.eE]'), x.3 || '.0', x.3)), arrayFilter(x -> x.1 != skip, arrayMap(e -> (JSONExtractString(e, 1), JSONExtractString(e, 2), JSONExtractRaw(e, 3)), JSONExtractArrayRaw(a)))), ','), '}');
-- a stored JSON path back to the OTLP key it came from
CREATE FUNCTION IF NOT EXISTS tqd_unescape AS (p) -> replaceAll(replaceAll(p, '%2E', '.'), '%25', '%');

CREATE TABLE tqd_g1.spans (
    trace_id        FixedString(16)          CODEC(ZSTD(1)),
    span_id         FixedString(8)           CODEC(ZSTD(1)),
    parent_span_id  FixedString(8)           CODEC(ZSTD(1)),
    start_ns        Int64                    CODEC(Delta, ZSTD(1)),
    duration_ns     Int64                    CODEC(T64, ZSTD(1)),
    service         LowCardinality(String)   CODEC(ZSTD(1)),
    resource_id     UInt128                  CODEC(ZSTD(1)),
    name            LowCardinality(String)   CODEC(ZSTD(1)),
    kind            UInt8                    CODEC(ZSTD(1)),
    status_code     UInt8                    CODEC(ZSTD(1)),
    status_message  String                   CODEC(ZSTD(1)),
    trace_state     String                   CODEC(ZSTD(1)),
    flags           UInt32                   CODEC(ZSTD(1)),
    scope_name      LowCardinality(String)   CODEC(ZSTD(1)),
    scope_version   LowCardinality(String)   CODEC(ZSTD(1)),
    scope_attrs     JSON                     CODEC(ZSTD(1)),
    attrs           JSON                     CODEC(ZSTD(1)),
    attrs_other     String                   CODEC(ZSTD(1)),
    dropped_attrs   UInt32                   CODEC(ZSTD(1)),
    events          Array(Tuple(time_ns Int64, name LowCardinality(String), attrs JSON, dropped_attrs UInt32)) CODEC(ZSTD(1)),
    dropped_events  UInt32                   CODEC(ZSTD(1)),
    links           Array(Tuple(trace_id FixedString(16), span_id FixedString(8), trace_state String, flags UInt32, attrs JSON, dropped_attrs UInt32)) CODEC(ZSTD(1)),
    dropped_links   UInt32                   CODEC(ZSTD(1))
) ENGINE = ReplacingMergeTree
PARTITION BY toDate(fromUnixTimestamp64Nano(start_ns))
ORDER BY (intDiv(start_ns, 300000000000), trace_id, start_ns, span_id, kind)
SETTINGS ttl_only_drop_parts = 1, index_granularity = 2048;

-- one row per distinct resource per day; `service` is the service name, which is
-- NOT repeated inside attrs (the span row carries it as the sort key's column)
CREATE TABLE tqd_g1.resources (
    day            Date,
    resource_id    UInt128,
    service        LowCardinality(String),
    attrs          JSON,
    attrs_other    String,
    dropped_attrs  UInt32,
    schema_url     String
) ENGINE = ReplacingMergeTree
PARTITION BY day
ORDER BY (service, resource_id);

CREATE TABLE tqd_g1.traces (
    day           Date,
    trace_id      FixedString(16)                                      CODEC(ZSTD(1)),
    start_ns      SimpleAggregateFunction(min, Int64)                  CODEC(ZSTD(1)),
    end_ns        SimpleAggregateFunction(max, Int64)                  CODEC(ZSTD(1)),
    root_service  SimpleAggregateFunction(max, LowCardinality(String)) CODEC(ZSTD(1)),
    root_name     SimpleAggregateFunction(max, LowCardinality(String)) CODEC(ZSTD(1)),
    services      SimpleAggregateFunction(groupUniqArrayArray, Array(String)) CODEC(ZSTD(1))
) ENGINE = AggregatingMergeTree
PARTITION BY day
ORDER BY trace_id
SETTINGS index_granularity = 1024, ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW tqd_g1.traces_mv TO tqd_g1.traces AS
SELECT toDate(fromUnixTimestamp64Nano(s)) AS day, trace_id, s AS start_ns, e AS end_ns,
       rs AS root_service, rn AS root_name, sv AS services
FROM (SELECT trace_id, min(start_ns) AS s, max(start_ns + duration_ns) AS e,
             maxIf(service, parent_span_id = toFixedString('', 8)) AS rs,
             maxIf(name, parent_span_id = toFixedString('', 8)) AS rn,
             groupUniqArray(toString(service)) AS sv
      FROM tqd_g1.spans
      GROUP BY trace_id);

-- The tag catalogs. Time-less and without a TTL, because docs/api.md 4.3 makes
-- name discovery time-less and lets entries outlive span retention, and makes an
-- unnarrowed value lookup a catalog read. They are written by the writer from
-- its own parse, behind a per-process cache, the way metric metadata is; they
-- hold one row per DISTINCT (scope, key[, value, type]), never one per span.
CREATE TABLE tqd_g1.tag_names (
    scope  LowCardinality(String),   -- span | resource | event | link | instrumentation
    key    String
) ENGINE = ReplacingMergeTree
ORDER BY (scope, key);

CREATE TABLE tqd_g1.tag_values (
    scope     LowCardinality(String),
    key       String,
    value     String,
    val_type  LowCardinality(String)  -- string | int | float | bool
) ENGINE = ReplacingMergeTree
ORDER BY (scope, key, value, val_type);

INSERT INTO tqd_g1.spans (trace_id, span_id, parent_span_id, start_ns, duration_ns, service, resource_id, name, kind, status_code, status_message, scope_name, scope_version, scope_attrs, attrs, events, links)
SELECT unhex(trace_id), unhex(span_id), if(parent_span_id = '', toFixedString('', 8), unhex(parent_span_id)),
       start_ns, end_ns - start_ns, service, reinterpretAsUInt128(sipHash128(resource)), name, kind, status_code, status_message, scope_name, scope_version,
       CAST(tqd_kv2json(scope_attrs) AS JSON),
       CAST(tqd_kv2json(attrs) AS JSON),
       arrayMap(e -> (JSONExtract(e, 1, 'Int64'), JSONExtractString(e, 2), CAST(tqd_kv2json(JSONExtractRaw(e, 3)) AS JSON), 0), JSONExtractArrayRaw(events)),
       arrayMap(l -> (unhex(JSONExtractString(l, 1)), unhex(JSONExtractString(l, 2)), '', 0, CAST(tqd_kv2json(JSONExtractRaw(l, 3)) AS JSON), 0), JSONExtractArrayRaw(links))
FROM tqd_g1.raw;

INSERT INTO tqd_g1.resources (day, resource_id, service, attrs)
SELECT DISTINCT toDate(fromUnixTimestamp64Nano(start_ns)), reinterpretAsUInt128(sipHash128(resource)), service,
       CAST(tqd_kv2json_skip(resource, 'service.name') AS JSON)
FROM tqd_g1.raw;

-- What the writer sends to the catalogs, expressed over the staging rows: the
-- distinct (scope, key) and (scope, key, value, type) it has seen.
INSERT INTO tqd_g1.tag_names (scope, key)
SELECT DISTINCT scope, key FROM (
    SELECT 'span' AS scope, JSONExtractString(e, 1) AS key FROM tqd_g1.raw ARRAY JOIN JSONExtractArrayRaw(attrs) AS e
    UNION ALL
    SELECT 'resource', JSONExtractString(e, 1) FROM tqd_g1.raw ARRAY JOIN JSONExtractArrayRaw(resource) AS e
    UNION ALL
    SELECT 'event', JSONExtractString(a, 1) FROM tqd_g1.raw
        ARRAY JOIN JSONExtractArrayRaw(events) AS ev ARRAY JOIN JSONExtractArrayRaw(JSONExtractRaw(ev, 3)) AS a
    UNION ALL
    SELECT 'link', JSONExtractString(a, 1) FROM tqd_g1.raw
        ARRAY JOIN JSONExtractArrayRaw(links) AS lk ARRAY JOIN JSONExtractArrayRaw(JSONExtractRaw(lk, 3)) AS a
    UNION ALL
    SELECT 'instrumentation', JSONExtractString(e, 1) FROM tqd_g1.raw
        ARRAY JOIN JSONExtractArrayRaw(scope_attrs) AS e);

INSERT INTO tqd_g1.tag_values (scope, key, value, val_type)
SELECT DISTINCT scope, key, value, val_type FROM (
    SELECT 'span' AS scope, JSONExtractString(e, 1) AS key,
           if(JSONExtractString(e, 2) = 's', JSONExtractString(e, 3), JSONExtractRaw(e, 3)) AS value,
           transform(JSONExtractString(e, 2), ['s', 'i', 'd', 'b'], ['string', 'int', 'float', 'bool'], 'string') AS val_type
    FROM tqd_g1.raw ARRAY JOIN JSONExtractArrayRaw(attrs) AS e
    UNION ALL
    SELECT 'resource', JSONExtractString(e, 1),
           if(JSONExtractString(e, 2) = 's', JSONExtractString(e, 3), JSONExtractRaw(e, 3)),
           transform(JSONExtractString(e, 2), ['s', 'i', 'd', 'b'], ['string', 'int', 'float', 'bool'], 'string')
    FROM tqd_g1.raw ARRAY JOIN JSONExtractArrayRaw(resource) AS e
    UNION ALL
    SELECT 'event', JSONExtractString(a, 1),
           if(JSONExtractString(a, 2) = 's', JSONExtractString(a, 3), JSONExtractRaw(a, 3)),
           transform(JSONExtractString(a, 2), ['s', 'i', 'd', 'b'], ['string', 'int', 'float', 'bool'], 'string')
    FROM tqd_g1.raw ARRAY JOIN JSONExtractArrayRaw(events) AS ev ARRAY JOIN JSONExtractArrayRaw(JSONExtractRaw(ev, 3)) AS a
    UNION ALL
    SELECT 'link', JSONExtractString(a, 1),
           if(JSONExtractString(a, 2) = 's', JSONExtractString(a, 3), JSONExtractRaw(a, 3)),
           transform(JSONExtractString(a, 2), ['s', 'i', 'd', 'b'], ['string', 'int', 'float', 'bool'], 'string')
    FROM tqd_g1.raw ARRAY JOIN JSONExtractArrayRaw(links) AS lk ARRAY JOIN JSONExtractArrayRaw(JSONExtractRaw(lk, 3)) AS a
    UNION ALL
    SELECT 'instrumentation', JSONExtractString(e, 1),
           if(JSONExtractString(e, 2) = 's', JSONExtractString(e, 3), JSONExtractRaw(e, 3)),
           transform(JSONExtractString(e, 2), ['s', 'i', 'd', 'b'], ['string', 'int', 'float', 'bool'], 'string')
    FROM tqd_g1.raw ARRAY JOIN JSONExtractArrayRaw(scope_attrs) AS e);
