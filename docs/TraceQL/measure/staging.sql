-- The staging table every layout is built from: one row per span, exactly the
-- shape docs/TraceQL/measure/gen_corpus.py and otlp_to_rows.py emit. Attribute
-- lists arrive as JSON arrays of [key, type, value] and are held as text here,
-- so the staging load itself makes no typing decision.
--
--   clickhouse-client --query "$(cat staging.sql)"   -- or over HTTP, one statement at a time
-- then load:
--   INSERT INTO <db>.raw SELECT * FROM file('g1/spans.jsonl', JSONEachRow, '<the column list below>')
--   SETTINGS input_format_json_read_arrays_as_strings = 1, input_format_json_read_objects_as_strings = 1

CREATE TABLE IF NOT EXISTS raw
(
    trace_id        String,
    span_id         String,
    parent_span_id  String,
    name            String,
    kind            UInt8,
    start_ns        Int64,
    end_ns          Int64,
    status_code     UInt8,
    status_message  String,
    service         String,
    resource        String,   -- JSON array of [key, type, value]
    scope_name      String,
    scope_version   String,
    scope_attrs     String,   -- JSON array of [key, type, value]
    attrs           String,   -- JSON array of [key, type, value]
    events          String,   -- JSON array of [time_ns, name, attrs]
    links           String    -- JSON array of [trace_id, span_id, attrs]
) ENGINE = MergeTree ORDER BY tuple();
