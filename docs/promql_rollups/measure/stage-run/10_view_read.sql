DROP DATABASE IF EXISTS m800_view SYNC;

CREATE DATABASE m800_view;

CREATE TABLE m800_view.plain (k UInt64, v Float64) ENGINE = MergeTree ORDER BY k;

CREATE TABLE m800_view.viewed (k UInt64, v Float64) ENGINE = MergeTree ORDER BY k;

CREATE TABLE m800_view.copy (k UInt64, v Float64) ENGINE = MergeTree ORDER BY k;

CREATE MATERIALIZED VIEW m800_view.viewed_mv TO m800_view.copy AS SELECT k, v FROM m800_view.viewed;

-- log_comment = 'm800 view-read plain'
INSERT INTO m800_view.plain SELECT number, number / 7 FROM numbers(30000);

-- log_comment = 'm800 view-read viewed'
INSERT INTO m800_view.viewed SELECT number, number / 7 FROM numbers(30000);
