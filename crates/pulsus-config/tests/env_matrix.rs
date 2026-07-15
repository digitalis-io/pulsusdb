//! Exhaustive, table-driven proof that every documented environment
//! variable (docs/configuration.md §§1–8, 41 variables) parses. Each row
//! clears the environment, sets only its own variable, calls `parse()`
//! (not `load()` — see issue #2 architect plan amendment 2), and asserts
//! the target field. `PULSUS_AUTH_USER`/`PULSUS_AUTH_PASSWORD` need no
//! special-casing because `parse()` performs no cross-field validation
//! (the one-sided basic-auth rule lives in `validate()`).
//!
//! A set-equality assertion against `pulsus_config::ALL_ENV_VARS` makes a
//! missing or misnamed row (or a variable `apply_env` doesn't wire up) a
//! red build.

mod support;

use std::time::Duration;

use pulsus_config::{ByteSize, ChProto, Config, InsertMode, LogLevel, Mode, TierPolicy};

struct Row {
    var: &'static str,
    value: &'static str,
    check: fn(&Config) -> bool,
}

const ROWS: &[Row] = &[
    Row {
        var: "PULSUS_MODE",
        value: "writer",
        check: |c| c.mode == Mode::Writer,
    },
    Row {
        var: "PULSUS_HOST",
        value: "row-host",
        check: |c| c.host == "row-host",
    },
    Row {
        var: "PULSUS_PORT",
        value: "4444",
        check: |c| c.port == 4444,
    },
    Row {
        var: "PULSUS_LOG_LEVEL",
        value: "trace",
        check: |c| c.log_level == LogLevel::Trace,
    },
    Row {
        var: "PULSUS_AUTH_USER",
        value: "row-user",
        check: |c| c.auth_user.as_deref() == Some("row-user"),
    },
    Row {
        var: "PULSUS_AUTH_PASSWORD",
        value: "row-pass",
        check: |c| c.auth_password.as_ref().map(|s| s.expose()) == Some("row-pass"),
    },
    Row {
        var: "PULSUS_COMPAT_ENDPOINTS",
        value: "true",
        check: |c| c.compat_endpoints,
    },
    Row {
        var: "PULSUS_CORS_ORIGIN",
        value: "https://row.example",
        check: |c| c.cors_origin == "https://row.example",
    },
    Row {
        var: "PULSUS_QUERY_TIMEOUT",
        value: "45s",
        check: |c| c.query_timeout.0 == Duration::from_secs(45),
    },
    Row {
        var: "CLICKHOUSE_SERVER",
        value: "row-ch",
        check: |c| c.clickhouse.server == "row-ch",
    },
    Row {
        var: "CLICKHOUSE_PORT",
        value: "9111",
        check: |c| c.clickhouse.port == 9111,
    },
    Row {
        var: "CLICKHOUSE_HTTP_PORT",
        value: "8111",
        check: |c| c.clickhouse.http_port == 8111,
    },
    Row {
        var: "CLICKHOUSE_DB",
        value: "row-db",
        check: |c| c.clickhouse.database == "row-db",
    },
    Row {
        var: "CLICKHOUSE_AUTH",
        value: "row-user:row-pass",
        check: |c| {
            c.clickhouse.auth.user == "row-user"
                && c.clickhouse.auth.password.expose() == "row-pass"
        },
    },
    Row {
        var: "CLICKHOUSE_PROTO",
        value: "http",
        check: |c| c.clickhouse.proto == ChProto::Http,
    },
    Row {
        var: "CLICKHOUSE_TLS_SKIP_VERIFY",
        value: "true",
        check: |c| c.clickhouse.tls_skip_verify,
    },
    Row {
        var: "PULSUS_CH_POOL_SIZE",
        value: "32",
        check: |c| c.clickhouse.pool_size == 32,
    },
    Row {
        var: "PULSUS_SKIP_DDL",
        value: "1",
        check: |c| c.skip_ddl,
    },
    Row {
        var: "PULSUS_RETENTION_DAYS",
        value: "30",
        check: |c| c.retention_days == 30,
    },
    Row {
        var: "PULSUS_STORAGE_POLICY",
        value: "row-policy",
        check: |c| c.storage_policy.as_deref() == Some("row-policy"),
    },
    Row {
        var: "PULSUS_ROTATION_INTERVAL",
        value: "3h",
        check: |c| c.rotation_interval.0 == Duration::from_secs(3 * 3_600),
    },
    Row {
        var: "PULSUS_LOG_ROLLUP_RESOLUTION",
        value: "15s",
        check: |c| c.log_rollup_resolution.0 == Duration::from_secs(15),
    },
    Row {
        var: "PULSUS_CLUSTER",
        value: "row-cluster",
        check: |c| c.cluster.as_deref() == Some("row-cluster"),
    },
    Row {
        var: "PULSUS_DIST_SUFFIX",
        value: "_row",
        check: |c| c.dist_suffix == "_row",
    },
    Row {
        var: "PULSUS_SKIP_UNAVAILABLE_SHARDS",
        value: "true",
        check: |c| c.skip_unavailable_shards,
    },
    Row {
        var: "PULSUS_BATCH_BYTES",
        value: "4MiB",
        check: |c| c.writer.batch_bytes == ByteSize(4 * 1024 * 1024),
    },
    Row {
        var: "PULSUS_BATCH_MS",
        value: "999",
        check: |c| c.writer.batch_ms == 999,
    },
    Row {
        var: "PULSUS_INSERT_MODE",
        value: "async",
        check: |c| c.writer.insert_mode == InsertMode::Async,
    },
    Row {
        var: "PULSUS_INGEST_QUEUE_BYTES",
        value: "8MiB",
        check: |c| c.writer.ingest_queue_bytes == ByteSize(8 * 1024 * 1024),
    },
    Row {
        var: "PULSUS_CACHE_TTL",
        value: "90s",
        check: |c| c.reader.cache_ttl.0 == Duration::from_secs(90),
    },
    Row {
        var: "PULSUS_CACHE_MAX_SERIES",
        value: "12345",
        check: |c| c.reader.cache_max_series == 12345,
    },
    Row {
        var: "PULSUS_SERIES_ACTIVITY_BUCKET",
        value: "2h",
        check: |c| c.reader.series_activity_bucket.0 == Duration::from_secs(2 * 3_600),
    },
    Row {
        var: "PULSUS_CACHE_WINDOW",
        value: "48h",
        check: |c| c.reader.cache_window.0 == Duration::from_secs(48 * 3_600),
    },
    Row {
        var: "PULSUS_PROMQL_MAX_SAMPLES",
        value: "42",
        check: |c| c.reader.promql_max_samples == 42,
    },
    Row {
        var: "PULSUS_PROMQL_LOOKBACK",
        value: "7m",
        check: |c| c.reader.promql_lookback.0 == Duration::from_secs(7 * 60),
    },
    Row {
        var: "PULSUS_LOGQL_SCAN_BUDGET_BYTES",
        value: "1GiB",
        check: |c| c.reader.logql_scan_budget_bytes == ByteSize(1024 * 1024 * 1024),
    },
    Row {
        var: "PULSUS_TRACEQL_MAX_CANDIDATES",
        value: "77",
        check: |c| c.reader.traceql_max_candidates == 77,
    },
    Row {
        var: "PULSUS_TRACEQL_SCAN_BUDGET_ROWS",
        value: "12345",
        check: |c| c.reader.traceql_scan_budget_rows == 12_345,
    },
    Row {
        var: "PULSUS_TIER_POLICY",
        value: "fast",
        check: |c| c.downsampling.tier_policy == TierPolicy::Fast,
    },
    Row {
        var: "PULSUS_RULER_ENABLED",
        value: "true",
        check: |c| c.ruler.enabled,
    },
    Row {
        var: "PULSUS_RULER_POLL_INTERVAL",
        value: "20s",
        check: |c| c.ruler.poll_interval.0 == Duration::from_secs(20),
    },
    Row {
        var: "PULSUS_RULER_MAX_RESULT_BYTES",
        value: "3MiB",
        check: |c| c.ruler.max_result_bytes == ByteSize(3 * 1024 * 1024),
    },
];

#[test]
fn matrix_rows_exactly_match_all_env_vars() {
    let mut declared: Vec<&str> = ROWS.iter().map(|r| r.var).collect();
    declared.sort_unstable();
    let mut deduped = declared.clone();
    deduped.dedup();
    assert_eq!(
        declared.len(),
        deduped.len(),
        "env_matrix.rs must not repeat a variable"
    );
    assert_eq!(
        declared.len(),
        42,
        "docs/configuration.md §§1-8 document exactly 42 variables"
    );

    let mut canonical: Vec<&str> = pulsus_config::ALL_ENV_VARS.to_vec();
    canonical.sort_unstable();
    assert_eq!(
        declared, canonical,
        "env_matrix.rs rows must exactly match ALL_ENV_VARS (set equality)"
    );
}

#[test]
fn each_documented_env_var_parses_in_isolation() {
    let _guard = support::lock_env();

    for row in ROWS {
        support::clear_all();
        support::set(row.var, row.value);

        let cfg = pulsus_config::parse(None, None)
            .unwrap_or_else(|e| panic!("{}: parse() failed unexpectedly: {e}", row.var));
        assert!(
            (row.check)(&cfg),
            "{}: value {:?} did not produce the expected field value",
            row.var,
            row.value
        );
    }

    support::clear_all();
}
