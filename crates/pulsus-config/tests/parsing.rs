//! Every documented option parses into the expected typed value from a
//! single fixture YAML; duration/byte-size unit tables; overflow and
//! bad-suffix inputs return errors, never panic; `CLICKHOUSE_AUTH` splits
//! on the first colon only.

mod support;

use std::time::Duration;

use pulsus_config::{
    ByteSize, ChProto, HumanDuration, InsertMode, LogLevel, Mode, TierPolicy, parse,
};

const FIXTURE: &str = r#"
mode: writer
host: 10.0.0.1
port: 4100
log_level: warn
auth_user: alice
auth_password: s3cret
compat_endpoints: true
cors_origin: https://example.com
query_timeout: 30s
skip_ddl: true
retention_days: 14
storage_policy: hot_cold
rotation_interval: 2h
log_rollup_resolution: 10s
cluster: prod
dist_suffix: _replica
skip_unavailable_shards: true

clickhouse:
  server: ch.example.com
  port: 9440
  http_port: 8443
  database: analytics
  auth: "admin:pa:ss"
  proto: https
  tls_skip_verify: true
  pool_size: 16

writer:
  batch_bytes: 32MiB
  batch_ms: 500
  insert_mode: async
  ingest_queue_bytes: 512MiB

reader:
  cache_ttl: 30s
  cache_max_series: 10000
  series_activity_bucket: 1d
  cache_window: 12h
  promql_max_samples: 1000000
  promql_lookback: 10m
  logql_scan_budget_bytes: 10GiB
  traceql_max_candidates: 5000

downsampling:
  enabled: true
  raw_retention: 3d
  tier_policy: fast
  tiers:
    - name: 5m
      resolution: 5m
      table: metric_samples_5m
      retention: 90d
      min_step: 5m
    - name: 1h
      resolution: 1h
      table: metric_samples_1h
      retention: 730d
      min_step: 1h

ruler:
  enabled: true
  poll_interval: 15s
  max_result_bytes: 5MiB
"#;

#[test]
fn full_fixture_round_trips_into_typed_values() {
    let _guard = support::lock_env();
    support::clear_all();

    let path = support::write_temp_yaml("full-fixture", FIXTURE);
    let cfg = parse(Some(&path), None).expect("parse full fixture");

    assert_eq!(cfg.mode, Mode::Writer);
    assert_eq!(cfg.host, "10.0.0.1");
    assert_eq!(cfg.port, 4100);
    assert_eq!(cfg.log_level, LogLevel::Warn);
    assert_eq!(cfg.auth_user.as_deref(), Some("alice"));
    assert_eq!(
        cfg.auth_password.as_ref().map(|s| s.expose()),
        Some("s3cret")
    );
    assert!(cfg.compat_endpoints);
    assert_eq!(cfg.cors_origin, "https://example.com");
    assert_eq!(cfg.query_timeout.0, Duration::from_secs(30));
    assert!(cfg.skip_ddl);
    assert_eq!(cfg.retention_days, 14);
    assert_eq!(cfg.storage_policy.as_deref(), Some("hot_cold"));
    assert_eq!(cfg.rotation_interval.0, Duration::from_secs(2 * 3_600));
    assert_eq!(cfg.log_rollup_resolution.0, Duration::from_secs(10));
    assert_eq!(cfg.cluster.as_deref(), Some("prod"));
    assert_eq!(cfg.dist_suffix, "_replica");
    assert!(cfg.skip_unavailable_shards);

    assert_eq!(cfg.clickhouse.server, "ch.example.com");
    assert_eq!(cfg.clickhouse.port, 9440);
    assert_eq!(cfg.clickhouse.http_port, 8443);
    assert_eq!(cfg.clickhouse.database, "analytics");
    assert_eq!(cfg.clickhouse.auth.user, "admin");
    assert_eq!(
        cfg.clickhouse.auth.password.expose(),
        "pa:ss",
        "auth must split on the FIRST colon only"
    );
    assert_eq!(cfg.clickhouse.proto, ChProto::Https);
    assert!(cfg.clickhouse.tls_skip_verify);
    assert_eq!(cfg.clickhouse.pool_size, 16);

    assert_eq!(cfg.writer.batch_bytes, ByteSize(32 * 1024 * 1024));
    assert_eq!(cfg.writer.batch_ms, 500);
    assert_eq!(cfg.writer.insert_mode, InsertMode::Async);
    assert_eq!(cfg.writer.ingest_queue_bytes, ByteSize(512 * 1024 * 1024));

    assert_eq!(cfg.reader.cache_ttl.0, Duration::from_secs(30));
    assert_eq!(cfg.reader.cache_max_series, 10_000);
    assert_eq!(
        cfg.reader.series_activity_bucket.0,
        Duration::from_secs(86_400)
    );
    assert_eq!(cfg.reader.cache_window.0, Duration::from_secs(12 * 3_600));
    assert_eq!(cfg.reader.promql_max_samples, 1_000_000);
    assert_eq!(cfg.reader.promql_lookback.0, Duration::from_secs(600));
    assert_eq!(
        cfg.reader.logql_scan_budget_bytes,
        ByteSize(10 * 1024 * 1024 * 1024)
    );
    assert_eq!(cfg.reader.traceql_max_candidates, 5_000);

    assert!(cfg.downsampling.enabled);
    assert_eq!(
        cfg.downsampling.raw_retention.map(|d| d.0),
        Some(Duration::from_secs(3 * 86_400))
    );
    assert_eq!(cfg.downsampling.tier_policy, TierPolicy::Fast);
    assert_eq!(cfg.downsampling.tiers.len(), 2);
    assert_eq!(cfg.downsampling.tiers[0].name, "5m");
    assert_eq!(cfg.downsampling.tiers[0].table, "metric_samples_5m");
    assert_eq!(cfg.downsampling.tiers[1].name, "1h");
    assert_eq!(cfg.downsampling.tiers[1].table, "metric_samples_1h");

    assert!(cfg.ruler.enabled);
    assert_eq!(cfg.ruler.poll_interval.0, Duration::from_secs(15));
    assert_eq!(cfg.ruler.max_result_bytes, ByteSize(5 * 1024 * 1024));

    let _ = std::fs::remove_file(&path);
    support::clear_all();
}

#[test]
fn clickhouse_auth_env_var_splits_on_first_colon() {
    let _guard = support::lock_env();
    support::clear_all();

    support::set("CLICKHOUSE_AUTH", "admin:pa:ss");
    let cfg = parse(None, None).expect("parse with CLICKHOUSE_AUTH env var");
    assert_eq!(cfg.clickhouse.auth.user, "admin");
    assert_eq!(cfg.clickhouse.auth.password.expose(), "pa:ss");

    support::clear_all();
}

#[test]
fn parse_duration_table() {
    let cases: &[(&str, Duration)] = &[
        ("5s", Duration::from_secs(5)),
        ("2m", Duration::from_secs(120)),
        ("1h", Duration::from_secs(3_600)),
        ("40h", Duration::from_secs(40 * 3_600)),
        ("90d", Duration::from_secs(90 * 86_400)),
        ("730d", Duration::from_secs(730 * 86_400)),
        ("1w", Duration::from_secs(7 * 86_400)),
    ];
    for (input, expected) in cases {
        let got: HumanDuration = input.parse().unwrap_or_else(|e| panic!("{input}: {e:?}"));
        assert_eq!(got.0, *expected, "input {input}");
    }
}

#[test]
fn parse_bytes_table() {
    let cases: &[(&str, u64)] = &[
        ("16MiB", 16 * 1024 * 1024),
        ("256MiB", 256 * 1024 * 1024),
        ("50GiB", 50 * 1024 * 1024 * 1024),
        ("10MiB", 10 * 1024 * 1024),
        ("1024", 1024),
        ("1KB", 1_000),
    ];
    for (input, expected) in cases {
        let got: ByteSize = input.parse().unwrap_or_else(|e| panic!("{input}: {e:?}"));
        assert_eq!(got.0, *expected, "input {input}");
    }
}

#[test]
fn overflow_and_bad_suffix_return_errors_not_panics() {
    // u64::MAX days, multiplied by seconds/day, overflows u64.
    assert!("18446744073709551615d".parse::<HumanDuration>().is_err());
    assert!("5xyz".parse::<HumanDuration>().is_err());
    assert!(
        "200".parse::<HumanDuration>().is_err(),
        "duration requires a unit suffix"
    );
    // u64::MAX TiB overflows u64 bytes.
    assert!("18446744073709551615TiB".parse::<ByteSize>().is_err());
    assert!("5xyz".parse::<ByteSize>().is_err());
}
