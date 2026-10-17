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
  cache_window: 12h
  promql_max_samples: 1000000
  promql_lookback: 10m
  promql_max_metric_fanout: 123
  logql_scan_budget_bytes: 10GiB
  traceql_max_candidates: 5000
  traceql_tag_lookback: 3h

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
    assert_eq!(cfg.reader.cache_window.0, Duration::from_secs(12 * 3_600));
    assert_eq!(cfg.reader.promql_max_samples, 1_000_000);
    assert_eq!(cfg.reader.promql_lookback.0, Duration::from_secs(600));
    assert_eq!(cfg.reader.promql_max_metric_fanout, 123);
    assert_eq!(
        cfg.reader.logql_scan_budget_bytes,
        ByteSize(10 * 1024 * 1024 * 1024)
    );
    // AC9(iii), issue M6-09: the fixture's partial `reader:` block omits
    // `logql_pipeline_scan_factor`, so it must resolve from the
    // container-level `Default` (10) — never from `u32::default()` (0),
    // which a field-level `#[serde(default)]` would have produced.
    assert_eq!(cfg.reader.logql_pipeline_scan_factor, 10);
    assert_eq!(cfg.reader.traceql_max_candidates, 5_000);
    assert_eq!(
        cfg.reader.traceql_tag_lookback.0,
        Duration::from_secs(3 * 3_600)
    );

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

/// AC9(iii), issue M6-09: a zero `logql_pipeline_scan_factor` — whether
/// it arrives via YAML or via `PULSUS_LOGQL_PIPELINE_SCAN_FACTOR` — is
/// rejected by the load pipeline as `ConfigError::Value` naming the
/// field; a non-zero YAML value parses through normally.
#[test]
fn zero_logql_pipeline_scan_factor_is_rejected_from_yaml_and_env() {
    let _guard = support::lock_env();
    support::clear_all();

    // YAML: 0 fails `load` (parse + validate) with the named field.
    let path = support::write_temp_yaml(
        "zero-scan-factor",
        "reader:\n  logql_pipeline_scan_factor: 0\n",
    );
    match pulsus_config::load(Some(&path), None) {
        Err(pulsus_config::ConfigError::Value { field, .. }) => {
            assert_eq!(field, "reader.logql_pipeline_scan_factor");
        }
        other => panic!("expected a Value error for YAML factor 0, got {other:?}"),
    }
    let _ = std::fs::remove_file(&path);

    // Env: 0 fails `load` the same way.
    support::set("PULSUS_LOGQL_PIPELINE_SCAN_FACTOR", "0");
    match pulsus_config::load(None, None) {
        Err(pulsus_config::ConfigError::Value { field, .. }) => {
            assert_eq!(field, "reader.logql_pipeline_scan_factor");
        }
        other => panic!("expected a Value error for env factor 0, got {other:?}"),
    }
    support::clear_all();

    // A non-zero YAML value parses and validates through.
    let path = support::write_temp_yaml(
        "nonzero-scan-factor",
        "reader:\n  logql_pipeline_scan_factor: 4\n",
    );
    let cfg = pulsus_config::load(Some(&path), None).expect("factor 4 must load");
    assert_eq!(cfg.reader.logql_pipeline_scan_factor, 4);
    let _ = std::fs::remove_file(&path);
    support::clear_all();
}

/// Issue #602 part 2c-2: the three settings only the deleted search
/// engine read are gone, and `deny_unknown_fields` makes a YAML that still
/// names one fail to load, naming the key.
#[test]
fn the_three_removed_traceql_settings_are_refused_in_yaml() {
    let _guard = support::lock_env();
    support::clear_all();
    for key in [
        "traceql_max_candidates",
        "traceql_event_set_max_values",
        "traceql_generator_max_memory_bytes",
    ] {
        let path =
            support::write_temp_yaml(&format!("removed-{key}"), &format!("reader:\n  {key}: 1\n"));
        let err =
            pulsus_config::load(Some(&path), None).expect_err("a removed reader key must not load");
        assert!(err.to_string().contains(key), "{key}: {err}");
        let _ = std::fs::remove_file(&path);
    }
    support::clear_all();
}

/// Issue #602 part 2c-2b-2: the three settings' environment variables are
/// no longer read. Each is set to `0`, which their range checks refused
/// (`>= 1`), so before the removal every one failed `load`; now the
/// configuration loads and the value goes nowhere.
#[test]
fn the_three_removed_traceql_environment_variables_are_ignored() {
    let _guard = support::lock_env();
    support::clear_all();
    for var in [
        "PULSUS_TRACEQL_MAX_CANDIDATES",
        "PULSUS_TRACEQL_EVENT_SET_MAX_VALUES",
        "PULSUS_TRACEQL_GENERATOR_MAX_MEMORY_BYTES",
    ] {
        support::set(var, "0");
        let loaded = pulsus_config::load(None, None);
        // SAFETY: see `support::clear_all`; the guard is held.
        unsafe { std::env::remove_var(var) };
        loaded.unwrap_or_else(|e| panic!("{var}=0 must be ignored, not refused: {e}"));
    }
    support::clear_all();
}

/// Issue #311: an unknown `reader.template_timezone` — from YAML or from
/// `PULSUS_TEMPLATE_TIMEZONE` — fails config load naming the offending
/// value. There is deliberately NO fallback: a server that silently
/// renders in some other zone is the defect this setting removes. A known
/// zone loads through, and the default is UTC.
#[test]
fn an_unknown_template_timezone_is_rejected_from_yaml_and_env() {
    let _guard = support::lock_env();
    support::clear_all();

    assert_eq!(
        pulsus_config::Config::default().reader.template_timezone,
        pulsus_config::TemplateTimezone::UTC,
        "the documented default is UTC"
    );

    // YAML: the deserializer rejects it, so `Config` can never hold it.
    let path = support::write_temp_yaml(
        "unknown-template-timezone",
        "reader:\n  template_timezone: Europe/Lundon\n",
    );
    match pulsus_config::load(Some(&path), None) {
        Err(err @ pulsus_config::ConfigError::Yaml { .. }) => {
            let msg = err.to_string();
            assert!(
                msg.contains("Europe/Lundon") && msg.contains("IANA"),
                "message must name the offending zone: {msg}"
            );
        }
        other => panic!("expected a Yaml error for an unknown zone, got {other:?}"),
    }
    let _ = std::fs::remove_file(&path);

    // Env: the same rejection, naming the variable.
    support::set("PULSUS_TEMPLATE_TIMEZONE", "Europe/Lundon");
    match pulsus_config::load(None, None) {
        Err(pulsus_config::ConfigError::Env { var, msg }) => {
            assert_eq!(var, "PULSUS_TEMPLATE_TIMEZONE");
            assert!(
                msg.contains("Europe/Lundon") && msg.contains("IANA"),
                "message must name the offending zone: {msg}"
            );
        }
        other => panic!("expected an Env error for an unknown zone, got {other:?}"),
    }
    support::clear_all();

    // A known zone loads through and keeps its canonical IANA name.
    let path = support::write_temp_yaml(
        "known-template-timezone",
        "reader:\n  template_timezone: Europe/London\n",
    );
    let cfg = pulsus_config::load(Some(&path), None).expect("a known zone must load");
    assert_eq!(cfg.reader.template_timezone.name(), "Europe/London");
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
