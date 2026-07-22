//! Pins the YAML shape in docs/configuration.md §9 against this crate's
//! model: parses the literal §9 example block (root-level scalars, not
//! nested under `schema:`/`cluster:`) and asserts it succeeds under
//! `deny_unknown_fields` with every value resolving to its documented
//! default. A future re-nesting of the root fields (regressing to the
//! rejected `SchemaConfig`/`ClusterConfig` shape) would fail this test.

mod support;

use std::time::Duration;

use pulsus_config::{Config, parse};

/// Copied verbatim from docs/configuration.md §9.
const GOLDEN_YAML: &str = r#"
mode: all                        # all | writer | reader | init
host: 0.0.0.0
port: 3100
log_level: info                  # error | warn | info | debug | trace
auth_user: null                  # both set => basic auth; one-sided => startup error
auth_password: null              # secret: never appears in redacted output
compat_endpoints: false
cors_origin: "*"
query_timeout: 2m
tls_cert: null                   # PEM cert chain path; with tls_key set, the one listener is TLS-only
tls_key: null                    # PEM private key path; one-sided => startup error
skip_ddl: false
retention_days: 7
storage_policy: null
rotation_interval: 1h
log_rollup_resolution: 5s
cluster: null                    # ClickHouse cluster name; enables distributed DDL
dist_suffix: _dist
skip_unavailable_shards: false

clickhouse:
  server: localhost
  port: 9000
  http_port: 8123
  database: pulsus
  auth: "default:"               # user:password, split on first colon; password is secret
  proto: http                     # http | https  (native rejected at startup — ADR 0001)
  tls_skip_verify: false
  pool_size: 8

writer:
  batch_bytes: 16MiB
  batch_ms: 200
  insert_mode: sync              # sync | async
  ingest_queue_bytes: 256MiB

reader:
  cache_ttl: 60s
  cache_max_series: 50000
  series_activity_bucket: 1h
  cache_window: 24h
  promql_max_samples: 50000000
  promql_lookback: 5m
  promql_experimental_functions: false
  promql_max_metric_fanout: 1000
  promql_max_cache_scan: 200000
  logql_scan_budget_bytes: 50GiB
  logql_pipeline_scan_factor: 10
  traceql_max_candidates: 100000
  traceql_scan_budget_rows: 50000000

downsampling:
  enabled: false
  raw_retention: null            # overrides retention_days for metric_samples when set
  tier_policy: exact             # exact | fast
  tiers: []                      # see §7; name/table unique, resolution/min_step/retention
                                 # strictly increasing, min_step >= resolution per tier

ruler:
  enabled: false
  poll_interval: 30s
  max_result_bytes: 10MiB
"#;

#[test]
fn golden_yaml_from_configuration_md_section_9_parses_and_matches_defaults() {
    let _guard = support::lock_env();
    support::clear_all();

    let path = support::write_temp_yaml("golden-yaml", GOLDEN_YAML);
    let cfg = parse(Some(&path), None)
        .expect("docs/configuration.md §9's YAML must parse under deny_unknown_fields");

    assert_eq!(cfg.retention_days, 7);
    assert_eq!(cfg.clickhouse.server, "localhost");
    assert_eq!(cfg.clickhouse.pool_size, 8);
    assert_eq!(cfg.writer.batch_ms, 200);
    assert_eq!(cfg.reader.cache_max_series, 50_000);
    assert_eq!(cfg.ruler.poll_interval.0, Duration::from_secs(30));
    assert_eq!(cfg.downsampling.tiers.len(), 0);
    assert_eq!(
        cfg,
        Config::default(),
        "§9's documented YAML must resolve to exactly the built-in defaults"
    );

    let _ = std::fs::remove_file(&path);
    support::clear_all();
}
