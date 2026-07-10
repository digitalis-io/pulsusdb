//! Precedence: CLI flag > environment variable > YAML (`--config`) >
//! built-in default (docs/configuration.md intro), plus the
//! empty-env-var-is-unset rule.

mod support;

use std::time::Duration;

use pulsus_config::{ByteSize, InsertMode, LogLevel, Mode, parse};

#[test]
fn defaults_only_when_nothing_is_set() {
    let _guard = support::lock_env();
    support::clear_all();

    let cfg = parse(None, None).expect("parse with defaults");
    assert_eq!(cfg.host, "0.0.0.0");
    assert_eq!(cfg.port, 3100);
    assert_eq!(cfg.mode, Mode::All);
    assert_eq!(cfg.writer.insert_mode, InsertMode::Sync);

    support::clear_all();
}

#[test]
fn yaml_overrides_default() {
    let _guard = support::lock_env();
    support::clear_all();

    let path =
        support::write_temp_yaml("yaml-over-default", "host: yaml-host.example\nport: 9999\n");
    let cfg = parse(Some(&path), None).expect("parse yaml over default");
    assert_eq!(cfg.host, "yaml-host.example");
    assert_eq!(cfg.port, 9999);
    // Untouched keys still resolve to their documented default.
    assert_eq!(cfg.mode, Mode::All);

    let _ = std::fs::remove_file(&path);
    support::clear_all();
}

#[test]
fn env_overrides_yaml_for_a_representative_variable_of_each_type() {
    let _guard = support::lock_env();
    support::clear_all();

    let path = support::write_temp_yaml(
        "env-over-yaml",
        "host: yaml-host\nport: 1111\ncompat_endpoints: false\nquery_timeout: 1m\nlog_level: info\nwriter:\n  batch_bytes: 1MiB\n",
    );

    support::set("PULSUS_HOST", "env-host"); // string
    support::set("PULSUS_PORT", "2222"); // u16
    support::set("PULSUS_COMPAT_ENDPOINTS", "true"); // bool
    support::set("PULSUS_QUERY_TIMEOUT", "5m"); // duration
    support::set("PULSUS_BATCH_BYTES", "2MiB"); // byte size
    support::set("PULSUS_LOG_LEVEL", "debug"); // enum

    let cfg = parse(Some(&path), None).expect("parse yaml+env");
    assert_eq!(cfg.host, "env-host");
    assert_eq!(cfg.port, 2222);
    assert!(cfg.compat_endpoints);
    assert_eq!(cfg.query_timeout.0, Duration::from_secs(300));
    assert_eq!(cfg.writer.batch_bytes, ByteSize(2 * 1024 * 1024));
    assert_eq!(cfg.log_level, LogLevel::Debug);

    let _ = std::fs::remove_file(&path);
    support::clear_all();
}

#[test]
fn cli_mode_override_beats_env_mode() {
    let _guard = support::lock_env();
    support::clear_all();

    support::set("PULSUS_MODE", "writer");
    let cfg = parse(None, Some("reader")).expect("parse with --mode override");
    assert_eq!(cfg.mode, Mode::Reader);

    support::clear_all();
}

#[test]
fn empty_string_env_var_is_treated_as_unset() {
    let _guard = support::lock_env();
    support::clear_all();

    support::set("PULSUS_HOST", "");
    let cfg = parse(None, None).expect("parse with empty env var");
    assert_eq!(
        cfg.host, "0.0.0.0",
        "an empty PULSUS_HOST must not override the default"
    );

    support::clear_all();
}
