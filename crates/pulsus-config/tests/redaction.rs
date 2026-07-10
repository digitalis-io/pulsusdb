//! Secrets never appear in the redacted config dump or in `Debug` output.

mod support;

use pulsus_config::parse;

#[test]
fn redacted_yaml_and_debug_never_contain_secret_values() {
    let _guard = support::lock_env();
    support::clear_all();

    support::set("PULSUS_AUTH_USER", "alice");
    support::set("PULSUS_AUTH_PASSWORD", "top-secret-auth-password");
    support::set("CLICKHOUSE_AUTH", "admin:top-secret-ch-password");

    let cfg = parse(None, None).expect("parse with secrets set");

    let dump = cfg.to_redacted_yaml().expect("redacted yaml dump");
    assert!(
        !dump.contains("top-secret-auth-password"),
        "auth_password leaked into the redacted dump"
    );
    assert!(
        !dump.contains("top-secret-ch-password"),
        "clickhouse.auth password leaked into the redacted dump"
    );
    assert!(
        dump.contains("***"),
        "redacted dump should contain the \"***\" placeholder"
    );

    let debug_repr = format!("{cfg:?}");
    assert!(
        !debug_repr.contains("top-secret-auth-password"),
        "auth_password leaked into Debug output"
    );
    assert!(
        !debug_repr.contains("top-secret-ch-password"),
        "clickhouse.auth password leaked into Debug output"
    );

    support::clear_all();
}
