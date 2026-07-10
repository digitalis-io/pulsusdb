//! `pulsusdb` with only `CLICKHOUSE_SERVER` set produces a valid effective
//! config (docs/configuration.md intro: "every option has a sane
//! single-node default").

mod support;

use pulsus_config::{Mode, load};

#[test]
fn only_clickhouse_server_set_produces_a_valid_effective_config() {
    let _guard = support::lock_env();
    support::clear_all();

    support::set("CLICKHOUSE_SERVER", "ch.internal");

    let cfg = load(None, None).expect("minimal config must load and validate");

    assert_eq!(cfg.clickhouse.server, "ch.internal");
    // Everything else resolves to its documented default.
    assert_eq!(cfg.mode, Mode::All);
    assert_eq!(cfg.host, "0.0.0.0");
    assert_eq!(cfg.port, 3100);
    assert_eq!(cfg.clickhouse.port, 9_000);
    assert_eq!(cfg.clickhouse.http_port, 8_123);
    assert_eq!(cfg.clickhouse.database, "pulsus");
    assert_eq!(cfg.clickhouse.pool_size, 8);
    assert_eq!(cfg.retention_days, 7);
    assert_eq!(cfg.dist_suffix, "_dist");
    assert!(!cfg.downsampling.enabled);
    assert!(!cfg.ruler.enabled);

    support::clear_all();
}
