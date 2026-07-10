//! docs/configuration.md §7 tier rules, each violated in isolation, plus
//! the sensible-guard rules (enabled-with-no-tiers, duplicate name/table).
//! These target `validate()` directly — no env or YAML I/O involved, so no
//! environment locking is needed here.

use std::time::Duration;

use pulsus_config::{Config, HumanDuration, Tier, validate};

fn tier(
    name: &str,
    resolution_secs: u64,
    table: &str,
    retention_secs: u64,
    min_step_secs: u64,
) -> Tier {
    Tier {
        name: name.to_string(),
        resolution: HumanDuration(Duration::from_secs(resolution_secs)),
        table: table.to_string(),
        retention: HumanDuration(Duration::from_secs(retention_secs)),
        min_step: HumanDuration(Duration::from_secs(min_step_secs)),
    }
}

fn valid_two_tier_config() -> Config {
    let mut cfg = Config::default();
    cfg.downsampling.enabled = true;
    cfg.downsampling.tiers = vec![
        tier("5m", 300, "metric_samples_5m", 90 * 86_400, 300),
        tier("1h", 3_600, "metric_samples_1h", 730 * 86_400, 3_600),
    ];
    cfg
}

#[test]
fn valid_two_tier_config_passes() {
    assert!(validate(&valid_two_tier_config()).is_ok());
}

#[test]
fn min_step_less_than_resolution_is_rejected() {
    let mut cfg = valid_two_tier_config();
    cfg.downsampling.tiers[0].min_step = HumanDuration(Duration::from_secs(60));
    assert!(validate(&cfg).is_err());
}

#[test]
fn non_strictly_increasing_resolution_is_rejected() {
    let mut cfg = valid_two_tier_config();
    let first_resolution = cfg.downsampling.tiers[0].resolution;
    cfg.downsampling.tiers[1].resolution = first_resolution;
    // min_step must stay >= resolution for the (now-equal) second tier too.
    cfg.downsampling.tiers[1].min_step = first_resolution;
    assert!(validate(&cfg).is_err());
}

#[test]
fn non_strictly_increasing_min_step_is_rejected() {
    let mut cfg = valid_two_tier_config();
    let first_min_step = cfg.downsampling.tiers[0].min_step;
    cfg.downsampling.tiers[1].min_step = first_min_step;
    assert!(validate(&cfg).is_err());
}

#[test]
fn non_strictly_increasing_retention_is_rejected() {
    let mut cfg = valid_two_tier_config();
    let first_retention = cfg.downsampling.tiers[0].retention;
    cfg.downsampling.tiers[1].retention = first_retention;
    assert!(validate(&cfg).is_err());
}

#[test]
fn enabled_with_no_tiers_is_rejected() {
    let mut cfg = Config::default();
    cfg.downsampling.enabled = true;
    assert!(validate(&cfg).is_err());
}

#[test]
fn duplicate_tier_name_is_rejected() {
    let mut cfg = valid_two_tier_config();
    cfg.downsampling.tiers[1].name = cfg.downsampling.tiers[0].name.clone();
    assert!(validate(&cfg).is_err());
}

#[test]
fn duplicate_tier_table_is_rejected() {
    let mut cfg = valid_two_tier_config();
    cfg.downsampling.tiers[1].table = cfg.downsampling.tiers[0].table.clone();
    assert!(validate(&cfg).is_err());
}

#[test]
fn zero_resolution_is_rejected() {
    let mut cfg = valid_two_tier_config();
    cfg.downsampling.tiers[0].resolution = HumanDuration(Duration::ZERO);
    cfg.downsampling.tiers[0].min_step = HumanDuration(Duration::ZERO);
    assert!(validate(&cfg).is_err());
}

#[test]
fn empty_tier_name_is_rejected() {
    let mut cfg = valid_two_tier_config();
    cfg.downsampling.tiers[0].name = String::new();
    assert!(validate(&cfg).is_err());
}

#[test]
fn empty_tier_table_is_rejected() {
    let mut cfg = valid_two_tier_config();
    cfg.downsampling.tiers[0].table = String::new();
    assert!(validate(&cfg).is_err());
}
