//! Issue #746 part A, A8b: the binary reads the dedup windows and the log
//! rollup resolution with the grammars `schema/schema.sh` reads them with.
//! The values are `docs/configuration.md`'s "Grammar boundaries" table.

mod support;

use pulsus_config::HumanDuration;

const DEDUP_VARS: [&str; 3] = [
    "PULSUS_METRICS_DEDUP_WINDOW",
    "PULSUS_LOG_DEDUP_WINDOW",
    "PULSUS_TRACE_DEDUP_WINDOW",
];

/// **A8b: each dedup window from the environment, and the log rollup
/// resolution from the environment and from YAML, are accepted and refused
/// exactly as the table says.**
#[test]
fn the_binary_reads_the_documented_boundaries() {
    let _guard = support::lock_env();
    for (accepted, grammar, values) in support::grammar_boundaries() {
        for v in values {
            if grammar == "dedup window" {
                for (at, var) in DEDUP_VARS.iter().enumerate() {
                    support::clear_all();
                    support::set(var, &v);
                    let got = pulsus_config::parse(None, None);
                    support::clear_all();
                    if accepted {
                        let cfg = got.unwrap_or_else(|e| panic!("{var}={v:?} refused: {e}"));
                        let windows = [
                            cfg.metrics_dedup_window,
                            cfg.log_dedup_window,
                            cfg.trace_dedup_window,
                        ];
                        assert_eq!(windows[at], v.parse::<u64>().unwrap(), "{var}={v:?}");
                    } else {
                        assert!(got.is_err(), "{var}={v:?} accepted: {got:?}");
                    }
                }
            } else {
                support::clear_all();
                support::set("PULSUS_LOG_ROLLUP_RESOLUTION", &v);
                let from_env = pulsus_config::parse(None, None);
                support::clear_all();
                let path = support::write_temp_yaml(
                    "log-rollup-resolution",
                    &format!("log_rollup_resolution: \"{v}\"\n"),
                );
                let from_yaml = pulsus_config::parse(Some(&path), None);
                std::fs::remove_file(&path).expect("remove the fixture");
                for (how, got) in [("environment", from_env), ("YAML", from_yaml)] {
                    if accepted {
                        let cfg = got.unwrap_or_else(|e| panic!("{how} {v:?} refused: {e}"));
                        assert_eq!(
                            cfg.log_rollup_resolution,
                            HumanDuration(support::resolution_of(&v)),
                            "{how} {v:?}"
                        );
                    } else {
                        assert!(got.is_err(), "{how} {v:?} accepted: {got:?}");
                    }
                }
            }
        }
    }
}
