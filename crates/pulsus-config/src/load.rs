//! The load pipeline: YAML < env < CLI precedence merge, then startup
//! validation. Split into [`parse`] (merge only, no cross-field validation)
//! and [`crate::validate::validate`] per the issue #2 architect plan
//! amendment 2 — this lets `tests/env_matrix.rs` assert every documented
//! environment variable parses without being blocked by a validation rule
//! that spans two variables (one-sided basic auth, rule #12).

use std::path::Path;

use crate::env::apply_env;
use crate::error::ConfigError;
use crate::model::Config;
use crate::validate::validate;

/// Merges defaults, an optional YAML file, environment variables, and a
/// `--mode` CLI override, in that precedence order (lowest to highest:
/// default < YAML < env < CLI). Performs no cross-field validation — see
/// [`load`] for the full startup pipeline.
pub fn parse(
    config_path: Option<&Path>,
    mode_override: Option<&str>,
) -> Result<Config, ConfigError> {
    let mut cfg = match config_path {
        Some(path) => read_yaml(path)?,
        None => Config::default(),
    };

    apply_env(&mut cfg)?;

    if let Some(mode) = mode_override {
        cfg.mode = mode.parse().map_err(|expected| ConfigError::Value {
            field: "mode".to_string(),
            msg: format!("invalid value {mode:?}"),
            expected,
        })?;
    }

    Ok(cfg)
}

fn read_yaml(path: &Path) -> Result<Config, ConfigError> {
    let contents = std::fs::read_to_string(path).map_err(|source| ConfigError::ReadFile {
        path: path.display().to_string(),
        source,
    })?;
    serde_norway::from_str(&contents).map_err(|source| ConfigError::Yaml {
        path: path.display().to_string(),
        source,
    })
}

/// The full startup pipeline: [`parse`] the effective configuration, then
/// [`crate::validate::validate`] it. This is the entry point used by
/// `pulsus-server`'s `main.rs`.
pub fn load(
    config_path: Option<&Path>,
    mode_override: Option<&str>,
) -> Result<Config, ConfigError> {
    let cfg = parse(config_path, mode_override)?;
    validate(&cfg)?;
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_with_no_yaml_and_no_env_returns_defaults() {
        // No env mutation here, so no lock needed against other test
        // threads — but ambient host env could still leak in; this is a
        // best-effort smoke test, the exhaustive isolation lives in
        // tests/env_matrix.rs and tests/precedence.rs.
        let cfg = parse(None, None).expect("parse with no yaml/env");
        assert_eq!(cfg.mode, crate::model::Mode::All);
    }

    #[test]
    fn read_yaml_reports_the_path_on_a_missing_file() {
        let path = Path::new("/nonexistent/pulsus-config-test.yaml");
        let err = read_yaml(path).unwrap_err();
        assert!(matches!(err, ConfigError::ReadFile { .. }));
    }

    #[test]
    fn invalid_mode_override_is_rejected() {
        let err = parse(None, Some("bogus")).unwrap_err();
        assert!(matches!(err, ConfigError::Value { field, .. } if field == "mode"));
    }
}
