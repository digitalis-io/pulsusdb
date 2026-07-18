//! Typed configuration for PulsusDB: environment-variable/YAML loading,
//! startup validation, and secret redaction. See
//! [`docs/configuration.md`](https://github.com/digitalis-io/pulsusdb/blob/main/docs/configuration.md)
//! for the variable tables (§§1–8) and the complete YAML schema (§9) this
//! crate implements 1:1.
//!
//! **Precedence:** CLI flag (`--mode`) > environment variable > YAML
//! (`--config`) > built-in default (docs/configuration.md intro). An
//! environment variable set to the empty string counts as unset.
//!
//! [`load`] runs the full pipeline (merge, then validate) and is what
//! `pulsus-server`'s `main.rs` calls at startup. [`parse`] and [`validate`]
//! are exposed separately so parsing and cross-field validation can be
//! tested independently (see `tests/env_matrix.rs`).

mod env;
mod error;
mod load;
mod model;
mod secret;
mod units;
mod validate;

pub use env::ALL_ENV_VARS;
pub use error::ConfigError;
pub use load::{load, parse};
pub use model::{
    AzDetect, ChAuth, ChProto, ChServerEntry, ClickHouseConfig, Config, DownsamplingConfig,
    ExpHistogramMode, InsertMode, LogLevel, Mode, ReaderConfig, RulerConfig, Tier, TierPolicy,
    WriterConfig,
};
pub use secret::Secret;
pub use units::{ByteSize, HumanDuration, UnitError};
pub use validate::validate;
