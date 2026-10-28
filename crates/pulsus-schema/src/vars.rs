//! The variables `schema/schema.sh` reads, and the one function from them to
//! a [`RenderCtx`] (issue #746 part A).

use std::collections::BTreeMap;

use crate::RenderCtx;

/// Every variable `schema/schema.sh` reads to decide the schema.
pub const SCHEMA_VARS: &[&str] = &[];

/// The variables `schema/schema.sh` reads only to reach the server.
pub const SCRIPT_CONNECTION_VARS: &[&str] = &[];

/// The most rollup levels a schema carries.
pub const MAX_LEVELS: usize = 0;

/// Every error `schema/schema.sh` prints, in script order: (message
/// template, exit).
pub const REFUSALS: &[(&str, &str)] = &[];

/// The script's `--mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaMode {
    Single,
    Cluster,
}

/// What `schema/schema.sh` decides from `vars` and `mode`.
pub fn ctx_from_vars(
    _vars: &BTreeMap<&str, &str>,
    _mode: Option<SchemaMode>,
) -> Result<RenderCtx, String> {
    Err("stub".into())
}
