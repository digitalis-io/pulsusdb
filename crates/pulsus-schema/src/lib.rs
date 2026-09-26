//! DDL templates, migrations, TTL rotation, and materialized view lifecycle.
//! See docs/architecture.md §3 and docs/schemas.md (the byte-authoritative
//! DDL this crate renders and executes).
//!
//! The public surface takes an already-connected `pulsus_clickhouse::ChClient`
//! plus [`SchemaParams`] (a plain, `Config`-derived struct) — this crate has
//! no dependency on `pulsus-config` or on how the connection was built
//! (task-manager resolution #4, issue #5: that mapping lives once in
//! `pulsus-server`).

mod bookkeeping;
mod catalog;
mod controller;
mod error;
mod render;
mod rotation;

pub use controller::{
    NameCatalogue, REQUIRED_SERVER_NAMES, SchemaParams, absent_server_names, apply_ttl,
    check_version, guard_skip_ddl_in_init, missing_server_names, mv_projection, reconcile,
    required_names_sql, run_init,
};
pub use error::SchemaError;
pub use render::{Family, RenderCtx, rollup_suffix};
pub use rotation::spawn_rotation;

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles() {}
}
