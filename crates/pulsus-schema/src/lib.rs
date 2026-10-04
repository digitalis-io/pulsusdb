//! What the binary needs from the schema, which is no longer how to create
//! it.
//!
//! The DDL lives in `schema/schema.sql` and `schema/schema.sh` applies it
//! (docs/schemas.md §6). This crate reads that file for the two things a
//! serving process asks at run time — a view's own projection and a table's
//! declared column list — checks the server version and the setting and
//! function names this build sends, and replays a landing window.
//!
//! Nothing here sends DDL. A test builds a schema through
//! `pulsus-schema-testkit`, which renders the same file.

mod checks;
mod error;
mod render;
mod replay;
mod sql;

pub use checks::{
    DEDUP_WINDOW_SECONDS, NameCatalogue, REQUIRED_SERVER_NAMES, absent_server_names, check_version,
    database_exists, missing_server_names, required_names_sql, server_version,
};
pub use error::SchemaError;
pub use render::{RenderCtx, SchemaParams, render_name, rollup_suffix};
pub use replay::{ReplayReport, replay_trace_window};
pub use sql::{SCHEMA_SQL, mv_projection, rendered, rendered_statements, table_column_names};
