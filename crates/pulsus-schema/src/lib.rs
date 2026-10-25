//! What the binary needs from the schema, which is no longer how to create
//! it.
//!
//! The DDL lives in `schema/schema.sql` and `schema/schema.sh` applies it
//! (docs/schemas.md §6). This crate reads that file for the one thing a
//! serving process asks at run time — a table's declared column list —
//! and checks the server version and the setting and function names this
//! build sends.
//!
//! Nothing here sends DDL. A test builds a schema through
//! `pulsus-schema-testkit`, which renders the same file.

mod checks;
mod error;
mod render;
mod sql;

pub use checks::{
    DEDUP_WINDOW_SECONDS, NameCatalogue, REQUIRED_SERVER_NAMES, SchemaRetention,
    absent_server_names, check_version, database_exists, decide_retention, missing_server_names,
    required_names_sql, retention_days_from_comment, schema_retention, server_version,
};
pub use error::SchemaError;
pub use render::{
    GRAPH_JOB_MAX_ROWS, ROLLUP_SETTLE_MS, RenderCtx, SchemaParams, render_name, rollup_suffix,
};
pub use sql::{SCHEMA_SQL, mv_projection, rendered, rendered_statements, table_column_names};
