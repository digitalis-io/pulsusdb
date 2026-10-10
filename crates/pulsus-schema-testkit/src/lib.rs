//! Building a schema in a test.
//!
//! **The product does not create schema.** `schema/schema.sh` does, and this
//! crate renders the same `schema/schema.sql` through
//! `pulsus_schema::rendered_statements` and sends it over a client the caller
//! already has. It is a dev-dependency of the crates whose suites need a
//! database and is never linked into the binary.
//!
//! It renders in-process rather than shelling out to the script because the
//! suites call it from 126 places: a subprocess per call costs about 0.8 s
//! (120 separate `curl` processes against one server took 0.83 s), which is
//! roughly two minutes across the suite, while reusing the caller's
//! connection pool costs nothing.
//!
//! `tests/schema_file.rs` in `pulsus-schema` holds the script and the
//! renderer to the identical text, so what a suite builds is what a
//! deployment gets.

use pulsus_clickhouse::{ChClient, Idempotency, QuerySettings};
use pulsus_schema::{SchemaError, SchemaParams, rendered_statements};

/// Creates the whole schema: the database, every table, every routing
/// wrapper when `params.cluster` is set, and every materialized view.
///
/// **Needs a database that does not hold the schema.** The file holds only
/// `CREATE` statements, so a second application fails at the first
/// materialized view. The script drops the database first; this does
/// **not** — a suite drops it itself, by exact name, before calling this.
pub async fn run_init(client: &ChClient, params: &SchemaParams) -> Result<(), SchemaError> {
    let version = pulsus_schema::server_version(client).await?;
    pulsus_schema::check_version(&version)?;
    for stmt in rendered_statements(params) {
        client
            .execute(&stmt, &QuerySettings::new(), Idempotency::Idempotent)
            .await?;
    }
    Ok(())
}
