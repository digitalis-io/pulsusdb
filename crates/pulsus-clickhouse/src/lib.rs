//! ClickHouse client wrapper: connection pool (RAII stream leases), per-query
//! settings injection, and a retryable-vs-poison error taxonomy, bound to
//! the `clickhouse` crate (HTTP + RowBinary) — the M0 spike winner. See
//! docs/architecture.md §1.2 and docs/decisions/0001-clickhouse-client.md.
//!
//! `insert_block` is **never** auto-retried by this crate: a retried partial
//! insert duplicates rows and can permanently inflate tier aggregates
//! (docs/schemas.md §2.2), and nothing at this layer can tell a repeat from
//! a first delivery. A *client's* retry is suppressed one layer up, by
//! `pulsus-write`'s per-writer push index inside
//! `PULSUS_INGEST_DEDUP_WINDOW` (issue #494). `execute` retries only when
//! the caller declares [`Idempotency::Idempotent`].

mod client;
mod config;
mod error;
mod pool;
mod settings;
mod tls;

pub use client::{ChClient, ChRow, ChRowStream, Row};
pub use config::{ChConnConfig, ChEndpoint, ChProto, ConsistencyConfig, ResolvedEndpoint};
pub use error::{ChError, Idempotency};
pub use pool::{ChPool, PooledConn, spawn_reprobe_loop};
pub use settings::QuerySettings;

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles() {}
}
