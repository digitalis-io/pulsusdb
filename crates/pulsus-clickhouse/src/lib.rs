//! ClickHouse client wrapper: connection pool (RAII stream leases), per-query
//! settings injection, and a retryable-vs-poison error taxonomy, bound to
//! the `clickhouse` crate (HTTP + RowBinary) — the M0 spike winner. See
//! docs/architecture.md §1.2 and docs/decisions/0001-clickhouse-client.md.
//!
//! `insert_block` is **never** auto-retried by this crate (append-only
//! exactly-once via writer batch atomicity, docs/schemas.md §8; a retried
//! partial insert duplicates rows and can permanently inflate tier
//! aggregates, docs/schemas.md §2.2). `execute` retries only when the
//! caller declares [`Idempotency::Idempotent`].

mod client;
mod config;
mod error;
mod pool;
mod settings;
mod tls;

pub use client::{ChClient, ChRow, ChRowStream, Row};
pub use config::{ChConnConfig, ChEndpoint, ChProto, ResolvedEndpoint};
pub use error::{ChError, Idempotency};
pub use pool::{ChPool, PooledConn};
pub use settings::QuerySettings;

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles() {}
}
