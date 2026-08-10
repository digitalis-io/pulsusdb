//! The one place this crate's environment-gated suites reach ClickHouse
//! to prepare their throwaway database.
//!
//! Included by each live suite via
//! `#[path = "support/live_db.rs"] mod live_db;` — a `tests/`
//! subdirectory, so cargo never builds this file as its own test binary
//! (same layout as `support/manifest.rs` and `support/source_scan.rs`).
//!
//! ## Why this file exists
//!
//! `drop_db` was copy-pasted into eleven suites in this crate, in five
//! slightly different spellings (`drop_db`, `drop_database`, and four
//! open-coded `DROP DATABASE IF EXISTS` sites in `prom_api_live.rs`),
//! each re-deriving the same two decisions: connect through the built-in
//! `default` database, because the target may not exist yet, and read the
//! server's address from `PULSUS_TEST_CH_HOST`/`PULSUS_TEST_CH_HTTP_PORT`.
//! A second hand-rolled copy of a shared decision is a defect this repo
//! has already paid for (issue #419 extracted the source-scan lexer for
//! the same reason).
//!
//! ## What it does not own
//!
//! The database *name*. That comes from [`pulsus_testkit::test_db`], which
//! every crate's live suites share — it is what lets several checkouts run
//! the same suite against one ClickHouse server. This module only takes a
//! name and drops it.

#![allow(dead_code)]

use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};

/// The ClickHouse host the live suites talk to. `localhost` unless
/// `PULSUS_TEST_CH_HOST` says otherwise (the CI job sets neither, and the
/// container publishes on loopback).
pub fn ch_host() -> String {
    std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string())
}

/// The ClickHouse HTTP port, `PULSUS_TEST_CH_HTTP_PORT` or the project's
/// 19123 convention.
pub fn ch_http_port() -> u16 {
    std::env::var("PULSUS_TEST_CH_HTTP_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(19123)
}

/// A small HTTP connection to `database` on the live server.
///
/// Deliberately modest: every caller here issues one or two DDL statements
/// and drops the client, so a wide pool would only hold connections open
/// while a suite's real work waits on them.
pub fn conn_config(database: &str) -> ChConnConfig {
    ChConnConfig {
        server: ch_host(),
        http_port: ch_http_port(),
        database: database.to_string(),
        proto: ChProto::Http,
        pool_size: 2,
        query_timeout: Duration::from_secs(30),
        ..ChConnConfig::default()
    }
}

/// `DROP DATABASE IF EXISTS db`, issued through ClickHouse's built-in
/// `default` database because `db` itself may not exist yet.
///
/// Load-bearing for exact-count assertions, not merely tidy: `log_samples`
/// is a plain `MergeTree`, so a re-run against a server that still holds
/// the previous run's rows for the same database name silently doubles
/// every count a byte-exact golden depends on.
pub async fn drop_db(db: &str) {
    let client = ChClient::new(conn_config("default"))
        .await
        .expect("connect bootstrap client to drop the test database");
    client
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
}
