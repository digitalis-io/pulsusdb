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
//!
//! ## Two ways to drop
//!
//! [`drop_db`] is the primitive: one statement, called where the test says.
//! [`ScopedDb`] wraps it in a guard that drops on entry AND on scope exit,
//! so a test cannot forget the second half — which seven of the eleven
//! tests in `traces_api_live.rs` had (issue #523). New live tests should
//! take the guard.

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
///
/// Calling this is a *choice*, and a test can decline it. [`ScopedDb`]
/// removes the choice; prefer it in new tests.
pub async fn drop_db(db: &str) {
    if let Err(why) = try_drop_db(db).await {
        panic!("{why}");
    }
}

/// The fallible form both [`drop_db`] and [`ScopedDb`]'s teardown use, so
/// the entry drop and the exit drop cannot drift into issuing different
/// statements against different connections.
async fn try_drop_db(db: &str) -> Result<(), String> {
    let client = ChClient::new(conn_config("default"))
        .await
        .map_err(|e| format!("connect bootstrap client to drop test database {db}: {e}"))?;
    client
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .map_err(|e| format!("drop test database {db}: {e}"))?;
    Ok(())
}

/// A throwaway database name that drops its database on the way **in** and
/// on the way **out**.
///
/// ## Why the guard, when `drop_db` already exists (issue #523)
///
/// A bare `drop_db` call is optional, and optional cleanup gets skipped.
/// Measured at `d542869b` on `crates/pulsus-server/tests/traces_api_live.rs`:
/// of its eleven `#[test]`/`#[tokio::test]` functions, two dropped the
/// database at both ends, one dropped it only on entry as a re-run guard,
/// seven live ones never dropped it at all, and one is hermetic and has no
/// database. Command:
///
/// ```text
/// grep -nE '^#\[(tokio::)?test|drop_db\(' crates/pulsus-server/tests/traces_api_live.rs
/// ```
///
/// The cost of the omission is in [`drop_db`]'s doc comment above: a
/// second run against retained rows doubles every count.
///
/// ## What the guard buys over an entry drop alone
///
/// An entry drop makes the *next* run correct. It leaves the rows resident
/// between runs, so any other reader of that server — a second suite that
/// happens to compose the same name, an operator looking at the box —
/// sees a database that no longer belongs to a running test. The exit drop
/// closes that window, and it runs on the failing path as well, because
/// [`Drop`] runs while the test's panic unwinds.
///
/// ## Declaration order matters
///
/// Locals drop in reverse declaration order, so declare the `ScopedDb`
/// **before** the server-process guard: the child is killed first, then its
/// database goes.
///
/// ```text
/// let db = live_db::ScopedDb::fresh(pulsus_testkit::test_db("pulsus_x_it")).await;
/// let _server = spawn_ready(PORT, &db);   // dropped first
/// …                                       // then `db` -> DROP DATABASE
/// ```
///
/// It does not own the *name*: that still comes from
/// [`pulsus_testkit::test_db`], which is what carries the per-checkout
/// prefix, and what `crates/pulsus-server/tests/live_db_naming.rs` checks
/// every live suite goes through.
#[derive(Debug)]
pub struct ScopedDb {
    name: String,
}

impl ScopedDb {
    /// Drops `name` if it is there, and hands back a guard that will drop
    /// it again when it goes out of scope.
    ///
    /// Takes the composed name by value so the call site reads
    /// `ScopedDb::fresh(pulsus_testkit::test_db("…")).await` — one
    /// expression, with no intermediate binding a test could use while
    /// forgetting the guard.
    pub async fn fresh(name: String) -> Self {
        if let Err(why) = try_drop_db(&name).await {
            panic!("entry drop: {why}");
        }
        Self { name }
    }

    /// The composed database name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl std::fmt::Display for ScopedDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.name)
    }
}

impl std::ops::Deref for ScopedDb {
    type Target = str;

    fn deref(&self) -> &str {
        &self.name
    }
}

impl Drop for ScopedDb {
    fn drop(&mut self) {
        // `Drop` cannot `.await`, and `Handle::block_on` panics when it is
        // called from a runtime worker thread — which is exactly where a
        // `#[tokio::test]` body's locals are dropped. A throwaway thread
        // with its own current-thread runtime avoids both, and joining it
        // means the database is gone before the test function returns
        // rather than at some later, unordered moment.
        //
        // The clone hands an owned name to a thread that outlives the
        // borrow of `self`; `self.name` stays intact for the message below.
        let name = self.name.clone();
        let outcome = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| format!("build the teardown runtime: {e}"))
                .and_then(|rt| rt.block_on(try_drop_db(&name)))
        })
        .join();
        let why = match outcome {
            Ok(Ok(())) => return,
            Ok(Err(why)) => why,
            Err(_) => "the teardown thread panicked".to_string(),
        };
        if std::thread::panicking() {
            // Panicking inside a panic aborts the process, and the
            // assertion message the test was reporting is never printed.
            // A failed teardown must not hide the failure that caused it.
            eprintln!(
                "live_db: exit drop of {} failed while the test was already failing — {why}.                  The database is still on the server; drop it before re-running.",
                self.name
            );
        } else {
            panic!("exit drop: {why}");
        }
    }
}
