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
//! so a test does not have to remember the second half — which seven of
//! the eleven tests in `traces_api_live.rs` had not (issue #523). New
//! live tests should take the guard.
//!
//! **What the guard does and does not promise.** It removes the
//! ACCIDENTAL omission: a test that takes the guard cannot leave the
//! database behind by forgetting a line, and a test in an adopting file
//! cannot obtain a name without it: the type refuses a bare `String` at
//! the server spawn, and `live_db_naming`'s third rule refuses it in
//! source.
//! It does not survive a DELIBERATE one. `std::mem::forget(db)` ends the
//! guard's life without running [`Drop`], and both checks stay green:
//! measured in the #523 review round 2, `23 tests run: 23 passed` from the
//! source scan, `1 test run: 1 passed` from the live test, and one
//! database left resident afterwards. That is out of scope on purpose —
//! nobody writes `mem::forget` on a database handle by accident, and
//! machinery to defeat it would be built for the check rather than for the
//! suite.

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
/// takes the choice away from anyone who is not trying to defeat it —
/// see its own note on `std::mem::forget`, which still works. Prefer it
/// in new tests.
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

/// The retention the schema these suites build carries, in days: a hundred
/// years.
///
/// **Their fixtures are fixed instants in the past.** At the default seven
/// days the delete-TTL drops the part before the first query runs — measured
/// while writing this: `template_timezone_live`'s 2023-11-14 line landed in
/// `log_samples` and was gone one part later. The suites that cared already
/// passed `PULSUS_RETENTION_DAYS=36500` to the server, which used to create
/// the schema; the schema is built here now, so the value belongs here.
///
/// The server's own `PULSUS_RETENTION_DAYS` is untouched by this and still
/// drives the read-side query-window clamp, which is a different thing.
pub const SCHEMA_RETENTION_DAYS: u32 = 36_500;

/// Builds the schema in `db`, as `schema/schema.sh` would.
///
/// **The binary creates none**, so a suite that spawns the server must do
/// this first or `/ready` never reaches 200 — the process logs "database
/// does not exist: build it with `schema/schema.sh`" and retries. The
/// in-process renderer is used rather than the script for the reason
/// `pulsus-schema-testkit`'s own documentation gives: a subprocess per call
/// site costs about 0.8 s, and `pulsus-schema`'s
/// `the_script_renders_exactly_what_this_crate_renders` holds the two to the
/// same text.
pub async fn build_schema(db: &str) {
    build_schema_with_retention(db, SCHEMA_RETENTION_DAYS).await;
}

/// [`build_schema`] at a stated retention.
///
/// **`api_conformance` needs the default seven days**, and the reason is
/// uncomfortable but it is the behaviour that suite has always had: its route
/// assertions push fixtures stamped 2023-11-14 through the ingest routes and
/// then assert that `/api/traces/v1/search` returns an EMPTY array. What makes
/// that true is the delete-TTL dropping the part as already expired. At a
/// hundred years the fixtures survive and the assertion fails on rows the
/// suite itself wrote.
///
/// That fragility pre-dates the schema moving out of the binary — the server
/// used to build the schema from its own `PULSUS_RETENTION_DAYS`, which that
/// suite leaves at the default — and it is recorded here rather than changed,
/// because tightening the assertion is a claim about the route and not about
/// where the DDL lives.
pub async fn build_schema_with_retention(db: &str, retention_days: u32) {
    let client = ChClient::new(conn_config("default"))
        .await
        .unwrap_or_else(|e| panic!("connect bootstrap client to build {db}: {e}"));

    // **Already there means leave it alone.** The schema file holds only
    // `CREATE` statements, so applying it to a database that holds it fails
    // at the first view. Suites that spawn a second server reach this a
    // second time. A suite that wants a fresh schema drops the database
    // first; `ScopedDb` does.
    if pulsus_schema::database_exists(&client, db)
        .await
        .unwrap_or_else(|e| panic!("read whether {db} exists: {e}"))
    {
        return;
    }

    let params = pulsus_schema::RenderCtx {
        retention_days,
        ..pulsus_schema::RenderCtx::for_tests(db)
    };
    pulsus_schema_testkit::run_init(&client, &params)
        .await
        .unwrap_or_else(|e| panic!("build the schema in {db}: {e}"));
}

/// [`drop_db`] then [`build_schema`]: what a suite that spawns the server
/// needs before it spawns one.
pub async fn fresh_db(db: &str) {
    drop_db(db).await;
    build_schema(db).await;
}

/// [`build_schema`] from a synchronous context.
///
/// **Every suite that spawns the binary calls this from its own
/// `spawn_ready`**, which is sync and reached from an async test — so it
/// cannot `.await`, and `Handle::block_on` panics on a runtime worker
/// thread. A throwaway thread with its own current-thread runtime avoids
/// both, the same way [`ScopedDb`]'s `Drop` does.
///
/// Putting it at the spawn rather than beside each `drop_db` is the point: a
/// spawn with no schema is a sixty-second `/ready` timeout, and there is one
/// spawn helper per suite but several drops.
pub fn build_schema_blocking(db: &str) {
    build_schema_blocking_with_retention(db, SCHEMA_RETENTION_DAYS);
}

/// [`build_schema_blocking`] at a stated retention. See
/// [`build_schema_with_retention`] for the one suite that needs the default.
pub fn build_schema_blocking_with_retention(db: &str, retention_days: u32) {
    let name = db.to_string();
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build a current-thread runtime")
            .block_on(build_schema_with_retention(&name, retention_days));
    })
    .join()
    .expect("the schema-build thread");
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
/// ## The one way past it, stated so it is not rediscovered
///
/// `std::mem::forget(db)` ends the guard's life without running [`Drop`],
/// and the database stays. Measured in the #523 review round 2: the
/// source scan reported `23 tests run: 23 passed`, the live test
/// `1 test run: 1 passed`, and `SELECT count() FROM system.databases`
/// matching the run's prefix returned 1 afterwards. Nothing here stops it,
/// and nothing here tries to: the failure this guard exists for is a test
/// that simply does not clean up, and defeating a deliberate leak would be
/// engineering for the check instead of for the suite.
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
        // The binary creates no schema, so a suite that spawns the server
        // needs one here or `/ready` never reaches 200.
        build_schema(&name).await;
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

/// The database each spawned server writes to, by its port, so a push
/// helper that knows only the port can wait for the data it pushed
/// (issue #624, part 3d). Each suite's spawn helper registers its server.
static PUSH_TARGETS: std::sync::Mutex<Vec<(u16, String)>> = std::sync::Mutex::new(Vec::new());

/// Records that the server on `port` writes to `db`.
pub fn register_push_target(port: u16, db: &str) {
    let mut targets = PUSH_TARGETS.lock().expect("the push-target registry");
    targets.retain(|(p, _)| *p != port);
    targets.push((port, db.to_string()));
}

/// After a trace push to the server on `port` is acknowledged, waits until
/// every table its landing block feeds holds the push (issue #624, part
/// 3d).
///
/// **Why.** A trace push is acknowledged once the old path's tables hold
/// it; the landing block, which feeds `spans`, `traces`, `resources`,
/// `tag_names`, `tag_values` and the other derived tables through their
/// views, carries no waiter (`crates/pulsus-write/src/writer/trace.rs`).
/// A route that reads those tables right after the acknowledgement may
/// not see the push yet. Writes not being readable the instant they are
/// acknowledged is a known, deferred property; a test does not assume
/// otherwise.
///
/// **What it waits for.** Both, in one poll:
///
/// * every `(trace_id, span_id)` the push carried is in `spans`, so the
///   landing insert that carries them has started writing; and
/// * no insert into `trace_landing` is running in the test's database
///   (`system.processes`), so that insert has ended.
///
/// The views run inside the landing `INSERT`, and each view's target
/// commits its part on its own: measured on 26.8.21.10 with a source
/// table and two views, the second sleeping a second per row, the first
/// target held all 3 rows while the second held 0 for three seconds,
/// with the insert still listed in `system.processes`; in the first poll
/// after it left the list, both held all 3. So the pairs alone do not
/// show that the other tables the routes read hold the push; the two
/// together do, for every table the landing block feeds.
///
/// Polls every 200 ms for at most 10 s. A push whose data never arrives
/// (the product may drop it) ends the wait at the limit and the test's own
/// assertions decide; the wait never fails a test by itself.
pub fn settle_pushed_spans(port: u16, keys: &[(Vec<u8>, Vec<u8>)], ctx: &str) {
    let mut pairs: Vec<String> = keys
        .iter()
        .map(|(t, s)| format!("('{}', '{}')", hex_upper(t), hex_upper(s)))
        .collect();
    pairs.sort_unstable();
    pairs.dedup();
    if pairs.is_empty() {
        return;
    }
    let db = PUSH_TARGETS
        .lock()
        .expect("the push-target registry")
        .iter()
        .find(|(p, _)| *p == port)
        .map(|(_, d)| d.clone())
        .unwrap_or_else(|| panic!("{ctx}: no database registered for port {port}"));
    let want = pairs.len() as u64;
    // `<pairs in spans>,<landing inserts running>`.
    let sql = format!(
        "SELECT assumeNotNull(concat(toString((SELECT count() FROM (SELECT DISTINCT trace_id, span_id \
         FROM {db}.spans WHERE (hex(trace_id), hex(span_id)) IN ({})))), ',', \
         toString((SELECT count() FROM system.processes WHERE query_kind = 'Insert' \
         AND position(query, 'trace_landing') > 0 \
         AND (current_database = '{db}' OR position(query, '{db}.') > 0))))) AS s",
        pairs.join(", ")
    );
    let ctx = ctx.to_string();
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build a current-thread runtime")
            .block_on(async move {
                let client = ChClient::new(conn_config(&db))
                    .await
                    .expect("connect to the test database");
                let deadline = std::time::Instant::now() + Duration::from_secs(10);
                loop {
                    let answer = client
                        .query_strings(&sql, &QuerySettings::new())
                        .await
                        .unwrap_or_else(|e| panic!("{ctx}: {sql}: {e}"))
                        .into_iter()
                        .next()
                        .unwrap_or_default();
                    let (got, running) = answer
                        .split_once(',')
                        .and_then(|(g, r)| Some((g.parse::<u64>().ok()?, r.parse::<u64>().ok()?)))
                        .unwrap_or((0, 0));
                    if got >= want && running == 0 {
                        return;
                    }
                    if std::time::Instant::now() >= deadline {
                        eprintln!(
                            "{ctx}: after 10 s, {got} of {want} pushed spans in `spans`, \
                             {running} landing inserts running"
                        );
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            });
    })
    .join()
    .expect("the settle thread");
}

fn hex_upper(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02X}")).collect()
}
