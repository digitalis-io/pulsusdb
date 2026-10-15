//! Issue #653: `pulsusdb rebuild-logs` against a live ClickHouse, run as the
//! binary an operator runs.
//!
//! Each test lands two tenants' rows on one fingerprint and two UTC days
//! through `log_landing`, so the five views fill every target as a push
//! does, then replays the window into a target and reads what each tenant
//! holds there.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-server --test rebuild_logs_live
//! ```

#[path = "support/live_db.rs"]
mod live_db;

use std::process::Command;

use live_db::ScopedDb;
use pulsus_clickhouse::{ChClient, Idempotency, QuerySettings};

const DAY_MS: i64 = 86_400_000;
const HOUR_MS: i64 = 3_600_000;
const MS_NS: i64 = 1_000_000;

/// The five targets, whether a replay needs `--drop-target-partitions`,
/// and whether the target collapses, so a snapshot reads it `FINAL`.
const TARGETS: [(&str, bool, bool); 5] = [
    ("log_samples", true, false),
    ("log_metrics_5s", true, true),
    ("log_patterns", true, true),
    ("log_streams", false, true),
    ("log_streams_idx", false, true),
];

/// The two targets a replay folds into, which `rebuild-logs` never deletes
/// from; the only ones a test stops merges on.
const FOLDING: [&str; 2] = ["log_streams", "log_streams_idx"];

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-server/tests/rebuild_logs_live.rs)"
            );
            return;
        }
    };
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

async fn client_for(db: &str) -> ChClient {
    ChClient::new(live_db::conn_config(db))
        .await
        .expect("connect to the live ClickHouse")
}

async fn exec(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::NonIdempotent)
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
}

async fn strings(client: &ChClient, sql: &str) -> Vec<String> {
    client
        .query_strings(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"))
}

/// Landing rows of `tenant-q` and `tenant-o` on fingerprint 7, on the two
/// whole UTC days before today: lines at hours 2 and 9, a stream
/// registration and a pattern aggregate on each day.
async fn seed(client: &ChClient, received_ms: i64) {
    let today = received_ms.div_euclid(DAY_MS) * DAY_MS;
    let mut rows = Vec::new();
    for org in ["tenant-q", "tenant-o"] {
        for day in [today - 2 * DAY_MS, today - DAY_MS] {
            let day_ns = day * MS_NS;
            for hour in [2, 9] {
                let at = (day + hour * HOUR_MS) * MS_NS;
                rows.push(format!(
                    "('{org}', {received_ms}, 0, 'rb', 7, {at}, '{org} {day} {hour}', \
                     toDate(0), '', 0, '', 0)"
                ));
            }
            rows.push(format!(
                "('{org}', {received_ms}, 1, 'rb', 7, 0, '', \
                 toStartOfMonth(fromUnixTimestamp64Nano(toInt64({day_ns}))), \
                 '{{\"app\":\"rb\",\"service_name\":\"rb\"}}', {day_ns}, '', 0)"
            ));
            rows.push(format!(
                "('{org}', {received_ms}, 2, '', 7, {}, '', toDate(0), '', 0, '{org} <_>', 2)",
                day_ns + 2 * HOUR_MS * MS_NS
            ));
        }
    }
    exec(
        client,
        &format!(
            "INSERT INTO log_landing (org_id, received_ms, kind, service, fingerprint, \
             timestamp_ns, body, month, labels, updated_ns, pattern, pattern_count) VALUES {}",
            rows.join(", ")
        ),
    )
    .await;
}

/// One tenant's rows of `table`: their count and a content hash, `FINAL`
/// where the table collapses.
async fn digest(client: &ChClient, table: &str, collapsing: bool, tenant: &str) -> String {
    let fin = if collapsing { " FINAL" } else { "" };
    strings(
        client,
        &format!(
            "SELECT concat(toString(count()), ' ', toString(sum(cityHash64(*)))) AS s \
             FROM {table}{fin} WHERE org_id = '{tenant}'"
        ),
    )
    .await
    .remove(0)
}

/// `tenant-o`'s row count in `table`, without `FINAL`.
async fn raw_count(client: &ChClient, table: &str) -> String {
    strings(
        client,
        &format!("SELECT toString(count()) AS s FROM {table} WHERE org_id = 'tenant-o'"),
    )
    .await
    .remove(0)
}

/// One `tenant-o` row of `table` the landing table does not hold, on the
/// earlier of the two days.
fn extra_row(table: &str, received_ms: i64) -> String {
    let today = received_ms.div_euclid(DAY_MS) * DAY_MS;
    let at = (today - 2 * DAY_MS + 5 * HOUR_MS) * MS_NS;
    match table {
        "log_samples" => format!(
            "INSERT INTO log_samples (org_id, service, fingerprint, timestamp_ns, body) \
             VALUES ('tenant-o', 'rb', 7, {at}, 'extra')"
        ),
        "log_metrics_5s" => format!(
            "INSERT INTO log_metrics_5s (org_id, fingerprint, bucket_ns, count, bytes) \
             VALUES ('tenant-o', 7, {at}, 1, 5)"
        ),
        "log_patterns" => format!(
            "INSERT INTO log_patterns (org_id, fingerprint, bucket_ns, pattern, count) \
             VALUES ('tenant-o', 7, {at}, 'extra', 1)"
        ),
        "log_streams" => format!(
            "INSERT INTO log_streams (org_id, month, fingerprint, service, labels, updated_ns) \
             VALUES ('tenant-o', toStartOfMonth(fromUnixTimestamp64Nano(toInt64({at}))), 9, \
             'extra', '{{\"app\":\"extra\"}}', {at})"
        ),
        _ => format!(
            "INSERT INTO log_streams_idx (org_id, month, key, val, fingerprint) \
             VALUES ('tenant-o', toStartOfMonth(fromUnixTimestamp64Nano(toInt64({at}))), \
             'extra', 'x', 7)"
        ),
    }
}

/// Runs `pulsusdb rebuild-logs` for `target` over the hour either side of
/// `received_ms`, and returns its exit status and output.
fn rebuild(
    db: &str,
    target: &str,
    received_ms: i64,
    drop: bool,
    tenant: Option<&str>,
) -> (bool, String) {
    let at = |ms: i64| {
        chrono::DateTime::from_timestamp_millis(ms)
            .expect("an instant")
            .to_rfc3339()
    };
    let mut command = Command::new(env!("CARGO_BIN_EXE_pulsusdb"));
    command
        .env("CLICKHOUSE_SERVER", live_db::ch_host())
        .env("CLICKHOUSE_HTTP_PORT", live_db::ch_http_port().to_string())
        .env("CLICKHOUSE_DB", db)
        .args(["rebuild-logs", "--target", target])
        .args(["--from", &at(received_ms - HOUR_MS)])
        .args(["--to", &at(received_ms + HOUR_MS)]);
    if drop {
        command.arg("--drop-target-partitions");
    }
    if let Some(tenant) = tenant {
        command.args(["--tenant", tenant]);
    }
    let output = command.output().expect("run pulsusdb rebuild-logs");
    (
        output.status.success(),
        format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ),
    )
}

/// One target's state around a rebuild: each tenant's snapshot as the
/// views wrote it, and `tenant-o`'s with the extra row.
struct Prepared {
    q_views: String,
    o_views: String,
    o_with_extra: String,
}

/// T-R1's setup for one target: snapshot both tenants, delete `tenant-q`'s
/// rows, add the extra `tenant-o` row.
async fn prepare(client: &ChClient, table: &str, collapsing: bool, received: i64) -> Prepared {
    let q_views = digest(client, table, collapsing, "tenant-q").await;
    let o_views = digest(client, table, collapsing, "tenant-o").await;
    assert_ne!(q_views, "0 0", "{table}: tenant-q has rows");
    exec(
        client,
        &format!(
            "DELETE FROM {table} WHERE org_id = 'tenant-q' SETTINGS lightweight_deletes_sync = 2"
        ),
    )
    .await;
    assert_eq!(
        digest(client, table, collapsing, "tenant-q").await,
        "0 0",
        "{table}: tenant-q's rows are deleted"
    );
    exec(client, &extra_row(table, received)).await;
    let o_with_extra = digest(client, table, collapsing, "tenant-o").await;
    assert_ne!(o_with_extra, o_views, "{table}: the extra row is stored");
    Prepared {
        q_views,
        o_views,
        o_with_extra,
    }
}

/// **T-R1 (issue #653): a tenant rebuild restores its rows and leaves the
/// others.** For each target: `tenant-q`'s rows deleted, one extra
/// `tenant-o` row the landing table does not hold, then `rebuild-logs
/// --tenant tenant-q`. `tenant-q` holds what the views wrote; `tenant-o`
/// holds what it held, extra row included. On the two folding targets,
/// merges are stopped and `tenant-o`'s count is read without `FINAL`: a
/// replay that wrote `tenant-o`'s rows again would raise it, and `FINAL`
/// would hide that.
#[tokio::test]
async fn a_tenant_rebuild_restores_its_rows_and_leaves_the_others() {
    skip_unless_live!();
    let db = ScopedDb::fresh(pulsus_testkit::test_db("rebuild_logs_tenant")).await;
    let client = client_for(db.name()).await;
    let received = now_ms();
    seed(&client, received).await;
    for (table, drop, collapsing) in TARGETS {
        let p = prepare(&client, table, collapsing, received).await;
        let folding = FOLDING.contains(&table);
        let o_raw = if folding {
            exec(&client, &format!("SYSTEM STOP MERGES {table}")).await;
            Some(raw_count(&client, table).await)
        } else {
            None
        };

        let (ok, out) = rebuild(db.name(), table, received, drop, Some("tenant-q"));
        let q_after = digest(&client, table, collapsing, "tenant-q").await;
        let o_after = digest(&client, table, collapsing, "tenant-o").await;
        let o_raw_after = if folding {
            Some(raw_count(&client, table).await)
        } else {
            None
        };
        if folding {
            exec(&client, &format!("SYSTEM START MERGES {table}")).await;
        }

        assert!(ok, "{table}: the rebuild with --tenant failed:\n{out}");
        assert_eq!(
            q_after, p.q_views,
            "{table}: tenant-q holds what the views wrote"
        );
        assert_eq!(
            o_after, p.o_with_extra,
            "{table}: tenant-o holds what it held, the extra row included"
        );
        assert_eq!(
            o_raw_after, o_raw,
            "{table}: tenant-o's rows without FINAL are unchanged"
        );
    }
}

/// **T-R2 (issue #653): a doubling target is refused without the flag.**
/// The three targets a replay would add to twice: exit 1, a message naming
/// the target, and the target unchanged.
#[tokio::test]
async fn a_doubling_target_is_refused_without_the_flag() {
    skip_unless_live!();
    let db = ScopedDb::fresh(pulsus_testkit::test_db("rebuild_logs_refused")).await;
    let client = client_for(db.name()).await;
    let received = now_ms();
    seed(&client, received).await;
    for (table, drop, collapsing) in TARGETS {
        if !drop {
            continue;
        }
        let before = (
            digest(&client, table, collapsing, "tenant-q").await,
            digest(&client, table, collapsing, "tenant-o").await,
        );
        for tenant in [None, Some("tenant-q")] {
            let (ok, out) = rebuild(db.name(), table, received, false, tenant);
            assert!(!ok, "{table}, tenant {tenant:?}: refused:\n{out}");
            assert!(
                out.contains(table),
                "{table}, tenant {tenant:?}: the message names the target:\n{out}"
            );
        }
        let after = (
            digest(&client, table, collapsing, "tenant-q").await,
            digest(&client, table, collapsing, "tenant-o").await,
        );
        assert_eq!(after, before, "{table}: unchanged");
    }
}

/// **T-R3 (issue #653): a full rebuild replays every tenant.** T-R1's
/// setup, then `--drop-target-partitions` and no `--tenant`: both tenants
/// hold what the views wrote; the extra row is gone from the three targets
/// whose partitions are dropped and kept in the two that fold.
#[tokio::test]
async fn a_full_rebuild_replays_every_tenant() {
    skip_unless_live!();
    let db = ScopedDb::fresh(pulsus_testkit::test_db("rebuild_logs_full")).await;
    let client = client_for(db.name()).await;
    let received = now_ms();
    seed(&client, received).await;
    for (table, drop, collapsing) in TARGETS {
        let p = prepare(&client, table, collapsing, received).await;
        let (ok, out) = rebuild(db.name(), table, received, true, None);
        assert!(ok, "{table}: the full rebuild failed:\n{out}");
        assert_eq!(
            digest(&client, table, collapsing, "tenant-q").await,
            p.q_views,
            "{table}: tenant-q holds what the views wrote"
        );
        let want_o = if drop { &p.o_views } else { &p.o_with_extra };
        assert_eq!(
            &digest(&client, table, collapsing, "tenant-o").await,
            want_o,
            "{table}: tenant-o after a rebuild of every tenant"
        );
    }
}

/// **T-R4 (issue #653): an invalid tenant is refused** by the header's own
/// rule, before anything is read or written.
#[tokio::test]
async fn an_invalid_tenant_is_refused() {
    skip_unless_live!();
    let db = ScopedDb::fresh(pulsus_testkit::test_db("rebuild_logs_invalid")).await;
    let received = now_ms();
    let (ok, out) = rebuild(db.name(), "log_samples", received, true, Some("a b"));
    assert!(!ok, "refused:\n{out}");
    assert!(
        out.contains("invalid X-Scope-OrgID"),
        "the header's rule names the refusal:\n{out}"
    );
}
