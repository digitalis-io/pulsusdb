//! Issue #623: `pulsusdb rebuild-metrics` against a live ClickHouse, run as
//! the binary an operator runs.
//!
//! Each test lands rows of three series on two UTC days through
//! `metric_landing`, so the views fill every target as a push does, then
//! replays the window into a target and reads what the target holds.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-server --test rebuild_live
//! ```

#[path = "support/live_db.rs"]
mod live_db;

use std::process::Command;
use std::sync::Arc;

use live_db::ScopedDb;
use pulsus_clickhouse::{ChClient, Idempotency, QuerySettings};
use pulsus_model::{CounterResetHint, Fingerprint, LabelSet, NativeHistogram, Span};
use pulsus_write::writer::MetricLandingRow;
use pulsus_write::{HistogramPoint, MetricPoint, SeriesRef};

const DAY_MS: i64 = 86_400_000;
const HOUR_MS: i64 = 3_600_000;

/// The targets a replay can store twice: the two sample tables. Activity
/// and the lookup fold a replayed row into the one already stored.
const SAMPLE_TARGETS: [&str; 2] = ["metric_samples", "metric_hist_samples"];

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-server/tests/rebuild_live.rs)"
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

fn histogram() -> NativeHistogram {
    NativeHistogram {
        counter_reset_hint: CounterResetHint::Unknown,
        schema: 0,
        zero_threshold: 0.0,
        zero_count: 0,
        count: 4,
        sum: 5.0,
        positive_spans: vec![Span {
            offset: 0,
            length: 3,
        }],
        negative_spans: vec![],
        positive_buckets: vec![1, 1, -1],
        negative_buckets: vec![],
        custom_values: vec![],
    }
}

/// Landing rows of three series on two whole UTC days, the two before
/// today: a float sample, a histogram sample and a kind-2 row at hours 2
/// and 9 of each day. Both days are wholly past, so every row is seeded
/// whatever the time of day the test runs; today would have none before
/// 02:00 UTC.
async fn seed_two_days(client: &ChClient, received_ms: i64) {
    seed_two_days_for(client, received_ms, "").await;
}

/// [`seed_two_days`] for one tenant (issue #635 part 4).
async fn seed_two_days_for(client: &ChClient, received_ms: i64, tenant: &str) {
    let org: Arc<str> = Arc::from(tenant);
    let today = received_ms.div_euclid(DAY_MS) * DAY_MS;
    let mut rows = Vec::new();
    for s in 1..=3i64 {
        let fp = Fingerprint::from_raw(u128::try_from(s).expect("positive"));
        let (labels, _) = LabelSet::from_normalized([("instance".to_string(), format!("i-{s}"))]);
        for day in [today - 2 * DAY_MS, today - DAY_MS] {
            for hour in [2, 9] {
                let at = day + hour * HOUR_MS + s * 1_000;
                rows.push(MetricLandingRow::float_sample(
                    &org,
                    received_ms,
                    &MetricPoint {
                        metric_name: Arc::from("rb"),
                        fingerprint: fp,
                        unix_milli: at,
                        value: 1.0,
                    },
                ));
                rows.push(MetricLandingRow::hist_sample(
                    &org,
                    received_ms,
                    &HistogramPoint {
                        metric_name: Arc::from("rb"),
                        fingerprint: fp,
                        unix_milli: at,
                        histogram: histogram(),
                    },
                ));
                rows.push(MetricLandingRow::series(
                    &org,
                    received_ms,
                    &SeriesRef {
                        metric_name: Arc::from("rb"),
                        fingerprint: fp,
                        labels: labels.clone(),
                    },
                    day + hour * HOUR_MS,
                    0,
                ));
            }
        }
    }
    client
        .insert_block("metric_landing", &rows)
        .await
        .expect("seed metric_landing");
}

/// Runs `pulsusdb rebuild-metrics` for `target` over the hour either side of
/// `received_ms`, and returns its exit status and output.
fn rebuild(db: &str, target: &str, received_ms: i64, drop: bool) -> (bool, String) {
    rebuild_tenant(db, target, received_ms, drop, None)
}

/// [`rebuild`], with `--tenant` when `tenant` names one (issue #635 part 4).
fn rebuild_tenant(
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
        .args(["rebuild-metrics", "--target", target])
        .args(["--from", &at(received_ms - HOUR_MS)])
        .args(["--to", &at(received_ms + HOUR_MS)]);
    if drop {
        command.arg("--drop-target-partitions");
    }
    if let Some(tenant) = tenant {
        command.args(["--tenant", tenant]);
    }
    let output = command.output().expect("run pulsusdb rebuild-metrics");
    (
        output.status.success(),
        format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ),
    )
}

async fn strings(client: &ChClient, sql: &str) -> Vec<String> {
    client
        .query_strings(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"))
}

/// The partitions `table` holds rows in.
async fn partitions(client: &ChClient, db: &str, table: &str) -> Vec<String> {
    strings(
        client,
        &format!("SELECT DISTINCT _partition_id AS s FROM {db}.{table} ORDER BY s"),
    )
    .await
}

async fn row_count(client: &ChClient, db: &str, table: &str) -> Vec<String> {
    strings(
        client,
        &format!("SELECT toString(count()) AS s FROM {db}.{table}"),
    )
    .await
}

/// **RB1 (issue #623): every sample target finds its partitions through
/// its own view.** Landing rows on two UTC days. A rebuild of each sample
/// table with `--drop-target-partitions` succeeds and leaves the table
/// holding the partitions and the rows the view wrote: the replay names
/// the partitions to drop by the view's projection, and a name that is not
/// a stored partition ID drops nothing, so the rows would be stored twice.
#[tokio::test]
async fn every_rebuild_target_discovers_its_partitions_through_its_view() {
    skip_unless_live!();
    let db = ScopedDb::fresh(pulsus_testkit::test_db("rebuild_partitions")).await;
    let client = client_for(db.name()).await;
    let received = now_ms();
    seed_two_days(&client, received).await;
    for target in SAMPLE_TARGETS {
        let stored = partitions(&client, db.name(), target).await;
        assert_eq!(stored.len(), 2, "{target}: rows on two days");
        let count = row_count(&client, db.name(), target).await;
        let (ok, out) = rebuild(db.name(), target, received, true);
        assert!(ok, "{target}: the rebuild failed:\n{out}");
        assert_eq!(
            partitions(&client, db.name(), target).await,
            stored,
            "{target}: the partitions the view wrote"
        );
        assert_eq!(
            row_count(&client, db.name(), target).await,
            count,
            "{target}: the rows the view wrote, once"
        );
    }
}

/// The activity and lookup rows folded to one text per key, after a merge:
/// the masks, and the first and last seen.
async fn folded(client: &ChClient, db: &str) -> (Vec<String>, Vec<String>) {
    for table in ["metric_series", "metric_labels"] {
        client
            .execute(
                &format!("OPTIMIZE TABLE {db}.{table} FINAL"),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("merge");
    }
    let activity = strings(
        client,
        &format!(
            "SELECT concat(toString(day), ' ', toString(fingerprint), ' ', metric_name, ' ', \
             toString(groupBitOr(hours))) AS s FROM {db}.metric_series \
             GROUP BY day, fingerprint, metric_name ORDER BY s"
        ),
    )
    .await;
    let lookup = strings(
        client,
        &format!(
            "SELECT concat(metric_name, ' ', toString(fingerprint), ' ', any(labels), ' ', \
             toString(min(first_seen)), ' ', toString(max(last_seen))) AS s \
             FROM {db}.metric_labels GROUP BY metric_name, fingerprint ORDER BY s"
        ),
    )
    .await;
    (activity, lookup)
}

/// **RB2 (issue #623): activity and the lookup rebuild without dropping a
/// partition.** The views write the activity and lookup rows; both tables
/// are truncated and each is rebuilt twice without
/// `--drop-target-partitions`. Merged, the masks and the first and last
/// seen are the ones the views wrote.
#[tokio::test]
async fn activity_rebuilds_without_dropping_partitions() {
    skip_unless_live!();
    let db = ScopedDb::fresh(pulsus_testkit::test_db("rebuild_activity")).await;
    let client = client_for(db.name()).await;
    let received = now_ms();
    seed_two_days(&client, received).await;
    let before = folded(&client, db.name()).await;
    assert!(
        before.0.len() >= 3 && before.1.len() == 3,
        "three series, on one or two days: {before:?}"
    );
    for table in ["metric_series", "metric_labels"] {
        client
            .execute(
                &format!("TRUNCATE TABLE {}.{table}", db.name()),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("truncate");
    }
    for run in 0..2 {
        for target in ["metric_series", "metric_labels"] {
            let (ok, out) = rebuild(db.name(), target, received, false);
            assert!(ok, "run {run}, {target}: the rebuild failed:\n{out}");
        }
    }
    assert_eq!(folded(&client, db.name()).await, before, "rebuilt twice");
}

/// **RB3 (issue #623): a drop and replay leaves each row once.** Landing
/// rows on two UTC days; each sample table is rebuilt with
/// `--drop-target-partitions`, twice. Each then holds as many rows as the
/// view wrote, and every `(fingerprint, unix_milli)` once.
#[tokio::test]
async fn drop_and_replay_leaves_each_row_once() {
    skip_unless_live!();
    let db = ScopedDb::fresh(pulsus_testkit::test_db("rebuild_replay")).await;
    let client = client_for(db.name()).await;
    let received = now_ms();
    seed_two_days(&client, received).await;
    for target in SAMPLE_TARGETS {
        let original = row_count(&client, db.name(), target).await;
        for run in 0..2 {
            let (ok, out) = rebuild(db.name(), target, received, true);
            assert!(ok, "run {run}, {target}: the rebuild failed:\n{out}");
        }
        assert_eq!(
            row_count(&client, db.name(), target).await,
            original,
            "{target}: as many rows as the view wrote"
        );
        let repeated = strings(
            &client,
            &format!(
                "SELECT toString(fingerprint) AS s FROM {}.{target} \
                 GROUP BY fingerprint, unix_milli HAVING count() > 1",
                db.name()
            ),
        )
        .await;
        assert!(
            repeated.is_empty(),
            "{target}: keys stored twice: {repeated:?}"
        );
    }
}

/// The rows of `table`, merged, one text per row.
async fn merged_rows(client: &ChClient, db: &str, table: &str, columns: &str) -> Vec<String> {
    client
        .execute(
            &format!("OPTIMIZE TABLE {db}.{table} FINAL"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("merge");
    strings(
        client,
        &format!("SELECT concat({columns}) AS s FROM {db}.{table} ORDER BY s"),
    )
    .await
}

/// **RB (issue #635): a replay into the label index leaves its rows as the
/// view wrote them.** The window is replayed into a table that already
/// holds it; merged, the replay's copies collapse into the rows already
/// there.
async fn a_replay_keeps_the_rows(db: ScopedDb, target: &str, columns: &str) {
    let client = client_for(db.name()).await;
    let received = now_ms();
    seed_two_days(&client, received).await;
    let before = merged_rows(&client, db.name(), target, columns).await;
    assert!(!before.is_empty(), "{target}: the view wrote rows");
    let (ok, out) = rebuild(db.name(), target, received, false);
    assert!(ok, "{target}: the rebuild failed:\n{out}");
    assert_eq!(
        merged_rows(&client, db.name(), target, columns).await,
        before,
        "{target}: the rows after the replay"
    );
}

#[tokio::test]
async fn the_label_index_rebuilds_to_the_rows_the_view_wrote() {
    skip_unless_live!();
    a_replay_keeps_the_rows(
        ScopedDb::fresh(pulsus_testkit::test_db("rebuild_label_index")).await,
        "metric_label_index",
        "key, '=', value, ' ', toString(fingerprint)",
    )
    .await;
}

#[tokio::test]
async fn the_label_values_rebuild_to_the_rows_the_view_wrote() {
    skip_unless_live!();
    a_replay_keeps_the_rows(
        ScopedDb::fresh(pulsus_testkit::test_db("rebuild_label_values")).await,
        "metric_label_values",
        "key, '=', value",
    )
    .await;
}

/// The seven targets, and whether a replay into each must drop first.
const ALL_TARGETS: [(&str, bool); 7] = [
    ("metric_samples", true),
    ("metric_hist_samples", true),
    ("metric_series", false),
    ("metric_metadata", false),
    ("metric_labels", false),
    ("metric_label_index", false),
    ("metric_label_values", false),
];

/// One tenant's rows of `table`, merged: their count and a content hash.
async fn tenant_digest(client: &ChClient, db: &str, table: &str, tenant: &str) -> String {
    client
        .execute(
            &format!("OPTIMIZE TABLE {db}.{table} FINAL"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("merge");
    strings(
        client,
        &format!(
            "SELECT concat(toString(count()), ' ', toString(sum(cityHash64(*)))) AS s \
             FROM {db}.{table} WHERE org_id = '{tenant}'"
        ),
    )
    .await
    .remove(0)
}

/// Deletes `tenant`'s rows of `table` whose `where_` holds.
async fn delete_rows(client: &ChClient, db: &str, table: &str, tenant: &str, where_: &str) {
    client
        .execute(
            &format!(
                "DELETE FROM {db}.{table} WHERE org_id = '{tenant}' AND {where_} \
                 SETTINGS lightweight_deletes_sync = 2"
            ),
            &QuerySettings::new(),
            Idempotency::NonIdempotent,
        )
        .await
        .expect("delete");
}

/// The descriptor rows `metric_metadata` is rebuilt from, for one tenant.
async fn seed_descriptor(client: &ChClient, received_ms: i64, tenant: &str, help: &str) {
    let row = MetricLandingRow::metadata(
        &Arc::from(tenant),
        received_ms,
        &pulsus_write::MetricMetadata {
            metric_name: Arc::from("rb"),
            metric_type: "counter".to_string(),
            help: help.to_string(),
            unit: String::new(),
            updated_ns: received_ms * 1_000_000,
        },
    );
    client
        .insert_block("metric_landing", &[row])
        .await
        .expect("seed a descriptor");
}

/// **T8 (issue #635 part 4): a rebuild with `--tenant` replays that tenant
/// alone.** Two tenants land the same rows. Before each rebuild every row
/// of tenant-a's is deleted from the target, and so is part of
/// tenant-b's. With `--tenant tenant-a` the target holds tenant-a's rows
/// as the views wrote them, and tenant-b's as they were before the
/// rebuild: its deleted rows stay deleted. Without `--tenant` both are
/// replayed, and both hold what the views wrote.
#[tokio::test]
async fn a_rebuild_with_a_tenant_replays_that_tenant_alone() {
    skip_unless_live!();
    let db = ScopedDb::fresh(pulsus_testkit::test_db("rebuild_tenant")).await;
    let client = client_for(db.name()).await;
    let received = now_ms();
    for tenant in ["tenant-a", "tenant-b"] {
        seed_two_days_for(&client, received, tenant).await;
        seed_descriptor(&client, received, tenant, tenant).await;
    }
    for (target, drop) in ALL_TARGETS {
        let views_a = tenant_digest(&client, db.name(), target, "tenant-a").await;
        let views_b = tenant_digest(&client, db.name(), target, "tenant-b").await;
        assert_ne!(views_a, "0 0", "{target}: tenant-a has rows");

        // Tenant-b loses part of its rows: one series, or its descriptor.
        let partial = if target == "metric_metadata" {
            "metric_name = 'rb'"
        } else if target == "metric_label_values" {
            "key = 'instance' AND value = 'i-1'"
        } else {
            "fingerprint = 1"
        };
        delete_rows(&client, db.name(), target, "tenant-a", "1").await;
        delete_rows(&client, db.name(), target, "tenant-b", partial).await;
        let before_b = tenant_digest(&client, db.name(), target, "tenant-b").await;
        assert_ne!(before_b, views_b, "{target}: tenant-b lost rows");

        let (ok, out) = rebuild_tenant(db.name(), target, received, drop, Some("tenant-a"));
        assert!(ok, "{target}: the rebuild with --tenant failed:\n{out}");
        assert_eq!(
            tenant_digest(&client, db.name(), target, "tenant-a").await,
            views_a,
            "{target}: tenant-a holds the rows the views wrote"
        );
        assert_eq!(
            tenant_digest(&client, db.name(), target, "tenant-b").await,
            before_b,
            "{target}: tenant-b is untouched"
        );

        let (ok, out) = rebuild_tenant(db.name(), target, received, drop, None);
        assert!(ok, "{target}: the rebuild without --tenant failed:\n{out}");
        assert_eq!(
            tenant_digest(&client, db.name(), target, "tenant-a").await,
            views_a,
            "{target}: tenant-a after a rebuild of every tenant"
        );
        assert_eq!(
            tenant_digest(&client, db.name(), target, "tenant-b").await,
            views_b,
            "{target}: tenant-b after a rebuild of every tenant"
        );
    }
}
