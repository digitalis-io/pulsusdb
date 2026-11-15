//! The profile landing table, its five views and the five tables they
//! maintain, against a running ClickHouse server (issue #813).
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`, with the connection
//! `live_schema.rs` uses:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:26.8.21.10
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-schema --test live_profile_views
//! podman rm -f pulsus-ch-test
//! ```
//!
//! Each test uses its own database, dropped at the start and at the end, so
//! the tests run concurrently against one server.

use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;

/// The landing table and the five tables its views maintain, in the order
/// every count below is read.
const TABLES: [&str; 6] = [
    "profile_landing",
    "profile_samples",
    "profile_stacks",
    "profile_series",
    "profile_entries",
    "profile_locations",
];

/// The rows the fixture block puts in each of [`TABLES`]: five different
/// counts per kind, so a view that reads another kind's rows gives another
/// count.
const FIXTURE_COUNTS: [u64; 6] = [15, 5, 3, 1, 2, 4];

/// The landing insert's row ceiling, as the writer pins it.
const MAX_ROWS: u64 = 1_048_576;

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; **panics** rather than skipping when
/// the gate is absent in a live CI job, so a lost `env:` block reddens the
/// build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-schema/tests/live_profile_views.rs for setup)"
            );
            return;
        }
    };
}

fn test_config() -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: std::env::var("PULSUS_TEST_CH_DATABASE")
            .unwrap_or_else(|_| "default".to_string()),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(20),
        ..ChConnConfig::default()
    }
}

async fn exec(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::Idempotent)
        .await
        .unwrap_or_else(|e| panic!("execute failed: {e}\nSQL:\n{sql}"));
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CountRow {
    n: u64,
}

async fn count(client: &ChClient, sql: &str) -> u64 {
    let mut stream = client
        .query_stream::<CountRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("count failed: {e}\nSQL:\n{sql}"));
    stream.next().await.expect("one row").expect("decode").n
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct TextRow {
    v: String,
}

/// The one value `sql` reads, as the server writes it in text.
async fn text(client: &ChClient, sql: &str) -> String {
    let mut stream = client
        .query_stream::<TextRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("read failed: {e}\nSQL:\n{sql}"));
    stream.next().await.expect("one row").expect("decode").v
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct TenantCountRow {
    org_id: String,
    n: u64,
}

/// A fresh database `db` with the schema built in it, and a check that the
/// six profile tables are there.
async fn fresh(client: &ChClient, db: &str) {
    exec(client, &format!("DROP DATABASE IF EXISTS {db} SYNC")).await;
    run_init(client, &RenderCtx::for_tests(db, 7))
        .await
        .expect("run_init");
    let present = count(
        client,
        &format!(
            "SELECT count() AS n FROM system.tables WHERE database = '{db}' AND name IN ({})",
            TABLES.map(|t| format!("'{t}'")).join(", ")
        ),
    )
    .await;
    assert_eq!(present, 6, "{db}: the six profile tables are created");
}

/// Each of [`TABLES`]' row counts, in that order.
async fn counts(client: &ChClient, db: &str) -> Vec<(&'static str, u64)> {
    let mut out = Vec::new();
    for table in TABLES {
        let n = count(client, &format!("SELECT count() AS n FROM {db}.{table}")).await;
        out.push((table, n));
    }
    out
}

fn expected_counts() -> Vec<(&'static str, u64)> {
    TABLES.into_iter().zip(FIXTURE_COUNTS).collect()
}

/// The test's clock in milliseconds, read once.
fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_millis(),
    )
    .expect("fits i64")
}

/// The fixture block: five samples, three stacks, one series, two entries
/// and four locations, every row at the one instant `ms`.
fn fixture(db: &str, ms: i64) -> String {
    let ns = ms * 1_000_000;
    let t = "'process_cpu:cpu:nanoseconds:cpu:nanoseconds'";
    let z = "toUUID('00000000-0000-0000-0000-000000000000')";
    let p1 = "toUUID('00000000-0000-7000-8000-000000000001')";
    let p2 = "toUUID('00000000-0000-7000-8000-000000000002')";
    let rows = [
        format!("('t1',{ms},0,{t},'checkout',11,{ns},101,10,[],[],[],0,0,[],'',{p1},0,[],[])"),
        format!("('t1',{ms},0,{t},'checkout',11,{ns},102,20,[],[],[],0,0,[],'',{p1},0,[],[])"),
        format!("('t1',{ms},0,{t},'checkout',11,{ns},103,30,[],[],[],0,0,[],'',{p1},0,[],[])"),
        format!("('t1',{ms},0,{t},'checkout',11,{ns},101,40,[],[],[],0,0,[],'',{p2},0,[],[])"),
        format!("('t1',{ms},0,{t},'checkout',11,{ns},102,50,[],[],[],0,0,[],'',{p2},0,[],[])"),
        format!("('t1',{ms},1,'','',0,0,101,0,[1,2],['main','a'],[201,202],0,0,[],'',{z},0,[],[])"),
        format!("('t1',{ms},1,'','',0,0,102,0,[1,3],['main','b'],[201,203],0,0,[],'',{z},0,[],[])"),
        format!(
            "('t1',{ms},1,'','',0,0,103,0,[1,3,4],['main','b','c'],[201,203,204],0,0,[],'',{z},0,[],[])"
        ),
        format!(
            "('t1',{ms},2,{t},'checkout',11,{ns},0,0,[],[],[],0,0,[],'{{\"service_name\":\"checkout\"}}',{z},0,[],[])"
        ),
        format!(
            "('t1',{ms},3,{t},'checkout',11,{ns},0,60,[],[],[],0,0,[],'',{p1},15000000000,['k'],['v'])"
        ),
        format!(
            "('t1',{ms},3,{t},'checkout',11,{ns},0,90,[],[],[],0,0,[],'',{p2},15000000000,[],[])"
        ),
        format!("('t1',{ms},4,'','',0,0,0,0,[],[],[],201,4096,['main'],'',{z},0,[],[])"),
        format!("('t1',{ms},4,'','',0,0,0,0,[],[],[],202,4112,['a'],'',{z},0,[],[])"),
        format!("('t1',{ms},4,'','',0,0,0,0,[],[],[],203,4128,['b'],'',{z},0,[],[])"),
        format!("('t1',{ms},4,'','',0,0,0,0,[],[],[],204,4144,['c'],'',{z},0,[],[])"),
    ];
    format!(
        "INSERT INTO {db}.profile_landing (org_id, received_ms, kind, type_id, service, \
         fingerprint, timestamp_ns, stack_id, value, node_ids, names, location_ids, location_id, \
         address, line_functions, labels, profile_id, duration_ns, annotation_keys, \
         annotation_values) VALUES {}",
        rows.join(", ")
    )
}

/// One landing insert, with the settings the writer pins.
async fn push(client: &ChClient, sql: &str, token: &str) -> Result<(), pulsus_clickhouse::ChError> {
    client
        .execute(
            sql,
            &QuerySettings::landing_insert(token, MAX_ROWS),
            Idempotency::NonIdempotent,
        )
        .await
}

/// Stops merges on the five tables the views maintain, so a repeated row is
/// counted rather than merged away.
async fn stop_target_merges(client: &ChClient, db: &str) {
    for table in &TABLES[1..] {
        exec(client, &format!("SYSTEM STOP MERGES {db}.{table}")).await;
    }
}

/// **V1: one block fills each target with its own kind.** Each table holds
/// the fixture's count for its kind, and the values each view copies are
/// the fixture's.
#[tokio::test]
async fn one_block_fills_each_target_with_its_own_kind() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = &pulsus_testkit::test_db("pulsus_schema_it_profile_views_v1");
    fresh(&client, db).await;

    let ms = now_ms();
    let ns = ms * 1_000_000;
    push(&client, &fixture(db, ms), "p813-v1")
        .await
        .expect("the block commits");
    assert_eq!(counts(&client, db).await, expected_counts(), "row counts");

    let p1 = "toUUID('00000000-0000-7000-8000-000000000001')";
    let checks = [
        (
            format!("SELECT toString(sum(value)) AS v FROM {db}.profile_samples"),
            "150",
        ),
        (
            format!("SELECT toString(uniqExact(profile_id)) AS v FROM {db}.profile_samples"),
            "2",
        ),
        (
            format!("SELECT toString(groupUniqArray(org_id)) AS v FROM {db}.profile_samples"),
            "['t1']",
        ),
        (
            format!("SELECT toString(sum(total)) AS v FROM {db}.profile_entries"),
            "150",
        ),
        (
            format!("SELECT toString(sum(duration_ns)) AS v FROM {db}.profile_entries"),
            "30000000000",
        ),
        (
            format!(
                "SELECT toString(annotation_keys) AS v FROM {db}.profile_entries \
                 WHERE profile_id = {p1}"
            ),
            "['k']",
        ),
        (
            format!(
                "SELECT toString(countIf(day = toDate(fromUnixTimestamp64Nano({ns}), 'UTC'))) AS v \
                 FROM {db}.profile_series"
            ),
            "1",
        ),
        (
            format!(
                "SELECT toString(countIf(hours = bitShiftLeft(toUInt32(1), \
                 toHour(fromUnixTimestamp64Nano({ns}), 'UTC')))) AS v FROM {db}.profile_series"
            ),
            "1",
        ),
        (
            format!("SELECT labels AS v FROM {db}.profile_series"),
            "{\"service_name\":\"checkout\"}",
        ),
        (
            format!("SELECT toString(sum(length(location_ids))) AS v FROM {db}.profile_stacks"),
            "7",
        ),
        (
            format!(
                "SELECT toString(countIf(day = toDate(fromUnixTimestamp64Milli({ms}), 'UTC'))) AS v \
                 FROM {db}.profile_stacks"
            ),
            "3",
        ),
        (
            format!("SELECT toString(names) AS v FROM {db}.profile_stacks WHERE stack_id = 103"),
            "['main','b','c']",
        ),
        (
            format!("SELECT toString(sum(address)) AS v FROM {db}.profile_locations"),
            "16480",
        ),
        (
            format!(
                "SELECT toString(line_functions) AS v FROM {db}.profile_locations \
                 WHERE location_id = 204"
            ),
            "['c']",
        ),
    ];
    let mut failures = Vec::new();
    for (sql, want) in &checks {
        let got = text(&client, sql).await;
        if got != *want {
            failures.push(format!("{sql}: {got:?}, expected {want:?}"));
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");

    exec(&client, &format!("DROP DATABASE IF EXISTS {db} SYNC")).await;
}

/// **V2: a resent block stores nothing twice.** The same block twice under
/// one token, with merges stopped on every target: each table holds the
/// fixture's count, once.
#[tokio::test]
async fn a_resent_block_stores_nothing_twice() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = &pulsus_testkit::test_db("pulsus_schema_it_profile_views_v2");
    fresh(&client, db).await;
    stop_target_merges(&client, db).await;

    let block = fixture(db, now_ms());
    for attempt in 1..=2 {
        push(&client, &block, "p813-v2")
            .await
            .unwrap_or_else(|e| panic!("send {attempt} commits: {e}"));
    }
    assert_eq!(
        counts(&client, db).await,
        expected_counts(),
        "row counts after the resend"
    );

    exec(&client, &format!("DROP DATABASE IF EXISTS {db} SYNC")).await;
}

/// **V2, after a failure: a block retried after one view failed stores every
/// row once.** `profile_locations` refuses every row while a constraint is
/// on it, so the first send fails after the landing table and the other
/// targets have stored theirs. The constraint is dropped and the block sent
/// again under the same token: every table then holds the fixture's count,
/// nothing doubled and nothing missing.
#[tokio::test]
async fn a_block_retried_after_a_view_failed_stores_each_row_once() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = &pulsus_testkit::test_db("pulsus_schema_it_profile_views_v2_retry");
    fresh(&client, db).await;
    stop_target_merges(&client, db).await;
    exec(
        &client,
        &format!(
            "ALTER TABLE {db}.profile_locations ADD CONSTRAINT p813_refuse CHECK location_id = 0"
        ),
    )
    .await;

    let block = fixture(db, now_ms());
    let first = push(&client, &block, "p813-v2-retry").await;
    assert!(first.is_err(), "the first send fails: {first:?}");
    let partial = counts(&client, db).await;
    let locations = partial
        .iter()
        .find(|(t, _)| *t == "profile_locations")
        .map(|(_, n)| *n);
    let others_stored = partial
        .iter()
        .filter(|(t, _)| *t != "profile_landing" && *t != "profile_locations")
        .any(|(_, n)| *n > 0);
    assert!(
        locations == Some(0) && others_stored,
        "after the failed send, another target holds rows and profile_locations none: {partial:?}"
    );

    exec(
        &client,
        &format!("ALTER TABLE {db}.profile_locations DROP CONSTRAINT p813_refuse"),
    )
    .await;
    push(&client, &block, "p813-v2-retry")
        .await
        .expect("the retry commits");
    assert_eq!(
        counts(&client, db).await,
        expected_counts(),
        "row counts after the retry"
    );

    exec(&client, &format!("DROP DATABASE IF EXISTS {db} SYNC")).await;
}

/// **V3: two tenants with one stack keep two rows.** Three blocks, tenants
/// `t1`, `t2`, `t1`, each with the same stack and location; after
/// `OPTIMIZE … FINAL` each tenant holds one row of each.
#[tokio::test]
async fn two_tenants_with_one_stack_keep_two_rows() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = &pulsus_testkit::test_db("pulsus_schema_it_profile_views_v3");
    fresh(&client, db).await;

    let ms = now_ms();
    let z = "toUUID('00000000-0000-0000-0000-000000000000')";
    for (tenant, token) in [
        ("t1", "p813-v3-a"),
        ("t2", "p813-v3-b"),
        ("t1", "p813-v3-c"),
    ] {
        let sql = format!(
            "INSERT INTO {db}.profile_landing (org_id, received_ms, kind, stack_id, node_ids, \
             names, location_ids, location_id, address, profile_id) VALUES \
             ('{tenant}',{ms},1,9001,[1,2],['main','x'],[9101],0,0,{z}), \
             ('{tenant}',{ms},4,0,[],[],[],9101,77,{z})"
        );
        push(&client, &sql, token)
            .await
            .unwrap_or_else(|e| panic!("{token} commits: {e}"));
    }
    for table in ["profile_stacks", "profile_locations"] {
        exec(&client, &format!("OPTIMIZE TABLE {db}.{table} FINAL")).await;
    }

    for (table, filter) in [
        ("profile_stacks", "stack_id = 9001"),
        ("profile_locations", "location_id = 9101"),
    ] {
        let sql = format!(
            "SELECT toString(org_id) AS org_id, count() AS n FROM {db}.{table} \
             WHERE {filter} GROUP BY org_id ORDER BY org_id"
        );
        let mut stream = client
            .query_stream::<TenantCountRow>(&sql, &QuerySettings::new())
            .await
            .unwrap_or_else(|e| panic!("read {table}: {e}"));
        let mut got = Vec::new();
        while let Some(row) = stream.next().await {
            got.push(row.expect("decode"));
        }
        let want = vec![
            TenantCountRow {
                org_id: "t1".to_string(),
                n: 1,
            },
            TenantCountRow {
                org_id: "t2".to_string(),
                n: 1,
            },
        ];
        assert_eq!(got, want, "{table}: one row per tenant");
    }

    exec(&client, &format!("DROP DATABASE IF EXISTS {db} SYNC")).await;
}
