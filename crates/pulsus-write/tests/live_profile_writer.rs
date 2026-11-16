//! Live end-to-end test: [`ProfileWriter`] against a ClickHouse server — one
//! profiles push is one insert of one block into `profile_landing`, and the
//! five tables the views maintain are what this file reads back. Everything
//! else about the profile writer is asserted over the block it hands a mock
//! (`src/writer/profile.rs`, `tests/a494_profiles.rs`).
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`, as `live_log_writer.rs` is:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!     cargo test -p pulsus-write --test live_profile_writer
//! ```

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_config::WriterConfig;
use pulsus_profiles_wire::google_v1::{Function, Line, Location, Profile, Sample, ValueType};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;
use pulsus_write::writer::ProfileWriter;
use pulsus_write::{ProfileInput, ProfileSink, PushHeaders, parse_profiles};

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; panics rather than skipping when
/// the gate is absent in a live CI job (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

fn test_config(database: &str) -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: database.to_string(),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(20),
        ..ChConnConfig::default()
    }
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-write/tests/live_profile_writer.rs for setup)"
            );
            return;
        }
    };
}

async fn drop_database(client: &ChClient, db: &str) {
    client
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CountRow {
    n: u64,
}

async fn count(client: &ChClient, sql: &str) -> u64 {
    let mut stream = client
        .query_stream::<CountRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\nSQL:\n{sql}"));
    stream
        .next()
        .await
        .expect("one row")
        .expect("decode CountRow")
        .n
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct TextRow {
    s: String,
}

async fn texts(client: &ChClient, sql: &str) -> Vec<String> {
    let mut stream = client
        .query_stream::<TextRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\nSQL:\n{sql}"));
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode TextRow").s);
    }
    out
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, Copy)]
struct LogCounts {
    ended: u64,
    pending: u64,
}

/// `live_log_writer.rs`'s: waits until every statement `filter` selects in
/// `system.query_log` has its ending row, and at least `at_least` have
/// ended. `filter` must not name `type`. Fails after 60 s.
async fn settle_query_log(client: &ChClient, filter: &str, at_least: u64) {
    let sql = format!(
        "SELECT toUInt64(countIf(e)) AS ended, toUInt64(countIf(s AND NOT e)) AS pending \
         FROM (SELECT query_id, max(type = 'QueryStart') AS s, \
                 max(type IN ('QueryFinish', 'ExceptionWhileProcessing', \
                              'ExceptionBeforeStart')) AS e \
               FROM system.query_log \
               WHERE ({filter}) AND query NOT LIKE '%system.query_log%' \
               GROUP BY query_id, query_start_time_microseconds)"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        client
            .execute(
                "SYSTEM FLUSH LOGS",
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("SYSTEM FLUSH LOGS");
        let mut stream = client
            .query_stream::<LogCounts>(&sql, &QuerySettings::new())
            .await
            .unwrap_or_else(|e| panic!("query failed: {e}\nSQL:\n{sql}"));
        let counts = stream
            .next()
            .await
            .expect("one row")
            .expect("decode LogCounts");
        drop(stream);
        if counts.ended >= at_least && counts.pending == 0 {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "system.query_log did not settle within 60 s for {filter}: {counts:?}, \
             at least {at_least} expected"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Part A §0's profile: two sample types, locations `main`, `a`, `b`, stacks
/// `main→a` and `main→b`.
fn example_profile(time_nanos: i64) -> Profile {
    let st = [
        "",
        "samples",
        "count",
        "cpu",
        "nanoseconds",
        "main",
        "a",
        "b",
        "main.go",
    ];
    Profile {
        sample_type: vec![
            ValueType { r#type: 1, unit: 2 },
            ValueType { r#type: 3, unit: 4 },
        ],
        sample: vec![
            Sample {
                location_id: vec![2, 1],
                value: vec![3, 30_000_000],
                label: Vec::new(),
            },
            Sample {
                location_id: vec![3, 1],
                value: vec![1, 10_000_000],
                label: Vec::new(),
            },
        ],
        function: (1..=3u64)
            .map(|id| Function {
                id,
                name: 4 + id as i64,
                system_name: 0,
                filename: 8,
                start_line: 1,
            })
            .collect(),
        location: (1..=3u64)
            .map(|id| Location {
                id,
                mapping_id: 0,
                address: 0,
                line: vec![Line {
                    function_id: id,
                    line: 10,
                }],
                is_folded: false,
            })
            .collect(),
        string_table: st.iter().map(|s| (*s).to_owned()).collect(),
        time_nanos,
        duration_nanos: 10_000_000_000,
        period_type: Some(ValueType { r#type: 3, unit: 4 }),
        period: 10_000_000,
        ..Profile::default()
    }
}

/// **L1.** One push is one landing insert, which the server accepts, and the
/// five views fill every profile table off it: samples 4, entries 2, series
/// 2, stacks 2, locations 3. The minted id reads back as the version-7 UUID
/// of the push's receive time, on the entries and the samples alike.
#[tokio::test]
async fn one_push_is_one_landing_insert_filling_every_table() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_write_it_profiles");
    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    drop_database(&bootstrap, &db).await;
    run_init(&bootstrap, &RenderCtx::for_tests(&db, 7))
        .await
        .expect("run_init");
    let client = Arc::new(
        ChClient::new(test_config(&db))
            .await
            .expect("connect (target db)"),
    );
    let cfg = WriterConfig {
        landing_batch_ms: 0,
        ..WriterConfig::default()
    };
    let writer = ProfileWriter::new(client.clone(), &cfg);

    // This run's statements only: the log keeps earlier runs' rows.
    let start = count(
        &client,
        "SELECT toUInt64(toUnixTimestamp64Micro(now64(6))) AS n",
    )
    .await;
    let now_ms = i64::try_from(start / 1_000).expect("a representable stamp");
    let landing = parse_profiles(
        vec![ProfileInput {
            labels: vec![
                ("__name__".to_owned(), "process_cpu".to_owned()),
                ("service_name".to_owned(), "checkout".to_owned()),
            ],
            profile: example_profile(now_ms * 1_000_000),
            decompressed_bytes: 1_000,
            id: None,
            annotations: Vec::new(),
            language: None,
        }],
        now_ms,
    )
    .expect("the push is accepted");
    let t1 =
        pulsus_model::Tenant::from_header(Some(&axum::http::HeaderValue::from_static("t1")), false)
            .expect("a valid tenant");
    writer
        .admit_flush(&t1, landing, PushHeaders::default())
        .expect("the push is admitted")
        .await
        .expect("the landing insert commits");

    let filter = format!(
        "current_database = '{db}' AND query_kind = 'Insert' \
         AND startsWith(query, 'INSERT INTO `profile_landing`') \
         AND query_start_time_microseconds >= fromUnixTimestamp64Micro(toInt64({start}))"
    );
    settle_query_log(&client, &filter, 1).await;
    assert_eq!(
        count(
            &client,
            &format!(
                "SELECT count() AS n FROM system.query_log WHERE type = 'QueryFinish' AND {filter}"
            )
        )
        .await,
        1,
        "one landing insert"
    );

    for (table, want) in [
        ("profile_samples", 4u64),
        ("profile_entries", 2),
        ("profile_series", 2),
        ("profile_stacks", 2),
        ("profile_locations", 3),
    ] {
        assert_eq!(
            count(&client, &format!("SELECT count() AS n FROM {db}.{table}")).await,
            want,
            "{table}"
        );
    }

    let tenants = texts(
        &client,
        &format!("SELECT toString(groupUniqArray(org_id)) AS s FROM {db}.profile_landing"),
    )
    .await;
    assert_eq!(tenants, vec!["['t1']".to_string()], "every row is t1's");

    // The id the writer minted: a version-7 UUID whose first 48 bits are the
    // block's receive time, so a pair written in the wrong order reads back
    // as some other text.
    let received = count(
        &client,
        &format!("SELECT toUInt64(any(received_ms)) AS n FROM {db}.profile_landing"),
    )
    .await;
    let minted = format!(
        "{:08x}-{:04x}-7",
        (received >> 16) & 0xFFFF_FFFF,
        received & 0xFFFF
    );
    let entry_ids = texts(
        &client,
        &format!("SELECT DISTINCT toString(profile_id) AS s FROM {db}.profile_entries"),
    )
    .await;
    assert_eq!(entry_ids.len(), 1, "one input, one id: {entry_ids:?}");
    assert!(
        entry_ids[0].starts_with(&minted),
        "the entries' id {} is the version-7 UUID of receive time {received} ({minted}…)",
        entry_ids[0]
    );
    let sample_ids = texts(
        &client,
        &format!("SELECT DISTINCT toString(profile_id) AS s FROM {db}.profile_samples"),
    )
    .await;
    assert_eq!(sample_ids, entry_ids, "every sample carries its entry's id");

    writer.shutdown(Duration::from_secs(5)).await;
    drop_database(&bootstrap, &db).await;
}
