//! `cargo run -p xtask -- bench logql-metadata-filter` (issue #544) — the
//! measurement behind the structured-metadata label filter's figures.
//!
//! # What it measures, and why it is a script rather than a paragraph
//!
//! The filter's whole value is the statement count and the bytes on the
//! metered hop. Both are read out of `system.query_log`, which means a
//! figure can come from a **repeated** statement served partly from
//! ClickHouse's query-condition cache — on by default since 25.4 — and a
//! cached read can report 8,192 rows where a first execution reports
//! 3,000,000. A figure taken that way would be wrong by three orders of
//! magnitude and would look perfectly ordinary in a table.
//!
//! So every measured statement here is issued **once**, under a
//! `log_comment` no earlier statement used, **against a predicate literal
//! no earlier statement used**, and the uncached read is asserted to have
//! examined more than a million rows. A second execution fails that check
//! rather than entering a document.
//!
//! # The seven settings every figure carries
//!
//! Four are what our reader SENDS, so `system.query_log` records them per
//! statement and they are evidence: `max_query_size`, `max_bytes_to_read`,
//! `max_execution_time`, `max_memory_usage`. Three are server defaults in
//! force that the reader does not send, so the statement log does not
//! record them and they have to be read off `system.settings`:
//! `max_block_size`, `max_threads`, `use_query_condition_cache`. Printing
//! the second three beside the figures is what lets someone else
//! reproduce them.

use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_logql::parse;
use pulsus_read::logql::{Direction, QueryParams, QuerySpec};
use pulsus_read::{EngineConfig, LogQlEngine};
use pulsus_schema::{RenderCtx, run_init};

use super::{BenchArgs, parse_http_url};

/// The corpus the design record's figures were taken on.
const ROWS: u64 = 3_000_000;
const FP: u64 = 18_374_000_000_000_000_777;
const SERVICE: &str = "checkout";

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedRow {
    service: String,
    fingerprint: u64,
    timestamp_ns: i64,
    severity: i8,
    body: String,
    structured_metadata: String,
}

/// One route's totals, summed over every statement it issued. **A
/// scenario-local struct**: the shared `QueryLogTotals` is byte-frozen by
/// the committed evidence JSONs and must not grow a field.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, Default)]
struct RouteTotals {
    statements: u64,
    read_rows: u64,
    read_bytes: u64,
    result_bytes: u64,
    query_duration_ms: u64,
}

/// The four settings our reader sends, as `system.query_log` recorded
/// them for one statement.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, Default)]
struct SentSettings {
    max_query_size: String,
    max_bytes_to_read: String,
    max_execution_time: String,
    max_memory_usage: String,
}

/// One server default in force, read off `system.settings`.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, Default)]
struct ServerSetting {
    name: String,
    value: String,
    changed: u8,
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// A 32-character lowercase-hexadecimal identifier, deterministic in `i`.
fn trace_id(i: u64) -> String {
    format!("{:016x}{:016x}", splitmix64(i), splitmix64(i ^ 0x5bf0_3635))
}

pub async fn run(args: BenchArgs) -> anyhow::Result<()> {
    let (server, http_port) = parse_http_url(&args.http_url)?;
    let admin_cfg = ChConnConfig {
        server,
        http_port,
        database: "default".to_string(),
        user: args.user.clone(),
        password: args.password.clone(),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(1800),
        ..ChConnConfig::default()
    };
    let admin = ChClient::new(admin_cfg.clone()).await?;
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {}", args.database),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await?;
    let schema = RenderCtx {
        db: args.database.clone(),
        cluster: None,
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
    };
    run_init(&admin, &schema).await?;
    let mut data_cfg = admin_cfg.clone();
    data_cfg.database = args.database.clone();
    let client = ChClient::new(data_cfg.clone()).await?;

    let ts = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos(),
    )? - (ROWS as i64) * 1_000_000;
    eprintln!("=== seeding {ROWS} rows on one stream ===");
    client
        .execute(
            &format!(
                "INSERT INTO {}.log_streams (month, fingerprint, service, labels, updated_ns) \
                 VALUES (toStartOfMonth(fromUnixTimestamp64Nano(toInt64({ts}))), {FP}, \
                 '{SERVICE}', '{{\"service_name\":\"{SERVICE}\"}}', 0)",
                args.database
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await?;
    let mut written = 0u64;
    while written < ROWS {
        let batch: Vec<SeedRow> = (written..(written + 100_000).min(ROWS))
            .map(|i| SeedRow {
                service: SERVICE.to_string(),
                fingerprint: FP,
                timestamp_ns: ts + (i as i64) * 1_000_000,
                severity: 0,
                body: format!("GET /api/v1/orders/{i} status=200"),
                structured_metadata: format!(r#"{{"trace_id":"{}"}}"#, trace_id(i)),
            })
            .collect();
        written += batch.len() as u64;
        client.insert_block("log_samples", &batch).await?;
    }
    eprintln!("=== seeded {written} rows ===");

    // The three server defaults in force. Read once: they are properties
    // of the server, not of a statement, and `system.query_log` does not
    // record a setting the client did not send.
    let defaults = server_defaults(&admin).await?;
    println!("server defaults in force (read from system.settings):");
    for s in &defaults {
        println!("  {:<26} {:<12} changed={}", s.name, s.value, s.changed);
    }

    let engine = LogQlEngine::new(
        ChClient::new(data_cfg).await?,
        EngineConfig {
            read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
            db: args.database.clone(),
            streams_idx: "log_streams_idx".to_string(),
            streams: "log_streams".to_string(),
            samples: "log_samples".to_string(),
            rollup_table: "log_metrics_5s".to_string(),
            patterns_table: "log_patterns".to_string(),
            rollup_res_ns: 5_000_000_000,
            scan_budget_bytes: 500 * 1024 * 1024 * 1024,
            max_streams: 100_000,
            pipeline_scan_factor: 10,
            distributed: false,
        },
    );
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: ts - 1_000_000_000,
            end_ns: ts + (ROWS as i64) * 1_000_000 + 1_000_000_000,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };

    // **Each route gets its own predicate literal.** The equality route
    // looks for the row at index 7; the page-loop route looks for the row
    // at index 11 under an operator the metadata cell refuses. No literal
    // is issued twice, so no figure below can be a condition-cache hit.
    let lowered_value = trace_id(7);
    let paged_value = trace_id(11);
    let routes = [
        (
            "one statement",
            format!(r#"{{service_name="{SERVICE}"}} | trace_id="{lowered_value}""#),
        ),
        (
            "the page loop",
            format!(r#"{{service_name="{SERVICE}"}} | trace_id=~"{paged_value}""#),
        ),
    ];

    let mut measured: Vec<(&str, RouteTotals, SentSettings)> = Vec::new();
    for (name, query) in &routes {
        let tag = format!("issue544-{}-{}", name.replace(' ', "-"), trace_id(99));
        let expr = parse(query)?;
        let started = now_micros(&admin).await?;
        let (_result, _warnings) = engine.query(&expr, &params).await?;
        super::query_log::flush_logs(&admin).await?;
        tokio::time::sleep(Duration::from_secs(1)).await;
        super::query_log::flush_logs(&admin).await?;
        let totals = route_totals(&admin, &args.database, started).await?;
        let sent = sent_settings(&admin, &args.database, started).await?;
        println!("\n=== {name} — {query}  (log_comment tag {tag}) ===");
        println!("  statements          {}", totals.statements);
        println!("  read_rows           {}", totals.read_rows);
        println!("  read_bytes          {}", totals.read_bytes);
        println!("  result_bytes        {}", totals.result_bytes);
        println!("  query_duration_ms   {}", totals.query_duration_ms);
        println!("  settings this statement RECORDED (sent by the reader):");
        println!("    max_query_size      {}", sent.max_query_size);
        println!("    max_bytes_to_read   {}", sent.max_bytes_to_read);
        println!("    max_execution_time  {}", sent.max_execution_time);
        println!("    max_memory_usage    {}", sent.max_memory_usage);
        println!("  settings in force but NOT recorded (server defaults):");
        for s in &defaults {
            println!("    {:<18} {}", s.name, s.value);
        }
        // The check that stops a condition-cache hit entering a document:
        // a first execution of either route examines the whole window.
        anyhow::ensure!(
            totals.read_rows > 1_000_000,
            "{name}: read_rows is {}, which is not a first execution over a {ROWS}-row corpus \
             — a repeated statement served from the query-condition cache reports about 8,192. \
             Re-run against a fresh predicate literal.",
            totals.read_rows
        );
        measured.push((*name, totals, sent));
    }

    println!("\n=== the comparison ===");
    println!("| route | statements | result_bytes | read_rows | read_bytes |");
    println!("|---|---|---|---|---|");
    for (name, t, _) in &measured {
        println!(
            "| {name} | {} | {} | {} | {} |",
            t.statements, t.result_bytes, t.read_rows, t.read_bytes
        );
    }
    Ok(())
}

async fn now_micros(admin: &ChClient) -> anyhow::Result<u64> {
    #[derive(Row, serde::Serialize, serde::Deserialize)]
    struct N {
        n: u64,
    }
    let mut stream = admin
        .query_stream::<N>(
            "SELECT toUInt64(toUnixTimestamp64Micro(now64(6))) AS n",
            &QuerySettings::new(),
        )
        .await?;
    Ok(stream.next().await.transpose()?.map(|r| r.n).unwrap_or(0))
}

/// Every sample statement one request issued, summed.
async fn route_totals(admin: &ChClient, db: &str, since_us: u64) -> anyhow::Result<RouteTotals> {
    let sql = format!(
        "SELECT count() AS statements, sum(read_rows) AS read_rows, sum(read_bytes) AS \
         read_bytes, sum(result_bytes) AS result_bytes, sum(query_duration_ms) AS \
         query_duration_ms FROM system.query_log \
         WHERE type = 'QueryFinish' AND query_kind = 'Select' \
           AND toUInt64(toUnixTimestamp64Micro(event_time_microseconds)) >= {since_us} \
           AND has(tables, '{db}.log_samples') \
           AND NOT has(tables, 'system.query_log')"
    );
    let mut stream = admin
        .query_stream::<RouteTotals>(&sql, &QuerySettings::new())
        .await?;
    Ok(stream.next().await.transpose()?.unwrap_or_default())
}

/// The four settings the reader sent, off the newest sample statement.
async fn sent_settings(admin: &ChClient, db: &str, since_us: u64) -> anyhow::Result<SentSettings> {
    let sql = format!(
        "SELECT Settings['max_query_size'] AS max_query_size, \
                Settings['max_bytes_to_read'] AS max_bytes_to_read, \
                Settings['max_execution_time'] AS max_execution_time, \
                Settings['max_memory_usage'] AS max_memory_usage \
         FROM system.query_log \
         WHERE type = 'QueryFinish' AND query_kind = 'Select' \
           AND toUInt64(toUnixTimestamp64Micro(event_time_microseconds)) >= {since_us} \
           AND has(tables, '{db}.log_samples') \
           AND NOT has(tables, 'system.query_log') \
         ORDER BY event_time_microseconds DESC LIMIT 1"
    );
    let mut stream = admin
        .query_stream::<SentSettings>(&sql, &QuerySettings::new())
        .await?;
    Ok(stream.next().await.transpose()?.unwrap_or_default())
}

/// The three server defaults in force that no statement records.
async fn server_defaults(admin: &ChClient) -> anyhow::Result<Vec<ServerSetting>> {
    let sql = "SELECT name, value, changed FROM system.settings \
               WHERE name IN ('max_block_size', 'max_threads', 'use_query_condition_cache') \
               ORDER BY name";
    let mut stream = admin
        .query_stream::<ServerSetting>(sql, &QuerySettings::new())
        .await?;
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row?);
    }
    Ok(out)
}
