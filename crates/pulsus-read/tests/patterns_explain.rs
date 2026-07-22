//! Live Tier-1 gate + end-to-end engine assertion for `/api/logs/v1/patterns`
//! (M7-C3, issue #171). Gated behind `PULSUS_TEST_CLICKHOUSE=1`, reusing the
//! `explain_indexes.rs` harness pattern against the same ClickHouse 24.8
//! container the `schema-it` CI job runs.
//!
//! Two proofs (AC6, v2 PK-order delta):
//!  1. **Time-key pruning** (distinct from partition pruning): one fingerprint
//!     seeded across a wide `bucket_ns` span inside a SINGLE partition day; a
//!     narrow-window `EXPLAIN indexes = 1` selects strictly FEWER PrimaryKey
//!     granules than the fingerprint's full-window total — proving the
//!     `(fingerprint, bucket_ns, pattern)` order (bucket_ns before pattern)
//!     prunes at the PK level within a partition.
//!  2. **Partition pruning**: a second day is seeded; a one-day query selects
//!     fewer parts than the two-day span (the daily `PARTITION BY`).
//!
//! Plus an end-to-end engine assertion: seeded `log_patterns` rows assemble
//! into `PatternSeries` with the pinned ordering (total desc, pattern asc) and
//! exact `step_ns` re-bucketing.
//!
//! Run locally:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-patterns -p 19123:8123 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test patterns_explain
//! podman rm -f pulsus-ch-patterns
//! ```

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_read::logql::sql::{self, TimeWindow};
use pulsus_read::{EngineConfig, LogQlEngine, TimeBounds};
use pulsus_schema::{RenderCtx, SchemaParams, run_init};

fn should_run() -> bool {
    std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1")
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
        query_timeout: Duration::from_secs(30),
        ..ChConnConfig::default()
    }
}

fn test_ctx(db: &str) -> SchemaParams {
    RenderCtx {
        db: db.to_string(),
        cluster: None,
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
    }
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-read/tests/patterns_explain.rs for setup)"
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

async fn execute(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::Idempotent)
        .await
        .unwrap_or_else(|e| panic!("execute failed: {e}\nSQL:\n{sql}"));
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct ExplainRow {
    explain: String,
}

/// A `log_patterns` row for RowBinary seeding via `insert_block` (the same
/// wire path the writer uses — an `INSERT ... SELECT FROM numbers()` over the
/// HTTP query API does not land rows through the `clickhouse` crate's
/// statement executor, so seeding goes through the block inserter).
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct PatSeedRow {
    fingerprint: u64,
    bucket_ns: i64,
    pattern: String,
    count: u64,
}

/// A ClickHouse client scoped to `db` (its `?database=` is `db`). `insert_block`
/// prepends the CONNECTION database to the bare table name (it does not parse a
/// `db.table` qualifier), so pattern seeding needs a db-scoped connection — the
/// same way the production writer's client targets the configured database.
async fn db_client(db: &str) -> ChClient {
    ChClient::new(ChConnConfig {
        database: db.to_string(),
        ..test_config()
    })
    .await
    .expect("connect (db-scoped)")
}

async fn insert_patterns(db: &str, rows: &[PatSeedRow]) {
    let dbc = db_client(db).await;
    dbc.insert_block("log_patterns", rows)
        .await
        .expect("insert_block log_patterns");
    dbc.execute(
        "OPTIMIZE TABLE log_patterns FINAL",
        &QuerySettings::new(),
        Idempotency::Idempotent,
    )
    .await
    .expect("optimize log_patterns");
}

async fn explain_raw(client: &ChClient, sql: &str) -> String {
    let full = format!("EXPLAIN indexes = 1 {sql}");
    let mut stream = client
        .query_stream::<ExplainRow>(&full, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("explain query failed: {e}\nSQL:\n{full}"));
    let mut out = String::new();
    while let Some(row) = stream.next().await {
        out.push_str(&row.expect("decode explain row").explain);
        out.push('\n');
    }
    out
}

/// The `Granules: <sel>/<total>` counts under a named `EXPLAIN indexes = 1`
/// block (`PrimaryKey`/`MinMax`/`Partition`), or `None` if absent.
fn block_granules(raw: &str, block: &str) -> Option<(u64, u64)> {
    slash_pair(raw, block, "Granules:")
}

/// The `Parts: <sel>/<total>` counts under a named block.
fn block_parts(raw: &str, block: &str) -> Option<(u64, u64)> {
    slash_pair(raw, block, "Parts:")
}

const BLOCK_TITLES: &[&str] = &["MinMax", "Partition", "PrimaryKey"];

fn slash_pair(raw: &str, block: &str, field: &str) -> Option<(u64, u64)> {
    let mut in_block = false;
    for line in raw.lines() {
        let t = line.trim();
        if t == block {
            in_block = true;
            continue;
        }
        if in_block {
            if let Some(rest) = t.strip_prefix(field) {
                let (sel, total) = rest.trim().split_once('/')?;
                return Some((sel.trim().parse().ok()?, total.trim().parse().ok()?));
            }
            // Only ANOTHER index block title ends the current block — the
            // `Keys:` sub-block's bare key-name lines (`fingerprint`,
            // `bucket_ns`) must NOT be mistaken for a block boundary.
            if BLOCK_TITLES.contains(&t) {
                in_block = false;
            }
        }
    }
    None
}

const FP: u64 = 18_374_000_000_000_000_001;
const DAY_NS: i64 = 86_400_000_000_000;
const SECOND_NS: i64 = 1_000_000_000;
const BUCKET_NS: i64 = 10_000_000_000; // 10s ingest bucket

/// Nanoseconds since the Unix epoch, right now.
fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits i64")
}

/// The start (UTC midnight) of the day that was `days_ago` days before today.
/// Seeds must be **recent** (within `log_patterns`'s 7-day TTL): the fixture's
/// `OPTIMIZE ... FINAL` applies the delete-TTL, so an expired timestamp would
/// be dropped to zero rows. Day-aligned so a same-day span stays in one daily
/// partition.
fn day_start_ago(days_ago: i64) -> i64 {
    let today_start = (now_ns() / DAY_NS) * DAY_NS;
    today_start - days_ago * DAY_NS
}

#[tokio::test]
async fn patterns_read_prunes_at_the_primary_key_time_prefix_within_one_partition() {
    skip_unless_live!();
    let db = format!("pulsus_patterns_it_{}", std::process::id());
    let client = Arc::new(ChClient::new(test_config()).await.expect("connect"));
    drop_database(&client, &db).await;
    run_init(&client, &test_ctx(&db))
        .await
        .expect("schema init");

    // 40k distinct bucket_ns values 1s apart (≈11h span) on ONE recent day ⇒
    // ~5 granules (8192 rows each) in a single daily partition — so any granule
    // pruning below is PK time-key pruning (bucket_ns is the 2nd key column),
    // NOT partition pruning. Recent (yesterday) so the delete-TTL keeps them.
    let base = day_start_ago(1);
    let rows: Vec<PatSeedRow> = (0..40_000i64)
        .map(|i| PatSeedRow {
            fingerprint: FP,
            bucket_ns: base + i * SECOND_NS,
            pattern: format!("pattern alpha {i}"),
            count: 1,
        })
        .collect();
    insert_patterns(&db, &rows).await;

    let dbc = db_client(&db).await;
    let table = "log_patterns";
    let full_window = TimeWindow {
        start_ns: base - BUCKET_NS,
        end_ns: base + 40_000 * SECOND_NS,
    };
    let full_raw = explain_raw(
        &dbc,
        &sql::log_patterns_read(table, &[FP], full_window, BUCKET_NS as u64),
    )
    .await;
    let (_full_sel, total_granules) = block_granules(&full_raw, "PrimaryKey")
        .unwrap_or_else(|| panic!("no PrimaryKey Granules:\n{full_raw}"));
    assert!(
        total_granules > 1,
        "the seed must span multiple granules for the pruning to be meaningful:\n{full_raw}"
    );

    // A narrow window near the END of the span (last ~100 buckets).
    let narrow_window = TimeWindow {
        start_ns: base + 39_900 * SECOND_NS,
        end_ns: base + 40_000 * SECOND_NS,
    };
    let narrow_raw = explain_raw(
        &dbc,
        &sql::log_patterns_read(table, &[FP], narrow_window, BUCKET_NS as u64),
    )
    .await;
    let (narrow_sel, _) = block_granules(&narrow_raw, "PrimaryKey")
        .unwrap_or_else(|| panic!("no PrimaryKey Granules:\n{narrow_raw}"));

    assert!(
        narrow_sel < total_granules,
        "narrow-window PrimaryKey granules ({narrow_sel}) must be strictly fewer than the \
         fingerprint's full-span total ({total_granules}) — bucket_ns must prune at the PK level \
         within one partition\nnarrow EXPLAIN:\n{narrow_raw}"
    );

    drop_database(&client, &db).await;
}

#[tokio::test]
async fn patterns_read_prunes_daily_partitions() {
    skip_unless_live!();
    let db = format!("pulsus_patterns_part_it_{}", std::process::id());
    let client = Arc::new(ChClient::new(test_config()).await.expect("connect"));
    drop_database(&client, &db).await;
    run_init(&client, &test_ctx(&db))
        .await
        .expect("schema init");

    // One bucket yesterday, one two days ago ⇒ two daily partitions (two
    // parts), both recent enough to survive the delete-TTL.
    let day_a = day_start_ago(1);
    let day_b = day_start_ago(2);
    insert_patterns(
        &db,
        &[
            PatSeedRow {
                fingerprint: FP,
                bucket_ns: day_a,
                pattern: "pattern one".to_string(),
                count: 1,
            },
            PatSeedRow {
                fingerprint: FP,
                bucket_ns: day_b,
                pattern: "pattern two".to_string(),
                count: 1,
            },
        ],
    )
    .await;

    let dbc = db_client(&db).await;
    let table = "log_patterns";
    // Query the `day_a` bucket only: its `bucket_ns` window prunes the `day_b`
    // part via the MinMax index over the daily partitions (the MinMax block
    // sees both parts and selects one; the downstream PrimaryKey block only
    // ever sees the already-pruned survivor, so day pruning is read off MinMax
    // — distinct from the in-partition granule pruning proven above).
    let window = TimeWindow {
        start_ns: day_a - BUCKET_NS,
        end_ns: day_a + BUCKET_NS,
    };
    let raw = explain_raw(
        &dbc,
        &sql::log_patterns_read(table, &[FP], window, BUCKET_NS as u64),
    )
    .await;
    let (sel, total) =
        block_parts(&raw, "MinMax").unwrap_or_else(|| panic!("no MinMax Parts:\n{raw}"));
    assert_eq!(total, 2, "two daily partitions (two parts) seeded:\n{raw}");
    assert_eq!(
        sel, 1,
        "a one-day query must prune to a single daily part:\n{raw}"
    );

    drop_database(&client, &db).await;
}

fn engine_config(db: &str) -> EngineConfig {
    EngineConfig {
        db: db.to_string(),
        streams_idx: "log_streams_idx".to_string(),
        streams: "log_streams".to_string(),
        samples: "log_samples".to_string(),
        rollup_table: "log_metrics_5s".to_string(),
        patterns_table: "log_patterns".to_string(),
        rollup_res_ns: 5_000_000_000,
        scan_budget_bytes: 50 * 1024 * 1024 * 1024,
        max_streams: 100_000,
        pipeline_scan_factor: 10,
    }
}

#[tokio::test]
async fn patterns_engine_assembles_ordered_series_with_exact_step_rebucketing() {
    skip_unless_live!();
    let db = format!("pulsus_patterns_e2e_it_{}", std::process::id());
    let client = Arc::new(ChClient::new(test_config()).await.expect("connect"));
    drop_database(&client, &db).await;
    run_init(&client, &test_ctx(&db))
        .await
        .expect("schema init");

    // A recent day start (a multiple of 20s, so the step math below is exact).
    let b0 = day_start_ago(1);

    // Register the stream so stage-1 resolution finds FP.
    execute(
        &client,
        &format!(
            "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, updated_ns) VALUES \
             (toStartOfMonth(fromUnixTimestamp64Nano(toInt64({b0}))), {FP}, 'checkout', \
             '{{\"service_name\":\"checkout\"}}', 0)"
        ),
    )
    .await;
    execute(
        &client,
        &format!(
            "INSERT INTO {db}.log_streams_idx (month, key, val, fingerprint) VALUES \
             (toStartOfMonth(fromUnixTimestamp64Nano(toInt64({b0}))), 'service_name', 'checkout', {FP})"
        ),
    )
    .await;

    // Two patterns: "b" with total 3 (across two 10s buckets in the same 20s
    // step) and "a" with total 2 (one bucket). Ordering must be total-desc so
    // "b" precedes "a" despite "a" < "b" lexically.
    let b1 = b0 + BUCKET_NS; // still step 0 at step=20s
    let b2 = b0 + 2 * BUCKET_NS; // step 1 at step=20s
    insert_patterns(
        &db,
        &[
            PatSeedRow {
                fingerprint: FP,
                bucket_ns: b0,
                pattern: "pattern b".to_string(),
                count: 1,
            },
            PatSeedRow {
                fingerprint: FP,
                bucket_ns: b1,
                pattern: "pattern b".to_string(),
                count: 2,
            },
            PatSeedRow {
                fingerprint: FP,
                bucket_ns: b2,
                pattern: "pattern a".to_string(),
                count: 2,
            },
        ],
    )
    .await;

    let engine = LogQlEngine::new(db_client(&db).await, engine_config(&db));
    let expr = pulsus_logql::parse(r#"{service_name="checkout"}"#).expect("parse");
    let bounds = TimeBounds {
        start_ns: b0 - 1,
        end_ns: b0 + 10 * BUCKET_NS,
    };
    // step = 20s ⇒ b0+b1 collapse into step 0.
    let step_ns = 2 * BUCKET_NS as u64;
    let series = engine
        .patterns(&expr, bounds, step_ns)
        .await
        .expect("patterns");

    assert_eq!(series.len(), 2);
    // Ordering: "pattern b" (total 3) first, then "pattern a" (total 2).
    assert_eq!(series[0].pattern, "pattern b");
    assert_eq!(series[1].pattern, "pattern a");
    // "pattern b": b0 and b1 re-bucket into the SAME 20s step (secs = DAY0/1e9),
    // summing to 3 — one sample point, not two.
    let step0_secs = b0 / 1_000_000_000;
    assert_eq!(series[0].samples, vec![(step0_secs, 3)]);
    // "pattern a": one point at step 1 (b2 floored to 20s → DAY0 + 0? actually
    // intDiv(b2,20s)*20s). Compute the expected floored second.
    let a_ts_ns = (b2 / step_ns as i64) * step_ns as i64;
    assert_eq!(series[1].samples, vec![(a_ts_ns / 1_000_000_000, 2)]);

    drop_database(&client, &db).await;
}
