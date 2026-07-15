//! Issue #55: the trace-by-ID read path's own gates — the hermetic AC1
//! SQL snapshot (byte-equal to docs/schemas.md §4.2's canonical point
//! read) and the live `EXPLAIN indexes = 1` primary-key gate (AC6): on a
//! seeded many-trace corpus the point read must show a `PrimaryKey` block
//! keyed on `trace_id` with granules pruned (`Granules: k/N`, `k < N`) —
//! proving it is a primary-index read, not a scan. Scale-invariant.
//! Extraction mirrors `explain_indexes.rs`'s "reduce the raw text to its
//! stable lines" idiom, plus the granule ratio this gate is specifically
//! about (which `explain_indexes.rs` deliberately drops).
//!
//! Live half gated behind `PULSUS_TEST_CLICKHOUSE=1`, same podman harness
//! as the other live suites:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test traces_point_read
//! podman rm -f pulsus-ch-test
//! ```

use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_read::traces::sql::point_read_sql;
use pulsus_read::{TraceEngine, TraceReadConfig};
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
        database: "default".to_string(),
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

/// AC1: the generated point read is byte-equal to docs/schemas.md §4.2's
/// canonical "Trace by ID" query (hermetic — also unit-pinned inside
/// `traces/sql.rs`; duplicated here so the read-path gate file carries the
/// contract alongside the EXPLAIN gate it licenses).
#[test]
fn point_read_sql_byte_equals_schemas_md_4_2() {
    assert_eq!(
        point_read_sql("trace_spans", "4bf92f3577b34da6a3ce929d0e0e4736"),
        "SELECT trace_id, span_id, parent_id, payload_type, payload\n\
         FROM trace_spans\n\
         WHERE trace_id = unhex('4bf92f3577b34da6a3ce929d0e0e4736')"
    );
}

/// Unlike `explain_indexes.rs`'s `String`-typed explain row, this one
/// reads raw bytes: the point read's `EXPLAIN` output renders the
/// `unhex('…')` FixedString(16) literal as raw bytes inside its
/// `Condition:` line, which is not valid UTF-8 — the gate's own lines
/// (`Keys:`/`Granules:`) are plain ASCII, so a lossy decode is exact where
/// it matters.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct ExplainRow {
    #[serde(with = "serde_bytes")]
    explain: Vec<u8>,
}

async fn explain_raw(client: &ChClient, sql: &str) -> String {
    let full = format!("EXPLAIN indexes = 1 {sql}");
    let mut stream = client
        .query_stream::<ExplainRow>(&full, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("explain query failed: {e}\nSQL:\n{full}"));
    let mut out = String::new();
    while let Some(row) = stream.next().await {
        out.push_str(&String::from_utf8_lossy(
            &row.expect("decode explain row").explain,
        ));
        out.push('\n');
    }
    out
}

/// The `PrimaryKey` block's key names and its `Granules: k/N` ratio, from
/// raw `EXPLAIN indexes = 1` text. Panics (with the raw text) when the
/// block is missing — a point read without primary-key analysis *is* the
/// regression this gate exists to catch.
fn primary_key_usage(raw: &str) -> (Vec<String>, u64, u64) {
    const BLOCK_TITLES: &[&str] = &["MinMax", "Partition", "PrimaryKey", "Skip"];
    let mut in_pk = false;
    let mut capturing_keys = false;
    let mut keys: Vec<String> = Vec::new();
    let mut granules: Option<(u64, u64)> = None;
    for line in raw.lines() {
        let trimmed = line.trim();
        if BLOCK_TITLES.contains(&trimmed) {
            if in_pk {
                break; // left the PrimaryKey block
            }
            in_pk = trimmed == "PrimaryKey";
            continue;
        }
        if !in_pk {
            continue;
        }
        if trimmed == "Keys:" {
            capturing_keys = true;
            continue;
        }
        if capturing_keys {
            if !trimmed.is_empty() && !trimmed.contains(':') {
                keys.push(trimmed.to_string());
                continue;
            }
            capturing_keys = false;
        }
        if let Some(ratio) = trimmed.strip_prefix("Granules: ") {
            let (selected, total) = ratio.split_once('/').unwrap_or_else(|| {
                panic!("unparseable Granules line {trimmed:?} in EXPLAIN output:\n{raw}")
            });
            granules = Some((
                selected.trim().parse().expect("selected granule count"),
                total.trim().parse().expect("total granule count"),
            ));
        }
    }
    let (selected, total) = granules.unwrap_or_else(|| {
        panic!("no PrimaryKey block with a Granules line in EXPLAIN output:\n{raw}")
    });
    (keys, selected, total)
}

const CORPUS_TRACES: u64 = 100_000;
/// An arbitrary mid-corpus trace to point-read.
const TARGET: u64 = 54_321;

fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits i64")
}

/// Seeds `CORPUS_TRACES` single-span traces (distinct `trace_id`s, one
/// part, ~13 granules at the default 8192-row granularity) in one
/// server-side `INSERT ... SELECT` — timestamps wall-clock-recent so the
/// 7-day TTL can never drop the part underfoot (same rationale as
/// `explain_indexes.rs`).
async fn seed_corpus(client: &ChClient, db: &str) {
    let now = now_ns();
    client
        .execute(
            &format!(
                "INSERT INTO {db}.trace_spans \
                 (trace_id, span_id, parent_id, name, service, timestamp_ns, duration_ns, \
                  status_code, kind, payload_type, payload) \
                 SELECT \
                   toFixedString(unhex(leftPad(lower(hex(number)), 32, '0')), 16), \
                   toFixedString(unhex(leftPad(lower(hex(number)), 16, '0')), 8), \
                   toFixedString(unhex('0000000000000000'), 8), \
                   'gate-span', 'gate-svc', {now} + toInt64(number), 1000, 0, 1, 1, 'p' \
                 FROM numbers({CORPUS_TRACES})"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed trace_spans corpus");
}

#[tokio::test]
async fn point_read_is_a_primary_index_read_with_pruned_granules() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
             (see crates/pulsus-read/tests/traces_point_read.rs for setup)"
        );
        return;
    }

    let db = "pulsus_traces_point_read_it";
    let admin = ChClient::new(test_config()).await.expect("connect");
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
    run_init(&admin, &test_ctx(db)).await.expect("run_init");

    let mut data_cfg = test_config();
    data_cfg.database = db.to_string();
    let client = ChClient::new(data_cfg).await.expect("connect data client");
    seed_corpus(&client, db).await;

    let hex32 = format!("{TARGET:032x}");
    let sql = point_read_sql("trace_spans", &hex32);

    // AC6: PrimaryKey block keyed on trace_id, granules pruned (k < N).
    let raw = explain_raw(&client, &sql).await;
    let (keys, selected, total) = primary_key_usage(&raw);
    assert!(
        keys.iter().any(|k| k == "trace_id"),
        "PrimaryKey block must be keyed on trace_id, got {keys:?}\n{raw}"
    );
    assert!(
        selected >= 1 && selected < total,
        "point read must prune granules (selected {selected} / total {total}) — a full-granule \
         selection means the primary index is not engaged\n{raw}"
    );

    // The engine's own fetch over the same corpus: one stored span, the
    // expected span_id/payload — proving `StoredSpanRow`'s RowBinary
    // column alignment against the real table, not just the SQL text.
    let engine = TraceEngine::new(
        ChClient::new({
            let mut cfg = test_config();
            cfg.database = db.to_string();
            cfg
        })
        .await
        .expect("connect engine client"),
        TraceReadConfig {
            spans_table: "trace_spans".to_string(),
            attrs_table: "trace_attrs_idx".to_string(),
            max_candidates: 100_000,
            scan_budget_rows: 50_000_000,
            distributed: false,
            skip_unavailable_shards: false,
        },
    );
    let spans = engine.fetch_by_id(&hex32).await.expect("fetch_by_id");
    assert_eq!(spans.len(), 1, "exactly one stored span for {hex32}");
    assert_eq!(spans[0].span_id, TARGET.to_be_bytes());
    assert_eq!(spans[0].payload_type, 1);
    assert_eq!(spans[0].payload, b"p".to_vec());

    // An absent id (outside the corpus) is an empty fetch, never an error.
    let absent = format!("{:032x}", CORPUS_TRACES + 17);
    let empty = engine.fetch_by_id(&absent).await.expect("fetch absent");
    assert!(empty.is_empty(), "absent trace must fetch zero spans");
}
