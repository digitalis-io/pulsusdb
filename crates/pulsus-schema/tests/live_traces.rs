//! Traces-family integration tests against a real ClickHouse server (issue
//! #53, M4-T1): `run_init` creates the three trace tables + the tag-catalog
//! MV idempotently, sample data round-trips (the MV populates
//! `trace_tag_catalog`), a `PULSUS_RETENTION_DAYS` change propagates to both
//! retained trace tables, and the two docs/schemas.md §4.2 EXPLAIN gates
//! hold on a seeded ≥100k-row corpus.
//!
//! 24.8 constraints (binding findings on issue #53):
//! - `EXPLAIN projections = 1` does not exist on 24.8 — the projection gate
//!   uses `EXPLAIN indexes = 1` plus `system.query_log.projections`.
//! - Projection selection is data-dependent: on tiny fixtures the optimizer
//!   reads the base table. The gate corpus therefore seeds ≥100k spans with
//!   a low-frequency (4%) target `service` and timestamps spread across the
//!   whole query window.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`, mirroring `live_schema.rs`:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-schema --test live_traces
//! podman rm -f pulsus-ch-test
//! ```
//!
//! Each test uses its own dedicated database (dropped at the start of the
//! test) so tests can run concurrently against the same server.

use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_schema::{RenderCtx, SchemaParams, apply_ttl, run_init};

/// Corpus size for both EXPLAIN gates — ≥100k per the binding 24.8 finding
/// on issue #53 (projection selection is data-dependent below that scale).
const CORPUS_ROWS: u64 = 120_000;

/// The seeded corpora span the most recent 6 days — inside the 7-day
/// `PULSUS_RETENTION_DAYS` TTL window (`ttl_only_drop_parts = 1` makes a
/// whole already-expired part eligible for deletion right after insert, so
/// out-of-window seeds would flake) while still crossing ~7 daily
/// partitions.
const CORPUS_SPAN_NS: i64 = 6 * 86_400 * 1_000_000_000;

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
        query_timeout: Duration::from_secs(60),
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
                 (see crates/pulsus-schema/tests/live_traces.rs for setup)"
            );
            return;
        }
    };
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct NameRow {
    name: String,
}

async fn table_names(client: &ChClient, db: &str) -> Vec<String> {
    let sql = format!("SELECT name FROM system.tables WHERE database = '{db}' ORDER BY name");
    let mut stream = client
        .query_stream::<NameRow>(&sql, &QuerySettings::new())
        .await
        .expect("query system.tables");
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode NameRow").name);
    }
    out
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CreateQueryRow {
    create_table_query: String,
}

/// The live `CREATE TABLE` statement ClickHouse reports back for `name`.
async fn create_table_query(client: &ChClient, db: &str, name: &str) -> String {
    let sql = format!(
        "SELECT create_table_query FROM system.tables WHERE database = '{db}' AND name = '{name}'"
    );
    let mut stream = client
        .query_stream::<CreateQueryRow>(&sql, &QuerySettings::new())
        .await
        .expect("query system.tables create_table_query");
    stream
        .next()
        .await
        .expect("row present")
        .expect("decode")
        .create_table_query
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
        .unwrap_or_else(|e| panic!("count query failed: {e}\nSQL:\n{sql}"));
    stream.next().await.expect("one row").expect("decode").n
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct ExplainRow {
    explain: String,
}

/// Collects `EXPLAIN indexes = 1` output as one line per element.
async fn explain_indexes(client: &ChClient, sql: &str) -> Vec<String> {
    let explain_sql = format!("EXPLAIN indexes = 1 {sql}");
    let mut stream = client
        .query_stream::<ExplainRow>(&explain_sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("EXPLAIN failed: {e}\nSQL:\n{explain_sql}"));
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode explain row").explain);
    }
    out
}

/// Parses the LAST `Granules: k/N` line of an `EXPLAIN indexes = 1` plan —
/// the PrimaryKey section's post-pruning selection (MinMax/Partition
/// sections precede it).
fn last_granules(plan: &[String]) -> (u64, u64) {
    let line = plan
        .iter()
        .rev()
        .find_map(|l| l.trim().strip_prefix("Granules: "))
        .unwrap_or_else(|| panic!("no Granules line in plan:\n{}", plan.join("\n")));
    let (k, n) = line.split_once('/').expect("k/N shape");
    (
        k.trim().parse().expect("granules selected"),
        n.trim().parse().expect("granules total"),
    )
}

/// Asserts the plan's `PrimaryKey` `Keys:` list starts with `expected` in
/// order (the plan prints one key per line under `Keys:`).
fn assert_primary_key_keys(plan: &[String], expected: &[&str]) {
    let keys_at = plan
        .iter()
        .position(|l| l.trim() == "Keys:")
        .unwrap_or_else(|| panic!("no Keys: section in plan:\n{}", plan.join("\n")));
    for (i, key) in expected.iter().enumerate() {
        let got = plan
            .get(keys_at + 1 + i)
            .map(|l| l.trim())
            .unwrap_or_default();
        assert_eq!(
            got,
            *key,
            "PrimaryKey key #{i} must be {key}, got {got}:\n{}",
            plan.join("\n")
        );
    }
}

/// Runs `sql` (fully drained) tagged with `query_id`, flushes logs, and
/// returns the `QueryFinish` evidence (`projections`, `read_rows`) from
/// `system.query_log`.
async fn run_and_capture_query_log(
    client: &ChClient,
    sql: &str,
    query_id: &str,
) -> (Vec<String>, u64) {
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct QueryLogRow {
        projections: Vec<String>,
        read_rows: u64,
    }

    let settings = QuerySettings::new().set("query_id", query_id);
    let mut stream = client
        .query_stream::<CountRow>(sql, &settings)
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\nSQL:\n{sql}"));
    while let Some(row) = stream.next().await {
        row.expect("decode row");
    }
    drop(stream);

    client
        .execute(
            "SYSTEM FLUSH LOGS",
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("flush logs");

    let log_sql = format!(
        "SELECT projections, read_rows FROM system.query_log \
         WHERE query_id = '{query_id}' AND type = 'QueryFinish' \
         ORDER BY event_time_microseconds DESC LIMIT 1"
    );
    let mut log_stream = client
        .query_stream::<QueryLogRow>(&log_sql, &QuerySettings::new())
        .await
        .expect("query system.query_log");
    let evidence = log_stream
        .next()
        .await
        .unwrap_or_else(|| panic!("no query_log row for query_id {query_id}"))
        .expect("decode query_log row");
    (evidence.projections, evidence.read_rows)
}

/// The most recent whole second as nanoseconds since epoch.
fn now_ns() -> i64 {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    i64::try_from(secs).expect("fits i64") * 1_000_000_000
}

/// Seeds [`CORPUS_ROWS`] `trace_spans` rows server-side, deterministically
/// derived from `number` (only the time anchor varies run to run): `service
/// = 'checkout'` on 4% of rows (`number % 25 = 0` — the low-frequency
/// target the projection gate needs), timestamps spread evenly across
/// `[base_ns, base_ns + CORPUS_SPAN_NS)`.
async fn seed_spans_corpus(client: &ChClient, db: &str, base_ns: i64) {
    let step_ns = CORPUS_SPAN_NS / i64::try_from(CORPUS_ROWS).expect("fits i64");
    let sql = format!(
        "INSERT INTO {db}.trace_spans \
             (trace_id, span_id, parent_id, name, service, timestamp_ns, duration_ns, \
              status_code, kind, payload_type, payload) \
         SELECT \
             toFixedString(hex(cityHash64(number)), 16), \
             toFixedString(substring(hex(cityHash64(number, 1)), 1, 8), 8), \
             toFixedString(substring(hex(cityHash64(number, 2)), 1, 8), 8), \
             concat('op-', toString(number % 20)), \
             if(number % 25 = 0, 'checkout', concat('svc-', toString(number % 40))), \
             {base_ns} + number * {step_ns}, \
             (number % 1000) * 10000000, \
             0, 2, 1, \
             concat('payload-', toString(number)) \
         FROM numbers({CORPUS_ROWS})"
    );
    client
        .execute(&sql, &QuerySettings::new(), Idempotency::NonIdempotent)
        .await
        .expect("seed trace_spans corpus");
}

/// Seeds [`CORPUS_ROWS`] `trace_attrs_idx` rows as ONE dense
/// `(key='http.status_code', val='500', scope='span')` group with
/// timestamps spread evenly across `[base_ns, base_ns + CORPUS_SPAN_NS)` —
/// so that single `(key, val, scope)` prefix spans many granules and
/// pruning under a narrower time window is attributable to the timestamp
/// predicate alone (issue #53 plan v2 delta 3, scope column added by issue
/// #54's amended migration 17).
async fn seed_attrs_corpus(client: &ChClient, db: &str, base_ns: i64) {
    let step_ns = CORPUS_SPAN_NS / i64::try_from(CORPUS_ROWS).expect("fits i64");
    let sql = format!(
        "INSERT INTO {db}.trace_attrs_idx \
             (date, key, val, scope, val_num, timestamp_ns, trace_id, span_id, duration_ns) \
         SELECT \
             toDate(fromUnixTimestamp64Nano({base_ns} + number * {step_ns})), \
             'http.status_code', \
             '500', \
             'span', \
             500, \
             {base_ns} + number * {step_ns}, \
             toFixedString(hex(cityHash64(number)), 16), \
             toFixedString(substring(hex(cityHash64(number, 1)), 1, 8), 8), \
             (number % 1000) * 10000000 \
         FROM numbers({CORPUS_ROWS})"
    );
    client
        .execute(&sql, &QuerySettings::new(), Idempotency::NonIdempotent)
        .await
        .expect("seed trace_attrs_idx corpus");
}

/// AC2 (issue #53): `run_init` on a fresh database creates every trace
/// object, a second run adds/removes nothing (in particular no
/// `MigrationDrift`), inserted spans + attrs round-trip, and the MV
/// populates `trace_tag_catalog` with the deduplicated `(key, val)` set.
#[tokio::test]
async fn run_init_creates_trace_tables_and_mv_and_round_trips_via_the_catalog_mv() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = "pulsus_schema_it_traces_full";
    drop_database(&client, db).await;
    let ctx = test_ctx(db);

    run_init(&client, &ctx).await.expect("run_init (first run)");

    let names = table_names(&client, db).await;
    for t in [
        "trace_spans",
        "trace_attrs_idx",
        "trace_tag_catalog",
        "trace_tag_catalog_mv",
    ] {
        assert!(
            names.contains(&t.to_string()),
            "missing {t} in system.tables: {names:?}"
        );
    }
    // Single-node mode: no `_dist` wrappers at all — and `trace_tag_catalog`
    // never gets one in any mode (Replication::Global catalog).
    assert!(!names.iter().any(|n| n.ends_with("_dist")));

    run_init(&client, &ctx)
        .await
        .expect("run_init (second run, no-op)");
    let names_after = table_names(&client, db).await;
    assert_eq!(
        names, names_after,
        "second run must not add or remove objects"
    );

    // Round-trip: recent timestamps (within the 7-day TTL window —
    // `ttl_only_drop_parts = 1` would make an already-expired part eligible
    // for deletion right after insert).
    let ts = now_ns();
    client
        .execute(
            &format!(
                "INSERT INTO {db}.trace_spans \
                     (trace_id, span_id, parent_id, name, service, timestamp_ns, duration_ns, \
                      status_code, kind, payload_type, payload) \
                 VALUES \
                     ('0123456789abcdef', 'span0001', '00000000', 'op-a', 'checkout', {ts}, \
                      1000000, 0, 2, 1, 'payload-a'), \
                     ('fedcba9876543210', 'span0002', 'span0001', 'op-b', 'billing', {ts}, \
                      2000000, 2, 3, 1, 'payload-b')"
            ),
            &QuerySettings::new(),
            Idempotency::NonIdempotent,
        )
        .await
        .expect("insert trace_spans");
    assert_eq!(
        count(
            &client,
            &format!("SELECT count() AS n FROM {db}.trace_spans")
        )
        .await,
        2
    );

    // Five attr rows, four distinct (scope, key, val) triples — the MV
    // must land the deduplicated scoped set in trace_tag_catalog (read
    // FINAL: ReplacingMergeTree). 'deployment.environment'/'prod' exists
    // at BOTH scopes (issue #54's dual-scope case): the catalog must keep
    // both rows, separated only by scope.
    client
        .execute(
            &format!(
                "INSERT INTO {db}.trace_attrs_idx \
                     (date, key, val, scope, val_num, timestamp_ns, trace_id, span_id, duration_ns) \
                 VALUES \
                     (toDate(fromUnixTimestamp64Nano({ts})), 'http.status_code', '500', 'span', 500, {ts}, \
                      '0123456789abcdef', 'span0001', 1000000), \
                     (toDate(fromUnixTimestamp64Nano({ts})), 'http.status_code', '500', 'span', 500, {ts}, \
                      'fedcba9876543210', 'span0002', 2000000), \
                     (toDate(fromUnixTimestamp64Nano({ts})), 'http.status_code', '404', 'span', 404, {ts}, \
                      '0123456789abcdef', 'span0001', 1000000), \
                     (toDate(fromUnixTimestamp64Nano({ts})), 'deployment.environment', 'prod', 'resource', NULL, {ts}, \
                      '0123456789abcdef', 'span0001', 1000000), \
                     (toDate(fromUnixTimestamp64Nano({ts})), 'deployment.environment', 'prod', 'span', NULL, {ts}, \
                      '0123456789abcdef', 'span0001', 1000000)"
            ),
            &QuerySettings::new(),
            Idempotency::NonIdempotent,
        )
        .await
        .expect("insert trace_attrs_idx");
    assert_eq!(
        count(
            &client,
            &format!("SELECT count() AS n FROM {db}.trace_attrs_idx")
        )
        .await,
        5
    );

    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
    struct TagRow {
        scope: String,
        key: String,
        val: String,
    }
    let mut stream = client
        .query_stream::<TagRow>(
            &format!(
                "SELECT scope, key, val FROM {db}.trace_tag_catalog FINAL ORDER BY scope, key, val"
            ),
            &QuerySettings::new(),
        )
        .await
        .expect("select trace_tag_catalog");
    let mut tags = Vec::new();
    while let Some(row) = stream.next().await {
        tags.push(row.expect("decode tag row"));
    }
    let expected: Vec<TagRow> = [
        ("resource", "deployment.environment", "prod"),
        ("span", "deployment.environment", "prod"),
        ("span", "http.status_code", "404"),
        ("span", "http.status_code", "500"),
    ]
    .into_iter()
    .map(|(scope, key, val)| TagRow {
        scope: scope.to_string(),
        key: key.to_string(),
        val: val.to_string(),
    })
    .collect();
    assert_eq!(
        tags, expected,
        "the MV must populate trace_tag_catalog with the deduplicated (scope, key, val) set — \
         the dual-scope pair stays two rows, separated only by scope"
    );
}

/// AC3a (issue #53): on the seeded ≥100k-span corpus, the docs/schemas.md
/// §4.2 Stage-1 intrinsics shape selects the `service_time` projection —
/// `EXPLAIN indexes = 1` shows `ReadFromMergeTree (service_time)` with
/// projection primary-key keys `service, timestamp_ns` (the base PK is
/// `trace_id, timestamp_ns`), and running the query records the projection
/// in `system.query_log.projections`. `EXPLAIN projections = 1` is 25.x-only
/// and deliberately not used here (binding 24.8 finding).
#[tokio::test]
async fn stage1_intrinsics_query_selects_the_service_time_projection() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = "pulsus_schema_it_traces_projection";
    drop_database(&client, db).await;
    let ctx = test_ctx(db);
    run_init(&client, &ctx).await.expect("run_init");

    let end_ns = now_ns();
    let base_ns = end_ns - CORPUS_SPAN_NS;
    seed_spans_corpus(&client, db, base_ns).await;

    let stage1 = format!(
        "SELECT trace_id, span_id, timestamp_ns, duration_ns FROM {db}.trace_spans \
         PREWHERE service = 'checkout' \
         WHERE timestamp_ns > {base_ns} AND timestamp_ns <= {end_ns} \
           AND duration_ns > 2000000000"
    );

    let plan = explain_indexes(&client, &stage1).await;
    let plan_text = plan.join("\n");
    assert!(
        plan_text.contains("ReadFromMergeTree (service_time)"),
        "the optimizer must read the service_time projection, got:\n{plan_text}"
    );
    assert_primary_key_keys(&plan, &["service", "timestamp_ns"]);
    let (selected, total) = last_granules(&plan);
    assert!(
        selected < total,
        "the projection's service prefix must prune granules ({selected}/{total}):\n{plan_text}"
    );

    // Corroborate by execution: query_log must record the projection, and
    // the read must not be a full scan. Wrapped in count() so the RowBinary
    // shape stays trivial — the inner read shape is unchanged.
    let query_id = format!("pulsus-it-traces-proj-{}", std::process::id());
    let (projections, read_rows) = run_and_capture_query_log(
        &client,
        &format!("SELECT count() AS n FROM ({stage1})"),
        &query_id,
    )
    .await;
    assert_eq!(
        projections,
        vec![format!("{db}.trace_spans.service_time")],
        "query_log must attribute the read to the service_time projection"
    );
    assert!(
        read_rows > 0 && read_rows < CORPUS_ROWS,
        "projection read must not scan the full corpus (read_rows = {read_rows} of {CORPUS_ROWS})"
    );
}

/// AC3b (issue #53 plan v2 delta 3, re-proven under issue #54's amended
/// scoped DDL): within ONE dense `(key, val, scope)` prefix spanning many
/// granules, the identical Stage-2 attr query (now scoped, docs/schemas.md
/// §4.2) over a narrow (1h) time window must read strictly fewer granules —
/// and rows — than the full-range run, proving the `(key, val, scope,
/// timestamp_ns, ...)` primary key time-prunes *within* the scoped key
/// prefix (not merely via key/val selectivity).
#[tokio::test]
async fn narrow_time_window_prunes_granules_within_a_fixed_key_val_prefix() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = "pulsus_schema_it_traces_attr_prune";
    drop_database(&client, db).await;
    let ctx = test_ctx(db);
    run_init(&client, &ctx).await.expect("run_init");

    let end_ns = now_ns();
    let base_ns = end_ns - CORPUS_SPAN_NS;
    seed_attrs_corpus(&client, db, base_ns).await;

    let stage2 = |from_ns: i64| {
        format!(
            "SELECT trace_id, span_id FROM {db}.trace_attrs_idx \
             WHERE key = 'http.status_code' AND val = '500' AND scope = 'span' \
               AND timestamp_ns > {from_ns} AND timestamp_ns <= {end_ns}"
        )
    };
    let full_sql = stage2(base_ns);
    let narrow_sql = stage2(end_ns - 3_600 * 1_000_000_000);

    let full_plan = explain_indexes(&client, &full_sql).await;
    let narrow_plan = explain_indexes(&client, &narrow_sql).await;
    assert_primary_key_keys(&full_plan, &["key", "val", "scope", "timestamp_ns"]);
    assert_primary_key_keys(&narrow_plan, &["key", "val", "scope", "timestamp_ns"]);

    let (full_selected, full_total) = last_granules(&full_plan);
    let (narrow_selected, narrow_total) = last_granules(&narrow_plan);
    assert_eq!(full_total, narrow_total, "same table, same granule total");
    assert!(
        full_selected > 1,
        "the dense (key, val) group must span multiple granules or the gate proves nothing \
         (full-range selected {full_selected})"
    );
    assert!(
        narrow_selected < full_selected,
        "the narrow window must prune granules within the fixed (key, val) prefix: \
         narrow {narrow_selected}/{narrow_total} vs full {full_selected}/{full_total}\n\
         narrow plan:\n{}\nfull plan:\n{}",
        narrow_plan.join("\n"),
        full_plan.join("\n")
    );

    // Execution-side corroboration: strictly fewer rows read too. NOT a
    // bare `count()` wrap (issue #54 re-prove finding): on 24.8 a filtered
    // count whose predicate is fully primary-key-analyzable is answered
    // from index metadata for every fully-matched granule range, so
    // `read_rows` collapses to the *boundary* parts only — the full-range
    // run reads just the first (partial) day partition and the narrow run
    // the last, and which is larger depends on the time of day the corpus
    // was seeded (a latent flake). `uniqExact(span_id)` forces every
    // matched row to actually be read, so `read_rows` reflects real
    // scanning on both legs.
    let pid = std::process::id();
    let (_, full_read_rows) = run_and_capture_query_log(
        &client,
        &format!("SELECT uniqExact(span_id) AS n FROM ({full_sql})"),
        &format!("pulsus-it-traces-attr-full-{pid}"),
    )
    .await;
    let (_, narrow_read_rows) = run_and_capture_query_log(
        &client,
        &format!("SELECT uniqExact(span_id) AS n FROM ({narrow_sql})"),
        &format!("pulsus-it-traces-attr-narrow-{pid}"),
    )
    .await;
    assert!(
        narrow_read_rows < full_read_rows,
        "narrow window must read strictly fewer rows ({narrow_read_rows} vs {full_read_rows})"
    );
}

/// Plan v2 delta 1 (issue #53): a `PULSUS_RETENTION_DAYS` change re-init
/// must succeed (retention is excluded from migration identity — no
/// `MigrationDrift`) and must propagate the new TTL to BOTH retained trace
/// tables via `apply_ttl` (`trace_tag_catalog` is a bounded catalog and
/// carries no TTL).
#[tokio::test]
async fn run_init_after_retention_days_change_updates_ttl_on_both_trace_tables() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = "pulsus_schema_it_traces_retention";
    drop_database(&client, db).await;
    let mut ctx = test_ctx(db);
    ctx.retention_days = 7;

    run_init(&client, &ctx)
        .await
        .expect("run_init (retention_days=7)");
    for table in ["trace_spans", "trace_attrs_idx"] {
        let ddl = create_table_query(&client, db, table).await;
        // `apply_ttl` (issue #131) supersedes the CREATE-time TTL with the
        // saturating expression; ClickHouse normalizes the rendered
        // `{{retention_days}} * 86400` product by wrapping it in parens.
        assert!(
            ddl.contains("least(intDiv(timestamp_ns, 1000000000) + (7 * 86400), 4294967295)"),
            "{table}'s initial TTL must reflect retention_days=7: {ddl}"
        );
    }

    ctx.retention_days = 30;
    run_init(&client, &ctx)
        .await
        .expect("re-init after a PULSUS_RETENTION_DAYS change must succeed, not MigrationDrift");

    for table in ["trace_spans", "trace_attrs_idx"] {
        let ddl = create_table_query(&client, db, table).await;
        assert!(
            ddl.contains("(30 * 86400)"),
            "{table}'s TTL must be updated to the new retention_days: {ddl}"
        );
        assert!(!ddl.contains("(7 * 86400)"), "{table}: stale TTL: {ddl}");
    }

    let catalog_ddl = create_table_query(&client, db, "trace_tag_catalog").await;
    assert!(
        !catalog_ddl.contains("TTL"),
        "trace_tag_catalog is a bounded catalog and must carry no TTL: {catalog_ddl}"
    );
}

/// The last admitted trace timestamp (issue #131): the final nanosecond of
/// day 49_709 (2106-02-06), whose floor-seconds value `4_294_943_999` is
/// the last whole second of the last fully u32-representable UTC day.
const BOUNDARY_TS_NS: i64 = 49_710 * 86_400_000_000_000 - 1;

/// The saturating trace TTL expression `apply_ttl` renders (issue #131,
/// Resolution C), as a SELECT-able snippet over a literal `ts`.
fn new_ttl_expr(ts_ns: i64, retention_days: u32) -> String {
    format!(
        "toDateTime(least(intDiv(toInt64({ts_ns}), 1000000000) + {retention_days} * 86400, \
         4294967295))"
    )
}

/// Issue #131 AC10a/b/d: semantics of the saturating trace TTL expression
/// on a live 24.8 server —
/// (a) for a normal-range timestamp it is value-identical to the pre-#131
///     `toDateTime(fromUnixTimestamp64Nano(ts)) + INTERVAL n DAY` form;
/// (b) at the boundary timestamp it clamps exactly to
///     `toDateTime(4294967295)` (2106-02-07T06:28:15Z);
/// (d) `apply_ttl` with `retention_days = u32::MAX` is accepted by the
///     server (the rendered arithmetic stays Int64 — max operand sum
///     ≈ 3.71e14, no overflow, no type-coercion surprise) and the
///     expression clamps an admitted present-day timestamp exactly to
///     `toDateTime(4294967295)`.
#[tokio::test]
async fn trace_ttl_expression_is_equivalent_in_range_and_saturates_at_the_boundary() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = "pulsus_schema_it_traces_ttl_expr";
    drop_database(&client, db).await;
    let mut ctx = test_ctx(db);
    run_init(&client, &ctx).await.expect("run_init");

    // (a) Equivalence for a normal-range timestamp (2023-11-14T22:13:20Z).
    let normal_ts_ns: i64 = 1_700_000_000_123_456_789;
    let new_expr = new_ttl_expr(normal_ts_ns, 7);
    let equal = count(
        &client,
        &format!(
            "SELECT toUInt64({new_expr} = \
             (toDateTime(fromUnixTimestamp64Nano(toInt64({normal_ts_ns}))) + INTERVAL 7 DAY)) AS n"
        ),
    )
    .await;
    assert_eq!(
        equal, 1,
        "new expression must equal the pre-#131 expiry for a normal-range ts"
    );

    // (b) Saturation at the boundary timestamp.
    let boundary_expr = new_ttl_expr(BOUNDARY_TS_NS, 7);
    let saturated = count(
        &client,
        &format!("SELECT toUInt64({boundary_expr} = toDateTime(4294967295)) AS n"),
    )
    .await;
    assert_eq!(
        saturated, 1,
        "boundary ts + 7d must clamp exactly to toDateTime(4294967295)"
    );

    // (d) Extreme retention: the rendered ALTER is accepted at
    // retention_days = u32::MAX, and the expression clamps an admitted
    // present-day ts exactly to the u32::MAX instant. Also pin the
    // arithmetic type: the un-clamped sum stays Int64 on the server.
    ctx.retention_days = u32::MAX;
    apply_ttl(&client, &ctx)
        .await
        .expect("apply_ttl at retention_days = u32::MAX must be accepted");
    for table in ["trace_spans", "trace_attrs_idx"] {
        let ddl = create_table_query(&client, db, table).await;
        assert!(
            ddl.contains("(4294967295 * 86400)"),
            "{table}'s TTL must carry the extreme retention product: {ddl}"
        );
    }
    let now = now_ns();
    let extreme_expr = new_ttl_expr(now, u32::MAX);
    let clamped = count(
        &client,
        &format!("SELECT toUInt64({extreme_expr} = toDateTime(4294967295)) AS n"),
    )
    .await;
    assert_eq!(
        clamped, 1,
        "an admitted present-day ts must clamp exactly to toDateTime(4294967295) \
         at retention_days = u32::MAX"
    );
    let int64_type = count(
        &client,
        &format!(
            "SELECT toUInt64(toTypeName(intDiv(toInt64({now}), 1000000000) + \
             4294967295 * 86400) = 'Int64') AS n"
        ),
    )
    .await;
    assert_eq!(
        int64_type, 1,
        "the un-clamped seconds arithmetic must resolve to Int64 on the server"
    );

    drop_database(&client, db).await;
}

/// Issue #131 AC10c (survival, non-vacuous): a `trace_spans` row at the
/// boundary timestamp survives `MATERIALIZE TTL` + `OPTIMIZE ... FINAL`
/// under the saturating expression `apply_ttl` installed (retention 7),
/// then DROPS once the pre-#131 wrapping expression is re-installed — its
/// wrapped expiry is ~1970-01-07, so the part reads as long-expired
/// (`ttl_only_drop_parts = 1`). The second phase is the pre-fix behavior
/// pinned in-test: on pre-#131 `apply_ttl` text the first phase fails.
#[tokio::test]
async fn boundary_span_survives_saturating_ttl_and_drops_under_the_wrapping_ttl() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = "pulsus_schema_it_traces_ttl_boundary";
    drop_database(&client, db).await;
    let ctx = test_ctx(db); // retention_days = 7; run_init applies the new TTL
    run_init(&client, &ctx).await.expect("run_init");

    client
        .execute(
            &format!(
                "INSERT INTO {db}.trace_spans \
                     (trace_id, span_id, parent_id, name, service, timestamp_ns, duration_ns, \
                      status_code, kind, payload_type, payload) \
                 VALUES (toFixedString('0123456789ABCDEF', 16), toFixedString('01234567', 8), \
                         toFixedString('00000000', 8), 'op-boundary', 'svc-boundary', \
                         {BOUNDARY_TS_NS}, 0, 0, 2, 1, 'payload-boundary')"
            ),
            &QuerySettings::new(),
            Idempotency::NonIdempotent,
        )
        .await
        .expect("insert boundary span");

    let materialize_and_optimize = |sql_db: String| {
        let client = &client;
        async move {
            client
                .execute(
                    &format!(
                        "ALTER TABLE {sql_db}.trace_spans MATERIALIZE TTL \
                         SETTINGS mutations_sync = 2"
                    ),
                    &QuerySettings::new(),
                    Idempotency::Idempotent,
                )
                .await
                .expect("MATERIALIZE TTL");
            client
                .execute(
                    &format!("OPTIMIZE TABLE {sql_db}.trace_spans FINAL"),
                    &QuerySettings::new(),
                    Idempotency::Idempotent,
                )
                .await
                .expect("OPTIMIZE FINAL");
        }
    };

    materialize_and_optimize(db.to_string()).await;
    let survived = count(
        &client,
        &format!("SELECT count() AS n FROM {db}.trace_spans"),
    )
    .await;
    assert_eq!(
        survived, 1,
        "the boundary row must survive MATERIALIZE TTL + OPTIMIZE FINAL under the \
         saturating expression (fails on the pre-#131 wrapping expression)"
    );

    // Re-install the pre-#131 wrapping expression verbatim: the same row's
    // expiry wraps past u32::MAX to ~1970-01-07 and the part is dropped.
    client
        .execute(
            &format!(
                "ALTER TABLE {db}.trace_spans MODIFY TTL \
                 toDateTime(fromUnixTimestamp64Nano(timestamp_ns)) + INTERVAL 7 DAY DELETE"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("re-install the pre-#131 wrapping TTL");
    materialize_and_optimize(db.to_string()).await;
    let dropped = count(
        &client,
        &format!("SELECT count() AS n FROM {db}.trace_spans"),
    )
    .await;
    assert_eq!(
        dropped, 0,
        "the same row must drop under the pre-#131 wrapping expression — this pins \
         the defect the saturating expression closes"
    );

    drop_database(&client, db).await;
}
