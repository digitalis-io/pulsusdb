//! Issue #58 AC9 (Tier-1, scale-invariant): live index gates for the
//! §4.3 tag-discovery reads against ClickHouse 24.8, on the exact SQL
//! `tags_sql` emits (the byte-frozen builder surface `TraceEngine`
//! executes).
//!
//! The four query shapes and their honest index behaviour
//! (docs/schemas.md §4.1 — the catalog orders `(scope, key, val)`,
//! scope FIRST):
//!
//! - **scoped tag-names** (`WHERE scope = …`) → strict `(scope)`
//!   primary-key-prefix prune (`selected < total`, two-shape
//!   comparison + `system.query_log` corroboration — the #53 AC3b
//!   idiom);
//! - **scoped + keyed values** (`WHERE key = … AND scope = …`) → strict
//!   `(scope, key)` prefix prune;
//! - **unscoped tag-names** (no predicate) → full catalog scan by
//!   nature — recorded via `query_log.read_rows == the whole catalog`,
//!   documented, never silently dropped;
//! - **unscoped values** (`WHERE key = …`, no scope) → the documented
//!   degraded path (no `(scope)` prefix to prune on); its granule ratio
//!   and `read_rows` are recorded, and the gate pins the honest bound:
//!   it never reads FEWER rows than its scoped twin, and the scoped
//!   twin stays strictly under the full-catalog baseline.
//!
//! All ratios are granule/row *ratios* — scale-invariant, no wall-time.
//! Live-gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test traces_tags_explain
//! podman rm -f pulsus-ch-test
//! ```

use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_read::logql::escape::ch_string;
use pulsus_read::logql::{ReadError, TooBroadReason};
use pulsus_read::traces::rows::{TagNameRow, TagValueRow};
use pulsus_read::traces::tags_sql::{tag_names_sql, tag_values_sql};
use pulsus_read::{TAG_NAMES_MAX, TAG_VALUES_MAX, TraceEngine, TraceReadConfig};
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

const DB: &str = "pulsus_traces_tags_it";
/// Distinct values per (scope, key) — 10 keys × 2 scopes ×
/// `VALS_PER_KEY` = 200k catalog rows, ~25 granules at the default 8192
/// granularity: enough for granule-level discrimination on every shape.
const VALS_PER_KEY: u64 = 10_000;
const KEYS_PER_SCOPE: u64 = 10;
const TOTAL_ROWS: u64 = 2 * KEYS_PER_SCOPE * VALS_PER_KEY;

async fn exec(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::Idempotent)
        .await
        .unwrap_or_else(|e| panic!("execute failed: {e}\nSQL:\n{sql}"));
}

/// Seeds the catalog directly (the MV path is #54/#53's covered ground;
/// this gate is about the read shapes): both scopes carry the same ten
/// keys `k0..k9`, `VALS_PER_KEY` distinct values each — a multi-scope /
/// multi-key fixture where a scoped read genuinely has something to
/// prune away (the other scope's half).
async fn seed_catalog(client: &ChClient, db: &str) {
    for scope in ["resource", "span"] {
        exec(
            client,
            &format!(
                "INSERT INTO {db}.trace_tag_catalog (scope, key, val) \
                 SELECT '{scope}', \
                        concat('k', toString(number % {KEYS_PER_SCOPE})), \
                        concat('v', leftPad(toString(intDiv(number, {KEYS_PER_SCOPE})), 7, '0')) \
                 FROM numbers({})",
                KEYS_PER_SCOPE * VALS_PER_KEY
            ),
        )
        .await;
    }
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct ExplainRow {
    #[serde(with = "serde_bytes")]
    explain: Vec<u8>,
}

async fn explain_raw(client: &ChClient, sql: &str) -> String {
    let full = format!("EXPLAIN indexes = 1 {sql}");
    let mut out = String::new();
    let mut stream = client
        .query_stream::<ExplainRow>(&full, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("explain failed: {e}\nSQL:\n{full}"));
    while let Some(row) = stream.next().await {
        out.push_str(&String::from_utf8_lossy(
            &row.expect("decode explain row").explain,
        ));
        out.push('\n');
    }
    out
}

/// The `PrimaryKey` block's `Granules: k/N` ratio (panics with the raw
/// text when absent — the `traces_search_explain.rs` idiom).
fn primary_key_granules(raw: &str) -> (u64, u64) {
    const BLOCK_TITLES: &[&str] = &["MinMax", "Partition", "PrimaryKey", "Skip"];
    let mut in_pk = false;
    for line in raw.lines() {
        let trimmed = line.trim();
        if BLOCK_TITLES.contains(&trimmed) {
            in_pk = trimmed == "PrimaryKey";
            continue;
        }
        if in_pk && let Some(ratio) = trimmed.strip_prefix("Granules: ") {
            let (selected, total) = ratio
                .split_once('/')
                .unwrap_or_else(|| panic!("unparseable granules {trimmed:?}\n{raw}"));
            return (
                selected.trim().parse().expect("selected"),
                total.trim().parse().expect("total"),
            );
        }
    }
    panic!("no PrimaryKey Granules line in EXPLAIN output:\n{raw}");
}

/// Drains one tagged tag-names query (rows are tiny; the SQL LIMIT
/// bounds the transfer).
async fn drain_names(client: &ChClient, sql: &str, query_id: &str) -> usize {
    let settings = QuerySettings::new().set("query_id", query_id);
    let mut n = 0usize;
    let mut stream = client
        .query_stream::<TagNameRow>(sql, &settings)
        .await
        .unwrap_or_else(|e| panic!("tagged names query failed: {e}\nSQL:\n{sql}"));
    while let Some(row) = stream.next().await {
        row.expect("decode tag name row");
        n += 1;
    }
    n
}

async fn drain_values(client: &ChClient, sql: &str, query_id: &str) -> usize {
    let settings = QuerySettings::new().set("query_id", query_id);
    let mut n = 0usize;
    let mut stream = client
        .query_stream::<TagValueRow>(sql, &settings)
        .await
        .unwrap_or_else(|e| panic!("tagged values query failed: {e}\nSQL:\n{sql}"));
    while let Some(row) = stream.next().await {
        row.expect("decode tag value row");
        n += 1;
    }
    n
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct QueryLogRow {
    read_rows: u64,
}

/// The `QueryFinish` `read_rows` for an exact `query_id`.
async fn read_rows_by_id(client: &ChClient, query_id: &str) -> u64 {
    let sql = format!(
        "SELECT read_rows FROM system.query_log \
         WHERE query_id = '{query_id}' AND type = 'QueryFinish' \
         ORDER BY event_time_microseconds DESC LIMIT 1"
    );
    let mut stream = client
        .query_stream::<QueryLogRow>(&sql, &QuerySettings::new())
        .await
        .expect("query_log read");
    let mut row = None;
    while let Some(r) = stream.next().await {
        row = Some(r.expect("decode query_log row"));
    }
    row.unwrap_or_else(|| panic!("no QueryFinish row for query_id {query_id}"))
        .read_rows
}

#[tokio::test]
async fn tag_discovery_prunes_scoped_shapes_and_records_the_degraded_paths() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
             (see crates/pulsus-read/tests/traces_tags_explain.rs for setup)"
        );
        return;
    }

    let admin = ChClient::new(test_config()).await.expect("connect");
    exec(&admin, &format!("DROP DATABASE IF EXISTS {DB}")).await;
    run_init(&admin, &test_ctx(DB)).await.expect("run_init");

    let mut cfg = test_config();
    cfg.database = DB.to_string();
    let client = ChClient::new(cfg).await.expect("connect data client");
    seed_catalog(&client, DB).await;

    let resource = ch_string("resource");
    let key = ch_string("k3");
    let scoped_names = tag_names_sql("trace_tag_catalog", Some(&resource), TAG_NAMES_MAX + 1);
    let unscoped_names = tag_names_sql("trace_tag_catalog", None, TAG_NAMES_MAX + 1);
    let scoped_values = tag_values_sql(
        "trace_tag_catalog",
        &key,
        Some(&resource),
        TAG_VALUES_MAX + 1,
    );
    let unscoped_values = tag_values_sql("trace_tag_catalog", &key, None, TAG_VALUES_MAX + 1);

    // ---- Gate 1 (strict): scoped tag-names prune on the (scope) PK
    // prefix — selected strictly below the catalog total. ----------------
    let raw = explain_raw(&client, &scoped_names).await;
    let (names_sel, names_total) = primary_key_granules(&raw);
    assert!(
        names_sel > 0 && names_sel < names_total,
        "scoped tag-names must engage and strictly prune the (scope) prefix \
         ({names_sel}/{names_total}):\n{raw}"
    );

    // ---- Gate 2 (strict): scoped values prune on the (scope, key)
    // prefix — strictly fewer granules than the scoped-names shape, which
    // fixes only the scope. -----------------------------------------------
    let raw = explain_raw(&client, &scoped_values).await;
    let (values_sel, values_total) = primary_key_granules(&raw);
    assert!(
        values_sel > 0 && values_sel < values_total,
        "scoped values must engage and strictly prune the (scope, key) prefix \
         ({values_sel}/{values_total}):\n{raw}"
    );
    assert!(
        values_sel < names_sel,
        "fixing (scope, key) must prune strictly deeper than fixing (scope) alone \
         (values {values_sel} vs names {names_sel} of {names_total})"
    );

    // ---- Gate 3 (two-shape + query_log, the #53 AC3b idiom): the scoped
    // reads' physical row footprint stays strictly under the full-catalog
    // baseline's. ----------------------------------------------------------
    let n = drain_names(&client, &scoped_names, "tags-scoped-names").await;
    assert_eq!(n, KEYS_PER_SCOPE as usize, "one scope's distinct keys");
    let n = drain_names(&client, &unscoped_names, "tags-unscoped-names").await;
    assert_eq!(n, 2 * KEYS_PER_SCOPE as usize, "both scopes' distinct keys");
    let n = drain_values(&client, &scoped_values, "tags-scoped-values").await;
    assert_eq!(
        n,
        TAG_VALUES_MAX + 1,
        "k3 holds VALS_PER_KEY > cap distinct values; the SQL LIMIT ships cap + 1 (the probe)"
    );
    let n = drain_values(&client, &unscoped_values, "tags-unscoped-values").await;
    assert_eq!(n, TAG_VALUES_MAX + 1);
    exec(&client, "SYSTEM FLUSH LOGS").await;

    let scoped_names_rows = read_rows_by_id(&client, "tags-scoped-names").await;
    let baseline_rows = read_rows_by_id(&client, "tags-unscoped-names").await;
    let scoped_values_rows = read_rows_by_id(&client, "tags-scoped-values").await;
    let unscoped_values_rows = read_rows_by_id(&client, "tags-unscoped-values").await;
    assert!(
        scoped_names_rows < baseline_rows,
        "the scoped tag-names read must touch strictly fewer rows than the full-catalog \
         baseline (scoped {scoped_names_rows} vs baseline {baseline_rows})"
    );
    assert!(
        scoped_values_rows < baseline_rows,
        "the scoped values read must touch strictly fewer rows than the full-catalog \
         baseline (scoped {scoped_values_rows} vs baseline {baseline_rows})"
    );

    // ---- Gate 4 (recorded, not gated as a prune): the two degraded
    // paths. Unscoped tag-names has no predicate — the full catalog scan
    // by nature, asserted exactly. Unscoped values has no (scope) prefix
    // to prune on — measured 24.8 physics on this fixture: ClickHouse's
    // generic granule-exclusion still skips granules OPPORTUNISTICALLY
    // (within ranges where the leading `scope` is constant, `key` is
    // monotone and usable — observed 4/24 granules), so `selected ==
    // total` would pin a falsehood; the honest recorded bounds are
    // "never prunes deeper than the scoped twin, never reads fewer
    // rows". The contract stays "treat it as a full (small) catalog
    // scan" — the opportunistic exclusion is layout-dependent, not a
    // guarantee. Granule ratio + read_rows RECORDED (eprintln below). ----
    assert_eq!(
        baseline_rows, TOTAL_ROWS,
        "unscoped tag-names is a full catalog scan — it reads every distinct \
         (scope, key, val) tuple"
    );
    let raw = explain_raw(&client, &unscoped_values).await;
    let (unscoped_sel, unscoped_total) = primary_key_granules(&raw);
    eprintln!(
        "recorded degraded paths: unscoped-names read_rows={baseline_rows}/{TOTAL_ROWS}; \
         unscoped-values granules={unscoped_sel}/{unscoped_total} \
         read_rows={unscoped_values_rows}"
    );
    assert!(
        unscoped_values_rows >= scoped_values_rows,
        "the unscoped values read can never beat its scoped twin \
         (unscoped {unscoped_values_rows} vs scoped {scoped_values_rows})"
    );
    assert!(
        unscoped_sel >= values_sel && unscoped_sel <= unscoped_total,
        "unscoped values cannot prune deeper than the scoped shape \
         (unscoped {unscoped_sel} vs scoped {values_sel} of {unscoped_total})"
    );
}

// ============================================================================
// Issue #58 re-review (plan comment 5021046856): a Layer-1 read-row
// budget bounds the two catalog shapes that have no PK-prefix to prune
// (unscoped tag-names; bare-key values). This gate is ADDITIVE to the
// pruning gate above, which is untouched — that gate proves index
// engagement on raw SQL with NO budget; this one proves a tight-budget
// `TraceEngine` genuinely aborts the two unbounded shapes and still
// serves the two bounded (scoped) shapes.
// ============================================================================

/// A wide row-count gap (no near-boundary flakiness): `resource` is tiny
/// and stays comfortably under the tight budget below; `span`'s `k3`
/// alone (`SPAN_VALS_PER_KEY` rows) is well over 10x the budget, and the
/// unscoped full-catalog scan is over 10x the budget too.
const RESOURCE_VALS_PER_KEY: u64 = 5;
const SPAN_VALS_PER_KEY: u64 = 15_000;
const BUDGET_KEYS_PER_SCOPE: u64 = 10;
const BUDGET_TOTAL_ROWS: u64 =
    BUDGET_KEYS_PER_SCOPE * RESOURCE_VALS_PER_KEY + BUDGET_KEYS_PER_SCOPE * SPAN_VALS_PER_KEY;
/// Tight enough that the 50-row `resource` scope (and its 5-row `k3`
/// slice) stay far under budget, while the 150,000-row `span` scope (and
/// its 15,000-row `k3` slice, visible to the unscoped bare-key lookup)
/// blow well past it.
const TIGHT_BUDGET_ROWS: u64 = 12_000;
/// Empirically observed 24.8 physics (verified live, not assumed): a
/// `max_rows_to_read` breach on a real `MergeTree` table does NOT always
/// stop at "budget + one granule" — depending on whether the optimizer
/// can estimate the matching row count from the primary-key range before
/// reading (bare-key values here: `ExceptionBeforeStart`, `read_rows =
/// 0`) or only detects the breach mid-execution (unscoped names here:
/// `ExceptionWhileProcessing`, `read_rows` = one execution block —
/// ClickHouse's default `max_block_size` = 65,536, not the 8,192-row
/// granule). This constant is a generous bound above the observed
/// one-block overshoot, still far under `BUDGET_TOTAL_ROWS` — the
/// meaningful claim ("bounded scan, not a full scan") holds either way.
const READ_ROWS_OVERSHOOT_SLACK: u64 = 100_000;

async fn seed_budget_catalog(client: &ChClient, db: &str) {
    for (scope, vals_per_key) in [
        ("resource", RESOURCE_VALS_PER_KEY),
        ("span", SPAN_VALS_PER_KEY),
    ] {
        exec(
            client,
            &format!(
                "INSERT INTO {db}.trace_tag_catalog (scope, key, val) \
                 SELECT '{scope}', \
                        concat('k', toString(number % {BUDGET_KEYS_PER_SCOPE})), \
                        concat('v', leftPad(toString(intDiv(number, {BUDGET_KEYS_PER_SCOPE})), 7, '0')) \
                 FROM numbers({})",
                BUDGET_KEYS_PER_SCOPE * vals_per_key
            ),
        )
        .await;
    }
}

fn tight_budget_config() -> TraceReadConfig {
    TraceReadConfig {
        spans_table: "trace_spans".to_string(),
        attrs_table: "trace_attrs_idx".to_string(),
        catalog_table: "trace_tag_catalog".to_string(),
        edges_table: "trace_edges".to_string(),
        max_candidates: 100,
        scan_budget_rows: TIGHT_BUDGET_ROWS,
        max_series: 1_000,
        generator_max_memory_bytes: 536_870_912,
        distributed: false,
        skip_unavailable_shards: false,
    }
}

async fn budget_data_client(db: &str) -> ChClient {
    let mut cfg = test_config();
    cfg.database = db.to_string();
    ChClient::new(cfg)
        .await
        .expect("connect budget data client")
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct BudgetQueryLogRow {
    /// `system.query_log.type`, aliased to a non-keyword column name in
    /// the SQL (`type` is a Rust keyword).
    kind: String,
    read_rows: u64,
    exception_code: i32,
}

/// The exact `system.query_log` rows the four `TraceEngine` calls below
/// produced, in call order — matched by the byte-frozen `SELECT
/// DISTINCT` prefix (both `tags_sql` builders emit only that shape) and
/// this run's dedicated database, EXCLUDING the fixture's `INSERT`
/// statements (they don't match the `SELECT DISTINCT` prefix). Asserts
/// the row count is exactly 4 — no ambiguity about which row is which.
async fn budget_query_log_rows(admin: &ChClient, db: &str) -> Vec<BudgetQueryLogRow> {
    let sql = format!(
        "SELECT toString(type) AS kind, read_rows, exception_code FROM system.query_log \
         WHERE current_database = '{db}' AND type != 'QueryStart' \
         AND query LIKE 'SELECT DISTINCT%' \
         ORDER BY query_start_time_microseconds ASC, event_time_microseconds ASC"
    );
    let mut stream = admin
        .query_stream::<BudgetQueryLogRow>(&sql, &QuerySettings::new())
        .await
        .expect("query system.query_log");
    let mut rows = Vec::new();
    while let Some(row) = stream.next().await {
        rows.push(row.expect("decode query_log row"));
    }
    rows
}

/// AC2 (issue #58 re-review): both unscoped catalog shapes THROW
/// `QueryTooBroad(TraceScanBudgetRows)` under a tight `scan_budget_rows`
/// — never a silent unbounded scan — while both scoped shapes stay under
/// budget and keep returning `Ok`. Non-vacuous: without `catalog_settings`
/// applied (i.e. on the pre-fix `QuerySettings::new()`), both unscoped
/// calls would return `Ok` (the `LIMIT` still caps the tiny *output*,
/// masking the large *scan*) — this test fails if that budget regresses.
#[tokio::test]
async fn tag_discovery_bounds_unscoped_scans_at_the_read_budget() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
             (see crates/pulsus-read/tests/traces_tags_explain.rs for setup)"
        );
        return;
    }

    // Per-run nonce'd database (the `traces_tags_live.rs` rationale):
    // `system.query_log` outlives databases, so a fixed name would
    // aggregate rows across local re-runs and break the exact-count
    // corroboration in Gate 3 below.
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let budget_db = format!("pulsus_traces_tags_budget_it_{nonce}");
    let budget_db = budget_db.as_str();

    let admin = ChClient::new(test_config()).await.expect("connect");
    exec(&admin, &format!("DROP DATABASE IF EXISTS {budget_db}")).await;
    run_init(&admin, &test_ctx(budget_db))
        .await
        .expect("run_init");

    let seed_client = budget_data_client(budget_db).await;
    seed_budget_catalog(&seed_client, budget_db).await;

    let engine = TraceEngine::new(budget_data_client(budget_db).await, tight_budget_config());

    // ---- Gate 1: both unscoped shapes throw QueryTooBroad --------------
    match engine.list_tag_names(None).await {
        Err(ReadError::QueryTooBroad(TooBroadReason::TraceScanBudgetRows { budget_rows })) => {
            assert_eq!(budget_rows, TIGHT_BUDGET_ROWS);
        }
        other => panic!(
            "unscoped tag-names over a {BUDGET_TOTAL_ROWS}-row catalog must abort at a \
             {TIGHT_BUDGET_ROWS}-row budget, got {other:?}"
        ),
    }
    match engine.list_tag_values("k3", None).await {
        Err(ReadError::QueryTooBroad(TooBroadReason::TraceScanBudgetRows { budget_rows })) => {
            assert_eq!(budget_rows, TIGHT_BUDGET_ROWS);
        }
        other => panic!(
            "bare-key values for k3 (spanning both scopes, {SPAN_VALS_PER_KEY}+ rows in `span` \
             alone) must abort at a {TIGHT_BUDGET_ROWS}-row budget, got {other:?}"
        ),
    }

    // ---- Gate 2: both scoped shapes stay under budget -> Ok -------------
    let names = engine
        .list_tag_names(Some("resource"))
        .await
        .expect("scoped tag-names prune to the 50-row resource partition, well under budget");
    assert_eq!(names.names.len(), BUDGET_KEYS_PER_SCOPE as usize);
    assert!(!names.truncated);

    let values = engine
        .list_tag_values("k3", Some("resource"))
        .await
        .expect("scoped values prune to the 5-row resource/k3 partition, well under budget");
    assert_eq!(values.values.len(), RESOURCE_VALS_PER_KEY as usize);
    assert!(!values.truncated);

    // ---- Gate 3: query_log corroboration -- bounded scanned rows, not a
    // full scan, for the two aborted shapes (closes the re-review's TEST
    // GAP). Exactly 4 rows in call order: [unscoped-names,
    // unscoped-values, scoped-names, scoped-values]. -----------------------
    exec(&seed_client, "SYSTEM FLUSH LOGS").await;
    let rows = budget_query_log_rows(&admin, budget_db).await;
    assert_eq!(
        rows.len(),
        4,
        "expected exactly one query_log row per TraceEngine call above: {rows:?}"
    );
    let (unscoped_names, unscoped_values, scoped_names, scoped_values) =
        (&rows[0], &rows[1], &rows[2], &rows[3]);

    for aborted in [unscoped_names, unscoped_values] {
        assert_eq!(
            aborted.exception_code, 158,
            "expected the row-budget overflow code (158): {aborted:?}"
        );
        assert_ne!(
            aborted.kind, "QueryFinish",
            "an aborted query must not finalize as QueryFinish: {aborted:?}"
        );
        assert!(
            aborted.read_rows <= TIGHT_BUDGET_ROWS + READ_ROWS_OVERSHOOT_SLACK,
            "scanned rows must stay bounded near the budget, not run to the full catalog: \
             {aborted:?} (budget {TIGHT_BUDGET_ROWS}, catalog {BUDGET_TOTAL_ROWS})"
        );
        assert!(
            aborted.read_rows < BUDGET_TOTAL_ROWS,
            "an aborted query must never have scanned the whole catalog: {aborted:?}"
        );
    }
    eprintln!(
        "recorded aborted scans: unscoped-names read_rows={}; unscoped-values read_rows={} \
         (catalog {BUDGET_TOTAL_ROWS} rows, budget {TIGHT_BUDGET_ROWS})",
        unscoped_names.read_rows, unscoped_values.read_rows
    );

    // Both scoped shapes prune to the `scope = 'resource'` partition
    // (`BUDGET_KEYS_PER_SCOPE * RESOURCE_VALS_PER_KEY` rows, one granule)
    // and physically read exactly that partition — the `key = 'k3'`
    // filter on the values shape is applied AFTER the granule read, so
    // it does not shrink `read_rows` further; both are far under budget.
    let resource_partition_rows = BUDGET_KEYS_PER_SCOPE * RESOURCE_VALS_PER_KEY;
    for finished in [scoped_names, scoped_values] {
        assert_eq!(finished.kind, "QueryFinish", "{finished:?}");
        assert_eq!(finished.exception_code, 0, "{finished:?}");
        assert_eq!(finished.read_rows, resource_partition_rows, "{finished:?}");
        assert!(finished.read_rows <= TIGHT_BUDGET_ROWS);
    }
}
