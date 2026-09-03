//! Issue #58 AC9 (Tier-1, scale-invariant): live index gates for the
//! §4.3 tag-discovery reads against ClickHouse 26.3, on the exact SQL
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
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 clickhouse/clickhouse-server:26.3
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test traces_tags_explain
//! podman rm -f pulsus-ch-test
//! ```

use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_read::SpanFilterCtx;
use pulsus_read::logql::escape::ch_string;
use pulsus_read::logql::{ReadError, TooBroadReason};
use pulsus_read::traces::rows::{TagNameRow, TagValueRow};
use pulsus_read::traces::tag_narrow::narrowing_from_query;
use pulsus_read::traces::tags_sql::{
    DaySpan, attr_values_narrowed_sql, span_name_values_sql, tag_names_sql, tag_values_sql,
};
use pulsus_read::{TAG_NAMES_MAX, TAG_VALUES_MAX, TraceEngine, TraceReadConfig};
use pulsus_schema::{RenderCtx, SchemaParams, run_init};

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; **panics** rather than skipping when
/// the gate is absent in a live CI job, so a lost `env:` block reddens the
/// build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
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

static DB: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_traces_tags_it");
/// Distinct values per (scope, key) — 10 keys × 2 scopes ×
/// `VALS_PER_KEY` = 200k catalog rows, ~25 granules at the default 8192
/// granularity: enough for granule-level discrimination on every shape.
const VALS_PER_KEY: u64 = 10_000;
const KEYS_PER_SCOPE: u64 = 10;
/// The two ATTRIBUTE scopes' half of the fixture — what a bare-key or
/// unscoped read is allowed to touch after issue #475.
const ATTR_SCOPE_ROWS: u64 = 2 * KEYS_PER_SCOPE * VALS_PER_KEY;
/// The two writer-RESERVED intrinsic scopes' half. Same size, so the
/// prune has something substantial to exclude and a read that forgot the
/// `scope IN` predicate doubles its row count instead of moving it
/// slightly.
const RESERVED_SCOPE_ROWS: u64 = 2 * KEYS_PER_SCOPE * VALS_PER_KEY;
const TOTAL_ROWS: u64 = ATTR_SCOPE_ROWS + RESERVED_SCOPE_ROWS;
/// One ClickHouse granule. `index_granularity` is unset in the catalog's
/// DDL (`crates/pulsus-schema/src/catalog.rs`), so the default 8192
/// applies; the unscoped read may cross ONE boundary between the
/// contiguous reserved range and the attribute range.
const GRANULE_ROWS: u64 = 8_192;

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
    // The two writer-reserved intrinsic scopes carry the same key/value
    // shape, with values prefixed `a` so they sort BEFORE the attribute
    // scopes' `v` values: a read that failed to exclude them returns
    // their rows first, which the content checks below can see.
    for scope in ["event:intrinsic", "link:intrinsic", "resource", "span"] {
        let prefix = if scope.ends_with(":intrinsic") {
            'a'
        } else {
            'v'
        };
        exec(
            client,
            &format!(
                "INSERT INTO {db}.trace_tag_catalog (scope, key, val) \
                 SELECT '{scope}', \
                        concat('k', toString(number % {KEYS_PER_SCOPE})), \
                        concat('{prefix}', leftPad(toString(intDiv(number, {KEYS_PER_SCOPE})), 7, '0')) \
                 FROM numbers({})",
                KEYS_PER_SCOPE * VALS_PER_KEY
            ),
        )
        .await;
    }
}

/// `system.tables`'s two key columns (issue #476 AC6).
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct KeysRow {
    primary_key: String,
    sorting_key: String,
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

/// Returns the VALUES, not just the count: issue #475's content check
/// asks which rows came back, and a count alone cannot see a reserved
/// scope's row substituted for an attribute one.
async fn drain_values(client: &ChClient, sql: &str, query_id: &str) -> Vec<String> {
    let settings = QuerySettings::new().set("query_id", query_id);
    let mut out = Vec::new();
    let mut stream = client
        .query_stream::<TagValueRow>(sql, &settings)
        .await
        .unwrap_or_else(|e| panic!("tagged values query failed: {e}\nSQL:\n{sql}"));
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode tag value row").val);
    }
    out
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct QueryLogRow {
    read_rows: u64,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CountRow {
    n: u64,
}

/// The fixture's own row count, read directly rather than derived from
/// the row constants.
async fn catalog_count(client: &ChClient) -> u64 {
    let mut stream = client
        .query_stream::<CountRow>(
            "SELECT toUInt64(count()) AS n FROM trace_tag_catalog",
            &QuerySettings::new(),
        )
        .await
        .expect("catalog count");
    let mut n = 0;
    while let Some(row) = stream.next().await {
        n = row.expect("decode count row").n;
    }
    n
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
    run_init(&admin, &test_ctx(&DB)).await.expect("run_init");

    let mut cfg = test_config();
    cfg.database = DB.to_string();
    let client = ChClient::new(cfg).await.expect("connect data client");
    seed_catalog(&client, &DB).await;

    // ---- Issue #476 AC6: migration 41 APPENDS to the sorting key and
    // leaves the primary key alone. Asserted here, immediately before the
    // prune gates below, because the prune gates are exactly what a
    // replacing (rather than appending) `MODIFY ORDER BY` would destroy —
    // and asserted as TWO separate string comparisons, so a change that
    // moved both would have to move both to exactly these values. ---------
    let mut stream = client
        .query_stream::<KeysRow>(
            &format!(
                "SELECT primary_key, sorting_key FROM system.tables \
                 WHERE database = '{DB}' AND name = 'trace_tag_catalog'"
            ),
            &QuerySettings::default(),
        )
        .await
        .expect("read system.tables");
    let keys = stream
        .next()
        .await
        .expect("trace_tag_catalog must exist")
        .expect("decode system.tables row");
    drop(stream);
    assert_eq!(
        keys.primary_key, "scope, key, val",
        "migration 41 must leave the primary key alone — it is what prunes every tag read"
    );
    assert_eq!(
        keys.sorting_key, "scope, key, val, val_type",
        "migration 41 must APPEND val_type to the sorting key"
    );

    let resource = ch_string("resource");
    let key = ch_string("k3");
    let scoped_names = tag_names_sql(Some(&resource), TAG_NAMES_MAX + 1);
    let unscoped_names = tag_names_sql(None, TAG_NAMES_MAX + 1);
    let scoped_values = tag_values_sql(&key, Some(&resource), TAG_VALUES_MAX + 1);
    let unscoped_values = tag_values_sql(&key, None, TAG_VALUES_MAX + 1);

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
    // The fixture really holds the reserved rows — asserted against a
    // literal, separately from every ratio below, so a redefinition of
    // the row constants cannot make the prune assertions vacuous.
    assert_eq!(
        catalog_count(&client).await,
        400_000,
        "the fixture seeds four scopes: two attribute, two reserved"
    );
    assert_eq!(TOTAL_ROWS, 400_000, "the constants describe the fixture");

    let n = drain_names(&client, &scoped_names, "tags-scoped-names").await;
    assert_eq!(n, KEYS_PER_SCOPE as usize, "one scope's distinct keys");
    // CONTENT check (issue #475): the unscoped listing carries the two
    // ATTRIBUTE scopes' keys only. With the reserved scopes included it
    // would be 40.
    let n = drain_names(&client, &unscoped_names, "tags-unscoped-names").await;
    assert_eq!(
        n,
        2 * KEYS_PER_SCOPE as usize,
        "the two attribute scopes' distinct keys, and NOT the two reserved scopes'"
    );
    let values = drain_values(&client, &scoped_values, "tags-scoped-values").await;
    assert_eq!(
        values.len(),
        TAG_VALUES_MAX + 1,
        "k3 holds VALS_PER_KEY > cap distinct values; the SQL LIMIT ships cap + 1 (the probe)"
    );
    let values = drain_values(&client, &unscoped_values, "tags-unscoped-values").await;
    assert_eq!(values.len(), TAG_VALUES_MAX + 1);
    // CONTENT check: the reserved scopes' values are prefixed `a` and
    // sort first, so a bare-key read that forgot the `scope IN`
    // predicate returns them at the head of this very list.
    assert!(
        values.iter().all(|v| !v.starts_with('a')),
        "a reserved-scope value reached a bare-key lookup: {:?}",
        values
            .iter()
            .filter(|v| v.starts_with('a'))
            .take(5)
            .collect::<Vec<_>>()
    );
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

    // ---- Gate 4: the unscoped shapes. Since issue #475 the unscoped
    // tag-names read carries `WHERE scope IN (…)` on the catalog's
    // LEADING primary-key column, so it prunes the two reserved scopes
    // away instead of scanning the whole table — bounded below by the
    // attribute half and above by one granule of boundary overshoot. Unscoped values has no (scope) prefix
    // to prune on — measured 24.8 physics on this fixture: ClickHouse's
    // generic granule-exclusion still skips granules OPPORTUNISTICALLY
    // (within ranges where the leading `scope` is constant, `key` is
    // monotone and usable — observed 4/24 granules), so `selected ==
    // total` would pin a falsehood; the honest recorded bounds are
    // "never prunes deeper than the scoped twin, never reads fewer
    // rows". The contract stays "treat it as a full (small) catalog
    // scan" — the opportunistic exclusion is layout-dependent, not a
    // guarantee. Granule ratio + read_rows RECORDED (eprintln below). ----
    assert!(
        (ATTR_SCOPE_ROWS..=ATTR_SCOPE_ROWS + GRANULE_ROWS).contains(&baseline_rows),
        "the unscoped tag-names read prunes to the attribute scopes: it must read their \
         rows and at most one granule more, never the whole catalog \
         (read {baseline_rows}, attribute half {ATTR_SCOPE_ROWS}, catalog {TOTAL_ROWS})"
    );
    assert!(
        baseline_rows < TOTAL_ROWS,
        "the unscoped tag-names read must not be a full catalog scan \
         (read {baseline_rows} of {TOTAL_ROWS})"
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
        // Issue #398: the per-query ClickHouse memory ceiling; the
        // production default, so this fixture keeps today's behaviour.
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        spans_table: "trace_spans".to_string(),
        attrs_table: "trace_attrs_idx".to_string(),
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
    let budget_db = pulsus_testkit::test_db(&format!("pulsus_traces_tags_budget_it_{nonce}"));
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
    match engine
        .list_tag_values("k3", None, unnarrowed_values_request())
        .await
    {
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
        .list_tag_values("k3", Some("resource"), unnarrowed_values_request())
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

/// Issue #478 added a request argument to `list_tag_values`. With no `q`
/// the read is byte-identical to the pre-#478 catalog read, so the window
/// is inert here; it is still a real one rather than zeros so nothing
/// depends on an unrepresentable value.
fn unnarrowed_values_request() -> pulsus_read::TagValuesRequest<'static> {
    pulsus_read::TagValuesRequest {
        q: None,
        start_ns: 1_700_000_000_000_000_000,
        end_ns: 1_700_003_600_000_000_000,
    }
}

// ============================================================================
// Issue #478 (Tier-1, scale-invariant): the two STORE-BACKED tag-value
// reads.
//
// Every assertion here is a RELATION — a plan node's identity, a strict
// inequality, an equality between two plans, or an identity between two
// counts. No absolute granule count, byte count or ratio is pinned:
// denominators move with part layout and with corpus, and the
// architect's own controls moved from 733 to 1223 on the same query when
// the corpus grew. The identities are the finding.
// ============================================================================

static SPAN_DB: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_traces_spans_it");

/// Spans seeded for the store-backed gates: enough rows to cross several
/// granules at the default 8192 granularity, so a prune has something to
/// exclude.
const SPAN_ROWS: u64 = 400_000;
/// Distinct span names, and distinct services.
const SPAN_NAMES: u64 = 500;
const SPAN_SERVICES: u64 = 50;
/// Two index rows per span, so a `(trace_id, span_id)` set built from the
/// index is non-empty against the span table.
const ATTR_ROWS: u64 = 2 * SPAN_ROWS;
const ATTR_KEYS: u64 = 20;
const ATTR_VALS: u64 = 1_000;

/// One UTC day, an hour ago — derived from the clock, never a literal.
/// The span tables carry a retention TTL with `ttl_only_drop_parts = 1`,
/// so a literal timestamp older than retention would be dropped at
/// INSERT and every gate below would read an empty table and still pass
/// its "reads fewer granules" comparison vacuously.
fn span_base_ns() -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs() as i64;
    (now - 3_600) * 1_000_000_000
}

async fn seed_spans(client: &ChClient, db: &str, base_ns: i64) {
    exec(
        client,
        &format!(
            "INSERT INTO {db}.trace_spans \
             (trace_id, span_id, parent_id, name, service, timestamp_ns, duration_ns, \
              status_code, kind, payload_type, payload) \
             SELECT reinterpretAsFixedString(cityHash64(number)), \
                    reinterpretAsFixedString(toUInt64(number)), \
                    reinterpretAsFixedString(toUInt64(0)), \
                    concat('op.', toString(number % {SPAN_NAMES})), \
                    concat('svc-', leftPad(toString(number % {SPAN_SERVICES}), 3, '0')), \
                    {base_ns} + number * 100, 1500000, 1, 2, 0, '' \
             FROM numbers({SPAN_ROWS})"
        ),
    )
    .await;
    exec(
        client,
        &format!(
            "INSERT INTO {db}.trace_attrs_idx \
             (date, key, val, scope, val_num, timestamp_ns, trace_id, span_id, duration_ns, \
              val_type) \
             SELECT toDate(fromUnixTimestamp64Nano({base_ns})), \
                    concat('k', toString(number % {ATTR_KEYS})), \
                    concat('v', toString(number % {ATTR_VALS})), \
                    'span', NULL, {base_ns}, \
                    reinterpretAsFixedString(cityHash64(number % {SPAN_ROWS})), \
                    reinterpretAsFixedString(toUInt64(number % {SPAN_ROWS})), \
                    1500000, 'string' \
             FROM numbers({ATTR_ROWS})"
        ),
    )
    .await;
}

/// The name of the table or projection an `EXPLAIN` plan reads from.
fn read_source(raw: &str) -> String {
    for line in raw.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("ReadFromMergeTree (") {
            return rest.trim_end_matches(')').to_string();
        }
    }
    panic!("no ReadFromMergeTree node in EXPLAIN output:\n{raw}");
}

/// The PrimaryKey block's `Condition:` line.
fn primary_key_condition(raw: &str) -> String {
    const BLOCK_TITLES: &[&str] = &["MinMax", "Partition", "PrimaryKey", "Skip"];
    let mut in_pk = false;
    for line in raw.lines() {
        let trimmed = line.trim();
        if BLOCK_TITLES.contains(&trimmed) {
            in_pk = trimmed == "PrimaryKey";
            continue;
        }
        if in_pk && let Some(cond) = trimmed.strip_prefix("Condition: ") {
            return cond.to_string();
        }
    }
    panic!("no PrimaryKey Condition line in EXPLAIN output:\n{raw}");
}

/// The MinMax/Partition/PrimaryKey `Granules: k/N` line of whichever
/// block appears LAST — the count after every index has been applied.
fn final_granules(raw: &str) -> (u64, u64) {
    let mut last = None;
    for line in raw.lines() {
        if let Some(ratio) = line.trim().strip_prefix("Granules: ") {
            let (selected, total) = ratio
                .split_once('/')
                .unwrap_or_else(|| panic!("unparseable granules {ratio:?}\n{raw}"));
            last = Some((
                selected.trim().parse().expect("selected"),
                total.trim().parse().expect("total"),
            ));
        }
    }
    last.unwrap_or_else(|| panic!("no Granules line in EXPLAIN output:\n{raw}"))
}

/// Issue #478, criterion 1. **The day-grain projection serves the
/// unnarrowed span-name read, and the day expression is what selects
/// it.**
///
/// Two halves, and the second is the discriminator: a plan that named
/// `span_name_day` for every window would pass the first half alone.
/// Expressing the same window on `timestamp_ns` instead — the obvious
/// alternative, and the one a later reader is most likely to try —
/// defeats the projection, which is exactly why
/// `tags_sql::span_name_values_sql` carries only the day clause.
#[tokio::test]
async fn span_name_projection_is_selected_and_prunes() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see this file's module docs)");
        return;
    }
    let admin = ChClient::new(test_config()).await.expect("connect");
    exec(&admin, &format!("DROP DATABASE IF EXISTS {SPAN_DB}")).await;
    run_init(&admin, &test_ctx(&SPAN_DB))
        .await
        .expect("run_init");
    let mut cfg = test_config();
    cfg.database = SPAN_DB.to_string();
    let client = ChClient::new(cfg).await.expect("connect data client");
    let base_ns = span_base_ns();
    seed_spans(&client, &SPAN_DB, base_ns).await;
    exec(
        &client,
        &format!(
            "ALTER TABLE {SPAN_DB}.trace_spans MATERIALIZE PROJECTION span_name_day \
             SETTINGS mutations_sync = 2"
        ),
    )
    .await;

    let ctx = SpanFilterCtx {
        spans_table: "trace_spans",
        attrs_table: "trace_attrs_idx",
    };
    let days = DaySpan::from_window(base_ns, base_ns);
    let sql = span_name_values_sql(ctx, days, &[], TAG_VALUES_MAX + 1);

    // (a) the projection is the plan.
    let raw = explain_raw(&client, &sql).await;
    assert_eq!(
        read_source(&raw),
        "span_name_day",
        "the unnarrowed span-name read must be served by the day-grain projection:\n{raw}"
    );
    let (selected, total) = final_granules(&raw);

    // (b) the same query WITHOUT the projection reads the base table, and
    //     reads strictly more granules out of the same denominator.
    let base_raw = explain_raw(
        &client,
        &format!("{sql} SETTINGS optimize_use_projections = 0"),
    )
    .await;
    assert_eq!(
        read_source(&base_raw),
        format!("{SPAN_DB}.trace_spans"),
        "the control must read the base table:\n{base_raw}"
    );
    let (base_selected, base_total) = final_granules(&base_raw);
    assert_eq!(
        total, base_total,
        "the two plans must share a denominator, or the comparison is not a comparison"
    );
    assert!(
        selected < base_selected,
        "the projection must select strictly fewer granules ({selected}/{total}) than the \
         base-table read ({base_selected}/{base_total})"
    );

    // (c) the discriminator: the same window on `timestamp_ns` is NOT
    //     served by the projection.
    // The same read with the window expressed on `timestamp_ns` — the
    // one line of the builder's output that decides this.
    let ts_sql = sql
        .lines()
        .map(|line| {
            if line.starts_with("WHERE ") {
                format!(
                    "WHERE timestamp_ns >= {base_ns} AND timestamp_ns <= {}",
                    base_ns + 86_400_000_000_000i64
                )
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    let ts_raw = explain_raw(&client, &ts_sql).await;
    assert_ne!(
        read_source(&ts_raw),
        "span_name_day",
        "a window expressed on timestamp_ns must NOT select the day-grain projection — that \
         is why the builder carries only the day clause:\n{ts_raw}"
    );

    // Criterion 13: the projection's row count is the distinct
    // `(day, name)` count of the base table. An identity, so it holds at
    // any scale; perturbing either side breaks it.
    exec(
        &client,
        &format!("OPTIMIZE TABLE {SPAN_DB}.trace_spans FINAL"),
    )
    .await;
    let projected = scalar(
        &client,
        &format!(
            "SELECT toUInt64(sum(rows)) AS n FROM system.projection_parts \
             WHERE database = '{SPAN_DB}' AND table = 'trace_spans' \
               AND name = 'span_name_day' AND active"
        ),
    )
    .await;
    let distinct = scalar(
        &client,
        &format!(
            "SELECT toUInt64(uniqExact((toDate(fromUnixTimestamp64Nano(timestamp_ns)), name))) \
             AS n FROM {SPAN_DB}.trace_spans"
        ),
    )
    .await;
    assert_eq!(
        projected, distinct,
        "the projection holds one row per distinct (UTC day, name) pair"
    );
    assert_eq!(
        distinct, SPAN_NAMES,
        "the fixture must have the distinct names it claims, or the identity above is vacuous"
    );

    exec(&admin, &format!("DROP DATABASE IF EXISTS {SPAN_DB}")).await;
}

async fn scalar(client: &ChClient, sql: &str) -> u64 {
    let mut stream = client
        .query_stream::<CountRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("scalar query failed: {e}\nSQL:\n{sql}"));
    let mut n = 0;
    while let Some(row) = stream.next().await {
        n = row.expect("decode count row").n;
    }
    n
}

/// Issue #478, criterion 2. **What prunes a narrowed read, with the
/// control that removes each proposed cause.**
///
/// The property this replaces was FALSE, and its own control refuted it:
/// the semi-join was assumed to prune and does not. What it is, is a
/// CORRECTNESS mechanism — the answer would be wrong without it — and
/// asserting it as a pruning one would have pinned a claim the plan
/// cannot keep.
///
/// **Scope of the two statements, because they are not the same
/// statement** (this is the distinction an earlier revision of this plan
/// got wrong by generalising one table's measurement to the join):
///
/// * On `trace_attrs_idx` the set cannot prune STRUCTURALLY, for any
///   set: the key is `(key, val, scope, timestamp_ns, trace_id, span_id)`,
///   so with `val` and `timestamp_ns` unconstrained the identifier
///   columns sit behind an open range.
/// * On `trace_spans`, `trace_id` LEADS the key and a set CAN exclude
///   granules — a localised five-trace set read 1/245 and a scattered
///   five-trace set 5/245, the latter measured independently by plan
///   review round 6 and by code review round 1 on their own
///   2,000,000-span corpora. The sets this feature produces do not
///   exclude anything, because they are large and scattered enough to
///   intersect every granule (the real 333k-trace set read 245/245).
///   That is a property of the workload, not of the schema, and this
///   test asserts nothing about it — deliberately: the scattered figure
///   is corpus-shaped, and an earlier revision quoted `9/245` from a
///   third corpus as though it were the property.
#[tokio::test]
async fn narrowed_reads_prune_on_key_and_partition_not_on_the_set() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see this file's module docs)");
        return;
    }
    let admin = ChClient::new(test_config()).await.expect("connect");
    let db = pulsus_testkit::test_db("pulsus_traces_narrow_explain_it");
    let db = db.as_str();
    exec(&admin, &format!("DROP DATABASE IF EXISTS {db}")).await;
    run_init(&admin, &test_ctx(db)).await.expect("run_init");
    let mut cfg = test_config();
    cfg.database = db.to_string();
    let client = ChClient::new(cfg).await.expect("connect data client");
    let base_ns = span_base_ns();
    seed_spans(&client, db, base_ns).await;
    exec(
        &client,
        &format!(
            "ALTER TABLE {db}.trace_spans MATERIALIZE PROJECTION span_name_day \
             SETTINGS mutations_sync = 2"
        ),
    )
    .await;

    let ctx = SpanFilterCtx {
        spans_table: "trace_spans",
        attrs_table: "trace_attrs_idx",
    };
    let days = DaySpan::from_window(base_ns, base_ns);
    let terms = narrowing_from_query("{resource.service.name=\"svc-007\"}");
    assert!(!terms.is_empty(), "the fixture query must lower to a term");
    let key = ch_string("k3");
    let scope = ch_string("span");
    let narrowed = attr_values_narrowed_sql(
        ctx,
        &key,
        Some(&scope),
        days,
        terms.terms(),
        TAG_VALUES_MAX + 1,
    );

    // 2c — the set is present in the primary-key Condition. Its absence
    // would be a CORRECTNESS regression even though its presence buys no
    // pruning, so it is asserted on the text.
    let raw = explain_raw(&client, &narrowed).await;
    let condition = primary_key_condition(&raw);
    assert!(
        condition.contains("(trace_id, span_id) in"),
        "the semi-join must reach the primary-key condition: {condition}"
    );
    assert!(
        condition.contains("element set"),
        "the set must be a materialized set, not a constant-folded predicate: {condition}"
    );

    // 2a — the control: remove the set and the granule count does not
    // move; remove the `key` predicate and it does.
    let set_free = narrowed
        .lines()
        .take_while(|l| !l.trim_start().starts_with("AND (trace_id, span_id) IN ("))
        .collect::<Vec<_>>()
        .join("\n")
        + "\nORDER BY val, val_type\nLIMIT 1001";
    let set_free_raw = explain_raw(&client, &set_free).await;
    let (with_set, total) = final_granules(&raw);
    let (without_set, total_free) = final_granules(&set_free_raw);
    assert_eq!(total, total_free, "the two plans must share a denominator");
    assert_eq!(
        with_set, without_set,
        "the semi-join is a correctness mechanism, not a pruning one: adding it must not \
         change the granule count ({with_set}/{total} against {without_set}/{total_free})"
    );

    let key_free = narrowed.replace(&format!("WHERE key = {key} AND"), "WHERE");
    assert_ne!(
        key_free, narrowed,
        "the key predicate must have been removed"
    );
    let key_free_raw = explain_raw(&client, &key_free).await;
    let (without_key, total_key_free) = final_granules(&key_free_raw);
    assert_eq!(total, total_key_free, "same denominator again");
    assert!(
        with_set < without_key,
        "the `(key)` prefix is what prunes: {with_set}/{total} with it against \
         {without_key}/{total_key_free} without it"
    );

    // 2b — a physical term selects the `service_time` projection, and
    // that is where ITS pruning comes from: with projections off, the
    // same term prunes nothing, because `trace_spans` is ordered
    // `(trace_id, timestamp_ns)` and `service` leads neither key.
    let span_narrowed = span_name_values_sql(ctx, days, terms.terms(), TAG_VALUES_MAX + 1);
    let span_raw = explain_raw(&client, &span_narrowed).await;
    assert_eq!(
        read_source(&span_raw),
        "service_time",
        "a physical service term must select the service_time projection:\n{span_raw}"
    );
    let (with_term, span_total) = final_granules(&span_raw);
    let base_raw = explain_raw(
        &client,
        &format!("{span_narrowed} SETTINGS optimize_use_projections = 0"),
    )
    .await;
    let (base_selected, base_total) = final_granules(&base_raw);
    assert_eq!(span_total, base_total, "same denominator");
    assert!(
        with_term < base_selected,
        "the projection is what the physical term buys: {with_term}/{span_total} against \
         {base_selected}/{base_total} with projections disabled"
    );

    exec(&admin, &format!("DROP DATABASE IF EXISTS {db}")).await;
}

// ============================================================================
// Issue #509: an attribute key containing `?`.
//
// The catalog read inlines the requested KEY as a SQL literal, and the
// ClickHouse driver we vendor reads a bare `?` in query text as a bind
// placeholder. The fix doubles every `?` at one choke point
// (`traces::dispatch`), which the driver collapses back before the
// statement reaches the server — so the read must still be the same
// index-served read it was for a `?`-free key, and this gate is what
// says so.
// ============================================================================

static QM_DB: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_traces_tags_qm_it");

/// The `?`-bearing key. Stored under `span` with `v`-prefixed values,
/// and under a writer-RESERVED intrinsic scope with `a`-prefixed ones —
/// so a read that lost its scope predicate returns the reserved rows
/// FIRST and the content check below sees it.
const QM_KEY: &str = "a?b";
const QM_KEYS_PER_SCOPE: u64 = 10;
const QM_VALS_PER_KEY: u64 = 10_000;

async fn seed_qm_catalog(client: &ChClient, db: &str) {
    for scope in ["resource", "span"] {
        exec(
            client,
            &format!(
                "INSERT INTO {db}.trace_tag_catalog (scope, key, val) \
                 SELECT '{scope}', \
                        concat('k', toString(number % {QM_KEYS_PER_SCOPE})), \
                        concat('v', leftPad(toString(intDiv(number, {QM_KEYS_PER_SCOPE})), 7, '0')) \
                 FROM numbers({})",
                QM_KEYS_PER_SCOPE * QM_VALS_PER_KEY
            ),
        )
        .await;
    }
    for (scope, prefix) in [("span", 'v'), ("event:intrinsic", 'a')] {
        exec(
            client,
            &format!(
                "INSERT INTO {db}.trace_tag_catalog (scope, key, val) \
                 SELECT '{scope}', '{QM_KEY}', \
                        concat('{prefix}', leftPad(toString(number), 7, '0')) \
                 FROM numbers({QM_VALS_PER_KEY})"
            ),
        )
        .await;
    }
}

fn qm_config() -> TraceReadConfig {
    TraceReadConfig {
        scan_budget_rows: 10_000_000,
        ..tight_budget_config()
    }
}

/// Issue #509, criterion 9: the unnarrowed values read for a key
/// containing `?` still prunes strictly on the `(scope, key)`
/// primary-key prefix, and still answers.
///
/// Two halves, and they check different things. The EXPLAIN half is the
/// index claim: `selected < total` on the `PrimaryKey` block, the same
/// strict prune the `?`-free case above asserts. The engine half is that
/// the read RUNS — before the fix it was
/// `500 … invalid SQL: unbound query argument`, so a granule ratio
/// alone would have been a claim about SQL nobody could execute.
#[tokio::test]
async fn unnarrowed_values_for_a_question_mark_key_prune_and_answer() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
             (see crates/pulsus-read/tests/traces_tags_explain.rs for setup)"
        );
        return;
    }

    let admin = ChClient::new(test_config()).await.expect("connect");
    exec(&admin, &format!("DROP DATABASE IF EXISTS {QM_DB}")).await;
    run_init(&admin, &test_ctx(&QM_DB)).await.expect("run_init");

    let mut cfg = test_config();
    cfg.database = QM_DB.to_string();
    let client = ChClient::new(cfg).await.expect("connect data client");
    seed_qm_catalog(&client, &QM_DB).await;

    let scope = ch_string("span");
    let key = ch_string(QM_KEY);
    let sql = tag_values_sql(&key, Some(&scope), TAG_VALUES_MAX + 1);
    assert!(
        sql.contains(QM_KEY),
        "the builder must inline the key as a literal, or this gate tests nothing:\n{sql}"
    );

    // The EXPLAIN goes over this test's OWN driver hop, which applies
    // the same placeholder rule the engine's dispatcher exists for — so
    // the `?` is doubled here too. What ClickHouse parses is therefore
    // the identical statement either way; the doubling is a wire
    // encoding, not a change of query.
    let raw = explain_raw(&client, &sql.replace('?', "??")).await;
    let (selected, total) = primary_key_granules(&raw);
    assert!(
        selected > 0 && selected < total,
        "a `?`-bearing key must still engage and strictly prune the (scope, key) prefix \
         ({selected}/{total}):\n{raw}"
    );

    // And the read answers, through the engine, over the dispatcher.
    let engine = TraceEngine::new(
        {
            let mut cfg = test_config();
            cfg.database = QM_DB.to_string();
            ChClient::new(cfg).await.expect("connect engine client")
        },
        qm_config(),
    );
    let values = engine
        .list_tag_values(QM_KEY, Some("span"), unnarrowed_values_request())
        .await
        .expect("a `?` in the key is data: the read must answer, not fail the driver");
    assert_eq!(
        values.values.len(),
        TAG_VALUES_MAX + 1,
        "the key holds more than the cap, so the SQL LIMIT ships cap + 1 (the probe)"
    );
    assert!(values.truncated);
    assert!(
        values.values.iter().all(|v| !v.val.starts_with('a')),
        "a reserved-scope value reached the scoped lookup: {:?}",
        values
            .values
            .iter()
            .filter(|v| v.val.starts_with('a'))
            .take(5)
            .map(|v| v.val.as_str())
            .collect::<Vec<_>>()
    );

    exec(&admin, &format!("DROP DATABASE IF EXISTS {QM_DB}")).await;
}
