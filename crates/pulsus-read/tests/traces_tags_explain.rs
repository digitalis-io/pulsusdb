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
use pulsus_read::traces::rows::{TagNameRow, TagValueRow};
use pulsus_read::traces::tags_sql::{tag_names_sql, tag_values_sql};
use pulsus_read::{TAG_NAMES_MAX, TAG_VALUES_MAX};
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
