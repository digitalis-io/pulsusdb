//! Issue #458 AC 3b: the **answer** the `nestedSetParent` lowering
//! produces, against live ClickHouse, on a corpus whose shape is asserted
//! before any answer is.
//!
//! AC 3a (`traces_metrics_nested_set_grid.rs`) pins the accept surface —
//! status and exact body — and it was measured that it CANNOT see either
//! of the answer corruptions this suite exists for: rendering the root
//! test as the constant `1`, and inverting its polarity, both leave every
//! probe's accept/reject verdict exactly where it was. This is the truth
//! oracle; `traces_metrics_explain.rs`'s AC 6 block is the cost oracle.
//! **Neither substitutes for the other** — a planner that returned `{}`'s
//! SQL for `{ nestedSetParent<0 }` would pass AC 6 with a perfect pruning
//! ratio and fail this suite at `S` against a required `T`.
//!
//! Both queries are built with `plan_trace_metrics` and executed through
//! `TraceEngine`, never over hand-assembled SQL: the three totals are
//! properties of the CORPUS and survive any bucketing, but only the
//! planner's own output can show what the planner actually emitted.
//!
//! # The fixture's populations are deliberately unequal
//!
//! `T`, `S − T` and `S` are asserted **pairwise distinct** before any
//! decision assertion, and so are the `flag=true`/`flag=false`
//! populations. This is not decoration. A symmetric bare-truthiness
//! fixture (40 000 `true`, 40 000 `false`) was built during planning and
//! the inverted lowering answered **40 000 either way** — the break was
//! invisible, and a symmetric corpus is exactly what a reasonable person
//! would build.
//!
//! Live-gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 clickhouse/clickhouse-server:26.3
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test traces_metrics_nested_set_live
//! podman rm -f pulsus-ch-test
//! ```

use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_read::traces::metrics_plan::{MetricsParams, plan_trace_metrics};
use pulsus_read::{TraceEngine, TraceReadConfig};
use pulsus_schema::{RenderCtx, SchemaParams, run_init};

/// `true` when this suite should run. Skips cleanly on a developer
/// machine with no container; **panics** rather than skipping when the
/// gate is absent in a live CI job, so a lost `env:` block reddens the
/// build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

static DB: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_traces_ns_metrics_it");

/// Traces in the nested-set corpus — one parentless span each.
const TRACES: u64 = 2_000;
/// Spans per trace: one root plus four children.
const SPANS_PER_TRACE: u64 = 5;
/// Total spans. `TRACES` (2 000), `SPANS − TRACES` (8 000) and `SPANS`
/// (10 000) are pairwise distinct — asserted below, not assumed.
const SPANS: u64 = TRACES * SPANS_PER_TRACE;

/// `flag = true` spans in the bare-truthiness corpus.
const FLAG_TRUE: u64 = 300;
/// `flag = false` spans. Deliberately ≠ `FLAG_TRUE`.
const FLAG_FALSE: u64 = 700;
const FLAG_SPANS: u64 = FLAG_TRUE + FLAG_FALSE;

const NS_PER_S: i64 = 1_000_000_000;
/// Each corpus occupies its own hour-wide window; the two never overlap,
/// so a window-bounded query sees exactly one of them.
const WINDOW_NS: i64 = 3_600 * NS_PER_S;

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

fn engine_config() -> TraceReadConfig {
    TraceReadConfig {
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        spans_table: "trace_spans".to_string(),
        attrs_table: "trace_attrs_idx".to_string(),
        edges_table: "trace_edges".to_string(),
        max_candidates: 100_000,
        scan_budget_rows: 50_000_000,
        max_series: 1_000,
        generator_max_memory_bytes: 536_870_912,
        distributed: false,
        skip_unavailable_shards: false,
    }
}

fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits i64")
}

async fn exec(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::Idempotent)
        .await
        .unwrap_or_else(|e| panic!("execute failed: {e}\nSQL:\n{sql}"));
}

async fn scalar(client: &ChClient, sql: &str) -> u64 {
    use futures::StreamExt;
    #[derive(pulsus_clickhouse::Row, serde::Serialize, serde::Deserialize, Debug)]
    struct N {
        n: u64,
    }
    let mut stream = client
        .query_stream::<N>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("scalar query failed: {e}\nSQL:\n{sql}"));
    let mut out = None;
    while let Some(row) = stream.next().await {
        out = Some(row.expect("decode scalar row").n);
    }
    out.unwrap_or_else(|| panic!("no row for\nSQL:\n{sql}"))
}

async fn data_client() -> ChClient {
    let mut cfg = test_config();
    cfg.database = DB.to_string();
    ChClient::new(cfg).await.expect("connect data client")
}

/// `TRACES` traces of `SPANS_PER_TRACE` spans each, over `[base, base +
/// WINDOW_NS)`. Ids are offset by 1 so no span id is the all-zero
/// `FixedString(8)` — a root whose OWN id was zero would make its children
/// look parentless, which the validity gate below caught when they were
/// not offset. Span `n` belongs to trace `n / SPANS_PER_TRACE`; the
/// first span of each trace carries the all-zero `parent_id` (our "no
/// parent" convention, the same one the reference's `IsRoot` reads) and
/// every other span points at it. So each trace has **exactly one**
/// parentless span, which is the identity the totals below rest on.
async fn seed_nested_set_corpus(client: &ChClient, base_ns: i64) {
    let spread = WINDOW_NS / SPANS as i64;
    exec(
        client,
        &format!(
            "INSERT INTO {DB}.trace_spans \
             (trace_id, span_id, parent_id, name, service, timestamp_ns, duration_ns, \
              status_code, kind, payload_type, payload) \
             SELECT \
               toFixedString(unhex(leftPad(lower(hex(intDiv(number, {SPANS_PER_TRACE}) + 1)), 32, '0')), 16), \
               toFixedString(unhex(leftPad(lower(hex(number + 1)), 16, '0')), 8), \
               if(number % {SPANS_PER_TRACE} = 0, \
                  toFixedString(unhex('0000000000000000'), 8), \
                  toFixedString(unhex(leftPad(lower(hex(intDiv(number, {SPANS_PER_TRACE}) * {SPANS_PER_TRACE} + 1)), 16, '0')), 8)), \
               'op', 'checkout', \
               {base_ns} + toInt64(number) * {spread}, \
               1000000, 0, 1, 1, 'p' \
             FROM numbers({SPANS})"
        ),
    )
    .await;
}

/// `FLAG_SPANS` single-span traces over `[base, base + WINDOW_NS)`, each
/// carrying a `flag` attribute stored as the boolean's TEXT (`'true'` /
/// `'false'`) — the storage convention `{ .a }`'s equality lowering
/// reads. The two populations differ (300 / 700) so an inverted lowering
/// answers a different number.
async fn seed_flag_corpus(client: &ChClient, base_ns: i64) {
    let spread = WINDOW_NS / FLAG_SPANS as i64;
    exec(
        client,
        &format!(
            "INSERT INTO {DB}.trace_spans \
             (trace_id, span_id, parent_id, name, service, timestamp_ns, duration_ns, \
              status_code, kind, payload_type, payload) \
             SELECT \
               toFixedString(unhex(leftPad(lower(hex(number + 100000)), 32, '0')), 16), \
               toFixedString(unhex(leftPad(lower(hex(number + 100000)), 16, '0')), 8), \
               toFixedString(unhex('0000000000000000'), 8), \
               'op', 'flags', \
               {base_ns} + toInt64(number) * {spread}, \
               1000000, 0, 1, 1, 'p' \
             FROM numbers({FLAG_SPANS})"
        ),
    )
    .await;
    exec(
        client,
        &format!(
            "INSERT INTO {DB}.trace_attrs_idx \
             (date, key, val, scope, val_num, timestamp_ns, trace_id, span_id, duration_ns) \
             SELECT \
               toDate(fromUnixTimestamp64Nano({base_ns} + toInt64(number) * {spread})), \
               'flag', if(number < {FLAG_TRUE}, 'true', 'false'), 'span', NULL, \
               {base_ns} + toInt64(number) * {spread}, \
               toFixedString(unhex(leftPad(lower(hex(number + 100000)), 32, '0')), 16), \
               toFixedString(unhex(leftPad(lower(hex(number + 100000)), 16, '0')), 8), \
               1000000 \
             FROM numbers({FLAG_SPANS})"
        ),
    )
    .await;
}

/// The sum of every non-null sample across every series returned for
/// `q` over `[start_ns, end_ns)`. Timestamps are never compared — the
/// totals are bucketing-independent, which is what makes them a corpus
/// property rather than a step property.
async fn total_over_buckets(engine: &TraceEngine, q: &str, start_ns: i64, end_ns: i64) -> f64 {
    let query = pulsus_traceql::parse(q).unwrap_or_else(|e| panic!("{q} parses: {e:?}"));
    let plan = plan_trace_metrics(
        &query,
        &MetricsParams {
            start_ns,
            end_ns,
            step_s: 300,
        },
        &engine.metrics_ctx(),
    )
    .unwrap_or_else(|e| panic!("{q} plans: {e:?}"));
    let result = engine
        .metrics_range(&plan)
        .await
        .unwrap_or_else(|e| panic!("{q} executes: {e}"));
    result
        .series
        .iter()
        .flat_map(|s| s.samples.iter())
        .map(|(_, v)| *v)
        .filter(|v| !v.is_nan())
        .sum()
}

#[tokio::test]
async fn nested_set_and_bare_truthiness_answers_match_the_seeded_corpus() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
             (see crates/pulsus-read/tests/traces_metrics_nested_set_live.rs for setup)"
        );
        return;
    }

    // ---- Validity gate: the fixture's populations are pairwise distinct.
    // Asserted BEFORE any decision assertion, and before the corpus is
    // even seeded, so a later "simplification" that collapses two of them
    // reddens here rather than silently making a break invisible.
    assert_ne!(TRACES, SPANS - TRACES, "T and S-T must differ");
    assert_ne!(TRACES, SPANS, "T and S must differ");
    assert_ne!(SPANS - TRACES, SPANS, "S-T and S must differ");
    assert_ne!(
        FLAG_TRUE, FLAG_FALSE,
        "the bare-truthiness populations must differ — a symmetric fixture answers the same \
         number under an inverted lowering, which is how this break went invisible in planning"
    );

    let admin = ChClient::new(test_config()).await.expect("connect");
    exec(&admin, &format!("DROP DATABASE IF EXISTS {DB}")).await;
    run_init(&admin, &test_ctx(&DB)).await.expect("run_init");

    let now = now_ns();
    // Two disjoint windows: the nested-set corpus, then the flag corpus
    // an hour earlier.
    let ns_start = now - WINDOW_NS;
    let ns_end = now;
    let flag_start = now - 3 * WINDOW_NS;
    let flag_end = now - 2 * WINDOW_NS;

    let client = data_client().await;
    seed_nested_set_corpus(&client, ns_start).await;
    seed_flag_corpus(&client, flag_start).await;

    // ---- Validity gate: the SEEDED corpus really has that shape.
    let zero = "toFixedString(unhex('0000000000000000'), 8)";
    let seeded_spans = scalar(
        &client,
        &format!(
            "SELECT toUInt64(count()) AS n FROM {DB}.trace_spans \
             WHERE timestamp_ns >= {ns_start} AND timestamp_ns < {ns_end}"
        ),
    )
    .await;
    assert_eq!(seeded_spans, SPANS, "seeded span count");
    let seeded_roots = scalar(
        &client,
        &format!(
            "SELECT toUInt64(count()) AS n FROM {DB}.trace_spans \
             WHERE timestamp_ns >= {ns_start} AND timestamp_ns < {ns_end} AND parent_id = {zero}"
        ),
    )
    .await;
    assert_eq!(seeded_roots, TRACES, "seeded parentless span count");
    let traces_with_one_root = scalar(
        &client,
        &format!(
            "SELECT toUInt64(count()) AS n FROM (\
               SELECT trace_id, countIf(parent_id = {zero}) AS roots FROM {DB}.trace_spans \
               WHERE timestamp_ns >= {ns_start} AND timestamp_ns < {ns_end} \
               GROUP BY trace_id HAVING roots = 1)"
        ),
    )
    .await;
    assert_eq!(
        traces_with_one_root, TRACES,
        "every trace must have EXACTLY one parentless span — the identity the totals rest on"
    );

    let engine = TraceEngine::new(data_client().await, engine_config());

    // ---- Decision assertions: the three totals, through the planner.
    let roots = total_over_buckets(
        &engine,
        "{ nestedSetParent < 0 } | count_over_time()",
        ns_start,
        ns_end,
    )
    .await;
    let non_roots = total_over_buckets(
        &engine,
        "{ nestedSetParent >= 1 } | count_over_time()",
        ns_start,
        ns_end,
    )
    .await;
    let all = total_over_buckets(&engine, "{} | count_over_time()", ns_start, ns_end).await;

    assert_eq!(
        roots, TRACES as f64,
        "one root per trace: {{ nestedSetParent < 0 }} must total T ({TRACES})"
    );
    assert_eq!(
        non_roots,
        (SPANS - TRACES) as f64,
        "{{ nestedSetParent >= 1 }} must total S - T ({})",
        SPANS - TRACES
    );
    assert_eq!(
        all, SPANS as f64,
        "{{}} must total S ({SPANS}) — the control that catches a lowering which simply \
         matches everything"
    );
    assert_eq!(
        roots + non_roots,
        all,
        "the root and non-root halves must partition the corpus"
    );

    // ---- The conjoined form the Grafana panel actually sends, and the
    // one that puts the nested-set leaf under an `And` next to a leaf
    // that lowers on its own.
    let by_service = total_over_buckets(
        &engine,
        r#"{ resource.service.name = "checkout" && nestedSetParent < 0 } | count_over_time()"#,
        ns_start,
        ns_end,
    )
    .await;
    assert_eq!(
        by_service, TRACES as f64,
        "the hoisted service equality and the residual root test must compose"
    );
    let with_regex = total_over_buckets(
        &engine,
        r#"{ name =~ ".*" && nestedSetParent < 0 } | count_over_time()"#,
        ns_start,
        ns_end,
    )
    .await;
    assert_eq!(
        with_regex, TRACES as f64,
        "a nested-set leaf under an And beside a regex leaf must still lower"
    );

    // ---- Bare attribute truthiness, on its own window.
    let flag_true = total_over_buckets(
        &engine,
        "{ .flag } | count_over_time()",
        flag_start,
        flag_end,
    )
    .await;
    assert_eq!(
        flag_true, FLAG_TRUE as f64,
        "{{ .flag }} IS `.flag = true` — it must count the {FLAG_TRUE} true-valued spans, not \
         the {FLAG_FALSE} false-valued ones and not all {FLAG_SPANS}"
    );
    let flag_explicit = total_over_buckets(
        &engine,
        "{ .flag = true } | count_over_time()",
        flag_start,
        flag_end,
    )
    .await;
    assert_eq!(
        flag_true, flag_explicit,
        "{{ .flag }} and {{ .flag = true }} must answer identically — the same leaf lowering"
    );
    let absent = total_over_buckets(
        &engine,
        "{ .missing } | count_over_time()",
        flag_start,
        flag_end,
    )
    .await;
    assert_eq!(absent, 0.0, "an absent key is no match");

    exec(&admin, &format!("DROP DATABASE IF EXISTS {DB}")).await;
}
