//! Issue #743 part A: old parts move to a cold volume on an object store,
//! and every answer is unchanged; `schema/schema.sh` refuses a storage
//! policy or cold volume the server lacks before it drops anything.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`, against one ClickHouse server
//! that carries `ci/clickhouse-cluster/config.d/storage.xml` and can reach
//! the object store it names. The `schema-it-tiering` job in
//! `.github/workflows/ci.yml` starts both; a local run starts them with the
//! same commands and then:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=19123 \
//!     cargo test -p pulsus-schema --test live_tiering
//! ```
//!
//! A server without the storage file fails each test rather than skipping
//! it.

use std::process::Command;
use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;

static DB_TIERED: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_tiering_moved");
static DB_PLAIN: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_tiering_plain");
static DB_REFUSE: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_tiering_refuse");

/// The policy `storage.xml` defines: a local `hot` volume and a `cold`
/// volume on the object store, moving only by TTL.
const POLICY: &str = "pulsus_test_tiered";

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a ClickHouse carrying \
                 ci/clickhouse-cluster/config.d/storage.xml to run this test \
                 (see crates/pulsus-schema/tests/live_tiering.rs for setup)"
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
        query_timeout: Duration::from_secs(60),
        ..ChConnConfig::default()
    }
}

/// One column, every row, as text.
async fn strings(client: &ChClient, sql: &str) -> Vec<String> {
    client
        .query_strings(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// The one value of a one-row, one-column answer.
async fn one(client: &ChClient, sql: &str) -> String {
    let rows = strings(client, sql).await;
    assert_eq!(rows.len(), 1, "{sql}: one row");
    rows.into_iter().next().expect("one row")
}

async fn exec(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::Idempotent)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// The server must carry `storage.xml`'s policy: without it these tests
/// would prove nothing, so its absence fails them.
async fn require_policy(client: &ChClient) {
    let have = one(
        client,
        &format!(
            "SELECT toString(count()) AS s FROM system.storage_policies \
             WHERE policy_name = '{POLICY}'"
        ),
    )
    .await;
    if have == "0" {
        panic!(
            "the server has no storage policy {POLICY}: mount \
             ci/clickhouse-cluster/config.d/storage.xml (the schema-it-tiering job in \
             .github/workflows/ci.yml)"
        );
    }
}

async fn create_table_query(client: &ChClient, db: &str, table: &str) -> String {
    one(
        client,
        &format!(
            "SELECT create_table_query AS s FROM system.tables \
             WHERE database = '{db}' AND name = '{table}'"
        ),
    )
    .await
}

/// The six tables A5 writes, each with its `ORDER BY` columns.
const TABLES: [(&str, &str); 6] = [
    ("metric_samples", "org_id, fingerprint, unix_milli"),
    ("log_samples", "org_id, service, fingerprint, timestamp_ns"),
    (
        "spans",
        "org_id, intDiv(start_ns, 300000000000), trace_id, start_ns, span_id, kind",
    ),
    ("traces", "org_id, trace_id"),
    ("metric_series", "org_id, fingerprint"),
    ("resources", "org_id, service, resource_id"),
];

/// **A5: old days move, and every answer is unchanged.** Two databases
/// hold the same rows, one on the tiered policy with every family moving
/// after two days, one with no policy. The parts of days past the move age
/// are on the object store, the rest and every never-move table are local,
/// and each table answers the same in both.
#[tokio::test]
async fn old_days_move_and_every_answer_is_unchanged() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    require_policy(&client).await;

    let tiered = RenderCtx {
        storage_policy: Some(POLICY.to_string()),
        cold_volume: Some("cold".to_string()),
        metrics_hot_days: Some(2),
        logs_hot_days: Some(2),
        traces_hot_days: Some(2),
        ..RenderCtx::for_tests(&DB_TIERED, 30)
    };
    let plain = RenderCtx::for_tests(&DB_PLAIN, 30);
    for ctx in [&tiered, &plain] {
        exec(&client, &format!("DROP DATABASE IF EXISTS {} SYNC", ctx.db)).await;
        run_init(&client, ctx).await.expect("build the schema");
    }

    let now_ms: i64 = one(
        &client,
        "SELECT toString(toUnixTimestamp64Milli(now64())) AS s",
    )
    .await
    .parse()
    .expect("milliseconds");
    // (k, age in days, timestamp in milliseconds)
    let rows: Vec<(u32, f64, i64)> = [0.5, 3.5, 29.5]
        .iter()
        .enumerate()
        .map(|(i, a)| (i as u32 + 1, *a, now_ms - (a * 86_400_000.0) as i64))
        .collect();
    let tuples = rows
        .iter()
        .map(|(k, a, ts)| format!("({k}, {a}, toInt64({ts}))"))
        .collect::<Vec<_>>()
        .join(", ");
    let src = format!(
        "FROM (SELECT arrayJoin([{tuples}]) AS r) \
         ARRAY JOIN [toUInt8(1), toUInt8(2)] AS fp"
    );
    let one_fp = "WHERE fp = 1";
    let id = "toFixedString(unhex(lpad(hex(r.1), 32, '0')), 16)";
    let day = "toDate(fromUnixTimestamp64Milli(r.3), 'UTC')";
    for db in [DB_TIERED.name(), DB_PLAIN.name()] {
        for insert in [
            format!(
                "INSERT INTO {db}.metric_samples (org_id, fingerprint, unix_milli, value) \
                 SELECT '', fp, r.3, r.2 {src}"
            ),
            format!(
                "INSERT INTO {db}.log_samples (org_id, service, fingerprint, timestamp_ns, body) \
                 SELECT '', 's', 1, r.3 * 1000000, concat('line ', toString(r.2)) {src} {one_fp}"
            ),
            format!(
                "INSERT INTO {db}.spans (org_id, trace_id, start_ns, service, name) \
                 SELECT '', {id}, r.3 * 1000000, 's', 'n' {src} {one_fp}"
            ),
            format!(
                "INSERT INTO {db}.traces \
                 (org_id, day, trace_id, start_ns, end_ns, last_start_ns, buckets) \
                 SELECT '', {day}, {id}, r.3 * 1000000, r.3 * 1000000, r.3 * 1000000, \
                 [intDiv(r.3 * 1000000, 300000000000)] {src} {one_fp}"
            ),
            format!(
                "INSERT INTO {db}.metric_series (org_id, day, fingerprint, metric_name, hours) \
                 SELECT '', {day}, 1, 'm', 1 {src} {one_fp}"
            ),
            format!(
                "INSERT INTO {db}.resources (org_id, day, resource_id, service) \
                 SELECT '', {day}, 1, 's' {src} {one_fp}"
            ),
        ] {
            exec(&client, &insert).await;
        }
        for (table, _) in TABLES {
            exec(&client, &format!("OPTIMIZE TABLE {db}.{table} FINAL")).await;
        }
    }

    // (a) Where each day's parts are.
    let t = DB_TIERED.name();
    for (k, a, ts) in &rows {
        let partition = one(
            &client,
            &format!(
                "SELECT toString(toDate(fromUnixTimestamp64Milli(toInt64({ts})), 'UTC')) AS s"
            ),
        )
        .await;
        let want = if *a > 2.0 { "cold_store" } else { "default" };
        for table in ["metric_samples", "log_samples", "spans"] {
            let disks = strings(
                &client,
                &format!(
                    "SELECT disk_name AS s FROM system.parts WHERE database = '{t}' \
                     AND table = '{table}' AND active AND partition = '{partition}'"
                ),
            )
            .await;
            assert!(!disks.is_empty(), "{table}: day {k} has a part");
            assert!(
                disks.iter().all(|d| d == want),
                "{table}: the part of a day {a} days old is on {want}: {disks:?}"
            );
        }
    }
    for table in ["traces", "metric_series", "resources"] {
        let disks = strings(
            &client,
            &format!(
                "SELECT disk_name AS s FROM system.parts WHERE database = '{t}' \
                 AND table = '{table}' AND active"
            ),
        )
        .await;
        assert!(!disks.is_empty(), "{table} has parts");
        assert!(
            disks.iter().all(|d| d == "default"),
            "{table} never moves: {disks:?}"
        );
    }

    // (b) and (c): every table answers the same in both databases.
    for ((table, key), want_count) in TABLES.iter().zip([6, 3, 3, 3, 3, 3]) {
        let mut answers = Vec::new();
        for db in [DB_TIERED.name(), DB_PLAIN.name()] {
            let summary = one(
                &client,
                &format!(
                    "SELECT concat(toString(count()), ' ', toString(sum(cityHash64({key})))) AS s \
                     FROM {db}.{table}"
                ),
            )
            .await;
            let mut text = strings(
                &client,
                &format!(
                    "SELECT formatRow('JSONEachRow', *) AS s FROM {db}.{table} ORDER BY {key}"
                ),
            )
            .await;
            // Rows equal under the key (one fingerprint over three days)
            // come back in either order.
            text.sort();
            answers.push((summary, text));
        }
        assert_eq!(
            answers[0].0.split(' ').next(),
            Some(want_count.to_string().as_str()),
            "{table}: the row count"
        );
        assert_eq!(answers[0], answers[1], "{table}: the same answer in both");
    }

    // (d) The statements the server holds.
    for table in ["metric_samples", "log_samples", "spans"] {
        let q = create_table_query(&client, t, table).await;
        assert!(q.contains("TO VOLUME 'cold'"), "{table}: {q}");
    }
    for table in ["traces", "metric_series", "resources"] {
        let q = create_table_query(&client, t, table).await;
        assert!(!q.contains("storage_policy"), "{table}: {q}");
    }

    for db in [DB_TIERED.name(), DB_PLAIN.name()] {
        exec(&client, &format!("DROP DATABASE IF EXISTS {db} SYNC")).await;
    }
}

/// Environment variables for one script run, by name.
type Vars<'a> = Vec<(&'a str, &'a str)>;

/// `schema/schema.sh --mode single` against the test server, building `db`
/// with `PULSUS_RETENTION_DAYS=30` and `vars`; every other `PULSUS_*`
/// variable removed. Its exit code and standard error.
fn script(db: &str, vars: &[(&str, &str)]) -> (Option<i32>, String) {
    let cfg = test_config();
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut cmd = Command::new("sh");
    cmd.arg(root.join("schema/schema.sh"))
        .args(["--mode", "single"])
        .current_dir(&root);
    for (var, _) in std::env::vars() {
        if var.starts_with("PULSUS_") {
            cmd.env_remove(var);
        }
    }
    cmd.env("CLICKHOUSE_SERVER", &cfg.server)
        .env("CLICKHOUSE_HTTP_PORT", cfg.http_port.to_string())
        .env("PULSUS_SCHEMA_WAIT_SECONDS", "30")
        .env("CLICKHOUSE_DB", db)
        .env("PULSUS_RETENTION_DAYS", "30");
    for (var, value) in vars {
        cmd.env(var, value);
    }
    let out = cmd.output().expect("run schema.sh");
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// **A6: the script refuses an absent policy, or a cold volume the policy
/// lacks, puts first or could move by disk pressure, before it drops
/// anything; and builds on a valid one.**
#[tokio::test]
async fn the_script_refuses_an_absent_policy_before_it_drops() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    require_policy(&client).await;
    let d = DB_REFUSE.name();
    exec(&client, &format!("DROP DATABASE IF EXISTS {d} SYNC")).await;
    exec(&client, &format!("CREATE DATABASE {d}")).await;
    exec(
        &client,
        &format!("CREATE TABLE {d}.keep (x UInt8) ENGINE = MergeTree ORDER BY x"),
    )
    .await;
    // `EXISTS TABLE`, as a column.
    let keep = format!(
        "SELECT toString(count()) AS s FROM system.tables WHERE database = '{d}' AND name = 'keep'"
    );

    let hot = [
        ("PULSUS_METRICS_HOT_DAYS", "2"),
        ("PULSUS_LOGS_HOT_DAYS", "2"),
        ("PULSUS_TRACES_HOT_DAYS", "2"),
    ];
    let with_volume = |policy: &'static str, volume: &'static str| {
        let mut vars = vec![
            ("PULSUS_STORAGE_POLICY", policy),
            ("PULSUS_COLD_VOLUME", volume),
        ];
        vars.extend(hot);
        vars
    };
    let refused = |p: &str, v: &str| {
        format!(
            "schema.sh: storage policy '{p}' with a volume '{v}' after its first and \
             move_factor 0 is defined on 0 of 1 hosts; every server's configuration must \
             carry it (docs/configuration.md §3)"
        )
    };
    let cases: Vec<(&str, Vars, String)> = vec![
        (
            "(1) an absent policy",
            vec![("PULSUS_STORAGE_POLICY", "pulsus_no_such_policy")],
            "schema.sh: storage policy 'pulsus_no_such_policy' is defined on 0 of 1 hosts; \
             every server's configuration must carry it (docs/configuration.md §3)"
                .to_string(),
        ),
        (
            "(2) the first volume as the cold one",
            with_volume(POLICY, "hot"),
            refused(POLICY, "hot"),
        ),
        (
            "(3) a policy that moves by disk pressure",
            with_volume("pulsus_test_pressure", "cold"),
            refused("pulsus_test_pressure", "cold"),
        ),
        (
            "(4) an absent volume",
            with_volume(POLICY, "nosuch"),
            refused(POLICY, "nosuch"),
        ),
    ];
    for (what, vars, message) in cases {
        let (code, err) = script(d, &vars);
        assert_eq!(code, Some(1), "{what}: exit 1: {err}");
        assert_eq!(
            err.trim_end().lines().last(),
            Some(message.as_str()),
            "{what}: the message"
        );
        assert_eq!(one(&client, &keep).await, "1", "{what}: nothing is dropped");
    }

    let (code, err) = script(d, &with_volume(POLICY, "cold"));
    assert_eq!(code, Some(0), "(5) a valid policy and volume: {err}");
    assert_eq!(
        one(&client, &keep).await,
        "0",
        "(5) the database is rebuilt"
    );
    let q = create_table_query(&client, d, "metric_samples").await;
    assert!(q.contains("TO VOLUME 'cold'"), "(5) metric_samples: {q}");
    assert!(
        q.contains(&format!("storage_policy = '{POLICY}'")),
        "(5) metric_samples: {q}"
    );
    let q = create_table_query(&client, d, "traces").await;
    assert!(!q.contains("storage_policy"), "(5) traces: {q}");

    exec(&client, &format!("DROP DATABASE IF EXISTS {d} SYNC")).await;
}
