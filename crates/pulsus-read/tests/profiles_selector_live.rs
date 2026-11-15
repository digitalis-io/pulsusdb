//! Issue #820: the profiles label selector compiles to one series subquery
//! that answers its own tenant's series, against a live ClickHouse.
//!
//! The rows are kind-2 landing rows, so `profile_series_mv` writes the
//! series table as a push does. Each case compiles a request, checks the
//! text with the tenant check, runs it and compares the fingerprints.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-read --test profiles_selector_live
//! ```

use std::collections::BTreeMap;
use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChError, ChProto, Idempotency, QuerySettings};
use pulsus_model::Tenant;
use pulsus_read::profiles::selector::{
    Compiled, SelectorError, SelectorRequest, WindowLimits, compile, read_error,
};
use pulsus_read::profiles::tenant_scope;
use pulsus_read::traces::tenant_scope::TenantTerm;
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;

const CPU: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
const ALLOC: &str = "memory:alloc_space:bytes:space:bytes";
const HOUR_MS: i64 = 3_600_000;
const DAY_MS: i64 = 86_400_000;

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
        query_timeout: Duration::from_secs(120),
        ..ChConnConfig::default()
    }
}

fn tenant(name: &str) -> Tenant {
    let header = http::HeaderValue::from_str(name).expect("a header value");
    Tenant::from_header(Some(&header), false).expect("a valid tenant")
}

async fn exec(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::NonIdempotent)
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
}

/// One series of the plan's fixture table.
struct Row {
    fp: u128,
    org: &'static str,
    type_id: &'static str,
    service: &'static str,
    labels: &'static [(&'static str, &'static str)],
    offset_ms: i64,
}

const fn row(
    fp: u128,
    org: &'static str,
    type_id: &'static str,
    service: &'static str,
    labels: &'static [(&'static str, &'static str)],
    offset_ms: i64,
) -> Row {
    Row {
        fp,
        org,
        type_id,
        service,
        labels,
        offset_ms,
    }
}

const ROWS: [Row; 17] = [
    row(
        11,
        "t1",
        CPU,
        "checkout",
        &[("case", "neq"), ("env", "p")],
        0,
    ),
    row(12, "t1", CPU, "checkout", &[("case", "neq")], 0),
    row(
        21,
        "t1",
        CPU,
        "checkout",
        &[("case", "re"), ("env", "prod")],
        0,
    ),
    row(
        22,
        "t1",
        CPU,
        "checkout",
        &[("case", "re"), ("env", "prod-eu")],
        0,
    ),
    row(31, "t1", CPU, "checkout", &[("case", "and"), ("a", "1")], 0),
    row(
        32,
        "t1",
        CPU,
        "checkout",
        &[("case", "and"), ("a", "1"), ("b", "2")],
        0,
    ),
    row(41, "t1", CPU, "checkout", &[("case", "typed")], 0),
    row(42, "t1", ALLOC, "checkout", &[("case", "typed")], 0),
    row(43, "t1", CPU, "cart", &[("case", "typed")], 0),
    row(51, "t1", CPU, "checkout", &[("case", "lookback")], 0),
    row(
        52,
        "t1",
        CPU,
        "checkout",
        &[("case", "lookback")],
        2 * HOUR_MS,
    ),
    row(
        71,
        "t1",
        CPU,
        "checkout",
        &[("case", "nre"), ("env", "prod")],
        0,
    ),
    row(
        72,
        "t1",
        CPU,
        "checkout",
        &[("case", "nre"), ("env", "prod-eu")],
        0,
    ),
    row(
        73,
        "t1",
        CPU,
        "checkout",
        &[("case", "nre"), ("env", "dev")],
        0,
    ),
    row(74, "t1", CPU, "checkout", &[("case", "nre")], 0),
    row(61, "t1", CPU, "checkout", &[("case", "tenant")], 0),
    row(62, "t2", CPU, "checkout", &[("case", "tenant")], 0),
];

/// The landing insert for every row at `t`: kind 2, each label set holding
/// `service_name` and `__profile_type__` besides its own, keys sorted.
fn fixture_sql(t: i64, now: i64) -> String {
    let values: Vec<String> = ROWS
        .iter()
        .map(|r| {
            let mut labels: BTreeMap<&str, &str> = r.labels.iter().copied().collect();
            labels.insert("service_name", r.service);
            labels.insert("__profile_type__", r.type_id);
            let json = serde_json::to_string(&labels).expect("labels as JSON");
            format!(
                "('{}', {now}, 2, '{}', '{}', {}, {}, '{}')",
                r.org,
                r.type_id,
                r.service,
                r.fp,
                (t + r.offset_ms) * 1_000_000,
                json.replace('\\', "\\\\").replace('\'', "\\'")
            )
        })
        .collect();
    format!(
        "INSERT INTO profile_landing \
         (org_id, received_ms, kind, type_id, service, fingerprint, timestamp_ns, labels) \
         VALUES {}",
        values.join(", ")
    )
}

struct Fixture {
    bootstrap: ChClient,
    client: ChClient,
    db: String,
    /// `T`: the current UTC hour − 4 h + 30 min.
    t: i64,
}

impl Fixture {
    async fn new() -> Self {
        let db = pulsus_testkit::test_db("pulsus_read_it_profiles_selector");
        let bootstrap = ChClient::new(test_config("default"))
            .await
            .expect("connect (bootstrap)");
        exec(&bootstrap, &format!("DROP DATABASE IF EXISTS {db}")).await;
        run_init(&bootstrap, &RenderCtx::for_tests(&db, 7))
            .await
            .expect("run_init");
        let client = ChClient::new(test_config(&db)).await.expect("connect");
        let now = chrono::Utc::now().timestamp_millis();
        let t = now.div_euclid(HOUR_MS) * HOUR_MS - 4 * HOUR_MS + 30 * 60_000;
        exec(&client, &fixture_sql(t, now)).await;
        Fixture {
            bootstrap,
            client,
            db,
            t,
        }
    }

    async fn finish(self) {
        exec(
            &self.bootstrap,
            &format!("DROP DATABASE IF EXISTS {}", self.db),
        )
        .await;
    }

    /// The default window, `[T − 1 h, T + 1 h]`.
    fn window(&self) -> (i64, i64) {
        (self.t - HOUR_MS, self.t + HOUR_MS)
    }

    /// Compiles `selectors` for `org`, with the wall clock as `now`.
    fn compile(
        &self,
        org: &str,
        selectors: &[&str],
        profile_type: Option<&str>,
        (start_ms, end_ms): (i64, i64),
        now_ms: i64,
    ) -> Result<Compiled, SelectorError> {
        compile(
            &tenant(org),
            "profile_series",
            &SelectorRequest {
                selectors,
                profile_type,
                start_ms,
                end_ms,
            },
            now_ms,
            WindowLimits::default(),
        )
    }

    /// The compiled text, after the tenant check passes it.
    fn series_text(
        &self,
        org: &str,
        selectors: &[&str],
        profile_type: Option<&str>,
        window: (i64, i64),
        now_ms: i64,
    ) -> String {
        let sql = match self.compile(org, selectors, profile_type, window, now_ms) {
            Ok(Compiled::Series { sql, .. }) => sql,
            other => panic!("expected Series for {selectors:?}, got {other:?}"),
        };
        assert_eq!(
            tenant_scope::check(
                &sql,
                &TenantTerm::of(&tenant(org)),
                &tenant_scope::tables("")
            ),
            Ok(()),
            "{sql}"
        );
        sql
    }

    /// Runs `sql` and returns its fingerprints, sorted. Each literal `?` is
    /// doubled at the execution boundary, as every dispatcher does
    /// (`logql::exec::escape_query_placeholders`): the client reads a lone
    /// `?` as a bind placeholder.
    async fn run(&self, sql: &str) -> Result<Vec<u128>, ChError> {
        let wrapped =
            format!("SELECT toString(fingerprint) AS s FROM (\n{sql}\n)").replace('?', "??");
        let rows = self
            .client
            .query_strings(&wrapped, &QuerySettings::new())
            .await?;
        let mut fps: Vec<u128> = rows
            .iter()
            .map(|s| s.parse().expect("a fingerprint"))
            .collect();
        fps.sort_unstable();
        Ok(fps)
    }

    /// The fingerprints `selectors` selects for `org` in `window`.
    async fn select(
        &self,
        org: &str,
        selectors: &[&str],
        profile_type: Option<&str>,
        window: (i64, i64),
        now_ms: i64,
    ) -> Vec<u128> {
        let sql = self.series_text(org, selectors, profile_type, window, now_ms);
        self.run(&sql)
            .await
            .unwrap_or_else(|e| panic!("{e}\n{sql}"))
    }
}

/// L1–L9 against one database: the cases share the fixture rows, each its
/// own `case` label.
#[tokio::test]
async fn the_selector_answers_its_tenants_series() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let fx = Fixture::new().await;
    let now = chrono::Utc::now().timestamp_millis();
    let w = fx.window();

    // L1: an absent label reads as empty, so `!=` selects it.
    assert_eq!(
        fx.select("t1", &[r#"{case="neq",env!="p"}"#], None, w, now)
            .await,
        [12],
        "L1"
    );

    // L2: a regex is anchored.
    assert_eq!(
        fx.select("t1", &[r#"{case="re",env=~"prod"}"#], None, w, now)
            .await,
        [21],
        "L2"
    );

    // L3: several selectors are AND-ed.
    assert_eq!(
        fx.select("t1", &[r#"{case="and",a="1"}"#, r#"{b="2"}"#], None, w, now)
            .await,
        [32],
        "L3"
    );

    // L4: the database judges the pattern, even with no row to read.
    let sql = fx.series_text("t-empty", &[r#"{env=~"("}"#], None, w, now);
    match fx.run(&sql).await {
        Err(e @ ChError::Server { code: 427, .. }) => {
            let ChError::Server { message, .. } = &e else {
                unreachable!()
            };
            assert!(message.contains("cannot compile re2"), "L4: {message}");
            match read_error(&e) {
                Some(SelectorError::InvalidArgument(d)) => {
                    assert!(d.contains("missing )"), "L4: {d}")
                }
                other => panic!("L4: expected InvalidArgument, got {other:?}"),
            }
        }
        other => panic!("L4: expected a 427, got {other:?}\n{sql}"),
    }

    // L9: an absent label does not match the regex, so `!~` selects it.
    assert_eq!(
        fx.select("t1", &[r#"{case="nre",env!~"prod"}"#], None, w, now)
            .await,
        [72, 73, 74],
        "L9"
    );

    // L5: the type and the service read their key columns.
    let typed = [r#"{service_name="checkout",case="typed"}"#];
    assert_eq!(fx.select("t1", &typed, Some(CPU), w, now).await, [41], "L5");
    let sql = fx.series_text("t1", &typed, Some(CPU), w, now);
    let plan = fx
        .client
        .query_strings(
            &format!("SELECT explain AS s FROM (EXPLAIN indexes = 1 {sql})").replace('?', "??"),
            &QuerySettings::new(),
        )
        .await
        .expect("EXPLAIN");
    assert_eq!(
        primary_keys(&plan),
        ["org_id", "type_id", "service"],
        "L5: {plan:#?}"
    );

    // L6: the start is clamped to the lookback floor, `T + 1 h`.
    let late_now = fx.t + 7 * DAY_MS + HOUR_MS;
    assert_eq!(
        fx.select(
            "t1",
            &[r#"{case="lookback"}"#],
            None,
            (late_now - 8 * DAY_MS, fx.t + 3 * HOUR_MS),
            late_now
        )
        .await,
        [52],
        "L6"
    );

    // L7: a window longer than the limit is refused.
    match fx.compile("t1", &[], None, (now - 25 * HOUR_MS, now), now) {
        Err(SelectorError::InvalidArgument(m)) => assert!(
            m.starts_with("the query time range exceeds the limit"),
            "L7: {m}"
        ),
        other => panic!("L7: expected InvalidArgument, got {other:?}"),
    }

    // L8: each tenant reads its own series.
    for (org, expected) in [("t1", 61), ("t2", 62)] {
        assert_eq!(
            fx.select(org, &[r#"{case="tenant"}"#], None, w, now).await,
            [expected],
            "L8 as {org}"
        );
    }

    fx.finish().await;
}

/// The keys `EXPLAIN indexes = 1` lists under `PrimaryKey`.
fn primary_keys(plan: &[String]) -> Vec<String> {
    let mut lines = plan
        .iter()
        .map(|l| l.trim())
        .skip_while(|l| *l != "PrimaryKey");
    lines.next();
    if lines.next() != Some("Keys:") {
        return Vec::new();
    }
    lines
        .take_while(|l| !l.contains(':'))
        .map(str::to_string)
        .collect()
}
