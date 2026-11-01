//! Issue #494 part 1: on a cluster a landing insert goes through its table's
//! `Distributed` wrapper, keyed on `cityHash64(received_ms)`, so a push and
//! every resend of it reach one shard whichever server receives them.
//!
//! Against the 2-shard fixture at `ci/clickhouse-cluster/compose.yaml`: shard
//! 1 has two replicas, shard 2 one. Each server is dialled directly, through
//! the variables `crates/pulsus-schema/tests/live_cluster.rs` reads, and each
//! case builds its own database with `schema/schema.sh` and `PULSUS_CLUSTER`.
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! docker compose -f ci/clickhouse-cluster/compose.yaml up -d
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-write --test live_cluster_landing
//! docker compose -f ci/clickhouse-cluster/compose.yaml down -v
//! ```
//!
//! Every case names its landing table with `landing_for(Some("_dist"))`, so
//! where the wrapper is absent it inserts into the bare table and fails on
//! the duplicate rather than on a missing table.
//!
//! **A push keyed to a shard.** The writer stamps `received_ms` from its own
//! clock, so a case that needs a push on a chosen shard sends it through
//! [`RecordingForwarder`], which stamps every row with [`SHARD1_MS`] or
//! [`SHARD2_MS`] before forwarding. Each case first asks the server which
//! shard the stamp selects: `cityHash64(toInt64(4102444800000)) % 2` is `0`,
//! the fixture's first shard, and `cityHash64(toInt64(4102444800001)) % 2` is
//! `1`, its second (measured on 26.8.21.10; both shards weigh 1).

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{
    ChClient, ChConnConfig, ChEndpoint, ChError, ChProto, ChRow, Idempotency, QuerySettings, Row,
};
use pulsus_config::WriterConfig;
use pulsus_model::{ACTIVITY_BUCKET_MS, Date, Fingerprint, LabelSet, UnixNano};
use pulsus_write::writer::{
    BlockInserter, ChBlockInserter, LogLandingRow, LogPatternRow, LogWriter, MetricLandingRow,
    MetricWriter, MetricWriterTables, TraceLandingRow, TraceWriter, TraceWriterTables,
    WriterRuntime, WriterTables,
};
use pulsus_write::{
    ExemplarPoint, HistogramPoint, LogRow, LogSink, MetricMetadata, MetricPoint, MetricSink,
    ParsedLogs, ParsedMetrics, PushHeaders, SeriesRef, StreamRow, TraceSink,
};

const CLUSTER: &str = "pulsus_test_cluster";
const SUFFIX: &str = "_dist";

/// A stamp `cityHash64` sends to the fixture's first shard (2100-01-01, far
/// enough ahead that the landing table's TTL is not due).
const SHARD1_MS: i64 = 4_102_444_800_000;
/// A stamp `cityHash64` sends to the fixture's second shard.
const SHARD2_MS: i64 = 4_102_444_800_001;

static DB_POISON_LOGS: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_write_it_routed_poison_logs");
static DB_POISON_TRACES: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_write_it_routed_poison_traces");
static DB_POISON_METRICS: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_write_it_routed_poison_metrics");
static DB_REPLICA_FAILOVER: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_write_it_routed_replica_failover");
static DB_SHARD_MISSING: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_write_it_routed_shard_missing");
static DB_ZONE: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_write_it_routed_zone");
static DB_RESEND: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_write_it_routed_resend");
static DB_RESEND_CONTROL: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_write_it_routed_resend_control");

/// One case at a time: each builds a clustered database through the
/// cluster's DDL queue, and 1-L6 reads the servers' query logs.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; **panics** rather than skipping when
/// the gate is absent in a live CI job (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with the 2-shard cluster fixture up to \
                 run this test (see crates/pulsus-write/tests/live_cluster_landing.rs for setup)"
            );
            return;
        }
    };
}

// -- the fixture --------------------------------------------------------

/// One server of the fixture: its host and HTTP port.
#[derive(Clone)]
struct Server {
    host: String,
    port: u16,
}

fn server(host_env: &str, default_host: &str, port_env: &str) -> Server {
    Server {
        host: std::env::var(host_env).unwrap_or_else(|_| default_host.to_string()),
        port: std::env::var(port_env)
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(8123),
    }
}

/// Shard 1's first replica.
fn shard1() -> Server {
    server(
        "PULSUS_TEST_CH_SHARD1_HOST",
        "172.28.0.11",
        "PULSUS_TEST_CH_SHARD1_HTTP_PORT",
    )
}

/// Shard 1's second replica.
fn shard1b() -> Server {
    server(
        "PULSUS_TEST_CH_SHARD1B_HOST",
        "172.28.0.13",
        "PULSUS_TEST_CH_SHARD1B_HTTP_PORT",
    )
}

/// Shard 2's only replica.
fn shard2() -> Server {
    server(
        "PULSUS_TEST_CH_SHARD2_HOST",
        "172.28.0.12",
        "PULSUS_TEST_CH_SHARD2_HTTP_PORT",
    )
}

fn config(servers: &[(&Server, Option<&str>)], db: &str) -> ChConnConfig {
    let first = servers[0].0;
    ChConnConfig {
        server: first.host.clone(),
        http_port: first.port,
        database: db.to_string(),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(30),
        endpoints: if servers.len() > 1 || servers[0].1.is_some() {
            servers
                .iter()
                .map(|(s, zone)| ChEndpoint {
                    host: s.host.clone(),
                    http_port: s.port,
                    zone: zone.map(str::to_string),
                })
                .collect()
        } else {
            Vec::new()
        },
        ..ChConnConfig::default()
    }
}

async fn client(servers: &[(&Server, Option<&str>)], db: &str) -> ChClient {
    ChClient::new(config(servers, db))
        .await
        .expect("connect to the fixture")
}

/// One client per server of the fixture, each bound to `db`.
struct Nodes {
    s1a: ChClient,
    s1b: ChClient,
    s2: ChClient,
}

impl Nodes {
    async fn of(db: &str) -> Self {
        Nodes {
            s1a: client(&[(&shard1(), None)], db).await,
            s1b: client(&[(&shard1b(), None)], db).await,
            s2: client(&[(&shard2(), None)], db).await,
        }
    }

    fn all(&self) -> [(&'static str, &ChClient); 3] {
        [
            ("shard 1, first replica", &self.s1a),
            ("shard 1, second replica", &self.s1b),
            ("shard 2", &self.s2),
        ]
    }
}

/// Builds `db` with `schema/schema.sh` and `PULSUS_CLUSTER`, through shard
/// 1's first replica, as `crates/pulsus-schema/tests/live_cluster.rs` does.
/// The script drops the database before it builds it.
fn build_schema(db: &str) {
    let at = shard1();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut cmd = std::process::Command::new("sh");
    cmd.arg(root.join("schema/schema.sh")).current_dir(&root);
    for (var, _) in std::env::vars() {
        if var.starts_with("PULSUS_") || var.starts_with("CLICKHOUSE_") {
            cmd.env_remove(var);
        }
    }
    cmd.env("CLICKHOUSE_SERVER", &at.host)
        .env("CLICKHOUSE_HTTP_PORT", at.port.to_string())
        .env("PULSUS_SCHEMA_WAIT_SECONDS", "30")
        .env("PULSUS_CLUSTER", CLUSTER)
        .env("CLICKHOUSE_DB", db)
        .env("PULSUS_RETENTION_DAYS", "7");
    let out = cmd.output().expect("run schema.sh");
    assert_eq!(
        out.status.code(),
        Some(0),
        "schema.sh built {db}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

async fn drop_db(nodes: &Nodes, db: &str) {
    exec(
        &nodes.s1a,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER}' SYNC"),
    )
    .await;
}

async fn exec(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::Idempotent)
        .await
        .unwrap_or_else(|e| panic!("execute failed: {e}\nSQL:\n{sql}"));
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct NumRow {
    n: u64,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct TextRow {
    s: String,
}

/// The one `UInt64` `sql` answers, as column `n`.
async fn num(client: &ChClient, sql: &str) -> u64 {
    let mut stream = client
        .query_stream::<NumRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\nSQL:\n{sql}"));
    let row = stream
        .next()
        .await
        .unwrap_or_else(|| panic!("one row: {sql}"))
        .unwrap_or_else(|e| panic!("decode: {e}\nSQL:\n{sql}"));
    row.n
}

/// Every `String` `sql` answers, as column `s`.
async fn texts(client: &ChClient, sql: &str) -> Vec<String> {
    let mut stream = client
        .query_stream::<TextRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\nSQL:\n{sql}"));
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.unwrap_or_else(|e| panic!("decode: {e}\nSQL:\n{sql}")).s);
    }
    out
}

/// Two shards in the cluster as both shards' servers see it, and two
/// different servers answering: every claim here is about two shards.
async fn require_two_shards(nodes: &Nodes) {
    let mut hosts = Vec::new();
    for (name, client) in [("shard 1", &nodes.s1a), ("shard 2", &nodes.s2)] {
        let shards = num(
            client,
            &format!(
                "SELECT toUInt64(count(DISTINCT shard_num)) AS n FROM system.clusters \
                 WHERE cluster = '{CLUSTER}'"
            ),
        )
        .await;
        assert_eq!(shards, 2, "{name} reports {shards} shards in '{CLUSTER}'");
        hosts.extend(texts(client, "SELECT hostName() AS s").await);
    }
    assert_ne!(hosts[0], hosts[1], "one server answered for both shards");
}

/// Asserts that `ms` selects the shard at `index` (0 is shard 1, 1 is shard 2).
async fn assert_stamp_selects(client: &ChClient, ms: i64, index: u64) {
    let got = num(
        client,
        &format!("SELECT cityHash64(toInt64({ms})) % 2 AS n"),
    )
    .await;
    assert_eq!(got, index, "cityHash64({ms}) % 2");
}

/// Waits for every replicated table of `db` on `client` to catch up with its
/// replica set.
async fn sync_replicas(client: &ChClient, db: &str) {
    for table in texts(
        client,
        &format!("SELECT table AS s FROM system.replicas WHERE database = '{db}' ORDER BY s"),
    )
    .await
    {
        exec(client, &format!("SYSTEM SYNC REPLICA {db}.{table}")).await;
    }
}

async fn sync_everywhere(nodes: &Nodes, db: &str) {
    for (_, client) in nodes.all() {
        sync_replicas(client, db).await;
    }
}

// -- the writers' spool ------------------------------------------------

/// A spool root of this case's own, under the build's temporary directory.
fn spool_root(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "routed-landing-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("create the spool root");
    dir
}

/// How many record files the writer filed under `kind` (`uncertain`,
/// `poison`), in any table's directory.
fn spooled(root: &Path, kind: &str) -> usize {
    let Ok(tables) = std::fs::read_dir(root.join(kind)) else {
        return 0;
    };
    tables
        .flatten()
        .filter(|e| e.path().is_dir())
        .map(|t| {
            std::fs::read_dir(t.path())
                .map(|files| {
                    files
                        .flatten()
                        .filter(|f| f.path().extension().and_then(|x| x.to_str()) == Some("json"))
                        .count()
                })
                .unwrap_or(0)
        })
        .sum()
}

fn runtime(root: &Path, retries: u32) -> WriterRuntime {
    let mut runtime = WriterRuntime::from_config(&WriterConfig::default());
    runtime.spool_dir = root.to_path_buf();
    runtime.log_landing_retries = retries;
    runtime.metrics_landing_retries = retries;
    runtime.trace_landing_retries = retries;
    runtime.log_landing_inserters = 1;
    runtime.metrics_landing_inserters = 1;
    runtime.trace_landing_inserters = 1;
    runtime
}

fn no_tenant() -> pulsus_model::Tenant {
    pulsus_model::Tenant::from_header(None, false).expect("no header is the empty tenant")
}

// -- the forwarding inserter -------------------------------------------

/// A landing row whose receive stamp a test may set.
trait Stamped: ChRow + Clone + Send + Sync + 'static {
    fn stamp(&mut self, ms: i64);
}

impl Stamped for LogLandingRow {
    fn stamp(&mut self, ms: i64) {
        self.received_ms = ms;
    }
}

impl Stamped for MetricLandingRow {
    fn stamp(&mut self, ms: i64) {
        self.received_ms = ms;
    }
}

impl Stamped for TraceLandingRow {
    fn stamp(&mut self, ms: i64) {
        self.received_ms = ms;
    }
}

/// One forwarded call: its table, the rows sent and the settings, token
/// included.
type Forwarded<R> = (String, Vec<R>, QuerySettings);

/// A `BlockInserter` over a `ChBlockInserter` that, when given a stamp, sets
/// every row's `received_ms` to it, then records each call's rows and
/// settings before forwarding them. Every attempt of one push carries the
/// stamp in force when it was sent, so a resend keeps its push's shard.
struct RecordingForwarder<R> {
    inner: ChBlockInserter,
    stamp: Mutex<Option<i64>>,
    sent: Mutex<Vec<Forwarded<R>>>,
}

impl<R: Stamped> RecordingForwarder<R> {
    fn new(client: Arc<ChClient>) -> Arc<Self> {
        Arc::new(RecordingForwarder {
            inner: ChBlockInserter::new(client),
            stamp: Mutex::new(None),
            sent: Mutex::new(Vec::new()),
        })
    }

    fn set_stamp(&self, ms: i64) {
        *self.stamp.lock().expect("stamp mutex") = Some(ms);
    }

    fn calls(&self) -> usize {
        self.sent.lock().expect("sent mutex").len()
    }

    fn last(&self) -> Forwarded<R> {
        self.sent
            .lock()
            .expect("sent mutex")
            .last()
            .cloned()
            .expect("at least one forwarded call")
    }
}

impl<R: Stamped> BlockInserter<R> for RecordingForwarder<R> {
    fn insert<'a>(
        &'a self,
        table: &'a str,
        rows: &'a [R],
    ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>> {
        let empty = QuerySettings::new();
        let mut rows = rows.to_vec();
        if let Some(ms) = *self.stamp.lock().expect("stamp mutex") {
            rows.iter_mut().for_each(|r| r.stamp(ms));
        }
        self.sent.lock().expect("sent mutex").push((
            table.to_string(),
            rows.clone(),
            empty.clone(),
        ));
        Box::pin(async move { self.inner.insert_with(table, &rows, &empty).await })
    }

    fn insert_with<'a>(
        &'a self,
        table: &'a str,
        rows: &'a [R],
        extra: &'a QuerySettings,
    ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>> {
        let mut rows = rows.to_vec();
        if let Some(ms) = *self.stamp.lock().expect("stamp mutex") {
            rows.iter_mut().for_each(|r| r.stamp(ms));
        }
        self.sent.lock().expect("sent mutex").push((
            table.to_string(),
            rows.clone(),
            extra.clone(),
        ));
        Box::pin(async move { self.inner.insert_with(table, &rows, extra).await })
    }
}

// -- fixtures ------------------------------------------------------------

fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock is past the epoch")
            .as_millis(),
    )
    .expect("a representable millisecond stamp")
}

const SERVICE: &str = "checkout-api";

fn log_line(fingerprint: u128, ts_ns: i64, body: &str) -> LogRow {
    LogRow {
        service: SERVICE.to_string(),
        fingerprint: Fingerprint::from_raw(fingerprint),
        timestamp_ns: UnixNano(ts_ns),
        severity: 0,
        body: body.to_string(),
        structured_metadata: String::new(),
    }
}

fn log_stream(fingerprint: u128, ts_ns: i64) -> StreamRow {
    StreamRow {
        month: Date::start_of_month_utc(ts_ns).expect("a representable month"),
        fingerprint: Fingerprint::from_raw(fingerprint),
        service: SERVICE.to_string(),
        labels: LabelSet::from_normalized([
            ("service_name".to_string(), SERVICE.to_string()),
            ("env".to_string(), "production".to_string()),
        ])
        .0,
        updated_ns: ts_ns,
    }
}

/// One push of the given lines, all of one stream, with its registration.
fn log_push(fingerprint: u128, bodies: &[&str]) -> ParsedLogs {
    let ts_ns = now_ms() * 1_000_000;
    ParsedLogs {
        rows: bodies
            .iter()
            .map(|b| log_line(fingerprint, ts_ns, b))
            .collect(),
        streams: vec![log_stream(fingerprint, ts_ns)],
        ..Default::default()
    }
}

/// An OTLP span.
fn otlp_span(
    trace_id: u8,
    span_id: u8,
    name: &str,
    start_ns: u64,
    error: bool,
) -> opentelemetry_proto::tonic::trace::v1::Span {
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
    use opentelemetry_proto::tonic::trace::v1::{Span, Status};
    Span {
        trace_id: vec![trace_id; 16],
        span_id: vec![span_id; 8],
        name: name.to_string(),
        start_time_unix_nano: start_ns,
        end_time_unix_nano: start_ns + 1_000_000,
        attributes: vec![KeyValue {
            key: "http.method".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue("GET".to_string())),
            }),
            key_strindex: 0,
        }],
        status: error.then(|| Status {
            message: "failed".to_string(),
            code: 2,
        }),
        ..Default::default()
    }
}

/// One trace push of `spans`, under one resource whose `service.name` is a
/// string, decoded as a handler decodes it.
fn trace_push(
    spans: Vec<opentelemetry_proto::tonic::trace::v1::Span>,
) -> pulsus_write::ParsedTraceLanding {
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans};
    let req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue(SERVICE.to_string())),
                    }),
                    key_strindex: 0,
                }],
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    pulsus_write::parse_trace_landing(&req).expect("the landing decode")
}

/// One metrics push: one float sample per name, each its own series.
fn metric_push(names: &[&str]) -> ParsedMetrics {
    let ms = now_ms() - 60_000;
    let mut batch = ParsedMetrics::default();
    for (i, name) in names.iter().enumerate() {
        let metric_name: Arc<str> = Arc::from(*name);
        let fingerprint = Fingerprint::from_raw(100 + i as u128);
        batch.samples.push(MetricPoint {
            metric_name: metric_name.clone(),
            fingerprint,
            unix_milli: ms,
            value: 1.0,
        });
        batch.series.push(SeriesRef {
            metric_name,
            fingerprint,
            labels: LabelSet::from_normalized([("job".to_string(), "checkout".to_string())]).0,
        });
    }
    batch
}

// -- 1-L1 to 1-L3: every attempt throws after the send ------------------

/// The scratch target a throwing view would write, and the view itself, on
/// every server: it throws on a marked row, so every insert carrying one
/// fails after the block was sent (code 395).
async fn throwing_view(nodes: &Nodes, db: &str, landing: &str, column: &str) {
    exec(
        &nodes.s1a,
        &format!(
            "CREATE TABLE {db}.poison_seen ON CLUSTER '{CLUSTER}' ({column} String) \
             ENGINE = MergeTree ORDER BY tuple()"
        ),
    )
    .await;
    exec(
        &nodes.s1a,
        &format!(
            "CREATE MATERIALIZED VIEW {db}.poison_mv ON CLUSTER '{CLUSTER}' TO {db}.poison_seen \
             AS SELECT {column} FROM {db}.{landing} \
             WHERE throwIf({column} = 'poison', 'a poison row') = 0"
        ),
    )
    .await;
}

/// The landing rows of `table` on shard 1 and on shard 2: what a push's
/// placement is read from. Shard 1 is read on its first replica after its
/// second has been synced into it.
async fn landing_per_shard(nodes: &Nodes, db: &str, table: &str) -> (u64, u64) {
    sync_everywhere(nodes, db).await;
    let sql = format!("SELECT count() AS n FROM {db}.{table}");
    (num(&nodes.s1a, &sql).await, num(&nodes.s2, &sql).await)
}

/// The push's landing rows are on at most one shard, and there once.
fn assert_once_on_one_shard(what: &str, per_shard: (u64, u64), rows: u64) {
    assert!(
        matches!(per_shard, (0, 0)) || per_shard == (rows, 0) || per_shard == (0, rows),
        "{what}: the push's {rows} landing rows must be on at most one shard, once; \
         shard 1 holds {}, shard 2 holds {}",
        per_shard.0,
        per_shard.1
    );
}

async fn assert_scratch_empty(nodes: &Nodes, db: &str) {
    for (name, client) in nodes.all() {
        assert_eq!(
            num(
                client,
                &format!("SELECT count() AS n FROM {db}.poison_seen")
            )
            .await,
            0,
            "{name}: the throwing view wrote nothing"
        );
    }
}

/// **1-L1.** A logs push whose every attempt throws after the send settles
/// `uncertain/`, and its landing block is on one shard, once — whichever
/// endpoint each attempt reached.
#[tokio::test]
async fn a_log_push_that_throws_on_every_attempt_lands_on_one_shard_once() {
    skip_unless_live!();
    let _serial = SERIAL.lock().await;
    let db = DB_POISON_LOGS.name();
    build_schema(db);
    let nodes = Nodes::of(db).await;
    require_two_shards(&nodes).await;
    throwing_view(&nodes, db, "log_landing", "body").await;

    let root = spool_root("poison-logs");
    let both = Arc::new(client(&[(&shard1(), None), (&shard2(), None)], db).await);
    let forwarder = RecordingForwarder::<LogLandingRow>::new(both);
    let writer = LogWriter::with_landing_inserter_and_runtime(
        forwarder.clone(),
        runtime(&root, 2),
        WriterTables::landing_for(Some(SUFFIX)),
    );
    let answer = writer
        .admit_flush(
            &no_tenant(),
            log_push(
                11,
                &["request 1 completed", "poison", "request 2 completed"],
            ),
            PushHeaders::default(),
        )
        .expect("the queue has room")
        .await;
    writer.shutdown(Duration::from_secs(10)).await;

    assert!(answer.is_err(), "every attempt threw: {answer:?}");
    assert_eq!(forwarder.calls(), 3, "one send and two resends");
    assert_eq!(spooled(&root, "uncertain"), 1, "the push settled uncertain");
    let rows = forwarder.last().1.len() as u64;
    let per_shard = landing_per_shard(&nodes, db, "log_landing").await;
    assert_once_on_one_shard("log_landing", per_shard, rows);
    assert_scratch_empty(&nodes, db).await;
    let repeated = texts(
        &nodes.s1a,
        &format!("SELECT body AS s FROM {db}.log_samples{SUFFIX} GROUP BY body HAVING count() > 1"),
    )
    .await;
    assert!(repeated.is_empty(), "lines stored twice: {repeated:?}");

    drop_db(&nodes, db).await;
    std::fs::remove_dir_all(&root).ok();
}

/// **1-L2.** As 1-L1, for a trace push of four spans, one named `poison`.
#[tokio::test]
async fn a_trace_push_that_throws_on_every_attempt_lands_on_one_shard_once() {
    skip_unless_live!();
    let _serial = SERIAL.lock().await;
    let db = DB_POISON_TRACES.name();
    build_schema(db);
    let nodes = Nodes::of(db).await;
    require_two_shards(&nodes).await;
    throwing_view(&nodes, db, "trace_landing", "name").await;

    let root = spool_root("poison-traces");
    let both = Arc::new(client(&[(&shard1(), None), (&shard2(), None)], db).await);
    let forwarder = RecordingForwarder::<TraceLandingRow>::new(both);
    let writer = TraceWriter::with_inserters_and_runtime(
        forwarder.clone(),
        runtime(&root, 2),
        TraceWriterTables::landing_for(Some(SUFFIX)),
    );
    let start = u64::try_from(now_ms() - 60_000).expect("a positive stamp") * 1_000_000;
    let push = trace_push(vec![
        otlp_span(0xa1, 1, "GET /a", start, false),
        otlp_span(0xa1, 2, "GET /b", start, false),
        otlp_span(0xa1, 3, "poison", start, false),
        otlp_span(0xa1, 4, "GET /c", start, false),
    ]);
    let answer = writer
        .admit_flush(&no_tenant(), push, PushHeaders::default())
        .expect("the queue has room")
        .await;
    writer.shutdown(Duration::from_secs(10)).await;

    assert!(answer.is_err(), "every attempt threw: {answer:?}");
    assert_eq!(forwarder.calls(), 3, "one send and two resends");
    assert_eq!(spooled(&root, "uncertain"), 1, "the push settled uncertain");
    let rows = forwarder.last().1.len() as u64;
    let per_shard = landing_per_shard(&nodes, db, "trace_landing").await;
    assert_once_on_one_shard("trace_landing", per_shard, rows);
    assert_scratch_empty(&nodes, db).await;
    let spans = num(
        &nodes.s1a,
        &format!("SELECT count() AS n FROM {db}.spans{SUFFIX}"),
    )
    .await;
    assert!(spans <= 4, "the push has four spans and {spans} are stored");

    drop_db(&nodes, db).await;
    std::fs::remove_dir_all(&root).ok();
}

/// **1-L3.** As 1-L1, for a metrics push carrying a sample named `poison`.
#[tokio::test]
async fn a_metric_push_that_throws_on_every_attempt_lands_on_one_shard_once() {
    skip_unless_live!();
    let _serial = SERIAL.lock().await;
    let db = DB_POISON_METRICS.name();
    build_schema(db);
    let nodes = Nodes::of(db).await;
    require_two_shards(&nodes).await;
    throwing_view(&nodes, db, "metric_landing", "metric_name").await;

    let root = spool_root("poison-metrics");
    let both = Arc::new(client(&[(&shard1(), None), (&shard2(), None)], db).await);
    let forwarder = RecordingForwarder::<MetricLandingRow>::new(both);
    let writer = MetricWriter::with_landing_inserter_and_runtime(
        forwarder.clone(),
        runtime(&root, 2),
        ACTIVITY_BUCKET_MS,
        MetricWriterTables::landing_for(Some(SUFFIX)),
    );
    let answer = writer
        .admit_flush(
            &no_tenant(),
            metric_push(&["up", "poison", "requests_total"]),
            PushHeaders::default(),
        )
        .expect("the queue has room")
        .await;
    writer.shutdown(Duration::from_secs(10)).await;

    assert!(answer.is_err(), "every attempt threw: {answer:?}");
    assert_eq!(forwarder.calls(), 3, "one send and two resends");
    assert_eq!(spooled(&root, "uncertain"), 1, "the push settled uncertain");
    let rows = forwarder.last().1.len() as u64;
    let per_shard = landing_per_shard(&nodes, db, "metric_landing").await;
    assert_once_on_one_shard("metric_landing", per_shard, rows);
    assert_scratch_empty(&nodes, db).await;
    let repeated = texts(
        &nodes.s1a,
        &format!(
            "SELECT metric_name AS s FROM {db}.metric_samples{SUFFIX} \
             GROUP BY metric_name, fingerprint, unix_milli HAVING count() > 1"
        ),
    )
    .await;
    assert!(repeated.is_empty(), "samples stored twice: {repeated:?}");

    drop_db(&nodes, db).await;
    std::fs::remove_dir_all(&root).ok();
}

// -- 1-L4 to 1-L6: where the block goes ---------------------------------

/// The `kind = 0` rows `client` holds for `body` in `db.log_landing`.
async fn lines_with_body(client: &ChClient, db: &str, body: &str) -> u64 {
    num(
        client,
        &format!("SELECT count() AS n FROM {db}.log_landing WHERE kind = 0 AND body = '{body}'"),
    )
    .await
}

/// **1-L4.** With `log_landing` detached on shard 1's second replica alone,
/// pushes keyed to shard 1 through that replica are stored on its peer:
/// the coordinator's own replica is not special.
#[tokio::test]
async fn a_push_through_a_replica_without_the_table_is_stored_on_its_peer() {
    skip_unless_live!();
    let _serial = SERIAL.lock().await;
    let db = DB_REPLICA_FAILOVER.name();
    build_schema(db);
    let nodes = Nodes::of(db).await;
    require_two_shards(&nodes).await;
    assert_stamp_selects(&nodes.s1a, SHARD1_MS, 0).await;
    exec(&nodes.s1b, &format!("DETACH TABLE {db}.log_landing")).await;

    let root = spool_root("replica-failover");
    let only_1b = Arc::new(client(&[(&shard1b(), None)], db).await);
    let forwarder = RecordingForwarder::<LogLandingRow>::new(only_1b);
    forwarder.set_stamp(SHARD1_MS);
    let writer = LogWriter::with_landing_inserter_and_runtime(
        forwarder.clone(),
        runtime(&root, 3),
        WriterTables::landing_for(Some(SUFFIX)),
    );
    let bodies: Vec<String> = (0..4).map(|i| format!("failover push {i}")).collect();
    let mut answers = Vec::new();
    for (i, body) in bodies.iter().enumerate() {
        answers.push(
            writer
                .admit_flush(
                    &no_tenant(),
                    log_push(20 + i as u128, &[body.as_str()]),
                    PushHeaders::default(),
                )
                .expect("the queue has room")
                .await,
        );
    }
    writer.shutdown(Duration::from_secs(10)).await;
    exec(&nodes.s1b, &format!("ATTACH TABLE {db}.log_landing")).await;

    for (body, answer) in bodies.iter().zip(&answers) {
        assert!(answer.is_ok(), "{body}: answered {answer:?}");
    }
    sync_everywhere(&nodes, db).await;
    for (name, client) in [
        ("shard 1, first replica", &nodes.s1a),
        ("shard 1, second replica", &nodes.s1b),
    ] {
        for body in &bodies {
            assert_eq!(
                lines_with_body(client, db, body).await,
                1,
                "{name} holds {body} once"
            );
        }
    }

    drop_db(&nodes, db).await;
    std::fs::remove_dir_all(&root).ok();
}

/// **1-L5.** A push keyed to shard 2, whose only replica lacks
/// `log_landing`, is not stored anywhere else: every attempt fails, it
/// settles `uncertain/`, and once the table is back its recorded block and
/// token, resent, are stored once on shard 2.
#[tokio::test]
async fn a_push_keyed_to_a_shard_without_the_table_is_stored_nowhere_else() {
    skip_unless_live!();
    let _serial = SERIAL.lock().await;
    let db = DB_SHARD_MISSING.name();
    build_schema(db);
    let nodes = Nodes::of(db).await;
    require_two_shards(&nodes).await;
    assert_stamp_selects(&nodes.s1a, SHARD2_MS, 1).await;
    exec(&nodes.s2, &format!("DETACH TABLE {db}.log_landing")).await;

    let root = spool_root("shard-missing");
    let only_1a = Arc::new(client(&[(&shard1(), None)], db).await);
    let forwarder = RecordingForwarder::<LogLandingRow>::new(only_1a.clone());
    forwarder.set_stamp(SHARD2_MS);
    let table = WriterTables::landing_for(Some(SUFFIX));
    let landing = table.landing.clone();
    let writer =
        LogWriter::with_landing_inserter_and_runtime(forwarder.clone(), runtime(&root, 2), table);
    let answer = writer
        .admit_flush(
            &no_tenant(),
            log_push(30, &["missing shard push"]),
            PushHeaders::default(),
        )
        .expect("the queue has room")
        .await;
    writer.shutdown(Duration::from_secs(10)).await;

    assert!(
        answer.is_err(),
        "a push keyed to a shard without the table must fail: {answer:?}"
    );
    assert_eq!(spooled(&root, "uncertain"), 1, "the push settled uncertain");
    let on_shard1 = num(
        &nodes.s1a,
        &format!("SELECT count() AS n FROM {db}.log_landing"),
    )
    .await;
    assert_eq!(on_shard1, 0, "nothing is stored on the receiving shard");
    exec(&nodes.s2, &format!("ATTACH TABLE {db}.log_landing")).await;
    sync_everywhere(&nodes, db).await;
    assert_eq!(
        num(
            &nodes.s2,
            &format!("SELECT count() AS n FROM {db}.log_landing")
        )
        .await,
        0,
        "nothing is stored on shard 2"
    );

    let (sent_to, rows, settings) = forwarder.last();
    assert_eq!(sent_to, &*landing);
    assert!(
        rows.iter().all(|r| r.received_ms == SHARD2_MS),
        "the recorded rows carry shard 2's stamp"
    );
    only_1a
        .insert_block_with(&landing, &rows, &settings)
        .await
        .expect("the resend is stored");
    sync_everywhere(&nodes, db).await;
    let landing_sql = format!("SELECT count() AS n FROM {db}.log_landing");
    assert_eq!(
        num(&nodes.s2, &landing_sql).await,
        rows.len() as u64,
        "the resend is on shard 2, once"
    );
    assert_eq!(num(&nodes.s1a, &landing_sql).await, 0, "and not on shard 1");
    assert_eq!(
        num(
            &nodes.s1a,
            &format!(
                "SELECT count() AS n FROM {db}.log_samples{SUFFIX} \
                 WHERE body = 'missing shard push'"
            )
        )
        .await,
        1,
        "its line is stored once"
    );

    drop_db(&nodes, db).await;
    std::fs::remove_dir_all(&root).ok();
}

/// **1-L6.** With the zone preferring shard 1's endpoint, every insert is
/// coordinated by shard 1, and the pushes keyed to shard 2 are stored on
/// shard 2: the shard follows the key, not the zone.
#[tokio::test]
async fn the_shard_follows_the_key_and_not_the_zone() {
    skip_unless_live!();
    let _serial = SERIAL.lock().await;
    let db = DB_ZONE.name();
    build_schema(db);
    let nodes = Nodes::of(db).await;
    require_two_shards(&nodes).await;
    assert_stamp_selects(&nodes.s1a, SHARD1_MS, 0).await;
    assert_stamp_selects(&nodes.s1a, SHARD2_MS, 1).await;

    let root = spool_root("zone");
    let mut cfg = config(&[(&shard1(), Some("a")), (&shard2(), Some("b"))], db);
    cfg.local_zone = Some("a".to_string());
    let zoned = Arc::new(ChClient::new(cfg).await.expect("connect"));
    let forwarder = RecordingForwarder::<LogLandingRow>::new(zoned);
    let writer = LogWriter::with_landing_inserter_and_runtime(
        forwarder.clone(),
        runtime(&root, 3),
        WriterTables::landing_for(Some(SUFFIX)),
    );
    let pushes = [
        ("zone push s1 0", SHARD1_MS),
        ("zone push s2 0", SHARD2_MS),
        ("zone push s1 1", SHARD1_MS),
        ("zone push s2 1", SHARD2_MS),
    ];
    for (i, (body, stamp)) in pushes.iter().enumerate() {
        forwarder.set_stamp(*stamp);
        writer
            .admit_flush(
                &no_tenant(),
                log_push(40 + i as u128, &[body]),
                PushHeaders::default(),
            )
            .expect("the queue has room")
            .await
            .unwrap_or_else(|e| panic!("{body}: {e:?}"));
    }
    writer.shutdown(Duration::from_secs(10)).await;

    let initial_inserts = format!(
        "SELECT count() AS n FROM system.query_log \
         WHERE type = 'QueryFinish' AND is_initial_query AND query_kind = 'Insert' \
         AND (current_database = '{db}' OR has(databases, '{db}'))"
    );
    for (_, client) in nodes.all() {
        exec(client, "SYSTEM FLUSH LOGS").await;
    }
    assert_eq!(
        num(&nodes.s1a, &initial_inserts).await,
        4,
        "shard 1 coordinated every insert"
    );
    assert_eq!(
        num(&nodes.s2, &initial_inserts).await,
        0,
        "shard 2 coordinated none"
    );
    sync_everywhere(&nodes, db).await;
    for (body, stamp) in pushes {
        let (here, there) = if stamp == SHARD1_MS {
            (&nodes.s1a, &nodes.s2)
        } else {
            (&nodes.s2, &nodes.s1a)
        };
        assert_eq!(
            lines_with_body(here, db, body).await,
            1,
            "{body} on its shard"
        );
        assert_eq!(
            lines_with_body(there, db, body).await,
            0,
            "{body} not on the other"
        );
    }

    drop_db(&nodes, db).await;
    std::fs::remove_dir_all(&root).ok();
}

// -- 1-L7: one block, one token, three servers --------------------------

/// The three blocks 1-L7 sends, every row stamped [`SHARD1_MS`].
struct Blocks {
    logs: Vec<LogLandingRow>,
    metrics: Vec<MetricLandingRow>,
    traces: Vec<TraceLandingRow>,
}

fn blocks() -> Blocks {
    let org: Arc<str> = Arc::from("");
    let ms = SHARD1_MS;
    let now = now_ms() - 60_000;
    let ts_ns = now * 1_000_000;

    // Logs: three lines of one stream, its registration, one pattern of 3.
    let fingerprint = 7u128;
    let mut logs: Vec<LogLandingRow> = [
        "request 1 completed",
        "request 2 completed",
        "request 3 completed",
    ]
    .iter()
    .map(|b| LogLandingRow::line(&org, ms, &log_line(fingerprint, ts_ns, b)))
    .collect();
    logs.push(LogLandingRow::stream(
        &org,
        ms,
        &log_stream(fingerprint, ts_ns),
    ));
    logs.push(LogLandingRow::pattern(
        &org,
        ms,
        LogPatternRow {
            fingerprint: Fingerprint::from_raw(fingerprint),
            bucket_ns: ts_ns - ts_ns % 60_000_000_000,
            pattern: "request <_> completed".to_string(),
            level: pulsus_write::patterns::PatternLevel("info"),
            count: 3,
        },
    ));

    // Metrics: two float samples of one series, one native-histogram
    // sample, the series registration, one descriptor and one exemplar.
    let name: Arc<str> = Arc::from("http_requests_total");
    let series_fp = Fingerprint::from_raw(42);
    let series = SeriesRef {
        metric_name: name.clone(),
        fingerprint: series_fp,
        labels: LabelSet::from_normalized([
            ("job".to_string(), "checkout".to_string()),
            ("instance".to_string(), "a".to_string()),
        ])
        .0,
    };
    let metrics = vec![
        MetricLandingRow::float_sample(
            &org,
            ms,
            &MetricPoint {
                metric_name: name.clone(),
                fingerprint: series_fp,
                unix_milli: now,
                value: 1.0,
            },
        ),
        MetricLandingRow::float_sample(
            &org,
            ms,
            &MetricPoint {
                metric_name: name.clone(),
                fingerprint: series_fp,
                unix_milli: now + 1_000,
                value: 2.0,
            },
        ),
        MetricLandingRow::hist_sample(
            &org,
            ms,
            &HistogramPoint {
                metric_name: Arc::from("http_request_duration_seconds"),
                fingerprint: Fingerprint::from_raw(43),
                unix_milli: now,
                histogram: pulsus_model::NativeHistogram {
                    counter_reset_hint: pulsus_model::CounterResetHint::Unknown,
                    schema: 0,
                    zero_threshold: 0.0,
                    zero_count: 0,
                    count: 3,
                    sum: 1.5,
                    positive_spans: vec![pulsus_model::Span {
                        offset: 1,
                        length: 3,
                    }],
                    negative_spans: vec![],
                    positive_buckets: vec![1, -1, 2],
                    negative_buckets: vec![],
                    custom_values: vec![],
                },
            },
        ),
        MetricLandingRow::series(&org, ms, &series, now - now % ACTIVITY_BUCKET_MS, 0),
        MetricLandingRow::metadata(
            &org,
            ms,
            &MetricMetadata {
                metric_name: name.clone(),
                metric_type: "counter".to_string(),
                help: "requests".to_string(),
                unit: String::new(),
                updated_ns: ts_ns,
            },
        ),
        MetricLandingRow::exemplar(
            &org,
            ms,
            &ExemplarPoint {
                metric_name: name,
                fingerprint: series_fp,
                unix_milli: now,
                value: 0.25,
                trace_id: std::array::from_fn(|i| i as u8 + 1),
                span_id: std::array::from_fn(|i| i as u8 + 17),
                labels: LabelSet::from_normalized([("a".to_string(), "b".to_string())]).0,
                histogram: false,
            },
        ),
    ];

    // Traces: two spans of one trace, one with status error, under one
    // resource whose service name is a string, each span carrying one string
    // attribute, so `service_bucket_resources` and `tag_bucket_values` are
    // written: one resource, and a tag name and a tag value for each of the
    // span attribute and the resource's `service.name`.
    let start = u64::try_from(ts_ns).expect("a positive stamp");
    let parsed = trace_push(vec![
        otlp_span(0xb7, 1, "GET /a", start, false),
        otlp_span(0xb7, 2, "GET /b", start + 1_000, true),
    ]);
    let mut traces: Vec<TraceLandingRow> = Vec::new();
    traces.extend(
        parsed
            .spans
            .into_iter()
            .map(|s| TraceLandingRow::span(&org, ms, s)),
    );
    traces.extend(
        parsed
            .resources
            .into_iter()
            .map(|r| TraceLandingRow::resource(&org, ms, r)),
    );
    traces.extend(
        parsed
            .tag_names
            .into_iter()
            .map(|t| TraceLandingRow::tag_name(&org, ms, t)),
    );
    traces.extend(
        parsed
            .tag_values
            .into_iter()
            .map(|t| TraceLandingRow::tag_value(&org, ms, t)),
    );
    Blocks {
        logs,
        metrics,
        traces,
    }
}

/// How 1-L7 reads a derived table: through its wrapper, bare on each shard
/// and summed, or bare on one server once every replica is synced.
#[derive(Clone, Copy, Debug)]
enum Read {
    Wrapper,
    EachShard,
    ClusterWide,
}

/// Every table each landing table's views write, and how 1-L7 reads it.
const DERIVED: &[(&str, Read)] = &[
    ("log_metrics_5s", Read::Wrapper),
    ("log_patterns", Read::Wrapper),
    ("log_samples", Read::Wrapper),
    ("log_streams", Read::Wrapper),
    ("log_streams_idx", Read::Wrapper),
    ("metric_samples", Read::Wrapper),
    ("metric_hist_samples", Read::Wrapper),
    ("metric_exemplars", Read::Wrapper),
    ("metric_series", Read::Wrapper),
    ("metric_labels", Read::Wrapper),
    ("metric_label_index", Read::Wrapper),
    ("metric_label_values", Read::EachShard),
    ("metric_metadata", Read::ClusterWide),
    ("spans", Read::Wrapper),
    ("traces", Read::Wrapper),
    ("error_spans", Read::Wrapper),
    ("resources", Read::ClusterWide),
    ("tag_names", Read::ClusterWide),
    ("tag_values", Read::ClusterWide),
    ("tag_bucket_values", Read::ClusterWide),
    ("service_bucket_resources", Read::ClusterWide),
];

/// What `table` holds in `db`, read as `how` says: `sum(count)` for the two
/// tables that aggregate a count, `count()` for every other, with `FINAL`
/// where the engine replaces or aggregates.
async fn derived_count(nodes: &Nodes, db: &str, table: &str, how: Read) -> u64 {
    let engine = texts(
        &nodes.s1a,
        &format!(
            "SELECT engine AS s FROM system.tables WHERE database = '{db}' AND name = '{table}'"
        ),
    )
    .await
    .pop()
    .unwrap_or_else(|| panic!("{db}.{table} exists"));
    let final_ = if ["Replacing", "Aggregating", "Summing", "Collapsing"]
        .iter()
        .any(|e| engine.contains(e))
    {
        " FINAL"
    } else {
        ""
    };
    let measure = if table.starts_with("log_metrics_") || table == "log_patterns" {
        "toUInt64(sum(count))"
    } else {
        "count()"
    };
    match how {
        Read::Wrapper => {
            num(
                &nodes.s1a,
                &format!("SELECT {measure} AS n FROM {db}.{table}{SUFFIX}{final_}"),
            )
            .await
        }
        Read::EachShard => {
            let sql = format!("SELECT {measure} AS n FROM {db}.{table}{final_}");
            num(&nodes.s1a, &sql).await + num(&nodes.s2, &sql).await
        }
        Read::ClusterWide => {
            num(
                &nodes.s1a,
                &format!("SELECT {measure} AS n FROM {db}.{table}{final_}"),
            )
            .await
        }
    }
}

/// Sends `rows` under `settings` to `table` through each of `clients`.
async fn send_through<R: ChRow>(
    clients: &[(&str, &ChClient)],
    table: &str,
    rows: &[R],
    settings: &QuerySettings,
) {
    for (name, client) in clients {
        client
            .insert_block_with(table, rows, settings)
            .await
            .unwrap_or_else(|e| panic!("{table} through {name}: {e}"));
    }
}

fn kinds<T>(rows: &[T], kind: impl Fn(&T) -> u8) -> Vec<u8> {
    let mut out: Vec<u8> = rows.iter().map(kind).collect();
    out.sort_unstable();
    out
}

/// **1-L7.** One block under one token, sent through shard 1's first
/// replica, then shard 1's second, then shard 2's server, is stored once,
/// on shard 1, and every table its views write holds exactly what one insert
/// of the same block into a fresh control database holds.
#[tokio::test]
async fn one_block_resent_through_every_server_is_stored_once_on_its_shard() {
    skip_unless_live!();
    let _serial = SERIAL.lock().await;
    let db = DB_RESEND.name();
    let control = DB_RESEND_CONTROL.name();
    build_schema(db);
    build_schema(control);
    let nodes = Nodes::of(db).await;
    let controls = Nodes::of(control).await;
    require_two_shards(&nodes).await;
    assert_stamp_selects(&nodes.s1a, SHARD1_MS, 0).await;

    let blocks = blocks();
    assert_eq!(
        kinds(&blocks.logs, |r| r.kind),
        vec![0, 0, 0, 1, 2],
        "the logs block"
    );
    assert_eq!(
        kinds(&blocks.metrics, |r| r.kind),
        vec![0, 0, 1, 2, 3, 4],
        "the metrics block"
    );
    assert_eq!(
        kinds(&blocks.traces, |r| r.row_kind),
        vec![0, 0, 1, 2, 2, 3, 3],
        "the traces block"
    );
    let max_rows = 1_048_576;
    let log_settings = QuerySettings::landing_insert("routed-l7-logs", max_rows);
    let metric_settings = QuerySettings::landing_insert("routed-l7-metrics", max_rows);
    let trace_settings = QuerySettings::trace_landing_insert("routed-l7-traces", max_rows);
    let log_table = WriterTables::landing_for(Some(SUFFIX)).landing;
    let metric_table = MetricWriterTables::landing_for(Some(SUFFIX)).landing;
    let trace_table = TraceWriterTables::landing_for(Some(SUFFIX)).landing;

    let order = [
        ("shard 1, first replica", &nodes.s1a),
        ("shard 1, second replica", &nodes.s1b),
        ("shard 2", &nodes.s2),
    ];
    send_through(&order, &log_table, &blocks.logs, &log_settings).await;
    send_through(&order, &metric_table, &blocks.metrics, &metric_settings).await;
    send_through(&order, &trace_table, &blocks.traces, &trace_settings).await;
    let once = [("control, shard 1", &controls.s1a)];
    send_through(&once, "log_landing", &blocks.logs, &log_settings).await;
    send_through(&once, "metric_landing", &blocks.metrics, &metric_settings).await;
    send_through(&once, "trace_landing", &blocks.traces, &trace_settings).await;

    sync_everywhere(&nodes, db).await;
    sync_everywhere(&controls, control).await;

    for (landing, rows) in [
        ("log_landing", blocks.logs.len() as u64),
        ("metric_landing", blocks.metrics.len() as u64),
        ("trace_landing", blocks.traces.len() as u64),
    ] {
        let sql = format!("SELECT count() AS n FROM {db}.{landing}");
        for (name, client) in [
            ("shard 1, first replica", &nodes.s1a),
            ("shard 1, second replica", &nodes.s1b),
        ] {
            assert_eq!(num(client, &sql).await, rows, "{name} holds {landing} once");
        }
        assert_eq!(
            num(&nodes.s2, &sql).await,
            0,
            "shard 2 holds none of {landing}"
        );
    }

    let mut want = std::collections::BTreeMap::new();
    for (table, how) in DERIVED {
        let got = derived_count(&nodes, db, table, *how).await;
        let expected = derived_count(&controls, control, table, *how).await;
        assert_eq!(got, expected, "{table} ({how:?}) against the control");
        want.insert(*table, expected);
    }
    for (table, n) in [
        ("log_samples", 3),
        ("metric_samples", 2),
        ("metric_hist_samples", 1),
        ("metric_exemplars", 1),
        ("spans", 2),
        ("error_spans", 1),
        ("resources", 1),
    ] {
        assert_eq!(want[table], n, "the control's {table}");
    }
    for table in ["tag_bucket_values", "service_bucket_resources"] {
        assert!(
            want[table] > 0,
            "the control's {table} is compared, not empty"
        );
    }

    drop_db(&nodes, db).await;
    drop_db(&controls, control).await;
}
