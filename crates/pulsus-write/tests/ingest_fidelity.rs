//! Ingest fidelity gate (issue #16 plan amendment, [medium] finding): proves
//! that a **raw OTLP log payload** produces byte-identical `log_samples`/
//! `log_streams`/`log_streams_idx` rows whether it travels through the
//! **product ingest path** (Path A: protobuf decode -> `otlp_logs::parse` ->
//! `LogWriter` -> ClickHouse) or through the **bulk RowBinary direct-write
//! path** (Path B: an independently-written flattener feeding
//! `ChClient::insert_block` directly — the same mechanism `xtask bench
//! logs-read`'s dataset generator uses to load the CI-scale/Tier-2
//! corpora). Both paths are asserted against a third, independent
//! reference: a **hand-derived golden expectation** per fixture — not
//! `A == B`, which could mask a shared bug, and not a value computed by
//! calling `pulsus-write`/`pulsus-model` inside this test (which would make
//! the assertion tautological).
//!
//! **"Hand-derived" in practice.** Every field in a fixture's `golden`
//! block except `fingerprint` is derived by hand, directly from
//! docs/architecture.md §2.2/§2.3's documented canonicalization rules
//! (character-class substitution, key sorting, greatest-key/greatest-value
//! collision tie-break, timestamp fallback order). `fingerprint` is a
//! 64-bit `cityHash64` — infeasible to hand-compute — so it is derived from
//! an **independent oracle: ClickHouse's own `cityHash64`**, not
//! `pulsus_model::stream_fingerprint` (issue #16 CODE review [medium]
//! finding: a golden produced by calling the same Rust implementation under
//! test makes the fingerprint assertion tautological — `A`, `B`, and the
//! golden would all silently share a canonicalization bug). The buffer
//! layout being hashed is still the hand-derived, documented one
//! (`pulsus_model::fingerprint::build_stream_buffer`: labels sorted by key,
//! `key ++ 0xFF ++ value ++ 0xFF` per pair, concatenated) — only the hash
//! primitive itself is infeasible to hand-compute, so *that* step alone
//! goes to ClickHouse. Each fixture's `golden.fingerprint` literal was
//! produced once, offline, by running:
//!
//! ```sql
//! SELECT cityHash64(concat(
//!     '<key1>', unhex('FF'), '<value1>', unhex('FF'),
//!     '<key2>', unhex('FF'), '<value2>', unhex('FF'),
//!     -- ... one ('<key>', unhex('FF'), '<value>', unhex('FF')) group per
//!     -- label, in the same sorted-key order as golden.labels_json --
//! ))
//! ```
//!
//! against a live ClickHouse 24.8 server, substituting that fixture's own
//! `golden.labels_json` keys/values in sorted order — e.g. case 1's
//! `deployment_environment`/`k8s_pod_name`/`otel_scope_name`/
//! `otel_scope_version`/`service_name` pairs, in that order. `unhex('FF')`
//! avoids any ambiguity from client-side string-escaping rules for the
//! `0xFF` separator byte. This test itself never calls `stream_fingerprint`
//! (or any other `pulsus-model`/`pulsus-write` function) to produce an
//! expectation. Path B's own write (below) uses the *identical* live
//! `cityHash64` SQL derivation — [`ch_stream_fingerprint`] — rather than
//! `pulsus_model::stream_fingerprint`, so Path B never touches the code
//! path being validated either; only Path A (the real product ingest path)
//! exercises `pulsus_model::stream_fingerprint`.
//!
//! **Scope note (deviation from the architect plan's case list).** The
//! plan's case 2 names "resource-level and log-record-level attrs"; this
//! crate's `otlp_logs::parse` (issue #8, `build_scope_labels`) only ever
//! promotes **resource** and **scope** (`ScopeLogs`) attributes into the
//! label set — a `LogRecord`'s own `attributes` field is never read for
//! labels, confirmed by `protocols/otlp_logs.rs`'s exhaustive unit tests.
//! Adding per-record attribute promotion would be a parser behavior change,
//! out of scope for a read-path benchmark issue. Case 2 here instead
//! exercises **resource vs. scope** attribute flattening (the two sources
//! that actually exist), which is what this crate implements today.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`, reusing the harness pattern
//! from `crates/pulsus-schema/tests/live_schema.rs` /
//! `crates/pulsus-read/tests/explain_indexes.rs`.
//!
//! Run locally:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-write --test ingest_fidelity
//! podman rm -f pulsus-ch-test
//! ```

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::routing::post;
use futures::StreamExt;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use tower::ServiceExt;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_config::WriterConfig;
use pulsus_model::{Date, LabelSet};
use pulsus_schema::{RenderCtx, SchemaParams, run_init};
use pulsus_write::ingest::http::logs;
use pulsus_write::writer::{LogSampleRow, LogStreamRow};
use pulsus_write::{LogWriter, WriterTables};

fn should_run() -> bool {
    std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1")
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-write/tests/ingest_fidelity.rs for setup)"
            );
            return;
        }
    };
}

fn base_config() -> ChConnConfig {
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
        query_timeout: Duration::from_secs(20),
        ..ChConnConfig::default()
    }
}

fn db_config(db: &str) -> ChConnConfig {
    ChConnConfig {
        database: db.to_string(),
        ..base_config()
    }
}

fn schema_params(db: &str) -> SchemaParams {
    RenderCtx {
        db: db.to_string(),
        cluster: None,
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
    }
}

/// Nanoseconds since the Unix epoch, right now. Fixture timestamps are
/// expressed as an offset from this value (never a fixed historical
/// literal): `log_samples`'s `ttl_only_drop_parts = 1` retention makes an
/// already-expired part deletion-eligible almost immediately, exactly the
/// hazard `crates/pulsus-read/tests/explain_indexes.rs::now_ns` documents.
fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits i64")
}

/// Prepares a fresh, empty database (`DROP DATABASE IF EXISTS` + `run_init`)
/// and returns a client bound to it.
async fn fresh_db(db: &str) -> ChClient {
    let admin = ChClient::new(base_config()).await.expect("connect admin");
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
    run_init(&admin, &schema_params(db))
        .await
        .expect("run_init");
    ChClient::new(db_config(db)).await.expect("connect db")
}

// ---------------------------------------------------------------------
// Fixture format.
// ---------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct FixtureFile {
    resource_attributes: Vec<(String, String)>,
    scope_name: String,
    scope_version: String,
    scope_attributes: Vec<(String, String)>,
    /// Offset in nanoseconds from [`now_ns`] used as `time_unix_nano`.
    /// `None` -> the OTLP wire value `0` ("unset").
    time_unix_nano_offset_ns: Option<i64>,
    observed_time_unix_nano_offset_ns: Option<i64>,
    severity_number: i32,
    body: String,
    golden: GoldenFile,
}

#[derive(serde::Deserialize)]
struct GoldenFile {
    service: String,
    labels_json: String,
    fingerprint: u64,
    body: String,
    severity: i8,
    /// Which offset field resolves to `timestamp_ns` under the documented
    /// OTLP fallback rule (`time_unix_nano` if non-zero, else
    /// `observed_time_unix_nano`, else "now") — pre-declared by the fixture
    /// author, not re-derived by re-implementing the fallback in this test.
    timestamp_source: String,
    idx_pairs: Vec<(String, String)>,
}

struct Fixture {
    file: FixtureFile,
    /// The resolved `time_unix_nano`/`observed_time_unix_nano` wire values
    /// and the expected `timestamp_ns`, computed once against a single
    /// [`now_ns`] snapshot so both paths and the assertion agree on the
    /// exact literal.
    time_unix_nano: u64,
    observed_time_unix_nano: u64,
    expected_timestamp_ns: i64,
}

fn load_fixture(name: &str) -> Fixture {
    let path = format!(
        "{}/tests/fixtures/otlp/{name}.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let bytes = std::fs::read(&path).unwrap_or_else(|e| panic!("read fixture {path}: {e}"));
    let file: FixtureFile =
        serde_json::from_slice(&bytes).unwrap_or_else(|e| panic!("parse fixture {path}: {e}"));

    let now = now_ns();
    let time_unix_nano = file
        .time_unix_nano_offset_ns
        .map(|o| (now + o) as u64)
        .unwrap_or(0);
    let observed_time_unix_nano = file
        .observed_time_unix_nano_offset_ns
        .map(|o| (now + o) as u64)
        .unwrap_or(0);
    let expected_timestamp_ns = match file.golden.timestamp_source.as_str() {
        "time_unix_nano" => time_unix_nano as i64,
        "observed_time_unix_nano" => observed_time_unix_nano as i64,
        other => panic!("fixture {name}: unsupported timestamp_source {other:?}"),
    };

    Fixture {
        file,
        time_unix_nano,
        observed_time_unix_nano,
        expected_timestamp_ns,
    }
}

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.to_string())),
        }),
        key_strindex: 0,
    }
}

/// Builds the raw OTLP protobuf request Path A ingests: one
/// `ResourceLogs` > one `ScopeLogs` > one `LogRecord`, straight off the
/// fixture's fields.
fn build_request(f: &Fixture) -> ExportLogsServiceRequest {
    let resource = Resource {
        attributes: f
            .file
            .resource_attributes
            .iter()
            .map(|(k, v)| kv(k, v))
            .collect(),
        dropped_attributes_count: 0,
        entity_refs: vec![],
    };
    let scope = InstrumentationScope {
        name: f.file.scope_name.clone(),
        version: f.file.scope_version.clone(),
        attributes: f
            .file
            .scope_attributes
            .iter()
            .map(|(k, v)| kv(k, v))
            .collect(),
        dropped_attributes_count: 0,
    };
    let record = LogRecord {
        time_unix_nano: f.time_unix_nano,
        observed_time_unix_nano: f.observed_time_unix_nano,
        severity_number: f.file.severity_number,
        body: Some(AnyValue {
            value: Some(Value::StringValue(f.file.body.clone())),
        }),
        ..Default::default()
    };
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(resource),
            scope_logs: vec![ScopeLogs {
                scope: Some(scope),
                log_records: vec![record],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

// ---------------------------------------------------------------------
// Path A: product ingest (protobuf -> POST /v1/logs -> LogWriter).
// ---------------------------------------------------------------------

async fn run_path_a(db: &str, f: &Fixture) {
    let client = fresh_db(db).await;
    let writer = Arc::new(LogWriter::new_with_tables(
        Arc::new(client),
        &WriterConfig::default(),
        WriterTables::logs_default(),
    ));
    let router: Router = Router::new()
        .route("/v1/logs", post(logs::<LogWriter>))
        .with_state(writer);

    let body = build_request(f).encode_to_vec();
    let request = axum::http::Request::builder()
        .method("POST")
        .uri("/v1/logs")
        .body(Body::from(body))
        .expect("build request");
    let response = router.oneshot(request).await.expect("router call");
    assert_eq!(
        response.status(),
        axum::http::StatusCode::OK,
        "product ingest path must accept a well-formed request"
    );
}

// ---------------------------------------------------------------------
// Path B: bulk RowBinary direct write. Independently flattens the same
// fixture fields (its own loop, not `otlp_logs::build_scope_labels`) and
// inserts straight through `ChClient::insert_block` — the mechanism `xtask
// bench logs-read`'s dataset generator uses for the CI-scale/Tier-2
// corpora. Reuses `LabelSet::from_normalized` (`pulsus-model`'s frozen
// canonicalization primitive, the single source of truth every writer —
// product or bulk — must agree with) and the `LogSampleRow`/
// `LogStreamRow` wire shapes; everything else (attribute-pair ordering,
// timestamp/severity resolution, and — deliberately — the fingerprint
// hash itself, [`ch_stream_fingerprint`] not `pulsus_model::
// stream_fingerprint`) is written fresh here.
// ---------------------------------------------------------------------

/// Computes a label set's stream fingerprint via a **live** `SELECT
/// cityHash64(...)` against this test's own ClickHouse connection — the
/// same independent-oracle derivation the fixtures' `golden.fingerprint`
/// literals were produced with (see the module doc comment), built from
/// the documented buffer layout (`pulsus_model::fingerprint::
/// build_stream_buffer`: labels sorted by key — guaranteed by
/// [`LabelSet`]'s own invariant — `key ++ 0xFF ++ value ++ 0xFF` per pair).
/// Deliberately does **not** call `pulsus_model::stream_fingerprint`:
/// issue #16 CODE review [medium] finding — Path B computing its
/// comparison value with the same Rust function the golden was once
/// derived from would make the assertion tautological even after fixing
/// the golden. `unhex('FF')` avoids any ambiguity from client-side
/// string-escaping rules for the separator byte.
async fn ch_stream_fingerprint(client: &ChClient, labels: &LabelSet) -> u64 {
    let mut parts = Vec::new();
    for (k, v) in labels.iter() {
        parts.push(format!("'{}'", sql_escape(k)));
        parts.push("unhex('FF')".to_string());
        parts.push(format!("'{}'", sql_escape(v)));
        parts.push("unhex('FF')".to_string());
    }
    let sql = format!("SELECT cityHash64(concat({})) AS fp", parts.join(", "));

    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct FpRow {
        fp: u64,
    }
    let mut stream = client
        .query_stream::<FpRow>(&sql, &QuerySettings::new())
        .await
        .expect("query cityHash64 for the Path B fingerprint oracle");
    stream
        .next()
        .await
        .expect("cityHash64 returns exactly one row")
        .expect("decode fp row")
        .fp
}

/// Minimal SQL string-literal escaping for the label keys/values
/// [`ch_stream_fingerprint`] inlines into a `SELECT` — every fixture's
/// labels are plain ASCII identifiers/values with no quotes or
/// backslashes, so this only needs to be correct for that input space, not
/// general-purpose.
fn sql_escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

async fn run_path_b(db: &str, f: &Fixture) {
    let client = fresh_db(db).await;

    let mut pairs: Vec<(String, String)> = Vec::new();
    pairs.extend(f.file.resource_attributes.iter().cloned());
    pairs.push(("otel_scope_name".to_string(), f.file.scope_name.clone()));
    pairs.push((
        "otel_scope_version".to_string(),
        f.file.scope_version.clone(),
    ));
    pairs.extend(f.file.scope_attributes.iter().cloned());

    let (labels, _collisions) = LabelSet::from_normalized(pairs);
    let fingerprint = ch_stream_fingerprint(&client, &labels).await;
    let service = labels.service().to_string();

    let timestamp_ns = f.expected_timestamp_ns;
    let severity = if (1..=24).contains(&f.file.severity_number) {
        f.file.severity_number as i8
    } else {
        0
    };

    let sample = LogSampleRow {
        service: service.clone(),
        fingerprint,
        timestamp_ns,
        severity,
        body: f.file.body.clone(),
    };
    let stream = LogStreamRow {
        month: Date::start_of_month_utc(timestamp_ns).days_since_epoch(),
        fingerprint,
        service,
        labels: labels.to_canonical_json(),
        updated_ns: now_ns(),
    };

    client
        .insert_block("log_samples", &[sample])
        .await
        .expect("insert log_samples");
    client
        .insert_block("log_streams", &[stream])
        .await
        .expect("insert log_streams");
}

// ---------------------------------------------------------------------
// Read-back + assertions.
// ---------------------------------------------------------------------

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SampleReadRow {
    service: String,
    fingerprint: u64,
    timestamp_ns: i64,
    severity: i8,
    body: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct StreamReadRow {
    fingerprint: u64,
    labels: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct IdxReadRow {
    key: String,
    val: String,
}

async fn fetch_sample(client: &ChClient, db: &str, fingerprint: u64) -> Option<SampleReadRow> {
    let sql = format!(
        "SELECT service, fingerprint, timestamp_ns, severity, body FROM {db}.log_samples \
         WHERE fingerprint = {fingerprint} ORDER BY timestamp_ns LIMIT 1"
    );
    let mut stream = client
        .query_stream::<SampleReadRow>(&sql, &QuerySettings::new())
        .await
        .expect("query log_samples");
    stream.next().await.and_then(|r| r.ok())
}

async fn fetch_stream(client: &ChClient, db: &str, fingerprint: u64) -> Option<StreamReadRow> {
    let sql = format!(
        "SELECT fingerprint, labels FROM {db}.log_streams WHERE fingerprint = {fingerprint} \
         LIMIT 1 BY fingerprint"
    );
    let mut stream = client
        .query_stream::<StreamReadRow>(&sql, &QuerySettings::new())
        .await
        .expect("query log_streams");
    stream.next().await.and_then(|r| r.ok())
}

async fn fetch_idx_pairs(client: &ChClient, db: &str, fingerprint: u64) -> Vec<(String, String)> {
    // `GROUP BY` rather than `FINAL` — query-time dedup, docs/architecture.md
    // §3.2's documented convention for `log_streams_idx`'s
    // `ReplacingMergeTree`.
    let sql = format!(
        "SELECT key, val FROM {db}.log_streams_idx WHERE fingerprint = {fingerprint} \
         GROUP BY key, val ORDER BY key"
    );
    let mut out = Vec::new();
    let mut stream = client
        .query_stream::<IdxReadRow>(&sql, &QuerySettings::new())
        .await
        .expect("query log_streams_idx");
    while let Some(row) = stream.next().await {
        let row = row.expect("decode idx row");
        out.push((row.key, row.val));
    }
    out
}

/// Polls `log_streams_idx` until it carries `expected` rows for
/// `fingerprint`, or a bounded deadline elapses — the `log_streams_idx_mv`
/// materialized view settle-time guard the architect plan requires ("no
/// fixed sleeps", docs/architecture.md §9 convention), mirroring
/// `crates/pulsus-schema/tests/live_cluster.rs::poll_until_matching`.
async fn poll_until_idx_settled(
    client: &ChClient,
    db: &str,
    fingerprint: u64,
    expected: usize,
) -> Vec<(String, String)> {
    let mut last = Vec::new();
    for _ in 0..40 {
        last = fetch_idx_pairs(client, db, fingerprint).await;
        if last.len() == expected {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    last
}

/// Runs both paths for `fixture_name` against two independent, freshly
/// initialized databases, then asserts **A == golden** and **B == golden**
/// (not `A == B`) across `log_samples`, `log_streams`, and the MV-derived
/// `log_streams_idx`.
async fn assert_fidelity(case: &str) {
    skip_unless_live!();
    let fixture = load_fixture(case);
    let db_a = format!("pulsus_write_fidelity_{case}_a");
    let db_b = format!("pulsus_write_fidelity_{case}_b");

    run_path_a(&db_a, &fixture).await;
    run_path_b(&db_b, &fixture).await;

    let read_client_a = ChClient::new(db_config(&db_a)).await.expect("connect a");
    let read_client_b = ChClient::new(db_config(&db_b)).await.expect("connect b");

    let fp = fixture.file.golden.fingerprint;

    // --- log_samples ---
    let sample_a = fetch_sample(&read_client_a, &db_a, fp)
        .await
        .unwrap_or_else(|| panic!("{case}: path A produced no log_samples row"));
    let sample_b = fetch_sample(&read_client_b, &db_b, fp)
        .await
        .unwrap_or_else(|| panic!("{case}: path B produced no log_samples row"));

    for (label, sample) in [("A", &sample_a), ("B", &sample_b)] {
        assert_eq!(
            sample.service, fixture.file.golden.service,
            "{case} path {label}: service"
        );
        assert_eq!(
            sample.fingerprint, fixture.file.golden.fingerprint,
            "{case} path {label}: fingerprint"
        );
        assert_eq!(
            sample.timestamp_ns, fixture.expected_timestamp_ns,
            "{case} path {label}: timestamp_ns"
        );
        assert_eq!(
            sample.severity, fixture.file.golden.severity,
            "{case} path {label}: severity"
        );
        assert_eq!(
            sample.body, fixture.file.golden.body,
            "{case} path {label}: body"
        );
    }

    // --- log_streams ---
    let stream_a = fetch_stream(&read_client_a, &db_a, fp)
        .await
        .unwrap_or_else(|| panic!("{case}: path A produced no log_streams row"));
    let stream_b = fetch_stream(&read_client_b, &db_b, fp)
        .await
        .unwrap_or_else(|| panic!("{case}: path B produced no log_streams row"));
    assert_eq!(
        stream_a.labels, fixture.file.golden.labels_json,
        "{case} path A: canonical labels JSON"
    );
    assert_eq!(
        stream_b.labels, fixture.file.golden.labels_json,
        "{case} path B: canonical labels JSON"
    );

    // --- log_streams_idx (MV-derived) ---
    let expected_idx = fixture.file.golden.idx_pairs.clone();
    let idx_a = poll_until_idx_settled(&read_client_a, &db_a, fp, expected_idx.len()).await;
    let idx_b = poll_until_idx_settled(&read_client_b, &db_b, fp, expected_idx.len()).await;
    assert_eq!(idx_a, expected_idx, "{case} path A: log_streams_idx rows");
    assert_eq!(idx_b, expected_idx, "{case} path B: log_streams_idx rows");
}

#[tokio::test]
async fn label_canonicalization_order() {
    assert_fidelity("case1_label_canonicalization_order").await;
}

#[tokio::test]
async fn resource_vs_scope_attributes() {
    assert_fidelity("case2_resource_vs_scope_attributes").await;
}

#[tokio::test]
async fn duplicate_colliding_labels() {
    assert_fidelity("case3_duplicate_colliding_labels").await;
}

#[tokio::test]
async fn timestamp_units() {
    assert_fidelity("case4_timestamp_units").await;
}

#[tokio::test]
async fn nonascii_body_encoding() {
    assert_fidelity("case5_nonascii_body").await;
}

#[tokio::test]
async fn mv_created_idx_rows() {
    assert_fidelity("case6_mv_created_idx_rows").await;
}
