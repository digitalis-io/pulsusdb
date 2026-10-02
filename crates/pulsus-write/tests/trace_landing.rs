//! `TraceWriter`'s landing half: **one push is one insert of one block into
//! `trace_landing`** (issue #586), the three per-push ceilings, the joint
//! admission that decides for both write paths at once, the push-suppression
//! index over three targets, and the two refusal containers the trace
//! transports render. All against mock `BlockInserter`s — no real ClickHouse,
//! except the one case whose whole observable is a `system` table and which
//! gates itself on `PULSUS_TEST_CLICKHOUSE=1`.
//!
//! **Nothing here reads a target table.** The five derived trace tables are
//! maintained by materialized view; what this file pins is the block the
//! writer hands over, and what the old two-table path does beside it.
//!
//! **No case here compares the old store with the new one.** The old path
//! keeps writing `trace_spans` and `trace_attrs_idx` until task 20, and no
//! equivalence between the two is claimed, required or tested; where a case
//! names `trace_spans` it is asserting that table's own expected value.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::http::{HeaderMap, StatusCode, header};
use prost::Message as _;

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

use pulsus_clickhouse::{ChError, ChRow, MAX_JSON_PATHS_PER_VALUE, QuerySettings};
use pulsus_config::{ByteSize, WriterConfig};
use pulsus_write::writer::{
    BlockInserter, TRACE_LANDING_DAY_LIMIT, TraceAttrRow, TraceLandingRow, TraceSpanRow,
    TraceWriter, TraceWriterTables, WriterRuntime, landing_block_overhead_bytes,
};
use pulsus_write::{
    AdmitRefusal, ParsedTraceLanding, ParsedTraces, PushHeaders, TraceSink,
    push_spans_too_many_days_message,
};

const LANDING: &str = "trace_landing";
const SPANS: &str = "trace_spans";
const ATTRS: &str = "trace_attrs_idx";

/// One nanosecond-resolution UTC day.
const NS_PER_DAY: i64 = 86_400_000_000_000;

// -- the mock inserter ------------------------------------------------

/// What one insert call does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Act {
    Ok,
    /// A non-retryable error: provably not committed.
    Poison,
    /// Never returns, so the block it was handed stays charged.
    Hang,
}

/// A scriptable mock [`BlockInserter`], generic over the row type so one
/// struct serves the landing table and both old-path tables. Records every
/// call's table, **rows** and settings.
///
/// The rows are kept as themselves rather than as `serde_json::Value`:
/// `TraceLandingRow::resource_id` is a `UInt128` and `serde_json` refuses a
/// `u128` above the `u64` range, so a JSON copy of a span row or a resource
/// row fails to serialise at all — measured, `number out of range`.
struct MockInserter<R> {
    act: Act,
    record: bool,
    calls: AtomicUsize,
    rows: Mutex<Vec<Vec<R>>>,
    settings: Mutex<Vec<Vec<(String, String)>>>,
    tables: Mutex<Vec<String>>,
    empty: QuerySettings,
}

impl<R> MockInserter<R> {
    fn always(act: Act) -> Arc<Self> {
        Self::built(act, true)
    }

    /// [`Self::always`], keeping no copy of what it was handed — for a case
    /// that pushes enough rows that a copy of each would dominate it.
    fn unrecorded(act: Act) -> Arc<Self> {
        Self::built(act, false)
    }

    fn built(act: Act, record: bool) -> Arc<Self> {
        Arc::new(MockInserter {
            act,
            record,
            calls: AtomicUsize::new(0),
            rows: Mutex::new(Vec::new()),
            settings: Mutex::new(Vec::new()),
            tables: Mutex::new(Vec::new()),
            empty: QuerySettings::new(),
        })
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn settings_of(&self, call: usize) -> Vec<(String, String)> {
        self.settings
            .lock()
            .expect("mock mutex poisoned")
            .get(call)
            .cloned()
            .unwrap_or_else(|| panic!("no insert call at index {call}"))
    }

    fn tables(&self) -> Vec<String> {
        self.tables.lock().expect("mock mutex poisoned").clone()
    }
}

impl<R: Clone> MockInserter<R> {
    fn rows_of(&self, call: usize) -> Vec<R> {
        self.rows
            .lock()
            .expect("mock mutex poisoned")
            .get(call)
            .cloned()
            .unwrap_or_else(|| panic!("no insert call at index {call}"))
    }
}

impl<R: ChRow + Clone> BlockInserter<R> for MockInserter<R> {
    fn insert<'a>(
        &'a self,
        table: &'a str,
        rows: &'a [R],
    ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>> {
        self.insert_with(table, rows, &self.empty)
    }

    fn insert_with<'a>(
        &'a self,
        table: &'a str,
        rows: &'a [R],
        extra: &'a QuerySettings,
    ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.record {
            self.tables
                .lock()
                .expect("mock mutex poisoned")
                .push(table.to_string());
            self.rows
                .lock()
                .expect("mock mutex poisoned")
                .push(rows.to_vec());
            self.settings.lock().expect("mock mutex poisoned").push(
                extra
                    .entries()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            );
        }
        let act = self.act;
        Box::pin(async move {
            match act {
                Act::Ok => Ok(()),
                Act::Poison => Err(ChError::Decode("mock poison".to_string())),
                Act::Hang => std::future::pending::<Result<(), ChError>>().await,
            }
        })
    }
}

/// A mock whose first call fails pre-send retryably and whose later calls
/// succeed, so the loop resends the identical block under the identical
/// token.
struct ResendOnceInserter {
    calls: AtomicUsize,
    settings: Mutex<Vec<Vec<(String, String)>>>,
}

impl ResendOnceInserter {
    fn new() -> Arc<Self> {
        Arc::new(ResendOnceInserter {
            calls: AtomicUsize::new(0),
            settings: Mutex::new(Vec::new()),
        })
    }

    fn tokens(&self) -> Vec<String> {
        self.settings
            .lock()
            .expect("mock mutex poisoned")
            .iter()
            .map(|entries| token_of(entries))
            .collect()
    }
}

impl<R: ChRow> BlockInserter<R> for ResendOnceInserter {
    fn insert<'a>(
        &'a self,
        _table: &'a str,
        _rows: &'a [R],
    ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    fn insert_with<'a>(
        &'a self,
        _table: &'a str,
        _rows: &'a [R],
        extra: &'a QuerySettings,
    ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        self.settings.lock().expect("mock mutex poisoned").push(
            extra
                .entries()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        );
        Box::pin(async move {
            if call == 0 {
                Err(ChError::Timeout("mock pre-send retryable".to_string()))
            } else {
                Ok(())
            }
        })
    }
}

fn token_of(entries: &[(String, String)]) -> String {
    entries
        .iter()
        .find(|(k, _)| k == "insert_deduplication_token")
        .map(|(_, v)| v.clone())
        .unwrap_or_else(|| panic!("every landing insert carries a token: {entries:?}"))
}

// -- writer construction ----------------------------------------------

fn spool_root(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pulsus-trace-landing-{name}-{}-{}",
        std::process::id(),
        unique()
    ));
    std::fs::create_dir_all(&dir).expect("create the spool root");
    dir
}

fn unique() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

fn runtime_at(cfg: &WriterConfig, spool: &Path) -> WriterRuntime {
    let mut runtime = WriterRuntime::from_config(cfg);
    runtime.spool_dir = spool.to_path_buf();
    runtime
}

/// A writer whose three inserters are the mocks given.
fn writer_with(
    cfg: &WriterConfig,
    spool: &Path,
    spans: Arc<dyn BlockInserter<TraceSpanRow>>,
    attrs: Arc<dyn BlockInserter<TraceAttrRow>>,
    landing: Arc<dyn BlockInserter<TraceLandingRow>>,
) -> TraceWriter {
    TraceWriter::with_inserters_and_runtime(
        spans,
        attrs,
        landing,
        runtime_at(cfg, spool),
        TraceWriterTables::traces_default(),
    )
}

/// The common shape: a succeeding old path and the landing mock given.
fn writer_ok_old(
    cfg: &WriterConfig,
    spool: &Path,
    landing: Arc<MockInserter<TraceLandingRow>>,
) -> (
    TraceWriter,
    Arc<MockInserter<TraceSpanRow>>,
    Arc<MockInserter<TraceAttrRow>>,
) {
    let spans: Arc<MockInserter<TraceSpanRow>> = MockInserter::unrecorded(Act::Ok);
    let attrs: Arc<MockInserter<TraceAttrRow>> = MockInserter::unrecorded(Act::Ok);
    let writer = writer_with(cfg, spool, spans.clone(), attrs.clone(), landing);
    (writer, spans, attrs)
}

/// Yields until `done` holds, so a case can let the insert workers make
/// progress, then fails naming what never happened.
async fn settle_until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

// -- fixtures ---------------------------------------------------------

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.to_string())),
        }),
        key_strindex: 0,
    }
}

/// A fixed instant inside the admitted UTC-day domain, so a fixture's day
/// count is the number of days its spans were spread over and nothing else.
fn base_ns() -> i64 {
    1_760_000_000_000_000_000
}

fn span_at(trace: u8, id: u8, start_ns: i64, attrs: Vec<KeyValue>) -> Span {
    Span {
        trace_id: [trace; 16].to_vec(),
        span_id: [id; 8].to_vec(),
        parent_span_id: Vec::new(),
        name: "GET /api".to_string(),
        kind: 2,
        start_time_unix_nano: u64::try_from(start_ns).expect("a post-epoch fixture"),
        end_time_unix_nano: u64::try_from(start_ns + 1_000_000).expect("a post-epoch fixture"),
        attributes: attrs,
        ..Default::default()
    }
}

fn request_of(service: &str, spans: Vec<Span>) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![kv("service.name", service)],
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
    }
}

/// Both decodes of one request, exactly as a handler runs them: the old
/// path's and the landing path's, independently, over the same bytes.
fn decode_both(req: &ExportTraceServiceRequest) -> (ParsedTraces, ParsedTraceLanding) {
    decode_both_at(req, base_ns())
}

/// [`decode_both`] at a named receive clock — for a case that decodes one
/// body twice at two instants, as two requests carrying it would.
fn decode_both_at(
    req: &ExportTraceServiceRequest,
    now_ns: i64,
) -> (ParsedTraces, ParsedTraceLanding) {
    (
        pulsus_write::parse_traces(req, now_ns).expect("the old path's decode"),
        pulsus_write::parse_trace_landing(req, now_ns).expect("the landing decode"),
    )
}

/// **The fixture every row-count assertion below is decidable against.** Six
/// spans of one trace under one resource, each carrying the same two
/// attribute keys with the same two values, so the push's landed rows are:
/// 6 kind-0, 1 kind-1, 3 kind-2 (two span keys and the one resource key the
/// decode lists) and 3 kind-3.
fn six_spans() -> ExportTraceServiceRequest {
    let base = base_ns();
    request_of(
        "checkout",
        (0..6u8)
            .map(|i| {
                span_at(
                    0xaa,
                    0x10 + i,
                    base + i64::from(i) * 1_000_000,
                    vec![kv("http.route", "/api"), kv("http.method", "GET")],
                )
            })
            .collect(),
    )
}

/// `six_spans` carrying **no `start_time_unix_nano` and no
/// `end_time_unix_nano`** — the shape the decode has a fallback for: a span
/// with no start time takes the request's own receive clock, and the
/// attribute day of every row that span produces is derived from it.
fn six_spans_without_start_times() -> ExportTraceServiceRequest {
    request_of(
        "checkout",
        (0..6u8)
            .map(|i| Span {
                start_time_unix_nano: 0,
                end_time_unix_nano: 0,
                ..span_at(
                    0xaa,
                    0x10 + i,
                    base_ns(),
                    vec![kv("http.route", "/api"), kv("http.method", "GET")],
                )
            })
            .collect(),
    )
}

/// Two receive clocks either side of one UTC midnight, both inside the
/// admitted day domain: the same bytes decoded at the first and at the
/// second take a different fallback timestamp **and** a different attribute
/// day, so a case over the pair reaches every value the fallback feeds.
fn clocks_across_midnight() -> (i64, i64) {
    let midnight = (base_ns() / NS_PER_DAY + 1) * NS_PER_DAY;
    (midnight - 1_000_000_000, midnight + 1_000_000_000)
}

/// `six_spans` under a different trace id, so two pushes are two distinct
/// contents.
fn six_spans_of(trace: u8, service: &str) -> ExportTraceServiceRequest {
    let base = base_ns();
    request_of(
        service,
        (0..6u8)
            .map(|i| {
                span_at(
                    trace,
                    0x10 + i,
                    base + i64::from(i) * 1_000_000,
                    vec![kv("http.route", "/api"), kv("http.method", "GET")],
                )
            })
            .collect(),
    )
}

/// One span per distinct UTC date, `days` of them, plus enough spans to make
/// the push 200 spans in total — the shape §3.3's gate decides on.
fn spans_over_days(days: u64) -> ExportTraceServiceRequest {
    let base = (base_ns() / NS_PER_DAY) * NS_PER_DAY;
    let mut spans = Vec::new();
    for d in 0..days {
        spans.push(span_at(
            0xbb,
            (d % 251) as u8 + 1,
            base + (d as i64) * NS_PER_DAY + 3_600_000_000_000,
            vec![kv("http.route", "/api")],
        ));
    }
    // Pad to 200 spans, all on the first date, so the span count is the same
    // on both sides of the boundary and only the date count moves.
    while spans.len() < 200 {
        let i = spans.len() as i64;
        spans.push(span_at(
            0xbc,
            (i % 251) as u8 + 1,
            base + 3_600_000_000_000 + i,
            vec![kv("http.route", "/api")],
        ));
    }
    request_of("checkout", spans)
}

/// How many rows of one recorded insert call carry `kind`.
fn count_of_kind(rows: &[TraceLandingRow], kind: u8) -> usize {
    rows.iter().filter(|r| r.row_kind == kind).count()
}

// -- T1/T2: one push, one insert --------------------------------------

/// One push is one `insert_with` call, into `trace_landing`, carrying every
/// kind the push produced — and **none** naming any of the five target
/// tables or the old path's two.
#[tokio::test]
async fn one_push_is_one_insert_into_the_landing_table() {
    let root = spool_root("one-insert");
    let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::always(Act::Ok);
    let (writer, spans_mock, _attrs) =
        writer_ok_old(&WriterConfig::default(), &root, landing.clone());

    let req = six_spans();
    let (parsed, landed) = decode_both(&req);
    let expected_rows = landed.total_rows() as usize;
    writer
        .admit_flush(parsed, landed, PushHeaders::default())
        .expect("queue has room")
        .await
        .expect("the old path commits");
    settle_until("the landing insert", || landing.call_count() == 1).await;

    assert_eq!(landing.call_count(), 1, "one push is one landing insert");
    assert_eq!(landing.tables(), vec![LANDING.to_string()]);
    for table in [
        "spans",
        "resources",
        "traces",
        "tag_names",
        "tag_values",
        SPANS,
        ATTRS,
    ] {
        assert!(
            !landing.tables().iter().any(|t| t == table),
            "the landing inserter must never name {table}"
        );
    }

    let rows = landing.rows_of(0);
    assert_eq!(rows.len(), expected_rows);
    assert_eq!(count_of_kind(&rows, 0), 6, "six spans");
    assert_eq!(count_of_kind(&rows, 1), 1, "one resource for one day");
    assert!(
        count_of_kind(&rows, 2) >= 2,
        "both span attribute keys are listed"
    );
    assert!(count_of_kind(&rows, 3) >= 2, "and both of their values");

    // The old path keeps writing, which is this change's whole premise.
    assert_eq!(spans_mock.call_count(), 1);

    writer.shutdown(Duration::from_secs(5)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// Two pushes are two inserts carrying two distinct tokens, neither block
/// holding the other's rows.
#[tokio::test]
async fn two_pushes_are_two_inserts() {
    let root = spool_root("two-inserts");
    let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::always(Act::Ok);
    let (writer, _spans, _attrs) = writer_ok_old(&WriterConfig::default(), &root, landing.clone());

    for (trace, service) in [(0xa1u8, "checkout"), (0xa2, "cart")] {
        let req = six_spans_of(trace, service);
        let (parsed, landed) = decode_both(&req);
        writer
            .admit_flush(parsed, landed, PushHeaders::default())
            .expect("queue has room")
            .await
            .expect("the old path commits");
    }
    settle_until("both landing inserts", || landing.call_count() == 2).await;

    assert_eq!(landing.call_count(), 2, "two pushes are two inserts");
    let first = token_of(&landing.settings_of(0));
    let second = token_of(&landing.settings_of(1));
    assert_ne!(first, second, "each sealed block mints its own token");

    writer.shutdown(Duration::from_secs(5)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// A token is minted per sealed block and repeated byte-identically on a
/// resend, and it is **never derived from the content**: two byte-identical
/// pushes carry different tokens.
#[tokio::test]
async fn a_token_is_minted_per_sealed_block_and_repeated_on_resend() {
    let root = spool_root("token");
    let landing = ResendOnceInserter::new();
    let spans: Arc<MockInserter<TraceSpanRow>> = MockInserter::unrecorded(Act::Ok);
    let attrs: Arc<MockInserter<TraceAttrRow>> = MockInserter::unrecorded(Act::Ok);
    let cfg = WriterConfig::default();
    let writer = writer_with(
        &cfg,
        &root,
        spans.clone(),
        attrs.clone(),
        landing.clone() as Arc<dyn BlockInserter<TraceLandingRow>>,
    );

    let req = six_spans();
    let (parsed, landed) = decode_both(&req);
    writer
        .admit_flush(parsed, landed, PushHeaders::default())
        .expect("queue has room")
        .await
        .expect("the old path commits");
    settle_until("the resend", || landing.tokens().len() == 2).await;
    let tokens = landing.tokens();
    assert_eq!(tokens.len(), 2, "one failed attempt and one resend");
    assert_eq!(
        tokens[0], tokens[1],
        "the resend carries the identical token, so the server drops it as a repeat"
    );
    writer.shutdown(Duration::from_secs(5)).await;

    // The same bytes again, through a writer with suppression off, mint a
    // different token: the token cannot be a content digest.
    let cfg = WriterConfig {
        ingest_dedup: false,
        ..Default::default()
    };
    let landing2: Arc<MockInserter<TraceLandingRow>> = MockInserter::always(Act::Ok);
    let (writer2, _s, _a) = writer_ok_old(&cfg, &root, landing2.clone());
    for _ in 0..2 {
        let (parsed, landed) = decode_both(&six_spans());
        writer2
            .admit_flush(parsed, landed, PushHeaders::default())
            .expect("queue has room")
            .await
            .expect("the old path commits");
    }
    settle_until("two landing inserts", || landing2.call_count() == 2).await;
    assert_ne!(
        token_of(&landing2.settings_of(0)),
        token_of(&landing2.settings_of(1)),
        "two byte-identical pushes are two genuine pushes and carry two tokens"
    );
    writer2.shutdown(Duration::from_secs(5)).await;
    std::fs::remove_dir_all(&root).ok();
}

// -- the three per-push ceilings --------------------------------------

/// **A push at the row ceiling is refused whole**, naming its own rows, the
/// row limit, its bytes and the byte limit; nothing is queued on either
/// path, nothing stays reserved, and the claim is rolled back rather than
/// left standing.
///
/// **The row count is over all four kinds**: a ceiling compared against the
/// span count alone passes at a push the server would split.
#[tokio::test]
async fn a_push_at_a_ceiling_is_refused_whole() {
    let root = spool_root("ceiling");
    let req = six_spans();
    let (parsed, landed) = decode_both(&req);
    let rows = landed.total_rows();
    assert!(
        rows > landed.spans.len() as u64,
        "the fixture has to carry more landed rows than spans, or this case \
         cannot tell the two counts apart: {rows} rows, {} spans",
        landed.spans.len()
    );

    let cfg = WriterConfig {
        trace_landing_max_rows: rows,
        ..Default::default()
    };
    let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::always(Act::Ok);
    let (writer, spans_mock, attrs_mock) = writer_ok_old(&cfg, &root, landing.clone());
    let before = writer.metrics().dedup.rollbacks_total;
    let err = writer
        .admit_flush(parsed.clone(), landed.clone(), PushHeaders::default())
        .expect_err("a push at the row ceiling is refused");
    let AdmitRefusal::PushTooLarge {
        rows: got_rows,
        row_limit,
        bytes,
        byte_limit,
    } = err
    else {
        panic!("expected PushTooLarge, got {err:?}");
    };
    assert_eq!(
        got_rows, rows,
        "the push's own row count, over all four kinds"
    );
    assert_eq!(row_limit, rows);
    assert_eq!(byte_limit, 16 * 1024 * 1024);
    assert!(bytes > 0, "the push's own byte estimate is reported");
    assert_eq!(landing.call_count(), 0, "a refusal stores nothing");
    assert_eq!(spans_mock.call_count(), 0, "on either path");
    assert_eq!(attrs_mock.call_count(), 0);
    assert_eq!(writer.metrics().queue_bytes, 0, "no bytes stay reserved");
    assert_eq!(writer.landing_metrics().queue_bytes, 0);
    assert_eq!(
        writer.metrics().dedup.rollbacks_total,
        before + 1,
        "the claim is rolled back, not left standing"
    );
    writer.shutdown(Duration::from_secs(5)).await;

    // The byte ceiling, at the push's own estimate and one below it.
    let charge = bytes;
    for (limit, refused) in [(charge, false), (charge - 1, true)] {
        let cfg = WriterConfig {
            batch_bytes: ByteSize(limit),
            ..Default::default()
        };
        let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::always(Act::Ok);
        let (writer, _s, _a) = writer_ok_old(&cfg, &root, landing.clone());
        let result = writer.admit_flush(parsed.clone(), landed.clone(), PushHeaders::default());
        if refused {
            let err = result.expect_err("refused at one byte under its own estimate");
            assert_eq!(
                err,
                AdmitRefusal::PushTooLarge {
                    rows,
                    row_limit: 1_048_576,
                    bytes: charge,
                    byte_limit: limit,
                }
            );
            assert_eq!(landing.call_count(), 0);
            assert_eq!(writer.landing_metrics().queue_bytes, 0);
        } else {
            result
                .expect("byte equality is admitted, not refused")
                .await
                .expect("the old path commits");
            settle_until("the landing insert", || landing.call_count() == 1).await;
        }
        writer.shutdown(Duration::from_secs(5)).await;
    }

    std::fs::remove_dir_all(&root).ok();
}

/// **The charge covers everything a queued block holds.** A span's events
/// and links are arrays of tuples, each element with its own text and its
/// own encoded `JSON`, and both are client-chosen and unbounded short of the
/// expansion ceiling — so a charge that priced the two vectors' headers
/// alone would not bound what the queue holds.
///
/// **It is asserted as a growth with the row count held fixed**, which is
/// what makes it discriminate: an assertion that the charge merely exceeds
/// the row slots plus the element text passes with the arrays' own term
/// deleted, because at 200 events of 20 attributes each the slots of the
/// catalog rows those attributes produce swamp the elements' text. So each
/// pair below lengthens one field that **no other kind's row carries**, four
/// times over, with every row count identical on both sides:
///
/// * an event's `name` — it produces no catalog row at all;
/// * an event attribute's value, under one key shared by all 200 events, so
///   the push's catalog rows are one name and one value whatever its length;
/// * a link's `trace_state`, likewise carried nowhere else.
#[tokio::test]
async fn the_charge_covers_everything_a_queued_block_holds() {
    let root = spool_root("charge");

    const SHORT: usize = 64;
    const LONG: usize = SHORT * 4;
    const EVENTS: u64 = 200;
    const LINKS: u64 = 50;

    for (what, short, long, copies) in [
        (
            "the events' own names",
            wide_span_request(SHORT, SHORT, SHORT),
            wide_span_request(LONG, SHORT, SHORT),
            EVENTS,
        ),
        (
            "the events' own attribute values",
            wide_span_request(SHORT, SHORT, SHORT),
            wide_span_request(SHORT, LONG, SHORT),
            EVENTS,
        ),
        (
            "the links' own trace states",
            wide_span_request(SHORT, SHORT, SHORT),
            wide_span_request(SHORT, SHORT, LONG),
            LINKS,
        ),
    ] {
        let (ps, ls) = decode_both(&short);
        let (pl, ll) = decode_both(&long);
        assert_eq!(
            ls.total_rows(),
            ll.total_rows(),
            "{what}: the two sides must carry the same landed rows, or the \
             growth could be a row count rather than the field"
        );
        let charge_short = refused_charge(&root, ps, ls).await;
        let charge_long = refused_charge(&root, pl, ll).await;
        let added = (LONG - SHORT) as u64 * copies;
        assert!(
            charge_long >= charge_short + added,
            "{what}: {copies} elements grew by {} bytes each, so the charge \
             had to grow by at least {added}: {charge_short} -> {charge_long}",
            LONG - SHORT
        );
    }

    // And the whole block's rows are charged their slots beside that text.
    let (parsed, landed) = decode_both(&wide_span_request(SHORT, SHORT, SHORT));
    let rows = landed.total_rows();
    let charge = refused_charge(&root, parsed, landed).await;
    let slots = rows * std::mem::size_of::<TraceLandingRow>() as u64;
    assert!(
        charge >= slots + landing_block_overhead_bytes::<TraceLandingRow>(),
        "the charge ({charge}) must cover the {rows} landing rows the queue \
         holds ({slots} bytes of slots) and the block they are sealed into"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// **The charge covers the catalog rows the block holds**, not only the
/// spans: a push of mostly catalog rows would otherwise exceed
/// `PULSUS_INGEST_QUEUE_BYTES` while being charged a span's text.
///
/// The discriminator is the **row count**, not one row's text: both pushes
/// carry one span, and the second's 500 distinct attributes are 1,000 more
/// landed catalog rows. Each of them is charged a whole row slot, because
/// the row type is the union of four kinds' columns; a charge over kind 0
/// alone would grow only by the span's own attribute paths.
#[tokio::test]
async fn the_queue_charge_covers_the_catalog_rows_it_holds() {
    let root = spool_root("catalog-charge");
    let base = base_ns();
    let one = request_of(
        "checkout",
        vec![span_at(0xaa, 0x11, base, vec![kv("app.user", "u-1")])],
    );
    let many = request_of(
        "checkout",
        vec![span_at(
            0xaa,
            0x11,
            base,
            (0..500)
                .map(|i| kv(&format!("app.k{i:03}"), &format!("v{i:03}")))
                .collect(),
        )],
    );
    let (p1, l1) = decode_both(&one);
    let (p2, l2) = decode_both(&many);
    assert_eq!(l1.spans.len(), 1);
    assert_eq!(l2.spans.len(), 1, "both pushes carry exactly one span");
    let added_rows = l2.total_rows() - l1.total_rows();
    assert!(
        added_rows >= 998,
        "the second push has to carry about a thousand more catalog rows:          {added_rows}"
    );

    let first = refused_charge(&root, p1, l1).await;
    let second = refused_charge(&root, p2, l2).await;
    let slot = std::mem::size_of::<TraceLandingRow>() as u64;
    assert!(
        second >= first + added_rows * slot,
        "a charge that priced kind 0 only would grow by the span's own \
         attribute paths and not by {added_rows} row slots ({} bytes): \
         {first} -> {second}",
        added_rows * slot
    );

    std::fs::remove_dir_all(&root).ok();
}

/// The push's own charge, read off the refusal a one-byte-under ceiling
/// produces — the one figure the queue reserves, the ceiling refuses against
/// and the release gives back.
async fn refused_charge(root: &Path, parsed: ParsedTraces, landed: ParsedTraceLanding) -> u64 {
    let cfg = WriterConfig {
        batch_bytes: ByteSize(1),
        ingest_queue_bytes: ByteSize(1024 * 1024 * 1024),
        ..Default::default()
    };
    let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::unrecorded(Act::Ok);
    let (writer, _s, _a) = writer_ok_old(&cfg, root, landing);
    let err = writer
        .admit_flush(parsed, landed, PushHeaders::default())
        .expect_err("one byte of ceiling refuses every push");
    writer.shutdown(Duration::from_secs(5)).await;
    match err {
        AdmitRefusal::PushTooLarge { bytes, .. } => bytes,
        other => panic!("expected PushTooLarge, got {other:?}"),
    }
}

/// One span carrying 200 events and 50 links, the three fields the charge
/// case grows written to order.
///
/// **Every event shares one attribute key and one attribute value**, so the
/// push's catalog rows are one name and one value whatever `attr_bytes` is:
/// the only thing that grows with it is the 200 copies the span row's
/// `events` column holds.
fn wide_span_request(
    name_bytes: usize,
    attr_bytes: usize,
    state_bytes: usize,
) -> ExportTraceServiceRequest {
    use opentelemetry_proto::tonic::trace::v1::span::{Event, Link};
    let base = base_ns();
    let name = "n".repeat(name_bytes);
    let value = "v".repeat(attr_bytes);
    let state = "s".repeat(state_bytes);
    let mut span = span_at(0xaa, 0x11, base, vec![kv("http.route", "/api")]);
    span.events = (0..200)
        .map(|i| Event {
            time_unix_nano: u64::try_from(base + i).expect("post-epoch"),
            name: name.clone(),
            attributes: vec![kv("event.payload", &value)],
            dropped_attributes_count: 0,
        })
        .collect();
    span.links = (0..50)
        .map(|i| Link {
            trace_id: [0xcc; 16].to_vec(),
            span_id: [(i % 251) as u8 + 1; 8].to_vec(),
            trace_state: state.clone(),
            attributes: vec![kv("link.payload", "p")],
            dropped_attributes_count: 0,
            flags: 0,
        })
        .collect();
    request_of("checkout", vec![span])
}

/// **A valid push with no span is a success and is charged for nothing**: no
/// landing block, no landing insert, no old-path insert, and the landing
/// counter is zero throughout.
#[tokio::test]
async fn an_empty_push_is_a_success_and_is_charged_for_nothing() {
    let root = spool_root("empty");
    let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::always(Act::Ok);
    let (writer, spans_mock, attrs_mock) =
        writer_ok_old(&WriterConfig::default(), &root, landing.clone());

    let req = request_of("checkout", Vec::new());
    let (parsed, landed) = decode_both(&req);
    assert!(landed.is_empty(), "the fixture carries no landed row");
    writer
        .admit_flush(parsed, landed, PushHeaders::default())
        .expect("an empty push is admitted")
        .await
        .expect("and answered a success");

    assert_eq!(landing.call_count(), 0, "no block, no insert");
    assert_eq!(spans_mock.call_count(), 0);
    assert_eq!(attrs_mock.call_count(), 0);
    assert_eq!(writer.landing_metrics().queue_bytes, 0);
    assert_eq!(writer.metrics().queue_bytes, 0);

    writer.shutdown(Duration::from_secs(5)).await;
    std::fs::remove_dir_all(&root).ok();
}

// -- the two queues, and the coupled admission ------------------------

/// The landing queue refuses at **its own** configured ceiling: the counter
/// never exceeds `PULSUS_INGEST_QUEUE_BYTES` and the refusal is
/// backpressure.
///
/// It asserts nothing about the process total, which is three counters today
/// and four while both trace paths run.
#[tokio::test]
async fn the_landing_queue_refuses_at_its_own_ceiling() {
    let root = spool_root("landing-ceiling");
    let (writer, landing, _spans, limit) = saturated_landing(&root).await;
    assert!(
        writer.landing_metrics().queue_bytes <= limit,
        "the landing counter ({}) must never exceed the configured value \
         ({limit})",
        writer.landing_metrics().queue_bytes
    );
    assert!(
        landing.call_count() <= 1,
        "one worker, parked on the first block"
    );
    writer.shutdown(Duration::from_millis(50)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// **One-sided landing saturation refuses the push**, and nothing is stored
/// on either side. The `429` is not the case — a refusal that had already
/// buffered the old path's rows is the defect.
#[tokio::test]
async fn one_sided_landing_saturation_refuses_the_push() {
    let root = spool_root("one-sided-landing");
    let (writer, landing, spans_mock, _limit) = saturated_landing(&root).await;
    let admitted_landing_calls = landing.call_count();
    let old_calls_before = spans_mock.call_count();
    assert_eq!(
        writer.metrics().queue_bytes,
        0,
        "the old path's own counter is free: its blocks committed and \
         released"
    );

    let (parsed, landed) = decode_both(&six_spans_of(0xd1, "pay"));
    let err = writer
        .admit_flush(parsed, landed, PushHeaders::default())
        .expect_err("the saturated landing queue refuses the push");
    assert_eq!(err, AdmitRefusal::Backpressure);
    assert_eq!(
        landing.call_count(),
        admitted_landing_calls,
        "the refused push queued no landing block"
    );
    assert_eq!(
        spans_mock.call_count(),
        old_calls_before,
        "and stored nothing on the old path either"
    );

    writer.shutdown(Duration::from_millis(50)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// **One-sided old-path saturation refuses the push**, and both stores are
/// empty afterwards. Without the coupled admission the landing block would
/// be sent for a push the route refused.
#[tokio::test]
async fn one_sided_old_path_saturation_refuses_the_push() {
    let root = spool_root("one-sided-old");
    // A hanging OLD span inserter: its generation never settles, so its
    // bytes stay reserved and the old counter climbs to its ceiling. The
    // landing inserter succeeds, so the landing counter returns to zero.
    // `PULSUS_BATCH_BYTES` is the landing path's own per-push ceiling now, so
    // it is left at its default: lowering it to force the old path's buffers
    // to flush would refuse every push `413` instead. The old path's
    // generations settle on `batch_age` instead.
    let cfg = WriterConfig {
        ingest_queue_bytes: ByteSize(64 * 1024),
        ingest_dedup: false,
        ..Default::default()
    };
    let spans: Arc<MockInserter<TraceSpanRow>> = MockInserter::unrecorded(Act::Hang);
    let attrs: Arc<MockInserter<TraceAttrRow>> = MockInserter::unrecorded(Act::Ok);
    let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::unrecorded(Act::Ok);
    let writer = writer_with(
        &cfg,
        &root,
        spans.clone(),
        attrs.clone(),
        landing.clone() as Arc<dyn BlockInserter<TraceLandingRow>>,
    );

    let mut admitted = 0usize;
    let mut refused = None;
    for trace in 0..200u8 {
        let (parsed, landed) = decode_both(&six_spans_of(trace + 1, "checkout"));
        match writer.admit(parsed, landed, PushHeaders::default()) {
            Ok(()) => admitted += 1,
            Err(e) => {
                refused = Some(e);
                break;
            }
        }
        settle_until("the landing block to commit and release", || {
            writer.landing_metrics().queue_bytes == 0
        })
        .await;
    }
    assert_eq!(
        refused,
        Some(AdmitRefusal::Backpressure),
        "the old path's own saturation refuses the push"
    );
    assert!(admitted > 0, "some push had to be admitted first");
    assert_eq!(
        writer.landing_metrics().queue_bytes,
        0,
        "the landing counter is free when the old path refuses"
    );
    assert_eq!(
        landing.call_count(),
        admitted,
        "the refused push queued no landing block: the landing inserter saw \
         one call per ADMITTED push and no more"
    );

    writer.shutdown(Duration::from_millis(50)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// Drives the landing counter to its ceiling with a parked landing inserter
/// and a succeeding old path, and returns the refusal point.
async fn saturated_landing(
    root: &Path,
) -> (
    TraceWriter,
    Arc<MockInserter<TraceLandingRow>>,
    Arc<MockInserter<TraceSpanRow>>,
    u64,
) {
    let limit = 256 * 1024;
    // `PULSUS_BATCH_BYTES` stays at its default: it is the landing path's own
    // per-push byte ceiling, and lowering it would refuse every push `413`
    // rather than saturating anything.
    let cfg = WriterConfig {
        ingest_queue_bytes: ByteSize(limit),
        ingest_dedup: false,
        ..Default::default()
    };
    let spans: Arc<MockInserter<TraceSpanRow>> = MockInserter::unrecorded(Act::Ok);
    let attrs: Arc<MockInserter<TraceAttrRow>> = MockInserter::unrecorded(Act::Ok);
    let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::unrecorded(Act::Hang);
    let mut runtime = runtime_at(&cfg, root);
    runtime.trace_landing_inserters = 1;
    let writer = TraceWriter::with_inserters_and_runtime(
        spans.clone(),
        attrs.clone(),
        landing.clone() as Arc<dyn BlockInserter<TraceLandingRow>>,
        runtime,
        TraceWriterTables::traces_default(),
    );

    for trace in 0..200u8 {
        let (parsed, landed) = decode_both(&six_spans_of(trace + 1, "checkout"));
        if writer
            .admit(parsed, landed, PushHeaders::default())
            .is_err()
        {
            break;
        }
    }
    settle_until("the old path's reservations to be released", || {
        writer.metrics().queue_bytes == 0
    })
    .await;
    assert!(
        writer.landing_metrics().queue_bytes > 0,
        "the parked landing inserter has to leave the landing queue charged"
    );
    (writer, landing, spans, limit)
}

// -- the suppression index over three targets --------------------------

/// **The case the three-target claim is for.** The landing insert fails
/// non-retryably while both old-path inserts commit, so the claim's three
/// targets disagree: the two that count toward the acknowledgement committed
/// and the landing block did not. The retry is still suppressed, so the old
/// path does not store the body a second time.
#[tokio::test]
async fn a_retry_is_suppressed_when_only_the_landing_insert_failed() {
    let root = spool_root("mixed-outcome");
    let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::always(Act::Poison);
    let (writer, spans_mock, _attrs) =
        writer_ok_old(&WriterConfig::default(), &root, landing.clone());

    let (parsed, landed) = decode_both(&six_spans());
    writer
        .admit_flush(parsed.clone(), landed.clone(), PushHeaders::default())
        .expect("queue has room")
        .await
        .expect("the old path commits, which is what the route answers");
    settle_until("the landing block to settle", || {
        writer.metrics().spool_poison_total == 1
    })
    .await;

    let again = writer
        .admit_flush(parsed, landed, PushHeaders::default())
        .expect("the retry is suppressed, not refused");
    again
        .await
        .expect("and is answered the original push's outcome");

    assert_eq!(
        landing.call_count(),
        1,
        "the retry queued no second landing block"
    );
    assert_eq!(
        spans_mock.call_count(),
        1,
        "and the old path did not store the body a second time, which is the \
         defect a claim over the landing block alone would leave"
    );
    assert_eq!(
        writer.metrics().dedup.mixed_outcome_total,
        1,
        "one claim whose targets disagreed"
    );

    writer.shutdown(Duration::from_secs(5)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// **The hermetic half of the push-identity set**: two identical bodies
/// under two distinct `Idempotency-Key`s are two identities, so both store,
/// on both paths.
#[tokio::test]
async fn a_distinct_key_is_a_distinct_identity_in_the_writer() {
    let root = spool_root("distinct-key");
    let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::always(Act::Ok);
    let (writer, spans_mock, _attrs) =
        writer_ok_old(&WriterConfig::default(), &root, landing.clone());

    let (parsed, landed) = decode_both(&six_spans());
    for key in ["k-1", "k-2"] {
        writer
            .admit_flush(
                parsed.clone(),
                landed.clone(),
                PushHeaders {
                    idempotency_key: Some(key.to_string()),
                    declared_retry: false,
                },
            )
            .expect("both keys are admitted")
            .await
            .expect("both commit");
    }
    settle_until("both landing inserts", || landing.call_count() == 2).await;

    assert_eq!(
        landing.call_count(),
        2,
        "two keys are two identities: with `trace_identity` ignoring its \
         headers both pushes carry the content digest and the second is \
         suppressed"
    );
    assert_eq!(spans_mock.call_count(), 2, "on the old path too");

    writer.shutdown(Duration::from_secs(5)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// **The same bytes sent twice with no key are one push.** Every span in the
/// body carries no start time, so each decode stamps it with that request's
/// own receive clock and derives the attribute day of every row from it; the
/// two decodes here are the same bytes at two instants either side of a UTC
/// midnight. With no `Idempotency-Key` the identity IS the content digest, so
/// a receiver-generated value inside that digest makes the retry a second
/// identity and the body is stored twice — on both write paths.
#[tokio::test]
async fn a_body_with_no_span_start_times_is_stored_once_without_a_key() {
    let root = spool_root("no-start-times");
    let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::always(Act::Ok);
    let (writer, spans_mock, _attrs) =
        writer_ok_old(&WriterConfig::default(), &root, landing.clone());

    let body = six_spans_without_start_times();
    let (first, second) = clocks_across_midnight();

    let (parsed, landed) = decode_both_at(&body, first);
    writer
        .admit_flush(parsed, landed, PushHeaders::default())
        .expect("the first push is admitted")
        .await
        .expect("and commits");
    settle_until("the first landing insert", || landing.call_count() == 1).await;

    let (parsed, landed) = decode_both_at(&body, second);
    writer
        .admit_flush(parsed, landed, PushHeaders::default())
        .expect("the retry is suppressed, not admitted")
        .await
        .expect("and is answered the first push's outcome");

    assert_eq!(
        landing.call_count(),
        1,
        "the retry queued no second landing block: one body is one identity \
         whatever the receive clock read"
    );
    assert_eq!(
        spans_mock.call_count(),
        1,
        "and the old path did not store the body a second time"
    );
    assert_eq!(
        writer.metrics().dedup.duplicate_pushes_total,
        1,
        "one suppressed push"
    );

    writer.shutdown(Duration::from_secs(5)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// **The same bytes sent twice under one key are one push, not a changed
/// body.** The fixture and the two clocks are the case above's; what differs
/// is that both pushes carry the same `Idempotency-Key`, so the digest is the
/// content the claim entry recorded rather than the identity. A
/// receiver-generated value inside it makes the retry read as one key
/// carrying two contents — a `400` over a body the client sent once.
#[tokio::test]
async fn a_body_with_no_span_start_times_is_suppressed_under_one_key() {
    let root = spool_root("no-start-times-keyed");
    let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::always(Act::Ok);
    let (writer, spans_mock, _attrs) =
        writer_ok_old(&WriterConfig::default(), &root, landing.clone());

    let body = six_spans_without_start_times();
    let (first, second) = clocks_across_midnight();
    let keyed = || PushHeaders {
        idempotency_key: Some("k-1".to_string()),
        declared_retry: false,
    };

    let (parsed, landed) = decode_both_at(&body, first);
    writer
        .admit_flush(parsed, landed, keyed())
        .expect("the first push is admitted")
        .await
        .expect("and commits");
    settle_until("the first landing insert", || landing.call_count() == 1).await;

    let (parsed, landed) = decode_both_at(&body, second);
    let outcome = writer.admit_flush(parsed, landed, keyed());
    assert!(
        outcome.is_ok(),
        "the retry under one key is suppressed, not refused as changed \
         content: {:?}",
        outcome.err()
    );
    outcome
        .expect("asserted above")
        .await
        .expect("and is answered the first push's outcome");

    assert_eq!(
        writer.metrics().dedup.key_reused_total,
        0,
        "no push was refused as a key carrying different content"
    );
    assert_eq!(
        landing.call_count(),
        1,
        "the retry queued no second landing block"
    );
    assert_eq!(
        spans_mock.call_count(),
        1,
        "and the old path did not store the body a second time"
    );

    writer.shutdown(Duration::from_secs(5)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// **The other half of the rule: the start time the request DID send is
/// still content.** Two bodies alike but for one span's own instant, both
/// decoded at one receive clock, are two identities and both store — the
/// digest reads that instant out of the span's `payload`, which is the span
/// re-encoded verbatim.
///
/// **Both ends of the span move by the same nanosecond**, which is what
/// makes the `payload` term the only thing that can discriminate the pair:
/// shifting the start alone also shifts `duration_ns`, and the case then
/// passes with `payload` deleted from the digest — measured, that is what it
/// did.
#[tokio::test]
async fn two_bodies_differing_only_in_a_span_instant_both_store() {
    let root = spool_root("start-time-differs");
    let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::always(Act::Ok);
    let (writer, spans_mock, _attrs) =
        writer_ok_old(&WriterConfig::default(), &root, landing.clone());

    let mut later = six_spans();
    let span = &mut later.resource_spans[0].scope_spans[0].spans[0];
    span.start_time_unix_nano += 1;
    span.end_time_unix_nano += 1;

    for body in [six_spans(), later] {
        let (parsed, landed) = decode_both_at(&body, base_ns());
        writer
            .admit_flush(parsed, landed, PushHeaders::default())
            .expect("both bodies are admitted")
            .await
            .expect("and both commit");
    }
    settle_until("both landing inserts", || landing.call_count() == 2).await;

    assert_eq!(
        landing.call_count(),
        2,
        "a span moved by one nanosecond is a changed body"
    );
    assert_eq!(spans_mock.call_count(), 2, "on the old path too");

    writer.shutdown(Duration::from_secs(5)).await;
    std::fs::remove_dir_all(&root).ok();
}

// -- the two trace transports -----------------------------------------

/// Reads a rendered response into its status, its headers and its body.
async fn parts(response: axum::response::Response) -> (StatusCode, HeaderMap, Vec<u8>) {
    let status = response.status();
    let headers = response.headers().clone();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("a rendered body")
        .to_vec();
    (status, headers, body)
}

fn header_of(headers: &HeaderMap, name: header::HeaderName) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

/// A Zipkin v2 JSON span array over `days` distinct UTC dates, padded to 200
/// spans — the Zipkin-route twin of [`spans_over_days`].
fn zipkin_body_over_days(days: u64) -> Vec<u8> {
    let base_us = ((base_ns() / NS_PER_DAY) * NS_PER_DAY) / 1_000 + 3_600_000_000;
    let day_us = NS_PER_DAY / 1_000;
    let mut spans: Vec<String> = Vec::new();
    for d in 0..days {
        spans.push(format!(
            r#"{{"traceId":"bb000000000000000000000000000001","id":"{:016x}","name":"get","timestamp":{},"duration":1000,"localEndpoint":{{"serviceName":"checkout"}}}}"#,
            d + 1,
            base_us + (d as i64) * day_us
        ));
    }
    while spans.len() < 200 {
        let i = spans.len() as i64;
        spans.push(format!(
            r#"{{"traceId":"bc000000000000000000000000000001","id":"{:016x}","name":"get","timestamp":{},"duration":1000,"localEndpoint":{{"serviceName":"checkout"}}}}"#,
            1000 + i,
            base_us + i
        ));
    }
    format!("[{}]", spans.join(",")).into_bytes()
}

/// **A push whose spans fall on more than a hundred distinct UTC dates is
/// refused `413`, on each trace transport** — and the gate's place is the
/// other half of the case: a gate sitting after the landing path's admission
/// refuses the push and leaves the old store empty, so it would pass the
/// status and the old-store assertions and fail these two.
#[tokio::test]
async fn a_push_spanning_more_than_a_hundred_dates_is_refused() {
    let root = spool_root("day-gate");
    let cfg = WriterConfig {
        ingest_dedup: false,
        ..Default::default()
    };

    for transport in ["otlp", "zipkin"] {
        // The admitted side: exactly the limit's worth of dates lands.
        let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::unrecorded(Act::Ok);
        let (writer, spans_mock, _attrs) = writer_ok_old(&cfg, &root, landing.clone());
        let ok = post(transport, &writer, TRACE_LANDING_DAY_LIMIT).await;
        let (status, _h, _b) = parts(ok).await;
        assert!(
            status.is_success(),
            "{transport}: {TRACE_LANDING_DAY_LIMIT} dates is admitted, got {status}"
        );
        settle_until("the admitted push's landing insert", || {
            landing.call_count() == 1
        })
        .await;
        assert_eq!(spans_mock.call_count(), 1, "{transport}: and stored");
        writer.shutdown(Duration::from_secs(5)).await;

        // One date past it is refused whole.
        let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::unrecorded(Act::Ok);
        let (writer, spans_mock, attrs_mock) = writer_ok_old(&cfg, &root, landing.clone());
        let refused = post(transport, &writer, TRACE_LANDING_DAY_LIMIT + 1).await;
        let (status, headers, body) = parts(refused).await;
        let message =
            push_spans_too_many_days_message(TRACE_LANDING_DAY_LIMIT + 1, TRACE_LANDING_DAY_LIMIT);
        assert_eq!(
            status,
            StatusCode::PAYLOAD_TOO_LARGE,
            "{transport}: one date past the limit is refused"
        );
        assert_eq!(
            header_of(&headers, header::X_CONTENT_TYPE_OPTIONS),
            None,
            "{transport}: an `AdmitRefusal` is raised at the sink, so it never \
             takes the pre-admission writer"
        );
        match transport {
            "otlp" => {
                assert_eq!(
                    header_of(&headers, header::CONTENT_TYPE).as_deref(),
                    Some("application/x-protobuf")
                );
                let status_msg = decode_status(&body);
                assert_eq!(status_msg.0, 8);
                assert_eq!(status_msg.1, message);
            }
            _ => {
                assert_eq!(
                    header_of(&headers, header::CONTENT_TYPE).as_deref(),
                    Some("text/plain; charset=utf-8")
                );
                assert_eq!(String::from_utf8_lossy(&body), message);
                assert_ne!(
                    body.last(),
                    Some(&b'\n'),
                    "the post-admission container carries no terminator"
                );
            }
        }
        assert_eq!(
            landing.call_count(),
            0,
            "{transport}: the recording inserter saw no call naming the \
             landing table"
        );
        assert_eq!(
            writer.landing_metrics().queue_bytes,
            0,
            "{transport}: and the landing path's own byte counter never rose \
             above 0, so no block was sealed and charged"
        );
        assert_eq!(
            spans_mock.call_count(),
            0,
            "{transport}: nothing reached the old path either"
        );
        assert_eq!(attrs_mock.call_count(), 0);
        writer.shutdown(Duration::from_secs(5)).await;
    }

    std::fs::remove_dir_all(&root).ok();
}

/// Posts a `days`-date push through `transport`'s own handler.
async fn post(transport: &str, writer: &TraceWriter, days: u64) -> axum::response::Response {
    match transport {
        "otlp" => {
            let body = spans_over_days(days).encode_to_vec();
            let mut headers = HeaderMap::new();
            headers.insert(
                header::CONTENT_TYPE,
                "application/x-protobuf".parse().unwrap(),
            );
            pulsus_write::ingest_traces(writer, headers, Body::from(body)).await
        }
        _ => {
            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
            pulsus_write::ingest_zipkin(writer, headers, Body::from(zipkin_body_over_days(days)))
                .await
        }
    }
}

/// `(code, message)` out of a `google.rpc.Status` body.
fn decode_status(body: &[u8]) -> (i32, String) {
    #[derive(Clone, PartialEq, ::prost::Message)]
    struct Status {
        #[prost(int32, tag = "1")]
        code: i32,
        #[prost(string, tag = "2")]
        message: String,
    }
    let decoded = Status::decode(body).expect("a google.rpc.Status body");
    (decoded.code, decoded.message)
}

/// **A span whose stored JSON paths exceed `format_binary_max_object_size`
/// is refused `400` at decode**, through this route's **decode** writer, not
/// its refusal mapper, because the refusal is at decode.
///
/// **`/v1/traces` only, and the two shipped bounds that make the Zipkin
/// transport unreachable are why.** The gate counts the stored paths of one
/// attribute list, and 100,001 of them cannot be built out of a Zipkin v2
/// span: `zipkin::decode` refuses above 65,536 tags per span, and a Zipkin
/// tag's value is a string, so there is no nested value to raise the path
/// count above the tag count. On `/v1/traces` the paths come from four
/// `kvlist` attributes instead, which keeps the OTLP pre-scan's own
/// 65,536-attributes-per-element bound out of the way — 100,000 flat
/// attributes are refused by that bound before this gate is reached.
#[tokio::test]
async fn a_span_with_too_many_paths_is_refused_at_decode_over_http() {
    let root = spool_root("path-gate");
    // 100,000 stored paths are 100,004 landed catalog rows, each charged a
    // whole row slot: at the default byte ceiling the ACCEPTED half is
    // refused `413` and never reaches the assertion it exists for.
    let cfg = WriterConfig {
        batch_bytes: ByteSize(1024 * 1024 * 1024),
        ingest_queue_bytes: ByteSize(1024 * 1024 * 1024),
        ingest_dedup: false,
        ..Default::default()
    };
    let limit = MAX_JSON_PATHS_PER_VALUE;
    let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::unrecorded(Act::Ok);
    let (writer, spans_mock, _attrs) = writer_ok_old(&cfg, &root, landing.clone());

    let ok = post_paths(&writer, limit).await;
    let (status, _h, _b) = parts(ok).await;
    assert!(
        status.is_success(),
        "a span at the path limit is admitted, got {status}"
    );
    settle_until("the admitted push's landing insert", || {
        landing.call_count() == 1
    })
    .await;

    let refused = post_paths(&writer, limit + 1).await;
    let (status, headers, body) = parts(refused).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "one path past the limit is a decode refusal"
    );
    assert_eq!(
        header_of(&headers, header::CONTENT_TYPE).as_deref(),
        Some("application/x-protobuf"),
        "it leaves through this route's whole-request error writer"
    );
    assert_eq!(header_of(&headers, header::X_CONTENT_TYPE_OPTIONS), None);
    assert_eq!(
        decode_status(&body).0,
        3,
        "`classify` already maps the oversize-message variant to code 3"
    );
    assert_eq!(
        landing.call_count(),
        1,
        "the refused push never reached admission"
    );
    assert_eq!(spans_mock.call_count(), 1, "on either path");
    writer.shutdown(Duration::from_secs(10)).await;

    std::fs::remove_dir_all(&root).ok();
}

/// One span whose attributes are four `kvlist`s holding `paths` leaves
/// between them, all under one value so the push's distinct-value count
/// stays at nothing — a kvlist contributes no kind-3 row.
fn paths_request(paths: u64) -> ExportTraceServiceRequest {
    use opentelemetry_proto::tonic::common::v1::KeyValueList;
    const GROUPS: u64 = 4;
    let per = paths / GROUPS;
    let remainder = paths % GROUPS;
    let attrs: Vec<KeyValue> = (0..GROUPS)
        .map(|g| {
            let count = per + u64::from(g == 0) * remainder;
            KeyValue {
                key: format!("g{g}"),
                value: Some(AnyValue {
                    value: Some(Value::KvlistValue(KeyValueList {
                        values: (0..count).map(|i| kv(&format!("k{i:06}"), "v")).collect(),
                    })),
                }),
                key_strindex: 0,
            }
        })
        .collect();
    request_of("checkout", vec![span_at(0xaa, 0x11, base_ns(), attrs)])
}

async fn post_paths(writer: &TraceWriter, paths: u64) -> axum::response::Response {
    let body = paths_request(paths).encode_to_vec();
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        "application/x-protobuf".parse().unwrap(),
    );
    pulsus_write::ingest_traces(writer, headers, Body::from(body)).await
}

/// **A push above the per-push ceilings is `413` on both trace transports,
/// in each route's own container** — and `trace_spans` is empty, because the
/// refusal is at the joint admission and not after the old path stored.
#[tokio::test]
async fn a_push_too_large_is_413_on_the_trace_transports() {
    let root = spool_root("too-large");
    let req = six_spans();
    let (_parsed, landed) = decode_both(&req);
    let rows = landed.total_rows();
    let cfg = WriterConfig {
        trace_landing_max_rows: rows,
        ingest_dedup: false,
        ..Default::default()
    };

    for transport in ["otlp", "zipkin"] {
        let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::unrecorded(Act::Ok);
        let (writer, spans_mock, attrs_mock) = writer_ok_old(&cfg, &root, landing.clone());
        let response = match transport {
            "otlp" => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    header::CONTENT_TYPE,
                    "application/x-protobuf".parse().unwrap(),
                );
                pulsus_write::ingest_traces(&writer, headers, Body::from(req.encode_to_vec())).await
            }
            _ => {
                let mut headers = HeaderMap::new();
                headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
                pulsus_write::ingest_zipkin(&writer, headers, Body::from(zipkin_six_spans())).await
            }
        };
        let (status, headers, body) = parts(response).await;
        assert_eq!(
            status,
            StatusCode::PAYLOAD_TOO_LARGE,
            "{transport}: a push that does not fit one block is refused whole"
        );
        assert_eq!(header_of(&headers, header::X_CONTENT_TYPE_OPTIONS), None);
        match transport {
            "otlp" => {
                assert_eq!(
                    header_of(&headers, header::CONTENT_TYPE).as_deref(),
                    Some("application/x-protobuf")
                );
                let (code, message) = decode_status(&body);
                assert_eq!(code, 8);
                assert!(
                    message.starts_with("push does not fit one block:"),
                    "{message}"
                );
            }
            _ => {
                assert_eq!(
                    header_of(&headers, header::CONTENT_TYPE).as_deref(),
                    Some("text/plain; charset=utf-8")
                );
                let text = String::from_utf8_lossy(&body).to_string();
                assert!(
                    text.starts_with("push does not fit one block:"),
                    "the Zipkin route renders the same message as its whole \
                     plain-text body: {text}"
                );
                assert_ne!(body.last(), Some(&b'\n'));
            }
        }
        assert_eq!(
            spans_mock.call_count(),
            0,
            "{transport}: the old path's own table is empty — the refusal is \
             at the joint admission, not after the old path stored"
        );
        assert_eq!(attrs_mock.call_count(), 0);
        assert_eq!(landing.call_count(), 0);
        writer.shutdown(Duration::from_secs(5)).await;
    }

    std::fs::remove_dir_all(&root).ok();
}

/// Six Zipkin spans of one trace, the Zipkin twin of [`six_spans`].
fn zipkin_six_spans() -> Vec<u8> {
    let base_us = base_ns() / 1_000;
    let spans: Vec<String> = (0..6)
        .map(|i| {
            format!(
                r#"{{"traceId":"aa000000000000000000000000000001","id":"{:016x}","name":"get","timestamp":{},"duration":1000,"localEndpoint":{{"serviceName":"checkout"}},"tags":{{"http.route":"/api","http.method":"GET"}}}}"#,
                0x10 + i,
                base_us + i
            )
        })
        .collect();
    format!("[{}]", spans.join(",")).into_bytes()
}

// -- the one live observable ------------------------------------------

/// **No landing insert reaches the asynchronous queue.** It is the one
/// observable that tells a synchronous insert from a queued one, and a
/// queued insert can carry another push's rows in the same committed block
/// with the deduplication token disregarded.
///
/// This row is a drift detector for the asynchronous path being taken at
/// all: with `client.rs`'s already-shipped pin removed the query setting's
/// own default is `true` and this case is red, while removing the landing
/// table's `CREATE` value leaves it green — what that value defends against
/// is a server-wide `<merge_tree>` default, which no case can set without
/// editing the shared server's configuration.
#[tokio::test]
async fn no_landing_insert_reaches_the_asynchronous_queue() {
    if !pulsus_testkit::live_clickhouse_enabled() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test");
        return;
    }
    use futures::StreamExt;
    use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, Row};
    use pulsus_schema::{RenderCtx, SchemaParams, run_init};

    #[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
    struct CountRow {
        n: u64,
    }

    let base = ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: "default".to_string(),
        proto: ChProto::Http,
        pool_size: 2,
        query_timeout: Duration::from_secs(30),
        ..ChConnConfig::default()
    };
    let db = pulsus_testkit::test_db("pulsus_trace_landing_async");
    let admin = ChClient::new(base.clone()).await.expect("connect");
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop the test database");
    let ctx: SchemaParams = RenderCtx::for_tests(&db);
    run_init(&admin, &ctx).await.expect("run_init");
    let client = Arc::new(
        ChClient::new(ChConnConfig {
            database: db.clone(),
            ..base.clone()
        })
        .await
        .expect("connect to the test database"),
    );

    // **The window this case reads is bounded at both ends.**
    // `system.asynchronous_insert_log` is server-wide and append-only, and
    // this suite's database name repeats across runs, so a row an earlier
    // run of this same case left behind is still there — measured: a run
    // with the client's `async_insert` pin removed left one, and the next
    // run read it and failed. The read starts at this instant.
    //
    // Each read below is in a block of its own, so its stream — and the
    // pooled connection it holds — is dropped before the next statement
    // runs; two live streams exhaust this client's two-connection pool and
    // the `DROP DATABASE` at the end times out.
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
    struct TextRow {
        s: String,
    }
    let started = {
        let mut stream = admin
            .query_stream::<TextRow>("SELECT toString(now64(6)) AS s", &QuerySettings::new())
            .await
            .expect("read the server's own clock");
        stream.next().await.expect("one row").expect("decode").s
    };

    let root = spool_root("async-queue");
    let mut runtime = runtime_at(&WriterConfig::default(), &root);
    runtime.trace_landing_inserters = 1;
    let writer = TraceWriter::new_with_tables(
        client.clone(),
        &WriterConfig::default(),
        TraceWriterTables::traces_default(),
    );
    let (parsed, landed) = decode_both(&six_spans());
    writer
        .admit_flush(parsed, landed, PushHeaders::default())
        .expect("queue has room")
        .await
        .expect("the old path commits");
    settle_until("the landing insert to commit", || {
        writer.landing_metrics().landing.flushes_total == 1
    })
    .await;
    writer.shutdown(Duration::from_secs(10)).await;

    admin
        .execute(
            "SYSTEM FLUSH LOGS",
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("flush the system logs");
    let sql = format!(
        "SELECT count() AS n FROM system.asynchronous_insert_log \
         WHERE database = '{db}' AND table = 'trace_landing' \
           AND event_time_microseconds >= toDateTime64('{started}', 6)"
    );
    let queued = {
        let mut stream = admin
            .query_stream::<CountRow>(&sql, &QuerySettings::new())
            .await
            .expect("read the asynchronous insert log");
        stream.next().await.expect("one row").expect("decode").n
    };
    assert_eq!(
        queued, 0,
        "a landing insert must never be queued: a queued block can carry two \
         pushes' rows with the deduplication token disregarded"
    );

    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop the test database");
    std::fs::remove_dir_all(&root).ok();
}
