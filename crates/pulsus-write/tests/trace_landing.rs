//! `TraceWriter`: **one push is one insert of one block into
//! `trace_landing`** (issues #586 and #602), the three per-push ceilings, the
//! admission over the landing queue, the push-suppression index, the sync
//! acknowledgement, and the two refusal containers the trace transports
//! render. All against mock `BlockInserter`s — no ClickHouse server,
//! except the one case whose whole observable is a `system` table and which
//! gates itself on `PULSUS_TEST_CLICKHOUSE=1`.
//!
//! **Nothing here reads a target table.** The five derived trace tables are
//! maintained by materialized view; what this file pins is the block the
//! writer hands over.

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
    BlockInserter, TRACE_LANDING_DAY_LIMIT, TraceLandingRow, TraceWriter, TraceWriterTables,
    WriterRuntime, landing_block_overhead_bytes,
};
use pulsus_write::{
    AdmitRefusal, ParsedTraceLanding, PushHeaders, TraceSink, push_spans_too_many_days_message,
};

const LANDING: &str = "trace_landing";

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
    /// A failure after the block was sent: its fate is unknown.
    Uncertain,
}

/// A scriptable mock [`BlockInserter`] for the landing table. Records every
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
                Act::Uncertain => Err(ChError::InsertUncertain("mock uncertain".to_string())),
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

/// A writer whose landing inserter is the mock given.
fn writer_with(
    cfg: &WriterConfig,
    spool: &Path,
    landing: Arc<dyn BlockInserter<TraceLandingRow>>,
) -> TraceWriter {
    TraceWriter::with_inserters_and_runtime(
        landing,
        runtime_at(cfg, spool),
        TraceWriterTables::traces_default(),
    )
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

/// The landing decode of one request, exactly as a handler runs it.
fn decode(req: &ExportTraceServiceRequest) -> ParsedTraceLanding {
    pulsus_write::parse_trace_landing(req).expect("the landing decode")
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
/// `end_time_unix_nano`**: each span is stored with a start of `0`
/// (issue #587 row 10), never the receive clock.
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
/// kind the push produced — and **none** naming any of the target tables.
#[tokio::test]
async fn one_push_is_one_insert_into_the_landing_table() {
    let root = spool_root("one-insert");
    let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::always(Act::Ok);
    let writer = writer_with(&WriterConfig::default(), &root, landing.clone());

    let req = six_spans();
    let landed = decode(&req);
    let expected_rows = landed.total_rows() as usize;
    writer
        .admit_flush(landed, PushHeaders::default())
        .expect("queue has room")
        .await
        .expect("the push commits");
    settle_until("the landing insert", || landing.call_count() == 1).await;

    assert_eq!(landing.call_count(), 1, "one push is one landing insert");
    assert_eq!(landing.tables(), vec![LANDING.to_string()]);
    for table in ["spans", "resources", "traces", "tag_names", "tag_values"] {
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

    writer.shutdown(Duration::from_secs(5)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// Two pushes are two inserts carrying two distinct tokens, neither block
/// holding the other's rows.
#[tokio::test]
async fn two_pushes_are_two_inserts() {
    let root = spool_root("two-inserts");
    let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::always(Act::Ok);
    let writer = writer_with(&WriterConfig::default(), &root, landing.clone());

    for (trace, service) in [(0xa1u8, "checkout"), (0xa2, "cart")] {
        let req = six_spans_of(trace, service);
        let landed = decode(&req);
        writer
            .admit_flush(landed, PushHeaders::default())
            .expect("queue has room")
            .await
            .expect("the push commits");
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
    let cfg = WriterConfig::default();
    let writer = writer_with(
        &cfg,
        &root,
        landing.clone() as Arc<dyn BlockInserter<TraceLandingRow>>,
    );

    let req = six_spans();
    let landed = decode(&req);
    writer
        .admit_flush(landed, PushHeaders::default())
        .expect("queue has room")
        .await
        .expect("the resend commits");
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
    let writer2 = writer_with(&cfg, &root, landing2.clone());
    for _ in 0..2 {
        let landed = decode(&six_spans());
        writer2
            .admit_flush(landed, PushHeaders::default())
            .expect("queue has room")
            .await
            .expect("the push commits");
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
    let landed = decode(&req);
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
    let writer = writer_with(&cfg, &root, landing.clone());
    let before = writer.metrics().dedup.rollbacks_total;
    let err = writer
        .admit_flush(landed.clone(), PushHeaders::default())
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
    assert_eq!(writer.metrics().queue_bytes, 0, "no bytes stay reserved");
    assert_eq!(writer.metrics().queue_bytes, 0);
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
        let writer = writer_with(&cfg, &root, landing.clone());
        let result = writer.admit_flush(landed.clone(), PushHeaders::default());
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
            assert_eq!(writer.metrics().queue_bytes, 0);
        } else {
            result
                .expect("byte equality is admitted, not refused")
                .await
                .expect("the push commits");
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
        let ls = decode(&short);
        let ll = decode(&long);
        assert_eq!(
            ls.total_rows(),
            ll.total_rows(),
            "{what}: the two sides must carry the same landed rows, or the \
             growth could be a row count rather than the field"
        );
        let charge_short = refused_charge(&root, ls).await;
        let charge_long = refused_charge(&root, ll).await;
        let added = (LONG - SHORT) as u64 * copies;
        assert!(
            charge_long >= charge_short + added,
            "{what}: {copies} elements grew by {} bytes each, so the charge \
             had to grow by at least {added}: {charge_short} -> {charge_long}",
            LONG - SHORT
        );
    }

    // And the whole block's rows are charged their slots beside that text.
    let landed = decode(&wide_span_request(SHORT, SHORT, SHORT));
    let rows = landed.total_rows();
    let charge = refused_charge(&root, landed).await;
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
    let l1 = decode(&one);
    let l2 = decode(&many);
    assert_eq!(l1.spans.len(), 1);
    assert_eq!(l2.spans.len(), 1, "both pushes carry exactly one span");
    let added_rows = l2.total_rows() - l1.total_rows();
    assert!(
        added_rows >= 998,
        "the second push has to carry about a thousand more catalog rows:          {added_rows}"
    );

    let first = refused_charge(&root, l1).await;
    let second = refused_charge(&root, l2).await;
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
async fn refused_charge(root: &Path, landed: ParsedTraceLanding) -> u64 {
    let cfg = WriterConfig {
        batch_bytes: ByteSize(1),
        ingest_queue_bytes: ByteSize(1024 * 1024 * 1024),
        ..Default::default()
    };
    let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::unrecorded(Act::Ok);
    let writer = writer_with(&cfg, root, landing);
    let err = writer
        .admit_flush(landed, PushHeaders::default())
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
/// landing block, no landing insert, and the landing counter is zero
/// throughout.
#[tokio::test]
async fn an_empty_push_is_a_success_and_is_charged_for_nothing() {
    let root = spool_root("empty");
    let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::always(Act::Ok);
    let writer = writer_with(&WriterConfig::default(), &root, landing.clone());

    let req = request_of("checkout", Vec::new());
    let landed = decode(&req);
    assert!(landed.is_empty(), "the fixture carries no landed row");
    writer
        .admit_flush(landed, PushHeaders::default())
        .expect("an empty push is admitted")
        .await
        .expect("and answered a success");

    assert_eq!(landing.call_count(), 0, "no block, no insert");
    assert_eq!(writer.metrics().queue_bytes, 0);
    assert_eq!(writer.metrics().queue_bytes, 0);

    writer.shutdown(Duration::from_secs(5)).await;
    std::fs::remove_dir_all(&root).ok();
}

// -- the landing queue -----------------------------------------------

/// The landing queue refuses at **its own** configured ceiling: the counter
/// never exceeds `PULSUS_INGEST_QUEUE_BYTES` and the refusal is
/// backpressure.
///
/// It asserts nothing about the process total, which is three counters today
/// and four while both trace paths run.
#[tokio::test]
async fn the_landing_queue_refuses_at_its_own_ceiling() {
    let root = spool_root("landing-ceiling");
    let (writer, landing, limit) = saturated_landing(&root).await;
    assert!(
        writer.metrics().queue_bytes <= limit,
        "the landing counter ({}) must never exceed the configured value \
         ({limit})",
        writer.metrics().queue_bytes
    );
    assert!(
        landing.call_count() <= 1,
        "one worker, parked on the first block"
    );
    writer.shutdown(Duration::from_millis(50)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// **Landing saturation refuses the push**, and the refused push queues
/// no block.
#[tokio::test]
async fn landing_saturation_refuses_the_push() {
    let root = spool_root("one-sided-landing");
    let (writer, landing, _limit) = saturated_landing(&root).await;
    let admitted_landing_calls = landing.call_count();

    let landed = decode(&six_spans_of(0xd1, "pay"));
    let err = writer
        .admit_flush(landed, PushHeaders::default())
        .expect_err("the saturated landing queue refuses the push");
    assert_eq!(err, AdmitRefusal::Backpressure);
    assert_eq!(
        landing.call_count(),
        admitted_landing_calls,
        "the refused push queued no landing block"
    );

    writer.shutdown(Duration::from_millis(50)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// Drives the landing counter to its ceiling with a parked landing inserter
/// and returns the refusal point.
async fn saturated_landing(root: &Path) -> (TraceWriter, Arc<MockInserter<TraceLandingRow>>, u64) {
    let limit = 256 * 1024;
    // `PULSUS_BATCH_BYTES` stays at its default: it is the per-push byte
    // ceiling, and lowering it would refuse every push `413` rather than
    // saturating anything.
    let cfg = WriterConfig {
        ingest_queue_bytes: ByteSize(limit),
        ingest_dedup: false,
        ..Default::default()
    };
    let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::unrecorded(Act::Hang);
    let mut runtime = runtime_at(&cfg, root);
    runtime.trace_landing_inserters = 1;
    let writer = TraceWriter::with_inserters_and_runtime(
        landing.clone() as Arc<dyn BlockInserter<TraceLandingRow>>,
        runtime,
        TraceWriterTables::traces_default(),
    );

    for trace in 0..200u8 {
        let landed = decode(&six_spans_of(trace + 1, "checkout"));
        if writer.admit(landed, PushHeaders::default()).is_err() {
            break;
        }
    }
    assert!(
        writer.metrics().queue_bytes > 0,
        "the parked landing inserter has to leave the landing queue charged"
    );
    (writer, landing, limit)
}

// -- the acknowledgement and the suppression index ----------------------

/// **A sync push is answered by its landing insert** (issue #602): a
/// landing insert that fails is the push's error.
#[tokio::test]
async fn a_sync_push_is_answered_by_its_landing_insert() {
    let root = spool_root("answered-by-landing");
    let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::always(Act::Poison);
    let writer = writer_with(&WriterConfig::default(), &root, landing.clone());
    let answer = writer
        .admit_flush(decode(&six_spans()), PushHeaders::default())
        .expect("queue has room")
        .await;
    assert!(
        answer.is_err(),
        "the failed landing insert is the answer: {answer:?}"
    );
    writer.shutdown(Duration::from_secs(5)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// **The answer waits for the landing insert**: while it is in flight the
/// sync push is not answered.
#[tokio::test]
async fn a_sync_push_is_not_answered_while_its_landing_insert_is_in_flight() {
    let root = spool_root("waits-for-landing");
    let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::unrecorded(Act::Hang);
    let writer = writer_with(&WriterConfig::default(), &root, landing.clone());
    let wait = writer
        .admit_flush(decode(&six_spans()), PushHeaders::default())
        .expect("queue has room");
    assert!(
        tokio::time::timeout(Duration::from_millis(300), wait)
            .await
            .is_err(),
        "the push was answered before its landing insert ended"
    );
    writer.shutdown(Duration::from_millis(50)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// **A retry of a push whose landing insert failed is stored**: the claim's
/// one target is the landing block, so its failure releases the claim.
#[tokio::test]
async fn a_retry_of_a_failed_landing_insert_is_stored() {
    let root = spool_root("retry-after-failure");
    let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::always(Act::Poison);
    let writer = writer_with(&WriterConfig::default(), &root, landing.clone());
    let landed = decode(&six_spans());
    let _ = writer
        .admit_flush(landed.clone(), PushHeaders::default())
        .expect("queue has room")
        .await;
    settle_until("the first landing block to settle", || {
        writer.metrics().spool_poison_total == 1
    })
    .await;
    let _ = writer
        .admit_flush(landed, PushHeaders::default())
        .expect("the retry is admitted")
        .await;
    assert_eq!(
        landing.call_count(),
        2,
        "the retry queued a second landing block rather than being suppressed"
    );
    assert_eq!(writer.metrics().dedup.duplicate_pushes_total, 0);
    writer.shutdown(Duration::from_secs(5)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// **A retry after an insert whose fate is unknown is stored** (issue
/// #602): the trace block is replay-safe, so an uncertain ending releases
/// the claim and the client's retry queues a block of its own. With no
/// resends configured each push makes exactly one attempt.
#[tokio::test]
async fn a_retry_after_an_uncertain_landing_insert_is_stored() {
    let root = spool_root("retry-after-uncertain");
    let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::always(Act::Uncertain);
    let cfg = WriterConfig {
        trace_landing_retries: 0,
        ..Default::default()
    };
    let writer = writer_with(&cfg, &root, landing.clone());
    let landed = decode(&six_spans());
    let first = writer
        .admit_flush(landed.clone(), PushHeaders::default())
        .expect("queue has room")
        .await;
    assert!(first.is_err(), "the uncertain insert is the push's error");
    let second = writer
        .admit_flush(landed, PushHeaders::default())
        .expect("the retry is admitted")
        .await;
    assert!(second.is_err());
    assert_eq!(
        landing.call_count(),
        2,
        "the retry queued a block of its own rather than being answered the first error"
    );
    assert_eq!(writer.metrics().dedup.duplicate_pushes_total, 0);
    writer.shutdown(Duration::from_secs(5)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// An inserter whose one call is held until the case releases it, and then
/// fails with a non-retryable error: provably not committed.
struct GatedPoisonInserter {
    entered: tokio::sync::Semaphore,
    gate: tokio::sync::Semaphore,
    calls: AtomicUsize,
}

impl BlockInserter<TraceLandingRow> for GatedPoisonInserter {
    fn insert<'a>(
        &'a self,
        _table: &'a str,
        _rows: &'a [TraceLandingRow],
    ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.add_permits(1);
        Box::pin(async move {
            self.gate
                .acquire()
                .await
                .expect("the gate is never closed")
                .forget();
            Err(ChError::Decode("gated poison".to_string()))
        })
    }
}

/// **A sync repeat waiting on a pending trace claim is answered the failure
/// the claim settles to** (issue #602; the retry contract's row 5, at the
/// writer). The original's landing insert is held in flight, the identical
/// push arrives and waits, then the insert fails: both callers are answered
/// the failure, and the repeat stores nothing.
#[tokio::test]
async fn a_sync_repeat_waiting_on_a_pending_trace_claim_is_answered_its_failure() {
    let root = spool_root("repeat-on-pending-failure");
    let landing = Arc::new(GatedPoisonInserter {
        entered: tokio::sync::Semaphore::new(0),
        gate: tokio::sync::Semaphore::new(0),
        calls: AtomicUsize::new(0),
    });
    let writer = writer_with(&WriterConfig::default(), &root, landing.clone());
    let landed = decode(&six_spans());

    let original = writer
        .admit_flush(landed.clone(), PushHeaders::default())
        .expect("queue has room");
    // The original's insert is in flight, so its claim is pending.
    landing
        .entered
        .acquire()
        .await
        .expect("the entered semaphore is never closed")
        .forget();
    let repeat = writer
        .admit_flush(landed, PushHeaders::default())
        .expect("the repeat waits on the pending claim");
    assert_eq!(writer.metrics().dedup.duplicate_pushes_total, 1);

    landing.gate.add_permits(1);
    let original = original.await;
    let repeat = tokio::time::timeout(Duration::from_secs(10), repeat)
        .await
        .expect("the repeat is answered once the claim settles");
    assert!(
        original.is_err(),
        "the failed insert is the original's answer"
    );
    assert!(
        repeat.is_err(),
        "the repeat is answered the failure its claim settled to: {repeat:?}"
    );
    assert_eq!(
        landing.calls.load(Ordering::SeqCst),
        1,
        "the repeat stored nothing"
    );
    writer.shutdown(Duration::from_secs(5)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// **The hermetic half of the push-identity set**: two identical bodies
/// under two distinct `Idempotency-Key`s are two identities, so both store.
#[tokio::test]
async fn a_distinct_key_is_a_distinct_identity_in_the_writer() {
    let root = spool_root("distinct-key");
    let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::always(Act::Ok);
    let writer = writer_with(&WriterConfig::default(), &root, landing.clone());

    let landed = decode(&six_spans());
    for key in ["k-1", "k-2"] {
        writer
            .admit_flush(
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

    writer.shutdown(Duration::from_secs(5)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// **The same bytes sent twice with no key are one push.** Every span in the
/// body carries no start time. With no `Idempotency-Key` the identity IS the
/// content digest, so a receiver-generated value inside that digest would
/// make the retry a second identity and the body would be stored twice.
#[tokio::test]
async fn a_body_with_no_span_start_times_is_stored_once_without_a_key() {
    let root = spool_root("no-start-times");
    let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::always(Act::Ok);
    let writer = writer_with(&WriterConfig::default(), &root, landing.clone());

    let body = six_spans_without_start_times();

    let landed = decode(&body);
    writer
        .admit_flush(landed, PushHeaders::default())
        .expect("the first push is admitted")
        .await
        .expect("and commits");
    settle_until("the first landing insert", || landing.call_count() == 1).await;

    let landed = decode(&body);
    writer
        .admit_flush(landed, PushHeaders::default())
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
        writer.metrics().dedup.duplicate_pushes_total,
        1,
        "one suppressed push"
    );

    writer.shutdown(Duration::from_secs(5)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// **The same bytes sent twice under one key are one push, not a changed
/// body.** The fixture is the case above's; what differs is that both pushes
/// carry the same `Idempotency-Key`, so the digest is the content the claim
/// entry recorded rather than the identity. A receiver-generated value
/// inside it would make the retry read as one key carrying two contents — a
/// `400` over a body the client sent once.
#[tokio::test]
async fn a_body_with_no_span_start_times_is_suppressed_under_one_key() {
    let root = spool_root("no-start-times-keyed");
    let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::always(Act::Ok);
    let writer = writer_with(&WriterConfig::default(), &root, landing.clone());

    let body = six_spans_without_start_times();
    let keyed = || PushHeaders {
        idempotency_key: Some("k-1".to_string()),
        declared_retry: false,
    };

    let landed = decode(&body);
    writer
        .admit_flush(landed, keyed())
        .expect("the first push is admitted")
        .await
        .expect("and commits");
    settle_until("the first landing insert", || landing.call_count() == 1).await;

    let landed = decode(&body);
    let outcome = writer.admit_flush(landed, keyed());
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

    writer.shutdown(Duration::from_secs(5)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// **The other half of the rule: the start time the request DID send is
/// still content.** Two bodies alike but for one span's own instant are two
/// identities and both store.
///
/// **Both ends of the span move by the same nanosecond**, so `duration_ns`
/// is unchanged and only the digest's `start_ns` and `end_ns` terms can
/// discriminate the pair.
#[tokio::test]
async fn two_bodies_differing_only_in_a_span_instant_both_store() {
    let root = spool_root("start-time-differs");
    let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::always(Act::Ok);
    let writer = writer_with(&WriterConfig::default(), &root, landing.clone());

    let mut later = six_spans();
    let span = &mut later.resource_spans[0].scope_spans[0].spans[0];
    span.start_time_unix_nano += 1;
    span.end_time_unix_nano += 1;

    for body in [six_spans(), later] {
        let landed = decode(&body);
        writer
            .admit_flush(landed, PushHeaders::default())
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
/// other half of the case: the gate is decided before anything is queued, so
/// the landing inserter is never called.
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
        let writer = writer_with(&cfg, &root, landing.clone());
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
        writer.shutdown(Duration::from_secs(5)).await;

        // One date past it is refused whole.
        let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::unrecorded(Act::Ok);
        let writer = writer_with(&cfg, &root, landing.clone());
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
            writer.metrics().queue_bytes,
            0,
            "{transport}: and the landing path's own byte counter never rose \
             above 0, so no block was sealed and charged"
        );
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
    let writer = writer_with(&cfg, &root, landing.clone());

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
/// in each route's own container** — and nothing reaches the landing
/// inserter, because the refusal is at admission.
#[tokio::test]
async fn a_push_too_large_is_413_on_the_trace_transports() {
    let root = spool_root("too-large");
    let req = six_spans();
    let landed = decode(&req);
    let rows = landed.total_rows();
    let cfg = WriterConfig {
        trace_landing_max_rows: rows,
        ingest_dedup: false,
        ..Default::default()
    };

    for transport in ["otlp", "zipkin"] {
        let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::unrecorded(Act::Ok);
        let writer = writer_with(&cfg, &root, landing.clone());
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

// -- issue #602: the protections, on the one decode left --------------

/// Posts `req` as OTLP protobuf through the trace route's own handler.
async fn post_request(
    writer: &TraceWriter,
    req: &ExportTraceServiceRequest,
) -> axum::response::Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        "application/x-protobuf".parse().unwrap(),
    );
    pulsus_write::ingest_traces(writer, headers, Body::from(req.encode_to_vec())).await
}

/// A writer whose per-push and queue ceilings sit far above the case's own
/// bound, so only the bound under test can refuse.
fn roomy_writer(root: &Path, landing: Arc<MockInserter<TraceLandingRow>>) -> TraceWriter {
    let cfg = WriterConfig {
        batch_bytes: ByteSize(4 << 30),
        ingest_queue_bytes: ByteSize(16 << 30),
        ingest_dedup: false,
        ..Default::default()
    };
    writer_with(&cfg, root, landing)
}

/// **`T-X1`.** The expansion bound on the trace route flips once, at the span
/// count derived here from the wire sizes. Each span carries one array
/// attribute whose single string is 256 KiB; an array is charged at the
/// JSON-escape worst case, six times its wire bytes, and the resource's
/// `service.name` twice (its promoted value and the attribute). Each span
/// row is charged its own size and its copies of the block's text. `N*` is
/// the smallest span count whose charge exceeds `MAX_EXPANDED_BYTES`.
#[tokio::test]
async fn t_x1_the_expansion_bound_flips_once_at_the_derived_span_count() {
    use opentelemetry_proto::tonic::common::v1::ArrayValue;
    let attr = KeyValue {
        key: "a".to_string(),
        value: Some(AnyValue {
            value: Some(Value::ArrayValue(ArrayValue {
                values: vec![AnyValue {
                    value: Some(Value::StringValue("x".repeat(256 * 1024))),
                }],
            })),
        }),
        key_strindex: 0,
    };
    let service = kv("service.name", "checkout");
    let fixed = 2 * service.encoded_len();
    // The array, charged where it is stored, then what the span copies: its
    // row and name, the rendered service twice, and a resource row with the
    // resource's attributes.
    let per_span = 6 * attr.encoded_len()
        + std::mem::size_of::<pulsus_write::LandingSpan>()
        + "GET /api".len()
        + 2 * "checkout".len()
        + std::mem::size_of::<pulsus_write::LandingResource>()
        + service.encoded_len();
    let max = pulsus_write::protocols::otlp_traces::MAX_EXPANDED_BYTES;
    let n_star = (max - fixed) / per_span + 1;
    eprintln!("T-X1: N* = {n_star}");
    let mut seen = Vec::new();
    for (n, admitted) in [(n_star - 1, true), (n_star, false), (n_star + 1, false)] {
        let root = spool_root("t-x1");
        let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::unrecorded(Act::Ok);
        let writer = roomy_writer(&root, landing.clone());
        let spans = (0..n)
            .map(|_| span_at(0xa1, 0, base_ns(), vec![attr.clone()]))
            .enumerate()
            .map(|(i, mut s)| {
                s.span_id = (i as u64 + 1).to_be_bytes().to_vec();
                s
            })
            .collect();
        let (status, _h, body) =
            parts(post_request(&writer, &request_of("checkout", spans)).await).await;
        seen.push((n, status.as_u16()));
        if admitted {
            assert_eq!(status, StatusCode::OK, "N = {n}");
            settle_until("the admitted push's landing insert", || {
                landing.call_count() == 1
            })
            .await;
        } else {
            assert_eq!(status, StatusCode::BAD_REQUEST, "N = {n}");
            let (code, message) = decode_status(&body);
            assert_eq!(code, 3, "N = {n}: google.rpc.Status code");
            assert!(
                message.contains("expanded trace row bytes"),
                "N = {n}: the expansion bound, not another one: {message}"
            );
            assert_eq!(landing.call_count(), 0, "N = {n}: nothing stored");
        }
        writer.shutdown(Duration::from_secs(5)).await;
        std::fs::remove_dir_all(&root).ok();
    }
    eprintln!("T-X1: (N, status) = {seen:?}");
}

/// **What every span copies is charged once per span** (issue #602). The
/// landing decode stores a resource's and a scope's attributes once per
/// block, but every span row carries its own copy of the rendered service
/// and of the scope, and every span may open a resource row of its own UTC
/// day — the service, the resource's attributes, its schema url and its
/// encoded entity references again. Five shapes, each one 1 MiB value and
/// `N` spans with no attributes of their own: a scope attribute, a resource
/// attribute, `service.name`, the resource's schema url, and an entity
/// reference. `N*` is derived from the wire sizes and `size_of`; the bound
/// flips there.
#[tokio::test]
async fn what_every_span_copies_is_charged_once_per_span() {
    use opentelemetry_proto::tonic::common::v1::{EntityRef, InstrumentationScope};
    let big = "x".repeat(1024 * 1024);
    let max = pulsus_write::protocols::otlp_traces::MAX_EXPANDED_BYTES;
    let span_row = std::mem::size_of::<pulsus_write::LandingSpan>() + "GET /api".len();
    let resource_row = std::mem::size_of::<pulsus_write::LandingResource>();
    let mut wrong = Vec::new();
    for shape in [
        "scope attribute",
        "resource attribute",
        "service.name",
        "resource schema url",
        "entity reference",
    ] {
        let mut service = "checkout".to_string();
        let mut resource_extra = None;
        let mut scope_attr = None;
        let mut schema_url = String::new();
        let mut entity_refs = Vec::new();
        match shape {
            "scope attribute" => scope_attr = Some(kv("s", &big)),
            "resource attribute" => resource_extra = Some(kv("r", &big)),
            "service.name" => service = big.clone(),
            "resource schema url" => schema_url = big.clone(),
            _ => entity_refs.push(EntityRef {
                r#type: big.clone(),
                ..Default::default()
            }),
        }
        let service_kv = kv("service.name", &service);
        // A string attribute is charged its wire bytes. Once per block: the
        // service's promoted value, every resource attribute (`service.name`
        // among them) and every scope attribute.
        let resource_attrs =
            service_kv.encoded_len() + resource_extra.as_ref().map_or(0, |kv| kv.encoded_len());
        let scope_attrs = scope_attr.as_ref().map_or(0, |kv| kv.encoded_len());
        let once = service_kv.encoded_len() + resource_attrs + scope_attrs;
        // The entity references as the resource row stores them: a resource
        // carrying only that field, encoded.
        let refs_len = Resource {
            entity_refs: entity_refs.clone(),
            ..Default::default()
        }
        .encoded_len();
        // Once per span: its row and name, the service twice (its row's and
        // its resource row's), the scope's attributes, and a resource row
        // with the resource's attributes, schema url and entity references.
        let per_span = span_row
            + 2 * service.len()
            + scope_attrs
            + resource_row
            + resource_attrs
            + schema_url.len()
            + refs_len;
        let n_star = (max - once) / per_span + 1;
        eprintln!("{shape}: N* = {n_star}");
        for (n, admitted) in [(n_star - 1, true), (n_star, false)] {
            let root = spool_root("copies");
            let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::unrecorded(Act::Ok);
            let writer = roomy_writer(&root, landing.clone());
            let mut attributes = vec![service_kv.clone()];
            attributes.extend(resource_extra.clone());
            let req = ExportTraceServiceRequest {
                resource_spans: vec![ResourceSpans {
                    resource: Some(Resource {
                        attributes,
                        dropped_attributes_count: 0,
                        entity_refs: entity_refs.clone(),
                    }),
                    scope_spans: vec![ScopeSpans {
                        scope: Some(InstrumentationScope {
                            attributes: scope_attr.clone().into_iter().collect(),
                            ..Default::default()
                        }),
                        spans: (0..n)
                            .map(|i| {
                                let mut s = span_at(0xa3, 0, base_ns(), Vec::new());
                                s.span_id = (i as u64 + 1).to_be_bytes().to_vec();
                                s
                            })
                            .collect(),
                        schema_url: String::new(),
                    }],
                    schema_url: schema_url.clone(),
                }],
            };
            let (status, _h, body) = parts(post_request(&writer, &req).await).await;
            if admitted {
                if status != StatusCode::OK {
                    wrong.push(format!("{shape}: N = {n}: {status}, expected 200"));
                } else {
                    settle_until("the admitted push's landing insert", || {
                        landing.call_count() == 1
                    })
                    .await;
                }
            } else if status != StatusCode::BAD_REQUEST {
                wrong.push(format!("{shape}: N = {n}: {status}, expected 400"));
            } else {
                let (code, message) = decode_status(&body);
                if code != 3 || !message.contains("expanded trace row bytes") {
                    wrong.push(format!("{shape}: N = {n}: code {code}, {message}"));
                }
                if landing.call_count() != 0 {
                    wrong.push(format!("{shape}: N = {n}: something was stored"));
                }
            }
            writer.shutdown(Duration::from_secs(5)).await;
            std::fs::remove_dir_all(&root).ok();
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// **`T-X2`.** An attribute nested one level past `MAX_ANYVALUE_DEPTH` is
/// `400` on the trace route and stores nothing; at the cap it is stored.
#[tokio::test]
async fn t_x2_an_attribute_past_the_depth_cap_is_refused_on_the_route() {
    use opentelemetry_proto::tonic::common::v1::ArrayValue;
    use pulsus_write::protocols::otlp_depth::MAX_ANYVALUE_DEPTH;
    let deep = |levels: usize| {
        let mut value = AnyValue {
            value: Some(Value::StringValue("leaf".to_string())),
        };
        for _ in 1..levels {
            value = AnyValue {
                value: Some(Value::ArrayValue(ArrayValue {
                    values: vec![value],
                })),
            };
        }
        KeyValue {
            key: "deep".to_string(),
            value: Some(value),
            key_strindex: 0,
        }
    };
    for (levels, admitted) in [(MAX_ANYVALUE_DEPTH, true), (MAX_ANYVALUE_DEPTH + 1, false)] {
        let root = spool_root("t-x2");
        let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::unrecorded(Act::Ok);
        let writer = roomy_writer(&root, landing.clone());
        let req = request_of(
            "checkout",
            vec![span_at(0xa2, 0x01, base_ns(), vec![deep(levels)])],
        );
        let (status, _h, body) = parts(post_request(&writer, &req).await).await;
        if admitted {
            assert_eq!(status, StatusCode::OK, "{levels} levels");
            settle_until("the admitted push's landing insert", || {
                landing.call_count() == 1
            })
            .await;
        } else {
            assert_eq!(status, StatusCode::BAD_REQUEST, "{levels} levels");
            assert_eq!(
                decode_status(&body).0,
                3,
                "{levels} levels: google.rpc.Status code"
            );
            assert_eq!(landing.call_count(), 0, "{levels} levels: nothing stored");
        }
        writer.shutdown(Duration::from_secs(5)).await;
        std::fs::remove_dir_all(&root).ok();
    }
}

/// **`T-X5`.** In one push, a span at `2106-02-07T00:00:00Z` is rejected and
/// one at `2106-02-06T23:59:59Z` is stored: `200` with a partial success of
/// one rejected span, and one span row in the landing block.
#[tokio::test]
async fn t_x5_the_first_day_past_the_domain_is_rejected_and_the_last_second_before_it_stored() {
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceResponse;
    const FIRST_UNSAFE_DAY_NS: i64 = 49_710 * NS_PER_DAY; // 2106-02-07T00:00:00Z
    let root = spool_root("t-x5");
    let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::always(Act::Ok);
    let writer = roomy_writer(&root, landing.clone());
    let req = request_of(
        "checkout",
        vec![
            span_at(0xa5, 0x01, FIRST_UNSAFE_DAY_NS, Vec::new()),
            span_at(0xa5, 0x02, FIRST_UNSAFE_DAY_NS - 1_000_000_000, Vec::new()),
        ],
    );
    let (status, _h, body) = parts(post_request(&writer, &req).await).await;
    assert_eq!(status, StatusCode::OK);
    let response = ExportTraceServiceResponse::decode(body.as_slice()).expect("an OTLP response");
    let partial = response.partial_success.expect("a partial success");
    assert_eq!(
        partial.rejected_spans, 1,
        "the first unsafe day's span is rejected"
    );
    settle_until("the landing insert", || landing.call_count() == 1).await;
    assert_eq!(
        count_of_kind(&landing.rows_of(0), 0),
        1,
        "one span row is stored"
    );
    writer.shutdown(Duration::from_secs(5)).await;
    std::fs::remove_dir_all(&root).ok();
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
    use pulsus_schema::{RenderCtx, SchemaParams};
    use pulsus_schema_testkit::run_init;

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
    let landed = decode(&six_spans());
    writer
        .admit_flush(landed, PushHeaders::default())
        .expect("queue has room")
        .await
        .expect("the push commits");
    settle_until("the landing insert to commit", || {
        writer.metrics().landing.flushes_total == 1
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

// === Issue #587: the stored shape, read back off a live server ============
//
// **Live, unlike every case above.** What these assert is what the five
// target tables HOLD, and the values issue #587 adds are produced by the
// writer and by the three materialized views together — only a server runs
// a view, so a mock inserter cannot carry any of them. Each case is gated
// behind `PULSUS_TEST_CLICKHOUSE=1`, like
// `no_landing_insert_reaches_the_asynchronous_queue` above.
//
// **None of these fetches through a route.** Every assertion is on what the
// tables hold.

use futures::StreamExt as _;
use opentelemetry_proto::tonic::common::v1::EntityRef;
use opentelemetry_proto::tonic::trace::v1::{Status, span};
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, Row};
use pulsus_schema::{RenderCtx, SchemaParams};
use pulsus_schema_testkit::run_init;

#[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
struct NumRow {
    n: u64,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
struct StrRow {
    s: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
struct BlobRow {
    #[serde(with = "serde_bytes")]
    b: Vec<u8>,
}

macro_rules! skip_unless_live {
    () => {
        if !pulsus_testkit::live_clickhouse_enabled() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-write/tests/trace_rows_v2.rs for setup)"
            );
            return;
        }
    };
}

fn live_config() -> ChConnConfig {
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
        query_timeout: Duration::from_secs(120),
        ..ChConnConfig::default()
    }
}

/// A fresh database with the whole schema in it, **with merges stopped on
/// the four write-path tables**, and a client bound to it.
///
/// **The merges are stopped because every fixture below is at a fixed past
/// instant, and `spans`, `traces` and `resources` all carry a delete TTL of `retention_days = 7`.** Measured on this engine: a part
/// holding one span at `start_ns = 1` and one at `1700000000000000000` is
/// reported with `rows = 0` within two seconds of the insert, because
/// `ttl_only_drop_parts = 1` drops a wholly-expired part as a background
/// merge. The 1970-01-01 partition `W-11` and `W-17` assert the label of is
/// exactly such a part.
///
/// **Stopping merges weakens no assertion here.** `FINAL` is a read-time
/// collapse and is unaffected, so the two cases that read through it still
/// read what a merged table would give; and `W-17` aggregates with
/// `groupUniqArrayArray` in every assertion, which gives the union whether
/// the day rows are merged or not. What it removes is the part drop, which
/// would leave every one of these cases reading an empty table.
///
/// **`db` is already composed by `pulsus_testkit::test_db` at the call
/// site**, so the per-checkout prefix reaches every name: two checkouts
/// sharing one ClickHouse would otherwise both use it and drop each
/// other's data, which `every_live_test_database_name_comes_from_the_helper`
/// refuses.
async fn live_db(db: String) -> (ChClient, String) {
    let admin = ChClient::new(live_config()).await.expect("connect");
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
    for table in ["spans", "traces", "resources", "trace_landing"] {
        admin
            .execute(
                &format!("SYSTEM STOP MERGES {db}.{table}"),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .unwrap_or_else(|e| panic!("stop merges on {db}.{table}: {e}"));
    }
    let client = ChClient::new(ChConnConfig {
        database: db.clone(),
        ..live_config()
    })
    .await
    .expect("connect to the test database");
    (client, db)
}

async fn drop_live_db(db: &str) {
    let admin = ChClient::new(live_config()).await.expect("connect");
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop the test database");
}

/// The settings every landing insert below carries — the writer's own
/// constructor, so a case's insert is pinned the way a push's is.
///
/// **The deduplication token is minted per call from a counter**, not from
/// the clock: two pushes of one trace inside the window are two blocks, and
/// a repeated token would have the second one suppressed, which is exactly
/// the state `W-17` exists to read across.
fn live_landing_settings() -> QuerySettings {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let n = NEXT.fetch_add(1, Ordering::SeqCst);
    QuerySettings::trace_landing_insert(&format!("it587-{}-{n}", unique()), 1_048_576)
}

fn landing_rows(parsed: &ParsedTraceLanding, received_ms: i64) -> Vec<TraceLandingRow> {
    let mut rows: Vec<TraceLandingRow> = Vec::with_capacity(parsed.total_rows() as usize);
    rows.extend(
        parsed
            .spans
            .iter()
            .cloned()
            .map(|s| TraceLandingRow::span(received_ms, s)),
    );
    rows.extend(
        parsed
            .resources
            .iter()
            .cloned()
            .map(|r| TraceLandingRow::resource(received_ms, r)),
    );
    rows.extend(
        parsed
            .tag_names
            .iter()
            .cloned()
            .map(|t| TraceLandingRow::tag_name(received_ms, t)),
    );
    rows.extend(
        parsed
            .tag_values
            .iter()
            .cloned()
            .map(|t| TraceLandingRow::tag_value(received_ms, t)),
    );
    rows
}

/// The wall clock in milliseconds, for `received_ms` alone: the landing
/// table's own TTL is six hours over that column, so a fixture's chosen
/// span instants must not reach it.
fn wall_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("a clock after the epoch")
            .as_millis(),
    )
    .expect("a representable instant")
}

/// Decodes `req` with the landing decoder and inserts every row it produces
/// into `trace_landing` through the production client, in **one** block —
/// one push is one insert.
async fn land_live(
    client: &ChClient,
    req: &ExportTraceServiceRequest,
    settings: &QuerySettings,
) -> ParsedTraceLanding {
    let parsed = pulsus_write::parse_trace_landing(req).expect("the landing decode");
    client
        .insert_block_with(LANDING, &landing_rows(&parsed, wall_ms()), settings)
        .await
        .expect("the landing insert");
    parsed
}

async fn live_count(client: &ChClient, sql: &str) -> u64 {
    let mut stream = client
        .query_stream::<NumRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("count failed: {e}\nSQL:\n{sql}"));
    stream.next().await.expect("one row").expect("decode").n
}

async fn live_scalar(client: &ChClient, sql: &str) -> String {
    let mut stream = client
        .query_stream::<StrRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("scalar failed: {e}\nSQL:\n{sql}"));
    stream.next().await.expect("one row").expect("decode").s
}

async fn live_texts(client: &ChClient, sql: &str) -> Vec<String> {
    let mut stream = client
        .query_stream::<StrRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("text query failed: {e}\nSQL:\n{sql}"));
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode").s);
    }
    out
}

async fn live_blob(client: &ChClient, sql: &str) -> Vec<u8> {
    let mut stream = client
        .query_stream::<BlobRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("blob query failed: {e}\nSQL:\n{sql}"));
    stream.next().await.expect("one row").expect("decode").b
}

/// A fixed instant well inside the admitted UTC-day domain and on a
/// different UTC day from the epoch: 2023-11-14, which
/// `intDiv(start_ns, 300000000000)` puts in bucket 5,666,666.
const FIXED_NS: i64 = 1_700_000_000_000_000_000;

fn any_kv(key: &str, value: Value) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue { value: Some(value) }),
        key_strindex: 0,
    }
}

/// One `ResourceSpans` carrying the resource given, with no scope.
fn resource_spans_of(resource: Resource, spans: Vec<Span>) -> ResourceSpans {
    ResourceSpans {
        resource: Some(resource),
        scope_spans: vec![ScopeSpans {
            scope: None,
            spans,
            schema_url: String::new(),
        }],
        schema_url: String::new(),
    }
}

/// **W-4 (issue #587 row 6).** A `service.name` that is present-but-empty,
/// or not a string, round-trips: it stays in `resources.attrs` as the typed
/// value it was.
///
/// The `attrs` skip applies only to a non-empty `StringValue` — the one arm
/// `spans.service` carries losslessly. A string `service.name`, empty or
/// not, is catalogued like any other resource key (issue #598 part 1): one
/// name row and one `string` value row per distinct value. The catalog rows
/// of the integer arm are not asserted (the plan's L5).
///
/// **Three values are three resource identities**, so this push stores
/// three `resources` rows: `service.name` is in the identity buffer and the
/// buffer is type-tagged.
#[tokio::test]
async fn w4_a_service_name_that_is_not_a_non_empty_string_round_trips() {
    skip_unless_live!();
    let (client, db) = live_db(pulsus_testkit::test_db("pulsus_trace_landing_it_w4")).await;

    let req = ExportTraceServiceRequest {
        resource_spans: vec![
            resource_spans_of(
                Resource {
                    attributes: vec![any_kv(
                        "service.name",
                        Value::StringValue("checkout".to_string()),
                    )],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                },
                vec![span_at(0xc4, 0x01, FIXED_NS, Vec::new())],
            ),
            resource_spans_of(
                Resource {
                    attributes: vec![any_kv("service.name", Value::StringValue(String::new()))],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                },
                vec![span_at(0xc4, 0x02, FIXED_NS, Vec::new())],
            ),
            resource_spans_of(
                Resource {
                    attributes: vec![any_kv("service.name", Value::IntValue(7))],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                },
                vec![span_at(0xc4, 0x03, FIXED_NS, Vec::new())],
            ),
        ],
    };
    land_live(&client, &req, &live_landing_settings()).await;

    // Three identities, three rows — §7.0c's derivation, not the fixture's
    // own count.
    assert_eq!(
        live_count(&client, "SELECT count() AS n FROM resources FINAL").await,
        3,
        "three `service.name` values are three resource identities"
    );
    assert_eq!(
        live_count(
            &client,
            "SELECT uniqExact(resource_id) AS n FROM resources FINAL"
        )
        .await,
        3,
        "and three distinct identities"
    );

    // The span column carries the text rendering in every case.
    assert_eq!(
        live_texts(
            &client,
            "SELECT toString(service) AS s FROM spans FINAL ORDER BY service"
        )
        .await,
        vec![String::new(), "7".to_string(), "checkout".to_string()],
        "`spans.service` renders every arm"
    );

    // And `spans.service_type` names the arm beside it, in the same order
    // (issue #589): the empty string is a string, which `service` alone
    // cannot tell from an absent key.
    assert_eq!(
        live_texts(
            &client,
            "SELECT toString(service_type) AS s FROM spans FINAL ORDER BY service"
        )
        .await,
        vec![
            "string".to_string(),
            "int".to_string(),
            "string".to_string()
        ],
        "`spans.service_type` names every arm"
    );

    // And the stored attributes keep the value for every arm BUT a
    // non-empty string: `None` is what `dynamicType` answers for a path the
    // JSON column does not hold.
    assert_eq!(
        live_texts(
            &client,
            "SELECT concat(toString(service), '|', \
                           dynamicType(attrs.`service%2Ename`)) AS s \
             FROM resources FINAL ORDER BY service"
        )
        .await,
        vec![
            "|String".to_string(),
            "7|Int64".to_string(),
            "checkout|None".to_string()
        ],
        "the empty string stays as a String path, the integer as an Int64 \
         path, and only the non-empty string is dropped"
    );
    assert_eq!(
        live_scalar(
            &client,
            "SELECT toString(length(assumeNotNull(attrs.`service%2Ename`.:String))) AS s \
             FROM resources FINAL WHERE service = ''"
        )
        .await,
        "0",
        "the empty-string arm is PRESENT and empty, not absent"
    );
    assert_eq!(
        live_scalar(
            &client,
            "SELECT toString(assumeNotNull(attrs.`service%2Ename`.:Int64)) AS s \
             FROM resources FINAL WHERE service = '7'"
        )
        .await,
        "7",
        "the integer arm keeps its integer value"
    );

    // A string `service.name` is catalogued: one name row, and one
    // `string` value row per distinct value, the empty one included.
    assert_eq!(
        live_count(
            &client,
            "SELECT count() AS n FROM tag_names FINAL \
             WHERE scope = 'resource' AND key = 'service.name'"
        )
        .await,
        1,
        "`service.name` is one catalog name"
    );
    assert_eq!(
        live_texts(
            &client,
            "SELECT concat(value, '|', val_type) AS s FROM tag_values FINAL \
             WHERE scope = 'resource' AND key = 'service.name' AND val_type = 'string' \
             ORDER BY value"
        )
        .await,
        vec!["|string".to_string(), "checkout|string".to_string()],
        "its two string values are catalogued, typed `string`"
    );

    drop_live_db(&db).await;
}

/// **W-5 (issue #587 row 7).** A span `kind` outside a byte and a link id
/// of the wrong length are stored as the sender sent them, and **no
/// rejection is introduced**.
///
/// The reference stores a signed integer kind and copies a link's ids with
/// no length check, and returns both verbatim, so narrowing or rejecting
/// either would be a divergence introduced here. `kind = 7` is the control
/// — a byte stores it either way — and `-1` and `300` are what
/// discriminate.
///
/// **The rejection half is asserted on the landing decode's own count**,
/// which is the figure the route's partial-success body reports.
#[tokio::test]
async fn w5_a_kind_outside_a_byte_and_an_off_length_link_id_are_stored_as_sent() {
    skip_unless_live!();
    let (client, db) = live_db(pulsus_testkit::test_db("pulsus_trace_landing_it_w5")).await;

    let mut spans = Vec::new();
    for (id, kind) in [(0x01u8, -1i32), (0x02, 300), (0x03, 7)] {
        spans.push(Span {
            kind,
            ..span_at(0xc5, id, FIXED_NS, Vec::new())
        });
    }
    spans.push(Span {
        links: vec![span::Link {
            trace_id: vec![0xaa, 0xbb, 0xcc, 0xdd],
            span_id: vec![0x01, 0x02, 0x03],
            trace_state: String::new(),
            attributes: Vec::new(),
            dropped_attributes_count: 0,
            flags: 0,
        }],
        ..span_at(0xc5, 0x04, FIXED_NS, Vec::new())
    });

    let parsed = land_live(
        &client,
        &request_with_resource_spans(spans),
        &live_landing_settings(),
    )
    .await;

    assert_eq!(
        parsed.rejected, 0,
        "no span is rejected: the fix is a wider column, not a refusal"
    );
    assert_eq!(parsed.rejected_message, None, "and no rejection message");
    assert_eq!(
        live_count(&client, "SELECT count() AS n FROM spans FINAL").await,
        4,
        "all four spans landed"
    );

    assert_eq!(
        live_texts(
            &client,
            "SELECT concat(hex(span_id), '|', toString(kind)) AS s \
             FROM spans FINAL ORDER BY span_id"
        )
        .await,
        vec![
            "0101010101010101|-1".to_string(),
            "0202020202020202|300".to_string(),
            "0303030303030303|7".to_string(),
            "0404040404040404|2".to_string(),
        ],
        "the protocol's own signed kind, stored as it arrived"
    );

    assert_eq!(
        live_scalar(
            &client,
            "SELECT concat(hex(links[1].trace_id), '|', hex(links[1].span_id)) AS s \
             FROM spans FINAL WHERE span_id = unhex('0404040404040404')"
        )
        .await,
        "AABBCCDD|010203",
        "a link's ids are the bytes the sender put on the wire"
    );

    drop_live_db(&db).await;
}

/// One request whose single resource carries `service.name = "checkout"`
/// and the spans given.
fn request_with_resource_spans(spans: Vec<Span>) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![resource_spans_of(
            Resource {
                attributes: vec![any_kv(
                    "service.name",
                    Value::StringValue("checkout".to_string()),
                )],
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            },
            spans,
        )],
    }
}

/// **W-6 (issue #587 row 8).** `Resource.entity_refs` is stored, in the
/// carrier §3.6 names, and it is part of the resource's identity.
///
/// Three halves, each its own failure: a dropped column loses the field; a
/// different carrier — a private wrapper, or concatenated bare messages —
/// fails the `Resource::decode` and the `0x1a` key-byte assertion; and an
/// identity that ignores the field collapses two resources differing only
/// in it into one row. The zero-bytes assertion is what pins the empty case
/// against an empty-message encoding, which would move every existing
/// `resource_id`.
#[tokio::test]
async fn w6_the_entity_references_are_stored_in_their_own_carrier_and_in_the_identity() {
    skip_unless_live!();
    let (client, db) = live_db(pulsus_testkit::test_db("pulsus_trace_landing_it_w6")).await;

    let entity = |kind: &str| EntityRef {
        schema_url: "https://example.invalid/v1".to_string(),
        r#type: kind.to_string(),
        id_keys: vec!["host.name".to_string()],
        description_keys: Vec::new(),
    };
    let attrs = || {
        vec![
            any_kv("service.name", Value::StringValue("checkout".to_string())),
            any_kv("host.name", Value::StringValue("node-a".to_string())),
        ]
    };
    let two = vec![entity("host"), entity("service")];

    // Three resources that differ in NOTHING but `entity_refs`.
    let req = ExportTraceServiceRequest {
        resource_spans: vec![
            resource_spans_of(
                Resource {
                    attributes: attrs(),
                    dropped_attributes_count: 0,
                    entity_refs: two.clone(),
                },
                vec![span_at(0xc6, 0x01, FIXED_NS, Vec::new())],
            ),
            resource_spans_of(
                Resource {
                    attributes: attrs(),
                    dropped_attributes_count: 0,
                    entity_refs: vec![entity("host")],
                },
                vec![span_at(0xc6, 0x02, FIXED_NS, Vec::new())],
            ),
            resource_spans_of(
                Resource {
                    attributes: attrs(),
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                },
                vec![span_at(0xc6, 0x03, FIXED_NS, Vec::new())],
            ),
        ],
    };
    land_live(&client, &req, &live_landing_settings()).await;

    assert_eq!(
        live_count(&client, "SELECT count() AS n FROM resources FINAL").await,
        3,
        "two resources differing only in `entity_refs` are two rows"
    );
    assert_eq!(
        live_count(
            &client,
            "SELECT uniqExact(resource_id) AS n FROM resources FINAL"
        )
        .await,
        3,
        "and the identity is what separates them"
    );
    assert_eq!(
        live_count(
            &client,
            "SELECT count() AS n FROM resources FINAL WHERE length(entity_refs) = 0"
        )
        .await,
        1,
        "an empty `entity_refs` stores ZERO bytes, not an empty message"
    );

    // The carrier, decoded as what §3.6 says it is.
    let carrier = live_blob(
        &client,
        "SELECT entity_refs AS b FROM resources FINAL \
         ORDER BY length(entity_refs) DESC LIMIT 1",
    )
    .await;
    assert_eq!(
        carrier.first(),
        Some(&0x1au8),
        "field 3, wire type 2 — `(3 << 3) | 2` — is the first key byte: {carrier:?}"
    );
    let decoded = Resource::decode(carrier.as_slice()).expect("a Resource");
    assert_eq!(decoded.entity_refs, two, "the references the sender sent");
    assert!(
        decoded.attributes.is_empty(),
        "fields 1 and 2 come back at their proto3 defaults and are NOT the \
         resource's: {decoded:?}"
    );
    assert_eq!(decoded.dropped_attributes_count, 0, "likewise field 2");

    drop_live_db(&db).await;
}

/// **W-8 (issue #587 row 9, and §3.5a).** A span's own end is stored
/// verbatim — zero, inverted and the unsigned maximum alike — while
/// `duration_ns` keeps its clamped value, and the per-trace end **saturates
/// rather than wrapping**.
///
/// Four spans, each its own trace (§7.0f), so no trace of this fixture
/// occupies two partitions and every per-trace assertion is over one span.
/// The `traces.end_ns` assertion for (d) is the **correct** value and not
/// the wrap: `max(start_ns + duration_ns)` over `Int64` operands wraps at
/// `1 + i64::MAX` to `-9223372036854775808`, which excluded the longest
/// trace the protocol can express from `{ trace:duration > 1s }`. The (c)
/// assertion is the other half: the clamp must change **nothing** that does
/// not overflow, so an implementation that saturates early reddens there.
#[tokio::test]
async fn w8_a_spans_own_end_is_verbatim_and_the_per_trace_end_saturates() {
    skip_unless_live!();
    let (client, db) = live_db(pulsus_testkit::test_db("pulsus_trace_landing_it_w8")).await;

    let start = u64::try_from(FIXED_NS).expect("a post-epoch fixture");
    let spans = vec![
        // (a) an unset end.
        Span {
            trace_id: vec![0xa8; 16],
            end_time_unix_nano: 0,
            ..span_at(0xa8, 0x01, FIXED_NS, Vec::new())
        },
        // (b) an end before the start.
        Span {
            trace_id: vec![0xb8; 16],
            end_time_unix_nano: start - 1_000_000_000,
            ..span_at(0xb8, 0x02, FIXED_NS, Vec::new())
        },
        // (c) an ordinary end.
        Span {
            trace_id: vec![0xc8; 16],
            end_time_unix_nano: start + 500_000_000,
            ..span_at(0xc8, 0x03, FIXED_NS, Vec::new())
        },
        // (d) the largest end the protocol can express, against the
        // smallest start it can: the accepted maximum, which
        // `parse_saturates_an_i64_overflowing_duration_to_max` pins as
        // saturating on the duration.
        Span {
            trace_id: vec![0xd8; 16],
            start_time_unix_nano: 1,
            end_time_unix_nano: u64::MAX,
            ..span_at(0xd8, 0x04, FIXED_NS, Vec::new())
        },
    ];
    land_live(
        &client,
        &request_with_resource_spans(spans),
        &live_landing_settings(),
    )
    .await;

    assert_eq!(
        live_texts(
            &client,
            "SELECT concat(hex(span_id), '|', toString(end_ns), '|', toString(duration_ns)) AS s \
             FROM spans FINAL ORDER BY span_id"
        )
        .await,
        vec![
            format!("0101010101010101|0|0"),
            format!("0202020202020202|{}|0", start - 1_000_000_000),
            format!("0303030303030303|{}|500000000", start + 500_000_000),
            "0404040404040404|18446744073709551615|9223372036854775807".to_string(),
        ],
        "every end is the sender's own, and `duration_ns` keeps its clamp"
    );

    // The per-trace aggregate: (d) saturates, (c) is untouched.
    assert_eq!(
        live_scalar(
            &client,
            "SELECT concat(toString(max(end_ns)), '|', toString(max(last_start_ns))) AS s \
             FROM traces FINAL WHERE trace_id = unhex('D8D8D8D8D8D8D8D8D8D8D8D8D8D8D8D8')"
        )
        .await,
        "9223372036854775807|1",
        "the per-trace end saturates at `i64::MAX` instead of wrapping \
         negative, and the per-trace last start is the span's own"
    );
    assert_eq!(
        live_scalar(
            &client,
            "SELECT toString(max(end_ns)) AS s FROM traces FINAL \
             WHERE trace_id = unhex('C8C8C8C8C8C8C8C8C8C8C8C8C8C8C8C8')"
        )
        .await,
        (start + 500_000_000).to_string(),
        "and the clamp changes nothing that does not overflow"
    );

    drop_live_db(&db).await;
}

/// **W-10's live half (issue #587 rows 8 and 13).** Five resources that
/// differ only in their dropped count and their entity references are
/// **five** stored rows, each carrying its own values.
///
/// The state is §7.0a's derivation and not the fixture's own count: all five
/// share byte-identical attributes and schema url, all five spans are in one
/// push on one UTC day, and `resources` is keyed `(resource_id, day)` — so
/// five distinct identities on one day are five rows.
///
/// **On the unamended tree this returns one row** carrying the first landed
/// resource's values, because the identity separates none of the five.
/// `1` against `5` is the discriminator; the hermetic half, in
/// `otlp_traces.rs`, is what makes the input set exhaustive.
#[tokio::test]
async fn w10_five_resources_differing_only_in_two_fields_are_five_rows() {
    skip_unless_live!();
    let (client, db) = live_db(pulsus_testkit::test_db("pulsus_trace_landing_it_w10")).await;

    let entity = |kind: &str| EntityRef {
        schema_url: "https://example.invalid/v1".to_string(),
        r#type: kind.to_string(),
        id_keys: vec!["host.name".to_string()],
        description_keys: Vec::new(),
    };
    let attrs = || {
        vec![
            any_kv("service.name", Value::StringValue("checkout".to_string())),
            any_kv("host.name", Value::StringValue("node-a".to_string())),
        ]
    };
    let five: Vec<(u8, u32, Vec<EntityRef>)> = vec![
        (0x01, 0, Vec::new()),
        (0x02, 5, Vec::new()),
        (0x03, 6, Vec::new()),
        (0x04, 5, vec![entity("host")]),
        (0x05, 5, vec![entity("service")]),
    ];
    let req = ExportTraceServiceRequest {
        resource_spans: five
            .iter()
            .map(|(id, dropped, refs)| {
                resource_spans_of(
                    Resource {
                        attributes: attrs(),
                        dropped_attributes_count: *dropped,
                        entity_refs: refs.clone(),
                    },
                    vec![span_at(0xca, *id, FIXED_NS, Vec::new())],
                )
            })
            .collect(),
    };
    land_live(&client, &req, &live_landing_settings()).await;

    assert_eq!(
        live_count(&client, "SELECT count() AS n FROM resources FINAL").await,
        5,
        "five identities on one day are five rows"
    );
    assert_eq!(
        live_count(
            &client,
            "SELECT uniqExact(resource_id) AS n FROM resources FINAL"
        )
        .await,
        5,
        "five DISTINCT identities"
    );
    assert_eq!(
        live_count(&client, "SELECT uniqExact(day) AS n FROM resources FINAL").await,
        1,
        "on one UTC day, so the five rows are five identities and not five days"
    );

    // Each row carries its own count and its own references.
    assert_eq!(
        live_texts(
            &client,
            "SELECT concat(toString(dropped_attrs), '|', toString(length(entity_refs) > 0)) AS s \
             FROM resources FINAL ORDER BY dropped_attrs, length(entity_refs)"
        )
        .await,
        vec![
            "0|0".to_string(),
            "5|0".to_string(),
            "5|1".to_string(),
            "5|1".to_string(),
            "6|0".to_string(),
        ],
        "every row carries the count and the references it was sent with"
    );

    drop_live_db(&db).await;
}

/// **W-11 (issue #587 row 10, §4.1 and §0.1d).** A span whose
/// `start_time_unix_nano` is zero is stored **as zero** — the sender's own
/// instant, in the sender's own partition.
///
/// **The case runs under two session timezones and asserts the same values
/// in both**, which is the only way the partition half can hold anything:
/// under UTC a bare `toDate(fromUnixTimestamp64Nano(0))` already answers
/// `1970-01-01`, so an unamended expression passes a UTC-only run. Under
/// `Pacific/Honolulu` the bare form answers `2149-06-06` — a pre-epoch
/// local instant underflowing the 16-bit `Date` domain — and `traces`'
/// delete TTL reads `day`, so the trace would outlive its retention by 126
/// years.
///
/// **The two `resources` counts mean two different things and the case says
/// which.** The raw count is the fixture's state: one resource referenced
/// by spans on two UTC days is two rows, keyed `(resource_id, day)`. The
/// finalised count is the identity: `day` is outside `resources`' sort key
/// and `do_not_merge_across_partitions_select_final` is `0` by default, so
/// `FINAL` collapses the two partitions into one row.
///
/// **And the `max`-against-`min` pair is here and nowhere else.** `W-8`'s
/// asserted trace has one span, so `max(start_ns)` and `min(start_ns)`
/// agree and an implementer who wrote `min` passed it. This trace has two
/// spans whose starts are `0` and a 2023 instant, so the two operators give
/// different answers and the case names which it means.
#[tokio::test]
async fn w11_a_zero_start_is_stored_as_zero_in_a_utc_dated_partition() {
    skip_unless_live!();

    for (leg, session, name) in [
        (
            "utc",
            None,
            pulsus_testkit::test_db("pulsus_trace_landing_it_w11_utc"),
        ),
        (
            "honolulu",
            Some("Pacific/Honolulu"),
            pulsus_testkit::test_db("pulsus_trace_landing_it_w11_honolulu"),
        ),
    ] {
        let (client, db) = live_db(name).await;

        let spans = vec![
            Span {
                start_time_unix_nano: 0,
                end_time_unix_nano: 0,
                ..span_at(0xcb, 0x01, FIXED_NS, Vec::new())
            },
            span_at(0xcb, 0x02, FIXED_NS, Vec::new()),
        ];
        let req = request_with_resource_spans(spans);

        let mut settings = live_landing_settings();
        if let Some(zone) = session {
            settings = settings.set("session_timezone", zone);
        }
        land_live(&client, &req, &settings).await;

        assert_eq!(
            live_scalar(
                &client,
                "SELECT toString(start_ns) AS s FROM spans FINAL \
                 WHERE span_id = unhex('0101010101010101')"
            )
            .await,
            "0",
            "{leg}: a zero start is the sender's own value and is stored as it arrived"
        );

        // The partition LABEL, which is what the explicit zone decides.
        assert_eq!(
            live_texts(
                &client,
                &format!(
                    "SELECT partition AS s FROM system.parts \
                     WHERE database = '{db}' AND table = 'spans' AND active \
                     ORDER BY partition"
                )
            )
            .await,
            vec!["1970-01-01".to_string(), "2023-11-14".to_string()],
            "{leg}: the zero-start span's part is dated by the UTC day the \
             sender named, whatever the session's own zone"
        );
        assert_eq!(
            live_scalar(&client, "SELECT toString(any(day)) AS s FROM traces FINAL").await,
            "1970-01-01",
            "{leg}: the per-trace row is filed under the block's MINIMUM \
             start day, and `traces`' delete TTL reads that column"
        );

        // One resource, two UTC days: two rows raw, one identity finalised.
        assert_eq!(
            live_count(&client, "SELECT count() AS n FROM resources").await,
            2,
            "{leg}: one resource referenced by spans on two UTC days is two \
             rows, keyed `(resource_id, day)`"
        );
        assert_eq!(
            live_count(&client, "SELECT count() AS n FROM resources FINAL").await,
            1,
            "{leg}: and one identity, because `day` is outside the sort key"
        );
        assert_eq!(
            live_count(&client, "SELECT count() AS n FROM traces FINAL").await,
            1,
            "{leg}: one trace"
        );

        // `max` against `min`, named.
        assert_eq!(
            live_scalar(
                &client,
                "SELECT concat(toString(min(start_ns)), '|', toString(max(last_start_ns))) AS s \
                 FROM traces FINAL"
            )
            .await,
            format!("0|{FIXED_NS}"),
            "{leg}: the per-trace start is the EARLIEST of the trace's spans \
             and `last_start_ns` the LATEST — `min` in the view's `s` and \
             `max` in its `ls`, which a one-span trace cannot tell apart"
        );

        // The ordinary span is unaffected.
        assert_eq!(
            live_scalar(
                &client,
                "SELECT toString(start_ns) AS s FROM spans FINAL \
                 WHERE span_id = unhex('0202020202020202')"
            )
            .await,
            FIXED_NS.to_string(),
            "{leg}: the ordinary span is untouched"
        );

        drop_live_db(&db).await;
    }
}

/// **W-12 (issue #587 row 11).** An event time above `i64::MAX` is stored
/// unsaturated.
///
/// The ordinary event is the control — an `Int64` member stores it either
/// way — and the maximum one discriminates. The column's own type is the
/// failure point before the comparison is ever reached: an `Int64` tuple
/// member refuses the `u64` on insert.
#[tokio::test]
async fn w12_an_event_time_above_the_signed_maximum_is_stored_unsaturated() {
    skip_unless_live!();
    let (client, db) = live_db(pulsus_testkit::test_db("pulsus_trace_landing_it_w12")).await;

    let span = Span {
        events: vec![
            span::Event {
                time_unix_nano: u64::MAX,
                name: "first".to_string(),
                attributes: Vec::new(),
                dropped_attributes_count: 0,
            },
            span::Event {
                time_unix_nano: 1_700_000_000_000_000_000,
                name: "second".to_string(),
                attributes: Vec::new(),
                dropped_attributes_count: 0,
            },
        ],
        ..span_at(0xcc, 0x01, FIXED_NS, Vec::new())
    };
    land_live(
        &client,
        &request_with_resource_spans(vec![span]),
        &live_landing_settings(),
    )
    .await;

    assert_eq!(
        live_scalar(
            &client,
            "SELECT concat(toString(events[1].time_ns), '|', \
                           toString(events[2].time_ns)) AS s FROM spans FINAL"
        )
        .await,
        "18446744073709551615|1700000000000000000",
        "the protocol's own unsigned event time, stored as it arrived"
    );

    drop_live_db(&db).await;
}

/// **W-13 (issue #587 row 12).** A `status.code` outside a byte is stored
/// as the protocol's own signed value.
///
/// `2` and the absent-`Status` `0` are the controls; `300` and `-1`
/// discriminate. Narrowing to a byte turns both into `0`, which is
/// `STATUS_CODE_UNSET` — so an out-of-range code came back as "unset"
/// rather than as the unknown code it is, with nothing in a response to say
/// so.
///
/// The absent-status row asserts §3.0a's exemption rather than assuming it:
/// an absent `Status` and `code = 0` both store `0`, and the reference does
/// the same.
#[tokio::test]
async fn w13_a_status_code_outside_a_byte_is_stored_as_the_signed_value_sent() {
    skip_unless_live!();
    let (client, db) = live_db(pulsus_testkit::test_db("pulsus_trace_landing_it_w13")).await;

    let mut spans = Vec::new();
    for (id, code) in [(0x01u8, 300i32), (0x02, -1), (0x03, 2)] {
        spans.push(Span {
            status: Some(Status {
                message: String::new(),
                code,
            }),
            ..span_at(0xcd, id, FIXED_NS, Vec::new())
        });
    }
    spans.push(Span {
        status: None,
        ..span_at(0xcd, 0x04, FIXED_NS, Vec::new())
    });
    land_live(
        &client,
        &request_with_resource_spans(spans),
        &live_landing_settings(),
    )
    .await;

    assert_eq!(
        live_texts(
            &client,
            "SELECT concat(hex(span_id), '|', toString(status_code)) AS s \
             FROM spans FINAL ORDER BY span_id"
        )
        .await,
        vec![
            "0101010101010101|300".to_string(),
            "0202020202020202|-1".to_string(),
            "0303030303030303|2".to_string(),
            "0404040404040404|0".to_string(),
        ],
        "the protocol's own signed status code, stored as it arrived"
    );

    drop_live_db(&db).await;
}

/// **W-17 (issue #587 §4.2).** `traces.buckets` is the trace's **distinct**
/// span buckets, asserted against the spans it is supposed to describe.
///
/// **Three pushes and a repeated bucket**, because one push would not test
/// the union across day rows and a push without a repeat would not test the
/// deduplication. §7.0d derives the state: pushes 2 and 3 file under the
/// same day as each other and push 1 under `1970-01-01`, so `traces` holds
/// two rows and the column's value is the union across both.
///
/// **Every assertion aggregates, and correct code fails one that does
/// not.** Measured: reading `arraySort(buckets)` over two day rows returns
/// **two rows**, `[0, 5666666]` and `[5666666, 5666667]`, while
/// `arraySort(groupUniqArrayArray(buckets))` returns the union. The
/// aggregate is also what makes these assertions independent of the merge
/// state: unmerged there are rows to union, merged there is one row already
/// unioned, and `groupUniqArrayArray` gives the same set either way.
///
/// **This column is the only stored value either half of issue #587's read
/// correctness depends on that is invisible in the statement text**: a
/// wrong divisor leaves the fetch returning fewer spans, silently, with
/// the SQL still correct. **Assertion (b) is what sees a wrong divisor**,
/// and that is checked by doing it — with `600000000000` the stored set
/// is `[0, 2833333]` against the spans' `[0, 5666666, 5666667]`.
///
/// **What (b) cannot see is the view's aggregate NAME**, and nothing here
/// can, because `traces` is an `AggregatingMergeTree`: the engine applies
/// the column's own declared `groupUniqArrayArray(4096)` when it writes
/// the part, whatever the view emitted. Measured on 26.3.29.7, a
/// 10-element array carrying each value twice inserted into a column of
/// that type:
///
/// ```text
///   on an AggregatingMergeTree  ->  [0,1,2,3,4]
///   on a plain MergeTree        ->  [0,0,1,1,2,2,3,3,4,4]
/// ```
///
/// So `groupArray` for `groupUniqArray` in the view leaves every assertion
/// here green — checked by doing that too. The declaration is what the
/// stored value answers to, and
/// `the_per_trace_aggregate_columns_carry_the_functions_this_design_names`
/// (`crates/pulsus-schema/tests/live_traces_v2.rs`) reads it back off the
/// server.
#[tokio::test]
async fn w17_the_stored_bucket_set_is_the_traces_own_distinct_span_buckets() {
    skip_unless_live!();
    let (client, db) = live_db(pulsus_testkit::test_db("pulsus_trace_landing_it_w17")).await;

    let trace = "CBCBCBCBCBCBCBCBCBCBCBCBCBCBCBCB";
    // Push 1: bucket 0 and bucket 5,666,666.
    land_live(
        &client,
        &request_with_resource_spans(vec![
            Span {
                start_time_unix_nano: 0,
                end_time_unix_nano: 0,
                ..span_at(0xcb, 0x01, FIXED_NS, Vec::new())
            },
            span_at(0xcb, 0x02, FIXED_NS, Vec::new()),
        ]),
        &live_landing_settings(),
    )
    .await;
    // Push 2: bucket 5,666,666 **again**, from a different span.
    land_live(
        &client,
        &request_with_resource_spans(vec![span_at(0xcb, 0x03, FIXED_NS, Vec::new())]),
        &live_landing_settings(),
    )
    .await;
    // Push 3: the next bucket, 5,666,667.
    land_live(
        &client,
        &request_with_resource_spans(vec![span_at(
            0xcb,
            0x04,
            FIXED_NS + 300_000_000_000,
            Vec::new(),
        )]),
        &live_landing_settings(),
    )
    .await;

    // §7.0d's derived state, asserted rather than assumed - **as the
    // number of DAY partitions the trace occupies, not as a raw row
    // count.** Pushes 2 and 3 file under one day and push 1 under
    // `1970-01-01`, so the trace's rows span two partitions; whether the
    // two same-day rows are one row or two is a merge this test does not
    // control, and a row count would pin that.
    assert_eq!(
        live_count(
            &client,
            &format!("SELECT uniqExact(day) AS n FROM traces WHERE trace_id = unhex('{trace}')")
        )
        .await,
        2,
        "three pushes whose minima fall on two days put the trace's rows in \
         two day partitions, so the union below has to cross them"
    );

    // (a) the literal control, so a reader can see the expected set.
    let stored = live_scalar(
        &client,
        &format!(
            "SELECT toString(arraySort(groupUniqArrayArray(buckets))) AS s \
             FROM traces FINAL WHERE trace_id = unhex('{trace}')"
        ),
    )
    .await;
    assert_eq!(
        stored, "[0,5666666,5666667]",
        "the stored bucket set, unioned across the trace's day rows"
    );

    // (b) the same value against the spans themselves, read in this test
    // rather than restated.
    let from_spans = live_scalar(
        &client,
        &format!(
            "SELECT toString(arraySort(groupUniqArray(intDiv(start_ns, 300000000000)))) AS s \
             FROM spans FINAL WHERE trace_id = unhex('{trace}')"
        ),
    )
    .await;
    assert_eq!(
        stored, from_spans,
        "the stored set must be the distinct buckets of the trace's own spans"
    );

    // (c) the repeated bucket is one element, not two.
    assert_eq!(
        live_scalar(
            &client,
            &format!(
                "SELECT toString(length(groupUniqArrayArray(buckets))) AS s \
                 FROM traces FINAL WHERE trace_id = unhex('{trace}')"
            )
        )
        .await,
        "3",
        "four spans over three distinct buckets are three elements"
    );

    // (d) the boundary: a push of 4,097 distinct buckets stores 4,096.
    //
    // **It sees the cap holding, and it cannot say WHICH expression held
    // it.** Two do: the view's `groupUniqArray(4096)` per push and the
    // column's declared `groupUniqArrayArray(4096)`, which an
    // `AggregatingMergeTree` applies when it writes the part — measured,
    // a 5,000-element array into a column of that type stores 5,000 on a
    // plain `MergeTree` and 4,096 on this one. Checked by removing each in
    // turn: this assertion stays green either way, because one push needs
    // only one of them. What it holds is the guarantee part 2's truncation
    // branch is written against — a stored row of exactly 4,096 elements
    // may be truncated and one shorter is complete — and the declaration
    // itself is read back off the server by
    // `the_per_trace_aggregate_columns_carry_the_functions_this_design_names`
    // (`crates/pulsus-schema/tests/live_traces_v2.rs`).
    let wide = "DBDBDBDBDBDBDBDBDBDBDBDBDBDBDBDB";
    let spans: Vec<Span> = (0..4097u32)
        .map(|i| Span {
            trace_id: vec![0xdb; 16],
            span_id: i.to_be_bytes().repeat(2),
            start_time_unix_nano: u64::from(i) * 300_000_000_000,
            end_time_unix_nano: u64::from(i) * 300_000_000_000 + 1_000_000,
            ..span_at(0xdb, 0x00, FIXED_NS, Vec::new())
        })
        .collect();
    land_live(
        &client,
        &request_with_resource_spans(spans),
        &live_landing_settings(),
    )
    .await;
    assert_eq!(
        live_texts(
            &client,
            &format!(
                "SELECT toString(length(buckets)) AS s FROM traces \
                 WHERE trace_id = unhex('{wide}')"
            )
        )
        .await,
        vec!["4096".to_string()],
        "one push of 4,097 distinct buckets stores ONE row capped at 4,096"
    );

    drop_live_db(&db).await;
}

/// A landing block still queued when the drain begins is sent while the grace
/// lasts. On this single-threaded runtime no worker runs between the admission
/// and the announcement, so the block is provably queued at it.
#[tokio::test]
async fn a_trace_block_queued_when_the_drain_begins_is_sent_inside_the_grace() {
    let root = spool_root("queued-at-drain");
    let landing: Arc<MockInserter<TraceLandingRow>> = MockInserter::always(Act::Ok);
    let writer = writer_with(&WriterConfig::default(), &root, landing.clone());

    let landed = decode(&six_spans());
    writer
        .admit(landed, PushHeaders::default())
        .expect("queue has room");
    writer.shutdown(Duration::from_secs(10)).await;

    assert_eq!(landing.call_count(), 1, "the queued landing block is sent");
    let spooled = std::fs::read_dir(root.join("poison").join(LANDING))
        .map(|d| d.count())
        .unwrap_or(0);
    assert_eq!(spooled, 0, "nothing is filed as never sent");
    std::fs::remove_dir_all(&root).ok();
}

/// **A retry after a server-declared failure is stored once the fault
/// clears** (issue #602), on one node. A view that throws fails the landing
/// insert after the block was sent — an uncertain ending — and may leave
/// some targets committed and others empty. The trace claim is
/// replay-safe, so the same push sent again in the same process is a new
/// block, and once the throwing view is gone every target holds the push.
#[tokio::test]
async fn a_retry_after_a_throwing_view_is_stored_once_the_fault_clears() {
    skip_unless_live!();
    let (client, db) = live_db(pulsus_testkit::test_db(
        "pulsus_trace_landing_it_retry_throw",
    ))
    .await;
    for sql in [
        "CREATE TABLE zz_fault (x UInt8) ENGINE = MergeTree ORDER BY x",
        "CREATE MATERIALIZED VIEW zz_fault_mv TO zz_fault AS \
         SELECT throwIf(row_kind = 0, 'injected fault') AS x FROM trace_landing",
    ] {
        client
            .execute(sql, &QuerySettings::new(), Idempotency::NonIdempotent)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let writer_client = Arc::new(
        ChClient::new(ChConnConfig {
            database: db.clone(),
            ..live_config()
        })
        .await
        .expect("connect the writer"),
    );
    let cfg = WriterConfig {
        trace_landing_retries: 0,
        ..Default::default()
    };
    let writer =
        TraceWriter::new_with_tables(writer_client, &cfg, TraceWriterTables::traces_default());
    let req = six_spans();

    let first = writer
        .admit_flush(decode(&req), PushHeaders::default())
        .expect("admitted")
        .await;
    assert!(
        first.is_err(),
        "the throwing view fails the push: {first:?}"
    );

    client
        .execute(
            "DROP VIEW zz_fault_mv",
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("clear the fault");
    let second = writer
        .admit_flush(decode(&req), PushHeaders::default())
        .expect("the retry is admitted, not suppressed")
        .await;
    assert!(
        second.is_ok(),
        "the retry commits once the fault is gone: {second:?}"
    );

    assert_eq!(
        live_count(&client, "SELECT count() AS n FROM spans FINAL").await,
        6
    );
    assert_eq!(
        live_count(&client, "SELECT count() AS n FROM traces FINAL").await,
        1
    );
    assert_eq!(writer.metrics().dedup.duplicate_pushes_total, 0);
    writer.shutdown(Duration::from_secs(5)).await;
    drop_live_db(&db).await;
}
