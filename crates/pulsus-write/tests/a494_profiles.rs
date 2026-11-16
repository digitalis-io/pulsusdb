//! Issue #494 for profiles: a push the profile writer has already accepted,
//! and still remembers, is not stored twice by that writer. The suppression
//! is checked before the insert, as for logs and traces. Against a mock
//! inserter, no ClickHouse.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use pulsus_clickhouse::{ChError, QuerySettings};
use pulsus_config::WriterConfig;
use pulsus_model::Tenant;
use pulsus_profiles_wire::google_v1::{Function, Line, Location, Profile, Sample, ValueType};
use pulsus_write::writer::{
    BlockInserter, ProfileLandingRow, ProfileWriter, ProfileWriterTables, WriterRuntime,
};
use pulsus_write::{ParsedProfileLanding, ProfileInput, ProfileSink, PushHeaders, parse_profiles};

// -- the mock inserter ------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Act {
    Ok,
    /// A non-retryable error: provably not committed.
    Poison,
}

/// A scripted mock: the script is consumed front to back and its last step
/// repeats. Counts its calls.
struct MockInserter {
    script: Vec<Act>,
    calls: AtomicUsize,
}

impl MockInserter {
    fn new(script: Vec<Act>) -> Arc<Self> {
        Arc::new(MockInserter {
            script,
            calls: AtomicUsize::new(0),
        })
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl BlockInserter<ProfileLandingRow> for MockInserter {
    fn insert<'a>(
        &'a self,
        table: &'a str,
        rows: &'a [ProfileLandingRow],
    ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>> {
        static EMPTY: std::sync::OnceLock<QuerySettings> = std::sync::OnceLock::new();
        self.insert_with(table, rows, EMPTY.get_or_init(QuerySettings::new))
    }

    fn insert_with<'a>(
        &'a self,
        _table: &'a str,
        _rows: &'a [ProfileLandingRow],
        _extra: &'a QuerySettings,
    ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let act = *self
            .script
            .get(call)
            .or(self.script.last())
            .expect("a non-empty script");
        Box::pin(async move {
            match act {
                Act::Ok => Ok(()),
                Act::Poison => Err(ChError::Decode("mock poison".to_string())),
            }
        })
    }
}

// -- the fixture ------------------------------------------------------

fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .expect("a representable stamp")
}

/// Part A §0's profile: two sample types, locations `main`, `a`, `b`, stacks
/// `main→a` and `main→b`.
fn example_profile(time_nanos: i64) -> Profile {
    let st = [
        "",
        "samples",
        "count",
        "cpu",
        "nanoseconds",
        "main",
        "a",
        "b",
        "main.go",
    ];
    Profile {
        sample_type: vec![
            ValueType { r#type: 1, unit: 2 },
            ValueType { r#type: 3, unit: 4 },
        ],
        sample: vec![
            Sample {
                location_id: vec![2, 1],
                value: vec![3, 30_000_000],
                label: Vec::new(),
            },
            Sample {
                location_id: vec![3, 1],
                value: vec![1, 10_000_000],
                label: Vec::new(),
            },
        ],
        function: (1..=3u64)
            .map(|id| Function {
                id,
                name: 4 + id as i64,
                system_name: 0,
                filename: 8,
                start_line: 1,
            })
            .collect(),
        location: (1..=3u64)
            .map(|id| Location {
                id,
                mapping_id: 0,
                address: 0,
                line: vec![Line {
                    function_id: id,
                    line: 10,
                }],
                is_folded: false,
            })
            .collect(),
        string_table: st.iter().map(|s| (*s).to_owned()).collect(),
        time_nanos,
        duration_nanos: 10_000_000_000,
        period_type: Some(ValueType { r#type: 3, unit: 4 }),
        period: 10_000_000,
        ..Profile::default()
    }
}

fn input(time_nanos: i64) -> ProfileInput {
    ProfileInput {
        labels: vec![
            ("__name__".to_owned(), "process_cpu".to_owned()),
            ("service_name".to_owned(), "checkout".to_owned()),
        ],
        profile: example_profile(time_nanos),
        decompressed_bytes: 1_000,
        id: None,
        annotations: Vec::new(),
    }
}

/// §0's push P, parsed once at `now_ms`.
fn push_p() -> ParsedProfileLanding {
    let now = now_ms();
    parse_profiles(vec![input(now * 1_000_000)], now).expect("the push is accepted")
}

fn tenant(name: &str) -> Tenant {
    Tenant::from_header(
        Some(&axum::http::HeaderValue::from_str(name).expect("a header value")),
        false,
    )
    .expect("a valid tenant")
}

fn spool_root(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pulsus-a494-profiles-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("create the spool root");
    dir
}

/// Suppression on (the default), every block sent alone, no resend.
fn writer(root: &Path, inserter: Arc<MockInserter>) -> ProfileWriter {
    let cfg = WriterConfig {
        landing_batch_ms: 0,
        profile_landing_retries: 0,
        ..WriterConfig::default()
    };
    let mut runtime = WriterRuntime::from_config(&cfg);
    runtime.spool_dir = root.to_path_buf();
    ProfileWriter::with_inserters_and_runtime(
        inserter,
        runtime,
        ProfileWriterTables::profiles_default(),
    )
}

/// `true` when the push is admitted and answered `Ok`.
async fn sync_push(writer: &ProfileWriter, who: &Tenant, landing: ParsedProfileLanding) -> bool {
    match writer.admit_flush(who, landing, PushHeaders::default()) {
        Ok(wait) => wait.await.is_ok(),
        Err(_) => false,
    }
}

/// Runs `pushes` in order through a fresh writer over `script`, and returns
/// each answer and the insert count.
async fn run(
    name: &str,
    script: Vec<Act>,
    pushes: Vec<(Tenant, ParsedProfileLanding)>,
) -> (Vec<bool>, usize) {
    let root = spool_root(name);
    let inserter = MockInserter::new(script);
    let w = writer(&root, inserter.clone());
    let mut answers = Vec::new();
    for (who, landing) in pushes {
        answers.push(sync_push(&w, &who, landing).await);
    }
    w.shutdown(Duration::from_secs(2)).await;
    std::fs::remove_dir_all(&root).ok();
    (answers, inserter.call_count())
}

/// **D1.** The same push twice from one tenant inside the window is one
/// insert, and both are answered `Ok`.
#[tokio::test]
async fn an_identical_push_inside_the_window_is_one_insert() {
    let p = push_p();
    let (answers, calls) = run(
        "d1",
        vec![Act::Ok],
        vec![(tenant("t1"), p.clone()), (tenant("t1"), p)],
    )
    .await;
    assert_eq!(answers, vec![true, true], "both answered Ok");
    assert_eq!(calls, 1, "the repeat is suppressed before the insert");
}

/// **D2.** The same push under another tenant is another push.
#[tokio::test]
async fn the_same_push_under_another_tenant_is_a_second_insert() {
    let p = push_p();
    let (answers, calls) = run(
        "d2",
        vec![Act::Ok],
        vec![(tenant("t1"), p.clone()), (tenant("t2"), p)],
    )
    .await;
    assert_eq!(answers, vec![true, true]);
    assert_eq!(calls, 2, "suppression is keyed by tenant");
}

/// **D3.** A client resending a push with no profile id and no time is
/// suppressed: the id the writer mints and the time defaulted from the
/// receive time are both left out of the digest.
#[tokio::test]
async fn a_resent_push_with_no_id_and_no_time_is_suppressed() {
    let now = now_ms();
    let first = parse_profiles(vec![input(0)], now).expect("the push is accepted");
    let again = parse_profiles(vec![input(0)], now + 1_000).expect("the push is accepted");
    assert!(first.series.iter().all(|s| s.time_defaulted));
    assert_ne!(
        first.series[0].timestamp_ns, again.series[0].timestamp_ns,
        "the defaulted times differ"
    );
    let (answers, calls) = run(
        "d3",
        vec![Act::Ok],
        vec![(tenant("t1"), first), (tenant("t1"), again)],
    )
    .await;
    assert_eq!(answers, vec![true, true]);
    assert_eq!(calls, 1, "the resend is suppressed");
}

/// **D4.** A push differing from an earlier one in one sample value is
/// stored.
#[tokio::test]
async fn a_push_differing_in_one_value_is_stored() {
    let p = push_p();
    let mut q = p.clone();
    q.samples[0].value += 1;
    let (answers, calls) = run(
        "d4",
        vec![Act::Ok],
        vec![(tenant("t1"), p), (tenant("t1"), q)],
    )
    .await;
    assert_eq!(answers, vec![true, true]);
    assert_eq!(calls, 2, "the digest covers every value");
}

/// **D5.** A push whose insert was refused is not remembered: the client's
/// retry is stored.
#[tokio::test]
async fn a_retry_after_a_failed_insert_is_stored() {
    let p = push_p();
    let (answers, calls) = run(
        "d5",
        vec![Act::Poison, Act::Ok],
        vec![(tenant("t1"), p.clone()), (tenant("t1"), p)],
    )
    .await;
    assert_eq!(
        answers,
        vec![false, true],
        "the refusal, then the retry's commit"
    );
    assert_eq!(calls, 2, "the retry reaches the insert");
}
