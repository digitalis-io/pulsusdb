//! Issue #494 part 2: how a landing worker forms a batch of log or trace
//! blocks, and how a batch runs to one ending for all its members.
//!
//! Every case drives `spawn_landing_worker` itself, over an inserter that
//! records each call and answers from a script, a spool gate that records
//! each write instead of making a file, and claim tickets that record what
//! they report. Times are milliseconds from the case's own origin.

use std::collections::HashMap;
use std::sync::Mutex;

use super::*;
use crate::ingest::metrics::MetricMetadata;
use crate::ingest::traces::{LandingTagName, TagScope};
use crate::writer::push_dedup::ClaimRecorder;
use crate::writer::rows::{LogLandingRow, MetricLandingRow, TraceLandingRow};
use crate::writer::spool::SpoolGate;
use tokio::time::Instant;

/// A landing row a case can tag with the member it belongs to.
trait TestRow: ChRow + SpoolEncode + LandingRow + Clone + Send + Sync + 'static {
    const TABLE: &'static str;
    /// Whether an uncertain ending releases the claim (traces) or confirms
    /// it failed (logs).
    const REPLAY_SAFE: bool;
    fn batch_settings() -> Option<fn(&str, u64) -> QuerySettings>;
    fn tagged(member: u32, row: u32) -> Self;
    fn member(&self) -> u32;
}

fn org() -> Arc<str> {
    Arc::from("tenant")
}

/// Every member's rows carry their own receive stamp, so a batch's re-stamp
/// is visible.
fn stamp_of(member: u32) -> i64 {
    1_700_000_000_000 + i64::from(member)
}

fn tag_of(text: &str) -> u32 {
    text.split('/')
        .next()
        .and_then(|m| m.parse().ok())
        .expect("a tagged row")
}

impl TestRow for LogLandingRow {
    const TABLE: &'static str = "log_landing";
    const REPLAY_SAFE: bool = false;
    fn batch_settings() -> Option<fn(&str, u64) -> QuerySettings> {
        Some(QuerySettings::landing_insert)
    }
    fn tagged(member: u32, row: u32) -> Self {
        LogLandingRow::line(
            &org(),
            stamp_of(member),
            &super::tests::log_row(format!("{member}/{row}")),
        )
    }
    fn member(&self) -> u32 {
        tag_of(&self.body)
    }
}

impl TestRow for TraceLandingRow {
    const TABLE: &'static str = "trace_landing";
    const REPLAY_SAFE: bool = true;
    fn batch_settings() -> Option<fn(&str, u64) -> QuerySettings> {
        Some(QuerySettings::trace_landing_insert)
    }
    fn tagged(member: u32, row: u32) -> Self {
        TraceLandingRow::tag_name(
            &org(),
            stamp_of(member),
            LandingTagName {
                scope: TagScope::Span,
                key: format!("{member}/{row}"),
                last_start_ns: 0,
            },
        )
    }
    fn member(&self) -> u32 {
        tag_of(&self.tag_key)
    }
}

impl TestRow for MetricLandingRow {
    const TABLE: &'static str = "metric_landing";
    const REPLAY_SAFE: bool = true;
    fn batch_settings() -> Option<fn(&str, u64) -> QuerySettings> {
        None
    }
    fn tagged(member: u32, row: u32) -> Self {
        MetricLandingRow::metadata(
            &org(),
            stamp_of(member),
            &MetricMetadata {
                metric_name: Arc::from(format!("{member}/{row}")),
                metric_type: "gauge".to_string(),
                help: String::new(),
                unit: String::new(),
                updated_ns: 0,
            },
        )
    }
    fn member(&self) -> u32 {
        tag_of(&self.metric_name)
    }
}

/// What one scripted attempt answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Ok,
    Uncertain,
    Poison,
}

fn uncertain_msg(k: usize) -> String {
    format!("uncertain attempt {k}")
}

fn poison_err(k: usize) -> ChError {
    ChError::Config(format!("poison attempt {k}"))
}

/// One call the inserter took.
#[derive(Debug, Clone)]
struct Call<R> {
    at: Instant,
    token: String,
    rows: Vec<R>,
}

/// Records every call, and answers attempt `k` of a batch (counted per
/// token) with `script[min(k, len - 1)]` after its latency; an empty script
/// answers `Ok` at once. With `hold_first`, the first call waits for a
/// permit before it answers.
struct ScriptedInserter<R> {
    calls: Mutex<Vec<Call<R>>>,
    script: Vec<(Outcome, Duration)>,
    tries: Mutex<HashMap<String, usize>>,
    hold_first: Option<tokio::sync::Semaphore>,
}

impl<R> ScriptedInserter<R> {
    fn new(script: Vec<(Outcome, Duration)>) -> Self {
        ScriptedInserter {
            calls: Mutex::new(Vec::new()),
            script,
            tries: Mutex::new(HashMap::new()),
            hold_first: None,
        }
    }

    fn calls(&self) -> Vec<Call<R>>
    where
        R: Clone,
    {
        self.calls.lock().expect("calls").clone()
    }
}

impl<R: TestRow> BlockInserter<R> for ScriptedInserter<R> {
    fn insert<'a>(
        &'a self,
        table: &'a str,
        rows: &'a [R],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ChError>> + Send + 'a>> {
        static NONE: std::sync::OnceLock<QuerySettings> = std::sync::OnceLock::new();
        self.insert_with(table, rows, NONE.get_or_init(QuerySettings::new))
    }

    fn insert_with<'a>(
        &'a self,
        _table: &'a str,
        rows: &'a [R],
        extra: &'a QuerySettings,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ChError>> + Send + 'a>> {
        let token = extra
            .get("insert_deduplication_token")
            .unwrap_or("")
            .to_string();
        let first = {
            let mut calls = self.calls.lock().expect("calls");
            calls.push(Call {
                at: Instant::now(),
                token: token.clone(),
                rows: rows.to_vec(),
            });
            calls.len() == 1
        };
        let k = {
            let mut tries = self.tries.lock().expect("tries");
            let k = tries.entry(token).or_insert(0);
            *k += 1;
            *k - 1
        };
        let answer = if self.script.is_empty() {
            (Outcome::Ok, Duration::ZERO)
        } else {
            self.script[k.min(self.script.len() - 1)]
        };
        Box::pin(async move {
            if first && let Some(hold) = &self.hold_first {
                hold.acquire().await.expect("held").forget();
            }
            tokio::time::sleep(answer.1).await;
            match answer.0 {
                Outcome::Ok => Ok(()),
                Outcome::Uncertain => Err(ChError::InsertUncertain(uncertain_msg(k))),
                Outcome::Poison => Err(poison_err(k)),
            }
        })
    }
}

/// How a member's waiter was answered: `Ok`, or the error's kind and text.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Answer {
    Ok,
    Uncertain(String),
    Poisoned(String),
    ShuttingDown,
}

fn answer_of(r: Result<(), WriteError>) -> Answer {
    match r {
        Ok(()) => Answer::Ok,
        Err(WriteError::Uncertain(m)) => Answer::Uncertain(m),
        Err(WriteError::Poisoned(m)) => Answer::Poisoned(m),
        Err(WriteError::ShuttingDown) => Answer::ShuttingDown,
    }
}

/// What a case sets.
#[derive(Clone)]
struct Opts {
    window_ms: u64,
    workers: usize,
    retries: u32,
    max_rows: u64,
    batch_bytes: u64,
    lapse: Duration,
    backoff: Duration,
    script: Vec<(Outcome, Duration)>,
    /// A metrics context: no batch settings.
    metrics: bool,
    park_spool: bool,
    hold_first: bool,
}

impl Default for Opts {
    fn default() -> Self {
        Opts {
            window_ms: 1_000,
            workers: 1,
            retries: 0,
            max_rows: 1_000_000,
            batch_bytes: u64::MAX / 4,
            lapse: Duration::ZERO,
            backoff: Duration::ZERO,
            script: Vec::new(),
            metrics: false,
            park_spool: false,
            hold_first: false,
        }
    }
}

/// One case's worker pool and what it records.
struct Rig<R> {
    ctx: Arc<LandingContext<R>>,
    inserter: Arc<ScriptedInserter<R>>,
    gate: Arc<SpoolGate>,
    claims: ClaimRecorder,
    answers: Arc<Mutex<Vec<(u32, Instant, Answer)>>>,
    hooks: Arc<Mutex<Vec<Vec<Vec<R>>>>>,
    tx: Option<mpsc::UnboundedSender<LandingBlock<R>>>,
    boundary: Option<crate::writer::drain::DrainBoundary>,
    workers: Vec<tokio::task::JoinHandle<()>>,
    observers: Vec<tokio::task::JoinHandle<()>>,
    t0: Instant,
}

impl<R: TestRow> Rig<R> {
    fn new(opts: &Opts) -> Self {
        let config = pulsus_config::WriterConfig {
            landing_batch_ms: opts.window_ms,
            ..pulsus_config::WriterConfig::default()
        };
        let mut runtime = WriterRuntime::from_config(&config);
        runtime.landing_batch = Duration::from_millis(opts.window_ms);
        runtime.batch_bytes = opts.batch_bytes;
        runtime.test_lapse = opts.lapse;
        runtime.test_backoff = Some(opts.backoff);
        runtime.spool_dir = std::env::temp_dir().join("pulsus-landing-batch-unused");
        let spool_dir = runtime.spool_dir.clone();
        let gate = Arc::new(if opts.park_spool {
            SpoolGate::parked()
        } else {
            SpoolGate::default()
        });
        let metrics = Arc::new(crate::writer::metrics::WriterMetrics::default());
        let mut inserter = ScriptedInserter::new(opts.script.clone());
        if opts.hold_first {
            inserter.hold_first = Some(tokio::sync::Semaphore::new(0));
        }
        let inserter = Arc::new(inserter);
        let hooks: Arc<Mutex<Vec<Vec<Vec<R>>>>> = Arc::new(Mutex::new(Vec::new()));
        let hook: CommitHook<R> = {
            let hooks = hooks.clone();
            Arc::new(move |parts: &[&[R]]| {
                hooks
                    .lock()
                    .expect("hooks")
                    .push(parts.iter().map(|p| p.to_vec()).collect());
            })
        };
        let ctx = Arc::new(LandingContext {
            table: Arc::from(R::TABLE),
            inserter: inserter.clone(),
            runtime: Arc::new(runtime),
            table_metrics: Arc::new(TableMetrics::default()),
            queued_bytes: Arc::new(AtomicU64::new(0)),
            spool: Arc::new(SpoolWriter::new(spool_dir, metrics).with_gate(gate.clone())),
            on_commit: Some(hook),
            retries: opts.retries,
            max_rows: opts.max_rows,
            batch_settings: if opts.metrics {
                None
            } else {
                R::batch_settings()
            },
            token_rng: std::sync::Mutex::new(XorShift64::from_seed(17)),
        });
        let (tx, rx) = mpsc::unbounded_channel();
        let queue = Arc::new(tokio::sync::Mutex::new(LandingQueue::new(rx)));
        let boundary = crate::writer::drain::DrainBoundary::new();
        let workers = (0..opts.workers)
            .map(|_| spawn_landing_worker(ctx.clone(), queue.clone(), boundary.watch()))
            .collect();
        Rig {
            ctx,
            inserter,
            gate,
            claims: Arc::new(Mutex::new(Vec::new())),
            answers: Arc::new(Mutex::new(Vec::new())),
            hooks,
            tx: Some(tx),
            boundary: Some(boundary),
            workers,
            observers: Vec::new(),
            t0: Instant::now(),
        }
    }

    fn ms(&self, at: Instant) -> i64 {
        if at >= self.t0 {
            at.duration_since(self.t0).as_millis() as i64
        } else {
            -(self.t0.duration_since(at).as_millis() as i64)
        }
    }

    fn at(&self, ms: i64) -> Instant {
        if ms >= 0 {
            self.t0 + Duration::from_millis(ms as u64)
        } else {
            self.t0 - Duration::from_millis(ms.unsigned_abs())
        }
    }

    /// Sleeps until `ms` from the origin.
    async fn until(&self, ms: i64) {
        tokio::time::sleep_until(self.at(ms)).await;
    }

    /// Queues member `member`: `rows` rows, a charge of `bytes`, admitted at
    /// `admitted_ms`, with `span`, a recorded claim and a sync waiter.
    fn push(
        &mut self,
        member: u32,
        rows: u32,
        bytes: u64,
        admitted_ms: i64,
        span: Option<(i64, i64)>,
    ) {
        let claim = ClaimTicket::inert().recorded(u64::from(member), self.claims.clone());
        let claim = if R::REPLAY_SAFE {
            claim.replay_safe()
        } else {
            claim
        };
        self.push_with_claim(member, rows, bytes, admitted_ms, span, claim);
    }

    fn push_with_claim(
        &mut self,
        member: u32,
        rows: u32,
        bytes: u64,
        admitted_ms: i64,
        span: Option<(i64, i64)>,
        claim: ClaimTicket,
    ) {
        let (wtx, wrx) = oneshot::channel();
        self.ctx.queued_bytes.fetch_add(bytes, Ordering::AcqRel);
        let block = LandingBlock::seal(
            (0..rows).map(|i| R::tagged(member, i)).collect(),
            bytes,
            QuerySettings::new(),
            claim,
            Some(wtx),
            self.at(admitted_ms),
            span,
        );
        self.tx
            .as_ref()
            .expect("the queue is open")
            .send(block)
            .unwrap_or_else(|_| panic!("the queue is open"));
        let answers = self.answers.clone();
        self.observers.push(tokio::spawn(async move {
            let answer = match wrx.await {
                Ok(r) => answer_of(r),
                Err(_) => panic!("member {member}'s waiter was dropped"),
            };
            answers
                .lock()
                .expect("answers")
                .push((member, Instant::now(), answer));
        }));
    }

    /// Waits for every member's answer, then closes the queue and joins the
    /// workers.
    async fn finish(&mut self) {
        for o in std::mem::take(&mut self.observers) {
            o.await.expect("an observer");
        }
        drop(self.tx.take());
        for w in std::mem::take(&mut self.workers) {
            w.await.expect("a worker");
        }
        drop(self.boundary.take());
    }

    /// Each call as (issued ms, token, members in row order, with one entry
    /// per member).
    fn inserts(&self) -> Vec<(i64, String, Vec<u32>)> {
        self.inserter
            .calls()
            .into_iter()
            .map(|c| {
                let mut members: Vec<u32> = Vec::new();
                for row in &c.rows {
                    if members.last() != Some(&row.member()) {
                        members.push(row.member());
                    }
                }
                (self.ms(c.at), c.token, members)
            })
            .collect()
    }

    fn answers(&self) -> Vec<(u32, i64, Answer)> {
        let mut out: Vec<_> = self
            .answers
            .lock()
            .expect("answers")
            .iter()
            .map(|(m, at, a)| (*m, self.ms(*at), a.clone()))
            .collect();
        out.sort_by_key(|(m, _, _)| *m);
        out
    }

    fn claims(&self) -> Vec<(u32, i64, TargetOutcome)> {
        let mut out: Vec<_> = self
            .claims
            .lock()
            .expect("claims")
            .iter()
            .map(|(m, o, at)| (*m as u32, self.ms(*at), *o))
            .collect();
        out.sort_by_key(|(m, _, _)| *m);
        out
    }
}

/// The member lists of `inserts`, in call order.
fn memberships(inserts: &[(i64, String, Vec<u32>)]) -> Vec<Vec<u32>> {
    inserts.iter().map(|(_, _, m)| m.clone()).collect()
}

/// Advances the paused clock in 1 ms steps until `done` holds, for at most
/// `limit_ms`; returns whether it held.
async fn settle_until(limit_ms: u64, mut done: impl FnMut() -> bool) -> bool {
    for _ in 0..limit_ms {
        if done() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    done()
}

/// The case's origin, far enough past the runtime's own start that a block
/// admitted a whole landing budget before it is still a valid instant.
async fn past_origin() {
    tokio::time::advance(Duration::from_secs(300)).await;
}

// ---------------------------------------------------------------------------
// 2-U1: the window runs from the worker's first take.
// ---------------------------------------------------------------------------

/// The plan's "0 ms" is `o`, the instant the earlier block's insert is
/// issued: its own window after it is queued.
async fn window_case(window_ms: u64) -> (i64, Vec<(i64, String, Vec<u32>)>) {
    past_origin().await;
    let opts = Opts {
        window_ms,
        hold_first: true,
        ..Opts::default()
    };
    let mut rig = Rig::<LogLandingRow>::new(&opts);
    let o = window_ms as i64;
    // The earlier block: its insert is held until 300 ms, so the worker is
    // busy when the first block is admitted at 0 ms.
    rig.push(9, 1, 10, 0, None);
    rig.until(o).await;
    rig.push(1, 1, 10, o, None);
    rig.until(o + 300).await;
    rig.inserter
        .hold_first
        .as_ref()
        .expect("held")
        .add_permits(1);
    rig.until(o + 1_299).await;
    rig.push(2, 1, 10, o + 1_299, None);
    rig.until(o + 1_300).await;
    rig.push(3, 1, 10, o + 1_300, None);
    rig.finish().await;
    (o, rig.inserts())
}

/// **2-U1.** The window runs from the worker's first take, not from the
/// admission; with window 0 every block is its own insert; and a metrics
/// context sends every block at once whatever the window.
#[tokio::test(start_paused = true)]
async fn u1_the_window_runs_from_the_first_take() {
    let (o, inserts) = window_case(1_000).await;
    assert_eq!(
        memberships(&inserts),
        vec![vec![9], vec![1, 2], vec![3]],
        "the earlier block, then [first, 1299], then [1300]: {inserts:?}"
    );
    assert_eq!(
        inserts[1].0 - o,
        1_300,
        "[first, 1299] is issued at 1300 ms"
    );
    assert_ne!(inserts[1].1, "", "a batch carries a token");
    assert_ne!(inserts[1].1, inserts[2].1, "two batches, two tokens");

    let (_, inserts) = window_case(0).await;
    assert_eq!(
        memberships(&inserts),
        vec![vec![9], vec![1], vec![2], vec![3]],
        "window 0: one insert per block: {inserts:?}"
    );

    let opts = Opts {
        window_ms: 1_000,
        metrics: true,
        ..Opts::default()
    };
    let mut rig = Rig::<MetricLandingRow>::new(&opts);
    rig.push(1, 1, 10, 0, None);
    rig.until(5).await;
    rig.push(2, 1, 10, 5, None);
    rig.finish().await;
    let inserts = rig.inserts();
    assert_eq!(
        inserts
            .iter()
            .map(|(at, _, m)| (*at, m.clone()))
            .collect::<Vec<_>>(),
        vec![(0, vec![1]), (5, vec![2])],
        "metrics are never batched"
    );
}

// ---------------------------------------------------------------------------
// 2-U2, 2-U3, 2-U4: the caps.
// ---------------------------------------------------------------------------

/// One queued block of a backlog: member, rows, charge and date span.
type Backlogged = (u32, u32, u64, Option<(i64, i64)>);

/// Queues `blocks` (member, rows, bytes, span) at once, before the worker
/// takes any, and returns the inserts.
async fn backlog_case(opts: Opts, blocks: &[Backlogged]) -> Vec<(i64, String, Vec<u32>)> {
    past_origin().await;
    let mut rig = Rig::<LogLandingRow>::new(&opts);
    for &(member, rows, bytes, span) in blocks {
        rig.push(member, rows, bytes, 0, span);
    }
    rig.finish().await;
    rig.inserts()
}

#[tokio::test(start_paused = true)]
async fn u2_a_batch_stays_under_the_row_limit() {
    let opts = Opts {
        max_rows: 1_000,
        ..Opts::default()
    };
    let inserts = backlog_case(
        opts.clone(),
        &[(1, 600, 1, None), (2, 300, 1, None), (3, 200, 1, None)],
    )
    .await;
    assert_eq!(
        memberships(&inserts),
        vec![vec![1, 2], vec![3]],
        "{inserts:?}"
    );
    assert_eq!(inserts[0].0, 0, "a batch at its cap is sent at once");
    let inserts = backlog_case(
        opts.clone(),
        &[(1, 600, 1, None), (2, 300, 1, None), (3, 99, 1, None)],
    )
    .await;
    assert_eq!(
        memberships(&inserts),
        vec![vec![1, 2, 3]],
        "999 rows join: {inserts:?}"
    );
    let inserts = backlog_case(
        opts,
        &[(1, 600, 1, None), (2, 300, 1, None), (3, 100, 1, None)],
    )
    .await;
    assert_eq!(
        memberships(&inserts),
        vec![vec![1, 2], vec![3]],
        "1000 rows do not: {inserts:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn u3_a_batch_spans_at_most_one_hundred_days() {
    let inserts = backlog_case(
        Opts::default(),
        &[
            (1, 1, 1, Some((1, 60))),
            (2, 1, 1, Some((61, 120))),
            (3, 1, 1, Some((61, 61))),
        ],
    )
    .await;
    assert_eq!(
        memberships(&inserts),
        vec![vec![1], vec![2, 3]],
        "{inserts:?}"
    );
    let inserts = backlog_case(
        Opts::default(),
        &[(1, 1, 1, Some((1, 99))), (2, 1, 1, Some((100, 100)))],
    )
    .await;
    assert_eq!(
        memberships(&inserts),
        vec![vec![1, 2]],
        "100 days join: {inserts:?}"
    );
    let inserts = backlog_case(
        Opts::default(),
        &[(1, 1, 1, Some((1, 100))), (2, 1, 1, Some((101, 101)))],
    )
    .await;
    assert_eq!(
        memberships(&inserts),
        vec![vec![1], vec![2]],
        "101 days do not: {inserts:?}"
    );
    let inserts = backlog_case(
        Opts::default(),
        &[(1, 1, 1, Some((1, 201))), (2, 1, 1, Some((1, 1)))],
    )
    .await;
    assert_eq!(
        memberships(&inserts),
        vec![vec![1], vec![2]],
        "the sparse block goes alone: {inserts:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn u4_a_batch_stays_within_the_byte_limit() {
    let opts = Opts {
        batch_bytes: 1_000,
        ..Opts::default()
    };
    let inserts = backlog_case(opts.clone(), &[(1, 1, 600, None), (2, 1, 400, None)]).await;
    assert_eq!(
        memberships(&inserts),
        vec![vec![1, 2]],
        "a charge at the limit joins: {inserts:?}"
    );
    let inserts = backlog_case(opts, &[(1, 1, 600, None), (2, 1, 401, None)]).await;
    assert_eq!(
        memberships(&inserts),
        vec![vec![1], vec![2]],
        "one byte over does not: {inserts:?}"
    );
}

// ---------------------------------------------------------------------------
// 2-U5: the drain sends the forming batch at once.
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn u5_the_drain_sends_the_forming_batch_at_once() {
    past_origin().await;
    let mut rig = Rig::<LogLandingRow>::new(&Opts::default());
    rig.push(1, 1, 1, 0, None);
    rig.until(5).await;
    rig.push(2, 1, 1, 5, None);
    rig.until(10).await;
    rig.boundary
        .as_ref()
        .expect("the boundary")
        .shutdown(Duration::from_secs(60))
        .await;
    rig.finish().await;
    let inserts = rig.inserts();
    assert_eq!(
        inserts
            .iter()
            .map(|(at, _, m)| (*at, m.clone()))
            .collect::<Vec<_>>(),
        vec![(10, vec![1, 2])],
        "one insert of both, at the announcement"
    );
}

// ---------------------------------------------------------------------------
// 2-U6, 2-U7: one batch, one outcome, every member.
// ---------------------------------------------------------------------------

/// Three members queued at once; returns the rig after every answer.
async fn three_members<R: TestRow>(opts: Opts) -> Rig<R> {
    past_origin().await;
    let mut rig = Rig::<R>::new(&opts);
    for member in 1..=3 {
        rig.push(member, member, 100 + u64::from(member), 0, None);
    }
    rig.finish().await;
    rig
}

/// Every call holds all three members' rows, equal by value, under one
/// non-empty token.
fn assert_every_call_is_the_batch<R: TestRow + PartialEq + std::fmt::Debug>(
    rig: &Rig<R>,
    calls: usize,
) {
    let all = rig.inserter.calls();
    assert_eq!(all.len(), calls, "calls: {:?}", rig.inserts());
    let first = &all[0];
    let members: Vec<u32> = first.rows.iter().map(TestRow::member).collect();
    assert_eq!(
        members,
        vec![1, 2, 2, 3, 3, 3],
        "every member's rows, in order"
    );
    for call in &all {
        assert_eq!(call.rows, first.rows, "every attempt sends the same rows");
        assert_eq!(
            call.token, first.token,
            "every attempt carries the batch's token"
        );
    }
    assert!(!first.token.is_empty(), "the batch carries a token");
}

#[tokio::test(start_paused = true)]
async fn u6_an_uncertain_attempt_then_a_commit_settles_every_member_once() {
    let opts = Opts {
        retries: 3,
        script: vec![
            (Outcome::Uncertain, Duration::from_millis(5)),
            (Outcome::Ok, Duration::from_millis(5)),
        ],
        ..Opts::default()
    };
    let rig = three_members::<LogLandingRow>(opts).await;
    assert_every_call_is_the_batch(&rig, 2);
    assert_eq!(
        rig.claims()
            .iter()
            .map(|(m, _, o)| (*m, *o))
            .collect::<Vec<_>>(),
        vec![
            (1, TargetOutcome::Committed),
            (2, TargetOutcome::Committed),
            (3, TargetOutcome::Committed)
        ],
        "each claim settles once, committed"
    );
    assert_eq!(
        rig.answers()
            .iter()
            .map(|(m, _, a)| (*m, a.clone()))
            .collect::<Vec<_>>(),
        vec![(1, Answer::Ok), (2, Answer::Ok), (3, Answer::Ok)],
    );
    assert_eq!(
        rig.ctx.queued_bytes.load(Ordering::SeqCst),
        0,
        "every charge is released"
    );
}

async fn always_uncertain<R: TestRow + PartialEq + std::fmt::Debug>(released: TargetOutcome) {
    let opts = Opts {
        retries: 2,
        script: vec![(Outcome::Uncertain, Duration::from_millis(5))],
        ..Opts::default()
    };
    let rig = three_members::<R>(opts).await;
    assert_every_call_is_the_batch(&rig, 3);
    let mut files: Vec<(SpoolKind, usize)> = rig
        .gate
        .records
        .lock()
        .expect("records")
        .iter()
        .map(|r| (r.kind, r.rows))
        .collect();
    files.sort_by_key(|(_, rows)| *rows);
    assert_eq!(
        files,
        vec![
            (SpoolKind::Uncertain, 1),
            (SpoolKind::Uncertain, 2),
            (SpoolKind::Uncertain, 3)
        ],
        "one uncertain/ file per member"
    );
    let msg = uncertain_msg(2);
    assert_eq!(
        rig.answers()
            .iter()
            .map(|(m, _, a)| (*m, a.clone()))
            .collect::<Vec<_>>(),
        (1..=3)
            .map(|m| (m, Answer::Uncertain(msg.clone())))
            .collect::<Vec<_>>(),
    );
    assert_eq!(
        rig.claims()
            .iter()
            .map(|(m, _, o)| (*m, *o))
            .collect::<Vec<_>>(),
        (1..=3).map(|m| (m, released)).collect::<Vec<_>>(),
    );
}

#[tokio::test(start_paused = true)]
async fn u7_an_uncertain_log_batch_files_and_confirms_every_member() {
    always_uncertain::<LogLandingRow>(TargetOutcome::Uncertain).await;
}

#[tokio::test(start_paused = true)]
async fn u7_an_uncertain_trace_batch_files_and_releases_every_member() {
    always_uncertain::<TraceLandingRow>(TargetOutcome::NotCommitted).await;
}

// ---------------------------------------------------------------------------
// 2-U11: one commit, one flush, one hook.
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn u11_a_committed_batch_is_one_flush_and_one_hook_call() {
    past_origin().await;
    let mut rig = Rig::<LogLandingRow>::new(&Opts::default());
    rig.push(1, 1, 101, 0, None);
    rig.until(200).await;
    rig.push(2, 2, 102, 200, None);
    rig.until(900).await;
    rig.push(3, 3, 103, 900, None);
    rig.finish().await;
    let snap = rig.ctx.table_metrics.snapshot();
    assert_eq!(snap.flushes_total, 1, "one insert");
    assert_eq!(snap.rows_total, 6, "the members' rows summed");
    assert_eq!(snap.bytes_total, 306, "the members' bytes summed");
    assert_eq!(snap.flush_latency_count, 1);
    assert_eq!(
        snap.flush_latency_sum_ns, 1_000_000_000,
        "latency from the earliest admission"
    );
    let hooks = rig.hooks.lock().expect("hooks").clone();
    assert_eq!(hooks.len(), 1, "the hook runs once");
    assert_eq!(
        hooks[0]
            .iter()
            .map(|part| part.iter().map(TestRow::member).collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        vec![vec![1], vec![2, 2], vec![3, 3, 3]],
        "over every member's rows"
    );
}

// ---------------------------------------------------------------------------
// 2-U13: a batch in flight at the shutdown deadline.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn u13_a_batch_in_flight_at_the_shutdown_deadline_files_every_member() {
    let opts = Opts {
        window_ms: 50,
        script: vec![(Outcome::Ok, Duration::from_secs(3_600))],
        ..Opts::default()
    };
    let mut rig = Rig::<LogLandingRow>::new(&opts);
    for member in 1..=3 {
        rig.push(member, member, 10, 0, None);
    }
    let in_flight = settle_until(5_000, || !rig.inserter.calls().is_empty()).await;
    assert!(in_flight, "the batch's insert is in flight");
    // A moment for any later block's own insert to start, as on `main`.
    tokio::time::sleep(Duration::from_millis(200)).await;
    rig.boundary
        .as_ref()
        .expect("the boundary")
        .shutdown(Duration::from_millis(100))
        .await;
    rig.finish().await;
    assert_eq!(
        memberships(&rig.inserts()),
        vec![vec![1, 2, 3]],
        "the three members are one insert"
    );
    let mut files: Vec<(SpoolKind, usize)> = rig
        .gate
        .records
        .lock()
        .expect("records")
        .iter()
        .map(|r| (r.kind, r.rows))
        .collect();
    files.sort_by_key(|(_, rows)| *rows);
    assert_eq!(
        files,
        vec![
            (SpoolKind::Uncertain, 1),
            (SpoolKind::Uncertain, 2),
            (SpoolKind::Uncertain, 3)
        ],
        "three uncertain/ files"
    );
    assert_eq!(
        rig.answers()
            .iter()
            .map(|(m, _, a)| (*m, a.clone()))
            .collect::<Vec<_>>(),
        (1..=3)
            .map(|m| (m, Answer::ShuttingDown))
            .collect::<Vec<_>>(),
    );
}

// ---------------------------------------------------------------------------
// 2-U20: claims first, then at most four spool writes at once.
// ---------------------------------------------------------------------------

async fn claims_before_writes<R: TestRow>(script: Vec<(Outcome, Duration)>, retries: u32) {
    use crate::writer::push_dedup::{Admission, PushDedup, PushDigest, PushIdentity, WaitMode};

    past_origin().await;
    let opts = Opts {
        retries,
        script,
        park_spool: true,
        ..Opts::default()
    };
    let mut rig = Rig::<R>::new(&opts);
    let dedup = PushDedup::new(
        16 * 1024 * 1024,
        Duration::from_secs(300),
        Duration::from_secs(120),
    );
    let identity = || PushIdentity {
        key: PushDigest::from_raw(10),
        content: PushDigest::from_raw(10),
        declared_retry: false,
    };
    for member in 1..=10u32 {
        let charge = 1_000 + u64::from(member);
        if member < 10 {
            rig.push(member, member, charge, 0, None);
            continue;
        }
        // Member 10 holds a claim in the index, so a client's retry of it
        // can be tried.
        let Admission::Admit(mut guard) = dedup.admit(identity(), WaitMode::Register) else {
            panic!("the first push is admitted");
        };
        let mut claim =
            ClaimTicket::new(Some(dedup.clone()), true).recorded(10, rig.claims.clone());
        claim.push(guard.key());
        guard.note_target(true);
        guard.seal();
        let claim = if R::REPLAY_SAFE {
            claim.replay_safe()
        } else {
            claim
        };
        rig.push_with_claim(member, member, charge, 0, None, claim);
    }
    let total: u64 = (1..=10).map(|m| 1_000 + m).sum();

    let claimed = settle_until(10_000, || rig.claims.lock().expect("claims").len() == 10).await;
    let ended = rig.gate.records.lock().expect("records").len();
    assert!(
        claimed && ended == 0,
        "all 10 claims settle before any write ends: {} claims, {ended} writes ended",
        rig.claims.lock().expect("claims").len()
    );
    assert_eq!(
        rig.claims()
            .iter()
            .map(|(m, _, o)| (*m, *o))
            .collect::<Vec<_>>(),
        (1..=10)
            .map(|m| (m, TargetOutcome::NotCommitted))
            .collect::<Vec<_>>(),
        "every claim is released"
    );
    assert!(
        matches!(dedup.admit(identity(), WaitMode::None), Admission::Admit(_)),
        "a client's retry of member 10 is admitted while the writes are open"
    );
    assert_eq!(
        rig.ctx.queued_bytes.load(Ordering::SeqCst),
        total,
        "no charge is released before its write ends"
    );

    let mut released = 0u64;
    for done in 1..=10usize {
        assert!(
            settle_until(100, || rig.gate.open.load(Ordering::SeqCst)
                == (10 - done + 1).min(4))
            .await
        );
        rig.gate.release(1);
        assert!(
            settle_until(100, || rig.gate.records.lock().expect("records").len()
                == done)
            .await
        );
        let rows = rig.gate.records.lock().expect("records")[done - 1].rows as u64;
        released += 1_000 + rows;
        assert!(
            settle_until(100, || rig.ctx.queued_bytes.load(Ordering::SeqCst)
                == total - released)
            .await,
            "the charge of member {rows} falls when its write ends: {} left, {} expected",
            rig.ctx.queued_bytes.load(Ordering::SeqCst),
            total - released
        );
    }
    rig.finish().await;
    assert_eq!(
        rig.gate.max_open.load(Ordering::SeqCst),
        4,
        "at most 4 writes at once"
    );
}

#[tokio::test(start_paused = true)]
async fn u20_a_poisoned_log_batch_reports_every_claim_before_any_write() {
    claims_before_writes::<LogLandingRow>(vec![(Outcome::Poison, Duration::from_millis(5))], 0)
        .await;
}

#[tokio::test(start_paused = true)]
async fn u20_an_uncertain_trace_batch_reports_every_claim_before_any_write() {
    claims_before_writes::<TraceLandingRow>(
        vec![(Outcome::Uncertain, Duration::from_millis(5))],
        2,
    )
    .await;
}

// ---------------------------------------------------------------------------
// 2-U12: the worker against the reference model of plan B §3.1.
// ---------------------------------------------------------------------------

const MODEL_QUEUED: &str = MSG_BUDGET_QUEUED;
const MODEL_BETWEEN: &str = MSG_BUDGET_BETWEEN;
const MODEL_IN_ATTEMPT: &str = MSG_BUDGET_IN_ATTEMPT;

/// One block of a case: its id, take and deadline, in ms.
#[derive(Debug, Clone, Copy)]
struct MBlock {
    id: u32,
    take: i64,
    deadline: i64,
}

/// One case.
#[derive(Debug, Clone)]
struct Case {
    seed: u64,
    trace: bool,
    window: i64,
    lapse: i64,
    backoff: i64,
    retries: u32,
    script: Vec<(Outcome, i64)>,
    blocks: Vec<MBlock>,
}

/// The fate, one way: never sent, then uncertain, never back.
#[derive(Debug, Clone, PartialEq, Eq)]
enum MFate {
    NeverSent(String),
    Uncertain(String),
}

impl MFate {
    fn pre_send(&mut self, msg: &str) {
        if let MFate::NeverSent(m) = self {
            *m = msg.to_string();
        }
    }
}

/// What the model says one block ends with.
#[derive(Debug, Clone, PartialEq, Eq)]
struct MSettled {
    id: u32,
    at: i64,
    answer: Answer,
    claim: TargetOutcome,
    /// The spool file's kind, if one is written.
    spool: Option<SpoolKind>,
}

/// What the model says one batch sent: its members and its attempts'
/// instants.
#[derive(Debug, Clone, PartialEq, Eq)]
struct MInsert {
    members: Vec<u32>,
    attempts: Vec<i64>,
}

/// Which listed situations a case reached.
#[derive(Debug, Default, Clone, Copy)]
struct Reached {
    deadline_in_lapse: bool,
    backoff_reaches_deadline: bool,
    uncertain_then_deadline_in_backoff: bool,
}

struct Model<'a> {
    case: &'a Case,
    inserts: Vec<MInsert>,
    settled: Vec<MSettled>,
    reached: Reached,
}

impl Model<'_> {
    fn settle(&mut self, members: &[MBlock], at: i64, fate: &MFate) {
        for b in members {
            let (answer, claim, spool) = match fate {
                MFate::NeverSent(m) => (
                    Answer::Poisoned(m.clone()),
                    TargetOutcome::NotCommitted,
                    SpoolKind::Poison,
                ),
                MFate::Uncertain(m) => (
                    Answer::Uncertain(m.clone()),
                    if self.case.trace {
                        TargetOutcome::NotCommitted
                    } else {
                        TargetOutcome::Uncertain
                    },
                    SpoolKind::Uncertain,
                ),
            };
            self.settled.push(MSettled {
                id: b.id,
                at,
                answer,
                claim,
                spool: Some(spool),
            });
        }
    }

    fn commit(&mut self, members: &[MBlock], at: i64) {
        for b in members {
            self.settled.push(MSettled {
                id: b.id,
                at,
                answer: Answer::Ok,
                claim: TargetOutcome::Committed,
                spool: None,
            });
        }
    }

    /// `send(batch, at=t)`.
    fn send(&mut self, batch: &[MBlock], t: i64) {
        let c = self.case;
        let d = batch.iter().map(|b| b.deadline).min().expect("a member");
        let (mut k, mut now, mut fate) = (0usize, t, MFate::NeverSent(String::new()));
        let mut attempts = Vec::new();
        let mut last_uncertain = false;
        loop {
            let ending = if k == 0 { MODEL_QUEUED } else { MODEL_BETWEEN };
            if now >= d {
                if k > 0 {
                    self.reached.backoff_reaches_deadline = true;
                    if last_uncertain {
                        self.reached.uncertain_then_deadline_in_backoff = true;
                    }
                }
                fate.pre_send(ending);
                self.settle(batch, d, &fate);
                break;
            }
            now = (now + c.lapse).min(d);
            if now >= d {
                self.reached.deadline_in_lapse = true;
                fate.pre_send(ending);
                self.settle(batch, d, &fate);
                break;
            }
            let (outcome, latency) = c.script[k.min(c.script.len() - 1)];
            attempts.push(now);
            if now + latency >= d {
                fate = MFate::Uncertain(MODEL_IN_ATTEMPT.to_string());
                self.settle(batch, d, &fate);
                break;
            }
            now += latency;
            match outcome {
                Outcome::Ok => {
                    self.commit(batch, now);
                    break;
                }
                Outcome::Uncertain => {
                    fate = MFate::Uncertain(uncertain_msg(k));
                    last_uncertain = true;
                }
                Outcome::Poison => {
                    fate.pre_send(&poison_err(k).to_string());
                    last_uncertain = false;
                }
            }
            if outcome == Outcome::Poison || k == c.retries as usize {
                self.settle(batch, now, &fate);
                break;
            }
            now = (now + c.backoff).min(d);
            k += 1;
        }
        self.inserts.push(MInsert {
            members: batch.iter().map(|b| b.id).collect(),
            attempts,
        });
    }

    fn run(case: &Case) -> Model<'_> {
        let mut m = Model {
            case,
            inserts: Vec::new(),
            settled: Vec::new(),
            reached: Reached::default(),
        };
        let w = case.window;
        let mut batch: Option<(i64, Vec<MBlock>)> = None;
        for &b in &case.blocks {
            if let Some((first_take, _)) = &batch
                && first_take + w <= b.take
            {
                let (first_take, members) = batch.take().expect("a batch");
                m.send(&members, first_take + w);
            }
            if b.deadline <= b.take {
                if let Some((_, members)) = batch.take() {
                    m.send(&members, b.take);
                }
                m.settle(&[b], b.take, &MFate::NeverSent(MODEL_QUEUED.to_string()));
                continue;
            }
            let (first_take, members) = batch.get_or_insert_with(|| (b.take, Vec::new()));
            members.push(b);
            let first_take = *first_take;
            if members.iter().map(|x| x.deadline).min().expect("a member") <= first_take + w {
                let (_, members) = batch.take().expect("a batch");
                m.send(&members, b.take);
            }
        }
        if let Some((first_take, members)) = batch.take() {
            m.send(&members, first_take + w);
        }
        m
    }
}

/// A case from `seed`: 1 to 6 blocks, takes in `[0, 3000)` ms, deadlines
/// before, at and 1 ms after the take, inside the window, at a close and
/// after it; `L` from 0, 5, 50 ms; `X` from 0, 30 ms; 0 to 3 retries;
/// scripts of 1 to 4 outcomes with latencies 1 to 400 ms.
fn generate(seed: u64) -> Case {
    let mut rng = XorShift64::from_seed(seed);
    let mut pick = |n: u64| rng.next_u64() % n;
    let window = 1_000;
    let n = 1 + pick(6) as usize;
    let mut takes: Vec<i64> = (0..n).map(|_| pick(3_000) as i64).collect();
    takes.sort_unstable();
    let blocks = takes
        .iter()
        .enumerate()
        .map(|(i, &take)| {
            // A close: the window's end from this take or an earlier one.
            let anchor = takes[pick(i as u64 + 1) as usize];
            let deadline = match pick(8) {
                0 => take - 50,
                1 => take,
                2 => take + 1,
                3 => take + 300,
                4 => anchor + window,
                5 => take + 1_500,
                6 => take + 1 + pick(window as u64) as i64,
                _ => take + 100_000,
            };
            MBlock {
                id: i as u32 + 1,
                take,
                deadline,
            }
        })
        .collect();
    let lapse = [0, 5, 50][pick(3) as usize];
    let backoff = [0, 30][pick(2) as usize];
    let retries = pick(4) as u32;
    let script = (0..1 + pick(4))
        .map(|_| {
            let outcome = match pick(3) {
                0 => Outcome::Ok,
                1 => Outcome::Uncertain,
                _ => Outcome::Poison,
            };
            (outcome, 1 + pick(400) as i64)
        })
        .collect();
    Case {
        seed,
        trace: seed % 2 == 1,
        window,
        lapse,
        backoff,
        retries,
        script,
        blocks,
    }
}

/// What the worker did with a case, in the model's terms.
struct Observed {
    inserts: Vec<MInsert>,
    tokens: Vec<String>,
    settled: Vec<MSettled>,
    claim_at: Vec<(u32, i64)>,
    spool: Vec<(u32, i64, SpoolKind)>,
}

async fn observe<R: TestRow>(case: &Case) -> Observed {
    let budget_ms = 120_000i64;
    let opts = Opts {
        window_ms: case.window as u64,
        workers: case.blocks.len() + 2,
        retries: case.retries,
        lapse: Duration::from_millis(case.lapse as u64),
        backoff: Duration::from_millis(case.backoff as u64),
        script: case
            .script
            .iter()
            .map(|&(o, l)| (o, Duration::from_millis(l as u64)))
            .collect(),
        ..Opts::default()
    };
    let mut rig = Rig::<R>::new(&opts);
    assert_eq!(
        rig.ctx.runtime.landing_budget,
        Duration::from_millis(budget_ms as u64),
        "the case's deadlines assume the default landing budget"
    );
    for b in &case.blocks {
        rig.until(b.take).await;
        // Each block has `id` rows, so its spool file names it.
        rig.push(b.id, b.id, 10, b.deadline - budget_ms, None);
    }
    rig.finish().await;

    // Group the calls by token, in the order each token first appears.
    let calls = rig.inserter.calls();
    let mut tokens: Vec<String> = Vec::new();
    let mut inserts: Vec<MInsert> = Vec::new();
    for call in &calls {
        let members = {
            let mut members: Vec<u32> = Vec::new();
            for row in &call.rows {
                if members.last() != Some(&row.member()) {
                    members.push(row.member());
                }
            }
            members
        };
        match tokens.iter().position(|t| *t == call.token) {
            Some(i) => {
                assert_eq!(
                    inserts[i].members, members,
                    "seed {}: a resend carries the batch's members",
                    case.seed
                );
                inserts[i].attempts.push(rig.ms(call.at));
            }
            None => {
                tokens.push(call.token.clone());
                inserts.push(MInsert {
                    members,
                    attempts: vec![rig.ms(call.at)],
                });
            }
        }
    }
    let mut spool: Vec<(u32, i64, SpoolKind)> = rig
        .gate
        .records
        .lock()
        .expect("records")
        .iter()
        .map(|r| (r.rows as u32, rig.ms(r.ended), r.kind))
        .collect();
    spool.sort_by_key(|(id, at, _)| (*id, *at));
    let claims = rig.claims();
    let settled = rig
        .answers()
        .into_iter()
        .map(|(id, at, answer)| {
            let claim = claims
                .iter()
                .find(|(m, _, _)| *m == id)
                .map(|(_, _, o)| *o)
                .unwrap_or_else(|| panic!("seed {}: member {id} reported no claim", case.seed));
            let spool = spool.iter().find(|(m, _, _)| *m == id).map(|(_, _, k)| *k);
            MSettled {
                id,
                at,
                answer,
                claim,
                spool,
            }
        })
        .collect();
    Observed {
        inserts,
        tokens,
        settled,
        claim_at: claims.iter().map(|(m, at, _)| (*m, *at)).collect(),
        spool,
    }
}

/// Runs `case` through the worker and the model and compares every insert,
/// token and settlement; returns which listed situations it reached.
async fn check(case: &Case) -> Reached {
    let model = Model::run(case);
    let seen = if case.trace {
        observe::<TraceLandingRow>(case).await
    } else {
        observe::<LogLandingRow>(case).await
    };
    let context = || {
        format!(
            "seed {} (trace {}): window {} lapse {} backoff {} retries {} script {:?}\n  blocks in take order (id, take, deadline): {:?}\n  model inserts {:?}\n  model settled {:?}",
            case.seed,
            case.trace,
            case.window,
            case.lapse,
            case.backoff,
            case.retries,
            case.script,
            case.blocks
                .iter()
                .map(|b| (b.id, b.take, b.deadline))
                .collect::<Vec<_>>(),
            model.inserts,
            model.settled,
        )
    };
    let mut want_inserts: Vec<MInsert> = model
        .inserts
        .iter()
        .filter(|i| !i.attempts.is_empty())
        .cloned()
        .collect();
    want_inserts.sort_by(|a, b| (a.attempts[0], &a.members).cmp(&(b.attempts[0], &b.members)));
    let mut seen_inserts = seen.inserts.clone();
    seen_inserts.sort_by(|a, b| (a.attempts[0], &a.members).cmp(&(b.attempts[0], &b.members)));
    assert_eq!(seen_inserts, want_inserts, "inserts differ\n{}", context());
    assert!(
        seen.tokens.iter().all(|t| !t.is_empty()),
        "every batch carries a token\n{}",
        context()
    );
    let mut want_settled = model.settled.clone();
    want_settled.sort_by_key(|s| s.id);
    assert_eq!(
        seen.settled,
        want_settled,
        "settlements differ\n{}",
        context()
    );
    for (id, at) in &seen.claim_at {
        let want = want_settled
            .iter()
            .find(|s| s.id == *id)
            .expect("a settled block")
            .at;
        assert_eq!(
            *at,
            want,
            "member {id}'s claim is reported when it settles\n{}",
            context()
        );
    }
    for (id, at, _) in &seen.spool {
        let want = want_settled
            .iter()
            .find(|s| s.id == *id)
            .expect("a settled block")
            .at;
        assert_eq!(
            *at,
            want,
            "member {id}'s spool file is written when it settles\n{}",
            context()
        );
    }
    model.reached
}

/// The listed situations, by hand, for each signal: `D` inside a lapse;
/// an `InsertUncertain` attempt whose backoff reaches `D`.
fn listed_cases() -> Vec<Case> {
    let mut out = Vec::new();
    for trace in [false, true] {
        out.push(Case {
            seed: 0,
            trace,
            window: 1_000,
            lapse: 50,
            backoff: 0,
            retries: 0,
            script: vec![(Outcome::Ok, 10)],
            blocks: vec![MBlock {
                id: 1,
                take: 0,
                deadline: 1_030,
            }],
        });
        out.push(Case {
            seed: 0,
            trace,
            window: 1_000,
            lapse: 0,
            backoff: 30,
            retries: 3,
            script: vec![(Outcome::Uncertain, 50)],
            blocks: vec![
                MBlock {
                    id: 1,
                    take: 0,
                    deadline: 1_070,
                },
                MBlock {
                    id: 2,
                    take: 10,
                    deadline: 50_000,
                },
                MBlock {
                    id: 3,
                    take: 20,
                    deadline: 50_000,
                },
            ],
        });
    }
    out
}

#[tokio::test(start_paused = true)]
async fn u12_the_worker_matches_the_reference_model() {
    past_origin().await;
    for case in listed_cases() {
        let reached = check(&case).await;
        assert!(
            reached.deadline_in_lapse || reached.uncertain_then_deadline_in_backoff,
            "a listed case reaches its situation: {case:?}"
        );
    }
    let mut counts = [0u32; 3];
    for i in 0..2_000u64 {
        let case = generate(0x494B_0000 + i);
        let reached = check(&case).await;
        counts[0] += u32::from(reached.deadline_in_lapse);
        counts[1] += u32::from(reached.backoff_reaches_deadline);
        counts[2] += u32::from(reached.uncertain_then_deadline_in_backoff);
    }
    assert!(
        counts.iter().all(|&c| c > 0),
        "the generated cases reach every listed situation (deadline in a lapse, a backoff reaching it, an uncertain attempt then the deadline in its backoff): {counts:?}"
    );
}
