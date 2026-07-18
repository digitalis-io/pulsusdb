//! `GET /api/logs/v1/tail` — the live-tail WebSocket (issue #74,
//! docs/api.md §2.4).
//!
//! ClickHouse has no push/notify, so tail is honestly a bounded polling
//! loop: each poll runs the IDENTICAL stage-1/2/3 plan machinery and the
//! SAME `CompiledPipeline` as the query path (`LogQlEngine::tail_poll` —
//! the task-manager-ratified semantics-drift-free invariant), fetching
//! one keyset page in global composite row order.
//!
//! **Cursor** (round-4 adjudication): `(scan watermark, boundary
//! cursor)`. The watermark advances to each exhausted slice's upper
//! bound unconditionally — empty slices included, so quiet backlog
//! windows drain at one slice per poll; the boundary cursor (the
//! occurrence-count keyset tuple) moves only on fetched rows and resumes
//! a `LIMIT`-split tie group exactly-once via the SQL `OFFSET`.
//!
//! **Topology** (plan v3 D3/D4): a producer task (poll loop) and the
//! socket-owning writer are joined by a bounded evicting [`FrameBuf`] —
//! saturation (a client not draining) evicts the OLDEST frame into a
//! persistent cumulative drop accumulator, reported on the wire as a
//! capped `dropped_entries` sample plus the exact `dropped_total`. A
//! shared two-way `watch` cancellation (process shutdown + per-connection
//! cancel) races EVERY await — engine polls, Text sends (additionally
//! bounded by `tail_send_timeout`), and the best-effort Close — so
//! neither a wedged ClickHouse read nor a non-reading client can stall
//! graceful shutdown. Either task's exit cancels its sibling; the writer
//! joins the producer and owns the connection-slot permit for the
//! socket's lifetime.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade, close_code};
use axum::extract::{RawQuery, State};
use axum::response::{IntoResponse, Response};

use pulsus_config::Config;
use pulsus_logql::Expr;
use pulsus_read::{
    Direction, LogQlEngine, QueryParams, QuerySpec, ReadError, StreamResult, TailLower, TailPage,
    TailSetup,
};
use tokio::sync::{Notify, watch};

use crate::app::AppState;

use super::encode;
use super::error::ApiError;
use super::handlers::engine_for;
use super::params::{self, ParamError};

/// Best-effort deadline on the final WebSocket Close frame. Deliberately
/// a SHORT documented constant, not `tail_send_timeout`: the Close is
/// sent on the shutdown/cancellation path, where the AC contract is
/// "run_tail returns promptly via cancellation" — a client that stopped
/// reading gets its Close attempt bounded here rather than pinning
/// shutdown for a full send timeout.
const CLOSE_GRACE: Duration = Duration::from_millis(250);

/// One evicted (undelivered) entry, reported in the frame's
/// `dropped_entries` sample (docs/api.md §2.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Dropped {
    /// The owning stream's canonical labels JSON, spliced verbatim.
    pub(super) labels_json: String,
    pub(super) timestamp_ns: i64,
}

// ---------------------------------------------------------------------
// Request params
// ---------------------------------------------------------------------

#[derive(Debug)]
struct TailParams {
    expr: Expr,
    start_ns: i64,
    delay_ns: i64,
    fetch_limit: u32,
}

/// The pre-query fetch clamp (plan v3 D5): a client `limit` above
/// `reader.tail_max_fetch_limit` is silently clamped BEFORE any SQL is
/// built — the row allocation is bounded pre-query.
fn clamped_fetch_limit(client_limit: u64, cap: u32) -> u32 {
    u32::try_from(client_limit).unwrap_or(u32::MAX).min(cap)
}

/// One daily `log_samples` partition, in nanoseconds — the partition-day
/// SLACK the retention clamp subtracts below the retention window.
const PARTITION_DAY_NS: i64 = 86_400_000_000_000;

/// The retention floor an ancient tail `start` is clamped up to (issue
/// #94 item 1): `now − retention_days·day − 1 partition-day`.
///
/// `log_samples` TTLs everything older than `retention_days`
/// (`TTL … + INTERVAL {retention_days} DAY DELETE`) and partitions daily,
/// so no non-expired data can exist before this floor and an ancient
/// `start` (e.g. `0`) need not walk pre-retention history one empty slice
/// per poll — the clamp bounds catch-up to the retention window and
/// collapses the stage-1 month IN-list a raw `start=0` would explode to
/// ~670 monthly literals.
///
/// The extra **partition-day slack** (binding adjudication
/// issuecomment-5007695597 / -5007752964) is load-bearing: with
/// `ttl_only_drop_parts=1` a daily partition is deleted only once WHOLLY
/// expired, so up to one partition-day of still-present, sub-retention
/// data can sit just below `now − retention`. Subtracting one partition-
/// day keeps that data reachable — the clamp never silently skips present
/// rows. All arithmetic saturates (a `u32::MAX` retention can never
/// overflow `i64`); the floor only ever RAISES an ancient `start`, never
/// lowers a legitimate recent one (the caller applies it via `max`).
fn retention_floor_ns(retention_days: u32, now_ns: i64) -> i64 {
    let win = i64::from(retention_days)
        .saturating_mul(PARTITION_DAY_NS)
        .saturating_add(PARTITION_DAY_NS);
    now_ns.saturating_sub(win)
}

fn parse_tail_params(pairs: &[(String, String)], cfg: &Config) -> Result<TailParams, ApiError> {
    let query = params::get(pairs, "query").ok_or(ParamError::MissingQuery)?;
    let expr = pulsus_logql::parse(query)?;
    if !matches!(expr, Expr::Log(_)) {
        return Err(ParamError::MetricQueryUnsupported { endpoint: "tail" }.into());
    }
    let fetch_limit = match params::get(pairs, "limit") {
        None => params::DEFAULT_LIMIT.min(cfg.reader.tail_max_fetch_limit),
        Some(raw) => {
            let n: u64 = raw
                .parse()
                .map_err(|_| ParamError::InvalidTailLimit(raw.to_string()))?;
            if n == 0 {
                return Err(ParamError::InvalidTailLimit(raw.to_string()).into());
            }
            clamped_fetch_limit(n, cfg.reader.tail_max_fetch_limit)
        }
    };
    let start_ns = match params::get(pairs, "start") {
        Some(v) => params::parse_ts(v)?,
        None => params::default_start_ns(params::now_ns()),
    };
    // Retention clamp (issue #94 item 1): raise an ancient first-page
    // lower bound to the retention floor so catch-up never walks
    // pre-retention (already-TTL'd) history one empty slice per poll.
    // `max` only ever RAISES the floor — a legitimate recent `start`
    // (default 1h, or an explicit within-retention value) is untouched.
    let start_ns = start_ns.max(retention_floor_ns(cfg.retention_days, params::now_ns()));
    // `delay_for`: default 0, clamped at `reader.tail_max_delay` (the
    // adjudicated public ceiling, docs/api.md §2.4).
    let delay_secs: u64 = match params::get(pairs, "delay_for") {
        None => 0,
        Some(raw) => raw
            .parse()
            .map_err(|_| ParamError::InvalidDelayFor(raw.to_string()))?,
    };
    let delay = Duration::from_secs(delay_secs).min(cfg.reader.tail_max_delay.0);
    let delay_ns = i64::try_from(delay.as_nanos()).unwrap_or(i64::MAX);
    Ok(TailParams {
        expr,
        start_ns,
        delay_ns,
        fetch_limit,
    })
}

/// The per-connection loop configuration, snapshotted from `Config` +
/// request params at upgrade time.
#[derive(Debug, Clone, Copy)]
struct TailLoopConfig {
    start_ns: i64,
    delay_ns: i64,
    fetch_limit: u32,
    poll_interval: Duration,
    slice_ns: i64,
    channel_depth: usize,
    send_timeout: Duration,
    dropped_sample_cap: usize,
}

impl TailLoopConfig {
    fn new(cfg: &Config, p: &TailParams) -> Self {
        TailLoopConfig {
            start_ns: p.start_ns,
            delay_ns: p.delay_ns,
            fetch_limit: p.fetch_limit,
            poll_interval: cfg.reader.tail_poll_interval.0,
            slice_ns: i64::try_from(cfg.reader.tail_catchup_slice.0.as_nanos()).unwrap_or(i64::MAX),
            channel_depth: cfg.reader.tail_channel_depth,
            send_timeout: cfg.reader.tail_send_timeout.0,
            dropped_sample_cap: cfg.reader.tail_max_entries_per_frame,
        }
    }
}

// ---------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------

/// `GET /api/logs/v1/tail`. Everything fallible happens BEFORE the
/// upgrade — bad params/metric exprs/uncompilable pipelines are 400, no
/// pool is 503, slot exhaustion is 429 — so a client only ever sees a
/// `101` once the tail is actually going to run.
pub(crate) async fn tail(
    State(state): State<AppState>,
    RawQuery(raw): RawQuery,
    ws: WebSocketUpgrade,
) -> Response {
    let pairs = params::parse_pairs(raw.as_deref().unwrap_or(""));
    let p = match parse_tail_params(&pairs, &state.config) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    let engine = match engine_for(&state).await {
        Ok(engine) => engine,
        Err(e) => return e.into_response(),
    };
    // Plan + compile once per connection (a bad regex/template 400s
    // here); each poll re-resolves fingerprints only.
    let qp = QueryParams {
        spec: QuerySpec::Range {
            start_ns: p.start_ns,
            end_ns: params::now_ns(),
            step_ns: 1_000_000_000,
        },
        limit: p.fetch_limit,
        direction: Direction::Forward,
    };
    let setup = match engine.tail_setup(&p.expr, &qp) {
        Ok(setup) => setup,
        Err(e) => return ApiError::Read(e).into_response(),
    };
    // The OWNED permit (plan v3 D4): acquired pre-upgrade (429 on
    // exhaustion), moved into the upgrade future, held by the
    // socket-owning task for the connection's lifetime.
    let permit = match Arc::clone(&state.tail.slots).try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => return ApiError::TailBusy.into_response(),
    };
    let shutdown = state.tail.shutdown.clone();
    let cfg = TailLoopConfig::new(&state.config, &p);

    ws.on_upgrade(move |socket| async move {
        use futures::StreamExt;
        // Held until run_tail returns — released exactly when the
        // connection ends, whatever the exit path.
        let _permit = permit;
        let fetcher = EngineFetcher { engine, setup };
        let (sink, stream) = socket.split();
        run_tail(
            fetcher,
            AxumSender(sink),
            AxumReceiver(stream),
            cfg,
            shutdown,
        )
        .await;
    })
}

// ---------------------------------------------------------------------
// Fetch + socket seams (hermetic-test injection points)
// ---------------------------------------------------------------------

/// One tail poll — the engine in production, a fake in the hermetic
/// cancellation/backpressure tests (plan v3 AC4: "inject a fake fetch").
trait TailFetcher: Send + 'static {
    fn poll(
        &mut self,
        lower: TailLower,
        upper_ns: i64,
        fetch_limit: u32,
    ) -> impl Future<Output = Result<TailPage, ReadError>> + Send;
}

struct EngineFetcher {
    engine: LogQlEngine,
    setup: TailSetup,
}

impl TailFetcher for EngineFetcher {
    async fn poll(
        &mut self,
        lower: TailLower,
        upper_ns: i64,
        fetch_limit: u32,
    ) -> Result<TailPage, ReadError> {
        // Best-effort month refresh (issue #94 item 2): re-resolve the
        // stage-1 month set when this poll's `upper_ns` crosses into a
        // calendar month the plan doesn't cover — a pure `plan::plan`
        // string rebuild, NO ClickHouse round-trip, ≤ once per calendar
        // month per connection. Swallowed on failure (`let _`): the tail
        // continues on the PRIOR plan (new-month streams surface on the
        // next successful refresh or a reconnect) — a re-plan error can
        // never tear down a working connection. The mutable borrow ends
        // before `tail_poll`'s immutable borrows begin.
        let _ = self.engine.tail_refresh_months(&mut self.setup, upper_ns);
        self.engine
            .tail_poll(
                &self.setup.compiled,
                &self.setup.plan,
                lower,
                upper_ns,
                fetch_limit,
            )
            .await
    }
}

/// The outbound half of the split socket — axum's `SplitSink` in
/// production, a fake sink in the hermetic tests.
trait TailSender: Send + 'static {
    fn send_text(&mut self, text: String) -> impl Future<Output = Result<(), ()>> + Send;
    fn send_close(&mut self, reason: Option<String>) -> impl Future<Output = ()> + Send;
}

/// The inbound half (review round 1, medium): a dedicated reader task
/// awaits `closed()` INDEPENDENTLY of the writer, so a client Close (or
/// transport error) reaches the shared cancellation even while the
/// writer sits inside a backpressured `send_text` — the in-flight send
/// is then cancelled via the watch, releasing the producer and the
/// connection permit immediately instead of after `tail_send_timeout`.
trait TailReceiver: Send + 'static {
    /// Resolves when the client sent Close or the transport ended/errored
    /// (other inbound frames — ping/pong/text — are ignored).
    fn closed(&mut self) -> impl Future<Output = ()> + Send;
}

struct AxumSender(futures::stream::SplitSink<WebSocket, Message>);

impl TailSender for AxumSender {
    async fn send_text(&mut self, text: String) -> Result<(), ()> {
        use futures::SinkExt;
        self.0
            .send(Message::Text(text.into()))
            .await
            .map_err(|_| ())
    }

    async fn send_close(&mut self, reason: Option<String>) {
        use futures::SinkExt;
        let frame = reason.map(|mut r| {
            // A WebSocket close reason is capped at 123 bytes; truncate
            // on a char boundary.
            let mut cut = r.len().min(123);
            while !r.is_char_boundary(cut) {
                cut -= 1;
            }
            r.truncate(cut);
            CloseFrame {
                code: close_code::ERROR,
                reason: r.into(),
            }
        });
        let _ = self.0.send(Message::Close(frame)).await;
    }
}

struct AxumReceiver(futures::stream::SplitStream<WebSocket>);

impl TailReceiver for AxumReceiver {
    async fn closed(&mut self) {
        use futures::StreamExt;
        loop {
            match self.0.next().await {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => return,
                Some(Ok(_)) => {}
            }
        }
    }
}

// ---------------------------------------------------------------------
// The bounded evicting frame buffer (plan v3 D4 / v4 D4)
// ---------------------------------------------------------------------

/// The persistent drop accumulator: `total` is exact and survives
/// consecutive saturations until a frame drains it; `sample` keeps the
/// most recent `cap` evicted rows as the wire's bounded
/// `dropped_entries` representative sample.
#[derive(Debug)]
struct DroppedAcc {
    sample: VecDeque<Dropped>,
    total: u64,
    cap: usize,
}

impl DroppedAcc {
    fn absorb(&mut self, streams: &[StreamResult]) {
        for s in streams {
            for (ts, _) in &s.entries {
                self.total += 1;
                self.sample.push_back(Dropped {
                    labels_json: s.labels_json.clone(),
                    timestamp_ns: *ts,
                });
                while self.sample.len() > self.cap {
                    self.sample.pop_front();
                }
            }
        }
    }

    fn drain(&mut self) -> (Vec<Dropped>, u64) {
        (
            self.sample.drain(..).collect(),
            std::mem::take(&mut self.total),
        )
    }
}

/// The bounded producer→writer buffer: pushing past `cap` evicts the
/// OLDEST frame into [`DroppedAcc`] (genuine drop-oldest — a slow
/// consumer loses the stalest data first, with exact accounting).
#[derive(Debug)]
struct FrameBuf {
    frames: VecDeque<Vec<StreamResult>>,
    dropped: DroppedAcc,
    cap: usize,
    /// A fatal producer error (e.g. "query too broad" from a stream-cap
    /// breach); the writer drains, then closes with this reason.
    error: Option<String>,
}

impl FrameBuf {
    fn new(cap: usize, dropped_sample_cap: usize) -> Self {
        FrameBuf {
            frames: VecDeque::new(),
            dropped: DroppedAcc {
                sample: VecDeque::new(),
                total: 0,
                cap: dropped_sample_cap,
            },
            cap,
            error: None,
        }
    }

    fn push_evicting(&mut self, streams: Vec<StreamResult>) {
        self.frames.push_back(streams);
        while self.frames.len() > self.cap {
            if let Some(oldest) = self.frames.pop_front() {
                self.dropped.absorb(&oldest);
            }
        }
    }

    /// Pops the next frame's STATE — the oldest queued frame with the
    /// drained drop accounting attached, or, when no frame is queued but
    /// drops are pending, a synthesized streams-empty frame so the
    /// cumulative count is never stranded. Deliberately does NOT encode
    /// (review round 1, low): the caller encodes AFTER releasing the
    /// queue mutex, so the per-stream/per-drop string building never
    /// runs under the lock the producer contends on.
    fn pop_next(&mut self) -> Option<PoppedFrame> {
        if let Some(streams) = self.frames.pop_front() {
            let (dropped, dropped_total) = self.dropped.drain();
            return Some(PoppedFrame {
                streams,
                dropped,
                dropped_total,
            });
        }
        if self.dropped.total > 0 {
            let (dropped, dropped_total) = self.dropped.drain();
            return Some(PoppedFrame {
                streams: Vec::new(),
                dropped,
                dropped_total,
            });
        }
        None
    }
}

/// One popped-but-not-yet-encoded frame: owned state moved out under the
/// lock, encoded outside it.
struct PoppedFrame {
    streams: Vec<StreamResult>,
    dropped: Vec<Dropped>,
    dropped_total: u64,
}

impl PoppedFrame {
    fn encode(self) -> String {
        encode::tail_frame(self.streams, &self.dropped, self.dropped_total)
    }
}

struct Shared {
    buf: Mutex<FrameBuf>,
    notify: Notify,
}

impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, FrameBuf> {
        // Poison recovery: a panicked lock holder cannot corrupt a
        // VecDeque of owned frames; continuing with the inner value is
        // strictly better than cascading the panic through shutdown.
        self.buf
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

// ---------------------------------------------------------------------
// Scan state (watermark + boundary cursor — round-4 adjudication #2)
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct ScanState {
    /// Everything at or below this instant has been fully scanned
    /// (except the boundary tuple's remainder when a page was split).
    watermark_ns: i64,
    /// The occurrence-count keyset cursor — moves only on fetched rows.
    boundary: Option<pulsus_read::TailCursor>,
}

impl ScanState {
    fn new(start_ns: i64) -> Self {
        ScanState {
            watermark_ns: start_ns,
            boundary: None,
        }
    }

    /// The next poll's lower bound: the boundary keyset while it still
    /// sits at/ahead of the watermark (an unexhausted slice), else the
    /// watermark as a first-page-style exclusive `start`.
    fn lower(&self) -> (TailLower, i64) {
        match self.boundary {
            Some(b) if b.tuple.0 >= self.watermark_ns => (TailLower::After(b), b.tuple.0),
            _ => (
                TailLower::Start {
                    start_ns: self.watermark_ns,
                },
                self.watermark_ns,
            ),
        }
    }

    /// Folds one page in; returns `true` when the slice was exhausted
    /// (the page came back under `fetch_limit`).
    fn observe(&mut self, page: &TailPage, upper_ns: i64, fetch_limit: u32) -> bool {
        if page.fetched >= fetch_limit {
            // Page full: the slice may hold more rows — the boundary
            // cursor resumes it, and the watermark advances only to the
            // boundary instant (never past unfetched rows).
            if let Some(next) = page.next {
                self.boundary = Some(next);
                self.watermark_ns = self.watermark_ns.max(next.tuple.0);
            }
            false
        } else {
            // Slice exhausted: the watermark advances to the slice's
            // upper bound UNCONDITIONALLY — empty slices included
            // (round-4 adjudication #2), so a quiet backlog window can
            // never be re-queried forever. The boundary moves only when
            // rows were actually fetched.
            if page.fetched > 0 {
                self.boundary = page.next;
            }
            self.watermark_ns = self.watermark_ns.max(upper_ns);
            true
        }
    }
}

// ---------------------------------------------------------------------
// The two tasks
// ---------------------------------------------------------------------

/// Resolves when either the process shutdown or the per-connection
/// cancel fires (or either sender is gone — runtime teardown). Every
/// long await in both tasks races this.
async fn cancelled(shutdown: &mut watch::Receiver<bool>, cancel: &mut watch::Receiver<bool>) {
    if *shutdown.borrow() || *cancel.borrow() {
        return;
    }
    tokio::select! {
        _ = shutdown.changed() => {}
        _ = cancel.changed() => {}
    }
}

/// Sleeps `dur` unless cancelled first; `true` = cancelled.
async fn sleep_or_cancelled(
    shutdown: &mut watch::Receiver<bool>,
    cancel: &mut watch::Receiver<bool>,
    dur: Duration,
) -> bool {
    tokio::select! {
        _ = cancelled(shutdown, cancel) => true,
        _ = tokio::time::sleep(dur) => false,
    }
}

async fn producer_loop<F: TailFetcher>(
    mut fetcher: F,
    shared: Arc<Shared>,
    cfg: TailLoopConfig,
    mut shutdown: watch::Receiver<bool>,
    mut cancel: watch::Receiver<bool>,
    cancel_tx: Arc<watch::Sender<bool>>,
) {
    let mut state = ScanState::new(cfg.start_ns);
    loop {
        if *shutdown.borrow() || *cancel.borrow() {
            break;
        }
        let horizon = params::now_ns().saturating_sub(cfg.delay_ns);
        let (lower, lower_ts) = state.lower();
        // The boundary tuple's instant may still hold undelivered ties
        // (inclusive resume); a Start bound is exclusive.
        let can_poll = match lower {
            TailLower::After(_) => horizon >= lower_ts,
            TailLower::Start { .. } => horizon > lower_ts,
        };
        if !can_poll {
            if sleep_or_cancelled(&mut shutdown, &mut cancel, cfg.poll_interval).await {
                break;
            }
            continue;
        }
        // Time-sliced catch-up (plan v4 D3): one query never scans/sorts
        // more than one `tail_catchup_slice` window.
        let upper = horizon.min(lower_ts.saturating_add(cfg.slice_ns));

        let result = tokio::select! {
            _ = cancelled(&mut shutdown, &mut cancel) => break,
            r = fetcher.poll(lower, upper, cfg.fetch_limit) => r,
        };
        let page = match result {
            Ok(page) => page,
            Err(e) => {
                // Fatal poll failure (e.g. a broad selector tripping the
                // stream cap): hand the reason to the writer, which
                // closes the socket with it.
                shared.lock().error = Some(e.to_string());
                shared.notify.notify_one();
                break;
            }
        };
        let exhausted = state.observe(&page, upper, cfg.fetch_limit);
        if page.streams.iter().any(|s| !s.entries.is_empty()) {
            shared.lock().push_evicting(page.streams);
            shared.notify.notify_one();
        }
        // Caught up ⇒ idle for one poll interval; otherwise (full page,
        // or more backlog slices ahead) re-poll immediately.
        if exhausted
            && upper >= horizon
            && sleep_or_cancelled(&mut shutdown, &mut cancel, cfg.poll_interval).await
        {
            break;
        }
    }
    let _ = cancel_tx.send(true);
}

/// The dedicated inbound-reader task (review round 1, medium): waits for
/// a client Close/transport end on the split stream and fires the shared
/// cancellation — independent of the writer, so a Close lands even while
/// a `send_text` is backpressured mid-flight.
async fn reader_loop<R: TailReceiver>(
    mut receiver: R,
    mut shutdown: watch::Receiver<bool>,
    mut cancel: watch::Receiver<bool>,
    cancel_tx: Arc<watch::Sender<bool>>,
) {
    tokio::select! {
        _ = cancelled(&mut shutdown, &mut cancel) => {}
        _ = receiver.closed() => {}
    }
    let _ = cancel_tx.send(true);
}

async fn writer_loop<S: TailSender>(
    mut sender: S,
    shared: Arc<Shared>,
    cfg: TailLoopConfig,
    mut shutdown: watch::Receiver<bool>,
    mut cancel: watch::Receiver<bool>,
    cancel_tx: Arc<watch::Sender<bool>>,
) {
    let mut close_reason: Option<String> = None;
    'conn: loop {
        // Drain every queued frame before waiting again (a Notify permit
        // does not count queued items). State pops under the lock;
        // ENCODING happens after the guard drops (review round 1, low).
        loop {
            let popped = shared.lock().pop_next();
            let Some(popped) = popped else { break };
            let text = popped.encode();
            tokio::select! {
                _ = cancelled(&mut shutdown, &mut cancel) => break 'conn,
                sent = tokio::time::timeout(cfg.send_timeout, sender.send_text(text)) => {
                    match sent {
                        Ok(Ok(())) => {}
                        // Send timeout (a client that stopped reading) or
                        // transport error: disconnect.
                        Ok(Err(())) | Err(_) => break 'conn,
                    }
                }
            }
        }
        if let Some(reason) = shared.lock().error.take() {
            close_reason = Some(reason);
            break;
        }
        if *shutdown.borrow() || *cancel.borrow() {
            break;
        }
        tokio::select! {
            _ = cancelled(&mut shutdown, &mut cancel) => break,
            _ = shared.notify.notified() => {}
        }
    }
    // Whatever path broke the loop, a pending producer error is the close
    // reason (the producer's cancel can win the select race against its
    // own error notification).
    if close_reason.is_none() {
        close_reason = shared.lock().error.take();
    }
    // Cancel the siblings FIRST so they stop while the (bounded,
    // best-effort) Close goes out.
    let _ = cancel_tx.send(true);
    let _ = tokio::time::timeout(CLOSE_GRACE, sender.send_close(close_reason)).await;
}

/// Runs one tail connection to completion: spawns the producer and the
/// inbound reader, runs the writer on the upgrade task, and joins both
/// on the way out (any task's exit cancels the others via the shared
/// watch).
async fn run_tail<F: TailFetcher, S: TailSender, R: TailReceiver>(
    fetcher: F,
    sender: S,
    receiver: R,
    cfg: TailLoopConfig,
    shutdown: watch::Receiver<bool>,
) {
    let shared = Arc::new(Shared {
        buf: Mutex::new(FrameBuf::new(cfg.channel_depth, cfg.dropped_sample_cap)),
        notify: Notify::new(),
    });
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let cancel_tx = Arc::new(cancel_tx);
    let producer = tokio::spawn(producer_loop(
        fetcher,
        Arc::clone(&shared),
        cfg,
        shutdown.clone(),
        cancel_rx.clone(),
        Arc::clone(&cancel_tx),
    ));
    let reader = tokio::spawn(reader_loop(
        receiver,
        shutdown.clone(),
        cancel_rx.clone(),
        Arc::clone(&cancel_tx),
    ));
    writer_loop(sender, shared, cfg, shutdown, cancel_rx, cancel_tx).await;
    let _ = producer.await;
    let _ = reader.await;
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Instant;

    use pulsus_read::TailCursor;

    fn stream(labels: &str, entries: Vec<(i64, &str)>) -> StreamResult {
        StreamResult {
            fingerprint: 1,
            service: "checkout".to_string(),
            labels_json: labels.to_string(),
            entries: entries
                .into_iter()
                .map(|(ts, line)| (ts, line.to_string()))
                .collect(),
        }
    }

    /// `start_ns` sits just behind "now" so the loop reaches steady
    /// state immediately — a deep past start would (correctly, by
    /// design) walk the whole backlog one slice per poll first.
    fn test_cfg() -> TailLoopConfig {
        TailLoopConfig {
            start_ns: params::now_ns() - 10_000_000,
            delay_ns: 0,
            fetch_limit: 100,
            poll_interval: Duration::from_millis(5),
            slice_ns: 60_000_000_000,
            channel_depth: 4,
            send_timeout: Duration::from_secs(60),
            dropped_sample_cap: 1_000,
        }
    }

    // -- clamp ---------------------------------------------------------

    /// Issue #74 pre-query clamp AC: the clamp happens before the SQL is
    /// built, and the built keyset SQL carries exactly the cap.
    #[test]
    fn client_limit_above_the_cap_is_clamped_into_the_built_sql_limit() {
        assert_eq!(clamped_fetch_limit(9_999_999, 5_000), 5_000);
        assert_eq!(clamped_fetch_limit(10, 5_000), 10);
        assert_eq!(clamped_fetch_limit(u64::from(u32::MAX) + 5, 5_000), 5_000);

        let sql = pulsus_read::logql::sql::stage3_keyset(
            "log_samples",
            &["'checkout'".to_string()],
            &[1],
            pulsus_read::logql::sql::TimeWindow {
                start_ns: 0,
                end_ns: 10,
            },
            pulsus_read::logql::sql::KeysetLower::First,
            pulsus_read::logql::params::Direction::Forward,
            &[],
            clamped_fetch_limit(9_999_999, 5_000),
        );
        assert!(sql.ends_with("LIMIT 5000"), "{sql}");
    }

    // -- retention clamp (issue #94 item 1) -----------------------------

    const DAY_NS: i64 = 86_400_000_000_000;

    /// Item 1 AC (binding arithmetic, adjudication -5007752964): the floor
    /// is `now − retention_days·day − ONE partition-day` (the slack), and a
    /// `u32::MAX` retention saturates without overflowing `i64`.
    #[test]
    fn retention_floor_is_the_window_plus_a_partition_day_slack_and_saturates() {
        let now = 1_700_000_000_000_000_000i64;
        // 7-day retention ⇒ 8 days below now (7 + 1 partition-day slack).
        assert_eq!(retention_floor_ns(7, now), now - 8 * DAY_NS);
        assert_eq!(retention_floor_ns(1, now), now - 2 * DAY_NS);
        // Degenerate saturation: no panic, floor sits far below `now` so it
        // can never raise a legitimate start.
        let floor = retention_floor_ns(u32::MAX, now);
        assert!(floor <= now, "saturating floor never exceeds now: {floor}");
    }

    /// Binding fixture (adjudication -5007695597): the partition-day slack
    /// KEEPS an oldest-still-present part within retention — a row just
    /// below `now − retention` (in a partially-expired daily partition that
    /// `ttl_only_drop_parts=1` has not yet dropped) sits ABOVE the clamp
    /// floor (so it is INCLUDED), whereas a no-slack floor would skip it.
    #[test]
    fn retention_clamp_includes_the_oldest_still_present_part_via_the_slack() {
        let now = 1_700_000_000_000_000_000i64;
        // A still-present row half a partition-day below the 7-day edge.
        let oldest_still_present = now - 7 * DAY_NS - DAY_NS / 2;
        let slack_floor = retention_floor_ns(7, now);
        assert!(
            slack_floor < oldest_still_present,
            "with the slack the floor sits below still-present data (INCLUDED)"
        );
        // Without the slack the floor would sit ABOVE it — silently skipped.
        let no_slack_floor = now - 7 * DAY_NS;
        assert!(
            no_slack_floor > oldest_still_present,
            "the no-slack floor would skip the still-present part"
        );
        // Through the clamp (with the REAL wall clock parse_tail_params
        // uses): an ancient start=0 lands at the slack floor, which is
        // below a still-present row half a partition-day past the retention
        // edge, so a `timestamp_ns > start` stage-3 predicate keeps it.
        let now_real = params::now_ns();
        let oldest_still_present_real = now_real - 7 * DAY_NS - DAY_NS / 2;
        let pairs = params::parse_pairs("query=%7Ba%3D%22x%22%7D&start=0");
        let p = parse_tail_params(&pairs, &cfg_default()).expect("ok");
        assert!(
            p.start_ns < oldest_still_present_real,
            "clamped start {} must stay below still-present data {oldest_still_present_real}",
            p.start_ns
        );
    }

    /// Item 1 AC: an ancient `start=0` is clamped up to the retention
    /// floor; a legitimate recent `start` (10 min ago) is UNTOUCHED.
    #[test]
    fn retention_clamp_raises_ancient_start_but_never_lowers_a_recent_one() {
        let cfg = cfg_default();
        // start=0 ⇒ clamped to ~now − 8 days (default 7d retention + slack).
        let base = "query=%7Ba%3D%22x%22%7D";
        let before = params::now_ns();
        let ancient = params::parse_pairs(&format!("{base}&start=0"));
        let p = parse_tail_params(&ancient, &cfg).expect("ok");
        let after = params::now_ns();
        let floor_lo = before - 8 * DAY_NS;
        let floor_hi = after - 8 * DAY_NS;
        assert!(
            p.start_ns >= floor_lo && p.start_ns <= floor_hi,
            "start=0 clamped to the retention floor, got {}",
            p.start_ns
        );
        assert!(p.start_ns > 0, "the clamp fired (not the raw start=0)");

        // A recent explicit start (10 min ago) is well within retention —
        // returned verbatim, never lowered.
        let recent = params::now_ns() - 600_000_000_000;
        let recent_pairs = params::parse_pairs(&format!("{base}&start={recent}"));
        let p = parse_tail_params(&recent_pairs, &cfg).expect("ok");
        assert_eq!(p.start_ns, recent, "a within-retention start is untouched");

        // The default start (1h ago) is likewise within retention.
        let default_pairs = params::parse_pairs(base);
        let before = params::now_ns();
        let p = parse_tail_params(&default_pairs, &cfg).expect("ok");
        let after = params::now_ns();
        let hour = 3_600_000_000_000i64;
        assert!(
            p.start_ns >= before - hour && p.start_ns <= after - hour,
            "the default 1h-ago start is within retention, unclamped"
        );
    }

    // -- scan state (watermark + boundary) ------------------------------

    /// Round-4 adjudication #2: an EMPTY exhausted slice still advances
    /// the watermark to the slice's upper bound — a quiet backlog window
    /// is never re-queried.
    #[test]
    fn empty_slice_advances_the_watermark_unconditionally() {
        let mut s = ScanState::new(100);
        let page = TailPage {
            streams: vec![],
            next: None,
            fetched: 0,
        };
        let exhausted = s.observe(&page, 500, 10);
        assert!(exhausted);
        assert_eq!(s.watermark_ns, 500);
        assert!(s.boundary.is_none());
        // The next lower bound derives from the watermark.
        let (lower, ts) = s.lower();
        assert!(matches!(lower, TailLower::Start { start_ns: 500 }));
        assert_eq!(ts, 500);
    }

    /// A full page pins the watermark at the boundary instant (never
    /// past unfetched rows) and resumes via the keyset.
    #[test]
    fn full_page_resumes_from_the_boundary_cursor_not_the_slice_upper() {
        let mut s = ScanState::new(100);
        let cursor = TailCursor {
            tuple: (250, 7, 42),
            seen: 3,
        };
        let page = TailPage {
            streams: vec![],
            next: Some(cursor),
            fetched: 10,
        };
        let exhausted = s.observe(&page, 500, 10);
        assert!(!exhausted);
        assert_eq!(s.watermark_ns, 250, "watermark stays at the boundary");
        let (lower, ts) = s.lower();
        assert!(matches!(lower, TailLower::After(c) if c == cursor));
        assert_eq!(ts, 250);
    }

    /// An exhausted slice with rows: boundary updates, watermark jumps to
    /// the slice upper, and the (now dominated) boundary no longer
    /// drives the lower bound — unless it sits exactly AT the watermark,
    /// where its remaining ties must stay reachable.
    #[test]
    fn exhausted_slice_with_rows_moves_both_and_the_watermark_dominates() {
        let mut s = ScanState::new(100);
        let page = TailPage {
            streams: vec![],
            next: Some(TailCursor {
                tuple: (300, 1, 1),
                seen: 1,
            }),
            fetched: 4,
        };
        assert!(s.observe(&page, 500, 10));
        assert_eq!(s.watermark_ns, 500);
        let (lower, _) = s.lower();
        assert!(
            matches!(lower, TailLower::Start { start_ns: 500 }),
            "a boundary below the watermark is dominated"
        );

        // Boundary exactly at the watermark (last row at the slice
        // upper): the keyset stays live so same-instant ties resume.
        let mut s = ScanState::new(100);
        let at_upper = TailCursor {
            tuple: (500, 1, 1),
            seen: 2,
        };
        let page = TailPage {
            streams: vec![],
            next: Some(at_upper),
            fetched: 4,
        };
        assert!(s.observe(&page, 500, 10));
        let (lower, _) = s.lower();
        assert!(matches!(lower, TailLower::After(c) if c == at_upper));
    }

    // -- FrameBuf / DroppedAcc (through the ENCODED frame) --------------

    fn parse(payload: &str) -> serde_json::Value {
        serde_json::from_str(payload).expect("frame is valid JSON")
    }

    /// Issue #74 eviction AC (v4): repeated saturation evicts the OLDEST
    /// frames; the next ENCODED frame reports the exact cumulative
    /// `dropped_total` (accumulated across consecutive saturations) with
    /// a bounded `dropped_entries` sample, and the following frame shows
    /// `dropped_total: 0` — drain/reset proven on the wire.
    #[test]
    fn oldest_eviction_accounting_reaches_the_encoded_frame_exactly_once() {
        let mut buf = FrameBuf::new(2, 3);
        // Six frames of one entry each into a depth-2 buffer: frames
        // 1-4 evicted (oldest first), 5-6 retained.
        for i in 1..=6i64 {
            buf.push_evicting(vec![stream(r#"{"app":"x"}"#, vec![(i, "line")])]);
        }
        let first = parse(&buf.pop_next().expect("frame 5").encode());
        assert_eq!(first["dropped_total"], 4, "exact cumulative count");
        let sample = first["dropped_entries"].as_array().expect("array");
        assert_eq!(sample.len(), 3, "sample bounded at the cap");
        // Most recent evicted rows are kept: timestamps 2,3,4.
        let ts: Vec<&str> = sample
            .iter()
            .map(|d| d["timestamp"].as_str().expect("ns string"))
            .collect();
        assert_eq!(ts, vec!["2", "3", "4"]);
        assert_eq!(sample[0]["labels"]["app"], "x");
        // The surviving frame's own entry is intact.
        assert_eq!(first["streams"][0]["values"][0][0], "5");

        let second = parse(&buf.pop_next().expect("frame 6").encode());
        assert_eq!(second["dropped_total"], 0, "drained exactly once");
        assert_eq!(second["dropped_entries"], serde_json::json!([]));
        assert!(buf.pop_next().is_none());
    }

    /// Pending drops with no queued frame synthesize a streams-empty
    /// frame — the cumulative count is never stranded.
    #[test]
    fn pending_drops_without_a_frame_synthesize_an_empty_streams_frame() {
        let mut buf = FrameBuf::new(2, 10);
        buf.dropped
            .absorb(&[stream(r#"{"app":"x"}"#, vec![(9, "line")])]);
        let frame = parse(&buf.pop_next().expect("synthesized frame").encode());
        assert_eq!(frame["streams"], serde_json::json!([]));
        assert_eq!(frame["dropped_total"], 1);
        assert_eq!(frame["dropped_entries"][0]["timestamp"], "9");
        assert!(buf.pop_next().is_none());
    }

    // -- run_tail cancellation suite (hermetic fakes) --------------------

    /// A fetcher that yields one single-entry frame per poll, forever.
    struct FrameEveryPoll;
    impl TailFetcher for FrameEveryPoll {
        async fn poll(
            &mut self,
            _lower: TailLower,
            upper_ns: i64,
            _fetch_limit: u32,
        ) -> Result<TailPage, ReadError> {
            Ok(TailPage {
                streams: vec![stream(r#"{"app":"x"}"#, vec![(upper_ns, "line")])],
                next: None,
                fetched: 1,
            })
        }
    }

    /// A fetcher whose poll never resolves (a wedged ClickHouse read).
    struct HangingFetcher;
    impl TailFetcher for HangingFetcher {
        async fn poll(
            &mut self,
            _lower: TailLower,
            _upper_ns: i64,
            _fetch_limit: u32,
        ) -> Result<TailPage, ReadError> {
            std::future::pending().await
        }
    }

    /// A fetcher that fails immediately (e.g. the stream cap tripping on
    /// a too-broad selector).
    struct FailingFetcher;
    impl TailFetcher for FailingFetcher {
        async fn poll(
            &mut self,
            _lower: TailLower,
            _upper_ns: i64,
            _fetch_limit: u32,
        ) -> Result<TailPage, ReadError> {
            Err(ReadError::QueryTooBroad(
                pulsus_read::logql::TooBroadReason::StreamCap {
                    count: 200_000,
                    cap: 100_000,
                },
            ))
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum SinkMode {
        /// `send_text` succeeds instantly.
        Accept,
        /// `send_text` never completes (a client that stopped reading,
        /// with full kernel buffers).
        BlockForever,
        /// `send_text` fails immediately (transport error).
        Fail,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum RecvMode {
        /// Inbound side stays silent forever.
        Silent,
        /// The client sends a Close after ~50ms.
        CloseSoon,
    }

    struct FakeSender {
        mode: SinkMode,
        sent: Arc<Mutex<Vec<String>>>,
        closed: Arc<Mutex<Option<Option<String>>>>,
    }

    impl FakeSender {
        fn new(mode: SinkMode) -> Self {
            FakeSender {
                mode,
                sent: Arc::new(Mutex::new(Vec::new())),
                closed: Arc::new(Mutex::new(None)),
            }
        }
    }

    impl TailSender for FakeSender {
        async fn send_text(&mut self, text: String) -> Result<(), ()> {
            match self.mode {
                SinkMode::Accept => {
                    self.sent.lock().unwrap().push(text);
                    Ok(())
                }
                SinkMode::BlockForever => std::future::pending().await,
                SinkMode::Fail => Err(()),
            }
        }

        async fn send_close(&mut self, reason: Option<String>) {
            *self.closed.lock().unwrap() = Some(reason);
        }
    }

    struct FakeReceiver(RecvMode);

    impl TailReceiver for FakeReceiver {
        async fn closed(&mut self) {
            match self.0 {
                RecvMode::Silent => std::future::pending().await,
                RecvMode::CloseSoon => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    }

    /// Issue #74 AC4 (v3 rewrite, cancellation-not-timeout): shutdown
    /// while the writer's send is blocked on a non-reading client — with
    /// a 60s send timeout, `run_tail` must return well under 1s, proving
    /// CANCELLATION unblocked it, not timeout expiry.
    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_under_blocked_send_returns_via_cancellation_not_timeout() {
        let (tx, rx) = watch::channel(false);
        let sender = FakeSender::new(SinkMode::BlockForever);
        let handle = tokio::spawn(run_tail(
            FrameEveryPoll,
            sender,
            FakeReceiver(RecvMode::Silent),
            test_cfg(),
            rx,
        ));
        // Let the producer yield a frame and the writer block on it.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let started = Instant::now();
        tx.send(true).expect("receiver alive");
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("run_tail must return well before the 60s send timeout")
            .expect("no panic");
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    /// AC4: shutdown while the ENGINE POLL hangs — the producer's fetch
    /// await is raced against cancellation too.
    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_under_hanging_poll_returns_promptly() {
        let (tx, rx) = watch::channel(false);
        let sender = FakeSender::new(SinkMode::Accept);
        let handle = tokio::spawn(run_tail(
            HangingFetcher,
            sender,
            FakeReceiver(RecvMode::Silent),
            test_cfg(),
            rx,
        ));
        tokio::time::sleep(Duration::from_millis(50)).await;
        tx.send(true).expect("receiver alive");
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("run_tail must return promptly on shutdown")
            .expect("no panic");
    }

    /// AC4: a writer transport failure cancels the producer (no orphaned
    /// poll loop) — run_tail returns with NO shutdown signal at all.
    #[tokio::test(flavor = "multi_thread")]
    async fn writer_send_failure_cancels_the_producer_and_run_tail_returns() {
        let (_tx, rx) = watch::channel(false);
        let sender = FakeSender::new(SinkMode::Fail);
        let handle = tokio::spawn(run_tail(
            FrameEveryPoll,
            sender,
            FakeReceiver(RecvMode::Silent),
            test_cfg(),
            rx,
        ));
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("a failed send must terminate both tasks")
            .expect("no panic");
    }

    /// AC4: a client Close terminates both tasks promptly — even while
    /// the producer is stuck inside a hanging engine poll.
    #[tokio::test(flavor = "multi_thread")]
    async fn client_close_cancels_both_tasks_even_mid_poll() {
        let (_tx, rx) = watch::channel(false);
        let sender = FakeSender::new(SinkMode::Accept);
        let handle = tokio::spawn(run_tail(
            HangingFetcher,
            sender,
            FakeReceiver(RecvMode::CloseSoon),
            test_cfg(),
            rx,
        ));
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("a client Close must terminate the connection")
            .expect("no panic");
    }

    /// Review round 1 (medium): a client Close arriving WHILE the
    /// writer's `send_text` is blocked on backpressure must still tear
    /// the connection down promptly — the independent inbound reader
    /// fires the shared cancellation, which the in-flight send races —
    /// releasing the producer AND the connection-slot permit in well
    /// under the 60s send timeout (and with no shutdown signal at all).
    #[tokio::test(flavor = "multi_thread")]
    async fn client_close_during_blocked_send_releases_producer_and_permit_promptly() {
        let (_tx, rx) = watch::channel(false);
        let sender = FakeSender::new(SinkMode::BlockForever);
        // The handler's exact permit topology: an owned permit moved into
        // the connection future, dropped when run_tail returns.
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = Arc::clone(&slots)
            .try_acquire_owned()
            .expect("slot available");
        let started = Instant::now();
        let handle = tokio::spawn(async move {
            let _permit = permit;
            run_tail(
                FrameEveryPoll,
                sender,
                FakeReceiver(RecvMode::CloseSoon),
                test_cfg(),
                rx,
            )
            .await;
        });
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("client Close must cancel the blocked send well before the 60s timeout")
            .expect("no panic");
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(
            slots.try_acquire().is_ok(),
            "the connection-slot permit must be released with the connection"
        );
    }

    /// Plan v2 D4: a fatal poll error ("query too broad") closes the
    /// socket with the error as the close reason.
    #[tokio::test(flavor = "multi_thread")]
    async fn producer_error_closes_the_socket_with_the_reason() {
        let (_tx, rx) = watch::channel(false);
        let sender = FakeSender::new(SinkMode::Accept);
        let closed = Arc::clone(&sender.closed);
        let handle = tokio::spawn(run_tail(
            FailingFetcher,
            sender,
            FakeReceiver(RecvMode::Silent),
            test_cfg(),
            rx,
        ));
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("a fatal poll error must terminate the connection")
            .expect("no panic");
        let reason = closed
            .lock()
            .unwrap()
            .clone()
            .expect("close was sent")
            .expect("close carried a reason");
        assert!(reason.contains("query too broad"), "{reason}");
    }

    /// Frames flow end to end through the loop (sanity for the fakes):
    /// the writer receives encoded frames with `dropped_total: 0`.
    #[tokio::test(flavor = "multi_thread")]
    async fn frames_flow_from_producer_to_writer() {
        let (tx, rx) = watch::channel(false);
        let sender = FakeSender::new(SinkMode::Accept);
        let sent = Arc::clone(&sender.sent);
        let handle = tokio::spawn(run_tail(
            FrameEveryPoll,
            sender,
            FakeReceiver(RecvMode::Silent),
            test_cfg(),
            rx,
        ));
        tokio::time::sleep(Duration::from_millis(100)).await;
        tx.send(true).expect("receiver alive");
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("returns")
            .expect("no panic");
        let sent = sent.lock().unwrap();
        assert!(!sent.is_empty(), "at least one frame was delivered");
        let frame = parse(&sent[0]);
        assert_eq!(frame["dropped_total"], 0);
        assert_eq!(frame["streams"][0]["stream"]["app"], "x");
    }

    /// The producer polls with sliced upper bounds during catch-up: with
    /// a synthetic fetcher recording windows, every window is bounded by
    /// the slice.
    #[tokio::test(flavor = "multi_thread")]
    async fn catch_up_polls_are_slice_bounded() {
        struct WindowRecorder {
            windows: Arc<Mutex<Vec<(i64, i64)>>>,
            polls: Arc<AtomicU32>,
        }
        impl TailFetcher for WindowRecorder {
            async fn poll(
                &mut self,
                lower: TailLower,
                upper_ns: i64,
                _fetch_limit: u32,
            ) -> Result<TailPage, ReadError> {
                let lower_ts = match lower {
                    TailLower::Start { start_ns } => start_ns,
                    TailLower::After(c) => c.tuple.0,
                };
                self.windows.lock().unwrap().push((lower_ts, upper_ns));
                self.polls.fetch_add(1, Ordering::SeqCst);
                Ok(TailPage {
                    streams: vec![],
                    next: None,
                    fetched: 0,
                })
            }
        }

        let windows = Arc::new(Mutex::new(Vec::new()));
        let polls = Arc::new(AtomicU32::new(0));
        let fetcher = WindowRecorder {
            windows: Arc::clone(&windows),
            polls: Arc::clone(&polls),
        };
        let mut cfg = test_cfg();
        // A backlog: start 10 slices in the past.
        cfg.slice_ns = 1_000_000_000; // 1s slices
        cfg.start_ns = params::now_ns() - 10_000_000_000;
        let (tx, rx) = watch::channel(false);
        let sender = FakeSender::new(SinkMode::Accept);
        let handle = tokio::spawn(run_tail(
            fetcher,
            sender,
            FakeReceiver(RecvMode::Silent),
            cfg,
            rx,
        ));
        // Wait until the backlog has been walked (≥ 10 polls).
        let deadline = Instant::now() + Duration::from_secs(2);
        while polls.load(Ordering::SeqCst) < 10 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tx.send(true).expect("receiver alive");
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("returns")
            .expect("no panic");
        let windows = windows.lock().unwrap();
        assert!(windows.len() >= 10, "backlog drained one slice per poll");
        for (lower, upper) in windows.iter() {
            assert!(
                upper - lower <= cfg.slice_ns,
                "poll window {lower}..{upper} exceeds the slice"
            );
        }
        // Consecutive slices resume where the previous one ended (the
        // watermark advanced despite every slice being empty).
        for pair in windows.windows(2).take(8) {
            assert_eq!(pair[1].0, pair[0].1, "lower must be the prior upper");
        }
    }

    // -- bare-GET rejection pin (the manifest's mounting oracle) ---------

    /// Pins the status axum's `WebSocketUpgrade` extractor returns for a
    /// plain GET with no upgrade headers — the conformance matrix's
    /// mounted-route oracle for the tail path (an unmounted path is an
    /// empty 404 instead). Verified empirically here so the manifest can
    /// never drift from axum's actual behavior.
    #[tokio::test]
    async fn bare_get_without_upgrade_headers_is_the_pinned_rejection_status() {
        use axum::Router;
        use axum::body::Body;
        use axum::http::Request;
        use axum::routing::get;
        use pulsus_config::Config;
        use std::sync::OnceLock;
        use tokio::sync::RwLock;
        use tower::ServiceExt;

        let state = AppState {
            pool: Arc::new(RwLock::new(None)),
            config: Arc::new(Config::default()),
            metrics: metrics_exporter_prometheus::PrometheusBuilder::new()
                .build_recorder()
                .handle(),
            build: crate::app::BuildInfo::from_build_env(),
            writer: Arc::new(crate::ingest::WriterSink::new(Arc::new(OnceLock::new()))),
            metric_writer: Arc::new(crate::ingest::MetricWriterSink::new(Arc::new(
                OnceLock::new(),
            ))),
            trace_writer: Arc::new(crate::ingest::TraceWriterSink::new(Arc::new(
                OnceLock::new(),
            ))),
            label_cache: Arc::new(OnceLock::new()),
            eval_gate: Arc::new(pulsus_read::EvalGate::new(
                pulsus_config::Config::default()
                    .reader
                    .query_eval_concurrency,
            )),
            started_at: std::time::SystemTime::now(),
            tail: Arc::new(crate::app::TailRuntime::for_tests()),
        };
        let router = Router::new()
            .route("/api/logs/v1/tail", get(tail))
            .with_state(state);
        let res = router
            .oneshot(
                Request::builder()
                    .uri("/api/logs/v1/tail?query=%7Bapp%3D%22x%22%7D")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        // axum 0.8's `WebSocketUpgrade` rejection for a plain GET with no
        // `Connection: upgrade` header: `400 Bad Request` (empirically
        // pinned; the conformance manifest row's `success_status` mirrors
        // this).
        assert_eq!(res.status(), axum::http::StatusCode::BAD_REQUEST);
        // ... with a plain-text rejection body — distinguishable from an
        // unmounted path's EMPTY 404 (the conformance oracle relies on
        // both halves).
        assert_eq!(
            res.headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("text/plain; charset=utf-8")
        );
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(&body[..], b"Connection header did not include 'upgrade'");
    }

    // -- param parsing ---------------------------------------------------

    fn cfg_default() -> Config {
        Config::default()
    }

    #[test]
    fn tail_params_reject_a_metric_query() {
        let pairs = params::parse_pairs("query=count_over_time(%7Ba%3D%22x%22%7D%5B1m%5D)");
        let err = parse_tail_params(&pairs, &cfg_default()).expect_err("metric query");
        assert!(matches!(
            err,
            ApiError::Param(ParamError::MetricQueryUnsupported { endpoint: "tail" })
        ));
    }

    #[test]
    fn tail_params_reject_zero_and_non_numeric_limits_but_clamp_large_ones() {
        let base = "query=%7Ba%3D%22x%22%7D";
        let zero = params::parse_pairs(&format!("{base}&limit=0"));
        assert!(matches!(
            parse_tail_params(&zero, &cfg_default()),
            Err(ApiError::Param(ParamError::InvalidTailLimit(_)))
        ));
        let bad = params::parse_pairs(&format!("{base}&limit=abc"));
        assert!(matches!(
            parse_tail_params(&bad, &cfg_default()),
            Err(ApiError::Param(ParamError::InvalidTailLimit(_)))
        ));
        let big = params::parse_pairs(&format!("{base}&limit=999999999"));
        let p = parse_tail_params(&big, &cfg_default()).expect("clamped, not rejected");
        assert_eq!(
            p.fetch_limit,
            cfg_default().reader.tail_max_fetch_limit,
            "silently clamped at the cap"
        );
    }

    #[test]
    fn tail_params_clamp_delay_for_at_the_max_and_default_it_to_zero() {
        let base = "query=%7Ba%3D%22x%22%7D";
        let none = params::parse_pairs(base);
        assert_eq!(
            parse_tail_params(&none, &cfg_default())
                .expect("ok")
                .delay_ns,
            0
        );
        let over = params::parse_pairs(&format!("{base}&delay_for=3600"));
        let p = parse_tail_params(&over, &cfg_default()).expect("clamped");
        assert_eq!(p.delay_ns, 5_000_000_000, "clamped at tail_max_delay (5s)");
        let bad = params::parse_pairs(&format!("{base}&delay_for=soon"));
        assert!(matches!(
            parse_tail_params(&bad, &cfg_default()),
            Err(ApiError::Param(ParamError::InvalidDelayFor(_)))
        ));
    }

    #[test]
    fn tail_params_default_start_is_an_hour_ago() {
        let pairs = params::parse_pairs("query=%7Ba%3D%22x%22%7D");
        let before = params::now_ns();
        let p = parse_tail_params(&pairs, &cfg_default()).expect("ok");
        let after = params::now_ns();
        let hour = 3_600_000_000_000i64;
        assert!(p.start_ns >= before - hour && p.start_ns <= after - hour);
    }
}
