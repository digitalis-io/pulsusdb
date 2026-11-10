//! `GET /api/logs/v1/tail` — the live-tail WebSocket (issue #74,
//! docs/api.md §2.4).
//!
//! ClickHouse has no push/notify, so tail is honestly a bounded polling
//! loop: each poll runs the IDENTICAL stage-1/2/3 plan machinery and the
//! SAME `CompiledPipeline` as the query path (`LogQlEngine::tail_poll` —
//! the task-manager-ratified semantics-drift-free invariant), fetching
//! one keyset page in global composite row order; across pages each
//! stream's entries stay in timestamp order. A stream whose labels become
//! readable after the scan passed some of its lines waits outside the scan
//! until a catch-up has read its lines from `start` to the cursor, one
//! slice per line query, then joins it; no read of its lines starts
//! `TAIL_CATCH_UP_RECHECK_NS` or more after its labels were read, and it
//! joins the scan after the one read already running then returns, within
//! that read's configured query timeout (`PULSUS_QUERY_TIMEOUT`); its older
//! entries arrive after newer entries of other streams (per-stream order,
//! issue #804).
//!
//! **Cursor** (round-4 adjudication): `(scan watermark, boundary
//! cursor)`. The watermark advances to each exhausted slice's upper
//! bound unconditionally — empty slices included, so quiet backlog
//! windows drain at one slice per poll; the boundary cursor (the
//! occurrence-count keyset tuple) moves only on fetched rows and resumes
//! a `LIMIT`-split tie group exactly-once via the SQL `OFFSET`.
//!
//! **Topology** (issue #806): A producer task sends pages and a fatal
//! error, in order, over one channel to the writer, which alone owns the
//! frame queue (bounded, evicting the oldest into a drop accumulator), the
//! error, the shutdown drain and the socket. The writer's decisions are
//! the pure `ConnState`; every socket step is preceded by its deadline
//! check, and a Close is never sent after a Text that was handed over and
//! is not known to be written. The reader only reports the client's close.

use std::collections::{BTreeSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade, close_code};
use axum::extract::{RawQuery, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};

use futures::Sink;
use pulsus_config::Config;
use pulsus_logql::Expr;
use pulsus_model::Fingerprint;
use pulsus_read::{
    CatchUpPage, Direction, LogQlEngine, QueryParams, QuerySpec, ReadError, ResponseOptions,
    StreamResult, TAIL_CATCH_UP_RECHECK_NS, TAIL_REGISTRATION_GRACE_NS, TailCeiling, TailLower,
    TailPage, TailSetup,
};
use tokio::sync::{mpsc, watch};

use crate::app::AppState;

use super::encode;
use super::engine::tail_engine;
use super::error::ApiError;
use super::handlers::tenant_of;
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

fn parse_tail_params(
    pairs: &[(String, String)],
    cfg: &Config,
    retention_days: u32,
) -> Result<TailParams, ApiError> {
    let query = params::get(pairs, "query").ok_or(ParamError::MissingQuery)?;
    let expr = super::parse_logql(query)?;
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
        // `now - 1h`. `/tail` is deliberately OUTSIDE issue #406 Part
        // C's `since` widening: it has no `end` and its own retention
        // clamp below already owns the lower bound, so the nine routes
        // that read `since` are exactly the ones going through
        // `handlers::parse_bounds`.
        None => params::now_ns().saturating_sub(params::DEFAULT_SINCE_NS),
    };
    // Retention clamp (issue #94 item 1): raise an ancient first-page
    // lower bound to the retention floor so catch-up never walks
    // pre-retention (already-TTL'd) history one empty slice per poll.
    // `max` only ever RAISES the floor — a legitimate recent `start`
    // (default 1h, or an explicit within-retention value) is untouched.
    let start_ns = start_ns.max(retention_floor_ns(retention_days, params::now_ns()));
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
///
/// `Clone`, not `Copy`, since issue #463: the connection's response
/// encoding flags are read ONCE at upgrade (a WebSocket has no per-frame
/// request) and travel with the config to the writer that encodes each
/// frame.
#[derive(Debug, Clone)]
struct TailLoopConfig {
    start_ns: i64,
    delay_ns: i64,
    fetch_limit: u32,
    poll_interval: Duration,
    slice_ns: i64,
    channel_depth: usize,
    send_timeout: Duration,
    dropped_sample_cap: usize,
    /// The scan-gate's registration-visibility grace (issue #94 v6-v8): a
    /// construction-site test seam, NOT an operator/request knob — the
    /// production path always gets [`TAIL_REGISTRATION_GRACE_NS`]
    /// (pinned by `narrow_grace_ns_defaults_to_the_production_constant`);
    /// only the hermetic producer tests inject a smaller value.
    narrow_grace_ns: i64,
    /// How long a joined stream may be caught up before it is admitted
    /// without the rest (issue #804): a construction-site test seam like
    /// `narrow_grace_ns`; production always gets
    /// [`TAIL_CATCH_UP_RECHECK_NS`].
    catch_up_recheck_ns: i64,
    /// `X-Loki-Response-Encoding-Flags` as received at upgrade (issue
    /// #463), echoed and acted on by every frame this connection emits.
    /// `Arc` so the per-frame clone the writer loop needs is a refcount
    /// bump rather than a `Vec<String>` copy.
    encoding_flags: Arc<[String]>,
}

impl TailLoopConfig {
    fn new(cfg: &Config, p: &TailParams, encoding_flags: Arc<[String]>) -> Self {
        TailLoopConfig {
            start_ns: p.start_ns,
            delay_ns: p.delay_ns,
            fetch_limit: p.fetch_limit,
            poll_interval: cfg.reader.tail_poll_interval.0,
            slice_ns: i64::try_from(cfg.reader.tail_catchup_slice.0.as_nanos()).unwrap_or(i64::MAX),
            channel_depth: cfg.reader.tail_channel_depth,
            send_timeout: cfg.reader.tail_send_timeout.0,
            dropped_sample_cap: cfg.reader.tail_max_entries_per_frame,
            narrow_grace_ns: TAIL_REGISTRATION_GRACE_NS,
            catch_up_recheck_ns: TAIL_CATCH_UP_RECHECK_NS,
            encoding_flags,
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
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
    ws: WebSocketUpgrade,
) -> Response {
    let tenant = match tenant_of(&headers) {
        Ok(tenant) => tenant,
        Err(e) => return e.into_response(),
    };
    let pairs = params::parse_pairs(raw.as_deref().unwrap_or(""));
    // Issue #497: the retention the database states; `503` before it is read.
    let Some(retention_days) = state.retention_days() else {
        return ApiError::PoolUnavailable.into_response();
    };
    let p = match parse_tail_params(&pairs, &state.config, retention_days) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    let engine = match tail_engine(&state, tenant).await {
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
    // Issue #463: read once, at upgrade. A WebSocket carries no
    // per-frame request, so the connection's wire shape is fixed here and
    // travels with the setup (the engine, which carries the categories)
    // and the loop config (the encoder, which advertises them).
    let encoding_flags: Arc<[String]> = params::encoding_flags(&headers).into();
    let opts = ResponseOptions {
        categorize_labels: params::wants_categorize_labels(&encoding_flags),
        partial_at_series_cap: false,
    };
    let setup = match engine.tail_setup_with(&p.expr, &qp, opts) {
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
    let cfg = TailLoopConfig::new(&state.config, &p, encoding_flags);

    ws.on_upgrade(move |socket| async move {
        use futures::StreamExt;
        // Held until run_tail returns — released exactly when the
        // connection ends, whatever the exit path.
        let _permit = permit;
        let fetcher = EngineFetcher { engine, setup };
        let (sink, stream) = socket.split();
        run_tail(
            fetcher,
            SinkSender(sink),
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
/// `narrow` is the producer's scan-gate certification (issue #94 v6-v8,
/// computed ONCE per iteration in `producer_loop` — see its doc) —
/// threaded through so `EngineFetcher` can forward it to
/// `tail_refresh_months`; the fakes below ignore it.
trait TailFetcher: Send + 'static {
    fn poll(
        &mut self,
        lower: TailLower,
        upper_ns: i64,
        narrow: bool,
        fetch_limit: u32,
    ) -> impl Future<Output = Result<TailPage, ReadError>> + Send;

    /// S4 (issue #804): each of `fps` with a line in `(after_ns,
    /// ceiling.ts()]`, with its earliest one.
    fn catch_up_first_lines(
        &mut self,
        _fps: &[Fingerprint],
        _after_ns: i64,
        _ceiling: TailCeiling,
    ) -> impl Future<Output = Result<Vec<(Fingerprint, i64)>, ReadError>> + Send {
        async { Ok(Vec::new()) }
    }

    /// S5 (issue #804): one catch-up page over `fps`.
    fn catch_up(
        &mut self,
        _fps: &[Fingerprint],
        _lower: TailLower,
        _upper_ns: i64,
        _ceiling: TailCeiling,
        _fetch_limit: u32,
    ) -> impl Future<Output = Result<CatchUpPage, ReadError>> + Send {
        async {
            Ok(CatchUpPage {
                page: TailPage {
                    streams: Vec::new(),
                    next: None,
                    fetched: 0,
                    joined: Vec::new(),
                },
                fetched_fps: Vec::new(),
            })
        }
    }

    /// Admits `fps` to the main scan (issue #804).
    fn admit(&mut self, _fps: &[Fingerprint]) {}

    /// Test seam: the deadline the producer gave the streams a poll
    /// reported joined.
    #[cfg(test)]
    fn deadline_set(&mut self, _deadline: Instant) {}

    /// Issue #803: called on a failed poll. `true` when the poll met a
    /// server failure and the fetcher has moved to another healthy server,
    /// so the producer may repeat the poll once at the same lower bound.
    fn take_server_failure(&mut self) -> bool {
        false
    }
}

struct EngineFetcher {
    engine: LogQlEngine,
    setup: TailSetup,
}

/// The poll window's lower instant, matching [`ScanState::lower`]'s own
/// bound exactly (`Start{start_ns}` for a first/resumed slice, the
/// keyset tuple's instant for a resumed page) — issue #94's bounded
/// month refresh anchors stage-1 to this ADVANCING lower edge, not the
/// connection's fixed setup floor.
fn tail_lower_ts(lower: TailLower) -> i64 {
    match lower {
        TailLower::Start { start_ns } => start_ns,
        TailLower::After(c) => c.tuple.0,
    }
}

impl TailFetcher for EngineFetcher {
    async fn poll(
        &mut self,
        lower: TailLower,
        upper_ns: i64,
        narrow: bool,
        fetch_limit: u32,
    ) -> Result<TailPage, ReadError> {
        // Best-effort, scan-gated month refresh (issue #94 v6-v8): the
        // scan set is anchored to `setup.scan_floor_ns` (frozen during
        // catch-up/fall-behind, advancing only when `narrow` certifies a
        // completed full-span live-edge poll already dwelt >= GRACE) — a
        // pure `plan::plan` string rebuild, NO ClickHouse round-trip.
        // Narrowing past a fingerprint's registration month is safe only
        // because `tail_poll` merges every poll's stage-1 result into a
        // cumulative cache (`TailSetup::resolved`) rather than replacing
        // it. Swallowed on failure (`let _`): the tail continues on the
        // PRIOR plan — a re-plan error can never tear down a working
        // connection. The mutable borrow ends before `tail_poll`'s own
        // mutable borrow begins.
        let lower_ns = tail_lower_ts(lower);
        let _ = self
            .engine
            .tail_refresh_months(&mut self.setup, lower_ns, upper_ns, narrow);
        self.engine
            .tail_poll(&mut self.setup, lower, upper_ns, fetch_limit)
            .await
    }

    async fn catch_up_first_lines(
        &mut self,
        fps: &[Fingerprint],
        after_ns: i64,
        ceiling: TailCeiling,
    ) -> Result<Vec<(Fingerprint, i64)>, ReadError> {
        let first = self
            .engine
            .tail_first_lines(&self.setup, fps, after_ns, ceiling)
            .await;
        accepted(&self.engine, first)
    }

    async fn catch_up(
        &mut self,
        fps: &[Fingerprint],
        lower: TailLower,
        upper_ns: i64,
        ceiling: TailCeiling,
        fetch_limit: u32,
    ) -> Result<CatchUpPage, ReadError> {
        let caught = self
            .engine
            .tail_catch_up(&self.setup, fps, lower, upper_ns, ceiling, fetch_limit)
            .await;
        accepted(&self.engine, caught)
    }

    fn admit(&mut self, fps: &[Fingerprint]) {
        self.setup.admit(fps);
    }

    /// Issue #803: the poll's server failed and another is healthy — the
    /// engine moves to it, and the producer repeats the poll there.
    fn take_server_failure(&mut self) -> bool {
        match self.engine.server_failure() {
            Some(f) if self.engine.other_healthy(f.endpoint) => {
                // Never back to the server that failed (code review r1).
                self.engine.rebind_avoiding(Some(f.endpoint));
                true
            }
            _ => false,
        }
    }
}

/// Issue #803: a catch-up read is accepted only when its server did not
/// fail during it; an `Ok` read that met a server failure becomes the
/// failure's error, so the producer repeats it rather than using it.
fn accepted<T>(engine: &LogQlEngine, read: Result<T, ReadError>) -> Result<T, ReadError> {
    let read = read?;
    match engine.server_failure() {
        Some(f) => Err(ReadError::Clickhouse(f.into())),
        None => Ok(read),
    }
}

/// Issue #803: the producer's reads that have failed once and are being run
/// a second time. A second failure of a marked read closes the tail.
#[derive(Debug, Default)]
struct FailedOver {
    poll: bool,
    waiting_probe: bool,
    catch_up_probe: bool,
    catch_up_page: bool,
}

/// Issue #803: whether a failed read may be repeated once, at once, on the
/// server the fetcher has moved to — never a third time.
fn may_fail_over<F: TailFetcher>(fetcher: &mut F, failed_over: &mut bool) -> bool {
    if !*failed_over && fetcher.take_server_failure() {
        *failed_over = true;
        true
    } else {
        false
    }
}

/// The outbound half of the split socket, in the three steps of a Text
/// send (issue #806): `ready`, `start`, `flush`. The writer checks the
/// step's deadline before each, and marks a Text half sent from `start`
/// until `flush` returns, so it never sends a Close that would first write
/// the rest of it. [`SinkSender`] in production, a scripted socket in the
/// tests.
trait TailSender: Send + 'static {
    /// Waits until the socket can take a frame. Hands nothing over, so it
    /// can be cancelled.
    fn ready(&mut self) -> impl Future<Output = Result<(), ()>> + Send;
    /// Hands a Text frame to the socket. From the call on, the frame may be
    /// wholly or partly handed over, whatever this returns.
    fn start(&mut self, text: String) -> Result<(), ()>;
    /// Writes what was handed over.
    fn flush(&mut self) -> impl Future<Output = Result<(), ()>> + Send;
    /// Takes the ALREADY-RENDERED close frame (issue #240 re-review):
    /// `writer_loop` maps the reason through [`error_close_frame`]
    /// BEFORE this boundary, so the hermetic sender fake captures the
    /// frame the production wiring produced — the render seam is
    /// pinned, and the impls are pure transport with no rendering
    /// decision left to bypass.
    fn send_close(&mut self, frame: Option<CloseFrame>) -> impl Future<Output = ()> + Send;
}

/// The inbound half (review round 1, medium): a dedicated reader task
/// awaits `closed()` independently of the writer, so a client Close (or
/// transport error) sets the cancellation even while the writer waits on
/// a backpressured socket step, which then stops, releasing the producer
/// and the connection permit at once instead of after
/// `tail_send_timeout`.
trait TailReceiver: Send + 'static {
    /// Resolves when the client sent Close or the transport ended/errored
    /// (other inbound frames — ping/pong/text — are ignored).
    fn closed(&mut self) -> impl Future<Output = ()> + Send;
}

/// The production sender over any WebSocket `Sink` (the socket's
/// `SplitSink` in production). Every sink error becomes `Err(())`.
struct SinkSender<S>(S);

impl<S: Sink<Message> + Unpin + Send + 'static> SinkSender<S> {
    async fn ready_inner(&mut self) -> Result<(), ()> {
        std::future::poll_fn(|cx| Pin::new(&mut self.0).poll_ready(cx))
            .await
            .map_err(|_| ())
    }

    async fn flush_inner(&mut self) -> Result<(), ()> {
        std::future::poll_fn(|cx| Pin::new(&mut self.0).poll_flush(cx))
            .await
            .map_err(|_| ())
    }
}

impl<S: Sink<Message> + Unpin + Send + 'static> TailSender for SinkSender<S> {
    async fn ready(&mut self) -> Result<(), ()> {
        self.ready_inner().await
    }

    fn start(&mut self, text: String) -> Result<(), ()> {
        Pin::new(&mut self.0)
            .start_send(Message::Text(text.into()))
            .map_err(|_| ())
    }

    async fn flush(&mut self) -> Result<(), ()> {
        self.flush_inner().await
    }

    /// `ready`, `start_send(Close)`, then `flush`, as `SinkExt::send` does.
    async fn send_close(&mut self, frame: Option<CloseFrame>) {
        if self.ready_inner().await.is_err() {
            return;
        }
        if Pin::new(&mut self.0)
            .start_send(Message::Close(frame))
            .is_err()
        {
            return;
        }
        let _ = self.flush_inner().await;
    }
}

/// Renders an error close reason into the wire close frame (issue #240
/// R4). Called by `writer_loop` on the SHARED path ahead of the
/// `TailSender` boundary, so the hermetic end-to-end test observes this
/// function's output through `send_close` (re-review: deleting that
/// wiring turns the captured close bare and reddens the suite). A
/// WebSocket close reason is capped at 123 bytes; truncate on a char
/// boundary. (The truncation itself is a documented, deliberately
/// retained limitation — docs/api.md §2.3.)
fn error_close_frame(mut reason: String) -> CloseFrame {
    let mut cut = reason.len().min(123);
    while !reason.is_char_boundary(cut) {
        cut -= 1;
    }
    reason.truncate(cut);
    CloseFrame {
        code: close_code::ERROR,
        reason: reason.into(),
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
#[derive(Debug)]
struct PoppedFrame {
    streams: Vec<StreamResult>,
    dropped: Vec<Dropped>,
    dropped_total: u64,
}

impl PoppedFrame {
    fn encode(self, encoding_flags: &[String]) -> String {
        encode::tail_frame(
            self.streams,
            &self.dropped,
            self.dropped_total,
            encoding_flags,
        )
    }
}

/// What the producer sends the writer, in order, over one channel (issue
/// #806): pages, then at most one fatal error; the channel's close is the
/// end.
#[derive(Debug)]
enum Event {
    Page(Vec<StreamResult>),
    Fatal(String),
}

/// What [`EventSource::try_next`] found.
#[derive(Debug)]
enum TryNext {
    Event(Event),
    Empty,
    Closed,
}

/// The writer's input (issue #806): the producer's channel in production,
/// a scripted source in the tests.
trait EventSource: Send {
    fn try_next(&mut self) -> TryNext;
    fn next(&mut self) -> impl Future<Output = Option<Event>> + Send;
}

impl EventSource for mpsc::Receiver<Event> {
    fn try_next(&mut self) -> TryNext {
        match self.try_recv() {
            Ok(ev) => TryNext::Event(ev),
            Err(mpsc::error::TryRecvError::Empty) => TryNext::Empty,
            Err(mpsc::error::TryRecvError::Disconnected) => TryNext::Closed,
        }
    }

    fn next(&mut self) -> impl Future<Output = Option<Event>> + Send {
        self.recv()
    }
}

/// The writer's next step, decided by [`ConnState::decide`] (issue #806).
#[derive(Debug)]
enum Action {
    Send {
        frame: PoppedFrame,
        deadline: Instant,
    },
    Wait {
        until: Option<Instant>,
    },
    Close(Option<String>),
    Drop,
}

/// The writer's state and decisions (issue #806), with no I/O.
#[derive(Debug)]
struct ConnState {
    queue: FrameBuf,
    fatal: Option<String>,
    /// The channel closed (after `Fatal`, or the producer returned).
    producer_done: bool,
    drain_until: Option<Instant>,
    /// The client closed, or a step failed or passed its deadline.
    must_stop: bool,
    /// A Text was handed to the sink and is not known to be written.
    half_sent: bool,
    send_timeout: Duration,
}

impl ConnState {
    fn new(depth: usize, dropped_sample_cap: usize, send_timeout: Duration) -> Self {
        ConnState {
            queue: FrameBuf::new(depth, dropped_sample_cap),
            fatal: None,
            producer_done: false,
            drain_until: None,
            must_stop: false,
            half_sent: false,
            send_timeout,
        }
    }

    /// An event from the producer; `None`: its channel closed. A page goes
    /// into the queue, evicting the oldest frame past the depth.
    fn on_event(&mut self, ev: Option<Event>) {
        match ev {
            Some(Event::Page(streams)) => self.queue.push_evicting(streams),
            Some(Event::Fatal(reason)) => {
                self.fatal = Some(reason);
                self.producer_done = true;
            }
            None => self.producer_done = true,
        }
    }

    /// Shutdown reached the connection: the drain ends `CLOSE_GRACE` after
    /// it was first read.
    fn on_shutdown(&mut self, now: Instant) {
        if self.drain_until.is_none() {
            self.drain_until = Some(now + CLOSE_GRACE);
        }
    }

    fn on_must_stop(&mut self) {
        self.must_stop = true;
    }

    fn on_handed_over(&mut self) {
        self.half_sent = true;
    }

    fn on_written(&mut self) {
        self.half_sent = false;
    }

    /// The next step. The first case that applies: the connection must
    /// stop; the shutdown drain has run out; a queued frame is sent; the
    /// producer is done; otherwise wait. Every end is a Close, unless a
    /// Text is half sent, which drops the socket.
    fn decide(&mut self, now: Instant) -> Action {
        let end = |s: &mut Self, reason: Option<String>| {
            if s.half_sent {
                Action::Drop
            } else {
                Action::Close(reason)
            }
        };
        if self.must_stop {
            return end(self, None);
        }
        if self.drain_until.is_some_and(|d| now >= d) {
            let reason = self.fatal.take();
            return end(self, reason);
        }
        if let Some(frame) = self.queue.pop_next() {
            let normal = now + self.send_timeout;
            return Action::Send {
                frame,
                deadline: self.drain_until.map_or(normal, |d| normal.min(d)),
            };
        }
        if self.producer_done {
            let reason = self.fatal.take();
            return end(self, reason);
        }
        Action::Wait {
            until: self.drain_until,
        }
    }
}

/// Reads the inputs already visible into `st` before a decision (issue
/// #806).
#[allow(clippy::too_many_arguments)]
fn observe_inputs<E: EventSource>(
    st: &mut ConnState,
    shutdown: &mut watch::Receiver<bool>,
    shutdown_open: &mut bool,
    cancel: &watch::Receiver<bool>,
    src: &mut E,
    rx_open: &mut bool,
    now: Instant,
) {
    if *cancel.borrow() {
        st.on_must_stop();
    }
    if *shutdown_open {
        match shutdown.has_changed() {
            // The sender is gone: that is shutdown too.
            Err(_) => {
                *shutdown_open = false;
                st.on_shutdown(now);
            }
            Ok(_) => {
                if *shutdown.borrow_and_update() {
                    st.on_shutdown(now);
                }
            }
        }
    }
    // At most one event per decision, so a source that is never empty
    // cannot delay a decision.
    if *rx_open {
        match src.try_next() {
            TryNext::Event(ev) => st.on_event(Some(ev)),
            TryNext::Closed => {
                *rx_open = false;
                st.on_event(None);
            }
            TryNext::Empty => {}
        }
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

/// Producer poll loop (issue #94 v6-v8: the ONLY place the scan-gate
/// `narrow` flag is decided, from a SINGLE per-iteration `now` read —
/// recomputing `now`/`horizon` downstream (e.g. inside a `TailFetcher`)
/// is a documented trap: a fresh clock read always sees `horizon`
/// advance past `upper`, misclassifying every steady-state live poll as
/// catch-up and silently reintroducing lifetime-unbounded stage-1 month
/// growth).
///
/// **Scan-gate rule:** `live = upper >= horizon` (the slice cap did not
/// bind — the cursor sits within one `tail_catchup_slice` of the
/// horizon). `live_since_ns` is set on the first live poll and RESET
/// (along with `qualifying_scan_done`) on any non-live poll — a
/// fall-behind episode re-arms the gate. `narrow = live &&
/// qualifying_scan_done` is consumed by THIS poll's refresh (refresh
/// runs before the poll — set-after-scan, consume-next-refresh).
/// `dwell_ok` (the poll's START-time dwell, not its duration) is
/// evaluated BEFORE the poll; `qualifying_scan_done` is set to `true`
/// only AFTER the poll returns `Ok` (a completed full-span stage-1 scan —
/// an `Err` breaks the loop, so no path narrows without a completed
/// qualifying scan).
async fn producer_loop<F: TailFetcher>(
    mut fetcher: F,
    events: mpsc::Sender<Event>,
    cfg: TailLoopConfig,
    mut shutdown: watch::Receiver<bool>,
    mut cancel: watch::Receiver<bool>,
) {
    let mut state = ScanState::new(cfg.start_ns);
    let mut live_since_ns: Option<i64> = None;
    let mut qualifying_scan_done = false;
    // Issue #804: streams whose labels were read after the first window,
    // each with its deadline, wait outside the main scan until a catch-up
    // has read their lines up to the scan's position.
    let recheck = Duration::from_nanos(u64::try_from(cfg.catch_up_recheck_ns).unwrap_or(0));
    let mut waiting: Vec<(Fingerprint, Instant)> = Vec::new();
    let mut next_probe = Instant::now();
    // Joined streams not yet probed: their first probe runs at once, before
    // any idle sleep, whatever the recheck timer says.
    let mut unprobed: Vec<Fingerprint> = Vec::new();
    let mut catch_up: Option<CatchUp> = None;
    // Issue #803: which reads are being run a second time because their
    // server failed. Each read has its own mark, cleared only when that read
    // is accepted, so no read runs more than twice: an accepted read of
    // another kind in between does not give a failed one a third run (code
    // review r1).
    let mut failed_over = FailedOver::default();
    loop {
        if *shutdown.borrow() || *cancel.borrow() {
            break;
        }
        if let Some(cu) = catch_up.as_mut() {
            expire(&mut fetcher, &mut cu.fps, &mut waiting);
            let mut close = cu.fps.is_empty();
            if !close {
                let fps: Vec<Fingerprint> = cu.fps.iter().map(|(fp, _)| *fp).collect();
                match cu.scan {
                    None => {
                        let result = tokio::select! {
                            _ = cancelled(&mut shutdown, &mut cancel) => break,
                            r = fetcher.catch_up_first_lines(&fps, cu.probe_after, cu.ceiling) => r,
                        };
                        match result {
                            Ok(first) => {
                                failed_over.catch_up_probe = false;
                                match first.iter().map(|(_, ts)| *ts).min() {
                                    Some(min_first) => {
                                        cu.scan = Some(ScanState::new(min_first.saturating_sub(1)));
                                    }
                                    None => close = true,
                                }
                            }
                            Err(e) => {
                                // Issue #803: nothing moved; the probe is
                                // repeated once on the server moved to.
                                if may_fail_over(&mut fetcher, &mut failed_over.catch_up_probe) {
                                    continue;
                                }
                                let _ = send_fatal(&events, e, &mut shutdown, &mut cancel).await;
                                break;
                            }
                        }
                    }
                    Some(mut scan) => {
                        let (lower, lower_ts) = scan.lower();
                        let upper = cu.ceiling.ts().min(lower_ts.saturating_add(cfg.slice_ns));
                        let result = tokio::select! {
                            _ = cancelled(&mut shutdown, &mut cancel) => break,
                            r = fetcher.catch_up(&fps, lower, upper, cu.ceiling, cfg.fetch_limit) => r,
                        };
                        let caught = match result {
                            Ok(caught) => {
                                failed_over.catch_up_page = false;
                                caught
                            }
                            Err(e) => {
                                // Issue #803: the slice's scan did not move;
                                // it is read again once on the server moved
                                // to.
                                if may_fail_over(&mut fetcher, &mut failed_over.catch_up_page) {
                                    continue;
                                }
                                let _ = send_fatal(&events, e, &mut shutdown, &mut cancel).await;
                                break;
                            }
                        };
                        cu.fetched.extend(caught.fetched_fps);
                        let exhausted = scan.observe(&caught.page, upper, cfg.fetch_limit);
                        if !send_page(&events, caught.page.streams, &mut shutdown, &mut cancel)
                            .await
                        {
                            break;
                        }
                        if !exhausted {
                            cu.scan = Some(scan);
                        } else if upper >= cu.ceiling.ts() {
                            close = true;
                        } else {
                            cu.probe_after = upper;
                            cu.scan = None;
                        }
                    }
                }
            }
            if !close {
                // No idle sleep while a catch-up is open.
                continue;
            }
            // Closing: admit the streams a page fetched from; the rest
            // stay waiting with their deadlines, and the next recheck
            // waits for its timer.
            if let Some(cu) = catch_up.take() {
                // A later catch-up's reads are other reads.
                failed_over.catch_up_probe = false;
                failed_over.catch_up_page = false;
                let admitted: Vec<Fingerprint> = cu
                    .fps
                    .iter()
                    .map(|(fp, _)| *fp)
                    .filter(|fp| cu.fetched.contains(fp))
                    .collect();
                if !admitted.is_empty() {
                    fetcher.admit(&admitted);
                    waiting.retain(|(fp, _)| !admitted.contains(fp));
                }
            }
        } else if !waiting.is_empty() {
            let mut all = std::mem::take(&mut waiting);
            expire(&mut fetcher, &mut all, &mut waiting);
            waiting = all;
            unprobed.retain(|fp| waiting.iter().any(|(w, _)| w == fp));
            let first_probe = !unprobed.is_empty();
            if !waiting.is_empty() && (first_probe || Instant::now() >= next_probe) {
                let ceiling = TailCeiling::of(state.lower().0);
                let fps: Vec<Fingerprint> = if first_probe {
                    std::mem::take(&mut unprobed)
                } else {
                    waiting.iter().map(|(fp, _)| *fp).collect()
                };
                let result = tokio::select! {
                    _ = cancelled(&mut shutdown, &mut cancel) => break,
                    r = fetcher.catch_up_first_lines(&fps, cfg.start_ns, ceiling) => r,
                };
                if result.is_err() && may_fail_over(&mut fetcher, &mut failed_over.waiting_probe) {
                    // Issue #803: the probe left nothing changed — its
                    // streams are still unprobed and the recheck timer has
                    // not moved — and it runs again at once on the server
                    // moved to.
                    if first_probe {
                        unprobed = fps;
                    }
                    continue;
                }
                // A first probe never brings the next recheck forward.
                next_probe = next_probe.max(Instant::now() + cfg.poll_interval);
                match result {
                    Ok(first) => {
                        failed_over.waiting_probe = false;
                        if let Some(min_first) = first.iter().map(|(_, ts)| *ts).min() {
                            let found: BTreeSet<Fingerprint> =
                                first.iter().map(|(fp, _)| *fp).collect();
                            catch_up = Some(CatchUp {
                                fps: waiting
                                    .iter()
                                    .filter(|(fp, _)| found.contains(fp))
                                    .copied()
                                    .collect(),
                                ceiling,
                                probe_after: cfg.start_ns,
                                scan: Some(ScanState::new(min_first.saturating_sub(1))),
                                fetched: BTreeSet::new(),
                            });
                            continue;
                        }
                    }
                    Err(e) => {
                        let _ = send_fatal(&events, e, &mut shutdown, &mut cancel).await;
                        break;
                    }
                }
            }
        }
        let now = params::now_ns();
        let horizon = now.saturating_sub(cfg.delay_ns);
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
        let live = upper >= horizon;
        if live {
            live_since_ns.get_or_insert(now);
        } else {
            // Fall-behind: re-arm the gate — the drain that follows must
            // earn a fresh qualifying full-span scan before narrowing
            // again.
            live_since_ns = None;
            qualifying_scan_done = false;
        }
        let narrow = live && qualifying_scan_done;
        let dwell_ok =
            live && now.saturating_sub(live_since_ns.unwrap_or(now)) >= cfg.narrow_grace_ns;

        let result = tokio::select! {
            _ = cancelled(&mut shutdown, &mut cancel) => break,
            r = fetcher.poll(lower, upper, narrow, cfg.fetch_limit) => r,
        };
        let mut page = match result {
            Ok(page) => {
                failed_over.poll = false;
                page
            }
            Err(e) => {
                // Issue #803: the poll's server failed and the fetcher has
                // moved to another. The poll sent no rows and the cursor did
                // not move, so the same lower bound is polled again at once
                // — once.
                if may_fail_over(&mut fetcher, &mut failed_over.poll) {
                    continue;
                }
                // Fatal poll failure (e.g. a broad selector tripping the
                // stream cap, or a second server failure in a row): hand the
                // reason to the writer, which closes the socket with it.
                let _ = send_fatal(&events, e, &mut shutdown, &mut cancel).await;
                break;
            }
        };
        // Read after the poll has returned, so a slow poll does not use up
        // a joined stream's time.
        let deadline = Instant::now() + recheck;
        #[cfg(test)]
        if !page.joined.is_empty() {
            fetcher.deadline_set(deadline);
        }
        unprobed.extend(page.joined.iter().copied());
        waiting.extend(
            std::mem::take(&mut page.joined)
                .into_iter()
                .map(|fp| (fp, deadline)),
        );
        // This poll's stage-1 COMPLETED; if it started at dwell >= grace
        // it was the qualifying full-span scan (narrow was false when it
        // ran) — narrowing is permitted from the NEXT refresh only.
        if dwell_ok {
            qualifying_scan_done = true;
        }
        let exhausted = state.observe(&page, upper, cfg.fetch_limit);
        if !send_page(&events, page.streams, &mut shutdown, &mut cancel).await {
            break;
        }
        // Caught up ⇒ idle for one poll interval; otherwise (full page,
        // or more backlog slices ahead) re-poll immediately.
        if exhausted
            && live
            && unprobed.is_empty()
            && sleep_or_cancelled(&mut shutdown, &mut cancel, cfg.poll_interval).await
        {
            break;
        }
    }
    // Returning drops the event sender: the writer reads that as the
    // end of the producer.
}

/// One open catch-up (issue #804): the streams it reads, each with its
/// deadline, up to `ceiling`, the main scan's position when it opened.
struct CatchUp {
    fps: Vec<(Fingerprint, Instant)>,
    ceiling: TailCeiling,
    /// The probe's lower bound.
    probe_after: i64,
    /// `None`: the probe runs next.
    scan: Option<ScanState>,
    /// Every stream a page fetched a row from.
    fetched: BTreeSet<Fingerprint>,
}

/// Run before every probe and every catch-up page (issue #804): admits the
/// entries of `list` whose deadline has passed, at the main scan's current
/// position, and removes them from `list` and from `waiting`. Their lines
/// below that position that no read started before the deadline read are
/// never sent.
fn expire<F: TailFetcher>(
    fetcher: &mut F,
    list: &mut Vec<(Fingerprint, Instant)>,
    waiting: &mut Vec<(Fingerprint, Instant)>,
) {
    let now = Instant::now();
    let expired: Vec<Fingerprint> = list
        .iter()
        .filter(|(_, deadline)| *deadline <= now)
        .map(|(fp, _)| *fp)
        .collect();
    if expired.is_empty() {
        return;
    }
    fetcher.admit(&expired);
    list.retain(|(fp, _)| !expired.contains(fp));
    waiting.retain(|(fp, _)| !expired.contains(fp));
}

/// Sends a page's streams to the writer, when any carries an entry,
/// racing shutdown and the cancellation (issue #806); `false` when the
/// producer must stop.
async fn send_page(
    events: &mpsc::Sender<Event>,
    streams: Vec<StreamResult>,
    shutdown: &mut watch::Receiver<bool>,
    cancel: &mut watch::Receiver<bool>,
) -> bool {
    if !streams.iter().any(|s| !s.entries.is_empty()) {
        return true;
    }
    tokio::select! {
        _ = cancelled(shutdown, cancel) => false,
        r = events.send(Event::Page(streams)) => r.is_ok(),
    }
}

/// A fatal read failure: sends the reason to the writer, after every page
/// sent before it, racing shutdown and the cancellation (issue #806).
async fn send_fatal(
    events: &mpsc::Sender<Event>,
    e: ReadError,
    shutdown: &mut watch::Receiver<bool>,
    cancel: &mut watch::Receiver<bool>,
) -> bool {
    tokio::select! {
        _ = cancelled(shutdown, cancel) => false,
        r = events.send(Event::Fatal(e.to_string())) => r.is_ok(),
    }
}

/// The inbound-reader task: waits for the client to close (a Close frame
/// or the transport ending) and sets the cancellation then, and on
/// nothing else. It stops when the writer sets the cancellation.
async fn reader_loop<R: TailReceiver>(
    mut receiver: R,
    mut cancel: watch::Receiver<bool>,
    cancel_tx: Arc<watch::Sender<bool>>,
) {
    tokio::select! {
        _ = cancel.wait_for(|stop| *stop) => {}
        _ = receiver.closed() => {
            let _ = cancel_tx.send(true);
        }
    }
}

/// The writer's inputs, borrowed by its waits.
struct Inputs<'a, E> {
    st: &'a mut ConnState,
    src: &'a mut E,
    rx_open: &'a mut bool,
    shutdown: &'a mut watch::Receiver<bool>,
    shutdown_open: &'a mut bool,
    cancel: &'a mut watch::Receiver<bool>,
}

impl<E: EventSource> Inputs<'_, E> {
    /// A shutdown change: shutdown when the value is `true` or the sender
    /// is gone.
    fn on_shutdown_changed(&mut self, changed: Result<(), watch::error::RecvError>) {
        let now = Instant::now();
        match changed {
            Err(_) => {
                *self.shutdown_open = false;
                self.st.on_shutdown(now);
            }
            Ok(()) => {
                if *self.shutdown.borrow_and_update() {
                    self.st.on_shutdown(now);
                }
            }
        }
    }

    fn on_received(&mut self, ev: Option<Event>) {
        if ev.is_none() {
            *self.rx_open = false;
        }
        self.st.on_event(ev);
    }

    /// Runs one socket step until it ends or `deadline`. The order, in
    /// every wait: the connection having to stop, the deadline, shutdown
    /// (which lowers the deadline to the drain's), the step, then a
    /// receive, taken only while the step is pending. `false`: the step
    /// failed, was cancelled or ran out of time.
    async fn step<F>(&mut self, fut: F, mut deadline: Instant) -> bool
    where
        F: Future<Output = Result<(), ()>>,
    {
        tokio::pin!(fut);
        loop {
            let (shutdown_open, rx_open) = (*self.shutdown_open, *self.rx_open);
            let got = tokio::select! {
                biased;
                _ = self.cancel.wait_for(|stop| *stop) => Got::Stop,
                _ = tokio::time::sleep_until(deadline) => Got::Stop,
                changed = self.shutdown.changed(), if shutdown_open => Got::Shutdown(changed),
                done = before(deadline, &mut fut) => done.map_or(Got::Stop, |r| Got::Step(r.is_ok())),
                ev = self.src.next(), if rx_open => Got::Event(ev),
            };
            match got {
                Got::Stop => return false,
                Got::Step(ok) => return ok,
                Got::Shutdown(changed) => {
                    self.on_shutdown_changed(changed);
                    if let Some(d) = self.st.drain_until {
                        deadline = deadline.min(d);
                    }
                }
                Got::Event(ev) => self.on_received(ev),
                Got::Woke => {}
            }
        }
    }

    /// Waits for one input, or until `until`.
    async fn wait(&mut self, until: Option<Instant>) {
        let (shutdown_open, rx_open) = (*self.shutdown_open, *self.rx_open);
        let got = tokio::select! {
            biased;
            _ = self.cancel.wait_for(|stop| *stop) => Got::Stop,
            _ = sleep_until_some(until) => Got::Woke,
            changed = self.shutdown.changed(), if shutdown_open => Got::Shutdown(changed),
            ev = self.src.next(), if rx_open => Got::Event(ev),
        };
        match got {
            Got::Stop => self.st.on_must_stop(),
            Got::Shutdown(changed) => self.on_shutdown_changed(changed),
            Got::Event(ev) => self.on_received(ev),
            Got::Woke | Got::Step(_) => {}
        }
    }
}

/// Polls `fut` only while the clock is before `deadline`: `None` once it
/// is not. The timer wakes on whole milliseconds and can still be pending
/// just after the deadline, so the step is guarded by the clock itself.
async fn before<F: Future + Unpin>(deadline: Instant, fut: &mut F) -> Option<F::Output> {
    std::future::poll_fn(|cx| {
        if Instant::now() >= deadline {
            return std::task::Poll::Ready(None);
        }
        Pin::new(&mut *fut).poll(cx).map(Some)
    })
    .await
}

/// What ended one of the writer's waits.
enum Got {
    /// The connection must stop, or the step's deadline passed.
    Stop,
    /// The wait's own deadline.
    Woke,
    Shutdown(Result<(), watch::error::RecvError>),
    Step(bool),
    Event(Option<Event>),
}

/// Sleeps until `until`, or forever when there is none.
async fn sleep_until_some(until: Option<Instant>) {
    match until {
        Some(t) => tokio::time::sleep_until(t).await,
        None => std::future::pending().await,
    }
}

/// The writer (issue #806): the one owner of the frame queue, the fatal
/// error, the shutdown drain and the socket. Each pass reads the inputs
/// already visible, then carries out [`ConnState::decide`]'s decision.
async fn writer_loop<S: TailSender, E: EventSource>(
    mut sender: S,
    mut src: E,
    cfg: TailLoopConfig,
    mut shutdown: watch::Receiver<bool>,
    mut cancel: watch::Receiver<bool>,
    cancel_tx: Arc<watch::Sender<bool>>,
) {
    let mut st = ConnState::new(cfg.channel_depth, cfg.dropped_sample_cap, cfg.send_timeout);
    let (mut shutdown_open, mut rx_open) = (true, true);
    loop {
        observe_inputs(
            &mut st,
            &mut shutdown,
            &mut shutdown_open,
            &cancel,
            &mut src,
            &mut rx_open,
            Instant::now(),
        );
        let action = st.decide(Instant::now());
        let mut inputs = Inputs {
            st: &mut st,
            src: &mut src,
            rx_open: &mut rx_open,
            shutdown: &mut shutdown,
            shutdown_open: &mut shutdown_open,
            cancel: &mut cancel,
        };
        match action {
            Action::Drop => break,
            Action::Close(reason) => {
                // The error reason is rendered into the wire frame here,
                // so every `TailSender` receives the production renderer's
                // output (issue #240 re-review).
                let frame = reason.map(error_close_frame);
                let _ = tokio::time::timeout(CLOSE_GRACE, sender.send_close(frame)).await;
                break;
            }
            Action::Wait { until } => inputs.wait(until).await,
            Action::Send { frame, deadline } => {
                let text = frame.encode(&cfg.encoding_flags);
                if !inputs.step(sender.ready(), deadline).await {
                    inputs.st.on_must_stop();
                    continue;
                }
                // A shutdown read while waiting for readiness lowers the
                // deadline of what follows to the drain's.
                let deadline = inputs.st.drain_until.map_or(deadline, |d| deadline.min(d));
                // Checked immediately before handing over: nothing is
                // handed over at or after a deadline.
                if Instant::now() >= deadline || *inputs.cancel.borrow() {
                    inputs.st.on_must_stop();
                    continue;
                }
                inputs.st.on_handed_over();
                if sender.start(text).is_err() {
                    inputs.st.on_must_stop();
                    continue;
                }
                if inputs.step(sender.flush(), deadline).await {
                    inputs.st.on_written();
                } else {
                    inputs.st.on_must_stop();
                }
            }
        }
    }
    let _ = cancel_tx.send(true);
}

/// Runs one tail connection to completion: spawns the producer and the
/// inbound reader, runs the writer on the upgrade task, and joins both on
/// the way out. The writer sets the cancellation when it returns, which
/// stops the other two.
async fn run_tail<F: TailFetcher, S: TailSender, R: TailReceiver>(
    fetcher: F,
    sender: S,
    receiver: R,
    cfg: TailLoopConfig,
    shutdown: watch::Receiver<bool>,
) {
    let (events, rx) = mpsc::channel(1);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let cancel_tx = Arc::new(cancel_tx);
    let producer = tokio::spawn(producer_loop(
        fetcher,
        events,
        cfg.clone(),
        shutdown.clone(),
        cancel_rx.clone(),
    ));
    let reader = tokio::spawn(reader_loop(
        receiver,
        cancel_rx.clone(),
        Arc::clone(&cancel_tx),
    ));
    writer_loop(sender, rx, cfg, shutdown, cancel_rx, cancel_tx).await;
    let _ = producer.await;
    let _ = reader.await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulsus_model::Fingerprint;

    use std::sync::Mutex;

    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Instant;

    use pulsus_read::TailCursor;

    fn stream(labels: &str, entries: Vec<(i64, &str)>) -> StreamResult {
        StreamResult {
            fingerprint: Fingerprint::from_raw(1),
            service: "checkout".to_string(),
            labels_json: labels.to_string(),
            entries: entries
                .into_iter()
                .map(|(ts, line)| (ts, line.to_string()))
                .collect(),
            categories: Vec::new(),
        }
    }

    /// Drives a fatal `PipelineInvalid { reason }` through the REAL
    /// producer→writer path (`run_tail`) and returns the close frame
    /// the PRODUCTION wiring rendered and handed to the sender
    /// (`writer_loop`'s `close_reason.map(error_close_frame)` — the
    /// fake only records what it was handed). Deliberately NEVER calls
    /// `error_close_frame`: every assertion made on the returned frame
    /// pins the production render, not the helper (issue #240
    /// re-review 2).
    async fn production_close_frame(reason: &str) -> CloseFrame {
        let (_tx, rx) = watch::channel(false);
        let sender = WireSender::new();
        let closed = Arc::clone(&sender.closed);
        let handle = tokio::spawn(run_tail(
            PipelineInvalidFetcher(reason.to_string()),
            sender,
            FakeReceiver(RecvMode::Silent),
            test_cfg(),
            rx,
        ));
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("a fatal poll error must terminate the connection")
            .expect("no panic");
        closed
            .lock()
            .unwrap()
            .clone()
            .expect("close was sent")
            .expect("close carried a frame rendered by the production path")
    }

    /// Issue #240 (R4, re-review): a `PipelineInvalid` poll failure
    /// closes the socket through the REAL producer→writer path, and the
    /// captured `CloseFrame` carries the ERROR code and the BARE reason
    /// bytes unchanged (no fixed prefix; a reason that itself begins
    /// `parse error ` appears exactly once, and a short reason is never
    /// truncated). Deleting the `writer_loop` map turns the close bare
    /// and reddens this.
    #[tokio::test(flavor = "multi_thread")]
    async fn pipeline_invalid_closes_the_socket_with_the_bare_reason_through_the_renderer() {
        let reason = "parse error : synthetic prefix-collision probe";
        let frame = production_close_frame(reason).await;
        assert_eq!(frame.code, close_code::ERROR);
        assert_eq!(frame.reason.as_str(), reason);
        assert_eq!(frame.reason.as_str().matches("parse error ").count(), 1);
    }

    /// Issue #240 (re-review 2): the 123-byte close-reason cap is
    /// applied ON THE PRODUCTION PATH — a long reason driven through
    /// `run_tail` reaches the socket already cut to exactly 123 bytes,
    /// and an exactly-123-byte reason passes through byte-identical
    /// (the cap is inclusive). This test never invokes
    /// `error_close_frame`, so replacing the `writer_loop` map with an
    /// inline untruncated frame — or moving the cap in either
    /// direction — reddens it.
    #[tokio::test(flavor = "multi_thread")]
    async fn long_reason_through_run_tail_reaches_the_socket_truncated_at_123_bytes() {
        let frame = production_close_frame(&"a".repeat(200)).await;
        assert_eq!(frame.code, close_code::ERROR);
        assert_eq!(frame.reason.as_str(), "a".repeat(123));
        // Inclusive boundary: exactly 123 bytes is NOT cut.
        let exact = "y".repeat(123);
        let frame = production_close_frame(&exact).await;
        assert_eq!(frame.reason.as_str(), exact);
    }

    /// Issue #240 (re-review 2): the char-boundary back-off is applied
    /// on the production path — a reason whose 3-byte '€' straddles the
    /// 123-byte cap reaches the socket cut at the 121-byte boundary, a
    /// valid UTF-8 prefix, never a split code point. (Deleting the
    /// boundary loop panics `String::truncate` inside `writer_loop`,
    /// reddening this through the joined task.)
    #[tokio::test(flavor = "multi_thread")]
    async fn multibyte_reason_through_run_tail_is_cut_on_a_char_boundary() {
        let long = format!("{}€ tail beyond the close-frame cap", "x".repeat(121));
        let frame = production_close_frame(&long).await;
        assert_eq!(frame.code, close_code::ERROR);
        assert_eq!(frame.reason.as_str(), "x".repeat(121));
    }

    /// Issue #240 (re-review 2, property enumeration): the frame's
    /// PRESENCE cuts both ways — an error-free shutdown closes with a
    /// BARE close (`None`: no code, no reason on the wire). Rendering a
    /// frame unconditionally would tag every graceful close with the
    /// ERROR code; this pins the `close_reason.map(..)` staying a map.
    #[tokio::test(flavor = "multi_thread")]
    async fn graceful_shutdown_closes_bare_with_no_frame() {
        let (tx, rx) = watch::channel(false);
        let sender = WireSender::new();
        let closed = Arc::clone(&sender.closed);
        let handle = tokio::spawn(run_tail(
            FrameEveryPoll,
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
        let closed = closed.lock().unwrap().clone().expect("close was sent");
        assert!(
            closed.is_none(),
            "a graceful close must carry no frame, got {closed:?}"
        );
    }

    /// Issue #240 (R4): the production renderer's 123-byte close-reason
    /// cap truncates on a char boundary — the frame carries a valid
    /// UTF-8 PREFIX of the reason, never a split code point. (The cap
    /// itself is the documented retained limitation — docs/api.md §2.3.)
    #[test]
    fn error_close_frame_truncates_at_123_bytes_on_a_char_boundary() {
        // 121 ASCII bytes, then a 3-byte '€' straddling the 123-byte
        // cap: the cut backs off to the 121-byte boundary.
        let long = format!("{}€ tail beyond the close-frame cap", "x".repeat(121));
        assert!(long.len() > 123);
        let frame = error_close_frame(long.clone());
        assert_eq!(frame.code, close_code::ERROR);
        assert_eq!(frame.reason.as_str(), "x".repeat(121));
        assert!(long.starts_with(frame.reason.as_str()));
        // At or under the cap the reason passes through byte-identical.
        let exact = "y".repeat(123);
        assert_eq!(error_close_frame(exact.clone()).reason.as_str(), exact);
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
            // Production default (issue #94 v6-v8): individual scan-gate
            // tests override this with an injected grace via the same
            // construction-site seam.
            narrow_grace_ns: TAIL_REGISTRATION_GRACE_NS,
            catch_up_recheck_ns: TAIL_CATCH_UP_RECHECK_NS,
            encoding_flags: Vec::new().into(),
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
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            "log_samples",
            &[pulsus_read::logql::predicate::literal("checkout")],
            &[Fingerprint::from_raw(1).sql_literal()],
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

    /// Issue #133: the pre-SQL fetch clamp still fires at the maximum
    /// config-accepted `reader.tail_max_fetch_limit` — a client limit one
    /// past the ceiling cap is clamped, and the ceiling itself passes
    /// through unclamped. The guard is not disable-able by any value
    /// config load accepts.
    #[test]
    fn fetch_clamp_still_fires_at_the_max_accepted_cap() {
        let cap = pulsus_config::TAIL_MAX_FETCH_LIMIT_CEILING;
        assert_eq!(clamped_fetch_limit(u64::from(cap) + 1, cap), cap);
        assert_eq!(clamped_fetch_limit(u64::from(cap), cap), cap);
    }

    /// Issue #133: the frame-buffer eviction still fires at the maximum
    /// config-accepted `reader.tail_channel_depth` — ceiling+1 pushes
    /// into a ceiling-deep buffer retain exactly `ceiling` frames with
    /// the one evicted (oldest) frame's entry absorbed into the exact
    /// drop accounting. 4_001 one-entry pushes — trivially CI-cheap.
    #[test]
    fn frame_eviction_still_fires_at_the_max_accepted_channel_depth() {
        let depth = pulsus_config::TAIL_CHANNEL_DEPTH_CEILING;
        let mut buf = FrameBuf::new(depth, 8);
        for i in 0..=(depth as i64) {
            buf.push_evicting(vec![stream(r#"{"app":"x"}"#, vec![(i, "line")])]);
        }
        assert_eq!(
            buf.frames.len(),
            depth,
            "the buffer must hold exactly the depth ceiling"
        );
        assert_eq!(buf.dropped.total, 1, "exactly one frame's entry evicted");
        assert_eq!(
            buf.dropped.sample[0].timestamp_ns, 0,
            "the OLDEST frame is the one evicted"
        );
    }

    /// Issue #133 AC10: `DroppedAcc` at the maximum config-accepted
    /// `reader.tail_max_entries_per_frame` — absorbing ceiling+1 entries
    /// retains exactly `ceiling` sampled rows while the cumulative total
    /// stays exact at ceiling+1. The sample bound is not disable-able by
    /// any value config load accepts.
    #[test]
    fn dropped_sample_bound_still_fires_at_the_max_accepted_entries_per_frame() {
        let cap = pulsus_config::TAIL_MAX_ENTRIES_PER_FRAME_CEILING;
        let mut acc = DroppedAcc {
            sample: VecDeque::new(),
            total: 0,
            cap,
        };
        // One entry per absorb — bounds transient memory to the retained
        // sample itself (never an O(ceiling) input Vec on top of it).
        for i in 0..=(cap as i64) {
            acc.absorb(&[stream(r#"{"app":"x"}"#, vec![(i, "line")])]);
        }
        assert_eq!(acc.sample.len(), cap, "sample bounded at the ceiling");
        assert_eq!(acc.total, cap as u64 + 1, "cumulative count stays exact");
        // Drop-oldest: the retained sample starts at the second entry.
        assert_eq!(acc.sample[0].timestamp_ns, 1);
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
        let p = parse_tail_params(&pairs, &cfg_default(), 7).expect("ok");
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
        // start=0 ⇒ clamped to ~now − 8 days (the 7d retention passed + slack).
        let base = "query=%7Ba%3D%22x%22%7D";
        let before = params::now_ns();
        let ancient = params::parse_pairs(&format!("{base}&start=0"));
        let p = parse_tail_params(&ancient, &cfg, 7).expect("ok");
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
        let p = parse_tail_params(&recent_pairs, &cfg, 7).expect("ok");
        assert_eq!(p.start_ns, recent, "a within-retention start is untouched");

        // The default start (1h ago) is likewise within retention.
        let default_pairs = params::parse_pairs(base);
        let before = params::now_ns();
        let p = parse_tail_params(&default_pairs, &cfg, 7).expect("ok");
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
            joined: Vec::new(),
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
            tuple: (250, Fingerprint::from_raw(7), 42),
            seen: 3,
        };
        let page = TailPage {
            streams: vec![],
            next: Some(cursor),
            fetched: 10,
            joined: Vec::new(),
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
                tuple: (300, Fingerprint::from_raw(1), 1),
                seen: 1,
            }),
            fetched: 4,
            joined: Vec::new(),
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
            tuple: (500, Fingerprint::from_raw(1), 1),
            seen: 2,
        };
        let page = TailPage {
            streams: vec![],
            next: Some(at_upper),
            fetched: 4,
            joined: Vec::new(),
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
        let first = parse(&buf.pop_next().expect("frame 5").encode(&[]));
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

        let second = parse(&buf.pop_next().expect("frame 6").encode(&[]));
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
        let frame = parse(&buf.pop_next().expect("synthesized frame").encode(&[]));
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
            _narrow: bool,
            _fetch_limit: u32,
        ) -> Result<TailPage, ReadError> {
            Ok(TailPage {
                streams: vec![stream(r#"{"app":"x"}"#, vec![(upper_ns, "line")])],
                next: None,
                fetched: 1,
                joined: Vec::new(),
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
            _narrow: bool,
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
            _narrow: bool,
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

    /// A fetcher that fails with a LogQL pipeline rejection (issue #240
    /// R4 — the close-reason construction path).
    struct PipelineInvalidFetcher(String);
    impl TailFetcher for PipelineInvalidFetcher {
        async fn poll(
            &mut self,
            _lower: TailLower,
            _upper_ns: i64,
            _narrow: bool,
            _fetch_limit: u32,
        ) -> Result<TailPage, ReadError> {
            Err(ReadError::PipelineInvalid {
                reason: self.0.clone(),
            })
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum RecvMode {
        /// Inbound side stays silent forever.
        Silent,
        /// The client sends a Close after ~50ms.
        CloseSoon,
    }

    type TInstant = tokio::time::Instant;

    /// One event on a [`WireSender`]'s log (issue #806). A scripted source
    /// writes its reads into the same log.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Wire {
        ReadyStart,
        Ready,
        Start(String),
        FlushStart,
        Flushed,
        CloseStart,
        Closed,
        /// A [`GatedFloodSource`] read: `try_next` (`false`) or `next` (`true`).
        Read(bool),
    }

    type WireLog = Arc<Mutex<Vec<(TInstant, Wire)>>>;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ReadyMode {
        Ready,
        /// Waits until the test releases [`WireSender::ready_gate`].
        Gate,
        Pending,
        Fail,
        Sleep(Duration),
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum StartMode {
        Ok,
        /// Buffers the text, then fails.
        BufferThenErr,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum FlushMode {
        Ready,
        YieldOnce,
        /// Waits until the test releases [`WireSender::gate`].
        Gate,
        Sleep(Duration),
        Pending,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum CloseMode {
        Ready,
        Block,
    }

    /// The scripted socket (issue #806): `start` buffers a Text, `flush`
    /// moves the buffer to `wire`, and every call is logged with its
    /// instant.
    struct WireSender {
        ready_mode: ReadyMode,
        start_mode: StartMode,
        flush_modes: VecDeque<FlushMode>,
        flush_default: FlushMode,
        close_mode: CloseMode,
        log: WireLog,
        buffered: Arc<Mutex<Vec<String>>>,
        wire: Arc<Mutex<Vec<String>>>,
        /// Outer `Some` = a close was sent; inner = the wire frame the
        /// PRODUCTION path rendered (`writer_loop` maps through
        /// `error_close_frame` before the `TailSender` boundary — issue
        /// #240 re-review: the fake captures, never re-renders).
        closed: Arc<Mutex<Option<Option<CloseFrame>>>>,
        gate: Arc<tokio::sync::Notify>,
        ready_gate: Arc<tokio::sync::Notify>,
    }

    impl WireSender {
        fn new() -> Self {
            WireSender {
                ready_mode: ReadyMode::Ready,
                start_mode: StartMode::Ok,
                flush_modes: VecDeque::new(),
                flush_default: FlushMode::Ready,
                close_mode: CloseMode::Ready,
                log: Arc::new(Mutex::new(Vec::new())),
                buffered: Arc::new(Mutex::new(Vec::new())),
                wire: Arc::new(Mutex::new(Vec::new())),
                closed: Arc::new(Mutex::new(None)),
                gate: Arc::new(tokio::sync::Notify::new()),
                ready_gate: Arc::new(tokio::sync::Notify::new()),
            }
        }

        fn ready(mut self, m: ReadyMode) -> Self {
            self.ready_mode = m;
            self
        }

        fn start(mut self, m: StartMode) -> Self {
            self.start_mode = m;
            self
        }

        fn flush(mut self, m: FlushMode) -> Self {
            self.flush_default = m;
            self
        }

        /// The modes of the first flushes, in order; `flush`'s mode after.
        fn flushes(mut self, ms: &[FlushMode]) -> Self {
            self.flush_modes = ms.iter().copied().collect();
            self
        }

        fn close(mut self, m: CloseMode) -> Self {
            self.close_mode = m;
            self
        }

        fn note(&self, w: Wire) {
            self.log.lock().unwrap().push((TInstant::now(), w));
        }

        fn write_buffered(&self) {
            let mut b = self.buffered.lock().unwrap();
            self.wire.lock().unwrap().extend(b.drain(..));
        }
    }

    impl TailSender for WireSender {
        async fn ready(&mut self) -> Result<(), ()> {
            self.note(Wire::ReadyStart);
            match self.ready_mode {
                ReadyMode::Ready => {}
                ReadyMode::Gate => self.ready_gate.notified().await,
                ReadyMode::Pending => std::future::pending::<()>().await,
                ReadyMode::Fail => return Err(()),
                ReadyMode::Sleep(d) => tokio::time::sleep(d).await,
            }
            self.note(Wire::Ready);
            Ok(())
        }

        fn start(&mut self, text: String) -> Result<(), ()> {
            self.note(Wire::Start(text.clone()));
            self.buffered.lock().unwrap().push(text);
            match self.start_mode {
                StartMode::Ok => Ok(()),
                StartMode::BufferThenErr => Err(()),
            }
        }

        async fn flush(&mut self) -> Result<(), ()> {
            self.note(Wire::FlushStart);
            let mode = self.flush_modes.pop_front().unwrap_or(self.flush_default);
            match mode {
                FlushMode::Ready => {}
                FlushMode::YieldOnce => tokio::task::yield_now().await,
                FlushMode::Gate => self.gate.notified().await,
                FlushMode::Sleep(d) => tokio::time::sleep(d).await,
                FlushMode::Pending => std::future::pending::<()>().await,
            }
            self.write_buffered();
            self.note(Wire::Flushed);
            Ok(())
        }

        async fn send_close(&mut self, frame: Option<CloseFrame>) {
            self.note(Wire::CloseStart);
            if self.close_mode == CloseMode::Block {
                std::future::pending::<()>().await;
            }
            self.write_buffered();
            *self.closed.lock().unwrap() = Some(frame);
            self.note(Wire::Closed);
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

    /// A client that closes when the test says so (issue #806).
    struct SignalReceiver(Arc<tokio::sync::Notify>);

    impl TailReceiver for SignalReceiver {
        async fn closed(&mut self) {
            self.0.notified().await;
        }
    }

    /// Issue #74 AC4 (v3 rewrite, cancellation-not-timeout): shutdown
    /// while the writer's send is blocked on a non-reading client — with
    /// a 60s send timeout, `run_tail` must return well under 1s, proving
    /// CANCELLATION unblocked it, not timeout expiry.
    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_under_blocked_send_returns_within_the_close_grace_not_the_send_timeout() {
        let (tx, rx) = watch::channel(false);
        let sender = WireSender::new().ready(ReadyMode::Pending);
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
        let sender = WireSender::new();
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
        let sender = WireSender::new().ready(ReadyMode::Fail);
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
        let sender = WireSender::new();
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
    /// writer's socket step is blocked on backpressure must still tear
    /// the connection down promptly — the independent inbound reader
    /// fires the shared cancellation, which the in-flight send races —
    /// releasing the producer AND the connection-slot permit in well
    /// under the 60s send timeout (and with no shutdown signal at all).
    #[tokio::test(flavor = "multi_thread")]
    async fn client_close_during_blocked_send_releases_producer_and_permit_promptly() {
        let (_tx, rx) = watch::channel(false);
        let sender = WireSender::new().ready(ReadyMode::Pending);
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
        let sender = WireSender::new();
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
        let frame = closed
            .lock()
            .unwrap()
            .clone()
            .expect("close was sent")
            .expect("close carried a reason");
        let reason = frame.reason.as_str();
        assert!(reason.contains("query too broad"), "{reason}");
    }

    /// Frames flow end to end through the loop (sanity for the fakes):
    /// the writer receives encoded frames with `dropped_total: 0`.
    #[tokio::test(flavor = "multi_thread")]
    async fn frames_flow_from_producer_to_writer() {
        let (tx, rx) = watch::channel(false);
        let sender = WireSender::new();
        let sent = Arc::clone(&sender.wire);
        let handle = tokio::spawn(run_tail(
            FrameEveryPoll,
            sender,
            FakeReceiver(RecvMode::Silent),
            test_cfg(),
            rx,
        ));
        // Shut down once a frame has been sent (10 ms steps, at most 10 s):
        // a fixed wait can end before a loaded machine sends one.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while sent.lock().unwrap().is_empty() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
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
                _narrow: bool,
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
                    joined: Vec::new(),
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
        let sender = WireSender::new();
        let handle = tokio::spawn(run_tail(
            fetcher,
            sender,
            FakeReceiver(RecvMode::Silent),
            cfg.clone(),
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

    // -- scan gate (issue #94 v6-v8, U6/U7) -------------------------------

    /// A full-page response mode for [`NarrowRecorder`] (issue #94 v6-v8,
    /// U6(e)/U7).
    #[derive(Clone, Copy)]
    enum FullPageMode {
        /// The cursor tracks the poll's OWN `upper_ns` exactly — a
        /// perpetually-full ingest that never drains its own poll window
        /// (`ScanState` advances the watermark by one slice per poll,
        /// forward-monotonic, exactly as a real backlog drain would) —
        /// U7.
        TracksUpper,
        /// The cursor is pinned at a fixed, already-stale `ts` — because
        /// `ScanState`'s watermark only ever advances (`max`), a stale
        /// cursor is a no-op for it: the watermark FREEZES at whatever it
        /// last was, stranding the connection behind the live horizon —
        /// a deterministic fall-behind episode (U6(e)).
        Frozen(i64),
    }

    /// Records each poll's `narrow` flag; optionally sleeps `poll_sleep`
    /// inside `poll` (U6(b): a poll's own DURATION must not qualify it —
    /// only the START-time dwell does) before returning a page per
    /// `full_page`'s mode (`None` = exhausted-empty, the default
    /// "instant" fake). A minimum 1ms per-poll floor bounds the busy-spin
    /// rate during non-live/catch-up phases (no idle-sleep applies there)
    /// — keeps iteration counts and timing predictable and avoids
    /// `std::sync::Mutex` contention with the test driver thread.
    struct NarrowRecorder {
        narrows: Arc<Mutex<Vec<bool>>>,
        poll_sleep: Duration,
        full_page: Arc<Mutex<Option<FullPageMode>>>,
    }
    impl TailFetcher for NarrowRecorder {
        async fn poll(
            &mut self,
            _lower: TailLower,
            upper_ns: i64,
            narrow: bool,
            fetch_limit: u32,
        ) -> Result<TailPage, ReadError> {
            self.narrows.lock().unwrap().push(narrow);
            tokio::time::sleep(self.poll_sleep.max(Duration::from_millis(1))).await;
            let mode = *self.full_page.lock().unwrap();
            match mode {
                Some(FullPageMode::TracksUpper) => Ok(TailPage {
                    streams: vec![],
                    next: Some(TailCursor {
                        tuple: (upper_ns, Fingerprint::from_raw(0), 0),
                        seen: 0,
                    }),
                    fetched: fetch_limit,
                    joined: Vec::new(),
                }),
                Some(FullPageMode::Frozen(ts)) => Ok(TailPage {
                    streams: vec![],
                    next: Some(TailCursor {
                        tuple: (ts, Fingerprint::from_raw(0), 0),
                        seen: 0,
                    }),
                    fetched: fetch_limit,
                    joined: Vec::new(),
                }),
                None => Ok(TailPage {
                    streams: vec![],
                    next: None,
                    fetched: 0,
                    joined: Vec::new(),
                }),
            }
        }
    }

    fn narrow_recorder(poll_sleep: Duration) -> (NarrowRecorder, Arc<Mutex<Vec<bool>>>) {
        let narrows = Arc::new(Mutex::new(Vec::new()));
        let fetcher = NarrowRecorder {
            narrows: Arc::clone(&narrows),
            poll_sleep,
            full_page: Arc::new(Mutex::new(None)),
        };
        (fetcher, narrows)
    }

    /// U6(a) (issue #94 v6-v8): `poll_interval` alone exceeding `GRACE`
    /// must NOT narrow at poll 2 — the scan gate requires a COMPLETED
    /// qualifying full-span scan, not merely elapsed wall clock (the exact
    /// kill of the v6 time-gated rule, codex FAIL 5025191315). Poll 1 has
    /// dwell 0; poll 2 runs at dwell >= 200ms > grace yet the flag is
    /// still unset at ITS decision time, so it too is full-span; poll 3
    /// narrows only once poll 2's scan has completed.
    #[tokio::test(flavor = "multi_thread")]
    async fn narrow_gate_requires_a_completed_scan_when_poll_interval_exceeds_grace() {
        let (fetcher, narrows) = narrow_recorder(Duration::ZERO);
        let mut cfg = test_cfg();
        cfg.narrow_grace_ns = 50_000_000; // 50ms
        cfg.poll_interval = Duration::from_millis(200);
        let (tx, rx) = watch::channel(false);
        let sender = WireSender::new();
        let handle = tokio::spawn(run_tail(
            fetcher,
            sender,
            FakeReceiver(RecvMode::Silent),
            cfg,
            rx,
        ));
        let deadline = Instant::now() + Duration::from_secs(5);
        while narrows.lock().unwrap().len() < 3 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tx.send(true).expect("receiver alive");
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("returns")
            .expect("no panic");
        let recorded = narrows.lock().unwrap();
        assert!(
            recorded.len() >= 3,
            "expected >= 3 polls, got {}",
            recorded.len()
        );
        assert_eq!(
            &recorded[..3],
            &[false, false, true],
            "poll 2's dwell exceeds grace but must still be full-span (the flag is unset at \
             decision time); poll 3 narrows only after poll 2's scan completed: {recorded:?}"
        );
    }

    /// U6(b) (issue #94 v6-v8): a poll's own IN-FLIGHT duration exceeding
    /// grace must not qualify it — only the START-time dwell counts. Poll
    /// 1 starts at dwell 0 (even though it then sleeps 150ms); poll 2 is
    /// the qualifying full-span scan (its start dwell is >= grace); poll 3
    /// narrows.
    #[tokio::test(flavor = "multi_thread")]
    async fn narrow_gate_uses_start_time_dwell_not_a_poll_own_duration() {
        let (fetcher, narrows) = narrow_recorder(Duration::from_millis(150));
        let mut cfg = test_cfg();
        cfg.narrow_grace_ns = 50_000_000; // 50ms
        cfg.poll_interval = Duration::from_millis(5);
        let (tx, rx) = watch::channel(false);
        let sender = WireSender::new();
        let handle = tokio::spawn(run_tail(
            fetcher,
            sender,
            FakeReceiver(RecvMode::Silent),
            cfg,
            rx,
        ));
        let deadline = Instant::now() + Duration::from_secs(5);
        while narrows.lock().unwrap().len() < 3 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tx.send(true).expect("receiver alive");
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("returns")
            .expect("no panic");
        let recorded = narrows.lock().unwrap();
        assert!(
            recorded.len() >= 3,
            "expected >= 3 polls, got {}",
            recorded.len()
        );
        assert_eq!(
            &recorded[..3],
            &[false, false, true],
            "a poll's own 150ms duration must not qualify it — only start-time dwell: {recorded:?}"
        );
    }

    /// U6(c) (issue #94 v6-v8): under normal cadence, at least TWO
    /// full-span live polls always precede the first narrowed poll (a
    /// theorem of the rule for grace > 0 — poll 1's dwell is 0, and the
    /// flag is set only after a completed poll).
    #[tokio::test(flavor = "multi_thread")]
    async fn narrow_gate_opens_after_at_least_two_full_span_live_polls_under_normal_cadence() {
        let (fetcher, narrows) = narrow_recorder(Duration::ZERO);
        let mut cfg = test_cfg();
        cfg.narrow_grace_ns = 50_000_000; // 50ms
        cfg.poll_interval = Duration::from_millis(5);
        let (tx, rx) = watch::channel(false);
        let sender = WireSender::new();
        let handle = tokio::spawn(run_tail(
            fetcher,
            sender,
            FakeReceiver(RecvMode::Silent),
            cfg,
            rx,
        ));
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if narrows.lock().unwrap().iter().any(|&n| n) {
                break;
            }
            assert!(Instant::now() < deadline, "narrow never opened within 5s");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tx.send(true).expect("receiver alive");
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("returns")
            .expect("no panic");
        let recorded = narrows.lock().unwrap();
        let first_true = recorded
            .iter()
            .position(|&n| n)
            .expect("some poll narrowed");
        assert!(
            first_true >= 2,
            "at least two full-span live polls must precede narrowing, got index \
             {first_true}: {recorded:?}"
        );
        assert!(
            recorded[..first_true].iter().all(|&n| !n),
            "every poll before the first narrowed one must be false: {recorded:?}"
        );
    }

    /// U6(d) (issue #94 v6-v8): every catch-up poll (a slice-bounded
    /// backlog drain) is `narrow = false` — the scan gate never opens
    /// while behind, no matter how small `narrow_grace_ns` is.
    #[tokio::test(flavor = "multi_thread")]
    async fn narrow_gate_never_opens_during_slice_bounded_catchup() {
        let (fetcher, narrows) = narrow_recorder(Duration::ZERO);
        let mut cfg = test_cfg();
        cfg.narrow_grace_ns = 1_000_000; // 1ms — even a trivial grace must not fire
        cfg.slice_ns = 1_000_000_000; // 1s slices
        cfg.start_ns = params::now_ns() - 10_000_000_000; // a 10-slice backlog
        let (tx, rx) = watch::channel(false);
        let sender = WireSender::new();
        let handle = tokio::spawn(run_tail(
            fetcher,
            sender,
            FakeReceiver(RecvMode::Silent),
            cfg,
            rx,
        ));
        let deadline = Instant::now() + Duration::from_secs(2);
        while narrows.lock().unwrap().len() < 10 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tx.send(true).expect("receiver alive");
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("returns")
            .expect("no panic");
        let recorded = narrows.lock().unwrap();
        assert!(recorded.len() >= 10, "backlog drained one slice per poll");
        assert!(
            recorded.iter().all(|&n| !n),
            "catch-up polls must never narrow, even with a trivial grace: {recorded:?}"
        );
    }

    /// U6(e) (issue #94 v6-v8): falling behind RE-ARMS the gate — after a
    /// fall-behind episode, the connection must re-earn further full-span
    /// polls before narrowing again (a state-not-reset regression would
    /// narrow again immediately on re-entry).
    #[tokio::test(flavor = "multi_thread")]
    async fn narrow_gate_rearms_after_falling_behind_and_reopens_only_after_further_live_polls() {
        let narrows = Arc::new(Mutex::new(Vec::new()));
        let full_page = Arc::new(Mutex::new(None));
        let fetcher = NarrowRecorder {
            narrows: Arc::clone(&narrows),
            poll_sleep: Duration::ZERO,
            full_page: Arc::clone(&full_page),
        };
        let mut cfg = test_cfg();
        cfg.narrow_grace_ns = 50_000_000; // 50ms
        cfg.poll_interval = Duration::from_millis(5);
        cfg.slice_ns = 10_000_000; // 10ms slices — a short re-catchup after falling behind
        let (tx, rx) = watch::channel(false);
        let sender = WireSender::new();
        let handle = tokio::spawn(run_tail(
            fetcher,
            sender,
            FakeReceiver(RecvMode::Silent),
            cfg,
            rx,
        ));

        // Phase 1: reach narrow=true under normal cadence.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if narrows.lock().unwrap().iter().any(|&n| n) {
                break;
            }
            assert!(Instant::now() < deadline, "narrow never opened within 5s");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // Phase 2: fall behind — full pages FROZEN 10s stale. Because
        // `ScanState`'s watermark only ever advances (`max`), a stale
        // cursor deterministically STRANDS it (upper < horizon ⇒ live =
        // false) for as long as the mode holds — re-arming the gate. Held
        // for 50ms of real time.
        *full_page.lock().unwrap() = Some(FullPageMode::Frozen(params::now_ns() - 10_000_000_000));
        tokio::time::sleep(Duration::from_millis(50)).await;
        let release_idx = {
            *full_page.lock().unwrap() = None;
            narrows.lock().unwrap().len()
        };

        // Phase 3: released — the connection re-drains the (small,
        // ~50ms / 10ms-slice) gap and must re-earn further full-span/live
        // polls before narrowing again.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let reopened = {
                let recorded = narrows.lock().unwrap();
                recorded.len() > release_idx && recorded[release_idx..].iter().any(|&n| n)
            };
            if reopened {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "narrow never reopened after re-arm within 5s"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tx.send(true).expect("receiver alive");
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("returns")
            .expect("no panic");

        let recorded = narrows.lock().unwrap();
        let post_release = &recorded[release_idx..];
        let first_true = post_release
            .iter()
            .position(|&n| n)
            .expect("narrow reopened after re-arm");
        assert!(
            first_true >= 2,
            "re-arm must force at least two more polls before narrowing again, got index \
             {first_true}: {post_release:?}"
        );
        assert!(
            post_release[..first_true].iter().all(|&n| !n),
            "every poll between re-arm and the next narrow must be false: {post_release:?}"
        );
    }

    /// U7 (issue #94 v6-v8, codex finding 2): a page-lagged cursor whose
    /// `upper` never reaches the horizon (ingest outrunning `fetch_limit`)
    /// is never classified live — `narrow` stays `false`, no matter how
    /// small `narrow_grace_ns` is, so the floor can never advance past it.
    /// Deterministic BY CONSTRUCTION (not by racing wall-clock speed): a
    /// 1000-slice backlog cannot be drained within the first 100 observed
    /// polls (each poll consumes at most one slice of SIMULATED
    /// backlog), so the horizon is provably unreachable within the
    /// observation window regardless of how fast the loop executes.
    #[tokio::test(flavor = "multi_thread")]
    async fn narrow_gate_never_opens_for_a_page_lagged_cursor_that_never_reaches_the_horizon() {
        let narrows = Arc::new(Mutex::new(Vec::new()));
        let fetcher = NarrowRecorder {
            narrows: Arc::clone(&narrows),
            poll_sleep: Duration::ZERO,
            full_page: Arc::new(Mutex::new(Some(FullPageMode::TracksUpper))),
        };
        let mut cfg = test_cfg();
        cfg.narrow_grace_ns = 1_000_000; // 1ms — even a trivial grace must never fire
        cfg.slice_ns = 10_000_000; // 10ms slices
        cfg.start_ns = params::now_ns() - 1_000 * cfg.slice_ns; // a 1000-slice backlog
        let (tx, rx) = watch::channel(false);
        let sender = WireSender::new();
        let handle = tokio::spawn(run_tail(
            fetcher,
            sender,
            FakeReceiver(RecvMode::Silent),
            cfg,
            rx,
        ));
        let deadline = Instant::now() + Duration::from_secs(5);
        while narrows.lock().unwrap().len() < 100 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tx.send(true).expect("receiver alive");
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("returns")
            .expect("no panic");
        let recorded = narrows.lock().unwrap();
        assert!(
            recorded.len() >= 100,
            "expected the lagging cursor to poll repeatedly, got {}",
            recorded.len()
        );
        assert!(
            recorded.iter().all(|&n| !n),
            "a page-lagged cursor whose backlog cannot possibly be drained within the \
             observed polls must never be classified live/narrow: {recorded:?}"
        );
    }

    /// Production always gets the documented constant — the grace field is
    /// a construction-site test seam, not an operator/request knob.
    #[test]
    fn narrow_grace_ns_defaults_to_the_production_constant() {
        let pairs = params::parse_pairs("query=%7Ba%3D%22x%22%7D");
        let p = parse_tail_params(&pairs, &cfg_default(), 7).expect("ok");
        let cfg = TailLoopConfig::new(&cfg_default(), &p, Vec::new().into());
        assert_eq!(cfg.narrow_grace_ns, TAIL_REGISTRATION_GRACE_NS);
        assert_eq!(cfg.catch_up_recheck_ns, TAIL_CATCH_UP_RECHECK_NS);
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
            retention_days: std::sync::Arc::new(std::sync::OnceLock::from(7)),
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
            rollup_levels: Arc::new(pulsus_read::metrics::rollup::LevelSlot::new()),
            graph_target: pulsus_read::GraphTargetCache::default(),
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

    // -- issue #804: a stream that joins late is caught up, then admitted --

    fn fpn(n: u128) -> Fingerprint {
        Fingerprint::from_raw(n)
    }

    /// [`TailLower`] with equality, for the call log.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Lower {
        Start(i64),
        After(TailCursor),
    }

    impl Lower {
        fn of(l: TailLower) -> Self {
            match l {
                TailLower::Start { start_ns } => Lower::Start(start_ns),
                TailLower::After(c) => Lower::After(c),
            }
        }

        fn ts(self) -> i64 {
            match self {
                Lower::Start(t) => t,
                Lower::After(c) => c.tuple.0,
            }
        }
    }

    /// Every call the producer makes on [`ScriptedFetcher`], in order.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Call {
        Poll {
            lower: Lower,
            upper: i64,
        },
        /// The poll above has returned, reporting these joined streams.
        PollDone {
            joined: Vec<Fingerprint>,
        },
        FirstLines {
            fps: Vec<Fingerprint>,
            after: i64,
            ceiling: TailCeiling,
        },
        CatchUp {
            fps: Vec<Fingerprint>,
            lower: Lower,
            upper: i64,
            ceiling: TailCeiling,
        },
        Admit(Vec<Fingerprint>),
        /// The deadline the producer set for the streams just joined.
        Deadline(Instant),
    }

    type CallLog = Arc<Mutex<Vec<(Instant, Call)>>>;

    /// How the scripted fetcher answers one catch-up page.
    enum CatchStep {
        Page(CatchUpPage),
        /// Answered after this long.
        Slow(Duration, CatchUpPage),
        /// Never answered.
        Hang,
        /// Fails after this long.
        Fail(Duration, ReadError),
    }

    type PollScript = Box<dyn FnMut(usize, TailLower, i64) -> TailPage + Send>;
    type FirstScript = Box<dyn FnMut(usize, &[(Instant, Call)]) -> Vec<(Fingerprint, i64)> + Send>;
    type CatchScript = Box<dyn FnMut(usize) -> CatchStep + Send>;

    /// Answers polls, S4 and catch-up pages from scripts and records every
    /// call, `admit` included (issue #804).
    struct ScriptedFetcher {
        log: CallLog,
        polls: usize,
        firsts: usize,
        catches: usize,
        poll_script: PollScript,
        first_script: FirstScript,
        catch_script: CatchScript,
        /// Issue #803: a failed catch-up read reports a server failure the
        /// fetcher has moved off.
        failover: bool,
        last_failed: bool,
        /// Issue #803: from this poll on, every poll fails, after a delay.
        failing_polls_from: Option<(usize, Duration)>,
    }

    impl ScriptedFetcher {
        fn new(
            poll_script: impl FnMut(usize, TailLower, i64) -> TailPage + Send + 'static,
            first_script: impl FnMut(usize, &[(Instant, Call)]) -> Vec<(Fingerprint, i64)>
            + Send
            + 'static,
            catch_script: impl FnMut(usize) -> CatchStep + Send + 'static,
        ) -> Self {
            ScriptedFetcher {
                log: Arc::new(Mutex::new(Vec::new())),
                polls: 0,
                firsts: 0,
                catches: 0,
                poll_script: Box::new(poll_script),
                first_script: Box::new(first_script),
                catch_script: Box::new(catch_script),
                failover: false,
                last_failed: false,
                failing_polls_from: None,
            }
        }

        fn with_failing_polls_from(mut self, n: usize, delay: Duration) -> Self {
            self.failing_polls_from = Some((n, delay));
            self
        }

        fn with_failover(mut self) -> Self {
            self.failover = true;
            self
        }

        fn record(&self, call: Call) {
            self.log.lock().unwrap().push((Instant::now(), call));
        }
    }

    impl TailFetcher for ScriptedFetcher {
        async fn poll(
            &mut self,
            lower: TailLower,
            upper_ns: i64,
            _narrow: bool,
            _fetch_limit: u32,
        ) -> Result<TailPage, ReadError> {
            self.record(Call::Poll {
                lower: Lower::of(lower),
                upper: upper_ns,
            });
            let n = self.polls;
            self.polls += 1;
            if let Some((from, delay)) = self.failing_polls_from
                && n >= from
            {
                tokio::time::sleep(delay).await;
                self.last_failed = true;
                return Err(ReadError::PipelineInvalid {
                    reason: format!("poll {n} failed"),
                });
            }
            let page = (self.poll_script)(n, lower, upper_ns);
            self.record(Call::PollDone {
                joined: page.joined.clone(),
            });
            Ok(page)
        }

        async fn catch_up_first_lines(
            &mut self,
            fps: &[Fingerprint],
            after_ns: i64,
            ceiling: TailCeiling,
        ) -> Result<Vec<(Fingerprint, i64)>, ReadError> {
            self.record(Call::FirstLines {
                fps: fps.to_vec(),
                after: after_ns,
                ceiling,
            });
            let n = self.firsts;
            self.firsts += 1;
            let answer = {
                let log = self.log.lock().unwrap();
                (self.first_script)(n, &log)
            };
            Ok(answer)
        }

        async fn catch_up(
            &mut self,
            fps: &[Fingerprint],
            lower: TailLower,
            upper_ns: i64,
            ceiling: TailCeiling,
            _fetch_limit: u32,
        ) -> Result<CatchUpPage, ReadError> {
            self.record(Call::CatchUp {
                fps: fps.to_vec(),
                lower: Lower::of(lower),
                upper: upper_ns,
                ceiling,
            });
            let n = self.catches;
            self.catches += 1;
            match (self.catch_script)(n) {
                CatchStep::Page(p) => Ok(p),
                CatchStep::Slow(d, p) => {
                    tokio::time::sleep(d).await;
                    Ok(p)
                }
                CatchStep::Hang => std::future::pending().await,
                CatchStep::Fail(d, e) => {
                    tokio::time::sleep(d).await;
                    self.last_failed = true;
                    Err(e)
                }
            }
        }

        fn admit(&mut self, fps: &[Fingerprint]) {
            self.record(Call::Admit(fps.to_vec()));
        }

        fn take_server_failure(&mut self) -> bool {
            self.failover && std::mem::take(&mut self.last_failed)
        }

        fn deadline_set(&mut self, deadline: tokio::time::Instant) {
            self.record(Call::Deadline(deadline.into_std()));
        }
    }

    /// A main-poll page: one stream holding `lines`, reporting `joined`.
    fn lines_page(lines: &[&str], joined: &[u128]) -> TailPage {
        let streams = if lines.is_empty() {
            Vec::new()
        } else {
            vec![stream(
                r#"{"app":"x"}"#,
                lines
                    .iter()
                    .enumerate()
                    .map(|(i, l)| (i as i64 + 1, *l))
                    .collect(),
            )]
        };
        TailPage {
            streams,
            next: None,
            fetched: lines.len() as u32,
            joined: joined.iter().map(|&n| fpn(n)).collect(),
        }
    }

    /// A catch-up page of `lines`, `fetched` raw rows, cursor `next`, rows
    /// fetched from `fps`.
    fn catch_page(
        lines: &[&str],
        fetched: u32,
        next: Option<TailCursor>,
        fps: &[u128],
    ) -> CatchUpPage {
        let mut page = lines_page(lines, &[]);
        page.fetched = fetched;
        page.next = next;
        CatchUpPage {
            page,
            fetched_fps: fps.iter().map(|&n| fpn(n)).collect(),
        }
    }

    /// One running `run_tail` over a scripted fetcher.
    struct ScriptedRun {
        handle: tokio::task::JoinHandle<()>,
        shutdown: watch::Sender<bool>,
        sent: Arc<Mutex<Vec<String>>>,
        closed: Arc<Mutex<Option<Option<CloseFrame>>>>,
        log: CallLog,
    }

    impl ScriptedRun {
        fn start(fetcher: ScriptedFetcher, cfg: TailLoopConfig) -> Self {
            let log = Arc::clone(&fetcher.log);
            let (tx, rx) = watch::channel(false);
            let sender = WireSender::new();
            let sent = Arc::clone(&sender.wire);
            let closed = Arc::clone(&sender.closed);
            let handle = tokio::spawn(run_tail(
                fetcher,
                sender,
                FakeReceiver(RecvMode::Silent),
                cfg,
                rx,
            ));
            ScriptedRun {
                handle,
                shutdown: tx,
                sent,
                closed,
                log,
            }
        }

        /// Waits, in 5 ms steps, until `done` holds on the call log or
        /// `limit` passes.
        async fn wait_for(&self, limit: Duration, done: impl Fn(&[(Instant, Call)]) -> bool) {
            let deadline = Instant::now() + limit;
            while Instant::now() < deadline {
                if done(&self.log.lock().unwrap()) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }

        /// Shuts the run down; it must return within 1 s.
        async fn stop(self) -> (Vec<(Instant, Call)>, Vec<String>) {
            let _ = self.shutdown.send(true);
            tokio::time::timeout(Duration::from_secs(1), self.handle)
                .await
                .expect("run_tail must return within 1 s of shutdown")
                .expect("no panic");
            let calls = self.log.lock().unwrap().clone();
            let lines = sent_lines(&self.sent.lock().unwrap());
            (calls, lines)
        }
    }

    /// Every line the writer sent, in the order sent.
    fn sent_lines(sent: &[String]) -> Vec<String> {
        let mut out = Vec::new();
        for text in sent {
            let frame = parse(text);
            for s in frame["streams"].as_array().expect("streams") {
                for v in s["values"].as_array().expect("values") {
                    out.push(v[1].as_str().expect("line").to_string());
                }
            }
        }
        out
    }

    fn has_admit(calls: &[(Instant, Call)]) -> bool {
        calls.iter().any(|(_, c)| matches!(c, Call::Admit(_)))
    }

    /// The index of the first `PollDone` reporting a joined stream.
    fn joining_poll(calls: &[(Instant, Call)]) -> Option<usize> {
        calls
            .iter()
            .position(|(_, c)| matches!(c, Call::PollDone { joined } if !joined.is_empty()))
    }

    /// The `upper` of the poll whose `PollDone` sits at `done`.
    fn poll_upper(calls: &[(Instant, Call)], done: usize) -> i64 {
        match &calls[done - 1].1 {
            Call::Poll { upper, .. } => *upper,
            other => panic!("expected a poll before its PollDone, got {other:?}"),
        }
    }

    /// The calls after `from`, without the `PollDone` markers.
    fn statements_after(calls: &[(Instant, Call)], from: usize) -> Vec<(Instant, Call)> {
        calls[from + 1..]
            .iter()
            .filter(|(_, c)| !matches!(c, Call::PollDone { .. } | Call::Deadline(_)))
            .cloned()
            .collect()
    }

    fn count(calls: &[(Instant, Call)], pred: impl Fn(&Call) -> bool) -> usize {
        calls.iter().filter(|(_, c)| pred(c)).count()
    }

    /// The H1 script: poll 1 sends A1, poll 2 sends A2 and reports stream
    /// 2 joined, later polls are empty.
    fn h1_polls(n: usize, _l: TailLower, _u: i64) -> TailPage {
        match n {
            0 => lines_page(&["A1"], &[]),
            1 => lines_page(&["A2"], &[2]),
            _ => lines_page(&[], &[]),
        }
    }

    /// H1 (issue #804): a stream reported joined is probed, caught up from
    /// its first line to the main scan's position, and only then admitted;
    /// the main scan resumes where it stood.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_joined_stream_is_caught_up_then_admitted() {
        let cfg = test_cfg();
        let start = cfg.start_ns;
        let fetcher = ScriptedFetcher::new(
            h1_polls,
            move |_, _| vec![(fpn(2), start + 1)],
            |_| CatchStep::Page(catch_page(&["B1"], 1, None, &[2])),
        );
        let run = ScriptedRun::start(fetcher, cfg);
        run.wait_for(Duration::from_secs(3), |c| {
            c.iter()
                .position(|(_, c)| matches!(c, Call::Admit(_)))
                .is_some_and(|i| c[i..].iter().any(|(_, c)| matches!(c, Call::Poll { .. })))
        })
        .await;
        let (calls, lines) = run.stop().await;
        let joined = joining_poll(&calls).expect("poll 2 reported stream 2 joined");
        let u2 = poll_upper(&calls, joined);
        let after: Vec<Call> = statements_after(&calls, joined)
            .into_iter()
            .map(|(_, c)| c)
            .take(4)
            .collect();
        let ceiling = TailCeiling::AtOrBelow(u2);
        let want = [
            Call::FirstLines {
                fps: vec![fpn(2)],
                after: start,
                ceiling,
            },
            Call::CatchUp {
                fps: vec![fpn(2)],
                lower: Lower::Start(start),
                upper: u2,
                ceiling,
            },
            Call::Admit(vec![fpn(2)]),
        ];
        assert_eq!(after.get(..3), Some(&want[..]), "{calls:#?}");
        assert!(
            matches!(after.get(3), Some(Call::Poll { lower: Lower::Start(l), .. }) if *l == u2),
            "the main scan resumes at Start{{u2}}: {calls:#?}"
        );
        assert_eq!(lines, ["A1", "A2", "B1"]);
    }

    /// H2 (issue #804): a catch-up walks one slice per page, re-probes for
    /// the next line after every slice that comes back under the limit, so
    /// empty slices are never read, and admits once a probe finds nothing.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_catch_up_reprobes_after_each_short_slice_and_never_walks_empty_ones() {
        const MS: i64 = 1_000_000;
        let mut cfg = test_cfg();
        cfg.fetch_limit = 2;
        cfg.slice_ns = 2 * MS;
        // Five frames arrive back to back; a depth of 4 could evict one.
        cfg.channel_depth = 64;
        let start = cfg.start_ns;
        let c = TailCursor {
            tuple: (start + MS, fpn(2), 0),
            seen: 1,
        };
        let mut joined_sent = false;
        let fetcher = ScriptedFetcher::new(
            move |n, l, _| {
                if n == 0 {
                    return lines_page(&["A1"], &[]);
                }
                if !joined_sent && Lower::of(l).ts() >= start + 8 * MS {
                    joined_sent = true;
                    return lines_page(&["A2"], &[2]);
                }
                lines_page(&[], &[])
            },
            move |n, _| match n {
                0 => vec![(fpn(2), start + 1)],
                1 => vec![(fpn(2), start + 5 * MS)],
                _ => Vec::new(),
            },
            move |n| {
                CatchStep::Page(match n {
                    0 => catch_page(&["B1", "B2"], 2, Some(c), &[2]),
                    1 => catch_page(&["B3"], 1, None, &[2]),
                    2 => catch_page(&["B4"], 1, None, &[2]),
                    _ => catch_page(&[], 0, None, &[]),
                })
            },
        );
        let run = ScriptedRun::start(fetcher, cfg);
        run.wait_for(Duration::from_secs(3), has_admit).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let (calls, lines) = run.stop().await;
        let joined = joining_poll(&calls).expect("a poll reported stream 2 joined");
        let ceiling = TailCeiling::AtOrBelow(poll_upper(&calls, joined));
        let after: Vec<Call> = statements_after(&calls, joined)
            .into_iter()
            .map(|(_, c)| c)
            .collect();
        let kinds: Vec<String> = after
            .iter()
            .take(7)
            .map(|c| match c {
                Call::FirstLines { after, .. } => format!("FirstLines({})", after - start),
                Call::CatchUp { lower, .. } => match lower {
                    Lower::Start(t) => format!("CatchUp(Start{{{}}})", t - start),
                    Lower::After(k) => format!("CatchUp(After({}))", k.tuple.0 - start),
                },
                Call::Admit(f) => format!("Admit({f:?})"),
                other => format!("{other:?}"),
            })
            .collect();
        let want: Vec<String> = vec![
            "FirstLines(0)".to_string(),
            "CatchUp(Start{0})".to_string(),
            format!("CatchUp(After({MS}))"),
            format!("FirstLines({})", 3 * MS),
            format!("CatchUp(Start{{{}}})", 5 * MS - 1),
            format!("FirstLines({})", 7 * MS - 1),
            format!("Admit({:?})", vec![fpn(2)]),
        ];
        assert_eq!(kinds, want, "{calls:#?}");
        assert_eq!(
            count(&calls, |c| matches!(c, Call::CatchUp { .. })),
            3,
            "{calls:#?}"
        );
        for (_, c) in &calls {
            if let Call::CatchUp {
                lower,
                upper,
                ceiling: cl,
                ..
            } = c
            {
                assert_eq!(*cl, ceiling);
                assert!(upper - lower.ts() <= 2 * MS, "slice too wide: {c:?}");
                assert!(*upper <= ceiling.ts(), "past the ceiling: {c:?}");
            }
        }
        assert_eq!(lines, ["A1", "A2", "B1", "B2", "B3", "B4"]);
    }

    /// H4 (issue #804): a catch-up page that never answers does not hold up
    /// shutdown.
    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_during_a_hanging_catch_up_returns_promptly() {
        let cfg = test_cfg();
        let start = cfg.start_ns;
        let fetcher = ScriptedFetcher::new(
            h1_polls,
            move |_, _| vec![(fpn(2), start + 1)],
            |_| CatchStep::Hang,
        );
        let run = ScriptedRun::start(fetcher, cfg);
        run.wait_for(Duration::from_secs(2), |c| {
            c.iter().any(|(_, c)| matches!(c, Call::CatchUp { .. }))
        })
        .await;
        let started = Instant::now();
        let _ = run.stop().await;
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    /// H5 (issue #804): a failed catch-up read closes the connection with
    /// its reason, sends nothing more and admits nothing.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_catch_up_closes_with_its_reason_and_admits_nothing() {
        let cfg = test_cfg();
        let start = cfg.start_ns;
        let fetcher = ScriptedFetcher::new(
            h1_polls,
            move |_, _| vec![(fpn(2), start + 1)],
            |_| {
                CatchStep::Fail(
                    Duration::ZERO,
                    ReadError::PipelineInvalid {
                        reason: "catch-up failed".to_string(),
                    },
                )
            },
        );
        let run = ScriptedRun::start(fetcher, cfg);
        let ScriptedRun {
            handle,
            shutdown,
            sent,
            closed,
            log,
        } = run;
        tokio::time::timeout(Duration::from_secs(3), handle)
            .await
            .expect("a failed catch-up must close the connection")
            .expect("no panic");
        drop(shutdown);
        let frame = closed
            .lock()
            .unwrap()
            .clone()
            .expect("close was sent")
            .expect("close carried a reason");
        assert_eq!(frame.reason.as_str(), "catch-up failed");
        assert_eq!(sent_lines(&sent.lock().unwrap()), ["A1", "A2"]);
        let calls = log.lock().unwrap().clone();
        assert!(!has_admit(&calls), "{calls:#?}");
    }

    /// Issues #803 and #804 (T9): a catch-up read whose server failed is
    /// read again once, at once, at the same position on the server the
    /// fetcher moved to; the tail stays open, the stream's line is sent
    /// once and the stream is admitted.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_catch_up_read_whose_server_failed_is_read_again_once() {
        let cfg = test_cfg();
        let start = cfg.start_ns;
        let fetcher = ScriptedFetcher::new(
            h1_polls,
            move |_, _| vec![(fpn(2), start + 1)],
            |n| match n {
                0 => CatchStep::Fail(
                    Duration::ZERO,
                    ReadError::PipelineInvalid {
                        reason: "server failed".to_string(),
                    },
                ),
                _ => CatchStep::Page(catch_page(&["B1"], 1, None, &[2])),
            },
        )
        .with_failover();
        let run = ScriptedRun::start(fetcher, cfg);
        run.wait_for(Duration::from_secs(3), has_admit).await;
        assert!(run.closed.lock().unwrap().is_none(), "the tail stays open");
        let (calls, lines) = run.stop().await;
        let catches: Vec<&Call> = calls
            .iter()
            .map(|(_, c)| c)
            .filter(|c| matches!(c, Call::CatchUp { .. }))
            .collect();
        assert!(catches.len() >= 2, "{calls:#?}");
        assert_eq!(
            catches[0], catches[1],
            "the same read, at the same position"
        );
        assert_eq!(lines.iter().filter(|l| *l == "B1").count(), 1, "{lines:?}");
        assert!(has_admit(&calls), "{calls:#?}");
    }

    /// T10 (issue #803, code review round 1): a poll whose server failed
    /// is repeated once, and an accepted probe of a waiting stream between
    /// the two does not give the poll a third run: the second failure of
    /// that poll closes the tail.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_accepted_probe_between_two_failures_of_a_poll_does_not_reset_its_count() {
        let cfg = test_cfg();
        let fetcher = ScriptedFetcher::new(
            h1_polls,
            // Every probe answers, and finds nothing, so stream 2 stays
            // waiting and is probed again each time its timer is due.
            |_, _| Vec::new(),
            |_| CatchStep::Page(catch_page(&[], 0, None, &[])),
        )
        .with_failover()
        // Polls 2 on fail, each after 20 ms, so the recheck timer is due
        // between two of them.
        .with_failing_polls_from(2, Duration::from_millis(20));
        let ScriptedRun {
            handle,
            shutdown,
            closed,
            log,
            ..
        } = ScriptedRun::start(fetcher, cfg);
        let ended = tokio::time::timeout(Duration::from_secs(3), handle).await;
        let _ = shutdown.send(true);
        let calls = log.lock().unwrap().clone();
        assert!(
            ended.is_ok(),
            "the second failure of the poll closes the tail: {calls:#?}"
        );
        let polls: Vec<usize> = calls
            .iter()
            .enumerate()
            .filter(|(_, (_, c))| matches!(c, Call::Poll { .. }))
            .map(|(i, _)| i)
            .collect();
        assert_eq!(
            polls.len(),
            4,
            "polls 0, 1, then the failing poll twice: {calls:#?}"
        );
        assert!(
            calls[polls[2]..polls[3]]
                .iter()
                .any(|(_, c)| matches!(c, Call::FirstLines { .. })),
            "a probe ran between the two failures: {calls:#?}"
        );
        let frame = closed
            .lock()
            .unwrap()
            .clone()
            .expect("close was sent")
            .expect("with a reason");
        assert_eq!(frame.reason.as_str(), "poll 3 failed");
    }

    /// H6 (issue #804): an empty probe, or a catch-up that fetches nothing
    /// from the stream, leaves it waiting; it is admitted once a read
    /// fetches from it, and its line is sent once.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_empty_catch_up_read_leaves_the_stream_waiting_until_a_read_fetches_from_it() {
        let cfg = test_cfg();
        let start = cfg.start_ns;
        let fetcher = ScriptedFetcher::new(
            h1_polls,
            move |n, _| match n {
                1 | 2 => vec![(fpn(2), start + 1)],
                _ => Vec::new(),
            },
            |n| {
                CatchStep::Page(match n {
                    1 => catch_page(&["B1"], 1, None, &[2]),
                    _ => catch_page(&[], 0, None, &[]),
                })
            },
        );
        let run = ScriptedRun::start(fetcher, cfg);
        run.wait_for(Duration::from_secs(3), has_admit).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let (calls, lines) = run.stop().await;
        let joined = joining_poll(&calls).expect("poll 2 reported stream 2 joined");
        let after: Vec<Call> = statements_after(&calls, joined)
            .into_iter()
            .map(|(_, c)| c)
            .collect();
        let fl: Vec<usize> = after
            .iter()
            .enumerate()
            .filter(|(_, c)| matches!(c, Call::FirstLines { .. }))
            .map(|(i, _)| i)
            .collect();
        assert!(fl.len() >= 3, "three probes: {calls:#?}");
        assert_eq!(
            fl[0], 0,
            "the first statement after joining is a probe: {calls:#?}"
        );
        let is_poll = |c: &Call| matches!(c, Call::Poll { .. });
        let is_cu = |c: &Call| matches!(c, Call::CatchUp { .. });
        assert!(
            after[fl[0] + 1..fl[1]].iter().any(is_poll)
                && !after[fl[0] + 1..fl[1]].iter().any(is_cu),
            "an empty probe opens no catch-up: {calls:#?}"
        );
        assert!(is_cu(&after[fl[1] + 1]), "{calls:#?}");
        assert!(
            is_poll(&after[fl[1] + 2]),
            "the empty catch-up is the only statement before the next main poll: {calls:#?}"
        );
        assert!(after[fl[1] + 2..fl[2]].iter().any(is_poll), "{calls:#?}");
        assert!(is_cu(&after[fl[2] + 1]), "{calls:#?}");
        assert_eq!(after[fl[2] + 2], Call::Admit(vec![fpn(2)]), "{calls:#?}");
        assert_eq!(
            count(&calls, |c| matches!(c, Call::Admit(_))),
            1,
            "{calls:#?}"
        );
        let ceilings: Vec<i64> = after
            .iter()
            .filter_map(|c| match c {
                Call::FirstLines { ceiling, .. } => Some(ceiling.ts()),
                _ => None,
            })
            .collect();
        assert!(
            ceilings.windows(2).all(|w| w[1] >= w[0]),
            "each probe's ceiling at or above the one before: {ceilings:?}"
        );
        assert_eq!(lines.iter().filter(|l| *l == "B1").count(), 1, "{lines:?}");
    }

    /// The instant poll 2's `PollDone` was recorded.
    fn joined_at(calls: &[(Instant, Call)]) -> Instant {
        calls[joining_poll(calls).expect("poll 2 reported stream 2 joined")].0
    }

    /// H7 (issue #804): a stream whose probes stay empty is admitted, with
    /// no catch-up, once its recheck bound has passed, and not before.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stream_waiting_past_the_recheck_bound_is_admitted_without_lines() {
        let mut cfg = test_cfg();
        cfg.catch_up_recheck_ns = 50_000_000;
        let fetcher = ScriptedFetcher::new(
            h1_polls,
            |_, _| Vec::new(),
            |_| CatchStep::Page(catch_page(&[], 0, None, &[])),
        );
        let run = ScriptedRun::start(fetcher, cfg);
        run.wait_for(Duration::from_secs(1), has_admit).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let (calls, _) = run.stop().await;
        let admit = calls
            .iter()
            .position(|(_, c)| *c == Call::Admit(vec![fpn(2)]))
            .expect("stream 2 is admitted within 1 s");
        assert_eq!(count(&calls, |c| matches!(c, Call::CatchUp { .. })), 0);
        let admit_at = calls[admit].0;
        assert!(
            !calls[admit..]
                .iter()
                .any(|(t, c)| matches!(c, Call::FirstLines { .. })
                    && *t < admit_at + Duration::from_millis(100)),
            "no probe after admission: {calls:#?}"
        );
        assert!(
            admit_at.duration_since(joined_at(&calls)) >= Duration::from_millis(50),
            "admitted before its bound: {calls:#?}"
        );
    }

    /// The deadline the producer recorded for the first joined stream.
    fn producer_deadline(calls: &[(Instant, Call)]) -> Instant {
        calls
            .iter()
            .find_map(|(_, c)| match c {
                Call::Deadline(d) => Some(*d),
                _ => None,
            })
            .expect("the producer set a deadline")
    }

    /// H8 (issue #804): no probe starts at or after the stream's deadline,
    /// so a line readable only after it is neither read nor sent.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_line_readable_only_after_the_bound_is_neither_read_nor_sent() {
        let mut cfg = test_cfg();
        cfg.catch_up_recheck_ns = 50_000_000;
        let start = cfg.start_ns;
        let fetcher = ScriptedFetcher::new(
            h1_polls,
            move |_, log| {
                if Instant::now() < producer_deadline(log) {
                    Vec::new()
                } else {
                    vec![(fpn(2), start + 1)]
                }
            },
            |_| CatchStep::Page(catch_page(&["B1"], 1, None, &[2])),
        );
        let run = ScriptedRun::start(fetcher, cfg);
        run.wait_for(Duration::from_secs(1), has_admit).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let (calls, lines) = run.stop().await;
        assert!(has_admit(&calls), "stream 2 is admitted: {calls:#?}");
        let deadline = producer_deadline(&calls);
        for (t, c) in &calls {
            if matches!(c, Call::FirstLines { .. }) {
                assert!(
                    *t < deadline,
                    "a probe started after the deadline: {calls:#?}"
                );
            }
        }
        assert_eq!(
            count(&calls, |c| matches!(c, Call::CatchUp { .. })),
            0,
            "{calls:#?}"
        );
        assert_eq!(
            count(&calls, |c| *c == Call::Admit(vec![fpn(2)])),
            1,
            "{calls:#?}"
        );
        assert!(!lines.iter().any(|l| l == "B1"), "{lines:?}");
    }

    /// H11 (issue #804, code review round 1): a joined stream's first probe
    /// runs before the producer idles, so a poll interval that outlasts
    /// the stream's bound cannot admit it before its readable lines below
    /// the cursor are read.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_joined_stream_is_probed_before_an_idle_interval_that_outlasts_its_bound() {
        let mut cfg = test_cfg();
        cfg.poll_interval = Duration::from_millis(200);
        cfg.catch_up_recheck_ns = 100_000_000;
        let start = cfg.start_ns;
        let fetcher = ScriptedFetcher::new(
            h1_polls,
            move |_, _| vec![(fpn(2), start + 1)],
            |_| CatchStep::Page(catch_page(&["B1"], 1, None, &[2])),
        );
        let run = ScriptedRun::start(fetcher, cfg);
        run.wait_for(Duration::from_secs(2), has_admit).await;
        let (calls, lines) = run.stop().await;
        let joined = joining_poll(&calls).expect("poll 2 reported stream 2 joined");
        let first = calls[joined..]
            .iter()
            .position(|(_, c)| matches!(c, Call::FirstLines { .. }))
            .map(|i| i + joined);
        let admit = calls
            .iter()
            .position(|(_, c)| *c == Call::Admit(vec![fpn(2)]));
        assert!(
            first.is_some() && admit.is_some() && first < admit,
            "the stream is probed before it is admitted: {calls:#?}"
        );
        assert_eq!(
            count(&calls, |c| matches!(c, Call::CatchUp { .. })),
            1,
            "{calls:#?}"
        );
        assert_eq!(lines, ["A1", "A2", "B1"]);
    }

    /// H9 (issue #804): a catch-up page that returns after the deadline is
    /// sent, and then the catch-up stops: the stream is admitted with no
    /// further read.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_catch_up_that_crosses_the_bound_stops_at_it() {
        let mut cfg = test_cfg();
        cfg.catch_up_recheck_ns = 100_000_000;
        cfg.fetch_limit = 2;
        let start = cfg.start_ns;
        let c = TailCursor {
            tuple: (start + 1, fpn(2), 0),
            seen: 1,
        };
        let fetcher = ScriptedFetcher::new(
            h1_polls,
            move |_, _| vec![(fpn(2), start + 1)],
            move |n| match n {
                0 => CatchStep::Slow(
                    Duration::from_millis(150),
                    catch_page(&["B1", "B2"], 2, Some(c), &[2]),
                ),
                _ => CatchStep::Page(catch_page(&["B3"], 1, None, &[2])),
            },
        );
        let run = ScriptedRun::start(fetcher, cfg);
        run.wait_for(Duration::from_secs(3), |c| {
            c.iter()
                .position(|(_, c)| matches!(c, Call::Admit(_)))
                .is_some_and(|i| c[i..].iter().any(|(_, c)| matches!(c, Call::Poll { .. })))
        })
        .await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let (calls, lines) = run.stop().await;
        let joined = joining_poll(&calls).expect("poll 2 reported stream 2 joined");
        let u2 = poll_upper(&calls, joined);
        assert_eq!(
            count(&calls, |c| matches!(c, Call::CatchUp { .. })),
            1,
            "{calls:#?}"
        );
        let cu = calls
            .iter()
            .position(|(_, c)| matches!(c, Call::CatchUp { .. }))
            .expect("one catch-up");
        let admit = calls
            .iter()
            .position(|(_, c)| *c == Call::Admit(vec![fpn(2)]))
            .expect("stream 2 admitted");
        assert!(admit > cu, "{calls:#?}");
        assert!(
            !calls[cu..]
                .iter()
                .any(|(_, c)| matches!(c, Call::FirstLines { .. })),
            "no probe after the catch-up: {calls:#?}"
        );
        assert!(
            matches!(
                calls[admit..].iter().find(|(_, c)| matches!(c, Call::Poll { .. })),
                Some((_, Call::Poll { lower: Lower::Start(l), .. })) if *l == u2
            ),
            "the poll after admission resumes at Start{{u2}}: {calls:#?}"
        );
        assert_eq!(lines, ["A1", "A2", "B1", "B2"]);
    }

    /// H10 (issue #804): rechecks keep to their own timer, at most one per
    /// poll interval, while the main scan polls a backlog without pause.
    #[tokio::test(flavor = "multi_thread")]
    async fn rechecks_are_limited_by_their_own_timer_while_the_main_scan_runs_without_pause() {
        let mut cfg = test_cfg();
        cfg.poll_interval = Duration::from_millis(20);
        cfg.catch_up_recheck_ns = 200_000_000;
        cfg.slice_ns = 1_000_000;
        cfg.start_ns = params::now_ns() - 1_000_000_000;
        let fetcher = ScriptedFetcher::new(
            |n, _, _| match n {
                1 => lines_page(&[], &[2]),
                _ => lines_page(&[], &[]),
            },
            |_, _| Vec::new(),
            |_| CatchStep::Page(catch_page(&[], 0, None, &[])),
        );
        let run = ScriptedRun::start(fetcher, cfg);
        run.wait_for(Duration::from_secs(3), has_admit).await;
        let (calls, _) = run.stop().await;
        let joined = joining_poll(&calls).expect("poll 2 reported stream 2 joined");
        let admit = calls
            .iter()
            .position(|(_, c)| *c == Call::Admit(vec![fpn(2)]))
            .expect("stream 2 admitted");
        let span = &calls[joined..admit];
        let polls = count(span, |c| matches!(c, Call::Poll { .. }));
        let probes: Vec<Instant> = span
            .iter()
            .filter(|(_, c)| matches!(c, Call::FirstLines { .. }))
            .map(|(t, _)| *t)
            .collect();
        assert!(polls > 11, "the main scan ran without pause: {polls} polls");
        assert!(!probes.is_empty(), "the waiting stream is rechecked");
        assert!(probes.len() <= 11, "{} rechecks", probes.len());
        for w in probes.windows(2) {
            assert!(
                w[1].duration_since(w[0]) >= Duration::from_millis(20),
                "rechecks closer than the poll interval: {probes:?}"
            );
        }
    }

    // -- issue #806: one owner of the queue, the error and the socket ----

    const GRACE_MS: u64 = 250;

    /// A one-entry page whose line is `line`, at timestamp `ts`.
    fn page_event(line: &str, ts: i64) -> Event {
        Event::Page(vec![stream(r#"{"app":"x"}"#, vec![(ts, line)])])
    }

    fn frame_lines(frame: &PoppedFrame) -> Vec<String> {
        frame
            .streams
            .iter()
            .flat_map(|s| s.entries.iter().map(|(_, l)| l.clone()))
            .collect()
    }

    fn conn(depth: usize) -> ConnState {
        ConnState::new(depth, 1_000, Duration::from_secs(60))
    }

    /// Sends every frame `decide` gives, marking each written, and returns
    /// their lines and the action that ended it.
    fn send_all(st: &mut ConnState, now: TInstant) -> (Vec<Vec<String>>, Action) {
        let mut sent = Vec::new();
        loop {
            match st.decide(now) {
                Action::Send { frame, .. } => {
                    sent.push(frame_lines(&frame));
                    st.on_handed_over();
                    st.on_written();
                }
                other => return (sent, other),
            }
        }
    }

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    /// S1 (issue #806): every page queued before the fatal error is sent,
    /// then the Close carries the error.
    #[test]
    fn pages_then_fatal_are_all_sent_then_closed_with_the_reason() {
        let mut st = conn(4);
        for (i, l) in ["A1", "A2", "A3"].iter().enumerate() {
            st.on_event(Some(page_event(l, i as i64)));
        }
        st.on_event(Some(Event::Fatal("boom".to_string())));
        let (sent, end) = send_all(&mut st, TInstant::now());
        assert_eq!(sent, [["A1"], ["A2"], ["A3"]]);
        assert!(
            matches!(end, Action::Close(Some(ref r)) if r == "boom"),
            "{end:?}"
        );
    }

    /// S2 (issue #806): a page that arrives after the writer saw an empty
    /// queue still goes out before the fatal Close.
    #[test]
    fn a_page_that_arrives_after_an_empty_queue_is_sent_before_the_fatal_close() {
        let mut st = conn(4);
        let now = TInstant::now();
        assert!(matches!(st.decide(now), Action::Wait { until: None }));
        st.on_event(Some(page_event("A", 1)));
        st.on_event(Some(Event::Fatal("boom".to_string())));
        let (sent, end) = send_all(&mut st, now);
        assert_eq!(sent, [["A"]]);
        assert!(
            matches!(end, Action::Close(Some(ref r)) if r == "boom"),
            "{end:?}"
        );
    }

    /// S3 (issue #806): a shutdown drain waits for the producer's channel
    /// to close, sending what arrives meanwhile within the drain.
    #[test]
    fn a_shutdown_drain_waits_for_the_producer() {
        let mut st = conn(4);
        let t0 = TInstant::now();
        st.on_shutdown(t0);
        assert!(matches!(st.decide(t0), Action::Wait { until: Some(u) } if u == t0 + ms(GRACE_MS)));
        st.on_event(Some(page_event("A", 1)));
        match st.decide(t0) {
            Action::Send { frame, deadline } => {
                assert_eq!(frame_lines(&frame), ["A"]);
                assert!(deadline <= t0 + ms(GRACE_MS));
            }
            other => panic!("expected Send A, got {other:?}"),
        }
        st.on_handed_over();
        st.on_written();
        st.on_event(None);
        assert!(matches!(st.decide(t0), Action::Close(None)));
    }

    /// S4 (issue #806): the drain stops at its deadline: a Close when
    /// nothing is half sent, no Close when something is.
    #[test]
    fn the_drain_cuts_at_its_deadline() {
        let t0 = TInstant::now();
        for half_sent in [false, true] {
            let mut st = conn(4);
            st.on_shutdown(t0);
            for (i, l) in ["F1", "F2", "F3"].iter().enumerate() {
                st.on_event(Some(page_event(l, i as i64)));
            }
            match st.decide(t0) {
                Action::Send { frame, deadline } => {
                    assert_eq!(frame_lines(&frame), ["F1"]);
                    assert_eq!(deadline, t0 + ms(GRACE_MS));
                }
                other => panic!("expected Send F1, got {other:?}"),
            }
            st.on_handed_over();
            if !half_sent {
                st.on_written();
            }
            let end = st.decide(t0 + ms(GRACE_MS));
            if half_sent {
                assert!(matches!(end, Action::Drop), "{end:?}");
            } else {
                assert!(matches!(end, Action::Close(None)), "{end:?}");
            }
        }
    }

    /// S5 (issue #806): once the connection must stop no data frame is
    /// sent, even with frames and an error queued.
    #[test]
    fn no_data_frame_after_the_connection_must_stop() {
        let mut st = conn(4);
        st.on_event(Some(page_event("A1", 1)));
        st.on_event(Some(page_event("A2", 2)));
        st.on_event(Some(Event::Fatal("boom".to_string())));
        st.on_must_stop();
        let end = st.decide(TInstant::now());
        assert!(matches!(end, Action::Close(None)), "{end:?}");
        assert_eq!(st.queue.frames.len(), 2);
    }

    /// S6 (issue #806): a Text handed over and not known to be written
    /// means no Close: the socket is dropped.
    #[test]
    fn a_half_sent_text_drops_the_socket() {
        let mut st = conn(4);
        st.on_event(Some(page_event("A", 1)));
        let now = TInstant::now();
        assert!(matches!(st.decide(now), Action::Send { .. }));
        st.on_handed_over();
        st.on_must_stop();
        assert!(matches!(st.decide(now), Action::Drop));
    }

    /// S7 (issue #806): a send is decided up to, and not at, the drain's
    /// deadline.
    #[test]
    fn no_send_is_decided_at_or_after_the_deadline() {
        let t0 = TInstant::now();
        let mut st = conn(4);
        st.on_shutdown(t0);
        st.on_event(Some(page_event("A", 1)));
        assert!(matches!(
            st.decide(t0 + ms(GRACE_MS - 1)),
            Action::Send { .. }
        ));
        let mut st = conn(4);
        st.on_shutdown(t0);
        st.on_event(Some(page_event("A", 1)));
        let end = st.decide(t0 + ms(GRACE_MS));
        assert!(matches!(end, Action::Close(None)), "{end:?}");
    }

    /// S8 (issue #806): a page waiting when the error comes evicts the
    /// oldest queued frame by the slow-client rule, and the eviction is
    /// reported before the Close: depth + 1 sends, nothing lost unreported.
    #[test]
    fn a_page_waiting_when_the_error_comes_evicts_by_the_slow_client_rule_and_is_reported() {
        let mut st = conn(2);
        let now = TInstant::now();
        st.on_event(Some(page_event("A1", 1)));
        let first = match st.decide(now) {
            Action::Send { frame, .. } => frame_lines(&frame),
            other => panic!("expected Send A1, got {other:?}"),
        };
        assert_eq!(first, ["A1"]);
        st.on_handed_over();
        st.on_event(Some(page_event("A2", 2)));
        st.on_event(Some(page_event("A3", 3)));
        st.on_event(Some(page_event("A4", 4)));
        st.on_event(Some(Event::Fatal("boom".to_string())));
        st.on_written();
        match st.decide(now) {
            Action::Send { frame, .. } => {
                assert_eq!(frame_lines(&frame), ["A3"]);
                assert_eq!(frame.dropped_total, 1);
                assert_eq!(
                    frame
                        .dropped
                        .iter()
                        .map(|d| d.timestamp_ns)
                        .collect::<Vec<_>>(),
                    [2]
                );
            }
            other => panic!("expected Send A3, got {other:?}"),
        }
        st.on_handed_over();
        st.on_written();
        let (sent, end) = send_all(&mut st, now);
        assert_eq!(sent, [["A4"]]);
        assert!(
            matches!(end, Action::Close(Some(ref r)) if r == "boom"),
            "{end:?}"
        );
    }

    /// S9 (issue #806): a shutdown channel whose sender is gone is read as
    /// shutdown before the frame is taken, so the frame's deadline is the
    /// drain's.
    #[test]
    fn a_closed_shutdown_channel_is_read_before_the_frame_is_taken() {
        let (shutdown_tx, mut shutdown) = watch::channel(false);
        drop(shutdown_tx);
        let (_cancel_tx, cancel) = watch::channel(false);
        let (tx, mut rx) = mpsc::channel(4);
        tx.try_send(page_event("F1", 1)).expect("room");
        let mut st = conn(4);
        let (mut shutdown_open, mut rx_open) = (true, true);
        let t0 = TInstant::now();
        observe_inputs(
            &mut st,
            &mut shutdown,
            &mut shutdown_open,
            &cancel,
            &mut rx,
            &mut rx_open,
            t0,
        );
        assert_eq!(st.drain_until, Some(t0 + ms(GRACE_MS)));
        match st.decide(t0) {
            Action::Send { frame, deadline } => {
                assert_eq!(frame_lines(&frame), ["F1"]);
                assert_eq!(deadline, t0 + ms(GRACE_MS));
            }
            other => panic!("expected Send F1, got {other:?}"),
        }
    }

    /// A source that replays a script of `try_next` answers; `next` never
    /// answers.
    struct ScriptSource(VecDeque<TryNext>);

    impl EventSource for ScriptSource {
        fn try_next(&mut self) -> TryNext {
            self.0.pop_front().unwrap_or(TryNext::Empty)
        }

        fn next(&mut self) -> impl Future<Output = Option<Event>> + Send {
            std::future::pending()
        }
    }

    /// S10 (issue #806): each `observe_inputs` takes at most one event, in
    /// order.
    #[test]
    fn observe_inputs_takes_one_event_per_call_in_order() {
        let (_shutdown_tx, mut shutdown) = watch::channel(false);
        let (_cancel_tx, cancel) = watch::channel(false);
        let mut src = ScriptSource(VecDeque::from([
            TryNext::Event(page_event("A", 1)),
            TryNext::Event(Event::Fatal("boom".to_string())),
            TryNext::Closed,
        ]));
        let mut st = conn(4);
        let (mut shutdown_open, mut rx_open) = (true, true);
        let t0 = TInstant::now();
        let mut observe = |st: &mut ConnState, rx_open: &mut bool| {
            observe_inputs(
                st,
                &mut shutdown,
                &mut shutdown_open,
                &cancel,
                &mut src,
                rx_open,
                t0,
            )
        };
        observe(&mut st, &mut rx_open);
        assert_eq!(st.queue.frames.len(), 1);
        assert_eq!(st.fatal, None);
        observe(&mut st, &mut rx_open);
        assert_eq!(st.fatal.as_deref(), Some("boom"));
        assert!(rx_open);
        observe(&mut st, &mut rx_open);
        assert!(st.producer_done);
        assert!(!rx_open);
        assert!(matches!(st.decide(t0), Action::Send { .. }));
    }

    /// Waits, by yielding only (time does not move), until `done` holds;
    /// panics after 100,000 yields.
    async fn yield_until(mut done: impl FnMut() -> bool) {
        for _ in 0..100_000 {
            if done() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("the expected event never came");
    }

    fn logged(log: &WireLog, pred: impl Fn(&Wire) -> bool) -> usize {
        log.lock().unwrap().iter().filter(|(_, w)| pred(w)).count()
    }

    fn time_of(log: &WireLog, pred: impl Fn(&Wire) -> bool) -> Option<TInstant> {
        log.lock()
            .unwrap()
            .iter()
            .find(|(_, w)| pred(w))
            .map(|(t, _)| *t)
    }

    fn lines_of_wire(wire: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
        sent_lines(&wire.lock().unwrap())
    }

    /// A source with input always waiting (issue #806, D14): `try_next`
    /// always yields a page; `next` yields one only when the test has added
    /// a permit. It logs each read on the sender's log, and panics after
    /// more than 10 reads since the last socket step, so a writer that
    /// drains it fails rather than spins.
    struct GatedFloodSource {
        log: WireLog,
        permits: Arc<tokio::sync::Semaphore>,
        n: i64,
    }

    impl GatedFloodSource {
        fn read(&mut self, via_next: bool) -> Event {
            let mut log = self.log.lock().unwrap();
            let since_step = log
                .iter()
                .rev()
                .take_while(|(_, w)| !matches!(w, Wire::Ready | Wire::Flushed))
                .filter(|(_, w)| matches!(w, Wire::Read(_)))
                .count();
            assert!(since_step < 10, "more than 10 reads between socket steps");
            log.push((TInstant::now(), Wire::Read(via_next)));
            self.n += 1;
            page_event(&format!("P{}", self.n), self.n)
        }
    }

    impl EventSource for GatedFloodSource {
        fn try_next(&mut self) -> TryNext {
            TryNext::Event(self.read(false))
        }

        async fn next(&mut self) -> Option<Event> {
            self.permits.acquire().await.expect("open").forget();
            Some(self.read(true))
        }
    }

    /// D14 (issue #806): a source with input always waiting cannot delay a
    /// ready socket step or the shutdown cut. P1's flush is held by a gate
    /// so that its completion and a receive become ready in the same
    /// synchronous step of the test (the gate and the permit are released
    /// together, with no await between, on this current-thread runtime):
    /// the writer's next poll finds both ready, and the step must win.
    #[tokio::test(start_paused = true)]
    async fn a_source_with_input_always_waiting_cannot_delay_sends_or_the_shutdown_cut() {
        let sender = WireSender::new()
            .flushes(&[FlushMode::Gate])
            .flush(FlushMode::Sleep(ms(100)));
        let (log, wire, closed, gate) = (
            Arc::clone(&sender.log),
            Arc::clone(&sender.wire),
            Arc::clone(&sender.closed),
            Arc::clone(&sender.gate),
        );
        let permits = Arc::new(tokio::sync::Semaphore::new(0));
        let src = GatedFloodSource {
            log: Arc::clone(&log),
            permits: Arc::clone(&permits),
            n: 0,
        };
        let (_shutdown_tx, shutdown) = watch::channel(true);
        let (cancel_tx, cancel) = watch::channel(false);
        let t0 = TInstant::now();
        let handle = tokio::spawn(writer_loop(
            sender,
            src,
            test_cfg(),
            shutdown,
            cancel,
            Arc::new(cancel_tx),
        ));
        yield_until(|| logged(&log, |w| *w == Wire::FlushStart) == 1).await;
        tokio::time::advance(ms(100)).await;
        // Released together: P1's flush and one receive are ready at the
        // writer's next poll.
        gate.notify_one();
        permits.add_permits(1);
        yield_until(|| {
            logged(&log, |w| *w == Wire::Flushed) == 1
                && logged(&log, |w| *w == Wire::Read(true)) >= 1
        })
        .await;
        {
            let l = log.lock().unwrap();
            let flushed = l
                .iter()
                .position(|(_, w)| *w == Wire::Flushed)
                .expect("P1 flushed");
            let received = l
                .iter()
                .position(|(_, w)| *w == Wire::Read(true))
                .expect("a receive");
            assert!(
                flushed < received,
                "the ready step goes before the ready receive: {l:?}"
            );
        }
        tokio::time::advance(ms(100)).await;
        yield_until(|| logged(&log, |w| *w == Wire::FlushStart) == 3).await;
        tokio::time::advance(ms(50)).await;
        yield_until(|| handle.is_finished()).await;
        handle.await.expect("no panic");
        assert_eq!(TInstant::now() - t0, ms(GRACE_MS));
        assert_eq!(lines_of_wire(&wire), ["P1", "P2"]);
        assert!(
            closed.lock().unwrap().is_none(),
            "no Close after a half-sent frame"
        );
        let l = log.lock().unwrap();
        let third = l
            .iter()
            .filter(|(_, w)| matches!(w, Wire::Start(_)))
            .nth(2)
            .map(|(t, _)| *t)
            .expect("a third frame handed over");
        assert_eq!(third - t0, ms(200));
        let mut reads = 0;
        for (_, w) in l.iter() {
            match w {
                Wire::Ready | Wire::Flushed => reads = 0,
                Wire::Read(_) => {
                    reads += 1;
                    assert!(
                        reads <= 2,
                        "more than two reads between socket steps: {l:?}"
                    );
                }
                _ => {}
            }
        }
    }

    /// Polls a script of lines, each a full page, then an error if one is
    /// given, then never answers.
    struct Polls {
        lines: VecDeque<&'static str>,
        then: Option<String>,
    }

    impl TailFetcher for Polls {
        async fn poll(
            &mut self,
            _lower: TailLower,
            upper_ns: i64,
            _narrow: bool,
            fetch_limit: u32,
        ) -> Result<TailPage, ReadError> {
            if let Some(line) = self.lines.pop_front() {
                return Ok(TailPage {
                    streams: vec![stream(r#"{"app":"x"}"#, vec![(upper_ns, line)])],
                    next: Some(TailCursor {
                        tuple: (upper_ns, Fingerprint::from_raw(1), 0),
                        seen: 1,
                    }),
                    fetched: fetch_limit,
                    joined: Vec::new(),
                });
            }
            if let Some(reason) = self.then.take() {
                return Err(ReadError::PipelineInvalid { reason });
            }
            std::future::pending().await
        }
    }

    fn polls(lines: &[&'static str], then: Option<&str>) -> Polls {
        Polls {
            lines: lines.iter().copied().collect(),
            then: then.map(str::to_string),
        }
    }

    /// D1 (issue #806): a fatal error closes the connection only after
    /// every frame queued before it is written.
    #[tokio::test]
    async fn a_fatal_error_closes_only_after_every_frame_queued_before_it() {
        let sender = WireSender::new().flush(FlushMode::YieldOnce);
        let (wire, closed) = (Arc::clone(&sender.wire), Arc::clone(&sender.closed));
        let (_tx, rx) = watch::channel(false);
        tokio::time::timeout(
            Duration::from_secs(5),
            run_tail(
                polls(&["A1", "A2", "A3"], Some("boom")),
                sender,
                FakeReceiver(RecvMode::Silent),
                test_cfg(),
                rx,
            ),
        )
        .await
        .expect("the tail closes");
        assert_eq!(lines_of_wire(&wire), ["A1", "A2", "A3"]);
        let frame = closed
            .lock()
            .unwrap()
            .clone()
            .expect("a Close")
            .expect("with a frame");
        assert_eq!(frame.code, close_code::ERROR);
        assert_eq!(frame.reason.as_str(), "boom");
    }

    /// D2 (issue #806): shutdown writes the frame in flight, then a bare
    /// Close.
    #[tokio::test(start_paused = true)]
    async fn shutdown_writes_the_frame_in_flight_before_the_close() {
        let sender = WireSender::new().flush(FlushMode::Gate);
        let (log, wire, closed, gate) = (
            Arc::clone(&sender.log),
            Arc::clone(&sender.wire),
            Arc::clone(&sender.closed),
            Arc::clone(&sender.gate),
        );
        let (tx, rx) = watch::channel(false);
        let t0 = TInstant::now();
        let handle = tokio::spawn(run_tail(
            polls(&["A1"], None),
            sender,
            FakeReceiver(RecvMode::Silent),
            test_cfg(),
            rx,
        ));
        yield_until(|| logged(&log, |w| *w == Wire::FlushStart) == 1).await;
        tx.send(true).expect("receiver alive");
        tokio::time::sleep(ms(10)).await;
        assert!(
            closed.lock().unwrap().is_none(),
            "no Close before the release"
        );
        gate.notify_one();
        handle.await.expect("no panic");
        assert!(TInstant::now() - t0 < ms(600));
        assert_eq!(lines_of_wire(&wire), ["A1"]);
        assert_eq!(*closed.lock().unwrap(), Some(None), "a bare Close");
    }

    /// D3 (issue #806): shutdown with a client that never reads: the Close
    /// starts at the drain's end and the connection ends one grace later.
    #[tokio::test(start_paused = true)]
    async fn shutdown_with_a_client_that_never_reads_ends_within_two_grace_periods() {
        let sender = WireSender::new()
            .ready(ReadyMode::Pending)
            .close(CloseMode::Block);
        let (log, wire) = (Arc::clone(&sender.log), Arc::clone(&sender.wire));
        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn(run_tail(
            FrameEveryPoll,
            sender,
            FakeReceiver(RecvMode::Silent),
            test_cfg(),
            rx,
        ));
        yield_until(|| logged(&log, |w| *w == Wire::ReadyStart) >= 1).await;
        let t0 = TInstant::now();
        tx.send(true).expect("receiver alive");
        handle.await.expect("no panic");
        let ended = TInstant::now() - t0;
        let close = time_of(&log, |w| *w == Wire::CloseStart).expect("the Close starts") - t0;
        assert!(
            close >= ms(249) && close <= ms(251),
            "the Close starts at {close:?}"
        );
        assert!(ended >= ms(499) && ended <= ms(501), "returns at {ended:?}");
        assert!(wire.lock().unwrap().is_empty());
    }

    /// D7 (issue #806): a shutdown sender that is gone starts the drain
    /// deadline, as `true` does.
    #[tokio::test(start_paused = true)]
    async fn a_dropped_shutdown_sender_starts_the_drain_deadline() {
        let sender = WireSender::new().flush(FlushMode::Sleep(ms(100)));
        let (log, wire, closed) = (
            Arc::clone(&sender.log),
            Arc::clone(&sender.wire),
            Arc::clone(&sender.closed),
        );
        let (tx, rx) = mpsc::channel(4);
        for (i, l) in ["F1", "F2", "F3"].iter().enumerate() {
            tx.try_send(page_event(l, i as i64)).expect("room");
        }
        let (shutdown_tx, shutdown) = watch::channel(false);
        drop(shutdown_tx);
        let (cancel_tx, cancel) = watch::channel(false);
        let t0 = TInstant::now();
        tokio::time::timeout(
            Duration::from_secs(2),
            writer_loop(
                sender,
                rx,
                test_cfg(),
                shutdown,
                cancel,
                Arc::new(cancel_tx),
            ),
        )
        .await
        .expect("the writer returns within 2 s");
        assert_eq!(TInstant::now() - t0, ms(GRACE_MS));
        assert_eq!(lines_of_wire(&wire), ["F1", "F2"]);
        let third = log
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, w)| matches!(w, Wire::Start(_)))
            .nth(2)
            .map(|(t, _)| *t)
            .expect("F3 handed over");
        assert_eq!(third - t0, ms(200));
        assert!(closed.lock().unwrap().is_none(), "no Close");
        drop(tx);
    }

    /// D9-D11 (issue #806): the three ways a connection must stop during a
    /// flush send no Close after the Text handed over.
    async fn stop_during_a_flush(how: &str) -> (Duration, Vec<String>, Option<Option<CloseFrame>>) {
        let sender = WireSender::new().flush(FlushMode::Pending);
        let (log, wire, closed) = (
            Arc::clone(&sender.log),
            Arc::clone(&sender.wire),
            Arc::clone(&sender.closed),
        );
        let client = Arc::new(tokio::sync::Notify::new());
        let (tx, rx) = watch::channel(false);
        let mut cfg = test_cfg();
        if how == "send timeout" {
            cfg.send_timeout = ms(100);
        }
        let handle = tokio::spawn(run_tail(
            polls(&["A1"], None),
            sender,
            SignalReceiver(Arc::clone(&client)),
            cfg,
            rx,
        ));
        yield_until(|| logged(&log, |w| matches!(w, Wire::Start(_))) == 1).await;
        let t0 = TInstant::now();
        match how {
            "client close" => client.notify_one(),
            "shutdown" => tx.send(true).expect("receiver alive"),
            _ => {}
        }
        handle.await.expect("no panic");
        let ended = TInstant::now() - t0;
        let wire = wire.lock().unwrap().clone();
        let closed = closed.lock().unwrap().clone();
        (ended, wire, closed)
    }

    #[tokio::test(start_paused = true)]
    async fn a_client_close_during_a_flush_sends_no_close_after_the_handed_over_text() {
        let (ended, wire, closed) = stop_during_a_flush("client close").await;
        assert!(wire.is_empty(), "{wire:?}");
        assert!(closed.is_none(), "{closed:?}");
        assert_eq!(ended, Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn a_send_timeout_during_a_flush_sends_no_close_after_the_handed_over_text() {
        let (ended, wire, closed) = stop_during_a_flush("send timeout").await;
        assert!(wire.is_empty(), "{wire:?}");
        assert!(closed.is_none(), "{closed:?}");
        assert!(ended <= ms(100), "{ended:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_shutdown_cut_during_a_flush_sends_no_close_after_the_handed_over_text() {
        let (ended, wire, closed) = stop_during_a_flush("shutdown").await;
        assert!(wire.is_empty(), "{wire:?}");
        assert!(closed.is_none(), "{closed:?}");
        assert_eq!(ended, ms(GRACE_MS));
    }

    /// D12 (issue #806): a hand-off that buffers and then fails drops the
    /// socket without a Close.
    #[tokio::test]
    async fn a_hand_off_error_drops_the_socket_without_a_close() {
        let sender = WireSender::new().start(StartMode::BufferThenErr);
        let (wire, closed) = (Arc::clone(&sender.wire), Arc::clone(&sender.closed));
        let (_tx, rx) = watch::channel(false);
        tokio::time::timeout(
            Duration::from_secs(5),
            run_tail(
                polls(&["A1"], None),
                sender,
                FakeReceiver(RecvMode::Silent),
                test_cfg(),
                rx,
            ),
        )
        .await
        .expect("the tail ends");
        assert!(wire.lock().unwrap().is_empty());
        assert!(closed.lock().unwrap().is_none());
    }

    /// D13 (issue #806): a step that becomes ready at its deadline hands
    /// nothing over.
    #[tokio::test(start_paused = true)]
    async fn a_step_ready_at_its_deadline_hands_nothing_over() {
        let sender = WireSender::new()
            .ready(ReadyMode::Sleep(ms(100)))
            .flush(FlushMode::Pending);
        let (buffered, wire, closed) = (
            Arc::clone(&sender.buffered),
            Arc::clone(&sender.wire),
            Arc::clone(&sender.closed),
        );
        let mut cfg = test_cfg();
        cfg.send_timeout = ms(100);
        let (_tx, rx) = watch::channel(false);
        let t0 = TInstant::now();
        run_tail(
            polls(&["A1"], None),
            sender,
            FakeReceiver(RecvMode::Silent),
            cfg,
            rx,
        )
        .await;
        assert!(buffered.lock().unwrap().is_empty());
        assert!(wire.lock().unwrap().is_empty());
        assert_eq!(*closed.lock().unwrap(), Some(None), "a bare Close");
        assert_eq!(TInstant::now() - t0, ms(100));
    }

    /// D15 (issue #806, code review round 1): shutdown that reaches the
    /// connection while the socket is not yet ready still bounds the flush
    /// that follows: it is cut at the drain deadline, not the send timeout.
    #[tokio::test(start_paused = true)]
    async fn a_shutdown_during_readiness_bounds_the_flush_that_follows() {
        let sender = WireSender::new()
            .ready(ReadyMode::Gate)
            .flush(FlushMode::Pending);
        let (log, wire, closed, ready_gate) = (
            Arc::clone(&sender.log),
            Arc::clone(&sender.wire),
            Arc::clone(&sender.closed),
            Arc::clone(&sender.ready_gate),
        );
        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn(run_tail(
            polls(&["A1"], None),
            sender,
            FakeReceiver(RecvMode::Silent),
            test_cfg(),
            rx,
        ));
        yield_until(|| logged(&log, |w| *w == Wire::ReadyStart) == 1).await;
        let t0 = TInstant::now();
        tx.send(true).expect("receiver alive");
        tokio::time::sleep(ms(10)).await;
        ready_gate.notify_one();
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("the flush is cut at the drain deadline, not the 60 s send timeout")
            .expect("no panic");
        assert_eq!(TInstant::now() - t0, ms(GRACE_MS));
        assert_eq!(
            logged(&log, |w| matches!(w, Wire::Start(_))),
            1,
            "A1 was handed over"
        );
        assert!(wire.lock().unwrap().is_empty());
        assert!(
            closed.lock().unwrap().is_none(),
            "no Close after a half-sent frame"
        );
    }

    /// The select order of one socket step (issue #806, code review round
    /// 1): with the deadline already reached and the step ready, the step
    /// fails without the step being polled.
    #[tokio::test(start_paused = true)]
    async fn a_step_whose_deadline_has_come_is_not_polled() {
        let mut st = conn(4);
        let mut src = ScriptSource(VecDeque::new());
        let (mut rx_open, mut shutdown_open) = (true, true);
        let (_shutdown_tx, mut shutdown) = watch::channel(false);
        let (_cancel_tx, mut cancel) = watch::channel(false);
        let mut inputs = Inputs {
            st: &mut st,
            src: &mut src,
            rx_open: &mut rx_open,
            shutdown: &mut shutdown,
            shutdown_open: &mut shutdown_open,
            cancel: &mut cancel,
        };
        let polled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let seen = Arc::clone(&polled);
        let ready_step = std::future::poll_fn(move |_| {
            seen.store(true, Ordering::SeqCst);
            std::task::Poll::Ready(Ok(()))
        });
        let ok = inputs.step(ready_step, TInstant::now()).await;
        assert!(!ok, "the step fails at its deadline");
        assert!(
            !polled.load(Ordering::SeqCst),
            "a step whose deadline has come is not polled"
        );
    }

    /// A deadline between timer ticks (issue #806, code review round 2):
    /// the timer wakes on whole milliseconds, so it can still be pending
    /// just after the deadline. On a paused clock the deadline is 300 us
    /// past a tick and the clock is moved to 301 us, so the deadline has
    /// passed and the timer has not fired. A step that is ready from then
    /// on must not be polled: the step fails.
    #[tokio::test(start_paused = true)]
    async fn a_step_is_never_polled_after_a_deadline_between_timer_ticks() {
        let mut st = conn(4);
        let mut src = ScriptSource(VecDeque::new());
        let (mut rx_open, mut shutdown_open) = (true, true);
        let (_shutdown_tx, mut shutdown) = watch::channel(false);
        let (_cancel_tx, mut cancel) = watch::channel(false);
        let mut inputs = Inputs {
            st: &mut st,
            src: &mut src,
            rx_open: &mut rx_open,
            shutdown: &mut shutdown,
            shutdown_open: &mut shutdown_open,
            cancel: &mut cancel,
        };
        let deadline = TInstant::now() + Duration::from_micros(300);
        let late = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let seen = Arc::clone(&late);
        // Pending, waking itself at once, until the deadline has passed;
        // then ready.
        let step = std::future::poll_fn(move |cx| {
            if TInstant::now() >= deadline {
                seen.store(true, Ordering::SeqCst);
                return std::task::Poll::Ready(Ok(()));
            }
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        });
        let (ok, ()) = tokio::join!(inputs.step(step, deadline), async {
            tokio::time::advance(Duration::from_micros(301)).await;
        });
        assert!(!ok, "the step fails at its deadline");
        assert!(
            !late.load(Ordering::SeqCst),
            "the step was polled after its deadline"
        );
    }

    /// A scripted WebSocket sink (issue #806). It logs every call and
    /// rejects a `start_send` not preceded by a ready `poll_ready`.
    #[derive(Default)]
    struct ScriptSink {
        log: Arc<Mutex<Vec<String>>>,
        written: Arc<Mutex<Vec<String>>>,
        buffer: Vec<String>,
        /// `false`: that `poll_ready` is pending once; empty: ready.
        ready_script: VecDeque<bool>,
        /// `false`: that `poll_flush` is pending, with no wake; empty: ready.
        flush_script: VecDeque<bool>,
        text_err_after_buffer: bool,
        ready_seen: bool,
        /// After the first `start_send`, `poll_ready` is pending, waking
        /// itself at once, until the clock reaches this instant.
        ready_from_after_first_send: Option<TInstant>,
        sends: usize,
    }

    impl Sink<Message> for ScriptSink {
        type Error = ();

        fn poll_ready(
            mut self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), ()>> {
            let held = self.sends > 0
                && self
                    .ready_from_after_first_send
                    .is_some_and(|from| TInstant::now() < from);
            if held || self.ready_script.pop_front() == Some(false) {
                self.log
                    .lock()
                    .unwrap()
                    .push("poll_ready (pending)".to_string());
                cx.waker().wake_by_ref();
                return std::task::Poll::Pending;
            }
            self.log.lock().unwrap().push("poll_ready".to_string());
            self.ready_seen = true;
            std::task::Poll::Ready(Ok(()))
        }

        fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), ()> {
            let (label, text) = match &item {
                Message::Text(t) => ("start_send(Text)", t.to_string()),
                Message::Close(_) => ("start_send(Close)", "Close".to_string()),
                _ => ("start_send(other)", String::new()),
            };
            self.sends += 1;
            if !std::mem::take(&mut self.ready_seen) {
                self.log.lock().unwrap().push(format!("rejected {label}"));
                return Err(());
            }
            self.log.lock().unwrap().push(label.to_string());
            self.buffer.push(text);
            if matches!(item, Message::Text(_)) && self.text_err_after_buffer {
                return Err(());
            }
            Ok(())
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), ()>> {
            self.log.lock().unwrap().push("poll_flush".to_string());
            if self.flush_script.pop_front() == Some(false) {
                return std::task::Poll::Pending;
            }
            let moved: Vec<String> = self.buffer.drain(..).collect();
            self.written.lock().unwrap().extend(moved);
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), ()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    /// Runs `writer_loop` over `SinkSender(sink)` with `lines` queued; the
    /// channel closes after them when `close` is set. `cancel_after_start`
    /// sets the cancellation once a Text has been handed over.
    async fn drive_sink(
        sink: ScriptSink,
        lines: &[&str],
        close: bool,
        cancel_after_start: bool,
    ) -> (Vec<String>, Vec<String>) {
        let (log, written) = (Arc::clone(&sink.log), Arc::clone(&sink.written));
        let (tx, rx) = mpsc::channel(4);
        for (i, l) in lines.iter().enumerate() {
            tx.try_send(page_event(l, i as i64)).expect("room");
        }
        let keep = if close {
            drop(tx);
            None
        } else {
            Some(tx)
        };
        let (_shutdown_tx, shutdown) = watch::channel(false);
        let (cancel_tx, cancel) = watch::channel(false);
        let cancel_tx = Arc::new(cancel_tx);
        let handle = tokio::spawn(writer_loop(
            SinkSender(sink),
            rx,
            test_cfg(),
            shutdown,
            cancel,
            Arc::clone(&cancel_tx),
        ));
        if cancel_after_start {
            yield_until(|| log.lock().unwrap().iter().any(|l| l == "start_send(Text)")).await;
            cancel_tx.send(true).expect("open");
        }
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("the writer returns")
            .expect("no panic");
        drop(keep);
        let log = log.lock().unwrap().clone();
        let written = written.lock().unwrap().clone();
        (log, written)
    }

    /// P5 (issue #806, code review round 3): the Close is tried for at most
    /// `CLOSE_GRACE`. A socket that becomes ready only when that time is up
    /// gets no Close: nothing is handed over or written after it.
    #[tokio::test(start_paused = true)]
    async fn the_production_close_hands_nothing_over_after_its_deadline() {
        let t0 = TInstant::now();
        let sink = ScriptSink {
            ready_from_after_first_send: Some(t0 + ms(GRACE_MS)),
            ..ScriptSink::default()
        };
        let (log, written) = (Arc::clone(&sink.log), Arc::clone(&sink.written));
        let (tx, rx) = mpsc::channel(4);
        tx.try_send(page_event("A1", 1)).expect("room");
        drop(tx);
        let (_shutdown_tx, shutdown) = watch::channel(false);
        let (cancel_tx, cancel) = watch::channel(false);
        let handle = tokio::spawn(writer_loop(
            SinkSender(sink),
            rx,
            test_cfg(),
            shutdown,
            cancel,
            Arc::new(cancel_tx),
        ));
        // A1 is written; the Close waits for readiness.
        yield_until(|| {
            log.lock()
                .unwrap()
                .iter()
                .any(|l| l == "poll_ready (pending)")
        })
        .await;
        tokio::time::advance(ms(GRACE_MS)).await;
        yield_until(|| handle.is_finished()).await;
        handle.await.expect("no panic");
        let log = log.lock().unwrap().clone();
        assert!(
            !log.iter().any(|l| l == "start_send(Close)"),
            "the Close is not handed over at its deadline: {log:?}"
        );
        let written = written.lock().unwrap().clone();
        assert_eq!(written.len(), 1, "only A1 is written: {written:?}");
    }

    /// P1 (issue #806): the production sender drops the socket when the
    /// sink errors after buffering a Text.
    #[tokio::test]
    async fn the_production_sender_drops_the_socket_when_the_sink_errors_after_buffering() {
        let sink = ScriptSink {
            text_err_after_buffer: true,
            ..ScriptSink::default()
        };
        let (log, written) = drive_sink(sink, &["A1"], false, false).await;
        assert_eq!(log, ["poll_ready", "start_send(Text)"]);
        assert!(written.is_empty(), "{written:?}");
    }

    /// P2 (issue #806): the production sender sends no Close after a
    /// cancelled flush.
    #[tokio::test(start_paused = true)]
    async fn the_production_sender_sends_no_close_after_a_cancelled_flush() {
        let sink = ScriptSink {
            flush_script: VecDeque::from([false]),
            ..ScriptSink::default()
        };
        let (log, written) = drive_sink(sink, &["A1"], false, true).await;
        assert_eq!(
            log.last().map(String::as_str),
            Some("poll_flush"),
            "{log:?}"
        );
        assert!(!log.iter().any(|l| l.contains("Close")), "{log:?}");
        assert!(written.is_empty(), "{written:?}");
    }

    /// P3 (issue #806): the normal path writes the Text, then the Close.
    #[tokio::test]
    async fn the_production_sender_writes_text_then_close() {
        let (log, written) = drive_sink(ScriptSink::default(), &["A1"], true, false).await;
        assert_eq!(written.len(), 2, "{log:?}");
        assert!(written[0].contains("A1"), "{written:?}");
        assert_eq!(written[1], "Close");
    }

    /// P4 (issue #806): the production Close waits for readiness.
    #[tokio::test]
    async fn the_production_close_waits_for_readiness() {
        let sink = ScriptSink {
            ready_script: VecDeque::from([true, false, true]),
            ..ScriptSink::default()
        };
        let (log, _) = drive_sink(sink, &["A1"], true, false).await;
        assert_eq!(
            log,
            [
                "poll_ready",
                "start_send(Text)",
                "poll_flush",
                "poll_ready (pending)",
                "poll_ready",
                "start_send(Close)",
                "poll_flush"
            ]
        );
    }

    // -- param parsing ---------------------------------------------------

    fn cfg_default() -> Config {
        Config::default()
    }

    #[test]
    fn tail_params_reject_a_metric_query() {
        let pairs = params::parse_pairs("query=count_over_time(%7Ba%3D%22x%22%7D%5B1m%5D)");
        let err = parse_tail_params(&pairs, &cfg_default(), 7).expect_err("metric query");
        assert!(matches!(
            err,
            ApiError::Param(ParamError::MetricQueryUnsupported { endpoint: "tail" })
        ));
    }

    /// Issue #279 (AC4): `/tail` cannot use the poolless-router probe the
    /// other surfaces get — axum's `WebSocketUpgrade` extractor rejects a
    /// plain `GET` with its own 400 (`Connection header did not include
    /// 'upgrade'`) BEFORE the handler body runs, so an HTTP probe never
    /// reaches `parse`. The cap is therefore gated here, directly on
    /// `parse_tail_params`: a valid selector of exactly 131,072 bytes
    /// (`MAX_QUERY_BYTES`, the reference's `maxInputSize`; one byte past
    /// the longest accepted query) rejects as the cap, before any
    /// socket/pool work.
    #[test]
    fn tail_params_reject_an_over_cap_query_as_query_too_long() {
        let pairs = vec![(
            "query".to_string(),
            format!(r#"{{app="{}"}}"#, "a".repeat(131_072 - 8)),
        )];
        let err = parse_tail_params(&pairs, &cfg_default(), 7).expect_err("over-cap query");
        assert!(matches!(
            err,
            ApiError::LogQl(pulsus_logql::LogQlError::QueryTooLong { .. })
        ));
    }

    #[test]
    fn tail_params_reject_zero_and_non_numeric_limits_but_clamp_large_ones() {
        let base = "query=%7Ba%3D%22x%22%7D";
        let zero = params::parse_pairs(&format!("{base}&limit=0"));
        assert!(matches!(
            parse_tail_params(&zero, &cfg_default(), 7),
            Err(ApiError::Param(ParamError::InvalidTailLimit(_)))
        ));
        let bad = params::parse_pairs(&format!("{base}&limit=abc"));
        assert!(matches!(
            parse_tail_params(&bad, &cfg_default(), 7),
            Err(ApiError::Param(ParamError::InvalidTailLimit(_)))
        ));
        let big = params::parse_pairs(&format!("{base}&limit=999999999"));
        let p = parse_tail_params(&big, &cfg_default(), 7).expect("clamped, not rejected");
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
            parse_tail_params(&none, &cfg_default(), 7)
                .expect("ok")
                .delay_ns,
            0
        );
        let over = params::parse_pairs(&format!("{base}&delay_for=3600"));
        let p = parse_tail_params(&over, &cfg_default(), 7).expect("clamped");
        assert_eq!(p.delay_ns, 5_000_000_000, "clamped at tail_max_delay (5s)");
        let bad = params::parse_pairs(&format!("{base}&delay_for=soon"));
        assert!(matches!(
            parse_tail_params(&bad, &cfg_default(), 7),
            Err(ApiError::Param(ParamError::InvalidDelayFor(_)))
        ));
    }

    #[test]
    fn tail_params_default_start_is_an_hour_ago() {
        let pairs = params::parse_pairs("query=%7Ba%3D%22x%22%7D");
        let before = params::now_ns();
        let p = parse_tail_params(&pairs, &cfg_default(), 7).expect("ok");
        let after = params::now_ns();
        let hour = 3_600_000_000_000i64;
        assert!(p.start_ns >= before - hour && p.start_ns <= after - hour);
    }

    /// Issue #391: `/tail`'s three scalar params collapse present-but-
    /// empty into absent, like every other scalar on this surface.
    ///
    /// Gated HERE rather than on `mod.rs`'s routed identity table for the
    /// same reason as `tail_params_reject_an_over_cap_query_as_query_too_long`
    /// above: axum's `WebSocketUpgrade` extractor rejects a plain `GET`
    /// before the handler body runs, so a routed probe would measure
    /// axum's rejection and not our parsing — an identity that holds for
    /// every parameter, including one we got wrong.
    ///
    /// Container-measured against `grafana/loki:3.7.4`, 2026-08-09:
    /// `/loki/api/v1/tail` with `limit=`, `delay_for=` or `start=` reaches
    /// the websocket-upgrade stage (a generic `Bad Request`) rather than a
    /// param error, while `delay_for=%20` is
    /// `strconv.Atoi: parsing " ": invalid syntax` and `limit=0` is
    /// `limit must be a positive value` — empty is accepted, a space is
    /// not.
    ///
    /// `start_ns` is compared within a 5 s window rather than for
    /// equality: its absent-path default derives from `now_ns()`, which
    /// advances between the two calls. `fetch_limit` and `delay_ns` are
    /// exact.
    #[test]
    fn empty_tail_params_default_exactly_as_absent_ones() {
        let cfg = cfg_default();
        let base = "query=%7Ba%3D%22x%22%7D";
        let absent = parse_tail_params(&params::parse_pairs(base), &cfg, 7).expect("absent params");
        for param in ["limit", "delay_for", "start"] {
            let pairs = params::parse_pairs(&format!("{base}&{param}="));
            let empty = parse_tail_params(&pairs, &cfg, 7)
                .unwrap_or_else(|e| panic!("`{param}=` must parse as absent, got {e:?}"));
            assert_eq!(
                empty.fetch_limit, absent.fetch_limit,
                "`{param}=` must yield the absent-path fetch_limit"
            );
            assert_eq!(
                empty.delay_ns, absent.delay_ns,
                "`{param}=` must yield the absent-path delay_ns"
            );
            assert!(
                (empty.start_ns - absent.start_ns).abs() < 5_000_000_000,
                "`{param}=` must yield the absent-path start_ns (within the wall-clock window), \
                 got {} vs {}",
                empty.start_ns,
                absent.start_ns
            );
        }
    }

    // -- issue #803: a poll whose server failed is repeated once ----------

    /// One scripted poll result, and what `take_server_failure` answers
    /// after it.
    type FailoverStep = (Result<TailPage, ReadError>, bool);

    /// Plays `script` one poll at a time, recording each poll's lower
    /// bound; past the end every poll waits forever, so the tail stays open
    /// and the scripted polls are the only ones counted.
    struct FailoverFetcher {
        script: VecDeque<FailoverStep>,
        lowers: Arc<Mutex<Vec<String>>>,
        failure: bool,
    }

    impl TailFetcher for FailoverFetcher {
        async fn poll(
            &mut self,
            lower: TailLower,
            _upper_ns: i64,
            _narrow: bool,
            _fetch_limit: u32,
        ) -> Result<TailPage, ReadError> {
            let Some((result, failure)) = self.script.pop_front() else {
                return std::future::pending().await;
            };
            self.lowers.lock().unwrap().push(format!("{lower:?}"));
            self.failure = failure;
            result
        }

        fn take_server_failure(&mut self) -> bool {
            std::mem::take(&mut self.failure)
        }
    }

    fn failover_page(lines: &[&str]) -> TailPage {
        let now = params::now_ns();
        TailPage {
            streams: vec![stream(
                r#"{"app":"x"}"#,
                lines.iter().map(|l| (now, *l)).collect(),
            )],
            next: None,
            fetched: lines.len() as u32,
            joined: Vec::new(),
        }
    }

    fn failover_error(reason: &str) -> ReadError {
        ReadError::PipelineInvalid {
            reason: reason.to_string(),
        }
    }

    /// What a scripted tail did: its polls' lower bounds, every frame sent,
    /// and the close (outer `Some` once closed).
    struct FailoverRun {
        lowers: Arc<Mutex<Vec<String>>>,
        sent: Arc<Mutex<Vec<String>>>,
        closed: Arc<Mutex<Option<Option<CloseFrame>>>>,
        shutdown: watch::Sender<bool>,
        handle: tokio::task::JoinHandle<()>,
    }

    fn run_failover(script: Vec<FailoverStep>) -> FailoverRun {
        let lowers = Arc::new(Mutex::new(Vec::new()));
        let fetcher = FailoverFetcher {
            script: script.into(),
            lowers: Arc::clone(&lowers),
            failure: false,
        };
        let sender = WireSender::new();
        let (sent, closed) = (Arc::clone(&sender.wire), Arc::clone(&sender.closed));
        let (shutdown, rx) = watch::channel(false);
        let handle = tokio::spawn(run_tail(
            fetcher,
            sender,
            FakeReceiver(RecvMode::Silent),
            test_cfg(),
            rx,
        ));
        FailoverRun {
            lowers,
            sent,
            closed,
            shutdown,
            handle,
        }
    }

    /// Waits (10 ms steps, at most 5 s) until `done` holds.
    async fn wait_for_failover(mut done: impl FnMut() -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !done() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn failover_delivered(sent: &Mutex<Vec<String>>, line: &str) -> bool {
        sent.lock()
            .unwrap()
            .iter()
            .any(|f| f.contains(&format!("\"{line}\"")))
    }

    async fn stop_failover(run: FailoverRun) {
        let _ = run.shutdown.send(true);
        tokio::time::timeout(Duration::from_secs(1), run.handle)
            .await
            .expect("run_tail returns on shutdown")
            .expect("no panic");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_server_failure_repeats_the_poll_once_at_the_same_lower_bound() {
        let run = run_failover(vec![
            (Err(failover_error("server failed")), true),
            (Ok(failover_page(&["line-a", "line-b"])), false),
        ]);
        wait_for_failover(|| {
            failover_delivered(&run.sent, "line-a") && failover_delivered(&run.sent, "line-b")
        })
        .await;
        assert!(failover_delivered(&run.sent, "line-a"), "line-a delivered");
        assert!(failover_delivered(&run.sent, "line-b"), "line-b delivered");
        assert!(run.closed.lock().unwrap().is_none(), "the tail is open");
        let lowers = run.lowers.lock().unwrap().clone();
        assert_eq!(lowers.len(), 2, "{lowers:?}");
        assert_eq!(lowers[1], lowers[0], "poll 2 repeats poll 1's lower bound");
        stop_failover(run).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn two_server_failures_in_a_row_close_the_tail() {
        let run = run_failover(vec![
            (Err(failover_error("first failure")), true),
            (Err(failover_error("second failure")), true),
        ]);
        wait_for_failover(|| run.closed.lock().unwrap().is_some()).await;
        let closed = run.closed.lock().unwrap().clone();
        let frame = closed
            .expect("the tail closed")
            .expect("with an error frame");
        assert_eq!(frame.reason.as_str(), "second failure");
        assert_eq!(run.lowers.lock().unwrap().len(), 2, "exactly two polls");
        stop_failover(run).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_poll_error_that_is_not_a_server_failure_closes_at_once() {
        let run = run_failover(vec![
            (
                Err(ReadError::QueryTooBroad(
                    pulsus_read::logql::TooBroadReason::StreamCap {
                        count: 200_000,
                        cap: 100_000,
                    },
                )),
                false,
            ),
            (Ok(failover_page(&["never"])), false),
        ]);
        wait_for_failover(|| run.closed.lock().unwrap().is_some()).await;
        assert!(run.closed.lock().unwrap().is_some(), "the tail closed");
        assert_eq!(run.lowers.lock().unwrap().len(), 1, "after one poll");
        assert!(!failover_delivered(&run.sent, "never"));
        stop_failover(run).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_successful_poll_resets_the_fail_over() {
        let run = run_failover(vec![
            (Err(failover_error("first failure")), true),
            (Ok(failover_page(&["line-1"])), false),
            (Err(failover_error("second failure")), true),
            (Ok(failover_page(&["line-2"])), false),
        ]);
        wait_for_failover(|| {
            failover_delivered(&run.sent, "line-2") || run.closed.lock().unwrap().is_some()
        })
        .await;
        assert!(failover_delivered(&run.sent, "line-1"), "line-1 delivered");
        assert!(failover_delivered(&run.sent, "line-2"), "line-2 delivered");
        assert_eq!(run.lowers.lock().unwrap().len(), 4, "four polls");
        assert!(run.closed.lock().unwrap().is_none(), "the tail is open");
        stop_failover(run).await;
    }
}
