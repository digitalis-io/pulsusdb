//! `serve::run`: process entry point for every *serving* mode (`all`,
//! `writer`, `reader` — never `init`, which `main.rs` dispatches to
//! `schema_init::run` and exits before this module is ever reached).
//! Initializes tracing, installs the Prometheus recorder, spawns the
//! background ClickHouse reconnect loop, builds the router, and serves it
//! with graceful shutdown.
//!
//! Data flow: req → CORS → gzip → TraceLayer(span) → [ops-authed group:
//! RequestDeadlineLayer(query_timeout) → auth(opt) → subsystem/compat routes], with
//! `/ready`/`/metrics` mounted outside the bracketed group entirely (see
//! `app::build_router`).

use std::process::ExitCode;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use pulsus_clickhouse::{ChClient, ChError, ChPool, spawn_reprobe_loop};
use pulsus_config::{Config, LogLevel, Mode};
use pulsus_read::LabelCache;
use pulsus_schema::{NameCatalogue, REQUIRED_SERVER_NAMES, SchemaError};
use pulsus_write::{LogWriter, MetricWriter, TraceWriter};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing_subscriber::EnvFilter;

use crate::app::{self, AppState, BuildInfo};
use crate::chconfig::{
    NameCheckError, build_label_cache, check_schema_present, check_server_names, conn_config_from,
    consistency_from, metric_writer_tables_from, trace_writer_tables_from, writer_tables_from,
};
use crate::ingest::{MetricWriterSink, TraceWriterSink, WriterSink};

/// Startup-time failures specific to the HTTP server layer (config-load and
/// schema-controller failures are handled separately, in `main.rs` /
/// `schema_init.rs`, before this module is reached).
#[derive(Debug, Error)]
pub(crate) enum ServeError {
    #[error("PULSUS_CORS_ORIGIN {0:?} is not a valid HTTP header value")]
    InvalidCorsOrigin(String),
    #[error("failed to bind {addr}: {source}")]
    Bind {
        addr: String,
        source: std::io::Error,
    },
    #[error("failed to initialize TLS: {0}")]
    Tls(#[from] crate::tls::TlsError),
}

/// The bound on [`LogWriter::shutdown`]'s drain, run between graceful HTTP
/// shutdown and background-task/pool teardown (issue #15 architect plan).
/// A documented constant for now, not a `PULSUS_*` var (task-manager
/// resolution: promote to config only when a deployment needs to tune it —
/// the same precedent `writer::config::WriterRuntime`'s own documented
/// constants set).
const WRITER_DRAIN_DEADLINE: Duration = Duration::from_secs(10);

/// Runs one serving process (`all`/`writer`/`reader`) to completion: init
/// tracing, install the metrics recorder, spawn the reconnect loop, build
/// the router, and serve with graceful shutdown. Shutdown ordering
/// (architect plan amendment, extended to the writer by issue #15):
/// graceful stop (accept loop drains in-flight requests — including a
/// sync-ingest request's held-open `FlushWait`) → drain the writer
/// (`LogWriter::shutdown`, bounded by [`WRITER_DRAIN_DEADLINE`]) → abort
/// the reconnect task, then join it → abort the label cache refresh and
/// re-probe tasks (if either was ever started), then join them → only then
/// does `pool_slot` (and the `ChPool` it may hold) drop, so pool teardown
/// never races an in-flight `connect`/`ping` *or* a still-draining writer
/// flush.
pub async fn run(config: Config) -> ExitCode {
    init_tracing(config.log_level);
    // Issue #311: the LogQL template zone comes from configuration, before
    // any query can compile a pipeline. Nothing downstream reads `$TZ` or
    // `/etc/localtime`, so this is the only thing that decides it — and
    // every node sharing this configuration renders identically.
    install_template_timezone(&config);

    let metrics = install_metrics_recorder();
    let config = Arc::new(config);
    let pool_slot: Arc<RwLock<Option<Arc<ChPool>>>> = Arc::new(RwLock::new(None));
    // Async-filled at most once by the reconnect loop, same shape as
    // `pool_slot` but a `OnceLock` (no readers before the first write ever
    // race a write — `WriterSink::admit`/`admit_flush` just see `None` and
    // return `Backpressure`, no lock needed) — set *before* `pool_slot` so
    // `/ready`=200 implies the ingest route is live too (issue #15
    // architect plan).
    let writer_slot: Arc<OnceLock<Arc<LogWriter>>> = Arc::new(OnceLock::new());
    // `MetricWriter`'s lifecycle-parity counterpart (issue #26 architect
    // plan): constructed + shutdown-drained alongside `LogWriter`. Wired
    // into `AppState` (via `MetricWriterSink`) and `/v1/metrics` below
    // (issue #27); `/api/v1/write` (Prometheus remote write) still lands
    // in #28. Its flush tasks simply idle (never admitted to) until this
    // slot is filled by the reconnect loop.
    let metric_writer_slot: Arc<OnceLock<Arc<MetricWriter>>> = Arc::new(OnceLock::new());
    // `TraceWriter`'s slot (issue #54): same lifecycle as the other two
    // writer slots — constructed by the reconnect loop in writer-enabled
    // modes, drained alongside them at shutdown.
    let trace_writer_slot: Arc<OnceLock<Arc<TraceWriter>>> = Arc::new(OnceLock::new());
    // The label cache's async-filled slot (issue #30 architect plan): same
    // shape as `writer_slot`/`metric_writer_slot`, constructed by the
    // reconnect loop only in reader-enabled modes (see `reader_enabled`).
    // `ops::ready` gates on `label_cache.get().is_some_and(|c| c.is_warm())`.
    let label_cache_slot: Arc<OnceLock<Arc<LabelCache>>> = Arc::new(OnceLock::new());
    // The label cache's refresh-loop handle, handed back over a oneshot
    // channel so `run` can abort and join it at shutdown. The reconnect
    // loop is a one-shot bootstrap (see its own doc comment): it runs
    // exactly once per process and spawns at most one of each task, so no
    // duplication or leak risk follows.
    let (label_cache_refresh_tx, label_cache_refresh_rx) = oneshot::channel();
    // The background ClickHouse-endpoint re-probe task's handle, handed back
    // the same way (issue #43 re-probe plan: spawned unconditionally right
    // after `pool_slot` is published — every serving mode holds a pool —
    // and abort+joined in shutdown ordering after the refresh handle).
    let (reprobe_tx, reprobe_rx) = oneshot::channel();
    let reconnect_handle = spawn_reconnect_loop(
        Arc::clone(&pool_slot),
        WriterSlots {
            log: Arc::clone(&writer_slot),
            metric: Arc::clone(&metric_writer_slot),
            trace: Arc::clone(&trace_writer_slot),
        },
        Arc::clone(&label_cache_slot),
        Arc::clone(&config),
        BackgroundHandoff {
            label_cache_refresh: label_cache_refresh_tx,
            reprobe: reprobe_tx,
        },
        REQUIRED_SERVER_NAMES,
    );

    // The live-tail shutdown signal (issue #74): fired inside the
    // graceful-shutdown future below so every tail poll/send loop breaks
    // BEFORE `axum::serve` waits on in-flight connections — a long-lived
    // WebSocket can never wedge the shutdown ordering that follows
    // (writer drain, background-task teardown).
    let (tail_shutdown_tx, tail_shutdown_rx) = tokio::sync::watch::channel(false);

    let state = AppState {
        pool: Arc::clone(&pool_slot),
        config: Arc::clone(&config),
        metrics,
        build: BuildInfo::from_build_env(),
        writer: Arc::new(WriterSink::new(Arc::clone(&writer_slot))),
        metric_writer: Arc::new(MetricWriterSink::new(Arc::clone(&metric_writer_slot))),
        trace_writer: Arc::new(TraceWriterSink::new(Arc::clone(&trace_writer_slot))),
        label_cache: Arc::clone(&label_cache_slot),
        eval_gate: Arc::new(pulsus_read::EvalGate::new(
            config.reader.query_eval_concurrency,
        )),
        started_at: std::time::SystemTime::now(),
        tail: Arc::new(crate::app::TailRuntime::new(
            tail_shutdown_rx,
            config.reader.tail_max_connections,
        )),
    };

    let router = match app::build_router(state, &config) {
        Ok(router) => router,
        Err(err) => {
            eprintln!("pulsusdb: {err}");
            shutdown_background_tasks(reconnect_handle, label_cache_refresh_rx, reprobe_rx).await;
            return ExitCode::FAILURE;
        }
    };

    // Inbound TLS (issue #174): load the cert/key pair *before* the bind,
    // so a missing/unreadable/malformed pair is a clean pre-listen startup
    // failure (the exact `build_router`-failure shape above) — never a
    // half-started server. `pulsus_config::validate` already rejected any
    // one-sided config (Rule 16), so `tls_paths` is all-or-nothing here.
    let tls_config = match tls_paths(&config) {
        Some((cert_path, key_path)) => match crate::tls::load_server_config(cert_path, key_path) {
            Ok(tls_config) => Some(tls_config),
            Err(source) => {
                eprintln!("pulsusdb: {}", ServeError::Tls(source));
                shutdown_background_tasks(reconnect_handle, label_cache_refresh_rx, reprobe_rx)
                    .await;
                return ExitCode::FAILURE;
            }
        },
        None => None,
    };

    let addr = format!("{}:{}", config.host, config.port);
    let listener = match TcpListener::bind(&addr).await {
        Ok(listener) => listener,
        Err(source) => {
            eprintln!("pulsusdb: {}", ServeError::Bind { addr, source });
            shutdown_background_tasks(reconnect_handle, label_cache_refresh_rx, reprobe_rx).await;
            return ExitCode::FAILURE;
        }
    };

    let proto = if tls_config.is_some() {
        "https"
    } else {
        "http"
    };
    tracing::info!(%addr, mode = ?config.mode, proto, "pulsusdb listening");

    // Both arms share the identical graceful-shutdown future and ordering
    // contract; only the listener differs. The `None` arm is today's
    // plaintext path, unchanged.
    let serve_result = match tls_config {
        Some(tls_config) => {
            axum::serve(crate::tls::TlsListener::new(listener, tls_config), router)
                .with_graceful_shutdown(async move {
                    shutdown_signal().await;
                    // Break every live-tail loop first (issue #74): axum's
                    // graceful stop waits for in-flight connections, and an
                    // upgraded WebSocket is in-flight until its handler
                    // returns.
                    let _ = tail_shutdown_tx.send(true);
                })
                .await
        }
        None => {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    shutdown_signal().await;
                    // Break every live-tail loop first (issue #74): axum's
                    // graceful stop waits for in-flight connections, and an
                    // upgraded WebSocket is in-flight until its handler
                    // returns.
                    let _ = tail_shutdown_tx.send(true);
                })
                .await
        }
    };
    if let Err(err) = serve_result {
        tracing::error!(error = %err, "server exited with an error");
    }

    // Drain any still-buffered/in-flight writer generations (async-mode
    // admits that never had a request-scoped `FlushWait` to hold graceful
    // shutdown open for) before the background tasks stop and
    // `pool_slot` drops (issue #15 architect plan). A no-op when this
    // process never mounted the writer (`writer_slot` stays empty).
    if let Some(writer) = writer_slot.get() {
        writer.shutdown(WRITER_DRAIN_DEADLINE).await;
    }
    // Same drain, same deadline, for `MetricWriter` (issue #26) — a no-op
    // when this process never mounted the writer subsystem.
    if let Some(metric_writer) = metric_writer_slot.get() {
        metric_writer.shutdown(WRITER_DRAIN_DEADLINE).await;
    }
    // And for `TraceWriter` (issue #54), same contract.
    if let Some(trace_writer) = trace_writer_slot.get() {
        trace_writer.shutdown(WRITER_DRAIN_DEADLINE).await;
    }

    shutdown_background_tasks(reconnect_handle, label_cache_refresh_rx, reprobe_rx).await;

    ExitCode::SUCCESS
}

/// Stops the reconnect task, then (if one was ever spawned) the label cache
/// refresh task, then (if one was ever spawned) the background re-probe
/// task — in that order, and always joined (never just
/// aborted-and-dropped) so callers never race `pool_slot`'s teardown
/// against an in-flight `connect`/`ping`/refresh sweep/re-probe pass.
/// Shared by the two pre-listen startup failure paths above and the normal
/// end-of-`run` shutdown path, so the ordering contract lives in exactly
/// one place.
async fn shutdown_background_tasks(
    reconnect_handle: JoinHandle<()>,
    label_cache_refresh_rx: oneshot::Receiver<JoinHandle<()>>,
    reprobe_rx: oneshot::Receiver<JoinHandle<()>>,
) {
    reconnect_handle.abort();
    let _ = reconnect_handle.await;

    // By now the reconnect task's fate (finished, or aborted mid-flight) is
    // sealed, so the sender side of `label_cache_refresh_rx`/`reprobe_rx`
    // has either already sent (task running) or been dropped
    // (writer-only/no-reader mode, or aborted before its first successful
    // pass) — these `.await`s therefore resolve immediately either way,
    // never hanging.
    if let Ok(refresh_handle) = label_cache_refresh_rx.await {
        refresh_handle.abort();
        let _ = refresh_handle.await;
    }
    // The re-probe task is spawned unconditionally the instant `pool_slot`
    // is published (issue #43 re-probe plan), so `reprobe_rx` only ever
    // fails to resolve here if the reconnect loop was aborted before its
    // first successful pass — same "never started" case the refresh
    // receiver above already handles.
    if let Ok(reprobe_handle) = reprobe_rx.await {
        reprobe_handle.abort();
        let _ = reprobe_handle.await;
    }
}

/// Initial backoff before retrying a failed `ChPool::connect`.
const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
/// Cap on the reconnect backoff so a persistently-unreachable ClickHouse
/// still gets retried at a bounded interval.
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// A failure from either half of [`ensure_schema_then_connect`] — the
/// reconnect loop only needs to log and retry the whole sequence, never
/// branch on kind.
#[derive(Debug, Error)]
enum StartupError {
    #[error("bootstrap connect failed: {0}")]
    Bootstrap(ChError),
    #[error("schema check failed: {0}")]
    Schema(SchemaError),
    #[error("clickhouse pool connect failed: {0}")]
    Pool(ChError),
    /// Issue #603: the server does not have a setting or function this build
    /// sends. **Terminal, not transient**: a name the server lacks is not
    /// going to appear, so the bootstrap task returns, nothing publishes, and
    /// `/ready` stays `503` — the same treatment the consistency-config
    /// violation gets.
    #[error("clickhouse is missing required names: {0:?}")]
    MissingServerNames(Vec<&'static str>),
    /// The configured database does not exist and this process does not
    /// create it — `schema/schema.sh` does.
    ///
    /// **Retried, unlike [`StartupError::MissingServerNames`].** A missing
    /// name will not appear; a schema will, the moment someone runs the
    /// script. The chart's init Job and the serving Deployments carry no
    /// ordering between them, so a server that started first must converge
    /// rather than need a restart. The retry warning carries this message,
    /// so every attempt says which command to run.
    #[error(
        "clickhouse database {0:?} does not exist: build it with \
         `schema/schema.sh` (set PULSUS_CLUSTER for the clustered variant), \
         or set PULSUS_SKIP_DDL=1 if the schema is managed elsewhere"
    )]
    SchemaAbsent(String),
}

/// The transient halves of a name check map onto the failures the reconnect
/// loop already retries; a missing name does not (issue #603).
impl From<NameCheckError> for StartupError {
    fn from(err: NameCheckError) -> Self {
        match err {
            NameCheckError::Connect(e) => StartupError::Bootstrap(e),
            NameCheckError::Statement(e) => StartupError::Schema(e),
            NameCheckError::Missing(names) => StartupError::MissingServerNames(names),
            NameCheckError::SchemaAbsent(db) => StartupError::SchemaAbsent(db),
        }
    }
}

/// The per-signal writer slots the reconnect loop fills (all three
/// together, before `pool_slot` — see [`spawn_reconnect_loop`]) in
/// writer-enabled modes. A bundling struct rather than three loose
/// parameters: the slots share one lifecycle and always travel together
/// (and clippy's `too_many_arguments` agrees).
struct WriterSlots {
    log: Arc<OnceLock<Arc<LogWriter>>>,
    metric: Arc<OnceLock<Arc<MetricWriter>>>,
    trace: Arc<OnceLock<Arc<TraceWriter>>>,
}

/// The one-shot channels the reconnect loop hands its spawned background
/// tasks back over, so `run` can abort and join them at shutdown. A bundling
/// struct rather than two loose parameters, for [`WriterSlots`]' reason:
/// they share one lifecycle and always travel together.
struct BackgroundHandoff {
    label_cache_refresh: oneshot::Sender<JoinHandle<()>>,
    reprobe: oneshot::Sender<JoinHandle<()>>,
}

/// Background task: repeatedly checks the server's preconditions and then
/// connects the serving `ChPool`, retrying the whole sequence with capped
/// exponential backoff until it succeeds, then stores the pool and exits.
/// `ChPool::connect` pings fail-fast on any failure, so a cold or
/// unreachable ClickHouse must never block startup — `/ready` reports 503
/// while `pool_slot` is `None` and reflects live ping results once this
/// loop's first successful pass lands.
fn spawn_reconnect_loop(
    pool_slot: Arc<RwLock<Option<Arc<ChPool>>>>,
    writer_slots: WriterSlots,
    label_cache_slot: Arc<OnceLock<Arc<LabelCache>>>,
    config: Arc<Config>,
    handoff: BackgroundHandoff,
    required: &'static [(&'static str, NameCatalogue)],
) -> JoinHandle<()> {
    let BackgroundHandoff {
        label_cache_refresh: label_cache_refresh_tx,
        reprobe: reprobe_tx,
    } = handoff;
    tokio::spawn(async move {
        let mut backoff = INITIAL_BACKOFF;
        loop {
            match ensure_schema_then_connect(&config, required).await {
                Ok(pool) => {
                    tracing::info!("clickhouse schema ready; pool established");
                    let pool = Arc::new(pool);
                    // Construct and store the writer(s) *before* the pool
                    // (issue #15 architect plan): `/ready`=200 (which gates
                    // on `pool_slot`) must imply the ingest route is live
                    // too, not just that the reader's pool exists.
                    if writer_enabled(&config) {
                        // Issue #114: install the consistency policy on the
                        // shared writer client (fallible — validates the
                        // quorum/deadline invariant). A config-invariant
                        // violation is non-self-healing (unlike the transient
                        // connect/schema failures that log-and-backoff), so
                        // it terminates the bootstrap task: the writer/cache
                        // never publish and `/ready` stays 503. In the real
                        // binary this is unreachable — `pulsus_config::load`
                        // already rejected it — but the type enforces it.
                        let client = match ChClient::from_shared_pool(
                            Arc::clone(&pool),
                            config.query_timeout.0,
                        )
                        .with_consistency(consistency_from(&config))
                        {
                            Ok(c) => Arc::new(c),
                            Err(e) => {
                                tracing::error!(
                                    error = %e,
                                    "invalid clickhouse consistency config"
                                );
                                return;
                            }
                        };
                        let writer = Arc::new(LogWriter::new_with_tables(
                            client.clone(),
                            &config.writer,
                            writer_tables_from(&config),
                        ));
                        // `spawn_reconnect_loop` is a one-shot bootstrap
                        // that `return`s on this, its first successful
                        // pass (see this fn's own doc comment) — the slot
                        // can therefore never already be set, so a `set`
                        // failure is unreachable.
                        let _ = writer_slots.log.set(writer);

                        // `MetricWriter` (issue #26 architect plan): same
                        // client, same lifecycle gate — shares one
                        // ClickHouse connection pool with `LogWriter`
                        // rather than opening a second one.
                        let metric_writer = Arc::new(MetricWriter::new_with_tables(
                            client.clone(),
                            &config.writer,
                            pulsus_model::ACTIVITY_BUCKET_MS,
                            metric_writer_tables_from(&config),
                        ));
                        let _ = writer_slots.metric.set(metric_writer);

                        // `TraceWriter` (issue #54): same shared client,
                        // same lifecycle gate as the other two writers.
                        let trace_writer = Arc::new(TraceWriter::new_with_tables(
                            client,
                            &config.writer,
                            trace_writer_tables_from(&config),
                        ));
                        let _ = writer_slots.trace.set(trace_writer);
                    }
                    // The label cache (issue #30 architect plan; code-review
                    // round-1 fix): built and stored *before* `pool_slot` is
                    // published, mirroring the writer-before-pool precedent
                    // above (issue #15) for the identical reason —
                    // `/ready`'s pool check runs first, but on the
                    // multi-threaded runtime another task can observe
                    // `pool_slot = Some` the instant this line below runs,
                    // with nothing forcing it to also observe
                    // `label_cache_slot` already set; `label_cache_ready`
                    // maps an unset slot to 200 (the correct behavior for
                    // writer/init modes), so a `None` slot here would let a
                    // concurrent `/ready` probe pass before the cache even
                    // exists. Publishing the slot first closes that window:
                    // `pool_slot = Some` now implies `label_cache_slot =
                    // Some(cache)` by construction, so `/ready` only ever
                    // needs `label_cache_ready` + `LabelCache::is_warm` to
                    // gate the rest (cold-cache "label cache warming" 503
                    // for the whole first sweep).
                    if reader_enabled(&config) {
                        // Issue #114: the label cache's shared client also
                        // installs the consistency policy (fallible). Same
                        // non-self-healing termination as the writer above.
                        let cache = match build_label_cache(Arc::clone(&pool), &config) {
                            Ok(c) => Arc::new(c),
                            Err(e) => {
                                tracing::error!(
                                    error = %e,
                                    "invalid clickhouse consistency config"
                                );
                                return;
                            }
                        };
                        let _ = label_cache_slot.set(Arc::clone(&cache));
                        let refresh_handle =
                            pulsus_read::spawn_refresh_loop(cache, config.reader.cache_ttl.0);
                        let _ = label_cache_refresh_tx.send(refresh_handle);
                    }
                    *pool_slot.write().await = Some(Arc::clone(&pool));
                    // Issue #43 re-probe: spawned unconditionally right
                    // after `pool_slot` publication — every serving mode
                    // (`all`/`writer`/`reader`) holds a pool, so demoted
                    // endpoints must recover in all of them, not just when
                    // reader-specific features are enabled.
                    let reprobe_handle = spawn_reprobe_loop(Arc::clone(&pool));
                    // The receiver may already be gone (e.g. `run` is tearing
                    // down after a pre-listen failure); in that case there is
                    // nothing left to hand the handle to, so drop it — the
                    // task itself keeps running detached until the process
                    // exits. Same reasoning as `label_cache_refresh_tx` above.
                    let _ = reprobe_tx.send(reprobe_handle);
                    return;
                }
                // Issue #603: a name the server does not have is terminal,
                // not transient. Log it naming every absent name and return:
                // no writer or pool slot publishes and `/ready` stays `503`,
                // exactly as a consistency-config violation is treated.
                Err(StartupError::MissingServerNames(names)) => {
                    tracing::error!(
                        missing = ?names,
                        "clickhouse is missing names this build sends; refusing to serve"
                    );
                    return;
                }
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        backoff_ms = backoff.as_millis() as u64,
                        "clickhouse startup step failed; retrying"
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = std::cmp::min(backoff * 2, MAX_BACKOFF);
                }
            }
        }
    })
}

/// Whether this process mounts the writer subsystem (`docs/architecture.md
/// §1`'s mode table: `all`/`writer` mount ingestion APIs, `reader` does
/// not) — the reconnect loop's gate on constructing a `LogWriter` at all
/// (issue #15 architect plan). Pure so the gate is unit-tested without
/// touching the network.
fn writer_enabled(cfg: &Config) -> bool {
    matches!(cfg.mode, Mode::All | Mode::Writer)
}

/// `Some((cert, key))` iff BOTH `tls_cert` and `tls_key` are set — the
/// inbound-TLS selection rule (issue #174): both set ⇒ the one listener is
/// TLS-only, both unset ⇒ plaintext. One-sided configs never reach here
/// (`pulsus_config::validate` Rule 16 rejects them at startup), so mapping
/// them to `None` is defensive, not a reachable third mode. Pure so the
/// selection is unit-tested, mirroring [`writer_enabled`]'s idiom.
fn tls_paths(cfg: &Config) -> Option<(&str, &str)> {
    match (cfg.tls_cert.as_deref(), cfg.tls_key.as_deref()) {
        (Some(cert), Some(key)) => Some((cert, key)),
        _ => None,
    }
}

/// Whether this process mounts the reader subsystem (`docs/architecture.md
/// §1`'s mode table: `all`/`reader` mount query APIs, `writer` does not) —
/// the reconnect loop's gate on constructing a [`LabelCache`] at all (issue
/// #30 architect plan). Pure so the gate is unit-tested without touching
/// the network, mirroring [`writer_enabled`]'s idiom.
fn reader_enabled(cfg: &Config) -> bool {
    matches!(cfg.mode, Mode::All | Mode::Reader)
}

/// One attempt at "check the preconditions, then connect the serving pool"
/// — the unit the reconnect loop retries as a whole. Retrying the pair
/// together (rather than caching a one-off "checked" flag) keeps this
/// self-healing: a pool connect that fails after a successful check is
/// simply checked again, and both checks are reads.
///
/// **This process creates no schema.** `schema/schema.sh` does. An absent
/// database is reported as such, naming the script, rather than becoming
/// `UNKNOWN_DATABASE` out of the pool connect.
async fn ensure_schema_then_connect(
    config: &Config,
    required: &'static [(&'static str, NameCatalogue)],
) -> Result<ChPool, StartupError> {
    // Issue #603: FIRST — no name is sent before it is known to exist.
    check_server_names(config, required).await?;
    // `PULSUS_SKIP_DDL=1` is the escape hatch for an operator-managed
    // schema this check cannot see, e.g. one built under a different
    // database name and reached through a view.
    if !config.skip_ddl {
        check_schema_present(config).await?;
    }
    ChPool::connect(conn_config_from(config))
        .await
        .map_err(StartupError::Pool)
}

/// Initializes the global `tracing` subscriber from `cfg.log_level`
/// (`PULSUS_LOG_LEVEL`, already parsed by `pulsus-config` — the single
/// source for that env var). `tracing`'s global subscriber is a
/// process-global singleton, so a second call (e.g. across tests in one
/// binary) is expected to fail; that failure is ignored rather than
/// propagated, matching the metrics recorder's same caveat below.
fn init_tracing(log_level: LogLevel) {
    let directive = match log_level {
        LogLevel::Error => "error",
        LogLevel::Warn => "warn",
        LogLevel::Info => "info",
        LogLevel::Debug => "debug",
        LogLevel::Trace => "trace",
    };
    let filter = EnvFilter::try_new(directive).unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

/// Installs the process-global Prometheus recorder. The recorder is a
/// process-global singleton (`metrics::set_global_recorder`); a second
/// install attempt within the same process is expected to fail (multiple
/// tests in one binary), in which case a standalone, unlinked handle is
/// returned instead of a hard startup error — `/metrics` still renders
/// (empty) rather than the process refusing to start.
fn install_metrics_recorder() -> PrometheusHandle {
    match PrometheusBuilder::new().install_recorder() {
        Ok(handle) => handle,
        Err(err) => {
            tracing::warn!(
                error = %err,
                "prometheus recorder already installed for this process; using a standalone handle"
            );
            PrometheusBuilder::new().build_recorder().handle()
        }
    }
}

/// Installs `reader.template_timezone` as the process's LogQL template
/// zone (issue #311). The zone is process-global — server configuration,
/// fixed for the process lifetime — so, exactly like the Prometheus
/// recorder above, a second install within one process (several tests in
/// one binary) is expected rather than fatal. It is only *reported* when
/// the second install disagrees with the first, since a real process
/// installs one configuration once.
fn install_template_timezone(config: &Config) {
    let tz = config.reader.template_timezone;
    match pulsus_read::logql::template::install_template_timezone(tz.tz()) {
        Ok(()) => tracing::info!(
            template_timezone = tz.name(),
            "LogQL template time functions render in the configured timezone"
        ),
        Err(err) if err.installed != err.attempted => tracing::warn!(
            error = %err,
            "LogQL template timezone left at the already-installed zone"
        ),
        Err(_) => {}
    }
}

/// Waits for SIGINT (Ctrl+C) or, on Unix, SIGTERM — whichever arrives
/// first. `axum::serve(...).with_graceful_shutdown` awaits this future
/// before draining in-flight requests and returning.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(err) => {
                tracing::error!(error = %err, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
    tracing::info!("shutdown signal received; draining in-flight requests");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Issue #603: the two transient halves of a name check map onto the
    /// failures the reconnect loop already retries, and only a missing name
    /// is terminal. Pure, so nothing here depends on server behaviour.
    #[test]
    fn a_name_check_failure_maps_to_the_startup_error_that_matches_it() {
        assert!(matches!(
            StartupError::from(NameCheckError::Statement(SchemaError::Version(
                "nonsense".to_string()
            ))),
            StartupError::Schema(_),
        ));
        assert!(matches!(
            StartupError::from(NameCheckError::Missing(vec!["pulsus_not_a_setting"])),
            StartupError::MissingServerNames(names) if names == vec!["pulsus_not_a_setting"],
        ));
    }

    /// Issue #603: a name the server does not have is TERMINAL. The bootstrap
    /// task returns rather than backing off, no writer or pool slot
    /// publishes, and `/ready` stays `503`.
    ///
    /// `skip_ddl = true` and the built-in `default` database, so the pool can
    /// connect with no schema run at all: the check is not gated by
    /// `skip_ddl`, and that is what this shows.
    #[tokio::test]
    async fn a_missing_server_name_publishes_nothing() {
        if !pulsus_testkit::live_clickhouse_enabled() {
            eprintln!("skipping: PULSUS_TEST_CLICKHOUSE is not set");
            return;
        }
        const MADE_UP: &[(&str, NameCatalogue)] =
            &[("pulsus_not_a_setting", NameCatalogue::Setting)];

        let base = Config::default();
        let cfg = Arc::new(Config {
            mode: Mode::Writer,
            skip_ddl: true,
            clickhouse: pulsus_config::ClickHouseConfig {
                database: "default".to_string(),
                http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
                    .ok()
                    .and_then(|p| p.parse().ok())
                    .unwrap_or(19123),
                server: std::env::var("PULSUS_TEST_CH_HOST")
                    .unwrap_or_else(|_| "localhost".to_string()),
                ..base.clickhouse.clone()
            },
            ..base
        });

        for (required, expect_published) in [(MADE_UP, false), (REQUIRED_SERVER_NAMES, true)] {
            let pool_slot: Arc<RwLock<Option<Arc<ChPool>>>> = Arc::new(RwLock::new(None));
            let slots = WriterSlots {
                log: Arc::new(OnceLock::new()),
                metric: Arc::new(OnceLock::new()),
                trace: Arc::new(OnceLock::new()),
            };
            let (refresh_tx, _refresh_rx) = oneshot::channel();
            let (reprobe_tx, _reprobe_rx) = oneshot::channel();
            let handle = spawn_reconnect_loop(
                Arc::clone(&pool_slot),
                WriterSlots {
                    log: Arc::clone(&slots.log),
                    metric: Arc::clone(&slots.metric),
                    trace: Arc::clone(&slots.trace),
                },
                Arc::new(OnceLock::new()),
                Arc::clone(&cfg),
                BackgroundHandoff {
                    label_cache_refresh: refresh_tx,
                    reprobe: reprobe_tx,
                },
                required,
            );
            tokio::time::timeout(Duration::from_secs(30), handle)
                .await
                .expect("the bootstrap task must finish, not back off forever")
                .expect("it does not panic");

            let published = pool_slot.read().await.is_some();
            assert_eq!(
                published, expect_published,
                "the pool slot must publish only when every required name exists"
            );
            assert_eq!(
                slots.metric.get().is_some(),
                expect_published,
                "the metric writer slot follows the pool slot"
            );
            if !expect_published {
                assert!(slots.log.get().is_none());
                assert!(slots.trace.get().is_none());
            }
        }
    }

    #[tokio::test]
    async fn reconnect_loop_handle_is_abortable_before_it_ever_connects() {
        let mut cfg = Config::default();
        cfg.clickhouse.http_port = 1; // nothing listens here
        cfg.clickhouse.pool_size = 1;
        let (label_cache_refresh_tx, _label_cache_refresh_rx) = oneshot::channel();
        let (reprobe_tx, _reprobe_rx) = oneshot::channel();
        let handle = spawn_reconnect_loop(
            Arc::new(RwLock::new(None)),
            WriterSlots {
                log: Arc::new(OnceLock::new()),
                metric: Arc::new(OnceLock::new()),
                trace: Arc::new(OnceLock::new()),
            },
            Arc::new(OnceLock::new()),
            Arc::new(cfg),
            BackgroundHandoff {
                label_cache_refresh: label_cache_refresh_tx,
                reprobe: reprobe_tx,
            },
            REQUIRED_SERVER_NAMES,
        );
        assert!(!handle.is_finished());
        handle.abort();
        let result = handle.await;
        assert!(result.is_err_and(|e| e.is_cancelled()));
    }

    #[test]
    fn writer_enabled_follows_the_mode_table() {
        use pulsus_config::Mode;

        for mode in [Mode::All, Mode::Writer] {
            let cfg = Config {
                mode,
                ..Config::default()
            };
            assert!(writer_enabled(&cfg), "{mode:?} must mount the writer");
        }
        let cfg = Config {
            mode: Mode::Reader,
            ..Config::default()
        };
        assert!(!writer_enabled(&cfg), "reader must not mount the writer");
    }

    #[test]
    fn reader_enabled_follows_the_mode_table() {
        use pulsus_config::Mode;

        for mode in [Mode::All, Mode::Reader] {
            let cfg = Config {
                mode,
                ..Config::default()
            };
            assert!(reader_enabled(&cfg), "{mode:?} must mount the reader");
        }
        let cfg = Config {
            mode: Mode::Writer,
            ..Config::default()
        };
        assert!(!reader_enabled(&cfg), "writer must not mount the reader");
    }

    /// Issue #174: the plaintext-selection pin — a default config (no TLS
    /// keys) must resolve to `None`, keeping today's raw-`TcpListener`
    /// branch, and only a complete pair selects TLS.
    #[test]
    fn tls_paths_selects_tls_only_when_both_fields_are_set() {
        assert_eq!(tls_paths(&Config::default()), None);

        let both = Config {
            tls_cert: Some("/etc/pulsus/server.crt".to_string()),
            tls_key: Some("/etc/pulsus/server.key".to_string()),
            ..Config::default()
        };
        assert_eq!(
            tls_paths(&both),
            Some(("/etc/pulsus/server.crt", "/etc/pulsus/server.key"))
        );

        // One-sided configs are rejected by `pulsus_config::validate`
        // (Rule 16) before `run()` is ever reached; the `None` mapping
        // here is defensive, never a reachable third mode.
        let cert_only = Config {
            tls_cert: Some("/etc/pulsus/server.crt".to_string()),
            ..Config::default()
        };
        assert_eq!(tls_paths(&cert_only), None);
        let key_only = Config {
            tls_key: Some("/etc/pulsus/server.key".to_string()),
            ..Config::default()
        };
        assert_eq!(tls_paths(&key_only), None);
    }

    /// Codex re-review finding (issue #174): branch *selection* alone is
    /// not a plaintext regression proof — with no TLS configured, the raw
    /// `TcpListener` path must still actually serve an HTTP request end
    /// to end. Hermetic: the same `build_router` + `axum::serve(listener,
    /// router)` shape `run()`'s `None` arm uses, driven by a bare
    /// loopback HTTP/1.1 GET against `/buildinfo` (a route that is 200
    /// with no ClickHouse behind it).
    #[tokio::test]
    async fn plaintext_listener_still_serves_http_when_tls_is_unset() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let config = Config::default();
        assert_eq!(
            tls_paths(&config),
            None,
            "the default config must select the plaintext branch"
        );

        let (_tail_tx, tail_rx) = tokio::sync::watch::channel(false);
        let state = AppState {
            pool: Arc::new(RwLock::new(None)),
            config: Arc::new(config.clone()),
            // A standalone handle, not the process-global recorder — this
            // test must not fight `install_metrics_recorder`'s singleton.
            metrics: PrometheusBuilder::new().build_recorder().handle(),
            build: BuildInfo::from_build_env(),
            writer: Arc::new(WriterSink::new(Arc::new(OnceLock::new()))),
            metric_writer: Arc::new(MetricWriterSink::new(Arc::new(OnceLock::new()))),
            trace_writer: Arc::new(TraceWriterSink::new(Arc::new(OnceLock::new()))),
            label_cache: Arc::new(OnceLock::new()),
            eval_gate: Arc::new(pulsus_read::EvalGate::new(
                config.reader.query_eval_concurrency,
            )),
            started_at: std::time::SystemTime::now(),
            tail: Arc::new(crate::app::TailRuntime::new(
                tail_rx,
                config.reader.tail_max_connections,
            )),
        };
        let router = app::build_router(state, &config).expect("router builds for defaults");

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });

        let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
        stream
            .write_all(b"GET /buildinfo HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .expect("write request");
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
            .await
            .expect("a response within 5s")
            .expect("read response");
        let text = String::from_utf8_lossy(&response);
        assert!(
            text.starts_with("HTTP/1.1 200"),
            "the plaintext listener must serve /buildinfo with 200, got: {text}"
        );
        assert!(
            text.contains("version"),
            "the /buildinfo body must arrive intact over plaintext, got: {text}"
        );

        server.abort();
        let _ = server.await;
    }

    /// The label cache refresh task is aborted and joined at shutdown
    /// (issue #30 architect plan).
    #[tokio::test]
    async fn shutdown_background_tasks_stops_a_pending_label_cache_refresh_task_too() {
        let reconnect_handle = tokio::spawn(async {});
        let (label_cache_refresh_tx, label_cache_refresh_rx) = oneshot::channel();
        let refresh_handle = tokio::spawn(std::future::pending::<()>());
        label_cache_refresh_tx
            .send(refresh_handle)
            .unwrap_or_else(|_| panic!("receiver must still be open"));
        // No re-probe task in this scenario: drop the sender immediately.
        let (reprobe_tx, reprobe_rx) = oneshot::channel::<JoinHandle<()>>();
        drop(reprobe_tx);

        tokio::time::timeout(
            Duration::from_secs(5),
            shutdown_background_tasks(reconnect_handle, label_cache_refresh_rx, reprobe_rx),
        )
        .await
        .expect("shutdown_background_tasks must not hang on a pending label cache refresh task");
    }

    /// The background re-probe task is aborted and joined after the refresh
    /// handle (issue #43 re-probe plan).
    #[tokio::test]
    async fn shutdown_background_tasks_stops_a_pending_reprobe_task_too() {
        let reconnect_handle = tokio::spawn(async {});
        // No label cache refresh task in this scenario (same reasoning as
        // the mirrored fix above).
        let (label_cache_refresh_tx, label_cache_refresh_rx) = oneshot::channel::<JoinHandle<()>>();
        drop(label_cache_refresh_tx);
        let (reprobe_tx, reprobe_rx) = oneshot::channel();
        let reprobe_handle = tokio::spawn(std::future::pending::<()>());
        reprobe_tx
            .send(reprobe_handle)
            .unwrap_or_else(|_| panic!("receiver must still be open"));

        tokio::time::timeout(
            Duration::from_secs(5),
            shutdown_background_tasks(reconnect_handle, label_cache_refresh_rx, reprobe_rx),
        )
        .await
        .expect("shutdown_background_tasks must not hang on a pending reprobe task");
    }

    /// A reconnect loop aborted before its first successful pass started
    /// nothing, so shutdown must be a clean no-op rather than hanging on an
    /// empty channel. Same for the label cache refresh task in a
    /// writer-only process.
    #[tokio::test]
    async fn shutdown_background_tasks_is_a_no_op_when_nothing_was_ever_started() {
        let reconnect_handle = tokio::spawn(async {});
        let (label_cache_refresh_tx, label_cache_refresh_rx) = oneshot::channel::<JoinHandle<()>>();
        drop(label_cache_refresh_tx);
        let (reprobe_tx, reprobe_rx) = oneshot::channel::<JoinHandle<()>>();
        drop(reprobe_tx);

        tokio::time::timeout(
            Duration::from_secs(5),
            shutdown_background_tasks(reconnect_handle, label_cache_refresh_rx, reprobe_rx),
        )
        .await
        .expect("shutdown_background_tasks must not hang when nothing was started");
    }

    /// Load-bearing regression test for the review finding: by default
    /// (`skip_ddl = false`) startup must attempt the schema-bootstrap
    /// connection first, not jump straight to the serving pool — otherwise a
    /// fresh ClickHouse (missing database) never becomes ready.
    #[tokio::test]
    async fn ensure_schema_then_connect_checks_the_server_first_by_default() {
        let mut cfg = Config::default();
        cfg.clickhouse.http_port = 1; // nothing listens here
        // `ChPool` is not `Debug` (pulsus-clickhouse), so match manually
        // instead of `.expect_err`/`.unwrap_err`.
        let err = match ensure_schema_then_connect(&cfg, REQUIRED_SERVER_NAMES).await {
            Err(err) => err,
            Ok(_) => panic!("nothing listens on port 1"),
        };
        assert!(matches!(err, StartupError::Bootstrap(_)));
    }

    /// Issue #603: the startup name check runs **first**, and
    /// `PULSUS_SKIP_DDL=1` does not skip it — a `skip_ddl` deployment still
    /// inserts landing blocks and still sends every name. So with nothing
    /// listening the failure is the bootstrap connect's under both
    /// settings, never the pool's.
    #[tokio::test]
    async fn the_name_check_runs_first_whether_or_not_ddl_is_skipped() {
        for skip_ddl in [false, true] {
            let cfg = Config {
                skip_ddl,
                clickhouse: pulsus_config::ClickHouseConfig {
                    http_port: 1, // nothing listens here
                    ..Config::default().clickhouse
                },
                ..Config::default()
            };
            // `ChPool` is not `Debug` (pulsus-clickhouse), so match manually
            // instead of `.expect_err`/`.unwrap_err`.
            let err = match ensure_schema_then_connect(&cfg, REQUIRED_SERVER_NAMES).await {
                Err(err) => err,
                Ok(_) => panic!("nothing listens on port 1"),
            };
            assert!(
                matches!(err, StartupError::Bootstrap(_)),
                "skip_ddl = {skip_ddl}: the name check's own connection fails first"
            );
        }
    }

    #[test]
    fn install_metrics_recorder_does_not_panic_when_called_twice() {
        let _ = install_metrics_recorder();
        // Second call in the same process: must fall back gracefully
        // ("ignore already-set in tests" per the architect plan), not panic.
        let _ = install_metrics_recorder();
    }

    #[test]
    fn init_tracing_does_not_panic_when_called_twice() {
        init_tracing(LogLevel::Debug);
        init_tracing(LogLevel::Debug);
    }

    /// Issue #311: the wiring test — the value in `reader.template_timezone`
    /// really reaches the read path's process setting, rather than being a
    /// config field nothing consumes.
    ///
    /// Scope, stated exactly: this covers `Config → process setting`. That
    /// `serve::run` invokes it is a single unconditional line at the top of
    /// `run`, not covered here (no test boots `run`); an end-to-end check
    /// belongs in the e2e harness, which drives the real binary.
    ///
    /// The zone slot is process-wide and install-once, so this is the ONLY
    /// installer in this test binary (`serve::run` is never called from a
    /// test) — the assertion is exact, and a second installer appearing
    /// here later would redden it rather than weaken it.
    #[test]
    fn install_template_timezone_wires_the_configured_zone_into_the_read_path() {
        use pulsus_read::logql::template::template_timezone;

        let mut cfg = Config::default();
        assert_eq!(
            cfg.reader.template_timezone,
            pulsus_config::TemplateTimezone::UTC,
            "the shipped default is UTC"
        );
        cfg.reader.template_timezone = "Europe/London".parse().expect("known zone");

        install_template_timezone(&cfg);
        assert_eq!(
            template_timezone().name(),
            "Europe/London",
            "the configured zone must reach the read path's process setting"
        );

        // Idempotent: a repeat install in the same process must not panic.
        install_template_timezone(&cfg);
        assert_eq!(template_timezone().name(), "Europe/London");
    }
}
