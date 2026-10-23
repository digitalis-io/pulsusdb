//! `POST /v1/logs`, `POST /v1/metrics`, `POST /v1/traces`, and
//! `POST /api/v1/write` server wiring (issue #15/#27/#28/#54 architect
//! plans): [`WriterSink`]/[`MetricWriterSink`]/[`TraceWriterSink`] adapt
//! their async-filled writer slots (`serve::spawn_reconnect_loop`
//! constructs the real [`LogWriter`]/[`MetricWriter`]/[`TraceWriter`] once
//! the ClickHouse pool is ready, *before* `pool_slot` — see that module's
//! doc comment) to `pulsus-write`'s [`LogSink`]/[`MetricSink`]/
//! [`TraceSink`] seams, and [`ingest_logs`]/[`ingest_metrics`]/
//! [`ingest_traces`]/[`ingest_remote_write`] are the thin
//! `State<AppState>` handlers that call straight into
//! `pulsus_write::ingest`/`pulsus_write::ingest_metrics`/
//! `pulsus_write::ingest_traces`/`pulsus_write::ingest_remote_write`'s
//! state-agnostic cores (see those fns' doc comments for why the server
//! cannot reuse `pulsus_write::ingest::http::{logs,metrics,traces}::<S>`'s
//! generic-`State` mount points). [`ingest_remote_write`] (issue #28)
//! reuses [`MetricWriterSink`] verbatim — the same
//! `AppState.metric_writer` field #27 introduced — adding only its own
//! route + handler, per the ratified "second issue rebases onto the first"
//! ordering rule.

use std::sync::{Arc, OnceLock};

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::Response;
use axum::routing::post;
use pulsus_model::Tenant;

use pulsus_write::writer::{
    MetricWriterMetricsSnapshot, TraceWriterMetricsSnapshot, WriterMetricsSnapshot,
};
use pulsus_write::{
    AdmitRefusal, FlushWait, LogSink, LogWriter, MetricSink, MetricWriter, ParsedLogs,
    ParsedMetrics, ParsedTraceLanding, PushHeaders, TraceSink, TraceWriter,
};

use crate::app::AppState;

/// Adapts the writer slot (`Arc<OnceLock<Arc<LogWriter>>>`, filled at most
/// once by the reconnect loop) to [`LogSink`]: delegates to the live
/// writer once it exists, and returns [`Backpressure`] (→ 429; the OTLP
/// collector retries) while the slot is still empty — the same "not ready
/// yet" contract `/ready` gives every other consumer of the pool before
/// the reconnect loop's first successful pass.
pub(crate) struct WriterSink {
    slot: Arc<OnceLock<Arc<LogWriter>>>,
    /// Test-only snapshot seam (issue #214 AC-7): lets a hermetic test
    /// drive the real `/metrics` bridge with a populated snapshot without a
    /// live `ChClient` (a `LogWriter` cannot be built or moved off zero
    /// without one). Stripped from release builds — production `metrics()`
    /// is exactly `self.slot.get().map(|w| w.metrics())`.
    #[cfg(test)]
    metrics_override: Option<WriterMetricsSnapshot>,
}

impl WriterSink {
    pub(crate) fn new(slot: Arc<OnceLock<Arc<LogWriter>>>) -> Self {
        WriterSink {
            slot,
            #[cfg(test)]
            metrics_override: None,
        }
    }

    /// Test-only constructor seeding [`WriterSink::metrics`] with a fixed
    /// snapshot (issue #214 AC-7).
    #[cfg(test)]
    pub(crate) fn with_metrics_snapshot(snap: WriterMetricsSnapshot) -> Self {
        WriterSink {
            slot: Arc::new(OnceLock::new()),
            metrics_override: Some(snap),
        }
    }

    /// The writer's ingest-metrics snapshot when the slot is filled (a
    /// writer/all process), else `None` (a reader-only process never fills
    /// the slot). Read-only `load(Relaxed)` on already-maintained atomics —
    /// no ingest-hot-path work (issue #214).
    pub(crate) fn metrics(&self) -> Option<WriterMetricsSnapshot> {
        #[cfg(test)]
        if let Some(snap) = self.metrics_override {
            return Some(snap);
        }
        self.slot.get().map(|w| w.metrics())
    }
}

impl LogSink for WriterSink {
    fn admit(
        &self,
        tenant: &Tenant,
        batch: ParsedLogs,
        push: PushHeaders,
    ) -> Result<(), AdmitRefusal> {
        match self.slot.get() {
            Some(writer) => writer.admit(tenant, batch, push),
            None => Err(AdmitRefusal::Backpressure),
        }
    }

    fn admit_flush(
        &self,
        tenant: &Tenant,
        batch: ParsedLogs,
        push: PushHeaders,
    ) -> Result<FlushWait, AdmitRefusal> {
        match self.slot.get() {
            Some(writer) => writer.admit_flush(tenant, batch, push),
            None => Err(AdmitRefusal::Backpressure),
        }
    }
}

/// [`WriterSink`]'s metrics counterpart (issue #27, deferred from #26):
/// adapts `Arc<OnceLock<Arc<MetricWriter>>>` to [`MetricSink`], same
/// "backpressure while empty" contract.
pub(crate) struct MetricWriterSink {
    slot: Arc<OnceLock<Arc<MetricWriter>>>,
    /// Test-only snapshot seam (issue #214 AC-7); see [`WriterSink`].
    #[cfg(test)]
    metrics_override: Option<MetricWriterMetricsSnapshot>,
}

impl MetricWriterSink {
    pub(crate) fn new(slot: Arc<OnceLock<Arc<MetricWriter>>>) -> Self {
        MetricWriterSink {
            slot,
            #[cfg(test)]
            metrics_override: None,
        }
    }

    /// Test-only constructor seeding [`MetricWriterSink::metrics`] (issue
    /// #214 AC-7).
    #[cfg(test)]
    pub(crate) fn with_metrics_snapshot(snap: MetricWriterMetricsSnapshot) -> Self {
        MetricWriterSink {
            slot: Arc::new(OnceLock::new()),
            metrics_override: Some(snap),
        }
    }

    /// The metric writer's ingest-metrics snapshot, else `None` on an empty
    /// slot (issue #214). See [`WriterSink::metrics`].
    pub(crate) fn metrics(&self) -> Option<MetricWriterMetricsSnapshot> {
        #[cfg(test)]
        if let Some(snap) = self.metrics_override {
            return Some(snap);
        }
        self.slot.get().map(|w| w.metrics())
    }
}

impl MetricSink for MetricWriterSink {
    fn admit(
        &self,
        tenant: &Tenant,
        batch: ParsedMetrics,
        push: PushHeaders,
    ) -> Result<(), AdmitRefusal> {
        match self.slot.get() {
            Some(writer) => writer.admit(tenant, batch, push),
            None => Err(AdmitRefusal::Backpressure),
        }
    }

    fn admit_flush(
        &self,
        tenant: &Tenant,
        batch: ParsedMetrics,
        push: PushHeaders,
    ) -> Result<FlushWait, AdmitRefusal> {
        match self.slot.get() {
            Some(writer) => writer.admit_flush(tenant, batch, push),
            None => Err(AdmitRefusal::Backpressure),
        }
    }
}

/// [`WriterSink`]'s traces counterpart (issue #54): adapts
/// `Arc<OnceLock<Arc<TraceWriter>>>` to [`TraceSink`], same "backpressure
/// while empty" contract.
pub(crate) struct TraceWriterSink {
    slot: Arc<OnceLock<Arc<TraceWriter>>>,
    /// Test-only snapshot seam (issue #214 AC-7); see [`WriterSink`].
    #[cfg(test)]
    metrics_override: Option<TraceWriterMetricsSnapshot>,
}

impl TraceWriterSink {
    pub(crate) fn new(slot: Arc<OnceLock<Arc<TraceWriter>>>) -> Self {
        TraceWriterSink {
            slot,
            #[cfg(test)]
            metrics_override: None,
        }
    }

    /// Test-only constructor seeding [`TraceWriterSink::metrics`] (issue
    /// #214 AC-7).
    #[cfg(test)]
    pub(crate) fn with_metrics_snapshot(snap: TraceWriterMetricsSnapshot) -> Self {
        TraceWriterSink {
            slot: Arc::new(OnceLock::new()),
            metrics_override: Some(snap),
        }
    }

    /// The trace writer's ingest-metrics snapshot, else `None` on an empty
    /// slot (issue #214). See [`WriterSink::metrics`].
    pub(crate) fn metrics(&self) -> Option<TraceWriterMetricsSnapshot> {
        #[cfg(test)]
        if let Some(snap) = self.metrics_override {
            return Some(snap);
        }
        self.slot.get().map(|w| w.metrics())
    }
}

impl TraceSink for TraceWriterSink {
    fn admit(&self, landing: ParsedTraceLanding, push: PushHeaders) -> Result<(), AdmitRefusal> {
        match self.slot.get() {
            Some(writer) => writer.admit(landing, push),
            None => Err(AdmitRefusal::Backpressure),
        }
    }

    fn admit_flush(
        &self,
        landing: ParsedTraceLanding,
        push: PushHeaders,
    ) -> Result<FlushWait, AdmitRefusal> {
        match self.slot.get() {
            Some(writer) => writer.admit_flush(landing, push),
            None => Err(AdmitRefusal::Backpressure),
        }
    }
}

/// `POST /v1/logs` (docs/api.md §1.1): pulls `AppState`'s `WriterSink` and
/// hands straight into `pulsus_write::ingest`'s reused #8 core — no logic
/// of its own beyond that seam.
pub(crate) async fn ingest_logs(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    pulsus_write::ingest(
        state.writer.as_ref(),
        headers,
        body,
        log_ingest_settings(&state),
    )
    .await
}

/// Builds the log receivers' settings from `AppState`, mirroring
/// `ingest_metrics`'s `MetricIngestSettings` construction below. One place,
/// so the OTLP and Loki-push receivers cannot disagree about a knob.
fn log_ingest_settings(state: &AppState) -> pulsus_write::LogIngestSettings {
    pulsus_write::LogIngestSettings {
        discover_log_levels: state.config.writer.discover_log_levels,
    }
}

/// `POST /v1/metrics` (docs/api.md §1.1, issue #27): pulls `AppState`'s
/// `MetricWriterSink` and hands straight into
/// `pulsus_write::ingest_metrics`'s reused core — no logic of its own
/// beyond that seam.
pub(crate) async fn ingest_metrics(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let settings = metric_ingest_settings(&state.config);
    pulsus_write::ingest_metrics(state.metric_writer.as_ref(), headers, body, settings).await
}

/// The OTLP metrics settings a configuration gives `POST /v1/metrics`.
pub(crate) fn metric_ingest_settings(
    config: &pulsus_config::Config,
) -> pulsus_write::MetricIngestSettings {
    pulsus_write::MetricIngestSettings {
        exp_histogram_mode: config.exp_histogram_mode,
        translation_strategy: config.otlp_translation_strategy,
        promote_scope_metadata: config.otlp_promote_scope_metadata,
        // `target_info`'s cadence is the PromQL lookback halved
        // (`helper.go:560-567 @ v3.13.0`), so the write path reads the same
        // knob the read path evaluates against.
        promql_lookback_ms: i64::try_from(config.reader.promql_lookback.0.as_millis())
            .unwrap_or(i64::MAX),
    }
}

/// `POST /v1/traces` (docs/api.md §1.1, issue #54): pulls `AppState`'s
/// `TraceWriterSink` and hands straight into
/// `pulsus_write::ingest_traces`'s reused core — no logic of its own
/// beyond that seam.
pub(crate) async fn ingest_traces(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    pulsus_write::ingest_traces(state.trace_writer.as_ref(), headers, body).await
}

/// `POST /api/v1/write` (docs/api.md §1.2, issue #28): Prometheus remote-
/// write. Pulls `AppState`'s `MetricWriterSink` — the same instance
/// `ingest_metrics` uses — and hands straight into
/// `pulsus_write::ingest_remote_write`'s reused core; no logic of its own
/// beyond that seam.
pub(crate) async fn ingest_remote_write(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    pulsus_write::ingest_remote_write(state.metric_writer.as_ref(), headers, body).await
}

/// `POST /loki/api/v1/push` (docs/api.md §8.2, issue #77): the flag-gated
/// Loki log push receiver. Pulls `AppState`'s `WriterSink` — the same
/// instance `ingest_logs` uses (a Loki push feeds the *existing* log-storage
/// path) — and hands straight into `pulsus_write::ingest_loki_push`'s reused
/// core; no logic of its own beyond that seam.
pub(crate) async fn ingest_loki_push(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    pulsus_write::ingest_loki_push(
        state.writer.as_ref(),
        headers,
        body,
        log_ingest_settings(&state),
    )
    .await
}

/// The Loki push compat surface (issue #77): a single writer-side compat
/// route, merged by `compat::apply_aliases` iff `PULSUS_COMPAT_ENDPOINTS`
/// **and** the Writer subsystem is mounted (`Gate::CompatAndWriter`) — the
/// first compat surface gated on Writer rather than Reader. Kept here beside
/// the handler (mirroring `logs_api::compat_router`'s co-location with its
/// query handlers).
pub(crate) fn loki_push_compat_router() -> Router<AppState> {
    Router::new().route("/loki/api/v1/push", post(ingest_loki_push))
}

/// `POST /api/v2/spans` (+ `/tempo/spans`) (docs/api.md §8.2, issue #75):
/// the flag-gated Zipkin v2 JSON trace receiver. Pulls `AppState`'s
/// `TraceWriterSink` — the same instance `ingest_traces` uses (a Zipkin
/// span adapts to OTLP and feeds the *existing* trace-storage path) — and
/// hands straight into `pulsus_write::ingest_zipkin`'s reused core; no logic
/// of its own beyond that seam.
pub(crate) async fn ingest_zipkin(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    pulsus_write::ingest_zipkin(state.trace_writer.as_ref(), headers, body).await
}

/// The Zipkin v2 JSON compat surface (issue #75): both documented ingest
/// paths bind to the same handler, merged by `compat::apply_aliases` iff
/// `PULSUS_COMPAT_ENDPOINTS` **and** the Writer subsystem is mounted
/// (`Gate::CompatAndWriter`, the Loki push precedent). Kept here beside the
/// handler.
pub(crate) fn zipkin_compat_router() -> Router<AppState> {
    Router::new()
        .route("/api/v2/spans", post(ingest_zipkin))
        .route("/tempo/spans", post(ingest_zipkin))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulsus_model::Fingerprint;

    use pulsus_model::UnixNano;
    use pulsus_write::LogRow;

    /// Issue #506, docs/configuration.md §6: `otlp_promote_scope_metadata`
    /// set to `true` in a YAML file loads through the binary's own loader,
    /// reaches `POST /v1/metrics`'s settings, and puts the scope's name and
    /// version on the series as `otel_scope_*` labels. By default it does
    /// none of that.
    #[test]
    fn scope_promotion_parses_and_reaches_the_metric_labels() {
        use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
        use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
        use opentelemetry_proto::tonic::metrics::v1::{
            Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric,
            number_data_point,
        };

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "my-scope".to_string(),
                        version: "1.0.0".to_string(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                    }),
                    metrics: vec![Metric {
                        name: "up".to_string(),
                        description: String::new(),
                        unit: String::new(),
                        metadata: vec![],
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![],
                                start_time_unix_nano: 0,
                                time_unix_nano: 1_700_000_000_000_000_000,
                                exemplars: vec![],
                                flags: 0,
                                value: Some(number_data_point::Value::AsDouble(1.0)),
                            }],
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        for (case, yaml, scope) in [
            (
                "enabled",
                "otlp_promote_scope_metadata: true\n",
                Some(("my-scope", "1.0.0")),
            ),
            ("default", "{}\n", None),
        ] {
            let path = std::env::temp_dir().join(format!(
                "pulsus-scope-promotion-{case}-{}.yaml",
                std::process::id()
            ));
            std::fs::write(&path, yaml).expect("write the configuration file");
            let loaded = pulsus_config::load(Some(&path), None);
            let _ = std::fs::remove_file(&path);
            let config = loaded.unwrap_or_else(|e| panic!("{case}: the configuration loads: {e}"));

            let settings = metric_ingest_settings(&config);
            assert_eq!(settings.promote_scope_metadata, scope.is_some(), "{case}");
            let parsed = pulsus_write::parse_metrics(&request, 0, settings)
                .unwrap_or_else(|e| panic!("{case}: the request parses: {e:?}"));
            let up = parsed
                .series
                .iter()
                .find(|s| &*s.metric_name == "up")
                .unwrap_or_else(|| panic!("{case}: an `up` series"));
            assert_eq!(
                up.labels.get("otel_scope_name"),
                scope.map(|(name, _)| name),
                "{case}"
            );
            assert_eq!(
                up.labels.get("otel_scope_version"),
                scope.map(|(_, version)| version),
                "{case}"
            );
        }
    }

    fn batch() -> ParsedLogs {
        ParsedLogs {
            rows: vec![LogRow {
                service: "svc".to_string(),
                fingerprint: Fingerprint::from_raw(1),
                timestamp_ns: UnixNano(1),
                severity: 0,
                body: "hello".to_string(),
                structured_metadata: String::new(),
            }],
            ..Default::default()
        }
    }

    #[test]
    fn admit_is_backpressure_while_the_slot_is_empty() {
        let sink = WriterSink::new(Arc::new(OnceLock::new()));
        assert_eq!(
            sink.admit(
                &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                batch(),
                PushHeaders::default()
            ),
            Err(AdmitRefusal::Backpressure)
        );
    }

    #[test]
    fn admit_flush_is_backpressure_while_the_slot_is_empty() {
        let sink = WriterSink::new(Arc::new(OnceLock::new()));
        assert!(
            sink.admit_flush(
                &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                batch(),
                PushHeaders::default()
            )
            .is_err()
        );
    }

    fn metrics_batch() -> ParsedMetrics {
        ParsedMetrics {
            samples: vec![pulsus_write::MetricPoint {
                metric_name: Arc::from("up"),
                fingerprint: Fingerprint::from_raw(1),
                unix_milli: 1,
                value: 1.0,
            }],
            ..Default::default()
        }
    }

    #[test]
    fn metric_admit_is_backpressure_while_the_slot_is_empty() {
        let sink = MetricWriterSink::new(Arc::new(OnceLock::new()));
        assert_eq!(
            sink.admit(
                &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                metrics_batch(),
                PushHeaders::default()
            ),
            Err(AdmitRefusal::Backpressure)
        );
    }

    #[test]
    fn metric_admit_flush_is_backpressure_while_the_slot_is_empty() {
        let sink = MetricWriterSink::new(Arc::new(OnceLock::new()));
        assert!(
            sink.admit_flush(
                &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                metrics_batch(),
                PushHeaders::default()
            )
            .is_err()
        );
    }

    #[test]
    fn trace_admit_is_backpressure_while_the_slot_is_empty() {
        let sink = TraceWriterSink::new(Arc::new(OnceLock::new()));
        assert_eq!(
            sink.admit(ParsedTraceLanding::default(), PushHeaders::default()),
            Err(AdmitRefusal::Backpressure)
        );
    }

    #[test]
    fn trace_admit_flush_is_backpressure_while_the_slot_is_empty() {
        let sink = TraceWriterSink::new(Arc::new(OnceLock::new()));
        assert!(
            sink.admit_flush(ParsedTraceLanding::default(), PushHeaders::default())
                .is_err()
        );
    }
}
