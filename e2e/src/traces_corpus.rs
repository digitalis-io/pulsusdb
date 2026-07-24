//! Deterministic OTLP-traces corpus generator for the M4 differential
//! scenario (issue #60 architect plan v1 + v2 deltas): a seeded set of
//! traces pushed **once** as OTLP/HTTP JSON into the collector, which
//! fans them out — via an `otlphttp` exporter to PulsusDB and an
//! `otlp/tempo` exporter to the pinned reference Tempo — so the two
//! stores receive **identical typed wire data** (plan v2 delta 1: every
//! attribute is emitted as its proper OTLP `AnyValue` variant, never
//! type-erased to a string).
//!
//! **PRNG:** hand-rolled `splitmix64` (issue #16 task-manager resolution
//! #3 precedent, `xtask/src/bench/dataset.rs`; duplicated per that
//! module's "duplicate, don't share" convention — same call
//! `e2e/src/corpus.rs` makes) is used only for trace/span **ids**; every
//! semantic feature of a trace is a pure function of its `trace_idx`, so
//! the whole corpus — including every case's expected trace-id set — is
//! byte-reproducible from a [`TraceCorpusSpec`] alone.
//!
//! **Two independent oracles** (plan v2 delta 6, the circularity
//! breaker): [`generate`] labels every trace with the case ids it
//! satisfies **by construction** (pure `trace_idx` arithmetic mirroring
//! the feature assignment below), while [`naive_matches`] re-derives
//! every shipped predicate directly over the generated **typed** spans —
//! never reading [`GeneratedTrace::matches`] and never sharing the
//! feature formulas. A hermetic unit test asserts the two agree for
//! every case and trace.

use std::collections::BTreeSet;

use crate::corpus::Scale;

/// The resource attribute isolating one run's traces (mirrors
/// `scenarios::RUN_ID_LABEL` / `corpus::RUN_ID_LABEL`, issue #15/#33
/// precedent): every gated `q=` case conjoins
/// `resource.run_id = "<run_id>"`, so a stale stack (`--keep` reruns)
/// can never leak foreign traces into a set-equality comparison.
pub const RUN_ATTR: &str = "run_id";

/// Every committed differential case id, in fixture order — the coverage
/// matrix (`test/fixtures/traces/differential.json` `cases[]`) must match
/// this list exactly (hermetic unit test), so corpus labeling and the
/// committed fixture can never drift.
pub const CASE_IDS: &[&str] = &[
    "service_eq",
    "service_eq_child",
    "service_regex",
    "span_attr_int_gte",
    "span_attr_int_eq",
    "span_attr_bool",
    "span_attr_float_gt",
    "resource_attr_eq",
    "unscoped_attr",
    "attr_regex",
    "neg_attr_key_on_all",
    "neg_regex_key_on_all",
    "neg_attr_missing_key",
    "name_eq",
    "status_error",
    "kind_consumer",
    "duration_gt",
    "conj_service_error",
    "or_spansets",
    "cross_spanset_and",
    "count_gt",
    "avg_duration_gt",
    "select_status",
    "nested_bool",
    // Issue #193: `by()` / `coalesce()` reshape the RESPONSE (grouped
    // spanSet arrays), never the matched TRACE set — so both share
    // name_eq's expectation. The grouped/coalesced spanSet-array VALUE
    // parity is pinned by the dedicated schema-it grouping differential
    // (traces_search_grouping_differential); here they gate that the
    // reshaped response keeps trace-ID-set + validity parity end-to-end.
    "by_name",
    "by_name_coalesce",
];

/// A typed OTLP attribute value (plan v2 delta 1): rendered to the
/// matching protojson `AnyValue` variant by [`to_otlp_export_requests`],
/// so Tempo and PulsusDB both receive the proper numeric/bool/string
/// type and the `val_num` differential is valid.
#[derive(Debug, Clone, PartialEq)]
pub enum AnyVal {
    Str(String),
    Int(i64),
    Double(f64),
    Bool(bool),
}

impl AnyVal {
    fn to_json(&self) -> serde_json::Value {
        match self {
            // protojson: 64-bit integers travel as strings.
            AnyVal::Str(s) => serde_json::json!({ "stringValue": s }),
            AnyVal::Int(i) => serde_json::json!({ "intValue": i.to_string() }),
            AnyVal::Double(d) => serde_json::json!({ "doubleValue": d }),
            AnyVal::Bool(b) => serde_json::json!({ "boolValue": b }),
        }
    }
}

/// Generation parameters for one corpus (issue #60 architect plan
/// interface; `trace_count` comes from the fixture's per-tier sizing).
#[derive(Debug, Clone)]
pub struct TraceCorpusSpec {
    pub seed: u64,
    pub scale: Scale,
    pub trace_count: usize,
    pub step_ns: i64,
    pub base_ns: i64,
    pub run_id: String,
}

/// One generated span, fully typed — the shape both [`naive_matches`]
/// and [`to_otlp_export_requests`] read.
#[derive(Debug, Clone, PartialEq)]
pub struct GeneratedSpan {
    pub span_id: [u8; 8],
    /// All zeros = root (no `parentSpanId` emitted on the wire).
    pub parent_id: [u8; 8],
    pub name: String,
    /// OTEL SpanKind: internal=1, server=2, client=3, producer=4,
    /// consumer=5.
    pub kind: i32,
    /// OTEL StatusCode: unset=0, ok=1, error=2.
    pub status_code: i32,
    pub service: String,
    pub start_ns: i64,
    pub duration_ns: i64,
    /// This span's resource attributes (excluding `service.name`, which
    /// is the `service` field) — identical for every span of the same
    /// service within one trace, so grouping by `service` recovers the
    /// per-resource attribute set.
    pub resource_attrs: Vec<(String, AnyVal)>,
    /// Span-scoped attributes.
    pub attrs: Vec<(String, AnyVal)>,
}

impl GeneratedSpan {
    pub fn end_ns(&self) -> i64 {
        self.start_ns + self.duration_ns
    }
}

/// One generated trace plus the gated-case ids it satisfies **by
/// construction** (pure `trace_idx` arithmetic — see the module doc
/// comment on the two-oracle split).
#[derive(Debug, Clone, PartialEq)]
pub struct GeneratedTrace {
    pub trace_id: [u8; 16],
    pub spans: Vec<GeneratedSpan>,
    pub matches: BTreeSet<&'static str>,
}

/// A generated corpus: every trace, the window it spans, and the run
/// identity every gated query is scoped by.
#[derive(Debug, Clone, PartialEq)]
pub struct TraceCorpus {
    pub run_id: String,
    pub traces: Vec<GeneratedTrace>,
    pub first_ts_ns: i64,
    pub last_ts_ns: i64,
    pub scale: Scale,
}

impl TraceCorpus {
    /// The by-construction span-id set (lowercase hex) for `trace_id_hex`
    /// (32-char lowercase hex) — the trace-by-ID hard gate's expectation
    /// (plan v2 delta 4: span-ID **sets** only).
    pub fn expected_span_ids(&self, trace_id_hex: &str) -> Option<BTreeSet<String>> {
        self.traces
            .iter()
            .find(|t| hex(&t.trace_id) == trace_id_hex)
            .map(|t| t.spans.iter().map(|s| hex(&s.span_id)).collect())
    }

    /// The by-construction expected trace-id set (32-char lowercase hex)
    /// for one committed case — the search gate's expectation.
    pub fn expected_case_trace_ids(&self, case_id: &str) -> BTreeSet<String> {
        self.traces
            .iter()
            .filter(|t| t.matches.contains(case_id))
            .map(|t| hex(&t.trace_id))
            .collect()
    }

    pub fn trace_id_hexes(&self) -> BTreeSet<String> {
        self.traces.iter().map(|t| hex(&t.trace_id)).collect()
    }

    pub fn total_spans(&self) -> usize {
        self.traces.iter().map(|t| t.spans.len()).sum()
    }
}

/// Lowercase hex rendering (the wire form of every id comparison in
/// `traces.rs`).
pub fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// splitmix64 (public-domain mix; duplicated from `e2e/src/corpus.rs` /
/// `xtask/src/bench/dataset.rs` per the established "duplicate, don't
/// share" convention for this generator shape). A bijection on `u64`, so
/// distinct inputs always produce distinct outputs — the id-uniqueness
/// argument below leans on this.
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Disjoint input domains per id kind (`seed ^ domain ^ counter` with
/// domain bits above any realistic counter), so trace-id halves and
/// span ids can never collide with each other across the corpus.
const TRACE_ID_DOMAIN: u64 = 1 << 40;
const SPAN_ID_DOMAIN: u64 = 2 << 40;

/// The id seed folds the run identity into the fixture seed (chained
/// splitmix64, the `corpus.rs` `mix_parts` shape) — still a pure
/// function of the spec (byte-reproducibility holds), but two runs of
/// the same binary (e.g. `traces_roundtrip` then `traces_differential`,
/// or a `--keep` rerun) can never emit colliding trace/span ids. This
/// is load-bearing: both scenarios share one fixture seed, and Tempo
/// merges same-id spans from different runs into one trace (caught by
/// the issue #60 live differential run — PulsusDB's read-time
/// span-id dedup masked the same collision).
fn id_seed(seed: u64, run_id: &str) -> u64 {
    let mut acc = seed;
    for b in run_id.bytes() {
        acc = splitmix64(acc ^ u64::from(b));
    }
    acc
}

fn trace_id_for(seed: u64, t: usize) -> [u8; 16] {
    let hi = splitmix64(seed ^ (TRACE_ID_DOMAIN | (t as u64) << 1));
    let lo = splitmix64(seed ^ (TRACE_ID_DOMAIN | ((t as u64) << 1 | 1)));
    let mut id = [0u8; 16];
    id[..8].copy_from_slice(&hi.to_be_bytes());
    id[8..].copy_from_slice(&lo.to_be_bytes());
    // splitmix64 is a bijection: at most one input in the whole domain
    // maps to 0, so `hi` and `lo` can never both be 0 — an all-zero
    // (OTLP-invalid) trace id is structurally impossible here.
    id
}

fn span_id_for(seed: u64, t: usize, s: usize) -> [u8; 8] {
    let mut raw = splitmix64(seed ^ (SPAN_ID_DOMAIN | (t as u64) << 8 | s as u64));
    if raw == 0 {
        // All-zero span ids are OTLP-invalid (and this corpus's root
        // `parent_id` sentinel). A bijection maps exactly one input to 0;
        // remapping it to a fixed odd constant keeps determinism (the
        // constant collides with another generated id only if splitmix64
        // also maps some corpus input to it — vanishingly unlikely and
        // caught by the uniqueness unit test below).
        raw = 0x0705_0301_0906_0402;
    }
    raw.to_be_bytes()
}

const MS: i64 = 1_000_000;

// ---------------------------------------------------------------------
// Per-trace feature assignment (all pure functions of `trace_idx`) — the
// single place trace semantics are defined. `construction_matches` below
// mirrors these formulas; `naive_matches` (the independent oracle) never
// touches them.
// ---------------------------------------------------------------------

const SERVICES: [&str; 3] = ["svc-checkout", "svc-payments", "svc-inventory"];
const NOTIFY_SERVICE: &str = "svc-notify";

fn root_service(t: usize) -> &'static str {
    SERVICES[t % 3]
}
fn span_count(t: usize) -> usize {
    3 + t % 3
}
fn is_error(t: usize) -> bool {
    t.is_multiple_of(4)
}
fn http_status(t: usize) -> i64 {
    if is_error(t) {
        500 + ((t / 4) % 2) as i64 * 3 // 500 / 503
    } else {
        200
    }
}
fn root_kind(t: usize) -> i32 {
    if t.is_multiple_of(2) { 2 } else { 5 } // server / consumer
}
fn child0_name(t: usize) -> &'static str {
    if t % 4 == 1 {
        "charge-card"
    } else {
        "reserve-stock"
    }
}
fn env_attr(t: usize) -> Option<&'static str> {
    match t % 5 {
        0 | 1 => Some("prod"),
        2 | 3 => Some("staging"),
        _ => None, // the matches-missing-key negation class
    }
}
fn region_attr(t: usize) -> &'static str {
    if t.is_multiple_of(2) { "eu" } else { "us" }
}
fn cache_hit(t: usize) -> bool {
    t.is_multiple_of(2)
}
fn sample_ratio(t: usize) -> f64 {
    if (t >> 1) % 2 == 1 { 0.75 } else { 0.25 }
}
fn tier(t: usize) -> &'static str {
    if t % 6 < 2 { "gold" } else { "standard" }
}
fn child2_is_notify(t: usize) -> bool {
    t % 4 == 2
}
fn root_duration_ms(t: usize) -> i64 {
    50 + 100 * (t % 12) as i64
}
fn child_duration_ms(t: usize, child: usize) -> i64 {
    match child {
        0 => 10 + (t % 7) as i64,
        1 => 15 + (t % 5) as i64,
        2 => 20 + (t % 3) as i64,
        _ => 25 + (t % 2) as i64,
    }
}

/// Total span duration of trace `t` in nanoseconds — by-construction
/// input to the `avg_duration_gt` label.
fn total_duration_ns(t: usize) -> i128 {
    let mut sum = i128::from(root_duration_ms(t) * MS);
    for child in 0..span_count(t) - 1 {
        sum += i128::from(child_duration_ms(t, child) * MS);
    }
    sum
}

/// `avg(duration) > 250ms`, in exact integer arithmetic (`sum > n * lit`
/// ⟺ `sum/n > lit` — all durations are whole milliseconds, sums are far
/// below 2^53, so both stores' f64 averages are exact and agree with
/// this).
const AVG_DURATION_LIT_NS: i128 = 250 * 1_000_000;
fn avg_duration_exceeds(t: usize) -> bool {
    total_duration_ns(t) > AVG_DURATION_LIT_NS * span_count(t) as i128
}

/// The by-construction case labels for trace `t` — mirrors the feature
/// formulas above, never inspects generated spans.
fn construction_matches(t: usize) -> BTreeSet<&'static str> {
    let mut m = BTreeSet::new();
    let mut tag = |case: &'static str, hit: bool| {
        if hit {
            m.insert(case);
        }
    };
    tag("service_eq", t.is_multiple_of(3));
    // The notify child is span index 3 (child 2), which only exists when
    // span_count(t) >= 4 — i.e. t % 3 >= 1 (caught by the independent
    // naive evaluator's agreement test).
    tag(
        "service_eq_child",
        child2_is_notify(t) && span_count(t) >= 4,
    );
    tag("service_regex", t % 3 <= 1);
    tag("span_attr_int_gte", is_error(t));
    tag("span_attr_int_eq", !is_error(t));
    tag("span_attr_bool", cache_hit(t));
    tag("span_attr_float_gt", (t >> 1) % 2 == 1);
    tag("resource_attr_eq", t % 5 < 2);
    tag("unscoped_attr", t % 6 < 2);
    tag("attr_regex", t % 5 != 4);
    tag("neg_attr_key_on_all", t % 2 == 1); // region != "eu" -> us
    tag("neg_regex_key_on_all", t.is_multiple_of(2)); // region !~ "u.*" -> eu
    // Our documented negation rule (docs/api.md §4.2): `!=` matches
    // spans lacking the key entirely as well as differing values.
    tag("neg_attr_missing_key", t % 5 >= 2);
    tag("name_eq", t % 4 == 1);
    tag("status_error", is_error(t));
    tag("kind_consumer", t % 2 == 1);
    tag("duration_gt", t % 12 >= 8);
    tag("conj_service_error", t.is_multiple_of(12)); // checkout (t%3==0) ∧ error (t%4==0)
    tag("or_spansets", is_error(t) || t % 2 == 1);
    tag("cross_spanset_and", t % 6 == 1); // gold (t%6<2) ∧ consumer (odd)
    tag("count_gt", t % 3 == 2); // 5 spans
    tag("avg_duration_gt", avg_duration_exceeds(t));
    tag("select_status", is_error(t));
    tag("nested_bool", t % 5 < 2); // prod traces (derivation: fixture doc)
    // Issue #193: identical filter to name_eq — by()/coalesce() reshape
    // the response, not the matched trace set.
    tag("by_name", t % 4 == 1);
    tag("by_name_coalesce", t % 4 == 1);
    m
}

/// Generates the corpus: every value is a pure function of
/// `(spec.seed, trace_idx, span_idx)` — no threaded RNG state — so a
/// given spec regenerates byte-for-byte (unit-tested below).
pub fn generate(spec: &TraceCorpusSpec) -> TraceCorpus {
    let id_seed = id_seed(spec.seed, &spec.run_id);
    let mut traces = Vec::with_capacity(spec.trace_count);
    for t in 0..spec.trace_count {
        let t0 = spec.base_ns + spec.step_ns * t as i64;
        let root_svc = root_service(t);
        let mut resource_attrs = vec![(RUN_ATTR.to_string(), AnyVal::Str(spec.run_id.clone()))];
        if let Some(env) = env_attr(t) {
            resource_attrs.push(("env".to_string(), AnyVal::Str(env.to_string())));
        }
        resource_attrs.push((
            "region".to_string(),
            AnyVal::Str(region_attr(t).to_string()),
        ));

        let root_id = span_id_for(id_seed, t, 0);
        let mut spans = vec![GeneratedSpan {
            span_id: root_id,
            parent_id: [0u8; 8],
            name: format!("GET /api/{}", root_svc.trim_start_matches("svc-")),
            kind: root_kind(t),
            status_code: 1, // ok
            service: root_svc.to_string(),
            start_ns: t0,
            duration_ns: root_duration_ms(t) * MS,
            resource_attrs: resource_attrs.clone(),
            attrs: Vec::new(),
        }];

        for child in 0..span_count(t) - 1 {
            let (name, kind, status_code, service, attrs): (
                String,
                i32,
                i32,
                String,
                Vec<(String, AnyVal)>,
            ) = match child {
                0 => (
                    child0_name(t).to_string(),
                    3, // client
                    if is_error(t) { 2 } else { 0 },
                    root_svc.to_string(),
                    vec![("http.status_code".to_string(), AnyVal::Int(http_status(t)))],
                ),
                1 => (
                    "cache-lookup".to_string(),
                    1, // internal
                    0,
                    root_svc.to_string(),
                    vec![
                        ("cache_hit".to_string(), AnyVal::Bool(cache_hit(t))),
                        ("sample_ratio".to_string(), AnyVal::Double(sample_ratio(t))),
                        ("tier".to_string(), AnyVal::Str(tier(t).to_string())),
                    ],
                ),
                2 => (
                    "notify".to_string(),
                    3,
                    0,
                    if child2_is_notify(t) {
                        NOTIFY_SERVICE.to_string()
                    } else {
                        root_svc.to_string()
                    },
                    Vec::new(),
                ),
                _ => (
                    "audit-log".to_string(),
                    3,
                    0,
                    root_svc.to_string(),
                    Vec::new(),
                ),
            };
            spans.push(GeneratedSpan {
                span_id: span_id_for(id_seed, t, child + 1),
                parent_id: root_id,
                name,
                kind,
                status_code,
                service,
                start_ns: t0 + (child as i64 + 1) * MS,
                duration_ns: child_duration_ms(t, child) * MS,
                resource_attrs: resource_attrs.clone(),
                attrs,
            });
        }

        traces.push(GeneratedTrace {
            trace_id: trace_id_for(id_seed, t),
            spans,
            matches: construction_matches(t),
        });
    }

    let first_ts_ns = spec.base_ns;
    let last_ts_ns = traces
        .iter()
        .flat_map(|t| t.spans.iter().map(GeneratedSpan::end_ns))
        .max()
        .unwrap_or(spec.base_ns);

    TraceCorpus {
        run_id: spec.run_id.clone(),
        traces,
        first_ts_ns,
        last_ts_ns,
        scale: spec.scale,
    }
}

/// One OTLP/HTTP-JSON `ExportTraceServiceRequest` per trace: spans
/// grouped by service into `resourceSpans` entries (order-preserving —
/// deterministic wire bytes), each carrying its typed resource
/// attributes plus `service.name`, one scope per resource. Typed
/// `AnyValue` variants throughout (plan v2 delta 1).
pub fn to_otlp_export_requests(c: &TraceCorpus) -> Vec<serde_json::Value> {
    c.traces
        .iter()
        .map(|trace| {
            // Group spans by service, preserving first-seen order.
            let mut groups: Vec<(&str, Vec<&GeneratedSpan>)> = Vec::new();
            for span in &trace.spans {
                match groups.iter_mut().find(|(svc, _)| *svc == span.service) {
                    Some((_, spans)) => spans.push(span),
                    None => groups.push((span.service.as_str(), vec![span])),
                }
            }
            let resource_spans: Vec<serde_json::Value> = groups
                .into_iter()
                .map(|(service, spans)| {
                    let mut attrs = vec![serde_json::json!({
                        "key": "service.name",
                        "value": { "stringValue": service },
                    })];
                    // All spans of one service in one trace share the
                    // same resource attrs by construction.
                    for (key, val) in &spans[0].resource_attrs {
                        attrs.push(serde_json::json!({ "key": key, "value": val.to_json() }));
                    }
                    let span_jsons: Vec<serde_json::Value> = spans
                        .iter()
                        .map(|s| span_json(&trace.trace_id, s))
                        .collect();
                    serde_json::json!({
                        "resource": { "attributes": attrs },
                        "scopeSpans": [{
                            "scope": { "name": "pulsus-e2e-traces", "version": "1" },
                            "spans": span_jsons,
                        }],
                    })
                })
                .collect();
            serde_json::json!({ "resourceSpans": resource_spans })
        })
        .collect()
}

fn span_json(trace_id: &[u8; 16], s: &GeneratedSpan) -> serde_json::Value {
    let mut span = serde_json::json!({
        "traceId": hex(trace_id),
        "spanId": hex(&s.span_id),
        "name": s.name,
        "kind": s.kind,
        "startTimeUnixNano": s.start_ns.to_string(),
        "endTimeUnixNano": s.end_ns().to_string(),
        "status": { "code": s.status_code },
    });
    if s.parent_id != [0u8; 8] {
        span["parentSpanId"] = serde_json::Value::String(hex(&s.parent_id));
    }
    if !s.attrs.is_empty() {
        span["attributes"] = serde_json::Value::Array(
            s.attrs
                .iter()
                .map(|(key, val)| serde_json::json!({ "key": key, "value": val.to_json() }))
                .collect(),
        );
    }
    span
}

// ---------------------------------------------------------------------
// The independent naive evaluator (plan v2 delta 6): re-derives every
// shipped predicate directly over the generated typed spans. It must
// never read `GeneratedTrace::matches` and never share the feature
// formulas above — its only inputs are the spans' own typed fields.
// Test-only by design (`#[cfg(test)]`): it exists purely as the AC5
// hermetic oracle; the live scenarios consume the by-construction
// expected sets.
// ---------------------------------------------------------------------

#[cfg(test)]
fn res_str<'a>(span: &'a GeneratedSpan, key: &str) -> Option<&'a str> {
    span.resource_attrs.iter().find_map(|(k, v)| {
        if k == key {
            match v {
                AnyVal::Str(s) => Some(s.as_str()),
                _ => None,
            }
        } else {
            None
        }
    })
}

#[cfg(test)]
fn attr_val<'a>(span: &'a GeneratedSpan, key: &str) -> Option<&'a AnyVal> {
    span.attrs
        .iter()
        .find_map(|(k, v)| if k == key { Some(v) } else { None })
}

#[cfg(test)]
fn attr_int(span: &GeneratedSpan, key: &str) -> Option<i64> {
    match attr_val(span, key) {
        Some(AnyVal::Int(i)) => Some(*i),
        _ => None,
    }
}

#[cfg(test)]
fn attr_f64(span: &GeneratedSpan, key: &str) -> Option<f64> {
    match attr_val(span, key) {
        Some(AnyVal::Double(d)) => Some(*d),
        _ => None,
    }
}

#[cfg(test)]
fn attr_bool(span: &GeneratedSpan, key: &str) -> Option<bool> {
    match attr_val(span, key) {
        Some(AnyVal::Bool(b)) => Some(*b),
        _ => None,
    }
}

/// Unscoped attribute lookup (`.key`): resource scope then span scope,
/// string values only (the one unscoped case is a string attr).
#[cfg(test)]
fn unscoped_str<'a>(span: &'a GeneratedSpan, key: &str) -> Option<&'a str> {
    res_str(span, key).or(match attr_val(span, key) {
        Some(AnyVal::Str(s)) => Some(s.as_str()),
        _ => None,
    })
}

#[cfg(test)]
fn run_scoped(span: &GeneratedSpan, run_id: &str) -> bool {
    res_str(span, RUN_ATTR) == Some(run_id)
}

/// Evaluates one committed case's predicate over `t`'s typed spans —
/// the AC5 independent oracle. Every arm re-states the case's TraceQL
/// semantics (same-span conjunction inside one `{…}`, trace-level
/// `&&`/`||` across spansets, pipeline aggregates over the matched
/// span set, our documented matches-missing negation rule).
#[cfg(test)]
pub fn naive_matches(case_id: &str, t: &GeneratedTrace, run_id: &str) -> bool {
    let spans = || t.spans.iter().filter(|s| run_scoped(s, run_id));
    match case_id {
        "service_eq" => spans().any(|s| s.service == "svc-checkout"),
        "service_eq_child" => spans().any(|s| s.service == "svc-notify"),
        // `=~ "svc-(checkout|payments)"`, full-value anchored.
        "service_regex" => {
            spans().any(|s| s.service == "svc-checkout" || s.service == "svc-payments")
        }
        "span_attr_int_gte" => {
            spans().any(|s| attr_int(s, "http.status_code").is_some_and(|v| v >= 500))
        }
        "span_attr_int_eq" => {
            spans().any(|s| attr_int(s, "http.status_code").is_some_and(|v| v == 200))
        }
        "span_attr_bool" => spans().any(|s| attr_bool(s, "cache_hit") == Some(true)),
        "span_attr_float_gt" => {
            spans().any(|s| attr_f64(s, "sample_ratio").is_some_and(|v| v > 0.5))
        }
        "resource_attr_eq" => spans().any(|s| res_str(s, "env") == Some("prod")),
        "unscoped_attr" => spans().any(|s| unscoped_str(s, "tier") == Some("gold")),
        // `=~ "prod|staging"`, full-value anchored: a missing key never
        // matches a positive regex.
        "attr_regex" => {
            spans().any(|s| matches!(res_str(s, "env"), Some("prod") | Some("staging")))
        }
        // `region != "eu"` — every span carries `region`.
        "neg_attr_key_on_all" => spans().any(|s| res_str(s, "region") != Some("eu")),
        // `region !~ "u.*"` (anchored): matches when the value does NOT
        // match — i.e. region == "eu" (or, per our documented rule, a
        // span lacking the key — none exist for `region`).
        "neg_regex_key_on_all" => {
            spans().any(|s| !res_str(s, "region").is_some_and(|v| v.starts_with('u')))
        }
        // `env != "prod"` under our documented rule: spans lacking the
        // key entirely match, as do differing values.
        "neg_attr_missing_key" => spans().any(|s| res_str(s, "env") != Some("prod")),
        "name_eq" => spans().any(|s| s.name == "charge-card"),
        // Issue #193: by()/coalesce() do not change the matched trace set.
        "by_name" | "by_name_coalesce" => spans().any(|s| s.name == "charge-card"),
        "status_error" => spans().any(|s| s.status_code == 2),
        "kind_consumer" => spans().any(|s| s.kind == 5),
        "duration_gt" => spans().any(|s| s.duration_ns > 800 * MS),
        // Same-span conjunction: one span must satisfy all four leaves.
        "conj_service_error" => spans().any(|s| {
            s.service == "svc-checkout"
                && attr_int(s, "http.status_code").is_some_and(|v| v >= 500)
                && s.status_code == 2
        }),
        "or_spansets" => spans().any(|s| s.status_code == 2) || spans().any(|s| s.kind == 5),
        "cross_spanset_and" => {
            spans().any(|s| unscoped_str(s, "tier") == Some("gold")) && spans().any(|s| s.kind == 5)
        }
        "count_gt" => spans().count() > 4,
        "avg_duration_gt" => {
            let (mut sum, mut n) = (0i128, 0i128);
            for s in spans() {
                sum += i128::from(s.duration_ns);
                n += 1;
            }
            n > 0 && sum > AVG_DURATION_LIT_NS * n
        }
        // `select()` never changes the matched trace set.
        "select_status" => {
            spans().any(|s| attr_int(s, "http.status_code").is_some_and(|v| v >= 500))
        }
        // `(env = "prod" || http.status_code = 200) && (kind = consumer
        // || cache_hit = true)` — same-span conjunction of the two
        // parenthesized disjunctions.
        "nested_bool" => spans().any(|s| {
            (res_str(s, "env") == Some("prod")
                || attr_int(s, "http.status_code").is_some_and(|v| v == 200))
                && (s.kind == 5 || attr_bool(s, "cache_hit") == Some(true))
        }),
        other => panic!("naive_matches: unknown case id {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(trace_count: usize) -> TraceCorpusSpec {
        TraceCorpusSpec {
            seed: 20_260_716,
            scale: Scale::Ci,
            trace_count,
            step_ns: 1_000_000_000,
            base_ns: 1_700_000_000_000_000_000,
            run_id: "e2e-traces-test-run".to_string(),
        }
    }

    /// AC5: byte-reproducibility — the corpus is a pure function of its
    /// spec.
    #[test]
    fn generate_is_deterministic_for_the_same_spec() {
        let a = generate(&spec(24));
        let b = generate(&spec(24));
        assert_eq!(a, b);
        // And the OTLP wire rendering is too.
        assert_eq!(to_otlp_export_requests(&a), to_otlp_export_requests(&b));
    }

    /// AC5 (plan v2 delta 6, the circularity breaker): the independent
    /// naive evaluator — which reads only the generated typed spans —
    /// agrees with the by-construction labels for every shipped case and
    /// every trace, at both tier sizes.
    #[test]
    fn naive_evaluator_agrees_with_by_construction_labels_for_every_case_and_trace() {
        for count in [24, 120] {
            let corpus = generate(&spec(count));
            for (idx, trace) in corpus.traces.iter().enumerate() {
                for case in CASE_IDS {
                    assert_eq!(
                        naive_matches(case, trace, &corpus.run_id),
                        trace.matches.contains(case),
                        "case {case:?} disagrees on trace_idx {idx} (trace_count {count})"
                    );
                }
            }
        }
    }

    /// Every gated comparison is a set equality, which is only
    /// well-defined when neither store truncates: every case must match
    /// at least one trace (a vacuously-empty case gates nothing) and
    /// strictly fewer than the differential request limit, at both
    /// tiers.
    #[test]
    fn every_case_set_is_non_empty_and_below_the_differential_limit() {
        const DIFFERENTIAL_LIMIT: usize = 500; // traces.rs requests this
        for count in [24, 120] {
            let corpus = generate(&spec(count));
            for case in CASE_IDS {
                let set = corpus.expected_case_trace_ids(case);
                assert!(
                    !set.is_empty(),
                    "case {case:?} matches no trace at trace_count {count}"
                );
                assert!(
                    set.len() < DIFFERENTIAL_LIMIT,
                    "case {case:?} matches {} traces at trace_count {count} — not below the \
                     {DIFFERENTIAL_LIMIT} request limit",
                    set.len()
                );
            }
        }
    }

    /// At least one case must also be a strict subset of the corpus (a
    /// gate where every case matched everything would prove nothing).
    #[test]
    fn case_sets_are_not_all_the_full_corpus() {
        let corpus = generate(&spec(24));
        let all = corpus.trace_id_hexes();
        assert!(
            CASE_IDS
                .iter()
                .any(|c| corpus.expected_case_trace_ids(c) != all),
            "every case matched the whole corpus"
        );
    }

    #[test]
    fn trace_and_span_ids_are_unique_across_the_corpus() {
        let corpus = generate(&spec(120));
        let mut trace_ids = BTreeSet::new();
        let mut span_ids = BTreeSet::new();
        for t in &corpus.traces {
            assert!(trace_ids.insert(t.trace_id), "duplicate trace id");
            assert_ne!(t.trace_id, [0u8; 16]);
            for s in &t.spans {
                assert!(span_ids.insert(s.span_id), "duplicate span id");
                assert_ne!(s.span_id, [0u8; 8]);
            }
        }
    }

    #[test]
    fn every_trace_has_one_root_and_children_parented_to_it() {
        let corpus = generate(&spec(24));
        for t in &corpus.traces {
            let roots: Vec<_> = t.spans.iter().filter(|s| s.parent_id == [0u8; 8]).collect();
            assert_eq!(roots.len(), 1);
            let root_id = roots[0].span_id;
            for s in &t.spans {
                if s.parent_id != [0u8; 8] {
                    assert_eq!(s.parent_id, root_id);
                }
            }
        }
    }

    /// Plan v2 delta 1: the corpus deliberately exercises every OTLP
    /// `AnyValue` type — string, int, double, bool — as typed wire data.
    #[test]
    fn corpus_carries_all_four_typed_attribute_variants_on_the_wire() {
        let corpus = generate(&spec(24));
        let rendered = serde_json::to_string(&to_otlp_export_requests(&corpus)).unwrap();
        for token in ["stringValue", "intValue", "doubleValue", "boolValue"] {
            assert!(rendered.contains(token), "wire payload missing {token}");
        }
        // And the int travels protojson-style, as a string.
        assert!(rendered.contains(r#""intValue":"200""#));
    }

    #[test]
    fn export_requests_group_spans_by_resource_service() {
        let corpus = generate(&spec(24));
        let requests = to_otlp_export_requests(&corpus);
        assert_eq!(requests.len(), corpus.traces.len());
        // trace_idx 2 (t%4==2) carries a svc-notify child -> 2 resources.
        let multi = &requests[2]["resourceSpans"];
        assert_eq!(multi.as_array().unwrap().len(), 2);
        // trace_idx 1 is single-service -> 1 resource.
        assert_eq!(requests[1]["resourceSpans"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn expected_span_ids_matches_the_generated_span_set() {
        let corpus = generate(&spec(24));
        let t0 = &corpus.traces[0];
        let expected: BTreeSet<String> = t0.spans.iter().map(|s| hex(&s.span_id)).collect();
        assert_eq!(corpus.expected_span_ids(&hex(&t0.trace_id)), Some(expected));
        assert_eq!(corpus.expected_span_ids(&"0".repeat(32)), None);
    }

    #[test]
    fn hex_renders_lowercase_fixed_width() {
        assert_eq!(hex(&[0x0a, 0xff, 0x00]), "0aff00");
    }
}
