//! The issue #478 acceptance corpora, in one place because two suites
//! must push the SAME spans: `trace_tag_values_differential.rs` pushes
//! them to the pinned reference and `traces_tag_values_narrow_live.rs`
//! pushes them to PulsusDB. A second copy of a corpus is a second thing
//! to drift.
//!
//! **Every timestamp of a corpus this module PUSHES is derived from the
//! clock at push time, and that is not a style preference.** `trace_spans`
//! and `trace_attrs_idx` carry `TTL … + INTERVAL {{retention_days}} DAY
//! DELETE` with `ttl_only_drop_parts = 1` while `trace_tag_catalog`
//! carries none, so a corpus older than retention is HALF visible: every
//! tag-values assertion still passes off the catalog while the span rows
//! are dropped at insert time. There is no grace window, it behaves
//! identically on a developer machine and in CI, and it presents as the
//! feature being broken rather than as a flake.
//!
//! **The rule is about corpora, not about digits**, and the difference
//! decides whether a literal is a defect. What can age out is a
//! timestamp that is INSERTED as data. A literal that is never inserted
//! cannot: `tags_sql`'s byte-exact goldens compare rendered SQL text
//! containing `1_700_000_000`, a conformance case sends
//! `start=1700003600` to a route that answers `400` before any read, and
//! this issue's fixture records the date its reference capture was
//! taken. None of those is a corpus, none feeds an insert, and none of
//! them ages. Sweeping for date-shaped literals finds all of them and is
//! the wrong instrument; the question to ask of each is whether a row
//! carrying it is written to a TTL'd table.

#![allow(dead_code)]

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status, status};

/// One corpus row: the span name, its service, and its two span
/// attributes.
pub struct Row {
    pub name: &'static str,
    pub service: &'static str,
    pub method: &'static str,
    pub status_code: i64,
}

const fn row(
    name: &'static str,
    service: &'static str,
    method: &'static str,
    status_code: i64,
) -> Row {
    Row {
        name,
        service,
        method,
        status_code,
    }
}

/// C10 — the primary corpus. Adversarial by construction: a fragment
/// inside a longer token (`pay.charge` inside `pay.charge.retry`), a
/// separator-bearing name, a brace path, non-ASCII, the empty string and
/// a single character.
pub const C10: [Row; 10] = [
    row("GET /api/v1/users", "cart", "GET", 200),
    row("GET /api/v1/users/{id}", "cart", "GET", 404),
    row("HTTP POST", "cart", "POST", 200),
    row("checkout-svc.process", "checkout", "POST", 500),
    row("checkout-svc.retry", "checkout", "POST", 500),
    row("派遣クエリ", "checkout", "GET", 200),
    row("", "checkout", "GET", 200),
    row("a", "pay", "GET", 200),
    row("pay.charge", "pay", "POST", 201),
    row("pay.charge.retry", "pay", "POST", 201),
];

/// C10's names in ascending byte order — the order our reads return them
/// in, written out so a test asserts an ORDER rather than a set.
pub const C10_NAMES_ASCENDING: [&str; 10] = [
    "",
    "GET /api/v1/users",
    "GET /api/v1/users/{id}",
    "HTTP POST",
    "a",
    "checkout-svc.process",
    "checkout-svc.retry",
    "pay.charge",
    "pay.charge.retry",
    "派遣クエリ",
];

/// The six adversarial typing names: exactly the shapes a
/// text-classifying inference would call int, float, bool, duration, int
/// and string. Every one must be reported `string`.
pub const C4_TYPED_NAMES: [&str; 6] = ["500", "1.5", "true", "1.5s", "-3", "checkout"];

/// The over-cap name's repeated character and length — 9,000 bytes,
/// past the 8,192-byte string-column cap.
pub const LONG_NAME_CHAR: char = 'L';
pub const LONG_NAME_LEN: usize = 9_000;
/// What the cap renders it as: the first 2,048 code points.
pub const LONG_NAME_CAPPED_LEN: usize = 2_048;

pub fn long_name() -> String {
    std::iter::repeat_n(LONG_NAME_CHAR, LONG_NAME_LEN).collect()
}

/// C4 — the typing corpus, pushed AFTER C10 by both suites. The
/// over-cap name is its last row.
pub fn c4_rows() -> Vec<(String, &'static str, &'static str, i64)> {
    let mut out: Vec<(String, &'static str, &'static str, i64)> = C4_TYPED_NAMES
        .iter()
        .map(|n| ((*n).to_string(), "typed", "GET", 200i64))
        .collect();
    out.push((long_name(), "typed", "GET", 200));
    out
}

fn kv_str(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.to_string())),
        }),
        key_strindex: 0,
    }
}

fn kv_int(key: &str, value: i64) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::IntValue(value)),
        }),
        key_strindex: 0,
    }
}

fn span_of(index: usize, name: &str, method: &str, status_code: i64, base_ns: u64) -> Span {
    let start = base_ns + (index as u64) * 1_000_000;
    let mut trace_id = vec![0u8; 16];
    trace_id[12] = 0xAA;
    trace_id[15] = index as u8;
    let mut span_id = vec![0u8; 8];
    span_id[4] = 0xBB;
    span_id[7] = index as u8;
    Span {
        trace_id,
        span_id,
        name: name.to_string(),
        kind: 2,
        start_time_unix_nano: start,
        end_time_unix_nano: start + 1_500_000,
        attributes: vec![
            kv_str("http.method", method),
            kv_int("http.status_code", status_code),
        ],
        status: Some(Status {
            message: String::new(),
            code: if status_code >= 500 {
                status::StatusCode::Error as i32
            } else {
                status::StatusCode::Ok as i32
            },
        }),
        ..Default::default()
    }
}

fn resource_group(service: &str, spans: Vec<Span>) -> ResourceSpans {
    ResourceSpans {
        resource: Some(Resource {
            attributes: vec![
                kv_str("service.name", service),
                kv_str("deployment.environment", "prod"),
            ],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        }),
        scope_spans: vec![ScopeSpans {
            scope: Some(InstrumentationScope {
                name: "i478".to_string(),
                ..Default::default()
            }),
            spans,
            schema_url: String::new(),
        }],
        schema_url: String::new(),
    }
}

fn group(
    rows: Vec<(usize, String, &'static str, &'static str, i64)>,
    base_ns: u64,
) -> ExportTraceServiceRequest {
    let mut by_service: Vec<(&'static str, Vec<Span>)> = Vec::new();
    for (index, name, service, method, status_code) in rows {
        let span = span_of(index, &name, method, status_code, base_ns);
        match by_service.iter_mut().find(|(s, _)| *s == service) {
            Some((_, spans)) => spans.push(span),
            None => by_service.push((service, vec![span])),
        }
    }
    ExportTraceServiceRequest {
        resource_spans: by_service
            .into_iter()
            .map(|(service, spans)| resource_group(service, spans))
            .collect(),
    }
}

/// The C10 push body.
pub fn c10_request(base_ns: u64) -> ExportTraceServiceRequest {
    group(
        C10.iter()
            .enumerate()
            .map(|(i, r)| (i, r.name.to_string(), r.service, r.method, r.status_code))
            .collect(),
        base_ns,
    )
}

/// The C4 push body. Indices continue past C10's so no span id collides.
pub fn c4_request(base_ns: u64) -> ExportTraceServiceRequest {
    group(
        c4_rows()
            .into_iter()
            .enumerate()
            .map(|(i, (name, service, method, code))| (C10.len() + i, name, service, method, code))
            .collect(),
        base_ns,
    )
}

/// `now - 60s` in nanoseconds — recent enough to be inside retention on
/// both sides and inside the reference's live-store window, which is
/// measured: a corpus stamped 2,000 s in the past was never visible
/// there at all.
pub fn base_ns() -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    base_ns_from(u64::try_from(now).expect("fits u64"))
}

/// Nanoseconds in a UTC day. The tag-values reads bound their window to
/// whole UTC days (ledger `traceql-tag-values-window-is-day-granular`),
/// so which day a corpus and a probe land on is what decides an answer.
pub const NS_PER_DAY: u64 = 86_400_000_000_000;

/// How far past `base_ns` the LAST instant of the widest corpus in this
/// module sits. `span_of` stamps row `i` at `base + i ms` and ends it
/// 1.5 ms later; the widest push is C10 followed by C4, whose last index
/// is `C10.len() + c4_rows().len() - 1 = 16`, so the last instant is
/// `base + 17.5 ms`. One second is that with headroom, and it is the
/// margin [`base_ns_from`] keeps from a UTC midnight.
pub const CORPUS_BLOCK_NS: u64 = 1_000_000_000;

/// [`base_ns`]'s arithmetic, with the clock passed in so it can be swept
/// over a whole day hermetically.
///
/// `now - 60 s`, then nudged FORWARD to the next UTC midnight if the
/// corpus block would otherwise straddle one. A straddling corpus is
/// half in each day, and every day-granular read then answers a partial
/// list — the same class of clock-dependent answer as the one below, at
/// a much lower rate, which is what makes it worth removing rather than
/// living with. The nudge is at most `CORPUS_BLOCK_NS` (1 s), so the
/// corpus stays ~60 s in the past and inside the reference's live-store
/// window.
pub fn base_ns_from(now_ns: u64) -> u64 {
    let base = now_ns - 60_000_000_000;
    let into_day = base % NS_PER_DAY;
    if into_day + CORPUS_BLOCK_NS >= NS_PER_DAY {
        base + (NS_PER_DAY - into_day)
    } else {
        base
    }
}

/// The instant, in whole seconds, that the zero-width-window case
/// (`start == end`, fixture `q_matrix.Q-AZ`) is issued at: the corpus's
/// own timestamp.
///
/// **This is derived from the corpus and not from the request window,
/// and that is the whole point.** The window these suites otherwise use
/// starts an hour before the corpus, and a zero-width probe placed there
/// falls on the PREVIOUS UTC day whenever the suite runs between
/// 00:01:00 and 01:01:00 UTC — measured, by emulating the whole run at
/// 14 virtual wall clocks: 10 values outside that band, 0 inside it,
/// flipping between 00:00:59 and 00:01:01 and back between 01:00:59 and
/// 01:01:01. Both answers obey the day-granular rule; only the input
/// moved. Anchoring the probe on the corpus instant makes the day it
/// resolves to the corpus's day at every wall clock.
pub fn zero_width_probe_secs(base_ns: u64) -> i64 {
    i64::try_from(base_ns / 1_000_000_000).expect("corpus second fits i64")
}

/// The request window every other case is issued over: an hour before
/// the corpus to ten minutes after it, in whole seconds. Its UTC day
/// span must CONTAIN the corpus's day at every wall clock.
pub fn window_secs(base_ns: u64) -> (i64, i64) {
    let base_secs = zero_width_probe_secs(base_ns);
    (base_secs - 3_600, base_secs + 600)
}

/// The empty half of the occupied-day / empty-day pair: an hour-wide
/// window 25 to 26 h before the corpus. Its UTC day span must EXCLUDE
/// the corpus's day at every wall clock — an empty answer over a window
/// that happened to touch the corpus's day would assert nothing.
pub fn empty_day_window_secs(base_ns: u64) -> (i64, i64) {
    let (start, _) = window_secs(base_ns);
    (start - 90_000, start - 86_400)
}

/// The UTC day a second-resolution instant falls in — days since the
/// epoch, the unit `DaySpan` resolves a request window to.
pub fn utc_day(secs: i64) -> i64 {
    secs.div_euclid(86_400)
}

// ---------------------------------------------------------------------
// The issue #476 acceptance corpus, so its committed reference sections
// can be REPLAYED rather than merely co-located (issue #478's fixture
// fold). This is a second Rust copy of the two spans
// `traces_tags_live.rs` pushes into PulsusDB; it exists because that
// suite pushes to PulsusDB and the oracle leg pushes to the reference,
// and the fixture's `_provenance.captures[0].corpus` describes exactly
// these values.
// ---------------------------------------------------------------------

fn kv_bool(key: &str, value: bool) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::BoolValue(value)),
        }),
        key_strindex: 0,
    }
}

fn kv_double(key: &str, value: f64) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::DoubleValue(value)),
        }),
        key_strindex: 0,
    }
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

fn ac_span(
    trace_hex: &str,
    span_hex: &str,
    name: &str,
    base_ns: u64,
    attrs: Vec<KeyValue>,
) -> Span {
    Span {
        trace_id: unhex(trace_hex),
        span_id: unhex(span_hex),
        name: name.to_string(),
        start_time_unix_nano: base_ns,
        end_time_unix_nano: base_ns + 1_000_000,
        attributes: attrs,
        ..Default::default()
    }
}

fn ac_group(service: &str, span: Span) -> ResourceSpans {
    ResourceSpans {
        resource: Some(Resource {
            attributes: vec![kv_str("service.name", service)],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        }),
        scope_spans: vec![ScopeSpans {
            scope: Some(InstrumentationScope {
                name: "io.otel.http".to_string(),
                version: "1.2.3".to_string(),
                ..Default::default()
            }),
            spans: vec![span],
            schema_url: String::new(),
        }],
        schema_url: String::new(),
    }
}

/// The two spans of the issue #476 acceptance corpus: one whose service
/// name is digits and whose attributes are strings that READ as other
/// types, and one carrying the same `port` text as an INT.
pub fn ac_476_request(base_ns: u64) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![
            ac_group(
                "12345",
                ac_span(
                    "1111111111111111aaaaaaaaaaaaaaaa",
                    "aaaaaaaaaaaaaaa1",
                    "checkout",
                    base_ns,
                    vec![
                        kv_str("build", "007"),
                        kv_str("timeout", "2s"),
                        kv_str("enabled", "true"),
                        kv_str("ratio", "1.5"),
                        kv_str("slo", "1.5"),
                        kv_str("note", ""),
                        kv_int("http.status_code", 500),
                        kv_bool("sampled", true),
                        kv_double("cpu", 1.5),
                        kv_str("port", "8080"),
                    ],
                ),
            ),
            ac_group(
                "checkout",
                ac_span(
                    "2222222222222222bbbbbbbbbbbbbbbb",
                    "bbbbbbbbbbbbbbb2",
                    "charge",
                    base_ns,
                    vec![kv_int("port", 8080)],
                ),
            ),
        ],
    }
}

// ---------------------------------------------------------------------
// The issue #509 acceptance corpus: attribute keys containing `?`.
//
// A `?` in query TEXT is a bind placeholder to the ClickHouse driver we
// vendor, and the unnarrowed tag-values read inlines the requested
// attribute key as a literal. The three shapes that produced are all
// here: an odd run (`k?q`, `a?b?c`, `a???b`), an even run (`a??b`, which
// the driver collapses so a DIFFERENT key is asked for) and `?fields`
// (which the driver replaces with the row's column list). Only the first
// was an error; the other two answered `200` with an empty list, which
// is why every case's expected VALUE is asserted and not its status.
//
// `plain509` is the control that puts the `?` in the VALUE rather than
// the key, and `span.nosuchkey` — stored by nobody, and deliberately not
// in this corpus — is the control that says an empty answer is not by
// itself evidence of anything.
// ---------------------------------------------------------------------

/// The two spans of the issue #509 acceptance corpus.
pub fn cq_request(base_ns: u64) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![kv_str("service.name", "q509"), kv_str("res.q?key", "rv?1")],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "i509".to_string(),
                    ..Default::default()
                }),
                spans: vec![
                    ac_span(
                        "50900000000000000000000000000001",
                        "5090000000000001",
                        "q509-a",
                        base_ns,
                        vec![
                            kv_str("k?q", "vq1"),
                            kv_str("a?", "v-trailing"),
                            kv_str("?a", "v-leading"),
                            kv_str("a??b", "v-double"),
                            kv_str("a?b?c", "v-multi"),
                            kv_str("a???b", "v-triple"),
                            kv_str("http.target?raw", "/x?y=1"),
                            kv_str("plain509", "v?1"),
                            kv_str("a?fields", "vf1"),
                            kv_str("?fields", "vf2"),
                        ],
                    ),
                    ac_span(
                        "50900000000000000000000000000002",
                        "5090000000000002",
                        "q509-b",
                        base_ns,
                        vec![kv_str("k?q", "vq2"), kv_str("plain509", "v?2")],
                    ),
                ],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}
